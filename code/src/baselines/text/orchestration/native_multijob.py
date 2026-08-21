"""Run single or staggered jobs through unmodified native text adapters.

This is deliberately a *thin* experiment harness: it starts immutable
manifest jobs at absolute times and preserves their official ``run-shard``
evidence.  It does not own batching, admission, routing, credit, or inflight
control, so it can characterize how the native Daft/Ray paths behave when jobs
overlap without relabelling a project scheduler as a framework baseline.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import random
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, Mapping, Sequence

from src.baselines.common.cell_instrumentation import instrumented_cell
from src.baselines.common.contracts import BaselineRequestResult
from src.baselines.common.database_identity import DatabaseIdentity
from src.baselines.common.redact import redact_argument_list, redact_text
from src.baselines.common.manifests import partition_summary, read_manifest
from src.baselines.common.provenance import adapter_provenance
from src.baselines.common.results import validate_results
from src.baselines.text.orchestration.gate_runner import (
    sample_vllm_token_counters,
    wait_for_idle,
)
from src.infrastructure.config_env import expand_structure
from src.infrastructure.ray_runtime_preflight import (
    RayNofileProbe,
    probe_ray_worker_nofile,
    validate_ray_worker_nofile,
)
from src.infrastructure.runner_lease import acquire_host_runner_lease


NATIVE_MULTI_JOB_ADAPTERS = frozenset(
    {"daft_native", "daft_ray", "ray_data_http", "duckdb_ai"}
)
_RAY_ADAPTERS = frozenset({"daft_ray", "ray_data_http"})
_BANNED_COMMAND_TOKENS = frozenset(
    {
        "shared-credit",
        "shared_work",
        "max-active-work",
        "project-profiler",
        "project_static",
        "coordinator",
        "shared-ready",
        "shared_ready",
        "--inflight",
        "--router",
        "--credit",
    }
)


@dataclass(frozen=True)
class NativeMultiJobJob:
    """One immutable, statically sharded job with a fixed absolute offset."""

    job_id: str
    manifest: Path
    manifest_sha256: str
    offset_s: float
    endpoint_summary: dict[str, object]


@dataclass(frozen=True)
class TimedPostgresManifestSource:
    """PostgreSQL source that is loaded and verified inside every shard."""

    database_url: str
    workload_name: str


@dataclass(frozen=True)
class NativeMultiJobArm:
    """Frozen native adapter configuration applied to one, two, or four jobs."""

    arm_id: str
    adapter: str
    python_executable: str
    concurrency_per_endpoint: int
    batch_size: int
    timeout_s: float
    process_timeout_s: float
    ray_address: str | None
    jobs: tuple[NativeMultiJobJob, ...]


@dataclass(frozen=True)
class NativeMultiJobConfig:
    """Formal repeat contract for native single/multi-job characterization."""

    experiment_id: str
    output_root: Path
    endpoint_urls: tuple[str, str]
    endpoint_ids: tuple[str, ...]
    model: str
    service_signature: tuple[tuple[str, object], ...]
    protocol: str | None
    output_cap: int | None
    organizer: str | None
    api_key_env: str | None
    service_prefix_caching: str
    service_max_num_seqs: int
    service_max_num_batched_tokens: int
    idle_timeout_s: float
    launch_lead_s: float
    warmup_repeats: int
    formal_repeats: int
    schedule_seed: int
    endpoint_work_skew_max: float
    minimum_measurement_seconds: float
    source: TimedPostgresManifestSource | None
    job_internal_arrival_contract: Literal["manifest_timed", "eager"]
    mfu_status: str
    gpu_peak_tflops_per_gpu: float
    mfu_precision: str
    mfu_reason: str
    performance_writeback_mode: str
    arms: tuple[NativeMultiJobArm, ...]
    service_identity: tuple[tuple[str, object], ...] = ()


@dataclass(frozen=True)
class NativeRunIdentity:
    """Explicit identity supplied by the matrix that owns cell ordering."""

    phase: str
    repeat: int
    order_index: int
    run_id: str | None = None


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or "REPLACE_ME" in value:
        raise ValueError(f"{field} must be a resolved non-empty string")
    return value.strip()


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _positive_float(value: object, field: str, *, allow_zero: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a finite number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0 or (not allow_zero and parsed == 0):
        comparison = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{field} must be a finite {comparison} number")
    return parsed


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _parse_job(
    raw: object,
    *,
    arm_id: str,
    seen_job_ids: set[str],
    seen_doc_ids: set[int],
    skew_max: float,
) -> NativeMultiJobJob:
    if not isinstance(raw, dict):
        raise ValueError(f"arm {arm_id} job must be an object")
    allowed = {"id", "manifest", "offset_s"}
    unknown = set(raw) - allowed
    missing = allowed - set(raw)
    if unknown or missing:
        raise ValueError(f"arm {arm_id} job fields invalid: missing={sorted(missing)} unknown={sorted(unknown)}")
    job_id = _string(raw["id"], f"arm {arm_id} job id")
    if job_id in seen_job_ids:
        raise ValueError(f"arm {arm_id} duplicate job id: {job_id}")
    seen_job_ids.add(job_id)
    manifest = Path(_string(raw["manifest"], f"arm {arm_id} job {job_id} manifest"))
    if not manifest.is_file():
        raise FileNotFoundError(f"arm {arm_id} job {job_id} manifest does not exist: {manifest}")
    requests = read_manifest(manifest)
    doc_ids = {request.doc_id for request in requests}
    overlap = doc_ids & seen_doc_ids
    if overlap:
        raise ValueError(f"arm {arm_id} jobs have overlapping doc_ids: {sorted(overlap)[:5]}")
    seen_doc_ids.update(doc_ids)
    if {request.endpoint_index for request in requests} != {0, 1}:
        raise ValueError(f"arm {arm_id} job {job_id} manifest must use endpoints 0 and 1")
    summary = partition_summary(requests, 2)
    if float(summary["endpoint_work_skew"]) > skew_max:
        raise ValueError(
            f"arm {arm_id} job {job_id} endpoint work skew exceeds {skew_max}: "
            f"{summary['endpoint_work_skew']}"
        )
    return NativeMultiJobJob(
        job_id=job_id,
        manifest=manifest,
        manifest_sha256=_sha256(manifest),
        offset_s=_positive_float(raw["offset_s"], f"arm {arm_id} job {job_id} offset_s", allow_zero=True),
        endpoint_summary=summary,
    )


def _parse_arm(raw: object, *, seen_arm_ids: set[str], skew_max: float) -> NativeMultiJobArm:
    if not isinstance(raw, dict):
        raise ValueError("each arm must be an object")
    required = {
        "id", "adapter", "python_executable", "concurrency_per_endpoint",
        "batch_size", "timeout_s", "process_timeout_s", "ray_address", "jobs",
    }
    missing = required - set(raw)
    unknown = set(raw) - required
    if missing or unknown:
        raise ValueError(f"arm fields invalid: missing={sorted(missing)} unknown={sorted(unknown)}")
    arm_id = _string(raw["id"], "arm id")
    if arm_id in seen_arm_ids:
        raise ValueError(f"duplicate arm id: {arm_id}")
    seen_arm_ids.add(arm_id)
    adapter = _string(raw["adapter"], f"arm {arm_id} adapter")
    if adapter not in NATIVE_MULTI_JOB_ADAPTERS:
        raise ValueError(f"arm {arm_id} adapter must be one of {sorted(NATIVE_MULTI_JOB_ADAPTERS)}")
    ray_address = raw["ray_address"]
    if adapter in _RAY_ADAPTERS:
        ray_address = _string(ray_address, f"arm {arm_id} ray_address")
    elif ray_address is not None:
        raise ValueError(f"arm {arm_id} ray_address must be null for {adapter}")
    jobs_raw = raw["jobs"]
    if not isinstance(jobs_raw, list) or len(jobs_raw) not in {1, 2, 4}:
        raise ValueError(f"arm {arm_id} must define exactly one, two, or four jobs")
    seen_job_ids: set[str] = set()
    seen_doc_ids: set[int] = set()
    jobs = tuple(
        _parse_job(
            item, arm_id=arm_id, seen_job_ids=seen_job_ids,
            seen_doc_ids=seen_doc_ids, skew_max=skew_max,
        )
        for item in jobs_raw
    )
    zero_offsets = sum(job.offset_s == 0 for job in jobs)
    positive_offsets = {job.offset_s for job in jobs if job.offset_s > 0}
    if len(jobs) == 1:
        if zero_offsets != 1:
            raise ValueError(f"arm {arm_id} single job must start at offset zero")
    elif zero_offsets != 1 or len(positive_offsets) != 1:
        raise ValueError(
            f"arm {arm_id} multi-job run requires exactly one zero-offset job "
            "and one shared positive arrival offset for every later job"
        )
    return NativeMultiJobArm(
        arm_id=arm_id,
        adapter=adapter,
        python_executable=_string(raw["python_executable"], f"arm {arm_id} python_executable"),
        concurrency_per_endpoint=_positive_int(raw["concurrency_per_endpoint"], f"arm {arm_id} concurrency_per_endpoint"),
        batch_size=_positive_int(raw["batch_size"], f"arm {arm_id} batch_size"),
        timeout_s=_positive_float(raw["timeout_s"], f"arm {arm_id} timeout_s"),
        process_timeout_s=_positive_float(
            raw["process_timeout_s"], f"arm {arm_id} process_timeout_s"
        ),
        ray_address=ray_address if isinstance(ray_address, str) else None,
        jobs=jobs,
    )


def load_native_multijob_config(
    path: str | Path, *, allow_existing_output_root: bool = False
) -> NativeMultiJobConfig:
    """Load the narrow single/multi-job contract and reject scheduler controls."""

    payload = expand_structure(json.loads(Path(path).read_text(encoding="utf-8")), "native_multijob_config")
    if not isinstance(payload, dict):
        raise ValueError("native multi-job config must be an object")
    required = {
        "schema_version", "experiment_id", "formal", "output_root", "endpoint_urls", "model",
        "api_key_env", "service", "idle_timeout_s", "launch_lead_s", "warmup_repeats",
        "formal_repeats", "schedule_seed", "endpoint_work_skew_max", "arms",
        "minimum_measurement_seconds",
    }
    matrix_optional = {
        "endpoint_ids", "service_signature", "protocol", "output_cap", "organizer",
        "source", "job_internal_arrival_contract", "mfu_contract",
        "performance_writeback_mode", "service_identity",
    }
    missing = required - set(payload)
    unknown = set(payload) - required - matrix_optional
    if missing or unknown:
        raise ValueError(f"config fields invalid: missing={sorted(missing)} unknown={sorted(unknown)}")
    if payload["schema_version"] != 1 or payload["formal"] is not True:
        raise ValueError("native multi-job runner requires schema_version=1 and formal=true")
    source_raw = payload.get("source")
    if source_raw is not None:
        if not isinstance(source_raw, dict) or set(source_raw) != {
            "kind", "database_url", "workload_name"
        }:
            raise ValueError("source must define kind, database_url, and workload_name")
        if source_raw["kind"] != "timed_postgres_manifest":
            raise ValueError("rankable native source must be timed_postgres_manifest")
    arrival_contract = _string(
        payload.get("job_internal_arrival_contract", "eager"),
        "job_internal_arrival_contract",
    )
    if arrival_contract != "eager":
        raise ValueError("native multi-job job_internal_arrival_contract must be eager")
    performance_writeback_mode = _string(
        payload.get("performance_writeback_mode", "none"),
        "performance_writeback_mode",
    )
    if performance_writeback_mode not in {"none", "json_text"}:
        raise ValueError("performance_writeback_mode must be none or json_text")
    mfu_raw = payload.get("mfu_contract")
    if mfu_raw is None:
        # Generic native characterization predates MFU publication. It remains
        # loadable, but its sentinel identity cannot match the SAOR matrix.
        mfu_raw = {
            "status": "unavailable",
            "gpu_peak_tflops_per_gpu": 0.0,
            "precision": "unknown",
            "reason": "not_declared_by_generic_native_contract",
        }
    elif not isinstance(mfu_raw, dict) or set(mfu_raw) != {
        "status", "gpu_peak_tflops_per_gpu", "precision", "reason",
    }:
        raise ValueError("native multi-job mfu_contract has an invalid schema")
    mfu_status = _string(mfu_raw["status"], "mfu_contract.status")
    if mfu_status not in {"available", "unavailable"}:
        raise ValueError("mfu_contract.status must be available or unavailable")
    output_root = Path(_string(payload["output_root"], "output_root"))
    if output_root.exists() and not allow_existing_output_root:
        raise FileExistsError(f"output_root already exists: {output_root}")
    endpoints = payload["endpoint_urls"]
    suffix = "/v1/chat/completions"
    if not isinstance(endpoints, list) or len(endpoints) != 2 or any(not isinstance(url, str) or not url.endswith(suffix) for url in endpoints):
        raise ValueError("endpoint_urls must contain exactly two chat-completions endpoints")
    service = payload["service"]
    if not isinstance(service, dict) or set(service) != {"prefix_caching", "max_num_seqs", "max_num_batched_tokens"}:
        raise ValueError("service must contain prefix_caching, max_num_seqs, max_num_batched_tokens")
    model = _string(payload["model"], "model")
    signature = payload.get("service_signature", {})
    if not isinstance(signature, dict):
        raise ValueError("service_signature must be an object")
    if signature:
        if _string(signature.get("model"), "service_signature.model") != model:
            raise ValueError("service_signature.model must equal model")
        _string(signature.get("service"), "service_signature.service")
    service_identity = payload.get("service_identity", {})
    if not isinstance(service_identity, dict):
        raise ValueError("service_identity must be an object")
    endpoint_ids = payload.get("endpoint_ids", [])
    if endpoint_ids and (
        not isinstance(endpoint_ids, list)
        or len(endpoint_ids) != 2
        or len(set(endpoint_ids)) != 2
        or any(not isinstance(item, str) or not item for item in endpoint_ids)
    ):
        raise ValueError("endpoint_ids must contain two unique non-empty strings")
    prefix = _string(service["prefix_caching"], "service.prefix_caching")
    if prefix not in {"enabled", "disabled", "unknown"}:
        raise ValueError("service.prefix_caching must be enabled/disabled/unknown")
    skew_max = _positive_float(payload["endpoint_work_skew_max"], "endpoint_work_skew_max", allow_zero=True)
    if skew_max >= 1:
        raise ValueError("endpoint_work_skew_max must be below 1")
    arms_raw = payload["arms"]
    if not isinstance(arms_raw, list) or len(arms_raw) < 2:
        raise ValueError("native multi-job formal comparison requires at least two arms")
    seen_arm_ids: set[str] = set()
    arms = tuple(_parse_arm(item, seen_arm_ids=seen_arm_ids, skew_max=skew_max) for item in arms_raw)
    warmup_repeats = _positive_int(payload["warmup_repeats"], "warmup_repeats")
    if warmup_repeats != 1:
        raise ValueError("native multi-job matrix freezes warmup_repeats=1")
    api_key_env = payload["api_key_env"]
    if api_key_env is not None:
        api_key_env = _string(api_key_env, "api_key_env")
    return NativeMultiJobConfig(
        experiment_id=_string(payload["experiment_id"], "experiment_id"),
        output_root=output_root,
        endpoint_urls=(endpoints[0], endpoints[1]),
        endpoint_ids=tuple(endpoint_ids),
        model=model,
        service_signature=tuple(sorted(signature.items())),
        protocol=(
            _string(payload["protocol"], "protocol")
            if "protocol" in payload else None
        ),
        output_cap=(
            _positive_int(payload["output_cap"], "output_cap")
            if "output_cap" in payload else None
        ),
        organizer=(
            _string(payload["organizer"], "organizer")
            if "organizer" in payload else None
        ),
        api_key_env=api_key_env,
        service_prefix_caching=prefix,
        service_max_num_seqs=_positive_int(service["max_num_seqs"], "service.max_num_seqs"),
        service_max_num_batched_tokens=_positive_int(service["max_num_batched_tokens"], "service.max_num_batched_tokens"),
        idle_timeout_s=_positive_float(payload["idle_timeout_s"], "idle_timeout_s"),
        launch_lead_s=_positive_float(payload["launch_lead_s"], "launch_lead_s", allow_zero=True),
        warmup_repeats=warmup_repeats,
        formal_repeats=_positive_int(payload["formal_repeats"], "formal_repeats"),
        schedule_seed=_positive_int(payload["schedule_seed"], "schedule_seed"),
        endpoint_work_skew_max=skew_max,
        minimum_measurement_seconds=_positive_float(
            payload["minimum_measurement_seconds"], "minimum_measurement_seconds"
        ),
        source=(
            TimedPostgresManifestSource(
                database_url=_string(source_raw["database_url"], "source.database_url"),
                workload_name=_string(source_raw["workload_name"], "source.workload_name"),
            )
            if source_raw is not None else None
        ),
        job_internal_arrival_contract=arrival_contract,
        mfu_status=mfu_status,
        gpu_peak_tflops_per_gpu=_positive_float(
            mfu_raw["gpu_peak_tflops_per_gpu"],
            "mfu_contract.gpu_peak_tflops_per_gpu",
            allow_zero=True,
        ),
        mfu_precision=_string(mfu_raw["precision"], "mfu_contract.precision"),
        mfu_reason=_string(mfu_raw["reason"], "mfu_contract.reason"),
        performance_writeback_mode=performance_writeback_mode,
        arms=arms,
        service_identity=tuple(sorted(service_identity.items())),
    )


def balanced_arm_order(config: NativeMultiJobConfig, phase: str, repeat: int) -> tuple[NativeMultiJobArm, ...]:
    """Seeded cyclic arm order; formal positions rotate deterministically."""

    if phase not in {"warmup", "formal"} or repeat <= 0:
        raise ValueError("phase must be warmup/formal and repeat must be positive")
    arms = list(config.arms)
    random.Random(f"{config.schedule_seed}:{phase}").shuffle(arms)
    offset = (repeat - 1) % len(arms)
    return tuple(arms[offset:] + arms[:offset])


def _metrics_url(endpoint_url: str) -> str:
    return endpoint_url.removesuffix("/v1/chat/completions") + "/metrics"


def _assert_immutable(job: NativeMultiJobJob) -> None:
    if _sha256(job.manifest) != job.manifest_sha256:
        raise RuntimeError(f"immutable manifest changed during execution: {job.manifest}")


def build_shard_command(
    *, runner_script: str | Path, arm: NativeMultiJobArm, job: NativeMultiJobJob,
    endpoint_index: int, endpoint_url: str, output_dir: Path, model: str,
    service_prefix_caching: str, service_max_num_seqs: int,
    service_max_num_batched_tokens: int, api_key: str | None,
    source: TimedPostgresManifestSource | None = None,
) -> list[str]:
    """Build one official adapter invocation; no project scheduler flag is accepted."""

    command = [
        arm.python_executable, str(runner_script), "run-shard", "--adapter", arm.adapter,
        "--manifest", str(job.manifest), "--endpoint-index", str(endpoint_index),
        "--endpoint-url", endpoint_url, "--model", model, "--concurrency",
        str(arm.concurrency_per_endpoint), "--batch-size", str(arm.batch_size),
        "--timeout-s", str(arm.timeout_s), "--output-dir", str(output_dir),
        "--service-prefix-caching", service_prefix_caching, "--service-max-num-seqs",
        str(service_max_num_seqs), "--service-max-num-batched-tokens",
        str(service_max_num_batched_tokens),
    ]
    if source is not None:
        command.extend(
            [
                "--database-url", source.database_url,
                "--source-workload-name", source.workload_name,
                "--timed-postgres-source",
            ]
        )
    if arm.ray_address is not None:
        command.extend(["--ray-address", arm.ray_address])
    if arm.adapter == "duckdb_ai":
        # Reuse the frozen product-owned concurrency selected by the bounded-output
        # single-query contract.  This is not a harness semaphore: each independent
        # DuckDB connection still lets the community extension schedule its own
        # requests.
        command.extend(
            [
                "--duckdb-max-concurrent-requests",
                str(arm.concurrency_per_endpoint),
            ]
        )
    if api_key is not None:
        command.extend(["--api-key", api_key])
    audit_command(command)
    return command


def redact_command(command: Sequence[str]) -> list[str]:
    """Preserve reproducible arguments without persisting any known secret."""

    return redact_argument_list(list(command))


def audit_command(command: Sequence[str]) -> None:
    """Fail closed if a supposedly native command contains project control knobs."""

    joined = " ".join(command).lower()
    banned = sorted(token for token in _BANNED_COMMAND_TOKENS if token in joined)
    if banned:
        raise ValueError("native multi-job command contains prohibited control option(s): " + ", ".join(banned))


def _read_results(path: Path) -> tuple[BaselineRequestResult, ...]:
    with path.open(encoding="utf-8", newline="") as stream:
        rows = tuple(csv.DictReader(stream))
    return tuple(
        BaselineRequestResult(
            doc_id=int(row["doc_id"]), endpoint_index=int(row["endpoint_index"]),
            status=row["status"], error=row["error"] or None,
            submitted_at_s=float(row["submitted_at_s"]), started_at_s=float(row["started_at_s"]),
            completed_at_s=float(row["completed_at_s"]), input_tokens=int(row["input_tokens"]),
            output_tokens=int(row["output_tokens"]), output_text=row["output_text"] or None,
            finish_reason=row["finish_reason"] or None,
        )
        for row in rows
    )


def _validate_shard_provenance(summary_path: Path, arm: NativeMultiJobArm) -> dict[str, object]:
    """Reject a shard whose recorded identity differs from its configured adapter.

    Native framework arms must remain framework-owned even in a multi-job run;
    bounded HTTP is retained only as an explicitly labelled direct-client control.
    """

    if not summary_path.is_file():
        raise FileNotFoundError(f"successful shard omitted summary.json: {summary_path}")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if not isinstance(summary, dict):
        raise ValueError(f"shard summary must be an object: {summary_path}")
    expected = adapter_provenance(arm.adapter).summary_fields()
    required = (
        "adapter",
        "comparison_role",
        "scheduler_owner",
        "custom_scheduling_code",
        "formal_baseline_eligible",
        "source_kind",
        "source_timing_boundary",
        "source_read_s",
        "source_validation_status",
        "server_version",
        "pgvector_version",
    )
    provenance_fields = required[:5]
    mismatches = {
        field: {"expected": expected.get(field) if field != "adapter" else arm.adapter, "observed": summary.get(field)}
        for field in provenance_fields
        if summary.get(field) != (arm.adapter if field == "adapter" else expected.get(field))
    }
    if mismatches:
        raise ValueError(f"shard provenance mismatch for {arm.arm_id}: {mismatches}")
    if arm.adapter in _RAY_ADAPTERS | {"daft_native", "duckdb_ai"} and (
        summary["custom_scheduling_code"] is not False
        or summary["formal_baseline_eligible"] is not True
    ):
        raise ValueError(f"native framework shard contains non-native scheduling evidence: {summary_path}")
    source_expected = {
        "source_kind": "timed_postgres_manifest",
        "source_timing_boundary": "inside_job_barrier",
        "source_validation_status": "ok",
    }
    if any(summary.get(field) != value for field, value in source_expected.items()):
        raise ValueError(f"shard source timing mismatch for {arm.arm_id}: {summary_path}")
    if not isinstance(summary["source_read_s"], (int, float)) or summary["source_read_s"] < 0:
        raise ValueError(f"shard source_read_s is invalid: {summary_path}")
    DatabaseIdentity.from_record(summary, f"shard {summary_path}")
    return {field: summary[field] for field in required}


def seal_native_cell_artifact_paths(
    record: Mapping[str, object],
    cell_root: Path,
) -> None:
    """Seal Job manifests and rewrite raw Job artifact locators relatively.

    The returned in-memory cell record remains unchanged for immediate
    normalization. Only JSON evidence stored under ``cell_root`` is rewritten,
    so an archived native cell no longer depends on its original absolute path.
    """

    root = cell_root.resolve()
    jobs = record.get("jobs", [])
    if not isinstance(jobs, list):
        raise ValueError("native cell jobs must be a list before sealing")

    def relative_artifact(
        raw_path: object,
        evidence_name: str,
        *,
        allow_empty: bool = False,
    ) -> str:
        path = Path(str(raw_path)).resolve()
        try:
            relative = path.relative_to(root)
        except ValueError as error:
            raise ValueError(f"{evidence_name} escapes native cell root") from error
        if not path.is_file() or (not allow_empty and path.stat().st_size <= 0):
            raise ValueError(f"{evidence_name} is missing or empty")
        return relative.as_posix()

    inputs = root / "evidence" / "manifests"
    inputs.mkdir(parents=True, exist_ok=True)
    for position, job in enumerate(jobs):
        if not isinstance(job, dict):
            raise ValueError("native cell Job evidence must be an object")
        source_manifest = Path(str(job.get("manifest", ""))).resolve()
        manifest_sha256 = str(job.get("manifest_sha256", ""))
        if not source_manifest.is_file() or _sha256(source_manifest) != manifest_sha256:
            raise ValueError("native cell Job manifest identity drifted")
        sealed_manifest = inputs / f"job-{position}-{manifest_sha256[:12]}.jsonl"
        shutil.copyfile(source_manifest, sealed_manifest)
        if _sha256(sealed_manifest) != manifest_sha256:
            raise ValueError("sealed native Job manifest identity drifted")

        job_id = str(job.get("job_id", ""))
        summary_path = root / "jobs" / job_id / "job_summary.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if not isinstance(summary, dict):
            raise ValueError("native Job summary must be an object before sealing")
        summary["manifest"] = sealed_manifest.relative_to(root).as_posix()
        shards = summary.get("shards")
        if not isinstance(shards, list):
            raise ValueError("native Job shard evidence must be a list before sealing")
        for shard_index, shard in enumerate(shards):
            if not isinstance(shard, dict):
                raise ValueError("native Job shard evidence must be an object")
            for field in ("log", "summary", "requests"):
                shard[field] = relative_artifact(
                    shard.get(field, ""),
                    f"native Job {position} shard {shard_index} {field}",
                    allow_empty=field == "log",
                )
        _atomic_json(summary_path, summary)


def _repository_commit() -> str:
    """Return the immutable source revision recorded with the matrix evidence."""

    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parents[5],
    ).stdout.strip()


def _counter_delta(before: Mapping[int, Mapping[str, int]], after: Mapping[int, Mapping[str, int]]) -> dict[str, object]:
    if set(before) != set(after):
        raise ValueError("service counter endpoint set changed")
    delta: dict[int, dict[str, int]] = {}
    for index in before:
        if set(before[index]) != set(after[index]):
            raise ValueError("service counter keys changed")
        row = {name: after[index][name] - before[index][name] for name in before[index]}
        if any(value < 0 for value in row.values()):
            raise ValueError("service counter decreased")
        delta[index] = row
    if not any(value > 0 for row in delta.values() for value in row.values()):
        raise ValueError("service counters did not increase during native multi-job run")
    return {"before": before, "after": after, "delta": delta}


def _wait_for_processes(
    processes: Sequence[subprocess.Popen[object]],
    *,
    timeout_s: float,
) -> tuple[list[int], bool]:
    """Wait against one shared wall deadline and terminate every survivor.

    The per-request HTTP timeout is not a process-lifecycle bound: a client can
    retain a CLOSE_WAIT socket after vLLM has drained.  A shared deadline keeps
    one hung shard from serially consuming one timeout per endpoint.
    """

    deadline = time.monotonic() + timeout_s
    return_codes: list[int] = []
    timed_out = False
    try:
        for process in processes:
            remaining_s = deadline - time.monotonic()
            if remaining_s <= 0:
                raise subprocess.TimeoutExpired(process.args, timeout_s)
            return_codes.append(int(process.wait(timeout=remaining_s)))
    except subprocess.TimeoutExpired:
        timed_out = True
        return_codes = []
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                return_codes.append(int(process.wait(timeout=10.0)))
            except subprocess.TimeoutExpired:
                process.kill()
                return_codes.append(int(process.wait(timeout=10.0)))
    return return_codes, timed_out


def _run_job(
    *, target_epoch_s: float, arm: NativeMultiJobArm, job: NativeMultiJobJob,
    arm_root: Path, runner_script: str | Path, config: NativeMultiJobConfig,
    api_key: str | None, popen_factory: Callable[..., subprocess.Popen[object]] = subprocess.Popen,
    now: Callable[[], float] = time.time,
) -> dict[str, object]:
    """Wait for a job's absolute launch time, then start its two native shards."""

    _assert_immutable(job)
    job_root = arm_root / "jobs" / job.job_id
    job_root.mkdir(parents=True)
    while now() < target_epoch_s:
        time.sleep(min(0.02, target_epoch_s - now()))
    scheduled = target_epoch_s
    launched = now()
    commands = [
        build_shard_command(
            runner_script=runner_script, arm=arm, job=job, endpoint_index=index,
            endpoint_url=config.endpoint_urls[index], output_dir=job_root / f"shard_{index}",
            model=config.model, service_prefix_caching=config.service_prefix_caching,
            service_max_num_seqs=config.service_max_num_seqs,
            service_max_num_batched_tokens=config.service_max_num_batched_tokens,
            api_key=api_key, source=config.source,
        )
        for index in (0, 1)
    ]
    _atomic_json(job_root / "commands.json", [redact_command(command) for command in commands])
    logs = [
        (job_root / "shard_0.log").open("x", encoding="utf-8"),
        (job_root / "shard_1.log").open("x", encoding="utf-8"),
    ]
    try:
        processes = [
            popen_factory(command, stdout=log, stderr=subprocess.STDOUT, text=True)
            for command, log in zip(commands, logs)
        ]
        pids = [int(process.pid) for process in processes]
        return_codes, timed_out = _wait_for_processes(
            processes,
            timeout_s=arm.process_timeout_s,
        )
    finally:
        for log in logs:
            log.close()
    ended = now()
    record: dict[str, object] = {
        "job_id": job.job_id, "scheduled_launch_epoch_s": scheduled,
        "actual_launch_epoch_s": launched, "launch_lateness_s": launched - scheduled,
        "ended_epoch_s": ended, "job_barrier_jct_s": ended - launched,
        "manifest": str(job.manifest), "manifest_sha256": job.manifest_sha256,
        "endpoint_partition": job.endpoint_summary, "pids": pids,
        "return_codes": return_codes, "shards": [
            {
                "endpoint_index": index, "log": str(job_root / f"shard_{index}.log"),
                "summary": str(job_root / f"shard_{index}" / "summary.json"),
                "requests": str(job_root / f"shard_{index}" / "requests.csv"),
            }
            for index in (0, 1)
        ],
        "process_timeout_s": arm.process_timeout_s,
        "process_timed_out": timed_out,
    }
    if timed_out or return_codes != [0, 0]:
        record.update({
            "status": "failed",
            "failure_reason": (
                "shard_process_timeout" if timed_out else "nonzero_shard_exit"
            ),
        })
        _atomic_json(job_root / "job_summary.json", record)
        return record
    try:
        requests = read_manifest(job.manifest)
        provenance = [
            _validate_shard_provenance(job_root / f"shard_{index}" / "summary.json", arm)
            for index in (0, 1)
        ]
        results = tuple(
            result
            for index in (0, 1)
            for result in _read_results(job_root / f"shard_{index}" / "requests.csv")
        )
        validate_results(requests, results)
        input_tokens = sum(result.input_tokens for result in results)
        output_tokens = sum(result.output_tokens for result in results)
        record.update({
            "status": "passed", "exactly_once": True,
            "shard_provenance": provenance,
            "completed_count": len(results), "expected_count": len(requests),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens, "total_tokens": input_tokens + output_tokens,
            "job_barrier_tokens_per_s": (
                (input_tokens + output_tokens) / (ended - launched) if ended > launched else 0.0
            ),
        })
    except Exception as exc:
        record.update({
            "status": "failed",
            "failure_reason": redact_text(f"{type(exc).__name__}: {exc}"),
            "exactly_once": False,
        })
    _atomic_json(job_root / "job_summary.json", record)
    return record


def _initial_index(config: NativeMultiJobConfig) -> dict[str, object]:
    return {
        "schema_version": 1, "experiment_id": config.experiment_id, "status": "running",
        "comparison_admission": "pending", "warmup_repeats": config.warmup_repeats,
        "formal_repeats": config.formal_repeats, "schedule_seed": config.schedule_seed,
        "minimum_measurement_seconds": config.minimum_measurement_seconds,
        "resource_time_series": {
            "status": "collected_per_run",
            "gpu": "gpu_resource.csv",
            "vllm": "gauge_summary and vllm_latency_deltas in matrix_index.json",
        },
        "arms": [
            {"id": arm.arm_id, "adapter": arm.adapter, "job_count": len(arm.jobs)}
            for arm in config.arms
        ],
        "runs": [],
    }


def run_native_multijob_cell(
    config: NativeMultiJobConfig,
    arm: NativeMultiJobArm,
    identity: NativeRunIdentity,
    output_dir: Path,
    *,
    runner_script: str | Path,
    popen_factory: Callable[..., subprocess.Popen[object]] = subprocess.Popen,
    queue_waiter: Callable[[tuple[str, ...], float], Mapping[int, Mapping[str, int]]] = wait_for_idle,
    counter_sampler: Callable[[tuple[str, ...]], Mapping[int, Mapping[str, int]]] = sample_vllm_token_counters,
    now: Callable[[], float] = time.time,
    repository_commit: str,
    cell_instrumenter: Callable[..., object] = instrumented_cell,
) -> dict[str, object]:
    """Execute one native physical cell without scheduling or acquiring a lease."""

    if identity.phase not in {
        "gate", "warmup", "formal", "selector_sanity_development",
    }:
        raise ValueError("native cell phase is unsupported")
    if identity.repeat <= 0 or identity.order_index < 0:
        raise ValueError("native cell repeat/order_index must be valid")
    if not repository_commit:
        raise ValueError("repository_commit must be non-empty")
    output_dir.mkdir(parents=True)
    for job in arm.jobs:
        _assert_immutable(job)
    run_id = identity.run_id or (
        f"{identity.phase}_{identity.repeat:02d}_{identity.order_index:03d}_{arm.arm_id}"
    )
    record: dict[str, object] = {
        "run_id": run_id,
        "phase": identity.phase,
        "repeat": identity.repeat,
        "order_index": identity.order_index,
        "interleaved_position": identity.order_index,
        "arm_id": arm.arm_id,
        "adapter": arm.adapter,
        "job_count": len(arm.jobs),
        "output_root": str(output_dir),
        "repository_commit": repository_commit,
        "status": "running",
    }
    metrics_urls = tuple(_metrics_url(url) for url in config.endpoint_urls)
    api_key = os.environ.get(config.api_key_env) if config.api_key_env else None
    if config.api_key_env and not api_key:
        raise ValueError(f"api_key_env is not set: {config.api_key_env}")
    try:
        queue_before = queue_waiter(metrics_urls, config.idle_timeout_s)
        counters_before = counter_sampler(metrics_urls)
        gpu_trace = output_dir / "gpu_resource.csv"
        with cell_instrumenter(metrics_urls, gpu_trace) as instrumentation:  # type: ignore[attr-defined]
            t0 = now() + config.launch_lead_s
            record["t0_epoch_s"] = t0
            with ThreadPoolExecutor(max_workers=len(arm.jobs)) as pool:
                futures = [
                    pool.submit(
                        _run_job,
                        target_epoch_s=t0 + job.offset_s,
                        arm=arm,
                        job=job,
                        arm_root=output_dir,
                        runner_script=runner_script,
                        config=config,
                        api_key=api_key,
                        popen_factory=popen_factory,
                        now=now,
                    )
                    for job in arm.jobs
                ]
                jobs = [future.result() for future in futures]
            queue_after = queue_waiter(metrics_urls, config.idle_timeout_s)
        counters_after = counter_sampler(metrics_urls)
        service = _counter_delta(counters_before, counters_after)
        service_path = output_dir / "service_counters.json"
        _atomic_json(service_path, service)
        arm_barrier_jct_s = now() - t0
        record.update(
            {
                "jobs": jobs,
                "queue_before": queue_before,
                "queue_final": queue_after,
                "service_counters": str(service_path),
                "gpu_resource_trace": str(gpu_trace),
                "gpu_summary": instrumentation.gpu_summary,
                "gauge_summary": instrumentation.gauge_summary,
                "vllm_latency_deltas": instrumentation.ttft_deltas,
                "arm_barrier_jct_s": arm_barrier_jct_s,
                "status": "passed" if all(job["status"] == "passed" for job in jobs) else "failed",
                "exactly_once": all(job.get("exactly_once") is True for job in jobs),
            }
        )
        record["comparison_eligible"] = (
            identity.phase == "formal"
            and record["status"] == "passed"
            and arm_barrier_jct_s >= config.minimum_measurement_seconds
        )
        record["duration_status"] = (
            "gate_not_ranked"
            if identity.phase == "gate"
            else f"{identity.phase}_not_ranked"
            if identity.phase != "formal"
            else "passed"
            if record["comparison_eligible"]
            else "below_minimum_not_rankable"
        )
        total_tokens = sum(int(job.get("total_tokens", 0)) for job in jobs)
        record["group_barrier_tokens_per_s"] = (
            total_tokens / arm_barrier_jct_s if arm_barrier_jct_s > 0 else 0.0
        )
        if record["status"] != "passed":
            record["error"] = "RuntimeError: one or more native job shards failed"
    except Exception as exc:
        record.update({
            "status": "failed",
            "error": redact_text(f"{type(exc).__name__}: {exc}"),
        })
    return record


def run_native_multijob(
    config_path: str | Path, *, runner_script: str | Path,
    popen_factory: Callable[..., subprocess.Popen[object]] = subprocess.Popen,
    queue_waiter: Callable[[tuple[str, ...], float], Mapping[int, Mapping[str, int]]] = wait_for_idle,
    counter_sampler: Callable[[tuple[str, ...]], Mapping[int, Mapping[str, int]]] = sample_vllm_token_counters,
    now: Callable[[], float] = time.time,
    repository_commit_getter: Callable[[], str] = _repository_commit,
    host_lease_acquirer: Callable[..., object] = acquire_host_runner_lease,
    cell_instrumenter: Callable[..., object] = instrumented_cell,
    ray_nofile_probe: RayNofileProbe = probe_ray_worker_nofile,
    gate_only: bool = False,
) -> dict[str, object]:
    """Execute warmup/formal native arms, preserving failed evidence fail-closed."""

    config = load_native_multijob_config(config_path)
    api_key = os.environ.get(config.api_key_env) if config.api_key_env else None
    if config.api_key_env and not api_key:
        raise ValueError(f"api_key_env is not set: {config.api_key_env}")
    config.output_root.mkdir(parents=True)
    index_path = config.output_root / "matrix_index.json"
    index = _initial_index(config)
    index["execution_mode"] = "gate" if gate_only else "formal_matrix"
    repository_commit = repository_commit_getter()
    if not repository_commit:
        raise RuntimeError("repository commit must be non-empty")
    index["repository_commit"] = repository_commit
    _atomic_json(index_path, index)
    metrics_urls = tuple(_metrics_url(url) for url in config.endpoint_urls)
    ordinal = 0
    lease = host_lease_acquirer(
        config.output_root.parent,
        repository_commit=repository_commit,
    )
    try:
        try:
            index["ray_worker_nofile"] = validate_ray_worker_nofile(
                (
                    arm.ray_address
                    for arm in config.arms
                    if arm.ray_address is not None
                ),
                probe=ray_nofile_probe,
            )
        except Exception as exc:
            index.update(
                {
                    "status": "failed",
                    "comparison_admission": "not_rankable",
                    "runtime_preflight_error": redact_text(
                        f"{type(exc).__name__}: {exc}"
                    ),
                }
            )
            _atomic_json(index_path, index)
            raise
        _atomic_json(index_path, index)
        phases = (
            (("gate", 1),)
            if gate_only
            else (("warmup", config.warmup_repeats), ("formal", config.formal_repeats))
        )
        for phase, repeats in phases:
            for repeat in range(1, repeats + 1):
                scheduled = (
                    tuple(arm for arm in config.arms if len(arm.jobs) == 4)
                    if gate_only
                    else balanced_arm_order(config, phase, repeat)
                )
                for position, arm in enumerate(scheduled, start=1):
                    ordinal += 1
                    run_id = f"{ordinal:03d}_{phase}_{repeat:02d}_{arm.arm_id}"
                    arm_root = config.output_root / "runs" / run_id
                    record = {
                        "run_id": run_id, "phase": phase, "repeat": repeat,
                        "order_index": position, "interleaved_position": position,
                        "arm_id": arm.arm_id, "adapter": arm.adapter,
                        "job_count": len(arm.jobs), "output_root": str(arm_root),
                        "repository_commit": repository_commit, "status": "running",
                    }
                    index["runs"].append(record)  # type: ignore[index]
                    _atomic_json(index_path, index)
                    completed = run_native_multijob_cell(
                        config,
                        arm,
                        NativeRunIdentity(phase, repeat, position, run_id),
                        arm_root,
                        runner_script=runner_script,
                        popen_factory=popen_factory,
                        queue_waiter=queue_waiter,
                        counter_sampler=counter_sampler,
                        now=now,
                        repository_commit=repository_commit,
                        cell_instrumenter=cell_instrumenter,
                    )
                    record.update(completed)
                    if record["status"] != "passed":
                        index.update({"status": "failed", "comparison_admission": "not_rankable", "failed_run": run_id})
                        _atomic_json(index_path, index)
                        raise RuntimeError(str(record.get("error", "native cell failed")))
                    _atomic_json(index_path, index)
    except Exception:
        raise
    finally:
        lease.release()  # type: ignore[union-attr]
    formal = [item for item in index["runs"] if isinstance(item, dict) and item.get("phase") == "formal"]
    if gate_only:
        passed = bool(index["runs"]) and all(
            isinstance(item, dict) and item.get("status") == "passed"
            for item in index["runs"]
        )
        index.update({
            "status": "passed" if passed else "failed",
            "comparison_admission": "not_rankable",
            "gate_runs_total": len(index["runs"]),
        })
        _atomic_json(index_path, index)
        return index
    admissible = bool(formal) and all(
        item.get("comparison_eligible") is True for item in formal
    )
    index.update({
        "status": "passed" if admissible else "not_rankable",
        "comparison_admission": "admissible" if admissible else "not_rankable",
        "formal_runs_total": len(formal),
        "formal_runs_rankable": sum(
            item.get("comparison_eligible") is True for item in formal
        ),
    })
    _atomic_json(index_path, index)
    return index


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run native framework single/multi-job characterization.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--runner-script", required=True, help="Path to existing run_official_baseline.py")
    parser.add_argument("--gate-only", action="store_true", help="Run only four-job arms once as non-rankable gates")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_native_multijob(
            args.config, runner_script=args.runner_script, gate_only=args.gate_only
        )
    except Exception as exc:
        print(json.dumps({
            "status": "failed",
            "error": redact_text(f"{type(exc).__name__}: {exc}"),
        }))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" and (
        args.gate_only or result["comparison_admission"] == "admissible"
    ) else 2
