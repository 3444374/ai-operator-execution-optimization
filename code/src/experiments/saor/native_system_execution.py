"""Concrete executor adapters for the five-arm matched-system runner."""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from src.baselines.common.database_identity import consistent_database_versions
from src.baselines.common.redact import redact_text
from src.baselines.text.orchestration.native_multijob import (
    NativeRunIdentity,
    audit_command,
    load_native_multijob_config,
    run_native_multijob_cell,
    seal_native_cell_artifact_paths,
)
from src.experiments.saor.native_system_bindings import validate_executor_bindings
from src.experiments.saor.native_system_contract import MatchedArm, ScheduledMatchedCell
from src.experiments.saor.native_system_readiness import (
    audit_readiness,
)
from src.experiments.saor.native_system_matched import (
    load_matched_system_config,
    normalize_request_tail_status,
    run_matched_system,
)
from src.experiments.saor.native_system_completion import (
    build_completion_evidence,
    collect_completion_rows,
    expected_doc_ids_from_manifests,
)
from src.experiments.saor.native_system_observation import (
    JobObservationContract,
    build_system_observation,
    summarize_gateway_rows,
)
from src.experiments.shared_vllm import (
    GroupRunIdentity,
    RunnerOptions,
    load_config as load_project_config,
    run_shared_vllm_group_cell,
)
from src.experiments.shared_vllm.preflight import wait_for_idle
from src.observability.request_gateway import GatewayRoute, ObservationGateway


@dataclass(frozen=True)
class MatchedExecutionOptions:
    config: Path
    native_config: Path
    project_config: Path
    native_runner: Path
    profiler: Path
    python_executable: Path
    health_url: str
    metrics_urls: tuple[str, ...]
    ray_address: str
    idle_timeout_s: float
    start_delay_s: float
    rehearsal: bool
    correctness_smoke: bool
    correctness_smoke_root: Path | None
    vllm_python: Path
    runtime_identity_paths: tuple[Path, ...]
    installed_source_audit: Path
    system_preflight_evidence: Path
    correctness_smoke_evidence: Path | None
    formal_authorization: Path | None
    rehearsal_validation: Path | None
    rehearsal_root: Path | None
    rehearsal_archive: Path | None


def _require_native_cell_passed(record: dict[str, object]) -> None:
    """Raise the native runner's redacted primary failure before normalization."""

    if record.get("status") == "passed":
        return
    reason = redact_text(str(
        record.get("error") or "native cell execution did not pass"
    ))
    raise RuntimeError(f"native cell execution failed: {reason}")


def normalize_native_evidence(
    arm: MatchedArm, record: dict[str, object]
) -> dict[str, object]:
    _require_native_cell_passed(record)
    jobs = record["jobs"]
    if not isinstance(jobs, list):
        raise RuntimeError("native Job evidence must encode a list")
    provenance = [
        shard for job in jobs if isinstance(job, dict)
        for shard in job.get("shard_provenance", []) if isinstance(shard, dict)
    ]
    versions = consistent_database_versions(provenance, "native shard provenance")
    service_payload = json.loads(
        Path(str(record["service_counters"])).read_text(encoding="utf-8")
    )
    deltas = service_payload.get("delta", {})
    unavailable_tail = {
        "request_p50_s": "unavailable",
        "request_p95_s": "unavailable",
        "request_p99_status": "unavailable",
        "request_p99_s": "unavailable",
        "slo_status": "unavailable",
        "slo_violation_ratio": "unavailable",
        "tail_reason": "native framework API does not expose a common per-request clock",
    }
    jobs = [{**job, **unavailable_tail} for job in jobs]
    return {
        **record,
        **versions,
        "implementation_source": "official_native_single_cell_runner",
        "start_epoch_s": record["t0_epoch_s"],
        "end_epoch_s": float(record["t0_epoch_s"]) + float(record["arm_barrier_jct_s"]),
        "database_operator_e2e_s": record["arm_barrier_jct_s"],
        "service_metrics": {
            "metrics_status": "ok",
            "prompt_tokens_delta": sum(int(row.get("prompt_tokens", 0)) for row in deltas.values()),
            "generation_tokens_delta": sum(int(row.get("generation_tokens", 0)) for row in deltas.values()),
        },
        "resource_metrics": {
            "resource_metrics_status": "ok",
            "path": record["gpu_resource_trace"],
            "gpu_summary": record["gpu_summary"],
            "gauge_summary": record["gauge_summary"],
        },
        "request_tail_status": normalize_request_tail_status(arm.unsupported_request_tails),
        "service_fairness_metrics": {
            "starvation_status": "unavailable",
            "longest_no_service_s": "unavailable",
            "completion_service_lag_status": "unavailable",
            "completion_service_lag_p95_work": "unavailable",
            "completion_service_lag_max_work": "unavailable",
            "reason": "native framework API exposes completions but no completion-accounted service ledger",
        },
        "mfu_contract": arm.mfu_contract.__dict__,
        "output_paths": {
            "service_counters": record["service_counters"],
            "resources": record["gpu_resource_trace"],
        },
        "completion_evidence": record["completion_evidence"],
        "jobs": jobs,
    }


def _read_gateway_trace(path: Path) -> list[dict[str, object]]:
    """Read the cell-local passive gateway JSONL after it has stopped."""

    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if any(not isinstance(row, dict) for row in rows):
        raise RuntimeError("observation gateway trace rows must be objects")
    return rows


def _attach_common_observation(
    evidence: dict[str, object],
    *,
    trace_path: Path,
    routes: tuple[GatewayRoute, ...],
    contracts: tuple[JobObservationContract, ...],
) -> dict[str, object]:
    """Replace framework-specific pseudo tails with the common passive clock."""

    jobs = evidence.get("jobs")
    if not isinstance(jobs, list) or any(not isinstance(job, dict) for job in jobs):
        raise RuntimeError("executor Job evidence is invalid")
    gateway = summarize_gateway_rows(_read_gateway_trace(trace_path), contracts)
    gateway_jobs = gateway.get("jobs")
    if not isinstance(gateway_jobs, dict) or any(
        int(gateway_jobs.get(str(job["job_id"]), {}).get("request_count", -1))
        != int(job["expected_count"])
        for job in jobs
    ):
        raise RuntimeError(
            "observation gateway request count does not match exactly-once results"
        )
    service_metrics = evidence.get("service_metrics")
    if not isinstance(service_metrics, dict):
        raise RuntimeError("executor service metrics are invalid")
    gateway_prompt = sum(
        int(job["actual_prompt_tokens"])
        for job in gateway_jobs.values()
    )
    gateway_output = sum(
        int(job["actual_output_tokens"])
        for job in gateway_jobs.values()
    )
    if (
        int(service_metrics.get("prompt_tokens_delta", -1)) != gateway_prompt
        or int(service_metrics.get("generation_tokens_delta", -1)) != gateway_output
    ):
        raise RuntimeError(
            "gateway actual token usage does not reconcile with vLLM counters"
        )
    system = build_system_observation(
        gateway,
        t0_by_job={
            str(job["job_id"]): float(job["actual_launch_epoch_s"])
            for job in jobs
        },
        t1_by_job={
            str(job["job_id"]): float(job["first_batch_ready_epoch_s"])
            for job in jobs
        },
        t4_by_job={
            str(job["job_id"]): float(job["result_visible_epoch_s"])
            for job in jobs
        },
    )
    observed_jobs = system["jobs"]
    assert isinstance(observed_jobs, dict)
    for job in jobs:
        observed = observed_jobs[str(job["job_id"])]
        assert isinstance(observed, dict)
        job.update(observed)
        job.update(
            {
                "request_p99_status": "available",
                "slo_status": "available",
                "tail_reason": "common observation-only gateway",
                "actual_work": int(observed["actual_total_tokens"]),
            }
        )
    request_count = sum(int(job["request_count"]) for job in observed_jobs.values())
    slo_misses = sum(
        float(job["request_slo_violation_ratio"]) * int(job["request_count"])
        for job in observed_jobs.values()
    )
    request_tail_status = {
        "request_p99": {
            "status": "available",
            "value": max(float(job["request_p99_s"]) for job in observed_jobs.values()),
            "reason": "common observation-only gateway",
        },
        "slo": {
            "status": "available",
            "value": slo_misses / request_count,
            "reason": "common observation-only gateway",
        },
    }
    service_fairness = system.get("service_fairness")
    if not isinstance(service_fairness, dict):
        raise RuntimeError("common service fairness evidence is missing")
    fairness_status = str(service_fairness.get("status", "unavailable"))
    normalized_fairness = {
        "starvation_status": fairness_status,
        "longest_no_service_s": service_fairness.get(
            "longest_no_service_s", "unavailable"
        ),
        "completion_service_lag_status": fairness_status,
        "completion_service_lag_p95_work": service_fairness.get(
            "completion_service_lag_p95_work", "unavailable"
        ),
        "completion_service_lag_max_work": service_fairness.get(
            "completion_service_lag_max_work", "unavailable"
        ),
        "reason": "common gateway-observed actual completed token work",
    }
    trace_sha256 = hashlib.sha256(trace_path.read_bytes()).hexdigest()
    output_paths = evidence.get("output_paths")
    if not isinstance(output_paths, dict):
        raise RuntimeError("executor output paths are invalid")
    output_paths["observation_gateway_trace"] = str(trace_path)
    evidence.update(
        {
            "start_epoch_s": min(
                float(job["t0_job_release_epoch_s"])
                for job in observed_jobs.values()
            ),
            "end_epoch_s": max(
                float(job["t4_result_visible_epoch_s"])
                for job in observed_jobs.values()
            ),
            "database_operator_e2e_s": system["group_jct_s"],
            "correct_throughput_tokens_per_s": system[
                "correct_throughput_tokens_per_s"
            ],
            "request_tail_status": request_tail_status,
            "service_fairness_metrics": normalized_fairness,
            "system_observation": system,
            "observation_gateway": {
                "status": "passed",
                "mode": "pass_through_no_queue_no_retry",
                "trace_path": str(trace_path),
                "trace_sha256": trace_sha256,
                "routes": [route.__dict__ for route in routes],
                "integrity": gateway["gateway_integrity"],
            },
        }
    )
    return evidence


def project_job_epoch_fields(
    record: dict[str, object], job_index: int
) -> dict[str, float]:
    """Separate Project launcher, child-source, submit, and completion clocks."""

    sources = {
        "scheduled_launch_epoch_s": "replay_configured_start_epoch_s",
        "actual_launch_epoch_s": "replay_observed_start_epoch_s",
        "source_arrival_epoch_s": "job_arrival_start_epoch_s",
        "first_submit_epoch_s": "replay_actual_submit_start_epoch_s",
        "ended_epoch_s": "job_completion_end_epoch_s",
    }
    vectors: dict[str, list[object]] = {}
    for output_field, record_field in sources.items():
        decoded = json.loads(str(record[record_field]))
        if not isinstance(decoded, list) or any(
            not isinstance(value, (int, float)) or isinstance(value, bool)
            for value in decoded
        ):
            raise RuntimeError(f"Project {record_field} must encode numeric epochs")
        vectors[output_field] = decoded
    lengths = {len(vector) for vector in vectors.values()}
    if len(lengths) != 1 or not 0 <= job_index < next(iter(lengths)):
        raise RuntimeError("Project Job epoch vectors are misaligned")
    return {
        field: float(vector[job_index])
        for field, vector in vectors.items()
    }


def normalize_project_evidence(
    arm: MatchedArm,
    record: dict[str, object],
    output_dir: Path,
) -> dict[str, object]:
    configured_raw = json.loads(str(record["replay_configured_start_epoch_s"]))
    if not isinstance(configured_raw, list) or not configured_raw:
        raise RuntimeError("Project configured Job epochs must encode a list")
    epochs = [
        project_job_epoch_fields(record, index)
        for index in range(len(configured_raw))
    ]
    configured = [item["scheduled_launch_epoch_s"] for item in epochs]
    observed = [item["source_arrival_epoch_s"] for item in epochs]
    actual_work = json.loads(str(record["job_actual_work"]))
    expected_counts = json.loads(str(record["job_expected_count"]))
    completed_counts = json.loads(str(record["job_completed_count"]))
    exactly_once = json.loads(str(record["job_exactly_once"]))
    job_p99 = json.loads(str(record["job_p99_s"]))
    job_slo = json.loads(str(record["job_slo_violation_ratio"]))
    shared_credit = json.loads(str(record.get("shared_credit_final", "[]")))
    if not isinstance(shared_credit, list):
        raise RuntimeError("Project shared_credit_final must encode a list")
    for snapshot in shared_credit:
        if not isinstance(snapshot, dict):
            raise RuntimeError("Project shared_credit_final snapshot must be an object")
        for field in (
            "active_by_job", "active_work_by_job", "waiting_by_job",
            "waiting_work_by_job", "waiting_head_work_by_job",
        ):
            if field in snapshot and isinstance(snapshot[field], str):
                snapshot[field] = json.loads(snapshot[field])
            if field in snapshot and not isinstance(snapshot[field], (list, dict)):
                raise RuntimeError(f"Project shared_credit_final {field} must encode a container")
    summaries: list[dict[str, str]] = []
    for path in sorted((output_dir / "jobs").glob("*.runs.csv")):
        with path.open(encoding="utf-8", newline="") as stream:
            rows = list(csv.DictReader(stream))
        if len(rows) != 1:
            raise RuntimeError(f"Project Job summary is not unique: {path}")
        summaries.append(rows[0])
    if len(summaries) != len(observed):
        raise RuntimeError("Project Job summary evidence is incomplete")
    versions = consistent_database_versions(summaries, "Project Job summary")
    observed_shas = [str(row.get("request_manifest_sha256", "") or "") for row in summaries]
    for index, observed_sha in enumerate(observed_shas):
        if observed_sha != arm.job_manifests[index].sha256:
            raise RuntimeError(f"Project Job {index} observed manifest SHA-256 drifted")
    lifecycle_minima: list[tuple[float, float]] = []
    for path in sorted((output_dir / "jobs").glob("*.submissions.csv")):
        with path.open(encoding="utf-8", newline="") as stream:
            rows = [row for row in csv.DictReader(stream) if row.get("ready_epoch_s") and row.get("credit_registered_epoch_s")]
        if rows:
            lifecycle_minima.append((
                min(float(row["ready_epoch_s"]) for row in rows),
                min(float(row["credit_registered_epoch_s"]) for row in rows),
            ))
    is_saor = arm.arm_id == "project_bounded_ready_saor_0125we"
    if is_saor and len(lifecycle_minima) != len(observed):
        raise RuntimeError("SAOR release-gated ready lifecycle evidence is incomplete")
    if [round(float(value) - float(configured[0]), 6) for value in configured] != [0.0, 5.0]:
        raise RuntimeError("Project Job release schedule drifted from [0, 5]")
    jobs = [{
        "job_id": arm.job_manifests[index].job_id,
        "manifest_sha256": observed_shas[index],
        "scheduled_launch_epoch_s": epochs[index]["scheduled_launch_epoch_s"],
        "actual_launch_epoch_s": epochs[index]["actual_launch_epoch_s"],
        "source_arrival_epoch_s": epochs[index]["source_arrival_epoch_s"],
        "first_batch_ready_epoch_s": float(
            summaries[index]["first_batch_ready_epoch_s"]
        ),
        **({
            "concrete_ready_epoch_s": lifecycle_minima[index][0],
            "credit_registered_epoch_s": lifecycle_minima[index][1],
            "first_submit_epoch_s": epochs[index]["first_submit_epoch_s"],
        } if is_saor else {}),
        "ended_epoch_s": epochs[index]["ended_epoch_s"],
        "result_visible_epoch_s": float(
            summaries[index]["result_visible_epoch_s"]
        ),
        "completed_count": int(completed_counts[index]),
        "expected_count": int(expected_counts[index]),
        "actual_work": int(actual_work[index]),
        "request_p50_s": float(summaries[index]["request_e2e_s_p50"]),
        "request_p95_s": float(summaries[index]["request_e2e_s_p95"]),
        "request_p99_status": "available",
        "request_p99_s": float(job_p99[index]),
        "slo_status": "available",
        "slo_violation_ratio": float(job_slo[index]),
        "tail_reason": "",
        "exactly_once": bool(exactly_once[index]),
        "shard_provenance": [{
            "source_kind": dict(arm.source)["kind"],
            "source_timing_boundary": dict(arm.source)["timing_boundary"],
            "source_validation_status": "ok" if summaries[index].get("request_manifest_validation_status") == "ok" and float(summaries[index].get("db_fetch_s", "-1")) >= 0 else "failed",
            "source_read_s": float(summaries[index]["db_fetch_s"]),
        }],
    } for index in range(len(observed))]
    command_files = list((output_dir / "traces").glob("*.commands.json"))
    resource_files = list((output_dir / "traces").glob("*.resources.csv"))
    completion_files = sorted(
        (output_dir / "jobs").glob("*.completions.csv")
    )
    if (
        len(command_files) != 1
        or len(resource_files) != 1
        or len(completion_files) != len(observed)
    ):
        raise RuntimeError(
            "Project command/resource/completion evidence is incomplete"
        )
    command_evidence = json.loads(command_files[0].read_text(encoding="utf-8"))
    return {
        **record,
        **versions,
        "shared_credit_final": json.dumps(shared_credit, sort_keys=True),
        "command": [token for command in command_evidence.get("commands", []) for token in command],
        "implementation_source": "project_shared_vllm_single_cell_runner",
        "database_operator_e2e_s": float(record["end_epoch_s"]) - float(record["start_epoch_s"]),
        "jobs": jobs,
        "service_metrics": {
            "metrics_status": record["metrics_status"],
            "prompt_tokens_delta": record["prompt_tokens_delta"],
            "generation_tokens_delta": record["generation_tokens_delta"],
        },
        "resource_metrics": {"resource_metrics_status": record["resource_metrics_status"], "path": str(resource_files[0])},
        "exactly_once": all(bool(value) for value in exactly_once),
        "request_tail_status": normalize_request_tail_status(arm.unsupported_request_tails),
        "service_fairness_metrics": {
            "starvation_status": str(record.get("completion_service_lag_status", "unavailable")),
            "longest_no_service_s": record.get("completion_longest_no_service_s", "unavailable"),
            "completion_service_lag_status": str(record.get("completion_service_lag_status", "unavailable")),
            "completion_service_lag_p95_work": record.get("completion_service_lag_p95_work", "unavailable"),
            "completion_service_lag_max_work": record.get("completion_service_lag_max_work", "unavailable"),
            "reason": "" if record.get("completion_service_lag_status") not in (None, "", "unavailable") else "completion-accounted ledger unavailable",
        },
        "mfu_contract": arm.mfu_contract.__dict__,
        "output_paths": {
            "commands": str(command_files[0]),
            "resources": str(resource_files[0]),
            **{
                f"completion_evidence_job{index}": str(path)
                for index, path in enumerate(completion_files)
            },
        },
        "completion_evidence": record["completion_evidence"],
        "status": "passed",
    }


def execute_matched_system(options: MatchedExecutionOptions) -> dict[str, object]:
    if Path(sys.executable).absolute() != options.python_executable.absolute():
        raise RuntimeError(
            "outer five-arm runner must be invoked by the declared DRIVER_PYTHON"
        )
    native = load_native_multijob_config(options.native_config)
    native_provenance = {
        arm_id: dict(fields)
        for arm_id, fields in native.native_implementation_provenance
    }
    project = load_project_config(options.project_config)
    matched = load_matched_system_config(options.config)
    observation_contracts = tuple(
        JobObservationContract(
            job_id=item.job_id,
            role=item.role,
            weight=item.weight,
            request_slo_s=item.request_slo_s,
            job_jct_slo_s=item.job_jct_slo_s,
        )
        for item in matched.job_observation_contracts
    )
    validate_executor_bindings(
        matched, native, project,
        matched_config_path=options.config,
        project_config_path=options.project_config,
        runner_metrics_urls=options.metrics_urls,
        runner_health_url=options.health_url,
    )
    readiness = audit_readiness(
        options.config,
        options.native_config,
        options.project_config,
        live_service=True,
        installed_source_audit=options.installed_source_audit,
        vllm_python=options.vllm_python,
        runtime_identity_paths=options.runtime_identity_paths,
        system_preflight_evidence=options.system_preflight_evidence,
        correctness_smoke_evidence=(
            None if options.correctness_smoke else options.correctness_smoke_evidence
        ),
    )
    if options.correctness_smoke:
        if readiness.get("status") != "system_preflight_passed":
            raise RuntimeError(
                "static, service, and system stages must pass before correctness smoke"
            )
    elif readiness.get("rehearsal_ready") is not True:
        raise RuntimeError("all four readiness stages must pass before matrix execution")
    service_identity_preflight = readiness
    vllm_runtime = readiness["service_identity"]["installed_source"]["python_runtime"]
    if str(Path(sys.prefix).absolute()) == str(Path(vllm_runtime["sys_prefix"]).absolute()):
        raise RuntimeError(
            "DRIVER_PYTHON and VLLM_PYTHON must use isolated Python environments"
        )
    repository = Path(__file__).resolve().parents[4]
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True,
        text=True, cwd=repository,
    ).stdout.strip()
    native_by_id = {item.arm_id: item for item in native.arms}
    project_by_id = {item.scenario_id: item for item in project.scenarios}

    def observed_routes(arm: MatchedArm) -> tuple[GatewayRoute, ...]:
        """Bind every frozen Job/endpoint identity to the direct backend once."""

        return tuple(
            GatewayRoute(
                job_id=job.job_id,
                endpoint_id=endpoint_id,
                upstream_url=matched.endpoint_urls[index],
            )
            for job in arm.job_manifests
            for index, endpoint_id in enumerate(arm.endpoint_ids)
        )

    def completion_evidence(
        arm: MatchedArm, output_dir: Path, producer: str
    ) -> dict[str, object]:
        """Validate one cell's trace identities outside the measured boundary."""

        expected = expected_doc_ids_from_manifests(
            Path(job.path) for job in arm.job_manifests
        )
        return build_completion_evidence(
            collect_completion_rows(output_dir),
            expected_doc_ids=expected,
            producer=producer,
        )

    def native_executor(arm: MatchedArm, cell: ScheduledMatchedCell, output_dir: Path):
        routes = observed_routes(arm)
        trace_path = output_dir / "observation_gateway.jsonl"
        with ObservationGateway(
            routes=routes,
            trace_path=trace_path,
            request_timeout_s=matched.observation_gateway_request_timeout_s,
        ) as gateway:
            endpoint_urls_by_job = {
                job.job_id: gateway.urls_for_job(job.job_id, arm.endpoint_ids)
                for job in arm.job_manifests
            }
            record = run_native_multijob_cell(
                native, native_by_id[arm.arm_id],
                NativeRunIdentity(cell.phase, cell.repeat, cell.order_index),
                output_dir, runner_script=options.native_runner,
                repository_commit=commit,
                endpoint_urls_by_job=endpoint_urls_by_job,
            )
        _require_native_cell_passed(record)
        record["completion_evidence"] = completion_evidence(
            arm, output_dir, "native_official_adapter",
        )
        seal_native_cell_artifact_paths(record, output_dir)
        commands = [command for job in record.get("jobs", []) for command in json.loads((output_dir / "jobs" / job["job_id"] / "commands.json").read_text())]
        for command in commands:
            audit_command(command)
        normalized = normalize_native_evidence(arm, record)
        normalized["native_implementation_provenance"] = dict(
            native_provenance[arm.arm_id]
        )
        normalized["command"] = [token for command in commands for token in command]
        return _attach_common_observation(
            normalized,
            trace_path=trace_path,
            routes=routes,
            contracts=observation_contracts,
        )

    def project_executor(arm: MatchedArm, cell: ScheduledMatchedCell, output_dir: Path):
        routes = observed_routes(arm)
        trace_path = output_dir / "observation_gateway.jsonl"
        with ObservationGateway(
            routes=routes,
            trace_path=trace_path,
            request_timeout_s=matched.observation_gateway_request_timeout_s,
        ) as gateway:
            routed = tuple(
                gateway.urls_for_job(job.job_id, arm.endpoint_ids)
                for job in arm.job_manifests
            )
            runner = RunnerOptions(
                config_path=options.project_config, profiler_path=options.profiler,
                python_executable=options.python_executable, output_dir=output_dir,
                health_url=options.health_url, metrics_urls=options.metrics_urls,
                ray_address=options.ray_address, idle_timeout_s=options.idle_timeout_s,
                start_delay_s=options.start_delay_s,
                rehearsal=options.rehearsal or options.correctness_smoke,
                observation_endpoint_urls_by_job=routed,
            )
            for child in ("jobs", "logs", "traces", "records"):
                (output_dir / child).mkdir(parents=True, exist_ok=True)
            record = run_shared_vllm_group_cell(
                runner, project, project_by_id[arm.arm_id],
                GroupRunIdentity(cell.phase, cell.repeat, cell.order_index),
            )
        record["completion_evidence"] = completion_evidence(
            arm, output_dir, "project_profiler",
        )
        return _attach_common_observation(
            normalize_project_evidence(arm, record, output_dir),
            trace_path=trace_path,
            routes=routes,
            contracts=observation_contracts,
        )

    return run_matched_system(
        options.config, native_executor=native_executor,
        project_executor=project_executor,
        idle_gate=lambda _position: wait_for_idle(
            options.health_url, options.metrics_urls, options.idle_timeout_s
        ),
        instrumenter=lambda *_args: None,
        repository_commit_getter=lambda: commit,
        rehearsal=options.rehearsal,
        correctness_smoke=options.correctness_smoke,
        matrix_output_root_override=options.correctness_smoke_root,
        formal_authorization_path=options.formal_authorization,
        rehearsal_validation_path=options.rehearsal_validation,
        rehearsal_root=options.rehearsal_root,
        rehearsal_archive=options.rehearsal_archive,
        service_identity_preflight=service_identity_preflight,
        native_config_path=options.native_config,
        project_config_path=options.project_config,
        native_implementation_provenance=native_provenance,
    )
