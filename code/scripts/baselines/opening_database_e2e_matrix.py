#!/usr/bin/env python3
"""Run the two frozen opening database-E2E three-arm text comparisons.

The measured wall is identical for the two in-process arms:
PostgreSQL scan -> frozen-manifest validation -> two pinned endpoint shards ->
unified PostgreSQL sink.  The project arm invokes the real profiler with its
opt-in clean database-E2E boundary and the same manifest/sink contract.

This driver deliberately exposes only three static arms.  It contains no
adaptive controller, no additional baseline, and no parameter scan.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.cell_instrumentation import instrumented_cell  # noqa: E402
from src.baselines.common.contracts import BaselineRequestResult, ChatRequest  # noqa: E402
from src.baselines.common.manifests import (  # noqa: E402
    read_manifest,
    read_manifest_metadata,
)
from src.baselines.text.orchestration.gate_runner import wait_for_idle  # noqa: E402
from src.baselines.text.orchestration.postgres_manifest import source_row_hash  # noqa: E402
from src.baselines.text.products.direct_client import (  # noqa: E402
    DirectClientConfig,
    run_direct_client,
)
from src.baselines.text.products.duckdb_ai import (  # noqa: E402
    DuckDBAiConfig,
    inspect_duckdb_ai_runtime,
    run_duckdb_ai_complete,
)
from src.baselines.text.products.project_static import (  # noqa: E402
    ProjectStaticConfig,
    run_project_static,
)
from src.data.sinks.postgres import write_completions  # noqa: E402
from src.infrastructure.config_env import expand_structure  # noqa: E402
from src.infrastructure.runner_lease import acquire_host_runner_lease  # noqa: E402
from src.infrastructure.vllm_preflight import verify_live_vllm_config  # noqa: E402
from src.observability.metrics import estimate_mfu, squad_quality_metrics  # noqa: E402


ARMS = (
    "direct_static_sharded",
    "duckdb_ai_static_sharded",
    "project_frozen_static",
)


@dataclass(frozen=True)
class Workload:
    label: str
    name: str
    rows: int
    max_tokens: int
    manifest: Path
    quality: str


@dataclass(frozen=True)
class Config:
    experiment_id: str
    database_url: str
    endpoint_urls: tuple[str, str]
    model: str
    tokenizer: str
    output_root: Path
    project_python: str
    workloads: tuple[Workload, ...]
    concurrency_per_endpoint: int = 32
    timeout_s: float = 180.0
    write_batch_rows: int = 500
    token_budget: int = 6144
    active_work_per_endpoint: int = 65536
    actor_workers_per_endpoint: int = 8
    actor_concurrency: int = 4
    service_prefix_caching: str = "enabled"
    service_max_num_seqs: int = 256
    service_max_num_batched_tokens: int = 8192
    gpu_peak_tflops_each: float = 165.0
    service_conditioning_before_cell: bool = True
    seed: int = 20260807


def _load_config(path: Path) -> Config:
    payload = expand_structure(
        json.loads(path.read_text(encoding="utf-8")),
        "opening_database_e2e_config",
    )
    endpoints = tuple(payload["endpoint_urls"])
    if len(endpoints) != 2 or any(
        not value.endswith("/v1/chat/completions") for value in endpoints
    ):
        raise ValueError("exactly two /v1/chat/completions endpoints are required")
    workloads = tuple(
        Workload(
            label=str(row["label"]),
            name=str(row["name"]),
            rows=int(row["rows"]),
            max_tokens=int(row["max_tokens"]),
            manifest=Path(row["manifest"]),
            quality=str(row["quality"]),
        )
        for row in payload["workloads"]
    )
    if {item.quality for item in workloads} - {"squad", "completion_validity"}:
        raise ValueError("quality must be squad or completion_validity")
    if any(not item.manifest.is_file() for item in workloads):
        missing = [str(item.manifest) for item in workloads if not item.manifest.is_file()]
        raise FileNotFoundError(f"missing manifest(s): {missing}")
    config = Config(
        experiment_id=str(payload["experiment_id"]),
        database_url=str(payload["database_url"]),
        endpoint_urls=endpoints,  # type: ignore[arg-type]
        model=str(payload["model"]),
        tokenizer=str(payload["tokenizer"]),
        output_root=Path(payload["output_root"]),
        project_python=str(payload["project_python"]),
        workloads=workloads,
        concurrency_per_endpoint=int(payload.get("concurrency_per_endpoint", 32)),
        timeout_s=float(payload.get("timeout_s", 180.0)),
        write_batch_rows=int(payload.get("write_batch_rows", 500)),
        token_budget=int(payload.get("token_budget", 6144)),
        active_work_per_endpoint=int(payload.get("active_work_per_endpoint", 65536)),
        actor_workers_per_endpoint=int(payload.get("actor_workers_per_endpoint", 8)),
        actor_concurrency=int(payload.get("actor_concurrency", 4)),
        service_prefix_caching=str(payload.get("service_prefix_caching", "enabled")),
        service_max_num_seqs=int(payload.get("service_max_num_seqs", 256)),
        service_max_num_batched_tokens=int(
            payload.get("service_max_num_batched_tokens", 8192)
        ),
        gpu_peak_tflops_each=float(payload.get("gpu_peak_tflops_each", 165.0)),
        service_conditioning_before_cell=bool(
            payload.get("service_conditioning_before_cell", True)
        ),
        seed=int(payload.get("seed", 20260807)),
    )
    if config.output_root.exists():
        raise FileExistsError(f"output_root already exists: {config.output_root}")
    if config.concurrency_per_endpoint != 32:
        raise ValueError("opening contract freezes concurrency_per_endpoint=32")
    if (
        config.token_budget,
        config.active_work_per_endpoint,
        config.actor_workers_per_endpoint,
        config.actor_concurrency,
    ) != (6144, 65536, 8, 4):
        raise ValueError("project frozen-static contract must remain 6144/65536/8x4")
    for workload in workloads:
        manifest = read_manifest(workload.manifest)
        if len(manifest) != workload.rows:
            raise ValueError(f"{workload.label} manifest row count mismatch")
        if {row.endpoint_index for row in manifest} != {0, 1}:
            raise ValueError(f"{workload.label} manifest must use both endpoints")
        if {row.max_output_tokens for row in manifest} != {workload.max_tokens}:
            raise ValueError(f"{workload.label} manifest output cap mismatch")
        metadata = read_manifest_metadata(workload.manifest)
        if not metadata or metadata.get("partition_policy") != "equal_rows":
            raise ValueError(f"{workload.label} requires equal_rows metadata")
        if int(metadata.get("partition_seed", -1)) != config.seed:
            raise ValueError(f"{workload.label} partition seed mismatch")
    return config


def _metrics_urls(endpoint_urls: tuple[str, str]) -> tuple[str, str]:
    return tuple(url.split("/v1/", 1)[0] + "/metrics" for url in endpoint_urls)  # type: ignore[return-value]


def _endpoint_base(url: str) -> str:
    return url.rsplit("/chat/completions", 1)[0]


def _manifest_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _scan_source(
    conn,
    workload: Workload,
    manifest: tuple[ChatRequest, ...],
) -> tuple[tuple[ChatRequest, ...], dict[int, tuple[int, str]], dict[int, tuple[str, list[str]]]]:
    by_doc = {row.doc_id: row for row in manifest}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT doc_id, text, arrival_time_s, prompt_tokens,
                   target_output_tokens, tenant_id, category,
                   source_example_id, reference_answers
            FROM documents
            WHERE workload_name = %s
            ORDER BY doc_id
            """,
            (workload.name,),
        )
        rows = cur.fetchall()
    if len(rows) != workload.rows:
        raise ValueError(
            f"database row count mismatch for {workload.name}: "
            f"{len(rows)} != {workload.rows}"
        )
    scanned: list[ChatRequest] = []
    sidecar: dict[int, tuple[int, str]] = {}
    scoring: dict[int, tuple[str, list[str]]] = {}
    for raw in rows:
        (
            doc_id_raw,
            prompt_raw,
            arrival_raw,
            prompt_tokens_raw,
            target_output_raw,
            tenant_raw,
            category_raw,
            source_id_raw,
            references_raw,
        ) = raw
        doc_id = int(doc_id_raw)
        frozen = by_doc.get(doc_id)
        if frozen is None:
            raise ValueError(f"database doc_id {doc_id} is absent from manifest")
        prompt = str(prompt_raw)
        arrival = float(arrival_raw or 0.0)
        prompt_tokens = int(prompt_tokens_raw)
        target_output = int(target_output_raw)
        observed = ChatRequest(
            doc_id=doc_id,
            prompt=prompt,
            arrival_time_s=arrival,
            prompt_tokens=prompt_tokens,
            max_output_tokens=workload.max_tokens,
            estimated_output_tokens=workload.max_tokens,
            source_row_hash=source_row_hash(
                workload_name=workload.name,
                doc_id=doc_id,
                prompt=prompt,
                arrival_time_s=arrival,
                prompt_tokens=prompt_tokens,
                target_output_tokens=target_output,
            ),
            endpoint_index=frozen.endpoint_index,
        )
        if observed != frozen:
            raise ValueError(f"database/manifest semantic mismatch for doc_id={doc_id}")
        references = references_raw
        if isinstance(references, str):
            references = json.loads(references)
        sidecar[doc_id] = (int(tenant_raw), str(category_raw))
        scoring[doc_id] = (
            str(source_id_raw or doc_id),
            list(references or []),
        )
        scanned.append(observed)
    return tuple(scanned), sidecar, scoring


def _clean_sink(conn, doc_ids: Iterable[int]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM document_completions WHERE doc_id = ANY(%s)",
            (list(doc_ids),),
        )
    conn.commit()


def _sink_payload(
    results: Iterable[BaselineRequestResult],
    sidecar: Mapping[int, tuple[int, str]],
) -> list[dict]:
    rows = list(results)
    return [
        {
            "doc_id": [row.doc_id for row in rows],
            "tenant_id": [sidecar[row.doc_id][0] for row in rows],
            "category": [sidecar[row.doc_id][1] for row in rows],
            "output_text": [row.output_text or "" for row in rows],
        }
    ]


def _pairs_digest(pairs: Iterable[tuple[int, str]]) -> str:
    canonical = sorted((int(doc_id), str(text)) for doc_id, text in pairs)
    return hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _sink_readback(conn, results: tuple[BaselineRequestResult, ...]) -> dict:
    expected = [(row.doc_id, row.output_text or "") for row in results]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT doc_id, completion_text FROM document_completions "
            "WHERE doc_id = ANY(%s) ORDER BY doc_id",
            ([row.doc_id for row in results],),
        )
        observed = [(int(doc_id), str(text)) for doc_id, text in cur.fetchall()]
    expected_digest = _pairs_digest(expected)
    observed_digest = _pairs_digest(observed)
    return {
        "expected_rows": len(expected),
        "observed_rows": len(observed),
        "expected_digest": expected_digest,
        "observed_digest": observed_digest,
        "matched": len(expected) == len(observed) and expected_digest == observed_digest,
    }


def _run_shards(
    arm: str,
    requests: tuple[ChatRequest, ...],
    config: Config,
    workload: Workload,
) -> tuple[BaselineRequestResult, ...]:
    shards = tuple(
        tuple(row for row in requests if row.endpoint_index == endpoint_index)
        for endpoint_index in (0, 1)
    )

    def one(endpoint_index: int) -> tuple[BaselineRequestResult, ...]:
        endpoint = config.endpoint_urls[endpoint_index]
        if arm == "direct_static_sharded":
            return run_direct_client(
                shards[endpoint_index],
                DirectClientConfig(
                    endpoint_url=endpoint,
                    model=config.model,
                    max_tokens=workload.max_tokens,
                    max_concurrent_requests=config.concurrency_per_endpoint,
                    timeout_s=config.timeout_s,
                ),
            )
        if arm == "duckdb_ai_static_sharded":
            return run_duckdb_ai_complete(
                shards[endpoint_index],
                DuckDBAiConfig(
                    endpoint_base_url=_endpoint_base(endpoint),
                    model=config.model,
                    api_key="EMPTY",
                    max_tokens=workload.max_tokens,
                    max_concurrent_requests=config.concurrency_per_endpoint,
                    timeout_seconds=int(config.timeout_s),
                ),
            )
        raise ValueError(f"unsupported in-process arm: {arm}")

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(one, endpoint_index) for endpoint_index in (0, 1)]
        results = tuple(row for future in futures for row in future.result())
    return tuple(sorted(results, key=lambda row: row.doc_id))


def _service_condition(
    requests: tuple[ChatRequest, ...], config: Config, workload: Workload
) -> None:
    if not config.service_conditioning_before_cell:
        return
    _run_shards("direct_static_sharded", requests, config, workload)
    wait_for_idle(_metrics_urls(config.endpoint_urls), 180.0)


def _write_requests(path: Path, results: tuple[BaselineRequestResult, ...]) -> None:
    fields = [
        "doc_id", "endpoint_index", "status", "error", "submitted_at_s",
        "started_at_s", "completed_at_s", "input_tokens", "output_tokens",
        "output_text", "finish_reason",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in results:
            writer.writerow(
                {
                    field: (
                        "" if getattr(row, field) is None else getattr(row, field)
                    )
                    for field in fields
                }
            )


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def _quality(
    workload: Workload,
    results: tuple[BaselineRequestResult, ...],
    scoring: Mapping[int, tuple[str, list[str]]],
) -> tuple[dict, int]:
    successful = [
        row for row in results
        if row.status == "completed" and not row.error and row.output_text is not None
    ]
    if workload.quality == "completion_validity":
        nonempty = sum(bool((row.output_text or "").strip()) for row in successful)
        return (
            {
                "quality_status": "completion_validity_no_reference",
                "successful_rows": len(successful),
                "nonempty_completion_rows": nonempty,
            },
            nonempty,
        )
    references = {source_id: answers for source_id, answers in scoring.values()}
    predictions = {
        scoring[row.doc_id][0]: (
            row.output_text
            if row.status == "completed" and not row.error and row.output_text is not None
            else None
        )
        for row in results
    }
    metrics = squad_quality_metrics(predictions, references)
    return metrics, int(metrics["squad_exact_match_rows"])


def _result_metrics(
    workload: Workload,
    manifest: tuple[ChatRequest, ...],
    results: tuple[BaselineRequestResult, ...],
    scoring: Mapping[int, tuple[str, list[str]]],
    e2e_s: float,
) -> tuple[dict, bool]:
    expected_ids = {row.doc_id for row in manifest}
    observed_ids = [row.doc_id for row in results]
    exactly_once = len(observed_ids) == len(set(observed_ids)) and set(observed_ids) == expected_ids
    quality, correct_rows = _quality(workload, results, scoring)
    failures = [row for row in results if row.status != "completed" or row.error]
    cap_failures = [
        row for row in failures
        if "max_tokens" in (row.error or "") or "maximum token" in (row.error or "").lower()
    ]
    infrastructure_failures = [row for row in failures if row not in cap_failures]
    latencies = [row.completed_at_s - row.submitted_at_s for row in results]
    observed_tokens = sum(row.input_tokens + row.output_tokens for row in results)
    endpoint_rows = {
        str(index): sum(row.endpoint_index == index for row in results) for index in (0, 1)
    }
    endpoint_tokens = {
        str(index): sum(
            row.input_tokens + row.output_tokens
            for row in results
            if row.endpoint_index == index
        )
        for index in (0, 1)
    }
    metrics = {
        "row_count": len(results),
        "exactly_once": exactly_once,
        "endpoint_rows": endpoint_rows,
        "endpoint_observed_tokens": endpoint_tokens,
        "failure_count": len(failures),
        "cap_semantic_failure_count": len(cap_failures),
        "infrastructure_failure_count": len(infrastructure_failures),
        "null_output_count": sum(row.output_text is None for row in results),
        "finish_reason_length_count": sum(row.finish_reason == "length" for row in results),
        "observed_tokens": observed_tokens,
        "database_e2e_s": e2e_s,
        "raw_rows_per_s": len(results) / e2e_s if e2e_s else 0.0,
        "correct_rows": correct_rows,
        "correct_rows_per_s": correct_rows / e2e_s if e2e_s else 0.0,
        "request_latency_s_p50": _percentile(latencies, 0.50),
        "request_latency_s_p95": _percentile(latencies, 0.95),
        "request_latency_s_p99": _percentile(latencies, 0.99),
        "quality": quality,
    }
    return metrics, exactly_once and not infrastructure_failures


def _instrumentation_metrics(instrumentation, e2e_s: float, peak_each: float) -> dict:
    deltas = instrumentation.ttft_deltas or {}
    observed_tokens = sum(
        int(row.get("vllm_prompt_tokens_delta", 0) or 0)
        + int(row.get("vllm_generation_tokens_delta", 0) or 0)
        for row in deltas.values()
    )
    estimated_flops = sum(
        float(row.get("vllm_estimated_flops_per_gpu_delta", 0) or 0)
        for row in deltas.values()
    )
    gpu = dict(instrumentation.gpu_summary)
    total_power = sum(
        float(value)
        for key, value in gpu.items()
        if key.endswith("_power_mean")
    )
    mfu = estimate_mfu(
        estimated_flops=estimated_flops,
        observed_tokens=observed_tokens,
        operator_wall_s=e2e_s,
        model_flops_per_token=0.0,
        gpu_peak_tflops=peak_each * 2,
        precision="bf16_aggregate_two_gpu",
    )
    return {
        "service_observed_tokens": observed_tokens,
        "service_tokens_per_s": observed_tokens / e2e_s if e2e_s else 0.0,
        "estimated_flops": estimated_flops,
        "gpu_energy_j": total_power * e2e_s,
        "energy_j_per_correct_row": "computed_in_report",
        "gpu": gpu,
        "vllm_gauges": instrumentation.gauge_summary or {},
        "endpoint_vllm_deltas": {str(key): value for key, value in deltas.items()},
        "mfu": mfu,
    }


def _pg_identity(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("SHOW server_version")
        server = str(cur.fetchone()[0])
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        vector_row = cur.fetchone()
    return {
        "server_version": server,
        "pgvector_version": str(vector_row[0]) if vector_row else "not_installed",
    }


def _series(values: list[int]) -> dict[str, float]:
    mean = statistics.mean(values) if values else 0.0
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "min": min(values) if values else 0,
        "p50": _percentile([float(value) for value in values], 0.50),
        "p95": _percentile([float(value) for value in values], 0.95),
        "p99": _percentile([float(value) for value in values], 0.99),
        "max": max(values) if values else 0,
        "mean": mean,
        "stdev": stdev,
        "cv": stdev / mean if mean else 0.0,
    }


def _preflight(config: Config) -> dict:
    import psycopg

    verify_live_vllm_config(
        config.endpoint_urls,
        {
            "--max-num-seqs": str(config.service_max_num_seqs),
            "--max-num-batched-tokens": str(config.service_max_num_batched_tokens),
            "--max-model-len": "8192",
            "--gpu-memory-utilization": "0.90",
        },
        True,
        tag="opening-database-e2e",
    )
    for metrics_url in _metrics_urls(config.endpoint_urls):
        wait_for_idle((metrics_url,), 60.0)
    duckdb_identity = inspect_duckdb_ai_runtime(
        DuckDBAiConfig(
            endpoint_base_url=_endpoint_base(config.endpoint_urls[0]),
            model=config.model,
            api_key="EMPTY",
            max_tokens=64,
            max_concurrent_requests=config.concurrency_per_endpoint,
            timeout_seconds=int(config.timeout_s),
        )
    )
    workload_stats = {}
    with psycopg.connect(config.database_url) as conn:
        pg = _pg_identity(conn)
        for workload in config.workloads:
            manifest = read_manifest(workload.manifest)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT prompt_tokens, target_output_tokens FROM documents "
                    "WHERE workload_name = %s ORDER BY doc_id",
                    (workload.name,),
                )
                database_work = [(int(a), int(b)) for a, b in cur.fetchall()]
            if len(database_work) != workload.rows:
                raise ValueError(f"preflight row mismatch for {workload.name}")
            prompts = [row[0] for row in database_work]
            targets = [row[1] for row in database_work]
            estimated = [row.estimated_work for row in manifest]
            workload_stats[workload.label] = {
                "rows": workload.rows,
                "max_tokens": workload.max_tokens,
                "manifest_sha256": _manifest_sha(workload.manifest),
                "manifest_metadata": read_manifest_metadata(workload.manifest),
                "prompt_tokens": _series(prompts),
                "target_output_tokens": _series(targets),
                "estimated_work": _series(estimated),
                "prompt_buckets": {
                    "short_lt256": sum(value < 256 for value in prompts),
                    "medium_256_to_1024": sum(256 <= value <= 1024 for value in prompts),
                    "long_gt1024": sum(value > 1024 for value in prompts),
                },
            }
    ray_address = __import__("os").environ.get("RAY_ADDRESS", "")
    if not ray_address:
        raise RuntimeError("RAY_ADDRESS must point to the shared preflighted Ray head")
    return {
        "status": "passed",
        "postgres": pg,
        "duckdb": duckdb_identity,
        "ray_address_present": True,
        "service": {
            "model": config.model,
            "endpoint_count": 2,
            "prefix_caching": config.service_prefix_caching,
            "max_num_seqs": config.service_max_num_seqs,
            "max_num_batched_tokens": config.service_max_num_batched_tokens,
            "max_model_len": 8192,
            "gpu_memory_utilization": 0.90,
        },
        "workloads": workload_stats,
    }


def _run_in_process_cell(
    config: Config,
    workload: Workload,
    arm: str,
    cell_dir: Path,
) -> dict:
    import psycopg

    manifest = read_manifest(workload.manifest)
    cell_dir.mkdir(parents=True)
    with psycopg.connect(config.database_url) as conn:
        _clean_sink(conn, (row.doc_id for row in manifest))
    _service_condition(manifest, config, workload)
    metrics_urls = _metrics_urls(config.endpoint_urls)
    with instrumented_cell(
        metrics_urls,
        cell_dir / "gpu_resource.csv",
        interval_s=0.3,
    ) as instrumentation:
        started = time.perf_counter()
        with psycopg.connect(config.database_url) as conn:
            requests, sidecar, scoring = _scan_source(conn, workload, manifest)
            results = _run_shards(arm, requests, config, workload)
            written = write_completions(
                conn,
                _sink_payload(results, sidecar),
                "json_text",
                config.write_batch_rows,
            )
        e2e_s = time.perf_counter() - started
    with psycopg.connect(config.database_url) as conn:
        readback = _sink_readback(conn, results)
        pg_identity = _pg_identity(conn)
    _write_requests(cell_dir / "requests.csv", results)
    metrics, execution_ok = _result_metrics(
        workload, manifest, results, scoring, e2e_s
    )
    resources = _instrumentation_metrics(
        instrumentation, e2e_s, config.gpu_peak_tflops_each
    )
    correct_rows = int(metrics["correct_rows"])
    resources["energy_j_per_correct_row"] = (
        resources["gpu_energy_j"] / correct_rows if correct_rows else "unavailable"
    )
    report = {
        "status": "passed" if execution_ok and readback["matched"] and written == workload.rows else "failed",
        "arm": arm,
        "scheduler_owner": (
            "project_bounded_http_static_control"
            if arm == "direct_static_sharded"
            else "experiment_harness_static_shard + duckdb_ai_extension + vllm"
        ),
        "database_e2e_boundary": "postgres_scan_to_unified_sink_external_wall",
        "manifest_sha256": _manifest_sha(workload.manifest),
        "metrics": metrics,
        "resources": resources,
        "sink": {"rows_written": written, "readback": readback},
        "identity": pg_identity,
    }
    (cell_dir / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return report


def _read_profiler_row(path: Path) -> dict[str, str]:
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    formal = [row for row in rows if row.get("status") == "ok" and row.get("phase") == "formal"]
    if len(formal) != 1:
        raise ValueError(f"expected one formal profiler row, found {len(formal)}")
    return formal[0]


def _scan_scoring_only(conn, workload: Workload) -> dict[int, tuple[str, list[str]]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT doc_id, source_example_id, reference_answers FROM documents "
            "WHERE workload_name = %s ORDER BY doc_id",
            (workload.name,),
        )
        rows = cur.fetchall()
    scoring = {}
    for doc_id, source_id, raw_answers in rows:
        answers = raw_answers
        if isinstance(answers, str):
            answers = json.loads(answers)
        scoring[int(doc_id)] = (str(source_id or doc_id), list(answers or []))
    return scoring


def _run_project_cell(
    config: Config,
    workload: Workload,
    cell_dir: Path,
) -> dict:
    import psycopg

    manifest = read_manifest(workload.manifest)
    cell_dir.mkdir(parents=True)
    with psycopg.connect(config.database_url) as conn:
        _clean_sink(conn, (row.doc_id for row in manifest))
    _service_condition(manifest, config, workload)
    project = ProjectStaticConfig(
        database_url=config.database_url,
        workload_name=workload.name,
        endpoint_url=config.endpoint_urls[0],
        endpoint_urls=config.endpoint_urls,
        model=config.model,
        max_tokens=workload.max_tokens,
        token_budget=config.token_budget,
        max_inflight=config.concurrency_per_endpoint,
        max_active_work_per_endpoint=config.active_work_per_endpoint,
        actor_workers_per_endpoint=config.actor_workers_per_endpoint,
        ray_actor_max_concurrency=config.actor_concurrency,
        total_rows=workload.rows,
        write_batch_rows=config.write_batch_rows,
        request_timeout_s=config.timeout_s,
        python_executable=config.project_python,
        scenario_id=f"opening_{workload.label}_project_frozen_static",
        request_manifest=str(workload.manifest),
        database_e2e_timing_boundary=True,
    )
    run = run_project_static(project, cell_dir / "profiler")
    if run.exit_code != 0 or not run.formal_row_found:
        raise RuntimeError(
            f"project profiler failed: exit={run.exit_code}, formal={run.formal_row_found}, "
            f"stderr={run.stderr_tail[-300:]}"
        )
    assignment = {row.doc_id: row.endpoint_index for row in manifest}
    results = tuple(
        BaselineRequestResult(
            doc_id=row.doc_id,
            endpoint_index=assignment[row.doc_id],
            status=row.status,
            error=row.error,
            submitted_at_s=row.submitted_at_s,
            started_at_s=row.started_at_s,
            completed_at_s=row.completed_at_s,
            input_tokens=row.input_tokens,
            output_tokens=row.output_tokens,
            output_text=row.output_text,
            finish_reason=row.finish_reason,
        )
        for row in run.results
    )
    profiler_row = _read_profiler_row(cell_dir / "profiler" / "project_static_summary.csv")
    e2e_s = float(profiler_row["e2e_s"])
    with psycopg.connect(config.database_url) as conn:
        scoring = _scan_scoring_only(conn, workload)
        readback = _sink_readback(conn, results)
        pg_identity = _pg_identity(conn)
    metrics, execution_ok = _result_metrics(workload, manifest, results, scoring, e2e_s)
    resources = {
        key: profiler_row.get(key, "")
        for key in (
            "gpu_utilization_pct_mean", "gpu_utilization_pct_p50",
            "gpu_utilization_pct_p95", "gpu_utilization_pct_max", "gpu_power_w_mean",
            "gpu_energy_j", "energy_j_per_1k_observed_tokens", "mfu_status",
            "mfu_estimate", "vllm_running_mean", "vllm_running_max",
            "vllm_waiting_mean", "vllm_waiting_max", "vllm_kv_cache_usage_mean",
            "vllm_kv_cache_usage_max", "vllm_prefix_cache_hit_rate",
            "vllm_prompt_tokens_delta", "vllm_generation_tokens_delta",
        )
    }
    energy = float(profiler_row.get("gpu_energy_j") or 0.0)
    correct_rows = int(metrics["correct_rows"])
    resources["energy_j_per_correct_row"] = (
        energy / correct_rows if correct_rows else "unavailable"
    )
    report = {
        "status": "passed" if execution_ok and readback["matched"] else "failed",
        "arm": "project_frozen_static",
        "scheduler_owner": "project_frozen_static_ray_actor",
        "database_e2e_boundary": "profiler_opt_in_scan_to_unified_sink",
        "manifest_sha256": _manifest_sha(workload.manifest),
        "metrics": metrics,
        "resources": resources,
        "sink": {"rows_written": len(run.sunk_pairs), "readback": readback},
        "identity": pg_identity,
        "profiler_summary": "profiler/project_static_summary.csv",
    }
    (cell_dir / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return report


def _arm_order(seed: int, workload: Workload, phase: str, repeat: int) -> list[str]:
    arms = list(ARMS)
    material = f"{seed}:{workload.label}:{phase}:{repeat}"
    derived = int(hashlib.sha256(material.encode()).hexdigest()[:16], 16)
    random.Random(derived).shuffle(arms)
    return arms


def _git_commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=CODE_ROOT.parent,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def run_matrix(config: Config) -> dict:
    config.output_root.mkdir(parents=True)
    records: list[dict] = []
    commit = _git_commit()
    preflight: dict = {}
    try:
        with acquire_host_runner_lease(
            config.output_root.parent,
            repository_commit=commit,
        ):
            preflight = _preflight(config)
            (config.output_root / "preflight.json").write_text(
                json.dumps(
                    preflight,
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            for workload in config.workloads:
                for phase, count in (("warmup", 1), ("formal", 3)):
                    for repeat in range(1, count + 1):
                        for order, arm in enumerate(
                            _arm_order(config.seed, workload, phase, repeat), start=1
                        ):
                            cell_dir = (
                                config.output_root / "raw" / workload.label
                                / f"{phase}_{repeat:02d}_{order:02d}_{arm}"
                            )
                            started = time.time()
                            record = {
                                "workload": workload.label,
                                "phase": phase,
                                "repeat": repeat,
                                "order": order,
                                "arm": arm,
                                "cell_dir": str(cell_dir),
                                "started_at": started,
                            }
                            try:
                                if arm == "project_frozen_static":
                                    report = _run_project_cell(config, workload, cell_dir)
                                else:
                                    report = _run_in_process_cell(
                                        config, workload, arm, cell_dir
                                    )
                                record.update(
                                    {
                                        "status": report["status"],
                                        "database_e2e_s": report["metrics"]["database_e2e_s"],
                                        "correct_rows_per_s": report["metrics"]["correct_rows_per_s"],
                                        "failure_count": report["metrics"]["failure_count"],
                                    }
                                )
                            except Exception as exc:
                                record.update(
                                    {
                                        "status": "failed",
                                        "error": f"{type(exc).__name__}: {exc}"[:1000],
                                    }
                                )
                            record["finished_at"] = time.time()
                            records.append(record)
                            (config.output_root / "matrix_status.json").write_text(
                                json.dumps(records, indent=2, ensure_ascii=False) + "\n",
                                encoding="utf-8",
                            )
                            if record["status"] != "passed":
                                raise RuntimeError(f"cell failed closed: {record}")
    except Exception:
        raise
    summary = {
        "status": "passed",
        "experiment_id": config.experiment_id,
        "git_commit": commit,
        "cells": len(records),
        "formal_cells": sum(row["phase"] == "formal" for row in records),
        "preflight": preflight,
        "records": records,
    }
    (config.output_root / "run_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return summary


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse(sys.argv[1:] if argv is None else argv)
    config = _load_config(Path(args.config))
    summary = run_matrix(config)
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
