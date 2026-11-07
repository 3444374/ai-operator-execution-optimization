"""Direct no-Job control adapter for the shared-vLLM experiment runner."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import math
import os
from dataclasses import asdict, replace
from pathlib import Path
from urllib.parse import urlsplit

from src.baselines.common.manifests import read_manifest
from src.baselines.common.redact import redact_text
from src.baselines.common.results import validate_results
from src.baselines.text.controls.async_http import (
    BoundedHttpConfig,
    HttpAdmissionEvent,
    TimedHttpJob,
    run_bounded_http_jobs,
)
from src.observability.metrics import percentile

from .config import (
    SharedVllmConfig,
    SharedVllmScenario,
    _argument_value,
    _csv_argument_values,
)


def direct_control_contract(config: SharedVllmConfig) -> BoundedHttpConfig:
    """Derive the direct client from the exact project request contract."""

    endpoint_urls = _csv_argument_values(
        config.common_args,
        "--completion-endpoint-urls",
    )
    if len(endpoint_urls) != len(config.endpoint_ids):
        raise ValueError(
            "direct_no_job requires one completion endpoint per endpoint_id"
        )
    protocol = _argument_value(
        config.common_args,
        "--completion-protocol",
        "completions",
    )
    prompt_format = _argument_value(
        config.common_args,
        "--completion-prompt-format",
        "raw",
    )
    if protocol not in {"completions", "chat_completions"}:
        raise ValueError("direct_no_job requires a supported completion protocol")
    if prompt_format not in {"raw", "chatml"}:
        raise ValueError("direct_no_job requires a supported prompt format")
    expected_path = (
        "/v1/completions"
        if protocol == "completions"
        else "/v1/chat/completions"
    )
    if any(not urlsplit(url).path.endswith(expected_path) for url in endpoint_urls):
        raise ValueError(
            "direct_no_job endpoint paths must match the completion protocol"
        )
    model = _argument_value(
        config.common_args,
        "--completion-model",
        "",
    )
    if not model:
        raise ValueError("direct_no_job requires --completion-model")
    temperature_raw = _argument_value(
        config.common_args,
        "--completion-temperature",
        "",
    )
    temperature = float(temperature_raw) if temperature_raw else None
    return BoundedHttpConfig(
        endpoint_urls=endpoint_urls,
        model=model,
        concurrency_per_endpoint=config.request_limit_per_endpoint,
        timeout_s=float(
            _argument_value(
                config.common_args,
                "--completion-request-timeout-s",
                "120",
            )
        ),
        api_key=(
            _argument_value(
                config.common_args,
                "--completion-api-key",
                "",
            )
            or os.environ.get("COMPLETION_API_KEY")
            or None
        ),
        replay_arrivals=True,
        arrival_time_scale=float(
            _argument_value(
                config.common_args,
                "--arrival-time-scale",
                "1.0",
            )
        ),
        ignore_eos="--completion-ignore-eos" in config.common_args,
        protocol=protocol,  # type: ignore[arg-type]
        prompt_format=prompt_format,  # type: ignore[arg-type]
        temperature=temperature,
        return_token_ids="--completion-return-token-ids" in config.common_args,
        keepalive_expiry_s=float(
            _argument_value(
                config.common_args,
                "--completion-http-keepalive-expiry-s",
                "4.0",
            )
        ),
    )


def run_direct_control(
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
    *,
    start_epoch_s: float,
    output_dir: Path,
    run_stem: str,
) -> list[dict[str, object]]:
    """Execute and persist one merged-arrival control group."""

    if scenario.policy not in {"direct_no_job", "direct_work_limited"}:
        raise ValueError("run_direct_control requires a direct control policy")
    if not scenario.request_manifests or not all(scenario.request_manifests):
        raise ValueError("direct control requires immutable request manifests")
    contract = direct_control_contract(config)
    contract = replace(contract, replay_start_epoch_s=start_epoch_s)
    manifests = [Path(str(path)).resolve() for path in scenario.request_manifests]
    requests_by_job = [read_manifest(path) for path in manifests]
    seen_doc_ids: set[int] = set()
    for index, requests in enumerate(requests_by_job):
        if len(requests) != scenario.row_count(index):
            raise RuntimeError(
                f"direct job {index} manifest row count does not match scenario"
            )
        ids = {request.doc_id for request in requests}
        if seen_doc_ids & ids:
            raise RuntimeError("direct jobs contain duplicate doc_id values")
        seen_doc_ids.update(ids)
    jobs = tuple(
        TimedHttpJob(
            job_id=f"direct-job-{index}",
            requests=requests,
            arrival_offset_s=scenario.arrival_offsets_s[index],
        )
        for index, requests in enumerate(requests_by_job)
    )
    work_cost = config.completion_work_cost
    request_limit, work_limit = scenario.endpoint_limits(
        config.request_limit_per_endpoint,
        config.work_limit_per_endpoint,
    )
    if request_limit != contract.concurrency_per_endpoint:
        contract = replace(contract, concurrency_per_endpoint=request_limit)
    admission_events: list[HttpAdmissionEvent] = []

    def estimated_work(request) -> int:
        return work_cost.estimated_work(
            request.prompt_tokens,
            request.estimated_output_tokens,
        )

    grouped = asyncio.run(
        run_bounded_http_jobs(
            jobs,
            contract,
            work_limit_per_endpoint=(
                work_limit
                if scenario.policy == "direct_work_limited"
                else None
            ),
            request_work_estimator=estimated_work,
            admission_event_sink=admission_events.append,
        )
    )
    admission_path = (
        output_dir / "traces" / f"{run_stem}.direct_admission.csv"
    )
    _write_admission_trace(admission_path, admission_events)
    admission_summary = _summarize_admission_events(
        admission_events,
        endpoint_count=len(config.endpoint_ids),
        request_limit=request_limit,
        reference_work_limit=work_limit,
        work_limit_applied=scenario.policy == "direct_work_limited",
        trace_path=Path("traces") / admission_path.name,
    )
    slo_ms = float(
        _argument_value(config.common_args, "--request-slo-ms", "0")
    )
    slo_s = slo_ms / 1000.0 if slo_ms > 0 else None
    evidence = []
    for index, (job, requests, manifest_path) in enumerate(
        zip(jobs, requests_by_job, manifests)
    ):
        results = grouped[job.job_id]
        if not results:
            raise RuntimeError(f"direct job {index} produced no results")
        trace_path = (
            output_dir / "jobs" / f"{run_stem}_job{index}.requests.csv"
        )
        _write_direct_trace(
            trace_path,
            job.job_id,
            results,
            admission_events,
        )
        validate_results(requests, results)
        request_by_id = {request.doc_id: request for request in requests}
        actual_work_by_request = [
            result.input_tokens + result.output_tokens for result in results
        ]
        latencies = [result.latency_s for result in results]
        slo_met = [
            latency <= slo_s if slo_s is not None else True
            for latency in latencies
        ]
        arrivals = [result.submitted_at_s for result in results]
        completions = [result.completed_at_s for result in results]
        jct_s = max(completions) - min(arrivals)
        if not math.isfinite(jct_s) or jct_s <= 0:
            raise RuntimeError("direct job JCT must be finite and positive")
        endpoint_counts: dict[str, int] = {}
        for result in results:
            endpoint_id = config.endpoint_ids[result.endpoint_index]
            endpoint_counts[endpoint_id] = endpoint_counts.get(endpoint_id, 0) + 1
        manifest_payload = manifest_path.read_bytes()
        evidence.append(
            {
                "jct_s": jct_s,
                "p99_s": percentile(latencies, 99),
                "completion_lag_s": max(completions) - max(arrivals),
                "slo_violation_ratio": 1.0 - sum(slo_met) / len(slo_met),
                "slo_goodput_per_s": sum(slo_met) / jct_s,
                "slo_token_goodput_per_s": sum(
                    work
                    for work, met in zip(actual_work_by_request, slo_met)
                    if met
                )
                / jct_s,
                "predicted_work": sum(
                    work_cost.estimated_work(
                        request.prompt_tokens,
                        request.estimated_output_tokens,
                    )
                    for request in requests
                ),
                "actual_work": sum(actual_work_by_request),
                "expected_count": len(requests),
                "completed_count": len(results),
                # validate_results() above already proves a one-to-one,
                # successful logical-request join for this direct Job.
                "exactly_once": True,
                "actual_prompt_work": sum(result.input_tokens for result in results),
                "actual_output_work": sum(result.output_tokens for result in results),
                "actual_work_source": "service_usage_prompt_plus_output",
                "source_row_offset": 0,
                "request_manifest_path": str(manifest_path),
                "request_manifest_sha256": hashlib.sha256(
                    manifest_payload
                ).hexdigest(),
                "runtime_job_id": job.job_id,
                "arrival_start_epoch_s": min(arrivals),
                "completion_end_epoch_s": max(completions),
                "service_completion_events": sorted(
                    zip(completions, actual_work_by_request)
                ),
                "request_backlog_intervals": sorted(zip(arrivals, completions)),
                "endpoint_counts": endpoint_counts,
                "actor_worker_failures": 0,
                "http_keepalive_expiry_s": contract.keepalive_expiry_s,
                "replay_configured_start_epoch_s": (
                    start_epoch_s + scenario.arrival_offsets_s[index]
                ),
                "replay_observed_start_epoch_s": min(arrivals),
                # submitted_at_s is the logical HTTP enqueue time. The
                # semaphore-acquire time is treatment-induced queueing and
                # must not invalidate the immutable arrival replay gate.
                "replay_actual_submit_start_epoch_s": min(arrivals),
                **admission_summary,
            }
        )
        if set(request_by_id) != {result.doc_id for result in results}:
            raise RuntimeError("direct request identity changed during execution")
    return evidence


def _write_direct_trace(
    path: Path,
    job_id: str,
    results,
    admission_events: list[HttpAdmissionEvent],
) -> None:
    acquired = {
        event.doc_id: event
        for event in admission_events
        if event.action == "acquire"
    }
    rows = []
    for result in sorted(results, key=lambda item: item.doc_id):
        admission = acquired.get(result.doc_id)
        if admission is None:
            raise RuntimeError("direct request lacks an admission event")
        rows.append(
            {
                "job_id": job_id,
                "doc_id": result.doc_id,
                "endpoint_index": result.endpoint_index,
                "status": result.status,
                "error": redact_text(result.error or ""),
                "submitted_at_s": result.submitted_at_s,
                "started_at_s": result.started_at_s,
                "admission_wait_s": admission.admission_wait_s,
                "estimated_work": admission.estimated_work,
                "completed_at_s": result.completed_at_s,
                "input_tokens": result.input_tokens,
                "output_tokens": result.output_tokens,
                "finish_reason": result.finish_reason or "",
                "output_sha256": hashlib.sha256(
                    (result.output_text or "").encode("utf-8")
                ).hexdigest(),
            }
        )
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def _write_admission_trace(
    path: Path,
    events: list[HttpAdmissionEvent],
) -> None:
    if not events:
        raise RuntimeError("direct admission trace is empty")
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [asdict(event) for event in events]
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def _summarize_admission_events(
    events: list[HttpAdmissionEvent],
    *,
    endpoint_count: int,
    request_limit: int,
    reference_work_limit: int,
    work_limit_applied: bool,
    trace_path: Path,
) -> dict[str, object]:
    """Derive time-weighted K/W occupancy from the lossless direct ledger."""

    if endpoint_count <= 0 or request_limit <= 0 or reference_work_limit <= 0:
        raise ValueError("direct occupancy limits must be positive")
    acquired = [event for event in events if event.action == "acquire"]
    released = [event for event in events if event.action == "release"]
    if not acquired or len(acquired) != len(released):
        raise RuntimeError("direct admission lifecycle is incomplete")
    if len({event.doc_id for event in acquired}) != len(acquired):
        raise RuntimeError("direct admission trace contains duplicate requests")
    if len({event.doc_id for event in released}) != len(released):
        raise RuntimeError("direct admission trace contains duplicate releases")
    acquired_by_id = {event.doc_id: event for event in acquired}
    released_by_id = {event.doc_id: event for event in released}
    if set(acquired_by_id) != set(released_by_id):
        raise RuntimeError("direct admission acquire/release identities differ")
    if any(
        released_by_id[doc_id].endpoint_index != event.endpoint_index
        or released_by_id[doc_id].estimated_work != event.estimated_work
        for doc_id, event in acquired_by_id.items()
    ):
        raise RuntimeError("direct admission release identity or work drifted")

    request_fraction_by_endpoint: dict[str, float] = {}
    work_fraction_by_endpoint: dict[str, float] = {}
    request_max_by_endpoint: dict[str, int] = {}
    work_max_by_endpoint: dict[str, int] = {}
    for endpoint_index in range(endpoint_count):
        rows = sorted(
            (
                event
                for event in events
                if event.endpoint_index == endpoint_index
            ),
            key=lambda event: event.event_epoch_s,
        )
        if not rows:
            raise RuntimeError(
                f"direct admission endpoint {endpoint_index} has no events"
            )
        if any(
            event.active_requests < 0
            or event.active_work < 0
            or event.active_requests > request_limit
            for event in rows
        ):
            raise RuntimeError("direct admission occupancy is outside K")
        expected_requests = 0
        expected_work = 0
        for event in rows:
            direction = 1 if event.action == "acquire" else -1
            expected_requests += direction
            expected_work += direction * event.estimated_work
            if (
                event.active_requests != expected_requests
                or event.active_work != expected_work
            ):
                raise RuntimeError(
                    "direct admission snapshots do not replay losslessly"
                )
        if work_limit_applied and any(
            event.active_work > reference_work_limit for event in rows
        ):
            raise RuntimeError("direct admission occupancy is outside W")
        if rows[-1].active_requests != 0 or rows[-1].active_work != 0:
            raise RuntimeError("direct admission occupancy did not drain")
        duration = rows[-1].event_epoch_s - rows[0].event_epoch_s
        request_area = 0.0
        work_area = 0.0
        for left, right in zip(rows, rows[1:]):
            interval = max(0.0, right.event_epoch_s - left.event_epoch_s)
            request_area += left.active_requests * interval
            work_area += left.active_work * interval
        key = str(endpoint_index)
        request_fraction_by_endpoint[key] = (
            request_area / duration / request_limit if duration > 0 else 0.0
        )
        work_fraction_by_endpoint[key] = (
            work_area / duration / reference_work_limit
            if duration > 0
            else 0.0
        )
        request_max_by_endpoint[key] = max(
            event.active_requests for event in rows
        )
        work_max_by_endpoint[key] = max(event.active_work for event in rows)

    waits = [event.admission_wait_s for event in acquired]
    return {
        "direct_admission_trace_status": "ok:lossless_acquire_release_ledger",
        "direct_admission_trace_path": str(trace_path),
        "direct_admission_events": len(events),
        "direct_work_limit_applied": work_limit_applied,
        "direct_request_occupancy_max": max(request_max_by_endpoint.values()),
        "direct_estimated_work_occupancy_max": max(
            work_max_by_endpoint.values()
        ),
        "direct_request_occupancy_fraction_mean": (
            sum(request_fraction_by_endpoint.values()) / endpoint_count
        ),
        "direct_estimated_work_to_reference_w_fraction_mean": (
            sum(work_fraction_by_endpoint.values()) / endpoint_count
        ),
        "direct_request_occupancy_max_by_endpoint": json.dumps(
            request_max_by_endpoint,
            sort_keys=True,
        ),
        "direct_estimated_work_occupancy_max_by_endpoint": json.dumps(
            work_max_by_endpoint,
            sort_keys=True,
        ),
        "direct_admission_wait_p50_s": percentile(waits, 50),
        "direct_admission_wait_p95_s": percentile(waits, 95),
        "direct_admission_wait_p99_s": percentile(waits, 99),
        "direct_admission_wait_max_s": max(waits),
    }
