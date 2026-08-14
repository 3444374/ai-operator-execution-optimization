"""Shared-vLLM exactly-once, resume, trace, and failure evidence helpers."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import subprocess
import time
from dataclasses import asdict
from pathlib import Path

from src.observability.metrics import percentile

from .config import (
    GroupRunIdentity,
    RunnerOptions,
    SharedVllmConfig,
    SharedVllmScenario,
    _csv_argument_values,
)

_CODE_ROOT = Path(__file__).resolve().parents[3]


def _validate_replay_starts(
    job_evidence: list[dict[str, object]],
    *,
    expected_start_epoch_s: float,
    arrival_offsets_s: tuple[float, ...],
    max_lateness_s: float,
    max_skew_s: float,
) -> None:
    if len(job_evidence) != len(arrival_offsets_s):
        raise RuntimeError("replay start evidence is incomplete")
    normalized_barrier_starts = []
    for index, (evidence, offset_s) in enumerate(
        zip(job_evidence, arrival_offsets_s)
    ):
        configured = float(
            evidence["replay_configured_start_epoch_s"]
        )
        observed = float(evidence["replay_observed_start_epoch_s"])
        actual_submit = float(
            evidence["replay_actual_submit_start_epoch_s"]
        )
        expected = expected_start_epoch_s + offset_s
        if abs(configured - expected) > 0.01:
            raise RuntimeError(
                f"job {index} replay configured start does not match runner"
            )
        barrier_lateness = observed - configured
        if barrier_lateness < -0.01:
            raise RuntimeError(
                f"job {index} crossed replay barrier before its deadline"
            )
        if barrier_lateness > max_lateness_s:
            raise RuntimeError(
                f"job {index} missed replay start deadline by "
                f"{barrier_lateness:.6f}s"
            )
        # First submission is intentionally downstream of admission/credit
        # control.  Its post-barrier delay is an experimental outcome, not a
        # launch-validity condition; bounding it here would reject selectors
        # that deliberately queue a ready Job.  It may never precede the
        # observed replay barrier.
        if actual_submit < observed - 0.01:
            raise RuntimeError(
                f"job {index} submitted before crossing its replay barrier"
            )
        normalized_barrier_starts.append(observed - offset_s)
    if (
        normalized_barrier_starts
        and max(normalized_barrier_starts) - min(normalized_barrier_starts)
        > max_skew_s
    ):
        raise RuntimeError("cross-job replay start skew exceeded limit")

def _validate_runner_topology(
    options: RunnerOptions,
    config: SharedVllmConfig,
) -> None:
    if len(options.metrics_urls) != len(config.endpoint_ids):
        raise ValueError(
            "runner requires one metrics URL per configured endpoint"
        )
    if len(set(options.metrics_urls)) != len(options.metrics_urls):
        raise ValueError("runner metrics URLs must be unique")
    configured_metrics = _csv_argument_values(
        config.common_args,
        "--model-metrics-urls",
    )
    if configured_metrics and configured_metrics != options.metrics_urls:
        raise ValueError(
            "runner metrics URLs must match profiler model metrics URLs"
        )
    configured_endpoints = _csv_argument_values(
        config.common_args,
        "--completion-endpoint-urls",
    )
    if configured_endpoints and len(configured_endpoints) != len(
        config.endpoint_ids
    ):
        raise ValueError(
            "completion endpoint count must match endpoint_ids"
        )

def _validate_job_evidence(
    options: RunnerOptions,
    scenario: SharedVllmScenario,
    identity: GroupRunIdentity,
    job_index: int,
) -> dict[str, object]:
    run_stem = (
        f"{identity.order_index:03d}_{identity.phase}_"
        f"{identity.repeat_index}_{scenario.scenario_id}"
    )
    job_stem = options.output_dir / "jobs" / f"{run_stem}_job{job_index}"
    summary_rows = _read_csv(job_stem.with_suffix(".runs.csv"))
    request_rows = _read_csv(job_stem.with_suffix(".requests.csv"))
    submission_rows = _read_csv(job_stem.with_suffix(".submissions.csv"))
    if len(summary_rows) != 1 or summary_rows[0].get("status") != "ok":
        raise RuntimeError(f"job {job_index} has no unique successful summary")
    summary = summary_rows[0]
    expected_rows = scenario.row_count(job_index)
    if int(summary.get("total_rows", -1)) != expected_rows:
        raise RuntimeError(f"job {job_index} processed an unexpected row count")
    if len(request_rows) != expected_rows:
        raise RuntimeError(f"job {job_index} request trace is not exactly-once")
    if len(submission_rows) != expected_rows:
        raise RuntimeError(
            f"job {job_index} submission trace is not exactly-once"
        )
    expected_offset = (
        scenario.source_row_offsets[job_index]
        if scenario.source_row_offsets
        else 0
    )
    if (
        scenario.source_row_offsets
        and int(summary.get("source_row_offset", -1)) != expected_offset
    ):
        raise RuntimeError(f"job {job_index} source offset does not match")
    expected_manifest = (
        scenario.request_manifests[job_index]
        if scenario.request_manifests
        else None
    )
    observed_manifest = str(summary.get("request_manifest_path", "") or "")
    if expected_manifest is not None:
        if (
            not observed_manifest
            or Path(observed_manifest).resolve()
            != Path(expected_manifest).resolve()
            or summary.get("request_manifest_validation_status") != "ok"
            or int(summary.get("request_manifest_validated_rows", -1))
            != expected_rows
        ):
            raise RuntimeError(
                f"job {job_index} request manifest evidence does not match"
            )
    request_ids = [row.get("request_id", "") for row in request_rows]
    if len(set(request_ids)) != len(request_ids) or "" in request_ids:
        raise RuntimeError(f"job {job_index} has duplicate request IDs")
    if any(not _request_trace_succeeded(row) for row in request_rows):
        raise RuntimeError(f"job {job_index} contains failed requests")
    request_job_ids = [
        str(row.get("job_id", "") or "").strip()
        for row in request_rows
    ]
    if any(not job_id for job_id in request_job_ids):
        raise RuntimeError(f"job {job_index} has a missing runtime job ID")
    runtime_job_ids = set(request_job_ids)
    if len(runtime_job_ids) != 1:
        raise RuntimeError(f"job {job_index} has inconsistent runtime job IDs")
    runtime_job_id = runtime_job_ids.pop()
    summary_job_id = str(summary.get("job_id", "") or "").strip()
    if not summary_job_id:
        raise RuntimeError(f"job {job_index} summary has a missing runtime job ID")
    if summary_job_id != runtime_job_id:
        raise RuntimeError(
            f"job {job_index} summary/request runtime job IDs disagree"
        )
    arrival = [float(row["arrival_epoch_s"]) for row in request_rows]
    completion = [float(row["completion_epoch_s"]) for row in request_rows]
    e2e = [float(row["e2e_s"]) for row in request_rows]
    submission_starts = [
        float(row["submit_epoch_s"]) for row in request_rows
    ]
    request_submit_by_submission_id: dict[str, float] = {}
    for row in request_rows:
        submission_id = str(row.get("submission_id", "") or "")
        if not submission_id:
            continue
        if submission_id in request_submit_by_submission_id:
            raise RuntimeError(
                f"job {job_index} has duplicate submission IDs in the "
                "request trace"
            )
        request_submit_by_submission_id[submission_id] = float(
            row["submit_epoch_s"]
        )
    ready_lifecycle_rows = []
    for row in submission_rows:
        ready_raw = row.get("ready_epoch_s", "")
        registered_raw = row.get("credit_registered_epoch_s", "")
        granted_raw = row.get("credit_granted_epoch_s", "")
        if not ready_raw and not registered_raw and not granted_raw:
            continue
        if not ready_raw or not registered_raw or not granted_raw:
            raise RuntimeError(
                f"job {job_index} has an incomplete ready lifecycle"
            )
        ready_epoch_s = float(ready_raw)
        registered_epoch_s = float(registered_raw)
        granted_epoch_s = float(granted_raw)
        submission_id = str(row.get("submission_id", "") or "")
        if submission_id not in request_submit_by_submission_id:
            raise RuntimeError(
                f"job {job_index} cannot join ready lifecycle submission "
                f"{submission_id or '<missing>'!r} to the request trace"
            )
        # The submission trace owns actor/credit lifecycle timestamps, while
        # the request trace owns the scheduler submit timestamp. Join the two
        # schemas by submission_id instead of assuming a duplicate timestamp
        # column in the submission trace.
        submit_epoch_s = request_submit_by_submission_id[submission_id]
        if not (
            ready_epoch_s
            <= registered_epoch_s
            <= granted_epoch_s
            <= submit_epoch_s
        ):
            raise RuntimeError(
                f"job {job_index} has an unordered ready lifecycle"
            )
        ready_lifecycle_rows.append(
            {
                "request_id": submission_id,
                "endpoint_id": row["endpoint_id"],
                "ready_epoch_s": ready_epoch_s,
                "registered_epoch_s": registered_epoch_s,
                "granted_epoch_s": granted_epoch_s,
                "submit_epoch_s": submit_epoch_s,
            }
        )
    slo_met = [
        str(row.get("slo_met", "")).strip().lower() == "true"
        for row in request_rows
    ]
    jct_s = max(completion) - min(arrival)
    completed_in_slo = sum(slo_met)
    actual_prompt_work = sum(int(row["prompt_tokens"]) for row in request_rows)
    actual_output_work_by_request = []
    for row in request_rows:
        observed = row.get("actual_output_tokens")
        fallback = (
            row.get("client_estimated_output_tokens")
            or row["estimated_output_tokens"]
        )
        actual_output_work_by_request.append(
            int(observed) if observed not in (None, "") else int(fallback)
        )
    actual_work_by_request = [
        int(row["prompt_tokens"]) + output_work
        for row, output_work in zip(request_rows, actual_output_work_by_request)
    ]
    request_service_by_submission_id = {
        str(row.get("submission_id", "") or ""): (
            float(row["completion_epoch_s"]),
            actual_work_by_request[index],
        )
        for index, row in enumerate(request_rows)
        if str(row.get("submission_id", "") or "")
    }
    for lifecycle in ready_lifecycle_rows:
        completion_epoch_s, actual_request_work = (
            request_service_by_submission_id[str(lifecycle["request_id"])]
        )
        lifecycle["completion_epoch_s"] = completion_epoch_s
        lifecycle["actual_work"] = actual_request_work
    actual_work = sum(actual_work_by_request)
    slo_token_goodput = sum(
        work
        for work, met in zip(actual_work_by_request, slo_met)
        if met
    ) / jct_s
    predicted_work = sum(
        int(row["prompt_tokens"])
        + int(
            row["client_estimated_output_tokens"]
            or row["estimated_output_tokens"]
        )
        for row in request_rows
    )
    endpoint_counts: dict[str, int] = {}
    for row in request_rows:
        endpoint_id = row["endpoint_id"]
        endpoint_counts[endpoint_id] = (
            endpoint_counts.get(endpoint_id, 0) + 1
        )
    return {
        "completed_count": len(request_rows),
        "expected_count": expected_rows,
        "exactly_once": (
            len(request_rows) == expected_rows
            and len(submission_rows) == expected_rows
        ),
        "jct_s": jct_s,
        "p99_s": percentile(e2e, 99),
        "completion_lag_s": max(completion) - max(arrival),
        "slo_violation_ratio": 1.0 - completed_in_slo / len(slo_met),
        "slo_goodput_per_s": completed_in_slo / jct_s,
        "slo_token_goodput_per_s": slo_token_goodput,
        "predicted_work": predicted_work,
        "actual_work": actual_work,
        "actual_prompt_work": actual_prompt_work,
        "actual_output_work": sum(actual_output_work_by_request),
        "actual_work_source": (
            "prompt_plus_actual_or_client_estimate_fallback"
        ),
        "source_row_offset": expected_offset,
        "request_manifest_path": observed_manifest,
        "request_manifest_sha256": str(
            summary.get("request_manifest_sha256", "") or ""
        ),
        "runtime_job_id": runtime_job_id,
        "arrival_start_epoch_s": min(arrival),
        "completion_end_epoch_s": max(completion),
        "service_completion_events": sorted(
            zip(completion, actual_work_by_request)
        ),
        "request_backlog_intervals": sorted(zip(arrival, completion)),
        "endpoint_counts": endpoint_counts,
        "actor_worker_failures": _sum_semicolon_integers(
            summary.get("actor_worker_failures", "")
        ),
        "replay_configured_start_epoch_s": float(
            summary.get("arrival_replay_start_epoch_s", "0") or 0
        ),
        "replay_observed_start_epoch_s": float(
            summary.get(
                "arrival_replay_observed_start_epoch_s",
                "0",
            )
            or 0
        ),
        "replay_actual_submit_start_epoch_s": min(submission_starts),
        "ready_lifecycle_rows": ready_lifecycle_rows,
        "ready_lifecycle_complete": len(ready_lifecycle_rows) == expected_rows,
        "max_ready_requests_seen": int(
            summary.get("max_ready_requests_seen", "0") or 0
        ),
        "max_ready_work_seen": int(
            summary.get("max_ready_work_seen", "0") or 0
        ),
        "max_ready_payload_bytes_seen": int(
            summary.get("max_ready_payload_bytes_seen", "0") or 0
        ),
        "ready_requests_transition_mean": float(
            summary.get("ready_requests_transition_mean", "0") or 0
        ),
        "ready_requests_transition_p95": float(
            summary.get("ready_requests_transition_p95", "0") or 0
        ),
        "ready_work_transition_mean": float(
            summary.get("ready_work_transition_mean", "0") or 0
        ),
        "ready_work_transition_p95": float(
            summary.get("ready_work_transition_p95", "0") or 0
        ),
        "ready_payload_bytes_transition_mean": float(
            summary.get("ready_payload_bytes_transition_mean", "0") or 0
        ),
        "ready_payload_bytes_transition_p95": float(
            summary.get("ready_payload_bytes_transition_p95", "0") or 0
        ),
    }


def _validate_runtime_job_ids(
    job_evidence: list[dict[str, object]],
) -> None:
    """Require one stable, unique runtime identity per concurrent Job."""

    runtime_job_ids = [
        str(evidence.get("runtime_job_id", "") or "").strip()
        for evidence in job_evidence
    ]
    if any(not job_id for job_id in runtime_job_ids):
        raise RuntimeError("concurrent Job evidence has a missing runtime job ID")
    if len(runtime_job_ids) != len(set(runtime_job_ids)):
        raise RuntimeError("concurrent Job runtime IDs must be unique")

def _sum_semicolon_integers(value: object) -> int:
    fields = [
        item.strip()
        for item in str(value or "").split(";")
        if item.strip()
    ]
    return sum(int(item) for item in fields)

def _request_trace_succeeded(row: dict[str, str]) -> bool:
    return (
        row.get("status", "").strip().lower() == "completed"
        and not row.get("error_type", "").strip()
    )

def _validate_final_credit(
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
    snapshots: list[dict[str, object]],
) -> None:
    if len(snapshots) != len(config.endpoint_ids):
        raise RuntimeError("shared credit final snapshot is incomplete")
    request_limit, work_limit = _maximum_scenario_capacity(config, scenario)
    for snapshot in snapshots:
        if (
            int(snapshot["active_requests"]) != 0
            or int(snapshot["active_work"]) != 0
            or int(snapshot["waiting_requests"]) != 0
            or int(snapshot["waiting_work"]) != 0
        ):
            raise RuntimeError("shared credit did not return to zero")
        if (
            int(snapshot["max_active_requests_seen"])
            > request_limit
        ):
            raise RuntimeError("shared request limit was exceeded")
        if (
            int(snapshot["max_active_work_seen"])
            > work_limit
        ):
            raise RuntimeError("shared work limit was exceeded")


def _maximum_scenario_capacity(
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
) -> tuple[int, int]:
    """Return the largest configured safe arm used for peak validation."""
    if (
        scenario.policy == "state_aware_adaptive"
        and config.state_aware_control is not None
    ):
        return (
            max(config.state_aware_control.request_candidates),
            max(config.state_aware_control.work_candidates),
        )
    if (
        scenario.policy == "saor_capacity"
        and config.saor_capacity_control is not None
    ):
        return (
            max(item.arm.request_limit for item in config.saor_capacity_control.arms),
            max(item.arm.work_limit for item in config.saor_capacity_control.arms),
        )
    return scenario.endpoint_limits(
        config.request_limit_per_endpoint,
        config.work_limit_per_endpoint,
    )

def _terminate_processes(processes: list[subprocess.Popen]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
    deadline = time.monotonic() + 10.0
    for process in processes:
        if process.poll() is not None:
            continue
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

def _write_trace_rows_atomic(
    path: Path,
    rows: list[dict[str, object]],
    *,
    fieldnames: tuple[str, ...] | None = None,
) -> None:
    if not rows and fieldnames is None:
        return
    output_fields = list(fieldnames or tuple(rows[0]))
    if any(list(row) != output_fields for row in rows):
        raise ValueError("trace rows have inconsistent schemas")
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)

def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))

def _load_group_record(
    path: Path,
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
    identity: GroupRunIdentity,
) -> dict[str, object]:
    record = json.loads(path.read_text(encoding="utf-8"))
    expected = {
        "schema_version": 2,
        "experiment_id": config.experiment_id,
        "scenario_id": scenario.scenario_id,
        "phase": identity.phase,
        "repeat_index": identity.repeat_index,
        "order_index": identity.order_index,
        "policy": scenario.policy,
        "job_count": scenario.job_count,
        "rows_per_job": scenario.rows_per_job,
        "run_instance_id": _run_instance_id(path.parent.parent),
        "incidents": 0,
        "actor_worker_failures": 0,
    }
    for key, value in expected.items():
        if record.get(key) != value:
            raise RuntimeError(
                f"completed group record does not match {key}"
            )
    return record

def _rewrite_group_runs(
    path: Path,
    output_dir: Path,
    completed_runs: list[dict[str, object]],
) -> None:
    records = []
    for completed in sorted(
        completed_runs,
        key=lambda item: int(item["order_index"]),
    ):
        relative = Path(str(completed.get("record_path", "")))
        if (
            not relative.parts
            or relative.parts[0] != "records"
            or ".." in relative.parts
            or relative.is_absolute()
        ):
            raise RuntimeError("manifest contains an unsafe record_path")
        record_path = output_dir / relative
        if not record_path.exists():
            raise RuntimeError("manifest completed record is missing")
        records.append(json.loads(record_path.read_text(encoding="utf-8")))
    if not records:
        return
    fieldnames = list(records[0])
    if any(list(record) != fieldnames for record in records):
        raise RuntimeError("completed group records have mixed schemas")
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    os.replace(temporary, path)

def _group_failure_path(output_dir: Path, run_stem: str) -> Path:
    return output_dir / "traces" / f"{run_stem}.failure.json"

def _group_artifacts_exist(output_dir: Path, run_stem: str) -> bool:
    patterns = (
        ("jobs", f"{run_stem}_job*"),
        ("logs", f"{run_stem}_job*"),
        ("traces", f"{run_stem}.*"),
    )
    return any(
        any((output_dir / child).glob(pattern))
        for child, pattern in patterns
    )

def _coordinator_name(
    experiment_id: str,
    run_instance_id: str,
    run_stem: str,
) -> str:
    raw = f"credit-{experiment_id}-{run_instance_id}-{run_stem}"
    return re.sub(r"[^A-Za-z0-9_.-]", "-", raw)

def _run_instance_id(output_dir: Path) -> str:
    resolved = str(output_dir.resolve())
    digest = hashlib.sha256(resolved.encode("utf-8")).hexdigest()[:12]
    label = re.sub(r"[^A-Za-z0-9_.-]", "-", output_dir.name)
    return f"{label}-{digest}"

def _run_stem(
    scenario: SharedVllmScenario,
    identity: GroupRunIdentity,
) -> str:
    return (
        f"{identity.order_index:03d}_{identity.phase}_"
        f"{identity.repeat_index}_{scenario.scenario_id}"
    )

def _config_fingerprint(config: SharedVllmConfig, schedule) -> str:
    payload = {
        "config": _redacted_config(config),
        "schedule": [asdict(item) for item in schedule],
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

def _redacted_config(config: SharedVllmConfig) -> dict[str, object]:
    return {
        "experiment_id": config.experiment_id,
        "seed": config.seed,
        "warmup_runs_per_scenario": config.warmup_runs_per_scenario,
        "formal_repeats": config.formal_repeats,
        "endpoint_ids": config.endpoint_ids,
        "service_signature": dict(config.service_signature),
        "request_limit_per_endpoint": config.request_limit_per_endpoint,
        "work_limit_per_endpoint": config.work_limit_per_endpoint,
        "credit_quantum": config.credit_quantum,
        "shared_credit_namespace": config.shared_credit_namespace,
        "gpu_peak_tflops": config.gpu_peak_tflops,
        "mfu_precision": config.mfu_precision,
        "common_args": _redact_command(list(config.common_args)),
        "scenarios": [asdict(item) for item in config.scenarios],
        "service_metadata": dict(config.service_metadata),
        "fail_closed_rehearsal": config.fail_closed_rehearsal,
        "ready_observation_contract": config.ready_observation_contract,
        "ready_payload_bytes_limit_per_job": (
            config.ready_payload_bytes_limit_per_job
        ),
        "calibration_contract": (
            {
                "path": config.calibration_contract.path,
                "sha256": config.calibration_contract.sha256,
                "selection": dict(config.calibration_contract.selection),
            }
            if config.calibration_contract is not None
            else None
        ),
        "state_aware_control": (
            asdict(config.state_aware_control)
            if config.state_aware_control is not None
            else None
        ),
        "saor_capacity_control": (
            asdict(config.saor_capacity_control)
            if config.saor_capacity_control is not None
            else None
        ),
        "saor_release_control": (
            asdict(config.saor_release_control)
            if config.saor_release_control is not None
            else None
        ),
    }

def _redact_command(command: list[str]) -> list[str]:
    secret_flags = {
        "--completion-api-key",
        "--database-url",
        "--embedding-api-key",
    }
    redacted = []
    redact_next = False
    for item in command:
        if redact_next:
            redacted.append("***")
            redact_next = False
            continue
        flag, separator, _ = item.partition("=")
        if separator and flag in secret_flags:
            redacted.append(f"{flag}=***")
            continue
        redacted.append(item)
        redact_next = item in secret_flags
    return redacted

def _repository_commit() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=_CODE_ROOT.parent,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()

def _load_resume_manifest(path: Path, expected: dict) -> dict:
    if not path.exists():
        raise ValueError("--resume requires an existing manifest.json")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    for key in (
        "schema_version",
        "experiment_id",
        "config_fingerprint",
        "repository_commit",
        "run_instance_id",
        "redacted_config",
        "schedule",
    ):
        expected_value = json.loads(json.dumps(expected[key]))
        if manifest.get(key) != expected_value:
            raise ValueError(f"resume manifest does not match {key}")
    if not isinstance(manifest.get("completed_runs"), list):
        raise ValueError("resume manifest has invalid completed_runs")
    if not isinstance(manifest.get("incidents"), list):
        raise ValueError("resume manifest has invalid incidents")
    return manifest

def _write_json_atomic(path: Path, payload: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, path)
