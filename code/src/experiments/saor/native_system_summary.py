"""Validate stored SAOR matched-system evidence and emit two separate summaries."""

from __future__ import annotations

import csv
import json
import math
import statistics
from pathlib import Path
from typing import Any

from .native_system_matched import (
    FORMAL_AUTHORIZATION_SCOPE,
    REQUIRED_ARM_IDS,
    SELECTOR_SANITY_ARM_IDS,
    SYSTEM_ARM_IDS,
    sha256_file,
    sha256_payload,
    validate_native_final_queue,
    validate_project_final_credit,
)


FORMAL_REPEATS = 3
_RANKING_OUTPUT_NAMES = (
    "system_summary.csv", "project_selector_sanity.csv",
    "job_summary.csv", "resource_summary.csv",
)
_OUTPUT_NAMES = ("all_runs.csv", *_RANKING_OUTPUT_NAMES, "validation.json")
_PROJECT_FLAG_FRAGMENTS = (
    "credit", "coordinator", "router", "bounded-ready", "bounded_ready",
    "max-active-work", "max_active_work", "ready-observation", "ready_observation",
)


def _validation(
    status: str,
    errors: list[str] | None = None,
    *,
    formal_authorization_verified: bool = False,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "status": status,
        "comparison_scope": "complete_system_empirical_plus_project_internal_sanity",
        "selector_victory_decided": False,
        "formal_authorized": formal_authorization_verified,
        "native_baseline_count": 3,
        "project_control_count": 5,
    }
    if errors:
        payload["errors"] = errors
    return payload


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise ValueError(f"refusing to write empty CSV: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _remove_named_generation(directory: Path) -> None:
    for name in _OUTPUT_NAMES:
        path = directory / name
        if path.is_file():
            path.unlink()


def _remove_staging(staging_dir: Path) -> None:
    _remove_named_generation(staging_dir)
    if staging_dir.is_dir():
        staging_dir.rmdir()


def _publish_failed_validation(
    output_dir: Path,
    audit_rows: list[dict[str, object]],
    errors: list[str],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    _publish_validation(output_dir, "failing")
    for name in _RANKING_OUTPUT_NAMES:
        path = output_dir / name
        if path.is_file():
            path.unlink()
    audit_temporary = output_dir / ".all_runs.csv.failed.tmp"
    if audit_temporary.is_file():
        audit_temporary.unlink()
    _write_csv(audit_temporary, audit_rows)
    audit_temporary.replace(output_dir / "all_runs.csv")
    temporary = output_dir / ".validation.json.failed.tmp"
    if temporary.is_file():
        temporary.unlink()
    _write_json(temporary, _validation("failed", errors))
    temporary.replace(output_dir / "validation.json")


def _publish_validation(output_dir: Path, status: str) -> None:
    temporary = output_dir / f".validation.json.{status}.tmp"
    if temporary.is_file():
        temporary.unlink()
    _write_json(temporary, _validation(status))
    temporary.replace(output_dir / "validation.json")


def _finite(value: object, name: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be finite")
    return parsed


def _positive(value: object, name: str) -> float:
    parsed = _finite(value, name)
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _decode_json_value(value: object, run_id: str, field: str) -> object:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError(f"{run_id} {field} is not valid JSON") from error
    return value


def _native_queue_empty(value: object, run_id: str) -> None:
    validate_native_final_queue(value, run_id)


def _project_credit_empty(value: object, run_id: str, *, frozen_static: bool) -> None:
    validate_project_final_credit(value, run_id, frozen_static=frozen_static)


def _validate_source(job: dict[str, Any], run_id: str) -> None:
    provenance = job.get("shard_provenance")
    if not isinstance(provenance, list) or not provenance:
        raise ValueError(f"{run_id} lacks PostgreSQL source provenance")
    for shard in provenance:
        if not isinstance(shard, dict) or any(
            shard.get(name) != expected
            for name, expected in {
                "source_kind": "timed_postgres_manifest",
                "source_timing_boundary": "inside_job_barrier",
                "source_validation_status": "ok",
            }.items()
        ):
            raise ValueError(f"{run_id} source timing is outside the cell")


def _availability_metric(
    payload: dict[str, Any], metric: str, value_name: str, run_id: str,
) -> tuple[str, object, str]:
    raw = payload.get(metric, payload)
    if not isinstance(raw, dict):
        raise ValueError(f"{run_id} {metric} availability has an invalid schema")
    status = str(raw.get("status", ""))
    reason = str(raw.get("reason", ""))
    if set(raw) != {"status", "value", "reason"}:
        raise ValueError(f"{run_id} {metric} must contain status/value/reason")
    raw_value = raw.get("value")
    if status == "unavailable":
        if not reason or raw_value not in (None, "", "unavailable"):
            raise ValueError(f"{run_id} {metric} unavailable contract is invalid")
        return status, "unavailable", reason
    if status != "available" or raw_value in (None, "", "unavailable"):
        raise ValueError(f"{run_id} {metric} availability is invalid")
    value = _finite(raw_value, f"{run_id} {metric} {value_name}")
    if metric == "request_p99" and value < 0:
        raise ValueError(f"{run_id} request_p99_s must be nonnegative")
    if metric == "slo" and not 0 <= value <= 1:
        raise ValueError(f"{run_id} slo_violation_ratio must be in [0, 1]")
    return status, value, reason


def _resource_row(
    cell: dict[str, Any], start: float, end: float, run_id: str
) -> dict[str, object]:
    resource = cell.get("resource_metrics")
    if not isinstance(resource, dict) or resource.get("resource_metrics_status") != "ok":
        raise ValueError(f"{run_id} resource metrics are unavailable")
    path = Path(str(resource.get("path", "")))
    if not path.is_file() or path.stat().st_size <= 0:
        raise ValueError(f"{run_id} resource trace is missing")
    with path.open(encoding="utf-8", newline="") as stream:
        samples = list(csv.DictReader(stream))
    absolute = [
        row for row in samples
        if row.get("observed_epoch_s") not in (None, "")
        and start <= _finite(row["observed_epoch_s"], "resource timestamp") <= end
    ]
    relative = [
        row for row in samples
        if row.get("sample_epoch_s") not in (None, "")
        and 0 <= _finite(row["sample_epoch_s"], "resource timestamp") <= end - start
    ]
    in_boundary = absolute or relative
    if not in_boundary:
        raise ValueError(f"{run_id} resource trace has no in-boundary sample")

    def mean(name: str) -> object:
        values = [
            _finite(row[name], name) for row in in_boundary
            if row.get(name) not in (None, "")
        ]
        return statistics.fmean(values) if values else "unavailable"

    power = mean("gpu_power_w")

    return {
        "run_id": run_id,
        "arm_id": cell["arm_id"],
        "sample_count": len(in_boundary),
        "gpu_utilization_pct_mean": mean("gpu_utilization_pct"),
        "power_w_mean": power,
        "energy_j": (
            power * (end - start) if power != "unavailable" else "unavailable"
        ),
        "mfu_fraction_mean": mean("mfu_fraction"),
        "vllm_running_mean": mean("running"),
        "vllm_waiting_mean": mean("waiting"),
        "vllm_kv_cache_usage_mean": mean("kv_usage"),
    }


def _normalize_cell(
    cell: object,
) -> tuple[dict[str, object], list[dict[str, object]], dict[str, object]]:
    if not isinstance(cell, dict):
        raise ValueError("matrix cell must be an object")
    arm_id = str(cell.get("arm_id", ""))
    phase = str(cell.get("phase", ""))
    repeat = int(cell.get("repeat", 0))
    order_index = int(cell.get("order_index", -1))
    run_id = str(
        cell.get("run_id")
        or cell.get("run_instance_id")
        or f"{phase}-{repeat}-{order_index}-{arm_id}"
    )
    if not run_id:
        raise ValueError("matrix cell lacks run_id")
    if arm_id not in REQUIRED_ARM_IDS:
        raise ValueError(f"{run_id} has unknown arm {arm_id!r}")
    if cell.get("status") != "passed" or cell.get("exactly_once") is not True:
        raise ValueError(f"{run_id} failed status/exactly-once gate")
    if (
        phase not in {"warmup", "formal", "selector_sanity_development"}
        or repeat < 1
    ):
        raise ValueError(f"{run_id} has invalid phase/repeat")
    start = _finite(cell.get("start_epoch_s"), f"{run_id} start")
    end = _finite(cell.get("end_epoch_s"), f"{run_id} end")
    duration = _positive(
        cell.get("database_operator_e2e_s"), f"{run_id} database_operator_e2e_s"
    )
    if end <= start:
        raise ValueError(f"{run_id} has invalid common timing boundary")
    if arm_id in SYSTEM_ARM_IDS[:3] and "queue_final" in cell:
        _native_queue_empty(cell["queue_final"], run_id)
    elif arm_id not in SYSTEM_ARM_IDS[:3] and "shared_credit_final" in cell:
        _project_credit_empty(
            cell["shared_credit_final"], run_id,
            frozen_static=arm_id == "project_frozen_static",
        )
    else:
        raise ValueError(f"{run_id} lacks its system-specific final queue evidence")

    service = cell.get("service_metrics")
    if not isinstance(service, dict) or service.get("metrics_status") != "ok":
        raise ValueError(f"{run_id} service counters cannot be attributed")
    prompt = _finite(service.get("prompt_tokens_delta"), f"{run_id} prompt delta")
    generation = _finite(
        service.get("generation_tokens_delta"), f"{run_id} generation delta"
    )
    if prompt < 0 or generation < 0 or prompt + generation <= 0:
        raise ValueError(f"{run_id} service counter delta is invalid")

    jobs = cell.get("jobs")
    if not isinstance(jobs, list) or len(jobs) != 2:
        raise ValueError(f"{run_id} must contain exactly two Jobs")
    scheduled_starts: list[float] = []
    actual_starts: list[float] = []
    ends: list[float] = []
    for job in jobs:
        if not isinstance(job, dict) or job.get("exactly_once") is not True:
            raise ValueError(f"{run_id} has invalid Job exactly-once evidence")
        if (
            int(job.get("completed_count", -1)) <= 0
            or int(job.get("completed_count", -1)) != int(job.get("expected_count", -2))
        ):
            raise ValueError(f"{run_id} has invalid Job row accounting")
        _validate_source(job, run_id)
        scheduled_starts.append(
            _finite(
                job.get("scheduled_launch_epoch_s"),
                f"{run_id} Job scheduled release",
            )
        )
        actual_starts.append(
            _finite(job.get("actual_launch_epoch_s"), f"{run_id} Job actual launch")
        )
        ends.append(_finite(job.get("ended_epoch_s"), f"{run_id} Job completion"))
    if any(completion <= release for completion, release in zip(ends, scheduled_starts)):
        raise ValueError(f"{run_id} completion precedes scheduled release")
    overlap = min(ends) - max(actual_starts)
    if overlap <= 0:
        raise ValueError(f"{run_id} has non-positive Job overlap")

    request_tail = cell.get("request_tail_status")
    if not isinstance(request_tail, dict):
        raise ValueError(f"{run_id} lacks request-tail availability")
    p99_status, p99_value, p99_reason = _availability_metric(
        request_tail, "request_p99", "p99_s", run_id
    )
    slo_status, slo_value, slo_reason = _availability_metric(
        request_tail, "slo", "violation_ratio", run_id
    )
    if arm_id in SYSTEM_ARM_IDS[:3] and (
        p99_status != "unavailable" or slo_status != "unavailable"
    ):
        raise ValueError(f"{run_id} native tails must be unavailable with reasons")

    command = cell.get("command", [])
    command_text = " ".join(str(item).lower() for item in command)
    if arm_id in SYSTEM_ARM_IDS[:3] and any(
        fragment in command_text for fragment in _PROJECT_FLAG_FRAGMENTS
    ):
        raise ValueError(f"{run_id} native command contains Project flags")

    total = prompt + generation
    run_row = {
        "run_id": run_id,
        "arm_id": arm_id,
        "phase": phase,
        "repeat": repeat,
        "order_index": order_index,
        "scheduler_owner": str(cell.get("scheduler_owner", "")),
        "implementation_source": str(cell.get("implementation_source", "")),
        "report_blocks": json.dumps(cell.get("report_blocks", [])),
        "database_operator_e2e_s": duration,
        "service_prompt_tokens": prompt,
        "service_generation_tokens": generation,
        "service_total_tokens": total,
        "service_tokens_per_s": total / duration,
        "request_p99_status": p99_status,
        "request_p99_s": p99_value,
        "request_p99_reason": p99_reason,
        "slo_status": slo_status,
        "slo_violation_ratio": slo_value,
        "slo_reason": slo_reason,
        "scheduled_launch_epoch_s": json.dumps(scheduled_starts),
        "actual_launch_epoch_s": json.dumps(actual_starts),
        "scheduled_launch_offset_s": json.dumps([
            value - scheduled_starts[0] for value in scheduled_starts
        ]),
        "actual_launch_offset_s": json.dumps([
            value - actual_starts[0] for value in actual_starts
        ]),
        "launch_deviation_s": json.dumps([
            actual - scheduled
            for actual, scheduled in zip(actual_starts, scheduled_starts)
        ]),
        "exactly_once": True,
    }
    job_rows = [
        {
            "run_id": run_id,
            "arm_id": arm_id,
            "repeat": repeat,
            "job_role": role,
            "scheduled_release_epoch_s": scheduled,
            "actual_launch_epoch_s": actual,
            "scheduled_launch_offset_s": scheduled - scheduled_starts[0],
            "actual_launch_offset_s": actual - actual_starts[0],
            "launch_deviation_s": actual - scheduled,
            "completion_epoch_s": completion,
            "job_jct_s": completion - scheduled,
            "overlap_s": overlap,
            "completion_order": 1 + sorted(ends).index(completion),
        }
        for role, scheduled, actual, completion in zip(
            ("bulk", "foreground"), scheduled_starts, actual_starts, ends
        )
    ]
    return run_row, job_rows, _resource_row(cell, start, end, run_id)


def _sample_cv(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if mean else 0.0


def _summary_row(
    arm_id: str,
    rows: list[dict[str, object]],
    jobs: list[dict[str, object]],
    role: str,
) -> dict[str, object]:
    ordered = sorted(rows, key=lambda row: int(row["repeat"]))
    throughput = [float(row["service_tokens_per_s"]) for row in ordered]
    duration = [float(row["database_operator_e2e_s"]) for row in ordered]
    run_ids = [str(row["run_id"]) for row in ordered]
    selected_jobs = [row for row in jobs if str(row["run_id"]) in run_ids]

    def job_values(role_name: str, key: str) -> list[float]:
        by_id = {
            str(row["run_id"]): float(row[key]) for row in selected_jobs
            if row["job_role"] == role_name
        }
        return [by_id[run_id] for run_id in run_ids]

    bulk_jct = job_values("bulk", "job_jct_s")
    foreground_jct = job_values("foreground", "job_jct_s")
    overlap = job_values("bulk", "overlap_s")
    availability: dict[str, tuple[set[str], set[str]]] = {}
    for metric in ("request_p99", "slo"):
        statuses = {str(row[f"{metric}_status"]) for row in ordered}
        reasons = {str(row[f"{metric}_reason"]) for row in ordered}
        if len(statuses) != 1 or len(reasons) != 1:
            raise ValueError(f"{arm_id} {metric} availability drifted")
        availability[metric] = statuses, reasons

    def optional_metric_summary(metric: str, value: str) -> tuple[object, str]:
        raw = [row[value] for row in ordered]
        if next(iter(availability[metric][0])) == "unavailable":
            return "unavailable", json.dumps(raw)
        values = [float(item) for item in raw]
        return statistics.fmean(values), json.dumps(values)

    p99_mean, p99_repeats = optional_metric_summary("request_p99", "request_p99_s")
    slo_mean, slo_repeats = optional_metric_summary("slo", "slo_violation_ratio")
    return {
        "arm_id": arm_id,
        "scheduler_owner": ordered[0]["scheduler_owner"],
        "report_role": role,
        "formal_repeats": len(ordered),
        "physical_run_ids": json.dumps([row["run_id"] for row in ordered]),
        "service_tokens_per_s_mean": statistics.fmean(throughput),
        "service_tokens_per_s_sample_cv": _sample_cv(throughput),
        "service_tokens_per_s_repeats": json.dumps(throughput),
        "database_operator_e2e_s_mean": statistics.fmean(duration),
        "database_operator_e2e_s_sample_cv": _sample_cv(duration),
        "database_operator_e2e_s_repeats": json.dumps(duration),
        "bulk_jct_s_mean": statistics.fmean(bulk_jct),
        "bulk_jct_s_sample_cv": _sample_cv(bulk_jct),
        "bulk_jct_s_repeats": json.dumps(bulk_jct),
        "foreground_jct_s_mean": statistics.fmean(foreground_jct),
        "foreground_jct_s_sample_cv": _sample_cv(foreground_jct),
        "foreground_jct_s_repeats": json.dumps(foreground_jct),
        "overlap_s_mean": statistics.fmean(overlap),
        "overlap_s_sample_cv": _sample_cv(overlap),
        "overlap_s_repeats": json.dumps(overlap),
        "request_p99_status": next(iter(availability["request_p99"][0])),
        "request_p99_s_mean": p99_mean,
        "request_p99_s_repeats": p99_repeats,
        "request_p99_reason": next(iter(availability["request_p99"][1])),
        "slo_status": next(iter(availability["slo"][0])),
        "slo_violation_ratio_mean": slo_mean,
        "slo_violation_ratio_repeats": slo_repeats,
        "slo_reason": next(iter(availability["slo"][1])),
    }


def _resource_summary_row(
    arm_id: str,
    runs: list[dict[str, object]],
    resources: list[dict[str, object]],
) -> dict[str, object]:
    ordered = sorted(runs, key=lambda row: int(row["repeat"]))
    by_id = {str(row["run_id"]): row for row in resources}
    fields = (
        "gpu_utilization_pct_mean", "power_w_mean", "energy_j",
        "mfu_fraction_mean",
        "vllm_running_mean", "vllm_waiting_mean", "vllm_kv_cache_usage_mean",
    )
    output: dict[str, object] = {
        "arm_id": arm_id,
        "physical_run_ids": json.dumps([row["run_id"] for row in ordered]),
        "formal_repeats": len(ordered),
    }
    for field in fields:
        raw = [by_id[str(row["run_id"])][field] for row in ordered]
        output[f"{field}_repeats"] = json.dumps(raw)
        if any(value == "unavailable" for value in raw):
            output[f"{field}_aggregate_status"] = "unavailable"
            output[f"{field}_aggregate_reason"] = "one or more runs lack the metric"
            output[f"{field}_aggregate_mean"] = "unavailable"
            output[f"{field}_sample_cv"] = "unavailable"
        else:
            values = [float(value) for value in raw]
            output[f"{field}_aggregate_status"] = "available"
            output[f"{field}_aggregate_reason"] = ""
            output[f"{field}_aggregate_mean"] = statistics.fmean(values)
            output[f"{field}_sample_cv"] = _sample_cv(values)
    return output


_AUDIT_FIELDS = (
    "run_id", "arm_id", "phase", "repeat", "order_index", "scheduler_owner",
    "implementation_source", "report_blocks", "status", "failure_reason",
    "validation_status", "repository_commit", "config_sha256",
    "config_fingerprint", "authorization_sha256", "manifest_path",
    "manifest_sha256", "service_signature", "database_operator_e2e_s",
    "service_prompt_tokens", "service_generation_tokens", "service_total_tokens",
    "service_tokens_per_s", "request_p99_status", "request_p99_s",
    "request_p99_reason", "slo_status", "slo_violation_ratio", "slo_reason",
    "scheduled_launch_epoch_s", "actual_launch_epoch_s",
    "scheduled_launch_offset_s", "actual_launch_offset_s", "launch_deviation_s",
    "exactly_once",
)


def _physical_id(cell: dict[str, Any]) -> str:
    return str(
        cell.get("run_id") or cell.get("run_instance_id")
        or (
            f"{cell.get('phase')}-{cell.get('repeat')}-"
            f"{cell.get('order_index')}-{cell.get('arm_id')}"
        )
    )


def _audit_row(
    cell: object,
    *,
    validation_status: str,
    failure_reason: str,
    normalized: dict[str, object] | None = None,
) -> dict[str, object]:
    raw = cell if isinstance(cell, dict) else {}
    row = {field: "" for field in _AUDIT_FIELDS}
    row.update(
        {
            "run_id": _physical_id(raw),
            "arm_id": str(raw.get("arm_id", "")),
            "phase": str(raw.get("phase", "")),
            "repeat": raw.get("repeat", ""),
            "order_index": raw.get("order_index", ""),
            "scheduler_owner": str(raw.get("scheduler_owner", "")),
            "implementation_source": str(raw.get("implementation_source", "")),
            "report_blocks": json.dumps(raw.get("report_blocks", [])),
            "status": str(raw.get("status", "invalid_cell")),
            "failure_reason": str(raw.get("error", "")) or failure_reason,
            "validation_status": validation_status,
            "repository_commit": str(raw.get("repository_commit", "")),
            "config_sha256": str(raw.get("config_sha256", "")),
            "config_fingerprint": str(raw.get("config_fingerprint", "")),
            "authorization_sha256": str(raw.get("authorization_sha256", "")),
            "manifest_path": str(raw.get("manifest_path", "")),
            "manifest_sha256": str(raw.get("manifest_sha256", "")),
            "service_signature": json.dumps(
                raw.get("service_signature", {}), sort_keys=True
            ),
            "exactly_once": raw.get("exactly_once", ""),
        }
    )
    if normalized is not None:
        row.update(normalized)
        row["status"] = str(raw.get("status", "passed"))
        row["failure_reason"] = failure_reason
        row["validation_status"] = validation_status
    return row


def _failed_audit_rows(
    raw_cells: object,
    failure_reason: str,
) -> list[dict[str, object]]:
    cells = raw_cells if isinstance(raw_cells, list) else []
    rows = [
        _audit_row(
            cell,
            validation_status="failed",
            failure_reason=failure_reason,
        )
        for cell in cells
    ]
    if rows:
        return rows
    return [
        _audit_row(
            {},
            validation_status="failed",
            failure_reason=failure_reason,
        )
    ]


def _load_authorized_identity(
    matrix_root: Path,
    index: dict[str, object],
    raw_cells: list[dict[str, Any]],
    authorization_path: Path,
) -> dict[str, dict[str, object]]:
    """Recompute every frozen identity before publishing any ranking."""

    authorization = json.loads(authorization_path.read_text(encoding="utf-8"))
    required_fields = {
        "schema_version", "status", "scope", "formal_authorized",
        "repository_commit", "config_sha256", "resolved_config_sha256",
        "manifest_sha256",
    }
    if not isinstance(authorization, dict) or set(authorization) != required_fields:
        raise ValueError("formal authorization schema is invalid")
    if (
        authorization.get("schema_version") != 1
        or authorization.get("status") != "authorized"
        or authorization.get("scope") != FORMAL_AUTHORIZATION_SCOPE
        or authorization.get("formal_authorized") is not True
    ):
        raise ValueError("formal authorization is not active for this scope")
    authorization_sha256 = sha256_file(authorization_path)

    snapshot_path = matrix_root / "matrix_contract_snapshot.json"
    if sha256_file(snapshot_path) != str(index.get("contract_snapshot_sha256", "")):
        raise ValueError("matrix contract snapshot SHA drifted")
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    if not isinstance(snapshot, dict) or set(snapshot) != {
        "schema_version", "runtime_identity", "resolved_config"
    }:
        raise ValueError("matrix contract snapshot schema is invalid")
    runtime = snapshot.get("runtime_identity")
    resolved = snapshot.get("resolved_config")
    if not isinstance(runtime, dict) or not isinstance(resolved, dict):
        raise ValueError("matrix contract snapshot identity is invalid")
    if sha256_payload(resolved) != authorization.get("resolved_config_sha256"):
        raise ValueError("resolved config fingerprint drifted")
    identity_fields = (
        "repository_commit", "config_sha256", "resolved_config_sha256",
        "manifest_sha256",
    )
    for field in identity_fields:
        if runtime.get(field) != authorization.get(field):
            raise ValueError(f"runtime {field} drifted from authorization")
    if (
        runtime.get("execution_mode") != "formal"
        or runtime.get("status") != "authorized"
        or runtime.get("formal_authorized") is not True
        or runtime.get("authorization_sha256") != authorization_sha256
    ):
        raise ValueError("runtime authorization identity is invalid")
    expected_index = {
        "repository_commit": authorization["repository_commit"],
        "config_sha256": authorization["config_sha256"],
        "config_fingerprint": authorization["resolved_config_sha256"],
        "manifest_sha256": authorization["manifest_sha256"],
        "authorization_sha256": authorization_sha256,
        "execution_mode": "formal",
    }
    for field, expected in expected_index.items():
        if index.get(field) != expected:
            raise ValueError(f"matrix index {field} drifted")

    arms = resolved.get("arms")
    if not isinstance(arms, list):
        raise ValueError("resolved config lacks arms")
    by_arm = {
        str(arm.get("arm_id", "")): arm
        for arm in arms
        if isinstance(arm, dict)
    }
    if set(by_arm) != set(REQUIRED_ARM_IDS):
        raise ValueError("resolved arm identity is incomplete")
    scheduler_owners = {
        arm_id: str(arm["scheduler_owner"]) for arm_id, arm in by_arm.items()
    }
    if index.get("scheduler_owners") != scheduler_owners:
        raise ValueError("matrix scheduler-owner identity drifted")
    service_signatures = {
        json.dumps(arm.get("service_signature", {}), sort_keys=True)
        for arm in by_arm.values()
    }
    if len(service_signatures) != 1 or index.get("service_signature") != next(
        iter(by_arm.values())
    ).get("service_signature"):
        raise ValueError("matrix service signature drifted")
    for arm in by_arm.values():
        manifest_path = Path(str(arm.get("manifest_path", "")))
        if (
            str(arm.get("manifest_sha256", ""))
            != authorization["manifest_sha256"]
            or sha256_file(manifest_path) != authorization["manifest_sha256"]
        ):
            raise ValueError("frozen manifest identity drifted")

    schedule = index.get("schedule")
    if not isinstance(schedule, list):
        raise ValueError("matrix index lacks frozen schedule")
    if index.get("status") == "completed" and len(schedule) != len(raw_cells):
        raise ValueError("completed matrix schedule/cell shape drifted")
    if len(raw_cells) > len(schedule):
        raise ValueError("matrix contains cells outside the frozen schedule")
    for position, cell in enumerate(raw_cells):
        scheduled = schedule[position]
        if not isinstance(scheduled, dict):
            raise ValueError("matrix schedule entry is invalid")
        for field in ("arm_id", "phase", "repeat", "order_index"):
            if cell.get(field) != scheduled.get(field):
                raise ValueError(f"cell {position} {field} drifted from schedule")
        arm = by_arm.get(str(cell.get("arm_id", "")))
        if arm is None:
            raise ValueError(f"cell {position} references an unknown arm")
        expected_cell = {
            "repository_commit": authorization["repository_commit"],
            "config_sha256": authorization["config_sha256"],
            "config_fingerprint": authorization["resolved_config_sha256"],
            "authorization_sha256": authorization_sha256,
            "manifest_path": arm["manifest_path"],
            "manifest_sha256": arm["manifest_sha256"],
            "service_signature": arm["service_signature"],
            "scheduler_owner": arm["scheduler_owner"],
        }
        for field, expected in expected_cell.items():
            if cell.get(field) != expected:
                raise ValueError(f"cell {position} {field} identity drifted")
    return by_arm


def summarize_matched_system(
    matrix_root: Path,
    output_dir: Path,
    *,
    formal_authorization_path: Path,
) -> bool:
    """Summarize only stored matrix evidence; return false on every contract failure."""

    output_dir.mkdir(parents=True, exist_ok=True)
    staging_dir = output_dir.parent / f".{output_dir.name}.matched-summary-staging"
    raw_cells: object = []
    try:
        _remove_staging(staging_dir)
        index = json.loads(
            (matrix_root / "matrix_index.json").read_text(encoding="utf-8")
        )
        if not isinstance(index, dict):
            raise ValueError("matrix index must be an object")
        raw_cells = index.get("cells")
        if not isinstance(raw_cells, list) or not all(
            isinstance(cell, dict) for cell in raw_cells
        ):
            raise ValueError("matrix cells must be a list")
        _load_authorized_identity(
            matrix_root,
            index,
            raw_cells,
            formal_authorization_path,
        )
        if index.get("status") != "completed":
            raise ValueError("matrix index is not completed")
        formal_cells = [
            cell for cell in raw_cells
            if isinstance(cell, dict) and cell.get("phase") == "formal"
        ]
        development_cells = [
            cell for cell in raw_cells
            if isinstance(cell, dict)
            and cell.get("phase") == "selector_sanity_development"
        ]
        repeat_contract = index.get("repeat_contract", {})
        if not isinstance(repeat_contract, dict):
            raise ValueError("matrix index lacks repeat contract")
        formal_repeats = int(repeat_contract.get("formal", 0))
        development_repeats = int(
            repeat_contract.get("selector_sanity_development", 0)
        )
        if formal_repeats != FORMAL_REPEATS:
            raise ValueError("formal matrix must retain the frozen three repeats")
        if development_repeats < 1 or development_repeats > formal_repeats:
            raise ValueError("selector development repeat contract is invalid")

        run_ids = [
            _physical_id(cell) for cell in raw_cells
        ]
        if len(run_ids) != len(set(run_ids)) or any(not item for item in run_ids):
            raise ValueError("formal run IDs must be non-empty and unique")
        observed = {
            arm_id: sorted(
                int(cell.get("repeat", 0)) for cell in formal_cells
                if cell.get("arm_id") == arm_id
            )
            for arm_id in SYSTEM_ARM_IDS
        }
        if any(
            repeats != list(range(1, formal_repeats + 1))
            for repeats in observed.values()
        ):
            raise ValueError("every system arm must have all formal repeats")
        if len(formal_cells) != len(SYSTEM_ARM_IDS) * formal_repeats:
            raise ValueError("formal matrix has an invalid system-arm shape")
        development_arm_ids = tuple(
            arm_id for arm_id in SELECTOR_SANITY_ARM_IDS
            if arm_id != "project_bounded_ready_saor_0125we"
        )
        development_observed = {
            arm_id: sorted(
                int(cell.get("repeat", 0)) for cell in development_cells
                if cell.get("arm_id") == arm_id
            )
            for arm_id in development_arm_ids
        }
        if any(
            repeats != list(range(1, development_repeats + 1))
            for repeats in development_observed.values()
        ) or len(development_cells) != len(development_arm_ids) * development_repeats:
            raise ValueError("selector development matrix has an invalid control-arm shape")

        project_limits = {
            (
                int(cell.get("request_limit_per_endpoint", -1)),
                int(cell.get("work_limit_per_endpoint", -1)),
            )
            for cell in formal_cells + development_cells
            if cell.get("arm_id") not in SYSTEM_ARM_IDS[:3]
        }
        if (
            len(project_limits) != 1
            or next(iter(project_limits))[0] <= 0
            or next(iter(project_limits))[1] <= 0
        ):
            raise ValueError("Project K/W contract drifted or is invalid")

        run_rows: list[dict[str, object]] = []
        job_rows: list[dict[str, object]] = []
        resource_rows: list[dict[str, object]] = []
        for cell in raw_cells:
            normalized, jobs, resource = _normalize_cell(cell)
            run_rows.append(
                _audit_row(
                    cell,
                    validation_status="passed",
                    failure_reason="",
                    normalized=normalized,
                )
            )
            job_rows.extend(jobs)
            resource_rows.append(resource)
        phase_order = {"warmup": 0, "formal": 1, "selector_sanity_development": 2}
        run_rows.sort(key=lambda row: (
            phase_order[str(row["phase"])], int(row["repeat"]),
            int(row["order_index"]),
        ))
        job_rows.sort(
            key=lambda row: (
                int(row["repeat"]), str(row["run_id"]), str(row["job_role"])
            )
        )
        resource_rows.sort(key=lambda row: str(row["run_id"]))

        def rows_for(arm_id: str) -> list[dict[str, object]]:
            return [
                row for row in run_rows
                if row["arm_id"] == arm_id and row["phase"] == "formal"
            ]

        def selector_rows_for(arm_id: str) -> list[dict[str, object]]:
            phase = (
                "formal"
                if arm_id == "project_bounded_ready_saor_0125we"
                else "selector_sanity_development"
            )
            return [
                row for row in run_rows
                if row["arm_id"] == arm_id
                and row["phase"] == phase
                and int(row["repeat"]) <= development_repeats
            ]

        system = [
            _summary_row(
                arm_id, rows_for(arm_id), job_rows, "complete_system_empirical"
            )
            for arm_id in SYSTEM_ARM_IDS
        ]
        selector = [
            _summary_row(
                arm_id, selector_rows_for(arm_id), job_rows, "project_internal_sanity"
            )
            for arm_id in SELECTOR_SANITY_ARM_IDS
        ]
        system_saor = next(row for row in system if row["arm_id"].endswith("0125we"))
        selector_saor = next(row for row in selector if row["arm_id"].endswith("0125we"))
        if (
            json.loads(str(system_saor["physical_run_ids"]))[:development_repeats]
            != json.loads(str(selector_saor["physical_run_ids"]))
            or json.loads(str(system_saor["service_tokens_per_s_repeats"]))[
                :development_repeats
            ] != json.loads(str(selector_saor["service_tokens_per_s_repeats"]))
        ):
            raise ValueError("SAOR reports do not originate from the same physical runs")

        staging_dir.mkdir()
        _write_csv(staging_dir / "all_runs.csv", run_rows)
        _write_csv(staging_dir / "system_summary.csv", system)
        _write_csv(staging_dir / "project_selector_sanity.csv", selector)
        _write_csv(staging_dir / "job_summary.csv", job_rows)
        _write_csv(
            staging_dir / "resource_summary.csv",
            [
                _resource_summary_row(
                    arm_id,
                    rows_for(arm_id) if arm_id in SYSTEM_ARM_IDS
                    else selector_rows_for(arm_id),
                    resource_rows,
                )
                for arm_id in REQUIRED_ARM_IDS
            ],
        )
        _write_json(
            staging_dir / "validation.json",
            _validation("passed", formal_authorization_verified=True),
        )
        _publish_validation(output_dir, "publishing")
        for name in _OUTPUT_NAMES[:-1]:
            (staging_dir / name).replace(output_dir / name)
        (staging_dir / "validation.json").replace(output_dir / "validation.json")
        staging_dir.rmdir()
        return True
    # Engineering decision: no ordinary validation/publish bug may leave an old
    # passed ranking visible. KeyboardInterrupt/SystemExit still propagate.
    except Exception as error:
        _remove_staging(staging_dir)
        _publish_failed_validation(
            output_dir,
            _failed_audit_rows(raw_cells, str(error)),
            [str(error)],
        )
        return False
