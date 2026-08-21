"""Validate stored SAOR DB-E2E evidence and emit one five-arm summary."""

from __future__ import annotations

import csv
import json
import math
import statistics
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from src.baselines.common.database_identity import DatabaseIdentity
from src.baselines.common.manifests import read_manifest
from src.baselines.common.redact import redact_text

from .native_system_matched import (
    FORMAL_AUTHORIZATION_SCOPE,
    REQUIRED_ARM_IDS,
    SYSTEM_ARM_IDS,
    sha256_file,
    sha256_payload,
    resolve_matrix_evidence_path,
    validate_native_final_queue,
    validate_project_final_credit,
)
from .native_system_publisher import (
    RANKING_OUTPUT_NAMES,
    publish_failed_generation,
)
from .native_system_validator import validate_uniform_cell_identity


FORMAL_REPEATS = 3
_RANKING_OUTPUT_NAMES = RANKING_OUTPUT_NAMES
_OUTPUT_NAMES = ("all_runs.csv", *_RANKING_OUTPUT_NAMES, "validation.json")
_PROJECT_FLAG_FRAGMENTS = (
    "credit", "coordinator", "router", "bounded-ready", "bounded_ready",
    "max-active-work", "max_active_work", "ready-observation", "ready_observation",
)
_FORMAL_AUTHORIZATION_FIELDS = {
    "schema_version", "status", "scope", "formal_authorized",
    "repository_commit", "config_sha256", "native_config_sha256",
    "project_config_sha256", "resolved_config_sha256", "manifest_sha256",
    "job_manifests", "mfu_contract", "rehearsal_evidence",
}
_FORMAL_IDENTITY_FIELDS = (
    "repository_commit", "config_sha256", "native_config_sha256",
    "project_config_sha256", "resolved_config_sha256", "manifest_sha256",
    "job_manifests", "mfu_contract", "rehearsal_evidence",
)


def _validate_formal_authorization_binding(
    authorization: object,
    runtime: dict[str, object],
) -> None:
    """Require the snapshot to bind every field in the exact formal artifact."""

    if (
        not isinstance(authorization, dict)
        or set(authorization) != _FORMAL_AUTHORIZATION_FIELDS
    ):
        raise ValueError("formal authorization schema is invalid")
    if (
        authorization.get("schema_version") != 1
        or authorization.get("status") != "authorized"
        or authorization.get("scope") != FORMAL_AUTHORIZATION_SCOPE
        or authorization.get("formal_authorized") is not True
    ):
        raise ValueError("formal authorization is not active for this scope")
    for field in _FORMAL_IDENTITY_FIELDS:
        if runtime.get(field) != authorization.get(field):
            raise ValueError(f"runtime {field} drifted from authorization")


def _command_flag_values(command: list[object], flag: str) -> list[str]:
    """Collect every value of a repeated flag from flattened command evidence."""

    values: list[str] = []
    for index, token in enumerate(command):
        if str(token) != flag:
            continue
        if index + 1 >= len(command):
            raise ValueError(f"stored command {flag} has no value")
        values.append(str(command[index + 1]))
    return values


def _validate_executor_command(
    command: list[object],
    arm_id: str,
    arm_identity: dict[str, object],
    observation_gateway: object,
) -> None:
    """Recheck the dispatch endpoints and native C/B/adapter from raw commands."""

    endpoints = arm_identity.get("matrix_endpoint_urls")
    if not isinstance(endpoints, list) or len(endpoints) != 2:
        raise ValueError(f"{arm_id} lacks frozen endpoint URL identity")
    if not isinstance(observation_gateway, dict):
        raise ValueError(f"{arm_id} lacks observation gateway evidence")
    raw_routes = observation_gateway.get("routes")
    if not isinstance(raw_routes, list):
        raise ValueError(f"{arm_id} lacks observation gateway routes")
    expected_route_bindings = {
        (f"job{job_index}", f"endpoint-{endpoint_index}", str(endpoint))
        for job_index in range(2)
        for endpoint_index, endpoint in enumerate(endpoints)
    }
    route_bindings = {
        (
            str(route.get("job_id", "")),
            str(route.get("endpoint_id", "")),
            str(route.get("upstream_url", "")),
        )
        for route in raw_routes
        if isinstance(route, dict)
    }
    if route_bindings != expected_route_bindings:
        raise ValueError(f"{arm_id} observation gateway upstream binding drifted")

    def validate_gateway_url(value: str, job_id: str, endpoint_id: str) -> str:
        parsed = urlsplit(value)
        expected_path = f"/observe/{job_id}/{endpoint_id}/v1/chat/completions"
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"127.0.0.1", "localhost"}
            or parsed.port is None
            or parsed.path != expected_path
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(f"{arm_id} stored gateway endpoint route drifted")
        return f"{parsed.scheme}://{parsed.netloc}"
    if arm_id in SYSTEM_ARM_IDS[:3]:
        selection = arm_identity.get("executor_selection")
        if not isinstance(selection, dict):
            raise ValueError(f"{arm_id} lacks native executor selection identity")
        for flag, field in (
            ("--adapter", "adapter"),
            ("--concurrency", "concurrency_per_endpoint"),
            ("--batch-size", "batch_size"),
        ):
            values = _command_flag_values(command, flag)
            if len(values) != 4 or any(
                value != str(selection.get(field)) for value in values
            ):
                raise ValueError(f"{arm_id} stored command {flag} drifted")
        endpoint_indices = _command_flag_values(command, "--endpoint-index")
        endpoint_values = _command_flag_values(command, "--endpoint-url")
        expected_pairs = [
            (str(index), f"job{job}", f"endpoint-{index}")
            for job in range(2)
            for index in range(2)
        ]
        observed_pairs = []
        origins = set()
        for index, (endpoint_index, endpoint_value) in enumerate(
            zip(endpoint_indices, endpoint_values, strict=True)
        ):
            _, job_id, endpoint_id = expected_pairs[index]
            origins.add(validate_gateway_url(endpoint_value, job_id, endpoint_id))
            observed_pairs.append((endpoint_index, job_id, endpoint_id))
        if observed_pairs != expected_pairs or len(origins) != 1:
            raise ValueError(f"{arm_id} stored endpoint-index mapping drifted")
        return

    endpoint_values = _command_flag_values(
        command, "--completion-endpoint-urls"
    )
    if len(endpoint_values) != 2:
        raise ValueError(f"{arm_id} stored Project endpoint routes are incomplete")
    origins = set()
    for job_index, csv_value in enumerate(endpoint_values):
        values = csv_value.split(",")
        if len(values) != 2:
            raise ValueError(f"{arm_id} stored Project command endpoints drifted")
        for endpoint_index, value in enumerate(values):
            origins.add(
                validate_gateway_url(
                    value, f"job{job_index}", f"endpoint-{endpoint_index}"
                )
            )
    if len(origins) != 1:
        raise ValueError(f"{arm_id} Project Jobs used different gateway origins")
    metrics = arm_identity.get("matrix_metrics_urls")
    if not isinstance(metrics, list) or len(metrics) != 2:
        raise ValueError(f"{arm_id} lacks frozen metrics URL identity")
    expected_metrics_csv = ",".join(str(value) for value in metrics)
    metrics_values = _command_flag_values(command, "--model-metrics-urls")
    if not metrics_values or any(
        value != expected_metrics_csv for value in metrics_values
    ):
        raise ValueError(f"{arm_id} stored Project metrics endpoints drifted")


def _validation(
    status: str,
    errors: list[str] | None = None,
    *,
    formal_authorization_verified: bool = False,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "status": status,
        "comparison_scope": "database_e2e_five_arm_system_matrix",
        "official_vtc_evidence_included": False,
        # This repository never grants authorization. A passed summary records
        # only that the independent run-specific artifact was verified.
        "formal_authorized": False,
        "formal_authorization_verified": formal_authorization_verified,
        "native_baseline_count": 3,
        "project_control_count": 2,
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
    publish_failed_generation(
        output_dir,
        audit_rows,
        errors,
        _validation("failed"),
    )


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
    matrix_root: Path,
    cell: dict[str, Any],
    start: float,
    end: float,
    run_id: str,
) -> dict[str, object]:
    resource = cell.get("resource_metrics")
    if not isinstance(resource, dict) or resource.get("resource_metrics_status") != "ok":
        raise ValueError(f"{run_id} resource metrics are unavailable")
    path = resolve_matrix_evidence_path(
        matrix_root,
        resource.get("path", ""),
        f"{run_id} resource trace",
    )
    if not path.is_file():
        raise ValueError(f"{run_id} resource trace must be a file")
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
        "mfu_fraction_mean": (
            mean("mfu_fraction")
            if isinstance(cell.get("mfu_contract"), dict)
            and cell["mfu_contract"].get("status") == "available"
            else "unavailable"
        ),
        "mfu_status": cell.get("mfu_contract", {}).get("status", "unavailable"),
        "gpu_peak_tflops_per_gpu": cell.get("mfu_contract", {}).get(
            "gpu_peak_tflops_per_gpu", "unavailable"
        ),
        "mfu_precision": cell.get("mfu_contract", {}).get(
            "precision", "unavailable"
        ),
        "vllm_running_mean": mean("running"),
        "vllm_waiting_mean": mean("waiting"),
        "vllm_kv_cache_usage_mean": mean("kv_usage"),
    }


def _normalize_cell(
    matrix_root: Path,
    cell: object,
    arm_identity: dict[str, object],
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
    mfu_contract = cell.get("mfu_contract")
    if not isinstance(mfu_contract, dict) or mfu_contract != arm_identity.get(
        "mfu_contract"
    ):
        raise ValueError(f"{run_id} MFU peak/precision contract drifted")
    versions = DatabaseIdentity.from_record(cell, run_id).as_dict()
    if (
        phase not in {"warmup", "formal"}
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
    expected_jobs = arm_identity.get("job_manifests")
    if not isinstance(expected_jobs, list) or len(expected_jobs) != 2:
        raise ValueError(f"{run_id} lacks frozen per-Job identity")
    for job, expected_job in zip(jobs, expected_jobs, strict=True):
        if not isinstance(job, dict) or job.get("exactly_once") is not True:
            raise ValueError(f"{run_id} has invalid Job exactly-once evidence")
        if (
            not isinstance(expected_job, dict)
            or job.get("job_id") != expected_job.get("job_id")
            or job.get("manifest_sha256") != expected_job.get("sha256")
        ):
            raise ValueError(f"{run_id} Job identity drifted")
        if (
            int(job.get("completed_count", -1)) != int(expected_job.get("rows", -2))
            or int(job.get("expected_count", -1)) != int(expected_job.get("rows", -2))
        ):
            raise ValueError(f"{run_id} has invalid Job row accounting")
        job_p50 = _finite(job.get("request_p50_s"), f"{run_id} Job P50")
        job_p95 = _finite(job.get("request_p95_s"), f"{run_id} Job P95")
        job_p99 = _finite(job.get("request_p99_s"), f"{run_id} Job P99")
        job_slo = _finite(
            job.get("slo_violation_ratio"), f"{run_id} Job SLO ratio"
        )
        if (
            job.get("request_p99_status") != "available"
            or job.get("slo_status") != "available"
            or not 0 <= job_p50 <= job_p95 <= job_p99
            or not 0 <= job_slo <= 1
        ):
            raise ValueError(f"{run_id} per-Job gateway tail/SLO evidence is invalid")
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
        ends.append(
            _finite(
                job.get("t4_result_visible_epoch_s"),
                f"{run_id} Job result visibility",
            )
        )
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
    if p99_status != "available" or slo_status != "available":
        raise ValueError(f"{run_id} gateway tails must be available")
    fairness = cell.get("service_fairness_metrics")
    if not isinstance(fairness, dict):
        raise ValueError(f"{run_id} lacks service fairness availability")
    completion_evidence = cell.get("completion_evidence")
    if (
        not isinstance(completion_evidence, dict)
        or completion_evidence.get("status") != "passed"
        or completion_evidence.get("mode") != "completion_trace_digest"
        or completion_evidence.get("exactly_once") is not True
        or completion_evidence.get("observed_doc_id_digest")
        != completion_evidence.get("expected_doc_id_digest")
        or not completion_evidence.get("output_digest")
    ):
        raise ValueError(f"{run_id} lacks valid completion trace evidence")

    command = cell.get("command", [])
    if not isinstance(command, list):
        raise ValueError(f"{run_id} command evidence must be a list")
    command_text = " ".join(str(item).lower() for item in command)
    if arm_id in SYSTEM_ARM_IDS[:3] and any(
        fragment in command_text for fragment in _PROJECT_FLAG_FRAGMENTS
    ):
        raise ValueError(f"{run_id} native command contains Project flags")
    _validate_executor_command(
        command, arm_id, arm_identity, cell.get("observation_gateway")
    )

    total = prompt + generation
    system_observation = cell.get("system_observation")
    if not isinstance(system_observation, dict):
        raise ValueError(f"{run_id} lacks T0-T4 system observation")
    common_fairness = system_observation.get("service_fairness")
    isolation = system_observation.get("isolation_observation")
    if not isinstance(common_fairness, dict) or not isinstance(isolation, dict):
        raise ValueError(f"{run_id} lacks fairness/isolation observation")
    correct_throughput = _finite(
        system_observation.get("correct_throughput_tokens_per_s"),
        f"{run_id} correct throughput",
    )
    run_row = {
        "run_id": run_id,
        "arm_id": arm_id,
        "phase": phase,
        "repeat": repeat,
        "order_index": order_index,
        "scheduler_owner": str(cell.get("scheduler_owner", "")),
        "implementation_source": str(cell.get("implementation_source", "")),
        **versions,
        "report_blocks": json.dumps(cell.get("report_blocks", [])),
        "database_operator_e2e_s": duration,
        "group_jct_s": duration,
        "service_prompt_tokens": prompt,
        "service_generation_tokens": generation,
        "service_total_tokens": total,
        "service_tokens_per_s": correct_throughput,
        "correct_throughput_tokens_per_s": correct_throughput,
        "actual_completed_tokens": system_observation.get("actual_total_tokens"),
        "request_p99_status": p99_status,
        "request_p99_s": p99_value,
        "request_p99_reason": p99_reason,
        "slo_status": slo_status,
        "slo_violation_ratio": slo_value,
        "slo_reason": slo_reason,
        "starvation_status": fairness.get("starvation_status", "unavailable"),
        "longest_no_service_s": fairness.get("longest_no_service_s", "unavailable"),
        "completion_service_lag_status": fairness.get(
            "completion_service_lag_status", "unavailable"
        ),
        "completion_service_lag_p95_work": fairness.get(
            "completion_service_lag_p95_work", "unavailable"
        ),
        "completion_service_lag_max_work": fairness.get(
            "completion_service_lag_max_work", "unavailable"
        ),
        "service_fairness_reason": fairness.get("reason", ""),
        "weighted_jain_fairness": common_fairness.get(
            "weighted_jain_fairness", "unavailable"
        ),
        "weighted_service_share_by_job": json.dumps(
            common_fairness.get("weighted_service_share_by_job", {}),
            sort_keys=True,
        ),
        "common_backlog_duration_s": common_fairness.get(
            "common_backlog_duration_s", "unavailable"
        ),
        "isolation_status": isolation.get("status", "unavailable"),
        "victim_no_service_after_aggressor_release_s": isolation.get(
            "victim_no_service_after_aggressor_release_s", "unavailable"
        ),
        "victim_request_p99_inflation_ratio": isolation.get(
            "victim_request_p99_inflation_ratio", "unavailable"
        ),
        "victim_recovery_after_aggressor_service_end_s": isolation.get(
            "victim_recovery_after_aggressor_service_end_s", "unavailable"
        ),
        "completion_evidence_mode": completion_evidence["mode"],
        "completion_evidence_producer": completion_evidence.get("producer", ""),
        "completion_expected_rows": completion_evidence.get("expected_rows", ""),
        "completion_observed_rows": completion_evidence.get("observed_rows", ""),
        "completion_exactly_once": completion_evidence["exactly_once"],
        "completion_output_digest": completion_evidence.get("output_digest", ""),
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
    role_by_id = {
        str(expected_jobs[0]["job_id"]): "bulk",
        str(expected_jobs[1]["job_id"]): "foreground",
    }
    job_rows = [
        {
            "run_id": run_id,
            "arm_id": arm_id,
            "repeat": repeat,
            "job_id": str(job["job_id"]),
            "job_role": role_by_id[str(job["job_id"])],
            "scheduled_release_epoch_s": scheduled,
            "actual_launch_epoch_s": actual,
            "scheduled_launch_offset_s": scheduled - scheduled_starts[0],
            "actual_launch_offset_s": actual - actual_starts[0],
            "launch_deviation_s": actual - scheduled,
            "completion_epoch_s": completion,
            "job_jct_s": job.get("jct_s"),
            "source_s": job.get("source_s"),
            "execution_s": job.get("execution_s"),
            "service_span_s": job.get("service_span_s"),
            "actual_total_tokens": job.get("actual_total_tokens"),
            "request_p50_s": job.get("request_p50_s", "unavailable"),
            "request_p95_s": job.get("request_p95_s", "unavailable"),
            "request_p99_status": job.get("request_p99_status", "unavailable"),
            "request_p99_s": job.get("request_p99_s", "unavailable"),
            "slo_status": job.get("slo_status", "unavailable"),
            "slo_violation_ratio": job.get("slo_violation_ratio", "unavailable"),
            "job_jct_slo_s": job.get("job_jct_slo_s", "unavailable"),
            "job_jct_slo_status": job.get("job_jct_slo_status", "unavailable"),
            "job_jct_slo_violation": job.get(
                "job_jct_slo_violation", "unavailable"
            ),
            "tail_reason": job.get("tail_reason", "not recorded"),
            "overlap_s": overlap,
            "completion_order": 1 + sorted(ends).index(completion),
        }
        for job, scheduled, actual, completion in zip(
            jobs, scheduled_starts, actual_starts, ends, strict=True
        )
    ]
    return run_row, job_rows, _resource_row(
        matrix_root, cell, start, end, run_id
    )


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
    weighted_jain = [float(row["weighted_jain_fairness"]) for row in ordered]
    longest_no_service = [float(row["longest_no_service_s"]) for row in ordered]
    service_lag_p95 = [
        float(row["completion_service_lag_p95_work"]) for row in ordered
    ]
    victim_no_service_raw = [
        row["victim_no_service_after_aggressor_release_s"] for row in ordered
    ]
    victim_no_service = (
        [float(value) for value in victim_no_service_raw]
        if all(value != "unavailable" for value in victim_no_service_raw)
        else []
    )
    recovery_raw = [
        row["victim_recovery_after_aggressor_service_end_s"] for row in ordered
    ]
    recovery = (
        [float(value) for value in recovery_raw]
        if all(value != "unavailable" for value in recovery_raw)
        else []
    )
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
        "group_jct_s_mean": statistics.fmean(duration),
        "group_jct_s_sample_cv": _sample_cv(duration),
        "group_jct_s_repeats": json.dumps(duration),
        "bulk_jct_s_mean": statistics.fmean(bulk_jct),
        "bulk_jct_s_sample_cv": _sample_cv(bulk_jct),
        "bulk_jct_s_repeats": json.dumps(bulk_jct),
        "foreground_jct_s_mean": statistics.fmean(foreground_jct),
        "foreground_jct_s_sample_cv": _sample_cv(foreground_jct),
        "foreground_jct_s_repeats": json.dumps(foreground_jct),
        "overlap_s_mean": statistics.fmean(overlap),
        "overlap_s_sample_cv": _sample_cv(overlap),
        "overlap_s_repeats": json.dumps(overlap),
        "weighted_jain_fairness_mean": statistics.fmean(weighted_jain),
        "weighted_jain_fairness_repeats": json.dumps(weighted_jain),
        "longest_no_service_s_mean": statistics.fmean(longest_no_service),
        "longest_no_service_s_repeats": json.dumps(longest_no_service),
        "completion_service_lag_p95_work_mean": statistics.fmean(service_lag_p95),
        "completion_service_lag_p95_work_repeats": json.dumps(service_lag_p95),
        "victim_no_service_after_aggressor_release_s_mean": (
            statistics.fmean(victim_no_service)
            if victim_no_service else "unavailable"
        ),
        "victim_no_service_after_aggressor_release_s_repeats": json.dumps(
            victim_no_service_raw
        ),
        "victim_recovery_after_aggressor_service_end_s_mean": (
            statistics.fmean(recovery) if recovery else "unavailable"
        ),
        "victim_recovery_after_aggressor_service_end_s_repeats": json.dumps(
            recovery_raw
        ),
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
    contract_fields = (
        "mfu_status", "gpu_peak_tflops_per_gpu", "mfu_precision",
    )
    for field in contract_fields:
        values = {str(by_id[str(row["run_id"])][field]) for row in ordered}
        if len(values) != 1:
            raise ValueError(f"{arm_id} {field} drifted across repeats")
        output[field] = next(iter(values))
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
    "validation_status", "repository_commit", "matrix_instance_id", "config_sha256",
    "config_fingerprint", "authorization_sha256", "manifest_path",
    "manifest_sha256", "service_signature", "server_version", "pgvector_version",
    "database_operator_e2e_s", "group_jct_s",
    "service_prompt_tokens", "service_generation_tokens", "service_total_tokens",
    "service_tokens_per_s", "correct_throughput_tokens_per_s",
    "actual_completed_tokens", "request_p99_status", "request_p99_s",
    "request_p99_reason", "slo_status", "slo_violation_ratio", "slo_reason",
    "starvation_status", "longest_no_service_s",
    "completion_service_lag_status", "completion_service_lag_p95_work",
    "completion_service_lag_max_work", "service_fairness_reason",
    "weighted_jain_fairness", "weighted_service_share_by_job",
    "common_backlog_duration_s", "isolation_status",
    "victim_no_service_after_aggressor_release_s",
    "victim_request_p99_inflation_ratio",
    "victim_recovery_after_aggressor_service_end_s",
    "completion_evidence_mode", "completion_evidence_producer",
    "completion_expected_rows", "completion_observed_rows",
    "completion_exactly_once", "completion_output_digest",
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
            "failure_reason": redact_text(
                str(raw.get("error", "")) or failure_reason
            ),
            "validation_status": validation_status,
            "repository_commit": str(raw.get("repository_commit", "")),
            "matrix_instance_id": str(raw.get("matrix_instance_id", "")),
            "config_sha256": str(raw.get("config_sha256", "")),
            "config_fingerprint": str(raw.get("config_fingerprint", "")),
            "authorization_sha256": str(raw.get("authorization_sha256", "")),
            "manifest_path": str(raw.get("manifest_path", "")),
            "manifest_sha256": str(raw.get("manifest_sha256", "")),
            "service_signature": json.dumps(
                raw.get("service_signature", {}), sort_keys=True
            ),
            "server_version": str(raw.get("server_version", "")),
            "pgvector_version": str(raw.get("pgvector_version", "")),
            "exactly_once": raw.get("exactly_once", ""),
        }
    )
    if normalized is not None:
        row.update(normalized)
        row["status"] = str(raw.get("status", "passed"))
        row["failure_reason"] = redact_text(failure_reason)
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
    _validate_formal_authorization_binding(authorization, runtime)
    matrix_instance_id = runtime.get("matrix_instance_id")
    if (
        not isinstance(matrix_instance_id, str)
        or len(matrix_instance_id) != 32
        or any(
            character not in "0123456789abcdef"
            for character in matrix_instance_id
        )
    ):
        raise ValueError("matrix instance identity is invalid")
    if sha256_payload(resolved) != authorization.get("resolved_config_sha256"):
        raise ValueError("resolved config fingerprint drifted")
    if (
        runtime.get("execution_mode") != "formal"
        or runtime.get("status") != "authorized"
        or runtime.get("formal_authorized") is not True
        or runtime.get("authorization_sha256") != authorization_sha256
    ):
        raise ValueError("runtime authorization identity is invalid")
    manifest_evidence_path = runtime.get("manifest_evidence_path")
    manifest_path = resolve_matrix_evidence_path(
        matrix_root,
        manifest_evidence_path,
        "sealed manifest",
    )
    if (
        not manifest_path.is_file()
        or sha256_file(manifest_path) != authorization["manifest_sha256"]
    ):
        raise ValueError("sealed manifest identity drifted")
    job_manifest_evidence = runtime.get("job_manifest_evidence")
    if not isinstance(job_manifest_evidence, list):
        raise ValueError("sealed Job manifest evidence is missing")
    expected_jobs = authorization.get("job_manifests")
    if not isinstance(expected_jobs, list) or len(expected_jobs) != 2:
        raise ValueError("authorized Job manifest identity is invalid")
    observed_jobs: list[dict[str, object]] = []
    for expected, sealed in zip(expected_jobs, job_manifest_evidence, strict=True):
        if not isinstance(expected, dict) or not isinstance(sealed, dict):
            raise ValueError("sealed Job manifest evidence schema is invalid")
        identity = {
            field: sealed.get(field) for field in ("job_id", "rows", "sha256")
        }
        if identity != expected:
            raise ValueError("sealed Job manifest identity drifted")
        path = resolve_matrix_evidence_path(
            matrix_root,
            sealed.get("evidence_path"),
            f"sealed {expected.get('job_id')} manifest",
        )
        if (
            not path.is_file()
            or sha256_file(path) != expected.get("sha256")
            or len(read_manifest(path)) != expected.get("rows")
        ):
            raise ValueError("sealed Job manifest evidence drifted")
        observed_jobs.append(dict(sealed))
    if tuple(
        request
        for job in job_manifest_evidence
        for request in read_manifest(resolve_matrix_evidence_path(
            matrix_root,
            job["evidence_path"],
            "sealed Job manifest",
        ))
    ) != read_manifest(manifest_path):
        raise ValueError("sealed combined manifest does not equal job0+job1")
    expected_index = {
        "repository_commit": authorization["repository_commit"],
        "matrix_instance_id": matrix_instance_id,
        "config_sha256": authorization["config_sha256"],
        "native_config_sha256": authorization["native_config_sha256"],
        "project_config_sha256": authorization["project_config_sha256"],
        "config_fingerprint": authorization["resolved_config_sha256"],
        "manifest_sha256": authorization["manifest_sha256"],
        "manifest_evidence_path": manifest_evidence_path,
        "job_manifest_evidence": observed_jobs,
        "authorization_sha256": authorization_sha256,
        "execution_mode": "formal",
        "endpoint_urls": resolved.get("endpoint_urls"),
        "metrics_urls": resolved.get("metrics_urls"),
        "health_url": resolved.get("health_url"),
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
    endpoint_urls = resolved.get("endpoint_urls")
    metrics_urls = resolved.get("metrics_urls")
    health_url = resolved.get("health_url")
    if (
        not isinstance(endpoint_urls, list) or len(endpoint_urls) != 2
        or not isinstance(metrics_urls, list) or len(metrics_urls) != 2
        or not isinstance(health_url, str) or not health_url
    ):
        raise ValueError("resolved service endpoint identity is incomplete")
    for arm in by_arm.values():
        arm["matrix_endpoint_urls"] = endpoint_urls
        arm["matrix_metrics_urls"] = metrics_urls
        arm["matrix_health_url"] = health_url
    scheduler_owners = {
        arm_id: str(arm["scheduler_owner"]) for arm_id, arm in by_arm.items()
    }
    if index.get("scheduler_owners") != scheduler_owners:
        raise ValueError("matrix scheduler-owner identity drifted")
    validate_uniform_cell_identity(raw_cells, scheduler_owners)
    service_signatures = {
        json.dumps(arm.get("service_signature", {}), sort_keys=True)
        for arm in by_arm.values()
    }
    if len(service_signatures) != 1 or index.get("service_signature") != next(
        iter(by_arm.values())
    ).get("service_signature"):
        raise ValueError("matrix service signature drifted")
    mfu_contracts = {
        json.dumps(arm.get("mfu_contract", {}), sort_keys=True)
        for arm in by_arm.values()
    }
    if len(mfu_contracts) != 1 or index.get("mfu_contract") != next(
        iter(by_arm.values())
    ).get("mfu_contract"):
        raise ValueError("matrix MFU peak/precision contract drifted")
    for arm in by_arm.values():
        if str(arm.get("manifest_sha256", "")) != authorization["manifest_sha256"]:
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
            "matrix_instance_id": matrix_instance_id,
            "config_sha256": authorization["config_sha256"],
            "native_config_sha256": authorization["native_config_sha256"],
            "project_config_sha256": authorization["project_config_sha256"],
            "config_fingerprint": authorization["resolved_config_sha256"],
            "authorization_sha256": authorization_sha256,
            "manifest_path": manifest_evidence_path,
            "manifest_sha256": arm["manifest_sha256"],
            "service_signature": arm["service_signature"],
            "mfu_contract": arm["mfu_contract"],
            "scheduler_owner": arm["scheduler_owner"],
            "endpoint_urls": endpoint_urls,
            "metrics_urls": metrics_urls,
            "health_url": health_url,
            "executor_selection": arm.get("executor_selection"),
        }
        for field, expected in expected_cell.items():
            if cell.get(field) != expected:
                raise ValueError(f"cell {position} {field} identity drifted")
        resource = cell.get("resource_metrics")
        if not isinstance(resource, dict):
            raise ValueError(f"cell {position} resource evidence is invalid")
        resolve_matrix_evidence_path(
            matrix_root,
            resource.get("path", ""),
            f"cell {position} resource trace",
        )
        output_paths = cell.get("output_paths")
        if not isinstance(output_paths, dict) or not output_paths:
            raise ValueError(f"cell {position} output evidence is invalid")
        for name, stored_path in output_paths.items():
            artifact = resolve_matrix_evidence_path(
                matrix_root,
                stored_path,
                f"cell {position} output artifact {name}",
            )
            if not artifact.is_file():
                raise ValueError(
                    f"cell {position} output artifact {name} must be a file"
                )
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
        DatabaseIdentity.consistent(raw_cells, "native-system matrix")
        resolved_arms = _load_authorized_identity(
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
        repeat_contract = index.get("repeat_contract", {})
        if not isinstance(repeat_contract, dict):
            raise ValueError("matrix index lacks repeat contract")
        formal_repeats = int(repeat_contract.get("formal", 0))
        if formal_repeats != FORMAL_REPEATS:
            raise ValueError("formal matrix must retain the frozen three repeats")

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
        project_limits = {
            (
                int(cell.get("request_limit_per_endpoint", -1)),
                int(cell.get("work_limit_per_endpoint", -1)),
            )
            for cell in formal_cells
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
            arm_id = str(cell.get("arm_id", ""))
            arm_identity = resolved_arms.get(arm_id)
            if arm_identity is None:
                raise ValueError(f"matrix cell references unknown arm {arm_id!r}")
            normalized, jobs, resource = _normalize_cell(
                matrix_root, cell, arm_identity
            )
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
        phase_order = {"warmup": 0, "formal": 1}
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

        system = [
            _summary_row(
                arm_id, rows_for(arm_id), job_rows, "complete_system_empirical"
            )
            for arm_id in SYSTEM_ARM_IDS
        ]
        staging_dir.mkdir()
        _write_csv(staging_dir / "all_runs.csv", run_rows)
        _write_csv(staging_dir / "system_summary.csv", system)
        _write_csv(staging_dir / "job_summary.csv", job_rows)
        _write_csv(
            staging_dir / "resource_summary.csv",
            [
                _resource_summary_row(
                    arm_id,
                    rows_for(arm_id),
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
        redacted_error = redact_text(str(error))
        _publish_failed_validation(
            output_dir,
            _failed_audit_rows(raw_cells, redacted_error),
            [redacted_error],
        )
        return False
