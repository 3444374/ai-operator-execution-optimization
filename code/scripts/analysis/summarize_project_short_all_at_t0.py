#!/usr/bin/env python3
"""Audit the Project short-job all-at-t0 diagnostic and align timer boundaries."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from collections import Counter
from pathlib import Path


NUMERIC_FIELDS = (
    "e2e_s",
    "source_fetch_s",
    "actor_ready_s",
    "model_request_wall_s",
    "operator_wall_s",
    "bounded_wait_s",
    "fanin_s",
    "rows_per_s",
    "tokens_per_s",
    "model_request_tokens_per_s",
    "operator_tokens_per_s",
    "request_e2e_s_p50",
    "request_e2e_s_p95",
    "request_e2e_s_p99",
    "gpu_utilization_pct_mean",
    "gpu_utilization_pct_p95",
    "gpu_utilization_pct_max",
    "gpu_utilization_below_10pct_ratio",
    "mfu_estimate",
    "vllm_running_mean",
    "vllm_running_p95",
    "vllm_running_max",
    "vllm_waiting_mean",
    "vllm_waiting_max",
    "vllm_kv_cache_usage_mean",
    "vllm_kv_cache_usage_max",
    "vllm_e2e_request_latency_mean_s",
    "vllm_request_queue_time_mean_s",
    "vllm_request_inference_time_mean_s",
    "vllm_request_prefill_time_mean_s",
    "vllm_request_decode_time_mean_s",
    "vllm_time_to_first_token_mean_s",
    "vllm_inter_token_latency_mean_s",
    "vllm_request_success_delta",
)

TRACE_NUMERIC_FIELDS = (
    "request_jct_s",
    "arrival_span_s",
    "submit_to_completion_window_s",
    "service_to_completion_window_s",
)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _number(row: dict[str, str], field: str) -> float:
    try:
        value = float(row[field])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"missing/non-numeric {field}") from exc
    if not math.isfinite(value):
        raise ValueError(f"non-finite {field}")
    return value


def _mean(values: list[float]) -> float:
    if not values:
        raise ValueError("cannot average an empty sequence")
    return statistics.mean(values)


def _sample_cv(values: list[float]) -> float:
    mean = _mean(values)
    if len(values) < 2 or mean == 0:
        return 0.0
    return statistics.stdev(values) / mean


def _argument_value(arguments: list[str], flag: str) -> str:
    positions = [index for index, value in enumerate(arguments) if value == flag]
    if len(positions) != 1 or positions[0] + 1 >= len(arguments):
        raise ValueError(f"expected exactly one {flag}")
    return arguments[positions[0] + 1]


def _pct_delta(value: float, baseline: float) -> float:
    if baseline == 0:
        raise ValueError("percentage baseline cannot be zero")
    return (value / baseline - 1.0) * 100.0


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise ValueError(f"refusing to write empty CSV: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(rows[0]),
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def _load_reference_rows(
    timing_path: Path,
    replay_breakdown_path: Path,
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    timing = {row["system"]: row for row in _read_csv(timing_path)}
    if set(timing) != {"project", "daft_native"}:
        raise ValueError("timing reference must contain project and daft_native")
    replay_rows = {
        row["scenario"]: row for row in _read_csv(replay_breakdown_path)
    }
    if "single_short_full_pool" not in replay_rows:
        raise ValueError("replay breakdown lacks single_short_full_pool")
    return timing["project"], timing["daft_native"], replay_rows[
        "single_short_full_pool"
    ]


def _load_daft_service_windows(native_root: Path) -> list[float]:
    paths = sorted(
        native_root.glob("runs/*_formal_*_daft_native/daft_native/gate.json")
    )
    if len(paths) != 3:
        raise ValueError("expected three Daft Native formal gate files")
    windows: list[float] = []
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("status") != "passed" or payload.get("incidents"):
            raise ValueError(f"Daft Native gate did not pass cleanly: {path}")
        metrics = payload.get("metrics", {})
        if int(metrics.get("manifest_rows", -1)) != 512:
            raise ValueError(f"Daft Native gate used an unexpected row count: {path}")
        value = float(metrics["group_service_wall_s"])
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"invalid Daft Native service window: {path}")
        windows.append(value)
    return windows


def _timing_contract_rows() -> list[dict[str, object]]:
    return [
        {
            "timer_id": "T0_full_pipeline_wall",
            "start": "before source/framework preparation",
            "end": "all requested outputs materialized (and sink, if enabled)",
            "required_for_fair_comparison": "same source, sink, input visibility, and outer start",
            "current_cross_system_status": "unavailable",
            "reason": "Daft Native outer preparation was completed before its recorded timer",
        },
        {
            "timer_id": "T1_offered_work_jct",
            "start": "first request becomes visible to the measured path",
            "end": "last request completes",
            "required_for_fair_comparison": "same offered-arrival trace and visibility boundary",
            "current_cross_system_status": "not_rankable_across_replay_and_eager",
            "reason": "Project replay has a 66.875 s arrival span; eager runs have zero span",
        },
        {
            "timer_id": "T2_framework_execute_wall",
            "start": "framework execute/collect call after its declared preparation boundary",
            "end": "framework result materialized",
            "required_for_fair_comparison": "same preparation ownership before timer",
            "current_cross_system_status": "diagnostic_only",
            "reason": "Project operator and Daft collect own different preparation work",
        },
        {
            "timer_id": "T3_model_request_window",
            "start": "earliest model request submission",
            "end": "latest model response completion",
            "required_for_fair_comparison": "same manifest, model service, output cap, and input visibility",
            "current_cross_system_status": "comparable_short_job_diagnostic",
            "reason": "both are request submission-to-completion windows; short duration is not a 60 s capacity rank",
        },
        {
            "timer_id": "T4_vllm_request_service_mean",
            "start": "vLLM request admission counter boundary",
            "end": "vLLM request completion counter boundary",
            "required_for_fair_comparison": "same service counter definition and request set",
            "current_cross_system_status": "comparable_short_job_diagnostic",
            "reason": "same vLLM counter family; interpret jointly with aggregate work rate and MFU",
        },
    ]


def _timing_alignment_rows(
    summary: dict[str, float],
    replay: dict[str, str],
    daft: dict[str, str],
    daft_service_windows: list[float],
) -> list[dict[str, object]]:
    daft_window_mean = _mean(daft_service_windows)
    return [
        {
            "system_run": "project_arrival_replay_single_short",
            "input_visibility": "request_level_replay_66.875s_span",
            "timer_id": "T0_full_pipeline_wall",
            "mean_s": float(replay["profiler_e2e_s_mean"]),
            "formal_repeats": 3,
            "cross_system_rankability": "no",
            "boundary_note": "Project profiler includes source and actor-ready; stages overlap",
        },
        {
            "system_run": "project_arrival_replay_single_short",
            "input_visibility": "request_level_replay_66.875s_span",
            "timer_id": "T1_offered_work_jct",
            "mean_s": float(replay["reported_wall_s_mean"]),
            "formal_repeats": 3,
            "cross_system_rankability": "no",
            "boundary_note": "valid only against another run with the identical replay trace",
        },
        {
            "system_run": "project_all_at_t0_single_short",
            "input_visibility": "all_512_requests_visible_zero_arrival_span",
            "timer_id": "T0_full_pipeline_wall",
            "mean_s": summary["e2e_s_mean"],
            "formal_repeats": 3,
            "cross_system_rankability": "no",
            "boundary_note": "Daft matching outer timer was not collected",
        },
        {
            "system_run": "project_all_at_t0_single_short",
            "input_visibility": "all_512_requests_visible_zero_arrival_span",
            "timer_id": "T1_offered_work_jct",
            "mean_s": summary["request_jct_s_mean"],
            "formal_repeats": 3,
            "cross_system_rankability": "no",
            "boundary_note": "Project visibility precedes source/organizer work that Daft completed before collect",
        },
        {
            "system_run": "project_all_at_t0_single_short",
            "input_visibility": "all_512_requests_visible_zero_arrival_span",
            "timer_id": "T2_framework_execute_wall",
            "mean_s": summary["operator_wall_s_mean"],
            "formal_repeats": 3,
            "cross_system_rankability": "diagnostic_only",
            "boundary_note": "closest Project framework span, but not identical to Daft collect ownership",
        },
        {
            "system_run": "project_all_at_t0_single_short",
            "input_visibility": "all_512_requests_visible_zero_arrival_span",
            "timer_id": "T3_model_request_window",
            "mean_s": summary["submit_to_completion_window_s_mean"],
            "formal_repeats": 3,
            "cross_system_rankability": "comparable_short_job_diagnostic",
            "boundary_note": "earliest submit to latest completion from request trace",
        },
        {
            "system_run": "project_all_at_t0_single_short",
            "input_visibility": "all_512_requests_visible_zero_arrival_span",
            "timer_id": "T4_vllm_request_service_mean",
            "mean_s": summary["vllm_e2e_request_latency_mean_s_mean"],
            "formal_repeats": 3,
            "cross_system_rankability": "comparable_short_job_diagnostic",
            "boundary_note": "same vLLM counter family",
        },
        {
            "system_run": "daft_native_single_short",
            "input_visibility": "full_manifest_before_collect",
            "timer_id": "T0_full_pipeline_wall",
            "mean_s": "",
            "formal_repeats": 0,
            "cross_system_rankability": "unavailable",
            "boundary_note": "manifest/provider/DataFrame/expression preparation was outside timer",
        },
        {
            "system_run": "daft_native_single_short",
            "input_visibility": "full_manifest_before_collect",
            "timer_id": "T2_framework_execute_wall",
            "mean_s": daft_window_mean,
            "formal_repeats": len(daft_service_windows),
            "cross_system_rankability": "diagnostic_only",
            "boundary_note": "collect request window after graph preparation",
        },
        {
            "system_run": "daft_native_single_short",
            "input_visibility": "full_manifest_before_collect",
            "timer_id": "T3_model_request_window",
            "mean_s": daft_window_mean,
            "formal_repeats": len(daft_service_windows),
            "cross_system_rankability": "comparable_short_job_diagnostic",
            "boundary_note": "earliest shard submit to latest shard completion",
        },
        {
            "system_run": "daft_native_single_short",
            "input_visibility": "full_manifest_before_collect",
            "timer_id": "T4_vllm_request_service_mean",
            "mean_s": float(daft["vllm_e2e_request_latency_mean_s"]),
            "formal_repeats": len(daft_service_windows),
            "cross_system_rankability": "comparable_short_job_diagnostic",
            "boundary_note": "same vLLM counter family",
        },
    ]


def _comparison_rows(
    summary: dict[str, float],
    replay: dict[str, str],
    daft: dict[str, str],
    replay_breakdown: dict[str, str],
) -> list[dict[str, object]]:
    replay_e2e = float(replay["profiler_e2e_s_mean"])
    replay_operator = float(replay_breakdown["profiler_operator_wall_s_mean"])
    replay_service_rate = float(replay["group_service_tokens_per_s_mean"])
    replay_mfu = float(replay["group_mfu_pct_mean"])
    replay_running = float(replay["group_running_mean"])
    replay_kv = float(replay["group_kv_pct_mean"])
    daft_wall = float(daft["reported_wall_s_mean"])
    daft_service_rate = float(daft["group_service_tokens_per_s_mean"])
    daft_mfu = float(daft["group_mfu_pct_mean"])
    daft_running = float(daft["group_running_mean"])
    daft_kv = float(daft["group_kv_pct_mean"])
    daft_vllm_e2e = float(daft["vllm_e2e_request_latency_mean_s"])

    definitions = [
        (
            "all_at_t0_vs_arrival_replay",
            "profiler_e2e_s",
            replay_e2e,
            summary["e2e_s_mean"],
            "within_project_same_profiler_boundary",
            True,
            "same Project profiler boundary; input visibility is the intended change",
        ),
        (
            "all_at_t0_vs_arrival_replay",
            "operator_wall_s",
            replay_operator,
            summary["operator_wall_s_mean"],
            "within_project_same_operator_boundary",
            True,
            "same Project operator boundary",
        ),
        (
            "all_at_t0_vs_arrival_replay",
            "model_service_tokens_per_s",
            replay_service_rate,
            summary["model_request_tokens_per_s_mean"],
            "within_project_service_characterization",
            True,
            "vLLM counter work divided by the Project model-request wall",
        ),
        (
            "all_at_t0_vs_arrival_replay",
            "mfu_pct",
            replay_mfu,
            summary["mfu_estimate_mean"] * 100.0,
            "within_project_same_mfu_convention",
            True,
            "same 165-TFLOPS bf16 dense convention",
        ),
        (
            "all_at_t0_vs_arrival_replay",
            "running_mean",
            replay_running,
            summary["vllm_running_mean_mean"],
            "within_project_state_characterization",
            True,
            "sum across two endpoints",
        ),
        (
            "all_at_t0_vs_arrival_replay",
            "kv_usage_pct_mean",
            replay_kv,
            summary["vllm_kv_cache_usage_mean_mean"] * 100.0,
            "within_project_state_characterization",
            True,
            "vLLM fraction converted to percent",
        ),
        (
            "project_total_vs_daft_collect",
            "wall_s",
            daft_wall,
            summary["e2e_s_mean"],
            "cross_system_misaligned_timer",
            False,
            "not rankable: Project includes source/actor-ready; Daft timer starts before collect",
        ),
        (
            "project_operator_vs_daft_collect",
            "wall_s",
            daft_wall,
            summary["operator_wall_s_mean"],
            "cross_system_approximate_framework_boundary",
            False,
            "closer execution boundary, still not identical framework timing",
        ),
        (
            "project_model_request_vs_daft_request_window",
            "wall_s",
            daft_wall,
            summary["submit_to_completion_window_s_mean"],
            "cross_system_aligned_model_request_window",
            True,
            "same manifest and eager visibility; short diagnostic, not a 60-second capacity rank",
        ),
        (
            "project_all_at_t0_vs_daft_native_short",
            "model_service_tokens_per_s",
            daft_service_rate,
            summary["model_request_tokens_per_s_mean"],
            "cross_system_service_characterization",
            True,
            "same short manifest; service-counter characterization, not 60-second capacity ranking",
        ),
        (
            "project_all_at_t0_vs_daft_native_short",
            "mfu_pct",
            daft_mfu,
            summary["mfu_estimate_mean"] * 100.0,
            "cross_system_service_characterization",
            True,
            "same service and MFU convention",
        ),
        (
            "project_all_at_t0_vs_daft_native_short",
            "running_mean",
            daft_running,
            summary["vllm_running_mean_mean"],
            "cross_system_state_characterization",
            True,
            "different submission shapes; state characterization only",
        ),
        (
            "project_all_at_t0_vs_daft_native_short",
            "kv_usage_pct_mean",
            daft_kv,
            summary["vllm_kv_cache_usage_mean_mean"] * 100.0,
            "cross_system_state_characterization",
            True,
            "different submission shapes; state characterization only",
        ),
        (
            "project_all_at_t0_vs_daft_native_short",
            "vllm_request_e2e_mean_s",
            daft_vllm_e2e,
            summary["vllm_e2e_request_latency_mean_s_mean"],
            "cross_system_vllm_counter",
            True,
            "vLLM counter mean; lower per-request latency does not imply higher aggregate capacity",
        ),
    ]
    return [
        {
            "comparison": comparison,
            "metric": metric,
            "baseline": baseline,
            "project_all_at_t0": value,
            "ratio": value / baseline,
            "delta_pct": _pct_delta(value, baseline),
            "comparison_class": comparison_class,
            "rankable_for_stated_diagnostic": rankable,
            "interpretation_boundary": boundary,
        }
        for (
            comparison,
            metric,
            baseline,
            value,
            comparison_class,
            rankable,
            boundary,
        ) in definitions
    ]


def summarize(
    root: Path,
    timing_reference: Path,
    replay_breakdown: Path,
    daft_native_root: Path,
    output_dir: Path,
    *,
    expected_rows: int,
    expected_rows_per_endpoint: int,
    repository_commit: str,
    archive_sha256: str,
    short_manifest_sha256: str,
) -> dict[str, object]:
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "completed" or manifest.get("incidents"):
        raise ValueError("all-at-t0 manifest must be completed with zero incidents")
    config = manifest["redacted_config"]
    common_args = config["common_args"]
    if "--arrival-replay" in common_args:
        raise ValueError("all-at-t0 diagnostic must not enable arrival replay")
    if _argument_value(common_args, "--source-order") != "doc_id":
        raise ValueError("all-at-t0 request manifest must use doc_id order")
    if int(_argument_value(common_args, "--max-inflight")) != 128:
        raise ValueError("diagnostic must retain K128 per endpoint")
    if int(_argument_value(common_args, "--max-active-work-per-endpoint")) != 65536:
        raise ValueError("diagnostic must retain W65536 per endpoint")
    if int(_argument_value(common_args, "--token-budget")) != 6144:
        raise ValueError("diagnostic must retain token budget 6144")

    run_rows = _read_csv(root / "runs.csv")
    formal = [row for row in run_rows if row.get("phase") == "formal"]
    if len(formal) != 3 or any(row.get("status") != "ok" for row in formal):
        raise ValueError("expected exactly three successful formal rows")
    if any(row.get("resource_metrics_status") != "ok" for row in formal):
        raise ValueError("resource metrics gate failed")
    if any(row.get("mfu_status") != "ok" for row in formal):
        raise ValueError("MFU gate failed")
    if any(_number(row, "vllm_request_success_delta") != expected_rows for row in formal):
        raise ValueError("vLLM success counter does not match expected rows")

    formal_rows: list[dict[str, object]] = []
    identity_hashes = set()
    for row in formal:
        repeat = int(row["repeat_index"])
        request_paths = list(root.glob(f"*_formal_{repeat}_*.requests.csv"))
        if len(request_paths) != 1:
            raise ValueError(f"repeat {repeat} lacks one request trace")
        request_path = request_paths[0]
        submission_path = request_path.with_name(
            request_path.name.replace(".requests.csv", ".submissions.csv")
        )
        resource_path = request_path.with_name(
            request_path.name.replace(".requests.csv", ".resources.csv")
        )
        requests = _read_csv(request_path)
        submissions = _read_csv(submission_path)
        resources = _read_csv(resource_path)
        doc_ids = [item["doc_id"] for item in requests]
        if len(requests) != expected_rows or len(set(doc_ids)) != expected_rows:
            raise ValueError(f"repeat {repeat} request exactly-once gate failed")
        if len(submissions) != expected_rows:
            raise ValueError(f"repeat {repeat} submission count gate failed")
        endpoint_counts = Counter(item["endpoint_id"] for item in submissions)
        expected_counts = {
            "endpoint-0": expected_rows_per_endpoint,
            "endpoint-1": expected_rows_per_endpoint,
        }
        if endpoint_counts != expected_counts:
            raise ValueError(f"repeat {repeat} endpoint balance gate failed")
        if not resources:
            raise ValueError(f"repeat {repeat} resource trace is empty")
        arrival_epochs = [float(item["arrival_epoch_s"]) for item in requests]
        submit_epochs = [float(item["submit_epoch_s"]) for item in requests]
        service_epochs = [
            float(item["service_start_epoch_s"])
            for item in requests
            if item.get("service_start_epoch_s")
        ]
        completion_epochs = [
            float(item["completion_epoch_s"]) for item in requests
        ]
        if not service_epochs:
            raise ValueError(f"repeat {repeat} lacks service start timestamps")
        trace_metrics = {
            "request_jct_s": max(completion_epochs) - min(arrival_epochs),
            "arrival_span_s": max(arrival_epochs) - min(arrival_epochs),
            "submit_to_completion_window_s": (
                max(completion_epochs) - min(submit_epochs)
            ),
            "service_to_completion_window_s": (
                max(completion_epochs) - min(service_epochs)
            ),
        }
        identity = "\n".join(
            sorted(
                f"{item['doc_id']}|{item.get('source_row_hash', '')}"
                for item in requests
            )
        ).encode("utf-8")
        identity_hashes.add(hashlib.sha256(identity).hexdigest())
        formal_rows.append(
            {
                "repeat_index": repeat,
                **{field: _number(row, field) for field in NUMERIC_FIELDS},
                **trace_metrics,
                "request_rows": len(requests),
                "unique_doc_ids": len(set(doc_ids)),
                "submission_rows": len(submissions),
                "endpoint_0_rows": endpoint_counts["endpoint-0"],
                "endpoint_1_rows": endpoint_counts["endpoint-1"],
                "resource_rows": len(resources),
            }
        )
    if len(identity_hashes) != 1:
        raise ValueError("formal repeats do not contain the same request identities")

    summary: dict[str, float] = {"formal_repeats": float(len(formal_rows))}
    for field in NUMERIC_FIELDS:
        values = [float(row[field]) for row in formal_rows]
        summary[f"{field}_mean"] = _mean(values)
        summary[f"{field}_cv_sample"] = _sample_cv(values)
    for field in TRACE_NUMERIC_FIELDS:
        values = [float(row[field]) for row in formal_rows]
        summary[f"{field}_mean"] = _mean(values)
        summary[f"{field}_cv_sample"] = _sample_cv(values)
    for field in ("e2e_s", "tokens_per_s", "model_request_tokens_per_s", "mfu_estimate"):
        if summary[f"{field}_cv_sample"] >= 0.05:
            raise ValueError(f"formal stability gate failed for {field}")

    replay, daft, replay_detail = _load_reference_rows(
        timing_reference,
        replay_breakdown,
    )
    daft_service_windows = _load_daft_service_windows(daft_native_root)
    observed_daft_mean = _mean(daft_service_windows)
    reference_daft_mean = float(daft["reported_wall_s_mean"])
    if not math.isclose(observed_daft_mean, reference_daft_mean, abs_tol=1e-9):
        raise ValueError("Daft raw and compact timing reference disagree")
    comparisons = _comparison_rows(summary, replay, daft, replay_detail)
    timing_contract = _timing_contract_rows()
    timing_alignment = _timing_alignment_rows(
        summary,
        replay,
        daft,
        daft_service_windows,
    )
    pre_operator_s = summary["e2e_s_mean"] - summary["operator_wall_s_mean"]
    cross_track_gap_s = summary["e2e_s_mean"] - float(daft["reported_wall_s_mean"])
    summary["project_pre_operator_visible_s"] = pre_operator_s
    summary["project_total_minus_daft_collect_s"] = cross_track_gap_s
    summary["pre_operator_fraction_of_cross_track_gap"] = (
        pre_operator_s / cross_track_gap_s
    )
    summary["project_operator_minus_daft_collect_s"] = (
        summary["operator_wall_s_mean"] - float(daft["reported_wall_s_mean"])
    )

    _write_csv(output_dir / "formal_runs.csv", formal_rows)
    _write_csv(output_dir / "summary.csv", [summary])
    _write_csv(output_dir / "comparisons.csv", comparisons)
    _write_csv(output_dir / "timing_contract.csv", timing_contract)
    _write_csv(output_dir / "timing_alignment.csv", timing_alignment)
    audit = {
        "status": "passed",
        "scope": "short_job_timer_boundary_diagnostic_not_60s_capacity_ranking",
        "formal_repeats": 3,
        "expected_rows_per_repeat": expected_rows,
        "expected_rows_per_endpoint": expected_rows_per_endpoint,
        "request_identity_sha256": next(iter(identity_hashes)),
        "short_manifest_sha256": short_manifest_sha256,
        "short_manifest_sha_boundary": "observed_on_server_before_run",
        "repository_commit": repository_commit,
        "repository_commit_boundary": "observed_server_HEAD_before_run",
        "raw_archive_sha256": archive_sha256,
        "arrival_replay": False,
        "source_order": "doc_id",
        "project_static_k_per_endpoint": 128,
        "project_active_work_per_endpoint": 65536,
        "token_budget": 6144,
        "comparison_boundary": {
            "T0_full_pipeline_wall": "not_available_cross_system",
            "T1_offered_work_jct": "same_input_visibility_only",
            "T2_framework_execute_wall": "diagnostic_only",
            "T3_model_request_window": "comparable_short_job_diagnostic",
            "T4_vllm_request_service_mean": "comparable_short_job_diagnostic",
        },
        "daft_native_service_window_repeats_s": daft_service_windows,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "audit.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return {
        "audit": audit,
        "summary": summary,
        "comparisons": comparisons,
        "timing_contract": timing_contract,
        "timing_alignment": timing_alignment,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--timing-reference", required=True, type=Path)
    parser.add_argument("--replay-breakdown", required=True, type=Path)
    parser.add_argument("--daft-native-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--expected-rows", type=int, default=512)
    parser.add_argument("--expected-rows-per-endpoint", type=int, default=256)
    parser.add_argument("--repository-commit", required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--short-manifest-sha256", required=True)
    args = parser.parse_args()
    result = summarize(
        args.root,
        args.timing_reference,
        args.replay_breakdown,
        args.daft_native_root,
        args.output_dir,
        expected_rows=args.expected_rows,
        expected_rows_per_endpoint=args.expected_rows_per_endpoint,
        repository_commit=args.repository_commit,
        archive_sha256=args.archive_sha256,
        short_manifest_sha256=args.short_manifest_sha256,
    )
    print(json.dumps(result["audit"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
