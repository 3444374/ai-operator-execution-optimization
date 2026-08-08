"""Audit and summarize matched short-only versus staggered short/long evidence.

The output separates project causal controls from vendor-native observations,
preserves missing request-tail boundaries, and emits pre-arrival/overlap/drain
state summaries for the project two-job runs. It does not draw figures.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Mapping, Sequence


GPU_PEAK_TFLOPS = 165.0
EXPECTED_NATIVE_ARMS = ("daft_native", "daft_ray", "ray_data_http")


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _json_list(value: object) -> list[object]:
    decoded = json.loads(value) if isinstance(value, str) else value
    if not isinstance(decoded, list):
        raise ValueError("expected JSON list")
    return decoded


def _float(value: object, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{field} must be finite")
    return parsed


def _mean(values: Iterable[float]) -> float:
    materialized = list(values)
    if not materialized:
        raise ValueError("cannot average an empty sequence")
    return statistics.mean(materialized)


def _summary(values: Sequence[float]) -> tuple[float, float, float]:
    mean = statistics.mean(values)
    sd = statistics.stdev(values) if len(values) >= 2 else 0.0
    return mean, sd, sd / mean if mean else 0.0


def _percentile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("cannot take percentile of an empty sequence")
    if not 0.0 <= probability <= 1.0:
        raise ValueError("probability must be in [0, 1]")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    if not rows:
        raise ValueError(f"refusing to write empty CSV: {path}")
    fields = list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _project_rows(root: Path, expected_scenarios: set[str]) -> list[dict[str, object]]:
    manifest = _read_json(root / "manifest.json")
    if manifest.get("status") != "completed":
        raise ValueError(f"project matrix is not completed: {root}")
    rows: list[dict[str, object]] = []
    for path in sorted((root / "records").glob("*.json")):
        record = _read_json(path)
        if record.get("phase") != "formal":
            continue
        scenario = str(record["scenario_id"])
        if scenario not in expected_scenarios:
            raise ValueError(f"unexpected project scenario: {scenario}")
        jct = [_float(value, "job_jct_s") for value in _json_list(record["job_jct_s"])]
        p99 = [_float(value, "job_p99_s") for value in _json_list(record["job_p99_s"])]
        actual_work = [
            _float(value, "job_actual_work") for value in _json_list(record["job_actual_work"])
        ]
        manifests = [str(value) for value in _json_list(record["request_manifest_sha256"])]
        starts = [
            _float(value, "replay start")
            for value in _json_list(record["replay_configured_start_epoch_s"])
        ]
        overlap = (
            max(0.0, starts[0] + jct[0] - starts[1])
            if len(jct) == 2 and len(starts) == 2
            else 0.0
        )
        rows.append(
            {
                "system": "project",
                "scenario": scenario,
                "repeat": int(record["repeat_index"]),
                "short_jct_s": jct[0],
                "short_p99_s": p99[0],
                "short_actual_work": actual_work[0],
                "short_work_per_s": actual_work[0] / jct[0],
                "group_service_tokens_per_s": _float(record["tokens_per_s"], "tokens_per_s"),
                "group_gpu_util_pct": _float(
                    record["gpu_utilization_pct_mean"], "gpu_utilization_pct_mean"
                ),
                "group_mfu_fraction": _float(record["mfu_estimate"], "mfu_estimate"),
                "group_running_mean": _float(record["vllm_running_mean"], "vllm_running_mean"),
                "group_waiting_mean": _float(record["vllm_waiting_mean"], "vllm_waiting_mean"),
                "group_kv_mean": _float(record["vllm_kv_usage_mean"], "vllm_kv_usage_mean"),
                "group_gpu_energy_j": "",
                "short_long_overlap_s": overlap,
                "short_manifest_sha256": manifests[0],
                "request_p99_status": "observed",
                "throughput_scope": "group_all_active_jobs",
            }
        )
    counts = defaultdict(int)
    for row in rows:
        counts[str(row["scenario"])] += 1
    if counts != {scenario: 3 for scenario in expected_scenarios}:
        raise ValueError(f"project formal repeat mismatch: {dict(counts)}")
    return rows


def _native_single_rows(root: Path) -> list[dict[str, object]]:
    index = _read_json(root / "matrix_index.json")
    if index.get("status") != "passed":
        raise ValueError("native single-short matrix did not pass")
    rows: list[dict[str, object]] = []
    for run in index.get("runs", []):
        if not isinstance(run, dict) or run.get("phase") != "formal":
            continue
        if run.get("status") != "passed":
            raise ValueError(f"native single formal failed: {run.get('run_id')}")
        arm = str(run["arm_id"])
        run_root = Path(str(run["output_root"]))
        if not run_root.exists():
            run_root = root / "runs" / str(run["run_id"])
        gates = glob.glob(str(run_root / "*" / "gate.json"))
        if len(gates) != 1:
            raise ValueError(f"expected one gate for {run['run_id']}")
        metrics = _read_json(Path(gates[0]))["metrics"]
        if not isinstance(metrics, dict):
            raise ValueError("native gate metrics must be an object")
        wall = _float(metrics["group_service_wall_s"], "group_service_wall_s")
        latency = run.get("vllm_latency_deltas")
        if not isinstance(latency, dict):
            raise ValueError("native single vLLM deltas missing")
        flops = sum(
            _float(item["vllm_estimated_flops_per_gpu_delta"], "flops")
            for item in latency.values()
            if isinstance(item, dict)
        )
        gpu = run.get("gpu_summary")
        gauge = run.get("gauge_summary")
        if not isinstance(gpu, dict) or not isinstance(gauge, dict):
            raise ValueError("native single resource summary missing")
        power_w = _float(gpu["gpu0_power_mean"], "gpu0_power_mean") + _float(
            gpu["gpu1_power_mean"], "gpu1_power_mean"
        )
        rows.append(
            {
                "system": arm,
                "scenario": "single_short_native",
                "repeat": int(run["repeat"]),
                "short_jct_s": wall,
                "short_p99_s": "",
                "short_actual_work": "",
                "short_work_per_s": "",
                "group_service_tokens_per_s": _float(
                    metrics["group_service_total_tokens_per_s"], "group service tokens/s"
                ),
                "group_gpu_util_pct": _mean(
                    [
                        _float(gpu["gpu0_util_mean"], "gpu0_util_mean"),
                        _float(gpu["gpu1_util_mean"], "gpu1_util_mean"),
                    ]
                ),
                "group_mfu_fraction": flops / (wall * 2 * GPU_PEAK_TFLOPS * 1e12),
                "group_running_mean": _float(gauge["vllm_running_mean"], "running"),
                "group_waiting_mean": _float(gauge["vllm_waiting_mean"], "waiting"),
                "group_kv_mean": _float(gauge["vllm_kv_cache_usage_mean"], "kv"),
                "short_long_overlap_s": 0.0,
                "short_manifest_sha256": str(index["manifest_sha256"]),
                "request_p99_status": "unavailable:native_adapter_boundary",
                "throughput_scope": "short_job_group",
                "group_gpu_energy_j": power_w * wall,
            }
        )
    _assert_native_counts(rows)
    return rows


def _native_single_service_timing_rows(
    root: Path,
    arm_id: str = "daft_native",
) -> list[dict[str, object]]:
    """Expose service-side timing while preserving Daft's barrier timestamp boundary."""

    index = _read_json(root / "matrix_index.json")
    rows: list[dict[str, object]] = []
    metric_fields = (
        "vllm_e2e_request_latency_mean_s",
        "vllm_request_queue_time_mean_s",
        "vllm_request_inference_time_mean_s",
        "vllm_request_prefill_time_mean_s",
        "vllm_request_decode_time_mean_s",
        "vllm_time_to_first_token_mean_s",
        "vllm_inter_token_latency_mean_s",
    )
    for run in index.get("runs", []):
        if (
            not isinstance(run, dict)
            or run.get("phase") != "formal"
            or run.get("arm_id") != arm_id
        ):
            continue
        run_root = Path(str(run["output_root"]))
        if not run_root.exists():
            run_root = root / "runs" / str(run["run_id"])
        gates = tuple(run_root.glob("*/gate.json"))
        if len(gates) != 1:
            raise ValueError(f"expected one gate for {run['run_id']}")
        metrics = _read_json(gates[0]).get("metrics")
        latency = run.get("vllm_latency_deltas")
        if not isinstance(metrics, dict) or not isinstance(latency, dict):
            raise ValueError("native service timing evidence is incomplete")
        endpoint_metrics = [item for item in latency.values() if isinstance(item, dict)]
        wall = _float(metrics["group_service_wall_s"], "group_service_wall_s")
        row: dict[str, object] = {
            "system": arm_id,
            "scenario": "single_short_native",
            "repeat": int(run["repeat"]),
            "input_visibility": "full_manifest_before_timer",
            "timer_start": "immediately_before_daft_collect",
            "timer_end": "after_daft_collect_to_pylist",
            "reported_wall_s": wall,
            "arrival_span_s": 0.0,
            "post_last_arrival_drain_s": wall,
            "client_request_timing_status": "unavailable:barrier_stamped",
            "pretimer_setup_status": "excluded:not_instrumented",
        }
        for field in metric_fields:
            row[field] = _mean(
                _float(item[field], field) for item in endpoint_metrics
            )
        rows.append(row)
    if len(rows) != 3:
        raise ValueError(f"expected three formal {arm_id} timing rows, got {len(rows)}")
    return rows


def _native_single_service_timing_summary(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    first = rows[0]
    output: dict[str, object] = {
        "system": first["system"],
        "scenario": first["scenario"],
        "formal_repeats": len(rows),
        "input_visibility": first["input_visibility"],
        "timer_start": first["timer_start"],
        "timer_end": first["timer_end"],
        "client_request_timing_status": first["client_request_timing_status"],
        "pretimer_setup_status": first["pretimer_setup_status"],
    }
    for field, value in first.items():
        if isinstance(value, (int, float)) and not isinstance(value, bool) and field != "repeat":
            output[f"{field}_mean"] = _mean(_float(row[field], field) for row in rows)
    return output


def _native_two_rows(root: Path) -> list[dict[str, object]]:
    index = _read_json(root / "matrix_index.json")
    if index.get("status") != "passed":
        raise ValueError("native two-job matrix did not pass")
    rows: list[dict[str, object]] = []
    for run in index.get("runs", []):
        if not isinstance(run, dict) or run.get("phase") != "formal":
            continue
        if run.get("status") != "passed" or run.get("exactly_once") is not True:
            raise ValueError(f"native two-job formal failed: {run.get('run_id')}")
        jobs = run.get("jobs")
        if not isinstance(jobs, list) or len(jobs) != 2:
            raise ValueError("native two-job run must contain two jobs")
        by_id = {str(job["job_id"]): job for job in jobs if isinstance(job, dict)}
        short, long = by_id["short"], by_id["long"]
        short_jct = _float(short["job_barrier_jct_s"], "short JCT")
        overlap = max(
            0.0,
            min(_float(short["ended_epoch_s"], "short end"), _float(long["ended_epoch_s"], "long end"))
            - max(
                _float(short["actual_launch_epoch_s"], "short start"),
                _float(long["actual_launch_epoch_s"], "long start"),
            ),
        )
        gpu = run.get("gpu_summary")
        gauge = run.get("gauge_summary")
        latency = run.get("vllm_latency_deltas")
        counters_path = Path(str(run["service_counters"]))
        if not counters_path.exists():
            counters_path = root / "runs" / str(run["run_id"]) / "service_counters.json"
        counters = _read_json(counters_path)
        if not isinstance(gpu, dict) or not isinstance(gauge, dict) or not isinstance(latency, dict):
            raise ValueError("native two-job resource summary missing")
        wall = _float(run["arm_barrier_jct_s"], "arm barrier JCT")
        flops = sum(
            _float(item["vllm_estimated_flops_per_gpu_delta"], "flops")
            for item in latency.values()
            if isinstance(item, dict)
        )
        counter_deltas = counters.get("delta")
        if not isinstance(counter_deltas, dict):
            raise ValueError("native two-job service counter delta missing")
        service_tokens = sum(
            _float(item["prompt_tokens"], "prompt tokens")
            + _float(item["generation_tokens"], "generation tokens")
            for item in counter_deltas.values()
            if isinstance(item, dict)
        )
        rows.append(
            {
                "system": str(run["arm_id"]),
                "scenario": "staggered_short_long_native",
                "repeat": int(run["repeat"]),
                "short_jct_s": short_jct,
                "short_p99_s": "",
                "short_actual_work": "",
                "short_work_per_s": "",
                "group_service_tokens_per_s": service_tokens / wall,
                "group_gpu_util_pct": _mean(
                    [
                        _float(gpu["gpu0_util_mean"], "gpu0_util_mean"),
                        _float(gpu["gpu1_util_mean"], "gpu1_util_mean"),
                    ]
                ),
                "group_mfu_fraction": flops / (wall * 2 * GPU_PEAK_TFLOPS * 1e12),
                "group_running_mean": _float(gauge["vllm_running_mean"], "running"),
                "group_waiting_mean": _float(gauge["vllm_waiting_mean"], "waiting"),
                "group_kv_mean": _float(gauge["vllm_kv_cache_usage_mean"], "kv"),
                "short_long_overlap_s": overlap,
                "short_manifest_sha256": str(short["manifest_sha256"]),
                "request_p99_status": "unavailable:native_adapter_boundary",
                "throughput_scope": "group_all_active_jobs",
                "group_gpu_energy_j": (
                    _float(gpu["gpu0_power_mean"], "gpu0_power_mean")
                    + _float(gpu["gpu1_power_mean"], "gpu1_power_mean")
                )
                * wall,
            }
        )
    _assert_native_counts(rows)
    return rows


def _assert_native_counts(rows: Sequence[Mapping[str, object]]) -> None:
    counts = defaultdict(int)
    for row in rows:
        counts[str(row["system"])] += 1
    expected = {arm: 3 for arm in EXPECTED_NATIVE_ARMS}
    if counts != expected:
        raise ValueError(f"native formal repeat mismatch: {dict(counts)}")


def _aggregate(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["system"]), str(row["scenario"]))].append(row)
    output: list[dict[str, object]] = []
    numeric_fields = (
        "short_jct_s",
        "short_p99_s",
        "short_work_per_s",
        "group_service_tokens_per_s",
        "group_gpu_util_pct",
        "group_mfu_fraction",
        "group_running_mean",
        "group_waiting_mean",
        "group_kv_mean",
        "group_gpu_energy_j",
        "short_long_overlap_s",
    )
    for (system, scenario), items in sorted(grouped.items()):
        row: dict[str, object] = {
            "system": system,
            "scenario": scenario,
            "formal_repeats": len(items),
            "request_p99_status": items[0]["request_p99_status"],
            "throughput_scope": items[0]["throughput_scope"],
            "short_manifest_sha256": items[0]["short_manifest_sha256"],
        }
        for field in numeric_fields:
            values = [
                _float(item[field], field)
                for item in items
                if item.get(field) not in {None, ""}
            ]
            if not values:
                row[f"{field}_mean"] = ""
                row[f"{field}_sd"] = ""
                row[f"{field}_cv"] = ""
                continue
            mean, sd, cv = _summary(values)
            row[f"{field}_mean"] = mean
            row[f"{field}_sd"] = sd
            row[f"{field}_cv"] = cv
        output.append(row)
    return output


def _comparison(
    summary: Mapping[tuple[str, str], Mapping[str, object]],
    comparison_id: str,
    baseline: tuple[str, str],
    candidate: tuple[str, str],
    causal_status: str,
) -> dict[str, object]:
    before, after = summary[baseline], summary[candidate]

    def delta(field: str) -> object:
        left, right = before.get(field), after.get(field)
        if left in {None, "", 0, 0.0} or right in {None, ""}:
            return ""
        return (_float(right, field) / _float(left, field) - 1.0) * 100.0

    return {
        "comparison_id": comparison_id,
        "baseline": f"{baseline[0]}:{baseline[1]}",
        "candidate": f"{candidate[0]}:{candidate[1]}",
        "causal_status": causal_status,
        "short_jct_delta_pct": delta("short_jct_s_mean"),
        "short_p99_delta_pct": delta("short_p99_s_mean"),
        "short_work_rate_delta_pct": delta("short_work_per_s_mean"),
        "candidate_overlap_s_mean": after["short_long_overlap_s_mean"],
        "group_mfu_delta_pct": delta("group_mfu_fraction_mean"),
        "group_gpu_util_delta_pct": delta("group_gpu_util_pct_mean"),
        "note": (
            "group throughput/MFU/util include all active jobs; short work rate is only available "
            "for project request traces"
        ),
    }


def _project_phase_rows(root: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for record_path in sorted((root / "records").glob("*.json")):
        record = _read_json(record_path)
        if record.get("phase") != "formal":
            continue
        starts = [_float(value, "replay start") for value in _json_list(record["replay_configured_start_epoch_s"])]
        jct = [_float(value, "job JCT") for value in _json_list(record["job_jct_s"])]
        if len(starts) != 2:
            raise ValueError("phase summary requires two jobs")
        trace = root / "traces" / (
            f"{int(record['order_index']):03d}_formal_{int(record['repeat_index'])}_"
            f"{record['scenario_id']}.resources.csv"
        )
        samples_by_epoch: dict[float, list[dict[str, str]]] = defaultdict(list)
        with trace.open(encoding="utf-8", newline="") as handle:
            for item in csv.DictReader(handle):
                samples_by_epoch[_float(item["observed_epoch_s"], "observed_epoch_s")].append(item)
        points = []
        for epoch, endpoint_rows in sorted(samples_by_epoch.items()):
            points.append(
                {
                    "epoch": epoch,
                    "gpu": _mean(_float(item["gpu_utilization_pct"], "gpu") for item in endpoint_rows),
                    "running": sum(_float(item["running"], "running") for item in endpoint_rows),
                    "waiting": sum(_float(item["waiting"], "waiting") for item in endpoint_rows),
                    "kv": _mean(_float(item["kv_usage"], "kv") for item in endpoint_rows),
                }
            )
        short_end = starts[0] + jct[0]
        request_trace = root / "jobs" / (
            f"{int(record['order_index']):03d}_formal_{int(record['repeat_index'])}_"
            f"{record['scenario_id']}_job0.requests.csv"
        )
        short_completions: list[tuple[float, float]] = []
        with request_trace.open(encoding="utf-8", newline="") as handle:
            for item in csv.DictReader(handle):
                if item.get("status") != "completed":
                    continue
                short_completions.append(
                    (
                        _float(item["completion_epoch_s"], "completion_epoch_s"),
                        _float(item["prompt_tokens"], "prompt_tokens")
                        + _float(item["actual_output_tokens"], "actual_output_tokens"),
                    )
                )
        if len(short_completions) != 512:
            raise ValueError("project short request trace is not exactly-once")
        boundaries = (
            ("pre_long", starts[0], starts[1]),
            ("overlap", starts[1], short_end),
            ("long_drain", short_end, _float(record["end_epoch_s"], "end_epoch_s")),
        )
        for phase, begin, end in boundaries:
            selected = [point for point in points if begin <= point["epoch"] < end]
            completed = [work for epoch, work in short_completions if begin <= epoch < end]
            duration = max(0.0, end - begin)
            rows.append(
                {
                    "scenario": record["scenario_id"],
                    "repeat": int(record["repeat_index"]),
                    "phase": phase,
                    "duration_s": duration,
                    "samples": len(selected),
                    "gpu_util_pct_mean": _mean(point["gpu"] for point in selected) if selected else "",
                    "running_total_mean": _mean(point["running"] for point in selected) if selected else "",
                    "waiting_total_mean": _mean(point["waiting"] for point in selected) if selected else "",
                    "kv_per_endpoint_mean": _mean(point["kv"] for point in selected) if selected else "",
                    "short_completed_requests": len(completed),
                    "short_completed_work": sum(completed),
                    "short_completed_work_per_s": sum(completed) / duration if duration else "",
                    "mfu_status": "unavailable:no_interval_flops_counter",
                }
            )
    return rows


def _project_request_timing_metrics(
    request_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Decompose one project Job without treating overlapping stages as additive."""

    if not request_rows:
        raise ValueError("project request timing requires non-empty rows")
    required = (
        "arrival_epoch_s",
        "flush_epoch_s",
        "submit_epoch_s",
        "service_start_epoch_s",
        "completion_epoch_s",
        "e2e_s",
    )
    if any(row.get(field) in {None, ""} for row in request_rows for field in required):
        raise ValueError("project request timing contains missing timestamps")
    arrival = [_float(row["arrival_epoch_s"], "arrival") for row in request_rows]
    flush = [_float(row["flush_epoch_s"], "flush") for row in request_rows]
    submit = [_float(row["submit_epoch_s"], "submit") for row in request_rows]
    service_start = [
        _float(row["service_start_epoch_s"], "service start") for row in request_rows
    ]
    completion = [
        _float(row["completion_epoch_s"], "completion") for row in request_rows
    ]
    e2e = [_float(row["e2e_s"], "e2e") for row in request_rows]
    buffer = [right - left for left, right in zip(arrival, flush)]
    flush_to_submit = [right - left for left, right in zip(flush, submit)]
    submit_to_service = [right - left for left, right in zip(submit, service_start)]
    service = [right - left for left, right in zip(service_start, completion)]
    for name, values in (
        ("buffer", buffer),
        ("flush_to_submit", flush_to_submit),
        ("submit_to_service", submit_to_service),
        ("service", service),
        ("e2e", e2e),
    ):
        if any(value < -1e-6 for value in values):
            raise ValueError(f"negative {name} duration")
    jct = max(completion) - min(arrival)
    arrival_span = max(arrival) - min(arrival)
    completion_lag = max(completion) - max(arrival)
    if not math.isclose(jct, arrival_span + completion_lag, abs_tol=1e-6):
        raise ValueError("JCT does not equal arrival span plus completion lag")
    output: dict[str, object] = {
        "request_count": len(request_rows),
        "jct_s": jct,
        "arrival_span_s": arrival_span,
        "post_last_arrival_drain_s": completion_lag,
        "arrival_span_fraction": arrival_span / jct if jct else 0.0,
        "submit_span_s": max(submit) - min(submit),
        "completion_span_s": max(completion) - min(completion),
    }
    for name, values in (
        ("buffer_s", buffer),
        ("flush_to_submit_s", flush_to_submit),
        ("submit_to_service_s", submit_to_service),
        ("service_s", service),
        ("request_e2e_s", e2e),
    ):
        output[f"{name}_mean"] = _mean(values)
        output[f"{name}_p50"] = _percentile(values, 0.50)
        output[f"{name}_p95"] = _percentile(values, 0.95)
        output[f"{name}_p99"] = _percentile(values, 0.99)
        output[f"{name}_max"] = max(values)
    return output


def _project_request_timing_rows(root: Path) -> list[dict[str, object]]:
    """Read formal Job-0 profiler evidence and preserve its stage timing fields."""

    profiler_fields = (
        "db_fetch_s",
        "arrow_build_s",
        "source_fetch_s",
        "organizer_from_arrow_s",
        "organizer_plan_s",
        "organizer_collect_s",
        "actor_ready_s",
        "submit_s",
        "model_service_s",
        "model_request_wall_s",
        "operator_wall_s",
        "bounded_wait_s",
        "fanin_s",
        "scheduling_control_overhead_s",
        "vllm_e2e_request_latency_mean_s",
        "vllm_request_queue_time_mean_s",
        "vllm_request_inference_time_mean_s",
        "vllm_request_prefill_time_mean_s",
        "vllm_request_decode_time_mean_s",
        "vllm_time_to_first_token_mean_s",
        "vllm_inter_token_latency_mean_s",
        "e2e_s",
    )
    output: list[dict[str, object]] = []
    for record_path in sorted((root / "records").glob("*.json")):
        record = _read_json(record_path)
        if record.get("phase") != "formal":
            continue
        stem = (
            f"{int(record['order_index']):03d}_formal_{int(record['repeat_index'])}_"
            f"{record['scenario_id']}_job0"
        )
        with (root / "jobs" / f"{stem}.requests.csv").open(
            encoding="utf-8", newline=""
        ) as handle:
            requests = list(csv.DictReader(handle))
        with (root / "jobs" / f"{stem}.runs.csv").open(
            encoding="utf-8", newline=""
        ) as handle:
            summaries = list(csv.DictReader(handle))
        if len(summaries) != 1 or summaries[0].get("status") != "ok":
            raise ValueError(f"project Job-0 summary is not uniquely successful: {stem}")
        row: dict[str, object] = {
            "scenario": record["scenario_id"],
            "repeat": int(record["repeat_index"]),
            "timer_start": "min_request_arrival",
            "timer_end": "max_request_completion",
            "input_visibility": "request_level_arrival_replay",
            **_project_request_timing_metrics(requests),
        }
        for field in profiler_fields:
            value = summaries[0].get(field)
            row[f"profiler_{field}"] = (
                _float(value, field) if value not in {None, ""} else ""
            )
        row["profiler_stage_boundary"] = "overlapping_fields_do_not_sum"
        output.append(row)
    return output


def _project_request_timing_summary(
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["scenario"])].append(row)
    output: list[dict[str, object]] = []
    for scenario, items in sorted(grouped.items()):
        result: dict[str, object] = {
            "scenario": scenario,
            "formal_repeats": len(items),
            "timer_start": items[0]["timer_start"],
            "timer_end": items[0]["timer_end"],
            "input_visibility": items[0]["input_visibility"],
        }
        numeric_fields = [
            field
            for field, value in items[0].items()
            if field not in {"scenario", "repeat"}
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
        ]
        for field in numeric_fields:
            result[f"{field}_mean"] = _mean(
                _float(item[field], field) for item in items
            )
        result["profiler_stage_boundary"] = "overlapping_fields_do_not_sum"
        output.append(result)
    return output


def _single_short_boundary_rows(
    project_timing: Mapping[str, object],
    native_timing: Mapping[str, object],
    group_summary: Mapping[tuple[str, str], Mapping[str, object]],
) -> list[dict[str, object]]:
    """Align only named timing boundaries; blanks remain explicitly unavailable."""

    project_group = group_summary[("project", "single_short_full_pool")]
    native_group = group_summary[("daft_native", "single_short_native")]
    fields = (
        "system",
        "input_visibility",
        "timer_start",
        "timer_end",
        "reported_wall_s_mean",
        "arrival_span_s_mean",
        "post_last_arrival_drain_s_mean",
        "arrival_span_fraction_mean",
        "buffer_s_mean_mean",
        "buffer_s_p99_mean",
        "flush_to_submit_s_mean_mean",
        "flush_to_submit_s_p99_mean",
        "submit_to_service_s_mean_mean",
        "submit_to_service_s_p99_mean",
        "service_s_mean_mean",
        "service_s_p99_mean",
        "request_e2e_s_mean_mean",
        "request_e2e_s_p99_mean",
        "vllm_e2e_request_latency_mean_s",
        "vllm_request_queue_time_mean_s",
        "vllm_request_inference_time_mean_s",
        "vllm_request_prefill_time_mean_s",
        "vllm_request_decode_time_mean_s",
        "vllm_time_to_first_token_mean_s",
        "vllm_inter_token_latency_mean_s",
        "source_fetch_s_mean",
        "actor_ready_s_mean",
        "profiler_e2e_s_mean",
        "group_service_tokens_per_s_mean",
        "group_mfu_pct_mean",
        "group_running_mean",
        "group_waiting_mean",
        "group_kv_pct_mean",
        "timing_boundary",
    )
    project = {
        "system": "project",
        "input_visibility": project_timing["input_visibility"],
        "timer_start": project_timing["timer_start"],
        "timer_end": project_timing["timer_end"],
        "reported_wall_s_mean": project_timing["jct_s_mean"],
        "arrival_span_s_mean": project_timing["arrival_span_s_mean"],
        "post_last_arrival_drain_s_mean": project_timing[
            "post_last_arrival_drain_s_mean"
        ],
        "arrival_span_fraction_mean": project_timing["arrival_span_fraction_mean"],
        "buffer_s_mean_mean": project_timing["buffer_s_mean_mean"],
        "buffer_s_p99_mean": project_timing["buffer_s_p99_mean"],
        "flush_to_submit_s_mean_mean": project_timing["flush_to_submit_s_mean_mean"],
        "flush_to_submit_s_p99_mean": project_timing["flush_to_submit_s_p99_mean"],
        "submit_to_service_s_mean_mean": project_timing[
            "submit_to_service_s_mean_mean"
        ],
        "submit_to_service_s_p99_mean": project_timing[
            "submit_to_service_s_p99_mean"
        ],
        "service_s_mean_mean": project_timing["service_s_mean_mean"],
        "service_s_p99_mean": project_timing["service_s_p99_mean"],
        "request_e2e_s_mean_mean": project_timing["request_e2e_s_mean_mean"],
        "request_e2e_s_p99_mean": project_timing["request_e2e_s_p99_mean"],
        "vllm_e2e_request_latency_mean_s": project_timing[
            "profiler_vllm_e2e_request_latency_mean_s_mean"
        ],
        "vllm_request_queue_time_mean_s": project_timing[
            "profiler_vllm_request_queue_time_mean_s_mean"
        ],
        "vllm_request_inference_time_mean_s": project_timing[
            "profiler_vllm_request_inference_time_mean_s_mean"
        ],
        "vllm_request_prefill_time_mean_s": project_timing[
            "profiler_vllm_request_prefill_time_mean_s_mean"
        ],
        "vllm_request_decode_time_mean_s": project_timing[
            "profiler_vllm_request_decode_time_mean_s_mean"
        ],
        "vllm_time_to_first_token_mean_s": project_timing[
            "profiler_vllm_time_to_first_token_mean_s_mean"
        ],
        "vllm_inter_token_latency_mean_s": project_timing[
            "profiler_vllm_inter_token_latency_mean_s_mean"
        ],
        "source_fetch_s_mean": project_timing["profiler_source_fetch_s_mean"],
        "actor_ready_s_mean": project_timing["profiler_actor_ready_s_mean"],
        "profiler_e2e_s_mean": project_timing["profiler_e2e_s_mean"],
        "group_service_tokens_per_s_mean": project_group[
            "group_service_tokens_per_s_mean"
        ],
        "group_mfu_pct_mean": _float(
            project_group["group_mfu_fraction_mean"], "project MFU"
        )
        * 100.0,
        "group_running_mean": project_group["group_running_mean_mean"],
        "group_waiting_mean": project_group["group_waiting_mean_mean"],
        "group_kv_pct_mean": _float(project_group["group_kv_mean_mean"], "project KV")
        * 100.0,
        "timing_boundary": "arrival_replay_jct;profiler_stages_overlap_do_not_sum",
    }
    native = {
        "system": "daft_native",
        "input_visibility": native_timing["input_visibility"],
        "timer_start": native_timing["timer_start"],
        "timer_end": native_timing["timer_end"],
        "reported_wall_s_mean": native_timing["reported_wall_s_mean"],
        "arrival_span_s_mean": native_timing["arrival_span_s_mean"],
        "post_last_arrival_drain_s_mean": native_timing[
            "post_last_arrival_drain_s_mean"
        ],
        "vllm_e2e_request_latency_mean_s": native_timing[
            "vllm_e2e_request_latency_mean_s_mean"
        ],
        "vllm_request_queue_time_mean_s": native_timing[
            "vllm_request_queue_time_mean_s_mean"
        ],
        "vllm_request_inference_time_mean_s": native_timing[
            "vllm_request_inference_time_mean_s_mean"
        ],
        "vllm_request_prefill_time_mean_s": native_timing[
            "vllm_request_prefill_time_mean_s_mean"
        ],
        "vllm_request_decode_time_mean_s": native_timing[
            "vllm_request_decode_time_mean_s_mean"
        ],
        "vllm_time_to_first_token_mean_s": native_timing[
            "vllm_time_to_first_token_mean_s_mean"
        ],
        "vllm_inter_token_latency_mean_s": native_timing[
            "vllm_inter_token_latency_mean_s_mean"
        ],
        "group_service_tokens_per_s_mean": native_group[
            "group_service_tokens_per_s_mean"
        ],
        "group_mfu_pct_mean": _float(
            native_group["group_mfu_fraction_mean"], "native MFU"
        )
        * 100.0,
        "group_running_mean": native_group["group_running_mean_mean"],
        "group_waiting_mean": native_group["group_waiting_mean_mean"],
        "group_kv_pct_mean": _float(native_group["group_kv_mean_mean"], "native KV")
        * 100.0,
        "timing_boundary": "collect_wall;full_manifest_and_graph_setup_before_timer",
    }
    return [{field: row.get(field, "") for field in fields} for row in (project, native)]


def _phase_summary(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["scenario"]), str(row["phase"]))].append(row)
    output = []
    for (scenario, phase), items in sorted(grouped.items()):
        output.append(
            {
                "scenario": scenario,
                "phase": phase,
                "formal_repeats": len(items),
                "duration_s_mean": _mean(_float(item["duration_s"], "duration") for item in items),
                "gpu_util_pct_mean": _mean(_float(item["gpu_util_pct_mean"], "gpu") for item in items),
                "running_total_mean": _mean(_float(item["running_total_mean"], "running") for item in items),
                "waiting_total_mean": _mean(_float(item["waiting_total_mean"], "waiting") for item in items),
                "kv_per_endpoint_mean": _mean(_float(item["kv_per_endpoint_mean"], "kv") for item in items),
                "short_completed_requests_mean": _mean(
                    _float(item["short_completed_requests"], "short completed requests")
                    for item in items
                ),
                "short_completed_work_per_s_mean": _mean(
                    _float(item["short_completed_work_per_s"], "short completed work rate")
                    for item in items
                ),
                "mfu_status": "unavailable:no_interval_flops_counter",
            }
        )
    return output


def summarize(args: argparse.Namespace) -> dict[str, object]:
    project_single = _project_rows(
        args.project_single_root,
        {"single_short_full_pool", "single_short_half_pool"},
    )
    project_two = _project_rows(
        args.project_two_root,
        {"staggered_static_partition", "staggered_shared_work"},
    )
    native_single = _native_single_rows(args.native_single_root)
    native_two = _native_two_rows(args.native_two_root)
    all_rows = project_single + project_two + native_single + native_two
    manifest_shas = {str(row["short_manifest_sha256"]) for row in all_rows}
    if len(manifest_shas) != 1:
        raise ValueError(f"short manifest mismatch: {sorted(manifest_shas)}")
    summary_rows = _aggregate(all_rows)
    by_key = {(str(row["system"]), str(row["scenario"])): row for row in summary_rows}
    comparisons = [
        _comparison(
            by_key,
            "project_half_quota_only",
            ("project", "single_short_full_pool"),
            ("project", "single_short_half_pool"),
            "causal:quota_only",
        ),
        _comparison(
            by_key,
            "project_long_competition_static",
            ("project", "single_short_half_pool"),
            ("project", "staggered_static_partition"),
            "causal:matched_local_cap",
        ),
        _comparison(
            by_key,
            "project_long_competition_shared",
            ("project", "single_short_full_pool"),
            ("project", "staggered_shared_work"),
            "causal:matched_global_cap",
        ),
    ]
    for arm in EXPECTED_NATIVE_ARMS:
        overlap = _float(
            by_key[(arm, "staggered_short_long_native")]["short_long_overlap_s_mean"],
            "overlap",
        )
        comparisons.append(
            _comparison(
                by_key,
                f"{arm}_native_two_job_observation",
                (arm, "single_short_native"),
                (arm, "staggered_short_long_native"),
                "observational:overlap_present" if overlap > 0 else "not_causal:no_overlap",
            )
        )
    phase_rows = _project_phase_rows(args.project_two_root)
    project_request_timing_rows = (
        _project_request_timing_rows(args.project_single_root)
        + _project_request_timing_rows(args.project_two_root)
    )
    project_request_timing_summary = _project_request_timing_summary(
        project_request_timing_rows
    )
    native_service_timing_rows = _native_single_service_timing_rows(
        args.native_single_root
    )
    native_service_timing_summary = _native_single_service_timing_summary(
        native_service_timing_rows
    )
    project_single_timing = next(
        row
        for row in project_request_timing_summary
        if row["scenario"] == "single_short_full_pool"
    )
    single_short_boundary_rows = _single_short_boundary_rows(
        project_single_timing,
        native_service_timing_summary,
        by_key,
    )
    args.output.mkdir(parents=True, exist_ok=False)
    _write_csv(args.output / "formal_runs.csv", all_rows)
    _write_csv(args.output / "summary.csv", summary_rows)
    _write_csv(args.output / "comparisons.csv", comparisons)
    _write_csv(args.output / "project_phase_runs.csv", phase_rows)
    _write_csv(args.output / "project_phase_summary.csv", _phase_summary(phase_rows))
    _write_csv(
        args.output / "project_request_timing_runs.csv",
        project_request_timing_rows,
    )
    _write_csv(
        args.output / "project_request_timing_summary.csv",
        project_request_timing_summary,
    )
    _write_csv(
        args.output / "native_single_service_timing_runs.csv",
        native_service_timing_rows,
    )
    _write_csv(
        args.output / "single_short_project_daft_timing.csv",
        single_short_boundary_rows,
    )
    audit = {
        "status": "passed",
        "short_manifest_sha256": next(iter(manifest_shas)),
        "formal_rows": len(all_rows),
        "summary_rows": len(summary_rows),
        "comparisons": len(comparisons),
        "project_phase_rows": len(phase_rows),
        "project_request_timing_rows": len(project_request_timing_rows),
        "native_single_service_timing_rows": len(native_service_timing_rows),
        "single_short_project_daft_timing_rows": len(single_short_boundary_rows),
        "project_request_timing_identity": "jct=arrival_span+post_last_arrival_drain",
        "project_profiler_stage_boundary": "overlapping_fields_do_not_sum",
        "request_p99_boundary": "project_only",
        "interval_mfu_boundary": "unavailable:no_interval_flops_counter",
        "native_short_duration_boundary": "characterization_not_60s_capacity_ranking",
    }
    (args.output / "audit.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return audit


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-single-root", type=Path, required=True)
    parser.add_argument("--project-two-root", type=Path, required=True)
    parser.add_argument("--native-single-root", type=Path, required=True)
    parser.add_argument("--native-two-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = summarize(args)
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": f"{type(exc).__name__}: {exc}"}))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
