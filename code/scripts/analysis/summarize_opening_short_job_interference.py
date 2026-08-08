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
                "short_long_overlap_s": max(0.0, jct[0] - 15.0) if len(jct) == 2 else 0.0,
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
        gates = glob.glob(str(Path(str(run["output_root"])) / "*" / "gate.json"))
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
        counters = _read_json(Path(str(run["service_counters"])))
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
    args.output.mkdir(parents=True, exist_ok=False)
    _write_csv(args.output / "formal_runs.csv", all_rows)
    _write_csv(args.output / "summary.csv", summary_rows)
    _write_csv(args.output / "comparisons.csv", comparisons)
    _write_csv(args.output / "project_phase_runs.csv", phase_rows)
    _write_csv(args.output / "project_phase_summary.csv", _phase_summary(phase_rows))
    audit = {
        "status": "passed",
        "short_manifest_sha256": next(iter(manifest_shas)),
        "formal_rows": len(all_rows),
        "summary_rows": len(summary_rows),
        "comparisons": len(comparisons),
        "project_phase_rows": len(phase_rows),
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
