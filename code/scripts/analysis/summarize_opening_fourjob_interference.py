#!/usr/bin/env python3
"""Audit and summarize matched single versus 1-short+3-long interference.

The script emits job-, group-, and phase-level CSVs without drawing figures.
Native framework request tails stay explicitly unavailable because those
adapters expose barrier timestamps; Project request-level P95/P99 are recomputed
from raw request evidence.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Mapping, Sequence


JOBS = ("short", "long1", "long2", "long3")
CONCURRENT_PROJECT = {
    "staggered_fourjob_static_partition": "static_partition",
    "staggered_fourjob_shared_work": "shared_work",
}


def _json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _list(value: object, field: str) -> list[object]:
    decoded = json.loads(value) if isinstance(value, str) else value
    if not isinstance(decoded, list):
        raise ValueError(f"{field} must be a list")
    return decoded


def _float(value: object, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{field} must be finite")
    return parsed


def _mean(values: Iterable[float]) -> float:
    rows = list(values)
    if not rows:
        raise ValueError("cannot average empty values")
    return statistics.mean(rows)


def _percentile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("cannot take percentile of empty values")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    if not rows:
        raise ValueError(f"refusing to write empty CSV: {path}")
    fields: list[str] = []
    for row in rows:
        for field in row:
            if field not in fields:
                fields.append(field)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows({field: row.get(field, "") for field in fields} for row in rows)


def _scenario_jobs(scenario: str, count: int) -> tuple[str, ...]:
    if count == 4 and scenario in CONCURRENT_PROJECT:
        return JOBS
    if count == 1 and scenario.startswith("single_"):
        for job in JOBS:
            if scenario.startswith(f"single_{job}_"):
                return (job,)
    raise ValueError(f"unsupported project scenario/job_count: {scenario}/{count}")


def _pairwise_overlap(starts: Sequence[float], ends: Sequence[float], index: int) -> dict[str, float]:
    return {
        JOBS[other]: max(0.0, min(ends[index], ends[other]) - max(starts[index], starts[other]))
        for other in range(len(starts))
        if other != index
    }


def _project_rows(root: Path) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    manifest = _json(root / "manifest.json")
    if manifest.get("status") != "completed":
        raise ValueError("Project matrix is not completed")
    job_rows: list[dict[str, object]] = []
    group_rows: list[dict[str, object]] = []
    counts: defaultdict[str, int] = defaultdict(int)
    for path in sorted((root / "records").glob("*.json")):
        record = _json(path)
        if record.get("phase") != "formal":
            continue
        scenario = str(record["scenario_id"])
        count = int(record["job_count"])
        names = _scenario_jobs(scenario, count)
        if len(names) != count:
            raise ValueError(f"job name/count mismatch: {path}")
        jcts = [_float(v, "job_jct_s") for v in _list(record["job_jct_s"], "job_jct_s")]
        p99s = [_float(v, "job_p99_s") for v in _list(record["job_p99_s"], "job_p99_s")]
        work = [_float(v, "job_actual_work") for v in _list(record["job_actual_work"], "job_actual_work")]
        starts = [
            _float(v, "replay_configured_start_epoch_s")
            for v in _list(record["replay_configured_start_epoch_s"], "starts")
        ]
        shas = [str(v) for v in _list(record["request_manifest_sha256"], "manifest sha")]
        if not all(len(values) == count for values in (jcts, p99s, work, starts, shas)):
            raise ValueError(f"Project job vector length mismatch: {path}")
        ends = [start + jct for start, jct in zip(starts, jcts)]
        for index, name in enumerate(names):
            request_path = root / "jobs" / f"{path.stem}_job{index}.requests.csv"
            requests = _read_csv(request_path)
            completed = [row for row in requests if row["status"] == "completed"]
            if len(requests) != 512 or len(completed) != 512:
                raise ValueError(f"Project exactly-once failure: {request_path}")
            e2e = [_float(row["e2e_s"], "request e2e") for row in completed]
            job_rows.append(
                {
                    "system": "project",
                    "scenario": scenario,
                    "policy": CONCURRENT_PROJECT.get(
                        scenario, "single_quarter" if "quarter_pool" in scenario else "single_full"
                    ),
                    "repeat": int(record["repeat_index"]),
                    "run_stem": path.stem,
                    "job": name,
                    "job_index": index,
                    "job_jct_s": jcts[index],
                    "request_p95_s": _percentile(e2e, 0.95),
                    "request_p99_s": _percentile(e2e, 0.99),
                    "request_tail_status": "observed:request_timestamp",
                    "actual_work": work[index],
                    "work_per_s": work[index] / jcts[index],
                    "start_epoch_s": starts[index],
                    "end_epoch_s": ends[index],
                    "overlap_with_any_s": max(_pairwise_overlap(starts, ends, index).values(), default=0.0),
                    "pairwise_overlap_s": json.dumps(_pairwise_overlap(starts, ends, index), sort_keys=True),
                    "manifest_sha256": shas[index],
                    "completed_rows": len(completed),
                    "exactly_once": True,
                }
            )
        group_rows.append(
            {
                "system": "project",
                "scenario": scenario,
                "policy": CONCURRENT_PROJECT.get(
                    scenario, "single_quarter" if "quarter_pool" in scenario else "single_full"
                ),
                "repeat": int(record["repeat_index"]),
                "run_stem": path.stem,
                "job_count": count,
                "group_jct_s": _float(record["duration_s"], "duration_s"),
                "group_tokens_per_s": _float(record["tokens_per_s"], "tokens_per_s"),
                "jain_fairness": _float(record["jain_fairness"], "jain_fairness"),
                "gpu_util_pct_mean": _float(record["gpu_utilization_pct_mean"], "gpu util"),
                "mfu_fraction": _float(record["mfu_estimate"], "MFU"),
                "running_mean": _float(record["vllm_running_mean"], "running"),
                "waiting_mean": _float(record["vllm_waiting_mean"], "waiting"),
                "kv_fraction_mean": _float(record["vllm_kv_usage_mean"], "KV"),
                "normalized_service_disparity": record.get("normalized_cumulative_service_disparity_ratio", ""),
                "exactly_once": True,
            }
        )
        counts[scenario] += 1
    expected = {
        *(f"single_{job}_full_pool" for job in JOBS),
        *(f"single_{job}_quarter_pool" for job in JOBS),
        *CONCURRENT_PROJECT,
    }
    if counts != defaultdict(int, {scenario: 3 for scenario in expected}):
        raise ValueError(f"Project formal repeat/scenario mismatch: {dict(counts)}")
    return job_rows, group_rows


def _native_rows(root: Path) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    index = _json(root / "matrix_index.json")
    if index.get("status") != "passed" or index.get("comparison_admission") != "admissible":
        raise ValueError("native four-job matrix is not admissible")
    job_rows: list[dict[str, object]] = []
    group_rows: list[dict[str, object]] = []
    counts: defaultdict[str, int] = defaultdict(int)
    for run in index.get("runs", []):
        if not isinstance(run, dict) or run.get("phase") != "formal":
            continue
        if run.get("status") != "passed" or run.get("exactly_once") is not True:
            raise ValueError(f"native formal failure: {run.get('run_id')}")
        adapter = str(run["adapter"])
        arm = str(run["arm_id"])
        jobs = run.get("jobs")
        if not isinstance(jobs, list) or len(jobs) not in {1, 4}:
            raise ValueError(f"native arm job count mismatch: {arm}")
        scenario = "fourjob" if len(jobs) == 4 else "single_full"
        starts = [_float(job["actual_launch_epoch_s"], "native start") for job in jobs if isinstance(job, dict)]
        ends = [_float(job["ended_epoch_s"], "native end") for job in jobs if isinstance(job, dict)]
        for position, job in enumerate(jobs):
            if not isinstance(job, dict) or job.get("status") != "passed" or job.get("exactly_once") is not True:
                raise ValueError(f"native job failure: {run.get('run_id')}")
            name = str(job["job_id"])
            if name not in JOBS or int(job["completed_count"]) != 512:
                raise ValueError(f"native job identity/count mismatch: {job}")
            jct = _float(job["job_barrier_jct_s"], "native job JCT")
            total_tokens = _float(job["total_tokens"], "native total tokens")
            job_rows.append(
                {
                    "system": adapter,
                    "scenario": scenario,
                    "policy": "native_independent_jobs" if len(jobs) == 4 else "single_full",
                    "repeat": int(run["repeat"]),
                    "run_stem": str(run["run_id"]),
                    "job": name,
                    "job_index": position,
                    "job_jct_s": jct,
                    "request_p95_s": "",
                    "request_p99_s": "",
                    "request_tail_status": "unavailable:native_adapter_barrier_timestamp",
                    "actual_work": total_tokens,
                    "work_per_s": total_tokens / jct,
                    "start_epoch_s": starts[position],
                    "end_epoch_s": ends[position],
                    "overlap_with_any_s": max(_pairwise_overlap(starts, ends, position).values(), default=0.0),
                    "pairwise_overlap_s": json.dumps(_pairwise_overlap(starts, ends, position), sort_keys=True),
                    "manifest_sha256": str(job["manifest_sha256"]),
                    "completed_rows": int(job["completed_count"]),
                    "exactly_once": True,
                }
            )
        gpu = run.get("gpu_summary")
        gauge = run.get("gauge_summary")
        latency = run.get("vllm_latency_deltas")
        if not isinstance(gpu, dict) or not isinstance(gauge, dict) or not isinstance(latency, dict):
            raise ValueError(f"native resource summary missing: {run.get('run_id')}")
        wall = _float(run["arm_barrier_jct_s"], "native group JCT")
        flops = sum(
            _float(item["vllm_estimated_flops_per_gpu_delta"], "native flops")
            for item in latency.values() if isinstance(item, dict)
        )
        group_rows.append(
            {
                "system": adapter,
                "scenario": scenario,
                "policy": "native_independent_jobs" if len(jobs) == 4 else "single_full",
                "repeat": int(run["repeat"]),
                "run_stem": str(run["run_id"]),
                "job_count": len(jobs),
                "group_jct_s": wall,
                "group_tokens_per_s": _float(run["group_barrier_tokens_per_s"], "native tokens/s"),
                "jain_fairness": "",
                "gpu_util_pct_mean": _mean(
                    [_float(gpu["gpu0_util_mean"], "gpu0 util"), _float(gpu["gpu1_util_mean"], "gpu1 util")]
                ),
                "mfu_fraction": flops / (wall * 2 * 165.0e12),
                "running_mean": _float(gauge["vllm_running_mean"], "native running"),
                "waiting_mean": _float(gauge["vllm_waiting_mean"], "native waiting"),
                "kv_fraction_mean": _float(gauge["vllm_kv_cache_usage_mean"], "native KV"),
                "normalized_service_disparity": "unavailable:native_adapter_boundary",
                "exactly_once": True,
            }
        )
        counts[arm] += 1
    expected = {
        *(f"{adapter}_single_{job}" for adapter in ("daft_native", "daft_ray", "ray_data_http") for job in JOBS),
        *(f"{adapter}_fourjob" for adapter in ("daft_native", "daft_ray", "ray_data_http")),
    }
    if counts != defaultdict(int, {arm: 3 for arm in expected}):
        raise ValueError(f"native formal repeat/arm mismatch: {dict(counts)}")
    return job_rows, group_rows


def _mean_resource_samples(rows: Sequence[Mapping[str, str]]) -> dict[str, object]:
    by_epoch: defaultdict[str, list[Mapping[str, str]]] = defaultdict(list)
    for row in rows:
        by_epoch[row["observed_epoch_s"]].append(row)
    if not by_epoch:
        return {
            "resource_samples": 0, "gpu_util_pct_mean": "", "running_total_mean": "",
            "waiting_total_mean": "", "kv_per_endpoint_mean": "", "gpu_power_total_w_mean": "",
        }
    totals = []
    for samples in by_epoch.values():
        totals.append(
            {
                "gpu": _mean(_float(item["gpu_utilization_pct"], "gpu util") for item in samples),
                "running": sum(_float(item["running"], "running") for item in samples),
                "waiting": sum(_float(item["waiting"], "waiting") for item in samples),
                "kv": _mean(_float(item["kv_usage"], "KV") for item in samples),
                "power": sum(_float(item["gpu_power_w"], "power") for item in samples),
            }
        )
    return {
        "resource_samples": len(totals),
        "gpu_util_pct_mean": _mean(item["gpu"] for item in totals),
        "running_total_mean": _mean(item["running"] for item in totals),
        "waiting_total_mean": _mean(item["waiting"] for item in totals),
        "kv_per_endpoint_mean": _mean(item["kv"] for item in totals),
        "gpu_power_total_w_mean": _mean(item["power"] for item in totals),
    }


def _project_phase_rows(root: Path, job_rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    grouped: defaultdict[tuple[str, int, str], list[Mapping[str, object]]] = defaultdict(list)
    for row in job_rows:
        if row["scenario"] in CONCURRENT_PROJECT:
            grouped[(str(row["scenario"]), int(row["repeat"]), str(row["run_stem"]))].append(row)
    output: list[dict[str, object]] = []
    for (scenario, repeat, stem), jobs in sorted(grouped.items()):
        by_job = {str(row["job"]): row for row in jobs}
        if set(by_job) != set(JOBS):
            raise ValueError(f"Project phase input missing jobs: {scenario}/{repeat}")
        short_start = _float(by_job["short"]["start_epoch_s"], "short start")
        long_start = min(_float(by_job[job]["start_epoch_s"], "long start") for job in JOBS[1:])
        short_end = _float(by_job["short"]["end_epoch_s"], "short end")
        group_end = max(_float(by_job[job]["end_epoch_s"], "job end") for job in JOBS)
        phases = (
            ("short_only", short_start, long_start),
            ("four_job_overlap", long_start, min(short_end, group_end)),
            ("long_only_drain", short_end, group_end),
        )
        resource_path = root / "traces" / f"{stem}.resources.csv"
        resources = _read_csv(resource_path)
        credit_path = root / "traces" / f"{stem}.credits.csv"
        credits = _read_csv(credit_path) if credit_path.is_file() else []
        requests_by_job = {
            job: _read_csv(root / "jobs" / f"{stem}_job{index}.requests.csv")
            for index, job in enumerate(JOBS)
        }
        for phase, start, end in phases:
            duration = end - start
            if duration <= 0:
                raise ValueError(f"non-positive Project phase: {scenario}/{repeat}/{phase}")
            phase_resources = [
                row for row in resources
                if start <= _float(row["observed_epoch_s"], "resource epoch") < end
            ]
            metrics = _mean_resource_samples(phase_resources)
            row: dict[str, object] = {
                "system": "project", "scenario": scenario,
                "policy": CONCURRENT_PROJECT[scenario], "repeat": repeat,
                "run_stem": stem, "phase": phase, "start_epoch_s": start,
                "end_epoch_s": end, "duration_s": duration, **metrics,
                "energy_j_estimate": (
                    _float(metrics["gpu_power_total_w_mean"], "phase power") * duration
                    if metrics["gpu_power_total_w_mean"] != "" else ""
                ),
                "mfu_status": "unavailable:no_interval_flops_counter",
            }
            total_work = 0.0
            total_completed = 0
            for job in JOBS:
                completed = [
                    request for request in requests_by_job[job]
                    if request["status"] == "completed"
                    and start <= _float(request["completion_epoch_s"], "completion") < end
                ]
                completed_work = sum(_float(request["total_tokens"], "total tokens") for request in completed)
                row[f"{job}_completed_rows"] = len(completed)
                row[f"{job}_completed_work"] = completed_work
                row[f"{job}_completed_work_per_s"] = completed_work / duration
                total_completed += len(completed)
                total_work += completed_work
            row["total_completed_rows"] = total_completed
            row["total_completed_work"] = total_work
            row["total_completed_work_per_s"] = total_work / duration
            phase_credits = [
                credit for credit in credits
                if start <= _float(credit["observed_epoch_s"], "credit epoch") < end
            ]
            row["credit_trace_status"] = "observed" if phase_credits else "not_applicable:static"
            row["active_work_total_mean"] = (
                _mean(_float(credit["active_work"], "active work") for credit in phase_credits)
                if phase_credits else ""
            )
            row["waiting_work_total_mean"] = (
                _mean(_float(credit["waiting_work"], "waiting work") for credit in phase_credits)
                if phase_credits else ""
            )
            output.append(row)
    return output


def _aggregate(rows: Sequence[Mapping[str, object]], keys: Sequence[str], fields: Sequence[str]) -> list[dict[str, object]]:
    grouped: defaultdict[tuple[str, ...], list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row[key]) for key in keys)].append(row)
    output: list[dict[str, object]] = []
    for key, items in sorted(grouped.items()):
        record: dict[str, object] = dict(zip(keys, key))
        record["formal_repeats"] = len(items)
        for field in fields:
            values = [_float(item[field], field) for item in items if item.get(field, "") != ""]
            if not values:
                record[f"{field}_mean"] = ""
                record[f"{field}_cv"] = ""
                continue
            mean = statistics.mean(values)
            record[f"{field}_mean"] = mean
            record[f"{field}_cv"] = statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0
        output.append(record)
    return output


def _comparisons(job_summary: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    lookup = {
        (str(row["system"]), str(row["scenario"]), str(row["job"])): row
        for row in job_summary
    }
    output: list[dict[str, object]] = []
    systems = ("project", "daft_native", "daft_ray", "ray_data_http")
    for system in systems:
        for job in JOBS:
            base_scenario = f"single_{job}_full_pool" if system == "project" else "single_full"
            base = lookup[(system, base_scenario, job)]
            targets = (
                ("single_quarter", f"single_{job}_quarter_pool"),
                ("static_fourjob", "staggered_fourjob_static_partition"),
                ("shared_fourjob", "staggered_fourjob_shared_work"),
            ) if system == "project" else (("native_fourjob", "fourjob"),)
            for comparison, target_scenario in targets:
                target = lookup[(system, target_scenario, job)]
                base_jct = _float(base["job_jct_s_mean"], "base JCT")
                target_jct = _float(target["job_jct_s_mean"], "target JCT")
                output.append(
                    {
                        "system": system, "job": job, "comparison": comparison,
                        "baseline_scenario": base_scenario, "target_scenario": target_scenario,
                        "baseline_jct_s": base_jct, "target_jct_s": target_jct,
                        "jct_slowdown": target_jct / base_jct,
                        "jct_change_pct": (target_jct / base_jct - 1) * 100,
                        "request_p99_change_pct": (
                            (_float(target["request_p99_s_mean"], "target p99") /
                             _float(base["request_p99_s_mean"], "base p99") - 1) * 100
                            if base.get("request_p99_s_mean", "") != "" else ""
                        ),
                    }
                )
            if system == "project":
                quarter = lookup[(system, f"single_{job}_quarter_pool", job)]
                static = lookup[(system, "staggered_fourjob_static_partition", job)]
                shared = lookup[(system, "staggered_fourjob_shared_work", job)]
                for comparison, baseline, target in (
                    ("matched_competition_static", quarter, static),
                    ("shared_vs_static", static, shared),
                ):
                    base_jct = _float(baseline["job_jct_s_mean"], "base JCT")
                    target_jct = _float(target["job_jct_s_mean"], "target JCT")
                    output.append(
                        {
                            "system": system, "job": job, "comparison": comparison,
                            "baseline_scenario": baseline["scenario"], "target_scenario": target["scenario"],
                            "baseline_jct_s": base_jct, "target_jct_s": target_jct,
                            "jct_slowdown": target_jct / base_jct,
                            "jct_change_pct": (target_jct / base_jct - 1) * 100,
                            "request_p99_change_pct": (
                                _float(target["request_p99_s_mean"], "target p99") /
                                _float(baseline["request_p99_s_mean"], "base p99") * 100 - 100
                            ),
                        }
                    )
    return output


def _long_spread(job_rows: Sequence[Mapping[str, object]], comparisons: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    slowdown = {
        (str(row["system"]), str(row["target_scenario"]), str(row["job"])): _float(row["jct_slowdown"], "slowdown")
        for row in comparisons
        if str(row["comparison"]) in {"static_fourjob", "shared_fourjob", "native_fourjob"}
    }
    grouped: defaultdict[tuple[str, str, int], list[Mapping[str, object]]] = defaultdict(list)
    for row in job_rows:
        if row["job"] != "short" and row["scenario"] in {*CONCURRENT_PROJECT, "fourjob"}:
            grouped[(str(row["system"]), str(row["scenario"]), int(row["repeat"]))].append(row)
    output = []
    for (system, scenario, repeat), rows in sorted(grouped.items()):
        if {str(row["job"]) for row in rows} != {"long1", "long2", "long3"}:
            raise ValueError(f"missing long job in {system}/{scenario}/{repeat}")
        jcts = [_float(row["job_jct_s"], "long JCT") for row in rows]
        slowdowns = [slowdown[(system, scenario, str(row["job"]))] for row in rows]
        output.append(
            {
                "system": system, "scenario": scenario, "repeat": repeat,
                "long_jct_min_s": min(jcts), "long_jct_max_s": max(jcts),
                "long_jct_range_s": max(jcts) - min(jcts),
                "long_jct_cv": statistics.stdev(jcts) / statistics.mean(jcts),
                "long_slowdown_min": min(slowdowns), "long_slowdown_max": max(slowdowns),
                "long_slowdown_range": max(slowdowns) - min(slowdowns),
                "long_slowdown_cv": statistics.stdev(slowdowns) / statistics.mean(slowdowns),
                "slowest_long_job": str(rows[jcts.index(max(jcts))]["job"]),
                "completion_order": json.dumps(
                    [str(row["job"]) for row in sorted(rows, key=lambda item: _float(item["end_epoch_s"], "end"))]
                ),
            }
        )
    return output


def summarize(project_root: Path, native_root: Path, output: Path) -> dict[str, object]:
    project_jobs, project_groups = _project_rows(project_root)
    native_jobs, native_groups = _native_rows(native_root)
    jobs = project_jobs + native_jobs
    groups = project_groups + native_groups
    job_summary = _aggregate(
        jobs, ("system", "scenario", "policy", "job"),
        ("job_jct_s", "request_p95_s", "request_p99_s", "actual_work", "work_per_s", "overlap_with_any_s"),
    )
    group_summary = _aggregate(
        groups, ("system", "scenario", "policy"),
        ("group_jct_s", "group_tokens_per_s", "jain_fairness", "gpu_util_pct_mean", "mfu_fraction", "running_mean", "waiting_mean", "kv_fraction_mean"),
    )
    comparisons = _comparisons(job_summary)
    long_spread = _long_spread(jobs, comparisons)
    project_phases = _project_phase_rows(project_root, project_jobs)
    concurrent = [row for row in jobs if row["scenario"] in {*CONCURRENT_PROJECT, "fourjob"}]
    if any(_float(row["overlap_with_any_s"], "overlap") <= 0 for row in concurrent):
        raise ValueError("one or more concurrent jobs did not overlap")
    manifests: defaultdict[str, set[str]] = defaultdict(set)
    for row in jobs:
        manifests[str(row["job"])].add(str(row["manifest_sha256"]))
    if set(manifests) != set(JOBS) or any(len(values) != 1 for values in manifests.values()):
        raise ValueError(f"manifest identity mismatch: {dict(manifests)}")
    output.mkdir(parents=True, exist_ok=False)
    _write_csv(output / "job_formal_runs.csv", jobs)
    _write_csv(output / "job_summary.csv", job_summary)
    _write_csv(output / "job_slowdown_comparisons.csv", comparisons)
    _write_csv(output / "group_formal_runs.csv", groups)
    _write_csv(output / "group_summary.csv", group_summary)
    _write_csv(output / "long_job_spread.csv", long_spread)
    _write_csv(output / "project_phase_runs.csv", project_phases)
    _write_csv(
        output / "project_phase_summary.csv",
        _aggregate(
            project_phases, ("system", "scenario", "policy", "phase"),
            (
                "duration_s", "gpu_util_pct_mean", "running_total_mean",
                "waiting_total_mean", "kv_per_endpoint_mean", "gpu_power_total_w_mean",
                "energy_j_estimate", "total_completed_work_per_s", "short_completed_work_per_s",
                "long1_completed_work_per_s", "long2_completed_work_per_s",
                "long3_completed_work_per_s", "active_work_total_mean", "waiting_work_total_mean",
            ),
        ),
    )
    audit = {
        "schema_version": 1,
        "status": "passed",
        "project_root": str(project_root.resolve()),
        "native_root": str(native_root.resolve()),
        "formal_job_rows": len(jobs),
        "formal_group_rows": len(groups),
        "project_phase_rows": len(project_phases),
        "manifest_sha256_by_job": {key: next(iter(value)) for key, value in sorted(manifests.items())},
        "project_request_tail": "recomputed from request timestamps",
        "native_request_tail": "unavailable: native adapters expose barrier timestamps",
        "figures_drawn": False,
    }
    (output / "audit.json").write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--native-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.project_root, args.native_root, args.output), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
