#!/usr/bin/env python3
"""Audit and summarize image single/four-job native and project evidence."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise ValueError(f"refuse to write empty summary: {path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0])
    fieldnames.extend(
        sorted({key for row in rows for key in row if key not in fieldnames})
    )
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _native_rows(root: Path) -> tuple[str, list[dict[str, object]], list[dict[str, object]]]:
    index = json.loads((root / "matrix_index.json").read_text(encoding="utf-8"))
    if index.get("status") != "passed":
        raise ValueError("native image matrix is not passed")
    manifest_sha = str(index["job_manifest_sha256"])
    jobs = []
    groups = []
    for group_path in sorted((root / "runs").glob("formal_*_*/group_summary.json")):
        group = json.loads(group_path.read_text(encoding="utf-8"))
        if group.get("status") != "passed":
            raise ValueError(f"native group is not passed: {group_path}")
        total_rows = 0
        flops_per_image = None
        peak_flops = None
        for job_summary in group["jobs"]:
            job_id = job_summary["job_id"]
            row = json.loads(
                (group_path.parent / job_id / "run.json").read_text(encoding="utf-8")
            )["row"]
            if row.get("exactly_once") is not True:
                raise ValueError(f"native image job failed exactly-once: {group_path}/{job_id}")
            total_rows += int(row["rows"])
            flops_per_image = float(row["model_flops_per_image"])
            peak_flops = float(row["gpu_peak_flops_per_s"])
            jobs.append(
                {
                    "system": group["adapter"],
                    "scenario_id": group["arm_id"],
                    "phase": "formal",
                    "repeat": group["repeat"],
                    "job_id": job_id,
                    "job_count": len(group["jobs"]),
                    "job_manifest_sha256": manifest_sha,
                    "rows": row["rows"],
                    "jct_s": row["operator_e2e_s"],
                    "first_output_s": row["first_output_s"],
                    "images_per_s": row["images_per_s"],
                    "input_encoded_bytes": row["input_encoded_bytes"],
                    "device_input_bytes": row["device_input_bytes"],
                    "batch_preprocess_p95_s": row["batch_preprocess_p95_s"],
                    "batch_h2d_p95_s": row["batch_h2d_p95_s"],
                    "batch_forward_p95_s": row["batch_forward_p95_s"],
                    "batch_completion_wall_p50_s": row["batch_completion_wall_p50_s"],
                    "batch_completion_wall_p95_s": row["batch_completion_wall_p95_s"],
                    "batch_unattributed_wait_p50_s": row["batch_unattributed_wait_p50_s"],
                    "batch_unattributed_wait_p95_s": row["batch_unattributed_wait_p95_s"],
                    "batch_source_next_p50_s": row["batch_source_next_p50_s"],
                    "batch_source_next_p95_s": row["batch_source_next_p95_s"],
                    "source_next_total_s": row["source_next_total_s"],
                    "driver_materialize_total_s": row["driver_materialize_total_s"],
                    "formal_start_lateness_s": row["formal_start_lateness_s"],
                    "start_epoch_s": row["formal_start_epoch_s_actual"],
                    "completion_epoch_s": (
                        float(row["formal_start_epoch_s_actual"])
                        + float(row["operator_e2e_s"])
                    ),
                    "timing_granularity": (
                        "framework_query_barrier"
                        if group["adapter"] == "daft_builtin_embed"
                        else "framework_stream_barrier"
                    ),
                }
            )
        group_jct = float(group["group_end_epoch_s"]) - float(group["group_start_epoch_s"])
        gpu = group["gpu_summary"]
        cpu = group["cpu_summary"]
        ray = group["ray_resource_summary"]
        groups.append(
            {
                "system": group["adapter"],
                "scenario_id": group["arm_id"],
                "phase": "formal",
                "repeat": group["repeat"],
                "job_count": len(group["jobs"]),
                "job_manifest_sha256": manifest_sha,
                "rows": total_rows,
                "group_jct_s": group_jct,
                "images_per_s": total_rows / group_jct,
                "estimated_e2e_mfu": total_rows * flops_per_image / (group_jct * 2 * peak_flops),
                "gpu_util_mean_pct": gpu["gpu_active_util_mean_pct"],
                "gpu_energy_estimate_j": gpu["gpu_energy_estimate_j"],
                "cpu_busy_cores_mean": cpu["cpu_busy_cores_mean"],
                "host_net_recv_bytes": cpu["host_net_recv_bytes"],
                "host_disk_read_bytes": cpu["host_disk_read_bytes"],
                "shm_used_bytes_peak": ray["shm_used_bytes_peak"],
                "ray_available_cpu_min": ray["ray_available_cpu_min"],
                "ray_available_gpu_min": ray["ray_available_gpu_min"],
            }
        )
    if len(groups) != 30 or len(jobs) != 48:
        raise ValueError(f"native formal counts invalid: groups={len(groups)} jobs={len(jobs)}")
    _require_fourjob_overlap(jobs, absolute_time=True)
    return manifest_sha, jobs, groups


def _project_rows(root: Path) -> tuple[str, list[dict[str, object]], list[dict[str, object]]]:
    index = json.loads((root / "matrix_index.json").read_text(encoding="utf-8"))
    if index.get("status") != "passed":
        raise ValueError("project image matrix is not passed")
    manifest_sha = str(index["job_manifest_sha256"])
    jobs = [
        {"system": "project_ray", **row}
        for row in _read_csv(root / "job_runs.csv")
        if row["phase"] == "formal"
    ]
    groups = [
        {"system": "project_ray", **row}
        for row in _read_csv(root / "group_runs.csv")
        if row["phase"] == "formal"
    ]
    if len(groups) != 18 or len(jobs) != 36:
        raise ValueError(f"project formal counts invalid: groups={len(groups)} jobs={len(jobs)}")
    if any(row["job_manifest_sha256"] != manifest_sha for row in jobs + groups):
        raise ValueError("project row manifest SHA mismatch")
    _require_fourjob_overlap(jobs, absolute_time=False)
    return manifest_sha, jobs, groups


def _require_fourjob_overlap(
    jobs: list[dict[str, object]], *, absolute_time: bool
) -> None:
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in jobs:
        if int(row["job_count"]) == 4:
            grouped[(str(row["system"]), str(row["scenario_id"]), str(row["repeat"]))].append(row)
    for key, rows in grouped.items():
        if len(rows) != 4:
            raise ValueError(f"four-job evidence is incomplete: {key}")
        by_id = {str(row["job_id"]): row for row in rows}
        short = by_id["short"]
        if absolute_time:
            short_end = float(short["completion_epoch_s"])
            overlaps = [
                short_end - float(by_id[job_id]["start_epoch_s"])
                for job_id in ("long1", "long2", "long3")
            ]
        else:
            short_end = float(short["completion_elapsed_s"])
            overlaps = [
                short_end - float(by_id[job_id]["arrival_offset_s"])
                for job_id in ("long1", "long2", "long3")
            ]
        if min(overlaps) <= 0:
            raise ValueError(f"four-job run lacks measured short/long overlap: {key}")


def _mean_by(rows: list[dict[str, object]], keys: tuple[str, ...], metric: str) -> dict[tuple[str, ...], float]:
    values: dict[tuple[str, ...], list[float]] = defaultdict(list)
    for row in rows:
        values[tuple(str(row[key]) for key in keys)].append(float(row[metric]))
    return {key: statistics.fmean(group) for key, group in values.items()}


def _slowdowns(jobs: list[dict[str, object]]) -> list[dict[str, object]]:
    means = _mean_by(jobs, ("system", "scenario_id", "job_id"), "jct_s")
    output = []
    systems = sorted({str(row["system"]) for row in jobs})
    for system in systems:
        four_scenarios = sorted(
            {
                str(row["scenario_id"])
                for row in jobs
                if row["system"] == system and int(row["job_count"]) == 4
            }
        )
        for four in four_scenarios:
            for job_id in ("short", "long1", "long2", "long3"):
                single_candidates = [
                    key for key in means
                    if key[0] == system and key[2] == job_id and "single" in key[1]
                ]
                if len(single_candidates) != 1:
                    raise ValueError(f"missing unique single control for {system}/{job_id}")
                single = means[single_candidates[0]]
                concurrent = means[(system, four, job_id)]
                output.append(
                    {
                        "system": system,
                        "fourjob_scenario": four,
                        "job_id": job_id,
                        "single_jct_s_mean": single,
                        "fourjob_jct_s_mean": concurrent,
                        "slowdown_pct": (concurrent / single - 1.0) * 100.0,
                    }
                )
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-root", required=True, type=Path)
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    native_sha, native_jobs, native_groups = _native_rows(args.native_root)
    project_sha, project_jobs, project_groups = _project_rows(args.project_root)
    if native_sha != project_sha:
        raise ValueError("native/project immutable image manifest SHA differs")
    jobs = native_jobs + project_jobs
    groups = native_groups + project_groups
    slowdowns = _slowdowns(jobs)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    _write_csv(args.output_dir / "job_formal_runs.csv", jobs)
    _write_csv(args.output_dir / "group_formal_runs.csv", groups)
    _write_csv(args.output_dir / "job_slowdown_comparisons.csv", slowdowns)
    audit = {
        "schema_version": 1,
        "status": "passed",
        "job_manifest_sha256": native_sha,
        "native_formal_job_rows": len(native_jobs),
        "native_formal_group_rows": len(native_groups),
        "project_formal_job_rows": len(project_jobs),
        "project_formal_group_rows": len(project_groups),
        "slowdown_rows": len(slowdowns),
        "claim_boundary": (
            "within-system single-to-four-job slowdown; project static/shared causal "
            "comparison only; no cross-system absolute JCT ranking"
        ),
    }
    (args.output_dir / "audit.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(audit, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
