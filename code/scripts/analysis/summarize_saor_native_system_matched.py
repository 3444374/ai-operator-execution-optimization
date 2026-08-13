#!/usr/bin/env python3
"""Validate stored SAOR matched-system evidence and emit two separate summaries."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.saor.native_system_matched import (
    REQUIRED_ARM_IDS,
    SELECTOR_SANITY_ARM_IDS,
    SYSTEM_ARM_IDS,
)


FORMAL_REPEATS = 3
_PROJECT_FLAG_FRAGMENTS = (
    "credit", "coordinator", "router", "bounded-ready", "bounded_ready",
    "max-active-work", "max_active_work", "ready-observation", "ready_observation",
)


def _validation(status: str) -> dict[str, object]:
    return {
        "status": status,
        "comparison_scope": "complete_system_empirical_plus_project_internal_sanity",
        "selector_victory_decided": False,
        "formal_authorized": False,
        "native_baseline_count": 3,
        "project_control_count": 5,
    }


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


def _queue_empty(value: object, run_id: str) -> None:
    if value is None:
        return
    if isinstance(value, str):
        value = json.loads(value)

    live_names = {
        "running", "waiting", "active_requests", "active_work",
        "waiting_requests", "waiting_work",
    }

    def final_values(item: object, *, nested: bool = False) -> list[float]:
        if isinstance(item, dict):
            direct = [
                _finite(child, f"{run_id} final queue")
                for name, child in item.items()
                if name in live_names
            ]
            descendants = [
                descendant
                for name, child in item.items() if name not in live_names
                for descendant in final_values(child, nested=True)
            ]
            return direct + descendants
        if isinstance(item, list):
            return [
                value for child in item
                for value in final_values(child, nested=True)
            ]
        if nested and isinstance(item, (int, float)) and not isinstance(item, bool):
            return [float(item)]
        return []

    values = final_values(value)
    if not values or any(item != 0 for item in values):
        raise ValueError(f"{run_id} final queue is not empty")


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
    final_queue = cell.get("queue_final", cell.get("final_queue"))
    final_credit = cell.get("shared_credit_final")
    if final_queue is not None:
        _queue_empty(final_queue, run_id)
    elif final_credit is not None:
        _queue_empty(final_credit, run_id)
    else:
        raise ValueError(f"{run_id} lacks final queue/credit evidence")

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
    starts: list[float] = []
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
        starts.append(_finite(job.get("actual_launch_epoch_s"), f"{run_id} Job release"))
        ends.append(_finite(job.get("ended_epoch_s"), f"{run_id} Job completion"))
    overlap = min(ends) - max(starts)
    if overlap <= 0:
        raise ValueError(f"{run_id} has non-positive Job overlap")

    request_tail = cell.get("request_tail_status")
    if not isinstance(request_tail, dict):
        raise ValueError(f"{run_id} lacks request-tail availability")
    status = str(request_tail.get("status", ""))
    reason = str(request_tail.get("reason", ""))
    if arm_id in SYSTEM_ARM_IDS[:3] and (status != "unavailable" or not reason):
        raise ValueError(f"{run_id} native request tail must be unavailable with reason")

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
        "request_p99_status": status,
        "request_p99_reason": reason,
        "request_p99_s": (
            "unavailable"
            if status == "unavailable"
            else request_tail.get("p99_s", "")
        ),
        "exactly_once": True,
    }
    job_rows = [
        {
            "run_id": run_id,
            "arm_id": arm_id,
            "repeat": repeat,
            "job_role": role,
            "release_epoch_s": release,
            "completion_epoch_s": completion,
            "job_jct_s": completion - release,
            "overlap_s": overlap,
            "completion_order": 1 + sorted(ends).index(completion),
        }
        for role, release, completion in zip(("bulk", "foreground"), starts, ends)
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
    tail_statuses = {str(row["request_p99_status"]) for row in ordered}
    tail_reasons = {str(row["request_p99_reason"]) for row in ordered}
    if len(tail_statuses) != 1 or len(tail_reasons) != 1:
        raise ValueError(f"{arm_id} request-tail availability drifted")
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
        "request_p99_status": next(iter(tail_statuses)),
        "request_p99_reason": next(iter(tail_reasons)),
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


def summarize_matched_system(matrix_root: Path, output_dir: Path) -> bool:
    """Summarize only stored matrix evidence; return false on every contract failure."""

    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        index = json.loads(
            (matrix_root / "matrix_index.json").read_text(encoding="utf-8")
        )
        if not isinstance(index, dict) or index.get("status") != "completed":
            raise ValueError("matrix index is not completed")
        if not str(index.get("repository_commit", "")):
            raise ValueError("matrix index lacks repository commit")
        raw_cells = index.get("cells")
        if not isinstance(raw_cells, list):
            raise ValueError("matrix cells must be a list")
        formal_cells = [
            cell for cell in raw_cells
            if isinstance(cell, dict) and cell.get("phase") == "formal"
        ]

        def physical_id(cell: dict[str, Any]) -> str:
            return str(
                cell.get("run_id") or cell.get("run_instance_id")
                or (
                    f"{cell.get('phase')}-{cell.get('repeat')}-"
                    f"{cell.get('order_index')}-{cell.get('arm_id')}"
                )
            )

        run_ids = [
            physical_id(cell) for cell in raw_cells if isinstance(cell, dict)
        ]
        if len(run_ids) != len(set(run_ids)) or any(not item for item in run_ids):
            raise ValueError("formal run IDs must be non-empty and unique")
        observed = {
            arm_id: sorted(
                int(cell.get("repeat", 0)) for cell in formal_cells
                if cell.get("arm_id") == arm_id
            )
            for arm_id in REQUIRED_ARM_IDS
        }
        if any(repeats != [1, 2, 3] for repeats in observed.values()):
            raise ValueError("every required arm must have formal repeats [1, 2, 3]")
        if len(formal_cells) != len(REQUIRED_ARM_IDS) * FORMAL_REPEATS:
            raise ValueError("formal matrix must contain exactly eight arms by three repeats")

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
            run, jobs, resource = _normalize_cell(cell)
            run_rows.append(run)
            job_rows.extend(jobs)
            resource_rows.append(resource)
        run_rows.sort(key=lambda row: (int(row["repeat"]), int(row["order_index"])))
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
        selector = [
            _summary_row(
                arm_id, rows_for(arm_id), job_rows, "project_internal_sanity"
            )
            for arm_id in SELECTOR_SANITY_ARM_IDS
        ]
        system_saor = next(row for row in system if row["arm_id"].endswith("0125we"))
        selector_saor = next(row for row in selector if row["arm_id"].endswith("0125we"))
        if (
            system_saor["physical_run_ids"] != selector_saor["physical_run_ids"]
            or system_saor["service_tokens_per_s_repeats"]
            != selector_saor["service_tokens_per_s_repeats"]
        ):
            raise ValueError("SAOR reports do not originate from the same physical runs")

        _write_csv(output_dir / "all_runs.csv", run_rows)
        _write_csv(output_dir / "system_summary.csv", system)
        _write_csv(output_dir / "project_selector_sanity.csv", selector)
        _write_csv(output_dir / "job_summary.csv", job_rows)
        _write_csv(
            output_dir / "resource_summary.csv",
            [
                _resource_summary_row(arm_id, rows_for(arm_id), resource_rows)
                for arm_id in REQUIRED_ARM_IDS
            ],
        )
        _write_json(output_dir / "validation.json", _validation("passed"))
        return True
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        _write_json(output_dir / "validation.json", _validation("failed"))
        return False


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = _args()
    return 0 if summarize_matched_system(args.matrix_root, args.output_dir) else 2


if __name__ == "__main__":
    raise SystemExit(main())
