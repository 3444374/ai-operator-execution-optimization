#!/usr/bin/env python3
"""Audit static request/work credit across multiple workloads."""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path


SCENARIO_PATTERN = re.compile(
    r"^(?P<control>[kw])(?P<limit>[1-9][0-9]*)$"
)
SCENARIO_BODY_PATTERN = re.compile(
    r"^(?:"
    r"k(?P<request_limit>[1-9][0-9]*)"
    r"(?:_w(?P<combined_work_limit>[1-9][0-9]*))?"
    r"|w(?P<work_limit>[1-9][0-9]*)"
    r")$"
)


def _formal_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = [
            row
            for row in csv.DictReader(handle)
            if row.get("phase") == "formal"
        ]
    if not rows:
        raise ValueError(f"{path} contains no formal rows")
    return rows


def _float(row: dict[str, str], field: str) -> float:
    value = row.get(field, "")
    if value == "":
        raise ValueError(f"missing required field {field}")
    return float(value)


def _median(rows: list[dict[str, str]], field: str) -> float:
    return statistics.median(_float(row, field) for row in rows)


def _mean(rows: list[dict[str, str]], field: str) -> float:
    return statistics.mean(_float(row, field) for row in rows)


def _cv(rows: list[dict[str, str]], field: str) -> float:
    values = [_float(row, field) for row in rows]
    mean = statistics.mean(values)
    if len(values) < 2 or mean == 0:
        return 0.0
    return statistics.stdev(values) / mean


def _unique(rows: list[dict[str, str]], field: str) -> str:
    values = {row.get(field, "") for row in rows}
    if "" in values or len(values) != 1:
        raise ValueError(f"{field} must be present and constant")
    return values.pop()


def _parse_surface(value: str) -> tuple[str, Path]:
    workload, separator, raw_path = value.partition("=")
    if not separator or not workload or not raw_path:
        raise argparse.ArgumentTypeError(
            "surface must use WORKLOAD=/path/to/runs.csv"
        )
    return workload, Path(raw_path)


def _scenario_for_workload(
    raw_scenario_id: str,
    workload: str,
    workload_names: set[str],
    *,
    require_prefix: bool,
) -> str | None:
    body = raw_scenario_id
    matched_workload = next(
        (
            candidate
            for candidate in sorted(
                workload_names, key=len, reverse=True
            )
            if raw_scenario_id.startswith(f"{candidate}_")
        ),
        None,
    )
    if matched_workload is not None:
        if matched_workload != workload:
            return None
        body = raw_scenario_id[len(matched_workload) + 1 :]
    elif require_prefix:
        raise ValueError(
            f"shared runs.csv scenario {raw_scenario_id!r} must start "
            f"with one of {sorted(workload_names)}"
        )

    match = SCENARIO_BODY_PATTERN.fullmatch(body)
    if match is None:
        raise ValueError(
            f"{workload} has invalid scenario_id {raw_scenario_id!r}"
        )
    work_limit = (
        match["combined_work_limit"] or match["work_limit"]
    )
    if work_limit is not None:
        return f"w{work_limit}"
    return f"k{match['request_limit']}"


def summarize(
    surfaces: dict[str, Path],
    *,
    minimum_repeats: int = 3,
    capacity_floor_ratio: float = 0.95,
    acceptable_ratio: float = 0.97,
    minimum_cross_regret: float = 0.05,
    maximum_repeat_cv: float = 0.05,
    maximum_equivalent_spread: float = 0.05,
) -> dict[str, object]:
    if len(surfaces) < 2:
        raise ValueError("at least two workload surfaces are required")

    workloads: dict[str, dict[str, dict[str, object]]] = {}
    audit_failures: list[dict[str, object]] = []
    path_counts: dict[Path, int] = defaultdict(int)
    for path in surfaces.values():
        path_counts[path.resolve()] += 1
    workload_names = set(surfaces)
    for workload, path in sorted(surfaces.items()):
        grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in _formal_rows(path):
            scenario_id = _scenario_for_workload(
                row.get("scenario_id", ""),
                workload,
                workload_names,
                require_prefix=path_counts[path.resolve()] > 1,
            )
            if scenario_id is None:
                continue
            if row.get("status") != "ok":
                raise ValueError(f"{workload} contains a non-ok formal row")
            failures = [
                int(value)
                for value in row.get(
                    "actor_worker_failures", "0"
                ).split(";")
                if value
            ]
            if any(failures):
                raise ValueError(
                    f"{workload} contains actor worker failures"
                )
            normalized_row = dict(row)
            normalized_row["scenario_id"] = scenario_id
            grouped[scenario_id].append(normalized_row)

        cells: dict[str, dict[str, object]] = {}
        for scenario_id, rows in sorted(grouped.items()):
            repeats = {int(row["repeat_index"]) for row in rows}
            if len(repeats) < minimum_repeats:
                raise ValueError(
                    f"{workload} {scenario_id} has {len(repeats)} "
                    f"repeats; need {minimum_repeats}"
                )
            match = SCENARIO_PATTERN.fullmatch(scenario_id)
            if match is None:
                raise AssertionError("scenario was validated above")
            control = match["control"]
            limit = int(match["limit"])
            endpoint_count = int(round(_median(rows, "endpoint_count")))
            observed_limit = _median(
                rows,
                (
                    "max_inflight_seen"
                    if control == "k"
                    else "max_active_work_per_endpoint_seen"
                ),
            )
            configured_limit = (
                limit * endpoint_count if control == "k" else limit
            )
            throughput_cv = _cv(
                rows, "model_request_tokens_per_s"
            )
            admission_wait = _median(rows, "bounded_wait_s")
            total_rows = _median(rows, "total_rows")
            no_admission_pressure = (
                admission_wait <= 1e-9
                and (
                    control == "w"
                    and observed_limit < 0.95 * configured_limit
                    or control == "k" and observed_limit >= total_rows
                )
            )
            token_id_coverage = _median(
                rows, "request_actual_output_tokens_observed"
            )
            cells[scenario_id] = {
                "control": control,
                "limit": limit,
                "repeats": len(repeats),
                "server_version": _unique(rows, "server_version"),
                "pgvector_version": _unique(
                    rows, "pgvector_version"
                ),
                "model_request_tokens_per_s_mean": _mean(
                    rows, "model_request_tokens_per_s"
                ),
                "model_request_tokens_per_s_median": _median(
                    rows, "model_request_tokens_per_s"
                ),
                "model_request_tokens_per_s_cv": throughput_cv,
                "request_slo_goodput_per_s_median": _median(
                    rows, "request_slo_goodput_per_s"
                ),
                "request_slo_violation_ratio_mean": _mean(
                    rows, "request_slo_violation_ratio"
                ),
                "e2e_s_median": _median(rows, "e2e_s"),
                "request_e2e_s_p95_median": _median(
                    rows, "request_e2e_s_p95"
                ),
                "request_e2e_s_p99_median": _median(
                    rows, "request_e2e_s_p99"
                ),
                "vllm_running_mean_median": _median(
                    rows, "vllm_running_mean"
                ),
                "vllm_waiting_mean_median": _median(
                    rows, "vllm_waiting_mean"
                ),
                "vllm_kv_cache_usage_mean_median": _median(
                    rows, "vllm_kv_cache_usage_mean"
                ),
                "mfu_estimate_median": _median(
                    rows, "mfu_estimate"
                ),
                "observed_limit_median": observed_limit,
                "configured_limit": configured_limit,
                "bounded_wait_s_median": admission_wait,
                "no_admission_pressure": no_admission_pressure,
                "request_actual_output_tokens_observed_median": (
                    token_id_coverage
                ),
                "_rows": rows,
            }
            if throughput_cv > maximum_repeat_cv:
                audit_failures.append(
                    {
                        "kind": "repeat_instability",
                        "workload": workload,
                        "scenario_id": scenario_id,
                        "observed_cv": throughput_cv,
                        "maximum_cv": maximum_repeat_cv,
                    }
                )

        no_pressure = [
            cell
            for cell in cells.values()
            if bool(cell["no_admission_pressure"])
        ]
        if len(no_pressure) >= 2:
            values = [
                float(cell["model_request_tokens_per_s_median"])
                for cell in no_pressure
            ]
            spread = (max(values) - min(values)) / max(values)
            if spread > maximum_equivalent_spread:
                audit_failures.append(
                    {
                        "kind": "equivalent_arm_instability",
                        "workload": workload,
                        "scenario_ids": sorted(
                            scenario_id
                            for scenario_id, cell in cells.items()
                            if bool(cell["no_admission_pressure"])
                        ),
                        "observed_spread": spread,
                        "maximum_spread": maximum_equivalent_spread,
                    }
                )
        if any(
            float(
                cell[
                    "request_actual_output_tokens_observed_median"
                ]
            )
            <= 0
            for cell in cells.values()
        ):
            audit_failures.append(
                {
                    "kind": "missing_per_request_output_tokens",
                    "workload": workload,
                }
            )
        workloads[workload] = cells

    arm_sets = {
        tuple(
            sorted(
                scenario_id
                for scenario_id, cell in cells.items()
                if cell["control"] == "w"
            )
        )
        for cells in workloads.values()
    }
    if len(arm_sets) != 1:
        raise ValueError(
            "every workload must contain the same active-work arms"
        )

    selected: dict[str, str] = {}
    acceptable: dict[str, list[str]] = {}
    objectives: dict[str, dict[str, float]] = {}
    for workload, cells in sorted(workloads.items()):
        work_cells = {
            scenario_id: cell
            for scenario_id, cell in cells.items()
            if cell["control"] == "w"
        }
        peak = max(
            float(cell["model_request_tokens_per_s_median"])
            for cell in work_cells.values()
        )
        safe = {
            scenario_id: cell
            for scenario_id, cell in work_cells.items()
            if float(cell["model_request_tokens_per_s_median"])
            >= capacity_floor_ratio * peak
        }
        workload_objectives = {
            scenario_id: float(
                cell["request_slo_goodput_per_s_median"]
            )
            for scenario_id, cell in work_cells.items()
        }
        objectives[workload] = workload_objectives
        selected[workload] = min(
            safe,
            key=lambda scenario_id: (
                -workload_objectives[scenario_id],
                int(work_cells[scenario_id]["limit"]),
            ),
        )
        best_objective = max(
            workload_objectives[scenario_id]
            for scenario_id in safe
        )
        acceptable[workload] = sorted(
            (
                scenario_id
                for scenario_id in safe
                if workload_objectives[scenario_id]
                >= acceptable_ratio * best_objective
            ),
            key=lambda scenario_id: int(
                work_cells[scenario_id]["limit"]
            ),
        )

    pairs = []
    workload_names = sorted(workloads)
    for left_index, left in enumerate(workload_names):
        for right in workload_names[left_index + 1 :]:
            left_arm = selected[left]
            right_arm = selected[right]
            left_best = objectives[left][left_arm]
            right_best = objectives[right][right_arm]
            left_regret = (
                left_best - objectives[left][right_arm]
            ) / max(left_best, 1e-12)
            right_regret = (
                right_best - objectives[right][left_arm]
            ) / max(right_best, 1e-12)
            target = left if left_regret >= right_regret else right
            best_arm = selected[target]
            wrong_arm = right_arm if target == left else left_arm
            best_rows = {
                int(row["repeat_index"]): row
                for row in workloads[target][best_arm]["_rows"]
            }
            wrong_rows = {
                int(row["repeat_index"]): row
                for row in workloads[target][wrong_arm]["_rows"]
            }
            common = sorted(set(best_rows) & set(wrong_rows))
            directional_wins = sum(
                _float(
                    best_rows[repeat],
                    "request_slo_goodput_per_s",
                )
                > _float(
                    wrong_rows[repeat],
                    "request_slo_goodput_per_s",
                )
                for repeat in common
            )
            pairs.append(
                {
                    "left": left,
                    "right": right,
                    "left_selected_arm": left_arm,
                    "right_selected_arm": right_arm,
                    "acceptable_sets_disjoint": not (
                        set(acceptable[left]) & set(acceptable[right])
                    ),
                    "left_cross_regret": left_regret,
                    "right_cross_regret": right_regret,
                    "max_cross_regret": max(
                        left_regret, right_regret
                    ),
                    "directional_wins": directional_wins,
                    "paired_repeats": len(common),
                }
            )

    clean = not audit_failures
    adaptation_signal = any(
        pair["left_selected_arm"] != pair["right_selected_arm"]
        and bool(pair["acceptable_sets_disjoint"])
        and float(pair["max_cross_regret"]) >= minimum_cross_regret
        and int(pair["directional_wins"]) * 3
        >= int(pair["paired_repeats"]) * 2
        for pair in pairs
    )
    if not clean:
        status = "inconclusive"
        decision = "rerun_controlled_static_credit_gate"
    elif adaptation_signal:
        status = "passed"
        decision = "continue_adaptive_experiments"
    else:
        status = "not_justified"
        decision = "stop_adaptive_formal_ranking"

    serializable_workloads = {
        workload: {
            scenario_id: {
                key: value
                for key, value in cell.items()
                if key != "_rows"
            }
            for scenario_id, cell in cells.items()
        }
        for workload, cells in workloads.items()
    }
    return {
        "schema_version": 1,
        "status": status,
        "decision": decision,
        "thresholds": {
            "capacity_floor_ratio": capacity_floor_ratio,
            "acceptable_ratio": acceptable_ratio,
            "minimum_cross_regret": minimum_cross_regret,
            "maximum_repeat_cv": maximum_repeat_cv,
            "maximum_equivalent_spread": (
                maximum_equivalent_spread
            ),
        },
        "audit_failures": audit_failures,
        "selected_active_work_arm_by_workload": selected,
        "acceptable_active_work_arms_by_workload": acceptable,
        "pairs": pairs,
        "cells": serializable_workloads,
    }


def write_summary_csv(result: dict[str, object], path: Path) -> None:
    fields = [
        "workload",
        "scenario_id",
        "control",
        "limit",
        "repeats",
        "server_version",
        "pgvector_version",
        "model_request_tokens_per_s_mean",
        "model_request_tokens_per_s_median",
        "model_request_tokens_per_s_cv",
        "request_slo_goodput_per_s_median",
        "request_slo_violation_ratio_mean",
        "e2e_s_median",
        "request_e2e_s_p95_median",
        "request_e2e_s_p99_median",
        "vllm_running_mean_median",
        "vllm_waiting_mean_median",
        "vllm_kv_cache_usage_mean_median",
        "mfu_estimate_median",
        "observed_limit_median",
        "configured_limit",
        "bounded_wait_s_median",
        "no_admission_pressure",
        "request_actual_output_tokens_observed_median",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fields, lineterminator="\n"
        )
        writer.writeheader()
        cells = result["cells"]
        if not isinstance(cells, dict):
            raise TypeError("summary cells must be a mapping")
        for workload, workload_cells in sorted(cells.items()):
            if not isinstance(workload_cells, dict):
                raise TypeError("workload cells must be a mapping")
            for scenario_id, cell in sorted(workload_cells.items()):
                if not isinstance(cell, dict):
                    raise TypeError("cell must be a mapping")
                writer.writerow(
                    {
                        "workload": workload,
                        "scenario_id": scenario_id,
                        **{
                            field: cell[field]
                            for field in fields
                            if field
                            not in {"workload", "scenario_id"}
                        },
                    }
                )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--surface",
        action="append",
        required=True,
        type=_parse_surface,
        metavar="WORKLOAD=RUNS_CSV",
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--summary-csv", type=Path)
    parser.add_argument("--require-pass", action="store_true")
    args = parser.parse_args()
    surfaces = dict(args.surface)
    if len(surfaces) != len(args.surface):
        raise SystemExit("surface workload names must be unique")
    result = summarize(surfaces)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if args.summary_csv is not None:
        write_summary_csv(result, args.summary_csv)
    print(json.dumps(result, sort_keys=True))
    if args.require_pass and result["status"] != "passed":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
