#!/usr/bin/env python3
"""Evaluate whether workload-dependent static K optima justify adaptation."""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path


SCENARIO_PATTERN = re.compile(r"^(?P<workload>.+)_k(?P<k>[1-9][0-9]*)$")


def _formal_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = [
            row
            for row in csv.DictReader(handle)
            if row.get("phase") == "formal"
        ]
    if not rows:
        raise ValueError("static-K surface contains no formal rows")
    return rows


def _median(rows: list[dict[str, str]], field: str) -> float:
    return statistics.median(float(row[field]) for row in rows)


def summarize(
    path: Path,
    *,
    minimum_repeats: int = 3,
    capacity_floor_ratio: float = 0.95,
    acceptable_ratio: float = 0.97,
    minimum_k_ratio: float = 2.0,
    minimum_cross_regret: float = 0.05,
) -> dict[str, object]:
    grouped: dict[tuple[str, int], list[dict[str, str]]] = defaultdict(list)
    for row in _formal_rows(path):
        match = SCENARIO_PATTERN.fullmatch(row["scenario_id"])
        if match is None:
            raise ValueError(
                f"invalid static-K scenario_id: {row['scenario_id']}"
            )
        if row.get("status") != "ok":
            raise ValueError("static-K surface contains a non-ok row")
        failures = [
            int(value)
            for value in row.get("actor_worker_failures", "0").split(";")
            if value
        ]
        if any(failures):
            raise ValueError("static-K surface contains actor worker failures")
        grouped[(match["workload"], int(match["k"]))].append(row)

    workloads: dict[str, dict[int, dict[str, object]]] = defaultdict(dict)
    for (workload, k), rows in grouped.items():
        repeats = {int(row["repeat_index"]) for row in rows}
        if len(repeats) < minimum_repeats:
            raise ValueError(
                f"{workload} K={k} has {len(repeats)} repeats; "
                f"need {minimum_repeats}"
            )
        workloads[workload][k] = {
            "rows": rows,
            "throughput": _median(rows, "model_request_tokens_per_s"),
            "slo_goodput": _median(rows, "request_slo_goodput_per_s"),
            "jct_s": _median(rows, "e2e_s"),
        }
    k_sets = {tuple(sorted(cells)) for cells in workloads.values()}
    if len(k_sets) != 1:
        raise ValueError("every workload must contain the same static K arms")

    selected: dict[str, int] = {}
    acceptable: dict[str, list[int]] = {}
    objective_by_workload: dict[str, dict[int, float]] = {}
    use_slo_by_workload: dict[str, bool] = {}
    summary_cells = {}
    for workload, cells in sorted(workloads.items()):
        peak_throughput = max(
            float(cell["throughput"]) for cell in cells.values()
        )
        safe = [
            k
            for k, cell in cells.items()
            if float(cell["throughput"])
            >= capacity_floor_ratio * peak_throughput
        ]
        use_slo_goodput = any(
            float(cells[k]["slo_goodput"]) > 0 for k in safe
        )
        use_slo_by_workload[workload] = use_slo_goodput
        objectives = {
            k: (
                float(cell["slo_goodput"])
                if use_slo_goodput
                else 1.0 / max(float(cell["jct_s"]), 1e-12)
            )
            for k, cell in cells.items()
        }
        objective_by_workload[workload] = objectives
        selected[workload] = min(
            safe,
            key=lambda k: (-objectives[k], k),
        )
        best_objective = max(objectives[k] for k in safe)
        acceptable[workload] = sorted(
            k
            for k in safe
            if objectives[k] >= acceptable_ratio * best_objective
        )
        summary_cells[workload] = {
            str(k): {
                "model_request_tokens_per_s_median": cell["throughput"],
                "request_slo_goodput_per_s_median": cell["slo_goodput"],
                "e2e_s_median": cell["jct_s"],
            }
            for k, cell in sorted(cells.items())
        }

    workload_names = sorted(workloads)
    pair_evidence = []
    for left_index, left in enumerate(workload_names):
        for right in workload_names[left_index + 1 :]:
            left_k = selected[left]
            right_k = selected[right]
            k_ratio = max(left_k, right_k) / min(left_k, right_k)
            intervals_disjoint = not (
                set(acceptable[left]) & set(acceptable[right])
            )
            right_best = objective_by_workload[right][right_k]
            left_best = objective_by_workload[left][left_k]
            right_regret = (
                right_best - objective_by_workload[right][left_k]
            ) / right_best
            left_regret = (
                left_best - objective_by_workload[left][right_k]
            ) / left_best
            cross_regret = max(left_regret, right_regret)
            if right_regret >= left_regret:
                target_workload, best_k, wrong_k = (
                    right,
                    right_k,
                    left_k,
                )
            else:
                target_workload, best_k, wrong_k = (
                    left,
                    left_k,
                    right_k,
                )
            best_rows = {
                int(row["repeat_index"]): row
                for row in workloads[target_workload][best_k]["rows"]
            }
            wrong_rows = {
                int(row["repeat_index"]): row
                for row in workloads[target_workload][wrong_k]["rows"]
            }
            common_repeats = sorted(set(best_rows) & set(wrong_rows))
            directional_wins = sum(
                (
                    float(best_rows[repeat]["request_slo_goodput_per_s"])
                    > float(wrong_rows[repeat]["request_slo_goodput_per_s"])
                    if use_slo_by_workload[target_workload]
                    else float(best_rows[repeat]["e2e_s"])
                    < float(wrong_rows[repeat]["e2e_s"])
                )
                for repeat in common_repeats
            )
            direction_passes = (
                directional_wins * 3 >= len(common_repeats) * 2
            )
            pair_evidence.append(
                {
                    "left": left,
                    "right": right,
                    "left_selected_k": left_k,
                    "right_selected_k": right_k,
                    "k_ratio": k_ratio,
                    "acceptable_sets_disjoint": intervals_disjoint,
                    "max_cross_workload_regret": cross_regret,
                    "cross_regret_target_workload": target_workload,
                    "directional_wins": directional_wins,
                    "paired_repeats": len(common_repeats),
                    "passes": (
                        (
                            k_ratio >= minimum_k_ratio
                            or intervals_disjoint
                        )
                        and cross_regret >= minimum_cross_regret
                        and direction_passes
                    ),
                }
            )
    passed = any(bool(item["passes"]) for item in pair_evidence)
    return {
        "schema_version": 1,
        "status": "passed" if passed else "not_justified",
        "decision": (
            "continue_adaptive_experiments"
            if passed
            else "stop_adaptive_formal_ranking"
        ),
        "thresholds": {
            "capacity_floor_ratio": capacity_floor_ratio,
            "acceptable_ratio": acceptable_ratio,
            "minimum_k_ratio": minimum_k_ratio,
            "minimum_cross_regret": minimum_cross_regret,
        },
        "selected_k_by_workload": selected,
        "acceptable_k_by_workload": acceptable,
        "pairs": pair_evidence,
        "cells": summary_cells,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--require-pass", action="store_true")
    args = parser.parse_args()
    result = summarize(args.runs)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True))
    if args.require_pass and result["status"] != "passed":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
