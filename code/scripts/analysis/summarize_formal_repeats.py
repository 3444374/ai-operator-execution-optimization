#!/usr/bin/env python3
"""Add CI/CV and paired regression counts to formal experiment CSVs."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.observability.metrics import (  # noqa: E402
    paired_performance_regression_count,
    repeat_summary,
)


DEFAULT_METRICS = (
    "e2e_s",
    "operator_wall_s",
    "tokens_per_s",
    "request_e2e_s_p99",
    "request_slo_total_tokens_goodput_per_s",
    "observed_p99_slo_scale",
    "scheduling_control_overhead_pct",
)
HIGHER_IS_BETTER = {
    "tokens_per_s",
    "rows_per_s",
    "request_slo_goodput_per_s",
    "request_slo_input_tokens_goodput_per_s",
    "request_slo_output_tokens_goodput_per_s",
    "request_slo_total_tokens_goodput_per_s",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--group-field", action="append", default=["scenario_id"])
    parser.add_argument("--metric", action="append", default=[])
    parser.add_argument("--baseline-scenario-id", default="")
    parser.add_argument("--regression-tolerance-pct", type=float, default=0.0)
    return parser.parse_args()


def summarize(
    rows: list[dict[str, str]],
    *,
    group_fields: tuple[str, ...],
    metrics: tuple[str, ...],
    baseline_scenario_id: str,
    regression_tolerance_pct: float,
) -> dict[str, object]:
    formal = [row for row in rows if row.get("phase") == "formal"]
    grouped: dict[tuple[str, ...], list[dict[str, str]]] = {}
    for row in formal:
        grouped.setdefault(tuple(row.get(field, "") for field in group_fields), []).append(row)
    summaries = []
    for group_key, group_rows in sorted(grouped.items()):
        metric_summaries = {}
        for metric in metrics:
            values = [
                float(row[metric])
                for row in group_rows
                if row.get(metric, "") not in ("", None)
            ]
            metric_summaries[metric] = repeat_summary(values)
        summaries.append(
            {
                "group": dict(zip(group_fields, group_key)),
                "metrics": metric_summaries,
            }
        )
    regressions = []
    if baseline_scenario_id:
        baseline = {
            int(row["repeat_index"]): row
            for row in formal
            if row.get("scenario_id") == baseline_scenario_id
        }
        for group_key, group_rows in sorted(grouped.items()):
            if dict(zip(group_fields, group_key)).get("scenario_id") == baseline_scenario_id:
                continue
            candidate = {int(row["repeat_index"]): row for row in group_rows}
            repeats = sorted(set(baseline) & set(candidate))
            for metric in metrics:
                if not repeats or any(
                    baseline[index].get(metric, "") in ("", None)
                    or candidate[index].get(metric, "") in ("", None)
                    for index in repeats
                ):
                    continue
                regressions.append(
                    {
                        "group": dict(zip(group_fields, group_key)),
                        "metric": metric,
                        "paired_repeats": len(repeats),
                        "regression_count": paired_performance_regression_count(
                            [float(baseline[index][metric]) for index in repeats],
                            [float(candidate[index][metric]) for index in repeats],
                            higher_is_better=metric in HIGHER_IS_BETTER,
                            tolerance_ratio=regression_tolerance_pct / 100.0,
                        ),
                    }
                )
    return {
        "schema_version": 1,
        "formal_rows": len(formal),
        "group_fields": list(group_fields),
        "metrics": list(metrics),
        "repeat_summaries": summaries,
        "baseline_scenario_id": baseline_scenario_id,
        "paired_regressions": regressions,
    }


def main() -> None:
    args = parse_args()
    if args.output.exists():
        raise FileExistsError(f"output already exists: {args.output}")
    if args.regression_tolerance_pct < 0:
        raise ValueError("--regression-tolerance-pct must be non-negative")
    with args.input_csv.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    result = summarize(
        rows,
        group_fields=tuple(dict.fromkeys(args.group_field)),
        metrics=tuple(args.metric or DEFAULT_METRICS),
        baseline_scenario_id=args.baseline_scenario_id,
        regression_tolerance_pct=args.regression_tolerance_pct,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
