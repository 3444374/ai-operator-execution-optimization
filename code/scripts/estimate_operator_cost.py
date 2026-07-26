#!/usr/bin/env python3
"""Fit and evaluate an offline AI-operator cost estimator from profile CSVs."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from dataclasses import asdict
from pathlib import Path

import numpy as np

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.cost_estimation import (  # noqa: E402
    FEATURE_NAMES,
    RidgeCostEstimator,
    grouped_train_test_split,
    regression_metrics,
)


GROUP_FIELDS = (
    "model_name",
    "cost_model_id",
    "source_workload_name",
    "batching_policy",
    "output_cost_mode",
    "total_rows",
    "completion_max_tokens",
    "token_budget",
    "max_inflight_limit",
    "flush_policy",
    "flush_timeout_ms",
    "flush_max_wait_ms",
    "arrival_replay",
    "arrival_time_scale",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", action="append", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--target",
        choices=["e2e_s", "model_service_s"],
        default="e2e_s",
    )
    parser.add_argument("--test-fraction", type=float, default=0.25)
    parser.add_argument("--seed", type=int, default=20260726)
    parser.add_argument("--alpha", type=float, default=1.0)
    return parser.parse_args()


def feature_vector(row: dict[str, str]) -> list[float]:
    flush_policy = row.get("flush_policy", "")
    return [
        _number(row, "total_rows"),
        _number(row, "token_count"),
        _number(row, "completion_max_tokens"),
        _number(row, "token_budget"),
        _number(row, "packing_batch_count"),
        _number(row, "batch_estimated_cost_units_p50"),
        _number(row, "batch_estimated_cost_units_p95"),
        _number(row, "batch_estimated_cost_units_max"),
        _number(row, "max_inflight_limit"),
        _number(row, "flush_timeout_ms"),
        _number(row, "flush_max_wait_ms"),
        _number(row, "arrival_time_scale"),
        float(_boolean(row.get("arrival_replay", ""))),
        float(flush_policy == "queue_adaptive"),
        float(flush_policy == "immediate"),
    ]


def scenario_group(row: dict[str, str]) -> str:
    signature = json.dumps(
        {field: row.get(field, "") for field in GROUP_FIELDS},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16]


def load_dataset(
    paths: list[Path],
    target: str,
) -> tuple[np.ndarray, np.ndarray, list[str], int]:
    features = []
    targets = []
    groups = []
    excluded = 0
    for path in paths:
        with path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                if row.get("status") != "ok":
                    excluded += 1
                    continue
                try:
                    vector = feature_vector(row)
                    target_value = _number(row, target)
                except (KeyError, TypeError, ValueError):
                    excluded += 1
                    continue
                if target_value < 0:
                    excluded += 1
                    continue
                features.append(vector)
                targets.append(target_value)
                groups.append(scenario_group(row))
    if not features:
        raise ValueError("no complete profile rows were loaded")
    return (
        np.asarray(features, dtype=float),
        np.asarray(targets, dtype=float),
        groups,
        excluded,
    )


def estimate(
    paths: list[Path],
    *,
    target: str,
    test_fraction: float,
    seed: int,
    alpha: float,
) -> dict[str, object]:
    features, targets, groups, excluded = load_dataset(paths, target)
    split = grouped_train_test_split(
        groups,
        test_fraction=test_fraction,
        seed=seed,
    )
    train = np.asarray(split.train_indices, dtype=int)
    test = np.asarray(split.test_indices, dtype=int)
    estimator = RidgeCostEstimator(alpha=alpha).fit(
        features[train],
        targets[train],
    )
    predicted = estimator.predict(features[test])
    baseline = np.full(len(test), float(targets[train].mean()))
    return {
        "schema_version": 1,
        "target": target,
        "feature_names": list(FEATURE_NAMES),
        "post_execution_features_used": [],
        "source_csvs": [_portable_path(path) for path in paths],
        "rows_loaded": int(len(targets)),
        "rows_excluded": excluded,
        "group_count": len(set(groups)),
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "train_group_count": len(split.train_groups),
        "test_group_count": len(split.test_groups),
        "test_groups": list(split.test_groups),
        "seed": seed,
        "test_fraction": test_fraction,
        "model": {
            "type": "standardized_log1p_ridge",
            "alpha": alpha,
            "intercept": float(estimator.coefficients_[0]),
            "coefficients": {
                name: float(value)
                for name, value in zip(
                    FEATURE_NAMES,
                    estimator.coefficients_[1:],
                )
            },
            "feature_mean": {
                name: float(value)
                for name, value in zip(FEATURE_NAMES, estimator.feature_mean_)
            },
            "feature_scale": {
                name: float(value)
                for name, value in zip(FEATURE_NAMES, estimator.feature_scale_)
            },
        },
        "mean_baseline_metrics": asdict(
            regression_metrics(targets[test], baseline)
        ),
        "ridge_metrics": asdict(
            regression_metrics(targets[test], predicted)
        ),
        "target_train_mean": float(targets[train].mean()),
        "target_test_mean": float(targets[test].mean()),
    }


def _number(row: dict[str, str], field: str) -> float:
    value = row.get(field, "")
    if value in ("", None):
        raise ValueError(f"missing numeric field {field}")
    parsed = float(value)
    if not np.isfinite(parsed):
        raise ValueError(f"non-finite numeric field {field}")
    return parsed


def _boolean(value: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no", ""}:
        return False
    raise ValueError(f"invalid boolean value: {value}")


def _portable_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(Path.cwd().resolve()))
    except ValueError:
        return str(resolved)


def main() -> None:
    args = parse_args()
    result = estimate(
        args.input_csv,
        target=args.target,
        test_fraction=args.test_fraction,
        seed=args.seed,
        alpha=args.alpha,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
