#!/usr/bin/env python3
"""Fit and evaluate an offline AI-operator cost estimator from profile CSVs."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

import numpy as np

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.operator_costs.profiles import (  # noqa: E402
    FEATURE_NAMES,
    build_profile_matrix,
    load_profile_rows,
)

from src.planning.costs.regression import (  # noqa: E402
    RidgeCostEstimator,
    grouped_train_test_split,
    regression_metrics,
    selection_metrics,
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


def estimate(
    paths: list[Path],
    *,
    target: str,
    test_fraction: float,
    seed: int,
    alpha: float,
) -> dict[str, object]:
    rows, excluded = load_profile_rows(paths, target)
    if not rows:
        raise ValueError("no complete profile rows were loaded")
    features, targets, groups, decision_contexts, candidate_ids = build_profile_matrix(rows, target)
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
        "schema_version": 2,
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
        "mean_baseline_selection_metrics": selection_metrics(
            targets[test],
            baseline,
            [decision_contexts[index] for index in test],
            [candidate_ids[index] for index in test],
        ),
        "ridge_selection_metrics": selection_metrics(
            targets[test],
            predicted,
            [decision_contexts[index] for index in test],
            [candidate_ids[index] for index in test],
        ),
        "target_train_mean": float(targets[train].mean()),
        "target_test_mean": float(targets[test].mean()),
    }


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
