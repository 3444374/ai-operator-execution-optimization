#!/usr/bin/env python3
"""Compare existing offline cost estimators on explicitly selected profile CSVs.

The grouped splits, prediction/ranking/selection metrics and five seeds remain
those of the existing comparison. LightGBM is optional and imported only on fit.
"""

from __future__ import annotations

import argparse
import statistics
import sys
from pathlib import Path

import numpy as np

CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
REPO_ROOT = CODE_ROOT.parent
sys.path.insert(0, str(CODE_ROOT))

from src.experiments.operator_costs.profiles import (  # noqa: E402
    build_profile_matrix,
    load_profile_rows,
    reference_csv_paths,
)
from src.experiments.operator_costs.comparison import ESTIMATORS, fit_predict  # noqa: E402

from src.planning.costs.regression import (  # noqa: E402
    grouped_train_test_split,
    pairwise_accuracy,
    regression_metrics,
    residual_interval_bounds,
    selection_metrics,
    top_k_precision,
)

SEEDS = (20260726, 20260727, 20260728, 20260729, 20260730)
TARGET = "e2e_s"
TEST_FRACTION = 0.25
CONFIDENCE = 0.9


METRIC_KEYS = (
    "mae", "rmse", "q95", "spearman", "pairwise", "topk5",
    "pick_rate", "decision_regret_pct", "interval_coverage", "interval_width",
    "n_contexts",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--data-csv", type=Path, help="profile runs.csv to evaluate")
    source.add_argument("--ref-json", type=Path, help="JSON with repository-relative or absolute source_csvs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = ([args.data_csv] if args.data_csv is not None
             else reference_csv_paths(args.ref_json, REPO_ROOT))
    missing = [path for path in paths if not path.exists()]
    if missing:
        raise SystemExit(f"MISSING input csvs: {[str(path) for path in missing[:3]]}")
    rows, _excluded = load_profile_rows(paths, TARGET)
    features, targets, groups, contexts, candidates = build_profile_matrix(rows, TARGET)
    print(f"rows={len(rows)} seeds={list(SEEDS)} target={TARGET} test_fraction={TEST_FRACTION}")
    acc = {e: {k: [] for k in METRIC_KEYS} for e in ESTIMATORS}
    skipped: dict[str, str] = {}

    for seed in SEEDS:
        split = grouped_train_test_split(groups, test_fraction=TEST_FRACTION, seed=seed)
        tr = np.asarray(split.train_indices, dtype=int)
        te = np.asarray(split.test_indices, dtype=int)
        train_rows = [rows[i] for i in tr]
        test_rows = [rows[i] for i in te]
        train_feat, test_feat = features[tr], features[te]
        train_y, test_y = targets[tr], targets[te]
        t_ctx = [contexts[i] for i in te]
        t_cand = [candidates[i] for i in te]
        for name in ESTIMATORS:
            test_pred, train_pred = fit_predict(
                name, train_rows, test_rows, train_feat, test_feat, train_y
            )
            if test_pred is None:
                skipped[name] = "lightgbm unavailable"
                continue
            reg = regression_metrics(test_y, test_pred)
            sel = selection_metrics(test_y, test_pred, t_ctx, t_cand)
            residuals = train_y - train_pred
            lo, hi = residual_interval_bounds(residuals, confidence=CONFIDENCE)
            if np.isfinite(lo) and np.isfinite(hi):
                errors = test_y - test_pred
                coverage = float(np.mean((errors >= lo) & (errors <= hi)))
                width = float(np.mean(hi - lo))
            else:
                coverage = float("nan")
                width = float("nan")
            m = acc[name]
            m["mae"].append(reg.mae)
            m["rmse"].append(reg.rmse)
            m["q95"].append(reg.q_error_p95)
            m["spearman"].append(reg.spearman_rho)
            m["pairwise"].append(pairwise_accuracy(test_y, test_pred))
            m["topk5"].append(top_k_precision(test_y, test_pred, k=5))
            m["pick_rate"].append(sel.get("pick_rate", 0.0))
            m["decision_regret_pct"].append(sel.get("decision_regret_pct", 0.0))
            m["interval_coverage"].append(coverage)
            m["interval_width"].append(width)
            m["n_contexts"].append(sel.get("decision_contexts_evaluated", 0))

    print(f"\ncontexts_evaluated avg={statistics.mean(acc['CE3_ridge']['n_contexts']):.1f} "
          f"(selection metrics noisy at this scale)")
    if skipped:
        print(f"skipped: {skipped}")
    print("\n=== unified cost-estimator baseline (5-seed mean) ===")
    print("(prediction: lower MAE/RMSE/Q95 better, higher Spearman better; "
          "ranking: higher pairwise/topk/pick better; decision: lower regret better)")
    hdr = f"{'estimator':16s} {'MAE':>7s} {'RMSE':>7s} {'Q95':>6s} {'Sρ':>6s} {'pairwise':>9s} {'topK5':>6s} {'pick':>6s} {'regret%':>8s} {'intvCov':>8s} {'intvW':>7s}"
    print(hdr)
    print("-" * len(hdr))
    for name in ESTIMATORS:
        if not acc[name]["mae"]:
            print(f"{name:16s}  (skipped: {skipped.get(name,'?')})")
            continue
        a = acc[name]

        def mm(key, fmt="{:.2f}"):
            vals = [v for v in a[key] if isinstance(v, (int, float)) and np.isfinite(v)]
            return fmt.format(statistics.mean(vals)) if vals else "  nan"

        print(
            f"{name:16s} {mm('mae','{:7.2f}')} {mm('rmse','{:7.2f}')} {mm('q95','{:6.2f}')} "
            f"{mm('spearman','{:6.3f}')} {mm('pairwise','{:9.3f}')} {mm('topk5','{:6.2f}')} "
            f"{mm('pick_rate','{:6.2f}')} {mm('decision_regret_pct','{:8.2f}')} "
            f"{mm('interval_coverage','{:8.2f}')} {mm('interval_width','{:7.2f}')}"
        )
    print("\nCE6_oracle: MAE=0, Q=1, Spearman=1, pairwise=1, pick=1, regret=0 (reference upper bound; not fit).")
    print("\nRead: Heinrich 'accuracy != selection' — a lower-MAE estimator can still pick worse plans")
    print("(higher regret). Watch pairwise/topK vs MAE divergence, and interval coverage vs width.")


if __name__ == "__main__":
    main()
