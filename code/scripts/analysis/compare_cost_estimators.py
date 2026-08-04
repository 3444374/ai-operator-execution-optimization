#!/usr/bin/env python3
"""Compare mean vs ridge operator-cost estimators across 5 seeds (prediction + selection).

Reuses ``estimate_operator_cost.estimate()`` unchanged. Closes the
``operator_cost_estimation_20260726`` README "排序能力分析待补" gap by reporting
Spearman rho / pick_rate / decision_regret (schema-v2 selection metrics) for the
mean baseline vs the ridge cost model, so we can test whether the ridge model wins
on *selection* (the optimizer-relevant layer per Heinrich SIGMOD 2025), not only on
point-prediction MAE.

This script ADDS no estimator and modifies no existing code; it only re-runs the
existing mean-vs-ridge comparison with the current (selection-metric-emitting) driver.
"""

from __future__ import annotations

import importlib.util
import json
import statistics
import sys
from pathlib import Path


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
REPO_ROOT = CODE_ROOT.parent
DRIVER = CODE_ROOT / "scripts" / "analysis" / "estimate_operator_cost.py"
REF_JSON = REPO_ROOT / "experiments" / "results" / "operator_cost_estimation_20260726" / "e2e_cost_model.json"
SEEDS = (20260726, 20260727, 20260728, 20260729, 20260730)
TARGET = "e2e_s"
TEST_FRACTION = 0.25
ALPHA = 1.0


def _load_estimate():
    spec = importlib.util.spec_from_file_location("estimate_operator_cost", DRIVER)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module.estimate


def _resolve_inputs() -> list[Path]:
    ref = json.loads(REF_JSON.read_text(encoding="utf-8"))
    paths = []
    for raw in ref["source_csvs"]:
        p = Path(raw.replace("\\", "/"))
        if not p.is_absolute():
            p = REPO_ROOT / p
        paths.append(p)
    return paths


def main() -> None:
    estimate = _load_estimate()
    paths = _resolve_inputs()
    missing = [p for p in paths if not p.exists()]
    if missing:
        print("MISSING input csvs:", [str(p) for p in missing[:5]])
        raise SystemExit(1)
    print(f"input_csvs={len(paths)} target={TARGET} test_fraction={TEST_FRACTION} seeds={list(SEEDS)}")
    print(f"repo_commit_reference=see e2e_cost_model.json; re-run with current driver (schema_version 2)\n")

    results = []
    for seed in SEEDS:
        r = estimate(paths, target=TARGET, test_fraction=TEST_FRACTION, seed=seed, alpha=ALPHA)
        results.append(r)
        mb = r["mean_baseline_metrics"]
        rb = r["ridge_metrics"]
        ms = r["mean_baseline_selection_metrics"]
        rs = r["ridge_selection_metrics"]
        print(
            f"seed={seed} test_rows={r['test_rows']:3d} ctx={rs.get('decision_contexts_evaluated','?'):>3} | "
            f"mean:  MAE={mb['mae']:6.2f}s Sρ={mb['spearman_rho']:.3f} pick={ms.get('pick_rate',0):.3f} regret={ms.get('decision_regret_pct',0):6.2f}% | "
            f"ridge: MAE={rb['mae']:6.2f}s Sρ={rb['spearman_rho']:.3f} pick={rs.get('pick_rate',0):.3f} regret={rs.get('decision_regret_pct',0):6.2f}%"
        )

    def avg(arm: str, layer: str, key: str) -> float:
        vals = [r[f"{arm}_{layer}_metrics"].get(key) for r in results]
        vals = [v for v in vals if isinstance(v, (int, float))]
        return statistics.mean(vals) if vals else float("nan")

    print("\n=== 5-seed averages (lower MAE/regret = better; higher Sρ/pick = better) ===")
    print(f"  prediction: ridge MAE={avg('ridge','regression','mae'):.2f}s  vs  mean MAE={avg('mean_baseline','regression','mae'):.2f}s")
    print(f"  prediction: ridge Spearman={avg('ridge','regression','spearman_rho'):.3f}  vs  mean={avg('mean_baseline','regression','spearman_rho'):.3f}")
    print(f"  selection:  ridge pick_rate={avg('ridge','selection','pick_rate'):.3f}  vs  mean={avg('mean_baseline','selection','pick_rate'):.3f}")
    print(f"  selection:  ridge regret%={avg('ridge','selection','decision_regret_pct'):.2f}  vs  mean={avg('mean_baseline','selection','decision_regret_pct'):.2f}")
    print(f"  selection:  ridge surpassed={avg('ridge','selection','surpassed_plans'):.2f}  vs  mean={avg('mean_baseline','selection','surpassed_plans'):.2f}")
    sel_status = results[0]["ridge_selection_metrics"].get("selection_status")
    print(f"\n  (selection_status={sel_status}; pairwise-accuracy & Top-K precision not yet implemented — needs selection_metrics extension)")


if __name__ == "__main__":
    main()
