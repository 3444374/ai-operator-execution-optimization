#!/usr/bin/env python3
"""Evaluate cost estimators on unseen decision contexts with reproducible evidence.

Each fold holds out one complete multi-candidate decision context, including all
repeats of every candidate.  The script reports row-level prediction error,
candidate-aggregated within-context ranking, and plan-selection outcomes.  It
writes per-fold predictions plus macro/pooled summaries so headline regret can
be independently recomputed instead of copied from console output.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import platform
import statistics
import sys
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any

from importlib.metadata import PackageNotFoundError, version

import numpy as np

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
REPO_ROOT = CODE_ROOT.parent
sys.path.insert(0, str(CODE_ROOT))

_driver_spec = importlib.util.spec_from_file_location(
    "estimate_operator_cost", CODE_ROOT / "scripts" / "analysis" / "estimate_operator_cost.py"
)
_driver = importlib.util.module_from_spec(_driver_spec)
assert _driver_spec and _driver_spec.loader
_driver_spec.loader.exec_module(_driver)

_full_spec = importlib.util.spec_from_file_location(
    "compare_cost_estimators_full",
    CODE_ROOT / "scripts" / "analysis" / "compare_cost_estimators_full.py",
)
_full = importlib.util.module_from_spec(_full_spec)
assert _full_spec and _full_spec.loader
_full_spec.loader.exec_module(_full)

from src.planning.costs.regression import (  # noqa: E402
    pairwise_accuracy,
    regression_metrics,
    selection_metrics,
    top_k_precision,
)

REF_JSON = (
    REPO_ROOT
    / "experiments"
    / "results"
    / "operator_cost_estimation_20260726"
    / "e2e_cost_model.json"
)
DEFAULT_OUTPUT = REF_JSON.parent / "ce_context_loo_20260804.json"
TARGET = "e2e_s"
ESTIMATORS = _full.ESTIMATORS
load_rows = _full.load_rows
build = _full.build
fit_predict = _full._fit_predict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--overwrite", action="store_true")
    # Reproducibility (audit F3/F6): the historical REF_JSON points at a deleted ref + REPO_ROOT-
    # relative source_csvs, so the committed LOO JSON was not regenerable. --data-csv lets the
    # evaluator consume any runs.csv directly; --ref-json overrides the ref-json path. Both make
    # _source_evidence / source_reference robust to out-of-repo paths (no more relative_to crash).
    parser.add_argument(
        "--data-csv",
        type=Path,
        default=None,
        help="runs.csv to evaluate (e.g. the v2 cost-profile runs.csv); overrides REF_JSON",
    )
    parser.add_argument(
        "--ref-json",
        type=Path,
        default=None,
        help="ref-json to read source_csvs from (defaults to the historical REF_JSON)",
    )
    return parser.parse_args()


def _repo_relpath(path: Path) -> str:
    """path.relative_to(REPO_ROOT) when possible, else the absolute string (out-of-repo inputs)."""

    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path.resolve())


def _apply_data_source(args: argparse.Namespace) -> Callable[[], None] | None:
    """Point _full.REF_JSON + this module's REF_JSON at the requested data source.

    Returns an atexit cleanup for any temp ref created, or None. With neither --data-csv nor
    --ref-json given, leaves the historical REF_JSON untouched (legacy behavior).
    """

    global REF_JSON
    cleanup: Callable[[], None] | None = None
    ref_path: Path | None = args.ref_json
    if args.data_csv is not None:
        if not args.data_csv.is_file():
            raise SystemExit(f"--data-csv not found: {args.data_csv}")
        import tempfile

        tmp = Path(tempfile.NamedTemporaryFile(suffix=".json", delete=False).name)
        tmp.write_text(
            json.dumps(
                {
                    "source_csvs": [str(args.data_csv.resolve())],
                    "experiment_id": args.data_csv.stem,
                }
            ),
            encoding="utf-8",
        )
        ref_path = tmp

        def _cleanup(tmp: Path = tmp) -> None:
            try:
                tmp.unlink()
            except OSError:
                pass

        cleanup = _cleanup
    if ref_path is not None:
        REF_JSON = ref_path
        _full.REF_JSON = ref_path  # _full.load_rows reads this at call time
    return cleanup


def aggregate_candidate_repeats(
    actual: np.ndarray,
    predicted: np.ndarray,
    candidate_ids: list[str],
) -> list[dict[str, Any]]:
    """Average formal repeats before computing within-context ranking metrics."""

    if not (len(actual) == len(predicted) == len(candidate_ids)):
        raise ValueError("candidate aggregation inputs are not aligned")
    grouped: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for actual_value, predicted_value, candidate_id in zip(actual, predicted, candidate_ids):
        grouped[candidate_id].append((float(actual_value), float(predicted_value)))
    return [
        {
            "candidate_id": candidate_id,
            "repeat_count": len(values),
            "actual_mean_s": statistics.mean(value[0] for value in values),
            "predicted_mean_s": statistics.mean(value[1] for value in values),
        }
        for candidate_id, values in sorted(grouped.items())
    ]


def summarize(values: list[float]) -> dict[str, float | int]:
    finite = [float(value) for value in values if np.isfinite(value)]
    if not finite:
        return {"count": 0}
    return {
        "count": len(finite),
        "mean": statistics.mean(finite),
        "median": statistics.median(finite),
        "min": min(finite),
        "max": max(finite),
    }


def pool_fold_selection(folds: list[dict[str, Any]]) -> dict[str, float | int | str]:
    """Pool already-evaluated folds without changing their candidate tie order."""

    evaluated = sum(int(fold["selection"]["decision_contexts_evaluated"]) for fold in folds)
    selected_runtime = sum(float(fold["selection"]["selected_runtime"]) for fold in folds)
    oracle_runtime = sum(float(fold["selection"]["oracle_runtime"]) for fold in folds)
    picked = sum(
        float(fold["selection"]["pick_rate"])
        * int(fold["selection"]["decision_contexts_evaluated"])
        for fold in folds
    )
    return {
        "selection_status": "ok" if evaluated else "unavailable:no_multi_candidate_context",
        "decision_contexts_evaluated": evaluated,
        "pick_rate": picked / evaluated if evaluated else 0.0,
        "selected_runtime": selected_runtime,
        "oracle_runtime": oracle_runtime,
        "decision_regret_pct": (
            100.0 * (selected_runtime - oracle_runtime) / oracle_runtime
            if oracle_runtime > 0
            else 0.0
        ),
        "performance_regression_count": sum(
            int(fold["selection"]["performance_regression_count"]) for fold in folds
        ),
        "selected_plan_rank_mean": (
            sum(float(fold["selection"]["selected_plan_rank_mean"]) for fold in folds)
            / evaluated
            if evaluated
            else 0.0
        ),
        "surpassed_plans": sum(int(fold["selection"]["surpassed_plans"]) for fold in folds),
        "predicted_best_tie_contexts": sum(
            int(fold["selection"].get("predicted_best_tie_contexts", 0))
            for fold in folds
        ),
        "tie_policy": (
            "minimum predicted candidate mean; exact ties use lexicographically "
            "smallest candidate_id"
        ),
    }


def _context_payload(row: dict[str, str]) -> dict[str, str]:
    return _driver.decision_context_payload(row)


def _candidate_payload(row: dict[str, str]) -> dict[str, str]:
    return _driver.candidate_payload(row)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _package_version(distribution: str) -> str:
    try:
        return version(distribution)
    except PackageNotFoundError:
        return "unavailable"


def _source_evidence() -> list[dict[str, str]]:
    reference = json.loads(REF_JSON.read_text(encoding="utf-8"))
    paths = [Path(value).resolve() if Path(value).is_absolute() else (REPO_ROOT / value.replace("\\", "/"))
             for value in reference["source_csvs"]]
    return [
        {
            "path": _repo_relpath(path),
            "sha256": _sha256(path),
        }
        for path in paths
    ]


def _dataset_coverage(rows: list[dict[str, str]], contexts: list[str], candidates: list[str]) -> dict[str, Any]:
    context_candidates: dict[str, set[str]] = defaultdict(set)
    context_rows: dict[str, int] = defaultdict(int)
    for context, candidate in zip(contexts, candidates):
        context_candidates[context].add(candidate)
        context_rows[context] += 1
    counts = sorted((len(values) for values in context_candidates.values()), reverse=True)
    target_contexts = 20
    target_candidates = 4
    existing_deficit = sum(max(0, target_candidates - count) for count in counts)
    new_contexts = max(0, target_contexts - len(counts))
    new_context_cells = new_contexts * target_candidates
    return {
        "row_count": len(rows),
        "decision_context_count": len(counts),
        "multi_candidate_context_count": sum(count >= 2 for count in counts),
        "candidate_counts_descending": counts,
        "context_row_counts": dict(sorted(context_rows.items())),
        "planned_minimum": {
            "decision_contexts": target_contexts,
            "candidates_per_context": target_candidates,
            "additional_cells_for_existing_contexts": existing_deficit,
            "new_contexts_required": new_contexts,
            "additional_cells_for_new_contexts": new_context_cells,
            "total_additional_configuration_cells": existing_deficit + new_context_cells,
            "runs_at_one_warmup_plus_three_formal": (existing_deficit + new_context_cells) * 4,
            "duration_status": "requires measured pilot; no wall-clock estimate inferred",
        },
    }


def _promotion_contract(metrics: dict[str, list[float]]) -> dict[str, object]:
    """Plan §6 frozen gates
    (experiments/plans/completed/operator_cost_profile_dual4090_formal_20260804.md §6).

    candidate pairwise >= 0.75, median regret <= 5%, macro-MEAN regret <= 5%, MAX regret <= 15%.
    Any single failure blocks plan selection. Pure (no I/O) so the contract logic is unit-testable.
    Note: pooled regret + row-level pairwise are reported separately and are NOT gates (audit F1).
    """

    regret = metrics["decision_regret_pct"]
    candidate_pairwise = metrics["candidate_pairwise_accuracy"]
    median_pass = statistics.median(regret) <= 5.0
    macro_mean_pass = statistics.mean(regret) <= 5.0
    max_pass = max(regret) <= 15.0
    pairwise_pass = statistics.mean(candidate_pairwise) >= 0.75
    regret_pass = bool(median_pass and macro_mean_pass and max_pass)
    return {
        "candidate_pairwise_accuracy_at_least": 0.75,
        "median_decision_regret_pct_at_most": 5.0,
        "macro_mean_decision_regret_pct_at_most": 5.0,
        "max_decision_regret_pct_at_most": 15.0,
        "metric_contract_note": (
            "plan §6 frozen gates: candidate-aggregated pairwise + median + macro-mean + max "
            "regret. Pooled regret + row-level pairwise are reported separately, NOT gates."
        ),
        "regret_median_pass": median_pass,
        "regret_macro_mean_pass": macro_mean_pass,
        "regret_max_pass": max_pass,
        "pairwise_pass": pairwise_pass,
        "regret_pass": regret_pass,
        "passed": bool(regret_pass and pairwise_pass),
    }


def evaluate() -> dict[str, Any]:
    rows = load_rows()
    features, targets, _groups, contexts, candidates = build(rows)
    context_indices: dict[str, list[int]] = defaultdict(list)
    for index, context in enumerate(contexts):
        context_indices[context].append(index)
    held_out_contexts = {
        context: indices
        for context, indices in context_indices.items()
        if len({candidates[index] for index in indices}) >= 2
    }

    estimators: dict[str, dict[str, Any]] = {
        name: {"status": "ok", "folds": []} for name in ESTIMATORS
    }
    for context, test_indices in sorted(held_out_contexts.items()):
        test_set = set(test_indices)
        train_indices = [index for index in range(len(rows)) if index not in test_set]
        train_contexts = {contexts[index] for index in train_indices}
        if context in train_contexts:
            raise AssertionError(f"decision-context leakage detected: {context}")

        train = np.asarray(train_indices, dtype=int)
        test = np.asarray(test_indices, dtype=int)
        train_rows = [rows[index] for index in train]
        test_rows = [rows[index] for index in test]
        train_features, test_features = features[train], features[test]
        train_targets, test_targets = targets[train], targets[test]
        test_candidates = [candidates[index] for index in test]

        candidate_payloads: dict[str, dict[str, str]] = {}
        for index in test:
            candidate_payloads[candidates[index]] = _candidate_payload(rows[index])

        for name in ESTIMATORS:
            if estimators[name]["status"] != "ok":
                continue
            try:
                test_predictions, _ = fit_predict(
                    name,
                    train_rows,
                    test_rows,
                    train_features,
                    test_features,
                    train_targets,
                )
            except (ImportError, ModuleNotFoundError) as exc:
                estimators[name] = {
                    "status": "skipped",
                    "reason": f"{type(exc).__name__}: {exc}",
                    "folds": [],
                }
                continue
            if test_predictions is None:
                estimators[name] = {
                    "status": "skipped",
                    "reason": "optional estimator dependency unavailable",
                    "folds": [],
                }
                continue
            predictions = np.asarray(test_predictions, dtype=float)
            if len(predictions) != len(test_targets) or not np.all(np.isfinite(predictions)):
                raise ValueError(f"{name} produced invalid predictions for context {context}")

            aggregated = aggregate_candidate_repeats(test_targets, predictions, test_candidates)
            candidate_actual = np.asarray([item["actual_mean_s"] for item in aggregated])
            candidate_predicted = np.asarray([item["predicted_mean_s"] for item in aggregated])
            for item in aggregated:
                item["candidate"] = candidate_payloads[item["candidate_id"]]
            candidate_regression = regression_metrics(candidate_actual, candidate_predicted)
            selection = selection_metrics(
                test_targets,
                predictions,
                [context] * len(test),
                test_candidates,
            )
            estimators[name]["folds"].append(
                {
                    "context_id": context,
                    "context": _context_payload(rows[test_indices[0]]),
                    "train_row_count": len(train),
                    "test_row_count": len(test),
                    "candidate_count": len(aggregated),
                    "row_regression": asdict(regression_metrics(test_targets, predictions)),
                    "row_ranking": {
                        "pairwise_accuracy": pairwise_accuracy(test_targets, predictions),
                        "top_k_precision": top_k_precision(test_targets, predictions, k=5),
                        "top_k_effective": max(1, min(5, len(test_targets) // 2)),
                        "warning": "formal repeats are separate rows; use candidate_ranking for plan semantics",
                    },
                    "candidate_ranking": {
                        "spearman_rho": candidate_regression.spearman_rho,
                        "pairwise_accuracy": pairwise_accuracy(candidate_actual, candidate_predicted),
                        "top_k_precision": top_k_precision(candidate_actual, candidate_predicted, k=5),
                        "top_k_effective": max(1, min(5, len(aggregated) // 2)),
                        "predicted_best_tie_count": int(
                            np.sum(
                                np.isclose(
                                    candidate_predicted,
                                    np.min(candidate_predicted),
                                    rtol=1e-12,
                                    atol=1e-12,
                                )
                            )
                        ),
                    },
                    "selection": selection,
                    "candidates": aggregated,
                }
            )

    for name, result in estimators.items():
        if result["status"] != "ok":
            continue
        folds = result["folds"]
        if len(folds) != len(held_out_contexts):
            raise AssertionError(f"{name} evaluated {len(folds)} incomplete LOO folds")
        metrics = {
            "row_mae_s": [fold["row_regression"]["mae"] for fold in folds],
            "row_pairwise_accuracy": [
                fold["row_ranking"]["pairwise_accuracy"] for fold in folds
            ],
            "row_top_k_precision": [
                fold["row_ranking"]["top_k_precision"] for fold in folds
            ],
            "candidate_spearman": [fold["candidate_ranking"]["spearman_rho"] for fold in folds],
            "candidate_pairwise_accuracy": [
                fold["candidate_ranking"]["pairwise_accuracy"] for fold in folds
            ],
            "candidate_top_k_precision": [
                fold["candidate_ranking"]["top_k_precision"] for fold in folds
            ],
            "predicted_best_tie_count": [
                fold["candidate_ranking"]["predicted_best_tie_count"] for fold in folds
            ],
            "pick_rate": [fold["selection"]["pick_rate"] for fold in folds],
            "decision_regret_pct": [
                fold["selection"]["decision_regret_pct"] for fold in folds
            ],
            "selected_plan_rank": [
                fold["selection"]["selected_plan_rank_mean"] for fold in folds
            ],
            "surpassed_plans": [fold["selection"]["surpassed_plans"] for fold in folds],
        }
        pooled_selection = pool_fold_selection(folds)
        result["summary"] = {
            "fold_count": len(folds),
            "macro_fold_distributions": {
                metric: summarize(values) for metric, values in metrics.items()
            },
            "pooled_selection": pooled_selection,
            "promotion_contract": _promotion_contract(metrics),
        }

    return {
        "schema_version": 1,
        "evaluation": "leave_one_decision_context_out",
        "target": TARGET,
        "interpretation": "unseen decision-context generalization; not necessarily unseen candidates",
        "source_reference": _repo_relpath(REF_JSON),
        "code_evidence": {
            "context_loo_script_sha256": _sha256(Path(__file__).resolve()),
            "full_driver_sha256": _sha256(
                CODE_ROOT / "scripts" / "analysis" / "compare_cost_estimators_full.py"
            ),
            "estimator_module_sha256": _sha256(
                CODE_ROOT / "src" / "planning" / "costs" / "estimators.py"
            ),
            "metric_module_sha256": _sha256(
                CODE_ROOT / "src" / "planning" / "costs" / "regression.py"
            ),
        },
        "source_csvs": _source_evidence(),
        "dataset_coverage": _dataset_coverage(rows, contexts, candidates),
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "numpy": np.__version__,
            "lightgbm": _package_version("lightgbm"),
        },
        "estimators": estimators,
    }


def _write_output(path: Path, payload: dict[str, Any], overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"refusing to overwrite {path}; pass --overwrite to replace it")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def main() -> None:
    args = parse_args()
    cleanup = _apply_data_source(args)
    try:
        payload = evaluate()
    finally:
        if cleanup is not None:
            cleanup()
    _write_output(args.output, payload, args.overwrite)
    print(
        f"rows={payload['dataset_coverage']['row_count']} "
        f"contexts={payload['dataset_coverage']['decision_context_count']} "
        f"multi={payload['dataset_coverage']['multi_candidate_context_count']}"
    )
    print(
        "estimator          MAE-mean  rowPair  candPair  pick  "
        "regret-mean  regret-pooled  promote"
    )
    for name, result in payload["estimators"].items():
        if result["status"] != "ok":
            print(f"{name:18s} skipped: {result['reason']}")
            continue
        distributions = result["summary"]["macro_fold_distributions"]
        pooled = result["summary"]["pooled_selection"]
        print(
            f"{name:18s} {distributions['row_mae_s']['mean']:8.2f} "
            f"{distributions['row_pairwise_accuracy']['mean']:8.3f} "
            f"{distributions['candidate_pairwise_accuracy']['mean']:9.3f} "
            f"{distributions['pick_rate']['mean']:5.2f} "
            f"{distributions['decision_regret_pct']['mean']:11.2f} "
            f"{pooled['decision_regret_pct']:13.2f} "
            f"{str(result['summary']['promotion_contract']['passed']):>7s}"
        )
    print(f"evidence={args.output}")


if __name__ == "__main__":
    main()
