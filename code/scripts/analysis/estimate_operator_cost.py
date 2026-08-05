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

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.planning.costs.regression import (  # noqa: E402
    FEATURE_NAMES,
    RidgeCostEstimator,
    grouped_train_test_split,
    regression_metrics,
    selection_metrics,
)


DECISION_CONTEXT_FIELDS = (
    "model_name",
    "cost_model_id",
    "source_workload_name",
    "total_rows",
    "completion_max_tokens",
    "arrival_replay",
    "arrival_time_scale",
    "server_version",
    "pgvector_version",
    "model_backend",
    "completion_protocol",
    "completion_http_transport",
    "service_prefix_caching",
)

CANDIDATE_FIELDS = (
    "batching_policy",
    "output_cost_mode",
    "token_budget",
    "max_inflight_limit",
    "max_active_work_per_endpoint",
    "actor_workers_per_endpoint",
    "ray_actor_max_concurrency",
    "endpoint_count",
    "admission_scope",
    "per_endpoint_inflight_limit",
    "service_quantum_tokens",
    "submission_granularity",
    "flush_policy",
    "flush_timeout_ms",
    "flush_max_wait_ms",
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
        _number_or_default(row, "max_active_work_per_endpoint"),
        _number_or_default(row, "per_endpoint_inflight_limit"),
        _number_or_default(row, "actor_workers_per_endpoint"),
        _number_or_default(row, "ray_actor_max_concurrency"),
        float(_endpoint_count(row)),
        _number_or_default(row, "service_quantum_tokens"),
        _number_or_default(row, "gpu_peak_tflops"),
        _per_endpoint_number(row, "gpu_memory_total_mib"),
    ]


def scenario_group(row: dict[str, str]) -> str:
    signature = json.dumps(
        {
            "context": decision_context_payload(row),
            "candidate": candidate_payload(row),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16]


def _signature(row: dict[str, str], fields: tuple[str, ...]) -> str:
    payload = json.dumps(
        {field: row.get(field, "") for field in fields},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _payload_signature(payload: dict[str, str]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def decision_context_payload(row: dict[str, str]) -> dict[str, str]:
    """Identity for one comparable plan-selection context.

    GPU model/memory and serving protocol are part of the context so profiles from
    different physical environments cannot be merged into one plan-selection fold.
    Endpoint count remains a candidate field: choosing one or more endpoints can be
    an optimizer decision on a fixed host.
    """

    payload = {field: row.get(field, "") for field in DECISION_CONTEXT_FIELDS}
    payload["gpu_model"] = _normalized_gpu_model(row.get("gpu_name", ""))
    payload["gpu_memory_total_mib_per_gpu"] = str(
        _per_endpoint_number(row, "gpu_memory_total_mib")
    )
    return payload


def candidate_payload(row: dict[str, str]) -> dict[str, str]:
    payload = {field: row.get(field, "") for field in CANDIDATE_FIELDS}
    payload["endpoint_count"] = str(_endpoint_count(row))
    return payload


def is_formal_profile_row(row: dict[str, str]) -> bool:
    """Accept only measured formal repeats for estimator fitting/evaluation.

    Warm-up runs exercise caches and compilation and are intentionally excluded.
    Missing phase metadata fails closed instead of being silently treated as formal.
    """

    return row.get("phase", "").strip().lower() == "formal"


def load_dataset(
    paths: list[Path],
    target: str,
) -> tuple[
    np.ndarray,
    np.ndarray,
    list[str],
    int,
    list[str],
    list[str],
]:
    features = []
    targets = []
    groups = []
    decision_contexts = []
    candidate_ids = []
    excluded = 0
    for path in paths:
        with path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                if not is_formal_profile_row(row):
                    excluded += 1
                    continue
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
                decision_contexts.append(_payload_signature(decision_context_payload(row)))
                candidate_ids.append(_payload_signature(candidate_payload(row)))
    if not features:
        raise ValueError("no complete profile rows were loaded")
    return (
        np.asarray(features, dtype=float),
        np.asarray(targets, dtype=float),
        groups,
        excluded,
        decision_contexts,
        candidate_ids,
    )


def estimate(
    paths: list[Path],
    *,
    target: str,
    test_fraction: float,
    seed: int,
    alpha: float,
) -> dict[str, object]:
    (
        features,
        targets,
        groups,
        excluded,
        decision_contexts,
        candidate_ids,
    ) = load_dataset(paths, target)
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


def _number(row: dict[str, str], field: str) -> float:
    value = row.get(field, "")
    if value in ("", None):
        raise ValueError(f"missing numeric field {field}")
    parsed = float(value)
    if not np.isfinite(parsed):
        raise ValueError(f"non-finite numeric field {field}")
    return parsed


def _number_or_default(
    row: dict[str, str],
    field: str,
    default: float = 0.0,
) -> float:
    value = row.get(field, "")
    if value in ("", None):
        return default
    parsed = float(value)
    if not np.isfinite(parsed):
        raise ValueError(f"non-finite numeric field {field}")
    return parsed


def _endpoint_count(row: dict[str, str]) -> int:
    explicit = _number_or_default(row, "endpoint_count")
    if explicit > 0 and explicit.is_integer():
        return int(explicit)
    identifiers = str(row.get("endpoint_gpu_ids", "")).replace(",", ";")
    values = [value.strip() for value in identifiers.split(";") if value.strip()]
    return len(values) if values else 1


def _per_endpoint_number(row: dict[str, str], field: str) -> float:
    raw = str(row.get(field, "") or "").strip()
    if not raw:
        return 0.0
    parts = [value.strip() for value in raw.split(";") if value.strip()]
    values = [float(value) for value in parts]
    if not values or not all(np.isfinite(value) for value in values):
        raise ValueError(f"invalid per-endpoint numeric field {field}")
    if len(values) > 1:
        return sum(values) / len(values)
    return values[0] / _endpoint_count(row)


def _normalized_gpu_model(value: str) -> str:
    models = [item.strip() for item in str(value).split(";") if item.strip()]
    return ";".join(sorted(set(models)))


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
