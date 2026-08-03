"""Dependency-light operator cost estimation over pre-execution features."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass

import numpy as np


FEATURE_NAMES = (
    "total_rows",
    "prompt_token_count",
    "completion_max_tokens",
    "token_budget",
    "packing_batch_count",
    "batch_estimated_cost_p50",
    "batch_estimated_cost_p95",
    "batch_estimated_cost_max",
    "max_inflight_limit",
    "flush_timeout_ms",
    "flush_max_wait_ms",
    "arrival_time_scale",
    "arrival_replay_enabled",
    "flush_is_adaptive",
    "flush_is_immediate",
)


@dataclass(frozen=True)
class DatasetSplit:
    train_indices: tuple[int, ...]
    test_indices: tuple[int, ...]
    train_groups: tuple[str, ...]
    test_groups: tuple[str, ...]


@dataclass(frozen=True)
class RegressionMetrics:
    count: int
    mae: float
    mape_pct: float
    rmse: float
    r2: float


def grouped_train_test_split(
    groups: list[str],
    *,
    test_fraction: float,
    seed: int,
) -> DatasetSplit:
    if not groups:
        raise ValueError("groups must be non-empty")
    if not math.isfinite(test_fraction) or not 0.0 < test_fraction < 1.0:
        raise ValueError("test_fraction must be between 0 and 1")
    unique_groups = sorted(set(groups))
    if len(unique_groups) < 2:
        raise ValueError("at least two groups are required")
    ranked = sorted(
        unique_groups,
        key=lambda value: hashlib.sha256(
            f"{seed}:{value}".encode("utf-8")
        ).digest(),
    )
    test_count = min(
        len(ranked) - 1,
        max(1, math.floor(len(ranked) * test_fraction + 0.5)),
    )
    test_groups = frozenset(ranked[:test_count])
    train_indices = tuple(
        index for index, group in enumerate(groups) if group not in test_groups
    )
    test_indices = tuple(
        index for index, group in enumerate(groups) if group in test_groups
    )
    return DatasetSplit(
        train_indices=train_indices,
        test_indices=test_indices,
        train_groups=tuple(sorted(set(groups) - test_groups)),
        test_groups=tuple(sorted(test_groups)),
    )


class RidgeCostEstimator:
    """Ridge regression on standardized features and log1p execution cost."""

    def __init__(self, alpha: float = 1.0):
        if not math.isfinite(alpha) or alpha < 0:
            raise ValueError("alpha must be finite and non-negative")
        self.alpha = alpha
        self.feature_mean_: np.ndarray | None = None
        self.feature_scale_: np.ndarray | None = None
        self.coefficients_: np.ndarray | None = None

    def fit(self, features: np.ndarray, targets: np.ndarray) -> "RidgeCostEstimator":
        x, y = _validated_arrays(features, targets)
        self.feature_mean_ = x.mean(axis=0)
        scale = x.std(axis=0)
        self.feature_scale_ = np.where(scale > 0, scale, 1.0)
        standardized = (x - self.feature_mean_) / self.feature_scale_
        design = np.column_stack([np.ones(len(standardized)), standardized])
        penalty = np.eye(design.shape[1]) * self.alpha
        penalty[0, 0] = 0.0
        self.coefficients_ = np.linalg.pinv(
            design.T @ design + penalty
        ) @ design.T @ np.log1p(y)
        return self

    def predict(self, features: np.ndarray) -> np.ndarray:
        if (
            self.feature_mean_ is None
            or self.feature_scale_ is None
            or self.coefficients_ is None
        ):
            raise RuntimeError("estimator is not fitted")
        x = np.asarray(features, dtype=float)
        if x.ndim != 2 or x.shape[1] != len(self.feature_mean_):
            raise ValueError("features have incompatible shape")
        standardized = (x - self.feature_mean_) / self.feature_scale_
        design = np.column_stack([np.ones(len(standardized)), standardized])
        return np.maximum(0.0, np.expm1(design @ self.coefficients_))


def regression_metrics(
    actual: np.ndarray,
    predicted: np.ndarray,
    *,
    mape_epsilon: float = 1e-6,
) -> RegressionMetrics:
    y = np.asarray(actual, dtype=float)
    y_hat = np.asarray(predicted, dtype=float)
    if y.ndim != 1 or y_hat.ndim != 1 or len(y) != len(y_hat) or not len(y):
        raise ValueError("actual and predicted must be equal non-empty vectors")
    errors = y_hat - y
    denominator = np.maximum(np.abs(y), mape_epsilon)
    residual_sum = float(np.sum(errors**2))
    total_sum = float(np.sum((y - y.mean()) ** 2))
    return RegressionMetrics(
        count=len(y),
        mae=float(np.mean(np.abs(errors))),
        mape_pct=float(np.mean(np.abs(errors) / denominator) * 100.0),
        rmse=float(np.sqrt(np.mean(errors**2))),
        r2=(1.0 - residual_sum / total_sum) if total_sum > 0 else 0.0,
    )


def _validated_arrays(
    features: np.ndarray,
    targets: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    x = np.asarray(features, dtype=float)
    y = np.asarray(targets, dtype=float)
    if x.ndim != 2 or y.ndim != 1 or len(x) != len(y) or not len(y):
        raise ValueError("features and targets have incompatible shapes")
    if not np.all(np.isfinite(x)) or not np.all(np.isfinite(y)):
        raise ValueError("features and targets must be finite")
    if np.any(y < 0):
        raise ValueError("targets must be non-negative")
    return x, y
