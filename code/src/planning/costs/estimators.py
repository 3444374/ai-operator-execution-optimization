"""Operator cost estimators CE1/CE2/CE4/CE5 complementing the existing CE3 Ridge.

Hierarchy (per the project cost-estimation baseline plan; each is a *method*, not a
system):
  CE1 Scaled Analytical   - structured linear model on physical aggregate features
                            (overhead + per-prompt-token + per-output-cap-token).
                            Domain-knowledge functional form, OLS-calibrated.
  CE2 Profile Lookup      - bucket-by-config historical median, graceful relaxation.
  CE3 Ridge (existing)    - standardized log1p ridge over the 15-feature vector
                            (see regression.py).
  CE4 FlatVector LightGBM - gradient-boosted trees over the same 15 features
                            (GRACEFUL/COSTREAM/LCM-eval classic learned baseline;
                            lightgbm imported lazily so its absence only disables CE4).
  CE5 Hybrid              - CE1 analytical base + learned residual correction
                            (project candidate; Heinrich R4 "do not discard traditional
                            cost knowledge"). Static pre-execution variant.

All estimators expose fit/predict. CE1/CE2/CE5 consume raw profile row dicts (for
physical features / bucket keys); CE3/CE4 consume the 15-element feature matrix.
The comparison driver threads both through. Nothing here modifies regression.py.
"""

from __future__ import annotations

from collections import defaultdict

import numpy as np

from .regression import RidgeCostEstimator  # used by CE3 in the driver; re-exported here for convenience


class _SignedRidge:
    """Standardized Ridge on signed targets (no log1p, no non-negativity).

    For the hybrid's learned residual correction: residuals can be negative, so the
    public RidgeCostEstimator (which log1p's and requires non-negative targets) cannot
    be reused. Same closed-form standardized ridge, plain target.
    """

    def __init__(self, alpha: float = 1.0) -> None:
        self.alpha = alpha
        self.feature_mean_: np.ndarray | None = None
        self.feature_scale_: np.ndarray | None = None
        self.coefficients_: np.ndarray | None = None

    def fit(self, features, targets) -> "_SignedRidge":
        x = np.asarray(features, dtype=float)
        y = np.asarray(targets, dtype=float)
        if x.ndim != 2 or y.ndim != 1 or len(x) != len(y) or not len(y):
            raise ValueError("features and targets have incompatible shapes")
        if not np.all(np.isfinite(x)) or not np.all(np.isfinite(y)):
            raise ValueError("features and targets must be finite")
        self.feature_mean_ = x.mean(axis=0)
        scale = x.std(axis=0)
        self.feature_scale_ = np.where(scale > 0, scale, 1.0)
        standardized = (x - self.feature_mean_) / self.feature_scale_
        design = np.column_stack([np.ones(len(standardized)), standardized])
        penalty = np.eye(design.shape[1]) * self.alpha
        penalty[0, 0] = 0.0
        self.coefficients_, *_ = np.linalg.lstsq(
            design.T @ design + penalty, design.T @ y, rcond=None
        )
        return self

    def predict(self, features) -> np.ndarray:
        assert self.coefficients_ is not None, "estimator not fitted"
        x = np.asarray(features, dtype=float)
        if x.ndim != 2 or x.shape[1] != len(self.feature_mean_):
            raise ValueError("features have incompatible shape")
        standardized = (x - self.feature_mean_) / self.feature_scale_
        design = np.column_stack([np.ones(len(standardized)), standardized])
        return design @ self.coefficients_


def _row_float(row: dict, key: str) -> float:
    value = row.get(key, "")
    if value in ("", None):
        raise KeyError(key)
    parsed = float(value)
    if not np.isfinite(parsed):
        raise ValueError(f"non-finite {key}")
    return parsed


class AnalyticalCostEstimator:
    """CE1: scaled analytical cost model.

    ``T_est = T_overhead + c_prompt * prompt_tokens + c_output * output_cap`` where
    ``output_cap = total_rows * completion_max_tokens`` (the pre-execution upper
    bound on generated tokens). Coefficients calibrated by ordinary least squares.
    The structure encodes pipeline domain knowledge (fixed overhead + per prefill
    token + per decode-cap token); it deliberately avoids the kitchen-sink 15-feature
    set, so it tests whether domain structure beats a generic learner (Heinrich R1/R4).
    """

    name = "CE1_analytical"

    def __init__(self) -> None:
        self._coef: np.ndarray | None = None

    def _design(self, rows: list[dict]) -> np.ndarray:
        n = len(rows)
        design = np.empty((n, 3), dtype=float)
        for i, r in enumerate(rows):
            prompt = _row_float(r, "token_count")
            cap = _row_float(r, "completion_max_tokens")
            total_rows = _row_float(r, "total_rows")
            design[i] = (1.0, prompt, total_rows * cap)
        return design

    def fit(self, rows: list[dict], targets) -> "AnalyticalCostEstimator":
        y = np.asarray(targets, dtype=float)
        self._coef, *_ = np.linalg.lstsq(self._design(rows), y, rcond=None)
        return self

    def predict(self, rows: list[dict]) -> np.ndarray:
        assert self._coef is not None, "estimator not fitted"
        return np.maximum(0.0, self._design(rows) @ self._coef)


class LookupCostEstimator:
    """CE2: profile bucket lookup with graceful dimension relaxation.

    Bucket by ``(model_name, completion_max_tokens, total_rows, batching_policy)``
    and predict the training-bucket median target. If a bucket has fewer than
    ``min_group`` train rows, relax by dropping the least-physical dimensions
    (batching_policy -> total_rows -> completion_max_tokens) until a level has
    enough rows, else fall back to the global training median.
    """

    name = "CE2_lookup"

    def __init__(self, min_group: int = 3) -> None:
        self.min_group = min_group
        self._dims = ("model_name", "completion_max_tokens", "total_rows", "batching_policy")
        self._levels: list[tuple[tuple[str, ...], dict[tuple, list[float]]]] = []
        self._global = 0.0

    def _key(self, row: dict, dims: tuple[str, ...]) -> tuple:
        return tuple(str(row.get(d, "")) for d in dims)

    def fit(self, rows: list[dict], targets) -> "LookupCostEstimator":
        self._global = float(np.median(np.asarray(targets, dtype=float)))
        self._levels = []
        for ndim in range(len(self._dims), 0, -1):
            dims = self._dims[:ndim]
            buckets: dict[tuple, list[float]] = defaultdict(list)
            for r, t in zip(rows, targets):
                buckets[self._key(r, dims)].append(float(t))
            self._levels.append((dims, buckets))
        return self

    def _lookup(self, row: dict) -> float:
        for dims, buckets in self._levels:
            values = buckets.get(self._key(row, dims))
            if values and len(values) >= self.min_group:
                return float(np.median(values))
        return self._global

    def predict(self, rows: list[dict]) -> np.ndarray:
        return np.asarray([self._lookup(r) for r in rows], dtype=float)


class LightGBMCostEstimator:
    """CE4: FlatVector LightGBM learned baseline.

    Gradient-boosted trees over the same 15-feature vector used by CE3 Ridge
    (GRACEFUL FlatVector / COSTREAM / LCM-eval). lightgbm is imported lazily so a
    missing dependency only disables this estimator rather than the whole comparison.
    """

    name = "CE4_lightgbm"

    def __init__(self, **params) -> None:
        self._params = params
        self._model = None

    def fit(self, features, targets) -> "LightGBMCostEstimator":
        try:
            import lightgbm as lgb  # lazy import
        except (ImportError, OSError) as exc:  # OSError covers missing DLL deps on Windows
            raise ImportError(f"lightgbm unavailable: {exc}") from exc

        x = np.asarray(features, dtype=float)
        y = np.asarray(targets, dtype=float)
        # Native API (lgb.train + Dataset) avoids the scikit-learn dependency that
        # the LGBMRegressor sklearn wrapper requires.
        params = {
            "objective": "regression",
            "metric": "rmse",
            "num_leaves": 15,
            "learning_rate": 0.05,
            "min_data_in_leaf": 3,
            "feature_fraction": 0.9,
            "subsample_for_bin": 64,
            "verbose": -1,
        }
        params.update(self._params)
        self._model = lgb.train(params, lgb.Dataset(x, label=y), num_boost_round=200)
        return self

    def predict(self, features) -> np.ndarray:
        assert self._model is not None, "estimator not fitted"
        return np.maximum(0.0, self._model.predict(np.asarray(features, dtype=float)))


class HybridCostEstimator:
    """CE5: analytical base + learned residual correction (project candidate).

    ``T_pred = analytical(rows) + ridge(features on residual = target - analytical)``.
    Preserves the analytical pipeline structure; the learner only corrects the
    formula's residual, never refits the whole E2E surface. Static pre-execution
    variant of the project method (runtime-state correction is a separate track).
    """

    name = "CE5_hybrid"

    def __init__(self, alpha: float = 1.0) -> None:
        self._analytical = AnalyticalCostEstimator()
        self._ridge = _SignedRidge(alpha=alpha)

    def fit(self, rows: list[dict], features, targets) -> "HybridCostEstimator":
        self._analytical.fit(rows, targets)
        base = self._analytical.predict(rows)
        residual = np.asarray(targets, dtype=float) - base
        self._ridge.fit(features, residual)
        return self

    def predict(self, rows: list[dict], features) -> np.ndarray:
        return np.maximum(
            0.0,
            self._analytical.predict(rows) + self._ridge.predict(features),
        )
