"""Numerical matrix cost estimation with an optional LightGBM dependency."""

from __future__ import annotations

import numpy as np


class LightGBMCostEstimator:
    """CE4: FlatVector LightGBM learned baseline.

    Gradient-boosted trees over the same pre-execution vector used by CE3 Ridge
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
