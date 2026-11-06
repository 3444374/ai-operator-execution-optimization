"""Existing offline estimator selection over prepared profile data."""
from __future__ import annotations

import numpy as np

from src.planning.costs.regression import RidgeCostEstimator
from src.planning.costs.estimators import (
    AnalyticalCostEstimator,
    HybridCostEstimator,
    LightGBMCostEstimator,
    LookupCostEstimator,
)


ESTIMATORS = ("CE0_mean", "CE1_analytical", "CE2_lookup", "CE3_ridge", "CE4_lightgbm", "CE5_hybrid")


def fit_predict(name, train_rows, test_rows, train_feat, test_feat, train_y):
    """Return (test_pred, train_pred_for_residuals) or (None, None) if unavailable."""
    if name == "CE0_mean":
        m = float(np.mean(train_y))
        return np.full(len(test_rows), m), np.full(len(train_rows), m)
    if name == "CE1_analytical":
        e = AnalyticalCostEstimator().fit(train_rows, train_y)
        return e.predict(test_rows), e.predict(train_rows)
    if name == "CE2_lookup":
        e = LookupCostEstimator().fit(train_rows, train_y)
        return e.predict(test_rows), e.predict(train_rows)
    if name == "CE3_ridge":
        e = RidgeCostEstimator(alpha=1.0).fit(train_feat, train_y)
        return e.predict(test_feat), e.predict(train_feat)
    if name == "CE4_lightgbm":
        try:
            e = LightGBMCostEstimator().fit(train_feat, train_y)
        except (ImportError, ModuleNotFoundError):
            return None, None
        return e.predict(test_feat), e.predict(train_feat)
    if name == "CE5_hybrid":
        e = HybridCostEstimator(alpha=1.0).fit(train_rows, train_feat, train_y)
        return e.predict(test_rows, test_feat), e.predict(train_rows, train_feat)
    raise ValueError(name)
