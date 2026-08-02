"""Distribution summaries for batch-level results."""

from __future__ import annotations

import math
import statistics


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = math.ceil((p / 100.0) * len(ordered)) - 1
    index = min(max(index, 0), len(ordered) - 1)
    return ordered[index]

def batch_result_stats(results: list[dict]) -> dict[str, float | int]:
    rows = [int(result.get("rows", 0)) for result in results]
    tokens = [int(result.get("token_count", 0)) for result in results]
    service_s = [float(result.get("service_s", 0.0)) for result in results]
    return {
        "batch_rows_min": min(rows) if rows else 0,
        "batch_rows_max": max(rows) if rows else 0,
        "batch_rows_mean": statistics.mean(rows) if rows else 0.0,
        "batch_tokens_min": min(tokens) if tokens else 0,
        "batch_tokens_max": max(tokens) if tokens else 0,
        "batch_tokens_mean": statistics.mean(tokens) if tokens else 0.0,
        "batch_tokens_p50": percentile([float(value) for value in tokens], 50),
        "batch_tokens_p95": percentile([float(value) for value in tokens], 95),
        "batch_service_s_p50": percentile(service_s, 50),
        "batch_service_s_p95": percentile(service_s, 95),
        "batch_service_s_p99": percentile(service_s, 99),
    }
