"""Exactly-once validation and shared summary metrics for baseline runs."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Iterable

from .contracts import BaselineRequestResult, ChatRequest


def validate_results(
    requests: Iterable[ChatRequest],
    results: Iterable[BaselineRequestResult],
) -> None:
    """Reject any run that lacks successful exactly-once completion."""

    expected = tuple(requests)
    observed = tuple(results)
    expected_ids = [request.doc_id for request in expected]
    observed_ids = [result.doc_id for result in observed]
    if (
        len(observed_ids) != len(expected_ids)
        or len(set(observed_ids)) != len(observed_ids)
        or set(observed_ids) != set(expected_ids)
    ):
        raise ValueError(
            "exactly-once validation failed: "
            f"expected={len(expected_ids)} observed={len(observed_ids)}"
        )
    request_by_id = {request.doc_id: request for request in expected}
    for result in observed:
        request = request_by_id[result.doc_id]
        if result.status != "completed" or result.error:
            raise ValueError(
                f"failed request result for doc_id={result.doc_id}: "
                f"{result.error or result.status}"
            )
        if result.endpoint_index != request.endpoint_index:
            raise ValueError(
                "endpoint assignment mismatch for "
                f"doc_id={result.doc_id}"
            )
        if result.completed_at_s < result.submitted_at_s:
            raise ValueError(
                f"negative request latency for doc_id={result.doc_id}"
            )


def _quantile(values: list[float], probability: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return (
        ordered[lower] * (1 - fraction)
        + ordered[upper] * fraction
    )


def summarize_results(
    requests: Iterable[ChatRequest],
    results: Iterable[BaselineRequestResult],
) -> dict[str, object]:
    """Return comparable end-to-end metrics after hard validity checks."""

    expected = tuple(requests)
    observed = tuple(results)
    validate_results(expected, observed)
    input_tokens = sum(result.input_tokens for result in observed)
    output_tokens = sum(result.output_tokens for result in observed)
    total_tokens = input_tokens + output_tokens
    if observed:
        jct_s = (
            max(result.completed_at_s for result in observed)
            - min(result.submitted_at_s for result in observed)
        )
    else:
        jct_s = 0.0
    latencies = [result.latency_s for result in observed]
    endpoint_counts = Counter(
        result.endpoint_index for result in observed
    )
    endpoint_work: dict[int, int] = defaultdict(int)
    for request in expected:
        endpoint_work[request.endpoint_index] += request.estimated_work
    work_values = list(endpoint_work.values())
    endpoint_work_skew = (
        (max(work_values) - min(work_values)) / max(work_values)
        if work_values and max(work_values) > 0
        else 0.0
    )
    return {
        "request_count": len(expected),
        "completed_count": len(observed),
        "failed_count": 0,
        "exactly_once": True,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "jct_s": jct_s,
        "tokens_per_s": total_tokens / jct_s if jct_s > 0 else 0.0,
        "latency_p50_s": _quantile(latencies, 0.50),
        "latency_p95_s": _quantile(latencies, 0.95),
        "latency_p99_s": _quantile(latencies, 0.99),
        "endpoint_counts": dict(sorted(endpoint_counts.items())),
        "endpoint_predicted_work": dict(sorted(endpoint_work.items())),
        "endpoint_work_skew": endpoint_work_skew,
    }
