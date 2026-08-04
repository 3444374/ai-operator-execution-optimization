"""Ground-truth retrieval quality metrics for AI_EMBED experiments."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence


def retrieval_quality_metrics(
    ranked_ids: Mapping[str, Sequence[str]],
    relevant_ids: Mapping[str, set[str] | frozenset[str]],
    *,
    k_values: Sequence[int] = (1, 5, 10),
) -> dict[str, float | int | str]:
    """Compute macro Recall@K, MRR, and nDCG@K from explicit relevance.

    Queries without a non-empty relevance set are excluded and counted.  The
    function never substitutes embedding checksum or self-neighbor overlap for
    task relevance.
    """

    if not k_values or any(k <= 0 for k in k_values):
        raise ValueError("k_values must contain positive integers")
    valid_queries = [
        query_id
        for query_id, relevance in relevant_ids.items()
        if relevance and query_id in ranked_ids
    ]
    result: dict[str, float | int | str] = {
        "retrieval_quality_status": (
            "ok" if valid_queries else "unavailable:no_queries_with_relevance"
        ),
        "retrieval_queries_observed": len(valid_queries),
        "retrieval_queries_excluded": len(relevant_ids) - len(valid_queries),
    }
    reciprocal_ranks = []
    for query_id in valid_queries:
        relevance = relevant_ids[query_id]
        first_rank = next(
            (
                rank
                for rank, candidate_id in enumerate(
                    ranked_ids[query_id],
                    start=1,
                )
                if candidate_id in relevance
            ),
            None,
        )
        reciprocal_ranks.append(1.0 / first_rank if first_rank else 0.0)
    result["mrr"] = (
        sum(reciprocal_ranks) / len(reciprocal_ranks)
        if reciprocal_ranks
        else 0.0
    )
    for k in sorted(set(k_values)):
        recalls = []
        ndcgs = []
        for query_id in valid_queries:
            relevance = relevant_ids[query_id]
            top_k = list(ranked_ids[query_id])[:k]
            hits = [candidate_id in relevance for candidate_id in top_k]
            recalls.append(sum(hits) / len(relevance))
            dcg = sum(
                1.0 / math.log2(rank + 1)
                for rank, hit in enumerate(hits, start=1)
                if hit
            )
            ideal_hits = min(k, len(relevance))
            ideal_dcg = sum(
                1.0 / math.log2(rank + 1)
                for rank in range(1, ideal_hits + 1)
            )
            ndcgs.append(dcg / ideal_dcg if ideal_dcg > 0 else 0.0)
        result[f"recall_at_{k}"] = (
            sum(recalls) / len(recalls) if recalls else 0.0
        )
        result[f"ndcg_at_{k}"] = sum(ndcgs) / len(ndcgs) if ndcgs else 0.0
    return result
