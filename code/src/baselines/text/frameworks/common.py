"""Shared input validation for vendor-native text runtime shards."""

from __future__ import annotations

from src.baselines.common.contracts import ChatRequest


def validate_single_endpoint_shard(
    requests: tuple[ChatRequest, ...],
    max_tokens: int,
) -> None:
    """Require one endpoint and one output cap without changing row semantics."""

    if max_tokens < 0:
        raise ValueError("max_tokens must be non-negative")
    caps = {request.max_output_tokens for request in requests}
    if caps and caps != {max_tokens}:
        raise ValueError(
            "official runtime shard requires the same max_output_tokens "
            "for every request"
        )
    endpoint_indexes = {request.endpoint_index for request in requests}
    if len(endpoint_indexes) > 1:
        raise ValueError(
            "official runtime adapter accepts one endpoint shard at a time"
        )
