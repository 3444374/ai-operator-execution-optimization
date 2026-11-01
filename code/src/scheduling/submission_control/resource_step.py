"""Costed native KV proposals; neither GPU compute capacity nor admission grants.

The adapter binds one compatible, single KV pool and already prepared token
counts to the current engine/session. Unknown layout or clocks must stay unknown.
Core and the native engine retain their independent hard limits and ownership.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Hashable


@dataclass(frozen=True)
class NativeKVFrame:
    engine_uid: str | None
    session_id: int | None
    sequence: int | None
    # Both timestamps have already been mapped into clock_domain by the adapter.
    observed_at_s: float | None
    received_at_s: float | None
    total_blocks: int | None
    block_tokens: int | None
    usage_fraction: float | None
    clock_domain: str | None = "driver.monotonic"
    cost_model: str | None = None


@dataclass(frozen=True)
class RequestKVWork:
    request_key: Hashable
    prompt_tokens: int | None
    max_output_tokens: int | None


@dataclass(frozen=True)
class ResourceStep:
    target_window: int | None
    reason: str
    evidence: dict = field(default_factory=dict)


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def propose_kv_window(frame, *, now_s, max_age_s, expected_engine_uid,
                      expected_session_id, current_window, hard_limit,
                      ready, committed, clock_domain="driver.monotonic"):
    """Propose a costed ready prefix, or explicitly require geometric fallback.

    The only supported cost model is ordinary single-pool, single-sequence
    token blocks. The adapter must have excluded extra lookahead/speculative or
    hybrid-pool requirements. Prompt counts use the same prepared chat template;
    generation supplies the real maximum output length, never an observed mean.
    All already committed requests are reserved at their *full* token bound in
    addition to sampled pool usage. This double reservation is conservative and
    avoids treating outstanding growth as free capacity. It is not a guarantee
    against unrelated future tenants, nor a prediction of a performance plateau.
    """
    if (type(current_window) is not int or type(hard_limit) is not int
            or not 1 <= current_window <= hard_limit
            or not _finite(now_s) or now_s < 0
            or not _finite(max_age_s) or max_age_s <= 0):
        raise ValueError("valid request limits and caller clock bounds are required")
    evidence = dict(resource="native_KV_token_blocks", physical_headroom=None,
        compute_headroom=None, cost_model=None, hard_limit=hard_limit,
        fallback_scope="this optional cost estimator only; resource signals may still drive a trial",
        reservation="sampled used blocks plus full committed-request bounds",
        performance_platform_proven=False)

    def unavailable(reason):
        return ResourceStep(None, reason, dict(evidence, fallback_required=True))

    if not isinstance(frame, NativeKVFrame):
        return unavailable("native_kv_frame_unknown")
    evidence.update(engine_uid=frame.engine_uid, session_id=frame.session_id,
                    sequence=frame.sequence, cost_model=frame.cost_model)
    if (not isinstance(expected_engine_uid, str) or not expected_engine_uid
            or frame.engine_uid != expected_engine_uid
            or type(expected_session_id) is not int or expected_session_id < 0
            or type(frame.session_id) is not int or frame.session_id != expected_session_id
            or type(frame.sequence) is not int or frame.sequence < 0):
        return unavailable("native_kv_identity_unknown_or_mismatched")
    if (not clock_domain or frame.clock_domain != clock_domain
            or not _finite(frame.observed_at_s) or not _finite(frame.received_at_s)
            or not 0 <= frame.observed_at_s <= frame.received_at_s <= now_s):
        return unavailable("native_kv_clock_unknown_or_invalid")
    evidence.update(sample_age_s=now_s-frame.observed_at_s,
                    receive_age_s=now_s-frame.received_at_s)
    if now_s-frame.observed_at_s > max_age_s:
        return unavailable("native_kv_frame_stale")
    if (type(frame.total_blocks) is not int or frame.total_blocks < 1
            or type(frame.block_tokens) is not int or frame.block_tokens < 1
            or not _finite(frame.usage_fraction) or not 0 <= frame.usage_fraction <= 1):
        return unavailable("native_kv_capacity_unknown_or_invalid")
    if frame.cost_model != "single_pool_token_blocks":
        return unavailable("native_kv_cost_model_unknown_or_unsupported")
    if type(ready) is not tuple or type(committed) is not tuple:
        return unavailable("request_cost_sequences_unknown")
    keys, costs = set(), []
    for work in committed+ready:
        if (not isinstance(work, RequestKVWork)
                or type(work.prompt_tokens) is not int or work.prompt_tokens < 0
                or type(work.max_output_tokens) is not int or work.max_output_tokens < 1):
            return unavailable("prepared_request_cost_unknown_or_invalid")
        try:
            if work.request_key is None or work.request_key in keys:
                return unavailable("request_identity_missing_or_duplicated")
            keys.add(work.request_key)
        except TypeError:
            return unavailable("request_identity_missing_or_duplicated")
        tokens = work.prompt_tokens+work.max_output_tokens
        costs.append((tokens+frame.block_tokens-1)//frame.block_tokens)
    free = math.floor(frame.total_blocks*(1-frame.usage_fraction))
    reserved = sum(costs[:len(committed)])
    available = max(0, free-reserved)
    prefix, used = 0, 0
    for cost in costs[len(committed):]:
        if len(committed)+prefix >= hard_limit or used+cost > available:
            break
        used += cost
        prefix += 1
    target = len(committed)+prefix
    evidence.update(total_blocks=frame.total_blocks, block_tokens=frame.block_tokens,
        sampled_free_blocks=free, committed_requests=len(committed),
        reserved_committed_blocks=reserved, available_blocks=available,
        ready_requests=len(ready), fitting_ready_prefix=prefix, prefix_blocks=used,
        tested_per_request_cost="ceil((prepared_prompt_tokens+max_output_tokens)/block_tokens)")
    if prefix == 0 or target <= current_window or target > hard_limit:
        return unavailable("native_kv_no_costed_growth")
    return ResourceStep(target, "native_kv_costed_ready_prefix",
                        dict(evidence, fallback_required=False))
