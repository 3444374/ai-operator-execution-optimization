"""Bounded complete-request ordering candidates; engine batches stay engine-owned."""
from dataclasses import dataclass

from ..core.session_contract import TaskKey


@dataclass(frozen=True)
class PreparedRequestOrder:
    mode: str
    candidate_limit: int
    prefix_token_limit: int
    block_tokens: int

    def __post_init__(self):
        if self.mode not in ('length', 'prefix', 'prefix_length'):
            raise ValueError('unknown prepared-request ordering candidate')
        if any(type(v) is not int or v <= 0 for v in
               (self.candidate_limit, self.prefix_token_limit, self.block_tokens)):
            raise ValueError('candidate, prefix and block hypotheses must be positive integers')


@dataclass(frozen=True)
class RequestOrganizationProfile:
    key: TaskKey
    compatibility: tuple
    prompt_tokens: int
    output_cap: int
    token_prefix: tuple[int, ...]

def order_prepared_window(profiles, config):
    """Keep the oldest request first, reorder only its compatible finite cohort.

    Prefix blocks and complete-request length are proxies. They do not assert
    resident KV, GPU iteration membership, graph padding, or reduced compute.
    """
    # The adapter supplies accepted identities and bounded, tokenized profiles.
    if not profiles:
        return ()
    anchor = profiles[0]
    compatible = [p for p in profiles[1:] if p.compatibility == anchor.compatibility]
    others = [p for p in profiles[1:] if p.compatibility != anchor.compatibility]

    def score(value):
        distance = abs(value.prompt_tokens + value.output_cap - anchor.prompt_tokens - anchor.output_cap)
        if config.mode == 'length':
            return (distance,)
        common = 0
        for left, right in zip(anchor.token_prefix, value.token_prefix):
            if left != right:
                break
            common += 1
        blocks = common // config.block_tokens
        if config.mode == 'prefix':
            return (-blocks,)
        return -blocks, distance

    compatible.sort(key=score)  # Stable ties preserve the received candidate order.
    return tuple(p.key for p in (anchor, *compatible, *others))
