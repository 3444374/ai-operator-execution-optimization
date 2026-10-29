"""Submission batches of complete prepared requests; no admission or inference."""
from dataclasses import dataclass

from src.modalities.text.tokenization import chat_token_count
from src.scheduling.organization.batching import PendingBatchBuilder, RowArrival
from src.serving.backends.vllm_engine import _prepared


@dataclass(frozen=True)
class PreparedBatching:
    policy: str
    max_rows: int
    token_budget: int = 0

    def __post_init__(self):
        if self.policy not in ('fixed_rows', 'token_budget'):
            raise ValueError('prepared batching requires fixed_rows or token_budget')
        if type(self.max_rows) is not int or self.max_rows <= 0:
            raise ValueError('prepared batching max_rows must be positive')
        if (type(self.token_budget) is not int or self.token_budget < 0 or
                (self.policy == 'token_budget') != (self.token_budget > 0)):
            raise ValueError('a positive B is required only for token_budget batching')


@dataclass(frozen=True)
class PreparedRequestBatch:
    rows: tuple
    input_tokens: int
    output_cap_tokens: int
    reason: str
    oversize: bool

    @property
    def estimated_tokens(self):
        return self.input_tokens + self.output_cap_tokens


class PreparedRequestBatcher:
    """Pack one model deployment and generation configuration in input order.

    Read chunks do not close a batch. Only the source EOF closes its remaining
    tail. Oversize requests remain intact as singleton batches.
    """
    def __init__(self, config, tokenizer):
        if type(config) is not PreparedBatching:
            raise TypeError('prepared batching needs PreparedBatching')
        self.config, self.tokenizer = config, tokenizer

    def batches(self, rows, generation):
        # The worker still accepts a finite ready set; preserve its validation
        # and presubmission estimation boundary for the existing callers.
        _prepared(rows)
        return tuple(self.iter_batches((rows,), generation, tail_reason='ready_exhausted'))

    def iter_batches(self, chunks, generation, *, max_bytes=None, size=None,
                     tail_reason='source_exhausted'):
        """Accumulate across reads with independent row and logical-byte guards.

        ``size`` charges the caller's pending payload and prepared-message bytes;
        it is not a process RSS estimate. No source prefetch, timer or model work
        is introduced here. The finite source must make progress synchronously.
        """
        if max_bytes is not None and (type(max_bytes) is not int or max_bytes <= 0 or not callable(size)):
            raise ValueError('a byte guard requires a positive max_bytes and size function')
        builder = PendingBatchBuilder(self.config.max_rows, self.config.token_budget)
        compatibility, pending_rows, pending_bytes, seen = None, 0, 0, set()

        def close(reason):
            nonlocal pending_rows, pending_bytes
            batch = builder.close()
            result = PreparedRequestBatch(tuple(r.payload_ref for r in batch.rows),
                batch.prompt_tokens, batch.estimated_output_tokens, reason,
                self.config.token_budget > 0 and batch.estimated_total_tokens > self.config.token_budget)
            pending_rows, pending_bytes = 0, 0
            return result

        for chunk in chunks:
            for row in chunk:
                _, conversations = _prepared([row])
                if row['call_id'] in seen:
                    raise ValueError('prepared source repeated a call identity')
                seen.add(row['call_id'])
                charge = 0 if max_bytes is None else size(row)
                if type(charge) is not int or charge < 0 or max_bytes is not None and charge > max_bytes:
                    raise BufferError('complete prepared request exceeds the logical byte allowance')
                arrival = RowArrival(row['call_id'], 0.0,
                    chat_token_count(self.tokenizer, conversations[0]), generation.max_tokens, '', row)
                key = tuple(row[name] for name in ('query_id', 'algorithm_stage', 'model_role'))
                if pending_rows and key != compatibility:
                    yield close('compatibility')
                if builder.would_exceed_token_budget(arrival):
                    yield close('token_budget')
                if pending_rows and max_bytes is not None and pending_bytes + charge > max_bytes:
                    yield close('max_bytes')
                compatibility = key
                pending_rows += 1
                pending_bytes += charge
                if builder.add(arrival):
                    yield close('max_rows' if pending_rows == self.config.max_rows else 'token_budget')
        if pending_rows:
            yield close(tail_reason)
