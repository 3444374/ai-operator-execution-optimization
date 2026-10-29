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

    The ready set is finite: its tail flushes immediately, without waiting for
    more source rows. Oversize requests remain intact as singleton batches.
    """
    def __init__(self, config, tokenizer):
        if type(config) is not PreparedBatching:
            raise TypeError('prepared batching needs PreparedBatching')
        self.config, self.tokenizer = config, tokenizer

    def batches(self, rows, generation):
        _, conversations = _prepared(rows)
        arrivals = [RowArrival(row['call_id'], 0.0, chat_token_count(self.tokenizer, conversation),
            generation.max_tokens, '', index) for index, (row, conversation) in enumerate(zip(rows, conversations))]
        builder = PendingBatchBuilder(self.config.max_rows, self.config.token_budget)
        output, compatibility, pending_rows = [], None, 0

        def close(reason):
            nonlocal pending_rows
            batch = builder.close()
            output.append(PreparedRequestBatch(tuple(rows[r.payload_ref] for r in batch.rows),
                batch.prompt_tokens, batch.estimated_output_tokens, reason,
                self.config.token_budget > 0 and batch.estimated_total_tokens > self.config.token_budget))
            pending_rows = 0

        for row, arrival in zip(rows, arrivals):
            key = tuple(row[name] for name in ('query_id', 'algorithm_stage', 'model_role'))
            if pending_rows and key != compatibility:
                close('compatibility')
            if builder.would_exceed_token_budget(arrival):
                close('token_budget')
            compatibility = key
            pending_rows += 1
            if builder.add(arrival):
                close('max_rows' if pending_rows == self.config.max_rows else 'token_budget')
        if pending_rows:
            close('ready_exhausted')
        return tuple(output)
