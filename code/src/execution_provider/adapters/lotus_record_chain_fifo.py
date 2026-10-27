"""Explicit FIFO continuation consumer over the original production Core driver."""

from src.semantic_methods.driver import MethodDriver, RowIdentity
from src.semantic_methods.lotus.record_continuation import LotusChainProgram


class LotusChainFIFO(MethodDriver):
    """The source and result consumer keep the existing offer/release protocol.

    This is a separate opt-in entry. LotusRecordChain.run_batch stays fused.
    Core owns request capacity; MethodBudget owns ready/state/final envelopes.
    The external owner advances/reaps Engine and closes the same model process.
    """

    def __init__(self, session, chain, limits, budget, *, token_budget=None):
        if limits.stages < len(chain.stages):
            raise ValueError("method stage reservation must cover the finite chain")
        for stage in chain.stages:
            session.spec.resolve(stage.stage_id)
        if token_budget is not None:
            from .lotus_chain_token_budget import ChainTokenBudget
            if type(token_budget) is not ChainTokenBudget:
                raise TypeError("token_budget must be ChainTokenBudget or None")
            if (session.spec.work_unit != "tokens" or session.limits.active_work != token_budget.active_tokens
                    or set(token_budget.expected_output) != {s.stage_id for s in chain.stages}):
                raise ValueError("chain stages and Core W must match the token configuration")
        self.program = LotusChainProgram(chain, max_result_bytes=session.limits.item_result_bytes,
            estimate_tokens=None if token_budget is None else token_budget.estimate)
        super().__init__(session, self.program, limits, budget, ready_order="fifo",
                         describe_work=None if token_budget is None else token_budget.describe_work)

    def offer_record(self, record, *, parent_call_ids=(), columns=None, allow_nulls=False):
        return self.offer_row(RowIdentity(record.source_position, record.row_id),
            self.program.input(record, parent_call_ids=parent_call_ids,
                               columns=columns, allow_nulls=allow_nulls))
