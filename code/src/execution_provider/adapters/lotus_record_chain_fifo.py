"""Explicit FIFO continuation consumer over the original production Core driver."""

from src.semantic_methods.driver import MethodDriver, RowIdentity
from src.semantic_methods.budget import row_reservation
from src.semantic_methods.lotus.record_continuation import LotusChainProgram


class _CompletionBatch:
    def __init__(self):
        self.members, self.released = set(), set()
        self.sealed = False


class LotusChainFIFO(MethodDriver):
    """The source and result consumer keep the existing offer/release protocol.

    This is a separate opt-in entry. LotusRecordChain.run_batch stays fused.
    Core owns request capacity; MethodBudget owns ready/state/final envelopes.
    The external owner advances/reaps Engine and closes the same model process.
    """

    def __init__(self, session, chain, limits, budget, *, token_budget=None,
                 ready_order="fifo", successor_first=None, clock=None, on_ready_event=None,
                 completion_batch_rows=None):
        if completion_batch_rows is not None:
            retained = min(budget.capacity.runs, budget.capacity.bytes // row_reservation(limits))
            if type(completion_batch_rows) is not int or not 1 <= completion_batch_rows <= retained:
                raise ValueError("completion batch must fit the retained row and byte grant")
            if token_budget is not None or ready_order != "fifo":
                raise ValueError("batch-stage progression currently requires count admission and FIFO")
        self.completion_batch_rows = completion_batch_rows
        self._completion_groups, self._completion_open = {}, None
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
        super().__init__(session, self.program, limits, budget, ready_order=ready_order,
                         describe_work=None if token_budget is None else token_budget.describe_work,
                         successor_first=successor_first, clock=clock, on_ready_event=on_ready_event)

    def offer_row(self, row, value):
        accepted = super().offer_row(row, value)
        if accepted and self.completion_batch_rows is not None:
            if self._completion_open is None:
                self._completion_open = _CompletionBatch()
            batch = self._completion_open
            batch.members.add(row.sequence)
            self._completion_groups[row.sequence] = batch
            if len(batch.members) == self.completion_batch_rows:
                self._seal_completion_batch()
        return accepted

    def _seal_completion_batch(self):
        batch, self._completion_open = self._completion_open, None
        if batch is not None:
            batch.sealed = True
            self._release_completion_batch(batch)

    def _release_completion_batch(self, batch):
        if batch.sealed and batch.members == batch.released:
            for sequence in batch.members:
                del self._completion_groups[sequence]

    def end_input(self):
        super().end_input()
        self._seal_completion_batch()

    def _candidates(self, maximum):
        for sequence, reason in super()._candidates(maximum):
            row = self._rows[sequence]
            if self.completion_batch_rows is not None and row.stages:
                batch = self._completion_groups[sequence]
                if not batch.sealed:
                    continue
                # A released/filtered member is final. Other peers must have
                # consumed this stage's result, not merely accepted its task.
                peers = (self._rows[s] for s in batch.members - batch.released)
                if any(peer.run.final is None and (peer.stages < row.stages or
                       peer.stages == row.stages and peer.run.pending is None) for peer in peers):
                    continue
            yield sequence, reason

    def release_result(self, row):
        batch = self._completion_groups.get(row.sequence)
        super().release_result(row)
        if batch is not None:
            batch.released.add(row.sequence)
            self._release_completion_batch(batch)

    def close(self):
        try:
            return super().close()
        finally:
            self._completion_groups.clear()
            self._completion_open = None

    def offer_record(self, record, *, parent_call_ids=(), columns=None, allow_nulls=False):
        return self.offer_row(RowIdentity(record.source_position, record.row_id),
            self.program.input(record, parent_call_ids=parent_call_ids,
                               columns=columns, allow_nulls=allow_nulls))
