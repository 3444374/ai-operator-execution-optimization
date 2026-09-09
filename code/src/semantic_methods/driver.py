"""Bounded sequential methods over many rows, using one exclusively owned new session."""

from collections import OrderedDict
from dataclasses import dataclass, replace

from ..planning.work import StageWork, WorkDescriptor
from ..scheduling.core.session_contract import State, TaskInfo, TaskKey
from .budget import MethodBudget, row_reservation
from .continuation import Method, MethodLimits, MethodRun


@dataclass(frozen=True)
class RowIdentity:
    sequence: int
    call_id: str

    def __post_init__(self):
        if type(self.sequence) is not int or not 0 <= self.sequence < 2**64:
            raise ValueError("invalid row sequence")
        if type(self.call_id) is not str or not 0 < len(self.call_id.encode()) <= 256:
            raise ValueError("invalid call identity")


@dataclass(frozen=True)
class MethodResult:
    row: RowIdentity
    value: bytes


@dataclass
class _Row:
    identity: RowIdentity
    run: MethodRun
    delivered: bool = False
    stages: int = 0


class MethodDriver:
    """One owner thread; the service owner advances Engine for registered Jobs.

    offer_row never pulls from a source. False means leave the row upstream. Outputs
    remain charged until release_result; stop consumers before closing this driver.
    A callback or task failure terminates this method flow, not other Jobs.
    """

    def __init__(
        self,
        session,
        method: Method,
        limits: MethodLimits,
        budget: MethodBudget,
        *,
        describe_work=None,
    ):
        self._reservation = row_reservation(limits)
        if budget.capacity.bytes < self._reservation:
            raise ValueError("method budget cannot hold one stage transition")
        if session.state != State.OPEN:
            raise ValueError("method driver requires a new open session")
        budget.claim()
        self.session, self.method, self.limits, self.budget = (
            session,
            method,
            limits,
            budget,
        )
        self._rows = OrderedDict()
        self._tasks = {}
        self._row_sequence = -1
        self._task_sequence = 0
        self._eof = self._sealed = self._closed = False
        self.error = None
        self._describe_work = describe_work

    @property
    def active_rows(self):
        return len(self._rows)

    @property
    def finished(self):
        return self._eof and not self._rows and self.session.state == State.FINISHED

    def _check_open(self):
        if self._closed:
            raise RuntimeError("method driver closed")

    def offer_row(self, row: RowIdentity, value: bytes) -> bool:
        self._check_open()
        if self._eof:
            raise RuntimeError("method source already ended")
        if type(row) is not RowIdentity or row.sequence <= self._row_sequence:
            raise ValueError("row sequence must increase")
        if not self.budget.reserve(self._reservation):
            return False
        try:
            run = MethodRun(self.method, value, self.limits)
            self._rows[row.sequence] = _Row(row, run)
        except Exception as error:
            self.budget.release(self._reservation)
            self._abort(error)
            raise
        self._row_sequence = row.sequence
        return True

    def end_input(self):
        self._check_open()
        self._eof = True

    def _submit(self, maximum):
        actions = 0
        # Rotate visited rows to keep a waiting row ahead of newly generated stages.
        for sequence in tuple(self._rows):
            if actions >= maximum:
                break
            row = self._rows[sequence]
            request = row.run.pending
            if request is None:
                continue
            # Work estimates come from the method adapter, never from parsing model payloads.
            stage = str(row.stages)
            work = (
                self._describe_work(row.identity, row.stages, request)
                if self._describe_work
                else WorkDescriptor(
                    (
                        StageWork(
                            stage, request.estimated_work, self.session.spec.work_unit
                        ),
                    ),
                    stage,
                    "method-declared",
                )
            )
            offered = replace(
                request.offered(self._task_sequence),
                info=TaskInfo(
                    row.identity.call_id,
                    row.identity.sequence,
                    work.primary_stage,
                    work,
                ),
            )
            outcome = self.session.offer((offered,))
            if not outcome.accepted_prefix_count:
                if outcome.status != "BACKPRESSURE":
                    raise RuntimeError(f"method task rejected: {outcome.reason}")
                break
            key = TaskKey(self.session.session_id, self._task_sequence)
            row.run.accepted(key)
            row.stages += 1
            self._tasks[key] = sequence
            self._task_sequence += 1
            self._rows.move_to_end(sequence)
            actions += 1
        return actions

    def advance(self, maximum: int = 1):
        """Bound submissions/deliveries; return session wake/deadline information unchanged.

        Release each core delivery before attempting a subsequent stage. The reserved row
        envelope already covers its new continuation, so one core result slot is sufficient.
        """
        self._check_open()
        if (
            type(maximum) is not int
            or not 0 < maximum <= self.session.limits.held_tasks
        ):
            raise ValueError("invalid method advance bound")
        try:
            progress = self.session.advance(maximum)
            # Even a later callback failure must release every delivery in this batch.
            try:
                for delivery in progress.deliveries:
                    if delivery.status != "completed":
                        raise RuntimeError("method task failed")
                    sequence = self._tasks.pop(delivery.key)
                    self._rows[sequence].run.complete(delivery.key, delivery.result)
            finally:
                self.session.release(tuple(d.lease_id for d in progress.deliveries))
            if progress.state in (State.FAILED, State.CANCELLED):
                raise RuntimeError(progress.error or "method session terminated")
            submitted = self._submit(maximum)
            sealed = False
            if (
                self._eof
                and not self._sealed
                and all(r.run.final is not None for r in self._rows.values())
            ):
                self.session.seal()
                self._sealed = sealed = True
            return replace(
                progress,
                has_immediate_work=(
                    progress.has_immediate_work
                    or bool(submitted or progress.deliveries or sealed)
                ),
            )
        except Exception as error:
            self._abort(error)
            raise

    def results(self, maximum: int = 1) -> tuple[MethodResult, ...]:
        self._check_open()
        if type(maximum) is not int or maximum <= 0:
            raise ValueError("invalid result bound")
        output = []
        for row in self._rows.values():
            if row.run.final is not None and not row.delivered:
                output.append(MethodResult(row.identity, row.run.final.value))
                row.delivered = True
                if len(output) == maximum:
                    break
        return tuple(output)

    def release_result(self, row: RowIdentity):
        self._check_open()
        held = self._rows.get(row.sequence)
        if held is None or held.identity != row or not held.delivered:
            raise ValueError("result is not held by this consumer")
        del self._rows[row.sequence]
        self.budget.release(self._reservation)

    def _abort(self, error):
        # Do not retain arbitrary callback payloads through exception messages.
        self.error = type(error).__name__[:128]
        self.close()

    def close(self):
        """Stop local consumers first; unresolved remote work stays in the Engine ledger."""
        if self._closed:
            return
        self._closed = True
        try:
            return self.session.close_consumer(clean=self.finished)
        finally:
            self._tasks.clear()
            while self._rows:
                self._rows.popitem()
                self.budget.release(self._reservation)
            self.budget.close()
