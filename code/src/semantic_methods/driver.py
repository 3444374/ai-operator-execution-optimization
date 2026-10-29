"""Bounded sequential methods over many rows, using one exclusively owned new session."""

from collections import OrderedDict, deque
from dataclasses import dataclass, replace
import math
import time

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


@dataclass(frozen=True)
class SuccessorFirst:
    """Explicit service/age settings; neither value is inferred from a workload."""

    max_burst: int
    max_wait_s: float

    def __post_init__(self):
        if type(self.max_burst) is not int or self.max_burst <= 0:
            raise ValueError("successor burst must be a positive integer")
        if (type(self.max_wait_s) not in (int, float) or not math.isfinite(self.max_wait_s)
                or self.max_wait_s <= 0):
            raise ValueError("ready aging must be finite and positive")


@dataclass(frozen=True)
class ReadyEvent:
    """Local method-ready and Core-offer clocks; never native engine admission."""

    row: RowIdentity
    stage_index: int
    action: str
    ready_at: float
    at: float
    reason: str | None = None
    key: TaskKey | None = None


@dataclass
class _Row:
    identity: RowIdentity
    run: MethodRun
    delivered: bool = False
    stages: int = 0
    ready_at: float | None = None


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
        ready_order="round_robin",
        successor_first: SuccessorFirst | None = None,
        clock=None,
        on_ready_event=None,
    ):
        if ready_order not in ("round_robin", "fifo", "successor_first"):
            raise ValueError("unknown continuation ready order")
        if ((ready_order == "successor_first") != (successor_first is not None)
                or successor_first is not None and type(successor_first) is not SuccessorFirst):
            raise ValueError("successor_first order requires its explicit settings")
        if clock is not None and not callable(clock):
            raise TypeError("ready clock must be callable")
        if on_ready_event is not None and not callable(on_ready_event):
            raise TypeError("ready observer must be callable")
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
        self._ready = None if ready_order == "round_robin" else deque()
        self._ready_order, self._priority = ready_order, successor_first
        self._successor_burst = 0
        self._clock = time.monotonic if clock is None else clock
        self._on_ready_event = on_ready_event
        self.ready_observer_error = None

    def _ready_event(self, row, action, reason=None, key=None):
        if self._on_ready_event is not None:
            try:
                at = self._clock()
                if row.ready_at is None:
                    row.ready_at = at
                event = ReadyEvent(row.identity, row.stages, action, row.ready_at,
                                   at, reason, key)
                self._on_ready_event(event)
            except Exception as error:
                # Optional diagnostics cannot fail the method or its required audit.
                self.ready_observer_error = type(error).__name__
                self._on_ready_event = None

    def _mark_ready(self, sequence):
        row = self._rows[sequence]
        # Aging needs a real policy timestamp; diagnostic clocks are optional.
        row.ready_at = self._clock() if self._priority is not None else None
        if self._ready is not None:
            self._ready.append(sequence)
        self._ready_event(row, "method_ready")

    def _candidates(self, maximum):
        if self._priority is None:
            for sequence in tuple(self._rows) if self._ready is None else tuple(self._ready):
                yield sequence, self._ready_order
            return
        for _ in range(maximum):
            if not self._ready:
                return
            oldest = self._ready[0]
            if self._clock() - self._rows[oldest].ready_at >= self._priority.max_wait_s:
                yield oldest, "aged"
                continue
            source, successor = None, None
            for sequence in self._ready:
                if self._rows[sequence].stages == 0:
                    if source is None:
                        source = sequence
                elif successor is None:
                    successor = sequence
                if source is not None and successor is not None:
                    break
            if source is None:
                yield successor, "only_successor"
            elif successor is None:
                yield source, "only_source"
            elif self._successor_burst >= self._priority.max_burst:
                yield source, "source_turn"
            else:
                yield successor, "successor_first"

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
        if run.pending is not None:
            try:
                self._mark_ready(row.sequence)
            except Exception as error:
                self._abort(error)
                raise
        return True

    def end_input(self):
        self._check_open()
        self._eof = True

    def _submit(self, maximum):
        actions = 0
        # Rotate visited rows to keep a waiting row ahead of newly generated stages.
        for sequence, reason in self._candidates(maximum):
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
                self._ready_event(row, "Core_backpressure", reason)
                if outcome.status != "BACKPRESSURE":
                    raise RuntimeError(f"method task rejected: {outcome.reason}")
                break
            key = TaskKey(self.session.session_id, self._task_sequence)
            row.run.accepted(key)
            self._ready_event(row, "Core_offer_accepted", reason, key)
            if self._priority is not None:
                self._successor_burst = (min(self._priority.max_burst, self._successor_burst + 1)
                                         if row.stages else 0)
            row.stages += 1
            row.ready_at = None
            self._tasks[key] = sequence
            self._task_sequence += 1
            self._rows.move_to_end(sequence)
            if self._ready is not None:
                self._ready.remove(sequence)
            actions += 1
        return actions

    def advance(self, maximum: int = 1, *, offer_ready_frontier=False):
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
        if type(offer_ready_frontier) is not bool:
            raise TypeError('offer_ready_frontier must be boolean')
        try:
            progress = self.session.advance(maximum)
            # Even a later callback failure must release every delivery in this batch.
            try:
                for delivery in progress.deliveries:
                    if delivery.status != "completed":
                        raise RuntimeError("method task failed")
                    sequence = self._tasks.pop(delivery.key)
                    self._rows[sequence].run.complete(delivery.key, delivery.result)
                    if self._rows[sequence].run.pending is not None:
                        self._mark_ready(sequence)
            finally:
                self.session.release(tuple(d.lease_id for d in progress.deliveries))
            if progress.state in (State.FAILED, State.CANCELLED):
                raise RuntimeError(progress.error or "method session terminated")
            submitted = self._submit(self.session.limits.held_tasks if offer_ready_frontier else maximum)
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
            if self._ready is not None:
                self._ready.clear()
            while self._rows:
                self._rows.popitem()
                self.budget.release(self._reservation)
            self.budget.close()
