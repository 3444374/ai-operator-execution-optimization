"""Record-local query iteration over one existing Core and model owner.

Source validation, MethodDriver progress, complete Extracts and result consumption
stay in this runtime. The caller assembles the model/transport owner. No new
scheduler, task ledger or resource allocation is introduced here.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, replace
import json
import time

from src.infrastructure.deadlines import deadline_expired, earliest_deadline, remaining_time
from src.scheduling.core.session_contract import SessionSpec, TaskKey, TaskProfile
from src.semantic_methods.lotus.record_results import _source_binding
from src.semantic_methods.lotus.record_types import RecordOccurrence


def _check(deadline, cancelled=None):
    if cancelled is not None and cancelled():
        raise InterruptedError("Core LOTUS chain cancelled")
    if deadline_expired(deadline):
        raise TimeoutError("Core LOTUS chain deadline expired")


class _RecordLocalRows:
    """Coordinate existing row programs and complete Extracts on one model owner."""

    def __init__(self, owner, records, run, cancelled):
        self.owner, self.source, self.run = owner, iter(records), run
        self.cancelled = cancelled
        self.bindings = {record.row_id: record for record in run.current}
        self.offered = set()
        self.source_eof = self.source_closed = self.finished = self.completed = False
        self.driver = self.session = self.section = self.pending = None
        self.pool = None
        self._phase_source = None
        self._phase_bindings = {}
        self._prepared = {}
        self._extract_results = []
        self._output = None
        self.full_relation_ready_ns = None
        self._output_exhausted = self._root_failed = False
        self._source_errors = []
        self.error = self.last_close_report = None
        self.peak_core_usage = asdict(owner.core.capacity.usage())
        self._event_session = None

    @property
    def calls(self):
        return self.owner.calls

    @property
    def columns(self):
        return self.run.columns

    @property
    def batch_stage_stats(self):
        return [deepcopy(self.run.stats)]

    @property
    def raw_result_batches(self):
        return {}

    @property
    def unassociated_results(self):
        return []

    @property
    def production_core_connected(self):
        return self.owner.production_core_connected

    def __iter__(self):
        return self

    def _close_source(self):
        if self.source_closed:
            return
        try:
            close = getattr(self.source, "close", None)
            if close is not None:
                close()
            self.source_closed = True
        except BaseException as error:
            self._source_errors.append("source_iterator_close: " + type(error).__name__)
            raise

    def _pull_source(self):
        _check(self.owner.deadline, self.cancelled)
        try:
            record = next(self.source)
        except StopIteration:
            self.source_eof = True
            if self.offered != set(self.bindings):
                raise ValueError("record-local source omitted expected occurrences")
            self._close_source()
            _check(self.owner.deadline, self.cancelled)
            raise
        if type(record) is not RecordOccurrence:
            raise ValueError("record-local source changed or duplicated an occurrence")
        # Own this read snapshot once; backpressure may retain the row upstream
        # of MethodDriver for several ticks while the producer reuses its data.
        record = replace(record)
        if (record.row_id not in self.bindings
                or record.row_id in self.offered
                or _source_binding(record) != _source_binding(self.bindings[record.row_id])):
            raise ValueError("record-local source changed or duplicated an occurrence")
        self.offered.add(record.row_id)
        self.bindings[record.row_id] = None
        return record

    def _begin_phase(self):
        from src.semantic_methods.budget import MethodBudgetPool
        from src.semantic_methods.driver import MethodDriver
        from src.semantic_methods.lotus.record_continuation import PreparedCallProgram
        from .lotus_record_chain_fifo import LotusChainFIFO

        if self.run.position == len(self.run.chain.stages):
            result = self.run.result()
            self._output = iter(result["records"])
            return
        self._using_source = not self.source_eof
        self._phase_eof = False
        self._extract_results = []
        if self.run.method.stage.kind == "extract":
            # Full-relation preparation never treats the expected declaration
            # alone as proof that the actual source has completed.
            while not self.source_eof:
                try:
                    self._pull_source()
                except StopIteration:
                    break
            self._using_source = False
            stage = self.run.method.stage
            prepared = self.run.prepare()
            self._stage_ids = (stage.stage_id,)
            self.section = None
            if not prepared:
                self.run.complete_relation(source_exhausted=True)
                self._history(None, 0)
                return
            self._prepared = {row["row_id"]: row for row in prepared}
            program = PreparedCallProgram(stage, max_result_bytes=self.owner.capacity.item_result_bytes)
            stages = (stage,)
            self._phase_source = iter(self.run.current)
        else:
            self.section = self.run.delegate_record_local()
            self._phase_bindings = {row.row_id: _source_binding(row) for row in self.section.records}
            stages = self.section.chain.stages
            self._stage_ids = tuple(stage.stage_id for stage in stages)
            self._phase_source = self.source if self._using_source else iter(self.section.records)
        self._phase_count = 0
        self.session = self.owner.core.open(SessionSpec(self.run.chain.query_id,
            "chain-section-" + str(self.run.position), "lotus-record-prepared-v1",
            work_unit="model_calls" if self.owner.token_budget is None else "tokens",
            task_profiles=tuple(TaskProfile(stage.stage_id, "prepared-chain") for stage in stages)))
        self.owner.transport.bind_query(self.session.session_id, None)
        if self.owner._supply_feedback is not None:
            self.owner._supply_feedback.bind(self)
        limits = self.owner.method_limits
        capacity = self.owner.capacity.method_capacity(self.owner.resources, limits)
        self.pool = MethodBudgetPool(capacity)
        budget = self.pool.allocate(capacity)
        ready_options = dict(self.owner._ready_options)
        if self.owner._supply_feedback is not None:
            ready_options["on_ready_event"] = self.owner._supply_feedback.ready
        self.driver = (LotusChainFIFO(self.session, self.section.chain, limits, budget,
                                     token_budget=self.owner.token_budget,
                                     completion_batch_rows=self.owner.progression_batch_rows,
                                     **ready_options) if self.section is not None
                       else MethodDriver(self.session, program, limits, budget,
                            on_ready_event=None if self.owner._supply_feedback is None
                            else self.owner._supply_feedback.ready))

    def _fill(self):
        from src.semantic_methods.driver import RowIdentity

        if self._phase_eof:
            return False
        if self.pending is None:
            try:
                self.pending = self._pull_source() if self._using_source else next(self._phase_source)
            except StopIteration:
                self._phase_eof = True
                _check(self.owner.deadline, self.cancelled)
                self.driver.end_input()
                return False
        row = self.pending
        if self.section is not None:
            accepted = self.driver.offer_record(row,
                parent_call_ids=self.section.initial_parents[(row.row_id, row.source_position)],
                columns=self.section.columns, allow_nulls=self.section.allow_nulls)
        else:
            accepted = self.driver.offer_row(RowIdentity(row.source_position, row.row_id),
                self.driver.method.input(self._prepared[row.row_id],
                    parent_call_ids=self.run.parents[(row.row_id, row.source_position)]))
        if accepted:
            self._phase_count += 1
            self.pending = None
        return accepted

    def _history(self, session_id, inputs):
        self.owner.phase_history.append(dict(phase_stage_ids=list(self._stage_ids),
            source_exhausted=self.source_eof, source_closed=self.source_closed,
            session_id=session_id, input_occurrences=inputs,
            output_occurrences=len(self.run.current), verified_success=True,
            core_usage=asdict(self.owner.core.capacity.usage()),
            transition="record_local_adoption" if self.section is not None else "complete_extract_relation"))

    def _finish_phase(self):
        if (not self.source_eof or not self.source_closed or self.pending is not None
                or any(asdict(self.owner.core.capacity.usage()).values())
                or self.owner.transport.retained_refs() or self.pool.used != (0, 0)):
            raise RuntimeError("record-local phase still owns source, result or transport work")
        retired = self.session.close()
        if retired.status != "CLOSED" or any(asdict(retired.usage).values()):
            raise RuntimeError("record-local session did not retire completely")
        if self.section is not None:
            self.run.adopt_record_local(self.section, source_exhausted=True)
        else:
            self.run.complete(self._extract_results)
            self.run.complete_relation(source_exhausted=True)
        self._history(self.session.session_id, self._phase_count)
        if self.owner._supply_feedback is not None:
            self.owner._supply_feedback.end_phase()
        self.driver.close()
        self.driver, self.section = None, None
        self._prepared, self._phase_bindings = {}, {}

    @property
    def can_prepare(self):
        return (self.driver is not None and self.section is not None and not self._phase_eof
                and self.driver.active_rows < self.pool.capacity.runs)

    def _consume_method_results(self, bound):
        consumed = 0
        for result in self.driver.results(bound):
            try:
                value = json.loads(result.value)
                if self.section is not None:
                    self.section.accept(dict(value, batch_id=str(result.row.sequence),
                        inputs=[self._phase_bindings[result.row.call_id]]))
                else:
                    self._extract_results.append(value)
                consumed += 1
            finally:
                self.driver.release_result(result.row)
        return consumed

    def _record_core_usage(self):
        for key, value in asdict(self.owner.core.capacity.usage()).items():
            self.peak_core_usage[key] = max(self.peak_core_usage[key], value)

    def _tick_event(self):
        backend = self.owner.transport.backend
        if not callable(getattr(backend, 'progress', None)):
            raise ValueError('event preparation requires backend progress notifications')
        if self._event_session != self.session.session_id:
            self._event_session = self.session.session_id
            self._backend_seen = -1
            self._core_due_at = None
            self._core_results_pending = self._method_results_pending = False
            self._consumer_due_at = 0.
            self._last_compute = 0
            self._last_dispatch_token = None
            self._end_input_pending = True
        # Capture the shared wake before reading backend-only notification state.
        # Self offers change the shared wake, never backend.progress().
        generation = self.owner.core.wake.generation
        notification = backend.progress()
        now = time.monotonic()
        organizer = self.owner._token_organizer
        feedback = self.owner._supply_feedback
        token = organizer.dispatch_token(self.can_prepare, self._last_compute)
        deadline_due = self._core_due_at is not None and now >= self._core_due_at
        service = (notification.generation != self._backend_seen
            or notification.pending and not notification.contended
            or self._core_results_pending or self._end_input_pending or deadline_due
            or token is not None and token != self._last_dispatch_token
                and not self.session._pending_members and now >= self.session._retry_at)
        bound = min(self.owner.capacity.step_actions, self.session.limits.held_tasks)
        consumed = 0
        if service:
            # Between Core advances, offers only add reservations. Observe the
            # end of that monotone stretch before progress can release them.
            self._record_core_usage()
            self._last_dispatch_token = token
            self._end_input_pending = False
            progress = self.driver.advance(bound, offer_ready_frontier=True)
            # A notification racing with advance remains newer than this token.
            self._backend_seen = notification.generation
            self._core_due_at = progress.next_deadline
            records = self.session._records()
            self._core_results_pending = any(r.phase == 'READY' and not r.preparation_held for r in records)
            self._last_compute = sum(r.compute for r in records)
            organizer.refresh_prefix(tuple(r for r in records if r.phase == 'QUEUED'), self._last_compute)
            consumed = self._consume_method_results(bound)
            self._method_results_pending = any(r.run.final is not None for r in self.driver._rows.values())
            self._consumer_due_at = (time.monotonic()+self.owner.capacity.poll_interval_s
                if self._method_results_pending and not consumed else 0.)
            if self._phase_eof and not self.driver.active_rows and not self.driver.finished:
                self._end_input_pending = True
        elif self._method_results_pending and now >= self._consumer_due_at:
            consumed = self._consume_method_results(bound)
            self._method_results_pending = any(r.run.final is not None for r in self.driver._rows.values())
            self._consumer_due_at = (time.monotonic()+self.owner.capacity.poll_interval_s
                if self._method_results_pending and not consumed else 0.)
            if self._phase_eof and not self.driver.active_rows:
                self._end_input_pending = True
        prepared = False
        if self.can_prepare:
            before = self._phase_count, self._phase_eof
            sequence = self.driver._task_sequence
            self._fill()
            self.driver._submit(self.session.limits.held_tasks)
            organizer.append_ready(self.owner.core.capacity.records[TaskKey(self.session.session_id, i)]
                for i in range(sequence, self.driver._task_sequence))
            prepared = before != (self._phase_count, self._phase_eof)
            if before[1] != self._phase_eof:
                self._end_input_pending = True
        if service:
            organizer.usage()
        if feedback is not None:
            # Observe after result/continuation progress, as in the ordinary
            # loop; an unfinished continuation is not exhausted source work.
            previous_window = organizer.request_window
            feedback.update(self)
            if organizer.request_window != previous_window:
                # A permit change can enable queued work without a backend event.
                organizer.refresh_prefix(tuple(r for r in self.session._records()
                    if r.phase == 'QUEUED'), self._last_compute)
        if self.driver.finished:
            self._finish_phase()
        elif not prepared and not consumed:
            current = backend.progress()
            ready = (current.generation != self._backend_seen
                     or current.pending and not current.contended
                     or self._core_results_pending or self._end_input_pending)
            token = organizer.dispatch_token(self.can_prepare, self._last_compute)
            ready |= (token is not None and token != self._last_dispatch_token
                      and not self.session._pending_members and time.monotonic() >= self.session._retry_at)
            if not ready:
                deadlines = [self.owner.deadline]
                if self._core_due_at is not None:
                    deadlines.append(self._core_due_at)
                if self._method_results_pending:
                    deadlines.append(self._consumer_due_at)
                if feedback is not None and feedback.controller is not None:
                    deadlines.append(feedback.next_poll)
                # Keep the existing owner cancellation/consumer wait bound.
                deadlines.append(time.monotonic()+self.owner.capacity.poll_interval_s)
                self.owner.core.wake.wait(generation, max(0., earliest_deadline(*deadlines)-time.monotonic()))

    def _tick(self):
        _check(self.owner.deadline, self.cancelled)
        if self.driver is None:
            self._begin_phase()
            return
        if getattr(self.owner, '_event_driven_preparation', False) and self.section is not None:
            self._tick_event()
            return
        interleaved = getattr(self.owner, '_interleaved_preparation', False) and self.section is not None
        if not interleaved:
            if self.owner._async_token_batches:
                while not self._phase_eof and self.driver.active_rows < self.pool.capacity.runs:
                    if not self._fill():
                        break  # Storage can bind before the Method row grant.
            else:
                self._fill()
        _check(self.owner.deadline, self.cancelled)
        bound = min(self.owner.capacity.step_actions, self.session.limits.held_tasks)
        progress = self.driver.advance(bound, offer_ready_frontier=True) if self.owner._async_token_batches else self.driver.advance(bound)
        self._consume_method_results(bound)
        if interleaved and self.can_prepare:
            # A complete record is the existing nonpreemptible preparation unit.
            # Revisit dispatch/results before the next unit; offer is not an RPC.
            before = self._phase_count, self._phase_eof
            self._fill()
            offered = self.driver._submit(self.session.limits.held_tasks)
            if offered or before != (self._phase_count, self._phase_eof):
                progress = replace(progress, has_immediate_work=True)
        self._record_core_usage()
        if self.owner._async_token_batches:
            self.owner._token_organizer.usage()
            if self.owner._supply_feedback is not None:
                self.owner._supply_feedback.update(self)
        if self.driver.finished:
            self._finish_phase()
        elif not progress.has_immediate_work:
            self.owner.core.wake.wait(progress.generation,
                remaining_time(self.owner.deadline, self.owner.capacity.poll_interval_s))

    def __next__(self):
        self.owner._control()
        if self.finished:
            raise StopIteration
        try:
            while True:
                _check(self.owner.deadline, self.cancelled)
                if self._output is not None:
                    try:
                        return next(self._output)
                    except StopIteration:
                        self._output_exhausted = True
                        self.full_relation_ready_ns = time.monotonic_ns()
                        report = self.owner.finish_query()
                        if report["status"] != "closed":
                            error = RuntimeError("record-local query cleanup remains unconfirmed")
                            error.chain_cleanup_report = report
                            raise error
                        self.completed = report["query_completed"] = True
                        raise
                self._tick()
        except StopIteration:
            raise
        except BaseException as error:
            self.error = error
            try:
                report = self.owner.close()
                if report["status"] != "closed":
                    error.add_note("record-local query cleanup remains unconfirmed")
            except BaseException as cleanup:
                error.add_note("record-local close also failed: " + type(cleanup).__name__)
            error.chain_calls = self.calls
            error.chain_stage_stats = deepcopy(self.run.stats)
            raise

    def _close_query(self):
        if getattr(self.owner, '_event_driven_preparation', False):
            self._record_core_usage()
        self.finished, self.pending = True, None
        if self.driver is not None:
            self.driver.close()
        if not self._output_exhausted and self.error is None:
            self.error = InterruptedError("record-local consumer closed before result EOF")
        try:
            self._close_source()
        except BaseException:
            pass  # The exact failure stays in _source_errors and the close report.
        return list(self._source_errors)

    def close(self, *, timeout=None):
        return self.owner.close(timeout=timeout)

    def release_query_payloads(self, report):
        """Drop this runtime's query data after its owner authorizes transfer."""
        self.source = self.run = self.section = self.error = None
        self.driver = self.session = self.pending = self.pool = None
        self._output = self._phase_source = self.cancelled = self.owner = None
        self.bindings, self.offered = {}, set()
        self._phase_bindings, self._prepared, self._extract_results = {}, {}, []
        self.last_close_report = report

    def query_payloads_retired(self):
        """Read the actual retained data, without a second retirement marker."""
        return all(value is None for value in (
            self.source, self.run, self.section, self.error, self.driver, self.session,
            self.pending, self.pool, self._output, self._phase_source, self.owner,
        )) and not (
            self.bindings or self._phase_bindings or self._prepared or self._extract_results
            or 'calls' in (self.last_close_report or {})
        )
