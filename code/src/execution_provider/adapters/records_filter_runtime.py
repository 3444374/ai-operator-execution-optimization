"""Input, receipt validation and result iteration for one ordinary Filter query.

The model owner retains actors, capacity and query setup. This iterator owns
raw input intake, delivery and query-side cleanup over the existing Core.
"""

from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import time
from typing import Any, Iterator

from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import OfferedTask, SessionSpec, State, TaskInfo
from src.semantic_methods.lotus.records import CallLedger, identity

from .records_model_adapter import _encode, _measure
from .records_transport import _check


class _CoreRows(Iterator[dict[str, Any]]):
    def __init__(self, owner, source_rows, expected, query_id, deadline, cancelled,
                 prepare_method=None, session_limits=None, execution_profile=None):
        self.owner, self.source_rows = owner, iter(source_rows)
        self.expected = {row["call_id"]: row for row in expected}
        self.query_id, self.deadline, self.cancelled = query_id, deadline, cancelled
        self.session = owner.core.open(SessionSpec(
            query_id, "lotus-records-filter", "lotus-records-local-model-v1", work_unit="model_calls",
        ), limits=session_limits)
        self._execution_profile = dict(owner.execution_profile if execution_profile is None else execution_profile)
        owner.transport.bind_query(self.session.session_id, prepare_method)
        self.received, self.offered = set(), set()
        self.completed_work = (0, 0, 0)
        self.completed_effective_work = (0, 0, 0)
        self.sequence = 0
        self.pending = None
        self.source_eof = self.finished = self.source_closed = False
        self._failure = None
        self.unconsumed_call_results = []
        self.last_close_report = None
        self.peak_usage = asdict(owner.core.capacity.usage())
        self.timings = owner.transport.timings
        if owner._feedback is not None:
            owner._feedback.stream = self

    def __iter__(self):
        return self

    @property
    def production_core_connected(self):
        return isinstance(self.owner.core, SessionEngine)

    @property
    def execution_profile(self):
        return dict(self._execution_profile)

    def _observe(self):
        if self.owner._feedback is not None:
            with _measure(self.timings, "feedback_progress"):
                self.owner._feedback.observe(self)
        for name, value in asdict(self.owner.core.capacity.usage()).items():
            self.peak_usage[name] = max(value, self.peak_usage[name])

    def _fill(self):
        for _ in range(self.owner.capacity.step_actions):
            _check(self.deadline, self.cancelled)
            if self.source_eof:
                return
            if self.pending is None:
                if self.owner.core.capacity.usage().held_tasks >= self.session.limits.held_tasks:
                    return
                try:
                    raw = dict(next(self.source_rows))
                except StopIteration:
                    self.source_eof = True
                    self.session.seal()
                    return
                occurrence = identity(self.query_id, raw["row_id"], raw["source_position"])
                call_id = occurrence["call_id"]
                if occurrence != self.expected.get(call_id) or call_id in self.offered:
                    raise ValueError("Core source returned an undeclared or duplicate occurrence")
                if any(type(value) is not str for name, value in raw.items()
                       if name not in ("row_id", "source_position")):
                    raise ValueError("Core records payload columns must be text")
                payload = _encode(dict(raw, **occurrence),
                                  self.owner.capacity.item_input_bytes)
                work = WorkDescriptor((StageWork("model", 1, "model_calls"),),
                                      "model", "request-count-not-token-cost-v1")
                self.pending = OfferedTask(
                    self.sequence, payload, 1, self.owner.capacity.item_result_bytes,
                    info=TaskInfo(hashlib.sha256(call_id.encode()).hexdigest(),
                                  occurrence["source_position"], "filter", work),
                )
            offered = self.session.offer((self.pending,))
            if offered.status == "REJECTED":
                raise ValueError("Core rejected raw record: " + offered.reason)
            if not offered.accepted_prefix_count:
                return
            self.offered.add(json.loads(self.pending.payload)["call_id"])
            self.sequence += 1
            self.pending = None
            self._observe()

    def _value(self, payload):
        value = json.loads(payload)
        expected = self.expected.get(value.get("call_id"))
        if expected is None or value["call_id"] in self.received:
            raise ValueError("Core returned an undeclared or duplicate call")
        audit = CallLedger([expected], 1)
        audit.record(value)
        _, input_work, output_work = self.completed_work
        if (value["status"] != "succeeded" or value.get("prompt_tokens") is None
                or value.get("completion_tokens") is None or input_work is None):
            input_work = output_work = None
        else:
            input_work += value["prompt_tokens"]
            output_work += value["completion_tokens"]
        self.completed_work = (len(self.received) + 1, input_work, output_work)
        if self.owner._feedback is not None and self.owner._feedback.uses_computed_work:
            _, computed, cached = self.completed_effective_work
            new_computed, new_cached = value.get("prompt_computed_tokens"), value.get("prompt_cached_tokens")
            valid = (value["status"] == "succeeded" and computed is not None
                and type(new_computed) is int and type(new_cached) is int
                and min(new_computed, new_cached) >= 0
                and new_computed + new_cached == value.get("prompt_tokens"))
            self.completed_effective_work = (len(self.received) + 1,
                computed + new_computed if valid else None, cached + new_cached if valid else None)
        self.received.add(value["call_id"])
        if self.owner._feedback is not None:
            self.owner._feedback.observe(self)
        return value

    def completed_model_work(self):
        """Combine delivered totals with validated retained receipts, without consuming them."""
        completed, input_work, output_work = self.completed_work
        effective_count, computed, cached = self.completed_effective_work
        work = dict(completed_items_total=completed, completed_input_work_total=input_work,
            completed_output_work_total=output_work,
            completed_computed_input_work_total=computed if effective_count == completed else None,
            completed_cached_input_work_total=cached if effective_count == completed else None)
        fields = (('completed_input_work_total', 'prompt_tokens'),
                  ('completed_output_work_total', 'completion_tokens'),
                  ('completed_computed_input_work_total', 'prompt_computed_tokens'),
                  ('completed_cached_input_work_total', 'prompt_cached_tokens'))
        for key, payload in self.owner.transport.failure_receipts():
            if key.session_id != self.session.session_id:
                continue
            value = json.loads(payload)
            if value['call_id'] in self.received:
                continue
            if value['status'] != 'succeeded':
                work.update({target: None for target, _ in fields})
                continue
            work['completed_items_total'] += 1
            computed, cached = value.get('prompt_computed_tokens'), value.get('prompt_cached_tokens')
            if (type(computed) is not int or type(cached) is not int or min(computed, cached) < 0
                    or computed + cached != value.get('prompt_tokens')):
                value['prompt_computed_tokens'] = value['prompt_cached_tokens'] = None
            for target, source in fields:
                item = value.get(source)
                work[target] = (work[target]+item if work[target] is not None
                                and type(item) is int and item >= 0 else None)
        return work

    def __next__(self):
        self.owner._control()
        if self.finished:
            raise StopIteration
        try:
            while True:
                if self._failure is not None:
                    # Receipts from a failed tick retain known usage independently
                    # of Core's rule to discard unpublished success deliveries.
                    for key, payload in self.owner.transport.failure_receipts():
                        self.owner.transport.acknowledge(key)
                        if json.loads(payload).get("call_id") not in self.received:
                            return self._value(payload)
                    raise self._failure
                _check(self.deadline, self.cancelled)
                with _measure(self.timings, "source_offer"):
                    self._fill()
                _check(self.deadline, self.cancelled)
                with _measure(self.timings, "control_advance"):
                    progress = self.session.advance(1)
                self._observe()
                if progress.state in (State.FAILED, State.CANCELLED):
                    self._failure = RuntimeError("Core records execution failed: " + str(progress.error))
                    continue
                if progress.deliveries:
                    delivery = progress.deliveries[0]
                    try:
                        with _measure(self.timings, "delivery_processing"):
                            value = self._value(delivery.result)
                            self.owner.transport.acknowledge(delivery.key)
                    finally:
                        with _measure(self.timings, "lease_release"):
                            self.session.release((delivery.lease_id,))
                    return value
                if progress.state == State.FINISHED:
                    if self.received != set(self.expected):
                        raise ValueError("Core records omitted expected calls")
                    report = self.close()
                    if report["cleanup_errors"]:
                        raise RuntimeError("Core source cleanup is unconfirmed")
                    raise StopIteration
                if not progress.has_immediate_work:
                    wait = min(self.owner.capacity.poll_interval_s,
                               max(0, self.deadline - time.monotonic()))
                    if progress.next_deadline is not None:
                        wait = min(wait, max(0, progress.next_deadline - time.monotonic()))
                    with _measure(self.timings, "control_wait"):
                        self.owner.core.wake.wait(progress.generation, wait)
        except StopIteration:
            raise
        except BaseException as error:
            report = self.close()
            if report["status"] != "closed":
                error.add_note("Core query cleanup remains unconfirmed")
            raise

    def close(self):
        self.owner._control()
        self.finished = True
        self.pending = None
        errors = []
        if not self.source_closed:
            try:
                close = getattr(self.source_rows, "close", None)
                if close is not None:
                    close()
                self.source_closed = True
            except BaseException as error:
                errors.append("source_iterator_close: " + type(error).__name__)
        core_report = self.session.close()
        self.owner.core.reap(self.owner.capacity.step_actions)
        self.collect_receipts()
        usage = self.owner.core.capacity.usage()
        self.last_close_report = dict(
            status="closed" if self.source_closed and not usage.held_tasks and not errors else "unknown",
            cleanup_scope="core_query_input_and_results", source_closed=self.source_closed,
            received_occurrences=len(self.received), expected_occurrences=len(self.expected),
            core_close=asdict(core_report), core_usage=asdict(usage), peak_core_usage=self.peak_usage,
            cleanup_errors=errors, unconsumed_call_results=list(self.unconsumed_call_results),
        )
        if self.timings is not None:
            self.last_close_report["stage_timings"] = self.timings.snapshot()
        if self.owner._feedback is not None:
            self.last_close_report["admission_feedback"] = self.owner._feedback.finish(self)
        self.owner.last_query_report = self.last_close_report
        return self.last_close_report

    def collect_receipts(self):
        for payload in self.owner.transport.take_receipts(self.session.session_id):
            row = json.loads(payload)
            if row["call_id"] not in self.received and row not in self.unconsumed_call_results:
                self.unconsumed_call_results.append(row)
