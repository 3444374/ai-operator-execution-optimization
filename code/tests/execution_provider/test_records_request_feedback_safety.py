"""Ordinary feedback safety with accepted Core tasks and retained CPU receipts.

Native inputs and receipt retention are explicit fixtures. The actual observation,
permit, policy and Core ownership code run without a model or a real transport.
"""
from dataclasses import asdict, replace
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import records_request_feedback as wiring
from src.execution_provider.adapters.lotus_records_core import _CoreRows
from src.execution_provider.adapters.records_transport import _RecordTransport
from src.scheduling.core.models import AdmissionObservation
from src.scheduling.core.session_contract import SessionSpec, TaskKey, Uncertain
from src.scheduling.runtime.observations import ServiceMetricsSnapshot
from src.scheduling.submission_control.feedback_options import ResourceRequestConfig
from src.semantic_methods.lotus.records import identity
from tests.scheduling.test_incremental_session import setup, task
from tests.scheduling.test_resource_request_controller import ResourceTrace


class OrdinarySafetyFixture:
    def __init__(self, status):
        trace = ResourceTrace(maximum=16, options=ResourceRequestConfig(poll_interval_s=.01))
        trace.until(lambda report, _: report['window'] == 16
            and report['verified_performance_window'] == 16
            and not report['active_unverified_trial'], count=lambda c: 40*c)
        self.controller = trace.controller
        self.now, self.sequence = trace.now, trace.sequence
        self.native_times = {}
        size = 4096
        self.core, self.session, self.backend, _ = setup(held_tasks=64, input_bytes=64*size,
            result_bytes=64*size, item_input_bytes=size, item_result_bytes=size,
            active_requests=16, active_work=16, offer_tasks=64, step_actions=64, wait_timeout_s=1000.)
        while self.session.session_id < 7:
            self.session.close()
            self.session = self.core.open(SessionSpec('fixture-query', 'fixture-flow', 'CPU-only'))
        occurrences = [identity('fixture-query', 'fixture-row-'+str(i), i) for i in range(32)]
        offered = tuple(task(i, payload=json.dumps(dict(value, reviewText='CPU fixture')).encode(),
            max_result_bytes=size) for i, value in enumerate(occurrences))
        assert self.session.offer(offered).accepted_prefix_count == len(offered)
        self.core.policies.admission.limit = 16
        self.session.advance(1)
        self.key = TaskKey(self.session.session_id, 0)
        assert self.key in self.backend.pending
        record = self.core.capacity.records[self.key]
        occurrence = occurrences[self.key.sequence]
        assert all(json.loads(record.task.payload)[k] == v for k, v in occurrence.items())
        receipt = dict(occurrence, status='succeeded', prompt_tokens=100, completion_tokens=4,
            prompt_computed_tokens=80, prompt_cached_tokens=20)
        if status in ('unknown', 'failed'):
            receipt.update(status=status, prompt_tokens=None, completion_tokens=None,
                prompt_computed_tokens=None, prompt_cached_tokens=None, submission_confirmed=False,
                error_type='CPUFixtureUnknownOutcome')
        elif status == 'optional_prompt_unknown':
            # The validator permits optional usage. This broader receipt fixture
            # does not assert that the current model emits such successful rows.
            receipt['prompt_tokens'] = None
        _RecordTransport._audit_result(None, occurrence, receipt)
        self.retained = ((self.key, json.dumps(receipt).encode()),)
        ready = dict(engine_uid='CPU-engine', batch_mode='independent_final_results', closed=False,
            resource_owner=dict(schema='semloom.model_resource_owner.v1', engine_uid='CPU-engine',
                request_memory_owner='vllm', native_feedback_clock='private_child_and_model_actor.monotonic'))
        owner = SimpleNamespace(core=self.core, _actors=SimpleNamespace(model_ready=ready,
            limits=SimpleNamespace(max_inflight=16, result_slots=64, result_bytes=64*size,
                                   item_result_bytes=size)),
            resources=SimpleNamespace(max_active_batches=16),
            transport=SimpleNamespace(failure_receipts=lambda: self.retained))
        self.stream = _CoreRows.__new__(_CoreRows)
        self.stream.owner, self.stream.session = owner, self.session
        self.stream.received = set()
        self.stream.expected = {value['call_id']: value for value in occurrences}
        self.stream.offered = set(self.stream.expected)
        self.stream.completed_work = (trace.completed, trace.input, trace.output)
        self.stream.completed_effective_work = (trace.completed, trace.computed, trace.cached)
        self.stream.source_eof, self.stream.pending = False, None
        self.feedback = wiring.CoreResourceRequestFeedback(owner,
            {'kind': 'resource_feedback', 'poll_interval_s': .01})
        self.feedback._stream, self.feedback.session_id = self.stream, self.session.session_id
        self.feedback.controller, self.feedback.gate.limit = self.controller, 16
        self.core.policies = replace(self.core.policies, admission=self.feedback.gate)

    def ownership(self):
        return dict(usage=asdict(self.core.capacity.usage()), retained=self.retained,
            backend_pending=tuple(self.backend.pending),
            task_state=tuple((key, r.phase, r.compute, r.member)
                for key, r in self.core.capacity.records.items()),
            received=frozenset(self.stream.received))

    def observe(self, *, observation=None, source=None, elapsed=.02):
        self.now += elapsed
        self.sequence += 1
        changes = dict(source or {})
        native_sequence = changes.get('source_sequence', self.sequence)
        source_at = self.native_times.setdefault(native_sequence, self.now)
        native = ServiceMetricsSnapshot(16, 0, .96, preemptions_total=1,
            source_sequence=native_sequence, source_uid='CPU-engine',
            source_session_id=self.session.session_id, source_observed_at_s=source_at)
        native = replace(native, **changes)
        raw = AdmissionObservation(self.now, True, 16, 16, 0, .96,
            sample_age_s=self.now-native.source_observed_at_s,
            sample_sequence=self.sequence, preemptions_total=native.preemptions_total)
        raw = replace(raw, **(observation or {}))
        self.feedback.provider = SimpleNamespace(latest_with_source=lambda inflight: (raw, native))
        before = self.ownership()
        with patch.object(wiring, 'time', SimpleNamespace(monotonic=lambda: self.now)), \
                patch.object(self.controller, 'update_supply', wraps=self.controller.update_supply) as update:
            self.feedback.observe(self.stream)
        assert before == self.ownership()
        return raw, update.call_args.args[0], self.controller.report()


class OrdinaryRequestSafetyTests(unittest.TestCase):
    def test_fresh_preemption_survives_unknown_failed_and_optional_usage(self):
        for status in ('succeeded', 'unknown', 'failed', 'optional_prompt_unknown'):
            with self.subTest(status=status):
                fixture = OrdinarySafetyFixture(status)
                raw, applied, report = fixture.observe()
                self.assertTrue(raw.fresh)
                self.assertEqual(applied.fresh, raw.fresh)
                self.assertEqual(fixture.feedback.gate.limit, 8)
                self.assertEqual(report['reasons']['resource_pressure'], 1)
                self.assertIsNone(report['verified_performance_window'])
                self.assertIsNone(fixture.controller.export_hint())
                fixture.session.advance(1)
                self.assertEqual(fixture.core.capacity.usage().active_requests, 16)
                self.assertEqual(len(fixture.backend.pending), 16)

    def test_missing_work_pauses_performance_without_synthesizing_useful_work(self):
        for status in ('unknown', 'failed', 'optional_prompt_unknown'):
            with self.subTest(status=status):
                fixture = OrdinarySafetyFixture(status)
                before = fixture.controller.report()
                raw, applied, report = fixture.observe(source={'preemptions_total': 0})
                self.assertEqual(applied.fresh, raw.fresh)
                self.assertIsNone(applied.completed_input_work_total)
                self.assertEqual(report['window'], 16)
                self.assertEqual(report['changes'], before['changes'])
                self.assertEqual(report['accepted_steps'], before['accepted_steps'])
                self.assertIsNone(report['verified_performance_window'])
                self.assertIn('completed_work_unknown', report['reasons'])

    def test_actual_uncertain_event_retains_Core_ownership_during_pressure_response(self):
        fixture = OrdinarySafetyFixture('unknown')
        handle, _ = fixture.backend.pending[fixture.key]
        fixture.backend.events.append(Uncertain(fixture.key, handle))
        fixture.core.reap(1)
        record = fixture.core.capacity.records[fixture.key]
        self.assertEqual(record.phase, 'UNCERTAIN')
        self.assertTrue(record.compute)
        before = fixture.ownership()
        fixture.observe()
        self.assertEqual(fixture.feedback.gate.limit, 8)
        self.assertEqual(fixture.ownership(), before)
        self.assertTrue(fixture.core.capacity.records[fixture.key].compute)
        self.assertIn(fixture.key, fixture.backend.pending)

    def test_duplicate_native_source_does_not_repeat_pressure_with_new_receiver_sequence(self):
        for fresh in (False, True):
            with self.subTest(receiver_fresh=fresh):
                fixture = OrdinarySafetyFixture('unknown')
                fixture.observe()
                sequence = fixture.sequence
                for _ in range(12):
                    fixture.observe(source={'source_sequence': sequence}, observation={'fresh': fresh})
                report = fixture.controller.report()
                self.assertEqual(fixture.feedback.gate.limit, 8)
                self.assertEqual(report['reasons']['resource_pressure'], 1)
                self.assertEqual(fixture.controller._preemptions, 1)

    def test_stale_or_not_fresh_native_data_does_not_apply_a_preemption(self):
        for changes in ({'fresh': False}, {'sample_age_s': 2.}):
            with self.subTest(changes=changes):
                fixture = OrdinarySafetyFixture('unknown')
                raw, applied, report = fixture.observe(observation=changes)
                self.assertEqual(applied.fresh, raw.fresh)
                self.assertEqual(fixture.feedback.gate.limit, 16)
                self.assertNotIn('resource_pressure', report['reasons'])
                self.assertEqual(fixture.controller._preemptions, 0)

    def test_foreign_owner_or_session_cannot_contribute_native_pressure(self):
        for changes in ({'source_uid': 'foreign-engine'}, {'source_session_id': 6},
                        {'source_session_id': 8}):
            with self.subTest(changes=changes):
                fixture = OrdinarySafetyFixture('unknown')
                fixture.observe(source=changes)
                report = fixture.controller.report()
                self.assertEqual(fixture.feedback.gate.limit, 16)
                self.assertNotIn('resource_pressure', report['reasons'])
                self.assertEqual(fixture.controller._preemptions, 0)

    def test_another_query_stream_is_ignored_before_sampling(self):
        fixture = OrdinarySafetyFixture('unknown')
        fixture.feedback.provider = SimpleNamespace(latest_with_source=lambda inflight:
            self.fail('another query stream must not sample this feedback source'))
        before = fixture.ownership()
        fixture.feedback.observe(SimpleNamespace(session=SimpleNamespace(session_id=8)))
        self.assertEqual(fixture.ownership(), before)
        self.assertEqual(fixture.feedback.gate.limit, 16)


if __name__ == '__main__':
    unittest.main()
