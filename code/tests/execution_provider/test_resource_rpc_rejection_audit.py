"""Bounded rejected-resource diagnostics; local Futures only, no Ray/model."""
from copy import deepcopy
import json
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_chain_token_batches import RequestSupplyFeedback
from tests.execution_provider import test_actor_resource_probe as fixture


class RejectedResourceAuditTests(unittest.TestCase):
    def feedback(self, *, reference=False, observer=None):
        feedback, calls, futures = fixture.ActorResourceProbeTests().feedback(reference=reference)
        self.addCleanup(feedback.close)
        if observer is not None:
            feedback.provider = SimpleNamespace(observe_native=observer, is_running=False,
                close=lambda:None, pause=lambda:None, resume=lambda:None)
        return feedback, calls, futures

    def frame(self, *, session=7, native=True, interval=.25):
        packet = fixture.resource_packet(interval=interval)
        packet['sample']['sources']['host'].update(cpu_metrics_status='observed',
            cpu_counter_sample_sequence=2, cpu_counter_read_start_monotonic_ns=100,
            cpu_counter_read_end_monotonic_ns=200)
        return dict(session_id=session, resources=packet,
            ipc=dict(native_feedback=(dict(engine_uid=fixture.UID, sequence=12,
                observed_at_s=time.monotonic(), running=1, waiting=0, kv_usage=.1,
                generation_tokens_total=4, preemptions_total=0) if native else None)))

    def receive(self, feedback, futures, frame):
        self.assertIsNone(feedback.sample())
        futures[-1].set_result(frame)
        return feedback.sample()

    def test_qualified_CPU_survives_missing_native_for_diagnosis_only(self):
        events=[]
        f, calls, futures = self.feedback(observer=lambda native, **context:events.append(context))
        self.assertIsNone(self.receive(f, futures, self.frame(native=False)))
        row, = f.resource_rpc_audit
        self.assertTrue(row['host_cpu_qualified'])
        self.assertEqual(row['host_cpu_raw_interval_s'], .25)
        self.assertEqual(row['native_reason'], 'native_frame_missing')
        self.assertFalse(row['native_frame_present'])
        self.assertFalse(row['native_validation_accepted'])
        self.assertIsNone(f.last_native)
        self.assertEqual((f.resource_samples, f.resource_audit), ([], []))
        self.assertEqual(events[0]['resource_attempt'], row)
        self.assertFalse(events[0]['accepted'])
        self.assertEqual(calls, [dict(observe_resources=True)])

    def test_actor_unbound_and_previous_session_are_distinguishable(self):
        for session, unbound in [(-1, True), (6, False)]:
            with self.subTest(session=session):
                f, _, futures = self.feedback()
                self.assertIsNone(self.receive(f, futures, self.frame(session=session)))
                row, = f.resource_rpc_audit
                self.assertEqual((row['request_session_id'], row['current_session_id'],
                                  row['response_session_id']), (7, 7, session))
                self.assertEqual(row['actor_unbound'], unbound)
                self.assertEqual(row['native_reason'], 'session_mismatch')
                self.assertEqual((f.resource_rpc_counts['actor_unbound'],
                                  f.resource_rpc_counts['session_mismatch']), (int(unbound), 1))
                self.assertEqual(f.resource_samples, [])

    def test_CPU_interval_reasons_are_independent_of_native_rejection(self):
        for interval, expected in [(0, 'cpu_interval_nonpositive'),
                                   (2, 'cpu_interval_exceeds_feedback_window'),
                                   (None, 'cpu_interval_missing_or_invalid'),
                                   (float('nan'), 'cpu_interval_missing_or_invalid')]:
            with self.subTest(interval=interval):
                f, _, futures = self.feedback()
                self.receive(f, futures, self.frame(native=False, interval=interval))
                row, = f.resource_rpc_audit
                self.assertEqual(row['native_reason'], 'native_frame_missing')
                self.assertEqual(row['host_cpu_reject_reason'], expected)
                self.assertFalse(row['host_cpu_qualified'])
                json.dumps(f.report(), allow_nan=False)

    def test_priming_is_preserved_without_claiming_a_CPU_interval(self):
        f, _, futures = self.feedback()
        frame = self.frame(native=False, interval=None)
        frame['resources']['sample']['sources']['host']['cpu_metrics_status'] = 'cpu_percent_baseline_priming'
        self.receive(f, futures, frame)
        row, = f.resource_rpc_audit
        self.assertEqual(row['host_cpu_reject_reason'], 'cpu_percent_baseline_priming')
        self.assertIsNone(row['host_cpu_raw_interval_s'])

    def test_valid_native_snapshot_and_policy_resource_inputs_are_unchanged(self):
        f, _, futures = self.feedback()
        frame = self.frame()
        snapshot = self.receive(f, futures, frame)
        self.assertEqual((snapshot.source_uid, snapshot.source_session_id, snapshot.source_sequence),
                         (fixture.UID, 7, 12))
        self.assertIs(snapshot, f.last_native)
        view, = f.resource_samples
        self.assertEqual((view.gpu_busy_pct, view.host_peak_cpu_pct), (10, 20))
        row, = f.resource_rpc_audit
        self.assertTrue(row['native_validation_accepted'])
        self.assertTrue(row['audit_only'])
        self.assertEqual(row['resource_engine_uid'], fixture.UID)
        self.assertEqual(row['resource_session_id'], 7)
        self.assertEqual(len(f.resource_audit), 1)
        self.assertEqual(f.report()['resource_observation']['RPC_counts']['native_eligible'], 1)

    def bind_without_sampler(self, feedback, session):
        feedback.owner._expected_count = 0
        with patch.object(feedback, '_new_controller', return_value=None), \
                patch.object(feedback, '_reset_controller'):
            feedback.bind(SimpleNamespace(session=SimpleNamespace(session_id=session)))

    def test_new_bound_query_resets_attempt_history_and_counts(self):
        f, _, futures = self.feedback()
        self.receive(f, futures, self.frame(native=False))
        prior = deepcopy(f.report()['resource_observation'])
        self.bind_without_sampler(f, 8)
        self.assertEqual(f.resource_rpc_audit, [])
        self.assertTrue(all(v == 0 for v in f.resource_rpc_counts.values()))
        self.assertEqual(prior['RPC_counts']['total'], 1)
        self.assertEqual(f.report()['resource_observation']['RPC_audit_session_id'], 8)

    def test_old_pending_return_is_correlated_but_cannot_seed_the_next_query(self):
        f, _, futures = self.feedback()
        f.sample()
        self.bind_without_sampler(f, 8)
        futures[0].set_result(self.frame())
        self.assertIsNone(f.sample())
        row, = f.resource_rpc_audit
        self.assertEqual((row['request_session_id'], row['current_session_id'],
                          row['response_session_id']), (7, 8, 7))
        self.assertEqual(row['native_reason'], 'sampling_session_changed')
        self.assertIsNone(f.last_native)
        self.assertEqual(f.resource_samples, [])

    def test_bind_during_diagnostic_read_keeps_counter_and_history_atomic(self):
        f, _, futures = self.feedback()
        f.sample(); futures[0].set_result(self.frame())
        entered, release = threading.Event(), threading.Event()
        read = f._read_resource
        def blocked_read(*args, **kwargs):
            entered.set()
            if not release.wait(2):
                raise AssertionError('fixture did not release the diagnostic read')
            return read(*args, **kwargs)
        results=[]
        with patch.object(f, '_read_resource', side_effect=blocked_read):
            thread=threading.Thread(target=lambda:results.append(f.sample()))
            thread.start()
            try:
                self.assertTrue(entered.wait(1))
                self.bind_without_sampler(f, 8)
            finally:
                release.set();thread.join(2)
            self.assertFalse(thread.is_alive())
        self.assertEqual(results, [None])
        self.assertEqual((len(f.resource_rpc_audit), f.resource_rpc_counts['total']), (1, 1))
        row, = f.resource_rpc_audit
        self.assertEqual((row['request_session_id'], row['current_session_id']), (7, 8))
        self.assertIsNone(f.last_native)
        self.assertEqual(f.resource_samples, [])

    def test_terminal_close_retains_diagnostics_and_all_existing_terminal_proof(self):
        f, _, futures = self.feedback()
        f.sample(); futures[0].set_result(self.frame(native=False))
        f.close()
        report = f.report()
        self.assertEqual(report['snapshot_rpc_cleanup_status'], 'closed')
        self.assertTrue(report['snapshot_rpc_terminal_evidence_complete'])
        row, = report['resource_observation']['recent_RPC_attempts']
        self.assertEqual(row['native_reason'], 'feedback_closed')
        self.assertTrue(row['host_cpu_qualified'])
        self.assertFalse(row['native_validation_accepted'])
        self.assertIsNone(f.ref)

    def test_capacity_eviction_keeps_totals_and_does_not_issue_more_RPCS(self):
        f, calls, futures = self.feedback()
        for _ in range(70):
            self.receive(f, futures, self.frame(native=False))
        rows = f.resource_rpc_audit
        self.assertEqual(len(rows), 64)
        self.assertEqual((rows[0]['request_index'], rows[-1]['request_index']), (7, 70))
        self.assertEqual(f.resource_rpc_counts, dict(total=70, native_eligible=0, actor_unbound=0,
            session_mismatch=0, native_missing=70, CPU_qualified=70, CPU_rejected=0, evicted=6, errors=0))
        self.assertEqual(len(calls), 70)
        self.assertTrue(all(future.done() for future in futures))

    def test_reference_path_keeps_optional_resource_sampling_off(self):
        events=[]
        f, calls, futures = self.feedback(reference=True, observer=lambda native, **c:events.append(c))
        self.assertIsNotNone(self.receive(f, futures, self.frame()))
        self.assertEqual(calls, [{}])
        self.assertEqual(f.resource_rpc_audit, [])
        self.assertNotIn('resource_attempt', events[0])

    def test_new_rejected_audit_excludes_payload_and_unrecognized_source_text(self):
        f, _, futures = self.feedback()
        frame = self.frame(native=False, interval=None)
        marker = 'NEVER_RETAIN_SAMPLE_PAYLOAD'
        frame.update(prompt=marker, output=marker, row=dict(private_sample=marker))
        packet = frame['resources']
        packet['private_data'] = marker
        packet['sample']['values']['private_sample'] = marker
        host = packet['sample']['sources']['host']
        host.update(cpu_metrics_status=marker, cpu_counter_sample_sequence=marker)
        self.receive(f, futures, frame)
        row, = f.resource_rpc_audit
        self.assertNotIn(marker, json.dumps(row))
        self.assertEqual(row['host_cpu_reject_reason'], 'cpu_source_unavailable')
        self.assertIsNone(row['host_cpu_counter_sequence'])

    def test_optional_audit_failure_preserves_valid_native_and_single_RPC(self):
        f, calls, futures = self.feedback()
        with patch.object(f, '_audit_resource_rpc', side_effect=RuntimeError('fixture observer failure')):
            snapshot = self.receive(f, futures, self.frame())
        self.assertIsNotNone(snapshot)
        self.assertIs(snapshot, f.last_native)
        self.assertEqual(len(f.resource_samples), 1)
        self.assertEqual((len(calls), f.errors, f.resource_rpc_counts['errors']), (1, 0, 1))
        self.assertEqual(f.resource_rpc_counts['total'], 1)

    def test_observer_cannot_mutate_the_retained_diagnostic_row(self):
        def mutate(native, **context):
            context['resource_attempt']['host_cpu_qualified'] = False
        f, _, futures = self.feedback(observer=mutate)
        self.assertIsNotNone(self.receive(f, futures, self.frame()))
        self.assertTrue(f.resource_rpc_audit[0]['host_cpu_qualified'])


if __name__ == '__main__':
    unittest.main()
