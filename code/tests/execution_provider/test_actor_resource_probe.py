"""Optional actor resource packets; CPU objects/futures only, no hardware reads."""
from concurrent.futures import Future
from copy import deepcopy
from dataclasses import asdict
import json
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.execution_provider.adapters.lotus_chain_token_batches import BatchSupplyFeedback
from src.execution_provider.adapters.records_async_model import AsyncCoreModelActor
from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.execution_provider.resource_request_sampler_fixture import ready, frame_age, request_sampler


UID = 'cpu-resource-engine'
GPU = 'GPU-00000000-0000-0000-0000-000000000001'


def resource_packet(*, interval=.1):
    return dict(engine_uid=UID, session_id=7, source_elapsed_s=0,
        gpu_uuids=[GPU], status='observed_or_partial',
        sample=dict(values=dict(gpu_utilization_pct=10, host_cpu_per_core_max_pct=20),
            sources=dict(host=dict(cpu_percent_interval_s=interval)), missing={}))


class ActorResourceProbeTests(unittest.TestCase):
    def actor(self):
        spec = EngineSpec('/tmp/actor-resource-cpu-fixture', sys.executable, sys.executable)
        actor = AsyncCoreModelActor(asdict(spec), asdict(GenerationSpec()), 1, asdict(AsyncEngineLimits()))
        actor._session = 7
        native = dict(engine_uid=UID, sequence=1, running=1, waiting=0, kv_usage=.1)
        snapshot = dict(owner_uid='cpu-producer-owner', native_feedback=native,
            held_requests=0, active_requests=0, input_bytes=0, reserved_result_bytes=0,
            ready_results=0, queued_commands=0, queued_controls=0, unknown_requests=0,
            broken=False, closing=False, child_alive=True)
        actor.engine = SimpleNamespace(ready_report=dict(engine_uid=UID),
                                       snapshot=lambda: deepcopy(snapshot))
        return actor, native

    def test_sampling_is_opt_in_cached_and_outside_the_actor_condition(self):
        actor, native = self.actor()

        def collect():
            self.assertFalse(actor._condition._is_owned(), 'optional readers must not retain the model condition')
            return deepcopy(resource_packet()['sample'])

        source = Mock(side_effect=collect)
        source.gpu_ids = (GPU,)
        with patch('src.observability.records_telemetry.MetricSources.from_cuda_visibility',
                   return_value=source) as construct:
            self.assertNotIn('resources', actor.snapshot())
            self.assertEqual(actor.drain_query(time.monotonic()+1)['status'], 'retained')
            construct.assert_not_called()
            for _ in range(2):
                snapshot = actor.snapshot(observe_resources=True)
                self.assertEqual(snapshot['ipc']['native_feedback'], native)
                packet = snapshot['resources']
                self.assertEqual((packet['engine_uid'], packet['session_id'], packet['gpu_uuids']),
                                 (UID, 7, [GPU]))
                self.assertEqual(packet['sample']['values']['gpu_utilization_pct'], 10)
                self.assertGreaterEqual(packet['source_elapsed_s'], 0)
            construct.assert_called_once_with()
            self.assertEqual(source.call_count, 2)
            actor.drain_query(time.monotonic()+1)
            self.assertEqual(source.call_count, 2, 'ordinary drain must keep the optional reader dormant')

    def test_optional_reader_failure_preserves_the_native_snapshot(self):
        actor, native = self.actor()
        source = Mock(side_effect=RuntimeError('CPU fixture resource reader failure'))
        source.gpu_ids = (GPU,)
        with patch('src.observability.records_telemetry.MetricSources.from_cuda_visibility', return_value=source):
            snapshot = actor.snapshot(observe_resources=True)
        self.assertEqual(snapshot['ipc']['native_feedback'], native)
        self.assertEqual(snapshot['resources']['status'], 'unavailable')
        self.assertEqual(snapshot['resources']['error_type'], 'RuntimeError')
        self.assertEqual(actor.snapshot()['ipc']['native_feedback'], native)

    def test_snapshot_resource_flag_requires_an_explicit_boolean(self):
        actor, _ = self.actor()
        for value in (1, None, 'true'):
            with self.subTest(value=value), self.assertRaises(TypeError):
                actor.snapshot(observe_resources=value)

    def feedback(self, *, reference=False):
        calls, futures = [], []

        def remote(**kwargs):
            calls.append(kwargs)
            future = Future()
            futures.append(future)
            return SimpleNamespace(future=lambda: future)

        owner = SimpleNamespace(
            _actors=SimpleNamespace(model_actor=SimpleNamespace(snapshot=SimpleNamespace(remote=remote)),
                model_ready=ready(UID), ray=SimpleNamespace(cancel=lambda *a, **k: None),
                limits=AsyncEngineLimits()),
            prepared_batching=PreparedBatching('token_budget', None, 256),
            resources=RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=4),
            engine=EngineSpec('/tmp/resource-receiver-fixture', sys.executable, sys.executable))
        feedback = (BatchSupplyFeedback(owner, SupplyFeedbackConfig(4)) if reference
                    else request_sampler(owner, 7))
        feedback.session_id, feedback.started = 7, 0.
        return feedback, calls, futures

    def receive(self, packet, *, reference=False):
        feedback, calls, futures = self.feedback(reference=reference)
        self.assertIsNone(feedback.sample())
        futures[0].set_result(frame_age(dict(session_id=7, resources=packet, ipc=dict(native_feedback=dict(
            engine_uid=UID, sequence=12, observed_at_s=time.monotonic(), running=1, waiting=0,
            kv_usage=.1, generation_tokens_total=4, preemptions_total=0)))))
        snapshot = feedback.sample()
        self.assertIsNotNone(snapshot, 'malformed optional resource fields cannot erase valid native feedback')
        self.assertEqual((snapshot.source_uid, snapshot.source_session_id), (UID, 7))
        self.assertEqual(feedback.requests, 1)
        self.assertEqual(len(feedback.snapshot_rpc_receipts), 1)
        self.assertEqual(feedback.snapshot_rpc_receipts[0]['rpc_terminal_status'], 'returned')
        self.assertEqual(feedback.errors, 0)
        feedback.close()
        self.assertTrue(feedback.report()['snapshot_rpc_terminal_evidence_complete'])
        return feedback, calls

    def test_receiver_requests_resources_on_the_existing_single_ascending_snapshot_rpc(self):
        feedback, calls = self.receive(resource_packet())
        self.assertEqual(calls, [dict(observe_resources=True)])
        view, = feedback.resource_samples
        self.assertEqual((view.engine_uid, view.session_id, view.request_index, view.gpu_uuids),
                         (UID, 7, 1, (GPU,)))
        self.assertEqual((view.gpu_busy_pct, view.host_peak_cpu_pct, view.source_elapsed_s), (10, 20, 0))
        self.assertLessEqual(view.requested_at_s, view.received_at_s)

    def test_reference_path_keeps_its_snapshot_rpc_without_optional_sampling(self):
        feedback, calls = self.receive(resource_packet(), reference=True)
        self.assertEqual(calls, [{}])
        self.assertEqual(feedback.resource_samples, [])

    def test_malformed_optional_payload_stays_unknown_without_discarding_native_stats(self):
        for packet in (None, [], {'sample': []}, {'sample': {'values': [], 'sources': []}},
                       {'gpu_uuids': [1], 'sample': {'values': {'gpu_utilization_pct': True,
                                                              'host_cpu_per_core_max_pct': float('nan')}}}):
            with self.subTest(packet=packet):
                feedback, _ = self.receive(packet)
                view, = feedback.resource_samples
                self.assertIsNone(view.gpu_busy_pct)
                self.assertIsNone(view.host_peak_cpu_pct)

    def test_host_peak_is_unknown_when_its_sampling_interval_is_not_current(self):
        for interval in (None, 0, -.1, 2, True, '.1', float('nan')):
            with self.subTest(interval=interval):
                feedback, _ = self.receive(resource_packet(interval=interval))
                view, = feedback.resource_samples
                self.assertEqual(view.gpu_busy_pct, 10)
                self.assertIsNone(view.host_peak_cpu_pct)
                self.assertIsNone(feedback.resource_audit[0]['host_cpu_interval_s'])

    def test_rejected_CPU_interval_and_source_reason_remain_in_audit(self):
        for interval, reason in ((0, 'cpu_interval_nonpositive'), (-.1, 'cpu_interval_nonpositive'),
                                 (2, 'cpu_interval_exceeds_feedback_window'),
                                 (True, 'cpu_interval_missing_or_invalid'),
                                 (float('nan'), 'cpu_interval_missing_or_invalid')):
            with self.subTest(interval=interval):
                packet = resource_packet(interval=interval)
                host = packet['sample']['sources']['host']
                host.update(cpu_baseline_scope='sampler_instance', cpu_counter_sample_sequence=12,
                            cpu_metrics_status='observed')
                feedback, _ = self.receive(packet)
                audit, = feedback.resource_audit
                expected = interval if type(interval) in (int, float) and interval == interval else None
                self.assertEqual(audit['host_cpu_raw_interval_s'], expected)
                self.assertEqual(audit['host_cpu_reject_reason'], reason)
                self.assertEqual(audit['host_cpu_source']['cpu_counter_sample_sequence'], 12)
                json.dumps(audit, allow_nan=False)

    def test_CPU_priming_reason_does_not_erase_native_feedback_and_remains_explicit(self):
        packet = resource_packet(interval=None)
        packet['sample']['values']['host_cpu_per_core_max_pct'] = None
        packet['sample']['sources']['host']['cpu_metrics_status'] = 'cpu_percent_baseline_priming'
        feedback, _ = self.receive(packet)
        self.assertEqual(feedback.resource_audit[0]['host_cpu_reject_reason'], 'cpu_percent_baseline_priming')


if __name__ == '__main__':
    unittest.main()
