"""Selection and observation tests for Sema post-HTTP native paths."""
from types import SimpleNamespace
import asyncio
import hashlib
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock

from src.experiments.postgresql.sema_native_query import (
    SEMA_NATIVE_ARMS, gateway_phase_summary, service_phase_summary, validate_native_options,
)


class SemaNativeQueryTests(unittest.TestCase):
    def test_persistent_owner_reports_native_service_and_vendor_actor_lifetimes(self):
        from src.experiments.postgresql.persistent_native_adapter_query import _ArmOwner
        for arm in SEMA_NATIVE_ARMS:
            owner = object.__new__(_ArmOwner)
            owner.group = SimpleNamespace(ray_session_id='owned-session')
            owner.arm, owner.identity, owner.queries = arm, 'owner', 0
            owner.sema_executor = owner.execution = owner.lm = owner.connection = owner.native = None
            value = owner.lifecycle()
            self.assertEqual(value['request_service_scope'], 'per query HTTP service; finite native snapshots')
            if SEMA_NATIVE_ARMS[arm] == 'ray-data':
                self.assertIn('creates its own actors', value['native_ray_actor_scope'])

    def test_all_native_arms_require_bounded_held_responses(self):
        options = SimpleNamespace(concurrency=4)
        for arm in SEMA_NATIVE_ARMS:
            with self.subTest(arm=arm):
                validate_native_options(arm, options, 128, 'ready-runtime.json')
                for held in (3, 129):
                    with self.assertRaises(ValueError):
                        validate_native_options(arm, options, held, 'ready-runtime.json')

    def test_imlane_requires_explicit_configuration(self):
        with self.assertRaisesRegex(ValueError, 'ready-runtime'):
            validate_native_options('sema-imlane-request-service', SimpleNamespace(concurrency=4), 8)
        validate_native_options('sema-native-direct', SimpleNamespace(concurrency=256), 256)

    def test_stages_use_service_clock_and_separate_vendor_from_model(self):
        fields = ['proxy_arrived_ns', 'body_read_ns', 'service_accepted_ns', 'native_enqueued_ns',
                  'native_snapshot_ns', 'native_graph_started_ns', 'native_outcome_received_ns',
                  'response_written_ns']
        row = dict(request_sequence=0, **{key: 1+i*1_000_000 for i, key in enumerate(fields)})
        result = service_phase_summary([row])
        self.assertEqual(result['phases']['service_request_e2e']['samples'], [.007])
        self.assertEqual(result['phases']['native_execution_to_receipt']['samples'], [.001])
        self.assertEqual(result['model_inference']['status'], 'unavailable')
        self.assertEqual(result['phases']['service_request_e2e']['count'], 1)
        self.assertTrue(result['phases']['service_request_e2e']['p99_is_sample_maximum'])

    def test_missing_observation_stays_missing(self):
        result = service_phase_summary([dict(request_sequence=0, proxy_arrived_ns=1)])
        self.assertEqual(result['phases']['service_request_e2e']['status'], 'unavailable')
        self.assertEqual(result['phases']['service_request_e2e']['missing_count'], 1)
        self.assertIsNone(result['phases']['service_request_e2e']['p99'])

    def test_actual_gateway_span_is_separate_from_model_inference(self):
        value = gateway_phase_summary([dict(upstream_headers_send_started_monotonic_ns=1,
            upstream_response_body_read_completed_monotonic_ns=1_000_001)])
        self.assertEqual(value['phases']['model_http']['samples'], [.001])
        self.assertEqual(value['phases']['connection_and_submit']['status'], 'unavailable')
        self.assertIn('neither pure inference', value['addition'])
        with self.assertRaises(ValueError):
            gateway_phase_summary([dict(upstream_headers_send_started_monotonic_ns=2,
                upstream_response_body_read_completed_monotonic_ns=1)])

    def test_duplicate_occurrence_and_reversed_clocks_are_errors(self):
        row = dict(request_sequence=0, proxy_arrived_ns=2, body_read_ns=1)
        with self.assertRaises(ValueError):
            service_phase_summary([row])
        with self.assertRaises(ValueError):
            service_phase_summary([dict(request_sequence=0), dict(request_sequence=0)])

    def test_query_entry_consumes_sequential_native_process_results_and_service_stages(self):
        from src.baselines.text.frameworks.native_calls import NativeCallOutcome, _request
        from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
        from src.baselines.text.products import sema
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.execution_provider.adapters.sema_native import SemaNativeService
        from src.execution_provider.semantic_map import SemanticMapPlan
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.shared_request_budget import CellBudgetLedger
        from src.experiments.postgresql import supplier_adapter_query as query
        from tests.experiments.test_native_adapter_query import fixture_server
        from tests.execution_provider.test_sema_service import _PROCESS

        class FixtureExecutor:
            def __init__(self, backend, options):
                self.backend, self.options = backend, options
                self.last_close_report = None

            def iter_batch(self, calls, *, cancelled, timeout_s):
                for call in calls:
                    if cancelled():
                        raise InterruptedError('CPU fixture cancelled')
                    result = asyncio.run(_request(call))
                    self.assert_outcome(result, call, NativeCallOutcome)
                    yield result
                self.last_close_report = dict(clean=True, graph_consumer_stopped=True,
                                              remote_http_stopped='unavailable')

            @staticmethod
            def assert_outcome(result, call, outcome_type):
                if type(result) is not outcome_type or result.call_id != call.call_id:
                    raise ValueError('fixture changed complete-call identity')

        def fixture_service(stack, arm, *, query_id, model, limits, trace_path, options,
                            held_tasks, imlane_config=None):
            backend = SEMA_NATIVE_ARMS[arm]
            return stack.enter_context(SemaNativeService(query_id=query_id,
                upstream_url=model.endpoint_url, limits=limits, trace_path=trace_path,
                backend=backend, options=options, held_request_capacity=held_tasks,
                snapshot_rows=held_tasks, executor=FixtureExecutor(backend, options)))

        for arm in SEMA_NATIVE_ARMS:
            with self.subTest(arm=arm), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                binary = root/'fixture-sema'
                binary.write_text('#!'+sys.executable+'\n'+_PROCESS)
                binary.chmod(0o700)
                ledger = CellBudgetLedger.create(root/'ledger.sqlite', AttemptBudget('fixture', 2),
                                                 deadline_utc=time.time()+60)
                with fixture_server(content='"ok"') as (endpoint, posts), \
                        mock.patch.object(sema, 'BINARY_SHA256', hashlib.sha256(binary.read_bytes()).hexdigest()), \
                        mock.patch.object(query, '_runtime', return_value=(None, {'owner': 'CPU fixture'})), \
                        mock.patch.object(query, 'open_native_service', fixture_service):
                    result = query.run_supplier_query(arm,
                        load_source=lambda: iter([dict(row_id=str(i), text='same') for i in range(2)]),
                        plan=SemanticMapPlan('Return ok.', 'fixture', 16),
                        model=FixedModelConfig(endpoint, 'fixture', 5000), ledger=ledger,
                        unit_id='fixture', root=root/'query', options=NativeGraphOptions(),
                        sema_binary=binary, sema_native_threads=1, query_timeout_s=5,
                        imlane_config=root/'fixture-only.json', reference_outputs={'0': 'ok', '1': 'ok'})
                self.assertEqual(result['status'], 'passed')
                self.assertEqual(result['actual_posts'], 2)
                self.assertEqual(len(posts), 2)
                self.assertEqual(result['execution']['recorded_rows'], 2)
                self.assertEqual(result['service_timing']['phases']['service_request_e2e']['count'], 2)
                self.assertEqual(result['service_timing']['phases']['native_execution_to_receipt']['count'], 2)
                self.assertEqual(result['gateway_timing']['phases']['model_http']['count'], 2)
                self.assertEqual(result['call_timing']['status'], 'unavailable')
                self.assertEqual(result['request_service']['held_requests'], 0)
                self.assertFalse(result['request_service']['native_bridge_thread_alive'])
                self.assertIsNone(result['semloom_capacity'])
