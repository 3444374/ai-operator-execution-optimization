"""Zero-network regression for observer errors followed by actual cleanup errors."""
from contextlib import contextmanager, ExitStack
import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments import execution_timing as timing
from src.experiments.postgresql.cell_evidence import CellErrors
from src.experiments.postgresql import persistent_native_adapter_query as persistent
from src.experiments.postgresql import supplier_adapter_query as supplier
from src.observability.request_gateway import GatewayRoute, ObservationGateway


class MemoryWriter:
    def __init__(self, path=None, fail_operation=None):
        self.events = []
        self.fail_operation = fail_operation

    def record(self, event):
        if event['operation'] == self.fail_operation and event['event'] == 'span_ended':
            raise OSError('synthetic timing end failure')
        self.events.append(event)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class NoNetworkGateway:
    def __init__(self, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def endpoint_url(self, *args):
        return 'http://fixture.invalid/v1/chat/completions'


class CommonTimingErrorPriorityTests(unittest.TestCase):
    @contextmanager
    def observations(self, root, *, enabled=True, fail_operation=None):
        with patch.dict(os.environ, {'SEMLOOM_DETAILED_TIMING_DIR': str(root) if enabled else ''}), \
                patch.object(timing, '_shared', None):
            if not enabled:
                yield
                return
            trace = timing.Timing(root, writer_factory=lambda path: MemoryWriter(path, fail_operation))
            with patch.object(timing, '_shared', trace):
                try:
                    yield
                finally:
                    trace.close()

    def test_cell_errors_promotes_real_error_after_timing_error_and_retains_both_details(self):
        observer = timing._timing_error(OSError('observer failed'))
        real = ValueError('real resource close failed')
        errors = CellErrors()
        errors.record('observation', observer)
        errors.record('cleanup', real)
        self.assertIs(errors.first, real)
        with self.assertRaises(ValueError) as caught:
            errors.raise_if_failed()
        self.assertIs(caught.exception, real)
        self.assertEqual(set(errors.details), {'observation', 'cleanup'})
        self.assertTrue(any('OSError' in note for note in real.__notes__))

    def test_cell_errors_preserves_first_real_error_for_all_later_failures(self):
        real = LookupError('original execution failed')
        errors = CellErrors()
        errors.record('execution', real)
        errors.record('observation', timing._timing_error(OSError('observer failed')))
        errors.record('cleanup', ValueError('resource close failed'))
        self.assertIs(errors.first, real)
        self.assertEqual(set(errors.details), {'execution', 'observation', 'cleanup'})

    def supplier_failure(self, root, transcript, *, original=None, cleanup=None):
        def build(*args, **kwargs):
            if original is not None:
                raise original
            return object()

        def drain(execution):
            transcript.append('execution-close')
            if cleanup is not None:
                raise cleanup

        def budget_close(*args):
            transcript.append('budget-close')
            if original is not None and cleanup is not None:
                raise cleanup

        ledger = SimpleNamespace(reserve_unit=lambda *args: None,
                                 claim_shared_unit=lambda *args: object(),
                                 close_shared_unit=budget_close)
        with patch.object(supplier, 'ObservationGateway', NoNetworkGateway), \
                patch.object(supplier, 'BufferedEvents', MemoryWriter), \
                patch.object(supplier, 'build_native_execution', side_effect=build), \
                patch.object(supplier, '_drain_execution', side_effect=drain):
            return supplier.run_supplier_query('lotus-method-semloom-local-diagnostic',
                load_source=lambda: iter([dict(row_id='fixture-row', text='fixture-content')]),
                plan=SemanticMapPlan('Return ok.', 'fixture', 16),
                model=FixedModelConfig('http://fixture.invalid/v1/chat/completions', 'fixture', 3000),
                ledger=ledger, unit_id='fixture-query', root=root,
                options=NativeGraphOptions(), query_timeout_s=10)

    def test_supplier_create_observation_failure_does_not_hide_real_execution_close_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root, transcript = Path(directory), []
            real = ValueError('real execution close failed')
            with self.observations(root/'timing', fail_operation='semloom.executor_create'):
                with self.assertRaises(ValueError) as caught:
                    self.supplier_failure(root/'query', transcript, cleanup=real)
            self.assertIs(caught.exception, real)
            self.assertEqual(transcript, ['execution-close', 'budget-close'])
            summary = json.loads((root/'query/summary.json').read_text())
            self.assertEqual(summary['status'], 'failed')
            self.assertEqual(set(summary['errors']), {'query', 'execution_drain'})
            self.assertTrue(any('OSError' in note for note in real.__notes__))

    def test_supplier_keeps_original_execution_error_when_budget_close_also_fails_on_and_off(self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory() as directory:
                root, transcript = Path(directory), []
                original, cleanup = LookupError('original build failed'), ValueError('budget close failed')
                with self.observations(root/'timing', enabled=enabled):
                    with self.assertRaises(LookupError) as caught:
                        self.supplier_failure(root/'query', transcript, original=original, cleanup=cleanup)
                self.assertIs(caught.exception, original)
                self.assertEqual(transcript, ['budget-close'])

    def test_owner_start_observation_failure_does_not_hide_real_owner_close_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root, transcript = Path(directory), []
            real = ValueError('real owner close failed')
            def close():
                transcript.append('owner-close')
                raise real
            owner = SimpleNamespace(close=close)
            group = persistent.PersistentAdapterGroup(('fixed-map-native-daft',),
                plan=None, model=None, ledger=None, root=root/'group')
            with self.observations(root/'timing', fail_operation='owner.arm_prepare'), \
                    patch.object(persistent, '_runtime', return_value=(None, {})), \
                    patch.object(persistent, '_ArmOwner', return_value=owner):
                with self.assertRaises(ValueError) as caught:
                    group.__enter__()
            self.assertIs(caught.exception, real)
            self.assertEqual(transcript, ['owner-close'])
            self.assertTrue(any('OSError' in note for note in real.__notes__))

    def test_owner_exit_promotes_real_cleanup_error_over_passed_timing_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root, transcript = Path(directory), []
            real = ValueError('real group close failed')
            def close():
                transcript.append('group-close')
                raise real
            group = persistent.PersistentAdapterGroup(('fixed-map-native-daft',),
                plan=None, model=None, ledger=None, root=root/'group')
            group.root.mkdir()
            group.stack.callback(close)
            observer = timing._timing_error(OSError('earlier timing failure'))
            self.assertIsNone(sys.exception())
            with self.observations(root/'timing'):
                with self.assertRaises(ValueError) as caught:
                    group.__exit__(type(observer), observer, None)
            self.assertIs(caught.exception, real)
            self.assertEqual(transcript, ['group-close'])
            self.assertTrue(any('OSError' in note for note in real.__notes__))

    def test_owner_start_keeps_real_execution_error_over_real_cleanup_error_on_and_off(self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory() as directory:
                root, transcript = Path(directory), []
                original, cleanup = LookupError('original owner init failed'), ValueError('runtime close failed')
                def close():
                    transcript.append('runtime-close')
                    raise cleanup
                def runtime(stack, *args):
                    stack.callback(close)
                    return None, {}
                group = persistent.PersistentAdapterGroup(('fixed-map-native-daft',),
                    plan=None, model=None, ledger=None, root=root/'group')
                with self.observations(root/'timing', enabled=enabled), \
                        patch.object(persistent, '_runtime', side_effect=runtime), \
                        patch.object(persistent, '_ArmOwner', side_effect=original):
                    with self.assertRaises(LookupError) as caught:
                        group.__enter__()
                self.assertIs(caught.exception, original)
                self.assertEqual(transcript, ['runtime-close'])

    def test_owner_exit_keeps_passed_cancellation_over_real_cleanup_error_on_and_off(self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory() as directory:
                root, transcript = Path(directory), []
                original = asyncio.CancelledError('original cancellation')
                def close():
                    transcript.append('group-close')
                    raise ValueError('real close failed')
                group = persistent.PersistentAdapterGroup(('fixed-map-native-daft',),
                    plan=None, model=None, ledger=None, root=root/'group')
                group.root.mkdir()
                group.stack.callback(close)
                with self.observations(root/'timing', enabled=enabled):
                    self.assertIsNone(group.__exit__(type(original), original, None))
                self.assertEqual(transcript, ['group-close'])
                self.assertTrue(any('ValueError' in note for note in original.__notes__))

    def test_arm_query_observation_failure_does_not_hide_real_http_drain_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root, transcript = Path(directory), []
            real = ValueError('real persistent HTTP drain failed')
            def snapshot(*args):
                transcript.append('query-http-drain')
                raise real
            owner = object.__new__(persistent._ArmOwner)
            owner.binding, owner.service, owner.execution = None, None, None
            owner.group = SimpleNamespace(model=SimpleNamespace(timeout_ms=1000), poisoned=False)
            owner.gateway = SimpleNamespace(reset_upstream_connections=lambda: None, snapshot=snapshot)
            with self.observations(root/'timing', fail_operation='fixture.query_body'):
                with self.assertRaises(ValueError) as caught:
                    with owner.query('fixture-query', root, None, None, None):
                        with timing.timing_span('fixture.query_body'):
                            pass
            self.assertIs(caught.exception, real)
            self.assertEqual(transcript, ['query-http-drain'])
            self.assertIsNone(owner.binding)
            self.assertIsNone(owner.service)
            self.assertTrue(owner.group.poisoned)
            self.assertTrue(any('OSError' in note for note in real.__notes__))

    def test_arm_init_observation_failure_does_not_hide_real_gateway_close_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root, transcript = Path(directory), []
            real = ValueError('real persistent gateway close failed')
            class GatewayCloseFailure(NoNetworkGateway):
                def __exit__(self, *_):
                    transcript.append('gateway-close')
                    raise real
            @timing.timed_call('lotus.lm_prepare')
            def lm(*args):
                return object()
            group = SimpleNamespace(root=root/'group', physical=None,
                model=FixedModelConfig('http://fixture.invalid/v1/chat/completions', 'fixture', 3000),
                plan=SemanticMapPlan('Return ok.', 'fixture', 16), options=NativeGraphOptions(),
                query_timeout_s=10, tokenizer_path=None)
            with self.observations(root/'timing', fail_operation='lotus.lm_prepare'), \
                    patch.object(persistent, 'ObservationGateway', GatewayCloseFailure), \
                    patch.object(persistent, 'BufferedEvents', MemoryWriter), \
                    patch.object(persistent, 'prepare_lotus_lm', side_effect=lm):
                with self.assertRaises(ValueError) as caught:
                    persistent._ArmOwner(group, 'lotus-adapted-native')
            self.assertIs(caught.exception, real)
            self.assertEqual(transcript, ['gateway-close'])
            self.assertTrue(any('OSError' in note for note in real.__notes__))

    def test_gateway_stop_wrapper_keeps_diagnostic_provenance_after_actual_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            transcript = []
            gateway = ObservationGateway(routes=(GatewayRoute('job', 'model', 'http://fixture.invalid'),),
                                         trace_path=Path(directory)/'gateway.jsonl')
            gateway._thread = SimpleNamespace(join=lambda **kwargs: transcript.append('thread-join'),
                                              is_alive=lambda: False)
            observer = OSError('stored timing failure')
            gateway._detailed_timing_error = observer
            with self.assertRaises(RuntimeError) as caught:
                gateway.stop()
            self.assertTrue(timing.is_timing_error(caught.exception))
            self.assertIs(caught.exception.__cause__, observer)
            self.assertIsNone(gateway._thread)
            self.assertEqual(transcript, ['thread-join'])
            real = ValueError('later real cleanup failure')
            errors = CellErrors()
            errors.record('gateway_timing', caught.exception)
            errors.record('resource_close', real)
            self.assertIs(errors.first, real)

    def test_gateway_real_stop_failure_takes_priority_over_passed_timing_error(self):
        with tempfile.TemporaryDirectory() as directory:
            transcript = []
            gateway = ObservationGateway(routes=(GatewayRoute('job', 'model', 'http://fixture.invalid'),),
                                         trace_path=Path(directory)/'gateway.jsonl')
            gateway._thread = SimpleNamespace(join=lambda **kwargs: transcript.append('thread-join'),
                                              is_alive=lambda: True)
            gateway._detailed_timing_error = OSError('stored timing failure')
            observer = timing._timing_error(OSError('body timing failure'))
            with self.assertRaisesRegex(RuntimeError, 'did not stop cleanly') as caught:
                gateway.__exit__(type(observer), observer, None)
            self.assertFalse(timing.is_timing_error(caught.exception))
            self.assertIsNotNone(gateway._thread)
            self.assertEqual(transcript, ['thread-join'])
            self.assertTrue(any('OSError' in note for note in caught.exception.__notes__))


if __name__ == '__main__':
    unittest.main()
