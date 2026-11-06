"""Single-query duration is caller owned; startup, IPC and cleanup remain finite."""
from collections import deque
from dataclasses import asdict
import json
import sys
import threading
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters import lotus_records_static as static
from src.execution_provider.adapters.lotus_records_actor_owner import RecordsActorOwner
from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity, CoreChainCapacity
from src.execution_provider.adapters.records_transport import _check, close_prepared_core
from src.infrastructure.deadlines import remaining_time
from src.semantic_methods.lotus.records import FilterMethod, RecordScan, RecordsResources
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec


class QueryDeadlineTests(unittest.TestCase):
    def test_default_null_and_explicit_duration_round_trip_without_changing_capacity(self):
        for duration in (None, 300):
            value = RecordsResources(max_active_batches=3, query_timeout_s=duration)
            self.assertEqual(RecordsResources(**json.loads(json.dumps(asdict(value)))), value)
            for capacity, args in ((CoreRecordsCapacity(), (value,)), (CoreChainCapacity(), (value, 2))):
                limits = capacity.limits(*args)
                self.assertEqual(limits.wait_timeout_s, duration)
                self.assertIsNone(limits.phase_timeout('INFLIGHT'))
                self.assertGreater(limits.input_bytes, 0)
        self.assertIsNone(RecordsResources().query_timeout_s)
        for name in ('query_timeout_s', 'startup_timeout_s'):
            for bad in (0, -1, False, '60', float('inf'), float('nan')):
                with self.subTest(name=name, bad=bad), self.assertRaises(ValueError):
                    RecordsResources(**{name: bad})
        with self.assertRaises(ValueError):
            RecordsResources(startup_timeout_s=None)

    def test_core_and_static_preflight_can_exceed_sixty_seconds_unless_caller_sets_duration(self):
        class ReachedScan(Exception):
            pass

        for kind in (core.SemLoomRecordsCore, static.SemLoomRecordsStaticReference):
            for options, expected in (({}, ReachedScan), ({'query_timeout_s': None}, ReachedScan),
                                      ({'query_timeout_s': 300}, ReachedScan),
                                      ({'query_timeout_s': 60}, TimeoutError)):
                clock = SimpleNamespace(now=100.)
                method = FilterMethod('fixture', 'Keep {text}', resolved_input_columns=('text',))
                source = SimpleNamespace(preflight=lambda _: setattr(clock, 'now', 161.),
                    scan_spec=lambda _: RecordScan((('row_id', 'string'), ('source_position', 'int64'),
                                                    ('text', 'string')), ('text',)),
                    expected=lambda _: [])
                sdk = ModuleType('src.semantic_methods.lotus.engine')
                sdk.bind_filter_method = lambda value: value
                sdk.validate_generation = lambda *_: None
                module = core if kind is core.SemLoomRecordsCore else static
                with self.subTest(kind=kind.__name__, options=options), \
                        patch.dict(sys.modules, {sdk.__name__: sdk}), \
                        patch.object(EngineSpec, 'validate_driver_environment', return_value={}), \
                        patch.object(module.time, 'monotonic', side_effect=lambda: clock.now), \
                        patch.object(module, 'prepare_native_records_scan', side_effect=ReachedScan):
                    owner = kind(method, EngineSpec('/tmp/cpu-model', sys.executable, sys.executable),
                        GenerationSpec(), RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                                                           max_active_batches=1, **options))
                    with self.assertRaises(expected):
                        owner.evaluate(source, query_id='single-query')

    def test_no_query_deadline_still_checks_cancel_and_polls_ready_with_finite_wait(self):
        with self.assertRaises(InterruptedError):
            _check(None, lambda: True)
        _check(None)
        owner = object.__new__(RecordsActorOwner)
        owner.close_deadline = None
        owner._closing = owner._start_failed = False
        owner.ray = SimpleNamespace(wait=Mock(side_effect=[([], ['ref']), (['ref'], [])]),
                                    get=Mock(return_value='done'))
        self.assertEqual(owner.wait_ready('ref', None), 'done')
        self.assertEqual([call.kwargs['timeout'] for call in owner.ray.wait.call_args_list], [.05, .05])
        with self.assertRaises(ValueError):
            owner.start(None)

    def test_no_query_timer_keeps_cleanup_on_the_existing_owner_window(self):
        actors = SimpleNamespace(_model_close_timeout_s=lambda: 15.,
            close=Mock(return_value=dict(cleanup_errors=[])))
        with patch('time.monotonic', return_value=100.):
            result = close_prepared_core(actors, None, None, None, deadline=None)
        self.assertEqual(actors.close_deadline, 116.)
        self.assertTrue(result['transport_closed'])
        actors.close.assert_called_once_with()

    def test_async_IPC_accepts_null_deadline_without_relaxing_slot_capacity(self):
        from tests.serving.test_async_engine_process import row
        owner = object.__new__(AsyncEngineProcess)
        owner.spec = EngineSpec('/tmp/cpu-model', sys.executable, sys.executable, max_model_len=8192)
        owner.generation = GenerationSpec()
        owner.limits = AsyncEngineLimits(max_inflight=2, result_slots=2)
        owner._owner_uid, owner._sequence, owner._query_id = 'cpu', 0, None
        owner._slots = {}
        owner._data_commands, owner._controls, owner._ready = deque(), deque(), deque()
        owner._condition, owner._io_done = threading.Condition(), threading.Event()
        owner._broken, owner._closing = None, False
        owner._submitted_batches = owner._peak_batch_members = owner._peak_batch_frame_bytes = 0
        owner.process = SimpleNamespace(poll=lambda: None)
        for batch in (False, True):
            if batch:
                result = owner.submit_batch([row(1)], generation=owner.generation, deadline_monotonic=None)
            else:
                result = owner.submit(row(0), deadline_monotonic=None)
            self.assertEqual(result.status, 'queued')
            self.assertIsNone(json.loads(owner._data_commands[-1][4:])['deadline'])
        self.assertEqual(owner.submit(row(2), deadline_monotonic=None).status, 'backpressure')
        self.assertEqual(len(owner._slots), 2)
        self.assertGreater(owner._usage()['reserved_result_bytes'], 0)
        self.assertEqual(owner.submit(row(3), deadline_monotonic=1).status, 'rejected')

    def test_async_actor_transfers_null_query_deadline_and_keeps_abort_responsibility(self):
        from src.execution_provider.adapters.records_async_model import AsyncCoreModelActor
        from src.serving.backends.async_engine_process import SubmitResult
        from tests.serving.test_async_engine_process import row
        spec = EngineSpec('/tmp/cpu-model', sys.executable, sys.executable, max_model_len=8192)
        limits = AsyncEngineLimits(max_inflight=1, result_slots=1)
        actor = AsyncCoreModelActor(asdict(spec), asdict(GenerationSpec()), 1, asdict(limits))
        actor.engine = SimpleNamespace(submit=Mock(return_value=SubmitResult('queued', 'ticket')),
            poll=Mock(return_value=()), abort=Mock(return_value=True))
        self.assertEqual(actor.try_submit_core([row(0)], None, 65536, (0, 0))['status'], 'accepted')
        self.assertIsNone(actor.engine.submit.call_args.kwargs['deadline_monotonic'])
        self.assertIsNone(actor._held[(0, 0)].deadline)
        self.assertEqual(actor.try_submit_core([row(1)], None, 65536, (0, 1))['status'], 'backpressure')
        self.assertFalse(actor.abort_core((0, 0))['engine_terminal_confirmed'])
        self.assertEqual(len(actor._held), 1)
        actor.engine.abort.assert_called_once_with('ticket')

    def test_IMLane_task_wait_can_outlive_sixty_seconds_but_command_timeout_stays_finite(self):
        from integrations.imlane_records.client import RecordsBridge
        from integrations.imlane_records.records_host import RecordsResults
        client = object.__new__(RecordsBridge)
        client.timeout_s = 10.
        clock = SimpleNamespace(now=100.)
        def ready(**kwargs):
            self.assertIsNone(kwargs['deadline'])
            clock.now = 161.
            return ['block']
        with patch('time.monotonic', side_effect=lambda: clock.now):
            client.ready = ready
            self.assertEqual(client.wait_ready(timeout_s=None), ['block'])
            rows = object.__new__(RecordsResults)
            rows.deadline = None
            self.assertIsNone(rows._remaining())
            self.assertEqual(remaining_time(None, client.timeout_s), 10.)
        self.assertEqual(client.timeout_s, 10.)


class IMLaneOperationDurationTests(unittest.TestCase):
    def test_command_and_cleanup_accept_explicit_finite_durations_above_six_hundred(self):
        from integrations.imlane_records.client import RecordsBridge
        from integrations.imlane_records.records_host import RecordsResults
        from integrations.imlane_records.filter_workload import PROTOCOL
        limits = SimpleNamespace(max_pending_blocks=1, describe=lambda: dict(fixture=True))
        stream = SimpleNamespace(fileno=lambda: -1)
        process = SimpleNamespace(stdin=stream, stdout=stream)
        ready = dict(event='ready', protocol=PROTOCOL, max_pending_blocks=1,
                     transport_limits=limits.describe())
        for options, expected in (({}, 10.), (dict(timeout_s=601.), 601.)):
            with self.subTest(options=options), \
                    patch('integrations.imlane_records.client.os.set_blocking'), \
                    patch.object(RecordsBridge, '_read', return_value=ready) as read, \
                    patch('time.monotonic', return_value=100.):
                bridge = RecordsBridge(process, abort_owner=lambda: None, max_pending=1, limits=limits,
                                       protocol=PROTOCOL, **options)
                self.assertEqual(bridge.timeout_s, expected)
                read.assert_called_once_with(100. + expected)
        for value in (None, 0, -1, True, float('inf'), float('nan')):
            with self.subTest(value=value), self.assertRaises(ValueError):
                RecordsBridge(process, abort_owner=lambda: None, max_pending=1, limits=limits,
                              timeout_s=value)
        rows = RecordsResults([], bridge, query_id='q', mapping=None, stage_id='filter', block_prefix='b',
                              control_dir='/tmp/cpu-no-allocation', deadline=None,
                              close_owner=lambda: {}, close_timeout_s=601.)
        self.assertEqual(rows.close_timeout_s, 601.)

    def test_existing_start_control_uses_the_whole_explicit_remaining_duration(self):
        from integrations.imlane_records.resident import IMLaneResidentSession, control_block
        from integrations.imlane_records.filter_workload import PROTOCOL
        session = object.__new__(IMLaneResidentSession)
        block, inputs = control_block('session_start', 'q')
        result = dict(kind='resident_control', operation='session_start',
                      _occurrence_id=inputs[0]['call_id'], _position=0)
        reply = dict(call_id=inputs[0]['call_id'], row_id=0, outcome_json=json.dumps(dict(
            protocol=PROTOCOL, query_id='q', stage_id='filter', status='ok', result=result)))
        session.client = SimpleNamespace(submit=Mock(), wait_ready=Mock(return_value=[block]),
                                         collect=Mock(return_value=[reply]))
        session.ready = None
        with patch('time.monotonic', return_value=100.):
            self.assertEqual(session._control('session_start', 'q', deadline=701.), result)
        session.client.wait_ready.assert_called_once_with(timeout_s=601.)
        self.assertEqual(session.client.submit.call_args.kwargs['deadline'], 701.)


if __name__ == '__main__':
    unittest.main()
