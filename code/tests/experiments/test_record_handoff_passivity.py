"""Diagnostic callbacks do not insert I/O, scans or another RPC Future adapter."""
from concurrent.futures import Future
from dataclasses import make_dataclass
import json
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from tests.experiments import test_record_resource_capture as fixtures


class HandoffPassivityTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.observer = self.fixture.observer
        self.observer.settings()['profile'] = 'handoff'

    def driver(self):
        class Backend:
            def retained_refs(self): return 0
            def _submit_task(self, *args): pass
            def _audit_result(self, *args): pass
            def _result_transferred(self, *args): pass
        class Core:
            def __init__(self, capacity):
                self.capacity, self.backend, self.sink = capacity, Backend(), None
        class Owner:
            def close(self): return {}
        class AsyncOwner:
            def poll_prepared_batch(self, key): return self.ref
        backend_module = ModuleType('src.execution_provider.adapters.lotus_chain_actor_backend')
        backend_module.LotusChainActorBackend = Backend
        core_module = ModuleType('src.execution_provider.adapters.lotus_records_core')
        core_module.SessionEngine = Core
        owner_module = sys.modules['src.execution_provider.adapters.lotus_records_actor_owner']
        async_module = sys.modules['src.execution_provider.adapters.lotus_records_async_actor']
        owner_module.RecordsActorOwner = Owner
        owner_module._runtime_env = lambda python: dict(env_vars={})
        async_module.AsyncRecordsActorOwner = AsyncOwner
        fixtures.load('record_ipc_observer')
        modules = {backend_module.__name__: backend_module, core_module.__name__: core_module}
        with patch.dict(sys.modules, modules), \
             patch.object(self.observer, 'install_cpu_hooks'), \
             patch.object(self.observer, 'install_ready_hook'), \
             patch.object(self.observer, 'install_feedback_observer'):
            self.observer.install_driver()
        return Core, AsyncOwner

    def test_poll_observer_leaves_the_owned_future_conversion_to_the_transport(self):
        _, Owner = self.driver()
        conversions = []
        class Ref:
            def future(self):
                value = Future()
                value.set_result(b'bounded result')
                conversions.append(value)
                return value
        owner = Owner(); owner.ref = Ref()
        result = owner.poll_prepared_batch((1, 7))
        self.assertIs(result, owner.ref)
        self.assertEqual(conversions, [], 'the observer must not construct its own RPC Future')
        self.assertEqual(result.future().result(), b'bounded result')
        self.assertEqual(len(conversions), 1)

    def test_handoff_core_event_omits_usage_scan_and_hot_path_disk_io(self):
        Core, _ = self.driver()
        usage = Mock(side_effect=AssertionError('per-event full table scan'))
        core = Core(SimpleNamespace(usage=usage))
        Key = make_dataclass('Key', [('session_id', int), ('sequence', int)])
        with patch.object(self.observer.os, 'write') as write:
            core.sink('released', Key(1, 3))
            usage.assert_not_called()
            write.assert_not_called()
        self.observer.close_events()
        event = next(v for v in self.fixture.events() if v['event'] == 'Core_released')
        self.assertEqual(event['key'], dict(session_id=1, sequence=3))
        self.assertIsNone(event['usage'], 'missing per-event counters are unknown, not zero')

    def test_full_profile_keeps_its_explicit_usage_observation(self):
        self.observer.settings()['profile'] = 'full'
        Core, _ = self.driver()
        Usage = make_dataclass('Usage', [('active_requests', int)])
        usage = Mock(return_value=Usage(7))
        core = Core(SimpleNamespace(usage=usage))
        Key = make_dataclass('Key', [('session_id', int), ('sequence', int)])
        core.sink('released', Key(1, 3))
        usage.assert_called_once_with()
        event = next(v for v in self.fixture.events() if v['event'] == 'Core_released')
        self.assertEqual(event['usage'], dict(active_requests=7))

    def test_member_ready_notification_does_not_write_before_await_continuation(self):
        future = Future(); continued = []
        self.observer.observe_ref_ready(SimpleNamespace(future=lambda: future),
            dict(session_id=1, sequence=2), [fixtures.IDENTITY])
        future.add_done_callback(lambda value: continued.append(value.result()))
        with patch.object(self.observer.os, 'write') as write:
            future.set_result(b'original')
            write.assert_not_called()
        self.assertEqual(continued, [b'original'])
        self.assertEqual(self.fixture.events(), [])
        self.observer.set_phase('post_return')
        self.observer.close_events()
        event = next(v for v in self.fixture.events() if v['event'] == 'driver_future_done')
        self.assertEqual(event['phase'], 'online')
        self.assertLessEqual(event['observation_ns'], event['monotonic_ns'])
        status = json.loads(next((self.fixture.directory/'events').glob('*.status.json')).read_text())
        self.assertGreaterEqual(status['feedback_buffer']['enqueue_wall_ns'], 0)
        self.assertGreaterEqual(status['feedback_buffer']['enqueue_thread_cpu_ns'], 0)
        self.assertTrue(status['feedback_buffer']['drain_complete'])

    def test_buffer_clock_failure_is_reported_without_changing_member_result(self):
        self.observer.emit('observer_fixture_open')
        future = Future()
        self.observer.observe_ref_ready(SimpleNamespace(future=lambda: future),
            dict(session_id=1, sequence=2), [fixtures.IDENTITY])
        with patch.object(self.observer.time, 'thread_time_ns', side_effect=OSError('clock unavailable')):
            future.set_result(b'original')
        self.assertEqual(future.result(), b'original')
        self.observer.close_events()
        status = json.loads(next((self.fixture.directory/'events').glob('*.status.json')).read_text())
        self.assertEqual(status['feedback_buffer']['errors'], 1)
        self.assertFalse(status['feedback_buffer']['drain_complete'])
        self.assertIsNone(status['feedback_buffer']['enqueue_wall_ns'])
        self.assertIsNone(status['feedback_buffer']['enqueue_thread_cpu_ns'])
