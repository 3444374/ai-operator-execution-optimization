"""First native source publication through actual bounded CPU child IPC."""
from pathlib import Path
import asyncio
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_async_engine import AsyncEngineLimits, NativeStats, VllmAsyncEngine
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.serving.test_async_engine_process import row


class FirstFeedbackProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        fixture = Path(__file__).with_name('native_first_feedback_fixture.py')
        return [sys.executable, str(fixture), *map(str, descriptors)]


class FirstNativeFeedbackTests(unittest.TestCase):
    def owner(self, mode='delayed', *, feedback_interval_s=30.):
        with patch.object(EngineSpec, 'validate_driver_environment', return_value={}):
            owner = FirstFeedbackProcess(EngineSpec('/tmp/first-feedback/'+mode,
                sys.executable, sys.executable), GenerationSpec(),
                limits=AsyncEngineLimits(max_inflight=2, result_slots=2,
                    feedback_interval_s=feedback_interval_s), startup_timeout_s=10)
        self.addCleanup(owner.close, timeout=3)
        return owner

    def until(self, owner, predicate):
        deadline = time.monotonic()+3
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail('fixture native source or terminal did not arrive before the long regular tick: '
                    + str(owner.snapshot()))
            owner.wait(.01)

    def test_first_real_source_reaches_parent_before_the_held_first_request_finishes(self):
        owner = self.owner()
        self.assertIsNone(owner.snapshot()['native_feedback'])
        submitted = owner.submit(row(text='hold'), deadline_monotonic=time.monotonic()+10)
        self.assertEqual(submitted.status, 'queued')
        self.until(owner, lambda: owner.snapshot()['native_feedback'] is not None)
        value = owner.snapshot()
        self.assertEqual((value['active_requests'], value['ready_results']), (1, 0))
        self.assertEqual(value['native_feedback']['running'], 1)
        self.assertEqual(value['native_feedback']['gated_requests'], 1)
        self.assertEqual(value['native_feedback']['finished_total'], 0)
        self.assertEqual(value['native_feedback']['generation_tokens_total'], 0)
        self.assertTrue(owner.abort(submitted.request_id))
        self.until(owner, lambda: owner.snapshot()['ready_results'] == 1)
        result, = owner.poll()
        self.assertTrue(result.engine_terminal_confirmed)
        self.assertEqual(result.result['call_id'], 'q:0')
        self.assertEqual(owner.snapshot()['held_requests'], 0)

    def test_an_already_real_initial_source_has_no_mandatory_first_sleep(self):
        owner = self.owner('initial')
        self.until(owner, lambda: owner.snapshot()['native_feedback'] is not None)
        self.assertEqual(owner.snapshot()['accepted_requests'], 0)
        self.assertEqual(owner.snapshot()['native_feedback']['running'], 0)

    def test_close_interrupts_wait_for_a_source_that_never_exists(self):
        owner = self.owner()
        self.assertIsNone(owner.snapshot()['native_feedback'])
        report = owner.close(timeout=3)
        self.assertEqual(report['status'], 'closed')
        self.assertTrue(report['producer_quiescence']['confirmed'])
        self.assertEqual(owner.snapshot()['accepted_requests'], 0)

    def test_feedback_disabled_does_not_publish_or_wait_for_a_native_source(self):
        owner = self.owner('initial', feedback_interval_s=0.)
        self.assertEqual(owner.close(timeout=3)['status'], 'closed')
        self.assertIsNone(owner.snapshot()['native_feedback'])
        self.assertEqual(owner.snapshot()['accepted_requests'], 0)

    def test_optional_first_source_notification_error_does_not_replace_model_terminal(self):
        owner = self.owner('notify-error')
        submitted = owner.submit(row(), deadline_monotonic=time.monotonic()+10)
        self.assertEqual(submitted.status, 'queued')
        self.until(owner, lambda: owner.snapshot()['ready_results'] == 1)
        result, = owner.poll()
        self.assertEqual(result.result['status'], 'succeeded')
        self.assertEqual(result.result['call_id'], 'q:0')
        self.assertEqual(result.result['total_tokens'], 12)
        self.assertTrue(result.engine_terminal_confirmed)
        self.assertEqual(owner.snapshot()['held_requests'], 0)


class FirstNativeSourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_scheduler_record_signals_once_and_keeps_its_source_time(self):
        now = [10.]
        stats = NativeStats('fixture-engine', clock=lambda: now[0])
        event = stats.first_sample_ready
        engine = object.__new__(VllmAsyncEngine)
        engine._feedback = stats
        waiting = asyncio.create_task(engine.wait_for_first_feedback())
        await asyncio.sleep(0)
        self.assertFalse(waiting.done())
        self.assertIsNone(stats.snapshot())
        stats.record(None, SimpleNamespace(num_preempted_reqs=0,
            num_generation_tokens=0, finished_requests=[]))
        self.assertFalse(event.is_set(), 'missing scheduler state must remain unknown')
        native = SimpleNamespace(num_running_reqs=1, num_waiting_reqs=0, kv_cache_usage=.1)
        stats.record(native, None)
        await waiting
        first = stats.snapshot()
        self.assertEqual((first['sequence'], first['observed_at_s'], first['running']), (1, 10., 1))
        event.clear()
        now[0] = 10.1
        stats.record(native, None)
        self.assertFalse(event.is_set(), 'later records keep the existing periodic transport cadence')
        self.assertEqual(stats.snapshot()['sequence'], 2)
        self.assertEqual(first['observed_at_s'], 10.)
