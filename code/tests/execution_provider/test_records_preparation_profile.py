"""Preparation/profile composition without model or tokenizer allocation."""
from dataclasses import replace
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
from src.execution_provider.adapters.lotus_records_core import create_records_core
from src.execution_provider.adapters.records_profile import checked_preparation_options
from src.execution_provider.adapters import records_query_runtime
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec


class PreparationFeedbackProfileTests(unittest.TestCase):
    def test_public_factory_accepts_current_feedback_without_allocating_a_model(self):
        engine = EngineSpec('/tmp/CPU-fixture-model', sys.executable, sys.executable)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=4)
        with patch.object(AsyncRecordsActorOwner, '__init__') as allocate:
            owner = create_records_core(FilterMethod('fixture', instruction='Keep {passage}'),
                engine, GenerationSpec(), resources,
                profile=dict(mode='async', admission=dict(kind='resource_feedback')),
                prepared_batching=PreparedBatching('token_budget', None, 512), rolling_refill=True,
                interleaved_preparation=True, event_driven_preparation=True)
            self.assertTrue(owner.interleaved_preparation)
            self.assertTrue(owner.event_driven_preparation)
            self.assertEqual(owner.execution_profile['record_batching']['request_feedback_kind'], 'resource_feedback')
            self.assertIsNone(owner.owner)
            allocate.assert_not_called()

    def test_legacy_feedback_is_not_implicitly_enabled_by_the_combination(self):
        with self.assertRaises(ValueError):
            checked_preparation_options(dict(rolling_refill=True, request_feedback={},
                request_feedback_kind='legacy_resource_feedback'), interleaved_preparation=True)

    def test_preparation_and_feedback_do_not_require_application_token_packing(self):
        engine = EngineSpec('/tmp/CPU-fixture-model', sys.executable, sys.executable)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=4)
        for admission in (None, dict(kind='resource_feedback')):
            profile = dict(mode='async', **({} if admission is None else dict(admission=admission)))
            with self.subTest(admission=admission), patch.object(AsyncRecordsActorOwner, '__init__') as allocate:
                owner = create_records_core(FilterMethod('fixture', instruction='Keep {passage}'),
                    engine, GenerationSpec(), resources, profile=profile, rolling_refill=True,
                    interleaved_preparation=True, event_driven_preparation=True)
                batching = owner.execution_profile['record_batching']
                self.assertIsNone(batching['prepared_batching'])
                self.assertIsNone(batching['B'])
                self.assertTrue(batching['interleaved_preparation'])
                self.assertTrue(batching['event_driven_preparation'])
                self.assertIs(owner.engine, engine)
                self.assertIsNone(owner.engine.max_model_len)
                self.assertIsNone(owner.engine.max_num_seqs)
                self.assertIsNone(owner.engine.max_num_batched_tokens)
                self.assertIsNone(owner.owner)
                allocate.assert_not_called()
        with patch.object(AsyncRecordsActorOwner, '__init__') as allocate:
            with self.assertRaisesRegex(ValueError, 'tokenizer-owned chat template'):
                create_records_core(FilterMethod('fixture', instruction='Keep {passage}'),
                    replace(engine, chat_template='custom'), GenerationSpec(), resources,
                    profile=dict(mode='async'), rolling_refill=True, interleaved_preparation=True)
            allocate.assert_not_called()


class EventFeedbackTurnTests(unittest.TestCase):
    def turn(self):
        stream = records_query_runtime._RecordLocalRows.__new__(records_query_runtime._RecordLocalRows)
        records = [SimpleNamespace(phase='QUEUED', compute=False, preparation_held=False)]
        prefix = []
        organizer = SimpleNamespace(request_window=1, usage=Mock())
        organizer.dispatch_token = lambda *_: tuple(prefix) or None
        organizer.refresh_prefix = Mock(side_effect=lambda *_: prefix.__setitem__(slice(None), [organizer.request_window]))
        feedback = SimpleNamespace(controller=object(), next_poll=100.01, update=Mock())
        backend = SimpleNamespace(progress=lambda: SimpleNamespace(generation=1, pending=False, contended=False))
        wake = SimpleNamespace(generation=7, wait=Mock())
        stream.owner = SimpleNamespace(transport=SimpleNamespace(backend=backend),
            core=SimpleNamespace(wake=wake), capacity=SimpleNamespace(step_actions=4, poll_interval_s=.4),
            deadline=101., _token_organizer=organizer, _supply_feedback=feedback)
        stream.session = SimpleNamespace(session_id=1, limits=SimpleNamespace(held_tasks=4),
            _pending_members=(), _retry_at=0., _records=lambda: records)
        stream.driver = SimpleNamespace(finished=False, active_rows=1,
            _rows={1:SimpleNamespace(run=SimpleNamespace(final=None))},
            advance=Mock(return_value=SimpleNamespace(next_deadline=None)))
        stream._event_session, stream._backend_seen = 1, 1
        stream._core_due_at = stream._last_dispatch_token = None
        stream._core_results_pending = stream._method_results_pending = stream._end_input_pending = False
        stream._phase_eof, stream._consumer_due_at, stream._last_compute = True, 0., 1
        stream.section = object()
        stream._consume_method_results = Mock(return_value=0)
        stream._record_core_usage = Mock()
        return stream

    def test_feedback_deadline_bounds_wait_without_preparation_or_backend_activity(self):
        stream = self.turn()
        with patch.object(records_query_runtime, 'time', SimpleNamespace(monotonic=lambda:100.)):
            stream._tick_event()
        stream.driver.advance.assert_not_called()
        stream.owner._supply_feedback.update.assert_called_once_with(stream)
        stream.owner.core.wake.wait.assert_called_once_with(7, 100.01-100.)

    def test_window_growth_dispatches_existing_prefix_without_a_backend_notification(self):
        stream = self.turn()
        organizer = stream.owner._token_organizer
        stream.owner._supply_feedback.update.side_effect = lambda _: setattr(organizer, 'request_window', 2)
        with patch.object(records_query_runtime, 'time', SimpleNamespace(monotonic=lambda:100.)):
            stream._tick_event()
            organizer.refresh_prefix.assert_called_once()
            stream.owner.core.wake.wait.assert_not_called()
            stream._tick_event()
        stream.driver.advance.assert_called_once()

    def test_eof_after_final_release_advances_without_waiting_for_a_backend_event(self):
        from src.semantic_methods.continuation import Final
        from src.semantic_methods.driver import RowIdentity
        from tests.scheduling.test_method_driver import make_driver

        for service in (True, False):
            with self.subTest(service=service):
                stream = self.turn()
                driver, _engine, backend, pool = make_driver(
                    rows=1, method=SimpleNamespace(start=lambda _: Final(b'{}')))
                try:
                    driver.offer_row(RowIdentity(0, 'last'), b'zero')
                    driver.end_input()
                    if not service:
                        driver.advance(1)
                    stream.driver, stream.session = driver, driver.session
                    stream._event_session = stream.session.session_id
                    stream._backend_seen = 0 if service else 1
                    stream._method_results_pending = not service
                    stream.section, stream._extract_results = None, []
                    del stream._consume_method_results
                    stream._finish_phase = Mock()
                    self.assertEqual(driver.active_rows, 1)
                    with patch.object(records_query_runtime, 'time', SimpleNamespace(monotonic=lambda:100.)):
                        stream._tick_event()
                        self.assertEqual(driver.active_rows, 0)
                        self.assertEqual(pool.used, (0, 0))
                        self.assertFalse(driver.finished)
                        self.assertTrue(stream._end_input_pending)
                        stream._tick_event()
                    self.assertTrue(driver.finished)
                    self.assertEqual(stream._extract_results, [{}])
                    stream._finish_phase.assert_called_once()
                    stream.owner.core.wake.wait.assert_not_called()
                    self.assertEqual(backend.routes, [])
                finally:
                    driver.close()
                self.assertEqual(pool.allocated, (0, 0))
