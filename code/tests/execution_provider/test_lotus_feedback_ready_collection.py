"""Ready control RPCs publish without waiting for the next dispatch tick; CPU only."""
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_chain_token_batches as batching
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig
from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig
from tests.execution_provider import test_lotus_snapshot_rpc_receipts as receipts


class FeedbackReadyCollectionTests(unittest.TestCase):
    def bound_sampler(self, *, observer=False):
        helper = receipts.SnapshotRPCReceiptTests()
        feedback, refs, cancels = helper.sampler()
        # Long test-only tick makes the event ordering decisive, not a millisecond
        # performance threshold. No production default or policy is overridden.
        feedback.config = SupplyFeedbackConfig(4, ResourceFeedbackConfig(poll_interval_s=3, window_s=3))
        feedback.owner._expected_count = 1
        feedback.owner._token_organizer = SimpleNamespace(concurrency=1)
        stream = SimpleNamespace(session=SimpleNamespace(session_id=1),
            run=SimpleNamespace(current=[None]), _stage_ids=['map0'])
        first, published = threading.Event(), threading.Event()
        sample = feedback.sample
        def sampled():
            result = sample()
            first.set()
            return result
        feedback.sample = sampled
        with patch('src.scheduling.runtime.observations.threading.Thread'):
            feedback.bind(stream)
        provider = feedback.provider
        provider.observer = (lambda event:None) if observer else None
        observed = provider._observe
        writer_threads = []
        def observe(snapshot, **context):
            observed(snapshot, **context)
            if context['stage'] == 'sample' and context['accepted']:
                writer_threads.append(threading.current_thread().name)
                published.set()
        provider._observe = observe
        thread = threading.Thread(target=provider._sample_loop, name='fixture-sampler')
        provider._thread = thread
        return feedback, refs, cancels, first, published, writer_threads, thread

    def check_ready_collection(self, observer):
        feedback, refs, _, first, published, writers, thread = self.bound_sampler(observer=observer)
        thread.start()
        try:
            self.assertTrue(first.wait(1), 'initial regular sample must return before completing the RPC')
            frame = receipts.SnapshotRPCReceiptTests().frame()
            refs[0][1].set_result(frame)
            self.assertTrue(published.wait(.5), 'completed RPC stayed asleep until the next dispatch tick')
            self.assertEqual(len(refs), 1, 'completion wake must not dispatch another RPC')
            self.assertEqual(writers, ['fixture-sampler'], 'completion callback cannot publish on its own thread')
            row, = feedback.report()['snapshot_rpc_receipts']
            self.assertEqual(row['rpc_terminal_status'], 'returned')
            self.assertEqual(feedback.provider.latest(1).running, 1)
        finally:
            feedback.close()
            thread.join(1)
            self.assertFalse(thread.is_alive())

    def test_completed_RPC_is_published_before_next_tick_with_observer_off(self):
        self.check_ready_collection(False)

    def test_completed_RPC_is_published_before_next_tick_with_observer_on(self):
        self.check_ready_collection(True)

    def manual_sampler(self, **options):
        feedback, refs, cancels, *_ = self.bound_sampler(**options)
        feedback.provider._thread = SimpleNamespace(is_alive=lambda:False, join=lambda **k:None)
        self.addCleanup(feedback.close)
        return feedback, refs, cancels

    def test_early_collection_spends_the_original_regular_turn_without_extra_RPCs(self):
        feedback, refs, _ = self.manual_sampler()
        feedback.sample(); refs[0][1].set_result(receipts.SnapshotRPCReceiptTests().frame())
        feedback.provider._sample_once(feedback.collect_ready, ready=True)
        self.assertTrue(feedback.provider.latest(1).fresh)
        for _ in range(5):
            self.assertIsNone(feedback.collect_ready())
        self.assertEqual(len(refs), 1)
        feedback.provider._sample_once(feedback.sample)
        self.assertFalse(feedback.provider.latest(1).fresh, 'cached collection cannot refresh the source')
        self.assertEqual(len(refs), 1)
        feedback.sample()
        self.assertEqual(len(refs), 2)
        self.assertIs(feedback.ref, refs[1][0])

    def test_close_interrupts_ready_wait_and_late_terminal_keeps_old_report_unknown(self):
        from copy import deepcopy
        feedback, refs, cancels, first, _, _, thread = self.bound_sampler()
        thread.start()
        try:
            self.assertTrue(first.wait(1))
            stopped = threading.Event()
            closer = threading.Thread(target=lambda:(feedback.close(), stopped.set()))
            closer.start()
            try:
                self.assertTrue(stopped.wait(.5), 'close cannot wait for the regular sample period')
            finally:
                closer.join(4)
            self.assertFalse(thread.is_alive())
            before = deepcopy(feedback.report())
            self.assertEqual(before['snapshot_rpc_cleanup_status'], 'unknown')
            self.assertEqual(cancels, [refs[0][0]])
            refs[0][1].set_result(receipts.SnapshotRPCReceiptTests().frame())
            self.assertEqual(before['snapshot_rpc_receipts'], [])
            after = feedback.report()
            self.assertEqual(after['snapshot_rpc_cleanup_status'], 'closed')
            self.assertEqual(after['snapshot_rpc_receipts'][0]['frame_disposition'], 'closed')
            self.assertEqual(len(refs), 1)
        finally:
            feedback.close(); thread.join(1)

    def test_local_future_cancel_wakes_but_retains_one_unknown_RPC(self):
        feedback, refs, _ = self.manual_sampler()
        feedback.sample(); refs[0][1].cancel()
        self.assertTrue(feedback._snapshot_ready.is_set())
        self.assertIsNone(feedback.collect_ready())
        for _ in range(4):feedback.sample()
        self.assertEqual(len(refs), 1)
        self.assertIs(feedback.ref, refs[0][0])
        self.assertTrue(feedback.report()['snapshot_rpc_pending']['client_future_cancelled'])
        self.assertEqual(feedback.report()['snapshot_rpc_receipts'], [])

    def test_failed_ready_registration_falls_back_to_the_original_poll(self):
        feedback, refs, _ = self.manual_sampler()
        remote = feedback.owner._actors.model_actor.snapshot.remote
        def create():
            ref = remote()
            def unavailable(callback):raise RuntimeError('fixture callback registration failed')
            refs[-1][1].add_done_callback = unavailable
            return ref
        feedback.owner._actors.model_actor.snapshot.remote = create
        feedback.sample(); refs[0][1].set_result(receipts.SnapshotRPCReceiptTests().frame())
        self.assertFalse(feedback._snapshot_ready.is_set())
        self.assertIsNotNone(feedback.sample())
        row, = feedback.report()['snapshot_rpc_receipts']
        self.assertEqual(row['future_wakeup_error_type'], 'RuntimeError')
        self.assertEqual(row['rpc_terminal_status'], 'returned')
        self.assertEqual(len(refs), 1)

    def test_phase_resume_does_not_reintroduce_an_early_old_session_frame(self):
        feedback, refs, _ = self.manual_sampler()
        feedback.sample(); refs[0][1].set_result(receipts.SnapshotRPCReceiptTests().frame())
        feedback.provider._sample_once(feedback.collect_ready, ready=True)
        self.assertTrue(feedback.provider.latest(1).fresh)
        feedback.end_phase()
        feedback.bind(SimpleNamespace(session=SimpleNamespace(session_id=2),
            run=SimpleNamespace(current=[None]), _stage_ids=['map0']))
        feedback.provider._sample_once(feedback.sample)
        self.assertIsNone(feedback.provider.latest(1).running)
        self.assertEqual(len(refs), 1)
        feedback.sample()
        self.assertEqual(feedback.report()['snapshot_rpc_pending']['request_session_id'], 2)

    def test_early_collection_does_not_refresh_source_age_or_relax_freshness(self):
        feedback, refs, _ = self.manual_sampler()
        frame = receipts.SnapshotRPCReceiptTests().frame()
        frame['ipc']['native_feedback']['observed_at_s'] = feedback.started
        feedback.provider.clock = lambda:feedback.started+10
        feedback.sample(); refs[0][1].set_result(frame)
        feedback.provider._sample_once(feedback.collect_ready, ready=True)
        observed = feedback.provider.latest(1)
        self.assertFalse(observed.fresh)
        self.assertEqual(observed.sample_age_s, 10)
        self.assertEqual(feedback.provider.stale_after_s, 3)

    def test_inline_terminal_and_stale_notification_do_not_clear_a_valid_sample(self):
        feedback, refs, _ = self.manual_sampler(observer=True)
        remote = feedback.owner._actors.model_actor.snapshot.remote
        def create():
            ref = remote()
            refs[-1][1].set_result(receipts.SnapshotRPCReceiptTests().frame())
            return ref
        feedback.owner._actors.model_actor.snapshot.remote = create
        feedback.provider._sample_once(feedback.sample)
        feedback._snapshot_ready.set()
        feedback.provider._sample_once(feedback.collect_ready, ready=True)
        self.assertTrue(feedback.provider.latest(1).fresh)
        self.assertEqual(len(refs), 1)
        self.assertIsNone(feedback._collected_ready)

    def test_invalid_or_raised_early_result_also_spends_the_original_collect_turn(self):
        for result in ('invalid', 'raised'):
            with self.subTest(result=result):
                feedback, refs, _ = self.manual_sampler()
                feedback.sample()
                if result == 'raised':refs[0][1].set_exception(ValueError('fixture RPC failed'))
                else:refs[0][1].set_result(receipts.SnapshotRPCReceiptTests().frame(session=999))
                self.assertIsNone(feedback.collect_ready())
                row, = feedback.report()['snapshot_rpc_receipts']
                self.assertEqual(row['rpc_terminal_status'], 'raised' if result == 'raised' else 'returned')
                self.assertIsNone(feedback.sample())
                self.assertEqual(len(refs), 1)
                feedback.sample()
                self.assertEqual(len(refs), 2)

    def test_wakeup_callback_does_not_keep_a_closed_owner_alive(self):
        import gc
        import weakref
        feedback, refs, _, _, _, _, thread = self.bound_sampler()
        feedback.provider._thread = SimpleNamespace(is_alive=lambda:False, join=lambda **k:None)
        feedback.sample(); feedback.close()
        owner = weakref.ref(feedback)
        del feedback, thread; gc.collect()
        self.assertIsNone(owner())
        refs[0][1].set_result(receipts.SnapshotRPCReceiptTests().frame())


if __name__ == '__main__':unittest.main()
