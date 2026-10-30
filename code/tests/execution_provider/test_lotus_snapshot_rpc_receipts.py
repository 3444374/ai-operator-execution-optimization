"""Snapshot RPC ownership and terminal receipts; no model, Ray or GPU required."""
from concurrent.futures import Future
from copy import deepcopy
import inspect
import json
import sys
import threading
import time
from types import SimpleNamespace
import unittest

from src.execution_provider.adapters.lotus_chain_token_batches import BatchSupplyFeedback
from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec


class SnapshotRPCReceiptTests(unittest.TestCase):
    def sampler(self, *, cancel_error=False, future_error=False):
        refs, cancels = [], []
        def remote():
            future, reads = Future(), []
            def get_future():
                reads.append(1)
                if future_error and len(reads) == 1:
                    raise RuntimeError('fixture future unavailable')
                return future
            ref = SimpleNamespace(future=get_future)
            refs.append((ref, future))
            return ref
        def cancel(ref, **kwargs):
            cancels.append(ref)
            if cancel_error:raise RuntimeError('fixture cancel failed')
        actors = SimpleNamespace(model_actor=SimpleNamespace(snapshot=SimpleNamespace(remote=remote)),
            ray=SimpleNamespace(cancel=cancel), model_ready={'engine_uid':'fixture-engine'},
            limits=AsyncEngineLimits(max_inflight=4, result_slots=4))
        owner = SimpleNamespace(_actors=actors, prepared_batching=PreparedBatching('token_budget', None, 256),
            resources=RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=4),
            engine=EngineSpec('/tmp/snapshot-fixture', sys.executable, sys.executable))
        feedback = BatchSupplyFeedback(owner, SupplyFeedbackConfig(4))
        feedback.session_id, feedback.started = 1, 0.
        return feedback, refs, cancels

    def frame(self, *, session=1, uid='fixture-engine', sequence=12):
        return dict(session_id=session, ipc=dict(native_feedback=dict(engine_uid=uid,
            sequence=sequence, observed_at_s=time.monotonic(), running=1, waiting=0,
            kv_usage=.1, generation_tokens_total=4, preemptions_total=0)))

    def test_close_collects_an_already_terminal_future_without_cancel(self):
        feedback, refs, cancels = self.sampler()
        feedback.sample(); refs[0][1].set_result(self.frame())
        feedback.close()
        self.assertIsNone(feedback.ref, 'known terminal snapshot must be collected before close flags are published')
        self.assertEqual(cancels, [])
        report = feedback.report()
        self.assertEqual(report['snapshot_rpc_cleanup_status'], 'closed')
        row, = report['snapshot_rpc_receipts']
        self.assertEqual((row['rpc_terminal_status'], row['frame_disposition']), ('returned', 'closed'))
        self.assertTrue(row['future_done_confirmed'])

    def test_late_terminal_after_cancel_error_is_collected_on_recheck(self):
        feedback, refs, cancels = self.sampler(cancel_error=True)
        feedback.sample(); feedback.close()
        first = deepcopy(feedback.report())
        self.assertIs(feedback.ref, refs[0][0])
        refs[0][1].set_result(self.frame())
        feedback.close()
        self.assertIsNone(feedback.ref, 'late terminal future must be reaped, not left as an ambiguous handle')
        report = feedback.report()
        self.assertEqual(report['snapshot_rpc_cleanup_status'], 'closed')
        self.assertEqual(first['snapshot_rpc_cleanup_status'], 'unknown')
        self.assertFalse(first['snapshot_rpc_terminal_evidence_complete'])
        row, = report['snapshot_rpc_receipts']
        self.assertEqual(row['cancel_error_type'], 'RuntimeError')
        self.assertEqual(row['rpc_terminal_status'], 'returned')
        self.assertEqual(len(cancels), 1)
        self.assertEqual(len(refs), 1)

    def test_normal_return_is_persisted_with_source_identity(self):
        feedback, refs, _ = self.sampler()
        feedback.sample(); refs[0][1].set_result(self.frame())
        snapshot = feedback.sample()
        self.assertEqual(snapshot.source_sequence, 12)
        report = feedback.report()
        self.assertIn('snapshot_rpc_receipts', report)
        row, = report['snapshot_rpc_receipts']
        self.assertEqual((row['request_index'], row['request_session_id'], row['response_session_id']), (1, 1, 1))
        self.assertEqual((row['expected_engine_uid'], row['response_engine_uid']), ('fixture-engine', 'fixture-engine'))
        self.assertEqual(row['response_source_sequence'], 12)
        self.assertEqual(row['frame_disposition'], 'eligible_for_provider')
        self.assertGreaterEqual(row['terminal_observed_at_s'], row['issued_at_s'])
        self.assertEqual(json.loads(json.dumps(row)), row)

    def test_cancel_request_does_not_prove_terminal_or_release_the_single_slot(self):
        for failed in (False, True):
            with self.subTest(failed=failed):
                feedback, refs, cancels = self.sampler(cancel_error=failed)
                for _ in range(6):feedback.sample()
                feedback.close(); feedback.close()
                report = feedback.report()
                self.assertEqual(len(refs), 1)
                self.assertEqual(len(cancels), 1)
                self.assertFalse(refs[0][1].done())
                self.assertIs(feedback.ref, refs[0][0])
                self.assertEqual(report['snapshot_rpc_cleanup_status'], 'unknown')
                self.assertEqual(report['snapshot_rpc_receipts'], [])
                self.assertEqual(report['snapshot_rpc_pending']['rpc_terminal_status'], 'unknown')
                self.assertTrue(report['snapshot_ref_retained'])

    def test_terminal_future_exception_is_recorded_without_a_normal_return_claim(self):
        feedback, refs, _ = self.sampler()
        feedback.sample(); refs[0][1].set_exception(RuntimeError('fixture RPC failed'))
        feedback.close()
        row, = feedback.report()['snapshot_rpc_receipts']
        self.assertEqual((row['rpc_terminal_status'], row['rpc_error_type']), ('raised', 'RuntimeError'))
        self.assertIsNone(feedback.ref)
        self.assertIsNone(feedback.last_native)

    def test_local_future_cancel_is_not_a_remote_RPC_terminal_receipt(self):
        feedback, refs, _ = self.sampler()
        feedback.sample()
        feedback.owner._actors.ray.cancel = lambda ref, **kwargs: refs[0][1].cancel()
        feedback.close()
        report = feedback.report()
        self.assertEqual(report['snapshot_rpc_receipts'], [])
        self.assertEqual(report['snapshot_rpc_cleanup_status'], 'unknown')
        self.assertTrue(report['snapshot_rpc_pending']['client_future_cancelled'])
        self.assertIs(feedback.ref, refs[0][0])

    def test_remote_result_exception_records_terminal_without_claiming_returned_frame(self):
        class TaskCancelledError(Exception):pass
        feedback, refs, _ = self.sampler()
        feedback.sample(); refs[0][1].set_exception(TaskCancelledError('fixture remote exception'))
        feedback.close()
        row, = feedback.report()['snapshot_rpc_receipts']
        self.assertEqual((row['rpc_terminal_status'], row['rpc_error_type']), ('raised', 'TaskCancelledError'))
        self.assertIsNone(feedback.ref)

    def test_future_access_error_keeps_the_issued_ref_and_later_records_terminal(self):
        feedback, refs, _ = self.sampler(future_error=True)
        feedback.sample()
        self.assertIs(feedback.ref, refs[0][0])
        feedback.sample(); refs[0][1].set_result(self.frame())
        feedback.close()
        row, = feedback.report()['snapshot_rpc_receipts']
        self.assertEqual(row['future_access_error_type'], 'RuntimeError')
        self.assertEqual(row['rpc_terminal_status'], 'returned')
        self.assertEqual(len(refs), 1)

    def test_old_phase_return_is_terminal_but_not_a_new_phase_observation(self):
        feedback, refs, _ = self.sampler()
        feedback.sample(); feedback.session_id = 2
        refs[0][1].set_result(self.frame(session=1))
        self.assertIsNone(feedback.sample())
        row, = feedback.report()['snapshot_rpc_receipts']
        self.assertEqual((row['rpc_terminal_status'], row['frame_disposition']), ('returned', 'stale_session'))
        self.assertEqual(row['request_session_id'], 1)
        self.assertIsNone(feedback.last_native)
        feedback.sample()
        self.assertEqual(len(refs), 2)

    def test_physical_cleanup_metadata_cannot_turn_a_pending_RPC_into_returned(self):
        feedback, refs, _ = self.sampler()
        feedback.sample(); feedback.close()
        feedback.owner.last_close_report = dict(status='closed', gpu_exit_confirmed=True,
            producer_quiescence={'all_processes_exited':True})
        report = feedback.report()
        self.assertEqual(report['snapshot_rpc_cleanup_status'], 'unknown')
        self.assertFalse(report['snapshot_rpc_terminal_evidence_complete'])
        self.assertEqual(report['snapshot_rpc_receipts'], [])
        self.assertFalse(refs[0][1].done())

    def test_empty_sampler_close_has_no_RPC_and_complete_empty_ledger(self):
        feedback, refs, _ = self.sampler()
        feedback.close()
        report = feedback.report()
        self.assertEqual(refs, [])
        self.assertEqual(report['snapshot_rpc_cleanup_status'], 'closed')
        self.assertTrue(report['snapshot_rpc_terminal_evidence_complete'])
        self.assertEqual(report['snapshot_rpc_receipts'], [])

    def test_cached_owner_close_rechecks_only_control_and_preserves_prior_unknown_report(self):
        from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
        feedback, refs, _ = self.sampler()
        feedback.sample()
        owner = object.__new__(SemLoomRecordChainCore)
        owner._supply_feedback = feedback
        owner.last_close_report = dict(status='closed', query_completed=False, calls=[dict(status='unknown')])
        first = owner._close_record_local()
        self.assertEqual(first['batch_supply_feedback']['snapshot_rpc_cleanup_status'], 'unknown')
        refs[0][1].set_result(self.frame())
        later = owner._close_record_local()
        self.assertEqual(later['batch_supply_feedback']['snapshot_rpc_cleanup_status'], 'closed')
        self.assertEqual(first['batch_supply_feedback']['snapshot_rpc_cleanup_status'], 'unknown')
        self.assertEqual(later['calls'], first['calls'])
        self.assertFalse(later['query_completed'])

    def test_invalid_close_payload_remains_a_returned_RPC_without_non_JSON_headers(self):
        feedback, refs, _ = self.sampler()
        feedback.sample()
        refs[0][1].set_result(dict(session_id=object(), ipc=dict(native_feedback=dict(
            engine_uid=object(), sequence=object(), observed_at_s=float('nan')))))
        feedback.close()
        report = feedback.report()
        row, = report['snapshot_rpc_receipts']
        self.assertEqual((row['rpc_terminal_status'], row['frame_disposition']), ('returned', 'closed'))
        self.assertIsNone(row['response_source_observed_at_s'])
        json.dumps(report, allow_nan=False)

    def test_dispatch_exception_keeps_the_unknown_single_RPC_slot(self):
        feedback, _, _ = self.sampler()
        sent = []
        def failed_remote():
            sent.append(1)
            raise RuntimeError('fixture dispatch outcome unavailable')
        feedback.owner._actors.model_actor.snapshot.remote = failed_remote
        for _ in range(5):feedback.sample()
        feedback.close()
        report = feedback.report()
        self.assertEqual(sent, [1])
        self.assertEqual(report['snapshot_rpc_cleanup_status'], 'unknown')
        self.assertEqual(report['snapshot_rpc_pending']['dispatch_error_type'], 'RuntimeError')
        self.assertEqual(report['snapshot_rpc_receipts'], [])

    def test_close_does_not_race_a_sampler_still_issuing_its_one_RPC(self):
        import threading
        from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider
        feedback, refs, _ = self.sampler()
        entered, release = threading.Event(), threading.Event()
        original = feedback.owner._actors.model_actor.snapshot.remote
        def blocked_remote():
            entered.set()
            if not release.wait(2):raise TimeoutError('fixture remote was not released')
            return original()
        feedback.owner._actors.model_actor.snapshot.remote = blocked_remote
        feedback.provider = NonBlockingMetricsObservationProvider(feedback.sample,
            poll_interval_s=.01, close_timeout_s=0)
        self.addCleanup(release.set)
        self.addCleanup(feedback.provider.close)
        self.assertTrue(entered.wait(2))
        feedback.close()
        before = feedback.report()
        self.assertFalse(before['sampler_stopped'])
        self.assertEqual(before['snapshot_rpc_cleanup_status'], 'unknown')
        self.assertEqual(before['snapshot_rpc_pending']['request_index'], 1)
        release.set(); feedback.provider._thread.join(2)
        self.assertFalse(feedback.provider.is_running)
        self.assertEqual(len(refs), 1)
        refs[0][1].set_result(self.frame())
        feedback.close()
        self.assertEqual(feedback.report()['snapshot_rpc_cleanup_status'], 'closed')
        self.assertEqual(before['snapshot_rpc_cleanup_status'], 'unknown')

    def test_late_future_terminal_during_report_keeps_one_ownership_snapshot(self):
        from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider
        def line_of(method, text):
            lines, start = inspect.getsourcelines(method)
            return start+next(i for i, line in enumerate(lines) if text in line)
        for pause_at in ("rpc_terminal_status='unknown'", 'complete ='):
            with self.subTest(pause_at=pause_at):
                feedback, refs, _ = self.sampler()
                feedback.sample()
                entered, release = threading.Event(), threading.Event()
                poll_line = line_of(feedback._consume_snapshot, 'done = self.future.done()')
                report_line = line_of(feedback.report, pause_at)
                def worker_trace(frame, event, arg):
                    if (event == 'line' and frame.f_code is feedback._consume_snapshot.__func__.__code__
                            and frame.f_lineno == poll_line):
                        entered.set()
                        if not release.wait(3):raise TimeoutError('fixture sampler not released')
                    return worker_trace
                old_trace = threading.gettrace()
                threading.settrace(worker_trace)
                try:
                    feedback.provider = NonBlockingMetricsObservationProvider(feedback.sample,
                        poll_interval_s=.01, close_timeout_s=0)
                finally:
                    threading.settrace(old_trace)
                paused = False
                def report_trace(frame, event, arg):
                    nonlocal paused
                    if (event == 'line' and frame.f_code is feedback.report.__func__.__code__
                            and frame.f_lineno == report_line and not paused):
                        paused = True
                        refs[0][1].set_result(self.frame())
                        release.set()
                        feedback.provider._thread.join(3)
                        self.assertFalse(feedback.provider.is_running)
                    return report_trace
                old_trace = sys.gettrace()
                try:
                    self.assertTrue(entered.wait(3))
                    feedback.close()
                    self.assertTrue(feedback.provider.is_running)
                    self.assertFalse(refs[0][1].done())
                    sys.settrace(report_trace)
                    before = feedback.report()
                    sys.settrace(old_trace)
                    self.assertTrue(paused)
                    self.assertEqual(before['snapshot_requests'], 1)
                    self.assertEqual(before['snapshot_rpc_receipts'], [])
                    self.assertEqual(before['snapshot_rpc_pending']['request_index'], 1)
                    self.assertEqual(before['snapshot_rpc_cleanup_status'], 'unknown')
                    after = feedback.report()
                    self.assertEqual(after['snapshot_rpc_cleanup_status'], 'closed')
                    self.assertIsNone(after['snapshot_rpc_pending'])
                    self.assertEqual([r['request_index'] for r in after['snapshot_rpc_receipts']], [1])
                finally:
                    sys.settrace(old_trace)
                    release.set()
                    feedback.provider.close()
                    feedback.provider._thread.join(3)

    def test_source_clock_overflow_keeps_known_RPC_terminal_with_unknown_metadata(self):
        feedback, refs, _ = self.sampler()
        feedback.sample()
        frame = self.frame()
        frame['ipc']['native_feedback']['observed_at_s'] = 10**1000
        refs[0][1].set_result(frame)
        feedback.close()
        report = feedback.report()
        row, = report['snapshot_rpc_receipts']
        self.assertEqual(row['rpc_terminal_status'], 'returned')
        self.assertIsNone(row['response_source_observed_at_s'])
        self.assertEqual(row['response_source_observed_at_error_type'], 'OverflowError')
        self.assertIsNone(feedback.ref)
        json.dumps(report, allow_nan=False)

    def test_issue_receipt_clock_failure_keeps_the_single_RPC_accounted_for(self):
        from unittest.mock import patch
        from src.execution_provider.adapters import lotus_chain_token_batches as batching
        feedback, refs, _ = self.sampler()
        monotonic, attempts = time.monotonic, []
        def clock():
            attempts.append(1)
            if len(attempts) == 1:raise RuntimeError('fixture issue timestamp unavailable')
            return monotonic()
        with patch.object(batching, 'time', SimpleNamespace(monotonic=clock)):
            feedback.sample()
        self.assertEqual(len(refs), 1)
        refs[0][1].set_result(self.frame()); feedback.close()
        report = feedback.report()
        row, = report['snapshot_rpc_receipts']
        self.assertEqual(report['snapshot_requests'], 1)
        self.assertIsNone(report['snapshot_rpc_pending'])
        self.assertIsNone(row['issued_at_s'])
        self.assertEqual(row['issued_at_error_type'], 'RuntimeError')
        self.assertEqual(row['rpc_terminal_status'], 'returned')

    def _owner_close_with_failed_receipt_clock(self, *, cached):
        from unittest.mock import patch
        from src.execution_provider.adapters import lotus_chain_token_batches as batching
        from src.execution_provider.adapters import lotus_record_chain_core as owner_module
        feedback, refs, _ = self.sampler()
        feedback.sample(); refs[0][1].set_result(self.frame())
        owner = object.__new__(owner_module.SemLoomRecordChainCore)
        owner._supply_feedback = feedback
        prior = dict(status='closed', calls=[dict(status='unknown')]) if cached else None
        owner.last_close_report, owner._active, owner._actors = prior, None, feedback.owner._actors
        owner.transport = owner.core = owner.capacity = owner.deadline = None
        class CleanupError(Exception):pass
        cleanup_error = CleanupError('fixture original cleanup error')
        def unavailable_clock():raise RuntimeError('fixture receipt timestamp unavailable')
        with patch.object(batching, 'time', SimpleNamespace(monotonic=unavailable_clock)), \
                patch.object(owner_module, 'close_prepared_core', side_effect=cleanup_error) as cleanup:
            if cached:
                result = owner._close_record_local()
                cleanup.assert_not_called()
                self.assertEqual(result['calls'], prior['calls'])
                self.assertNotIn('batch_supply_feedback', prior)
            else:
                with self.assertRaises(CleanupError) as caught:owner._close_record_local()
                self.assertIs(caught.exception, cleanup_error)
                cleanup.assert_called_once()
        row, = feedback.report()['snapshot_rpc_receipts']
        self.assertEqual(row['rpc_terminal_status'], 'returned')
        self.assertIsNone(row['terminal_observed_at_s'])
        self.assertEqual(row['terminal_observed_at_error_type'], 'RuntimeError')
        self.assertIsNone(feedback.ref)

    def test_receipt_clock_failure_does_not_break_cached_owner_close(self):
        self._owner_close_with_failed_receipt_clock(cached=True)

    def test_receipt_clock_failure_still_runs_cleanup_and_preserves_its_exception(self):
        self._owner_close_with_failed_receipt_clock(cached=False)

    def test_receipt_clock_failure_preserves_terminal_RPC_exception(self):
        from unittest.mock import patch
        from src.execution_provider.adapters import lotus_chain_token_batches as batching
        feedback, refs, _ = self.sampler()
        feedback.sample(); refs[0][1].set_exception(ValueError('fixture RPC exception'))
        def unavailable_clock():raise RuntimeError('fixture receipt timestamp unavailable')
        with patch.object(batching, 'time', SimpleNamespace(monotonic=unavailable_clock)):
            feedback.close()
        row, = feedback.report()['snapshot_rpc_receipts']
        self.assertEqual((row['rpc_terminal_status'], row['rpc_error_type']), ('raised', 'ValueError'))
        self.assertEqual(row['terminal_observed_at_error_type'], 'RuntimeError')
        self.assertIsNone(feedback.ref)


class SnapshotRPCEntryTests(unittest.TestCase):
    def test_normal_Movie_CLI_persists_control_receipts_without_changing_model_calls(self):
        from contextlib import redirect_stdout
        import io
        from unittest.mock import patch
        from src.baselines.text.movie_records import MovieRecordSource
        from src.experiments import lotus_records_movie as movie
        from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
        from tests.experiments.test_movie_async_token_entry import AsyncTokenEntryTests
        from tests.serving.test_native_resource_feedback import CPUFeedbackProcess
        helper = AsyncTokenEntryTests(); helper.setUp(); self.addCleanup(helper.doCleanups)
        helper.runtime()
        source = MovieRecordSource.from_manifest(helper.helper.manifests['movie-q1'])
        helper.model.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text=str(r['source_position']==0)) for r in source.rows()})))
        config = dict(helper.config(), batch_concurrency=1,
            batch_feedback=dict(max_batches=2, options=dict(poll_interval_s=.002, window_s=.05)))
        path = helper.save(config); output = helper.root/'snapshot-rpc-entry'; output.mkdir()
        with patch.object(AsyncCoreModelActor, '_process_type', return_value=CPUFeedbackProcess), redirect_stdout(io.StringIO()):
            self.assertEqual(movie.main(['run', '--source-manifest', str(source.manifest_path),
                '--sembench-checkout', str(helper.root), '--config', str(path), '--output-dir', str(output),
                '--execute-model', '--worker']), 0)
        report = json.loads((output/'query.json').read_text())
        self.assertEqual((report['status'], report['result']), ('completed', {'reviewId':['1']}))
        self.assertEqual(report['generation'], json.loads(json.dumps(config['generation'])))
        self.assertEqual(report['calls']['counts']['succeeded'], 2)
        self.assertEqual(report['owner_close']['core_usage']['held_tasks'], 0)
        feedback = report['owner_close']['batch_supply_feedback']
        self.assertTrue(feedback['sampler_stopped'])
        self.assertGreater(feedback['snapshot_requests'], 0)
        receipts = feedback['snapshot_rpc_receipts']
        self.assertEqual(len(receipts)+int(feedback['snapshot_rpc_pending'] is not None), feedback['snapshot_requests'])
        self.assertEqual(len({r['request_index'] for r in receipts}), len(receipts))
        self.assertTrue(all(r['future_done_confirmed'] for r in receipts))
        self.assertTrue(all(r['rpc_terminal_status'] in ('returned', 'raised') for r in receipts))
        if feedback['snapshot_rpc_pending'] is not None:
            self.assertEqual(feedback['snapshot_rpc_cleanup_status'], 'unknown')
            self.assertFalse(feedback['snapshot_rpc_terminal_evidence_complete'])


if __name__ == '__main__':unittest.main()
