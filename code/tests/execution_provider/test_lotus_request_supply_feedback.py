"""Request actuation and real Core/CPU IPC; no resource-benefit claim."""
from dataclasses import replace
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_chain_token_batches as batches
from src.scheduling.core.session_contract import Acceptance, BatchSubmission
from src.scheduling.submission_control.stage_work import default_supply_feedback_options, RequestSupplyFeedbackController
from src.serving.backends.async_engine_process import AsyncEngineProcess
from tests.execution_provider import test_lotus_async_token_batches as base
from tests.execution_provider import test_lotus_token_rolling_refill as rolling
from tests.execution_provider.test_lotus_batch_supply_feedback import logical_feedback_frontier
from tests.execution_provider import test_lotus_feedback_ready_collection as ready
from tests.execution_provider import test_lotus_snapshot_rpc_receipts as rpc


class CPUWorkProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        return [sys.executable, str(Path(__file__).with_name('request_feedback_child_fixture.py')),
                *map(str, descriptors)]


class RequestRollingTests(unittest.TestCase):
    setUp = rolling.RollingRefillTests.setUp
    tick_until = rolling.RollingRefillTests.tick_until

    def owner(self, plan, **options):
        return base.AsyncTokenBatchTests.owner(self, plan, C=None, rolling_refill=True,
            request_feedback=options.pop('request_feedback', default_supply_feedback_options()), **options)

    def actuate(self, owner, value):
        # Isolate the admission mechanism. Policy and single-RPC tests below
        # separately exercise actual observation-driven decisions.
        feedback = owner._supply_feedback
        if feedback.controller is None:
            bind = feedback.bind
            def bound(stream):
                bind(stream)
                feedback.bind = bind
                self.actuate(owner, value)
            feedback.bind = bound
        else:
            feedback.next_poll = float('inf')
            feedback._set_window(value)

    def test_grow_and_shrink_stop_new_admission_without_cancelling_owned_requests(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=6)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        for i in range(3):config['responses']['step_0|occ-'+str(i)]['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=4)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: owner.core.capacity.usage().active_requests == 1)
                self.assertEqual(owner._token_organizer.request_window, 1)
                self.actuate(owner, 3)
                self.tick_until(stream, lambda: owner.core.capacity.usage().active_requests == 3)
                self.actuate(owner, 1)
                for _ in range(4):stream._tick()
                self.assertEqual(owner.core.capacity.usage().active_requests, 3)
                self.assertFalse(any(e['event']=='cancel' for e in self.helper.events()))
                gate.touch()
                self.assertEqual(len(list(stream)), 6)
                stats = owner.last_close_report['async_token_batches']
                self.assertEqual((stats['concurrency'], stats['C_request'], stats['request_limit']), (None, 1, 4))
                self.assertEqual(stats['peak_requests'], 3)
                self.assertTrue(all(b['requests_before']+b['members'] <= b['C_request'] for b in stats['recent_batches']))
                self.assertEqual(owner.last_close_report['core_usage']['held_tasks'], 0)
            finally:
                gate.touch(); owner.close(timeout=3)

    def test_fast_successor_refills_before_slow_peer_with_dynamic_controller_installed(self):
        plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=4)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=3)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.actuate(owner, 2)
                self.tick_until(stream, lambda: any(c['algorithm_stage']=='step_1' and c['row_id']=='occ-1'
                    and c['status']=='succeeded' for c in owner.calls))
                self.assertFalse(any(e['event']=='end' and e['token']=='step_0|occ-0' for e in self.helper.events()))
                self.assertEqual(owner._token_organizer.peak_requests, 2)
                gate.touch()
                self.assertEqual(len(list(stream)), 4)
                self.assertTrue(owner.last_close_report['query_completed'])
            finally:
                gate.touch(); owner.close(timeout=3)

    def test_rejected_atomic_selection_drains_after_shrink_without_deadlock_or_new_admission(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=5)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        for i in range(2):config['responses']['step_0|occ-'+str(i)]['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=3)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.actuate(owner, 2)
                with patch.object(owner.transport, 'try_submit_batch', return_value=BatchSubmission(Acceptance.NOT_ACCEPTED)):
                    self.tick_until(stream, lambda: len(stream.session._pending_members) == 2 if stream.session is not None else False)
                    self.assertEqual(len(stream.session._pending_members), 2)
                    self.assertEqual(owner.core.capacity.usage().active_requests, 0)
                    owner._supply_feedback.next_poll = 0
                    owner._supply_feedback.update(stream)
                    state, = owner._supply_feedback.controller.report()['demand_windows']
                    self.assertEqual(state['local_capacity']['pending_admission_requests'], 2)
                    self.assertEqual(state['local_capacity']['charged_requests'], 2)
                self.actuate(owner, 1)
                self.tick_until(stream, lambda: owner.core.capacity.usage().active_requests == 2)
                for _ in range(4):stream._tick()
                self.assertEqual(len(owner._token_organizer.history), 1)
                gate.touch()
                self.assertEqual(len(list(stream)), 5)
                self.assertEqual(owner.last_close_report['async_token_batches']['peak_requests'], 2)
                self.assertEqual(owner.last_close_report['core_usage']['held_tasks'], 0)
            finally:
                gate.touch(); owner.close(timeout=3)

    def test_observation_counts_requests_separately_from_batches_and_R(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=4)
        self.helper.config_for(plan, rows, hold=['step_0|occ-0', 'step_0|occ-1'])
        from src.scheduling.core.models import AdmissionObservation
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=4)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.actuate(owner, 2)
                self.tick_until(stream, lambda: owner.core.capacity.usage().active_requests == 2)
                seen = []
                def latest(inflight):
                    seen.append(inflight)
                    return AdmissionObservation(batches.time.monotonic(), False, inflight, None, None, None)
                with patch.object(owner._supply_feedback.provider, 'latest', latest):
                    owner._supply_feedback.next_poll = 0
                    owner._supply_feedback.update(stream)
                self.assertEqual(seen, [2])
                state, = owner._supply_feedback.controller.report()['demand_windows']
                self.assertEqual((state['active_requests'], state['active_batches']), (2, 1))
                self.assertEqual(state['local_capacity']['limits']['active_requests'], 4)
            finally:
                owner.close(timeout=3)

    def test_missing_native_feedback_retains_one_and_preserves_original_relations(self):
        for pattern in ('MFEM', 'EM'):
            results = []
            for enabled in (False, True):
                plan, rows = base.relation.relation_chain(pattern), base.fixture.source_rows(size=3)
                self.helper.config_for(plan, rows)
                with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
                    owner = self.owner(plan, R=3, request_feedback=default_supply_feedback_options() if enabled else None)
                    results.append(list(owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)))
                    self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                    if enabled:
                        f = owner.last_close_report['request_supply_feedback']
                        self.assertTrue(f['sampler_stopped'])
                        self.assertTrue(all(p['changes']==0 and p['control_unit']=='single_model_requests' for p in f['phases']))
                        self.assertEqual(owner._token_organizer.peak_requests, 1)
                        self.assertTrue(all(p['source_exhausted'] for p in owner.last_close_report['phase_history']))
                    else:self.assertNotIn('request_supply_feedback', owner.last_close_report)
            self.assertEqual(results[0], results[1])

    def test_cancel_unknown_and_failure_keep_original_result_and_retirement_semantics(self):
        for mode in ('cancel', 'unknown', 'failed'):
            with self.subTest(mode=mode):
                plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=3)
                config = self.helper.config_for(plan, rows, hold=['step_0|occ-0'] if mode=='cancel' else [])
                if mode!='cancel':config['responses']['step_0|occ-0']['status'] = mode
                self.helper.config.write_text(json.dumps(config))
                with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
                    owner = self.owner(plan, R=3)
                    def cancelled():return mode=='cancel' and owner.core is not None and owner.core.capacity.usage().active_requests > 0
                    try:
                        with self.assertRaises(InterruptedError if mode=='cancel' else RuntimeError):
                            list(owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id, cancelled=cancelled))
                        report = owner.close(timeout=3)
                        self.assertEqual(report['status'], 'closed')
                        self.assertEqual(report['core_usage']['held_tasks'], 0)
                        self.assertTrue(report['request_supply_feedback']['sampler_stopped'])
                        self.assertFalse(any(c['algorithm_stage']=='step_1' and c['row_id']=='occ-0' for c in owner.calls))
                        if mode!='cancel':self.assertEqual(sum(c['status']==mode for c in owner.calls), 1)
                    finally:owner.close(timeout=3)

    def test_empty_input_starts_neither_model_nor_snapshot(self):
        plan = base.fixture.chain_for('M')
        with base.ThreadedRuntime([]) as runtime:
            owner = self.owner(plan, R=4)
            self.assertEqual(list(owner.evaluate(iter(()), plan, expected=[], query_id=plan.query_id)), [])
            self.assertEqual(runtime.async_actors, [])
            self.assertEqual(owner.last_close_report['request_supply_feedback']['snapshot_requests'], 0)

    def test_real_CPU_snapshot_path_drives_measured_request_trial_with_logical_frontiers(self):
        outputs = []
        for enabled in (False, True):
            plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=96)
            self.helper.config_for(plan, rows)
            options = replace(default_supply_feedback_options(), poll_interval_s=.002, min_samples=1)
            with logical_feedback_frontier(), \
                    patch('src.scheduling.submission_control.stage_work.RequestSupplyFeedbackController',
                        lambda maximum, config:RequestSupplyFeedbackController(maximum, config, clock=lambda:batches.time.monotonic())), \
                    base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=CPUWorkProcess):
                owner = self.owner(plan, R=8, B=256, request_feedback=options if enabled else None)
                try:
                    outputs.append(list(owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)))
                    if enabled:
                        f = owner.last_close_report['request_supply_feedback']
                        phase, = f['phases']
                        self.assertGreater(phase['changes'], 0, phase)
                        self.assertTrue(any(h['reason']=='performance_probe' for h in phase['history']), phase)
                        self.assertGreater(phase['exploration_wall_s'], 0)
                        self.assertTrue(f['sampler_stopped'])
                        self.assertGreater(f['snapshot_requests'], 0)
                        self.assertLessEqual(owner._token_organizer.peak_requests, 8)
                        self.assertTrue(all(h['requests_before']+h['members'] <= h['C_request'] for h in owner._token_organizer.history))
                        self.assertEqual(phase['control_unit'], 'single_model_requests')
                finally:owner.close(timeout=3)
        self.assertEqual(outputs[0], outputs[1])


class RequestSnapshotTests(ready.FeedbackReadyCollectionTests):
    """Run the existing ready/session/single-RPC contract on request control too."""
    def bound_sampler(self, **kwargs):
        result = super().bound_sampler(request_control=True, **kwargs)
        self.assertIsInstance(result[0], batches.RequestSupplyFeedback)
        return result


class RequestRPCReceiptTests(unittest.TestCase):
    """Keep unknown/terminal ownership contracts on the real request sampler."""
    frame = rpc.SnapshotRPCReceiptTests.frame
    # Remaining ready contracts run above. These selected RPC boundaries have
    # no assumptions about the controlled window or the sampler's unit.
    test_close_collects_terminal = rpc.SnapshotRPCReceiptTests.test_close_collects_an_already_terminal_future_without_cancel
    test_late_after_cancel_error = rpc.SnapshotRPCReceiptTests.test_late_terminal_after_cancel_error_is_collected_on_recheck
    test_source_clock_overflow = rpc.SnapshotRPCReceiptTests.test_source_clock_overflow_keeps_known_RPC_terminal_with_unknown_metadata

    def sampler(self, **options):
        old, refs, cancels = rpc.SnapshotRPCReceiptTests().sampler(**options)
        feedback = batches.RequestSupplyFeedback(old.owner, default_supply_feedback_options())
        feedback.session_id, feedback.started = 1, 0.
        return feedback, refs, cancels
