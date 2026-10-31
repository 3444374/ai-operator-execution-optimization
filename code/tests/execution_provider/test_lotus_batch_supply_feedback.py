"""Original Core/method/actor/IPC with an explicit CPU-only inference fixture."""
import json
import sys
import time
from concurrent.futures import Future
from contextlib import contextmanager
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig
from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig
from tests.execution_provider import test_lotus_async_token_batches as batch_fixture
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.execution_provider import test_lotus_record_local_query as query_fixture
from tests.semantic_methods import test_lotus_chain_composition as fixture
from tests.semantic_methods import test_lotus_chain_relation_handoff as relation
from tests.serving.test_native_resource_feedback import CPUFeedbackProcess


@contextmanager
def logical_feedback_frontier():
    """Real snapshot RPC/CPU IPC, logical time at busy Core/native frontiers.

    No sleeps or OS-latency threshold decides whether useful work survives long
    enough for a trial. The separate wall-clock test retains the real sampler.
    """
    from src.execution_provider.adapters import lotus_chain_token_batches as batches
    from src.scheduling.core.models import AdmissionObservation
    from src.scheduling.submission_control.stage_work import SupplyFeedbackController
    now = [0.]

    class Frontier:
        def __init__(self, sampler, **kwargs):
            self.sampler, self.enabled, self.is_running = sampler, True, True
            self.sequence, self.points = None, 0

        def latest(self, inflight):
            snapshot = self.sampler()
            # Sample only a busy Core frontier in this actuation fixture.
            # Between completing and dispatching batches, OS ordering otherwise
            # chooses whether a native point clears the partial measurement.
            fresh = (inflight > 0 and snapshot is not None
                     and snapshot.source_sequence != self.sequence)
            if fresh:
                self.sequence = snapshot.source_sequence
                self.points += 1
                # Deterministically exercise recovery before further trials.
                now[0] += 2 if self.points == 2 else 1
            return AdmissionObservation(now[0], fresh, inflight,
                None if snapshot is None else snapshot.running,
                None if snapshot is None else snapshot.waiting,
                None if snapshot is None else snapshot.kv_usage,
                sample_age_s=None if snapshot is None else 0,
                preemptions_total=None if snapshot is None else snapshot.preemptions_total)

        def pause(self):self.enabled = False
        def resume(self):self.enabled = True
        def close(self):self.is_running = False

    original = batches.BatchSupplyFeedback.update
    def at_frontier(feedback, stream):
        feedback.next_poll = 0  # Poll at Core frontiers; only new samples advance time.
        return original(feedback, stream)
    with patch.object(batches, 'time', SimpleNamespace(monotonic=lambda: now[0])), \
            patch('src.scheduling.submission_control.stage_work.SupplyFeedbackController',
                  lambda config: SupplyFeedbackController(config, clock=lambda: now[0])), \
            patch('src.scheduling.runtime.observations.NonBlockingMetricsObservationProvider', Frontier), \
            patch.object(batches.BatchSupplyFeedback, 'update', at_frontier):
        yield


class SnapshotOwnershipTests(unittest.TestCase):
    def test_supply_snapshot_audit_has_identity_and_exact_rejection_reasons(self):
        for mutation,reason in [({},'RPC_sample_valid'),({'engine_uid':'b'*32},'engine_uid_mismatch'),
                                ({'observed_at_s':-1},'source_predates_query')]:
            feedback,refs=self.sampler();feedback.owner._actors.model_ready={'engine_uid':'a'*32};events=[]
            feedback.provider=SimpleNamespace(observe_native=lambda native,**context:events.append((native,context)))
            feedback.sample()
            native=dict(engine_uid='a'*32,sequence=1,observed_at_s=1.,running=1,waiting=0,
                kv_usage=.1,generation_tokens_total=2,preemptions_total=0);native.update(mutation)
            refs[0][1].set_result(dict(session_id=1,ipc=dict(native_feedback=native)))
            value=feedback.sample();self.assertEqual(events[-1][1]['reason'],reason)
            self.assertEqual(events[-1][1]['accepted'],reason=='RPC_sample_valid')
            if value is not None:
                self.assertEqual((value.source_uid,value.source_session_id),('a'*32,1))
                self.assertIsNotNone(value.received_at_s)
            else:self.assertNotEqual(reason,'RPC_sample_valid')

    def test_supply_audit_clock_or_callback_failure_cannot_discard_valid_snapshot(self):
        feedback,refs=self.sampler();feedback.owner._actors.model_ready={'engine_uid':'a'*32}
        def fail(*args,**kw):raise RuntimeError('CPU observer failure')
        feedback.provider=SimpleNamespace(observe_native=fail);feedback.sample()
        refs[0][1].set_result(dict(session_id=1,ipc=dict(native_feedback=dict(engine_uid='a'*32,
            sequence=1,observed_at_s=1.,running=1,waiting=0,kv_usage=.1,generation_tokens_total=2,preemptions_total=0))))
        # Fail only the receive-audit clock. Terminal receipt clock failures
        # have their own explicit metadata/error accounting tests.
        with patch('src.execution_provider.adapters.lotus_chain_token_batches.time.monotonic',
                   side_effect=[RuntimeError('CPU observer clock failure'), time.monotonic()]):
            value=feedback.sample()
        self.assertIsNotNone(value);self.assertIsNone(value.received_at_s);self.assertEqual(feedback.errors,0)

    def sampler(self, *, cancel_error=True, future_error=False):
        from src.execution_provider.adapters.lotus_chain_token_batches import BatchSupplyFeedback
        refs = []
        def remote():
            future = Future()
            attempts = []
            def read_future():
                attempts.append(1)
                if future_error and len(attempts) == 1:
                    raise RuntimeError('fixture future unavailable')
                return future
            ref = SimpleNamespace(future=read_future)
            refs.append((ref, future))
            return ref
        def cancel(ref, **kwargs):
            if cancel_error:
                raise RuntimeError('fixture cancellation transport error')
        owner = SimpleNamespace(_actors=SimpleNamespace(model_actor=SimpleNamespace(
            snapshot=SimpleNamespace(remote=remote)), ray=SimpleNamespace(cancel=cancel)))
        feedback = BatchSupplyFeedback(owner, SupplyFeedbackConfig(4))
        feedback.session_id, feedback.started = 1, 0.
        return feedback, refs

    def test_review_cancel_error_retains_one_unsettled_snapshot(self):
        feedback, refs = self.sampler()
        clock = [0.]
        with patch('src.execution_provider.adapters.lotus_chain_token_batches.time.monotonic', lambda: clock[0]):
            for second in (0., 2., 3., 5., 6.):
                clock[0] = second
                self.assertIsNone(feedback.sample())
        self.assertEqual((len(refs), feedback.requests, feedback.errors), (1, 1, 1))
        self.assertFalse(refs[0][1].done())
        self.assertIs(feedback.ref, refs[0][0])
        # Only a real terminal future allows a subsequent RPC. Late/expired
        # snapshots cannot be treated as fresh native data.
        refs[0][1].set_result({})
        self.assertIsNone(feedback.sample())
        self.assertIsNone(feedback.ref)
        self.assertIsNone(feedback.sample())
        self.assertEqual(len(refs), 2)

    def test_future_acquisition_error_keeps_the_already_issued_rpc(self):
        feedback, refs = self.sampler(future_error=True)
        self.assertIsNone(feedback.sample())
        self.assertIsNone(feedback.sample())
        self.assertEqual(len(refs), 1)
        self.assertIs(feedback.ref, refs[0][0])
        refs[0][1].set_exception(RuntimeError('fixture snapshot failed'))
        self.assertIsNone(feedback.sample())
        self.assertIsNone(feedback.ref)

    def test_successful_cancel_request_also_waits_for_terminal_confirmation(self):
        feedback, refs = self.sampler(cancel_error=False)
        clock = [0.]
        with patch('src.execution_provider.adapters.lotus_chain_token_batches.time.monotonic', lambda: clock[0]):
            feedback.sample()
            for second in (2., 3., 5.):
                clock[0] = second
                feedback.sample()
        self.assertEqual(len(refs), 1)
        self.assertFalse(refs[0][1].done())
        self.assertIs(feedback.ref, refs[0][0])


class BatchSupplyIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.helper = batch_fixture.AsyncTokenBatchTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)

    def test_cpu_native_feedback_reaches_existing_organizer_and_preserves_results(self):
        with logical_feedback_frontier():
            self.feedback_comparison(logical=True)

    def test_wallclock_native_sampling_preserves_results_and_cleanup(self):
        self.feedback_comparison(logical=False)

    def feedback_comparison(self, *, logical):
        outputs = []
        prompts = []
        options = ResourceFeedbackConfig(poll_interval_s=.002, window_s=1 if logical else .05,
            min_samples=1, min_remaining=0, allow_exploration=True)
        for enabled in (False, True):
            plan, rows = fixture.chain_for('M'), fixture.source_rows(size=40)
            self.helper.helper.config_for(plan, rows)
            with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=CPUFeedbackProcess):
                owner = self.helper.owner(plan, R=8, C=1, B=256,
                    batch_feedback=SupplyFeedbackConfig(3, options) if enabled else None)
                result = list(owner.evaluate(iter(rows), plan, expected=rows,
                    columns=('passage',), query_id=plan.query_id))
                outputs.append(result)
                prompts.append(sorted((c['algorithm_stage'], c['source_position'], c['messages_sha256'])
                                      for c in owner.calls))
                report = owner.last_close_report
                self.assertEqual(report['status'], 'closed')
                self.assertEqual(len(owner.calls), 40)
                self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                self.assertEqual(owner.transport.retained_refs(), 0)
                if enabled:
                    feedback = report['batch_supply_feedback']
                    self.assertTrue(feedback['sampler_stopped'])
                    self.assertGreater(feedback['snapshot_requests'], 0)
                    self.assertEqual(feedback['sample_errors'], 0)
                    self.assertEqual(feedback['fixed_B'], 256)
                    self.assertTrue(feedback['phases'][0]['demand_windows'])
                    phase = feedback['phases'][0]
                    if logical:
                        self.assertGreater(phase['changes'], 0, feedback)
                        self.assertEqual(phase['feedback_gaps'], 1, feedback)
                        self.assertNotEqual(phase['stop_reason'], 'feedback_gap')
                    self.assertEqual([(r['at_s'], r['window']) for r in phase['history']],
                                     [(r['at_s'], r['window']) for r in phase['demand_changes']])
                    self.assertLessEqual(report['async_token_batches']['peak_batches'], 3)
                else:self.assertNotIn('batch_supply_feedback', report)
        self.assertEqual(outputs[0], outputs[1])
        self.assertEqual(prompts[0], prompts[1])

    def test_missing_native_feedback_holds_one_without_extra_model_calls(self):
        plan, rows = fixture.chain_for('M'), fixture.source_rows(size=8)
        self.helper.helper.config_for(plan, rows)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            owner = self.helper.owner(plan, R=4, C=1, B=256, batch_feedback=SupplyFeedbackConfig(3))
            list(owner.evaluate(iter(rows), plan, expected=rows, columns=('passage',), query_id=plan.query_id))
            report = owner.last_close_report
            self.assertEqual(len(owner.calls), 8)
            self.assertEqual(report['async_token_batches']['concurrency'], 1)
            self.assertEqual(report['batch_supply_feedback']['phases'][0]['changes'], 0)
            self.assertEqual(owner.core.capacity.usage().held_tasks, 0)

    def test_successors_and_extract_barrier_match_fixed_reference(self):
        for pattern in ('MMF', 'EM'):
            outputs = []
            for enabled in (False, True):
                plan = relation.relation_chain(pattern)
                rows = fixture.source_rows(size=3)
                self.helper.helper.config_for(plan, rows)
                with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
                    owner = self.helper.owner(plan, R=4, C=1, B=256,
                        batch_feedback=SupplyFeedbackConfig(3) if enabled else None)
                    outputs.append(list(owner.evaluate(iter(rows), plan, expected=rows,
                        columns=('passage',), query_id=plan.query_id)))
                    self.assertEqual(owner.last_close_report['status'], 'closed')
                    self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                    if enabled:
                        phases = owner.last_close_report['batch_supply_feedback']['phases']
                        self.assertEqual(len(phases), 2 if pattern == 'EM' else 1)
            self.assertEqual(outputs[0], outputs[1])

    def test_empty_input_does_not_start_model_or_snapshot_sampler(self):
        plan = fixture.chain_for('M')
        with ThreadedRuntime([]) as runtime:
            owner = self.helper.owner(plan, R=4, C=1, B=256, batch_feedback=SupplyFeedbackConfig(3))
            self.assertEqual(list(owner.evaluate(iter(()), plan, expected=[],
                columns=('passage',), query_id=plan.query_id)), [])
            self.assertEqual(runtime.async_actors, [])
            self.assertEqual(owner.last_close_report['batch_supply_feedback']['snapshot_requests'], 0)

    def test_existing_ordinary_factory_forwards_optional_feedback_and_checks_members(self):
        from src.execution_provider.adapters.lotus_records_core import create_records_core
        from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        from tests.execution_provider.test_lotus_records_core import Source
        from tests.execution_provider.chain_tokenizer_fixture import tokenizer
        class FiniteSource(Source):
            def rows(self):return iter(self.raw)
        path = self.helper.helper.config.parent/'tiny-tokenizer'
        tokenizer().save_pretrained(path)
        engine = EngineSpec(str(self.helper.helper.config), sys.executable, sys.executable,
            tokenizer_path=str(path), max_model_len=4096, max_num_seqs=2)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=4, startup_timeout_s=8, query_timeout_s=12)
        from src.semantic_methods.lotus.prepared_batching import PreparedBatching
        from src.semantic_methods.lotus import engine as methods
        formatter, binder = methods.LotusFilterMethod, methods.bind_filter_method
        rows = [dict(row_id='r'+str(i), source_position=i, reviewId='r'+str(i),
                     id='taken_3', reviewText='fixture') for i in range(8)]
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=CPUFeedbackProcess), \
                patch.object(methods, 'LotusFilterMethod', formatter), patch.object(methods, 'bind_filter_method', binder):
            core = create_records_core(FilterMethod('openai/cpu-fixture', 'Evaluate {reviewText}'),
                engine, GenerationSpec(max_tokens=8), resources, profile=dict(mode='async', max_inflight=4),
                prepared_batching=PreparedBatching('token_budget', None, 256), batch_concurrency=1,
                batch_feedback=SupplyFeedbackConfig(3))
            values = list(core.evaluate(FiniteSource(rows), query_id='ordinary-feedback'))
            self.assertEqual(len(values), 8)
            report = core.last_close_report
            self.assertEqual(report['status'], 'closed')
            self.assertEqual(report['record_batching']['feedback']['max_batches'], 3)
            self.assertEqual(report['batch_supply_feedback']['initial_batches'], 1)
            self.assertEqual(sum(len(b['receipt_members']) for b in report['transport_batches']), 8)
            self.assertTrue(report['batch_supply_feedback']['sampler_stopped'])

    def test_configuration_rejected_before_model_or_source(self):
        plan, rows = fixture.chain_for('M'), fixture.source_rows(size=1)
        for C, maximum in [(2, 3), (1, 5)]:
            with self.assertRaises(ValueError):
                self.helper.owner(plan, R=4, C=C, B=256,
                                  batch_feedback=SupplyFeedbackConfig(maximum))

    def test_unknown_keeps_original_outcome_and_cleanup_ownership(self):
        plan, rows = fixture.chain_for('M'), fixture.source_rows(size=3)
        config = self.helper.helper.config_for(plan, rows)
        next(iter(config['responses'].values()))['status'] = 'unknown'
        self.helper.helper.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            owner = self.helper.owner(plan, R=4, C=1, B=256, batch_feedback=SupplyFeedbackConfig(3))
            with self.assertRaises(RuntimeError):
                list(owner.evaluate(iter(rows), plan, expected=rows,
                     columns=('passage',), query_id=plan.query_id))
            self.assertTrue(any(v['status'] == 'unknown' for v in owner.calls))
            self.assertTrue(owner.last_close_report['batch_supply_feedback']['sampler_stopped'])
            self.assertEqual(owner.core.capacity.usage().active_requests, 0)
            self.assertFalse(owner.last_close_report['query_completed'])

    def test_existing_core_sink_is_composed_not_replaced(self):
        from src.scheduling.core.session import SessionEngine
        seen = []
        original = SessionEngine.__init__
        def instrument(core, *args, **kwargs):
            original(core, *args, **kwargs)
            core.sink = lambda action, key: seen.append((action, key))
        plan, rows = fixture.chain_for('M'), fixture.source_rows(size=3)
        self.helper.helper.config_for(plan, rows)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process), \
                patch.object(SessionEngine, '__init__', instrument):
            owner = self.helper.owner(plan, R=4, C=1, B=256, batch_feedback=SupplyFeedbackConfig(3))
            list(owner.evaluate(iter(rows), plan, expected=rows, columns=('passage',), query_id=plan.query_id))
            self.assertEqual(sum(action == 'submitted' for action, _ in seen), 3)
            self.assertEqual(sum(action == 'terminal' for action, _ in seen), 3)
            self.assertEqual(sum(action == 'released' for action, _ in seen), 3)

    def test_cancel_while_members_are_held_stops_sampler_and_retires_original_credits(self):
        plan, rows = fixture.chain_for('M'), fixture.source_rows(size=3)
        held = [plan.stages[0].stage_id+'|'+row.row_id for row in rows]
        self.helper.helper.config_for(plan, rows, hold=held)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            owner = self.helper.owner(plan, R=4, C=1, B=256, batch_feedback=SupplyFeedbackConfig(3))
            def cancelled():
                return owner.core is not None and owner.core.capacity.usage().active_requests > 0
            with self.assertRaises(InterruptedError):
                list(owner.evaluate(iter(rows), plan, expected=rows, columns=('passage',),
                    query_id=plan.query_id, cancelled=cancelled))
            self.assertTrue(owner.last_close_report['batch_supply_feedback']['sampler_stopped'])
            self.assertFalse(owner.last_close_report['query_completed'])
            self.assertEqual(owner.core.capacity.usage().active_requests, 0)

    def test_invalid_metric_clock_is_unknown_without_changing_business_result(self):
        from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider
        plan, rows = fixture.chain_for('M'), fixture.source_rows(size=3)
        self.helper.helper.config_for(plan, rows)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process), \
                patch.object(NonBlockingMetricsObservationProvider, 'latest', side_effect=ValueError('invalid metric clock')):
            owner = self.helper.owner(plan, R=4, C=1, B=256, batch_feedback=SupplyFeedbackConfig(3))
            self.assertEqual(len(list(owner.evaluate(iter(rows), plan, expected=rows,
                columns=('passage',), query_id=plan.query_id))), 3)
            report = owner.last_close_report
            self.assertEqual(report['status'], 'closed')
            self.assertGreater(report['batch_supply_feedback']['sample_errors'], 0)
            self.assertEqual(report['batch_supply_feedback']['phases'][0]['changes'], 0)


if __name__ == '__main__':unittest.main()
