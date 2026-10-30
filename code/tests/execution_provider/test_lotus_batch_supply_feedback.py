"""Original Core/method/actor/IPC with an explicit CPU-only inference fixture."""
import json
import sys
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


class BatchSupplyIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.helper = batch_fixture.AsyncTokenBatchTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)

    def test_cpu_native_feedback_reaches_existing_organizer_and_preserves_results(self):
        outputs = []
        prompts = []
        options = ResourceFeedbackConfig(poll_interval_s=.002, window_s=.05,
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
                    self.assertGreater(feedback['phases'][0]['changes'], 0, feedback)
                    phase = feedback['phases'][0]
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
