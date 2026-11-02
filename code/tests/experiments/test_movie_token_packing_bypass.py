"""Public normal/resident entry qualification using only local CPU fixtures."""
import json
import unittest

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_records_core import resolve_record_batching
from src.experiments import lotus_records_movie as movie, movie_records_session as session
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from tests.experiments.movie_token_resident_fixture import MovieTokenResidentFixture, query_close, query_calls
from tests.execution_provider import test_lotus_async_token_batches as batches
from tests.execution_provider import test_lotus_token_rolling_refill as rolling


class TokenPackingBypassEntryTests(unittest.TestCase):
    def test_default_and_explicit_false_resolve_identically_and_zero_B_still_fails(self):
        resolved = dict(mode='async', max_inflight=8, stage_timing=False)
        batching = PreparedBatching('token_budget', None, 4096)
        default = resolve_record_batching(resolved, batching, rolling_refill=True)
        explicit = resolve_record_batching(resolved, batching, rolling_refill=True,
                                          experimental_skip_token_packing=False)
        self.assertEqual(default, explicit)
        self.assertNotIn('experimental_skip_token_packing', default)
        with self.assertRaises(ValueError):
            PreparedBatching('token_budget', None, 0)

    def test_opt_in_rejects_fused_nonrolling_missing_batching_and_nonboolean(self):
        engine = EngineSpec('/tmp/cpu-fixture-model', '/tmp/cpu-model-python', '/tmp/cpu-driver-python')
        resources = RecordsResources()
        for options, error in (
            (dict(prepared_batching=PreparedBatching('token_budget', None, 4096)), ValueError),
            (dict(execution='record_local_sections', prepared_batching=PreparedBatching('token_budget', None, 4096)), ValueError),
            (dict(execution='record_local_sections', rolling_refill=True), ValueError),
            (dict(experimental_skip_token_packing=1), TypeError)):
            with self.subTest(options=options):
                params = dict(experimental_skip_token_packing=True)
                params.update(options)
                with self.assertRaises(error):
                    SemLoomRecordChainCore(engine, resources, **params)
        resolved = dict(mode='async', max_inflight=8, stage_timing=False)
        for options in (dict(), dict(prepared_batching=PreparedBatching('token_budget', None, 4096))):
            with self.subTest(factory=options):
                with self.assertRaises(ValueError):
                    resolve_record_batching(resolved, experimental_skip_token_packing=True, **options)
        with self.assertRaises(TypeError):
            resolve_record_batching(resolved, experimental_skip_token_packing=1)
        with self.assertRaisesRegex(ValueError, 'fixed rolling'):
            resolve_record_batching(dict(resolved, admission={}),
                PreparedBatching('token_budget', None, 4096), rolling_refill=True,
                experimental_skip_token_packing=True)
        from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig
        with self.assertRaisesRegex(ValueError, 'fixed rolling'):
            SemLoomRecordChainCore(engine, resources, execution='record_local_sections', rolling_refill=True,
                prepared_batching=PreparedBatching('token_budget', None, 4096),
                experimental_skip_token_packing=True, request_feedback=ResourceFeedbackConfig())

    def test_normal_movie_config_preflight_retains_positive_B_and_opt_in(self):
        fixture = MovieTokenResidentFixture(self)
        config = fixture.helper.rolling_config()
        config['experimental_skip_token_packing'] = True
        path = fixture.root/'packing-config.json'
        path.write_text(json.dumps(config))
        parsed = movie.read_config(path)[0]
        self.assertIs(parsed['experimental_skip_token_packing'], True)
        options = movie._record_batching_options(parsed)
        self.assertIs(options['experimental_skip_token_packing'], True)
        self.assertGreater(options['prepared_batching'].token_budget, 0)
        self.assertEqual(len(fixture.runtime.async_actors), 0)

    def test_same_public_resident_factory_runs_two_queries_and_keeps_payloads(self):
        results = {}
        for skip in (False, True):
            with self.subTest(skip=skip):
                fixture = MovieTokenResidentFixture(self)
                plan = fixture.plan('RF')
                plan['experimental_skip_token_packing'] = skip
                checked = session.check_plan(plan)
                self.assertEqual(checked['expected_calls'], 4)
                if skip:
                    self.assertIs(checked['record_batching']['experimental_skip_token_packing'], True)
                else:
                    self.assertNotIn('experimental_skip_token_packing', checked['record_batching'])
                report = fixture.run(plan)
                self.assertEqual(report['status'], 'completed')
                self.assertEqual(sum(e['event']=='construct' for e in fixture.events()), 1)
                self.assertEqual(sum(e['event']=='close' for e in fixture.events()), 1)
                normalized = []
                for q in report['queries']:
                    close = query_close(q)
                    self.assertTrue(close['query_completed'])
                    self.assertEqual(close['core_usage']['held_tasks'], 0)
                    self.assertEqual(close['async_token_batches'].get('experimental_skip_token_packing', False), skip)
                    normalized.append(dict(result=q['backend_report']['result'],
                        calls=[{k:c[k] for k in ('call_id', 'row_id', 'fixture_messages', 'fixture_generation', 'status', 'text')}
                               for c in query_calls(q)]))
                results[skip] = normalized
        self.assertEqual(results[False], results[True])


class BypassReadyTests(unittest.TestCase):
    setUp = rolling.RollingRefillTests.setUp
    tick_until = rolling.RollingRefillTests.tick_until

    def owner(self, plan, **options):
        return batches.AsyncTokenBatchTests.owner(self, plan, C=None, rolling_refill=True,
            experimental_skip_token_packing=True, **options)

    test_bypass_refills_source_before_slow_peer = rolling.RollingRefillTests.test_fast_final_refills_new_source_rows_while_slow_peer_is_unfinished
    test_bypass_legal_successor_before_slow_peer = rolling.RollingRefillTests.test_fast_legal_successor_runs_before_slow_same_batch_peer
    test_bypass_cancel_keeps_prefix_and_unknown = rolling.RollingRefillTests.test_cancel_preserves_known_prefix_unknown_peer_and_no_false_successor


if __name__ == '__main__':
    unittest.main()
