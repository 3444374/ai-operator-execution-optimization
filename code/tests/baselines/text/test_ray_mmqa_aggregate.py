"""Original MMQA hierarchy through the existing software native Ray seam."""

from copy import deepcopy
import json
from pathlib import Path
import shutil
import time
import unittest
from unittest.mock import patch

from src.experiments import native_ray_sembench as driver, sembench_text_query as entry
from src.experiments.attempt_ledger import BudgetExhausted
from src.infrastructure.private_artifacts import read_json
from tests.baselines.text.ray_native_fixtures import recorded_processor
from tests.experiments import test_sembench_lotus_mmqa_aggregate as fixture


@unittest.skipUnless(fixture.CHECKOUT and fixture.HAS_SDK, 'requires pinned original query and LOTUS SDK')
class RayMMQAAggregateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.NativeMMQAAggregateTests.setUpClass()

    def setUp(self):
        self.source = fixture.NativeMMQAAggregateTests()
        self.source.setUp()
        self.addCleanup(self.source.doCleanups)
        model = self.source.directory('software-model')
        self.tokenizer = model/'tokenizer.json'
        shutil.copyfile(self.source.tokenizer_path, self.tokenizer)

    def configure(self, service, data, *, engine_cap=40, allowance=40):
        counter = dict(service['token_counter'], path=str(self.tokenizer))
        config = dict(schema='semloom.sembench_text_run.v1', scenario='mmqa', query='Q5',
            query_id='native-aggregate', backend='ray-data', model_name=service['model_name'],
            generation=service['generation'], engine=dict(model_path=str(self.tokenizer.parent),
                model_python='CPU-no-model', driver_python='CPU-no-model',
                max_model_calls=engine_cap, max_model_len=service['native_lm_max_ctx_len']),
            resources=dict(query_timeout_s=60), profile={}, token_counter=counter,
            ray_native=dict(engine_kwargs=dict(tokenizer_revision=counter['tokenizer_revision']), processor_options={}))
        path = self.source.root/('ray-config-'+str(next(self.source.serial))+'.json')
        path.write_text(json.dumps(config))
        config, engine, generation, resources = entry.configuration(path)
        query = entry.query_from_config(config, self.source.checkout, data, generation)
        checked = entry.check(config, query, engine)
        ledger, budget = self.source.ledger(config, allowance)
        config['budget'] = budget
        return config, query, engine, resources, ledger, checked

    @staticmethod
    def answer(prompt, mode):
        answer = 'Another Actor' if mode == 'wrong_actor' else 'Lizzy Caplan'
        if mode == 'long_summaries' and ('\n\tDocument ' in prompt or prompt.count('\n\tSource ') > 2):
            answer += ' '+'detail '*90
        return answer

    def invoke(self, config, query, engine, resources, *, mode='compact', close_error=None, reply=None):
        directory = self.source.directory('ray-query')
        self.last = directory
        if reply is None:
            reply = lambda row, request: self.answer(request['messages'][0]['content'], mode)
        with recorded_processor(reply, close_error=close_error) as sdk:
            self.sdk = sdk
            report = driver.run_query(config, query, engine, resources, directory)
        return report, sdk

    def test_preflight_matches_original_leaf_tokens_without_inference_or_budget_claim(self):
        from lotus.models import LM

        data = self.source.source()
        for service, expected in ((self.source.config()[0], 1), (self.source.multi_config()[0], None)):
            with self.subTest(expected=expected), patch.object(LM, '_process_uncached_messages',
                    side_effect=AssertionError('preflight inferred')):
                original = self.source.entry.check(service, self.source.checkout, data_dir=data)
                config, query, engine, resources, ledger, checked = self.configure(service, data)
            self.assertEqual(query.aggregate_inputs['native_plan'], original['native_plan'])
            self.assertEqual(query.describe()['expected_model_calls'], expected)
            self.assertEqual(query.describe()['maximum_model_calls'], expected)
            self.assertEqual(query.describe()['minimum_model_calls'], original['work']['minimum_model_calls'])
            self.assertEqual(checked['inference_calls'], 0)
            self.assertEqual(ledger.snapshot()['units'][0]['claimed'], 0)
            self.assertEqual(query.lm_options['max_ctx_len'], service['native_lm_max_ctx_len'])
            self.assertTrue(query.aggregate_inputs['native_plan']['token_count_calls'])

    def test_single_and_multilevel_match_service_prompts_tokens_full_result_and_original_score(self):
        from lotus.models import LM

        counts = []
        count = LM.count_tokens
        def observed(lm, value):
            result = count(lm, value)
            counts.append((time.monotonic_ns(), deepcopy(value), result))
            return result

        cases = ((self.source.config()[0], 'compact'), (self.source.multi_config()[0], 'compact'),
                 (self.source.multi_config()[0], 'long_summaries'), (self.source.config()[0], 'wrong_actor'))
        for service, mode in cases:
            with self.subTest(mode=mode, context=service['native_lm_max_ctx_len']):
                data = self.source.source()
                replies = fixture.NativeAggregateReplies(self, mode=mode)
                with patch.object(LM, 'count_tokens', observed):
                    original, _, _ = self.source.invoke(service, data, replies, allowance=40)
                original_counts = [(value, total) for stamp, value, total in counts
                                   if original['source_started_ns'] <= stamp <= original['sink_ready_ns']]
                counts.clear()
                config, query, engine, resources, ledger, _ = self.configure(service, data)
                with patch.object(LM, 'count_tokens', observed):
                    report, sdk = self.invoke(config, query, engine, resources, mode=mode)
                native_counts = [(value, total) for stamp, value, total in counts
                                 if report['source_started_ns'] <= stamp <= report['sink_ready_ns']]
                counts.clear()
                self.assertEqual(report['result'], original['result'])
                self.assertEqual(report['quality'], original['quality'])
                self.assertEqual(native_counts, original_counts)
                self.assertEqual([row['messages'] for row in sdk.requests],
                                 [request['body']['messages'] for request in replies.requests])
                self.assertEqual([batch['occurrences'] for batch in report['native_lm_batches']],
                                 [batch['input_occurrences'] for batch in original['native_lm_calls']])
                self.assertEqual(report['physical_attempts_reserved'], original['model_calls'])
                self.assertEqual(sdk.events.count('init'), 1)
                self.assertEqual(sdk.events.count('close'), 1)
                self.assertFalse(sdk.initialized)
                self.assertEqual(report['query_clock_version'], 'lotus_ray.source_to_sink.v1')
                self.assertLessEqual(report['sink_ready_ns'], report['native']['events']['close_started_ns'])
                self.assertLessEqual(report['native']['events']['close_finished_ns'], report['evaluation_started_ns'])
                self.assertFalse(report['lotus_lm']['model_tokenizer_verified'])
                self.assertEqual(report['lotus_lm']['max_ctx_len'], engine.max_model_len)
                self.assertEqual(sdk.configs[0]['engine_kwargs']['max_model_len'], engine.max_model_len)
                for request in sdk.requests:
                    self.assertEqual(request['sampling_params']['max_tokens'], service['generation']['max_tokens'])
                    self.assertEqual(request['sampling_params']['stop'], service['generation']['stop'])
                if mode == 'long_summaries':
                    self.assertGreater(len(report['native_lm_batches']), 2)
                    self.assertTrue(any('detail' in row['messages'][0]['content'] and '\n\tSource ' in row['messages'][0]['content']
                                        for row in sdk.requests))

    def test_fresh_source_is_inside_clock_while_identity_binding_and_evaluation_are_outside(self):
        import pandas as pd
        from src.baselines.text import sembench_mmqa_aggregate as task

        service, _ = self.source.multi_config()
        data = self.source.source()
        config, query, engine, resources, _, _ = self.configure(service, data)
        events = []
        def observe(name, function):
            def wrapped(*args, **kwargs):
                events.append((name, time.monotonic_ns()))
                return function(*args, **kwargs)
            return wrapped
        with patch.object(pd, 'read_csv', observe('source', pd.read_csv)), \
                patch.object(task, 'load_module', observe('bind/eval', task.load_module)), \
                patch.object(task, 'file_identity', observe('identity', task.file_identity)):
            report, _ = self.invoke(config, query, engine, resources)
        inside = [name for name, stamp in events if report['source_started_ns'] <= stamp <= report['sink_ready_ns']]
        self.assertEqual(inside, ['source'])
        self.assertEqual(report['source_to_sink_seconds'], (report['sink_ready_ns']-report['source_started_ns'])/1e9)

    def test_actual_summary_work_stops_at_engine_or_permanent_budget_and_closes(self):
        service, _ = self.source.multi_config()
        data = self.source.source()
        original = self.source.entry.check(service, self.source.checkout, data_dir=data)
        minimum = original['work']['minimum_model_calls']
        for engine_cap, allowance in ((minimum, 40), (40, minimum)):
            with self.subTest(engine_cap=engine_cap, allowance=allowance):
                config, query, engine, resources, ledger, _ = self.configure(service, data,
                    engine_cap=engine_cap, allowance=allowance)
                with self.assertRaises(BudgetExhausted):
                    self.invoke(config, query, engine, resources, mode='long_summaries')
                report = read_json(self.last/'query.json')
                self.assertEqual(report['status'], 'failed')
                self.assertIsNone(report['source_to_sink_seconds'])
                self.assertNotIn('quality', report)
                self.assertLessEqual(report['physical_attempts_reserved'], min(engine_cap, allowance))
                self.assertTrue(report['native']['ray_driver_disconnected'])
                self.assertEqual(self.sdk.events.count('close'), 1)

    def test_unknown_total_requires_a_finite_engine_limit_and_minimum_reserved_budget(self):
        service, _ = self.source.multi_config()
        data = self.source.source()
        for cap in (None, 1):
            with self.subTest(cap=cap), self.assertRaisesRegex(ValueError, 'max_model_calls must cover'):
                self.configure(service, data, engine_cap=cap)
        config, query, engine, resources, ledger, _ = self.configure(service, data, allowance=1)
        self.assertIsNone(query.describe()['expected_model_calls'])
        with self.assertRaisesRegex(ValueError, 'budget cannot cover'):
            self.invoke(config, query, engine, resources)
        report = read_json(self.last/'query.json')
        self.assertIsNone(report['source_started_ns'])
        self.assertEqual(report['physical_attempts_reserved'], 0)
        self.assertEqual(self.sdk.events, [])

    def test_summary_failure_is_primary_when_final_runtime_close_also_fails(self):
        service, _ = self.source.multi_config()
        config, query, engine, resources, _, _ = self.configure(service, self.source.source())
        failure = RuntimeError('CPU native summary failed')
        def reply(row, request):
            if '\n\tSource ' in request['messages'][0]['content']:
                raise failure
            return 'Lizzy Caplan'
        with self.assertRaises(RuntimeError) as caught:
            self.invoke(config, query, engine, resources, reply=reply, close_error=ValueError('CPU close failed'))
        self.assertIs(caught.exception, failure)
        self.assertIn('native Ray close failed: ValueError', failure.__notes__)
        report = read_json(self.last/'query.json')
        self.assertIsNone(report['source_to_sink_seconds'])
        self.assertEqual(report['native_lm_batches'][-1]['status'], 'failed')
        self.assertTrue(read_json(self.last/'native-predictions.json')['batches'][0]['predictions'])
        self.assertEqual(self.sdk.events.count('close'), 1)

    def test_bad_counter_context_or_backend_fails_before_native_execution(self):
        from lotus.models import LM
        from src.baselines.text.sembench_text import SemBenchTextQuery

        service, _ = self.source.config()
        config, query, engine, resources, _, _ = self.configure(service, self.source.source())
        changes = (
            dict(token_counter=dict(config['token_counter'], path=str(self.source.tokenizer_path))),
            dict(token_counter=dict(config['token_counter'], sha256='0'*64)),
            dict(token_counter=dict(config['token_counter'], tokenizer_revision='another-revision')),
            dict(engine=dict(config['engine'], max_model_len=None)),
            dict(ray_native=dict(config['ray_native'], engine_kwargs=dict(max_model_len=600))),
            dict(backend='lotus-native'),
        )
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                entry.query_from_config(dict(config, **change), self.source.checkout, query.data_dir, query.generation)
        with patch.object(LM, 'count_tokens', side_effect=AssertionError('default tokenizer used')), \
                self.assertRaisesRegex(ValueError, 'local token counter'):
            SemBenchTextQuery(self.source.checkout, query.data_dir, 'mmqa', 'Q5',
                execution_id='invalid-counter', model_name=query.model_name, generation=query.generation,
                lm_options=dict(max_ctx_len=engine.max_model_len, tokenizer=None))


if __name__ == '__main__':
    unittest.main()
