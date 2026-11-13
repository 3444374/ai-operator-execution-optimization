"""Complete original visual tasks over the existing software Ray processor."""

from collections import Counter
from contextlib import redirect_stdout
from copy import deepcopy
from dataclasses import replace
from io import StringIO
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from src.experiments import native_ray_sembench as driver, sembench_text_query as entry
from src.experiments.attempt_ledger import BudgetExhausted
from src.infrastructure.private_artifacts import content_digest, read_json
from tests.baselines.text.ray_native_fixtures import recorded_processor
from tests.experiments import test_sembench_lotus_image_query as fixture


@unittest.skipUnless(fixture.CHECKOUT and fixture.HAS_SDK, 'requires pinned original queries and LOTUS SDK')
class RayOriginalVisualTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.NativeLotusImageQueryTests.setUpClass()

    def setUp(self):
        self.source = fixture.NativeLotusImageQueryTests()
        self.source.setUp()
        self.addCleanup(self.source.doCleanups)

    def configure(self, scenario, query, inputs, *, allowance=64, **options):
        service, _ = self.source.config(scenario, query, **options)
        config = dict(schema='semloom.sembench_text_run.v1', scenario=scenario, query=query,
            query_id='raw-visual', backend='ray-data', model_name=service['model_name'],
            generation=service['generation'], engine=dict(model_path='CPU-no-model',
                model_python='CPU-no-model', driver_python='CPU-no-model', max_model_calls=allowance),
            resources=dict(query_timeout_s=60), profile={},
            ray_native=dict(engine_kwargs={}, processor_options=dict(prepare_image_stage=True)))
        if 'scale_factor' in options:
            config['scale_factor'] = options['scale_factor']
        config.update({name: str(value) for name, value in inputs.items() if name != 'data_dir'})
        path = self.source.root/('ray-config-'+str(next(self.source.serial))+'.json')
        path.write_text(json.dumps(config))
        config, engine, generation, resources = entry.configuration(path)
        task = entry.query_from_config(config, self.source.checkout, inputs.get('data_dir'), generation)
        checked = entry.check(config, task, engine)
        ledger, budget = self.source.ledger(config, allowance)
        config['budget'] = budget
        return service, config, task, engine, resources, ledger, checked, path

    def invoke(self, config, task, engine, resources, replies, *, close_error=None, response=None):
        import httpx

        directory = self.source.new_directory('ray-query')
        self.last = directory
        def answer(row, request):
            if response is not None:
                return response(row, request)
            body = dict(deepcopy(request['sampling_params']), model='local-image-fixture',
                        messages=deepcopy(request['messages']))
            if body.pop('structured_outputs', None) == {'json_object': True}:
                body['response_format'] = {'type': 'json_object'}
            reply = replies.respond(httpx.Request('POST', 'http://localhost:18473/v1/chat/completions',
                headers=dict(authorization='Bearer '+fixture.KEY_VALUE), json=body))
            reply.raise_for_status()
            return reply.json()['choices'][0]['message']['content']
        with recorded_processor(answer, close_error=close_error) as sdk:
            self.sdk = sdk
            report = driver.run_query(config, task, engine, resources, directory)
        return report, sdk

    def compare(self, scenario, query, inputs, *, mode='success', **options):
        import numpy as np

        service, config, task, engine, resources, ledger, checked, path = self.configure(
            scenario, query, inputs, **options)
        reference = fixture.NativeHTTPReplies(self, scenario, query=query, mode=mode)
        state = np.random.get_state()
        self.addCleanup(np.random.set_state, state)
        original, _, _ = self.source.invoke(service, inputs, reference, allowance=engine.max_model_calls)
        np.random.set_state(state)
        replies = fixture.NativeHTTPReplies(self, scenario, query=query, mode=mode)
        report, sdk = self.invoke(config, task, engine, resources, replies)
        self.assertTrue(task.visual_query)
        self.assertIsNone(task.operation)
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(report['result'], original['result'])
        actual_quality, expected_quality = deepcopy(report['quality']), deepcopy(original['quality'])
        if scenario == 'animals' and query in ('Q3', 'Q10'):
            # Original gold SQL returns every tied maximum without ORDER BY.
            self.assertCountEqual(actual_quality.pop('ground_truth'), expected_quality.pop('ground_truth'))
        self.assertEqual(actual_quality, expected_quality)
        self.assertEqual(report['physical_attempts_reserved'], original['model_calls'])
        self.assertEqual(Counter(content_digest(row['body']['messages']) for row in replies.requests),
                         Counter(content_digest(row['body']['messages']) for row in reference.requests))
        self.assertEqual(Counter(batch['occurrences'] for batch in report['native_lm_batches']),
                         Counter(batch['input_occurrences'] for batch in original['native_lm_calls']))
        self.assertEqual(len({row['call_id'] for row in sdk.requests}), len(sdk.requests))
        self.assertEqual(report['query_clock_version'], 'lotus_ray.source_to_sink.v1')
        self.assertIn('synchronous actor observer', report['timing_scope'])
        if original['model_calls']:
            self.assertLessEqual(report['sink_ready_ns'], report['native']['events']['close_started_ns'])
            self.assertLessEqual(report['native']['events']['close_finished_ns'], report['evaluation_started_ns'])
            self.assertEqual(sdk.events.count('init'), 1)
            self.assertEqual(sdk.events.count('close'), 1)
        else:
            self.assertEqual(sdk.events, [])
        self.assertFalse(sdk.initialized)
        return report, task, sdk

    def test_animals_filters_share_manifest_source_and_preserve_both_independent_passes(self):
        inputs = self.source.animals_inputs()
        for query in ('Q1', 'Q3', 'Q7', 'Q10'):
            with self.subTest(query=query):
                report, task, _ = self.compare('animals', query, inputs)
                self.assertEqual([batch['occurrences'] for batch in report['native_lm_batches']],
                                 [4,4] if query == 'Q7' else [4])
                self.assertIsNone(task.data_dir)
                self.assertEqual(task.describe()['expected_model_calls'], 8 if query == 'Q7' else 4)

    def test_cars_and_medical_sources_keep_full_prelimit_joins_and_multiimage_occurrences(self):
        for scenario, queries, inputs, scale in (
                ('cars', ('Q3','Q8'), self.source.cars_inputs(), 7),
                ('medical', ('Q3','Q9'), self.source.medical_inputs(), 29)):
            for query in queries:
                with self.subTest(scenario=scenario, query=query):
                    report, task, _ = self.compare(scenario, query, inputs, scale_factor=scale)
                    self.assertEqual(report['physical_attempts_reserved'], task.visual_inputs['rows'])
                    self.assertEqual(task.visual_inputs['scale_factor'], scale)
                    if scenario == 'cars':
                        self.assertGreater(report['physical_attempts_reserved'], 10)
                    if query == 'Q9':
                        self.assertEqual(task.visual_inputs['images_per_occurrence'], 2)
        # Reuse the existing native-service Q8 fixture: its original evaluator
        # samples 100 gold rows after the authored LIMIT 100.
        self.compare('medical','Q8',self.source.medical_inputs(103),scale_factor=29,allowance=128)

    def test_small_medical_gold_retains_original_evaluation_failure_after_sink(self):
        inputs = self.source.medical_inputs()
        service, config, task, engine, resources, ledger, _, _ = self.configure('medical','Q8',inputs,scale_factor=29)
        with self.assertRaises(ValueError) as reference:
            self.source.invoke(service,inputs,fixture.NativeHTTPReplies(self,'medical',query='Q8'),allowance=64)
        with self.assertRaises(ValueError) as native:
            self.invoke(config,task,engine,resources,fixture.NativeHTTPReplies(self,'medical',query='Q8'))
        self.assertEqual(str(native.exception),str(reference.exception))
        report = read_json(self.last/'query.json')
        self.assertIsNotNone(report['source_to_sink_seconds'])
        self.assertNotIn('quality',report)
        self.assertIn('result',report)
        self.assertTrue(report['native']['ray_driver_disconnected'])

    def test_filter_extract_uses_actual_nonempty_survivors_and_unmodified_json_result(self):
        report, task, _ = self.compare('ecomm', 'Q12', self.source.ecomm_inputs())
        self.assertEqual([batch['occurrences'] for batch in report['native_lm_batches']], [4,3])
        self.assertEqual(task.describe()['minimum_model_calls'], 4)
        self.assertEqual(task.describe()['maximum_model_calls'], 8)
        self.assertIsNone(task.describe()['expected_model_calls'])

    def test_visual_map_uses_the_same_original_formatter_parser_and_result(self):
        report, _, _ = self.compare('ecomm', 'Q6', self.source.ecomm_outfit_inputs('Q6'))
        self.assertEqual(report['physical_attempts_reserved'],2)

    def test_visual_unary_and_join_frontiers_preserve_original_messages_and_work(self):
        import pandas as pd

        for query, calls in (('Q2', 5), ('Q4', 4), ('Q8', 5), ('Q9', 25), ('Q13', 5)):
            with self.subTest(query=query):
                inputs = self.source.ecomm_outfit_inputs(query)
                data = Path(inputs['data_dir'])
                if query == 'Q4':
                    path = data/'image_mapping.parquet'
                    pd.read_parquet(path).iloc[[0,2,3,4]].to_parquet(path, index=False)
                if query == 'Q8':
                    path = data/'styles_details.parquet'
                    styles = pd.read_parquet(path)
                    styles.at[0, 'productDescriptors'] = {'description': {'value': 'long description '*200}}
                    styles.to_parquet(path, index=False)
                report, task, _ = self.compare('ecomm', query, inputs)
                self.assertEqual(report['physical_attempts_reserved'], calls)
                self.assertEqual(task.describe()['minimum_model_calls'], calls)
                if query == 'Q8':
                    self.assertFalse(report['semantic']['native_join_inputs'][0]['all_declared_columns_selected'])

    def test_branch_joins_and_grouped_topk_use_original_columns_methods_and_thread_lm(self):
        import lotus

        previous = object.__getattribute__(lotus.settings, 'lm')
        for query in ('Q10','Q11','Q14'):
            with self.subTest(query=query):
                report, task, _ = self.compare('ecomm', query, self.source.ecomm_outfit_inputs(query))
                self.assertIsNone(task.describe()['maximum_model_calls'])
                self.assertEqual(task.describe()['minimum_model_calls'], 5)
                joins = report['semantic']['native_join_inputs']
                self.assertTrue(joins)
                self.assertTrue(all(not item['all_declared_columns_selected'] for item in joins))
                self.assertTrue(all(len(item['selected_columns']) == 2 for item in joins))
        self.assertIs(object.__getattribute__(lotus.settings, 'lm'), previous)

    def test_empty_filter_keeps_original_sink_and_empty_extract_keeps_original_keyerror(self):
        self.compare('medical', 'Q9', self.source.medical_inputs(), scale_factor=29, mode='no_matches')
        for mode in ('no_matches','empty_json'):
            with self.subTest(mode=mode):
                inputs = self.source.ecomm_inputs()
                service, config, task, engine, resources, ledger, _, _ = self.configure('ecomm','Q12',inputs)
                with self.assertRaises(KeyError) as reference:
                    self.source.invoke(service, inputs, fixture.NativeHTTPReplies(self,'ecomm',mode=mode), allowance=64)
                with self.assertRaises(KeyError) as native:
                    self.invoke(config, task, engine, resources, fixture.NativeHTTPReplies(self,'ecomm',mode=mode))
                self.assertEqual(str(native.exception), str(reference.exception))
                report = read_json(self.last/'query.json')
                self.assertIsNone(report['source_to_sink_seconds'])
                self.assertNotIn('quality', report)
                self.assertEqual(report['physical_attempts_reserved'], 4 if mode == 'no_matches' else 7)
                self.assertTrue(report['native']['ray_driver_disconnected'])

    def test_empty_join_frontier_keeps_original_empty_relation_without_runtime_start(self):
        import pandas as pd

        inputs = self.source.ecomm_outfit_inputs('Q8')
        path = Path(inputs['data_dir'])/'styles_details.parquet'
        styles = pd.read_parquet(path)
        styles['gender'] = 'Women'
        styles.to_parquet(path,index=False)
        report, task, sdk = self.compare('ecomm','Q8',inputs)
        self.assertEqual(task.describe()['minimum_model_calls'],0)
        self.assertEqual(report['physical_attempts_reserved'],0)

    def test_empty_grouped_topk_keeps_original_failure_after_actual_empty_join(self):
        import httpx

        inputs = self.source.ecomm_outfit_inputs('Q14')
        service, config, task, engine, resources, ledger, _, _ = self.configure('ecomm','Q14',inputs)
        replies = fixture.NativeHTTPReplies(self,'ecomm',query='Q14')
        original = replies.respond
        def false_reply(request):
            payload = original(request).json()
            payload['choices'][0]['message']['content'] = 'False'
            return httpx.Response(200,request=request,json=payload)
        with patch.object(replies,'respond',side_effect=false_reply), self.assertRaises(ValueError) as reference:
            self.source.invoke(service,inputs,replies,allowance=64)
        with self.assertRaises(ValueError) as native:
            self.invoke(config,task,engine,resources,fixture.NativeHTTPReplies(self,'ecomm',query='Q14'),
                response=lambda row,request:'False')
        self.assertEqual(str(native.exception),str(reference.exception))
        report = read_json(self.last/'query.json')
        self.assertIsNone(report['source_to_sink_seconds'])
        self.assertEqual(report['physical_attempts_reserved'],5)
        self.assertTrue(report['native']['ray_driver_disconnected'])

    def test_dynamic_branch_budget_and_failed_followup_preserve_evidence_and_close(self):
        import lotus

        inputs = self.source.ecomm_outfit_inputs('Q10')
        service, config, task, engine, resources, ledger, _, _ = self.configure('ecomm','Q10',inputs,allowance=6)
        with self.assertRaises(BudgetExhausted):
            self.invoke(config,task,engine,resources,fixture.NativeHTTPReplies(self,'ecomm',query='Q10'))
        report = read_json(self.last/'query.json')
        self.assertEqual(report['status'],'failed')
        self.assertIsNone(report['source_to_sink_seconds'])
        self.assertLessEqual(report['physical_attempts_reserved'],6)
        self.assertTrue(report['native']['ray_driver_disconnected'])

        inputs = self.source.ecomm_inputs()
        service, config, task, engine, resources, ledger, _, _ = self.configure('ecomm','Q12',inputs)
        failure = RuntimeError('CPU visual Extract failed')
        def respond(row,request):
            if request['sampling_params'].get('structured_outputs') == {'json_object':True}:
                raise failure
            return 'True'
        previous = object.__getattribute__(lotus.settings,'lm')
        with self.assertRaises(RuntimeError) as caught:
            self.invoke(config,task,engine,resources,fixture.NativeHTTPReplies(self,'ecomm'),
                response=respond,close_error=ValueError('CPU Ray close failed'))
        self.assertIs(caught.exception,failure)
        self.assertIn('native Ray close failed: ValueError',failure.__notes__)
        self.assertIs(object.__getattribute__(lotus.settings,'lm'),previous)
        report = read_json(self.last/'query.json')
        self.assertIsNone(report['source_to_sink_seconds'])
        self.assertNotIn('quality',report)
        self.assertTrue(read_json(self.last/'native-predictions.json')['batches'][0]['predictions'])

    def test_missing_assets_or_missing_native_image_stage_stop_before_any_backend_or_claim(self):
        inputs = self.source.ecomm_inputs()
        service, config, task, engine, resources, ledger, _, _ = self.configure('ecomm','Q12',inputs)
        from src.experiments.sembench_image_query import check
        state = check(self.source.checkout, 'ecomm', 'Q12', data_dir=inputs['data_dir'])
        self.assertIn('complete original LOTUS visual query wired', state['baseline_states']['ray-data'])
        self.assertFalse(state['execution_enabled'])
        with self.assertRaisesRegex(ValueError, 'native Ray image preparation'):
            entry.query_from_config(dict(config, ray_native=dict(engine_kwargs={},processor_options={})),
                self.source.checkout, inputs['data_dir'], task.generation)
        image = Path(inputs['data_dir'])/'images/1.png'
        image.unlink()
        with self.assertRaises(FileNotFoundError):
            entry.query_from_config(config, self.source.checkout, inputs['data_dir'], task.generation)
        self.assertEqual(ledger.snapshot()['units'][0]['claimed'], 0)

    def test_global_format_and_unsupported_generation_cannot_override_original_visual_operators(self):
        inputs = self.source.ecomm_inputs()
        _, config, task, _, _, ledger, _, _ = self.configure('ecomm', 'Q12', inputs)
        for options in ({'response_format': {'type': 'json_object'}},
                        {'logprobs': 1}, {'repetition_penalty': 2}):
            with self.subTest(options=options), self.assertRaisesRegex(ValueError, 'Extract owns'):
                entry.query_from_config(config, self.source.checkout, inputs['data_dir'],
                                        replace(task.generation, **options))
        self.assertEqual(ledger.snapshot()['units'][0]['claimed'], 0)

    def test_original_source_reads_and_image_formatting_stay_inside_complete_query_clock(self):
        import pandas as pd
        from src.baselines.text import sembench_ecomm

        inputs = self.source.ecomm_inputs()
        service, config, task, engine, resources, _, _, _ = self.configure('ecomm','Q12',inputs)
        events = []
        def observe(name, function):
            def call(*args, **kwargs):
                import time
                events.append((name, time.monotonic_ns()))
                return function(*args, **kwargs)
            return call
        with patch.object(pd, 'read_parquet', observe('source', pd.read_parquet)), \
                patch.object(sembench_ecomm, 'load_module', observe('bind/eval', sembench_ecomm.load_module)), \
                patch.object(sembench_ecomm, 'file_identity', observe('identity', sembench_ecomm.file_identity)):
            report, _ = self.invoke(config, task, engine, resources, fixture.NativeHTTPReplies(self,'ecomm'))
        inside = [name for name, at in events if report['source_started_ns'] <= at <= report['sink_ready_ns']]
        self.assertEqual(inside, ['source','source'])
        self.assertEqual(report['source_to_sink_seconds'],(report['sink_ready_ns']-report['source_started_ns'])/1e9)

    def test_animals_cli_checks_manifest_without_data_dir_or_execution(self):
        inputs = self.source.animals_inputs()
        service, config, task, engine, resources, ledger, _, path = self.configure('animals','Q7',inputs)
        with redirect_stdout(StringIO()):
            for action in ('check','run'):
                arguments = [action,'--config',str(path),'--sembench-checkout',str(self.source.checkout)]
                if action == 'run':
                    arguments.extend(('--output-dir',str(self.source.root/'not-created')))
                self.assertEqual(entry.main(arguments),0)
        self.assertFalse((self.source.root/'not-created').exists())
        self.assertEqual(ledger.snapshot()['units'][0]['claimed'],0)

    def test_animals_supervisor_and_worker_share_the_manifest_without_fabricated_data_dir(self):
        inputs = self.source.animals_inputs()
        service, config, task, engine, resources, ledger, _, path = self.configure('animals','Q7',inputs)
        output = self.source.root/'cli-owned'
        commands = []
        def supervise(command,*args,**kwargs):
            commands.append(command)
            return {'software_fixture':True}
        arguments = ['run','--config',str(path),'--sembench-checkout',str(self.source.checkout),
            '--output-dir',str(output),'--execute-model']
        with patch('src.experiments.lotus_records_supervisor.supervise',side_effect=supervise):
            self.assertEqual(entry.main(arguments),0)
        self.assertEqual(len(commands),1)
        self.assertNotIn('--data-dir',commands[0])
        with patch.object(entry,'run_worker') as worker:
            self.assertEqual(entry.main(commands[0][2:]),0)
        query = worker.call_args.args[1]
        self.assertIsNone(query.data_dir)
        self.assertEqual(query.manifest,Path(inputs['manifest']).resolve())
        self.assertEqual(query.manifest_digest,inputs['manifest_digest'])


if __name__ == '__main__':
    unittest.main()
