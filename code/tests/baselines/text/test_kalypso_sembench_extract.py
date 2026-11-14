"""Native SemMap task adaptation; original text Extract relations and errors."""

import asyncio
import csv
import json
import os
from pathlib import Path
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.kalypso_native import (
    adapter, field_binding, map_observation, movie_map_inputs, native_server, sembench_inputs,
)
from src.baselines.text.frameworks.kalypso_native.query_runner import KalypsoRunner
from src.baselines.text.frameworks.kalypso_native.sembench_extract import ExtractMapOperation
from src.experiments.kalypso_query_lifecycle import query_driver_type
from tests.baselines.text import test_kalypso_movie_map as map_tests
from tests.experiments import test_sembench_text_original as originals


@unittest.skipUnless(originals.READY, 'requires pinned public SemBench and driver SDK')
class ExtractMapContracts(unittest.TestCase):
    def setUp(self):
        self.helper = originals.OriginalSemBenchTextTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.helper._write_ecomm_inputs()
        self.helper._write_cars_inputs()

    def bind(self, scenario='mmqa', query='1', *, name='q'):
        original = self.helper._query(scenario, query) if scenario != 'cars' else sembench_inputs.SemBenchTextQuery(
            originals.CHECKOUT, self.helper.data, scenario, query, execution_id=name,
            model_name='openai/local-fixture', generation=self.helper.generation, scale_factor=7)
        count = len(original.operation.records)
        spec = dict(task='sembench-extract-map', scenario=scenario, query=query, query_id=name,
                    expected_calls=count, max_native_calls=2*count)
        if scenario == 'cars':
            spec['scale_factor'] = 7
        unit = dict(call_cap=2*count, queries=[spec])
        cfg = dict(sembench=originals.CHECKOUT, data_dir=str(self.helper.data), model='m',
            model_name='openai/local-fixture', units=[unit], execution_enabled=False, CPU_fixture_only=True,
            native_trace=str(self.helper.data/'trace.jsonl'), map_prompt_root=str(self.helper.data/(name+'-prompts')),
            source_data_sha256={f: adapter.sha(self.helper.data/f) for f in original.files})
        return cfg, unit, spec, sembench_inputs.make_query(cfg, name, spec, kind='extract')

    def run_query(self, cfg, unit, spec, query, answers, *, name='run'):
        response, events = map_tests.trace(len(answers))
        response['latency_sec'] = .25
        for row in response['results']:
            row['outputs'][0]['value'] = answers[row['idx']]
        for row in events:
            row['query_id'] = spec['query_id']
            if row['event'] == 'map_result':
                row['text'] = answers[row['idx']]
        def transport(payload, qid, directory):
            self.assertEqual(payload['ops'], [dict(op='sem_map', args=dict(prompt=
                field_binding.extract_instruction(query.operation.stage.method.output_columns)))])
            self.assertEqual(qid, spec['query_id'])
            self.assertNotIn('response_format', payload)
            self.assertNotIn('sem_extract', json.dumps(payload))
            return response
        with patch.object(adapter, 'load_dependencies', return_value=lambda p,v:p.write_text(json.dumps(v))), \
                patch.object(adapter, 'read_trace', side_effect=lambda p,q: [] if Path(p).name == 'map-trace.jsonl' else events):
            driver = KalypsoRunner(cfg, unit, self.helper.data/name, transport)
            driver.model_uid = 'u'
            return driver.query(spec, lambda event: None)

    def test_explicit_profile_reuses_one_runner_and_original_native_map_call_bound(self):
        cfg, unit, spec, query = self.bind()
        self.assertIs(query_driver_type(unit), KalypsoRunner)
        self.assertEqual(native_server.physical_call_caps(unit), {'q': 2*len(query.operation.records)})
        self.assertEqual(native_server.query_task(spec), dict(scenario='mmqa', query_id='1'))
        with self.assertRaisesRegex(ValueError, 'ordinary text Map'):
            sembench_inputs.make_query(cfg, 'q', dict(spec, task='sembench-map'))

    def test_four_text_families_keep_original_complete_relations_scoring_and_native_identity(self):
        for scenario, key, value in [('mmqa','1','Michael Ritchie'), ('mmqa','4','comedy'),
                                     ('medical','10','ALLERGY'), ('ecomm','3','Acme'), ('cars','10','ENGINE')]:
            with self.subTest(scenario=scenario, query=key):
                cfg, unit, spec, query = self.bind(scenario, key, name='q-'+scenario+'-'+key)
                columns = query.operation.stage.method.output_columns
                answers = [json.dumps({f: value for f in columns})]*len(query.operation.records)
                expected, _ = self.helper._reference(query, answers)
                from src.baselines.text.sembench_prepared import result_value
                encoded = result_value(expected)
                quality = query.evaluate(expected)
                result = self.run_query(cfg, unit, spec, query, answers, name='run-'+scenario+'-'+key)
                self.assertEqual(result['result'], encoded)
                self.assertEqual(result['quality'], quality)
                self.assertEqual(result['physical_native_calls'], len(answers))
                self.assertEqual(result['physical_call_cap'], 2*len(answers))
                self.assertFalse(result['native_extract_operator'])
                self.assertFalse(result['native_JSON_format_forwarded'])
                self.assertIn('engineering variant', result['task_adaptation'])
                self.assertEqual(result['memory_sink_timing']['trace_audit_status'], 'passed')

    def test_frozen_driver_inputs_use_original_output_descriptions_and_native_map_CPU_path(self):
        cfg, unit, spec, query = self.bind()
        with patch.object(movie_map_inputs, 'binding', return_value={'explicit_CPU_fixture': True}):
            frozen = movie_map_inputs.prepare_inputs(cfg, unit, self.helper.data/'frozen')
            cfg['native_cpu_inputs'] = {key: frozen[key] for key in ('manifest','sha256')}
            _, inputs = movie_map_inputs.load_inputs(cfg)
        table = inputs[0]['table']
        self.assertEqual(table['instruction'], field_binding.extract_instruction(query.operation.stage.method.output_columns))
        self.assertEqual([r['fields'] for r in table['rows']], [r.values for r in query.operation.records])
        self.assertEqual([r['source_index'] for r in table['rows']], [r.source_index for r in query.operation.records])
        # The frozen native CPU boundary imports neither task nor evaluator SDKs.
        source = inputs[0]['paths']['source']
        description = json.loads(source.read_text())
        self.assertEqual(description['operation']['kind'], 'extract')
        self.assertIn('length retry', description['native_retry_policy'])

    def test_native_output_keys_null_collisions_and_missing_keys_keep_original_extract_behavior(self):
        cfg, unit, spec, query = self.bind('mmqa', '4')
        answers = [json.dumps(dict(genres='comedy', title='Replacement', extra=None))]*len(query.operation.records)
        expected, _ = self.helper._reference(query, answers)
        operation = object.__new__(ExtractMapOperation)
        actual = operation.restore_relation(query.operation, answers)
        self.assertEqual(actual['extra'].tolist(), ['None']*len(answers))
        self.assertTrue((actual['title'] == 'Replacement').all())
        result, _ = query.bind_execution()(lambda op, invoke: dict(frame=operation.restore_relation(op, answers)))
        self.helper._assert_result_equal(result, expected)
        for texts in (['{}']*len(answers), ['invalid JSON']*len(answers)):
            with self.subTest(texts=texts), self.assertRaises(KeyError):
                query.bind_execution()(lambda op, invoke: dict(frame=operation.restore_relation(op, texts)))

    def test_empty_extract_keeps_original_missing_column_error_without_inventing_a_sink(self):
        import pandas as pd

        path = self.helper.data/'ben_piazza_text_data.csv'
        pd.read_csv(path).iloc[:0].to_csv(path, index=False)
        cfg, unit, spec, query = self.bind()
        self.assertEqual(spec['expected_calls'], 0)
        self.assertEqual(native_server.physical_call_caps(unit), {'q': 0})
        with self.assertRaises(KeyError):
            self.helper._reference(query, [])
        with self.assertRaises(KeyError) as caught:
            self.run_query(cfg, unit, spec, query, [])
        self.assertIsNone(caught.exception.query_report['memory_sink_timing']['memory_sink_ready_ns'])
        self.assertNotIn('quality', caught.exception.query_report)

    @unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'), 'requires fixed author Git objects')
    def test_fixed_native_frontend_keeps_image_JSON_as_text_and_has_no_extract_operator(self):
        from tests.baselines.text.kalypso_join_author import methods

        path = self.helper.data/'image-context.csv'
        image = dict(type='image_url', image_url=dict(url='data:image/png;base64,fixture'))
        with path.open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=['data'])
            writer.writeheader()
            writer.writerow(dict(data=json.dumps([image])))
        manager = SimpleNamespace(token_length=lambda text: len(str(text)))
        with methods(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'], manager, planning=True) as author:
            contexts = list(author._data_source(None, path, None))
            self.assertIsInstance(contexts[0].input.data[0]['content'], str)
            self.assertIn('image_url', contexts[0].input.data[0]['content'])
            self.assertFalse(hasattr(author.OpName, 'SEM_EXTRACT'))
            with self.assertRaisesRegex(ValueError, 'Unknown op'):
                author.SemanticPlan(None).parse_ops([dict(op='sem_extract', args={})])

    @unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'), 'requires fixed author Git objects')
    def test_real_author_map_template_estimator_and_method_execute_json_without_new_operator(self):
        from tests.baselines.text.kalypso_join_author import methods

        cfg, unit, spec, query = self.bind()
        rows, _ = sembench_inputs.prepare(cfg, spec, self.helper.data/'native-input', query.operation, kind='extract')
        manager = SimpleNamespace(token_length=lambda text: len(str(text)), apply_chat_template=lambda prompt: str(prompt))
        with methods(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'], manager, mapping=True) as author:
            prompt = ModuleType('vllm.kalypso.sem_ops.prompt_utils')
            prompt.__dict__.update(vars(author))
            base = ModuleType('vllm.kalypso.sem_ops.base'); base.OpName = author.OpName
            context = ModuleType('vllm.kalypso.context'); context.RETRY_TASK = author.RETRY_TASK
            with patch.dict('sys.modules', {'vllm.kalypso.sem_ops.prompt_utils':prompt,
                    'vllm.kalypso.sem_ops.base':base, 'vllm.kalypso.context':context}):
                map_observation.install(cfg, unit, self.helper.data, native_server, author.SemMap, author.MapRatioEstimator)
                instruction = field_binding.extract_instruction(query.operation.stage.method.output_columns)
                op = author.SemMap(instruction, position=0)
                calls = []
                async def execute(**kwargs):
                    calls.append(kwargs)
                    return SimpleNamespace(text='{"director":"Michael Ritchie"}', request_id='software-'+str(len(calls)), finish_reason='stop')
                executor = SimpleNamespace(execute=execute)
                for row in rows:
                    ctx = author.SemContext(input=author.SemanticInput(row['data'].strip()),
                        state=author.ExecutionState(idx=row['idx'], executor=executor,
                            raw_request=SimpleNamespace(headers={'X-Kalypso-Observation':'q'})))
                    self.assertIs(asyncio.run(op._run_single(ctx)), ctx)
                    self.assertIn('Map Instruction:', calls[-1]['prompt'][-1]['content'])
                    self.assertIn('"director"', calls[-1]['prompt'][-1]['content'])
                    self.assertNotIn('{{', calls[-1]['prompt'][-1]['content'])
                    self.assertEqual(ctx.output[-1][str(type(op))], '{"director":"Michael Ritchie"}')
                self.assertEqual(len(calls), len(rows))
                self.assertFalse(op.pin)


if __name__ == '__main__':
    unittest.main()
