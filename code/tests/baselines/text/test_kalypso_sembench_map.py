"""Original multi-field Map source, author template and complete sink; no model."""
from src.baselines.text.frameworks.kalypso_native.query_runner import KalypsoRunner
import ast
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import ModuleType, SimpleNamespace as NS
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.kalypso_native import sembench_inputs, adapter, map_observation, movie_map, movie_map_inputs, native_server
from src.experiments.kalypso_query_lifecycle import query_driver_type
from tests.baselines.text import test_kalypso_movie_map as movie_tests
from tests.experiments import test_sembench_text_original as original_tests


def map_spec(count=3):
    return dict(task='sembench-map', scenario='ecomm', query='5', query_id='q',
                expected_calls=count, max_native_calls=2*count)


class MapTaskContracts(unittest.TestCase):
    def test_shared_driver_and_native_retry_budget_do_not_depend_on_movie_question_ids(self):
        for count in (1,7,13):
            spec=map_spec(count)
            unit=dict(call_cap=2*count, queries=[spec])
            self.assertIs(query_driver_type(unit), KalypsoRunner)
            self.assertEqual(native_server.query_task(spec), dict(scenario='ecomm',query_id='5'))
            self.assertEqual(native_server.physical_call_caps(unit),dict(q=2*count))
            spec['max_native_calls']=count
            with self.assertRaisesRegex(ValueError,'twice'):
                native_server.physical_call_caps(unit)


@unittest.skipUnless(original_tests.READY,'requires the pinned public SemBench checkout and driver SDK')
class OriginalMapContracts(unittest.TestCase):
    def setUp(self):
        self.helper=original_tests.OriginalSemBenchTextTests()
        self.helper.setUp();self.addCleanup(self.helper.doCleanups)
        self.source=self.helper._write_ecomm_inputs()
        self.cfg=dict(sembench=os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'],
            data_dir=str(self.helper.data),model='m',model_name='openai/local-fixture',
            native_trace=str(self.helper.data/'trace.jsonl'),
            map_prompt_root=str(self.helper.data/'prompts'),execution_enabled=False,CPU_fixture_only=True)
        self.freeze_source()

    def freeze_source(self):
        self.cfg['source_data_sha256']={'styles_details.parquet':adapter.sha(self.helper.data/'styles_details.parquet')}

    def test_complete_multifield_sources_restore_original_categories_and_evaluator(self):
        import pandas as pd

        for count in (1,7,13):
            with self.subTest(count=count):
                selected=self.source.iloc[[i%3 for i in range(count)]].copy()
                selected['id']=range(1000,1000+count)
                pd.concat([self.source.iloc[3:],selected],ignore_index=True).to_parquet(
                    self.helper.data/'styles_details.parquet',index=False)
                self.freeze_source()
                spec=map_spec(count);unit=dict(call_cap=2*count,queries=[spec]);self.cfg['units']=[unit]
                self.cfg['map_prompt_root']=str(self.helper.data/f'prompts-{count}')
                query=sembench_inputs.make_query(self.cfg,'q',spec)
                answers=[('Topwear','Topwear','Socks')[i%3] for i in range(count)]
                expected,_=self.helper._reference(query,answers)
                expected_quality=query.evaluate(expected)
                with patch.object(movie_map_inputs,'binding',return_value={'explicit_CPU_fixture':True}):
                    frozen=movie_map_inputs.prepare_inputs(self.cfg,unit,self.helper.data/f'freeze-{count}')
                    self.cfg['native_cpu_inputs']={key:frozen[key] for key in ('manifest','sha256')}
                    _,loaded=movie_map_inputs.load_inputs(self.cfg)
                rows=loaded[0]['table']['rows']
                self.assertEqual([r['source_index'] for r in rows],list(range(3,3+count)))
                self.assertEqual(len({r['row_id'] for r in rows}),count)
                for row,record in zip(rows,query.operation.records,strict=True):
                    self.assertEqual(row['fields'],record.values)
                    self.assertEqual(json.loads(row['data']),record.values)
                    self.assertNotIn('id',row['fields'])
                    self.assertNotIn('subCategory',row['fields'])
                    self.assertIn('汉字',row['fields']['productDescriptors'])
                # Identical inputs remain separate occurrences with separate replies.
                if count>1:self.assertEqual(rows[0]['data'],rows[1]['data'])
                response,events=movie_tests.trace(count);response['latency_sec']=.25
                for row in response['results']:row['outputs'][0]['value']=answers[row['idx']]
                for event in events:
                    if event['event']=='map_result':event['text']=answers[event['idx']]
                def transport(payload,qid,directory):
                    self.assertEqual(payload['ops'],[dict(op='sem_map',args=dict(prompt=query.operation.stage.method.instruction))])
                    self.assertEqual(pd.read_csv(payload['data_path'])['data'].tolist(),[row['data'] for row in rows])
                    return response
                def save(path,value):path.write_text(json.dumps(value))
                emitted=[]
                with patch.object(adapter,'load_dependencies',return_value=save), \
                        patch.object(adapter,'read_trace',side_effect=lambda path,qid:
                            [] if Path(path).name=='map-trace.jsonl' else events):
                    driver=KalypsoRunner(self.cfg,unit,self.helper.data/f'run-{count}',transport)
                    driver.model_uid='u'
                    report=driver.query(spec,emitted.append)
                self.assertEqual(report['result'],expected.to_dict(orient='split'))
                self.assertEqual(report['quality'],expected_quality)
                self.assertEqual(report['physical_native_calls'],count)
                self.assertEqual(report['physical_call_cap'],2*count)
                self.assertEqual(emitted,['memory_sink_ready','result_ready'])
                self.assertEqual(report['memory_sink_timing']['query_clock_version'],'kalypso.source_to_sink.v1')
                self.assertIn('ecomm Q5',report['semantic_operator'])

    def test_original_filter_extract_join_are_not_relabelled_as_map(self):
        for query_id in ('1','3','7'):
            with self.subTest(query=query_id),self.assertRaisesRegex(ValueError,'ordinary text Map'):
                sembench_inputs.make_query(self.cfg,'q',dict(map_spec(),query=query_id))

    def test_empty_original_frontier_preserves_zero_rows_without_native_retries(self):
        self.source.iloc[3:].to_parquet(self.helper.data/'styles_details.parquet',index=False)
        self.freeze_source()
        spec=map_spec(0);unit=dict(call_cap=0,queries=[spec]);self.cfg['units']=[unit]
        self.assertEqual(native_server.physical_call_caps(unit),{'q':0})
        query=sembench_inputs.make_query(self.cfg,'q',spec)
        expected,_=self.helper._reference(query,[])
        response,events=movie_tests.trace(0);response['latency_sec']=.1
        with patch.object(adapter,'load_dependencies',return_value=lambda p,v:p.write_text(json.dumps(v))), \
                patch.object(adapter,'read_trace',return_value=events):
            driver=KalypsoRunner(self.cfg,unit,self.helper.data/'zero-run',lambda *a:response)
            driver.model_uid='u';report=driver.query(spec,lambda event:None)
        self.assertEqual(report['result'],expected.to_dict(orient='split'))
        self.assertEqual(report['physical_native_calls'],0)
        self.assertEqual(report['physical_call_cap'],0)
        self.assertEqual(report['native_API_units'],[])
        if os.environ.get('SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'):
            from tests.baselines.text.kalypso_join_author import empty_cpu_plan
            with patch.object(movie_map_inputs,'binding',return_value={'explicit_CPU_fixture':True}):
                frozen=movie_map_inputs.prepare_inputs(self.cfg,unit,self.helper.data/'zero-cpu-inputs')
                self.cfg['native_cpu_inputs']={key:frozen[key] for key in ('manifest','sha256')}
                cpu=empty_cpu_plan(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'],self.cfg,
                    self.helper.data/'zero-cpu',kind='map')
            self.assertIsNone(cpu['plans'][0]['max_prompt_tokens'])

    def test_explicit_movie_query_reuses_the_existing_map_policy(self):
        spec=dict(map_spec(),scenario='movie',query='Q9')
        cfg=dict(self.cfg,source_data_sha256={'Reviews.csv':adapter.sha(self.helper.data/'Reviews.csv')})
        query=sembench_inputs.make_query(cfg,'q',spec)
        self.assertEqual(query.ranking,'map')
        self.assertEqual(query.operation.describe()['kind'],'map')
        with self.assertRaisesRegex(ValueError,'ordinary text Map'):
            sembench_inputs.make_query(cfg,'q',dict(spec,ranking='topk'))

    def test_multifield_context_is_stable_across_equivalent_column_iteration_orders(self):
        from dataclasses import replace
        query=sembench_inputs.make_query(self.cfg,'q',map_spec())
        left,_=movie_map.prepare(self.cfg,map_spec(),self.helper.data/'left',query.operation)
        records=tuple(replace(row,values=dict(reversed(list(row.values.items())))) for row in query.operation.records)
        cfg=dict(self.cfg,map_prompt_root=str(self.helper.data/'right-prompts'))
        right,_=movie_map.prepare(cfg,map_spec(),self.helper.data/'right',replace(query.operation,records=records))
        self.assertEqual([row['data'] for row in left],[row['data'] for row in right])

    def test_full_parquet_identity_is_required_before_model_submission(self):
        with self.assertRaisesRegex(ValueError,'file hashes'):
            sembench_inputs.make_query(dict(self.cfg,source_data_sha256={}),'q',map_spec())
        changed=dict(self.cfg,source_data_sha256={'styles_details.parquet':'wrong'})
        with self.assertRaisesRegex(ValueError,'source data changed'):
            sembench_inputs.make_query(changed,'q',map_spec())

    @unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'),
        'requires author05024 Git objects for the original template functions')
    def test_original_author_template_receives_all_fields_without_model_or_executor_changes(self):
        source=os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT']
        def author_file(name):
            return subprocess.check_output(['git','-C',source,'show',
                native_server.AUTHOR_SOURCE_COMMIT+':vllm/kalypso/sem_ops/'+name],text=True)
        names=('SEM_FILTER','SEM_MAP','SEM_AGG','SEM_TOPK','SEM_JOIN','SEM_CLASSIFY')
        op=NS(**{name:name for name in names})
        scope=dict(OpName=op)
        tree=ast.parse(author_file('prompt_utils.py'))
        tree.body=[node for node in tree.body if not isinstance(node,(ast.Import,ast.ImportFrom))]
        exec(compile(tree,'author05024/prompt_utils.py','exec'),scope)
        map_class=next(node for node in ast.parse(author_file('map.py')).body
                       if isinstance(node,ast.ClassDef) and node.name=='SemMap')
        method=next(node for node in map_class.body if isinstance(node,ast.FunctionDef) and node.name=='_build_prompt')
        exec(compile(ast.Module(body=[method],type_ignores=[]),'author05024/map.py','exec'),scope)
        async def no_model(*args,**kwargs):raise AssertionError('prompt test submitted a model task')
        Map=type('Map',(),dict(_build_prompt=scope['_build_prompt'],_run_single=no_model))
        Estimator=type('Estimator',(),dict(reset=lambda self:None))
        helpers=ModuleType('vllm.kalypso.sem_ops.prompt_utils');helpers.__dict__.update(scope)
        base=ModuleType('vllm.kalypso.sem_ops.base');base.OpName=op
        context=ModuleType('vllm.kalypso.context');context.RETRY_TASK=object()
        query=sembench_inputs.make_query(self.cfg,'q',map_spec())
        with tempfile.TemporaryDirectory() as raw,patch.dict('sys.modules',
                {helpers.__name__:helpers,base.__name__:base,context.__name__:context}):
            rows,_=movie_map.prepare(self.cfg,map_spec(),Path(raw)/'input',query.operation)
            binding=map_observation.install(self.cfg,dict(queries=[map_spec()],call_cap=6),Path(raw),
                native_server,Map,Estimator)
            instance=Map();instance.instruction=query.operation.stage.method.instruction
            for row in rows:
                data=scope['get_data_prompt'](row['data'].strip())
                ctx=NS(input=NS(data=data,right_data=[]),state=NS(idx=row['idx'],
                    raw_request=NS(headers={'X-Kalypso-Observation':'q'})))
                data_prompt,prompt=instance._build_prompt(ctx)
                self.assertEqual(data_prompt,scope['get_system_prompt']()+data)
                self.assertEqual(prompt,scope['get_prompt'](instance.instruction.format_map(row['fields']),data,op.SEM_MAP))
                self.assertIn(row['fields']['productDisplayName'],prompt[-1]['content'])
                self.assertIn(row['fields']['productDescriptors'],prompt[-1]['content'])
                ctx.input.data=scope['get_data_prompt']('missing descriptor')
                with self.assertRaisesRegex(ValueError,'source occurrence'):instance._build_prompt(ctx)
            self.assertFalse(binding['identical_to_LOTUS_message_template'])
            self.assertTrue(binding['original_length_estimation_and_retry_implementation'])


if __name__=='__main__':unittest.main()
