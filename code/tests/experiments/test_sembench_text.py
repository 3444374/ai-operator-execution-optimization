"""Original relation restoration and single-operator query accounting boundaries."""

from dataclasses import replace
from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.sembench_text import TextOperation
from src.semantic_methods.lotus.record_types import ExtractMethod, MapMethod, RecordOccurrence, StageSpec
from src.semantic_methods.lotus.records import FilterMethod
from src.serving.backends.vllm_protocol import GenerationSpec
from src.experiments import sembench_text_query as entry


PANDAS = importlib.util.find_spec('pandas') is not None


@unittest.skipUnless(PANDAS, 'requires pandas, no framework or model')
class TextRelationTests(unittest.TestCase):
    def operation(self, kind, index=(17,18,19)):
        import pandas as pd
        frame = pd.DataFrame(dict(text=['a','b','c'], title=['A','B','C'], gold=[True,False,True]), index=list(index))
        generation = GenerationSpec()
        if kind == 'filter':
            method=FilterMethod('fixture','Accept {text}',resolved_input_columns=('text',))
        elif kind == 'map':
            method=MapMethod('fixture','Label {text}','_map',resolved_input_columns=('text',))
        else:
            method=ExtractMethod('fixture',('text',),dict(label='label'))
            generation=replace(generation,response_format={'type':'json_object'})
        records=tuple(RecordOccurrence('r'+str(p),p,label,dict(text=text))
                      for p,(label,text) in enumerate(zip(index,frame.text,strict=True)))
        return TextOperation(frame,StageSpec(kind,method,generation),records)

    def test_filter_restores_original_context_dtypes_and_source_order(self):
        import pandas as pd
        op=self.operation('filter')
        out=op.result_frame((op.records[2],op.records[0]))
        pd.testing.assert_frame_equal(out,op.frame.iloc[[0,2]])

    def test_map_uses_occurrences_with_duplicate_index_and_keeps_business_columns(self):
        op=self.operation('map',(7,7,11))
        records=tuple(replace(r,values=dict(r.values,_map=value))
                      for r,value in zip(op.records,('first','second','third'),strict=True))
        out=op.result_frame(tuple(reversed(records)))
        self.assertEqual(out['_map'].tolist(),['first','second','third'])
        self.assertEqual(out.title.tolist(),['A','B','C'])
        self.assertEqual(out.index.tolist(),[7,7,11])
        self.assertNotIn('gold',records[0].values)

    def test_extract_preserves_actual_extra_and_collision_keys_with_label_assignment(self):
        op=self.operation('extract',(7,7,11))
        updates=({'label':'early','title':'changed'},{'label':'late','extra':'present'}, {})
        records=tuple(replace(r,values=dict(r.values,**u),index_updates=u)
                      for r,u in zip(op.records,updates,strict=True))
        out=op.result_frame(records)
        self.assertEqual(out.title.tolist(),['changed','changed','C'])
        self.assertEqual(out.label.tolist(),['late','late',None])
        self.assertEqual(out.extra.tolist(),['present','present',None])
        self.assertEqual(list(out.columns),['text','title','gold','label','extra'])

    def test_extract_empty_json_does_not_invent_declared_columns(self):
        import pandas as pd
        op=self.operation('extract')
        pd.testing.assert_frame_equal(op.result_frame(op.records),op.frame)
        empty=replace(op,frame=op.frame.iloc[:0],records=())
        self.assertEqual(empty.describe()['model_columns'],['text'])
        self.assertNotIn('label',empty.result_frame(()).columns)

    def test_empty_map_adds_native_output_column_without_framework_allocation(self):
        op=self.operation('map')
        empty=replace(op,frame=op.frame.iloc[:0],records=())
        out=empty.result_frame(())
        self.assertEqual(list(out.columns),['text','title','gold','_map'])
        self.assertEqual(len(out),0)


class TextEntryTests(unittest.TestCase):
    def test_source_module_registers_dataclasses_and_restores_failed_imports(self):
        from src.baselines.text import sembench_source as source

        name = 'sembench_loader_fixture'
        sentinel = object()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'src').mkdir()
            path = root / 'src/fixture.py'
            path.write_text('from __future__ import annotations\nfrom dataclasses import dataclass\n'
                            '@dataclass\nclass Metric:\n    score: float = 1.0\n')
            with patch.object(source, 'verify_checkout', return_value=root), \
                    patch.dict(sys.modules, {'runner.generic_runner': None,
                        'evaluator.generic_evaluator': None, name: sentinel}):
                module = source.load_module(root, name, 'src/fixture.py')
                self.assertIs(sys.modules[name], module)
                self.assertEqual(module.Metric().score, 1)
                path.write_text('raise RuntimeError("original import failed")\n')
                with self.assertRaisesRegex(RuntimeError, 'original import failed'):
                    source.load_module(root, name, 'src/fixture.py')
                self.assertIs(sys.modules[name], module)

    def test_help_does_not_load_framework_or_model(self):
        with self.assertRaises(SystemExit) as stopped,redirect_stdout(io.StringIO()):
            entry.main(['--help'])
        self.assertEqual(stopped.exception.code,0)

    def test_configuration_keeps_optional_engine_capacity_unset(self):
        config=dict(schema='semloom.sembench_text_run.v1',scenario='movie',query='8',query_id='q8',
            backend='lotus-native',model_name='fixture',generation={},
            engine=dict(model_path='/tmp/model',model_python='/tmp/python',driver_python='/tmp/python',max_model_calls=120),
            resources={},profile=dict(native_lm_max_batch_size=4))
        with tempfile.TemporaryDirectory() as raw:
            path=Path(raw)/'config.json'
            path.write_text(json.dumps(config))
            _,engine,_,_=entry.configuration(path)
        for name in ('max_model_len','max_num_seqs','max_num_batched_tokens','dtype','gpu_memory_utilization'):
            self.assertIsNone(getattr(engine,name))

    def test_fixture_rejects_incomplete_answers_before_operator_execution(self):
        query=SimpleNamespace(describe=lambda:dict(expected_model_calls=2))
        with self.assertRaisesRegex(ValueError,'every original operator input'):
            entry.fixture({},query,['one'],Path('/tmp'))

    def test_native_ray_config_keeps_full_sink_route(self):
        template = Path(__file__).parents[2] / 'configs/sembench_ray_native.example.json'
        config, _, _, _ = entry.configuration(template)
        self.assertIn('ray_native', config)

    def test_native_ray_uses_its_runtime_capacity_without_old_record_worker_limit(self):
        template = Path(__file__).parents[2] / 'configs/sembench_ray_native.example.json'
        config = json.loads(template.read_text())
        config['resources']['cpu_workers'] = 2
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw)/'native.json'
            path.write_text(json.dumps(config))
            _, _, _, resources = entry.configuration(path)
            self.assertEqual(resources.cpu_workers, 2)
            config.pop('ray_native')
            path.write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError, 'record graphs'):
                entry.configuration(path)

    def test_exact_join_checks_native_ray_processor_without_borrowing_old_record_graph_support(self):
        from src.serving.backends.vllm_protocol import EngineSpec
        engine=EngineSpec(model_path='/tmp/model',model_python='/tmp/python',driver_python='/tmp/python',max_model_calls=6)
        query=SimpleNamespace(generation=GenerationSpec(),operation=SimpleNamespace(
            describe=lambda:dict(kind='join'),generation=GenerationSpec()),
            describe=lambda:dict(expected_model_calls=6))
        config=dict(backend='ray-data',profile={},ray_native={})
        self.assertEqual(entry.check(config,query,engine)['inference_calls'],0)
        with self.assertRaises(ValueError):entry.check(dict(backend='ray-data',profile={}),query,engine)

    def test_backend_support_uses_actual_operator_independently_of_query_number(self):
        from src.serving.backends.vllm_protocol import EngineSpec
        engine = EngineSpec(model_path='/tmp/model', model_python='/tmp/python',
                            driver_python='/tmp/python', max_model_calls=3)
        for query_id in ('1', '5', '9', 'another-task'):
            for backend in entry.BACKENDS:
                for lifecycle in ('isolated', 'resident'):
                    for kind in ('filter', 'map', 'extract', 'join', 'topk', 'aggregate'):
                        with self.subTest(query_id=query_id, backend=backend, lifecycle=lifecycle, kind=kind):
                            query = SimpleNamespace(generation=GenerationSpec(),
                                operation=SimpleNamespace(describe=lambda: dict(kind=kind), generation=GenerationSpec()),
                                describe=lambda: dict(query=query_id, expected_model_calls=3))
                            config = dict(backend=backend, lifecycle=lifecycle, profile={})
                            supported = (kind in ('filter', 'map', 'extract') or
                                kind in ('join', 'topk') and backend == 'lotus-native' and lifecycle == 'resident')
                            if supported:
                                checked = entry.check(config, query, engine)
                                self.assertEqual(checked['inference_calls'], 0)
                            else:
                                with self.assertRaises(ValueError):
                                    entry.check(config, query, engine)

    def test_result_projection_preserves_original_ratio_precision(self):
        import pandas as pd
        value=2/3
        report=entry._frame_value(pd.DataFrame({'positivity_ratio':[value]}))
        self.assertEqual(report['data'],[[value]])
        self.assertEqual(json.loads(json.dumps(report))['data'],[[value]])

    def test_missing_occurrence_fails_before_evaluation_and_keeps_backend_evidence(self):
        query=SimpleNamespace(describe=lambda:dict(expected_model_calls=2),
            operation=SimpleNamespace(records=[SimpleNamespace(row_id='r0',source_position=0),
                                                SimpleNamespace(row_id='r1',source_position=1)]),
            execute=lambda execute:(None,dict(calls=[dict(row_id='r0',source_position=0,status='succeeded')],
                                              backend_close=dict(status='closed'))),
            evaluate=lambda frame:self.fail('partial result was evaluated'))
        with self.assertRaises(RuntimeError) as failed:
            entry.run_query(dict(backend='ray-data'),query,None,None,Path('/tmp'))
        report=failed.exception.query_report
        self.assertEqual(report['status'],'failed')
        self.assertIsNone(report['source_to_sink_seconds'])
        self.assertEqual(len(report['operator']['calls']),1)
        self.assertNotIn('quality',report)

    def test_isolated_time_is_never_promoted_to_uniform_source_to_sink(self):
        import pandas as pd
        result = pd.DataFrame({'score': [2 / 3]})
        for backend in entry.BACKENDS:
            with self.subTest(backend=backend):
                query = SimpleNamespace(describe=lambda: dict(expected_model_calls=0), operation=None,
                    execute=lambda execute: (result, dict(calls=[], backend_close=dict(status='closed'))),
                    evaluate=lambda frame: {'score': float(frame.score.iloc[0])})
                # Query return is after the operator's audit/close. Later
                # accounting and evaluation do not change that legacy scope.
                with patch.object(entry.time, 'monotonic', side_effect=[100, 118, 121, 128, 128]):
                    report = entry.run_query(dict(backend=backend), query, None, None, Path('/tmp'))
                self.assertEqual(report['status'], 'completed')
                self.assertEqual(report['query_seconds'], 18)
                self.assertEqual(report['evaluation_seconds'], 7)
                self.assertEqual(report['elapsed_seconds'], 28)
                self.assertIsNone(report['source_to_sink_seconds'])
                self.assertIn('audit and close', report['source_to_sink_unavailable_reason'])
                self.assertEqual(report['quality']['score'], 2 / 3)

    def test_insufficient_claim_is_recorded_without_allocating_a_model_or_refunding(self):
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        query=SimpleNamespace(describe=lambda:dict(expected_model_calls=2))
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw)
            ledger=CellBudgetLedger.create(root/'budget.sqlite',AttemptBudget('cpu',1),deadline_utc=None)
            ledger.reserve_unit('q',1)
            config=dict(backend='lotus-native',budget=dict(path=str(root/'budget.sqlite'),id='cpu',limit=1,unit_id='q'))
            engine=SimpleNamespace(validate_driver_environment=lambda:None)
            with patch.object(entry,'run_query') as execute,self.assertRaisesRegex(ValueError,'cannot cover'):
                entry.run_worker(config,query,engine,None,root)
            execute.assert_not_called()
            report=json.loads((root/'query.json').read_text())
            self.assertEqual(report['status'],'failed')
            self.assertEqual(report['permanent_budget']['units'][0]['claimed'],1)
            self.assertEqual(ledger.snapshot()['allocated_requests'],1)


if __name__ == '__main__':
    unittest.main()
