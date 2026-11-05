"""Original relation restoration and single-operator query accounting boundaries."""

from dataclasses import replace
from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
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
        self.assertEqual(len(report['operator']['calls']),1)
        self.assertNotIn('quality',report)

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
