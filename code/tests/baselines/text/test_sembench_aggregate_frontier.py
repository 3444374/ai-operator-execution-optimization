"""Original aggregate source/sink for a system-owned reduction algorithm."""

from unittest.mock import patch
import unittest

from src.baselines.text.sembench_text import SemBenchTextQuery
from src.serving.backends.vllm_protocol import GenerationSpec
from tests.experiments import test_sembench_lotus_mmqa_aggregate as fixture


@unittest.skipUnless(fixture.CHECKOUT and fixture.HAS_SDK, 'requires original SemBench and LOTUS SDK')
class AggregateFrontierTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.NativeMMQAAggregateTests.setUpClass()

    def setUp(self):
        self.helper=fixture.NativeMMQAAggregateTests();self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)

    def test_original_selected_rows_and_final_projection_surround_one_native_reduction(self):
        import pandas as pd
        for rows in (0,1,7):
            with self.subTest(rows=rows):
                source=self.helper.source(selected=rows)
                query=SemBenchTextQuery(self.helper.checkout,source,'mmqa','Q5',execution_id='aggregate',
                    model_name='openai/no-inference',generation=GenerationSpec(),aggregate_operator=True)
                description=query.describe()
                self.assertIsNone(description['expected_model_calls'])
                self.assertEqual(description['operation']['kind'],'aggregate')
                self.assertEqual(description['operation']['input_rows'],rows)
                self.assertEqual(set(query.operation.columns),{'title','text'})
                calls=[]
                def execute(operation, unused_original):
                    calls.append(operation)
                    self.assertEqual(len(operation.records),rows)
                    self.assertTrue(all(row.values['title'] in ('Crashing','Cobweb') for row in operation.records))
                    return dict(frame=pd.DataFrame({'_output':['Lizzy Caplan'],'native_only':['retained until projection']}))
                bound=query.bind_execution()
                with patch('lotus.models.LM.__call__',side_effect=AssertionError('task invoked the LOTUS hierarchy')):
                    result,report=bound(execute)
                self.assertEqual(len(calls),1)
                self.assertEqual(result.columns.tolist(),['_output'])
                self.assertEqual(result.iloc[0,0],'Lizzy Caplan')
                query.verify_source()
                self.assertEqual(query.evaluate(result)['original_metric']['f1_score'],1)

    def test_changed_input_is_rejected_before_the_native_operator(self):
        source=self.helper.source(selected=2)
        query=SemBenchTextQuery(self.helper.checkout,source,'mmqa','5',execution_id='aggregate',
            model_name='openai/no-inference',generation=GenerationSpec(),aggregate_operator=True)
        with (source/'lizzy_caplan_text_data.csv').open('a') as stream:
            stream.write('Crashing,changed\n')
        with self.assertRaisesRegex(ValueError,'input changed'):
            query.bind_execution()


if __name__=='__main__':unittest.main()
