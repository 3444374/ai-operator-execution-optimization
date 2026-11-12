"""Prepared public Ray API contracts without importing Ray or starting models."""

from copy import deepcopy
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest

from src.baselines.text.frameworks import ray_data_sembench_native as native


class NativeRayPreparedAPITests(unittest.TestCase):
    def test_sampling_and_messages_are_independent_for_every_occurrence(self):
        messages=[dict(role='user',content='same text')]
        sampling=dict(max_tokens=8192,structured_outputs=dict(regex='True|False'))
        expected=deepcopy(sampling)
        first=native._prepared_input(dict(call_id='a',messages=messages),sampling_params=sampling)
        second=native._prepared_input(dict(call_id='b',messages=messages),sampling_params=sampling)
        first['sampling_params']['structured_outputs'].pop('regex')
        first['messages'][0]['content']='native mutation'
        self.assertEqual(second,dict(messages=messages,sampling_params=expected))
        self.assertEqual(sampling,expected)

    def test_raw_answers_and_observed_usage_keep_occurrence_identity(self):
        output=native._prepared_output(dict(call_id='b',generated_text='Answer: False',
            request_id='native-1',num_generated_tokens=5,unused_label='not an output'))
        self.assertEqual(output,dict(call_id='b',generated_text='Answer: False',
            native_usage=dict(request_id='native-1',num_generated_tokens=5)))
        with self.assertRaises(KeyError):native._prepared_output(dict(call_id='b'))

    def test_unknown_or_ignored_stage_options_fail_before_native_allocation(self):
        for options in (dict(runner_threads=1),dict(max_active_batches=3),
                        dict(tokenize_stage=dict(batch_size=8)),
                        dict(chat_template_stage=SimpleNamespace(batch_size=8))):
            with self.subTest(options=options),self.assertRaises(ValueError):
                native.execute_prepared([],model='unused',sampling_params={},engine_kwargs={},
                    processor_options=options,runtime_options={},engine_runtime_env={},on_result_ready=lambda _:None)

    def test_empty_source_runs_sink_without_framework_model_or_core_imports(self):
        program='''
import importlib.abc,sys
sys.path.insert(0,sys.argv[1])
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,*unused):
        if fullname.split('.')[0] in ('ray','daft','vllm','torch','lotus') or fullname.startswith(('src.execution_provider','src.scheduling')):
            raise AssertionError('forbidden dependency: '+fullname)
sys.meta_path.insert(0,Guard())
from src.baselines.text.frameworks.ray_data_sembench_native import execute_prepared
ready=[]
result=execute_prepared([],model='unused',sampling_params={},engine_kwargs={},processor_options={},
    runtime_options={},engine_runtime_env={},on_result_ready=lambda value:ready.append(value))
assert result['predictions']==[] and ready==[result]
assert result['events']['predictions_materialized_ns']>0
'''
        result=subprocess.run([sys.executable,'-B','-c',program,str(Path(__file__).resolve().parents[3])],
            capture_output=True,text=True,timeout=20)
        self.assertEqual(result.returncode,0,result.stderr)


if __name__=='__main__':unittest.main()
