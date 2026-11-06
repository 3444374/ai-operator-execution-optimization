"""Actual async conversion through the optional child sidecar; no runtime."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import patch


class BootstrapTests(unittest.TestCase):
    def test_sidecar_observes_actual_conversion_without_changing_results_or_errors(self):
        from types import SimpleNamespace
        from src.serving.backends import vllm_async_engine as serving, vllm_engine as legacy
        from src.serving.backends import vllm_protocol as protocol
        path=Path(__file__).resolve().parents[2]/'scripts/experiments/probe_movie_cancellation_child.py'
        spec=importlib.util.spec_from_file_location('cancellation_conversion_test',path)
        bootstrap=importlib.util.module_from_spec(spec);spec.loader.exec_module(bootstrap)
        class Native:
            async def add_request(self,*args,**kwargs):return None
            async def abort(self,*args,**kwargs):return None
        modules={name:ModuleType(name) for name in ('vllm','vllm.v1','vllm.v1.engine','vllm.v1.engine.async_llm')}
        for value in modules.values():value.__path__=[]
        modules['vllm.v1.engine.async_llm'].AsyncLLM=Native
        owner=object.__new__(serving.VllmAsyncEngine)
        owner.engine_uid='conversion-fixture'
        owner._llm=SimpleNamespace(get_tokenizer=lambda:None)
        occurrence=dict(query_id='lifecycle-fixture',row_id='one',source_position=0,call_index=0,
            call_id='fixture-call',model_role='main',algorithm_stage='lifecycle-fixture')
        outputs={finish:SimpleNamespace(request_id='native-'+finish,finished=True,prompt_token_ids=[1],
            outputs=[SimpleNamespace(index=0,token_ids=[2],text='fixture',finish_reason=finish)])
            for finish in ('stop','abort')}
        def convert(output):
            return owner._convert(occurrence,output,output.request_id,[1],protocol.GenerationSpec(),1.,None)
        business_error=LookupError('fixture native result property')
        class BrokenOutput:
            request_id='broken'
            @property
            def outputs(self):raise business_error
        broken=BrokenOutput()
        with tempfile.TemporaryDirectory() as directory, patch('time.monotonic',return_value=2.), \
                patch.dict(sys.modules,modules), \
                patch.object(protocol,'result_from_output',protocol.result_from_output), \
                patch.object(legacy,'result_from_output',legacy.result_from_output), \
                patch.object(serving.VllmAsyncEngine,'generate_prepared',serving.VllmAsyncEngine.generate_prepared):
            before={key:convert(value) for key,value in outputs.items()}
            with self.assertRaises(LookupError) as original_error:
                owner._convert(occurrence,broken,'broken',[1],protocol.GenerationSpec(),1.,None)
            bootstrap.install(directory)
            after={key:convert(value) for key,value in outputs.items()}
            self.assertEqual(after,before)
            with self.assertRaises(LookupError) as observed_error:
                owner._convert(occurrence,broken,'broken',[1],protocol.GenerationSpec(),1.,None)
            self.assertIs(original_error.exception,business_error)
            self.assertIs(observed_error.exception,business_error)
            events=[json.loads(line) for file in Path(directory).glob('native-*.jsonl')
                    for line in file.read_text().splitlines()]
        finals={row['request_id']:row for row in events if row['event']=='native_final_observed'}
        self.assertEqual(set(finals),{'native-stop','native-abort'})
        self.assertEqual(finals['native-stop']['status'],'succeeded')
        self.assertEqual(finals['native-abort']['status'],'failed')
        self.assertEqual(after['abort']['status'],'unknown')
        self.assertFalse(after['abort']['engine_terminal_confirmed'])


if __name__ == "__main__":
    unittest.main()
