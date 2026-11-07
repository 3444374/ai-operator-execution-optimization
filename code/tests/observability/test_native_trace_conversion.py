"""Actual async conversion through optional observers; no runtime."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import patch


class BootstrapTests(unittest.TestCase):
    def test_sidecar_observes_actual_conversion_without_changing_results_or_errors(self):
        self.assert_conversion(record_observer=False)

    def test_record_observer_observes_actual_conversion_without_changing_results_or_errors(self):
        self.assert_conversion(record_observer=True)

    def test_optional_metric_error_does_not_change_successful_actual_conversion(self):
        self.assert_conversion(record_observer=True, broken_metrics=True)

    def assert_conversion(self, *, record_observer, broken_metrics=False):
        from types import SimpleNamespace
        from src.serving.backends import vllm_async_engine as serving, vllm_engine as legacy
        from src.serving.backends import vllm_protocol as protocol
        scripts=Path(__file__).resolve().parents[2]/'scripts'
        class Native:
            async def add_request(self,*args,**kwargs):return None
            async def abort(self,*args,**kwargs):return None
        modules={name:ModuleType(name) for name in ('vllm','vllm.engine','vllm.engine.arg_utils',
                 'vllm.v1','vllm.v1.engine','vllm.v1.engine.async_llm','torch','record_ipc_observer')}
        for value in modules.values():value.__path__=[]
        modules['vllm.v1.engine.async_llm'].AsyncLLM=Native
        modules['vllm.engine.arg_utils'].AsyncEngineArgs=type('Args',(),{})
        modules['record_ipc_observer'].install_transport=lambda **kwargs:None
        owner=object.__new__(serving.VllmAsyncEngine)
        owner.engine_uid='conversion-fixture'
        owner._llm=SimpleNamespace(get_tokenizer=lambda:None)
        occurrence=dict(query_id='lifecycle-fixture',row_id='one',source_position=0,call_index=0,
            call_id='fixture-call',model_role='main',algorithm_stage='lifecycle-fixture')
        outputs={finish:SimpleNamespace(request_id='native-'+finish,finished=True,prompt_token_ids=[1],
            outputs=[SimpleNamespace(index=0,token_ids=[2],text='fixture',finish_reason=finish)])
            for finish in ('stop','abort')}
        if broken_metrics:
            class Metrics:
                @property
                def arrival_time(self):raise LookupError('optional observer metrics')
            outputs['stop'].metrics=Metrics()
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
                patch.object(serving.VllmAsyncEngine,'generate_prepared',serving.VllmAsyncEngine.generate_prepared), \
                patch.object(serving.VllmAsyncEngine,'__init__',serving.VllmAsyncEngine.__init__), \
                patch.object(serving.VllmAsyncEngine,'close',serving.VllmAsyncEngine.close), \
                patch.dict(os.environ,SEM_RECORD_OBSERVATION='on',SEM_RECORD_OBSERVER_ROOT=directory):
            before={key:convert(value) for key,value in outputs.items()}
            with self.assertRaises(LookupError) as original_error:
                owner._convert(occurrence,broken,'broken',[1],protocol.GenerationSpec(),1.,None)
            if record_observer:
                (Path(directory)/'config.json').write_text(json.dumps({'observation':dict(
                    mode='on',max_events_per_process=10000,max_event_bytes=65536)}))
                path=scripts/'profiling/record_observation/record_observer.py'
            else:
                path=scripts/'experiments/probe_movie_cancellation_child.py'
            spec=importlib.util.spec_from_file_location('native_conversion_test',path)
            bootstrap=importlib.util.module_from_spec(spec);spec.loader.exec_module(bootstrap)
            try:
                if record_observer:bootstrap.install_model()
                else:bootstrap.install(directory)
                after={key:convert(value) for key,value in outputs.items()}
                self.assertEqual(after,before)
                with self.assertRaises(LookupError) as observed_error:
                    owner._convert(occurrence,broken,'broken',[1],protocol.GenerationSpec(),1.,None)
                self.assertIs(original_error.exception,business_error)
                self.assertIs(observed_error.exception,business_error)
            finally:
                if record_observer:bootstrap.close_events()
            pattern='events/*.jsonl' if record_observer else 'native-*.jsonl'
            events=[json.loads(line) for file in Path(directory).glob(pattern)
                    for line in file.read_text().splitlines()]
        final_event='native_api_native_final_observed' if record_observer else 'native_final_observed'
        finals={row['request_id']:row for row in events if row['event']==final_event}
        self.assertEqual(set(finals),{'native-abort'} if broken_metrics else {'native-stop','native-abort'})
        if not broken_metrics:self.assertEqual(finals['native-stop']['status'],'succeeded')
        self.assertEqual(finals['native-abort']['status'],'failed')
        self.assertEqual(after['stop']['status'],'succeeded')
        self.assertTrue(after['stop']['engine_terminal_confirmed'])
        self.assertEqual(after['abort']['status'],'unknown')
        self.assertFalse(after['abort']['engine_terminal_confirmed'])


if __name__ == "__main__":
    unittest.main()
