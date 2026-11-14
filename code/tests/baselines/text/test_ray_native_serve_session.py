"""Public Serve processor lifecycle with software SDK objects only."""

from copy import deepcopy
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import ray_native_serve_session as native


class ServeDeploymentStage:
    def __init__(self):
        self.map_batches_kwargs = {}


class Config:
    def __init__(self, **kwargs):
        self.value = kwargs

    def model_dump(self, **kwargs):
        return {key: value for key, value in self.value.items() if key not in kwargs.get('exclude', set())}


def software_sdk(*, existing_runtime=False, existing_app=False, start_failure=False):
    state = SimpleNamespace(initialized=existing_runtime, events=[], configs=[], requests=[],
        applications={'owned': object()} if existing_app else {}, stages=[])

    def initialize(**kwargs):
        state.initialized = True
        state.events.append('ray.init')

    def shutdown():
        state.events.append('ray.shutdown')
        state.initialized = False

    def run(app, **kwargs):
        state.events.append('serve.run')
        state.applications[kwargs['name']] = app
        if start_failure:
            raise ValueError('software deployment startup failed')

    def delete(name):
        state.events.append('delete:'+name)
        del state.applications[name]

    def deployment(config, **kwargs):
        state.configs.append(config.value)
        state.events.append('build_deployment')
        return object()

    def build(config, *, preprocess, postprocess):
        stage = ServeDeploymentStage()
        state.stages.append(stage)
        class Processor:
            def list_stage_names(self):
                return ['ServeDeploymentStage']
            def get_stage_by_name(self, name):
                return stage
            def __call__(self, rows):
                result = []
                for row in rows:
                    prepared = preprocess(row)
                    state.requests.append(prepared)
                    result.append(postprocess(dict(row, id='chatcmpl-'+row['call_id'],
                        choices=[dict(message=dict(content='{"keep":true}'), finish_reason='stop')],
                        model=prepared['request_kwargs']['model'], request_id='stage-local',
                        usage=dict(prompt_tokens=3, completion_tokens=1))))
                state.events.append('materialize')
                return SimpleNamespace(materialize=lambda: SimpleNamespace(take_all=lambda: result))
        return Processor()

    sdk = SimpleNamespace(ray=SimpleNamespace(__version__='2.56.1', is_initialized=lambda: state.initialized,
        init=initialize, shutdown=shutdown, data=SimpleNamespace(from_items=lambda rows: rows)),
        serve=SimpleNamespace(start=lambda **kwargs: state.events.append('serve.start'),
            status=lambda: SimpleNamespace(applications=state.applications), run=run, delete=delete),
        LLMConfig=Config, build_llm_deployment=deployment, ServeDeploymentProcessorConfig=Config,
        build_processor=build, ChatCompletionRequest=object, NativeObserverCallback=object)
    return sdk, state


class NativeServeSessionTests(unittest.TestCase):
    def test_original_plain_and_extract_sampling_keep_their_native_request_formats(self):
        messages=[dict(role='user',content='original LOTUS message')]
        for sampling in (dict(n=1,max_tokens=8192,temperature=0,stop=[]),
                dict(n=1,max_tokens=8192,response_format={'type':'json_object'}),
                dict(n=1,max_tokens=8192,structured_outputs={'json_object':True})):
            before=deepcopy(sampling)
            value=native._input(dict(messages=messages),model='served',sampling_params=sampling)['request_kwargs']
            expected=deepcopy(sampling)
            if expected.pop('structured_outputs',None) is not None:
                expected['response_format']={'type':'json_object'}
            self.assertEqual(value,dict(expected,model='served',messages=messages,stream=False))
            self.assertEqual(sampling,before)

    def options(self):
        return dict(model='served-model', model_source='unused-local-model', app_name='owned',
            deployment_name='single-model', engine_kwargs={'enable_prefix_caching': True},
            processor_options={'batch_size': 2, 'concurrency': 1}, runtime_options={},
            model_runtime_env=dict(py_executable='unused-python', env_vars={
                'PYTHONPATH': 'unused-code', 'SEMLOOM_RAY_NATIVE_OBSERVER': 'unused-budget'}))

    def test_one_owned_deployment_serves_two_native_graphs_until_session_close(self):
        sdk, state = software_sdk()
        options = self.options()
        messages = [dict(role='system', content='independent JSON'), dict(role='user', content='text')]
        original = deepcopy(messages)
        with patch.object(native, '_load_native', return_value=sdk):
            owner = native.NativeServeSession(**options)
            owner.start()
            for index in range(2):
                result = owner.predict([dict(call_id=str(index), messages=messages)],
                    sampling_params=dict(n=1, max_tokens=32, structured_outputs={'json_object': True}))
                self.assertEqual(result['predictions'][0]['call_id'], str(index))
                self.assertEqual(result['predictions'][0]['response_id'], 'chatcmpl-'+str(index))
                self.assertNotEqual(result['predictions'][0]['response_id'],
                    result['predictions'][0]['native_response']['request_id'])
            self.assertNotIn('delete:owned', state.events)
            owner.close()
            owner.close()
        self.assertEqual(messages, original)
        self.assertEqual(state.events.count('build_deployment'), 1)
        self.assertEqual(state.events.count('serve.run'), 1)
        self.assertEqual(state.events.count('materialize'), 2)
        self.assertEqual(state.events[-2:], ['delete:owned', 'ray.shutdown'])
        config = state.configs[0]
        self.assertEqual(config['deployment_config'], dict(name='single-model', num_replicas=1))
        self.assertEqual(config['runtime_env'], options['model_runtime_env'])
        self.assertEqual(config['callback_config'], dict(callback_class=sdk.NativeObserverCallback))
        self.assertTrue(all(not request['request_kwargs']['stream'] for request in state.requests))
        self.assertTrue(all(request['request_kwargs']['response_format'] == {'type': 'json_object'} for request in state.requests))
        self.assertTrue(all('SEMLOOM_RAY_NATIVE_OBSERVER' not in stage.map_batches_kwargs['runtime_env']['env_vars']
                            for stage in state.stages))
        self.assertTrue(owner.report['driver_disconnected'])
        self.assertIsNone(owner.report['model_process_exit_confirmed'])

    def test_existing_runtime_or_application_is_never_deleted(self):
        for runtime in (True, False):
            sdk, state = software_sdk(existing_runtime=runtime, existing_app=not runtime)
            with self.subTest(runtime=runtime), patch.object(native, '_load_native', return_value=sdk):
                owner = native.NativeServeSession(**self.options())
                with self.assertRaises((RuntimeError, ValueError)):
                    owner.start()
                owner.close()
                self.assertNotIn('delete:owned', state.events)
                if runtime:
                    self.assertNotIn('ray.shutdown', state.events)

    def test_partially_created_owned_application_is_deleted_after_startup_failure(self):
        sdk, state = software_sdk(start_failure=True)
        with patch.object(native, '_load_native', return_value=sdk):
            owner = native.NativeServeSession(**self.options())
            with self.assertRaisesRegex(ValueError, 'startup failed'):
                owner.start()
            owner.close()
        self.assertEqual(state.events[-2:], ['delete:owned', 'ray.shutdown'])

    def test_errors_streaming_chunks_and_multiple_choices_are_not_complete_predictions(self):
        valid = dict(call_id='row', id='response', choices=[dict(
            finish_reason='stop', message=dict(content='{"keep":true}'))])
        for value in (dict(valid, error={'message': 'failed'}), dict(valid, choices=[]),
                      dict(valid, choices=valid['choices']*2), dict(valid, choices=[dict(
                          finish_reason=None, message=dict(content='partial'))])):
            with self.subTest(value=value), self.assertRaises(ValueError):
                native._output(value)

    def test_module_and_message_mapping_import_no_native_or_LOTUS_SDK(self):
        script = '''
import importlib.abc,sys
class Guard(importlib.abc.MetaPathFinder):
 def find_spec(self,name,*args):
  if name.split('.')[0] in ('ray','vllm','torch','transformers','lotus'):
   raise AssertionError('forbidden SDK import: '+name)
sys.meta_path.insert(0,Guard())
from src.baselines.text.frameworks.ray_native_serve_session import _input,_output
r=_input({'messages':[{'role':'user','content':'text'}]},model='fixture',sampling_params={'n':1,'structured_outputs':{'json_object':True}})
assert r['request_kwargs']['stream'] is False
'''
        result = subprocess.run([sys.executable, '-B', '-c', script], capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
