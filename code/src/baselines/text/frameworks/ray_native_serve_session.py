"""Owned Ray Serve deployment shared by complete native Ray Data queries.

Ray 2.56.1's public ServeDeploymentProcessorConfig owns submission and batching.
The isolated native caller owns Ray Serve and its vLLM request protocol;
imports, deployment and model construction happen only in start(). Model owner
identity and physical GPU exit belong to the caller's host observations.
"""

from collections import Counter
from copy import deepcopy
from functools import partial
from types import SimpleNamespace
import time


def _load_native():
    import ray
    import ray.data
    from ray import serve
    from ray.data.llm import ServeDeploymentProcessorConfig, build_processor
    from ray.serve.llm import LLMConfig, build_llm_deployment
    from ray.serve.llm.openai_api_models import ChatCompletionRequest
    from src.baselines.text.frameworks.ray_native_serve_observer import NativeObserverCallback

    return SimpleNamespace(ray=ray, serve=serve, LLMConfig=LLMConfig,
        build_llm_deployment=build_llm_deployment,
        ServeDeploymentProcessorConfig=ServeDeploymentProcessorConfig,
        build_processor=build_processor, ChatCompletionRequest=ChatCompletionRequest,
        NativeObserverCallback=NativeObserverCallback)


def _input(row, *, model, sampling_params):
    options = deepcopy(sampling_params)
    structured = options.pop('structured_outputs', None)
    if structured is not None:
        if structured != {'json_object': True} or 'response_format' in options:
            raise ValueError('Serve requires one unambiguous JSON-object response format')
        options['response_format'] = {'type': 'json_object'}
    return dict(method='chat', dtype='ChatCompletionRequest', request_kwargs=dict(
        options, model=model, messages=deepcopy(row['messages']), stream=False))


def _output(row):
    if row.get('error') is not None or row.get('__inference_error__'):
        raise ValueError('native Serve returned an error response')
    choices = row['choices']
    if (len(choices) != 1 or choices[0]['finish_reason'] is None
            or not isinstance(choices[0]['message']['content'], str)
            or not isinstance(row['id'], str) or not row['id']):
        raise ValueError('native Serve requires one complete non-streaming text response')
    response = {key: deepcopy(row[key]) for key in
        ('id', 'model', 'choices', 'usage', 'request_id', 'batch_uuid', 'time_taken') if key in row}
    return dict(call_id=row['call_id'], generated_text=choices[0]['message']['content'],
        response_id=row['id'], native_response=response)


def check_options(runtime_options, processor_options):
    if runtime_options.get('address', 'local') != 'local':
        raise ValueError('owned Serve sessions require an isolated local Ray runtime')
    if processor_options.keys() - {'batch_size', 'concurrency'}:
        raise ValueError('Serve processor options support batch_size and concurrency only')


class NativeServeSession:
    """Own one deployment; processor materializations do not own the model."""

    def __init__(self, *, model, model_source, app_name, deployment_name,
                 engine_kwargs, processor_options, runtime_options, model_runtime_env):
        check_options(runtime_options, processor_options)
        self.model, self.model_source = model, model_source
        self.app_name, self.deployment_name = app_name, deployment_name
        self.engine_kwargs, self.processor_options = deepcopy(engine_kwargs), deepcopy(processor_options)
        self.runtime_options, self.model_runtime_env = deepcopy(runtime_options), deepcopy(model_runtime_env)
        self.native, self.owns_runtime, self.creation_attempted = None, False, False
        self.closed = False
        self.report = dict(profile='native-ray-serve', events={}, batches=[],
            app_name=app_name, deployment_name=deployment_name, num_replicas=1,
            scheduler_owner='Ray Data ServeDeploymentProcessor and Ray Serve',
            model_process_exit_confirmed=None, driver_disconnected=None)

    def start(self):
        if self.closed or self.native is not None:
            raise RuntimeError('native Serve session cannot be started twice or after close')
        self.native = sdk = _load_native()
        if sdk.ray.is_initialized():
            raise RuntimeError('owned Serve sessions require an isolated Ray worker')
        self.owns_runtime = True
        events = self.report['events']
        events['runtime_start_ns'] = time.monotonic_ns()
        sdk.ray.init(**dict(self.runtime_options, address='local'))
        if sdk.ray.__version__ != '2.56.1':
            raise ValueError('the Serve profile is fixed to Ray 2.56.1')
        self.report['ray_version'] = sdk.ray.__version__
        sdk.serve.start(proxy_location='Disabled')
        if self.app_name in sdk.serve.status().applications:
            raise ValueError('the named Serve application already exists')
        events['deployment_start_ns'] = time.monotonic_ns()
        config = sdk.LLMConfig(model_loading_config=dict(model_id=self.model, model_source=self.model_source),
            engine_kwargs=deepcopy(self.engine_kwargs), runtime_env=deepcopy(self.model_runtime_env),
            callback_config=dict(callback_class=sdk.NativeObserverCallback),
            deployment_config=dict(name=self.deployment_name, num_replicas=1))
        app = sdk.build_llm_deployment(config, override_serve_options=dict(name=self.deployment_name))
        self.creation_attempted = True
        sdk.serve.run(app, name=self.app_name, route_prefix=None)
        events['deployment_ready_ns'] = time.monotonic_ns()
        self.report['deployment_startup_seconds'] = (events['deployment_ready_ns']-events['deployment_start_ns'])/1e9

    def predict(self, requests, *, sampling_params):
        if self.closed or not self.report['events'].get('deployment_ready_ns'):
            raise RuntimeError('native Serve session is not ready')
        source = [dict(row) for row in requests]
        expected = [row['call_id'] for row in source]
        if len(expected) != len(set(expected)):
            raise ValueError('native Serve requires unique occurrence call IDs')
        batch = dict(predictions=[], input_call_ids=expected, events={})
        self.report['batches'].append(batch)
        if not source:
            batch['events']['predictions_materialized_ns'] = time.monotonic_ns()
            return batch
        sdk = self.native
        config = sdk.ServeDeploymentProcessorConfig(app_name=self.app_name,
            deployment_name=self.deployment_name,
            dtype_mapping={'ChatCompletionRequest': sdk.ChatCompletionRequest},
            should_continue_on_error=False, **deepcopy(self.processor_options))
        processor = sdk.build_processor(config,
            preprocess=partial(_input, model=self.model, sampling_params=sampling_params), postprocess=_output)
        stages = [processor.get_stage_by_name(name) for name in processor.list_stage_names()]
        if len(stages) != 1 or type(stages[0]).__name__ != 'ServeDeploymentStage':
            raise RuntimeError('one native Serve deployment stage is required')
        client_runtime = {key: deepcopy(value) for key, value in self.model_runtime_env.items() if key != 'env_vars'}
        client_runtime['env_vars'] = {key: value for key, value in self.model_runtime_env.get('env_vars', {}).items()
                                      if key != 'SEMLOOM_RAY_NATIVE_OBSERVER'}
        stages[0].map_batches_kwargs.update(max_restarts=0, max_task_retries=0, runtime_env=client_runtime)
        batch['processor_config'] = config.model_dump(mode='json', exclude={'dtype_mapping'})
        batch['events']['native_graph_submit_ns'] = time.monotonic_ns()
        returned = processor(sdk.ray.data.from_items(source)).materialize()
        batch['predictions'] = returned.take_all()
        batch['events']['predictions_materialized_ns'] = time.monotonic_ns()
        if Counter(row['call_id'] for row in batch['predictions']) != Counter(expected):
            raise ValueError('native Serve predictions have missing, duplicate or unknown occurrences')
        return batch

    def close(self):
        if self.closed:
            return
        self.closed = True
        if not self.owns_runtime:
            return
        error = None
        events = self.report['events']
        events['close_started_ns'] = time.monotonic_ns()
        try:
            if self.creation_attempted and self.app_name in self.native.serve.status().applications:
                self.native.serve.delete(self.app_name)
                self.report['owned_app_deleted'] = True
        except BaseException as caught:
            error = caught
            self.report['delete_error_type'] = type(caught).__name__
        finally:
            try:
                self.native.ray.shutdown()
                self.report['driver_disconnected'] = not self.native.ray.is_initialized()
                if not self.report['driver_disconnected']:
                    raise RuntimeError('Ray remains initialized after owned Serve close')
            except BaseException as caught:
                if error is None:
                    error = caught
                else:
                    error.add_note('Ray shutdown also failed: '+type(caught).__name__)
            events['close_finished_ns'] = time.monotonic_ns()
        if error is not None:
            raise error
