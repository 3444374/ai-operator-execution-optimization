"""Prepared original-query messages through the public Ray Data LLM graph.

Ray owns chat templating, tokenization, model actors, requests and batching.
The task layer retains original SemBench queries, parsing and complete sinks.
"""

from collections import Counter
from collections.abc import Mapping
from contextlib import contextmanager
from copy import deepcopy
from functools import partial
import sys
import time


_PROCESSOR_OPTIONS = frozenset((
    "batch_size", "concurrency", "max_concurrent_batches",
    "chat_template_stage", "tokenize_stage", "detokenize_stage",
    "prepare_image_stage",
))
_USAGE_FIELDS = (
    "num_input_tokens", "num_generated_tokens", "request_id", "batch_uuid",
    "time_taken_llm", "params", "metrics",
)


def _options(values):
    values = deepcopy(dict(values or {}))
    unknown = values.keys() - _PROCESSOR_OPTIONS
    if unknown:
        raise ValueError("unsupported native Ray processor options: " + ", ".join(sorted(unknown)))
    for name in ("chat_template_stage", "tokenize_stage", "detokenize_stage"):
        # Ray 2.56.1 replaces stage batch_size with processor batch_size in
        # StatefulStage.get_dataset_map_batches_kwargs; do not advertise it.
        stage = values.get(name)
        stage_batch_size = (stage.get("batch_size") if isinstance(stage, Mapping)
                            else getattr(stage, "batch_size", None))
        if stage_batch_size is not None:
            raise ValueError("native Ray stage batch_size is ignored; use processor batch_size")
    return values


def _prepared_input(row, *, sampling_params):
    return dict(messages=deepcopy(row['messages']), sampling_params=deepcopy(sampling_params))


def _prepared_output(row):
    return dict(call_id=row['call_id'], generated_text=row['generated_text'],
                native_usage={name: row[name] for name in _USAGE_FIELDS if name in row})


def _sampling_params(value):
    """Map the original Extract JSON request to Ray's native vLLM schema."""
    options = deepcopy(dict(value))
    response_format = options.pop('response_format', None)
    if response_format is not None:
        if response_format != {'type': 'json_object'} or 'structured_outputs' in options:
            raise ValueError('one unambiguous JSON object response format is required')
        options['structured_outputs'] = {'json_object': True}
    return options


class _NativeQueryOwner:
    """One local runtime; each prediction batch uses the native processor graph.

    The complete original query may make several dependent LM calls. This
    owner neither schedules those calls nor promises actor/model reuse between
    materializations. Physical GPU exit still belongs to the host supervisor.
    """

    def __init__(self, *, model, engine_kwargs, processor_options, runtime_options, engine_runtime_env):
        self.options = _options(processor_options)
        if runtime_options.get('address', 'local') != 'local':
            raise ValueError('this finite native binding requires a local owned Ray runtime')
        self.model = model
        self.engine_kwargs = deepcopy(engine_kwargs)
        self.runtime_options = deepcopy(runtime_options)
        self.engine_runtime_env = deepcopy(engine_runtime_env)
        self._ray = None
        self._closed = False
        self.report = dict(batches=[], events={}, ray_driver_disconnected=None,
            model_process_exit_confirmed=None, scheduler_owner='Ray Data LLM processor',
            lifecycle='query-owned runtime; native processor per prediction batch; model reuse unverified')

    def _start(self, events):
        if self._ray is not None:
            events['runtime_start_ns'] = self.report['events']['runtime_start_ns']
            return
        import ray
        import ray.data

        if ray.is_initialized():
            raise RuntimeError('native full-query binding requires an isolated Ray worker')
        self._ray = ray
        self.report['events']['runtime_start_ns'] = time.monotonic_ns()
        events['runtime_start_ns'] = self.report['events']['runtime_start_ns']
        ray.init(**{'address': 'local', **self.runtime_options})
        self.report['ray_version'] = ray.__version__

    def predict(self, requests, *, sampling_params):
        if self._closed:
            raise RuntimeError('native Ray query owner is closed')
        source = [dict(row) for row in requests]
        expected = [row['call_id'] for row in source]
        if len(set(expected)) != len(expected):
            raise ValueError('native Ray requests require unique occurrence call IDs')
        report = dict(predictions=[], events={}, ray_driver_disconnected=None,
            model_process_exit_confirmed=None, scheduler_owner='Ray Data LLM processor',
            first_engine_accept_ns=None, kernel_seconds=None,
            retry_policy=dict(max_restarts=0, max_task_retries=0),
            requested_config=dict(model_source=self.model, sampling_params=deepcopy(sampling_params),
                engine_kwargs=deepcopy(self.engine_kwargs), processor_options=deepcopy(self.options),
                runtime_options=deepcopy(self.runtime_options)), native_sampling_params=None)
        self.report['batches'].append(report)
        events = report['events']
        if not source:
            events['predictions_materialized_ns'] = time.monotonic_ns()
            return report

        sampling = report['native_sampling_params'] = _sampling_params(sampling_params)
        self._start(events)
        from ray.data.llm import build_processor, vLLMEngineProcessorConfig
        from ray._private.runtime_env.setup_hook import export_setup_func_module

        report['ray_version'] = self.report['ray_version']
        config = vLLMEngineProcessorConfig(model_source=self.model,
            engine_kwargs=deepcopy(self.engine_kwargs), **deepcopy(self.options))
        processor = build_processor(config,
            preprocess=partial(_prepared_input, sampling_params=sampling),
            postprocess=_prepared_output)
        stages = [processor.get_stage_by_name(name) for name in processor.list_stage_names()]
        engines = [stage for stage in stages if type(stage).__name__ == 'vLLMEngineStage']
        if len(engines) != 1:
            raise RuntimeError('one native engine stage is required for the mandatory request budget')
        stage = engines[0]
        env = deepcopy(dict(self.engine_runtime_env))
        hook = 'src.baselines.text.frameworks.ray_native_observer.setup'
        # Same version-bound setup seam as the already qualified native Q7 route.
        # Export the module hook once; vLLM's nested workers must not re-export it.
        stage.map_batches_kwargs.update(max_restarts=0, max_task_retries=0,
            runtime_env=export_setup_func_module(env, hook))
        report['processor_config'] = config.model_dump(mode='json')
        report['stages'] = processor.list_stage_names()
        events['native_graph_submit_ns'] = time.monotonic_ns()
        predictions = processor(self._ray.data.from_items(source)).materialize()
        report['predictions'] = predictions.take_all()
        events['predictions_materialized_ns'] = time.monotonic_ns()
        actual = [row['call_id'] for row in report['predictions']]
        if Counter(actual) != Counter(expected):
            raise ValueError('native Ray predictions have missing, duplicate or unknown occurrences')
        return report

    def close(self):
        self._closed = True
        if self._ray is None:
            return
        events = self.report['events']
        events['close_started_ns'] = time.monotonic_ns()
        try:
            self._ray.shutdown()
            self.report['ray_driver_disconnected'] = not self._ray.is_initialized()
            if not self.report['ray_driver_disconnected']:
                raise RuntimeError('Ray remains initialized after shutdown')
        except BaseException as error:
            self.report['close_error_type'] = type(error).__name__
            raise
        finally:
            events['close_finished_ns'] = time.monotonic_ns()
            for batch in self.report['batches']:
                batch['events'].update(close_started_ns=events['close_started_ns'],
                    close_finished_ns=events['close_finished_ns'])
                batch['ray_driver_disconnected'] = self.report['ray_driver_disconnected']
                if 'close_error_type' in self.report:
                    batch['close_error_type'] = self.report['close_error_type']


@contextmanager
def query_scope(*, model, engine_kwargs, processor_options, runtime_options, engine_runtime_env):
    """Keep the owned runtime alive through the complete original query sink.

    Entry allocates no Ray runtime. Startup happens inside the first nonempty
    predict(), so a caller may bind this scope before its real source clock.
    """
    owner = _NativeQueryOwner(model=model, engine_kwargs=engine_kwargs,
        processor_options=processor_options, runtime_options=runtime_options,
        engine_runtime_env=engine_runtime_env)
    loaded = sys.modules.get('ray')
    if loaded is not None and loaded.is_initialized():
        raise RuntimeError('native full-query binding requires an isolated Ray worker')
    primary = None
    try:
        yield owner
    except BaseException as error:
        primary = error
    finally:
        try:
            owner.close()
        except BaseException as error:
            if primary is None:
                primary = error
            else:
                primary.add_note('native Ray close failed: ' + type(error).__name__)
    if primary is not None:
        primary.native_query_report = owner.report
        raise primary


def execute_prepared(requests, *, model, sampling_params, engine_kwargs,
                     processor_options, runtime_options, engine_runtime_env, on_result_ready):
    """One prepared batch, retaining its public sink-before-close contract."""
    scope = dict(model=model, engine_kwargs=engine_kwargs, processor_options=processor_options,
        runtime_options=runtime_options, engine_runtime_env=engine_runtime_env)
    source = [dict(row) for row in requests]
    if not source:
        # The legacy empty path never borrows or closes an existing runtime.
        report = _NativeQueryOwner(**scope).predict(source, sampling_params=sampling_params)
        on_result_ready(report)
        return report
    owner = None
    try:
        with query_scope(**scope) as owner:
            report = owner.predict(source, sampling_params=sampling_params)
            on_result_ready(report)
    except BaseException as error:
        if owner is not None and owner.report['batches']:
            error.native_query_report = owner.report['batches'][-1]
        raise
    return report
