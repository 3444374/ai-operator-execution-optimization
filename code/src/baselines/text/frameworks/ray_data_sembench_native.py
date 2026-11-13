"""Prepared original-query messages through the public Ray Data LLM graph.

Ray owns chat templating, tokenization, model actors, requests and batching.
The task layer retains original SemBench queries, parsing and complete sinks.
"""

from collections import Counter
from collections.abc import Mapping
from copy import deepcopy
from functools import partial
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


def execute_prepared(requests, *, model, sampling_params, engine_kwargs,
                     processor_options, runtime_options, engine_runtime_env, on_result_ready):
    """Materialize native predictions and invoke the complete sink before close.

    The callback runs the original parser/query and then its audits. It is not a
    row UDF or a replacement scheduler. No response is keyed by prompt text.
    Runtime ownership is exclusive and local; physical process exit still needs
    the existing outer host supervisor.
    """
    options = _options(processor_options)
    source = [dict(row) for row in requests]
    expected = [row['call_id'] for row in source]
    if len(set(expected)) != len(expected):
        raise ValueError('native Ray requests require unique occurrence call IDs')
    if runtime_options.get('address', 'local') != 'local':
        raise ValueError('this finite native binding requires a local owned Ray runtime')
    report = dict(predictions=[], events={}, ray_driver_disconnected=None,
        model_process_exit_confirmed=None, scheduler_owner='Ray Data LLM processor',
        first_engine_accept_ns=None, kernel_seconds=None,
        retry_policy=dict(max_restarts=0, max_task_retries=0),
        requested_config=dict(model_source=model, sampling_params=deepcopy(sampling_params),
            engine_kwargs=deepcopy(engine_kwargs), processor_options=deepcopy(options),
            runtime_options=deepcopy(runtime_options)))
    events = report['events']
    if not source:
        events['predictions_materialized_ns'] = time.monotonic_ns()
        on_result_ready(report)
        return report

    import ray
    import ray.data
    from ray.data.llm import build_processor, vLLMEngineProcessorConfig
    from ray._private.runtime_env.setup_hook import export_setup_func_module

    if ray.is_initialized():
        raise RuntimeError('native full-query binding requires an isolated Ray worker')
    primary = None
    try:
        events['runtime_start_ns'] = time.monotonic_ns()
        ray.init(**{'address': 'local', **runtime_options})
        report['ray_version'] = ray.__version__
        config = vLLMEngineProcessorConfig(model_source=model,
            engine_kwargs=deepcopy(engine_kwargs), **options)
        processor = build_processor(config,
            preprocess=partial(_prepared_input, sampling_params=deepcopy(sampling_params)),
            postprocess=_prepared_output)
        stages = [processor.get_stage_by_name(name) for name in processor.list_stage_names()]
        engines = [stage for stage in stages if type(stage).__name__ == 'vLLMEngineStage']
        if len(engines) != 1:
            raise RuntimeError('one native engine stage is required for the mandatory request budget')
        stage = engines[0]
        env = deepcopy(dict(engine_runtime_env))
        hook = 'src.baselines.text.frameworks.ray_native_observer.setup'
        # Same version-bound setup seam as the already qualified native Q7 route.
        # Export the module hook once; vLLM's nested workers must not re-export it.
        stage.map_batches_kwargs.update(max_restarts=0, max_task_retries=0,
            runtime_env=export_setup_func_module(env, hook))
        report['processor_config'] = config.model_dump(mode='json')
        report['stages'] = processor.list_stage_names()
        events['native_graph_submit_ns'] = time.monotonic_ns()
        predictions = processor(ray.data.from_items(source)).materialize()
        report['predictions'] = predictions.take_all()
        events['predictions_materialized_ns'] = time.monotonic_ns()
        actual = [row['call_id'] for row in report['predictions']]
        if Counter(actual) != Counter(expected):
            raise ValueError('native Ray predictions have missing, duplicate or unknown occurrences')
        on_result_ready(report)
    except BaseException as error:
        primary = error
    finally:
        events['close_started_ns'] = time.monotonic_ns()
        try:
            ray.shutdown()
            report['ray_driver_disconnected'] = not ray.is_initialized()
            if not report['ray_driver_disconnected']:
                raise RuntimeError('Ray remains initialized after shutdown')
        except BaseException as error:
            report['close_error_type'] = type(error).__name__
            if primary is None:
                primary = error
            else:
                primary.add_note('native Ray close failed: ' + type(error).__name__)
        events['close_finished_ns'] = time.monotonic_ns()
    if primary is not None:
        primary.native_query_report = report
        raise primary
    return report
