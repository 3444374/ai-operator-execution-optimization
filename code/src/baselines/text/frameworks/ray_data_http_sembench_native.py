"""Legacy LOTUS-compatible Q3 through Ray HTTP and a caller-owned local service.

This is a separate topology from the local vLLMEngineProcessor. Ray owns its
native HTTP stage, batching and retries; this binding never starts or stops the
external model service and does not repair the local actor shutdown path.
"""

from collections import Counter
from collections.abc import Mapping, Sequence
from copy import deepcopy
from functools import partial
import ipaddress
import json
from time import monotonic
from typing import Any
from urllib.parse import urlsplit


def _configuration(url, sampling_params):
    address = urlsplit(url)
    if (address.scheme != 'http' or not address.hostname or address.username is not None
            or address.password is not None or address.query or address.fragment):
        raise ValueError('native Ray HTTP Q3 requires a local HTTP URL without credentials, query or fragment')
    try:
        local = address.hostname.lower() == 'localhost' or ipaddress.ip_address(address.hostname).is_loopback
        address.port  # Validate an explicitly supplied port without resolving DNS.
    except ValueError:
        local = False
    if not local:
        raise ValueError('native Ray HTTP Q3 requires a loopback service URL')
    sampling = deepcopy(dict(sampling_params))
    if sampling.keys() & {'model', 'messages', 'stream'}:
        raise ValueError('sampling parameters cannot override model, messages or stream')
    if 'n' in sampling and (type(sampling['n']) is not int or sampling['n'] != 1):
        raise ValueError('Movie Q3 requires exactly one completion per occurrence (n=1)')
    json.dumps(sampling, allow_nan=False)
    return sampling


def _eligible(row):
    return row['id'] == 'taken_3'


def _preprocess(row, *, model, sampling_params):
    from src.baselines.text.sembench_movie_native import q3_messages

    return dict(payload=dict(deepcopy(sampling_params), model=model,
        messages=q3_messages(row['reviewText']), stream=False, n=1))


def _response_error(message, response):
    error = ValueError(message)
    error.http_response = deepcopy(response)
    raise error


def _postprocess(row):
    from src.baselines.text.sembench_movie_native import q3_decision

    response = row['http_response']
    if not isinstance(response, Mapping) or 'error' in response:
        _response_error('native Ray HTTP endpoint returned an invalid or error JSON response', response)
    try:
        json.dumps(response, allow_nan=False)
    except (TypeError, ValueError):
        _response_error('native Ray HTTP endpoint returned non-JSON or non-finite response values', response)
    choices = response.get('choices')
    if (type(choices) is not list or len(choices) != 1 or not isinstance(choices[0], Mapping)
            or not isinstance(choices[0].get('message'), Mapping)
            or type(choices[0]['message'].get('content')) is not str):
        _response_error('native Ray HTTP Q3 requires exactly one textual chat completion', response)
    text = choices[0]['message']['content']
    return dict(occurrence_id=row['occurrence_id'], reviewId=row['reviewId'], id=row['id'],
        generated_text=text, decision=q3_decision(text), native_usage={
            name: response[name] for name in ('id', 'model', 'created', 'usage', 'system_fingerprint')
            if name in response})


def _positive(row):
    return row['decision']


def execute_q3(
    rows: Sequence[Mapping[str, Any]], *, url: str, model: str,
    sampling_params: Mapping[str, Any],
) -> dict[str, Any]:
    """Run one full-cohort native HTTP graph and audit its materialized outputs.

    The caller qualifies and owns the local service, its model/tokenizer/template,
    health, request-attempt accounting and physical shutdown. Service preparation
    and teardown are outside this function's query timing. Unspecified HTTP and
    Ray Data settings retain native defaults, including their retry policies.
    """
    sampling = _configuration(url, sampling_params)
    source = [dict(row) for row in rows]
    eligible = [row for row in source if _eligible(row)]
    expected = Counter((row['occurrence_id'], row['reviewId'], row['id']) for row in eligible)
    if len({row['occurrence_id'] for row in eligible}) != len(eligible):
        raise ValueError('native Q3 requires unique eligible source occurrence IDs')
    metadata = dict(
        backend='ray-data-http-native', workload_binding='project-sembench-movie-q3',
        baseline_identity='lotus-compatible-ray-http-q3',
        semantic_profile='project LOTUS 1.2.4 non-CoT compatible prompt/parser; not independent native semantics',
        author_original_semantic_method=False,
        profile='ray-data-http-external-vllm',
        scheduler_owner='Ray Data native HTTP processor; external model service owns inference',
        requested_config=dict(url=url, model=model, sampling_params=deepcopy(sampling)),
        request_contract=dict(n=1, stream=False), processor_config=None, stages=[], ray_version=None,
        input_rows=len(source), logical_inference_rows=len(eligible),
        native_capacity=dict(status='unobserved', source='external service; HTTP batch size is not model capacity'),
        retry_policy=dict(adapter_overrides={}, native_source_version='2.56.1',
            actor_pool_source_defaults=dict(max_restarts=-1, max_task_retries=-1), physical_attempts=None),
        lifecycle=dict(profile='query', runtime_owner='none', ray_driver_disconnected=None,
            external_service_owner='caller', external_service_lifecycle_managed=False,
            model_process_exit_confirmed=None),
        timing=dict(scope='Ray query and owned Ray shutdown; excludes external service preparation and teardown'),
    )
    result = dict(predictions=[], positive_review_cnt=0, metadata=metadata)
    if not source:
        return result
    result['positive_review_cnt'] = None

    import ray
    import ray.data

    owns_runtime = not ray.is_initialized()
    metadata['ray_version'] = ray.__version__
    metadata['lifecycle']['runtime_owner'] = 'binding' if owns_runtime else 'caller'
    primary = None
    started = monotonic()
    try:
        context = ray.data.DataContext.get_current()
        metadata['retry_policy'].update({name: getattr(context, name) for name in (
            'actor_init_retry_on_errors', 'actor_init_max_retries', 'actor_task_retry_on_errors',
            'retried_map_errors', 'max_map_retries')})
        metadata['data_context'] = dict(max_tasks_in_flight_per_actor=context.max_tasks_in_flight_per_actor)
        selected = ray.data.from_items(source).filter(_eligible)
        if not expected:
            result['positive_review_cnt'] = selected.count()
        else:
            from ray.data.llm import HttpRequestProcessorConfig, build_processor

            config = HttpRequestProcessorConfig(url=url)
            processor = build_processor(config,
                preprocess=partial(_preprocess, model=model, sampling_params=sampling),
                postprocess=_postprocess)
            metadata['processor_config'] = config.model_dump(mode='json')
            metadata['stages'] = processor.list_stage_names()
            predictions = processor(selected).materialize()
            output = predictions.take_all()
            result['predictions'] = output
            if Counter((row['occurrence_id'], row['reviewId'], row['id']) for row in output) != expected:
                raise ValueError('native Ray HTTP predictions have missing, duplicate or unknown occurrences')
            result['positive_review_cnt'] = predictions.filter(_positive).count()
    except BaseException as error:
        primary = error
        error.native_q3_report = result
    finally:
        metadata['timing']['query_seconds'] = monotonic() - started
        if owns_runtime:
            closing = monotonic()
            try:
                ray.shutdown()
                # This is the SDK connection flag. Process/SHM exit needs the
                # caller's independent supervision after the worker exits.
                closed = not ray.is_initialized()
                metadata['lifecycle']['ray_driver_disconnected'] = closed
                if not closed:
                    raise RuntimeError('native Ray driver remains connected after shutdown')
            except BaseException as error:
                metadata['lifecycle']['cleanup_error_type'] = type(error).__name__
                if primary is None:
                    primary = error
                    error.native_q3_report = result
                else:
                    primary.add_note('native Ray runtime cleanup failed: ' + type(error).__name__)
            finally:
                metadata['timing']['ray_shutdown_seconds'] = monotonic() - closing
    if primary is not None:
        raise primary
    return result
