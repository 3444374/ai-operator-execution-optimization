"""Prepared original-query requests using Daft's native AI expression."""

import time


def _provider_options(value):
    options = dict(value or {})
    allowed = {'do_prefix_routing', 'max_buffer_size', 'min_bucket_size',
               'prefix_match_threshold', 'batch_size'}
    if options.keys() - allowed:
        raise ValueError('unsupported native Daft AI options: ' + ', '.join(sorted(options.keys() - allowed)))
    return options


def execute_prepared_map(requests, *, model, sampling_params, engine_kwargs=None,
                         provider_options=None, close_timeout_s=120, on_result_ready=None,
                         on_native_event=None, before_generate=None):
    """Native AI execution only; the driver owns Map meaning and evaluation."""
    import daft
    from daft.functions.ai import prompt
    from .daft_native_owner import NativeAsyncModelOwner
    from .daft_native_trace import NativeMapTrace

    if daft.get_or_infer_runner_type() != 'native':
        raise ValueError('this binding requires the Daft native runner in its caller process')
    options = _provider_options(provider_options)
    trace = NativeMapTrace(on_native_event, before_generate=before_generate)
    owner = NativeAsyncModelOwner(close_timeout_s, on_model_created=trace.attach)
    metadata = dict(backend='daft-native-ai', daft_version=daft.__version__,
        implementation='prepared prompts; driver owns SemBench semantics',
        logical_inference_rows=len(requests), model=model,
        sampling_params=dict(sampling_params), engine_kwargs=dict(engine_kwargs or {}),
        provider_options=options, model_close_receipt=None,
        usage='native final events are API evidence, not GPU kernel measurements')
    result = dict(predictions=[], metadata=metadata)
    try:
        with trace, owner:
            frame = daft.from_pylist(requests).with_column('generated_text', prompt(
                daft.col('prompt'), provider='vllm-prefix-caching', model=model,
                engine_args=dict(engine_kwargs or {}),
                generate_args={'sampling_params': native_sampling_params(sampling_params)}, **options))
            returned = frame.select('call_id', 'generated_text').collect().to_pylist()
            by_id = {row['call_id']: row for row in returned}
            if len(by_id) != len(returned) or by_id.keys() != {row['call_id'] for row in requests}:
                raise ValueError('native Map returned duplicate, missing or unrelated occurrences')
            result['predictions'] = [by_id[row['call_id']] for row in requests]
            metadata['native_api'] = trace.report()
            metadata['result_ready_monotonic_ns'] = time.monotonic_ns()
            if on_result_ready is not None:
                on_result_ready(result)
    except BaseException as error:
        metadata.update(native_api=trace.report(), model_close_receipt=owner.report)
        error.native_query_report = result
        error.model_close_report = owner.report
        raise
    metadata.update(native_api=trace.report(), model_close_receipt=owner.report)
    return result

def native_sampling_params(sampling_params):
    """Preserve public generation options and materialize JSON format at the API."""
    from vllm import SamplingParams

    options = dict(sampling_params)
    response_format = options.pop('response_format', None)
    if response_format is not None:
        if response_format != {'type': 'json_object'} or 'structured_outputs' in options:
            raise ValueError('one unambiguous JSON object response format is required')
        options['structured_outputs'] = {'json_object': True}
    if 'structured_outputs' in options:
        from vllm.sampling_params import StructuredOutputsParams
        options['structured_outputs'] = StructuredOutputsParams(**options['structured_outputs'])
    return SamplingParams(**options)
