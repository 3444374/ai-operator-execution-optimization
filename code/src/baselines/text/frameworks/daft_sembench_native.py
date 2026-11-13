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


def _message_columns(requests):
    """Bind the static-system content-parts shape supported by native prompt."""
    import base64
    import binascii
    from daft.ai.openai.protocols.prompter import OpenAIPrompter

    rows, seen, system, shape = [], set(), None, None
    for request in requests:
        call_id, messages = request['call_id'], request['messages']
        if not isinstance(call_id, str) or not call_id or call_id in seen:
            raise ValueError('native message batches require unique nonempty call_id values')
        seen.add(call_id)
        if (len(messages) != 2 or any(set(message) != {'role', 'content'} for message in messages)
                or messages[0]['role'] != 'system' or not isinstance(messages[0]['content'], str)
                or messages[1]['role'] != 'user' or not isinstance(messages[1]['content'], list)
                or not messages[1]['content']):
            raise ValueError('native message batches support system text and one user content-parts array')
        if system is None:
            system = messages[0]['content']
        elif system != messages[0]['content']:
            raise ValueError('native prompt requires one static system message per batch')
        parts, kinds = [], []
        for part in messages[1]['content']:
            kind = part.get('type')
            if kind == 'text' and set(part) == {'type', 'text'} and isinstance(part['text'], str):
                text = part['text']
                url = text.lower().split('?')[0].split('#')[0]
                if (url.startswith(('http://', 'https://'))
                        and url.endswith(tuple(OpenAIPrompter.IMAGE_EXTENSIONS | OpenAIPrompter.VIDEO_EXTENSIONS))):
                    raise ValueError('native prompt would infer an image/video URL from an original text part')
                value = text
            elif (kind == 'image_url' and set(part) == {'type', 'image_url'}
                    and set(part['image_url']) == {'url'} and isinstance(part['image_url']['url'], str)):
                prefix, separator, encoded = part['image_url']['url'].partition(',')
                if not separator or prefix != 'data:image/png;base64':
                    raise ValueError('native image parts require PNG data URIs')
                try:
                    value = base64.b64decode(encoded, validate=True)
                except (binascii.Error, ValueError) as error:
                    raise ValueError('native PNG messages require canonical base64') from error
                if (not value.startswith(b'\x89PNG\r\n\x1a\n')
                        or base64.b64encode(value).decode('ascii') != encoded):
                    raise ValueError('native PNG messages require canonical PNG data URIs')
            else:
                raise ValueError('native prompt cannot preserve this original content part')
            parts.append(value)
            kinds.append(kind)
        if shape is None:
            shape = kinds
        elif shape != kinds:
            raise ValueError('native prompt requires the same content-part layout per batch')
        rows.append(dict(call_id=call_id, **{'part_' + str(i): value for i, value in enumerate(parts)}))
    return system, rows


def execute_prepared_messages(requests, *, model, sampling_params, provider_options, on_result_ready=None):
    """Run original message batches with Daft's builtin OpenAI/vLLM API graph.

    The caller owns the isolated worker and releases its SDK client resources by
    process exit after the complete query sink ACK. No model is owned or closed
    here, and native SDK client close is not claimed.
    """
    from copy import deepcopy
    import daft
    from daft.ai.openai.provider import OpenAIProvider
    from daft.functions.ai import prompt

    if daft.get_or_infer_runner_type() != 'native':
        raise ValueError('this binding requires the Daft native runner in its caller process')
    metadata = dict(backend='daft-native-ai', daft_version=daft.__version__,
        provider='builtin OpenAI chat completions over a vLLM-compatible service',
        native_api_name='daft.functions.ai.prompt', model=model, logical_inference_rows=len(requests),
        sampling_params=deepcopy(sampling_params), model_process_owned=False, model_close_receipt=None,
        client_resource_release='caller-owned isolated worker process exit; SDK explicit client close unverified')
    result = dict(predictions=[], metadata=metadata)
    try:
        system, rows = _message_columns(requests)
        effective = deepcopy(sampling_params)
        if 'repetition_penalty' in effective:
            penalty = effective.pop('repetition_penalty')
            extra = effective.get('extra_body')
            if extra is not None and (not isinstance(extra, dict) or 'repetition_penalty' in extra):
                raise ValueError('repetition_penalty must have one unambiguous native API value')
            effective['extra_body'] = dict(extra or {}, repetition_penalty=penalty)
        metadata['native_sampling_params'] = deepcopy(effective)
        if rows:
            frame = daft.from_pylist(rows).with_column('generated_text', prompt(
                [daft.col(name) for name in rows[0] if name != 'call_id'],
                provider=OpenAIProvider(**dict(provider_options)), model=model,
                system_message=system, use_chat_completions=True, return_format=None, **effective))
            returned = frame.select('call_id', 'generated_text').collect().to_pylist()
            by_id = {row['call_id']: row for row in returned}
            if (len(by_id) != len(returned) or by_id.keys() != {row['call_id'] for row in requests}
                    or any(not isinstance(row['generated_text'], str) for row in returned)):
                raise ValueError('native messages returned duplicate, missing or invalid occurrences')
            result['predictions'] = [by_id[row['call_id']] for row in requests]
        metadata['result_ready_monotonic_ns'] = time.monotonic_ns()
        if on_result_ready is not None:
            on_result_ready(result)
    except BaseException as error:
        error.native_query_report = result
        raise
    return result
