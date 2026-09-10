"""Verify occurrence identity from input/POST evidence to the final Map rows."""
import json

from src.baselines.common.private_artifacts import content_digest
from src.execution_provider.adapters.completion_response import decode_backend_completion
from src.execution_provider.semantic_map import completion_status, MapCompletionStatus
from src.execution_provider.completion import Completion
from .map_direct import request_body


def unique(events, event_name, identity):
    result = {}
    for event in events:
        if event['event'] != event_name:
            continue
        key = identity(event)
        if key in result:
            raise ValueError('duplicate ' + event_name + ' identity')
        result[key] = event
    return result


def task_key(event):
    key = event.get('key')
    if not isinstance(key, dict) or set(key) != {'session_id', 'sequence'}:
        raise ValueError('missing independent task identity')
    if (type(key['session_id']) is not int or key['session_id']<1
            or type(key['sequence']) is not int or key['sequence']<0):
        raise ValueError('invalid independent task identity')
    return key['session_id'], key['sequence']


def verify_native_map_results(arm, texts, predictions, events, *, plan, source_positions=None):
    requests = unique(events, 'request', lambda e:e['attempt'])
    bound = {}
    if arm == 'pg-source-direct':
        inputs = unique(events, 'direct_input', task_key)
        posts = unique(events, 'request', task_key)
        completions = unique(events, 'direct_completion', task_key)
        if inputs.keys() != posts.keys() or inputs.keys() != completions.keys():
            raise ValueError('direct input, POST and completion identities differ')
        for key, event in inputs.items():
            row_id = event['row_id']
            if row_id not in texts or row_id in bound:
                raise ValueError('direct input occurrence differs from source')
            expected = content_digest(request_body(plan,texts[row_id]))
            if (event['request_values_sha256'] != expected or content_digest(posts[key]['body']) != expected):
                raise ValueError('direct actual request differs from its input row')
            value = completions[key]
            completion = Completion(**{name:value[name] for name in (
                'raw_output','response_model_id','prompt_tokens','output_tokens','finish_reason')})
            if completion_status(plan,completion) != MapCompletionStatus.VALID:
                raise ValueError('direct completion violates Map semantics')
            bound[row_id] = completion.raw_output
    elif arm == 'ray-data':
        responses = unique(events, 'http_response', lambda e:e['attempt'])
        if responses.keys() != requests.keys():
            raise ValueError('native Ray POST and response identities differ')
        positions = set()
        for attempt, request in requests.items():
            identity = request.get('identity')
            if not isinstance(identity, dict):
                raise ValueError('native Ray request lacks input occurrence identity')
            row_id, position = identity['row_id'], identity['source_position']
            if row_id not in texts or row_id in bound or position in positions:
                raise ValueError('native Ray input occurrence differs from source')
            if source_positions is not None and source_positions.get(row_id) != position:
                raise ValueError('native Ray source position differs from the raw source')
            positions.add(position)
            if content_digest(request['body']) != content_digest(request_body(plan,texts[row_id])):
                raise ValueError('native Ray actual request differs from its input row')
            completion = decode_backend_completion(json.dumps(responses[attempt]['response']).encode())
            if completion_status(plan,completion) != MapCompletionStatus.VALID:
                raise ValueError('native Ray response violates Map semantics')
            bound[row_id] = completion.raw_output
    else:
        raise ValueError('unsupported native Map association arm')
    if bound != predictions or set(bound) != set(texts):
        raise ValueError('model completions differ from final per-row outputs')
    return dict(rows=len(bound),attempts=len(requests),independent_occurrence_binding=True)
