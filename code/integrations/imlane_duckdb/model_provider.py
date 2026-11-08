"""One author-owned batch over local IPC to one independently owned AsyncLLM.

The database retains batching and CPython execution. This client neither loads a
model nor introduces row concurrency, admission, retries or output parsing.
"""

from copy import deepcopy
from functools import cache
import json
import math
import os
from pathlib import Path
import socket
import time
import uuid

from integrations.imlane_duckdb.model_wire import SCHEMA, encode_frame, receive_frame
from src.infrastructure.private_artifacts import content_digest, publish_private_json, require_outside_git

CONFIG_ENV = 'SEMLOOM_IMLANE_Q3_MODEL_CONFIG'
CONFIG_SCHEMA = 'semloom.imlane.duckdb.q3.model_provider.v2'
RECEIPT_SCHEMA = 'semloom.imlane.duckdb.q3.model_batch.v2'


class ModelBatchError(RuntimeError):
    """A completed IPC reply reported a model error; no replacement value."""


def _reject_constant(value):
    raise ValueError('non-finite JSON value: ' + value)


def configuration(value):
    """Validate the external configuration before any model allocation."""
    required = {'schema', 'socket_path', 'model', 'sampling_params',
                'request_timeout_s', 'receipt_dir', 'provider_mode'}
    if (type(value) is not dict or required - value.keys()
            or value.keys() - (required | {'deadline_monotonic'})
            or value['schema'] != CONFIG_SCHEMA):
        raise ValueError('an explicit native IMLane IPC provider configuration is required')
    path = value['socket_path']
    if type(path) is not str or not path or not Path(path).is_absolute() or '\0' in path:
        raise ValueError('model provider requires an absolute local Unix socket path')
    if type(value['model']) is not str or not value['model']:
        raise ValueError('model provider requires an explicit served model identity')
    if value['provider_mode'] not in ('model', 'software_ipc'):
        raise ValueError('provider_mode must distinguish model from software_ipc')
    sampling = value['sampling_params']
    if (type(sampling) is not dict or not {'max_tokens', 'temperature', 'top_p'} <= sampling.keys()
            or sampling.keys() & {'model', 'messages', 'stream'}):
        raise ValueError('explicit max_tokens, temperature and top_p required without reserved overrides')
    if (type(sampling['max_tokens']) is not int or sampling['max_tokens'] < 1
            or type(sampling['temperature']) not in (int, float) or sampling['temperature'] < 0
            or type(sampling['top_p']) not in (int, float) or not 0 < sampling['top_p'] <= 1
            or 'n' in sampling and (type(sampling['n']) is not int or sampling['n'] != 1)):
        raise ValueError('Q3 sampling requires positive max_tokens, temperature>=0, 0<top_p<=1 and n=1')
    json.dumps(sampling, allow_nan=False)
    timeout = value['request_timeout_s']
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('request_timeout_s must be finite positive seconds')
    deadline = value.get('deadline_monotonic')
    if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline) or deadline <= 0):
        raise ValueError('deadline_monotonic must be finite positive seconds or null')
    directory = Path(value['receipt_dir']).resolve()
    require_outside_git(directory)
    return dict(value, sampling_params=deepcopy(sampling), receipt_dir=str(directory),
                deadline_monotonic=deadline)


@cache
def _configuration(path):
    path = Path(path)
    require_outside_git(path)
    config = configuration(json.loads(path.read_text(encoding='utf-8'), parse_constant=_reject_constant))
    if not Path(config['receipt_dir']).is_dir():
        raise ValueError('model provider requires an existing private receipt directory')
    return config


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('Q3 absolute IPC request deadline exhausted')
    return remaining


def _accept_response(response, request, receipt):
    if (type(response) is not dict or response.get('schema') != SCHEMA
            or response.get('batch_id') != request['batch_id']
            or response.get('model') != request['model']
            or type(response.get('calls')) is not list
            or len(response['calls']) != len(receipt['calls'])):
        raise ValueError('IPC response does not identify the same model batch')
    texts = []
    for index, (call, result) in enumerate(zip(receipt['calls'], response['calls'], strict=True)):
        if (type(result) is not dict or result.get('position') != index
                or type(result.get('request_id')) is not str or not result['request_id']
                or type(result.get('attempted')) is not bool
                or result.get('status') not in {'succeeded', 'failed', 'unknown', 'not_started'}):
            raise ValueError('IPC response has an invalid positional model result')
        call.update(attempted=result['attempted'], status=result['status'], response=deepcopy(result))
        if result['status'] == 'succeeded':
            if (result['attempted'] is not True or type(result.get('generated_text')) is not str
                    or result.get('finish_reason') not in {'stop', 'length'}
                    or type(result.get('usage')) is not dict
                    or result.get('requested_max_tokens') != request['sampling_params']['max_tokens']
                    or result.get('resolved_max_tokens') != request['sampling_params']['max_tokens']):
                raise ValueError('IPC model result is not a completed single text generation')
            texts.append(result['generated_text'])
            call.update(truncated=result['finish_reason'] == 'length',
                        response_id=result['request_id'], usage=result['usage'],
                        finish_reason=result['finish_reason'])
    succeeded = all(call['status'] == 'succeeded' for call in receipt['calls'])
    expected = 'succeeded' if succeeded else (
        'unknown' if any(call['status'] == 'unknown' for call in receipt['calls']) else 'failed')
    if response.get('status') != expected:
        raise ValueError('IPC batch status disagrees with its per-row results')
    receipt['engine_generate_invocations'] = sum(call['attempted'] for call in receipt['calls'])
    if not succeeded:
        failures = [result for result in response['calls'] if result['status'] != 'succeeded']
        raise ModelBatchError('native model batch failed: ' + json.dumps(failures, ensure_ascii=False))
    return texts


def _run_batch(config, request, receipt):
    client = None
    primary = None
    try:
        _remaining(request['deadline_monotonic'])
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        receipt['client_created'] = True
        client.settimeout(_remaining(request['deadline_monotonic']))
        client.connect(config['socket_path'])
        client.settimeout(_remaining(request['deadline_monotonic']))
        receipt['ipc_attempted'] = True
        client.sendall(encode_frame(request))
        response = receive_frame(client, request['deadline_monotonic'])
        receipt['response'] = response
        receipt['response_sha256'] = content_digest(response)
        return _accept_response(response, request, receipt)
    except BaseException as error:
        primary = error
        raise
    finally:
        if client is not None:
            try:
                client.close()
                receipt['client_closed'] = True
            except BaseException as error:
                if primary is None:
                    raise
                primary.add_note('IPC client close also failed: ' + type(error).__name__)


def infer_batch(messages_batch):
    """Return original model texts in the same order, or preserve the failure."""
    if type(messages_batch) is not list:
        raise TypeError('Q3 model provider requires a list of message lists')
    if not messages_batch:
        return []
    path = os.environ.get(CONFIG_ENV)
    if not path:
        raise RuntimeError('Q3 model provider configuration is absent')
    config = _configuration(str(Path(path).resolve()))
    messages = deepcopy(messages_batch)
    for row in messages:
        if type(row) is not list or not row:
            raise ValueError('each Q3 request requires its original nonempty message list')
    json.dumps(messages, allow_nan=False)
    now = time.monotonic()
    deadline = now + config['request_timeout_s']
    if config['deadline_monotonic'] is not None:
        deadline = min(deadline, config['deadline_monotonic'])
    batch_id = uuid.uuid4().hex
    sampling = dict(config['sampling_params'], n=1)
    request = dict(schema=SCHEMA, batch_id=batch_id, model=config['model'],
        sampling_params=sampling, messages_batch=messages, deadline_monotonic=deadline)
    receipt = dict(schema=RECEIPT_SCHEMA, batch_id=batch_id, phase='started',
        provider_mode=config['provider_mode'], worker_pid=os.getpid(), status='unknown',
        started_monotonic_ns=time.monotonic_ns(), client_created=False, client_closed=None,
        ipc_attempted=False, engine_generate_invocations=None, physical_model_attempts=None,
        request=request, request_sha256=content_digest(request), response=None, response_sha256=None,
        transport=dict(kind='AF_UNIX SOCK_STREAM', connections_per_author_batch=1,
            additional_row_concurrency_limit=None, retries=0,
            deadline_scope='one entire batch RPC, bounded by the original absolute query deadline',
            missing_terminal='IPC delivery and all model outcomes remain unknown'),
        calls=[dict(position=index, messages_sha256=content_digest(row),
            request_payload_sha256=content_digest(dict(sampling, model=config['model'], messages=row)),
            requested_payload=dict(sampling, model=config['model'], messages=row),
            attempted=None, status='unknown', response=None, truncated=None)
            for index, row in enumerate(messages)])
    destination = Path(config['receipt_dir']) / ('model-batch-' + batch_id)
    publish_private_json(destination.with_suffix('.started.json'), receipt)
    primary = None
    texts = None
    try:
        texts = _run_batch(config, request, receipt)
        receipt['status'] = 'succeeded'
    except BaseException as error:
        primary = error
        receipt.update(status=(receipt['response']['status'] if isinstance(error, ModelBatchError) else 'unknown'),
                       error_type=type(error).__name__)
        if not receipt['ipc_attempted']:
            receipt['status'] = 'failed'
            for call in receipt['calls']:
                call.update(attempted=False, status='not_started')
    receipt.update(phase='terminal', finished_monotonic_ns=time.monotonic_ns())
    try:
        publish_private_json(destination.with_suffix('.terminal.json'), receipt)
    except BaseException as error:
        if primary is None:
            raise
        primary.add_note('model-provider terminal receipt also failed: ' + type(error).__name__)
    if primary is not None:
        primary.imlane_model_provider_report = receipt
        raise primary
    return texts
