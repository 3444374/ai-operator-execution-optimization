"""One author-owned Q3 batch to a caller-owned local OpenAI-compatible service.

No service/model lifecycle or row batching is introduced here. A missing
terminal receipt leaves the started batch unknown, including its POST attempts.
HTTP observations never establish a physical model-execution count.
"""

import asyncio
import base64
from copy import deepcopy
from functools import cache
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import time
from urllib.parse import urlsplit
import uuid

import httpx

from src.infrastructure.private_artifacts import content_digest, publish_private_json, require_outside_git

CONFIG_ENV = 'SEMLOOM_IMLANE_Q3_MODEL_CONFIG'
CONFIG_SCHEMA = 'semloom.imlane.duckdb.q3.model_provider.v1'
RECEIPT_SCHEMA = 'semloom.imlane.duckdb.q3.model_batch.v1'


def _reject_constant(value):
    raise ValueError('non-finite JSON value: ' + value)


def configuration(value):
    """Validate before service allocation; do not read secrets or create paths."""
    required = {'schema', 'url', 'model', 'sampling_params', 'request_timeout_s', 'receipt_dir', 'provider_mode'}
    if (type(value) is not dict or required - value.keys()
            or value.keys() - (required | {'api_key_env'}) or value['schema'] != CONFIG_SCHEMA):
        raise ValueError('an explicit native IMLane Q3 model-provider configuration is required')
    if type(value['url']) is not str:
        raise ValueError('model provider URL must be a string')
    address = urlsplit(value['url'])
    if (address.scheme != 'http' or not address.hostname or address.username is not None
            or address.password is not None or address.query or address.fragment
            or address.path != '/v1/chat/completions'):
        raise ValueError('model provider requires a credential-free loopback HTTP chat-completions URL')
    try:
        local = address.hostname.lower() == 'localhost' or ipaddress.ip_address(address.hostname).is_loopback
        address.port
    except ValueError:
        local = False
    if not local:
        raise ValueError('model provider requires a loopback HTTP service')
    if type(value['model']) is not str or not value['model']:
        raise ValueError('model provider requires an explicit served model identity')
    if value['provider_mode'] not in ('model', 'software_http'):
        raise ValueError('provider_mode must distinguish model from software_http')
    sampling = value['sampling_params']
    if (type(sampling) is not dict or not {'max_tokens', 'temperature', 'top_p'} <= sampling.keys()
            or sampling.keys() & {'model', 'messages', 'stream'}):
        raise ValueError('explicit max_tokens, temperature and top_p are required without reserved overrides')
    if (type(sampling['max_tokens']) is not int or sampling['max_tokens'] < 1
            or type(sampling['temperature']) not in (int, float) or sampling['temperature'] < 0
            or type(sampling['top_p']) not in (int, float) or not 0 < sampling['top_p'] <= 1
            or 'n' in sampling and (type(sampling['n']) is not int or sampling['n'] != 1)):
        raise ValueError('Q3 sampling requires positive max_tokens, temperature>=0, 0<top_p<=1 and n=1')
    json.dumps(sampling, allow_nan=False)
    timeout = value['request_timeout_s']
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('request_timeout_s must be finite positive seconds')
    key_env = value.get('api_key_env')
    if key_env is not None and (type(key_env) is not str or not key_env):
        raise ValueError('api_key_env must name an environment variable or be null')
    directory = Path(value['receipt_dir']).resolve()
    require_outside_git(directory)
    return dict(value, sampling_params=deepcopy(sampling), receipt_dir=str(directory), api_key_env=key_env)


@cache
def _configuration(path):
    path = Path(path)
    require_outside_git(path)
    config = configuration(json.loads(path.read_text(encoding='utf-8'), parse_constant=_reject_constant))
    if not Path(config['receipt_dir']).is_dir():
        raise ValueError('model provider requires an existing private receipt directory')
    return config


def _response(response, call, model):
    body = response.content
    call.update(http_status=response.status_code,
        response_body_base64=base64.b64encode(body).decode('ascii'),
        response_body_sha256=hashlib.sha256(body).hexdigest())
    parse_error = None
    try:
        data = json.loads(body, parse_constant=_reject_constant)
        json.dumps(data, allow_nan=False)
        call['response'] = data
    except (ValueError, UnicodeError) as error:
        parse_error = error
        call['response_parse_error_type'] = type(error).__name__
    response.raise_for_status()
    if parse_error is not None:
        raise parse_error
    if type(data) is not dict or 'error' in data:
        raise ValueError('model endpoint returned an error or non-object JSON response')
    if data.get('model') != model:
        raise ValueError('model endpoint returned a different served model identity')
    choices = data.get('choices')
    if (type(choices) is not list or len(choices) != 1 or type(choices[0]) is not dict
            or type(choices[0].get('message')) is not dict
            or type(choices[0]['message'].get('content')) is not str
            or type(choices[0].get('finish_reason')) is not str):
        raise ValueError('Q3 requires exactly one completed textual chat response')
    choice = choices[0]
    call.update(response_id=data.get('id'), usage=data.get('usage'), finish_reason=choice['finish_reason'],
                truncated=choice['finish_reason'] == 'length')
    if choice['finish_reason'] not in ('stop', 'length'):
        raise ValueError('Q3 endpoint did not finish a normal text generation')
    # The original Q3 parser receives length-limited output too. Preserve it;
    # the outer quality audit decides whether truncation qualifies a comparison.
    return choice['message']['content']


async def _request(client, config, payload, call):
    call.update(attempted=True, http_attempt_started_monotonic_ns=time.monotonic_ns())
    try:
        async with asyncio.timeout(config['request_timeout_s']):
            response = await client.post(config['url'], json=payload)
            text = _response(response, call, config['model'])
        call['status'] = 'succeeded'
        return text
    except BaseException as error:
        call.update(status='unknown' if isinstance(error, (httpx.RequestError, TimeoutError, asyncio.CancelledError))
                    else 'failed', error_type=type(error).__name__)
        raise
    finally:
        call['http_attempt_finished_monotonic_ns'] = time.monotonic_ns()


async def _run_batch(config, payloads, receipt):
    headers = {}
    if config['api_key_env'] is not None:
        key = os.environ.get(config['api_key_env'])
        if not key:
            raise ValueError('configured API credential is absent from the environment')
        headers['Authorization'] = 'Bearer ' + key
    limits = httpx.Limits(max_connections=None, max_keepalive_connections=None)
    client = None
    primary = None
    outcomes = None
    try:
        client = httpx.AsyncClient(headers=headers, limits=limits,
            timeout=httpx.Timeout(config['request_timeout_s']), follow_redirects=False, trust_env=False)
        receipt['client_created'] = True
        async with client:
            try:
                outcomes = await asyncio.gather(*(
                    _request(client, config, payload, call)
                    for payload, call in zip(payloads, receipt['calls'], strict=True)
                ), return_exceptions=True)
                primary = next((value for value in outcomes if isinstance(value, BaseException)), None)
            except BaseException as error:
                primary = error
        receipt['client_closed'] = client.is_closed
    except BaseException as error:
        if primary is None:
            primary = error
        else:
            primary.add_note('HTTP batch client cleanup also failed: ' + type(error).__name__)
        if client is not None and not client.is_closed:
            try:
                await client.aclose()
                receipt['client_closed'] = client.is_closed
            except BaseException as cleanup:
                primary.add_note('HTTP client failed-entry cleanup also failed: ' + type(cleanup).__name__)
    finally:
        if client is not None:
            receipt['client_is_closed'] = client.is_closed
    if primary is not None:
        raise primary
    return outcomes


def infer_batch(messages_batch):
    """Return all original texts in input order, or raise after draining the batch."""
    if type(messages_batch) is not list:
        raise TypeError('Q3 model provider requires a list of message lists')
    if not messages_batch:
        return []
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError('synchronous infer_batch cannot run inside an active event loop')
    path = os.environ.get(CONFIG_ENV)
    if not path:
        raise RuntimeError('Q3 model provider configuration is absent')
    config = _configuration(str(Path(path).resolve()))
    payloads = [dict(deepcopy(config['sampling_params']), model=config['model'],
        messages=deepcopy(messages), stream=False, n=1) for messages in messages_batch]
    for payload in payloads:
        if type(payload['messages']) is not list or not payload['messages']:
            raise ValueError('each Q3 request requires its original nonempty message list')
        json.dumps(payload, allow_nan=False)
    batch_id = uuid.uuid4().hex
    receipt = dict(schema=RECEIPT_SCHEMA, batch_id=batch_id, phase='started',
        provider_mode=config['provider_mode'], worker_pid=os.getpid(), status='unknown',
        started_monotonic_ns=time.monotonic_ns(), client_created=False, client_closed=None, client_is_closed=None,
        physical_model_attempts=None, http_post_attempts=None,
        requested_config={name: deepcopy(config[name]) for name in
            ('url', 'model', 'sampling_params', 'request_timeout_s', 'api_key_env')},
        transport=dict(client='httpx.AsyncClient', max_connections=None, max_keepalive_connections=None,
            pool_policy='project choice: one author batch; no additional concurrency cap',
            retries=0, follow_redirects=False, trust_env=False,
            timeout_scope='whole POST/body read wall time; httpx per-operation timeout uses the same value',
            attempt_semantics='client.post invocation, not verified wire or physical model execution',
            missing_terminal='all attempt/results remain unknown; started is a pre-dispatch snapshot'),
        calls=[dict(position=index, messages_sha256=content_digest(payload['messages']),
            request_payload_sha256=content_digest(payload), requested_payload=payload,
            attempted=False, status='unknown', http_status=None, response=None,
            error_type=None, http_attempt_started_monotonic_ns=None, http_attempt_finished_monotonic_ns=None)
            for index, payload in enumerate(payloads)])
    destination = Path(config['receipt_dir']) / ('model-batch-' + batch_id)
    publish_private_json(destination.with_suffix('.started.json'), receipt)
    primary = None
    texts = None
    try:
        texts = asyncio.run(_run_batch(config, payloads, receipt))
        receipt['status'] = 'succeeded'
    except BaseException as error:
        primary = error
        receipt.update(status='unknown' if any(call['attempted'] and call['status'] == 'unknown'
            for call in receipt['calls']) else 'failed', error_type=type(error).__name__)
    receipt.update(phase='terminal', finished_monotonic_ns=time.monotonic_ns(),
                   http_post_attempts=sum(call['attempted'] for call in receipt['calls']))
    for call in receipt['calls']:
        if not call['attempted']:
            call['status'] = 'not_started'
    try:
        publish_private_json(destination.with_suffix('.terminal.json'), receipt)
    except Exception as audit_error:
        if primary is None:
            audit_error.imlane_model_provider_report = receipt
            raise
        primary.add_note('model-provider terminal receipt also failed: ' + type(audit_error).__name__)
    if primary is not None:
        primary.imlane_model_provider_report = receipt
        raise primary
    return texts
