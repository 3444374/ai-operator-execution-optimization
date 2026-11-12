"""One native AsyncLLM shared by the author's independent Arrow executors.

The Unix transport adds no row queue, concurrency limit or retry policy. Each
connection owns one batch, and each row enters vLLM's native continuous batching.
The outer owner remains responsible for proving physical process/GPU exit.
"""

import argparse
import asyncio
from copy import deepcopy
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import signal
import stat
import sys
import time
import uuid

from integrations.imlane_duckdb.model_wire import (
    READY_SCHEMA, SCHEMA, SHUTDOWN_SCHEMA, decode_json, read_frame, write_frame,
)
from src.infrastructure.private_artifacts import content_digest, publish_private_json, require_outside_git
from src.serving.backends.vllm_protocol import sampling_params


_RESOLVED_FIELDS = {
    'model_config': ('dtype', 'max_model_len', 'generation_config', 'renderer_num_workers'),
    'scheduler_config': ('max_num_seqs', 'max_num_batched_tokens', 'max_model_len',
                         'policy', 'async_scheduling', 'enable_chunked_prefill'),
    'cache_config': ('gpu_memory_utilization', 'kv_cache_memory_bytes', 'block_size',
                     'enable_prefix_caching', 'num_gpu_blocks'),
    'parallel_config': ('tensor_parallel_size', 'pipeline_parallel_size', 'data_parallel_size'),
}


def _positive(value, name):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError(name + ' must be finite positive seconds')
    return value


def configuration(value):
    required = {'model', 'served_model_name', 'dtype', 'engine_options',
                'socket_path', 'ready_path', 'shutdown_path'}
    if (type(value) is not dict or required - value.keys()
            or value.keys() - (required | {'request_budget'})):
        raise ValueError('model IPC worker requires exactly: ' + ', '.join(sorted(required)))
    for name in ('model', 'served_model_name', 'dtype'):
        if type(value[name]) is not str or not value[name].strip():
            raise ValueError(name + ' must be nonempty text')
    options = value['engine_options']
    if (type(options) is not dict or any(type(key) is not str for key in options)
            or options.keys() & {'model', 'dtype', 'generation_config', 'override_generation_config',
                                'served_model_name'}):
        raise ValueError('engine_options cannot override owner model/dtype/generation identity')
    json.dumps(options, allow_nan=False)
    result = deepcopy(value)
    for name in ('socket_path', 'ready_path', 'shutdown_path'):
        path = Path(value[name]).absolute()
        require_outside_git(path)
        result[name] = str(path)
    if len({result[name] for name in ('socket_path', 'ready_path', 'shutdown_path')}) != 3:
        raise ValueError('socket, ready and shutdown paths must be distinct')
    return result


def _request(value, model):
    required = {'schema', 'batch_id', 'model', 'sampling_params', 'messages_batch', 'deadline_monotonic'}
    if type(value) is not dict or set(value) != required or value['schema'] != SCHEMA:
        raise ValueError('invalid model IPC request schema or fields')
    if type(value['batch_id']) is not str or not value['batch_id'] or value['model'] != model:
        raise ValueError('model IPC batch and served model identities must match')
    _positive(value['deadline_monotonic'], 'deadline_monotonic')
    params = value['sampling_params']
    if (type(params) is not dict or not {'max_tokens', 'temperature', 'top_p'} <= params.keys()
            or params.keys() & {'model', 'messages', 'stream', 'output_kind'}
            or type(params['max_tokens']) is not int or params['max_tokens'] < 1
            or type(params['temperature']) not in (int, float) or params['temperature'] < 0
            or type(params['top_p']) not in (int, float) or not 0 < params['top_p'] <= 1
            or type(params.get('n', 1)) is not int or params.get('n', 1) != 1):
        raise ValueError('Q3 requires explicit sampling with a single final text output')
    messages = value['messages_batch']
    if (type(messages) is not list or any(type(row) is not list or not row
            or any(type(message) is not dict for message in row) for row in messages)):
        raise ValueError('messages_batch must preserve the original nonempty message lists')
    json.dumps(value, allow_nan=False)
    return value


@dataclass
class NativeModel:
    engine: object
    sampling_type: object
    final_only: object
    chat_params: object
    version: str | None
    structured_outputs_type: object = None


def native_model(config):
    """Construct only the public vLLM API, inside its owning asyncio loop."""
    import vllm
    from vllm.engine.arg_utils import AsyncEngineArgs
    from vllm.renderers import ChatParams
    from vllm.sampling_params import RequestOutputKind, SamplingParams, StructuredOutputsParams
    from vllm.utils.mistral import is_mistral_tokenizer
    from vllm.v1.engine.async_llm import AsyncLLM

    args = AsyncEngineArgs(model=config['model'], served_model_name=config['served_model_name'], dtype=config['dtype'],
        generation_config='vllm', **config['engine_options'])
    engine = AsyncLLM.from_engine_args(args)
    try:
        # These are LLM.chat's public plain-chat defaults, rather than the empty
        # ChatParams dataclass defaults. No thinking/template override is added.
        chat = ChatParams(chat_template_content_format='auto', chat_template_kwargs=dict(
            add_generation_prompt=True, continue_final_message=False, tools=None,
            tokenize=(is_mistral_tokenizer(engine.renderer.tokenizer)
                      or engine.model_config.enable_prompt_embeds)))
        return NativeModel(engine, SamplingParams, RequestOutputKind.FINAL_ONLY, chat,
                           getattr(vllm, '__version__', None), StructuredOutputsParams)
    except BaseException as primary:
        try:
            engine.shutdown()
        except BaseException as cleanup:
            primary.add_note('native construction cleanup also failed: ' + repr(cleanup))
        raise


def _error(error):
    value = dict(error_type=type(error).__name__, error=str(error))
    if error.__cause__ is not None:
        value['error_cause'] = dict(error_type=type(error.__cause__).__name__, error=str(error.__cause__))
    if notes := getattr(error, '__notes__', None):
        value['error_notes'] = list(notes)
    return value


def _retain(primary, error, label):
    if primary is None:
        return error
    primary.add_note(label + ': ' + repr(error))
    return primary


def _ready(model, config, identity):
    resolved = {}
    for section, fields in _RESOLVED_FIELDS.items():
        native = getattr(model.engine.vllm_config, section)
        resolved[section] = {field: getattr(native, field, None) for field in fields}
    dtype = resolved['model_config']['dtype']
    resolved['model_config']['dtype'] = None if dtype is None else str(dtype)
    length = resolved['model_config']['max_model_len']
    if type(length) is not int or length < 1:
        raise ValueError('constructed native model did not report a positive max_model_len')
    return dict(schema=READY_SCHEMA, worker_pid=os.getpid(), served_model_name=config['served_model_name'],
        max_model_len=length, engine_instances=1, native_capacity=resolved, vllm_version=model.version,
        native_capacity_source='constructed AsyncLLM.vllm_config; not free capacity',
        generation_config='vllm', socket_identity=dict(device=identity[0], inode=identity[1]))


def request_budget(value):
    """Open the caller's existing once-claimed unit; never create or refund it."""
    from src.experiments.attempt_ledger import AttemptBudget
    from src.experiments.shared_request_budget import SharedClaimedUnit
    if (type(value) is not dict
            or set(value) != {'path', 'budget_id', 'limit', 'unit_id'}):
        raise ValueError('an explicit existing shared request-budget descriptor is required')
    unit = SharedClaimedUnit(Path(value['path']), AttemptBudget(value['budget_id'], value['limit']),
                             value['unit_id'])
    unit.remaining  # Validate the actual claim before model construction.
    return unit


class ModelWorker:
    def __init__(self, config, model, stop, budget=None):
        self.config, self.model, self.stop = config, model, stop
        self.budget = budget
        self.connections = {}
        self.cleanup_error = None

    def accept(self, reader, writer):
        if self.stop.is_set():
            writer.close()
            return
        task = asyncio.create_task(self.connection(reader, writer))
        self.connections[task] = writer
        task.add_done_callback(lambda task: self.connections.pop(task, None))

    async def _row(self, request, messages, call):
        generator = None
        primary = None
        try:
            if self.stop.is_set() or time.monotonic() >= request['deadline_monotonic']:
                raise TimeoutError('model IPC deadline exhausted before rendering')
            sampling = sampling_params(dict(deepcopy(request['sampling_params']), n=1,
                                            output_kind=self.model.final_only),
                                       self.model.sampling_type, self.model.structured_outputs_type)
            _, prompts = await self.model.engine.renderer.render_chat_async([messages], self.model.chat_params)
            if len(prompts) != 1 or prompts[0].get('type') != 'token':
                raise ValueError('native text chat renderer did not return one token prompt')
            prompt_ids = prompts[0]['prompt_token_ids']
            length = self.model.engine.vllm_config.model_config.max_model_len
            call.update(rendered_prompt_tokens=len(prompt_ids), max_model_len=length,
                        requested_max_tokens=request['sampling_params']['max_tokens'],
                        resolved_max_tokens=sampling.max_tokens)
            if len(prompt_ids) + sampling.max_tokens > length:
                raise ValueError('rendered prompt plus requested output exceed native max_model_len')
            if self.stop.is_set() or time.monotonic() >= request['deadline_monotonic']:
                raise TimeoutError('model IPC deadline exhausted before generate')
            if self.budget is not None:
                call['budget_attempt'] = self.budget.reserve(content_digest(dict(
                    messages=messages, prompt_token_ids=prompt_ids,
                    sampling_params=request['sampling_params'])))
                call['rendered_prompt_token_ids'] = list(prompt_ids)
            call.update(attempted=True, generate_started_monotonic_ns=time.monotonic_ns())
            generator = self.model.engine.generate(prompts[0], sampling, call['request_id'])
            final = None
            async for output in generator:
                if output.finished:
                    if final is not None:
                        raise ValueError('native generation produced more than one final output')
                    final = output
            if final is None or final.request_id != call['request_id'] or len(final.outputs) != 1:
                raise ValueError('native generation did not return its single identified final output')
            choice = final.outputs[0]
            call.update(generated_text=choice.text, finish_reason=choice.finish_reason)
            usage = dict(prompt_tokens=len(final.prompt_token_ids), completion_tokens=len(choice.token_ids),
                         total_tokens=len(final.prompt_token_ids) + len(choice.token_ids))
            call['usage'] = usage
            if choice.index != 0 or type(choice.text) is not str or choice.finish_reason not in ('stop', 'length'):
                raise ValueError('native Q3 generation did not finish a normal text output')
            if time.monotonic() >= request['deadline_monotonic']:
                raise TimeoutError('native model final output arrived after the batch deadline')
            call['status'] = 'succeeded'
        except BaseException as error:
            primary = error
            status = 'not_started' if not call['attempted'] else (
                'unknown' if isinstance(error, (asyncio.CancelledError, TimeoutError))
                or call.get('finish_reason') == 'abort' else 'failed')
            call.update(status=status, **_error(error))
            raise
        finally:
            try:
                if generator is not None:
                    await generator.aclose()
            except BaseException as cleanup:
                if primary is not None:
                    primary.add_note('native generator close also failed: ' + repr(cleanup))
                    call.update(**_error(primary))
                else:
                    call.update(status='unknown', **_error(cleanup))
                    raise
            finally:
                call['finished_monotonic_ns'] = time.monotonic_ns()

    async def _abort_and_drain(self, tasks, calls, primary):
        active = [call['request_id'] for call in calls
                  if call['attempted'] and call['status'] != 'succeeded']
        try:
            if active:
                await self.model.engine.abort(active)
        except BaseException as cleanup:
            primary.add_note('native abort also failed: ' + repr(cleanup))
            self.cleanup_error = self.cleanup_error or primary
            self.stop.set()
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for call in calls:
                if call['status'] == 'not_started' and 'error_type' not in call:
                    call.update(**_error(primary), finished_monotonic_ns=time.monotonic_ns())

    async def batch(self, request):
        batch_uid = uuid.uuid4().hex
        calls = [dict(position=position, request_id='imlane-q3-' + batch_uid + '-' + str(position),
                      attempted=False, status='not_started', usage={})
                 for position in range(len(request['messages_batch']))]
        tasks = [asyncio.create_task(self._row(request, messages, call))
                 for messages, call in zip(request['messages_batch'], calls, strict=True)]
        primary = None
        try:
            if tasks:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION,
                    timeout=max(0, request['deadline_monotonic'] - time.monotonic()))
                primary = next((task.exception() for task in tasks if task in done and not task.cancelled()
                                and task.exception() is not None), None)
                if primary is None and any(task.cancelled() for task in done):
                    primary = asyncio.CancelledError('native model row was cancelled')
                if primary is None and pending:
                    primary = TimeoutError('model IPC batch deadline exceeded')
        except BaseException as error:
            primary = error
        if primary is not None:
            await self._abort_and_drain(tasks, calls, primary)
        else:
            await asyncio.gather(*tasks)
        status = 'succeeded' if all(call['status'] == 'succeeded' for call in calls) else (
            'unknown' if any(call['status'] == 'unknown' for call in calls) else 'failed')
        return dict(schema=SCHEMA, batch_id=request['batch_id'], model=request['model'], status=status,
                    calls=calls, **({} if primary is None else _error(primary)))

    async def connection(self, reader, writer):
        work = disconnected = None
        primary = None
        try:
            request = _request(await read_frame(reader), self.config['served_model_name'])
            if self.stop.is_set():
                raise InterruptedError('native model owner is closing')
            work = asyncio.create_task(self.batch(request))
            disconnected = asyncio.create_task(reader.read(1))
            done, _ = await asyncio.wait((work, disconnected), return_when=asyncio.FIRST_COMPLETED)
            if work not in done:
                if disconnected.result():
                    raise ValueError('model IPC connection accepts exactly one batch frame')
                raise ConnectionError('model IPC client disconnected before its batch completed')
            await write_frame(writer, work.result())
        except BaseException as error:
            primary = error
        finally:
            for task in (work, disconnected):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(*(task for task in (work, disconnected) if task is not None),
                                 return_exceptions=True)
            writer.close()
            try:
                await writer.wait_closed()
            except BaseException as cleanup:
                if primary is None:
                    primary = cleanup
                else:
                    primary.add_note('model IPC connection close also failed: ' + repr(cleanup))
        if primary is not None:
            print(json.dumps(dict(event='model_ipc_connection_error', **_error(primary)), allow_nan=False),
                  file=sys.stderr, flush=True)


def _shutdown_deadline(path):
    if not Path(path).exists():
        return None
    value = decode_json(Path(path).read_bytes())
    if (type(value) is not dict or set(value) != {'schema', 'native_shutdown_deadline'}
            or value['schema'] != SHUTDOWN_SCHEMA):
        raise ValueError('invalid native model shutdown declaration')
    return _positive(value['native_shutdown_deadline'], 'native_shutdown_deadline')


async def serve(config, *, model_factory=native_model, stop=None, install_signals=True):
    """Own one socket and one engine; software factories are a CPU-test seam."""
    config = configuration(config)
    budget = request_budget(config['request_budget']) if 'request_budget' in config else None
    socket_path = Path(config['socket_path'])
    parent = socket_path.parent.lstat()
    if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid()
            or stat.S_IMODE(parent.st_mode) != 0o700):
        raise ValueError('model IPC socket requires an owned private 0700 parent directory')
    if os.path.lexists(socket_path):
        raise FileExistsError('model IPC socket path already exists')
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGTERM, signal.SIGINT) if install_signals else ()
    for signum in signals:
        loop.add_signal_handler(signum, stop.set)
    model = worker = server = identity = None
    primary = None
    try:
        model = model_factory(config)
        worker = ModelWorker(config, model, stop, budget)
        # Bind ourselves so start_unix_server cannot replace an existing socket.
        import socket
        bound = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            bound.bind(str(socket_path))
            observed = socket_path.lstat()
            identity = (observed.st_dev, observed.st_ino)
            os.chmod(socket_path, 0o600)
            bound.setblocking(False)
            server = await asyncio.start_unix_server(worker.accept, sock=bound)
        except BaseException:
            bound.close()
            raise
        publish_private_json(Path(config['ready_path']), _ready(model, config, identity))
        await stop.wait()
    except BaseException as error:
        primary = error
    finally:
        deadline = None
        try:
            deadline = _shutdown_deadline(config['shutdown_path'])
        except BaseException as cleanup:
            if primary is None:
                primary = cleanup
            else:
                primary.add_note('shutdown declaration also failed: ' + repr(cleanup))
        if server is not None:
            server.close()
        if worker is not None:
            connections = list(worker.connections)
            for task, writer in list(worker.connections.items()):
                writer.close()
                task.cancel()
            if connections:
                done, pending = await asyncio.wait(connections,
                    timeout=None if deadline is None else max(0, deadline - time.monotonic()))
                for task in done:
                    if not task.cancelled() and task.exception() is not None:
                        primary = primary or task.exception()
                if pending:
                    primary = primary or TimeoutError('native model connections did not drain before shutdown')
            primary = primary or worker.cleanup_error
        if server is not None:
            try:
                await asyncio.wait_for(server.wait_closed(),
                    timeout=None if deadline is None else max(0, deadline - time.monotonic()))
            except BaseException as cleanup:
                primary = _retain(primary, cleanup, 'model IPC listener close also failed')
        if model is not None:
            try:
                # v0.25.1 supports timeout for EngineCore, but renderer shutdown
                # has no timeout: the external owner's hard deadline still rules.
                remaining = None if deadline is None else max(0, deadline - time.monotonic())
                if remaining == 0:
                    primary = _retain(primary, TimeoutError('native model shutdown deadline exhausted'),
                                      'native shutdown has no remaining time')
                model.engine.shutdown(timeout=remaining)
                if deadline is not None and time.monotonic() >= deadline:
                    primary = _retain(primary, TimeoutError('native model shutdown returned after deadline'),
                                      'native shutdown exceeded its declared deadline')
            except BaseException as cleanup:
                if primary is None:
                    primary = cleanup
                else:
                    primary.add_note('native model shutdown also failed: ' + repr(cleanup))
        if identity is not None and os.path.lexists(socket_path):
            try:
                observed = socket_path.lstat()
                if (observed.st_dev, observed.st_ino) != identity or not stat.S_ISSOCK(observed.st_mode):
                    raise RuntimeError('model IPC socket identity changed during ownership')
                socket_path.unlink()
            except BaseException as cleanup:
                primary = _retain(primary, cleanup, 'model IPC socket cleanup also failed')
        for signum in signals:
            loop.remove_signal_handler(signum)
    if primary is not None:
        raise primary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    args = parser.parse_args(argv)
    config_path = Path(args.config)
    require_outside_git(config_path)
    asyncio.run(serve(decode_json(config_path.read_bytes())))


if __name__ == '__main__':
    main()
