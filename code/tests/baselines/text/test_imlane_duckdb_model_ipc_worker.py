"""Real Unix sockets with a deterministic async engine, never a model runtime."""

import asyncio
from copy import deepcopy
import io
import json
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import threading
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from integrations.imlane_duckdb import model_ipc_worker as worker
from integrations.imlane_duckdb.model_wire import SCHEMA, SHUTDOWN_SCHEMA, read_frame, write_frame
from src.baselines.text.sembench_movie_native import q3_messages


PARAMS = dict(temperature=0, top_p=1, max_tokens=8192, n=1, stop=None,
              presence_penalty=0, frequency_penalty=0, repetition_penalty=1)


class FakeSampling:
    def __init__(self, **values):
        self.__dict__.update(values)


class FakeRenderer:
    def __init__(self):
        self.calls = []
        self.prompt_tokens = 3
        self.gate = None
        self.tokenizer = object()

    async def render_chat_async(self, conversations, chat_params):
        index = len(self.calls)
        self.calls.append((deepcopy(conversations), chat_params))
        if self.gate is not None:
            await self.gate.wait()
        return conversations, [dict(type='token', prompt_token_ids=list(range(self.prompt_tokens)),
                                    fixture_index=index)]


class FakeEngine:
    def __init__(self):
        self.renderer = FakeRenderer()
        self.vllm_config = SimpleNamespace(
            model_config=SimpleNamespace(dtype='bfloat16', max_model_len=8195, generation_config='vllm'),
            scheduler_config=SimpleNamespace(max_num_seqs=77, max_num_batched_tokens=16384),
            cache_config=SimpleNamespace(), parallel_config=SimpleNamespace(tensor_parallel_size=1))
        self.model_config = SimpleNamespace(enable_prompt_embeds=False)
        self.release, self.entered = asyncio.Event(), asyncio.Event()
        self.expected = 1
        self.calls, self.aborts, self.drained, self.shutdowns = [], [], [], []
        self.active = set()
        self.fail_first = False
        self.abort_error = self.shutdown_error = None
        self.shutdown_delay = 0
        self.generate_delay = 0
        self.texts, self.finishes = {}, {}

    async def generate(self, prompt, sampling, request_id):
        index = prompt['fixture_index']
        self.calls.append((prompt, sampling, request_id, time.monotonic()))
        self.active.add(request_id)
        if len(self.calls) >= self.expected:
            self.entered.set()
        try:
            await asyncio.sleep(0)
            if self.fail_first and index == 0:
                raise RuntimeError('original native row failure')
            await self.release.wait()
            if self.generate_delay:
                time.sleep(self.generate_delay)
            yield SimpleNamespace(finished=True, request_id=request_id,
                prompt_token_ids=prompt['prompt_token_ids'], outputs=[SimpleNamespace(index=0,
                    text=self.texts.get(index, 'Answer: True'), token_ids=[10],
                    finish_reason=self.finishes.get(index, 'stop'))])
        finally:
            self.active.discard(request_id)
            self.drained.append(request_id)

    async def abort(self, ids):
        self.aborts.append(list(ids))
        if self.abort_error is not None:
            raise self.abort_error

    def shutdown(self, timeout=None):
        self.shutdowns.append(dict(timeout=timeout, thread=threading.get_ident(),
                                   loop=asyncio.get_running_loop(), active=set(self.active)))
        if self.shutdown_delay:
            time.sleep(self.shutdown_delay)
        if self.shutdown_error is not None:
            raise self.shutdown_error


class ModelWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='imlane-ipc-', dir='/tmp')
        self.directory = Path(self.temporary.name)
        self.config = dict(model='/fixture/model', served_model_name='fixture', dtype='bfloat16',
            engine_options={}, socket_path=str(self.directory / 'rpc.sock'),
            ready_path=str(self.directory / 'ready.json'), shutdown_path=str(self.directory / 'shutdown.json'))
        self.engine, self.stop = FakeEngine(), asyncio.Event()
        self.task = None
        self.factory_calls = []
        self.stderr = io.StringIO()
        self.guards = [patch.object(worker.sys, 'stderr', self.stderr),
                       patch.object(subprocess, 'Popen', side_effect=AssertionError('process launch forbidden'))]
        for guard in self.guards:
            guard.start()

    async def asyncTearDown(self):
        try:
            if self.task is not None:
                self.stop.set()
                await asyncio.wait_for(asyncio.shield(self.task), 2)
        finally:
            for guard in reversed(self.guards):
                guard.stop()
            self.temporary.cleanup()

    def factory(self, config):
        self.factory_calls.append(deepcopy(config))
        return worker.NativeModel(self.engine, FakeSampling, 'final-only', object(), 'software-fixture')

    async def start(self):
        self.task = asyncio.create_task(worker.serve(self.config, model_factory=self.factory,
            stop=self.stop, install_signals=False))
        until = time.monotonic() + 2
        while not Path(self.config['ready_path']).exists():
            if self.task.done():
                self.task.result()
            if time.monotonic() > until:
                self.fail('software worker did not publish ready')
            await asyncio.sleep(.001)
        return json.loads(Path(self.config['ready_path']).read_text())

    def request(self, count, **changes):
        value = dict(schema=SCHEMA, batch_id='batch-' + str(time.monotonic_ns()), model='fixture',
            sampling_params=deepcopy(PARAMS), messages_batch=[q3_messages(str(i)) for i in range(count)],
            deadline_monotonic=time.monotonic() + 2)
        value.update(changes)
        return value

    async def call(self, request):
        reader, writer = await asyncio.open_unix_connection(self.config['socket_path'])
        try:
            await write_frame(writer, request)
            return await asyncio.wait_for(read_frame(reader), 2)
        finally:
            writer.close()
            await writer.wait_closed()

    async def until_drained(self, count):
        async with asyncio.timeout(1):
            while len(self.engine.drained) < count:
                await asyncio.sleep(.001)

    async def test_four_clients_enter_one_engine_before_any_batch_returns(self):
        ready = await self.start()
        self.engine.expected = 8
        requests = [self.request(2) for _ in range(4)]
        calls = [asyncio.create_task(self.call(request)) for request in requests]
        try:
            await asyncio.wait_for(self.engine.entered.wait(), 1)
            self.assertEqual(len(self.engine.active), 8)
            self.assertTrue(all(not call.done() for call in calls))
            self.engine.release.set()
            responses = await asyncio.gather(*calls)
        finally:
            self.engine.release.set()
            await asyncio.gather(*calls, return_exceptions=True)
        self.assertEqual(len(self.factory_calls), 1)
        self.assertEqual(ready['engine_instances'], 1)
        self.assertEqual(ready['native_capacity']['scheduler_config']['max_num_seqs'], 77)
        self.assertIsNone(ready['native_capacity']['cache_config']['num_gpu_blocks'])
        self.assertIn('not free capacity', ready['native_capacity_source'])
        for request, response in zip(requests, responses, strict=True):
            self.assertEqual(response['batch_id'], request['batch_id'])
            self.assertEqual(response['status'], 'succeeded')
            self.assertEqual([call['position'] for call in response['calls']], [0, 1])
        self.assertEqual(len({call[2] for call in self.engine.calls}), 8)

    async def test_129_rows_in_one_batch_have_no_adapter_submission_cap(self):
        await self.start()
        self.engine.expected = 129
        task = asyncio.create_task(self.call(self.request(129)))
        try:
            await asyncio.wait_for(self.engine.entered.wait(), 1)
            self.assertEqual(len(self.engine.active), 129)
            self.engine.release.set()
            response = await task
        finally:
            self.engine.release.set()
            await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(response['status'], 'succeeded')
        self.assertEqual(len(response['calls']), 129)
        self.assertFalse(self.engine.active)

    async def test_original_messages_sampler_empty_text_and_length_are_preserved(self):
        await self.start()
        self.engine.release.set()
        self.engine.texts = {0: '', 1: 'unchanged length-limited output'}
        self.engine.finishes[1] = 'length'
        request = self.request(2)
        response = await self.call(request)
        self.assertEqual([call[0][0] for call in self.engine.renderer.calls], request['messages_batch'])
        self.assertEqual([call['generated_text'] for call in response['calls']],
                         ['', 'unchanged length-limited output'])
        self.assertEqual(response['calls'][1]['finish_reason'], 'length')
        for _, sampling, _, _ in self.engine.calls:
            self.assertEqual(vars(sampling), dict(PARAMS, output_kind='final-only'))
        for call in response['calls']:
            self.assertEqual((call['requested_max_tokens'], call['resolved_max_tokens'],
                              call['rendered_prompt_tokens']), (8192, 8192, 3))
            self.assertEqual(call['usage'], dict(prompt_tokens=3, completion_tokens=1, total_tokens=4))

    async def test_actual_context_boundary_accepts_exact_and_rejects_plus_one_without_generate(self):
        await self.start()
        self.engine.release.set()
        self.assertEqual((await self.call(self.request(1)))['status'], 'succeeded')
        request = self.request(1, sampling_params=dict(PARAMS, max_tokens=8193))
        response = await self.call(request)
        self.assertEqual(len(self.engine.calls), 1)
        self.assertEqual(response['status'], 'failed')
        self.assertFalse(response['calls'][0]['attempted'])
        self.assertEqual(response['calls'][0]['requested_max_tokens'], 8193)
        self.assertEqual(response['calls'][0]['resolved_max_tokens'], 8193)
        self.assertEqual(response['calls'][0]['rendered_prompt_tokens'], 3)

    async def test_expired_deadline_and_rendering_past_deadline_start_zero_generations(self):
        await self.start()
        expired = await self.call(self.request(2, deadline_monotonic=time.monotonic() - .01))
        self.assertEqual(expired['status'], 'failed')
        self.engine.renderer.gate = asyncio.Event()
        slow = await self.call(self.request(2, deadline_monotonic=time.monotonic() + .03))
        for response in (expired, slow):
            self.assertTrue(all(call['status'] == 'not_started' and not call['attempted']
                                for call in response['calls']))
        self.assertEqual(self.engine.calls, [])
        self.assertEqual(self.engine.aborts, [])

    async def test_deadline_aborts_all_owned_requests_and_drains_generators(self):
        await self.start()
        response = await self.call(self.request(2, deadline_monotonic=time.monotonic() + .03))
        self.assertEqual(response['status'], 'unknown')
        self.assertEqual(len(self.engine.aborts[0]), 2)
        self.assertEqual(len(self.engine.drained), 2)
        self.assertFalse(self.engine.active)
        self.assertTrue(all(call['attempted'] and call['usage'] == {} for call in response['calls']))

    async def test_native_error_is_retained_and_other_requests_abort_and_drain(self):
        await self.start()
        self.engine.fail_first = True
        response = await self.call(self.request(2))
        self.assertEqual(response['error_type'], 'RuntimeError')
        self.assertEqual(response['error'], 'original native row failure')
        self.assertEqual(response['calls'][0]['status'], 'failed')
        self.assertEqual(response['calls'][1]['status'], 'unknown')
        self.assertEqual(len(self.engine.aborts[0]), 2)
        self.assertEqual(len(self.engine.drained), 2)
        self.assertFalse(self.engine.active)

    async def test_final_output_blocking_loop_past_deadline_keeps_text_and_usage_unknown(self):
        await self.start()
        self.engine.release.set()
        self.engine.generate_delay = .03
        response = await self.call(self.request(1, deadline_monotonic=time.monotonic() + .01))
        self.assertEqual(response['status'], 'unknown')
        call, = response['calls']
        self.assertEqual(call['status'], 'unknown')
        self.assertEqual(call['error_type'], 'TimeoutError')
        self.assertEqual(call['generated_text'], 'Answer: True')
        self.assertEqual(call['finish_reason'], 'stop')
        self.assertEqual(call['usage'], dict(prompt_tokens=3, completion_tokens=1, total_tokens=4))
        self.assertEqual(len(self.engine.aborts[0]), 1)
        self.assertFalse(self.engine.active)

    async def test_disconnect_aborts_owned_requests_and_drains_before_reuse(self):
        await self.start()
        self.engine.expected = 2
        _reader, writer = await asyncio.open_unix_connection(self.config['socket_path'])
        await write_frame(writer, self.request(2))
        await asyncio.wait_for(self.engine.entered.wait(), 1)
        writer.close()
        await writer.wait_closed()
        await self.until_drained(2)
        self.assertEqual(len(self.engine.aborts[0]), 2)
        self.assertFalse(self.engine.active)
        self.assertIn('client disconnected', self.stderr.getvalue())
        self.engine.release.set()
        self.assertEqual((await self.call(self.request(1)))['status'], 'succeeded')

    async def test_shutdown_stops_accept_aborts_drains_and_calls_native_on_owner_loop(self):
        ready = await self.start()
        metadata = Path(self.config['socket_path']).lstat()
        self.assertTrue(stat.S_ISSOCK(metadata.st_mode))
        self.assertEqual(stat.S_IMODE(metadata.st_mode), 0o600)
        self.assertEqual(ready['socket_identity'], dict(device=metadata.st_dev, inode=metadata.st_ino))
        self.engine.expected = 2
        client = asyncio.create_task(self.call(self.request(2)))
        await asyncio.wait_for(self.engine.entered.wait(), 1)
        until = time.monotonic() + .5
        Path(self.config['shutdown_path']).write_text(json.dumps(dict(schema=SHUTDOWN_SCHEMA,
                                                               native_shutdown_deadline=until)))
        self.stop.set()
        await asyncio.wait_for(self.task, 1)
        self.task = None
        await asyncio.gather(client, return_exceptions=True)
        self.assertFalse(self.engine.active)
        self.assertEqual(len(self.engine.drained), 2)
        close, = self.engine.shutdowns
        self.assertTrue(0 < close['timeout'] <= .5)
        self.assertEqual(close['thread'], threading.get_ident())
        self.assertIs(close['loop'], asyncio.get_running_loop())
        self.assertEqual(close['active'], set())
        self.assertFalse(Path(self.config['socket_path']).exists())
        self.assertTrue(self.directory.exists())

    async def test_abort_failure_preserves_primary_and_forces_worker_failure(self):
        await self.start()
        self.engine.fail_first = True
        self.engine.abort_error = RuntimeError('native abort broke')
        client = asyncio.create_task(self.call(self.request(2)))
        with self.assertRaisesRegex(RuntimeError, 'original native row failure') as caught:
            await asyncio.wait_for(asyncio.shield(self.task), 1)
        self.task = None
        await asyncio.gather(client, return_exceptions=True)
        self.assertIn('native abort broke', str(caught.exception.__notes__))
        self.assertFalse(self.engine.active)
        self.assertEqual(len(self.engine.shutdowns), 1)

    async def test_expired_shutdown_deadline_is_not_success_even_if_native_returns(self):
        await self.start()
        Path(self.config['shutdown_path']).write_text(json.dumps(dict(schema=SHUTDOWN_SCHEMA,
            native_shutdown_deadline=time.monotonic() - .01)))
        self.stop.set()
        with self.assertRaises(TimeoutError):
            await self.task
        self.task = None
        self.assertEqual(self.engine.shutdowns[0]['timeout'], 0)

    async def test_native_shutdown_return_after_declared_deadline_is_not_success(self):
        await self.start()
        self.engine.shutdown_delay = .03
        Path(self.config['shutdown_path']).write_text(json.dumps(dict(schema=SHUTDOWN_SCHEMA,
            native_shutdown_deadline=time.monotonic() + .01)))
        self.stop.set()
        with self.assertRaisesRegex(TimeoutError, 'returned after deadline'):
            await self.task
        self.task = None

    async def test_ready_publication_failure_preserves_primary_and_shutdown_error(self):
        ready = Path(self.config['ready_path'])
        ready.write_text('original evidence')
        self.engine.shutdown_error = RuntimeError('native shutdown broke')
        with self.assertRaises(FileExistsError) as caught:
            await worker.serve(self.config, model_factory=self.factory, stop=self.stop, install_signals=False)
        self.assertEqual(ready.read_text(), 'original evidence')
        self.assertIn('native shutdown broke', str(caught.exception.__notes__))
        self.assertFalse(Path(self.config['socket_path']).exists())

    async def test_shutdown_error_is_not_success_and_foreign_socket_path_is_preserved(self):
        await self.start()
        socket_path = Path(self.config['socket_path'])
        socket_path.unlink()
        socket_path.write_text('replacement belongs to another owner')
        self.engine.shutdown_error = RuntimeError('native shutdown broke')
        self.stop.set()
        with self.assertRaisesRegex(RuntimeError, 'native shutdown broke') as caught:
            await self.task
        self.task = None
        self.assertIn('socket identity changed', str(caught.exception.__notes__))
        self.assertEqual(socket_path.read_text(), 'replacement belongs to another owner')

    async def test_existing_socket_path_and_nonprivate_parent_fail_before_allocation(self):
        path = Path(self.config['socket_path'])
        path.write_text('do not replace')
        with self.assertRaises(FileExistsError):
            await worker.serve(self.config, model_factory=self.factory, install_signals=False)
        self.assertEqual(path.read_text(), 'do not replace')
        path.unlink()
        self.directory.chmod(0o755)
        try:
            with self.assertRaisesRegex(ValueError, '0700'):
                await worker.serve(self.config, model_factory=self.factory, install_signals=False)
        finally:
            self.directory.chmod(0o700)
        self.assertEqual(self.factory_calls, [])

    def test_native_factory_uses_public_api_and_passes_options_without_capacity_defaults(self):
        modules = {name: ModuleType(name) for name in (
            'vllm', 'vllm.engine', 'vllm.engine.arg_utils', 'vllm.renderers', 'vllm.sampling_params',
            'vllm.utils', 'vllm.utils.mistral', 'vllm.v1', 'vllm.v1.engine', 'vllm.v1.engine.async_llm')}
        calls = []
        modules['vllm'].__version__ = 'software-public-api-fixture'
        modules['vllm.engine.arg_utils'].AsyncEngineArgs = lambda **kwargs: kwargs
        modules['vllm.renderers'].ChatParams = lambda **kwargs: kwargs
        modules['vllm.sampling_params'].RequestOutputKind = SimpleNamespace(FINAL_ONLY='final-only')
        modules['vllm.sampling_params'].SamplingParams = FakeSampling
        modules['vllm.utils.mistral'].is_mistral_tokenizer = lambda tokenizer: False
        modules['vllm.v1.engine.async_llm'].AsyncLLM = SimpleNamespace(
            from_engine_args=lambda args: (calls.append(args), self.engine)[1])
        config = dict(self.config, engine_options=dict(gpu_memory_utilization=.7, enable_prefix_caching=True))
        with patch.dict(sys.modules, modules):
            model = worker.native_model(config)
        self.assertEqual(calls, [dict(model='/fixture/model', served_model_name='fixture', dtype='bfloat16', generation_config='vllm',
                                    gpu_memory_utilization=.7, enable_prefix_caching=True)])
        self.assertEqual(model.chat_params, dict(chat_template_content_format='auto',
            chat_template_kwargs=dict(add_generation_prompt=True, continue_final_message=False,
                                      tools=None, tokenize=False)))
        self.assertIs(model.engine, self.engine)

    def test_configuration_and_request_reject_real_reserved_or_nonfinite_boundaries(self):
        for options in ({'generation_config': 'auto'}, {'model': 'other'},
                        {'override_generation_config': {}}, {'max_num_seqs': float('nan')}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                worker.configuration(dict(self.config, engine_options=options))
        for change in (dict(model='other'), dict(deadline_monotonic=0),
                       dict(sampling_params=dict(PARAMS, n=2)),
                       dict(sampling_params=dict(PARAMS, temperature=float('nan')))):
            with self.subTest(change=change), self.assertRaises(ValueError):
                worker._request(self.request(1, **change), 'fixture')


if __name__ == '__main__':
    unittest.main()
