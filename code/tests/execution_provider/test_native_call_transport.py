"""Complete-call transport contracts with no HTTP, model, or real Ray execution."""

import asyncio
from dataclasses import replace
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import sys
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from src.baselines.text.frameworks import native_calls
from src.execution_provider.adapters import native_call_transport, native_tasks
from src.execution_provider.adapters.full_response import FullModelResponse, decode_full_response
from src.execution_provider.adapters.incremental_execution import IncrementalExecution
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import (
    RayMapConfig, RayMapTransport, _Block, _HttpActor, _RemoteFailure, _RemoteResult, _Row,
    _model_identity, _native_call_node,
)
from src.scheduling.core.session_contract import BackendTask, SessionSpec, State, TaskKey, Usage
from src.scheduling.runtime.async_backend import ConfirmedTaskFailure
from tests.execution_provider.test_ray_map_transport import FakeRay


CONFIG = FixedModelConfig('http://localhost/fixture', 'fixture', 1000)


def call(index=0, **changes):
    value = native_calls.NativeCall('call-' + str(index), index,
        json.dumps(dict(model='fixture', messages=[dict(role='user', content='original')])).encode(),
        CONFIG.endpoint_url, (('Authorization', 'Bearer EMPTY'), ('x-repeat', 'one'), ('x-repeat', 'two')),
        1, connect_timeout_s=0.25, max_response_bytes=4096,
        skip_auto_headers=('Content-Type',), follow_redirects=False)
    return replace(value, **changes)


def request(value=None):
    value = value or call()
    task = native_tasks.prepare_native_task(native_calls._call_wire(value).encode(), value.row_id,
        row_sequence=value.row_id, call_id=value.call_id, max_result_bytes=value.max_response_bytes)
    return BackendTask(TaskKey(0, value.row_id), SessionSpec('query', 'operator', 'fixture'), task)


def table(*values):
    return {name: [SimpleNamespace(as_py=lambda item=item: item) for item in column] for name, column in
            dict(session_id=[value.key.session_id for value in values],
                 sequence=[value.key.sequence for value in values],
                 payload=[value.task.payload for value in values]).items()}


class Client:
    closed = False

    async def close(self):
        self.closed = True


class NativeRay(FakeRay):
    def __init__(self, execute, *, ready=None, nodes=None):
        super().__init__(execute)
        self.ready_reply = ready if ready is not None else dict(
            identity=_model_identity(CONFIG, 'native-call-full'), node_id='fixture-node')
        self.live_nodes = nodes if nodes is not None else [dict(NodeID='fixture-node', Alive=True)]

    def nodes(self):
        return self.live_nodes

    def get_runtime_context(self):
        return SimpleNamespace(gcs_address='fixture-cluster', get_node_id=lambda: 'fixture-node')

    def remote(self, **options):
        create = super().remote(**options)
        def cls(actual):
            factory = create(actual)
            original = factory.remote
            def actor(*args):
                value = original(*args)
                value.ready.remote = lambda: self.ready_reply
                return value
            factory.remote = actor
            return factory
        return cls


def physical(mode='native-call-full'):
    return RayMapConfig('fixture-cluster', 1, 1, 1048600, 2097200, response_mode=mode)


class NativeCallTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_call_and_compressed_http_error_survive_with_one_original_pool(self):
        seen, client = [], Client()
        body = gzip.compress(b'original error body')
        response = FullModelResponse(429, (('content-encoding', 'gzip'), ('x-repeat', 'one'), ('x-repeat', 'two')), body)
        async def kernel(value, control=None, *, client=None):
            seen.append((value, client))
            return native_calls.NativeCallOutcome(value.call_id, value.row_id, response)
        transport = native_call_transport.NativeCallTransport(CONFIG, 8)
        with patch.object(native_calls, '_new_http_client', return_value=client) as create, \
                patch.object(native_calls, '_request', kernel):
            try:
                for index in range(2):
                    value = call(index)
                    self.assertEqual(decode_full_response(await transport.execute(request(value), 'model')), response)
                    self.assertEqual(seen[-1], (value, client))
                self.assertEqual(create.call_count, 1)
            finally:
                await transport.close()
        self.assertTrue(client.closed)
        self.assertTrue(transport.closed)

    async def test_invalid_route_model_identity_or_reservation_never_enters_http(self):
        values = [call(endpoint='http://localhost/other'), call(payload=b'{"model":"other"}'),
                  call(headers=(('Authorization', 'Bearer other'),)), call(timeout_s=2), call(follow_redirects=True)]
        good = request()
        requests = [request(value) for value in values]
        requests += [replace(good, task=replace(good.task, info=replace(good.task.info, call_id='other'))),
                     replace(good, task=replace(good.task, max_result_bytes=100))]
        transport = native_call_transport.NativeCallTransport(CONFIG, 8)
        with patch.object(native_calls, '_request', new_callable=AsyncMock) as send, \
                patch.object(native_calls, '_new_http_client') as create:
            for value in requests:
                with self.subTest(sequence=value.key.sequence), self.assertRaises(ConfirmedTaskFailure) as failure:
                    await transport.execute(value, 'model')
                self.assertEqual(failure.exception.key, value.key)
            send.assert_not_called()
            create.assert_not_called()
        await transport.close()

    async def test_transport_error_and_wrong_response_identity_are_unknown(self):
        for outcome in [native_calls.NativeCallOutcome('call-0', 0, None, transport_error='TimeoutError'),
                        native_calls.NativeCallOutcome('other', 0, FullModelResponse(200, (), b'ok'))]:
            transport = native_call_transport.NativeCallTransport(CONFIG, 8)
            with patch.object(native_calls, '_new_http_client', return_value=Client()), \
                    patch.object(native_calls, '_request', AsyncMock(return_value=outcome)):
                try:
                    with self.assertRaisesRegex(RuntimeError, 'unknown') as failure:
                        await transport.execute(request(), 'model')
                    self.assertNotIsInstance(failure.exception, ConfirmedTaskFailure)
                finally:
                    await transport.close()

    def test_private_sources_are_guarded_and_source_identity_changes_worker_identity(self):
        identity = native_call_transport.native_call_transport_identity()
        self.assertIn('native_call_transport', identity['sources'])
        current = _model_identity(CONFIG, 'native-call-full')
        with patch.object(native_call_transport, 'native_call_transport_identity', return_value={'changed': True}):
            self.assertNotEqual(_model_identity(CONFIG, 'native-call-full'), current)
        with patch.dict(native_call_transport._SOURCE_HASHES, {native_calls: '0' * 64}):
            with self.assertRaisesRegex(RuntimeError, 'dependency changed'):
                native_call_transport.NativeCallTransport(CONFIG, 8)


class NativeCallRayTests(unittest.IsolatedAsyncioTestCase):
    async def test_old_identity_bytes_and_ready_behavior_are_unchanged(self):
        self.assertEqual(_model_identity(CONFIG), '484e87f3b779228318d51babab66e60b9fe5773f7a1b4f1afa31374f7dc43d94')
        self.assertEqual(_model_identity(CONFIG, 'full'), '915ad8681276374ea20c8ecb367b3f49f6686841eb76e319ac67cc479c7942bc')
        for mode in ('completion', 'full'):
            actor = _HttpActor(CONFIG, 2, response_mode=mode)
            try:
                self.assertIs(await actor.ready(), True)
            finally:
                await actor.close()

    async def test_new_ready_reports_actual_source_and_node_and_pool_claim_rejects_old_sources(self):
        actor = _HttpActor(CONFIG, 2, managed=True, response_mode='native-call-full')
        ray = NativeRay(None)
        with patch.dict(sys.modules, {'ray': ray}):
            self.assertEqual(await actor.ready(), dict(identity=actor.identity, node_id='fixture-node'))
        try:
            with self.assertRaisesRegex(ValueError, 'differs'):
                await actor.claim('a' * 32, 'changed-source', 2)
            self.assertTrue(await actor.claim('b' * 32, _model_identity(CONFIG, 'native-call-full'), 2))
            await actor.release('b' * 32)
        finally:
            await actor.close()

    def test_wrong_worker_source_node_or_multiple_live_nodes_fail_before_dispatch(self):
        sent = AsyncMock()
        for reply, nodes in [(dict(identity='wrong', node_id='fixture-node'), None),
                (dict(identity=_model_identity(CONFIG, 'native-call-full'), node_id='other'), None),
                (None, [dict(NodeID='fixture-node', Alive=True), dict(NodeID='other', Alive=True)])]:
            ray = NativeRay(sent, ready=reply, nodes=nodes)
            with self.subTest(reply=reply, nodes=nodes), self.assertRaisesRegex(ValueError, 'node|source'):
                RayMapTransport(CONFIG, 8, physical=physical(), ray_api=ray)
        sent.assert_not_called()
        self.assertEqual(_native_call_node(NativeRay(None)), 'fixture-node')

    async def test_confirmed_pre_send_failure_releases_only_its_block_without_http(self):
        actor = _HttpActor(CONFIG, 8, response_mode='native-call-full')
        value = request(call(endpoint='http://localhost/other'))
        ray = NativeRay(actor.execute)
        transport = RayMapTransport(CONFIG, 8, physical=physical(), ray_api=ray)
        transport.blocks[0] = _Block(table(value), 64, {value.key})
        transport.used_bytes = 64
        row = _Row(value, asyncio.get_running_loop().create_future())
        with patch.object(native_calls, '_request', new_callable=AsyncMock) as send:
            try:
                await transport._run_row(row, 0, 0)
                with self.assertRaises(ConfirmedTaskFailure):
                    await row.future
                self.assertEqual(transport.used_bytes, 0)
                self.assertEqual(transport.unknown, set())
                send.assert_not_called()
            finally:
                await transport.close()
                await actor.close()

    async def test_http_error_remains_full_response_through_actor_and_existing_row_completion(self):
        actor = _HttpActor(CONFIG, 8, response_mode='native-call-full')
        value = request()
        response = FullModelResponse(429, (('x-repeat', 'one'), ('x-repeat', 'two')), b'original')
        transport = RayMapTransport(CONFIG, 8, physical=physical(), ray_api=NativeRay(actor.execute))
        transport.blocks[0] = _Block(table(value), 64, {value.key})
        transport.used_bytes = 64
        row = _Row(value, asyncio.get_running_loop().create_future())
        outcome = native_calls.NativeCallOutcome('call-0', 0, response)
        with patch.object(native_calls, '_new_http_client', return_value=Client()), \
                patch.object(native_calls, '_request', AsyncMock(return_value=outcome)):
            try:
                await transport._run_row(row, 0, 0)
                self.assertEqual(decode_full_response(await row.future), response)
                self.assertEqual(transport.used_bytes, 0)
                self.assertEqual(transport.unknown, set())
            finally:
                await transport.close()
                await actor.close()

    async def test_wrong_key_unconfirmed_http_or_rpc_error_cannot_release_object(self):
        value = request()
        async def rpc_error(*args):
            raise OSError('fixture RPC reply unavailable')
        replies = [_RemoteFailure(TaskKey(99, 0), 'pre_send', {'exception_type': 'ValueError'}),
                   _RemoteFailure(value.key, 'http', {'exception_type': 'RuntimeError'}),
                   _RemoteFailure(value.key, 'invalid-stage', {'exception_type': 'ValueError'}), rpc_error]
        for reply in replies:
            execute = reply if callable(reply) else AsyncMock(return_value=reply)
            transport = RayMapTransport(CONFIG, 8, physical=physical(), ray_api=NativeRay(execute))
            transport.blocks[0] = _Block(table(value), 64, {value.key})
            transport.used_bytes = 64
            row = _Row(value, asyncio.get_running_loop().create_future())
            await transport._run_row(row, 0, 0)
            with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
                await row.future
            self.assertEqual(transport.used_bytes, 64)
            self.assertEqual(transport.unknown, {value.key})
            with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
                await transport.close()

    async def test_unknown_blocks_same_block_new_block_and_new_entry_only_in_new_mode(self):
        for mode in ('native-call-full', 'full', 'completion'):
            dispatched = []
            async def reply(actual, index, template):
                dispatched.append(template.key.sequence)
                if template.key.sequence == 0:
                    return _RemoteFailure(template.key, 'http', {'exception_type': 'RuntimeError'})
                return _RemoteResult(template.key, b'fixture-response', 0, 0, None)
            transport = RayMapTransport(CONFIG, 8, physical=physical(mode), ray_api=NativeRay(reply))
            values = [request(call(index)) for index in range(4)]
            transport.blocks[0] = _Block(table(*values[:2]), 64, {value.key for value in values[:2]})
            transport.used_bytes = 64
            rows = [_Row(value, asyncio.get_running_loop().create_future()) for value in values[:3]]
            try:
                await transport._run_row(rows[0], 0, 0)
                with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
                    await rows[0].future
                transport.blocks[1] = _Block(table(values[2]), 64, {values[2].key})
                transport.used_bytes += 64
                for row, block, index in [(rows[1], 0, 1), (rows[2], 1, 0)]:
                    await transport._run_row(row, block, index)
                    if mode == 'native-call-full':
                        with self.assertRaises(ConfirmedTaskFailure):
                            await row.future
                    else:
                        self.assertEqual(await row.future, b'fixture-response')
                if mode == 'native-call-full':
                    with self.assertRaises(ConfirmedTaskFailure):
                        await transport.execute(values[3], 'model')
                    self.assertNotIn(values[3].key, transport.rows)
                self.assertEqual(dispatched, [0] if mode == 'native-call-full' else [0, 1, 2])
                self.assertEqual(transport.unknown, {values[0].key})
                self.assertEqual(transport.used_bytes, 64)
                self.assertEqual(transport.blocks[0].members, {values[0].key})
                self.assertNotIn(1, transport.blocks)
            finally:
                with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
                    await transport.close()

    async def test_unknown_during_async_guard_blocks_its_unsent_waiter(self):
        entered, release = asyncio.Event(), asyncio.Event()
        dispatched = []
        async def guard(value):
            if value.key.sequence == 1:
                entered.set()
                await release.wait()
        async def reply(actual, index, template):
            dispatched.append(template.key.sequence)
            return _RemoteFailure(template.key, 'http', {'exception_type': 'RuntimeError'})
        transport = RayMapTransport(CONFIG, 8, physical=physical(), ray_api=NativeRay(reply), before_request=guard)
        values = [request(call(index)) for index in range(2)]
        rows = [_Row(value, asyncio.get_running_loop().create_future()) for value in values]
        for index, value in enumerate(values):
            transport.blocks[index] = _Block(table(value), 64, {value.key})
        transport.used_bytes = 128
        waiter = asyncio.create_task(transport._run_row(rows[1], 1, 0))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            await transport._run_row(rows[0], 0, 0)
            with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
                await rows[0].future
            release.set()
            await waiter
            with self.assertRaises(ConfirmedTaskFailure):
                await rows[1].future
            self.assertEqual(dispatched, [0])
            self.assertFalse(rows[1].sent)
            self.assertEqual(transport.used_bytes, 64)
            self.assertEqual(transport.unknown, {values[0].key})
        finally:
            release.set()
            await waiter
            with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
                await transport.close()

    async def test_unknown_keeps_already_sent_peer_until_its_actual_reply(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def reply(actual, index, template):
            if template.key.sequence == 1:
                entered.set()
                await release.wait()
                return _RemoteResult(template.key, b'actual-peer-response', 0, 0, None)
            return _RemoteFailure(template.key, 'http', {'exception_type': 'RuntimeError'})
        transport = RayMapTransport(CONFIG, 8, physical=physical(), ray_api=NativeRay(reply))
        values = [request(call(index)) for index in range(2)]
        rows = [_Row(value, asyncio.get_running_loop().create_future()) for value in values]
        for index, value in enumerate(values):
            transport.blocks[index] = _Block(table(value), 64, {value.key})
        transport.used_bytes = 128
        peer = asyncio.create_task(transport._run_row(rows[1], 1, 0))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            self.assertTrue(rows[1].sent)
            await transport._run_row(rows[0], 0, 0)
            with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
                await rows[0].future
            self.assertFalse(peer.done())
            self.assertEqual(transport.used_bytes, 128)
            release.set()
            await peer
            self.assertEqual(await rows[1].future, b'actual-peer-response')
            self.assertEqual(transport.used_bytes, 64)
            self.assertEqual(transport.unknown, {values[0].key})
        finally:
            release.set()
            await peer
            with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
                await transport.close()

    async def test_unknown_wakes_object_wait_and_drains_pending_without_put(self):
        waiting = asyncio.Event()
        class Changed(asyncio.Event):
            async def wait(self):
                waiting.set()
                return await super().wait()
        class Table(dict):
            num_rows = 1
            def get_total_buffer_size(self):
                return 64
        def batches(data, *args, **kwargs):
            yield Table(table(request(call(1))))
        dispatched = []
        async def reply(actual, index, template):
            dispatched.append(template.key.sequence)
            return _RemoteFailure(template.key, 'http', {'exception_type': 'RuntimeError'})
        ray = NativeRay(reply)
        transport = RayMapTransport(CONFIG, 8, physical=physical(), ray_api=ray)
        transport.changed = Changed()
        first = request()
        transport.blocks[0] = _Block(table(first), transport.physical.object_bytes, {first.key})
        transport.used_bytes = transport.physical.object_bytes
        original = _Row(first, asyncio.get_running_loop().create_future())
        with patch('src.execution_provider.adapters.ray_map_transport.iter_payload_batches', batches):
            queued = asyncio.create_task(transport.execute(request(call(1)), 'model'))
            try:
                await asyncio.wait_for(waiting.wait(), 1)
                await transport._run_row(original, 0, 0)
                with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
                    await original.future
                with self.assertRaises(ConfirmedTaskFailure):
                    await asyncio.wait_for(queued, 1)
                extra = _Row(request(call(2)), asyncio.get_running_loop().create_future())
                transport.pending.append(extra)
                await transport._flush()
                with self.assertRaises(ConfirmedTaskFailure):
                    await extra.future
                self.assertEqual(dispatched, [0])
                self.assertEqual(ray.puts, [])
                self.assertEqual(transport.used_bytes, transport.physical.object_bytes)
                self.assertEqual(transport.blocks[0].members, {first.key})
            finally:
                transport.changed.set()
                if not queued.done():
                    queued.cancel()
                await asyncio.gather(queued, return_exceptions=True)
                with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
                    await transport.close()

    async def test_confirmed_failure_with_wrong_key_or_old_mode_is_not_promoted(self):
        for mode, key in [('native-call-full', TaskKey(99, 0)), ('full', TaskKey(0, 0))]:
            actor = _HttpActor(CONFIG, 8, response_mode=mode)
            actor.transport.execute = AsyncMock(side_effect=ConfirmedTaskFailure(key, {'exception_type': 'ValueError'}))
            value = request()
            try:
                reply = await actor.execute(table(value), 0, replace(value, task=replace(value.task, payload=b'')))
                self.assertEqual(reply.stage, 'http')
            finally:
                await actor.close()

    def test_native_builder_preserves_only_explicit_complete_call_mode(self):
        execution = IncrementalExecution(SimpleNamespace(), None, lambda timeout=5: True, 1)
        for mode, selected in [('completion', 'full'), ('full', 'full'), ('native-call-full', 'native-call-full')]:
            with patch.object(native_tasks, '_ray_transport_options', return_value={}) as factory, \
                    patch.object(native_tasks, '_build_model_execution', return_value=execution) as build:
                native_tasks.build_native_execution(CONFIG, physical=physical(mode))
            self.assertEqual(factory.call_args.args[0].response_mode, selected)
            self.assertEqual(factory.call_args.args[1], native_tasks.MAX_FRAME_BYTES)
            self.assertEqual(build.call_args.kwargs['item_input_bytes'], native_tasks.MAX_FRAME_BYTES)

    def test_confirmed_presend_failure_settles_real_core_without_http(self):
        execution = native_tasks.build_native_execution(CONFIG, physical=None,
            transport_factory=native_call_transport.NativeCallTransport, max_tasks=2)
        job = native_tasks.NativeQueryJob(execution, 'presend-query')
        flow = job.open_session('model')
        try:
            value = request(call(endpoint='http://localhost/other')).task
            self.assertEqual(flow.offer((value,)).accepted_prefix_count, 1)
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                progress = flow.advance(1)
                if progress.state == State.FAILED:
                    break
                flow.wait(progress)
            self.assertEqual(progress.state, State.FAILED)
            flow.close()
            job.close()
            execution.engine.advance()
            self.assertEqual(execution.engine.capacity.usage(), Usage())
            self.assertFalse(execution.engine.jobs.jobs)
            self.assertTrue(execution.close())
        finally:
            flow.close()
            job.close()
            execution.engine.backend.close()


@unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_CALL_HTTP_TEST') == '1',
                     'real loopback nonredirect qualification is explicitly opt-in')
class NativeCallHttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_307_and_308_return_original_response_without_a_second_post(self):
        for status in (307, 308):
            posts = []
            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *_):
                    pass

                def do_POST(self):
                    self.rfile.read(int(self.headers['Content-Length']))
                    posts.append(self.path)
                    self.send_response(status if self.path == '/start' else 200)
                    self.send_header('Location', '/redirect-target')
                    self.send_header('Content-Length', '8')
                    self.end_headers()
                    self.wfile.write(b'original')
            server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            endpoint = f'http://127.0.0.1:{server.server_port}/start'
            transport = native_call_transport.NativeCallTransport(replace(CONFIG, endpoint_url=endpoint), 1)
            try:
                response = decode_full_response(await transport.execute(request(call(endpoint=endpoint)), 'model'))
                self.assertEqual((response.status_code, response.body), (status, b'original'))
                self.assertIn(('Location', '/redirect-target'), response.headers)
                self.assertEqual(posts, ['/start'])
            finally:
                await transport.close()
                server.shutdown()
                server.server_close()
                thread.join(3)


if __name__ == '__main__':
    unittest.main()
