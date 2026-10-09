"""Real socket frames, controlled completions, cancellation and finite ownership."""

import asyncio
import base64
import json
import socket
import struct
import threading
import time
import unittest

from src.execution_provider.adapters.duckdb_batch_socket import DuckDBBatchConnection, PROTOCOL
from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.execution_provider.wire.framing import encode_frame, read_frame
from src.scheduling.core.session_contract import Usage


class Service:
    def __init__(self, execute, **options):
        self.client, self.server = socket.socketpair()
        self.client.settimeout(3)
        self.ready, self.returned, self.reap = threading.Event(), threading.Event(), threading.Event()
        self.failure = None
        self.execution = None
        def run():
            try:
                self.execution = build_native_execution(FixedModelConfig('http://localhost/fixture', 'fixture', 1000),
                    physical=None, execute=execute, max_tasks=4, **options)
                self.ready.set()
                DuckDBBatchConnection(self.execution, frame_timeout_s=0.5).serve(self.server)
                self.at_disconnect = self.execution.engine.capacity.usage()
                self.returned.set()
                self.reap.wait(3)
                deadline = time.monotonic() + 2
                while self.execution.engine.capacity.usage().held_tasks and time.monotonic() < deadline:
                    progress = self.execution.engine.advance()
                    if not progress.has_immediate_work:
                        self.execution.engine.wake.wait(self.execution.engine.wake.generation, 0.005)
                self.settled = self.execution.engine.capacity.usage()
                self.closed = self.execution.close()
            except BaseException as error:
                self.failure = error
            finally:
                self.ready.set()
                self.returned.set()
                self.server.close()
        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()
        assert self.ready.wait(3)
        if self.failure: raise self.failure

    def send(self, kind, **fields):
        self.client.sendall(encode_frame(dict(type=kind, protocol=PROTOCOL, **fields)))

    def read(self):
        return read_frame(self.client)

    def open(self):
        self.send('open', query_id='query', operator_id='map')
        return self.read()

    def close(self):
        self.client.close()
        self.reap.set()
        self.thread.join(5)
        if self.thread.is_alive(): raise AssertionError('socket owner did not stop')
        if self.failure: raise self.failure


def row(sequence, payload=None):
    return dict(sequence=str(sequence), row_sequence=str(sequence + 100), call_id=f'row-{sequence}',
                stage_id='model', payload=payload or json.dumps({'index': sequence}))


class DuckDBSocketTests(unittest.TestCase):
    def test_open_idle_tail_end_and_exact_request_response(self):
        seen = []
        async def execute(task, endpoint):
            seen.append(task.task.payload)
            return encode_full_response(FullModelResponse(429, (('retry-after', '7'),), b'\xff{"error":"native"}'))
        service = Service(execute)
        try:
            opened = service.open()
            self.assertEqual(opened['type'], 'opened')
            service.send('poll')
            self.assertEqual(service.read()['type'], 'idle')
            payload = '{ "messages": [], "model": "fixture", "logprobs": true, "max_tokens": 7 }'
            service.send('offer', tasks=[row(0, payload)])
            self.assertEqual(service.read()['accepted_prefix_count'], 1)
            self.assertEqual(seen, [])
            service.send('end')
            self.assertEqual(service.read()['type'], 'ended')
            service.send('poll')
            reply = service.read()
            self.assertEqual((reply['sequence'], reply['row_sequence'], reply['call_id'], reply['stage_id']),
                             ('0', '100', 'row-0', 'model'))
            self.assertEqual(reply['status_code'], 429)
            self.assertEqual(reply['headers'], [['retry-after', '7']])
            self.assertEqual(base64.b64decode(reply['body_base64']), b'\xff{"error":"native"}')
            self.assertEqual(seen, [payload.encode()])
            service.send('poll')
            self.assertEqual(service.read()['type'], 'finished')
        finally: service.close()
        self.assertEqual(service.settled, Usage())
        self.assertTrue(service.closed)

    def test_unordered_batch_is_returned_by_identity_before_slow_peer(self):
        release = threading.Event()
        async def execute(task, endpoint):
            if task.key.sequence == 0:
                while not release.is_set(): await asyncio.sleep(0.002)
            return encode_full_response(FullModelResponse(200, (), str(task.key.sequence).encode()))
        service = Service(execute)
        try:
            service.open()
            service.send('offer', tasks=[row(0), row(1)])
            self.assertEqual(service.read()['accepted_prefix_count'], 2)
            service.send('poll')
            fast = service.read()
            self.assertEqual(fast['sequence'], '1')
            self.assertEqual(base64.b64decode(fast['body_base64']), b'1')
            release.set()
            service.send('poll')
            self.assertEqual(service.read()['sequence'], '0')
            service.send('end'); service.read()
            service.send('poll')
            self.assertEqual(service.read()['type'], 'finished')
        finally:
            release.set(); service.close()
        self.assertEqual(service.settled, Usage())

    def test_cancel_while_polling_reports_unconfirmed_remote_then_reaps_late_result(self):
        started, release = threading.Event(), threading.Event()
        async def execute(task, endpoint):
            started.set()
            while not release.is_set(): await asyncio.sleep(0.002)
            return encode_full_response(FullModelResponse(200, (), b'late'))
        service = Service(execute)
        try:
            service.open();service.send('offer', tasks=[row(0)]);service.read()
            service.send('poll')
            self.assertTrue(started.wait(2))
            service.send('cancel')
            reply = service.read()
            self.assertEqual((reply['type'], reply['uncertain_requests']), ('cancelled', 1))
            self.assertTrue(service.returned.wait(2))
            self.assertEqual(service.at_disconnect.active_requests, 1)
        finally:
            release.set();service.close()
        self.assertEqual(service.settled, Usage())

    def test_disconnect_during_poll_does_not_release_model_capacity(self):
        started, release = threading.Event(), threading.Event()
        async def execute(task, endpoint):
            started.set()
            while not release.is_set(): await asyncio.sleep(0.002)
            return encode_full_response(FullModelResponse(200, (), b'late'))
        service = Service(execute)
        try:
            service.open();service.send('offer', tasks=[row(0)]);service.read();service.send('poll')
            self.assertTrue(started.wait(2))
            service.client.shutdown(socket.SHUT_RDWR)
            self.assertTrue(service.returned.wait(2))
            self.assertEqual(service.at_disconnect.active_requests, 1)
        finally:
            release.set();service.close()
        self.assertEqual(service.settled, Usage())

    def test_invalid_batch_never_sends_prefix_to_backend(self):
        seen = []
        async def execute(task, endpoint):
            seen.append(task.key)
            return encode_full_response(FullModelResponse(200, (), b'ok'))
        for rows in ([row(0),row(9)], [row(0),dict(row(1),row_sequence=True)], [row(i) for i in range(5)]):
            with self.subTest(rows=len(rows)):
                service = Service(execute)
                try:
                    service.open();service.send('offer', tasks=rows)
                    reply = service.read()
                    self.assertIn(reply['type'], ('accepted', 'error'))
                    if reply['type'] == 'accepted': self.assertEqual(reply['accepted_prefix_count'], 0)
                finally: service.close()
        self.assertEqual(seen, [])

    def test_duplicate_fields_and_partial_frames_are_rejected_without_task_dispatch(self):
        async def execute(task, endpoint): raise AssertionError('invalid frames cannot dispatch')
        for payload in (b'{"type":"open","protocol":"'+PROTOCOL.encode()+b'","query_id":"q","query_id":"r","operator_id":"map"}', b'{'):
            with self.subTest(payload=payload[:15]):
                service = Service(execute)
                try:
                    service.client.sendall(struct.pack('!I',len(payload))+payload)
                    self.assertEqual(service.read()['type'], 'error')
                finally: service.close()
        service = Service(execute)
        try:
            service.client.sendall(b'\x00')
            self.assertTrue(service.returned.wait(2))
        finally: service.close()
        self.assertEqual(service.settled, Usage())
