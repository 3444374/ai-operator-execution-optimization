"""Controlled native batches exercise real Core ownership with zero model calls."""

from dataclasses import replace
import asyncio
import json
import threading
import time
import unittest

from src.execution_provider.adapters.incremental_execution import IncrementalExecution
from src.execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task, build_native_execution
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.full_response import FullModelResponse, decode_full_response, encode_full_response
from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import State, TaskKey, Uncertain, Usage
from tests.scheduling.test_incremental_session import setup


def fixture(**changes):
    options = dict(held_tasks=2, input_bytes=2048, result_bytes=1024,
        item_input_bytes=1024, item_result_bytes=512, metadata_bytes=1024,
        active_requests=2, active_work=2)
    options.update(changes)
    original, _, backend, clock = setup(**options)
    engine = SessionEngine(original.capacity.limits, backend, original.policies, clock=clock)
    execution = IncrementalExecution(engine, None, lambda: True, 10)
    return execution, backend, clock


def task(sequence, **kwargs):
    return prepare_native_task(json.dumps({'model': 'fixture', 'messages': [], 'index': sequence}).encode(),
                               sequence, row_sequence=sequence + 10, call_id=f'call-{sequence}',
                               max_result_bytes=512, **kwargs)


class NativeTaskTests(unittest.TestCase):
    def test_batch_offer_dispatches_nothing_and_caller_retains_suffix(self):
        execution, backend, _ = fixture(held_tasks=3)
        flow = NativeTaskSession(execution, 'query', 'map-1')
        tasks = tuple(task(i) for i in range(3))
        offered = flow.offer(tasks)
        self.assertEqual((offered.status, offered.accepted_prefix_count), ('BACKPRESSURE', 2))
        self.assertEqual(backend.pending, {})
        self.assertEqual(execution.engine.capacity.usage().held_tasks, 2)
        self.assertEqual(flow.offer(tasks[2:]).accepted_prefix_count, 0)
        flow.advance(2)
        keys = tuple(backend.pending)
        self.assertEqual(len(keys), 2)
        self.assertTrue(all(backend.pending[key][1].task.payload == tasks[key.sequence].payload for key in keys))
        backend.complete(keys[1], encode_full_response(FullModelResponse(200, (), b'fast')))
        delivery = flow.advance(1).deliveries[0]
        self.assertEqual((delivery.key.sequence, delivery.info.row_sequence, delivery.info.call_id,
                          delivery.info.stage_id), (1, 11, 'call-1', 'model'))
        self.assertEqual(decode_full_response(delivery.result).body, b'fast')
        self.assertEqual(flow.offer(tasks[2:]).accepted_prefix_count, 0)
        flow.release((delivery.lease_id,))
        self.assertEqual(flow.offer(tasks[2:]).accepted_prefix_count, 1)
        flow.close()
        for key in tuple(backend.pending): backend.complete(key)
        execution.engine.advance()
        self.assertEqual(execution.engine.capacity.usage(), Usage())

    def test_invalid_suffix_rejects_entire_offer_and_preserves_sequence(self):
        execution, backend, _ = fixture()
        flow = NativeTaskSession(execution, 'query', 'map')
        self.assertEqual(flow.offer((task(0), replace(task(1), sequence=99))).accepted_prefix_count, 0)
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertEqual(flow.offer((task(0),)).accepted_prefix_count, 1)
        self.assertEqual(backend.pending, {})
        flow.close()

    def test_http_error_is_native_result_with_full_protocol_and_no_core_failure(self):
        execution, backend, _ = fixture()
        flow = NativeTaskSession(execution, 'query', 'map')
        flow.offer((task(0), task(1)))
        flow.end_input()
        flow.advance(2)
        body = b'{"error":{"message":"quota","type":"limit","code":"capacity"},"detail":123}'
        backend.complete(TaskKey(flow.session.session_id, 0),
                         encode_full_response(FullModelResponse(429, (('retry-after', '3'),), body)))
        error = flow.advance(1).deliveries[0]
        raw = decode_full_response(error.result)
        self.assertEqual((raw.status_code, raw.body, raw.headers), (429, body, (('retry-after', '3'),)))
        self.assertEqual(flow.session.error, None)
        flow.release((error.lease_id,))
        backend.complete(TaskKey(flow.session.session_id, 1), encode_full_response(FullModelResponse(200, (), b'ok')))
        success = flow.advance(1).deliveries[0]
        flow.release((success.lease_id,))
        self.assertEqual(flow.advance(1).state, State.FINISHED)
        flow.close(clean=True)
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertEqual(execution.engine.jobs.jobs, {})

    def test_cancel_preserves_remote_ownership_until_late_completion(self):
        execution, backend, _ = fixture()
        flow = NativeTaskSession(execution, 'query', 'map')
        flow.offer((task(0),))
        flow.advance(1)
        flow.request_cancel()
        self.assertEqual(flow.advance(1).state, State.CANCELLED)
        report = flow.close()
        self.assertEqual(report.uncertain_requests, 1)
        self.assertEqual(execution.engine.capacity.usage().active_requests, 1)
        key = next(iter(backend.pending))
        backend.complete(key, encode_full_response(FullModelResponse(200, (), b'late')))
        execution.engine.advance()
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertEqual(execution.engine.jobs.jobs, {})
        self.assertEqual(flow.close(), report)

    def test_transport_unknown_is_not_retried_or_released_by_local_close(self):
        execution, backend, _ = fixture()
        flow = NativeTaskSession(execution, 'query', 'map')
        flow.offer((task(0),))
        flow.advance(1)
        key = next(iter(backend.pending))
        handle = backend.pending[key][0]
        backend.events.append(Uncertain(key, handle))
        self.assertEqual(flow.advance(1).state, State.FAILED)
        report = flow.close()
        self.assertEqual(report.uncertain_requests, 1)
        for _ in range(4): execution.engine.advance()
        self.assertEqual(execution.engine.capacity.usage().active_requests, 1)
        self.assertEqual(len(backend.routes), 1)
        backend.complete(key, encode_full_response(FullModelResponse(200, (), b'late')))
        execution.engine.advance()
        self.assertEqual(execution.engine.capacity.usage(), Usage())

    def test_idle_is_valid_and_input_end_is_not_result_release(self):
        execution, backend, _ = fixture()
        flow = NativeTaskSession(execution, 'query', 'map')
        self.assertEqual(flow.advance(1).state, State.OPEN)
        flow.offer((task(0),))
        flow.end_input()
        self.assertEqual(flow.offer((task(1),)).accepted_prefix_count, 0)
        flow.advance(1)
        backend.complete(next(iter(backend.pending)), encode_full_response(FullModelResponse(200, (), b'ok')))
        delivery = flow.advance(1).deliveries[0]
        self.assertEqual(flow.advance(1).state, State.DRAINING)
        self.assertEqual(execution.engine.capacity.usage().result_bytes, 512)
        flow.release((delivery.lease_id,))
        self.assertEqual(flow.advance(1).state, State.FINISHED)
        flow.close(clean=True)

    def test_bulk_close_discards_consumer_results_without_retaining_history(self):
        execution, backend, _ = fixture()
        flow = NativeTaskSession(execution, 'query', 'map')
        flow.offer((task(0),))
        flow.advance(1)
        backend.complete(next(iter(backend.pending)), encode_full_response(FullModelResponse(200, (), b'ok')))
        self.assertEqual(len(flow.advance(1).deliveries), 1)
        flow.close()
        self.assertEqual(execution.engine.capacity.usage(), Usage())


class NativeServiceCloseTests(unittest.TestCase):
    def test_empty_release_keeps_backend_wait_generation_in_real_io_thread(self):
        release, started = threading.Event(), threading.Event()
        async def execute(request, endpoint):
            started.set()
            while not release.is_set(): await asyncio.sleep(0.002)
            return encode_full_response(FullModelResponse(200, (), b'late'))
        execution = build_native_execution(FixedModelConfig('http://localhost/fixture', 'fixture', 1000),
                                          physical=None, execute=execute, max_tasks=4)
        flow = NativeTaskSession(execution, 'query', 'map')
        try:
            self.assertEqual(flow.offer(tuple(task(i) for i in range(4))).accepted_prefix_count, 4)
            progress = flow.advance(4)
            self.assertTrue(started.wait(1))
            self.assertEqual(progress.deliveries, ())
            self.assertFalse(progress.has_immediate_work)
            self.assertEqual(progress.blocked_reason, 'WAIT_BACKEND')
            # Same feedback pattern as the LOTUS batch: release every returned tuple,
            # including no deliveries, then wait using this progress generation.
            flow.release(tuple(d.lease_id for d in progress.deliveries))
            self.assertEqual(execution.engine.wake.generation, progress.generation)
            before = time.monotonic()
            flow.wait(progress)
            self.assertGreaterEqual(time.monotonic() - before, 0.005)
            self.assertEqual(execution.engine.capacity.usage().active_requests, 4)
            release.set()
            completed = []
            deadline = time.monotonic() + 2
            while len(completed) != 4 and time.monotonic() < deadline:
                progress = flow.advance(4)
                completed.extend(progress.deliveries)
                flow.release(tuple(d.lease_id for d in progress.deliveries))
                flow.wait(progress)
            self.assertEqual(sorted(d.key.sequence for d in completed), [0, 1, 2, 3])
            self.assertEqual(execution.engine.capacity.usage(), Usage())
            flow.end_input()
            self.assertEqual(flow.advance(1).state, State.FINISHED)
            flow.close(clean=True)
            self.assertTrue(execution.close())
        finally:
            release.set()
            flow.close()
            deadline = time.monotonic() + 2
            while execution.engine.capacity.usage().held_tasks and time.monotonic() < deadline:
                execution.engine.advance()
                execution.engine.wake.wait(execution.engine.wake.generation, 0.005)
            execution.engine.backend.close()

    def test_service_close_waits_for_open_flow_and_late_remote_confirmation(self):
        release, started = threading.Event(), threading.Event()
        async def execute(request, endpoint):
            started.set()
            while not release.is_set(): await asyncio.sleep(0.002)
            return encode_full_response(FullModelResponse(200, (), b'late'))
        execution = build_native_execution(FixedModelConfig('http://localhost/fixture', 'fixture', 1000),
                                          physical=None, execute=execute, max_tasks=2)
        flow = NativeTaskSession(execution, 'query', 'map')
        try:
            self.assertFalse(execution.close())
            flow.offer((task(0),));flow.advance(1)
            self.assertTrue(started.wait(1))
            self.assertEqual(flow.close().uncertain_requests, 1)
            self.assertFalse(execution.close())
            release.set()
            deadline = time.monotonic() + 2
            while execution.engine.capacity.usage().held_tasks and time.monotonic() < deadline:
                execution.engine.advance()
                execution.engine.wake.wait(execution.engine.wake.generation, 0.005)
            self.assertEqual(execution.engine.capacity.usage(), Usage())
            self.assertTrue(execution.close())
        finally:
            release.set()
            execution.engine.backend.close()

    def test_completed_local_transport_error_does_not_claim_full_service_cleanup(self):
        attempts = []
        async def execute(request, endpoint):
            attempts.append(request.key)
            raise OSError('controlled unknown remote result')
        execution = build_native_execution(FixedModelConfig('http://localhost/fixture', 'fixture', 1000),
                                          physical=None, execute=execute, max_tasks=2)
        flow = NativeTaskSession(execution, 'query', 'map')
        try:
            flow.offer((task(0),))
            deadline = time.monotonic() + 2
            progress = flow.advance(1)
            while progress.state != State.FAILED and time.monotonic() < deadline:
                flow.wait(progress);progress = flow.advance(1)
            self.assertEqual(progress.state, State.FAILED)
            self.assertEqual(flow.close().uncertain_requests, 1)
            self.assertFalse(execution.close())
            self.assertEqual(execution.engine.capacity.usage().active_requests, 1)
            self.assertEqual(len(attempts), 1)
        finally:
            # End only this fixture's finished I/O thread; Core still reports unknown ownership.
            execution.engine.backend.close()
