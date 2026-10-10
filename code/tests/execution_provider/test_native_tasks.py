"""Controlled native batches exercise real Core ownership with zero model calls."""

from dataclasses import replace
import asyncio
import json
import threading
import time
import unittest
from unittest import mock

from src.execution_provider.adapters.incremental_execution import IncrementalExecution
from src.execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task, build_native_execution
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters import native_tasks
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
    def test_query_job_survives_sequential_vector_sessions_with_one_grant(self):
        execution, backend, _ = fixture()
        with native_tasks.NativeQueryJob(execution, 'sql-query') as query:
            job = query.job
            sessions = []
            for vector in range(3):
                flow = query.open_session('duckdb-map')
                sessions.append(flow.session.session_id)
                self.assertIs(flow.job, job)
                self.assertEqual(flow.session.spec.job_id, job.job_id)
                flow.offer((task(0), task(1)))
                flow.end_input()
                flow.advance(2)
                for key in tuple(backend.pending):
                    backend.complete(key, encode_full_response(FullModelResponse(200, (), b'ok')))
                progress = flow.advance(2)
                flow.release(tuple(delivery.lease_id for delivery in progress.deliveries))
                self.assertEqual(flow.advance(2).state, State.FINISHED)
                flow.close(clean=True)
                self.assertIn(job, execution.engine.jobs.jobs)
                self.assertFalse(execution.engine.jobs.jobs[job].closing)
                self.assertEqual(execution.engine.capacity.usage(), Usage())
            self.assertEqual(len(set(sessions)), 3)
            self.assertEqual(execution.engine.jobs.sequence, 1)
        self.assertEqual(execution.engine.jobs.jobs, {})

    def test_query_cancel_keeps_late_remote_work_and_prevents_another_vector(self):
        execution, backend, _ = fixture()
        query = native_tasks.NativeQueryJob(execution, 'sql-query')
        flow = query.open_session('duckdb-map')
        flow.offer((task(0),))
        flow.advance(1)
        query.request_cancel()
        self.assertEqual(flow.advance(1).state, State.CANCELLED)
        with self.assertRaises(InterruptedError):
            query.open_session('another-vector')
        report = flow.close()
        query.close()
        self.assertEqual(report.uncertain_requests, 1)
        self.assertEqual(execution.engine.capacity.usage().active_requests, 1)
        self.assertTrue(execution.engine.jobs.jobs[query.job].closing)
        for key in tuple(backend.pending):
            backend.complete(key, encode_full_response(FullModelResponse(200, (), b'late')))
        execution.engine.advance()
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertEqual(execution.engine.jobs.jobs, {})

    def test_query_owner_keeps_original_error_when_job_close_fails(self):
        execution, _backend, _ = fixture()
        query = native_tasks.NativeQueryJob(execution, 'sql-query')
        primary = ValueError('fixture input failed')
        with mock.patch.object(execution.engine, 'close_job', side_effect=RuntimeError('fixture cleanup failed')):
            with self.assertRaises(ValueError) as observed:
                with query:
                    raise primary
        self.assertIs(observed.exception, primary)
        self.assertEqual(query.cleanup_errors, [dict(phase='close_job', type='RuntimeError')])
        self.assertIn('Native query Job cleanup also failed: RuntimeError', primary.__notes__)
        self.assertIn(query.job, execution.engine.jobs.jobs)
        query.close()
        self.assertEqual(execution.engine.jobs.jobs, {})

    def test_owned_job_close_is_attempted_after_consumer_close_fails(self):
        for job_failure in (None,RuntimeError('fixture Job close failed')):
            with self.subTest(job_failure=job_failure is not None):
                execution,_backend,_=fixture()
                flow=NativeTaskSession(execution,'query','map')
                primary=ValueError('fixture consumer close failed')
                original_close=execution.engine.close_job
                with mock.patch.object(flow.session,'close_consumer',side_effect=primary), \
                        mock.patch.object(execution.engine,'close_job',wraps=original_close,
                                          side_effect=job_failure) as close_job:
                    with self.assertRaises(ValueError) as observed:
                        flow.close()
                self.assertIs(observed.exception,primary)
                close_job.assert_called_once_with(flow.job)
                if job_failure is not None:
                    self.assertIn('Native owned Job cleanup also failed: RuntimeError',primary.__notes__)
                flow.close()
                self.assertFalse(execution.engine.jobs.jobs)

    def test_borrowed_session_cannot_join_a_job_from_another_execution(self):
        first, _backend, _ = fixture()
        second, _other, _ = fixture()
        with native_tasks.NativeQueryJob(first, 'query') as query:
            with self.assertRaisesRegex(ValueError, 'unregistered Job'):
                NativeTaskSession(second, 'query', 'map', job=query.job, limits=query.limits)
        self.assertFalse(first.engine.jobs.jobs)
        self.assertFalse(second.engine.jobs.jobs)

    def test_query_cancel_leaves_other_query_job_and_remote_work_independent(self):
        original,backend,clock=fixture(held_tasks=4,input_bytes=4096,result_bytes=2048,
                                      active_requests=4,active_work=4)
        execution=replace(original,engine=SessionEngine(original.engine.capacity.limits,backend,
            original.engine.policies,max_jobs=2,clock=clock))
        first=native_tasks.NativeQueryJob(execution,'first-query')
        with native_tasks.NativeQueryJob(execution,'other-query') as other:
            a=first.open_session('map');b=other.open_session('map')
            a.offer((task(0),));b.offer((task(0),));a.advance(2)
            first.request_cancel()
            self.assertEqual(a.advance(1).state,State.CANCELLED)
            a.close();first.close()
            self.assertFalse(execution.engine.jobs.jobs[other.job].closing)
            self.assertEqual(b.session.spec.job_id,other.job.job_id)
            b.end_input()
            key=TaskKey(b.session.session_id,0)
            backend.complete(key,encode_full_response(FullModelResponse(200,(),b'other-result')))
            delivery=b.advance(1).deliveries[0]
            self.assertEqual(decode_full_response(delivery.result).body,b'other-result')
            b.release((delivery.lease_id,));b.close(clean=True)
            self.assertIn(other.job,execution.engine.jobs.jobs)
            self.assertEqual(execution.engine.capacity.usage().active_requests,1)
        for key in tuple(backend.pending):
            backend.complete(key,encode_full_response(FullModelResponse(200,(),b'late-first-result')))
        execution.engine.advance()
        self.assertEqual(execution.engine.capacity.usage(),Usage())
        self.assertFalse(execution.engine.jobs.jobs)

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
