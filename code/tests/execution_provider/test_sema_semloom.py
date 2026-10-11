"""Public Core/HTTP diagnostic substitution; actual Daft/Ray checks run separately."""

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock

from src.execution_provider.adapters.full_response import FullResponseTransport
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import build_native_execution, NativeTaskSession
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.adapters.sema_semloom import SemaSemLoomService
from src.execution_provider.adapters import sema_semloom
from src.execution_provider.adapters.sema_service import SemaServiceLimits, SemaServiceError, SemaProjection
from src.scheduling.core.session_contract import Usage

import test_sema_service as service_tests


class _FailingEvents(list):
    def __init__(self, values, event_name, error):
        super().__init__(values)
        self.event_name, self.error = event_name, error

    def append(self, event):
        if event.get('event') == self.event_name:
            raise self.error
        super().append(event)


def _public_core_diagnostic(service):
    class GuardedDiagnosticTransport(FullResponseTransport):
        async def execute(self, task, endpoint):
            try:
                service._before_request(task)
            except Exception:
                # Match the existing Ray hook's confirmed pre-send response.
                return b'{"bridge_error":"MODEL_UNAVAILABLE"}'
            return await super().execute(task, endpoint)

    with mock.patch('src.execution_provider.adapters.native_tasks.FullResponseTransport',
                    GuardedDiagnosticTransport):
        return build_native_execution(service.model_config, physical=None,
            max_tasks=service.max_held_tasks, max_active_requests=service.max_active_requests,
            observer=service._observe)


class SemaSemLoomDiagnosticTests(service_tests.SemaServiceTests):
    def setUp(self):
        super().setUp()
        patch = mock.patch.object(SemaSemLoomService, '_build_execution', _public_core_diagnostic)
        patch.start()
        self.addCleanup(patch.stop)
        self.physical = RayMapConfig('127.0.0.1:16379', 1, 1, 2**21, 2**23)

    def service(self, max_active_requests=1, **kwargs):
        return SemaSemLoomService(query_id='query-a', model_config=FixedModelConfig(self.url, 'fixture-model', 2000),
            physical=self.physical, limits=self.limits, trace_path=self.root / 'requests.jsonl',
            max_held_tasks=2, max_active_requests=max_active_requests, **kwargs)

    def test_complete_json_payload_and_equal_requests_stay_separate(self):
        # Include the explicitly supported non-streaming single-model schema.
        super().test_complete_json_payload_and_equal_requests_stay_separate()
        rows = [json.loads(line) for line in (self.root / 'requests.jsonl').read_text().splitlines()]
        self.assertEqual([r['core_task_sequence'] for r in rows[:2]], [0, 1])

    def test_complete_response_decoded_once_without_a_forwarding_client(self):
        with mock.patch('aiohttp.ClientSession',
                        side_effect=AssertionError('SemLoom must use its execution transport')), \
                mock.patch.object(sema_semloom, 'decode_full_response',
                                  wraps=sema_semloom.decode_full_response) as decode:
            with self.service() as service:
                status, body, headers = self.post(service.endpoint_url)
                self.assertEqual((status, body), (200, self.server.body))
                self.assertEqual(headers['X-Sema-Fixture'], 'preserve')
                service.end_input()
        self.assertEqual(decode.call_count, 1)
        self.assertEqual(len(self.server.calls), 1)
        self.assertEqual(service._pending, {})
        self.assertEqual(service._execution.engine.capacity.records, {})
        self.assertEqual(service.cleanup_errors, [])

    def test_default_pump_advances_on_the_owner_without_a_wait_executor(self):
        core_threads = set()

        def build(service):
            execution = _public_core_diagnostic(service)
            original_operation = execution.engine._operation

            def operation():
                core_threads.add(threading.get_ident())
                return original_operation()

            execution.engine._operation = operation
            return execution

        with mock.patch.object(SemaSemLoomService, '_build_execution', build), \
                mock.patch.object(NativeTaskSession, 'wait', wraps=NativeTaskSession.wait) as wait:
            with self.service() as service:
                self.assertEqual(self.post(service.endpoint_url)[0], 200)
                service.end_input()
        wait.assert_not_called()
        self.assertEqual(core_threads, {service._thread.ident})
        self.assertEqual(service._execution.engine.capacity.records, {})
        self.assertEqual(service.cleanup_errors, [])

    def test_owner_observation_is_deferred_to_the_next_loop_turn(self):
        with self.service() as service:
            async def observe():
                service._observe(dict(event='fixture-owner-observation'))
                self.assertNotIn('fixture-owner-observation',
                                 [event['event'] for event in service.summary['core_events']])
                await asyncio.sleep(0)
                self.assertIn('fixture-owner-observation',
                              [event['event'] for event in service.summary['core_events']])

            asyncio.run_coroutine_threadsafe(observe(), service._loop).result(1)
            service.end_input()

    def test_foreign_thread_observation_returns_to_the_http_owner(self):
        with self.service() as service:
            recorded = []
            original_record = service._record_event

            def record(event, when):
                if event['event'] == 'fixture-foreign-observation':
                    recorded.append(threading.get_ident())
                original_record(event, when)

            async def barrier():
                return None

            with mock.patch.object(service, '_record_event', record):
                service._observe(dict(event='fixture-foreign-observation'))
                asyncio.run_coroutine_threadsafe(barrier(), service._loop).result(1)
            self.assertEqual(recorded, [service._thread.ident])
            service.end_input()

    def test_late_deferred_record_error_fails_query_even_after_http_and_projection_success(self):
        primary = ValueError('controlled terminal recording failure')
        service = self.service()
        held = []
        original_observe = SemaSemLoomService._observe

        def hold_terminal(current, event):
            if event.get('event') == 'terminal':
                held.append(dict(event))
            else:
                original_observe(current, event)

        class Native:
            def execute(native):
                self.assertEqual(self.post(service.endpoint_url)[0], 200)
                yield 'row', 'answer'
            def cancel(native):
                pass

        loop_errors = []
        with mock.patch.object(SemaSemLoomService, '_observe', hold_terminal):
            with self.assertRaises(SemaServiceError) as caught:
                with service:
                    query = SemaProjection(Native(), service, time.monotonic_ns())
                    self.assertEqual(list(query.execute()), [('row', 'answer')])
                    self.assertEqual(query.status, 'completed')
                    self.assertEqual(len(held), 1)

                    async def deliver_late():
                        service._loop.set_exception_handler(lambda loop, context: loop_errors.append(context))
                        service._core_events = _FailingEvents(service._core_events, 'terminal', primary)
                        original_observe(service, held[0])
                        await asyncio.sleep(0)

                    asyncio.run_coroutine_threadsafe(deliver_late(), service._loop).result(1)
        self.assertIs(caught.exception, service.first_error)
        self.assertIs(caught.exception.__cause__, primary)
        self.assertEqual(service.first_error.phase, 'observation_record')
        self.assertEqual(service.first_error.sequence, 0)
        self.assertEqual(loop_errors, [])
        self.assertEqual(len(self.server.calls), 1)
        self.assertEqual(service._execution.engine.capacity.usage(), Usage())
        self.assertEqual(service._execution.engine.jobs.jobs, {})
        self.assertEqual(service.cleanup_errors, [])

    def test_observation_error_remains_primary_when_native_cancel_and_cleanup_fail(self):
        primary, cancellation, cleanup = ValueError('recording failed'), OSError('native cancel failed'), RuntimeError('executor cleanup failed')
        service = self.service()

        def cancel_native():
            if service.first_error is not None:
                self.assertIs(service.first_error.__cause__, primary)
            raise cancellation

        original_close = service._close_executor
        async def failed_close():
            await original_close()
            raise cleanup

        with self.assertRaises(SemaServiceError) as caught:
            with mock.patch.object(service, '_close_executor', failed_close):
                with service:
                    service.bind_native_cancel(cancel_native)
                    async def fail_records():
                        service._core_events = _FailingEvents(service._core_events, 'terminal', primary)
                    asyncio.run_coroutine_threadsafe(fail_records(), service._loop).result(1)
                    self.post(service.endpoint_url)
                    service.end_input()
        self.assertIs(caught.exception, service.first_error)
        self.assertIs(caught.exception.__cause__, primary)
        stages = [failure['stage'] for failure in service.summary['cleanup_failures']]
        self.assertIn('native_cancel', stages)
        self.assertIn('executor_close', stages)
        self.assertEqual(service._execution.engine.capacity.usage(), Usage())
        self.assertEqual(service._execution.engine.jobs.jobs, {})

    def test_existing_http_error_is_not_replaced_by_later_record_error(self):
        self.server.status = 503
        primary = ValueError('secondary recording failure')
        with self.service() as service:
            async def fail_records():
                service._core_events = _FailingEvents(service._core_events, 'terminal', primary)
            asyncio.run_coroutine_threadsafe(fail_records(), service._loop).result(1)
            self.assertEqual(self.post(service.endpoint_url)[0], 503)
            first = service.first_error
        self.assertIs(service.first_error, first)
        self.assertEqual(first.phase, 'http_response')
        self.assertEqual(first.status, 503)
        self.assertIn('observation_record', [failure['stage'] for failure in service.summary['cleanup_failures']])
        self.assertIs(service._cleanup_error, primary)

    def test_missing_ray_record_field_cancels_without_releasing_unconfirmed_work(self):
        self.server.gate = threading.Event()
        service = self.service()
        with self.assertRaises(SemaServiceError) as caught:
            with service:
                with ThreadPoolExecutor(max_workers=1) as callers:
                    future = callers.submit(self.post, service.endpoint_url)
                    try:
                        self.assertTrue(self.server.received.wait(1))
                        async def malformed_record():
                            service._observe(dict(event='ray_http_completed',
                                key=dict(session_id=service._session.session.session_id, sequence=0),
                                rpc_started_ns=time.monotonic_ns(), shared_clock=True))
                            await asyncio.sleep(0)
                        asyncio.run_coroutine_threadsafe(malformed_record(), service._loop).result(1)
                        self.assertEqual(future.result(timeout=1)[0], 502)
                        async def retained():
                            self.assertEqual(service._execution.engine.capacity.usage().active_requests, 1)
                        asyncio.run_coroutine_threadsafe(retained(), service._loop).result(1)
                        self.assertEqual(self.post(service.endpoint_url)[0], 410)
                    finally:
                        self.server.gate.set()
        self.assertIs(caught.exception, service.first_error)
        self.assertIsInstance(caught.exception.__cause__, KeyError)
        self.assertEqual(service.first_error.phase, 'observation_record')
        self.assertEqual(len(self.server.calls), 1)
        self.assertEqual(service._execution.engine.capacity.usage(), Usage())
        self.assertEqual(service._execution.engine.jobs.jobs, {})

    def test_record_failure_during_slow_http_write_keeps_result_lease(self):
        from aiohttp import web
        primary = ValueError('recording failed during HTTP write')
        started_write = threading.Event()
        finish_write = asyncio.Event()
        original_write = web.Response.write_eof
        async def write(response, *args, **kwargs):
            if response.body == self.server.body:
                started_write.set()
                await finish_write.wait()
            return await original_write(response, *args, **kwargs)

        service = self.service()
        with mock.patch.object(web.Response, 'write_eof', write):
            with self.assertRaises(SemaServiceError) as caught:
                with service:
                    with ThreadPoolExecutor(max_workers=1) as callers:
                        future = callers.submit(self.post, service.endpoint_url)
                        try:
                            self.assertTrue(started_write.wait(1))
                            async def fail_and_inspect():
                                terminal = next(event for event in service._core_events if event['event'] == 'terminal')
                                service._core_events = _FailingEvents(service._core_events, 'terminal', primary)
                                service._observe(terminal)
                                await asyncio.sleep(.02)
                                self.assertEqual(next(iter(service._execution.engine.capacity.records.values())).phase, 'LEASED')
                                self.assertEqual(len(service._pending), 1)
                                self.assertIsNone(service._rows[0]['response_written_ns'])
                            asyncio.run_coroutine_threadsafe(fail_and_inspect(), service._loop).result(1)
                        finally:
                            service._loop.call_soon_threadsafe(finish_write.set)
                        self.assertEqual(future.result(timeout=1)[0], 200)
        self.assertIs(caught.exception.__cause__, primary)
        self.assertIsNotNone(service._rows[0]['response_written_ns'])
        self.assertEqual(service._execution.engine.capacity.usage(), Usage())
        self.assertEqual(service._execution.engine.jobs.jobs, {})
        self.assertEqual(service._pending, {})


    def test_delivery_lease_stays_held_until_complete_http_write(self):
        from aiohttp import web
        started_write = threading.Event()
        finish_write = asyncio.Event()
        original_write = web.Response.write_eof

        async def write(response, *args, **kwargs):
            if response.body == self.server.body:
                started_write.set()
                await finish_write.wait()
            return await original_write(response, *args, **kwargs)

        with mock.patch.object(web.Response, 'write_eof', write):
            with self.service() as service:
                with ThreadPoolExecutor(max_workers=1) as callers:
                    future = callers.submit(self.post, service.endpoint_url)
                    try:
                        self.assertTrue(started_write.wait(1))

                        async def inspect_lease():
                            row = service._rows[0]
                            clocks = [row[key] for key in ('core_accepted_ns', 'core_delivery_ns',
                                      'response_ready_ns', 'forward_resumed_ns')]
                            self.assertTrue(all(clock is not None for clock in clocks))
                            self.assertEqual(clocks, sorted(clocks))
                            self.assertIsNone(row['response_written_ns'])
                            record = next(iter(service._execution.engine.capacity.records.values()))
                            self.assertEqual(record.phase, 'LEASED')
                            self.assertEqual(len(service._pending), 1)

                        asyncio.run_coroutine_threadsafe(inspect_lease(), service._loop).result(1)
                    finally:
                        service._loop.call_soon_threadsafe(finish_write.set)
                    self.assertEqual(future.result(timeout=1)[:2], (200, self.server.body))
                service.end_input()
        row = service._rows[0]
        self.assertLessEqual(row['forward_resumed_ns'], row['response_written_ns'])
        self.assertEqual(service._pending, {})
        self.assertEqual(service._execution.engine.capacity.records, {})
        self.assertEqual(service.cleanup_errors, [])

    def test_capacity_waiting_uses_public_session_and_returns_all_results(self):
        self.server.gate = threading.Event()
        with self.service() as service:
            with ThreadPoolExecutor(max_workers=3) as callers:
                futures = [callers.submit(self.post, service.endpoint_url) for _ in range(3)]
                self.assertTrue(self.server.received.wait(1))
                time.sleep(0.05)
                self.assertEqual(len(self.server.calls), 1)
                self.server.gate.set()
                self.assertEqual([f.result()[0] for f in futures], [200, 200, 200])
            service.end_input()
        self.assertEqual(service.summary['forwarded_posts'], 3)
        self.assertEqual(service.cleanup_errors, [])
        self.assertEqual(service._execution.engine.capacity.records, {})
        events = [e['event'] for e in service.summary['core_events']]
        self.assertIn('submitted', events)
        self.assertIn('terminal', events)

    def test_waiting_http_callers_do_not_reprepare_while_model_capacity_is_held(self):
        self.server.gate = threading.Event()
        service = SemaSemLoomService(query_id='query-a',
            model_config=FixedModelConfig(self.url, 'fixture-model', 2000),
            physical=self.physical, limits=self.limits,
            trace_path=self.root / 'requests.jsonl', max_held_tasks=4, max_active_requests=4)
        with mock.patch.object(sema_semloom, 'prepare_native_task',
                               wraps=sema_semloom.prepare_native_task) as prepare, \
                mock.patch.object(NativeTaskSession, 'offer', autospec=True,
                                  side_effect=NativeTaskSession.offer) as offer:
            with service:
                with ThreadPoolExecutor(max_workers=8) as callers:
                    futures = [callers.submit(self.post, service.endpoint_url) for _ in range(8)]
                    try:
                        deadline = time.monotonic() + 1.5
                        while (len(self.server.calls) != 4 or
                               sum(row['body_read_ns'] is not None for row in service._rows) != 8):
                            if time.monotonic() > deadline:
                                self.fail('fixture did not hold four model calls with four HTTP callers waiting')
                            time.sleep(0.005)
                        before = prepare.call_count
                        before_offers = offer.call_count
                        time.sleep(0.1)
                        self.assertEqual(prepare.call_count, before,
                            'unchanged held capacity must not repeatedly prepare pending HTTP bodies')
                        self.assertEqual(offer.call_count, before_offers,
                            'unchanged held capacity must not repeatedly validate pending offers')
                    finally:
                        self.server.gate.set()
                    self.assertEqual([f.result()[0] for f in futures], [200] * 8)
                service.end_input()
            self.assertEqual(prepare.call_count, 8, 'each complete HTTP body is prepared once')
        self.assertEqual(service._execution.engine.capacity.records, {})

    def test_one_capacity_waiter_offers_after_release_without_broadcast_rechecks(self):
        self.server.gate = threading.Event()
        offered_rows = []
        original_offer = NativeTaskSession.offer

        def offer(session, tasks):
            result = original_offer(session, tasks)
            offered_rows.append((tasks[0].info.row_sequence, result.accepted_prefix_count))
            return result

        with mock.patch.object(NativeTaskSession, 'offer', offer):
            with self.service() as service:
                with ThreadPoolExecutor(max_workers=8) as callers:
                    futures = [callers.submit(self.post, service.endpoint_url) for _ in range(8)]
                    try:
                        deadline = time.monotonic() + 1.5
                        while sum(row['body_read_ns'] is not None for row in service._rows) != 8:
                            if time.monotonic() > deadline:
                                self.fail('fixture did not read all eight waiting HTTP bodies')
                            time.sleep(0.005)
                        waiting = {row for row, accepted in offered_rows if not accepted}
                        self.assertEqual(len(waiting), 1,
                            'one full Core should retain one offering capacity waiter')
                    finally:
                        self.server.gate.set()
                    self.assertEqual([f.result()[0] for f in futures], [200] * 8)
                service.end_input()
        self.assertEqual(sum(accepted for row, accepted in offered_rows), 8)
        self.assertEqual(len(self.server.calls), 8)
        self.assertEqual(service._pending, {})
        self.assertEqual(service._execution.engine.capacity.records, {})
        self.assertEqual(service.cleanup_errors, [])

    def test_admission_wait_deadline_does_not_accept_or_send_a_late_body(self):
        with self.service() as service:
            async def hold_admission():
                # A stalled owner operation cannot extend a waiting body's deadline.
                await service._admission_lock.acquire()
                row = dict(request_sequence=0)
                try:
                    with mock.patch.object(service, 'limits', replace(service.limits, timeout_s=.02)):
                        with self.assertRaises(TimeoutError):
                            await service._forward(self.payload, {}, row, None)
                    self.assertNotIn('core_task_sequence', row)
                    self.assertTrue(service._admission_lock.locked())
                finally:
                    service._admission_lock.release()
            asyncio.run_coroutine_threadsafe(hold_admission(), service._loop).result(1)
            service.end_input()
        self.assertEqual(service.summary['forwarded_posts'], 0)
        self.assertEqual(self.server.calls, [])
        self.assertEqual(service._execution.engine.capacity.records, {})

    def test_first_http_error_suppresses_other_accepted_and_unaccepted_requests(self):
        self.server.status = 503
        self.server.gate = threading.Event()
        with self.service() as service:
            with ThreadPoolExecutor(max_workers=3) as callers:
                futures = [callers.submit(self.post, service.endpoint_url) for _ in range(3)]
                self.assertTrue(self.server.received.wait(1))
                self.server.gate.set()
                results = [f.result() for f in futures]
            self.assertIn((503, self.server.body), [r[:2] for r in results])
            self.assertEqual(service.first_error.status, 503)
            self.assertEqual(self.post(service.endpoint_url)[0], 410)
        self.assertEqual(len(self.server.calls), 1)
        self.assertEqual(service.summary['forwarded_posts'], 1)
        self.assertEqual(service._execution.engine.capacity.records, {})

    def test_cancellation_with_remote_work_keeps_result_ownership_until_settled(self):
        self.server.gate = threading.Event()
        with self.service() as service:
            with ThreadPoolExecutor(max_workers=1) as callers:
                future = callers.submit(self.post, service.endpoint_url)
                self.assertTrue(self.server.received.wait(1))
                service.cancel()
                self.assertEqual(self.post(service.endpoint_url)[0], 410)
                self.server.gate.set()
                self.assertIn(future.result()[0], (200, 502))
        self.assertEqual(len(self.server.calls), 1)
        self.assertEqual(service._execution.engine.capacity.records, {})
        self.assertEqual(service.cleanup_errors, [])

    def test_cancellation_wakes_http_callers_waiting_for_held_storage(self):
        self.server.gate = threading.Event()
        service = SemaSemLoomService(query_id='query-a',
            model_config=FixedModelConfig(self.url, 'fixture-model', 2000),
            physical=self.physical, limits=self.limits,
            trace_path=self.root / 'requests.jsonl', max_held_tasks=4, max_active_requests=4)
        with service:
            with ThreadPoolExecutor(max_workers=8) as callers:
                futures = [callers.submit(self.post, service.endpoint_url) for _ in range(8)]
                deadline = time.monotonic() + 1.5
                try:
                    while len(self.server.calls) != 4 or service.summary['received_requests'] != 8:
                        if time.monotonic() > deadline:
                            self.fail('fixture did not reach held storage before cancellation')
                        time.sleep(0.005)
                    service.cancel()
                finally:
                    self.server.gate.set()
                self.assertTrue(all(f.result(timeout=2)[0] in (200, 502) for f in futures))
                self.assertEqual(self.post(service.endpoint_url)[0], 410)
        self.assertEqual(len(self.server.calls), 4)
        self.assertEqual(service._execution.engine.capacity.records, {})
        self.assertEqual(service.cleanup_errors, [])

    def test_late_results_wake_waiting_callers_after_their_consumers_leave(self):
        self.server.gate = threading.Event()
        service = SemaSemLoomService(query_id='query-a',
            model_config=FixedModelConfig(self.url, 'fixture-model', 2000),
            physical=self.physical, limits=self.limits,
            trace_path=self.root / 'requests.jsonl', max_held_tasks=4, max_active_requests=4)
        rows = [{'request_sequence': i} for i in range(8)]

        async def forward(row):
            try:
                return await service._forward(self.payload, {}, row, None)
            finally:
                service._release_response(row)

        with service:
            futures = [asyncio.run_coroutine_threadsafe(forward(row), service._loop) for row in rows]
            try:
                deadline = time.monotonic() + 1.5
                while len(self.server.calls) != 4:
                    if time.monotonic() > deadline:
                        self.fail('fixture did not hold all four accepted requests')
                    time.sleep(0.005)
                accepted = [i for i, row in enumerate(rows) if 'core_task_sequence' in row]
                self.assertEqual(len(accepted), 4)
                for i in accepted:
                    futures[i].cancel()
                while service._pending:
                    if time.monotonic() > deadline:
                        self.fail('departed consumers kept local response futures')
                    time.sleep(0.005)
                # Only late-result disposal can restore capacity: none of these
                # four consumers will write a response or release a delivered lease.
                self.server.gate.set()
                remaining = [f for i, f in enumerate(futures) if i not in accepted]
                self.assertEqual([f.result(timeout=1).status for f in remaining], [200] * 4)
                service.end_input()
            finally:
                self.server.gate.set()
                for future in futures:
                    future.cancel()
        self.assertEqual(len(self.server.calls), 8)
        self.assertEqual(service._execution.engine.capacity.records, {})
        self.assertEqual(service.cleanup_errors, [])

    def test_streaming_or_wrong_model_is_rejected_before_model_post(self):
        with self.service() as service:
            body = json.dumps({'model': 'fixture-model', 'stream': True, 'messages': []}).encode()
            self.assertEqual(self.post(service.endpoint_url, body)[0], 502)
            self.assertEqual(service.summary['forwarded_posts'], 0)
        self.assertEqual(self.server.calls, [])
        row = service._rows[0]
        self.assertTrue(all(row[key] is None for key in
                            ('core_delivery_ns', 'response_ready_ns', 'forward_resumed_ns')))

    def test_out_of_order_complete_responses_keep_their_http_caller(self):
        def reply_for(body):
            tag = json.loads(body)['messages'][0]['content']
            return 200, json.dumps({'native_value': tag}).encode(), 0.06 if tag == 'first' else 0
        self.server.reply_for = reply_for
        with self.service(max_active_requests=2) as service:
            with ThreadPoolExecutor(max_workers=2) as callers:
                bodies = [self.payload.replace(b'same', tag.encode()) for tag in ('first', 'second')]
                futures = [callers.submit(self.post, service.endpoint_url, bodies[0])]
                self.assertTrue(self.server.received.wait(1))
                futures.append(callers.submit(self.post, service.endpoint_url, bodies[1]))
                completed = [json.loads(f.result()[1])['native_value'] for f in as_completed(futures)]
                self.assertEqual(completed, ['second', 'first'])
                self.assertEqual([json.loads(f.result()[1])['native_value'] for f in futures], ['first', 'second'])
            service.end_input()
        self.assertEqual(Counter(r[1] for r in self.server.calls), Counter(bodies))
        self.assertEqual(service._execution.engine.capacity.records, {})

    def test_main_path_requires_ray_config_and_allows_both_payload_backends(self):
        config = FixedModelConfig(self.url, 'fixture-model', 2000)
        with self.assertRaisesRegex(ValueError, 'RayMapConfig'):
            SemaSemLoomService(query_id='q', model_config=config, physical=None,
                limits=self.limits, trace_path=self.root / 'invalid.jsonl', max_held_tasks=2, max_active_requests=1)
        for backend in ('daft', 'arrow'):
            with self.subTest(backend=backend):
                physical = replace(self.physical, payload_backend=backend)
                owner = sema_semloom.SemaSemLoomExecutor(model_config=config, physical=physical,
                    max_held_tasks=2, max_active_requests=1)
                service = SemaSemLoomService(query_id='q', model_config=config, physical=physical,
                    limits=self.limits, trace_path=self.root / 'selected.jsonl',
                    max_held_tasks=2, max_active_requests=1, executor_owner=owner)
                self.assertEqual(service.summary['payload_backend'], backend)
                self.assertEqual(owner.physical.payload_backend, backend)


class SemaResidentExecutorTests(unittest.TestCase):
    def setUp(self):
        service_tests.SemaServiceTests.setUp(self)
        self.physical = RayMapConfig('127.0.0.1:16379', 1, 1, 2**21, 2**23)
    stop_model = service_tests.SemaServiceTests.stop_model
    post = service_tests.SemaServiceTests.post

    def owner(self):
        return sema_semloom.SemaSemLoomExecutor(
            model_config=FixedModelConfig(self.url, 'fixture-model', 2000),
            physical=self.physical, max_held_tasks=4, max_active_requests=2)

    def query_service(self, owner, name):
        return SemaSemLoomService(query_id=name, model_config=owner.model_config, physical=self.physical,
            limits=self.limits, trace_path=self.root / (name + '.jsonl'),
            max_held_tasks=4, max_active_requests=2, executor_owner=owner)

    def test_two_query_sessions_reuse_one_executor_and_keep_hooks_and_results_separate(self):
        builds = []

        def build(owner):
            builds.append(threading.get_ident())
            return _public_core_diagnostic(owner)

        with mock.patch.object(sema_semloom.SemaSemLoomExecutor, '_build_execution', build):
            with sema_semloom.SemaSemLoomExecutor(
                    model_config=FixedModelConfig(self.url, 'fixture-model', 2000),
                    physical=self.physical, max_held_tasks=4, max_active_requests=2) as owner:
                identities, charged, session_ids = [], [], []
                for number, count in enumerate((2, 3)):
                    calls = []
                    service = SemaSemLoomService(query_id='query-' + str(number),
                        model_config=owner.model_config, physical=self.physical,
                        limits=self.limits, trace_path=self.root / ('q' + str(number) + '.jsonl'),
                        max_held_tasks=4, max_active_requests=2, before_post=calls.append,
                        executor_owner=owner)
                    with service:
                        identities.append(id(service._execution))
                        session_ids.append(service._session.session.session_id)
                        if number:
                            owner._observe(dict(event='ray_http_completed',
                                                key=dict(session_id=session_ids[0], sequence=0)))
                        for i in range(count):
                            body = self.payload.replace(b'same', ('q' + str(number) + '-' + str(i)).encode())
                            self.assertEqual(self.post(service.endpoint_url, body)[0], 200)
                        service.end_input()
                    charged.append(calls)
                    self.assertEqual(service.summary['forwarded_posts'], count)
                    self.assertEqual(service.cleanup_errors, [])
                    self.assertTrue(owner._thread.is_alive())
                    self.assertEqual(owner.snapshot()['core_jobs'], 0)
                    self.assertFalse(owner.execution.engine.capacity.records)
                    rows = [json.loads(line) for line in service.trace_path.read_text().splitlines()]
                    self.assertEqual({row['query_id'] for row in rows}, {service.query_id})
                    self.assertEqual([row['core_task_sequence'] for row in rows], list(range(count)))
                    self.assertIn('submitted', [event['event'] for event in service.summary['core_events']])
                self.assertEqual(len(set(identities)), 1)
                self.assertEqual(len(builds), 1)
                self.assertEqual(len(set(session_ids)), 2)
                self.assertEqual([len(calls) for calls in charged], [2, 3])
        self.assertFalse(owner._thread.is_alive())
        self.assertTrue(owner._loop.is_closed())
        self.assertEqual(len(self.server.calls), 5)

    def test_cancelled_query_drains_before_owner_rejects_another_query(self):
        self.server.gate = threading.Event()
        with mock.patch.object(sema_semloom.SemaSemLoomExecutor, '_build_execution', _public_core_diagnostic):
            with self.owner() as owner:
                with self.query_service(owner, 'cancelled') as service:
                    with ThreadPoolExecutor(max_workers=1) as callers:
                        future = callers.submit(self.post, service.endpoint_url)
                        self.assertTrue(self.server.received.wait(1))
                        service.cancel()
                        self.server.gate.set()
                        self.assertEqual(future.result()[0], 502)
                self.assertTrue(owner.poisoned)
                self.assertEqual(owner.snapshot()['core_jobs'], 0)
                self.assertFalse(owner.execution.engine.capacity.records)
                with self.assertRaisesRegex(RuntimeError, 'startup failed'):
                    with self.query_service(owner, 'later'):
                        self.fail('cancelled owner accepted a new query')
        self.assertEqual(len(self.server.calls), 1)
        self.assertFalse(owner._thread.is_alive())

    def test_record_failure_after_success_poisoned_owner_rejects_next_query(self):
        primary = ValueError('resident recording failed')
        with mock.patch.object(sema_semloom.SemaSemLoomExecutor, '_build_execution', _public_core_diagnostic):
            with self.owner() as owner:
                with self.assertRaises(SemaServiceError) as caught:
                    with self.query_service(owner, 'failed-observation') as service:
                        self.assertEqual(self.post(service.endpoint_url)[0], 200)
                        async def fail_record():
                            terminal = next(event for event in service._core_events if event['event'] == 'terminal')
                            service._core_events = _FailingEvents(service._core_events, 'terminal', primary)
                            service._observe(terminal)
                            await asyncio.sleep(0)
                        asyncio.run_coroutine_threadsafe(fail_record(), service._loop).result(1)
                        service.end_input()
                self.assertIs(caught.exception.__cause__, primary)
                self.assertTrue(owner.poisoned)
                self.assertEqual(owner.snapshot()['core_jobs'], 0)
                with self.assertRaisesRegex(RuntimeError, 'startup failed'):
                    with self.query_service(owner, 'later'):
                        self.fail('owner accepted another query after recording failed')
        self.assertEqual(len(self.server.calls), 1)
        self.assertFalse(owner._thread.is_alive())

    def test_owner_close_failure_retains_executor_and_keeps_original_error(self):
        with mock.patch.object(sema_semloom.SemaSemLoomExecutor, '_build_execution', _public_core_diagnostic):
            owner = self.owner().__enter__()
            original = owner.execution
            try:
                with self.query_service(owner, 'completed') as service:
                    self.assertEqual(self.post(service.endpoint_url)[0], 200)
                    service.end_input()
                owner.execution = replace(original, close=lambda _timeout: False)
                primary = ValueError('fixture query error')
                owner.__exit__(ValueError, primary, None)
                self.assertTrue(owner._thread.is_alive())
                self.assertTrue(owner.poisoned)
                self.assertIn('Sema executor owner cleanup also failed: RuntimeError', primary.__notes__)
            finally:
                owner.execution = original
                owner.__exit__(None, None, None)
        self.assertFalse(owner._thread.is_alive())

    def test_startup_failure_does_not_leave_an_owner_control_thread(self):
        failure = ValueError('fixture executor startup')
        with mock.patch.object(sema_semloom.SemaSemLoomExecutor, '_build_execution', side_effect=failure):
            owner = self.owner()
            with self.assertRaisesRegex(RuntimeError, 'startup did not settle') as observed:
                owner.__enter__()
            self.assertIs(observed.exception.__cause__, failure)
            owner._thread.join(1)
            self.assertFalse(owner._thread.is_alive())
            self.assertTrue(owner._loop.is_closed())
        self.assertFalse(self.server.calls)

    def test_late_startup_after_timeout_closes_its_idle_executor(self):
        def build(owner):
            time.sleep(.03)
            return _public_core_diagnostic(owner)
        owner = self.owner()
        with mock.patch.object(sema_semloom.SemaSemLoomExecutor, '_build_execution', build), \
                mock.patch.object(owner._ready, 'wait', return_value=False):
            with self.assertRaisesRegex(RuntimeError, 'startup did not settle'):
                owner.__enter__()
        self.assertTrue(owner._execution_closed)
        self.assertFalse(owner._thread.is_alive())
        self.assertTrue(owner._loop.is_closed())
        self.assertFalse(self.server.calls)


if __name__ == '__main__':
    unittest.main()
