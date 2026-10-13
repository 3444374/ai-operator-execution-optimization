"""DuckDB SemLoom handoff and real Core ownership; no model or HTTP calls."""
from __future__ import annotations

import ctypes as ct
from dataclasses import replace
import json
import threading
import unittest
from unittest.mock import patch

from src.semantic_methods import duckdb_ai
from src.semantic_methods.duckdb_ai import (
    DuckDBCall, DuckDBResponse, DuckDBSemLoomBridge, DuckDBNativeTaskExecutor,
    _Call, _Response, _Cancelled, _Consume,
)


def raw_calls(count=2, *, payload=b'same', headers=()):
    owners = []
    calls = (_Call * count)()
    for index in range(count):
        body = ct.create_string_buffer(payload)
        header_array = (ct.c_char_p * len(headers))(*headers)
        owners.extend((body, header_array))
        calls[index] = _Call(7, b'query', f'call-{index}'.encode(), b'fixture',
                             b'http://localhost/model', ct.addressof(body), len(payload),
                             header_array, len(headers), 1, 5, 5, 1)
    return calls, owners


def bridge_for(execute):
    bridge = object.__new__(DuckDBSemLoomBridge)
    bridge.execute = execute
    bridge.collect_timings = False
    bridge.last_error = bridge.last_cleanup_error = bridge.last_timings = None
    bridge._buffers, bridge.batch_sizes, bridge._lock = {}, [], threading.Lock()
    return bridge


def dispatch(bridge, calls, *, count=None, consume=lambda *_: 0, cancelled=lambda: False, batch_id=1):
    count = len(calls) if count is None else count
    outputs = (_Response * count)()
    status = bridge._dispatch(batch_id, calls, count, outputs,
                              _Cancelled(lambda _: int(cancelled())), _Consume(consume), None)
    return outputs, status


class DuckDBSemLoomHandoffTests(unittest.TestCase):
    def test_row_limit_is_checked_before_reading_or_executing_the_batch(self):
        executed = []
        calls, owners = raw_calls(2049)
        bridge = bridge_for(lambda calls, _: executed.append(calls) or [
            DuckDBResponse(call.call_id, b'ok', 200, -1) for call in calls])
        _, status = dispatch(bridge, calls)
        self.assertEqual(status, 2)
        self.assertIn('row limit', bridge.last_error)
        self.assertFalse(executed)
        bridge._release(1)
        self.assertEqual(bridge.retained_batches, 0)

    def test_declared_oversize_payload_is_rejected_before_pointer_copy(self):
        calls, owners = raw_calls(1)
        calls[0].payload_size = duckdb_ai.MAX_BATCH_BYTES + 1
        bridge = bridge_for(lambda *_: self.fail('oversize input was executed'))
        with patch.object(duckdb_ai.ct, 'string_at', side_effect=AssertionError('invalid pointer copy')) as copy:
            _, status = dispatch(bridge, calls)
        self.assertEqual(status, 2)
        self.assertIn('request byte limit', bridge.last_error)
        copy.assert_not_called()
        bridge._release(1)

    def test_input_limit_counts_headers_and_rejects_before_the_excess_copy(self):
        calls, owners = raw_calls(payload=b'1234567', headers=(b'xx',))
        bridge = bridge_for(lambda *_: self.fail('oversize input was executed'))
        with patch.object(duckdb_ai, 'MAX_BATCH_BYTES', 16), \
                patch.object(duckdb_ai.ct, 'string_at', wraps=ct.string_at) as copy:
            _, status = dispatch(bridge, calls)
        self.assertEqual(status, 2)
        self.assertIn('request byte limit', bridge.last_error)
        self.assertEqual(copy.call_count, 1)
        bridge._release(1)

    def test_missing_abi_pointers_are_rejected_without_payload_reads(self):
        for missing in ('payload', 'headers', 'query_id', 'call_id', 'model', 'endpoint'):
            with self.subTest(missing=missing):
                calls, owners = raw_calls(1, headers=(b'Content-Type: application/json',))
                setattr(calls[0], missing, None)
                bridge = bridge_for(lambda *_: self.fail('invalid metadata was executed'))
                with patch.object(duckdb_ai.ct, 'string_at', side_effect=AssertionError('invalid pointer copy')) as copy:
                    _, status = dispatch(bridge, calls)
                self.assertEqual(status, 2)
                self.assertIn('invalid request metadata', bridge.last_error)
                copy.assert_not_called()
                bridge._release(1)

    def test_duplicate_rows_and_payloads_keep_distinct_call_identity_and_full_bytes(self):
        calls, owners = raw_calls()
        bodies = {f'call-{i}': b'{"usage":{"total_tokens":6},"extra":true}\x00' + bytes((255 - i,))
                  for i in range(2)}
        consumed = []
        def execute(prepared, _):
            self.assertEqual([call.row for call in prepared], [7, 7])
            self.assertEqual([call.payload for call in prepared], [b'same', b'same'])
            return [DuckDBResponse(call.call_id, bodies[call.call_id], 429, -1)
                    for call in reversed(prepared)]
        bridge = bridge_for(execute)
        outputs, status = dispatch(bridge, calls, consume=lambda index, *_: consumed.append(index) or 0)
        self.assertEqual(status, 0)
        self.assertEqual(consumed, [1, 0])
        self.assertEqual([(out.row, out.http_status, out.elapsed_ms, ct.string_at(out.body, out.body_size))
                          for out in outputs], [(7, 429, -1, bodies[f'call-{i}']) for i in range(2)])
        self.assertEqual(bridge.retained_batches, 1)
        bridge._release(1)
        self.assertEqual(bridge.retained_batches, 0)

    def test_cancel_during_production_discards_response_before_native_consume(self):
        calls, owners = raw_calls()
        stopped, consumed, state = [], [], [False]
        def execute(prepared, _):
            try:
                state[0] = True
                yield DuckDBResponse(prepared[0].call_id, b'ok', 200, -1)
            finally:
                stopped.append(True)
        bridge = bridge_for(execute)
        _, status = dispatch(bridge, calls, cancelled=lambda: state[0],
                             consume=lambda index, *_: consumed.append(index) or 0)
        self.assertEqual((status, consumed, stopped), (1, [], [True]))
        bridge._release(1)

    def test_cancel_from_consumer_closes_producer_before_next_response(self):
        calls, owners = raw_calls()
        produced, consumed, stopped, state = [], [], [], [False]
        def execute(prepared, _):
            try:
                for call in prepared:
                    produced.append(call.call_id)
                    yield DuckDBResponse(call.call_id, b'ok', 200, -1)
            finally:
                stopped.append(True)
        def consume(index, *_):
            consumed.append(index)
            state[0] = True
            return 0
        bridge = bridge_for(execute)
        _, status = dispatch(bridge, calls, consume=consume, cancelled=lambda: state[0])
        self.assertEqual((status, consumed, produced, stopped), (1, [0], ['call-0'], [True]))
        bridge._release(1)

    def test_elapsed_time_cannot_wrap_the_signed_abi_field(self):
        calls, owners = raw_calls(1)
        consumed = []
        bridge = bridge_for(lambda prepared, _: [DuckDBResponse(prepared[0].call_id, b'ok', 200, 2**64)])
        _, status = dispatch(bridge, calls, consume=lambda *_: consumed.append(True) or 0)
        self.assertEqual(status, 2)
        self.assertIn('invalid status or elapsed time', bridge.last_error)
        self.assertFalse(consumed)
        bridge._release(1)

    def test_executor_cleanup_diagnostic_survives_primary_error_and_next_batch_resets_it(self):
        class Execute:
            bad = True
            last_cleanup_error = None
            def __call__(self, prepared, _):
                self.last_cleanup_error = None
                try:
                    if self.bad:
                        raise ValueError('fixture first execution failure')
                    for call in prepared:
                        yield DuckDBResponse(call.call_id, b'ok', 200, -1)
                finally:
                    if self.bad:
                        self.last_cleanup_error = 'fixture additional graph close failure'
        calls, owners = raw_calls()
        execute = Execute()
        bridge = bridge_for(execute)
        _, status = dispatch(bridge, calls)
        self.assertEqual(status, 2)
        self.assertIn('first execution failure', bridge.last_error)
        self.assertIn('additional graph close failure', bridge.last_cleanup_error)
        bridge._release(1)
        execute.bad = False
        _, status = dispatch(bridge, calls, batch_id=2)
        self.assertEqual(status, 0)
        self.assertIsNone(bridge.last_error)
        self.assertIsNone(bridge.last_cleanup_error)
        bridge._release(2)


def controlled_execution(*, complete_all=True, completion_order=None, response_for=None):
    from src.execution_provider.adapters.incremental_execution import IncrementalExecution
    from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
    from src.scheduling.core.session import SessionEngine
    from tests.scheduling.test_incremental_session import setup
    original, _, backend, clock = setup(held_tasks=3, input_bytes=3072, result_bytes=1536,
        item_input_bytes=1024, item_result_bytes=512, metadata_bytes=1024,
        active_requests=3, active_work=3, offer_tasks=3)
    execution = IncrementalExecution(SessionEngine(original.capacity.limits, backend,
                                     original.policies, clock=clock), None, lambda: True, 5)
    original_poll = backend.poll
    remaining = list(completion_order or ())
    backend.completed_sequences = []
    def poll(handles, maximum):
        keys = tuple(backend.pending)
        if completion_order is not None:
            keys = tuple(key for key in keys if remaining and key.sequence == remaining[0])
            if keys:
                remaining.pop(0)
        elif not complete_all:
            keys = keys[:1]
        for key in keys:
            response = (response_for(key.sequence) if response_for else FullModelResponse(
                200, (), b'full-response:' + str(key.sequence).encode()))
            backend.complete(key, encode_full_response(response))
            backend.completed_sequences.append(key.sequence)
        return original_poll(handles, maximum)
    backend.poll = poll
    return execution, backend


def prepared_calls(*, headers=(), prefix='call'):
    return tuple(DuckDBCall(7, 'query', f'{prefix}-{i}', 'fixture', 'http://localhost/model',
                           b'same', headers, 1, 5, 5, 1) for i in range(3))


class DuckDBSemLoomCoreTests(unittest.TestCase):
    def test_duplicate_input_identity_is_rejected_before_opening_core(self):
        from src.execution_provider.adapters.model_config import FixedModelConfig
        calls = prepared_calls()
        calls = calls[:1] + (replace(calls[1], call_id=calls[0].call_id),) + calls[2:]
        adapter = DuckDBNativeTaskExecutor(None, FixedModelConfig('http://localhost/model', 'fixture', 5000))
        with patch('src.execution_provider.adapters.native_tasks.NativeTaskSession') as session:
            with self.assertRaisesRegex(ValueError, 'repeats a call identity'):
                list(adapter(calls, lambda: False))
            session.assert_not_called()

    def test_duplicate_authorization_is_rejected_before_opening_core(self):
        from src.execution_provider.adapters.model_config import FixedModelConfig
        config = FixedModelConfig('http://localhost/model', 'fixture', 5000, bearer_token='fixture')
        adapter = DuckDBNativeTaskExecutor(None, config)
        calls = prepared_calls(headers=('Authorization: Bearer fixture', 'authorization: Bearer other'))
        with patch('src.execution_provider.adapters.native_tasks.NativeTaskSession') as session:
            with self.assertRaisesRegex(ValueError, 'differs from the common fixed transport'):
                list(adapter(calls, lambda: False))
            session.assert_not_called()

    def test_cancel_between_core_deliveries_never_yields_a_second_result(self):
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.scheduling.core.session_contract import Usage
        execution, backend = controlled_execution()
        adapter = DuckDBNativeTaskExecutor(execution, FixedModelConfig('http://localhost/model', 'fixture', 5000))
        state = [False]
        responses = adapter(prepared_calls(), lambda: state[0])
        first = next(responses)
        self.assertEqual((first.call_id, first.body), ('call-0', b'full-response:0'))
        state[0] = True
        with self.assertRaises(InterruptedError):
            next(responses)
        responses.close()
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertFalse(execution.engine.jobs.jobs)
        self.assertTrue(execution.close())


class DuckDBSemLoomFanInTests(unittest.TestCase):
    def adapter(self, **options):
        from src.execution_provider.adapters.model_config import FixedModelConfig
        execution, backend = controlled_execution(**options)
        return DuckDBNativeTaskExecutor(execution,
            FixedModelConfig('http://localhost/model', 'fixture', 5000)), backend

    def assert_returned(self, adapter, backend):
        from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
        from src.scheduling.core.session_contract import Usage
        for key in tuple(backend.pending):
            backend.complete(key, encode_full_response(FullModelResponse(200, (), b'late')))
        adapter.execution.engine.advance()
        self.assertEqual(adapter.execution.engine.capacity.usage(), Usage())
        self.assertFalse(adapter.execution.engine.jobs.jobs)
        self.assertTrue(adapter.execution.close())

    def test_later_fast_and_earlier_slow_failures_return_in_input_order_with_complete_bodies(self):
        from src.execution_provider.adapters.full_response import FullModelResponse
        def response_for(sequence):
            return FullModelResponse(429, (('x-detail', 'first'), ('x-detail', 'second')),
                json.dumps({'error': {'message': 'earlier failure' if sequence == 0 else 'later failure'},
                            'usage': {'total_tokens': 6}, 'logprobs': {'retained': True},
                            'extra': sequence}).encode())
        adapter, backend = self.adapter(completion_order=(2, 1, 0), response_for=response_for)
        responses = list(adapter(prepared_calls(), lambda: False))
        self.assertEqual(backend.completed_sequences, [2, 1, 0])
        self.assertEqual([response.call_id for response in responses], [f'call-{i}' for i in range(3)])
        self.assertEqual([(response.http_status, response.body) for response in responses],
                         [(429, response_for(i).body) for i in range(3)])
        self.assert_returned(adapter, backend)

    def test_cumulative_response_limit_charges_pending_and_already_delivered_bytes(self):
        from src.execution_provider.adapters.full_response import FullModelResponse
        for order, expected_deliveries in (((2, 1, 0), []), ((0, 1, 2), ['call-0', 'call-1'])):
            with self.subTest(completion_order=order):
                adapter, backend = self.adapter(completion_order=order,
                    response_for=lambda _: FullModelResponse(200, (), b'0123456789'))
                delivered = []
                with patch.object(duckdb_ai, 'MAX_BATCH_BYTES', 25):
                    with self.assertRaisesRegex(ValueError, 'response exceeds its batch byte allowance'):
                        for response in adapter(prepared_calls(), lambda: False):
                            delivered.append(response.call_id)
                self.assertEqual(delivered, expected_deliveries)
                self.assertEqual(backend.completed_sequences, list(order))
                self.assert_returned(adapter, backend)

    def test_single_response_limit_is_checked_after_releasing_its_core_lease(self):
        from src.execution_provider.adapters.full_response import FullModelResponse
        from src.execution_provider.adapters.native_tasks import NativeTaskSession
        adapter, backend = self.adapter(completion_order=(2, 1, 0),
            response_for=lambda _: FullModelResponse(200, (), b'0123456789'))
        released = []
        original_release = NativeTaskSession.release
        def release(flow, leases):
            original_release(flow, leases)
            released.extend(leases)
        with patch.object(duckdb_ai, 'MAX_RESPONSE_BYTES', 9), patch.object(NativeTaskSession, 'release', release):
            with self.assertRaisesRegex(ValueError, 'response exceeds its batch byte allowance'):
                list(adapter(prepared_calls(), lambda: False))
        self.assertEqual(len(released), 1)
        self.assertGreater(adapter.last_close_report.uncertain_requests, 0)
        self.assert_returned(adapter, backend)

    def test_first_input_parser_stop_keeps_cleanup_error_and_cancel_status(self):
        from src.execution_provider.adapters.full_response import FullModelResponse
        from src.execution_provider.adapters.native_tasks import NativeTaskSession
        original_close = NativeTaskSession.close
        def close(flow, **kwargs):
            original_close(flow, **kwargs)
            raise RuntimeError('fixture fan-in close failed')
        for parsed, cancel, expected_status in ((1, False, 3), (2, False, 2), (0, True, 1)):
            with self.subTest(parsed=parsed, cancel=cancel):
                adapter, backend = self.adapter(completion_order=(1, 0), response_for=lambda sequence:
                    FullModelResponse(429, (), b'earlier failure' if sequence == 0 else b'later failure'))
                calls, owners = raw_calls()
                bridge = bridge_for(adapter)
                consumed, state = [], [False]
                def consume(index, response, _):
                    consumed.append((index, ct.string_at(response.contents.body, response.contents.body_size)))
                    state[0] = cancel
                    return parsed
                with patch.object(NativeTaskSession, 'close', close):
                    _, status = dispatch(bridge, calls, consume=consume, cancelled=lambda: state[0])
                self.assertEqual(backend.completed_sequences, [1, 0])
                self.assertEqual(consumed, [(0, b'earlier failure')])
                self.assertEqual(status, expected_status)
                self.assertIn('fan-in close failed', bridge.last_cleanup_error)
                if parsed == 2:
                    self.assertIn('consumer rejected metadata', bridge.last_error)
                else:
                    self.assertIsNone(bridge.last_error)
                bridge._release(1)
                self.assertEqual(bridge.retained_batches, 0)
                self.assert_returned(adapter, backend)

    def test_cancel_after_pending_release_yields_nothing_and_retains_unknown_work(self):
        from src.execution_provider.adapters.native_tasks import NativeTaskSession
        adapter, backend = self.adapter(completion_order=(2,))
        original_release = NativeTaskSession.release
        state = [False]
        def release(flow, leases):
            original_release(flow, leases)
            state[0] = True
        delivered = []
        with patch.object(NativeTaskSession, 'release', release):
            with self.assertRaises(InterruptedError):
                for response in adapter(prepared_calls(), lambda: state[0]):
                    delivered.append(response.call_id)
        self.assertFalse(delivered)
        self.assertEqual(backend.completed_sequences, [2])
        self.assertGreater(adapter.last_close_report.uncertain_requests, 0)
        self.assert_returned(adapter, backend)

    def test_repeated_delivery_keeps_association_error_when_release_and_close_also_fail(self):
        from src.execution_provider.adapters.native_tasks import NativeTaskSession
        adapter, backend = self.adapter()
        original_advance, original_release, original_close = (
            NativeTaskSession.advance, NativeTaskSession.release, NativeTaskSession.close)
        released = set()
        def advance(flow, count):
            progress = original_advance(flow, count)
            if progress.deliveries:
                progress = replace(progress, deliveries=progress.deliveries + progress.deliveries[:1])
            return progress
        def release(flow, leases):
            original_release(flow, leases)
            if any(lease in released for lease in leases):
                raise RuntimeError('fixture repeated release failed')
            released.update(leases)
        def close(flow, **kwargs):
            original_close(flow, **kwargs)
            raise RuntimeError('fixture fan-in close failed')
        with patch.object(NativeTaskSession, 'advance', advance), \
                patch.object(NativeTaskSession, 'release', release), patch.object(NativeTaskSession, 'close', close):
            with self.assertRaisesRegex(ValueError, 'changes or repeats call identity'):
                list(adapter(prepared_calls(), lambda: False))
        self.assertIn('release:', adapter.last_cleanup_errors[0])
        self.assertIn('fan-in close failed', adapter.last_cleanup_errors[1])
        self.assert_returned(adapter, backend)

    def test_finished_core_cannot_hide_a_missing_input_response(self):
        from src.execution_provider.adapters.native_tasks import NativeTaskSession
        adapter, backend = self.adapter()
        original_advance = NativeTaskSession.advance
        def advance(flow, count):
            progress = original_advance(flow, count)
            missing = tuple(delivery for delivery in progress.deliveries if delivery.key.sequence == 0)
            if missing:
                flow.release(tuple(delivery.lease_id for delivery in missing))
                progress = replace(progress, deliveries=tuple(delivery for delivery in progress.deliveries
                                                             if delivery.key.sequence != 0))
            return progress
        with patch.object(NativeTaskSession, 'advance', advance):
            with self.assertRaisesRegex(ValueError, 'response is incomplete'):
                list(adapter(prepared_calls(), lambda: False))
        self.assert_returned(adapter, backend)

    def test_query_owner_reuses_one_grant_for_vectors_with_duplicate_rows(self):
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.execution_provider.adapters.native_tasks import NativeQueryJob
        from src.scheduling.core.session_contract import Usage
        execution, backend = controlled_execution()
        with NativeQueryJob(execution, 'sql-query') as query:
            adapter = DuckDBNativeTaskExecutor(execution,
                FixedModelConfig('http://localhost/model', 'fixture', 5000), query_owner=query)
            for vector in range(2):
                responses = list(adapter(prepared_calls(prefix=f'vector-{vector}'), lambda: False))
                self.assertEqual([response.call_id for response in responses],
                                 [f'vector-{vector}-{i}' for i in range(3)])
                self.assertEqual(execution.engine.capacity.usage(), Usage())
                self.assertIn(query.job, execution.engine.jobs.jobs)
                self.assertFalse(execution.engine.jobs.jobs[query.job].closing)
            self.assertEqual(execution.engine.jobs.sequence, 1)
        self.assertFalse(execution.engine.jobs.jobs)
        self.assertTrue(execution.close())

    def test_consumer_stop_cancels_query_but_keeps_unknown_work_until_terminal(self):
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
        from src.execution_provider.adapters.native_tasks import NativeQueryJob
        from src.scheduling.core.session_contract import Usage
        execution, backend = controlled_execution(complete_all=False)
        query = NativeQueryJob(execution, 'sql-query')
        adapter = DuckDBNativeTaskExecutor(execution,
            FixedModelConfig('http://localhost/model', 'fixture', 5000), query_owner=query)
        responses = adapter(prepared_calls(), lambda: False)
        next(responses)
        responses.close()
        self.assertGreater(adapter.last_close_report.uncertain_requests, 0)
        self.assertGreater(execution.engine.capacity.usage().active_requests, 0)
        with self.assertRaises(InterruptedError):
            list(adapter(prepared_calls(prefix='later-vector'), lambda: False))
        query.close()
        self.assertTrue(execution.engine.jobs.jobs[query.job].closing)
        for key in tuple(backend.pending):
            backend.complete(key, encode_full_response(FullModelResponse(200, (), b'late')))
        execution.engine.advance()
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertFalse(execution.engine.jobs.jobs)
        self.assertTrue(execution.close())


if __name__ == '__main__':
    unittest.main()
