"""Private process bridge tests: real Core, fixture I/O, no models or HTTP."""

import asyncio
import multiprocessing as mp
import threading
import time
import unittest

from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_query_bridge import NativeTaskBridge, NativeTaskClient, _encode
from src.execution_provider.adapters.native_tasks import build_native_execution, prepare_native_task
from src.scheduling.core.session_contract import State
from src.scheduling.core.session_jobs import round_robin_flow, shared_compute_job_budget
from src.scheduling.submission_control.query_sharing import BoundedShortJobBurst


def task(sequence, label='q'):
    return prepare_native_task(('body-' + label + str(sequence)).encode(), sequence,
                              row_sequence=sequence, call_id='map')


def response():
    return encode_full_response(FullModelResponse(200, (), b'{"choices":[]}'))


def frontend_crash(connection, ready, disconnect=False):
    client = NativeTaskClient(connection, 'a', timeout_s=5)
    flow = client.open_session('a', 'map')
    flow.offer((task(0),))
    while not flow.advance(1).deliveries:
        time.sleep(.005)
    if disconnect:
        connection.close()
    ready.set()
    while True:
        time.sleep(.1)  # Intentionally retain one unreleased delivery until killed.


def eventually(predicate, timeout=4):
    end = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= end:
            raise AssertionError('bridge fixture did not reach expected state')
        time.sleep(.005)


class NativeQueryBridgeTests(unittest.TestCase):
    def bridge(self, counts, *, execute=None, candidate=False):
        async def immediate(task, endpoint):
            await asyncio.sleep(.005)
            return response()
        def factory():
            execution = build_native_execution(FixedModelConfig('http://localhost/fixture', 'fixture', 1000),
                physical=None, execute=execute or immediate, max_tasks=8, max_active_requests=2,
                max_jobs=len(counts), allocate_job=shared_compute_job_budget,
                choose_flow=BoundedShortJobBurst(work_unit='work_units', short_work_limit=2,
                                                bypass_work_limit=2) if candidate else
                            round_robin_flow)
            execution.engine.observe_job_work = True
            return execution
        bridge = NativeTaskBridge(factory, counts).start()
        self.addCleanup(self.finish, bridge)
        return bridge

    def finish(self, bridge):
        for peer in bridge._peers.values():
            peer.client_connection.close()
        self.assertTrue(bridge.close(4), bridge.snapshot())

    def open(self, bridge, label):
        client = NativeTaskClient(bridge.client_connection(label), label, timeout_s=2)
        return client, client.open_session(label, 'map')

    def consume(self, flow):
        deliveries = []
        while True:
            progress = flow.advance(4)
            deliveries.extend(progress.deliveries)
            flow.release(tuple(d.lease_id for d in progress.deliveries))
            if progress.state == State.FINISHED:
                break
            flow.wait(progress)
        flow.close(clean=True)
        return deliveries

    def test_two_queries_use_separate_sessions_on_one_owner_with_matching_policy_api(self):
        for candidate in (False, True):
            bridge = self.bridge({'a': 2, 'b': 2}, candidate=candidate)
            _, a = self.open(bridge, 'a')
            _, b = self.open(bridge, 'b')
            a.offer((task(0, 'a'), task(1, 'a')))
            b.offer((task(0, 'b'), task(1, 'b')))
            a.end_input(); b.end_input()
            da, db = self.consume(a), self.consume(b)
            self.assertNotEqual(da[0].key.session_id, db[0].key.session_id)
            self.assertEqual([d.key.sequence for d in da], [0, 1])
            self.assertEqual([d.key.sequence for d in db], [0, 1])
            snap = bridge.snapshot()
            self.assertNotEqual(snap['owner_thread'], threading.get_ident())
            eventually(lambda: bridge.snapshot()['jobs'] == 0)

    def test_typed_limits_description_has_no_job_or_compute_side_effect(self):
        bridge=self.bridge({'a':1,'b':1})
        client=NativeTaskClient(bridge.client_connection('a'),'a')
        from src.scheduling.core.session_contract import SessionLimits
        limits=client.describe_limits()
        self.assertIs(type(limits),SessionLimits)
        snapshot=bridge.snapshot()
        self.assertEqual(snapshot['jobs'],0)
        self.assertFalse(any(snapshot['usage'].values()))
        a=client.open_session('a','map')
        self.assertEqual(a.limits.item_input_bytes,limits.item_input_bytes)
        a.offer((task(0),));a.end_input()
        self.assertEqual(len(self.consume(a)),1)

    def test_connection_cannot_open_another_query_or_seal_partial_input(self):
        bridge = self.bridge({'a': 2, 'b': 1})
        client = NativeTaskClient(bridge.client_connection('a'), 'a', timeout_s=2)
        with self.assertRaises(RuntimeError):
            client.open_session('b', 'map')
        _, b = self.open(bridge, 'b')
        b.offer((task(0),)); b.end_input()
        self.assertEqual(len(self.consume(b)), 1)
        self.assertEqual(bridge.snapshot()['peers']['a']['accepted'], 0)

    def test_input_seal_requires_declared_complete_job(self):
        bridge = self.bridge({'a': 2})
        _, a = self.open(bridge, 'a')
        a.offer((task(0),))
        with self.assertRaises(RuntimeError):
            a.end_input()
        a.close()

    def test_foreign_delivery_release_is_local_to_the_offending_query(self):
        bridge = self.bridge({'a': 1, 'b': 1})
        _, a = self.open(bridge, 'a'); _, b = self.open(bridge, 'b')
        a.offer((task(0),)); b.offer((task(0),))
        a.end_input(); b.end_input()
        result = None
        while not result:
            result = b.advance(1).deliveries
            time.sleep(.005)
        with self.assertRaises(RuntimeError):
            a.release((result[0].lease_id,))
        a.close()
        b.release((result[0].lease_id,))
        self.assertEqual(b.advance(1).state, State.FINISHED)
        b.close(clean=True)
        self.assertIsNone(bridge.snapshot()['error'])

    def test_cancel_keeps_accepted_compute_until_terminal_then_peer_progresses(self):
        entered, release = threading.Event(), threading.Event()
        async def gated(task, endpoint):
            if task.spec.flow_id == 'map' and b'body-a' in task.task.payload:
                entered.set()
                while not release.is_set(): await asyncio.sleep(.005)
            return response()
        bridge = self.bridge({'a': 2, 'b': 1}, execute=gated)
        _, a = self.open(bridge, 'a'); _, b = self.open(bridge, 'b')
        a.offer((task(0, 'a'), task(1, 'a'))); a.end_input()
        self.assertTrue(entered.wait(2))
        bridge.request_cancel('a')
        eventually(lambda: a.advance(1).state == State.CANCELLED)
        a.close()
        self.assertEqual(bridge.snapshot()['usage']['active_requests'], 2)
        b.offer((task(0, 'b'),)); b.end_input()
        self.assertEqual(b.advance(1).deliveries, ())
        release.set()
        self.assertEqual(len(self.consume(b)), 1)

    def test_slow_consumer_holds_its_lease_without_blocking_another_query(self):
        bridge = self.bridge({'a': 1, 'b': 1})
        _, a = self.open(bridge, 'a'); _, b = self.open(bridge, 'b')
        a.offer((task(0),)); a.end_input()
        delivery = ()
        while not delivery: delivery = a.advance(1).deliveries
        b.offer((task(0),)); b.end_input()
        self.assertEqual(len(self.consume(b)), 1)
        self.assertEqual(bridge.snapshot()['leased_results'], 1)
        a.release((delivery[0].lease_id,)); a.advance(1); a.close(clean=True)

    def test_process_stop_proof_reaps_orphan_delivery(self):
        context = mp.get_context('spawn')
        bridge = self.bridge({'a': 1})
        ready = context.Event()
        process = context.Process(target=frontend_crash, args=(bridge.client_connection('a'), ready))
        process.start(); bridge.bind_process('a', process)
        bridge.client_connection('a').close()
        try:
            self.assertTrue(ready.wait(5))
            self.assertEqual(bridge.snapshot()['leased_results'], 1)
        finally:
            process.terminate(); process.join(3)
        self.assertFalse(process.is_alive())
        eventually(lambda: not bridge.snapshot()['usage']['held_tasks'])
        self.assertTrue(bridge.snapshot()['peers']['a']['stop_proven'])

    def test_eof_is_not_process_stop_proof_and_close_retains_delivery(self):
        context = mp.get_context('spawn')
        bridge = self.bridge({'a': 1, 'b': 1})
        ready = context.Event()
        process = context.Process(target=frontend_crash,
                                  args=(bridge.client_connection('a'), ready, True))
        process.start(); bridge.bind_process('a', process)
        bridge.client_connection('a').close()
        try:
            self.assertTrue(ready.wait(5))
            eventually(lambda: bridge.snapshot()['peers']['a']['disconnected'])
            self.assertTrue(process.is_alive())
            self.assertEqual(bridge.snapshot()['leased_results'], 1)
            _, b = self.open(bridge, 'b')
            b.offer((task(0),)); b.end_input()
            self.assertEqual(len(self.consume(b)), 1)
            self.assertFalse(bridge.close(.05))
            self.assertEqual(bridge.snapshot()['leased_results'], 1)
        finally:
            process.terminate(); process.join(3)
        eventually(lambda: bridge.snapshot()['jobs'] == 0)

    def test_malformed_peer_frame_does_not_block_other_query(self):
        bridge = self.bridge({'a': 1, 'b': 1})
        bridge.client_connection('a').send_bytes(b'not-a-pickle-frame')
        eventually(lambda: bridge.snapshot()['peers']['a']['disconnected'])
        _, b = self.open(bridge, 'b')
        b.offer((task(0),)); b.end_input()
        self.assertEqual(len(self.consume(b)), 1)
        self.assertIsNone(bridge.snapshot()['error'])

    def test_frame_byte_limit_is_explicit(self):
        with self.assertRaises(ValueError):
            _encode(b'x' * (9 * 1024 * 1024))

    def test_delivery_batch_respects_frame_reservation(self):
        async def large(task, endpoint):
            return encode_full_response(FullModelResponse(200, (), b'x' * 900_000))
        bridge = self.bridge({'a': 8}, execute=large)
        _, a = self.open(bridge, 'a')
        a.offer(tuple(task(i) for i in range(8))); a.end_input()
        eventually(lambda: bridge.snapshot()['ready_results'] == 8)
        first = a.advance(8).deliveries
        self.assertEqual(len(first), 7)
        self.assertLess(len(_encode(first)), 8 * 1024 * 1024)
        a.release(tuple(d.lease_id for d in first))
        self.assertEqual(len(self.consume(a)), 1)

    def test_concurrent_teardown_closes_actual_pipe_descriptor_once(self):
        bridge = NativeTaskBridge(lambda: None, {'a':1})
        peer = bridge._peers['a']
        original, calls = peer.connection.close, []
        barrier = threading.Barrier(2)
        def observed_close():
            calls.append(1)
            time.sleep(.02)
            original()
        peer.connection.close = observed_close
        def closing():
            barrier.wait()
            bridge._close_connection(peer)
        threads=[threading.Thread(target=closing) for _ in range(2)]
        for thread in threads:thread.start()
        for thread in threads:thread.join(1)
        peer.client_connection.close()
        self.assertTrue(peer.connection.closed)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(len(calls),1)


if __name__ == '__main__':
    unittest.main()
