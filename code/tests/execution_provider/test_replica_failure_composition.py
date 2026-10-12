"""Compose both never-sent paths with another Job's unknown Ray/HTTP responsibility."""

import asyncio
from dataclasses import asdict
import threading
import time
import unittest
from unittest.mock import patch

from src.execution_provider.completion import CompletionAdapterError
from src.execution_provider.adapters.full_response import FullModelResponse, decode_full_response, encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.model_replicas import (
    ModelReplica, ModelReplicaSet, build_model_replica_execution, replica_usage,
)
from src.execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport, _HttpActor
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.experiments.map_observation_probe import SyntheticTable
from src.scheduling.core.session_contract import State, TaskKey, Usage
from tests.execution_provider.test_ray_map_transport import FakeRay


class ReplicaFailureCompositionTests(unittest.TestCase):
    observations = []

    def wait_for(self, predicate, advance=lambda: None):
        deadline = time.monotonic() + 3
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail('composed responsibility fixture did not progress')
            advance()
            time.sleep(0.001)

    def test_payload_and_observer_failures_do_not_refund_unknown_rpc_or_http_job(self):
        for unknown_stage in ('ray_await', 'http'):
            with self.subTest(unknown_stage=unknown_stage):
                children, events, entered, payload_gate = {}, [], threading.Event(), {}
                replicas = ModelReplicaSet('fixture', tuple(ModelReplica(name,
                    'http://localhost/' + name, 'fixture-location', (), 'a' * 64, 1, 1)
                    for name in ('payload', 'observer', 'unknown')), 3, 3)
                physical = {replica.endpoint_id: RayMapConfig('fixture-cluster', 1, 1,
                    MAX_FRAME_BYTES + 24, 2 * MAX_FRAME_BYTES, payload_backend='arrow')
                    for replica in replicas.replicas}

                class Child(RayMapTransport):
                    def __init__(child, config, capacity, observer, *, physical, before_request):
                        name = config.endpoint_url.rsplit('/', 1)[1]
                        child.corrupt, child.http_calls, child.rpc_calls = name == 'payload', [], []
                        child.actor = _HttpActor(config, capacity, response_mode='full')
                        async def http(actual, endpoint):
                            child.http_calls.append(actual.key)
                            if name == 'unknown':
                                raise TimeoutError('controlled HTTP outcome unknown')
                            return encode_full_response(FullModelResponse(200, (), b'ok'))
                        child.actor.transport.execute = http
                        async def remote(table, index, template):
                            child.rpc_calls.append(template.key)
                            if name == 'payload' and child.corrupt:
                                gate = asyncio.Event()
                                payload_gate['value'] = gate
                                entered.set()
                                await gate.wait()
                            if name == 'unknown' and unknown_stage == 'ray_await':
                                raise OSError('controlled RPC reply unavailable')
                            return await child.actor.execute(table, index, template)
                        ray = FakeRay(remote)
                        put = ray.put
                        def corrupted_put(table):
                            reference = put(table)
                            if child.corrupt:
                                return SyntheticTable(tuple((sid + 99, sequence, body)
                                                            for sid, sequence, body in table.rows))
                            return reference
                        ray.put = corrupted_put
                        super().__init__(config, capacity, observer, physical=physical,
                                         before_request=before_request, ray_api=ray)
                        child.fake_ray = ray
                        children[name] = child

                    async def close(child):
                        try:
                            await super().close()
                        finally:
                            await child.actor.close()

                def observe(event):
                    events.append(event)
                    if event['event'] == 'replica_transport_started' and event['endpoint_id'] == 'observer':
                        raise RuntimeError('controlled observer failure before child execution')

                def batches(rows, *args, **kwargs):
                    yield SyntheticTable(rows)

                def task():
                    return prepare_native_task(b'input', 0, row_sequence=0,
                                               call_id='fixture-call', max_result_bytes=512)

                with patch('src.execution_provider.adapters.ray_map_transport.RayMapTransport', Child), \
                        patch('src.execution_provider.adapters.ray_map_transport.iter_payload_batches', batches):
                    execution = build_model_replica_execution(FixedModelConfig('http://localhost/unused', 'fixture', 1000),
                        replicas=replicas, physical=physical, response_mode='full', max_tasks=6,
                        max_jobs=3, observer=observe)
                    first = NativeTaskSession(execution, 'payload-query', 'map')
                    local = NativeTaskSession(execution, 'observer-query', 'map')
                    peer = NativeTaskSession(execution, 'unknown-query', 'map')
                    for flow in (first, local, peer):
                        flow.offer((task(),))
                    followup = None
                    try:
                        execution.engine.advance()
                        self.assertTrue(entered.wait(2))
                        first.session.fail('first caller error')
                        execution.engine.backend._loop.call_soon_threadsafe(payload_gate['value'].set)
                        self.wait_for(lambda: (replica_usage(execution)['payload']['active_requests'] == 0
                            and replica_usage(execution)['observer']['active_requests'] == 0
                            and replica_usage(execution)['unknown']['uncertain_requests'] == 1), execution.engine.advance)
                        self.assertEqual(first.session.error, 'first caller error')
                        self.assertEqual(execution.engine.capacity.usage(job_id=first.job.job_id), Usage())
                        self.assertEqual(local.session.state, State.OPEN)
                        local.session.set_dispatch_enabled(False)
                        rejected = local.advance(1).deliveries[0]
                        with self.assertRaises(CompletionAdapterError):
                            decode_full_response(rejected.result)
                        local.release((rejected.lease_id,))
                        self.assertEqual(execution.engine.capacity.usage(job_id=local.job.job_id), Usage())
                        wrapper = execution.engine.backend._execute.__self__
                        self.assertEqual(wrapper.observation_failure['reason']['exception_type'], 'builtins.RuntimeError')
                        self.assertEqual((children['observer'].rpc_calls, children['observer'].fake_ray.puts), ([], []))
                        self.assertEqual(children['payload'].http_calls, [])
                        payload_failure = next(e for e in events if e['event'] == 'ray_execution_error'
                                               and e['endpoint_id'] == 'payload')
                        remote_failure = next(e for e in events if e['event'] == 'ray_execution_error'
                                              and e['endpoint_id'] == 'unknown')
                        self.assertEqual((payload_failure['stage'], payload_failure['remote_outcome']), ('payload', 'not_sent'))
                        self.assertEqual((remote_failure['stage'], remote_failure['remote_outcome']), (unknown_stage, 'unconfirmed'))
                        peer_usage = execution.engine.capacity.usage(job_id=peer.job.job_id)
                        self.assertEqual((peer_usage.active_requests, peer_usage.active_work), (1, 1))
                        unknown_bytes = children['unknown'].used_bytes
                        self.assertGreater(unknown_bytes, 0)
                        first.close(clean=True); local.close(clean=True)
                        self.assertEqual(peer.close().uncertain_requests, 1)
                        peer_error = peer.session.error
                        for _ in range(8): execution.engine.advance()
                        self.assertEqual(execution.engine.capacity.usage(job_id=peer.job.job_id), peer_usage)
                        self.assertEqual(children['unknown'].used_bytes, unknown_bytes)
                        self.assertEqual(len(children['unknown'].rpc_calls), 1)
                        self.assertFalse(execution.close())
                        children['payload'].corrupt = False
                        followup = NativeTaskSession(execution, 'followup-query', 'map')
                        followup.offer((task(),)); followup.end_input()
                        self.wait_for(lambda: any(r.phase == 'READY' and r.key.session_id == followup.session.session_id
                            for r in execution.engine.capacity.records.values()), execution.engine.advance)
                        delivery = followup.advance(1).deliveries[0]
                        self.assertEqual(decode_full_response(delivery.result).body, b'ok')
                        followup.release((delivery.lease_id,)); followup.close(clean=True)
                        self.assertEqual(children['payload'].used_bytes, 0)
                        self.assertEqual(sum(e['event'] == 'ray_block_released' and e['endpoint_id'] == 'payload'
                                             for e in events), 2)
                        self.assertEqual(execution.engine.capacity.usage(job_id=peer.job.job_id), peer_usage)
                        self.assertEqual(peer.session.error, peer_error)
                        self.assertFalse(execution.close())
                        self.assertEqual(children['unknown'].unknown, {TaskKey(peer.session.session_id, 0)})
                        self.observations.append(dict(unknown_stage=unknown_stage,
                            unknown_job_usage=asdict(peer_usage), retained_object_bytes=unknown_bytes,
                            unknown_rpc_submissions=len(children['unknown'].rpc_calls),
                            observer_child_submissions=len(children['observer'].rpc_calls),
                            payload_failure_http_calls=0, followup_completed=True,
                            payload_block_releases=sum(e['event'] == 'ray_block_released'
                                and e['endpoint_id'] == 'payload' for e in events),
                            first_error=first.session.error, wire_http_posts=0))
                    finally:
                        if 'value' in payload_gate:
                            execution.engine.backend._loop.call_soon_threadsafe(payload_gate['value'].set)
                        for flow in (first, local, peer, followup):
                            if flow is not None: flow.close(clean=True)
                        self.wait_for(lambda: all(slot.future.done() for slot in execution.engine.backend._slots.values()))
                        # Stop this test's local I/O thread while preserving the Core
                        # unknown record; teardown is never a synthetic terminal.
                        with self.assertRaisesRegex(RuntimeError, 'finalization failed'):
                            execution.engine.backend.close()
                        self.assertEqual(execution.engine.capacity.usage().active_requests, 1)
