"""Bounded Core/broker diagnostics with table and backend seams; no HTTP."""
import asyncio
from contextlib import contextmanager, ExitStack
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport
from src.scheduling.core.session_contract import State, Usage
from src.scheduling.runtime.stage_broker import StageBrokerLimits
from tests.execution_provider.test_map_preparation import batches, wait_until
from tests.execution_provider.test_native_tasks import fixture
from tests.execution_provider.test_ray_map_transport import FakeRay
from tests.modalities.image.stage_block_trace import StageBlockTrace


class StageBlockTraceTests(unittest.TestCase):
    @contextmanager
    def scenario(self, batch_rows, *, payload_bytes=32, ready_bytes=8192, ready_work=2):
        execution,backend,_=fixture(held_tasks=4,offer_tasks=4,input_bytes=4096,result_bytes=2048)
        physical=RayMapConfig("fixture-cluster",1,batch_rows,4096,8192,payload_backend="arrow",
            preparation=StageBrokerLimits(4096,ready_bytes,ready_work,1,4))
        transport=RayMapTransport(FixedModelConfig("http://localhost/fixture","fixture",1000),2,
            physical=physical,ray_api=FakeRay(None))
        preparation=transport.prepare_inputs(execution.engine.capacity.limits,lambda:None)
        execution.engine.preparation=preparation
        flow=NativeTaskSession(execution,"same-query","same-map")
        trace=StageBlockTrace(execution,lambda:"model")
        tasks=tuple(prepare_native_task(bytes([65+index])*payload_bytes,index,row_sequence=index,
            call_id="row",max_result_bytes=512) for index in range(3))
        with ExitStack() as stack:
            stack.enter_context(patch("src.execution_provider.adapters.map_preparation.iter_payload_batches",batches))
            trace.attach(stack)
            try:
                self.assertEqual(flow.offer(tasks[:2]).accepted_prefix_count,2)
                for _ in range(4):
                    flow.advance(4)
                    wait_until(lambda:not preparation.running)
                self.assertEqual({key.sequence for key in backend.pending},{0,1})
                for _,task in backend.pending.values():preparation.claim(task)
                yield execution,backend,flow,preparation,trace,tasks
            finally:
                # Give every modeled member a known receipt before local close.
                for key in tuple(backend.pending):
                    row=preparation.rows.get(key)
                    if row is not None and row.state!="done":
                        if row.state=="ready":preparation.claim(backend.pending[key][1])
                        preparation.confirm(key)
                    backend.complete(key,b"ok")
                flow.end_input()
                for _ in range(20):
                    progress=flow.advance(4)
                    flow.release(tuple(delivery.lease_id for delivery in progress.deliveries))
                    for key in tuple(backend.pending):
                        preparation.claim(backend.pending[key][1])
                        preparation.confirm(key);backend.complete(key,b"ok")
                    if progress.state==State.FINISHED:break
                    wait_until(lambda:not preparation.running)
                flow.close(clean=True)
                self.assertEqual(execution.engine.capacity.usage(),Usage())
                self.assertEqual(execution.engine.jobs.jobs,{})
                asyncio.run(transport.close())

    def fast_terminal(self, backend, flow, preparation):
        key=next(key for key in backend.pending if key.sequence==1)
        preparation.confirm(key)
        backend.complete(key,b"fast")
        progress=flow.advance(4)
        self.assertEqual([delivery.key.sequence for delivery in progress.deliveries],[1])
        flow.release(tuple(delivery.lease_id for delivery in progress.deliveries))

    def test_shared_tail_blocks_work_only_with_legal_same_job_capacity(self):
        with self.scenario(2) as (execution,backend,flow,preparation,trace,tasks):
            self.fast_terminal(backend,flow,preparation)
            self.assertEqual(flow.offer(tasks[2:]).accepted_prefix_count,1)
            flow.advance(4);wait_until(lambda:not preparation.running)
            trace.checkpoint("before_wait",False)
            event=trace.checkpoints[-1];candidate=event["candidates"][0]
            self.assertEqual(candidate["reasons"],["ready_work"])
            self.assertTrue(candidate["authority_work_refund_would_fit"])
            self.assertEqual(candidate["legal_model_rows"],[dict(session_id=0,sequence=2)])
            self.assertEqual(event["global_usage"],event["scopes"]["0"]["job_usage"])
            self.assertEqual(event["global_usage"],event["scopes"]["0"]["session_usage"])
            self.assertEqual((event["global_usage"]["active_requests"],event["global_usage"]["active_work"]),(1,1))
            self.assertEqual(event["preparation"]["ready_held_work"],2)
            self.assertEqual(event["tails"][0]["confirmed_work"],1)
            self.assertGreater(event["tails"][0]["physical_bytes"],0)
            self.assertTrue(any(r["reasons"]==["ready_work"] for r in trace.refusals))
            frozen=event["tails"][0]["members"]["0:0"]["authority"]
            self.assertIsNone(frozen)
        self.assertIsNone(event["tails"][0]["members"]["0:0"]["authority"])

    def test_fixed_single_rows_remove_tail_hold_with_same_quotas(self):
        with self.scenario(1) as (_,backend,flow,preparation,trace,tasks):
            self.fast_terminal(backend,flow,preparation)
            self.assertEqual(flow.offer(tasks[2:]).accepted_prefix_count,1)
            flow.advance(4);wait_until(lambda:not preparation.running)
            self.assertTrue(preparation.is_ready(next(key for key in preparation.rows if key.sequence==2)))
            trace.checkpoint("same_checkpoint",False)
            self.assertEqual(trace.checkpoints[-1]["tails"],[])
            self.assertEqual(trace.checkpoints[-1]["candidates"],[])
            self.assertEqual(trace.summary()["work_only_opportunities"],[])

    def test_next_double_byte_reservation_is_distinct_from_tail_work(self):
        with self.scenario(2,payload_bytes=650,ready_bytes=2712,ready_work=4) as (_,backend,flow,preparation,trace,tasks):
            self.fast_terminal(backend,flow,preparation)
            self.assertEqual(flow.offer(tasks[2:]).accepted_prefix_count,1)
            flow.advance(4);wait_until(lambda:not preparation.running)
            trace.checkpoint("same_checkpoint",False)
            event=trace.checkpoints[-1];candidate=event["candidates"][0]
            self.assertEqual(candidate["reasons"],["ready_bytes"])
            self.assertFalse(candidate["authority_work_refund_would_fit"])
            self.assertLessEqual(event["preparation"]["ready_held_bytes"]+candidate["next_physical_upper_bound"],2712)
            self.assertGreater(event["preparation"]["ready_held_bytes"]+candidate["next_ready_reservation"],2712)

    def test_unconfirmed_members_do_not_enter_work_refund_counterfactual(self):
        with self.scenario(2) as (_,_,flow,preparation,trace,tasks):
            self.assertEqual(flow.offer(tasks[2:]).accepted_prefix_count,1)
            flow.advance(4);wait_until(lambda:not preparation.running)
            trace.checkpoint("unconfirmed",False)
            event=trace.checkpoints[-1]
            self.assertEqual(event["tails"],[])
            self.assertFalse(event["candidates"][0]["authority_work_refund_would_fit"])
            self.assertEqual(event["candidates"][0]["legal_model_rows"],[])
            self.assertEqual(event["preparation"]["model_inflight"],1)
            self.assertEqual(preparation.broker.limits.model_inflight,4)

    def test_disabled_dispatch_is_not_a_legal_capacity_opportunity(self):
        with self.scenario(2) as (_,backend,flow,preparation,trace,tasks):
            self.fast_terminal(backend,flow,preparation)
            self.assertEqual(flow.offer(tasks[2:]).accepted_prefix_count,1)
            flow.advance(4);wait_until(lambda:not preparation.running)
            flow.session.set_dispatch_enabled(False)
            trace.checkpoint("disabled",False)
            self.assertEqual(trace.checkpoints[-1]["candidates"][0]["legal_model_rows"],[])
            self.assertFalse(any(item["checkpoint"]==len(trace.checkpoints)-1
                for item in trace.summary()["work_only_opportunities"]))
            flow.session.set_dispatch_enabled(True)

    def test_observed_wait_preserves_positional_and_keyword_progress(self):
        with self.scenario(2) as (_,_,flow,_,trace,_):
            progress=SimpleNamespace(has_immediate_work=True)
            self.assertIsNone(flow.wait(progress))
            self.assertIsNone(flow.wait(progress=progress,timeout_s=.01))
            self.assertEqual([event["label"] for event in trace.checkpoints[-2:]],["before_wait","before_wait"])


if __name__=="__main__":
    unittest.main()
