"""Complete native HTTP input capacity; existing PG framing remains independent."""
import asyncio
from dataclasses import replace
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import patch

import httpx

from src.execution_provider.adapters.full_response import FullResponseTransport, decode_full_response
from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import NativeTaskSession, build_native_execution, prepare_native_task
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, ray_map_factory
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.scheduling.core.session_contract import State, Usage
from src.scheduling.runtime.stage_broker import StageBrokerLimits


MIB=1024*1024
MODEL=FixedModelConfig('http://localhost/native-fixture','fixture',3000)
ERROR=b'\xfforiginal429\x00'


def offered(sequence,size):
    return prepare_native_task(str(sequence).encode()+b'x'*(size-1),sequence,
                               row_sequence=sequence+100,call_id=f'large-{sequence}')


def poll(flow):
    deadline=time.monotonic()+5
    while time.monotonic()<deadline:
        progress=flow.advance(min(4,flow.limits.held_tasks))
        if progress.deliveries or progress.state==State.FAILED:
            return progress
        flow.wait(progress)
    raise TimeoutError('controlled native fixture did not progress')


def drain(execution):
    deadline=time.monotonic()+5
    while execution.engine.capacity.records and time.monotonic()<deadline:
        progress=execution.engine.advance()
        if not progress.has_immediate_work:
            execution.engine.wake.wait(execution.engine.wake.generation,.01)


class NativeRequestCapacityTests(unittest.TestCase):
    def make(self,*,item=4*MIB,total=8*MIB,held=3,active=2,handler=None):
        calls=[]
        async def serve(request):
            calls.append(request.content)
            if handler is not None:
                await handler(request)
            return httpx.Response(429,headers=[('x-duplicate','one'),('x-duplicate','two')],
                                  stream=httpx.ByteStream(ERROR))
        def transport(config,concurrency,observer):
            result=FullResponseTransport(config,concurrency,observer)
            result._client=httpx.AsyncClient(transport=httpx.MockTransport(serve))
            return result
        execution=build_native_execution(MODEL,physical=None,max_request_bytes=item,
            input_bytes=total,max_tasks=held,max_active_requests=active,transport_factory=transport)
        return execution,calls

    def test_default_native_and_pg_remain_one_mib(self):
        for factory in (lambda:build_native_execution(MODEL,physical=None),
                        lambda:build_fixed_model_execution(MODEL)):
            execution=factory()
            try:
                self.assertEqual(execution.engine.capacity.limits.item_input_bytes,MIB)
                self.assertEqual(execution.engine.capacity.limits.input_bytes,MIB)
            finally:
                self.assertTrue(execution.close())
        self.assertEqual(MAX_FRAME_BYTES,MIB)

    def test_illegal_native_capacity_fails_before_transport_creation(self):
        for options in ({'max_request_bytes':True},{'max_request_bytes':0},
                        {'max_request_bytes':8*MIB+1},{'max_request_bytes':1.5},
                        {'max_request_bytes':4*MIB},{'max_request_bytes':4*MIB,'input_bytes':None},
                        {'max_request_bytes':4*MIB,'input_bytes':3*MIB},
                        {'item_input_bytes':8*MIB}):
            with self.subTest(options=options),patch(
                    'src.execution_provider.adapters.native_tasks.FullResponseTransport') as transport:
                with self.assertRaises(ValueError):
                    build_native_execution(MODEL,physical=None,**options)
                transport.assert_not_called()

    def test_pg_public_factories_reject_capacity_override(self):
        physical=RayMapConfig('127.0.0.1:12345',1,1,MIB+24,2*MIB)
        for factory in (build_fixed_model_execution,ray_map_factory(physical)):
            for options in ({'max_request_bytes':4*MIB},{'item_input_bytes':4*MIB}):
                with self.subTest(options=options),patch(
                        'src.execution_provider.adapters.ray_map_transport.RayMapTransport') as transport:
                    with self.assertRaises(TypeError):factory(MODEL,**options)
                    transport.assert_not_called()

    def test_bad_ray_window_and_preparation_fail_before_transport_creation(self):
        stage=StageBrokerLimits(MIB,8*MIB+64,2,1,3)
        values=(RayMapConfig('127.0.0.1:12345',1,1,4*MIB-1,8*MIB),
                RayMapConfig('127.0.0.1:12345',1,1,4*MIB+24,8*MIB+64,preparation=stage),
                RayMapConfig('127.0.0.1:12345',1,1,4*MIB+24,8*MIB,
                             preparation=replace(stage,encoded_bytes=4*MIB+24,ready_bytes=8*MIB)))
        for physical in values:
            with self.subTest(physical=physical),patch(
                    'src.execution_provider.adapters.ray_map_transport.RayMapTransport') as transport:
                with self.assertRaises(ValueError):
                    build_native_execution(MODEL,physical=physical,max_request_bytes=4*MIB,input_bytes=8*MIB)
                transport.assert_not_called()

    def test_boundary_and_intermediate_payloads_preserve_bytes(self):
        for size in (MIB,2516582,8*MIB):
            with self.subTest(size=size):
                execution,calls=self.make(item=8*MIB,total=8*MIB,held=1,active=1)
                flow=NativeTaskSession(execution,'capacity','opaque-http')
                task=offered(0,size)
                try:
                    self.assertEqual(flow.offer((task,)).accepted_prefix_count,1)
                    delivery=poll(flow).deliveries[0]
                    response=decode_full_response(delivery.result)
                    self.assertEqual(response.body,ERROR)
                    self.assertEqual(response.status_code,429)
                    self.assertEqual([v for k,v in response.headers if k=='x-duplicate'],['one','two'])
                    self.assertEqual(hashlib.sha256(calls[0]).digest(),hashlib.sha256(task.payload).digest())
                    self.assertEqual(len(calls[0]),size)
                    flow.release((delivery.lease_id,));flow.end_input()
                    self.assertEqual(flow.advance(1).state,State.FINISHED)
                    flow.close(clean=True)
                    self.assertEqual(execution.engine.capacity.usage(),Usage())
                    self.assertFalse(execution.engine.jobs.jobs)
                    self.assertTrue(execution.close())
                finally:
                    flow.close();execution.engine.backend.close()

    def test_eight_mib_plus_one_is_rejected_before_send(self):
        execution,calls=self.make(item=8*MIB,total=8*MIB,held=1,active=1)
        flow=NativeTaskSession(execution,'oversized','opaque-http')
        try:
            outcome=flow.offer((offered(0,8*MIB+1),))
            self.assertEqual(outcome.accepted_prefix_count,0)
            self.assertNotEqual(outcome.status,'BACKPRESSURE')
            self.assertEqual(calls,[])
            self.assertEqual(execution.engine.capacity.usage(),Usage())
        finally:
            flow.close();self.assertTrue(execution.close())

    def test_total_input_partial_prefix_resends_suffix_and_reorders_by_identity(self):
        release=threading.Event()
        active=peak=0
        async def handler(request):
            nonlocal active,peak
            active+=1;peak=max(peak,active)
            try:
                if request.content[0]==ord('0'):
                    while not release.is_set():await asyncio.sleep(.002)
            finally:active-=1
        execution,calls=self.make(handler=handler)
        flow=NativeTaskSession(execution,'prefix','opaque-http')
        tasks=tuple(offered(i,3*MIB) for i in range(3))
        order=[]
        try:
            self.assertEqual(flow.offer(tasks).accepted_prefix_count,2)
            self.assertEqual(flow.offer(tasks[2:]).status,'BACKPRESSURE')
            first=poll(flow).deliveries[0]
            self.assertEqual(first.key.sequence,1);order.append(1)
            self.assertEqual(first.info.row_sequence,101)
            flow.release((first.lease_id,))
            release.set()
            second=poll(flow).deliveries[0]
            self.assertEqual(second.key.sequence,0);order.append(0)
            flow.release((second.lease_id,))
            self.assertEqual(flow.offer(tasks[2:]).accepted_prefix_count,1)
            third=poll(flow).deliveries[0]
            order.append(third.key.sequence);flow.release((third.lease_id,));flow.end_input()
            self.assertEqual(order,[1,0,2])
            self.assertLessEqual(peak,2)
            self.assertCountEqual(calls,[t.payload for t in tasks])
            self.assertEqual(flow.advance(1).state,State.FINISHED)
            flow.close(clean=True)
            self.assertEqual(execution.engine.capacity.usage(),Usage())
            self.assertFalse(execution.engine.jobs.jobs)
            self.assertTrue(execution.close())
        finally:
            release.set();flow.close();drain(execution);execution.engine.backend.close()

    def test_large_cancel_retains_borrow_until_late_completion(self):
        release=threading.Event();entered=threading.Event()
        async def handler(request):
            entered.set()
            while not release.is_set():await asyncio.sleep(.002)
        execution,calls=self.make(handler=handler)
        flow=NativeTaskSession(execution,'cancel','opaque-http')
        try:
            flow.offer((offered(0,3*MIB),));flow.advance(1)
            self.assertTrue(entered.wait(2))
            report=flow.close()
            self.assertEqual(report.uncertain_requests,1)
            self.assertEqual(execution.engine.capacity.usage().input_bytes,3*MIB)
            self.assertFalse(execution.close())
            release.set();drain(execution)
            self.assertEqual(execution.engine.capacity.usage(),Usage())
            self.assertFalse(execution.engine.jobs.jobs)
            self.assertTrue(execution.close())
        finally:
            release.set();flow.close();drain(execution);execution.engine.backend.close()

    def test_large_unknown_transport_error_cannot_be_claimed_clean(self):
        async def fail(request):raise OSError('fixture unknown remote completion')
        execution,calls=self.make(handler=fail)
        flow=NativeTaskSession(execution,'unknown','opaque-http')
        try:
            flow.offer((offered(0,3*MIB),))
            self.assertEqual(poll(flow).state,State.FAILED)
            self.assertEqual(flow.close().uncertain_requests,1)
            self.assertEqual(len(calls),1)
            self.assertEqual(execution.engine.capacity.usage().input_bytes,3*MIB)
            self.assertFalse(execution.close())
        finally:
            flow.close();execution.engine.backend.close()


@unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_LARGE_RAY_TEST')=='1' and
                     all(importlib.util.find_spec(n) for n in ('ray','pyarrow')),
                     'explicit actual CPU Ray qualification is not selected')
class NativeLargeRayTests(unittest.TestCase):
    def test_real_arrow_payloads_and_prefix_release_preserve_complete_calls(self):
        import ray
        root=Path(os.environ['SEMLOOM_NATIVE_LARGE_RAY_TMP'])
        if len(str(root).encode())>40 or root.exists() or ray.is_initialized():
            raise ValueError('large native Ray fixture needs one new short owned runtime')
        root.mkdir(parents=True)
        release=threading.Event();calls=[];lock=threading.Lock()
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_POST(self):
                body=self.rfile.read(int(self.headers['Content-Length']))
                with lock:calls.append(body)
                if body[0]==ord('0') and not release.wait(15):return
                self.send_response(429)
                self.send_header('Content-Length',str(len(ERROR)))
                self.send_header('x-duplicate','one');self.send_header('x-duplicate','two')
                self.end_headers();self.wfile.write(ERROR)
        http=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=http.serve_forever,daemon=True);thread.start()
        execution=flow=None;events=[]
        try:
            context=ray.init(address='local',num_cpus=4,num_gpus=0,include_dashboard=False,
                object_store_memory=512*MIB,_node_ip_address='127.0.0.1',_temp_dir=str(root),
                runtime_env={'env_vars':{'PYTHONPATH':str(Path(__file__).resolve().parents[2]),
                                        'OMP_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1'}})
            self.assertEqual(ray.cluster_resources().get('GPU',0),0)
            physical=RayMapConfig(context.address_info['address'],2,1,4*MIB+24,8*MIB,
                                  payload_backend='arrow')
            config=replace(MODEL,endpoint_url=f'http://localhost:{http.server_port}/fixture',timeout_ms=20000)
            execution=build_native_execution(config,physical=physical,max_request_bytes=4*MIB,
                input_bytes=8*MIB,max_tasks=3,max_active_requests=2,observer=events.append)
            flow=NativeTaskSession(execution,'real-ray','opaque-http')
            tasks=tuple(offered(i,3*MIB) for i in range(3))
            self.assertEqual(flow.offer(tasks).accepted_prefix_count,2)
            self.assertEqual(flow.offer(tasks[2:]).status,'BACKPRESSURE')
            def consume(sequence):
                delivery=poll(flow).deliveries[0]
                self.assertEqual(delivery.key.sequence,sequence)
                self.assertEqual(delivery.info.row_sequence,delivery.key.sequence+100)
                response=decode_full_response(delivery.result)
                self.assertEqual((response.status_code,response.body),(429,ERROR))
                self.assertEqual([v for k,v in response.headers if k=='x-duplicate'],['one','two'])
                lease=delivery.lease_id
                del response,delivery
                flow.release((lease,))
            consume(1);release.set()
            consume(0)
            self.assertEqual(flow.offer(tasks[2:]).accepted_prefix_count,1)
            consume(2);flow.end_input()
            self.assertEqual(flow.advance(1).state,State.FINISHED)
            flow.close(clean=True)
            flow=NativeTaskSession(execution,'ray-boundary','opaque-http')
            boundary=offered(0,4*MIB)
            self.assertEqual(flow.offer((boundary,)).accepted_prefix_count,1)
            delivery=poll(flow).deliveries[0]
            self.assertEqual(decode_full_response(delivery.result).body,ERROR)
            lease=delivery.lease_id
            del delivery
            flow.release((lease,));flow.end_input();flow.close(clean=True)
            self.assertCountEqual([hashlib.sha256(x).hexdigest() for x in calls],
                                  [hashlib.sha256(t.payload).hexdigest() for t in (*tasks,boundary)])
            self.assertEqual(execution.engine.capacity.usage(),Usage())
            self.assertFalse(execution.engine.jobs.jobs)
            self.assertTrue(execution.close());execution=None
            self.assertTrue(any(e['event']=='ray_transport_closed' and e['confirmed'] for e in events))
        finally:
            release.set()
            if flow is not None:flow.close()
            if execution is not None:
                drain(execution);execution.engine.backend.close()
            ray.shutdown();http.shutdown();http.server_close();thread.join(2)
        self.assertFalse(ray.is_initialized())


if __name__=='__main__':unittest.main()
