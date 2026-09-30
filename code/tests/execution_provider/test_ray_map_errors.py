"""A remote HTTP exception must reach diagnostics without becoming a receipt."""
import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import httpx

from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport, _HttpActor, _Block, _Row
from src.experiments.buffered_events import compact_event
from src.scheduling.core.session_contract import BackendTask, OfferedTask, SessionSpec, TaskKey
from tests.execution_provider.test_ray_map_transport import FakeRay


class RayMapErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_first_payload_and_submit_are_recorded_once_without_loading_daft(self):
        class Table(dict):
            num_rows=1
            def get_total_buffer_size(self):return 64
        for failed in (False,True):
            with self.subTest(failed=failed):
                events=[]
                def batches(rows, *args, **kwargs):
                    if failed:raise ValueError('fixture payload preparation failure')
                    for sid,sequence,payload in rows:
                        yield Table({name:[SimpleNamespace(as_py=lambda value=value:value)]
                            for name,value in (('session_id',sid),('sequence',sequence),('payload',payload))})
                async def execute(table,index,template):return template.key,b'ok',1,2
                ray=FakeRay(execute)
                transport=RayMapTransport(FixedModelConfig('http://localhost/fixture','model',1000),4,
                    events.append,physical=RayMapConfig('fixture-cluster',1,1,1024,2048),ray_api=ray)
                requests=[BackendTask(TaskKey(0,i),SessionSpec('job','flow','fixture'),
                          OfferedTask(i,b'input',1,1024)) for i in range(2)]
                with patch('src.execution_provider.adapters.ray_map_transport.iter_payload_batches',side_effect=batches):
                    try:
                        results=await asyncio.gather(*(transport.execute(request,'model') for request in requests))
                        if failed:self.assertTrue(all(b'MODEL_UNAVAILABLE' in value for value in results))
                        else:self.assertEqual(results,[b'ok',b'ok'])
                    finally:await transport.close()
                payload=[e for e in events if e.get('stage')=='first_payload']
                self.assertEqual(len(payload),1)
                self.assertEqual(payload[0]['status'],'failed' if failed else 'completed')
                self.assertGreaterEqual(compact_event(payload[0])['elapsed_seconds'],0)
                self.assertEqual(sum(e['event']=='ray_first_submit' for e in events),0 if failed else 1)
                self.assertEqual(transport.used_bytes,0)
                self.assertEqual(len(ray.killed),1)

    async def test_rpc_and_result_errors_keep_their_distinct_stages(self):
        def submit_failure(*_):
            raise ConnectionError('private RPC address')
        async def await_failure(*_):
            raise OSError('private worker details')
        async def wrong_key(*_):
            return TaskKey(99,0), b'ok', 1, 2
        for execute,stage,kind in ((submit_failure,'ray_submit','builtins.ConnectionError'),
                                   (await_failure,'ray_await','builtins.OSError'),
                                   (wrong_key,'result_validation','builtins.ValueError')):
            with self.subTest(stage=stage):
                events=[];ray=FakeRay(execute)
                config=FixedModelConfig('http://localhost/model','fixture',1000)
                transport=RayMapTransport(config,4,events.append,
                    physical=RayMapConfig('fixture-cluster',1,2,1024,2048),ray_api=ray)
                request=BackendTask(TaskKey(0,0),SessionSpec('job','flow','fixture'),
                                    OfferedTask(0,b'input',1,1024))
                row=_Row(request,asyncio.get_running_loop().create_future())
                transport.blocks[0]=_Block(None,64,{request.key});transport.used_bytes=64
                try:
                    await transport._run_row(row,0,0)
                    with self.assertRaisesRegex(RuntimeError,'unconfirmed'):
                        await row.future
                    failure=[e for e in events if e['event']=='ray_execution_error']
                    self.assertEqual(len(failure),1)
                    self.assertEqual(failure[0]['stage'],stage)
                    self.assertEqual(failure[0]['reason']['exception_type'],kind)
                    self.assertNotIn('private',json.dumps(failure))
                    self.assertEqual(transport.used_bytes,64)
                finally:
                    with self.assertRaisesRegex(RuntimeError,'unconfirmed'):
                        await transport.close()
                self.assertEqual(len(ray.killed),1)

    async def test_remote_http_failure_keeps_cause_key_and_unknown_charge(self):
        key = TaskKey(0, 319)
        request = BackendTask(key, SessionSpec('job', 'flow', 'fixture'),
                              OfferedTask(319, b'', 1, 1024))
        class Value:
            def __init__(self, value): self.value = value
            def as_py(self): return self.value
        table = {name:[Value(value)] for name,value in (
            ('session_id',0), ('sequence',319), ('payload',b'private prompt'))}
        config = FixedModelConfig('http://localhost/model', 'fixture', 1000)
        actor = _HttpActor(config, 128)
        calls = []
        async def fail(message):
            calls.append(message)
            try:
                raise OSError('private socket details')
            except OSError as cause:
                raise httpx.ReadError('private endpoint and response details') from cause
        actor.transport._client = httpx.AsyncClient(transport=httpx.MockTransport(fail))
        events = []
        ray = FakeRay(actor.execute)
        transport = RayMapTransport(config,128,events.append,
            physical=RayMapConfig('fixture-cluster',1,16,1024,2048),ray_api=ray)
        row = _Row(request,asyncio.get_running_loop().create_future())
        transport.blocks[7] = _Block(table,541,{key});transport.used_bytes = 541
        try:
            await transport._run_row(row,7,0)
            with self.assertRaisesRegex(RuntimeError,'unconfirmed'):
                await row.future
            self.assertEqual(len(calls),1)
            self.assertEqual(transport.unknown,{key})
            self.assertEqual(transport.used_bytes,541)
            failure = [e for e in events if e['event']=='ray_execution_error']
            self.assertEqual(len(failure),1)
            self.assertEqual(failure[0]['key'],{'session_id':0,'sequence':319})
            self.assertEqual(failure[0]['stage'],'http')
            self.assertEqual(failure[0]['reason']['exception_type'],'httpx.ReadError')
            self.assertEqual(failure[0]['reason']['cause_types'],['builtins.OSError'])
            self.assertNotIn('private',json.dumps(failure))
            self.assertNotIn('/',failure[0]['reason']['origin']['file'])
            public = compact_event(failure[0])
            self.assertEqual(public['stage'],'http')
            self.assertEqual(public['remote_outcome'],'unconfirmed')
        finally:
            with self.assertRaisesRegex(RuntimeError,'unconfirmed'):
                await transport.close()
            await actor.close()
        self.assertEqual(len(ray.killed),1)
