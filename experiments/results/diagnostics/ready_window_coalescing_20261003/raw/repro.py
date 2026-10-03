import asyncio
from dataclasses import asdict
import json
from types import SimpleNamespace
from unittest.mock import patch

from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport, _RemoteResult
from tests.execution_provider.test_ray_map_transport import FakeRay, task


class Scalar:
    def __init__(self, value): self.value=value
    def as_py(self): return self.value


class Table:
    def __init__(self, rows): self.rows=rows; self.num_rows=len(rows)
    def __getitem__(self, name):
        index={'session_id':0,'sequence':1,'payload':2}[name]
        return [Scalar(row[index]) for row in self.rows]
    def get_total_buffer_size(self): return sum(len(row[2])+24 for row in self.rows)


async def scenario():
    first, all_started, release=asyncio.Event(),asyncio.Event(),asyncio.Event()
    windows=[]; begun=[]
    async def execute(table,index,template,*args):
        begun.append(template.key.sequence)
        first.set()
        if len(begun)==5: all_started.set()
        await release.wait()
        return _RemoteResult(template.key,b'ok',1,2,None)
    def batches(rows,limits,*,batch_rows,backend):
        windows.append(len(rows))
        for offset in range(0,len(rows),batch_rows): yield Table(rows[offset:offset+batch_rows])
    physical=SimpleNamespace(**(asdict(RayMapConfig('fixture-cluster',1,4,1024,2048))|{'ready_wait_ms':1.0}))
    transport=RayMapTransport(FixedModelConfig('http://localhost/fixture','model',1000),8,
                              physical=physical,ray_api=FakeRay(execute))
    with patch('src.execution_provider.adapters.ray_map_transport.iter_payload_batches',batches):
        tasks=[asyncio.create_task(transport.execute(task(0),'model'))]
        await asyncio.wait_for(first.wait(),1)
        await transport.flusher
        tasks.append(asyncio.create_task(transport.execute(task(1),'model')))
        await asyncio.sleep(0)
        tasks.extend(asyncio.create_task(transport.execute(task(i),'model')) for i in range(2,5))
        try:
            await asyncio.wait_for(all_started.wait(),1)
        finally:
            release.set()
        output=await asyncio.gather(*tasks)
        await transport.close()
    assert output==[b'ok']*5 and transport.used_bytes==0 and not transport.blocks
    return windows


async def main():
    repeats=[await scenario() for _ in range(3)]
    print(json.dumps({'windows':repeats,'http_requests':0,'model_requests':0,'resources_drained':True}))
    assert all(len(windows)<=2 for windows in repeats),'fragmented preparation while another request is in flight'


asyncio.run(main())
