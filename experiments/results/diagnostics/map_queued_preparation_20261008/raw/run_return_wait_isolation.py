"""Bounded local return-path controls; genuine Core, substitute vendors and worker."""
import argparse
import asyncio
from dataclasses import asdict, dataclass, replace
import gzip
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import time
from unittest.mock import patch

from src.experiments import map_observation_probe as fixture
from src.experiments.async_request_guard import ThreadedRequestGuard
from src.scheduling.runtime.stage_broker import StageBrokerLimits


@dataclass(frozen=True)
class Profile:
    path: str
    guard: str
    guard_delay_s: float
    prepare_s: float = .0005
    prepare_kind: str = 'sleep'
    recording: str = 'memory'


def cost(seconds, kind):
    if kind == 'sleep':
        time.sleep(seconds)
    else:
        until=time.perf_counter()+seconds
        value=0
        while time.perf_counter()<until:
            value=(value+1)%997


def run_case(output, profile, *, rows=128, capacity=8):
    transports, holders, ready = [], [], []
    original_guard, original_ray = fixture._guard_remote_request, fixture.SyntheticRay
    original_transport, original_capture = fixture.RayMapTransport, fixture.EventCapture
    original_build=fixture.build_fixed_model_execution

    class DelayedUnit:
        def __init__(self, unit):self.unit=unit
        def reserve(self,digest):
            time.sleep(profile.guard_delay_s)
            return self.unit.reserve(digest)

    def guard(task,unit,observe,record):
        if profile.guard=='threaded':
            if not holders:
                holders.append(ThreadedRequestGuard(DelayedUnit(unit),observe,record))
            return holders[0](task)
        return original_guard(task,DelayedUnit(unit),observe,record)

    class Ray(original_ray):
        def _submit(self,table,index,template):
            sequence=template.key.sequence
            if sequence not in self.charged:raise AssertionError('call before accounting')
            self.issued.append(sequence)
            future=asyncio.run_coroutine_threadsafe(self._execute(table,index,template),self.loop)
            future.add_done_callback(lambda _:ready.append((sequence,time.monotonic_ns())))
            return asyncio.wrap_future(future)

    class Transport(original_transport):
        def __init__(self,*args,**kwargs):
            kwargs['physical']=replace(kwargs['physical'],window_bytes=2**21,object_bytes=2**22,
                preparation=StageBrokerLimits(2**21,2**22,128,1,128) if profile.path=='coalesced' else None,
                coalesce_queued_preparation=profile.path=='coalesced')
            super().__init__(*args,**kwargs)
            transports.append(self)

    class Capture(original_capture):
        def __init__(self,maximum,writer=None):super().__init__(max(maximum,64*rows+1024),writer)

    def build(*args,**kwargs):
        kwargs['max_tasks']=min(128,kwargs['max_tasks'])
        if profile.path=='coalesced':
            kwargs['preparation_factory']=lambda transport,limits,notify:transport.prepare_inputs(limits,notify)
        return original_build(*args,**kwargs)

    def batches(data,limits,*,batch_rows,backend):
        assert backend=='arrow' and len(data)<=limits.rows
        assert sum(len(row[2])+24 for row in data)<=limits.bytes
        transports[0].observer(dict(event='fixture_window',rows=len(data)))
        for start in range(0,len(data),batch_rows):
            cost(profile.prepare_s,profile.prepare_kind)
            yield fixture.SyntheticTable(data[start:start+batch_rows])

    try:
        with patch.object(fixture,'_guard_remote_request',guard),patch.object(fixture,'SyntheticRay',Ray), \
             patch.object(fixture,'RayMapTransport',Transport),patch.object(fixture,'EventCapture',Capture), \
             patch.object(fixture,'build_fixed_model_execution',build), \
             patch('src.execution_provider.adapters.map_preparation.iter_payload_batches',batches), \
             patch.object(fixture,'PREPARE_S',profile.prepare_s), \
             patch.object(fixture,'SERVICE_S',.001 if rows<=8 else .020):
            # The immediate materializer uses the same cost-kind control.
            def instant_batches(data,limits,*,batch_rows,backend):
                yield from batches(data,limits,batch_rows=batch_rows,backend=backend)
            with patch('src.execution_provider.adapters.ray_map_transport.iter_payload_batches',instant_batches):
                # run_case owns its own immediate batch replacement, so apply its
                # cost-kind via the fixture PREPARE_S control; CPU profiles only
                # select the explicit preparation path.
                result=fixture.run_case(output,'memory',profile.recording,rows=rows,
                    capacity=min(capacity,rows),batch_rows=min(8,capacity,rows))
    finally:
        for holder in holders:holder.__exit__(None,None,None)
    with gzip.open(output/'events.jsonl.gz','rt') as stream:events=[json.loads(line) for line in stream]
    receipts=[e for e in events if e['event']=='ray_http_completed']
    available=dict(ready)
    assert len(available)==len(ready)==len(receipts)==rows
    waits=[]
    for event in receipts:
        at=available[event['key']['sequence']]
        assert event['worker_ended_ns']<=at<=event['received_ns']
        waits.append((event['received_ns']-at)/1e6)
    result.update(profile=asdict(profile),mean_future_ready_to_receive_ms=statistics.mean(waits),
        median_future_ready_to_receive_ms=statistics.median(waits),
        prepared_blocks=sum(e['event']=='ray_preparation_completed' for e in events),
        future_ready_records=[dict(sequence=sequence,future_ready_ns=at) for sequence,at in ready],
        scope='same-process fake Ray; separate synthetic worker loop; actual Core/Map coroutine and memory accounting API')
    assert result['exactly_once'] and result['resources_drained']
    assert result['synthetic_calls']==result['accounted_calls']==rows
    assert result['core_active_peak']<=capacity
    (output/'probe.json').write_text(json.dumps(result,indent=2)+'\n')
    return result


def main(output):
    output.mkdir(exist_ok=False)
    begin=time.monotonic()
    profiles=[Profile(path,guard,.006) for path in ('immediate','coalesced') for guard in ('sync','threaded')]
    profiles += [Profile('coalesced',guard,.006,.005,kind) for kind in ('sleep','cpu') for guard in ('sync','threaded')]
    profiles += [Profile('coalesced',guard,0) for guard in ('sync','threaded')]
    cases=[]
    for phase,repeat,rows in [('check',0,16),('measurement',1,128),('measurement',2,128),('measurement',3,128)]:
        ordered=list(enumerate(profiles))
        if repeat%2==0:ordered.reverse()
        for index,profile in ordered:
            if time.monotonic()-begin>=360:raise TimeoutError('finite local suite elapsed')
            name=f'{phase}-{repeat}-{index}'
            result=run_case(output/name,profile,rows=rows)
            cases.append(dict(case=name,phase=phase,repeat=repeat,rows=rows,profile=asdict(profile),result=result))
            print(json.dumps(dict(case=name,query_seconds=result['query_seconds'],
                ready_to_receive_ms=result['mean_future_ready_to_receive_ms'],status='passed')),flush=True)
    assert sum(c['rows'] for c in cases)==4000
    summary=dict(status='passed',cases=cases,http_requests=0,model_requests=0,pg_queries=0,
        simulated_calls=4000,elapsed_seconds=time.monotonic()-begin,
        source_commit=subprocess.check_output(['git','rev-parse','HEAD']).decode().strip(),
        probe_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        limitations=['no real vendor/Ray/SQL/GPU calls','CPU control shares the GIL with a synthetic worker; not an independent actor process'])
    (output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    main(args.output)
