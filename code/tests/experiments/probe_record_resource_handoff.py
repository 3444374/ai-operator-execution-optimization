"""Actual2212 actor reader guard and driver _execute; synthetic Future, no Ray RPC."""
import argparse
import asyncio
from concurrent.futures import Future
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('public',type=Path);parser.add_argument('output',type=Path);parser.add_argument('mode',choices=['off','on'])
args=parser.parse_args();root=Path(__file__).resolve().parents[2]/'scripts/profiling/record_observation';out=args.output;out.mkdir(parents=True,exist_ok=False)
(out/'config.json').write_text(json.dumps(dict(observation=dict(mode=args.mode,profile='handoff',
    max_events_per_process=200,max_event_bytes=65536,max_pending_frame_metadata=4))))
os.environ.update(SEM_RECORD_OBSERVATION=args.mode,SEM_RECORD_OBSERVER_ROOT=str(out),
    SEM_RECORD_OBSERVER_CODE=str(args.public/'code'),SEM_RECORD_RUN_ID='CPU-handoff',
    SEM_RECORD_SESSION_ID='CPU-'+args.mode,LITELLM_LOCAL_MODEL_COST_MAP='True')
sys.path[:0]=[str(args.public/'code'),str(root)]
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.execution_provider.adapters.lotus_records_actor_owner import _PrepareActor as PublicPrepareActor
from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
from src.semantic_methods.lotus.record_chain import RecordOccurrence,stage_identity
from src.serving.backends.vllm_engine import EngineSpec,GenerationSpec,_prepared,result_record
from src.serving.backends.async_engine_process import AsyncEngineLimits
from src.scheduling.core.session_contract import TaskKey
import record_observer as observer
observer.install_driver()
from record_ipc_observer import ObservedAsyncCoreModelActor,ObservedPrepareActor

secret='CPU_PAYLOAD_MUST_NOT_BE_LOGGED_9281'
identity=stage_identity('CPU-handoff',RecordOccurrence('row',0,0,{'text':secret}),'map0')
row=dict(identity,messages_json=json.dumps([{'role':'user','content':secret}]))
prepared=_prepared([row])[0][0]
generation=GenerationSpec(max_tokens=3)
reply=result_record(prepared,engine_uid='CPU-engine',start=1,end=2,status='succeeded',submitted=True)
reply.update(text=secret,prompt_tokens=1,completion_tokens=1,total_tokens=2,engine_terminal_confirmed=True)
payload=json.dumps(reply).encode();model=out/'CPU-model.json';model.write_text('{}')
engine=EngineSpec(str(model),sys.executable,sys.executable,max_num_seqs=128)
actor=ObservedAsyncCoreModelActor(asdict(engine),asdict(generation),1,asdict(AsyncEngineLimits()))
native_calls=[];reader_at_return=[]
def native_run(instance,rows,deadline,maximum,request_key,actual_generation=None):
    native_calls.append((rows,request_key,actual_generation));reader_at_return.append(instance._readers)
    assert rows[0]==row and actual_generation==generation
    return payload

future=Future();future_calls=[]
class Reference:
    def future(self):future_calls.append(True);return future
ref=Reference();acked=[];submitted=[];receipts=[]
class Actors:
    def submit_prepared_core(self,rows,deadline,maximum,key,gen,token_cost):
        submitted.append((rows,gen,token_cost));value=actor.run_prepared_core(rows,deadline,maximum,
            [key.session_id,key.sequence],gen,token_cost)
        assert value is payload
        future.set_result(value);return ref
    def acknowledge_core(self,key):acked.append(key)
    def cancel_core(self,key):return None
backend=LotusChainActorBackend(Actors(),1,lambda:None,deadline_monotonic=time.monotonic()+30,
    on_result=lambda key,value:receipts.append((key,value)))
key=TaskKey(1,2)
task=SimpleNamespace(key=key,member=None,task=SimpleNamespace(payload=json.dumps(dict(row=row,generation=asdict(generation),parent_call_ids=[])).encode(),
    max_result_bytes=65536,profile_name='map0',estimated_work=1))
with patch.object(AsyncCoreModelActor,'_run_core',native_run):
    try:result=asyncio.run(backend._execute(task,'local'))
    finally:assert backend.close(3)
assert result is payload and len(native_calls)==1 and len(submitted)==1 and acked==[key]
assert reader_at_return==[1] and actor._readers==0 and len(receipts)==1
assert future_calls==[True]*(2 if args.mode=='on'else 1)
native_close=dict(status='closed',producer_quiescence=dict(schema='semloom.async_actor_quiescence.v1',
    confirmed=True,input_gate_closed=True,active_readers=0))
with patch.object(AsyncCoreModelActor,'close',lambda *a,**k:native_close):assert actor.close(1)is native_close
if args.mode=='on':
    status=json.loads(next((out/'events').glob('*.status.json')).read_text())
    assert status['closed']and status['flush_confirmed']and status['dropped']==0
observer.close_events()
events=[json.loads(line)for path in (out/'events').glob('*.jsonl')for line in path.read_text().splitlines()]
assert secret not in json.dumps(events)
expected=['actor_result_return','driver_future_done','driver_result_audit_enter','Ray_result_to_Core']
for name in expected:assert sum(e['event']==name for e in events)==(1 if args.mode=='on'else 0),name
if args.mode=='on':
    marker=next(e for e in events if e['event']=='actor_result_return');assert marker['key']==asdict(key)
    assert marker['identities']==[identity]
    import cloudpickle
    serialized=[]
    for cls,base in [(ObservedAsyncCoreModelActor,AsyncCoreModelActor),(ObservedPrepareActor,PublicPrepareActor)]:
        assert cls.__bases__==(base,)
        encoded=cloudpickle.dumps(cls);decoded=cloudpickle.loads(encoded);assert issubclass(decoded,base)
        serialized.append(dict(actor=cls.__name__,bytes=len(encoded),actual_public_base=base.__qualname__))
else:serialized=[]
report=dict(status='passed_actual_public_actor_guard_and_driver_execute',mode=args.mode,
    original_native_call_once=True,result_bytes_identity_preserved=True,original_generation_preserved=True,
    native_reader_guard_active_at_return=True,extra_future_subscriptions=1 if args.mode=='on'else 0,
    native_ack_once=True,original_actor_close_report_identity_preserved=True,
    actor_sink_finalized_before_owner_kill=args.mode=='on',observed_boundaries=[e['event']for e in events if e['event']in expected],
    output_sha256=hashlib.sha256(result).hexdigest(),serialized_classes=serialized,
    real_Ray_RPC=False,real_model_calls=0,GPU=False)
(out/'PUBLIC-HANDOFF.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
