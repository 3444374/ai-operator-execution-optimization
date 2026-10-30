"""Actual bulk owner/actor/transport, synthetic IPC/Futures; no Ray RPC or model."""
import argparse
from collections import Counter
from concurrent.futures import Future
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sys
import subprocess
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('public',type=Path);parser.add_argument('output',type=Path)
parser.add_argument('mode',choices=['off','on'])
args=parser.parse_args();out=args.output;out.mkdir(parents=True,exist_ok=False)
root=Path(__file__).resolve().parents[2]/'scripts/profiling/record_observation'
(out/'config.json').write_text(json.dumps(dict(observation=dict(mode=args.mode,profile='handoff',
    max_events_per_process=400,max_event_bytes=65536))))
os.environ.update(SEM_RECORD_OBSERVATION=args.mode,SEM_RECORD_OBSERVER_ROOT=str(out),
    SEM_RECORD_OBSERVER_CODE=str(args.public/'code'),SEM_RECORD_RUN_ID='CPU-batch-handoff',
    SEM_RECORD_SESSION_ID='CPU-'+args.mode,LITELLM_LOCAL_MODEL_COST_MAP='True')
sys.path[:0]=[str(args.public/'code'),str(root)]
from src.execution_provider.adapters import lotus_records_async_actor as native
if not hasattr(native.AsyncCoreModelActor,'poll_prepared_batch'):
    report=dict(status='not_supported_by_target',mode=args.mode,bulk_handoff_qualified=False,
        real_Ray_RPC=False,real_model_calls=0,GPU=False)
    (out/'PUBLIC-BATCH-HANDOFF.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report));sys.exit(0)
from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
from src.semantic_methods.lotus.record_chain import RecordOccurrence,stage_identity
from src.serving.backends.vllm_engine import EngineSpec,GenerationSpec,_prepared,result_record
from src.serving.backends.async_engine_process import AsyncEngineLimits
from src.scheduling.core.session_contract import Acceptance,BatchKey,BatchMember,TaskKey,Terminal
import record_observer as observer
observer.install_driver()
from record_ipc_observer import ObservedAsyncCoreModelActor

secret='CPU_PRIVATE_BATCH_PAYLOAD_9281'
rows=[dict(stage_identity('CPU-batch',RecordOccurrence('row-'+str(i),i,0,{}),'map0'),
           messages_json=json.dumps([dict(role='user',content=secret+str(i))]))for i in range(3)]
generation=GenerationSpec(max_tokens=3);prepared=_prepared(rows)[0]
replies=[]
for value in prepared:
    result=result_record(value,engine_uid='CPU-engine',start=1,end=2,status='succeeded',submitted=True)
    result.update(text=secret+'x'*2800,prompt_tokens=1,completion_tokens=1,total_tokens=2,
                  engine_terminal_confirmed=True)
    assert len(json.dumps(result).encode())<4096
    replies.append(result)
model=out/'CPU-model.json';model.write_text('{}')
limits=AsyncEngineLimits(max_inflight=3,result_slots=3,item_result_bytes=4096,
                         result_bytes=3*4096,control_frame_bytes=4096)
engine=EngineSpec(str(model),sys.executable,sys.executable,max_num_seqs=1)
actor=ObservedAsyncCoreModelActor(asdict(engine),asdict(generation),1,asdict(limits))
submitted=[];events=[]
class IPC:
    def submit_batch(self,actual_rows,**kwargs):
        assert actual_rows==rows and kwargs['generation']==generation
        submitted.append(actual_rows)
        events.extend(SimpleNamespace(request_id='ticket-'+str(i),result=value,
            engine_terminal_confirmed=True,error_type=None)for i,value in enumerate(replies))
        return SimpleNamespace(status='queued',request_ids=tuple('ticket-'+str(i)for i in range(3)))
    def poll(self,maximum):
        result=events[:maximum];del events[:maximum];return result
actor.engine=IPC()
references=[];poll_pages=[];ack_attempts=Counter();acked=[];native_methods=[]
class Reference:
    def __init__(self,value,label):
        self.value=Future();self.value.set_result(value);self.label=label;self.calls=0
        references.append(self)
    def future(self):self.calls+=1;return self.value
class Remote:
    def __init__(self,method,label):self.method=method;self.label=label
    def remote(self,*a,**k):
        native_methods.append(self.label)
        return Reference(self.method(*a,**k),self.label)
def poll(key):
    value=actor.poll_prepared_batch(key);poll_pages.append(json.loads(value));return value
def ack(key):
    key=tuple(key);ack_attempts[key]+=1
    if key==(1,0)and ack_attempts[key]==1:return False  # First ACK never reaches actor.
    value=actor.acknowledge_core(key)
    if value:acked.append(key)
    return value
owner=native.AsyncRecordsActorOwner.__new__(native.AsyncRecordsActorOwner)
owner.limits=limits;owner.model_actor=SimpleNamespace(
    submit_prepared_batch=Remote(actor.submit_prepared_batch,'submit'),
    poll_prepared_batch=Remote(poll,'poll'),acknowledge_core=Remote(ack,'ack'))
notified=threading.Event();received=[]
backend=LotusChainActorBackend(owner,3,notified.set,deadline_monotonic=time.monotonic()+20,
    on_result=lambda key,value:received.append((key,value)),bulk=True)
backend.poll_interval_s=.001  # Normally supplied by the owning Core session.
group=BatchKey(1,7)
tasks=tuple(SimpleNamespace(key=TaskKey(1,i),member=BatchMember(group,i,3),task=SimpleNamespace(
    payload=json.dumps(dict(row=row,generation=asdict(generation),parent_call_ids=[])).encode(),
    max_result_bytes=4096,profile_name='map0',estimated_work=1))for i,row in enumerate(rows))
try:
    outcome=backend.try_submit_batch(tasks,'local');assert outcome.acceptance==Acceptance.ACCEPTED
    pending=dict(zip((t.key for t in tasks),outcome.handles));results={};deadline=time.monotonic()+10
    while pending:
        for event in backend.poll(tuple(pending.items()),3):
            assert type(event)is Terminal,(event,repr(backend.backend._slots[event.key].future.exception()))
            results[event.key]=json.loads(event.result);pending.pop(event.key)
        assert time.monotonic()<deadline,(pending,poll_pages)
        notified.wait(.01);notified.clear()
finally:assert backend.close(3)
assert len(submitted)==1 and native_methods.count('submit')==1
assert [results[TaskKey(1,i)]for i in range(3)]==replies
assert len(poll_pages)>=2 and all(len(p['results'])<3 for p in poll_pages)
page_keys=[tuple(v['request_key'])for page in poll_pages for v in page['results']]
assert page_keys.count((1,0))==2 and set(page_keys)=={(1,i)for i in range(3)}
assert Counter(acked)==Counter({(1,i):1 for i in range(3)})and not actor._held
assert len(received)==3 and not backend._bulk and backend.retained_refs()==0
close=dict(status='closed',producer_quiescence=dict(schema='semloom.async_actor_quiescence.v1',
    confirmed=True,input_gate_closed=True,active_readers=0))
with patch.object(native.AsyncCoreModelActor,'close',return_value=close):assert actor.close(1)is close
observer.close_events()
markers=[json.loads(line)for p in(out/'events').glob('*.jsonl')for line in p.read_text().splitlines()]
assert secret not in json.dumps(markers)
expected={'actor_result_return':3,'driver_future_done':3,'driver_result_audit_enter':3,
          'Ray_result_to_Core':3,'driver_batch_future_done':len(poll_pages)}
(out/'OBSERVED-COUNTS.json').write_text(json.dumps(dict(
    event_counts={k:sum(e['event']==k for e in markers)for k in expected},
    members=3,poll_RPCS=len(poll_pages),page_member_occurrences=len(page_keys),
    business_results_and_ACKs_passed=True),indent=2)+'\n')
for name,count in expected.items():
    assert sum(e['event']==name for e in markers)==(count if args.mode=='on'else 0),name
assert all(r.calls==(2 if args.mode=='on'else 1)for r in references if r.label=='poll')
if args.mode=='on':
    for name in ('actor_result_return','driver_future_done','Ray_result_to_Core'):
        found=[e for e in markers if e['event']==name]
        assert {tuple(e['key'][n]for n in('session_id','sequence'))for e in found}=={(1,i)for i in range(3)}
        assert all(e['batch_key']==asdict(group)for e in found),name
        assert {e['identities'][0]['call_id']for e in found}=={r['call_id']for r in rows}
    status=json.loads(next((out/'events').glob('*.status.json')).read_text())
    assert status['flush_confirmed']and status['errors']==status['dropped']==0
report=dict(status='passed_actual_public_bulk_owner_actor_transport',mode=args.mode,
    bulk_handoff_qualified=True,members=3,data_batch_RPCS=1,poll_RPCS=len(poll_pages),
    returned_page_member_occurrences=len(page_keys),first_actor_member_markers=3 if args.mode=='on'else 0,
    lost_ACK_replayed_without_duplicate_member_completion=True,original_generation_and_rows_preserved=True,
    outputs_preserved=True,native_ACK_once_per_member=True,native_close_report_identity_preserved=True,
    event_counts={k:sum(e['event']==k for e in markers)for k in expected},
    output_sha256=hashlib.sha256(json.dumps(replies,sort_keys=True).encode()).hexdigest(),
    real_Ray_RPC=False,real_model_calls=0,GPU=False)
report['public_source']=subprocess.check_output(['git','rev-parse','HEAD'],cwd=args.public,text=True).strip()
(out/'PUBLIC-BATCH-HANDOFF.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
