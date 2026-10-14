"""Recompute published Q2 quality evidence with stdlib, without model/network."""
import base64,hashlib,json,tarfile
from collections import Counter
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from retained_evidence import read_bytes, read_json, open_archive

ROOT=Path(__file__).resolve().parent
proof=read_json(ROOT/'quality-verification.json');manifest=read_json(ROOT/'quality-members-sha256.json');archive=ROOT/'quality-observations.tar.gz'
assert hashlib.sha256(read_bytes(archive)).hexdigest()==proof['public_archive']['sha256']
with open_archive(archive) as t:raw={m.name:t.extractfile(m).read() for m in t if m.isfile()}
assert set(raw)==set(manifest['members']) and len(raw)==proof['public_archive']['members']
for n,v in manifest['members'].items():assert hashlib.sha256(raw[n]).hexdigest()==v['public_sha256'],n
load=lambda n:json.loads(raw[n]);rows=lambda n:[json.loads(l) for l in raw[n].splitlines()]
sends=rows('pre-send-records.jsonl');assert len(sends)==2160 and Counter(v['kind'] for v in sends)=={'gateway':720,'router':720,'model':720}
assert all(v['phase']=='quality' and v['status']==200 and v['returned_ns'] is not None and not v['error_type'] for v in sends)
model=load('model/result.json');watch=load('model-watchdog.json');gold=load('quality-gold-identity.json')
assert model['phases']['quality']['passed'] and model['model_calls']==720 and model['model_transmission_posts']==2160
assert model['error_type']=='TimeoutError' and not model['passed'] and watch['returncode']==1 and not watch['forced_cleanup']
assert not model['cleanup']['owned_survivors'] and not watch['cleanup']['owned_survivors'] and not model['gpu_after']
assert gold['positive_occurrences']==gold['original_gold_sql_rows']==14 and gold['gold_and_evaluator_source_verified']
assert gold['original_sembench_preflight']['sembench_commit']=='c814e3807e72d4cf876b852b17e77f3cc94575c2'
outputs=[];qualities=[];requests=[];traces=[];protocols=[]
for case in model['phases']['quality']['cases']:
 p='model/quality/round-'+str(case['round'])+'-position-'+str(case['position'])+'-'+case['arm']+'/'
 unit=p+'output/'+case['matrix']['cells'][0]['unit_id']+'/'
 q=load(unit+'summary.json');assert q['method_http_values_audit']['status']=='passed' and load(unit+'model-method-check.json')['passed']
 assert q['quality']['classification_audit']==dict(true_positive=7,false_positive=0,true_negative=106,false_negative=7,invalid=0,evaluated_rows=120,missing_rows=0,count_error_from_classifications=-7)
 assert q['quality']['original_metric']==dict(precision=1.,recall=1.,f1_score=1.)
 values=rows(unit+'protocols.jsonl');assert len(values)==120 and q['native_http_audit']['readable_json_responses']==120
 calls=load(unit+'calls.json');assert len(calls)==120 and len({(c['row_id'],c['stage_id'],c['stage_ordinal'],c['call_id']) for c in calls})==120
 requests.append(Counter(c['request_values_sha256'] for c in calls));outputs.append(load(unit+'filter-decisions.json'));qualities.append(q['quality'])
 protocols+=values;traces+=rows(unit+'http-trace.jsonl')
assert len(outputs)==6 and all(v==outputs[0] for v in outputs) and all(v==qualities[0] for v in qualities) and all(v==requests[0] for v in requests)
assert requests[0]==Counter(load('model-preparation/route-manifest.json')['request_weights'])
assert Counter(t['request_body_sha256'] for t in traces)==Counter(v['body_sha256'] for v in sends if v['kind']=='gateway')
assert len(rows('model/router-http-trace.jsonl'))==720 and all(t['status']=='completed' and t['retry_count']==0 and not t['errors'] for t in traces)
usage=dict(prompt=sum(p['response']['usage']['prompt_tokens'] for p in protocols),generation=sum(p['response']['usage']['completion_tokens'] for p in protocols))
assert usage==proof['response_usage']==dict(prompt=89556,generation=2880)
def metric(name,text):return sum(float(l.split()[-1]) for l in text.splitlines() if l.startswith(name+'{'))
ready=raw['model/quality/service-0/ready-metrics.txt'].decode();final=raw['model/quality/service-0/final-metrics.txt'].decode()
assert metric('vllm:request_success_total',ready)==0 and metric('vllm:request_success_total',final)==720
assert metric('vllm:prompt_tokens_total',final)-metric('vllm:prompt_tokens_total',ready)==89556
assert metric('vllm:generation_tokens_total',final)-metric('vllm:generation_tokens_total',ready)==2880
decoded=0
def inspect(v):
 global decoded
 if isinstance(v,dict):
  for k,item in v.items():
   if k in ('payload','response_body_base64','response_wire') and isinstance(item,str):base64.b64decode(item,validate=True).decode();decoded+=1
   else:inspect(item)
 elif isinstance(v,list):
  for x in v:inspect(x)
for n,b in raw.items():
 if n.endswith('.json'):inspect(json.loads(b))
 elif n.endswith('.jsonl'):
  for l in b.splitlines():inspect(json.loads(l))
assert decoded==proof['privacy']['decoded_payloads_scanned']
print(json.dumps(dict(passed=True,source_head=proof['source_head'],public_members=len(raw),logical=720,transmission_posts=2160,decoded_payloads=decoded,original_timeout_retained=True,all_six_quality_equal=True,classification_recall=.5,model_or_network_calls=0)))
