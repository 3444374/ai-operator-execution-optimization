"""Recompute complete single and partial dual facts; stdlib, no model/network."""
import base64,hashlib,json,tarfile
from collections import Counter,defaultdict
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from retained_evidence import read_bytes, read_json, open_archive

ROOT=Path(__file__).resolve().parent
proof=read_json(ROOT/'performance-verification.json');manifest=read_json(ROOT/'performance-members-sha256.json');archive=ROOT/'performance-observations.tar.gz'
assert hashlib.sha256(read_bytes(archive)).hexdigest()==proof['public_archive']['sha256']
with open_archive(archive) as t:raw={m.name:t.extractfile(m).read() for m in t if m.isfile()}
assert set(raw)==set(manifest['members'])
for n,m in manifest['members'].items():assert hashlib.sha256(raw[n]).hexdigest()==m['public_sha256'],n
load=lambda n:json.loads(raw[n]);rows=lambda n:[json.loads(l) for l in raw[n].splitlines()]
result=load('model-resume/result.json');watch=load('model-resume-watchdog.json');sends=rows('pre-send-records.jsonl')
assert result['error_type']=='JSONDecodeError' and not result['passed'] and watch['returncode']==1 and not watch['forced_cleanup']
assert not result['cleanup']['owned_survivors'] and not result['cleanup']['affinity_violations'] and not result['gpu_after']
assert result['original_first_clock_unchanged'] and result['elapsed_seconds']<5100 and result['startup_seconds_used']<900
assert len(sends)==23040 and all(v['returned_ns'] and v['status']==200 and not v['error_type'] for v in sends)
for phase,n in (('quality',720),('single',4320),('dual',2640)):assert Counter(v['kind'] for v in sends if v['phase']==phase)==dict(gateway=n,router=n,model=n)
assert len(result['phases']['single']['cases'])==18 and result['phases']['single']['passed'] and len(result['phases']['dual']['cases'])==11
assert len(rows('model-resume/router-http-trace.jsonl'))==6960
grouped=defaultdict(list);protocols=[];all_traces=[]
for unit in proof['units']:
 p=unit['path'];s=load(p+'summary.json');values=rows(p+'protocols.jsonl');traces=rows(p+'http-trace.jsonl');calls=load(p+'calls.json')
 assert len(values)==len(traces)==len(calls)==120 and s['native_http_audit']['readable_json_responses']==120 and load(p+'model-method-check.json')['passed']
 assert not s['errors'] and s['quality']==proof['quality'] and all(t['status']=='completed' and t['retry_count']==0 and not t['errors'] for t in traces)
 assert Counter(c['request_values_sha256'] for c in calls)==Counter(load('model-preparation/route-manifest.json')['request_weights']) or (unit['arm']=='lotus-adapted-native' and s['method_http_values_audit']['status']=='unavailable')
 assert unit['full_query_seconds']==s['full_query_seconds'] and unit['ready_query_seconds']==s['execution']['ready_query_seconds']
 if unit['phase']=='measurement':grouped[(unit['topology'],unit['arm'])].append((unit,traces))
 protocols+=values;all_traces+=traces
assert len(proof['units'])==58 and len(protocols)==6960
assert Counter(t['request_body_sha256'] for t in all_traces)==Counter(v['body_sha256'] for v in sends if v['phase'] in ('single','dual') and v['kind']=='gateway')
def stats(values):
 v=sorted(values);n=len(v)
 def q(p):
  at=(n-1)*p;lo=int(at);hi=min(lo+1,n-1);return v[lo]+(v[hi]-v[lo])*(at-lo)
 return dict(n=n,p50=q(.5),p95=q(.95),p99=q(.99),max=v[-1],min=v[0],mean=sum(v)/n,percentile='linear interpolation; descriptive')
for a in proof['aggregates']:
 group=grouped[(a['topology'],a['arm'])];values=[u for u,_ in group];traces=[t for _,tr in group for t in tr]
 assert a['query_n']==len(values) and a['request_n']==len(traces)
 assert a['full_query_values_s']==[u['full_query_seconds'] for u in values] and a['ready_query_values_s']==[u['ready_query_seconds'] for u in values]
 assert a['full_query_stats_s']==stats(a['full_query_values_s']) and a['ready_query_stats_s']==stats(a['ready_query_values_s'])
 assert a['request_latency_s']==stats([(t['request_terminal_monotonic_ns']-t['received_monotonic_ns'])/1e9 for t in traces])
 assert a['query_n']==(3 if a['topology']=='single' else 1 if a['arm']=='lotus-method-native-imlane-ray' else 2)
usage=dict(prompt=sum(p['response']['usage']['prompt_tokens'] for p in protocols),generation=sum(p['response']['usage']['completion_tokens'] for p in protocols))
assert usage==proof['completed_response_usage']==dict(prompt=865708,generation=27840)
assert proof['whole_model_response_usage']==dict(prompt=955264,generation=30720)
decoded=0
def inspect(value):
 global decoded
 if isinstance(value,dict):
  for k,v in value.items():
   if k in ('payload','response_body_base64','response_wire') and isinstance(v,str):base64.b64decode(v,validate=True).decode();decoded+=1
   else:inspect(v)
 elif isinstance(value,list):
  for v in value:inspect(v)
for n,b in raw.items():
 if n.endswith('.json'):inspect(json.loads(b))
 elif n.endswith('.jsonl'):
  for l in b.splitlines():inspect(json.loads(l))
assert decoded==proof['privacy']['decoded_payloads_scanned']
print(json.dumps(dict(passed=True,source_head=proof['source_head'],public_members=len(raw),logical_total=7680,transmission_posts=23040,single_slots=18,dual_slots=11,unrun_dual_slots=7,measured_queries=29,warmup_queries=29,decoded_payloads=decoded,model_or_network_calls=0)))
