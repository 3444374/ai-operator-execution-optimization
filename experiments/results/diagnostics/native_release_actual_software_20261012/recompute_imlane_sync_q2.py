"""Offline checks of saved synchronous Q2 software observations; no execution."""
from collections import Counter
import base64,hashlib,json,tarfile
from pathlib import Path

def main():
 root=Path(__file__).resolve().parent
 facts=json.loads((root/'imlane-sync-q2-verification.json').read_text())
 members=json.loads((root/'imlane-sync-q2-members-sha256.json').read_text())
 path=root/facts['public_archive']['path']
 assert hashlib.sha256(path.read_bytes()).hexdigest()==facts['public_archive']['sha256']
 with tarfile.open(path) as archive:raw={m.name:archive.extractfile(m).read() for m in archive if m.isfile()}
 assert len(raw)==facts['public_archive']['members']==len(members['members'])
 assert set(raw)==set(members['members'])
 for name,data in raw.items():assert hashlib.sha256(data).hexdigest()==members['members'][name]['public_sha256']
 load=lambda n:json.loads(raw[n]);rows=lambda n:[json.loads(line) for line in raw[n].splitlines()]
 result=load('software/result.json');watchdog=load('software-watchdog.json');final=load('software-final-audit.json');assets=load('asset-check.json')
 assert all(r['passed'] for r in (result,watchdog,final,assets))
 assert watchdog['elapsed_seconds']==facts['elapsed_seconds']<facts['max_seconds']==300 and watchdog['returncode']==0
 assert not watchdog['work_stop_sent'] and not watchdog['forced_cleanup']
 assert result['counts']==dict(total=480,gateway=240,router=0,model=0,fixture=240)
 assert result['fixture_cleanup']==dict(active_connections=0,request_threads_alive=[]) and not result['fixture_thread_alive'] and not result['fixture_errors']
 for cleanup in (result['cleanup'],watchdog['cleanup']):
  assert not cleanup['owned_survivors'] and not cleanup['affinity_violations']
  assert all(set(p['affinity'])<=set(range(8,16)) for p in cleanup['processes'])
 assert len(watchdog['cleanup']['processes'])==final['registered_process_identities']==31
 assert not final['owned_survivors'] and not final['source_changed'] and not final['gpu_compute_after'] and not final['new_model_start_file_exists']
 assert assets['source_files_verified']==facts['source_files_verified']==1001 and assets['runtime_preflight_returncode']==0
 sends=rows('pre-send-records.jsonl');receipts=result['fixture_receipts'];received=load('fixture-received.json')
 assert len(sends)==480 and Counter(s['kind'] for s in sends)==Counter(gateway=240,fixture=240)
 assert all(s['phase']=='software' and s['returned_ns'] and s['status']==200 and not s['error_type'] for s in sends)
 assert len(receipts)==len(received)==240 and len({r['sequence'] for r in received})==240
 assert Counter(r['body_sha256'] for r in receipts)==Counter(s['body_sha256'] for s in sends if s['kind']=='fixture')
 assert all(r['end_ns'] and r['answer']==('True' if int(r['messages_sha256'][-2:],16)%2==0 else 'False') for r in receipts)
 for pid in {s['pid'] for s in sends}:
  marker=load('bootstrap/'+str(pid)+'.json');assert marker['pid']==pid and marker['installed'] and 'aiohttp' in marker['hooks']
  assert marker['helper_identity']==members['members']['harness-identity.json']['original_sha256']
 pairs=[];traces=[];worker_pids=set();encoded_payloads=0
 for ordinal,arm in enumerate(('lotus-method-native-imlane','lotus-method-native-imlane-ray')):
  prefix='software/cell-'+str(ordinal)+'/';matrix=load(prefix+'output/matrix.json');preflight=load(prefix+'output/preflight.json');config=load(prefix+'config.json');author=load(prefix+'author-start.json')
  assert matrix['status']=='passed' and len(matrix['cells'])==1 and config['warmups']==0 and config['max_attempts']==120
  assert preflight['relation_rows']==2000 and preflight['taken_3_rows']==120
  assert load(prefix+'driver-bootstrap.json')['cli_waiting']
  assert all(load(prefix+n+'-command.json')['returncode']==0 for n in ('data-preflight','preflight','execute'))
  assert len(author['actual_worker_bootstraps'])==2 and author['actual_meta']==dict(native_executors=4,native_task_threads=6)
  for worker in author['actual_worker_bootstraps']:
   worker_pids.add(worker['pid']);assert load('bootstrap/'+str(worker['pid'])+'.json')['start_ticks']==worker['start_ticks']
   if ordinal==1:assert worker['command'][0]=='ray::ActorWrapper'
   else:assert '--parent_pid' in worker['command'] and '--executor_id' in worker['command']
  cell=matrix['cells'][0];unit=prefix+'output/'+cell['unit_id']+'/';summary=load(unit+'summary.json');protocols=rows(unit+'protocols.jsonl');trace=rows(unit+'http-trace.jsonl');calls=load(unit+'calls.json');decisions=load(unit+'filter-decisions.json')
  assert cell['arm']==arm and cell['query_id']==2 and cell['actual_posts']==len(protocols)==len(trace)==len(calls)==len(decisions)==120
  assert summary['status']=='passed' and not summary['errors'] and summary['method_http_values_audit']['status']=='passed' and not summary['performance_qualified']
  assert len({(c['row_id'],c['stage_id'],c['stage_ordinal'],c['call_id']) for c in calls})==120
  assert all(t['forwarded'] and t['status']=='completed' and not t['errors'] and t['retry_count']==0 and t['upstream_status']==200 and t['request_body_sha256']==t['forwarded_body_sha256'] for t in trace)
  audit=summary['quality']['classification_audit'];assert [audit[n] for n in ('true_positive','false_positive','true_negative','false_negative','invalid','missing_rows')]==[9,54,52,5,0,0]
  assert Counter(bool(d[1]) for d in decisions)==Counter({True:63,False:57}) and summary['quality']['original_metric']['f1_score']==0
  pairs.append((protocols,decisions,load(unit+'original-query-result.json'),summary['quality'],Counter(c['request_values_sha256'] for c in calls)))
  traces.extend(trace)
  worker_rows=[row for n in raw if n.startswith(prefix) and '/worker-traces/' in n for row in rows(n)]
  assert len(worker_rows)==120 and {r['worker_pid'] for r in worker_rows}=={w['pid'] for w in author['actual_worker_bootstraps']}
  assert all(r['status']==200 and not r['execution_error_type'] and not r['transport_error_type'] and r['kernel_sha256']==author['udf_sha256'] for r in worker_rows)
  for name in raw:
   if name.startswith(prefix) and '/batches/' in name and name.endswith('/input.jsonl'):
    for call in rows(name):
     body=json.loads(base64.b64decode(call['payload'],validate=True));assert body['max_completion_tokens']==8 and all('original_content_sha256' in m for m in body['messages']);encoded_payloads+=1
 assert pairs[0][1:]==pairs[1][1:] and Counter(json.dumps(p,sort_keys=True) for p in pairs[0][0])==Counter(json.dumps(p,sort_keys=True) for p in pairs[1][0])
 assert encoded_payloads==240 and worker_pids=={s['pid'] for s in sends if s['kind']=='gateway'}
 assert Counter(t['request_body_sha256'] for t in traces)==Counter(s['body_sha256'] for s in sends if s['kind']=='gateway')
 return dict(passed=True,archive_members=len(raw),logical_calls=240,transport_posts=480,actual_used_worker_pids=len(worker_pids),elapsed_seconds=watchdog['elapsed_seconds'],software_only=True,new_requests=0,model=0,gpu=0)

if __name__=='__main__':print(json.dumps(main()))
