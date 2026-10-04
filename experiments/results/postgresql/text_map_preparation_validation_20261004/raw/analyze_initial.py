"""Check retained PG lifecycle and the stopped real-model qualification."""
from collections import Counter
import argparse,hashlib,json,sys
from pathlib import Path
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--evidence',type=Path,required=True)
parser.add_argument('--inputs',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--repository',type=Path,default=Path(__file__).resolve().parents[5])
args=parser.parse_args()
sys.path.insert(0,str(args.repository/'code'))
from src.baselines.common.private_artifacts import content_digest
from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.postgresql.map_bindings import parse_pg_bindings,verify_bound_map_results
from src.experiments.postgresql.map_direct import request_body
from src.experiments.postgresql.query_workloads import load_manifest,read_prepared
from src.experiments.query_resources import verify_logical_resources

pg=args.evidence/'pg';model=args.evidence/'model-first'
checks=[];runs=[];total=0
for name in ('pm4l','pm4b','pm4c','pm4d'):
    root=pg/'lifecycle'/name
    controller=json.loads((root/'controller-summary.json').read_text())
    worker=json.loads((root/'worker/summary.json').read_text()) if (root/'worker/summary.json').exists() else {}
    checks.extend(worker.get('checks',[]));total+=worker.get('http_requests',0)
    runs.append(dict(run=name,status=controller['status'],http_requests=worker.get('http_requests',0),
        checks=len(worker.get('checks',[])),errors=worker.get('errors',[]) or controller['errors']))
assert len(checks)==16 and len({(v['mode'],v['name']) for v in checks})==16 and total==26
inputs=args.inputs/'qualification/manifest.json'
raw=list(read_prepared(inputs,'raw.jsonl'))
plan=SemanticMapPlan(MOVIE_MAP_INSTRUCTION,'Qwen2.5-7B-Instruct',128)
result=dict(source_commit='380824f2951f6843bb7f326c3f65c95b7cd4352a',
    pg_lifecycle=dict(unique_checks=len(checks),loopback_http_requests=total,model_requests=0,runs=runs,checks=checks),
    real_model=dict(status='failed',actual_requests=32,queries=[],measurements_1024=0))
for mode in ('immediate','prepared'):
    root=model/'real01/worker'/(mode+'16')/'check16'
    summary=json.loads((root/'summary.json').read_text())
    events=[json.loads(v) for v in (root/'events.jsonl').read_text().splitlines()]
    sessions=[json.loads(v) for v in (root/'sessions.jsonl').read_text().splitlines()]
    output=(root/'q0/results.jsonl').read_bytes()
    assert hashlib.sha256(output).hexdigest()==summary['execution']['results_sha256']
    rows=[json.loads(v)['row'] for v in output.splitlines()]
    bindings=parse_pg_bindings((root/'q0-producer.log').read_text().splitlines())
    association=verify_bound_map_results([(r[1],r[3]) for r in raw],rows,bindings,events,sessions,plan=plan)
    assert [v.row_id for v in bindings]==[v[0] for v in rows]
    guards=[v for v in events if v['event']=='remote_request_guard']
    assert len(guards)==16 and all(v['status']=='completed' for v in guards)
    requests=[content_digest(v['body']) if 'body' in v else v['request_values_sha256'] for v in events if v['event']=='request']
    assert Counter(requests)==Counter(content_digest(request_body(plan,r[3])) for r in raw)
    logical=verify_logical_resources(events,dict(held_tasks=256,input_bytes=134217728,
        result_bytes=134217728,active_requests=64,active_work=64),expected_jobs=1)
    live={};mismatches=[]
    for index,event in enumerate(events):
        if event['event']=='core_ray_block_reserved':live[event['block_id']]=event['bytes']
        elif event['event']=='core_ray_block_released':del live[event['block_id']]
        elif event['event']!='core_ray_block_put':continue
        if sum(live.values())!=event['object_bytes']:
            mismatches.append(dict(event_index=index,event=event['event'],block_id=event['block_id'],
                reconstructed_bytes=sum(live.values()),observed_bytes=event['object_bytes']))
    assert not live
    if mode=='immediate':assert not mismatches
    else:assert mismatches[0]['reconstructed_bytes']==1939 and mismatches[0]['observed_bytes']==0
    memory=summary['resources']['processes']
    result['real_model']['queries'].append(dict(mode=mode,rows=16,status=summary['status'],
        independently_matched_rows=association['matched_rows'],durable_guards=16,
        logical_resources=logical,object_observation_mismatches=mismatches,
        samples=memory['samples'],sampled_total_pss_peak_bytes=memory['sampled_total_pss_peak_bytes'],
        pss_unavailable=memory['pss_unavailable']))
samples=json.loads((model/'real01/service-samples.json').read_text())
assert samples[-1]['metrics']['vllm:request_success_total']==32
assert samples[-1]['metrics']['vllm:num_requests_running']==samples[-1]['metrics']['vllm:num_requests_waiting']==0
owner=json.loads((model/'real01/controller-summary.json').read_text())
assert owner['actual_posts']==owner['server_access_posts']==32 and not owner['gpu_compute_pids']
result['real_model']['owner_seconds']=owner['elapsed_seconds']
args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({'PG_checks':16,'PG_HTTP':26,'model_requests':32,'model_rows_independently_matched':32,'measured_1024_queries':0,'new_model_requests':0}))
