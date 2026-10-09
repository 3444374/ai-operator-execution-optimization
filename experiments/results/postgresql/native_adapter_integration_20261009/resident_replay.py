"""Replay the public persistent-model observations without any model service."""
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import statistics
import sys


def main():
    root=Path(__file__).resolve().parent
    sys.path.insert(0,str(root.parents[3]/'code'))
    from src.experiments.postgresql.native_adapter_metrics import summarize_calls
    from src.experiments.postgresql.native_adapter_http import bind_native_http_events
    report=json.loads((root/'resident-model-analysis.json').read_text())
    archive=root/report['public_archive']['path']
    assert hashlib.sha256(archive.read_bytes()).hexdigest()==report['public_archive']['sha256']
    members={};originals={}
    for line in gzip.decompress(archive.read_bytes()).splitlines():
        row=json.loads(line)
        assert row['path'] not in members and '..' not in Path(row['path']).parts
        assert hashlib.sha256(row['text'].encode()).hexdigest()==row['sha256']
        members[row['path']]=row['text'];originals[row['path']]=row['original_sha256']
    assert len(members)==report['public_archive']['members']
    def value(path):return json.loads(members[path])
    def lines(path):return [json.loads(line) for line in members[path].splitlines() if line.strip()]
    base='resident-model02/'
    final=value(base+'worker-final.json');ledger=value(base+'ledger-final.json')
    service=value(base+'service-count.json');owner=value(base+'owner-exit.json')
    assert final['status']=='passed' and len(final['records'])==90 and final['posts']==5760
    assert ledger['limit']==ledger['allocated_requests']==5760 and len(ledger['units'])==90
    assert sum(unit['requests'] for unit in ledger['units'])==5760 and all(unit['claimed']==1 for unit in ledger['units'])
    assert service['success_delta']==5760 and service['running']==service['waiting']==0
    assert owner['status']=='passed' and not owner['cleanup_errors'] and not owner['remaining_gpu_compute']
    assert not any(owner['ports_open'].values())
    responses=set();usage=Counter();requests={};observations=[];lifetimes=defaultdict(list)
    all_calls=0
    for item in final['records']:
        arm,ordinal=item['arm'],item['ordinal']
        prefix=base+'arms/'+arm+'/query-'+str(ordinal)+'/'
        summary=value(prefix+'summary.json');timing=value(prefix+'persistent-query.json')
        execution=value(prefix+'q0/execution.json');ready=value(prefix+'q0/ready-timing.json')
        assert originals[prefix+'summary.json']==timing['query_summary_sha256']==item['summary_sha256']
        assert summary['status']=='passed' and summary['actual_posts']==summary['rows']==timing['rows']==item['rows']
        assert timing['actual_posts']==item['rows'] and execution['recorded_rows']==item['rows']
        assert summary['preparation_model_posts']==0
        assert timing['persistent_lifecycle']['result_cache'] is False
        life=timing['persistent_lifecycle'];lifetimes[arm].append(life)
        assert life['core_jobs'] in (None,0) and (life['core_usage'] is None or not any(life['core_usage'].values()))
        output=prefix+'q0/results.jsonl'
        assert originals[output]==execution['results_sha256']
        rows=lines(output);reference=value('resident-model01/references-'+str(item['rows'])+'.json')
        assert len(rows)==len({row['row'][0] for row in rows})==item['rows']
        assert {row['row'][0] for row in rows}==set(reference)
        assert all(row['row'][1] in ('POSITIVE','NEGATIVE') for row in rows)
        assert sum(row['row'][1]==reference[row['row'][0]] for row in rows)==summary['quality']['correct']
        protocol=lines(prefix+'protocols.jsonl');http=lines(prefix+'http-trace.jsonl')
        assert len(protocol)==len(http)==item['rows']
        requests[(arm,ordinal)]=Counter(p['request_values_sha256'] for p in protocol)
        for row in protocol:
            assert row['http_status']==200 and row['request_parameters']['model']=='Qwen2.5-7B-Instruct'
            response=row['response'];assert response['id'] not in responses
            responses.add(response['id'])
            assert response['model']=='Qwen2.5-7B-Instruct' and len(response['choices'])==1
            assert response['choices'][0]['finish_reason']=='stop'
            tokens=response['usage'];assert tokens['total_tokens']==tokens['prompt_tokens']+tokens['completion_tokens']
            usage.update(prompt_tokens=tokens['prompt_tokens'],completion_tokens=tokens['completion_tokens'])
        assert all(row['status']=='completed' and row['query_id']==item['unit_id']
            and row['retry_count']==0 and row['upstream_headers_send_count']==1
            and row['request_body_sha256']==row['forwarded_body_sha256'] for row in http)
        first=min(row['upstream_dispatch_started_monotonic_ns'] for row in http)
        last=max(row['upstream_response_body_read_completed_monotonic_ns'] for row in http)
        stamps=[timing['t_release_ns'],timing['t_submit_ns'],first,last,timing['t_eof_ns']]
        assert stamps==sorted(stamps)
        assert (stamps[-1]-stamps[0])/1e9==timing['release_query_seconds']
        assert (stamps[-1]-stamps[1])/1e9==ready['ready_query_seconds']
        calls=value(prefix+'calls.json');events=lines(prefix+'method-events.jsonl')
        for name in members:
            if name.startswith(prefix+'worker-events/'):
                events.extend(lines(name))
        if arm in ('fixed-map-native-daft','fixed-map-native-ray'):
            events=bind_native_http_events(events)
        timing_calls=summarize_calls(events,calls)
        assert len(timing_calls['calls'])==item['rows']
        all_calls+=len(calls)
        observations.append(dict(arm=arm,ordinal=ordinal,phase=item['phase'],rows=item['rows'],
            release=timing['release_query_seconds'],submit=ready['ready_query_seconds'],
            coverage=(last-first)/1e9,correct=summary['quality']['correct']))
    assert len(responses)==all_calls==5760 and dict(usage)==report['protocol_tokens']
    for group in [('fixed-map-native-daft','fixed-map-native-ray','fixed-map-semloom'),
                  ('lotus-adapted-native','lotus-method-semloom-local-diagnostic','lotus-method-semloom')]:
        for ordinal in range(15):
            assert requests[(group[0],ordinal)]==requests[(group[1],ordinal)]==requests[(group[2],ordinal)]
    for arm,items in lifetimes.items():
        assert len(items)==15
        for key in ('owner_id','execution_id','lm_id','ray_session_id'):
            assert len({item[key] for item in items})==1
        pools=[row for row in lines(base+'arms/'+arm+'/owners/'+arm+'/owner-events.jsonl') if row.get('event')=='ray_worker_pool']
        if arm in ('fixed-map-semloom','lotus-method-semloom'):
            assert len(pools)==1 and pools[0]['identity_available'] and len(pools[0]['actor_ids'])==2
            assert pools[0]['actor_ids']==report['reused_lifetimes'][arm]['actual_ray_actor_ids']
    for aggregate in report['measurement_aggregates']:
        selected=[row for row in observations if row['arm']==aggregate['arm']
                  and row['rows']==aggregate['rows'] and row['phase']=='measurement']
        assert len(selected)==5
        assert [row['release'] for row in selected]==aggregate['release_samples']
        for source,target in [('release','release_query_seconds'),('submit','submit_query_seconds'),('coverage','dispatch_coverage_seconds')]:
            assert statistics.median(row[source] for row in selected)==aggregate['medians'][target]
    duration=owner['ended_epoch']-value(base+'ownership.json')['started_epoch']
    assert duration==report['model_owner_seconds']
    print(f'{len(members)} public members verified; 90 queries / 5760 unique model responses, rows, request hashes, actor reuse and all measurement medians replayed')


if __name__=='__main__':
    main()
