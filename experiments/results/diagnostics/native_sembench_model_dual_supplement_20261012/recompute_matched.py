"""Verify the finite four-group Q2 run from saved evidence; stdlib, no network."""
import argparse
import hashlib
import json
import math
import tarfile
from collections import Counter, defaultdict
from pathlib import Path

from recompute_phases import ARMS, query_fact, mean

SOURCE = '3bd5358706679b639e790585347133cd8d60b836'
GROUPS = [('single-a', 'single', [[0,1,2,3,4,5], [5,4,3,2,1,0]]),
          ('dual-a', 'dual', [[5,4,3,2,1,0], [0,1,2,3,4,5]]),
          ('dual-b', 'dual', [[2,3,4,5,0,1], [1,0,5,4,3,2]]),
          ('single-b', 'single', [[1,0,5,4,3,2], [2,3,4,5,0,1]])]


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def stats(values):
    v = sorted(values)
    assert v
    def q(p):
        at = (len(v)-1)*p
        lo = int(at)
        hi = min(lo+1, len(v)-1)
        return v[lo] + (v[hi]-v[lo])*(at-lo)
    return {'n': len(v), 'p50': q(.5), 'p95': q(.95), 'p99': q(.99),
            'min': v[0], 'max': v[-1], 'mean': mean(v),
            'percentile': 'linear interpolation; descriptive finite queries'}


def metric(name, text):
    return sum(float(line.split()[-1]) for line in text.splitlines()
               if line.startswith(name+'{'))


def audit(raw, original=False, members=None):
    load = lambda name: json.loads(raw[name])
    rows = lambda name: [json.loads(line) for line in raw[name].splitlines() if line.strip()]
    result = load('model-matched/result.json')
    watch = load('model-matched-watchdog.json')
    clock = load('model-start.json')
    plan = load('matched-plan.json')
    collection = load('collection-check.json')
    preflight = load('zero-model-preflight/result.json')
    prepare = load('prepare-result.json')
    quality = load('reference-quality.json')
    routes = load('model-preparation/route-manifest.json')
    assert result['source_head'] == plan['source_head'] == clock['source_head'] == SOURCE
    assert plan['publication_baseline'] == 'a0f3fbd393a6bf0879a2c0a973f727c737d00389'
    assert result['passed'] and watch['passed'] and watch['returncode'] == 0
    assert not watch['stop_sent'] and not watch['forced_cleanup'] and not result['forced_service_cleanup']
    assert not result.get('error_type') and not result.get('router_cleanup_error_type')
    for cleanup in [result['cleanup'], watch['cleanup'], preflight['cleanup']]:
        assert not cleanup['owned_survivors'] and not cleanup['affinity_violations']
        assert all(set(p['affinity']) <= set(range(72, 98)) for p in cleanup['processes'])
    assert not result['gpu_after'] and not collection['gpu_after'] and collection['owned_survivors'] == 0
    assert collection['passed'] and not collection['partial'] and collection['source_files_verified'] == 1001
    assert collection['old_histories_unchanged'] and result['old_histories_unchanged']
    assert result['run_identity_unchanged'] and result['original_first_clock_unchanged'] and result['independent_clock_unchanged']
    assert not clock['original_window_clock_reset']
    assert clock['deadline_monotonic_ns']-clock['started_monotonic_ns'] == 2700_000_000_000
    outer_elapsed = load('model-matched-launch.json')['started_monotonic'] - clock['started_monotonic_ns']/1e9 + watch['elapsed_since_supervisor_launch']
    assert 0 < result['elapsed_seconds'] <= outer_elapsed <= 2700
    assert result['startup_seconds_used'] <= 600
    assert (plan['slots'],plan['queries'],plan['planned_logical'],plan['planned_posts'],plan['max_logical'],plan['max_posts'],plan['max_seconds']) == (48,96,11520,34560,14336,43008,2700)
    assert plan['topology_limits'] == {'single':[7168,21504], 'dual':[7168,21504]}
    assert plan['research_strategies_off'] and plan['independent_clock_and_ledger'] and not plan['old_ledgers_changed']
    assert [(g['id'],g['topology'],g['orders']) for g in plan['groups']] == GROUPS
    assert list(result['phases']) == [g[0] for g in GROUPS]
    assert prepare['passed'] and prepare['source_files_checked'] == 1001 and prepare['payload_files'] == 16 and prepare['payload_uncompressed_bytes'] == 88177
    assert not prepare['old_clocks_and_ledgers_changed'] and prepare['serving_versions_and_source_unchanged']
    assert preflight['passed'] and len(preflight['cases']) == 12
    assert preflight['model_calls'] == preflight['posts'] == preflight['gpu'] == preflight['counts']['total'] == 0
    assert all(c['returncode'] == 0 for c in preflight['cases'])
    for name in ['harness-identity.json','matched-harness-identity.json']:
        identity = load(name)
        assert identity['source_head'] == SOURCE
        for path, sha in identity['files'].items():
            observed = hashlib.sha256(raw['validation/'+path]).hexdigest() if original else members['validation/'+path]['original_sha256']
            assert observed == sha, path
    assert load('harness-identity.json')['files']['send_counts.py'] == '2b08c3f9a9e69845d236c29b8e532e85ba82b9a9473d58080d9b3a7907d33e25'
    assert load('harness-identity.json')['files']['pre_send_ledger.py'] == '13774352fc07489302947a607c82e7e764e53e0fb091f8b3c9134aa2413a59c9'
    sends = rows('pre-send-records.jsonl')
    assert len(sends) == result['model_transmission_posts'] == collection['actual_posts'] == 34560
    assert result['model_calls'] == 11520
    assert [r['sequence'] for r in sends] == list(range(1,34561))
    assert all(r['returned_ns'] and r['status'] == 200 and not r['error_type'] for r in sends)
    assert Counter((r['phase'],r['kind']) for r in sends) == {(p,k):5760 for p in ('single','dual') for k in ('gateway','router','model')}
    counter = {r['scope']:(r['total'],r['logical']) for r in load('send-totals-final.json')}
    assert counter == {'software':(0,0),'quality':(0,0),'single':(17280,5760),'dual':(17280,5760),'__model__':(34560,11520)}
    routers = rows('model-matched/router-http-trace.jsonl')
    gpu = rows('model-matched/gpu-timeseries.jsonl')
    assert len(routers) == 11520 and all(t['status']=='completed' and not t['errors'] and t['retry_count']==0 and t['request_body_sha256']==t['forwarded_body_sha256'] for t in routers)
    facts, slots, protocols_all, traces_all, services = [], [], [], [], []
    reference = None
    endpoint_totals = defaultdict(lambda:dict(requests=0,prompt=0,generation=0))
    query_intervals = []
    previous_cleanup = None
    for label, topology, orders in GROUPS:
        group = result['phases'][label]
        assert group['passed'] and group['topology'] == topology and group['service_history_before'] == 0
        assert len(group['cases']) == 12 and group['counts_delta'] == dict(total=8640,gateway=2880,router=2880,model=2880,fixture=0)
        expected = [(r,p,ARMS[a]) for r,order in enumerate(orders) for p,a in enumerate(order)]
        assert [(c['round'],c['position'],c['arm']) for c in group['cases']] == expected
        group_services = [s for s in result['services'] if s['phase']==label]
        assert len(group_services) == (1 if topology=='single' else 2)
        if previous_cleanup is not None:
            assert min(s['started_ns'] for s in group_services) >= previous_cleanup
        previous_cleanup = group['cleanup_ended_ns']
        for case in group['cases']:
            prefix = f"model-matched/{label}/{label}-round-{case['round']}-position-{case['position']}-{case['arm']}/"
            config = load(prefix+'config.json')
            assert (config['arms'],config['queries'],config['warmups'],config['repeats'],config['max_attempts']) == ([case['arm']],[2],1,1,240)
            assert config['lotus_observation_mode']=='performance'
            assert config['native_options']==dict(concurrency=6 if case['arm'] in ARMS[4:] else 4,num_threads=8,ray_actors=2,ray_async_batches_per_actor=2,ray_batch_rows=1,source_blocks=2)
            if case['arm']==ARMS[3]:
                assert config['max_held_tasks']==8 and config['physical']==dict(address='resolved',workers=2,batch_rows=2,window_bytes=2097152,object_bytes=8388608,payload_backend='daft')
            assert all(load(prefix+a+'-command.json')['returncode']==0 for a in ('data-preflight','preflight','execute'))
            assert case['matrix']['status']=='passed' and [c['phase'] for c in case['matrix']['cells']]==['warmup','measurement']
            slot_facts = []
            for cell in case['matrix']['cells']:
                path = prefix+'output/'+cell['unit_id']+'/'
                summary = load(path+'summary.json')
                protocols = rows(path+'protocols.jsonl')
                traces = rows(path+'http-trace.jsonl')
                calls = load(path+'calls.json')
                decisions = load(path+'filter-decisions.json')
                assert len(protocols)==len(traces)==len(calls)==len(decisions)==cell['actual_posts']==120
                assert Counter(digest(p['request']) if original else p['original_request_values_sha256'] for p in protocols)==Counter(routes['request_weights'])
                assert len({(c['row_id'],c['stage_id'],c['stage_ordinal'],c['call_id']) for c in calls})==120
                assert {c['row_id'] for c in calls}=={d[0] for d in decisions}
                pair = (decisions,load(path+'original-query-result.json'),summary['quality'])
                if reference is None: reference=pair
                assert pair==reference and summary['quality']==quality
                assert summary['execution']['status']=='completed' and not summary['errors'] and summary['native_http_audit']['readable_json_responses']==120
                assert summary['identity']['original_method_preserved'] and summary['identity']['original_query']['query_id']==2 and summary['identity']['original_query']['commit']=='c814e3807e72d4cf876b852b17e77f3cc94575c2'
                assert load(path+'model-method-check.json')['passed']
                assert all(t['status']=='completed' and t['retry_count']==0 and not t['errors'] and t['request_body_sha256']==t['forwarded_body_sha256'] for t in traces)
                assert cell['full_query_seconds']==summary['full_query_seconds'] and cell['ready_query_seconds']==summary['execution']['ready_query_seconds']
                unit = dict(cell,topology=topology,arm=case['arm'],round=case['round'],position=case['position'],path=path)
                fact = query_fact(raw,routers,gpu,label,unit)
                request_values = [(t['request_terminal_monotonic_ns']-t['received_monotonic_ns'])/1e9 for t in traces]
                fact['request_stats_s']=stats(request_values)
                qm = load(prefix+'query-metrics/'+cell['unit_id']+'-metrics.json')
                before={v['endpoint']:v['text'] for v in qm['before']}
                after={v['endpoint']:v['text'] for v in qm['after']}
                assert before.keys()==after.keys() and len(before)==len(group_services)
                delta={ep:{key:metric(name,after[ep])-metric(name,before[ep]) for key,name in [('requests','vllm:request_success_total'),('prompt','vllm:prompt_tokens_total'),('generation','vllm:generation_tokens_total')]} for ep in before}
                assert sum(v['requests'] for v in delta.values())==120 and sum(v['prompt'] for v in delta.values())==14926 and sum(v['generation'] for v in delta.values())==480
                assert all(v['requests']==(120 if topology=='single' else 60) for v in delta.values())
                for ep,values in delta.items():
                    for key,value in values.items():endpoint_totals[(label,ep)][key]+=value
                fact['model_metric_delta']=delta
                fact['group']=label
                fact['original_quality']=quality
                query_intervals.append((summary['query_preparation_started_ns'],summary['execution']['t_query_terminal_ns']))
                facts.append(fact);slot_facts.append(fact);protocols_all.extend(protocols);traces_all.extend(traces)
            startup = load(prefix+'output/owner-0/startup.json')
            group_summary = load(prefix+'output/owner-0/group-summary.json')
            preflights=sum(load(prefix+a+'-command.json')['seconds'] for a in ('data-preflight','preflight'))
            other=case['full_slot_seconds']-preflights-startup['startup_seconds']-sum(f['full_query_s'] for f in slot_facts)
            assert other>=0
            slots.append(dict(group=label,topology=topology,arm=case['arm'],round=case['round'],position=case['position'],full_slot_s=case['full_slot_seconds'],owner_startup_s=startup['startup_seconds'],zero_model_preflight_s=preflights,slot_other_s=other,owner_closed_ns=group_summary['ended_ns'],ledger_rows_before_slot=case['count_before']['total'],ledger_rows_after_slot=case['count_after']['total']))
        for index, service in enumerate(group_services):
            assert service['api_exit']==0 and service['service_history_before']==0
            assert set(service['actual_api_affinity'])==set(service['engine_affinity'])==set(range(82+index*8,90+index*8))
            before=raw[f'model-matched/{label}/service-{index}/ready-metrics.txt'].decode()
            after=raw[f'model-matched/{label}/service-{index}/final-metrics.txt'].decode()
            ep=service['endpoint'].rsplit('/v1/chat/completions',1)[0]
            expected_requests=2880 if topology=='single' else 1440
            assert metric('vllm:request_success_total',before)==0 and metric('vllm:request_success_total',after)==expected_requests
            assert metric('vllm:prompt_tokens_total',after)==endpoint_totals[(label,ep)]['prompt'] and metric('vllm:generation_tokens_total',after)==endpoint_totals[(label,ep)]['generation']
            services.append({k:service[k] for k in ('phase','replica','started_ns','ready_ns','readiness_seconds','api_exit','service_history_before','cpu_affinity')}|dict(final_requests=expected_requests,final_prompt_tokens=endpoint_totals[(label,ep)]['prompt'],final_generation_tokens=endpoint_totals[(label,ep)]['generation']))
    assert len(facts)==96 and len(slots)==48 and len(traces_all)==11520
    assert Counter(t['request_body_sha256'] for t in traces_all)==Counter(r['body_sha256'] for r in sends if r['kind']=='gateway')
    assert Counter(t['request_body_sha256'] for t in routers)==Counter(r['body_sha256'] for r in sends if r['kind']=='router')
    assert Counter(t['forwarded_body_sha256'] for t in routers)==Counter(r['body_sha256'] for r in sends if r['kind']=='model')
    query_intervals.sort()
    assert all(a[1]<=b[0] for a,b in zip(query_intervals,query_intervals[1:]))
    usage=dict(prompt=sum(p['response']['usage']['prompt_tokens'] for p in protocols_all),generation=sum(p['response']['usage']['completion_tokens'] for p in protocols_all))
    assert usage==dict(prompt=96*14926,generation=96*480)
    aggregates=[]
    for topology in ('single','dual'):
        for arm in ARMS:
            measurements=[f for f in facts if (f['topology'],f['arm'],f['phase'])==(topology,arm,'measurement')]
            assert len(measurements)==4
            warm=[f for f in facts if (f['topology'],f['arm'],f['phase'])==(topology,arm,'warmup')]
            group_values=[]
            for group in [g[0] for g in GROUPS if g[1]==topology]:
                selected=[f for f in measurements if f['group']==group]
                assert len(selected)==2
                group_values.append(dict(group=group,full_s=[f['full_query_s'] for f in selected],ready_s=[f['ready_query_s'] for f in selected],owner_dispatch_ms=[f['owner_http']['stage_sums_s']['dispatch_to_headers']/120*1000 for f in selected],router_dispatch_ms=[f['router_http']['stage_sums_s']['dispatch_to_headers']/120*1000 for f in selected],vllm_inference_ms=[f['vllm']['sums_s']['inference']/120*1000 for f in selected]))
            aggregate=dict(topology=topology,arm=arm,measurement_n=4,service_histories=2,request_n=480,groups=group_values,warmup_full_s=[f['full_query_s'] for f in warm],full_stats_s=stats([f['full_query_s'] for f in measurements]),ready_stats_s=stats([f['ready_query_s'] for f in measurements]))
            requests=[]
            for f in measurements:
                requests.extend((t['request_terminal_monotonic_ns']-t['received_monotonic_ns'])/1e9 for t in rows(f['path']+'http-trace.jsonl'))
            aggregate['request_stats_s']=stats(requests)
            aggregate['wall_mean_s']={k:mean([f['wall_parts_s'][k] for f in measurements]) for k in measurements[0]['wall_parts_s']}
            aggregate['owner_dispatch_mean_ms']=mean([f['owner_http']['stage_sums_s']['dispatch_to_headers']/120*1000 for f in measurements])
            aggregate['router_dispatch_mean_ms']=mean([f['router_http']['stage_sums_s']['dispatch_to_headers']/120*1000 for f in measurements])
            aggregate['vllm_mean_ms']={k:mean([f['vllm']['sums_s'][k]/120*1000 for f in measurements]) for k in measurements[0]['vllm']['sums_s']}
            aggregates.append(aggregate)
    identities={(p['pid'],p['born']) for cleanup in (result['cleanup'],watch['cleanup']) for p in cleanup['processes']}
    return dict(schema='semloom.sembench_q2_budget_repair_matched_verification.v1',passed=True,run_passed=True,source_head=SOURCE,publication_baseline=plan['publication_baseline'],slots=48,warmup_queries=48,measurement_queries=48,model_logical=11520,transmission_posts=34560,by_topology={p:dict(logical=5760,posts=17280) for p in ('single','dual')},hard_limits=dict(seconds=2700,logical=14336,posts=43008,startup_seconds=600),started_epoch=clock['started_epoch'],hard_deadline_epoch=clock['deadline_epoch'],outer_elapsed_s=outer_elapsed,supervisor_elapsed_s=result['elapsed_seconds'],startup_seconds=result['startup_seconds_used'],watchdog_exit=0,api_exits=[s['api_exit'] for s in services],services=services,quality=quality,classification_recall=.5,classification_f1=2/3,response_usage=usage,owned_process_identities=len(identities),independent_process_identity_checks=collection['process_identities_checked'],owned_survivors=0,affinity_violations=0,gpu_after=[],old_three_histories_unchanged=True,source_files_verified=1001,query_seriality=dict(queries=96,overlaps=0),units=facts,slots_observed=slots,aggregates=aggregates,limits=dict(stable_p99_supported=False,complete_performance_ranking_supported=False,model_or_network_calls_from_recompute=0,old_and_new_results_pooled=False,unique_causal_share_of_ledger_change_established=False,gpu_temperature_or_frequency_collected=False,request_samples_independent_across_queries=False))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--result-dir',type=Path,default=Path(__file__).resolve().parent)
    parser.add_argument('--write',type=Path)
    args=parser.parse_args()
    root=args.result_dir
    proof=json.loads((root/'matched-verification.json').read_text())
    path=root/'matched-observations.tar.gz'
    assert hashlib.sha256(path.read_bytes()).hexdigest()==proof['public_archive']['sha256']
    with tarfile.open(path) as archive:
        raw={m.name:archive.extractfile(m).read() for m in archive if m.isfile()}
    manifest=json.loads((root/'matched-members-sha256.json').read_text())
    restoring=set()
    def restore(name):
        if name in raw:return raw[name]
        assert name not in restoring,'cyclic member recovery'
        restoring.add(name)
        storage=manifest['members'][name]['storage']
        if storage['kind']=='reuse':raw[name]=restore(storage['member'])
        elif storage['kind']=='concat':raw[name]=b''.join(restore(part) for part in storage['parts'])
        else:raise AssertionError('stored member missing: '+name)
        restoring.remove(name)
        return raw[name]
    for name in manifest['members']:restore(name)
    assert set(raw)==set(manifest['members'])
    assert all(hashlib.sha256(raw[n]).hexdigest()==m['public_sha256'] for n,m in manifest['members'].items())
    actual=audit(raw,members=manifest['members'])
    assert actual=={k:v for k,v in proof.items() if k not in ('public_archive','privacy','private_archive_sha256')}
    if args.write:args.write.write_text(json.dumps(actual,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in actual.items() if k not in ('units','slots_observed','aggregates','quality','services')},ensure_ascii=False))


if __name__=='__main__':main()
