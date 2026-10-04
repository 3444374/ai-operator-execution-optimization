"""Replay queue membership and reuse the published row/count/resource checks."""
import argparse
from collections import Counter
import gzip
import importlib.util
import json
import os
from pathlib import Path
import statistics


def identities(rows):
    return [(row['session_id'],row['sequence']) for row in rows]


def select_prefix(before,capacity,byte_limit):
    selected=[];size=0
    for row in before:
        amount=row['payload_bytes']+24
        if len(selected)==capacity:break
        if selected and (row['job_id']!=selected[0]['job_id'] or size+amount>byte_limit):break
        selected.append(row);size+=amount
    return selected


def same_job_group(before,capacity,byte_limit):
    job=before[0]['job_id'];selected=[];size=0
    for row in before:
        if row['job_id']!=job:continue
        amount=row['payload_bytes']+24
        if len(selected)==capacity or size+amount>byte_limit:break
        selected.append(row);size+=amount
    return selected


def projected_checks(evidence,initial,common,work):
    spec=importlib.util.spec_from_file_location('published_checks',common)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    original=json.loads((evidence/'suite/summary.json').read_text())
    results={}
    for enabled in (False,True):
        root=work/('observed' if enabled else 'control');(root/'suite').mkdir(parents=True)
        subset=[case for case in original['cases'] if case['observation']==enabled]
        projected=dict(original,cases=subset,http_requests=2080)
        projected['metrics']={arm:original['metrics'][arm+('-on' if enabled else '-off')]
                              for arm in ('one-daft','one-arrow','two-daft','two-arrow')}
        (root/'suite/summary.json').write_text(json.dumps(projected))
        (root/'cleanup.json').write_bytes((evidence/'cleanup.json').read_bytes())
        for case in subset:
            alias=root/'suite'/f"{case['phase']}-{case['repeat']}-{case['arm']}";alias.mkdir()
            source=evidence/'suite'/case['case_name']
            for name in ('events.jsonl.gz','result.json','ledger.projection.json'):
                os.link(source/name,alias/name)
        # The earlier component supplies the same canonical payload hashes only.
        # Its preparation calls are not replayed and are not attributed to this run.
        results['on' if enabled else 'off']=module.analyze(initial,root)['core']
    return results


def analyze(evidence,initial,common,work):
    suite=json.loads((evidence/'suite/summary.json').read_text())
    checked=projected_checks(evidence,initial,common,work)
    cases=[]
    for case in suite['cases']:
        source=evidence/'suite'/case['case_name']
        with gzip.open(source/'events.jsonl.gz','rt') as stream:events=[json.loads(line) for line in stream]
        windows=[event['snapshot'] for event in events if event['event']=='ray_ready_window']
        if not case['observation']:
            assert not windows
            continue
        costs=json.loads((source/'trace-cost.json').read_text())
        assert costs['windows']==len(windows) and len(costs['samples'])==len(windows)
        assert [row['window_id'] for row in costs['samples']]==list(range(len(windows)))
        assert costs['observer_call_ns']==sum(row['observer_call_ns'] for row in costs['samples'])
        assert costs['instrumented_deque_and_seal_ns']==sum(row['instrumented_deque_and_seal_ns'] for row in costs['samples'])
        blocks=[event for event in events if event['event']=='ray_block_put']
        assert len(blocks)==len(windows)
        posted=[event for event in events if event['event']=='ray_http_completed']
        rpc={tuple(event['key'][name] for name in ('session_id','sequence')):event for event in posted}
        offers=[event for event in events if event['event']=='offer' and event['status']=='ACCEPTED']
        offered={tuple(event['key'][name] for name in ('session_id','sequence')):event['monotonic_ns'] for event in offers}
        submits={tuple(event['key'][name] for name in ('session_id','sequence')):event['monotonic_ns']
                 for event in events if event['event']=='submitted'}
        singles=one_ready=opportunities=0
        stop_counts=Counter();before_sizes=Counter();examples=[];accepted_backlog=[]
        previous_remaining=[];all_selected=[]
        for index,snapshot in enumerate(windows):
            before=snapshot['before'];selected=snapshot['selected'];remaining=snapshot['remaining']
            assert snapshot['window_id']==index and 1<=len(before)<=snapshot['capacity']==4
            assert len(set(identities(before)))==len(before)
            assert selected==select_prefix(before,snapshot['capacity'],snapshot['window_bytes'])
            assert remaining==before[len(selected):]
            assert identities(before)[:len(previous_remaining)]==previous_remaining
            previous_remaining=identities(remaining)
            assert not any(row['sent'] or row['cancelled'] for row in before)
            assert snapshot['selection_started_ns']<=snapshot['selection_ended_ns']
            assert len(selected)==blocks[index]['rows']
            assert all(rpc[key]['rpc_started_ns']>=snapshot['selection_ended_ns'] for key in identities(selected))
            assert snapshot['selected_input_bytes']==sum(row['payload_bytes']+24 for row in selected)
            conditions=[]
            if not remaining:conditions.append('empty')
            if len(selected)==snapshot['capacity']:conditions.append('row_limit')
            if remaining:
                if remaining[0]['job_id']!=selected[0]['job_id']:conditions.append('different_job')
                if snapshot['selected_input_bytes']+remaining[0]['payload_bytes']+24>snapshot['window_bytes']:conditions.append('input_bytes')
            assert snapshot['stop_conditions']==conditions
            stop_counts['+'.join(conditions)]+=1;before_sizes[len(before)]+=1
            if len(selected)==1:singles+=1
            if len(before)==1:one_ready+=1
            candidate=same_job_group(before,snapshot['capacity'],snapshot['window_bytes'])
            if len(candidate)>len(selected):
                opportunities+=1
                if len(examples)<2:examples.append(dict(window_id=index,before=before,selected=selected,candidate=candidate))
            instant=snapshot['selection_started_ns']
            # This is an observation of accepted inputs lacking a submission event,
            # not a guarantee that arbitrary SQL inputs are ready for preprocessing.
            accepted_backlog.append(sum(offered[key]<=instant<submits[key] for key in rpc))
            all_selected.extend(identities(selected))
        assert len(all_selected)==case['total_rows'] and set(all_selected)==set(rpc) and len(set(all_selected))==len(all_selected)
        assert previous_remaining==[]
        assert costs['instrumented_deque_and_seal_ns']==sum(snapshot['instrumented_deque_and_seal_ns'] for snapshot in windows)
        cases.append(dict(case_name=case['case_name'],phase=case['phase'],repeat=case['repeat'],arm=case['arm'],
            windows=len(windows),singleton_windows=singles,one_ready_windows=one_ready,
            same_job_group_opportunities=opportunities,before_sizes=dict(sorted(before_sizes.items())),stop_conditions=dict(stop_counts),
            accepted_without_submission_observation_median=statistics.median(accepted_backlog),
            instrumented_deque_and_seal_ms=costs['instrumented_deque_and_seal_ns']/1e6,
            observer_call_ms=costs['observer_call_ns']/1e6,examples=examples))
    metrics={}
    for arm in ('one-daft','one-arrow','two-daft','two-arrow'):
        group=[case for case in cases if case['arm']==arm and case['phase']=='measurement']
        assert len(group)==3
        metrics[arm]={name:dict(values=[case[name] for case in group],sum=sum(case[name] for case in group))
                      for name in ('windows','singleton_windows','one_ready_windows','same_job_group_opportunities')}
        metrics[arm]['snapshot_cost_ms']={name:dict(values=[case[name] for case in group],median=statistics.median(case[name] for case in group))
                                         for name in ('instrumented_deque_and_seal_ms','observer_call_ms')}
    assert len(suite['cases'])==40 and suite['http_requests']==4160
    return dict(status='passed',http_requests=4160,model_requests=0,pg_queries=0,
                core_checks=checked,queue_metrics=metrics,queue_cases=cases,
                common_check_source=str(common.name),component_hash_reference='map-preparation-20261004a, canonical 128 payload digests',
                source_identity=json.loads((evidence/'reference-identity.json').read_text()),
                shared_ray_startup_ms=suite['ray_startup_ms'],suite_seconds=suite['suite_seconds'])


def main():
    parser=argparse.ArgumentParser()
    for name in ('evidence','initial','common','work','output'):parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args();args.work.mkdir(parents=True,exist_ok=False)
    result=analyze(args.evidence,args.initial,args.common,args.work)
    with args.output.open('x') as stream:json.dump(result,stream,separators=(',',':'))
    print(json.dumps(dict(status=result['status'],http_requests=4160,queue_metrics=result['queue_metrics'])))


if __name__=='__main__':main()
