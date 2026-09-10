"""Evaluate completed query recordings after reading their independent sources."""
from collections import Counter
import json
import math

from src.baselines.common.private_artifacts import content_digest
from src.baselines.text.sembench_movie import evaluate_original,classification_audit,MOVIE_FILTER_INSTRUCTION
from src.execution_provider.wire import v3
from src.observability.metrics.squad import squad_example_scores
from .map_direct import request_body
from .map_bindings import parse_pg_bindings,verify_bound_map_results
from .native_map_bindings import verify_native_map_results
from .movie_queries import verify_filter_decisions
from .query_workloads import read_prepared
from .window_memory import verify_window_memory
from .map_query_recording import evaluate_recording
from src.experiments.query_resources import verify_logical_resources


def read_events(path):
    if path.stat().st_size>256*1048576:
        raise ValueError('query event history exceeds audit bound')
    with path.open() as stream:
        return [json.loads(line) for line in stream]


def http_accounting(events,maximum):
    active=peak=0
    timeline=sorted((e for e in events if e['event'] in (
        'http_started','http_finished','core_http_started','core_http_finished')),
        key=lambda e:e['monotonic_ns'])
    for event in timeline:
        active += 1 if event['event'].endswith('started') else -1
        if active<0 or active>maximum:
            raise ValueError('actual HTTP occupancy violates query configuration')
        peak=max(peak,active)
    if active!=0:
        raise ValueError('HTTP responsibilities did not settle')
    return dict(peak_http=peak,final_http_active=active,started_requests=len(timeline)//2)


def json_metric(value):
    """Keep upstream infinite-error meaning while writing strict JSON."""
    if isinstance(value,float) and not math.isfinite(value):
        return 'Infinity' if value>0 else '-Infinity' if value<0 else 'NaN'
    if isinstance(value,dict):
        return {k:json_metric(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):
        return [json_metric(v) for v in value]
    return value


def pg_plan_summary(plan):
    """Expose the selected nodes and initialized window without another model run."""
    nodes=[]
    def visit(node):
        if 'Semantic Spec' in node or 'Semantic Input Window' in node:
            nodes.append({key:node.get(key) for key in ('Node Type','Custom Plan Provider','Semantic Spec',
                'Physical Algorithm','Semantic Input Window','Semantic Window Fallback Reason')})
        for child in node.get('Plans',[]):
            visit(child)
    for item in plan:
        visit(item['Plan'])
    return dict(semantic_nodes=nodes,scope='plain EXPLAIN before execution on the same connection/settings',
                absent_window='not exposed by this plan; never inferred from configured HTTP capacity')


def evaluate(config, inputs, plan, manifest_path, root, checkout):
    return evaluate_recording(root/'q0', lambda rows: _evaluate_rows(
        config, inputs, plan, manifest_path, root, checkout, rows))['result']


def _evaluate_rows(config, inputs, plan, manifest_path, root, checkout, recorded):
    raw=list(read_prepared(manifest_path,'raw.jsonl'))
    refs={r['row_id']:r for r in read_prepared(manifest_path,'references.jsonl')}
    if config.task in ('movie-q2','movie-q3'):
        selected=[row for row in raw if row[2]=='taken_3']
    elif config.movie_id is not None:
        selected=[row for row in raw if row[2]==config.movie_id]
    else:
        selected=raw
    texts={row[1]:inputs.convert(row)['input_text'] for row in selected}
    events=read_events(root/'events.jsonl')
    requests=[event['body'] for event in events if event['event']=='request']
    if any(body.get('model')!=plan.model_id for body in requests):
        raise ValueError('an outgoing request used another model')
    if config.arm=='lotus' and any(
        body.get('max_completion_tokens',body.get('max_tokens'))!=8 or body.get('temperature')!=0 or
        body.get('top_p')!=1 or body.get('stop')!=['\n'] or body.get('stream',False) is not False or
        body.get('n',1)!=1 for body in requests):
        raise ValueError('native LOTUS effective generation settings differ')
    report=dict(actual_posts=len(requests),http=http_accounting(events,config.concurrency))
    if report['http']['started_requests']!=len(requests):
        raise ValueError('outgoing POST and HTTP occupancy histories differ')
    if config.arm=='pg':
        active_work=config.concurrency
        report['plan']=pg_plan_summary(json.loads((root/'plan.json').read_text()))
        if config.organization_config is not None:
            from src.execution_provider.adapters.map_organization import MapOrganizationConfig
            from .organization_evaluation import verify_organization
            organization=MapOrganizationConfig.load(root/'organization.json')
            active_work=organization.active_work
            report['organization']=verify_organization(organization,events)
        if config.pg_total_budget:
            report['pg_memory']=verify_window_memory((root/'q0-producer.log').read_text().splitlines(),
                backend_pid=json.loads((root/'pg-backend.json').read_text())['backend_pid'],
                retained_limit=config.pg_window_bytes,staging_limit=config.pg_staging_bytes,window=config.window)
        report['logical_resources']=verify_logical_resources(events,
            dict(held_tasks=config.window,input_bytes=config.input_bytes,result_bytes=config.result_bytes,
                 active_requests=config.concurrency,active_work=active_work),require_usage=bool(requests))
        report['drained_jobs']=report['logical_resources']['drained_jobs']
    if config.task=='map':
        predictions=dict(recorded)
        if len(predictions)!=len(recorded) or set(predictions)!=set(texts) or any(not isinstance(v,str) for v in predictions.values()):
            raise ValueError('Map outputs are not exactly one text per source row')
        expected=Counter(content_digest(request_body(plan,text)) for text in texts.values())
        if Counter(map(content_digest,requests))!=expected:
            raise ValueError('Map actual request multiset differs from raw PG inputs')
        if config.arm=='pg' and selected:
            report['association']=verify_bound_map_results(texts.items(),predictions.items(),
                parse_pg_bindings((root/'q0-producer.log').read_text().splitlines()),events,
                read_events(root/'sessions.jsonl'),plan=plan)
        if config.arm in ('pg-source-direct','ray-data'):
            report['association']=verify_native_map_results(config.arm,texts,predictions,events,plan=plan,
                source_positions={row[1]:row[0] for row in selected})
        if inputs.kind=='movie':
            report['quality']=classification_audit({key:refs[key]['reference'] for key in texts},predictions.items())
        else:
            scores=[squad_example_scores(prediction,refs[key]['reference']) for key,prediction in predictions.items()]
            report['quality']=dict(rows=len(scores),exact_match_percent=100*sum(v[0] for v in scores)/len(scores) if scores else None,
                                    token_f1_percent=100*sum(v[1] for v in scores)/len(scores) if scores else None)
        return report
    query_id=int(config.task[-1])
    if config.arm=='pg':
        decisions=verify_filter_decisions((root/'q0-producer.log').read_text().splitlines(),texts,
                                         plan.model_id,complete=query_id==3)
        expected=Counter(content_digest(dict(model=plan.model_id,
            messages=json.loads(v3.canonical_messages(MOVIE_FILTER_INSTRUCTION,texts[key])),
            **v3.GENERATION_CONSTRAINTS)) for key in decisions)
        if Counter(map(content_digest,requests))!=expected:
            raise ValueError('Filter actual messages differ from their PG input bindings')
    else:
        raw_decisions=json.loads((root/'native-decisions.json').read_text())
        decisions=dict(raw_decisions)
        if len(decisions)!=len(raw_decisions) or set(decisions)!=set(texts):
            raise ValueError('native Filter did not audit its complete input relation')
        prompts=json.loads((root/'native-prompts.json').read_text()) if (root/'native-prompts.json').exists() else []
        if {p['row_id'] for p in prompts}!=set(decisions) or len(prompts)!=len(decisions):
            raise ValueError('native prompt binding set is incomplete')
        if Counter(content_digest(p['messages']) for p in prompts)!=Counter(content_digest(body['messages']) for body in requests):
            raise ValueError('native actual messages differ from its original prompt program')
    if len(requests)!=len(decisions) or any(type(v) is not bool for v in decisions.values()):
        raise ValueError('Filter is not one bounded invocation per evaluated row')
    kept={key for key,value in decisions.items() if value}
    if query_id==3:
        if recorded!=[[len(kept)]]:
            raise ValueError('COUNT output differs from actual kept-row decisions')
    else:
        returned=[row[0] for row in recorded]
        if len(returned)!=len(set(returned)) or len(returned)>5 or not set(returned)<=kept:
            raise ValueError('LIMIT output does not match kept source IDs')
        if len(returned)<5 and set(decisions)!=set(texts):
            raise ValueError('short LIMIT result did not exhaust the eligible source')
        if config.arm=='pg' and set(returned)!=kept:
            raise ValueError('strict-demand PG Filter evaluated an extra kept row')
    import pandas as pd
    labeled=pd.DataFrame([dict(reviewId=r['review_id'],id=r['movie_id'],scoreSentiment=r['reference']) for r in refs.values()])
    original_results=recorded if query_id==3 else [[refs[row[0]]['review_id']] for row in recorded]
    result=pd.DataFrame(original_results,columns=['count' if query_id==3 else 'reviewId'])
    report['quality']=json_metric(evaluate_original(checkout,query_id,result,labeled))
    report['row_audit']=classification_audit({key:refs[key]['reference'] for key in texts},
        [(key,'POSITIVE' if value else 'NEGATIVE') for key,value in decisions.items()],require_complete=query_id==3)
    report['evaluated_rows']=len(decisions)
    report['evaluated_but_not_returned_rows']=len(decisions)-len(recorded) if query_id!=3 else None
    if query_id!=3:
        passed=0
        report['evaluations_after_fifth_kept_in_input_order']=0
        for value in decisions.values():
            if passed>=5:
                report['evaluations_after_fifth_kept_in_input_order']+=1
            passed+=value
    return report
