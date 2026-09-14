"""Finite real-input M1 query schedule; the caller owns PG and the model service."""
from contextlib import ExitStack
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from statistics import median

from src.baselines.common.private_artifacts import new_private_directory, write_private_json
from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.adapters.map_organization import MapOrganizationConfig
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from .persistent_gateway import PersistentMapGateway
from .query_config import QueryConfig
from .query_inputs import QueryInputs
from .query_tables import install_input_table
from .query_workloads import load_manifest, read_prepared
from .waiting_positions import analyze_waiting_positions

CAPACITIES = (4,8,16,32,64)
MAX_POSTS = 44544


def select_capacity(samples):
    """Pick the smallest capacity within 97% of the observed median peak rate."""
    if set(samples)!=set(CAPACITIES) or any(len(values)!=2 or any(v<=0 for v in values) for values in samples.values()):
        raise ValueError('selection requires two positive query times for each declared capacity')
    medians={capacity:median(values) for capacity,values in samples.items()}
    fastest=min(medians.values())
    selected=min(capacity for capacity in CAPACITIES if fastest/medians[capacity]>=.97)
    return dict(selected=selected,medians=medians,rule='smallest C at least 97% of observed median completion rate',
        saturation_proven=False,short_query_diagnostic=True)


def run_campaign(config_path):
    """No service startup, retries, budget initialization, or implicit input selection."""
    import psycopg
    c=json.loads(Path(config_path).read_text());root=Path(c['output_root'])
    inputs_root=Path(c['inputs_root']);model_path=Path(c['model_config']);pg_log=Path(c['pg_log'])
    if c['max_posts']!=MAX_POSTS or c['max_seconds']!=1800:
        raise ValueError('campaign differs from declared finite allocation')
    manifests={key:load_manifest(inputs_root/key/'manifest.json') for key in ('tuning','evaluation')}
    if [manifests[key]['rows'] for key in ('tuning','evaluation')]!=[512,1024]:
        raise ValueError('campaign input counts differ')
    for key in manifests:
        if manifests[key]['sha256']!=c['manifest_sha256'][key]:raise ValueError('input identity differs')
    budget=AttemptBudget(c['budget_id'],MAX_POSTS);ledger=CellBudgetLedger(c['budget_path'],budget)
    plan=SemanticMapPlan(MOVIE_MAP_INSTRUCTION,c['model_id'],128)
    new_private_directory(root)
    tables={key:'m1_'+hashlib.sha256((str(root)+key).encode()).hexdigest()[:16] for key in manifests}
    with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
        for key in manifests:
            install_input_table(connection,QueryInputs('movie',tables[key],manifests[key]['rows']),
                                read_prepared(inputs_root/key/'manifest.json','raw.jsonl'))
    rows=[]
    def configuration(unit,split,capacity,mode,organization=None):
        options={}
        if organization is not None:
            path=root/(unit+'-organization.json');write_private_json(path,asdict(organization))
            options=dict(organization_config=str(path),organization_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        return QueryConfig(unit,'pg','map',tables[split],concurrency=capacity,window=128,
            input_bytes=33554432,result_bytes=33554432,pg_window_bytes=33554432,pg_staging_bytes=4194304,
            pg_total_budget=True,query_timeout_s=90,event_content=mode,**options)
    def open_group(stack,cfg,split,count):
        return stack.enter_context(PersistentMapGateway(cfg,plan=plan,manifest_path=inputs_root/split/'manifest.json',
            model_path=model_path,ledger=ledger,root=root/cfg.unit_id,query_count=count))
    def query(group,repeat,split,arm,pids):
        unit=f'q-{repeat}';summary=group.run_query(unit,dsn=os.environ['SEMLOOM_TEST_PG_DSN'],pg_log=pg_log,
            trace_flow=group.config.event_content=='full',peer_pids=pids)
        output=group.root/unit
        write_private_json(output/'waiting-contract.json',dict(eligibility='sealed-immutable-full-scan-at-invocation',
            manifest_sha256=summary['manifest_sha256'],role='warmup' if repeat==0 else split,
            arm=arm,repeat=repeat,driver='persistent',gateway='persistent'))
        execution=summary['execution']
        timing=dict(preparation_to_eof_seconds=(execution['t_query_terminal_ns']-summary['query_preparation_started_ns'])/1e9,
                    sql_to_eof_seconds=execution['query_jct_seconds'])
        if group.config.event_content=='full':
            timing=analyze_waiting_positions(output,expected_rows=manifests[split]['rows'],window=128)
            write_private_json(output/'waiting-positions.json',timing)
        quality=summary['evaluation']['quality']
        if quality['invalid']!=0:raise ValueError('model returned an invalid classification')
        record=dict(group=group.config.unit_id,repeat=repeat,warmup=repeat==0,split=split,arm=arm,
                    event_content=group.config.event_content,capacity=group.config.concurrency,timing=timing,
                    evaluation=summary['evaluation'])
        rows.append(record);write_private_json(root/f'completed-{len(rows):03}.json',record)
        return record
    with ExitStack() as stack:
        groups={capacity:open_group(stack,configuration('tune-'+str(capacity),'tuning',capacity,'compact'),'tuning',3)
                for capacity in CAPACITIES}
        pids={str(capacity):g.gateway.pid for capacity,g in groups.items()}
        times={capacity:[] for capacity in CAPACITIES}
        for repeat,order in enumerate((CAPACITIES,CAPACITIES,tuple(reversed(CAPACITIES)))):
            for capacity in order:
                record=query(groups[capacity],repeat,'tuning','request',pids)
                if repeat:times[capacity].append(record['timing']['preparation_to_eof_seconds'])
    selection=select_capacity(times);write_private_json(root/'selection.json',selection)
    capacity=selection['selected'];work=c['work_limits']
    if work['tight']!=4096 or work['wide']<work['tight']:
        raise ValueError('work limits differ from the declared input description')
    described=json.loads((inputs_root/'evaluation/work.json').read_text())['work']
    if sum(sorted(described,reverse=True)[:capacity]) <= work['tight']:
        write_private_json(root/'comparison.json',dict(status='inconclusive',selection=selection,rows=rows,
            reason='selected request capacity cannot trigger the declared work limit',actual_posts=7680))
        return
    with ExitStack() as stack:
        groups=[]
        for mode in ('full','compact'):
            for arm in ('request','wide','tight'):
                organization=None
                if arm!='request':
                    organization=MapOrganizationConfig('rows',128,128,work['wide'],work[arm],c['model_id'],
                        c['model_revision'],c['service_signature'],c['tokenizer_path'],c['tokenizer_fingerprint'],4096)
                cfg=configuration(mode+'-'+arm,'evaluation',capacity,mode,organization)
                groups.append((open_group(stack,cfg,'evaluation',6),arm))
        pids={g.config.unit_id:g.gateway.pid for g,_ in groups}
        # Six-group rotations fix order before outcomes; all groups remain resident.
        for repeat in range(6):
            shift=0 if repeat==0 else repeat-1
            for index in list(range(6))[shift:]+list(range(6))[:shift]:
                group,arm=groups[index];query(group,repeat,'evaluation',arm,pids)
    write_private_json(root/'comparison.json',dict(status='completed',rows=rows,selection=selection,
        actual_posts=sum(512 if r['split']=='tuning' else 1024 for r in rows),
        interpretation='real short-query comparison; no steady-state capacity claim'))


if __name__=='__main__':
    import sys
    run_campaign(sys.argv[1])
