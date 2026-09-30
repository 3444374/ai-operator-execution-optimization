"""One authorized finite comparison; services and deadline are caller-owned."""
from dataclasses import asdict, replace
from pathlib import Path
import hashlib
import json
import os
import sys
import time

from src.baselines.common.private_artifacts import content_digest, write_private_json
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.query_config import QueryConfig
from src.experiments.postgresql.query_inputs import QueryInputs
from src.experiments.postgresql.query_tables import install_input_table
from src.experiments.postgresql.query_workloads import load_manifest, read_prepared, file_identity
from src.experiments.postgresql.text_map_comparison import MAIN_ROLES as ROLES, configuration_identity
from src.experiments.postgresql.text_map_campaign import SCHEMA, preflight, run_stage
from src.experiments.postgresql.text_map_candidates import main_candidates


def ref(path):
    return dict(path=str(path), sha256=file_identity(path)['sha256'])


def main(config_path):
    import psycopg
    import ray
    c=json.loads(Path(config_path).read_text())
    root=Path(c['output_root']);root.mkdir(mode=0o700)
    if c['max_posts']!=41024:
        raise ValueError('this worker requires the exact reviewed request allowance')
    os.environ['RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY']='1'
    os.environ['RAY_USAGE_STATS_ENABLED']='0'
    inputs=json.loads(Path(c['inputs_record']).read_text())['inputs']
    model=Path(c['model_config'])
    results=[]
    installed={}
    with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
        if connection.info.server_version!=180003:
            raise ValueError('PostgreSQL version differs from the plan')
        for split,data in inputs.items():
            manifest=load_manifest(data['path'])
            if manifest['sha256']!=data['manifest_sha256']:
                raise ValueError('prepared source identity changed')
            table='comparison_'+split
            record=install_input_table(connection,QueryInputs('movie',table,manifest['rows'],
                manifest['max_source_bytes'],manifest['max_input_bytes']),read_prepared(data['path'],'raw.jsonl'))
            path=root/(split+'-installation.json')
            write_private_json(path,dict(record,manifest_sha256=manifest['sha256']))
            installed[split]=(table,ref(path))
    ray.init(address='local',num_cpus=8,num_gpus=0,include_dashboard=False,
             _node_ip_address='127.0.0.1',object_store_memory=268435456,_temp_dir=c['ray_root'])
    address=ray.get_runtime_context().gcs_address
    report=dict(status='failed',max_posts=41024,stages=results)
    try:
        write_private_json(root/'ray-runtime.json',dict(version=ray.__version__,address=address,
            declared_cpus=8,object_store_bytes=268435456,owner='campaign',gpu_resources=0))
        transport=root/'transport.json'
        write_private_json(transport,dict(address=address,workers=1,batch_rows=16,
            window_bytes=2*1048576,object_bytes=4*1048576))
        def candidates_for(split):
            return main_candidates(installed[split][0],inputs[split]['rows'],ray_address=address,
                transport_path=str(transport),transport_sha256=file_identity(transport)['sha256'])
        tuning_candidates=candidates_for('tuning')
        selected=None
        for stage,limit,seconds in (('qualification',64,300),('tuning',24576,1500),('evaluation',16384,1200)):
            if time.time()>=c['deadline_utc']-30:
                raise TimeoutError('campaign has no remaining stage and cleanup time')
            if stage=='qualification':
                candidates=[v for v in candidates_for(stage) if v['config']['concurrency']==16]
            elif stage=='tuning':candidates=tuning_candidates
            else:
                candidates=[]
                for role in ROLES:
                    matches=[v for v in tuning_candidates if v['role']==role and
                             configuration_identity(v['config'])==selected['selected'][role]]
                    if len(matches)!=1:raise ValueError('tuning choice is not one declared candidate')
                    chosen=matches[0]
                    cfg=replace(QueryConfig(**chosen['config']),table=installed[stage][0],max_posts=inputs[stage]['rows'])
                    candidates.append(dict(chosen,config=asdict(cfg)))
            keys=[v['id'] for v in candidates]
            shifts=[0] if stage=='qualification' else [0,3,6,9] if stage=='tuning' else [0,1,2,3]
            orders=[keys[shift:]+keys[:shift] for shift in shifts]
            budget_id='text-map-real02-'+stage
            budget_path=root/(stage+'-budget.sqlite')
            spec=dict(schema=SCHEMA,profile='main',stage=stage,repeats=3,candidates=candidates,orders=orders,
                max_posts=limit,max_seconds=seconds,manifest=ref(Path(inputs[stage]['path'])),model=ref(model),
                installation=installed[stage][1],environment=ref(Path(c['environment_report'])),
                service_signature=c['service_signature'],dsn_env='SEMLOOM_TEST_PG_DSN',
                budget_path=str(budget_path),budget_id=budget_id,output_root=str(root/stage),pg_log=c['pg_log'])
            if stage=='evaluation':spec['tuning_result']=ref(root/'tuning/result.json')
            check=preflight(spec)
            spec_path=root/(stage+'.json');write_private_json(spec_path,spec)
            write_private_json(root/(stage+'-preflight.json'),check)
            CellBudgetLedger.create(budget_path,AttemptBudget(budget_id,limit),
                deadline_utc=min(time.time()+seconds,c['deadline_utc']-15))
            print(json.dumps(dict(stage=stage,event='started',queries=check['queries'],max_posts=limit)),flush=True)
            result=run_stage(spec_path)
            actual=sum(row['actual_posts'] for row in result['completed'])
            if actual!=limit:raise ValueError('completed stage count differs')
            results.append(dict(stage=stage,actual_posts=actual,queries=len(result['completed']),result=ref(root/stage/'result.json')))
            if stage=='tuning':selected=result
            print(json.dumps(dict(stage=stage,event='completed',actual_posts=actual)),flush=True)
        report.update(status='passed',actual_posts=sum(v['actual_posts'] for v in results))
    finally:
        ray.shutdown()
        report['ray_driver_disconnected']=not ray.is_initialized()
        write_private_json(root/'summary.json',report)
    if report['status']!='passed':raise ValueError('campaign did not finish')


if __name__=='__main__':
    main(sys.argv[1])
