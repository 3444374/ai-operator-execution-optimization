"""Explicit finite SemLoom diagnostic; no automatic retry or budget extension."""
from dataclasses import asdict
from pathlib import Path
import hashlib,json,os,sys,time
from src.baselines.common.private_artifacts import write_private_json
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.query_config import QueryConfig
from src.experiments.postgresql.query_inputs import QueryInputs
from src.experiments.postgresql.query_tables import install_input_table
from src.experiments.postgresql.query_workloads import load_manifest,read_prepared,file_identity
from src.experiments.postgresql.query_supervisor import supervise
from src.experiments.postgresql.text_map_comparison import read_run


def ref(p):return dict(path=str(p),sha256=file_identity(p)['sha256'])


def main(config_path):
 import psycopg,ray
 c=json.loads(Path(config_path).read_text());root=Path(c['output_root']);root.mkdir(mode=0o700)
 if c['max_posts']!=2064:raise ValueError('exact diagnostic budget required')
 os.environ['RAY_USAGE_STATS_ENABLED']='0'
 inputs=json.loads(Path(c['inputs_record']).read_text())['inputs'];installed={}
 with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
  if connection.info.server_version!=180003:raise ValueError('wrong PostgreSQL version')
  for split,value in inputs.items():
   m=load_manifest(value['path'])
   if m['sha256']!=value['manifest_sha256']:raise ValueError('source changed')
   table='diagnostic_'+split
   receipt=install_input_table(connection,QueryInputs('movie',table,m['rows'],m['max_source_bytes'],m['max_input_bytes']),read_prepared(value['path'],'raw.jsonl'))
   write_private_json(root/(split+'-installation.json'),dict(receipt,manifest_sha256=m['sha256']))
   installed[split]=table
 ray.init(address='local',num_cpus=8,num_gpus=0,include_dashboard=False,_node_ip_address='127.0.0.1',object_store_memory=268435456,_temp_dir=c['ray_root'])
 report=dict(status='failed',max_posts=2064,completed=[])
 try:
  address=ray.get_runtime_context().gcs_address
  write_private_json(root/'ray-runtime.json',dict(address=address,version=ray.__version__,owner='diagnostic',declared_cpus=8,object_store_bytes=268435456))
  physical=root/'transport.json';write_private_json(physical,dict(address=address,workers=1,batch_rows=16,window_bytes=2*1048576,object_bytes=4*1048576))
  configs=[]
  for unit,split,cap in [('check16','qualification',16)]+[(f'c128-r{i}','tuning',128) for i in range(4)]:
   cfg=QueryConfig(unit,'pg','map',installed[split],concurrency=cap,window=256,
    input_bytes=128*1048576,result_bytes=128*1048576,pg_window_bytes=256*1048576,pg_total_budget=True,pg_staging_bytes=4*1048576,
    query_timeout_s=120,max_posts=inputs[split]['rows'],map_transport_config=str(physical),map_transport_sha256=file_identity(physical)['sha256'],ray_num_cpus=8,ray_object_store_bytes=268435456)
   configs.append(dict(config=asdict(cfg),manifest=ref(Path(inputs[split]['path']))))
  spec=dict(purpose='finite diagnostic after unconfirmed remote execution; not baseline selection',configs=configs,max_posts=2064,deadline_utc=c['deadline_utc'],model=ref(Path(c['model_config'])),service_signature=c['service_signature'])
  write_private_json(root/'specification.json',spec)
  budget=AttemptBudget('semloom-ray-error-diagnostic',2064)
  ledger=CellBudgetLedger.create(root/'diagnostic-budget.sqlite',budget,deadline_utc=c['deadline_utc']-15)
  for item in configs:
   cfg=QueryConfig(**item['config']);remaining=c['deadline_utc']-time.time()-30
   if remaining<=0:raise TimeoutError('no diagnostic time remains')
   config=root/(cfg.unit_id+'.json');write_private_json(config,item['config'])
   output=root/cfg.unit_id;output.mkdir(mode=0o700)
   command=[sys.executable,'-m','src.experiments.postgresql.query_cli','run','--worker','--config',str(config),
    '--manifest',item['manifest']['path'],'--model',c['model_config'],'--budget',str(ledger.path),'--budget-id',budget.budget_id,
    '--max-attempts','2064','--output',str(output),'--dsn-env','SEMLOOM_TEST_PG_DSN','--pg-log',c['pg_log']]
   print(json.dumps(dict(event='started',unit=cfg.unit_id,rows=cfg.max_posts)),flush=True)
   supervise(command,output,lambda:ledger.close_shared_unit(cfg.unit_id),query_timeout_s=120,max_duration_s=min(135,remaining))
   result=read_run(ref(output/'unit/summary.json'),'pg-daft-ray')
   if result['rows']!=cfg.max_posts:raise ValueError('diagnostic rows differ')
   summary=json.loads((output/'unit/summary.json').read_text())
   if summary['evaluation']['actual_posts']!=cfg.max_posts:raise ValueError('diagnostic requests differ')
   report['completed'].append(dict(unit=cfg.unit_id,actual_posts=cfg.max_posts,summary=ref(output/'unit/summary.json')))
   print(json.dumps(dict(event='completed',unit=cfg.unit_id,actual_posts=cfg.max_posts)),flush=True)
  report.update(status='passed',actual_posts=sum(v['actual_posts'] for v in report['completed']))
 finally:
  ray.shutdown();report['ray_driver_disconnected']=not ray.is_initialized()
  write_private_json(root/'summary.json',report)

if __name__=='__main__':main(sys.argv[1])
