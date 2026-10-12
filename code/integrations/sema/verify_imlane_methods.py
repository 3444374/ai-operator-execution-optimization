"""Bounded public LOTUS/DuckDB/Sema entries on both original IMLane modes."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import uuid

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))
from src.baselines.common.private_artifacts import write_private_json
from src.baselines.common.redact import redact_text
import verify_imlane_calls as fixture_module


METHOD_CLIENT = r'''
import json,os,pathlib,sys,time
code,config,native_source,tokenizer,result_file=sys.argv[1:]
sys.path.insert(0,code)
from src.baselines.common.private_artifacts import write_private_json
from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.shared_request_budget import CellBudgetLedger
from src.experiments.postgresql.supplier_adapter_query import run_supplier_query
from src.experiments.postgresql.persistent_native_adapter_query import PersistentAdapterGroup
from tests.experiments.test_native_adapter_query import fixture_server
value=json.loads(pathlib.Path(config).read_text());value.pop('fixture_endpoint')
cfg=IMLaneCallConfig(**value);mode=cfg.runtime
options=NativeGraphOptions(concurrency=8,num_threads=8)
root=pathlib.Path(result_file).parent/'method-queries';root.mkdir()
plan=SemanticMapPlan('Return ok.','fixture',16)
ledger=CellBudgetLedger.create(root/'budget.sqlite',AttemptBudget('imlane-public-methods',64),deadline_utc=time.time()+180)
values=[dict(row_id='a',text='same'),dict(row_id='b',text='same'),dict(row_id='c',text='中文'),dict(row_id='d',text='fourth')]
suffix='-ray' if mode=='ray' else ''
results=[];posts_total=0;models=0
for method in ('lotus','duckdb','sema'):
 arm=('sema-native-imlane'+suffix+'-request-service' if method=='sema' else method+'-method-native-imlane'+suffix)
 with fixture_server(content='"ok"' if method=='sema' else 'ok') as (endpoint,posts):
  result=run_supplier_query(arm,load_source=lambda:iter(values),plan=plan,
   model=FixedModelConfig(endpoint,'fixture',20000),ledger=ledger,unit_id=method+'-'+mode,
   root=root/method,options=options,query_timeout_s=20,
   imlane_call_configs={mode:cfg},tokenizer_path=pathlib.Path(tokenizer),
   duckdb_library=pathlib.Path(DUCKDB_EXTENSION),sema_binary=pathlib.Path(SEMA_BINARY),sema_native_threads=2,
   reference_outputs={r['row_id']:'ok' for r in values},allowed_outputs=('ok',))
  assert result['status']=='passed' and result['actual_posts']==len(posts)==4,result
  assert result['execution']['recorded_rows']==4,result['execution']
  assert result['identity']['imlane_runtime']==mode,result['identity']
  assert result['runtime']['owner']=='IMLane author Coordinator',result['runtime']
  if method=='sema':
   service=result['request_service']
   assert service['uncertain_requests']==0 and not service['native_bridge_thread_alive'],service
   assert service['held_requests']==0 and service['native_driver_stopped'],service
   close=service['native_close_reports']
   assert all(r['clean'] is True for r in close),close
  elif method=='duckdb':
   close=result['identity']['diagnostics']['executor']['last_close_report']
   assert close['clean'] is True and close['received']==4,close
  results.append(dict(arm=arm,passed=True,posts=len(posts),rows=4,identity=result['identity'],runtime=result['runtime']))
  posts_total+=len(posts)
  write_private_json(root/('progress-'+str(len(results))+'.json'),dict(fixture_posts=posts_total,results=results,models=0))
# Reuse caller-owned Coordinator and original LOTUS LM across two public group queries.
arm='lotus-method-native-imlane'+suffix
with fixture_server() as (endpoint,posts):
 with PersistentAdapterGroup((arm,),plan=plan,model=FixedModelConfig(endpoint,'fixture',20000),
  ledger=ledger,root=root/'persistent',options=options,query_timeout_s=20,
  imlane_call_configs={mode:cfg},tokenizer_path=pathlib.Path(tokenizer)) as group:
  for i in range(2):
   result=group.run(arm,unit_id='persistent-'+mode+'-'+str(i),root=root/('persistent-query-'+str(i)),
    load_source=lambda:iter(values[:2]),phase='qualification',allowed_outputs=('ok',),
    reference_outputs={r['row_id']:'ok' for r in values[:2]})
   assert result['status']=='passed' and result['actual_posts']==2,result
 assert len(posts)==4,posts
 posts_total+=4
results.append(dict(arm=arm,case='persistent-two-queries',passed=True,posts=4,rows=4))
assert posts_total==16,posts_total
write_private_json(pathlib.Path(result_file),dict(passed=True,fixture_posts=posts_total,
 methods=results,models=0,gpu_resources=0,retries=0,imlane_runtime=mode))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('author-root','output','bridge-binary','lotus-python','lotus-native-source',
                 'tokenizer','duckdb-extension','sema-binary'):
        parser.add_argument('--'+name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    token = 'im-method-' + uuid.uuid4().hex
    os.environ['SEMLOOM_IMLANE_METHOD_FIXTURE'] = token
    path = args.author_root/'harness/code/integrations/imlane/verify.py'
    spec = importlib.util.spec_from_file_location('fixed_author_helpers', path)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    fixture_module.LOTUS_CLIENT = (
        'DUCKDB_EXTENSION='+repr(str(args.duckdb_extension))+'\n'
        'SEMA_BINARY='+repr(str(args.sema_binary))+'\n'+METHOD_CLIENT)
    fixture = fixture_module.HTTPFixture()
    reports = []
    try:
        for mode in ('cpython','ray'):
            report = fixture_module.check_mode(args, mode, helper, fixture)
            client = report.get('stages',{}).get('lotus',{})
            if not client:
                # Failed clients still retain each public query's proxy/SQL facts.
                mode_root = next(args.output.glob(mode+'-*'))
                sums = [json.loads(p.read_text()) for p in (mode_root/'method-queries').glob('*/summary.json')]
                client = dict(passed=False,fixture_posts=sum(v.get('attempted_posts',v.get('actual_posts',0)) for v in sums),
                              recovery='public query summaries; client did not produce its final file')
            report['public_method_client'] = client
            report['total_fixture_posts'] = report['fixture_posts'] + client.get('fixture_posts',0)
            reports.append(report)
            if not report['passed']:
                break
        import psutil
        survivors = []
        for process in psutil.process_iter(['pid']):
            if process.pid == os.getpid():
                continue
            try:
                if process.environ().get('SEMLOOM_IMLANE_METHOD_FIXTURE') == token:
                    survivors.append(process.pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        result = dict(passed=len(reports)==2 and all(r['passed'] for r in reports) and not survivors,
            modes=reports,fixture_posts=sum(r['total_fixture_posts'] for r in reports),
            owned_tagged_survivors=survivors,models=0,gpu_resources=0,retries=0,
            fixture_posts_limit=128,
            verifier_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        write_private_json(args.output/'public-method-summary.json', result)
        print(json.dumps({k:v for k,v in result.items() if k!='modes'}))
        return 0 if result['passed'] and result['fixture_posts']<=128 else 1
    except BaseException as error:
        write_private_json(args.output/'public-method-error.json',dict(
            type=type(error).__name__,error=redact_text(str(error)),models=0,gpu_resources=0))
        raise
    finally:
        fixture.close()


if __name__ == '__main__':
    raise SystemExit(main())
