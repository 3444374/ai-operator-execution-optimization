"""Normal isolated Movie N entry with real Core/CPU IPC, fixture Ray, no model."""
import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('public',type=Path);parser.add_argument('output',type=Path)
parser.add_argument('scenario',choices=('off','on','prepare_error','prepare_error_off'))
args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=False)
os.environ['LITELLM_LOCAL_MODEL_COST_MAP']='True'
sys.path[:0]=[str(args.public/'code'),str(args.public/'code/scripts/profiling/record_observation')]
from tests.experiments import test_movie_async_token_entry as fixtures
from src.baselines.text.movie_records import MovieRecordSource
from src.experiments import movie_records_session as session
from src.execution_provider.adapters import lotus_records_actor_owner as owners
from src.semantic_methods.lotus import engine as methods

helper=fixtures.AsyncTokenEntryTests();helper.setUp()
try:
    runtime=helper.runtime();config=helper.config()
    mode='off'if args.scenario in ('off','prepare_error_off')else'on'
    inject_failure=args.scenario.startswith('prepare_error');broken=None
    (args.output/'config.json').write_text(json.dumps(dict(observation=dict(mode=mode,profile='full',
        max_events_per_process=1000,max_event_bytes=65536,max_pending_frame_metadata=4))))
    os.environ.update(SEM_RECORD_OBSERVATION=mode,SEM_RECORD_OBSERVER_ROOT=str(args.output))
    import record_observer as observer
    from record_ipc_observer import ObservedPrepareActor,install_prepare_close
    if mode=='on':
        # These are the same two ordinary-preparer bindings used by install_driver.
        owners._PrepareActor=ObservedPrepareActor
        install_prepare_close(owners.RecordsActorOwner)
    plan=dict(schema='semloom.movie_session_plan.v1',backend='semloom-core',lifecycle='isolated',
        model_name='openai/cpu-fixture',generation=config['generation'],engine=config['engine'],
        resources=config['resources'],profile=config['core_profile'],queries=[dict(query_id='N-prepare',
        task='movie-q1',manifest=helper.helper.manifests['movie-q1'],phase='measured')])
    assert not any(k in plan for k in ('prepared_batching','rolling_refill','batch_concurrency','batch_feedback'))
    checked=session.check_plan(plan);query=plan['queries'][0]
    source=MovieRecordSource.from_manifest(query['manifest'])
    runtime.rows=list(source.rows())
    helper.model.write_text(json.dumps(dict(responses={
        'filter|'+r['row_id']:dict(text=str(r['source_position']==0))for r in source.rows()})))
    context=dict(owner_context={},claims={'N-prepare':None},sembench_checkout=helper.root,
        checked_queries={q['query_id']:q for q in checked['queries']})
    driver=session._MovieDriver(plan,context,helper.root/'N-prepare',query)
    killed=[];original_kill=runtime.ray.kill
    def kill(actor,**kwargs):
        if isinstance(actor.instance,owners._PrepareActor):
            killed.append(bool(list((args.output/'events').glob('*.status.json'))))
        return original_kill(actor,**kwargs)
    runtime.ray.kill=kill
    failure=None;report=None
    with patch('integrations.imlane_records.chain_runtime.check_external_lease'):
        driver.start()
        try:
            if inject_failure:
                with patch.object(methods.LotusFilterMethod,'prepare_rows',side_effect=ValueError('CPU preparation failure'))as broken:
                    report=driver.query(query,lambda event:None)
            else:report=driver.query(query,lambda event:None)
        except BaseException as error:failure=error
        finally:closed=driver.close()
    if not inject_failure:assert closed['status']=='closed',closed
    actor_close=closed['receipts']['model']['model_owner']
    submits=[e for e in runtime.events if e[0]=='submit'and e[1].endswith('.close_observation')]
    if mode=='on':
        receipt,=actor_close['prepare_observation'];assert receipt['status']=='closed'and receipt['rpc_terminal_status']=='returned'
        assert not receipt['ref_retained']and killed==[True]
        footer=json.loads(next((args.output/'events').glob('*.status.json')).read_text())
        assert footer['closed']and footer['flush_confirmed']and footer['dropped']==footer['errors']==0
        assert len(submits)==1
    else:assert not submits and killed==[False]and'prepare_observation'not in actor_close
    if inject_failure:assert failure is not None and report is None and broken.called
    else:
        if failure is not None:raise failure
        assert report['status']=='completed'
    fields=('messages_sha256','prompt_token_ids_json','output_token_ids_json','text','decision',
        'prompt_tokens','completion_tokens','prompt_computed_tokens','prompt_cached_tokens','finish_reason')
    business=None if report is None else dict(result=report['result'],decisions=[{k:c.get(k)for k in fields}for c in sorted(report['decisions'],key=lambda c:c['source_position'])])
    digest=None if business is None else hashlib.sha256(json.dumps(business,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    result=dict(status='passed_actual_isolated_original_records_prepare_finalization',scenario=args.scenario,
        exact_original_N_plan=True,prepare_close_RPCS=len(submits),footer_before_kill=all(killed)if mode=='on'else None,
        business_sha256=digest,original_failure_preserved=failure is not None and broken.called if inject_failure else None,
        query_failure_type=None if failure is None else type(failure).__name__,outer_close_status=closed['status'],
        native_owner_status=actor_close['status'],actual_calls=0 if report is None else len(report['calls']['records']),
        Ray='explicit CPU fixture',IPC='actual CPU child/socket',real_model_calls=0,GPU=False)
    (args.output/'PUBLIC-PREPARE.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
finally:helper.doCleanups()
