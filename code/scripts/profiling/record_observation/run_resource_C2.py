"""Configured public Movie session or separately labelled legal-chain capture."""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sys
import time


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()




def observer_environment(config):
    if config['observation']['mode']=='off':return {}
    return dict(SEM_RECORD_OBSERVATION='on',SEM_RECORD_OBSERVER_ROOT=str(config['artifact_root']),
                SEM_RECORD_OBSERVER_CODE=str(Path(config['source_root'])/'code'),
                SEM_RECORD_RUN_ID=config['run_id'],SEM_RECORD_SESSION_ID=config['cell_id'])


def call_records(root):
    records={}
    for path in (Path(root)/'session').glob('query-*.json'):
        result=json.loads(path.read_text());backend=result.get('backend_report') or {}
        for call in (backend.get('calls') or {}).get('records',[]):records[call['call_id']]=call
    return list(records.values())


_CALL_FIELDS=('query_id','model_role','algorithm_stage','row_id','source_position','call_index','call_id',
              'engine_request_id','engine_uid','core_task_key','parent_call_ids','model_start_monotonic',
              'model_end_monotonic','prompt_tokens','prompt_cached_tokens','completion_tokens',
              'total_tokens','status','submission_confirmed','engine_terminal_confirmed','messages_sha256')


def safe_calls(calls):return [{k:c[k] for k in _CALL_FIELDS if k in c} for c in calls]


def movie_session(config,ledger,*,entry=None,checker=None):
    native_entry=entry is None
    if entry is None or checker is None:
        from src.experiments.movie_records_session import run_movie_session,check_plan
        entry=entry or run_movie_session;checker=checker or check_plan
    plan=config['movie_plan']
    checked=checker(plan)
    expected={q['query_id']:q['expected_calls'] for q in checked['queries']}
    if checked['expected_calls']!=config['workload']['expected_calls'] or expected!=config['units']:
        raise ValueError('public whole-query call accounting differs from frozen declarations')
    claims={key:ledger.claim_unit(key) for key in expected}
    runtime=deepcopy(config['runtime_binding'])
    context=dict(owner_context=config['external_owner'],runtime_binding=runtime,claims=claims,
                 budget=dict(path=str(Path(config['artifact_root'])/'budget.sqlite'),id=config['run_id'],limit=checked['expected_calls']),
                 sembench_checkout=config['sembench_checkout'],worker_entry_ns=config['worker_entry_ns'],
                 worker_deadline_monotonic=time.monotonic()+config['limits']['worker_timeout_s'])
    observed=config['observation']['mode']=='on'
    observer=None
    if observed:
        from record_observer import emit,memory_snapshot
        def observer(event):
            emit('query_boundary',boundary=event['event'],query_id=event.get('query_id'))
            if event['event'] in ('result_ready','owner_ready','owner_closed'):memory_snapshot(event['event'])
    if not native_entry or not config.get('ray_options'):
        return entry(plan,context,Path(config['artifact_root'])/'session',observer=observer)
    # Existing private runner's runtime binding, applied identically on/off.
    # This configures Ray startup once; it is not a sampling/observation hook.
    import ray
    plasma=config['ray_options'].get('_plasma_directory')
    if plasma:Path(plasma).mkdir(mode=0o700,parents=True,exist_ok=True)
    original=ray.init
    def initialize(*args,**kwargs):
        options=deepcopy(config['ray_options'])
        if set(options)-{'object_store_memory','_plasma_directory','_temp_dir'}:
            raise ValueError('Movie runtime overrides must preserve public worker/device capacities')
        return original(*args,**dict(kwargs,**options))
    ray.init=initialize
    try:return entry(plan,context,Path(config['artifact_root'])/'session',observer=observer)
    finally:ray.init=original


def record_chain(config,ledger):
    from dataclasses import asdict
    import csv
    import itertools
    from src.experiments.record_chain_validation import make_chain,replay_native_answers
    from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
    from src.semantic_methods.lotus.record_chain import RecordOccurrence
    from src.semantic_methods.lotus.records import RecordsResources
    from src.serving.backends.vllm_engine import EngineSpec,GenerationSpec
    from transformers import AutoTokenizer
    import ray
    root=Path(config['artifact_root']);binding=config['workload']['input']
    path=Path(binding['csv'])
    if hashlib.sha256(path.read_bytes()).hexdigest()!=binding['sha256']:raise ValueError('chain input changed')
    with path.open(newline='') as stream:rows=list(itertools.islice(csv.DictReader(stream),config['workload']['rows']))
    records=[RecordOccurrence('movie:'+str(i),i,r[binding['id_column']],{'text':r[binding['text_column']]}) for i,r in enumerate(rows)]
    engine=EngineSpec.from_dict(config['engine']);generation=GenerationSpec.from_dict(config['generation'])
    resources=RecordsResources(**config['resources'])
    claim=ledger.claim_unit(next(iter(config['units'])));claim.bind_execution()
    chain=make_chain(config['model_name'],generation)
    tokenizer=AutoTokenizer.from_pretrained(engine.tokenizer_path,local_files_only=True)
    observed=config['observation']['mode']=='on'
    emit=(lambda *a,**k:None)
    if observed:
        from record_observer import emit
    env={k:v for k,v in os.environ.items() if k.startswith('SEM_RECORD_')}
    options=deepcopy(config['ray_options']);options['runtime_env']={'env_vars':env}
    started=time.monotonic_ns();emit('ray_init_start');ray.init(**options);emit('ray_init_end')
    owner=None;outputs=[];report={'Ray_init_s':(time.monotonic_ns()-started)/1e9}
    try:
        emit('source_read_end',rows=len(records),source_values_utf8_bytes=sum(len(r[binding['text_column']].encode()) for r in rows))
        emit('owner_construct_start')
        owner=SemLoomRecordChainCore(engine,resources,execution='record_local_sections',control_dir=root/'control')
        emit('owner_construct_end');emit('evaluate_start')
        stream=owner.evaluate(iter(records),chain,expected=records,query_id=chain.query_id);emit('evaluate_end')
        for row in stream:
            outputs.append(row);emit('consumer_record',row_id=row.row_id,source_position=row.source_position)
        close=owner.close(timeout=config['limits']['owner_close_s']);calls=list(owner.calls)
        if len(calls)!=config['workload']['expected_calls'] or close['status']!='closed':raise RuntimeError('chain accounting/close mismatch')
        if observed:
            from record_observer import set_phase
            set_phase('offline_replay')
        replay=replay_native_answers(records,chain,dict(calls=calls,records=outputs),tokenizer)
        if observed:set_phase('online')
        report.update(status='completed',calls=safe_calls(calls),close=close,replay=replay,output_rows=len(outputs),
                      scope='finite legal MMF mechanism; not a complete original SemBench query score')
    except BaseException as error:
        report.update(status='failed',error_type=type(error).__name__)
    finally:
        if owner is not None:
            try:report['final_close']=owner.close(timeout=config['limits']['owner_close_s'])
            except BaseException as error:report.update(status='failed',close_error_type=type(error).__name__)
        emit('ray_shutdown_start');started=time.monotonic_ns();ray.shutdown();emit('ray_shutdown_end')
        report['Ray_shutdown_s']=(time.monotonic_ns()-started)/1e9
        if owner is not None and 'calls' not in report:report['calls']=safe_calls(owner.calls)
    return report


def dispatch(config,ledger,*,movie_entry=None,movie_checker=None,chain_entry=None):
    if config['workload']['kind']=='movie_session':return movie_session(config,ledger,entry=movie_entry,checker=movie_checker)
    if config['workload']['kind']=='record_chain':return (chain_entry or record_chain)(config,ledger)
    raise ValueError('explicit existing public workload entry required')


def main(root):
    started=time.monotonic_ns();root=Path(root);config=json.loads((root/'config.json').read_text())
    if config.get('execution_enabled') is not True:raise RuntimeError('controller-owned execution binding required')
    sys.path.insert(0,str(Path(config['source_root'])/'code'))
    from integrations.imlane_records.chain_runtime import check_external_lease
    from src.experiments.attempt_ledger import AttemptBudget
    from src.experiments.cell_budget import CellBudgetLedger
    from src.baselines.common.private_artifacts import write_private_json
    check_external_lease(config['external_owner'])
    config['worker_entry_ns']=started
    os.environ.update(observer_environment(config))
    observed=config['observation']['mode']=='on'
    light=observed and config['observation'].get('profile')=='handoff'
    sampler=telemetry=None
    if observed:
        from record_observer import install_driver,memory_snapshot,emit,close_events
        install_driver()
        if not light:
            from src.experiments.process_sampling import ProcessSampler
            from src.observability.records_telemetry import RecordsTelemetry
            memory_snapshot('worker_entry');probe=config['observation']
            sampler=ProcessSampler(root/'rss.jsonl',{'driver':os.getpid()},interval=probe['sample_interval_s'],max_samples=probe['max_samples'],include_children=True)
            sampler.__enter__()
            telemetry=RecordsTelemetry(root/'telemetry',gpu_ids=[config['runtime_binding']['cuda_visible_devices']],interval_s=probe['sample_interval_s'],max_samples=probe['max_samples']).start()
    cap=config['workload']['expected_calls']
    ledger=CellBudgetLedger(root/'budget.sqlite',AttemptBudget(config['run_id'],cap))
    report=dict(status='failed',actual_calls=0,observation_kind='real_model_capture',scope=config['workload']['name'],
                source_commit=config['expected_source']['head'],workload_kind=config['workload']['kind'],
                generation_fingerprint=digest((config.get('movie_plan') or config)['generation']))
    try:
        result=dispatch(config,ledger)
        calls=call_records(root) if config['workload']['kind']=='movie_session' else result.get('calls',[])
        report.update(status=result['status'],actual_calls=len(calls),calls=safe_calls(calls),
                      public_result_path='session/session.json' if config['workload']['kind']=='movie_session' else None)
        if config['workload']['kind']=='record_chain':report.update(result)
        if result['status']=='completed' and len(calls)!=cap:raise RuntimeError('actual whole-workload call count differs from frozen budget')
        report['quality']=[{'query_id':q.get('query_id'),'quality':(q.get('backend_report') or {}).get('quality'),
                            'result':(q.get('backend_report') or {}).get('result')} for q in result.get('queries',[])]
    except BaseException as error:
        report.update(status='failed',error_type=type(error).__name__)
        if config['workload']['kind']=='movie_session':
            calls=call_records(root);report.update(actual_calls=len(calls),calls=safe_calls(calls))
    finally:
        if observed:
            if not light:memory_snapshot('worker_final')
            emit('worker_report',observed=report['actual_calls'])
            if sampler is not None:
                sampler.__exit__(None,None,None);report['rss']=sampler.summary();report['telemetry']=telemetry.close()
            close_events()
        report['worker_wall_s']=(time.monotonic_ns()-started)/1e9
        report['actual_call_count_complete']=report['status']=='completed' and report['actual_calls']==cap
        report['actual_calls_scope']='known ledger records; incomplete on failure'
        write_private_json(root/'WORKER-RESULT.json',report)
    print(json.dumps({key:report[key] for key in ('status','actual_calls','worker_wall_s')}),flush=True)
    if report['status']!='completed':raise SystemExit(1)


if __name__=='__main__':main(sys.argv[1])
