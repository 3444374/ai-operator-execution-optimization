"""Existing lease/supervisor around frozen configured observer cells; no retries."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from run_resource_C2 import observer_environment

RUNTIME_FILES={'record_gpu_observer.py','record_observer.py','record_ipc_observer.py',
               'run_resource_C2.py','control_resource_C2.py','record_model_bootstrap.py','sitecustomize.py'}


def profiled(config):
    return config['observation']['mode']=='on'and config['observation'].get('profile')!='handoff'


def checked_json(binding):
    path=Path(binding['path'])
    if hashlib.sha256(path.read_bytes()).hexdigest()!=binding['sha256']:raise ValueError('private frozen file changed')
    return json.loads(path.read_text())


def source_identity(config):
    root=Path(config['source_root'])
    def git(*args):return subprocess.check_output(['git',*args],cwd=root,text=True).strip()
    observed=dict(head=git('rev-parse','HEAD'),tree=git('rev-parse','HEAD^{tree}'))
    if observed!=config['expected_source'] or git('status','--porcelain','--untracked-files=no'):
        raise ValueError('source head/tree or tracked state differs from frozen cell')
    return observed


def worker_command(config):
    worker=[(config.get('movie_plan') or config)['engine']['driver_python'],'-u','-B',
            str(Path(__file__).parent/'run_resource_C2.py'),config['artifact_root']]
    if not profiled(config):return worker
    probe=config['observation']
    return [probe['nsys_executable'],'profile','--trace=cuda,nvtx,osrt','--sample=none','--cpuctxsw=none',
            '--cuda-graph-trace=node','--capture-range=cudaProfilerApi','--capture-range-end=stop',
            '--flush-on-cudaprofilerstop=false','--duration='+str(probe['trace_duration_s']),
            '--kill=none','--wait=all','--force-overwrite=false',
            '--output='+str(Path(config['artifact_root'])/'trace'/config['cell_id']),*worker]


def worker_environment(config,current):
    env={k:v for k,v in current.items() if not k.startswith('SEM_RECORD_')}
    env.update(config['environment'])
    for key in tuple(env):
        if key.startswith('SEM_RECORD_'):env.pop(key)
    env.update(observer_environment(config))
    env.update(PYTHONDONTWRITEBYTECODE='1',HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',
               LITELLM_LOCAL_MODEL_COST_MAP='True',VLLM_WORKER_MULTIPROC_METHOD='spawn')
    env['CUDA_VISIBLE_DEVICES']=config['runtime_binding']['cuda_visible_devices']
    code=str(Path(config['source_root'])/'code')
    env['PYTHONPATH']=code+(os.pathsep+str(Path(__file__).parent) if config['observation']['mode']=='on' else '')
    if profiled(config):env['TMPDIR']=config['nsys_tmp']
    return env


def check_material(config):
    cpu=checked_json(config['cpu_check'])
    if config['observation']['mode']not in ('off','on'):raise ValueError('unknown observation mode')
    if config['observation'].get('profile','full')not in ('full','handoff'):raise ValueError('unknown observation profile')
    if cpu['status']!='passed_CPU_fixtures':raise ValueError('CPU observer qualification required')
    if set(config['observer_sha256'])!=RUNTIME_FILES:raise ValueError('freeze all five changed files and both reused bootstrap files')
    for name,digest in config['observer_sha256'].items():
        actual=hashlib.sha256((Path(__file__).parent/name).read_bytes()).hexdigest()
        if digest!=actual or cpu['observer_sha256'].get(name)!=actual:raise ValueError('observer changed after CPU qualification')
    expected=sum(config['units'].values())
    if expected!=config['workload']['expected_calls']:raise ValueError('unit reservations must exactly cover declared work')
    if config['observation']['mode']=='on':
        required=('max_events_per_process','max_event_bytes')
        if profiled(config):required+=('max_pending_frame_metadata','max_samples')
        for key in required:
            if type(config['observation'][key]) is not int or config['observation'][key]<=0:raise ValueError('positive explicit operational observer bounds required')
    if config.get('execution_enabled') is not False:raise ValueError('prepared cell must not already be enabled')


def trace_storage_bytes(directories):
    total = 0
    for directory in directories:
        for path in directory.rglob('*'):
            try:
                if path.is_file():
                    total += path.stat().st_size
            except FileNotFoundError:
                # Nsight removes transient injection metadata while profiling.
                continue
    return total


def capture_status(root):
    sinks=[];parse_errors=0;sample_known=sample_unknown=0
    for path in (Path(root)/'events').glob('*.jsonl'):
        pid=int(path.stem.split('-')[-1]);status=path.with_suffix('.status.json')
        value=json.loads(status.read_text()) if status.exists() else None
        sinks.append(dict(pid=pid,receipt=value,flush_status='observed' if value and value.get('flush_confirmed') else 'unknown'))
        for line in path.read_text().splitlines():
            try:event=json.loads(line)
            except json.JSONDecodeError:parse_errors+=1;continue
            if event['event']=='iteration_sample_start':
                if isinstance(event.get('scheduled_tokens'),dict):sample_known+=1
                else:sample_unknown+=1
    return dict(observed_sinks=sinks,event_parse_errors=parse_errors,
                observed_sample_member_sets=sample_known,unknown_sample_member_sets=sample_unknown,
                CUPTI_capture_complete=None,expected_process_role_coverage=None,
                scope='observed sink receipts only; inspect profiler export/drop diagnostics and process identities before qualification')


def main(root):
    root=Path(root);prepared=json.loads((root/'PREPARED-CONFIG.json').read_text())
    check_material(prepared);identity=source_identity(prepared)
    binding=prepared['host_helpers'];path=Path(binding['path'])
    if hashlib.sha256(path.read_bytes()).hexdigest()!=binding['sha256']:raise ValueError('existing host helper changed')
    spec=importlib.util.spec_from_file_location('capture_host_helpers',path);H=importlib.util.module_from_spec(spec);spec.loader.exec_module(H)
    api=H.production_api(Path(prepared['source_root'])/'code');host=checked_json(prepared['host_config'])
    started=time.monotonic();owned=set();lease=None
    result=dict(status='failed',actual_known_model_requests=0,actual_call_count_complete=False,
                declared_call_bound=prepared['workload']['expected_calls'],source=identity,
                observation=prepared['observation']['mode'])
    def write(name,value):
        path=root/name
        with path.open('x') as stream:json.dump(value,stream,indent=2)
        path.chmod(0o600)
    try:
        lease=api.acquire_runner_lease(Path(host['lease_scope']),config_fingerprint=api.HOST_SCOPE_FINGERPRINT,
                                     repository_commit=identity['head'],recover_stale=False)
        before=H.snapshot(api,host,'resource-capture-before',owned);write('BEFORE.json',before);H.assert_idle(before,host)
        config=dict(prepared,execution_enabled=True,external_owner=dict(artifact_root=str(lease.path.parent.parent),
                    lease_sha256=hashlib.sha256(lease.path.read_bytes()).hexdigest()))
        write('config.json',config)
        cap=config['workload']['expected_calls']
        ledger=api.CellBudgetLedger.create(root/'budget.sqlite',api.AttemptBudget(config['run_id'],cap),
                                           deadline_utc=time.time()+config['limits']['controller_timeout_s'])
        for unit,requests in config['units'].items():ledger.reserve_unit(unit,requests)
        command=worker_command(config)
        if profiled(config):(root/'trace').mkdir(mode=0o700,exist_ok=False)
        write('COMMAND-IDENTITY.json',dict(source=identity,observer_sha256=config['observer_sha256'],
               entry='movie_records_session' if config['workload']['kind']=='movie_session' else 'existing finite chain',
               observed=config['observation']['mode']=='on',config_sha256=hashlib.sha256((root/'config.json').read_bytes()).hexdigest(),
               scope='private command/environment paths retained in frozen config, not emitted into diagnostics'))
        env=worker_environment(config,os.environ)
        if profiled(config):Path(config['nsys_tmp']).mkdir(mode=0o700,exist_ok=True)
        os.environ.clear();os.environ.update(env)
        if prepared.get('cpu_affinity') is not None:os.sched_setaffinity(0,set(prepared['cpu_affinity']))
        last_sample=[0.0];snapshots=[]
        def monitor(identities):
            owned.update(identities);H.check_lease(lease)
            if time.monotonic()-started>config['limits']['controller_timeout_s']:raise TimeoutError('controller operational timeout')
            if profiled(config):
                directories={root/'trace',Path(config['nsys_tmp'])}
                total=trace_storage_bytes(directories)
                if total>config['observation']['trace_storage_bytes']:raise RuntimeError('trace storage bound')
                if time.monotonic()-last_sample[0]>=config['observation']['physical_gpu_interval_s']:
                    lower=time.monotonic_ns()
                    sample=subprocess.run([config['observation']['nvidia_smi_executable'],'-i',config['runtime_binding']['cuda_visible_devices'],
                        '--query-gpu=index,uuid,memory.used,utilization.gpu,power.draw','--format=csv,noheader,nounits'],
                        capture_output=True,text=True,timeout=config['observation']['probe_timeout_s'],check=True)
                    snapshots.append(dict(lower_monotonic_ns=lower,upper_monotonic_ns=time.monotonic_ns(),values=sample.stdout.strip()))
                    (root/'GPU-SNAPSHOTS.json').write_text(json.dumps(snapshots));last_sample[0]=time.monotonic()
        supervision=api.supervise(command,root/'supervision',timeout_s=config['limits']['worker_timeout_s'],
                   grace_s=config['limits']['cleanup_grace_s'],natural_exit_grace_s=config['limits']['natural_exit_grace_s'],monitor=monitor)
        owned.update(H.report_identities(supervision))
        report=json.loads((root/'WORKER-RESULT.json').read_text())
        result.update(status=report['status'],actual_known_model_requests=report['actual_calls'],actual_call_count_complete=report['actual_call_count_complete'])
    except BaseException as error:
        owned.update(H.report_identities(getattr(error,'report',{})))
        result['error_type']=type(error).__name__
        if (root/'WORKER-RESULT.json').exists():
            partial=json.loads((root/'WORKER-RESULT.json').read_text())
            result.update(actual_known_model_requests=partial.get('actual_calls',0),
                          actual_call_count_complete=partial.get('actual_call_count_complete',False))
    finally:
        cleanup=H.cleanup_owned(api,owned,grace_s=prepared['limits']['cleanup_grace_s'])
        final=H.snapshot(api,host,'resource-capture-final',owned);idle=False
        try:H.assert_idle(final,host);idle=not H.live_owned(api,owned)
        except BaseException as error:result['final_inventory_error_type']=type(error).__name__
        write('FINAL.json',dict(cleanup=cleanup,snapshot=final,physical_cleanup_confirmed=idle))
        if prepared['observation']['mode']=='on':write('CAPTURE-STATUS.json',capture_status(root))
        result['physical_cleanup_confirmed']=idle
        if lease is not None and idle:H.check_lease(lease);lease.release();result['lease_released']=True
        else:result['lease_released']=False
        result['elapsed_s']=time.monotonic()-started;write('RESULT.json',result)
    print(json.dumps(result),flush=True)
    if result['status']!='completed' or not result['physical_cleanup_confirmed'] or not result['lease_released']:raise SystemExit(1)


if __name__=='__main__':main(sys.argv[1])
