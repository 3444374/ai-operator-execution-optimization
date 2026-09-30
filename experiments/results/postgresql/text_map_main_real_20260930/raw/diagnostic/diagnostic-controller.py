"""Owned services for the approved 2064-POST, fifteen-minute text Map diagnostic."""
from contextlib import ExitStack
from pathlib import Path
import argparse
import hashlib
import json
import os
import pwd
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.request
import psutil

from src.infrastructure.environment import load_env_file
from src.baselines.common.private_artifacts import content_digest,write_private_json
from src.baselines.common.redact import redact_text
from src.experiments.postgresql.runtime_helpers import isolated_pg18_cluster
from src.experiments.process_sampling import ProcessSampler
from src.observability.metrics.timing import PeriodicSampler
from src.observability.metrics.vllm import scrape_prometheus_metrics

parser=argparse.ArgumentParser()
parser.add_argument('--allow-model-posts',type=int,required=True)
args=parser.parse_args()
if args.allow_model_posts!=2064:raise SystemExit('exact reviewed allowance required; no model started')
base=Path('<run-root>')
source=base/'src-git'
root=base/'diag01';root.mkdir(mode=0o700)
user=pwd.getpwnam('postgres')
os.chown(root,user.pw_uid,user.pw_gid)
started=time.time();deadline=started+900;work_until=deadline-120
model_process=worker=None
report=dict(status='failed',errors=[],max_posts=2064,max_seconds=900,gpu='0',started_utc=started)
inputs=json.loads((base/'assets/diagnostic-inputs.json').read_text())['inputs']
paths={Path('/root'),Path('<data-root>'),base,base/'assets',base/'preflight-diagnostic.json',base/'assets/diagnostic-inputs.json'}
for value in inputs.values():
    directory=Path(value['path']).parent
    paths.update([directory,*directory.iterdir()])
    paths.update(p for p in directory.parents if p!=base and p.is_relative_to(base))
acl={str(p):subprocess.check_output(['getfacl','-p',str(p)],text=True) for p in paths}
write_private_json(root/'acl-before.json',acl)

def verify(path,expected):
    digest=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(8*1048576),b''):
            if time.time()>=work_until:raise TimeoutError('campaign duration reached during identity check')
            digest.update(block)
    if digest.hexdigest()!=expected:raise ValueError('source/model identity changed: '+Path(path).name)

def stop_worker():
    if worker is None or worker.poll() is not None:return
    owned=[]
    with __import__('contextlib').suppress(psutil.NoSuchProcess):
        owned=psutil.Process(worker.pid).children(recursive=True)
    worker.send_signal(signal.SIGINT)
    try:worker.wait(10)
    except subprocess.TimeoutExpired:
        for p in owned:
            with __import__('contextlib').suppress(psutil.NoSuchProcess):p.terminate()
        _,alive=psutil.wait_procs(owned,timeout=10)
        for p in alive:
            with __import__('contextlib').suppress(psutil.NoSuchProcess):p.kill()
        worker.kill();worker.wait(10)

def stop_model():
    if model_process is None or (root/'model-stop.json').exists():return
    forced=False
    if model_process.poll() is None:os.killpg(model_process.pid,signal.SIGTERM)
    try:model_process.wait(30)
    except subprocess.TimeoutExpired:
        forced=True;os.killpg(model_process.pid,signal.SIGKILL);model_process.wait(10)
    write_private_json(root/'model-stop.json',dict(returncode=model_process.returncode,forced=forced))

def fail(phase,error):
    report['errors'].append(dict(phase=phase,type=type(error).__name__,message=redact_text(str(error))))

try:
    if json.loads((base/'preflight-diagnostic.json').read_text())['status']!='ok':raise ValueError('Map environment check failed')
    controlled=json.loads((base/'err01/controller-summary.json').read_text())
    if controlled['status']!='passed' or controlled['tests']!=7:raise ValueError('controlled query/campaign checks did not pass')
    native=json.loads((base/'ns06/controller-summary.json').read_text())
    if native['status']!='passed':raise ValueError('native supply validation did not pass')
    snapshot=json.loads((base/'source-diagnostic.json').read_text())
    for name,digest in snapshot.items():verify(source/name,digest)
    reference=json.loads((base/'model-reference.json').read_text())
    model=Path('<data-root>/models/Qwen2.5-7B-Instruct')
    for name,digest in reference['files'].items():verify(model/name,digest)
    if subprocess.check_output(['nvidia-smi','-i','0','--query-compute-apps=pid','--format=csv,noheader,nounits'],text=True).strip():
        raise RuntimeError('selected GPU is occupied')
    for port in (18092,55494):
        with socket.socket() as probe:probe.bind(('127.0.0.1',port))
    for p in paths:
        access='--x' if p in (Path('/root'),Path('<data-root>'),base) else 'r-x' if p.is_dir() else 'r--'
        subprocess.run(['setfacl','-m','u:postgres:'+access,str(p)],check=True)
    write_private_json(root/'identity.json',dict(source=snapshot,model=reference,
        library_sha256=hashlib.sha256(Path('/opt/semloom-pg18.3-tmc-20260930/lib/postgresql/semloom_pg.so').read_bytes()).hexdigest(),
        worker_sha256=hashlib.sha256((base/'diagnostic-worker.py').read_bytes()).hexdigest()))
    with ExitStack() as stack:
        pgroot=root/'pg';pgroot.mkdir(mode=0o700);os.chown(pgroot,user.pw_uid,user.pw_gid)
        connection=stack.enter_context(isolated_pg18_cluster(Path('/opt/semloom-pg18.3-tmc-20260930'),pgroot,user,port=55494))
        runtime=load_env_file(Path('<data-root>/ai-operator-runtime.env'))
        cache=root/'cache';cache.mkdir(mode=0o700)
        env=dict(os.environ,CUDA_VISIBLE_DEVICES='0',HF_HUB_OFFLINE='1',CUDA_HOME=runtime['CUDA_HOME'],
            PATH='<data-root>/venvs/vllm-4090/bin:'+runtime['CUDA_NVCC_BIN']+':'+os.environ['PATH'],
            HF_HOME=str(cache/'hf'),XDG_CACHE_HOME=str(cache/'xdg'),VLLM_CACHE_ROOT=str(cache/'vllm'),
            TORCHINDUCTOR_CACHE_DIR=str(cache/'torch'),FLASHINFER_WORKSPACE_BASE=str(cache/'flashinfer'),
            TRITON_CACHE_DIR=str(cache/'triton'),TMPDIR=str(cache),TOKENIZERS_PARALLELISM='false')
        service_args=['--model',str(model),'--tokenizer',str(model),'--served-model-name','Qwen2.5-7B-Instruct',
            '--dtype','bfloat16','--max-model-len','4096','--gpu-memory-utilization','0.8','--scheduling-policy','fcfs',
            '--max-num-seqs','128','--max-num-batched-tokens','8192','--tensor-parallel-size','1',
            '--host','127.0.0.1','--port','18092','--generation-config','vllm','--no-enable-prefix-caching','--enable-chunked-prefill']
        command=['<data-root>/venvs/vllm-4090/bin/python',str(source/'code/scripts/services/launch_vllm_with_identity.py'),
                 '--identity-output',str(root/'vllm-identity.json'),'--port','18092','--',*service_args]
        write_private_json(root/'service-command.json',command)
        model_log=stack.enter_context((root/'model.log').open('x'))
        model_process=subprocess.Popen(command,stdout=model_log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
        stack.callback(stop_model)
        ready_until=min(work_until,time.time()+300);url='http://127.0.0.1:18092'
        while time.time()<ready_until:
            if model_process.poll() is not None:raise RuntimeError('model startup failed; no retry')
            try:
                with urllib.request.urlopen(url+'/health',timeout=1) as response:
                    if response.status==200:break
            except OSError:time.sleep(.5)
        else:raise TimeoutError('model startup exceeded five minutes')
        identity=json.loads((root/'vllm-identity.json').read_text())
        if identity['package_version']!='0.25.1':raise ValueError('vLLM version differs')
        with urllib.request.urlopen(url+'/v1/models',timeout=2) as response:served=json.load(response)
        if [v['id'] for v in served['data']]!=['Qwen2.5-7B-Instruct']:raise ValueError('served model differs')
        actual=Path('/proc/'+str(model_process.pid)+'/cmdline').read_bytes().decode().split('\0')[:-1]
        if actual[-len(service_args):]!=service_args:raise ValueError('actual serving arguments differ')
        write_private_json(root/'service-ready.json',dict(ready_utc=time.time(),actual_argv=actual,model_id_verified=True))
        def sample():
            value=dict(monotonic_ns=time.monotonic_ns())
            try:value['metrics']=scrape_prometheus_metrics(url+'/metrics',timeout_s=2)
            except BaseException as error:value['metrics_error']=redact_text(str(error))
            try:value['gpu']=subprocess.check_output(['nvidia-smi','-i','0','--query-gpu=memory.used,utilization.gpu,power.draw',
                '--format=csv,noheader,nounits'],text=True,timeout=3).strip()
            except BaseException as error:value['gpu_error']=redact_text(str(error))
            return value
        stack.enter_context(ProcessSampler(root/'model-rss.jsonl',{'model_server':model_process.pid},interval=1,include_children=True))
        sampler=PeriodicSampler(sample,interval_s=1)
        def finish_samples():sampler.close();write_private_json(root/'service-samples.json',list(sampler.samples))
        stack.callback(finish_samples)
        before=sample();write_private_json(root/'service-before.json',before)
        if 'metrics' not in before:raise ValueError('service metrics unavailable before queries')
        model_config=root/'model.json';write_private_json(model_config,dict(endpoint_url=url+'/v1/chat/completions',model_id='Qwen2.5-7B-Instruct',timeout_ms=30000))
        os.chown(model_config,user.pw_uid,user.pw_gid)
        worker_config=dict(output_root=str(root/'worker'),ray_root=str(root/'r'),pg_log=str(pgroot/'postgres.log'),
            model_config=str(model_config),inputs_record=str(base/'assets/diagnostic-inputs.json'),
            environment_report=str(base/'preflight-diagnostic.json'),deadline_utc=work_until,max_posts=2064,
            service_signature=content_digest(dict(args=service_args,revision=reference['revision'],gpu='0',
                ray_cpus=8,ray_object_store_bytes=268435456,ray_version='2.56.1',daft_version='0.7.21',
                source=content_digest(snapshot),executor_lifecycle='per-query',comparison_profile='diagnostic',
                semloom_workers=1,semloom_batch_rows=16,pg_window=256)))
        file=root/'worker-config.json';write_private_json(file,worker_config);os.chown(file,user.pw_uid,user.pw_gid)
        child=['runuser','-u','postgres','--','env','PYTHONPATH='+str(source/'code'),'OMP_NUM_THREADS=1','OPENBLAS_NUM_THREADS=1',
            'TOKENIZERS_PARALLELISM=false','CUDA_VISIBLE_DEVICES=','TMPDIR='+str(root/'driver-tmp'),'HOME='+str(root/'driver-home'),
            'XDG_CACHE_HOME='+str(root/'driver-cache'),'SEMLOOM_TEST_PG_DSN='+connection.info.dsn,
            '<data-root>/venvs/text-baselines/bin/python',str(base/'diagnostic-worker.py'),str(file)]
        for name in ('driver-tmp','driver-home','driver-cache'):
            p=root/name;p.mkdir(mode=0o700);os.chown(p,user.pw_uid,user.pw_gid)
        worker_log=stack.enter_context((root/'worker.log').open('x'))
        print('Starting approved finite comparison: 2064 POSTs maximum',flush=True)
        worker=subprocess.Popen(child,stdout=worker_log,stderr=subprocess.STDOUT,start_new_session=True)
        stack.callback(stop_worker)
        stack.enter_context(ProcessSampler(root/'worker-tree-rss.jsonl',{'campaign_worker':worker.pid},interval=1,include_children=True))
        worker.wait(timeout=max(1,work_until-time.time()))
        if worker.returncode:raise RuntimeError('comparison worker failed; no retry')
        result=json.loads((root/'worker/summary.json').read_text());report['worker_summary']=result
        if result['status']!='passed' or result['actual_posts']!=2064:raise ValueError('campaign did not complete its declared stages')
        final=sample();write_private_json(root/'service-after.json',final)
        if any(final.get('metrics',{}).get(k)!=0 for k in ('vllm:num_requests_running','vllm:num_requests_waiting')):
            raise ValueError('service occupancy not confirmed idle')
        successes=final['metrics'].get('vllm:request_success_total',-1)-before['metrics'].get('vllm:request_success_total',0)
        report['server_success_delta']=successes
        if successes!=2064:raise ValueError('server success count differs from completed requests')
        report['status']='passed'
except BaseException as error:fail('run',error)
finally:
    for phase,callback in (('worker_cleanup',stop_worker),('model_cleanup',stop_model)):
        try:callback()
        except BaseException as error:fail(phase,error)
    for path,value in acl.items():
        try:subprocess.run(['setfacl','--restore=-'],input=value,text=True,check=True)
        except BaseException as error:fail('acl_restore',error)
    posts=0
    for path in (root/'worker').glob('*-budget.sqlite'):
        with sqlite3.connect(path) as db:
            if db.execute("SELECT count(*) FROM sqlite_master WHERE name='shared_requests'").fetchone()[0]:
                posts+=db.execute('SELECT count(*) FROM shared_requests').fetchone()[0]
    log=(root/'model.log').read_text() if (root/'model.log').exists() else ''
    report.update(actual_posts=posts,server_access_posts=log.count('POST /v1/chat/completions '),elapsed_seconds=time.time()-started,
        gpu_compute_pids=subprocess.check_output(['nvidia-smi','-i','0','--query-compute-apps=pid','--format=csv,noheader,nounits'],text=True).strip())
    if report['gpu_compute_pids'] or report['errors']:report['status']='failed'
    write_private_json(root/'controller-summary.json',report)
    print(json.dumps({k:report[k] for k in ('status','actual_posts','server_access_posts','elapsed_seconds','errors')},indent=2),flush=True)
raise SystemExit(0 if report['status']=='passed' else 1)
