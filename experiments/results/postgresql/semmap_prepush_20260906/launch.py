"""Own one local model server and run the bounded check, always cleaning up."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import pwd
import signal
import socket
import subprocess
import sys
import time
import urllib.request


def read_json(path):return json.loads(path.read_text())
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def start_ticks(pid):
    s=Path(f'/proc/{pid}/stat').read_text()
    return int(s[s.rfind(')')+2:].split()[19])


def validate_endpoint(settings, configuration):
    assert configuration['endpoint_url']==f"http://127.0.0.1:{settings['model_port']}/v1/chat/completions", 'model_endpoint_binding'
    assert configuration['timeout_ms']==120000, 'model_timeout'


def group_alive(pgid):
    # Exclude zombies: they hold no running workload or open file descriptors.
    rows=subprocess.check_output(['ps','-e','-o','pgid=,stat='],text=True)
    return any(int(parts[0])==pgid and not parts[1].startswith('Z')
               for line in rows.splitlines() if len(parts:=line.split())==2)


def stop_group(process, terminate_seconds):
    if process is None:return False
    pgid=process.pid
    if group_alive(pgid):
        try:os.killpg(pgid,signal.SIGTERM)
        except ProcessLookupError:pass
    deadline=time.monotonic()+terminate_seconds
    while group_alive(pgid) and time.monotonic()<deadline:
        process.poll()
        time.sleep(.02)
    forced=group_alive(pgid)
    if forced:
        try:os.killpg(pgid,signal.SIGKILL)
        except ProcessLookupError:pass
    process.wait(timeout=10)
    deadline=time.monotonic()+10
    while group_alive(pgid) and time.monotonic()<deadline:time.sleep(.02)
    if group_alive(pgid):raise RuntimeError('owned_process_group_survived')
    return forced


def run(path):
    settings=read_json(path)
    root=path.parent
    assert read_json(root/'preflight.json')['status']=='ok','environment_preflight'
    verified=read_json(root/'model-verification.json')
    upstream=read_json(Path(settings['upstream_identity']))
    assert all(verified['files'][name]==digest for name,digest in upstream['weights'].items()),'official_weight_identity'
    assert verified['revision']==settings['model_revision']==upstream['commit'],'revision_identity'
    configuration=read_json(Path(settings['config']))
    validate_endpoint(settings,configuration)
    argv=settings['service_args']
    expected={'--model':settings['model_root'],'--tokenizer':settings['model_root'],
        '--served-model-name':configuration['model_id'],'--dtype':'bfloat16','--max-model-len':'4096',
        '--gpu-memory-utilization':'0.8','--scheduling-policy':'fcfs','--max-num-seqs':'4',
        '--max-num-batched-tokens':'4096','--tensor-parallel-size':'1','--host':'127.0.0.1',
        '--port':str(settings['model_port']),'--generation-config':'vllm'}
    actual={};index=0
    while index<len(argv):
        flag=argv[index]
        assert flag not in actual,'duplicate_service_flag'
        if flag in ('--enforce-eager','--no-enable-prefix-caching'):
            actual[flag]=True;index+=1
        else:actual[flag]=argv[index+1];index+=2
    assert actual=={**expected,'--enforce-eager':True,'--no-enable-prefix-caching':True},'service_arguments'
    with socket.socket() as check:
        check.bind(('127.0.0.1',settings['model_port']))
    sys.path.insert(0,str(Path(settings['source'])/'code'))
    from src.experiments.attempt_ledger import AttemptLedger,AttemptBudget
    budget=AttemptBudget(settings['budget_id'],settings['budget_limit'])
    ledger_path=Path(settings['ledger'])
    ledger=AttemptLedger(ledger_path,budget) if ledger_path.exists() else AttemptLedger.create(ledger_path,budget)
    assert ledger.attempts==0,'preparation_budget_not_empty'
    user=pwd.getpwnam(settings['pg_user'])
    os.chown(settings['ledger'],user.pw_uid,user.pw_gid)
    (root/'cache').mkdir()
    env=dict(os.environ,CUDA_VISIBLE_DEVICES=settings['gpu_devices'],HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',
             PATH=str(Path(settings['vllm_python']).parent)+os.pathsep+os.environ.get('PATH',''),
             TMPDIR=settings['temporary_root'],TORCHINDUCTOR_CACHE_DIR=str(root/'cache'),PYTHONDONTWRITEBYTECODE='1')
    launcher=Path(settings['source'])/'code/scripts/services/launch_vllm_with_identity.py'
    result={'status':'incomplete','source_commit':settings['source_commit'],'request_limit':budget.limit,
            'model_revision':settings['model_revision'],'official_weights_match':True}
    gpu_before=subprocess.check_output(['nvidia-smi','--query-gpu=index,memory.used','--format=csv,noheader,nounits'],text=True)
    gpu_values={line.split(',')[0].strip():int(line.split(',')[1]) for line in gpu_before.splitlines()}
    assert settings['gpu_devices'].isdigit() and gpu_values[settings['gpu_devices']]<=100,'selected_gpu_not_idle'
    save(root/'gpu-before.json',gpu_values)
    process=None
    driver=None
    try:
        with (root/'vllm.log').open('x') as log:
            process=subprocess.Popen([settings['vllm_python'],str(launcher),'--identity-output',str(root/'vllm-identity.json'),
                    '--port',str(settings['model_port']),'--',*argv],env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            ticks=start_ticks(process.pid)
            save(root/'controller-progress.json',{'state':'model_starting','pid':process.pid,'start_time_ticks':ticks})
            endpoint=f"http://127.0.0.1:{settings['model_port']}"
            deadline=time.monotonic()+240
            while time.monotonic()<deadline and process.poll() is None:
                try:
                    with urllib.request.urlopen(endpoint+'/v1/models',timeout=2) as response:model_api=json.load(response)
                    if model_api.get('data'):break
                except (OSError,ValueError):pass
                time.sleep(1)
            else:raise RuntimeError('model_startup_failed')
            assert len(model_api['data'])==1 and model_api['data'][0]['id']==configuration['model_id'],'model_api_identity'
            identity={'verified':True,'pid':process.pid,'start_time_ticks':ticks,
                'cmdline_sha256':hashlib.sha256(Path(f'/proc/{process.pid}/cmdline').read_bytes()).hexdigest(),
                'model_revision':settings['model_revision'],'config_sha256':hashlib.sha256(Path(settings['config']).read_bytes()).hexdigest(),
                'official_weights_match':True,'vllm_version':verified['versions']['vllm'],
                'endpoint_url':configuration['endpoint_url'],'gpu_devices':settings['gpu_devices']}
            settings['service_verification']=str(root/'service-verified.json')
            save(root/'service-verified.json',identity)
            save(path,settings)
            save(root/'controller-progress.json',{'state':'checking','pid':process.pid,'start_time_ticks':ticks})
            command=['runuser','-u',settings['pg_user'],'--','env',f"PYTHONPATH={settings['source']}/code",'PYTHONDONTWRITEBYTECODE=1',
                sys.executable,str(root/'real_check.py'),'--settings',str(path)]
            with (root/'driver.log').open('x') as output:
                driver=subprocess.Popen(command,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
                driver.wait(timeout=600)
            result['driver_exit_code']=driver.returncode
            result['run_status']=read_json(Path(settings['root'])/'summary.json')['status']
            assert driver.returncode==0 and result['run_status']=='passed','real_check_failed'
            result['status']='passed'
    except BaseException as error:
        result['error_type']=type(error).__name__
        result['error_code']=str(error) if str(error).replace('_','').isalnum() else 'controller_failed'
        raise
    finally:
        cleanup_errors={}
        try:
            if stop_group(driver,90):result['forced_driver_kill']=True
        except Exception as error:cleanup_errors['driver']=type(error).__name__
        pgdata=Path(settings['root'])/'data'
        try:
            if (pgdata/'postmaster.pid').exists():
                with (root/'emergency-pg-stop.log').open('x') as output:
                    stopped=subprocess.run(['runuser','-u',settings['pg_user'],'--',str(Path(settings['prefix'])/'bin/pg_ctl'),
                        '-D',str(pgdata),'-m','fast','-w','stop'],stdout=output,stderr=subprocess.STDOUT,timeout=60)
                result['emergency_pg_cleanup']=stopped.returncode
        except Exception as error:cleanup_errors['postgres']=type(error).__name__
        try:
            if process is not None:
                if process.poll() is None:assert start_ticks(process.pid)==ticks,'cleanup_identity_changed'
                if stop_group(process,60):result['forced_model_kill']=True
        except Exception as error:cleanup_errors['model']=type(error).__name__
        if cleanup_errors or any(key in result for key in ('forced_driver_kill','forced_model_kill','emergency_pg_cleanup')):
            result['status']='incomplete'
            result['cleanup_errors']=cleanup_errors
        try:result['model_requests']=ledger.attempts
        except Exception:
            result['model_requests']=None
            result['status']='incomplete'
        with socket.socket() as check:result['model_port_closed']=check.connect_ex(('127.0.0.1',settings['model_port']))!=0
        gpu=subprocess.check_output(['nvidia-smi','--query-gpu=index,memory.used','--format=csv,noheader,nounits'],text=True)
        result['gpu_memory_mib']={line.split(',')[0].strip():int(line.split(',')[1]) for line in gpu.splitlines()}
        if not result['model_port_closed'] or any(v>100 for v in result['gpu_memory_mib'].values()):result['status']='incomplete'
        save(root/'controller-summary.json',result)
        print(json.dumps(result,indent=2))
    return 0 if result['status']=='passed' else 1


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--settings',type=Path,required=True)
    raise SystemExit(run(parser.parse_args().settings))
