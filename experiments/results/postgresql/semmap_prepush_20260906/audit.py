"""Publish only allowed summaries and hashes from a completed private check."""
import argparse
import hashlib
import json
from pathlib import Path


def read(path):return json.loads(path.read_text())
def digest(path):
    with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def events(path):return [json.loads(line) for line in path.read_text().splitlines()]
def find_map(value):
    if isinstance(value,dict):
        if value.get('Semantic Spec')=='semloom.semantic.sem_map.generate.v1':return value
        for item in value.values():
            found=find_map(item)
            if found:return found
    if isinstance(value,list):
        for item in value:
            found=find_map(item)
            if found:return found


def audit(settings_path, previous_ledger):
    settings=read(settings_path);base=settings_path.parent;root=Path(settings['root'])
    summary=read(root/'summary.json');controller=read(base/'controller-summary.json')
    output={k:summary[k] for k in ('status','runtime_commit','required_phases','model_requests','request_limit','quality_evaluated','performance_evaluated','response_delay_ms')}
    output['controller']={k:controller[k] for k in ('status','driver_exit_code','model_requests','model_port_closed','gpu_memory_mib','official_weights_match','model_revision')}
    configuration=read(Path(settings['config']))
    verification=read(base/'model-verification.json')
    output['model']={k:verification[k] for k in ('repo_id','revision','files','versions','cached_manifest_sha256')}
    output['pg_port']=settings['pg_port'];output['gpu_devices']=settings['gpu_devices']
    output['budget_id']=settings['budget_id'];output['ledger_sha256']=digest(Path(settings['ledger']))
    output['previous_ledger']={'attempts':len(previous_ledger.read_text().splitlines())-1,'sha256':digest(previous_ledger)}
    output['zero_task_check']=read(root/'zero-task.json')
    output['token_preflight']=read(root/'token-preflight.json')
    output['phases']={}
    for name in summary['required_phases']:
        phase=read(root/name/'phase_report.json')
        records=events(root/'http-events.jsonl') if name=='warmup' else read(root/name/'http.json')
        completions=[e for e in records if e['event']=='completion']
        requests=[e for e in records if e['event']=='request']
        value={k:phase[k] for k in ('state','measurement_status','policy_status','qualification_status','diagnostic_status','safe')}
        value['checks']=read(root/name/'checks.json')
        value['attempts']=[e['attempt'] for e in requests]
        value['completion_metadata']=[{**{k:e[k] for k in ('finish_reason','prompt_tokens','output_tokens')},
            'model_matches':e['response_model_id']==configuration['model_id'],
            'output_bytes':len(e['raw_output'].encode()),'output_sha256':hashlib.sha256(e['raw_output'].encode()).hexdigest()} for e in completions]
        value['peak_provider_uds_combined']=phase['diagnostics']['peak']['peak']['provider_uds_session_fd_peak_delta_combined']
        cleanup=phase['diagnostics']['cleanup']['cleanup'];peak=phase['diagnostics']['peak']['peak']
        value['end_provider_uds_combined']=cleanup['provider_uds_session_fd_end_delta_combined']
        value['per_role']={}
        for role in ('backend','gateway'):
            value['per_role'][role]={k:v for k,v in cleanup['per_role'][role].items() if k in ('total_fd_end_delta','thread_end_delta','rss_end_delta')}
            value['per_role'][role]['rss_peak_delta']=peak['per_role'][role]['rss_peak_delta']
        value['phase_report_sha256']=digest(root/name/'phase_report.json')
        value['http_evidence_sha256']=digest(root/name/'http.json')
        output['phases'][name]=value
    node=find_map(read(root/'insert/plan.json'))
    output['insert_plan']={k:node[k] for k in ('Model Calls','Accepted Rows','Emitted Rows','Prompt Tokens','Output Tokens')}
    readback=read(root/'insert/readback.json')
    output['insert_independent_audit_connection']=readback['audit_backend_pid']!=readback['measured_backend_pid']
    pids=set()
    for item in events(root/'sessions.jsonl'):
        for key in ('gateway_pid','peer_pid'):
            if type(item.get(key)) is int and item[key]>0:pids.add(item[key])
    output['owned_processes']={'observed':len(pids),'still_present':sum(Path(f'/proc/{pid}').exists() for pid in pids),'pg_pidfile_absent':not (root/'data/postmaster.pid').exists()}
    checked=mismatches=0
    for manifest in root.rglob('SHA256SUMS.json'):
        for relative,expected in read(manifest).items():
            checked+=1;mismatches+=digest(manifest.parent/relative)!=expected
    output['raw_hash_check']={'entries':checked,'mismatches':mismatches,'manifest_sha256':digest(root/'SHA256SUMS.json')}
    output['driver_files_sha256']={name:digest(base/name) for name in ('real_check.py','launch.py','test_controller.py')}
    output['preparation_failure']={k:read(base.parent/'controller-summary.json')[k] for k in ('status','error_type','error_code','model_requests','model_port_closed','gpu_memory_mib')}
    output['preparation_failure_files_sha256']={name:digest(base.parent/name) for name in ('launch.py','real_check.py','controller-summary.json','vllm.log')}
    return output


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--settings',type=Path,required=True)
    p.add_argument('--previous-ledger',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();value=audit(a.settings,a.previous_ledger)
    with a.output.open('x') as f:json.dump(value,f,indent=2,sort_keys=True);f.write('\n')
    print(json.dumps(value,indent=2,sort_keys=True))
