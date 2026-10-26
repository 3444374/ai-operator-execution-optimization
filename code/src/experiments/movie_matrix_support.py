"""Shared launch configuration and existing Matrix finalization, no scheduler.

Launch fields derive from the r3 CPU candidate. Physical finalization is the
existing unit_final algorithm, with packet/host dependencies supplied explicitly.
"""
from pathlib import Path
import sys
import json
from copy import deepcopy
import math
from src.experiments.sembench_records_formal import sha
from src.baselines.common.private_artifacts import content_digest
from src.observability.records_telemetry import RecordsTelemetry

def _diagnostic_warning(phase,error):
    try:print(json.dumps(dict(event='diagnostic_warning',phase=phase,error_type=type(error).__name__)),file=sys.stderr,flush=True)
    except Exception:pass


def matrix_budget(matrix):
    """Use the sealed matrix's existing limits, not an experiment's old size."""
    budget=matrix['budget']
    for name in ('max_new_calls','query_count','max_total_wall_s',
                 'launch_deadline_s','cleanup_reserve_s'):
        if type(budget[name]) is not int or budget[name] <= 0:
            raise ValueError('matrix needs a positive declared '+name)
    if (budget['launch_deadline_s']+budget['cleanup_reserve_s'] != budget['max_total_wall_s']
            or budget['automatic_retries'] != 0 or budget['model_repair_reserve'] != 0):
        raise ValueError('explicit launch/cleanup bounds and zero automatic retries required')
    return budget


def audit_tokenizer(engine):
    """Load only the declared local tokenizer and effective chat template."""
    from transformers import AutoTokenizer
    tokenizer=AutoTokenizer.from_pretrained(engine.get('tokenizer_path') or engine['model_path'],
        local_files_only=True,trust_remote_code=False)
    template=engine.get('chat_template')
    if template is not None:
        try:
            if Path(template).is_file():template=Path(template).read_text()
        except OSError:
            pass  # Inline Jinja may exceed the filesystem name limit.
        tokenizer.chat_template=template
    if type(tokenizer.chat_template) is not str or not tokenizer.chat_template:
        raise ValueError('one explicit text chat template required for token audit')
    return tokenizer


def scan_jobs(group, index):
    """Enumerate only predeclared scan identities; the public runtime runs them."""
    spec=group.get('calibration')
    if spec is None:return []
    required={'cache_dir','candidates','repetitions','near_peak_ratio','timeout_s'}
    if type(spec) is not dict or set(spec) not in (required,required|{'search'}) or group['backend']!='semloom-core':
        raise ValueError('Core calibration requires an explicit finite policy')
    if (type(spec['repetitions']) is not int or spec['repetitions']<1
            or not spec['candidates'] or not Path(spec['cache_dir']).is_absolute()
            or type(spec['near_peak_ratio']) not in (int,float)
            or not 0<spec['near_peak_ratio']<=1
            or type(spec['timeout_s']) not in (int,float)
            or not math.isfinite(spec['timeout_s']) or spec['timeout_s']<=0):
        raise ValueError('finite Core calibration candidates, repeats, time and cache path required')
    keys=[]
    candidates=_calibration_candidates(spec)
    for candidate in candidates:
        if (type(candidate) is not dict or set(candidate)!={'max_inflight','cpu_workers'}
                or any(type(v) is not int or v<1 for v in candidate.values())):
            raise ValueError('Core candidates require positive max_inflight/cpu_workers')
        keys.append((candidate['max_inflight'],candidate['cpu_workers']))
    if len(set(keys))!=len(keys):raise ValueError('duplicate Core candidate')
    return [dict(configuration=deepcopy(candidate),repeat_index=repeat,candidate_index=position,
                 sample_id=f'scan-g{index}-r{repeat}-c{position}')
            for repeat in range(1,spec['repetitions']+1)
            for position,candidate in enumerate(candidates)]


def _calibration_candidates(spec):
    """Use public expansion only to predeclare finite budget/identity slots."""
    if 'search' not in spec:return spec['candidates']
    from src.scheduling.runtime.calibration import IncreasingSearch,search_candidates
    if len(spec['candidates'])!=1 or spec['repetitions']<3:
        raise ValueError('increasing Core scan requires one seed and at least three repeats')
    return search_candidates(spec['candidates'][0],IncreasingSearch(**spec['search']))


def group_slots(groups):
    slots=[];current=0
    for index,group in enumerate(groups):
        slots.append(current);current+=1+len(scan_jobs(group,index))
    return slots


def planned_queries(plan, group, index):
    queries=[]
    for job in scan_jobs(group,index):
        queries.extend(sample_queries(plan,job['sample_id']))
    return queries+plan['queries']


def sample_queries(plan,sample_id):
    # CellBudget unit IDs keep their existing safe alphabet; generic Core IDs
    # need not adopt an experiment ledger's naming restriction.
    return [dict(q,query_id=sample_id+'--q'+str(position)) for position,q in enumerate(plan['queries'])]


def validate_core_scan(plan, group, index, cpu_limit):
    """Validate declared capacities without pretending to observe hardware."""
    jobs=scan_jobs(group,index)
    if not jobs:return
    from src.execution_provider.adapters.lotus_records_core import resolve_core_profile
    from src.serving.backends.vllm_engine import EngineSpec
    from src.semantic_methods.lotus.records import RecordsResources
    if plan['backend']!='semloom-core' or plan['profile'].get('mode')!='async':
        raise ValueError('measured Core selection currently supports async only; fixed sync remains available')
    resources=RecordsResources(**plan['resources']);engine=EngineSpec.from_dict(plan['engine'])
    for candidate in _calibration_candidates(group['calibration']):
        resolve_core_profile(engine,resources,dict(plan['profile'],max_inflight=candidate['max_inflight']))
        if candidate['cpu_workers']+resources.gpu_workers+1>cpu_limit:
            raise ValueError('Core candidate exceeds declared prepare/model/driver CPU budget')


def core_observations(cfg, packet, gpu):
    """Reuse fresh host/model preflight, never infer a measured performance value."""
    from src.infrastructure.environment import observe_machine
    machine=observe_machine()
    shared=packet.read(packet.SOURCE/'deploy/autodl/sembench_records/comparison.json')['shared_resources']
    receipt=packet.read(packet.HERE/'runtime/model-preflight.json')
    if (receipt['status']!='passed' or receipt['source_commit']!=packet.COMMIT
            or len(machine.gpu_driver_versions)!=1):
        raise ValueError('fresh model/source and selected GPU driver observations required')
    return dict(hardware=dict(machine_id=machine.machine_id,gpu_ids=[gpu['uuid']],
            gpu_driver=machine.gpu_driver_versions[0],cpu_limit=min(machine.cpu_slots,len(shared['cpu_affinity_slots'])),
            resource_limits=shared,gpu_name=gpu['name'],gpu_memory_mib=gpu['total_mib']),
        model=dict(model_revision=content_digest(dict(revision=receipt['model_revision'],
            files={n:v['sha256'] for n,v in receipt['model_files'].items()})),
            tokenizer_revision=content_digest(receipt['tokenizer_artifacts_sha256']),
            template_sha256=receipt['template_sha256']),
        software=dict(source_revision=packet.COMMIT,versions=dict(driver=cfg['driver_versions'],model=receipt['model_versions'])))


def materialize_core_plan(base, group, index, binding, *, source_revision, checked=None):
    """Rebuild only declared candidate parameters; cache/proof validation is shared."""
    from src.experiments.calibration.core import build_core_identity,core_candidate_plan
    from src.scheduling.runtime.calibration import resolve_calibration
    if checked is None:
        from src.experiments.movie_records_session import check_plan
        checked=check_plan(base)
    observed=binding['observed']
    if observed['software']['source_revision']!=source_revision:
        raise ValueError('Core calibration source differs')
    identity=build_core_identity(base,checked,**observed)
    if binding['kind']=='scan':
        jobs=scan_jobs(group,index)
        job=next((j for j in jobs if j['sample_id']==binding['sample_id']),None)
        if job is None or job['configuration']!=binding['configuration']:
            raise ValueError('scan job was not predeclared')
        sample_id=job['sample_id']
    elif binding['kind']=='selected':
        proof=resolve_calibration(Path(binding['selection_path']),identity,policy=binding['policy'])
        if (proof['status']!='ready' or proof['selection_sha256']!=binding['selection_sha256']
                or proof['candidate']!=binding['configuration']):
            raise ValueError('Core measured selection proof differs')
        sample_id=None
    else:raise ValueError('unsupported Core plan binding')
    result=core_candidate_plan(base,checked,binding['configuration'],identity=identity)
    if sample_id is not None:result['queries']=sample_queries(result,sample_id)
    return result


def record_core_selection(report, context):
    if 'core_selection' not in context:return
    resolution=context['core_selection']
    actual=report['core_execution']
    if (resolution['status']!='ready' or resolution['performance_tuned'] is not True
            or actual['mode']!='async' or actual['max_inflight']!=resolution['candidate']['max_inflight']):
        raise ValueError('actual Core configuration differs from measured selection')
    report['core_execution']=dict(actual,performance_tuned=True,calibration_scope=resolution['scope'],
        selection_sha256=resolution['selection_sha256'])
    report['core_calibration']=resolution


def bootstrap_context(context, output):
    """Validate every sitecustomize field before spawning any child/model."""
    import re
    if (not isinstance(context.get('ipc_prefix'),str)
            or not re.fullmatch(r'/s[0-9a-f]{8}[0-9]',context['ipc_prefix'])
            or context.get('ipc_receipts')!=str(Path(output)/'ipc')
            or not (Path(output)/'ipc').is_dir()
            or (Path(output)/'ipc').stat().st_mode & 0o777 != 0o700):
        raise ValueError('complete private IPC bootstrap configuration required')


def owner_context_contract(context):
    """Native additionally requires the exact normal lease_path before startup."""
    from src.infrastructure.runner_lease import HOST_SCOPE_DIR_NAME,LEASE_NAME
    value=context.get('owner_context')
    if type(value) is not dict or any(key not in value for key in ('lease_path','lease_sha256','artifact_root','gpu_idle_max_mib','owner_resource_limits')):
        raise ValueError('complete shared and Native owner context required')
    expected=Path(value['artifact_root'])/HOST_SCOPE_DIR_NAME/LEASE_NAME
    path=Path(value['lease_path'])
    if (not path.is_absolute() or path!=expected or path.is_symlink() or not path.is_file()
            or sha(path)!=value['lease_sha256']):
        raise ValueError('exact normal lease path and digest required')


def build_launch_context(cfg,packet_sha,index,group,lease,output,unit_deadline,gpu,*,packet,budget_path,budget_id):
    """Use the same complete configuration for CPU preflight and owner startup."""
    shared=packet.read(packet.SOURCE/'deploy/autodl/sembench_records/comparison.json')['shared_resources']
    prefix='/s'+format(int(cfg['ipc_prefix_base'][2:],16)+index,'08x')+'0'
    return dict(packet_sha256=packet_sha,plan_sha256=packet.sha(packet.HERE/group['plan']),
        assigned_gpu_uuid=gpu['uuid'],worker_deadline_monotonic=unit_deadline-60,
        ipc_prefix=prefix,ipc_receipts=str(output/'ipc'),
        budget=dict(path=str(budget_path),id=budget_id,limit=cfg['budget_limit']),
        owner_context=dict(lease_path=str(lease.path),lease_sha256=sha(lease.path),
            artifact_root=str(Path(cfg['lease_scope']).parent),gpu_idle_max_mib=cfg['gpu_idle_max_mib'],
            owner_resource_limits=dict(cpu_cores=len(shared['cpu_affinity_slots']),
                rss_bytes=shared['max_sampled_owned_rss_bytes'],shm_bytes=shared['max_owned_shm_bytes'])),
        runtime_binding=dict(packet.runtime_binding(cfg),cuda_visible_devices=gpu['uuid']),
        sembench_checkout=str(packet.HERE/'sembench'))


def validate_launch_context(context,output,backend):
    """Check fields consumed by this worker before claims or model startup."""
    bootstrap_context(context,output);owner_context_contract(context)
    required=[
        ('context',context,('packet_sha256','plan_sha256','assigned_gpu_uuid',
            'worker_deadline_monotonic','budget','runtime_binding','sembench_checkout')),
        ('budget',context.get('budget',{}),('path','id','limit')),
        ('runtime_binding',context.get('runtime_binding',{}),
            ('driver_python','model_python','model_path','cuda_visible_devices'))]
    if backend=='imlane':
        required.extend([
            ('runtime_binding',context.get('runtime_binding',{}),('imlane_artifact_root',
                'imlane_author_root','imlane_method_python','imlane_bridge_binary','runtime_preflight_report')),
            ('owner_resource_limits',context['owner_context']['owner_resource_limits'],
                ('cpu_cores','rss_bytes','shm_bytes'))])
    for section,value,keys in required:
        if type(value) is not dict:raise ValueError('launch configuration needs '+section+' dictionary')
        missing=[key for key in keys if key not in value]
        if missing:raise ValueError('missing launch configuration '+section+': '+', '.join(missing))


def unit_final(cfg, output, supervised, monitor, *, require_supervisor=True,host,packet):
    resources = monitor.finish()
    report = packet.read_report(output / 'query.json') if (output / 'query.json').is_file() else {}
    owned = host.identities(supervised) | host.identities(report) | monitor.owned
    a = host.api()
    cleanup = None
    live = host.helper.live_owned(a, owned)
    if live:
        cleanup = host.helper.cleanup_owned(a, owned, 15)
        owned.update(host.identities(cleanup))
    resources['registered_ipc_cleanup'] = host.cleanup_ipc(cfg, output, owned)
    packet.save(output / 'independent-finalization-observation.json', dict(
        schema='semloom.q3_unit_final_observation.v1', status='observation_only_not_qualification',
        resources=resources,
        observed_owned=[dict(pid=p, create_time=b) for p, b in sorted(owned)],
        extra_cleanup=cleanup))
    after = host.idle(cfg, 'unit-full-final-inventory',
        evidence_path=output / 'independent-final-inventory.json')
    absent = not host.helper.live_owned(a, owned)
    physical = dict(resources=resources, independent_after=after,
        observed_owned=[dict(pid=p, create_time=b) for p, b in sorted(owned)],
        all_observed_identities_absent=absent, extra_cleanup=cleanup,
        independent_physical_exit=absent and resources['passed'],
        original_supervisor_passed=(supervised is not None and supervised.get('status') == 'passed'
            and supervised.get('observed_owned_exit_confirmed') is True and cleanup is None),
        observed_only_native_monitor_is_not_full_inventory=True)
    packet.save(output / 'independent-physical.json', physical)
    if not physical['independent_physical_exit']:
        raise RuntimeError('physical/resource cleanup remains unresolved')
    if require_supervisor and not physical['original_supervisor_passed']:
        raise RuntimeError('original supervisor failed/unknown or extra owned cleanup was needed')
    return physical


class OptionalPerformance:
    def __init__(self,cfg,output,gpu,event_paths,*,host,packet):
        self.output=output;self.packet=packet
        self.sampler=host.OptionalSampler(host.monitoring_flags(cfg)['resource_sampling'],
            lambda:RecordsTelemetry(output/'performance',gpu_ids=(gpu,),event_paths=event_paths,interval_s=.25).start())
    def close(self):
        self.sampler.invoke('close')
        try:self.packet.save(self.output/'telemetry-state.json',self.sampler.state())
        except Exception:pass


def optional_performance(cfg,output,gpu,event_paths,*,host,packet):
    return OptionalPerformance(cfg,output,gpu,event_paths,host=host,packet=packet)
