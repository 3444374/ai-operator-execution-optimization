"""Maintained owner/worker entry for a sealed Movie matrix.

The packet supplies pinned host/asset/sampling checks. It is an explicit current
input, never located by searching historical experiment directories. Scheduling,
model ownership, query results, and continuation accounting remain native.
"""
from pathlib import Path
from copy import deepcopy
import argparse
from functools import partial
from types import SimpleNamespace
import datetime
import importlib
import json
import os
import subprocess
import sys
import time
import psutil
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.movie_records_session import check_plan, run_movie_session
from src.experiments.sembench_records_formal import validate_result, QueryClock, read, sha
from src.baselines.text.movie_records import MovieRecordSource
from src.baselines.common.private_artifacts import require_outside_git,write_private_json,content_digest
from src.semantic_methods.lotus.records import CallLedger
from src.observability.records_telemetry import TelemetryEvents
from src.experiments.movie_matrix_support import (
    _diagnostic_warning, build_launch_context, validate_launch_context,
    unit_final, optional_performance, matrix_budget, audit_tokenizer,
    scan_jobs,group_slots,planned_queries,validate_core_scan,core_observations,materialize_core_plan,
)


def load_runtime(packet_dir, packet_sha):
    """Load the already sealed, self-contained current packet's runtime adapters."""
    packet_dir = Path(packet_dir).resolve()
    manifest = packet_dir / 'packet.json'
    if sys.flags.optimize or sha(manifest) != packet_sha:
        raise ValueError('exact packet identity and non-optimized Python required')
    files = read(manifest)['files']
    required = ('q3_packet.py', 'q3_host.py', 'host_helpers.py', 'execution_policy.py',
                'active_failure_evidence.py', 'token_audit.py')
    if not set(required).issubset(files):
        raise ValueError('self-contained current packet runtime adapters required')
    for name, digest in files.items():
        path = packet_dir / name
        if Path(name).is_absolute() or '..' in Path(name).parts or path.is_symlink() or sha(path) != digest:
            raise ValueError('packet runtime file differs: ' + name)
    for name in ('q3_packet', 'q3_host', 'active_failure_evidence', 'token_audit'):
        prior = sys.modules.get(name)
        if prior is not None and Path(prior.__file__).resolve() != packet_dir / (name + '.py'):
            raise ValueError('one explicit packet per worker process')
    sys.path.insert(0, str(packet_dir))
    return SimpleNamespace(binding=importlib.import_module('q3_packet'),
        host=importlib.import_module('q3_host'),
        evidence=importlib.import_module('active_failure_evidence'),
        audit=importlib.import_module('token_audit'))


class MovieMatrix:
    """One explicit matrix/owner process; no alternate provider or scheduler."""

    def __init__(self, runtime, output, entrypoint):
        self.packet, self.host = runtime.binding, runtime.host
        self.evidence, self.audit = runtime.evidence, runtime.audit
        self.root, self.entrypoint = Path(output), Path(entrypoint)
        cfg = self.packet.read(self.packet.HERE / 'base-config.json')
        continuation = self.packet.HERE / 'continuation.json'
        self.budget_id = cfg['budget_id']
        self.budget_path = (Path(self.packet.read(continuation)['budget_path'])
                            if continuation.exists() else self.root / 'budget.sqlite')
        self.build_context = partial(build_launch_context, packet=self.packet,
                                    budget_path=self.budget_path, budget_id=self.budget_id)
        self.validate_context = validate_launch_context
        self.finalizers = SimpleNamespace(
            unit_final=partial(unit_final, host=self.host, packet=self.packet),
            optional_performance=partial(optional_performance, host=self.host, packet=self.packet))

    def worker_argv(self, cfg, packet_sha, index, context, output):
        return [cfg['driver_python'], '-B', str(self.entrypoint), 'worker',
            '--packet', str(self.packet.HERE), '--packet-sha', packet_sha,
            '--output-dir', str(self.root), '--index', str(index),
            '--context', str(context), '--worker-output', str(output)]

    def audit_argv(self, cfg, report, output):
        return [cfg['engine']['model_python'], '-B', str(self.entrypoint), 'audit',
            '--packet', str(self.packet.HERE), '--packet-sha', self.packet_digest,
            '--output-dir', str(self.root), '--report', str(report), '--audit-output', str(output)]

    def check(self, packet_sha, *, offline=False):
        cfg, matrix, groups = self.bound(packet_sha, host_checks=not offline)
        cpu_limit=len(self.packet.read(self.packet.SOURCE/'deploy/autodl/sembench_records/comparison.json')['shared_resources']['cpu_affinity_slots']) if any(g.get('calibration') for g in groups) else 0
        for index,group in enumerate(groups):
            plan = check_plan(self.packet.read(self.packet.HERE / group['plan']))
            if plan['expected_calls'] != group['max_calls']:
                raise ValueError('owner plan calls differ from matrix')
            validate_core_scan(self.packet.read(self.packet.HERE/group['plan']),group,index,cpu_limit)
        return dict(status='checked_offline_not_runtime_qualified' if offline else 'checked_not_executed', owner_groups=len(groups),
            scan_owner_groups=sum(len(scan_jobs(g,i)) for i,g in enumerate(groups)),
            queries=matrix['budget']['query_count'],
            max_calls=matrix['budget']['max_new_calls'], actual_calls=0, claims=0,
            host_checks_performed=not offline, runtime_qualification=False)

    def bound(self, packet_sha, *, check_schedule=True, host_checks=True):
        self.packet_digest = packet_sha
        source = self.packet.SOURCE.resolve()
        if (Path(__file__).resolve() != source / 'code/src/experiments/movie_records_matrix.py'
                or self.entrypoint.resolve() != source / 'code/scripts/experiments/run_movie_records_matrix.py'):
            raise ValueError('maintained entry and driver must belong to the bound source')
        cfg = self.packet.verify(packet_sha)
        matrix = self.packet.read(self.packet.HERE / 'matrix.json')
        groups = self.packet.read(self.packet.HERE / 'owner-groups.json')
        assert type(matrix['execution_enabled']) is bool and matrix['source_binding']['final_source'] == self.packet.COMMIT
        if host_checks:
            assert matrix['execution_enabled'] is True
            self.host.protected(cfg)
            self.host.verify_assets(cfg)
        assert matrix['source_binding']['final_lock'] == self.packet.LOCK
        budget = matrix_budget(matrix)
        assert cfg['budget_limit'] == budget['max_new_calls'] and cfg['wall_s'] == budget['max_total_wall_s']
        if check_schedule:
            queries = [q for i,g in enumerate(groups) for q in planned_queries(self.packet.read(self.packet.HERE/g['plan']),g,i)]
            assert len(queries) == len({q['query_id'] for q in queries}) == budget['query_count']
            assert sum(g['max_calls']*(1+len(scan_jobs(g,i))) for i,g in enumerate(groups)) == budget['max_new_calls']
        for task, entry in self.packet.read(self.packet.HERE / 'input-bindings.json').items():
            assert self.packet.sha(entry['manifest']) == entry['manifest_sha256']
            assert self.packet.sha(entry['token_reference']) == entry['token_reference_sha256']
            assert self.packet.sha(entry['messages']) == entry['messages_sha256']
            source = MovieRecordSource.from_manifest(entry['manifest'])
            assert source.task.name == task and len(source.expected('cpu-check')) == entry['calls']
        return (cfg, matrix, groups)

    def worker(self, packet_sha, index, context_path, output):
        cfg, matrix, groups = self.bound(packet_sha, check_schedule=False)
        group = groups[index]
        plan = self.packet.read(self.packet.HERE / group['plan'])
        context = self.packet.read(context_path)
        if 'plan_binding' in context:
            binding=context['plan_binding'];base_checked=check_plan(plan)
            effective=materialize_core_plan(plan,group,index,binding,source_revision=self.packet.COMMIT,checked=base_checked)
            expected_slot=group_slots(groups)[index]
            if binding['kind']=='scan':
                expected_slot+=1+next(i for i,j in enumerate(scan_jobs(group,index)) if j['sample_id']==binding['sample_id'])
            if (context['slot_index']!=expected_slot or binding['declared_plan_sha256']!=self.packet.sha(self.packet.HERE/group['plan'])
                    or content_digest(base_checked)!=content_digest(binding['checked'])
                    or context['execution_plan_path']!=str(output/'execution-plan.json')
                    or content_digest(effective)!=content_digest(self.packet.read(context['execution_plan_path']))
                    or context['plan_sha256']!=self.packet.sha(context['execution_plan_path'])):
                raise ValueError('bound Core execution plan drift before claim/owner')
            plan=effective
        checked = check_plan(plan)
        self.validate_context(context, output, plan['backend'])
        from integrations.imlane_records.chain_runtime import check_external_lease
        check_external_lease(context['owner_context'])
        expected_sha=self.packet.sha(context['execution_plan_path']) if 'plan_binding' in context else self.packet.sha(self.packet.HERE/group['plan'])
        assert context['packet_sha256'] == packet_sha and context['plan_sha256'] == expected_sha
        assert self.host.assigned_gpu(cfg)['uuid'] == context['assigned_gpu_uuid']
        ledger = CellBudgetLedger(self.budget_path, AttemptBudget(self.budget_id, matrix_budget(matrix)['max_new_calls']))
        context['claims'] = {q['query_id']: ledger.claim_unit(q['query_id']) for q in checked['queries']}
        context['worker_entry_ns'] = time.monotonic_ns()
        bindings = self.packet.read(self.packet.HERE / 'input-bindings.json')
        seen_owner = {}
        checkpoints = []

        def checkpoint(boundary):
            check_external_lease(context['owner_context'])
            assert time.monotonic() < context['worker_deadline_monotonic']
            query = self.packet.read_report(output / 'session' / ('query-' + str(boundary['query_index']) + '.json'))
            declaration = checked['queries'][boundary['query_index']]
            source = MovieRecordSource.from_manifest(declaration['manifest'])
            raw = query['backend_report']
            validate_result(raw, source.expected(query['query_id']), source=source)
            reference = self.packet.read(bindings[query['task']]['token_reference'])['rows']
            rows = raw['calls']['records']
            for row in rows:
                assert row['status'] == 'succeeded' and row['submission_confirmed'] is True
                assert row['messages_sha256'] == reference[row['row_id']]['messages_sha256']
                assert self.audit.normalize_ids(row, plan['backend'])[0] == reference[row['row_id']]['expected_prompt_token_ids']
            if plan['lifecycle'] == 'resident':
                ready = self.packet.read(output / 'session/owner-ready-0.json')['ready']
                uids = {r['engine_uid'] for r in rows}
                assert len(uids) == 1 and None not in uids
                current = dict(engine_uids=sorted(uids))
                if plan['backend'] == 'imlane':
                    current.update(worker_uid=ready['worker_uid'], worker_pid=ready['worker_pid'], worker_birth=psutil.Process(ready['worker_pid']).create_time(), model_pid=ready['engine_ready']['pid'], model_birth=psutil.Process(ready['engine_ready']['pid']).create_time())
                    assert current['model_birth'] == ready['model_process_created']
                    assert ready['model_initialization_attempts'] == ready['model_ready_count'] == 1
                if seen_owner:
                    assert current == seen_owner
                else:
                    seen_owner.update(current)
            elif plan['lifecycle'] == 'runtime_reuse_only':
                ready = self.packet.read(output / 'session/owner-ready-0.json')['ready']
                assert ready['no_session_model'] is True
                current = dict(runtime_identity=ready['runtime_identity'])
                if seen_owner:
                    assert current == seen_owner
                else:
                    seen_owner.update(current)
            result = dict(**boundary, status=('passed_complete_input_actual_prompt_and_same_runtime'
                if plan['lifecycle'] == 'runtime_reuse_only' else 'passed_complete_input_actual_prompt_and_same_owner'),
                calls=len(rows), owner=seen_owner)
            checkpoints.append(result)
            self.packet.save(output / ('checkpoint-' + str(boundary['query_index']) + '.json'), result)
            return result
        primary = None
        with TelemetryEvents(output / 'session-events.jsonl') as events:
            clock = QueryClock(event_sink=events)
            try:
                with self.evidence.durable_terminals(CallLedger, output, self.packet.save, query_ids=[q['query_id'] for q in checked['queries']]):
                    report = run_movie_session(plan, context, output / 'session', observer=clock.record, checkpoint=checkpoint)
                    self.packet.save(output / 'query.json', report)
                assert report['status'] == 'completed' and len(checkpoints) == len(checked['queries'])
                assert all((o['status'] == 'closed' for o in report['owners']))
                assert len(report['owners']) == (len(checked['queries']) if plan['lifecycle'] == 'isolated' else 1)
                records = [r for q in report['queries'] for r in q['backend_report']['calls']['records']]
                assert len(records) == len({r['call_id'] for r in records}) == group['max_calls']
                self.packet.save(output / 'functional-validation.json', dict(status='passed', actual_calls=len(records), checkpoints=checkpoints, model_load_count=None, model_load_count_reason='not directly instrumented'))
            except BaseException as error:
                primary = error
                if not (output / 'query.json').exists():
                    self.packet.save(output / 'query.json', getattr(error, 'session_report', dict(status='failed', error=self.host.helper.error_record(error))))
            finally:
                try:
                    self.packet.save(output / 'clock.json', clock.events)
                except Exception as diagnostic:
                    _diagnostic_warning('query_clock', diagnostic)
        if primary is not None:
            raise primary

    def token_audit(self, report_path, output):
        """One CPU tokenizer initialization per completed owner group."""
        assert os.environ.get('CUDA_VISIBLE_DEVICES') == ''
        import torch
        cfg = self.packet.read(self.packet.HERE / 'base-config.json')
        report = self.packet.read_report(report_path)
        query_ids = [q['query_id'] for q in report['queries']]
        plans = [self.packet.read(self.packet.HERE / g['plan'])
                 for g in self.packet.read(self.packet.HERE / 'owner-groups.json')]
        context_path=report_path.parent/'context.json'
        if context_path.exists():
            context=self.packet.read(context_path)
            if 'plan_binding' in context:
                group=self.packet.read(self.packet.HERE/'owner-groups.json')[context['plan_index']]
                base=self.packet.read(self.packet.HERE/group['plan']);binding=context['plan_binding']
                effective=materialize_core_plan(base,group,context['plan_index'],binding,
                    source_revision=self.packet.COMMIT,checked=binding['checked'])
                if (binding['declared_plan_sha256']!=self.packet.sha(self.packet.HERE/group['plan'])
                        or context['execution_plan_path']!=str(report_path.parent/'execution-plan.json')
                        or self.packet.sha(context['execution_plan_path'])!=context['plan_sha256']
                        or content_digest(effective)!=content_digest(self.packet.read(context['execution_plan_path']))):
                    raise ValueError('token audit execution-plan binding differs')
                plans=[effective]
        matches = [p for p in plans if p['backend'] == report['backend']
                   and [q['query_id'] for q in p['queries']] == query_ids]
        if len(matches) != 1:
            raise ValueError('token audit needs the exact declared owner/query sequence')
        plan = matches[0]
        generation = plan['generation']
        if generation not in cfg['finite_generation_profiles']:
            raise ValueError('owner generation is outside the sealed profiles')
        tokenizer = audit_tokenizer(plan['engine'])
        import hashlib
        assert hashlib.sha256(tokenizer.chat_template.encode()).hexdigest() == cfg['expected_template_sha256']
        bindings = self.packet.read(self.packet.HERE / 'input-bindings.json')
        rows = []
        for query in report['queries']:
            if query['backend_report']['generation'] != generation:
                raise ValueError('observed query generation differs from owner plan')
            entry = bindings[query['task']]
            reference = self.packet.read(entry['token_reference'])
            assert reference['messages_sha256'] == self.packet.sha(entry['messages'])
            self.audit.PER_QUERY_CALLS = entry['calls']
            audited = self.audit.audit_rows(query['backend_report'], report['backend'], reference['rows'], tokenizer, generation)
            rows.append(dict(query_id=query['query_id'], actual_calls=len(audited), rows=audited))
        assert not torch.cuda.is_initialized()
        self.packet.save(output, dict(status='passed', actual_ids_reencoded=False, queries=rows, report_sha256=self.packet.sha(report_path), real_model_calls_in_audit=0))

    def run_group(self, cfg, packet_sha, index, group, lease, deadline, emit, *, slot_index=None, plan_binding=None, work_deadline=None):
        from src.experiments.lotus_records_supervisor import supervise
        self.host.idle(cfg, 'before-owner-' + str(index))
        self.host.helper.check_lease(lease)
        gpu = self.host.assigned_gpu(cfg)
        slot=index if slot_index is None else slot_index
        self.last_native_slot=slot
        output = self.root / 'units' / ('g' + str(slot).zfill(3))
        output.mkdir(mode=448)
        (output / 'ipc').mkdir(mode=448)
        plan = self.packet.read(self.packet.HERE / group['plan'])
        if plan_binding is not None:
            plan=materialize_core_plan(plan,group,index,plan_binding,source_revision=self.packet.COMMIT,
                checked=plan_binding['checked'])
            write_private_json(output/'execution-plan.json',plan)
        budget = matrix_budget(self.packet.read(self.packet.HERE / 'matrix.json'))
        remaining = deadline - time.monotonic()
        resources = plan['resources']
        starts = len(plan['queries']) if plan['lifecycle'] == 'isolated' else 1
        owner_bound = starts * (resources['startup_timeout_s'] + 60) + len(plan['queries']) * resources['query_timeout_s']
        limit = min(owner_bound, remaining - budget['cleanup_reserve_s'])
        if work_deadline is not None:limit=min(limit,work_deadline-time.monotonic())
        if limit <= 60:
            raise TimeoutError('remaining execution/cleanup window cannot cover next owner')
        unit_deadline = time.monotonic() + limit
        context = self.build_context(cfg, packet_sha, slot, group, lease, output, unit_deadline, gpu)
        context.update(plan_index=index,slot_index=slot)
        if plan_binding is not None:
            context.update(plan_binding=plan_binding,execution_plan_path=str(output/'execution-plan.json'))
            context['plan_sha256']=self.packet.sha(output/'execution-plan.json')
            if plan_binding['kind']=='selected':
                context['core_selection']=plan_binding['resolution']
        self.validate_context(context, output, plan['backend'])
        self.packet.save(output / 'context.json', context)
        rpc = cfg['rpc_root_prefix'] + str(slot)
        ray = cfg['ray_root_prefix'] + str(slot)
        assert not Path(rpc).exists() and (not Path(ray).exists())
        Path(rpc).mkdir(mode=448)
        for name in ('tmp', 'mpl'):
            (self.root / 'runtime' / name).mkdir(parents=True, exist_ok=True, mode=0o700)
        os.environ.update(PYTHONPATH=str(self.packet.SOURCE / 'code') + os.pathsep + str(self.packet.HERE), AI_OPERATOR_ENV_FILE=cfg['environment_file'], CUDA_VISIBLE_DEVICES=gpu['uuid'], RAY_TMPDIR=ray, VLLM_RPC_BASE_PATH=rpc, TMPDIR=str(self.root / 'runtime/tmp'), MPLCONFIGDIR=str(self.root / 'runtime/mpl'), SEMLOOM_RUN_IPC_PREFIX=context['ipc_prefix'], SEMLOOM_RUN_IPC_RECEIPTS=context['ipc_receipts'], SEMLOOM_RUN_IPC_CONTEXT=str(output / 'context.json'), RAY_USAGE_STATS_ENABLED='0', DAFT_RUNNER='native', VLLM_WORKER_MULTIPROC_METHOD='spawn', PYTHONHASHSEED='0', PYTHONOPTIMIZE='0')
        monitor = self.host.FullMonitor(cfg, self.packet.read(self.packet.SOURCE / 'deploy/autodl/sembench_records/comparison.json')['shared_resources'], unit_deadline)
        performance = self.finalizers.optional_performance(cfg, output, gpu['uuid'], (output / 'session-events.jsonl',))
        error = supervised = None
        physical = None
        start = time.monotonic()
        emit(dict(event='owner_group_started', index=index, backend=group['backend'], profile=group['profile'], repetition=group['repetition'], max_calls=group['max_calls'], queries=[q['query_id'] for q in plan['queries']]))
        try:
            supervised = supervise(self.worker_argv(cfg, packet_sha, index, output / 'context.json', output), output, timeout_s=limit - 60, grace_s=15, natural_exit_grace_s=1, monitor=monitor)
        except BaseException as failure:
            error = failure
            supervised = getattr(failure, 'report', None)
        finally:
            try:
                physical = self.finalizers.unit_final(cfg, output, supervised, monitor, require_supervisor=False)
            except BaseException as cleanup:
                if error is None:
                    error = cleanup
                else:
                    error.add_note('independent cleanup: ' + type(cleanup).__name__)
            for key in ('SEMLOOM_RUN_IPC_PREFIX', 'SEMLOOM_RUN_IPC_RECEIPTS', 'SEMLOOM_RUN_IPC_CONTEXT'):
                os.environ.pop(key, None)
            try:
                performance.close()
            except Exception as diagnostic:
                _diagnostic_warning('performance_close', diagnostic)
        if error is not None:
            raise error
        assert physical['original_supervisor_passed'] and physical['independent_physical_exit']
        assert self.packet.read(output / 'functional-validation.json')['status'] == 'passed'
        audit_deadline=work_deadline if work_deadline is not None else deadline-60
        audited = subprocess.run(self.audit_argv(cfg, output / 'query.json', output / 'token-audit.json'), env=dict(os.environ, CUDA_VISIBLE_DEVICES=''), capture_output=True, text=True, timeout=min(90, max(1, audit_deadline - time.monotonic())))
        self.packet.save(output / 'token-audit-command.json', dict(returncode=audited.returncode, stdout=audited.stdout, stderr=audited.stderr))
        assert audited.returncode == 0 and self.packet.read_report(output / 'token-audit.json')['status'] == 'passed'
        report = self.packet.read_report(output / 'query.json')
        result = dict(index=index, backend=group['backend'], profile=group['profile'], repetition=group['repetition'], group=group['group'], status='completed', actual_calls=group['max_calls'], application_e2e_s=report['application_e2e_s'], outer_elapsed_s=time.monotonic() - start, physical_exit_confirmed=True, query_report_sha256=self.packet.sha(output / 'query.json'), queries=[dict(query_id=q['query_id'], task=q['task'], phase=q['phase'], full_query_s=q['full_query_seconds'], post_result_s=q['post_result_seconds'], counts=q['backend_report']['calls']['counts'], result=q['backend_report']['result']) for q in report['queries']])
        result.update(slot_index=slot,stage='scan' if plan_binding is not None and plan_binding['kind']=='scan' else 'target')
        self.packet.save(output / 'result.json', result)
        emit(dict(event='owner_group_completed', **result))
        return result

    def run_calibrated_group(self,cfg,packet_sha,index,group,lease,deadline,emit,*,slot_index):
        """Adapt workload callbacks; all scan/cache/choice sequencing is public API."""
        group_started=time.monotonic()
        from src.scheduling.runtime.calibration import ScanBudget,IncreasingSearch,run_calibrated,calibrate
        from src.experiments.calibration.core import build_core_identity,core_measurement,core_runtime_measurement
        spec=group['calibration'];base=self.packet.read(self.packet.HERE/group['plan']);checked=check_plan(base)
        require_outside_git(Path(spec['cache_dir']))
        observed=core_observations(cfg,self.packet,self.host.assigned_gpu(cfg))
        identity=build_core_identity(base,checked,**observed)
        jobs=scan_jobs(group,index)
        validate_core_scan(base,group,index,observed['hardware']['cpu_limit'])
        work=checked['expected_calls'];directory=self.root/'calibration'/('g'+str(index).zfill(3))
        directory.mkdir(parents=True,mode=0o700)
        rows=[];self.last_calibration_physical=True
        common=dict(cache_dir=Path(spec['cache_dir']),work_per_sample=work,repetitions=spec['repetitions'],
                    near_peak_ratio=spec['near_peak_ratio'])
        if 'search' in spec:common['search']=IncreasingSearch(**spec['search'])

        def binding(kind,configuration,**extra):
            return dict(kind=kind,configuration=configuration,observed=observed,checked=checked,
                declared_plan_sha256=self.packet.sha(self.packet.HERE/group['plan']),**extra)

        def native(plan_binding,slot,work_deadline=None):
            self.last_calibration_physical=False
            try:
                value=self.run_group(cfg,packet_sha,index,group,lease,deadline,emit,
                    slot_index=slot,plan_binding=plan_binding,work_deadline=work_deadline)
            except BaseException:
                path=self.root/'units'/('g'+str(slot).zfill(3))/'independent-physical.json'
                if path.exists():
                    proof=self.packet.read_report(path)
                    self.last_calibration_physical=(proof['independent_physical_exit'] and
                        not self.host.helper.live_owned(self.host.api(),self.host.identities(proof)))
                raise
            self.last_calibration_physical=True
            return value

        def measure(configuration,*,repeat_index,deadline_monotonic,work_limit):
            if work_limit!=work:raise ValueError('scan work_limit differs from complete workload')
            position=next(i for i,j in enumerate(jobs)
                if j['repeat_index']==repeat_index and j['configuration']==configuration)
            job=jobs[position];slot=slot_index+1+position
            sample_binding=binding('scan',configuration,sample_id=job['sample_id'])
            sample_plan=materialize_core_plan(base,group,index,sample_binding,source_revision=self.packet.COMMIT,checked=checked)
            output=self.root/'units'/('g'+str(slot).zfill(3));primary=None
            try:
                value=native(sample_binding,slot,deadline_monotonic)
            except Exception as error:
                primary=error
                if not self.last_calibration_physical:raise
                value=None
            report=(self.packet.read_report(output/'query.json') if (output/'query.json').exists()
                    else dict(backend='semloom-core',lifecycle=base['lifecycle'],status='failed',queries=[]))
            sample=core_measurement(sample_plan,check_plan(sample_plan),report,identity=identity,
                repeat_index=repeat_index,resources_passed=primary is None)
            self.packet.save(output/'core-scan-measurement.json',sample)
            rows.append(dict(slot_index=slot,measurement=sample,result=value,
                error=None if primary is None else self.host.helper.error_record(primary)))
            if primary is not None and (sample['unknown_model_calls'] or sample['recorded_model_calls']<work):
                raise primary
            return core_runtime_measurement(sample)

        def execute(configuration):
            resolution=calibrate(identity,spec['candidates'],measure=None,**common)
            if resolution['status']!='ready' or resolution['candidate']!=configuration:
                raise ValueError('selected runtime calibration changed before target execution')
            receipt=Path(resolution['selection_path']).read_bytes()
            import hashlib
            if hashlib.sha256(receipt).hexdigest()!=resolution['selection_sha256']:
                raise ValueError('selection changed while copying execution proof')
            proof_path=directory/'selection-receipt.json';proof_path.write_bytes(receipt)
            resolution=dict(resolution,selection_path=str(proof_path))
            selected_binding=binding('selected',configuration,selection_path=str(proof_path),
                selection_sha256=resolution['selection_sha256'],policy=json.loads(receipt)['policy'],resolution=resolution)
            return native(selected_binding,slot_index)

        frontend_preparation_s=time.monotonic()-group_started
        try:
            result=run_calibrated(identity,spec['candidates'],execute,measure=measure,
                budget=ScanBudget(timeout_s=min(spec['timeout_s'],deadline-time.monotonic()-matrix_budget(self.packet.read(self.packet.HERE/'matrix.json'))['cleanup_reserve_s']),
                                  max_work=work*len(jobs)),**common)
        except BaseException as error:
            report=getattr(error,'calibration_report',getattr(error,'report',{}))
            self.last_calibration_failure=dict(index=index,report=report,scan_units=rows)
            try:self.packet.save(directory/'failed.json',self.last_calibration_failure)
            except Exception as recording:error.add_note('calibration report write failed: '+type(recording).__name__)
            raise
        result.update(frontend_preparation_s=frontend_preparation_s,frontend_total_wall_s=time.monotonic()-group_started)
        self.packet.save(directory/'public-result.json',result)
        target=result['result']
        target['calibration_run']={k:v for k,v in result.items() if k!='result'}
        target['scan_units']=rows
        return target

    def controller(self, packet_sha):
        cfg, matrix, groups = self.bound(packet_sha)
        budget = matrix_budget(matrix)
        cap, wall, launch = budget['max_new_calls'], budget['max_total_wall_s'], budget['launch_deadline_s']
        require_outside_git(self.root)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        assert not (self.root / 'budget.sqlite').exists() and (not (self.root / 'campaign.json').exists()) and (not (self.root / 'units').exists())
        if (self.packet.HERE / 'continuation.json').exists():
            if any(g.get('calibration') for g in groups):raise ValueError('old continuation cannot reserve new calibration work')
            policy = self.packet.read(self.packet.HERE / 'continuation.json')
            # Exact legacy r3 replay: paid/unknown calls are never re-reserved.
            assert (cap, wall, budget['query_count']) == (44280, 10800, 87)
            assert policy['budget_path'] == str(self.budget_path) and policy['budget_id'] == self.budget_id and (policy['skip_owner_groups'] == [0]) and (policy['remaining_queries'] == 83) and (policy['max_new_calls'] == 41920)
            ledger = CellBudgetLedger(self.budget_path, AttemptBudget(self.budget_id, cap))
            snapshot = ledger.snapshot()
            assert snapshot == policy['original_budget_snapshot']
            remaining = policy['original_controller_deadline_utc'] - time.time()
            assert budget['cleanup_reserve_s'] < remaining <= wall and policy['original_controller_deadline_utc'] <= snapshot['deadline_utc']
            deadline = time.monotonic() + remaining
            start = deadline - wall
        else:
            policy = None
            start = time.monotonic()
            deadline = start + wall
        lease = None
        physical_ok = True
        primary = None
        result = dict(status='failed', source_commit=self.packet.COMMIT, source_lock_sha256=self.packet.LOCK, packet_sha256=packet_sha, max_calls=cap if policy is None else policy['max_new_calls'], original_limit=cap, original_claimed_calls_not_retried=0 if policy is None else 2360, original_logical_unknown=0 if policy is None else 120, original_matrix_complete=policy is None, max_wall_s=wall, launch_deadline_s=launch, automatic_retries=0, units=[], MFU='unavailable_no_reliable_source', monitoring=self.host.monitoring_flags(cfg))

        def emit(value):
            value = dict(value, utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), elapsed_s=round(time.monotonic() - start, 3))
            try:
                with (self.root / 'progress.jsonl').open('a') as f:
                    f.write(json.dumps(value) + '\n')
                    f.flush()
                    os.fsync(f.fileno())
            except Exception as diagnostic:
                _diagnostic_warning('campaign_progress', diagnostic)
            try:
                print(json.dumps(value), flush=True)
            except (OSError, BrokenPipeError):
                pass

        def retain_final_error(error, phase):
            nonlocal primary
            result['status'] = 'failed'
            result.setdefault('finalization_errors', []).append(dict(phase=phase, error=self.host.helper.error_record(error)))
            if primary is None:
                primary = error
            elif primary is not error:
                primary.add_note(phase + ': ' + type(error).__name__)
        self.host.idle(cfg, 'formal-initial')
        assert not Path(cfg['lease_scope'], '.runner-lease.json').exists()
        if policy is None:
            # Input preflight supplies exact finite occurrence counts for every task.
            checked = [check_plan(self.packet.read(self.packet.HERE / g['plan'])) for g in groups]
            if any(g.get('calibration') for g in groups):
                cpu_limit=len(self.packet.read(self.packet.SOURCE/'deploy/autodl/sembench_records/comparison.json')['shared_resources']['cpu_affinity_slots'])
                for index,g in enumerate(groups):
                    validate_core_scan(self.packet.read(self.packet.HERE/g['plan']),g,index,cpu_limit)
            if (any(p['expected_calls'] != g['max_calls'] for g,p in zip(groups,checked,strict=True))
                    or sum(p['expected_calls']*(1+len(scan_jobs(g,i))) for i,(g,p) in enumerate(zip(groups,checked,strict=True))) != cap):
                raise ValueError('declared input calls differ from sealed matrix budget')
            ledger = CellBudgetLedger.create(self.budget_path, AttemptBudget(self.budget_id, cap), deadline_utc=time.time() + wall)
            for index,(group,plan) in enumerate(zip(groups,checked,strict=True)):
                for query in planned_queries(plan,group,index):
                    ledger.reserve_unit(query['query_id'], query['expected_calls'])
            assert ledger.snapshot()['allocated_requests'] == cap
        (self.root / 'units').mkdir(mode=0o700)
        if policy is not None:
            emit(dict(event='formal_existing_budget_continuation', reserved=44280, existing_claimed_calls=2360, new_reserved=0, actual_new_calls=0, owner_groups=len(groups) - 1, queries=83, original_controller_deadline_utc=policy['original_controller_deadline_utc']))
        else:
            emit(dict(event='formal_budget_created', reserved=cap, claims=0, actual_calls=0, owner_groups=len(groups), queries=budget['query_count']))
        try:
            lease = self.host.acquire_owner_lease(cfg)
            slots=group_slots(groups)
            for index, group in enumerate(groups):
                if policy is not None and index in policy['skip_owner_groups']:
                    continue
                if time.monotonic() - start >= launch:
                    raise TimeoutError('formal launch deadline reached')
                physical_ok = False
                if group.get('calibration'):
                    self.last_calibration_physical=True
                    try:
                        value=self.run_calibrated_group(cfg,packet_sha,index,group,lease,deadline,emit,slot_index=slots[index])
                    finally:
                        physical_ok=self.last_calibration_physical
                    result['units'].append(value)
                else:
                    options={} if slots[index]==index else dict(slot_index=slots[index])
                    result['units'].append(self.run_group(cfg, packet_sha, index, group, lease, deadline, emit,**options))
                physical_ok = True
            result['status'] = 'completed_remaining83_original_matrix_incomplete' if policy else 'completed'
        except BaseException as error:
            primary = error
            result['error'] = self.host.helper.error_record(error)
            if getattr(self,'last_calibration_failure',None) is not None:
                result['calibration_failure']=self.last_calibration_failure
            candidate = self.root / 'units' / ('g' + str(getattr(self,'last_native_slot',index)).zfill(3)) / 'independent-physical.json' if 'index' in locals() else None
            if candidate is not None and candidate.exists():
                p = self.packet.read_report(candidate)
                physical_ok = p['independent_physical_exit'] and (not self.host.helper.live_owned(self.host.api(), self.host.identities(p)))
        finally:
            try:
                result['final_resources'] = self.host.idle(cfg, 'formal-final')
            except BaseException as error:
                physical_ok = False
                result['final_resource_error'] = self.host.helper.error_record(error)
                retain_final_error(error, 'final_resources')
            if not physical_ok:
                retain_final_error(RuntimeError('final physical exit remains unconfirmed'), 'physical_exit')
            if time.monotonic() > deadline:
                retain_final_error(TimeoutError('formal total wall exceeded '+str(wall)+'s'), 'total_wall')
            result.update(budget=ledger.snapshot(), elapsed_s=time.monotonic() - start, physical_exit_confirmed=physical_ok, lease_status='held_unresolved' if lease else 'none_acquired')
            result['scan_succeeded_calls']=sum(row['measurement']['succeeded_model_calls']
                for unit in result['units'] for row in unit.get('scan_units',[])) + sum(
                row['measurement']['succeeded_model_calls'] for row in result.get('calibration_failure',{}).get('scan_units',[]))
            result['target_succeeded_calls']=sum(unit['actual_calls'] for unit in result['units'])
            evidence_saved = False
            try:
                self.packet.save(self.root / 'before-release.json', result)
                evidence_saved = True
            except BaseException as error:
                retain_final_error(error, 'before_release_evidence')
            if lease is not None and physical_ok and evidence_saved:
                try:
                    self.host.helper.check_lease(lease)
                    lease.release()
                    result['lease_status'] = 'released'
                except BaseException as error:
                    result['lease_status'] = 'release_unconfirmed'
                    retain_final_error(error, 'lease_release')
            result['elapsed_s'] = time.monotonic() - start
            if time.monotonic() > deadline and (not any((e['phase'] == 'total_wall' for e in result.get('finalization_errors', [])))):
                retain_final_error(TimeoutError('formal total wall exceeded '+str(wall)+'s'), 'total_wall')
            if primary is not None:
                result['error'] = self.host.helper.error_record(primary)
            result['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            try:
                self.packet.save(self.root / 'campaign.json', result)
            except BaseException as error:
                retain_final_error(error, 'campaign_evidence')
            emit(dict(event='formal_campaign_completed', status=result['status'], completed_owner_groups=len(result['units']), successful_calls=sum((u['actual_calls'] for u in result['units'])), lease_status=result['lease_status'], physical_exit_confirmed=physical_ok, error=result.get('error')))
        if primary is not None:
            raise primary
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('check', 'run', 'worker', 'audit'))
    parser.add_argument('--packet', required=True, type=Path)
    parser.add_argument('--packet-sha', required=True)
    parser.add_argument('--output-dir', required=True, type=Path)
    parser.add_argument('--execute-model', action='store_true')
    parser.add_argument('--offline', action='store_true', help='check package/source/inputs/plans without claiming target-host qualification')
    parser.add_argument('--index', type=int)
    parser.add_argument('--context', type=Path)
    parser.add_argument('--worker-output', type=Path)
    parser.add_argument('--report', type=Path)
    parser.add_argument('--audit-output', type=Path)
    args = parser.parse_args(argv)
    if args.offline and (args.execute_model or args.action not in ('check','run')):
        parser.error('--offline is only for non-executing package checks')
    if args.action == 'worker' and (args.index is None or args.context is None or args.worker_output is None):
        parser.error('worker requires --index, --context and --worker-output')
    if args.action == 'audit' and (args.report is None or args.audit_output is None):
        parser.error('audit requires --report and --audit-output')
    runtime = load_runtime(args.packet, args.packet_sha)
    entrypoint = Path(__file__).resolve().parents[2] / 'scripts/experiments/run_movie_records_matrix.py'
    driver = MovieMatrix(runtime, args.output_dir, entrypoint)
    driver.packet_digest = args.packet_sha
    if args.action == 'check' or (args.action == 'run' and not args.execute_model):
        result = driver.check(args.packet_sha, offline=args.offline)
    elif args.action == 'run':
        result = driver.controller(args.packet_sha)
    elif args.action == 'worker':
        return driver.worker(args.packet_sha, args.index, args.context, args.worker_output)
    else:
        return driver.token_audit(args.report, args.audit_output)
    print(json.dumps(result))
    return 0
