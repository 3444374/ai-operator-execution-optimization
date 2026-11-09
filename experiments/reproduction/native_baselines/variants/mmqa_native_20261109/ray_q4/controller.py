"""One native Ray MMQA Q4 cold query under the existing host supervisor."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback
from collections import Counter

ROOT = Path(__file__).resolve().parent
CANDIDATE = ROOT
SOURCE = Path(os.environ['CASE_SOURCE'])
DRIVER = os.environ['CASE_DRIVER_PYTHON']
MODEL = os.environ['CASE_MODEL_PYTHON']
FROZEN_SHA = sys.argv[1]
TREE = 'a4d51b7527361613f6793cd1a2c59efb76761482'
BASE = 'f44f174a27e564734774c015fa0847334698a321'
HELPER = Path(os.environ['CASE_HOST_HELPER'])
SOCKET_HELPER = Path(os.environ['CASE_SOCKET_HELPER'])


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def verify_candidate():
    if sha(CANDIDATE / 'FROZEN.json') != FROZEN_SHA:
        raise ValueError('frozen candidate changed')
    frozen = json.loads((CANDIDATE / 'FROZEN.json').read_text())
    for name, expected in frozen['files_sha256'].items():
        if sha(CANDIDATE / name) != expected:
            raise ValueError('candidate bytes differ: ' + name)
    for path, expected in frozen['external_files_sha256'].items():
        if sha(path) != expected:
            raise ValueError('external frozen file changed: ' + path)
    tree = subprocess.check_output(['git', '-C', str(SOURCE), 'write-tree'], text=True).strip()
    if tree != TREE or subprocess.check_output(['git', '-C', str(SOURCE), 'diff', '--name-only'], text=True):
        raise ValueError('source differs from verified f44 tree')
    return frozen


def physical_audit(directory):
    events = [json.loads(line) for path in directory.glob('native-events-*.jsonl')
              for line in path.read_text().splitlines()]
    kinds = Counter(row['event'] for row in events)
    started = [row for row in events if row['event'] == 'native_generate_started']
    finished = [row for row in events if row['event'] == 'native_generate_finished']
    constructors = [row for row in events if row['event'] == 'native_constructor_ready']
    predictions = json.loads((directory / 'predictions-PRIVATE.json').read_text())
    messages = json.loads((directory / 'messages-PRIVATE.json').read_text())
    receipts = [json.loads(path.read_text()) for path in directory.glob('native-output-*.json')]
    by_native = {str(row['native_usage']['request_id']): row for row in predictions}
    direct_matches = sum(str(row['request_id']) in by_native and len(row['outputs']) == 1
        and by_native[str(row['request_id'])]['generated_text'] == row['outputs'][0]['text']
        for row in receipts)
    expected_ids = [row['call_id'] for row in messages]
    actual_ids = [row['call_id'] for row in predictions]
    from src.experiments.shared_request_budget import SharedClaimedUnit
    from src.experiments.attempt_ledger import AttemptBudget
    bound = json.loads((directory/'RUN-BOUND.json').read_text())
    charged = SharedClaimedUnit(directory/'logical-unit.sqlite',AttemptBudget(bound['budget_id'],24),bound['budget_id']).attempts
    terminal_by_id = {str(row['request_id']): row for row in finished}
    consumer_closed = [row for row in events if row['event'] == 'native_generate_consumer_closed']
    consumer_close_valid = all(str(row['request_id']) in terminal_by_id
        and row.get('terminal_confirmed') is True
        and terminal_by_id[str(row['request_id'])]['monotonic_ns'] < row['monotonic_ns']
        and terminal_by_id[str(row['request_id'])]['attempt'] == row['attempt']
        for row in consumer_closed)
    complete = (direct_matches == 24 and len(by_native) == 24 and consumer_close_valid and len(started) == len(finished) == len(receipts) == len(predictions) == 24
        and charged == 24 and len(constructors) == 1
        and len(set(actual_ids)) == 24 and set(actual_ids) == set(expected_ids)
        and Counter(str(row['request_id']) for row in started) == Counter(str(row['request_id']) for row in finished)
        and len({str(row['request_id']) for row in started}) == 24
        and not kinds['native_generate_error'] and not kinds['native_generate_unconfirmed']
        and Counter(row['generated_text'] for row in predictions)
            == Counter(output['text'] for row in receipts for output in row['outputs']))
    report = dict(complete=complete, direct_request_id_and_output_text_matches=direct_matches, consumer_closes_follow_confirmed_terminal=consumer_close_valid, event_counts=dict(kinds), physical_attempts=charged,
        unique_logical_occurrences=len(set(actual_ids)), unique_native_request_ids=len({str(row['request_id']) for row in started}),
        one_model_constructor=len(constructors) == 1, model_constructors=constructors,
        started_and_finished_request_ids_equal=Counter(str(row['request_id']) for row in started) == Counter(str(row['request_id']) for row in finished),
        raw_text_multiset_matches_terminal_receipts=Counter(row['generated_text'] for row in predictions) == Counter(output['text'] for row in receipts for output in row['outputs']),
        native_add_request_observed=False, kernel_execution_count=None,
        unknown=len(started)-len(finished), output_tokens=sum(row.get('output_tokens',0) for row in finished),
        prompt_tokens=sum(row.get('prompt_tokens',0) for row in finished),
        first_generate_monotonic_ns=min((row['monotonic_ns'] for row in started), default=None),
        last_terminal_monotonic_ns=max((row['monotonic_ns'] for row in finished), default=None))
    return report


def cleanup_inactive_semaphores(names, before, after):
    """Remove only observed new POSIX semaphores after all owned work exited."""
    import ctypes
    cleanup = []
    for name in names:
        if not name.startswith('sem.mp-'):
            raise RuntimeError('unexpected remaining SHM type; stop without unlinking')
        path = Path('/dev/shm') / name
        stat = path.stat()
        if not (stat.st_uid == os.getuid() and stat.st_size == 32
                and before['captured_epoch_s'] <= stat.st_mtime <= after['captured_epoch_s']):
            raise RuntimeError('remaining semaphore is not bound to this round')
        holders = []
        errors = []
        for proc in Path('/proc').iterdir():
            if not proc.name.isdigit():
                continue
            try:
                if name in (proc/'maps').read_text():
                    holders.append(dict(pid=int(proc.name),kind='mapping'))
                for fd in (proc/'fd').iterdir():
                    try:
                        if name in os.readlink(fd):
                            holders.append(dict(pid=int(proc.name),kind='fd'))
                    except FileNotFoundError:
                        pass
            except (FileNotFoundError,ProcessLookupError):
                pass
            except PermissionError:
                errors.append(int(proc.name))
        evidence = dict(name=name,inode=stat.st_ino,uid=stat.st_uid,size=stat.st_size,
            mtime=stat.st_mtime,sha256=sha(path),holders=holders,scan_errors=errors,
            scope='new name within this exclusive round; exact allocating library unproven; native cleanup did not remove it')
        if holders or errors or path.stat().st_ino != stat.st_ino:
            raise RuntimeError('semaphore holder or identity remains unresolved')
        libc = ctypes.CDLL(None,use_errno=True)
        unlink = libc.sem_unlink
        unlink.argtypes = [ctypes.c_char_p]
        unlink.restype = ctypes.c_int
        rc = unlink(('/'+name.removeprefix('sem.')).encode())
        if rc != 0:
            raise OSError(ctypes.get_errno(),'inactive semaphore cleanup failed')
        evidence.update(action='sem_unlink exact inactive owned name',removed=not path.exists())
        cleanup.append(evidence)
    return cleanup


def run_round(number, host, api, resources):
    directory = ROOT / ('run-Q' + str(number))
    directory.mkdir(mode=0o700)
    started = time.monotonic_ns()
    verify_candidate()
    run_id = 'ray-native-mmqa-q4-cold-1109-r1'
    temp = Path(os.environ['CASE_TEMP'])
    temp.mkdir(mode=0o700)
    ray_temp = temp / 'r'
    before = host.snapshot(api, resources, run_id + '-before')
    host.private_json(directory / 'before.json', before)
    host.assert_idle(before, resources)
    lease = api.acquire_runner_lease(Path(os.environ['CASE_LEASE_SCOPE']),
        config_fingerprint=api.HOST_SCOPE_FINGERPRINT, repository_commit=BASE, recover_stale=False)
    from src.observability.process_resources.supervised import ResourceMonitor
    from src.experiments.attempt_ledger import AttemptBudget
    from src.experiments.cell_budget import CellBudgetLedger
    current = json.loads(json.dumps(resources))
    available = os.statvfs('/dev/shm')
    current['shared_resources']['max_owned_shm_bytes'] = available.f_bavail * available.f_frsize
    host.private_json(directory / 'RESOURCE-CONFIG.json', current)
    owned = set()
    reports = []
    failure = None
    ledger = None
    monitor = ResourceMonitor(current['shared_resources'])
    audit = worker = driver = None
    try:
        budget = AttemptBudget(run_id, 24)
        ledger = CellBudgetLedger.create(directory / 'logical-unit.sqlite', budget, deadline_utc=time.time()+1800)
        ledger.reserve_unit(run_id,24)
        ledger.claim_shared_unit(run_id)
        bound = dict(source=str(SOURCE), budget_id=run_id, physical_attempt_limit=24,
            ray_temp_dir=str(ray_temp), baseline_name='Ray Data LLM native', semantic_operator='MMQA Q4 all24 text genres JSON Extract -> original genre/title groups',
            execution_backend='Ray Data LLM build_processor / native vLLM 0.22',
            profile='independent cold query', task=number, config_path=str(ROOT/'CONFIG.json'), round=number, warmup_extra=0, whole_query_retries=0)
        host.private_json(directory / 'RUN-BOUND.json', bound)
        host.private_json(directory / 'STARTED.json',dict(pid=os.getpid(),monotonic_ns=started,run_id=run_id,
            frozen_sha256=FROZEN_SHA,source_tree=TREE,logical_budget=ledger.snapshot()))
        os.environ.update(PYTHONPATH=str(SOURCE/'code'),CUDA_VISIBLE_DEVICES='0',
            HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',PYTHONDONTWRITEBYTECODE='1',
            LITELLM_LOCAL_MODEL_COST_MAP='True',TMPDIR=str(temp),VLLM_RPC_BASE_PATH=str(temp),
            RAY_TMPDIR=str(ray_temp),OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',
            TOKENIZERS_PARALLELISM='false')
        report = api.supervise([DRIVER,'-u','-B',str(CANDIDATE/'prepare.py'),str(directory)],
            directory/'prepare-supervision',timeout_s=120,grace_s=8,natural_exit_grace_s=4,monitor=monitor)
        reports.append(report)
        owned |= host.report_identities(report)
        host.private_json(directory/'PREPARE-SUPERVISOR.json',report)
        if report['worker_exit'] != 0:
            raise RuntimeError('fresh source scan/original MMQA Q4 preparation failed before model')
        report = api.supervise([MODEL,'-u','-B',str(CANDIDATE/'worker.py'),str(directory)],
            directory/'worker-supervision',timeout_s=1320,grace_s=8,natural_exit_grace_s=4,monitor=monitor)
        reports.append(report)
        owned |= host.report_identities(report)
        host.private_json(directory/'WORKER-SUPERVISOR.json',report)
        worker=json.loads((directory/'worker.json').read_text())
        if report['worker_exit'] != 0 or worker['status'] != 'completed' or not worker['ray_driver_disconnected']:
            raise RuntimeError('native worker did not complete and disconnect')
        audit = physical_audit(directory)
        host.private_json(directory/'PHYSICAL-AUDIT.json',audit)
        if not audit['complete']:
            raise RuntimeError('native terminal or occurrence audit incomplete')
        report = api.supervise([DRIVER,'-u','-B',str(CANDIDATE/'finish.py'),str(directory)],
            directory/'finish-supervision',timeout_s=120,grace_s=8,natural_exit_grace_s=4,monitor=monitor)
        reports.append(report)
        owned |= host.report_identities(report)
        host.private_json(directory/'FINISH-SUPERVISOR.json',report)
        if report['worker_exit'] != 0:
            raise RuntimeError('original MMQA Q4 sink/evaluator failed')
        driver = json.loads((directory/'driver.json').read_text())
    except BaseException as error:
        extra = getattr(error,'report',None)
        if extra is not None:
            reports.append(extra)
            owned |= host.report_identities(extra)
        failure=dict(type=type(error).__name__,message=str(error),traceback=traceback.format_exc())
    finally:
        if host.live_owned(api,owned):
            host.cleanup_owned(api,owned,20)
        live=host.live_owned(api,owned)
        resource_report=monitor.finish()
        host.private_json(directory/'RESOURCES-BEFORE-EXTERNAL-CLEANUP.json',resource_report)
        after=host.snapshot(api,current,run_id+'-after',owned)
        host.private_json(directory/'after.json',after)
        idle_error=None
        try:
            host.assert_idle(after,current)
        except BaseException as error:
            idle_error=dict(type=type(error).__name__,message=str(error))
        sockets_cleanup=None
        if not live and idle_error is None:
            cleaner=load_module('ray_q4_private_temp',SOCKET_HELPER)
            sockets_cleanup=cleaner.cleanup_inactive_socket_nodes(temp,Path('/proc/net/unix').read_text())
        sockets=[str(path.relative_to(temp)) for path in temp.rglob('*') if path.is_socket()]
        semaphore_cleanup = []
        if not live and idle_error is None and not resource_report['failures']:
            semaphore_cleanup = cleanup_inactive_semaphores(resource_report['remaining_new_shm_names'], before, after)
            resource_report = monitor.finish()
        host.private_json(directory/'RESOURCES.json',resource_report)
        physical_idle=not live and idle_error is None and not sockets and resource_report['passed']
        released=False
        if physical_idle:
            host.check_lease(lease)
            lease.release()
            released=True
        final=dict(status='completed' if failure is None and physical_idle else 'failed',round=number,
            run_id=run_id,profile='independent cold query',failure=failure,idle_error=idle_error,
            source_tree=TREE,frozen_sha256=FROZEN_SHA,logical_budget=None if ledger is None else ledger.snapshot(),
            physical_audit=audit,worker=worker,quality=None if driver is None else driver['quality'],
            timing=dict(controller_started_monotonic_ns=started,completed_monotonic_ns=time.monotonic_ns(),
                sink_ready_monotonic_ns=None if driver is None else driver['timing']['sink_ready_monotonic_ns'],
                finish=None if driver is None else driver['timing'],
                scope='frozen-input verification through new native owner, inference, shutdown and original sink; fresh CSV/original LOTUS message preparation included, original CSV reread also occurs in finish; evaluation separate; not warm time'),
            resource_passed=resource_report['passed'],remaining_owned=live,physical_idle=physical_idle,
            socket_cleanup=sockets_cleanup,inactive_semaphore_cleanup=semaphore_cleanup,remaining_private_sockets=sockets,lease_released=released,
            no_automatic_retry=True)
        host.private_json(directory/'FINAL.json',final)
        print(json.dumps(dict(round=number,status=final['status'],quality=final['quality'],
            physical_idle=physical_idle,lease_released=released)),flush=True)
    return final


def main():
    if (ROOT/'STARTED.json').exists() or any(ROOT.glob('run-Q*')):
        raise FileExistsError('campaign already claimed; inspect existing state instead of restarting')
    verify_candidate()
    if sha(HELPER) != '340bb1acf01b8e3e8faded3f21bc11177bfb49a31b670a31e101f2568d733760':
        raise ValueError('host helper identity changed')
    if sha(SOCKET_HELPER) != '64bb48944f91524eea254a7f7097175165af9d128fd1b459fc6a830be15b7577':
        raise ValueError('private TEMP cleanup identity changed')
    host=load_module('ray_q4_host',HELPER)
    api=host.production_api(SOURCE/'code')
    resources=json.loads(Path(os.environ['CASE_RESOURCES']).read_text())
    os.sched_setaffinity(0,set(resources['shared_resources']['cpu_affinity_slots']))
    os.chdir(SOURCE)
    host.private_json(ROOT/'STARTED.json',dict(pid=os.getpid(),monotonic_ns=time.monotonic_ns(),rounds=1,per_round=24,campaign_total_max=24,
        baseline_name='Ray Data LLM native',semantic_operator='MMQA Q4 genres Extract / original genre groups; independent cold',execution_backend='Ray build_processor / vLLM0.22',
        profile='one independent cold query; not a warm query',frozen_sha256=FROZEN_SHA))
    finals=[]
    for number in ('4',):
        final=run_round(number,host,api,resources)
        finals.append(final)
        if final['status'] != 'completed':
            break
    host.private_json(ROOT/'FINAL.json',dict(status='completed' if len(finals)==1 and all(x['status']=='completed' for x in finals) else 'stopped',
        rounds=finals,completed_monotonic_ns=time.monotonic_ns(),retries=0,extra_warmup=0))


if __name__ == '__main__':
    main()
