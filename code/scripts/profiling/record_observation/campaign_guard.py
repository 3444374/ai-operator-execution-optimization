"""Finite capture-campaign guards around the existing supervisor and call ledger.

No scheduling/model policy changes. Limits are sampled operational stop bounds,
not filesystem quotas: cleanup can add evidence after a stop. Never delete it.
"""
import argparse
from contextlib import contextmanager
from functools import wraps
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import stat
import sys
import time

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO/'code'))
sys.path.insert(0, str(HERE))

from src.baselines.common.private_artifacts import (
    publish_private_json, require_outside_git, write_private_json,
)
from src.experiments.attempt_ledger import AttemptBudget, BudgetError
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments import lotus_records_supervisor as supervision


class GuardStop(RuntimeError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


def _identity():
    path = Path('/proc/sys/kernel/random/boot_id')
    if path.exists():
        return (platform.node(), path.read_text().strip())
    import psutil
    return (platform.node(), int(psutil.boot_time()))


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def scan_bytes(roots):
    """Count complete owned trees, including completed cells; do not follow links."""
    roots = {Path(p).absolute() for p in roots}
    if any(root.is_symlink() or not root.is_dir() for root in roots):
        raise ValueError('artifact root must remain a real directory')
    roots = sorted({p.resolve(strict=True) for p in roots}, key=lambda p: len(p.parts))
    selected = []
    for root in roots:
        if not any(root == p or p in root.parents for p in selected):
            selected.append(root)
    result = dict(bytes=0, files=0, vanished=0, links=0)
    def visit(directory):
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    path = Path(entry.path)
                    try:
                        value = path.lstat()
                        if stat.S_ISLNK(value.st_mode):
                            target = path.resolve(strict=False)
                            if not any(target == r or r in target.parents for r in selected):
                                raise ValueError('artifact link escapes the monitored roots')
                            result['links'] += 1
                            result['bytes'] += value.st_size
                        elif stat.S_ISDIR(value.st_mode):
                            visit(path)
                        elif stat.S_ISREG(value.st_mode):
                            result['files'] += 1
                            result['bytes'] += value.st_size
                    except FileNotFoundError:
                        result['vanished'] += 1  # Atomic publication / SQLite journal race.
        except FileNotFoundError:
            if directory in selected:
                raise
            result['vanished'] += 1
    for root in selected:
        visit(root)
    return result


_WRITE_ENV_PATHS = ('TMPDIR', 'RAY_TMPDIR', 'VLLM_LOG_DIR', 'PYTHONPYCACHEPREFIX')


def _require_covered(manifest, paths):
    roots = [Path(p).resolve(strict=True) for p in manifest['artifact_roots']]
    for path in paths:
        if type(path) is not str or not Path(path).is_absolute():
            raise BudgetError('owned output paths must be explicit absolute paths')
        target = Path(path).resolve(strict=False)
        if not any(target == root or root in target.parents for root in roots):
            raise BudgetError('owned output path is outside the monitored campaign roots')


def _configuration_coverage(manifest, config):
    """Write destinations only; model/data/source/immutable host bindings are inputs."""
    paths = [config[k] for k in ('artifact_root', 'nsys_tmp') if config.get(k) is not None]
    ray = config.get('ray_options') or {}
    paths += [ray[k] for k in ('_temp_dir', '_plasma_directory') if ray.get(k) is not None]
    environment = config.get('environment') or {}
    paths += [environment[k] for k in _WRITE_ENV_PATHS if environment.get(k) is not None]
    _require_covered(manifest, paths)
    if (config.get('workload') or {}).get('kind') == 'movie_session':
        if not environment.get('TMPDIR') or not (ray.get('_temp_dir') or environment.get('RAY_TMPDIR')):
            raise BudgetError('Movie runtime temporary directories must be explicitly bound')


def validate_coverage(manifest):
    paths = [manifest[k] for k in ('runtime_temp_root', 'ray_temp_root') if manifest.get(k) is not None]
    for cell in manifest['cells']:
        paths.append(str(Path(cell['prepared_config']).parent))
        if cell.get('ready_dir') is not None:
            paths.append(cell['ready_dir'])
        base = Path(cell['base_config'])
        raw = base.read_bytes()
        if hashlib.sha256(raw).hexdigest() != cell['base_sha256']:
            raise BudgetError('frozen private base changed')
        _configuration_coverage(manifest, json.loads(raw))
    _require_covered(manifest, paths)


class Guard:
    @classmethod
    def create(cls, root, manifest, **options):
        root = Path(root).absolute()
        require_outside_git(root)
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        for key in ('wall_seconds', 'max_bytes', 'scan_interval_s',
                    'cleanup_reserve_s', 'collection_reserve_s'):
            value = manifest[key]
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError('campaign limits must be finite positive numbers')
        if manifest['cleanup_reserve_s']+manifest['collection_reserve_s'] >= manifest['wall_seconds']:
            raise ValueError('the campaign must reserve a positive work interval')
        if manifest['schema'] != 'semloom.record_campaign.v1':
            raise ValueError('explicit campaign schema required')
        budget = AttemptBudget(manifest['campaign_id'], manifest['call_limit'])
        if type(manifest['max_bytes']) is not int:
            raise ValueError('the artifact bound is an integer byte count')
        cells = manifest['cells']
        if (not cells or len({c['id'] for c in cells}) != len(cells)
                or any(type(c['calls']) is not int or c['calls'] <= 0 for c in cells)
                or sum(c['calls'] for c in cells) != budget.limit):
            raise ValueError('distinct whole cells must exactly cover the fixed call limit')
        if type(manifest['historical_charge']) is not int or manifest['historical_charge'] < 0:
            raise ValueError('historical charge must remain a nonnegative integer')
        for cell in cells:
            AttemptBudget(cell['id'], cell['calls'])
            AttemptBudget(cell['stage'], 1)
            if (type(cell['base_sha256']) is not str
                    or re.fullmatch('[0-9a-f]{64}', cell['base_sha256']) is None):
                raise ValueError('every private base must be frozen before initialization')
            require_outside_git(Path(cell['base_config']))
            require_outside_git(Path(cell['prepared_config']))
        for path in manifest['artifact_roots']:
            require_outside_git(Path(path))
        scan_bytes(manifest['artifact_roots'])
        validate_coverage(manifest)
        if any((root/name).exists() for name in ('CAMPAIGN.json', 'START.json', 'campaign-budget.sqlite')):
            raise FileExistsError('never reset a previous campaign or its reservations')
        clock, epoch = options.get('clock', time.monotonic), options.get('epoch', time.time)
        identity = options.get('clock_identity', _identity)
        identity = identity() if callable(identity) else identity
        start = dict(monotonic=clock(), epoch=epoch(), clock_identity=identity,
                     manifest_sha256=_digest(manifest))
        write_private_json(root/'CAMPAIGN.json', manifest)
        write_private_json(root/'START.json', start)
        ledger = CellBudgetLedger.create(root/'campaign-budget.sqlite', budget, deadline_utc=None)
        for cell in cells:
            ledger.reserve_unit(cell['id'], cell['calls'])
        return cls(root, **options)

    def __init__(self, root, *, clock=time.monotonic, epoch=time.time, clock_identity=_identity):
        self.root, self.clock, self.epoch = Path(root).absolute(), clock, epoch
        self.manifest = json.loads((self.root/'CAMPAIGN.json').read_text())
        self.start = json.loads((self.root/'START.json').read_text())
        identity = clock_identity() if callable(clock_identity) else clock_identity
        if (_digest(self.manifest) != self.start['manifest_sha256']
                or json.loads(json.dumps(identity)) != self.start['clock_identity']
                or clock() < self.start['monotonic']):
            raise ValueError('campaign clock/boot or frozen configuration changed')
        self.ledger = CellBudgetLedger(self.root/'campaign-budget.sqlite',
            AttemptBudget(self.manifest['campaign_id'], self.manifest['call_limit']))
        snapshot = self.ledger.snapshot()
        if (snapshot['allocated_requests'] != self.manifest['call_limit']
                or {u['unit_id']:u['requests'] for u in snapshot['units']}
                   != {c['id']:c['calls'] for c in self.manifest['cells']}):
            raise ValueError('the original whole-campaign reservation is incomplete')
        self.deadline = self.start['monotonic']+self.manifest['wall_seconds']
        self.work_deadline = self.deadline-self.manifest['cleanup_reserve_s']-self.manifest['collection_reserve_s']
        self.last_scan = None
        self.observation = dict(bytes=None, files=None, vanished=0)
        self.metrics = dict(checks=0, scans=0, scan_wall_seconds=0., scan_thread_cpu_seconds=0.,
                            maximum_observed_bytes=0)

    def _stop(self, reason, error=None):
        path = self.root/'STOP.json'
        if not path.exists():
            write_private_json(path, dict(reason=reason, observed_monotonic=self.clock(),
                observed_epoch=self.epoch(), error_type=None if error is None else type(error).__name__,
                observation=self.observation, limits={k:self.manifest[k] for k in
                    ('wall_seconds','max_bytes','cleanup_reserve_s','collection_reserve_s')},
                scope='first sampled stop; not a hard filesystem quota; preserve cleanup overshoot'))
        raise GuardStop(reason) from error

    def check(self, *, force=False, collection=False):
        self.metrics['checks'] += 1
        if (self.root/'STOP.json').exists():
            raise GuardStop('campaign_already_stopped')
        now = self.clock()
        stop_at = (self.deadline-min(self.manifest['collection_reserve_s']/3,
                    self.manifest['cleanup_reserve_s']) if collection else self.work_deadline)
        if now >= stop_at:
            self._stop('time_limit')
        if force or self.last_scan is None or now-self.last_scan >= self.manifest['scan_interval_s']:
            started, cpu = time.monotonic(), time.thread_time()
            try:
                self.observation = scan_bytes(self.manifest['artifact_roots'])
            except Exception as error:
                self._stop('artifact_scan_error', error)
            finally:
                self.metrics['scan_wall_seconds'] += time.monotonic()-started
                self.metrics['scan_thread_cpu_seconds'] += time.thread_time()-cpu
            self.last_scan = now
            self.metrics['scans'] += 1
            self.metrics['maximum_observed_bytes'] = max(self.metrics['maximum_observed_bytes'],
                                                         self.observation['bytes'])
            if self.observation['bytes'] >= self.manifest['max_bytes']:
                self._stop('artifact_limit')
        return dict(self.observation)

    def claim(self, cell_id):
        self.check(force=True)
        validate_coverage(self.manifest)
        return self.ledger.claim_unit(cell_id)

    @contextmanager
    def supervision(self, *, collection=False):
        original = supervision.supervise
        if getattr(original, '_campaign_guard', False):
            raise RuntimeError('a campaign guard cannot be nested')
        @wraps(original)
        def bounded(*args, **kwargs):
            previous = kwargs.get('monitor')
            def monitor(identities):
                self.check(collection=collection)
                if previous is not None:
                    previous(identities)
            return original(*args, **dict(kwargs, monitor=monitor))
        bounded._campaign_guard = True
        supervision.supervise = bounded
        try:
            yield
        finally:
            supervision.supervise = original

    def save_metrics(self, label):
        write_private_json(self.root/(label+'-GUARD-METRICS.json'),
            dict(self.metrics, last_observation=self.observation,
                 elapsed_seconds=self.clock()-self.start['monotonic'],
                 scope='this guard invocation; scanning thread CPU, not total application CPU'))


def account_calls(root, cap, claimed):
    """Known receipts and unresolved claimed allowances are different quantities."""
    root = Path(root)
    charged = cap if claimed is True else 0 if claimed is False else claimed
    if type(charged) is not int or not 0 <= charged <= cap:
        raise BudgetError('invalid claimed allowance')
    records = {}
    def consume(values):
        seen = set()
        for row in values:
            key = row['call_id']
            if key in seen:
                raise BudgetError('duplicate call ID in one ledger')
            seen.add(key)
            if row.get('status') not in ('succeeded','failed','unknown'):
                raise BudgetError('invalid call terminal status')
            if type(row.get('submission_confirmed')) is not bool:
                raise BudgetError('explicit submission evidence required')
            if row['status']=='succeeded' and not row['submission_confirmed']:
                raise BudgetError('success requires a confirmed submission')
            previous = records.get(key)
            if previous is not None:
                for field in ('query_id','status','submission_confirmed','engine_uid','engine_request_id'):
                    if field in previous and field in row and previous[field] != row[field]:
                        raise BudgetError('call receipt identity or terminal conflict')
            records[key] = dict(previous or {}, **row)
    for path in sorted((root/'session').glob('query-*.json')):
        value = json.loads(path.read_text()).get('backend_report') or {}
        calls = value.get('calls') or {}
        consume(calls.get('records', []) if isinstance(calls, dict) else calls)
        close = value.get('backend_close') or {}
        consume(close.get('calls', []))
    path = root/'WORKER-RESULT.json'
    if path.exists():
        consume(json.loads(path.read_text()).get('calls', []))
    if len(records) > charged:
        raise BudgetError('call records exceed the once-claimed allowance')
    counts = {status:sum(r['status']==status for r in records.values())
              for status in ('succeeded','failed','unknown')}
    submitted = sum(r['submission_confirmed'] for r in records.values())
    missing = charged-len(records)
    return dict(cap=cap, claimed_cap=charged, observed_records=len(records),
        confirmed_submitted=submitted, succeeded=counts['succeeded'], failed=counts['failed'],
        unknown_records=counts['unknown'], missing_claimed=missing,
        unknown_responsibilities=counts['unknown']+missing,
        actual_submitted_exact=None if missing or counts['unknown'] else submitted,
        conservative_charge=charged,
        scope='missing claimed allowance is unresolved responsibility, not proof of submitted inference')


def _exclusive(root):
    @contextmanager
    def locked():
        with (root/'campaign.lock').open('a') as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
    return locked()


def _cell(guard, cell_id):
    return next(c for c in guard.manifest['cells'] if c['id']==cell_id)


def _check_gate(guard, cell):
    cells = guard.manifest['cells']
    index = cells.index(cell)
    for previous in cells[:index]:
        receipt = guard.root/(previous['id']+'-GUARD-END.json')
        if not receipt.exists() or json.loads(receipt.read_text())['status'] != 'passed':
            raise BudgetError('every preceding cell must complete before the next')
    if index and cells[index-1]['stage'] != cell['stage']:
        stage = cells[index-1]['stage']
        gate = json.loads((guard.root/('GATE-'+stage+'.json')).read_text())
        expected = {c['id']:hashlib.sha256((guard.root/(c['id']+'-GUARD-END.json')).read_bytes()).hexdigest()
                    for c in cells[:index] if c['stage']==stage}
        if gate.get('status')!='passed' or gate.get('source')!=guard.manifest['source'] or gate.get('cells')!=expected:
            raise BudgetError('independent stage qualification receipt required')


def _check_ray(guard):
    marker = guard.root/'REAL-RAY-READY.json'
    if not marker.exists():
        raise BudgetError('five actual Ray CPU tests must pass before any model claim')
    value = json.loads(marker.read_text())
    log = guard.root/('CPU-ray-'+guard.manifest['cells'][0]['id'])/'worker.log'
    if (value.get('status')!='passed' or value.get('source')!=guard.manifest['source']
            or value.get('tests')!=5 or value.get('skips')!=0 or not log.is_file()
            or hashlib.sha256(log.read_bytes()).hexdigest()!=value.get('log_sha256')):
        raise BudgetError('the real Ray qualification receipt or complete log changed')


def run_cell(guard, cell_id):
    import workflow
    import control_resource_C2 as controller
    cell = _cell(guard, cell_id)
    validate_coverage(guard.manifest)
    _check_gate(guard, cell)
    _check_ray(guard)
    path = Path(cell['prepared_config'])
    config = workflow.check(path)
    _configuration_coverage(guard.manifest, config)
    if (path.resolve() != Path(config['artifact_root']).resolve()/'PREPARED-CONFIG.json'
            or config['expected_source'] != guard.manifest['source']
            or config['tool_source'] != guard.manifest['source']
            or config['workload']['expected_calls'] != cell['calls']
            or config['base_config']['sha256'] != cell['base_sha256']
            or controller.profiled(config)):
        raise BudgetError('same-tree non-Nsight whole-cell identity required')
    effective = controller.worker_environment(config, os.environ)
    _require_covered(guard.manifest, [effective[k] for k in _WRITE_ENV_PATHS if effective.get(k)])
    if any(key in effective for key in ('SEMLOOM_MFU_TRACE_DIR','SEMLOOM_MFU_TRACE_MILESTONES')):
        raise BudgetError('optional native trace writers must be off in both arms')
    if effective.get('RAY_ADDRESS'):
        raise BudgetError('a campaign must not attach to an unrelated Ray cluster')
    guard.check(force=True)
    # Retain enough of the campaign for a complete planned cell and its final
    # cleanup/collection; no partial cell is started to spend leftover calls.
    if guard.clock()+config['limits']['controller_timeout_s'] > guard.work_deadline:
        guard._stop('insufficient_time_for_whole_cell')
    guard.claim(cell_id)
    saved_environment = dict(os.environ)
    error = None
    try:
        with guard.supervision():
            controller.main(path.parent)  # Original lease, worker, monitor, cleanup and footer owner.
        guard.check(force=True)
    except BaseException as caught:
        error = caught
    finally:
        os.environ.clear()
        os.environ.update(saved_environment)
        try:
            counts = account_calls(path.parent, cell['calls'], True)
        except Exception as accounting_error:
            if error is None:
                error = accounting_error
            counts = dict(claimed_cap=cell['calls'], conservative_charge=cell['calls'],
                confirmed_submitted=None, succeeded=None, failed=None, unknown_records=None,
                missing_claimed=None, unknown_responsibilities=None, actual_submitted_exact=None,
                accounting_error_type=type(accounting_error).__name__)
        result_path = path.parent/'RESULT.json'
        try:
            result = json.loads(result_path.read_text()) if result_path.exists() else {}
        except Exception as result_error:
            if error is None:
                error = result_error
            result = dict(status='unknown', result_read_error_type=type(result_error).__name__)
        passed = (error is None and counts['succeeded']==cell['calls']
            and not counts['failed'] and not counts['unknown_responsibilities']
            and result.get('status')=='completed' and result.get('physical_cleanup_confirmed') is True
            and result.get('lease_released') is True)
        write_private_json(guard.root/(cell_id+'-GUARD-END.json'), dict(
            status='passed' if passed else 'failed', source=guard.manifest['source'], counts=counts,
            error_type=None if error is None else type(error).__name__,
            formal_controller_result=result, conservative_historical_plus_charge=
                guard.manifest['historical_charge']+sum(u['requests'] for u in guard.ledger.snapshot()['units'] if u['claimed'])))
        guard.save_metrics(cell_id)
    if not passed:
        guard._stop('cell_failed_or_unknown', error)
    guard.check(force=True)  # Include the final receipt/guard-metadata writes.


def run_cpu(guard, kind, cell_id=None):
    """Only existing zero-model entrypoints, under the same campaign deadline."""
    import workflow
    cell = (_cell(guard, cell_id) if cell_id and kind!='ray'
            else guard.manifest['cells'][0])
    validate_coverage(guard.manifest)
    temporary = {k:guard.manifest[k] for k in ('runtime_temp_root','ray_temp_root')}
    _require_covered(guard.manifest, list(temporary.values()))
    for path in temporary.values():
        Path(path).mkdir(mode=0o700, parents=True, exist_ok=True)
    base_path = Path(cell['base_config'])
    if hashlib.sha256(base_path.read_bytes()).hexdigest() != cell['base_sha256']:
        raise BudgetError('frozen private base changed')
    base = workflow.read_private(base_path)
    _configuration_coverage(guard.manifest, base)
    workflow.validate_base(base)
    driver = base['movie_plan']['engine']['driver_python']
    command = [driver, '-B']
    tool = str(HERE/'workflow.py')
    if kind == 'ray':
        command += ['-m','unittest','-v','tests.execution_provider.test_core_feedback_ray_future']
    elif kind == 'qualify':
        command += [tool,'qualify','--config',str(base_path),'--output-root',cell['ready_dir'],
                    '--tool-commit',guard.manifest['source']['head']]
    elif kind == 'prepare':
        command += [tool,'prepare','--config',str(base_path),'--ready',str(Path(cell['ready_dir'])/'CPU-READY.json'),
            '--output-root',str(Path(cell['prepared_config']).parent),'--cell-id',cell['id'],
            '--mode',cell['mode'],'--profile','handoff']
    elif kind == 'check':
        command += [tool,'check','--config',cell['prepared_config']]
    else:
        raise ValueError('use an existing Ray, qualify, prepare or check CPU entry')
    label = 'CPU-'+kind+'-'+cell['id']
    env = dict(os.environ)
    for key in tuple(os.environ):
        if key.startswith('SEM_RECORD_') or key in ('RAY_ADDRESS','SEMLOOM_MFU_TRACE_DIR','SEMLOOM_MFU_TRACE_MILESTONES'):
            os.environ.pop(key)
    os.environ.update(CUDA_VISIBLE_DEVICES='', SEMLOOM_REAL_RAY_CPU='1',
        TMPDIR=temporary['runtime_temp_root'], RAY_TMPDIR=temporary['ray_temp_root'],
        SEMLOOM_REAL_RAY_TMPDIR=temporary['ray_temp_root'],
        PYTHONPATH=str(REPO/'code')+os.pathsep+str(HERE),
        HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1', LITELLM_LOCAL_MODEL_COST_MAP='True',
        PYTHONDONTWRITEBYTECODE='1')
    try:
        guard.check(force=True)
        with guard.supervision():
            result = supervision.supervise(command, guard.root/label,
                timeout_s=min(base['limits']['worker_timeout_s'], guard.work_deadline-guard.clock()),
                grace_s=base['limits']['cleanup_grace_s'],
                natural_exit_grace_s=base['limits'].get('natural_exit_grace_s', 0))
        if kind=='ray':
            log = (guard.root/label/'worker.log').read_text()
            if (not result['output_capture']['complete']
                    or re.search(r'^Ran 5 tests in ', log, flags=re.MULTILINE) is None
                    or re.search(r'^OK\s*$',log,flags=re.MULTILINE) is None
                    or re.search(r'\(skipped=|^FAILED',log,flags=re.MULTILINE)):
                raise RuntimeError('five actual Ray tests without skips must be evidenced')
            write_private_json(guard.root/'REAL-RAY-READY.json', dict(status='passed',
                source=guard.manifest['source'], tests=5, skips=0,
                log_sha256=hashlib.sha256((guard.root/label/'worker.log').read_bytes()).hexdigest()))
        guard.check(force=True)
    except BaseException as error:
        guard._stop('CPU_qualification_failed', error)
    finally:
        os.environ.clear()
        os.environ.update(env)
        guard.save_metrics(label)
    guard.check(force=True)
    return result


def collect(guard):
    """Hash immutable per-cell originals with existing Git manifest tools."""
    driver = None
    validate_coverage(guard.manifest)
    for cell in guard.manifest['cells']:
        end = guard.root/(cell['id']+'-GUARD-END.json')
        if not end.exists() or json.loads(end.read_text())['status'] != 'passed':
            raise BudgetError('full successful collection requires all cell responsibility receipts')
        config = json.loads(Path(cell['prepared_config']).read_text())
        driver = config['movie_plan']['engine']['driver_python']
        manifest = guard.root/('ORIGINALS-'+cell['id']+'.json')
        for action in ('manifest','verify'):
            command = [driver,'-B',str(HERE/'workflow.py'),action,
                '--root',str(Path(cell['prepared_config']).parent)]
            command += ['--output' if action=='manifest' else '--manifest',str(manifest)]
            guard.check(force=True, collection=True)
            remaining = guard.deadline-guard.clock()
            with guard.supervision(collection=True):
                supervision.supervise(command,guard.root/('COLLECT-'+action+'-'+cell['id']),
                    timeout_s=remaining, grace_s=min(guard.manifest['collection_reserve_s']/3,remaining/3))
    guard.check(force=True, collection=True)
    guard.save_metrics('COLLECTION')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    init = commands.add_parser('init')
    init.add_argument('--root', required=True)
    init.add_argument('--manifest', required=True)
    run = commands.add_parser('run-cell')
    run.add_argument('--root', required=True)
    run.add_argument('--cell-id', required=True)
    run.add_argument('--execute-model', action='store_true', required=True)
    cpu = commands.add_parser('cpu')
    cpu.add_argument('--root', required=True)
    cpu.add_argument('--kind', choices=('ray','qualify','prepare','check'), required=True)
    cpu.add_argument('--cell-id')
    final = commands.add_parser('collect')
    final.add_argument('--root', required=True)
    args = parser.parse_args(argv)
    import workflow
    if args.command=='init':
        manifest = json.loads(Path(args.manifest).read_text())
        workflow.git_identity(REPO, manifest['source'])
        _require_covered(manifest, [manifest[k] for k in ('runtime_temp_root','ray_temp_root')])
        root = Path(args.root).resolve()
        if not any(root == Path(p).resolve() or Path(p).resolve() in root.parents
                   for p in manifest['artifact_roots']):
            raise ValueError('the guard ledger and receipts must be inside the storage bound')
        Guard.create(args.root, manifest)
        print(json.dumps(dict(status='campaign_initialized', model_calls=0)))
        return
    root = Path(args.root)
    with _exclusive(root):
        guard = Guard(root)
        workflow.git_identity(REPO, guard.manifest['source'])
        if args.command=='run-cell':
            run_cell(guard, args.cell_id)
        elif args.command=='cpu':
            run_cpu(guard,args.kind,args.cell_id)
        else:
            collect(guard)


if __name__=='__main__':
    main()
