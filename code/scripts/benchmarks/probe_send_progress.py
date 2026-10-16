#!/usr/bin/env python3
"""Bounded local SQLite/HTTP fixture; never starts an author runtime or model."""
import argparse
import ast
from contextlib import closing, contextmanager
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import multiprocessing as mp
import os
from pathlib import Path
import platform
import sqlite3
import sys
import threading
import time
import traceback
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.request import Request, urlopen

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))
from src.baselines.common.redact import redact_text

COUNTS = CODE/'src/experiments/send_counts.py'
LEDGER = Path(__file__).with_name('sembench_pre_send_ledger.py')
SQL = 'SELECT count(*),max(at_ns),max(returned_ns) FROM sends'
PLAN = dict(history_rows=[0, 6000, 114000, 228000], order=['before', 'after', 'after', 'before'],
            workers=4, logical_per_worker=64, poll_sleep_s=.1, response_delay_s=.12,
            model_requests=0, server_access=False, journal_mode='delete')


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def ledger(root, source, counters):
    """Load the real accounting functions; only HTTP hook ownership is a fixture."""
    names = {'_get_timing', '_emit_timing', '_timed_call', '_outside_call', '_ledger_connection',
             'initialize', 'reserve', 'finish', 'counts', 'assert_hooks'}
    functions = [node for node in ast.parse(Path(source).read_text()).body
                 if isinstance(node, ast.FunctionDef) and node.name in names]
    marker = SimpleNamespace(request=lambda: None)
    hooks = {'aiohttp': (marker, 'request', marker.request)}
    hooks.update({'send_counts_'+name: (counters, name, getattr(counters, name))
                  for name in ('snapshot', 'verify_increment', 'initialize_counts', 'check_identity', 'mark_initialized')})
    state = dict(minimal_sync_public_cli_software=dict(max_logical=10000000, max_posts=30000000),
                 matrix=dict(stages={p: [10000000, 30000000, 300] for p in counters.PHASES if p!='software'},
                             hard_logical_total=10000000, hard_transmission_total=30000000))
    values = dict(ROOT=Path(root), DB=Path(root)/'sends.sqlite', STATE=state, _send_counts=counters,
                  sqlite3=sqlite3, time=time, os=os, json=json, hashlib=hashlib,
                  contextmanager=contextmanager, _hooks=hooks, _installed=True)
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), 'exec'), values)
    return values


def controller(path, counters):
    node = next(n for n in ast.parse(Path(path).read_text()).body if isinstance(n, ast.FunctionDef) and n.name=='progress')
    values = dict(Path=Path, json=json, sqlite3=sqlite3, send_counts=counters)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), values)
    return values['progress']


class Recorder:
    def __init__(self):
        self.rows = []

    def record_interval(self, operation, started, ended, **fields):
        self.rows.append(dict(operation=operation, start_ns=started, end_ns=ended, **fields))


def initialize(root, source, counters):
    root.mkdir()
    (root/'phase.json').write_text(json.dumps(dict(phase='work-fixture', unit='paired')))
    (root/'model-start.json').write_text(json.dumps(dict(deadline_monotonic_ns=time.monotonic_ns()+300_000_000_000)))
    return ledger(root, source, counters)


def backup(source, target):
    with closing(sqlite3.connect(Path(source).as_uri()+'?mode=ro', uri=True)) as original:
        with closing(sqlite3.connect(target)) as copied:
            original.backup(copied)


def seed(root, source, counters, retained, number):
    ctx = initialize(root, source, counters)
    ctx['initialize']()
    # Replay into a new open fixture. The original closed caller gate is never reopened.
    with closing(sqlite3.connect(retained.as_uri()+'?mode=ro', uri=True)) as original:
        columns = [r[1] for r in original.execute('PRAGMA table_info(sends)') if r[1]!='sequence']
        rows = list(original.execute('SELECT '+','.join(columns)+' FROM sends ORDER BY sequence'))
    floor = time.monotonic_ns()-1_000_000_000-number*1000
    def replay():
        for index in range(number):
            row = list(rows[index%len(rows)])
            if row[columns.index('at_ns')] is not None:
                row[columns.index('at_ns')] = floor+index*1000
            if row[columns.index('returned_ns')] is not None:
                row[columns.index('returned_ns')] = floor+index*1000+100
            yield row
    with closing(sqlite3.connect(root/'sends.sqlite')) as db, db:
        db.executemany('INSERT INTO sends('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')', replay())
    return digest(root/'sends.sqlite')


def worker(root, source, counter_source, url, start, ready, output):
    recorder = Recorder()
    try:
        ctx = ledger(root, source, module(counter_source, 'fixture_worker_counts'))
        ctx['_get_timing'] = lambda: recorder
        ctx['initialize']()
        ready.put(True)
        if not start.wait(30):
            raise TimeoutError('fixture start barrier')
        logical = []
        for index in range(PLAN['logical_per_worker']):
            started = time.monotonic_ns()
            sequences = [ctx['reserve'](kind, 'http://127.0.0.1/fixture', b'{"fixture":true}')
                         for kind in ('gateway', 'router', 'model')]
            error, status = None, None
            if index%29==0:
                error = OSError('intentional fixture transport error')
            else:
                request = Request(url, data=json.dumps(dict(index=index)).encode(), method='POST')
                try:
                    response = urlopen(request, timeout=10)
                except HTTPError as response_error:
                    response = response_error
                with response:
                    status = response.status
                    if json.loads(response.read()) != dict(index=index):
                        raise RuntimeError('fixture response mismatch')
            for hop, sequence in reversed(list(enumerate(sequences))):
                if hop==0 and index%31==0:
                    continue  # Deliberately unfinished; its reservation must remain counted.
                ctx['finish'](sequence, response=SimpleNamespace(status=status), error=error)
            logical.append(dict(start_ns=started, end_ns=time.monotonic_ns(), status=status,
                                transport_error=error is not None, unfinished_gateway=index%31==0))
        output.put(dict(pid=os.getpid(), timing=recorder.rows, logical=logical))
    except BaseException:
        output.put(dict(error=redact_text(traceback.format_exc()), timing=recorder.rows))
        raise


class FixtureHTTP(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        self.lock, self.active, self.peak, self.posts = threading.Lock(), 0, 0, 0
        super().__init__(('127.0.0.1', 0), FixtureHandler)


class FixtureHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        value = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        with self.server.lock:
            self.server.posts += 1
            self.server.active += 1
            self.server.peak = max(self.server.peak, self.server.active)
        try:
            time.sleep(PLAN['response_delay_s'])
            body = json.dumps(value).encode()
            self.send_response(503 if value['index']%17==0 else 200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        finally:
            with self.server.lock:
                self.server.active -= 1

    def log_message(self, *_):
        pass


def cell(root, seed_path, source, counter_source, control_source):
    total_start = time.monotonic_ns()
    counters = module(counter_source, 'fixture_cell_counts')
    ctx = initialize(root, source, counters)
    backup(seed_path, root/'sends.sqlite')
    recorder = Recorder()
    ctx['_get_timing'] = lambda: recorder
    ctx['initialize']()
    with closing(sqlite3.connect(root/'sends.sqlite')) as db, db:
        db.execute('INSERT OR REPLACE INTO control VALUES(1,?,?,?,?,?,?,?)',
                   ('single', 'fixture', 'paired', time.time(), time.time()+300, 0, time.monotonic_ns()+300_000_000_000))
        initial = db.execute(SQL).fetchone()
        initial_incomplete = db.execute('SELECT count(*) FROM sends WHERE returned_ns IS NULL').fetchone()[0]
        initial_failed = db.execute("SELECT count(*) FROM sends WHERE error_type!='' OR status>=400").fetchone()[0]
    server = FixtureHTTP()
    thread = threading.Thread(target=server.serve_forever, kwargs=dict(poll_interval=.02))
    thread.start()
    processes = []
    context = mp.get_context('spawn')
    start, ready, output = context.Event(), context.Queue(), context.Queue()
    url = 'http://127.0.0.1:%d/fixture' % server.server_port
    progress = controller(control_source, counters)
    ownership = SimpleNamespace(observe=lambda: None, live=lambda: [])
    polls, results = [], []
    try:
        for _ in range(PLAN['workers']):
            child = context.Process(target=worker, args=(root, source, counter_source, url, start, ready, output))
            child.start()
            processes.append(child)
        for _ in processes:
            ready.get(timeout=30)
        started = time.monotonic_ns()
        start.set()
        while len(results)<len(processes):
            before, cpu = time.monotonic_ns(), time.thread_time_ns()
            value = progress(root, ownership)
            polls.append(dict(start_ns=before, end_ns=time.monotonic_ns(), cpu_ns=time.thread_time_ns()-cpu,
                              events=value[2]))
            while not output.empty():
                results.append(output.get(timeout=1))
            if time.monotonic_ns()-started>60_000_000_000:
                raise TimeoutError('local fixture safety cap; not a model policy')
            if len(results)<len(processes):
                time.sleep(PLAN['poll_sleep_s'])
        work_end = time.monotonic_ns()
        for child in processes:
            child.join(5)
            if child.exitcode!=0:
                raise RuntimeError('fixture worker failed')
        if any('error' in row for row in results):
            raise RuntimeError('fixture worker reported an error')
        with closing(sqlite3.connect(root/'sends.sqlite')) as db, db:
            db.execute('BEGIN IMMEDIATE')
            audited = counters.initialize_counts(db)  # Full offline history audit remains mandatory.
            final = db.execute(SQL).fetchone()
            incomplete = db.execute('SELECT count(*) FROM sends WHERE returned_ns IS NULL').fetchone()[0]
            failed = db.execute("SELECT count(*) FROM sends WHERE error_type!='' OR status>=400").fetchone()[0]
        if (final[0]!=initial[0]+768 or incomplete!=initial_incomplete+12 or failed!=initial_failed+68
                or server.posts!=244 or server.peak!=4):
            raise RuntimeError('fixture count/concurrency mismatch')
        return dict(initial=initial, final=final, initial_unfinished=initial_incomplete, initial_failed=initial_failed,
                    audited=audited, unfinished=incomplete, failed=failed,
                    http_posts=server.posts, peak_http=server.peak, work_wall_ns=work_end-started,
                    polls=polls, workers=results, initialization=recorder.rows,
                    total_before_cleanup_ns=time.monotonic_ns()-total_start)
    finally:
        start.set()
        for child in processes:
            if child.is_alive():
                child.terminate()
            child.join(5)
        server.shutdown()
        server.server_close()
        thread.join(5)


def closed_copy_check(root, retained, source, counter_source, control_source):
    counters = module(counter_source, 'closed_copy_counts')
    ctx = initialize(root, source, counters)
    backup(retained, root/'sends.sqlite')
    def raw_identity():
        with closing(sqlite3.connect(root/'sends.sqlite')) as db:
            rows = hashlib.sha256(json.dumps(list(db.execute('SELECT * FROM sends ORDER BY sequence'))).encode()).hexdigest()
            return rows, db.execute('SELECT closed FROM caller_gate WHERE id=1').fetchone(), db.execute(SQL).fetchone()
    original = raw_identity()
    recorder = Recorder()
    ctx['_get_timing'] = lambda: recorder
    ctx['initialize']()
    observed = controller(control_source, counters)(root, SimpleNamespace(observe=lambda: None, live=lambda: []))[2]
    with closing(sqlite3.connect(root/'sends.sqlite')) as db:
        statements = [SQL, 'SELECT coalesce(max(sequence),0) FROM sends']
        if counter_source==COUNTS:
            statements += ['SELECT max(at_ns) FROM sends INDEXED BY send_progress_at_ns',
                           'SELECT max(returned_ns) FROM sends INDEXED BY send_progress_returned_ns']
        plans = {sql: list(db.execute('EXPLAIN QUERY PLAN '+sql)) for sql in statements}
        pragmas = {name: db.execute('PRAGMA '+name).fetchone()[0] for name in ('journal_mode', 'synchronous', 'page_size')}
    if raw_identity()!=original or tuple(observed)!=original[2] or original[1]!=(1,):
        raise RuntimeError('closed ledger rows/gate/progress changed')
    return dict(rows_sha256=original[0], caller_gate_closed=1, progress=observed, plans=plans,
                pragmas=pragmas, initialization=recorder.rows)


def lock_check(root, source, counter_source):
    """Causal lock-release checks, separate from the unmodified polling workload."""
    counters = module(counter_source, 'lock_check_counts')
    ctx = initialize(root, source, counters)
    ctx['initialize']()
    with closing(sqlite3.connect(root/'sends.sqlite')) as db, db:
        db.execute('INSERT INTO control VALUES(1,?,?,?,?,?,?,?)',
                   ('software', 'fixture', 'lock', time.time(), time.time()+300, 0, time.monotonic_ns()+300_000_000_000))
    sequence = ctx['reserve']('gateway', 'http://127.0.0.1/fixture', b'{}')
    cases = []
    for operation in ('reserve', 'finish'):
        recorder = Recorder()
        ctx['_get_timing'] = lambda: recorder
        entered, done = threading.Event(), threading.Event()
        errors = []
        def call():
            entered.set()
            try:
                if operation=='reserve':
                    ctx['reserve']('gateway', 'http://127.0.0.1/fixture', b'{}')
                else:
                    ctx['finish'](sequence, response=SimpleNamespace(status=200))
            except BaseException as error:
                errors.append(type(error).__name__)
            finally:
                done.set()
        with closing(sqlite3.connect(root/'sends.sqlite')) as holder:
            holder.execute('BEGIN IMMEDIATE' if operation=='reserve' else 'BEGIN')
            holder.execute('SELECT * FROM sends').fetchall()
            held = time.monotonic_ns()
            thread = threading.Thread(target=call)
            thread.start()
            try:
                if not entered.wait(1):
                    raise TimeoutError('lock check thread did not start')
                completed_while_held = done.wait(.1)
            finally:
                release_start = time.monotonic_ns()
                holder.rollback()
                released = time.monotonic_ns()
                thread.join(12)
            if thread.is_alive() or errors or completed_while_held:
                raise RuntimeError('lock check did not block and settle as expected')
        stage = 'begin_immediate_call' if operation=='reserve' else 'context_exit_commit'
        interval = next(row for row in recorder.rows if row['operation']=='send_ledger.'+operation+'.'+stage)
        overlap = max(0, min(interval['end_ns'], release_start)-max(interval['start_ns'], held))
        if overlap<50_000_000:
            raise RuntimeError('lock-sensitive SQLite call did not overlap the held lock')
        cases.append(dict(operation=operation, holder='RESERVED writer' if operation=='reserve' else 'SHARED reader',
                          held_start_ns=held, release_start_ns=release_start, released_ns=released,
                          call_overlap_with_hold_ns=overlap, timing=recorder.rows,
                          interpretation='controlled blocking overlap; not natural-workload pure busy wait'))
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--checks-only', action='store_true')
    parser.add_argument('--attribution', action='store_true', help='228k symmetric before/index_scan/after control')
    for name in ('ledger-copy', 'baseline-ledger', 'baseline-counts', 'controller-before', 'controller-after', 'output'):
        parser.add_argument('--'+name, type=Path)
    args = parser.parse_args()
    run_plan = dict(PLAN)
    if args.attribution:
        run_plan.update(history_rows=[228000], order=['before', 'index_scan', 'after', 'after', 'index_scan', 'before'])
    if not args.execute:
        print(json.dumps(run_plan, indent=2))
        return
    if any(value is None for key, value in vars(args).items() if key not in ('execute', 'checks_only', 'attribution')):
        parser.error('execute requires all input paths and a new output directory')
    args.output.mkdir()
    if not args.ledger_copy.is_absolute():
        parser.error('ledger-copy must be an absolute local path')
    inputs = {key: digest(path) for key, path in vars(args).items() if key not in ('execute', 'checks_only', 'attribution', 'output')}
    metadata = dict(plan=run_plan, inputs_sha256=inputs, python=sys.version, sqlite=sqlite3.sqlite_version,
                    base_commit='7e59e5a10eaceb7ac4a1bd0766b10244ca3e192d',
                    after_sources_sha256=dict(ledger=digest(LEDGER), counters=digest(COUNTS)),
                    probe_sha256=digest(__file__), checks_only=args.checks_only,
                    arms=dict(before='original writes and original scan', index_scan='indexed writes and original scan',
                              after='indexed writes and indexed progress'),
                    os=platform.system(), machine=platform.machine(), cpu_count=os.cpu_count(),
                    caller_ownership='empty fixture; OS/GPU ownership work is not modeled',
                    http_shape='one localhost POST surrounded by three nested ledger reserves/finishes',
                    pure_lock_wait='unavailable in Python sqlite3; begin/update/commit intervals include locks and SQL/I/O')
    (args.output/'metadata.json').write_text(json.dumps(metadata, indent=2)+'\n')
    before = module(args.baseline_counts, 'fixture_seed_counts')
    runs = []
    try:
        checks = {}
        for arm in ('before', 'after'):
            source = args.baseline_ledger if arm=='before' else LEDGER
            counter_source = args.baseline_counts if arm=='before' else COUNTS
            control_source = args.controller_before if arm=='before' else args.controller_after
            checks[arm] = dict(closed_copy=closed_copy_check(args.output/('closed-'+arm), args.ledger_copy,
                                                          source, counter_source, control_source),
                               locks=lock_check(args.output/('locks-'+arm), source, counter_source))
        (args.output/'checks.json').write_text(json.dumps(checks, indent=2)+'\n')
        if args.checks_only:
            return
        for size in run_plan['history_rows']:
            began = time.monotonic_ns()
            seed_root = args.output/f'seed-{size}'
            seed_hash = seed(seed_root, args.baseline_ledger, before, args.ledger_copy, size)
            (seed_root/'replay.json').write_text(json.dumps(dict(rows=size, sha256=seed_hash,
                preparation_ns=time.monotonic_ns()-began, clocks='rebased only in fresh replay, original ledger unchanged'))+'\n')
            for index, arm in enumerate(run_plan['order']):
                root = args.output/f'{size}-{index}-{arm}'
                began = time.monotonic_ns()
                source = args.baseline_ledger if arm=='before' else LEDGER
                counter_source = args.baseline_counts if arm=='before' else COUNTS
                control_source = args.controller_after if arm=='after' else args.controller_before
                result = cell(root, seed_root/'sends.sqlite', source, counter_source, control_source)
                result.update(history_rows=size, order=index, arm=arm, total_fixture_ns=time.monotonic_ns()-began)
                with gzip.open(root/'samples.json.gz', 'wt') as stream:
                    json.dump(result, stream, separators=(',', ':'))
                runs.append(dict(history_rows=size, order=index, arm=arm,
                                 artifact=root.name+'/samples.json.gz', sha256=digest(root/'samples.json.gz')))
                print(json.dumps(dict(history_rows=size, order=index, arm=arm, work_s=result['work_wall_ns']/1e9,
                                      total_s=result['total_fixture_ns']/1e9, polls=len(result['polls']))), flush=True)
    except BaseException:
        (args.output/'failure.txt').write_text(redact_text(traceback.format_exc()))
        raise
    finally:
        (args.output/'runs.json').write_text(json.dumps(runs, indent=2)+'\n')
    if digest(args.ledger_copy)!=inputs['ledger_copy']:
        raise RuntimeError('input ledger changed')


if __name__ == '__main__':
    main()
