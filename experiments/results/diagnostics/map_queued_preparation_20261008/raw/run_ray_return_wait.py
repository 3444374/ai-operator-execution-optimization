"""Finite real Ray/Daft return-path diagnosis; the actor service is a sleep fixture."""
import argparse
import asyncio
from dataclasses import asdict
import gzip
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import signal
import sqlite3
import statistics
import subprocess
import threading
import time

from src.baselines.common.private_artifacts import new_private_directory
from src.baselines.common.redact import redact_json_values
from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport, _RemoteResult, _clock_domain
from src.experiments.async_request_guard import ThreadedRequestGuard
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.choice_gateway_observer import _guard_remote_request
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.session_contract import OfferedTask, SessionSpec, State, TaskInfo
from src.scheduling.runtime.stage_broker import StageBrokerLimits


ROWS, CAPACITY, HELD, BATCH, MAX_CALLS = 1024, 64, 128, 16, 16448
ARMS = (('immediate', 'sync'), ('coalesced', 'sync'),
        ('immediate', 'threaded'), ('coalesced', 'threaded'))
WORK = WorkDescriptor((StageWork('model', 1, 'work_units'),), 'model', 'ray-sleep-fixture')


def payload(sequence):
    return f'fixture-input:{sequence}:'.encode() + b'x' * 128


def response(sequence):
    return f'fixture-output:{sequence}'.encode()


class Capture:
    def __init__(self, rows):
        self.maximum = 64 * rows + 1024
        self.events, self.lock = [], threading.Lock()

    def __call__(self, event):
        value = dict(event, monotonic_ns=time.monotonic_ns())
        with self.lock:
            if len(self.events) >= self.maximum:
                raise RuntimeError('finite event capacity exceeded')
            self.events.append(value)


class SleepActor:
    """Independent Ray process with complete input/identity checks; no client or HTTP."""
    def __init__(self, config, capacity):
        if config.model_id != 'fixture':
            raise ValueError('fixture model identity differs')
        self.capacity, self.active, self.peak = capacity, 0, 0
        self.calls, self.seen, self.closed = [], set(), False
        self.clock_domain = _clock_domain()

    async def ready(self):
        return True

    async def execute(self, table, index, template):
        start = time.monotonic_ns()
        key = template.key
        if self.closed or self.active >= self.capacity or key.sequence in self.seen:
            raise AssertionError('actor capacity or exactly-once input differs')
        if (table['session_id'][index].as_py(), table['sequence'][index].as_py()) != (key.session_id, key.sequence):
            raise AssertionError('actor row identity differs')
        body = table['payload'][index].as_py()
        if body != payload(key.sequence) or template.task.payload != b'':
            raise AssertionError('actor full input differs')
        self.seen.add(key.sequence)
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(.020 + key.sequence % 7 * .001)
            ended = time.monotonic_ns()
            self.calls.append(dict(sequence=key.sequence, started_ns=start, ended_ns=ended,
                                   request_sha256=hashlib.sha256(body).hexdigest()))
            return _RemoteResult(key, response(key.sequence), start, ended, self.clock_domain)
        finally:
            self.active -= 1

    async def snapshot(self):
        return dict(pid=os.getpid(), active=self.active, peak=self.peak, calls=self.calls,
                    clock_domain=self.clock_domain)

    async def close(self):
        if self.active:
            raise AssertionError('actor still active')
        self.closed = True
        return True


class ExecuteProxy:
    def __init__(self, actor, charged, issued, ready):
        self.actor, self.charged, self.issued, self.ready = actor, charged, issued, ready

    def remote(self, *args):
        sequence = args[2].key.sequence
        if sequence not in self.charged or sequence in self.issued:
            raise AssertionError('RPC preceded accounting or duplicated input')
        self.issued.append(sequence)
        reference = self.actor.execute.remote(*args)
        # Ray 2.56.1 ObjectRef.__await__ uses this same future/wrap_future path.
        future = reference.future()
        future.add_done_callback(lambda _: self.ready.append((sequence, time.monotonic_ns())))
        return asyncio.wrap_future(future)


class ActorProxy:
    def __init__(self, actor, charged, issued, ready):
        self.actor = actor
        self.execute = ExecuteProxy(actor, charged, issued, ready)

    def __getattr__(self, name):
        return getattr(self.actor, name)


class RealRayProbe:
    """Real vendor API; substitute only the service actor and observe the await path."""
    def __init__(self, ray, charged):
        self.ray, self.charged = ray, charged
        self.actors, self.issued, self.ready, self.killed = [], [], [], []

    def __getattr__(self, name):
        return getattr(self.ray, name)

    def remote(self, **options):
        if options != dict(num_cpus=1, max_restarts=0, max_task_retries=0, max_concurrency=CAPACITY):
            raise AssertionError('declared actor resources differ')
        probe = self
        class Factory:
            def remote(self, *args):
                actor = probe.ray.remote(**options)(SleepActor).remote(*args)
                proxy = ActorProxy(actor, probe.charged, probe.issued, probe.ready)
                probe.actors.append(proxy)
                return proxy
        return lambda _: Factory()

    def kill(self, actor, **kwargs):
        if actor not in self.actors or actor in self.killed:
            raise AssertionError('unknown or already disposed actor')
        self.ray.kill(actor.actor, **kwargs)
        self.killed.append(actor)


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        raise AssertionError('missing timing samples')
    return dict(count=len(ordered), mean_ms=statistics.mean(ordered)/1e6,
                median_ms=statistics.median(ordered)/1e6,
                p95_ms=ordered[math.ceil(.95*len(ordered))-1]/1e6,
                sum_seconds=sum(ordered)/1e9)


def analyze(events, ready, worker, rows):
    receipts = [event for event in events if event['event'] == 'ray_http_completed']
    available = dict(ready)
    if (len(receipts) != rows or len(available) != len(ready) or len(ready) != rows
            or set(available) != set(range(rows))
            or {event['key']['sequence'] for event in receipts} != set(range(rows))):
        raise AssertionError('future or receipt population differs')
    first, second, total = [], [], []
    for event in receipts:
        at = available[event['key']['sequence']]
        if not event['shared_clock'] or not event['worker_ended_ns'] <= at <= event['received_ns']:
            raise AssertionError('shared clock or callback chronology differs')
        first.append(at-event['worker_ended_ns'])
        second.append(event['received_ns']-at)
        total.append(event['after_worker_ns'])
    if sum(first)+sum(second) != sum(total):
        raise AssertionError('return segment totals differ')
    guard_events = [event for event in events if event['event'] == 'remote_request_guard']
    guards = [event['elapsed_ns'] for event in guard_events]
    if len(guards) != rows or worker['active'] or worker['peak'] > CAPACITY:
        raise AssertionError('guard population or actor capacity differs')
    if {event['key']['sequence'] for event in guard_events} != set(range(rows)):
        raise AssertionError('guard row association differs')
    prepared = [e for e in events if e['event'] == 'ray_preparation_completed']
    if any(e['failed'] for e in prepared):
        raise AssertionError('input preparation failed')
    return dict(worker_end_to_callback=distribution(first), callback_to_receive=distribution(second),
                worker_end_to_receive=distribution(total), guard=distribution(guards),
                worker_active_peak=worker['peak'], prepared_blocks=len(prepared),
                object_batches=sum(e['event']=='ray_block_put' for e in events),
                object_peak_bytes=max(e.get('object_bytes', 0) for e in events))


def run_case(output, ray, address, path, guard_kind, rows, deadline):
    new_private_directory(output)
    capture = Capture(rows)
    ledger = CellBudgetLedger.create(output/'ledger.sqlite', AttemptBudget('ray-sleep-fixture', rows),
                                    deadline_utc=deadline)
    ledger.reserve_unit('case', rows)
    unit = ledger.claim_shared_unit('case')
    charged = {}
    expected = {hashlib.sha256(payload(i)).hexdigest(): i for i in range(rows)}
    def observe_request(attempt, body):
        sequence = expected[hashlib.sha256(body).hexdigest()]
        if sequence in charged or attempt != len(charged)+1:
            raise AssertionError('accounted input or attempt order differs')
        charged[sequence] = attempt
        capture(dict(event='fixture_accounted', sequence=sequence, attempt=attempt))
    guard = (ThreadedRequestGuard(unit, observe_request, capture) if guard_kind=='threaded'
             else lambda task: _guard_remote_request(task, unit, observe_request, capture))
    vendor = RealRayProbe(ray, charged)
    physical = RayMapConfig(address, 1, BATCH, 2**21, 2**22, payload_backend='daft',
        preparation=StageBrokerLimits(2**21, 2**22, HELD, 1, HELD) if path=='coalesced' else None,
        coalesce_queued_preparation=path=='coalesced')
    config = FixedModelConfig('http://localhost/unused-fixture', 'fixture', 60000)
    transport, execution, job, session, failure = None, None, None, None, None
    def factory(config, maximum, observer):
        nonlocal transport
        transport = RayMapTransport(config, maximum, observer, physical=physical,
                                    before_request=guard, ray_api=vendor)
        return transport
    start = time.monotonic_ns()
    report = dict(status='running', path=path, guard_kind=guard_kind, rows=rows,
                  physical=asdict(physical), http_requests=0, model_requests=0, pg_queries=0)
    try:
        execution = build_fixed_model_execution(config, observer=capture, max_tasks=HELD,
            max_active_requests=CAPACITY, transport_factory=factory,
            preparation_factory=(lambda t, limits, notify: t.prepare_inputs(limits, notify))
                                if path=='coalesced' else None)
        job, session, _ = execution.open_job('fixture', SessionSpec('fixture', 'map', 'fixture'))
        began = time.monotonic_ns()
        offered, outputs, pending, first_result, sealed = 0, [], {}, None, False
        while True:
            if time.time() >= deadline or time.monotonic_ns()-start >= 60*10**9:
                raise TimeoutError('finite case or suite deadline reached')
            usage = execution.engine.capacity.usage()
            if usage.active_requests > CAPACITY or usage.active_work > CAPACITY or usage.held_tasks > HELD:
                raise AssertionError('Core resource capacity exceeded')
            available = HELD-usage.held_tasks
            if offered < rows and available:
                tasks = [OfferedTask(i, payload(i), 1, 64, info=TaskInfo('fixture', i, 'model', WORK))
                         for i in range(offered, min(rows, offered+available))]
                intake = session.offer(tasks)
                if intake.status == 'REJECTED':
                    raise AssertionError('Core rejected fixture input')
                offered += intake.accepted_prefix_count
            if offered == rows and not sealed:
                session.seal()
                sealed = True
            progress = execution.engine.advance()
            result = session.advance(HELD)
            if result.error or progress.error or result.state in (State.CANCELLED, State.FAILED):
                raise RuntimeError('Core fixture execution failed')
            for delivery in result.deliveries:
                sequence = delivery.key.sequence
                if sequence in pending or sequence < len(outputs) or delivery.result != response(sequence):
                    raise AssertionError('consumer identity or full output differs')
                pending[sequence] = delivery.result
                if first_result is None:
                    first_result = (time.monotonic_ns()-began)/1e9
            while len(outputs) in pending:
                outputs.append(pending.pop(len(outputs)))
            if result.deliveries:
                session.release([d.lease_id for d in result.deliveries])
            if result.state == State.FINISHED:
                break
            if not result.deliveries and not progress.has_immediate_work and not result.has_immediate_work:
                execution.engine.wake.wait(result.generation, .01)
        ended = time.monotonic_ns()
        worker = ray.get(vendor.actors[0].snapshot.remote(), timeout=5)
        if outputs != [response(i) for i in range(rows)] or pending:
            raise AssertionError('ordered output history differs')
        if sorted(vendor.issued) != list(range(rows)) or sorted(c['sequence'] for c in worker['calls']) != list(range(rows)):
            raise AssertionError('actor calls were not exactly once')
        if unit.attempts != rows or unit.remaining or len(charged) != rows:
            raise AssertionError('finite accounting differs')
        if any(vars(execution.engine.capacity.usage()).values()):
            raise AssertionError('Core resources remain held')
        with ledger._transaction() as connection:
            saved = connection.execute('SELECT sequence,request_sha256 FROM shared_requests ORDER BY sequence').fetchall()
        if saved != [(attempt, hashlib.sha256(payload(seq)).hexdigest()) for seq, attempt in sorted(charged.items(), key=lambda x:x[1])]:
            raise AssertionError('durable request digest history differs')
        if session.close().status != 'CLOSED':
            raise AssertionError('session did not close')
        execution.engine.close_job(job)
        report.update(analyze(capture.events, vendor.ready, worker, rows), status='passed',
            query_seconds=(ended-began)/1e9, first_result_seconds=first_result,
            startup_seconds=(began-start)/1e9, query_started_ns=began, query_completed_ns=ended,
            actor_calls=worker['calls'], future_ready=[dict(sequence=s, future_ready_ns=at) for s, at in vendor.ready],
            actor_pid=worker['pid'], clock_domain=worker['clock_domain'], exactly_once=True,
            result_digest_by_sequence={i:hashlib.sha256(value).hexdigest() for i, value in enumerate(outputs)},
            request_digest_by_sequence={i:hashlib.sha256(payload(i)).hexdigest() for i in range(rows)})
    except BaseException as error:
        failure = error
        report.update(status='failed', error_type=type(error).__name__, error_message=str(error))
    finally:
        cleanup_errors = []
        if execution is not None:
            try:
                if failure and job is not None:
                    execution.engine.close_job(job)
                    session.close_consumer()
                if not execution.close(10):
                    raise RuntimeError('backend did not close')
            except BaseException as error:
                cleanup_errors.append(dict(resource='backend', error_type=type(error).__name__))
                failure = failure or error
        if isinstance(guard, ThreadedRequestGuard):
            guard.__exit__(None, None, None)
        try:
            report['accounted_calls'] = unit.attempts
            ledger.close_shared_unit('case')
        except BaseException as error:
            cleanup_errors.append(dict(resource='accounting', error_type=type(error).__name__))
            failure = failure or error
        report.update(issued_calls=len(vendor.issued), cleanup_errors=cleanup_errors,
            case_seconds=(time.monotonic_ns()-start)/1e9,
            actor_disposed=len(vendor.killed)==len(vendor.actors),
            transport_drained=transport is not None and not (transport.rows or transport.blocks or transport.unknown or transport.used_bytes))
        if not report['actor_disposed'] or not report['transport_drained']:
            failure = failure or AssertionError('final transport resources remain held')
        if failure:
            report['status'] = 'failed'
        with gzip.open(output/'events.jsonl.gz', 'xt') as stream:
            for event in capture.events:
                stream.write(json.dumps(redact_json_values(event), separators=(',', ':'))+'\n')
        (output/'result.json').write_text(json.dumps(redact_json_values(report), indent=2)+'\n')
    if failure:
        raise RuntimeError('real Ray fixture failed; evidence retained') from None
    return report


def main(output, ray_temp):
    import ray
    new_private_directory(output)
    deadline = time.time()+600
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError('finite suite deadline reached')))
    signal.alarm(600)
    begin = time.monotonic()
    report = dict(status='running', actor_call_limit=MAX_CALLS, seconds_limit=600,
        http_requests=0, model_requests=0, pg_queries=0, gpu_requested=0,
        python=platform.python_version(), sqlite=sqlite3.sqlite_version,
        versions={name:importlib.metadata.version(name) for name in ('ray', 'daft', 'pyarrow')},
        ray_commit=ray.__commit__, reference_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
        probe_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), runs=[])
    names = ('code/src/execution_provider/adapters/ray_map_transport.py',
             'code/src/execution_provider/adapters/map_preparation.py',
             'code/src/execution_provider/adapters/incremental_execution.py',
             'code/src/scheduling/core/session.py', 'code/src/scheduling/runtime/async_backend.py',
             'code/src/experiments/choice_gateway_observer.py', 'code/src/experiments/async_request_guard.py',
             'code/src/experiments/cell_budget.py', 'code/src/experiments/shared_request_budget.py')
    report['source_sha256'] = {name:hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in names}
    try:
        if ray.is_initialized():
            raise AssertionError('probe requires its own Ray cluster')
        ray.init(num_cpus=8, num_gpus=0, object_store_memory=256*1024*1024,
                 _temp_dir=str(ray_temp), include_dashboard=False,
                 runtime_env={'env_vars':{'PYTHONPATH':os.environ['PYTHONPATH']}})
        address = ray.get_runtime_context().gcs_address
        report['cluster_startup_seconds'] = time.monotonic()-begin
        for phase, repeat, rows in [('check', 0, 16), ('warmup', 0, ROWS),
                                    ('measurement', 1, ROWS), ('measurement', 2, ROWS), ('measurement', 3, ROWS)]:
            arms = ARMS[repeat:] + ARMS[:repeat]
            for path, guard in arms:
                if time.time() >= deadline-60:
                    raise TimeoutError('remaining suite time cannot cover one case')
                name = f'{phase}-{repeat}-{path}-{guard}'
                report['active_run'] = name
                (output/'progress.json').write_text(json.dumps(redact_json_values(report), indent=2)+'\n')
                result = run_case(output/name, ray, address, path, guard, rows, deadline)
                report['runs'].append(dict(run_id=name, phase=phase, repeat=repeat, **result))
                print(json.dumps(dict(case=name, status='passed', query_seconds=result['query_seconds'],
                    worker_end_to_callback_ms=result['worker_end_to_callback']['mean_ms'],
                    callback_to_receive_ms=result['callback_to_receive']['mean_ms'])), flush=True)
        if sum(r['issued_calls'] for r in report['runs']) != MAX_CALLS:
            raise AssertionError('suite call count differs')
        report['status'] = 'passed'
    except BaseException as error:
        report.update(status='failed', error_type=type(error).__name__, error_message=str(error))
        failed = output/report.get('active_run', 'not-started')/'result.json'
        if failed.is_file():
            report['failed_run'] = json.loads(failed.read_text())
        raise
    finally:
        ray.shutdown()
        signal.alarm(0)
        report.update(suite_seconds=time.monotonic()-begin, cluster_connection_closed=not ray.is_initialized())
        report['issued_calls'] = sum(r['issued_calls'] for r in report['runs']) + report.get('failed_run', {}).get('issued_calls', 0)
        (output/'summary.json').write_text(json.dumps(redact_json_values(report), indent=2)+'\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--ray-temp', type=Path, required=True)
    args = parser.parse_args()
    main(args.output, args.ray_temp)
