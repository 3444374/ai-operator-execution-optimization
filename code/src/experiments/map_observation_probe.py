"""Finite Core/transport diagnosis with synthetic tables and independent workers.

No Ray, Daft, Arrow, database, HTTP or model service is invoked. The real shared
engine, coroutine backend, Map transport, guards and optional event writer run.
"""

import argparse
import asyncio
from dataclasses import asdict
import gzip
import hashlib
import json
import math
from pathlib import Path
import platform
import sqlite3
import statistics
import subprocess
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

from src.baselines.common.private_artifacts import new_private_directory, write_private_json
from src.baselines.common.redact import redact_json_values
from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport, _RemoteResult
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.session_contract import SessionSpec, State
from src.scheduling.core.session_contract import OfferedTask, TaskInfo
from .attempt_ledger import AttemptBudget
from .buffered_events import BufferedEvents, compact_event
from .cell_budget import CellBudgetLedger
from .choice_gateway_observer import _guard_remote_request


ROWS, CAPACITY, BATCH_ROWS = 1024, 64, 16
ARMS = (('durable', 'memory'), ('memory', 'memory'),
        ('durable', 'buffered'), ('memory', 'buffered'))
CLOCK = 'same-process-synthetic-worker'
PREPARE_S, PUT_S, SERVICE_S = .0005, .0002, .020
WORK = WorkDescriptor((StageWork('model', 1, 'work_units'),), 'model', 'synthetic-request-count')


def payload(sequence):
    return f'fixture-input:{sequence}:'.encode() + b'x' * 128


def response(sequence):
    return f'fixture-output:{sequence}'.encode()


class EventCapture:
    """The same bounded raw history in both arms, with optional real writer work."""

    def __init__(self, maximum, writer=None):
        self.maximum, self.writer = maximum, writer
        self.events, self.projection_ns, self.writer_call_ns = [], 0, 0
        self.lock = threading.Lock()

    def __call__(self, fields):
        event = dict(fields, monotonic_ns=time.monotonic_ns())
        projection = written = 0
        if self.writer:
            before = time.monotonic_ns()
            projected = compact_event(event)
            after = time.monotonic_ns()
            self.writer.record(projected)
            projection, written = after - before, time.monotonic_ns() - after
        with self.lock:
            if len(self.events) >= self.maximum:
                raise RuntimeError('finite raw event capacity exhausted')
            self.events.append(event)
            self.projection_ns += projection
            self.writer_call_ns += written


class SyntheticTable:
    """Only the finite scalar/table interface used by the existing transport."""

    def __init__(self, rows):
        self.rows, self.num_rows = tuple(rows), len(rows)

    def __getitem__(self, name):
        column = ('session_id', 'sequence', 'payload').index(name)
        return [SimpleNamespace(as_py=lambda value=row[column]: value) for row in self.rows]

    def get_total_buffer_size(self):
        return 8 + sum(24 + len(row[2]) for row in self.rows)


class SyntheticRay:
    """Vendor calls are substitutes; worker progress has its own event loop."""

    def __init__(self, charged):
        self.charged = charged
        self.loop = asyncio.new_event_loop()
        self.ready = threading.Event()
        self.active = self.peak = 0
        self.issued, self.completed = [], []
        self.killed = False
        self.thread = threading.Thread(target=self._run, name='fixture-worker', daemon=True)
        self.thread.start()
        if not self.ready.wait(5):
            raise RuntimeError('synthetic worker did not start')
        self.actor = SimpleNamespace(ready=SimpleNamespace(remote=lambda: True),
                                     execute=SimpleNamespace(remote=self._submit),
                                     close=SimpleNamespace(remote=self._actor_close))

    def _run(self):
        asyncio.set_event_loop(self.loop)
        self.ready.set()
        self.loop.run_forever()
        self.loop.close()

    async def _execute(self, table, index, template):
        started = time.monotonic_ns()
        key = template.key
        if (table.rows[index][0], table.rows[index][1]) != (key.session_id, key.sequence):
            raise AssertionError('synthetic table identity differs')
        if table.rows[index][2] != payload(key.sequence):
            raise AssertionError('synthetic complete input differs')
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(SERVICE_S + (key.sequence % 7) * .001)
            ended = time.monotonic_ns()
            self.completed.append(key.sequence)
            return _RemoteResult(key, response(key.sequence), started, ended, CLOCK)
        finally:
            self.active -= 1

    def _submit(self, table, index, template):
        sequence = template.key.sequence
        if sequence not in self.charged:
            raise AssertionError('synthetic call preceded its accounting')
        self.issued.append(sequence)
        future = asyncio.run_coroutine_threadsafe(self._execute(table, index, template), self.loop)
        return asyncio.wrap_future(future)

    async def _actor_close(self):
        if self.active:
            raise RuntimeError('synthetic worker still active')
        return True

    def is_initialized(self):
        return True

    def get_runtime_context(self):
        return SimpleNamespace(gcs_address='fixture-cluster')

    def remote(self, **_):
        return lambda _: SimpleNamespace(remote=lambda *_: self.actor)

    def get(self, value, **_):
        return value

    def put(self, table):
        time.sleep(PUT_S)
        return table

    def kill(self, actor, **_):
        if actor is not self.actor:
            raise AssertionError('unknown synthetic actor')
        self.killed = True

    def close(self):
        if self.active:
            raise RuntimeError('cannot stop active synthetic worker')
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        if self.thread.is_alive():
            raise RuntimeError('synthetic worker did not stop')


def _distribution(values):
    ordered = sorted(values)
    if not ordered:
        raise AssertionError('missing timing population')
    return dict(count=len(ordered), median_ms=statistics.median(ordered) / 1e6,
                p95_ms=ordered[math.ceil(.95 * len(ordered)) - 1] / 1e6,
                sum_seconds=sum(ordered) / 1e9)


def analyze(events, rows, began, ended, worker_peak):
    events = sorted(events, key=lambda e: e['monotonic_ns'])
    keyed = lambda kind: {e['key']['sequence']: e for e in events if e['event'] == kind}
    submitted, terminal = keyed('submitted'), keyed('terminal')
    rpc, guard = keyed('ray_http_completed'), keyed('remote_request_guard')
    if any(set(records) != set(range(rows)) for records in (submitted, terminal, rpc, guard)):
        raise AssertionError('row event history incomplete')
    if not all(e['shared_clock'] for e in rpc.values()):
        raise AssertionError('synthetic same-process timing differs')
    elapsed = ended - began
    last, current, area, peak = began, 0, 0, 0
    for event in events:
        if 'usage' not in event or not began <= event['monotonic_ns'] <= ended:
            continue
        area += current * (event['monotonic_ns'] - last)
        last, current = event['monotonic_ns'], event['usage']['active_requests']
        peak = max(peak, current)
    area += current * (ended - last)
    if current or not 0 < peak <= CAPACITY or worker_peak > peak:
        raise AssertionError('logical or worker capacity differs')
    stages = {}
    for stage in ('payload_next', 'object_put', 'payload_close'):
        stages[stage] = {field: sum(e[field] for e in events
                            if e['event'] == 'ray_work' and e['stage'] == stage) / 1e9
                         for field in ('work_ns', 'queue_ns', 'resume_ns')}
    windows = [e['rows'] for e in events if e['event'] == 'fixture_window']
    before = [rpc[i]['rpc_started_ns'] - submitted[i]['monotonic_ns'] for i in range(rows)]
    return dict(query_seconds=elapsed / 1e9, preparation_windows=len(windows), window_rows=windows,
                object_batches=sum(e['event'] == 'ray_block_put' for e in events),
                object_peak_bytes=max(e.get('object_bytes', 0) for e in events),
                core_active_mean=area / elapsed, core_active_peak=peak,
                synthetic_worker_active_mean=sum(e['worker_elapsed_ns'] for e in rpc.values()) / elapsed,
                synthetic_worker_peak=worker_peak, stages_seconds=stages,
                guard_seconds={field: sum(e[field] for e in guard.values()) / 1e9
                               for field in ('hash_ns', 'reserve_ns', 'request_observe_ns', 'elapsed_ns')},
                submitted_to_rpc=_distribution(before),
                submitted_event_after_rpc_rows=sum(value < 0 for value in before),
                rpc_submit=_distribution([e['submit_elapsed_ns'] for e in rpc.values()]),
                rpc_to_worker=_distribution([e['before_worker_ns'] for e in rpc.values()]),
                synthetic_worker=_distribution([e['worker_elapsed_ns'] for e in rpc.values()]),
                worker_to_receive=_distribution([e['after_worker_ns'] for e in rpc.values()]),
                receive_to_terminal=_distribution([terminal[i]['monotonic_ns'] - rpc[i]['received_ns']
                                                  for i in range(rows)]))


def run_case(output, accounting, recording, *, rows=ROWS, capacity=CAPACITY, batch_rows=BATCH_ROWS):
    if (accounting, recording) not in ARMS or not 0 < rows <= ROWS or not 0 < batch_rows <= capacity <= CAPACITY:
        raise ValueError('unsupported finite diagnostic configuration')
    output = Path(output)
    new_private_directory(output)
    writer = BufferedEvents(output / 'buffered.jsonl', max_events=16384) if recording == 'buffered' else None
    capture = EventCapture(12 * rows + 1000, writer)
    report = dict(accounting=accounting, recording=recording, rows=rows, capacity=capacity,
                  batch_rows=batch_rows, status='running', http_requests=0, model_requests=0, pg_queries=0)
    write_private_json(output / 'manifest.json', report)
    budget = AttemptBudget('fixture.observation', rows)
    ledger = CellBudgetLedger.create(output / 'ledger.sqlite', budget, deadline_utc=time.time() + 60)
    ledger.reserve_unit('case', rows)
    charged_unit = ledger.claim_shared_unit('case') if accounting == 'durable' else ledger.claim_unit('case')
    expected = {payload(i): i for i in range(rows)}
    charged = {}

    def observe_request(attempt, body):
        sequence = expected[body]
        if sequence in charged or attempt != len(charged) + 1:
            raise AssertionError('accounting identity or sequence differs')
        charged[sequence] = attempt
        capture(dict(event='fixture_request_observed', key=dict(session_id=0, sequence=sequence), attempt=attempt))

    def batches(data, limits, *, batch_rows, backend):
        if backend != 'arrow' or len(data) > limits.rows or sum(len(row[2]) + 24 for row in data) > limits.bytes:
            raise AssertionError('synthetic preparation exceeds declared limits')
        capture(dict(event='fixture_window', rows=len(data)))
        for offset in range(0, len(data), batch_rows):
            time.sleep(PREPARE_S)
            yield SyntheticTable(data[offset:offset + batch_rows])

    ray = SyntheticRay(charged)
    transport = None
    execution = session = job = None
    failure = None
    teardown = time.monotonic_ns()
    case_started = teardown

    def factory(config, maximum, observer):
        nonlocal transport
        transport = RayMapTransport(config, maximum, observer, ray_api=ray,
            physical=RayMapConfig('fixture-cluster', 1, batch_rows, 65536, 131072, payload_backend='arrow'),
            before_request=lambda task: _guard_remote_request(task, charged_unit, observe_request, capture))
        return transport

    # The table replacement is explicit; selecting its adapter name does not run Arrow.
    with patch('src.execution_provider.adapters.ray_map_transport.iter_payload_batches', batches), \
         patch('src.execution_provider.adapters.ray_map_transport._clock_domain', return_value=CLOCK), \
         patch('socket.socket.connect', side_effect=AssertionError('network forbidden in local fixture')):
        try:
            execution = build_fixed_model_execution(
                FixedModelConfig('http://localhost/unused-fixture', 'fixture', 60000),
                observer=capture, max_tasks=min(256, rows), max_active_requests=capacity,
                transport_factory=factory)
            job, session, _ = execution.open_job('fixture', SessionSpec('fixture', 'map', 'fixture'))
            report['limits'] = asdict(execution.engine.capacity.limits)
            began = time.monotonic_ns()
            offered, outputs, pending, first_result, reorder_peak = 0, [], {}, None, 0
            sealed = False
            while True:
                if time.monotonic_ns() - case_started >= 60 * 1e9:
                    raise TimeoutError('finite local case deadline')
                available = session.limits.held_tasks - execution.engine.capacity.usage().held_tasks
                if offered < rows and available:
                    tasks = [OfferedTask(i, payload(i), 1, 64,
                                        info=TaskInfo('fixture', i, 'model', WORK))
                             for i in range(offered, min(rows, offered + available))]
                    intake = session.offer(tasks)
                    if intake.status == 'REJECTED':
                        raise AssertionError('Core rejected finite fixture input')
                    offered += intake.accepted_prefix_count
                if offered == rows and not sealed:
                    session.seal()
                    sealed = True
                progress = execution.engine.advance()
                result = session.advance(session.limits.held_tasks)
                if result.error or progress.error or result.state in (State.CANCELLED, State.FAILED):
                    report['core_error'] = result.error or progress.error or result.state.value
                    raise RuntimeError('Core fixture execution failed')
                for delivery in result.deliveries:
                    sequence = delivery.key.sequence
                    if sequence in pending or sequence < len(outputs) or delivery.result != response(sequence):
                        raise AssertionError('Core result identity or content differs')
                    pending[sequence] = delivery.result
                    if first_result is None:
                        first_result = (time.monotonic_ns() - began) / 1e9
                reorder_peak = max(reorder_peak, len(pending))
                while len(outputs) in pending:
                    outputs.append(pending.pop(len(outputs)))
                if result.deliveries:
                    session.release([d.lease_id for d in result.deliveries])
                if result.state == State.FINISHED:
                    break
                if not result.deliveries and not progress.has_immediate_work and not result.has_immediate_work:
                    execution.engine.wake.wait(result.generation, .01)
            ended = time.monotonic_ns()
            if outputs != [response(i) for i in range(rows)] or pending:
                raise AssertionError('ordered consumer history differs')
            if sorted(ray.issued) != list(range(rows)) or sorted(ray.completed) != list(range(rows)):
                raise AssertionError('synthetic calls are not exactly once')
            if charged_unit.attempts != rows or charged_unit.remaining:
                raise AssertionError('finite accounting differs')
            if any(vars(execution.engine.capacity.usage()).values()):
                raise AssertionError('Core resources remain held')
            if session.close().status != 'CLOSED':
                raise AssertionError('Core session did not close')
            execution.engine.close_job(job)
            teardown = time.monotonic_ns()
            report.update(analyze(capture.events, rows, began, ended, ray.peak),
                          query_started_ns=began, query_completed_ns=ended,
                          startup_seconds=(began - case_started) / 1e9,
                          first_result_seconds=first_result, reorder_peak_rows=reorder_peak,
                          synthetic_calls=len(ray.issued), resources_drained=True, exactly_once=True)
            if accounting == 'durable':
                with ledger._transaction() as connection:
                    saved = connection.execute('SELECT sequence,request_sha256 FROM shared_requests ORDER BY sequence').fetchall()
                if saved != [(attempt, hashlib.sha256(payload(sequence)).hexdigest())
                             for sequence, attempt in sorted(charged.items(), key=lambda item: item[1])]:
                    raise AssertionError('durable digest history differs')
            report.update(status='passed', accounted_calls=charged_unit.attempts,
                          durable_per_call_history=accounting == 'durable')
        except BaseException as error:
            failure = error
            report.update(status='failed', error_type=type(error).__name__,
                          synthetic_calls=len(ray.issued), accounted_calls=len(charged),
                          calls_when_failure_observed=len(ray.issued))
        finally:
            cleanup_errors = []
            if failure and execution and job:
                # Stop intake, suppress unsent work, and let confirmed replies settle.
                # Unknown work stays charged; stopping a fixture is not a receipt.
                try:
                    execution.engine.close_job(job)
                    session.close_consumer()
                    drain_until = time.monotonic() + 5
                    while execution.engine.capacity.usage().active_requests and time.monotonic() < drain_until:
                        execution.engine.advance()
                        execution.engine.wake.wait(execution.engine.wake.generation, .001)
                except BaseException as error:
                    cleanup_errors.append(dict(resource='core_drain', error_type=type(error).__name__))
            for name, close in (
                ('backend', lambda: execution.close(5) if execution else True),
                ('synthetic_worker', ray.close),
                ('event_writer', lambda: writer.close() if writer else None),
            ):
                try:
                    if close() is False:
                        raise RuntimeError('diagnostic resource did not close')
                except BaseException as error:
                    cleanup_errors.append(dict(resource=name, error_type=type(error).__name__))
                    failure = failure or error
            # A call can pass its guard while the owner is requesting cancellation.
            # Count after the drain, retaining the earlier failure observation too.
            report['synthetic_calls'] = len(ray.issued)
            try:
                report['accounted_calls'] = charged_unit.attempts
            except BaseException as error:
                report['accounted_calls'] = None
                cleanup_errors.append(dict(resource='accounting_snapshot', error_type=type(error).__name__))
                failure = failure or error
            report.update(teardown_seconds=(time.monotonic_ns() - teardown) / 1e9,
                          case_seconds=(time.monotonic_ns() - case_started) / 1e9,
                          projection_seconds=capture.projection_ns / 1e9,
                          writer_call_seconds=capture.writer_call_ns / 1e9,
                          writer=writer.snapshot() if writer else None,
                          cleanup_errors=cleanup_errors,
                          synthetic_worker_stopped=not ray.thread.is_alive(),
                          actor_disposed=ray.killed,
                          transport_drained=transport is not None and not (
                              transport.rows or transport.blocks or transport.unknown or transport.used_bytes))
            if not failure and not (report['synthetic_worker_stopped'] and report['actor_disposed']
                                    and report['transport_drained']):
                failure = AssertionError('final diagnostic resources remain held')
            if failure:
                report['status'] = 'failed'
            with gzip.open(output / 'events.jsonl.gz', 'xt', encoding='utf-8') as stream:
                for event in capture.events:
                    stream.write(json.dumps(redact_json_values(event), separators=(',', ':')) + '\n')
            write_private_json(output / 'result.json', redact_json_values(report))
    if failure:
        raise RuntimeError('local observation diagnosis failed; evidence retained') from failure
    return report


def run_probe(output):
    output = Path(output).absolute()
    new_private_directory(output)
    repo = Path(__file__).resolve().parents[3]
    sources = ('code/src/experiments/map_observation_probe.py',
               'code/src/experiments/choice_gateway_observer.py', 'code/src/experiments/cell_budget.py',
               'code/src/experiments/shared_request_budget.py', 'code/src/experiments/buffered_events.py',
               'code/src/execution_provider/adapters/ray_map_transport.py',
               'code/src/execution_provider/adapters/incremental_execution.py',
               'code/src/scheduling/core/session.py', 'code/src/scheduling/runtime/async_backend.py')
    report = dict(schema='semloom.map_observation_probe.v1', status='running',
        reference_commit=subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip(),
        source_sha256={name: hashlib.sha256((repo / name).read_bytes()).hexdigest() for name in sources},
        scope='local_core_transport_with_synthetic_vendor_and_service',
        python=platform.python_version(), platform=platform.system(), machine=platform.machine(),
        sqlite=sqlite3.sqlite_version, max_seconds=300, synthetic_call_limit=16448,
        http_requests=0, model_requests=0, pg_queries=0, rows=ROWS, capacity=CAPACITY, batch_rows=BATCH_ROWS,
        synthetic_prepare_seconds=PREPARE_S, synthetic_put_seconds=PUT_S,
        synthetic_service_seconds=[SERVICE_S, SERVICE_S + .006], measured_repeats=3, runs=[])
    write_private_json(output / 'manifest.json', report)
    began = time.monotonic()
    try:
        for phase, repeat in [('check', -1), ('warmup', 0), ('measurement', 1),
                              ('measurement', 2), ('measurement', 3)]:
            arms = ARMS if repeat <= 0 else ARMS[repeat - 1:] + ARMS[:repeat - 1]
            for accounting, recording in arms:
                if time.monotonic() - began >= 240:  # Reserve one case's 60 seconds.
                    raise TimeoutError('overall local probe deadline')
                identity = f'{phase}-{repeat}-{accounting}-{recording}'
                report['active_run'] = identity
                run = run_case(output / identity, accounting, recording, rows=16 if phase == 'check' else ROWS)
                report['runs'].append(dict(run_id=identity, phase=phase, repeat=repeat, **run))
                print(json.dumps({key: run[key] for key in
                    ('accounting', 'recording', 'rows', 'query_seconds', 'preparation_windows',
                     'core_active_mean', 'synthetic_worker_active_mean')}, sort_keys=True), flush=True)
        if sum(run['synthetic_calls'] for run in report['runs']) != report['synthetic_call_limit']:
            raise AssertionError('suite call accounting differs')
        report['status'] = 'passed'
    except BaseException as error:
        report.update(status='failed', error_type=type(error).__name__)
        failed_result = output / report.get('active_run', 'not-started') / 'result.json'
        if failed_result.is_file():
            report['failed_run'] = json.loads(failed_result.read_text())
        raise
    finally:
        report['suite_seconds'] = time.monotonic() - began
        report['synthetic_calls_completed'] = sum(r['synthetic_calls'] for r in report['runs']) + (
            report.get('failed_run', {}).get('synthetic_calls', 0))
        write_private_json(output / 'summary.json', redact_json_values(report))
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path, help='new directory outside Git')
    args = parser.parse_args(argv)
    run_probe(args.output)


if __name__ == '__main__':
    main()
