"""Finite same-host accounting component comparison; no HTTP or model calls."""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
import multiprocessing
from multiprocessing.connection import wait
from pathlib import Path
import platform
import sqlite3
import subprocess
import time

from src.baselines.common.private_artifacts import new_private_directory, write_private_json
from src.baselines.common.redact import redact_json_values
from .attempt_ledger import AttemptBudget
from .cell_budget import CellBudgetLedger
from .mapped_request_budget import MappedUnitClient, MappedUnitOwner


ROWS, REPEATS = 1024, 3
ARMS = (('durable', 1), ('mapped', 1), ('durable', 4), ('mapped', 4))


def digest(sequence):
    return hashlib.sha256(f'fixture-input-{sequence % 127}'.encode()).hexdigest()


def _worker(resource, mode, inputs, start, connection):
    client = None
    grants = []
    try:
        client = MappedUnitClient(resource) if mode == 'mapped' else resource
        connection.send(dict(state='ready'))
        if not start.wait(10):
            raise TimeoutError('component start signal timed out')
        for sequence, value in inputs:
            grants.append((sequence, value, client.reserve(value)))
        connection.send(dict(state='completed', grants=grants))
    except BaseException as error:
        connection.send(dict(state='failed', error_type=type(error).__name__, grants=grants))
    finally:
        if mode == 'mapped' and client is not None:
            client.close()
        connection.close()


def run_case(root, mode, workers, repeat):
    new_private_directory(root)
    context = multiprocessing.get_context('spawn')
    start = context.Event()
    active, connections, owner, ledger = [], [], None, None
    report = dict(mode=mode, workers=workers, repeat=repeat, rows=ROWS,
                  warmup=repeat == 0, status='running', http_requests=0, model_requests=0, pg_queries=0)
    write_private_json(root / 'manifest.json', report)
    values = tuple(digest(i) for i in range(ROWS))
    began = time.monotonic()
    deadline = began + 30
    failure, grants = None, []
    try:
        budget = AttemptBudget(f'fixture.mapped.{mode}.{workers}.{repeat}', ROWS)
        ledger = CellBudgetLedger.create(root / 'ledger.sqlite', budget, deadline_utc=time.time() + 30)
        ledger.reserve_unit('case', ROWS)
        if mode == 'mapped':
            owner = MappedUnitOwner.prepare(ledger, 'case', values, root / 'mapped')
            resource = owner.descriptor
        else:
            resource = ledger.claim_shared_unit('case')
        for worker in range(workers):
            parent, child = context.Pipe(duplex=False)
            process = context.Process(target=_worker, args=(resource, mode,
                tuple((i, values[i]) for i in range(worker, ROWS, workers)), start, child))
            process.start()
            child.close()
            active.append(process)
            connections.append(parent)
        awaiting = set(connections)
        while awaiting:
            if time.monotonic() >= deadline:
                raise TimeoutError('component preparation deadline')
            for connection in wait(awaiting, .05):
                result = connection.recv()
                if result['state'] != 'ready':
                    raise RuntimeError('component caller failed during preparation')
                awaiting.remove(connection)
        dispatched = time.monotonic()
        start.set()
        awaiting = set(connections)
        while awaiting:
            if time.monotonic() >= deadline:
                raise TimeoutError('component completion deadline')
            for connection in wait(awaiting, .05):
                result = connection.recv()
                grants.extend(result.get('grants', ()))
                if result['state'] != 'completed':
                    raise RuntimeError('component caller failed')
                awaiting.remove(connection)
        collected = time.monotonic()
        if sorted(row[0] for row in grants) != list(range(ROWS)) or sorted(row[2] for row in grants) != list(range(1, ROWS + 1)):
            raise AssertionError('component identity or attempt prefix differs')
        if any(value != values[sequence] for sequence, value, _ in grants):
            raise AssertionError('component digest binding differs')
        if mode == 'mapped':
            state = owner.client.snapshot()
            if state['attempts'] != ROWS or state['remaining']:
                raise AssertionError('mapped final accounting differs')
            report['mapped_state_bytes'] = state['state_bytes']
        else:
            with ledger._transaction() as connection:
                saved = connection.execute('SELECT sequence,request_sha256 FROM shared_requests ORDER BY sequence').fetchall()
            if saved != sorted((attempt, value) for _, value, attempt in grants):
                raise AssertionError('durable history differs')
        if ledger.snapshot()['allocated_requests'] != ROWS:
            raise AssertionError('component durable total differs')
        report.update(status='passed', preparation_seconds=dispatched - began,
                      grant_seconds=collected - dispatched, verified_requests=ROWS,
                      allocations_durable=True, unique_prefix=True, digest_bindings=True)
    except BaseException as error:
        failure = error
        report.update(status='failed', error_type=type(error).__name__)
    finally:
        close_error = None
        try:
            if owner is not None:
                owner.close()
            elif ledger is not None:
                ledger.close_shared_unit('case')
        except BaseException as error:
            close_error = type(error).__name__
            failure = failure or error
        start.set()
        for process in active:
            process.join(max(0, min(1, deadline - time.monotonic())))
            if process.is_alive():
                process.kill()
                process.join(5)
        for connection in connections:
            connection.close()
        report.update(component_total_seconds=time.monotonic() - began,
                      received_grants=len(grants), worker_exits=[p.exitcode for p in active],
                      workers_stopped=all(not p.is_alive() for p in active), close_error=close_error)
        if not failure and (not report['workers_stopped'] or any(p.exitcode for p in active)):
            failure = RuntimeError('component caller cleanup failed')
        if failure:
            report['status'] = 'failed'
        with gzip.open(root / 'grants.jsonl.gz', 'xt') as stream:
            for sequence, value, attempt in grants:
                stream.write(json.dumps(dict(sequence=sequence, request_sha256=value, attempt=attempt)) + '\n')
        write_private_json(root / 'result.json', redact_json_values(report))
    if failure:
        raise RuntimeError('mapped component run failed; evidence retained') from failure
    return report


def run_probe(output):
    output = Path(output).absolute()
    new_private_directory(output)
    repo = Path(__file__).resolve().parents[3]
    source_names = ('cell_budget.py', 'shared_request_budget.py', 'mapped_request_budget.py', 'mapped_request_probe.py')
    summary = dict(schema='semloom.mapped_request_probe.v1', status='running',
        started_utc=datetime.now(timezone.utc).isoformat(),
        reference_commit=subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip(),
        source_sha256={name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest() for name in source_names},
        platform=platform.system(), machine=platform.machine(), python=platform.python_version(), sqlite=sqlite3.sqlite_version,
        scope='local_accounting_component', rows=ROWS, max_seconds=300, grant_limit=16384,
        http_requests=0, model_requests=0, pg_queries=0, runs=[])
    write_private_json(output / 'manifest.json', summary)
    began = time.monotonic()
    try:
        for repeat in range(REPEATS + 1):
            arms = ARMS if repeat == 0 else ARMS[repeat - 1:] + ARMS[:repeat - 1]
            for mode, workers in arms:
                if time.monotonic() - began >= 270:
                    raise TimeoutError('overall local component deadline')
                identity = f'{repeat}-{mode}-{workers}'
                summary['active_run'] = identity
                run = run_case(output / identity, mode, workers, repeat)
                summary['runs'].append(dict(run_id=identity, **run))
                print(json.dumps({key: run[key] for key in ('mode', 'workers', 'repeat',
                    'grant_seconds', 'component_total_seconds')}), flush=True)
        if sum(run['received_grants'] for run in summary['runs']) != summary['grant_limit']:
            raise AssertionError('component suite total differs')
        summary['status'] = 'passed'
    except BaseException as error:
        summary.update(status='failed', error_type=type(error).__name__)
        raise
    finally:
        summary.update(completed_utc=datetime.now(timezone.utc).isoformat(), suite_seconds=time.monotonic() - began)
        write_private_json(output / 'summary.json', redact_json_values(summary))
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    run_probe(parser.parse_args(argv).output)


if __name__ == '__main__':
    main()
