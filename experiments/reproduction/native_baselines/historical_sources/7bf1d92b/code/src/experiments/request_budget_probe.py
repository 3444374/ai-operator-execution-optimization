"""Bounded local durable-accounting comparison; no network or model calls."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import sqlite3
import statistics
import time

from src.baselines.common.private_artifacts import new_private_directory, open_private_text, write_private_json
from src.baselines.common.redact import redact_json_values
from .attempt_ledger import AttemptBudget
from .cell_budget import CellBudgetLedger


ROWS = 1024
BATCH_SIZES = (1, 4, 16, 64)
REPEATS = 3
MAX_SECONDS = 300


def run_probe(output):
    """Retain each ledger and run record, including the first failed attempt."""
    output = Path(output).absolute()
    new_private_directory(output)
    source = Path(__file__).parent
    files = ('cell_budget.py', 'shared_request_budget.py', 'request_budget_probe.py')
    summary = dict(schema='semloom.request_budget_batch_probe.v1', status='running',
                   platform=platform.system(), machine=platform.machine(),
                   python=platform.python_version(), sqlite=sqlite3.sqlite_version,
                   rows_per_run=ROWS, batch_sizes=BATCH_SIZES, measured_repeats=REPEATS,
                   warmups_per_size=1, reservation_limit=ROWS*len(BATCH_SIZES)*(REPEATS+1),
                   max_seconds=MAX_SECONDS, http_requests=0, model_requests=0, pg_queries=0,
                   source_sha256={name: hashlib.sha256((source/name).read_bytes()).hexdigest()
                                  for name in files}, runs=[])
    write_private_json(output/'manifest.json', summary)
    digests = tuple(hashlib.sha256(f'fixture-{i%127}'.encode()).hexdigest() for i in range(ROWS))
    deadline = time.monotonic()+MAX_SECONDS
    active = None
    try:
        with open_private_text(output/'runs.jsonl') as stream:
            for repeat in range(REPEATS+1):
                sizes = BATCH_SIZES if repeat == 0 else BATCH_SIZES[repeat-1:]+BATCH_SIZES[:repeat-1]
                for size in sizes:
                    active = dict(batch_size=size, repeat=repeat, warmup=repeat == 0)
                    remaining = deadline-time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError('probe deadline reached')
                    path = output/f'ledger-{repeat}-{size}.sqlite'
                    budget = AttemptBudget(f'fixture.batch.{repeat}.{size}', ROWS)
                    ledger = CellBudgetLedger.create(path, budget, deadline_utc=time.time()+min(60, remaining))
                    ledger.reserve_unit('cell', ROWS)
                    shared = ledger.claim_shared_unit('cell')
                    with ledger._transaction() as connection:
                        journal = connection.execute('PRAGMA journal_mode').fetchone()[0]
                        synchronous = connection.execute('PRAGMA synchronous').fetchone()[0]
                    chunks = tuple(digests[i:i+size] for i in range(0, ROWS, size))
                    attempts = []
                    started = time.perf_counter_ns()
                    for chunk in chunks:
                        if size == 1:
                            attempts.append(shared.reserve(chunk[0]))
                        else:
                            attempts.extend(shared.reserve_many(chunk))
                    elapsed = (time.perf_counter_ns()-started)/1e9
                    if time.monotonic() >= deadline or elapsed >= 60:
                        raise TimeoutError('probe deadline reached')
                    if attempts != list(range(1, ROWS+1)):
                        raise AssertionError('attempt sequence differs')
                    reopened = CellBudgetLedger(path, budget)
                    with reopened._transaction() as connection:
                        records = connection.execute(
                            'SELECT sequence,request_sha256 FROM shared_requests ORDER BY sequence').fetchall()
                    if records != list(enumerate(digests, 1)):
                        raise AssertionError('durable request history differs')
                    if reopened.snapshot()['allocated_requests'] != ROWS or shared.remaining != 0:
                        raise AssertionError('budget total differs')
                    item = dict(**active, status='passed', rows=ROWS, elapsed_seconds=elapsed,
                                reserve_transactions=len(chunks), journal_mode=journal,
                                synchronous=synchronous, verified_history=True)
                    summary['runs'].append(item)
                    stream.write(json.dumps(item, sort_keys=True)+'\n')
                    stream.flush()
                    print(json.dumps(item, sort_keys=True), flush=True)
        summary['medians_seconds'] = {
            str(size): statistics.median(r['elapsed_seconds'] for r in summary['runs']
                                         if r['batch_size'] == size and not r['warmup'])
            for size in BATCH_SIZES}
        summary['reservations_completed'] = sum(r['rows'] for r in summary['runs'])
        summary['status'] = 'passed'
    except BaseException as error:
        summary.update(status='failed', failed_run=active, error_type=type(error).__name__)
        raise
    finally:
        write_private_json(output/'summary.json', redact_json_values(summary))
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path, help='new directory outside Git')
    args = parser.parse_args(argv)
    result = run_probe(args.output)
    print(json.dumps(dict(status=result['status'], medians_seconds=result['medians_seconds'])))


if __name__ == '__main__':
    main()
