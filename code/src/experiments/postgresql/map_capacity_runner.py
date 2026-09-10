"""Run one declared SQuAD Map cell against an existing service and optional PG.

The caller owns model/cluster setup and campaign selection. This runner owns a
cell's gateway, fixed input table, budget claim, bounded recording and evaluation.
"""
import asyncio
from collections import Counter
from contextlib import ExitStack
from dataclasses import dataclass, asdict
import hashlib
import json
import os
from pathlib import Path
import sys
import time

from src.baselines.common.private_artifacts import content_digest, new_private_directory, write_private_json, open_private_text
from src.baselines.text.squad_map import validate_manifest
from src.baselines.text.map_inputs import verify_text_roundtrip
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget, observe_async_http_posts
from src.experiments.buffered_events import BufferedEvents, compact_event
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.expected_requests import ExpectedRequests, expected_request_manifest
from src.experiments.process_sampling import ProcessSampler
from src.observability.metrics.squad import squad_example_scores
from .map_direct import DirectMap, request_body
from .map_bindings import parse_pg_bindings, verify_bound_map_results
from .map_query_recording import record_pg_query, record_async_execution, evaluate_recording
from .runtime_helpers import owned_child_process, wait_for_path


@dataclass(frozen=True)
class CellConfig:
    unit_id: str
    arm: str
    split: str
    rows: int
    queries: int
    concurrency: int
    window: int
    input_bytes: int
    result_bytes: int
    pg_window_bytes: int
    statement_timeout_ms: int = 300000
    event_mode: str = 'compact-buffered'
    flush_rows: int = 64
    producer_trace: bool = True

    def __post_init__(self):
        if self.arm not in ('direct', 'pg') or self.split not in ('tuning', 'evaluation'):
            raise ValueError('unsupported cell path or split')
        limits = dict(rows=6000, queries=20, concurrency=256, window=256,
                      input_bytes=256*1048576, result_bytes=256*1048576,
                      pg_window_bytes=256*1048576, statement_timeout_ms=3600000, flush_rows=10000)
        for name, maximum in limits.items():
            if type(getattr(self, name)) is not int or not 1 <= getattr(self, name) <= maximum:
                raise ValueError('invalid cell ' + name)
        if self.event_mode not in ('compact-buffered', 'qualification'):
            raise ValueError('unknown observation mode')
        if type(self.producer_trace) is not bool:
            raise ValueError('producer_trace must be boolean')


def read_events(path):
    if path.stat().st_size > 256*1048576:
        raise ValueError('cell event history exceeds evaluation bound')
    with path.open() as stream:
        return [json.loads(line) for line in stream]


def resource_accounting(events, config):
    active = peak = 0
    peaks = {}
    drained = []
    for event in events:
        if event['event'] in ('http_started', 'core_http_started'):
            active += 1
            peak = max(peak, active)
        elif event['event'] in ('http_finished', 'core_http_finished'):
            active -= 1
        if not 0 <= active <= config.concurrency:
            raise ValueError('HTTP concurrency accounting invalid')
        usage = event.get('usage')
        if isinstance(usage, dict):
            for key, value in usage.items():
                if type(value) is int:
                    peaks[key] = max(peaks.get(key, 0), value)
            for key, limit in dict(held_tasks=config.window, input_bytes=config.input_bytes,
                                   result_bytes=config.result_bytes, active_requests=config.concurrency,
                                   active_work=config.concurrency).items():
                if usage.get(key, 0) > limit:
                    raise ValueError('logical resource limit exceeded')
        if event['event'] == 'core_job_drained':
            drained.append(usage)
    if active != 0 or peak == 0:
        raise ValueError('HTTP work did not settle')
    if config.arm == 'pg' and (len(drained) != config.queries or any(any(v for v in u.values()) for u in drained)):
        raise ValueError('PG logical resources did not drain')
    return dict(http_peak=peak, final_http_active=active, logical_peaks=peaks,
                drained_jobs=len(drained), work_unit='one request', result_bytes_kind='reserved capacity')


def evaluate_query(directory, rows, *, plan, bindings=None, events=(), sessions=()):
    references = {r['source_example_id']: r for r in rows}

    def evaluate(results):
        predictions = {}
        exact = f1 = 0
        with open_private_text(directory / 'scores.jsonl') as scores:
            for identity, output in results:
                if identity not in references or identity in predictions or not isinstance(output, str):
                    raise ValueError('invalid result identity or value')
                predictions[identity] = output
                em, token_f1 = squad_example_scores(output, references[identity]['reference_answers'])
                exact += em
                f1 += token_f1
                scores.write(json.dumps(dict(source_example_id=identity, exact_match=em, token_f1=token_f1)) + '\n')
        if predictions.keys() != references.keys():
            raise ValueError('missing query results')
        association = None
        if bindings is not None:
            association = verify_bound_map_results(
                [(r['source_example_id'], r['input_text']) for r in rows], predictions.items(),
                bindings, events, sessions, plan=plan)
            by_seq = {b.sequence: b.row_id for b in bindings}
            for event in events:
                if event.get('event') == 'core_map_completion':
                    if event['prompt_tokens'] != references[by_seq[event['sequence']]]['input_tokens']:
                        raise ValueError('PG complete-message token usage differs')
        return dict(rows=len(rows), exact_match_rows=exact, exact_match_percent=100*exact/len(rows),
                    token_f1_percent=100*f1/len(rows), association=association,
                    evaluator_retention='row-ID/reference and prediction dictionaries bounded by declared cell rows')
    return evaluate_recording(directory, evaluate, mode='stream')


def _prepare_pg(connection, rows, plan, root, config, socket):
    from psycopg import sql
    connection.execute('CREATE TEMP TABLE capacity_inputs (source_example_id text PRIMARY KEY,input_text text NOT NULL)')
    inputs = [(r['source_example_id'], r['input_text']) for r in rows]
    with connection.cursor().copy('COPY capacity_inputs FROM STDIN') as copy:
        for row in inputs:
            copy.write_row(row)
    roundtrip = verify_text_roundtrip(inputs, connection.execute('SELECT * FROM capacity_inputs').fetchall())
    write_private_json(root / 'roundtrip.json', roundtrip)
    connection.execute('ANALYZE capacity_inputs')
    settings = {'semloom_pg.gateway_socket': str(socket),
                'semloom_pg.provider_execution_profile': 'incremental-map',
                'semloom_pg.provider_window_tasks': str(config.window),
                'semloom_pg.provider_window_bytes': str(config.pg_window_bytes),
                'semloom_pg.test_map_binding_id_column': 'source_example_id' if config.producer_trace else '',
                'statement_timeout': str(config.statement_timeout_ms)}
    for name, value in settings.items():
        connection.execute('SELECT set_config(%s,%s,false)', (name, value))
    statement = sql.SQL('SELECT source_example_id,ai_semantic.map(input_text,{},{}::jsonb) FROM ONLY capacity_inputs').format(
        sql.Literal(plan.instruction), sql.Literal(json.dumps(dict(model=plan.model_id, max_tokens=64, temperature=0))))
    explain = connection.execute(sql.SQL('EXPLAIN (FORMAT JSON) ') + statement).fetchone()[0]
    write_private_json(root / 'plan.json', explain)
    encoded = json.dumps(explain)
    if 'SemLoom SemMap' not in encoded or f'"Semantic Input Window": {config.window}' not in encoded:
        raise ValueError('expected incremental Map plan/window absent')
    return statement


def run_cell(config, *, manifest, fixed_model_file, budget_file, budget, root, connection=None, pg_log=None):
    """Reserve a new cell once. Failure preserves artifacts and never refunds quota."""
    validate_manifest(manifest)
    rows = manifest['splits'][config.split][:config.rows]
    if len(rows) != config.rows or any(type(r.get('input_tokens')) is not int for r in rows):
        raise ValueError('cell requires enough token-profiled rows')
    if config.arm == 'pg' and (connection is None or pg_log is None):
        raise ValueError('PG cell requires caller connection and server log')
    model = load_fixed_model_config(fixed_model_file)
    plan = SemanticMapPlan(manifest['instruction'], model.model_id, 64)
    root = Path(root)
    new_private_directory(root)
    snapshot = dict(config=asdict(config), manifest_sha256=manifest['sha256'], semantic_spec_sha256=plan.digest,
                    selected_ids_sha256=content_digest([r['source_example_id'] for r in rows]))
    write_private_json(root / 'cell.json', snapshot)
    ledger = CellBudgetLedger(budget_file, budget)
    ledger.reserve_unit(config.unit_id, config.rows * config.queries)
    bodies = [request_body(plan, r['input_text']) for r in rows]
    expected_manifest = expected_request_manifest(body for _ in range(config.queries) for body in bodies)
    write_private_json(root / 'expected.json', expected_manifest)
    started = time.monotonic_ns()
    summary = dict(status='failed', **snapshot, performance_qualified=False, queries=[])
    sampler = None
    try:
        if config.arm == 'direct':
            executions, observer, sampler = asyncio.run(_run_direct(config, rows, plan, model, ledger, root, expected_manifest))
        else:
            executions, observer, sampler = _run_pg(config, rows, plan, connection, Path(pg_log), fixed_model_file,
                                                     budget_file, budget, root)
        summary['t_execution_cleanup_ns'] = time.monotonic_ns()
        events = read_events(root / 'events.jsonl')
        actual = Counter(e['request_values_sha256'] for e in events if e['event'] == 'request')
        if actual != Counter(content_digest(b) for _ in range(config.queries) for b in bodies):
            raise ValueError('actual request multiset differs from cell')
        if observer['observed_attempts'] != config.rows * config.queries:
            raise ValueError('observed attempt count differs')
        sessions = read_events(root / 'sessions.jsonl') if config.arm == 'pg' else []
        with ProcessSampler(root / 'evaluation-rss.jsonl', {'evaluator': os.getpid()}) as evaluation_sampler:
            evaluation_sampler.phase = 'evaluation'
            for index, execution in enumerate(executions):
                query = root / f'q{index}'
                bindings = None
                query_events = []
                if config.arm == 'pg' and config.producer_trace:
                    bindings = parse_pg_bindings((root / f'q{index}-producer.log').read_text().splitlines())
                    starts = [e for e in sessions if e.get('event') == 'session_start']
                    if len(starts) != config.queries:
                        raise ValueError('unexpected query session count')
                    query_events = [e for e in events if e.get('session_id') == starts[index]['session_id']]
                quality = evaluate_query(query, rows, plan=plan, bindings=bindings, events=query_events, sessions=sessions)
                summary['queries'].append(dict(execution=execution, evaluation=quality))
        summary.update(status='passed', producer_binding_verified=config.arm == 'pg' and config.producer_trace,
                       observer=observer, resource_accounting=resource_accounting(events, config), query_resources=sampler.summary(),
                       evaluation_resources=evaluation_sampler.summary(), actual_requests=sum(actual.values()))
        return summary
    finally:
        summary.update(started_ns=started, ended_ns=time.monotonic_ns())
        write_private_json(root / 'summary.json', summary)


async def _run_direct(config, rows, plan, model, ledger, root, expected_manifest):
    unit = ledger.claim_unit(config.unit_id)
    guard = ExpectedRequests(expected_manifest, available_attempts=unit.remaining)
    with ExitStack() as stack:
        events = (stack.enter_context(BufferedEvents(root / 'events.jsonl'))
                  if config.event_mode == 'compact-buffered' else None)
        stream = stack.enter_context(open_private_text(root / 'events.jsonl')) if events is None else None
        def record(event):
            event = compact_event(dict(event, monotonic_ns=time.monotonic_ns()))
            if events is not None:
                events.record(event)
            else:
                stream.write(json.dumps(event) + '\n')
                stream.flush()
        def request(attempt, payload):
            value = json.loads(payload)
            guard.accept(value)
            record(dict(event='request', attempt=attempt, request_values_sha256=content_digest(value),
                        request_bytes_sha256=hashlib.sha256(payload).hexdigest()))
        direct = DirectMap(model, config.concurrency, plan, record)
        executions = []
        try:
            with observe_async_http_posts(unit, request), ProcessSampler(root / 'query-rss.jsonl', {'consumer_direct': os.getpid()}) as sampler:
                sampler.phase = 'query'
                for index in range(config.queries):
                    executions.append(await record_async_execution(root / f'q{index}',
                        lambda index=index: direct.rows(rows, index+1), max_rows=len(rows),
                        max_result_bytes=len(rows)*70000, flush_rows=config.flush_rows))
        finally:
            await direct.close()
        if guard.remaining:
            raise ValueError('direct requests incomplete')
    observer = dict(observed_attempts=unit.attempts, events=events.snapshot() if events else None,
                    event_mode=config.event_mode)
    write_private_json(root / 'observer.json', observer)
    return executions, observer, sampler


def _run_pg(config, rows, plan, connection, pg_log, fixed_file, budget_file, budget, root):
    socket = root / 'g.sock'
    if len(str(socket).encode()) > 100:
        raise ValueError('use a short private artifact root for Unix sockets')
    event_args = (['--events', str(root / 'public-events.jsonl'), '--private-events', str(root / 'events.jsonl')]
                  if config.event_mode == 'qualification' else ['--events', str(root / 'events.jsonl')])
    command = [sys.executable, '-m', 'src.experiments.choice_gateway_observer', *event_args,
               '--session-events', str(root / 'sessions.jsonl'),
               '--event-mode', config.event_mode, '--observer-summary', str(root / 'observer.json'),
               '--expected-request-hashes', str(root / 'expected.json'), '--cell-budget', str(budget_file),
               '--unit-id', config.unit_id, '--budget-id', budget.budget_id, '--max-attempts', str(budget.limit),
               '--', '--socket', str(socket), '--fixed-model-config', str(fixed_file), '--incremental-map',
               '--max-active-jobs', '1', '--max-active-requests', str(config.concurrency),
               '--max-held-tasks', str(config.window), '--input-buffer-bytes', str(config.input_bytes),
               '--result-buffer-bytes', str(config.result_bytes), '--test-max-sessions', str(config.queries)]
    write_private_json(root / 'gateway-command.json', command)
    statement = _prepare_pg(connection, rows, plan, root, config, socket)
    executions = []
    try:
        with owned_child_process(command, root, 'gateway', os.environ.copy(), None) as gateway:
            wait_for_path(socket, gateway)
            pids = {'consumer': os.getpid(), 'gateway_core': gateway.pid,
                    'pg_backend': connection.info.backend_pid}
            with ProcessSampler(root / 'query-rss.jsonl', pids) as sampler:
                for index in range(config.queries):
                    log_offset = pg_log.stat().st_size
                    sampler.phase = 'query'
                    executions.append(record_pg_query(connection, statement, root / f'q{index}',
                        max_rows=len(rows), max_result_bytes=len(rows)*70000, flush_rows=config.flush_rows))
                    sampler.phase = 'between_queries'
                    with pg_log.open('rb') as log:
                        log.seek(log_offset)
                        trace = log.read()
                    with open_private_text(root / f'q{index}-producer.log') as out:
                        out.write(trace.decode())
            gateway.wait(timeout=30)
            if gateway.returncode != 0 or socket.exists():
                raise ValueError('gateway did not complete and clean up')
    finally:
        connection.execute('DROP TABLE IF EXISTS capacity_inputs')
    return executions, json.loads((root / 'observer.json').read_text()), sampler
