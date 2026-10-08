"""Bounded application Map comparison with native prompt/parser ownership.

The existing PG runner remains an independent peer. Native queries read the
same raw PG relation inside measured execution and retain their own AI APIs.
"""
import argparse
import base64
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import time
import threading

from src.baselines.common.private_artifacts import (
    new_private_directory, open_private_text, write_private_json)
from src.baselines.common.redact import redact_text
from src.baselines.text.frameworks.semantic_map import ROLES, open_rows, prepare_rows, validate_source
from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION, classification_audit
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.process_sampling import ProcessSampler
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from .map_query_recording import record_execution, evaluate_recording
from .ready_query_recording import record_prepared_execution, proxy_http_peak
from .cell_evidence import CellErrors
from .query_inputs import QueryInputs
from .query_workloads import load_manifest, read_prepared


def evaluate_outputs(recorded, references):
    predictions = dict(recorded)
    if len(predictions) != len(recorded) or set(predictions) != set(references):
        raise ValueError('native application outputs do not match all source occurrences')
    if any(not isinstance(v, str) for v in predictions.values()):
        raise ValueError('native application returned a non-text output')
    return classification_audit(references, predictions.items())


def native_file_capacity(rows, *, limits=None):
    """Check co-resident SDK/proxy/upstream sockets without limiting native work."""
    if type(rows) is not int or not 1 <= rows <= 4096:
        raise ValueError('native file capacity requires a bounded nonempty source')
    required = 3 * rows + 128
    if limits is None:
        import resource
        limits = resource.getrlimit(resource.RLIMIT_NOFILE)
    soft, hard = limits
    if soft != -1 and soft < required:
        raise ValueError(f'native query requires at least {required} local file descriptors; '
                         f'current soft limit is {soft}; prepare the caller environment first')
    return dict(soft_limit=soft, hard_limit=hard, required=required,
                estimate='up to three co-resident sockets per source row plus 128 ancillary descriptors')


def record_native_response(stream, body, response, status, model_id):
    record = dict(request=json.loads(body), http_status=status)
    try:
        decoded = json.loads(response)
    except (json.JSONDecodeError, UnicodeDecodeError):
        record.update(response=None, response_bytes_base64=base64.b64encode(response).decode('ascii'))
        stream.write(json.dumps(record, ensure_ascii=False)+'\n')
        raise
    record['response'] = decoded
    stream.write(json.dumps(record, ensure_ascii=False)+'\n')
    if status != 200 or not isinstance(decoded, dict) or decoded.get('model') != model_id:
        raise ValueError('native completion returned another model or an HTTP failure')
    if len(decoded.get('choices', [])) != 1 or decoded['choices'][0].get('finish_reason') != 'stop':
        raise ValueError('native completion did not terminate with its declared single answer')


def run_native_query(role, *, manifest_path, table, model_path, ledger, unit_id,
                     root, dsn, concurrency=16, num_threads=8, tokenizer_path=None,
                     query_timeout_s=120, sema_binary=None, timing_mode='application'):
    if timing_mode not in ('application', 'ready'):
        raise ValueError('unknown semantic query timing mode')
    started = time.monotonic_ns()
    manifest = load_manifest(Path(manifest_path))
    if manifest['kind'] != 'movie' or role not in ROLES or manifest['rows'] > 4096:
        raise ValueError('native semantic comparison requires a bounded Movie Map source')
    model = load_fixed_model_config(Path(model_path))
    plan = SemanticMapPlan(MOVIE_MAP_INSTRUCTION, model.model_id, 128)
    inputs = QueryInputs('movie', table, manifest['rows'], manifest['max_source_bytes'],
                         manifest['max_input_bytes'])
    root = Path(root)
    new_private_directory(root)
    summary = dict(schema='semloom.semantic_system_query.v1', status='failed',
        role=role, unit_id=unit_id, manifest_sha256=manifest['sha256'],
        model_config_sha256=hashlib.sha256(Path(model_path).read_bytes()).hexdigest(),
        concurrency=concurrency, num_threads=num_threads, rows=manifest['rows'],
        query_preparation_started_ns=started, performance_qualified=False,
        generation_limit_owner='model service default' if role == 'sema-map' else 'explicit request cap128',
        comparison='same application; native prompts/parsers; no message-equivalence claim')
    if timing_mode == 'ready':
        summary['timing_mode'] = timing_mode
        summary['query_entry'] = 'native SQL' if role in ('duckdb-ai', 'sema-map') else 'native API'
    shared = None
    reserved = False
    observed = []
    request_count = 0
    http_failed = threading.Event()
    query_stopped = threading.Event()
    cancel_prepared = None
    errors = CellErrors()
    def before(route, body):
        nonlocal request_count
        if timing_mode == 'ready' and (http_failed.is_set() or query_stopped.is_set()):
            raise RuntimeError('this prepared query stopped further model forwarding')
        values = json.loads(body)
        if values.get('model') != model.model_id or values.get('stream', False):
            raise ValueError('native system changed model or requested streaming')
        cap = values.get('max_completion_tokens', values.get('max_tokens'))
        expected_cap = None if role == 'sema-map' else 128
        if cap != expected_cap or values.get('temperature') != 0:
            raise ValueError('native system changed declared generation settings')
        attempt = shared.reserve(hashlib.sha256(body).hexdigest())
        request_count += 1
        observed.append(dict(attempt=attempt, request=values,
                             request_sha256=hashlib.sha256(body).hexdigest()))
    def after(route, body, response, status):
        try:
            record_native_response(protocols, body, response, status, model.model_id)
        except BaseException as failure:
            if timing_mode == 'ready':
                errors.record('http_failure', failure)
                http_failed.set()
                if cancel_prepared is not None:
                    errors.attempt('http_failure_cancel', cancel_prepared)
            raise
    try:
        summary['file_capacity'] = native_file_capacity(manifest['rows'])
        ledger.reserve_unit(unit_id, manifest['rows'])
        reserved = True
        shared = ledger.claim_shared_unit(unit_id)
        import psycopg
        with open_private_text(root/'protocols.jsonl') as protocols:
            with ObservationGateway(routes=(GatewayRoute(unit_id, 'model', model.endpoint_url),),
                    trace_path=root/'http-trace.jsonl', request_timeout_s=query_timeout_s,
                    before_forward=before, after_forward=after) as gateway:
                routed = replace(model, endpoint_url=gateway.endpoint_url(unit_id, 'model'))
                with psycopg.connect(dsn, autocommit=True) as connection:
                    connection.execute('SELECT set_config(%s,%s,false)',
                        ('statement_timeout', str(int(query_timeout_s * 1000))))
                    def load_source():
                        statement, parameters = inputs.select_sql()
                        with connection.transaction():
                            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                            with connection.cursor() as cursor:
                                rows = []
                                for row in cursor.stream(statement, parameters):
                                    if len(rows) == inputs.max_rows:
                                        raise ValueError('native PG source exceeds declared rows')
                                    rows.append(row)
                                return rows
                    with ProcessSampler(root/'query-rss.jsonl', {'native_driver': os.getpid(),
                            'pg_backend': connection.info.backend_pid}) as sampler:
                        options = dict(max_rows=manifest['rows'],
                            max_result_bytes=manifest['rows']*70000, flush_rows=64,
                            query_timeout_s=query_timeout_s,
                            cancel_query=lambda: connection.cancel_safe(timeout=1))
                        if timing_mode == 'ready':
                            values = validate_source(load_source(), inputs)
                            with prepare_rows(role, values, plan, routed,
                                    concurrency=concurrency, num_threads=num_threads,
                                    tokenizer_path=tokenizer_path, sema_binary=sema_binary,
                                    artifact_root=root) as prepared:
                                cancel_prepared = getattr(prepared, 'cancel', None)
                                def stop_prepared_query():
                                    query_stopped.set()
                                    if cancel_prepared is not None:
                                        cancel_prepared()
                                options['cancel_query'] = stop_prepared_query
                                summary['native_deadline_scope'] = (
                                    'stop further forwarding and cancel owned native session' if cancel_prepared is not None
                                    else 'stop further forwarding; SDK request timeouts and caller-owned process deadline supervise a noninterruptible API')
                                summary['preparation_model_posts'] = request_count
                                if request_count:
                                    raise ValueError('native preparation unexpectedly called the model')
                                from contextlib import contextmanager
                                @contextmanager
                                def submitted_rows():
                                    iterator = prepared.execute()
                                    try:
                                        yield iterator
                                    finally:
                                        close = getattr(iterator, 'close', None)
                                        if close is not None:
                                            close()
                                execution = record_prepared_execution(root/'q0', submitted_rows,
                                    backend_ready_ns=time.monotonic_ns(),
                                    preparation_started_ns=started, **options)
                        else:
                            execution = record_execution(root/'q0',
                                lambda: open_rows(role, load_source, inputs, plan, routed,
                                    concurrency=concurrency, num_threads=num_threads,
                                    tokenizer_path=tokenizer_path, sema_binary=sema_binary,
                                    artifact_root=root), **options)
                    summary['execution'] = execution
                    summary['resources'] = sampler.summary()
        trace = [json.loads(line) for line in (root/'http-trace.jsonl').read_text().splitlines()]
        if request_count != manifest['rows'] or len(trace) != request_count or any(
                r['status'] != 'completed' or r['request_body_sha256'] != r['forwarded_body_sha256']
                for r in trace):
            raise ValueError('native application request count or completion audit failed')
        summary['actual_posts'] = request_count
        if timing_mode == 'ready':
            summary['first_http_error_observed'] = http_failed.is_set()
        timeline = sorted((point, change) for r in trace for point, change in
            ((r['upstream_start_epoch_s'], 1), (r['upstream_response_epoch_s'], -1)))
        active = peak = 0
        for _, change in timeline:
            active += change
            peak = max(peak, active)
        summary['observed_peak_http'] = peak
        if timing_mode == 'ready':
            if min(value['received_monotonic_ns'] for value in trace) < execution['t_submit_ns']:
                raise ValueError('native preparation unexpectedly called the model')
            summary['legacy_http_peak'] = peak
            summary['observed_peak_http'] = proxy_http_peak(trace)
            summary['http_peak_scope'] = 'common proxy actual upstream dispatch through complete response body read'
        summary['model_usage'] = {name: sum(r[name] for r in trace) if all(
            r[name] is not None for r in trace) else None for name in
            ('actual_prompt_tokens', 'actual_output_tokens', 'actual_total_tokens')}
        references = {r['row_id']: r['reference'] for r in read_prepared(Path(manifest_path), 'references.jsonl')}
        summary['quality'] = evaluate_recording(root/'q0',
            lambda rows: evaluate_outputs(rows, references))['result']
        summary['full_query_seconds'] = (execution['t_query_terminal_ns'] - started) / 1e9
        if summary['quality']['invalid']:
            raise ValueError('native application returned illegal classification labels')
        summary['status'] = 'passed'
    except BaseException as error:
        errors.record('query', error)
        summary['error'] = dict(type=type(error).__name__, message=redact_text(str(error)))
    finally:
        if reserved:
            errors.attempt('budget_close', lambda:ledger.close_shared_unit(unit_id))
        errors.attempt('request_record', lambda:write_private_json(root/'request-reservations.json', observed))
        summary['actual_posts'] = request_count
        if timing_mode == 'ready':
            summary['query_stopped_observed'] = query_stopped.is_set()
        summary['errors'] = errors.details
        if errors.first is not None:
            summary['status'] = 'failed'
            summary['error'] = dict(type=type(errors.first).__name__, message=redact_text(str(errors.first)))
        summary['ended_ns'] = time.monotonic_ns()
        errors.attempt('summary_write', lambda:write_private_json(root/'summary.json', summary))
    errors.raise_if_failed()
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--role', choices=ROLES, required=True)
    for name in ('manifest', 'model', 'budget', 'output'):
        parser.add_argument('--'+name, type=Path, required=True)
    for name in ('table', 'unit-id', 'budget-id'):
        parser.add_argument('--'+name, required=True)
    parser.add_argument('--max-attempts', type=int, required=True)
    parser.add_argument('--dsn-env', default='SEMLOOM_QUERY_DSN')
    parser.add_argument('--concurrency', type=int, default=16)
    parser.add_argument('--num-threads', type=int, default=8)
    parser.add_argument('--tokenizer', type=Path)
    parser.add_argument('--sema-binary', type=Path)
    parser.add_argument('--timing-mode', choices=('application', 'ready'), default='application')
    args = parser.parse_args(argv)
    if not os.environ.get(args.dsn_env):
        parser.error('required PG DSN environment variable is unset')
    ledger = CellBudgetLedger(args.budget, AttemptBudget(args.budget_id, args.max_attempts))
    run_native_query(args.role, manifest_path=args.manifest, table=args.table,
        model_path=args.model, ledger=ledger, unit_id=args.unit_id, root=args.output,
        dsn=os.environ[args.dsn_env], concurrency=args.concurrency,
        num_threads=args.num_threads, tokenizer_path=args.tokenizer, sema_binary=args.sema_binary,
        timing_mode=args.timing_mode)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
