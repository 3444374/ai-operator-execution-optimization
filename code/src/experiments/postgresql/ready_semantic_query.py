"""Prepared database-source Map queries through one common HTTP observer."""
import argparse
import hashlib
import json
from pathlib import Path
import os
import time
import threading

from src.baselines.common.private_artifacts import new_private_directory, write_private_json, open_private_text
from src.baselines.common.redact import redact_text
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.experiments.attempt_ledger import AttemptBudget
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from .query_config import QueryConfig
from .query_runner import run_query
from .query_workloads import load_manifest
from .ready_query_recording import proxy_http_peak
from .cell_evidence import CellErrors
from .semantic_system_query import record_native_response


def run_ready_database_query(config, *, manifest_path, model_path, budget_path, budget,
                             root, dsn, pg_log=None, ray_temp_root=None):
    """Each runner keeps its existing attempt guard; the proxy only observes."""
    started = time.monotonic_ns()
    root = Path(root)
    new_private_directory(root)
    manifest = load_manifest(Path(manifest_path))
    model = load_fixed_model_config(Path(model_path))
    if config.arm not in ('pg', 'pg-source-direct', 'ray-data', 'daft-native') or config.task != 'map':
        raise ValueError('prepared comparison requires a supported database-source Map query')
    summary = dict(schema=('semloom.ready_pg_query.v1' if config.arm == 'pg'
                           else 'semloom.ready_database_query.v1'), status='failed',
                   unit_id=config.unit_id, query_preparation_started_ns=started,
                   original_model_config_sha256=hashlib.sha256(Path(model_path).read_bytes()).hexdigest(),
                   rows=manifest['rows'], timing_mode='ready',
                   query_entry='native SQL' if config.arm == 'pg' else 'database-source query API',
                   performance_qualified=False)
    if config.arm != 'pg':
        summary['source_reading_scope'] = 'installed immutable PG relation; ordinary SQL scans and native reader connections are query-owned timed work'
    errors = CellErrors()
    stopped = threading.Event()
    def before(route, body):
        if stopped.is_set():
            raise RuntimeError('this prepared query stopped further model forwarding')
    def after(route, body, response, status):
        try:
            record_native_response(protocols, body, response, status, model.model_id)
        except BaseException as error:
            stopped.set()
            errors.record('http_failure', error)
            raise
    try:
        with open_private_text(root/'protocols.jsonl') as protocols, ObservationGateway(
                routes=(GatewayRoute(config.unit_id, 'model', model.endpoint_url),),
                trace_path=root/'http-trace.jsonl', request_timeout_s=config.query_timeout_s,
                before_forward=before, after_forward=after) as gateway:
            routed = json.loads(Path(model_path).read_text())
            routed['endpoint_url'] = gateway.endpoint_url(config.unit_id, 'model')
            routed_path = root/'routed-model.json'
            write_private_json(routed_path, routed)
            runtime_options = {'ray_temp_root':ray_temp_root} if ray_temp_root is not None else {}
            result = run_query(config, manifest_path=manifest_path, model_path=routed_path,
                budget_path=budget_path, budget=budget, root=root/'unit', dsn=dsn,
                pg_log=pg_log, timing_mode='ready', **runtime_options)
        traces = [json.loads(line) for line in (root/'http-trace.jsonl').read_text().splitlines()]
        actual = result['evaluation']['actual_posts']
        if len(traces) != actual or actual != manifest['rows'] or any(
                value['status'] != 'completed' or
                value['request_body_sha256'] != value['forwarded_body_sha256'] for value in traces):
            raise ValueError('prepared database query proxy count or response audit failed')
        execution = result['execution']
        first_request = min(value['received_monotonic_ns'] for value in traces)
        if first_request < execution['t_submit_ns']:
            raise ValueError('database query preparation unexpectedly called the model')
        http = result['evaluation'].get('http', {})
        summary.update(status='passed', execution=execution, preparation_model_posts=0,
            actual_posts=actual, quality=result['evaluation']['quality'],
            model_usage=result['evaluation'].get('model_usage'),
            observed_peak_http=proxy_http_peak(traces),
            http_peak_scope='common proxy actual upstream dispatch through complete response body read',
            resources=result.get('resources'),evaluation_resources=result.get('evaluation_resources'),
            full_query_seconds=(execution['t_query_terminal_ns']-started)/1e9,
            original_query_summary_sha256=hashlib.sha256((root/'unit/summary.json').read_bytes()).hexdigest())
        if config.arm == 'pg':
            summary.update(core_peak_http=http.get('peak_http'),core_http_observation=http)
        else:
            summary['client_http_observation'] = http
    except BaseException as error:
        errors.record('query', error)
        primary = errors.first
        summary['error'] = dict(type=type(primary).__name__, message=redact_text(str(primary)))
    finally:
        summary['errors'] = errors.details
        summary['ended_ns'] = time.monotonic_ns()
        errors.attempt('summary_write', lambda:write_private_json(root/'summary.json', summary))
    errors.raise_if_failed()
    return summary


def run_ready_pg_query(config, *, manifest_path, model_path, budget_path, budget,
                       root, dsn, pg_log):
    """Keep the existing PG-only public entry and its summary schema."""
    if config.arm != 'pg':
        raise ValueError('prepared semantic comparison requires a PG Map query')
    return run_ready_database_query(config, manifest_path=manifest_path, model_path=model_path,
        budget_path=budget_path, budget=budget, root=root, dsn=dsn, pg_log=pg_log)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config', 'manifest', 'model', 'budget', 'output'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--pg-log', type=Path)
    parser.add_argument('--ray-temp-root', type=Path)
    parser.add_argument('--budget-id', required=True)
    parser.add_argument('--max-attempts', type=int, required=True)
    parser.add_argument('--dsn-env', default='SEMLOOM_QUERY_DSN')
    args = parser.parse_args(argv)
    if not os.environ.get(args.dsn_env):
        parser.error('required PG DSN environment variable is unset')
    config = QueryConfig(**json.loads(args.config.read_text()))
    if config.arm == 'pg' and args.pg_log is None:
        parser.error('PG query requires --pg-log')
    run_ready_database_query(config, manifest_path=args.manifest, model_path=args.model,
        budget_path=args.budget, budget=AttemptBudget(args.budget_id, args.max_attempts),
        root=args.output, dsn=os.environ[args.dsn_env], pg_log=args.pg_log,
        ray_temp_root=args.ray_temp_root)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
