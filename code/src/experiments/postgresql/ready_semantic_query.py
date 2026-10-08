"""Prepared PG queries observed through the same HTTP proxy as native peers."""
import argparse
import hashlib
import json
from pathlib import Path
import os
import time

from src.baselines.common.private_artifacts import new_private_directory, write_private_json
from src.baselines.common.redact import redact_text
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.experiments.attempt_ledger import AttemptBudget
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from .query_config import QueryConfig
from .query_runner import run_query
from .query_workloads import load_manifest
from .ready_query_recording import proxy_http_peak
from .cell_evidence import CellErrors


def run_ready_pg_query(config, *, manifest_path, model_path, budget_path, budget,
                       root, dsn, pg_log):
    """Count PG attempts only in its existing guard, never again at the proxy."""
    started = time.monotonic_ns()
    root = Path(root)
    new_private_directory(root)
    manifest = load_manifest(Path(manifest_path))
    model = load_fixed_model_config(Path(model_path))
    if config.arm != 'pg' or config.task != 'map':
        raise ValueError('prepared semantic comparison requires a PG Map query')
    summary = dict(schema='semloom.ready_pg_query.v1', status='failed',
                   unit_id=config.unit_id, query_preparation_started_ns=started,
                   original_model_config_sha256=hashlib.sha256(Path(model_path).read_bytes()).hexdigest(),
                   rows=manifest['rows'], timing_mode='ready', query_entry='native SQL',
                   performance_qualified=False)
    errors = CellErrors()
    try:
        with ObservationGateway(routes=(GatewayRoute(config.unit_id, 'model', model.endpoint_url),),
                trace_path=root/'http-trace.jsonl', request_timeout_s=config.query_timeout_s) as gateway:
            routed = json.loads(Path(model_path).read_text())
            routed['endpoint_url'] = gateway.endpoint_url(config.unit_id, 'model')
            routed_path = root/'routed-model.json'
            write_private_json(routed_path, routed)
            result = run_query(config, manifest_path=manifest_path, model_path=routed_path,
                budget_path=budget_path, budget=budget, root=root/'unit', dsn=dsn,
                pg_log=pg_log, timing_mode='ready')
        traces = [json.loads(line) for line in (root/'http-trace.jsonl').read_text().splitlines()]
        actual = result['evaluation']['actual_posts']
        if len(traces) != actual or actual != manifest['rows'] or any(
                value['status'] != 'completed' or
                value['request_body_sha256'] != value['forwarded_body_sha256'] for value in traces):
            raise ValueError('prepared PG proxy request count or response audit failed')
        execution = result['execution']
        first_request = min(value['received_monotonic_ns'] for value in traces)
        if first_request < execution['t_submit_ns']:
            raise ValueError('PG preparation unexpectedly called the model')
        summary.update(status='passed', execution=execution, preparation_model_posts=0,
            actual_posts=actual, quality=result['evaluation']['quality'],
            model_usage=result['evaluation'].get('model_usage'),
            observed_peak_http=proxy_http_peak(traces),
            core_peak_http=result['evaluation']['http'].get('peak_http'),
            core_http_observation=result['evaluation']['http'],
            http_peak_scope='common proxy actual upstream dispatch through complete response body read',
            full_query_seconds=(execution['t_query_terminal_ns']-started)/1e9,
            original_query_summary_sha256=hashlib.sha256((root/'unit/summary.json').read_bytes()).hexdigest())
    except BaseException as error:
        errors.record('query', error)
        summary['error'] = dict(type=type(error).__name__, message=redact_text(str(error)))
    finally:
        summary['errors'] = errors.details
        summary['ended_ns'] = time.monotonic_ns()
        errors.attempt('summary_write', lambda:write_private_json(root/'summary.json', summary))
    errors.raise_if_failed()
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config', 'manifest', 'model', 'budget', 'output', 'pg-log'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--budget-id', required=True)
    parser.add_argument('--max-attempts', type=int, required=True)
    parser.add_argument('--dsn-env', default='SEMLOOM_QUERY_DSN')
    args = parser.parse_args(argv)
    if not os.environ.get(args.dsn_env):
        parser.error('required PG DSN environment variable is unset')
    config = QueryConfig(**json.loads(args.config.read_text()))
    run_ready_pg_query(config, manifest_path=args.manifest, model_path=args.model,
        budget_path=args.budget, budget=AttemptBudget(args.budget_id, args.max_attempts),
        root=args.output, dsn=os.environ[args.dsn_env], pg_log=args.pg_log)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
