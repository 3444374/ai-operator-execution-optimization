"""Finite author Sema SQL through the public six-path query entry, with a software response."""

from __future__ import annotations

import argparse
from dataclasses import asdict, replace
from functools import partial
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from unittest import mock

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))

from src.baselines.common.private_artifacts import new_private_directory, write_private_json
from src.baselines.common.redact import redact_json_values, redact_text
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.baselines.text.products.sema import BINARY_SHA256, UPSTREAM_COMMIT
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.shared_request_budget import CellBudgetLedger
from src.experiments.postgresql.sema_native_query import SemaServiceConfig
from src.experiments.postgresql.supplier_adapter_query import run_supplier_query
from src.experiments.postgresql import supplier_adapter_query as supplier_module
from integrations.sema.verify_native import library_identity
from tests.experiments import test_native_adapter_query as fixture_module


ARMS = ('sema-native-direct', 'sema-native-daft-request-service',
        'sema-native-ray-data-request-service', 'sema-method-semloom-request-service',
        'sema-native-imlane-request-service', 'sema-native-imlane-ray-request-service')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--arm', choices=ARMS, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--fixture-port', type=int, default=46510)
    parser.add_argument('--service-port', type=int, default=46511)
    parser.add_argument('--observation-port', type=int, default=46512)
    parser.add_argument('--rows', type=int, default=8)
    parser.add_argument('--sema-native-threads', type=int, default=4)
    parser.add_argument('--max-held-tasks', type=int, default=8)
    parser.add_argument('--options', type=Path)
    parser.add_argument('--sema-service-config', type=Path)
    parser.add_argument('--ray-physical', type=Path)
    parser.add_argument('--ray-temp-root', type=Path)
    parser.add_argument('--ray-address')
    parser.add_argument('--imlane-cpython-config', type=Path)
    parser.add_argument('--imlane-ray-config', type=Path)
    args = parser.parse_args(argv)
    if sys.platform != 'linux':
        parser.error('the pinned author executable requires Linux')
    ports = (args.fixture_port, args.service_port, args.observation_port)
    if (not 1 <= args.rows <= 128 or any(not 1 <= port <= 65535 for port in ports)
            or len(set(ports)) != len(ports)):
        parser.error('the finite public check requires 1..128 rows and one declared loopback port')
    if hashlib.sha256(args.binary.read_bytes()).hexdigest() != BINARY_SHA256:
        parser.error('Sema binary differs from the pinned author artifact')
    options = NativeGraphOptions(**json.loads(args.options.read_text())) if args.options else NativeGraphOptions()
    service_config = SemaServiceConfig.load(args.sema_service_config)
    if args.arm != 'sema-native-direct':
        service_config = replace(service_config or SemaServiceConfig(), listen_port=args.service_port)
    physical = RayMapConfig.load(args.ray_physical) if args.ray_physical else None
    new_private_directory(args.output)
    report = dict(status='failed', arm=args.arm, rows=args.rows, real_model_posts=0, gpu_resources=0,
        fixture_posts_limit=args.rows, author_commit=UPSTREAM_COMMIT, binary_sha256=BINARY_SHA256,
        started_monotonic_ns=time.monotonic_ns(), cpu_affinity=sorted(os.sched_getaffinity(0)),
        query_timeout_s=20, source='original prepared author SQL with a loopback software model response',
        loopback_ports=dict(fixture=args.fixture_port, service=args.service_port,
                           observation=args.observation_port),
        options=asdict(options), sema_native_threads=args.sema_native_threads,
        max_held_tasks=args.max_held_tasks)
    primary = None
    try:
        if args.arm in ('sema-native-daft-request-service', 'sema-method-semloom-request-service'):
            report['daft'] = library_identity('daft')
        if args.arm in ('sema-native-ray-data-request-service', 'sema-method-semloom-request-service'):
            report['ray'] = library_identity('ray-data')
        ledger = CellBudgetLedger.create(args.output / 'budget.sqlite',
            AttemptBudget('sema-public-software', args.rows), deadline_utc=time.time() + 180)
        server_class = fixture_module.ThreadingHTTPServer

        def listener(_address, handler):
            return server_class(('127.0.0.1', args.fixture_port), handler)

        # Only select the existing fixture's port; author SQL, SDK and graphs are actual runtimes.
        gateway = partial(supplier_module.ObservationGateway, bind_port=args.observation_port)
        with mock.patch.object(fixture_module, 'ThreadingHTTPServer', listener), \
                mock.patch.object(supplier_module, 'ObservationGateway', gateway), \
                fixture_module.fixture_server(content='"ok"') as (endpoint, posts):
            result = run_supplier_query(args.arm,
                load_source=lambda: iter(dict(row_id=str(i), text='same original fixture text')
                                         for i in range(args.rows)),
                plan=SemanticMapPlan('Return ok.', 'fixture', 16),
                model=FixedModelConfig(endpoint, 'fixture', 20000), ledger=ledger,
                unit_id='sema-public-software', root=args.output / 'query', options=options,
                sema_binary=args.binary, sema_native_threads=args.sema_native_threads,
                max_held_tasks=args.max_held_tasks, query_timeout_s=20,
                sema_service_config=service_config, physical=physical,
                ray_temp_root=args.ray_temp_root, ray_address=args.ray_address,
                imlane_call_configs={mode: path for mode, path in
                    (('cpython', args.imlane_cpython_config), ('ray', args.imlane_ray_config)) if path is not None},
                reference_outputs={str(i): 'ok' for i in range(args.rows)}, allowed_outputs=('ok',))
            if (result['status'] != 'passed' or len(posts) != args.rows
                    or result['actual_posts'] != args.rows or result['execution']['recorded_rows'] != args.rows):
                raise ValueError('author SQL, software POST and original parser result counts differ')
            service = result.get('request_service')
            if service is not None and (service['cleanup_errors'] or not service['listener_shutdown_confirmed']
                    or service.get('uncertain_requests', 0) or service.get('held_requests', 0)
                    or service.get('native_bridge_thread_alive', False)):
                raise ValueError('Sema source, responses or local service ownership did not settle')
            report.update(status='passed', actual_fixture_posts=len(posts),
                quality_scope='software response parsed by the fixed original author; no model quality claim',
                query_summary_sha256=hashlib.sha256((args.output / 'query/summary.json').read_bytes()).hexdigest(),
                request_service=service, runtime=result.get('runtime', {'owner': 'fixed Sema author binary'}))
    except BaseException as error:
        primary = error
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        raise
    finally:
        report['ended_monotonic_ns'] = time.monotonic_ns()
        try:
            write_private_json(args.output / 'public-verification.json', redact_json_values(report))
        except BaseException as error:
            if primary is None:
                raise
            primary.add_note('Public verification artifact also failed: ' + type(error).__name__)
    print(json.dumps(redact_json_values(report), sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
