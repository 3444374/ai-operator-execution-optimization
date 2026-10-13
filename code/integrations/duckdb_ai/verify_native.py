#!/usr/bin/env python3
"""Validate compiled SQL with a pinned native execution graph and localhost fixtures."""
from __future__ import annotations

import argparse
from dataclasses import asdict, is_dataclass
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys
import unittest

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))
from src.baselines.common.redact import redact_text
from src.baselines.common.private_artifacts import require_outside_git, write_private_json


IMLANE_MODES = {'imlane-cpython': 'cpython', 'imlane-ray': 'ray'}
SCHEDULER_OWNERS = {'daft': 'daft_native', 'ray': 'ray_core', 'ray-data': 'ray_data',
                    'imlane-cpython': 'IMLane author CPython', 'imlane-ray': 'IMLane author Ray'}


def imlane_config(backend, path):
    """Use the public mode/PID/build-identity configuration contract."""
    from src.experiments.postgresql.imlane_query import load_imlane_call_configs
    mode = IMLANE_MODES[backend]
    return load_imlane_call_configs({mode: path})[mode]


def author_build_versions(config):
    path = Path(config.author_build_identity)
    require_outside_git(path)
    identity = json.loads(path.read_text())
    versions = identity.get('python_arrow', {})
    if (versions.get('arrow') != '16.1.0' or type(versions.get('python')) is not str
            or not versions['python'].startswith('3.12.')):
        raise RuntimeError('IMLane validation requires the recorded Python 3.12 / Arrow 16.1.0 build')
    return dict(python=versions['python'], arrow=versions['arrow'],
                evidence='author build identity; live Python module imports belong to the caller-owned runtime')


def author_close_state(closes):
    """Missing child state and a missing exit code cannot establish reusable work."""
    if not closes:
        return dict(author_cleanup_required=None, author_runtime_reusable=None)
    required, known = False, True
    for close in closes:
        if close.get('external_coordinator_requires_cleanup') is True:
            required = True
        if close.get('bridge_started') is True or close.get('author_submission_possible') is True:
            code = close.get('returncode')
            if not (close.get('clean') is True and close.get('bridge_stopped') is True
                    and type(code) is int and code == 0):
                required = True
        elif close.get('bridge_started') is not False:
            known = False
    return dict(author_cleanup_required=True if required else False if known else None,
                author_runtime_reusable=False if required else True if known else None)


def ray_node_ports(base):
    if type(base) is not int or not 1024 <= base <= 65436:
        raise ValueError('Ray port base must leave 100 ports between 1024 and 65535')
    return dict(gcs_server_port=base, node_manager_port=base + 1,
                object_manager_port=base + 2, metrics_agent_port=base + 3,
                metrics_export_port=base + 4, dashboard_agent_listen_port=base + 5,
                runtime_env_agent_port=base + 6, dashboard_port=base + 7,
                min_worker_port=base + 10, max_worker_port=base + 99)


def start_ray(ray, temporary, port_base):
    """The pinned Cluster API owns only this verifier's node and its processes.

    Ray 2.56.1 ray.init rejects node/worker port kwargs. Cluster.add_node
    forwards these explicit ports to RayParams instead.
    """
    runtime_env = {'env_vars': {'PYTHONPATH': str(CODE), 'CUDA_VISIBLE_DEVICES': '',
                                'OMP_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1'}}
    if port_base is None:
        ray.init(address='local', num_cpus=4, num_gpus=0, include_dashboard=False,
                 _node_ip_address='127.0.0.1', _temp_dir=str(temporary),
                 object_store_memory=256 * 1024 * 1024, runtime_env=runtime_env)
        return None
    from ray.cluster_utils import Cluster
    from ray._private.services import get_node_ip_address
    # ray.init canonicalizes localhost to this machine's interface address.
    # Bind the owned node to the same address so both sides agree in containers.
    node_ip = get_node_ip_address()
    cluster = Cluster()
    try:
        cluster.add_node(num_cpus=4, num_gpus=0, include_dashboard=False,
                         node_ip_address=node_ip, temp_dir=str(temporary),
                         object_store_memory=256 * 1024 * 1024,
                         **ray_node_ports(port_base))
        ray.init(address=cluster.address, runtime_env=runtime_env)
        return cluster
    except BaseException as primary:
        for action in (ray.shutdown, cluster.shutdown):
            try:
                action()
            except BaseException as cleanup:
                if hasattr(primary, 'add_note'):
                    primary.add_note('owned Ray startup cleanup failed: ' + type(cleanup).__name__)
        raise


def load_suite(config, case):
    if config is not None:
        from tests.semantic_methods.test_duckdb_imlane_paths import sql_suite
        return sql_suite(config, case)
    from tests.semantic_methods.test_duckdb_native import DuckDBNativeGraphLibraryTests
    return unittest.defaultTestLoader.loadTestsFromTestCase(DuckDBNativeGraphLibraryTests)


class _Result(unittest.TextTestResult):
    def __init__(self, *args):
        super().__init__(*args)
        self.fixture_counts = []

    def stopTest(self, test):
        server = getattr(test, 'server', None)
        if server is not None:
            bridge = getattr(test, 'bridge', None)
            graph = getattr(test, 'graph', None)
            adapter = getattr(test, 'adapter', None)
            adapter_close = getattr(adapter, 'last_close_report', None)
            imlane = getattr(graph, '_imlane', None)
            records = getattr(server, 'records', ())
            execution = getattr(test, 'execution', None)
            engine = getattr(execution, 'engine', None)
            capacity = getattr(engine, 'capacity', None)
            usage = capacity.usage() if capacity is not None else None
            self.fixture_counts.append(dict(test=test.id(), http_requests=len(records),
                                            total_http_requests=getattr(server, 'total_requests', len(records)),
                                            peak_http_requests=getattr(server, 'peak', None),
                                            final_active=getattr(server, 'active', None),
                                            bridge_error=getattr(bridge, 'last_error', None),
                                            bridge_cleanup_error=getattr(bridge, 'last_cleanup_error', None),
                                            graph_close_report=getattr(graph, 'last_close_report', None),
                                            adapter_close_report=asdict(adapter_close) if is_dataclass(adapter_close) else adapter_close,
                                            core_capacity_usage=asdict(usage) if is_dataclass(usage) else None,
                                            adapter_delivered_count=getattr(adapter, 'last_delivered_count', None),
                                            imlane_limits=asdict(imlane.limits) if imlane is not None else None,
                                            unsupported=getattr(test, 'unsupported', []),
                                            graph_events=getattr(test, 'graph_observations', [])))
        super().stopTest(test)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--extension', type=Path, required=True)
    parser.add_argument('--backend', choices=tuple(SCHEDULER_OWNERS), required=True)
    parser.add_argument('--ray-temp', type=Path)
    parser.add_argument('--ray-port-base', type=int)
    parser.add_argument('--imlane-config', type=Path,
                        help='private public-call configuration for a ready caller-owned Coordinator')
    parser.add_argument('--imlane-case', choices=('complete', 'parser-stop', 'cancel', 'transport-error'),
                        help='each case requires its own fresh caller-owned Coordinator')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    is_imlane = args.backend in IMLANE_MODES
    if is_imlane != (args.imlane_config is not None):
        parser.error('IMLane modes require --imlane-config; other backends cannot use it')
    if not is_imlane and args.imlane_case is not None:
        parser.error('--imlane-case requires an IMLane backend')
    if args.backend not in ('ray', 'ray-data') and (args.ray_temp is not None or args.ray_port_base is not None):
        parser.error('Ray startup options apply only to verifier-owned Ray Core or Ray Data')
    if args.backend in ('ray', 'ray-data'):
        if args.ray_temp is None or len(str(args.ray_temp.absolute()).encode()) > 40:
            parser.error('Ray execution requires --ray-temp with a short data-disk path')
        if args.ray_port_base is not None:
            try:
                ray_node_ports(args.ray_port_base)
            except ValueError as error:
                parser.error(str(error))
            try:
                fixture_port = int(os.environ.get('DUCKDB_FIXTURE_PORT', '0'))
            except ValueError:
                parser.error('DUCKDB_FIXTURE_PORT must be an integer')
            if args.ray_port_base <= fixture_port <= args.ray_port_base + 99:
                parser.error('fixture port overlaps the verifier-owned Ray port range')
    extension = args.extension.resolve(strict=True)
    require_outside_git(args.output)
    if args.output.exists():
        raise FileExistsError('validation output already exists; preserve the earlier evidence')
    os.environ['DUCKDB_SEMLOOM_EXTENSION'] = str(extension)
    os.environ['DUCKDB_NATIVE_BACKEND'] = 'imlane' if is_imlane else args.backend
    os.environ['CUDA_VISIBLE_DEVICES'] = ''
    os.environ.setdefault('RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY', '2')
    ray = cluster = None
    ray_owned = False
    versions = dict(duckdb=None, daft=None, ray=None)
    report = dict(backend=args.backend, scheduler_owner=SCHEDULER_OWNERS[args.backend],
                  execution_role='diagnostic_reference' if args.backend == 'ray' else 'native_baseline',
                  imlane_runtime=IMLANE_MODES.get(args.backend),
                  imlane_case=(args.imlane_case or 'complete') if is_imlane else None,
                  runtime_lifecycle='caller owns Coordinator and author Ray' if is_imlane else 'verifier owns runtime',
                  extension_sha256=hashlib.sha256(extension.read_bytes()).hexdigest(),
                  versions=versions, real_model_requests=0, gpu_resources=0,
                  fixture_request_limit=4000, tests=0, failures=0, errors=0, skips=0,
                  total_http_requests=0, fixture_counts=[], failure_text=[], passed=False)
    if is_imlane:
        report.update(remote_http_stopped='unavailable', author_cleanup_required=None,
                      author_cleanup_confirmed=False, author_runtime_reusable=None,
                      unsupported=['remote request-start/stop causality and recovery after uncertain author work'])
    try:
        config = imlane_config(args.backend, args.imlane_config) if is_imlane else None
        if is_imlane:
            report['author_build_versions'] = author_build_versions(config)
            report['configured_imlane_limits'] = asdict(config.batch_limits)
        duckdb = importlib.import_module('duckdb')
        versions['duckdb'] = duckdb.__version__
        if duckdb.__version__ != '1.5.4':
            raise RuntimeError('compiled native validation requires DuckDB 1.5.4')
        if not is_imlane:
            daft = importlib.import_module('daft')
            versions['daft'] = daft.__version__
            if daft.__version__ != '0.7.21':
                raise RuntimeError('compiled native graph validation requires Daft 0.7.21')
            daft.set_runner_native(num_threads=8)
        if args.backend in ('ray', 'ray-data'):
            ray = importlib.import_module('ray')
            versions['ray'] = ray.__version__
            if ray.__version__ != '2.56.1' or ray.is_initialized():
                raise RuntimeError('native validation needs its own pinned Ray 2.56.1 connection')
            ray_owned = True
            report['owned_ray_started'] = False
            cluster = start_ray(ray, args.ray_temp.absolute(), args.ray_port_base)
            report['owned_ray_started'] = True
            from ray.data import DataContext
            DataContext.get_current().enable_progress_bars = False
            report['ray_node_ports'] = ray_node_ports(args.ray_port_base) if args.ray_port_base is not None else None
        suite = load_suite(config, args.imlane_case or 'complete')
        result = unittest.TextTestRunner(verbosity=2, resultclass=_Result).run(suite)
        http_requests = sum(item['total_http_requests'] for item in result.fixture_counts)
        report.update(tests=result.testsRun, failures=len(result.failures), errors=len(result.errors),
                      skips=len(result.skipped), fixture_counts=result.fixture_counts,
                      total_http_requests=http_requests,
                      passed=result.testsRun > 0 and result.wasSuccessful() and not result.skipped and http_requests <= 4000,
                      failure_text=[redact_text(text) for _, text in result.failures + result.errors])
        if is_imlane:
            closes = [item['graph_close_report'] for item in result.fixture_counts
                      if item['graph_close_report'] is not None]
            report.update(author_close_state(closes))
            report['unsupported'] += [reason for item in result.fixture_counts for reason in item['unsupported']]
    except Exception as error:
        report.update(passed=False, error_type=type(error).__name__, error=redact_text(str(error)),
                      error_notes=[redact_text(note) for note in getattr(error, '__notes__', ())])
    finally:
        if ray_owned:
            cleanup_errors = []
            try:
                ray.shutdown()
            except Exception as error:
                cleanup_errors.append(redact_text(str(error)))
            try:
                if cluster is not None:
                    cluster.shutdown()
            except Exception as error:
                cleanup_errors.append(redact_text(str(error)))
            report['owned_ray_cleanup'] = ('completed' if report.get('owned_ray_started') is True
                                           and not cleanup_errors else 'unconfirmed')
            if cleanup_errors:
                report.update(passed=False, cleanup_errors=cleanup_errors)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        write_private_json(args.output, report)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
