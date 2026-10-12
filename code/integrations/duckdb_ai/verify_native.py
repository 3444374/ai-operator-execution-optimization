#!/usr/bin/env python3
"""Validate compiled SQL with a pinned native execution graph and localhost fixtures."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import unittest

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))
from src.baselines.common.redact import redact_text


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
            self.fixture_counts.append(dict(test=test.id(), http_requests=len(server.records),
                                            total_http_requests=server.total_requests,
                                            peak_http_requests=server.peak, final_active=server.active,
                                            bridge_error=getattr(bridge, 'last_error', None),
                                            bridge_cleanup_error=getattr(bridge, 'last_cleanup_error', None),
                                            graph_close_report=getattr(graph, 'last_close_report', None),
                                            adapter_delivered_count=getattr(adapter, 'last_delivered_count', None),
                                            graph_events=getattr(test, 'graph_observations', [])))
        super().stopTest(test)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--extension', type=Path, required=True)
    parser.add_argument('--backend', choices=('daft', 'ray', 'ray-data'), required=True)
    parser.add_argument('--ray-temp', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    extension = args.extension.resolve(strict=True)
    os.environ['DUCKDB_SEMLOOM_EXTENSION'] = str(extension)
    os.environ['DUCKDB_NATIVE_BACKEND'] = args.backend
    os.environ['CUDA_VISIBLE_DEVICES'] = ''
    os.environ.setdefault('RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY', '2')
    ray = None
    import daft
    import duckdb
    if daft.__version__ != '0.7.21' or duckdb.__version__ != '1.5.4':
        raise RuntimeError('compiled native validation requires Daft 0.7.21 and DuckDB 1.5.4')
    daft.set_runner_native(num_threads=8)
    if args.backend in ('ray', 'ray-data'):
        if args.ray_temp is None or len(str(args.ray_temp.resolve()).encode()) > 40:
            parser.error('Ray execution requires --ray-temp with a short data-disk path')
        import ray
        if ray.__version__ != '2.56.1' or ray.is_initialized():
            raise RuntimeError('native validation needs its own pinned Ray 2.56.1 connection')
        ray.init(num_cpus=4, num_gpus=0, include_dashboard=False,
                 _node_ip_address='127.0.0.1', _temp_dir=str(args.ray_temp.resolve()),
                 object_store_memory=256 * 1024 * 1024,
                 runtime_env={'env_vars': {'PYTHONPATH': str(CODE), 'CUDA_VISIBLE_DEVICES': '',
                                            'OMP_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1'}})
        from ray.data import DataContext
        DataContext.get_current().enable_progress_bars = False
    try:
        from tests.semantic_methods.test_duckdb_native import DuckDBNativeGraphLibraryTests
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(DuckDBNativeGraphLibraryTests)
        result = unittest.TextTestRunner(verbosity=2, resultclass=_Result).run(suite)
        http_requests = sum(item['total_http_requests'] for item in result.fixture_counts)
        report = dict(backend=args.backend, scheduler_owner={'daft': 'daft_native', 'ray': 'ray_core',
                                                            'ray-data': 'ray_data'}[args.backend],
                      execution_role='diagnostic_reference' if args.backend == 'ray' else 'native_baseline',
                      extension_sha256=hashlib.sha256(extension.read_bytes()).hexdigest(),
                      versions=dict(duckdb=duckdb.__version__, daft=daft.__version__,
                                    ray=ray.__version__ if ray is not None else None),
                      tests=result.testsRun, failures=len(result.failures), errors=len(result.errors),
                      skips=len(result.skipped), real_model_requests=0, fixture_counts=result.fixture_counts,
                      total_http_requests=http_requests, fixture_request_limit=4000,
                      passed=result.wasSuccessful() and not result.skipped and http_requests <= 4000,
                      failure_text=[redact_text(text) for _, text in result.failures + result.errors])
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        return 0 if report['passed'] else 1
    finally:
        if ray is not None:
            ray.shutdown()


if __name__ == '__main__':
    raise SystemExit(main())
