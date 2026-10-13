#!/usr/bin/env python3
"""Run two bounded compiled SQL checks through actual Daft/Ray and local HTTP fixtures."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import unittest
from pathlib import Path

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))

from tests.semantic_methods.test_duckdb_ai import DuckDBNativeLibraryTests
from tests.semantic_methods.test_duckdb_native import _ExpandedFixture
from integrations.duckdb_ai.verify_native import _Result, start_ray
from src.baselines.common.private_artifacts import require_outside_git, write_private_json
from src.baselines.common.redact import redact_text
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.semantic_methods.duckdb_ai import DuckDBNativeTaskExecutor, DuckDBSemLoomBridge


class RayDuckDBChecks(DuckDBNativeLibraryTests):
    address = ''

    def setUp(self):
        super().setUp()
        import threading
        self.server.RequestHandlerClass = _ExpandedFixture
        self.server.total_requests = 0
        self.server.second_error_sent = threading.Event()
        self.events = []

    def common(self):
        timeout = self.connection.execute("SELECT current_setting('duckdb_ai_timeout_seconds')").fetchone()[0]
        config = FixedModelConfig(self.endpoint, 'fixture', timeout * 1000, bearer_token='EMPTY')
        physical = RayMapConfig(self.address, workers=1, batch_rows=2,
                                window_bytes=2 * 1024 * 1024, object_bytes=4 * 1024 * 1024)
        self.events = []
        self.execution = build_native_execution(config, physical=physical, max_tasks=4, max_active_requests=2,
                                               observer=self.events.append)
        self.adapter = DuckDBNativeTaskExecutor(self.execution, config)
        self.bridge = DuckDBSemLoomBridge(__import__('os').environ['DUCKDB_SEMLOOM_EXTENSION'], self.adapter)
        self.bridge.enable(self.connection)

    def test_fatal_preparation_keeps_the_valid_provider_prefix(self):
        expected = []
        for prompts in (['http-error', ''], ['ok', ''], ['', 'http-error']):
            before = self.server.total_requests
            with self.assertRaises(Exception) as error:
                self.select(prompts, try_complete=False)
            expected.append((type(error.exception), str(error.exception), self.server.total_requests - before))
        self.common()
        for prompts, (kind, message, posts) in zip(
                (['http-error', ''], ['ok', ''], ['', 'http-error']), expected):
            before = self.server.total_requests
            with self.subTest(prompts=prompts), self.assertRaises(kind) as error:
                self.select(prompts, try_complete=False)
            self.assertEqual(str(error.exception), message)
            self.assertEqual(self.server.total_requests - before, posts)

    def test_later_fast_failure_keeps_the_first_input_sql_error(self):
        self.connection.execute('SET duckdb_ai_max_concurrent_requests=4')
        self.connection.execute('SET duckdb_ai_timeout_seconds=20')
        prompts = ['slow-first-error', 'fast-second-error']
        with self.assertRaises(Exception) as control:
            self.select(prompts, try_complete=False)
        self.server.second_error_sent.clear()
        control_try = self.select(prompts)
        self.common()
        self.server.second_error_sent.clear()
        with self.assertRaises(type(control.exception)) as selected:
            self.select(prompts, try_complete=False)
        self.assertEqual(str(selected.exception), str(control.exception))
        self.server.second_error_sent.clear()
        self.assertEqual(self.select(prompts), control_try)

    def tearDown(self):
        super().tearDown()
        self.assertTrue(any(e['event'] == 'ray_block_put' for e in self.events))
        self.assertTrue(any(e['event'] == 'ray_http_completed' for e in self.events))
        self.assertTrue(any(e['event'] == 'ray_transport_closed' and e['confirmed'] for e in self.events))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ray-temp', type=Path, required=True)
    parser.add_argument('--ray-port-base', type=int)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    if not os.environ.get('DUCKDB_SEMLOOM_EXTENSION'):
        parser.error('requires DUCKDB_SEMLOOM_EXTENSION pointing to the compiled pinned extension')
    if args.output is not None:
        require_outside_git(args.output)
        if args.output.exists():
            raise FileExistsError('validation output already exists; preserve earlier evidence')
    if len(str(args.ray_temp.resolve()).encode()) > 40:
        parser.error('--ray-temp needs a short path for the Ray UNIX sockets')
    if hasattr(os, 'sched_getaffinity'):
        os.sched_setaffinity(0, set(sorted(os.sched_getaffinity(0))[:4]))
    import ray
    import daft
    import duckdb
    if (ray.__version__, daft.__version__, duckdb.__version__) != ('2.56.1', '0.7.21', '1.5.4'):
        raise RuntimeError('SemLoom SQL validation requires the pinned Ray, Daft and DuckDB versions')
    os.environ['CUDA_VISIBLE_DEVICES'] = ''
    daft.set_runner_native(num_threads=8)
    cluster = start_ray(ray, args.ray_temp.absolute(), args.ray_port_base)
    RayDuckDBChecks.address = ray.get_runtime_context().gcs_address
    try:
        suite = unittest.TestSuite(RayDuckDBChecks(name) for name in (
            'test_compiled_batch_bypasses_native_one_worker_and_guard',
            'test_patched_native_and_semloom_use_identical_complete_messages_and_return',
            'test_fatal_preparation_keeps_the_valid_provider_prefix',
            'test_later_fast_failure_keeps_the_first_input_sql_error',
        ))
        result = unittest.TextTestRunner(verbosity=2, resultclass=_Result).run(suite)
        posts = sum(row['total_http_requests'] for row in result.fixture_counts)
        report = dict(scheduler_owner='SemLoom Daft/Ray', tests=result.testsRun,
            failures=len(result.failures), errors=len(result.errors), skips=len(result.skipped),
            fixture_counts=result.fixture_counts, total_http_requests=posts, fixture_request_limit=4000,
            versions=dict(ray=ray.__version__, daft=daft.__version__, duckdb=duckdb.__version__),
            extension_sha256=hashlib.sha256(Path(os.environ['DUCKDB_SEMLOOM_EXTENSION']).read_bytes()).hexdigest(),
            failure_text=[redact_text(text) for _, text in result.failures + result.errors],
            real_model_requests=0, gpu_resources=0,
            passed=result.wasSuccessful() and not result.skipped and posts <= 4000)
        if args.output is not None:
            write_private_json(args.output, report)
        return 0 if report['passed'] else 1
    finally:
        ray.shutdown()
        if cluster is not None:
            cluster.shutdown()


if __name__ == '__main__':
    raise SystemExit(main())
