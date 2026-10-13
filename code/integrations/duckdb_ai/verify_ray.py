#!/usr/bin/env python3
"""Run bounded compiled SQL checks through actual Daft/Ray and local HTTP fixtures."""
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import sys
import unittest
from pathlib import Path

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))

from tests.semantic_methods.test_duckdb_ai import DuckDBNativeLibraryTests
from tests.semantic_methods.test_duckdb_native import _ExpandedFixture
from integrations.duckdb_ai.verify_native import _Result, ray_node_ports, start_ray
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
        self.connection.execute('SET duckdb_ai_connect_timeout_seconds=5')

    def common(self):
        timeout = self.connection.execute("SELECT current_setting('duckdb_ai_timeout_seconds')").fetchone()[0]
        config = FixedModelConfig(self.endpoint, 'fixture', timeout * 1000, bearer_token='EMPTY')
        physical = RayMapConfig(self.address, workers=1, batch_rows=2,
                                window_bytes=2 * 1024 * 1024 + 24, object_bytes=4 * 1024 * 1024)
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
        self.connection.execute('SET duckdb_ai_connect_timeout_seconds=20')
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
    if len(str(args.ray_temp.absolute()).encode()) > 40:
        parser.error('--ray-temp needs a short path for the Ray UNIX sockets')
    try:
        fixture_port = int(os.environ.get('DUCKDB_FIXTURE_PORT', '0'))
    except ValueError:
        parser.error('DUCKDB_FIXTURE_PORT must be an integer')
    if fixture_port != 0 and not 1024 <= fixture_port <= 65535:
        parser.error('DUCKDB_FIXTURE_PORT must be zero or between 1024 and 65535')
    if args.ray_port_base is not None:
        try:
            ray_node_ports(args.ray_port_base)
        except ValueError as error:
            parser.error(str(error))
        if args.ray_port_base <= fixture_port <= args.ray_port_base + 99:
            parser.error('fixture port overlaps the verifier-owned Ray port range')
    if hasattr(os, 'sched_getaffinity'):
        os.sched_setaffinity(0, set(sorted(os.sched_getaffinity(0))[:4]))
    os.environ['CUDA_VISIBLE_DEVICES'] = ''
    ray = cluster = None
    ray_owned = False
    failure = None
    versions = dict(ray=None, daft=None, duckdb=None)
    report = dict(scheduler_owner='SemLoom Daft/Ray', tests=0, failures=0, errors=0, skips=0,
        fixture_counts=[], total_http_requests=0, fixture_request_limit=4000, versions=versions,
        extension_sha256=None, failure_text=[], real_model_requests=0, gpu_resources=0,
        owned_ray_started=False, owned_ray_cleanup='not_started', cleanup_errors=[], passed=False)
    try:
        extension = Path(os.environ['DUCKDB_SEMLOOM_EXTENSION']).resolve(strict=True)
        report['extension_sha256'] = hashlib.sha256(extension.read_bytes()).hexdigest()
        ray = importlib.import_module('ray')
        daft = importlib.import_module('daft')
        duckdb = importlib.import_module('duckdb')
        versions.update(ray=ray.__version__, daft=daft.__version__, duckdb=duckdb.__version__)
        if (ray.__version__, daft.__version__, duckdb.__version__) != ('2.56.1', '0.7.21', '1.5.4'):
            raise RuntimeError('SemLoom SQL validation requires the pinned Ray, Daft and DuckDB versions')
        if ray.is_initialized():
            raise RuntimeError('SemLoom validation requires its own fresh Ray connection')
        from src.infrastructure.daft_runtime import configure_daft_runner
        configure_daft_runner('native', num_threads=8)
        ray_owned = True
        cluster = start_ray(ray, args.ray_temp.absolute(), args.ray_port_base)
        report['owned_ray_started'] = True
        RayDuckDBChecks.address = ray.get_runtime_context().gcs_address
        suite = unittest.TestSuite(RayDuckDBChecks(name) for name in (
            'test_compiled_batch_bypasses_native_one_worker_and_guard',
            'test_patched_native_and_semloom_use_identical_complete_messages_and_return',
            'test_fatal_preparation_keeps_the_valid_provider_prefix',
            'test_later_fast_failure_keeps_the_first_input_sql_error',
        ))
        result = unittest.TextTestRunner(verbosity=2, resultclass=_Result).run(suite)
        posts = sum(row['total_http_requests'] for row in result.fixture_counts)
        report.update(tests=result.testsRun,
            failures=len(result.failures), errors=len(result.errors), skips=len(result.skipped),
            fixture_counts=result.fixture_counts, total_http_requests=posts, fixture_request_limit=4000,
            failure_text=[redact_text(text) for _, text in result.failures + result.errors],
            passed=result.testsRun > 0 and result.wasSuccessful() and not result.skipped and posts <= 4000)
    except BaseException as error:
        failure = error
        report.update(passed=False, error_type=type(error).__name__, error=redact_text(str(error)),
                      error_notes=[redact_text(note) for note in getattr(error, '__notes__', ())])
    finally:
        if ray_owned:
            for component, action in (('ray', ray.shutdown),
                                      ('cluster', None if cluster is None else cluster.shutdown)):
                if action is None:
                    continue
                try:
                    action()
                except BaseException as error:
                    report['cleanup_errors'].append(dict(component=component, error_type=type(error).__name__,
                        error=redact_text(str(error)),
                        error_notes=[redact_text(note) for note in getattr(error, '__notes__', ())]))
            report['owned_ray_cleanup'] = ('completed' if report['owned_ray_started']
                                           and not report['cleanup_errors'] else 'unconfirmed')
            if report['cleanup_errors']:
                report['passed'] = False
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            write_private_json(args.output, report)
        else:
            print(json.dumps(report, indent=2))
    if failure is not None and not isinstance(failure, Exception):
        raise failure
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
