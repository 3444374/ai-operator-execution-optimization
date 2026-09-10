"""Opt-in real PG and local HTTP checks; fixture requests never invoke a model.

Requires SEMLOOM_TEST_PG_DSN, SEMLOOM_TEST_PG_LOG and SEMLOOM_TEST_ARTIFACT_ROOT
pointing to a caller-owned isolated PostgreSQL 18.3 cluster and private artifacts.
"""

from contextlib import suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import threading
import time
import unittest

from src.baselines.common.private_artifacts import content_digest, write_private_json
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.map_capacity_runner import CellConfig, run_cell
from src.experiments.postgresql.map_query_recording import record_pg_query
from tests.experiments import test_map_capacity_runner as fixtures


@unittest.skipUnless(all(os.environ.get(name) for name in (
    'SEMLOOM_TEST_PG_DSN', 'SEMLOOM_TEST_PG_LOG', 'SEMLOOM_TEST_ARTIFACT_ROOT'
)), 'requires an explicitly selected isolated PostgreSQL cluster')
class PgQueryExecutionIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.root = Path(os.environ['SEMLOOM_TEST_ARTIFACT_ROOT'])
        cls.root.mkdir(mode=0o700, exist_ok=False)
        cls.manifest = fixtures.CapacityTests().prepared()['manifests']['natural']
        # Distinct source IDs with an identical model payload exercise association.
        rows = cls.manifest['splits']['tuning'][:4]
        rows[2] = dict(rows[0], source_example_id='fixture-duplicate-payload')
        cls.manifest['splits']['tuning'] = rows
        cls.manifest['sha256'] = content_digest({k: v for k, v in cls.manifest.items() if k != 'sha256'})
        inputs = {row['input_text']: row for row in rows}
        cls.delay = 0.0
        cls.posts = 0
        lock = threading.Lock()

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                row = inputs[body['messages'][-1]['content']]
                with lock:
                    cls.posts += 1
                time.sleep(cls.delay or (.03 if row is inputs[rows[0]['input_text']] else .001))
                payload = json.dumps(dict(model='fixture-model', choices=[dict(
                    message={'content': 'fixture answer'}, finish_reason='stop')],
                    usage=dict(prompt_tokens=row['input_tokens'], completion_tokens=2))).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(payload)))
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                with suppress(BrokenPipeError, ConnectionResetError):
                    self.wfile.write(payload)

            def log_message(self, *_):
                pass

        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.worker = threading.Thread(target=cls.server.serve_forever)
        cls.worker.start()
        cls.addClassCleanup(cls.close_server)
        cls.model = cls.root / 'model.json'
        write_private_json(cls.model, dict(
            endpoint_url=f'http://127.0.0.1:{cls.server.server_port}/v1/chat/completions',
            model_id='fixture-model', timeout_ms=2000))
        cls.budget = AttemptBudget('fixture.query-execution', 36)
        cls.ledger = CellBudgetLedger.create(cls.root / 'budget.sqlite', cls.budget,
                                             deadline_utc=time.time() + 120)
        with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'], autocommit=True) as connection:
            if connection.info.server_version != 180003:
                raise RuntimeError('fixture requires PostgreSQL 18.3')
            connection.execute('CREATE EXTENSION IF NOT EXISTS semloom_pg')

    @classmethod
    def close_server(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.worker.join(3)
        if cls.worker.is_alive():
            raise RuntimeError('fixture server did not stop')
        write_private_json(cls.root / 'fixture-summary.json', dict(
            actual_model_requests=0, fixture_http_posts=cls.posts, server_stopped=True,
            budget=cls.ledger.snapshot()))

    def test_two_arms_and_independent_observer_modes(self):
        import psycopg
        for arm in ('pg', 'direct'):
            for content, mode in (('full', 'buffered'), ('compact', 'synchronous')):
                unit = arm[0] + content[0]
                with self.subTest(arm=arm, content=content, mode=mode), \
                     psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'], autocommit=True) as connection:
                    config = CellConfig(unit, arm, 'tuning', 4, 2, 2, 4, 4*1048576,
                                        2*1048576, 8388608, event_content=content, event_write_mode=mode)
                    result = run_cell(config, manifest=self.manifest, fixed_model_file=self.model,
                        budget_file=self.ledger.path, budget=self.budget, root=self.root / unit,
                        connection=connection, pg_log=Path(os.environ['SEMLOOM_TEST_PG_LOG']))
                    self.assertEqual(result['status'], 'passed')
                    self.assertEqual(result['actual_requests'], 8)
                    self.assertEqual(result['resource_accounting']['final_http_active'], 0)
                    self.assertEqual(result['observer']['event_content'], content)
                    self.assertEqual(result['observer']['event_write_mode'], mode)

    def test_direct_deadline_preserves_execution_failure_and_partial_evidence(self):
        self.__class__.delay = 1.0
        self.addCleanup(setattr, self.__class__, 'delay', 0.0)
        before_posts = self.posts
        config = CellConfig('timeout', 'direct', 'tuning', 4, 1, 2, 4, 4*1048576,
                            4*1048576, 8388608, statement_timeout_ms=500)
        with self.assertRaises(TimeoutError):
            run_cell(config, manifest=self.manifest, fixed_model_file=self.model,
                     budget_file=self.ledger.path, budget=self.budget, root=self.root / 'timeout')
        execution = json.loads((self.root / 'timeout/q0/execution.json').read_text())
        summary = json.loads((self.root / 'timeout/summary.json').read_text())
        self.assertEqual(execution['query_error']['type'], 'TimeoutError')
        self.assertIsNotNone(execution['t_cancel_triggered_ns'])
        self.assertEqual(summary['status'], 'failed')
        self.assertEqual(summary['evidence']['resource_state'], 'unknown')
        self.assertGreater(self.posts, before_posts, 'deadline must interrupt an actual fixture request')

    def test_pg_error_and_timeout_keep_the_original_sqlstate(self):
        import psycopg
        for name, statement, timeout, state in (
            ('sqlerr', "SELECT g::text,(10/(3-g))::text FROM generate_series(1,4) g", None, '22012'),
            ('pgtime', "SELECT 'wait',pg_sleep(.3)::text", .04, '57014'),
        ):
            with self.subTest(name=name), psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'], autocommit=True) as connection:
                with self.assertRaises(psycopg.Error) as error:
                    record_pg_query(connection, statement, self.root / name,
                                    max_rows=4, max_result_bytes=4096, query_timeout_s=timeout)
                self.assertEqual(error.exception.sqlstate, state)
                record = json.loads((self.root / name / 'execution.json').read_text())
                self.assertEqual(record['query_error']['sqlstate'], state)
                self.assertLessEqual(record['t_query_terminal_ns'], record['t_stream_cleanup_ns'])
