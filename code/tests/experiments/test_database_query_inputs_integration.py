"""Opt-in real PG raw-source/Map comparison against one local HTTP fixture."""
import asyncio
from collections import Counter
from contextlib import ExitStack, suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import hashlib
import os
from pathlib import Path
import sys
import threading
import time
import unittest

from src.baselines.common.private_artifacts import write_private_json, content_digest
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.postgresql.query_inputs import QueryInputs
from src.experiments.postgresql.query_tables import install_input_table, map_statement
from src.experiments.postgresql.pg_source_direct import PgSourceDirectMap
from src.experiments.postgresql.map_query_recording import record_async_execution, record_pg_query
from src.experiments.postgresql.runtime_helpers import owned_child_process, wait_for_path


@unittest.skipUnless(os.environ.get('SEMLOOM_TEST_PG_DSN') and os.environ.get('SEMLOOM_TEST_ARTIFACT_ROOT'),
                     'requires an isolated PG18.3 cluster and new private artifact root')
class DatabaseInputsIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.stack = ExitStack()
        cls.addClassCleanup(cls.stack.close)
        cls.root = Path(os.environ['SEMLOOM_TEST_ARTIFACT_ROOT'])
        cls.root.mkdir(mode=0o700, exist_ok=False)
        cls.bodies, cls.active = [], 0
        cls.lock = threading.Lock()
        cls.delay = 0

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                with cls.lock:
                    cls.bodies.append(body)
                    cls.active += 1
                try:
                    text = body['messages'][-1]['content']
                    time.sleep(cls.delay or (.03 if text == 'slow' else .001))
                    payload = json.dumps(dict(model='fixture-model', choices=[dict(
                        message={'content':'answer:'+text}, finish_reason='stop')],
                        usage=dict(prompt_tokens=1, completion_tokens=2))).encode()
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(payload)))
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    with suppress(BrokenPipeError, ConnectionResetError):
                        self.wfile.write(payload)
                finally:
                    with cls.lock:
                        cls.active -= 1

            def log_message(self, *_):
                pass

        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.worker = threading.Thread(target=cls.server.serve_forever)
        cls.worker.start()
        cls.stack.callback(cls.close_server)
        cls.model_path = cls.root/'model.json'
        write_private_json(cls.model_path, dict(endpoint_url=f'http://127.0.0.1:{cls.server.server_port}/v1/chat/completions',
                                               model_id='fixture-model', timeout_ms=3000))
        cls.plan = SemanticMapPlan('Echo the review.', 'fixture-model', 128)
        table = 'query_raw_'+hashlib.sha256(str(cls.root).encode()).hexdigest()[:16]
        cls.inputs = QueryInputs('movie', table, 3, movie_id='chosen')
        cls.connection = cls.stack.enter_context(psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'], autocommit=True))
        cls.connection.execute('CREATE EXTENSION IF NOT EXISTS semloom_pg')
        manifest = install_input_table(cls.connection, cls.inputs, [
            (0,'a','chosen','slow'), (1,'b','chosen','fast'), (2,'c','other','forbidden')])
        write_private_json(cls.root/'source.json', manifest)
        cls.socket = cls.root/'gateway.sock'
        code = str(Path(__file__).resolve().parents[2])
        command = [sys.executable,'-m','src.experiments.choice_gateway_observer','--fixture-only',
                   '--events',str(cls.root/'gateway-events.jsonl'),'--','--socket',str(cls.socket),
                   '--fixed-model-config',str(cls.model_path),'--incremental-map','--max-held-tasks','4',
                   '--max-active-requests','2']
        process = cls.stack.enter_context(owned_child_process(command, cls.root, 'gateway',
                                    dict(os.environ, PYTHONPATH=code), None))
        wait_for_path(cls.socket, process)
        for key,value in {'semloom_pg.gateway_socket':str(cls.socket),
            'semloom_pg.provider_execution_profile':'incremental-map',
            'semloom_pg.enable_predicate_prefetch':'on','statement_timeout':'5000'}.items():
            cls.connection.execute('SELECT set_config(%s,%s,false)', (key,value))

    @classmethod
    def close_server(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.worker.join(3)
        until = time.monotonic()+4
        while cls.active and time.monotonic()<until:
            time.sleep(.01)
        write_private_json(cls.root/'fixture-summary.json',dict(actual_model_posts=0, fixture_http_posts=len(cls.bodies),
                          active_http=cls.active, listener_closed=not cls.worker.is_alive()))
        if cls.active or cls.worker.is_alive():
            raise RuntimeError('fixture did not settle')

    def test_same_raw_relation_and_messages_in_pg_and_direct(self):
        before = len(self.bodies)
        pg = record_pg_query(self.connection, map_statement(self.inputs, self.plan), self.root/'pg',
                             max_rows=3, max_result_bytes=4096)
        middle = len(self.bodies)
        direct = asyncio.run(self.run_direct('direct'))
        self.assertEqual(Counter(map(content_digest,self.bodies[before:middle])),
                         Counter(map(content_digest,self.bodies[middle:])))
        self.assertEqual(pg['recorded_rows'], 2)
        self.assertEqual(direct['recorded_rows'], 2)
        self.assertEqual(self.metrics['rows'], 2)
        self.assertEqual(self.metrics['peak_pending'], 2)
        self.assertTrue(all('forbidden' not in json.dumps(body) for body in self.bodies[before:]))
        rows = lambda name: [json.loads(line)['row'] for line in (self.root/name/'results.jsonl').read_text().splitlines()]
        self.assertEqual(sorted(rows('pg')), sorted(rows('direct')))

    async def run_direct(self, name, timeout=5):
        import psycopg
        direct = PgSourceDirectMap(load_fixed_model_config(self.model_path), 2, self.plan, lambda _:None)
        try:
            async with await psycopg.AsyncConnection.connect(os.environ['SEMLOOM_TEST_PG_DSN'], autocommit=True) as connection:
                result = await record_async_execution(self.root/name, lambda:direct.query(connection,self.inputs,1),
                                        max_rows=3,max_result_bytes=4096,query_timeout_s=timeout)
                self.assertEqual(int(connection.info.transaction_status), 0)
                self.metrics = direct.source_metrics
                return result
        finally:
            await direct.close()

    def test_deadline_interrupts_raw_read_and_http_without_replacing_reason(self):
        self.__class__.delay = 1
        try:
            with self.assertRaises(TimeoutError):
                asyncio.run(self.run_direct('timeout', .2))
        finally:
            self.__class__.delay = 0
        value = json.loads((self.root/'timeout/execution.json').read_text())
        self.assertEqual(value['query_error']['type'], 'TimeoutError')
        self.assertIsNotNone(value['t_cancel_triggered_ns'])

    def test_source_is_immutable_and_labels_are_absent(self):
        import psycopg
        with self.assertRaisesRegex(psycopg.Error, 'immutable benchmark input'):
            self.connection.execute(f"UPDATE {self.inputs.table} SET review_text='changed'")
        columns = self.connection.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s", (self.inputs.table,)).fetchall()
        self.assertEqual({row[0] for row in columns},{'source_position','row_id','movie_id','review_text','review_id'})

    @unittest.skipUnless(os.environ.get('SEMLOOM_TEST_RAY_TMPDIR'), 'requires an explicitly owned Ray temporary root')
    def test_native_ray_sql_http_processor(self):
        import psycopg
        import ray
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        from src.experiments.native_http_observer import NativeSessionFactory
        from src.baselines.text.frameworks.ray_data_pg_http import open_rows, RaySqlHttpConfig
        from src.experiments.postgresql.map_query_recording import record_execution
        if ray.is_initialized():
            raise RuntimeError('fixture must not change an existing Ray runtime')
        budget = CellBudgetLedger.create(self.root/'ray-budget.sqlite',AttemptBudget('ray-fixture',2),
                                         deadline_utc=time.time()+120)
        budget.reserve_unit('query',2)
        shared = budget.claim_shared_unit('query')
        events = self.root/'ray-http-events'
        events.mkdir()
        factory = NativeSessionFactory(shared,str(events),load_fixed_model_config(self.model_path).endpoint_url,5)
        dsn = os.environ['SEMLOOM_TEST_PG_DSN']
        def connection_factory():
            return psycopg.connect(dsn, options='-c default_transaction_read_only=on -c statement_timeout=5000')
        before = len(self.bodies)
        ray.init(num_cpus=4,num_gpus=0,include_dashboard=False,_node_ip_address='127.0.0.1',
                 object_store_memory=134217728,_temp_dir=os.environ['SEMLOOM_TEST_RAY_TMPDIR'])
        try:
            result = record_execution(self.root/'ray', lambda:open_rows(self.inputs,self.plan,
                RaySqlHttpConfig(2,2,2,2),connection_factory,factory),max_rows=3,max_result_bytes=4096)
            self.assertEqual(result['recorded_rows'],2)
            self.assertEqual(shared.attempts,2)
            self.assertEqual(len(self.bodies)-before,2)
            rows = [json.loads(line)['row'] for line in (self.root/'ray/results.jsonl').read_text().splitlines()]
            self.assertEqual(sorted(rows),[['a','answer:slow'],['b','answer:fast']])
            observed = [json.loads(line) for file in events.glob('*.jsonl') for line in file.read_text().splitlines()]
            self.assertEqual(sum(e['event']=='request' for e in observed),2)
            self.assertEqual(sum(e['event']=='http_finished' for e in observed),2)
        finally:
            ray.shutdown()
