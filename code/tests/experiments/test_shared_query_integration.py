"""Real PostgreSQL queries share one gateway; HTTP is a bounded local fixture."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest

from src.baselines.common.private_artifacts import write_private_json
from src.experiments.postgresql.map_bindings import parse_pg_bindings, verify_bound_map_results
from src.execution_provider.semantic_map import SemanticMapPlan


def wait_for(predicate, seconds=10):
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError('shared query condition timed out')
        time.sleep(.01)


@unittest.skipUnless(all(os.environ.get(k) for k in (
    'SEMLOOM_TEST_PG_DSN', 'SEMLOOM_TEST_PG_LOG', 'SEMLOOM_TEST_SHARED_QUERY_ROOT')),
    'requires an isolated PG and fresh shared-query artifact root')
class SharedQueryIntegrationTests(unittest.TestCase):
    def test_shared_queries_dependencies_and_lifecycle(self):
        import psycopg

        root = Path(os.environ['SEMLOOM_TEST_SHARED_QUERY_ROOT'])
        root.mkdir(mode=0o700)
        lock, hold = threading.Lock(), threading.Event()
        posts, queries = [], []
        active = 0
        peak = 0
        started_at = time.monotonic()

        class Handler(BaseHTTPRequestHandler):
            def do_POST(handler):
                nonlocal active, peak
                request = json.loads(handler.rfile.read(int(handler.headers['Content-Length'])))
                with lock:
                    permitted = len(posts) < 256 and time.monotonic() - started_at < 120
                    row = dict(start=time.monotonic(), request=request)
                    posts.append(row)
                    active += 1
                    peak = max(peak, active)
                try:
                    if not permitted:
                        handler.send_error(429, 'fixture request or time budget exhausted')
                        return
                    while hold.is_set() and time.monotonic() - started_at < 120:
                        time.sleep(.005)
                    time.sleep(.03)
                    text = request['messages'][-1]['content']
                    tokens = request.get('max_tokens', request.get('max_completion_tokens'))
                    if tokens not in (8, 128):
                        handler.send_error(400, 'undeclared generation limit')
                        return
                    output = ('TRUE' if '[KEEP]' in text else 'FALSE') if tokens == 8 else text
                    body = json.dumps(dict(model='fixture-model', choices=[dict(
                        message=dict(content=output), finish_reason='stop')],
                        usage=dict(prompt_tokens=10, completion_tokens=1))).encode()
                    handler.send_response(200)
                    handler.send_header('Content-Length', str(len(body)))
                    handler.send_header('Content-Type', 'application/json')
                    handler.end_headers()
                    with suppress(BrokenPipeError, ConnectionResetError):
                        handler.wfile.write(body)
                finally:
                    with lock:
                        active -= 1
                        row['end'] = time.monotonic()

            def log_message(self, *_):
                pass

        http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        http_thread = threading.Thread(target=http.serve_forever)
        http_thread.start()
        gateway = None
        connections = []
        log = (root / 'gateway.log').open('w')
        events_file, sessions_file = root / 'events.jsonl', root / 'sessions.jsonl'
        socket_path = root / 'gateway.sock'
        config = root / 'model.json'
        write_private_json(config, dict(endpoint_url=f'http://127.0.0.1:{http.server_port}/v1/chat/completions',
            model_id='fixture-model', timeout_ms=5000))
        command = [sys.executable, '-m', 'src.experiments.choice_gateway_observer', '--fixture-only',
            '--events', str(events_file), '--session-events', str(sessions_file), '--',
            '--socket', str(socket_path), '--fixed-model-config', str(config), '--incremental-map',
            '--job-compute-policy', 'shared', '--max-active-jobs', '4', '--max-connections', '16',
            '--max-active-requests', '4', '--max-held-tasks', '32', '--frame-timeout-ms', '500']
        write_private_json(root / 'contract.json', dict(actual_model=False, max_posts=256,
            max_seconds=120, queries=4, requests=4, held_tasks=32, query_window=4))

        def events():
            if not events_file.exists():
                return []
            # The observer writes one complete line under its record lock.
            return [json.loads(line) for line in events_file.read_text().splitlines() if line.endswith('}')]

        def live_jobs():
            counts = Counter(e['event'] for e in events())
            return counts['core_job_opened'] - counts['core_job_drained']

        def connect():
            conn = psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'], autocommit=True)
            connections.append(conn)
            conn.execute("SELECT set_config('semloom_pg.gateway_socket', %s, false)", (str(socket_path),))
            conn.execute("SET statement_timeout='10s'; SET semloom_pg.provider_execution_profile='query-job';"
                "SET semloom_pg.enable_query_job_window=on; SET semloom_pg.provider_window_tasks=4;"
                "SET semloom_pg.enable_total_window_budget=on; SET semloom_pg.test_map_binding_id_column='id'")
            return conn

        map_sql = "ai_semantic.map(body,'Echo.', '{\"model\":\"fixture-model\",\"temperature\":0,\"max_tokens\":128}'::jsonb)"
        filter_sql = "ai_semantic.filter(body,'Keep inputs containing [KEEP].', '{\"model\":\"fixture-model\",\"temperature\":0,\"max_tokens\":8}'::jsonb)"
        select = f'SELECT id,{map_sql} FROM ONLY shared_query_inputs'
        values = [(i, ('[KEEP]' if i % 2 == 0 else '[DROP]') + f' row {i}') for i in range(8)]

        def run(label, *, dependent=False, delay=0, barrier=None, conn=None):
            conn = conn or connect()
            entry = dict(label=label, backend_pid=conn.info.backend_pid, dependent=dependent,
                         release=time.monotonic() + delay)
            queries.append(entry)
            try:
                if barrier:
                    barrier.wait(5)
                time.sleep(delay)
                entry['start'] = time.monotonic()
                rows = conn.execute(select + (f' WHERE {filter_sql}' if dependent else '')).fetchall()
                entry['rows'] = rows
                self.assertEqual(rows, values[::2] if dependent else values)
                entry['status'] = 'completed'
                return rows
            except psycopg.errors.QueryCanceled:
                entry['status'] = 'cancelled'
                raise
            finally:
                entry['end'] = time.monotonic()
                conn.close()

        try:
            gateway = subprocess.Popen(command, stdout=log, stderr=log)
            wait_for(lambda: socket_path.exists() or gateway.poll() is not None)
            self.assertIsNone(gateway.poll(), (root / 'gateway.log').read_text())
            with connect() as conn:
                conn.execute('CREATE EXTENSION IF NOT EXISTS semloom_pg')
                conn.execute('CREATE TABLE shared_query_inputs(id integer PRIMARY KEY, body text)')
                with conn.cursor() as cursor:
                    cursor.executemany('INSERT INTO shared_query_inputs VALUES (%s,%s)', values)
                self.assertEqual(conn.execute(select + ' LIMIT 0').fetchall(), [])
            self.assertEqual(live_jobs(), 0)

            for count, dependent, stagger in ((2, False, False), (4, False, False),
                                               (2, False, True), (4, True, False)):
                barrier = threading.Barrier(count)
                before = len(posts)
                with ThreadPoolExecutor(count) as pool:
                    futures = [pool.submit(run, f'parallel-{count}-{dependent}-{stagger}-{i}',
                        dependent=dependent, delay=i * .1 if stagger else 0, barrier=barrier)
                        for i in range(count)]
                    for future in futures:
                        future.result(15)
                wait_for(lambda: live_jobs() == 0)
                self.assertEqual(len(posts) - before, count * (12 if dependent else 8))

            paused = []
            for index in range(3):
                conn = connect()
                conn.execute('BEGIN')
                cursor = conn.cursor(name=f'paused_{index}')
                cursor.execute(select)
                self.assertEqual(cursor.fetchmany(1), values[:1])
                paused.append((conn, cursor))
            wait_for(lambda: active == 0)
            self.assertEqual(live_jobs(), 3)
            # Three real query Jobs remain idle; the fourth can use every compute slot.
            hold.set()
            with ThreadPoolExecutor(1) as pool:
                future = pool.submit(run, 'idle-neighbours')
                wait_for(lambda: active == 4)
                self.assertEqual(live_jobs(), 4)
                with connect() as rejected:
                    with self.assertRaises(psycopg.Error):
                        rejected.execute(select).fetchall()
                hold.clear()
                future.result(15)
            time.sleep(.6)  # Exceeds the communication frame timeout while cursors are idle.
            for conn, cursor in paused:
                self.assertEqual(cursor.fetchall(), values[1:])
                cursor.close()
                conn.execute('COMMIT')
                conn.close()
            wait_for(lambda: live_jobs() == 0)

            cancelled = connect()
            hold.set()
            with ThreadPoolExecutor(2) as pool:
                victim = pool.submit(run, 'cancelled', conn=cancelled)
                wait_for(lambda: active == 4)
                survivor = pool.submit(run, 'cancel-survivor')
                cancelled.cancel()
                with self.assertRaises(psycopg.errors.QueryCanceled):
                    victim.result(5)
                hold.clear()
                survivor.result(15)
            wait_for(lambda: live_jobs() == 0)

            for index in range(7):
                with connect() as conn:
                    self.assertEqual(conn.execute(select + ' LIMIT 1').fetchall(), values[:1])
                wait_for(lambda: live_jobs() == 0)

            # Reuse independent PG before-offer bindings and socket peer identity.
            all_events = events()
            flow_jobs = {e['engine_session_id']: e['job_id'] for e in all_events
                         if e['event'] == 'core_query_flow_joined'}
            running = set()
            for event in all_events:
                if event['event'] in ('core_submitted', 'core_terminal'):
                    key = (event['key']['session_id'], event['key']['sequence'])
                    self.assertIn(key[0], flow_jobs)
                    if event['event'] == 'core_submitted':
                        self.assertNotIn(key, running)
                        running.add(key)
                    else:
                        self.assertIn(key, running)
                        running.remove(key)
                    self.assertLessEqual(len(running), 4)
                    self.assertEqual(event['usage']['active_requests'], len(running))
                if 'usage' in event:
                    self.assertLessEqual(event['usage']['held_tasks'], 32)
                if event['event'] == 'core_job_drained':
                    self.assertTrue(all(v == 0 for v in event['usage'].values()))
            self.assertFalse(running)
            sessions = [json.loads(line) for line in sessions_file.read_text().splitlines()]
            bindings = parse_pg_bindings(Path(os.environ['SEMLOOM_TEST_PG_LOG']).read_text().splitlines())
            audits = []
            for query in queries:
                if query['status'] != 'completed':
                    continue
                pids = {s['session_id'] for s in sessions if s.get('event') == 'session_start'
                        and s.get('peer_pid') == query['backend_pid']}
                selected = [e for e in all_events if e.get('session_id') in pids
                            and e.get('event') in ('core_map_task', 'core_map_completion')]
                job_ids = {e['job_id'] for e in all_events if e.get('session_id') in pids
                           and e.get('event') in ('core_map_task', 'core_filter_task')}
                self.assertEqual(len(job_ids), 1)
                expected = values[::2] if query['dependent'] else values
                audits.append(verify_bound_map_results(
                    [(str(i), text) for i, text in expected],
                    [(str(i), text) for i, text in query['rows']],
                    [b for b in bindings if b.backend_pid == query['backend_pid']], selected, sessions,
                    plan=SemanticMapPlan('Echo.', 'fixture-model', 128)))
            self.assertEqual(peak, 4)
            self.assertLessEqual(len(posts), 256)
            write_private_json(root / 'audit.json', dict(status='passed', peak_http=peak,
                fixture_posts=len(posts), actual_model_posts=0, live_jobs=live_jobs(), bindings=audits))
        finally:
            hold.clear()
            for conn in connections:
                conn.close()
            if gateway is not None and gateway.poll() is None:
                gateway.terminate()
                try:
                    gateway.wait(12)
                except subprocess.TimeoutExpired:
                    gateway.kill()
                    gateway.wait(3)
            http.shutdown()
            http.server_close()
            http_thread.join(3)
            log.close()
            write_private_json(root / 'queries.json', queries)
            write_private_json(root / 'http.json', posts)
            write_private_json(root / 'cleanup.json', dict(active_http=active,
                http_thread_alive=http_thread.is_alive(), gateway_returncode=gateway.poll() if gateway else None))
            self.assertEqual(active, 0)
            self.assertFalse(http_thread.is_alive())
