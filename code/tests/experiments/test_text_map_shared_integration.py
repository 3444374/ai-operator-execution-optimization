"""Four query owners on isolated PG/Ray with a local, finite HTTP fixture."""
from contextlib import suppress
from dataclasses import asdict, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import threading
import time
import unittest

from src.baselines.common.private_artifacts import write_private_json
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.shared_request_budget import SharedClaimedUnit
from src.experiments.postgresql.query_config import QueryConfig
from src.experiments.postgresql.query_inputs import QueryInputs
from src.experiments.postgresql.query_supervisor import supervise
from src.experiments.postgresql.query_tables import install_input_table
from src.experiments.postgresql.query_workloads import prepare, read_prepared
from src.experiments.postgresql.text_map_comparison import ROLES, MAIN_ROLES, SCHEMA, read_run, summarize
from src.experiments.postgresql.text_map_campaign import SCHEMA as CAMPAIGN_SCHEMA, run_stage


# These pinned upstream actors belong to the cluster, not to one query driver.
RAY_SERVICE_ACTORS = {'datasets_stats_actor': '_StatsActor',
    'AutoscalingCoordinator': '_AutoscalingCoordinatorActor',
    'ActorLocationTracker': 'ActorLocationTracker', 'llm_batch_telemetry': '_TelemetryAgent'}


def wait_until(predicate, seconds=30):
    end = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= end:
            raise AssertionError('controlled lifecycle check exceeded its deadline')
        time.sleep(.05)


def reference(path):
    return dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())


@unittest.skipUnless(all(os.environ.get(k) for k in (
    'SEMLOOM_TEST_PG_DSN', 'SEMLOOM_TEST_PG_LOG', 'SEMLOOM_TEST_RAY_ADDRESS',
    'SEMLOOM_TEST_TEXT_MAP_ROOT')), 'requires isolated PG18.3, a caller-owned Ray cluster and fresh artifacts')
class SharedRuntimeComparisonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        import ray
        from ray._private.state import actors
        cls.root = Path(os.environ['SEMLOOM_TEST_TEXT_MAP_ROOT'])
        cls.root.mkdir(mode=0o700)
        cls.ray = ray
        if not ray.is_initialized() or ray.__version__ != '2.56.1':
            raise RuntimeError('the controller must own the pinned Ray runtime')
        if ray.get_runtime_context().gcs_address != os.environ['SEMLOOM_TEST_RAY_ADDRESS']:
            raise RuntimeError('fixture Ray address differs from the controller')
        cls.actors = staticmethod(actors)
        cls.base_actors = {key for key, value in actors().items() if value['State'] != 'DEAD'}
        cls.cpu_count = int(ray.cluster_resources()['CPU'])
        cls.object_bytes = int(ray.cluster_resources()['object_store_memory'])
        cls.lock, cls.release, cls.started_request = threading.Lock(), threading.Event(), threading.Event()
        cls.release.set()
        cls.mode, cls.active, cls.posts, cls.sequence = 'normal', 0, [], 0
        cls.cleanups = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(handler):
                body = json.loads(handler.rfile.read(int(handler.headers['Content-Length'])))
                with cls.lock:
                    permitted = len(cls.posts) < 512
                    mode = cls.mode
                    cls.posts.append(dict(mode=mode, body=body, started_ns=time.monotonic_ns()))
                    cls.active += 1
                try:
                    cls.started_request.set()
                    if not permitted:
                        handler.send_error(429, 'finite fixture allowance exhausted')
                        return
                    if mode == 'error':
                        handler.send_error(500, 'controlled fixture failure')
                        return
                    if mode == 'disconnect':
                        handler.connection.shutdown(socket.SHUT_RDWR)
                        handler.connection.close()
                        return
                    if mode == 'hold' and not cls.release.wait(45):
                        handler.send_error(504, 'controlled hold expired')
                        return
                    if mode == 'delay':
                        time.sleep(.2)
                    output = 'POSITIVE' if '[GOOD]' in body['messages'][-1]['content'] else 'NEGATIVE'
                    response = json.dumps(dict(model='fixture-model',
                        choices=[dict(message=dict(content=output), finish_reason='stop')],
                        usage=dict(prompt_tokens=10, completion_tokens=1))).encode()
                    handler.send_response(200)
                    handler.send_header('Content-Type', 'application/json')
                    handler.send_header('Content-Length', str(len(response)))
                    handler.end_headers()
                    with suppress(BrokenPipeError, ConnectionResetError):
                        handler.wfile.write(response)
                finally:
                    with cls.lock:
                        cls.active -= 1

            def log_message(self, *_):
                pass

        cls.http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.thread = threading.Thread(target=cls.http.serve_forever)
        cls.thread.start()
        cls.addClassCleanup(cls.stop_fixture)
        cls.model = cls.root/'model.json'
        write_private_json(cls.model, dict(endpoint_url=f'http://127.0.0.1:{cls.http.server_port}/v1/chat/completions',
                                          model_id='fixture-model', timeout_ms=30000))
        cls.budget = AttemptBudget('text-map.shared-fixture', 512)
        cls.ledger = CellBudgetLedger.create(cls.root/'budget.sqlite', cls.budget, deadline_utc=time.time()+900)
        cls.transport = cls.root/'transport.json'
        write_private_json(cls.transport, dict(address=os.environ['SEMLOOM_TEST_RAY_ADDRESS'], workers=1,
            batch_rows=4, window_bytes=2*1048576, object_bytes=4*1048576))
        cls.table = 'text_map_'+hashlib.sha256(str(cls.root).encode()).hexdigest()[:12]
        prepare(cls.root/'inputs', 'movie',
            ((str(i), 'fixture-movie', ('[GOOD] 重复评论' if i % 2 == 0 else '[BAD] review'),
              'POSITIVE' if i % 2 == 0 else 'NEGATIVE', 'original-'+str(i//2)) for i in range(8)),
            dict(source='local controlled fixture; duplicate texts and original IDs retained'), max_rows=8)
        cls.manifest = cls.root/'inputs/manifest.json'
        with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'], autocommit=True) as connection:
            if connection.info.server_version != 180003:
                raise RuntimeError('requires PostgreSQL 18.3')
            installed = install_input_table(connection, QueryInputs('movie', cls.table, 8), read_prepared(cls.manifest, 'raw.jsonl'))
            cls.installation = cls.root/'installation.json'
            write_private_json(cls.installation, dict(installed, manifest_sha256=json.loads(cls.manifest.read_text())['sha256']))

    @classmethod
    def stop_fixture(cls):
        cls.release.set()
        cls.http.shutdown()
        cls.http.server_close()
        cls.thread.join(5)
        wait_until(lambda: cls.active == 0)
        write_private_json(cls.root/'fixture.json', dict(model_requests=0, fixture_posts=len(cls.posts),
            requests=cls.posts, cleanups=cls.cleanups, server_stopped=not cls.thread.is_alive(),
            ledger=cls.ledger.snapshot() if hasattr(cls, 'ledger') else None,
            campaign_ledger=cls.campaign_ledger.snapshot() if hasattr(cls, 'campaign_ledger') else None))

    def settled(self):
        actors = self.actors()
        live = {key: value for key, value in actors.items() if value['State'] != 'DEAD' and key not in self.base_actors}
        services = {key for key, value in live.items() if value['IsDetached']
                    and RAY_SERVICE_ACTORS.get(value['Name']) == value['ActorClassName']}
        unexpected = set(live) - services
        self.last_actor_snapshot = [dict(actor_id=key, state=value['State'], detached=value['IsDetached'],
            name=value['Name'], actor_class=value['ActorClassName'], cluster_service=key in services)
            for key, value in live.items()]
        if len({live[key]['Name'] for key in services}) != len(services):
            return False
        return not unexpected and self.ray.available_resources().get('CPU', 0) >= self.cpu_count

    def configuration(self, role, unit, timeout=90):
        arm = 'pg' if role in ('pg-http', 'pg-daft-ray') else role
        options = dict(concurrency=4, window=8, query_timeout_s=timeout, max_posts=8,
            ray_num_cpus=self.cpu_count, ray_actors=1, ray_batch_rows=4, ray_object_store_bytes=self.object_bytes)
        if role == 'pg-daft-ray':
            options.update(map_transport_config=str(self.transport),
                           map_transport_sha256=reference(self.transport)['sha256'])
        if role == 'ray-data':
            options['ray_address'] = os.environ['SEMLOOM_TEST_RAY_ADDRESS']
        if arm == 'pg':
            options.update(pg_total_budget=True, pg_staging_bytes=4*1048576)
        return QueryConfig(unit, arm, 'map', self.table, **options)

    def run_role(self, role, *, expect_failure=False, timeout=90, async_batches=1):
        type(self).sequence += 1
        unit = f'q{self.sequence:02}'
        config = self.configuration(role, unit, timeout)
        if async_batches != 1:
            config = replace(config,ray_async_batches_per_actor=async_batches,ray_batch_rows=1)
        config_path = self.root/(unit+'.json')
        write_private_json(config_path, asdict(config))
        output = self.root/unit
        output.mkdir(mode=0o700)
        command = [sys.executable, '-m', 'src.experiments.postgresql.query_cli', 'run', '--worker',
            '--config', str(config_path), '--manifest', str(self.manifest), '--model', str(self.model),
            '--budget', str(self.ledger.path), '--budget-id', self.budget.budget_id,
            '--max-attempts', str(self.budget.limit), '--output', str(output),
            '--dsn-env', 'SEMLOOM_TEST_PG_DSN', '--pg-log', os.environ['SEMLOOM_TEST_PG_LOG']]
        if expect_failure:
            with self.assertRaises((RuntimeError, TimeoutError)):
                supervise(command, output, lambda: self.ledger.close_shared_unit(unit), query_timeout_s=timeout)
            failure_report = json.loads((output/'supervisor.json').read_text())
            self.assertEqual(failure_report['status'], 'failed')
            self.assertEqual(failure_report['remaining_owned_pids'], [])
            if (output/'unit/summary.json').exists():
                self.assertEqual(json.loads((output/'unit/summary.json').read_text())['status'], 'failed')
            else:
                self.assertEqual(failure_report['error_type'], 'TimeoutError')
                self.assertTrue((output/'unit/q0/started.json').exists())
        else:
            supervise(command, output, lambda: self.ledger.close_shared_unit(unit), query_timeout_s=timeout)
        with self.assertRaisesRegex(BudgetExhausted, 'closed'):
            SharedClaimedUnit(self.ledger.path, self.budget, unit).reserve('a'*64)
        try:
            wait_until(self.settled)
        finally:
            write_private_json(output/'actor-cleanup.json', dict(actors=self.last_actor_snapshot,
                available_cpus=self.ray.available_resources().get('CPU', 0), declared_cpus=self.cpu_count))
        self.cleanups.append(dict(unit=unit, role=role, expected_failure=expect_failure,
            no_live_query_actors=True, all_declared_cpus_available=True,
            controller_connected=self.ray.is_initialized(), monotonic_ns=time.monotonic_ns()))
        self.assertTrue(self.ray.is_initialized())
        ref = reference(output/('supervisor.json' if expect_failure else 'unit/summary.json'))
        if not expect_failure:
            record = read_run(ref, role)
            self.assertEqual(record['quality']['false_positive']+record['quality']['false_negative'], 0)
            self.assertEqual(record['rows'], 8)
            self.assertEqual(record['model_usage']['prompt_tokens'], 80)
            self.assertEqual(record['model_usage']['output_tokens'], 8)
        return ref

    def test_complete_four_path_repeats_and_offline_selection(self):
        candidates = {role: dict(id=role, role=role, warmup=[], measured=[]) for role in ROLES}
        for repeat in range(4):
            order = ROLES[repeat:] + ROLES[:repeat]
            for role in order:
                candidates[role]['warmup' if repeat == 0 else 'measured'].append(self.run_role(role))
        spec = dict(schema=SCHEMA, stage='tuning', repeats=3, candidates=list(candidates.values()))
        write_private_json(self.root/'comparison-input.json', spec)
        result = summarize(spec)
        self.assertEqual(set(result['selected']), set(ROLES))
        self.assertFalse(result['performance_qualified'])
        write_private_json(self.root/'comparison.json', result)

    def test_main_profile_complete_repeats_include_native_daft(self):
        candidates={role:dict(id=role,role=role,warmup=[],measured=[]) for role in MAIN_ROLES}
        for repeat in range(4):
            for role in MAIN_ROLES[repeat:]+MAIN_ROLES[:repeat]:
                candidates[role]['warmup' if repeat==0 else 'measured'].append(self.run_role(role))
        result=summarize(dict(schema=SCHEMA,profile='main',stage='tuning',repeats=3,
                              candidates=list(candidates.values())))
        self.assertEqual(set(result['selected']),set(MAIN_ROLES))
        write_private_json(self.root/'main-comparison.json',result)

    def test_error_then_recovery_for_each_execution_owner(self):
        for role in (*ROLES,'daft-native'):
            with self.subTest(role=role):
                type(self).mode = 'error'
                try:
                    self.run_role(role, expect_failure=True)
                finally:
                    type(self).mode = 'normal'
                wait_until(lambda: self.active == 0)
                self.run_role(role)

    def test_native_async_batches_overlap_without_changing_request_count(self):
        type(self).mode = 'delay'
        try:
            ref=self.run_role('ray-data',async_batches=4)
        finally:
            type(self).mode = 'normal'
        summary=json.loads(Path(ref['path']).read_text())
        self.assertEqual(summary['evaluation']['actual_posts'],8)
        self.assertEqual(summary['evaluation']['http']['peak_http'],4)
        self.assertEqual(summary['resources']['native_async_batches_per_actor'],4)
        self.assertEqual(summary['resources']['native_http_capacity_upper_bound'],4)

    def test_ray_disconnect_preserves_remote_cause_then_recovers(self):
        type(self).mode = 'disconnect'
        try:
            ref = self.run_role('pg-daft-ray', expect_failure=True)
        finally:
            type(self).mode = 'normal'
        unit = Path(ref['path']).parent/'unit'
        events = [json.loads(line) for line in (unit/'public-events.jsonl').read_text().splitlines()]
        failures = [e for e in events if e['event']=='core_ray_execution_error']
        unknown = [e for e in events if e['event']=='core_uncertain']
        self.assertTrue(failures)
        self.assertTrue(unknown)
        self.assertTrue({tuple(e['key'].values()) for e in unknown} <=
                        {tuple(e['key'].values()) for e in failures})
        for event in failures:
            self.assertEqual(event['stage'],'http')
            self.assertEqual(event['remote_outcome'],'unconfirmed')
            self.assertIn(event['reason']['exception_type'],('httpx.RemoteProtocolError','httpx.ReadError'))
            self.assertNotIn('messages',json.dumps(event))
        self.assertEqual(json.loads((unit/'summary.json').read_text())['status'],'failed')
        wait_until(lambda: self.active == 0)
        self.run_role('pg-daft-ray')

    def test_finite_campaign_uses_supervised_native_and_pg_workers(self):
        path = self.root/'campaign.json'
        environment = self.root/'fixture-environment.json'
        write_private_json(environment, dict(status='ok', ray_version=self.ray.__version__,
            source='explicit isolated PG18.3/Ray fixture controller; not a model service qualification'))
        budget = AttemptBudget('text-map.campaign-fixture', 32)
        type(self).campaign_ledger = CellBudgetLedger.create(self.root/'campaign.sqlite', budget, deadline_utc=time.time()+280)
        spec = dict(schema=CAMPAIGN_SCHEMA, stage='qualification', repeats=3,
            candidates=[dict(id=role, role=role, config=asdict(self.configuration(role, role))) for role in ROLES],
            orders=[list(ROLES)], max_posts=32, max_seconds=300, manifest=reference(self.manifest),
            model=reference(self.model), installation=reference(self.installation), environment=reference(environment),
            service_signature=reference(self.model)['sha256'], dsn_env='SEMLOOM_TEST_PG_DSN',
            budget_path=str(self.campaign_ledger.path), budget_id=budget.budget_id,
            output_root=str(self.root/'campaign'), pg_log=os.environ['SEMLOOM_TEST_PG_LOG'])
        write_private_json(path, spec)
        result = run_stage(path)
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['budget']['allocated_requests'], 32)
        self.assertFalse(result['next_stage_started'])
        for unit in result['completed']:
            read_run(unit['summary'], unit['role'])
            with self.assertRaisesRegex(BudgetExhausted, 'closed'):
                SharedClaimedUnit(self.campaign_ledger.path, budget, unit['unit']).reserve('a'*64)
        wait_until(self.settled)

    def test_timeout_during_http_then_recovery_for_ray_paths(self):
        for role in ('pg-daft-ray', 'ray-data', 'daft-native'):
            with self.subTest(role=role):
                type(self).mode = 'hold'
                self.release.clear()
                self.started_request.clear()
                timer = threading.Timer(25, self.release.set)
                timer.start()
                try:
                    self.run_role(role, expect_failure=True, timeout=15)
                    self.assertTrue(self.started_request.is_set(), 'timeout must exercise an actual HTTP request')
                finally:
                    self.release.set()
                    timer.cancel()
                    type(self).mode = 'normal'
                wait_until(lambda: self.active == 0)
                self.run_role(role)
