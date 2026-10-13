"""Original supplier parsers see HTTP errors before comparison qualification."""
from contextlib import contextmanager
import base64
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from src.baselines.common.redact import redact_json_values
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.supplier_adapter_query import run_supplier_query
from src.experiments.postgresql.persistent_native_adapter_query import PersistentAdapterGroup
from tests.experiments.test_native_adapter_query import fixture_server


@contextmanager
def error_server():
    class State:
        records = []
        status = 200
        raw = b''
    state = State()
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            state.records.append(self.rfile.read(int(self.headers['Content-Length'])))
            self.send_response(state.status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(state.raw)))
            self.end_headers()
            self.wfile.write(state.raw)
        def log_message(self, *_args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}/v1/chat/completions', state
    finally:
        server.shutdown(); server.server_close(); thread.join(5)
        if thread.is_alive():
            raise RuntimeError('supplier error fixture did not stop')


@unittest.skipUnless(os.environ.get('SEMLOOM_SUPPLIER_ERRORS_ARM'),
                     'actual supplier parser error checks are explicitly opt-in')
class SupplierNativeErrors(unittest.TestCase):
    def test_original_status_body_and_parser_error_survive_query_observation(self):
        selected = os.environ['SEMLOOM_SUPPLIER_ERRORS_ARM']
        supplier = 'lotus' if selected.startswith('lotus-') else 'duckdb'
        original = supplier + '-adapted-native'
        root = Path(tempfile.mkdtemp(dir=os.environ.get('SEMLOOM_FIXTURE_ARTIFACT_ROOT')))
        cases = [
            ('unauthorized', 401, b'{"error":{"message":"fixture auth","type":"auth"}}'),
            ('quota', 429, b'{"error":{"message":"fixture quota","type":"rate_limit"}}'),
            ('malformed', 200, b'{"broken":'),
            ('length', 200, json.dumps(dict(id='fixture', object='chat.completion', created=1,
                model='fixture', choices=[dict(index=0, message=dict(role='assistant',content='ok'),
                finish_reason='length')], usage=dict(prompt_tokens=2,completion_tokens=1,total_tokens=3))).encode()),
        ]
        results = []
        with error_server() as (endpoint, state):
            ledger = CellBudgetLedger.create(root/'budget.sqlite', AttemptBudget('supplier-errors', 8),
                                             deadline_utc=time.time()+900)
            for name, status, raw in cases:
                paired = []
                state.status, state.raw = status, raw
                for index, arm in enumerate((original, selected)):
                    query = root/(name+'-'+str(index))
                    error = None
                    try:
                        value = run_supplier_query(arm, load_source=lambda:iter([dict(row_id='one',text='input')]),
                            plan=SemanticMapPlan('Return ok.','fixture',16),
                            model=FixedModelConfig(endpoint,'fixture',20000), ledger=ledger,
                            unit_id=name+'-'+str(index), root=query,
                            options=NativeGraphOptions(concurrency=4,num_threads=8),
                            ray_temp_root=Path('/root/autodl-tmp/ne.'+uuid.uuid4().hex[:8])
                                if arm.endswith(('native-ray','native-ray-data')) else None,
                            duckdb_library=Path(os.environ['SEMLOOM_DUCKDB_LIBRARY']) if supplier=='duckdb' else None,
                            query_timeout_s=120, allowed_outputs=('ok',))
                    except Exception as failure:
                        error = failure
                        value = json.loads((query/'summary.json').read_text())
                    trace = [json.loads(line) for line in (query/'http-trace.jsonl').read_text().splitlines()]
                    self.assertEqual(len(trace), 1)
                    self.assertEqual(trace[0]['upstream_status'], status)
                    self.assertEqual(trace[0]['client_status'], status)
                    self.assertEqual(trace[0]['callback_error_type'],'')
                    self.assertEqual(trace[0]['response_write_status'],'completed')
                    self.assertEqual(trace[0]['status'],'completed' if 200<=status<300 else 'failed')
                    recorded = json.loads((query/'upstream-response-bodies.jsonl').read_text().strip())
                    self.assertEqual(base64.b64decode(recorded['response_body_base64']), raw)
                    self.assertNotIn('http_failure',value['errors'])
                    item = dict(case=name, arm=arm, exception_type=type(error).__name__ if error else None,
                        exception_status=getattr(error,'status_code',None), query_status=value['status'],
                        upstream_status=trace[0]['upstream_status'],client_status=trace[0]['client_status'],
                        actual_posts=value['attempted_posts'])
                    paired.append(item); results.append(item)
                self.assertEqual(paired[0]['exception_type'],paired[1]['exception_type'],name)
                self.assertEqual(paired[0]['exception_status'],paired[1]['exception_status'],name)
                self.assertEqual(paired[0]['query_status'],paired[1]['query_status'],name)
            self.assertEqual(len(state.records),8)
        (root/'error-verification.json').write_text(json.dumps(redact_json_values(dict(
            arm=selected, real_model_requests=0, actual_posts=8, cases=results)),indent=2)+'\n')


@unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_MIXED_SUPPLIER'),
                     'actual mixed native owner validation is explicitly opt-in')
class NativeMixedOwners(unittest.TestCase):
    def test_each_arm_reports_its_owner_in_one_shared_ray_runtime(self):
        supplier = os.environ['SEMLOOM_NATIVE_MIXED_SUPPLIER']
        self.assertIn(supplier,('lotus','duckdb'))
        arms = tuple(supplier+'-method-native-'+name for name in ('daft','ray','ray-data'))
        root = Path(tempfile.mkdtemp(dir=os.environ.get('SEMLOOM_FIXTURE_ARTIFACT_ROOT')))
        with fixture_server(content='ok') as (endpoint, requests):
            ledger=CellBudgetLedger.create(root/'budget.sqlite',AttemptBudget('mixed-native',6),
                                          deadline_utc=time.time()+300)
            summaries=[]
            with PersistentAdapterGroup(arms,plan=SemanticMapPlan('Return ok.','fixture',16),
                    model=FixedModelConfig(endpoint,'fixture',20000),ledger=ledger,root=root/'group',
                    options=NativeGraphOptions(concurrency=4,num_threads=8),
                    ray_temp_root=Path('/root/autodl-tmp/nm.'+uuid.uuid4().hex[:8]),
                    duckdb_library=Path(os.environ['SEMLOOM_DUCKDB_LIBRARY']) if supplier=='duckdb' else None) as group:
                for index, arm in enumerate(arms):
                    value=group.run(arm,unit_id='mixed-'+str(index),root=root/str(index),
                        load_source=lambda:iter([dict(row_id='one',text='input'),dict(row_id='two',text='other')]),
                        phase='qualification',allowed_outputs=('ok',))
                    self.assertEqual(value['runtime']['owner'],('Daft Native','Ray Core','Ray Data')[index])
                    self.assertIsNone(value['semloom_capacity'])
                    self.assertIsNone(group.owners[arm].execution)
                    summaries.append(value)
                self.assertEqual(len({v['persistent_lifecycle']['ray_session_id'] for v in summaries}),1)
            self.assertEqual(len(requests),6)
        (root/'mixed-verification.json').write_text(json.dumps(redact_json_values(dict(
            supplier=supplier,real_model_requests=0,actual_posts=6,
            owners=[v['runtime'] for v in summaries])),indent=2)+'\n')
