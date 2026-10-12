"""Opt-in supplier error parsing through the unified query recorder; no models."""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import base64
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
import uuid

from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.supplier_adapter_query import run_supplier_query


@contextmanager
def response_fixture(status, body):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}/v1/chat/completions', requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        if thread.is_alive():
            raise RuntimeError('supplier HTTP fixture did not stop')


@unittest.skipUnless(os.environ.get('SEMLOOM_SUPPLIER_HTTP_ARM'),
                     'requires an explicitly selected actual supplier error fixture')
class SupplierHttpPreservationTests(unittest.TestCase):
    def test_original_http_error_malformed_json_and_length_reach_supplier_parser(self):
        arm = os.environ['SEMLOOM_SUPPLIER_HTTP_ARM']
        root = Path(tempfile.mkdtemp(dir=os.environ.get('SEMLOOM_FIXTURE_ARTIFACT_ROOT')))
        plan = SemanticMapPlan('Return ok.', 'fixture', 16)
        length = json.dumps(dict(id='fixture', object='chat.completion', created=0,
            model='fixture', choices=[dict(index=0, message=dict(role='assistant', content='ok'),
                                           finish_reason='length')],
            usage=dict(prompt_tokens=1, completion_tokens=1, total_tokens=2))).encode()
        cases = ((429, b'{"error":{"message":"original quota","type":"rate_limit"}}'),
                 (200, b'{"broken":'), (200, length))
        outcomes = []
        for index, (status, body) in enumerate(cases):
            case = root / str(index)
            case.mkdir()
            ledger = CellBudgetLedger.create(case / 'budget.sqlite', AttemptBudget('http-fixture', 1),
                                             deadline_utc=time.time() + 180)
            with self.subTest(case=index), response_fixture(status, body) as (endpoint, requests):
                failure = None
                try:
                    run_supplier_query(arm, load_source=lambda: iter([dict(row_id='row', text='fixture')]),
                        plan=plan, model=FixedModelConfig(endpoint, 'fixture', 10000), ledger=ledger,
                        unit_id='http-fixture', root=case / 'query', options=NativeGraphOptions(num_threads=8),
                        query_timeout_s=120, allowed_outputs=('ok',),
                        ray_temp_root=Path('/root/autodl-tmp/nhp.' + uuid.uuid4().hex[:8])
                            if arm.endswith(('native-ray', 'native-ray-data')) else None,
                        duckdb_library=Path(os.environ['SEMLOOM_DUCKDB_LIBRARY'])
                            if arm.startswith('duckdb-') else None)
                except Exception as error:
                    failure = error
                self.assertEqual(len(requests), 1)
                summary = json.loads((case / 'query' / 'summary.json').read_text())
                trace = [json.loads(line) for line in (case / 'query' / 'http-trace.jsonl').read_text().splitlines()]
                self.assertEqual(len(trace), 1)
                self.assertEqual(trace[0]['upstream_status'], status)
                self.assertEqual(trace[0]['client_status'], status)
                self.assertEqual(trace[0]['response_write_status'], 'completed')
                self.assertEqual(trace[0]['status'], 'completed' if 200 <= status < 300 else 'failed')
                raw = json.loads((case / 'query' / 'upstream-response-bodies.jsonl').read_text())
                self.assertEqual(base64.b64decode(raw['response_body_base64']), body)
                self.assertEqual(raw['response_body_sha256'], hashlib.sha256(body).hexdigest())
                self.assertNotIn('http_failure',summary['errors'])
                if index == 2 and arm.startswith('lotus-'):
                    self.assertIsNone(failure)
                    self.assertEqual(summary['status'], 'passed')
                else:
                    self.assertIsNotNone(failure)
                    self.assertEqual(summary['status'], 'failed')
                outcomes.append(dict(case=index, original_http_status=status,
                    caller_http_status=trace[0]['client_status'],
                    error_type=None if failure is None else type(failure).__name__,
                    status=summary['status'], actual_posts=len(requests),
                    response_body_sha256=raw['response_body_sha256']))
        output = os.environ.get('SEMLOOM_SUPPLIER_HTTP_SUMMARY')
        if output:
            Path(output).write_text(json.dumps(dict(arm=arm, passed=True,
                real_model_requests=0, actual_posts=3, cases=outcomes), indent=2) + '\n')
