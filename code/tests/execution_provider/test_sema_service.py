import gzip
from concurrent.futures import ThreadPoolExecutor
import base64
import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from urllib import error, request
from unittest import mock

from src.baselines.text.products import sema
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.sema_service import (
    SemaRequestService, SemaServiceError, SemaServiceLimits, prepare_sema_projection,
)


class _Model(BaseHTTPRequestHandler):
    def do_POST(self):
        raw = self.rfile.read(int(self.headers['Content-Length']))
        self.server.calls.append((self.path, raw, dict(self.headers)))
        self.server.received.set()
        if self.server.gate is not None:
            self.server.gate.wait(2)
        status, payload = self.server.status, self.server.body
        if self.server.reply_for is not None:
            status, payload, delay = self.server.reply_for(raw)
            time.sleep(delay)
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        if self.server.compressed:
            self.send_header('Content-Encoding', 'gzip')
        self.send_header('X-Sema-Fixture', 'preserve')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *_args):
        pass


_PROCESS = r'''
import csv, json, re, sys
from urllib import request
rows = []
pending = ''
url = model = ''
for line in sys.stdin:
    pending += line
    if not line.rstrip().endswith(';'):
        continue
    sql, pending = pending.strip(), ''
    def value():
        return re.findall(r"'((?:[^']|'')*)'", sql)[0].replace("''", "'")
    if sql.startswith('SET llm_url='):
        url = value()
    elif sql.startswith('SET llm_model='):
        model = value()
    elif sql.startswith('COPY input'):
        with open(value(), newline='') as stream:
            rows = list(csv.DictReader(stream))
    elif sql.startswith('SELECT row_id,s'):
        for row in rows:
            body = json.dumps({'model': model, 'messages': [{'role':'user','content':row['review_text']}],
                               'response_format':{'type':'json_schema','json_schema':{'schema':{'type':'string'}}}}).encode()
            try:
                with request.urlopen(request.Request(url, data=body, headers={'Content-Type':'application/json'}), timeout=3) as response:
                    raw = json.loads(response.read())
            except Exception:
                # Some author failures do not produce a failing process exit.
                sys.exit(0)
            label = json.loads(raw['choices'][0]['message']['content'])
            csv.writer(sys.stdout).writerow((row['row_id'],label));sys.stdout.flush()
    elif sql.startswith("SELECT '__sema_complete__'"):
        values = [v.replace("''", "'") for v in re.findall(r"'((?:[^']|'')*)'", sql)]
        if 'COUNT(*)' in sql:
            values.append(str(len(rows)))
        csv.writer(sys.stdout).writerow(values);sys.stdout.flush()
'''


class SemaServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='sema-service-test-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), _Model)
        self.server.calls, self.server.status, self.server.compressed = [], 200, False
        self.server.gate = None
        self.server.reply_for = None
        self.server.received = threading.Event()
        self.server.body = b'{"choices":[{"message":{"content":"\\\"POSITIVE\\\""}}],"usage":{"total_tokens":9}}'
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_model)
        self.url = 'http://127.0.0.1:' + str(self.server.server_port) + '/v1/chat/completions'
        self.payload = b' { "model":"fixture-model", "messages":[{"role":"user","content":"same"}], "response_format":{"json_schema":{"schema":{"type":"string"}}} } '
        self.limits = SemaServiceLimits(16, 65536, 65536, 2)

    def stop_model(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)

    def service(self, **kwargs):
        return SemaRequestService(query_id='query-a', upstream_url=self.url,
                                  limits=self.limits, trace_path=self.root / 'requests.jsonl', **kwargs)

    def post(self, url, body=None):
        data = self.payload if body is None else body
        try:
            with request.urlopen(request.Request(url, data=data, headers={'Content-Type': 'application/json'}), timeout=3) as r:
                return r.status, r.read(), dict(r.headers)
        except error.HTTPError as r:
            return r.code, r.read(), dict(r.headers)

    def fake_binary(self):
        binary = self.root / 'fake-sema'
        binary.write_text('#!' + sys.executable + '\n' + _PROCESS)
        binary.chmod(0o700)
        return binary

    def test_complete_json_payload_and_equal_requests_stay_separate(self):
        charged = []
        with self.service(before_post=charged.append) as service:
            for _ in range(2):
                status, body, headers = self.post(service.endpoint_url)
                self.assertEqual((status, body), (200, self.server.body))
                self.assertEqual(headers['X-Sema-Fixture'], 'preserve')
            service.end_input()
            self.assertEqual(self.post(service.endpoint_url)[0], 410)
        self.assertEqual(charged, [self.payload, self.payload])
        self.assertEqual([r[1] for r in self.server.calls], charged)
        self.assertEqual(service.summary['forwarded_posts'], 2)
        rows = [json.loads(line) for line in (self.root / 'requests.jsonl').read_text().splitlines()]
        self.assertEqual(rows[0]['native_task_ready_status'], 'unavailable')
        self.assertIsNone(rows[0]['native_task_ready_ns'])
        for row in rows[:2]:
            clocks = [row[k] for k in ('proxy_arrived_ns', 'body_read_ns', 'forward_started_ns',
                                      'model_returned_ns', 'response_written_ns')]
            known_clocks = [clock for clock in clocks if clock is not None]
            self.assertEqual(known_clocks, sorted(known_clocks))
            if service.mode == 'transparent':
                self.assertEqual(len(known_clocks), 5)

    def test_original_error_bytes_status_and_metadata_survive_and_later_posts_stop(self):
        self.server.status = 429
        self.server.body = b' {"error":{"type":"rate_limit","detail":"original"}}\n'
        cancelled = []
        with self.service() as service:
            service.bind_native_cancel(lambda: cancelled.append(True))
            status, body, headers = self.post(service.endpoint_url)
            self.assertEqual((status, body), (429, self.server.body))
            self.assertEqual(headers['X-Sema-Fixture'], 'preserve')
            first = service.first_error
            for _ in range(3):
                self.assertEqual(self.post(service.endpoint_url)[0], 410)
            self.assertIs(service.first_error, first)
            self.assertEqual(first.body, self.server.body)
            self.assertEqual(first.status, 429)
        self.assertEqual(len(self.server.calls), 1)
        self.assertTrue(cancelled)
        saved = json.loads((self.root / 'requests.first-error.json').read_text())
        self.assertEqual(saved['status'], 429)
        self.assertEqual(base64.b64decode(saved['body_base64']), self.server.body)
        self.assertEqual(saved['observed_ns'], first.observed_ns)

    def test_failed_error_artifact_write_keeps_the_original_http_error(self):
        self.server.status = 422
        with mock.patch('src.execution_provider.adapters.sema_service.write_private_json',
                        side_effect=OSError('fixture artifact write error')):
            with self.service() as service:
                self.assertEqual(self.post(service.endpoint_url)[:2], (422, self.server.body))
        self.assertEqual(service.first_error.status, 422)
        self.assertIn('first_error_write:OSError', service.cleanup_errors)

    def test_unknown_query_does_not_send_or_poison_the_owned_query(self):
        with self.service() as service:
            other = service.endpoint_url.replace('query-a', 'query-b')
            self.assertEqual(self.post(other)[0], 404)
            self.assertEqual(self.post(service.endpoint_url)[0], 200)
            self.assertIsNone(service.first_error)
            self.assertEqual(service.summary['received_requests'], 1)

    def test_cancellation_rejects_later_calls_without_counting_another_model_post(self):
        with self.service() as service:
            service.cancel()
            self.assertEqual(self.post(service.endpoint_url)[0], 410)
        self.assertEqual(self.server.calls, [])
        self.assertEqual(service.summary['forwarded_posts'], 0)

    def test_http_redirect_is_returned_without_following_or_retry(self):
        self.server.status = 307
        with self.service() as service:
            # urllib treats the deliberately unchanged redirect as an HTTPError.
            self.assertEqual(self.post(service.endpoint_url)[:2], (307, self.server.body))
        self.assertEqual(len(self.server.calls), 1)

    def test_compressed_raw_response_matches_content_encoding(self):
        self.server.compressed = True
        self.server.body = gzip.compress(self.server.body)
        with self.service() as service:
            status, raw, headers = self.post(service.endpoint_url)
            self.assertEqual((status, raw), (200, self.server.body))
            self.assertEqual(headers['Content-Encoding'], 'gzip')

    def test_response_limit_fails_stops_owner_and_does_not_retry(self):
        self.server.body = b'x' * 65537
        with self.service() as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 502)
            self.assertEqual(service.first_error.phase, 'transport')
            self.assertEqual(self.post(service.endpoint_url)[0], 410)
        self.assertEqual(len(self.server.calls), 1)

    def test_budget_failure_before_send_is_zero_model_posts(self):
        def exhausted(_body):
            raise RuntimeError('fixture accounting error')
        with self.service(before_post=exhausted) as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 502)
        self.assertEqual(self.server.calls, [])
        self.assertEqual(service.summary['forwarded_posts'], 0)

    def test_finite_run_limit_stops_before_an_extra_model_post(self):
        self.limits = SemaServiceLimits(1, 65536, 65536, 2)
        with self.service() as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 200)
            self.assertEqual(self.post(service.endpoint_url)[0], 410)
            self.assertEqual(service.first_error.phase, 'request_limit')
        self.assertEqual(len(self.server.calls), 1)

    def test_first_error_is_preserved_when_native_cancel_callback_fails(self):
        self.server.status = 503
        def failed_cancel():
            raise RuntimeError('fixture native cancellation error')
        with self.service() as service:
            service.bind_native_cancel(failed_cancel)
            self.assertEqual(self.post(service.endpoint_url)[:2], (503, self.server.body))
            self.assertEqual(service.first_error.status, 503)
        self.assertIn('RuntimeError', service.summary['cleanup_errors'])

    def test_existing_process_lifecycle_keeps_native_parser_and_query_timings(self):
        binary = self.fake_binary()
        values = ({'source_example_id': 'a', 'input_text': 'same'},
                  {'source_example_id': 'b', 'input_text': 'same'})
        with mock.patch.object(sema, 'BINARY_SHA256', hashlib.sha256(binary.read_bytes()).hexdigest()):
            with self.service() as service:
                model = FixedModelConfig(self.url, 'fixture-model', 2000)
                with prepare_sema_projection(values, SimpleNamespace(instruction='Classify.'), model,
                        binary=binary, root=self.root, num_threads=1, service=service) as query:
                    self.assertEqual(self.server.calls, [])
                    self.assertEqual(list(query.execute()), [('a', 'POSITIVE'), ('b', 'POSITIVE')])
                    self.assertEqual(query.summary['status'], 'completed')
                    self.assertGreater(query.summary['full_query_elapsed_ns'], query.summary['native_query_elapsed_ns'])
                    with self.assertRaisesRegex(RuntimeError, 'already been submitted'):
                        list(query.execute())
                self.assertIsNotNone(query.returncode)
        self.assertEqual(len(self.server.calls), 2)

    def test_native_exit_does_not_hide_first_http_failure(self):
        self.server.status, self.server.body = 422, b'{"error":"author HTTP error"}'
        binary = self.fake_binary()
        with mock.patch.object(sema, 'BINARY_SHA256', hashlib.sha256(binary.read_bytes()).hexdigest()):
            with self.service() as service:
                with prepare_sema_projection(({'source_example_id': 'a', 'input_text': 'review'},),
                        SimpleNamespace(instruction='Classify.'), FixedModelConfig(self.url, 'fixture-model', 2000),
                        binary=binary, root=self.root, num_threads=1, service=service) as query:
                    with self.assertRaises(SemaServiceError) as caught:
                        list(query.execute())
                    self.assertEqual((caught.exception.status, caught.exception.body), (422, self.server.body))
                    self.assertEqual(query.status, 'failed')
        self.assertEqual(len(self.server.calls), 1)


class SemaConnectorControlTests(unittest.TestCase):
    setUp = SemaServiceTests.setUp
    stop_model = SemaServiceTests.stop_model
    post = SemaServiceTests.post

    def service(self, concurrency):
        return SemaRequestService(query_id='query-a', upstream_url=self.url, limits=self.limits,
            trace_path=self.root / 'requests.jsonl', upstream_concurrency=concurrency)

    def wait_for(self, predicate):
        deadline = time.monotonic() + 1.5
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail('fixture did not reach the declared connector capacity and waiting state')
            time.sleep(.005)

    def test_two_connections_hold_upstream_capacity_and_record_pool_wait(self):
        self.server.gate = threading.Event()
        with self.service(2) as service:
            with ThreadPoolExecutor(max_workers=4) as callers:
                futures = [callers.submit(self.post, service.endpoint_url) for _ in range(4)]
                try:
                    self.wait_for(lambda: len(self.server.calls) == 2 and
                        sum('connector_wait_started_ns' in row for row in service._rows) == 2)
                    self.assertEqual(service._client.connector.limit, 2)
                    self.assertEqual(service._client.connector.limit_per_host, 2)
                finally:
                    self.server.gate.set()
                replies = [future.result() for future in futures]
                self.assertEqual([(status, body) for status, body, _headers in replies], [(200, self.server.body)] * 4)
                self.assertTrue(all(headers['X-Sema-Fixture'] == 'preserve' for _, _, headers in replies))
            service.end_input()
        rows = [json.loads(line) for line in service.trace_path.read_text().splitlines()]
        waiting = [row for row in rows if 'connector_wait_started_ns' in row]
        self.assertEqual(len(waiting), 2)
        self.assertTrue(all(row['connector_wait_started_ns'] < row['connector_wait_ended_ns']
                            < row['model_returned_ns'] for row in waiting))
        self.assertEqual(service.summary['upstream_concurrency'], 2)
        self.assertEqual(len(self.server.calls), 4)

    def test_bounded_forwarding_keeps_original_http_failure_and_stops_later_calls(self):
        self.server.status = 429
        self.server.body = b'{"error":"fixture original failure"}'
        with self.service(2) as service:
            self.assertEqual(self.post(service.endpoint_url)[:2], (429, self.server.body))
            self.assertEqual(self.post(service.endpoint_url)[0], 410)
            self.assertEqual(service.first_error.body, self.server.body)
        self.assertEqual(len(self.server.calls), 1)

    def test_cancellation_rejects_waiting_connections_without_another_upstream_post(self):
        self.server.gate = threading.Event()
        with self.service(2) as service:
            with ThreadPoolExecutor(max_workers=4) as callers:
                futures = [callers.submit(self.post, service.endpoint_url) for _ in range(4)]
                try:
                    self.wait_for(lambda: len(self.server.calls) == 2 and
                        sum('connector_wait_started_ns' in row for row in service._rows) == 2)
                    service.cancel()
                finally:
                    self.server.gate.set()
                self.assertEqual(sorted(future.result()[0] for future in futures), [200, 200, 502, 502])
            self.assertEqual(self.post(service.endpoint_url)[0], 410)
        self.assertEqual(len(self.server.calls), 2)
        self.assertEqual(service.first_error.phase, 'transport')

    def test_zero_preserves_unlimited_connector_and_original_forwarding(self):
        self.server.gate = threading.Event()
        with self.service(0) as service:
            with ThreadPoolExecutor(max_workers=4) as callers:
                futures = [callers.submit(self.post, service.endpoint_url) for _ in range(4)]
                try:
                    self.wait_for(lambda: len(self.server.calls) == 4)
                    self.assertEqual(service._client.connector.limit, 0)
                    self.assertEqual(service._client.connector.limit_per_host, 0)
                    self.assertTrue(all('connector_wait_started_ns' not in row for row in service._rows))
                finally:
                    self.server.gate.set()
                self.assertEqual([future.result()[0] for future in futures], [200] * 4)
            service.end_input()
        self.assertIsNone(service.first_error)


if __name__ == '__main__':
    unittest.main()
