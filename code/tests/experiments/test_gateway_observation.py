"""Exercise shared observers over real local UDS and a synthetic HTTP endpoint."""
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.execution_provider import server
from src.execution_provider.completion import Completion
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.wire import v3, v4, v5
from src.execution_provider.generation_profile import GenerationProfile
from src.execution_provider.adapters.openai_compatible_fixed import VLLM_CHOICE_FORMAT
from src.execution_provider.wire.framing import encode_frame, read_frame
from src.experiments.attempt_ledger import AttemptBudget, AttemptLedger
from src.experiments.postgresql.provider_session_attribution import session_windows

CODE = Path(__file__).resolve().parents[2]


class GatewayObservationTests(unittest.TestCase):
    def test_golden_and_budgeted_http_use_the_same_session_observation(self):
        for codec in (v3, v4, v5):
            for fixed in (False, True):
                with self.subTest(version=codec.PROTOCOL_VERSION, fixed=fixed), tempfile.TemporaryDirectory(dir='/tmp', prefix='obs-') as directory:
                    self.check_gateway(Path(directory), codec, fixed)

    def check_gateway(self, root, codec, fixed):
        version = codec.PROTOCOL_VERSION
        model = 'fixture-configured-model'
        profile = GenerationProfile('semloom.generation.choice.tristate', 1, 'CHOICE', ('TRUE', 'FALSE', 'UNKNOWN'))
        plan = (SemanticMapPlan('Keep text.', model, 32) if version == 5 else
                codec.SemanticFilterPlan('Classify.', model, profile if version == 4 else None))
        prompt_tokens, output_tokens = (11, 3) if fixed or version == 5 else (0, 1)
        expected = Completion(' 空白 \n' if version == 5 else 'TRUE', model, prompt_tokens, output_tokens, 'stop')
        execution_id = f'semloom.provider.openai-compatible-fixed.uds.v{version}' if fixed else codec.GOLDEN_EXECUTION_ID
        opened = codec.build_open_message(plan, provider_execution_id=execution_id)
        task = codec.build_task_message(plan, sequence=0, input_value='input', provider_execution_id=execution_id)
        requests = []
        endpoint = worker = None
        session_events = root / 'sessions.jsonl'
        if fixed:
            class Handler(BaseHTTPRequestHandler):
                def do_POST(self):
                    requests.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                    body = json.dumps({'model': plan.model_id, 'choices': [{'index': 0,
                        'message': {'content': expected.raw_output}, 'finish_reason': 'stop'}],
                        'usage': {'prompt_tokens': 11, 'completion_tokens': 3}}).encode()
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                def log_message(self, *_): pass
            endpoint = HTTPServer(('127.0.0.1', 0), Handler)
            worker = threading.Thread(target=endpoint.serve_forever)
            worker.start()
            config = root / 'model.json'
            config.write_text(json.dumps({'model_id': plan.model_id, 'timeout_ms': 2000,
                'endpoint_url': f'http://127.0.0.1:{endpoint.server_port}/completion',
                **({'choice_format': VLLM_CHOICE_FORMAT} if version == 4 else {})}))
            ledger = AttemptLedger.create(root / 'attempts.jsonl', AttemptBudget('fixture.map', 2))
            ledger.reserve('a' * 64)
            prior = ledger.path.read_bytes()
            module = 'src.experiments.choice_gateway_observer'
            arguments = ['--events', str(root/'http.jsonl'), '--session-events', str(session_events),
                         '--ledger', str(ledger.path), '--budget-id', 'fixture.map', '--max-attempts', '2']
            adapter_args = ['--fixed-model-config', str(config)]
        else:
            fixture = root / 'fixture.json'
            fixture.write_text(json.dumps({task['semantic_payload_digest']: asdict(expected) if version == 5 else expected.raw_output}))
            module = 'src.experiments.postgresql.semmap_resource_gateway_observer'
            # The held path is the same observer, with an already-released fixture barrier.
            release = root / 'release'
            release.touch()
            arguments = ['--events', str(session_events), '--release', str(release)]
            adapter_args = ['--golden-fixture', str(fixture)]
        path = root / 'provider.sock'
        process = subprocess.Popen([sys.executable, '-m', module, *arguments, '--',
            '--socket', str(path), '--once', *adapter_args], cwd=root,
            env=dict(os.environ, PYTHONPATH=str(CODE)), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(4)
                deadline = time.monotonic() + 4
                while not path.exists():
                    if process.poll() is not None or time.monotonic() > deadline:
                        self.fail('gateway failed to start')
                    time.sleep(.01)
                client.connect(str(path))
                client.sendall(encode_frame(opened))
                self.assertEqual(read_frame(client)['type'], 'opened')
                client.sendall(encode_frame(task))
                actual = read_frame(client)
                context = codec.validate_open(opened, provider_execution_id=execution_id)
                self.assertEqual(actual, codec.build_completion_message(context, sequence=0,
                    payload_digest=task['semantic_payload_digest'], completion=expected))
                if fixed:
                    client.sendall(encode_frame({**task, 'sequence': '1'}))
                    self.assertEqual(read_frame(client)['code'], 'GATEWAY_INTERNAL')
            stdout, stderr = process.communicate(timeout=4)
            self.assertEqual((process.returncode, stdout, stderr), (0, b'', b''))
            self.assertFalse(path.exists())
            text = session_events.read_text()
            events = [json.loads(line) for line in text.splitlines()]
            self.assertEqual(events[0]['event'], 'session_start')
            self.assertEqual(events[-1]['event'], 'session_end')
            self.assertTrue(events[-1]['connection_closed'])
            if sys.platform == 'linux':
                self.assertEqual(len(session_windows(events)), 1)
            else:
                self.assertIsNone(events[0]['peer_pid'])
            self.assertNotIn(expected.raw_output, text)
            if fixed:
                self.assertEqual(ledger.attempts, 2)
                self.assertTrue(ledger.path.read_bytes().startswith(prior))
                self.assertEqual(len(requests), 1)
                self.assertEqual(requests[0]['model'], plan.model_id)
                self.assertEqual(requests[0]['messages'], task['canonical_messages'])
                if version == 4:
                    self.assertEqual(requests[0]['structured_outputs'], {'choice': ['TRUE', 'FALSE', 'UNKNOWN']})
                self.assertIn('task_error', [event['event'] for event in events])
        finally:
            if process.poll() is None: process.kill()
            process.communicate(timeout=4)
            if endpoint:
                endpoint.shutdown()
                worker.join()
                endpoint.server_close()

    def test_wrapper_failure_closes_owned_connection_without_global_replacement(self):
        with tempfile.TemporaryDirectory(dir='/tmp', prefix='obs-') as directory:
            path = Path(directory) / 'provider.sock'
            connection, peer = socket.socketpair()
            original = server._run_session
            listener = unittest.mock.Mock()
            def bind(target): Path(target).touch()
            listener.bind.side_effect = bind
            listener.accept.return_value = (connection, None)
            def wrapper(run):
                self.assertIs(run, original)
                def fail(*args, **kwargs): raise RuntimeError('observer failed')
                return fail
            with peer, patch.object(server.socket, 'socket', return_value=listener), \
                 patch.object(server.signal, 'signal'):
                with self.assertRaisesRegex(RuntimeError, 'observer failed'):
                    server.main(['--socket', str(path), '--once'], session_wrapper=wrapper)
            self.assertEqual(connection.fileno(), -1)
            self.assertIs(server._run_session, original)
            listener.close.assert_called_once()


if __name__ == '__main__': unittest.main()
