"""CPU-only diagnostic of the retained query's real HTTP transport.

The local endpoint returns a fixture and can reset the second request on a
connection. It never contacts a model, PostgreSQL, Ray, or a GPU.
"""
import argparse
import asyncio
import hashlib
import json
import socket
import struct
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

from src.execution_provider.adapters.async_fixed_model import (
    AsyncFixedModelTransport,
    exception_details,
)


@dataclass(frozen=True)
class Key:
    session_id: int
    sequence: int


def trial(mode):
    served = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def setup(self):
            super().setup()
            self.request_number = 0

        def log_message(self, *args):
            pass

        def do_POST(self):
            self.request_number += 1
            served.append({'connection_request': self.request_number})
            if mode != 'plain' and self.request_number == 2:
                self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                           struct.pack('ii', 1, 0))
                self.close_connection = True
                self.connection.close()
                return
            self.rfile.read(int(self.headers.get('Content-Length', '0')))
            body = b'{"fixture":"POSITIVE"}'
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever,
                              kwargs={'poll_interval': .02}, daemon=True)
    thread.start()
    observed = []
    endpoint = 'http://127.0.0.1:%s/v1/chat/completions' % server.server_port
    config = SimpleNamespace(endpoint_url=endpoint, timeout_ms=1000,
                             bearer_token=None)

    async def run():
        transport = AsyncFixedModelTransport(config, 1, observer=observed.append)
        results = []
        try:
            for sequence in range(2):
                if mode == 'fresh' and sequence:
                    await transport.close()
                    transport._client = None
                request = SimpleNamespace(
                    key=Key(0, sequence),
                    task=SimpleNamespace(payload=b'{"fixture":true}',
                                         max_result_bytes=128),
                )
                try:
                    response = await transport.execute(request, 'model')
                    results.append({'status': 'passed',
                                    'sha256': hashlib.sha256(response).hexdigest()})
                except BaseException as error:
                    results.append({'status': 'failed',
                                    'reason': exception_details(error)})
        finally:
            await transport.close()
        return results

    started = time.monotonic()
    try:
        results = asyncio.run(run())
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    return dict(results=results, observed=observed, served=served,
                elapsed_seconds=time.monotonic() - started,
                cleanup_passed=not thread.is_alive())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--mode', choices=['reset', 'plain', 'fresh'], default='reset')
    parser.add_argument('--repeats', type=int, default=5)
    args = parser.parse_args()
    if not 1 <= args.repeats <= 10:
        raise ValueError('diagnostic repetition exceeds planned range')
    args.output.mkdir(parents=True, exist_ok=False)
    trials = [trial(args.mode) for _ in range(args.repeats)]
    all_passed = all(row['status'] == 'passed'
                     for sample in trials for row in sample['results'])
    summary = dict(schema='semloom.http_transport_diagnosis.v1', mode=args.mode,
                   real_model_requests=0, postgres_queries=0, gpu_usage=0,
                   source_scope='actual AsyncFixedModelTransport; local HTTP fixture',
                   trials=trials, status='passed' if all_passed else 'failed')
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps({'mode': args.mode, 'status': summary['status'],
                      'trials': len(trials), 'local_requests': sum(len(v['served']) for v in trials),
                      'errors': [v['results'][1].get('reason') for v in trials],
                      'real_model_requests': 0,
                      'cleanup_passed': all(v['cleanup_passed'] for v in trials)}))
    return 0 if all_passed else 1


if __name__ == '__main__':
    raise SystemExit(main())
