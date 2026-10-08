"""Match the retained BrokenPipe read failure without a model or PostgreSQL.

The fixture closes an idle HTTP/1.1 socket after five seconds. A trace callback
pauses after a pooled connection has been selected, before headers are written.
"""
import argparse
import asyncio
import hashlib
import json
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import httpx
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
            self.connection.settimeout(5)

        def log_message(self, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers.get('Content-Length', '0')))
            served.append(time.monotonic())
            body = b'{}'
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever,
                              kwargs={'poll_interval': .02}, daemon=True)
    thread.start()
    origin = time.monotonic()
    trace_events = []
    observers = []
    state = dict(second=False, new_connection=False, injected_pauses=0)
    expiry = 4.0 if mode == 'retire' else 5.0
    pause = 0.0 if mode == 'no_pause' else .3

    async def hook(request):
        state['new_connection'] = False

        async def trace(event, info):
            trace_events.append({'event': event,
                                 'seconds': time.monotonic() - origin,
                                 'second_request': state['second']})
            if event.endswith('connect_tcp.started'):
                state['new_connection'] = True
            if state['second'] and not state['new_connection'] and event == 'http11.send_request_headers.started' and pause:
                state['injected_pauses'] += 1
                time.sleep(pause)

        request.extensions['trace'] = trace

    async def run():
        config = SimpleNamespace(
            endpoint_url='http://127.0.0.1:%s/v1/chat/completions' % server.server_port,
            timeout_ms=3000, bearer_token=None)
        transport = AsyncFixedModelTransport(config, 1, observer=observers.append)
        original = httpx.AsyncClient

        def client(*args, **kwargs):
            if mode == 'retire':
                kwargs['limits'] = httpx.Limits(max_connections=1,
                                               max_keepalive_connections=1,
                                               keepalive_expiry=expiry)
            kwargs['event_hooks'] = {'request': [hook]}
            return original(*args, **kwargs)

        httpx.AsyncClient = client
        results = []
        try:
            task = SimpleNamespace(payload=b'{}', max_result_bytes=128)
            for sequence in range(2):
                if sequence:
                    await asyncio.sleep(4.85)
                    state['second'] = True
                try:
                    body = await transport.execute(SimpleNamespace(key=Key(0, sequence), task=task), 'model')
                    results.append({'status': 'passed', 'sha256': hashlib.sha256(body).hexdigest()})
                except BaseException as error:
                    results.append({'status': 'failed', 'reason': exception_details(error)})
        finally:
            httpx.AsyncClient = original
            await transport.close()
        return results

    try:
        results = asyncio.run(run())
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)
    return dict(mode=mode, results=results, client_expiry_seconds=expiry,
                server_idle_timeout_seconds=5.0, injected_pause_seconds=pause,
                injected_pauses=state['injected_pauses'],
                request_attempts=2, server_received_requests=len(served),
                trace=trace_events, observer=observers,
                cleanup_passed=not thread.is_alive())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    cases = []
    for repeat in range(5):
        for mode in ['current', 'retire', 'no_pause']:
            case = dict(trial(mode), repeat=repeat)
            cases.append(case)
            expected_failure = mode == 'current'
            second = case['results'][1]
            observed_failure = second['status'] == 'failed'
            matched = observed_failure == expected_failure
            if observed_failure:
                matched = matched and second['reason']['exception_type'] == 'httpx.ReadError' and second['reason']['cause_types'][-1] == 'builtins.BrokenPipeError'
            case['matches_prediction'] = matched
            print(json.dumps({'mode': mode, 'repeat': repeat,
                              'second_result': second, 'matches_prediction': matched,
                              'real_model_requests': 0}), flush=True)
            (args.output / ('%s.%s.json' % (mode, repeat))).write_text(json.dumps(case, indent=2) + '\n')
    result = dict(schema='semloom.idle_connection_diagnosis.v1',
                  status='passed' if all(case['matches_prediction'] and case['cleanup_passed'] for case in cases) else 'failed',
                  cases=cases, local_request_attempts=len(cases) * 2,
                  real_model_requests=0, postgres_queries=0, gpu_usage=0,
                  exact_original_socket_lifetime='unavailable',
                  source_scope='actual transport; controlled local idle timeout and client pause')
    (args.output / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({key: value for key, value in result.items() if key != 'cases'}))
    return 0 if result['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
