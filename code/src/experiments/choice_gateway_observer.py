"""Run the unchanged gateway with experiment-only HTTP/response observations.

Real POSTs require a pre-existing durable ledger. Fixture runs must explicitly
select fixture mode; resolver blocking is available only in that mode.
"""
import argparse
from contextlib import nullcontext
from dataclasses import asdict
import json
from pathlib import Path
import socket
import sys
import time

from src.execution_provider import server
from src.execution_provider.adapters.semantic_session import CompletionAdapterError
from src.experiments.choice_attempt_ledger import AttemptLedger, observe_http_posts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--events', type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--ledger', type=Path)
    mode.add_argument('--fixture-only', action='store_true')
    parser.add_argument('--dns-release-file', type=Path)
    args, gateway_args = parser.parse_known_args()
    if args.dns_release_file and not args.fixture_only:
        parser.error('resolver faults require fixture mode')
    args.events.touch(exist_ok=False)

    def record(event):
        with args.events.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(event, ensure_ascii=False, separators=(',', ':')) + '\n')

    original_adapter = server.OpenAICompatibleFixedAdapter

    class ObservedAdapter(original_adapter):
        def complete(self, request):
            started = time.monotonic()
            try:
                completion = super().complete(request)
            except CompletionAdapterError as error:
                record(dict(event='error', payload_digest=request.semantic_payload_digest,
                            code=error.code, elapsed_seconds=time.monotonic() - started))
                raise
            record(dict(event='completion', payload_digest=request.semantic_payload_digest,
                        elapsed_seconds=time.monotonic() - started, **asdict(completion)))
            return completion

    server.OpenAICompatibleFixedAdapter = ObservedAdapter
    original_resolve = socket.getaddrinfo
    if args.dns_release_file:
        def blocked_resolve(*positional, **keywords):
            record(dict(event='dns-enter'))
            while not args.dns_release_file.exists():
                time.sleep(0.02)
            try:
                return original_resolve(*positional, **keywords)
            finally:
                record(dict(event='dns-exit'))
        socket.getaddrinfo = blocked_resolve

    observer = nullcontext()
    if args.ledger:
        ledger = AttemptLedger(args.ledger)
        observer = observe_http_posts(ledger, lambda attempt, body:
            record(dict(event='request', attempt=attempt, body=json.loads(body))))
    sys.argv = [sys.argv[0]] + (gateway_args[1:] if gateway_args[:1] == ['--'] else gateway_args)
    try:
        with observer:
            return server.main()
    finally:
        socket.getaddrinfo = original_resolve
        server.OpenAICompatibleFixedAdapter = original_adapter


if __name__ == '__main__':
    raise SystemExit(main())
