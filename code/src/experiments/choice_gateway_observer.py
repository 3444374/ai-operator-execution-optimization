"""Observe fixed-model Filter or Map requests using a configured durable budget.

Real POSTs require a pre-existing durable ledger. Fixture runs must explicitly
select fixture mode; resolver blocking is available only in that mode.
"""
import argparse
from contextlib import ExitStack, nullcontext
from dataclasses import asdict
import json
from pathlib import Path
import socket
import time

from src.execution_provider import server
from src.execution_provider.adapters.semantic_session import CompletionAdapterError
from src.experiments.attempt_ledger import AttemptBudget, AttemptLedger, observe_http_posts
from src.experiments.choice_attempt_ledger import CHOICE_BUDGET
from src.experiments.gateway_observer import ObservedAdapter, SessionObserver
from src.baselines.common.redact import redact_text


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--events', type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--ledger', type=Path)
    mode.add_argument('--fixture-only', action='store_true')
    parser.add_argument('--dns-release-file', type=Path)
    parser.add_argument('--budget-id', help='expected identity of the existing ledger')
    parser.add_argument('--max-attempts', type=int, help='expected total ledger limit, including history')
    parser.add_argument('--session-events', type=Path, help='optional passive socket/session event stream')
    args, gateway_args = parser.parse_known_args(argv)
    if args.dns_release_file and not args.fixture_only:
        parser.error('resolver faults require fixture mode')
    if (args.budget_id is None) != (args.max_attempts is None):
        parser.error('--budget-id and --max-attempts must be supplied together')
    if args.fixture_only and args.budget_id is not None:
        parser.error('fixture mode cannot select a real request budget')
    budget = CHOICE_BUDGET if args.budget_id is None else AttemptBudget(args.budget_id, args.max_attempts)
    ledger = AttemptLedger(args.ledger, budget) if args.ledger else None
    args.events.touch(exist_ok=False)

    def record(event):
        with args.events.open('a', encoding='utf-8') as handle:
            handle.write(redact_text(json.dumps(event, ensure_ascii=False, separators=(',', ':'))) + '\n')

    def observe_completion(request, complete):
        started = time.monotonic()
        try:
            completion = complete(request)
        except CompletionAdapterError as error:
            record(dict(event='error', payload_digest=request.semantic_payload_digest,
                        code=error.code, elapsed_seconds=time.monotonic() - started))
            raise
        record(dict(event='completion', payload_digest=request.semantic_payload_digest,
                    elapsed_seconds=time.monotonic() - started, **asdict(completion)))
        return completion

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
    if ledger is not None:
        observer = observe_http_posts(ledger, lambda attempt, body:
            record(dict(event='request', attempt=attempt, body=json.loads(body))))
    gateway_args = gateway_args[1:] if gateway_args[:1] == ['--'] else gateway_args
    try:
        with ExitStack() as stack:
            session_observer = None
            if args.session_events:
                handle = stack.enter_context(args.session_events.open('x', encoding='ascii', buffering=1))
                session_observer = SessionObserver(lambda event: handle.write(json.dumps(event) + '\n'))

            def wrap_adapter(adapter):
                observed = ObservedAdapter(adapter, observe_completion)
                return (observed if session_observer is None else
                        ObservedAdapter(observed, session_observer.complete))

            def wrap_session(run):
                return lambda connection, **kw: session_observer.run_session(connection, run, **kw)

            stack.enter_context(observer)
            return server.main(gateway_args, adapter_wrapper=wrap_adapter,
                               session_wrapper=wrap_session if session_observer else None)
    finally:
        socket.getaddrinfo = original_resolve


if __name__ == '__main__':
    raise SystemExit(main())
