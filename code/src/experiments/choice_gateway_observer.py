"""Observe fixed-model Filter or Map requests using a configured durable budget.

Real POSTs require a pre-existing durable ledger. Fixture runs must explicitly
select fixture mode; resolver blocking is available only in that mode.
"""

import argparse
from contextlib import ExitStack, nullcontext
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import socket
import time
import threading

from src.execution_provider import server
from src.execution_provider.completion import CompletionAdapterError
from src.experiments.attempt_ledger import (
    AttemptBudget,
    AttemptLedger,
    BudgetError,
    observe_http_posts,
    observe_async_http_posts,
)
from src.experiments.gateway_observer import ObservedAdapter, SessionObserver
from src.baselines.common.redact import redact_json_values
from src.baselines.common.private_artifacts import content_digest, open_private_text
from src.experiments.expected_requests import ExpectedRequests


CHOICE_BUDGET = AttemptBudget("semloom.choice.4c.v1", 100)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--private-events", type=Path, help="verbatim private events outside Git")
    parser.add_argument("--expected-request-hashes", type=Path,
                        help="complete request-value multiset checked before HTTP send")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--ledger", type=Path)
    mode.add_argument("--fixture-only", action="store_true")
    parser.add_argument("--dns-release-file", type=Path)
    parser.add_argument("--budget-id", help="expected identity of the existing ledger")
    parser.add_argument(
        "--max-attempts", type=int, help="expected total ledger limit, including history"
    )
    parser.add_argument(
        "--session-events", type=Path, help="optional passive socket/session event stream"
    )
    args, gateway_args = parser.parse_known_args(argv)
    if args.dns_release_file and not args.fixture_only:
        parser.error("resolver faults require fixture mode")
    if (args.budget_id is None) != (args.max_attempts is None):
        parser.error("--budget-id and --max-attempts must be supplied together")
    if args.fixture_only and args.budget_id is not None:
        parser.error("fixture mode cannot select a real request budget")
    if args.expected_request_hashes and not args.ledger:
        parser.error("expected-request hashes require an existing durable ledger")
    budget = (
        CHOICE_BUDGET
        if args.budget_id is None
        else AttemptBudget(args.budget_id, args.max_attempts)
    )
    ledger = AttemptLedger(args.ledger, budget) if args.ledger else None
    expected = (ExpectedRequests.load(args.expected_request_hashes,
                available_attempts=budget.limit-ledger.attempts)
                if args.expected_request_hashes else None)
    args.events.touch(exist_ok=False)
    record_lock = threading.Lock()
    session_observer = SessionObserver(lambda event: None)
    private_handle = None

    def record(event):
        event = dict(
            event, session_id=session_observer.current_session, task=session_observer.current_task,
            monotonic_ns=time.monotonic_ns(),
        )
        with record_lock, args.events.open("a", encoding="utf-8") as handle:
            if private_handle is not None:
                private_handle.write(json.dumps(event, ensure_ascii=False) + "\n")
                private_handle.flush()
            handle.write(
                json.dumps(redact_json_values(event), ensure_ascii=False, separators=(",", ":")) + "\n"
            )

    def observe_completion(request, complete):
        started = time.monotonic()
        try:
            completion = complete(request)
        except BudgetError:
            # The HTTP observer rejected dispatch before calling the transport.
            raise CompletionAdapterError("GATEWAY_INTERNAL") from None
        except CompletionAdapterError as error:
            record(
                dict(
                    event="error",
                    payload_digest=request.semantic_payload_digest,
                    code=error.code,
                    elapsed_seconds=time.monotonic() - started,
                )
            )
            raise
        record(
            dict(
                event="completion",
                payload_digest=request.semantic_payload_digest,
                elapsed_seconds=time.monotonic() - started,
                **asdict(completion),
            )
        )
        return completion

    original_resolve = socket.getaddrinfo
    if args.dns_release_file:

        def blocked_resolve(*positional, **keywords):
            record(dict(event="dns-enter"))
            while not args.dns_release_file.exists():
                time.sleep(0.02)
            try:
                return original_resolve(*positional, **keywords)
            finally:
                record(dict(event="dns-exit"))

        socket.getaddrinfo = blocked_resolve

    observer = nullcontext()
    if ledger is not None:
        observe = (
            observe_async_http_posts if "--incremental-map" in gateway_args else observe_http_posts
        )
        def observe_request(attempt, body):
            values = json.loads(body)
            identity = dict(attempt=attempt, request_bytes_sha256=hashlib.sha256(body).hexdigest(),
                            request_values_sha256=content_digest(values))
            if expected is not None:
                try:
                    expected.accept(values)
                except BudgetError:
                    record(dict(event="request_rejected_before_send", **identity))
                    raise
            record(dict(event="request", body=values, **identity))

        observer = observe(ledger, observe_request)
    gateway_args = gateway_args[1:] if gateway_args[:1] == ["--"] else gateway_args
    try:
        with ExitStack() as stack:
            if args.private_events:
                private_handle = stack.enter_context(open_private_text(args.private_events))
            if args.session_events:
                handle = stack.enter_context(
                    args.session_events.open("x", encoding="ascii", buffering=1)
                )
                session_observer = SessionObserver(
                    lambda event: handle.write(json.dumps(event) + "\n")
                )

            def wrap_adapter(adapter):
                observed = ObservedAdapter(adapter, observe_completion)
                return ObservedAdapter(observed, session_observer.complete)

            def wrap_session(run):
                return lambda connection, **kw: session_observer.run_session(connection, run, **kw)

            stack.enter_context(observer)
            options = {}
            if "--incremental-map" in gateway_args:
                options["incremental_observer"] = lambda event: record(
                    dict(event, event="core_" + event["event"])
                )
            code = server.main(
                gateway_args, adapter_wrapper=wrap_adapter, session_wrapper=wrap_session, **options
            )
            if expected is not None:
                record(dict(event="expected_requests_final", remaining=expected.remaining))
                if code == 0 and expected.remaining:
                    return 1
            return code

    finally:
        socket.getaddrinfo = original_resolve


if __name__ == "__main__":
    raise SystemExit(main())
