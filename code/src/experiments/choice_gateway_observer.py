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
from src.baselines.common.private_artifacts import content_digest, open_private_text, write_private_json, require_outside_git
from src.experiments.expected_requests import ExpectedRequests
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.buffered_events import BufferedEvents, compact_event


CHOICE_BUDGET = AttemptBudget("semloom.choice.4c.v1", 100)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--private-events", type=Path, help="verbatim private events outside Git")
    parser.add_argument("--expected-request-hashes", type=Path,
                        help="complete request-value multiset checked before HTTP send")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--ledger", type=Path)
    mode.add_argument("--cell-budget", type=Path, help="pre-reserved experiment-unit budget")
    parser.add_argument("--shared-unit-budget", action="store_true",
                        help="durably account each POST, matching native worker observation")
    mode.add_argument("--fixture-only", action="store_true")
    parser.add_argument("--unit-id")
    parser.add_argument("--event-mode", choices=("qualification", "compact-buffered"), default="qualification")
    parser.add_argument("--event-content", choices=("compact", "full"),
                        help="full content requires a separate private event file")
    parser.add_argument("--event-write-mode", choices=("synchronous", "buffered"))
    parser.add_argument("--observer-summary", type=Path)
    parser.add_argument("--event-buffer-bytes", type=int, default=8 * 1024 * 1024)
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
    if bool(args.cell_budget) != bool(args.unit_id) or (args.cell_budget and args.budget_id is None):
        parser.error("cell budgets require unit ID, budget identity and limit")
    content = args.event_content or (
        "compact" if args.event_mode == "compact-buffered" else "full" if args.private_events else "redacted"
    )
    write_mode = args.event_write_mode or (
        "buffered" if args.event_mode == "compact-buffered" else "synchronous"
    )
    if content == "compact" and args.private_events:
        parser.error("compact events do not write full private content")
    if content == "full" and not args.private_events:
        parser.error("full events require --private-events outside Git")
    if args.private_events:
        require_outside_git(args.private_events)
    if content == "redacted":
        # Legacy redaction removes credentials, not arbitrary input/output content.
        require_outside_git(args.events)
    if args.event_buffer_bytes < 1:
        parser.error("event buffer must be positive")
    if args.observer_summary and args.observer_summary.exists():
        parser.error("observer summary already exists")
    if args.expected_request_hashes and not (args.ledger or args.cell_budget):
        parser.error("expected-request hashes require an existing durable ledger")
    budget = (
        CHOICE_BUDGET
        if args.budget_id is None
        else AttemptBudget(args.budget_id, args.max_attempts)
    )
    if args.shared_unit_budget and not args.cell_budget:
        parser.error("shared-unit budget requires --cell-budget")
    ledger = AttemptLedger(args.ledger, budget) if args.ledger else None
    if args.cell_budget:
        owner = CellBudgetLedger(args.cell_budget, budget)
        ledger = (owner.claim_shared_unit(args.unit_id) if args.shared_unit_budget else
                  owner.claim_unit(args.unit_id))
    available = ledger.remaining if args.cell_budget else budget.limit - ledger.attempts if ledger else 0
    expected = (ExpectedRequests.load(args.expected_request_hashes,
                available_attempts=available)
                if args.expected_request_hashes else None)
    if write_mode == "synchronous":
        args.events.touch(exist_ok=False)
    record_lock = threading.Lock()
    session_observer = SessionObserver(lambda event: None)
    private_handle = None
    buffered = session_buffered = private_buffered = None

    def record(event):
        event = dict(
            event, session_id=session_observer.current_session, task=session_observer.current_task,
            monotonic_ns=time.monotonic_ns(),
        )
        public_event = redact_json_values(event) if content == "redacted" else compact_event(event)
        with record_lock:
            if private_buffered is not None:
                private_buffered.record(event)
            if private_handle is not None:
                private_handle.write(json.dumps(event, ensure_ascii=False) + "\n")
                private_handle.flush()
            if buffered is not None:
                buffered.record(public_event)
            else:
                with args.events.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(public_event, ensure_ascii=False, separators=(",", ":")) + "\n")

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
            if write_mode == "buffered":
                buffered = stack.enter_context(BufferedEvents(args.events, max_pending_bytes=args.event_buffer_bytes))
            if args.private_events:
                if write_mode == "buffered":
                    private_buffered = stack.enter_context(BufferedEvents(args.private_events,
                                                                         max_pending_bytes=args.event_buffer_bytes))
                else:
                    private_handle = stack.enter_context(open_private_text(args.private_events))
            if args.session_events:
                if buffered is not None:
                    session_buffered = stack.enter_context(BufferedEvents(args.session_events,
                                                                         max_pending_bytes=args.event_buffer_bytes))
                    session_observer = SessionObserver(session_buffered.record)
                else:
                    handle = stack.enter_context(args.session_events.open("x", encoding="ascii", buffering=1))
                    session_observer = SessionObserver(lambda event: handle.write(json.dumps(event) + "\n"))

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
        if args.observer_summary:
            write_private_json(args.observer_summary, {
                "event_mode": args.event_mode,
                "event_content": content,
                "event_write_mode": write_mode,
                "unit_id": args.unit_id,
                "observed_attempts": ledger.attempts if ledger else None,
                "events": buffered.snapshot() if buffered else None,
                "sessions": session_buffered.snapshot() if session_buffered else None,
                "private_events": private_buffered.snapshot() if private_buffered else None,
            })


if __name__ == "__main__":
    raise SystemExit(main())
