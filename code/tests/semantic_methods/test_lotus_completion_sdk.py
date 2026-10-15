"""Real owner loop and fixed LOTUS SDK, served by two nonpreemptive localhost slots."""

from contextlib import contextmanager
from dataclasses import asdict, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.util import find_spec
import base64
import json
import re
import threading
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.full_response import decode_full_response
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.scheduling.core.session_contract import Usage
from src.semantic_methods.budget import MethodCapacity, row_reservation
from src.semantic_methods.continuation import MethodLimits
from src.semantic_methods.driver import MethodDriver
from src.semantic_methods.lotus.driver import iter_two_map_rows
from src.semantic_methods.lotus.maps import LotusTwoMapMethod, encode, staged_two_map
from tests.semantic_methods.test_lotus_adapter import STAGES, config, model, close_execution


PINNED = all(find_spec(name) is not None for name in ("lotus", "litellm", "openai", "httpx"))
POSITIVE = ((1, 1), (8, 1), (4, 1), (1, 8))
NEGATIVE = ((1, 8), (8, 1), (4, 1), (1, 1))
LIMITS = MethodLimits(4096, 65536, 65536, 2)


def identity(payload):
    content = json.loads(payload)["messages"][-1]["content"]
    return int(re.search(r"row:(\d+)", content).group(1)), int("Classify" in content)


def payloads(events):
    # Native SDK and prepared calls can serialize identical fields in a different order.
    return sorted(json.dumps(json.loads(base64.b64decode(e["payload"])), sort_keys=True)
                  for e in events if e["event"] == "start")


def select_task(name):
    def choose(candidates):
        if name == "fifo":
            return candidates[0].key if candidates else None
        if name == "sjf":
            key = lambda c: (c.task.estimated_work, c.key.sequence)
        elif name == "final":
            key = lambda c: (c.task.info.stage_id != "1", c.key.sequence)
        else:
            raise ValueError("unknown fixture control")
        return min(candidates, key=key).key if candidates else None
    return choose


@contextmanager
def service(costs=POSITIVE, unit_s=0, failure=None):
    events, lock, slots = [], threading.Lock(), threading.BoundedSemaphore(2)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        disable_nagle_algorithm = True

        def do_POST(self):
            payload = self.rfile.read(int(self.headers["Content-Length"]))
            row, stage = identity(payload)
            with slots:
                with lock:
                    events.append(dict(event="start", ns=time.monotonic_ns(), row=row,
                                       stage=stage, payload=base64.b64encode(payload).decode()))
                time.sleep(unit_s * costs[row][stage])
                response = dict(id=f"row:{row}:{stage}", object="chat.completion", created=1,
                    model=json.loads(payload)["model"],
                    choices=[dict(index=0, message=dict(role="assistant", content=f"row:{row}:stage:{stage}"),
                                  finish_reason="stop", logprobs=dict(content=[]))],
                    usage=dict(prompt_tokens=7, completion_tokens=3, total_tokens=10,
                               prompt_tokens_details=dict(cached_tokens=2)),
                    provider_extra=dict(retained=True))
                status, body = (failure(row, stage, response) if failure
                                else (200, encode(response)))
                # Fixed headers permit byte-for-byte full-response comparisons across arms.
                self.send_response_only(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                self.wfile.flush()
                with lock:
                    events.append(dict(event="end", ns=time.monotonic_ns(), row=row, stage=stage,
                                       status=status, body=base64.b64encode(body).decode()))

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=.01))
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1", events
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        if thread.is_alive():
            raise RuntimeError("completion fixture did not stop")


class OwnerCase:
    """Test-only runner; retains the real owner/backend between paired queries."""

    def __init__(self, base, costs=POSITIVE, window=3):
        self.costs, self.window = costs, window
        self.core_events = []
        self.config = config(base, "gpt-4o-mini")
        self.execution = build_native_execution(self.config, physical=None,
            max_tasks=window, max_active_requests=2, active_work=16,
            observer=self.core_events.append)
        self.lm = model(base, "gpt-4o-mini")

    def close(self):
        close_execution(self.execution)

    def run(self, *, completion_first=False, selector="fifo", iterate=iter_two_map_rows,
            original_advance=None):
        import lotus
        self.lm.reset_stats()
        self.core_events.clear()
        self.execution.engine.policies = replace(self.execution.engine.policies,
            organize=None, choose_task=select_task(selector))
        owner = self
        counters = dict(engine_cpu_ns=0, method_cpu_ns=0, engine_calls=0)
        query_events, tick_actions = [], []
        advance = original_advance or self.execution.engine.advance

        class DeclaredCostMethod(LotusTwoMapMethod):
            def _request(self, state):
                step = super()._request(state)
                row, stage = identity(step.request.payload)
                return replace(step, request=replace(step.request, estimated_work=owner.costs[row][stage]))

        class MeasuredDriver(MethodDriver):
            def advance(self, maximum):
                started = time.thread_time_ns()
                try:
                    return super().advance(maximum)
                finally:
                    counters["method_cpu_ns"] += time.thread_time_ns() - started

        def measured(*args, **kwargs):
            started = time.thread_time_ns()
            try:
                report = advance(*args, **kwargs)
            finally:
                counters["engine_cpu_ns"] += time.thread_time_ns() - started
                counters["engine_calls"] += 1
            tick_actions.append((kwargs.get("dispatch", True), report.events))
            return report

        def rows():
            for index in range(len(self.costs)):
                query_events.append(dict(event="source", row=index, ns=time.monotonic_ns()))
                yield encode(dict(text=f"row:{index}"))

        output = []
        self.lm.reset_stats()
        started, cpu = time.monotonic_ns(), time.thread_time_ns()
        options = {} if completion_first is None else dict(completion_first=completion_first)
        with lotus.settings.context(enable_cache=False), \
             patch.object(self.execution.engine, "advance", measured), \
             patch.dict(iterate.__globals__, MethodDriver=MeasuredDriver):
            method = DeclaredCostMethod(self.lm, STAGES, model_config=self.config)
            for result in iterate(self.execution, method, rows(), query_id="completion-fixture",
                    operator_id="two-map", limits=LIMITS,
                    capacity=MethodCapacity(self.window, self.window * row_reservation(LIMITS)), **options):
                output.append((result.row.sequence, json.loads(result.value)))
                query_events.append(dict(event="yield", row=result.row.sequence, ns=time.monotonic_ns()))
        counters.update(owner_cpu_ns=time.thread_time_ns() - cpu, wall_ns=time.monotonic_ns() - started)
        return dict(**counters, output=sorted(output), stats=asdict(self.lm.stats),
                    events=list(self.core_events), query_events=query_events, tick_actions=tick_actions,
                    usage=asdict(self.execution.engine.capacity.usage()))


@unittest.skipUnless(PINNED, "fixed LOTUS SDK unavailable; no install performed")
class CompletionSdkTests(unittest.TestCase):
    def test_payload_rows_usage_and_full_responses_match_original_maps(self):
        import lotus
        import pandas as pd
        with service() as (base, events):
            native_lm = model(base, "gpt-4o-mini")
            with lotus.settings.context(enable_cache=False):
                native = staged_two_map(pd.DataFrame(dict(text=[f"row:{i}" for i in range(4)])), native_lm, STAGES)
            expected_payloads = payloads(events)
            owner = OwnerCase(base)
            try:
                for first in (None, False, True):
                    for selector in ("fifo", "sjf", "final"):
                        with self.subTest(first=first, selector=selector):
                            events.clear()
                            actual = owner.run(completion_first=first, selector=selector)
                            self.assertEqual([v[1]["row"] for v in actual["output"]], native.to_dict("records"))
                            self.assertEqual(actual["stats"], asdict(native_lm.stats))
                            self.assertEqual(payloads(events), expected_payloads)
                            self.assertEqual(actual["usage"], asdict(Usage()))
                            self.assertEqual(len(actual["output"]), 4)
                            for _, value in actual["output"]:
                                self.assertEqual(len(value["raw_responses"]), 2)
                                for raw in value["raw_responses"]:
                                    full = decode_full_response(base64.b64decode(raw))
                                    self.assertTrue(json.loads(full.body)["provider_extra"]["retained"])
            finally:
                owner.close()

    def test_sdk_errors_preserve_full_response_and_cleanup_in_both_orders(self):
        def missing(row, stage, response):
            response["choices"][0]["message"].pop("content")
            return 200, encode(response)
        cases = ((missing, 200, True),
                 (lambda *args: (200, b"{broken"), 200, False),
                 (lambda *args: (429, b'{"error":{"message":"fixture"}}'), 429, False))
        for first in (False, True):
            for failure, status, used in cases:
                with self.subTest(first=first, status=status, used=used), service(failure=failure) as (base, _):
                    owner = OwnerCase(base)
                    try:
                        with self.assertRaises(Exception) as raised:
                            owner.run(completion_first=first)
                        self.assertEqual(raised.exception.full_response.status_code, status)
                        self.assertEqual(owner.lm.stats.physical_usage.total_tokens > 0, used)
                        self.assertTrue(all(job.closing for job in owner.execution.engine.jobs.jobs.values()))
                    finally:
                        owner.close()
                    self.assertEqual(owner.execution.engine.capacity.usage(), Usage())
                    self.assertEqual(owner.execution.engine.jobs.jobs, {})

    def test_usage_limit_failure_keeps_native_accounting_and_owner_draining(self):
        from lotus.types import LotusUsageLimitException, UsageLimit
        for first in (False, True):
            with self.subTest(first=first), service() as (base, _):
                owner = OwnerCase(base)
                owner.lm.physical_usage_limit = UsageLimit(total_tokens_limit=1)
                try:
                    with self.assertRaises(LotusUsageLimitException) as raised:
                        owner.run(completion_first=first)
                    self.assertEqual(raised.exception.full_response.status_code, 200)
                    self.assertEqual(owner.lm.stats.physical_usage.total_tokens, 10)
                    self.assertEqual(owner.lm.stats.virtual_usage.total_tokens, 10)
                    self.assertTrue(all(job.closing for job in owner.execution.engine.jobs.jobs.values()))
                finally:
                    owner.close()
                self.assertEqual(owner.execution.engine.capacity.usage(), Usage())
                self.assertEqual(owner.execution.engine.jobs.jobs, {})
