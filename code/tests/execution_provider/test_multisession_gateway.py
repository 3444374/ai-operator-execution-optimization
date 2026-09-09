"""Actual socket workers share one owner; slow or failed clients cannot own another Job's slice."""

import asyncio
from contextlib import contextmanager
from functools import partial
import json
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.execution_provider.multiplexed_gateway import MultiSessionMapGateway
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.session_dispatch import run_session
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.wire import v6
from src.execution_provider.wire.framing import encode_frame, read_frame


def completion(text):
    return json.dumps(
        {
            "model": "model",
            "choices": [{"message": {"content": text}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 2, "completion_tokens": 1},
        }
    ).encode()


def wait_for(predicate, timeout=3):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("condition timed out")
        time.sleep(0.005)


@contextmanager
def service(
    execute,
    *,
    unknown=False,
    max_jobs=2,
    execution_factory=None,
    max_tasks=4,
    max_active_requests=2,
    result_bytes=None,
    max_connections=3,
):
    stop, ready = threading.Event(), threading.Event()
    failures, instances, events = [], [], []
    with tempfile.TemporaryDirectory() as directory:
        path = str(Path(directory) / "gateway.sock")

        def serve():
            gateway = None
            listener = socket.socket(socket.AF_UNIX)
            try:
                gateway = MultiSessionMapGateway(
                    FixedModelConfig("http://localhost/v1/chat/completions", "model", 100),
                    max_jobs=max_jobs,
                    max_connections=max_connections,
                    max_tasks=max_tasks,
                    max_active_requests=max_active_requests,
                    result_bytes=result_bytes,
                    frame_timeout_ms=3000,
                    execute=execute,
                    observer=events.append,
                    execution_factory=execution_factory,
                )
                instances.append(gateway)
                listener.bind(path)
                listener.listen(3)
                ready.set()
                handler = partial(
                    run_session,
                    completion_adapter=gateway,
                    response_delay_ms=0,
                    tamper_evidence_digest=False,
                    disconnect_on_task=False,
                    completion_fixture=None,
                )
                gateway.serve(listener, stop, handler=handler)
            except BaseException as exc:
                failures.append(exc)
                ready.set()
            finally:
                listener.close()
                if gateway is not None:
                    gateway.close()

        worker = threading.Thread(target=serve)
        worker.start()
        assert ready.wait(3)
        if not instances:
            raise failures[0]
        try:
            yield path, instances[0], events
        finally:
            stop.set()
            instances[0].request_stop()
            worker.join(6)
            assert not worker.is_alive()
        if unknown:
            assert len(failures) == 1 and "unconfirmed" in str(failures[0]), failures
        elif failures:
            raise failures[0]


class Client:
    def __init__(self, path, *, open_now=True, window=2, binding=None):
        self.socket = socket.socket(socket.AF_UNIX)
        self.socket.settimeout(3)
        self.socket.connect(path)
        if binding is not None:
            self.send(binding)
            assert read_frame(self.socket) == {"type": "stream_joined", "binding_version": 1}
        self.plan = SemanticMapPlan("Echo.", "model", 8)
        self.sequence = 0
        self.window = window
        self.send(v6.build_open_message(self.plan))
        if open_now:
            self.opened()

    def send(self, message):
        self.socket.sendall(encode_frame(message))

    def opened(self):
        result = read_frame(self.socket)
        assert result["type"] == "opened" and result["max_inflight_tasks"] == self.window, result

    def offer(self, text):
        self.send(v6.build_task_message(self.plan, sequence=self.sequence, input_value=text))
        accepted = read_frame(self.socket)["accepted_prefix_count"]
        self.sequence += accepted
        return accepted

    def poll(self):
        self.send({"type": "poll", "protocol_version": 6})

    def result(self):
        return read_frame(self.socket)

    def close(self):
        self.socket.close()


class MultiSessionGatewayTests(unittest.TestCase):
    def test_one_job_reuses_service_loop_and_recovers_for_next_connection(self):
        async def execute(task, endpoint):
            return completion(json.loads(task.task.payload)["messages"][-1]["content"])

        with service(execute, max_jobs=1) as (path, gateway, events):
            for name in ("first", "second"):
                client = Client(path, window=4)
                try:
                    for index in range(4):
                        self.assertEqual(client.offer(f"{name}-{index}"), 1)
                    observed = set()
                    for _ in range(4):
                        client.poll()
                        observed.add(client.result()["raw_output"])
                    self.assertEqual(observed, {f"{name}-{index}" for index in range(4)})
                finally:
                    client.close()
            wait_for(lambda: len([e for e in events if e["event"] == "job_drained"]) == 2)
            self.assertEqual(gateway.engine.capacity.usage().held_tasks, 0)

    def test_execution_policy_owns_job_grants(self):
        from src.execution_provider.adapters.incremental_execution import (
            build_fixed_model_execution,
        )
        from src.scheduling.core.session_jobs import JobBudget

        def allocate(engine):
            limits = engine.capacity.limits
            # An intentionally unequal grant proves the wire layer does not divide capacity.
            count = 3 if not engine.jobs.jobs else 1
            return JobBudget(
                count, count * limits.item_input_bytes, count * limits.item_result_bytes, 1, 1
            )

        def factory(config, **kwargs):
            return build_fixed_model_execution(config, allocate_job=allocate, **kwargs)

        async def execute(task, endpoint):
            return completion("ok")

        with service(execute, execution_factory=factory) as (path, gateway, events):
            a, b = Client(path, window=3), Client(path, window=1)
            try:
                for index in range(3):
                    self.assertEqual(a.offer(str(index)), 1)
                self.assertEqual(a.offer("overflow"), 0)
                self.assertEqual(b.offer("B"), 1)
                b.poll()
                self.assertEqual(b.result()["raw_output"], "ok")
            finally:
                a.close()
                b.close()

    def test_concurrent_connections_share_capacity_and_keep_results(self):
        active = set()
        peak = 0

        async def execute(task, endpoint):
            nonlocal peak
            active.add(task.key)
            peak = max(peak, len(active))
            await asyncio.sleep(0.02)
            active.remove(task.key)
            return completion(json.loads(task.task.payload)["messages"][-1]["content"])

        with service(execute) as (path, gateway, events):
            a, b = Client(path), Client(path)
            try:
                for value in ("A0", "A1"):
                    self.assertEqual(a.offer(value), 1)
                self.assertEqual(a.offer("overflow"), 0)
                for value in ("B0", "B1"):
                    self.assertEqual(b.offer(value), 1)
                results_a, results_b = set(), set()
                for _ in range(2):
                    a.poll()
                    b.poll()
                    results_a.add(a.result()["raw_output"])
                    results_b.add(b.result()["raw_output"])
                self.assertEqual(results_a, {"A0", "A1"})
                self.assertEqual(results_b, {"B0", "B1"})
                self.assertEqual(peak, 2)
            finally:
                a.close()
                b.close()
            wait_for(lambda: len([e for e in events if e["event"] == "job_drained"]) == 2)
            self.assertEqual(gateway.engine.capacity.usage().held_tasks, 0)

    def test_full_input_and_pending_registration_do_not_block_other_job(self):
        async def execute(task, endpoint):
            return completion("B-ok")

        with service(execute) as (path, gateway, events):
            a, b = Client(path), Client(path)
            c = None
            try:
                self.assertEqual(a.offer("A0"), 1)
                self.assertEqual(a.offer("A1"), 1)
                self.assertEqual(a.offer("overflow"), 0)
                c = Client(path, open_now=False)
                self.assertEqual(b.offer("B"), 1)
                b.poll()
                self.assertEqual(b.result()["raw_output"], "B-ok")
                self.assertLessEqual(len(gateway.connections) + len(gateway.waiting), 3)
                a.close()
                c.opened()
                self.assertEqual(c.offer("C"), 1)
                c.poll()
                self.assertEqual(c.result()["type"], "completion")
            finally:
                a.close()
                b.close()
                if c:
                    c.close()

    def test_slow_send_retains_lease_while_other_job_runs(self):
        sending = threading.Event()
        original = socket.socket.sendall

        def send(sock, frame, *args, **kwargs):
            if len(frame) > 50000 and b'"type":"completion"' in frame:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
                sending.set()
            return original(sock, frame, *args, **kwargs)

        async def execute(task, endpoint):
            value = json.loads(task.task.payload)["messages"][-1]["content"]
            return completion("A" + "\n" * 60000 if value.startswith("A") else "B-ok")

        with (
            patch.object(socket.socket, "sendall", send),
            service(execute) as (path, gateway, events),
        ):
            a, b = Client(path), Client(path)
            try:
                a.offer("A")
                a.poll()
                self.assertTrue(sending.wait(3))
                b.offer("B")
                b.poll()
                self.assertEqual(b.result()["raw_output"], "B-ok")
                self.assertGreater(gateway.engine.capacity.usage().held_tasks, 0)
                a.close()
                wait_for(lambda: any(e["event"] == "job_drained" for e in events))
                b.offer("B-again")
                b.poll()
                self.assertEqual(b.result()["raw_output"], "B-ok")
            finally:
                a.close()
                b.close()

    def test_identified_model_timeout_is_local_and_keeps_unknown_credit(self):
        async def execute(task, endpoint):
            value = json.loads(task.task.payload)["messages"][-1]["content"]
            if value == "A":
                raise TimeoutError("remote outcome unknown")
            return completion("B-ok")

        with service(execute, unknown=True) as (path, gateway, events):
            a, b = Client(path), Client(path)
            try:
                a.offer("A")
                b.offer("B")
                a.poll()
                b.poll()
                self.assertEqual(a.result()["code"], "MODEL_TIMEOUT")
                self.assertEqual(b.result()["raw_output"], "B-ok")
                self.assertIsNone(gateway.engine.error)
                self.assertEqual(gateway.engine.capacity.usage().active_requests, 1)
                b.offer("B-again")
                b.poll()
                self.assertEqual(b.result()["raw_output"], "B-ok")
            finally:
                a.close()
                b.close()
