"""Idle vs partial frames, connection promises and failed publication through real core APIs."""

import socket
import threading
import time
import unittest
from unittest.mock import patch
from src.execution_provider.wire.framing import read_frame, RegisteredStream
from src.execution_provider.query_registration import QueryRegistry
from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
from src.execution_provider.adapters.model_config import FixedModelConfig
from tests.execution_provider.test_query_registration import control, binding, echo
from tests.execution_provider.test_multisession_gateway import service, Client, wait_for


class QueryLifecycleTests(unittest.TestCase):
    def test_idle_stream_survives_but_partial_frame_times_out(self):
        a, b = socket.socketpair()
        a.settimeout(0.05)
        result = []

        def receive():
            try:
                result.append(read_frame(RegisteredStream(a)))
            except TimeoutError:
                result.append("timeout")

        worker = threading.Thread(target=receive)
        try:
            worker.start()
            time.sleep(0.12)
            self.assertTrue(worker.is_alive())
            b.sendall(b"\x00")
            worker.join(1)
            self.assertEqual(result, ["timeout"])
            self.assertEqual(a.gettimeout(), 0.05)
        finally:
            a.close()
            b.close()
            worker.join(1)

    def test_reserved_stream_survives_competing_standalone_and_half_handshake(self):
        with patch("src.execution_provider.multiplexed_gateway.trusted_peer", return_value=(1, 42)):
            with service(echo, max_connections=4, frame_timeout_ms=200) as (path, gateway, events):
                owner, opened = control(path, 2)
                first = Client(path, window=1, binding=binding(opened["token"], 0))
                try:
                    first.offer("first")
                    first.poll()
                    self.assertEqual(first.result()["raw_output"], "first")
                    time.sleep(0.4)
                    first.offer("again")
                    first.poll()
                    self.assertEqual(first.result()["raw_output"], "again")
                    outsider = Client(path, open_now=False)
                    try:
                        self.assertIsNone(read_frame(outsider.socket))
                    finally:
                        outsider.close()
                    with socket.socket(socket.AF_UNIX) as slow:
                        slow.connect(path)
                        slow.sendall(b"\x00")
                        # The second stream waits behind the bounded handshake, not a stolen grant.
                        second = Client(path, window=1, binding=binding(opened["token"], 1))
                        try:
                            second.offer("second")
                            second.poll()
                            self.assertEqual(second.result()["raw_output"], "second")
                        finally:
                            second.close()
                    self.assertLessEqual(len(gateway.connections), 4)
                finally:
                    first.close()
                    owner.close()
                wait_for(lambda: any(e["event"] == "job_drained" for e in events))

    def test_control_rejects_even_one_unexpected_byte(self):
        with patch("src.execution_provider.multiplexed_gateway.trusted_peer", return_value=(1, 42)):
            with service(echo, max_connections=4) as (path, gateway, events):
                owner, opened = control(path, 2)
                try:
                    owner.sendall(b"\x00")
                    self.assertIsNone(read_frame(owner))
                finally:
                    owner.close()
                wait_for(lambda: any(e["event"] == "job_drained" for e in events))

    def test_registration_and_join_roll_back_unpublished_resources(self):
        execution = build_fixed_model_execution(
            FixedModelConfig("http://localhost/v1/chat/completions", "model", 100), max_tasks=2
        )
        registry = QueryRegistry(execution)

        class BadMap(dict):
            def __setitem__(self, key, value):
                super().__setitem__(key, value)
                raise RuntimeError("publication failed")

        class BadSet(set):
            def add(self, value):
                super().add(value)
                raise RuntimeError("publication failed")

        try:
            with patch(
                "src.execution_provider.query_registration.secrets.token_hex",
                side_effect=RuntimeError,
            ):
                with self.assertRaises(RuntimeError):
                    registry.register((1, 42), 2)
            self.assertFalse(execution.engine.jobs.jobs)
            registry.registrations = BadMap()
            with self.assertRaises(RuntimeError):
                registry.register((1, 42), 2)
            self.assertFalse(execution.engine.jobs.jobs)
            self.assertFalse(registry.registrations)
            registry.registrations = {}
            registration, _ = registry.register((1, 42), 2)
            registration.joined = BadSet()
            with self.assertRaises(RuntimeError):
                registry.join((1, 42), registration.token, 0)
            self.assertFalse(registration.joined)
            registration.joined = set()
            # A leaked session would exhaust this Job's two-session allowance.
            _, a = registry.join((1, 42), registration.token, 0)
            _, b = registry.join((1, 42), registration.token, 1)
            a.close_consumer()
            b.close_consumer()
            registry.close(registration)
            self.assertFalse(execution.engine.jobs.jobs)
            self.assertEqual(execution.engine.capacity.usage().held_tasks, 0)
        finally:
            execution.close()
