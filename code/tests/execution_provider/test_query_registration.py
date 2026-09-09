"""Query control lifetime and shared storage, exercised through production socket workers."""

import json
import asyncio
import threading
import socket
import unittest
from unittest.mock import patch

from src.execution_provider.query_registration import validate_registration, trusted_peer
from src.execution_provider.wire.framing import encode_frame, read_frame, ProtocolError
from tests.execution_provider.test_multisession_gateway import service, Client, completion, wait_for


async def echo(task, endpoint):
    return completion(json.loads(task.task.payload)["messages"][-1]["content"])


def control(path, count):
    conn = socket.socket(socket.AF_UNIX)
    conn.settimeout(3)
    conn.connect(path)
    conn.sendall(encode_frame({"type": "query_open", "binding_version": 1, "flow_count": count}))
    opened = read_frame(conn)
    return conn, opened


def binding(token, flow):
    return {"type": "stream_join", "binding_version": 1, "token": token, "flow": flow}


class QueryRegistrationTests(unittest.TestCase):
    def test_one_grant_survives_gap_between_operator_connections(self):
        with patch("src.execution_provider.multiplexed_gateway.trusted_peer", return_value=(1, 42)):
            with service(echo, max_connections=6) as (path, gateway, events):
                conn, opened = control(path, 2)
                try:
                    for flow in (0, 1):
                        client = Client(path, window=1, binding=binding(opened["token"], flow))
                        try:
                            self.assertEqual(client.offer(str(flow)), 1)
                            client.poll()
                            self.assertEqual(client.result()["raw_output"], str(flow))
                        finally:
                            client.close()
                        wait_for(
                            lambda: (
                                sum(e["event"] == "connection_closed" for e in events) == flow + 1
                            )
                        )
                        self.assertFalse(any(e["event"] == "job_drained" for e in events))
                    jobs = [e for e in events if e["event"] == "job_opened"]
                    joined = [e for e in events if e["event"] == "query_flow_joined"]
                    self.assertEqual(len(jobs), 1)
                    self.assertEqual({e["job_id"] for e in joined}, {jobs[0]["job_id"]})
                    self.assertEqual(jobs[0]["job_budget"]["held_tasks"], 2)
                    self.assertEqual(jobs[0]["job_budget"]["max_sessions"], 2)
                    self.assertNotIn(opened["token"], repr(events))
                finally:
                    conn.close()
                wait_for(lambda: any(e["event"] == "job_drained" for e in events))

    def test_insufficient_query_storage_rejected_before_any_backend_call(self):
        async def forbidden(*args):
            raise AssertionError("registration must not call model")

        with patch("src.execution_provider.multiplexed_gateway.trusted_peer", return_value=(1, 42)):
            with service(forbidden) as (path, gateway, events):
                conn, opened = control(path, 3)
                conn.close()
                self.assertEqual(
                    opened, {"type": "query_error", "binding_version": 1, "code": "QUERY_CAPACITY"}
                )
                self.assertFalse(any(e["event"] == "job_opened" for e in events))

    def test_invalid_membership_cannot_cancel_owner_or_other_query(self):
        with patch(
            "src.execution_provider.multiplexed_gateway.trusted_peer", return_value=(1, 42)
        ) as peer:
            with service(echo, max_connections=6) as (path, gateway, events):
                conn, opened = control(path, 2)
                try:
                    for token, flow, credentials in (
                        ("0" * 64, 0, (1, 42)),
                        (opened["token"], 2, (1, 42)),
                        (opened["token"], 0, (1, 43)),
                    ):
                        peer.return_value = credentials
                        with socket.socket(socket.AF_UNIX) as invalid:
                            invalid.settimeout(3)
                            invalid.connect(path)
                            invalid.sendall(encode_frame(binding(token, flow)))
                            self.assertEqual(
                                read_frame(invalid),
                                {
                                    "type": "query_error",
                                    "binding_version": 1,
                                    "code": "QUERY_MEMBERSHIP",
                                },
                            )
                    peer.return_value = (1, 42)
                    client = Client(path, window=1, binding=binding(opened["token"], 0))
                    client.close()
                    wait_for(lambda: any(e["event"] == "connection_closed" for e in events))
                    with socket.socket(socket.AF_UNIX) as invalid:
                        invalid.settimeout(3)
                        invalid.connect(path)
                        invalid.sendall(encode_frame(binding(opened["token"], 0)))
                        self.assertEqual(
                            read_frame(invalid),
                            {
                                "type": "query_error",
                                "binding_version": 1,
                                "code": "QUERY_MEMBERSHIP",
                            },
                        )
                    other = Client(path)
                    try:
                        other.offer("other")
                        other.poll()
                        self.assertEqual(other.result()["raw_output"], "other")
                    finally:
                        other.close()
                finally:
                    conn.close()

    def test_query_cancel_retains_remote_credit_and_other_query_progresses(self):
        started, release = threading.Event(), threading.Event()

        async def blocked(task, endpoint):
            text = json.loads(task.task.payload)["messages"][-1]["content"]
            if text == "blocked":
                started.set()
                while not release.is_set():
                    await asyncio.sleep(0.001)
            return completion(text)

        with patch("src.execution_provider.multiplexed_gateway.trusted_peer", return_value=(1, 42)):
            with service(blocked, max_connections=6) as (path, gateway, events):
                conn, opened = control(path, 2)
                client = Client(path, window=1, binding=binding(opened["token"], 0))
                try:
                    client.offer("blocked")
                    client.poll()
                    self.assertTrue(started.wait(3))
                    conn.close()
                    wait_for(lambda: any(e["event"] == "connection_closed" for e in events))
                    self.assertEqual(gateway.engine.capacity.usage().active_requests, 1)
                    other = Client(path)
                    try:
                        other.offer("other")
                        other.poll()
                        self.assertEqual(other.result()["raw_output"], "other")
                    finally:
                        other.close()
                finally:
                    conn.close()
                    client.close()
                    release.set()
                wait_for(lambda: len([e for e in events if e["event"] == "job_drained"]) == 2)
                self.assertEqual(gateway.engine.capacity.usage().held_tasks, 0)

    def test_registration_schema_is_strict(self):
        for count in (True, 0, -1, "2", 2**31):
            with self.subTest(count=count), self.assertRaises(ProtocolError):
                validate_registration(
                    {"type": "query_open", "binding_version": 1, "flow_count": count}
                )
        with self.assertRaises(ProtocolError):
            validate_registration({"type": "query_open", "binding_version": True, "flow_count": 1})

    @unittest.skipUnless(hasattr(socket, "SO_PEERCRED"), "Linux peer credential test")
    def test_kernel_peer_identity(self):
        import os

        a, b = socket.socketpair()
        try:
            self.assertEqual(trusted_peer(a), (os.getuid(), os.getpid()))
        finally:
            a.close()
            b.close()
