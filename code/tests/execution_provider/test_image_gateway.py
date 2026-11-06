"""Actual socket workers and core leases, with explicit local image fixtures."""

import socket
import tempfile
import threading
import unittest
import json
from unittest.mock import Mock, patch
from contextlib import contextmanager
from functools import partial
from pathlib import Path

from src.execution_provider.adapters.image_execution import ImageServiceConfig, build_image_execution
from src.execution_provider.image_gateway import ImageGateway
from src.execution_provider.image_connection import ImageConnection
from src.execution_provider.gateway_runtime import ConnectionMailbox, ConnectionState, PendingOffer
from src.execution_provider.wire import image
from src.execution_provider.wire.framing import ProtocolError, encode_frame, read_frame
from src.scheduling.runtime.stage_broker import StageBrokerLimits
from tests.modalities.image.test_incremental_image_backend import Actors, FakeRay, PLAN


@contextmanager
def service(mode="staged", actors=None, *, max_jobs=1, execution_factory=build_image_execution):
    actors = actors or Actors()
    stop, ready = threading.Event(), threading.Event()
    failures, gateways = [], []
    config = ImageServiceConfig(PLAN, mode, StageBrokerLimits(262144, 96, 8, 1, 1), 1, 1, 1, 1000)
    with tempfile.TemporaryDirectory() as directory:
        path = str(Path(directory) / "image.sock")

        class Reference:
            def embed(self, encoded):
                return PLAN.encode_result([1, 0])

        def run():
            gateway = None
            listener = socket.socket(socket.AF_UNIX)
            try:
                gateway = ImageGateway(config, max_jobs=max_jobs, max_connections=3, max_tasks=4,
                    max_active_requests=1 if mode == "reference" else 2, frame_timeout_ms=2000,
                    input_bytes=4 * 262144, result_bytes=32,
                    execution_factory=partial(execution_factory, worker_pool=actors.pool,
                                               ray_api=FakeRay(), reference=Reference()))
                gateways.append(gateway)
                listener.bind(path)
                listener.listen(3)
                ready.set()
                gateway.serve(listener, stop)
            except BaseException as failure:
                failures.append(failure)
                ready.set()
            finally:
                listener.close()
                if gateway is not None and not gateway.close():
                    failures.append(AssertionError("image service did not drain"))

        worker = threading.Thread(target=run)
        worker.start()
        assert ready.wait(3)
        try:
            if failures:
                raise failures[0]
            yield path, gateways[0]
        finally:
            stop.set()
            if gateways:
                gateways[0].request_stop()
            worker.join(5)
            assert not worker.is_alive()
        if failures:
            raise failures[0]


def connect(path, mode, window):
    connection = socket.socket(socket.AF_UNIX)
    connection.settimeout(3)
    connection.connect(path)
    connection.sendall(encode_frame(image.open_message(PLAN, mode, window)))
    opened = read_frame(connection)
    assert opened["type"] == "opened", opened
    return connection


class ImageGatewayTest(unittest.TestCase):
    def test_sync_and_staged_use_identical_result_bytes(self):
        for mode in ("reference", "staged"):
            with self.subTest(mode=mode), service(mode) as (path, gateway):
                connection = connect(path, mode, 1)
                with connection:
                    task = image.task_message(PLAN, mode, 0, b"\x00\xff")
                    connection.sendall(encode_frame(task))
                    if mode == "staged":
                        self.assertTrue(read_frame(connection)["accepted"])
                        connection.sendall(encode_frame({"type": "receive", "protocol_version": 7}))
                    result = read_frame(connection)
                    self.assertEqual(result, image.completion_message(PLAN, mode, 0,
                        task["semantic_payload_digest"], PLAN.encode_result([1, 0])))


    def test_invalid_payload_fails_without_worker_submission(self):
        actors = Actors()
        with service(actors=actors) as (path, gateway):
            with connect(path, "staged", 1) as connection:
                message = image.task_message(PLAN, "staged", 0, b"x")
                message["encoded_hex"] = "ff"
                connection.sendall(encode_frame(message))
                self.assertEqual(read_frame(connection)["type"], "error")
        self.assertEqual(actors.prepares, [])

    def test_open_identity_and_unknown_fields_are_rejected(self):
        message = image.open_message(PLAN, "staged", 1)
        for changed in ({**message, "extra": 1}, {**message, "max_inflight_tasks": True},
                        {**message, "semantic_spec_digest": "0" * 64}):
            with self.assertRaises(ProtocolError):
                image.validate_open(changed, PLAN)

    def test_method_failure_can_close_its_session_without_stopping_shared_service(self):
        actors = Actors()
        actors.bad_result = True
        with service(actors=actors) as (path, gateway):
            with connect(path, "staged", 1) as connection:
                connection.sendall(encode_frame(image.task_message(PLAN, "staged", 0, b"x")))
                self.assertTrue(read_frame(connection)["accepted"])
                connection.sendall(encode_frame({"type": "receive", "protocol_version": 7}))
                self.assertEqual(read_frame(connection)["type"], "error")
            actors.bad_result = False
            with connect(path, "staged", 1) as other:
                other.sendall(encode_frame(image.task_message(PLAN, "staged", 0, b"x")))
                self.assertTrue(read_frame(other)["accepted"])
                other.sendall(encode_frame({"type": "receive", "protocol_version": 7}))
                self.assertEqual(read_frame(other)["type"], "completion")


    def test_slow_consumer_keeps_results_charged_and_other_job_can_finish(self):
        with service(max_jobs=2) as (path, gateway):
            with connect(path, "staged", 2) as slow, connect(path, "staged", 2) as fast:
                for connection in (slow, fast):
                    for sequence in range(2):
                        connection.sendall(encode_frame(image.task_message(PLAN, "staged", sequence, b"x")))
                        self.assertTrue(read_frame(connection)["accepted"])
                for _ in range(2):
                    fast.sendall(encode_frame({"type": "receive", "protocol_version": 7}))
                    self.assertEqual(read_frame(fast)["type"], "completion")
                self.assertGreaterEqual(gateway.method_pool.used[0], 2)
                self.assertGreaterEqual(gateway.method_pool.used[1], 2 * gateway.row_reservation)
                for _ in range(2):
                    slow.sendall(encode_frame({"type": "receive", "protocol_version": 7}))
                    self.assertEqual(read_frame(slow)["type"], "completion")


    def test_blocked_result_send_keeps_method_reservation_after_core_release(self):
        entered, release, wrapped = threading.Event(), threading.Event(), threading.Event()
        original = ImageConnection.run_image

        class SlowSend:
            def __init__(self, connection):
                self.connection = connection
            def __getattr__(self, name):
                return getattr(self.connection, name)
            def sendall(self, data):
                if json.loads(data[4:]).get("type") == "completion":
                    entered.set()
                    if not release.wait(3):
                        raise TimeoutError("controlled slow send was not released")
                self.connection.sendall(data)

        def run_image(adapter, opened):
            if not wrapped.is_set():
                adapter.connection = SlowSend(adapter.connection)
                wrapped.set()
            return original(adapter, opened)

        with patch.object(ImageConnection, "run_image", run_image), service(max_jobs=2) as (path, gateway):
            with connect(path, "staged", 1) as slow:
                slow.sendall(encode_frame(image.task_message(PLAN, "staged", 0, b"x")))
                self.assertTrue(read_frame(slow)["accepted"])
                slow.sendall(encode_frame({"type": "receive", "protocol_version": 7}))
                try:
                    self.assertTrue(entered.wait(2))
                    self.assertEqual(gateway.engine.capacity.usage().held_tasks, 0)
                    self.assertEqual(gateway.method_pool.used, (1, gateway.row_reservation))
                    with connect(path, "staged", 1) as other:
                        other.sendall(encode_frame(image.task_message(PLAN, "staged", 0, b"x")))
                        self.assertTrue(read_frame(other)["accepted"])
                        other.sendall(encode_frame({"type": "receive", "protocol_version": 7}))
                        self.assertEqual(read_frame(other)["type"], "completion")
                finally:
                    release.set()
                self.assertEqual(read_frame(slow)["type"], "completion")


class ImageOwnerTest(unittest.TestCase):
    def make_owner(self, actors):
        config = ImageServiceConfig(PLAN, "staged", StageBrokerLimits(262144, 96, 8, 1, 1),
                                    1, 1, 1, 1000)
        gateway = ImageGateway(config, max_jobs=1, max_connections=3, max_tasks=2,
            max_active_requests=1, frame_timeout_ms=2000, input_bytes=2 * 262144,
            result_bytes=2 * PLAN.result_bytes,
            execution_factory=partial(build_image_execution, worker_pool=actors.pool,
                                      ray_api=FakeRay()))
        mailbox = ConnectionMailbox(None, gateway.engine.wake, 2)
        state = ConnectionState(Mock(), mailbox, None, Mock(), connection_id=0)
        gateway.connections[0] = state
        gateway.connection_capacity.accept(0)
        mailbox.command = ("standalone", ())
        gateway._command(state)
        self.assertIsNone(mailbox.reply)
        self.assertIsNotNone(mailbox.session)
        request = image.validate_task(image.task_message(PLAN, "staged", 0, b"x"),
                                      PLAN, "staged", 0)
        offered = gateway.session_command(state, "offer", (PendingOffer(request, 0),))
        self.assertEqual(offered.accepted_prefix_count, 1)
        return gateway, state

    def test_consumed_image_result_releases_method_and_query_storage(self):
        gateway, state = self.make_owner(Actors())
        try:
            for _ in range(20):
                gateway.engine.advance()
                update = gateway.session_command(state, "advance", (1,))
                if update.results:
                    break
            self.assertEqual(len(update.results), 1)
            result = update.results[0]
            self.assertEqual(result.value, PLAN.encode_result([1, 0]))
            self.assertEqual(gateway.engine.capacity.usage().held_tasks, 0)
            self.assertEqual(gateway.method_pool.used, (1, gateway.row_reservation))
            gateway.session_command(state, "release", ((result.row,),))
            self.assertEqual(gateway.method_pool.used, (0, 0))
            state.mailbox.clean = True
            state.mailbox.done.set()
            gateway._controls()
            gateway.engine.advance()
            self.assertEqual(gateway.connections, {})
            self.assertFalse(gateway.engine.jobs.jobs)
            self.assertEqual(gateway.method_pool.allocated, (0, 0))
            state.worker.join.assert_called_once_with()
            self.assertTrue(gateway.close())
        finally:
            gateway.close()

    def test_cancelled_image_retains_compute_until_the_model_returns(self):
        actors = Actors()
        actors.model_ready = False
        gateway, state = self.make_owner(actors)
        for _ in range(20):
            gateway.engine.advance()
            gateway.session_command(state, "advance", (1,))
            if actors.models:
                break
        self.assertEqual(len(actors.models), 1)
        state.mailbox.cancel()
        gateway._controls()
        self.assertEqual(gateway.engine.capacity.usage().active_requests, 1)
        self.assertEqual(gateway.method_pool.allocated[0], 2)
        state.mailbox.done.set()
        gateway._controls()
        self.assertFalse(gateway.close())
        self.assertEqual(gateway.engine.capacity.usage().active_requests, 1)
        self.assertTrue(gateway.engine.jobs.jobs[state.job].closing)
        actors.models[0].ready = True
        for _ in range(20):
            gateway.engine.advance()
            if not gateway.engine.capacity.usage().held_tasks:
                break
        self.assertFalse(gateway.engine.jobs.jobs)
        self.assertEqual(gateway.method_pool.used, (0, 0))
        self.assertEqual(gateway.method_pool.allocated, (0, 0))
        self.assertTrue(gateway.close())




if __name__ == "__main__":
    unittest.main()
