"""Complete-call HTTP behavior and opt-in actual Daft/Ray Data graphs; no models."""
from contextlib import contextmanager
import base64
import gzip
import itertools
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.util import find_spec
import os
from pathlib import Path
import socket
import threading
import time
import unittest

from src.baselines.text.frameworks.native_calls import (
    NativeCall, NativeCallExecutor, _DAFT_CONTROLS, _GraphControl, _request,
)
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.baselines.common.redact import redact_json_values


_FIXTURE_SEQUENCE = itertools.count()


def record_fixture(name, values):
    output = os.environ.get("SEMLOOM_NATIVE_CALL_ARTIFACT")
    if output:
        root = Path(output)
        root.mkdir(parents=True, exist_ok=True)
        with (root / f"{next(_FIXTURE_SEQUENCE):04}-{name}.json").open("x") as stream:
            json.dump(redact_json_values(dict(schema=1, test_id=os.environ.get("SEMLOOM_NATIVE_CALL_CASE"),
                real_model_requests=0, **values)), stream, indent=2)


@contextmanager
def complete_server(status=200, body=b"not-json", *, compressed=False, delay=0):
    class Calls(list):
        active = peak = 0
        started = threading.Event()
        lock = threading.Lock()
    calls = Calls()
    raw = gzip.compress(body) if compressed else body

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            payload = self.rfile.read(int(self.headers["Content-Length"]))
            with calls.lock:
                calls.append((self.path, payload, self.headers.get_all("X-Input")))
                calls.active += 1
                calls.peak = max(calls.peak, calls.active)
            calls.started.set()
            try:
                time.sleep(delay)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("X-Repeated", "one")
                self.send_header("X-Repeated", "two")
                if compressed:
                    self.send_header("Content-Encoding", "gzip")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with calls.lock:
                    calls.active -= 1

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/complete?query=1", calls, raw
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        if thread.is_alive():
            raise RuntimeError("complete-call fixture did not stop")
        record_fixture("complete-http", dict(status=status,
            raw_response=base64.b64encode(raw).decode(), compressed=compressed,
            actual_posts=len(calls), active_after_close=calls.active, peak_active=calls.peak,
            requests=[dict(path=path, raw_body=base64.b64encode(body).decode(), repeated_headers=headers)
                      for path, body, headers in calls]))


def call(endpoint, index=0, *, timeout=5, maximum=8192):
    return NativeCall("call:" + str(index), index, b'{ "messages" : ["duplicate"] }', endpoint,
        (("Content-Type", "application/json"), ("X-Input", "one"), ("X-Input", "two")),
        timeout, max_response_bytes=maximum)


class NativeHttpTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        os.environ["SEMLOOM_NATIVE_CALL_CASE"] = self.id()

    async def test_exact_body_duplicate_headers_and_compressed_http_error_remain_values(self):
        with complete_server(429, compressed=True) as (endpoint, calls, raw):
            outcome = await _request(call(endpoint))
            self.assertEqual(calls, [("/complete?query=1", call(endpoint).payload, ["one", "two"])])
            self.assertEqual(outcome.response.status_code, 429)
            self.assertEqual(outcome.response.body, raw)
            self.assertEqual(outcome.response.http_version, "HTTP/1.1")
            self.assertEqual([v for k, v in outcome.response.headers if k.lower() == "x-repeated"], ["one", "two"])
            self.assertEqual(outcome.transport_error, "")

    async def test_oversized_body_is_hard_failure_and_connection_error_has_no_http_status(self):
        with complete_server(200, b"too-large") as (endpoint, calls, raw):
            with self.assertRaisesRegex(ValueError, "byte allowance"):
                await _request(call(endpoint, maximum=2))
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        outcome = await _request(call(f"http://127.0.0.1:{port}/closed", timeout=1))
        self.assertIsNone(outcome.response)
        self.assertIn("ClientConnectorError", outcome.transport_error)

    async def test_cancelled_before_kernel_sends_no_request(self):
        with complete_server() as (endpoint, calls, raw):
            control = _GraphControl()
            control.stop()
            outcome = await _request(call(endpoint), control)
            self.assertIsNone(outcome.response)
            self.assertEqual(outcome.transport_error, "NativeCallCancelled")
            self.assertEqual(calls, [])


ENABLED = os.environ.get("SEMLOOM_NATIVE_CALL_TEST") == "1" and all(
    find_spec(name) is not None for name in ("daft", "ray", "pyarrow"))
_DAFT_TEST_RUNTIME_READY = False


def prepare_native_test_runtime():
    global _DAFT_TEST_RUNTIME_READY
    if not _DAFT_TEST_RUNTIME_READY:
        import daft
        daft.set_runner_native(num_threads=8)
        _DAFT_TEST_RUNTIME_READY = True


@unittest.skipUnless(ENABLED, "actual pinned native graph validation is explicitly opt-in")
class NativeGraphTests(unittest.TestCase):
    BACKENDS = ("daft", "ray-data")

    def setUp(self):
        os.environ["SEMLOOM_NATIVE_CALL_CASE"] = self.id()
    @classmethod
    def setUpClass(cls):
        import ray
        prepare_native_test_runtime()
        if ray.is_initialized():
            raise RuntimeError("native graph tests require their own Ray runtime")
        root = Path(os.environ["SEMLOOM_NATIVE_CALL_RAY_TMP"])
        root.mkdir(parents=True, exist_ok=True)
        ray.init(num_cpus=8, num_gpus=0, include_dashboard=False, object_store_memory=134217728,
                 _temp_dir=str(root))
        cls.options = NativeGraphOptions(concurrency=4, ray_actors=2,
            ray_async_batches_per_actor=2, ray_batch_rows=1, source_blocks=4)

    @classmethod
    def tearDownClass(cls):
        import ray
        ray.shutdown()

    def test_native_graphs_preserve_duplicate_occurrences_and_full_http_errors(self):
        for backend in self.BACKENDS:
            with self.subTest(backend=backend), complete_server(429, compressed=True) as (endpoint, calls, raw):
                executor = NativeCallExecutor(backend, self.options)
                outcomes = tuple(executor.iter_batch(tuple(call(endpoint, i) for i in range(7))))
                self.assertEqual({(o.call_id, o.row_id) for o in outcomes}, {(f"call:{i}", i) for i in range(7)})
                self.assertTrue(all(o.response.status_code == 429 and o.response.body == raw for o in outcomes))
                self.assertEqual(len(calls), 7)
                self.assertLessEqual(calls.peak, self.options.concurrency)
                self.assertTrue(executor.last_close_report["graph_consumer_stopped"])
                self.assertTrue(executor.last_close_report["clean"])
                self.assertEqual(_DAFT_CONTROLS, {})

    def test_provider_errors_are_values_but_byte_limit_failure_stops_the_graph(self):
        for backend in self.BACKENDS:
            with self.subTest(backend=backend), complete_server(body=b"long-body") as (endpoint, calls, raw):
                executor = NativeCallExecutor(backend, self.options)
                with self.assertRaises(Exception):
                    tuple(executor.iter_batch(tuple(call(endpoint, i, maximum=2) for i in range(3))))
                self.assertTrue(executor.last_close_report["graph_consumer_stopped"])
                self.assertEqual(_DAFT_CONTROLS, {})

    def test_first_consumer_stop_closes_its_graph_and_waits_for_fixture_activity(self):
        for backend in self.BACKENDS:
            with self.subTest(backend=backend), complete_server(delay=0.1) as (endpoint, calls, raw):
                executor = NativeCallExecutor(backend, self.options)
                iterator = executor.iter_batch(tuple(call(endpoint, i) for i in range(24)))
                first = next(iterator)
                self.assertIsNotNone(first.response)
                iterator.close()
                stopped_count = len(calls)
                limit = time.monotonic() + 6
                while calls.active and time.monotonic() < limit:
                    time.sleep(0.01)
                self.assertEqual(calls.active, 0)
                time.sleep(0.1)
                self.assertEqual(len(calls), stopped_count)
                self.assertLessEqual(stopped_count, 24)
                self.assertTrue(executor.last_close_report["graph_consumer_stopped"])
                self.assertFalse(executor.last_close_report["clean"])
                self.assertEqual(executor.last_close_report["remote_http_stopped"], "unavailable")
                self.assertEqual(_DAFT_CONTROLS, {})

    def test_cancellation_and_timeout_interrupt_blocked_native_iteration(self):
        for backend in self.BACKENDS:
            for stop in ("cancel", "timeout"):
                with self.subTest(backend=backend, stop=stop), complete_server(delay=1) as (endpoint, calls, raw):
                    executor = NativeCallExecutor(backend, self.options)
                    options = (dict(cancelled=calls.started.is_set) if stop == "cancel"
                               else dict(timeout_s=0.05))
                    with self.assertRaises(InterruptedError if stop == "cancel" else TimeoutError):
                        tuple(executor.iter_batch(tuple(call(endpoint, i) for i in range(8)), **options))
                    limit = time.monotonic() + 6
                    while calls.active and time.monotonic() < limit:
                        time.sleep(0.01)
                    self.assertEqual(calls.active, 0)
                    self.assertTrue(executor.last_close_report["graph_consumer_stopped"])


@unittest.skipUnless(ENABLED, "actual pinned Ray Core reference validation is explicitly opt-in")
class RayCoreReferenceTests(NativeGraphTests):
    BACKENDS = ("ray",)

    def test_actor_cpu_allowance_and_termination_belong_to_this_reference(self):
        import ray
        with complete_server(delay=0.01) as (endpoint, calls, raw):
            too_many = NativeGraphOptions(concurrency=9, num_threads=8)
            executor = NativeCallExecutor("ray", too_many)
            with self.assertRaisesRegex(ValueError, "logical CPU"):
                tuple(executor.iter_batch((call(endpoint),)))
            self.assertEqual(calls, [])
            self.assertEqual(executor.last_close_report["owned_ray_actors"], 0)
            executor = NativeCallExecutor("ray", self.options)
            outcomes = tuple(executor.iter_batch(tuple(call(endpoint, i) for i in range(7))))
            self.assertEqual(len(outcomes), 7)
            report = executor.last_close_report
            self.assertEqual(report["owned_ray_actors"], self.options.concurrency)
            self.assertEqual(report["ray_actor_shutdown_requested"], self.options.concurrency)
            deadline = time.monotonic() + 6
            states = {}
            while time.monotonic() < deadline:
                all_actors = ray._private.state.actors()
                states = {key: all_actors.get(key, {}).get("State") for key in report["owned_ray_actor_ids"]}
                if (all(value == "DEAD" for value in states.values())
                        and ray.available_resources().get("CPU", 0) == ray.cluster_resources()["CPU"]):
                    break
                time.sleep(0.02)
            self.assertTrue(all(value == "DEAD" for value in states.values()), states)
            self.assertEqual(ray.available_resources().get("CPU", 0), ray.cluster_resources()["CPU"])
            record_fixture("ray-core-lifecycle", dict(report=report, actor_states=states,
                available_cpus=ray.available_resources().get("CPU", 0), actual_posts=len(calls), peak_active=calls.peak))


if __name__ == "__main__":
    unittest.main()
