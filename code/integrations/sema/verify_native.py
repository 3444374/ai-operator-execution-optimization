#!/usr/bin/env python3
"""Check pinned native graphs and live Sema HTTP delivery with zero model calls."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib
import ipaddress
import itertools
import inspect
import json
import os
from pathlib import Path
import re
import socket
import sys
import threading
import time
import unittest
from unittest import mock
from urllib import error, request
from urllib.parse import urlsplit

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))

from src.baselines.common.private_artifacts import new_private_directory, open_private_text, write_private_json
from src.baselines.common.redact import redact_json_values, redact_text
from src.baselines.text.frameworks.native_calls import NativeCallExecutor, _DAFT_CONTROLS
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.sema_native import SemaNativeLimits
from src.infrastructure.daft_runtime import configure_daft_runner
from tests.baselines import test_native_calls as native_http_fixtures
from tests.baselines.test_native_calls import call, record_fixture

_SERVICE_SEQUENCE = itertools.count()


def validate_ray_address(address):
    """Accept one declared local IPv4 interface, never a remotely selected cluster."""
    match = re.fullmatch(r"(localhost|[0-9.]+):([0-9]{1,5})", address)
    if match is None or not 1 <= int(match.group(2)) <= 65535:
        raise ValueError('Ray needs one explicit local IPv4 address and port')
    host = match.group(1)
    if host == 'localhost':
        return
    value = ipaddress.IPv4Address(host)
    if value.is_loopback:
        return
    import psutil
    local = {entry.address for entries in psutil.net_if_addrs().values()
             for entry in entries if entry.family == socket.AF_INET}
    if host not in local:
        raise ValueError('Ray address is not assigned to a local interface')


@contextmanager
def complete_server(*args, fixture_port=0, **kwargs):
    """Reuse the shared real HTTP fixture with this verifier's chosen bind port."""
    def server_factory(address, handler):
        if address != ("127.0.0.1", 0):
            raise RuntimeError("shared complete fixture changed its loopback binding contract")
        return ThreadingHTTPServer(("127.0.0.1", fixture_port), handler)

    with mock.patch.object(native_http_fixtures, "ThreadingHTTPServer", server_factory):
        with native_http_fixtures.complete_server(*args, **kwargs) as fixture:
            yield fixture


@contextmanager
def structured_response_server(*, fixture_port=0):
    """Return a complete chat envelope, including fields the original parser owns."""
    envelope = dict(id="fixture-completion", object="chat.completion", model="fixture",
        choices=[dict(index=0, message=dict(role="assistant", content="result"), finish_reason="stop",
                      logprobs=dict(content=[dict(token="result", logprob=-0.25)]))],
        usage=dict(prompt_tokens=11, completion_tokens=1, total_tokens=12))
    raw = json.dumps(envelope, indent=2).encode() + b"\n"
    calls = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            calls.append(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-Repeated", "one")
            self.send_header("X-Repeated", "two")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", fixture_port), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.05), daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/structured", calls, raw, envelope
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        if thread.is_alive():
            raise RuntimeError("structured fixture did not stop")
        record_fixture("structured-http", dict(actual_posts=len(calls), body_sha256=hashlib.sha256(raw).hexdigest()))


@contextmanager
def staggered_server(*, fixture_port=0):
    """Make response completion differ from request occurrence order."""
    completed, requests = [], []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            sequence = int(self.headers["X-Order"])
            with lock:
                requests.append((sequence, body))
            time.sleep(0.4 if sequence == 0 else 0.005)
            result = body + b":" + str(sequence).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(result)))
            self.end_headers()
            self.wfile.write(result)
            with lock:
                completed.append(sequence)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", fixture_port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/staggered", requests, completed
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        if thread.is_alive():
            raise RuntimeError("staggered fixture did not stop")
        record_fixture("staggered-http", dict(actual_posts=len(requests), completed_order=completed))

def check_outcomes(outcomes, expected, status, body):
    """Validate occurrences independently of the graph's delivery order."""
    if len(outcomes) != len(expected) or {(o.call_id, o.row_id) for o in outcomes} != expected:
        raise AssertionError("native graph lost or repeated an input occurrence")
    for outcome in outcomes:
        response = outcome.response
        if outcome.transport_error or response is None:
            raise AssertionError("native graph replaced an HTTP response with a transport error")
        if (response.status_code, response.body, response.http_version) != (status, body, "HTTP/1.1"):
            raise AssertionError("native graph changed the complete HTTP response")
        if [v for k, v in response.headers if k.lower() == "x-repeated"] != ["one", "two"]:
            raise AssertionError("native graph changed repeated response headers")


def library_identity(backend):
    modules = {"daft": ("daft", "daft.runners.native_runner"),
               "ray-data": ("ray", "ray.data._internal.planner.plan_udf_map_op",
                            "ray.llm._internal.batch.stages.http_request_stage",
                            "ray.data._internal.execution.streaming_executor")}[backend]
    result = {}
    for name in modules:
        module = importlib.import_module(name)
        source = Path(module.__file__)
        result[name] = dict(version=getattr(module, "__version__", None),
                            source_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
    if backend == "daft":
        source = Path(inspect.getfile(importlib.import_module("daft").func.batch))
        result["daft.func.batch"] = dict(source_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
    expected = "0.7.21" if backend == "daft" else "2.56.1"
    if result[modules[0]]["version"] != expected:
        raise RuntimeError("native validation requires the pinned library version")
    return result


class NativeSemaChecks(unittest.TestCase):
    backend = ""
    artifact_root = None
    observations = None
    native_limits = SemaNativeLimits(snapshot_rows=8)
    fixture_port = service_port = 0
    options = NativeGraphOptions(concurrency=4, ray_actors=2,
        ray_async_batches_per_actor=2, ray_batch_rows=1, source_blocks=4)

    def record(self, **values):
        self.observations.append(dict(test=self._testMethodName, **values))

    def executor(self):
        return NativeCallExecutor(self.backend, self.options, max_batch_rows=128)

    def settled(self, executor, *, unconfirmed=False):
        self.assertTrue(executor.last_close_report["graph_consumer_stopped"])
        self.assertIs(executor.last_close_report["unconfirmed_work"], unconfirmed)
        self.assertIs(executor.last_close_report["reuse_allowed"], not unconfirmed)
        self.assertIs(executor.last_close_report["clean"], not unconfirmed)
        self.assertEqual(_DAFT_CONTROLS, {})
        self.record(close_report=executor.last_close_report)

    def refuses_reuse(self, executor, url, calls):
        count = len(calls)
        report = dict(executor.last_close_report)
        with self.assertRaisesRegex(RuntimeError, "unconfirmed work"):
            tuple(executor.iter_batch((call(url, 100),)))
        self.assertEqual(executor.last_close_report, report)
        self.assertEqual(len(calls), count)
        self.record(reuse_refused=True, actual_posts_after_refused_reuse=len(calls))

    def test_graph_full_errors_duplicates_and_actual_http_peak(self):
        for status in (200, 429):
            with self.subTest(status=status), complete_server(status, compressed=True, delay=0.05,
                    fixture_port=self.fixture_port) as (url, calls, raw):
                executor = self.executor()
                outcomes = tuple(executor.iter_batch(tuple(call(url, i) for i in range(12))))
                check_outcomes(outcomes, {(f"call:{i}", i) for i in range(12)}, status, raw)
                self.assertEqual(len(calls), 12)
                self.assertTrue(all(body == call(url).payload and headers == ["one", "two"]
                                    for _, body, headers in calls))
                self.assertGreater(calls.peak, 1)
                self.assertLessEqual(calls.peak, self.options.concurrency)
                self.settled(executor)
                if status == 429:
                    reused = tuple(executor.iter_batch((call(url, 100),)))
                    check_outcomes(reused, {("call:100", 100)}, status, raw)
                    self.assertEqual(len(calls), 13)
                    self.settled(executor)
                self.record(status=status, actual_posts=len(calls), actual_peak_http=calls.peak,
                            malformed_json_body=True, automatic_retries=0,
                            explicit_reuse_posts=1 if status == 429 else 0)

    def test_graph_transport_timeout_is_a_value_without_retry(self):
        with complete_server(delay=0.5, fixture_port=self.fixture_port) as (url, calls, _raw):
            executor = self.executor()
            outcomes = tuple(executor.iter_batch(tuple(call(url, i, timeout=0.05) for i in range(4))))
            self.assertEqual(len(calls), 4)
            self.assertEqual(len(outcomes), 4)
            self.assertTrue(all(o.response is None and "TimeoutError" in o.transport_error for o in outcomes))
            self.settled(executor, unconfirmed=True)
            self.refuses_reuse(executor, url, calls)
            self.record(actual_posts=len(calls), transport_errors=4, automatic_retries=0)

    def test_graph_preserves_usage_logprobs_and_complete_chat_envelope(self):
        with structured_response_server(fixture_port=self.fixture_port) as (url, calls, raw, envelope):
            executor = self.executor()
            outcomes = tuple(executor.iter_batch(tuple(call(url, i) for i in range(2))))
            check_outcomes(outcomes, {(f"call:{i}", i) for i in range(2)}, 200, raw)
            self.assertTrue(all(json.loads(outcome.response.body) == envelope for outcome in outcomes))
            expected_headers = (("Content-Type", "application/json"), ("X-Repeated", "one"),
                                ("X-Repeated", "two"), ("Content-Length", str(len(raw))))
            for outcome in outcomes:
                self.assertEqual(tuple((key, value) for key, value in outcome.response.headers
                    if key.lower() not in ("server", "date")), expected_headers)
            self.assertEqual(calls, [call(url).payload] * 2)
            self.settled(executor)
            self.record(actual_posts=len(calls), usage=envelope["usage"],
                        logprobs=envelope["choices"][0]["logprobs"], complete_body_sha256=hashlib.sha256(raw).hexdigest())

    def test_graph_staggered_responses_keep_occurrence_association(self):
        with staggered_server(fixture_port=self.fixture_port) as (url, requests, completed):
            calls = tuple(replace(call(url, i), headers=call(url, i).headers + (("X-Order", str(i)),))
                          for i in range(8))
            executor = self.executor()
            outcomes = tuple(executor.iter_batch(calls))
            self.assertEqual({(o.call_id, o.row_id) for o in outcomes}, {(f"call:{i}", i) for i in range(8)})
            self.assertEqual(len(requests), 8)
            self.assertTrue(all(raw == call(url).payload for _, raw in requests))
            self.assertNotEqual(completed, list(range(8)))
            self.assertTrue(all(o.response.status_code == 200 and
                o.response.body == call(url).payload + b":" + str(o.row_id).encode() for o in outcomes))
            self.settled(executor)
            self.record(actual_posts=len(requests), fixture_completion_order=completed,
                        graph_delivery_order=[o.row_id for o in outcomes])

    def test_graph_cancel_and_batch_deadline_stop_the_consumer(self):
        for stop in ("cancel", "timeout"):
            with self.subTest(stop=stop), complete_server(delay=0.5,
                    fixture_port=self.fixture_port) as (url, calls, _raw):
                executor = self.executor()
                settings = (dict(cancelled=calls.started.is_set) if stop == "cancel"
                            else dict(timeout_s=0.05))
                with self.assertRaises(InterruptedError if stop == "cancel" else TimeoutError):
                    tuple(executor.iter_batch(tuple(call(url, i, timeout=2) for i in range(12)), **settings))
                count_at_stop = len(calls)
                until = time.monotonic() + 4
                while calls.active and time.monotonic() < until:
                    time.sleep(0.01)
                self.assertEqual(calls.active, 0)
                time.sleep(0.1)
                self.assertEqual(len(calls), count_at_stop)
                self.settled(executor, unconfirmed=True)
                self.refuses_reuse(executor, url, calls)
                self.record(stop=stop, actual_posts=len(calls), local_fixture_active_after_wait=calls.active,
                            remote_http_stopped=executor.last_close_report["remote_http_stopped"])

    def service(self, url):
        from src.execution_provider.adapters.sema_native import SemaNativeService
        from src.execution_provider.adapters.sema_service import SemaServiceLimits
        root = self.artifact_root / (self._testMethodName + "-" + str(next(_SERVICE_SEQUENCE)))
        root.mkdir(mode=0o700)
        return SemaNativeService(query_id="native-fixture", upstream_url=url,
            limits=SemaServiceLimits(16, 65536, 65536, 45), trace_path=root / "service.jsonl",
            backend=self.backend, options=self.options,
            held_request_capacity=max(8, self.native_limits.snapshot_rows), native_limits=self.native_limits,
            listen_port=self.service_port)

    @staticmethod
    def post(url, body):
        try:
            response = request.urlopen(request.Request(url, data=body,
                headers={"Content-Type": "application/json"}), timeout=50)
        except error.HTTPError as response:
            return response.code, response.read(), response.headers.get_all("X-Repeated")
        with response:
            return response.status, response.read(), response.headers.get_all("X-Repeated")

    def service_settled(self, service, *, normal=False):
        summary = service.summary
        self.assertFalse(summary["service_thread_alive"])
        self.assertTrue(summary["io_loop_closed"])
        self.assertFalse(summary["native_bridge_thread_alive"])
        self.assertEqual(summary["held_requests"], 0)
        self.assertEqual(summary["http_handlers_remaining"], 0)
        self.assertEqual(summary["cleanup_errors"], [])
        if normal:
            self.assertTrue(summary["native_close_reports"])
            self.assertTrue(all(report.get("clean") is True and report.get("graph_consumer_stopped") is True
                                for report in summary["native_close_reports"]))
            self.assertEqual(summary["uncertain_requests"], 0)
        self.record(service_summary=summary)

    def service_running(self, service):
        endpoint = urlsplit(service.endpoint_url)
        self.assertEqual(endpoint.hostname, "127.0.0.1")
        if self.service_port:
            self.assertEqual(endpoint.port, self.service_port)
        self.record(service_listener_host=endpoint.hostname, service_listener_port=endpoint.port,
                    configured_service_port=self.service_port)

    def test_service_sequential_supply_does_not_wait_for_query_end(self):
        with complete_server(fixture_port=self.fixture_port) as (url, calls, raw):
            service = self.service(url)
            with service:
                self.service_running(service)
                for _ in range(2):
                    self.assertEqual(self.post(service.endpoint_url, call(url).payload), (200, raw, ["one", "two"]))
                self.assertEqual(len(calls), 2)
                self.assertGreaterEqual(service.summary["snapshot_count"], 2)
                self.assertEqual(service.summary["native_task_ready"]["status"], "unavailable")
                service.end_input()
                self.assertEqual(self.post(service.endpoint_url, call(url).payload)[0], 410)
            self.assertEqual([body for _, body, _ in calls], [call(url).payload] * 2)
            self.service_settled(service, normal=True)

    def test_service_concurrent_duplicates_and_error_latch(self):
        with complete_server(delay=0.05, fixture_port=self.fixture_port) as (url, calls, raw):
            service = self.service(url)
            with service, ThreadPoolExecutor(max_workers=6) as pool:
                self.service_running(service)
                replies = list(pool.map(lambda _: self.post(service.endpoint_url, call(url).payload), range(6)))
                self.assertEqual(replies, [(200, raw, ["one", "two"])] * 6)
                self.assertEqual(len(calls), 6)
                self.assertLessEqual(calls.peak, self.options.concurrency)
                self.assertLessEqual(service.summary["peak_held_requests"], 8)
                service.end_input()
            self.service_settled(service, normal=True)
        with complete_server(429, fixture_port=self.fixture_port) as (url, calls, raw):
            service = self.service(url)
            with service:
                self.service_running(service)
                self.assertEqual(self.post(service.endpoint_url, call(url).payload), (429, raw, ["one", "two"]))
                first = service.first_error
                self.assertEqual(self.post(service.endpoint_url, call(url).payload)[0], 410)
                self.assertIs(service.first_error, first)
                self.assertEqual(first.body, raw)
            self.assertEqual(len(calls), 1)
            self.service_settled(service)

    def test_service_returns_usage_logprobs_and_complete_chat_envelope(self):
        with structured_response_server(fixture_port=self.fixture_port) as (url, calls, raw, envelope):
            service = self.service(url)
            with service:
                self.service_running(service)
                for _ in range(2):
                    status, body, headers = self.post(service.endpoint_url, call(url).payload)
                    self.assertEqual((status, body, headers), (200, raw, ["one", "two"]))
                    self.assertEqual(json.loads(body), envelope)
                service.end_input()
            self.assertEqual(calls, [call(url).payload] * 2)
            self.service_settled(service, normal=True)

    def test_service_cancel_stops_further_native_sends(self):
        with complete_server(delay=0.5, fixture_port=self.fixture_port) as (url, calls, _raw):
            service = self.service(url)
            with service, ThreadPoolExecutor(max_workers=1) as pool:
                self.service_running(service)
                pending = pool.submit(self.post, service.endpoint_url, call(url).payload)
                self.assertTrue(calls.started.wait(30))
                service.cancel()
                self.assertIn(pending.result(timeout=15)[0], (410, 502))
                self.assertEqual(self.post(service.endpoint_url, call(url).payload)[0], 410)
            self.assertEqual(len(calls), 1)
            self.service_settled(service)


class _RedactedStream:
    def __init__(self, stream):
        self.stream = stream

    def write(self, value):
        return self.stream.write(redact_text(value))

    def flush(self):
        self.stream.flush()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("daft", "ray-data"), required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--ray-temp", type=Path)
    parser.add_argument("--ray-address", help="Local IPv4 host:port for an explicitly caller-started private cluster")
    parser.add_argument("--fixture-port", type=int, default=0)
    parser.add_argument("--service-port", type=int, default=0)
    parser.add_argument("--scope", choices=("all", "graph", "service"), default="all")
    parser.add_argument("--snapshot-rows", type=int, default=8)
    parser.add_argument("--snapshot-payload-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--pending-response-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--cumulative-response-bytes", type=int, default=32 * 1024 * 1024)
    args = parser.parse_args(argv)
    for port in (args.fixture_port, args.service_port):
        if not 0 <= port <= 65535:
            parser.error("Fixture and Sema service ports must be from 0 to 65535")
    if (args.scope != "graph" and args.fixture_port and args.fixture_port == args.service_port):
        parser.error("The Sema listener and upstream fixture need separate ports")
    if args.ray_address is not None:
        try:
            validate_ray_address(args.ray_address)
        except ValueError as failure:
            parser.error(str(failure))
        if args.backend != "ray-data":
            parser.error("--ray-address requires Ray Data")
        if args.ray_temp is not None:
            parser.error("Choose --ray-address or a new local --ray-temp")
    if args.backend == "ray-data" and args.ray_address is None and (
            args.ray_temp is None or len(str(args.ray_temp.resolve()).encode()) > 40):
        parser.error("Ray Data needs a caller-owned --ray-temp path of at most 40 bytes")
    try:
        native_limits = SemaNativeLimits(args.snapshot_rows, args.snapshot_payload_bytes,
                                        args.pending_response_bytes, args.cumulative_response_bytes)
    except ValueError as failure:
        parser.error(str(failure))
    new_private_directory(args.artifact_root)
    os.environ["RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY"] = "2"
    os.environ["SEMLOOM_NATIVE_CALL_ARTIFACT"] = str(args.artifact_root)
    for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[name] = "1"
    if hasattr(os, "sched_getaffinity"):
        os.sched_setaffinity(0, set(sorted(os.sched_getaffinity(0))[:8]))
    runtime_started = time.monotonic_ns()
    ray = None
    owns_ray = False
    summary = dict(backend=args.backend, scope=args.scope, real_model_requests=0, gpu_tasks=0,
                   tests_run=0, failures=0, errors=0, skipped=0, observations=[],
                   fixture_host="127.0.0.1", fixture_port=args.fixture_port, service_port=args.service_port,
                   native_limits=dict(vars(native_limits)), cumulative_response_scope="per_snapshot",
                   cpu_affinity=sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
                   verifier_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   native_calls_source_sha256=hashlib.sha256((CODE / "src/baselines/text/frameworks/native_calls.py").read_bytes()).hexdigest())
    service_source = CODE / "src/execution_provider/adapters/sema_native.py"
    if args.scope != "graph" and service_source.is_file():
        summary["sema_native_source_sha256"] = hashlib.sha256(service_source.read_bytes()).hexdigest()
    try:
        summary["library_identity"] = library_identity(args.backend)
        if args.backend == "daft":
            configure_daft_runner("native", num_threads=8)
        else:
            import ray
            if ray.is_initialized():
                raise RuntimeError("native verifier requires an independently owned Ray runtime")
            if args.ray_address is not None:
                summary["ray_mode"] = "caller_started_cluster_connection"
                summary["ray_cluster_owner"] = "caller runtime; verifier owns only this process connection"
                summary["ray_address"] = args.ray_address
                owns_ray = True
                ray.init(address=args.ray_address, namespace=f"sema-native-{os.getpid()}")
            else:
                if args.ray_temp.is_symlink():
                    raise RuntimeError("Ray temporary directory cannot be a symlink")
                if args.ray_temp.exists():
                    if (not args.ray_temp.is_dir() or any(args.ray_temp.iterdir())
                            or args.ray_temp.stat().st_uid != os.getuid()):
                        raise RuntimeError("Ray temporary directory must be empty and caller-owned before startup")
                else:
                    new_private_directory(args.ray_temp)
                summary["ray_temp_preflight"] = "empty caller-owned short directory checked before ray.init"
                summary["ray_mode"] = "owned_local_runtime"
                owns_ray = True
                ray.init(address="local", namespace=f"sema-native-{os.getpid()}", num_cpus=8, num_gpus=0,
                         include_dashboard=False, object_store_memory=128 * 1024 * 1024,
                         _temp_dir=str(args.ray_temp.resolve()))
        summary["runtime_prepare_ns"] = time.monotonic_ns() - runtime_started
        NativeSemaChecks.backend, NativeSemaChecks.artifact_root = args.backend, args.artifact_root
        NativeSemaChecks.observations = summary["observations"]
        NativeSemaChecks.native_limits = native_limits
        NativeSemaChecks.fixture_port, NativeSemaChecks.service_port = args.fixture_port, args.service_port
        names = [name for name in unittest.defaultTestLoader.getTestCaseNames(NativeSemaChecks)
                 if args.scope == "all" or name.startswith("test_" + args.scope + "_")]
        with open_private_text(args.artifact_root / "checks.log") as log:
            result = unittest.TextTestRunner(stream=_RedactedStream(log), verbosity=2).run(
                unittest.TestSuite(NativeSemaChecks(name) for name in names))
        summary.update(tests_run=result.testsRun, failures=len(result.failures), errors=len(result.errors),
                       skipped=len(result.skipped), passed=bool(names) and result.wasSuccessful() and not result.skipped)
    except Exception as failure:
        summary.update(passed=False, startup_error=redact_text(f"{type(failure).__name__}: {failure}"))
    finally:
        if owns_ray:
            try:
                ray.shutdown()
                summary["owned_ray_shutdown"] = True
                summary["ray_shutdown_scope"] = ("verifier connection only; cluster remains caller-owned"
                    if args.ray_address is not None else "local runtime started by this verifier")
            except Exception as failure:
                summary.update(passed=False, owned_ray_shutdown=False,
                               cleanup_error=redact_text(f"{type(failure).__name__}: {failure}"))
        write_private_json(args.artifact_root / "summary.json", redact_json_values(summary))
    print(json.dumps({k: v for k, v in summary.items() if k not in ("library_identity", "observations")}))
    return 0 if summary.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
