"""Real Core and two independent loopback HTTP fixtures; no database or GPU is started."""

from collections import Counter
from contextlib import ExitStack
from dataclasses import asdict, replace
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from src.execution_provider.adapters.full_response import FullResponseTransport, decode_full_response
from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.model_replicas import (
    ModelReplica, ModelReplicaSet, ReplicaTransport, build_model_replica_execution, replica_usage,
)
from src.execution_provider.adapters.native_tasks import NativeQueryJob, NativeTaskSession, prepare_native_task
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.multiplexed_gateway import MultiSessionMapGateway
from src.execution_provider.completion import CompletionAdapterError
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.wire import v6
from src.execution_provider.wire.framing import MAX_FRAME_BYTES, encode_frame, read_frame
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.session_contract import State, TaskKey, Usage


DEPLOYMENT = "a" * 64


class Endpoint:
    def __init__(self, identity, *, hold=False, status=200, disconnect=False, body=None):
        self.identity, self.status, self.disconnect, self.body = identity, status, disconnect, body
        self.released = threading.Event()
        if not hold:
            self.released.set()
        self.changed = threading.Condition()
        self.requests, self.active, self.peak = [], 0, 0
        site = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                payload = self.rfile.read(int(self.headers["Content-Length"]))
                with site.changed:
                    site.requests.append((self.path, payload, self.headers.get("Authorization")))
                    site.active += 1
                    site.peak = max(site.peak, site.active)
                    site.changed.notify_all()
                try:
                    if not site.released.wait(10):
                        return
                    if site.disconnect:
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                    value = json.loads(payload)
                    tag = value.get("tag", value["messages"][-1]["content"])
                    body = site.body if site.body is not None else json.dumps(dict(
                        model=value["model"], endpoint=site.identity, tag=tag,
                        choices=[dict(message=dict(content=tag), finish_reason="stop",
                                      logprobs={"content": [{"token": "x", "logprob": -0.2}]})],
                        usage=dict(prompt_tokens=7, completion_tokens=1, total_tokens=8),
                        vendor_extension=[1, 2, {"complete": True}]), separators=(",", ":")).encode()
                    self.send_response(site.status)
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("X-Replica", site.identity)
                    self.send_header("X-Duplicate", "first")
                    self.send_header("X-Duplicate", "second")
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    with site.changed:
                        site.active -= 1
                        site.changed.notify_all()

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(target=partial(self.httpd.serve_forever, poll_interval=0.01), daemon=True)
        self.url = f"http://127.0.0.1:{self.httpd.server_port}/v1/chat/completions"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.released.set()
        self.httpd.shutdown()
        self.thread.join(2)
        self.httpd.server_close()


def task(sequence, *, tag=None, work=1, row=None):
    payload = json.dumps(dict(model="fixture", messages=[dict(role="user", content="same prompt")],
        temperature=0, max_tokens=7, stop=["end"], seed=42, vendor_parameter={"keep": [1, 2]},
        tag=tag or f"row-{sequence}"), ensure_ascii=False, indent=1).encode()
    descriptor = WorkDescriptor((StageWork("model", work, "work_units"),), "model", "declared-fixture-work")
    return prepare_native_task(payload, sequence, row_sequence=100 + sequence if row is None else row,
                               call_id=f"original-call-{sequence}", work=descriptor, max_result_bytes=4096)


def directory(sites, *, capacities=None, requests=None, work=None, routing="round_robin"):
    capacities = capacities or [(2, 2)] * len(sites)
    return ModelReplicaSet("fixture", tuple(ModelReplica(site.identity, site.url, "fixture-machine", (),
        DEPLOYMENT, *capacity) for site, capacity in zip(sites, capacities)),
        sum(c[0] for c in capacities) if requests is None else requests,
        sum(c[1] for c in capacities) if work is None else work, routing)


class ModelReplicaTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.executions, self.flows = [], []

    def tearDown(self):
        for site in self.sites:
            site.released.set()
        for flow in self.flows:
            flow.close()
        for execution in self.executions:
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline and any(
                    not slot.future.done() for slot in execution.engine.backend._slots.values()):
                execution.engine.advance()
                execution.engine.wake.wait(execution.engine.wake.generation, 0.005)
            execution.engine.advance()
            # Local I/O cleanup can finish even while Core deliberately retains an
            # unknown remote outcome. That is not asserted as service cleanup.
            self.assertTrue(execution.engine.backend.close())
        self.stack.close()

    @property
    def sites(self):
        return getattr(self, "_sites", [])

    def site(self, identity, **options):
        site = self.stack.enter_context(Endpoint(identity, **options))
        if not hasattr(self, "_sites"):
            self._sites = []
        self._sites.append(site)
        return site

    def execution(self, replica_set, *, response_mode="full", timeout=2000, **options):
        fixed = FixedModelConfig(replica_set.replicas[0].endpoint_url, "fixture", timeout,
                                 bearer_token="fixture-auth")
        execution = build_model_replica_execution(fixed, replicas=replica_set,
            response_mode=response_mode, **options)
        self.executions.append(execution)
        return execution

    def flow(self, execution, query="query-original", operator="operator-original"):
        flow = NativeTaskSession(execution, query, operator)
        self.flows.append(flow)
        return flow

    def until(self, execution, condition, timeout=3):
        deadline = time.monotonic() + timeout
        while not condition() and time.monotonic() < deadline:
            execution.engine.advance()
            execution.engine.wake.wait(execution.engine.wake.generation, 0.003)
        self.assertTrue(condition(), "controlled HTTP condition did not complete")

    def collect(self, flow, count, *, mode="full"):
        result, deadline = {}, time.monotonic() + 4
        while len(result) < count and time.monotonic() < deadline:
            progress = flow.advance(min(32, flow.limits.held_tasks))
            self.assertNotIn(progress.state, (State.FAILED, State.CANCELLED), progress.error)
            for delivery in progress.deliveries:
                self.assertNotIn(delivery.key.sequence, result)
                payload = decode_full_response(delivery.result) if mode == "full" else delivery.result
                result[delivery.key.sequence] = (delivery, payload)
            flow.release(tuple(d.lease_id for d in progress.deliveries))
            flow.wait(progress)
        self.assertEqual(len(result), count)
        return result

    def test_real_urls_receive_exact_occurrences_and_complete_responses_keep_identity(self):
        a, b = self.site("replica-a"), self.site("replica-b")
        trace = []
        execution = self.execution(directory((a, b), requests=1, work=1), max_tasks=8, observer=trace.append)
        flow = self.flow(execution)
        tasks = tuple(task(i, row=5 if i < 2 else 100 + i) for i in range(6))
        self.assertEqual(flow.offer(tasks).accepted_prefix_count, 6)
        self.assertEqual(a.requests + b.requests, [])
        flow.end_input()
        result = self.collect(flow, 6)
        for sequence, (delivery, response) in result.items():
            self.assertEqual(delivery.key, TaskKey(flow.session.session_id, sequence))
            self.assertEqual(delivery.info, tasks[sequence].info)
            self.assertEqual(response.status_code, 200)
            self.assertEqual([v for k, v in response.headers if k.lower() == "x-duplicate"], ["first", "second"])
            body = json.loads(response.body)
            self.assertEqual(body["tag"], f"row-{sequence}")
            self.assertEqual(body["usage"], dict(prompt_tokens=7, completion_tokens=1, total_tokens=8))
            self.assertEqual(body["vendor_extension"], [1, 2, {"complete": True}])
            self.assertEqual(body["choices"][0]["logprobs"]["content"][0]["logprob"], -0.2)
        posts = a.requests + b.requests
        self.assertTrue(a.requests and b.requests)
        self.assertEqual(Counter(p[1] for p in posts), Counter(t.payload for t in tasks))
        self.assertTrue(all(path == "/v1/chat/completions" and auth == "Bearer fixture-auth"
                            for path, _, auth in posts))
        assignments = [e for e in trace if e["event"] == "replica_transport_started"]
        self.assertEqual(Counter(e["endpoint_id"] for e in assignments), {"replica-a": 3, "replica-b": 3})
        self.assertEqual({e["flow_id"] for e in assignments}, {"operator-original"})
        self.assertEqual({e["job_id"] for e in assignments}, {flow.job.job_id})
        self.assertEqual({e["replica_set_sha256"] for e in assignments},
                         {directory((a, b), requests=1, work=1).sha256})
        self.assertNotIn(a.url, str(trace))
        self.assertEqual(flow.job.label, "query-original")
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        flow.close(clean=True)
        self.assertTrue(execution.close())

    def test_per_replica_work_and_request_limits_hold_while_fast_replica_replenishes(self):
        a, b = self.site("a", hold=True), self.site("b", hold=True)
        execution = self.execution(directory((a, b), capacities=((2, 3), (3, 4)), requests=4, work=6), max_tasks=8)
        flow = self.flow(execution)
        flow.offer(tuple(task(i, work=2) for i in range(8)))
        flow.end_input()
        self.until(execution, lambda: len(a.requests) + len(b.requests) == 3)
        self.assertEqual(replica_usage(execution), dict(
            a=dict(active_requests=1, active_work=2, uncertain_requests=0),
            b=dict(active_requests=2, active_work=4, uncertain_requests=0)))
        self.assertEqual(execution.engine.capacity.usage().active_work, 6)
        b.released.set()
        self.until(execution, lambda: len(b.requests) == 7)
        self.assertEqual(len(a.requests), 1)
        self.assertEqual(replica_usage(execution)["a"]["active_requests"], 1)
        a.released.set()
        result = self.collect(flow, 8)
        self.assertEqual(set(result), set(range(8)))
        self.assertLessEqual(a.peak, 1)
        self.assertLessEqual(b.peak, 2)
        self.assertEqual(Counter(json.loads(r[1])["tag"] for r in a.requests + b.requests),
                         Counter(f"row-{i}" for i in range(8)))
        flow.close(clean=True)

    def test_global_request_limit_can_be_lower_than_sum_of_endpoint_capacities(self):
        a, b = self.site("a", hold=True), self.site("b", hold=True)
        execution = self.execution(directory((a, b), capacities=((2, 4), (2, 4)), requests=2, work=8), max_tasks=6)
        flow = self.flow(execution)
        flow.offer(tuple(task(i) for i in range(6)))
        self.until(execution, lambda: len(a.requests) + len(b.requests) == 2)
        for _ in range(10):
            execution.engine.advance()
        self.assertEqual(execution.engine.capacity.usage().active_requests, 2)
        self.assertEqual(len(a.requests) + len(b.requests), 2)
        a.released.set(); b.released.set()
        self.collect(flow, 6)
        flow.close()

    def test_static_sequence_assignment_waits_for_its_endpoint_without_rerouting(self):
        a, b = self.site("a", hold=True), self.site("b")
        execution = self.execution(directory((a, b), capacities=((1, 1), (1, 1)), routing="static"), max_tasks=4)
        flow = self.flow(execution)
        flow.offer(tuple(task(i) for i in range(4)))
        self.until(execution, lambda: len(a.requests) == len(b.requests) == 1)
        for _ in range(10):
            execution.engine.advance()
        self.assertEqual(len(a.requests), 1)
        self.assertEqual(len(b.requests), 1)
        a.released.set()
        self.collect(flow, 4)
        self.assertEqual([json.loads(p[1])["tag"] for p in a.requests], ["row-0", "row-2"])
        self.assertEqual([json.loads(p[1])["tag"] for p in b.requests], ["row-1", "row-3"])
        flow.close()

    def test_http_failure_keeps_full_body_while_cancelled_sibling_remains_charged(self):
        error = b'{"error":{"message":"original error"},"usage":{"total_tokens":4}}'
        a, b = self.site("a", hold=True), self.site("b", status=503, body=error)
        execution = self.execution(directory((a, b), capacities=((1, 1), (1, 1))))
        flow = self.flow(execution)
        flow.offer((task(0), task(1)))
        response = self.collect(flow, 1)[1][1]
        self.assertEqual((response.status_code, response.body), (503, error))
        flow.request_cancel()
        self.assertEqual(flow.advance(1).state, State.CANCELLED)
        self.assertEqual(flow.close().uncertain_requests, 1)
        self.assertEqual(replica_usage(execution)["a"]["active_requests"], 1)
        self.assertFalse(execution.close())
        a.released.set()
        self.until(execution, lambda: not execution.engine.capacity.records)
        self.assertEqual((len(a.requests), len(b.requests)), (1, 1))
        self.assertTrue(execution.close())

    def test_cancelled_query_keeps_its_replica_and_another_query_completes_independently(self):
        a, b = self.site("a", hold=True), self.site("b", hold=True)
        execution = self.execution(directory((a, b), capacities=((1, 1), (1, 1))), max_tasks=8, max_jobs=2)
        first, other = self.flow(execution, "first-query"), self.flow(execution, "other-query")
        first.offer((task(0, tag="first"),)); other.offer((task(0, tag="other"),))
        self.until(execution, lambda: len(a.requests) == len(b.requests) == 1)
        first.request_cancel()
        self.assertEqual(first.advance(1).state, State.CANCELLED)
        first.close()
        self.assertEqual(replica_usage(execution)["a"]["active_requests"], 1)
        self.assertEqual(replica_usage(execution)["b"]["active_requests"], 1)
        self.assertFalse(execution.engine.jobs.jobs[other.job].closing)
        self.assertFalse(execution.close())
        b.released.set()
        self.assertEqual(json.loads(self.collect(other, 1)[0][1].body)["tag"], "other")
        other.offer((task(1, tag="continuation"),))
        self.assertEqual(json.loads(self.collect(other, 1)[1][1].body)["tag"], "continuation")
        other.close()
        self.assertEqual(replica_usage(execution)["a"]["active_requests"], 1)
        a.released.set()
        self.until(execution, lambda: not execution.engine.capacity.records)
        self.assertTrue(execution.close())
        self.assertEqual((len(a.requests), len(b.requests)), (1, 2))

    def test_disconnect_retains_unknown_endpoint_without_retries_or_releasing_other_endpoint(self):
        a, b = self.site("a", disconnect=True), self.site("b", hold=True)
        execution = self.execution(directory((a, b), capacities=((1, 1), (1, 1))), max_tasks=8, max_jobs=2)
        first, other = self.flow(execution, "failed-query"), self.flow(execution, "other-query")
        first.offer((task(0, tag="unknown"),)); other.offer((task(0, tag="known"),))
        self.until(execution, lambda: first.session.state == State.FAILED and len(b.requests) == 1)
        self.assertEqual(first.close().uncertain_requests, 1)
        self.assertEqual(replica_usage(execution), dict(
            a=dict(active_requests=1, active_work=1, uncertain_requests=1),
            b=dict(active_requests=1, active_work=1, uncertain_requests=0)))
        self.assertFalse(execution.close())
        b.released.set()
        self.collect(other, 1)
        other.offer((task(1, tag="more-known"),))
        self.assertEqual(json.loads(self.collect(other, 1)[1][1].body)["endpoint"], "b")
        other.close()
        for _ in range(20):
            execution.engine.advance()
        self.assertEqual((len(a.requests), len(b.requests)), (1, 2))
        self.assertEqual(replica_usage(execution)["a"]["active_requests"], 1)
        self.assertFalse(execution.close())

    def test_read_timeout_keeps_remote_responsibility_even_after_fixture_finishes(self):
        a, b = self.site("a", hold=True), self.site("b")
        execution = self.execution(directory((a, b), capacities=((1, 1), (1, 1))),
                                   timeout=100, max_tasks=8, max_jobs=2)
        first, other = self.flow(execution, "timeout-query"), self.flow(execution, "fast-query")
        first.offer((task(0),)); other.offer((task(0),))
        self.collect(other, 1)
        other.close()
        self.until(execution, lambda: first.session.state == State.FAILED)
        self.assertEqual(first.session.error, "MODEL_TIMEOUT")
        self.assertEqual(first.close().uncertain_requests, 1)
        self.assertEqual(a.active, 1)
        a.released.set()
        self.until(execution, lambda: a.active == 0)
        self.assertEqual(replica_usage(execution)["a"]["uncertain_requests"], 1)
        self.assertFalse(execution.close())
        self.assertEqual((len(a.requests), len(b.requests)), (1, 1))

    def test_one_replica_preserves_the_existing_completion_body_and_error_mapping(self):
        for status, body in ((200, b'{"choices":[{"message":{"content":"unchanged"}}]}'),
                             (429, b'{"error":"no retry"}')):
            with self.subTest(status=status):
                site = self.site(f"model-{status}", status=status, body=body)
                config = FixedModelConfig(site.url, "fixture", 1000)
                old = build_fixed_model_execution(config, max_tasks=1)
                self.executions.append(old)
                new = self.execution(directory((site,), capacities=((1, 1),)), response_mode="completion")
                results = []
                for execution in (old, new):
                    flow = self.flow(execution)
                    flow.offer((task(0),))
                    results.append(self.collect(flow, 1, mode="completion")[0][1])
                    flow.close()
                self.assertEqual(results[0], results[1])
                self.assertEqual(results[1], body if status == 200 else b'{"bridge_error": "MODEL_REQUEST_REJECTED"}')
                self.assertEqual(len(site.requests), 2)

    def test_existing_gateway_factory_selects_both_real_endpoints(self):
        a, b = self.site("a"), self.site("b")
        replicas = directory((a, b), capacities=((1, 1), (1, 1)))
        gateway = MultiSessionMapGateway(FixedModelConfig(a.url, "fixture", 1000), max_jobs=1,
            max_connections=1, max_tasks=2, max_active_requests=2, frame_timeout_ms=1000,
            execution_factory=partial(build_model_replica_execution, replicas=replicas))
        self.executions.append(gateway.execution)
        flow = self.flow(gateway.execution)
        flow.offer((task(0), task(1)))
        self.collect(flow, 2, mode="completion")
        flow.close()
        self.assertEqual((len(a.requests), len(b.requests)), (1, 1))

    def test_ray_composition_uses_replica_urls_and_capacities_and_closes_in_reverse_order(self):
        # Actual Ray/Daft libraries are covered by their existing transport suite;
        # this fixture tests the new composition and performs real loopback HTTP.
        a, b = self.site("a"), self.site("b")
        replicas = directory((a, b), capacities=((1, 1), (2, 2)))
        physical = {r.endpoint_id: RayMapConfig("fixture-cluster:1", 1, 1, MAX_FRAME_BYTES + 24, 2 * MAX_FRAME_BYTES,
                                              response_mode="full") for r in replicas.replicas}
        created, closed = [], []

        class Child(FullResponseTransport):
            def __init__(self, config, capacity, observer, *, physical, before_request):
                created.append((config.endpoint_url, capacity, physical.response_mode))
                super().__init__(config, capacity, observer)

            async def close(self):
                closed.append(self.config.endpoint_url)
                await super().close()

        with mock.patch("src.execution_provider.adapters.ray_map_transport.RayMapTransport", Child), \
                mock.patch("src.execution_provider.adapters.model_replicas._prepare_payload_backend") as prepare:
            execution = self.execution(replicas, physical=physical)
            prepare.assert_called_once_with(physical)
            flow = self.flow(execution)
            flow.offer((task(0), task(1), task(2)))
            self.collect(flow, 3)
            flow.close()
            self.assertTrue(execution.close())
        self.assertEqual(created, [(a.url, 1, "full"), (b.url, 2, "full")])
        self.assertEqual(closed, [b.url, a.url])
        self.assertEqual(len(a.requests) + len(b.requests), 3)

    def test_gateway_cli_serves_v6_wire_with_two_actual_http_endpoints(self):
        a, b = self.site("a"), self.site("b")
        replicas = directory((a, b), capacities=((1, 1), (1, 1)))
        code = Path(__file__).resolve().parents[2]
        script = code / "scripts/services/run_model_replica_gateway.py"
        with tempfile.TemporaryDirectory(prefix="replica-wire-", dir="/tmp") as root:
            root = Path(root)
            config_path, replica_path, sock_path = root / "fixed.json", root / "replicas.json", root / "provider.sock"
            config_path.write_text(json.dumps(dict(endpoint_url=a.url, model_id="fixture", timeout_ms=2000)))
            replica_path.write_text(json.dumps(asdict(replicas)))
            environment = dict(os.environ)
            environment.pop("PYTHONPATH", None)
            process = subprocess.Popen([sys.executable, str(script), "--replica-config", str(replica_path),
                "--fixed-model-config", str(config_path), "--incremental-map", "--socket", str(sock_path),
                "--max-active-requests", "2", "--max-held-tasks", "4", "--once"], cwd=root,
                env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            client = socket.socket(socket.AF_UNIX)
            client.settimeout(3)
            try:
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline and process.poll() is None:
                    try:
                        client.connect(str(sock_path))
                        break
                    except (FileNotFoundError, ConnectionRefusedError):
                        time.sleep(0.005)
                else:
                    self.fail("replica gateway CLI did not create its socket")
                plan = SemanticMapPlan("Echo.", "fixture", 7)
                client.sendall(encode_frame(v6.build_open_message(plan)))
                self.assertEqual(read_frame(client)["type"], "opened")
                tasks = []
                for sequence in range(4):
                    item = v6.build_task_message(plan, sequence=sequence, input_value=f"wire-row-{sequence}")
                    tasks.append(item)
                    client.sendall(encode_frame(item))
                    self.assertEqual(read_frame(client)["accepted_prefix_count"], 1)
                result = {}
                for _ in range(4):
                    client.sendall(encode_frame(dict(type="poll", protocol_version=6)))
                    response = read_frame(client)
                    self.assertEqual(response["type"], "completion")
                    sequence = int(response["sequence"])
                    self.assertNotIn(sequence, result)
                    self.assertEqual(response["semantic_payload_digest"], tasks[sequence]["semantic_payload_digest"])
                    self.assertEqual(response["raw_output"], f"wire-row-{sequence}")
                    result[sequence] = response
                client.close()
                stdout, stderr = process.communicate(timeout=4)
                self.assertEqual((process.returncode, stdout, stderr), (0, b"", b""))
                self.assertFalse(sock_path.exists())
                self.assertTrue(a.requests and b.requests)
                self.assertEqual(len(a.requests) + len(b.requests), 4)
            finally:
                client.close()
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=3)

    def test_malformed_json_is_full_response_and_oversized_body_keeps_unknown_reservation(self):
        a, b = self.site("a", body=b'{"broken":'), self.site("b", body=b"x" * 5000)
        execution = self.execution(directory((a, b), capacities=((1, 1), (1, 1))), max_tasks=8, max_jobs=2)
        first, other = self.flow(execution, "malformed-query"), self.flow(execution, "oversized-query")
        first.offer((task(0),)); other.offer((task(0),))
        self.assertEqual(self.collect(first, 1)[0][1].body, b'{"broken":')
        first.close()
        self.until(execution, lambda: other.session.state == State.FAILED)
        self.assertEqual(other.close().uncertain_requests, 1)
        self.assertEqual(replica_usage(execution)["b"]["active_requests"], 1)
        self.assertEqual((len(a.requests), len(b.requests)), (1, 1))
        self.assertFalse(execution.close())

    def test_static_oversized_work_fails_without_dispatch_or_rerouting(self):
        a, b = self.site("a"), self.site("b")
        execution = self.execution(directory((a, b), capacities=((1, 1), (1, 2)), routing="static"))
        flow = self.flow(execution)
        flow.offer((task(0, work=2),))
        self.assertEqual(flow.advance(1).state, State.FAILED)
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertEqual(a.requests + b.requests, [])
        flow.close()

    def test_cleanup_failure_still_closes_the_other_replica_and_remains_visible(self):
        a, b = self.site("a"), self.site("b")
        closed = []

        class Child(FullResponseTransport):
            async def close(self):
                closed.append(self.config.endpoint_url)
                await super().close()
                if self.config.endpoint_url == b.url:
                    raise RuntimeError("controlled child cleanup failure")

        with mock.patch("src.execution_provider.adapters.model_replicas.FullResponseTransport", Child):
            execution = self.execution(directory((a, b)))
            with self.assertRaisesRegex(RuntimeError, "finalization failed"):
                execution.close()
            with self.assertRaisesRegex(RuntimeError, "cleanup previously failed"):
                execution.close()
        self.assertEqual(closed, [b.url, a.url])

    def test_observer_presend_failure_is_a_known_local_error_with_no_child_or_unknown_work(self):
        a = self.site("a")
        children, events = [], []

        class NoIOChild:
            def __init__(self, *args):
                self.calls, self.closed = 0, False
                children.append(self)

            async def execute(self, *args):
                self.calls += 1
                raise AssertionError("pre-send failure must not enter the child")

            async def close(self):
                self.closed = True

        def observe(event):
            events.append(event)
            if event["event"] == "replica_transport_started":
                raise RuntimeError("controlled pre-send observation failure")

        with mock.patch("src.execution_provider.adapters.model_replicas.FullResponseTransport", NoIOChild):
            execution = self.execution(directory((a,), capacities=((1, 1),)), observer=observe)
        flow = self.flow(execution)
        flow.offer((task(0),)); flow.end_input()
        result = self.collect(flow, 1, mode="completion")[0][1]
        with self.assertRaises(CompletionAdapterError):
            decode_full_response(result)
        self.assertEqual(sum(c.calls for c in children), 0)
        failure = execution.engine.backend._execute.__self__.observation_failure
        self.assertEqual((failure["event"], failure["reason"]["exception_type"]),
                         ("replica_transport_started", "builtins.RuntimeError"))
        self.assertEqual(a.requests, [])
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertEqual(replica_usage(execution)["a"]["uncertain_requests"], 0)
        flow.close(clean=True)
        with self.assertRaisesRegex(RuntimeError, "finalization failed"):
            execution.close()
        self.assertTrue(all(c.closed for c in children))
        self.assertEqual(execution.engine.jobs.jobs, {})

    def test_presend_observer_failure_does_not_refund_another_replica_query(self):
        a, b = self.site("a"), self.site("b", hold=True)

        def observe(event):
            if event["event"] == "replica_transport_started" and event["endpoint_id"] == "a":
                raise RuntimeError("controlled pre-send observation failure")

        execution = self.execution(directory((a, b), capacities=((1, 1), (1, 1))),
                                   observer=observe, max_tasks=8, max_jobs=2)
        first, other = self.flow(execution, "first"), self.flow(execution, "other")
        first.offer((task(0),)); other.offer((task(0),))
        self.until(execution, lambda: len(b.requests) == 1)
        payload = self.collect(first, 1, mode="completion")[0][1]
        with self.assertRaises(CompletionAdapterError):
            decode_full_response(payload)
        first.close()
        self.assertEqual(a.requests, [])
        self.assertEqual(replica_usage(execution)["a"]["active_requests"], 0)
        self.assertEqual(replica_usage(execution)["b"]["active_requests"], 1)
        self.assertFalse(execution.close())
        b.released.set()
        self.collect(other, 1)
        other.close()
        with self.assertRaisesRegex(RuntimeError, "finalization failed"):
            execution.close()

    def test_child_observer_errors_do_not_hide_completed_http_or_create_unknown_work(self):
        for event_name in ("http_started", "http_finished"):
            with self.subTest(event=event_name):
                site = self.site(event_name)
                def observe(event):
                    if event["event"] == event_name:
                        raise RuntimeError("controlled child observation failure")
                execution = self.execution(directory((site,), capacities=((1, 1),)), observer=observe)
                flow = self.flow(execution)
                flow.offer((task(0),)); flow.end_input()
                response = self.collect(flow, 1)[0][1]
                self.assertEqual(response.status_code, 200)
                self.assertEqual(json.loads(response.body)["tag"], "row-0")
                self.assertEqual(execution.engine.capacity.usage(), Usage())
                flow.close(clean=True)
                with self.assertRaisesRegex(RuntimeError, "finalization failed"):
                    execution.close()
                self.assertEqual(len(site.requests), 1)

    def test_observer_error_after_dispatch_cannot_refund_a_real_http_timeout(self):
        site = self.site("a", hold=True)
        def observe(event):
            if event["event"] == "http_finished":
                raise RuntimeError("controlled observation failure after HTTP dispatch")
        execution = self.execution(directory((site,), capacities=((1, 1),)), observer=observe, timeout=100)
        flow = self.flow(execution)
        flow.offer((task(0),))
        self.until(execution, lambda: flow.session.state == State.FAILED)
        self.assertEqual(len(site.requests), 1)
        self.assertEqual(flow.close().uncertain_requests, 1)
        self.assertEqual(replica_usage(execution)["a"]["uncertain_requests"], 1)
        self.assertFalse(execution.close())
        with self.assertRaisesRegex(RuntimeError, "finalization failed"):
            execution.engine.backend.close()


class ReplicaConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.a = ModelReplica("a", "http://localhost:8000/v1/chat/completions", "machine", (), DEPLOYMENT, 1, 1)
        self.b = replace(self.a, endpoint_id="b", endpoint_url="http://localhost:8001/v1/chat/completions")

    def test_strict_configuration_roundtrip_and_signature(self):
        value = ModelReplicaSet("fixture", (self.a, self.b), 2, 2)
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "replicas.json"
            path.write_text(json.dumps(asdict(value)))
            self.assertEqual(ModelReplicaSet.load(path), value)
            self.assertEqual(ModelReplicaSet.load(path).sha256, value.sha256)
            for change in ({"unknown": 1}, {"max_active_requests": True}, {"model_id": ""}):
                path.write_text(json.dumps(dict(asdict(value), **change)))
                with self.assertRaises(ValueError):
                    ModelReplicaSet.load(path)
        self.assertNotEqual(value.sha256, replace(value, replicas=(self.b, self.a)).sha256)
        self.assertNotIn(self.a.endpoint_url, repr(self.a))

    def test_equivalence_duplicate_urls_and_global_capacities_are_checked_before_io(self):
        for replicas, requests, work in (((self.a, replace(self.b, deployment_sha256="b" * 64)), 2, 2),
                                       ((self.a, replace(self.b, endpoint_url=self.a.endpoint_url)), 2, 2),
                                       ((self.a, self.a), 2, 2), ((self.a,), 2, 1), ((self.a,), 1, 2)):
            with self.subTest(requests=requests, work=work), self.assertRaises(ValueError):
                ModelReplicaSet("fixture", replicas, requests, work)
        for invalid in (dict(model_id="different"), dict(max_active_requests=3), dict(active_work=3),
                        dict(execute=lambda: None), dict(response_mode="invalid")):
            options = dict(invalid)
            config = FixedModelConfig(self.a.endpoint_url, options.pop("model_id", "fixture"), 1000)
            with self.subTest(options=invalid), self.assertRaises(ValueError):
                build_model_replica_execution(config, replicas=ModelReplicaSet("fixture", (self.a, self.b), 2, 2),
                                              **options)

    def test_ray_replica_directory_is_checked_before_actor_startup(self):
        replicas = ModelReplicaSet("fixture", (self.a, self.b), 2, 2)
        physical = RayMapConfig("fixture:1", 1, 1, 262200, 600000, worker_pool="same")
        with mock.patch("src.execution_provider.adapters.ray_map_transport.RayMapTransport") as factory:
            for mapping in ({"a": physical}, {"a": physical, "b": physical},
                            {"a": replace(physical, worker_pool=None),
                             "b": replace(physical, worker_pool=None, workers=2)}):
                with self.assertRaises(ValueError):
                    ReplicaTransport(FixedModelConfig(self.a.endpoint_url, "fixture", 1000), 2,
                                     directory=replicas, response_mode="full", physical=mapping)
            factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
