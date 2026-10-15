"""Real Core/I/O and original kernel fixtures; localhost POST is separately opt-in."""

import asyncio
from dataclasses import replace
import hashlib
import json
import os
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import native_calls
from src.execution_provider.adapters.full_response import FullModelResponse
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.scheduling.core.session_contract import State, Usage
from src.semantic_methods.palimpzest import semloom
from src.semantic_methods.palimpzest.backend import PalimpzestAdapterError
from src.semantic_methods.palimpzest.query_execution import QueryExecutionLimits
from tests.baselines.test_native_calls import complete_server


CONFIG = FixedModelConfig("http://127.0.0.1:19000/observe/query/model/v1/chat/completions", "fixture", 1000)


def call(index, operator="filter", *, text="fixture", **changes):
    identity = dict(query_id="query", plan_fingerprint="fixed-plan", physical_operator=operator,
        input_occurrence=index, source_indices=["review-"+str(index)], record_id="record-"+str(index),
        parent_ids=[], cardinality_idx=0, generation_step=0, attempt=0)
    call_id = "pz:" + hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    payload = json.dumps(dict(model="fixture", messages=[dict(role="user", content=text)])).encode()
    value = native_calls.NativeCall(call_id, index, payload, CONFIG.endpoint_url,
        (("Authorization", "Bearer EMPTY"), ("X-Repeated", "one"), ("X-Repeated", "two")),
        1, max_response_bytes=4096, follow_redirects=False)
    return replace(value, **changes), identity


def limits(**changes):
    values = dict(pz_workers=4, max_pending_calls=4, max_batch_rows=4,
        max_input_bytes=4*1024**2, max_batch_bytes=1024**2, max_request_bytes=65536,
        max_response_bytes=4096, max_retained_response_bytes=4*1024**2,
        batch_timeout_s=1, close_timeout_s=1)
    values.update(changes)
    return QueryExecutionLimits(**values)


class SemLoomProgressTests(unittest.TestCase):
    def make(self, **kwargs):
        progress = semloom.SemLoomProgress(kwargs.pop("model_config", CONFIG), query_id="query", operator_ids=("filter", "map"),
            limits=kwargs.pop("limits", limits()), max_active_requests=kwargs.pop("max_active_requests", 8), **kwargs)
        self.addCleanup(self.cleanup, progress)
        return progress

    def cleanup(self, progress):
        progress.finish(method_complete=False, timeout_s=0.1)
        # A fixture transport error retains Core ownership even after its local I/O
        # future ended. Only stop that completed local loop, without declaring cleanup.
        progress.execution.engine.backend.close(0.1)

    def batch(self, progress, values):
        remaining = list(values)
        events = []
        deadline = time.monotonic()+2
        while (remaining or progress.active) and time.monotonic() < deadline:
            if remaining:
                accepted = progress.start(tuple(value for value, _ in remaining),
                    identities={value.call_id: identity for value, identity in remaining}, cancelled=lambda: False, timeout_s=1)
                remaining = remaining[accepted:]
            event = progress.poll(0.05)
            if event:
                events.append(event)
                if event[0] == "error":
                    raise event[1]
        self.assertFalse(progress.active)
        self.assertFalse(remaining)
        return [value for kind, value in events if kind == "outcome"]

    def test_complete_calls_preserve_headers_redirect_and_error_response_across_snapshots(self):
        received = []
        async def kernel(value, control=None, *, client=None):
            received.append((value, threading.get_ident(), client))
            return native_calls.NativeCallOutcome(value.call_id, value.row_id,
                FullModelResponse(429, (("X-Repeated", "one"), ("X-Repeated", "two")), b"\x00not-json\xff"))
        with patch.object(native_calls, "_request", kernel):
            progress = self.make()
            first = self.batch(progress, [call(0), call(1)])
            self.assertEqual([value.response.status_code for value in first], [429, 429])
            self.assertTrue(all(value.response.body == b"\x00not-json\xff" for value in first))
            self.assertEqual(progress.execution.engine.capacity.usage(), Usage())
            self.assertTrue(all(flow.session.state == State.OPEN for flow in progress.flows.values()))
            job = progress.job.job
            second = self.batch(progress, [call(2, "map")])
            self.assertEqual(second[0].row_id, 2)
            self.assertIs(progress.job.job, job)
            self.assertFalse(any(value.follow_redirects for value, _, _ in received))
            self.assertTrue(all(value.headers[-2:] == (("X-Repeated", "one"), ("X-Repeated", "two"))
                                for value, _, _ in received))
            self.assertTrue(all(thread != threading.get_ident() for _, thread, _ in received))
            self.assertEqual(len({id(client) for _, _, client in received}), 1)
            report = progress.finish(method_complete=True, timeout_s=1)
            self.assertTrue(report["clean"])
            self.assertTrue(report["http_pool_closed"])
            self.assertFalse(report["unconfirmed_work"])
            self.assertEqual(report["declared_request_capacity"], 8)
            self.assertFalse(report["ray_backend"])

    def test_ready_downstream_dispatches_before_upstream_last_call_completes(self):
        last_started, release_last = threading.Event(), threading.Event()
        times = {}
        async def kernel(value, control=None, *, client=None):
            if value.row_id == 1:
                last_started.set()
                while not release_last.is_set():
                    await asyncio.sleep(0.001)
                times["upstream_last_done"] = time.monotonic_ns()
            elif value.row_id == 2:
                times["downstream_dispatched"] = time.monotonic_ns()
            return native_calls.NativeCallOutcome(value.call_id, value.row_id, FullModelResponse(200, (), b"ok"))
        with patch.object(native_calls, "_request", kernel):
            progress = self.make()
            self.addCleanup(release_last.set)
            upstream = [call(0), call(1)]
            self.assertEqual(progress.start(tuple(value for value, _ in upstream),
                identities={value.call_id: identity for value, identity in upstream}, cancelled=lambda: False, timeout_s=1), 2)
            first = None
            while first is None:
                first = progress.poll(0.05)
            self.assertEqual(first[1].row_id, 0)
            self.assertTrue(last_started.wait(1))
            self.assertTrue(progress.active)
            downstream, identity = call(2, "map")
            times["downstream_prepared"] = time.monotonic_ns()
            self.assertEqual(progress.start((downstream,), identities={downstream.call_id: identity},
                cancelled=lambda: False, timeout_s=1), 1)
            times["downstream_offered"] = time.monotonic_ns()
            second = None
            while second is None:
                second = progress.poll(0.05)
            self.assertEqual(second[1].row_id, 2)
            self.assertNotIn("upstream_last_done", times)
            self.assertTrue(progress.active)
            self.assertEqual(progress.flows["filter"].session.state, State.OPEN)
            self.assertEqual(progress.flows["map"].session.state, State.OPEN)
            release_last.set()
            third = None
            while third is None:
                third = progress.poll(0.05)
            self.assertEqual(third[1].row_id, 1)
            self.assertLess(times["downstream_prepared"], times["downstream_offered"])
            self.assertLess(times["downstream_offered"], times["downstream_dispatched"])
            self.assertLess(times["downstream_dispatched"], times["upstream_last_done"])
            self.assertTrue(progress.finish(method_complete=True, timeout_s=1)["clean"])

    def test_whole_snapshot_identity_and_route_preflight_prevents_any_dispatch(self):
        async def kernel(*args, **kwargs):
            self.fail("invalid snapshot reached the kernel")
        with patch.object(native_calls, "_request", kernel):
            progress = self.make()
            valid = call(0)
            wrong = call(1, endpoint="http://127.0.0.1:19001/v1/chat/completions")
            with self.assertRaisesRegex(ValueError, "registered route"):
                self.batch(progress, [valid, wrong])
            self.assertEqual(progress.execution.engine.capacity.usage(), Usage())
            self.assertEqual(progress.snapshot()["calls_submitted"], 0)
            self.assertIsNone(progress.transport._client)

    def test_core_accepts_only_prefix_and_caller_keeps_unaccepted_suffix(self):
        release = threading.Event()
        async def kernel(value, control=None, *, client=None):
            while not release.is_set():
                await asyncio.sleep(0.001)
            return native_calls.NativeCallOutcome(value.call_id, value.row_id, FullModelResponse(200, (), b"ok"))
        with patch.object(native_calls, "_request", kernel):
            progress = self.make()
            self.addCleanup(release.set)
            values = [call(index) for index in range(3)]
            accepted = progress.start(tuple(value for value, _ in values),
                identities={value.call_id: identity for value, identity in values}, cancelled=lambda: False, timeout_s=1)
            self.assertEqual(accepted, 2)
            self.assertEqual(progress.snapshot()["pending_calls"], 2)
            self.assertNotIn(values[2][0].call_id, {value.call_id for value in progress._expected.values()})
            with self.assertRaisesRegex(ValueError, "already owned"):
                progress.start((values[0][0],), identities={values[0][0].call_id: values[0][1]},
                    cancelled=lambda: False, timeout_s=1)
            release.set()
            received = 0
            while progress.active:
                event = progress.poll(0.05)
                if event:
                    self.assertEqual(event[0], "outcome")
                    received += 1
            self.assertEqual(received, 2)
            self.assertEqual(self.batch(progress, values[accepted:])[0].row_id, 2)
            self.assertTrue(progress.finish(method_complete=True, timeout_s=1)["clean"])

    def test_raw_64k_limit_and_full_wire_1mib_capacity_are_distinct(self):
        progress = self.make()
        too_large = call(0, text="x"*65536)
        with self.assertRaisesRegex(ValueError, "wire or response"):
            self.batch(progress, [too_large])
        expanded = self.make(limits=limits(max_request_bytes=1024**2))
        large = call(0, text="x"*800000)
        self.assertLess(len(large[0].payload), 1024**2)
        self.assertGreater(len(native_calls._call_wire(large[0]).encode()), 1024**2)
        with self.assertRaisesRegex(ValueError, "wire or response"):
            self.batch(expanded, [large])
        self.assertEqual(expanded.snapshot()["calls_submitted"], 0)

    def test_exact_64k_raw_body_is_accepted_with_larger_bounded_wire(self):
        value, identity = call(0, text="x"*(65536-len(call(0, text="")[0].payload)))
        self.assertEqual(len(value.payload), 65536)
        self.assertGreater(len(native_calls._call_wire(value).encode()), 65536)
        async def kernel(item, control=None, *, client=None):
            return native_calls.NativeCallOutcome(item.call_id, item.row_id, FullModelResponse(200, (), b"ok"))
        with patch.object(native_calls, "_request", kernel):
            progress = self.make()
            self.assertEqual(self.batch(progress, [(value, identity)])[0].response.body, b"ok")
            self.assertTrue(progress.finish(method_complete=True, timeout_s=1)["clean"])

    def test_largest_complete_wire_below_1mib_is_accepted_and_next_encoding_rejected(self):
        low, high = 0, 1024**2
        while low + 1 < high:
            middle = (low+high)//2
            if len(native_calls._call_wire(call(0, text="x"*middle)[0]).encode()) <= 1024**2:
                low = middle
            else:
                high = middle
        boundary = call(0, text="x"*low)
        next_wire = call(1, text="x"*high)
        self.assertLessEqual(len(native_calls._call_wire(boundary[0]).encode()), 1024**2)
        self.assertGreater(len(native_calls._call_wire(next_wire[0]).encode()), 1024**2)
        async def kernel(item, control=None, *, client=None):
            return native_calls.NativeCallOutcome(item.call_id, item.row_id, FullModelResponse(200, (), b"ok"))
        with patch.object(native_calls, "_request", kernel):
            progress = self.make(limits=limits(max_request_bytes=1024**2))
            self.assertEqual(len(self.batch(progress, [boundary])), 1)
            with self.assertRaisesRegex(ValueError, "wire or response"):
                self.batch(progress, [next_wire])
            self.assertTrue(progress.finish(method_complete=True, timeout_s=1)["clean"])

    def test_original_native_kernel_keeps_complete_call_with_a_session_fixture(self):
        received, clients = [], []
        class Content:
            async def iter_chunked(self, size):
                yield b"raw-gzip-fixture"
        class Response:
            status = 429
            raw_headers = ((b"X-Repeated", b"one"), (b"X-Repeated", b"two"))
            version = SimpleNamespace(major=1, minor=1)
            content = Content()
            async def __aenter__(self):
                return self
            async def __aexit__(self, *error):
                pass
        class Client:
            def __init__(self):
                self.loop = asyncio.get_running_loop()
                self.session, self.closed = self, False
                clients.append(self)
            def post(self, endpoint, **kwargs):
                received.append((endpoint, kwargs))
                return Response()
            async def close(self):
                self.closed = True
        with patch.object(native_calls, "_new_http_client", Client):
            progress = self.make()
            result = self.batch(progress, [call(0)])[0]
            self.assertEqual(result.response, FullModelResponse(429,
                (("X-Repeated", "one"), ("X-Repeated", "two")), b"raw-gzip-fixture"))
            self.assertEqual(received[0][1]["headers"][-2:], (("X-Repeated", "one"), ("X-Repeated", "two")))
            self.assertFalse(received[0][1]["allow_redirects"])
            self.assertEqual(received[0][0], CONFIG.endpoint_url)
            self.assertTrue(progress.finish(method_complete=True, timeout_s=1)["clean"])
            self.assertTrue(clients[0].closed)

    @unittest.skipUnless(os.environ.get("SEMLOOM_PZ_NATIVE_HTTP_TEST") == "1",
                         "real localhost native-call qualification is explicitly opt-in")
    def test_original_native_kernel_returns_real_loopback_gzip_error_response(self):
        with complete_server(429, compressed=True) as (endpoint, requests, raw):
            endpoint = endpoint.partition("?")[0]
            progress = self.make(model_config=replace(CONFIG, endpoint_url=endpoint))
            headers = (*call(0)[0].headers, ('X-Input', 'one'), ('X-Input', 'two'))
            outcomes = self.batch(progress, [call(0, endpoint=endpoint, headers=headers)])
            self.assertEqual(len(requests), 1)
            self.assertEqual(requests[0][2], ["one", "two"])
            self.assertEqual(outcomes[0].response.status_code, 429)
            self.assertEqual(outcomes[0].response.body, raw)
            self.assertEqual([value for name, value in outcomes[0].response.headers if name == "X-Repeated"],
                             ["one", "two"])
            self.assertTrue(progress.finish(method_complete=True, timeout_s=1)["http_pool_closed"])

    def test_transport_failure_retains_unknown_work_and_prevents_another_snapshot(self):
        attempts = []
        async def kernel(value, control=None, *, client=None):
            attempts.append(value.call_id)
            return native_calls.NativeCallOutcome(value.call_id, value.row_id, None, transport_error="TimeoutError")
        with patch.object(native_calls, "_request", kernel):
            progress = self.make()
            with self.assertRaises(PalimpzestAdapterError):
                self.batch(progress, [call(0)])
            report = progress.finish(method_complete=False, timeout_s=0.02)
            self.assertTrue(report["unconfirmed_work"])
            self.assertFalse(report["runtime_closed"])
            self.assertEqual(report["core_usage"]["active_requests"], 1)
            self.assertEqual(len(attempts), 1)
            with self.assertRaises(PalimpzestAdapterError):
                self.batch(progress, [call(1)])

    def test_cancel_retains_submitted_call_until_original_kernel_finishes(self):
        started, release, cancel = threading.Event(), threading.Event(), threading.Event()
        async def kernel(value, control=None, *, client=None):
            started.set()
            while not release.is_set():
                await asyncio.sleep(0.001)
            return native_calls.NativeCallOutcome(value.call_id, value.row_id, FullModelResponse(200, (), b"late"))
        with patch.object(native_calls, "_request", kernel):
            progress = self.make()
            self.addCleanup(release.set)
            value, identity = call(0)
            progress.start((value,), identities={value.call_id: identity}, cancelled=cancel.is_set, timeout_s=1)
            progress.poll(0.01)
            self.assertTrue(started.wait(1))
            cancel.set()
            self.assertEqual(progress.poll(0)[0], "error")
            first = progress.finish(method_complete=False, timeout_s=0.01)
            self.assertTrue(first["unconfirmed_work"])
            self.assertEqual(first["core_usage"]["active_requests"], 1)
            release.set()
            second = progress.finish(method_complete=False, timeout_s=1)
            self.assertFalse(second["unconfirmed_work"])
            self.assertTrue(second["runtime_closed"])

    def test_progress_rejects_other_control_threads(self):
        progress = self.make()
        errors = []
        def other_thread():
            try:
                progress.poll(0)
            except Exception as error:
                errors.append(error)
        thread = threading.Thread(target=other_thread)
        thread.start()
        thread.join(1)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], PalimpzestAdapterError)

    def test_zero_remaining_finish_deadline_keeps_submitted_ownership(self):
        started, release = threading.Event(), threading.Event()
        async def kernel(value, control=None, *, client=None):
            started.set()
            while not release.is_set():
                await asyncio.sleep(0.001)
            return native_calls.NativeCallOutcome(value.call_id, value.row_id, FullModelResponse(200, (), b"late"))
        with patch.object(native_calls, "_request", kernel):
            progress = self.make()
            self.addCleanup(release.set)
            value, identity = call(0)
            progress.start((value,), identities={value.call_id: identity}, cancelled=lambda: False, timeout_s=1)
            progress.poll(0.01)
            self.assertTrue(started.wait(1))
            report = progress.finish(method_complete=False, timeout_s=0)
            self.assertTrue(report["unconfirmed_work"])
            self.assertEqual(report["core_usage"]["active_requests"], 1)
            release.set()
            self.assertTrue(progress.finish(method_complete=False, timeout_s=1)["runtime_closed"])

    def test_source_guard_rejects_changed_private_kernel_dependency(self):
        with patch.dict(semloom._SOURCE_HASHES, {native_calls: "0"*64}):
            with self.assertRaisesRegex(PalimpzestAdapterError, "dependency changed"):
                self.make()

    def test_physical_configuration_requires_explicit_complete_call_daft_mode(self):
        physical = RayMapConfig('fixture-cluster', 1, 1, 1048600, 2097200, response_mode='native-call-full')
        for value in [SimpleNamespace(), replace(physical, response_mode='full'),
                      replace(physical, payload_backend='arrow')]:
            with self.subTest(physical=value), self.assertRaisesRegex(ValueError, 'explicit native-call-full'):
                self.make(physical=value)

    def test_ray_selection_passes_original_builder_and_does_not_claim_driver_pool_closed(self):
        physical = RayMapConfig('fixture-cluster', 1, 1, 1048600, 2097200, response_mode='native-call-full')
        original = semloom.native_tasks.build_native_execution
        seen = []
        async def fixture(request, endpoint):
            return semloom.full_response.encode_full_response(FullModelResponse(200, (), b'ok'))
        def controlled_builder(config, *, physical, **options):
            seen.append((physical, dict(options)))
            # This wiring test substitutes the actual Ray assembly, not the Core.
            # It grants no Ray or actual HTTP qualification.
            options['observer'](dict(event='ray_worker_pool', identity_available=False, scope='fixture'))
            return original(config, physical=None, execute=fixture, **options)
        with patch.object(semloom.native_tasks, 'build_native_execution', controlled_builder):
            progress = self.make(physical=physical)
        self.assertIs(seen[0][0], physical)
        self.assertNotIn('transport_factory', seen[0][1])
        self.assertIsNone(progress.transport._client)
        self.assertEqual(self.batch(progress, [call(0)])[0].response.body, b'ok')
        report = progress.finish(method_complete=True, timeout_s=1)
        self.assertTrue(report['ray_backend'])
        self.assertIsNone(report['http_pool_closed'])
        self.assertTrue(report['physical_transport_closed'])
        self.assertFalse(progress.transport.closed)
        self.assertEqual(report['physical_identity']['scope'], 'fixture')


if __name__ == "__main__":
    unittest.main()
