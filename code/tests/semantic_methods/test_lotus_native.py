"""Actual LOTUS method and SDK on native vendor execution graphs; zero models."""
from contextlib import contextmanager
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.util import find_spec
import json
import os
from pathlib import Path
import socket
import sys
import threading
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.native_calls import NativeCallExecutor
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.semantic_methods.lotus.batch import lotus_executor
from src.semantic_methods.lotus.native import LotusNativeBatchExecutor
from src.execution_provider.adapters.model_config import FixedModelConfig

from test_lotus_adapter import completion, config, fixture_server as original_fixture, model, STAGES
from src.semantic_methods.lotus.maps import encode, staged_two_map
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "baselines"))
from test_native_calls import complete_server, prepare_native_test_runtime, record_fixture


@contextmanager
def fixture_server(*args, **kwargs):
    with original_fixture(*args, **kwargs) as result:
        try:
            yield result
        finally:
            base, calls = result
            record_fixture("lotus-http", dict(actual_posts=len(calls), requests=list(calls),
                events=[dict(event=name, monotonic_ns=at, body=body) for name, at, body in calls.events]))


@contextmanager
def redirect_server(status):
    calls = []
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append(("POST", self.path, body))
            if self.path.endswith("chat/completions"):
                self.send_response(status)
                self.send_header("Location", "/response")
                self.end_headers()
            else:
                self.respond(body)

        def do_GET(self):
            calls.append(("GET", self.path, None))
            self.respond(next(body for method, path, body in reversed(calls) if body is not None))

        def respond(self, body):
            raw = encode(completion(body))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        if thread.is_alive():
            raise RuntimeError("redirect fixture did not stop")
        record_fixture("lotus-redirect", dict(redirect_status=status,
            actual_posts=sum(method == "POST" for method, path, body in calls),
            actual_gets=sum(method == "GET" for method, path, body in calls),
            requests=[dict(method=method, path=path, body=body) for method, path, body in calls]))


ENABLED = os.environ.get("SEMLOOM_NATIVE_CALL_TEST") == "1" and all(
    find_spec(name) is not None for name in ("lotus", "daft", "ray", "pyarrow"))


@unittest.skipUnless(ENABLED, "actual pinned LOTUS/native graph validation is explicitly opt-in")
class LotusNativeTests(unittest.TestCase):
    BACKENDS = ("daft", "ray-data")
    @classmethod
    def setUpClass(cls):
        import ray
        prepare_native_test_runtime()
        from src.semantic_methods.lotus.sdk import validate_source
        validate_source()
        if ray.is_initialized():
            raise RuntimeError("LOTUS native tests require their own Ray runtime")
        root = Path(os.environ["SEMLOOM_NATIVE_CALL_RAY_TMP"])
        root.mkdir(parents=True, exist_ok=True)
        ray.init(num_cpus=8, num_gpus=0, include_dashboard=False, object_store_memory=134217728,
                 _temp_dir=str(root))
        cls.options = NativeGraphOptions(concurrency=4, ray_actors=2, ray_async_batches_per_actor=2,
                                        ray_batch_rows=1, source_blocks=4)

    @classmethod
    def tearDownClass(cls):
        import ray
        ray.shutdown()

    def executor(self, backend, base, **options):
        native = NativeCallExecutor(backend, self.options)
        return LotusNativeBatchExecutor(native, config(base), query_id="native-query", operator_id="map", **options)

    def test_map_and_cache_usage_logprobs_match_original_without_parser_http(self):
        import lotus
        import pandas as pd
        frame = pd.DataFrame({"text": ["duplicate", "你好", "duplicate", "tail"]})
        for backend in self.BACKENDS:
            with self.subTest(backend=backend), fixture_server() as (base, calls):
                native_lm = model(base)
                with lotus.settings.context(lm=native_lm, enable_cache=True):
                    native = frame.sem_map("Summarize {text}.", return_raw_outputs=True)
                    native_cached = frame.sem_map("Summarize {text}.", return_raw_outputs=True)
                native_bodies = list(calls)
                calls.clear()
                lm = model(base)
                selected = self.executor(backend, base)
                with lotus.settings.context(lm=lm, enable_cache=True), lotus_executor(lm, selected):
                    result = frame.sem_map("Summarize {text}.", return_raw_outputs=True)
                    result_cached = frame.sem_map("Summarize {text}.", return_raw_outputs=True)
                self.assertEqual(result.to_dict("records"), native.to_dict("records"))
                self.assertEqual(result_cached.to_dict("records"), native_cached.to_dict("records"))
                self.assertEqual(lm.stats, native_lm.stats)
                self.assertCountEqual(calls, native_bodies)
                self.assertEqual(len(calls), 4)
                self.assertNotIn("_process_uncached_messages", lm.__dict__)
                calls.clear()
                prompts = [[{"role": "user", "content": text}] for text in ("cached", "miss", "cached")]
                expected_lm, lm = model(base), model(base)
                with lotus.settings.context(enable_cache=True):
                    expected_lm([prompts[0]], show_progress_bar=False, logprobs=True)
                    expected = expected_lm(prompts, show_progress_bar=False, logprobs=True)
                    selected = self.executor(backend, base)
                    with lotus_executor(lm, selected):
                        lm([prompts[0]], show_progress_bar=False, logprobs=True)
                        with patch("httpx.Client.send", side_effect=AssertionError("parser replay invoked real send")):
                            result = lm(prompts, show_progress_bar=False, logprobs=True)
                self.assertEqual(result, expected)
                self.assertEqual(lm.stats, expected_lm.stats)
                self.assertEqual(lm.stats.cache_hits, 2)
                self.assertEqual(len(selected.last_responses), 1)

    def test_original_http_json_and_missing_content_errors_keep_type_and_usage(self):
        import lotus
        cases = ((429, b'{"error":{"message":"fixture quota","type":"rate_limit"}}'),
                 (401, b'{"error":{"message":"fixture auth","type":"auth"}}'),
                 (200, b'{"broken":'),
                 (200, encode(dict(completion({"model": "lotus-fixture", "messages": [{"content": "error"}]}),
                                   choices=[dict(index=0, message=dict(role="assistant", content=None), finish_reason="stop")]))))
        messages = [[{"role": "user", "content": "error"}]]
        for backend in self.BACKENDS:
            for status, raw in cases:
                with self.subTest(backend=backend, status=status, raw=raw[:10]), fixture_server(
                        failure=lambda request: (status, raw)) as (base, calls), lotus.settings.context(enable_cache=False):
                    original_lm = model(base)
                    with self.assertRaises(Exception) as original:
                        original_lm(messages, show_progress_bar=False)
                    lm = model(base)
                    selected = self.executor(backend, base)
                    with self.assertRaises(type(original.exception)) as restored, lotus_executor(lm, selected):
                        lm(messages, show_progress_bar=False)
                    self.assertEqual(lm.stats, original_lm.stats)
                    self.assertEqual(len(calls), 2)
                    self.assertEqual(selected.last_responses[0].body, raw)
                    self.assertEqual(selected.last_responses[0].status_code, status)
                    self.assertEqual(getattr(restored.exception, "status_code", None),
                                     getattr(original.exception, "status_code", None))
                    self.assertTrue(selected.last_close_report["graph_consumer_stopped"])

    def test_compressed_response_and_native_sdk_redirect_behavior_match(self):
        import lotus
        messages = [[dict(role="user", content="compressed")]]
        raw = encode(completion(dict(model="lotus-fixture", messages=messages[0])))
        for backend in self.BACKENDS:
            with self.subTest(backend=backend, compressed=True), complete_server(body=raw, compressed=True) as (endpoint, calls, encoded), \
                    lotus.settings.context(enable_cache=False):
                base = endpoint.split("/complete")[0] + "/v1"
                original_lm, lm = model(base), model(base)
                expected = original_lm(messages, show_progress_bar=False)
                selected = self.executor(backend, base)
                with lotus_executor(lm, selected):
                    result = lm(messages, show_progress_bar=False)
                self.assertEqual(result, expected)
                self.assertEqual(lm.stats, original_lm.stats)
                self.assertEqual(selected.last_responses[0].body, encoded)
                self.assertEqual(len(calls), 2)
            for status in (302, 307):
                with self.subTest(backend=backend, redirect=status), redirect_server(status) as (base, calls), \
                        lotus.settings.context(enable_cache=False):
                    original_lm, lm = model(base), model(base)
                    expected = original_lm(messages, show_progress_bar=False)
                    original_calls = list(calls)
                    calls.clear()
                    selected = self.executor(backend, base)
                    with lotus_executor(lm, selected):
                        result = lm(messages, show_progress_bar=False)
                    self.assertEqual(result, expected)
                    self.assertEqual(lm.stats, original_lm.stats)
                    self.assertEqual(calls, original_calls)
                    self.assertEqual([method for method, path, body in calls], ["POST", "GET" if status == 302 else "POST"])

    def test_staged_two_map_keeps_whole_stage_wait_and_original_prompts(self):
        import lotus
        import pandas as pd
        frame = pd.DataFrame({"text": ["first", "duplicate", "duplicate"]})
        for backend in self.BACKENDS:
            with self.subTest(backend=backend), fixture_server(delay=lambda request: 0.01) as (base, calls), \
                    lotus.settings.context(enable_cache=False):
                original_lm = model(base)
                expected = staged_two_map(frame, original_lm, STAGES)
                original_bodies = list(calls)
                calls.clear()
                calls.events.clear()
                lm = model(base)
                selected = self.executor(backend, base)
                with lotus_executor(lm, selected):
                    result = staged_two_map(frame, lm, STAGES)
                self.assertEqual(result.to_dict("records"), expected.to_dict("records"))
                self.assertCountEqual(calls, original_bodies)
                first_ends = [at for name, at, body in calls.events
                              if name == "end" and "Classify" not in body["messages"][-1]["content"]]
                second_starts = [at for name, at, body in calls.events
                                 if name == "start" and "Classify" in body["messages"][-1]["content"]]
                self.assertEqual((len(first_ends), len(second_starts)), (3, 3))
                self.assertGreater(min(second_starts), max(first_ends))

    def test_closed_endpoint_and_real_read_timeout_keep_native_exception_and_usage(self):
        import lotus
        messages = [[dict(role="user", content="transport failure")]]

        def compare(backend, base, timeout):
            original_lm, lm = model(base), model(base)
            original_lm.kwargs["timeout"] = lm.kwargs["timeout"] = timeout
            selected = LotusNativeBatchExecutor(NativeCallExecutor(backend, self.options),
                FixedModelConfig(base + "/chat/completions", "lotus-fixture", int(timeout * 1000), "local-fixture"),
                query_id="transport", operator_id="map")
            with self.assertRaises(Exception) as original:
                original_lm(messages, show_progress_bar=False)
            with self.assertRaises(type(original.exception)) as restored, lotus_executor(lm, selected):
                lm(messages, show_progress_bar=False)
            self.assertEqual(lm.stats, original_lm.stats)
            self.assertEqual(getattr(restored.exception, "status_code", None), getattr(original.exception, "status_code", None))
            self.assertIsNone(restored.exception.full_response)
            self.assertTrue(restored.exception.native_transport_error)
            self.assertEqual(selected.last_responses, (None,))
            self.assertTrue(selected.last_close_report["graph_consumer_stopped"])
            record_fixture("lotus-transport", dict(backend=backend, timeout_s=timeout,
                original_exception_type=type(original.exception).__name__,
                restored_exception_type=type(restored.exception).__name__,
                original_status=getattr(original.exception, "status_code", None),
                restored_status=getattr(restored.exception, "status_code", None),
                native_transport_error=restored.exception.native_transport_error,
                original_stats=asdict(original_lm.stats), restored_stats=asdict(lm.stats)))

        with lotus.settings.context(enable_cache=False):
            for backend in self.BACKENDS:
                with socket.socket() as reserved:
                    reserved.bind(("127.0.0.1", 0))
                    port = reserved.getsockname()[1]
                with self.subTest(backend=backend, failure="closed"):
                    compare(backend, f"http://127.0.0.1:{port}/v1", 1)
                with self.subTest(backend=backend, failure="read-timeout"), complete_server(delay=0.3) as (endpoint, calls, raw):
                    compare(backend, endpoint.split("/complete")[0] + "/v1", 0.1)

    def test_invalid_parameters_and_cancellation_before_preparation_send_zero_posts(self):
        import lotus
        with fixture_server() as (base, calls), lotus.settings.context(enable_cache=False):
            for backend in self.BACKENDS:
                lm = model(base)
                selected = self.executor(backend, base)
                for change in (dict(num_retries=1), dict(n=2), dict(stream=True)):
                    with self.assertRaises(ValueError), lotus_executor(lm, selected):
                        lm([[dict(role="user", content="first")]], show_progress_bar=False, **change)
                incomplete = model(base)
                incomplete.kwargs.pop("num_retries")
                with self.assertRaisesRegex(ValueError, "explicit zero retries"), lotus_executor(incomplete, selected):
                    incomplete([[dict(role="user", content="first")]], show_progress_bar=False)
                with patch("litellm.cache", object()), self.assertRaisesRegex(ValueError, "cache policy"), lotus_executor(lm, selected):
                    lm([[dict(role="user", content="first")]], show_progress_bar=False)
                selected = self.executor(backend, base, cancelled=lambda: True)
                with self.assertRaises(InterruptedError), lotus_executor(lm, selected):
                    lm([[dict(role="user", content="first")]], show_progress_bar=False)
            self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
