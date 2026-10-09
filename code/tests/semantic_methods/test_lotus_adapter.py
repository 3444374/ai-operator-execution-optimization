"""Actual pinned LOTUS/LiteLLM, deterministic localhost responses, zero real models."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.util import find_spec
import json
import threading
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.semantic_methods.lotus.batch import LotusBatchExecutor, lotus_executor
from src.semantic_methods.lotus.driver import iter_two_map_rows

from src.semantic_methods.lotus.maps import LotusMapStage, LotusTwoMapMethod, encode, staged_two_map
from src.semantic_methods.lotus.sdk import prepare_call, response_model


HAS_LOTUS = find_spec("lotus") is not None
STAGES = (LotusMapStage("Summarize {text}.", "summary"), LotusMapStage("Classify {summary}.", "label"))


def completion(body):
    user = body["messages"][-1]["content"]
    content = "label:" + user if "Classify" in user else "summary:" + user
    return dict(id="fixture-response", object="chat.completion", created=1, model=body["model"],
        choices=[dict(index=0, message=dict(role="assistant", content=content), finish_reason="stop",
                      logprobs=dict(content=[dict(token="fixture", logprob=-0.25, bytes=[102], top_logprobs=[])]))],
        usage=dict(prompt_tokens=7, completion_tokens=3, total_tokens=10,
                   prompt_tokens_details=dict(cached_tokens=2)), provider_extra=dict(retained=True))


@contextmanager
def fixture_server(failure=None, delay=None):
    class Calls(list):
        def __init__(self):
            self.events = []
    calls = Calls()
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append(body)
            calls.events.append(("start", time.monotonic_ns(), body))
            if delay:
                time.sleep(delay(body))
            status, raw = failure(body) if failure else (200, encode(completion(body)))
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            calls.events.append(("end", time.monotonic_ns(), body))

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
            raise RuntimeError("fixture server did not stop")


def model(base, model_id="lotus-fixture"):
    from lotus.cache import InMemoryCache
    from lotus.models import LM
    return LM("openai/" + model_id, api_base=base, api_key="local-fixture", max_tokens=19,
              max_batch_size=2, top_p=1, timeout=5, num_retries=0, max_retries=0,
              cache=InMemoryCache(max_size=128))


def config(base, model_id="lotus-fixture"):
    return FixedModelConfig(base + "/chat/completions", model_id, 5000, "local-fixture")


def close_execution(execution):
    deadline = time.monotonic() + 6
    while execution.engine.capacity.usage().held_tasks and time.monotonic() < deadline:
        execution.engine.advance()
        time.sleep(0.005)
    if execution.engine.capacity.usage().held_tasks or not execution.close():
        raise RuntimeError("LOTUS test execution did not release resources")


@contextmanager
def local_execution(base):
    execution = build_native_execution(config(base), physical=None, max_tasks=2, max_active_requests=2)
    try:
        yield execution
    finally:
        close_execution(execution)


@unittest.skipUnless(HAS_LOTUS, "pinned LOTUS library is unavailable")
class LotusSdkTests(unittest.TestCase):
    def test_sdk_body_matches_native_http_and_complete_response(self):
        import lotus
        with fixture_server() as (base, calls):
            lm = model(base)
            messages = [{"role": "system", "content": "native system"}, {"role": "user", "content": "你好"}]
            kwargs = {**lm.kwargs, "logprobs": True, "top_logprobs": 10}
            prepared = prepare_call(lm, messages, kwargs)
            with lotus.settings.context(enable_cache=False):
                native = lm([messages], show_progress_bar=False, logprobs=True)
            self.assertEqual(len(calls), 1)
            self.assertEqual(json.loads(prepared.payload), calls[0])
            self.assertEqual(calls[0]["max_completion_tokens"], 19)
            restored = response_model(encode(completion(calls[0])))
            self.assertEqual(restored.usage.total_tokens, 10)
            self.assertEqual(restored.usage.prompt_tokens_details.cached_tokens, 2)
            self.assertEqual(lm._get_top_choice(restored), native.outputs[0])
            self.assertEqual(lm._get_top_choice_logprobs(restored), native.logprobs[0])
            self.assertEqual(restored.provider_extra, {"retained": True})

    def test_unhandled_parameters_roles_and_postprocessor_are_rejected(self):
        import lotus
        with fixture_server() as (base, calls):
            lm = model(base)
            messages = [{"role": "user", "content": "text"}]
            for kwargs in ({"stream": True}, {"tools": []}, {"n": 2}, {"num_retries": 1},
                           {"response_format": {"type": "json_object"}}):
                with self.assertRaises(ValueError):
                    prepare_call(lm, messages, {**lm.kwargs, **kwargs})
            with self.assertRaises(ValueError):
                prepare_call(lm, [{"role": "tool", "content": "text"}], lm.kwargs)
            with lotus.settings.context(enable_cache=False):
                with self.assertRaisesRegex(ValueError, "postprocessor"):
                    LotusTwoMapMethod(lm, STAGES, model_config=config(base), postprocessor=lambda *args: None)
                with self.assertRaisesRegex(ValueError, "consume"):
                    LotusTwoMapMethod(lm, (STAGES[0], LotusMapStage("Classify {text}", "label")), model_config=config(base))
            with lotus.settings.context(enable_cache=True):
                with self.assertRaisesRegex(ValueError, "cache disabled"):
                    LotusTwoMapMethod(lm, STAGES, model_config=config(base))
            self.assertEqual(calls, [])

    def test_two_map_method_matches_native_formatter_parser_and_stats(self):
        import lotus
        import pandas as pd
        from src.scheduling.core.session_contract import TaskKey
        from src.semantic_methods.continuation import MethodLimits, MethodRun
        with fixture_server() as (base, calls), lotus.settings.context(enable_cache=False):
            native_lm = model(base)
            native_lm.kwargs["logprobs"] = True
            frame = pd.DataFrame([{"text": "first"}, {"text": "second"}])
            native = staged_two_map(frame, native_lm, STAGES)
            lm = model(base)
            lm.kwargs["logprobs"] = True
            method = LotusTwoMapMethod(lm, STAGES, model_config=config(base))
            completed = []
            expected_calls = []
            for sequence, row in enumerate(frame.to_dict("records")):
                run = MethodRun(method, encode(row), MethodLimits(100000, 100000, 100000, 2))
                for stage in range(2):
                    payload = json.loads(run.pending.payload)
                    expected_calls.append(payload)
                    key = TaskKey(0, sequence * 2 + stage)
                    run.accepted(key)
                    run.complete(key, encode_full_response(FullModelResponse(200, (), encode(completion(payload)))))
                final = json.loads(run.final.value)
                completed.append(final["row"])
                self.assertEqual(len(final["raw_responses"]), 2)
            self.assertEqual(completed, native.to_dict("records"))
            self.assertCountEqual(expected_calls, calls)
            self.assertEqual(lm.stats, native_lm.stats)

    def test_batch_cache_stats_native_dispatch_and_instance_restoration(self):
        import lotus
        import lotus.models.lm as lm_module
        import pandas as pd
        with fixture_server() as (base, calls), local_execution(base) as execution:
            frame = pd.DataFrame({"text": ["one", "two", "one"]})
            native_lm = model(base)
            with lotus.settings.context(lm=native_lm, enable_cache=True):
                with patch.object(lm_module, "batch_completion", wraps=lm_module.batch_completion) as native_dispatch:
                    with lotus_executor(native_lm):
                        native = frame.sem_map("Summarize {text}.")
                        repeated = frame.sem_map("Summarize {text}.")
                self.assertEqual(native_dispatch.call_count, 1)
                self.assertEqual(native_lm.stats.operator_cache_hits, 1)
                self.assertEqual(native.to_dict(), repeated.to_dict())
            lm = model(base)
            visible = []
            executor = LotusBatchExecutor(execution, config(base), query_id="cache-query", operator_id="map",
                                          on_batch=lambda batch, kwargs: visible.append(len(batch)))
            # Both native limiters are bypassed after the whole miss batch is visible.
            lm.rate_limit, lm.tpm_limit = 1, 1
            with lotus.settings.context(lm=lm, enable_cache=True), lotus_executor(lm, executor):
                with self.assertRaisesRegex(ValueError, "already has"), lotus_executor(lm):
                    pass
                with patch.object(lm_module, "batch_completion", side_effect=AssertionError("native pool entered")), \
                     patch.object(lm, "_process_with_rate_limiting", side_effect=AssertionError("native RPM entered")), \
                     patch.object(lm, "_process_with_tpm_limiting", side_effect=AssertionError("native TPM entered")):
                    actual = frame.sem_map("Summarize {text}.")
                    actual_repeat = frame.sem_map("Summarize {text}.")
            self.assertNotIn("_process_uncached_messages", lm.__dict__)
            self.assertEqual(visible, [3])
            self.assertEqual(actual.to_dict(), native.to_dict())
            self.assertEqual(actual_repeat.to_dict(), native.to_dict())
            self.assertEqual(lm.stats, native_lm.stats)
            self.assertEqual(len(executor.last_responses), 3)
            self.assertEqual(len(calls), 6)
            self.assertEqual(execution.engine.capacity.usage().held_tasks, 0)
            self.assertEqual(execution.engine.jobs.jobs, {})

    def test_http_and_parser_errors_retain_full_response_and_release(self):
        import lotus
        from openai import OpenAIError
        for status, raw, error_type in ((429, b'{"error":{"message":"fixture quota"}}', OpenAIError),
                                        (200, b'{not-json}', ValueError)):
            with self.subTest(status=status), fixture_server(lambda body: (status, raw)) as (base, calls), \
                 local_execution(base) as execution:
                lm = model(base)
                executor = LotusBatchExecutor(execution, config(base), query_id="error-query", operator_id="map")
                with lotus.settings.context(enable_cache=False), self.assertRaises(error_type), lotus_executor(lm, executor):
                    lm([[{"role": "user", "content": "one"}]], show_progress_bar=False)
                self.assertNotIn("_process_uncached_messages", lm.__dict__)
                self.assertEqual(executor.last_responses[0].status_code, status)
                self.assertEqual(executor.last_responses[0].body, raw)
                self.assertEqual(len(calls), 1)
                self.assertEqual(execution.engine.capacity.usage().held_tasks, 0)

    def test_lm_partial_cache_and_logprobs_preserve_full_response(self):
        import lotus
        with fixture_server() as (base, calls), local_execution(base) as execution:
            lm = model(base)
            executor = LotusBatchExecutor(execution, config(base), query_id="partial-cache", operator_id="map")
            prompts = [[{"role": "user", "content": value}] for value in ("cached", "uncached", "cached")]
            with lotus.settings.context(enable_cache=True), lotus_executor(lm, executor):
                first = lm(prompts[:1], show_progress_bar=False, logprobs=True)
                result = lm(prompts, show_progress_bar=False, logprobs=True)
            self.assertEqual(result.outputs[0], first.outputs[0])
            self.assertEqual(result.outputs[2], first.outputs[0])
            self.assertEqual(lm.stats.cache_hits, 2)
            self.assertEqual(len(calls), 2)
            self.assertEqual(lm.stats.physical_usage.total_tokens, 20)
            self.assertEqual(lm.stats.virtual_usage.total_tokens, 40)
            self.assertEqual(len(executor.last_responses), 1)
            self.assertEqual(result.logprobs[0], first.logprobs[0])

    def test_batch_row_bound_and_cancellation_never_enter_native_pool(self):
        import lotus
        with fixture_server() as (base, calls), local_execution(base) as execution:
            lm = model(base)
            executor = LotusBatchExecutor(execution, config(base), query_id="bounded", operator_id="map", max_batch_rows=1)
            prompts = [[{"role": "user", "content": "same"}]] * 2
            with lotus.settings.context(enable_cache=False), self.assertRaisesRegex(ValueError, "row limit"), lotus_executor(lm, executor):
                lm(prompts, show_progress_bar=False)
            executor = LotusBatchExecutor(execution, config(base), query_id="cancel", operator_id="map", cancelled=lambda: True)
            with self.assertRaisesRegex(RuntimeError, "cancelled"), lotus_executor(lm, executor):
                lm(prompts, show_progress_bar=False)
            self.assertEqual(calls, [])
            self.assertEqual(execution.engine.capacity.usage().held_tasks, 0)
            self.assertEqual(execution.engine.jobs.jobs, {})

    def test_inflight_cancel_retains_remote_credit_until_completion(self):
        import lotus
        with fixture_server(delay=lambda body: 0.2) as (base, calls):
            execution = build_native_execution(config(base), physical=None, max_tasks=1)
            try:
                executor = LotusBatchExecutor(execution, config(base), query_id="inflight-cancel", operator_id="map",
                                              cancelled=lambda: bool(calls))
                with lotus.settings.context(enable_cache=False), self.assertRaisesRegex(RuntimeError, "cancelled"), \
                     lotus_executor(model(base), executor) as lm:
                    lm([[{"role": "user", "content": "slow"}]], show_progress_bar=False)
                self.assertEqual(len(calls), 1)
                self.assertEqual(execution.engine.capacity.usage().held_tasks, 1)
                self.assertFalse(execution.close())
            finally:
                close_execution(execution)
            self.assertEqual(execution.engine.capacity.usage().held_tasks, 0)
            self.assertEqual(execution.engine.jobs.jobs, {})

    def test_invalid_or_excess_external_input_is_rejected_without_dropping_rows(self):
        import lotus
        from src.semantic_methods.budget import MethodCapacity, row_reservation
        from src.semantic_methods.continuation import MethodLimits
        with fixture_server() as (base, calls), local_execution(base) as execution, lotus.settings.context(enable_cache=False):
            method = LotusTwoMapMethod(model(base), STAGES, model_config=config(base))
            limits = MethodLimits(100000, 100000, 100000, 2)
            capacity = MethodCapacity(1, row_reservation(limits))
            for rows in ([None], [encode({"text": "one"})] * 2):
                with self.assertRaises(ValueError):
                    list(iter_two_map_rows(execution, method, rows, query_id="bad-input", operator_id="chain",
                                          limits=limits, capacity=capacity, max_rows=1))
                self.assertEqual(execution.engine.jobs.jobs, {})
            self.assertEqual(calls, [])

    def test_iterator_matches_staged_rows_and_closes_early(self):
        import lotus
        import pandas as pd
        from src.semantic_methods.budget import MethodCapacity, row_reservation
        from src.semantic_methods.continuation import MethodLimits
        with fixture_server() as (base, calls), local_execution(base) as execution, lotus.settings.context(enable_cache=False):
            frame = pd.DataFrame({"text": ["one", "two", "one"]})
            native = staged_two_map(frame, model(base), STAGES)
            limits = MethodLimits(32768, 65536, 131072, 2)
            capacity = MethodCapacity(2, 2 * row_reservation(limits))
            method = LotusTwoMapMethod(model(base), STAGES, model_config=config(base), max_response_bytes=32768)
            results = list(iter_two_map_rows(execution, method, (encode(r) for r in frame.to_dict("records")),
                query_id="chain", operator_id="two-map", limits=limits, capacity=capacity))
            self.assertEqual([json.loads(r.value)["row"] for r in sorted(results, key=lambda r:r.row.sequence)],
                             native.to_dict("records"))
            self.assertEqual(len(calls), 12)
            self.assertEqual(execution.engine.jobs.jobs, {})
            iterator = iter_two_map_rows(execution, method, (encode(r) for r in frame.to_dict("records")),
                query_id="early-close", operator_id="two-map", limits=limits, capacity=capacity)
            next(iterator)
            iterator.close()
            deadline = time.monotonic() + 5
            while execution.engine.capacity.usage().held_tasks and time.monotonic() < deadline:
                execution.engine.advance()
                time.sleep(0.005)
            self.assertEqual(execution.engine.capacity.usage().held_tasks, 0)
            self.assertEqual(execution.engine.jobs.jobs, {})

    def test_gzip_decode_keeps_the_original_response_bytes(self):
        import gzip
        from src.semantic_methods.lotus.sdk import decode_lotus_response
        raw = encode(completion(dict(model="lotus-fixture", messages=[{"content": "text"}])))
        compressed = gzip.compress(raw)
        full, response = decode_lotus_response(encode_full_response(
            FullModelResponse(200, (("content-encoding", "gzip"),), compressed)))
        self.assertEqual(full.body, compressed)
        self.assertEqual(response.usage.total_tokens, 10)

    def test_priced_response_preserves_native_usage_cost(self):
        import lotus
        with fixture_server() as (base, calls), lotus.settings.context(enable_cache=False):
            native_lm = model(base, "gpt-4o-mini")
            prompts = [[{"role": "user", "content": "priced"}]]
            native = native_lm(prompts, show_progress_bar=False)
            selected = config(base, "gpt-4o-mini")
            execution = build_native_execution(selected, physical=None, max_tasks=1)
            try:
                lm = model(base, "gpt-4o-mini")
                executor = LotusBatchExecutor(execution, selected, query_id="priced", operator_id="map")
                with lotus_executor(lm, executor):
                    actual = lm(prompts, show_progress_bar=False)
                self.assertEqual(actual, native)
                self.assertGreater(native_lm.stats.physical_usage.total_cost, 0)
                self.assertEqual(lm.stats, native_lm.stats)
                self.assertEqual(calls[0], calls[1])
            finally:
                close_execution(execution)

    def test_response_retention_limit_preserves_rejected_body(self):
        import lotus
        with fixture_server() as (base, calls), local_execution(base) as execution:
            saved = []
            executor = LotusBatchExecutor(execution, config(base), query_id="response-limit", operator_id="map",
                max_batch_result_bytes=1, on_response=lambda index, full: saved.append(full))
            with lotus.settings.context(enable_cache=False), lotus_executor(model(base), executor) as lm:
                with self.assertRaisesRegex(ValueError, "byte limit") as caught:
                    lm([[{"role": "user", "content": "complete"}]], show_progress_bar=False)
            self.assertEqual(len(saved), 1)
            self.assertEqual(caught.exception.full_response.body, encode(completion(calls[0])))
            self.assertEqual(execution.engine.capacity.usage().held_tasks, 0)

    def test_method_parse_failure_keeps_native_usage_and_response(self):
        import lotus
        def missing_content(body):
            response = completion(body)
            response["choices"][0]["message"]["content"] = None
            return 200, encode(response)
        with fixture_server(missing_content) as (base, calls), lotus.settings.context(enable_cache=False):
            lm = model(base)
            method = LotusTwoMapMethod(lm, STAGES, model_config=config(base))
            step = method.start(encode({"text": "one"}))
            native_lm = model(base)
            with self.assertRaises(ValueError):
                native_lm([json.loads(step.request.payload)["messages"]], show_progress_bar=False)
            body = missing_content(calls[0])[1]
            with self.assertRaises(ValueError) as caught:
                method.resume(step.state, encode_full_response(FullModelResponse(200, (), body)))
            self.assertEqual(caught.exception.full_response.body, body)
            self.assertEqual(lm.stats, native_lm.stats)
            self.assertEqual(lm.stats.physical_usage.total_tokens, 10)

    def test_method_driver_releases_each_stage_and_advances_one_row_early(self):
        import lotus
        from src.scheduling.core.session_contract import SessionSpec, TaskProfile
        from src.semantic_methods.budget import MethodBudgetPool, MethodCapacity, row_reservation
        from src.semantic_methods.continuation import MethodLimits
        from src.semantic_methods.driver import MethodDriver, RowIdentity
        from tests.scheduling.test_incremental_session import setup
        with fixture_server() as (base, calls), lotus.settings.context(enable_cache=False):
            lm = model(base)
            method = LotusTwoMapMethod(lm, STAGES, model_config=config(base))
            engine, old, backend, _clock = setup(metadata_bytes=1024, held_tasks=2, active_requests=2,
                active_work=2, input_bytes=200000, result_bytes=200000,
                item_input_bytes=100000, item_result_bytes=100000)
            old.close()
            session = engine.open(SessionSpec("two-map", "map-chain", "model",
                                               task_profiles=(TaskProfile("lotus-map", "model"),)))
            limits = MethodLimits(100000, 100000, 100000, 2)
            pool = MethodBudgetPool(MethodCapacity(2, 2 * row_reservation(limits)))
            driver = MethodDriver(session, method, limits, pool.allocate(pool.capacity))
            try:
                for sequence in range(2):
                    self.assertTrue(driver.offer_row(RowIdentity(sequence, "chain"), encode({"text": str(sequence)})))
                driver.end_input()
                driver.advance(2)
                driver.advance(2)
                self.assertEqual(len(backend.pending), 2)
                slow, fast = sorted(backend.pending, key=lambda k: k.sequence)
                def finish(key):
                    body = json.loads(backend.pending[key][1].task.payload)
                    backend.complete(key, encode_full_response(FullModelResponse(200, (), encode(completion(body)))))
                finish(fast)
                driver.advance(2)
                driver.advance(2)
                successor = next(k for k in backend.pending if k != slow)
                self.assertEqual(backend.pending[successor][1].task.info.row_sequence, 1)
                self.assertEqual(backend.pending[successor][1].task.info.stage_id, "1")
                self.assertIn("summary:", json.loads(backend.pending[successor][1].task.payload)["messages"][-1]["content"])
                finish(successor)
                driver.advance(2)
                (result,) = driver.results(2)
                self.assertEqual(result.row.sequence, 1)
                driver.release_result(result.row)
                self.assertEqual(pool.used[0], 1)
                self.assertIn(slow, backend.pending)
            finally:
                driver.close()
                self.assertEqual(engine.capacity.usage().held_tasks, 1)
                for key in tuple(backend.pending):
                    backend.complete(key)
                engine.advance()
            self.assertEqual(pool.allocated, (0, 0))
            self.assertEqual(engine.capacity.usage().held_tasks, 0)
            self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
