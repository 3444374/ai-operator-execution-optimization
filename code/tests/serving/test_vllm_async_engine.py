"""Real async wrapper/shared conversion; every vLLM object is an explicit CPU fake."""

import asyncio
from contextlib import ExitStack
from dataclasses import asdict
import json
import sys
import threading
import types
import unittest
from unittest.mock import patch

from src.serving.backends import vllm_async_engine as module
from src.serving.backends import vllm_engine as common


def prepared(index=0, text="review", query="query"):
    return dict(query_id=query, model_role="main", algorithm_stage="map", row_id="row-" + str(index),
        source_position=index, call_index=0, call_id=query + ":" + str(index),
        messages_json=json.dumps([dict(role="user", content=text)]))


class FakeRuntime:
    def __init__(self, *, hold=(), abort_finish="abort", corrupt_prompt=False, shutdown_gate=None,
                 shutdown_error=False):
        runtime = self
        self.stack = ExitStack()
        self.rendered, self.started, self.samplings = [], [], []
        self.gates = {key: asyncio.Event() for key in hold}
        self.shutdown_calls = 0
        self.loop = asyncio.get_running_loop()

        class Sampling:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.output_kind = None
                runtime.samplings.append(self)

        class Args:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class ChatParams:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class Renderer:
            tokenizer = object()

            async def render_chat_async(self, conversations, params):
                runtime.rendered.append((conversations, params.kwargs))
                return conversations, [dict(type="token", prompt_token_ids=[101, 102, 103])]

        class Client:
            renderer = Renderer()
            model_config = types.SimpleNamespace(enable_prompt_embeds=False)

            def __init__(self):
                self.aborted = set()

            @classmethod
            def from_engine_args(cls, args):
                runtime.args = args.kwargs
                runtime.client = cls()
                return runtime.client

            async def generate(self, prompt, sampling, request_id):
                if prompt != dict(type="token", prompt_token_ids=[101, 102, 103]):
                    raise AssertionError("generate must receive exactly-once rendered token input")
                runtime.started.append((request_id, prompt, sampling))
                if request_id in runtime.gates:
                    await runtime.gates[request_id].wait()
                finish = abort_finish if request_id in self.aborted else "stop"
                probabilities = None
                if sampling.kwargs.get("logprobs") is not None:
                    probabilities = [{5: types.SimpleNamespace(logprob=-0.2, decoded_token="yes")},
                                     {6: types.SimpleNamespace(logprob=-0.3, decoded_token="\n")}]
                yield types.SimpleNamespace(request_id=request_id, finished=True,
                    prompt_token_ids=[999] if corrupt_prompt else prompt["prompt_token_ids"],
                    outputs=[types.SimpleNamespace(index=0, text="yes", token_ids=[5, 6],
                              finish_reason=finish, logprobs=probabilities)])

            async def abort(self, request_id):
                self.aborted.add(request_id)
                if request_id in runtime.gates:
                    runtime.gates[request_id].set()

            def get_tokenizer(self):
                return types.SimpleNamespace(decode=lambda ids: "decoded")

            def shutdown(self, timeout):
                runtime.shutdown_calls += 1
                if shutdown_gate is not None:
                    shutdown_gate.wait(2)
                if shutdown_error:
                    raise RuntimeError("fixture shutdown failed")
                for gate in runtime.gates.values():
                    runtime.loop.call_soon_threadsafe(gate.set)

        modules = {}
        for name in ("vllm", "vllm.engine", "vllm.engine.arg_utils", "vllm.entrypoints",
                     "vllm.entrypoints.chat_utils", "vllm.renderers", "vllm.sampling_params",
                     "vllm.utils", "vllm.utils.mistral", "vllm.v1", "vllm.v1.engine",
                     "vllm.v1.engine.async_llm"):
            modules[name] = types.ModuleType(name)
        modules["vllm"].__version__ = "0.25.1"
        modules["vllm"].SamplingParams = Sampling
        modules["vllm.engine.arg_utils"].AsyncEngineArgs = Args
        modules["vllm.entrypoints.chat_utils"].load_chat_template = lambda template: template
        modules["vllm.renderers"].ChatParams = ChatParams
        modules["vllm.sampling_params"].RequestOutputKind = types.SimpleNamespace(FINAL_ONLY="final-only")
        modules["vllm.sampling_params"].StructuredOutputsParams = lambda **kwargs: types.SimpleNamespace(**kwargs)
        modules["vllm.utils.mistral"].is_mistral_tokenizer = lambda tokenizer: False
        modules["vllm.v1.engine.async_llm"].AsyncLLM = Client
        self.stack.enter_context(patch.dict(sys.modules, modules))
        self.stack.enter_context(patch.object(common.EngineSpec, "validate_local", return_value={}))
        self.stack.enter_context(patch.object(common, "_validate_current_runtime", return_value=None))
        self.stack.enter_context(patch.object(module.importlib.metadata, "version", return_value="0.25.1"))
        self.engine = module.VllmAsyncEngine(common.EngineSpec("/tmp/model", "/tmp/model-python", "/tmp/driver-python"),
                                              common.GenerationSpec(max_tokens=8))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        for gate in self.gates.values():
            gate.set()
        self.stack.close()


class AsyncEngineTests(unittest.IsolatedAsyncioTestCase):
    async def entered(self, runtime, count):
        async with asyncio.timeout(1):
            while len(runtime.started) < count:
                await asyncio.sleep(0)

    async def generate(self, engine, index=0, key="request", **kwargs):
        return await engine.generate_prepared(prepared(index), request_id=key,
            deadline_monotonic=asyncio.get_running_loop().time() + 2, **kwargs)

    async def test_held_first_request_does_not_block_second_and_template_applies_once(self):
        with FakeRuntime(hold=("first",)) as runtime:
            first = asyncio.create_task(self.generate(runtime.engine, 0, "first"))
            await self.entered(runtime, 1)
            second = await self.generate(runtime.engine, 1, "second")
            self.assertFalse(first.done())
            self.assertEqual(second["call_id"], "query:1")
            self.assertEqual((second["prompt_tokens"], second["completion_tokens"]), (3, 2))
            runtime.gates["first"].set()
            result = await first
            self.assertEqual(result["call_id"], "query:0")
            self.assertEqual(len(runtime.rendered), 2)
            self.assertTrue(all(params["chat_template_kwargs"]["add_generation_prompt"] for _, params in runtime.rendered))
            self.assertTrue(all(entry[2].output_kind == "final-only" for entry in runtime.started))
            self.assertEqual(runtime.args["distributed_executor_backend"], "uni")
            await runtime.engine.close()

    async def test_generation_json_and_logprobs_use_shared_conversion_and_do_not_mutate_default(self):
        with FakeRuntime() as runtime:
            default = asdict(runtime.engine.generation)
            generation = common.GenerationSpec(max_tokens=12, logprobs=1, response_format={"type": "json_object"})
            with patch.object(common, "result_from_output", wraps=common.result_from_output) as convert:
                first = await self.generate(runtime.engine, 0, "a", generation=generation)
                await self.generate(runtime.engine, 1, "b")
            self.assertEqual(convert.call_count, 2)
            self.assertEqual(json.loads(first["logprobs_json"])[0]["logprob"], -0.2)
            self.assertEqual(asdict(runtime.engine.generation), default)
            self.assertEqual(runtime.started[0][2].kwargs["max_tokens"], 12)
            self.assertEqual(runtime.started[1][2].kwargs["max_tokens"], 8)
            self.assertIsNot(runtime.started[0][2], runtime.started[1][2])
            self.assertTrue(runtime.started[0][2].kwargs["structured_outputs"].json_object)
            await runtime.engine.close()

    async def test_abort_synthetic_final_keeps_usage_and_unknown_capacity(self):
        with FakeRuntime(hold=("a",)) as runtime:
            task = asyncio.create_task(self.generate(runtime.engine, 0, "a"))
            await self.entered(runtime, 1)
            requested = await runtime.engine.abort("a")
            self.assertFalse(requested["engine_terminal_confirmed"])
            result = await task
            self.assertEqual(result["status"], "unknown")
            self.assertEqual((result["prompt_tokens"], result["completion_tokens"]), (3, 2))
            self.assertEqual(len(runtime.engine._requests), 1)
            with self.assertRaisesRegex(RuntimeError, "unhealthy"):
                await self.generate(runtime.engine, 1, "b")
            await runtime.engine.close()

    async def test_late_real_completion_after_abort_still_keeps_known_success(self):
        with FakeRuntime(hold=("a",), abort_finish="stop") as runtime:
            task = asyncio.create_task(self.generate(runtime.engine, 0, "a"))
            await self.entered(runtime, 1)
            await runtime.engine.abort("a")
            result = await task
            self.assertEqual(result["status"], "succeeded")
            self.assertTrue(result["engine_terminal_confirmed"])
            self.assertEqual(runtime.engine._requests, {})
            await runtime.engine.close()

    async def test_deadline_retains_unknown_borrowing_after_generate_was_entered(self):
        with FakeRuntime(hold=("a",)) as runtime:
            result = await runtime.engine.generate_prepared(prepared(), request_id="a",
                deadline_monotonic=asyncio.get_running_loop().time() + 0.02)
            self.assertEqual(result["status"], "unknown")
            self.assertTrue(result["deadline_expired"])
            self.assertEqual(runtime.engine.ready()["reserved_model_calls"], 1)
            self.assertGreater(runtime.engine._input_bytes, 0)
            await runtime.engine.close()

    async def test_prompt_token_identity_mismatch_is_unknown_with_actual_usage(self):
        with FakeRuntime(corrupt_prompt=True) as runtime:
            result = await self.generate(runtime.engine)
            self.assertEqual(result["status"], "unknown")
            self.assertEqual(result["error_type"], "ResultAssociationError")
            self.assertEqual(result["prompt_tokens"], 1)
            await runtime.engine.close()

    async def test_full_native_capacity_rejects_before_second_model_call(self):
        with FakeRuntime(hold=("a",)) as runtime:
            runtime.engine.limits = module.AsyncEngineLimits(max_inflight=1)
            first = asyncio.create_task(self.generate(runtime.engine, 0, "a"))
            await self.entered(runtime, 1)
            with self.assertRaisesRegex(RuntimeError, "capacity"):
                await self.generate(runtime.engine, 1, "b")
            runtime.gates["a"].set()
            await first
            self.assertEqual(len(runtime.started), 1)
            self.assertEqual((await self.generate(runtime.engine, 1, "b"))["status"], "succeeded")
            await runtime.engine.close()

    async def test_unfinished_shutdown_is_retained_then_same_attempt_can_confirm(self):
        gate = threading.Event()
        with FakeRuntime(shutdown_gate=gate) as runtime:
            first = await runtime.engine.close(timeout=0.02)
            self.assertFalse(first["engine_api_shutdown_confirmed"])
            gate.set()
            second = await runtime.engine.close(timeout=1)
            self.assertTrue(second["engine_api_shutdown_confirmed"])
            self.assertEqual(runtime.shutdown_calls, 1)

    async def test_failed_shutdown_is_never_retried_as_a_false_noop_success(self):
        with FakeRuntime(shutdown_error=True) as runtime:
            first = await runtime.engine.close()
            second = await runtime.engine.close()
            self.assertFalse(first["engine_api_shutdown_confirmed"])
            self.assertFalse(second["engine_api_shutdown_confirmed"])
            self.assertEqual(runtime.shutdown_calls, 1)


if __name__ == "__main__":
    unittest.main()
