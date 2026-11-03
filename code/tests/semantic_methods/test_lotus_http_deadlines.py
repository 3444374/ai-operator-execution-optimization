"""Caller-declared HTTP deadlines with controlled SDK/Core fixtures, no model."""

from dataclasses import replace
import json
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import NativeTaskSession
from src.scheduling.core.session_contract import Usage
from src.semantic_methods.budget import MethodCapacity, row_reservation
from src.semantic_methods.continuation import MethodLimits
from src.semantic_methods.lotus.batch import LotusBatchExecutor
from src.semantic_methods.lotus.driver import iter_two_map_rows
from src.semantic_methods.lotus.limits import LotusCallLimits
from src.semantic_methods.lotus.sdk import PreparedLotusCall, prepare_call
from tests.execution_provider.test_native_tasks import fixture


MODEL = FixedModelConfig("http://localhost/v1/chat/completions", "fixture", 180000, "fixture")
INPUTS = [([dict(role="user", content="input")], None)]


class LotusHttpDeadlineTests(unittest.TestCase):
    def test_explicit_request_and_batch_deadlines_accept_120_and_180(self):
        for seconds in (120, 120.001, 180):
            limits = LotusCallLimits(seconds, batch_timeout_s=seconds)
            limits.check_model(replace(MODEL, timeout_ms=round(seconds * 1000)))
            self.assertEqual(limits.request_timeout_s, seconds)
            self.assertEqual(limits.batch_timeout_s, seconds)

    def test_deadlines_still_require_finite_positive_numbers(self):
        for value in (True, 0, -1, float("nan"), float("inf"), "180"):
            for field in ("request_timeout_s", "batch_timeout_s"):
                with self.subTest(value=value, field=field), self.assertRaises(ValueError):
                    LotusCallLimits(**(dict(request_timeout_s=180) | {field: value}))

    def test_supervised_batch_and_smaller_call_profiles_remain_explicit(self):
        execution, _, _ = fixture()
        selected = LotusBatchExecutor(execution, MODEL, query_id="supervised", operator_id="map",
                                       timeout_s=None)
        self.assertIsNone(selected.timeout_s)
        self.assertEqual(selected.call_limits.describe()["batch_deadline_scope"],
                         "externally_supervised_completion")
        with self.assertRaisesRegex(ValueError, "selected model"):
            LotusBatchExecutor(execution, MODEL, query_id="mismatch", operator_id="map",
                               call_limits=LotusCallLimits(120))

    def test_explicit_response_capacity_is_not_reduced_to_an_older_engine_limit(self):
        execution, _, _ = fixture()
        with self.assertRaisesRegex(ValueError, "Engine item capacity"):
            LotusBatchExecutor(execution, replace(MODEL, max_response_bytes=2 * 1024 * 1024),
                               query_id="capacity", operator_id="map")
        self.assertEqual(execution.engine.jobs.jobs, {})

    def test_sdk_preserves_explicit_180_second_timeout_without_network(self):
        litellm = ModuleType("litellm")
        litellm.OpenAIConfig = SimpleNamespace(get_config=lambda: {})
        utils = ModuleType("litellm.utils")
        utils.get_optional_params = lambda **kwargs: {}
        config = type("OpenAIGPTConfig", (), {"transform_request": lambda self, **kwargs: dict(
            model=kwargs["model"], messages=kwargs["messages"])})()
        utils.ProviderConfigManager = SimpleNamespace(get_provider_chat_config=lambda **kwargs: config)
        types = ModuleType("litellm.types.utils")
        types.LlmProviders = SimpleNamespace(OPENAI="openai")
        with patch.dict("sys.modules", {"litellm": litellm, "litellm.utils": utils,
                                        "litellm.types.utils": types}), patch(
            "src.semantic_methods.lotus.sdk.validate_source"
        ):
            for seconds in (120, 180):
                call = prepare_call(SimpleNamespace(model="openai/fixture"), INPUTS[0][0],
                    dict(api_base="http://localhost/v1", api_key="fixture", timeout=seconds))
                self.assertEqual(call.timeout_s, seconds)
                self.assertEqual(json.loads(call.payload)["messages"], INPUTS[0][0])
            for seconds in (True, 0, -1, float("nan"), float("inf")):
                with self.subTest(seconds=seconds), self.assertRaises(ValueError):
                    prepare_call(SimpleNamespace(model="openai/fixture"), INPUTS[0][0],
                        dict(api_base="http://localhost/v1", api_key="fixture", timeout=seconds))

    def batch(self, *, batch_timeout=180, slow_prepare=False):
        execution, backend, _ = fixture()
        state = dict(now=0.0, seen=[])
        call = PreparedLotusCall(json.dumps(dict(model="fixture", messages=INPUTS[0][0])).encode(),
                                  "http://localhost/v1", "fixture", 180.0)

        def prepare(*args, **kwargs):
            if slow_prepare:
                state["now"] = 0.02
            return call

        class Flow(NativeTaskSession):
            def wait(self, progress, timeout_s=None):
                for key in tuple(backend.pending):
                    state["seen"].append(backend.pending[key][1].task.payload)
                    backend.complete(key, encode_full_response(FullModelResponse(200, (), b"{}")))

        selected = LotusBatchExecutor(execution, MODEL, query_id="deadline", operator_id="map",
                                       timeout_s=batch_timeout)
        sdk = ModuleType("openai")
        sdk.OpenAIError = type("OpenAIError", (Exception,), {})
        with patch.dict("sys.modules", {"openai": sdk}), patch(
            "src.semantic_methods.lotus.batch.time", SimpleNamespace(monotonic=lambda: state["now"])
        ), patch("src.semantic_methods.lotus.batch.prepare_call", prepare), patch(
            "src.semantic_methods.lotus.batch.NativeTaskSession", Flow
        ), patch("src.semantic_methods.lotus.batch.parse_lotus_response", return_value="parsed"):
            if slow_prepare:
                with self.assertRaisesRegex(TimeoutError, "batch deadline"):
                    selected(None, INPUTS, {}, False, "fixture")
            else:
                self.assertEqual(selected(None, INPUTS, {}, False, "fixture"), ["parsed"])
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertEqual(execution.engine.jobs.jobs, {})
        return selected, backend, state

    def test_180_second_request_reaches_existing_core_with_explicit_batch_deadline(self):
        selected, backend, state = self.batch()
        self.assertEqual(selected.call_limits.request_timeout_s, 180)
        self.assertEqual(selected.timeout_s, 180)
        self.assertEqual(len(backend.routes), 1)
        self.assertEqual(len(state["seen"]), 1)

    def test_smaller_caller_deadline_stops_after_preparation_before_offer(self):
        selected, backend, state = self.batch(batch_timeout=0.01, slow_prepare=True)
        self.assertEqual(selected.timeout_s, 0.01)
        self.assertEqual(backend.routes, [])
        self.assertEqual(state["seen"], [])

    def test_two_map_driver_accepts_explicit_180_second_query_deadline(self):
        for seconds in (120, 180):
            execution, _, _ = fixture()
            limits = MethodLimits(128, 128, 256, 2)
            self.assertEqual(list(iter_two_map_rows(execution, None, (), query_id="driver",
                operator_id="chain", limits=limits,
                capacity=MethodCapacity(2, 2 * row_reservation(limits)), timeout_s=seconds)), [])
            self.assertEqual(execution.engine.capacity.usage(), Usage())
            self.assertEqual(execution.engine.jobs.jobs, {})


if __name__ == "__main__":
    unittest.main()
