"""Capacity rejection before dispatch and actual declared fields passed to a graph."""

import json
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.native_calls import NativeCallOutcome
from src.execution_provider.adapters.full_response import FullModelResponse
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.semantic_methods.lotus.batch import LotusBatchExecutor
from src.semantic_methods.lotus.limits import LotusCallLimits
from src.semantic_methods.lotus.native import LotusNativeBatchExecutor
from src.semantic_methods.lotus.sdk import PreparedLotusCall
from tests.execution_provider.test_native_tasks import fixture


class CaptureGraph:
    max_batch_rows, max_batch_bytes = 128, 32 * 1024 * 1024
    last_close_report, last_cleanup_errors = None, ()

    def __init__(self):
        self.calls, self.closed = [], False

    def iter_batch(self, calls, **options):
        self.calls.extend(calls)
        try:
            for call in calls:
                yield NativeCallOutcome(
                    call.call_id, call.row_id, FullModelResponse(200, (), b"{}")
                )
        finally:
            self.closed = True


class LotusCallLimitsTests(unittest.TestCase):
    config = FixedModelConfig("http://localhost/v1/chat/completions", "fixture", 1000, "fixture")
    inputs = [([dict(role="user", content="whole input")], None)]

    def selected(self, graph, **options):
        return LotusNativeBatchExecutor(
            graph, self.config, query_id="limits", operator_id="map", **options
        )

    def invoke(self, selected, *, after_parse=None):
        sdk = ModuleType("openai")
        sdk.OpenAIError = type("OpenAIError", (Exception,), {})
        litellm = ModuleType("litellm")
        litellm.cache = None
        prepared = PreparedLotusCall(
            json.dumps(dict(model="fixture", messages=self.inputs[0][0])).encode(),
            "http://localhost/v1",
            "fixture",
            1.0,
        )

        def parse(*args, **kwargs):
            if after_parse:
                after_parse()
            return SimpleNamespace()

        with (
            patch.dict("sys.modules", {"openai": sdk, "litellm": litellm}),
            patch("src.semantic_methods.lotus.native.validate_batch", return_value=prepared),
            patch("src.semantic_methods.lotus.native.parse_native_response", parse),
        ):
            return selected(None, self.inputs, dict(num_retries=0, max_retries=0), False, "fixture")

    def test_imlane_sized_target_2000_rejects_before_preparation_and_graph(self):
        graph = CaptureGraph()
        selected = self.selected(graph)
        with patch("src.semantic_methods.lotus.native.validate_batch") as preparing:
            with self.assertRaisesRegex(ValueError, "target batch rows"):
                selected(None, self.inputs * 2000, {}, False, "fixture")
        preparing.assert_not_called()
        self.assertEqual(graph.calls, [])
        self.assertEqual(selected.last_preflight["effective_max_batch_rows"], 128)
        self.assertEqual(selected.last_preflight["target_rows"], 2000)

    def test_native_default_and_explicit_complete_response_capacity_reach_the_graph(self):
        for limit in (1024 * 1024, 8192):
            graph = CaptureGraph()
            selected = self.selected(
                graph, call_limits=LotusCallLimits(1.0, max_response_bytes=limit)
            )
            self.assertEqual(len(self.invoke(selected)), 1)
            self.assertEqual(graph.calls[0].max_response_bytes, limit)
            self.assertEqual(graph.calls[0].timeout_s, 1.0)
            self.assertEqual(
                selected.call_limits.describe()["response_byte_scope"],
                "complete_http_with_metadata",
            )
            self.assertTrue(graph.closed)

    def test_request_or_batch_input_overflow_never_starts_graph(self):
        for options in (dict(max_request_bytes=1), dict(max_batch_input_bytes=1)):
            graph = CaptureGraph()
            selected = self.selected(graph, call_limits=LotusCallLimits(1.0, **options))
            with self.assertRaisesRegex(ValueError, "byte capacity"):
                self.invoke(selected)
            self.assertEqual(graph.calls, [])

    def test_explicit_profile_rejects_unsupported_engine_item_capacity_before_flow(self):
        execution, _, _ = fixture()
        with self.assertRaisesRegex(ValueError, "Engine item capacity"):
            LotusBatchExecutor(
                execution,
                self.config,
                query_id="limits",
                operator_id="map",
                call_limits=LotusCallLimits(1.0),
            )
        self.assertEqual(execution.engine.jobs.jobs, {})

    def test_request_deadline_must_match_selected_model(self):
        with self.assertRaisesRegex(ValueError, "selected model"):
            self.selected(CaptureGraph(), call_limits=LotusCallLimits(2.0))

    def test_batch_deadline_includes_the_last_sdk_interpretation(self):
        graph = CaptureGraph()
        selected = self.selected(graph, call_limits=LotusCallLimits(1.0, batch_timeout_s=0.01))
        state = dict(now=0.0)
        with patch(
            "src.semantic_methods.lotus.native.time",
            SimpleNamespace(monotonic=lambda: state["now"]),
        ):
            with self.assertRaisesRegex(TimeoutError, "batch deadline"):
                self.invoke(selected, after_parse=lambda: state.update(now=0.02))
        self.assertTrue(graph.closed)


if __name__ == "__main__":
    unittest.main()
