"""Native graph dispatch and ownership with real text contracts, no frameworks.

Only vendor API boundaries and existing graph evaluators are replaced. Original
LOTUS stage construction and TextOperation relation restoration remain real.
"""

from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import socket
import subprocess
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import lotus
import pandas as pd

from integrations.imlane_records import chain_backend as imlane_graph
from src.baselines.text.frameworks import (
    daft_lotus_chain as daft_graph,
    ray_data_lotus_chain as ray_graph,
    sembench_text as adapter,
)
from src.baselines.text.sembench_text import TextOperation
from src.semantic_methods.lotus.record_types import (
    ExtractMethod, MapMethod, RecordOccurrence, StageSpec, stage_identity,
)
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_async_engine import VllmAsyncEngine
from src.serving.backends.vllm_engine import VllmEngine
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec


class StreamFixture:
    def __init__(self, rows, calls, *, complete=True, iteration_error=None,
                 close_error=None, close_report=None, after_close=None):
        self.rows, self.calls = tuple(rows), list(calls)
        self.batch_stage_stats = [dict(scope="CPU graph boundary fixture")]
        self.completed, self.closed = False, False
        self.complete_on_eof, self.iteration_error = complete, iteration_error
        self.close_error, self.after_close = close_error, after_close
        self.close_report = close_report or dict(status="closed", cleanup_errors=[])
        self.last_close_report = None

    def __iter__(self):
        yield from self.rows
        if self.iteration_error is not None:
            self.iteration_error.chain_calls = deepcopy(self.calls)
            raise self.iteration_error
        self.completed = self.complete_on_eof

    def close(self):
        self.closed = True
        self.last_close_report = deepcopy(self.close_report)
        if self.after_close is not None:
            self.after_close(self)
        if self.close_error is not None:
            raise self.close_error
        return self.last_close_report


class SemBenchTextBackendTests(unittest.TestCase):
    def setUp(self):
        scope = lotus.settings.context(enable_cache=False)
        scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)
        self.guard_attempts = dict(network=0, real_engine=0, subprocess=0)
        connect = socket.socket.connect

        def forbid_network(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                self.guard_attempts["network"] += 1
                self.fail("graph dispatch attempted a network connection")
            return connect(sock, address)

        def forbid_engine(*args, **kwargs):
            self.guard_attempts["real_engine"] += 1
            self.fail("graph dispatch constructed a real model engine")

        def forbid_subprocess(*args, **kwargs):
            self.guard_attempts["subprocess"] += 1
            self.fail("graph dispatch started a framework or model subprocess")

        for scope in (patch.object(socket.socket, "connect", forbid_network),
                patch.object(VllmEngine, "__init__", forbid_engine),
                patch.object(VllmAsyncEngine, "__init__", forbid_engine),
                patch.object(subprocess, "Popen", forbid_subprocess)):
            scope.start()
            self.addCleanup(scope.stop)
        self.addCleanup(lambda: self.assertEqual(self.guard_attempts,
            dict(network=0, real_engine=0, subprocess=0)))
        self.engine = EngineSpec("/tmp/CPU-backend-fixture", sys.executable, "/tmp/driver",
            max_model_calls=20)
        self.resources = RecordsResources(cpu_workers=1, prepare_batch_rows=2,
            model_batch_rows=2, max_active_batches=1, query_timeout_s=2)
        self.control = Path("/tmp/semloom-backend-boundary-fixture")

    def operation(self, kind="filter", *, empty=False):
        frame = pd.DataFrame(dict(reviewText=["a", "b", "c"],
            business_id=["b0", "b1", "b2"], group=[1, 1, 2]),
            index=pd.Index([7, 7, 9], name="source_label"))
        if empty:
            frame = frame.iloc[:0]
        method = (FilterMethod("openai/local-fixture", "Keep {reviewText}") if kind == "filter"
            else MapMethod("openai/local-fixture", "Label {reviewText}", output_column="_map")
            if kind == "map" else ExtractMethod("openai/local-fixture", ("reviewText",),
                {"declared": "a requested field"}))
        generation = GenerationSpec(max_tokens=32,
            response_format={"type": "json_object"} if kind == "extract" else None)
        stage = StageSpec(kind, method, generation).bind()
        records = tuple(RecordOccurrence("row-" + str(position), position, int(label),
            dict(reviewText=text)) for position, (label, text) in
            enumerate(zip(frame.index, frame.reviewText, strict=True)))
        return TextOperation(frame, stage, records)

    def calls(self, operation):
        return [dict(stage_identity("query", row, operation.stage.stage_id),
            status="succeeded", prompt_tokens=11, completion_tokens=2, total_tokens=13)
            for row in operation.records]

    def install_ray(self, *, initialized=False, init_error=None, shutdown_error=None):
        state = SimpleNamespace(initialized=initialized, shutdown_requested=False, payload=None)
        dataset = object()
        ray, data = ModuleType("ray"), ModuleType("ray.data")
        ray.__path__ = []

        def initialize(**kwargs):
            state.initialized = True
            if init_error is not None:
                raise init_error

        def shutdown():
            state.shutdown_requested = True
            if shutdown_error is not None:
                raise shutdown_error
            state.initialized = False

        def from_items(payload):
            state.payload = payload
            return dataset

        ray.init, ray.shutdown = Mock(side_effect=initialize), Mock(side_effect=shutdown)
        ray.is_initialized = lambda: state.initialized
        data.from_items = Mock(side_effect=from_items)
        ray.data = data
        scope = patch.dict(sys.modules, {"ray": ray, "ray.data": data})
        scope.start()
        self.addCleanup(scope.stop)
        return ray, state, dataset

    def install_daft(self):
        state, frame = SimpleNamespace(payload=None, events=[]), object()
        daft = ModuleType("daft")

        def from_pylist(payload):
            state.events.append("source")
            state.payload = payload
            return frame

        daft.from_pylist = Mock(side_effect=from_pylist)
        scope = patch.dict(sys.modules, {"daft": daft})
        scope.start()
        self.addCleanup(scope.stop)
        configure = patch.object(daft_graph, "configure_native_runner",
            side_effect=lambda threads: state.events.append(("native_runner", threads)))
        configure.start()
        self.addCleanup(configure.stop)
        return daft, state, frame

    def run_operator(self, operation, backend, *, profile=None, context=None):
        return adapter.execute_records(operation, backend=backend, engine=self.engine,
            resources=self.resources, query_id="query", profile={} if profile is None else profile,
            control_dir=self.control, context=context)

    def assert_payload_only(self, payload, operation):
        self.assertTrue(all(set(row) == {"record_json"} for row in payload))
        self.assertEqual([json.loads(row["record_json"]) for row in payload],
            [record.describe() for record in operation.records])
        self.assertTrue(all(set(json.loads(row["record_json"])["values"]) == {"reviewText"}
            for row in payload))

    def assert_graph_binding(self, invocation, operation, source):
        self.assertIs(invocation.args[0], source)
        chain, engine, resources = invocation.args[1:]
        self.assertEqual(chain.query_id, "query")
        self.assertEqual(chain.stages, (operation.stage,))
        self.assertIs(engine, self.engine)
        self.assertIs(resources, self.resources)
        self.assertIs(invocation.kwargs["expected"], operation.records)
        self.assertEqual(invocation.kwargs["query_id"], "query")
        self.assertEqual(invocation.kwargs["columns"], ["reviewText"])
        self.assertIs(invocation.kwargs["control_dir"], self.control)

    def test_ray_filter_passes_only_records_and_retains_filtered_call_evidence(self):
        operation = self.operation()
        stream = StreamFixture((operation.records[2], operation.records[0]), self.calls(operation))
        ray, state, dataset = self.install_ray()
        profile = dict(num_cpus=2, include_dashboard=False)
        with patch.object(ray_graph, "evaluate", return_value=stream) as evaluate:
            report = self.run_operator(operation, "ray-data", profile=profile)
        self.assert_payload_only(state.payload, operation)
        self.assert_graph_binding(evaluate.call_args, operation, dataset)
        self.assertEqual(ray.init.call_args.kwargs, profile)
        self.assertFalse(state.initialized)
        self.assertTrue(report["ray_driver_disconnected"])
        self.assertTrue(stream.closed)
        self.assertEqual(report["calls"], self.calls(operation))
        pd.testing.assert_frame_equal(report["frame"], operation.frame.iloc[[0, 2]])

    def test_daft_map_keeps_native_profile_and_original_business_columns(self):
        operation = self.operation("map")
        outputs = tuple(replace(row, values=dict(row.values, _map=value))
            for row, value in zip(operation.records, ("first", "second", "third"), strict=True))
        stream = StreamFixture(reversed(outputs), self.calls(operation))
        _, state, frame = self.install_daft()
        profile = dict(runner_threads=2)
        with patch.object(daft_graph, "evaluate", return_value=stream) as evaluate:
            report = self.run_operator(operation, "daft", profile=profile)
        self.assert_payload_only(state.payload, operation)
        self.assert_graph_binding(evaluate.call_args, operation, frame)
        self.assertEqual(evaluate.call_args.kwargs["runner_threads"], profile["runner_threads"])
        self.assertEqual(state.events, [("native_runner", 2), "source"])
        expected = operation.frame.copy()
        expected["_map"] = ["first", "second", "third"]
        pd.testing.assert_frame_equal(report["frame"], expected)
        self.assertTrue(stream.closed)
        self.assertEqual(report["backend_close"]["status"], "closed")

    def test_imlane_extract_passes_author_ownership_and_restores_actual_label_writes(self):
        operation = self.operation("extract")
        updates = ({"actual": "early", "business_id": "changed"},
                   {"actual": "late", "extra": "present"}, {})
        outputs = tuple(replace(row, values=dict(row.values, **values), index_updates=values)
            for row, values in zip(operation.records, updates, strict=True))
        stream = StreamFixture(reversed(outputs), self.calls(operation))
        owner = SimpleNamespace(evaluate=Mock(return_value=stream))
        context = dict(author_args=SimpleNamespace(output=self.control),
            model_environment={"CUDA_VISIBLE_DEVICES": ""}, cancelled=lambda: False)
        with patch.object(imlane_graph, "IMLaneChainBackend", return_value=owner) as construct:
            report = self.run_operator(operation, "imlane", context=context)
        options = construct.call_args.kwargs
        self.assertIs(options["engine"], self.engine)
        self.assertIs(options["resources"], self.resources)
        self.assertIs(options["author_args"], context["author_args"])
        self.assertIs(options["model_environment"], context["model_environment"])
        self.assertIs(options["cancelled"], context["cancelled"])
        invocation = owner.evaluate.call_args
        self.assertEqual(tuple(invocation.args[0]), operation.records)
        self.assertEqual(invocation.args[1].stages, (operation.stage,))
        self.assertEqual(invocation.args[1].query_id, "query")
        self.assertIs(invocation.kwargs["expected"], operation.records)
        self.assertEqual(invocation.kwargs["columns"], ["reviewText"])
        result = report["frame"]
        self.assertEqual(result.business_id.tolist(), ["changed", "changed", "b2"])
        self.assertEqual(result.actual.tolist(), ["late", "late", None])
        self.assertEqual(result.extra.tolist(), ["present", "present", None])
        pd.testing.assert_series_equal(result["group"], operation.frame["group"])
        self.assertNotIn("declared", result.columns)
        self.assertTrue(stream.closed)

    def test_incomplete_eof_never_publishes_a_successful_relation(self):
        operation = self.operation()
        stream = StreamFixture(operation.records, self.calls(operation), complete=False)
        _, state, _ = self.install_daft()
        with patch.object(daft_graph, "evaluate", return_value=stream), \
                self.assertRaisesRegex(RuntimeError, "complete successful EOF") as caught:
            self.run_operator(operation, "daft")
        self.assertEqual(state.events, [("native_runner", None), "source"])
        report = caught.exception.operator_report
        self.assertEqual(report["status"], "failed")
        self.assertNotIn("frame", report)
        self.assertEqual(report["calls"], self.calls(operation))
        self.assertTrue(stream.closed)

    def test_close_error_preserves_unknown_physical_identity_and_known_usage(self):
        operation = self.operation("map")
        outputs = tuple(replace(row, values=dict(row.values, _map="label")) for row in operation.records)
        physical = dict(status="unknown", cleanup_errors=["fixture model still unconfirmed"],
            engine_reports=[dict(engine_uid="CPU-fixture", model_pid=12345)])
        error = OSError("fixture close failed")
        stream = StreamFixture(outputs, self.calls(operation), close_error=error, close_report=physical)
        self.install_daft()
        with patch.object(daft_graph, "evaluate", return_value=stream), self.assertRaises(OSError) as caught:
            self.run_operator(operation, "daft")
        self.assertIs(caught.exception, error)
        report = error.operator_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["backend_close"]["status"], "unknown")
        self.assertEqual(report["backend_close"]["engine_reports"], physical["engine_reports"])
        self.assertEqual(report["backend_close"]["close_error_type"], "OSError")
        self.assertEqual(report["calls"], self.calls(operation))
        self.assertNotIn("frame", report)

    def test_iteration_failure_survives_close_and_ray_shutdown_failures_with_final_ledger(self):
        operation = self.operation()
        calls = self.calls(operation)
        primary = ValueError("original graph iteration failed")
        physical = dict(status="unknown", engine_reports=[dict(engine_uid="CPU-fixture", model_pid=12345)])
        stream = StreamFixture(operation.records[:1], calls[:1], iteration_error=primary,
            close_error=OSError("fixture close failed"), close_report=physical,
            after_close=lambda target: target.calls.extend(calls[1:]))
        _, state, _ = self.install_ray(shutdown_error=LookupError("fixture shutdown failed"))
        with patch.object(ray_graph, "evaluate", return_value=stream), self.assertRaises(ValueError) as caught:
            self.run_operator(operation, "ray-data")
        self.assertIs(caught.exception, primary)
        report = primary.operator_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["calls"], calls)
        self.assertEqual(report["backend_close"]["engine_reports"], physical["engine_reports"])
        self.assertEqual(report["backend_close"]["status"], "unknown")
        self.assertIsNone(report["ray_driver_disconnected"])
        self.assertTrue(state.shutdown_requested)
        self.assertTrue(any("native operator close failed: OSError" in note for note in primary.__notes__))
        self.assertTrue(any("Ray runtime close failed: LookupError" in note for note in primary.__notes__))

    def test_partial_ray_init_failure_still_closes_its_runtime(self):
        operation = self.operation()
        error = OSError("Ray init failed after allocating state")
        _, state, _ = self.install_ray(init_error=error)
        with patch.object(ray_graph, "evaluate", side_effect=AssertionError("failed init opened a graph")), \
                self.assertRaises(OSError) as caught:
            self.run_operator(operation, "ray-data")
        self.assertIs(caught.exception, error)
        self.assertFalse(state.initialized)
        self.assertTrue(state.shutdown_requested)
        self.assertTrue(error.operator_report["ray_driver_disconnected"])
        self.assertEqual(error.operator_report["calls"], [])

    def test_partial_ray_init_and_shutdown_failure_keeps_original_error_and_unknown_state(self):
        operation = self.operation()
        error = OSError("original init failure")
        _, state, _ = self.install_ray(init_error=error, shutdown_error=LookupError("shutdown failed"))
        with patch.object(ray_graph, "evaluate", side_effect=AssertionError("failed init opened a graph")), \
                self.assertRaises(OSError) as caught:
            self.run_operator(operation, "ray-data")
        self.assertIs(caught.exception, error)
        self.assertTrue(state.shutdown_requested)
        self.assertIsNone(error.operator_report["ray_driver_disconnected"])
        self.assertEqual(error.operator_report["status"], "failed")
        self.assertTrue(any("Ray runtime close failed: LookupError" in note for note in error.__notes__))

    def test_existing_ray_runtime_is_rejected_without_shutting_down_another_owner(self):
        operation = self.operation()
        _, state, _ = self.install_ray(initialized=True)
        with patch.object(ray_graph, "evaluate", side_effect=AssertionError("borrowed runtime opened a graph")), \
                self.assertRaisesRegex(ValueError, "isolated Ray runtime") as caught:
            self.run_operator(operation, "ray-data")
        self.assertTrue(state.initialized)
        self.assertFalse(state.shutdown_requested)
        self.assertNotIn("ray_driver_disconnected", caught.exception.operator_report)

    def test_empty_relations_do_not_allocate_any_framework_or_model(self):
        operation = self.operation(empty=True)
        ray, state, _ = self.install_ray(initialized=True)
        daft, daft_state, _ = self.install_daft()
        forbidden = AssertionError("empty relation allocated a framework")
        with patch.object(ray, "init", side_effect=forbidden), \
                patch.object(ray.data, "from_items", side_effect=forbidden), \
                patch.object(daft, "from_pylist", side_effect=forbidden), \
                patch.object(ray_graph, "evaluate", side_effect=forbidden), \
                patch.object(daft_graph, "evaluate", side_effect=forbidden), \
                patch.object(imlane_graph, "IMLaneChainBackend", side_effect=forbidden):
            for backend in ("ray-data", "daft", "imlane"):
                with self.subTest(backend=backend):
                    report = self.run_operator(operation, backend)
                    self.assertEqual(report["status"], "completed")
                    self.assertEqual(report["calls"], [])
                    self.assertEqual(report["backend_close"]["status"], "closed")
                    pd.testing.assert_frame_equal(report["frame"], operation.frame)
        self.assertTrue(state.initialized)
        self.assertFalse(state.shutdown_requested)
        self.assertEqual(daft_state.events, [])


if __name__ == "__main__":
    unittest.main()
