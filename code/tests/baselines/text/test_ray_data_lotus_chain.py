"""CPU fixtures verify the native chain contract, never GPU performance."""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import fields
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.text.frameworks import ray_data_lotus_chain as native
from src.baselines.text.frameworks import lotus_chain_batch as shared_batch
from src.baselines.text.frameworks import lotus_chain_engine_process as transport
from src.baselines.text.frameworks.lotus_chain_results import ChainBatchResults, ChainExecutionError
from src.semantic_methods.lotus.record_chain import RecordOccurrence, stage_identity
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS


SUMMARY = "肯定摄影与节奏，整体积极。"
NEGATIVE_SUMMARY = "整体消极，不推荐。"
REVIEW = "摄影很美，节奏紧凑，是一部值得推荐的电影。"


def records():
    return [RecordOccurrence("r17", 0, 17, {"reviewText": REVIEW, "group_key": "g"}),
            RecordOccurrence("r18", 1, 18, {"reviewText": "情节拖沓，我没有看完。", "group_key": "g"}),
            RecordOccurrence("r19", 2, 19, {"reviewText": REVIEW, "group_key": "g"})]


class Table:
    def __init__(self, rows):
        self.rows = rows

    def to_pylist(self):
        return self.rows

    @property
    def num_rows(self):
        return len(self.rows)

    @classmethod
    def from_pylist(cls, rows):
        return cls(rows)


class FixtureChain:
    """Explicit native-graph fixture; original formatter/parser tests live upstream."""

    def __init__(self, query_id="q", failure=None, different_model=False, model_role="main"):
        self.query_id, self.failure = query_id, failure
        self.different_model, self.model_role = different_model, model_role
        self.stages = tuple(SimpleNamespace(
            stage_id=stage, model_role=model_role, kind=kind,
            method=SimpleNamespace(output_column="summary",
                model_name="other" if different_model and tokens == 37 else "fixture"),
            generation=GenerationSpec(max_tokens=tokens, stop=()))
            for stage, tokens, kind in (("keep-reviews", 8, "filter"), ("summarize-reviews", 37, "map"),
                                        ("extract-aspects", 55, "extract")))

    def describe(self):
        return dict(query_id=self.query_id, failure=self.failure,
                    different_model=self.different_model, model_role=self.model_role)

    @classmethod
    def from_dict(cls, value):
        return cls(**value)

    def run_batch(self, inputs, infer):
        current = [RecordOccurrence.from_dict(record.describe()) for record in inputs]
        calls, parents = [], {}
        columns = list(dict.fromkeys(key for record in inputs for key in record.values)) + ["summary"]
        try:
            for index, stage in enumerate(self.stages):
                if not current:
                    continue
                prepared = [dict(stage_identity(self.query_id, record, stage.stage_id, model_role=stage.model_role),
                                 messages_json=json.dumps([{"role": "user", "content":
                                     record.values["summary" if index == 2 else "reviewText"]}]))
                            for record in current]
                outputs = infer(prepared, generation=stage.generation)
                if self.failure == "association" and index == 1:
                    error = ValueError("fixture association failure")
                    error.chain_raw_results = outputs
                    raise error
                for record, result in zip(current, outputs, strict=True):
                    result["parent_call_ids"] = parents.get(record.row_id, [])
                    parents[record.row_id] = [result["call_id"]]
                    calls.append(result)
                if any(result["status"] != "succeeded" for result in outputs):
                    raise RuntimeError("fixture unknown generation")
                if self.failure == "parser" and index == 2:
                    raise ValueError("fixture parser failure")
                if index == 0:
                    for result in outputs:
                        result["decision"] = result["text"] == "True"
                    current = [record for record, result in zip(current, outputs, strict=True)
                               if result["decision"]]
                else:
                    for record, result in zip(current, outputs, strict=True):
                        record.values.update({"summary": result["text"]} if index == 1
                                             else json.loads(result["text"]))
            columns.extend(key for record in current for key in record.values if key not in columns)
            return dict(records=current, calls=calls, columns=list(dict.fromkeys(columns)),
                        stage_stats={"scope": "fixture"})
        except BaseException as exc:
            exc.chain_calls, exc.chain_stage_stats = calls, {"scope": "fixture"}
            raise


class NativeChainTests(unittest.TestCase):
    def setUp(self):
        self.inputs = records()
        self.spec = EngineSpec("/tmp/fixture-model", "/tmp/model-python", "/tmp/driver-python")
        self.resources = RecordsResources(model_batch_rows=2)
        self.chain = FixtureChain()
        self.root = tempfile.TemporaryDirectory(prefix="ray-chain-contract-")
        self.addCleanup(self.root.cleanup)

    def runtime(self, *, version="2.56.1", nodes=1, unknown_stage=None, close_failure=False, dynamic_keys=False):
        events, engines = [], []
        owner = self

        class Kernel:
            def __init__(self, engine, generation, control, deadline, startup_timeout_s):
                self.control = Path(control)
                self.calls, self.closed = [], 0
                engines.append(self)
                events.append(("engine_open",))

            def run_prepared(self, rows, *, generation):
                owner.assertEqual(self.closed, 0, "a dependent stage reached an already closed model")
                events.append(("engine_run", rows[0]["algorithm_stage"], len(rows)))
                self.calls.append((rows, generation))
                outputs = []
                for row in rows:
                    stage, prompt = row["algorithm_stage"], row["messages_json"]
                    text = ("False" if "情节拖沓" in json.loads(prompt)[0]["content"] else "True")
                    if stage == "summarize-reviews":
                        owner.assertIn(REVIEW, json.loads(prompt)[0]["content"])
                        text = SUMMARY
                    elif stage == "extract-aspects":
                        owner.assertEqual(json.loads(prompt)[0]["content"], SUMMARY)
                        text = json.dumps({"aspect": "摄影、节奏", "attitude": "积极"}, ensure_ascii=False)
                        if dynamic_keys:
                            text = json.dumps({"early_key" if row["row_id"] == "r17" else "late_key": "value"})
                    outputs.append(dict({key: row[key] for key in IDENTITY_FIELDS}, text=text,
                        status="unknown" if stage == unknown_stage else "succeeded",
                        prompt_tokens=11, completion_tokens=2, total_tokens=13,
                        engine_uid="cpu-fixture"))
                if owner.chain.failure == "association" and rows[0]["algorithm_stage"] == "summarize-reviews":
                    outputs[0]["call_id"] = "unassociated-fixture-result"
                return outputs

            def close(self):
                self.closed += 1
                events.append(("engine_close",))
                report = dict(status="unknown" if close_failure else "closed", fixture=True,
                    process_exit_confirmed=True, gpu_exit_confirmed=None,
                    cleanup_errors=["fixture_close_error"] if close_failure else [])
                (self.control / "engine-fixture.json").write_text(json.dumps(report))
                return report

        class Dataset:
            def __init__(self, rows, steps=()):
                self.rows, self.steps = rows, steps
                self._current_executor = SimpleNamespace(shutdown=lambda **kw: events.append(("shutdown", kw)))

            def map_batches(self, cls, **kwargs):
                events.append(("map_batches", cls, kwargs))
                return Dataset(self.rows, self.steps + ((cls, kwargs),))

            def iter_rows(self):
                events.append(("iter_rows",))
                for cls, kwargs in self.steps:
                    worker = cls(**kwargs["fn_constructor_kwargs"]) if "fn_constructor_kwargs" in kwargs else cls
                    for offset in range(0, len(self.rows), kwargs["batch_size"]):
                        yield from worker(Table(self.rows[offset:offset + kwargs["batch_size"]])).to_pylist()

        ray = ModuleType("ray")
        ray.__version__, ray.is_initialized = version, lambda: True
        ray.nodes = lambda: [{"Alive": True} for _ in range(nodes)]
        ray.data = ModuleType("ray.data")
        ray.data.ActorPoolStrategy = lambda **kwargs: kwargs
        stack = ExitStack()
        stack.enter_context(patch.dict(sys.modules, {"ray": ray, "ray.data": ray.data,
                                                    "pyarrow": SimpleNamespace(Table=Table)}))
        stack.enter_context(patch.object(native, "LotusRecordChain", FixtureChain))
        stack.enter_context(patch.object(shared_batch, "LotusRecordChain", FixtureChain))
        stack.enter_context(patch.object(EngineSpec, "validate_driver_environment", return_value={}))
        stack.enter_context(patch.object(transport, "ChainEngineProcess", Kernel))
        return stack, Dataset([native.record_row(record) for record in self.inputs]), events, engines

    def evaluate(self, dataset, **kwargs):
        return native.evaluate(dataset, self.chain, self.spec, self.resources,
            expected=self.inputs, query_id=self.chain.query_id, control_dir=self.root.name, **kwargs)

    def kernel(self):
        control = Path(self.root.name) / "shared-kernel"
        control.mkdir()
        (control / "owner").write_text("fixture")
        return native.FusedChainBatch(self.chain.describe(), self.spec.__dict__,
            [native._source_binding(record) for record in self.inputs], control,
            time.monotonic() + 5, 3, native.socket.gethostname(), "fixture")

    def test_shared_kernel_plain_rows_and_pure_results_preserve_the_ray_envelope(self):
        stack, _, _, engines = self.runtime()
        with stack:
            kernel = self.kernel()
            envelope = kernel.run_rows([native.record_row(record) for record in self.inputs])
            results = ChainBatchResults(self.inputs, self.chain)
            results.accept(envelope)
            results.verify_coverage()
            results.finalize_columns()
            rows = [results.pop() for _ in range(len(results.pending))]
        self.assertIs(native.FusedChainBatch, shared_batch.FusedChainBatch)
        self.assertEqual([row.row_id for row in rows], ["r17", "r19"])
        self.assertEqual(len(results.calls), 7)
        self.assertEqual(sum(call["total_tokens"] for call in results.calls), 91)
        self.assertEqual(len(engines), 1)
        self.assertNotIn("_iterator", results.__dict__)
        self.assertNotIn("result", results.__dict__)

    def test_shared_kernel_close_before_start_and_finite_timeout_validation(self):
        stack, _, _, engines = self.runtime()
        with stack:
            kernel = self.kernel()
            report = kernel.close()
            self.assertEqual(report["status"], "closed")
            self.assertTrue(report["gpu_exit_confirmed"])
            for value in (True, 0, -1, float("inf"), float("nan")):
                with self.subTest(timeout=value), self.assertRaises(ValueError):
                    kernel.close(value)
        self.assertEqual(engines, [])

    def test_shared_results_error_and_recovery_preserve_known_prefix_once(self):
        stack, _, _, _ = self.runtime()
        with stack:
            envelope = self.kernel().run_rows([native.record_row(record) for record in self.inputs])
        envelope.update(error_type="FixtureParserError", records=[])
        results = ChainBatchResults(self.inputs, self.chain)
        with self.assertRaises(ChainExecutionError) as caught:
            results.accept(envelope)
        self.assertEqual(caught.exception.remote_error_type, "FixtureParserError")
        self.assertEqual(len(caught.exception.chain_calls), 7)
        results.recover(envelope)
        results.recover(envelope)
        self.assertEqual(len(results.calls), 7)
        self.assertEqual(sum(call["total_tokens"] for call in results.calls), 91)
        self.assertFalse(results.pending)

    def test_shared_kernel_failed_constructor_retains_unknown_owner_without_retry(self):
        stack, _, _, _ = self.runtime()
        attempts = []

        class BrokenProcess:
            def __init__(self, *args):
                attempts.append(args)
                raise RuntimeError("fixture allocation failed")

        with stack:
            kernel = self.kernel()
            kernel._engine_type = BrokenProcess
            first = kernel.run_rows([native.record_row(record) for record in self.inputs])
            self.assertEqual(first["error_type"], "RuntimeError")
            self.assertEqual(kernel.run_rows([]), first)
            for _ in range(2):
                report = kernel.close()
                self.assertEqual(report["status"], "unknown")
                self.assertFalse(report["engine_api_shutdown_confirmed"])
                self.assertIsNone(report["gpu_exit_confirmed"])
        self.assertEqual(len(attempts), 1)

    def test_shared_kernel_close_retries_the_same_private_owner(self):
        stack, _, _, _ = self.runtime()

        class ClosingProcess:
            def __init__(self):
                self.attempts = []

            def close(self, timeout):
                self.attempts.append(timeout)
                return dict(status="unknown" if len(self.attempts) == 1 else "closed",
                            gpu_exit_confirmed=None)

        with stack:
            kernel = self.kernel()
            process = kernel.engine = ClosingProcess()
            self.assertEqual(kernel.close(2)["status"], "unknown")
            self.assertEqual(kernel.close(3)["status"], "closed")
            self.assertIs(kernel.engine, process)
        self.assertEqual(process.attempts, [2, 3])

    def test_native_batches_preserve_dependency_occurrences_columns_and_model_reuse(self):
        stack, dataset, events, engines = self.runtime()
        with stack:
            stream = self.evaluate(dataset)
            result = list(stream)
        self.assertEqual([row.row_id for row in result], ["r17", "r19"])
        self.assertEqual([row.source_index for row in result], [17, 19])
        self.assertEqual(result[0].values["reviewText"], result[1].values["reviewText"])
        self.assertTrue(all(row.values["summary"] == SUMMARY for row in result))
        self.assertTrue(all(row.values["group_key"] == "g" for row in result))
        self.assertEqual(stream.columns, ["reviewText", "group_key", "summary", "aspect", "attitude"])
        self.assertEqual((len(stream.calls), len({row["call_id"] for row in stream.calls})), (7, 7))
        self.assertEqual(sum(row["prompt_tokens"] for row in stream.calls), 77)
        self.assertEqual([call["algorithm_stage"] for call in stream.calls if call["row_id"] == "r18"],
                         ["keep-reviews"])
        self.assertEqual(len(engines), 1)
        self.assertEqual((len(engines[0].calls), engines[0].closed), (6, 1))
        self.assertEqual([generation.max_tokens for _, generation in engines[0].calls], [8, 37, 55, 8, 37, 55])
        self.assertEqual(len([event for event in events if event[0] == "map_batches"]), 1)
        self.assertEqual(len([event for event in events if event[0] == "iter_rows"]), 1)
        options = events[0][2]
        self.assertEqual((options["num_gpus"], options["num_cpus"]), (1, 1))
        self.assertEqual((options["max_restarts"], options["max_task_retries"]), (0, 0))
        self.assertEqual(options["runtime_env"]["py_executable"], self.spec.driver_python)
        self.assertEqual(stream.last_close_report["model_evaluations"], 7)
        self.assertTrue(stream.completed)
        self.assertIsNone(stream.last_close_report["gpu_exit_confirmed"])

    def test_all_discarded_batch_still_delivers_filter_usage_and_empty_relation_schema(self):
        self.inputs = [self.inputs[1]]
        stack, dataset, _, engines = self.runtime()
        with stack:
            stream = self.evaluate(dataset)
            self.assertEqual(list(stream), [])
        self.assertEqual(len(stream.calls), 1)
        self.assertEqual(len(engines[0].calls), 1)
        self.assertEqual(stream.columns, ["reviewText", "group_key", "summary"])
        self.assertEqual(stream.last_close_report["received_occurrences"], 1)

    def test_single_pass_iterator_and_materialized_reads_do_not_repeat_model_work(self):
        stack, dataset, events, engines = self.runtime()
        with stack:
            stream = self.evaluate(dataset)
            self.assertIs(iter(stream), stream)
            materialized = list(stream)
            first_records = [row.describe() for row in materialized]
            first_calls = [dict(call) for call in stream.calls]
            first_events = list(events)
            self.assertEqual(list(stream), [])
            self.assertEqual([row.describe() for row in tuple(materialized)], first_records)
            self.assertEqual((stream.calls, events), (first_calls, first_events))
            self.assertEqual(len(engines), 1)
            self.assertEqual(engines[0].closed, 1)
            self.chain = FixtureChain(query_id="independent-query")
            another = self.evaluate(dataset)
            self.assertEqual([row.row_id for row in another], [row.row_id for row in materialized])
            self.assertEqual(len(engines), 2)
            self.assertTrue({call["call_id"] for call in first_calls}.isdisjoint(
                call["call_id"] for call in another.calls))

    def test_all_source_filter_results_do_not_close_engine_before_dependent_stages_finish(self):
        self.inputs = [RecordOccurrence(row.row_id, row.source_position, row.source_index,
                                       {**row.values, "reviewText": REVIEW}) for row in self.inputs]
        self.resources = RecordsResources(model_batch_rows=len(self.inputs))
        stack, dataset, events, engines = self.runtime()
        with stack:
            stream = self.evaluate(dataset)
            self.assertEqual(len(list(stream)), len(self.inputs))
        lifecycle = [event for event in events if event[0].startswith("engine_")]
        self.assertEqual(lifecycle, [("engine_open",), ("engine_run", "keep-reviews", 3),
            ("engine_run", "summarize-reviews", 3), ("engine_run", "extract-aspects", 3),
            ("engine_close",)])
        self.assertEqual(engines[0].closed, 1)
        self.assertEqual(len(stream.calls), 9)

    def test_duplicate_source_labels_remain_distinct_occurrences_across_batches(self):
        self.inputs = [RecordOccurrence(row.row_id, row.source_position, "same-index", row.values,
                        source_refs={"reviews": {"member": row.row_id}}) for row in self.inputs]
        stack, dataset, _, _ = self.runtime()
        with stack:
            stream = self.evaluate(dataset)
            result = list(stream)
        self.assertEqual([row.source_index for row in result], ["same-index", "same-index"])
        self.assertEqual([row.source_refs["reviews"]["member"] for row in result], ["r17", "r19"])

    def test_dynamic_extract_columns_follow_source_order_after_out_of_order_native_batches(self):
        self.resources = RecordsResources(model_batch_rows=1)
        stack, dataset, _, _ = self.runtime(dynamic_keys=True)
        dataset.rows = list(reversed(dataset.rows))
        with stack:
            stream = self.evaluate(dataset)
            list(stream)
        self.assertEqual(stream.columns, ["reviewText", "group_key", "summary", "early_key", "late_key"])

    def test_repeated_source_in_later_batch_is_rejected_before_another_model_evaluation(self):
        stack, dataset, _, engines = self.runtime()
        dataset.rows.append(dataset.rows[0])
        with stack:
            stream = self.evaluate(dataset)
            with self.assertRaises(native.RayChainExecutionError):
                list(stream)
        self.assertEqual(len(engines[0].calls), 3)
        self.assertEqual(len(stream.calls), 4)
        self.assertFalse(stream.completed)

    def test_empty_declaration_still_validates_the_native_source_without_allocating_a_model(self):
        for nonempty in (False, True):
            with self.subTest(nonempty=nonempty):
                self.inputs = []
                stack, dataset, events, engines = self.runtime()
                if nonempty:
                    dataset.rows = [native.record_row(record) for record in records()]
                with stack:
                    stream = self.evaluate(dataset)
                    if nonempty:
                        with self.assertRaisesRegex(ValueError, "empty source declaration"):
                            list(stream)
                    else:
                        self.assertEqual(list(stream), [])
                self.assertEqual(engines, [])
                self.assertEqual(stream.completed, not nonempty)
                self.assertEqual(events[0][1], native._empty_source_batch)

    def test_parser_failure_and_unknown_call_keep_usage_and_do_not_publish_partial_relation(self):
        for failure in ("parser", "unknown"):
            with self.subTest(failure=failure):
                self.chain = FixtureChain(failure="parser" if failure == "parser" else None)
                stack, dataset, _, engines = self.runtime(
                    unknown_stage="summarize-reviews" if failure == "unknown" else None)
                with stack:
                    stream = self.evaluate(dataset)
                    with self.assertRaisesRegex(RuntimeError, "chain failed") as caught:
                        list(stream)
                self.assertEqual(len(stream.calls), 4 if failure == "parser" else 3)
                self.assertEqual(len(caught.exception.chain_calls), len(stream.calls))
                self.assertEqual(caught.exception.remote_error_type, "ValueError" if failure == "parser" else "RuntimeError")
                self.assertFalse(stream.completed)
                if failure == "parser":
                    self.assertTrue(all(call["status"] == "succeeded" for call in stream.calls))
                self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 13 * len(stream.calls))
                self.assertEqual(len(engines[0].calls), 3 if failure == "parser" else 2)

    def test_cleanup_failure_keeps_completed_calls(self):
        stack, dataset, _, _ = self.runtime(close_failure=True)
        with stack:
            stream = self.evaluate(dataset)
            with self.assertRaisesRegex(RuntimeError, "cleanup"):
                list(stream)
        self.assertEqual(len(stream.calls), 7)
        self.assertEqual(stream.last_close_report["status"], "unknown")

    def test_source_content_binding_rejects_same_identity_with_replaced_values_before_model(self):
        stack, dataset, _, engines = self.runtime()
        changed = self.inputs[0].describe()
        changed["values"]["reviewText"] = "replaced source content"
        dataset.rows[0] = native.record_row(RecordOccurrence.from_dict(changed))
        with stack:
            stream = self.evaluate(dataset)
            with self.assertRaisesRegex(RuntimeError, "chain failed: ValueError"):
                list(stream)
        self.assertEqual((stream.calls, engines), ([], []))
        self.assertEqual(stream.last_close_report["status"], "closed")

    def test_unassociated_raw_response_keeps_usage_without_becoming_a_successful_call(self):
        self.chain = FixtureChain(failure="association")
        stack, dataset, _, engines = self.runtime()
        with stack:
            stream = self.evaluate(dataset)
            with self.assertRaisesRegex(RuntimeError, "chain failed") as caught:
                list(stream)
        self.assertEqual(len(stream.calls), 3)
        self.assertEqual(sum(call["status"] == "unknown" for call in stream.calls), 1)
        self.assertEqual(len(stream.unassociated_results), 1)
        self.assertEqual(stream.unassociated_results[0]["total_tokens"], 13)
        self.assertEqual(caught.exception.chain_raw_results, stream.unassociated_results)
        self.assertEqual(len(engines[0].calls), 2)

    def test_partial_association_failure_recovers_one_raw_snapshot_without_duplicate_evidence(self):
        self.chain = FixtureChain(failure="association")
        self.inputs = [self.inputs[0], self.inputs[2]]
        stack, dataset, _, _ = self.runtime()
        with stack:
            stream = self.evaluate(dataset)
            with self.assertRaises(native.RayChainExecutionError) as caught:
                list(stream)
        self.assertEqual(len(stream.calls), 4)
        self.assertEqual(len(caught.exception.chain_raw_results), 2)
        self.assertEqual(len(stream.unassociated_results), 1)
        self.assertEqual(sum(row["status"] == "unknown" for row in stream.calls), 1)

    def test_raw_snapshot_recovery_is_idempotent_preserves_duplicates_and_rejects_conflicts(self):
        stack, dataset, _, _ = self.runtime()
        with stack:
            stream = self.evaluate(dataset)
            raw = dict(stage_identity("q", self.inputs[0], self.chain.stages[0].stage_id),
                       status="succeeded", total_tokens=13)
            record = dict(batch_id="fixture", raw_stage_id=self.chain.stages[0].stage_id,
                          raw_results=[raw, dict(raw)])
            stream._recover_raw(record, priority=2)
            stream._recover_raw(record, priority=2)
            self.assertEqual(len(stream.unassociated_results), 2)
            record["raw_results"] = [{**raw, "total_tokens": 99}]
            with self.assertRaisesRegex(ValueError, "conflicting recovered raw"):
                stream._recover_raw(record, priority=2)
            stream.close()

    def test_stage_model_role_is_checked_from_configuration(self):
        self.chain = FixtureChain(model_role="review-analysis")
        stack, dataset, _, _ = self.runtime()
        with stack:
            stream = self.evaluate(dataset)
            list(stream)
        self.assertEqual({call["model_role"] for call in stream.calls}, {"review-analysis"})

    def test_metadata_query_version_and_node_validation_precede_graph_allocation(self):
        for condition in ("row_id", "source_position", "query", "version", "nodes", "cancel", "model", "cpu_workers"):
            with self.subTest(condition=condition):
                stack, dataset, events, engines = self.runtime(
                    version="other" if condition == "version" else "2.56.1",
                    nodes=2 if condition == "nodes" else 1)
                expected = self.inputs
                chain = FixtureChain(query_id="other" if condition == "query" else "q",
                                     different_model=condition == "model")
                if condition in {"row_id", "source_position"}:
                    values = self.inputs[1].describe()
                    values[condition] = getattr(self.inputs[0], condition)
                    expected = [self.inputs[0], RecordOccurrence.from_dict(values)]
                with stack, self.assertRaises((ValueError, RuntimeError, InterruptedError)):
                    native.evaluate(dataset, chain, self.spec,
                        RecordsResources(cpu_workers=2) if condition == "cpu_workers" else self.resources, expected=expected,
                        query_id="q", control_dir=self.root.name,
                        cancelled=(lambda: True) if condition == "cancel" else None)
                self.assertEqual((events, engines), ([], []))

    def test_result_integrity_rejects_missing_dependent_call_without_running_a_parser(self):
        stack, dataset, _, _ = self.runtime()
        with stack:
            stream = self.evaluate(dataset)
            identities = [native._source_binding(row) for row in self.inputs]
            with self.assertRaisesRegex(ValueError, "dependent stage"):
                stream._accept(dict(batch_id="fixture", inputs=identities, records=[], calls=[], columns=[], stage_stats={}))
            stream.close()


class FixedProcess(transport.ChainEngineProcess):
    mode = "generation"

    def _command(self, engine, result_fd):
        return [sys.executable, str(Path(__file__).resolve()), "--engine-fixture", str(result_fd), self.mode]


class ChainResponsesProcess(FixedProcess):
    mode = "chain"


class NonObjectProcess(ChainResponsesProcess):
    mode = "chain-nonobject"


class PartialAssociationProcess(FixedProcess):
    mode = "bad-association"


class StalledProcess(ChainResponsesProcess):
    mode = "chain-stall"


class CpuFixtureBatch(native.FusedChainBatch):
    _engine_type = ChainResponsesProcess


class NonObjectBatch(CpuFixtureBatch):
    _engine_type = NonObjectProcess


class StalledBatch(CpuFixtureBatch):
    _engine_type = StalledProcess


class PrivateProcessTests(unittest.TestCase):
    def test_stage_generation_changes_without_rebuilding_owned_model_process(self):
        with tempfile.TemporaryDirectory(prefix="chain-engine-transport-") as root:
            engine = FixedProcess(EngineSpec(root, sys.executable, sys.executable), GenerationSpec(),
                                  root, time.monotonic() + 10, 5)
            try:
                prepared = [dict(stage_identity("q", records()[0], "arbitrary-stage"),
                                 messages_json='[{"role":"user","content":"fixture"}]')]
                first = engine.run_prepared(prepared, generation=GenerationSpec(max_tokens=21))
                second = engine.run_prepared(prepared, generation=GenerationSpec(max_tokens=31, stop=()))
                self.assertEqual((first[0]["max_tokens"], second[0]["max_tokens"]), (21, 31))
                self.assertEqual(first[0]["engine_uid"], second[0]["engine_uid"])
                self.assertNotEqual(first[0]["model_pid"], os.getpid())
                self.assertFalse(first[0]["lotus_imported"])
            finally:
                report = engine.close()
            self.assertEqual(report["status"], "closed")
            self.assertTrue(report["process_exit_confirmed"])
            self.assertIsNone(report["gpu_exit_confirmed"])

    def test_transport_preserves_raw_valid_usage_beside_an_invalid_response_identity(self):
        with tempfile.TemporaryDirectory(prefix="chain-engine-association-") as root:
            engine = PartialAssociationProcess(EngineSpec(root, sys.executable, sys.executable),
                GenerationSpec(), root, time.monotonic() + 10, 5)
            try:
                prepared = [dict(stage_identity("q", row, "arbitrary-stage"),
                    messages_json='[{"role":"user","content":"fixture"}]') for row in records()[:2]]
                result = engine.run_prepared(prepared, generation=GenerationSpec())
                self.assertEqual([row["status"] for row in result], ["succeeded", "succeeded"])
                self.assertEqual(sum(row["total_tokens"] for row in result), 4)
                self.assertEqual(result[0]["call_id"], prepared[0]["call_id"])
                self.assertEqual(result[1]["call_id"], "bad-association-result")
            finally:
                engine.close()


LIBRARIES = all(importlib.util.find_spec(name) is not None for name in ("ray", "pyarrow", "lotus"))
PER_CALL_GENERATION = "response_format" in {item.name for item in fields(GenerationSpec)}


@unittest.skipUnless(LIBRARIES and PER_CALL_GENERATION, "requires pinned Ray/LOTUS and per-call generation extension")
class RealRayCpuTests(unittest.TestCase):
    def setUp(self):
        import ray

        self.assertEqual(ray.__version__, native.RAY_VERSION)
        self.root = tempfile.TemporaryDirectory(prefix="ray-chain-cpu-libraries-")
        self.addCleanup(self.root.cleanup)
        ray.init(num_cpus=2, num_gpus=0, include_dashboard=False, log_to_driver=False)
        self.addCleanup(ray.shutdown)

    def stream(self, batch_type=CpuFixtureBatch, *, cancelled=None, inputs=None, expected=None, chain=None):
        import ray
        from src.semantic_methods.lotus.record_chain import (
            ExtractMethod, LotusFilterMapExtract, MapMethod, StageSpec,
        )
        from src.semantic_methods.lotus.records import FilterMethod

        chain = chain or LotusFilterMapExtract((
            StageSpec("filter-op", FilterMethod("openai/local-fixture"), GenerationSpec()),
            StageSpec("summary-op", MapMethod("openai/local-fixture", "Summarize {reviewText}."),
                      GenerationSpec(max_tokens=37, stop=())),
            StageSpec("extract-op", ExtractMethod("openai/local-fixture", ("summary",),
                      {"aspect": "aspect", "attitude": "attitude"}),
                      GenerationSpec(max_tokens=55, stop=(), response_format={"type": "json_object"})),
        ), query_id="real-ray-cpu")
        inputs = records() if inputs is None else inputs
        expected = inputs if expected is None else expected
        control = Path(self.root.name) / "control"
        control.mkdir()
        (control / "owner").write_text("fixture")
        engine = EngineSpec(self.root.name, sys.executable, sys.executable)
        resources = RecordsResources(model_batch_rows=2, startup_timeout_s=20)
        deadline = time.monotonic() + resources.query_timeout_s
        if inputs:
            dataset = ray.data.from_items([native.record_row(record) for record in inputs])
        else:
            import pyarrow as pa
            dataset = ray.data.from_arrow(pa.table({"record_json": pa.array([], type=pa.string())}))
        graph = native._graph(dataset, chain, engine, resources, expected, control, deadline,
                              "fixture", batch_type=batch_type, model_gpus=0)
        stream = native._RayChainRows(graph, expected, chain, deadline, cancelled, control)
        self.addCleanup(stream.close)
        return stream

    def test_original_lotus_chain_in_real_ray_batches_with_isolated_cpu_responses(self):
        from src.semantic_methods.lotus.record_chain import records_to_frame

        stream = self.stream()
        output = list(stream)
        self.assertEqual([record.row_id for record in output], ["r17", "r19"])
        frame = records_to_frame(output, columns=stream.columns)
        self.assertEqual(frame.index.tolist(), [17, 19])
        self.assertEqual(frame["summary"].tolist(), [SUMMARY, SUMMARY])
        self.assertEqual(frame["aspect"].tolist(), ["摄影、节奏", "摄影、节奏"])
        self.assertEqual(len(stream.calls), 7)
        self.assertEqual(len({row["engine_uid"] for row in stream.calls}), 1)
        self.assertTrue(all(row["model_pid"] != os.getpid() for row in stream.calls))
        self.assertTrue(all(not row["lotus_imported"] for row in stream.calls))
        self.assertEqual({row["max_tokens"] for row in stream.calls}, {8, 37, 55})
        self.assertEqual(stream.last_close_report["status"], "closed")

    def test_map_filter_uses_generated_summary_in_the_same_native_graph(self):
        from src.semantic_methods.lotus.record_chain import (
            LotusRecordChain, MapMethod, StageSpec, TextFilterMethod, records_to_frame,
        )

        chain = LotusRecordChain((
            StageSpec("summary-op", MapMethod("openai/local-fixture", "Summarize {reviewText}."),
                      GenerationSpec(max_tokens=37, stop=())),
            StageSpec("filter-summary-op", TextFilterMethod("openai/local-fixture", "Is {summary} positive?"),
                      GenerationSpec()),
        ), query_id="real-ray-map-filter")
        stream = self.stream(chain=chain)
        output = list(stream)
        frame = records_to_frame(output, columns=stream.columns)
        self.assertEqual(sorted(row.row_id for row in output), ["r17", "r19"])
        self.assertEqual(frame.index.tolist(), [17, 19])
        self.assertEqual(frame["summary"].tolist(), [SUMMARY, SUMMARY])
        self.assertEqual(len(stream.calls), 6)
        self.assertEqual(stream.last_close_report["application_call_upper_bound"], 6)
        self.assertEqual(stream.last_close_report["stage_kinds"], ["map", "filter"])
        self.assertEqual(len({call["engine_uid"] for call in stream.calls}), 1)
        decisions = [call["decision"] for call in stream.calls if call["algorithm_stage"] == "filter-summary-op"]
        self.assertEqual(sorted(decisions), [False, True, True])
        self.assertTrue(all(call["response_format"] is None for call in stream.calls))
        self.assertTrue(stream.completed)

    def test_nonobject_extract_retains_remote_attribute_error_and_successful_model_usage(self):
        stream = self.stream(NonObjectBatch)
        with self.assertRaises(native.RayChainExecutionError) as caught:
            list(stream)
        self.assertEqual(caught.exception.remote_error_type, "AttributeError")
        self.assertFalse(stream.completed)
        self.assertTrue(stream.calls)
        self.assertTrue(all(row["status"] == "succeeded" for row in stream.calls))
        self.assertTrue(all(row["total_tokens"] == 2 for row in stream.calls))
        self.assertEqual(stream.last_close_report["status"], "closed")

    def test_empty_declaration_uses_a_cpu_source_validation_action(self):
        stream = self.stream(inputs=[])
        self.assertEqual(list(stream), [])
        self.assertTrue(stream.completed)
        self.assertEqual(stream.last_close_report["engine_reports"], [])
        stream = self.stream(expected=[])
        with self.assertRaises(Exception):
            list(stream)
        self.assertFalse(stream.completed)
        self.assertEqual(stream.last_close_report["engine_reports"], [])

    def test_cancelled_native_actor_retains_declared_calls_and_reaps_private_process(self):
        cancelled, triggered, finished = threading.Event(), threading.Event(), threading.Event()
        stream = self.stream(StalledBatch, cancelled=cancelled.is_set)

        def cancel_model_call():
            until = time.monotonic() + 30
            while time.monotonic() < until and not finished.wait(0.02):
                for path in stream.control.glob("chain-audit-*.json"):
                    audit = json.loads(path.read_text())
                    if any(call["algorithm_stage"] == "extract-op" and call["status"] == "unknown"
                           for call in audit["calls"]):
                        triggered.set()
                        cancelled.set()
                        return

        watcher = threading.Thread(target=cancel_model_call, daemon=True)
        watcher.start()
        try:
            with self.assertRaises(InterruptedError):
                list(stream)
            self.assertTrue(triggered.is_set())
            self.assertFalse(stream.completed)
            self.assertTrue(any(row["algorithm_stage"] == "extract-op" and row["status"] == "unknown"
                                for row in stream.calls))
            reports = stream.last_close_report["engine_reports"]
            self.assertEqual(len(reports), 1)
            self.assertTrue(reports[0]["process_exit_confirmed"])
            self.assertIsNone(stream.last_close_report["gpu_exit_confirmed"])
        finally:
            finished.set()
            watcher.join(timeout=1)


def _fixture_serve(result_fd, mode):
    from src.serving.backends import vllm_engine

    class CpuEngine:
        def __init__(self, *_):
            pass

        def ready(self):
            return dict(engine_uid="cpu-process-" + str(os.getpid()), pid=os.getpid(), fixture=True)

        def run_prepared(self, rows, *, generation, deadline_monotonic):
            result = []
            for row in rows:
                text = "fixture"
                prompt = json.loads(row["messages_json"])
                if mode.startswith("chain"):
                    if row["algorithm_stage"] == "filter-op":
                        text = "False" if "情节拖沓" in json.dumps(prompt, ensure_ascii=False) else "True"
                    elif row["algorithm_stage"] == "filter-summary-op":
                        content = json.dumps(prompt, ensure_ascii=False)
                        assert SUMMARY in content or NEGATIVE_SUMMARY in content
                        assert REVIEW not in content and "情节拖沓" not in content
                        text = "True" if SUMMARY in content else "False"
                    elif row["algorithm_stage"] == "summary-op":
                        content = json.dumps(prompt, ensure_ascii=False)
                        assert REVIEW in content or "情节拖沓" in content
                        text = NEGATIVE_SUMMARY if "情节拖沓" in content else SUMMARY
                    else:
                        assert SUMMARY in json.dumps(prompt, ensure_ascii=False)
                        assert REVIEW not in json.dumps(prompt, ensure_ascii=False)
                        text = json.dumps({"aspect": "摄影、节奏", "attitude": "积极"}, ensure_ascii=False)
                        if mode == "chain-nonobject":
                            text = "[]"
                        elif mode == "chain-stall":
                            time.sleep(30)
                result.append(dict({key: row[key] for key in IDENTITY_FIELDS}, status="succeeded", text=text,
                         engine_uid="cpu-process-" + str(os.getpid()), model_pid=os.getpid(),
                         engine_request_id="cpu-response-" + row["call_id"], finish_reason="stop",
                         submission_confirmed=True, prompt_token_ids_json="[11]", output_token_ids_json="[12]",
                         max_tokens=generation.max_tokens, lotus_imported="lotus" in sys.modules,
                         response_format=getattr(generation, "response_format", None),
                         prompt_tokens=1, completion_tokens=1, total_tokens=2))
            if mode == "bad-association" and len(result) > 1:
                result[1]["call_id"] = "bad-association-result"
            return result

        def close(self):
            return dict(status="closed", engine_api_shutdown_confirmed=True)

    vllm_engine.VllmEngine = CpuEngine
    transport.serve(result_fd)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--engine-fixture":
        _fixture_serve(int(sys.argv[2]), sys.argv[3])
    else:
        unittest.main()
