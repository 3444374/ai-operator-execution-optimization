"""Daft finite-chain ownership contracts and opt-in zero-GPU library checks."""

from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
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

from src.baselines.text.frameworks import daft_lotus_chain as native
from src.baselines.text.frameworks.lotus_chain_results import _source_binding
from src.semantic_methods.lotus.record_chain import (
    ExtractMethod, LotusRecordChain, LotusRecordMethod, MapMethod, RecordOccurrence, StageSpec,
    records_to_frame, stage_identity,
)
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.baselines.text.daft_chain_fixture import (
    BadExtractBatch, CpuChainBatch, FailCloseBatch, StallBatch, UnknownBatch,
)


def inputs():
    return [RecordOccurrence("occ-" + str(i), i, 9 if i != 1 else 10,
        dict(paragraph="source-negative" if i == 1 else "source-positive",
             topic="new-topic", row_id="BIZ_NEG_ID" if i == 1 else "BIZ_POS_ID",
             call_id="BIZ_NEG_CALL" if i == 1 else "BIZ_POS_CALL")) for i in range(3)]


def chain(shape, query_id):
    model = "openai/local-fixture"
    specs = {
        "F": StageSpec("filter-source", FilterMethod(model, "Keep positive {paragraph}."), GenerationSpec()),
        "A": StageSpec("filter-again", FilterMethod(model, "Keep positive {row_id} and {call_id}."),
                       GenerationSpec(seed=19)),
        "M": StageSpec("map-first", MapMethod(model, "Rewrite {paragraph} about {topic}.", "rewrite"),
                       GenerationSpec(max_tokens=37, stop=(), seed=11), model_role="writer"),
        "N": StageSpec("map-second", MapMethod(model, "Revise {rewrite}.", "second"),
                       GenerationSpec(max_tokens=41, stop=(), temperature=0.2), model_role="editor"),
        "P": StageSpec("filter-generated", FilterMethod(model, "Keep positive {rewrite}."), GenerationSpec()),
        "E": StageSpec("extract-last", ExtractMethod(model, ("rewrite",), {"entity": None, "verdict": None}),
                       GenerationSpec(max_tokens=55, stop=(), response_format={"type": "json_object"})),
    }
    if shape == "MMF":
        specs["M"] = replace(specs["M"], method=replace(specs["M"].method, output_column="row_id"))
        specs["N"] = replace(specs["N"], method=replace(specs["N"].method,
            instruction="Revise {row_id} alongside {call_id}.", output_column="call_id"))
        specs["P"] = replace(specs["P"], method=replace(specs["P"].method,
            instruction="Keep positive {call_id}."))
        sequence = ("M", "N", "P")
    else:
        sequence = {"FME": ("F", "M", "E"), "MF": ("M", "P"),
                    "FFM": ("F", "A", "M")}[shape]
    return LotusRecordChain(tuple(specs[name] for name in sequence), query_id=query_id)


class DaftChainConsumerTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory(prefix="daft-chain-contract-")
        self.addCleanup(self.root.cleanup)
        self.source = inputs()
        self.chain = SimpleNamespace(query_id="q", stages=(SimpleNamespace(
            stage_id="filter-source", kind="filter", model_role="main"),))

    def stream(self, *, error_type=None, cleanup_unknown=False):
        control = Path(self.root.name) / "control"
        control.mkdir()
        (control / "engine-fixture.json").write_text(json.dumps(dict(
            status="unknown" if cleanup_unknown else "closed", process_exit_confirmed=True,
            gpu_exit_confirmed=None, cleanup_errors=["fixture cleanup"] if cleanup_unknown else [])))
        calls = [dict(stage_identity("q", record, "filter-source"), status="succeeded", decision=False,
                      text="False", prompt_tokens=3, completion_tokens=1, total_tokens=4, parent_call_ids=[])
                 for record in self.source]
        envelope = dict(batch_id="one-native-batch", inputs=[_source_binding(row) for row in self.source],
                        calls=calls, records=[], columns=list(self.source[0].values), stage_stats={},
                        raw_results=[], raw_stage_id=None, error_type=error_type, cleanup_errors=[])
        frame = SimpleNamespace(iter_rows=lambda: iter([dict(batch_json=json.dumps(envelope))]))
        return native._DaftChainRows(frame, self.source, self.chain, time.monotonic() + 3, None, control)

    def test_false_terminal_usage_is_independent_from_empty_final_relation(self):
        stream = self.stream()
        self.assertEqual(list(stream), [])
        self.assertTrue(stream.completed)
        self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 12)
        self.assertEqual(stream.last_close_report["scheduler_owner"], "Daft Native 0.7.21")
        self.assertNotIn("executor_shutdown_confirmed", stream.last_close_report)
        self.assertIs(stream.close(), stream.last_close_report)
        self.assertEqual(list(stream), [])

    def test_parser_failure_preserves_succeeded_calls_and_primary_error(self):
        stream = self.stream(error_type="FixtureParserError")
        with self.assertRaises(native.DaftChainExecutionError) as caught:
            list(stream)
        self.assertEqual(caught.exception.remote_error_type, "FixtureParserError")
        self.assertEqual(len(caught.exception.chain_calls), 3)
        self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 12)
        self.assertFalse(stream.completed)

    def test_cleanup_unknown_retains_calls_and_can_retry_without_changing_usage(self):
        stream = self.stream(cleanup_unknown=True)
        with self.assertRaisesRegex(RuntimeError, "cleanup remains unconfirmed"):
            list(stream)
        self.assertEqual(stream.last_close_report["status"], "unknown")
        self.assertTrue(stream.control.exists())
        before = deepcopy(stream.calls)
        (stream.control / "engine-fixture.json").write_text(json.dumps(dict(
            status="closed", process_exit_confirmed=True, cleanup_errors=[])))
        report = stream.close()
        self.assertEqual(report["status"], "closed")
        self.assertIsNone(report["gpu_exit_confirmed"])
        self.assertEqual(stream.calls, before)

    def test_forged_call_identity_is_rejected_without_rewriting_known_prefix(self):
        stream = self.stream()
        calls = [dict(stage_identity("q", record, "filter-source"), status="succeeded",
                      prompt_tokens=3, completion_tokens=1, total_tokens=4) for record in self.source]
        calls[1]["call_id"] = "business-call-is-not-evaluation-id"
        with self.assertRaisesRegex(ValueError, "evaluation identity"):
            stream._record_calls(calls)
        self.assertEqual(len(stream.calls), 1)
        stream.close()


class DaftRunnerTests(unittest.TestCase):
    def runtime(self, name):
        from src.infrastructure import daft_runtime

        runners = ModuleType("daft.runners")
        runners._get_runner = lambda: SimpleNamespace(name=name)
        runners.get_or_create_runner = runners._get_runner
        return daft_runtime, runners

    def test_existing_native_input_is_reused_without_resetting_the_vendor_runner(self):
        owner, runners = self.runtime("native")
        with patch.dict(sys.modules, {"daft": ModuleType("daft"), "daft.runners": runners}), \
             patch.object(owner, "_configured", None), patch.object(owner, "configure_daft_runner") as setup:
            native._native_runner(None)
            setup.assert_not_called()

    def test_foreign_runner_and_unapplied_thread_change_are_rejected(self):
        for selected, threads in (("ray", None), ("native", 3)):
            with self.subTest(selected=selected, threads=threads):
                owner, runners = self.runtime(selected)
                with patch.dict(sys.modules, {"daft": ModuleType("daft"), "daft.runners": runners}), \
                     patch.object(owner, "_configured", None), patch.object(owner, "configure_daft_runner") as setup:
                    with self.assertRaises(RuntimeError):
                        native._native_runner(threads)
                    setup.assert_not_called()


LIBRARIES = all(importlib.util.find_spec(name) for name in ("daft", "lotus", "psutil"))


@unittest.skipUnless(LIBRARIES and os.environ.get("RUN_LOTUS_RECORDS_CPU_LIBRARIES") == "1",
                     "explicit pinned Daft/LOTUS CPU qualification required")
class RealDaftChainCpuTests(unittest.TestCase):
    def setUp(self):
        import daft
        import lotus

        self.assertEqual(daft.__version__, native.DAFT_VERSION)
        self.scope = lotus.settings.context(enable_cache=False)
        self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)
        self.root = tempfile.TemporaryDirectory(prefix="daft-chain-cpu-")
        self.addCleanup(self.root.cleanup)
        self.engine = EngineSpec(self.root.name, sys.executable, sys.executable, max_model_calls=16)

    def save(self, name, value):
        destination = os.environ.get("RECORDS_CHAIN_CPU_ARTIFACTS")
        if destination:
            from src.baselines.common.private_artifacts import write_private_json
            from src.baselines.common.redact import redact_json_values
            write_private_json(Path(destination) / name, redact_json_values(value))

    def collect(self, stream, name, output=None):
        output = [] if output is None else output
        try:
            for record in stream:
                output.append(record)
            return output
        finally:
            self.save(name + "-raw.json", dict(output=[record.describe() for record in output],
                calls=stream.calls, close=stream.last_close_report, actual_model_calls=0,
                logical_gpus=0, usage_source="synthetic CPU fixture"))

    def stream(self, method, *, source=None, kernel=CpuChainBatch, expected=None, cancelled=None):
        import daft
        import pyarrow as pa

        source = inputs() if source is None else source
        expected = source if expected is None else expected
        if source:
            frame = daft.from_pylist([native.record_row(record) for record in source])
        else:
            frame = daft.from_arrow(pa.table({"record_json": pa.array([], type=pa.string())}))
        graph = native._graph
        with patch.object(EngineSpec, "validate_driver_environment", return_value={"CPU fixture": True}), \
             patch.object(native, "_graph", side_effect=lambda *args, **kwargs:
                          graph(*args, **dict(kwargs, batch_type=kernel, model_gpus=0))):
            return native.evaluate(frame, method, self.engine,
                RecordsResources(prepare_batch_rows=1, model_batch_rows=2,
                                 query_timeout_s=70, startup_timeout_s=30),
                expected=expected, query_id=method.query_id, cancelled=cancelled,
                control_dir=self.root.name)

    def reference(self, method, source, stream, output):
        import lotus
        import pandas as pd
        from src.semantic_methods.lotus.engine import engine_response, lm_report

        frame = pd.DataFrame([record.values for record in source], index=[record.source_index for record in source])
        proofs = []
        for stage in method.stages:
            work = LotusRecordMethod(stage)
            calls = sorted([call for call in stream.calls if call["algorithm_stage"] == stage.stage_id],
                           key=lambda row: row["source_position"])
            captured = []

            def respond(messages, options, *_args):
                self.assertEqual(len(messages), len(calls))
                captured.extend(message for message, _ in messages)
                self.assertEqual(options["max_completion_tokens"], stage.generation.max_tokens)
                return [engine_response(call, work.lm) for call in calls]

            work.lm._process_uncached_messages = respond
            if not frame.empty:
                with lotus.settings.context(lm=work.lm):
                    if stage.kind == "filter":
                        frame = frame.sem_filter(stage.method.instruction, default=stage.method.default,
                            output_tokens=stage.method.output_tokens, system_prompt=stage.method.system_prompt)
                    elif stage.kind == "map":
                        frame = frame.sem_map(stage.method.instruction, suffix=stage.method.output_column,
                                              system_prompt=stage.method.system_prompt)
                    else:
                        frame = frame.sem_extract(list(stage.method.input_columns), stage.method.output_columns,
                                                  extract_quotes=stage.method.extract_quotes)
            hashes = [hashlib.sha256(json.dumps(messages, ensure_ascii=False, sort_keys=True,
                                                separators=(",", ":")).encode()).hexdigest()
                      for messages in captured]
            self.assertEqual([call["messages_sha256"] for call in calls], hashes)
            for call in calls:
                self.assertEqual(call["max_tokens"], stage.generation.max_tokens)
                self.assertEqual(call["temperature"], stage.generation.temperature)
                self.assertEqual(call["seed"], stage.generation.seed)
                self.assertEqual(call["response_format"], stage.generation.response_format)
                self.assertEqual(call["model_role"], stage.model_role)
            if calls:
                snapshots = [snapshot[stage.stage_id]["stats"] for snapshot in stream.batch_stage_stats
                             if stage.stage_id in snapshot]
                native_stats = lm_report(work.lm)["stats"]
                self.assertIn(native_stats, snapshots)
            proofs.append(dict(stage_id=stage.stage_id, kind=stage.kind, calls=len(calls),
                               actual_messages_equal=True, generation_equal=True, lm_stats_equal=True))
        if source:
            pd.testing.assert_frame_equal(records_to_frame(output, columns=stream.columns), frame)
        self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 4 * len(stream.calls))
        return proofs

    def test_fme_mf_mmf_ffm_share_the_same_native_kernel_and_original_methods(self):
        for shape in ("FME", "MF", "MMF", "FFM"):
            with self.subTest(shape=shape):
                method = chain(shape, "daft-" + shape)
                source = inputs()
                stream = self.stream(method, source=source)
                output = self.collect(stream, "daft-" + shape)
                self.assertTrue(stream.completed)
                self.assertEqual([record.row_id for record in output], ["occ-0", "occ-2"])
                self.assertEqual([record.source_index for record in output], [9, 9])
                self.assertEqual(len(stream.calls), {"FME": 7, "MF": 6, "MMF": 9, "FFM": 7}[shape])
                self.assertEqual(len({call["engine_uid"] for call in stream.calls}), 1)
                self.assertTrue(all(not call["lotus_imported"] for call in stream.calls))
                self.assertEqual(stream.last_close_report["received_native_batch_envelopes"], 2)
                before = deepcopy(stream.calls)
                self.assertEqual(list(stream), [])
                self.assertIs(stream.close(), stream.last_close_report)
                self.assertEqual(stream.calls, before)
                proofs = self.reference(method, source, stream, output)
                self.save("daft-" + shape + ".json", dict(status="passed", shape=shape,
                    output=[record.describe() for record in output], calls=stream.calls,
                    proofs=proofs, stage_stats=stream.batch_stage_stats, close=stream.last_close_report,
                    actual_model_calls=0, logical_gpus=0, usage_source="synthetic CPU fixture"))

    def test_empty_source_and_all_false_keep_coverage_and_false_usage(self):
        method = chain("FME", "daft-empty")
        stream = self.stream(method, source=[])
        self.assertEqual(self.collect(stream, "daft-empty"), [])
        self.assertTrue(stream.completed)
        self.assertEqual(stream.calls, [])
        self.assertEqual(stream.last_close_report["engine_reports"], [])
        self.save("daft-empty.json", dict(calls=[], close=stream.last_close_report, actual_model_calls=0))
        source = [inputs()[1]]
        stream = self.stream(chain("FME", "daft-false"), source=source)
        self.assertEqual(self.collect(stream, "daft-false"), [])
        self.assertTrue(stream.completed)
        self.assertEqual(len(stream.calls), 1)
        self.assertFalse(stream.calls[0]["decision"])
        self.assertEqual(stream.calls[0]["total_tokens"], 4)
        self.save("daft-false.json", dict(calls=stream.calls, columns=stream.columns,
                                          close=stream.last_close_report, actual_model_calls=0))

    def test_parser_failure_keeps_true_model_status_and_usage(self):
        stream = self.stream(chain("FME", "daft-bad-extract"), kernel=BadExtractBatch)
        with self.assertRaises(native.DaftChainExecutionError) as caught:
            self.collect(stream, "daft-parser-error")
        self.assertEqual(caught.exception.remote_error_type, "AttributeError")
        self.assertTrue(stream.calls)
        self.assertTrue(all(call["status"] == "succeeded" for call in stream.calls))
        self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 4 * len(stream.calls))
        self.assertFalse(stream.completed)
        self.save("daft-parser-error.json", dict(calls=stream.calls, close=stream.last_close_report,
                                                 actual_model_calls=0))

    def test_unknown_later_stage_retains_known_prefix_without_zero_usage(self):
        stream = self.stream(chain("MMF", "daft-unknown"), kernel=UnknownBatch)
        with self.assertRaises(native.DaftChainExecutionError):
            self.collect(stream, "daft-unknown")
        known = [call for call in stream.calls if call["status"] == "succeeded"]
        unknown = [call for call in stream.calls if call["status"] == "unknown"]
        self.assertTrue(known and unknown)
        self.assertTrue(all(call["total_tokens"] == 4 for call in known))
        self.assertTrue(all(call["total_tokens"] is None for call in unknown))
        self.assertFalse(any(call["algorithm_stage"] == "filter-generated" for call in stream.calls))
        self.save("daft-unknown.json", dict(calls=stream.calls, close=stream.last_close_report,
                                           actual_model_calls=0))

    def test_last_stage_close_failure_does_not_erase_any_terminal_usage(self):
        stream = self.stream(chain("FME", "daft-close-unknown"), kernel=FailCloseBatch)
        output = []
        with self.assertRaisesRegex(RuntimeError, "cleanup remains unconfirmed"):
            self.collect(stream, "daft-close-unknown", output)
        self.assertEqual(len(output), 2)
        self.assertEqual(len(stream.calls), 7)
        self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 28)
        self.assertEqual(stream.last_close_report["status"], "unknown")
        self.assertTrue(all(report["process_exit_confirmed"] for report in stream.last_close_report["engine_reports"]))
        self.assertIsNone(stream.last_close_report["gpu_exit_confirmed"])
        before = deepcopy(stream.calls)
        self.assertEqual(stream.close()["status"], "unknown")
        self.assertEqual(stream.calls, before)
        self.save("daft-close-unknown.json", dict(calls=stream.calls, close=stream.last_close_report,
            audits={path.name: json.loads(path.read_text()) for path in stream.control.glob("*.json")},
            actual_model_calls=0))

    def test_cancelled_native_batch_recovers_prefix_and_reaps_registered_process(self):
        cancelled, finished, triggered = threading.Event(), threading.Event(), threading.Event()
        stream = self.stream(chain("MMF", "daft-cancel"), kernel=StallBatch, cancelled=cancelled.is_set)

        def cancel():
            until = time.monotonic() + 45
            while time.monotonic() < until and not finished.wait(0.02):
                for path in stream.control.glob("chain-audit-*.json"):
                    report = json.loads(path.read_text())
                    if any(call["algorithm_stage"] == "map-second" and call["status"] == "unknown"
                           for call in report["calls"]):
                        triggered.set()
                        cancelled.set()
                        return

        watcher = threading.Thread(target=cancel, daemon=True)
        watcher.start()
        try:
            with self.assertRaises(InterruptedError):
                self.collect(stream, "daft-cancel")
            self.assertTrue(triggered.is_set())
            self.assertTrue(any(call["status"] == "succeeded" for call in stream.calls))
            self.assertTrue(any(call["status"] == "unknown" for call in stream.calls))
            self.assertTrue(all(report["process_exit_confirmed"] for report in stream.last_close_report["engine_reports"]))
            self.assertIsNone(stream.last_close_report["gpu_exit_confirmed"])
            audits = {path.name: json.loads(path.read_text()) for path in stream.control.glob("*.json")}
            self.save("daft-cancel.json", dict(calls=stream.calls, close=stream.last_close_report,
                audits=audits, actual_model_calls=0, physical_gpu_exit_checked=False))
        finally:
            finished.set()
            watcher.join(timeout=1)

    def test_preflight_rejects_other_deployment_and_nonterminal_extract(self):
        ordinary = chain("FME", "preflight")
        stages = list(ordinary.stages)
        stages[1] = replace(stages[1], method=replace(stages[1].method, model_name="openai/other-fixture"))
        with patch.object(native, "_graph") as graph:
            with self.assertRaisesRegex(ValueError, "one model deployment"):
                native.evaluate(None, LotusRecordChain(tuple(stages), query_id="preflight"),
                    self.engine, RecordsResources(), expected=inputs(), query_id="preflight")
            graph.assert_not_called()
        with self.assertRaisesRegex(ValueError, "Extract must be terminal"):
            LotusRecordChain((ordinary.stages[2], ordinary.stages[0]), query_id="bad-extract-order")


if __name__ == "__main__":
    unittest.main()
