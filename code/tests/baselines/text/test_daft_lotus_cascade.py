"""Stage contracts and opt-in real Daft/LOTUS CPU cascade qualification."""

from copy import deepcopy
from dataclasses import replace
from importlib import import_module
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.frameworks import daft_lotus_cascade as native
from src.semantic_methods.lotus.cascade import CascadeStage, IDENTITY_FIELDS, cascade_identity, run_cascade
from src.semantic_methods.lotus.records import FilterMethod, MOVIE_Q3_INSTRUCTION, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from tests.baselines.text.daft_cascade_fixture import (
    CascadeCsvFixture, CpuCascadeEngineProcess, FailedCloseProcess, FailedRowProcess, response,
)


class StageContractTests(unittest.TestCase):
    def test_repeated_draws_compare_every_declared_input_and_keep_independent_calls(self):
        row = dict(row_id="same", source_position=2, paragraph="same text", hint="same hint")
        calls = [dict(cascade_identity("q", "same", 2, i, model_role="main", stage="threshold_sample"),
                      paragraph=row["paragraph"], hint=row["hint"]) for i in (10, 11)]
        selected = {("same", 2): calls}
        self.assertEqual(list(native._selected(row, selected=selected, input_columns=("paragraph", "hint"))), calls)
        self.assertEqual(list(native._selected(dict(row, source_position=3), selected=selected,
                                              input_columns=("paragraph", "hint"))), [])
        with self.assertRaisesRegex(ValueError, "source text"):
            list(native._selected(dict(row, hint="changed"), selected=selected,
                                  input_columns=("paragraph", "hint")))

    def test_unknown_cleanup_blocks_role_switch_and_retains_usage_receipts(self):
        stage = SimpleNamespace(name="helper_score", model_role="helper")
        reports = []
        report = dict(status="closed", cleanup_errors=[], unconsumed_call_results=[dict(prompt_tokens=12)],
            engine_reports=[dict(process_exit_confirmed=True, cleanup_errors=[],
                                 engine_api=dict(engine_api_shutdown_confirmed=False))])
        rows = native._StageRows(SimpleNamespace(close=lambda: deepcopy(report)), stage, reports)
        with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
            rows.close()
        self.assertFalse(reports[0]["stage_process_exit_confirmed"])
        self.assertEqual(reports[0]["unconsumed_call_results"], [dict(prompt_tokens=12)])
        report["engine_reports"][0]["engine_api"]["engine_api_shutdown_confirmed"] = True
        rows.close()
        self.assertEqual(len(reports), 1)
        self.assertTrue(reports[0]["stage_process_exit_confirmed"])

    def test_declared_deployments_and_limits_are_checked_before_allocation(self):
        engine = EngineSpec("/tmp/main", sys.executable, sys.executable)
        with self.assertRaisesRegex(ValueError, "distinct"):
            native.DaftCascadeBackend(None, engines=dict(main=engine, helper=engine), resources=RecordsResources())
        with self.assertRaisesRegex(ValueError, "runner_threads"):
            native.DaftCascadeBackend(None, engines=dict(main=engine, helper=replace(engine, model_path="/tmp/helper")),
                                      resources=RecordsResources(), runner_threads=0)

    def test_failed_native_graph_recovers_verified_cascade_receipts_and_keeps_unknowns(self):
        records = tuple(dict(cascade_identity("q", "occ-" + str(i), i, i, model_role="helper", stage="helper_score"),
                             paragraph="same input") for i in range(3))
        expected = [{name: row[name] for name in IDENTITY_FIELDS} for row in records]
        stage = CascadeStage("helper_score", "helper", "fixture", "Keep paragraph", True, None,
                             ("True", "False"), dict(logprobs=True), records, ("paragraph",))
        terminal = dict(response(records[0], "helper"), engine_protocol_error="damaged result envelope")
        report = dict(status="unknown", process_exit_confirmed=True, cleanup_errors=[],
            engine_ready=dict(engine_uid=terminal["engine_uid"]), result_failure=dict(verified_call_results=[terminal]))
        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory)
            (control / "engine-fixture.json").write_text(json.dumps(report))
            rows = native._CascadeDaftRows(None, expected, time.monotonic() + 30, None, control, stage=stage)
            unknown = dict(expected[1], status="unknown", submission_confirmed=False,
                           prompt_tokens=None, completion_tokens=None)
            rows.received.add(unknown["call_id"])
            closed = rows.close()
            self.assertEqual(closed["unconsumed_call_results"], [terminal])
            self.assertFalse(any("result_receipt_recovery" in item for item in closed["cleanup_errors"]))
            self.assertNotIn(unknown["call_id"], rows._result_receipts)

    def test_invalid_or_conflicting_receipts_cannot_replace_a_known_terminal(self):
        record = dict(cascade_identity("q", "occ", 0, 0, model_role="main", stage="threshold_sample"), paragraph="text")
        stage = CascadeStage("threshold_sample", "main", "fixture", "Keep paragraph", True, None,
                             ("True", "False"), dict(logprobs=False), (record,), ("paragraph",))
        terminal = dict(response(record, "main"), engine_protocol_error="damaged result envelope")
        with tempfile.TemporaryDirectory() as directory:
            rows = native._CascadeDaftRows(None, [{name: record[name] for name in IDENTITY_FIELDS}],
                                          time.monotonic() + 30, None, directory, stage=stage)
            report = dict(engine_ready=dict(engine_uid=terminal["engine_uid"]),
                          result_failure=dict(verified_call_results=[terminal]))
            self.assertEqual(rows._recover_result_receipts(report), [])
            changed = dict(terminal, prompt_tokens=99)
            self.assertEqual(rows._recover_result_receipts(dict(report,
                result_failure=dict(verified_call_results=[changed]))), ["result_receipt_recovery: ValueError"])
            self.assertEqual(rows._result_receipts[terminal["call_id"]]["prompt_tokens"], 12)
            wrong = dict(terminal, engine_uid="another-engine")
            self.assertEqual(rows._recover_result_receipts(dict(report,
                result_failure=dict(verified_call_results=[wrong]))), ["result_receipt_recovery: ValueError"])

    def test_stop_write_failure_still_cleans_registered_process_and_recovers_known_usage(self):
        record = dict(cascade_identity("q", "occ", 0, 0, model_role="helper", stage="helper_score"), paragraph="text")
        stage = CascadeStage("helper_score", "helper", "fixture", "Keep paragraph", True, None,
                             ("True", "False"), dict(logprobs=True), (record,), ("paragraph",))
        terminal = dict(response(record, "helper"), engine_protocol_error="damaged result envelope")
        initial = dict(status="starting", model_pid=12345, process_created=1.0, process_exit_confirmed=False,
                       engine_ready=dict(engine_uid=terminal["engine_uid"]), cleanup_errors=[],
                       result_failure=dict(verified_call_results=[terminal]))
        final = dict(initial, status="unknown", process_exit_confirmed=True)
        events = []
        with tempfile.TemporaryDirectory() as directory:
            rows = native._CascadeDaftRows(None, [{name: record[name] for name in IDENTITY_FIELDS}],
                                          time.monotonic() + 30, None, directory, stage=stage)
            rows._started = True
            rows.iterator = SimpleNamespace(close=Mock(side_effect=lambda: events.append("iterator_close")))
            rows.engine_cleanup._read = Mock(return_value=[initial])
            rows.engine_cleanup._stop_registered = Mock(side_effect=lambda report: (
                events.append("registered_process_cleanup") or final))
            original_stop = rows.engine_cleanup.stop
            stream = native._StageRows(rows, stage, [])
            with patch.object(Path, "touch", side_effect=PermissionError("declared stop-file failure")), \
                 patch.object(rows.engine_cleanup, "stop", side_effect=lambda **options: original_stop(grace_s=0, **options)):
                with self.assertRaisesRegex(PermissionError, "declared stop-file failure"):
                    stream.close()
            rows.engine_cleanup._stop_registered.assert_called_once_with(initial)
            rows.iterator.close.assert_called_once()
            self.assertEqual(events, ["registered_process_cleanup", "iterator_close"])
            report = stream.last_close_report
            self.assertEqual(report["status"], "unknown")
            self.assertFalse(report["stage_process_exit_confirmed"])
            self.assertEqual(report["unconsumed_call_results"], [terminal])
            self.assertIn("native_close: PermissionError", report["cleanup_errors"])
            self.assertIn("stop_request: PermissionError", report["cleanup_errors"])
            self.assertTrue(report["registered_engine_cleanup"]["process_exit_confirmed"])

    def test_registered_process_cleanup_still_signals_when_tree_observation_is_denied(self):
        import psutil
        from src.experiments import lotus_records_supervisor as supervisor

        report = dict(model_pid=12345, process_created=1.0, process_exit_confirmed=False)
        process = Mock()
        process.create_time.return_value = 1.0
        with patch.object(psutil, "Process", return_value=process), \
             patch.object(supervisor, "_observe", side_effect=PermissionError("process tree")), \
             patch.object(supervisor, "_live", side_effect=[[process], [], []]), \
             patch.object(supervisor, "_signal") as send, patch.object(supervisor, "_wait"):
            cleaned = native._CascadeDaftRows._stop_registered_engine(report)
        send.assert_called_once_with({(12345, 1.0)}, 12345, 1.0, signal.SIGTERM)
        self.assertEqual(cleaned["status"], "unknown")
        self.assertTrue(cleaned["process_exit_confirmed"])
        self.assertIn("process_tree_observation: PermissionError", cleaned["cleanup_errors"])
        self.assertFalse(cleaned["unobserved_descendants_confirmed"])

    def test_cancel_watchdog_cleans_registered_process_when_stop_write_fails_and_driver_is_blocked(self):
        record = dict(cascade_identity("q", "occ", 0, 0, model_role="helper", stage="helper_score"), paragraph="text")
        stage = CascadeStage("helper_score", "helper", "fixture", "Keep paragraph", True, None,
                             ("True", "False"), dict(logprobs=True), (record,), ("paragraph",))
        terminal = dict(response(record, "helper"), engine_protocol_error="damaged result envelope")
        engine = dict(status="unknown", process_exit_confirmed=True, cleanup_errors=[],
                      engine_ready=dict(engine_uid=terminal["engine_uid"]),
                      result_failure=dict(verified_call_results=[terminal]))
        cancelled, blocked, release, cleaned = (threading.Event() for _ in range(4))
        errors = []

        class BlockedNativeIterator:
            def __iter__(self):
                return self
            def __next__(self):
                blocked.set()
                release.wait(timeout=3)
                raise StopIteration
            close = Mock()

        iterator = BlockedNativeIterator()
        with tempfile.TemporaryDirectory() as directory:
            rows = native._CascadeDaftRows(SimpleNamespace(iter_rows=lambda: iterator),
                [{name: record[name] for name in IDENTITY_FIELDS}], time.monotonic() + 30,
                cancelled.is_set, directory, stage=stage)
            cleanup_report = dict(reports=[engine], errors=["stop_request: PermissionError"], process_exit_confirmed=True)
            def cleanup(**_):
                cleaned.set()
                return deepcopy(cleanup_report)
            rows.engine_cleanup.stop = Mock(side_effect=cleanup)
            stream = native._StageRows(rows, stage, [])
            def consume():
                try:
                    next(stream)
                except BaseException as error:
                    errors.append(error)
                finally:
                    try:
                        stream.close()
                    except BaseException:
                        pass
            consumer = threading.Thread(target=consume, daemon=True)
            with patch.object(Path, "touch", side_effect=PermissionError("declared cancel-stop failure")):
                consumer.start()
                try:
                    self.assertTrue(blocked.wait(timeout=1))
                    cancelled.set()
                    self.assertTrue(cleaned.wait(timeout=1), "watchdog must clean before blocked driver returns")
                    self.assertTrue(consumer.is_alive())
                    self.assertIsInstance(rows._failure, InterruptedError)
                finally:
                    release.set()
                    consumer.join(timeout=3)
                    rows._stop.set()
            self.assertFalse(consumer.is_alive())
            self.assertTrue(rows.engine_cleanup.stop.called)
            self.assertIsInstance(errors[0], InterruptedError)
            self.assertEqual(rows._result_receipts[terminal["call_id"]]["prompt_tokens"], 12)
            self.assertEqual(stream.last_close_report["unconsumed_call_results"], [terminal])
            self.assertEqual(stream.last_close_report["status"], "unknown")
            self.assertFalse(stream.last_close_report["stage_process_exit_confirmed"])


@unittest.skipUnless(os.environ.get("RUN_LOTUS_CASCADE_DAFT_CPU") == "1",
                     "explicit real Daft/LOTUS CPU graph qualification")
class RealDaftCascadeTests(unittest.TestCase):
    def setUp(self):
        import daft
        import lotus
        from lotus.types import SerializationFormat

        self.assertEqual(daft.__version__, "0.7.21")
        self.scope = lotus.settings.context(enable_cache=False, serialization_format=SerializationFormat.DEFAULT)
        self.scope.__enter__()
        self.addCleanup(lambda: self.scope.__exit__(None, None, None))
        self.root = tempfile.TemporaryDirectory(prefix="daft-cascade-cpu-")
        self.addCleanup(self.root.cleanup)
        self.resources = RecordsResources(prepare_batch_rows=2, model_batch_rows=3,
                                          query_timeout_s=120, startup_timeout_s=20)
        self.results, self.backends = [], []

    def models(self):
        from lotus.cache import InMemoryCache
        from lotus.models import LM
        from tokenizers import Tokenizer, models as token_models

        tokenizer = Tokenizer(token_models.WordLevel({"[UNK]": 0, "True": 1, "False": 2}, unk_token="[UNK]"))
        engines, models = {}, {}
        for role in ("main", "helper"):
            directory = Path(self.root.name) / role
            directory.mkdir(exist_ok=True)
            tokenizer.save(str(directory / "tokenizer.json"))
            engines[role] = EngineSpec(str(directory), sys.executable, sys.executable, max_model_calls=30)
            models[role] = LM("openai/daft-fixture-" + role, tokenizer=tokenizer,
                              cache=InMemoryCache(max_size=2), max_tokens=9, temperature=0)
        return models["main"], models["helper"], engines

    def source(self, main, instruction, *, helper_column=None, empty=False, duplicate_text=False):
        from src.semantic_methods.lotus.engine import bind_filter_method

        method = bind_filter_method(FilterMethod(main.model, instruction))
        columns = list(method.resolved_input_columns)
        if helper_column is not None and helper_column not in columns:
            columns.append(helper_column)
        rows = [dict(row_id="occ-" + str(i), source_position=i, selected="yes",
                     **{column: "native-position=" + str(i) + "; " + column for column in columns})
                for i in range(10)]
        if duplicate_text:
            for column in columns:
                rows[9][column] = rows[3][column]
        if empty:
            rows = []
        rows.append(dict(row_id="excluded", source_position=10, selected="no",
                         **{column: "must not reach preparation" for column in columns}))
        source = CascadeCsvFixture(self.root.name, method, payload_columns=columns, rows=rows)
        import pandas as pd

        frame = pd.DataFrame(source.selected(), columns=["row_id", "source_position", "selected", *columns])
        return source, frame.drop(columns=["selected"]), method

    def execute(self, frame, main, helper, args, source, engines, instruction,
                *, kernel=CpuCascadeEngineProcess):
        graph = native._stage_graph
        results = self.results

        def cpu_graph(*values, **options):
            return graph(*values, **dict(options, engine_type=kernel, model_gpus=0))

        class Observe(native.DaftCascadeBackend):
            def __call__(self, stage):
                stream = super().__call__(stage)
                if not stage.records:
                    return stream
                class Observed:
                    def __iter__(self):
                        return self
                    def __next__(self):
                        row = next(stream)
                        results.append(row)
                        return row
                    def close(self):
                        return stream.close()
                    @property
                    def last_close_report(self):
                        return stream.last_close_report
                return Observed()

        backend = Observe(source, engines=engines, resources=self.resources)
        self.backends.append(backend)
        with patch.object(EngineSpec, "validate_driver_environment", return_value={}), \
             patch.object(native, "_stage_graph", side_effect=cpu_graph):
            report = run_cascade(frame, main, helper, args, backend,
                instruction=instruction, query_id="real-daft-cpu", max_rows=10, max_calls=25)
        return report, backend

    def args(self, **options):
        from lotus.types import CascadeArgs

        return CascadeArgs(sampling_percentage=0.5, cascade_IS_random_seed=5,
                           cascade_num_calibration_quantiles=5, **options)

    def reference(self, frame, instruction, args):
        """Execute the installed original accessor, replacing only its LM responses."""
        import lotus
        from litellm.types.utils import ModelResponse

        module = import_module("lotus.sem_ops.sem_filter")
        main, helper, _ = self.models()
        sampled, native_calls = [], []
        cursors = dict(main=0)
        original_sample = module.importance_sampling

        def sample(scores, settings):
            indices, factors = original_sample(scores, settings)
            sampled.extend(indices.tolist())
            return indices, factors

        for role, lm in (("main", main), ("helper", helper)):
            def fixed(data, kwargs, *_, _role=role, _lm=lm):
                values = []
                stage = "helper_score" if _role == "helper" else (
                    "threshold_sample" if cursors["main"] == 0
                    and args.filter_pos_cascade_threshold is None else "fallback")
                if _role == "main":
                    cursors["main"] += 1
                for messages, _ in data:
                    position = int(re.search(r"native-position=(\d+);", json.dumps(messages)).group(1))
                    native_calls.append((stage, messages))
                    row = response(dict(cascade_identity("original", "occ-" + str(position), position,
                        len(native_calls), model_role=_role, stage=stage), messages_json=json.dumps(messages)), _role)
                    values.append(ModelResponse(id=row["call_id"], model=_lm.model,
                        choices=[dict(index=0, finish_reason="stop", message=dict(role="assistant", content=row["text"]),
                                      logprobs=dict(content=json.loads(row["logprobs_json"])) if kwargs.get("logprobs") else None)],
                        usage={name: row[name] for name in ("prompt_tokens", "completion_tokens", "total_tokens")}))
                return values
            lm._process_uncached_messages = fixed
        with lotus.settings.context(lm=main, helper_lm=helper, enable_cache=False), \
             patch.object(module, "importance_sampling", sample):
            result, stats = frame.sem_filter(instruction, cascade_args=args, return_all=True,
                return_raw_outputs=True, return_explanations=True, return_stats=True)
        return result, stats, sampled, native_calls

    def test_non_q3_different_helper_column_matches_original_global_controller(self):
        instruction = "Keep the useful paragraph: {paragraph}"
        main, helper, engines = self.models()
        source, frame, _ = self.source(main, instruction, helper_column="hint")
        args = self.args(helper_filter_instruction="Check the useful hint: {hint}")
        expected, stats, sampled, calls = self.reference(frame, instruction, args)
        report, backend = self.execute(frame, main, helper, args, source, engines, instruction)
        self.assertEqual(report["status"], "completed")
        self.assertEqual([item["decision"] for item in report["decisions"]], expected["_filter"].tolist())
        self.assertEqual(report["stats"], {key: value.model_dump(mode="json") if hasattr(value, "model_dump") else value
                                           for key, value in stats.items()})
        selected = next(item for item in report["calls"]["events"] if item["event"] == "sample_selected")
        self.assertEqual(selected["indices"], sampled)
        self.assertLess(len(set(sampled)), len(sampled))
        ordered = sorted(self.results, key=lambda row: row["call_index"])
        self.assertEqual([json.loads(row["messages_json"]) for row in ordered], [messages for _, messages in calls])
        self.assertEqual([item["name"] for item in report["calls"]["stages"]],
                         ["helper_score", "threshold_sample", "fallback"])
        sample_rows = [row for row in ordered if row["algorithm_stage"] == "threshold_sample"]
        self.assertEqual(len(sample_rows), 5)
        self.assertEqual(len({row["call_id"] for row in sample_rows}), 5)
        self.assertLess(len({row["source_position"] for row in sample_rows}), 5)
        self.assertEqual(len({row["call_id"] for row in ordered}), len(ordered))
        self.assertEqual(report["calls"]["usage"]["total_tokens"], len(ordered) * 13)
        self.assertEqual(len(backend.close_reports), 3)
        self.assertTrue(all(item["stage_process_exit_confirmed"] for item in backend.close_reports))
        self.assertTrue(all(item["scheduler_owner"] == "Daft Native" for item in backend.close_reports))
        self.assertTrue(all(row["prepare_worker_pid"] != os.getpid() and row["infer_worker_pid"] != os.getpid()
                            and row["model_fixture_pid"] != row["prepare_worker_pid"] for row in ordered))
        self.assertEqual(len({row["engine_uid"] for row in ordered}), 3)
        self.assertNotIn("excluded", {row["row_id"] for row in ordered})

    def test_q3_instruction_regression_matches_original(self):
        main, helper, engines = self.models()
        source, frame, _ = self.source(main, MOVIE_Q3_INSTRUCTION)
        args = self.args(filter_pos_cascade_threshold=0.8, filter_neg_cascade_threshold=0.2)
        expected, stats, _, _ = self.reference(frame, MOVIE_Q3_INSTRUCTION, args)
        report, backend = self.execute(frame, main, helper, args, source, engines, MOVIE_Q3_INSTRUCTION)
        self.assertEqual([item["decision"] for item in report["decisions"]], expected["_filter"].tolist())
        self.assertEqual(report["stats"], {key: value.model_dump(mode="json") if hasattr(value, "model_dump") else value
                                           for key, value in stats.items()})
        self.assertEqual([item["algorithm_stage"] for item in backend.close_reports], ["helper_score", "fallback"])

    def test_failed_call_keeps_known_usage_and_stops_before_main(self):
        main, helper, engines = self.models()
        instruction = "Keep {paragraph}"
        source, frame, _ = self.source(main, instruction)
        with self.assertRaisesRegex(RuntimeError, "failed, unknown or malformed") as failed:
            self.execute(frame, main, helper, self.args(), source, engines, instruction, kernel=FailedRowProcess)
        report = failed.exception.query_report
        self.assertEqual([stage["name"] for stage in report["calls"]["stages"]], ["helper_score"])
        self.assertEqual(report["calls"]["usage"]["prompt_tokens"], 120)
        self.assertEqual(report["calls"]["usage"]["completion_tokens"], 9)
        self.assertEqual(sum(row["status"] == "failed" for row in report["calls"]["records"]), 1)
        self.assertTrue(self.backends[-1].close_reports[-1]["stage_process_exit_confirmed"])

    def test_cleanup_failure_keeps_completed_usage_and_prevents_model_role_switch(self):
        main, helper, engines = self.models()
        instruction = "Keep {paragraph}"
        source, frame, _ = self.source(main, instruction)
        with self.assertRaisesRegex(RuntimeError, "cleanup") as failed:
            self.execute(frame, main, helper, self.args(), source, engines, instruction, kernel=FailedCloseProcess)
        report = failed.exception.query_report
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 130)
        backend = self.backends[-1]
        self.assertEqual(len(backend.close_reports), 1)
        self.assertFalse(backend.close_reports[0]["stage_process_exit_confirmed"])
        self.assertTrue(backend.close_reports[0]["engine_reports"][0]["process_exit_confirmed"])
        control = backend.close_reports[0]["cleanup_artifacts"]
        self.addCleanup(shutil.rmtree, control, ignore_errors=True)
        with self.assertRaisesRegex(RuntimeError, "previous cascade stage"):
            backend(SimpleNamespace(model_role="main"))

    def test_empty_relation_allocates_no_native_graph_or_engine(self):
        main, helper, engines = self.models()
        instruction = "Keep {paragraph}"
        source, frame, _ = self.source(main, instruction, empty=True)
        with patch.object(native, "_stage_graph", side_effect=AssertionError("empty graph allocated")):
            report, backend = self.execute(frame, main, helper, self.args(), source, engines, instruction)
        self.assertEqual(report["decisions"], [])
        self.assertEqual(report["calls"]["records"], [])
        self.assertEqual(backend.close_reports, [])

    def test_duplicate_source_text_remains_independent_native_occurrences(self):
        main, helper, engines = self.models()
        instruction = "Keep {paragraph}"
        source, frame, _ = self.source(main, instruction, duplicate_text=True)
        report, _ = self.execute(frame, main, helper, self.args(
            filter_pos_cascade_threshold=0.8, filter_neg_cascade_threshold=0.2), source, engines, instruction)
        helper_rows = [row for row in self.results if row["algorithm_stage"] == "helper_score"]
        duplicate = sorted((row for row in helper_rows if row["source_position"] in (3, 9)),
                           key=lambda row: row["source_position"])
        self.assertEqual(len(helper_rows), 10)
        self.assertEqual(len(duplicate), 2)
        self.assertEqual(duplicate[0]["messages_sha256"], duplicate[1]["messages_sha256"])
        self.assertNotEqual(duplicate[0]["call_id"], duplicate[1]["call_id"])
        self.assertEqual(len(report["decisions"]), 10)
        self.assertEqual(report["calls"]["usage"]["total_tokens"], len(self.results) * 13)

    def test_scan_must_cover_helper_columns_before_native_graph_allocation(self):
        main, helper, engines = self.models()
        instruction = "Keep {paragraph}"
        source, frame, _ = self.source(main, instruction)
        frame["hint"] = frame["paragraph"]
        backend = native.DaftCascadeBackend(source, engines=engines, resources=self.resources)
        with patch.object(EngineSpec, "validate_driver_environment", return_value={}), \
             patch.object(native, "_stage_graph") as graph:
            with self.assertRaisesRegex(ValueError, "omits cascade input columns"):
                run_cascade(frame, main, helper, self.args(helper_filter_instruction="Keep {hint}"), backend,
                    instruction=instruction, query_id="missing-helper-column", max_rows=10, max_calls=25)
            graph.assert_not_called()


if __name__ == "__main__":
    unittest.main()
