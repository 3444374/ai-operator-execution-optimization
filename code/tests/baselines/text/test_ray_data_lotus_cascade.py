"""Ray stage selection, cleanup and opt-in real CPU graph qualification."""

from copy import deepcopy
import csv
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest

from src.baselines.text.frameworks.ray_data_lotus_cascade import (
    _selected, _StageRows, _stage_graph, _CascadeRayRows,
)
from src.baselines.text.frameworks.ray_data_lotus_records import _CLOSE_COLUMN, _RayRows
from src.semantic_methods.lotus.cascade import cascade_identity, run_cascade
from src.semantic_methods.lotus.records import RecordScan, RecordsResources


class SelectionAndCleanupTests(unittest.TestCase):
    def test_repeated_draws_keep_their_call_identity(self):
        row = dict(row_id="same", source_position=2, reviewText="same review")
        calls = [dict(cascade_identity("q", "same", 2, i, model_role="main", stage="threshold_sample"),
                      reviewText=row["reviewText"]) for i in (10, 11)]
        selected = {(row["row_id"], 2): calls}
        self.assertEqual(list(_selected(row, selected=selected, input_columns=("reviewText",))), calls)
        self.assertEqual(list(_selected(dict(row, source_position=3), selected=selected, input_columns=("reviewText",))), [])
        with self.assertRaisesRegex(ValueError, "source text"):
            list(_selected(dict(row, reviewText="changed"), selected=selected, input_columns=("reviewText",)))

    def test_unknown_process_exit_blocks_role_switch_without_erasing_results(self):
        stage = SimpleNamespace(name="helper_score", model_role="helper")
        reports = []
        report = dict(iterator_closed=True, executor_shutdown_confirmed=True,
                      watchdog_stopped=True, cleanup_errors=[],
                      engine_close_report=dict(engine_api_shutdown_confirmed=True,
                                               process_exit_confirmed=False))
        rows = _StageRows(SimpleNamespace(close=lambda: deepcopy(report)), stage, reports)
        with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
            rows.close()
        self.assertFalse(reports[0]["stage_process_exit_confirmed"])
        report["engine_close_report"]["process_exit_confirmed"] = True
        rows.close()
        self.assertEqual(len(reports), 1)
        self.assertTrue(reports[0]["stage_process_exit_confirmed"])
        self.assertNotIn("gpu_exit_confirmed", reports[0]["engine_close_report"])

    def test_cancel_watchdog_waits_for_executor_under_construction(self):
        from unittest.mock import Mock

        with tempfile.TemporaryDirectory() as directory:
            result = SimpleNamespace(_current_executor=None)
            stream = _CascadeRayRows(result, [], time.monotonic() + 30, lambda: True,
                                     control=Path(directory))
            stream.engine_cleanup.stop = Mock(return_value=dict(reports=[], errors=[], process_exit_confirmed=True))
            watcher = threading.Thread(target=stream._watch)
            watcher.start()
            try:
                until = time.monotonic() + 1
                while not stream.engine_cleanup.stop.called and time.monotonic() < until:
                    time.sleep(0.005)
                self.assertTrue(watcher.is_alive())
                executor = Mock()
                result._current_executor = executor
                watcher.join(timeout=1)
                self.assertFalse(watcher.is_alive())
                executor.shutdown.assert_called_once_with(force=True)
                stream.engine_cleanup.stop.assert_called_once()
            finally:
                stream._stop.set()
                watcher.join(timeout=1)

    def test_stop_file_error_still_attempts_registered_process_cleanup(self):
        from unittest.mock import Mock, patch
        from src.baselines.text.frameworks.lotus_cascade_process import StageEngineCleanup

        with tempfile.TemporaryDirectory() as directory:
            cleanup = StageEngineCleanup(Path(directory))
            initial = dict(model_pid=12345, process_created=1.0, process_exit_confirmed=False)
            final = dict(initial, process_exit_confirmed=True, cleanup_errors=[])
            cleanup._read = Mock(return_value=[initial])
            cleanup._stop_registered = Mock(return_value=final)
            with patch.object(Path, "touch", side_effect=PermissionError("fixture")):
                result = cleanup.stop(grace_s=0)
            cleanup._stop_registered.assert_called_once_with(initial)
            self.assertTrue(result["process_exit_confirmed"])
            self.assertIn("stop_request: PermissionError", result["errors"])

    def test_repeated_cleanup_publishes_atomically_and_preserves_a_closed_stage(self):
        from src.baselines.text.frameworks.lotus_cascade_process import StageEngineCleanup

        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory)
            (control / "engine-closed.json").write_text(json.dumps(dict(
                status="closed", process_exit_confirmed=True, cleanup_errors=[],
                model_pid=12345, process_created=1.0)))
            cleanup = StageEngineCleanup(control)
            first = cleanup.stop(grace_s=0)
            second = cleanup.stop(grace_s=0)
            self.assertEqual(first, second)
            self.assertEqual(second["errors"], [])
            self.assertTrue(second["process_exit_confirmed"])
            self.assertEqual(json.loads((control / "driver-cleanup.json").read_text())["errors"], [])

    def test_one_failed_process_cleanup_still_attempts_other_registered_processes(self):
        from unittest.mock import Mock
        from src.baselines.text.frameworks.lotus_cascade_process import StageEngineCleanup

        with tempfile.TemporaryDirectory() as directory:
            cleanup = StageEngineCleanup(Path(directory))
            first = dict(model_pid=11, process_created=1, process_exit_confirmed=False)
            second = dict(model_pid=12, process_created=2, process_exit_confirmed=False)
            cleanup._read = Mock(return_value=[first, second])
            cleanup._stop_registered = Mock(side_effect=[PermissionError("fixture denied"),
                dict(second, process_exit_confirmed=True)])
            result = cleanup.stop(grace_s=0)
            self.assertEqual(cleanup._stop_registered.call_count, 2)
            self.assertEqual(result["reports"][1]["model_pid"], 12)
            self.assertTrue(result["reports"][1]["process_exit_confirmed"])
            self.assertFalse(result["process_exit_confirmed"])
            self.assertIn("registered_engine_cleanup: PermissionError", result["errors"])

    def test_corrupt_engine_report_does_not_hide_another_registered_process(self):
        from unittest.mock import Mock
        from src.baselines.text.frameworks.lotus_cascade_process import StageEngineCleanup

        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory)
            (control / "engine-bad.json").write_text("{")
            second = dict(model_pid=12, process_created=2, process_exit_confirmed=False)
            (control / "engine-valid.json").write_text(json.dumps(second))
            cleanup = StageEngineCleanup(control)
            def stop(report):
                if "model_pid" not in report:
                    raise ValueError("unknown process identity")
                return dict(report, process_exit_confirmed=True)
            cleanup._stop_registered = Mock(side_effect=stop)
            result = cleanup.stop(grace_s=0)
            self.assertTrue(any(row.get("model_pid") == 12 and row["process_exit_confirmed"]
                                for row in result["reports"]))
            self.assertFalse(result["process_exit_confirmed"])
            self.assertIn("engine_report_read: JSONDecodeError", result["errors"])

    def test_late_actor_receipts_survive_cached_process_exit_and_new_children_invalidate_it(self):
        from src.baselines.text.frameworks.lotus_cascade_process import StageEngineCleanup

        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory)
            cleanup = StageEngineCleanup(control)
            cached = dict(model_pid=22, process_created=2, process_exit_confirmed=True,
                          owned_processes=[[22, 2]], cleanup_errors=["driver API unknown"])
            cleanup._driver_reports[(22, 2)] = cached
            latest = dict(deepcopy(cached), process_exit_confirmed=False, cleanup_errors=[],
                          engine_ready=dict(engine_uid="late"),
                          result_failure=dict(verified_call_results=[dict(prompt_tokens=17)]))
            path = control / "engine-owned.json"
            path.write_text(json.dumps(latest))
            report = cleanup._read()[0]
            self.assertTrue(report["process_exit_confirmed"])
            self.assertEqual(report["result_failure"], latest["result_failure"])
            self.assertEqual(report["engine_ready"], latest["engine_ready"])
            self.assertIn("driver API unknown", report["cleanup_errors"])
            latest["owned_processes"].append([23, 3])
            path.write_text(json.dumps(latest))
            self.assertFalse(cleanup._read()[0]["process_exit_confirmed"])

    def test_ray_registered_failure_receipts_are_exposed_and_delivered_unknowns_stay_terminal(self):
        from unittest.mock import Mock, patch
        from src.semantic_methods.lotus.cascade import CascadeStage

        calls = [cascade_identity("q", "r", 0, index, model_role="main", stage="threshold_sample")
                 for index in range(2)]
        stage = CascadeStage("threshold_sample", "main", "fixture", "Check text", True, None,
                             ("True", "False"), dict(logprobs=False), tuple(calls), ("text",))
        receipts = [dict(call, status="succeeded", submission_confirmed=True,
                        engine_uid="engine", engine_protocol_error="batch fault",
                        prompt_tokens=17, completion_tokens=1) for call in calls]
        engine_report = dict(process_exit_confirmed=True, cleanup_errors=[],
            engine_ready=dict(engine_uid="engine"), result_failure=dict(verified_call_results=receipts))
        with tempfile.TemporaryDirectory() as directory:
            rows = _CascadeRayRows(None, calls, time.monotonic() + 30, None,
                                   control=Path(directory), stage=stage)
            rows._started = True
            rows.received.add(calls[1]["call_id"])
            rows.engine_cleanup.stop = Mock(return_value=dict(reports=[engine_report], errors=[],
                                                               process_exit_confirmed=True))
            with patch.object(_RayRows, "close", side_effect=RuntimeError("graph close failed")):
                stream = _StageRows(rows, stage, [])
                with self.assertRaisesRegex(RuntimeError, "graph close failed"):
                    stream.close()
            self.assertEqual(stream.last_close_report["unconsumed_call_results"], [receipts[0]])
            self.assertFalse(stream.last_close_report["stage_process_exit_confirmed"])


@unittest.skipUnless(os.environ.get("RUN_LOTUS_CASCADE_CPU") == "1", "optional pinned LOTUS CPU contract")
class MethodBindingTests(unittest.TestCase):
    def test_main_source_binding_covers_distinct_helper_and_stage_changes_fail_before_allocation(self):
        from dataclasses import replace
        from unittest.mock import Mock, patch
        import ray
        from tokenizers import Tokenizer, models as token_models
        from lotus.nl_expression import nle2str
        from src.baselines.text.frameworks.ray_data_lotus_cascade import RayCascadeBackend
        from src.semantic_methods.lotus.cascade import CascadeStage, _effective
        from src.semantic_methods.lotus.records import FilterMethod
        from src.serving.backends.vllm_engine import EngineSpec
        from tests.semantic_methods.test_lotus_cascade import models

        main, helper = models()
        method = FilterMethod(main.model, "Check {body}.", resolved_input_columns=("body",))
        helper_method = FilterMethod(helper.model, "Check {summary}.", resolved_input_columns=("summary",))
        scan = RecordScan((("row_id", "string"), ("source_position", "int64"),
                           ("body", "string"), ("summary", "string")), ("body", "summary"))
        source = SimpleNamespace(preflight=Mock(return_value={}), scan_spec=Mock(return_value=scan))
        with tempfile.TemporaryDirectory() as directory:
            tokenizer = Tokenizer(token_models.WordLevel({"[UNK]": 0, "True": 1, "False": 2}, unk_token="[UNK]"))
            engines = {}
            for role in ("main", "helper"):
                root = Path(directory) / role
                root.mkdir()
                tokenizer.save(str(root / "tokenizer.json"))
                engines[role] = EngineSpec(str(root), sys.executable, sys.executable, max_model_calls=7)
            effective = dict(main=_effective(main, {}, False), helper=_effective(helper, {}, True))
            with patch.object(EngineSpec, "validate_driver_environment", return_value={}), \
                 patch.object(ray, "is_initialized", return_value=True), \
                 patch.object(ray, "nodes", return_value=[dict(Alive=True, NodeID="local")]), \
                 patch.object(ray, "get_runtime_context", return_value=SimpleNamespace(get_node_id=lambda: "local")):
                backend = RayCascadeBackend(source, engines=engines, resources=RecordsResources())
                backend.preflight(main, helper, effective, max_possible_calls=25,
                                  method=method, helper_method=helper_method)
                source.preflight.assert_called_once_with(method)
                source.scan_spec.assert_called_once_with(method)
                stage = CascadeStage("helper_score", "helper", helper.model,
                    nle2str(helper_method.instruction, ["summary"]), True, None,
                    ("True", "False"), effective["helper"], (), ("summary",))
                self.assertEqual(list(backend(stage)), [])
                with self.assertRaisesRegex(ValueError, "method binding"):
                    backend(replace(stage, instruction="changed"))
                source.scan_spec.return_value = replace(scan, payload_columns=("body",))
                other = RayCascadeBackend(source, engines=engines, resources=RecordsResources())
                with self.assertRaisesRegex(ValueError, "main and helper"):
                    other.preflight(main, helper, effective, max_possible_calls=25,
                                    method=method, helper_method=helper_method)


@unittest.skipUnless(os.environ.get("RUN_LOTUS_CASCADE_RAY_CPU") == "1", "explicit real Ray CPU qualification")
class RealRayCascadeTests(unittest.TestCase):
    def test_generic_native_csv_multicolumn_stages_match_original_control(self):
        import lotus
        import pandas as pd
        import ray
        import ray.data
        from lotus.types import CascadeArgs

        self.assertEqual(ray.__version__, "2.56.1")
        self.assertFalse(ray.is_initialized(), "CPU qualification owns its local Ray runtime")

        class FixedCpuModel:
            """Explicit inference fixture; preparation and Ray execution use real libraries."""

            def __init__(self, role, expected_count):
                self.role, self.expected = role, expected_count
                self.completed = 0

            def __call__(self, batch):
                import pyarrow as pa

                rows = []
                for prepared in batch.to_pylist():
                    from tests.semantic_methods.test_lotus_cascade import terminal
                    rows.append(dict(prepared, **terminal(prepared, self.role),
                        infer_worker_pid=os.getpid(), **{_CLOSE_COLUMN: ""}))
                self.completed += len(rows)
                if self.completed == self.expected:
                    rows[-1][_CLOSE_COLUMN] = json.dumps(dict(
                        engine_api_shutdown_confirmed=True, process_exit_confirmed=True,
                        gpu_exit_confirmed=None, cleanup_scope="CPU software fixture; no engine process",
                        cleanup_errors=[]))
                return pa.Table.from_pylist(rows)

        from tests.semantic_methods.test_lotus_cascade import models, CascadeDifferentialTests

        instruction = "Do {title} and {body} support the claim?"
        helper_instruction = "Does {summary} support the claim?"
        frame = pd.DataFrame([dict(row_id="row-" + str(i), source_position=i,
            title="title " + str(i) + ";", body="body " + str(i) + ";",
            summary="summary " + str(i) + ";") for i in range(10)])
        scan = RecordScan((("row_id", "string"), ("source_position", "int64"),
            ("bucket", "int64"), ("title", "string"), ("body", "string"),
            ("summary", "string")), ("title", "body", "summary"), ("bucket", 3))
        with tempfile.TemporaryDirectory(prefix="lc-input-") as inputs, \
             tempfile.TemporaryDirectory(prefix="lc-ray-", dir="/tmp") as runtime:
            source = SimpleNamespace(csv_path=Path(inputs) / "records.csv")
            with source.csv_path.open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=[name for name, _ in scan.csv_schema])
                writer.writeheader()
                for row in frame.to_dict("records"):
                    writer.writerow(dict(row, bucket=3))
                writer.writerow(dict(row_id="excluded", source_position=10, bucket=2, title="excluded",
                                     body="must not reach model", summary="excluded"))
            reports, results = [], []
            resources = RecordsResources(prepare_batch_rows=2, model_batch_rows=3, query_timeout_s=120)
            deadline = time.monotonic() + 120
            try:
                ray.init(address="local", num_cpus=4, num_gpus=0, include_dashboard=False,
                         object_store_memory=80 * 1024**2, _temp_dir=runtime,
                         _node_ip_address="127.0.0.1")

                def evaluate(stage):
                    graph = _stage_graph(source, stage, resources, deadline,
                        scan=scan,
                        prepare_python=sys.executable, infer_class=FixedCpuModel,
                        infer_kwargs=dict(role=stage.model_role, expected_count=len(stage.records)),
                        infer_python=sys.executable, num_gpus=0)
                    identities = [{key: row[key] for key in cascade_identity(
                        "q", "row", 0, 0, model_role=stage.model_role, stage=stage.name)}
                        for row in stage.records]
                    class Observe:
                        def __init__(self):
                            self.rows = _StageRows(_RayRows(graph, identities, deadline, None), stage, reports)
                        def __iter__(self):
                            return self
                        def __next__(self):
                            value = next(self.rows)
                            results.append(value)
                            return value
                        def close(self):
                            return self.rows.close()
                    return Observe()

                main, helper = models()
                args = CascadeArgs(sampling_percentage=0.5, cascade_IS_random_seed=5,
                                   cascade_num_calibration_quantiles=5,
                                   helper_filter_instruction=helper_instruction)
                reference = CascadeDifferentialTests()
                reference.frame = frame
                native, native_stats, native_calls, _, native_main, native_helper = reference.reference(
                    args, instruction=instruction)
                with lotus.settings.context(enable_cache=False):
                    report = run_cascade(frame, main, helper, args, evaluate,
                                         instruction=instruction, query_id="real-ray-cpu", max_rows=10, max_calls=25)
                self.assertEqual(report["status"], "completed")
                names = [stage["name"] for stage in report["calls"]["stages"]]
                self.assertEqual(names, ["helper_score", "threshold_sample", "fallback"])
                self.assertEqual(len(reports), 3)
                self.assertTrue(all(row["stage_process_exit_confirmed"] for row in reports))
                self.assertTrue(all(row["prepare_worker_pid"] != os.getpid() for row in results))
                self.assertTrue(all(row["infer_worker_pid"] != os.getpid() for row in results))
                sample = [row for row in results if row["algorithm_stage"] == "threshold_sample"]
                self.assertEqual(len(sample), 5)
                self.assertLess(len({row["source_position"] for row in sample}), 5)
                self.assertEqual(len({row["call_id"] for row in sample}), 5)
                self.assertNotIn("excluded", {row["row_id"] for row in results})
                self.assertEqual(report["calls"]["usage"]["total_tokens"],
                                 sum(row["total_tokens"] for row in results))
                self.assertEqual([row["decision"] for row in report["decisions"]], native["_filter"].tolist())
                from src.semantic_methods.lotus.engine import lm_report
                self.assertEqual(report["lm"], dict(main=lm_report(native_main), helper=lm_report(native_helper)))
                self.assertEqual(report["stats"], {key: value.model_dump(mode="json") if hasattr(value, "model_dump") else value
                                                   for key, value in native_stats.items()})
                stage_options = {stage["name"]: stage["effective_kwargs"] for stage in report["calls"]["stages"]}
                self.assertEqual([(row["algorithm_stage"], json.loads(row["messages_json"]),
                                   stage_options[row["algorithm_stage"]])
                                  for row in sorted(results, key=lambda item: item["call_index"])], native_calls)
                self.assertEqual(len(report["decisions"]), len(frame))
                self.assertNotIn("positive_review_cnt", report)
            finally:
                ray.shutdown()


@unittest.skipUnless(os.environ.get("RUN_LOTUS_CASCADE_PROCESS_CPU") == "1", "explicit local process cleanup qualification")
class RegisteredProcessTests(unittest.TestCase):
    def test_driver_reaps_only_its_registered_private_session_and_retains_unknown_api(self):
        import subprocess
        import psutil
        from src.baselines.text.frameworks.lotus_cascade_process import StageEngineCleanup

        with tempfile.TemporaryDirectory(prefix="cascade-process-") as directory:
            process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
            try:
                created = psutil.Process(process.pid).create_time()
                control = Path(directory)
                (control / "engine-owned.json").write_text(json.dumps(dict(
                    model_pid=process.pid, process_created=created,
                    status="starting", process_exit_confirmed=False,
                    owned_processes=[[process.pid, created]], cleanup_errors=[])))
                result = StageEngineCleanup(control).stop(grace_s=0.02)
                process.wait(timeout=2)
                self.assertTrue(result["process_exit_confirmed"])
                self.assertEqual(result["reports"][0]["status"], "unknown")
                self.assertIsNone(result["reports"][0]["gpu_exit_confirmed"])
                self.assertTrue((control / "driver-cleanup.json").is_file())
                self.assertTrue(result["errors"])
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=2)


if __name__ == "__main__":
    unittest.main()
