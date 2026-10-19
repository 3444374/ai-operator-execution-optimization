"""Ray stage selection, cleanup and opt-in real CPU graph qualification."""

from copy import deepcopy
import csv
import json
import math
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
from src.semantic_methods.lotus.records import RecordsResources


class SelectionAndCleanupTests(unittest.TestCase):
    def test_repeated_draws_keep_their_call_identity(self):
        row = dict(row_id="same", source_position=2, reviewText="same review")
        calls = [dict(cascade_identity("q", "same", 2, i, model_role="main", stage="threshold_sample"),
                      reviewText=row["reviewText"]) for i in (10, 11)]
        selected = {(row["row_id"], 2): calls}
        self.assertEqual(list(_selected(row, selected=selected)), calls)
        self.assertEqual(list(_selected(dict(row, source_position=3), selected=selected)), [])
        with self.assertRaisesRegex(ValueError, "source text"):
            list(_selected(dict(row, reviewText="changed"), selected=selected))

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


@unittest.skipUnless(os.environ.get("RUN_LOTUS_CASCADE_RAY_CPU") == "1", "explicit real Ray CPU qualification")
class RealRayCascadeTests(unittest.TestCase):
    def test_native_csv_prepare_and_repeated_sample_graphs_match_original_control(self):
        import lotus
        import pandas as pd
        import ray
        import ray.data
        from lotus.cache import InMemoryCache
        from lotus.models import LM
        from lotus.types import CascadeArgs
        from tokenizers import Tokenizer, models as token_models

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
                    position = prepared["source_position"]
                    probability = (position + 1) / 12
                    token = "True" if self.role == "helper" and position % 2 else "False"
                    content = [dict(token=token, logprob=math.log(0.7), top_logprobs=[
                        dict(token="True", logprob=math.log(probability)),
                        dict(token="False", logprob=math.log(1-probability)),
                    ])]
                    rows.append(dict(prepared, text=token, status="succeeded", submission_confirmed=True,
                        prompt_tokens=12, completion_tokens=1, total_tokens=13,
                        finish_reason="stop", engine_request_id=prepared["call_id"],
                        engine_uid="real-ray-declared-cpu-fixture", output_token_ids_json="[1]",
                        logprobs_json=json.dumps(content) if self.role == "helper" else "null",
                        infer_worker_pid=os.getpid(), usage_source="fixed response; zero model inference",
                        **{_CLOSE_COLUMN: ""}))
                self.completed += len(rows)
                if self.completed == self.expected:
                    rows[-1][_CLOSE_COLUMN] = json.dumps(dict(
                        engine_api_shutdown_confirmed=True, process_exit_confirmed=True,
                        gpu_exit_confirmed=None, cleanup_scope="CPU software fixture; no engine process",
                        cleanup_errors=[]))
                return pa.Table.from_pylist(rows)

        def model(name):
            tokenizer = Tokenizer(token_models.WordLevel(
                {"[UNK]": 0, "True": 1, "False": 2}, unk_token="[UNK]"))
            return LM(name, tokenizer=tokenizer, cache=InMemoryCache(max_size=2), max_tokens=8)

        frame = pd.DataFrame([dict(row_id="movie-row-" + str(i), source_position=i,
                                   reviewText="review " + str(i)) for i in range(10)])
        with tempfile.TemporaryDirectory(prefix="lc-input-") as inputs, \
             tempfile.TemporaryDirectory(prefix="lc-ray-", dir="/tmp") as runtime:
            source = SimpleNamespace(csv_path=Path(inputs) / "records.csv")
            with source.csv_path.open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=["row_id", "source_position", "reviewId", "id", "reviewText"])
                writer.writeheader()
                for row in frame.to_dict("records"):
                    writer.writerow(dict(row, reviewId="duplicate", id="taken_3"))
                writer.writerow(dict(row_id="excluded", source_position=10, reviewId="duplicate",
                                     id="other_movie", reviewText="must not reach model"))
            reports, results = [], []
            resources = RecordsResources(prepare_batch_rows=2, model_batch_rows=3, query_timeout_s=120)
            deadline = time.monotonic() + 120
            try:
                ray.init(address="local", num_cpus=4, num_gpus=0, include_dashboard=False,
                         object_store_memory=80 * 1024**2, _temp_dir=runtime,
                         _node_ip_address="127.0.0.1")

                def evaluate(stage):
                    graph = _stage_graph(source, stage, resources, deadline,
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

                main, helper = model("openai/ray-fixture-main"), model("openai/ray-fixture-helper")
                args = CascadeArgs(sampling_percentage=0.5, cascade_IS_random_seed=5,
                                   cascade_num_calibration_quantiles=5)
                with lotus.settings.context(enable_cache=False):
                    report = run_cascade(frame, main, helper, args, evaluate,
                                         query_id="real-ray-cpu", max_rows=10, max_calls=25)
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
                self.assertEqual(report["calls"]["usage"]["total_tokens"], len(results) * 13)
                self.assertEqual(report["positive_review_cnt"], sum(row["decision"] for row in report["decisions"]))
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
