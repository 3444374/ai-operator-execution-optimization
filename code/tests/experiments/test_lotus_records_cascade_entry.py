"""Public cascade config/factory/lifecycle CPU contracts; model owners are fixtures."""

from contextlib import contextmanager, redirect_stdout
from copy import deepcopy
import csv
import hashlib
import io
import json
import math
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
from src.experiments import lotus_records_movie as runner
from src.semantic_methods.lotus.records import FilterMethod
from src.serving.backends.vllm_engine import EngineSpec


def configuration(backend="semloom-local-model-core-cascade"):
    return dict(schema="semloom.movie_records_cascade_run.v1", backend=backend, query_id="cascade-cli-q",
        method=FilterMethod("openai/fixture-main").describe(), helper_model_name="openai/fixture-helper",
        engines={role: dict(model_path="/tmp/cascade-" + role, model_python="/tmp/model-python",
                           driver_python="/tmp/driver-python", max_model_calls=2)
                 for role in ("main", "helper")},
        generations=dict(main=dict(max_tokens=8, seed=4), helper=dict(max_tokens=8, seed=7, logprobs=10)),
        cascade_args=dict(filter_pos_cascade_threshold=0, filter_neg_cascade_threshold=0),
        resources=dict(prepare_batch_rows=1, model_batch_rows=1), max_calls=4)


def read(value):
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder)/"config.json"
        path.write_text(json.dumps(value))
        return runner.read_cascade_config(path)


class CascadeConfigurationTests(unittest.TestCase):
    def test_explicit_roles_generation_and_caps_leave_ordinary_five_values_unchanged(self):
        for backend in runner.CASCADE_BACKENDS:
            result = read(configuration(backend))
            self.assertEqual(result[0]["backend"], backend)
            self.assertEqual(result[2]["helper"].logprobs, 10)
            self.assertEqual(result[2]["main"].seed, 4)
            self.assertEqual(result[4]["helper"].max_model_calls, 2)
        ordinary = dict(schema="semloom.movie_records_run.v1", backend="ray-data-local-model", query_id="old",
                        method=configuration()["method"], engine=configuration()["engines"]["main"])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"config.json"
            path.write_text(json.dumps(ordinary))
            self.assertEqual(len(runner.read_config(path)), 5)
            path.write_text(json.dumps(configuration()))
            with self.assertRaisesRegex(ValueError, "ordinary"):
                runner.read_config(path)
        with self.assertRaisesRegex(ValueError, "helper-cascade"):
            read(ordinary)

    def test_unknown_fields_invalid_roles_and_unbounded_caps_are_rejected(self):
        bad = []
        for name in ("method", "helper_model_name", "engines", "generations", "cascade_args", "max_calls"):
            value = configuration()
            del value[name]
            bad.append(value)
        bad += [dict(configuration(), async_limits={}), dict(configuration(), backend="ray-data-local-model"),
                dict(configuration(), engines=dict(main=configuration()["engines"]["main"])),
                dict(configuration(), generations=dict(helper=dict(logprobs=10)))]
        for cap in (None, True, 0, -1):
            value = configuration()
            value["engines"]["helper"]["max_model_calls"] = cap
            bad.append(value)
        for cap in (None, True, 0, -1, 4.5):
            bad.append(dict(configuration(), max_calls=cap))
        for value in bad:
            with self.subTest(value=value), self.assertRaises((ValueError, TypeError, KeyError)):
                read(value)

    def test_q3_binding_and_core_batch_guards_precede_engine_profile(self):
        bad = []
        value = configuration()
        value["method"]["instruction"] = "Is {reviewText} short?"
        bad.append(value)
        value = configuration()
        value["resources"]["model_batch_rows"] = 2
        bad.append(value)
        for value in bad:
            with self.subTest(value=value), patch.object(EngineSpec, "from_dict") as engine:
                with self.assertRaises(ValueError):
                    read(value)
                engine.assert_not_called()

    def test_helper_logs_and_original_args_are_strict_before_engine_profile(self):
        bad = []
        for logs in (None, True, 0, 21):
            value = configuration()
            value["generations"]["helper"]["logprobs"] = logs
            bad.append(value)
        for role, change in (("main", dict(logprobs=1)), ("helper", dict(response_format=dict(type="json_object"))),
                             ("main", dict(repetition_penalty=1.1))):
            value = configuration()
            value["generations"][role].update(change)
            bad.append(value)
        for args in (dict(typo=1), dict(sampling_percentage=True), dict(recall_target="0.8")):
            bad.append(dict(configuration(), cascade_args=args))
        for value in bad:
            with self.subTest(value=value), patch.object(EngineSpec, "from_dict") as engine:
                with self.assertRaises((ValueError, TypeError)):
                    read(value)
                engine.assert_not_called()

    def test_distinct_deployments_and_context_are_explicit(self):
        value = configuration()
        value["engines"]["helper"]["model_path"] = value["engines"]["main"]["model_path"]
        with self.assertRaisesRegex(ValueError, "distinct"):
            read(value)
        value = configuration()
        value["generations"]["helper"]["max_tokens"] = 4097
        with self.assertRaisesRegex(ValueError, "max_model_len"):
            read(value)


class CascadePreflightTests(unittest.TestCase):
    """Real LOTUS validation/LM configuration; local word tokenizer, no model weights."""

    def setUp(self):
        from tokenizers import Tokenizer, models

        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        original = self.root/"reviews.csv"
        with original.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["reviewId", "id", "reviewText", "scoreSentiment"])
            writer.writerows([["same", "taken_3", "yes", "POSITIVE"], ["same", "other", "omit", "NEGATIVE"],
                              ["same", "taken_3", "no", "NEGATIVE"]])
        manifest = prepare_movie_source(original, self.root/"source",
            expected_csv_sha256=hashlib.sha256(original.read_bytes()).hexdigest(), max_rows=3)
        self.source = MovieRecordSource.from_manifest(manifest)
        value = configuration()
        for role in ("main", "helper"):
            model = self.root/role
            model.mkdir()
            Tokenizer(models.WordLevel({"[UNK]": 0, "True": 1, "False": 2}, unk_token="[UNK]")).save(
                str(model/"tokenizer.json"))
            value["engines"][role]["model_path"] = str(model)
        self.config, self.method, self.generations, self.resources, self.engines, self.args = read(value)

    def preflight(self, *, args=None, cap=None):
        config = dict(self.config, max_calls=cap if cap is not None else self.config["max_calls"])
        with patch.object(runner, "verify_checkout"), patch.object(EngineSpec, "validate_driver_environment",
                return_value=dict(scope="CPU interpreter fixture")):
            return runner.cascade_model_preflight(config, self.source, "fixture-checkout", self.method,
                self.generations, self.engines, self.args if args is None else args)

    def test_real_original_args_and_effective_generation_are_validated_without_allocation(self):
        result = self.preflight()
        self.assertEqual(result["inference_calls"], 0)
        self.assertEqual(result["max_possible_calls"], 4)
        self.assertEqual(result["max_stage_calls"], dict(main=2, helper=2))
        self.assertEqual(result["effective_generation"]["helper"]["top_logprobs"], 10)
        self.assertIs(result["effective_generation"]["helper"]["logprobs"], True)
        self.assertIs(result["effective_generation"]["main"]["logprobs"], False)
        self.assertEqual(result["effective_generation"]["helper"]["seed"], 7)
        self.assertEqual(result["effective_generation"]["main"]["stop"], ["\n"])

    def test_global_and_per_lifetime_caps_cover_original_sampling_before_runtime(self):
        from lotus.types import CascadeArgs
        learned = CascadeArgs(sampling_percentage=.5, cascade_IS_random_seed=7)
        self.assertEqual(self.preflight(args=learned, cap=5)["max_possible_calls"], 5)
        with self.assertRaisesRegex(ValueError, "max_calls"):
            self.preflight(args=learned, cap=4)
        self.engines["main"] = EngineSpec.from_dict(dict(self.config["engines"]["main"], max_model_calls=1))
        with self.assertRaisesRegex(ValueError, "lifetime cap"):
            self.preflight()

    def test_wrong_helper_column_and_unsupported_original_args_fail_preflight(self):
        from lotus.types import CascadeArgs
        for args in (CascadeArgs(helper_filter_instruction="Is {otherText} positive?"),
                     CascadeArgs(map_instruction="map"), CascadeArgs(sampling_percentage=0)):
            with self.subTest(args=args), self.assertRaises(ValueError):
                self.preflight(args=args)

    def test_source_failure_precedes_interpreter_and_tokenizer_preflight(self):
        with patch.object(self.source, "preflight", side_effect=ValueError("source mismatch")), \
             patch.object(EngineSpec, "validate_driver_environment") as environment, \
             patch.object(runner, "_cascade_lms") as models:
            with self.assertRaisesRegex(ValueError, "source mismatch"):
                runner.cascade_model_preflight(self.config, self.source, "fixture-checkout", self.method,
                                               self.generations, self.engines, self.args)
            environment.assert_not_called()
            models.assert_not_called()

    def test_original_cascade_parser_usage_and_movie_evaluator_remain_the_execution_path(self):
        import lotus
        from src.experiments import lotus_cascade_movie as movie

        self.preflight()
        models = runner._cascade_lms(self.config, self.method, self.generations, self.engines)
        stages = []
        def execute(stage):
            stages.append(stage)
            return [dict({name: value for name, value in row.items() if name != "reviewText"},
                status="succeeded", text="True", finish_reason="stop", submission_confirmed=True,
                engine_request_id=row["call_id"], engine_uid="declared-model-fixture", prompt_tokens=12,
                completion_tokens=1, total_tokens=13, output_token_ids_json="[1]", logprobs_json=json.dumps([
                    dict(token="True", logprob=math.log(.8), top_logprobs=[
                        dict(token="True", logprob=math.log(.8)), dict(token="False", logprob=math.log(.2))])]))
                for row in stage.records]
        with lotus.settings.context(enable_cache=False), patch.object(movie, "verify_checkout"), \
             patch.object(movie, "evaluate_original", return_value=dict(fixture_evaluator=True)) as evaluate:
            report = movie.run_movie_cascade_query(self.source, "fixture-checkout", models["main"], models["helper"],
                self.args, execute, query_id=self.config["query_id"], max_calls=self.config["max_calls"])
        self.assertEqual(report["positive_review_cnt"], 2)
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 26)
        self.assertEqual(report["lm"]["helper"]["stats"]["physical_usage"]["total_tokens"], 26)
        self.assertEqual([stage.name for stage in stages], ["helper_score"])
        evaluate.assert_called_once()
        self.assertEqual(evaluate.call_args.args[1], 3)
        self.assertEqual(evaluate.call_args.args[2].iloc[0]["positive_review_cnt"], 2)


class CascadeEntryTests(unittest.TestCase):
    def test_three_factories_use_delivered_native_backends(self):
        paths = ("src.baselines.text.frameworks.ray_data_lotus_cascade.RayCascadeBackend",
                 "src.baselines.text.frameworks.daft_lotus_cascade.DaftCascadeBackend",
                 "src.execution_provider.adapters.lotus_cascade_core.CoreCascadeBackend")
        for backend, path in zip(runner.CASCADE_BACKENDS, paths, strict=True):
            with self.subTest(backend=backend), patch(path) as factory:
                config, _, _, resources, engines, _ = read(configuration(backend))
                self.assertIs(runner._cascade_backend(config, "source", engines, resources), factory.return_value)
                factory.assert_called_once_with("source", engines=engines, resources=resources)

    def test_check_and_unexecuted_run_never_construct_backend_or_runtime(self):
        parsed = read(configuration())
        source = SimpleNamespace(manifest_path=Path("source.json"))
        for action in ("cascade-check", "cascade-run"):
            argv = [action, "--source-manifest", "source.json", "--sembench-checkout", "checkout", "--config", "config"]
            if action.endswith("run"):
                argv += ["--output-dir", "unused"]
            with self.subTest(action=action), redirect_stdout(io.StringIO()) as output, \
                 patch.object(runner.MovieRecordSource, "from_manifest", return_value=source), \
                 patch.object(runner, "read_cascade_config", return_value=parsed), \
                 patch.object(runner, "cascade_model_preflight", return_value=dict(inference_calls=0)), \
                 patch.object(runner, "_cascade_backend") as factory, patch.object(runner, "_cascade_model_worker") as worker, \
                 patch.object(runner, "new_private_directory") as directory:
                self.assertEqual(runner.main(argv), 0)
                self.assertEqual(json.loads(output.getvalue())["inference_calls"], 0)
                factory.assert_not_called()
                worker.assert_not_called()
                directory.assert_not_called()

    def test_explicit_run_uses_original_supervisor_and_worker_action(self):
        parsed = read(configuration())
        source = SimpleNamespace(manifest_path=Path("source.json"))
        with patch.object(runner.MovieRecordSource, "from_manifest", return_value=source), \
             patch.object(runner, "read_cascade_config", return_value=parsed), \
             patch.object(runner, "cascade_model_preflight", return_value=dict(inference_calls=0)), \
             patch.object(runner, "new_private_directory"), patch.object(runner, "write_private_json"), \
             patch("src.experiments.lotus_records_supervisor.supervise") as supervise, \
             patch.object(runner, "_cascade_model_worker") as worker:
            runner.main(["cascade-run", "--source-manifest", "source.json", "--sembench-checkout", "checkout",
                         "--config", "config", "--output-dir", "unused", "--execute-model"])
        worker.assert_not_called()
        command, _ = supervise.call_args.args
        self.assertIn("cascade-run", command)
        self.assertIn("--worker", command)
        self.assertIn("--execute-model", command)
        self.assertEqual(supervise.call_args.kwargs["timeout_s"],
                         parsed[3].startup_timeout_s + parsed[3].query_timeout_s + 30)

    @contextmanager
    def worker(self, *, backend="semloom-local-model-core-cascade", close_error=None, close_status="closed"):
        self.parsed = read(configuration(backend))
        self.runtime = SimpleNamespace(init=Mock(), is_initialized=Mock(return_value=True), shutdown=Mock())
        self.owner = SimpleNamespace(close=Mock(return_value=dict(status=close_status, cleanup_errors=[]),
                                                side_effect=close_error),
            close_reports=[dict(stage_process_exit_confirmed=True, gpu_exit_confirmed=None)],
            production_core_connected=backend == "semloom-local-model-core-cascade")
        self.saved = []
        with patch.dict(sys.modules, {"ray": self.runtime}), \
             patch.object(runner, "cascade_model_preflight") as self.preflight, \
             patch.object(runner, "_cascade_lms", return_value=dict(main="main-lm", helper="helper-lm")), \
             patch.object(runner, "_cascade_backend", return_value=self.owner) as self.factory, \
             patch.object(runner, "write_private_json", side_effect=lambda _, v: self.saved.append(deepcopy(v))):
            yield

    def invoke(self):
        config, method, generations, resources, engines, args = self.parsed
        runner._cascade_model_worker(config, "source", "checkout", method, generations, resources, engines, args, Path("unused"))

    def test_worker_dispatch_preserves_original_cascade_report_and_scopes(self):
        for backend in runner.CASCADE_BACKENDS:
            with self.subTest(backend=backend), self.worker(backend=backend), \
                 patch("src.experiments.lotus_cascade_movie.run_movie_cascade_query",
                       return_value=dict(status="completed", calls=dict(usage=dict(total_tokens=26)))) as query:
                self.invoke()
                self.assertIs(query.call_args.args[5], self.owner)
                self.assertEqual(query.call_args.kwargs, dict(query_id="cascade-cli-q", max_calls=4))
                self.assertEqual(self.saved[0]["calls"]["usage"]["total_tokens"], 26)
                self.assertFalse(self.saved[0]["independent_gpu_process_exit_check"])
                self.assertFalse(self.saved[0]["performance_claim"])
                self.assertEqual(self.saved[0]["production_core_used"], backend == "semloom-local-model-core-cascade")
                self.runtime.init.assert_not_called() if backend.startswith("daft") else self.runtime.init.assert_called_once()

    def test_primary_error_unknown_and_known_usage_survive_cleanup_and_recording_errors(self):
        calls = dict(schema="semloom.lotus_cascade_calls.v1", records=[dict(status="succeeded", total_tokens=13),
            dict(status="unknown", total_tokens=None)], usage=dict(known_prompt_tokens=12, total_tokens=None))
        primary = ValueError("declared cascade failure")
        primary.query_report = dict(status="failed", calls=deepcopy(calls))
        with self.worker(close_error=OSError("declared cleanup failure")), \
             patch("src.experiments.lotus_cascade_movie.run_movie_cascade_query", side_effect=primary), \
             patch.object(runner, "write_private_json", side_effect=OSError("declared recording failure")):
            with self.assertRaises(ValueError) as caught:
                self.invoke()
            self.assertIs(caught.exception, primary)
            self.assertEqual(primary.query_report["calls"], calls)
            self.assertEqual(primary.query_report["close_error_type"], "OSError")
            self.assertEqual(len(primary.__notes__), 2)
            self.runtime.shutdown.assert_called_once()

    def test_successful_query_report_write_failure_is_failed_without_losing_usage(self):
        calls = dict(usage=dict(prompt_tokens=24, completion_tokens=2, total_tokens=26))
        recording = OSError("declared report write failure")
        with self.worker(), \
             patch("src.experiments.lotus_cascade_movie.run_movie_cascade_query",
                   return_value=dict(status="completed", calls=deepcopy(calls))), \
             patch.object(runner, "write_private_json", side_effect=recording):
            with self.assertRaises(OSError) as caught:
                self.invoke()
            self.assertIs(caught.exception, recording)
            self.assertEqual(recording.query_report["status"], "failed")
            self.assertEqual(recording.query_report["report_error_type"], "OSError")
            self.assertEqual(recording.query_report["calls"], calls)
            self.owner.close.assert_called_once()
            self.runtime.shutdown.assert_called_once()

    def test_metadata_getter_failure_closes_owner_and_runtime_without_replacing_primary(self):
        primary = ValueError("declared primary failure")
        calls = dict(records=[dict(status="unknown", total_tokens=None)],
                     usage=dict(known_prompt_tokens=12, total_tokens=None))
        primary.query_report = dict(status="failed", calls=deepcopy(calls))

        class MetadataOwner:
            close = Mock(return_value=dict(status="closed", cleanup_errors=[]))
            close_reports = []

            @property
            def production_core_connected(self):
                raise RuntimeError("declared metadata failure")

        with self.worker(), \
             patch("src.experiments.lotus_cascade_movie.run_movie_cascade_query", side_effect=primary):
            self.owner = MetadataOwner()
            self.factory.return_value = self.owner
            with self.assertRaises(ValueError) as caught:
                self.invoke()
            self.assertIs(caught.exception, primary)
            self.assertEqual(primary.query_report["calls"], calls)
            self.assertEqual(primary.query_report["close_metadata_error_type"], "RuntimeError")
            self.assertEqual(primary.__notes__, ["cascade close metadata also failed: RuntimeError"])
            self.owner.close.assert_called_once()
            self.runtime.shutdown.assert_called_once()

    def test_preflight_failure_precedes_backend_and_runtime_creation(self):
        with self.worker(), patch("src.experiments.lotus_cascade_movie.run_movie_cascade_query") as query:
            self.preflight.side_effect = ValueError("preallocation failure")
            with self.assertRaisesRegex(ValueError, "preallocation"):
                self.invoke()
            self.factory.assert_not_called()
            self.runtime.init.assert_not_called()
            query.assert_not_called()

    def test_unconfirmed_cleanup_cannot_become_success_or_hide_usage(self):
        with self.worker(close_status="unknown"), patch("src.experiments.lotus_cascade_movie.run_movie_cascade_query",
                return_value=dict(status="completed", calls=dict(usage=dict(total_tokens=26)))):
            with self.assertRaisesRegex(RuntimeError, "cleanup remains unconfirmed") as caught:
                self.invoke()
            self.assertEqual(caught.exception.query_report["status"], "failed")
            self.assertEqual(caught.exception.query_report["calls"]["usage"]["total_tokens"], 26)

    def test_native_stage_cleanup_without_a_final_owner_close_keeps_native_reports(self):
        with self.worker(backend="daft-native-local-model-cascade"), \
             patch("src.experiments.lotus_cascade_movie.run_movie_cascade_query", return_value=dict(status="completed")):
            del self.owner.close
            self.invoke()
            self.assertEqual(self.saved[0]["backend_close"], self.owner.close_reports)
            self.assertNotIn("owner_close", self.saved[0])
            self.runtime.shutdown.assert_not_called()


if __name__ == "__main__":
    unittest.main()
