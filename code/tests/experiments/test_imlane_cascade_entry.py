"""Generic public cascades over original LOTUS and a fixture IMLane author.

All child and socket allocation is prohibited. The in-memory engine supplies
declared responses through the existing author carrier and production kernel.
"""

from contextlib import redirect_stdout
from copy import deepcopy
from dataclasses import asdict
from functools import partial
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid


OPTIONAL_PROBE_DENIALS = []
_GUARDS = []


def setUpModule():
    os.environ.update(HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
        LITELLM_LOCAL_MODEL_COST_MAP="True", CUDA_VISIBLE_DEVICES="", RUN_LOTUS_CASCADE_CPU="1")
    original_run, original_socket = subprocess.run, socket.socket

    def optional_probe(command, *args, **kwargs):
        if command == "lscpu":
            OPTIONAL_PROBE_DENIALS.append("NumPy optional lscpu rejected with OSError; no child launched")
            raise OSError("optional CPU probe unavailable during NoPopen/NoSocket tests")
        return original_run(command, *args, **kwargs)

    class NoSocket(original_socket):
        def __new__(cls, *args, **kwargs):
            raise AssertionError("NoSocket: no author, provider or network connection in public cascade tests")

    for guard in (patch("subprocess.Popen", side_effect=AssertionError("NoPopen: no child/author/model")),
            patch("subprocess.run", side_effect=optional_probe), patch("socket.socket", NoSocket),
            patch("socket.socketpair", side_effect=AssertionError("NoSocket: socketpair prohibited")),
            patch("socket.create_connection", side_effect=AssertionError("NoSocket: connection prohibited"))):
        guard.start()
        _GUARDS.append(guard)


def tearDownModule():
    for guard in reversed(_GUARDS):
        guard.stop()


class MemoryCascadeEngine:
    """Only model responses/physical close are fixtures; no method controller."""

    def __init__(self, spec, generation, *unused, mode="normal"):
        self.spec, self.generation, self.mode = spec, generation, mode
        self.role = "helper" if generation.logprobs is not None else "main"
        self.calls, self.closed = [], False
        self.ready_report = dict(engine_uid="memory-cascade-" + uuid.uuid4().hex,
                                 fixture=True, real_model_requests=0)

    def run_prepared(self, rows):
        from tests.semantic_methods.test_lotus_cascade import terminal

        if self.closed or len(self.calls) + len(rows) > self.spec.max_model_calls:
            raise AssertionError("in-memory stage exceeded its declared instance allowance")
        self.calls.extend(deepcopy(rows))
        results = []
        for row in rows:
            value = terminal(row, self.role, omit_pair=self.mode == "no-pair")
            value.update(engine_uid=self.ready_report["engine_uid"],
                messages_sha256=hashlib.sha256(json.dumps(json.loads(row["messages_json"]),
                    ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                usage_source="declared in-memory response; no child or model inference")
            if self.mode == "failed-row" and row["source_position"] == 4:
                value.update(status="failed", text=None, finish_reason="abort", error_type="DeclaredCPUFailure")
            if self.mode == "bad-logs" and row["source_position"] == 4:
                value["logprobs_json"] = "[]"
            results.append(value)
        return results

    def close(self):
        confirmed = self.mode != "failed-close"
        self.closed = confirmed
        return dict(status="closed" if confirmed else "unknown", process_exit_confirmed=True,
            cleanup_errors=[], gpu_exit_confirmed=None, engine_ready=self.ready_report,
            fixture=True, real_model_requests=0,
            engine_api=dict(status="closed" if confirmed else "unknown",
                engine_api_shutdown_confirmed=confirmed, reserved_model_calls=len(self.calls)))


class IMLaneCascadeEntryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import lotus
        import pandas as pd
        from src.experiments import lotus_records_cascade, lotus_records_chain
        from src.semantic_methods.lotus import cascade
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        from tests.integrations import test_imlane_cascade
        from tests.semantic_methods import test_lotus_cascade

        cls.lotus, cls.pd = lotus, pd
        cls.runner, cls.sources, cls.semantic = lotus_records_cascade, lotus_records_chain, cascade
        cls.EngineSpec, cls.GenerationSpec = EngineSpec, GenerationSpec
        cls.fixture, cls.native = test_imlane_cascade, test_lotus_cascade

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="imlane-cascade-public-cpu-", dir="/tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        context = self.lotus.settings.context(enable_cache=False)
        context.__enter__()
        self.addCleanup(context.__exit__, None, None, None)
        guard = patch.object(self.EngineSpec, "validate_driver_environment", return_value=dict(fixture=True))
        guard.start()
        self.addCleanup(guard.stop)
        self.count = 0
        main, helper = self.native.models()
        self.models = dict(main=main, helper=helper)
        self.engines = {}
        for role, model in self.models.items():
            path = self.root / role
            path.mkdir()
            model.tokenizer.save(str(path / "tokenizer.json"))
            self.engines[role] = self.EngineSpec(str(path), sys.executable, sys.executable, max_model_calls=10)

    def write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")
        return path

    def source_value(self, n=10):
        from src.semantic_methods.lotus.record_chain import RecordOccurrence

        columns = ["title", "content", "synopsis", "row_id", "call_id", "precise"]
        records = [RecordOccurrence("row-" + str(i), i, 70 + i // 2,
            dict(title="title " + str(i) + ";", content="content " + str(i) + ";",
                synopsis="summary " + str(i) + ";", row_id="business-row", call_id="business-call",
                precise=1.2345678901234567 + i), source_refs={"document": [i]}).describe() for i in range(n)]
        return dict(schema="semloom.record_relation.v1", records=records, columns=columns,
            source_index=dict(dtype="int64", name="document-label"),
            empty_prototype=[dict(name=name, dtype="float64" if name == "precise" else "object") for name in columns])

    def configuration(self, *, fixed=False):
        from src.semantic_methods.lotus.records import FilterMethod, RecordsResources

        arguments = dict(sampling_percentage=.5, cascade_IS_random_seed=5, cascade_num_calibration_quantiles=5,
                         helper_filter_instruction="Does {title} and {synopsis} support the claim?")
        if fixed:
            arguments.update(filter_pos_cascade_threshold=.8, filter_neg_cascade_threshold=.2)
        generation = asdict(self.GenerationSpec(max_tokens=9, top_p=.95, seed=7))
        return dict(schema="semloom.records_cascade_run.v1", backend="imlane-author-local-model-cascade",
            query_id="public-cascade-query", method=FilterMethod("openai/fixture-main",
                "Do {content} and {title} support the claim?").describe(), helper_model_name="openai/fixture-helper",
            engines={role: asdict(engine) for role, engine in self.engines.items()},
            generations=dict(main=generation, helper=dict(generation, logprobs=10)), cascade_args=arguments,
            resources=asdict(RecordsResources(prepare_batch_rows=3, model_batch_rows=2,
                max_active_batches=2, query_timeout_s=120, startup_timeout_s=15)),
            max_calls=25, source_sha256="a" * 64, owner_sha256="b" * 64)

    def read_config(self, value):
        return self.runner.read_config(self.write("config.json", value))

    def inputs(self, *, config=None, source=None):
        path = self.write("source.json", source or self.source_value())
        config = self.configuration() if config is None else deepcopy(config)
        config["source_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        values = self.read_config(config)
        relation = self.sources.read_relation(path, expected_sha256=config["source_sha256"])
        return (values[0], relation, *values[1:])

    def execute(self, *, config=None, source=None, mode="normal", proxy=None, cancelled=None,
                author_cleanup_failure=False):
        from integrations.imlane_records.owner import StopBudget

        values = self.inputs(config=config, source=source)
        config, relation, method, generations, resources, engines, args = values
        self.count += 1
        output = self.root / ("query-" + str(self.count))
        output.mkdir()
        author = SimpleNamespace(output=output, stop_budget=StopBudget(threading.Event()))
        environment = {"CUDA_VISIBLE_DEVICES": "-1"}
        made = []

        def factory(source, **kwargs):
            backend = self.fixture.FixtureBackend(source, engine_type=partial(MemoryCascadeEngine, mode=mode),
                author_cleanup_failure=author_cleanup_failure, **kwargs)
            made.append(backend)
            return backend if proxy is None else proxy(backend)

        with patch.object(self.runner, "_make_backend", side_effect=factory):
            try:
                report = self.runner.run_query(*values, author, environment, cancelled=cancelled)
            except BaseException as error:
                error.fixture_backends, error.fixture_inputs = made, values
                raise
        return report, values, made[0]

    def reference(self, values, *, omit_pair=False):
        _, relation, method, _, _, _, args = values
        columns = tuple(dict.fromkeys((*self.lotus.nl_expression.parse_cols(method.instruction),
            *self.lotus.nl_expression.parse_cols(args.helper_filter_instruction or method.instruction))))
        frame = self.pd.DataFrame([dict(row_id=row.row_id, source_position=row.source_position,
            **{column: row.values[column] for column in columns}) for row in relation.records],
            columns=["row_id", "source_position", *columns])
        reference = self.native.CascadeDifferentialTests(methodName="runTest")
        reference.frame = frame
        return reference.reference(args, instruction=method.instruction, omit_pair=omit_pair,
            default=method.default, system_prompt=method.system_prompt,
            model_kwargs=dict(presence_penalty=0.0, frequency_penalty=0.0))

    def result_frame(self, report):
        result = report["result"]
        frame = self.pd.DataFrame(result["data"], columns=result["columns"], index=self.pd.Index(
            result["index"], dtype=result["index_dtype"], name=result["index_name"]))
        for name, dtype in zip(result["columns"], result["dtypes"], strict=True):
            frame[name] = frame[name].astype(dtype)
        return frame

    def assert_native(self, report, values, backend, *, omit_pair=False):
        from src.semantic_methods.lotus.engine import lm_report
        from src.semantic_methods.lotus.record_chain import records_to_frame

        native, stats, calls, sampled, main, helper = self.reference(values, omit_pair=omit_pair)
        decisions = native["_filter"].tolist()
        self.assertEqual([row["decision"] for row in report["decisions"]], decisions)
        self.assertEqual(report["stats"], {key: value.model_dump(mode="json") if hasattr(value, "model_dump") else value
                                         for key, value in stats.items()})
        self.assertEqual([backend.prepared[row["call_id"]] for row in report["calls"]["records"]], calls)
        self.assertEqual(report["lm"], dict(main=lm_report(main), helper=lm_report(helper)))
        self.assertNotIn("positive_review_cnt", report)
        relation = values[1]
        source = records_to_frame(relation.records, columns=relation.columns, source_index=relation.source_index)
        expected = source.iloc[[i for i, decision in enumerate(decisions) if decision]]
        self.pd.testing.assert_frame_equal(self.result_frame(report), expected, check_exact=True)
        kept = [row.describe() for row, decision in zip(relation.records, decisions, strict=True) if decision]
        self.assertEqual(report["output_records"], kept)
        if sampled:
            event = next(row for row in report["calls"]["events"] if row["event"] == "sample_selected")
            self.assertEqual(event["indices"], sampled)
        return sampled

    def test_explicit_generic_config_and_role_generations_roundtrip(self):
        config, method, generations, resources, engines, args = self.read_config(self.configuration())
        self.assertEqual(config["schema"], "semloom.records_cascade_run.v1")
        self.assertIn("{content}", method.instruction)
        self.assertEqual(generations["helper"].logprobs, 10)
        self.assertIsNone(generations["main"].logprobs)
        self.assertNotEqual(engines["main"].model_path, engines["helper"].model_path)
        self.assertEqual(resources.max_active_batches, 2)
        self.assertIn("{synopsis}", args.helper_filter_instruction)

    def test_required_schema_fields_roles_and_unknown_fields_rejected(self):
        invalid = []
        for name in self.configuration():
            value = self.configuration()
            del value[name]
            invalid.append(value)
        invalid += [dict(self.configuration(), schema="old"), dict(self.configuration(), typo=True),
                    dict(self.configuration(), backend="imlane-author-local-model-chain")]
        for name in ("engines", "generations"):
            value = self.configuration()
            del value[name]["helper"]
            invalid.append(value)
        value = self.configuration()
        del value["method"]["instruction"]
        invalid.append(value)
        for value in invalid:
            with self.subTest(keys=list(value)), self.assertRaises((ValueError, TypeError)):
                self.read_config(value)

    def test_helper_logs_and_ordinary_generation_options_are_strict(self):
        invalid = []
        for logs in (None, True, 0, 21, 1.5):
            value = self.configuration()
            value["generations"]["helper"]["logprobs"] = logs
            invalid.append(value)
        for role, update in (("main", dict(logprobs=1)), ("helper", dict(response_format=dict(type="json_object"))),
                             ("main", dict(repetition_penalty=1.1))):
            value = self.configuration()
            value["generations"][role].update(update)
            invalid.append(value)
        for value in invalid:
            with self.subTest(value=value["generations"]), self.assertRaises((ValueError, TypeError)):
                self.read_config(value)

    def test_global_cap_stage_caps_resource_window_and_raw_hash_are_bounded(self):
        invalid = []
        for cap in (None, True, 0, -1, 12289):
            invalid.append(dict(self.configuration(), max_calls=cap))
        for cap in (None, True, 0, 4097):
            value = self.configuration()
            value["engines"]["helper"]["max_model_calls"] = cap
            invalid.append(value)
        for name, bad in (("prepare_batch_rows", 17), ("model_batch_rows", 17),
                          ("max_active_batches", 3), ("cpu_workers", 2), ("gpu_workers", 2)):
            value = self.configuration()
            value["resources"][name] = bad
            invalid.append(value)
        for name in ("source_sha256", "owner_sha256"):
            for bad in (None, "", "z" * 64, "a" * 63):
                invalid.append(dict(self.configuration(), **{name: bad}))
        for value in invalid:
            with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                self.read_config(value)

    def test_distinct_deployments_and_unchanged_original_column_order_required(self):
        value = self.configuration()
        value["engines"]["helper"]["model_path"] = value["engines"]["main"]["model_path"]
        with self.assertRaisesRegex(ValueError, "distinct"):
            self.read_config(value)
        value = self.configuration()
        original = list(self.lotus.nl_expression.parse_cols(value["method"]["instruction"]))
        value["method"]["resolved_input_columns"] = list(reversed(original))
        with self.assertRaisesRegex(ValueError, "order"):
            self.read_config(value)

    def test_original_true_false_output_labels_cannot_be_reconfigured(self):
        for labels in (("Yes", "No"), ("False", "True"), ("True",), "True"):
            config = self.configuration()
            config["method"]["output_tokens"] = labels
            with self.subTest(labels=labels), self.assertRaises((TypeError, ValueError)):
                self.read_config(config)

    def preflight(self, values, *, limit=25):
        owner = dict(budget=dict(limit=limit))
        with patch.object(self.runner, "preflight_owner", return_value=dict(fixture_owner=True)), \
             patch.object(self.runner, "_make_backend") as backend:
            result = self.runner.preflight(*values, owner)
        backend.assert_not_called()
        return result

    def test_pure_preflight_uses_global_upper_and_separate_role_columns(self):
        values = self.inputs()
        result = self.preflight(values)
        self.assertEqual(result["max_possible_calls"], 25)
        self.assertEqual(result["max_stage_calls"], dict(helper=10, main=10))
        self.assertEqual(result["inference_calls"], 0)
        self.assertFalse(result["permanent_unit_claimed"])
        self.assertEqual(set(result["input_columns"]), {"title", "content", "synopsis"})
        self.assertTrue(result["effective_generation"]["helper"]["logprobs"])
        self.assertFalse(result["effective_generation"]["main"]["logprobs"])

    def test_preflight_rejects_missing_null_text_bad_policy_and_insufficient_allowance(self):
        for name, change in (("missing-helper-column", lambda c, s: c["cascade_args"].update(helper_filter_instruction="Use {missing}?")),
                             ("null-helper-text", lambda c, s: s["records"][0]["values"].update(synopsis=None)),
                             ("query-cap", lambda c, s: c.update(max_calls=24)),
                             ("stage-cap", lambda c, s: c["engines"]["main"].update(max_model_calls=9)),
                             ("probability", lambda c, s: c["cascade_args"].update(sampling_percentage=1.1))):
            config, source = self.configuration(), self.source_value()
            change(config, source)
            with self.subTest(name=name), self.assertRaises((ValueError, TypeError)):
                self.preflight(self.inputs(config=config, source=source))
        with self.assertRaisesRegex(ValueError, "permanent"):
            self.preflight(self.inputs(), limit=24)

    def test_preflight_rejects_deployed_multitoken_output_label_without_backend(self):
        from tokenizers import Tokenizer, models

        values = self.inputs()
        tokenizer = Tokenizer(models.BPE({"[UNK]": 0, **{c: i + 1 for i, c in enumerate("TrueFals")}},
                                         [], unk_token="[UNK]"))
        tokenizer.save(str(self.root / "helper/tokenizer.json"))
        with self.assertRaisesRegex(ValueError, "tokenizer|label"):
            self.preflight(values)

    def test_unicode_field_allowance_is_checked_without_truncation_or_backend(self):
        from src.baselines.text.cascade_records_source import MAX_FIELD_CHARS

        source = self.source_value(1)
        text = "字" * MAX_FIELD_CHARS
        source["records"][0]["values"]["content"] = text
        checked = self.preflight(self.inputs(config=self.configuration(fixed=True), source=source))
        self.assertEqual(checked["max_possible_calls"], 2)
        self.assertEqual(source["records"][0]["values"]["content"], text)
        for field in ("text", "row_id", "column"):
            config, source = self.configuration(fixed=True), self.source_value(1)
            oversized = "字" * (MAX_FIELD_CHARS + 1)
            if field == "text":
                source["records"][0]["values"]["synopsis"] = oversized
            elif field == "row_id":
                source["records"][0]["row_id"] = oversized
            else:
                source["columns"].append(oversized)
                source["empty_prototype"].append(dict(name=oversized, dtype="object"))
                source["records"][0]["values"][oversized] = "value"
                config["method"]["instruction"] = "Use {" + oversized + "}?"
                config["cascade_args"]["helper_filter_instruction"] = "Use {" + oversized + "}?"
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "field"):
                self.preflight(self.inputs(config=config, source=source))

    def test_learned_generic_global_control_matches_original_lotus(self):
        report, values, backend = self.execute()
        sampled = self.assert_native(report, values, backend)
        self.assertEqual(report["status"], "completed")
        self.assertLess(len(set(sampled)), len(sampled))
        self.assertEqual([row["algorithm_stage"] for row in report["backend_close"]],
                         ["helper_score", "threshold_sample", "fallback"])
        for worker in backend.workers:
            expected = tuple(self.lotus.nl_expression.parse_cols(
                values[2].instruction if worker.stage.model_role == "main" else values[-1].helper_filter_instruction))
            self.assertEqual(worker.stage.input_columns, expected)
            self.assertTrue(worker.closed)
            self.assertIsNone(worker.close_report["gpu_exit_confirmed"])
        self.assertEqual(len({worker.kernel.ready_report["engine_uid"] for worker in backend.workers}), 3)

    def test_repeated_global_draws_keep_distinct_canonical_call_ids(self):
        report, _, backend = self.execute()
        samples = backend.workers[1].stage.records
        self.assertLess(len({(row["row_id"], row["source_position"]) for row in samples}), len(samples))
        self.assertEqual(len({row["call_id"] for row in samples}), len(samples))
        self.assertTrue(all(row["model_role"] == "main" and row["algorithm_stage"] == "threshold_sample" for row in samples))
        for row in report["calls"]["records"]:
            self.assertTrue(row["call_id"].startswith("lotus-cascade-v1:"))
            self.assertEqual(row["query_id"], "public-cascade-query")

    def test_physical_batches_preserve_one_global_calibration(self):
        first, _, _ = self.execute()
        config = self.configuration()
        config["resources"].update(prepare_batch_rows=1, model_batch_rows=1)
        second, _, _ = self.execute(config=config)
        for key in ("decisions", "stats"):
            self.assertEqual(first[key], second[key])
        self.assertEqual(first["calls"]["events"], second["calls"]["events"])
        self.assertEqual(first["calls"]["usage"], second["calls"]["usage"])

    def test_fixed_thresholds_match_original_and_do_not_sample(self):
        report, values, backend = self.execute(config=self.configuration(fixed=True))
        self.assert_native(report, values, backend)
        self.assertNotIn("threshold_sample", [worker.stage.name for worker in backend.workers])
        self.assertNotIn("sample_selected", [row["event"] for row in report["calls"]["events"]])

    def test_missing_true_false_top_pair_retains_original_generated_token_fallback(self):
        report, values, backend = self.execute(config=self.configuration(fixed=True), mode="no-pair")
        self.assert_native(report, values, backend, omit_pair=True)

    def test_empty_relation_preserves_declared_frame_and_never_opens_author(self):
        report, values, backend = self.execute(source=self.source_value(0))
        relation = values[1]
        self.assertEqual(report["decisions"], [])
        self.assertEqual(report["calls"]["records"], [])
        self.assertTrue(report["empty_input_extension"])
        self.assertEqual(report["output_records"], [])
        self.assertEqual(backend.workers, [])
        self.pd.testing.assert_frame_equal(self.result_frame(report), relation.empty_prototype)

    def test_bad_helper_logs_keep_all_usage_and_block_main(self):
        with self.assertRaises((RuntimeError, ValueError)) as caught:
            self.execute(mode="bad-logs")
        report = caught.exception.query_report
        backend = caught.exception.fixture_backends[0]
        self.assertEqual(report["status"], "failed")
        self.assertEqual(len(backend.workers), 1)
        self.assertEqual(report["calls"]["usage"]["prompt_tokens"], 175)
        self.assertNotIn("global_calibration_completed", [row["event"] for row in report["calls"]["events"]])

    def test_known_helper_failure_retains_native_ledger_and_no_main_owner(self):
        with self.assertRaises((RuntimeError, ValueError)) as caught:
            self.execute(mode="failed-row")
        report = caught.exception.query_report
        backend = caught.exception.fixture_backends[0]
        self.assertEqual(len(backend.workers), 1)
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 75)
        self.assertEqual(report["calls"]["usage"]["known_completion_tokens"], 5)
        self.assertTrue(any(row.get("error_type") == "DeclaredCPUFailure" for row in report["calls"]["records"]))

    def test_unconfirmed_model_close_retains_known_calls_and_stops_role_switch(self):
        with self.assertRaises(RuntimeError) as caught:
            self.execute(mode="failed-close")
        report = caught.exception.query_report
        backend = caught.exception.fixture_backends[0]
        self.assertEqual(len(backend.workers), 1)
        self.assertEqual(report["calls"]["usage"]["prompt_tokens"], 175)
        self.assertFalse(report["backend_close"][0]["stage_process_exit_confirmed"])
        self.assertIsNotNone(backend._active)
        self.assertFalse(backend.workers[0].closed)

    def test_author_close_failure_remains_failed_after_physical_fixture_settlement(self):
        with self.assertRaises(RuntimeError) as caught:
            self.execute(author_cleanup_failure=True)
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["calls"]["usage"]["prompt_tokens"], 175)
        self.assertTrue(report["backend_close"][0]["author_physical_resources_confirmed"])
        self.assertFalse(report["backend_close"][0]["stage_process_exit_confirmed"])

    def test_metadata_getter_cannot_replace_primary_or_discard_known_usage(self):
        observed = {}
        original = self.semantic.run_cascade
        def control(*args, **kwargs):
            try:
                return original(*args, **kwargs)
            except BaseException as error:
                observed["primary"] = error
                raise
        class MetadataFailure:
            def __init__(self, backend):
                self.backend = backend
            def __getattr__(self, name):
                return getattr(self.backend, name)
            def __call__(self, stage):
                return self.backend(stage)
            @property
            def close_reports(self):
                observed["metadata_read"] = True
                raise RuntimeError("declared cascade metadata failure")
        with patch.object(self.semantic, "run_cascade", side_effect=control), self.assertRaises((RuntimeError, ValueError)) as caught:
            self.execute(mode="bad-logs", proxy=MetadataFailure)
        self.assertIs(caught.exception, observed["primary"])
        self.assertTrue(observed["metadata_read"])
        self.assertEqual(caught.exception.query_report["calls"]["usage"]["prompt_tokens"], 175)

    def test_success_then_metadata_failure_is_failed_with_original_calls(self):
        observed = {}
        original = self.semantic.run_cascade
        def control(*args, **kwargs):
            report = original(*args, **kwargs)
            observed["calls"] = report["calls"]
            observed["calls_snapshot"] = deepcopy(report["calls"])
            return report
        class MetadataFailure:
            def __init__(self, backend):
                self.backend = backend
            def __getattr__(self, name):
                return getattr(self.backend, name)
            def __call__(self, stage):
                return self.backend(stage)
            @property
            def close_reports(self):
                raise RuntimeError("declared cascade metadata failure")
        with patch.object(self.semantic, "run_cascade", side_effect=control), \
             self.assertRaisesRegex(RuntimeError, "metadata failure") as caught:
            self.execute(proxy=MetadataFailure)
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertIs(report["calls"], observed["calls"])
        self.assertEqual(report["calls"], observed["calls_snapshot"])

    def test_unknown_close_return_survives_metadata_failure_with_original_primary(self):
        observed = {}
        original = self.semantic.run_cascade
        def control(*args, **kwargs):
            try:
                return original(*args, **kwargs)
            except BaseException as error:
                observed["primary"] = error
                observed["calls"] = error.query_report["calls"]
                observed["calls_snapshot"] = deepcopy(error.query_report["calls"])
                raise
        class UnknownCloseAndMetadataFailure:
            def __init__(self, backend):
                self.backend = backend
            def __getattr__(self, name):
                return getattr(self.backend, name)
            def __call__(self, stage):
                return self.backend(stage)
            def close(self):
                closed = self.backend.close()
                observed["close_return"] = [*closed, dict(status="unknown",
                    algorithm_stage="threshold_sample", model_role="main",
                    stage_process_exit_confirmed=False, startup_error_type="DeclaredStartupFailure")]
                return observed["close_return"]
            @property
            def close_reports(self):
                observed["metadata_read"] = True
                raise RuntimeError("declared metadata unavailable after unknown close return")
        with patch.object(self.semantic, "run_cascade", side_effect=control), \
             self.assertRaises((RuntimeError, ValueError)) as caught:
            self.execute(mode="bad-logs", proxy=UnknownCloseAndMetadataFailure)
        self.assertIs(caught.exception, observed["primary"])
        self.assertTrue(observed["metadata_read"])
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertIs(report["calls"], observed["calls"])
        self.assertEqual(report["calls"], observed["calls_snapshot"])
        self.assertEqual(report["calls"]["usage"]["prompt_tokens"], 175)
        self.assertEqual(report["owner_close"], observed["close_return"])
        self.assertTrue(report["owner_close"][0]["stage_process_exit_confirmed"])
        self.assertTrue(report["owner_close"][0]["author_physical_resources_confirmed"])
        self.assertEqual(report["owner_close"][-1]["status"], "unknown")
        self.assertFalse(report["owner_close"][-1]["stage_process_exit_confirmed"])
        observed["close_return"][0]["later_fixture_mutation"] = True
        self.assertNotIn("later_fixture_mutation", report["owner_close"][0])
        self.assertTrue(any("owner close also failed" in note for note in caught.exception.__notes__))
        self.assertTrue(any("metadata backend_close also failed" in note for note in caught.exception.__notes__))

    def test_cancel_before_preflight_allocates_no_author(self):
        with self.assertRaises(InterruptedError) as caught:
            self.execute(cancelled=lambda: True)
        self.assertEqual(caught.exception.fixture_backends[0].workers, [])

    def cli_files(self):
        source = self.write("source.json", self.source_value())
        owner = self.write("owner.json", dict(fixture_owner=True))
        config = self.configuration()
        config.update(source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                      owner_sha256=hashlib.sha256(owner.read_bytes()).hexdigest())
        return self.write("config.json", config), source, owner

    def test_check_and_missing_execute_with_hidden_worker_only_preflight(self):
        for action, extra in (("record-cascade-check", []), ("record-cascade-run", []),
                              ("record-cascade-run", ["--worker"])):
            with self.subTest(action=action, extra=extra):
                config, source, owner = self.cli_files()
                argv = [action, "--config", str(config), "--source-json", str(source), "--owner-config", str(owner)]
                if action.endswith("-run"):
                    argv += ["--output-dir", str(self.root / "must-not-exist"), *extra]
                before, output = set(self.root.rglob("*")), io.StringIO()
                with patch.object(self.runner, "read_owner", return_value=dict(fixture_owner=True)), \
                     patch.object(self.runner, "preflight", return_value=dict(status="preflight_only", inference_calls=0)) as checked, \
                     patch.object(self.runner, "_make_backend") as backend, \
                     patch.object(self.runner, "run_worker") as worker, redirect_stdout(output):
                    self.assertEqual(self.runner.main(argv), 0)
                self.assertEqual(json.loads(output.getvalue())["inference_calls"], 0)
                checked.assert_called_once()
                backend.assert_not_called()
                worker.assert_not_called()
                self.assertEqual(set(self.root.rglob("*")), before)

    def test_raw_source_and_owner_drift_rejected_before_preflight(self):
        for name in ("source", "owner"):
            config, source, owner = self.cli_files()
            changed = source if name == "source" else owner
            changed.write_text(changed.read_text() + "\n")
            with self.subTest(name=name), patch.object(self.runner, "preflight") as checked, \
                 patch.object(self.runner, "_make_backend") as backend, self.assertRaisesRegex(ValueError, "changed"):
                self.runner.main(["record-cascade-check", "--config", str(config), "--source-json", str(source),
                                  "--owner-config", str(owner)])
            checked.assert_not_called()
            backend.assert_not_called()

    def test_shared_movie_entry_forwards_both_explicit_generic_cascade_actions(self):
        from src.experiments import lotus_records_movie
        for action in ("record-cascade-check", "record-cascade-run"):
            argv = [action, "--config", "fixture-config", "--source-json", "fixture-source", "--owner-config", "fixture-owner"]
            with self.subTest(action=action), patch.object(self.runner, "main", return_value=0) as forwarded:
                self.assertEqual(lotus_records_movie.main(argv), 0)
                forwarded.assert_called_once_with(argv)


if __name__ == "__main__":
    unittest.main(verbosity=2)
