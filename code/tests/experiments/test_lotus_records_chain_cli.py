"""Public ordinary-chain contracts over real LOTUS and a declared fixture author.

This module never runs the fixture's CPU child or an author/model process.
"""

from contextlib import redirect_stdout
from dataclasses import asdict
from functools import partial
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch


OPTIONAL_PROBE_DENIALS = []
_GUARDS = []


def setUpModule():
    os.environ.update(HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
                      LITELLM_LOCAL_MODEL_COST_MAP="True", CUDA_VISIBLE_DEVICES="")
    original_run = subprocess.run

    def optional_probe(command, *args, **kwargs):
        if command == "lscpu":
            OPTIONAL_PROBE_DENIALS.append("NumPy optional lscpu unavailable: OSError; no child launched")
            raise OSError("NumPy optional CPU probe unavailable in NoPopen qualification")
        return original_run(command, *args, **kwargs)

    for guard in (patch("subprocess.Popen", side_effect=AssertionError(
            "NoPopen: public-chain tests must not launch any child/author/model")),
            patch("subprocess.run", side_effect=optional_probe)):
        guard.start()
        _GUARDS.append(guard)


def tearDownModule():
    for guard in reversed(_GUARDS):
        guard.stop()


class PublicChainTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import lotus
        import pandas as pd
        from src.experiments import lotus_records_chain
        from src.serving.backends.vllm_engine import EngineSpec
        from tests.integrations import test_imlane_chain_backend

        cls.lotus, cls.pd, cls.runner = lotus, pd, lotus_records_chain
        cls.EngineSpec, cls.fixture = EngineSpec, test_imlane_chain_backend

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="imlane-chain-public-cpu-", dir="/tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        context = self.lotus.settings.context(enable_cache=False)
        context.__enter__()
        self.addCleanup(context.__exit__, None, None, None)
        guard = patch.object(self.EngineSpec, "validate_driver_environment", return_value={"fixture": True})
        guard.start()
        self.addCleanup(guard.stop)
        self.count = 0

    def write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")
        return path

    def source_value(self, records=None, *, index_dtype="int64", columns=None, dtypes=None):
        records = self.fixture.inputs() if records is None else records
        columns = list(records[0].values) if columns is None and records else list(columns or ("title", "body"))
        dtypes = dtypes or {name: "object" for name in columns}
        return dict(schema="semloom.record_relation.v1", records=[record.describe() for record in records],
            columns=columns, source_index=dict(dtype=index_dtype, name="document-label"),
            empty_prototype=[dict(name=name, dtype=dtypes[name]) for name in columns])

    def config_value(self, kinds=("map",), *, source_sha="a" * 64, owner_sha="b" * 64):
        from src.semantic_methods.lotus.records import RecordsResources
        chain = self.fixture.plan(kinds, query="public-chain-query")
        return dict(schema="semloom.records_chain_run.v1", backend="imlane-author-local-model-chain",
            chain=chain.describe(), engine=asdict(self.EngineSpec(str(self.root), sys.executable, sys.executable,
                                                                max_model_calls=64)),
            resources=asdict(RecordsResources(model_batch_rows=1, max_active_batches=2, query_timeout_s=90)),
            source_sha256=source_sha, owner_sha256=owner_sha)

    def read_config(self, value):
        return self.runner.read_config(self.write("config.json", value))

    def read_source(self, value):
        return self.runner.read_relation(self.write("source.json", value))

    def query_inputs(self, kinds=("map",), source=None):
        source = self.source_value() if source is None else source
        path = self.write("source.json", source)
        config = self.config_value(kinds, source_sha=hashlib.sha256(path.read_bytes()).hexdigest())
        raw, chain, engine, resources = self.read_config(config)
        return raw, self.runner.read_relation(path), chain, engine, resources

    def run_query(self, kinds=("map",), *, source=None, engine_type=None, proxy=None, author_fail=False,
                  cancelled=None):
        from integrations.imlane_records.owner import StopBudget
        raw, relation, chain, engine, resources = self.query_inputs(kinds, source)
        self.count += 1
        output = self.root / ("query-" + str(self.count))
        output.mkdir()
        args = SimpleNamespace(output=output, stop_budget=StopBudget(threading.Event()))
        environment = {"CUDA_VISIBLE_DEVICES": "-1"}
        backend = self.fixture.FixtureBackend(engine=engine, resources=resources,
            author_args=args, model_environment=environment,
            engine_type=engine_type or self.fixture.FixtureEngine, author_fail=author_fail, cancelled=cancelled)
        actual_evaluate = backend.evaluate
        streams = []

        def evaluate(*args, **kwargs):
            value = actual_evaluate(*args, **kwargs)
            streams.append(value)
            return value if proxy is None else proxy(value)

        with patch.object(backend, "evaluate", side_effect=evaluate), \
             patch.object(self.runner, "_make_backend", return_value=backend):
            try:
                report = self.runner.run_query(raw, relation, chain, engine, resources, args, environment,
                                               cancelled=cancelled)
            except BaseException as error:
                error.fixture_backend, error.fixture_streams = backend, streams
                raise
        return report, relation, chain, backend, streams

    def report_frame(self, report):
        value = report["result"]
        frame = self.pd.DataFrame(value["data"], columns=value["columns"],
            index=self.pd.Index(value["index"], dtype=value["index_dtype"], name=value["index_name"]))
        dtypes = value["dtypes"]
        if type(dtypes) is list:
            dtypes = dict(zip(value["columns"], dtypes, strict=True))
        for name, dtype in dtypes.items():
            frame[name] = frame[name].astype(dtype)
        return frame

    def original(self, chain, relation):
        from src.semantic_methods.lotus.record_chain import records_to_frame
        if relation.records:
            source = records_to_frame(relation.records, columns=relation.columns, source_index=relation.source_index)
        else:
            source = self.pd.DataFrame({name: self.pd.Series(dtype=(relation.empty_prototype[name].dtype
                if name in relation.empty_prototype else "object")) for name in relation.columns},
                index=relation.source_index[:0])
        return self.fixture.IMLaneChainBackendTests.original(self, chain, source.copy(deep=True))

    def test_explicit_schema_backend_and_stage_generation_roundtrip(self):
        raw, chain, engine, resources = self.read_config(self.config_value(("map", "extract", "filter")))
        self.assertEqual(raw["schema"], "semloom.records_chain_run.v1")
        self.assertEqual(raw["backend"], "imlane-author-local-model-chain")
        self.assertEqual([stage.kind for stage in chain.stages], ["map", "extract", "filter"])
        self.assertEqual(chain.stages[1].generation.response_format, {"type": "json_object"})
        self.assertEqual((engine.max_model_calls, resources.model_batch_rows, resources.max_active_batches), (64, 1, 2))

    def test_config_required_fields_unknown_keys_and_other_backends_are_rejected(self):
        invalid = []
        for name in ("schema", "backend", "chain", "engine", "resources", "source_sha256", "owner_sha256"):
            value = self.config_value()
            del value[name]
            invalid.append(value)
        invalid += [dict(self.config_value(), typo=True), dict(self.config_value(), schema="old"),
                    dict(self.config_value(), backend="semloom-local-model-core")]
        for value in invalid:
            with self.subTest(keys=list(value)), self.assertRaises((ValueError, TypeError)):
                self.read_config(value)

    def test_config_digest_and_finite_engine_allowance_are_strict(self):
        invalid = []
        for key in ("source_sha256", "owner_sha256"):
            for bad in (None, True, "", "a" * 63, "z" * 64):
                invalid.append(dict(self.config_value(), **{key: bad}))
        for cap in (None, True, 0, -1, 4097):
            value = self.config_value()
            value["engine"]["max_model_calls"] = cap
            invalid.append(value)
        for value in invalid:
            with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                self.read_config(value)

    def test_source_preserves_duplicate_index_business_names_and_parent_metadata(self):
        relation = self.read_source(self.source_value())
        self.assertIsInstance(relation.records, tuple)
        self.assertEqual(list(relation.source_index), [17, 17, 29])
        self.assertEqual(str(relation.source_index.dtype), "int64")
        self.assertEqual(relation.source_index.name, "document-label")
        self.assertEqual(relation.records[0].values["row_id"], "business")
        self.assertEqual(relation.records[0].source_refs, {"source": [0]})
        self.assertEqual(len(relation.empty_prototype), 0)
        self.assertEqual(list(relation.empty_prototype.columns), list(relation.columns))

    def test_source_int_object_and_string_index_preserve_label_types(self):
        for dtype, labels in (("int64", [17, 17, 29]), ("object", [17, "17", 29]),
                              ("string", ["17", "17", "29"])):
            value = self.source_value(index_dtype=dtype)
            for record, label in zip(value["records"], labels, strict=True):
                record["source_index"] = label
            with self.subTest(dtype=dtype):
                relation = self.read_source(value)
                self.assertEqual(list(relation.source_index), labels)
                self.assertEqual([type(item) for item in relation.source_index], [type(item) for item in labels])

    def test_source_empty_named_business_column_remains_ordinary_data(self):
        value = self.source_value()
        value["columns"].append("")
        value["empty_prototype"].append(dict(name="", dtype="object"))
        for row in value["records"]:
            row["values"][""] = "business extra key"
        relation = self.read_source(value)
        self.assertEqual(relation.columns[-1], "")
        self.assertEqual(relation.records[0].values[""], "business extra key")

    def test_source_rejects_schema_extras_invalid_columns_and_duplicate_occurrences(self):
        invalid = []
        for name in ("schema", "records", "columns", "source_index", "empty_prototype"):
            value = self.source_value()
            del value[name]
            invalid.append(value)
        invalid += [dict(self.source_value(), schema="old"), dict(self.source_value(), typo=True),
                    dict(self.source_value(), columns=["body", "body"])]
        value = self.source_value()
        value["records"][1]["row_id"] = value["records"][0]["row_id"]
        invalid.append(value)
        value = self.source_value()
        value["records"][1]["source_position"] = value["records"][0]["source_position"]
        invalid.append(value)
        value = self.source_value()
        value["columns"] = True
        invalid.append(value)
        for value in invalid:
            with self.subTest(keys=list(value)), self.assertRaises((ValueError, TypeError)):
                self.read_source(value)

    def test_declared_source_order_is_extended_without_dropping_payload_columns(self):
        value = self.source_value()
        value["columns"] = ["body"]
        relation = self.read_source(value)
        self.assertEqual(relation.columns, ("body", "title", "row_id", "call_id"))

    def test_source_rejects_boolean_none_multiindex_and_index_type_coercion(self):
        invalid = []
        for label in (None, True, [17, "group"]):
            value = self.source_value()
            value["records"][0]["source_index"] = label
            invalid.append(value)
        for dtype in ("bool", "float64", "Int64", "category", None):
            value = self.source_value()
            value["source_index"]["dtype"] = dtype
            invalid.append(value)
        value = self.source_value(index_dtype="string")
        invalid.append(value)  # Numeric labels must not silently become strings.
        value = self.source_value()
        value["records"][0]["source_index"] = "17"
        invalid.append(value)  # Numeric-looking strings must not become integers.
        for value in invalid:
            with self.subTest(value=value["source_index"]), self.assertRaises((ValueError, TypeError)):
                self.read_source(value)

    def test_source_rejects_pending_index_updates_and_nonfinite_values(self):
        value = self.source_value()
        value["records"][0]["index_updates"] = {"field": "unconsumed"}
        with self.assertRaises((ValueError, TypeError)):
            self.read_source(value)
        path = self.root / "source.json"
        value = self.source_value()
        value["records"][0]["values"]["extra"] = float("nan")
        path.write_text(json.dumps(value))
        with self.assertRaises((ValueError, TypeError)):
            self.runner.read_relation(path)

    def test_empty_prototype_dtype_whitelist_and_explicit_final_order(self):
        types = ["object", "string", "int64", "Int64", "uint64", "UInt64", "float64", "Float64", "bool", "boolean"]
        columns = ["column_" + str(i) for i in range(len(types))]
        relation = self.read_source(self.source_value([], columns=columns, dtypes=dict(zip(columns, types, strict=True))))
        self.assertEqual([str(dtype) for dtype in relation.empty_prototype.dtypes], types)
        self.assertEqual(list(relation.empty_prototype.columns), columns)
        for bad in ("category", "datetime64[ns]", "complex128", "not-a-dtype", None):
            value = self.source_value([])
            value["empty_prototype"][0]["dtype"] = bad
            with self.subTest(dtype=bad), self.assertRaises((ValueError, TypeError)):
                self.read_source(value)
        value = self.source_value([])
        value["empty_prototype"].reverse()
        relation = self.read_source(value)
        self.assertEqual(list(relation.empty_prototype.columns), ["body", "title"])

    def test_map_extract_me_and_extract_filter_match_original_lotus(self):
        for kinds in (("map",), ("extract",), ("map", "extract"), ("extract", "filter")):
            with self.subTest(kinds=kinds):
                report, relation, chain, backend, streams = self.run_query(kinds)
                expected, messages = self.original(chain, relation)
                self.pd.testing.assert_frame_equal(self.report_frame(report), expected)
                self.assertEqual(report["schema"], "semloom.records_chain_query.v1")
                self.assertEqual(report["status"], "completed")
                self.assertEqual(report["query_id"], chain.query_id)
                self.assertEqual(report["calls"], streams[0].calls)
                self.assertEqual(len(report["output_records"]), len(expected))
                declared = {row.row_id: row for row in relation.records}
                self.assertEqual(len({row["row_id"] for row in report["output_records"]}), len(expected))
                for row in report["output_records"]:
                    original = declared[row["row_id"]]
                    self.assertEqual((row["source_position"], row["source_index"], row["source_refs"]),
                                     (original.source_position, original.source_index, original.source_refs))
                self.assertIsNone(report["backend_close"]["gpu_exit_confirmed"])
                self.assertTrue(all(worker.closed for worker in backend.workers))
                for stage in chain.stages:
                    calls = sorted((row for row in report["calls"] if row["algorithm_stage"] == stage.stage_id),
                                   key=lambda row: row["source_position"])
                    self.assertEqual([backend.messages[row["call_id"]] for row in calls], messages[stage.stage_id])

    def test_extract_filter_keeps_grouped_parent_ids_for_duplicate_index(self):
        report, _, chain, backend, _ = self.run_query(("extract", "filter"))
        prefix = {row["call_id"] for row in report["calls"] if row["algorithm_stage"] == chain.stages[0].stage_id
                  and row["source_position"] in (0, 1)}
        later = [row for row in report["calls"] if row["algorithm_stage"] == chain.stages[1].stage_id
                 and row["source_position"] in (0, 1)]
        self.assertEqual(len(later), 2)
        self.assertTrue(all(set(row["parent_call_ids"]) == prefix for row in later))
        self.assertEqual(report["result"]["index"], [17, 17])
        self.assertEqual(len(backend.workers), 2)

    def test_result_json_preserves_ordinary_float64_values_exactly(self):
        source = self.source_value()
        source["columns"].append("precise")
        source["empty_prototype"].append(dict(name="precise", dtype="float64"))
        source["columns"].append("unsigned")
        source["empty_prototype"].append(dict(name="unsigned", dtype="uint64"))
        values = [1.2345678901234567, 9e-20, -12345.678901234567]
        integers = [2**64 - 1, 2**63 + 17, 0]
        for row, value, integer in zip(source["records"], values, integers, strict=True):
            row["values"]["precise"] = value
            row["values"]["unsigned"] = integer
        report, relation, chain, _, _ = self.run_query(("map",), source=source)
        result = json.loads(json.dumps(report["result"], allow_nan=False))
        for name, expected_values in (("precise", values), ("unsigned", integers)):
            column = result["columns"].index(name)
            self.assertEqual([row[column] for row in result["data"]], expected_values)
        expected, _ = self.original(chain, relation)
        self.pd.testing.assert_frame_equal(self.report_frame(report), expected, check_exact=True)

    def test_empty_relation_retains_original_sdk_result_dtypes_without_model(self):
        source = self.source_value([])
        expected, _ = self.original(self.fixture.plan(("map", "extract"), query="public-chain-query"),
                                    self.read_source(source))
        source["empty_prototype"] = [dict(name=name, dtype=str(dtype)) for name, dtype in expected.dtypes.items()]
        report, relation, chain, backend, _ = self.run_query(("map", "extract"), source=source)
        expected, _ = self.original(chain, relation)
        self.pd.testing.assert_frame_equal(self.report_frame(report), expected)
        self.assertEqual(report["calls"], [])
        self.assertEqual(report["output_records"], [])
        self.assertTrue(all(worker.batch.engine is None for worker in backend.workers))

    def test_incorrect_final_empty_prototype_fails_without_model_or_lost_close(self):
        with self.assertRaisesRegex(ValueError, "empty_prototype") as caught:
            self.run_query(("map", "extract"), source=self.source_value([]))
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["calls"], [])
        self.assertEqual(report["backend_close"]["status"], "closed")
        self.assertTrue(all(worker.batch.engine is None for worker in caught.exception.fixture_backend.workers))

    def test_known_failure_retains_terminal_usage_and_blocks_successor(self):
        from src.baselines.text.frameworks.lotus_chain_results import ChainExecutionError
        with self.assertRaises(ChainExecutionError) as caught:
            self.run_query(("extract", "filter"), engine_type=partial(self.fixture.FixtureEngine, fail=True))
        error = caught.exception
        report = error.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(len(error.fixture_backend.workers), 1)
        self.assertEqual(len(report["calls"]), 2)
        self.assertEqual(sum(row["prompt_tokens"] for row in report["calls"]), 10)
        self.assertEqual({row["status"] for row in report["calls"]}, {"succeeded", "failed"})
        self.assertIn("backend_close", report)

    def test_failed_close_keeps_completed_calls_and_physical_api_scope(self):
        with self.assertRaises(RuntimeError) as caught:
            self.run_query(("extract", "filter"), author_fail=True)
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertTrue(report["calls"])
        self.assertEqual(sum(row["prompt_tokens"] for row in report["calls"]), 15)
        self.assertEqual(report["backend_close"]["status"], "unknown")
        self.assertIsNone(report["backend_close"]["gpu_exit_confirmed"])

    def test_metadata_failure_cannot_replace_primary_or_drop_known_usage(self):
        from src.baselines.text.frameworks.lotus_chain_results import ChainExecutionError
        evidence = dict(metadata_reads=[], primary=None)
        class MetadataFailure:
            def __init__(self, stream):
                self.stream = stream
            def __iter__(self):
                try:
                    yield from self.stream
                except BaseException as error:
                    evidence["primary"] = error
                    raise
            def __getattr__(self, name):
                return getattr(self.stream, name)
            @property
            def last_close_report(self):
                evidence["metadata_reads"].append("last_close_report")
                raise RuntimeError("declared public-chain metadata getter failure")
            @property
            def phase_history(self):
                evidence["metadata_reads"].append("phase_history")
                raise RuntimeError("declared public-chain metadata getter failure")
        with self.assertRaises(ChainExecutionError) as caught:
            self.run_query(("extract", "filter"), engine_type=partial(self.fixture.FixtureEngine, fail=True),
                           proxy=MetadataFailure)
        report = caught.exception.query_report
        self.assertIs(caught.exception, evidence["primary"])
        self.assertTrue(evidence["metadata_reads"])
        self.assertEqual(report["status"], "failed")
        self.assertEqual(sum(row["prompt_tokens"] for row in report["calls"]), 10)
        self.assertTrue(any(row["status"] == "failed" for row in report["calls"]))
        self.assertTrue(all(worker.closed for worker in caught.exception.fixture_backend.workers))

    def test_cancel_before_author_allocation_preserves_interrupted_error(self):
        with self.assertRaises(InterruptedError) as caught:
            self.run_query(("map",), cancelled=lambda: True)
        self.assertEqual(caught.exception.fixture_backend.workers, [])
        self.assertEqual(caught.exception.query_report["calls"], [])
        self.assertEqual(caught.exception.query_report["status"], "failed")

    def test_successful_model_results_then_metadata_failure_report_failed_with_usage(self):
        class MetadataFailure:
            def __init__(self, stream):
                self.stream = stream
            def __iter__(self):
                return iter(self.stream)
            def __getattr__(self, name):
                return getattr(self.stream, name)
            @property
            def phase_history(self):
                raise RuntimeError("declared metadata failure after successful model fixture")
        with self.assertRaisesRegex(RuntimeError, "metadata failure") as caught:
            self.run_query(("map",), proxy=MetadataFailure)
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(len(report["calls"]), 3)
        self.assertEqual(sum(row["prompt_tokens"] for row in report["calls"]), 15)
        self.assertTrue(all(row["status"] == "succeeded" for row in report["calls"]))
        self.assertEqual(report["backend_close"]["status"], "closed")
        self.assertTrue(all(worker.closed for worker in caught.exception.fixture_backend.workers))
        self.assertEqual(report["metadata_errors"][0]["field"], "phase_history")

    def cli_files(self):
        source = self.write("source.json", self.source_value())
        owner = self.write("owner.json", dict(fixture_owner=True))
        config = self.write("config.json", self.config_value(
            source_sha=hashlib.sha256(source.read_bytes()).hexdigest(),
            owner_sha=hashlib.sha256(owner.read_bytes()).hexdigest()))
        return config, source, owner

    def test_cli_check_and_run_without_execute_only_preflight_and_write_nothing(self):
        for action, extra in (("chain-check", []), ("chain-run", []), ("chain-run", ["--worker"])):
            with self.subTest(action=action, extra=extra):
                config, source, owner = self.cli_files()
                argv = [action, "--config", str(config), "--source-json", str(source), "--owner-config", str(owner)]
                if action == "chain-run":
                    argv += ["--output-dir", str(self.root / "must-not-exist"), *extra]
                before = set(self.root.rglob("*"))
                output = io.StringIO()
                with patch.object(self.runner, "read_owner", return_value=dict(fixture_owner=True)), \
                     patch.object(self.runner, "preflight", return_value=dict(status="preflight_only", inference_calls=0)) as checked, \
                     patch.object(self.runner, "run_worker") as worker, \
                     patch.object(self.runner, "_make_backend") as factory, redirect_stdout(output):
                    self.assertEqual(self.runner.main(argv), 0)
                self.assertEqual(json.loads(output.getvalue())["status"], "preflight_only")
                checked.assert_called_once()
                worker.assert_not_called()
                factory.assert_not_called()
                self.assertEqual(set(self.root.rglob("*")), before)

    def test_cli_rejects_source_or_owner_byte_drift_before_preflight_or_backend(self):
        for target in ("source", "owner"):
            with self.subTest(target=target):
                config, source, owner = self.cli_files()
                changed = source if target == "source" else owner
                changed.write_text(changed.read_text() + "\n")
                with patch.object(self.runner, "preflight") as checked, \
                     patch.object(self.runner, "_make_backend") as factory, \
                     self.assertRaisesRegex(ValueError, "changed"):
                    self.runner.main(["chain-check", "--config", str(config), "--source-json", str(source),
                                      "--owner-config", str(owner)])
                checked.assert_not_called()
                factory.assert_not_called()

    def test_relation_expected_digest_checks_same_bytes_as_deserialization(self):
        path = self.write("source.json", self.source_value())
        expected = hashlib.sha256(path.read_bytes()).hexdigest()
        self.runner.read_relation(path, expected_sha256=expected)
        path.write_text(path.read_text() + "\n")
        with self.assertRaises(ValueError):
            self.runner.read_relation(path, expected_sha256=expected)

    def test_existing_movie_entry_forwards_both_chain_actions(self):
        from src.experiments import lotus_records_movie
        for action in ("chain-check", "chain-run"):
            argv = [action, "--config", "fixture-config", "--source-json", "fixture-source",
                    "--owner-config", "fixture-owner"]
            with self.subTest(action=action), patch.object(self.runner, "main", return_value=0) as forwarded:
                self.assertEqual(lotus_records_movie.main(argv), 0)
                forwarded.assert_called_once_with(argv)


if __name__ == "__main__":
    unittest.main(verbosity=2)
