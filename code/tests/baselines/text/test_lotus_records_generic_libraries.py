"""Opt-in four-backend ordinary Filter parity with real CPU libraries only."""

from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


@unittest.skipUnless(os.environ.get("RUN_LOTUS_RECORDS_CPU_LIBRARIES") == "1"
                     and os.environ.get("SEMBENCH_CHECKOUT"),
                     "explicit CPU-library qualification and fixed SemBench checkout required")
class GenericRecordsLibrariesTests(unittest.TestCase):
    def test_four_backends_preserve_generic_columns_method_and_optional_predicate(self):
        import daft
        import lotus
        import pandas as pd
        import ray
        import ray.data
        from ray.data.dataset import Dataset
        from src.baselines.common.private_artifacts import write_private_json
        from src.baselines.common.redact import redact_json_values
        from src.baselines.text.frameworks import daft_lotus_records as native_daft
        from src.baselines.text.frameworks import ray_data_lotus_records as native_ray
        from src.execution_provider.adapters import lotus_records_actor_owner as actors
        from src.execution_provider.adapters import lotus_records_core as core
        from src.execution_provider.adapters.lotus_records_static import SemLoomRecordsStaticReference
        from src.scheduling.core.session import SessionEngine
        from src.scheduling.runtime.async_backend import BoundedAsyncBackend
        from src.semantic_methods.lotus.engine import engine_response, interpret_results, lm_report, make_lm
        from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordScan, RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        from tests.baselines.text.generic_records_fixture import (
            CpuCoreActor, CpuEngineProcess, CpuInferBatch, CpuStaticActor, CsvFilterFixture,
        )
        from tests.baselines.text.generic_prepare_fixture import (
            CheckedFilterMethod, CheckedPrepareActor, CheckedPrepareBatch,
        )

        self.assertEqual((ray.__version__, daft.__version__), ("2.56.1", "0.7.21"))
        self.assertFalse(ray.is_initialized(), "this test must own a fresh local CPU runtime")
        method = FilterMethod("openai/local-fixture",
                              instruction="Does {heading} describe urgent work in {body}?")
        driver_seed = os.environ.get("PYTHONHASHSEED")
        driver_order = lotus.nl_expression.parse_cols(method.instruction)
        worker_seed = worker_order = None
        for candidate in range(1, 17):
            if str(candidate) == driver_seed:
                continue
            probe = subprocess.run([
                sys.executable, "-B", "-c",
                "import json,sys;print(json.dumps(list(set(json.loads(sys.argv[1])))))",
                json.dumps(["heading", "body"]),
            ], env=dict(os.environ, PYTHONHASHSEED=str(candidate)), capture_output=True,
                text=True, check=True, timeout=3)
            observed = json.loads(probe.stdout)
            if observed != driver_order:
                worker_seed, worker_order = str(candidate), observed
                break
        self.assertIsNotNone(worker_seed, "fixture must exercise a different original worker column order")
        child_environment = dict(PYTHONHASHSEED=worker_seed,
                                 RECORDS_EXPECTED_CHILD_HASHSEED=worker_seed,
                                 RECORDS_EXPECTED_COLUMN_ORDER=json.dumps(driver_order))
        generation = GenerationSpec()
        scan = RecordScan((("row_id", "string"), ("source_position", "int64"),
                           ("heading", "string"), ("body", "string"), ("bucket", "int64")),
                          ("heading", "body"), ("bucket", 2))
        raw = [dict(row_id="note-000", source_position=0, heading="Emergency", body="repair heater",
                    bucket=2, oracle="DO_NOT_SEND_ORACLE"),
               dict(row_id="note-001", source_position=1, heading="NULL", body="001",
                    bucket=2, oracle="DO_NOT_SEND_ORACLE"),
               dict(row_id="note-002", source_position=2, heading="Excluded", body="unused body",
                    bucket=7, oracle="DO_NOT_SEND_ORACLE"),
               dict(row_id="note-003", source_position=3, heading="None", body="line\nbreak",
                    bucket=2, oracle="DO_NOT_SEND_ORACLE")]
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                                     max_active_batches=2, query_timeout_s=90, startup_timeout_s=60)
        artifacts = os.environ.get("RECORDS_GENERIC_CPU_ARTIFACTS")

        def save(name, value):
            if artifacts:
                write_private_json(Path(artifacts) / name, redact_json_values(value))

        def check_original(source, query_id, results):
            expected = source.expected(query_id)
            ledger = CallLedger(expected, max_calls=len(expected))
            for row in results:
                ledger.record(row)
            self.assertEqual(ledger.report()["counts"]["succeeded"], len(expected))
            by_call = {row["call_id"]: row for row in results}
            ordered = [by_call[item["call_id"]] for item in expected]
            lm = make_lm(method, generation)
            with lotus.settings.context(lm=lm, enable_cache=False):
                decisions = interpret_results(ordered, lm, method)
            reference = make_lm(method, generation)
            captured = []

            def fixed(uncached, kwargs, *_):
                self.assertEqual(len(uncached), len(ordered))
                captured.extend(message for message, _ in uncached)
                return [engine_response(row, reference) for row in ordered]

            reference._process_uncached_messages = fixed
            frame = pd.DataFrame(source.selected(), columns=scan.payload_columns)
            with lotus.settings.context(lm=reference, enable_cache=False):
                native = frame.sem_filter(method.instruction, return_all=True, default=method.default,
                                          output_tokens=method.output_tokens,
                                          system_prompt=method.system_prompt)
            self.assertEqual([row["decision"] for row in decisions], native["_filter"].tolist())
            self.assertEqual(sum(row["decision"] for row in decisions), 1)
            hashes = [hashlib.sha256(json.dumps(messages, ensure_ascii=False, sort_keys=True,
                                                separators=(",", ":")).encode()).hexdigest()
                      for messages in captured]
            self.assertEqual([row["messages_sha256"] for row in ordered], hashes)
            self.assertEqual(lm_report(lm)["stats"], lm_report(reference)["stats"])
            self.assertEqual(ledger.report()["usage"]["total_tokens"], 4 * len(expected))
            return dict(status="passed", query_id=query_id, source=source.preflight(method),
                        scan=source.scan.describe(), method=asdict(method), result_count=1,
                        calls=ledger.report(), lm=lm_report(lm),
                        original_reference=dict(messages_equal=True, decisions_equal=True, count_equal=True,
                                                lm_stats_equal=True), actual_model_calls=0)

        original_remote, original_map_batches = ray.remote, Dataset.map_batches
        original_graph = native_daft._graph
        allocations = []

        class CpuActorOptions:
            def __init__(self, replacement, kind):
                self.replacement, self.kind = replacement, kind

            def options(self, **kwargs):
                assert kwargs["num_gpus"] == 1 and kwargs["max_concurrency"] == 1
                assert kwargs["max_restarts"] == kwargs["max_task_retries"] == 0
                self.settings = dict(kwargs)
                allocations.append(dict(backend=self.kind, requested_gpus=1, fixture_gpus=0))
                return self

            def remote(self, *args):
                return original_remote(self.replacement).options(
                    **dict(self.settings, num_gpus=0)).remote(*args)

        def cpu_remote(*args, **kwargs):
            if len(args) == 1 and not kwargs:
                if args[0] is actors._ModelActor:
                    return CpuActorOptions(CpuStaticActor, "static")
                if args[0] is core._CoreModelActor:
                    return CpuActorOptions(CpuCoreActor, "core")
                if args[0] is actors._PrepareActor:
                    return original_remote(CheckedPrepareActor)
            return original_remote(*args, **kwargs)

        def cpu_map_batches(dataset, fn, *args, **kwargs):
            if fn is CpuInferBatch:
                assert kwargs["num_gpus"] == 1
                allocations.append(dict(backend="ray", requested_gpus=1, fixture_gpus=0))
                kwargs["num_gpus"] = 0
                kwargs["num_cpus"] = 1
            return original_map_batches(dataset, fn, *args, **kwargs)

        def cpu_daft_graph(*args, **kwargs):
            allocations.append(dict(backend="daft", requested_gpus=1, fixture_gpus=0))
            return original_graph(*args, **dict(kwargs, prepare_type=CheckedFilterMethod,
                                               engine_type=CpuEngineProcess, model_gpus=0))

        reports, close_reports, sessions = {}, {}, []
        with patch.dict(os.environ, child_environment), \
                tempfile.TemporaryDirectory(prefix="generic-filter-input-") as inputs, \
                tempfile.TemporaryDirectory(prefix="genericray-", dir="/tmp") as runtime:
            sources = []
            for index, selected_scan in enumerate((scan, replace(scan, predicate=None))):
                directory = Path(inputs) / str(index)
                directory.mkdir()
                sources.append(CsvFilterFixture(directory, method, selected_scan, raw))
            engine = EngineSpec(inputs, sys.executable, sys.executable, max_model_calls=8)
            try:
                ray.init(address="local", num_cpus=4, num_gpus=0, include_dashboard=False,
                         object_store_memory=80 * 1024**2, _temp_dir=runtime,
                         _node_ip_address="127.0.0.1")
                with patch.object(ray, "remote", side_effect=cpu_remote), \
                        patch.object(Dataset, "map_batches", new=cpu_map_batches), \
                        patch.object(native_ray, "InferBatch", CpuInferBatch), \
                        patch.object(native_ray, "PrepareBatch", CheckedPrepareBatch), \
                        patch.object(native_daft, "_graph", side_effect=cpu_daft_graph), \
                        patch.object(EngineSpec, "validate_driver_environment",
                                     return_value={"scope": "explicit zero-GPU CPU fixture"}):
                    for kind in ("ray", "daft", "static", "core"):
                        owner = None
                        if kind == "static":
                            owner = SemLoomRecordsStaticReference(method, engine, generation, resources)
                        elif kind == "core":
                            owner = core.SemLoomRecordsCore(method, engine, generation, resources)
                        reports[kind] = []
                        try:
                            for index, source in enumerate(sources):
                                query_id = "generic-" + kind + "-" + str(index)
                                stream = (owner.evaluate(source, query_id=query_id) if owner is not None else
                                          (native_ray if kind == "ray" else native_daft).evaluate(
                                              source, method, engine, generation, resources, query_id=query_id))
                                try:
                                    results = list(stream)
                                finally:
                                    stream_close = stream.close()
                                save(query_id + "-terminal.json", dict(
                                    query_id=query_id, rows=results, query_close=stream_close,
                                    actual_model_calls=0, usage_source="synthetic CPU fixture",
                                ))
                                report = check_original(source, query_id, results)
                                if kind != "daft":
                                    self.assertTrue(all(row["prepare_python_hash_seed"] == worker_seed
                                                        for row in results))
                                report["query_close"] = stream_close
                                reports[kind].append(report)
                                save(query_id + ".json", report)
                                if kind == "core":
                                    self.assertIsInstance(owner.core, SessionEngine)
                                    self.assertIsInstance(owner.transport.backend, BoundedAsyncBackend)
                                    self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                                    self.assertEqual(owner.transport.retained_refs(), 0)
                                    sessions.append(owner._active.session.session_id)
                                if owner is None:
                                    close_reports[query_id] = stream_close
                        finally:
                            if owner is not None:
                                close_reports[kind] = owner.close()
                        if owner is not None:
                            values = [row for report in reports[kind] for row in report["calls"]["records"]]
                            self.assertEqual(len({row["engine_uid"] for row in values}), 1)
                            model = (close_reports[kind]["engine_close_report"] if kind == "static"
                                     else close_reports[kind]["model_owner"]["engine_close_report"])
                            self.assertTrue(model["engine_api_shutdown_confirmed"])
                            self.assertEqual(model["reserved_model_calls"], 7)
                            self.assertIsNone(close_reports[kind]["gpu_exit_confirmed"])
                        first, second = [report["calls"]["records"] for report in reports[kind]]
                        self.assertTrue({row["call_id"] for row in first}.isdisjoint(row["call_id"] for row in second))
                self.assertEqual(len(set(sessions)), 2)
                fields = ("messages_sha256", "text", "prompt_token_ids_json", "output_token_ids_json",
                          "prompt_tokens", "completion_tokens", "total_tokens", "finish_reason")
                for index in (0, 1):
                    normalized = [{row["row_id"]: {name: row[name] for name in fields}
                                   for row in reports[kind][index]["calls"]["records"]}
                                  for kind in reports]
                    self.assertTrue(all(value == normalized[0] for value in normalized))
                save("generic-close.json", close_reports)
                save("generic-composition.json", dict(
                    status="passed", ray=ray.__version__, daft=daft.__version__,
                    actual_model_calls=0, logical_ray_gpus=0, fixture_request_rows=28,
                    arms=list(reports), queries_per_arm=2, payload_columns=list(scan.payload_columns),
                    predicate_cases=[scan.describe(), replace(scan, predicate=None).describe()],
                    original_messages_decisions_count_and_lm_stats_equal=True,
                    four_backend_results_equal=True, same_engine_per_static_and_core_owner=True,
                    distinct_query_call_ids=True, distinct_core_session_ids=True,
                    driver_launch_hashseed=driver_seed, worker_hashseed=worker_seed,
                    original_driver_column_order=driver_order,
                    original_worker_column_order=worker_order,
                    worker_constructor_checks_distinct_seed_and_order=True,
                    production_core_connected=True, allocations=allocations,
                    cpu_fixture_scope="fixed responses and synthetic usage; no model/GPU/quality benefit",
                ))
            finally:
                ray.shutdown()


if __name__ == "__main__":
    unittest.main()
