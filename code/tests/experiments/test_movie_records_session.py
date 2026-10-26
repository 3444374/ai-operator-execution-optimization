"""Deterministic CPU lifecycle tests; real model/network/process startup forbidden."""

from dataclasses import asdict, replace
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.experiments import movie_records_session as session
from src.experiments import native_query_lifecycle as lifecycle
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from src.semantic_methods.lotus.records import RecordsResources


SOURCE = Path(__file__).resolve().parents[3]


def fixture_module(name, relative):
    spec = importlib.util.spec_from_file_location(name, SOURCE / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Clock:
    def __init__(self):
        self.value = 10_000_000_000
    def __call__(self):
        return self.value
    def advance(self, seconds):
        self.value += int(seconds * 1e9)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.output = Path(temporary.name) / "run"
        self.clock, self.drivers = Clock(), []
        self.query_error = self.close_error = self.start_error = None
        self.omit_result = False
        self.foreign = {"alive": True}
        self.queries = [dict(query_id="q" + str(i), task="task-" + str(i),
                             phase="warmup" if i == 0 else "measured") for i in range(3)]
        test = self
        class Driver:
            def __init__(self, directory, first_query):
                self.owned = {"alive": False}
                self.starts = self.closes = 0
                self.calls = []
                test.drivers.append(self)
            def start(self):
                self.starts += 1
                self.owned["alive"] = True
                test.clock.advance(100)
                if test.start_error:
                    raise test.start_error
                return {"engine_uid": "cpu-fixture-" + str(len(test.drivers)), "physical_model": False}
            def query(self, query, emit):
                calls = []  # Each logical task owns a fresh ledger/result collection.
                for name, duration in (("read", 5), ("prepare", 7), ("operator", 11), ("aggregate", 13)):
                    emit(name + "_start")
                    test.clock.advance(duration)
                    emit(name + "_end")
                calls.append(query["query_id"])
                self.calls.append(calls)
                if test.query_error and query["query_id"] == "q1":
                    test.query_error.query_report = {"status": "failed", "known_calls": 2, "unknown_calls": 1}
                    raise test.query_error
                if not test.omit_result:
                    emit("result_ready")
                test.clock.advance(17)
                return {"status": "completed", "calls": calls, "cache_enabled": False}
            def close(self):
                self.closes += 1
                self.owned["alive"] = False
                test.clock.advance(29)
                if test.close_error:
                    raise test.close_error
                return {"status": "closed", "cleanup_scope": "fixture_owned_only"}
        self.factory = Driver

    def run_session(self, lifecycle="resident"):
        return session.run_sequence(self.queries, self.factory, self.output, backend="semloom-core",
                                    lifecycle=lifecycle, clock=self.clock)

    def test_resident_reads_each_full_query_loads_once_and_separates_time_boundaries(self):
        report = self.run_session()
        self.assertEqual(report["status"], "completed")
        self.assertEqual(len(self.drivers), 1)
        self.assertEqual((self.drivers[0].starts, self.drivers[0].closes), (1, 1))
        self.assertEqual(report["owners"][0]["startup_seconds"], 100)
        self.assertEqual(report["owners"][0]["shutdown_seconds"], 29)
        self.assertEqual([q["full_query_seconds"] for q in report["queries"]], [36, 36, 36])
        self.assertEqual([q["post_result_seconds"] for q in report["queries"]], [17, 17, 17])
        self.assertEqual(report["application_e2e_s"], 100 + 3 * (36 + 17) + 29)
        self.assertIsNone(report["model_load_count_observed"])
        self.assertFalse(report["performance_claim"])
        self.assertLessEqual(report["owners"][0]["owner_ready_ns"], report["queries"][0]["query_start_ns"])
        self.assertLessEqual(report["queries"][-1]["query_terminal_ns"], report["owners"][0]["owner_close_start_ns"])
        self.assertEqual([q["phase"] for q in report["queries"]], ["warmup", "measured", "measured"])

    def test_different_tasks_keep_fresh_ledgers_and_one_owner(self):
        report = self.run_session()
        self.assertEqual([q["backend_report"]["calls"] for q in report["queries"]], [["q0"], ["q1"], ["q2"]])
        self.assertEqual(len({q["owner_id"] for q in report["queries"]}), 1)
        self.assertTrue(self.foreign["alive"])

    def test_isolated_mode_keeps_loading_inside_each_query_and_closes_each_owner(self):
        report = self.run_session("isolated")
        self.assertEqual(report["owner_start_count"], 3)
        self.assertEqual([q["full_query_seconds"] for q in report["queries"]], [136, 136, 136])
        self.assertEqual([driver.closes for driver in self.drivers], [1, 1, 1])
        starts = [event for event in report["events"] if event["event"] == "query_start"]
        self.assertTrue(all(event["owner_id"] is None for event in starts))
        for query in report["queries"]:
            owner = next(owner for owner in report["owners"] if owner["owner_id"] == query["owner_id"])
            self.assertLessEqual(query["events"][0]["monotonic_ns"], owner["owner_start_ns"])

    def test_failure_stops_later_queries_and_preserves_original_receipts_before_cleanup(self):
        self.query_error = ValueError("primary failure password=cpu-secret")
        with self.assertRaises(ValueError) as caught:
            self.run_session()
        self.assertIs(caught.exception, self.query_error)
        report = caught.exception.session_report
        self.assertEqual(report["skipped_queries"], ["q2"])
        self.assertEqual(report["queries"][1]["backend_report"]["known_calls"], 2)
        persisted = json.loads((self.output / "query-1.json").read_text())
        self.assertEqual(persisted["backend_report"]["unknown_calls"], 1)
        self.assertNotIn("cpu-secret", (self.output / "session.json").read_text())
        self.assertFalse(self.drivers[0].owned["alive"])
        self.assertTrue(self.foreign["alive"])

    def test_cleanup_failure_does_not_replace_query_failure(self):
        self.query_error, self.close_error = ValueError("primary"), RuntimeError("cleanup")
        with self.assertRaises(ValueError) as caught:
            self.run_session()
        report = caught.exception.session_report
        self.assertIs(caught.exception, self.query_error)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["owners"][0]["status"], "unknown")
        self.assertEqual(report["cleanup_failure"]["error"], "cleanup")
        self.assertEqual(self.drivers[0].closes, 1)

    def test_partial_start_failure_is_closed_without_running_a_query(self):
        self.start_error = RuntimeError("startup")
        with self.assertRaises(RuntimeError) as caught:
            self.run_session()
        self.assertEqual(caught.exception.session_report["skipped_queries"], ["q0", "q1", "q2"])
        self.assertEqual(self.drivers[0].calls, [])
        self.assertEqual(self.drivers[0].closes, 1)

    def test_missing_result_boundary_is_a_failure(self):
        self.omit_result = True
        with self.assertRaisesRegex(ValueError, "complete-result") as caught:
            self.run_session()
        self.assertFalse(caught.exception.session_report["queries"][0]["complete_result_observed"])
        self.assertEqual(caught.exception.session_report["skipped_queries"], ["q1", "q2"])

    def test_unsupported_residency_rejects_before_resources_or_output(self):
        for backend in ("ray-data", "daft"):
            with self.subTest(backend=backend), self.assertRaisesRegex(ValueError, "no cross-query"):
                session.run_sequence(self.queries, self.factory, self.output, backend=backend)
        self.assertEqual(self.drivers, [])
        self.assertFalse(self.output.exists())

    def test_existing_evidence_is_never_overwritten(self):
        self.run_session()
        original = (self.output / "session.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.run_session()
        self.assertEqual((self.output / "session.json").read_bytes(), original)

    def test_recording_failure_preserves_primary_exception(self):
        self.query_error = ValueError("primary")
        write = session.publish_private_json
        def record(path, value):
            if Path(path).name == "query-1.json":
                raise OSError("recording")
            return write(path, value)
        with patch.object(lifecycle, "publish_private_json", side_effect=record):
            with self.assertRaises(ValueError) as caught:
                self.run_session()
        self.assertIs(caught.exception, self.query_error)
        self.assertIn("query recording also failed", " ".join(caught.exception.__notes__))


class CoreReadinessTests(unittest.TestCase):
    def test_public_start_loads_before_source_and_two_queries_reuse_the_real_core(self):
        fixture = fixture_module("session_core_fixture", "code/tests/execution_provider/test_lotus_records_core.py")
        case = fixture.CoreRecordsTests()
        case.setUp()
        with fixture.Runtime(case.rows) as runtime:
            owner = case.owner()
            ready = owner.start()
            self.assertEqual(len(runtime.engines), 1)
            self.assertFalse(any(event[0] == "source_row" for event in runtime.events))
            self.assertIs(owner.start(), ready)
            first = list(owner.evaluate(fixture.Source(case.rows), query_id="first"))
            second = list(owner.evaluate(fixture.Source(case.rows), query_id="second"))
            self.assertEqual(len(runtime.engines), 1)
            self.assertEqual({row["query_id"] for row in first}, {"first"})
            self.assertEqual({row["query_id"] for row in second}, {"second"})
            self.assertFalse({row["call_id"] for row in first} & {row["call_id"] for row in second})
            self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
            self.assertEqual(owner.close()["status"], "closed")
            self.assertEqual(runtime.engines[0].closes, 1)
            with self.assertRaises(RuntimeError):
                owner.start()


class PlanTests(unittest.TestCase):
    def setUp(self):
        from src.baselines.text.movie_records import prepare_movie_source
        import csv
        import hashlib
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        csv_path = root / "Reviews.csv"
        with csv_path.open("w") as stream:
            writer = csv.writer(stream)
            writer.writerow(["reviewId", "id", "reviewText", "scoreSentiment"])
            writer.writerows([["1", "taken_3", "good", "POSITIVE"], ["2", "other", "bad", "NEGATIVE"]])
        digest = hashlib.sha256(csv_path.read_bytes()).hexdigest()
        manifests = {task: str(prepare_movie_source(csv_path, root / task, expected_csv_sha256=digest,
                                                  max_rows=2, task=task)) for task in ("movie-q1", "movie-q2", "movie-q3")}
        self.plan = dict(schema="semloom.movie_session_plan.v1", backend="semloom-core", lifecycle="resident",
            model_name="openai/cpu-fixture", generation=asdict(GenerationSpec()),
            engine=asdict(EngineSpec("/tmp/model", "/tmp/model-python", "/tmp/driver-python", max_model_calls=4)),
            resources=asdict(RecordsResources(prepare_batch_rows=1, model_batch_rows=1)), profile={},
            queries=[dict(query_id="q1", task="movie-q1", manifest=manifests["movie-q1"], phase="warmup"),
                     dict(query_id="q2", task="movie-q2", manifest=manifests["movie-q2"], phase="measured")])
        self.manifests = manifests

    def test_q1_q2_share_preparation_method_with_fresh_task_counts(self):
        result = session.check_plan(self.plan)
        self.assertEqual([query["expected_calls"] for query in result["queries"]], [2, 1])
        self.assertEqual(result["inference_calls"], 0)

    def test_core_differing_instructions_are_bound_per_query_in_one_session(self):
        self.plan["queries"][1].update(task="movie-q3", manifest=self.manifests["movie-q3"])
        checked = session.check_plan(self.plan)
        self.assertEqual([q["expected_calls"] for q in checked["queries"]], [2, 1])
        self.plan["lifecycle"] = "isolated"
        self.assertEqual(session.check_plan(self.plan)["status"], "checked_cpu_only")

    def test_engine_limit_covers_all_warmup_and_measured_queries(self):
        self.plan["engine"]["max_model_calls"] = 2
        with self.assertRaisesRegex(ValueError, "whole session"):
            session.check_plan(self.plan)

    def test_owner_api_validates_fresh_claims_before_allocating_and_restores_device_environment(self):
        from contextlib import ExitStack
        import os
        from src.experiments.cell_budget import ClaimedUnit, Reservation
        from integrations.imlane_records import chain_runtime
        from src.experiments import lotus_records_movie
        from types import SimpleNamespace
        claims = {"q1": ClaimedUnit(Reservation("q1", 1, 2), None),
                  "q2": ClaimedUnit(Reservation("q2", 3, 1), None)}
        context = dict(owner_context={}, claims=claims, sembench_checkout="cpu-fixture",
            runtime_binding=dict(driver_python="/tmp/driver-python", model_python="/tmp/model-python",
                                 model_path="/tmp/model", cuda_visible_devices="0"))
        parent = Path(self.manifests["movie-q1"]).parent.parent
        with ExitStack() as stack:
            stack.enter_context(patch.object(chain_runtime, "check_external_lease", return_value={}))
            stack.enter_context(patch.object(EngineSpec, "validate_driver_environment", return_value={}))
            stack.enter_context(patch.object(lotus_records_movie, "model_preflight", return_value={}))
            loop = stack.enter_context(patch.object(session, "run_sequence", return_value={"status": "completed"}))
            prior = os.environ.get("CUDA_VISIBLE_DEVICES")
            session.run_movie_session(self.plan, context, parent / "owner-api")
            self.assertEqual(os.environ.get("CUDA_VISIBLE_DEVICES"), prior)
            self.assertEqual(loop.call_count, 1)
            setup = loop.call_args.kwargs["setup"]
            self.assertEqual(setup["check"]["expected_calls"], 3)
            self.assertGreaterEqual(setup["preflight_seconds"], 0)
            claims["q1"].reserve("0" * 64)
            with self.assertRaisesRegex(ValueError, "fresh"):
                session.run_movie_session(self.plan, context, parent / "owner-api-reuse")
            self.assertEqual(loop.call_count, 1)

    def test_isolated_core_driver_binds_the_current_query_method_and_leaves_foreign_ray_alive(self):
        from contextlib import ExitStack
        from types import SimpleNamespace
        from src.execution_provider.adapters import lotus_records_core
        import sys
        self.plan["lifecycle"] = "isolated"
        self.plan["queries"][1].update(task="movie-q3", manifest=self.manifests["movie-q3"])
        checked = session.check_plan(self.plan)
        state = {"initialized": False, "shutdowns": 0}
        def initialize(**kwargs):
            state["initialized"] = True
        def shutdown():
            state["initialized"] = False
            state["shutdowns"] += 1
        ray = SimpleNamespace(is_initialized=lambda: state["initialized"], init=initialize, shutdown=shutdown)
        parent = Path(self.manifests["movie-q1"]).parent.parent
        with ExitStack() as stack:
            stack.enter_context(patch.dict(sys.modules, {"ray": ray}))
            owner_type = stack.enter_context(patch.object(lotus_records_core, "SemLoomRecordsCore"))
            owner_type.return_value.close.return_value = {"status": "closed"}
            owner_type.return_value.start.return_value = {"engine_uid": "cpu-fixture"}
            driver = session._MovieDriver(self.plan, {}, parent / "isolated-q3", self.plan["queries"][1])
            driver.start()
            from src.baselines.text.movie_tasks import MovieTask
            self.assertEqual(owner_type.call_args.args[0].instruction, MovieTask(3).instruction)
            self.assertEqual(driver.close()["status"], "closed")
            self.assertEqual(state["shutdowns"], 1)
            state["initialized"] = True
            foreign = session._MovieDriver(self.plan, {}, parent / "foreign", self.plan["queries"][0])
            with self.assertRaisesRegex(RuntimeError, "existing Ray"):
                foreign.start()
            self.assertEqual(foreign.close()["status"], "closed")
            self.assertTrue(state["initialized"])
            self.assertEqual(state["shutdowns"], 1)


if __name__ == "__main__":
    unittest.main()
