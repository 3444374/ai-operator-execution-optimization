"""Core profile selection reaches the existing model owner in both lifecycles."""

from copy import deepcopy
from contextlib import redirect_stdout
from dataclasses import asdict, replace
from io import StringIO
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters import lotus_records_async_actor as async_actor
from src.experiments import movie_records_session as session
from src.experiments import lotus_records_movie as movie
from src.baselines.text.movie_tasks import MovieTask
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.experiments import test_movie_records_session as session_fixture


class SessionProfileWiringTests(unittest.TestCase):
    def test_explicit_async_profile_reaches_existing_owner_in_both_lifecycles(self):
        fixture = session_fixture.PlanTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        for lifecycle in ("isolated", "resident"):
            with self.subTest(lifecycle=lifecycle):
                plan = deepcopy(fixture.plan)
                plan["lifecycle"] = lifecycle
                plan["resources"]["max_active_batches"] = 2
                plan["profile"] = dict(mode="async", max_inflight="auto")
                directory = Path(fixture.manifests["movie-q1"]).parent.parent / lifecycle
                ray = SimpleNamespace(is_initialized=lambda: False, init=lambda **_: None)
                process = SimpleNamespace(pid=41, create_time=lambda: 10.)
                with patch.dict(sys.modules, {"ray": ray}), \
                        patch.object(core, "SemLoomRecordsCore") as owner, \
                        patch.object(async_actor, "AsyncRecordsActorOwner") as actors, \
                        patch("psutil.Process", return_value=process):
                    owner.return_value.start.return_value = dict(pid=41, engine_uid="fixture")
                    driver = session._MovieDriver(plan, {}, directory, plan["queries"][0])
                    driver.start()
                actors.assert_called_once()
                self.assertIs(owner.call_args.kwargs["actor_owner"], actors.return_value)
                self.assertEqual(actors.call_args.args[3].max_active_batches, 2)


class CoreProfileTests(unittest.TestCase):
    def setUp(self):
        self.engine = EngineSpec("/tmp/model", "/tmp/model-python", "/tmp/driver-python", max_num_seqs=4)
        self.resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=8)

    def test_async_request_budget_can_exceed_engine_iteration_capacity(self):
        for initial in (5, 8):
            with self.subTest(initial=initial):
                resolved = core.resolve_core_profile(self.engine, self.resources,
                    dict(mode="async", max_inflight=initial))
                self.assertEqual(resolved["max_inflight"], initial)
                self.assertEqual(resolved["declared_request_budget"], 8)
                self.assertEqual(resolved["engine_max_num_seqs"], 4)
        feedback = core.resolve_core_profile(self.engine, self.resources,
            dict(mode="async", max_inflight=2, admission=dict(kind="resource_feedback")))
        self.assertEqual((feedback["initial_window"], feedback["owner_max_inflight"]), (2, 8))
        self.assertEqual(self.engine.max_num_seqs, 4)
        with self.assertRaises(ValueError):
            core.resolve_core_profile(self.engine, self.resources, dict(mode="async", max_inflight=9))

    def test_auto_selects_declared_request_budget_and_keeps_engine_iteration_capacity(self):
        resolved = core.resolve_core_profile(self.engine, self.resources,
                                             dict(mode="async", max_inflight="auto"))
        self.assertEqual(resolved["max_inflight"], 8)
        self.assertEqual(resolved["capacity_source"], "Core request budget")
        self.assertEqual(resolved["engine_max_num_seqs"], 4)
        smaller = core.resolve_core_profile(self.engine, replace(self.resources, max_active_batches=2),
                                            dict(mode="async", max_inflight="auto"))
        self.assertEqual(smaller["max_inflight"], 2)
        self.assertEqual(self.resources.max_active_batches, 8)

    def test_empty_profile_preserves_the_synchronous_reference_and_rejects_silent_overrides(self):
        resolved = core.resolve_core_profile(self.engine, self.resources, {})
        self.assertEqual(resolved["mode"], "sync")
        self.assertEqual(resolved["max_inflight"], 8)
        self.assertFalse(resolved["stage_timing"])
        for profile in (dict(mode="missing"), dict(mode="sync", max_inflight="auto"),
                        dict(mode="async", max_inflight=9), dict(mode="async", max_inflight=True),
                        dict(mode="async", stage_timing="yes"), dict(auto=True)):
            with self.subTest(profile=profile), self.assertRaises(ValueError):
                core.resolve_core_profile(self.engine, self.resources, profile)

    def test_profile_factory_keeps_method_generation_hardware_and_legacy_owner(self):
        method, generation = MovieTask(3).method("fixture"), GenerationSpec(max_tokens=8)
        with patch.object(core, "SemLoomRecordsCore") as owner, \
                patch.object(async_actor, "AsyncRecordsActorOwner") as actors:
            result = core.create_records_core(method, self.engine, generation, self.resources,
                profile=dict(mode="async", max_inflight=3))
        self.assertIs(result, owner.return_value)
        self.assertEqual(actors.call_args.args[:3], (method, self.engine, generation))
        self.assertEqual(owner.call_args.args[3], replace(self.resources, max_active_batches=3))
        with patch.object(core, "SemLoomRecordsCore") as owner, \
                patch.object(async_actor, "AsyncRecordsActorOwner") as actors:
            core.create_records_core(method, self.engine, generation, self.resources)
        actors.assert_not_called()
        self.assertEqual(owner.call_args.args, (method, self.engine, generation, self.resources))

    def test_direct_actor_injection_reports_its_actual_mode_and_timing(self):
        method, generation = MovieTask(3).method("fixture"), GenerationSpec()
        actors = async_actor.AsyncRecordsActorOwner(method, self.engine, generation, self.resources)
        owner = core.SemLoomRecordsCore(method, self.engine, generation, self.resources,
                                        actor_owner=actors, stage_timing=True)
        metadata = owner.execution_metadata()
        self.assertEqual(metadata["execution_profile"]["mode"], "async")
        self.assertEqual(metadata["execution_profile"]["model_batch_mode"], metadata["model_batch_mode"])
        self.assertEqual(metadata["execution_profile"]["max_inflight"], self.resources.max_active_batches)
        self.assertTrue(metadata["execution_profile"]["stage_timing"])
        reference = core.SemLoomRecordsCore(method, self.engine, generation, self.resources, stage_timing=True)
        self.assertEqual(reference.execution_metadata()["execution_profile"]["mode"], "sync")
        self.assertTrue(reference.execution_metadata()["execution_profile"]["stage_timing"])

    def test_cli_check_reports_effective_profile_for_both_core_selectors_without_allocation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            for backend in ("semloom-local-model-core", "semloom-local-model-core-async"):
                with self.subTest(backend=backend):
                    path.write_text(json.dumps(dict(schema="semloom.movie_records_run.v1", backend=backend,
                        query_id="profile-check", method=asdict(MovieTask(3).method("fixture")),
                        engine=asdict(self.engine), generation=asdict(GenerationSpec()),
                        resources=asdict(self.resources), core_profile=dict(mode="async", max_inflight="auto"))))
                    output = StringIO()
                    source = SimpleNamespace(task=SimpleNamespace(name="movie-q3"))
                    with patch.object(movie.MovieRecordSource, "from_manifest", return_value=source), \
                            patch.object(movie, "model_preflight", return_value=dict(status="software-fixture")), \
                            patch.object(core, "create_records_core") as allocate, redirect_stdout(output):
                        self.assertEqual(movie.main(["check", "--source-manifest", "fixture.json",
                            "--sembench-checkout", "/tmp/checkout", "--config", str(path)]), 0)
                    allocate.assert_not_called()
                    report = json.loads(output.getvalue())
                    self.assertEqual(report["core_execution"]["mode"], "async")
                    self.assertEqual(report["core_execution"]["max_inflight"], 8)
                    self.assertFalse(report["core_execution"]["performance_tuned"])
                    self.assertIsNone(report["requested_async_limits"])


if __name__ == "__main__":
    unittest.main()
