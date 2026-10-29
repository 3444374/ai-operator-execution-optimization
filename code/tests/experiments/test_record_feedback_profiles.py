"""Retained profile names through config, query/session and real CPU IPC."""
from copy import deepcopy
from dataclasses import asdict, replace
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.experiments import lotus_records_movie as movie, movie_records_session as session
from src.experiments.movie_matrix_support import validate_record_admission_cache
from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from tests.experiments import test_records_online_config as online


class WorkCounterProcess(online.CPUFeedbackWithBirth):
    @staticmethod
    def _command(spec, descriptors):
        fixture = Path(__file__).resolve().parents[1] / "serving/work_feedback_fixture.py"
        return [sys.executable, str(fixture), *map(str, descriptors)]


class ProfileEntryTests(unittest.TestCase):
    def fixture(self):
        case = online.OnlineConfigurationTests()
        case.setUp()
        case.engine = replace(case.engine, max_num_seqs=64, max_model_calls=16)
        case.resources = replace(case.resources, max_active_batches=64,
                                 startup_timeout_s=30, query_timeout_s=60)
        return case

    @staticmethod
    def features():
        return dict(work_unit="tokens", input_work=dict(mean=11), output_work=dict(mean=1),
                    cache=dict(enabled=True, scope="one_CPU_fixture_model"))

    def test_normal_config_accepts_both_names_and_rejects_missing_computed_features(self):
        case = self.fixture()
        try:
            for name in ("delivered-work-scan", "computed-work-local-pairs"):
                cfg = deepcopy(case.config())
                cfg.update(core_profile=name, engine=asdict(case.engine), resources=asdict(case.resources))
                cfg.pop("async_limits")
                if name == "computed-work-local-pairs":
                    cfg["admission_cache"]["workload_features"] = self.features()
                path = case.root / (name + ".json")
                path.write_text(json.dumps(cfg))
                value, _, _, resources, engine = movie.read_config(path)
                resolved = core.resolve_core_profile(engine, resources, movie._core_profile(value))
                self.assertEqual((resolved["initial_window"], resolved["owner_max_inflight"]), (8, 64))
                self.assertEqual(resolved["admission"]["poll_interval_s"], .1)
                self.assertEqual(resolved["admission"]["window_s"], .25)
                self.assertEqual(resolved["admission"]["exploration_s"], 10)
            with self.assertRaisesRegex(ValueError, "workload_features"):
                validate_record_admission_cache(case.cache, resolved)
            self.assertNotIn("admission", core.resolve_core_profile(case.engine, case.resources))
        finally:
            case.doCleanups()

    def test_both_retained_session_profiles_warmup_first_reuse_one_model(self):
        for name in ("delivered-work-scan", "computed-work-local-pairs"):
            with self.subTest(profile=name):
                case = self.fixture()
                try:
                    runtime, stack = case.runtime()
                    stack.enter_context(patch.object(AsyncCoreModelActor, "_process_type",
                                                     return_value=WorkCounterProcess))
                    stack.enter_context(patch("integrations.imlane_records.chain_runtime.check_external_lease",
                                              return_value={}))
                    stack.enter_context(patch("psutil.Process", side_effect=lambda pid: SimpleNamespace(
                        pid=pid, create_time=lambda: 1.)))
                    cache = deepcopy(case.cache)
                    if name == "computed-work-local-pairs":
                        cache["workload_features"] = self.features()
                    plan = deepcopy(case.fixture.plan)
                    plan.update(profile=name, admission_cache=cache, engine=asdict(case.engine),
                        resources=asdict(case.resources), queries=[dict(query_id="phase-" + str(i),
                            task="movie-q3", manifest=str(case.source.manifest_path),
                            phase="warmup" if i == 0 else "measured") for i in range(3)])
                    checked = session.check_plan(plan)
                    bindings = {q["query_id"]: q for q in checked["queries"]}
                    driver = session._MovieDriver(plan, dict(owner_context={}, checked_queries=bindings,
                        claims={q["query_id"]: None for q in plan["queries"]}, sembench_checkout="CPU-upstream"),
                        case.root / "resident", plan["queries"][0])
                    driver.start()
                    try:
                        reports = []
                        for declaration in plan["queries"]:
                            report = driver.query(declaration, lambda event: None)
                            reports.append(report)
                            if declaration["phase"] == "warmup":
                                self.assertFalse(Path(cache["cache_dir"]).exists())
                            self.assertEqual(report["status"], "completed")
                            self.assertEqual(report["calls"]["counts"]["succeeded"], 1)
                            self.assertEqual(report["backend_close"]["core_usage"]["held_tasks"], 0)
                            record = report["calls"]["records"][0]
                            self.assertEqual((record["prompt_computed_tokens"], record["prompt_cached_tokens"]),
                                             (5, 6))
                        self.assertEqual([r["admission_selection"]["cache_hit"] for r in reports],
                                         [False, False, True])
                        self.assertEqual(len(runtime.async_actors), 1)
                        self.assertEqual(runtime.async_actors[0].instance.engine._sequence, 3)
                        strategy = "onepass_local_pairs_v2" if name == "computed-work-local-pairs" else "onepass_geometric_v1"
                        self.assertTrue(all(r["backend_close"]["admission_feedback"]["strategy"] == strategy
                                            for r in reports))
                        self.assertTrue(all(r["backend_close"]["admission_feedback"]["accepted_steps"] == 0
                                            for r in reports))
                    finally:
                        self.assertEqual(driver.close()["status"], "closed")
                finally:
                    case.doCleanups()


if __name__ == "__main__":
    unittest.main()
