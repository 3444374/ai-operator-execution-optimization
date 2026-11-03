"""SemLoom launch budgets checked before Ray; native baseline retains its setup."""

from dataclasses import asdict
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.experiments import movie_records_session as session, lotus_records_movie as movie
from src.infrastructure.cpu_allocation import CpuCapacity
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


class RecordsCpuBudgetTests(unittest.TestCase):
    def setUp(self):
        self.resources = RecordsResources(cpu_workers=2)
        self.engine = EngineSpec("/tmp/fixture", sys.executable, sys.executable)
        self.generation = GenerationSpec()
        self.ray = SimpleNamespace(is_initialized=lambda: False, init=Mock(), shutdown=Mock(),
            get_runtime_context=lambda: SimpleNamespace(get_node_id=lambda: "fixture-node"))
        self.plan = dict(engine=asdict(self.engine), generation=asdict(self.generation),
            resources=asdict(self.resources), backend="semloom-core", lifecycle="resident", model_name="fixture")

    def test_resident_semloom_refuses_unknown_or_insufficient_CPU_before_ray_init(self):
        for backend in ("semloom-core", "semloom-static-reference"):
            for capacity in (CpuCapacity(64, 2.5, 2.5, "observed"), CpuCapacity(64, None, None, "unknown")):
                driver = session._MovieDriver(dict(self.plan, backend=backend), {}, Path("/tmp/fixture"), dict(task="movie-q3"))
                with self.subTest(backend=backend, capacity=capacity), \
                        patch.dict("sys.modules", ray=self.ray), \
                        patch.object(session, "new_private_directory"), \
                        patch("src.baselines.text.movie_tasks.MovieTask.from_config", return_value=SimpleNamespace(method=lambda name: object())), \
                        patch("src.infrastructure.cpu_allocation.observe_cpu_capacity", return_value=capacity):
                    with self.assertRaises((ValueError, RuntimeError)):
                        driver.start()
                self.ray.init.assert_not_called()
                self.assertFalse(driver.created_ray)

    def test_native_ray_data_does_not_get_a_semloom_budget_or_changed_declaration(self):
        driver = session._MovieDriver(dict(self.plan, backend="ray-data", lifecycle="runtime_reuse"), {},
                                      Path("/tmp/fixture"), dict(task="movie-q3"))
        with patch.dict("sys.modules", ray=self.ray), patch.object(session, "new_private_directory"), \
                patch("src.baselines.text.movie_tasks.MovieTask.from_config", return_value=SimpleNamespace(method=lambda name: object())), \
                patch("src.infrastructure.cpu_allocation.observe_cpu_capacity", side_effect=AssertionError("native owns its resources")):
            report = driver.start()
        self.assertTrue(report["no_session_model"])
        self.assertEqual(self.ray.init.call_args.kwargs["num_cpus"], 4)

    def test_ordinary_semloom_refuses_oversubscription_before_ray_init(self):
        config = dict(backend="semloom-local-model-static-reference", task="movie-q3", query_id="fixture")
        source = SimpleNamespace(task=SimpleNamespace(name="movie-q3"))
        with patch.dict("sys.modules", ray=self.ray), patch.object(movie, "write_private_json"), \
                patch("src.infrastructure.cpu_allocation.observe_cpu_capacity", return_value=CpuCapacity(64, 2.5, 2.5, "observed")):
            with self.assertRaisesRegex(ValueError, "effective allocation is 2.5"):
                movie._model_worker(config, source, None, None, self.generation, self.resources, self.engine, Path("/tmp/fixture"))
        self.ray.init.assert_not_called()
