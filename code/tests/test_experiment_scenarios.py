from __future__ import annotations

import sys
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiment_scenarios import (  # noqa: E402
    build_scenario_schedule,
    validate_service_metadata,
)
from scripts.run_ai_operator_scenarios import (  # noqa: E402
    RunnerOptions,
    run_experiment,
)


class ExperimentScenarioTests(unittest.TestCase):
    def test_service_metadata_requires_execution_parameters(self) -> None:
        metadata = {
            "vllm_version": "0.25.1",
            "enforce_eager": False,
            "compilation_mode": "default",
            "chunked_prefill": True,
            "max_num_batched_tokens": 4096,
            "max_num_seqs": 64,
            "gpu_memory_utilization": 0.75,
            "prefix_caching": False,
            "mfu_metrics": True,
        }

        validate_service_metadata(metadata)

    def test_service_metadata_rejects_missing_capacity(self) -> None:
        with self.assertRaisesRegex(ValueError, "max_num_batched_tokens"):
            validate_service_metadata({"vllm_version": "0.25.1"})

    def test_service_metadata_accepts_unknown_capacity(self) -> None:
        metadata = {
            "vllm_version": "0.25.1",
            "enforce_eager": False,
            "compilation_mode": "default",
            "chunked_prefill": True,
            "max_num_batched_tokens": "unknown",
            "max_num_seqs": "unknown",
            "gpu_memory_utilization": 0.75,
            "prefix_caching": False,
            "mfu_metrics": True,
        }

        validate_service_metadata(metadata)

    def test_service_metadata_rejects_invalid_capacity(self) -> None:
        for key in ("max_num_batched_tokens", "max_num_seqs"):
            with self.subTest(key=key):
                metadata = {
                    "vllm_version": "0.25.1",
                    "enforce_eager": False,
                    "compilation_mode": "default",
                    "chunked_prefill": True,
                    "max_num_batched_tokens": 4096,
                    "max_num_seqs": 64,
                    "gpu_memory_utilization": 0.75,
                    "prefix_caching": False,
                    "mfu_metrics": True,
                }
                metadata[key] = 0

                with self.assertRaisesRegex(ValueError, key):
                    validate_service_metadata(metadata)

    def test_service_metadata_rejects_invalid_utilization(self) -> None:
        for utilization in (0.0, 1.01):
            with self.subTest(utilization=utilization):
                metadata = {
                    "vllm_version": "0.25.1",
                    "enforce_eager": False,
                    "compilation_mode": "default",
                    "chunked_prefill": True,
                    "max_num_batched_tokens": 4096,
                    "max_num_seqs": 64,
                    "gpu_memory_utilization": utilization,
                    "prefix_caching": False,
                    "mfu_metrics": True,
                }

                with self.assertRaisesRegex(
                    ValueError,
                    "gpu_memory_utilization",
                ):
                    validate_service_metadata(metadata)

    def test_schedule_is_reproducible_and_interleaves_formal_scenarios(
        self,
    ) -> None:
        first = build_scenario_schedule(
            ["immediate", "fixed", "adaptive"],
            warmup_runs_per_scenario=1,
            formal_repeats=3,
            seed=7,
        )
        second = build_scenario_schedule(
            ["immediate", "fixed", "adaptive"],
            warmup_runs_per_scenario=1,
            formal_repeats=3,
            seed=7,
        )

        self.assertEqual(first, second)
        self.assertEqual(len(first), 12)
        self.assertEqual(
            [item.phase for item in first[:3]],
            ["warmup"] * 3,
        )
        for repeat_index in (1, 2, 3):
            group = [
                item
                for item in first
                if item.phase == "formal"
                and item.repeat_index == repeat_index
            ]
            self.assertEqual(
                sorted(item.scenario_id for item in group),
                ["adaptive", "fixed", "immediate"],
            )
        self.assertEqual(
            [item.order_index for item in first],
            list(range(12)),
        )
        self.assertTrue(all(item.random_seed == 7 for item in first))

    def test_different_seed_changes_formal_order(self) -> None:
        first = build_scenario_schedule(
            ["immediate", "fixed", "adaptive"],
            warmup_runs_per_scenario=0,
            formal_repeats=3,
            seed=7,
        )
        second = build_scenario_schedule(
            ["immediate", "fixed", "adaptive"],
            warmup_runs_per_scenario=0,
            formal_repeats=3,
            seed=8,
        )

        self.assertNotEqual(first, second)

    def test_schedule_rejects_invalid_inputs(self) -> None:
        invalid_cases = [
            (["fixed", "fixed"], 0, 1, "unique"),
            (["fixed", ""], 0, 1, "non-empty"),
            (["fixed"], -1, 1, "non-negative"),
            (["fixed"], 0, -1, "non-negative"),
            ([], 0, 1, "at least one"),
        ]
        for scenario_ids, warmups, repeats, message in invalid_cases:
            with self.subTest(
                scenario_ids=scenario_ids,
                warmups=warmups,
                repeats=repeats,
            ):
                with self.assertRaisesRegex(ValueError, message):
                    build_scenario_schedule(
                        scenario_ids,
                        warmup_runs_per_scenario=warmups,
                        formal_repeats=repeats,
                        seed=7,
                    )


class ScenarioRunnerTests(unittest.TestCase):
    def test_runner_validates_opted_in_metadata_before_external_work(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            profiler = self._write_fake_profiler(root)
            config = self._write_config(
                root,
                scenario_ids=["fixed"],
                formal_repeats=1,
                seed=7,
                require_complete_service_metadata=True,
            )
            output_dir = root / "output"
            idle_checks = []

            with self.assertRaisesRegex(
                ValueError,
                "max_num_batched_tokens",
            ):
                run_experiment(
                    RunnerOptions(
                        config_path=config,
                        profiler_path=profiler,
                        python_executable=Path(sys.executable),
                        output_dir=output_dir,
                        health_url="http://health",
                        metrics_url="http://metrics",
                        idle_timeout_s=1.0,
                    ),
                    idle_gate=lambda health, metrics, timeout: (
                        idle_checks.append((health, metrics, timeout))
                    ),
                )

            self.assertEqual(idle_checks, [])
            self.assertFalse(output_dir.exists())

    def test_runner_invokes_reproducible_schedule_and_writes_manifest(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            profiler = self._write_fake_profiler(root)
            config = self._write_config(
                root,
                scenario_ids=["fixed", "adaptive"],
                formal_repeats=2,
                seed=7,
            )
            output_dir = root / "output"
            idle_checks = []

            exit_code = run_experiment(
                RunnerOptions(
                    config_path=config,
                    profiler_path=profiler,
                    python_executable=Path(sys.executable),
                    output_dir=output_dir,
                    health_url="http://health",
                    metrics_url="http://metrics",
                    idle_timeout_s=1.0,
                ),
                idle_gate=lambda health, metrics, timeout: idle_checks.append(
                    (health, metrics, timeout)
                ),
            )

            manifest = json.loads(
                (output_dir / "manifest.json").read_text(encoding="utf-8")
            )
            expected = build_scenario_schedule(
                ["fixed", "adaptive"],
                warmup_runs_per_scenario=1,
                formal_repeats=2,
                seed=7,
            )

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(idle_checks), len(expected))
            self.assertEqual(
                [
                    (
                        item["scenario_id"],
                        item["phase"],
                        item["repeat_index"],
                    )
                    for item in manifest["completed_runs"]
                ],
                [
                    (item.scenario_id, item.phase, item.repeat_index)
                    for item in expected
                ],
            )
            self.assertEqual(manifest["incidents"], [])
            serialized = json.dumps(manifest)
            self.assertNotIn("top-secret", serialized)
            self.assertNotIn("postgres:secret@", serialized)
            self.assertIn("***", serialized)
            self.assertEqual(
                manifest["redacted_config"]["service_metadata"],
                {
                    "vllm_version": "0.25.1",
                    "prefix_caching": False,
                    "mfu_metrics": True,
                },
            )
            self.assertTrue((output_dir / "runs.csv").exists())

    def test_runner_stops_after_first_subprocess_failure(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            profiler = self._write_fake_profiler(root)
            scenario_ids = ["fixed", "adaptive", "pid"]
            seed = 11
            first = build_scenario_schedule(
                scenario_ids,
                warmup_runs_per_scenario=0,
                formal_repeats=1,
                seed=seed,
            )[0]
            (root / "fail_scenario.txt").write_text(
                first.scenario_id,
                encoding="utf-8",
            )
            config = self._write_config(
                root,
                scenario_ids=scenario_ids,
                formal_repeats=1,
                seed=seed,
                warmups=0,
            )
            output_dir = root / "output"

            exit_code = run_experiment(
                RunnerOptions(
                    config_path=config,
                    profiler_path=profiler,
                    python_executable=Path(sys.executable),
                    output_dir=output_dir,
                    health_url="http://health",
                    metrics_url="http://metrics",
                    idle_timeout_s=1.0,
                ),
                idle_gate=lambda _health, _metrics, _timeout: None,
            )

            manifest = json.loads(
                (output_dir / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(exit_code, 1)
            self.assertEqual(manifest["completed_runs"], [])
            self.assertEqual(len(manifest["incidents"]), 1)
            self.assertEqual(
                manifest["incidents"][0]["scenario_id"],
                first.scenario_id,
            )
            self.assertEqual(manifest["incidents"][0]["exit_code"], 3)

    def test_runner_resume_skips_completed_runs_and_recovers_failure(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            profiler = self._write_fake_profiler(root)
            scenario_ids = ["fixed", "adaptive", "pid"]
            seed = 11
            schedule = build_scenario_schedule(
                scenario_ids,
                warmup_runs_per_scenario=0,
                formal_repeats=1,
                seed=seed,
            )
            failed = schedule[1]
            (root / "fail_scenario.txt").write_text(
                failed.scenario_id,
                encoding="utf-8",
            )
            config = self._write_config(
                root,
                scenario_ids=scenario_ids,
                formal_repeats=1,
                seed=seed,
                warmups=0,
            )
            output_dir = root / "output"
            base_options = dict(
                config_path=config,
                profiler_path=profiler,
                python_executable=Path(sys.executable),
                output_dir=output_dir,
                health_url="http://health",
                metrics_url="http://metrics",
                idle_timeout_s=1.0,
            )

            first_exit = run_experiment(
                RunnerOptions(**base_options),
                idle_gate=lambda _health, _metrics, _timeout: None,
            )
            (root / "fail_scenario.txt").unlink()
            resumed_idle_checks = []
            resumed_exit = run_experiment(
                RunnerOptions(**base_options, resume=True),
                idle_gate=lambda health, metrics, timeout: (
                    resumed_idle_checks.append((health, metrics, timeout))
                ),
            )

            manifest = json.loads(
                (output_dir / "manifest.json").read_text(encoding="utf-8")
            )
            rows = (output_dir / "runs.csv").read_text(
                encoding="utf-8"
            ).splitlines()
            self.assertEqual(first_exit, 1)
            self.assertEqual(resumed_exit, 0)
            self.assertEqual(len(resumed_idle_checks), 2)
            self.assertEqual(len(rows), 4)
            self.assertEqual(len(manifest["completed_runs"]), 3)
            self.assertEqual(manifest["status"], "completed")
            self.assertEqual(len(manifest["incidents"]), 1)
            self.assertTrue(manifest["incidents"][0]["recovered"])

    def test_runner_resume_can_prune_failed_scenario(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            profiler = self._write_fake_profiler(root)
            scenario_ids = ["fixed", "adaptive", "pid"]
            seed = 11
            schedule = build_scenario_schedule(
                scenario_ids,
                warmup_runs_per_scenario=0,
                formal_repeats=2,
                seed=seed,
            )
            failed_scenario = schedule[1].scenario_id
            (root / "fail_scenario.txt").write_text(
                failed_scenario,
                encoding="utf-8",
            )
            config = self._write_config(
                root,
                scenario_ids=scenario_ids,
                formal_repeats=2,
                seed=seed,
                warmups=0,
            )
            output_dir = root / "output"
            base_options = dict(
                config_path=config,
                profiler_path=profiler,
                python_executable=Path(sys.executable),
                output_dir=output_dir,
                health_url="http://health",
                metrics_url="http://metrics",
                idle_timeout_s=1.0,
            )

            first_exit = run_experiment(
                RunnerOptions(**base_options),
                idle_gate=lambda _health, _metrics, _timeout: None,
            )
            resumed_exit = run_experiment(
                RunnerOptions(
                    **base_options,
                    resume=True,
                    skip_failed_scenarios=True,
                ),
                idle_gate=lambda _health, _metrics, _timeout: None,
            )

            manifest = json.loads(
                (output_dir / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(first_exit, 1)
            self.assertEqual(resumed_exit, 0)
            self.assertEqual(
                manifest["status"],
                "completed_with_pruned_scenarios",
            )
            self.assertTrue(manifest["incidents"][0]["pruned"])
            self.assertEqual(
                {
                    item["scenario_id"]
                    for item in manifest["skipped_runs"]
                },
                {failed_scenario},
            )
            self.assertEqual(len(manifest["skipped_runs"]), 2)

    @staticmethod
    def _write_config(
        root: Path,
        *,
        scenario_ids: list[str],
        formal_repeats: int,
        seed: int,
        warmups: int = 1,
        require_complete_service_metadata: bool = False,
    ) -> Path:
        config = {
            "schema_version": 1,
            "experiment_id": "runner-test",
            "seed": seed,
            "service_metadata": {
                "vllm_version": "0.25.1",
                "prefix_caching": False,
                "mfu_metrics": True,
            },
            "warmup_runs_per_scenario": warmups,
            "formal_repeats": formal_repeats,
            "common_args": [
                "--database-url",
                "postgresql://postgres:secret@localhost/db",
                "--completion-api-key",
                "top-secret",
            ],
            "scenarios": [
                {
                    "scenario_id": scenario_id,
                    "args": ["--flush-policy", scenario_id],
                }
                for scenario_id in scenario_ids
            ],
        }
        if require_complete_service_metadata:
            config["require_complete_service_metadata"] = True
        path = root / "config.json"
        path.write_text(json.dumps(config), encoding="utf-8")
        return path

    @staticmethod
    def _write_fake_profiler(root: Path) -> Path:
        path = root / "fake_profiler.py"
        path.write_text(
            "\n".join(
                [
                    "import argparse",
                    "import csv",
                    "from pathlib import Path",
                    "parser = argparse.ArgumentParser(add_help=False)",
                    "parser.add_argument('--output', required=True)",
                    "parser.add_argument('--experiment-id', required=True)",
                    "parser.add_argument('--scenario-id', required=True)",
                    "parser.add_argument('--random-seed', required=True)",
                    "parser.add_argument('--run-phase', required=True)",
                    "parser.add_argument('--run-repeat-index', required=True)",
                    "args, _ = parser.parse_known_args()",
                    "fail_path = Path(__file__).with_name('fail_scenario.txt')",
                    "if fail_path.exists() and fail_path.read_text(encoding='utf-8').strip() == args.scenario_id:",
                    "    raise SystemExit(3)",
                    "output = Path(args.output)",
                    "output.parent.mkdir(parents=True, exist_ok=True)",
                    "exists = output.exists()",
                    "row = {",
                    "    'status': 'ok',",
                    "    'experiment_id': args.experiment_id,",
                    "    'phase': args.run_phase,",
                    "    'repeat_index': args.run_repeat_index,",
                    "    'scenario_id': args.scenario_id,",
                    "    'random_seed': args.random_seed,",
                    "}",
                    "with output.open('a', newline='', encoding='utf-8') as handle:",
                    "    writer = csv.DictWriter(handle, fieldnames=list(row))",
                    "    if not exists:",
                    "        writer.writeheader()",
                    "    writer.writerow(row)",
                    "print(args.scenario_id)",
                ]
            ),
            encoding="utf-8",
        )
        return path


if __name__ == "__main__":
    unittest.main()
