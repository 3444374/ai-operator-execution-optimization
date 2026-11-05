from __future__ import annotations

import sys
import json
import os
import unittest
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.scenarios.core import (  # noqa: E402
    build_scenario_schedule,
    validate_service_metadata,
)
from src.experiments.shared_vllm.preflight import wait_for_idle  # noqa: E402
from scripts.experiments.run_ai_operator_scenarios import (  # noqa: E402
    RunnerOptions,
    _load_config,
    _verify_service_flags_match_live,
    parse_args,
    run_experiment,
)


class VerifyServiceFlagsMatchLiveTests(unittest.TestCase):
    """Audit F8 regression: _verify_service_flags_match_live must run main()'s preflight path
    without a NameError (an earlier fix used os.environ without importing os; the hermetic
    run_experiment tests missed it because main() was never exercised)."""

    def _config(self):
        # minimal config carrying the two declared capacity flags
        return type(
            "_Cfg",
            (),
            {"service_metadata": (("max_num_seqs", 256), ("max_num_batched_tokens", 8192))},
        )()

    def test_returns_cleanly_when_no_live_vllm_or_unsupported_host(self):
        # On a non-Linux test host (or with no vllm process), _read_live_cmdlines yields {} ->
        # the verifier warns + returns (it must NOT raise NameError on os.environ, and must NOT
        # fail-closed just because there is no live process to probe).
        import os as _os

        _os.environ["COMPLETION_ENDPOINT_URLS"] = "http://127.0.0.1:8000/v1/completions"
        try:
            _verify_service_flags_match_live(self._config())  # no exception
        finally:
            _os.environ.pop("COMPLETION_ENDPOINT_URLS", None)

    def test_skips_when_nothing_declared(self):
        cfg = type("_Cfg", (), {"service_metadata": ()})()
        _verify_service_flags_match_live(cfg)  # returns immediately, no env read


class ExperimentScenarioTests(unittest.TestCase):
    @staticmethod
    def _complete_service_metadata() -> dict[str, object]:
        return {
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

    def test_service_metadata_requires_execution_parameters(self) -> None:
        validate_service_metadata(self._complete_service_metadata())

    def test_parse_args_rejects_empty_metrics_url_list(self) -> None:
        with self.assertRaises(SystemExit):
            parse_args(
                [
                    "--config",
                    "config.json",
                    "--profiler",
                    "profiler.py",
                    "--python-executable",
                    sys.executable,
                    "--output-dir",
                    "output",
                    "--health-url",
                    "http://health",
                    "--metrics-urls",
                    " , ",
                ]
            )

    def test_wait_for_idle_reports_metrics_fetch_failure(self) -> None:
        health_response = MagicMock()
        health_response.__enter__.return_value.status = 200
        health_response.__exit__.return_value = False
        with (
            patch(
                "src.experiments.shared_vllm.preflight.request.urlopen",
                side_effect=[health_response, OSError("metrics unavailable")],
            ),
            patch(
                "src.experiments.shared_vllm.preflight.time.monotonic",
                side_effect=[0.0, 0.0, 2.0],
            ),
            patch("src.experiments.shared_vllm.preflight.time.sleep"),
        ):
            with self.assertRaisesRegex(
                TimeoutError,
                "metrics_at_http://metrics:OSError",
            ):
                wait_for_idle(
                    "http://health",
                    ("http://metrics",),
                    timeout_s=1.0,
                )

    def test_wait_for_idle_requires_both_idle_metrics(self) -> None:
        health_response = MagicMock()
        health_response.__enter__.return_value.status = 200
        health_response.__exit__.return_value = False
        metrics_response = MagicMock()
        metrics_response.__enter__.return_value.read.return_value = (
            b"vllm:num_requests_running 0\n"
        )
        metrics_response.__exit__.return_value = False
        with (
            patch(
                "src.experiments.shared_vllm.preflight.request.urlopen",
                side_effect=[health_response, metrics_response],
            ),
            patch(
                "src.experiments.shared_vllm.preflight.time.monotonic",
                side_effect=[0.0, 0.0, 2.0],
            ),
            patch("src.experiments.shared_vllm.preflight.time.sleep"),
        ):
            with self.assertRaisesRegex(
                TimeoutError,
                "missing_idle_metrics_at_http://metrics",
            ):
                wait_for_idle(
                    "http://health",
                    ("http://metrics",),
                    timeout_s=1.0,
                )

    def test_service_metadata_rejects_each_missing_required_key(self) -> None:
        for key in self._complete_service_metadata():
            with self.subTest(key=key):
                metadata = self._complete_service_metadata()
                del metadata[key]

                with self.assertRaisesRegex(ValueError, key):
                    validate_service_metadata(metadata)

    def test_service_metadata_rejects_unknown_capacity(self) -> None:
        for key in ("max_num_batched_tokens", "max_num_seqs"):
            with self.subTest(key=key):
                metadata = self._complete_service_metadata()
                metadata[key] = "unknown"

                with self.assertRaisesRegex(ValueError, key):
                    validate_service_metadata(metadata)

    def test_service_metadata_rejects_empty_required_strings(self) -> None:
        for key in ("vllm_version", "compilation_mode"):
            for invalid in ("", " ", "unknown"):
                with self.subTest(key=key, invalid=invalid):
                    metadata = self._complete_service_metadata()
                    metadata[key] = invalid

                    with self.assertRaisesRegex(ValueError, key):
                        validate_service_metadata(metadata)

    def test_service_metadata_rejects_non_boolean_flags(self) -> None:
        for key in (
            "enforce_eager",
            "chunked_prefill",
            "prefix_caching",
            "mfu_metrics",
        ):
            with self.subTest(key=key):
                metadata = self._complete_service_metadata()
                metadata[key] = "unknown"

                with self.assertRaisesRegex(ValueError, key):
                    validate_service_metadata(metadata)

    def test_service_metadata_rejects_invalid_capacity(self) -> None:
        for key in ("max_num_batched_tokens", "max_num_seqs"):
            for invalid in (0, -1, True, False, 1.0, None, "4096"):
                with self.subTest(key=key, invalid=invalid):
                    metadata = self._complete_service_metadata()
                    metadata[key] = invalid

                    with self.assertRaisesRegex(ValueError, key):
                        validate_service_metadata(metadata)

    def test_service_metadata_rejects_invalid_utilization(self) -> None:
        for utilization in (
            0,
            -0.1,
            1.01,
            True,
            False,
            None,
            float("nan"),
            float("inf"),
            float("-inf"),
            "0.75",
        ):
            with self.subTest(utilization=utilization):
                metadata = self._complete_service_metadata()
                metadata["gpu_memory_utilization"] = utilization

                with self.assertRaisesRegex(
                    ValueError,
                    "gpu_memory_utilization",
                ):
                    validate_service_metadata(metadata)

    def test_service_metadata_accepts_utilization_upper_boundary(self) -> None:
        metadata = self._complete_service_metadata()
        metadata["gpu_memory_utilization"] = 1.0

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
    def test_config_rejects_prefix_cache_row_metadata_mismatch(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_path = self._write_config(
                root,
                scenario_ids=["fixed"],
                formal_repeats=1,
                seed=7,
            )
            decoded = json.loads(config_path.read_text(encoding="utf-8"))
            decoded["service_metadata"]["prefix_caching"] = True
            decoded["common_args"].extend(
                ["--service-prefix-caching", "disabled"]
            )
            config_path.write_text(json.dumps(decoded), encoding="utf-8")

            with self.assertRaisesRegex(
                ValueError,
                "service_metadata.prefix_caching=True",
            ):
                _load_config(config_path)

    def test_config_rejects_empty_ray_address_before_execution(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_path = self._write_config(
                root,
                scenario_ids=["fixed"],
                formal_repeats=1,
                seed=7,
            )
            decoded = json.loads(config_path.read_text(encoding="utf-8"))
            decoded["common_args"] = [
                "--executor",
                "ray_actor",
                "--ray-address",
                "${EMPTY_RAY_ADDRESS}",
            ]
            config_path.write_text(json.dumps(decoded), encoding="utf-8")

            with patch.dict(
                os.environ,
                {"EMPTY_RAY_ADDRESS": ""},
                clear=False,
            ):
                with self.assertRaisesRegex(ValueError, "empty required"):
                    _load_config(config_path)

    def test_parse_recover_stale_lease_requires_resume(self) -> None:
        common = [
            "--config",
            "config.json",
            "--profiler",
            "profile.py",
            "--python-executable",
            sys.executable,
            "--output-dir",
            "output",
            "--health-url",
            "http://health",
            "--metrics-urls",
            "http://metrics",
            "--recover-stale-lease",
        ]

        with patch("sys.stderr", new=StringIO()) as stderr:
            with self.assertRaises(SystemExit):
                parse_args(common)
            self.assertIn("requires --resume", stderr.getvalue())

        options = parse_args([*common, "--resume"])
        self.assertTrue(options.recover_stale_lease)

    def test_runner_holds_lease_during_profiler_and_releases_after(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_dir = root / "output"

            exit_code = run_experiment(
                RunnerOptions(
                    config_path=self._write_config(
                        root,
                        scenario_ids=["fixed"],
                        formal_repeats=1,
                        seed=7,
                    ),
                    profiler_path=self._write_fake_profiler(
                        root,
                        require_runner_lease=True,
                    ),
                    python_executable=Path(sys.executable),
                    output_dir=output_dir,
                    health_url="http://health",
                    metrics_urls=("http://metrics",),
                    idle_timeout_s=1.0,
                ),
                idle_gate=lambda _health, _metrics, _timeout: None,
            )

            self.assertEqual(exit_code, 0)
            self.assertFalse(
                (output_dir / ".runner-lease.json").exists()
            )

    def test_config_expands_explicit_environment_references(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_path = self._write_config(
                root,
                scenario_ids=["fixed"],
                formal_repeats=1,
                seed=7,
            )
            decoded = json.loads(config_path.read_text(encoding="utf-8"))
            decoded["common_args"] = [
                "--completion-model",
                "${TEST_COMPLETION_MODEL}",
            ]
            config_path.write_text(
                json.dumps(decoded),
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {"TEST_COMPLETION_MODEL": "qwen-test"},
                clear=False,
            ):
                config = _load_config(config_path)

            self.assertEqual(
                config.common_args,
                ("--completion-model", "qwen-test"),
            )

    def test_config_rejects_unset_environment_reference(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_path = self._write_config(
                root,
                scenario_ids=["fixed"],
                formal_repeats=1,
                seed=7,
            )
            decoded = json.loads(config_path.read_text(encoding="utf-8"))
            decoded["common_args"] = ["--completion-model", "${MISSING_MODEL}"]
            config_path.write_text(
                json.dumps(decoded),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(
                    ValueError,
                    "MISSING_MODEL",
                ):
                    _load_config(config_path)

    def test_config_validates_calibration_contract_before_work(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            selection_path = self._write_calibration_selection(root)
            config_path = self._write_config(
                root,
                scenario_ids=["fixed"],
                formal_repeats=1,
                seed=7,
            )
            decoded = json.loads(config_path.read_text(encoding="utf-8"))
            decoded["calibration_contract"] = {
                "path": "${SELECTION_PATH}",
                "expected": {
                    "best_token_budget": "${BEST_TOKEN_BUDGET}",
                },
            }
            config_path.write_text(json.dumps(decoded), encoding="utf-8")

            with patch.dict(
                os.environ,
                {
                    "SELECTION_PATH": str(selection_path),
                    "BEST_TOKEN_BUDGET": "32768",
                },
                clear=False,
            ):
                config = _load_config(config_path)

            self.assertIsNotNone(config.calibration_contract)
            assert config.calibration_contract is not None
            self.assertEqual(
                dict(config.calibration_contract.selection),
                {"best_token_budget": 32768},
            )

    def test_config_rejects_stale_calibration_before_work(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            selection_path = self._write_calibration_selection(root)
            config_path = self._write_config(
                root,
                scenario_ids=["fixed"],
                formal_repeats=1,
                seed=7,
            )
            decoded = json.loads(config_path.read_text(encoding="utf-8"))
            decoded["calibration_contract"] = {
                "path": str(selection_path),
                "expected": {"best_token_budget": 8192},
            }
            config_path.write_text(json.dumps(decoded), encoding="utf-8")
            idle_checks = []

            with self.assertRaisesRegex(
                ValueError,
                "best_token_budget.*8192.*32768",
            ):
                run_experiment(
                    RunnerOptions(
                        config_path=config_path,
                        profiler_path=self._write_fake_profiler(root),
                        python_executable=Path(sys.executable),
                        output_dir=root / "output",
                        health_url="http://health",
                        metrics_urls=("http://metrics",),
                        idle_timeout_s=1.0,
                    ),
                    idle_gate=lambda *_args: idle_checks.append(True),
                )

            self.assertEqual(idle_checks, [])
            self.assertFalse((root / "output").exists())

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
                        metrics_urls=("http://metrics",),
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
                    metrics_urls=("http://metrics",),
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
                    metrics_urls=("http://metrics",),
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
                metrics_urls=("http://metrics",),
                idle_timeout_s=1.0,
            )

            first_exit = run_experiment(
                RunnerOptions(**base_options),
                idle_gate=lambda _health, _metrics, _timeout: None,
            )
            failed_manifest = json.loads(
                (output_dir / "manifest.json").read_text(encoding="utf-8")
            )
            failed_stderr_path = Path(
                failed_manifest["incidents"][0]["stderr_path"]
            )
            failed_stderr = failed_stderr_path.read_text(encoding="utf-8")
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
            self.assertEqual(
                failed_stderr_path.read_text(encoding="utf-8"),
                failed_stderr,
            )
            recovered_run = next(
                item
                for item in manifest["completed_runs"]
                if item["scenario_id"] == failed.scenario_id
            )
            self.assertNotEqual(
                recovered_run["stderr_path"],
                str(failed_stderr_path),
            )

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
                metrics_urls=("http://metrics",),
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
    def _write_calibration_selection(root: Path) -> Path:
        path = root / "selection.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "status": "ready",
                    "selection": {"best_token_budget": 32768},
                    "evidence": {
                        "feeding": {"status": "passed"},
                        "token_budget": {"status": "passed"},
                        "actor_pool": {"status": "passed"},
                    },
                }
            ),
            encoding="utf-8",
        )
        return path

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
    def _write_fake_profiler(
        root: Path,
        *,
        require_runner_lease: bool = False,
    ) -> Path:
        path = root / "fake_profiler.py"
        lease_check = (
            [
                "lease = output.parent / '.runner-lease.json'",
                "if not lease.exists():",
                "    print('runner lease missing', file=sys.stderr)",
                "    raise SystemExit(4)",
            ]
            if require_runner_lease
            else []
        )
        path.write_text(
            "\n".join(
                [
                    "import argparse",
                    "import csv",
                    "import sys",
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
                    "    print('intentional failure', file=sys.stderr)",
                    "    raise SystemExit(3)",
                    "output = Path(args.output)",
                    *lease_check,
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
