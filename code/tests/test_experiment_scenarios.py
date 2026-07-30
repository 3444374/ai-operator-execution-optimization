from __future__ import annotations

import sys
import json
import os
import unittest
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiment_scenarios import (  # noqa: E402
    build_scenario_schedule,
    validate_service_metadata,
)
from scripts.run_ai_operator_scenarios import (  # noqa: E402
    RunnerOptions,
    _load_config,
    parse_args,
    run_experiment,
    wait_for_idle,
)


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

    def test_committed_dual_gpu_templates_expand_and_validate(self) -> None:
        calibration_dir = TemporaryDirectory()
        self.addCleanup(calibration_dir.cleanup)
        calibration_path = Path(calibration_dir.name) / "selection.json"
        calibration_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "status": "ready",
                    "selection": {
                        "best_token_budget": 32768,
                        "project_static_k_per_endpoint": 256,
                        "project_active_work_per_endpoint": 65536,
                        "project_actor_workers_per_endpoint": 1,
                        "project_ray_actor_max_concurrency": 256,
                        "project_ray_worker_num_cpus": 0.5,
                    },
                    "evidence": {
                        "feeding": {"status": "passed"},
                        "token_budget": {"status": "passed"},
                        "actor_pool": {"status": "passed"},
                        "actor_pool": {"status": "passed"},
                    },
                }
            ),
            encoding="utf-8",
        )
        env = {
            "DATABASE_URL": "postgresql://example",
            "RAY_ADDRESS": "127.0.0.1:6380",
            "COMPLETION_ENDPOINT_URLS": (
                "http://gpu0/v1/completions,http://gpu1/v1/completions"
            ),
            "MODEL_METRICS_URLS": (
                "http://gpu0/metrics,http://gpu1/metrics"
            ),
            "SINGLE_COMPLETION_ENDPOINT_URL": "http://gpu0/v1/completions",
            "SINGLE_MODEL_METRICS_URL": "http://gpu0/metrics",
            "SINGLE_ENDPOINT_GPU_ID": "0",
            "SOURCE_WORKLOAD_NAME": "sharegpt_burstgpt",
            "SOURCE_MAX_PROMPT_TOKENS": "1500",
            "SHORT_TOTAL_ROWS": "512",
            "SHORT_REQUEST_MANIFEST": "/tmp/short.jsonl",
            "SHORT_SOURCE_WORKLOAD_NAME": "short_prompt_lt50",
            "LONG_TOTAL_ROWS": "325",
            "LONG_REQUEST_MANIFEST": "/tmp/long.jsonl",
            "LONG_SOURCE_WORKLOAD_NAME": "long_prompt_ge150",
            "COMPLETION_MODEL": "qwen2.5-7b",
            "COMPLETION_MAX_TOKENS": "256",
            "COMPLETION_PROTOCOL": "completions",
            "COMPLETION_PROMPT_FORMAT": "chatml",
            "TOKEN_BUDGET": "8192",
            "BEST_TOKEN_BUDGET": "32768",
            "TOKEN_BUDGET_CANDIDATES": (
                "2048,4096,8192,16384,32768,49152,65536"
            ),
            "ACTIVE_WORK_PER_ENDPOINT": "65536",
            "STRATEGY_CALIBRATION_SELECTION": str(calibration_path),
            "PROJECT_STATIC_K_PER_ENDPOINT": "256",
            "PROJECT_ACTIVE_WORK_PER_ENDPOINT": "65536",
            "PROJECT_ACTOR_WORKERS_PER_ENDPOINT": "1",
            "PROJECT_RAY_ACTOR_MAX_CONCURRENCY": "256",
            "PROJECT_RAY_WORKER_NUM_CPUS": "0.5",
            "PROJECT_FORMAL_REQUEST_MANIFEST": "/tmp/formal.jsonl",
            "ACTOR_WORKERS_PER_ENDPOINT": "2",
            "RAY_ACTOR_MAX_CONCURRENCY": "128",
            "RAY_WORKER_NUM_CPUS": "0.25",
            "CAPACITY_PROBE_TOKEN_BUDGET": "32768",
            "VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
            "VLLM_MAX_NUM_SEQS": "256",
            "REQUEST_SLO_MS": "30000",
            "GPU_PEAK_TFLOPS": "165",
            "MFU_PRECISION": "bf16_dense_fp32_accumulate",
        }
        templates = {
            "dual_gpu_capacity_scaling.example.json": 6,
            "dual_gpu_token_budget_curve.example.json": 7,
            "dual_gpu_data_organization.example.json": 4,
            "dual_gpu_submission_policy.example.json": 6,
            "dual_gpu_request_replay.example.json": 5,
            "dual_gpu_active_work_curve.example.json": 8,
            "dual_gpu_actor_pool_shape.example.json": 5,
            "dual_gpu_static_k_workload_surface.example.json": 9,
            "dual_gpu_static_credit_prompt_length_gate.example.json": 6,
            "dual_gpu_endpoint_adaptive_gate.example.json": 2,
            "dual_gpu_service_quantum.example.json": 6,
            "dual_gpu_slo_ewma_flush.example.json": 6,
        }

        with patch.dict(os.environ, env, clear=True):
            for filename, expected_scenarios in templates.items():
                with self.subTest(filename=filename):
                    config = _load_config(
                        CODE_ROOT.parent
                        / "deploy"
                        / "autodl"
                        / filename
                    )
                    self.assertEqual(
                        len(config.scenarios),
                        expected_scenarios,
                    )
                    self.assertEqual(
                        dict(config.service_metadata)[
                            "max_num_batched_tokens"
                        ],
                        8192,
                    )
                    self.assertEqual(
                        dict(config.service_metadata)["max_num_seqs"],
                        256,
                    )

            capacity = _load_config(
                CODE_ROOT.parent
                / "deploy"
                / "autodl"
                / "dual_gpu_token_budget_curve.example.json"
            )
            self.assertEqual(
                [item.scenario_id for item in capacity.scenarios],
                [
                    "tb2048",
                    "tb4096",
                    "tb8192",
                    "tb16384",
                    "tb32768",
                    "tb49152",
                    "tb65536",
                ],
            )
            self.assertIn("256", capacity.common_args)
            self.assertIn("2048", capacity.common_args)
            self.assertNotIn("--arrival-replay", capacity.common_args)
            work_index = capacity.common_args.index(
                "--max-active-work-per-endpoint"
            )
            active_work = int(capacity.common_args[work_index + 1])
            for scenario in capacity.scenarios:
                budget_index = scenario.args.index("--token-budget")
                token_budget = int(scenario.args[budget_index + 1])
                self.assertLessEqual(token_budget, active_work)

            active_work_curve = _load_config(
                CODE_ROOT.parent
                / "deploy"
                / "autodl"
                / "dual_gpu_active_work_curve.example.json"
            )
            self.assertIn("request", active_work_curve.common_args)
            self.assertEqual(
                [
                    item.scenario_id
                    for item in active_work_curve.scenarios
                ],
                [
                    "work16384",
                    "work24576",
                    "work32768",
                    "work49152",
                    "work65536",
                    "work81920",
                    "work98304",
                    "work131072",
                ],
            )

            actor_pool = _load_config(
                CODE_ROOT.parent
                / "deploy"
                / "autodl"
                / "dual_gpu_actor_pool_shape.example.json"
            )
            self.assertIn("request", actor_pool.common_args)
            self.assertIn("round_robin", actor_pool.common_args)
            self.assertEqual(
                [item.scenario_id for item in actor_pool.scenarios],
                [
                    "pool_1x256",
                    "pool_2x128",
                    "pool_4x64",
                    "pool_8x32",
                    "pool_16x16",
                ],
            )
            for scenario in actor_pool.scenarios:
                workers_index = scenario.args.index(
                    "--actor-workers-per-endpoint"
                )
                concurrency_index = scenario.args.index(
                    "--ray-actor-max-concurrency"
                )
                cpu_index = scenario.args.index("--ray-worker-num-cpus")
                workers = int(scenario.args[workers_index + 1])
                concurrency = int(scenario.args[concurrency_index + 1])
                worker_cpus = float(scenario.args[cpu_index + 1])
                self.assertEqual(workers * concurrency, 256)
                self.assertEqual(workers * worker_cpus, 0.5)

            service_quantum = _load_config(
                CODE_ROOT.parent
                / "deploy"
                / "autodl"
                / "dual_gpu_service_quantum.example.json"
            )
            work_index = service_quantum.common_args.index(
                "--max-active-work-per-endpoint"
            )
            workers_index = service_quantum.common_args.index(
                "--actor-workers-per-endpoint"
            )
            concurrency_index = service_quantum.common_args.index(
                "--ray-actor-max-concurrency"
            )
            cpu_index = service_quantum.common_args.index(
                "--ray-worker-num-cpus"
            )
            self.assertEqual(
                int(service_quantum.common_args[work_index + 1]),
                65536,
            )
            self.assertEqual(
                int(service_quantum.common_args[workers_index + 1])
                * int(service_quantum.common_args[concurrency_index + 1]),
                256,
            )
            self.assertEqual(
                int(service_quantum.common_args[workers_index + 1])
                * float(service_quantum.common_args[cpu_index + 1]),
                0.5,
            )
            self.assertEqual(
                [item.scenario_id for item in service_quantum.scenarios],
                [
                    "planning_batch",
                    "service_quantum_512",
                    "service_quantum_1024",
                    "service_quantum_2048",
                    "service_quantum_4096",
                    "request_diagnostic",
                ],
            )
            expected_args = {
                "planning_batch": ["--submission-granularity", "batch"],
                "service_quantum_512": [
                    "--submission-granularity",
                    "service_quantum",
                    "--service-quantum-tokens",
                    "512",
                ],
                "service_quantum_1024": [
                    "--submission-granularity",
                    "service_quantum",
                    "--service-quantum-tokens",
                    "1024",
                ],
                "service_quantum_2048": [
                    "--submission-granularity",
                    "service_quantum",
                    "--service-quantum-tokens",
                    "2048",
                ],
                "service_quantum_4096": [
                    "--submission-granularity",
                    "service_quantum",
                    "--service-quantum-tokens",
                    "4096",
                ],
                "request_diagnostic": [
                    "--submission-granularity",
                    "request",
                ],
            }
            self.assertEqual(
                {
                    item.scenario_id: list(item.args)
                    for item in service_quantum.scenarios
                },
                expected_args,
            )

            slo_ewma = _load_config(
                CODE_ROOT.parent
                / "deploy"
                / "autodl"
                / "dual_gpu_slo_ewma_flush.example.json"
            )
            self.assertIn("request", slo_ewma.common_args)
            self.assertIn(
                "--flush-service-capacity-tokens-s-per-endpoint",
                slo_ewma.common_args,
            )
            self.assertEqual(
                [item.scenario_id for item in slo_ewma.scenarios],
                [
                    "high_fixed50",
                    "high_queue25_50",
                    "high_slo_ewma",
                    "near_fixed50",
                    "near_queue25_50",
                    "near_slo_ewma",
                ],
            )
            for scenario in slo_ewma.scenarios:
                self.assertIn("--arrival-time-scale", scenario.args)
                self.assertIn("--flush-policy", scenario.args)
                scale_index = scenario.args.index("--arrival-time-scale")
                self.assertEqual(
                    scenario.args[scale_index + 1],
                    "0.006" if scenario.scenario_id.startswith("near_") else "0.001",
                )
                if scenario.scenario_id.endswith("slo_ewma"):
                    self.assertIn("slo_ewma", scenario.args)
                    self.assertIn("--flush-ewma-alpha", scenario.args)
                    self.assertIn("--flush-deadband-ratio", scenario.args)

            prompt_length_gate = _load_config(
                CODE_ROOT.parent
                / "deploy"
                / "autodl"
                / "dual_gpu_static_credit_prompt_length_gate.example.json"
            )
            self.assertIn("httpx_async", prompt_length_gate.common_args)
            self.assertIn(
                "--completion-return-token-ids",
                prompt_length_gate.common_args,
            )
            self.assertEqual(
                [
                    item.scenario_id
                    for item in prompt_length_gate.scenarios
                ],
                [
                    "short_k256",
                    "short_k256_w65536",
                    "short_k256_w98304",
                    "long_k256",
                    "long_k256_w65536",
                    "long_k256_w98304",
                ],
            )

    def test_wait_for_idle_reports_metrics_fetch_failure(self) -> None:
        health_response = MagicMock()
        health_response.__enter__.return_value.status = 200
        health_response.__exit__.return_value = False
        with (
            patch(
                "scripts.run_ai_operator_scenarios.request.urlopen",
                side_effect=[health_response, OSError("metrics unavailable")],
            ),
            patch(
                "scripts.run_ai_operator_scenarios.time.monotonic",
                side_effect=[0.0, 0.0, 2.0],
            ),
            patch("scripts.run_ai_operator_scenarios.time.sleep"),
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
                "scripts.run_ai_operator_scenarios.request.urlopen",
                side_effect=[health_response, metrics_response],
            ),
            patch(
                "scripts.run_ai_operator_scenarios.time.monotonic",
                side_effect=[0.0, 0.0, 2.0],
            ),
            patch("scripts.run_ai_operator_scenarios.time.sleep"),
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
