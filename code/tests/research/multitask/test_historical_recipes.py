"""Validate preserved research declarations without restoring their old launchers."""

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from scripts.experiments.run_ai_operator_scenarios import _load_config

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
ARCHIVED_RECIPES = CODE_ROOT.parent / "experiments/reproduction/legacy_recipes_20261105/autodl"


class HistoricalRecipeTests(unittest.TestCase):
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
            "dual_gpu_cost_profile_formal.example.json": 80,
        }

        with patch.dict(os.environ, env, clear=True):
            for filename, expected_scenarios in templates.items():
                with self.subTest(filename=filename):
                    config = _load_config(
                        ARCHIVED_RECIPES
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
                ARCHIVED_RECIPES
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
                ARCHIVED_RECIPES
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

            formal = _load_config(
                ARCHIVED_RECIPES
                / "dual_gpu_cost_profile_formal.example.json"
            )
            self.assertIs(
                dict(formal.service_metadata)["prefix_caching"],
                True,
            )
            prefix_index = formal.common_args.index(
                "--service-prefix-caching"
            )
            self.assertEqual(
                formal.common_args[prefix_index + 1],
                "enabled",
            )
            contexts: dict[tuple[str, int, int], set[int]] = {}
            for scenario in formal.scenarios:
                arguments = dict(zip(scenario.args[::2], scenario.args[1::2]))
                context = (
                    arguments["--source-workload-name"],
                    int(arguments["--total-rows"]),
                    int(arguments["--completion-max-tokens"]),
                )
                contexts.setdefault(context, set()).add(
                    int(arguments["--max-active-work-per-endpoint"])
                )
            self.assertEqual(
                {context[0] for context in contexts},
                {
                    "short_prompt_lt50",
                    "long_prompt_ge150",
                    "sharegpt_concentrated",
                    "sharegpt_multiturn",
                    "lmcache_agent",
                },
            )

            self.assertEqual(len(contexts), 20)
            self.assertTrue(
                all(
                    candidates == {32768, 49152, 65536, 98304}
                    for candidates in contexts.values()
                )
            )

            actor_pool = _load_config(
                ARCHIVED_RECIPES
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
                ARCHIVED_RECIPES
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
                ARCHIVED_RECIPES
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
                ARCHIVED_RECIPES
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


    def test_project_short_all_at_t0_diagnostic_is_eager_input_only(self) -> None:
        with TemporaryDirectory() as temporary_dir:
            selection_path = Path(temporary_dir) / "selection.json"
            selection_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "ready",
                        "selection": {
                            "project_static_k_per_endpoint": 128,
                            "project_active_work_per_endpoint": 65536,
                            "project_actor_workers_per_endpoint": 8,
                            "project_ray_actor_max_concurrency": 32,
                            "project_ray_worker_num_cpus": 0.25,
                        },
                        "evidence": {
                            "feeding": {"status": "passed"},
                            "token_budget": {"status": "passed"},
                            "actor_pool": {"status": "passed"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            env = {
                "DATABASE_URL": "postgresql://example",
                "RAY_ADDRESS": "127.0.0.1:6380",
                "VLLM_VERSION": "0.25.1",
                "VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
                "VLLM_MAX_NUM_SEQS": "256",
                "COMPLETION_MODEL": "qwen2.5-7b",
                "MODEL_PATH": "/models/qwen2.5-7b",
                "OPENING_SHORT_JOB_MANIFEST": "/tmp/short.jsonl",
                "SHAREGPT_PROJECT_K": "128",
                "SHAREGPT_PROJECT_CALIBRATION_CONTRACT": str(selection_path),
            }
            with patch.dict(os.environ, env, clear=True):
                config = _load_config(
                    ARCHIVED_RECIPES
                    / "opening_project_short_all_at_t0_diagnostic.example.json"
                )

        self.assertEqual(
            [item.scenario_id for item in config.scenarios],
            ["project_short_all_at_t0_static"],
        )
        self.assertNotIn("--arrival-replay", config.common_args)
        self.assertNotIn("--arrival-replay-start-epoch-s", config.common_args)
        self.assertEqual(
            config.common_args[config.common_args.index("--source-order") + 1],
            "doc_id",
        )
        self.assertEqual(
            config.common_args[config.common_args.index("--max-inflight") + 1],
            "128",
        )
        self.assertEqual(
            config.common_args[
                config.common_args.index("--max-active-work-per-endpoint") + 1
            ],
            "65536",
        )
        self.assertFalse(dict(config.service_metadata)["enforce_eager"])
