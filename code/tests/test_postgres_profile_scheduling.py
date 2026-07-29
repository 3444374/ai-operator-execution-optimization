from __future__ import annotations

import csv
import io
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts import postgres_ai_operator_profile as profile  # noqa: E402
from scripts.run_ai_operator_scenarios import _load_config  # noqa: E402
from src.baselines.contracts import ChatRequest  # noqa: E402
from src.baselines.manifests import write_manifest  # noqa: E402
from src.profiling import ray as profile_ray  # noqa: E402
from src.profiling import replay as profile_replay  # noqa: E402
from src.scheduling.adaptive_admission import AimdAdmissionController  # noqa: E402
from src.scheduling.admission import DynamicAdmissionGate  # noqa: E402
from src.scheduling.models import (  # noqa: E402
    SubmissionCompletion,
    SubmissionLifecycleEvent,
)
from src.scheduling.observations import (  # noqa: E402
    AdmissionTraceEvent,
    CachedMetricsObservationProvider,
    NonBlockingMetricsObservationProvider,
    ServiceMetricsSnapshot,
)
from src.scheduling.batching import (  # noqa: E402
    FlushTraceEvent,
    PendingBatchBuilder,
    ReplayServiceObservation,
)
from src.scheduling.lifecycle import (  # noqa: E402
    RequestLifecycleSeed,
    RequestTraceRow,
)
from src.scheduling.routing import (  # noqa: E402
    LeastQueuedEndpointRouter,
    RequestPoolRouter,
)
from src.scheduling.ray_runtime import RayWorkerOptions  # noqa: E402
from src.scheduling.scheduler import SchedulerResult  # noqa: E402


class _RecordingRemoteDefinition:
    def __init__(self) -> None:
        self.options_calls = []

    def options(self, **options):
        self.options_calls.append(options)
        return self


class _RecordingRay:
    @staticmethod
    def remote(_target):
        return _RecordingRemoteDefinition()


class SchedulingProfileHelperTests(unittest.TestCase):
    def test_committed_dual_gpu_scenarios_pass_profiler_dry_validation(
        self,
    ) -> None:
        env = {
            "DATABASE_URL": "postgresql://example",
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
            "COMPLETION_MODEL": "qwen2.5-7b",
            "COMPLETION_MAX_TOKENS": "256",
            "COMPLETION_PROMPT_FORMAT": "chatml",
            "TOKEN_BUDGET": "8192",
            "BEST_TOKEN_BUDGET": "8192",
            "ACTIVE_WORK_PER_ENDPOINT": "65536",
            "CAPACITY_PROBE_TOKEN_BUDGET": "32768",
            "VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
            "VLLM_MAX_NUM_SEQS": "256",
            "REQUEST_SLO_MS": "30000",
            "GPU_PEAK_TFLOPS": "165",
            "MFU_PRECISION": "bf16_dense_fp32_accumulate",
        }
        templates = (
            "dual_gpu_capacity_scaling.example.json",
            "dual_gpu_token_budget_curve.example.json",
            "dual_gpu_data_organization.example.json",
            "dual_gpu_request_replay.example.json",
            "dual_gpu_active_work_curve.example.json",
        )

        with patch.dict(os.environ, env, clear=True):
            for filename in templates:
                config = _load_config(
                    CODE_ROOT.parent / "deploy" / "autodl" / filename
                )
                for scenario in config.scenarios:
                    with self.subTest(
                        filename=filename,
                        scenario=scenario.scenario_id,
                    ):
                        args = profile.parse_args(
                            [
                                *config.common_args,
                                *scenario.args,
                                "--scenario-id",
                                scenario.scenario_id,
                                "--dry-run",
                            ]
                        )
                        row = profile.run_once(args, "formal", 1)
                        self.assertEqual(row["status"], "dry_run")

    def test_same_condition_project_templates_pass_dry_validation(
        self,
    ) -> None:
        def requests(
            row_count: int,
            *,
            start: int = 0,
        ) -> tuple[ChatRequest, ...]:
            return tuple(
                ChatRequest(
                    doc_id=index,
                    prompt=f"prompt-{index}",
                    arrival_time_s=0.0,
                    prompt_tokens=8 + index % 7,
                    max_output_tokens=256,
                    estimated_output_tokens=32 + index % 11,
                    source_row_hash=f"row-{index}",
                    endpoint_index=index % 2,
                )
                for index in range(start, start + row_count)
            )

        with tempfile.TemporaryDirectory() as directory:
            calibration_manifest = Path(directory) / "calibration.jsonl"
            formal_manifest = Path(directory) / "formal.jsonl"
            write_manifest(calibration_manifest, requests(512))
            write_manifest(formal_manifest, requests(2048, start=512))
            env = {
                "DATABASE_URL": "postgresql://example",
                "COMPLETION_CHAT_ENDPOINT_URLS": (
                    "http://gpu0/v1/chat/completions,"
                    "http://gpu1/v1/chat/completions"
                ),
                "MODEL_METRICS_URLS": (
                    "http://gpu0/metrics,http://gpu1/metrics"
                ),
                "SOURCE_WORKLOAD_NAME": "sharegpt_burstgpt",
                "SOURCE_MAX_PROMPT_TOKENS": "1500",
                "COMPLETION_MODEL": "qwen2.5-7b",
                "PROJECT_CALIBRATION_REQUEST_MANIFEST": str(
                    calibration_manifest
                ),
                "PROJECT_FORMAL_REQUEST_MANIFEST": str(formal_manifest),
                "PROJECT_STATIC_K_PER_ENDPOINT": "256",
                "PROJECT_ACTIVE_WORK_PER_ENDPOINT": "65536",
                "RAY_ADDRESS": "127.0.0.1:6380",
                "VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
                "VLLM_MAX_NUM_SEQS": "256",
                "REQUEST_SLO_MS": "30000",
                "GPU_PEAK_TFLOPS": "165",
                "MFU_PRECISION": "bf16_dense_fp32_accumulate",
            }
            templates = (
                "dual_gpu_same_condition_project_equivalence_gate.example.json",
                "dual_gpu_same_condition_project_calibration.example.json",
                "dual_gpu_same_condition_project_formal.example.json",
            )

            with patch.dict(os.environ, env, clear=True):
                for filename in templates:
                    config = _load_config(
                        CODE_ROOT.parent / "deploy" / "autodl" / filename
                    )
                    for scenario in config.scenarios:
                        with self.subTest(
                            filename=filename,
                            scenario=scenario.scenario_id,
                        ):
                            args = profile.parse_args(
                                [
                                    *config.common_args,
                                    *scenario.args,
                                    "--scenario-id",
                                    scenario.scenario_id,
                                    "--dry-run",
                                ]
                            )
                            row = profile.run_once(args, "formal", 1)
                            self.assertEqual(row["status"], "dry_run")
                            self.assertEqual(
                                row["request_manifest_validation_status"],
                                "not_executed",
                            )

    def test_equivalence_gate_uses_same_pressure_warmups_and_repeats(
        self,
    ) -> None:
        env = {
            "DATABASE_URL": "postgresql://example",
            "COMPLETION_CHAT_ENDPOINT_URLS": (
                "http://gpu0/v1/chat/completions,"
                "http://gpu1/v1/chat/completions"
            ),
            "MODEL_METRICS_URLS": (
                "http://gpu0/metrics,http://gpu1/metrics"
            ),
            "SOURCE_WORKLOAD_NAME": "sharegpt_burstgpt",
            "SOURCE_MAX_PROMPT_TOKENS": "1500",
            "COMPLETION_MODEL": "qwen2.5-7b",
            "PROJECT_CALIBRATION_REQUEST_MANIFEST": "manifest.jsonl",
            "RAY_ADDRESS": "127.0.0.1:6380",
            "VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
            "VLLM_MAX_NUM_SEQS": "256",
            "REQUEST_SLO_MS": "30000",
            "GPU_PEAK_TFLOPS": "165",
            "MFU_PRECISION": "bf16_dense_fp32_accumulate",
        }

        with patch.dict(os.environ, env, clear=True):
            config = _load_config(
                CODE_ROOT.parent
                / "deploy"
                / "autodl"
                / "dual_gpu_same_condition_project_equivalence_gate.example.json"
            )

        self.assertEqual(config.warmup_runs_per_scenario, 1)
        self.assertEqual(config.formal_repeats, 3)
        self.assertEqual(
            [scenario.scenario_id for scenario in config.scenarios],
            ["static_k256", "work98304_nonbinding"],
        )
        for scenario in config.scenarios:
            resolved = [*config.common_args, *scenario.args]
            self.assertIn("256", resolved)
        self.assertNotIn("--arrival-replay", config.common_args)
        self.assertIn("request", config.common_args)
        self.assertIn("manifest_pinned", config.common_args)
        self.assertIn("127.0.0.1:6380", config.common_args)

    def test_profiler_waits_for_actor_readiness_before_e2e_timer(self) -> None:
        source = Path(profile.__file__).read_text(encoding="utf-8")

        ready_index = source.index("wait_until_ready")
        timer_index = source.index('StageTimer.start("e2e")')

        self.assertLess(ready_index, timer_index)
        self.assertIn("actor_ready_s", profile.FORMAL_RESULT_FIELDS)

    def test_ray_task_worker_options_ignore_actor_only_concurrency(self) -> None:
        args = profile.parse_args(
            [
                "--executor",
                "ray_task",
                "--ray-actor-max-concurrency",
                "0",
                "--ray-worker-num-cpus",
                "0.5",
            ]
        )

        options = profile._ray_worker_options(args)

        self.assertEqual(options.actor_max_concurrency, 1)
        self.assertEqual(
            options.task_options(),
            {"num_cpus": 0.5, "num_gpus": 0, "max_retries": 0},
        )

    def test_ray_actor_worker_options_preserve_configured_concurrency(self) -> None:
        args = profile.parse_args(
            [
                "--executor",
                "ray_actor",
                "--ray-actor-max-concurrency",
                "4",
                "--ray-worker-num-cpus",
                "0.5",
            ]
        )

        options = profile._ray_worker_options(args)

        self.assertEqual(options.actor_max_concurrency, 4)
        self.assertEqual(options.num_cpus, 0.5)

    def test_http_actor_definition_receives_safe_ray_options(self) -> None:
        ray = _RecordingRay()
        options = RayWorkerOptions(0.25, actor_max_concurrency=4)

        remote_cls = profile._remote_actor_class(
            ray,
            profile.CompatibleHTTPCompletionActor,
            options,
        )

        self.assertEqual(
            remote_cls.options_calls,
            [
                {
                    "num_cpus": 0.25,
                    "num_gpus": 0,
                    "max_concurrency": 4,
                    "max_restarts": 0,
                    "max_task_retries": 0,
                }
            ],
        )

    def test_http_task_definition_disables_retry(self) -> None:
        ray = _RecordingRay()
        options = RayWorkerOptions(0.25)

        remote_fn = profile._remote_task(
            ray,
            profile.compatible_http_complete_batch,
            options,
        )

        self.assertEqual(
            remote_fn.options_calls,
            [{"num_cpus": 0.25, "num_gpus": 0, "max_retries": 0}],
        )

    def test_fake_ray_definitions_receive_the_same_worker_options(self) -> None:
        options = RayWorkerOptions(0.25, actor_max_concurrency=3)
        cases = [
            (profile.FakeEmbeddingActor, True, "max_concurrency", 3),
            (profile.FakeCompletionActor, True, "max_concurrency", 3),
            (profile.fake_embed_batch, False, "max_retries", 0),
            (profile.fake_complete_batch, False, "max_retries", 0),
        ]

        for target, is_actor, option_name, expected in cases:
            with self.subTest(target=target.__name__):
                ray = _RecordingRay()

                helper = (
                    profile._remote_actor_class
                    if is_actor
                    else profile._remote_task
                )
                remote = helper(ray, target, options)

                self.assertEqual(remote.options_calls[0][option_name], expected)
                self.assertEqual(remote.options_calls[0]["num_cpus"], 0.25)
                self.assertEqual(remote.options_calls[0]["num_gpus"], 0)

        self.assertFalse(hasattr(profile, "_remote_worker_definition"))

    def test_endpoint_help_describes_service_endpoints(self) -> None:
        output = io.StringIO()
        with patch.object(sys, "argv", ["profile", "--help"]):
            with patch("sys.stdout", output):
                with self.assertRaises(SystemExit):
                    profile.parse_args()

        help_text = " ".join(output.getvalue().split())
        self.assertIn("per service endpoint", help_text)

    def test_explicit_single_endpoint_overrides_plural_environment(self) -> None:
        with patch.dict(
            profile.os.environ,
            {
                "COMPLETION_ENDPOINT_URLS": (
                    "http://gpu0/v1/completions,"
                    "http://gpu1/v1/completions"
                ),
                "MODEL_METRICS_URLS": (
                    "http://gpu0/metrics,http://gpu1/metrics"
                ),
            },
            clear=False,
        ):
            args = profile.parse_args(
                [
                    "--completion-endpoint-url",
                    "http://single/v1/completions",
                    "--model-metrics-url",
                    "http://single/metrics",
                ]
            )

            self.assertEqual(
                profile.completion_endpoint_urls(args),
                ["http://single/v1/completions"],
            )
            self.assertEqual(
                profile.model_metrics_urls(args),
                ["http://single/metrics"],
            )

    def test_plural_environment_is_used_when_cli_is_absent(self) -> None:
        with patch.dict(
            profile.os.environ,
            {
                "COMPLETION_ENDPOINT_URLS": (
                    " http://gpu0/v1/completions ,"
                    " http://gpu1/v1/completions "
                )
            },
            clear=False,
        ):
            args = profile.parse_args([])

            self.assertEqual(
                profile.completion_endpoint_urls(args),
                [
                    "http://gpu0/v1/completions",
                    "http://gpu1/v1/completions",
                ],
            )

    def test_submit_metrics_merge_aggregates_chunks_and_missing_fields(self) -> None:
        aggregate = {
            "operator_invocations": 0,
            "max_inflight": 0,
            "submit_s": 0.0,
            "adaptive_limit_mean": 0.0,
            "endpoint_count": 0,
            "actor_worker_count": 0,
            "actor_worker_submission_counts": "",
        }

        profile._merge_submit_metrics(
            aggregate,
            {
                "operator_invocations": 1,
                "max_inflight": 2,
                "submit_s": 0.1,
                "adaptive_limit_mean": 2.0,
                "endpoint_count": 1,
                "actor_worker_count": 2,
                "actor_worker_submission_counts": "1;0",
            },
        )
        profile._merge_submit_metrics(
            aggregate,
            {
                "operator_invocations": 2,
                "max_inflight": 1,
                "submit_s": 0.2,
                "adaptive_limit_mean": 1.0,
                "endpoint_count": 1,
                "actor_worker_count": 2,
                "actor_worker_submission_counts": "0;2",
            },
        )
        profile._merge_submit_metrics(
            aggregate,
            {
                "operator_invocations": 1,
                "max_inflight": 1,
                "submit_s": 0.3,
            },
        )

        self.assertEqual(aggregate["operator_invocations"], 4)
        self.assertEqual(aggregate["max_inflight"], 2)
        self.assertAlmostEqual(aggregate["submit_s"], 0.6)
        self.assertEqual(aggregate["adaptive_limit_mean"], 2.0)
        self.assertEqual(aggregate["endpoint_count"], 1)
        self.assertEqual(aggregate["actor_worker_count"], 2)
        self.assertEqual(aggregate["actor_worker_submission_counts"], "1;2")

    def test_submit_metrics_merge_rejects_worker_count_width_change(self) -> None:
        aggregate = {"actor_worker_submission_counts": "1;0"}

        with self.assertRaisesRegex(
            ValueError,
            "submission count widths",
        ):
            profile._merge_submit_metrics(
                aggregate,
                {"actor_worker_submission_counts": "1"},
            )

    def test_ray_worker_cli_defaults_are_explicit(self) -> None:
        args = profile.parse_args([])

        self.assertEqual(args.actor_workers_per_endpoint, 0)
        self.assertEqual(args.ray_actor_max_concurrency, 1)
        self.assertEqual(args.ray_worker_num_cpus, 0.25)
        self.assertEqual(args.actor_worker_routing, "round_robin")

    def test_actor_pool_slots_bound_effective_endpoint_admission(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--executor",
                "ray_actor",
                "--actor-workers-per-endpoint",
                "2",
                "--ray-actor-max-concurrency",
                "4",
                "--actor-worker-routing",
                "least_active_work",
                "--admission-scope",
                "per_endpoint",
                "--max-inflight",
                "32",
            ]
        )

        row = profile.run_once(args, "formal", 1)

        self.assertEqual(row["actor_worker_routing"], "least_active_work")
        self.assertEqual(row["actor_pool_slots_per_endpoint"], 8)
        self.assertEqual(row["per_endpoint_inflight_limit"], 8)
        self.assertEqual(row["effective_global_inflight_limit"], 8)

    def test_service_quantum_cli_is_explicit_and_positive(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--submission-granularity",
                "service_quantum",
                "--service-quantum-tokens",
                "4096",
            ]
        )

        self.assertEqual(args.submission_granularity, "service_quantum")
        self.assertEqual(args.service_quantum_tokens, 4096)
        row = profile.run_once(args, "formal", 1)
        self.assertEqual(row["service_quantum_tokens"], 4096)

    def test_service_quantum_cli_rejects_missing_or_inapplicable_target(
        self,
    ) -> None:
        invalid_cases = [
            (
                [
                    "--dry-run",
                    "--submission-granularity",
                    "service_quantum",
                ],
                "service-quantum-tokens must be positive",
            ),
            (
                [
                    "--dry-run",
                    "--submission-granularity",
                    "batch",
                    "--service-quantum-tokens",
                    "4096",
                ],
                "service-quantum-tokens requires service_quantum",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--executor",
                    "ray_task",
                    "--submission-granularity",
                    "request",
                    "--service-quantum-tokens",
                    "4096",
                ],
                "service-quantum-tokens requires service_quantum",
            ),
        ]
        for argv, message in invalid_cases:
            with self.subTest(argv=argv):
                with self.assertRaisesRegex(SystemExit, message):
                    profile.run_once(profile.parse_args(argv), "formal", 1)

    def test_service_quantum_summary_separates_work_rows_and_oversized_rows(
        self,
    ) -> None:
        metrics = profile._service_quantum_run_metrics(
            [(10, 2, False), (7, 1, False), (20, 1, True)],
            target_tokens=10,
        )

        self.assertEqual(metrics["service_quantum_tokens"], 10)
        self.assertEqual(metrics["service_quantum_count"], 3)
        self.assertAlmostEqual(metrics["service_quantum_rows_mean"], 4 / 3, places=6)
        self.assertAlmostEqual(metrics["service_quantum_work_mean"], 37 / 3, places=6)
        self.assertEqual(metrics["service_quantum_work_p95"], 20)
        self.assertEqual(metrics["service_quantum_oversized_rows"], 1)

    def test_dry_run_records_ray_execution_contract(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--executor",
                "ray_actor",
                "--actor-workers-per-endpoint",
                "4",
                "--ray-actor-max-concurrency",
                "2",
                "--ray-worker-num-cpus",
                "0.25",
            ]
        )

        row = profile.run_once(args, "formal", 1)

        self.assertEqual(row["ray_version"], "")
        self.assertEqual(row["actor_workers_per_endpoint"], 4)
        self.assertEqual(row["ray_actor_max_concurrency"], 2)
        self.assertEqual(row["ray_worker_num_cpus"], 0.25)
        self.assertEqual(row["ray_worker_num_gpus"], 0)
        self.assertEqual(row["endpoint_count"], 1)
        self.assertEqual(row["actor_worker_count"], 4)
        self.assertEqual(row["actor_worker_submission_counts"], "")

    def test_per_endpoint_admission_scales_global_credit_by_endpoint_count(
        self,
    ) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--operator",
                "ai_complete",
                "--model-backend",
                "compatible_http",
                "--completion-endpoint-urls",
                "http://gpu0/v1,http://gpu1/v1",
                "--executor",
                "ray_actor",
                "--actor-workers-per-endpoint",
                "1",
                "--ray-actor-max-concurrency",
                "16",
                "--max-inflight",
                "16",
                "--admission-scope",
                "per_endpoint",
            ]
        )

        row = profile.run_once(args, "formal", 1)

        self.assertEqual(row["endpoint_count"], 2)
        self.assertEqual(row["max_inflight_limit"], 16)
        self.assertEqual(row["admission_scope"], "per_endpoint")
        self.assertEqual(row["per_endpoint_inflight_limit"], 16)
        self.assertEqual(row["effective_global_inflight_limit"], 32)

    def test_per_endpoint_admission_rejects_global_adaptive_controller(
        self,
    ) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--executor",
                "ray_task",
                "--ray-address",
                "auto",
                "--admission-scope",
                "per_endpoint",
                "--scheduling-policy",
                "aimd_hol",
            ]
        )

        with self.assertRaisesRegex(SystemExit, "static only"):
            profile.run_once(args, "formal", 1)

    def test_python_dry_run_records_non_applicable_ray_contract(self) -> None:
        args = profile.parse_args(["--dry-run", "--executor", "python"])

        row = profile.run_once(args, "formal", 1)

        self.assertEqual(row["ray_version"], "")
        self.assertEqual(row["actor_workers_per_endpoint"], 0)
        self.assertEqual(row["ray_actor_max_concurrency"], 0)
        self.assertEqual(row["ray_worker_num_cpus"], 0.0)
        self.assertEqual(row["ray_worker_num_gpus"], 0)
        self.assertEqual(row["actor_worker_count"], 0)
        self.assertEqual(row["actor_worker_submission_counts"], "")

    def test_ray_task_dry_run_records_effective_task_worker_contract(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--executor",
                "ray_task",
                "--ray-actor-max-concurrency",
                "7",
                "--ray-worker-num-cpus",
                "0.5",
            ]
        )

        row = profile.run_once(args, "formal", 1)

        self.assertEqual(row["ray_version"], "")
        self.assertEqual(row["actor_workers_per_endpoint"], 0)
        self.assertEqual(row["ray_actor_max_concurrency"], 0)
        self.assertEqual(row["ray_worker_num_cpus"], 0.5)
        self.assertEqual(row["ray_worker_num_gpus"], 0)
        self.assertEqual(row["actor_worker_count"], 0)
        self.assertEqual(row["actor_worker_submission_counts"], "")

    def test_compatible_http_requires_endpoint_before_dry_or_real_work(self) -> None:
        for dry_run in (False, True):
            argv = ["--model-backend", "http_openai"]
            if dry_run:
                argv.append("--dry-run")
            args = profile.parse_args(argv)
            with (
                patch.object(profile, "connect") as connect,
                patch.object(profile, "require_ray") as require_ray,
            ):
                with self.assertRaisesRegex(SystemExit, "Missing endpoint URL"):
                    profile.run_once(args, "formal", 1)

            connect.assert_not_called()
            require_ray.assert_not_called()

    def test_real_python_row_records_non_applicable_ray_contract(self) -> None:
        args = profile.parse_args(
            [
                "--database-url",
                "postgresql://unused",
                "--executor",
                "python",
                "--total-rows",
                "1",
                "--writeback-mode",
                "none",
            ]
        )
        table = pa.table(
            {
                "doc_id": [1],
                "tenant_id": [1],
                "category": ["test"],
                "text": ["hello"],
            }
        )
        connection = Mock()
        source = SimpleNamespace(
            fetch=Mock(
                return_value=SimpleNamespace(
                    table=table,
                    metrics={"db_fetch_s": 0.0, "arrow_build_s": 0.0},
                )
            )
        )
        organizer = SimpleNamespace(
            organize=Mock(
                return_value=SimpleNamespace(
                    batches=[table],
                    batch_cost_units=(1,),
                    batch_row_counts=(1,),
                    metrics={
                        "packing_scope": "organizer_input",
                        "organizer_from_arrow_s": 0.0,
                        "organizer_plan_s": 0.0,
                        "organizer_collect_s": 0.0,
                        "organization_policy_family": "fixed_rows",
                        "batch_prompt_token_spread_mean": 0.0,
                        "prefix_group_ratio": 0.0,
                        "partition_effective": "true",
                        "warnings": "",
                    },
                )
            )
        )

        with (
            patch.object(profile, "connect", return_value=connection),
            patch.object(profile, "gpu_metadata", return_value={}),
            patch.object(
                profile,
                "database_metadata",
                return_value={"server_version": "18.4", "pgvector_version": "0.8.2"},
            ),
            patch.object(
                profile,
                "embedding_vector_column_dim",
                return_value=args.embedding_dim,
            ),
            patch.object(profile, "create_job", return_value=1),
            patch.object(profile, "finish_job"),
            patch.object(profile, "make_source", return_value=source),
            patch.object(profile, "make_organizer", return_value=organizer),
            patch.object(profile, "write_embeddings", return_value=0),
        ):
            row = profile.run_once(args, "formal", 1)

        self.assertEqual(row["ray_version"], "")
        self.assertEqual(row["actor_workers_per_endpoint"], 0)
        self.assertEqual(row["ray_actor_max_concurrency"], 0)
        self.assertEqual(row["ray_worker_num_cpus"], 0.0)
        self.assertEqual(row["ray_worker_num_gpus"], 0)
        self.assertEqual(row["actor_worker_count"], 0)
        self.assertEqual(row["actor_worker_submission_counts"], "")

    def test_real_ray_actor_row_records_resolved_execution_contract(self) -> None:
        args = profile.parse_args(
            [
                "--database-url",
                "postgresql://unused",
                "--executor",
                "ray_actor",
                "--actor-workers-per-endpoint",
                "2",
                "--ray-actor-max-concurrency",
                "3",
                "--ray-worker-num-cpus",
                "0.5",
                "--total-rows",
                "1",
                "--writeback-mode",
                "none",
            ]
        )
        table = pa.table({"doc_id": [1], "prompt": ["hello"]})
        connection = Mock()
        source = SimpleNamespace(
            fetch=Mock(
                return_value=SimpleNamespace(
                    table=table,
                    metrics={"db_fetch_s": 0.0, "arrow_build_s": 0.0},
                )
            )
        )
        organizer = SimpleNamespace(
            organize=Mock(
                return_value=SimpleNamespace(
                    batches=[table],
                    batch_cost_units=(1,),
                    batch_row_counts=(1,),
                    metrics={
                        "packing_scope": "organizer_input",
                        "organizer_from_arrow_s": 0.0,
                        "organizer_plan_s": 0.0,
                        "organizer_collect_s": 0.0,
                        "organization_policy_family": "fixed_rows",
                        "batch_prompt_token_spread_mean": 0.0,
                        "prefix_group_ratio": 0.0,
                        "partition_effective": "true",
                        "warnings": "",
                    },
                )
            )
        )

        class RemoteDefinition:
            def options(self, **_options):
                return self

            def remote(self, *_args):
                return SimpleNamespace(
                    ready=SimpleNamespace(
                        remote=Mock(return_value=object())
                    )
                )

        ray_module = SimpleNamespace(
            __version__="2.test",
            init=Mock(),
            remote=Mock(return_value=RemoteDefinition()),
            get=Mock(side_effect=lambda refs: [None for _ in refs]),
        )
        submission_metrics = {
            "operator_invocations": 1,
            "max_inflight": 1,
            "bounded_wait_s": 0.0,
            "avg_bounded_wait_s": 0.0,
            "fanin_s": 0.0,
            "submit_s": 0.0,
            "adaptive_downshifts": 0,
            "adaptive_upshifts": 0,
            "adaptive_limit_mean": 1.0,
            "endpoint_count": 1,
            "actor_worker_count": 2,
            "actor_worker_submission_counts": "1;0",
        }

        with (
            patch.object(profile, "connect", return_value=connection),
            patch.object(profile, "gpu_metadata", return_value={}),
            patch.object(
                profile,
                "database_metadata",
                return_value={"server_version": "18.4", "pgvector_version": "0.8.2"},
            ),
            patch.object(
                profile,
                "embedding_vector_column_dim",
                return_value=args.embedding_dim,
            ),
            patch.object(profile, "create_job", return_value=1),
            patch.object(profile, "finish_job"),
            patch.object(profile, "require_ray", return_value=ray_module),
            patch.object(profile, "make_source", return_value=source),
            patch.object(profile, "make_organizer", return_value=organizer),
            patch.object(
                profile,
                "submit_with_backpressure",
                return_value=(
                    [{"rows": 1, "token_count": 1, "service_s": 0.0}],
                    submission_metrics,
                ),
            ),
            patch.object(profile, "write_embeddings", return_value=0),
        ):
            row = profile.run_once(args, "formal", 1)

        self.assertEqual(row["ray_version"], "2.test")
        self.assertEqual(row["actor_workers_per_endpoint"], 2)
        self.assertEqual(row["ray_actor_max_concurrency"], 3)
        self.assertEqual(row["ray_worker_num_cpus"], 0.5)
        self.assertEqual(row["ray_worker_num_gpus"], 0)
        self.assertEqual(row["endpoint_count"], 1)
        self.assertEqual(row["actor_worker_count"], 2)
        self.assertEqual(row["actor_worker_submission_counts"], "1;0")
        self.assertGreaterEqual(row["actor_ready_s"], 0.0)

    def test_single_endpoint_legacy_workers_remain_compatible(self) -> None:
        args = profile.parse_args(["--model-workers", "4"])

        self.assertEqual(
            profile._resolve_actor_workers_per_endpoint(args, 1),
            4,
        )

    def test_multi_endpoint_actor_requires_explicit_workers(self) -> None:
        args = profile.parse_args(["--executor", "ray_actor"])

        with self.assertRaisesRegex(
            SystemExit,
            "actor-workers-per-endpoint",
        ):
            profile._resolve_actor_workers_per_endpoint(args, 2)

    def test_invalid_ray_actor_resources_fail_before_initialization(self) -> None:
        invalid_options = [
            ("--ray-actor-max-concurrency", "0"),
            ("--ray-worker-num-cpus", "nan"),
        ]

        for option, value in invalid_options:
            with self.subTest(option=option, value=value):
                args = profile.parse_args([option, value])
                with (
                    patch.object(profile, "connect") as connect,
                    patch.object(profile, "require_ray") as require_ray,
                ):
                    with self.assertRaises(SystemExit):
                        profile.run_once(args, "formal", 1)

                connect.assert_not_called()
                require_ray.assert_not_called()

    def test_invalid_ray_task_cpu_fails_before_initialization(self) -> None:
        for value in ("0", "nan"):
            with self.subTest(value=value):
                args = profile.parse_args(
                    [
                        "--executor",
                        "ray_task",
                        "--ray-worker-num-cpus",
                        value,
                        "--database-url",
                        "postgresql://unused",
                    ]
                )
                with (
                    patch.object(
                        profile,
                        "connect",
                        side_effect=AssertionError("connect called"),
                    ) as connect,
                    patch.object(
                        profile,
                        "require_ray",
                        side_effect=AssertionError("require_ray called"),
                    ) as require_ray,
                ):
                    with self.assertRaisesRegex(
                        SystemExit,
                        "ray-worker-num-cpus",
                    ):
                        profile.run_once(args, "formal", 1)

                connect.assert_not_called()
                require_ray.assert_not_called()

    def test_batch_envelopes_preserve_arrow_payload_and_compute_cost(self) -> None:
        batch = pa.table(
            {
                "doc_id": [1, 2],
                "prompt_tokens": [10, 20],
                "prefix_key": ["shared", "shared"],
                "arrival_time_s": [1.5, 2.0],
            }
        )

        envelopes = profile_replay._batch_envelopes(
            [batch],
            job_id="job-1",
            operator="ai_complete",
            completion_max_tokens=8,
        )

        self.assertEqual(len(envelopes), 1)
        envelope = envelopes[0]
        self.assertIs(envelope.payload, batch)
        self.assertEqual(envelope.request.request_id, "job-1:batch:0")
        self.assertEqual(envelope.request.payload_id, "job-1:batch:0")
        self.assertEqual(envelope.request.row_count, 2)
        self.assertEqual(envelope.request.prompt_tokens, 30)
        self.assertEqual(envelope.request.estimated_output_tokens, 16)
        self.assertEqual(envelope.request.prefix_key, "shared")
        self.assertEqual(envelope.request.first_arrival_s, 1.5)
        self.assertEqual(envelope.request.oldest_arrival_s, 1.5)

    def test_batch_envelopes_use_trace_target_output_cost_without_changing_cap(
        self,
    ) -> None:
        batch = pa.table(
            {
                "doc_id": [1, 2],
                "prompt_tokens": [10, 20],
                "target_output_tokens": [7, 3],
            }
        )

        envelopes = profile_replay._batch_envelopes(
            [batch],
            job_id="job-1",
            operator="ai_complete",
            completion_max_tokens=16,
            output_cost_mode="trace_target_output",
        )

        self.assertEqual(envelopes[0].request.estimated_output_tokens, 10)

    def test_offline_batch_envelopes_seed_every_row_from_job_start(self) -> None:
        table = pa.table(
            {
                "doc_id": [1, 2],
                "prompt_tokens": [6, 4],
                "target_output_tokens": [3, 2],
                "prefix_key": ["p", "p"],
            }
        )

        envelopes, seeds = profile._offline_batch_envelopes(
            [table],
            job_id="job",
            operator="ai_complete",
            completion_max_tokens=16,
            output_cost_mode="trace_target_output",
            batch_index_start=7,
            job_start_epoch_s=100.0,
            ready_epoch_s=101.0,
        )

        self.assertEqual(envelopes[0].request.request_id, "job:batch:7")
        self.assertEqual([item.doc_id for item in seeds], ["1", "2"])
        self.assertEqual(
            [item.estimated_output_tokens for item in seeds],
            [3, 2],
        )
        self.assertTrue(all(item.arrival_epoch_s == 100.0 for item in seeds))
        self.assertTrue(all(item.flush_epoch_s == 101.0 for item in seeds))
        self.assertTrue(
            all(
                item.request_time_origin == "offline_job_start"
                for item in seeds
            )
        )

    def test_offline_request_granularity_emits_one_envelope_per_row(
        self,
    ) -> None:
        table = pa.table(
            {
                "doc_id": [11, 12],
                "text": ["a", "b"],
                "prompt_tokens": [3, 5],
                "target_output_tokens": [7, 9],
                "prefix_key": ["", ""],
                "preferred_endpoint_id": ["endpoint-1", "endpoint-0"],
            }
        )

        envelopes, seeds = profile._offline_batch_envelopes(
            [table],
            job_id="job",
            operator="ai_complete",
            completion_max_tokens=256,
            output_cost_mode="trace_target_output",
            batch_index_start=0,
            job_start_epoch_s=10.0,
            ready_epoch_s=10.1,
            submission_granularity="request",
        )

        self.assertEqual(
            [item.request.row_count for item in envelopes],
            [1, 1],
        )
        self.assertEqual(
            [item.request.request_id for item in envelopes],
            ["job:request:11", "job:request:12"],
        )
        self.assertEqual(
            [item.request.planning_batch_id for item in envelopes],
            ["job:batch:0", "job:batch:0"],
        )
        self.assertEqual(
            [item.request.preferred_endpoint_id for item in envelopes],
            ["endpoint-1", "endpoint-0"],
        )
        self.assertEqual(
            [seed.submission_id for seed in seeds],
            ["job:request:11", "job:request:12"],
        )

    def test_offline_service_quanta_preserve_planning_batch_membership(
        self,
    ) -> None:
        table = pa.table(
            {
                "doc_id": [1, 2, 3],
                "prompt_tokens": [6, 4, 7],
                "prefix_key": ["p", "p", "p"],
            }
        )
        planning = []
        quanta = []

        envelopes, seeds = profile._offline_batch_envelopes(
            [table],
            job_id="job",
            operator="ai_embed",
            completion_max_tokens=0,
            output_cost_mode="fixed_output_cap",
            batch_index_start=2,
            job_start_epoch_s=100.0,
            ready_epoch_s=101.0,
            submission_granularity="service_quantum",
            service_quantum_tokens=10,
            planning_sink=planning,
            quantum_sink=quanta,
        )

        self.assertEqual(
            [item.request.request_id for item in envelopes],
            ["job:batch:2:quantum:0", "job:batch:2:quantum:1"],
        )
        self.assertEqual(
            [item.request.planning_batch_id for item in envelopes],
            ["job:batch:2", "job:batch:2"],
        )
        self.assertEqual([item.request.row_count for item in envelopes], [2, 1])
        self.assertEqual(
            [item.submission_id for item in seeds],
            ["job:batch:2:quantum:0"] * 2 + ["job:batch:2:quantum:1"],
        )
        self.assertEqual(planning, [(17, 3)])
        self.assertEqual(quanta, [(10, 2, False), (7, 1, False)])

    def test_endpoint_topology_pairs_ids_and_urls_in_default_pool(self) -> None:
        topology = profile_ray._endpoint_topology(
            endpoint_ids=["endpoint-0", "endpoint-1"],
            endpoint_urls=["http://one", "http://two"],
        )

        self.assertEqual(
            [(item.endpoint_id, item.url) for item in topology.endpoints],
            [("endpoint-0", "http://one"), ("endpoint-1", "http://two")],
        )
        self.assertTrue(all(item.pool_id == "default" for item in topology.endpoints))
        self.assertTrue(all(item.gpu_id == "0" for item in topology.endpoints))
        self.assertTrue(all(item.healthy for item in topology.endpoints))

    def test_endpoint_topology_rejects_mismatched_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "same length"):
            profile_ray._endpoint_topology(["endpoint-0"], [])

    def test_endpoint_topology_preserves_pool_and_gpu_assignments(self) -> None:
        topology = profile_ray._endpoint_topology(
            ["endpoint-0", "endpoint-1"],
            ["http://one", "http://two"],
            pool_ids=["short", "long"],
            gpu_ids=["0", "1"],
        )

        self.assertEqual(
            [(item.pool_id, item.gpu_id) for item in topology.endpoints],
            [("short", "0"), ("long", "1")],
        )

    def test_scheduler_metrics_preserve_existing_profiler_schema(self) -> None:
        result = SchedulerResult(
            completions=(SubmissionCompletion("request-1", "completed", result={"ok": True}),),
            operator_invocations=1,
            max_inflight_seen=1,
            applied_limit=4,
            bounded_wait_s=0.2,
            avg_bounded_wait_s=0.2,
            fanin_s=0.1,
            submit_s=0.05,
        )

        metrics = profile_ray._scheduler_metrics(result)

        self.assertEqual(
            set(metrics),
            {
                "operator_invocations",
                "max_inflight",
                "max_active_work_per_endpoint_seen",
                "bounded_wait_s",
                "avg_bounded_wait_s",
                "fanin_s",
                "submit_s",
                "adaptive_downshifts",
                "adaptive_upshifts",
                "adaptive_limit_mean",
            },
        )
        self.assertEqual(metrics["operator_invocations"], 1)
        self.assertEqual(metrics["max_inflight"], 1)
        self.assertEqual(metrics["adaptive_downshifts"], 0)
        self.assertEqual(metrics["adaptive_upshifts"], 0)
        self.assertEqual(metrics["adaptive_limit_mean"], 4)

    def test_service_metrics_snapshot_maps_available_vllm_gauges(self) -> None:
        with patch.object(
            profile,
            "scrape_prometheus_metrics",
            return_value={
                "vllm:num_requests_running": 12.0,
                "vllm:num_requests_waiting": 3.0,
                "vllm:kv_cache_usage_perc": 0.7,
            },
        ):
            snapshot = profile._service_metrics_snapshot(["http://metrics"])

        self.assertEqual(snapshot.running, 12)
        self.assertEqual(snapshot.waiting, 3)
        self.assertEqual(snapshot.kv_usage, 0.7)

    def test_service_metrics_snapshot_returns_none_on_missing_scrape(self) -> None:
        with patch.object(profile, "scrape_prometheus_metrics", return_value={}):
            self.assertIsNone(
                profile._service_metrics_snapshot(["http://metrics"])
            )

    def test_multi_endpoint_metrics_use_per_gpu_flops_mean(self) -> None:
        with patch.object(
            profile,
            "scrape_prometheus_metrics",
            side_effect=[
                {
                    "vllm:prompt_tokens_total": 100.0,
                    "vllm:num_requests_running": 3.0,
                    "vllm:kv_cache_usage_perc": 0.4,
                    "vllm:estimated_flops_per_gpu_total": 60.0,
                },
                {
                    "vllm:prompt_tokens_total": 80.0,
                    "vllm:num_requests_running": 2.0,
                    "vllm:kv_cache_usage_perc": 0.7,
                    "vllm:estimated_flops_per_gpu_total": 40.0,
                },
            ],
        ):
            metrics = profile._scrape_model_metrics(
                ["http://metrics-0", "http://metrics-1"]
            )

        self.assertEqual(metrics["vllm:prompt_tokens_total"], 180.0)
        self.assertEqual(metrics["vllm:num_requests_running"], 5.0)
        self.assertEqual(metrics["vllm:kv_cache_usage_perc"], 0.7)
        self.assertEqual(
            metrics["vllm:estimated_flops_per_gpu_total"],
            50.0,
        )

    def test_build_adaptive_config_preserves_controller_across_submissions(self) -> None:
        traces = []
        config = profile._build_adaptive_config(
            scheduling_policy="aimd",
            metrics_urls=["http://metrics"],
            trace_events=traces,
            min_window=4,
            max_window=16,
            initial_window=4,
            sample_interval_s=0.25,
            ewma_alpha=0.3,
            pid_proportional_gain=0.5,
            pid_integral_gain=0.1,
            pid_derivative_gain=0.05,
            hol_age_congestion_s=2.0,
            hol_age_low_load_s=0.5,
        )
        try:
            self.assertIs(config["trace_events"], traces)
            self.assertEqual(config["admission_gate"].limit, 4)
            self.assertEqual(config["controller_name"], "aimd")
            self.assertEqual(config["min_window"], 4)
            self.assertEqual(config["max_window"], 16)
        finally:
            provider = config.get("observation_provider")
            if provider is not None:
                provider.close()

    def test_typed_adaptive_config_uses_nonblocking_provider(self) -> None:
        config = profile._build_adaptive_config(
            scheduling_policy="aimd",
            metrics_urls=["http://metrics"],
            trace_events=[],
            min_window=4,
            max_window=16,
            initial_window=4,
            sample_interval_s=0.25,
            ewma_alpha=0.5,
            pid_proportional_gain=1.0,
            pid_integral_gain=0.0,
            pid_derivative_gain=0.0,
            hol_age_congestion_s=2.0,
            hol_age_low_load_s=0.5,
        )
        try:
            self.assertIsInstance(
                config["observation_provider"],
                NonBlockingMetricsObservationProvider,
            )
        finally:
            provider = config.get("observation_provider")
            if provider is not None:
                provider.close()

    def test_hol_adaptive_config_uses_interval_clocked_provider(self) -> None:
        with patch.object(
            profile,
            "_service_metrics_snapshot",
            side_effect=AssertionError("HOL must not scrape service metrics"),
        ):
            config = profile._build_adaptive_config(
                scheduling_policy="aimd_hol",
                metrics_urls=["http://metrics"],
                trace_events=[],
                min_window=4,
                max_window=16,
                initial_window=4,
                sample_interval_s=0.25,
                ewma_alpha=0.5,
                pid_proportional_gain=1.0,
                pid_integral_gain=0.0,
                pid_derivative_gain=0.0,
                hol_age_congestion_s=2.0,
                hol_age_low_load_s=0.5,
            )
            config["admission_gate"].decide(0, hol_age_s=0.0)

        self.assertIsInstance(
            config["observation_provider"],
            CachedMetricsObservationProvider,
        )

    def test_typed_adaptive_decision_does_not_wait_for_metrics_scrape(self) -> None:
        sampler_entered = threading.Event()
        release_sampler = threading.Event()

        def blocked_snapshot(_metrics_url):
            sampler_entered.set()
            release_sampler.wait()
            return ServiceMetricsSnapshot(4, 0, 0.25)

        with patch.object(
            profile,
            "_service_metrics_snapshot",
            side_effect=blocked_snapshot,
        ):
            config = profile._build_adaptive_config(
                scheduling_policy="aimd",
                metrics_urls=["http://metrics"],
                trace_events=[],
                min_window=4,
                max_window=16,
                initial_window=4,
                sample_interval_s=0.25,
                ewma_alpha=0.5,
                pid_proportional_gain=1.0,
                pid_integral_gain=0.0,
                pid_derivative_gain=0.0,
                hol_age_congestion_s=2.0,
                hol_age_low_load_s=0.5,
            )
            try:
                self.assertTrue(sampler_entered.wait(timeout=1.0))
                started_at = time.perf_counter()
                decision = config["admission_gate"].decide(0)
                elapsed_s = time.perf_counter() - started_at

                self.assertLess(elapsed_s, 0.05)
                self.assertEqual(decision.limit, 4)
            finally:
                release_sampler.set()
                provider = config.get("observation_provider")
                if provider is not None:
                    provider.close()

    def test_run_once_closes_adaptive_provider_when_submission_fails(self) -> None:
        args = profile.parse_args(
            [
                "--database-url",
                "postgresql://unused",
                "--executor",
                "ray_task",
                "--scheduling-policy",
                "aimd",
                "--model-metrics-url",
                "http://metrics",
                "--total-rows",
                "1",
            ]
        )
        table = pa.table({"doc_id": [1], "prompt": ["hello"]})
        connection = Mock()
        provider = SimpleNamespace(close=Mock())
        adaptive_config = {
            "admission_gate": object(),
            "observation_provider": provider,
            "trace_events": [],
        }
        source = SimpleNamespace(
            fetch=Mock(
                return_value=SimpleNamespace(
                    table=table,
                    metrics={"db_fetch_s": 0.0, "arrow_build_s": 0.0},
                )
            )
        )
        organizer = SimpleNamespace(
            organize=Mock(
                return_value=SimpleNamespace(
                    batches=[table],
                    batch_cost_units=(),
                    batch_row_counts=(),
                    metrics={
                        "packing_scope": "organizer_input",
                        "organizer_from_arrow_s": 0.0,
                        "organizer_plan_s": 0.0,
                        "organizer_collect_s": 0.0,
                        "organization_policy_family": "fixed_rows",
                        "batch_prompt_token_spread_mean": 0.0,
                        "prefix_group_ratio": 0.0,
                        "partition_effective": "true",
                        "warnings": "",
                    },
                )
            )
        )
        ray_module = SimpleNamespace(
            init=Mock(),
            remote=Mock(return_value=_RecordingRemoteDefinition()),
        )

        with (
            patch.object(profile, "connect", return_value=connection),
            patch.object(profile, "gpu_metadata", return_value={}),
            patch.object(
                profile,
                "database_metadata",
                return_value={"server_version": "18.4", "pgvector_version": "0.8.2"},
            ),
            patch.object(
                profile,
                "embedding_vector_column_dim",
                return_value=args.embedding_dim,
            ),
            patch.object(profile, "create_job", return_value=1),
            patch.object(profile, "require_ray", return_value=ray_module),
            patch.object(profile, "scrape_prometheus_metrics", return_value={}),
            patch.object(profile, "make_source", return_value=source),
            patch.object(profile, "make_organizer", return_value=organizer),
            patch.object(
                profile,
                "_build_adaptive_config",
                return_value=adaptive_config,
            ),
            patch.object(
                profile,
                "submit_ray_tasks",
                side_effect=RuntimeError("submission failed"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "submission failed"):
                profile.run_once(args, "formal", 1)

        provider.close.assert_called_once_with()
        connection.close.assert_called_once_with()

    def test_fail_job_rolls_back_before_marking_failed(self) -> None:
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value

        profile.fail_job(connection, 7)

        connection.rollback.assert_called_once_with()
        cursor.execute.assert_called_once()
        self.assertIn("status = 'failed'", cursor.execute.call_args.args[0])
        connection.commit.assert_called_once_with()

    def test_run_once_marks_created_job_failed_and_preserves_base_exception(self) -> None:
        args = profile.parse_args(
            [
                "--database-url",
                "postgresql://unused",
                "--executor",
                "ray_task",
            ]
        )
        connection = Mock()
        original = KeyboardInterrupt("ray initialization interrupted")

        with (
            patch.object(profile, "connect", return_value=connection),
            patch.object(profile, "gpu_metadata", return_value={}),
            patch.object(
                profile,
                "database_metadata",
                return_value={"server_version": "18.4", "pgvector_version": "0.8.2"},
            ),
            patch.object(
                profile,
                "embedding_vector_column_dim",
                return_value=args.embedding_dim,
            ),
            patch.object(profile, "create_job", return_value=17),
            patch.object(profile, "require_ray", side_effect=original),
            patch.object(profile, "fail_job") as fail_job,
        ):
            with self.assertRaises(KeyboardInterrupt) as raised:
                profile.run_once(args, "formal", 1)

        self.assertIs(raised.exception, original)
        fail_job.assert_called_once_with(connection, 17)

    def test_fail_job_error_is_not_allowed_to_mask_original_error(self) -> None:
        args = profile.parse_args(
            [
                "--database-url",
                "postgresql://unused",
                "--executor",
                "ray_task",
            ]
        )
        connection = Mock()
        original = RuntimeError("ray initialization failed")

        with (
            patch.object(profile, "connect", return_value=connection),
            patch.object(profile, "gpu_metadata", return_value={}),
            patch.object(
                profile,
                "database_metadata",
                return_value={"server_version": "18.4", "pgvector_version": "0.8.2"},
            ),
            patch.object(
                profile,
                "embedding_vector_column_dim",
                return_value=args.embedding_dim,
            ),
            patch.object(profile, "create_job", return_value=17),
            patch.object(profile, "require_ray", side_effect=original),
            patch.object(
                profile,
                "fail_job",
                side_effect=OSError("database unavailable"),
            ),
        ):
            with self.assertRaises(RuntimeError) as raised:
                profile.run_once(args, "formal", 1)

        self.assertIs(raised.exception, original)
        self.assertTrue(
            any("database unavailable" in note for note in raised.exception.__notes__)
        )

    def test_main_preflights_output_before_formal_run(self) -> None:
        args = profile.parse_args(["--database-url", "postgresql://unused"])
        events = []

        def run_once(run_args, phase, repeat_index):
            events.append(
                ("run_once", run_args.dry_run, phase, repeat_index)
            )
            return {"status": "dry_run" if run_args.dry_run else "ok"}

        def preflight(path, keys, **options):
            events.append(("preflight", path, tuple(keys), options))

        with (
            patch.object(profile, "parse_args", return_value=args),
            patch.object(
                profile,
                "iter_requested_runs",
                return_value=iter([("formal", 1)]),
            ),
            patch.object(profile, "run_once", side_effect=run_once),
            patch.object(
                profile,
                "preflight_metrics_schema",
                side_effect=preflight,
            ),
            patch.object(profile, "append_metrics"),
            patch("builtins.print"),
        ):
            profile.main()

        self.assertEqual(events[0], ("run_once", True, "formal", 1))
        self.assertEqual(events[1][0], "preflight")
        self.assertEqual(
            events[1][2],
            profile.FORMAL_RESULT_FIELDS,
        )
        self.assertEqual(events[1][3], {})
        self.assertEqual(events[2], ("run_once", False, "formal", 1))

    def test_formal_result_schema_rejects_missing_non_ray_field(self) -> None:
        row = {field: "" for field in profile.FORMAL_RESULT_FIELDS}
        del row["server_version"]

        with self.assertRaisesRegex(RuntimeError, "formal result schema drift"):
            profile._validated_formal_result_row(row)

    def test_formal_result_schema_rejects_reordered_non_ray_field(self) -> None:
        fields = list(profile.FORMAL_RESULT_FIELDS)
        fields[0], fields[1] = fields[1], fields[0]
        row = {field: "" for field in fields}

        with self.assertRaisesRegex(RuntimeError, "formal result schema drift"):
            profile._validated_formal_result_row(row)

    def test_static_run_never_builds_adaptive_provider(self) -> None:
        args = profile.parse_args(
            [
                "--database-url",
                "postgresql://unused",
                "--executor",
                "python",
                "--total-rows",
                "1",
            ]
        )
        connection = Mock()
        source = SimpleNamespace(fetch=Mock(side_effect=RuntimeError("source failed")))

        with (
            patch.object(profile, "connect", return_value=connection),
            patch.object(profile, "gpu_metadata", return_value={}),
            patch.object(
                profile,
                "database_metadata",
                return_value={"server_version": "18.4", "pgvector_version": "0.8.2"},
            ),
            patch.object(
                profile,
                "embedding_vector_column_dim",
                return_value=args.embedding_dim,
            ),
            patch.object(profile, "create_job", return_value=1),
            patch.object(profile, "scrape_prometheus_metrics", return_value={}),
            patch.object(profile, "make_source", return_value=source),
            patch.object(profile, "_build_adaptive_config") as build_adaptive,
        ):
            with self.assertRaisesRegex(RuntimeError, "source failed"):
                profile.run_once(args, "formal", 1)

        build_adaptive.assert_not_called()
        connection.close.assert_called_once_with()

    def test_build_adaptive_config_requires_metrics_url(self) -> None:
        with self.assertRaisesRegex(ValueError, "metrics URL"):
            profile._build_adaptive_config(
                scheduling_policy="pid",
                metrics_urls=[],
                trace_events=[],
                min_window=2,
                max_window=16,
                initial_window=2,
                sample_interval_s=0.25,
                ewma_alpha=0.3,
                pid_proportional_gain=0.5,
                pid_integral_gain=0.1,
                pid_derivative_gain=0.05,
                hol_age_congestion_s=2.0,
                hol_age_low_load_s=0.5,
            )

    def test_write_control_trace_emits_plot_ready_rows(self) -> None:
        events = [
            AdmissionTraceEvent(
                observed_at_s=10.0,
                fresh=True,
                inflight=3,
                window=6,
                running=10,
                waiting=0,
                kv_usage=0.2,
                controller_action="increase",
                reason="low_load",
                allowed=True,
                sample_age_s=0.1,
                hol_age_s=0.25,
            ),
            AdmissionTraceEvent(
                observed_at_s=10.5,
                fresh=True,
                inflight=6,
                window=4,
                running=12,
                waiting=2,
                kv_usage=0.7,
                controller_action="decrease",
                reason="queue_congestion",
                allowed=False,
                sample_age_s=None,
            ),
        ]
        output = Path("control_trace.csv")
        captured_rows = []
        with patch(
            "src.profiling.traces.append_metrics",
            side_effect=lambda path, row: captured_rows.append((path, row)),
        ):
            profile._write_control_trace(
                output,
                experiment_id="experiment",
                phase="formal",
                repeat_index=2,
                job_id=9,
                server_version="18.4",
                pgvector_version="0.8.2",
                controller_name="aimd",
                trace_events=events,
            )

        self.assertEqual(len(captured_rows), 2)
        self.assertTrue(all(path == output for path, _ in captured_rows))
        rows = [row for _, row in captured_rows]
        self.assertEqual(rows[0]["elapsed_s"], 0.0)
        self.assertEqual(rows[1]["elapsed_s"], 0.5)
        self.assertEqual(rows[1]["k_max"], 4)
        self.assertEqual(rows[1]["controller_action"], "decrease")
        self.assertEqual(rows[0]["sample_age_s"], 0.1)
        self.assertEqual(rows[1]["sample_age_s"], "")
        self.assertEqual(rows[0]["hol_age_s"], 0.25)
        self.assertEqual(rows[1]["hol_age_s"], "")
        self.assertEqual(rows[0]["schema_version"], 2)
        self.assertEqual(rows[1]["server_version"], "18.4")
        self.assertEqual(rows[1]["pgvector_version"], "0.8.2")

    def test_build_routing_config_resolves_endpoint_assignments(self) -> None:
        config = profile._build_routing_config(
            endpoint_count=3,
            endpoint_routing="least_queued",
            pool_routing="request_cost",
            pool_ids_text="short,long,prefix",
            gpu_ids_text="0,0,1",
            long_request_tokens=1024,
        )

        self.assertIsInstance(
            config["endpoint_router"], LeastQueuedEndpointRouter
        )
        self.assertIsInstance(config["pool_router"], RequestPoolRouter)
        self.assertEqual(config["pool_ids"], ["short", "long", "prefix"])
        self.assertEqual(config["gpu_ids"], ["0", "0", "1"])

    def test_build_routing_config_defaults_gpu_ids_by_endpoint(self) -> None:
        config = profile._build_routing_config(
            endpoint_count=2,
            endpoint_routing="round_robin",
            pool_routing="none",
            pool_ids_text=None,
            gpu_ids_text=None,
            long_request_tokens=0,
        )

        self.assertEqual(config["gpu_ids"], ["0", "1"])

    def test_build_routing_config_rejects_assignment_count_mismatch(self) -> None:
        with self.assertRaisesRegex(ValueError, "pool IDs"):
            profile._build_routing_config(
                endpoint_count=2,
                endpoint_routing="round_robin",
                pool_routing="none",
                pool_ids_text="short",
                gpu_ids_text=None,
                long_request_tokens=1024,
            )

    def test_row_arrivals_preserve_complete_arrow_rows_and_metadata(self) -> None:
        table = pa.table(
            {
                "doc_id": pa.array([11, 12], type=pa.int64()),
                "tenant_id": pa.array([3, 4], type=pa.int32()),
                "text": ["alpha", "beta"],
                "prompt_tokens": pa.array([7, 9], type=pa.int32()),
                "prefix_key": ["shared", "other"],
                "arrival_time_s": pa.array([2.5, 2.75], type=pa.float64()),
            }
        )

        arrivals = profile_replay._row_arrivals(
            table,
            completion_max_tokens=5,
        )

        self.assertEqual(
            [
                (
                    item.row_id,
                    item.arrival_s,
                    item.prompt_tokens,
                    item.estimated_output_tokens,
                    item.prefix_key,
                )
                for item in arrivals
            ],
            [
                ("11", 2.5, 7, 5, "shared"),
                ("12", 2.75, 9, 5, "other"),
            ],
        )
        for index, arrival in enumerate(arrivals):
            self.assertIsInstance(arrival.payload_ref, pa.Table)
            self.assertEqual(arrival.payload_ref.schema, table.schema)
            self.assertEqual(
                arrival.payload_ref.to_pylist(),
                table.slice(index, 1).to_pylist(),
            )
            self.assertEqual(
                arrival.payload_ref.column("doc_id").chunk(0).buffers()[1].address,
                table.column("doc_id").chunk(0).buffers()[1].address,
            )

    def test_row_arrivals_use_trace_target_output_cost(self) -> None:
        table = pa.table(
            {
                "doc_id": [11, 12],
                "prompt_tokens": [7, 9],
                "target_output_tokens": [6, 2],
                "arrival_time_s": [2.5, 2.75],
            }
        )

        arrivals = profile_replay._row_arrivals(
            table,
            completion_max_tokens=16,
            output_cost_mode="trace_target_output",
        )

        self.assertEqual(
            [item.estimated_output_tokens for item in arrivals],
            [6, 2],
        )

    def test_trace_output_cost_requires_target_column(self) -> None:
        table = pa.table(
            {
                "doc_id": [11],
                "prompt_tokens": [7],
                "arrival_time_s": [2.5],
            }
        )

        with self.assertRaisesRegex(ValueError, "target_output_tokens"):
            profile_replay._row_arrivals(
                table,
                completion_max_tokens=16,
                output_cost_mode="trace_target_output",
            )

    def test_arrow_envelope_reconstructs_schema_order_and_values_exactly_once(
        self,
    ) -> None:
        table = pa.table(
            {
                "doc_id": pa.array([11, 12], type=pa.int64()),
                "text": ["alpha", "beta"],
                "prompt_tokens": pa.array([7, 9], type=pa.int32()),
                "prefix_key": ["shared", "shared"],
                "arrival_time_s": pa.array([2.5, 2.75], type=pa.float64()),
            }
        )
        arrivals = profile_replay._row_arrivals(
            table,
            completion_max_tokens=5,
        )
        builder = PendingBatchBuilder(max_rows=2, token_budget=0)
        for arrival in arrivals:
            builder.add(arrival)

        envelope = profile_replay._arrow_envelope(
            builder.close(),
            batch_index=4,
            job_id="job-7",
            operator="ai_complete",
        )

        self.assertEqual(envelope.payload.schema, table.schema)
        self.assertEqual(envelope.payload.column_names, table.column_names)
        self.assertEqual(envelope.payload.to_pylist(), table.to_pylist())
        self.assertEqual(envelope.payload.num_rows, table.num_rows)
        self.assertEqual(envelope.request.request_id, "job-7:batch:4")
        self.assertEqual(envelope.request.row_count, 2)
        self.assertEqual(envelope.request.prompt_tokens, 16)
        self.assertEqual(envelope.request.estimated_output_tokens, 10)
        self.assertEqual(envelope.request.prefix_key, "shared")
        self.assertEqual(envelope.request.first_arrival_s, 2.5)
        self.assertEqual(envelope.request.oldest_arrival_s, 2.5)

    def test_row_arrivals_reject_invalid_and_decreasing_arrival_values(self) -> None:
        invalid_columns = [
            pa.array([None], type=pa.float64()),
            pa.array([-0.1], type=pa.float64()),
            pa.array([True], type=pa.bool_()),
            pa.array([float("nan")], type=pa.float64()),
            pa.array([float("inf")], type=pa.float64()),
        ]
        for arrival_column in invalid_columns:
            with self.subTest(arrival=arrival_column.to_pylist()):
                table = pa.table(
                    {
                        "doc_id": [1],
                        "prompt_tokens": [1],
                        "arrival_time_s": arrival_column,
                    }
                )
                with self.assertRaisesRegex(ValueError, "arrival_time_s"):
                    profile_replay._row_arrivals(
                        table,
                        completion_max_tokens=0,
                    )

        decreasing = pa.table(
            {
                "doc_id": [1, 2],
                "prompt_tokens": [1, 1],
                "arrival_time_s": [2.0, 1.0],
            }
        )
        with self.assertRaisesRegex(ValueError, "non-decreasing"):
            profile_replay._row_arrivals(
                decreasing,
                completion_max_tokens=0,
            )

    def test_multiple_arrow_chunks_share_one_arrival_replay_origin(self) -> None:
        clock = _DeterministicReplayClock(now_s=100.0)
        args = SimpleNamespace(
            ray_batch_rows=8,
            batching_policy="fixed_rows",
            token_budget=0,
            flush_policy="fixed_timeout",
            flush_timeout_ms=1000.0,
            flush_max_wait_ms=2000.0,
            _replay_clock=clock,
        )
        tables = [
            pa.table(
                {
                    "doc_id": [1],
                    "prompt_tokens": [2],
                    "arrival_time_s": [10.0],
                }
            ),
            pa.table(
                {
                    "doc_id": [2],
                    "prompt_tokens": [3],
                    "arrival_time_s": [10.25],
                }
            ),
        ]
        trace_events = []

        envelopes = list(
            profile._arrival_replay_envelopes(
                tables,
                args,
                job_id="job",
                operator="ai_embed",
                service_observation=lambda: ReplayServiceObservation(
                    fresh=True,
                    running=0,
                    waiting=0,
                    kv_usage=0.0,
                ),
                trace_sink=trace_events,
            )
        )

        self.assertEqual(clock.waited_until, [100.25])
        self.assertEqual(len(envelopes), 1)
        self.assertEqual(
            envelopes[0].payload.column("doc_id").to_pylist(),
            [1, 2],
        )
        self.assertTrue(trace_events)

    def test_replay_timestamps_clamp_intended_arrival_to_observed_flush(
        self,
    ) -> None:
        args = SimpleNamespace(
            ray_batch_rows=2,
            batching_policy="fixed_rows",
            token_budget=0,
            flush_policy="fixed_timeout",
            flush_timeout_ms=1000.0,
            flush_max_wait_ms=2000.0,
            _replay_clock=_DeterministicReplayClock(now_s=0.0),
        )
        table = pa.table(
            {
                "doc_id": [1, 2],
                "prompt_tokens": [2, 3],
                "arrival_time_s": [0.0, 0.01],
            }
        )
        lifecycle_seeds = []
        epoch_values = iter([100.0, 100.005])

        list(
            profile._arrival_replay_envelopes(
                [table],
                args,
                job_id="job",
                operator="ai_embed",
                service_observation=lambda: ReplayServiceObservation(
                    fresh=True,
                    running=0,
                    waiting=0,
                    kv_usage=0.0,
                ),
                trace_sink=[],
                lifecycle_seed_sink=lifecycle_seeds,
                epoch_clock=lambda: next(epoch_values),
            )
        )

        self.assertEqual(
            [item.arrival_epoch_s for item in lifecycle_seeds],
            [100.0, 100.005],
        )
        self.assertEqual(
            [item.flush_epoch_s for item in lifecycle_seeds],
            [100.005, 100.005],
        )

    def test_replay_can_preserve_a_shared_fixed_epoch_origin(self) -> None:
        args = SimpleNamespace(
            ray_batch_rows=1,
            batching_policy="fixed_rows",
            token_budget=0,
            flush_policy="fixed_timeout",
            flush_timeout_ms=1000.0,
            flush_max_wait_ms=2000.0,
            _replay_clock=_DeterministicReplayClock(now_s=10.0),
        )
        table = pa.table(
            {
                "doc_id": [1],
                "prompt_tokens": [2],
                "arrival_time_s": [0.0],
            }
        )
        lifecycle_seeds = []

        list(
            profile._arrival_replay_envelopes(
                [table],
                args,
                job_id="job",
                operator="ai_embed",
                service_observation=lambda: ReplayServiceObservation(
                    fresh=True,
                    running=0,
                    waiting=0,
                    kv_usage=0.0,
                ),
                trace_sink=[],
                lifecycle_seed_sink=lifecycle_seeds,
                epoch_clock=lambda: 110.0,
                replay_origin_epoch_s=100.0,
            )
        )

        self.assertEqual(lifecycle_seeds[0].arrival_epoch_s, 100.0)
        self.assertEqual(lifecycle_seeds[0].flush_epoch_s, 110.0)

    def test_token_budget_membership_survives_arrow_assembly(self) -> None:
        args = SimpleNamespace(
            ray_batch_rows=8,
            batching_policy="token_budget",
            token_budget=10,
            flush_policy="fixed_timeout",
            flush_timeout_ms=1000.0,
            flush_max_wait_ms=2000.0,
            _replay_clock=_DeterministicReplayClock(),
        )
        table = pa.table(
            {
                "doc_id": [1, 2, 3],
                "text": ["one", "two", "oversized"],
                "prompt_tokens": [6, 6, 12],
                "arrival_time_s": [0.0, 0.0, 0.0],
            }
        )
        packing = []

        envelopes = list(
            profile._arrival_replay_envelopes(
                [table],
                args,
                job_id="job",
                operator="ai_embed",
                service_observation=lambda: ReplayServiceObservation(
                    fresh=True,
                    running=0,
                    waiting=0,
                    kv_usage=0.0,
                ),
                trace_sink=[],
                packing_sink=packing,
            )
        )

        self.assertEqual(
            [envelope.payload.column("doc_id").to_pylist() for envelope in envelopes],
            [[1], [2], [3]],
        )
        self.assertEqual(
            [envelope.request.prompt_tokens for envelope in envelopes],
            [6, 6, 12],
        )
        self.assertEqual(packing, [(6, 1), (6, 1), (12, 1)])

    def test_arrival_replay_emits_one_lifecycle_seed_per_complete_row(self) -> None:
        replay_clock = _DeterministicReplayClock()
        args = SimpleNamespace(
            ray_batch_rows=8,
            batching_policy="fixed_rows",
            token_budget=0,
            flush_policy="fixed_timeout",
            flush_timeout_ms=25.0,
            flush_max_wait_ms=50.0,
            max_inflight=8,
            arrival_time_scale=0.001,
            completion_max_tokens=4,
            _replay_clock=replay_clock,
        )
        table = pa.table(
            {
                "doc_id": [1, 2],
                "prompt_tokens": [10, 20],
                "arrival_time_s": [5.0, 15.0],
                "prefix_key": ["p", "p"],
            }
        )
        seeds = []

        envelopes = list(
            profile._arrival_replay_envelopes(
                [table],
                args,
                job_id="job",
                operator="ai_complete",
                service_observation=lambda: ReplayServiceObservation(
                    fresh=False,
                    running=None,
                    waiting=None,
                    kv_usage=None,
                ),
                trace_sink=[],
                lifecycle_seed_sink=seeds,
                epoch_clock=lambda: 1_000.0
                + (replay_clock.now() - 100.0),
            )
        )

        self.assertEqual(len(envelopes), 1)
        self.assertEqual(
            [seed.request_id for seed in seeds],
            ["job:row:1", "job:row:2"],
        )
        self.assertEqual(
            {seed.submission_id for seed in seeds},
            {"job:batch:0"},
        )
        self.assertEqual(
            [seed.arrival_epoch_s for seed in seeds],
            [1_000.0, 1_000.01],
        )
        self.assertEqual(
            [seed.flush_epoch_s for seed in seeds],
            [1_000.01, 1_000.01],
        )

    def test_request_granularity_expands_closed_batch_into_complete_requests(
        self,
    ) -> None:
        args = SimpleNamespace(
            ray_batch_rows=8,
            batching_policy="fixed_rows",
            token_budget=0,
            flush_policy="fixed_timeout",
            flush_timeout_ms=25.0,
            flush_max_wait_ms=50.0,
            max_inflight=8,
            arrival_time_scale=0.001,
            submission_granularity="request",
            _replay_clock=_DeterministicReplayClock(),
        )
        table = pa.table(
            {
                "doc_id": [1, 2],
                "text": ["one", "two"],
                "prompt_tokens": [10, 20],
                "arrival_time_s": [5.0, 15.0],
                "prefix_key": ["p", "p"],
            }
        )
        seeds = []
        packing = []

        envelopes = list(
            profile._arrival_replay_envelopes(
                [table],
                args,
                job_id="job",
                operator="ai_embed",
                service_observation=lambda: ReplayServiceObservation(
                    fresh=False,
                    running=None,
                    waiting=None,
                    kv_usage=None,
                ),
                trace_sink=[],
                lifecycle_seed_sink=seeds,
                packing_sink=packing,
                epoch_clock=lambda: 1_000.0,
            )
        )

        self.assertEqual(len(envelopes), 2)
        self.assertEqual(
            [item.payload.column("doc_id").to_pylist() for item in envelopes],
            [[1], [2]],
        )
        self.assertEqual(
            [item.request.request_id for item in envelopes],
            ["job:request:1", "job:request:2"],
        )
        self.assertEqual(
            [item.request.row_count for item in envelopes],
            [1, 1],
        )
        self.assertEqual(
            [item.submission_id for item in seeds],
            ["job:request:1", "job:request:2"],
        )
        self.assertEqual(
            {item.latency_granularity for item in seeds},
            {"request"},
        )
        self.assertEqual(packing, [(30, 2)])

    def test_service_quantum_granularity_expands_replay_batch_without_row_split(
        self,
    ) -> None:
        args = SimpleNamespace(
            ray_batch_rows=8,
            batching_policy="fixed_rows",
            token_budget=0,
            flush_policy="fixed_timeout",
            flush_timeout_ms=25.0,
            flush_max_wait_ms=50.0,
            max_inflight=8,
            arrival_time_scale=0.001,
            submission_granularity="service_quantum",
            service_quantum_tokens=10,
            completion_max_tokens=0,
            output_cost_mode="fixed_output_cap",
            _replay_clock=_DeterministicReplayClock(),
        )
        table = pa.table(
            {
                "doc_id": [1, 2, 3],
                "prompt_tokens": [6, 4, 7],
                "arrival_time_s": [5.0, 10.0, 15.0],
                "prefix_key": ["p", "p", "p"],
            }
        )
        seeds = []
        planning = []
        quanta = []

        envelopes = list(
            profile._arrival_replay_envelopes(
                [table],
                args,
                job_id="job",
                operator="ai_embed",
                service_observation=lambda: ReplayServiceObservation(
                    fresh=False,
                    running=None,
                    waiting=None,
                    kv_usage=None,
                ),
                trace_sink=[],
                lifecycle_seed_sink=seeds,
                packing_sink=planning,
                quantum_sink=quanta,
                epoch_clock=lambda: 1_000.0,
            )
        )

        self.assertEqual(
            [item.request.request_id for item in envelopes],
            ["job:batch:0:quantum:0", "job:batch:0:quantum:1"],
        )
        self.assertEqual(
            [item.payload.column("doc_id").to_pylist() for item in envelopes],
            [[1, 2], [3]],
        )
        self.assertEqual(
            [item.submission_id for item in seeds],
            ["job:batch:0:quantum:0"] * 2 + ["job:batch:0:quantum:1"],
        )
        self.assertEqual({item.latency_granularity for item in seeds}, {"submission"})
        self.assertEqual(planning, [(17, 3)])
        self.assertEqual(quanta, [(10, 2, False), (7, 1, False)])

    def test_queue_adaptive_uses_max_inflight_for_pressure_window(self) -> None:
        args = SimpleNamespace(
            ray_batch_rows=8,
            batching_policy="fixed_rows",
            token_budget=0,
            flush_policy="queue_adaptive",
            flush_timeout_ms=25.0,
            flush_max_wait_ms=50.0,
            max_inflight=4,
            _replay_clock=_DeterministicReplayClock(),
        )
        table = pa.table(
            {
                "doc_id": [1, 2],
                "prompt_tokens": [1, 1],
                "arrival_time_s": [0.0, 0.04],
            }
        )

        envelopes = list(
            profile._arrival_replay_envelopes(
                [table],
                args,
                job_id="job",
                operator="ai_embed",
                service_observation=lambda: ReplayServiceObservation(
                    fresh=True,
                    running=4,
                    waiting=0,
                    kv_usage=0.0,
                ),
                trace_sink=[],
            )
        )

        self.assertEqual(
            [envelope.payload.column("doc_id").to_pylist() for envelope in envelopes],
            [[1, 2]],
        )

    def test_nonadaptive_flush_never_reads_service_metrics(self) -> None:
        class BlockingMetricsMustNotRun:
            def latest(self, inflight):
                raise AssertionError("non-adaptive flush must not read metrics")

        for flush_policy in ("immediate", "fixed_timeout"):
            with self.subTest(flush_policy=flush_policy):
                args = SimpleNamespace(
                    ray_batch_rows=2,
                    batching_policy="fixed_rows",
                    token_budget=0,
                    flush_policy=flush_policy,
                    flush_timeout_ms=25.0,
                    flush_max_wait_ms=50.0,
                    _replay_clock=_DeterministicReplayClock(),
                )
                table = pa.table(
                    {
                        "doc_id": [1],
                        "prompt_tokens": [1],
                        "arrival_time_s": [0.0],
                    }
                )

                envelopes = list(
                    profile._arrival_replay_envelopes(
                        [table],
                        args,
                        job_id="job",
                        operator="ai_embed",
                        service_observation=BlockingMetricsMustNotRun(),
                        trace_sink=[],
                    )
                )

                self.assertEqual(len(envelopes), 1)

    def test_dry_run_records_default_and_explicit_replay_configuration(self) -> None:
        default_args = profile.parse_args(["--dry-run"])
        default_row = profile.run_once(default_args, "formal", 1)

        self.assertFalse(default_row["arrival_replay"])
        self.assertEqual(default_row["submission_granularity"], "batch")
        self.assertEqual(default_row["arrival_time_scale"], 1.0)
        self.assertEqual(default_row["arrival_replay_start_epoch_s"], 0.0)
        self.assertEqual(
            default_row["arrival_replay_observed_start_epoch_s"],
            0.0,
        )
        self.assertEqual(default_row["flush_policy"], "immediate")
        self.assertEqual(default_row["flush_timeout_ms"], 25.0)
        self.assertEqual(default_row["flush_max_wait_ms"], 50.0)
        self.assertEqual(default_row["flush_trace_output"], "")
        self.assertEqual(default_row["flush_trace_path"], "")
        self.assertEqual(default_row["token_budget_policy"], "static")

        implicit_trace_args = profile.parse_args(
            [
                "--dry-run",
                "--executor",
                "ray_task",
                "--data-source",
                "daft_postgres",
                "--source-order",
                "arrival_time",
                "--arrival-replay",
            ]
        )
        implicit_trace_row = profile.run_once(implicit_trace_args, "formal", 1)

        self.assertEqual(implicit_trace_row["flush_trace_output"], "")
        self.assertEqual(
            implicit_trace_row["flush_trace_path"],
            str(
                Path("feasibility/results/postgres_ai_operator_profile_flush_trace.csv")
            ),
        )

        replay_args = profile.parse_args(
            [
                "--dry-run",
                "--executor",
                "ray_task",
                "--data-source",
                "daft_postgres",
                "--source-order",
                "arrival_time",
                "--arrival-replay",
                "--submission-granularity",
                "request",
                "--arrival-time-scale",
                "0.0005",
                "--arrival-replay-start-epoch-s",
                "123.5",
                "--flush-policy",
                "fixed_timeout",
                "--flush-timeout-ms",
                "12.5",
                "--flush-max-wait-ms",
                "30",
                "--flush-trace-output",
                "trace.csv",
            ]
        )
        replay_row = profile.run_once(replay_args, "formal", 1)

        self.assertTrue(replay_row["arrival_replay"])
        self.assertEqual(replay_row["submission_granularity"], "request")
        self.assertEqual(replay_row["arrival_time_scale"], 0.0005)
        self.assertEqual(
            replay_row["arrival_replay_start_epoch_s"],
            123.5,
        )
        self.assertEqual(replay_row["flush_policy"], "fixed_timeout")
        self.assertEqual(replay_row["flush_timeout_ms"], 12.5)
        self.assertEqual(replay_row["flush_max_wait_ms"], 30.0)
        self.assertEqual(replay_row["flush_trace_output"], "trace.csv")
        self.assertEqual(replay_row["flush_trace_path"], "trace.csv")
        self.assertEqual(
            replay_row["arrival_replay_preload"],
            "bounded_requested_workload",
        )

    def test_replay_start_epoch_requires_arrival_replay(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--arrival-replay-start-epoch-s",
                "123.5",
            ]
        )

        with self.assertRaisesRegex(SystemExit, "requires --arrival-replay"):
            profile.run_once(args, "formal", 1)

    def test_wait_for_replay_start_sleeps_until_target(self) -> None:
        times = iter([100.0, 105.0])
        sleeps = []

        observed = profile._wait_for_replay_start(
            105.0,
            wall_clock=lambda: next(times),
            sleeper=sleeps.append,
        )

        self.assertEqual(sleeps, [5.0])
        self.assertEqual(observed, 105.0)

        dynamic_args = profile.parse_args(
            [
                "--dry-run",
                "--executor",
                "ray_task",
                "--data-source",
                "daft_postgres",
                "--source-order",
                "arrival_time",
                "--arrival-replay",
                "--batching-policy",
                "token_budget",
                "--token-budget",
                "4096",
                "--token-budget-policy",
                "service_quantum",
                "--token-budget-candidates",
                "2048,4096,8192",
            ]
        )
        dynamic_row = profile.run_once(dynamic_args, "formal", 1)

        self.assertEqual(
            dynamic_row["token_budget_policy"],
            "service_quantum",
        )
        self.assertEqual(
            dynamic_row["token_budget_candidates"],
            "2048,4096,8192",
        )

        shared_args = profile.parse_args(
            [
                "--dry-run",
                "--executor",
                "ray_task",
                "--ray-address",
                "auto",
                "--admission-scope",
                "per_endpoint",
                "--max-inflight",
                "64",
                "--max-active-work-per-endpoint",
                "32768",
                "--endpoint-routing",
                "least_work",
                "--shared-credit-coordinator-name",
                "test-credits",
                "--shared-credit-request-limit",
                "64",
                "--shared-credit-work-limit",
                "32768",
                "--shared-credit-quantum",
                "2048",
                "--shared-credit-job-weight",
                "2",
            ]
        )
        shared_row = profile.run_once(shared_args, "formal", 1)

        self.assertEqual(shared_row["max_active_work_per_endpoint"], 32768)
        self.assertEqual(shared_row["endpoint_routing"], "least_work")
        self.assertEqual(
            shared_row["shared_credit_coordinator_name"],
            "test-credits",
        )
        self.assertEqual(shared_row["shared_credit_request_limit"], 64)
        self.assertEqual(shared_row["shared_credit_work_limit"], 32768)
        self.assertEqual(shared_row["shared_credit_quantum"], 2048)
        self.assertEqual(shared_row["shared_credit_job_weight"], 2)

    def test_shared_credit_requires_explicit_ray_cluster_address(
        self,
    ) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--executor",
                "ray_task",
                "--shared-credit-coordinator-name",
                "test-credits",
                "--shared-credit-request-limit",
                "64",
                "--shared-credit-work-limit",
                "32768",
                "--shared-credit-quantum",
                "2048",
            ]
        )

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(SystemExit, "Ray address"):
                profile.run_once(args, "formal", 1)

    def test_dry_run_records_output_cost_provenance(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--output-cost-mode",
                "trace_target_output",
                "--cost-model-id",
                "qwen-test",
                "--cost-tokenizer-id",
                "qwen-tokenizer-test",
            ]
        )

        row = profile.run_once(args, "formal", 1)

        self.assertEqual(row["output_cost_mode"], "trace_target_output")
        self.assertEqual(
            row["output_cost_source"],
            "burstgpt_unpaired_trace_metadata",
        )
        self.assertEqual(row["cost_model_id"], "qwen-test")
        self.assertEqual(row["cost_tokenizer_id"], "qwen-tokenizer-test")
        self.assertEqual(row["packing_cost_unit"], "tokens")
        self.assertEqual(row["packing_algorithm"], "fixed_rows")
        self.assertEqual(row["packing_scope"], "organizer_input")
        self.assertEqual(row["packing_input_rows"], 0)
        self.assertEqual(row["packing_batch_count"], 0)

    def test_dry_run_records_exact_completion_observation_opt_in(
        self,
    ) -> None:
        default_row = profile.run_once(
            profile.parse_args(["--dry-run"]),
            "formal",
            1,
        )
        enabled_row = profile.run_once(
            profile.parse_args(
                [
                    "--dry-run",
                    "--operator",
                    "ai_complete",
                    "--model-backend",
                    "compatible_http",
                    "--completion-endpoint-url",
                    "http://localhost/v1/completions",
                    "--completion-return-token-ids",
                ]
            ),
            "formal",
            1,
        )

        self.assertFalse(default_row["completion_return_token_ids"])
        self.assertTrue(enabled_row["completion_return_token_ids"])
        self.assertEqual(default_row["completion_prompt_format"], "raw")
        self.assertEqual(default_row["completion_temperature"], "")
        self.assertEqual(default_row["source_max_prompt_tokens"], "")
        self.assertEqual(
            default_row["request_actual_output_tokens_observed"],
            0,
        )
        self.assertEqual(default_row["request_finish_reason_observed"], 0)

        chatml_row = profile.run_once(
            profile.parse_args(
                [
                    "--dry-run",
                    "--operator",
                    "ai_complete",
                    "--model-backend",
                    "compatible_http",
                    "--completion-endpoint-url",
                    "http://localhost/v1/completions",
                    "--completion-prompt-format",
                    "chatml",
                    "--completion-temperature",
                    "0",
                ]
            ),
            "formal",
            1,
        )
        self.assertEqual(chatml_row["completion_prompt_format"], "chatml")
        self.assertEqual(chatml_row["completion_temperature"], 0.0)

        filtered_row = profile.run_once(
            profile.parse_args(
                [
                    "--dry-run",
                    "--source-max-prompt-tokens",
                    "1500",
                ]
            ),
            "formal",
            1,
        )
        self.assertEqual(filtered_row["source_max_prompt_tokens"], 1500)

    def test_chat_protocol_is_recorded_in_dry_run_summary(self) -> None:
        row = profile.run_once(
            profile.parse_args(
                [
                    "--dry-run",
                    "--operator",
                    "ai_complete",
                    "--completion-protocol",
                    "chat_completions",
                ]
            ),
            "formal",
            0,
        )

        self.assertEqual(
            row["completion_protocol"],
            "chat_completions",
        )

    def test_dry_run_rejects_invalid_completion_observation_inputs(
        self,
    ) -> None:
        invalid_argv = (
            ["--dry-run", "--completion-temperature", "-0.1"],
            ["--dry-run", "--completion-temperature", "nan"],
            ["--dry-run", "--source-max-prompt-tokens", "0"],
            [
                "--dry-run",
                "--operator",
                "ai_complete",
                "--model-backend",
                "ollama",
                "--completion-return-token-ids",
            ],
            [
                "--dry-run",
                "--operator",
                "ai_complete",
                "--model-backend",
                "fake",
                "--completion-prompt-format",
                "chatml",
            ],
        )

        for argv in invalid_argv:
            with self.subTest(argv=argv):
                with self.assertRaises(SystemExit):
                    profile.run_once(
                        profile.parse_args(argv),
                        "formal",
                        1,
                    )

    def test_dry_run_records_resource_and_mfu_provenance(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--resource-sample-interval-s",
                "0.5",
                "--model-flops-per-token",
                "3000000000",
                "--gpu-peak-tflops",
                "100",
                "--mfu-precision",
                "bf16",
            ]
        )

        row = profile.run_once(args, "formal", 1)

        self.assertEqual(row["resource_sample_interval_s"], 0.5)
        self.assertEqual(row["resource_metrics_status"], "unavailable")
        self.assertEqual(row["gpu_utilization_pct_mean"], "")
        self.assertEqual(row["gpu_energy_j"], "")
        self.assertEqual(row["model_flops_per_token"], 3_000_000_000.0)
        self.assertEqual(row["gpu_peak_tflops"], 100.0)
        self.assertEqual(row["mfu_precision"], "bf16")
        self.assertEqual(row["mfu_estimate"], "")
        self.assertEqual(
            row["mfu_estimation_method"],
            "configured_flops_per_observed_token",
        )

    def test_dry_run_reports_row_cap_aware_packing_algorithm(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--batching-policy",
                "row_cap_aware_token_budget",
                "--token-budget",
                "10",
                "--ray-batch-rows",
                "3",
            ]
        )

        row = profile.run_once(args, "formal", 1)

        self.assertEqual(
            row["packing_algorithm"],
            "row_cap_aware_best_fit_decreasing",
        )
        self.assertEqual(row["packing_scope"], "organizer_input")

    def test_resource_sample_interval_must_be_positive(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--resource-sample-interval-s",
                "0",
            ]
        )

        with self.assertRaisesRegex(SystemExit, "resource-sample-interval-s"):
            profile.run_once(args, "formal", 1)

    def test_packing_run_metrics_aggregate_exact_batch_costs(self) -> None:
        metrics = profile._packing_run_metrics(
            batch_cost_units=[8, 12],
            batch_row_counts=[2, 1],
            capacity=10,
            row_cap=2,
            packing_scope="fetch_chunk_local",
            packing_algorithm="best_fit_decreasing",
        )

        self.assertEqual(metrics["packing_algorithm"], "best_fit_decreasing")
        self.assertEqual(metrics["packing_scope"], "fetch_chunk_local")
        self.assertEqual(metrics["packing_budget_utilization_mean"], 0.8)
        self.assertEqual(metrics["packing_budget_utilization_p95"], 0.8)
        self.assertEqual(metrics["packing_oversized_rows"], 1)
        self.assertEqual(metrics["packing_input_rows"], 3)
        self.assertEqual(metrics["packing_batch_count"], 2)
        self.assertEqual(metrics["batch_estimated_cost_units_p50"], 8.0)
        self.assertEqual(metrics["batch_estimated_cost_units_p95"], 12.0)
        self.assertEqual(metrics["batch_estimated_cost_units_p99"], 12.0)
        self.assertEqual(metrics["batch_estimated_cost_units_max"], 12)
        self.assertEqual(metrics["organization_batch_count"], 2)
        self.assertEqual(metrics["organization_batch_rows_mean"], 1.5)
        self.assertEqual(metrics["organization_batch_rows_max"], 2)
        self.assertEqual(metrics["organization_batch_cost_units_mean"], 10.0)
        self.assertEqual(metrics["organization_batch_cost_units_p95"], 12.0)
        self.assertEqual(metrics["organization_row_cap_hit_ratio"], 0.5)

    def test_request_submission_metrics_preserve_organization_shape(
        self,
    ) -> None:
        metrics = profile._packing_run_metrics(
            batch_cost_units=[30],
            batch_row_counts=[2],
            capacity=0,
            row_cap=8,
            packing_scope="arrival_order",
            packing_algorithm="sequential_pending",
        )

        self.assertEqual(metrics["organization_batch_count"], 1)
        self.assertEqual(metrics["organization_batch_rows_mean"], 2.0)
        self.assertEqual(metrics["organization_batch_rows_max"], 2)
        self.assertEqual(metrics["organization_batch_cost_units_mean"], 30.0)
        self.assertEqual(metrics["organization_batch_cost_units_p95"], 30.0)
        self.assertEqual(metrics["organization_row_cap_hit_ratio"], 0.0)

    def test_request_trace_cli_requires_supported_typed_ray_path(self) -> None:
        invalid_cases = [
            (
                [
                    "--dry-run",
                    "--request-trace-output",
                    "tmp/requests.csv",
                    "--executor",
                    "python",
                ],
                "request tracing requires a Ray executor",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--request-trace-output",
                    "tmp/requests.csv",
                    "--scheduling-policy",
                    "queue_adaptive",
                ],
                "request tracing requires the typed scheduler",
            ),
            (
                [
                    "--dry-run",
                    "--request-slo-ms",
                    "-1",
                ],
                "request-slo-ms must be non-negative",
            ),
        ]

        for argv, message in invalid_cases:
            with self.subTest(argv=argv):
                with self.assertRaisesRegex(SystemExit, message):
                    profile.run_once(profile.parse_args(argv), "formal", 1)

        args = profile.parse_args(
            [
                "--dry-run",
                "--arrival-replay",
                "--data-source",
                "daft_postgres",
                "--source-order",
                "arrival_time",
                "--request-trace-output",
                "tmp/requests.csv",
                "--request-slo-ms",
                "250",
                "--scenario-id",
                "fixed-timeout",
                "--random-seed",
                "7",
            ]
        )
        row = profile.run_once(args, "formal", 1)

        self.assertEqual(row["request_trace_path"], "tmp/requests.csv")
        self.assertEqual(row["request_slo_target_ms"], 250.0)
        self.assertEqual(row["scenario_id"], "fixed-timeout")
        self.assertEqual(row["random_seed"], 7)

        offline_args = profile.parse_args(
            [
                "--dry-run",
                "--request-trace-output",
                "tmp/offline-requests.csv",
            ]
        )
        offline_row = profile.run_once(offline_args, "formal", 1)

        self.assertEqual(
            offline_row["request_trace_path"],
            "tmp/offline-requests.csv",
        )

    def test_request_submission_granularity_allows_offline_execution(
        self,
    ) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--submission-granularity",
                "request",
            ]
        )

        row = profile.run_once(args, "formal", 1)

        self.assertFalse(row["arrival_replay"])
        self.assertEqual(row["submission_granularity"], "request")

    def test_request_manifest_cli_accepts_pinned_project_runtime(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--request-manifest",
                "manifest.jsonl",
                "--endpoint-routing",
                "manifest_pinned",
            ]
        )

        self.assertEqual(args.request_manifest, "manifest.jsonl")
        self.assertEqual(args.endpoint_routing, "manifest_pinned")

    def test_source_row_offset_is_recorded_and_must_be_non_negative(
        self,
    ) -> None:
        row = profile.run_once(
            profile.parse_args(
                [
                    "--dry-run",
                    "--source-row-offset",
                    "512",
                ]
            ),
            "formal",
            1,
        )
        self.assertEqual(row["source_row_offset"], 512)

        with self.assertRaisesRegex(
            SystemExit,
            "source-row-offset must be non-negative",
        ):
            profile.run_once(
                profile.parse_args(
                    [
                        "--dry-run",
                        "--source-row-offset",
                        "-1",
                    ]
                ),
                "formal",
                1,
            )

    def test_manifest_pinned_routing_requires_request_manifest(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--endpoint-routing",
                "manifest_pinned",
            ]
        )

        with self.assertRaisesRegex(
            SystemExit,
            "manifest_pinned routing requires --request-manifest",
        ):
            profile.run_once(args, "formal", 1)

    def test_request_manifest_dry_run_records_frozen_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest_path = Path(directory) / "requests.jsonl"
            metadata = write_manifest(
                manifest_path,
                (
                    ChatRequest(
                        doc_id=1,
                        prompt="one",
                        arrival_time_s=0.0,
                        prompt_tokens=3,
                        max_output_tokens=256,
                        estimated_output_tokens=7,
                        source_row_hash="row-1",
                        endpoint_index=0,
                    ),
                    ChatRequest(
                        doc_id=2,
                        prompt="two",
                        arrival_time_s=0.0,
                        prompt_tokens=4,
                        max_output_tokens=256,
                        estimated_output_tokens=8,
                        source_row_hash="row-2",
                        endpoint_index=1,
                    ),
                ),
            )
            args = profile.parse_args(
                [
                    "--dry-run",
                    "--request-manifest",
                    str(manifest_path),
                    "--total-rows",
                    "2",
                    "--operator",
                    "ai_complete",
                    "--model-backend",
                    "compatible_http",
                    "--completion-endpoint-urls",
                    "http://gpu0/v1,http://gpu1/v1",
                    "--completion-protocol",
                    "chat_completions",
                    "--completion-prompt-format",
                    "raw",
                    "--completion-temperature",
                    "0",
                    "--completion-max-tokens",
                    "256",
                    "--output-cost-mode",
                    "trace_target_output",
                    "--executor",
                    "ray_actor",
                    "--actor-workers-per-endpoint",
                    "1",
                    "--submission-granularity",
                    "request",
                    "--endpoint-routing",
                    "manifest_pinned",
                ]
            )

            row = profile.run_once(args, "formal", 1)

        self.assertEqual(row["request_manifest_path"], str(manifest_path))
        self.assertEqual(row["request_manifest_sha256"], metadata.sha256)
        self.assertEqual(row["request_manifest_rows"], 2)
        self.assertEqual(row["request_manifest_validated_rows"], 0)
        self.assertEqual(
            row["request_manifest_validation_status"],
            "not_executed",
        )

    def test_single_run_mode_selects_exact_phase_and_repeat(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--run-phase",
                "formal",
                "--run-repeat-index",
                "4",
            ]
        )

        self.assertEqual(
            list(profile.iter_requested_runs(args)),
            [("formal", 4)],
        )

        invalid_argv = [
            ["--dry-run", "--run-phase", "formal"],
            ["--dry-run", "--run-repeat-index", "1"],
            [
                "--dry-run",
                "--run-phase",
                "formal",
                "--run-repeat-index",
                "0",
            ],
        ]
        for argv in invalid_argv:
            with self.subTest(argv=argv):
                with self.assertRaisesRegex(SystemExit, "single-run"):
                    list(profile.iter_requested_runs(profile.parse_args(argv)))

    def test_replay_validation_rejects_invalid_formal_paths(self) -> None:
        invalid_cases = [
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "doc_id",
                ],
                "source-order arrival_time",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "arrow_postgres",
                    "--source-order",
                    "arrival_time",
                ],
                "data-source daft_postgres",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--executor",
                    "python",
                ],
                "Ray executor",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--flush-timeout-ms",
                    "-1",
                ],
                "flush-timeout-ms",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--flush-max-wait-ms",
                    "0",
                ],
                "flush-max-wait-ms",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--flush-policy",
                    "queue_adaptive",
                    "--flush-timeout-ms",
                    "50",
                    "--flush-max-wait-ms",
                    "25",
                ],
                "flush-max-wait-ms >= --flush-timeout-ms",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--flush-policy",
                    "queue_adaptive",
                    "--flush-timeout-ms",
                    "0",
                ],
                "adaptive flush requires --flush-timeout-ms > 0",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--flush-policy",
                    "slo_ewma",
                    "--request-slo-ms",
                    "0",
                ],
                "slo-ewma flush requires --request-slo-ms > 0",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--flush-policy",
                    "slo_ewma",
                    "--request-slo-ms",
                    "30000",
                ],
                "requires calibrated",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--flush-ewma-alpha",
                    "0",
                ],
                "flush-ewma-alpha",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--flush-deadband-ratio",
                    "1.1",
                ],
                "flush-deadband-ratio",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--batching-policy",
                    "length_align_fixed_rows",
                ],
                "offline reordering",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--batching-policy",
                    "best_fit_token_budget",
                    "--token-budget",
                    "10",
                ],
                "does not support best_fit_token_budget",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--batching-policy",
                    "row_cap_aware_token_budget",
                    "--token-budget",
                    "10",
                ],
                "does not support row_cap_aware_token_budget",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--arrival-time-scale",
                    "0",
                ],
                "arrival-time-scale",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--arrival-time-scale",
                    "nan",
                ],
                "arrival-time-scale",
            ),
        ]
        for argv, message in invalid_cases:
            with self.subTest(argv=argv):
                with self.assertRaisesRegex(SystemExit, message):
                    profile.run_once(profile.parse_args(argv), "formal", 1)

    def test_replay_disabled_retains_batch_envelope_behavior(self) -> None:
        args = profile.parse_args(["--dry-run"])
        batch = pa.table(
            {
                "doc_id": [1],
                "prompt_tokens": [3],
                "arrival_time_s": [1.0],
            }
        )

        envelopes = profile_replay._batch_envelopes(
            [batch],
            job_id="job",
            operator="ai_embed",
            completion_max_tokens=0,
        )

        self.assertFalse(args.arrival_replay)
        self.assertEqual(len(envelopes), 1)
        self.assertIs(envelopes[0].payload, batch)
        self.assertEqual(envelopes[0].request.request_id, "job:batch:0")

    def test_run_scheduler_consumes_a_single_pass_lazy_iterable(self) -> None:
        batch = pa.table({"doc_id": [1], "prompt_tokens": [3]})
        envelope = profile_replay._batch_envelopes(
            [batch],
            job_id="job",
            operator="ai_embed",
            completion_max_tokens=0,
        )[0]
        consumed = []

        def envelopes():
            consumed.append("started")
            yield envelope

        remote = _RecordingRemote()
        topology = profile_ray._endpoint_topology(
            ["endpoint-0"],
            ["ray://task/0"],
        )

        lifecycle_events = []
        results, metrics = profile_ray._run_scheduler(
            _ImmediateRay,
            envelopes(),
            topology,
            {"endpoint-0": lambda payload: remote.remote(payload)},
            profile_ray.StaticAdmissionController(1),
            submission_lifecycle_sink=lifecycle_events,
        )

        self.assertEqual(consumed, ["started"])
        self.assertEqual(results, [{"call_index": 0}])
        self.assertEqual(metrics["operator_invocations"], 1)
        self.assertEqual(
            [event.submission_id for event in lifecycle_events],
            ["job:batch:0"],
        )

    def test_flush_trace_writer_emits_all_fields_and_propagates_errors(self) -> None:
        events = [
            FlushTraceEvent(
                elapsed_s=0.125,
                pending_rows=2,
                pending_tokens=17,
                oldest_age_s=0.025,
                action="flush",
                reason="fixed_timeout",
                selected_wait_s=0.025,
                window_reason="fixed_timeout",
            )
        ]
        test_tmp_root = CODE_ROOT.parent / "tmp"
        test_tmp_root.mkdir(exist_ok=True)
        output = test_tmp_root / "task3_flush_trace_test.csv"
        output.unlink(missing_ok=True)
        try:
            profile._write_flush_trace(
                output,
                experiment_id="experiment",
                phase="formal",
                repeat_index=2,
                job_id=9,
                server_version="18.4",
                pgvector_version="0.8.2",
                flush_policy="fixed_timeout",
                flush_timeout_ms=25.0,
                flush_max_wait_ms=50.0,
                arrival_time_scale=0.0005,
                trace_events=events,
            )
            with output.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 1)
            self.assertEqual(
                set(rows[0]),
                {
                    "schema_version",
                    "experiment_id",
                    "phase",
                    "repeat_index",
                    "job_id",
                    "server_version",
                    "pgvector_version",
                    "flush_policy",
                    "flush_timeout_ms",
                    "flush_max_wait_ms",
                    "arrival_time_scale",
                    "trace_index",
                    "elapsed_s",
                    "pending_rows",
                    "pending_tokens",
                    "oldest_age_s",
                    "action",
                    "reason",
                    "selected_wait_s",
                    "window_reason",
                    "selected_token_budget",
                    "token_budget_reason",
                    "arrival_rate_tokens_s",
                    "service_rate_tokens_s_per_endpoint",
                },
            )
            self.assertEqual(rows[0]["pending_rows"], "2")
            self.assertEqual(rows[0]["schema_version"], "3")
            self.assertEqual(rows[0]["reason"], "fixed_timeout")
            self.assertEqual(rows[0]["arrival_time_scale"], "0.0005")
            self.assertEqual(rows[0]["selected_wait_s"], "0.025")
            self.assertEqual(rows[0]["window_reason"], "fixed_timeout")

            with self.assertRaises(OSError):
                profile._write_flush_trace(
                    test_tmp_root,
                    experiment_id="experiment",
                    phase="formal",
                    repeat_index=2,
                    job_id=9,
                    server_version="18.4",
                    pgvector_version="0.8.2",
                    flush_policy="fixed_timeout",
                    flush_timeout_ms=25.0,
                    flush_max_wait_ms=50.0,
                    arrival_time_scale=0.0005,
                    trace_events=events,
                )
        finally:
            output.unlink(missing_ok=True)

    def test_submission_and_resource_trace_writers_preserve_run_identity(self) -> None:
        test_tmp_root = CODE_ROOT.parent / "tmp"
        test_tmp_root.mkdir(exist_ok=True)
        submission_output = test_tmp_root / "submission_trace_test.csv"
        resource_output = test_tmp_root / "resource_trace_test.csv"
        submission_output.unlink(missing_ok=True)
        resource_output.unlink(missing_ok=True)
        try:
            profile._write_submission_trace(
                submission_output,
                experiment_id="experiment",
                phase="formal",
                repeat_index=2,
                job_id=9,
                server_version="18.4",
                pgvector_version="0.8.2",
                results=[
                    {
                        "doc_id": [11, 12],
                        "rows": 2,
                        "token_count": 30,
                        "input_token_count": 20,
                        "output_token_count": 10,
                        "service_s": 0.2,
                        "service_start_epoch_s": 100.0,
                        "service_end_epoch_s": 100.2,
                        "http_request_start_epoch_s": 100.01,
                        "http_response_headers_epoch_s": 100.18,
                        "http_response_body_epoch_s": 100.19,
                        "http_headers_wait_s": 0.17,
                        "http_body_read_s": 0.01,
                    }
                ],
                submission_events=[
                    SubmissionLifecycleEvent(
                        submission_id="9:request:11",
                        pool_id="default",
                        endpoint_id="endpoint-1",
                        gpu_id="1",
                        submit_epoch_s=99.9,
                        completion_epoch_s=100.2,
                        status="completed",
                        planning_batch_id="9:batch:0",
                        service_quantum_index=1,
                        service_quantum_oversized=False,
                        actor_worker_id="endpoint-1:worker:3",
                        actor_worker_index=3,
                        actor_worker_pid=4321,
                    )
                ],
            )
            profile._write_resource_trace(
                resource_output,
                experiment_id="experiment",
                phase="formal",
                repeat_index=2,
                job_id=9,
                server_version="18.4",
                pgvector_version="0.8.2",
                samples=[
                    {
                        "sample_index": 0,
                        "sample_epoch_s": 100.1,
                        "gpu_utilization_pct": "50",
                        "vllm_num_requests_running": 2,
                    }
                ],
            )
            with submission_output.open(newline="", encoding="utf-8") as handle:
                submission = list(csv.DictReader(handle))
            with resource_output.open(newline="", encoding="utf-8") as handle:
                resource = list(csv.DictReader(handle))

            self.assertEqual(submission[0]["doc_ids"], "11;12")
            self.assertEqual(submission[0]["schema_version"], "5")
            self.assertEqual(submission[0]["submission_id"], "9:request:11")
            self.assertEqual(submission[0]["planning_batch_id"], "9:batch:0")
            self.assertEqual(submission[0]["service_quantum_index"], "1")
            self.assertEqual(submission[0]["service_quantum_oversized"], "False")
            self.assertAlmostEqual(float(submission[0]["credit_held_s"]), 0.3)
            self.assertAlmostEqual(float(submission[0]["ray_to_service_s"]), 0.1)
            self.assertEqual(
                submission[0]["actor_worker_id"],
                "endpoint-1:worker:3",
            )
            self.assertEqual(submission[0]["actor_worker_index"], "3")
            self.assertEqual(submission[0]["actor_worker_pid"], "4321")
            self.assertEqual(submission[0]["endpoint_id"], "endpoint-1")
            self.assertEqual(submission[0]["gpu_id"], "1")
            self.assertEqual(submission[0]["job_id"], "9")
            self.assertEqual(submission[0]["server_version"], "18.4")
            self.assertEqual(submission[0]["pgvector_version"], "0.8.2")
            self.assertEqual(
                submission[0]["http_request_start_epoch_s"],
                "100.01",
            )
            self.assertEqual(
                submission[0]["http_response_headers_epoch_s"],
                "100.18",
            )
            self.assertEqual(submission[0]["http_headers_wait_s"], "0.17")
            self.assertEqual(submission[0]["http_body_read_s"], "0.01")
            self.assertEqual(resource[0]["gpu_utilization_pct"], "50")
            self.assertEqual(resource[0]["repeat_index"], "2")
        finally:
            submission_output.unlink(missing_ok=True)
            resource_output.unlink(missing_ok=True)

    def test_request_trace_writer_emits_versioned_plot_ready_rows(self) -> None:
        test_tmp_root = CODE_ROOT.parent / "tmp"
        test_tmp_root.mkdir(exist_ok=True)
        output = test_tmp_root / "request_trace_test.csv"
        output.unlink(missing_ok=True)
        rows = [
            RequestTraceRow(
                request_id="job:row:11",
                submission_id="job:batch:0",
                doc_id="11",
                pool_id="default",
                endpoint_id="task-0",
                gpu_id="0",
                prompt_tokens=10,
                estimated_output_tokens=4,
                client_estimated_output_tokens=2,
                actual_output_tokens=None,
                output_token_source="submission_aggregate_unavailable",
                total_tokens=None,
                prefix_key="shared",
                status="completed",
                error_type="",
                arrival_epoch_s=100.0,
                flush_epoch_s=100.025,
                submit_epoch_s=100.030,
                service_start_epoch_s=100.040,
                completion_epoch_s=100.300,
                buffer_s=0.025,
                submit_to_service_s=0.010,
                service_s=0.250,
                e2e_s=0.300,
                request_time_origin="replayed_arrival",
                latency_granularity="submission",
                slo_target_s=0.250,
                slo_met=False,
            ),
            RequestTraceRow(
                request_id="job:row:12",
                submission_id="job:batch:0",
                doc_id="12",
                pool_id="default",
                endpoint_id="task-0",
                gpu_id="0",
                prompt_tokens=20,
                estimated_output_tokens=4,
                client_estimated_output_tokens=1,
                actual_output_tokens=None,
                output_token_source="submission_aggregate_unavailable",
                total_tokens=None,
                prefix_key="shared",
                status="completed",
                error_type="",
                arrival_epoch_s=100.010,
                flush_epoch_s=100.025,
                submit_epoch_s=100.030,
                service_start_epoch_s=100.040,
                completion_epoch_s=100.300,
                buffer_s=0.015,
                submit_to_service_s=0.010,
                service_s=0.250,
                e2e_s=0.290,
                request_time_origin="replayed_arrival",
                latency_granularity="submission",
                slo_target_s=0.250,
                slo_met=False,
            ),
        ]
        expected_columns = [
            "schema_version",
            "experiment_id",
            "phase",
            "repeat_index",
            "scenario_id",
            "random_seed",
            "job_id",
            "server_version",
            "pgvector_version",
            "request_index",
            "request_id",
            "submission_id",
            "doc_id",
            "pool_id",
            "endpoint_id",
            "gpu_id",
            "prompt_tokens",
            "estimated_output_tokens",
            "client_estimated_output_tokens",
            "actual_output_tokens",
            "output_token_source",
            "total_tokens",
            "finish_reason",
            "prefix_key",
            "status",
            "error_type",
            "arrival_epoch_s",
            "flush_epoch_s",
            "submit_epoch_s",
            "service_start_epoch_s",
            "completion_epoch_s",
            "buffer_s",
            "submit_to_service_s",
            "service_s",
            "service_clock_domain",
            "e2e_s",
            "request_time_origin",
            "latency_granularity",
            "slo_target_s",
            "slo_met",
        ]

        try:
            profile._write_request_trace(
                output,
                experiment_id="experiment",
                phase="formal",
                repeat_index=2,
                scenario_id="fixed-timeout",
                random_seed=7,
                job_id=9,
                server_version="18.4",
                pgvector_version="0.8.2",
                rows=rows,
            )
            with output.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                written = list(reader)
                self.assertEqual(reader.fieldnames, expected_columns)

            self.assertEqual(len(written), 2)
            self.assertEqual(written[0]["schema_version"], "3")
            self.assertEqual(
                written[0]["request_time_origin"],
                "replayed_arrival",
            )
            self.assertEqual(written[0]["scenario_id"], "fixed-timeout")
            self.assertEqual(written[0]["server_version"], "18.4")
            self.assertEqual(written[0]["actual_output_tokens"], "")
            self.assertEqual(written[1]["e2e_s"], "0.29")
        finally:
            output.unlink(missing_ok=True)

    def test_profiler_builds_request_rows_without_splitting_aggregate_usage(
        self,
    ) -> None:
        seeds = [
            RequestLifecycleSeed(
                "job:row:11",
                "job:batch:0",
                "11",
                10,
                4,
                "shared",
                100.0,
                100.025,
                "replayed_arrival",
            ),
            RequestLifecycleSeed(
                "job:row:12",
                "job:batch:0",
                "12",
                20,
                4,
                "shared",
                100.010,
                100.025,
                "replayed_arrival",
            ),
        ]
        events = [
            SubmissionLifecycleEvent(
                "job:batch:0",
                "default",
                "task-0",
                "0",
                100.030,
                100.300,
                "completed",
            )
        ]
        results = [
            {
                "doc_id": [11, 12],
                "output_text": ["one two", ""],
                "output_token_count": 2,
                "output_token_counts": [2, 0],
                "finish_reasons": ["stop", "length"],
                "service_start_epoch_s": 100.040,
                "service_end_epoch_s": 100.290,
            }
        ]

        rows = profile._build_profiler_request_rows(
            seeds,
            events,
            results,
            operator="ai_complete",
            slo_target_s=0.250,
        )

        self.assertEqual(
            [row.client_estimated_output_tokens for row in rows],
            [2, 0],
        )
        self.assertEqual([row.actual_output_tokens for row in rows], [2, 0])
        self.assertEqual(
            [row.finish_reason for row in rows],
            ["stop", "length"],
        )
        self.assertEqual(
            {row.output_token_source for row in rows},
            {"endpoint_request"},
        )

        metrics = profile._request_trace_metrics(rows, e2e_s=0.5)
        self.assertAlmostEqual(metrics["request_e2e_s_p50"], 0.29)
        self.assertAlmostEqual(metrics["request_e2e_s_p95"], 0.30)
        self.assertAlmostEqual(metrics["request_e2e_s_p99"], 0.30)
        self.assertEqual(metrics["request_slo_violation_ratio"], 1.0)
        self.assertEqual(metrics["request_slo_goodput_per_s"], 0.0)
        self.assertEqual(metrics["latency_granularity"], "submission")
        self.assertEqual(metrics["request_actual_output_tokens_observed"], 2)
        self.assertEqual(metrics["request_actual_output_tokens_p50"], 0)
        self.assertEqual(metrics["request_finish_reason_observed"], 2)
        self.assertEqual(metrics["request_finish_reason_stop_ratio"], 0.5)
        self.assertEqual(metrics["request_finish_reason_length_ratio"], 0.5)

    def test_vllm_tokens_per_second_uses_observed_prometheus_deltas(self) -> None:
        stats = {
            "vllm_prompt_tokens_delta": 900,
            "vllm_generation_tokens_delta": 100,
        }

        self.assertEqual(profile._vllm_tokens_per_second(stats, 4.0), 250.0)
        self.assertEqual(profile._vllm_tokens_per_second(stats, 0.0), 0.0)


class _DeterministicReplayClock:
    def __init__(self, now_s: float = 100.0) -> None:
        self.current_s = now_s
        self.waited_until = []

    def now(self) -> float:
        return self.current_s

    def wait_until(self, deadline_s: float) -> None:
        self.waited_until.append(deadline_s)
        self.current_s = deadline_s


class _ImmediateRef:
    def __init__(self, result: object):
        self.result = result


class _ImmediateRay:
    @staticmethod
    def wait(handles, num_returns, timeout=None):
        del timeout
        return handles[:num_returns], handles[num_returns:]

    @staticmethod
    def get(handle):
        if isinstance(handle, list):
            return [item.result for item in handle]
        return handle.result


class _RecordingRemote:
    def __init__(self):
        self.calls = []

    def remote(self, *args):
        self.calls.append(args)
        return _ImmediateRef({"call_index": len(self.calls) - 1})


class StaticTaskSchedulingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.batches = [
            pa.table({"doc_id": [1], "prompt_tokens": [10]}),
            pa.table({"doc_id": [2], "prompt_tokens": [20]}),
        ]

    def _submit(self, remote, **overrides):
        arguments = {
            "ray_module": _ImmediateRay,
            "remote_embed": remote,
            "batches": self.batches,
            "max_inflight": 2,
            "operator": "ai_embed",
            "embedding_dim": 16,
            "model_backend": "fake",
            "endpoint_urls": [],
            "model_name": "model",
            "api_key": None,
            "timeout_s": 5.0,
            "completion_max_tokens": 8,
            "adaptive_config": None,
        }
        arguments.update(overrides)
        return profile.submit_ray_tasks(**arguments)

    def test_static_task_path_delegates_to_shared_scheduler(self) -> None:
        remote = _RecordingRemote()
        expected = ([{"ok": True}], {"operator_invocations": 1})

        with patch.object(
            profile_ray,
            "_run_static_scheduler",
            return_value=expected,
        ) as run:
            actual = self._submit(remote)

        self.assertEqual(actual, expected)
        run.assert_called_once()

    def test_fake_task_submitter_preserves_operator_arguments(self) -> None:
        remote = _RecordingRemote()

        results, metrics = self._submit(remote)

        self.assertEqual(results, [{"call_index": 0}, {"call_index": 1}])
        self.assertEqual(
            [(call[0], call[1]) for call in remote.calls],
            [(self.batches[0], 16), (self.batches[1], 16)],
        )
        self.assertEqual(metrics["operator_invocations"], 2)
        self.assertEqual(metrics["max_inflight"], 2)
        self.assertEqual(metrics["adaptive_downshifts"], 0)

    def test_http_task_submitters_route_across_endpoint_urls(self) -> None:
        remote = _RecordingRemote()

        self._submit(
            remote,
            model_backend="compatible_http",
            endpoint_urls=["http://one", "http://two"],
        )

        self.assertEqual(
            [call[1] for call in remote.calls],
            ["http://one", "http://two"],
        )
        self.assertTrue(all(call[2:] == ("model", None, 5.0) for call in remote.calls))

    def test_vllm_task_submitter_requests_per_choice_token_ids(self) -> None:
        remote = _RecordingRemote()

        self._submit(
            remote,
            operator="ai_complete",
            model_backend="compatible_http",
            endpoint_urls=["http://one"],
            completion_return_token_ids=True,
        )

        self.assertTrue(all(call[-4] is True for call in remote.calls))
        self.assertTrue(all(call[-3] == "raw" for call in remote.calls))
        self.assertTrue(all(call[-2] is None for call in remote.calls))
        self.assertTrue(
            all(call[-1] == "completions" for call in remote.calls)
        )
        self.assertTrue(all(len(call) == 10 for call in remote.calls))

    def test_adaptive_task_path_remains_isolated_from_static_scheduler(self) -> None:
        remote = _RecordingRemote()
        adaptive_config = {}

        with patch.object(profile_ray, "_run_static_scheduler") as run:
            self._submit(remote, adaptive_config=adaptive_config)

        run.assert_not_called()

    def test_typed_adaptive_task_path_uses_dynamic_gate_without_legacy_loop(self) -> None:
        remote = _RecordingRemote()
        traces = []
        gate = DynamicAdmissionGate(
            AimdAdmissionController(initial_window=4),
            CachedMetricsObservationProvider(
                lambda: ServiceMetricsSnapshot(10, 0, 0.2),
                min_sample_interval_s=0.0,
            ),
            trace_sink=traces.append,
        )

        results, metrics = self._submit(
            remote,
            adaptive_config={
                "admission_gate": gate,
                "trace_events": traces,
            },
        )

        self.assertEqual(len(results), 2)
        self.assertGreater(metrics["adaptive_upshifts"], 0)
        self.assertEqual(metrics["adaptive_downshifts"], 0)
        self.assertGreaterEqual(metrics["adaptive_limit_mean"], 4)

    def test_prebuilt_replay_envelopes_feed_existing_scheduler_lazily(self) -> None:
        remote = _RecordingRemote()
        consumed = []
        envelope = profile_replay._batch_envelopes(
            [self.batches[0]],
            job_id="replay",
            operator="ai_embed",
            completion_max_tokens=0,
        )[0]

        def replay_envelopes():
            consumed.append("started")
            yield envelope

        results, metrics = profile.submit_ray_tasks(
            ray_module=_ImmediateRay,
            remote_embed=remote,
            batches=[],
            max_inflight=2,
            operator="ai_embed",
            embedding_dim=16,
            model_backend="fake",
            endpoint_urls=[],
            model_name="model",
            api_key=None,
            timeout_s=5.0,
            completion_max_tokens=8,
            replay_envelopes=replay_envelopes(),
        )

        self.assertEqual(consumed, ["started"])
        self.assertEqual(results, [{"call_index": 0}])
        self.assertEqual(metrics["operator_invocations"], 1)
        self.assertIs(remote.calls[0][0], envelope.payload)

    def test_replay_envelopes_cover_typed_and_legacy_task_paths(self) -> None:
        envelope = profile_replay._batch_envelopes(
            [self.batches[0]],
            job_id="replay",
            operator="ai_embed",
            completion_max_tokens=0,
        )[0]
        for policy in ("typed", "legacy"):
            with self.subTest(policy=policy):
                remote = _RecordingRemote()
                if policy == "typed":
                    traces = []
                    adaptive_config = {
                        "admission_gate": DynamicAdmissionGate(
                            AimdAdmissionController(initial_window=4),
                            CachedMetricsObservationProvider(
                                lambda: ServiceMetricsSnapshot(10, 0, 0.2),
                                min_sample_interval_s=0.0,
                            ),
                            trace_sink=traces.append,
                        ),
                        "trace_events": traces,
                    }
                else:
                    adaptive_config = {}

                results, metrics = self._submit(
                    remote,
                    adaptive_config=adaptive_config,
                    model_backend="compatible_http",
                    endpoint_urls=["http://local-test-endpoint"],
                    replay_envelopes=iter([envelope]),
                )

                self.assertEqual(len(results), 1)
                self.assertEqual(metrics["operator_invocations"], 1)
                self.assertIs(remote.calls[0][0], envelope.payload)


class _RecordingActor:
    def __init__(self):
        self.execute_batch = _RecordingRemote()


class StaticActorSchedulingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.batches = [
            pa.table({"doc_id": [1], "prompt_tokens": [10]}),
            pa.table({"doc_id": [2], "prompt_tokens": [20]}),
            pa.table({"doc_id": [3], "prompt_tokens": [30]}),
        ]

    def _submit(
        self,
        actors,
        adaptive_config=None,
        routing_config=None,
        replay_envelopes=None,
    ):
        actor_pools = {
            f"endpoint-{index}": [actor]
            for index, actor in enumerate(actors)
        }
        return profile.submit_with_backpressure(
            ray_module=_ImmediateRay,
            actor_pools=actor_pools,
            endpoint_urls={
                endpoint_id: f"http://local/{endpoint_id}"
                for endpoint_id in actor_pools
            },
            batches=self.batches,
            max_inflight=2,
            method_name="execute_batch",
            adaptive_config=adaptive_config,
            routing_config=routing_config,
            replay_envelopes=replay_envelopes,
        )

    def test_static_actor_path_delegates_to_shared_scheduler(self) -> None:
        actors = [_RecordingActor()]
        expected = ([{"ok": True}], {"operator_invocations": 1})

        with patch.object(
            profile_ray,
            "_run_static_scheduler",
            return_value=expected,
        ) as run:
            actual = self._submit(actors)

        self.assertEqual(actual, expected)
        run.assert_called_once()

    def test_two_actor_workers_remain_one_service_endpoint(self) -> None:
        actors = [_RecordingActor(), _RecordingActor()]
        actor_pools = {"endpoint-0": actors}
        endpoint_urls = {
            "endpoint-0": "http://localhost:8000/v1/completions",
        }

        results, metrics = profile.submit_with_backpressure(
            ray_module=_ImmediateRay,
            actor_pools=actor_pools,
            endpoint_urls=endpoint_urls,
            batches=self.batches,
            max_inflight=2,
            method_name="execute_batch",
        )

        self.assertEqual(
            [call[0] for call in actors[0].execute_batch.calls],
            [self.batches[0], self.batches[2]],
        )
        self.assertEqual(
            [call[0] for call in actors[1].execute_batch.calls],
            [self.batches[1]],
        )
        self.assertEqual(len(results), 3)
        self.assertEqual(metrics["endpoint_count"], 1)
        self.assertEqual(metrics["actor_worker_count"], 2)
        self.assertEqual(metrics["actor_worker_submission_counts"], "2;1")

    def test_static_actor_path_requires_at_least_one_actor(self) -> None:
        with self.assertRaisesRegex(ValueError, "actor_pools must not be empty"):
            self._submit([])

    def test_actor_pool_and_endpoint_url_keys_must_match(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "identical service endpoint IDs",
        ):
            profile.submit_with_backpressure(
                ray_module=_ImmediateRay,
                actor_pools={"endpoint-0": [_RecordingActor()]},
                endpoint_urls={"endpoint-1": "http://local/endpoint-1"},
                batches=self.batches,
                max_inflight=2,
                method_name="execute_batch",
            )

    def test_actor_path_requires_endpoint_urls(self) -> None:
        with self.assertRaisesRegex(ValueError, "endpoint_urls must not be empty"):
            profile.submit_with_backpressure(
                ray_module=_ImmediateRay,
                actor_pools={"endpoint-0": [_RecordingActor()]},
                endpoint_urls={},
                batches=self.batches,
                max_inflight=2,
                method_name="execute_batch",
            )

    def test_adaptive_actor_path_remains_isolated_from_static_scheduler(self) -> None:
        actors = [_RecordingActor()]

        with patch.object(profile_ray, "_run_static_scheduler") as run:
            self._submit(actors, adaptive_config={})

        run.assert_not_called()

    def test_typed_adaptive_actor_path_uses_dynamic_gate(self) -> None:
        actors = [_RecordingActor()]
        traces = []
        gate = DynamicAdmissionGate(
            AimdAdmissionController(initial_window=8),
            CachedMetricsObservationProvider(
                lambda: ServiceMetricsSnapshot(10, 2, 0.2),
                min_sample_interval_s=0.0,
            ),
            trace_sink=traces.append,
        )

        results, metrics = self._submit(
            actors,
            adaptive_config={
                "admission_gate": gate,
                "trace_events": traces,
            },
        )

        self.assertEqual(len(results), 3)
        self.assertGreater(metrics["adaptive_downshifts"], 0)
        self.assertGreaterEqual(len(traces), 3)
        self.assertEqual(metrics["endpoint_count"], 1)
        self.assertEqual(metrics["actor_worker_count"], 1)
        self.assertEqual(metrics["actor_worker_submission_counts"], "3")

    def test_legacy_actor_path_round_robins_endpoints_and_local_workers(self) -> None:
        actors = [_RecordingActor() for _ in range(4)]
        actor_pools = {
            "endpoint-0": actors[:2],
            "endpoint-1": actors[2:],
        }
        batches = [
            pa.table({"doc_id": [index], "prompt_tokens": [index * 10]})
            for index in range(1, 9)
        ]

        results, metrics = profile.submit_with_backpressure(
            ray_module=_ImmediateRay,
            actor_pools=actor_pools,
            endpoint_urls={
                "endpoint-0": "http://local/endpoint-0",
                "endpoint-1": "http://local/endpoint-1",
            },
            batches=batches,
            max_inflight=2,
            method_name="execute_batch",
            adaptive_config={},
        )

        self.assertEqual(
            [call[0] for call in actors[0].execute_batch.calls],
            [batches[0], batches[4]],
        )
        self.assertEqual(
            [call[0] for call in actors[1].execute_batch.calls],
            [batches[2], batches[6]],
        )
        self.assertEqual(
            [call[0] for call in actors[2].execute_batch.calls],
            [batches[1], batches[5]],
        )
        self.assertEqual(
            [call[0] for call in actors[3].execute_batch.calls],
            [batches[3], batches[7]],
        )
        self.assertEqual(len(results), 8)
        self.assertEqual(metrics["endpoint_count"], 2)
        self.assertEqual(metrics["actor_worker_count"], 4)
        self.assertEqual(
            metrics["actor_worker_submission_counts"],
            "2;2;2;2",
        )

    def test_actor_submission_state_persists_across_fetch_chunks(self) -> None:
        actors = [_RecordingActor() for _ in range(4)]
        actor_pools = {
            "endpoint-0": actors[:2],
            "endpoint-1": actors[2:],
        }
        endpoint_urls = {
            endpoint_id: f"http://local/{endpoint_id}"
            for endpoint_id in actor_pools
        }
        state = profile.ActorSubmissionState(actor_pools, "execute_batch")
        routing_config = {
            "endpoint_router": profile.RoundRobinEndpointRouter(),
        }
        per_chunk_counts = []

        for batch in self.batches[:2] * 2:
            _, metrics = profile.submit_with_backpressure(
                ray_module=_ImmediateRay,
                actor_pools=actor_pools,
                endpoint_urls=endpoint_urls,
                batches=[batch],
                max_inflight=1,
                method_name="execute_batch",
                routing_config=routing_config,
                submission_state=state,
            )
            per_chunk_counts.append(metrics["actor_worker_submission_counts"])

        self.assertEqual(
            [len(actor.execute_batch.calls) for actor in actors],
            [1, 1, 1, 1],
        )
        self.assertEqual(
            per_chunk_counts,
            ["1;0;0;0", "0;0;1;0", "0;1;0;0", "0;0;0;1"],
        )

    def test_legacy_endpoint_rotation_persists_across_calls(self) -> None:
        actors = [_RecordingActor() for _ in range(4)]
        actor_pools = {
            "endpoint-0": actors[:2],
            "endpoint-1": actors[2:],
        }
        state = profile.ActorSubmissionState(actor_pools, "execute_batch")

        for batch in self.batches[:2] * 2:
            profile.submit_with_backpressure(
                ray_module=_ImmediateRay,
                actor_pools=actor_pools,
                endpoint_urls={
                    endpoint_id: f"http://local/{endpoint_id}"
                    for endpoint_id in actor_pools
                },
                batches=[batch],
                max_inflight=1,
                method_name="execute_batch",
                adaptive_config={},
                submission_state=state,
            )

        self.assertEqual(
            [len(actor.execute_batch.calls) for actor in actors],
            [1, 1, 1, 1],
        )

    def test_replay_envelopes_cover_static_typed_and_legacy_actor_paths(self) -> None:
        envelope = profile_replay._batch_envelopes(
            [self.batches[0]],
            job_id="replay",
            operator="ai_embed",
            completion_max_tokens=0,
        )[0]
        for policy in ("static", "typed", "legacy"):
            with self.subTest(policy=policy):
                actors = [_RecordingActor()]
                if policy == "typed":
                    traces = []
                    adaptive_config = {
                        "admission_gate": DynamicAdmissionGate(
                            AimdAdmissionController(initial_window=4),
                            CachedMetricsObservationProvider(
                                lambda: ServiceMetricsSnapshot(10, 0, 0.2),
                                min_sample_interval_s=0.0,
                            ),
                            trace_sink=traces.append,
                        ),
                        "trace_events": traces,
                    }
                elif policy == "legacy":
                    adaptive_config = {}
                else:
                    adaptive_config = None

                results, metrics = self._submit(
                    actors,
                    adaptive_config=adaptive_config,
                    replay_envelopes=iter([envelope]),
                )

                self.assertEqual(len(results), 1)
                self.assertEqual(metrics["operator_invocations"], 1)
                self.assertIs(
                    actors[0].execute_batch.calls[0][0],
                    envelope.payload,
                )

    def test_actor_pool_routes_short_and_long_requests_to_partitioned_actors(self) -> None:
        actors = [_RecordingActor(), _RecordingActor()]

        self._submit(
            actors,
            routing_config={
                "pool_ids": ["short", "long"],
                "gpu_ids": ["0", "0"],
                "endpoint_router": LeastQueuedEndpointRouter(),
                "pool_router": RequestPoolRouter(long_request_tokens=25),
            },
        )

        self.assertEqual(
            [call[0] for call in actors[0].execute_batch.calls],
            [self.batches[0], self.batches[1]],
        )
        self.assertEqual(
            [call[0] for call in actors[1].execute_batch.calls],
            [self.batches[2]],
        )

    def test_slo_ewma_flush_cli_records_controller_parameters(self) -> None:
        args = profile.parse_args(
            [
                "--dry-run",
                "--flush-policy",
                "slo_ewma",
                "--flush-ewma-alpha",
                "0.4",
                "--flush-deadband-ratio",
                "0.2",
                "--flush-service-capacity-tokens-s-per-endpoint",
                "4000",
                "--request-slo-ms",
                "30000",
            ]
        )

        self.assertEqual(args.flush_policy, "slo_ewma")
        self.assertEqual(args.flush_ewma_alpha, 0.4)
        self.assertEqual(args.flush_deadband_ratio, 0.2)
        self.assertEqual(
            args.flush_service_capacity_tokens_s_per_endpoint,
            4000.0,
        )

    def test_slo_ewma_requires_live_replay_feedback(self) -> None:
        self.assertTrue(
            profile._requires_replay_feedback(
                SimpleNamespace(
                    flush_policy="slo_ewma",
                    token_budget_policy="static",
                )
            )
        )
        self.assertFalse(
            profile._requires_replay_feedback(
                SimpleNamespace(
                    flush_policy="fixed_timeout",
                    token_budget_policy="static",
                )
            )
        )


if __name__ == "__main__":
    unittest.main()
