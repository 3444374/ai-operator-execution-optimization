from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.run_shared_vllm_experiment import parse_args  # noqa: E402
from src import shared_vllm_experiment as shared_vllm  # noqa: E402
from src.shared_vllm_experiment import (  # noqa: E402
    GroupRunIdentity,
    RunnerOptions,
    SharedVllmConfig,
    SharedVllmScenario,
    _load_resume_manifest,
    _coordinator_name,
    _redact_command,
    _rewrite_group_runs,
    _run_group,
    _run_instance_id,
    _validate_replay_starts,
    _validate_runner_topology,
    _validate_final_credit,
    build_job_command,
    group_resource_summary,
    group_metric_delta,
    jain_fairness,
    load_config,
    normalized_job_service_rates,
)


class SharedVllmExperimentTests(unittest.TestCase):
    def test_credit_observer_exports_code_root_to_ray_workers(self) -> None:
        ray_module = MagicMock()
        ray_module.is_initialized.return_value = False

        with patch.dict(sys.modules, {"ray": ray_module}):
            shared_vllm._RayCreditObserver(
                "127.0.0.1:6380",
                "namespace",
                "credits",
                ("task-0", "task-1"),
            )

        ray_module.init.assert_called_once()
        runtime_env = ray_module.init.call_args.kwargs["runtime_env"]
        pythonpath = runtime_env["env_vars"]["PYTHONPATH"].split(os.pathsep)
        self.assertIn(str(CODE_ROOT), pythonpath)
        self.assertEqual(
            runtime_env["env_vars"]["OPENBLAS_NUM_THREADS"],
            "1",
        )

    def test_credit_observer_prewarms_actor_before_replay(self) -> None:
        observer = shared_vllm._RayCreditObserver.__new__(
            shared_vllm._RayCreditObserver
        )
        observer.ray = MagicMock()
        observer.namespace = "namespace"
        observer.actor_name = "credits"
        observer.endpoint_ids = ("task-0", "task-1")
        observer.actor = None
        client = MagicMock()
        client.actor = object()

        with patch.object(
            shared_vllm,
            "get_or_create_shared_credit_client",
            return_value=client,
            create=True,
        ) as create_client:
            observer.prewarm(
                request_limit=256,
                work_limit=65536,
                quantum=2048,
            )

        create_client.assert_called_once_with(
            observer.ray,
            name="credits",
            namespace="namespace",
            capacities={
                "task-0": (256, 65536),
                "task-1": (256, 65536),
            },
            quantum=2048,
        )
        self.assertEqual(
            [call.args for call in client.snapshot.call_args_list],
            [("task-0",), ("task-1",)],
        )
        self.assertIs(observer.actor, client.actor)

    def test_request_trace_success_matches_profiler_schema(self) -> None:
        self.assertTrue(
            shared_vllm._request_trace_succeeded(
                {"status": "completed", "error_type": ""}
            )
        )
        self.assertFalse(
            shared_vllm._request_trace_succeeded(
                {"status": "ok", "error_type": ""}
            )
        )
        self.assertFalse(
            shared_vllm._request_trace_succeeded(
                {"status": "completed", "error_type": "RuntimeError"}
            )
        )

    def test_cli_resolves_child_process_paths_before_changing_cwd(self) -> None:
        root = Path.cwd()
        options = parse_args(
            [
                "--config",
                "config.json",
                "--profiler",
                "code/scripts/profile.py",
                "--python-executable",
                "bin/python",
                "--output-dir",
                "results/gate",
                "--health-url",
                "http://health",
                "--metrics-urls",
                "http://gpu0/metrics,http://gpu1/metrics",
                "--ray-address",
                "127.0.0.1:6380",
            ]
        )

        self.assertEqual(options.config_path, root / "config.json")
        self.assertEqual(
            options.profiler_path,
            root / "code" / "scripts" / "profile.py",
        )
        self.assertEqual(
            options.python_executable,
            root / "bin" / "python",
        )
        self.assertEqual(options.output_dir, root / "results" / "gate")

    def test_config_expands_environment_and_validates_scenarios(self) -> None:
        payload = self._config_payload(
            common_args=[
                "--database-url",
                "${DATABASE_URL}",
                "--arrival-replay",
            ]
        )
        with (
            patch.object(Path, "read_text", return_value=json.dumps(payload)),
            patch.dict(
                os.environ,
                {"DATABASE_URL": "postgresql://example/db"},
                clear=True,
            ),
        ):
            config = load_config(Path("config.json"))

        self.assertEqual(
            config.common_args,
            ("--database-url", "postgresql://example/db", "--arrival-replay"),
        )
        self.assertEqual(config.scenarios[0].weights, (1, 1))

    def test_config_rejects_setup_and_runner_owned_credit_flags(self) -> None:
        for forbidden in (
            "--setup",
            "--shared-credit-work-limit",
            "--resource-trace-output",
        ):
            with self.subTest(forbidden=forbidden):
                payload = self._config_payload(common_args=[forbidden])
                with patch.object(
                    Path,
                    "read_text",
                    return_value=json.dumps(payload),
                ):
                    with self.assertRaisesRegex(
                        ValueError,
                        "runner-owned flag",
                    ):
                        load_config(Path("config.json"))

    def test_command_audit_redacts_split_and_equals_secrets(self) -> None:
        command = [
            "python",
            "profile.py",
            "--database-url",
            "postgresql://user:password@host/db",
            "--completion-api-key=secret-token",
            "--operator",
            "ai_complete",
        ]

        redacted = _redact_command(command)

        self.assertEqual(redacted[3], "***")
        self.assertEqual(redacted[4], "--completion-api-key=***")
        self.assertNotIn("password", " ".join(redacted))
        self.assertNotIn("secret-token", " ".join(redacted))

    def test_config_requires_complete_service_metadata_when_requested(
        self,
    ) -> None:
        payload = self._config_payload()
        payload["require_complete_service_metadata"] = True
        payload["service_metadata"] = {"vllm_version": "0.25.1"}
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(payload),
        ):
            with self.assertRaisesRegex(
                ValueError,
                "service_metadata missing required keys",
            ):
                load_config(Path("config.json"))

    def test_policy_commands_keep_endpoint_total_capacity_semantics(
        self,
    ) -> None:
        root = CODE_ROOT / "test-output"
        path = root / "config.json"
        payload = self._config_payload(
                scenarios=[
                    {
                        "scenario_id": "independent",
                        "policy": "independent_full",
                        "job_count": 4,
                        "rows_per_job": 64,
                    },
                    {
                        "scenario_id": "partition",
                        "policy": "static_partition",
                        "job_count": 4,
                        "rows_per_job": 64,
                    },
                    {
                        "scenario_id": "fair",
                        "policy": "shared_drr",
                        "job_count": 4,
                        "rows_per_job": 64,
                    },
                ]
        )
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(payload),
        ):
            config = load_config(path)
        options = RunnerOptions(
            config_path=path,
            profiler_path=root / "profile.py",
            python_executable=Path(sys.executable),
            output_dir=root / "out",
            health_url="http://health",
            metrics_urls=("http://gpu0/metrics", "http://gpu1/metrics"),
            ray_address="127.0.0.1:6379",
            idle_timeout_s=1.0,
            start_delay_s=5.0,
        )
        identity = GroupRunIdentity("formal", 1, 0)

        independent = build_job_command(
            options,
            config,
            config.scenarios[0],
            identity,
            job_index=0,
            start_epoch_s=100.0,
            coordinator_name="unused",
        )
        partitioned = build_job_command(
            options,
            config,
            config.scenarios[1],
            identity,
            job_index=0,
            start_epoch_s=100.0,
            coordinator_name="unused",
        )
        shared = build_job_command(
            options,
            config,
            config.scenarios[2],
            identity,
            job_index=0,
            start_epoch_s=100.0,
            coordinator_name="credits",
        )

        self.assertEqual(self._flag_value(independent, "--max-inflight"), "256")
        self.assertEqual(
            self._flag_value(
                independent,
                "--max-active-work-per-endpoint",
            ),
            "65536",
        )
        self.assertEqual(self._flag_value(partitioned, "--max-inflight"), "64")
        self.assertEqual(
            self._flag_value(
                partitioned,
                "--max-active-work-per-endpoint",
            ),
            "16384",
        )
        self.assertEqual(self._flag_value(shared, "--max-inflight"), "256")
        self.assertEqual(
            self._flag_value(shared, "--shared-credit-work-limit"),
            "65536",
        )
        self.assertEqual(
            self._flag_value(shared, "--shared-credit-coordinator-name"),
            "credits",
        )
        self.assertEqual(
            self._flag_value(shared, "--arrival-replay-start-epoch-s"),
            "100.0",
        )

    def test_group_metrics_use_one_service_delta_not_per_job_summaries(
        self,
    ) -> None:
        before = [
            {
                "vllm:prompt_tokens_total": 100.0,
                "vllm:generation_tokens_total": 50.0,
            },
            {
                "vllm:prompt_tokens_total": 200.0,
                "vllm:generation_tokens_total": 75.0,
            },
        ]
        after = [
            {
                "vllm:prompt_tokens_total": 300.0,
                "vllm:generation_tokens_total": 150.0,
            },
            {
                "vllm:prompt_tokens_total": 500.0,
                "vllm:generation_tokens_total": 175.0,
            },
        ]

        metrics = group_metric_delta(before, after, duration_s=10.0)

        self.assertEqual(metrics["prompt_tokens_delta"], 500)
        self.assertEqual(metrics["generation_tokens_delta"], 200)
        self.assertEqual(metrics["tokens_per_s"], 70.0)

    def test_group_resources_deduplicate_host_gpu_sample_per_epoch(
        self,
    ) -> None:
        samples = [
            {
                "observed_epoch_s": 0.0,
                "endpoint_index": 0,
                "running": 0,
                "waiting": 0,
                "kv_usage": 0.0,
                "gpu_utilization_pct": "0",
            },
            {
                "observed_epoch_s": 0.0,
                "endpoint_index": 1,
                "running": 0,
                "waiting": 0,
                "kv_usage": 0.0,
                "gpu_utilization_pct": "0",
            },
        ] + [
            {
                "observed_epoch_s": epoch,
                "endpoint_index": endpoint,
                "running": running,
                "waiting": waiting,
                "kv_usage": kv,
                "gpu_utilization_pct": gpu,
            }
            for epoch, gpu, values in (
                (1.0, "50", ((2, 1, 0.2), (3, 0, 0.3))),
                (2.0, "100", ((4, 0, 0.4), (5, 2, 0.5))),
            )
            for endpoint, (running, waiting, kv) in enumerate(values)
        ]

        summary = group_resource_summary(
            samples,
            start_epoch_s=1.0,
            end_epoch_s=2.0,
        )

        self.assertEqual(summary["gpu_utilization_pct_mean"], 75.0)
        self.assertEqual(summary["gpu_utilization_pct_p95"], 100.0)
        self.assertEqual(summary["vllm_running_mean"], 7.0)
        self.assertEqual(summary["vllm_running_max"], 9.0)
        self.assertEqual(summary["vllm_waiting_max"], 2.0)

    def test_job_evidence_reports_nearest_rank_p99(self) -> None:
        options = RunnerOptions(
            config_path=Path("config.json"),
            profiler_path=Path("profile.py"),
            python_executable=Path(sys.executable),
            output_dir=Path("out"),
            health_url="http://health",
            metrics_urls=("http://metrics0", "http://metrics1"),
            ray_address="127.0.0.1:6380",
            idle_timeout_s=1.0,
        )
        scenario = SharedVllmScenario(
            scenario_id="latency",
            policy="independent_full",
            job_count=1,
            rows_per_job=4,
            weights=(1,),
            arrival_offsets_s=(0.0,),
        )
        request_rows = [
            {
                "request_id": f"request-{index}",
                "status": "completed",
                "error_type": "",
                "arrival_epoch_s": str(index),
                "completion_epoch_s": str(index + latency),
                "e2e_s": str(latency),
                "slo_met": "True",
                "prompt_tokens": "10",
                "client_estimated_output_tokens": "20",
                "estimated_output_tokens": "20",
                "endpoint_id": f"task-{index % 2}",
                "submit_epoch_s": str(index + 0.1),
            }
            for index, latency in enumerate((1.0, 2.0, 3.0, 100.0))
        ]
        summary_rows = [
            {
                "status": "ok",
                "total_rows": "4",
                "actor_worker_failures": "0",
                "arrival_replay_start_epoch_s": "100.0",
                "arrival_replay_observed_start_epoch_s": "100.0",
            }
        ]

        with patch.object(
            shared_vllm,
            "_read_csv",
            side_effect=[summary_rows, request_rows, [{}] * 4],
        ):
            evidence = shared_vllm._validate_job_evidence(
                options,
                scenario,
                GroupRunIdentity("formal", 1, 0),
                0,
            )

        self.assertEqual(evidence["p99_s"], 100.0)

    def test_jain_fairness_handles_equal_weight_and_zero_service(self) -> None:
        self.assertEqual(jain_fairness([100.0, 100.0]), 1.0)
        self.assertAlmostEqual(jain_fairness([100.0, 0.0]), 0.5)
        self.assertEqual(jain_fairness([0.0, 0.0]), 0.0)

    def test_normalized_service_uses_achieved_rate_not_offered_work(
        self,
    ) -> None:
        evidence = [
            {"predicted_work": 1000, "jct_s": 10.0},
            {"predicted_work": 1000, "jct_s": 20.0},
        ]

        rates = normalized_job_service_rates(evidence, (1, 1))

        self.assertEqual(rates, [100.0, 50.0])
        self.assertAlmostEqual(jain_fairness(rates), 0.9)

    def test_replay_start_validation_rejects_late_or_skewed_jobs(self) -> None:
        evidence = [
            {
                "replay_configured_start_epoch_s": 100.0,
                "replay_observed_start_epoch_s": 100.1,
                "replay_actual_submit_start_epoch_s": 100.1,
            },
            {
                "replay_configured_start_epoch_s": 100.0,
                "replay_observed_start_epoch_s": 100.8,
                "replay_actual_submit_start_epoch_s": 100.8,
            },
        ]

        with self.assertRaisesRegex(RuntimeError, "start skew"):
            _validate_replay_starts(
                evidence,
                expected_start_epoch_s=100.0,
                arrival_offsets_s=(0.0, 0.0),
                max_lateness_s=2.0,
                max_skew_s=0.5,
            )
        with self.assertRaisesRegex(RuntimeError, "start deadline"):
            _validate_replay_starts(
                evidence,
                expected_start_epoch_s=100.0,
                arrival_offsets_s=(0.0, 0.0),
                max_lateness_s=0.5,
                max_skew_s=1.0,
            )

    def test_runner_topology_rejects_duplicate_metrics_urls(self) -> None:
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(self._config_payload()),
        ):
            config = load_config(Path("config.json"))
        duplicate = RunnerOptions(
            config_path=Path("config.json"),
            profiler_path=Path("profile.py"),
            python_executable=Path(sys.executable),
            output_dir=Path("output"),
            health_url="http://health",
            metrics_urls=("http://metrics0", "http://metrics0"),
            ray_address="127.0.0.1:6379",
            idle_timeout_s=1.0,
        )

        with self.assertRaisesRegex(
            ValueError,
            "metrics URLs must be unique",
        ):
            _validate_runner_topology(duplicate, config)

    def test_coordinator_name_isolated_by_physical_output_run(self) -> None:
        first_id = _run_instance_id(Path("results/gate-a"))
        second_id = _run_instance_id(Path("results/gate-b"))

        self.assertNotEqual(first_id, second_id)
        self.assertNotEqual(
            _coordinator_name("experiment", first_id, "000_formal"),
            _coordinator_name("experiment", second_id, "000_formal"),
        )

    def test_resume_rejects_repository_commit_mismatch(self) -> None:
        manifest = {
            "schema_version": 1,
            "experiment_id": "experiment",
            "config_fingerprint": "fingerprint",
            "repository_commit": "old",
            "redacted_config": {},
            "schedule": [],
            "completed_runs": [],
            "incidents": [],
        }
        expected = {**manifest, "repository_commit": "new"}
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(manifest),
        ), patch.object(Path, "exists", return_value=True):
            with self.assertRaisesRegex(
                ValueError,
                "repository_commit",
            ):
                _load_resume_manifest(Path("manifest.json"), expected)

    def test_group_summary_is_rebuilt_from_durable_records(self) -> None:
        with TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            records_dir = output_dir / "records"
            records_dir.mkdir()
            first = {"scenario_id": "a", "tokens_per_s": 10.0}
            second = {"scenario_id": "b", "tokens_per_s": 20.0}
            (records_dir / "a.json").write_text(
                json.dumps(first),
                encoding="utf-8",
            )
            (records_dir / "b.json").write_text(
                json.dumps(second),
                encoding="utf-8",
            )
            completed = [
                {"order_index": 1, "record_path": "records/b.json"},
                {"order_index": 0, "record_path": "records/a.json"},
            ]
            summary_path = output_dir / "group_runs.csv"

            _rewrite_group_runs(summary_path, output_dir, completed)
            _rewrite_group_runs(summary_path, output_dir, completed)

            lines = summary_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 3)
            self.assertIn("a,10.0", lines[1])
            self.assertIn("b,20.0", lines[2])

    def test_post_child_validation_failure_persists_failure_evidence(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            options, config, scenario = self._group_fixture(Path(temp_dir))
            process = MagicMock()
            process.poll.return_value = 0
            process.wait.return_value = 0
            with (
                patch(
                    "src.shared_vllm_experiment.subprocess.Popen",
                    return_value=process,
                ) as popen,
                patch(
                    "src.shared_vllm_experiment.scrape_prometheus_metrics",
                    return_value={
                        "vllm:prompt_tokens_total": 1.0,
                        "vllm:generation_tokens_total": 1.0,
                        "vllm:request_success_total": 1.0,
                        "vllm:estimated_flops_per_gpu_total": 1.0,
                    },
                ),
                patch(
                    "src.shared_vllm_experiment._validate_job_evidence",
                    side_effect=RuntimeError("exactly-once failed"),
                ),
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "exactly-once failed",
                ):
                    _run_group(
                        options,
                        config,
                        scenario,
                        GroupRunIdentity("formal", 1, 0),
                    )

            child_env = popen.call_args.kwargs["env"]
            self.assertEqual(child_env["OMP_NUM_THREADS"], "1")
            self.assertEqual(child_env["OPENBLAS_NUM_THREADS"], "1")
            failure = (
                options.output_dir
                / "traces"
                / "000_formal_1_test.failure.json"
            )
            self.assertTrue(failure.exists())
            self.assertIn(
                "exactly-once failed",
                failure.read_text(encoding="utf-8"),
            )

    def test_worker_failure_is_a_hard_group_gate(self) -> None:
        with TemporaryDirectory() as temp_dir:
            options, config, scenario = self._group_fixture(Path(temp_dir))
            process = MagicMock()
            process.poll.return_value = 0
            process.wait.return_value = 0
            evidence = {
                "jct_s": 1.0,
                "p99_s": 1.0,
                "completion_lag_s": 1.0,
                "slo_violation_ratio": 0.0,
                "slo_goodput_per_s": 64.0,
                "predicted_work": 100,
                "endpoint_counts": {"task-0": 32, "task-1": 32},
                "actor_worker_failures": 1,
                "replay_configured_start_epoch_s": 100.0,
                "replay_observed_start_epoch_s": 100.0,
                "replay_actual_submit_start_epoch_s": 100.0,
            }
            with (
                patch(
                    "src.shared_vllm_experiment.time.time",
                    side_effect=[95.0, 95.0, 101.0],
                ),
                patch(
                    "src.shared_vllm_experiment.subprocess.Popen",
                    return_value=process,
                ),
                patch(
                    "src.shared_vllm_experiment.scrape_prometheus_metrics",
                    return_value={
                        "vllm:prompt_tokens_total": 1.0,
                        "vllm:generation_tokens_total": 1.0,
                        "vllm:request_success_total": 1.0,
                        "vllm:estimated_flops_per_gpu_total": 1.0,
                    },
                ),
                patch(
                    "src.shared_vllm_experiment._validate_job_evidence",
                    return_value=evidence,
                ),
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "actor worker failures observed",
                ):
                    _run_group(
                        options,
                        config,
                        scenario,
                        GroupRunIdentity("formal", 1, 0),
                    )

    def test_final_credit_rejects_global_peak_above_endpoint_limit(
        self,
    ) -> None:
        payload = self._config_payload()
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(payload),
        ):
            config = load_config(Path("config.json"))
        snapshots = [
            {
                "active_requests": 0,
                "active_work": 0,
                "waiting_requests": 0,
                "waiting_work": 0,
                "max_active_requests_seen": 257,
                "max_active_work_seen": 65536,
            },
            {
                "active_requests": 0,
                "active_work": 0,
                "waiting_requests": 0,
                "waiting_work": 0,
                "max_active_requests_seen": 256,
                "max_active_work_seen": 65536,
            },
        ]

        with self.assertRaisesRegex(
            RuntimeError,
            "shared request limit was exceeded",
        ):
            _validate_final_credit(config, snapshots)

    @staticmethod
    def _flag_value(command: list[str], flag: str) -> str:
        return command[command.index(flag) + 1]

    @staticmethod
    def _config_payload(
        *,
        common_args: list[str] | None = None,
        scenarios: list[dict] | None = None,
    ) -> dict:
        return {
            "schema_version": 1,
            "experiment_id": "shared-test",
            "seed": 17,
            "warmup_runs_per_scenario": 0,
            "formal_repeats": 1,
            "endpoint_ids": ["task-0", "task-1"],
            "request_limit_per_endpoint": 256,
            "work_limit_per_endpoint": 65536,
            "credit_quantum": 2048,
            "common_args": (
                common_args
                if common_args is not None
                else ["--arrival-replay"]
            ),
            "scenarios": scenarios
            or [
                {
                    "scenario_id": "fair_j2",
                    "policy": "shared_drr",
                    "job_count": 2,
                    "rows_per_job": 64,
                }
            ],
        }

    @staticmethod
    def _group_fixture(
        output_dir: Path,
    ) -> tuple[RunnerOptions, SharedVllmConfig, SharedVllmScenario]:
        for child in ("jobs", "logs", "traces", "records"):
            (output_dir / child).mkdir(parents=True, exist_ok=True)
        options = RunnerOptions(
            config_path=output_dir / "config.json",
            profiler_path=output_dir / "profile.py",
            python_executable=Path(sys.executable),
            output_dir=output_dir,
            health_url="http://health",
            metrics_urls=("http://metrics0", "http://metrics1"),
            ray_address="127.0.0.1:6379",
            idle_timeout_s=1.0,
            start_delay_s=5.0,
        )
        scenario = SharedVllmScenario(
            scenario_id="test",
            policy="independent_full",
            job_count=1,
            rows_per_job=64,
            weights=(1,),
            arrival_offsets_s=(0.0,),
        )
        config = SharedVllmConfig(
            experiment_id="experiment",
            seed=1,
            warmup_runs_per_scenario=0,
            formal_repeats=1,
            endpoint_ids=("task-0", "task-1"),
            request_limit_per_endpoint=256,
            work_limit_per_endpoint=65536,
            credit_quantum=2048,
            shared_credit_namespace="namespace",
            gpu_peak_tflops=165.0,
            mfu_precision="bf16",
            common_args=("--arrival-replay",),
            scenarios=(scenario,),
            service_metadata=(),
        )
        return options, config, scenario


if __name__ == "__main__":
    unittest.main()
