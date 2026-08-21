from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from src.experiments.shared_vllm.config import (
    GroupRunIdentity,
    RunnerOptions,
    build_job_command,
    load_config,
)
from src.experiments.shared_vllm.evidence import (
    _redacted_config,
    _validate_final_credit,
)
from src.experiments.shared_vllm.runner import _apply_saor_capacity_control
from src.scheduling.core.control import CapacityArm
from src.scheduling.runtime.saor_capacity import (
    LinearCostFeature,
    SaorArmEstimate,
    SaorCapacityController,
    SaorObservationModel,
)


class SaorSharedVllmConfigTest(unittest.TestCase):
    def test_bounded_ready_uses_distinct_profiler_policy(self) -> None:
        payload = self._bounded_priority_payload()
        payload["scenarios"][0]["policy"] = "saor_bounded_ready"
        payload["ready_observation_contract"] = (
            "bounded_concrete_pre_registration"
        )
        payload["ready_payload_bytes_limit_per_job"] = 1048576
        with patch.object(Path, "read_text", return_value=json.dumps(payload)):
            config = load_config(Path("bounded-ready.json"))
        scenario = config.scenarios[0]
        options = RunnerOptions(
            config_path=Path("bounded-ready.json"),
            profiler_path=Path("profile.py"),
            python_executable=Path("python"),
            output_dir=Path("out"),
            health_url="http://127.0.0.1/health",
            metrics_urls=("http://127.0.0.1/metrics",),
            ray_address="local",
            idle_timeout_s=1.0,
        )

        command = build_job_command(
            options,
            config,
            scenario,
            GroupRunIdentity("rehearsal", 0, 0),
            job_index=1,
            start_epoch_s=1.0,
            coordinator_name="bounded-ready",
        )

        self.assertEqual(
            self._flag(command, "--shared-credit-policy"),
            "saor_bounded_ready",
        )
        self.assertEqual(
            self._flag(command, "--shared-ready-observation-contract"),
            "bounded_concrete_pre_registration",
        )
        self.assertTrue(
            self._flag(command, "--completion-evidence-output").endswith(
                ".completions.csv"
            )
        )

    def test_matched_ready_observation_is_independent_of_selector(self) -> None:
        payload = self._bounded_priority_payload()
        payload["ready_observation_contract"] = (
            "bounded_concrete_pre_registration"
        )
        payload["ready_payload_bytes_limit_per_job"] = 1048576
        payload["scenarios"] = [
            {
                "scenario_id": "fifo",
                "policy": "shared_fifo",
                "ready_observation_contract": (
                    "bounded_concrete_pre_registration"
                ),
                "job_count": 2,
                "rows_per_job": 1,
                "arrival_offsets_s": [0.0, 5.0],
            }
        ]
        with patch.object(Path, "read_text", return_value=json.dumps(payload)):
            config = load_config(Path("matched-ready.json"))
        command = build_job_command(
            RunnerOptions(
                config_path=Path("matched-ready.json"),
                profiler_path=Path("profile.py"),
                python_executable=Path("python"),
                output_dir=Path("out"),
                health_url="http://127.0.0.1/health",
                metrics_urls=("http://127.0.0.1/metrics",),
                ray_address="local",
                idle_timeout_s=1.0,
            ),
            config,
            config.scenarios[0],
            GroupRunIdentity("rehearsal", 0, 0),
            job_index=0,
            start_epoch_s=1.0,
            coordinator_name="matched-ready",
        )

        self.assertEqual(self._flag(command, "--shared-credit-policy"), "fifo")
        self.assertEqual(
            self._flag(command, "--shared-ready-observation-contract"),
            "bounded_concrete_pre_registration",
        )
        self.assertEqual(
            self._flag(command, "--shared-ready-payload-bytes-limit"),
            "1048576",
        )

    def test_bounded_priority_uses_explicit_per_job_contract(self) -> None:
        payload = self._bounded_priority_payload()
        with patch.object(Path, "read_text", return_value=json.dumps(payload)):
            config = load_config(Path("bounded.json"))
        scenario = config.scenarios[0]
        options = RunnerOptions(
            config_path=Path("bounded.json"),
            profiler_path=Path("profile.py"),
            python_executable=Path("python"),
            output_dir=Path("out"),
            health_url="http://127.0.0.1/health",
            metrics_urls=("http://127.0.0.1/metrics",),
            ray_address="local",
            idle_timeout_s=1.0,
        )

        bulk = build_job_command(
            options,
            config,
            scenario,
            GroupRunIdentity("formal", 1, 0),
            job_index=0,
            start_epoch_s=1.0,
            coordinator_name="bounded",
        )
        foreground = build_job_command(
            options,
            config,
            scenario,
            GroupRunIdentity("formal", 1, 0),
            job_index=1,
            start_epoch_s=1.0,
            coordinator_name="bounded",
        )

        self.assertEqual(self._flag(bulk, "--shared-credit-policy"), "saor_bounded_priority")
        self.assertEqual(self._flag(bulk, "--shared-credit-job-priority"), "0")
        self.assertEqual(self._flag(bulk, "--shared-credit-job-debt-cap-work"), "8192")
        self.assertNotIn("--shared-credit-priority-window-ms", bulk)
        self.assertEqual(self._flag(foreground, "--shared-credit-job-priority"), "1")
        self.assertEqual(self._flag(foreground, "--shared-credit-job-slo-ms"), "30000")
        self.assertEqual(
            self._flag(foreground, "--shared-credit-priority-window-ms"),
            "30000",
        )
        self.assertNotIn("--shared-credit-job-debt-cap-work", foreground)

    def test_bounded_priority_rejects_implicit_or_invalid_job_roles(self) -> None:
        cases = []
        missing = self._bounded_priority_payload()
        del missing["scenarios"][0]["priorities"]
        cases.append((missing, "one value per job"))
        wrong_length = self._bounded_priority_payload()
        wrong_length["scenarios"][0]["slo_targets_s"] = [None]
        cases.append((wrong_length, "one value per job"))
        bad_cap = self._bounded_priority_payload()
        bad_cap["scenarios"][0]["debt_cap_fractions"] = [1.1, None]
        cases.append((bad_cap, "debt_cap_fractions"))
        missing_window = self._bounded_priority_payload()
        missing_window["scenarios"][0]["priority_windows_s"] = [None, None]
        cases.append((missing_window, "SLO target and priority window"))
        missing_control = self._bounded_priority_payload()
        del missing_control["saor_release_control"]
        cases.append((missing_control, "saor_release_control"))

        for payload, message in cases:
            with self.subTest(message=message), patch.object(
                Path, "read_text", return_value=json.dumps(payload)
            ):
                with self.assertRaisesRegex(ValueError, message):
                    load_config(Path("bounded.json"))

    def test_active_set_template_does_not_hardcode_k(self) -> None:
        repository = Path(__file__).resolve().parents[3]
        payload = json.loads(
            (
                repository
                / "deploy/autodl/saor_active_set_release.example.json"
            ).read_text(encoding="utf-8")
        )

        self.assertEqual(
            payload["request_limit_per_endpoint"],
            "${PROJECT_STATIC_K_PER_ENDPOINT}",
        )
        self.assertEqual(
            payload["work_limit_per_endpoint"],
            "${PROJECT_ACTIVE_WORK_PER_ENDPOINT}",
        )
        self.assertIs(payload["fail_closed_rehearsal"], True)
        arrival_scale_index = payload["common_args"].index("--arrival-time-scale")
        self.assertEqual(
            payload["common_args"][arrival_scale_index + 1],
            "${SAOR_ARRIVAL_TIME_SCALE}",
        )
        self.assertEqual(
            payload["readiness_contract"]["max_effective_manifest_span_s"],
            "${SAOR_MAX_EFFECTIVE_MANIFEST_SPAN_S}",
        )
        self.assertEqual(
            payload["readiness_contract"][
                "min_pre_foreground_work_envelopes_per_endpoint"
            ],
            "${SAOR_MIN_PRE_FOREGROUND_WORK_ENVELOPES}",
        )
        self.assertEqual(
            [scenario["policy"] for scenario in payload["scenarios"]],
            [
                "direct_no_job",
                "static_partition",
                "shared_fifo",
                "shared_drr",
                "external_vtc",
                "saor_release",
                "independent_full",
                "independent_full",
                "direct_no_job",
                "direct_no_job",
            ],
        )

    def test_rejects_unwired_saor_slo_weight(self) -> None:
        payload = {
            "schema_version": 1,
            "experiment_id": "saor-release-slo",
            "seed": 1,
            "warmup_runs_per_scenario": 0,
            "formal_repeats": 1,
            "endpoint_ids": ["endpoint-0"],
            "request_limit_per_endpoint": 4,
            "work_limit_per_endpoint": 40,
            "credit_quantum": 1,
            "common_args": ["--arrival-replay"],
            "saor_release_control": {
                "entitlement_weight": 1.0,
                "queue_weight": 0.0,
                "fairness_weight": 1.0,
                "slo_weight": 1.0,
            },
            "scenarios": [
                {
                    "scenario_id": "saor-release",
                    "policy": "saor_release",
                    "job_count": 2,
                    "rows_per_job": 1,
                }
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "not executable yet"):
                load_config(path)

    def test_loads_fixed_envelope_release_and_emits_only_release_controls(self) -> None:
        payload = {
            "schema_version": 1,
            "experiment_id": "saor-release-gate",
            "seed": 1,
            "warmup_runs_per_scenario": 0,
            "formal_repeats": 1,
            "endpoint_ids": ["endpoint-0"],
            "request_limit_per_endpoint": 4,
            "work_limit_per_endpoint": 40,
            "credit_quantum": 1,
            "common_args": ["--arrival-replay"],
            "saor_release_control": {
                "entitlement_weight": 1.0,
                "queue_weight": 0.0,
                "fairness_weight": 1.0,
                "slo_weight": 0.0,
            },
            "scenarios": [
                {
                    "scenario_id": "saor-release",
                    "policy": "saor_release",
                    "job_count": 2,
                    "rows_per_job": 1,
                }
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "config.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            config = load_config(path)
            command = build_job_command(
                RunnerOptions(
                    config_path=path,
                    profiler_path=root / "profiler.py",
                    python_executable=root / "python",
                    output_dir=root / "out",
                    health_url="http://127.0.0.1/health",
                    metrics_urls=("http://127.0.0.1/metrics",),
                    ray_address="local",
                    idle_timeout_s=1.0,
                ),
                config,
                config.scenarios[0],
                GroupRunIdentity("formal", 0, 0),
                job_index=0,
                start_epoch_s=1.0,
                coordinator_name="coordinator",
            )

        self.assertEqual(
            command[command.index("--max-inflight") + 1],
            "4",
        )
        self.assertEqual(
            command[command.index("--max-active-work-per-endpoint") + 1],
            "40",
        )
        self.assertEqual(
            command[command.index("--shared-credit-policy") + 1],
            "saor",
        )
        self.assertNotIn("--controller-min-window", command)
        self.assertEqual(
            _redacted_config(config)["saor_release_control"][
                "entitlement_weight"
            ],
            1.0,
        )

    def test_loads_capacity_only_policy_and_keeps_job_ceiling_at_max_arm(self) -> None:
        payload = {
            "schema_version": 1,
            "experiment_id": "saor-gate",
            "seed": 1,
            "warmup_runs_per_scenario": 0,
            "formal_repeats": 1,
            "endpoint_ids": ["endpoint-0"],
            "request_limit_per_endpoint": 2,
            "work_limit_per_endpoint": 20,
            "credit_quantum": 1,
            "common_args": ["--arrival-replay"],
            "saor_capacity_control": {
                "arms": [
                    {
                        "name": "lower",
                        "request_limit": 2,
                        "work_limit": 20,
                        "prior_goodput": 0.8,
                        "prior_tail_risk": 0.1,
                        "prior_energy": 0.0,
                    },
                    {
                        "name": "upper",
                        "request_limit": 4,
                        "work_limit": 40,
                        "prior_goodput": 1.0,
                        "prior_tail_risk": 0.2,
                        "prior_energy": 0.0,
                    },
                ],
                "initial_arm": "lower",
                "fallback_arm": "lower",
                "observation_model": {
                    "goodput_field": "service_rate_tokens_s",
                    "goodput_scale": 10.0,
                    "tail_features": [
                        {
                            "field": "organizer_oldest_queue_age_s",
                            "scale": 5.0,
                            "weight": 1.0,
                        }
                    ],
                    "energy_features": [],
                },
                "ewma_alpha": 0.5,
                "queue_work_scale": 10,
                "min_dwell_samples": 2,
                "max_state_age_s": 1.0,
                "v": 1.0,
                "tail_weight": 1.0,
                "energy_weight": 0.0,
                "switch_weight": 0.1,
            },
            "scenarios": [
                {
                    "scenario_id": "saor",
                    "policy": "saor_capacity",
                    "job_count": 2,
                    "rows_per_job": 1,
                }
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "config.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            config = load_config(path)
            command = build_job_command(
                RunnerOptions(
                    config_path=path,
                    profiler_path=root / "profiler.py",
                    python_executable=root / "python",
                    output_dir=root / "out",
                    health_url="http://127.0.0.1/health",
                    metrics_urls=("http://127.0.0.1/metrics",),
                    ray_address="local",
                    idle_timeout_s=1.0,
                ),
                config,
                config.scenarios[0],
                GroupRunIdentity("formal", 0, 0),
                job_index=0,
                start_epoch_s=1.0,
                coordinator_name="coordinator",
            )

        self.assertEqual(config.saor_capacity_control.initial_arm, "lower")
        redacted = _redacted_config(config)
        self.assertEqual(
            redacted["saor_capacity_control"]["initial_arm"],
            "lower",
        )
        self.assertEqual(
            redacted["saor_capacity_control"]["arms"][1]["name"],
            "upper",
        )
        self.assertEqual(command[command.index("--max-inflight") + 1], "4")
        self.assertEqual(
            command[command.index("--max-active-work-per-endpoint") + 1],
            "40",
        )
        self.assertEqual(
            command[command.index("--shared-credit-policy") + 1],
            "drr",
        )
        _validate_final_credit(
            config,
            config.scenarios[0],
            [
                {
                    "active_requests": 0,
                    "active_work": 0,
                    "waiting_requests": 0,
                    "waiting_work": 0,
                    "max_active_requests_seen": 4,
                    "max_active_work_seen": 40,
                }
            ],
        )

    def test_thin_runner_adapter_actuates_named_ray_credit_capacity(self) -> None:
        class Observer:
            def __init__(self) -> None:
                self.calls = []

            def update_capacity(self, endpoint_id, **kwargs):
                self.calls.append((endpoint_id, kwargs))
                return {
                    "request_limit": kwargs["request_limit"],
                    "work_limit": kwargs["work_limit"],
                }

        controller = SaorCapacityController(
            arms=(
                SaorArmEstimate("lower", CapacityArm(2, 20), 0.5, 0.1, 0.0),
                SaorArmEstimate("upper", CapacityArm(4, 40), 1.0, 0.1, 0.0),
            ),
            initial_arm="lower",
            fallback_arm="lower",
            ewma_alpha=1.0,
            queue_work_scale=10,
            min_dwell_samples=0,
            v=1.0,
            tail_weight=1.0,
            energy_weight=0.0,
            switch_weight=0.0,
        )
        observer = Observer()
        observed_at_s = time.time()
        rows = _apply_saor_capacity_control(
            [
                {
                    "endpoint_id": "endpoint-0",
                    "observed_epoch_s": observed_at_s,
                    "request_limit": 2,
                    "active_requests": 2,
                    "organizer_queued_work": 10,
                    "organizer_oldest_queue_age_s": 1.0,
                    "model_active_work": 10,
                    "model_capacity_work": 20,
                    "service_rate_tokens_s": 5.0,
                    "vllm_waiting": 0,
                }
            ],
            controllers={"endpoint-0": controller},
            observation_model=SaorObservationModel(
                goodput_field="service_rate_tokens_s",
                goodput_scale=10.0,
                tail_features=(
                    LinearCostFeature("organizer_oldest_queue_age_s", 10.0, 1.0),
                ),
            ),
            observer=observer,
            calibration_signature="sig",
            max_state_age_s=1.0,
        )

        self.assertEqual(len(observer.calls), 1)
        self.assertEqual(rows[0]["control_arm_name"], "upper")
        self.assertEqual(rows[0]["control_applied_work_limit"], 40)

    @staticmethod
    def _flag(command: list[str], flag: str) -> str:
        return command[command.index(flag) + 1]

    @staticmethod
    def _bounded_priority_payload() -> dict[str, object]:
        return {
            "schema_version": 1,
            "experiment_id": "bounded-priority",
            "seed": 1,
            "warmup_runs_per_scenario": 0,
            "formal_repeats": 1,
            "endpoint_ids": ["endpoint-0"],
            "request_limit_per_endpoint": 128,
            "work_limit_per_endpoint": 65536,
            "credit_quantum": 2048,
            "common_args": ["--arrival-replay"],
            "saor_release_control": {
                "entitlement_weight": 1.0,
                "queue_weight": 0.0,
                "fairness_weight": 1.0,
                "slo_weight": 0.0,
            },
            "scenarios": [
                {
                    "scenario_id": "bounded",
                    "policy": "saor_bounded_priority",
                    "job_count": 2,
                    "rows_per_job": 1,
                    "arrival_offsets_s": [0.0, 5.0],
                    "priorities": [0, 1],
                    "slo_targets_s": [None, 30.0],
                    "priority_windows_s": [None, 30.0],
                    "debt_cap_fractions": [0.125, None],
                }
            ],
        }


if __name__ == "__main__":
    unittest.main()
