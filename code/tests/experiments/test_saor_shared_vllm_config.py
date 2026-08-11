from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
