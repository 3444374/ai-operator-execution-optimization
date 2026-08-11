"""Load every phase-change template with one internally consistent contract."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

CODE_ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_ROOT = CODE_ROOT.parent
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.shared_vllm import (  # noqa: E402
    GroupRunIdentity,
    RunnerOptions,
    build_job_command,
    load_config,
)


class TestPhaseChangeConfigs(unittest.TestCase):
    def test_all_templates_load_and_preserve_arm_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            calibration_path = Path(temporary) / "calibration.json"
            calibration_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "ready",
                        "selection": {
                            "kind": "phase_change_capacity_calibration",
                            "status": "passed",
                            "lower_request_limit": 128,
                            "lower_work_limit": 131072,
                            "upper_request_limit": 160,
                            "upper_work_limit": 163840,
                            "target_service_rate_tokens_s_per_endpoint": 7600.0,
                            "output_cap": 512,
                            "arrival_time_scale": 1.0,
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
            environment = {
                "VLLM_VERSION": "0.10.0",
                "VLLM_MAX_NUM_BATCHED_TOKENS": "6144",
                "VLLM_MAX_NUM_SEQS": "256",
                "DATABASE_URL": "postgresql://postgres:postgres@localhost:5432/postgres",
                "COMPLETION_MODEL": "qwen-test",
                "MODEL_PATH": "/tmp/model",
                "PHASE_CHANGE_WORKLOAD": "phase_change_test",
                "PHASE_CHANGE_CLIENT0_ROWS": "600",
                "PHASE_CHANGE_CLIENT1_ROWS": "300",
                "PHASE_CHANGE_CLIENT0_OFFSET_S": "0.125",
                "PHASE_CHANGE_CLIENT1_OFFSET_S": "60.625",
                "PHASE_CHANGE_CLIENT0_MANIFEST": "/tmp/client_0.jsonl",
                "PHASE_CHANGE_CLIENT1_MANIFEST": "/tmp/client_1.jsonl",
                "PHASE_CHANGE_OUTPUT_CAP": "512",
                "PHASE_CHANGE_LOWER_K": "128",
                "PHASE_CHANGE_LOWER_W": "131072",
                "PHASE_CHANGE_UPPER_K": "160",
                "PHASE_CHANGE_UPPER_W": "163840",
                "PHASE_CHANGE_TARGET_SERVICE_RATE": "7600.0",
                "PHASE_CHANGE_CALIBRATION_CONTRACT": str(calibration_path),
            }
            names = (
                "phase_change_a_only_calibration.example.json",
                "phase_change_pressure_calibration.example.json",
                "phase_change_action_gate.example.json",
                "phase_change_formal.example.json",
            )
            with patch.dict(os.environ, environment, clear=True):
                configs = {
                    name: load_config(REPOSITORY_ROOT / "deploy" / "autodl" / name)
                    for name in names
                }

        formal = configs["phase_change_formal.example.json"]
        self.assertEqual(formal.warmup_runs_per_scenario, 1)
        self.assertEqual(formal.formal_repeats, 3)
        self.assertEqual(
            [scenario.policy for scenario in formal.scenarios],
            ["shared_drr", "shared_drr", "state_aware_adaptive"],
        )
        self.assertEqual(formal.state_aware_control.request_candidates, (128, 160))
        self.assertEqual(formal.state_aware_control.work_candidates, (131072, 163840))
        for config in configs.values():
            for scenario in config.scenarios:
                expected_offsets = (
                    (0.125,) if scenario.job_count == 1 else (0.125, 60.625)
                )
                self.assertEqual(scenario.arrival_offsets_s, expected_offsets)

        adaptive = formal.scenarios[2]
        command = build_job_command(
            RunnerOptions(
                config_path=Path("config.json"),
                profiler_path=Path("profile.py"),
                python_executable=Path(sys.executable),
                output_dir=Path("out"),
                health_url="http://health",
                metrics_urls=("http://metrics0", "http://metrics1"),
                ray_address="127.0.0.1:6380",
                idle_timeout_s=1.0,
            ),
            formal,
            adaptive,
            GroupRunIdentity("formal", 1, 0),
            job_index=0,
            start_epoch_s=100.0,
            coordinator_name="credits",
        )

        def flag_value(flag: str) -> str:
            return command[command.index(flag) + 1]

        self.assertEqual(flag_value("--max-inflight"), "160")
        self.assertEqual(flag_value("--max-active-work-per-endpoint"), "163840")
        self.assertEqual(flag_value("--shared-credit-request-limit"), "128")
        self.assertEqual(flag_value("--shared-credit-work-limit"), "131072")


if __name__ == "__main__":
    unittest.main()
