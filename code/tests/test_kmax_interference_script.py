from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts import run_kmax_interference_experiment as experiment  # noqa: E402


class KmaxInterferenceScriptTests(unittest.TestCase):
    def test_static_multi_gpu_defaults_to_endpoint_local_capacity(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "run_kmax_interference_experiment",
                "--endpoint-urls",
                "http://gpu0/v1/completions,http://gpu1/v1/completions",
                "--metrics-urls",
                "http://gpu0/metrics,http://gpu1/metrics",
            ],
        ):
            args = experiment.parse_args()

        command = experiment.profile_command(
            args,
            experiment_id="static_dual",
            total_rows=1,
            ray_batch_rows=1,
            max_inflight=16,
            output="output.csv",
            completion_max_tokens=1,
        )

        self.assertEqual(args.endpoint_routing, "least_queued")
        self.assertEqual(
            command[command.index("--admission-scope") + 1],
            "per_endpoint",
        )

    def test_global_only_controller_rejects_endpoint_local_scope(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "run_kmax_interference_experiment",
                "--include-aimd",
            ],
        ):
            args = experiment.parse_args()

        with self.assertRaisesRegex(ValueError, "requires --admission-scope global"):
            experiment.profile_command(
                args,
                experiment_id="invalid_aimd",
                total_rows=1,
                ray_batch_rows=1,
                max_inflight=16,
                output="output.csv",
                completion_max_tokens=1,
                scheduling_policy="aimd",
            )

    def test_multi_endpoint_urls_are_trimmed_and_cardinality_checked(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "run_kmax_interference_experiment",
                "--endpoint-urls",
                " http://gpu0/v1/completions , http://gpu1/v1/completions ",
                "--metrics-urls",
                " http://gpu0/metrics , http://gpu1/metrics ",
            ],
        ):
            args = experiment.parse_args()

        command = experiment.profile_command(
            args,
            experiment_id="dual",
            total_rows=1,
            ray_batch_rows=1,
            max_inflight=1,
            output="output.csv",
            completion_max_tokens=1,
        )

        self.assertEqual(
            command[command.index("--completion-endpoint-urls") + 1],
            "http://gpu0/v1/completions,http://gpu1/v1/completions",
        )

        args.metrics_urls = "http://gpu0/metrics"
        with self.assertRaisesRegex(ValueError, "count must equal"):
            experiment.profile_command(
                args,
                experiment_id="invalid",
                total_rows=1,
                ray_batch_rows=1,
                max_inflight=1,
                output="output.csv",
                completion_max_tokens=1,
            )

    def test_default_outputs_use_new_schema_version_without_overwriting_history(
        self,
    ) -> None:
        with patch.object(sys, "argv", ["run_kmax_interference_experiment"]):
            args = experiment.parse_args()

        self.assertTrue(args.small_output.endswith("_20260726.csv"))
        self.assertTrue(args.bulk_output.endswith("_20260726.csv"))

    def test_planned_scenarios_include_static_controls_and_typed_aimd(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "run_kmax_interference_experiment",
                "--background-static-kmax",
                "8,16",
                "--include-aimd",
                "--admission-scope",
                "global",
            ],
        ):
            args = experiment.parse_args()

        self.assertEqual(
            experiment.build_scenarios(args),
            [
                (8, "bulk_k8", "static"),
                (16, "bulk_k16", "static"),
                (16, "bulk_aimd", "aimd"),
            ],
        )

    def test_aimd_profile_command_emits_controller_and_trace_arguments(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "run_kmax_interference_experiment",
                "--trace-dir",
                "tmp/shared-vllm-traces",
                "--flush-policy",
                "queue_adaptive",
                "--flush-timeout-ms",
                "25",
                "--flush-max-wait-ms",
                "50",
                "--admission-scope",
                "global",
            ],
        ):
            args = experiment.parse_args()

        command = experiment.profile_command(
            args,
            experiment_id="interference_bulk_aimd_background_r1",
            total_rows=128,
            ray_batch_rows=64,
            max_inflight=16,
            output="tmp/background.csv",
            completion_max_tokens=512,
            scheduling_policy="aimd",
        )

        self.assertIn("--completion-return-token-ids", command)
        self.assertIn("--request-trace-output", command)
        self.assertIn("--submission-trace-output", command)
        self.assertIn("--resource-trace-output", command)
        self.assertIn("--control-trace-output", command)
        self.assertEqual(command[command.index("--random-seed") + 1], "20260804")
        self.assertEqual(
            command[command.index("--scenario-id") + 1],
            "interference_bulk_aimd_background_r1",
        )
        self.assertEqual(command[command.index("--flush-policy") + 1], "queue_adaptive")
        self.assertEqual(command[command.index("--flush-timeout-ms") + 1], "25.0")
        self.assertEqual(command[command.index("--flush-max-wait-ms") + 1], "50.0")
        self.assertEqual(command[command.index("--controller-min-window") + 1], "4")
        self.assertEqual(command[command.index("--controller-max-window") + 1], "16")
        self.assertEqual(command[command.index("--controller-initial-window") + 1], "8")
        request_trace = command[command.index("--request-trace-output") + 1]
        self.assertTrue(request_trace.endswith("interference_bulk_aimd_background_r1.requests.csv"))

    def test_aimd_hol_scenario_and_command_emit_hol_age_thresholds(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "run_kmax_interference_experiment",
                "--background-static-kmax",
                "",
                "--include-aimd-hol",
                "--admission-scope",
                "global",
                "--trace-dir",
                "tmp/shared-vllm-traces-hol",
                "--hol-age-congestion-s",
                "1.5",
                "--hol-age-low-load-s",
                "0.3",
            ],
        ):
            args = experiment.parse_args()

        self.assertEqual(
            experiment.build_scenarios(args),
            [(16, "bulk_aimd_hol", "aimd_hol")],
        )

        command = experiment.profile_command(
            args,
            experiment_id="interference_bulk_aimd_hol_background_r1",
            total_rows=512,
            ray_batch_rows=64,
            max_inflight=16,
            output="tmp/background.csv",
            completion_max_tokens=512,
            scheduling_policy="aimd_hol",
        )

        self.assertEqual(command[command.index("--scheduling-policy") + 1], "aimd_hol")
        self.assertEqual(command[command.index("--controller-max-window") + 1], "16")
        self.assertEqual(command[command.index("--hol-age-congestion-s") + 1], "1.5")
        self.assertEqual(command[command.index("--hol-age-low-load-s") + 1], "0.3")
        self.assertIn("--control-trace-output", command)

    def test_scenario_order_is_deterministic_and_interleaved_by_repeat(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "run_kmax_interference_experiment",
                "--background-static-kmax",
                "8,16",
                "--include-aimd",
                "--admission-scope",
                "global",
                "--random-seed",
                "20260804",
            ],
        ):
            args = experiment.parse_args()

        first = experiment.scenarios_for_repeat(args, repeat=1)
        repeated = experiment.scenarios_for_repeat(args, repeat=1)
        second = experiment.scenarios_for_repeat(args, repeat=2)

        self.assertEqual(first, repeated)
        self.assertCountEqual(first, experiment.build_scenarios(args))
        self.assertCountEqual(second, experiment.build_scenarios(args))
        self.assertNotEqual(first, second)

    def test_shared_credit_command_emits_work_limit_and_job_weight(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "run_kmax_interference_experiment",
                "--max-active-work-per-endpoint",
                "32768",
                "--shared-credit-coordinator-name",
                "interference-credits",
                "--shared-credit-request-limit",
                "64",
                "--shared-credit-work-limit",
                "32768",
                "--shared-credit-quantum",
                "2048",
            ],
        ):
            args = experiment.parse_args()

        command = experiment.profile_command(
            args,
            experiment_id="shared_foreground",
            total_rows=128,
            ray_batch_rows=64,
            max_inflight=64,
            output="tmp/foreground.csv",
            completion_max_tokens=256,
            shared_credit_job_weight=3,
        )

        self.assertEqual(
            command[command.index("--max-active-work-per-endpoint") + 1],
            "32768",
        )
        self.assertEqual(
            command[command.index("--shared-credit-coordinator-name") + 1],
            "interference-credits",
        )
        self.assertEqual(
            command[command.index("--shared-credit-job-weight") + 1],
            "3",
        )


if __name__ == "__main__":
    unittest.main()
