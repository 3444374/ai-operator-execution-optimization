from __future__ import annotations

import csv
import hashlib
import json
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.saor.feeding_gap_diagnostic import (
    D0,
    D1,
    P0,
    classify_feeding_gap,
    load_diagnostic_contract,
    sha256_file,
    sha256_lf_normalized_text_file,
    summarize_feeding_gap,
    validate_diagnostic_config,
    validate_prior_failed_lock,
)
from src.experiments.saor.feeding_gap_preflight import (
    collect_pre_run_clean_gate,
)
from src.experiments.scenarios.core import build_scenario_schedule
from src.experiments.shared_vllm.config import (
    SharedVllmConfig,
    SharedVllmScenario,
)


REPOSITORY = Path(__file__).resolve().parents[3]
ARM_ORDER_FOR_TEST = (D0, D1, P0)


class FeedingGapDiagnosticTests(unittest.TestCase):
    def test_four_way_classification_is_frozen(self) -> None:
        cases = {
            (0.94, 0.96): "work_envelope_primary",
            (0.96, 0.94): "project_path_primary",
            (0.94, 0.94): "work_envelope_and_project_path",
            (0.95, 0.95): "original_gap_not_reproduced",
        }
        for ratios, expected in cases.items():
            with self.subTest(ratios=ratios):
                self.assertEqual(
                    classify_feeding_gap(*ratios, ratio_min=0.95),
                    expected,
                )

    def test_repository_contract_preserves_failed_feeding_lock(self) -> None:
        diagnostic = load_diagnostic_contract(
            REPOSITORY
            / "deploy/autodl/saor_feeding_gap_diagnostic_contract.json"
        )
        prior = (
            REPOSITORY
            / "deploy/autodl/saor_project_mechanism_formal_contract.json"
        )

        self.assertEqual(validate_prior_failed_lock(diagnostic, prior), [])
        self.assertIs(diagnostic["may_change_prior_feeding_decision"], False)
        prior_payload = json.loads(prior.read_text(encoding="utf-8"))
        self.assertEqual(prior_payload["status"], "locked_failed_feeding")
        self.assertIs(prior_payload["formal_authorized"], False)

    def test_text_contract_hash_ignores_only_crlf_translation(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            lf_path = root / "lf.json"
            crlf_path = root / "crlf.json"
            changed_path = root / "changed.json"
            lf_path.write_bytes(b'{"status":"locked"}\n')
            crlf_path.write_bytes(b'{"status":"locked"}\r\n')
            changed_path.write_bytes(b'{"status":"changed"}\r\n')

            expected = sha256_lf_normalized_text_file(lf_path)

            self.assertEqual(
                sha256_lf_normalized_text_file(crlf_path),
                expected,
            )
            self.assertNotEqual(
                sha256_lf_normalized_text_file(changed_path),
                expected,
            )

    def test_structured_clean_gate_requires_all_three_subsystems(self) -> None:
        config = self._config(())
        passed = collect_pre_run_clean_gate(
            config,
            metrics_urls=("metrics-0", "metrics-1"),
            ray_address="ray://test",
            endpoint_probe=lambda: {"status": "passed"},
            postgres_probe=lambda: {"status": "passed"},
            ray_probe=lambda: {"status": "passed"},
        )
        failed = collect_pre_run_clean_gate(
            config,
            metrics_urls=("metrics-0", "metrics-1"),
            ray_address="ray://test",
            endpoint_probe=lambda: {"status": "passed"},
            postgres_probe=lambda: {"status": "failed"},
            ray_probe=lambda: {"status": "passed"},
        )

        self.assertEqual(passed["status"], "passed")
        self.assertEqual(failed["status"], "failed")

    def test_config_freezes_d0_d1_p0_without_job_fairness_in_d1(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            manifests = (root / "bulk.jsonl", root / "foreground.jsonl")
            for index, path in enumerate(manifests):
                path.write_text(f"request-{index}\n", encoding="utf-8")
            paths = tuple(str(path) for path in manifests)
            proposed = SharedVllmScenario(
                scenario_id="reference-saor",
                policy="saor_bounded_ready",
                job_count=2,
                rows_per_job=None,
                rows_per_jobs=(1, 1),
                weights=(1, 1),
                arrival_offsets_s=(0.0, 5.0),
                request_manifests=paths,
                priorities=(0, 1),
                slo_targets_s=(None, 30.0),
                priority_windows_s=(None, 30.0),
                debt_cap_fractions=(0.125, None),
                ready_observation_contract=(
                    "bounded_concrete_pre_registration"
                ),
            )
            reference = self._config((proposed,))
            arms = (
                replace(
                    proposed,
                    scenario_id=D0,
                    policy="direct_no_job",
                    ready_observation_contract="single_head",
                ),
                replace(
                    proposed,
                    scenario_id=D1,
                    policy="direct_work_limited",
                    ready_observation_contract="single_head",
                ),
                replace(
                    proposed,
                    scenario_id=P0,
                    policy="shared_fifo",
                    ready_observation_contract=(
                        "bounded_concrete_pre_registration"
                    ),
                ),
            )
            diagnostic = replace(
                reference,
                experiment_id="saor_feeding_gap_diagnostic",
                seed=20260815,
                warmup_runs_per_scenario=1,
                formal_repeats=3,
                scenarios=arms,
                shared_credit_namespace="feeding-gap",
            )
            contract = {
                "experiment_id": "saor_feeding_gap_diagnostic",
                "decision_contract": {"ratio_min": 0.95},
                "matrix": {
                    "k_per_endpoint": 128,
                    "w_per_endpoint": 65536,
                    "request_manifest_sha256": [
                        hashlib.sha256(path.read_bytes()).hexdigest()
                        for path in manifests
                    ],
                },
            }

            self.assertEqual(
                validate_diagnostic_config(reference, diagnostic, contract),
                [],
            )
            self.assertEqual(diagnostic.scenarios[1].policy, "direct_work_limited")
            self.assertEqual(
                diagnostic.scenarios[1].ready_observation_contract,
                "single_head",
            )

    def test_complete_matrix_attributes_a_work_envelope_gap(self) -> None:
        contract_path = (
            REPOSITORY
            / "deploy/autodl/saor_feeding_gap_diagnostic_contract.json"
        )
        prior_path = (
            REPOSITORY
            / "deploy/autodl/saor_project_mechanism_formal_contract.json"
        )
        contract = load_diagnostic_contract(contract_path)
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "jobs").mkdir()
            (root / "traces").mkdir()
            (root / "pre_run_clean_gate.json").write_text(
                json.dumps({"schema_version": 1, "status": "passed"}),
                encoding="utf-8",
            )
            (root / "manifest.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "completed",
                        "schedule": [
                            {
                                "scenario_id": scheduled.scenario_id,
                                "phase": scheduled.phase,
                                "repeat_index": scheduled.repeat_index,
                                "order_index": scheduled.order_index,
                            }
                            for scheduled in build_scenario_schedule(
                                ARM_ORDER_FOR_TEST,
                                1,
                                3,
                                20260815,
                            )
                        ],
                    }
                ),
                encoding="utf-8",
            )
            (root / "feeding_gap_contract_snapshot.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "diagnostic_only_ready",
                        "may_change_prior_feeding_decision": False,
                        "diagnostic_contract_sha256": sha256_file(contract_path),
                        "prior_failed_contract_sha256": (
                            sha256_lf_normalized_text_file(prior_path)
                        ),
                    }
                ),
                encoding="utf-8",
            )
            rows = []
            rates = {D0: 100.0, D1: 94.0, P0: 94.0}
            schedule = build_scenario_schedule(
                ARM_ORDER_FOR_TEST,
                1,
                3,
                20260815,
            )
            for scheduled in schedule:
                arm_id = scheduled.scenario_id
                phase = scheduled.phase
                repeat = scheduled.repeat_index
                order = scheduled.order_index
                rate = rates[arm_id]
                row = self._result_row(
                    arm_id,
                    phase,
                    repeat,
                    order,
                    rate,
                )
                rows.append(row)
                if arm_id == P0:
                    stem = f"{order:03d}_{phase}_{repeat}_{arm_id}"
                    for job_index in range(2):
                        self._write_rows(
                            root
                            / "jobs"
                            / f"{stem}_job{job_index}.runs.csv",
                            [
                                {
                                    "actor_ready_s": 1.0,
                                    "submit_s": 2.0,
                                    "bounded_wait_s": 3.0,
                                    "avg_bounded_wait_s": 0.3,
                                }
                            ],
                        )
                    self._write_rows(
                        root / "traces" / f"{stem}.credits.csv",
                        [
                            {
                                "active_requests": 100,
                                "active_work": 60000,
                                "waiting_requests": 10,
                                "waiting_work": 5000,
                            }
                        ],
                    )
            self._write_rows(root / "group_runs.csv", rows)

            summary = summarize_feeding_gap(
                root,
                prior_contract_path=prior_path,
                diagnostic_contract=contract,
                diagnostic_contract_sha256=sha256_file(contract_path),
            )

        self.assertTrue(summary["evidence_valid"])
        self.assertEqual(summary["classification"], "work_envelope_primary")
        self.assertAlmostEqual(summary["d1_over_d0_mean"], 0.94)
        self.assertAlmostEqual(summary["p0_over_d1_mean"], 1.0)

    def test_malformed_threshold_and_metric_fail_closed(self) -> None:
        contract_path = (
            REPOSITORY
            / "deploy/autodl/saor_feeding_gap_diagnostic_contract.json"
        )
        prior_path = (
            REPOSITORY
            / "deploy/autodl/saor_project_mechanism_formal_contract.json"
        )
        contract = load_diagnostic_contract(contract_path)
        malformed_contract = {
            **contract,
            "decision_contract": {"ratio_min": "not-a-number"},
        }
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pre_run_clean_gate.json").write_text(
                json.dumps({"status": "failed"}), encoding="utf-8"
            )
            summary = summarize_feeding_gap(
                root,
                prior_contract_path=prior_path,
                diagnostic_contract=malformed_contract,
                diagnostic_contract_sha256=sha256_file(contract_path),
            )

        self.assertFalse(summary["evidence_valid"])
        self.assertEqual(summary["classification"], "unavailable")
        self.assertTrue(
            any("ratio threshold" in error for error in summary["errors"])
        )

    @staticmethod
    def _result_row(
        arm_id: str,
        phase: str,
        repeat: int,
        order: int,
        rate: float,
    ) -> dict[str, object]:
        direct = arm_id in {D0, D1}
        return {
            "scenario_id": arm_id,
            "phase": phase,
            "repeat_index": repeat,
            "order_index": order,
            "policy": {
                D0: "direct_no_job",
                D1: "direct_work_limited",
                P0: "shared_fifo",
            }[arm_id],
            "metrics_status": "ok",
            "resource_metrics_status": "ok",
            "mfu_status": "ok",
            "tokens_per_s": rate,
            "gpu_utilization_pct_mean": 95.0,
            "gpu_power_w_mean": 400.0,
            "gpu_energy_j": 1000.0,
            "energy_j_per_1k_observed_tokens": 1.0,
            "vllm_running_mean": 100.0,
            "vllm_waiting_mean": 0.0,
            "vllm_kv_usage_mean": 0.5,
            "vllm_time_to_first_token_p99_s": 1.0,
            "vllm_inter_token_latency_p99_s": 0.1,
            "job_jct_s": "[10.0, 5.0]",
            "job_p99_s": "[2.0, 1.0]",
            "job_slo_violation_ratio": "[0.0, 0.0]",
            "job_slo_goodput_per_s": "[1.0, 1.0]",
            "job_exactly_once": "[true, true]",
            "mfu_estimate": 0.5,
            "direct_admission_trace_status": (
                "ok:lossless_acquire_release_ledger"
                if direct
                else "not_applicable"
            ),
            "direct_work_limit_applied": arm_id == D1,
            "direct_request_occupancy_fraction_mean": 0.8 if direct else 0.0,
            "direct_request_occupancy_max": 128 if direct else 0,
            "direct_estimated_work_occupancy_max": 65536 if direct else 0,
            "direct_estimated_work_to_reference_w_fraction_mean": (
                0.9 if direct else 0.0
            ),
            "direct_admission_wait_p50_s": 0.1 if direct else 0.0,
            "direct_admission_wait_p95_s": 0.2 if direct else 0.0,
            "direct_admission_wait_p99_s": 0.3 if direct else 0.0,
            "direct_admission_wait_max_s": 0.4 if direct else 0.0,
            "credit_trace_status": (
                "ok:sampled_endpoint_credit"
                if arm_id == P0
                else "unavailable:not_a_shared_policy"
            ),
            "bounded_ready_event_status": (
                "ok:actor_event_join" if arm_id == P0 else "not_applicable"
            ),
        }

    @staticmethod
    def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _config(
        scenarios: tuple[SharedVllmScenario, ...],
    ) -> SharedVllmConfig:
        return SharedVllmConfig(
            experiment_id="reference",
            seed=1,
            warmup_runs_per_scenario=1,
            formal_repeats=3,
            endpoint_ids=("endpoint-0", "endpoint-1"),
            service_signature=(("model", "qwen"), ("service", "vllm")),
            request_limit_per_endpoint=128,
            work_limit_per_endpoint=65536,
            credit_quantum=2048,
            shared_credit_namespace="reference",
            gpu_peak_tflops=165.0,
            mfu_precision="bf16",
            common_args=(
                "--database-url",
                "postgresql://postgres:postgres@localhost:5432/ai_operator",
                "--completion-endpoint-urls",
                "http://127.0.0.1:8000/v1/chat/completions,"
                "http://127.0.0.1:8001/v1/chat/completions",
                "--completion-model",
                "qwen",
                "--completion-protocol",
                "chat_completions",
                "--completion-prompt-token-overhead",
                "29",
                "--completion-max-tokens",
                "256",
                "--output-cost-mode",
                "fixed_output_cap",
            ),
            scenarios=scenarios,
            service_metadata=(("vllm_version", "test"),),
            fail_closed_rehearsal=True,
            ready_payload_bytes_limit_per_job=1024,
        )


if __name__ == "__main__":
    unittest.main()
