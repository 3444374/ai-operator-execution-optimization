from __future__ import annotations

import csv
import importlib.util
import json
import os
import re
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src.baselines.common.contracts import BaselineRequestResult, ChatRequest
from src.baselines.common.manifests import write_manifest
from src.experiments.saor.project_mechanism_formal import (
    EXPECTED_SCENARIOS as PROJECT_FORMAL_SCENARIOS,
    PROPOSED as PROJECT_FORMAL_PROPOSED,
    REVIEWED_REHEARSAL_EVIDENCE,
    VTC as PROJECT_FORMAL_VTC,
    completion_fairness_from_raw,
    load_contract as load_project_formal_contract,
    sha256_file as project_formal_sha256,
    validate_contract as validate_project_formal_contract,
)
from src.experiments.saor.feeding_ceiling import (
    CEILING_SCENARIO,
    summarize_feeding_ceiling,
    validate_ceiling_config,
)
from src.experiments.shared_vllm.config import (
    CompletionWorkCostConfig,
    SharedVllmConfig,
    SharedVllmScenario,
    load_config,
)
from src.experiments.shared_vllm.work_evidence import (
    audit_work_cost_matrix,
    join_request_submission_work,
)
from src.experiments.shared_vllm.direct_control import run_direct_control


REPOSITORY = Path(__file__).resolve().parents[3]


def _load(name: str, relative: str):
    spec = importlib.util.spec_from_file_location(name, REPOSITORY / relative)
    if spec is None or spec.loader is None:
        raise RuntimeError(relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


SUMMARY = _load(
    "summarize_saor_active_set",
    "code/scripts/analysis/summarize_saor_active_set.py",
)
AUDIT = _load(
    "audit_saor_formal_readiness",
    "code/scripts/analysis/audit_saor_formal_readiness.py",
)
PRIORITY_SUMMARY = _load(
    "summarize_saor_priority_reachability",
    "code/scripts/analysis/summarize_saor_priority_reachability.py",
)
BOUNDED_SUMMARY = _load(
    "summarize_saor_bounded_priority_gate",
    "code/scripts/analysis/summarize_saor_bounded_priority_gate.py",
)
MATCHED_READY_SUMMARY = _load(
    "summarize_saor_matched_ready_ablation",
    "code/scripts/analysis/summarize_saor_matched_ready_ablation.py",
)
OBSERVATION_BRIDGE_SUMMARY = _load(
    "summarize_saor_ready_observation_bridge",
    "code/scripts/analysis/summarize_saor_ready_observation_bridge.py",
)
PROJECT_FORMAL_SUMMARY = _load(
    "summarize_saor_project_mechanism_formal",
    "code/scripts/analysis/summarize_saor_project_mechanism_formal.py",
)
class SaorFormalToolsTests(unittest.TestCase):
    def test_prompt_overhead_audit_recomputes_uniform_chat_cost(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            jobs = root / "jobs"
            jobs.mkdir()
            self._write_group_rows(
                jobs / "cell.requests.csv",
                [
                    self._work_request_row(
                        scenario_id="cell",
                        job_id="job",
                        endpoint_id="endpoint-0",
                        submission_id="job:request:1",
                        doc_id="1",
                        completion_epoch_s=1.0,
                        raw_prompt_tokens=36,
                        output_tokens=256,
                        estimated_output_tokens=256,
                    ),
                    self._work_request_row(
                        scenario_id="cell",
                        job_id="job",
                        endpoint_id="endpoint-0",
                        submission_id="job:request:2",
                        doc_id="2",
                        completion_epoch_s=2.0,
                        raw_prompt_tokens=41,
                        output_tokens=100,
                        estimated_output_tokens=256,
                    ),
                ],
            )
            self._write_group_rows(
                jobs / "cell.submissions.csv",
                [
                    self._work_submission_row(
                        job_id="job",
                        endpoint_id="endpoint-0",
                        submission_id="job:request:1",
                        doc_id="1",
                        raw_prompt_tokens=36,
                        output_tokens=256,
                    ),
                    self._work_submission_row(
                        job_id="job",
                        endpoint_id="endpoint-0",
                        submission_id="job:request:2",
                        doc_id="2",
                        raw_prompt_tokens=41,
                        output_tokens=100,
                    ),
                ],
            )

            requests = list(
                csv.DictReader(
                    (jobs / "cell.requests.csv").read_text(
                        encoding="utf-8"
                    ).splitlines()
                )
            )
            submissions = list(
                csv.DictReader(
                    (jobs / "cell.submissions.csv").read_text(
                        encoding="utf-8"
                    ).splitlines()
                )
            )
            submissions = list(
                csv.DictReader(
                    (jobs / "cell.submissions.csv").read_text(
                        encoding="utf-8"
                    ).splitlines()
                )
            )
            joined = join_request_submission_work(
                requests,
                submissions,
                work_cost=CompletionWorkCostConfig(
                    "chat_completions",
                    29,
                    "fixed_output_cap",
                    256,
                ),
                context="cell",
                require_endpoint_usage=True,
                require_estimate_upper_bound=True,
            )

            self.assertEqual([item.prompt_overhead_tokens for item in joined], [29, 29])

    def test_prompt_overhead_audit_fails_nonuniform_or_batched_evidence(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            jobs = root / "jobs"
            jobs.mkdir()
            self._write_group_rows(
                jobs / "cell.requests.csv",
                [
                    self._work_request_row(
                        scenario_id="cell",
                        job_id="job",
                        endpoint_id="endpoint-0",
                        submission_id="job:request:1",
                        doc_id="1",
                        completion_epoch_s=1.0,
                        raw_prompt_tokens=36,
                        output_tokens=256,
                        estimated_output_tokens=256,
                    ),
                    self._work_request_row(
                        scenario_id="cell",
                        job_id="job",
                        endpoint_id="endpoint-0",
                        submission_id="job:request:2",
                        doc_id="2",
                        completion_epoch_s=2.0,
                        raw_prompt_tokens=41,
                        output_tokens=100,
                        estimated_output_tokens=256,
                    ),
                ],
            )
            self._write_group_rows(
                jobs / "cell.submissions.csv",
                [
                    self._work_submission_row(
                        job_id="job",
                        endpoint_id="endpoint-0",
                        submission_id="job:request:1",
                        doc_id="1",
                        raw_prompt_tokens=36,
                        output_tokens=256,
                    ),
                    {
                        **self._work_submission_row(
                            job_id="job",
                            endpoint_id="endpoint-0",
                            submission_id="job:request:2",
                            doc_id="2",
                            raw_prompt_tokens=41,
                            output_tokens=100,
                        ),
                        "rows": 2,
                    },
                ],
            )

            requests = list(
                csv.DictReader(
                    (jobs / "cell.requests.csv").read_text(
                        encoding="utf-8"
                    ).splitlines()
                )
            )
            submissions = list(
                csv.DictReader(
                    (jobs / "cell.submissions.csv").read_text(
                        encoding="utf-8"
                    ).splitlines()
                )
            )
            with self.assertRaisesRegex(ValueError, "one row"):
                join_request_submission_work(
                    requests,
                    submissions,
                    work_cost=CompletionWorkCostConfig(
                        "chat_completions",
                        29,
                        "fixed_output_cap",
                        256,
                    ),
                    context="cell",
                    require_endpoint_usage=True,
                    require_estimate_upper_bound=True,
                )

    def test_work_join_rejects_fallback_sources_and_estimate_overrun(
        self,
    ) -> None:
        request = self._work_request_row(
            scenario_id="cell",
            job_id="job",
            endpoint_id="endpoint-0",
            submission_id="submission-1",
            doc_id="1",
            completion_epoch_s=1.0,
            output_tokens=10,
            estimated_output_tokens=9,
        )
        submission = self._work_submission_row(
            job_id="job",
            endpoint_id="endpoint-0",
            submission_id="submission-1",
            doc_id="1",
            output_tokens=10,
        )
        fallback = {**submission, "token_count_source": "resolved_input_plus_output"}
        with self.assertRaisesRegex(ValueError, "not endpoint usage"):
            join_request_submission_work(
                [request],
                [fallback],
                work_cost=CompletionWorkCostConfig(
                    "chat_completions",
                    29,
                    "fixed_output_cap",
                    9,
                ),
                context="cell",
                require_endpoint_usage=True,
                require_estimate_upper_bound=True,
            )
        with self.assertRaisesRegex(ValueError, "exceeds estimate"):
            join_request_submission_work(
                [request],
                [submission],
                work_cost=CompletionWorkCostConfig(
                    "chat_completions",
                    29,
                    "fixed_output_cap",
                    9,
                ),
                context="cell",
                require_endpoint_usage=True,
                require_estimate_upper_bound=True,
            )

    def test_work_join_uses_admission_estimate_not_text_retokenization(
        self,
    ) -> None:
        request = self._work_request_row(
            scenario_id="cell",
            job_id="job",
            endpoint_id="endpoint-0",
            submission_id="submission-1",
            doc_id="1",
            completion_epoch_s=1.0,
            raw_prompt_tokens=1001,
            output_tokens=256,
            estimated_output_tokens=256,
            client_estimated_output_tokens=207,
        )
        submission = self._work_submission_row(
            job_id="job",
            endpoint_id="endpoint-0",
            submission_id="submission-1",
            doc_id="1",
            raw_prompt_tokens=1001,
            output_tokens=256,
        )

        joined = join_request_submission_work(
            [request],
            [submission],
            work_cost=CompletionWorkCostConfig(
                "chat_completions",
                29,
                "fixed_output_cap",
                256,
            ),
            context="cell",
            require_endpoint_usage=True,
            require_estimate_upper_bound=True,
        )

        self.assertEqual(joined[0].actual_work, 1286)
        self.assertEqual(joined[0].estimated_work, 1286)

        inflated = {**request, "estimated_output_tokens": 257}
        with self.assertRaisesRegex(
            ValueError,
            "does not equal the configured completion cap",
        ):
            join_request_submission_work(
                [inflated],
                [submission],
                work_cost=CompletionWorkCostConfig(
                    "chat_completions",
                    29,
                    "fixed_output_cap",
                    256,
                ),
                context="cell",
                require_endpoint_usage=True,
                require_estimate_upper_bound=True,
            )

    def test_completion_fairness_charges_endpoint_total_work(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            jobs = root / "jobs"
            jobs.mkdir()
            scenario_id = "cell"
            row = {
                "order_index": "0",
                "phase": "warmup",
                "repeat_index": "1",
                "scenario_id": scenario_id,
            }
            for job_index, completion in enumerate((1.0, 2.0)):
                stem = f"000_warmup_1_{scenario_id}_job{job_index}"
                submission_id = f"submission-{job_index}"
                self._write_group_rows(
                    jobs / f"{stem}.requests.csv",
                    [
                        self._work_request_row(
                            scenario_id=scenario_id,
                            job_id=f"job-{job_index}",
                            endpoint_id=f"endpoint-{job_index}",
                            submission_id=submission_id,
                            doc_id=str(job_index),
                            completion_epoch_s=completion,
                        )
                    ],
                )
                self._write_group_rows(
                    jobs / f"{stem}.submissions.csv",
                    [
                        self._work_submission_row(
                            job_id=f"job-{job_index}",
                            endpoint_id=f"endpoint-{job_index}",
                            submission_id=submission_id,
                            doc_id=str(job_index),
                            ready_epoch_s=0.0,
                            registered_epoch_s=0.0,
                            granted_epoch_s=0.0,
                        )
                    ],
                )

            fairness = completion_fairness_from_raw(
                root,
                row,
                work_cost=CompletionWorkCostConfig(
                    "chat_completions",
                    29,
                    "fixed_output_cap",
                    10,
                ),
            )

            self.assertEqual(
                fairness["completion_service_lag_status"],
                "ok:registered_backlog_completion_accounted_empirical",
            )
            self.assertEqual(fairness["completion_service_lag_p95_work"], 24.5)

    def test_completion_fairness_rejects_incomplete_lifecycle_with_path(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            jobs = root / "jobs"
            jobs.mkdir()
            scenario_id = "cell"
            row = {
                "order_index": "0",
                "phase": "warmup",
                "repeat_index": "1",
                "scenario_id": scenario_id,
            }
            for job_index in range(2):
                stem = f"000_warmup_1_{scenario_id}_job{job_index}"
                submission_id = f"submission-{job_index}"
                self._write_group_rows(
                    jobs / f"{stem}.requests.csv",
                    [
                        self._work_request_row(
                            scenario_id=scenario_id,
                            job_id=f"job-{job_index}",
                            endpoint_id=f"endpoint-{job_index}",
                            submission_id=submission_id,
                            doc_id=str(job_index),
                            completion_epoch_s=float(job_index + 1),
                        )
                    ],
                )
                self._write_group_rows(
                    jobs / f"{stem}.submissions.csv",
                    [
                        self._work_submission_row(
                            job_id=f"job-{job_index}",
                            endpoint_id=f"endpoint-{job_index}",
                            submission_id=submission_id,
                            doc_id=str(job_index),
                            ready_epoch_s=0.1,
                            registered_epoch_s=0.2,
                            granted_epoch_s=(0.3 if job_index else ""),
                        )
                    ],
                )

            with self.assertRaisesRegex(
                ValueError,
                (
                    "000_warmup_1_cell_job0.submissions.csv has an "
                    "incomplete registered-ready service join"
                ),
            ):
                completion_fairness_from_raw(
                    root,
                    row,
                    work_cost=CompletionWorkCostConfig(
                        "chat_completions",
                        29,
                        "fixed_output_cap",
                        10,
                    ),
                )

    def test_matrix_work_audit_rejects_missing_frozen_arm(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            rows = [
                {
                    "scenario_id": scenario_id,
                    "phase": "warmup",
                    "repeat_index": 1,
                    "order_index": index,
                }
                for index, scenario_id in enumerate(
                    tuple(PROJECT_FORMAL_SCENARIOS)[:-1]
                )
            ]
            self._write_group_rows(root / "group_runs.csv", rows)

            result = audit_work_cost_matrix(
                root,
                work_cost=CompletionWorkCostConfig(
                    "chat_completions",
                    29,
                    "fixed_output_cap",
                    10,
                ),
                expected_scenarios=PROJECT_FORMAL_SCENARIOS,
                expected_phase="warmup",
                expected_repeat_indexes=(1,),
                expected_requests_per_cell=1024,
            )

            self.assertEqual(result["status"], "failed")
            self.assertTrue(
                any("matrix cells differ" in error for error in result["errors"])
            )

    def test_feeding_ceiling_separates_valid_negative_from_bad_evidence(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            project = root / "project"
            ceiling = root / "ceiling"
            project.mkdir()
            ceiling.mkdir()
            common = {
                "phase": "warmup",
                "execution_mode": "rehearsal",
                "metrics_status": "ok",
                "resource_metrics_status": "ok",
                "request_manifest_sha256": '["a", "b"]',
                "arrival_offsets_s": "[0.0, 5.0]",
                "job_arrived_rows": "[512, 512]",
                "job_completed_rows": "[512, 512]",
                "job_failed_rows": "[0, 0]",
                "job_exactly_once": "[true, true]",
                "request_success_delta": "1024",
                "prompt_tokens_delta": "636378",
            }
            self._write_group_rows(
                project / "group_runs.csv",
                [
                    {
                        "scenario_id": PROJECT_FORMAL_PROPOSED,
                        "policy": "saor_bounded_ready",
                        "tokens_per_s": "100",
                        **common,
                    }
                ],
            )
            self._write_group_rows(
                ceiling / "group_runs.csv",
                [
                    {
                        "scenario_id": CEILING_SCENARIO,
                        "policy": "direct_no_job",
                        "tokens_per_s": "110",
                        **common,
                    }
                ],
            )

            result = summarize_feeding_ceiling(project, ceiling)
            self.assertTrue(result["evidence_valid"])
            self.assertFalse(result["feeding_gate_passed"])
            self.assertEqual(result["status"], "failed_feeding")

            rows = list(
                csv.DictReader(
                    (ceiling / "group_runs.csv").read_text(
                        encoding="utf-8"
                    ).splitlines()
                )
            )
            rows[0]["request_manifest_sha256"] = '["x", "y"]'
            self._write_group_rows(ceiling / "group_runs.csv", rows)
            invalid = summarize_feeding_ceiling(project, ceiling)
            self.assertFalse(invalid["evidence_valid"])
            self.assertEqual(invalid["status"], "invalid_evidence")

    def test_project_rehearsal_validator_writes_absolute_gate_result(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "matrix"
            root.mkdir()
            contract_path = Path(directory) / "contract.json"
            contract = load_project_formal_contract(
                REPOSITORY
                / "deploy/autodl/saor_project_mechanism_formal_contract.json"
            )
            contract_path.write_text(json.dumps(contract), encoding="utf-8")
            (root / "project_mechanism_contract.json").write_text(
                json.dumps(
                    {
                        "contract_sha256": project_formal_sha256(contract_path),
                        "contract": contract,
                        "readiness": {
                            "status": "passed",
                            "work_cost_calibration_identity": {
                                key: value
                                for key, value in contract[
                                    "work_cost_contract"
                                ]["calibration_identity"].items()
                                if key not in {"model_id", "tokenizer_id"}
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            (root / "manifest.json").write_text(
                json.dumps(
                    {
                        "status": "completed",
                        "execution_mode": "rehearsal",
                        "incidents": [],
                    }
                ),
                encoding="utf-8",
            )
            self._write_group_rows(
                root / "group_runs.csv",
                [
                    {"scenario_id": scenario_id}
                    for scenario_id in PROJECT_FORMAL_SCENARIOS
                ],
            )

            def fake_cell(_root, row, **_kwargs):
                return (
                    {
                        "scenario_id": row["scenario_id"],
                        "foreground_slo_violation": 0.0,
                        "completion_longest_no_service_s": 10.0,
                        "bounded_saor_debt_repayment_p95_s": 10.0,
                        "bounded_saor_debt_repayment_unresolved": 0,
                        "bounded_saor_debt_repayment_completed": 1,
                        "bounded_saor_projection_violation_events": 0,
                        "bounded_saor_projection_estimation_overrun_events": 0,
                    },
                    [],
                )

            with (
                patch.object(
                    PROJECT_FORMAL_SUMMARY,
                    "_cell_metrics",
                    side_effect=fake_cell,
                ),
                patch.object(
                    PROJECT_FORMAL_SUMMARY,
                    "audit_work_cost_matrix",
                    return_value={
                        "status": "passed",
                        "input_files_manifest_sha256": "a" * 64,
                    },
                ),
            ):
                result = PROJECT_FORMAL_SUMMARY.validate_rehearsal_root(
                    root,
                    contract_path,
                )

            self.assertEqual(result["status"], "passed")
            self.assertFalse(result["formal_authorized"])
            self.assertFalse(result["performance_ranking_decided"])
            self.assertTrue((root / "rehearsal_validation.json").is_file())

    def test_project_formal_summary_keeps_static_fairness_not_applicable(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrix = root / "matrix"
            contract_path = root / "contract.json"
            contract = load_project_formal_contract(
                REPOSITORY
                / "deploy/autodl/saor_project_mechanism_formal_contract.json"
            )
            contract["status"] = "formal_ready"
            contract["formal_authorized"] = True
            contract["rehearsal_validation"] = dict(
                REVIEWED_REHEARSAL_EVIDENCE
            )
            contract_path.write_text(
                json.dumps(contract),
                encoding="utf-8",
            )
            self._write_project_formal_matrix(
                matrix,
                contract,
                project_formal_sha256(contract_path),
            )

            with patch.object(
                PROJECT_FORMAL_SUMMARY,
                "audit_work_cost_matrix",
                return_value={"status": "passed"},
            ):
                result = PROJECT_FORMAL_SUMMARY.summarize(
                    matrix,
                    contract_path,
                    root / "summary",
                )

            self.assertTrue(result["evidence_valid"])
            self.assertTrue(result["claim_gate_passed"])
            formal_rows = list(
                csv.DictReader(
                    (root / "summary/formal_runs.csv").read_text(
                        encoding="utf-8"
                    ).splitlines()
                )
            )
            static = [
                row for row in formal_rows
                if row["scenario_id"]
                == "active_set_project_frozen_static"
            ]
            self.assertEqual(len(static), 3)
            self.assertTrue(
                all(
                    row["fairness_evidence_applicable"] == "False"
                    and row["fairness_gate_passed"] == "True"
                    for row in static
                )
            )

    def test_project_formal_decision_separates_validity_from_claim(self) -> None:
        contract = load_project_formal_contract(
            REPOSITORY
            / "deploy/autodl/saor_project_mechanism_formal_contract.json"
        )
        decision = contract["decision_contract"]
        metrics = []
        for scenario_id, (policy, _observation) in PROJECT_FORMAL_SCENARIOS.items():
            for repeat in range(1, 4):
                row = {
                    "scenario_id": scenario_id,
                    "policy": policy,
                    "repeat_index": repeat,
                    "tokens_per_s": 100.0,
                    "bulk_jct_s": 100.0,
                    "foreground_p99_s": 20.0,
                    "bulk_slo_violation": 0.5,
                    "foreground_slo_violation": 0.0,
                    "completion_service_lag_p95_work": 100.0,
                    "completion_longest_no_service_s": 10.0,
                    "bounded_saor_debt_repayment_p95_s": 10.0,
                    "bounded_saor_debt_repayment_unresolved": 0,
                    "bounded_saor_debt_repayment_completed": (
                        1
                        if scenario_id == PROJECT_FORMAL_PROPOSED
                        else 0
                    ),
                    "bounded_saor_projection_violation_events": 0,
                    "bounded_saor_projected_overshoot_bound_violation_events": 0,
                    "bounded_saor_projection_estimation_overrun_events": 0,
                    "bounded_saor_recovery_estimation_overrun_events": 0,
                }
                if scenario_id == PROJECT_FORMAL_PROPOSED:
                    row["tokens_per_s"] = 96.0
                    row["bulk_jct_s"] = 104.0
                    row["foreground_p99_s"] = 18.0
                    row["completion_service_lag_p95_work"] = 94.0
                metrics.append(row)

        passed, _paired = PROJECT_FORMAL_SUMMARY.evaluate_decision(
            metrics,
            decision,
        )
        self.assertTrue(passed["claim_gate_passed"])

        for row in metrics:
            if row["scenario_id"] == PROJECT_FORMAL_PROPOSED:
                row["tokens_per_s"] = 90.0
        failed, _paired = PROJECT_FORMAL_SUMMARY.evaluate_decision(
            metrics,
            decision,
        )
        self.assertFalse(failed["claim_gate_passed"])
        self.assertFalse(failed["throughput_noninferior"])

        for row in metrics:
            if row["scenario_id"] == PROJECT_FORMAL_PROPOSED:
                row["tokens_per_s"] = 96.0
                row["foreground_slo_violation"] = 0.02
                row["completion_longest_no_service_s"] = 31.0
        unsafe, _paired = PROJECT_FORMAL_SUMMARY.evaluate_decision(
            metrics,
            decision,
        )
        self.assertFalse(unsafe["claim_gate_passed"])
        self.assertFalse(unsafe["foreground_slo_satisfied"])
        self.assertFalse(unsafe["longest_no_service_absolute"])
        proposed = next(
            row
            for row in metrics
            if row["scenario_id"] == PROJECT_FORMAL_PROPOSED
        )
        rehearsal_safety = PROJECT_FORMAL_SUMMARY.rehearsal_safety(
            proposed,
            decision,
        )
        self.assertFalse(rehearsal_safety["foreground_slo_satisfied"])
        self.assertFalse(rehearsal_safety["longest_no_service_absolute"])

    def test_ready_observation_bridge_separates_the_two_effects(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrix = root / "round-1"
            self._write_ready_observation_bridge(matrix)

            result = OBSERVATION_BRIDGE_SUMMARY.summarize(
                (matrix,), root / "summary"
            )

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["native_baseline_count"], 0)
            self.assertFalse(result["shared_capacity_effect_decided"])
            self.assertFalse(result["ready_observation_effect_decided"])
            self.assertFalse(result["formal_authorized"])
            with (root / "summary/bridge_effects.csv").open(
                encoding="utf-8"
            ) as handle:
                effects = list(csv.DictReader(handle))
            self.assertEqual(
                [row["effect"] for row in effects],
                ["shared_capacity", "bounded_ready_observation"],
            )
            metrics = list(
                csv.DictReader(
                    (root / "summary/bridge_metrics.csv").read_text(
                        encoding="utf-8"
                    ).splitlines()
                )
            )
            self.assertEqual(
                metrics[0]["completion_service_lag_status"],
                "unavailable:requires_complete_registered_ready_ledger",
            )
            self.assertEqual(metrics[0]["completion_service_lag_p95_work"], "")

    def test_matched_ready_summary_preserves_internal_ablation_identity(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrix = root / "round-1"
            self._write_matched_ready_matrix(matrix)

            result = MATCHED_READY_SUMMARY.summarize(
                (matrix,), root / "summary"
            )

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["native_baseline_count"], 0)
            self.assertEqual(
                result["evaluation_scope"], "single_tenant_multi_job"
            )
            self.assertEqual(
                result["fairness_mode"], "differentiated_service"
            )
            self.assertFalse(result["selector_victory_decided"])
            self.assertFalse(result["formal_authorized"])
            with (root / "summary/ablation_metrics.csv").open(
                encoding="utf-8"
            ) as handle:
                metrics = list(csv.DictReader(handle))
            static = next(
                row for row in metrics
                if row["scenario_id"]
                == "active_set_project_frozen_static"
            )
            self.assertEqual(static["fairness_evidence_applicable"], "False")
            self.assertEqual(static["fairness_evidence_passed"], "False")
            self.assertEqual(static["fairness_gate_passed"], "True")
            self.assertEqual(static["cell_evidence_passed"], "True")
            bounded = [
                row for row in metrics
                if row["fairness_evidence_applicable"] == "True"
            ]
            self.assertEqual(len(bounded), 5)
            self.assertTrue(
                all(row["fairness_evidence_passed"] == "True" for row in bounded)
            )

    def test_matched_ready_summary_fails_without_completion_fairness(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrix = root / "round-1"
            self._write_matched_ready_matrix(
                matrix,
                include_fairness=False,
            )

            with self.assertRaisesRegex(ValueError, "completion-fairness"):
                MATCHED_READY_SUMMARY.summarize(
                    (matrix,), root / "summary"
                )

            validation = json.loads(
                (root / "summary/validation.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(validation["status"], "failed")

    def test_bounded_gate_uses_lossless_events_not_sampled_snapshots(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrices = (root / "round-1", root / "round-2")
            for matrix in matrices:
                self._write_bounded_matrix(matrix)

            result = BOUNDED_SUMMARY.summarize(matrices, root / "summary")

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["conclusion"], "formal_registration_candidate")
            self.assertTrue((root / "summary/gate_summary.csv").is_file())
            self.assertTrue((root / "summary/mechanism_summary.csv").is_file())

    def test_bounded_ready_gate_has_a_distinct_frozen_profile(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrices = (root / "round-1", root / "round-2")
            for matrix in matrices:
                self._write_bounded_matrix(matrix, ready_policy=True)

            result = BOUNDED_SUMMARY.summarize(
                matrices,
                root / "summary",
                profile="bounded_ready",
            )

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["profile"], "bounded_ready")

    def test_bounded_gate_fails_closed_without_event_ledger(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrices = (root / "round-1", root / "round-2")
            for matrix in matrices:
                self._write_bounded_matrix(matrix)
            next((matrices[0] / "traces").glob("*0125k*.release_events.csv")).unlink()

            with self.assertRaisesRegex(ValueError, "event ledger"):
                BOUNDED_SUMMARY.summarize(matrices, root / "summary")

            validation = json.loads(
                (root / "summary/validation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(validation["status"], "failed")

    def test_bounded_gate_fails_closed_on_event_sequence_gap(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrices = (root / "round-1", root / "round-2")
            for matrix in matrices:
                self._write_bounded_matrix(matrix)
            event_path = next(
                (matrices[0] / "traces").glob("*0125k*.release_events.csv")
            )
            with event_path.open(encoding="utf-8") as handle:
                events = list(csv.DictReader(handle))
            events[1]["event_seq"] = "3"
            self._write_group_rows(event_path, events)

            with self.assertRaisesRegex(ValueError, "gap, duplicate, or empty"):
                BOUNDED_SUMMARY.summarize(matrices, root / "summary")

            validation = json.loads(
                (root / "summary/validation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(validation["status"], "failed")

    def test_legacy_bounded_gate_counts_recovery_requests_and_work(self) -> None:
        summary = BOUNDED_SUMMARY._event_summary(
            [
                {
                    "event_seq": "1",
                    "endpoint_id": "endpoint-0",
                    "action": "grant",
                    "tier": "debt_recovery",
                    "recovery_inflight_by_job": '[ ["bulk", ["r1", "r2"]] ]',
                    "recovery_inflight_work_by_job": '[["bulk", 160]]',
                    "avoidable_idle": "False",
                    "foreign_grant_over_debt_critical": "False",
                }
            ]
        )

        self.assertEqual(summary["recovery_inflight_requests_max"], 2)
        self.assertEqual(summary["recovery_inflight_work_max"], 160.0)

    def test_priority_reachability_summary_passes_only_with_audited_action(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "matrix"
            output = Path(directory) / "summary"
            root.mkdir()
            self._write_clean_manifest(root)
            rows = self._priority_matrix_rows(priority_p99_s=30.0)
            self._write_group_rows(root / "group_runs.csv", rows)

            result = PRIORITY_SUMMARY.summarize(root, output)

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["release_only_reachability"], "passed")
            self.assertEqual(result["strict_priority_job_priorities"], [0, 1])
            self.assertTrue((output / "reachability_summary.csv").is_file())

    def test_priority_reachability_summary_fails_closed_on_tail_limit(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "matrix"
            output = Path(directory) / "summary"
            root.mkdir()
            self._write_clean_manifest(root)
            rows = self._priority_matrix_rows(priority_p99_s=31.0)
            self._write_group_rows(root / "group_runs.csv", rows)

            with self.assertRaisesRegex(ValueError, "foreground P99"):
                PRIORITY_SUMMARY.summarize(root, output)

            result = json.loads(
                (output / "validation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["release_only_reachability"], "failed")

    def test_legacy_near_simultaneous_drain_is_reclassified(self) -> None:
        row = {
            "active_set_mechanism_applicable": "True",
            "active_set_mechanism_passed": "False",
            "active_set_mechanism_status": "active_set_mechanism_not_observed",
            "active_set_lifecycle_passed": "True",
            "active_set_overlap_reclaim_observed": "True",
            "active_set_pre_bulk_dominant_share_max": "0.95",
            "active_set_bulk_only_post_samples": "0",
            "active_set_post_fit_violation_samples": "0",
            "arrival_offsets_s": "[0.0, 5.0]",
            "job_jct_s": "[68.743800, 63.737972]",
        }

        passed, status = SUMMARY._effective_mechanism_gate(row)

        self.assertTrue(passed)
        self.assertEqual(
            status,
            "reclassified:post_drain_below_trace_resolution",
        )

    def test_legacy_resolvable_drain_stays_failed(self) -> None:
        row = {
            "active_set_mechanism_applicable": "True",
            "active_set_mechanism_passed": "False",
            "active_set_mechanism_status": "active_set_mechanism_not_observed",
            "active_set_lifecycle_passed": "True",
            "active_set_overlap_reclaim_observed": "True",
            "active_set_pre_bulk_dominant_share_max": "0.95",
            "active_set_bulk_only_post_samples": "0",
            "active_set_post_fit_violation_samples": "0",
            "arrival_offsets_s": "[0.0, 5.0]",
            "job_jct_s": "[70.0, 63.0]",
        }

        passed, _ = SUMMARY._effective_mechanism_gate(row)

        self.assertFalse(passed)

    def test_new_schema_failure_cannot_use_legacy_reclassification(self) -> None:
        row = {
            "active_set_mechanism_applicable": "True",
            "active_set_mechanism_passed": "False",
            "active_set_mechanism_status": "active_set_mechanism_not_observed",
            "active_set_post_drain_applicable": "True",
            "active_set_lifecycle_passed": "True",
            "active_set_overlap_reclaim_observed": "True",
            "active_set_pre_bulk_dominant_share_max": "0.95",
            "active_set_bulk_only_post_samples": "0",
            "active_set_post_fit_violation_samples": "0",
            "arrival_offsets_s": "[0.0, 5.0]",
            "job_jct_s": "[68.743800, 63.737972]",
        }

        passed, _ = SUMMARY._effective_mechanism_gate(row)

        self.assertFalse(passed)

    def test_compact_mechanism_replay_is_explicitly_not_full_validation(
        self,
    ) -> None:
        matrix = (
            REPOSITORY
            / "experiments/results/"
            "saor_active_set_release_formal_20260812_69affc7e"
        )
        with TemporaryDirectory() as directory:
            payload = SUMMARY.replay_compact_mechanism_gate(
                matrix,
                Path(directory),
            )

        self.assertEqual(payload["status"], "passed")
        self.assertFalse(payload["full_formal_validation_updated"])
        self.assertEqual(len(payload["results"]), 12)
        self.assertEqual(
            sum(
                item["effective_mechanism_status"].startswith("reclassified:")
                for item in payload["results"]
            ),
            2,
        )

    def test_default_summary_declares_resolution_aware_full_validation(
        self,
    ) -> None:
        matrix = (
            REPOSITORY
            / "experiments/results/"
            "saor_active_set_release_formal_20260812_69affc7e"
        )
        with TemporaryDirectory() as directory:
            root = Path(directory) / "matrix"
            output = Path(directory) / "summary"
            root.mkdir()
            self._write_clean_manifest(root)
            (root / "group_runs.csv").write_bytes(
                (matrix / "group_runs.csv").read_bytes()
            )

            SUMMARY.summarize(root, output)

            validation = json.loads(
                (output / "validation.json").read_text(encoding="utf-8")
            )

        self.assertEqual(validation["status"], "passed")
        self.assertTrue(validation["full_formal_validation_updated"])
        self.assertEqual(
            validation["mechanism_gate_evaluation"],
            "resolution_aware_v2",
        )
        self.assertEqual(validation["trace_observation_interval_s"], 0.25)
        self.assertEqual(len(validation["mechanism_reclassifications"]), 2)

    def test_repository_formal_env_covers_template_contract(self) -> None:
        template_paths = (
            "deploy/autodl/saor_active_set_release.example.json",
            "deploy/autodl/saor_project_mechanism_formal.example.json",
        )
        env_example = (
            REPOSITORY / "deploy/autodl/saor_active_set_formal.env.example"
        ).read_text(encoding="utf-8")
        provided = set(
            re.findall(r"^export ([A-Z][A-Z0-9_]*)=", env_example, re.MULTILINE)
        )
        for relative in template_paths:
            template = (REPOSITORY / relative).read_text(encoding="utf-8")
            required = set(
                re.findall(r"\$\{([A-Z][A-Z0-9_]*)\}", template)
            )
            self.assertEqual(required - provided, {"DATABASE_URL"})
        self.assertIn("export COMPLETION_PROTOCOL=chat_completions", env_example)
        self.assertIn("export COMPLETION_PROMPT_TOKEN_OVERHEAD=29", env_example)
        self.assertIn("/v1/chat/completions", env_example)
        self.assertIn("export SAOR_ARRIVAL_TIME_SCALE=0.0001", env_example)
        self.assertIn(
            "export SAOR_ACTIVE_SET_WORKLOAD=sharegpt_multiturn", env_example
        )

    def test_priority_reachability_template_uses_formal_environment(self) -> None:
        template = (
            REPOSITORY
            / "deploy/autodl/saor_priority_reachability.example.json"
        ).read_text(encoding="utf-8")
        env_example = (
            REPOSITORY / "deploy/autodl/saor_active_set_formal.env.example"
        ).read_text(encoding="utf-8")
        required = set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)\}", template))
        provided = set(
            re.findall(r"^export ([A-Z][A-Z0-9_]*)=", env_example, re.MULTILINE)
        )
        decoded = json.loads(template)

        self.assertEqual(required - provided, {"DATABASE_URL"})
        self.assertEqual(
            [item["policy"] for item in decoded["scenarios"]],
            [
                "static_partition",
                "saor_release",
                "foreground_strict_priority",
            ],
        )

    def test_direct_control_emits_project_compatible_job_evidence(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "jobs").mkdir()
            manifests = (root / "bulk.jsonl", root / "foreground.jsonl")
            requests = (
                (self._request(1, 0), self._request(2, 1)),
                (self._request(3, 0), self._request(4, 1)),
            )
            for path, rows in zip(manifests, requests):
                write_manifest(path, rows)
            scenario = SharedVllmScenario(
                scenario_id="direct",
                policy="direct_no_job",
                job_count=2,
                rows_per_job=None,
                rows_per_jobs=(2, 2),
                weights=(1, 1),
                arrival_offsets_s=(0.0, 5.0),
                request_manifests=tuple(str(path) for path in manifests),
            )
            config = SharedVllmConfig(
                experiment_id="direct-test",
                seed=1,
                warmup_runs_per_scenario=0,
                formal_repeats=1,
                endpoint_ids=("endpoint-0", "endpoint-1"),
                service_signature=(("model", "qwen"), ("service", "vllm-test")),
                request_limit_per_endpoint=128,
                work_limit_per_endpoint=65536,
                credit_quantum=2048,
                shared_credit_namespace="test",
                gpu_peak_tflops=165.0,
                mfu_precision="bf16",
                common_args=(
                    "--arrival-replay",
                    "--completion-endpoint-urls",
                    "http://127.0.0.1:8000/v1/completions,"
                    "http://127.0.0.1:8001/v1/completions",
                    "--completion-model",
                    "qwen",
                    "--completion-protocol",
                    "completions",
                    "--completion-prompt-format",
                    "raw",
                    "--completion-temperature",
                    "0",
                    "--request-slo-ms",
                    "30000",
                ),
                scenarios=(scenario,),
                service_metadata=(),
            )

            async def fake_jobs(jobs, _contract):
                return {
                    job.job_id: tuple(
                        BaselineRequestResult(
                            doc_id=request.doc_id,
                            endpoint_index=request.endpoint_index,
                            status="completed",
                            error=None,
                            submitted_at_s=(
                                100.0 + job.arrival_offset_s + index * 0.1
                            ),
                            started_at_s=(
                                100.0 + job.arrival_offset_s + index * 0.1
                            ),
                            completed_at_s=(
                                101.0 + job.arrival_offset_s + index * 0.1
                            ),
                            input_tokens=4,
                            output_tokens=8,
                            output_text="ok",
                            finish_reason="length",
                        )
                        for index, request in enumerate(job.requests)
                    )
                    for job in jobs
                }

            with patch(
                "src.experiments.shared_vllm.direct_control.run_bounded_http_jobs",
                side_effect=fake_jobs,
            ):
                evidence = run_direct_control(
                    config,
                    scenario,
                    start_epoch_s=100.0,
                    output_dir=root,
                    run_stem="run",
                )

            self.assertEqual(len(evidence), 2)
            self.assertEqual(evidence[0]["actual_work"], 24)
            self.assertEqual(evidence[0]["expected_count"], 2)
            self.assertEqual(evidence[0]["completed_count"], 2)
            self.assertTrue(evidence[0]["exactly_once"])
            self.assertEqual(
                evidence[1]["replay_configured_start_epoch_s"], 105.0
            )
            self.assertEqual(
                evidence[0]["endpoint_counts"],
                {"endpoint-0": 1, "endpoint-1": 1},
            )
            self.assertTrue((root / "jobs/run_job0.requests.csv").is_file())

    def test_repository_formal_template_passes_static_readiness(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            bulk = root / "bulk.jsonl"
            foreground = root / "foreground.jsonl"
            write_manifest(
                bulk,
                (
                    self._request(1, 0),
                    self._request(2, 1),
                    # Exactly at the foreground boundary: this request cannot
                    # establish capacity before the second Job becomes active.
                    self._request(5, 0, arrival_time_s=5001.0),
                ),
            )
            write_manifest(
                foreground,
                (
                    self._request(3, 0),
                    self._request(4, 1),
                ),
            )
            selection = root / "selection.json"
            selection.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "ready",
                        "selection": {
                            "best_token_budget": 8192,
                            "project_static_k_per_endpoint": 128,
                            "project_active_work_per_endpoint": 65536,
                            "project_actor_workers_per_endpoint": 8,
                            "project_ray_actor_max_concurrency": 32,
                            "project_ray_worker_num_cpus": 0.25,
                        },
                        "evidence": {
                            "feeding": {"status": "passed"},
                            "token_budget": {
                                "status": "passed",
                                "frozen_token_budget": 8192,
                            },
                            "actor_pool": {"status": "passed"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            environment = {
                "VLLM_VERSION": "0.25.1",
                "VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
                "VLLM_MAX_NUM_SEQS": "256",
                "VLLM_GPU_MEMORY_UTILIZATION": "0.9",
                "STRATEGY_CALIBRATION_SELECTION": str(selection),
                "BEST_TOKEN_BUDGET": "8192",
                "PROJECT_STATIC_K_PER_ENDPOINT": "128",
                "PROJECT_ACTIVE_WORK_PER_ENDPOINT": "65536",
                "PROJECT_ACTOR_WORKERS_PER_ENDPOINT": "8",
                "PROJECT_RAY_ACTOR_MAX_CONCURRENCY": "32",
                "PROJECT_RAY_WORKER_NUM_CPUS": "0.25",
                "PROJECT_SHARED_CREDIT_QUANTUM": "2048",
                "DATABASE_URL": "postgresql://postgres:postgres@localhost/db",
                "SOURCE_MAX_PROMPT_TOKENS": "1500",
                "COMPLETION_ENDPOINT_URLS": (
                    "http://127.0.0.1:8000/v1/chat/completions,"
                    "http://127.0.0.1:8001/v1/chat/completions"
                ),
                "COMPLETION_MODEL": "qwen",
                "COMPLETION_HTTP_KEEPALIVE_EXPIRY_S": "4",
                "COMPLETION_PROTOCOL": "chat_completions",
                "COMPLETION_PROMPT_TOKEN_OVERHEAD": "29",
                "COMPLETION_MAX_TOKENS": "8",
                "MODEL_METRICS_URLS": (
                    "http://127.0.0.1:8000/metrics,"
                    "http://127.0.0.1:8001/metrics"
                ),
                "ENDPOINT_GPU_IDS": "0,1",
                "SAOR_ACTIVE_SET_WORKLOAD": "saor-test",
                "MODEL_PATH": "/models/qwen",
                "REQUEST_SLO_MS": "30000",
                "GPU_PEAK_TFLOPS": "165",
                "MFU_PRECISION": "bf16_dense_fp32_accumulate",
                "SAOR_BULK_ROWS": "3",
                "SAOR_FOREGROUND_ROWS": "2",
                "SAOR_FOREGROUND_OFFSET_S": "5",
                "SAOR_ARRIVAL_TIME_SCALE": "0.001",
                "SAOR_MAX_EFFECTIVE_MANIFEST_SPAN_S": "120",
                "SAOR_MIN_PRE_FOREGROUND_WORK_ENVELOPES": "0.0001",
                "SAOR_READY_PAYLOAD_BYTES_LIMIT_PER_JOB": "67108864",
                "SAOR_BULK_MANIFEST": str(bulk),
                "SAOR_FOREGROUND_MANIFEST": str(foreground),
            }
            with patch.dict(os.environ, environment, clear=True):
                result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_active_set_release.example.json"
                )
                priority_result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_priority_reachability.example.json",
                    profile="priority_reachability",
                )
                bounded_result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_bounded_priority.example.json",
                    profile="bounded_priority_development",
                )
                bounded_ready_result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_bounded_ready.example.json",
                    profile="bounded_ready_development",
                )
                matched_ready_result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_matched_ready_selector_ablation.example.json",
                    profile="matched_ready_selector_ablation",
                )
                project_formal_path = (
                    REPOSITORY
                    / "deploy/autodl/saor_project_mechanism_formal.example.json"
                )
                project_formal_result = AUDIT.audit(
                    project_formal_path,
                    profile="matched_ready_selector_ablation",
                )
                with patch.dict(
                    os.environ,
                    {"COMPLETION_MAX_TOKENS": "256"},
                ):
                    project_formal_config = load_config(project_formal_path)
                    feeding_ceiling_config = load_config(
                        REPOSITORY
                        / "deploy/autodl/saor_project_feeding_ceiling.example.json"
                    )
                project_formal_contract = load_project_formal_contract(
                    REPOSITORY
                    / "deploy/autodl/saor_project_mechanism_formal_contract.json"
                )
                project_contract_errors = validate_project_formal_contract(
                    project_formal_contract,
                    project_formal_config,
                    formal_run=False,
                )
                drifted_work_cost_contract = dict(project_formal_contract)
                drifted_work_cost_contract["work_cost_contract"] = dict(
                    project_formal_contract["work_cost_contract"]
                )
                drifted_work_cost_contract["work_cost_contract"][
                    "prompt_token_overhead_per_request"
                ] = 28
                project_work_cost_errors = validate_project_formal_contract(
                    drifted_work_cost_contract,
                    project_formal_config,
                    formal_run=False,
                )
                drifted_output_bound_contract = json.loads(
                    json.dumps(project_formal_contract)
                )
                drifted_output_bound_contract["work_cost_contract"][
                    "output_bound_source"
                ] = "trace_target_output"
                project_output_bound_errors = validate_project_formal_contract(
                    drifted_output_bound_contract,
                    project_formal_config,
                    formal_run=False,
                )
                drifted_output_cap_contract = json.loads(
                    json.dumps(project_formal_contract)
                )
                drifted_output_cap_contract["work_cost_contract"][
                    "completion_max_tokens"
                ] = 257
                project_output_cap_errors = validate_project_formal_contract(
                    drifted_output_cap_contract,
                    project_formal_config,
                    formal_run=False,
                )
                drifted_evidence_contract = json.loads(
                    json.dumps(project_formal_contract)
                )
                drifted_evidence_contract["work_cost_contract"][
                    "calibration_evidence"
                ]["predecessor_failed_archive_sha256"] = "not-a-sha"
                project_evidence_errors = validate_project_formal_contract(
                    drifted_evidence_contract,
                    project_formal_config,
                    formal_run=False,
                )
                project_formal_lock_errors = validate_project_formal_contract(
                    project_formal_contract,
                    project_formal_config,
                    formal_run=True,
                )
                feeding_config_errors = validate_ceiling_config(
                    project_formal_config,
                    feeding_ceiling_config,
                )
                drifted_feeding_config = replace(
                    feeding_ceiling_config,
                    common_args=(
                        *feeding_ceiling_config.common_args,
                        "--unexpected-drift",
                    ),
                )
                feeding_drift_errors = validate_ceiling_config(
                    project_formal_config,
                    drifted_feeding_config,
                )
                authorized_contract = json.loads(
                    json.dumps(project_formal_contract)
                )
                authorized_contract["status"] = "formal_ready"
                authorized_contract["formal_authorized"] = True
                project_authorized_errors = validate_project_formal_contract(
                    authorized_contract,
                    project_formal_config,
                    formal_run=True,
                )
                authorization_drift_errors = {}
                for field, replacement in {
                    "status": "passed_pending_independent_review",
                    "repository_commit": "0" * 40,
                    "root_id": "different-root",
                    "validation_sha256": "0" * 64,
                    "archive_sha256": "1" * 64,
                    "performance_ranking_decided": True,
                    "valid_rehearsal": False,
                }.items():
                    drifted_authorization = json.loads(
                        json.dumps(authorized_contract)
                    )
                    drifted_authorization["rehearsal_validation"][
                        field
                    ] = replacement
                    authorization_drift_errors[field] = (
                        validate_project_formal_contract(
                            drifted_authorization,
                            project_formal_config,
                            formal_run=True,
                        )
                    )
                observation_bridge_result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_ready_observation_bridge.example.json",
                    profile="ready_observation_bridge",
                )
                drift_config = root / "matched-ready-drift.json"
                drift_payload = json.loads(
                    (
                        REPOSITORY
                        / "deploy/autodl/saor_matched_ready_selector_ablation.example.json"
                    ).read_text(encoding="utf-8")
                )
                drift_payload["scenarios"][1][
                    "request_limit_per_endpoint"
                ] = 64
                drift_payload["scenarios"][1][
                    "work_limit_per_endpoint"
                ] = 32768
                drift_payload["scenarios"][2]["weights"] = [1, 2]
                drift_config.write_text(
                    json.dumps(drift_payload),
                    encoding="utf-8",
                )
                drift_result = AUDIT.audit(
                    drift_config,
                    profile="matched_ready_selector_ablation",
                )
            environment["SAOR_MIN_PRE_FOREGROUND_WORK_ENVELOPES"] = "1"
            with patch.dict(os.environ, environment, clear=True):
                insufficient_supply = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_active_set_release.example.json"
                )

        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["scenario_count"], 10)
        self.assertEqual(priority_result["status"], "passed")
        self.assertEqual(priority_result["scenario_count"], 3)
        self.assertEqual(bounded_result["status"], "passed")
        self.assertEqual(bounded_result["scenario_count"], 4)
        self.assertEqual(bounded_ready_result["status"], "passed")
        self.assertEqual(bounded_ready_result["scenario_count"], 4)
        self.assertEqual(matched_ready_result["status"], "passed")
        self.assertEqual(matched_ready_result["scenario_count"], 6)
        self.assertEqual(project_formal_result["status"], "passed")
        self.assertEqual(project_contract_errors, [])
        self.assertTrue(
            any("work-cost" in error for error in project_work_cost_errors)
        )
        self.assertTrue(
            any("output bound" in error for error in project_output_bound_errors)
        )
        self.assertTrue(
            any("completion_max_tokens" in error for error in project_output_cap_errors)
        )
        self.assertIn(
            "predecessor calibration archive SHA is invalid",
            project_evidence_errors,
        )
        self.assertEqual(
            project_formal_lock_errors,
            ["formal run is not authorized by the frozen contract"],
        )
        self.assertEqual(project_authorized_errors, [])
        self.assertEqual(feeding_config_errors, [])
        self.assertIn(
            "feeding ceiling common_args drifted",
            feeding_drift_errors,
        )
        for field, errors in authorization_drift_errors.items():
            self.assertIn(
                f"formal rehearsal evidence {field} drifted",
                errors,
            )
        self.assertEqual(observation_bridge_result["status"], "passed")
        self.assertEqual(observation_bridge_result["scenario_count"], 3)
        self.assertEqual(drift_result["status"], "failed")
        self.assertTrue(
            any(
                "request/work limits" in error
                for error in drift_result["errors"]
            )
        )
        self.assertTrue(
            any("weights drift" in error for error in drift_result["errors"])
        )
        self.assertIsNone(priority_result["direct_contract"])
        self.assertEqual(
            result["direct_contract"]["protocol"],
            "chat_completions",
        )
        self.assertEqual(result["direct_contract"]["prompt_format"], "raw")
        self.assertEqual(result["direct_contract"]["keepalive_expiry_s"], 4.0)
        self.assertEqual(
            result["pre_foreground_predicted_work_by_endpoint"],
            {"endpoint-0": 12, "endpoint-1": 12},
        )
        self.assertEqual(insufficient_supply["status"], "failed")
        self.assertTrue(
            any(
                "pre-foreground predicted work" in error
                for error in insufficient_supply["errors"]
            )
        )

    def test_readiness_rejects_token_budget_not_bound_to_evidence(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            bulk = root / "bulk.jsonl"
            foreground = root / "foreground.jsonl"
            write_manifest(bulk, (self._request(1, 0), self._request(2, 1)))
            write_manifest(
                foreground,
                (self._request(3, 0), self._request(4, 1)),
            )
            selection = root / "selection.json"
            selection.write_text(
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
                            "token_budget": {
                                "status": "passed",
                                "frozen_token_budget": 4096,
                            },
                            "actor_pool": {"status": "passed"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            environment = {
                "VLLM_VERSION": "0.25.1",
                "VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
                "VLLM_MAX_NUM_SEQS": "256",
                "VLLM_GPU_MEMORY_UTILIZATION": "0.9",
                "STRATEGY_CALIBRATION_SELECTION": str(selection),
                "BEST_TOKEN_BUDGET": "8192",
                "PROJECT_STATIC_K_PER_ENDPOINT": "128",
                "PROJECT_ACTIVE_WORK_PER_ENDPOINT": "65536",
                "PROJECT_ACTOR_WORKERS_PER_ENDPOINT": "8",
                "PROJECT_RAY_ACTOR_MAX_CONCURRENCY": "32",
                "PROJECT_RAY_WORKER_NUM_CPUS": "0.25",
                "PROJECT_SHARED_CREDIT_QUANTUM": "2048",
                "DATABASE_URL": "postgresql://postgres:postgres@localhost/db",
                "SOURCE_MAX_PROMPT_TOKENS": "1500",
                "COMPLETION_ENDPOINT_URLS": (
                    "http://127.0.0.1:8000/v1/completions,"
                    "http://127.0.0.1:8001/v1/completions"
                ),
                "COMPLETION_MODEL": "qwen",
                "COMPLETION_HTTP_KEEPALIVE_EXPIRY_S": "4",
                "COMPLETION_PROTOCOL": "completions",
                "COMPLETION_MAX_TOKENS": "8",
                "MODEL_METRICS_URLS": (
                    "http://127.0.0.1:8000/metrics,"
                    "http://127.0.0.1:8001/metrics"
                ),
                "ENDPOINT_GPU_IDS": "0,1",
                "SAOR_ACTIVE_SET_WORKLOAD": "saor-test",
                "MODEL_PATH": "/models/qwen",
                "REQUEST_SLO_MS": "30000",
                "GPU_PEAK_TFLOPS": "165",
                "MFU_PRECISION": "bf16_dense_fp32_accumulate",
                "SAOR_BULK_ROWS": "2",
                "SAOR_FOREGROUND_ROWS": "2",
                "SAOR_FOREGROUND_OFFSET_S": "5",
                "SAOR_ARRIVAL_TIME_SCALE": "0.001",
                "SAOR_MAX_EFFECTIVE_MANIFEST_SPAN_S": "120",
                "SAOR_MIN_PRE_FOREGROUND_WORK_ENVELOPES": "0.0001",
                "SAOR_BULK_MANIFEST": str(bulk),
                "SAOR_FOREGROUND_MANIFEST": str(foreground),
            }
            with patch.dict(os.environ, environment, clear=True):
                result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_active_set_release.example.json"
                )

        self.assertEqual(result["status"], "failed")
        self.assertIn(
            "configured token budget does not match calibration evidence",
            result["errors"],
        )

    def test_readiness_rejects_unscaled_multi_hour_manifest(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            bulk = root / "bulk.jsonl"
            foreground = root / "foreground.jsonl"
            write_manifest(
                bulk,
                (
                    self._request(1, 0, arrival_time_s=0),
                    self._request(2, 1, arrival_time_s=66_000),
                ),
            )
            write_manifest(
                foreground,
                (
                    self._request(3, 0, arrival_time_s=0),
                    self._request(4, 1, arrival_time_s=66_000),
                ),
            )
            selection = root / "selection.json"
            selection.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "ready",
                        "selection": {
                            "best_token_budget": 8192,
                            "project_static_k_per_endpoint": 128,
                            "project_active_work_per_endpoint": 65536,
                            "project_actor_workers_per_endpoint": 8,
                            "project_ray_actor_max_concurrency": 32,
                            "project_ray_worker_num_cpus": 0.25,
                        },
                        "evidence": {
                            "feeding": {"status": "passed"},
                            "token_budget": {
                                "status": "passed",
                                "frozen_token_budget": 8192,
                            },
                            "actor_pool": {"status": "passed"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            environment = {
                "VLLM_VERSION": "0.25.1",
                "VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
                "VLLM_MAX_NUM_SEQS": "256",
                "VLLM_GPU_MEMORY_UTILIZATION": "0.9",
                "STRATEGY_CALIBRATION_SELECTION": str(selection),
                "BEST_TOKEN_BUDGET": "8192",
                "PROJECT_STATIC_K_PER_ENDPOINT": "128",
                "PROJECT_ACTIVE_WORK_PER_ENDPOINT": "65536",
                "PROJECT_ACTOR_WORKERS_PER_ENDPOINT": "8",
                "PROJECT_RAY_ACTOR_MAX_CONCURRENCY": "32",
                "PROJECT_RAY_WORKER_NUM_CPUS": "0.25",
                "PROJECT_SHARED_CREDIT_QUANTUM": "2048",
                "DATABASE_URL": "postgresql://postgres:postgres@localhost/db",
                "SOURCE_MAX_PROMPT_TOKENS": "1500",
                "COMPLETION_ENDPOINT_URLS": (
                    "http://127.0.0.1:8000/v1/completions,"
                    "http://127.0.0.1:8001/v1/completions"
                ),
                "COMPLETION_MODEL": "qwen",
                "COMPLETION_HTTP_KEEPALIVE_EXPIRY_S": "4",
                "COMPLETION_PROTOCOL": "completions",
                "COMPLETION_MAX_TOKENS": "8",
                "MODEL_METRICS_URLS": (
                    "http://127.0.0.1:8000/metrics,"
                    "http://127.0.0.1:8001/metrics"
                ),
                "ENDPOINT_GPU_IDS": "0,1",
                "SAOR_ACTIVE_SET_WORKLOAD": "saor-test",
                "MODEL_PATH": "/models/qwen",
                "REQUEST_SLO_MS": "30000",
                "GPU_PEAK_TFLOPS": "165",
                "MFU_PRECISION": "bf16_dense_fp32_accumulate",
                "SAOR_BULK_ROWS": "2",
                "SAOR_FOREGROUND_ROWS": "2",
                "SAOR_FOREGROUND_OFFSET_S": "5",
                "SAOR_ARRIVAL_TIME_SCALE": "1",
                "SAOR_MAX_EFFECTIVE_MANIFEST_SPAN_S": "120",
                "SAOR_MIN_PRE_FOREGROUND_WORK_ENVELOPES": "0.0001",
                "SAOR_BULK_MANIFEST": str(bulk),
                "SAOR_FOREGROUND_MANIFEST": str(foreground),
            }
            with patch.dict(os.environ, environment, clear=True):
                result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_active_set_release.example.json"
                )

        self.assertEqual(result["status"], "failed")
        self.assertTrue(
            any("effective replay span" in item for item in result["errors"])
        )

    def test_summary_fails_closed_when_active_scenario_is_missing(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "matrix"
            output = Path(directory) / "summary"
            root.mkdir()
            (root / "manifest.json").write_text(
                json.dumps({"status": "completed", "incidents": []}),
                encoding="utf-8",
            )
            with (root / "group_runs.csv").open(
                "w", newline="", encoding="utf-8"
            ) as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=("scenario_id", "phase", "repeat_index"),
                )
                writer.writeheader()

            with self.assertRaisesRegex(ValueError, "scenario set"):
                SUMMARY.summarize(root, output)

            validation = json.loads(
                (output / "validation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(validation["status"], "failed")

    @staticmethod
    def _request(
        doc_id: int,
        endpoint_index: int,
        *,
        arrival_time_s: float | None = None,
    ) -> ChatRequest:
        return ChatRequest(
            doc_id=doc_id,
            prompt=f"prompt-{doc_id}",
            arrival_time_s=(
                float(doc_id) if arrival_time_s is None else arrival_time_s
            ),
            prompt_tokens=4,
            max_output_tokens=8,
            estimated_output_tokens=8,
            source_row_hash=f"hash-{doc_id}",
            endpoint_index=endpoint_index,
        )

    @staticmethod
    def _write_group_rows(path: Path, rows: list[dict[str, object]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _work_request_row(
        *,
        scenario_id: str,
        job_id: str,
        endpoint_id: str,
        submission_id: str,
        doc_id: str,
        completion_epoch_s: float,
        raw_prompt_tokens: int = 10,
        output_tokens: int = 10,
        estimated_output_tokens: int = 10,
        client_estimated_output_tokens: int | None = None,
        phase: str = "warmup",
        repeat_index: int = 1,
    ) -> dict[str, object]:
        return {
            "experiment_id": "test",
            "phase": phase,
            "repeat_index": repeat_index,
            "scenario_id": scenario_id,
            "job_id": job_id,
            "request_id": f"request-{submission_id}",
            "submission_id": submission_id,
            "doc_id": doc_id,
            "endpoint_id": endpoint_id,
            "status": "completed",
            "error_type": "",
            "prompt_tokens": raw_prompt_tokens,
            "actual_output_tokens": output_tokens,
            "client_estimated_output_tokens": (
                estimated_output_tokens
                if client_estimated_output_tokens is None
                else client_estimated_output_tokens
            ),
            "estimated_output_tokens": estimated_output_tokens,
            "output_token_source": "endpoint_request",
            "total_tokens": raw_prompt_tokens + output_tokens,
            "completion_epoch_s": completion_epoch_s,
        }

    @staticmethod
    def _work_submission_row(
        *,
        job_id: str,
        endpoint_id: str,
        submission_id: str,
        doc_id: str,
        raw_prompt_tokens: int = 10,
        output_tokens: int = 10,
        phase: str = "warmup",
        repeat_index: int = 1,
        ready_epoch_s: float | str = "",
        registered_epoch_s: float | str = "",
        granted_epoch_s: float | str = "",
    ) -> dict[str, object]:
        endpoint_prompt = raw_prompt_tokens + 29
        return {
            "experiment_id": "test",
            "phase": phase,
            "repeat_index": repeat_index,
            "job_id": job_id,
            "submission_id": submission_id,
            "doc_ids": doc_id,
            "endpoint_id": endpoint_id,
            "status": "completed",
            "error": "",
            "rows": 1,
            "token_count": endpoint_prompt + output_tokens,
            "input_token_count": endpoint_prompt,
            "output_token_count": output_tokens,
            "token_count_source": "endpoint_usage_total_tokens",
            "input_token_count_source": "endpoint_usage_prompt_tokens",
            "output_token_count_source": "endpoint_usage_completion_tokens",
            "ready_epoch_s": ready_epoch_s,
            "credit_registered_epoch_s": registered_epoch_s,
            "credit_granted_epoch_s": granted_epoch_s,
        }

    @staticmethod
    def _write_clean_manifest(root: Path) -> None:
        (root / "manifest.json").write_text(
            json.dumps({"status": "completed", "incidents": []}),
            encoding="utf-8",
        )

    @staticmethod
    def _priority_matrix_rows(
        *,
        priority_p99_s: float,
    ) -> list[dict[str, object]]:
        scenarios = (
            ("active_set_static_partition", "static_partition", 29.2, 0.0),
            ("active_set_saor_release", "saor_release", 50.3, 0.831),
            (
                "active_set_foreground_strict_priority",
                "foreground_strict_priority",
                priority_p99_s,
                0.005,
            ),
        )
        rows: list[dict[str, object]] = []
        for scenario_id, policy, foreground_p99, foreground_slo in scenarios:
            phases = (("warmup", 0), *(("formal", i) for i in range(1, 4)))
            for phase, repeat_index in phases:
                priorities = (
                    [0, 1]
                    if policy == "foreground_strict_priority"
                    else [0, 0]
                )
                credit_policy = policy in {
                    "saor_release",
                    "foreground_strict_priority",
                }
                rows.append(
                    {
                        "scenario_id": scenario_id,
                        "policy": policy,
                        "phase": phase,
                        "repeat_index": repeat_index,
                        "incidents": 0,
                        "metrics_status": "ok",
                        "resource_metrics_status": "ok",
                        "actor_worker_failures": 0,
                        "active_set_lifecycle_passed": True,
                        "active_set_mechanism_applicable": credit_policy,
                        "active_set_mechanism_passed": credit_policy,
                        "job_priorities": json.dumps(priorities),
                        "job_arrived_rows": "[512, 512]",
                        "job_completed_rows": "[512, 512]",
                        "job_failed_rows": "[0, 0]",
                        "job_p99_s": json.dumps([60.0, foreground_p99]),
                        "job_slo_violation_ratio": json.dumps(
                            [0.5, foreground_slo]
                        ),
                        "job_jct_s": json.dumps([70.0, 40.0]),
                        "tokens_per_s": 10_100.0,
                    }
                )
        return rows

    @staticmethod
    def _write_matched_ready_matrix(
        root: Path,
        *,
        include_fairness: bool = True,
    ) -> None:
        root.mkdir(parents=True)
        if include_fairness:
            (root / "jobs").mkdir()
        (root / "manifest.json").write_text(
            json.dumps(
                {
                    "status": "completed",
                    "execution_mode": "rehearsal",
                    "incidents": [],
                    "config_fingerprint": "same-config",
                    "repository_commit": "same-commit",
                    "redacted_config": {
                        "service_metadata": {
                            "vllm_version": "test",
                            "scheduling_policy": "fcfs",
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        scenarios = tuple(MATCHED_READY_SUMMARY.EXPECTED.items())
        rows = []
        for order_index, (
            scenario_id,
            (policy, identity, observation),
        ) in enumerate(scenarios):
            bounded = observation == "bounded_concrete_pre_registration"
            proposed = policy == "saor_bounded_ready"
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "policy": policy,
                    "experiment_identity": identity,
                    "ready_observation_contract": observation,
                    "phase": "warmup",
                    "repeat_index": 1,
                    "order_index": order_index,
                    "execution_mode": "rehearsal",
                    "incidents": 0,
                    "actor_worker_failures": 0,
                    "metrics_status": "ok",
                    "resource_metrics_status": "ok",
                    "active_set_lifecycle_passed": True,
                    "job_arrived_rows": "[512, 512]",
                    "job_completed_rows": "[512, 512]",
                    "job_failed_rows": "[0, 0]",
                    "job_p99_s": "[60, 20]",
                    "job_slo_violation_ratio": "[0.6, 0]",
                    "job_jct_s": "[70, 30]",
                    "tokens_per_s": 10000,
                    "duration_s": 70,
                    "mfu_estimate": 0.4,
                    "jain_fairness": 0.9,
                    "max_overlap_normalized_service_disparity": 100,
                    "bounded_ready_event_status": (
                        "ok:actor_event_join" if bounded else "not_applicable"
                    ),
                    "bounded_ready_lifecycle_complete": bounded,
                    "bounded_ready_jobs_with_intervals": 2 if bounded else 0,
                    "bounded_ready_intervals": 1024 if bounded else 0,
                    "bounded_ready_max_ready_requests_seen": 128 if bounded else 0,
                    "bounded_ready_max_ready_work_seen": 65536 if bounded else 0,
                    "bounded_ready_max_ready_payload_bytes_seen": 1024 if bounded else 0,
                    "bounded_ready_foreign_fallback_events": 0,
                    "bounded_saor_event_status": (
                        "ok:lossless_ledger" if proposed else "unavailable"
                    ),
                    "bounded_saor_event_sequence_complete": proposed,
                    "bounded_saor_slo_priority_grants": 1 if proposed else 0,
                    "bounded_saor_debt_recovery_grants": 1 if proposed else 0,
                    "bounded_saor_avoidable_idle_events": 0,
                    "bounded_saor_foreign_grant_over_debt_critical_events": 0,
                    "bounded_saor_recovery_inflight_max": 1 if proposed else 0,
                }
            )
            if include_fairness and bounded:
                for job_index in range(2):
                    stem = (
                        f"{order_index:03d}_warmup_1_{scenario_id}_"
                        f"job{job_index}"
                    )
                    submission_id = f"{scenario_id}-job{job_index}-r0"
                    SaorFormalToolsTests._write_group_rows(
                        root / "jobs" / f"{stem}.requests.csv",
                        [
                            SaorFormalToolsTests._work_request_row(
                                scenario_id=scenario_id,
                                job_id=f"job-{job_index}",
                                endpoint_id=f"endpoint-{job_index}",
                                submission_id=submission_id,
                                doc_id=f"doc-{job_index}",
                                completion_epoch_s=2.0 + job_index,
                            )
                        ],
                    )
                    SaorFormalToolsTests._write_group_rows(
                        root / "jobs" / f"{stem}.submissions.csv",
                        [
                            SaorFormalToolsTests._work_submission_row(
                                job_id=f"job-{job_index}",
                                endpoint_id=f"endpoint-{job_index}",
                                submission_id=submission_id,
                                doc_id=f"doc-{job_index}",
                                ready_epoch_s=0.0,
                                registered_epoch_s=0.1,
                                granted_epoch_s=0.2,
                            )
                        ],
                    )
                    SaorFormalToolsTests._write_group_rows(
                        root / "jobs" / f"{stem}.runs.csv",
                        [
                            {
                                "completion_protocol": "chat_completions",
                                "completion_prompt_token_overhead": 29,
                                "output_cost_mode": "fixed_output_cap",
                                "completion_max_tokens": 10,
                            }
                        ],
                    )
        SaorFormalToolsTests._write_group_rows(root / "group_runs.csv", rows)

    @staticmethod
    def _write_project_formal_matrix(
        root: Path,
        contract: dict[str, object],
        contract_sha256: str,
    ) -> None:
        (root / "jobs").mkdir(parents=True)
        (root / "project_mechanism_contract.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "contract_path": "test",
                    "contract_sha256": contract_sha256,
                    "contract": contract,
                    "readiness": {
                        "status": "passed",
                        "work_cost_calibration_identity": {
                            key: value
                            for key, value in contract[
                                "work_cost_contract"
                            ]["calibration_identity"].items()
                            if key not in {"model_id", "tokenizer_id"}
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        (root / "manifest.json").write_text(
            json.dumps(
                {
                    "status": "completed",
                    "execution_mode": "configured_matrix",
                    "incidents": [],
                }
            ),
            encoding="utf-8",
        )
        rows = []
        scenarios = tuple(PROJECT_FORMAL_SCENARIOS.items())
        for repeat in range(1, 4):
            for scenario_index, (
                scenario_id,
                (policy, observation),
            ) in enumerate(scenarios):
                order_index = 6 + (repeat - 1) * 6 + scenario_index
                bounded = observation == "bounded_concrete_pre_registration"
                proposed = scenario_id == PROJECT_FORMAL_PROPOSED
                rows.append(
                    {
                        "scenario_id": scenario_id,
                        "policy": policy,
                        "ready_observation_contract": observation,
                        "phase": "formal",
                        "repeat_index": repeat,
                        "order_index": order_index,
                        "execution_mode": "configured_matrix",
                        "incidents": 0,
                        "actor_worker_failures": 0,
                        "metrics_status": "ok",
                        "resource_metrics_status": "ok",
                        "active_set_lifecycle_passed": True,
                        "job_arrived_rows": "[1, 1]",
                        "job_completed_rows": "[1, 1]",
                        "job_failed_rows": "[0, 0]",
                        "job_jct_s": "[100, 30]",
                        "job_p99_s": (
                            "[70, 18]" if proposed else "[70, 20]"
                        ),
                        "job_slo_violation_ratio": "[0.5, 0]",
                        "job_slo_goodput_per_s": "[1, 1]",
                        "job_slo_token_goodput_per_s": "[20, 20]",
                        "tokens_per_s": 96 if proposed else 100,
                        "duration_s": 100,
                        "mfu_estimate": 0.4,
                        "jain_fairness": 0.9,
                        "bounded_ready_event_status": (
                            "ok:actor_event_join"
                            if bounded else "not_applicable"
                        ),
                        "bounded_ready_lifecycle_complete": bounded,
                        "bounded_ready_jobs_with_intervals": 2 if bounded else 0,
                        "bounded_ready_intervals": 2 if bounded else 0,
                        "bounded_ready_max_ready_requests_seen": 2 if bounded else 0,
                        "bounded_ready_max_ready_work_seen": 100 if bounded else 0,
                        "bounded_ready_max_ready_payload_bytes_seen": 1000 if bounded else 0,
                        "bounded_ready_foreign_fallback_events": 0,
                        "bounded_saor_event_status": (
                            "ok:lossless_ledger" if proposed else "unavailable"
                        ),
                        "bounded_saor_event_sequence_complete": proposed,
                        "bounded_saor_slo_priority_grants": 1 if proposed else 0,
                        "bounded_saor_debt_recovery_grants": 1 if proposed else 0,
                        "bounded_saor_recovery_completions": 1 if proposed else 0,
                        "bounded_saor_unmatched_recovery_grants": 0,
                        "bounded_saor_debt_repayment_episodes": 1 if proposed else 0,
                        "bounded_saor_debt_repayment_completed": 1 if proposed else 0,
                        "bounded_saor_debt_repayment_unresolved": 0,
                        "bounded_saor_debt_repayment_p95_s": 10 if proposed else 0,
                        "bounded_saor_recovery_completion_p95_s": 5 if proposed else 0,
                        "bounded_saor_projection_status": (
                            "ok:offline_recomputed"
                            if proposed else "not_applicable"
                        ),
                        "bounded_saor_projection_checked_events": (
                            1 if proposed else 0
                        ),
                        "bounded_saor_projection_expected_events": (
                            1 if proposed else 0
                        ),
                        "bounded_saor_projection_violation_events": 0,
                        "bounded_saor_projected_overshoot_bound_violation_events": 0,
                        "bounded_saor_projection_estimation_overrun_events": 0,
                        "bounded_saor_recovery_estimation_overrun_events": 0,
                        "bounded_saor_recovery_inflight_work_max": (
                            80 if proposed else 0
                        ),
                        "bounded_saor_avoidable_idle_events": 0,
                        "bounded_saor_foreign_grant_over_debt_critical_events": 0,
                        "bounded_saor_recovery_inflight_max": 1 if proposed else 0,
                        "active_set_post_drain_status": (
                            "ok:observed_work_conserving_drain"
                            if proposed else "not_applicable"
                        ),
                        "active_set_post_drain_applicable": proposed,
                        "active_set_post_work_conserving_passed": proposed,
                    }
                )
                for job_index in range(2):
                    stem = (
                        f"{order_index:03d}_formal_{repeat}_{scenario_id}_"
                        f"job{job_index}"
                    )
                    submission_id = (
                        f"{scenario_id}-{repeat}-job{job_index}-r0"
                    )
                    SaorFormalToolsTests._write_group_rows(
                        root / "jobs" / f"{stem}.requests.csv",
                        [
                            SaorFormalToolsTests._work_request_row(
                                scenario_id=scenario_id,
                                job_id=f"job-{job_index}",
                                endpoint_id=f"endpoint-{job_index}",
                                submission_id=submission_id,
                                doc_id=f"doc-{repeat}-{job_index}",
                                completion_epoch_s=2.0 + job_index,
                                estimated_output_tokens=256,
                                phase="formal",
                                repeat_index=repeat,
                            )
                        ],
                    )
                    SaorFormalToolsTests._write_group_rows(
                        root / "jobs" / f"{stem}.submissions.csv",
                        [
                            SaorFormalToolsTests._work_submission_row(
                                job_id=f"job-{job_index}",
                                endpoint_id=f"endpoint-{job_index}",
                                submission_id=submission_id,
                                doc_id=f"doc-{repeat}-{job_index}",
                                phase="formal",
                                repeat_index=repeat,
                                ready_epoch_s=(0.0 if bounded else ""),
                                registered_epoch_s=(0.1 if bounded else ""),
                                granted_epoch_s=(0.2 if bounded else ""),
                            )
                        ],
                    )
        for scenario_id, (policy, observation) in scenarios:
            rows.append(
                {
                    **rows[0],
                    "scenario_id": scenario_id,
                    "policy": policy,
                    "ready_observation_contract": observation,
                    "phase": "warmup",
                    "repeat_index": 1,
                }
            )
        SaorFormalToolsTests._write_group_rows(root / "group_runs.csv", rows)

    @staticmethod
    def _write_ready_observation_bridge(root: Path) -> None:
        root.mkdir(parents=True)
        (root / "manifest.json").write_text(
            json.dumps(
                {
                    "status": "completed",
                    "execution_mode": "rehearsal",
                    "incidents": [],
                    "config_fingerprint": "same-bridge-config",
                    "repository_commit": "same-bridge-commit",
                    "redacted_config": {
                        "service_metadata": {
                            "vllm_version": "test",
                            "scheduling_policy": "fcfs",
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        rows = []
        for index, (scenario_id, expected) in enumerate(
            OBSERVATION_BRIDGE_SUMMARY.EXPECTED.items()
        ):
            policy, identity, observation = expected
            bounded = observation == "bounded_concrete_pre_registration"
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "policy": policy,
                    "experiment_identity": identity,
                    "ready_observation_contract": observation,
                    "phase": "warmup",
                    "execution_mode": "rehearsal",
                    "incidents": 0,
                    "actor_worker_failures": 0,
                    "metrics_status": "ok",
                    "resource_metrics_status": "ok",
                    "active_set_lifecycle_passed": True,
                    "job_arrived_rows": "[512, 512]",
                    "job_completed_rows": "[512, 512]",
                    "job_failed_rows": "[0, 0]",
                    "job_jct_s": json.dumps([90 - 10 * index, 40 + index]),
                    "job_p99_s": json.dumps([80 - 10 * index, 30 + index]),
                    "job_slo_violation_ratio": "[0.6, 0]",
                    "tokens_per_s": 9000 + 1000 * index,
                    "duration_s": 92 - 10 * index,
                    "mfu_estimate": 0.35 + 0.04 * index,
                    "jain_fairness": 0.9,
                    "bounded_ready_event_status": (
                        "ok:actor_event_join" if bounded else "not_applicable"
                    ),
                    "bounded_ready_lifecycle_complete": bounded,
                    "bounded_ready_jobs_with_intervals": 2 if bounded else 0,
                    "bounded_ready_intervals": 1024 if bounded else 0,
                    "bounded_ready_max_ready_requests_seen": 128 if bounded else 0,
                    "bounded_ready_max_ready_work_seen": 65536 if bounded else 0,
                    "bounded_ready_max_ready_payload_bytes_seen": (
                        1024 if bounded else 0
                    ),
                }
            )
        SaorFormalToolsTests._write_group_rows(root / "group_runs.csv", rows)

    @staticmethod
    def _write_bounded_matrix(
        root: Path,
        *,
        ready_policy: bool = False,
    ) -> None:
        (root / "traces").mkdir(parents=True)
        (root / "manifest.json").write_text(
            json.dumps(
                {
                    "status": "completed",
                    "execution_mode": "rehearsal",
                    "incidents": [],
                    "config_fingerprint": "same-config",
                    "repository_commit": "same-commit",
                    "redacted_config": {
                        "service_metadata": {"vllm_version": "test", "scheduling_policy": "fcfs"}
                    },
                }
            ),
            encoding="utf-8",
        )
        bounded_policy = (
            "saor_bounded_ready"
            if ready_policy
            else "saor_bounded_priority"
        )
        bounded_stem = (
            "active_set_saor_bounded_ready"
            if ready_policy
            else "active_set_saor_bounded_priority"
        )
        scenarios = (
            ("active_set_static_partition", "static_partition"),
            ("active_set_saor_release", "saor_release"),
            (f"{bounded_stem}_0125k", bounded_policy),
            (f"{bounded_stem}_025k", bounded_policy),
        )
        rows = []
        for scenario_id, policy in scenarios:
            event_path = Path("traces") / f"000_warmup_0_{scenario_id}.release_events.csv"
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "policy": policy,
                    "phase": "warmup",
                    "repeat_index": 0,
                    "execution_mode": "rehearsal",
                    "incidents": 0,
                    "metrics_status": "ok",
                    "resource_metrics_status": "ok",
                    "actor_worker_failures": 0,
                    "active_set_lifecycle_passed": True,
                    "job_arrived_rows": "[512, 512]",
                    "job_completed_rows": "[512, 512]",
                    "job_failed_rows": "[0, 0]",
                    "job_p99_s": "[60.0, 30.0]",
                    "job_slo_violation_ratio": "[0.70, 0.0]",
                    "tokens_per_s": 10_100.0,
                    "release_event_trace_path": str(event_path),
                    "bounded_ready_event_status": (
                        "ok:actor_event_join"
                        if ready_policy
                        else "not_applicable"
                    ),
                    "bounded_ready_lifecycle_complete": ready_policy,
                    "bounded_ready_foreground_intervals": (
                        2 if ready_policy else 0
                    ),
                    "bounded_ready_foreign_fallback_events": 0,
                    "bounded_ready_foreground_max_ready_requests_seen": (
                        2 if ready_policy else 0
                    ),
                    "bounded_ready_foreground_max_ready_work_seen": (
                        100 if ready_policy else 0
                    ),
                }
            )
            if policy in {"saor_bounded_priority", "saor_bounded_ready"}:
                with (root / event_path).open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=(
                            "event_seq",
                            "event_time_s",
                            "endpoint_id",
                            "action",
                            "tier",
                            "recovery_inflight_by_job",
                            "constraint_conflict",
                            "avoidable_idle",
                            "foreign_grant_over_debt_critical",
                        ),
                    )
                    writer.writeheader()
                    writer.writerows(
                        (
                            {
                                "event_seq": 1,
                                "event_time_s": 10.0,
                                "endpoint_id": "endpoint-0",
                                "action": "grant",
                                "tier": "slo_priority",
                                "recovery_inflight_by_job": "[]",
                                "constraint_conflict": "False",
                                "avoidable_idle": "False",
                                "foreign_grant_over_debt_critical": "False",
                            },
                            {
                                "event_seq": 2,
                                "event_time_s": 10.005,
                                "endpoint_id": "endpoint-0",
                                "action": "grant",
                                "tier": "debt_recovery",
                                "recovery_inflight_by_job": '[["bulk", "r1"]]',
                                "constraint_conflict": "False",
                                "avoidable_idle": "False",
                                "foreign_grant_over_debt_critical": "False",
                            },
                        )
                    )
        SaorFormalToolsTests._write_group_rows(root / "group_runs.csv", rows)


if __name__ == "__main__":
    unittest.main()
