from __future__ import annotations

import json
import os
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.experiments.run_shared_vllm_experiment import parse_args  # noqa: E402
import src.experiments.shared_vllm as shared_vllm  # noqa: E402
from src.experiments.shared_vllm import (  # noqa: E402
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
    _validate_rehearsal_record,
    _validate_replay_starts,
    _validate_runner_topology,
    _validate_final_credit,
    active_set_phase_summary,
    bounded_saor_event_summary,
    build_job_command,
    completion_accounted_service_fairness,
    cumulative_service_disparity,
    group_resource_summary,
    group_metric_delta,
    jain_fairness,
    load_config,
    normalized_job_service_rates,
    run_experiment,
    run_shared_vllm_group_cell,
    shared_credit_trace_summary,
)
from src.experiments.shared_vllm.runner import _wait_for_eager_job_launch
from src.scheduling.submission_control.shared_credit import (  # noqa: E402
    SaorReleaseEvent,
)


class SharedVllmExperimentTests(unittest.TestCase):
    def test_eager_job_launch_waits_for_absolute_job_offset(self) -> None:
        now_values = iter((100.0, 104.0, 105.0))
        sleeps: list[float] = []

        observed = _wait_for_eager_job_launch(
            105.0,
            now=lambda: next(now_values),
            sleep=lambda seconds: sleeps.append(seconds),
        )

        self.assertEqual(sleeps, [0.05, 0.05])
        self.assertEqual(observed, 105.0)

    def test_single_group_cell_uses_explicit_identity_without_matrix_schedule(self) -> None:
        with TemporaryDirectory() as temp_dir:
            options, config, scenario = self._group_fixture(Path(temp_dir))
            identity = GroupRunIdentity("formal", 2, 7)
            expected = {"scenario_id": scenario.scenario_id, "status": "completed"}
            with patch(
                "src.experiments.shared_vllm.runner._run_group",
                return_value=expected,
            ) as group:
                actual = run_shared_vllm_group_cell(
                    options, config, scenario, identity
                )

            self.assertEqual(actual, expected)
            group.assert_called_once_with(
                options, config, scenario, identity, idle_gate=None
            )

    def test_eager_arrival_contract_omits_per_request_arrival_replay(self) -> None:
        payload = self._config_payload(common_args=[])
        payload["job_internal_arrival_contract"] = "eager"
        with patch.object(Path, "read_text", return_value=json.dumps(payload)):
            config = load_config(Path("config.json"))

        self.assertEqual(config.job_internal_arrival_contract, "eager")

    def test_arrival_contract_requires_matching_replay_flag(self) -> None:
        for contract, common_args, expected in (
            ("manifest_timed", [], "requires --arrival-replay"),
            ("eager", ["--arrival-replay"], "rejects --arrival-replay"),
        ):
            payload = self._config_payload(common_args=common_args)
            payload["job_internal_arrival_contract"] = contract
            with self.subTest(contract=contract), patch.object(
                Path, "read_text", return_value=json.dumps(payload)
            ), self.assertRaisesRegex(ValueError, expected):
                load_config(Path("config.json"))
    def test_rehearsal_record_gate_is_fail_closed(self) -> None:
        scenario = SharedVllmScenario(
            scenario_id="active_set_saor_release",
            policy="saor_release",
            job_count=2,
            rows_per_job=None,
            rows_per_jobs=(1, 1),
            weights=(1, 1),
            arrival_offsets_s=(0.0, 5.0),
        )
        record = {
            "execution_mode": "rehearsal",
            "metrics_status": "ok",
            "resource_metrics_status": "ok",
            "incidents": 0,
            "actor_worker_failures": 0,
            "active_set_lifecycle_passed": True,
            "active_set_mechanism_applicable": True,
            "active_set_mechanism_passed": True,
        }

        _validate_rehearsal_record(scenario, record)
        with self.assertRaisesRegex(RuntimeError, "mechanism gate failed"):
            _validate_rehearsal_record(
                scenario,
                {**record, "active_set_mechanism_passed": False},
            )
        with self.assertRaisesRegex(RuntimeError, "resource metrics"):
            _validate_rehearsal_record(
                scenario,
                {**record, "resource_metrics_status": "unavailable"},
            )

    def test_bounded_rehearsal_gate_uses_event_ledger_not_sampled_snapshot(
        self,
    ) -> None:
        scenario = SharedVllmScenario(
            scenario_id="active_set_saor_bounded_priority_0125k",
            policy="saor_bounded_priority",
            job_count=2,
            rows_per_job=None,
            rows_per_jobs=(1, 1),
            weights=(1, 1),
            arrival_offsets_s=(0.0, 5.0),
            priorities=(0, 1),
            slo_targets_s=(None, 30.0),
            priority_windows_s=(None, 30.0),
            debt_cap_fractions=(0.125, None),
        )
        record = {
            "execution_mode": "rehearsal",
            "metrics_status": "ok",
            "resource_metrics_status": "ok",
            "incidents": 0,
            "actor_worker_failures": 0,
            "active_set_lifecycle_passed": True,
            # A sampled snapshot can miss a transition shorter than 250 ms.
            "active_set_mechanism_applicable": True,
            "active_set_mechanism_passed": False,
            "bounded_saor_event_status": "ok:lossless_ledger",
            "bounded_saor_event_sequence_complete": True,
            "bounded_saor_slo_priority_grants": 1,
            "bounded_saor_debt_recovery_grants": 1,
            "bounded_saor_avoidable_idle_events": 0,
            "bounded_saor_foreign_grant_over_debt_critical_events": 0,
            "bounded_saor_recovery_inflight_max": 1,
        }

        _validate_rehearsal_record(scenario, record)
        with self.assertRaisesRegex(RuntimeError, "sequence is incomplete"):
            _validate_rehearsal_record(
                scenario,
                {**record, "bounded_saor_event_sequence_complete": False},
            )

    def test_bounded_ready_rehearsal_requires_epoch_join(self) -> None:
        scenario = SharedVllmScenario(
            scenario_id="active_set_saor_bounded_ready_0125k",
            policy="saor_bounded_ready",
            job_count=2,
            rows_per_job=None,
            rows_per_jobs=(1, 1),
            weights=(1, 1),
            arrival_offsets_s=(0.0, 5.0),
            priorities=(0, 1),
            slo_targets_s=(None, 30.0),
            priority_windows_s=(None, 30.0),
            debt_cap_fractions=(0.125, None),
            ready_observation_contract=(
                "bounded_concrete_pre_registration"
            ),
        )
        record = {
            "execution_mode": "rehearsal",
            "metrics_status": "ok",
            "resource_metrics_status": "ok",
            "incidents": 0,
            "actor_worker_failures": 0,
            "active_set_lifecycle_passed": True,
            "bounded_saor_event_status": "ok:lossless_ledger",
            "bounded_saor_event_sequence_complete": True,
            "bounded_saor_slo_priority_grants": 1,
            "bounded_saor_debt_recovery_grants": 1,
            "bounded_saor_avoidable_idle_events": 0,
            "bounded_saor_foreign_grant_over_debt_critical_events": 0,
            "bounded_saor_recovery_inflight_max": 1,
            "bounded_ready_event_status": "ok:actor_event_join",
            "bounded_ready_lifecycle_complete": True,
            "bounded_ready_intervals": 2,
            "bounded_ready_jobs_with_intervals": 2,
            "bounded_ready_max_ready_requests_seen": 2,
            "bounded_ready_max_ready_work_seen": 100,
            "bounded_ready_max_ready_payload_bytes_seen": 200,
            "bounded_ready_foreground_intervals": 1,
            "bounded_ready_foreign_fallback_events": 0,
            "bounded_ready_foreground_max_ready_requests_seen": 2,
            "bounded_ready_foreground_max_ready_work_seen": 100,
        }

        _validate_rehearsal_record(scenario, record)
        with self.assertRaisesRegex(RuntimeError, "observation gate"):
            _validate_rehearsal_record(
                scenario,
                {**record, "bounded_ready_foreign_fallback_events": 1},
            )

    def test_matched_fifo_requires_ready_lifecycle_not_saor_mechanism(self) -> None:
        scenario = SharedVllmScenario(
            scenario_id="active_set_shared_fifo_matched_ready",
            policy="shared_fifo",
            job_count=2,
            rows_per_job=1,
            weights=(1, 1),
            arrival_offsets_s=(0.0, 5.0),
        )
        record = {
            "execution_mode": "rehearsal",
            "metrics_status": "ok",
            "resource_metrics_status": "ok",
            "incidents": 0,
            "actor_worker_failures": 0,
            "active_set_lifecycle_passed": True,
            "active_set_mechanism_applicable": True,
            "active_set_mechanism_passed": True,
            "bounded_ready_event_status": "ok:actor_event_join",
            "bounded_ready_lifecycle_complete": True,
            "bounded_ready_intervals": 2,
            "bounded_ready_jobs_with_intervals": 2,
            "bounded_ready_max_ready_requests_seen": 2,
            "bounded_ready_max_ready_work_seen": 100,
            "bounded_ready_max_ready_payload_bytes_seen": 200,
        }

        _validate_rehearsal_record(
            scenario,
            record,
            ready_observation_contract="bounded_concrete_pre_registration",
        )
        with self.assertRaisesRegex(RuntimeError, "observation gate"):
            _validate_rehearsal_record(
                scenario,
                {**record, "bounded_ready_jobs_with_intervals": 1},
                ready_observation_contract=(
                    "bounded_concrete_pre_registration"
                ),
            )

    def test_rehearsal_runs_one_nonformal_cell_per_scenario(self) -> None:
        options = RunnerOptions(
            config_path=Path("config.json"),
            profiler_path=Path("profile.py"),
            python_executable=Path(sys.executable),
            output_dir=Path("out"),
            health_url="http://health",
            metrics_urls=("http://metrics",),
            ray_address="local",
            idle_timeout_s=1.0,
            rehearsal=True,
        )
        scenario = SharedVllmScenario(
            scenario_id="only",
            policy="independent_full",
            job_count=1,
            rows_per_job=1,
            weights=(1,),
            arrival_offsets_s=(0.0,),
        )
        config = SharedVllmConfig(
            experiment_id="formal-config",
            seed=1,
            warmup_runs_per_scenario=1,
            formal_repeats=3,
            endpoint_ids=("endpoint-0",),
            request_limit_per_endpoint=1,
            work_limit_per_endpoint=1,
            credit_quantum=1,
            shared_credit_namespace="test",
            gpu_peak_tflops=1.0,
            mfu_precision="bf16",
            common_args=("--arrival-replay",),
            scenarios=(scenario,),
            service_metadata=(),
        )

        with (
            patch(
                "src.experiments.shared_vllm.runner.load_config",
                return_value=config,
            ),
            patch(
                "src.experiments.shared_vllm.runner._validate_runner_topology"
            ),
            patch(
                "src.experiments.shared_vllm.runner._repository_commit",
                return_value="commit",
            ),
            patch(
                "src.experiments.shared_vllm.runner.acquire_runner_lease"
            ) as lease,
            patch(
                "src.experiments.shared_vllm.runner._run_locked",
                return_value=0,
            ) as locked,
        ):
            lease.return_value.__enter__.return_value.recovered_owner = None
            self.assertEqual(run_experiment(options, idle_gate=MagicMock()), 0)

        schedule = locked.call_args.args[2]
        self.assertEqual(len(schedule), 1)
        self.assertEqual(schedule[0].phase, "warmup")

    def test_active_set_phase_summary_uses_observed_lifecycle(self) -> None:
        evidence = [
            {
                "arrival_start_epoch_s": 10.0,
                "completion_end_epoch_s": 30.0,
                "runtime_job_id": "bulk",
            },
            {
                "arrival_start_epoch_s": 15.0,
                "completion_end_epoch_s": 20.0,
                "runtime_job_id": "foreground",
            },
        ]
        samples = [
            {
                "observed_epoch_s": 12.0,
                "request_limit": 100,
                "work_limit": 100,
                "active_by_job": '[["bulk", 100]]',
                "active_work_by_job": '[["bulk", 100]]',
            },
            {
                "observed_epoch_s": 17.0,
                "request_limit": 100,
                "work_limit": 100,
                "active_by_job": '[["bulk", 60], ["foreground", 40]]',
                "active_work_by_job": (
                    '[["bulk", 60], ["foreground", 40]]'
                ),
                "waiting_work_by_job": '[["bulk", 20]]',
            },
            {
                "observed_epoch_s": 25.0,
                "request_limit": 100,
                "work_limit": 100,
                "active_by_job": '[["bulk", 90]]',
                "active_work_by_job": '[["bulk", 90]]',
            },
        ]

        summary = active_set_phase_summary(evidence, samples)

        self.assertTrue(summary["active_set_contract_passed"])
        self.assertTrue(summary["active_set_lifecycle_passed"])
        self.assertTrue(summary["active_set_mechanism_passed"])
        self.assertTrue(summary["active_set_foreground_drained_first"])
        self.assertEqual(summary["active_set_overlap_s"], 5.0)
        self.assertEqual(
            summary["active_set_bulk_reborrow_fraction_max"],
            0.9,
        )
        self.assertTrue(summary["active_set_post_work_conserving_passed"])

    def test_active_set_phase_summary_fails_without_observed_overlap(self) -> None:
        evidence = [
            {
                "arrival_start_epoch_s": 10.0,
                "completion_end_epoch_s": 30.0,
                "runtime_job_id": "bulk",
            },
            {
                "arrival_start_epoch_s": 15.0,
                "completion_end_epoch_s": 20.0,
                "runtime_job_id": "foreground",
            },
        ]
        samples = [
            {
                "observed_epoch_s": 12.0,
                "request_limit": 100,
                "work_limit": 100,
                "active_by_job": '[["bulk", 100]]',
                "active_work_by_job": '[["bulk", 100]]',
            },
            {
                "observed_epoch_s": 17.0,
                "request_limit": 100,
                "work_limit": 100,
                "active_by_job": '[["foreground", 40]]',
                "active_work_by_job": '[["foreground", 40]]',
            },
            {
                "observed_epoch_s": 25.0,
                "request_limit": 100,
                "work_limit": 100,
                "active_by_job": '[["bulk", 90]]',
                "active_work_by_job": '[["bulk", 90]]',
            },
        ]

        summary = active_set_phase_summary(evidence, samples)

        self.assertTrue(summary["active_set_lifecycle_passed"])
        self.assertFalse(summary["active_set_mechanism_passed"])
        self.assertEqual(
            summary["active_set_mechanism_status"],
            "active_set_mechanism_not_observed",
        )

    def test_near_simultaneous_drain_is_below_trace_resolution(self) -> None:
        summary = active_set_phase_summary(
            [
                {
                    "arrival_start_epoch_s": 0.0,
                    "completion_end_epoch_s": 68.743800,
                    "runtime_job_id": "bulk",
                },
                {
                    "arrival_start_epoch_s": 5.0,
                    "completion_end_epoch_s": 68.737972,
                    "runtime_job_id": "foreground",
                },
            ],
            [
                {
                    "observed_epoch_s": 2.0,
                    "request_limit": 100,
                    "work_limit": 100,
                    "active_by_job": '[["bulk", 95]]',
                    "active_work_by_job": '[["bulk", 95]]',
                    "waiting_work_by_job": '[["bulk", 100]]',
                },
                {
                    "observed_epoch_s": 10.0,
                    "request_limit": 100,
                    "work_limit": 100,
                    "active_by_job": (
                        '[["bulk", 45], ["foreground", 40]]'
                    ),
                    "active_work_by_job": (
                        '[["bulk", 45], ["foreground", 40]]'
                    ),
                    "waiting_work_by_job": '[["bulk", 20]]',
                },
                {
                    "observed_epoch_s": 68.5,
                    "request_limit": 100,
                    "work_limit": 100,
                    "active_by_job": (
                        '[["bulk", 20], ["foreground", 10]]'
                    ),
                    "active_work_by_job": (
                        '[["bulk", 20], ["foreground", 10]]'
                    ),
                    "waiting_work_by_job": '[["bulk", 10]]',
                },
            ],
            observation_interval_s=0.25,
        )

        self.assertTrue(summary["active_set_mechanism_passed"])
        self.assertFalse(summary["active_set_post_drain_applicable"])
        self.assertEqual(summary["active_set_post_drain_observed_samples"], 0)
        self.assertAlmostEqual(
            summary["active_set_post_drain_duration_s"],
            0.005828,
        )
        self.assertEqual(
            summary["active_set_post_drain_status"],
            "not_applicable:drain_below_trace_resolution",
        )
        self.assertEqual(
            summary["active_set_mechanism_status"],
            "ok:observed_borrow_reclaim_post_drain_not_applicable",
        )

    def test_resolvable_post_drain_without_sample_still_fails_closed(self) -> None:
        summary = active_set_phase_summary(
            [
                {
                    "arrival_start_epoch_s": 10.0,
                    "completion_end_epoch_s": 30.0,
                    "runtime_job_id": "bulk",
                },
                {
                    "arrival_start_epoch_s": 15.0,
                    "completion_end_epoch_s": 20.0,
                    "runtime_job_id": "foreground",
                },
            ],
            [
                {
                    "observed_epoch_s": 12.0,
                    "request_limit": 100,
                    "work_limit": 100,
                    "active_by_job": '[["bulk", 95]]',
                    "active_work_by_job": '[["bulk", 95]]',
                    "waiting_work_by_job": '[["bulk", 100]]',
                },
                {
                    "observed_epoch_s": 17.0,
                    "request_limit": 100,
                    "work_limit": 100,
                    "active_by_job": (
                        '[["bulk", 45], ["foreground", 40]]'
                    ),
                    "active_work_by_job": (
                        '[["bulk", 45], ["foreground", 40]]'
                    ),
                    "waiting_work_by_job": '[["bulk", 20]]',
                },
            ],
            observation_interval_s=0.25,
        )

        self.assertFalse(summary["active_set_mechanism_passed"])
        self.assertTrue(summary["active_set_post_drain_applicable"])
        self.assertEqual(summary["active_set_post_drain_observed_samples"], 0)
        self.assertEqual(
            summary["active_set_post_drain_status"],
            "active_set_post_drain_not_observed",
        )

    def test_active_set_lifecycle_applies_without_credit_trace(self) -> None:
        summary = active_set_phase_summary(
            [
                {
                    "arrival_start_epoch_s": 10.0,
                    "completion_end_epoch_s": 30.0,
                    "runtime_job_id": "bulk",
                },
                {
                    "arrival_start_epoch_s": 15.0,
                    "completion_end_epoch_s": 20.0,
                    "runtime_job_id": "foreground",
                },
            ],
            [],
        )

        self.assertTrue(summary["active_set_lifecycle_passed"])
        self.assertFalse(summary["active_set_mechanism_applicable"])
        self.assertEqual(
            summary["active_set_mechanism_status"],
            "not_applicable:no_credit_trace",
        )

    def test_lifecycle_does_not_select_on_foreground_finishing_first(self) -> None:
        summary = active_set_phase_summary(
            [
                {
                    "arrival_start_epoch_s": 10.0,
                    "completion_end_epoch_s": 20.0,
                    "runtime_job_id": "bulk",
                },
                {
                    "arrival_start_epoch_s": 15.0,
                    "completion_end_epoch_s": 25.0,
                    "runtime_job_id": "foreground",
                },
            ],
            [],
        )

        self.assertTrue(summary["active_set_lifecycle_passed"])
        self.assertFalse(summary["active_set_foreground_drained_first"])

    def test_post_drain_accepts_sub_share_work_when_nothing_is_waiting(self) -> None:
        summary = active_set_phase_summary(
            [
                {
                    "arrival_start_epoch_s": 10.0,
                    "completion_end_epoch_s": 20.0,
                    "runtime_job_id": "bulk",
                },
                {
                    "arrival_start_epoch_s": 15.0,
                    "completion_end_epoch_s": 25.0,
                    "runtime_job_id": "foreground",
                },
            ],
            [
                {
                    "observed_epoch_s": 12.0,
                    "request_limit": 100,
                    "work_limit": 100,
                    "active_by_job": '[["bulk", 90]]',
                    "active_work_by_job": '[["bulk", 90]]',
                    "waiting_work_by_job": '[["bulk", 100]]',
                },
                {
                    "observed_epoch_s": 17.0,
                    "request_limit": 100,
                    "work_limit": 100,
                    "active_by_job": (
                        '[["bulk", 45], ["foreground", 40]]'
                    ),
                    "active_work_by_job": (
                        '[["bulk", 45], ["foreground", 40]]'
                    ),
                    "waiting_work_by_job": (
                        '[["bulk", 20], ["foreground", 10]]'
                    ),
                },
                {
                    "observed_epoch_s": 22.0,
                    "request_limit": 100,
                    "work_limit": 100,
                    "active_by_job": '[["foreground", 40]]',
                    "active_work_by_job": '[["foreground", 40]]',
                    "waiting_work_by_job": "[]",
                },
            ],
        )

        self.assertTrue(summary["active_set_mechanism_passed"])
        self.assertEqual(summary["active_set_first_drained_job_index"], 0)
        self.assertEqual(summary["active_set_remaining_job_index"], 1)
        self.assertEqual(summary["active_set_post_remaining_fraction_max"], 0.4)
        self.assertEqual(
            summary["active_set_post_remaining_waiting_work_max"],
            0.0,
        )

    def test_post_drain_rejects_idle_share_while_work_is_waiting(self) -> None:
        summary = active_set_phase_summary(
            [
                {
                    "arrival_start_epoch_s": 10.0,
                    "completion_end_epoch_s": 20.0,
                    "runtime_job_id": "bulk",
                },
                {
                    "arrival_start_epoch_s": 15.0,
                    "completion_end_epoch_s": 25.0,
                    "runtime_job_id": "foreground",
                },
            ],
            [
                {
                    "observed_epoch_s": 12.0,
                    "request_limit": 100,
                    "work_limit": 100,
                    "active_by_job": '[["bulk", 90]]',
                    "active_work_by_job": '[["bulk", 90]]',
                    "waiting_work_by_job": '[["bulk", 100]]',
                },
                {
                    "observed_epoch_s": 17.0,
                    "request_limit": 100,
                    "work_limit": 100,
                    "active_by_job": (
                        '[["bulk", 45], ["foreground", 40]]'
                    ),
                    "active_work_by_job": (
                        '[["bulk", 45], ["foreground", 40]]'
                    ),
                    "waiting_work_by_job": (
                        '[["bulk", 20], ["foreground", 10]]'
                    ),
                },
                {
                    "observed_epoch_s": 22.0,
                    "request_limit": 100,
                    "work_limit": 100,
                    "active_by_job": '[["foreground", 40]]',
                    "active_work_by_job": '[["foreground", 40]]',
                    "active_requests": 40,
                    "active_work": 40,
                    "waiting_by_job": '[["foreground", 1]]',
                    "waiting_work_by_job": '[["foreground", 60]]',
                    "waiting_head_work_by_job": '[["foreground", 10]]',
                },
            ],
        )

        self.assertFalse(summary["active_set_mechanism_passed"])
        self.assertFalse(summary["active_set_post_work_conserving_passed"])
        self.assertEqual(
            summary["active_set_post_remaining_waiting_work_max"],
            60.0,
        )
        self.assertEqual(summary["active_set_post_fit_violation_samples"], 1)

    def test_active_set_mechanism_aggregates_endpoints_per_epoch(self) -> None:
        evidence = [
            {
                "arrival_start_epoch_s": 10.0,
                "completion_end_epoch_s": 30.0,
                "runtime_job_id": "bulk",
            },
            {
                "arrival_start_epoch_s": 15.0,
                "completion_end_epoch_s": 20.0,
                "runtime_job_id": "foreground",
            },
        ]
        samples = [
            {
                "observed_epoch_s": 12.0,
                "endpoint_id": "endpoint-0",
                "request_limit": 50,
                "work_limit": 50,
                "active_by_job": '[["bulk", 50]]',
                "active_work_by_job": '[["bulk", 50]]',
            },
            {
                "observed_epoch_s": 12.0,
                "endpoint_id": "endpoint-1",
                "request_limit": 50,
                "work_limit": 50,
                "active_by_job": '[["bulk", 50]]',
                "active_work_by_job": '[["bulk", 50]]',
            },
            {
                "observed_epoch_s": 17.0,
                "endpoint_id": "endpoint-0",
                "request_limit": 50,
                "work_limit": 50,
                "active_by_job": '[["bulk", 50]]',
                "active_work_by_job": '[["bulk", 50]]',
                "waiting_work_by_job": '[["bulk", 20]]',
            },
            {
                "observed_epoch_s": 17.0,
                "endpoint_id": "endpoint-1",
                "request_limit": 50,
                "work_limit": 50,
                "active_by_job": '[["foreground", 40]]',
                "active_work_by_job": '[["foreground", 40]]',
            },
            {
                "observed_epoch_s": 25.0,
                "endpoint_id": "endpoint-0",
                "request_limit": 50,
                "work_limit": 50,
                "active_by_job": '[["bulk", 50]]',
                "active_work_by_job": '[["bulk", 50]]',
            },
            {
                "observed_epoch_s": 25.0,
                "endpoint_id": "endpoint-1",
                "request_limit": 50,
                "work_limit": 50,
                "active_by_job": '[["bulk", 40]]',
                "active_work_by_job": '[["bulk", 40]]',
            },
        ]

        summary = active_set_phase_summary(evidence, samples)

        self.assertTrue(summary["active_set_mechanism_passed"])
        self.assertEqual(summary["active_set_overlap_samples"], 1)
        self.assertEqual(summary["active_set_bulk_reborrow_fraction_max"], 0.9)

    def test_credit_trace_reports_idle_and_borrowed_work(self) -> None:
        summary = shared_credit_trace_summary(
            [
                {"active_work": 0, "active_work_by_job": "[]"},
                {
                    "active_work": 90,
                    "active_work_by_job": json.dumps([["a", 70], ["b", 20]]),
                },
            ],
            work_limit_per_endpoint=100,
            job_count=2,
        )

        self.assertEqual(summary["credit_endpoint_idle_sample_fraction"], 0.5)
        self.assertEqual(summary["credit_idle_capacity_fraction_mean"], 0.55)
        self.assertEqual(summary["credit_borrowed_work_mean"], 10.0)

    def test_bounded_saor_event_ledger_catches_five_ms_transitions(self) -> None:
        events = [
            {
                "event_seq": 1,
                "event_time_s": 10.000,
                "endpoint_id": "task-0",
                "action": "grant",
                "tier": "slo_priority",
                "recovery_inflight_by_job": "[]",
                "constraint_conflict": False,
                "avoidable_idle": False,
                "foreign_grant_over_debt_critical": False,
            },
            {
                "event_seq": 2,
                "event_time_s": 10.005,
                "endpoint_id": "task-0",
                "action": "grant",
                "tier": "debt_recovery",
                "recovery_inflight_by_job": '[["bulk", "r-1"]]',
                "constraint_conflict": True,
                "avoidable_idle": False,
                "foreign_grant_over_debt_critical": False,
            },
        ]

        summary = bounded_saor_event_summary(events)

        self.assertEqual(summary["bounded_saor_event_status"], "ok:lossless_ledger")
        self.assertEqual(summary["bounded_saor_slo_priority_grants"], 1)
        self.assertEqual(summary["bounded_saor_debt_recovery_grants"], 1)
        self.assertEqual(summary["bounded_saor_constraint_conflicts"], 1)
        self.assertEqual(summary["bounded_saor_recovery_inflight_max"], 1)

    def test_bounded_saor_mechanism_is_unavailable_without_event_ledger(self) -> None:
        summary = bounded_saor_event_summary([])

        self.assertEqual(
            summary["bounded_saor_event_status"],
            "unavailable:no_event_ledger",
        )
        self.assertFalse(summary["bounded_saor_event_sequence_complete"])

    def test_vtc_templates_expand_unequal_job_counts(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
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
                            "token_budget": {"status": "passed"},
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
                "SHAREGPT_PROJECT_CALIBRATION_CONTRACT": str(selection),
                "SHAREGPT_PROJECT_K": "128",
                "DATABASE_URL": "postgresql://postgres:postgres@localhost/db",
                "COMPLETION_MODEL": "qwen",
                "MODEL_PATH": "/models/qwen",
                "VTC_ON_OFF_WORKLOAD": "vtc_on_off",
                "VTC_ON_OFF_CLIENT0_ROWS": "7",
                "VTC_ON_OFF_CLIENT1_ROWS": "11",
                "VTC_ON_OFF_CLIENT0_OFFSET_S": "0.25",
                "VTC_ON_OFF_CLIENT1_OFFSET_S": "0.5",
                "VTC_ON_OFF_CLIENT0_MANIFEST": "/tmp/client0.jsonl",
                "VTC_ON_OFF_CLIENT1_MANIFEST": "/tmp/client1.jsonl",
            }
            template = (
                Path(__file__).resolve().parents[3]
                / "deploy/autodl/vtc_compatible_on_off_overload.example.json"
            )
            with patch.dict(os.environ, environment, clear=True):
                config = load_config(template)

        self.assertEqual(config.scenarios[-1].rows_per_jobs, (7, 11))
        self.assertEqual(config.scenarios[-1].arrival_offsets_s, (0.25, 0.5))
        self.assertEqual(config.scenarios[-1].job_count, 2)

    def test_config_accepts_per_job_row_counts_for_vtc_traces(self) -> None:
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "experiment_id": "vtc-compatible",
                        "seed": 1,
                        "warmup_runs_per_scenario": 0,
                        "formal_repeats": 1,
                        "endpoint_ids": ["endpoint-0", "endpoint-1"],
                        "request_limit_per_endpoint": 8,
                        "work_limit_per_endpoint": 1024,
                        "credit_quantum": 128,
                        "common_args": ["--arrival-replay", "--executor", "ray_actor"],
                        "scenarios": [
                            {
                                "scenario_id": "unequal",
                                "policy": "static_partition",
                                "job_count": 2,
                                "rows_per_jobs": [7, 11],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

        scenario = config.scenarios[0]
        self.assertIsNone(scenario.rows_per_job)
        self.assertEqual(scenario.rows_per_jobs, (7, 11))
        self.assertEqual(scenario.row_count(0), 7)
        self.assertEqual(scenario.row_count(1), 11)

    def test_config_accepts_warmup_only_effect_range_gate(self) -> None:
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "experiment_id": "effect-range-gate",
                        "seed": 1,
                        "warmup_runs_per_scenario": 1,
                        "formal_repeats": 0,
                        "endpoint_ids": ["endpoint-0", "endpoint-1"],
                        "request_limit_per_endpoint": 8,
                        "work_limit_per_endpoint": 4096,
                        "credit_quantum": 512,
                        "common_args": [
                            "--arrival-replay",
                            "--executor",
                            "ray_actor",
                        ],
                        "scenarios": [
                            {
                                "scenario_id": "shared_gate",
                                "policy": "shared_fifo",
                                "job_count": 1,
                                "rows_per_job": 1,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

        self.assertEqual(config.warmup_runs_per_scenario, 1)
        self.assertEqual(config.formal_repeats, 0)

    def test_config_rejects_empty_effect_range_schedule(self) -> None:
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "experiment_id": "empty-gate",
                        "seed": 1,
                        "warmup_runs_per_scenario": 0,
                        "formal_repeats": 0,
                        "endpoint_ids": ["endpoint-0"],
                        "request_limit_per_endpoint": 1,
                        "work_limit_per_endpoint": 512,
                        "credit_quantum": 512,
                        "common_args": [
                            "--arrival-replay",
                            "--executor",
                            "ray_actor",
                        ],
                        "scenarios": [
                            {
                                "scenario_id": "empty",
                                "policy": "independent_full",
                                "job_count": 1,
                                "rows_per_job": 1,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "at least one warmup"):
                load_config(path)

    def test_all_at_t0_multijob_template_keeps_matched_project_limits(self) -> None:
        with TemporaryDirectory() as directory:
            selection = Path(directory) / "selection.json"
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
                            "token_budget": {"status": "passed"},
                            "actor_pool": {"status": "passed"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            env = {
                "DATABASE_URL": "postgresql://example",
                "VLLM_VERSION": "0.25.1",
                "VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
                "VLLM_MAX_NUM_SEQS": "256",
                "COMPLETION_MODEL": "qwen2.5-7b",
                "MODEL_PATH": "/models/qwen2.5-7b",
                "OPENING_SHORT_JOB_MANIFEST": "/tmp/short.jsonl",
                "OPENING_LONG_JOB_MANIFEST": "/tmp/long.jsonl",
                "OPENING_MULTIJOB_OFFSET_S": "5",
                "SHAREGPT_PROJECT_K": "128",
                "SHAREGPT_PROJECT_CALIBRATION_CONTRACT": str(selection),
            }
            with patch.dict(os.environ, env, clear=True):
                config = load_config(
                    CODE_ROOT.parent
                    / "deploy"
                    / "autodl"
                    / "opening_project_multijob_all_at_t0_diagnostic.example.json"
                )
                half_pool = load_config(
                    CODE_ROOT.parent
                    / "deploy"
                    / "autodl"
                    / "opening_project_short_half_pool_all_at_t0_diagnostic.example.json"
                )

        self.assertEqual(config.request_limit_per_endpoint, 128)
        self.assertEqual(config.work_limit_per_endpoint, 65536)
        self.assertIn("--arrival-replay", config.common_args)
        scale = config.common_args.index("--arrival-time-scale")
        self.assertEqual(config.common_args[scale + 1], "0.000000001")
        self.assertEqual(
            [scenario.scenario_id for scenario in config.scenarios],
            [
                "single_short_full_pool_all_at_t0",
                "staggered_static_partition_all_at_t0",
                "staggered_shared_work_all_at_t0",
            ],
        )
        self.assertEqual(config.scenarios[0].job_count, 1)
        self.assertEqual(config.scenarios[1].arrival_offsets_s, (0.0, 5.0))

        self.assertEqual(len(half_pool.scenarios), 1)
        self.assertEqual(
            half_pool.scenarios[0].scenario_id,
            "single_short_half_pool_all_at_t0",
        )
        self.assertEqual(half_pool.scenarios[0].static_partition_count, 2)
        self.assertEqual(half_pool.request_limit_per_endpoint, 128)
        self.assertEqual(half_pool.work_limit_per_endpoint, 65536)
        half_scale = half_pool.common_args.index("--arrival-time-scale")
        self.assertEqual(half_pool.common_args[half_scale + 1], "0.000000001")

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

        with patch(
            "src.experiments.shared_vllm.runtime."
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
            policy="drr",
            record_ready_lifecycle_events=False,
        )
        self.assertEqual(
            [call.args for call in client.snapshot.call_args_list],
            [("task-0",), ("task-1",)],
        )
        self.assertIs(observer.actor, client.actor)

    def test_credit_observer_cleanup_preserves_primary_failure(self) -> None:
        observer = shared_vllm._RayCreditObserver.__new__(
            shared_vllm._RayCreditObserver
        )
        observer.ray = MagicMock()
        observer.actor = object()
        observer.ray.kill.side_effect = RuntimeError("ray unavailable")

        with self.assertWarnsRegex(RuntimeWarning, "cleanup failed"):
            observer.cleanup()

        self.assertIsNone(observer.actor)

    def test_credit_observer_drains_lossless_events(self) -> None:
        observer = shared_vllm._RayCreditObserver.__new__(
            shared_vllm._RayCreditObserver
        )
        observer.ray = MagicMock()
        observer.endpoint_ids = ("task-0",)
        observer.actor = MagicMock()
        event = SaorReleaseEvent(
            event_seq=1,
            event_time_s=12.0,
            event_epoch_s=112.0,
            endpoint_id="task-0",
            action="grant",
            tier="slo_priority",
        )
        observer.actor.drain_release_events.remote.return_value = (event,)
        observer.ray.get.side_effect = lambda value: value

        rows = observer.drain_release_events(100.0)

        self.assertEqual(rows[0]["event_seq"], 1)
        self.assertEqual(rows[0]["tier"], "slo_priority")
        self.assertEqual(rows[0]["schema_version"], 2)
        self.assertIn("observed_epoch_s", rows[0])

    def test_bounded_ready_join_rejects_foreign_fallback(self) -> None:
        job_evidence = [
            {
                "runtime_job_id": "bulk",
                "ready_lifecycle_complete": True,
                "max_ready_requests_seen": 1,
                "max_ready_work_seen": 10,
                "max_ready_payload_bytes_seen": 20,
                "ready_lifecycle_rows": [
                    {
                        "request_id": "bulk-r0",
                        "endpoint_id": "task-0",
                        "registered_epoch_s": 99.0,
                        "granted_epoch_s": 101.0,
                    }
                ],
            },
            {
                "runtime_job_id": "foreground",
                "ready_lifecycle_complete": True,
                "max_ready_requests_seen": 2,
                "max_ready_work_seen": 100,
                "max_ready_payload_bytes_seen": 200,
                "ready_lifecycle_rows": [
                    {
                        "request_id": "foreground-r0",
                        "endpoint_id": "task-0",
                        "registered_epoch_s": 100.0,
                        "granted_epoch_s": 102.0,
                    }
                ],
            },
        ]
        events = [
            {
                "action": "register",
                "tier": "ready_registration",
                "event_seq": 1,
                "event_epoch_s": 99.0,
                "endpoint_id": "task-0",
                "selected_job_id": "bulk",
                "selected_request_id": "bulk-r0",
            },
            {
                "action": "register",
                "tier": "ready_registration",
                "event_seq": 2,
                "event_epoch_s": 100.0,
                "endpoint_id": "task-0",
                "selected_job_id": "foreground",
                "selected_request_id": "foreground-r0",
            },
            {
                "action": "grant",
                "tier": "saor_fallback",
                "event_seq": 3,
                "event_epoch_s": 101.0,
                "endpoint_id": "task-0",
                "selected_job_id": "bulk",
                "selected_request_id": "bulk-r0",
            },
            {
                "action": "grant",
                "tier": "slo_priority",
                "event_seq": 4,
                "event_epoch_s": 102.0,
                "endpoint_id": "task-0",
                "selected_job_id": "foreground",
                "selected_request_id": "foreground-r0",
            },
        ]

        summary = shared_vllm.bounded_ready_event_summary(
            events,
            job_evidence,
            foreground_job_index=1,
        )

        self.assertEqual(
            summary["bounded_ready_event_status"],
            "ok:actor_event_join",
        )
        self.assertEqual(summary["bounded_ready_foreign_fallback_events"], 1)
        self.assertEqual(
            summary["bounded_ready_foreground_max_ready_requests_seen"],
            2,
        )

    def test_bounded_ready_join_requires_every_job_lifecycle(self) -> None:
        evidence = [
            {
                "runtime_job_id": "bulk",
                "ready_lifecycle_complete": True,
                "max_ready_requests_seen": 2,
                "max_ready_work_seen": 20,
                "max_ready_payload_bytes_seen": 40,
                "ready_lifecycle_rows": [
                    {
                        "request_id": "bulk-r0",
                        "endpoint_id": "task-0",
                        "registered_epoch_s": 100.0,
                        "granted_epoch_s": 101.0,
                    }
                ],
            },
            {
                "runtime_job_id": "foreground",
                "ready_lifecycle_complete": False,
                "max_ready_requests_seen": 0,
                "max_ready_work_seen": 0,
                "max_ready_payload_bytes_seen": 0,
                "ready_lifecycle_rows": [],
            },
        ]

        summary = shared_vllm.bounded_ready_event_summary(
            [], evidence, foreground_job_index=1
        )

        self.assertFalse(summary["bounded_ready_lifecycle_complete"])
        self.assertEqual(summary["bounded_ready_jobs_with_intervals"], 1)

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

    def test_shared_config_uses_same_calibration_contract(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            selection_path = root / "selection.json"
            selection_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "ready",
                        "selection": {
                            "best_token_budget": 32768,
                            "project_static_k_per_endpoint": 256,
                            "project_active_work_per_endpoint": 65536,
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
            payload = self._config_payload()
            payload["request_limit_per_endpoint"] = "${PROJECT_K}"
            payload["work_limit_per_endpoint"] = "${PROJECT_WORK}"
            payload["calibration_contract"] = {
                "path": "${SELECTION_PATH}",
                "expected": {
                    "best_token_budget": "${BEST_TOKEN_BUDGET}",
                    "project_static_k_per_endpoint": "${PROJECT_K}",
                    "project_active_work_per_endpoint": "${PROJECT_WORK}",
                },
            }
            config_path = root / "config.json"
            config_path.write_text(json.dumps(payload), encoding="utf-8")

            with patch.dict(
                os.environ,
                {
                    "SELECTION_PATH": str(selection_path),
                    "BEST_TOKEN_BUDGET": "32768",
                    "PROJECT_K": "256",
                    "PROJECT_WORK": "65536",
                },
                clear=True,
            ):
                config = load_config(config_path)

            self.assertEqual(config.request_limit_per_endpoint, 256)
            self.assertEqual(config.work_limit_per_endpoint, 65536)
            self.assertIsNotNone(config.calibration_contract)

    def test_scenario_pins_distinct_job_manifests_with_zero_offsets(self) -> None:
        payload = self._config_payload(
            scenarios=[
                {
                    "scenario_id": "heterogeneous_j2",
                    "policy": "shared_drr",
                    "job_count": 2,
                    "rows_per_job": 64,
                    "weights": [1, 3],
                    "arrival_offsets_s": [0.0, 15.0],
                    "source_row_offsets": [0, 0],
                    "request_manifests": [
                        "${SHORT_MANIFEST}",
                        "${LONG_MANIFEST}",
                    ],
                }
            ]
        )
        with (
            patch.object(Path, "read_text", return_value=json.dumps(payload)),
            patch.dict(
                os.environ,
                {
                    "SHORT_MANIFEST": "/evidence/short.jsonl",
                    "LONG_MANIFEST": "/evidence/long.jsonl",
                },
                clear=True,
            ),
        ):
            config = load_config(Path("config.json"))

        scenario = config.scenarios[0]
        self.assertEqual(scenario.source_row_offsets, (0, 0))
        self.assertEqual(
            scenario.request_manifests,
            ("/evidence/short.jsonl", "/evidence/long.jsonl"),
        )
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
        command = build_job_command(
            options,
            config,
            scenario,
            GroupRunIdentity("formal", 1, 0),
            job_index=1,
            start_epoch_s=100.0,
            coordinator_name="credits",
        )
        self.assertEqual(self._flag_value(command, "--source-row-offset"), "0")
        self.assertEqual(
            self._flag_value(command, "--request-manifest"),
            "/evidence/long.jsonl",
        )
        self.assertEqual(
            self._flag_value(command, "--arrival-replay-start-epoch-s"),
            "115.0",
        )
        self.assertEqual(
            self._flag_value(command, "--shared-credit-job-weight"),
            "3",
        )

    def test_foreground_strict_priority_is_separate_from_fairness_weight(
        self,
    ) -> None:
        payload = self._config_payload(
            scenarios=[
                {
                    "scenario_id": "strict_priority_diagnostic",
                    "policy": "foreground_strict_priority",
                    "job_count": 2,
                    "rows_per_job": 64,
                    "weights": [1, 1],
                    "arrival_offsets_s": [0.0, 5.0],
                }
            ]
        )
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(payload),
        ):
            config = load_config(Path("config.json"))
        scenario = config.scenarios[0]
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

        bulk = build_job_command(
            options,
            config,
            scenario,
            GroupRunIdentity("formal", 1, 0),
            job_index=0,
            start_epoch_s=100.0,
            coordinator_name="credits",
        )
        foreground = build_job_command(
            options,
            config,
            scenario,
            GroupRunIdentity("formal", 1, 0),
            job_index=1,
            start_epoch_s=100.0,
            coordinator_name="credits",
        )

        self.assertEqual(
            self._flag_value(bulk, "--shared-credit-policy"),
            "strict_priority",
        )
        self.assertEqual(
            self._flag_value(bulk, "--shared-credit-job-priority"),
            "0",
        )
        self.assertEqual(
            self._flag_value(foreground, "--shared-credit-job-priority"),
            "1",
        )
        self.assertEqual(
            self._flag_value(foreground, "--shared-credit-job-weight"),
            "1",
        )

    def test_foreground_strict_priority_requires_unique_later_job(self) -> None:
        payload = self._config_payload(
            scenarios=[
                {
                    "scenario_id": "invalid_priority_diagnostic",
                    "policy": "foreground_strict_priority",
                    "job_count": 2,
                    "rows_per_job": 64,
                    "arrival_offsets_s": [0.0, 0.0],
                }
            ]
        )
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(payload),
        ):
            with self.assertRaisesRegex(ValueError, "unique later foreground"):
                load_config(Path("config.json"))

    def test_arrival_offset_expands_numeric_environment_scalar(self) -> None:
        payload = self._config_payload(
            scenarios=[
                {
                    "scenario_id": "forced_overlap",
                    "policy": "shared_drr",
                    "job_count": 2,
                    "rows_per_job": 64,
                    "arrival_offsets_s": [0.0, "${JOB_OFFSET_S}"],
                }
            ]
        )
        with (
            patch.object(Path, "read_text", return_value=json.dumps(payload)),
            patch.dict(os.environ, {"JOB_OFFSET_S": "5"}, clear=True),
        ):
            config = load_config(Path("config.json"))

        self.assertEqual(config.scenarios[0].arrival_offsets_s, (0.0, 5.0))

    def test_external_vtc_uses_completion_corrected_credit_policy(self) -> None:
        payload = self._config_payload(
            scenarios=[
                {
                    "scenario_id": "external_vtc",
                    "policy": "external_vtc",
                    "job_count": 2,
                    "rows_per_job": 64,
                }
            ]
        )
        with patch.object(Path, "read_text", return_value=json.dumps(payload)):
            config = load_config(Path("config.json"))
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

        command = build_job_command(
            options,
            config,
            config.scenarios[0],
            GroupRunIdentity("formal", 1, 0),
            job_index=0,
            start_epoch_s=100.0,
            coordinator_name="credits",
        )

        self.assertEqual(
            self._flag_value(command, "--shared-credit-policy"),
            "vtc",
        )

    def test_scenario_can_freeze_a_distinct_static_capacity_arm(self) -> None:
        payload = self._config_payload(
            scenarios=[
                {
                    "scenario_id": "frozen_vtc",
                    "policy": "external_vtc",
                    "job_count": 2,
                    "rows_per_job": 64,
                    "request_limit_per_endpoint": 128,
                    "work_limit_per_endpoint": 131072,
                }
            ]
        )
        payload["request_limit_per_endpoint"] = 96
        payload["work_limit_per_endpoint"] = 98304
        with patch.object(Path, "read_text", return_value=json.dumps(payload)):
            config = load_config(Path("config.json"))
        scenario = config.scenarios[0]
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

        command = build_job_command(
            options,
            config,
            scenario,
            GroupRunIdentity("formal", 1, 0),
            job_index=0,
            start_epoch_s=100.0,
            coordinator_name="credits",
        )

        self.assertEqual(
            self._flag_value(command, "--shared-credit-request-limit"),
            "128",
        )
        self.assertEqual(
            self._flag_value(command, "--shared-credit-work-limit"),
            "131072",
        )

    def test_state_aware_policy_requires_bounded_calibrated_control(self) -> None:
        payload = self._config_payload(
            scenarios=[
                {
                    "scenario_id": "adaptive",
                    "policy": "state_aware_adaptive",
                    "job_count": 2,
                    "rows_per_job": 64,
                }
            ]
        )
        payload["request_limit_per_endpoint"] = 96
        payload["work_limit_per_endpoint"] = 98304
        with patch.object(Path, "read_text", return_value=json.dumps(payload)):
            with self.assertRaisesRegex(ValueError, "state_aware_control"):
                load_config(Path("config.json"))

        payload["state_aware_control"] = {
            "request_candidates": [96, 128, 160],
            "work_candidates": [98304, 131072, 131072],
            "initial_request_limit": payload["request_limit_per_endpoint"],
            "fallback_request_limit": 128,
            "fallback_work_limit": 131072,
            "target_service_rate_tokens_s_per_endpoint": 7600.0,
            "rate_ewma_alpha": 0.3,
            "congestion_kv_usage": 0.85,
            "consecutive_samples": 8,
            "increase_consecutive_samples": 2,
            "cooldown_samples": 8,
            "max_state_age_s": 1.0,
        }
        with patch.object(Path, "read_text", return_value=json.dumps(payload)):
            config = load_config(Path("config.json"))

        self.assertEqual(
            config.state_aware_control.request_candidates,
            (96, 128, 160),
        )
        self.assertEqual(config.state_aware_control.fallback_request_limit, 128)
        self.assertEqual(
            config.state_aware_control.increase_consecutive_samples,
            2,
        )

        scenario = config.scenarios[0]
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
        command = build_job_command(
            options,
            config,
            scenario,
            GroupRunIdentity("formal", 1, 0),
            job_index=0,
            start_epoch_s=100.0,
            coordinator_name="credits",
        )
        self.assertEqual(self._flag_value(command, "--max-inflight"), "160")
        self.assertEqual(
            self._flag_value(command, "--max-active-work-per-endpoint"),
            "131072",
        )
        self.assertEqual(
            self._flag_value(command, "--shared-credit-request-limit"),
            "96",
        )

    def test_arrival_offset_rejects_missing_environment_scalar(self) -> None:
        payload = self._config_payload(
            scenarios=[
                {
                    "scenario_id": "forced_overlap",
                    "policy": "shared_drr",
                    "job_count": 2,
                    "rows_per_job": 64,
                    "arrival_offsets_s": [0.0, "${JOB_OFFSET_S}"],
                }
            ]
        )
        with (
            patch.object(Path, "read_text", return_value=json.dumps(payload)),
            patch.dict(os.environ, {}, clear=True),
        ):
            with self.assertRaisesRegex(ValueError, "JOB_OFFSET_S"):
                load_config(Path("config.json"))

    def test_single_job_can_retain_one_of_two_static_partitions(self) -> None:
        payload = self._config_payload(
            scenarios=[
                {
                    "scenario_id": "single_short_half_pool",
                    "policy": "static_partition",
                    "job_count": 1,
                    "static_partition_count": 2,
                    "rows_per_job": 64,
                }
            ]
        )
        with patch.object(Path, "read_text", return_value=json.dumps(payload)):
            config = load_config(Path("config.json"))

        scenario = config.scenarios[0]
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
        command = build_job_command(
            options,
            config,
            scenario,
            GroupRunIdentity("formal", 1, 0),
            job_index=0,
            start_epoch_s=100.0,
            coordinator_name="",
        )

        self.assertEqual(scenario.static_partition_count, 2)
        self.assertEqual(self._flag_value(command, "--max-inflight"), "128")
        self.assertEqual(
            self._flag_value(command, "--max-active-work-per-endpoint"),
            "32768",
        )

    def test_static_partition_count_rejects_invalid_policy_or_underallocation(self) -> None:
        invalid = (
            {
                "scenario_id": "shared_invalid",
                "policy": "shared_drr",
                "job_count": 1,
                "static_partition_count": 2,
                "rows_per_job": 64,
            },
            {
                "scenario_id": "static_invalid",
                "policy": "static_partition",
                "job_count": 2,
                "static_partition_count": 1,
                "rows_per_job": 64,
            },
        )
        for scenario in invalid:
            with self.subTest(scenario=scenario["scenario_id"]):
                payload = self._config_payload(scenarios=[scenario])
                with patch.object(
                    Path,
                    "read_text",
                    return_value=json.dumps(payload),
                ):
                    with self.assertRaises(ValueError):
                        load_config(Path("config.json"))

    def test_manifest_selected_jobs_reject_nonzero_source_offsets(self) -> None:
        payload = self._config_payload(
            scenarios=[
                {
                    "scenario_id": "heterogeneous_j2",
                    "policy": "shared_drr",
                    "job_count": 2,
                    "rows_per_job": 64,
                    "source_row_offsets": [0, 64],
                    "request_manifests": ["short.jsonl", "long.jsonl"],
                }
            ]
        )
        with patch.object(Path, "read_text", return_value=json.dumps(payload)):
            with self.assertRaisesRegex(ValueError, "zero source_row_offsets"):
                load_config(Path("config.json"))
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

    def test_config_rejects_four_job_ray_task_worker_explosion(self) -> None:
        payload = self._config_payload(
            common_args=["--arrival-replay", "--executor", "ray_task"],
            scenarios=[
                {
                    "scenario_id": "j4",
                    "policy": "shared_drr",
                    "job_count": 4,
                    "rows_per_job": 64,
                }
            ],
        )
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(payload),
        ):
            with self.assertRaisesRegex(
                ValueError,
                "four-or-more-job.*ray_actor",
            ):
                load_config(Path("config.json"))

    def test_config_accepts_four_job_bounded_actor_pool(self) -> None:
        payload = self._config_payload(
            common_args=[
                "--arrival-replay",
                "--executor",
                "ray_actor",
                "--actor-workers-per-endpoint",
                "1",
                "--ray-actor-max-concurrency",
                "256",
            ],
            scenarios=[
                {
                    "scenario_id": "j4",
                    "policy": "shared_drr",
                    "job_count": 4,
                    "rows_per_job": 64,
                }
            ],
        )
        payload["endpoint_ids"] = ["endpoint-0", "endpoint-1"]
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(payload),
        ):
            config = load_config(Path("config.json"))

        self.assertEqual(config.scenarios[0].job_count, 4)

    def test_config_rejects_actor_endpoint_id_contract_mismatch(self) -> None:
        payload = self._config_payload(
            common_args=[
                "--arrival-replay",
                "--executor",
                "ray_actor",
            ],
        )
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(payload),
        ):
            with self.assertRaisesRegex(
                ValueError,
                "ray_actor endpoint_ids",
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
                "host_cpu_busy_cores": float(gpu) / 10,
                "host_cpu_per_core_max_pct": gpu,
                "host_memory_used_pct": 25 + epoch,
                "host_memory_available_mib": 1000 - 100 * epoch,
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
        self.assertEqual(summary["host_cpu_busy_cores_mean"], 7.5)
        self.assertEqual(summary["host_memory_available_mib_max"], 900.0)

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
                "actor_worker_failures": "0;0",
                "arrival_replay_start_epoch_s": "100.0",
                "arrival_replay_observed_start_epoch_s": "100.0",
                "max_ready_requests_seen": "3",
                "max_ready_work_seen": "90",
            }
        ]
        submission_rows = [
            {
                "submission_id": f"request-{index}",
                "endpoint_id": f"task-{index % 2}",
                "submit_epoch_s": str(index + 0.1),
            }
            for index in range(4)
        ]

        with patch(
            "src.experiments.shared_vllm.evidence._read_csv",
            side_effect=[summary_rows, request_rows, submission_rows],
        ):
            evidence = shared_vllm._validate_job_evidence(
                options,
                scenario,
                GroupRunIdentity("formal", 1, 0),
                0,
            )

        self.assertEqual(evidence["p99_s"], 100.0)
        self.assertEqual(evidence["actor_worker_failures"], 0)
        self.assertEqual(evidence["max_ready_requests_seen"], 3)
        self.assertEqual(evidence["max_ready_work_seen"], 90)

    def test_job_evidence_joins_ready_lifecycle_to_request_submit(self) -> None:
        options = RunnerOptions(
            config_path=Path("config.json"),
            profiler_path=Path("profile.py"),
            python_executable=Path(sys.executable),
            output_dir=Path("out"),
            health_url="http://health",
            metrics_urls=("http://metrics",),
            ray_address="127.0.0.1:6380",
            idle_timeout_s=1.0,
        )
        scenario = SharedVllmScenario(
            scenario_id="bounded-ready",
            policy="saor_bounded_ready",
            job_count=1,
            rows_per_job=1,
            weights=(1,),
            arrival_offsets_s=(0.0,),
        )
        summary_rows = [{
            "status": "ok",
            "total_rows": "1",
            "actor_worker_failures": "0",
            "max_ready_requests_seen": "2",
            "max_ready_work_seen": "90",
        }]
        request_rows = [{
            "request_id": "request-0",
            "submission_id": "submission-0",
            "status": "completed",
            "error_type": "",
            "arrival_epoch_s": "99.0",
            "submit_epoch_s": "100.3",
            "completion_epoch_s": "101.0",
            "e2e_s": "2.0",
            "slo_met": "True",
            "prompt_tokens": "10",
            "client_estimated_output_tokens": "20",
            "estimated_output_tokens": "20",
            "endpoint_id": "task-0",
        }]
        # The production submission schema intentionally has no
        # submit_epoch_s; that timestamp is owned by the request trace.
        submission_rows = [{
            "submission_id": "submission-0",
            "endpoint_id": "task-0",
            "ready_epoch_s": "100.0",
            "credit_registered_epoch_s": "100.1",
            "credit_granted_epoch_s": "100.2",
        }]

        with patch(
            "src.experiments.shared_vllm.evidence._read_csv",
            side_effect=[summary_rows, request_rows, submission_rows],
        ):
            evidence = shared_vllm._validate_job_evidence(
                options,
                scenario,
                GroupRunIdentity("formal", 1, 0),
                0,
            )

        self.assertTrue(evidence["ready_lifecycle_complete"])
        self.assertEqual(
            evidence["ready_lifecycle_rows"],
            [{
                "request_id": "submission-0",
                "endpoint_id": "task-0",
                "ready_epoch_s": 100.0,
                "registered_epoch_s": 100.1,
                "granted_epoch_s": 100.2,
                "submit_epoch_s": 100.3,
                "completion_epoch_s": 101.0,
                "actual_work": 30,
            }],
        )

    def test_jain_fairness_handles_equal_weight_and_zero_service(self) -> None:
        self.assertEqual(jain_fairness([100.0, 100.0]), 1.0)
        self.assertAlmostEqual(jain_fairness([100.0, 0.0]), 0.5)
        self.assertEqual(jain_fairness([0.0, 0.0]), 0.0)

    def test_normalized_service_uses_achieved_rate_not_offered_work(
        self,
    ) -> None:
        evidence = [
            {"predicted_work": 800, "actual_work": 1000, "jct_s": 10.0},
            {"predicted_work": 1200, "actual_work": 1000, "jct_s": 20.0},
        ]

        rates = normalized_job_service_rates(evidence, (1, 1))

        self.assertEqual(rates, [100.0, 50.0])
        self.assertAlmostEqual(jain_fairness(rates), 0.9)

    def test_cumulative_service_disparity_uses_actual_weighted_work(self) -> None:
        metrics = cumulative_service_disparity(
            [
                {
                    "actual_work": 1000,
                    "arrival_start_epoch_s": 0.0,
                    "completion_end_epoch_s": 2.0,
                    "service_completion_events": [(1.0, 1000)],
                },
                {
                    "actual_work": 1500,
                    "arrival_start_epoch_s": 0.0,
                    "completion_end_epoch_s": 2.0,
                    "service_completion_events": [(2.0, 1500)],
                },
            ],
            (1, 2),
        )

        self.assertEqual(metrics["normalized_cumulative_service_min"], 750.0)
        self.assertEqual(metrics["normalized_cumulative_service_max"], 1000.0)
        self.assertEqual(
            metrics["normalized_cumulative_service_disparity"],
            250.0,
        )
        self.assertEqual(metrics["overlap_service_disparity_samples"], 2)
        self.assertEqual(
            metrics["max_overlap_normalized_service_disparity"],
            1000.0,
        )

    def test_completion_accounted_fairness_uses_registered_backlog(self) -> None:
        evidence = [
            {
                "ready_lifecycle_complete": True,
                "ready_lifecycle_rows": [
                    {
                        "registered_epoch_s": 0.0,
                        "completion_epoch_s": 2.0,
                        "actual_work": 100.0,
                    },
                    {
                        "registered_epoch_s": 0.0,
                        "completion_epoch_s": 4.0,
                        "actual_work": 100.0,
                    },
                ],
            },
            {
                "ready_lifecycle_complete": True,
                "ready_lifecycle_rows": [
                    {
                        "registered_epoch_s": 0.0,
                        "completion_epoch_s": 3.0,
                        "actual_work": 100.0,
                    },
                ],
            },
        ]

        metrics = completion_accounted_service_fairness(evidence, (1, 1))

        self.assertEqual(
            metrics["completion_service_lag_status"],
            "ok:registered_backlog_completion_accounted_empirical",
        )
        self.assertEqual(metrics["completion_service_lag_samples"], 4)
        self.assertEqual(metrics["completion_service_lag_max_work"], 50.0)
        self.assertEqual(metrics["completion_longest_no_service_s"], 3.0)

    def test_completion_accounted_fairness_requires_ready_lifecycle(self) -> None:
        metrics = completion_accounted_service_fairness(
            [
                {
                    "ready_lifecycle_complete": False,
                    "ready_lifecycle_rows": [],
                },
                {
                    "ready_lifecycle_complete": False,
                    "ready_lifecycle_rows": [],
                },
            ],
            (1, 1),
        )

        self.assertIn("unavailable", metrics["completion_service_lag_status"])

    def test_replay_start_validation_rejects_late_or_skewed_jobs(self) -> None:
        skewed_barrier = [
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
                skewed_barrier,
                expected_start_epoch_s=100.0,
                arrival_offsets_s=(0.0, 0.0),
                max_lateness_s=2.0,
                max_skew_s=0.5,
            )
        with self.assertRaisesRegex(RuntimeError, "start deadline"):
            _validate_replay_starts(
                skewed_barrier,
                expected_start_epoch_s=100.0,
                arrival_offsets_s=(0.0, 0.0),
                max_lateness_s=0.5,
                max_skew_s=1.0,
            )

    def test_replay_start_validation_allows_scheduler_submit_skew(self) -> None:
        evidence = [
            {
                "replay_configured_start_epoch_s": 100.0,
                "replay_observed_start_epoch_s": 100.1,
                "replay_actual_submit_start_epoch_s": 100.1,
            },
            {
                "replay_configured_start_epoch_s": 105.0,
                "replay_observed_start_epoch_s": 105.1,
                "replay_actual_submit_start_epoch_s": 113.8,
            },
        ]

        _validate_replay_starts(
            evidence,
            expected_start_epoch_s=100.0,
            arrival_offsets_s=(0.0, 5.0),
            max_lateness_s=2.0,
            max_skew_s=0.5,
        )

    def test_replay_start_validation_rejects_submit_before_barrier(self) -> None:
        evidence = [
            {
                "replay_configured_start_epoch_s": 100.0,
                "replay_observed_start_epoch_s": 100.1,
                "replay_actual_submit_start_epoch_s": 100.0,
            },
        ]

        with self.assertRaisesRegex(RuntimeError, "before crossing"):
            _validate_replay_starts(
                evidence,
                expected_start_epoch_s=100.0,
                arrival_offsets_s=(0.0,),
                max_lateness_s=2.0,
                max_skew_s=0.5,
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

    def test_runner_topology_accepts_matching_profiler_urls(self) -> None:
        metrics = ("http://metrics0", "http://metrics1")
        payload = self._config_payload(
            common_args=[
                "--arrival-replay",
                "--model-metrics-urls",
                ",".join(metrics),
                "--completion-endpoint-urls",
                "http://endpoint0,http://endpoint1",
            ]
        )
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(payload),
        ):
            config = load_config(Path("config.json"))
        options = RunnerOptions(
            config_path=Path("config.json"),
            profiler_path=Path("profile.py"),
            python_executable=Path(sys.executable),
            output_dir=Path("output"),
            health_url="http://health",
            metrics_urls=metrics,
            ray_address="127.0.0.1:6379",
            idle_timeout_s=1.0,
        )

        _validate_runner_topology(options, config)

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

    def test_resume_compares_json_normalized_config_values(self) -> None:
        manifest = {
            "schema_version": 1,
            "experiment_id": "experiment",
            "config_fingerprint": "fingerprint",
            "repository_commit": "commit",
            "run_instance_id": "run",
            "redacted_config": {
                "endpoint_ids": ["endpoint-0", "endpoint-1"],
                "scenarios": [{"weights": [1, 1]}],
            },
            "schedule": [],
            "completed_runs": [],
            "incidents": [],
        }
        expected = {
            **manifest,
            "redacted_config": {
                "endpoint_ids": ("endpoint-0", "endpoint-1"),
                "scenarios": [{"weights": (1, 1)}],
            },
        }
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(manifest),
        ), patch.object(Path, "exists", return_value=True):
            loaded = _load_resume_manifest(Path("manifest.json"), expected)

        self.assertEqual(loaded, manifest)

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
                    "src.experiments.shared_vllm.runner.subprocess.Popen",
                    return_value=process,
                ) as popen,
                patch(
                    "src.experiments.shared_vllm.runner.scrape_prometheus_metrics",
                    return_value={
                        "vllm:prompt_tokens_total": 1.0,
                        "vllm:generation_tokens_total": 1.0,
                        "vllm:request_success_total": 1.0,
                        "vllm:estimated_flops_per_gpu_total": 1.0,
                    },
                ),
                patch(
                    "src.experiments.shared_vllm.runner._validate_job_evidence",
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
            self.assertTrue(
                (
                    options.output_dir
                    / "traces"
                    / "000_formal_1_test.release_events.csv"
                ).exists()
            )
            self.assertIn(
                "exactly-once failed",
                failure.read_text(encoding="utf-8"),
            )

    def test_shared_credit_actor_is_cleaned_after_group_failure(self) -> None:
        with TemporaryDirectory() as temp_dir:
            options, config, scenario = self._group_fixture(Path(temp_dir))
            scenario = replace(scenario, policy="shared_drr")
            observer = MagicMock()
            observer.prewarm.side_effect = RuntimeError("prewarm failed")
            observer.sample.return_value = []
            observer.final_snapshots.return_value = []

            with patch(
                "src.experiments.shared_vllm.runner._RayCreditObserver",
                return_value=observer,
            ):
                with self.assertRaisesRegex(RuntimeError, "prewarm failed"):
                    _run_group(
                        options,
                        config,
                        scenario,
                        GroupRunIdentity("formal", 1, 0),
                    )

            observer.cleanup.assert_called_once_with()

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
                    "src.experiments.shared_vllm.runner.time.time",
                    side_effect=[95.0, 95.0, 101.0],
                ),
                patch(
                    "src.experiments.shared_vllm.runner.subprocess.Popen",
                    return_value=process,
                ),
                patch(
                    "src.experiments.shared_vllm.runner.scrape_prometheus_metrics",
                    return_value={
                        "vllm:prompt_tokens_total": 1.0,
                        "vllm:generation_tokens_total": 1.0,
                        "vllm:request_success_total": 1.0,
                        "vllm:estimated_flops_per_gpu_total": 1.0,
                    },
                ),
                patch(
                    "src.experiments.shared_vllm.runner._validate_job_evidence",
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
            _validate_final_credit(config, config.scenarios[0], snapshots)

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
