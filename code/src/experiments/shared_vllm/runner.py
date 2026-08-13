"""Concurrent shared-vLLM group runner and lifecycle orchestration."""

from __future__ import annotations

import json
import math
import subprocess
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
from typing import Callable

from src.baselines.common.redact import redact_text
from src.experiments.scenarios.core import build_scenario_schedule
from src.infrastructure.runner_lease import acquire_runner_lease
from src.infrastructure.runtime_env import subprocess_env
from src.modalities.text.contracts import (
    build_text_runtime_snapshot,
    text_work_calibration_signature,
)
from src.observability.metrics import (
    aggregate_model_metric_snapshots,
    estimate_mfu,
    scrape_prometheus_metrics,
    vllm_metric_delta_stats,
)
from src.scheduling.submission_control.capacity import (
    BoundedCapacityController,
    CapacityArm,
)
from src.scheduling.runtime.saor_capacity import (
    SaorCapacityController,
    SaorObservationModel,
)
from src.scheduling.submission_control.saor import SaorReleaseConfig

from .config import (
    GroupRunIdentity,
    RunnerOptions,
    SharedVllmConfig,
    SharedVllmScenario,
    build_job_command,
    load_config,
)
from .direct_control import run_direct_control
from .evidence import (
    _config_fingerprint,
    _coordinator_name,
    _group_artifacts_exist,
    _group_failure_path,
    _load_group_record,
    _load_resume_manifest,
    _redact_command,
    _redacted_config,
    _repository_commit,
    _rewrite_group_runs,
    _run_instance_id,
    _run_stem,
    _terminate_processes,
    _validate_final_credit,
    _validate_job_evidence,
    _validate_replay_starts,
    _validate_runner_topology,
    _write_json_atomic,
    _write_trace_rows_atomic,
)
from .metrics import (
    active_set_phase_summary,
    bounded_saor_event_summary,
    bounded_ready_event_summary,
    completion_accounted_service_fairness,
    cumulative_service_disparity,
    group_metric_delta,
    group_resource_summary,
    jain_fairness,
    normalized_job_service_rates,
    shared_credit_trace_summary,
)
from .runtime import (
    EndpointServiceRateTracker,
    SAOR_RELEASE_EVENT_FIELDS,
    _RayCreditObserver,
    _resource_sample,
    build_observe_only_text_state_rows,
)


_CODE_ROOT = Path(__file__).resolve().parents[3]
_TRACE_SAMPLE_INTERVAL_S = 0.25

_REHEARSAL_CREDIT_POLICIES = {
    "shared_fifo",
    "shared_drr",
    "external_vtc",
    "saor_release",
    "foreground_strict_priority",
}


def _common_arg_value(
    arguments: tuple[str, ...],
    flag: str,
    default: str,
) -> str:
    for index, item in enumerate(arguments[:-1]):
        if item == flag:
            return arguments[index + 1]
    prefix = f"{flag}="
    return next(
        (item[len(prefix):] for item in arguments if item.startswith(prefix)),
        default,
    )


def _text_state_calibration_signature(config: SharedVllmConfig) -> str:
    metadata = dict(config.service_metadata)
    return text_work_calibration_signature(
        model_revision=_common_arg_value(
            config.common_args,
            "--completion-model",
            "unrecorded-model",
        ),
        serving_revision=f"vllm-{metadata.get('vllm_version', 'unrecorded')}",
        protocol=_common_arg_value(
            config.common_args,
            "--completion-protocol",
            "completions",
        ),
        cost_model_revision=_common_arg_value(
            config.common_args,
            "--output-cost-mode",
            "fixed_output_cap",
        ),
    )


def _validate_rehearsal_record(
    scenario: SharedVllmScenario,
    record: dict[str, object],
    *,
    ready_observation_contract: str | None = None,
) -> None:
    """Fail closed on evidence gates before a formal matrix is allowed."""

    if str(record.get("execution_mode")) != "rehearsal":
        raise RuntimeError("rehearsal record has an invalid execution mode")
    if record.get("metrics_status") != "ok":
        raise RuntimeError("rehearsal model metrics are incomplete")
    if record.get("resource_metrics_status") != "ok":
        raise RuntimeError("rehearsal resource metrics are incomplete")
    if int(record.get("incidents", -1)) != 0:
        raise RuntimeError("rehearsal record contains an incident")
    if int(record.get("actor_worker_failures", -1)) != 0:
        raise RuntimeError("rehearsal record contains an actor failure")
    is_staggered_two_job = bool(
        scenario.job_count == 2
        and len(scenario.arrival_offsets_s) == 2
        and not math.isclose(
            scenario.arrival_offsets_s[0],
            scenario.arrival_offsets_s[1],
            abs_tol=1e-9,
        )
    )
    if not is_staggered_two_job:
        return
    if record.get("active_set_lifecycle_passed") is not True:
        raise RuntimeError("rehearsal active-set lifecycle gate failed")
    if scenario.policy in {"saor_bounded_priority", "saor_bounded_ready"}:
        if record.get("bounded_saor_event_status") != "ok:lossless_ledger":
            raise RuntimeError("rehearsal bounded-SAOR event ledger is unavailable")
        if record.get("bounded_saor_event_sequence_complete") is not True:
            raise RuntimeError("rehearsal bounded-SAOR event sequence is incomplete")
        if (
            int(record.get("bounded_saor_slo_priority_grants", 0)) < 1
            or int(record.get("bounded_saor_debt_recovery_grants", 0)) < 1
            or int(record.get("bounded_saor_avoidable_idle_events", -1)) != 0
            or int(
                record.get(
                    "bounded_saor_foreign_grant_over_debt_critical_events",
                    -1,
                )
            )
            != 0
            or int(record.get("bounded_saor_recovery_inflight_max", 2)) > 1
        ):
            raise RuntimeError("rehearsal bounded-SAOR mechanism gate failed")
    observation_contract = (
        scenario.ready_observation_contract
        if ready_observation_contract is None
        else ready_observation_contract
    )
    if observation_contract == "bounded_concrete_pre_registration":
        if (
            record.get("bounded_ready_event_status") != "ok:actor_event_join"
            or record.get("bounded_ready_lifecycle_complete") is not True
            or int(record.get("bounded_ready_intervals", 0))
            < scenario.job_count
            or int(record.get("bounded_ready_jobs_with_intervals", 0))
            != scenario.job_count
            or int(record.get("bounded_ready_max_ready_requests_seen", 0)) < 2
            or int(record.get("bounded_ready_max_ready_work_seen", 0)) <= 0
            or int(
                record.get("bounded_ready_max_ready_payload_bytes_seen", 0)
            )
            <= 0
        ):
            raise RuntimeError("rehearsal bounded-ready observation gate failed")
        if (
            scenario.policy == "saor_bounded_ready"
            and int(record.get("bounded_ready_foreign_fallback_events", -1))
            != 0
        ):
            raise RuntimeError("rehearsal bounded-ready observation gate failed")
    if scenario.policy in {"saor_bounded_priority", "saor_bounded_ready"}:
        return
    mechanism_applicable = record.get("active_set_mechanism_applicable") is True
    if scenario.policy in _REHEARSAL_CREDIT_POLICIES:
        if not mechanism_applicable:
            raise RuntimeError("rehearsal credit trace is unavailable")
        if record.get("active_set_mechanism_passed") is not True:
            raise RuntimeError("rehearsal active-set mechanism gate failed")
    elif mechanism_applicable:
        raise RuntimeError("rehearsal control unexpectedly emitted a credit trace")


def _apply_state_control(
    rows: list[dict[str, object]],
    *,
    controllers: dict[str, BoundedCapacityController],
    observer: _RayCreditObserver,
    calibration_signature: str,
    max_state_age_s: float,
) -> list[dict[str, object]]:
    if not controllers:
        return rows
    for row in rows:
        endpoint_id = str(row["endpoint_id"])
        controller = controllers[endpoint_id]
        waiting_raw = row.get("vllm_waiting")
        waiting = (
            int(float(waiting_raw))
            if waiting_raw not in (None, "")
            else 0
        )
        rate_raw = row.get("service_rate_tokens_s")
        service_rate = (
            float(rate_raw) if rate_raw not in (None, "") else None
        )
        kv_raw = row.get("vllm_kv_usage")
        kv_usage = float(kv_raw) if kv_raw not in (None, "") else None
        observed_at_s = float(row["observed_epoch_s"])
        snapshot = build_text_runtime_snapshot(
            active_work=int(row["model_active_work"]),
            upstream_queued_work=int(row["organizer_queued_work"]),
            service_waiting_requests=waiting,
            active_requests=int(row["active_requests"]),
            oldest_upstream_age_s=float(
                row["organizer_oldest_queue_age_s"]
            ),
            observed_at_s=observed_at_s,
            capacity_work=int(row["model_capacity_work"]),
            calibration_signature=calibration_signature,
            service_rate_tokens_s=service_rate,
        )
        decision = controller.select(
            snapshot,
            active_requests=int(row["active_requests"]),
            service_waiting_requests=waiting,
            service_rate_tokens_s=service_rate,
            kv_usage=kv_usage,
            now_s=time.time(),
            max_age_s=max_state_age_s,
            calibration_signature=calibration_signature,
        )
        previous_limit = int(row["request_limit"])
        applied_limit = previous_limit
        previous_work_limit = int(row["model_capacity_work"])
        if (
            decision.arm.request_limit != previous_limit
            or decision.arm.work_limit != previous_work_limit
        ):
            updated = observer.update_capacity(
                endpoint_id,
                request_limit=decision.arm.request_limit,
                work_limit=decision.arm.work_limit,
            )
            applied_limit = int(updated["request_limit"])
        row.update(
            {
                "runtime_state_mode": "actuated",
                "control_action": decision.action,
                "control_reason": decision.reason,
                "control_previous_request_limit": previous_limit,
                "control_previous_work_limit": previous_work_limit,
                "control_recommended_request_limit": (
                    decision.arm.request_limit
                ),
                "control_applied_request_limit": applied_limit,
                "control_applied_work_limit": decision.arm.work_limit,
            }
        )
    return rows


def _apply_saor_capacity_control(
    rows: list[dict[str, object]],
    *,
    controllers: dict[str, SaorCapacityController],
    observation_model: SaorObservationModel,
    observer: _RayCreditObserver,
    calibration_signature: str,
    max_state_age_s: float,
) -> list[dict[str, object]]:
    """Apply only the SAOR capacity action to existing shared-credit state."""

    if not controllers:
        return rows
    for row in rows:
        endpoint_id = str(row["endpoint_id"])
        observed_at_s = float(row["observed_epoch_s"])
        rate_raw = row.get("service_rate_tokens_s")
        service_rate = float(rate_raw) if rate_raw not in (None, "") else None
        waiting_raw = row.get("vllm_waiting")
        waiting = int(float(waiting_raw)) if waiting_raw not in (None, "") else 0
        snapshot = build_text_runtime_snapshot(
            active_work=int(row["model_active_work"]),
            upstream_queued_work=int(row["organizer_queued_work"]),
            service_waiting_requests=waiting,
            active_requests=int(row["active_requests"]),
            oldest_upstream_age_s=float(row["organizer_oldest_queue_age_s"]),
            observed_at_s=observed_at_s,
            capacity_work=int(row["model_capacity_work"]),
            calibration_signature=calibration_signature,
            service_rate_tokens_s=service_rate,
        )
        try:
            goodput, tail_risk, energy = observation_model.evaluate(row)
            observation_status = "ok"
        except ValueError as exc:
            goodput, tail_risk, energy = math.nan, math.nan, math.nan
            observation_status = f"invalid:{type(exc).__name__}:{exc}"
        decision = controllers[endpoint_id].select(
            snapshot,
            observed_goodput=goodput,
            observed_tail_risk=tail_risk,
            observed_energy=energy,
            now_s=time.time(),
            max_age_s=max_state_age_s,
            calibration_signature=calibration_signature,
        )
        previous_limit = int(row["request_limit"])
        previous_work_limit = int(row["model_capacity_work"])
        applied_limit = previous_limit
        applied_work_limit = previous_work_limit
        if (
            decision.arm.request_limit != previous_limit
            or decision.arm.work_limit != previous_work_limit
        ):
            updated = observer.update_capacity(
                endpoint_id,
                request_limit=decision.arm.request_limit,
                work_limit=decision.arm.work_limit,
            )
            applied_limit = int(updated["request_limit"])
            applied_work_limit = int(updated["work_limit"])
        row.update(
            {
                "runtime_state_mode": "actuated_saor_capacity",
                "control_policy": "saor_capacity",
                "control_action": decision.action,
                "control_reason": decision.reason,
                "control_arm_name": decision.arm_name,
                "control_action_scores": json.dumps(
                    dict(decision.action_scores),
                    sort_keys=True,
                ),
                "control_observation_status": observation_status,
                "control_observed_goodput": (
                    goodput if math.isfinite(goodput) else ""
                ),
                "control_observed_tail_risk": (
                    tail_risk if math.isfinite(tail_risk) else ""
                ),
                "control_observed_energy": (
                    energy if math.isfinite(energy) else ""
                ),
                "control_previous_request_limit": previous_limit,
                "control_previous_work_limit": previous_work_limit,
                "control_recommended_request_limit": decision.arm.request_limit,
                "control_applied_request_limit": applied_limit,
                "control_applied_work_limit": applied_work_limit,
            }
        )
    return rows


def run_experiment(
    options: RunnerOptions,
    *,
    idle_gate: Callable[[str, tuple[str, ...], float], None],
) -> int:
    if not options.ray_address:
        raise ValueError("shared-vLLM runner requires an explicit Ray address")
    if not options.metrics_urls:
        raise ValueError("at least one metrics URL is required")
    if (
        not math.isfinite(options.start_delay_s)
        or options.start_delay_s <= 0
        or not math.isfinite(options.idle_timeout_s)
        or options.idle_timeout_s <= 0
        or not math.isfinite(options.max_start_lateness_s)
        or options.max_start_lateness_s < 0
        or not math.isfinite(options.max_start_skew_s)
        or options.max_start_skew_s < 0
    ):
        raise ValueError("runner timing bounds must be finite and valid")
    config = load_config(options.config_path)
    _validate_runner_topology(options, config)
    schedule = build_scenario_schedule(
        [scenario.scenario_id for scenario in config.scenarios],
        1 if options.rehearsal else config.warmup_runs_per_scenario,
        0 if options.rehearsal else config.formal_repeats,
        config.seed,
    )
    fingerprint = _config_fingerprint(config, schedule)
    repository_commit = _repository_commit()
    with acquire_runner_lease(
        options.output_dir,
        config_fingerprint=fingerprint,
        repository_commit=repository_commit,
        recover_stale=options.recover_stale_lease,
    ) as lease:
        return _run_locked(
            options,
            config,
            schedule,
            idle_gate=idle_gate,
            recovered_owner=lease.recovered_owner,
            fingerprint=fingerprint,
            repository_commit=repository_commit,
        )

def _run_locked(
    options: RunnerOptions,
    config: SharedVllmConfig,
    schedule,
    *,
    idle_gate: Callable[[str, tuple[str, ...], float], None],
    recovered_owner: dict[str, object] | None,
    fingerprint: str,
    repository_commit: str,
) -> int:
    options.output_dir.mkdir(parents=True, exist_ok=True)
    for child in ("jobs", "logs", "traces", "records"):
        (options.output_dir / child).mkdir(parents=True, exist_ok=True)
    manifest_path = options.output_dir / "manifest.json"
    group_runs_path = options.output_dir / "group_runs.csv"
    run_instance_id = _run_instance_id(options.output_dir)
    redacted_config = _redacted_config(config)
    if options.rehearsal:
        redacted_config = {
            **redacted_config,
            "warmup_runs_per_scenario": 1,
            "formal_repeats": 0,
        }
    expected = {
        "schema_version": 1,
        "experiment_id": config.experiment_id,
        "config_fingerprint": fingerprint,
        "repository_commit": repository_commit,
        "run_instance_id": run_instance_id,
        "execution_mode": "rehearsal" if options.rehearsal else "configured_matrix",
        "redacted_config": redacted_config,
        "schedule": [asdict(item) for item in schedule],
        "completed_runs": [],
        "incidents": [],
        "status": "running",
    }
    if options.resume:
        manifest = _load_resume_manifest(manifest_path, expected)
        manifest["status"] = "running"
    else:
        if manifest_path.exists() or group_runs_path.exists():
            raise ValueError(
                "output directory already contains experiment state; "
                "use a new directory or --resume"
            )
        manifest = expected
    if recovered_owner is not None:
        manifest["incidents"].append(
            {
                "reason": "stale_runner_lease_recovered",
                "recovered_owner": recovered_owner,
                "recovered": True,
            }
        )
    _write_json_atomic(manifest_path, manifest)
    definitions = {
        scenario.scenario_id: scenario for scenario in config.scenarios
    }
    completed_keys = {
        (
            item["scenario_id"],
            item["phase"],
            int(item["repeat_index"]),
        )
        for item in manifest["completed_runs"]
    }
    if manifest["completed_runs"]:
        _rewrite_group_runs(
            group_runs_path,
            options.output_dir,
            manifest["completed_runs"],
        )
    for scheduled in schedule:
        run_key = (
            scheduled.scenario_id,
            scheduled.phase,
            scheduled.repeat_index,
        )
        if run_key in completed_keys:
            continue
        scenario = definitions[scheduled.scenario_id]
        identity = GroupRunIdentity(
            scheduled.phase,
            scheduled.repeat_index,
            scheduled.order_index,
        )
        run_stem = _run_stem(scenario, identity)
        record_relative = f"records/{run_stem}.json"
        record_path = options.output_dir / record_relative
        try:
            if record_path.exists():
                if _group_failure_path(
                    options.output_dir,
                    run_stem,
                ).exists():
                    raise RuntimeError(
                        "completed record conflicts with failure evidence"
                    )
                record = _load_group_record(
                    record_path,
                    config,
                    scenario,
                    identity,
                )
            else:
                if _group_artifacts_exist(options.output_dir, run_stem):
                    raise RuntimeError(
                        "incomplete group artifacts exist without a "
                        "durable completed record; use a new output directory"
                    )
                idle_gate(
                    options.health_url,
                    options.metrics_urls,
                    options.idle_timeout_s,
                )
                record = run_shared_vllm_group_cell(
                    options,
                    config,
                    scenario,
                    identity,
                    idle_gate=idle_gate,
                )
            if options.rehearsal and config.fail_closed_rehearsal:
                _validate_rehearsal_record(
                    scenario,
                    record,
                    ready_observation_contract=(
                        scenario.ready_observation_contract
                    ),
                )
        except Exception as exc:
            manifest["incidents"].append(
                {
                    **asdict(scheduled),
                    "reason": redact_text(f"{type(exc).__name__}:{exc}"),
                    "recovered": False,
                }
            )
            manifest["status"] = "failed"
            _write_json_atomic(manifest_path, manifest)
            return 1
        manifest["completed_runs"].append(
            {
                **asdict(scheduled),
                "policy": scenario.policy,
                "job_count": scenario.job_count,
                "record_path": record_relative,
            }
        )
        completed_keys.add(run_key)
        _write_json_atomic(manifest_path, manifest)
        _rewrite_group_runs(
            group_runs_path,
            options.output_dir,
            manifest["completed_runs"],
        )
    manifest["status"] = "completed"
    _write_json_atomic(manifest_path, manifest)
    return 0

def run_shared_vllm_group_cell(
    options: RunnerOptions,
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
    identity: GroupRunIdentity,
    *,
    idle_gate: Callable[[str, tuple[str, ...], float], None] | None = None,
) -> dict[str, object]:
    """Execute one explicit Project scenario without a matrix or host lease."""

    return _run_group(
        options, config, scenario, identity, idle_gate=idle_gate
    )


def _wait_for_eager_job_launch(
    target_epoch_s: float,
    *,
    now: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
    on_wait: Callable[[], None] | None = None,
) -> float:
    """Cross one absolute Job barrier without imposing request replay."""

    while True:
        current = now()
        remaining = target_epoch_s - current
        if remaining <= 0:
            return current
        if on_wait is not None:
            on_wait()
        sleep(min(remaining, 0.05))


def _run_group(
    options: RunnerOptions,
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
    identity: GroupRunIdentity,
    *,
    idle_gate: Callable[[str, tuple[str, ...], float], None] | None = None,
) -> dict[str, object]:
    run_stem = _run_stem(scenario, identity)
    record_path = options.output_dir / "records" / f"{run_stem}.json"
    if record_path.exists():
        raise RuntimeError("completed group record already exists")
    run_instance_id = _run_instance_id(options.output_dir)
    coordinator_name = _coordinator_name(
        config.experiment_id,
        run_instance_id,
        run_stem,
    )
    start_epoch_s = 0.0
    commands: list[list[str]] = []
    observer = None
    processes = []
    direct_executor: ThreadPoolExecutor | None = None
    direct_future: Future[list[dict[str, object]]] | None = None
    log_handles = []
    resource_samples: list[dict[str, object]] = []
    credit_samples: list[dict[str, object]] = []
    release_events: list[dict[str, object]] = []
    state_samples: list[dict[str, object]] = []
    eager_job_launches: list[float] = []
    state_signature = _text_state_calibration_signature(config)
    control = config.state_aware_control
    saor_control = config.saor_capacity_control
    service_rate_tracker = EndpointServiceRateTracker(
        alpha=(
            control.rate_ewma_alpha
            if control is not None
            else saor_control.ewma_alpha
            if saor_control is not None
            else 0.3
        )
    )
    controllers = (
        {
            endpoint_id: BoundedCapacityController(
                tuple(
                    CapacityArm(limit, work_limit)
                    for limit, work_limit in zip(
                        control.request_candidates,
                        control.work_candidates,
                    )
                ),
                fallback=CapacityArm(
                    control.fallback_request_limit,
                    control.fallback_work_limit,
                ),
                initial=CapacityArm(
                    control.initial_request_limit,
                    control.work_candidates[
                        control.request_candidates.index(
                            control.initial_request_limit
                        )
                    ],
                ),
                target_service_rate_tokens_s=(
                    control.target_service_rate_tokens_s_per_endpoint
                ),
                consecutive_samples=control.consecutive_samples,
                increase_consecutive_samples=(
                    control.increase_consecutive_samples
                ),
                cooldown_samples=control.cooldown_samples,
                congestion_kv_usage=control.congestion_kv_usage,
            )
            for endpoint_id in config.endpoint_ids
        }
        if scenario.policy == "state_aware_adaptive" and control is not None
        else {}
    )
    saor_controllers = (
        {
            endpoint_id: SaorCapacityController(
                arms=saor_control.arms,
                initial_arm=saor_control.initial_arm,
                fallback_arm=saor_control.fallback_arm,
                ewma_alpha=saor_control.ewma_alpha,
                queue_work_scale=saor_control.queue_work_scale,
                min_dwell_samples=saor_control.min_dwell_samples,
                v=saor_control.v,
                tail_weight=saor_control.tail_weight,
                energy_weight=saor_control.energy_weight,
                switch_weight=saor_control.switch_weight,
            )
            for endpoint_id in config.endpoint_ids
        }
        if scenario.policy == "saor_capacity" and saor_control is not None
        else {}
    )
    group_launch_epoch_s = 0.0
    endpoint_request_limit, endpoint_work_limit = scenario.endpoint_limits(
        config.request_limit_per_endpoint,
        config.work_limit_per_endpoint,
    )
    try:
        observer = (
            _RayCreditObserver(
                options.ray_address,
                config.shared_credit_namespace,
                coordinator_name,
                config.endpoint_ids,
            )
            if scenario.policy in {
                "shared_drr",
                "shared_fifo",
                "external_vtc",
                "saor_release",
                "saor_bounded_priority",
                "saor_bounded_ready",
                "foreground_strict_priority",
                "state_aware_adaptive",
                "saor_capacity",
            }
            else None
        )
        if observer is not None:
            observer.prewarm(
                request_limit=endpoint_request_limit,
                work_limit=endpoint_work_limit,
                quantum=config.credit_quantum,
                policy=(
                    "fifo" if scenario.policy == "shared_fifo"
                    else "vtc" if scenario.policy == "external_vtc"
                    else "saor" if scenario.policy == "saor_release"
                    else scenario.policy
                    if scenario.policy in {
                        "saor_bounded_priority",
                        "saor_bounded_ready",
                    }
                    else "strict_priority"
                    if scenario.policy == "foreground_strict_priority"
                    else "drr"
                ),
                saor_release_config=(
                    SaorReleaseConfig(
                        **asdict(config.saor_release_control)
                    )
                    if scenario.policy in {
                        "saor_release",
                        "saor_bounded_priority",
                        "saor_bounded_ready",
                    }
                    and config.saor_release_control is not None
                    else None
                ),
                record_ready_lifecycle_events=(
                    scenario.ready_observation_contract
                    == "bounded_concrete_pre_registration"
                ),
            )
        start_epoch_s = time.time() + options.start_delay_s
        commands = (
            []
            if scenario.policy == "direct_no_job"
            else [
                build_job_command(
                    options,
                    config,
                    scenario,
                    identity,
                    job_index=job_index,
                    start_epoch_s=start_epoch_s,
                    coordinator_name=coordinator_name,
                )
                for job_index in range(scenario.job_count)
            ]
        )
        _write_json_atomic(
            options.output_dir / "traces" / f"{run_stem}.commands.json",
            {
                "schema_version": 1,
                "execution_owner": (
                    "bounded_http_then_vllm_fcfs"
                    if scenario.policy == "direct_no_job"
                    else "project_daft_ray_profiler"
                ),
                "commands": [
                    _redact_command(command) for command in commands
                ],
            },
        )
        group_launch_epoch_s = time.time()
        before = [
            scrape_prometheus_metrics(url)
            for url in options.metrics_urls
        ]

        def sample_executor_state() -> None:
            resource_batch = _resource_sample(
                options.metrics_urls,
                group_launch_epoch_s,
            )
            resource_samples.extend(resource_batch)
            service_rates = service_rate_tracker.update(
                resource_batch,
                endpoint_ids=config.endpoint_ids,
            )
            if observer is None:
                return
            credit_batch = observer.sample(group_launch_epoch_s)
            credit_samples.extend(credit_batch)
            if scenario.ready_observation_contract == (
                "bounded_concrete_pre_registration"
            ) or scenario.policy == "saor_bounded_priority":
                release_events.extend(
                    observer.drain_release_events(group_launch_epoch_s)
                )
            state_rows = build_observe_only_text_state_rows(
                credit_batch,
                resource_batch,
                endpoint_ids=config.endpoint_ids,
                calibration_signature=state_signature,
                service_rates=service_rates,
            )
            if saor_controllers and saor_control is not None:
                state_rows = _apply_saor_capacity_control(
                    state_rows,
                    controllers=saor_controllers,
                    observation_model=saor_control.observation_model,
                    observer=observer,
                    calibration_signature=state_signature,
                    max_state_age_s=saor_control.max_state_age_s,
                )
            else:
                state_rows = _apply_state_control(
                    state_rows,
                    controllers=controllers,
                    observer=observer,
                    calibration_signature=state_signature,
                    max_state_age_s=(
                        control.max_state_age_s
                        if control is not None
                        else 1.0
                    ),
                )
            state_samples.extend(state_rows)

        if scenario.policy == "direct_no_job":
            direct_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="direct-no-job",
            )
            direct_future = direct_executor.submit(
                run_direct_control,
                config,
                scenario,
                start_epoch_s=start_epoch_s,
                output_dir=options.output_dir,
                run_stem=run_stem,
            )
        for job_index, command in enumerate(commands):
            if config.job_internal_arrival_contract == "eager":
                eager_job_launches.append(_wait_for_eager_job_launch(
                    start_epoch_s + scenario.arrival_offsets_s[job_index],
                    on_wait=sample_executor_state,
                ))
            stdout_path = (
                options.output_dir
                / "logs"
                / f"{run_stem}_job{job_index}.stdout.log"
            )
            stderr_path = (
                options.output_dir
                / "logs"
                / f"{run_stem}_job{job_index}.stderr.log"
            )
            stdout_handle = stdout_path.open("w", encoding="utf-8")
            stderr_handle = stderr_path.open("w", encoding="utf-8")
            log_handles.extend([stdout_handle, stderr_handle])
            processes.append(
                subprocess.Popen(
                    command,
                    cwd=_CODE_ROOT,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                    text=True,
                    env=subprocess_env(),
                )
            )
        while any(process.poll() is None for process in processes) or (
            direct_future is not None and not direct_future.done()
        ):
            failed = [
                process.returncode
                for process in processes
                if process.poll() not in (None, 0)
            ]
            if failed:
                _terminate_processes(processes)
                raise RuntimeError(
                    f"profiler child failed with exit code {failed[0]}"
                )
            sample_executor_state()
            time.sleep(_TRACE_SAMPLE_INTERVAL_S)
        return_codes = [process.wait() for process in processes]
        if any(code != 0 for code in return_codes):
            raise RuntimeError(
                f"profiler children failed: {return_codes}"
            )
        direct_job_evidence = (
            direct_future.result() if direct_future is not None else None
        )
        if scenario.policy == "direct_no_job":
            if direct_job_evidence is None:
                raise RuntimeError("direct_no_job returned no job evidence")
            group_end_epoch_s = max(
                float(item["completion_end_epoch_s"])
                for item in direct_job_evidence
            )
            if idle_gate is None:
                raise RuntimeError("direct_no_job requires the final idle gate")
            idle_gate(
                options.health_url,
                options.metrics_urls,
                options.idle_timeout_s,
            )
        else:
            group_end_epoch_s = time.time()
        after = [
            scrape_prometheus_metrics(url)
            for url in options.metrics_urls
        ]
        final_credit = []
        if observer is not None:
            credit_samples.extend(observer.sample(group_launch_epoch_s))
            if scenario.ready_observation_contract == (
                "bounded_concrete_pre_registration"
            ) or scenario.policy == "saor_bounded_priority":
                release_events.extend(
                    observer.drain_release_events(group_launch_epoch_s)
                )
            final_credit = observer.final_snapshots()
        job_evidence = (
            direct_job_evidence
            if direct_job_evidence is not None
            else [
                _validate_job_evidence(
                    options,
                    scenario,
                    identity,
                    job_index,
                )
                for job_index in range(scenario.job_count)
            ]
        )
        if config.job_internal_arrival_contract == "eager":
            if len(eager_job_launches) != len(job_evidence):
                raise RuntimeError("eager Job launch evidence is incomplete")
            for job_index, evidence in enumerate(job_evidence):
                evidence["replay_configured_start_epoch_s"] = (
                    start_epoch_s + scenario.arrival_offsets_s[job_index]
                )
                evidence["replay_observed_start_epoch_s"] = (
                    eager_job_launches[job_index]
                )
        _validate_replay_starts(
            job_evidence,
            expected_start_epoch_s=start_epoch_s,
            arrival_offsets_s=scenario.arrival_offsets_s,
            max_lateness_s=options.max_start_lateness_s,
            max_skew_s=options.max_start_skew_s,
        )
        if not all(
            endpoint_id in {
                endpoint
                for evidence in job_evidence
                for endpoint in evidence["endpoint_counts"]
            }
            for endpoint_id in config.endpoint_ids
        ):
            raise RuntimeError(
                "not every configured endpoint received requests"
            )
        actor_worker_failures = sum(
            int(evidence["actor_worker_failures"])
            for evidence in job_evidence
        )
        if actor_worker_failures:
            raise RuntimeError(
                f"actor worker failures observed: {actor_worker_failures}"
            )
        if observer is not None:
            _validate_final_credit(config, scenario, final_credit)
            if not credit_samples:
                raise RuntimeError("shared credit trace is empty")
        if not resource_samples:
            raise RuntimeError("group resource trace is empty")
        duration_s = max(1e-9, group_end_epoch_s - start_epoch_s)
        service_metrics = group_metric_delta(
            before,
            after,
            duration_s=duration_s,
        )
        if service_metrics["metrics_status"] != "ok":
            raise RuntimeError("group vLLM metrics are unavailable")
        resource_metrics = group_resource_summary(
            resource_samples,
            start_epoch_s=start_epoch_s,
            end_epoch_s=group_end_epoch_s,
        )
        if resource_metrics["resource_metrics_status"] != "ok":
            raise RuntimeError("group resource metrics are unavailable")
        observed_tokens = int(service_metrics["prompt_tokens_delta"]) + int(
            service_metrics["generation_tokens_delta"]
        )
        mfu_metrics = estimate_mfu(
            estimated_flops=float(
                service_metrics["estimated_flops_per_gpu_delta"]
            ),
            observed_tokens=observed_tokens,
            operator_wall_s=duration_s,
            model_flops_per_token=0.0,
            gpu_peak_tflops=config.gpu_peak_tflops,
            precision=config.mfu_precision,
        )
        if mfu_metrics["mfu_status"] != "ok":
            raise RuntimeError(
                f"group MFU is invalid: {mfu_metrics['mfu_status']}"
            )
        normalized_service = normalized_job_service_rates(
            job_evidence,
            scenario.weights,
        )
        completion_fairness = completion_accounted_service_fairness(
            job_evidence,
            scenario.weights,
        )
        lag_work_limit = max(1, endpoint_work_limit)
        record = {
            "schema_version": 2,
            "experiment_id": config.experiment_id,
            "scenario_id": scenario.scenario_id,
            "phase": identity.phase,
            "repeat_index": identity.repeat_index,
            "order_index": identity.order_index,
            "policy": scenario.policy,
            "experiment_identity": (
                "project_internal_selector_ablation"
                if scenario.ready_observation_contract
                == "bounded_concrete_pre_registration"
                else "project_frozen_static_reference"
                if scenario.policy == "static_partition"
                else "project_policy"
            ),
            "ready_observation_contract": (
                scenario.ready_observation_contract
            ),
            "job_count": scenario.job_count,
            "static_partition_count": (
                scenario.static_partition_count
                if scenario.static_partition_count is not None
                else scenario.job_count
            ),
            "rows_per_job": scenario.rows_per_job,
            **(
                {
                    "rows_per_jobs": json.dumps(
                        [
                            scenario.row_count(index)
                            for index in range(scenario.job_count)
                        ]
                    )
                }
                if scenario.rows_per_jobs
                else {}
            ),
            "request_limit_per_endpoint": (
                endpoint_request_limit
            ),
            "work_limit_per_endpoint": endpoint_work_limit,
            "request_envelope_owner": (
                "direct_endpoint_http_semaphore"
                if scenario.policy == "direct_no_job"
                else "project_admission"
            ),
            "work_envelope_applied": scenario.policy != "direct_no_job",
            "http_keepalive_expiry_s": _common_arg_value(
                config.common_args,
                "--completion-http-keepalive-expiry-s",
                "4.0",
            ),
            "credit_quantum": config.credit_quantum,
            "runtime_state_mode": (
                "actuated_saor_capacity" if saor_controllers
                else "actuated" if controllers
                else "actuated_saor_release"
                if scenario.policy in {
                    "saor_release",
                    "saor_bounded_priority",
                    "saor_bounded_ready",
                }
                else "observe_only" if observer is not None
                else "direct_no_job_control"
                if scenario.policy == "direct_no_job"
                else "unavailable"
            ),
            "runtime_state_calibration_signature": (
                state_signature if observer is not None else ""
            ),
            "adaptive_capacity_increases": sum(
                row.get("control_action") == "increase"
                for row in state_samples
            ),
            "adaptive_capacity_decreases": sum(
                row.get("control_action") == "decrease"
                for row in state_samples
            ),
            "adaptive_capacity_fallbacks": sum(
                row.get("control_action") == "fallback"
                for row in state_samples
            ),
            "weights": json.dumps(scenario.weights),
            "arrival_offsets_s": json.dumps(
                scenario.arrival_offsets_s
            ),
            "source_row_offsets": json.dumps(
                scenario.source_row_offsets
            ),
            "request_manifests": json.dumps(
                scenario.request_manifests
            ),
            "request_manifest_sha256": json.dumps(
                [
                    evidence["request_manifest_sha256"]
                    for evidence in job_evidence
                ]
            ),
            "ray_address": options.ray_address,
            "scheduler_owner": (
                "endpoint_http_bound_then_vllm_fcfs"
                if scenario.policy == "direct_no_job"
                else "project_daft_ray_submission_then_vllm_fcfs"
            ),
            "coordinator_name": (
                coordinator_name
                if scenario.policy in {
                    "shared_drr",
                    "shared_fifo",
                    "external_vtc",
                    "saor_release",
                    "saor_bounded_priority",
                    "saor_bounded_ready",
                    "foreground_strict_priority",
                    "state_aware_adaptive",
                    "saor_capacity",
                }
                else ""
            ),
            "run_instance_id": run_instance_id,
            "execution_mode": (
                "rehearsal" if options.rehearsal else "configured_matrix"
            ),
            "start_epoch_s": start_epoch_s,
            "end_epoch_s": group_end_epoch_s,
            **service_metrics,
            **resource_metrics,
            **mfu_metrics,
            "jain_fairness": jain_fairness(normalized_service),
            **cumulative_service_disparity(
                job_evidence,
                scenario.weights,
            ),
            **completion_fairness,
            "completion_service_lag_p95_work_envelopes": (
                float(completion_fairness["completion_service_lag_p95_work"])
                / lag_work_limit
            ),
            "completion_service_lag_max_work_envelopes": (
                float(completion_fairness["completion_service_lag_max_work"])
                / lag_work_limit
            ),
            **shared_credit_trace_summary(
                credit_samples,
                work_limit_per_endpoint=endpoint_work_limit,
                job_count=scenario.job_count,
            ),
            **active_set_phase_summary(
                job_evidence,
                credit_samples,
                observation_interval_s=_TRACE_SAMPLE_INTERVAL_S,
            ),
            **bounded_saor_event_summary(release_events),
            **(
                bounded_ready_event_summary(
                    release_events,
                    job_evidence,
                    foreground_job_index=max(
                        range(scenario.job_count),
                        key=scenario.job_priority,
                    ),
                )
                if scenario.ready_observation_contract
                == "bounded_concrete_pre_registration"
                else {
                    "bounded_ready_event_status": "not_applicable",
                    "bounded_ready_lifecycle_complete": False,
                    "bounded_ready_intervals": 0,
                    "bounded_ready_jobs_with_intervals": 0,
                    "bounded_ready_max_ready_requests_seen": 0,
                    "bounded_ready_max_ready_work_seen": 0,
                    "bounded_ready_max_ready_payload_bytes_seen": 0,
                    "bounded_ready_requests_transition_mean_max": 0.0,
                    "bounded_ready_requests_transition_p95_max": 0.0,
                    "bounded_ready_work_transition_mean_max": 0.0,
                    "bounded_ready_work_transition_p95_max": 0.0,
                    "bounded_ready_payload_bytes_transition_mean_max": 0.0,
                    "bounded_ready_payload_bytes_transition_p95_max": 0.0,
                    "bounded_ready_foreground_intervals": 0,
                    "bounded_ready_foreign_fallback_events": 0,
                    "bounded_ready_foreground_max_ready_requests_seen": 0,
                    "bounded_ready_foreground_max_ready_work_seen": 0,
                }
            ),
            "job_jct_s": json.dumps(
                [evidence["jct_s"] for evidence in job_evidence]
            ),
            "job_arrival_start_epoch_s": json.dumps(
                [
                    evidence["arrival_start_epoch_s"]
                    for evidence in job_evidence
                ]
            ),
            "job_completion_end_epoch_s": json.dumps(
                [
                    evidence["completion_end_epoch_s"]
                    for evidence in job_evidence
                ]
            ),
            "job_priorities": json.dumps(
                [
                    scenario.job_priority(index)
                    for index in range(scenario.job_count)
                ]
            ),
            "job_p99_s": json.dumps(
                [evidence["p99_s"] for evidence in job_evidence]
            ),
            "job_completion_lag_s": json.dumps(
                [
                    evidence["completion_lag_s"]
                    for evidence in job_evidence
                ]
            ),
            "job_slo_violation_ratio": json.dumps(
                [
                    evidence["slo_violation_ratio"]
                    for evidence in job_evidence
                ]
            ),
            "job_slo_goodput_per_s": json.dumps(
                [
                    evidence["slo_goodput_per_s"]
                    for evidence in job_evidence
                ]
            ),
            "job_slo_token_goodput_per_s": json.dumps(
                [
                    evidence["slo_token_goodput_per_s"]
                    for evidence in job_evidence
                ]
            ),
            "job_predicted_work": json.dumps(
                [
                    evidence["predicted_work"]
                    for evidence in job_evidence
                ]
            ),
            "job_actual_work": json.dumps(
                [evidence["actual_work"] for evidence in job_evidence]
            ),
            "job_expected_count": json.dumps(
                [evidence["expected_count"] for evidence in job_evidence]
            ),
            "job_completed_count": json.dumps(
                [evidence["completed_count"] for evidence in job_evidence]
            ),
            "job_exactly_once": json.dumps(
                [evidence["exactly_once"] for evidence in job_evidence]
            ),
            **(
                {
                    "job_actual_prompt_work": json.dumps(
                        [
                            evidence["actual_prompt_work"]
                            for evidence in job_evidence
                        ]
                    ),
                    "job_actual_output_work": json.dumps(
                        [
                            evidence["actual_output_work"]
                            for evidence in job_evidence
                        ]
                    ),
                    "job_arrived_rows": json.dumps(
                        [
                            scenario.row_count(index)
                            for index in range(scenario.job_count)
                        ]
                    ),
                    "job_completed_rows": json.dumps(
                        [
                            scenario.row_count(index)
                            for index in range(scenario.job_count)
                        ]
                    ),
                    "job_failed_rows": json.dumps([0] * scenario.job_count),
                }
                if scenario.rows_per_jobs
                else {}
            ),
            "job_normalized_service_rate": json.dumps(
                normalized_service
            ),
            "replay_configured_start_epoch_s": json.dumps(
                [
                    evidence["replay_configured_start_epoch_s"]
                    for evidence in job_evidence
                ]
            ),
            "replay_observed_start_epoch_s": json.dumps(
                [
                    evidence["replay_observed_start_epoch_s"]
                    for evidence in job_evidence
                ]
            ),
            "replay_actual_submit_start_epoch_s": json.dumps(
                [
                    evidence["replay_actual_submit_start_epoch_s"]
                    for evidence in job_evidence
                ]
            ),
            "endpoint_counts": json.dumps(
                [
                    evidence["endpoint_counts"]
                    for evidence in job_evidence
                ],
                sort_keys=True,
            ),
            "actor_worker_failures": actor_worker_failures,
            "shared_credit_final": json.dumps(
                final_credit,
                sort_keys=True,
            ),
            "release_event_trace_schema_version": 2,
            "release_event_trace_path": str(
                Path("traces") / f"{run_stem}.release_events.csv"
            ),
            "release_event_trace_count": len(release_events),
            "incidents": 0,
        }
        _write_trace_rows_atomic(
            options.output_dir
            / "traces"
            / f"{run_stem}.resources.csv",
            resource_samples,
        )
        _write_trace_rows_atomic(
            options.output_dir
            / "traces"
            / f"{run_stem}.credits.csv",
            credit_samples,
        )
        _write_trace_rows_atomic(
            options.output_dir
            / "traces"
            / f"{run_stem}.states.csv",
            state_samples,
        )
        _write_trace_rows_atomic(
            options.output_dir
            / "traces"
            / f"{run_stem}.release_events.csv",
            release_events,
            fieldnames=SAOR_RELEASE_EVENT_FIELDS,
        )
        _write_json_atomic(record_path, record)
        return record
    except Exception as exc:
        _terminate_processes(processes)
        capture_error = None
        final_credit = []
        if observer is not None:
            try:
                credit_samples.extend(
                    observer.sample(group_launch_epoch_s)
                )
                if scenario.ready_observation_contract == (
                    "bounded_concrete_pre_registration"
                ) or scenario.policy == "saor_bounded_priority":
                    release_events.extend(
                        observer.drain_release_events(group_launch_epoch_s)
                    )
                final_credit = observer.final_snapshots()
            except Exception as evidence_exc:
                capture_error = redact_text(
                    f"{type(evidence_exc).__name__}:{evidence_exc}"
                )
        _write_trace_rows_atomic(
            options.output_dir
            / "traces"
            / f"{run_stem}.resources.csv",
            resource_samples,
        )
        _write_trace_rows_atomic(
            options.output_dir
            / "traces"
            / f"{run_stem}.credits.csv",
            credit_samples,
        )
        _write_trace_rows_atomic(
            options.output_dir
            / "traces"
            / f"{run_stem}.states.csv",
            state_samples,
        )
        _write_trace_rows_atomic(
            options.output_dir
            / "traces"
            / f"{run_stem}.release_events.csv",
            release_events,
            fieldnames=SAOR_RELEASE_EVENT_FIELDS,
        )
        _write_json_atomic(
            options.output_dir
            / "traces"
            / f"{run_stem}.failure.json",
            {
                "schema_version": 1,
                "reason": redact_text(f"{type(exc).__name__}:{exc}"),
                "return_codes": [
                    process.poll() for process in processes
                ],
                "final_credit_snapshots": final_credit,
                "credit_capture_error": capture_error,
                "release_event_trace_schema_version": 1,
                "release_event_trace_path": str(
                    Path("traces") / f"{run_stem}.release_events.csv"
                ),
                "release_event_trace_count": len(release_events),
            },
        )
        raise
    finally:
        if observer is not None:
            observer.cleanup()
        for handle in log_handles:
            handle.close()
        if direct_executor is not None:
            direct_executor.shutdown(wait=True, cancel_futures=True)
