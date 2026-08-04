"""Concurrent shared-vLLM group runner and lifecycle orchestration."""

from __future__ import annotations

import json
import math
import subprocess
import time
from dataclasses import asdict
from pathlib import Path
from typing import Callable

from src.experiments.scenarios.core import build_scenario_schedule
from src.infrastructure.runner_lease import acquire_runner_lease
from src.infrastructure.runtime_env import subprocess_env
from src.observability.metrics import (
    aggregate_model_metric_snapshots,
    estimate_mfu,
    scrape_prometheus_metrics,
    vllm_metric_delta_stats,
)

from .config import (
    GroupRunIdentity,
    RunnerOptions,
    SharedVllmConfig,
    SharedVllmScenario,
    build_job_command,
    load_config,
)
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
    cumulative_service_disparity,
    group_metric_delta,
    group_resource_summary,
    jain_fairness,
    normalized_job_service_rates,
)
from .runtime import _RayCreditObserver, _resource_sample


_CODE_ROOT = Path(__file__).resolve().parents[3]


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
        config.warmup_runs_per_scenario,
        config.formal_repeats,
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
    expected = {
        "schema_version": 1,
        "experiment_id": config.experiment_id,
        "config_fingerprint": fingerprint,
        "repository_commit": repository_commit,
        "run_instance_id": run_instance_id,
        "redacted_config": _redacted_config(config),
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
                _load_group_record(
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
                _run_group(
                    options,
                    config,
                    scenario,
                    identity,
                )
        except Exception as exc:
            manifest["incidents"].append(
                {
                    **asdict(scheduled),
                    "reason": f"{type(exc).__name__}:{exc}",
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

def _run_group(
    options: RunnerOptions,
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
    identity: GroupRunIdentity,
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
    log_handles = []
    resource_samples: list[dict[str, object]] = []
    credit_samples: list[dict[str, object]] = []
    group_launch_epoch_s = 0.0
    try:
        observer = (
            _RayCreditObserver(
                options.ray_address,
                config.shared_credit_namespace,
                coordinator_name,
                config.endpoint_ids,
            )
            if scenario.policy == "shared_drr"
            else None
        )
        if observer is not None:
            observer.prewarm(
                request_limit=config.request_limit_per_endpoint,
                work_limit=config.work_limit_per_endpoint,
                quantum=config.credit_quantum,
            )
        start_epoch_s = time.time() + options.start_delay_s
        commands = [
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
        _write_json_atomic(
            options.output_dir / "traces" / f"{run_stem}.commands.json",
            {
                "schema_version": 1,
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
        for job_index, command in enumerate(commands):
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
        while any(process.poll() is None for process in processes):
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
            resource_samples.extend(
                _resource_sample(
                    options.metrics_urls,
                    group_launch_epoch_s,
                )
            )
            if observer is not None:
                credit_samples.extend(
                    observer.sample(group_launch_epoch_s)
                )
            time.sleep(0.25)
        return_codes = [process.wait() for process in processes]
        if any(code != 0 for code in return_codes):
            raise RuntimeError(
                f"profiler children failed: {return_codes}"
            )
        group_end_epoch_s = time.time()
        after = [
            scrape_prometheus_metrics(url)
            for url in options.metrics_urls
        ]
        final_credit = []
        if observer is not None:
            credit_samples.extend(observer.sample(group_launch_epoch_s))
            final_credit = observer.final_snapshots()
        job_evidence = [
            _validate_job_evidence(
                options,
                scenario,
                identity,
                job_index,
            )
            for job_index in range(scenario.job_count)
        ]
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
            _validate_final_credit(config, final_credit)
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
        record = {
            "schema_version": 2,
            "experiment_id": config.experiment_id,
            "scenario_id": scenario.scenario_id,
            "phase": identity.phase,
            "repeat_index": identity.repeat_index,
            "order_index": identity.order_index,
            "policy": scenario.policy,
            "job_count": scenario.job_count,
            "rows_per_job": scenario.rows_per_job,
            "request_limit_per_endpoint": (
                config.request_limit_per_endpoint
            ),
            "work_limit_per_endpoint": config.work_limit_per_endpoint,
            "credit_quantum": config.credit_quantum,
            "weights": json.dumps(scenario.weights),
            "arrival_offsets_s": json.dumps(
                scenario.arrival_offsets_s
            ),
            "ray_address": options.ray_address,
            "coordinator_name": (
                coordinator_name
                if scenario.policy == "shared_drr"
                else ""
            ),
            "run_instance_id": run_instance_id,
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
            "job_jct_s": json.dumps(
                [evidence["jct_s"] for evidence in job_evidence]
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
        if observer is not None:
            observer.cleanup()
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
                final_credit = observer.final_snapshots()
            except Exception as evidence_exc:
                capture_error = (
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
        _write_json_atomic(
            options.output_dir
            / "traces"
            / f"{run_stem}.failure.json",
            {
                "schema_version": 1,
                "reason": f"{type(exc).__name__}:{exc}",
                "return_codes": [
                    process.poll() for process in processes
                ],
                "final_credit_snapshots": final_credit,
                "credit_capture_error": capture_error,
            },
        )
        raise
    finally:
        for handle in log_handles:
            handle.close()
