"""Formal orchestration primitives for shared-vLLM multi-job experiments."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import subprocess
import time
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .experiment_scenarios import (
    build_scenario_schedule,
    validate_service_metadata,
)
from .metrics import (
    aggregate_model_metric_snapshots,
    estimate_mfu,
    gpu_metadata,
    percentile,
    scrape_prometheus_metrics,
    vllm_metric_delta_stats,
)
from .runner_lease import acquire_runner_lease


POLICIES = {
    "independent_full",
    "static_partition",
    "shared_drr",
}
_SCENARIO_ID = re.compile(r"^[A-Za-z0-9_.-]+$")
_ENV_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_RUNNER_OWNED_FLAGS = {
    "--admission-scope",
    "--arrival-replay-start-epoch-s",
    "--control-trace-output",
    "--experiment-id",
    "--flush-trace-output",
    "--max-active-work-per-endpoint",
    "--max-inflight",
    "--output",
    "--random-seed",
    "--ray-address",
    "--repeats",
    "--request-trace-output",
    "--resource-trace-output",
    "--reset-documents",
    "--run-phase",
    "--run-repeat-index",
    "--scenario-id",
    "--setup",
    "--shared-credit-coordinator-name",
    "--shared-credit-job-weight",
    "--shared-credit-namespace",
    "--shared-credit-quantum",
    "--shared-credit-request-limit",
    "--shared-credit-work-limit",
    "--submission-granularity",
    "--submission-trace-output",
    "--total-rows",
    "--warmup-runs",
}


@dataclass(frozen=True)
class RunnerOptions:
    config_path: Path
    profiler_path: Path
    python_executable: Path
    output_dir: Path
    health_url: str
    metrics_urls: tuple[str, ...]
    ray_address: str
    idle_timeout_s: float
    start_delay_s: float = 15.0
    max_start_lateness_s: float = 2.0
    max_start_skew_s: float = 0.5
    resume: bool = False
    recover_stale_lease: bool = False


@dataclass(frozen=True)
class GroupRunIdentity:
    phase: str
    repeat_index: int
    order_index: int


@dataclass(frozen=True)
class SharedVllmScenario:
    scenario_id: str
    policy: str
    job_count: int
    rows_per_job: int
    weights: tuple[int, ...]
    arrival_offsets_s: tuple[float, ...]


@dataclass(frozen=True)
class SharedVllmConfig:
    experiment_id: str
    seed: int
    warmup_runs_per_scenario: int
    formal_repeats: int
    endpoint_ids: tuple[str, ...]
    request_limit_per_endpoint: int
    work_limit_per_endpoint: int
    credit_quantum: int
    shared_credit_namespace: str
    gpu_peak_tflops: float
    mfu_precision: str
    common_args: tuple[str, ...]
    scenarios: tuple[SharedVllmScenario, ...]
    service_metadata: tuple[tuple[str, object], ...]


_CODE_ROOT = Path(__file__).resolve().parents[1]


def load_config(path: Path) -> SharedVllmConfig:
    decoded = json.loads(path.read_text(encoding="utf-8"))
    if decoded.get("schema_version") != 1:
        raise ValueError("shared-vLLM config schema_version must be 1")
    experiment_id = _nonempty_string(
        decoded.get("experiment_id"),
        "experiment_id",
    )
    seed = _integer(decoded.get("seed"), "seed")
    warmups = _nonnegative_integer(
        decoded.get("warmup_runs_per_scenario"),
        "warmup_runs_per_scenario",
    )
    repeats = _positive_integer(
        decoded.get("formal_repeats"),
        "formal_repeats",
    )
    endpoint_ids_raw = decoded.get("endpoint_ids")
    if (
        not isinstance(endpoint_ids_raw, list)
        or not endpoint_ids_raw
        or any(
            not isinstance(item, str) or not item.strip()
            for item in endpoint_ids_raw
        )
        or len(set(endpoint_ids_raw)) != len(endpoint_ids_raw)
    ):
        raise ValueError("endpoint_ids must be unique non-empty strings")
    request_limit = _positive_integer(
        decoded.get("request_limit_per_endpoint"),
        "request_limit_per_endpoint",
    )
    work_limit = _positive_integer(
        decoded.get("work_limit_per_endpoint"),
        "work_limit_per_endpoint",
    )
    quantum = _positive_integer(
        decoded.get("credit_quantum"),
        "credit_quantum",
    )
    namespace = _nonempty_string(
        decoded.get(
            "shared_credit_namespace",
            "ai-operator-shared-vllm",
        ),
        "shared_credit_namespace",
    )
    common_args = _argument_list(
        decoded.get("common_args", []),
        "common_args",
    )
    if "--arrival-replay" not in common_args:
        raise ValueError("common_args must enable --arrival-replay")
    gpu_peak_tflops = _nonnegative_float(
        _argument_value(common_args, "--gpu-peak-tflops", "0"),
        "--gpu-peak-tflops",
    )
    mfu_precision = _argument_value(
        common_args,
        "--mfu-precision",
        "",
    )
    scenarios_raw = decoded.get("scenarios")
    if not isinstance(scenarios_raw, list) or not scenarios_raw:
        raise ValueError("scenarios must be a non-empty list")
    scenarios = tuple(
        _load_scenario(item, request_limit, work_limit)
        for item in scenarios_raw
    )
    scenario_ids = [item.scenario_id for item in scenarios]
    if len(set(scenario_ids)) != len(scenario_ids):
        raise ValueError("scenario_id values must be unique")
    service_metadata = decoded.get("service_metadata", {})
    if not isinstance(service_metadata, dict):
        raise ValueError("service_metadata must be an object")
    expanded_metadata = tuple(
        sorted(
            (
                _nonempty_string(key, "service_metadata key"),
                _expand_scalar(value, f"service_metadata.{key}"),
            )
            for key, value in service_metadata.items()
        )
    )
    if decoded.get("require_complete_service_metadata") is True:
        validate_service_metadata(dict(expanded_metadata))
    return SharedVllmConfig(
        experiment_id=experiment_id,
        seed=seed,
        warmup_runs_per_scenario=warmups,
        formal_repeats=repeats,
        endpoint_ids=tuple(endpoint_ids_raw),
        request_limit_per_endpoint=request_limit,
        work_limit_per_endpoint=work_limit,
        credit_quantum=quantum,
        shared_credit_namespace=namespace,
        gpu_peak_tflops=gpu_peak_tflops,
        mfu_precision=mfu_precision,
        common_args=common_args,
        scenarios=scenarios,
        service_metadata=expanded_metadata,
    )


def build_job_command(
    options: RunnerOptions,
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
    identity: GroupRunIdentity,
    *,
    job_index: int,
    start_epoch_s: float,
    coordinator_name: str,
) -> list[str]:
    if not 0 <= job_index < scenario.job_count:
        raise ValueError("job_index is outside scenario job_count")
    if not options.ray_address:
        raise ValueError("shared-vLLM runner requires an explicit Ray address")
    request_limit, work_limit = _local_limits(
        config,
        scenario,
        job_index,
    )
    run_stem = (
        f"{identity.order_index:03d}_{identity.phase}_"
        f"{identity.repeat_index}_{scenario.scenario_id}"
    )
    job_stem = options.output_dir / "jobs" / f"{run_stem}_job{job_index}"
    command = [
        str(options.python_executable),
        str(options.profiler_path),
        *config.common_args,
        "--total-rows",
        str(scenario.rows_per_job),
        "--db-fetch-rows",
        str(scenario.rows_per_job),
        "--max-inflight",
        str(request_limit),
        "--admission-scope",
        "per_endpoint",
        "--max-active-work-per-endpoint",
        str(work_limit),
        "--ray-address",
        options.ray_address,
        "--arrival-replay-start-epoch-s",
        str(start_epoch_s + scenario.arrival_offsets_s[job_index]),
        "--submission-granularity",
        "request",
        "--experiment-id",
        config.experiment_id,
        "--scenario-id",
        scenario.scenario_id,
        "--random-seed",
        str(config.seed + identity.order_index),
        "--run-phase",
        identity.phase,
        "--run-repeat-index",
        str(identity.repeat_index),
        "--warmup-runs",
        "0",
        "--repeats",
        "1",
        "--output",
        str(job_stem.with_suffix(".runs.csv")),
        "--request-trace-output",
        str(job_stem.with_suffix(".requests.csv")),
        "--submission-trace-output",
        str(job_stem.with_suffix(".submissions.csv")),
        "--flush-trace-output",
        str(job_stem.with_suffix(".flush.csv")),
    ]
    if scenario.policy == "shared_drr":
        if not coordinator_name:
            raise ValueError("shared_drr requires a coordinator name")
        command.extend(
            [
                "--shared-credit-coordinator-name",
                coordinator_name,
                "--shared-credit-namespace",
                config.shared_credit_namespace,
                "--shared-credit-request-limit",
                str(config.request_limit_per_endpoint),
                "--shared-credit-work-limit",
                str(config.work_limit_per_endpoint),
                "--shared-credit-quantum",
                str(config.credit_quantum),
                "--shared-credit-job-weight",
                str(scenario.weights[job_index]),
            ]
        )
    return command


def group_metric_delta(
    before_snapshots: list[dict[str, float]],
    after_snapshots: list[dict[str, float]],
    *,
    duration_s: float,
) -> dict[str, float | int | str]:
    if not math.isfinite(duration_s) or duration_s <= 0:
        raise ValueError("duration_s must be finite and positive")
    before = aggregate_model_metric_snapshots(before_snapshots)
    after = aggregate_model_metric_snapshots(after_snapshots)
    raw = vllm_metric_delta_stats(before, after)
    prompt_tokens = int(raw["vllm_prompt_tokens_delta"])
    generation_tokens = int(raw["vllm_generation_tokens_delta"])
    return {
        "metrics_status": raw["vllm_metrics_status"],
        "prompt_tokens_delta": prompt_tokens,
        "generation_tokens_delta": generation_tokens,
        "request_success_delta": int(raw["vllm_request_success_delta"]),
        "estimated_flops_per_gpu_delta": float(
            raw["vllm_estimated_flops_per_gpu_delta"]
        ),
        "tokens_per_s": (prompt_tokens + generation_tokens) / duration_s,
        "duration_s": duration_s,
    }


def jain_fairness(values: list[float]) -> float:
    if not values:
        raise ValueError("fairness values must not be empty")
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
        for value in values
    ):
        raise ValueError("fairness values must be finite and non-negative")
    total = float(sum(values))
    if total == 0:
        return 0.0
    return total * total / (
        len(values) * sum(float(value) ** 2 for value in values)
    )


def normalized_job_service_rates(
    job_evidence: list[dict[str, object]],
    weights: tuple[int, ...],
) -> list[float]:
    if len(job_evidence) != len(weights):
        raise ValueError("job evidence and weights must have equal length")
    rates = []
    for evidence, weight in zip(job_evidence, weights):
        predicted_work = float(evidence["predicted_work"])
        jct_s = float(evidence["jct_s"])
        if (
            not math.isfinite(predicted_work)
            or predicted_work < 0
            or not math.isfinite(jct_s)
            or jct_s <= 0
            or weight <= 0
        ):
            raise ValueError(
                "service inputs must contain finite non-negative work, "
                "positive JCT, and positive weights"
            )
        rates.append(predicted_work / jct_s / weight)
    return rates


def group_resource_summary(
    samples: list[dict[str, object]],
    *,
    start_epoch_s: float | None = None,
    end_epoch_s: float | None = None,
) -> dict[str, float | str]:
    by_epoch: dict[float, list[dict[str, object]]] = {}
    for sample in samples:
        observed_epoch_s = float(sample["observed_epoch_s"])
        if (
            start_epoch_s is not None
            and observed_epoch_s < start_epoch_s
        ):
            continue
        if end_epoch_s is not None and observed_epoch_s > end_epoch_s:
            continue
        by_epoch.setdefault(observed_epoch_s, []).append(sample)
    gpu_values = []
    running_values = []
    waiting_values = []
    kv_values = []
    for epoch_samples in by_epoch.values():
        gpu_value = _optional_float(
            epoch_samples[0].get("gpu_utilization_pct")
        )
        if gpu_value is not None:
            gpu_values.append(gpu_value)
        running = [
            value
            for sample in epoch_samples
            if (value := _optional_float(sample.get("running"))) is not None
        ]
        waiting = [
            value
            for sample in epoch_samples
            if (value := _optional_float(sample.get("waiting"))) is not None
        ]
        kv = [
            value
            for sample in epoch_samples
            if (value := _optional_float(sample.get("kv_usage"))) is not None
        ]
        if running:
            running_values.append(sum(running))
        if waiting:
            waiting_values.append(sum(waiting))
        if kv:
            kv_values.append(max(kv))
    if not by_epoch:
        return {
            "resource_metrics_status": "unavailable:no_samples",
            "gpu_utilization_pct_mean": "",
            "gpu_utilization_pct_p95": "",
            "gpu_utilization_pct_max": "",
            "vllm_running_mean": "",
            "vllm_running_p95": "",
            "vllm_running_max": "",
            "vllm_waiting_mean": "",
            "vllm_waiting_p95": "",
            "vllm_waiting_max": "",
            "vllm_kv_usage_mean": "",
            "vllm_kv_usage_p95": "",
            "vllm_kv_usage_max": "",
        }
    status = (
        "ok"
        if gpu_values and running_values and waiting_values and kv_values
        else "unavailable:incomplete_samples"
    )
    return {
        "resource_metrics_status": status,
        **_distribution_fields("gpu_utilization_pct", gpu_values),
        **_distribution_fields("vllm_running", running_values),
        **_distribution_fields("vllm_waiting", waiting_values),
        **_distribution_fields("vllm_kv_usage", kv_values),
    }


def _validate_replay_starts(
    job_evidence: list[dict[str, object]],
    *,
    expected_start_epoch_s: float,
    arrival_offsets_s: tuple[float, ...],
    max_lateness_s: float,
    max_skew_s: float,
) -> None:
    if len(job_evidence) != len(arrival_offsets_s):
        raise RuntimeError("replay start evidence is incomplete")
    normalized_starts = []
    for index, (evidence, offset_s) in enumerate(
        zip(job_evidence, arrival_offsets_s)
    ):
        configured = float(
            evidence["replay_configured_start_epoch_s"]
        )
        observed = float(evidence["replay_observed_start_epoch_s"])
        actual_submit = float(
            evidence["replay_actual_submit_start_epoch_s"]
        )
        expected = expected_start_epoch_s + offset_s
        if abs(configured - expected) > 0.01:
            raise RuntimeError(
                f"job {index} replay configured start does not match runner"
            )
        barrier_lateness = observed - configured
        if barrier_lateness < -0.01:
            raise RuntimeError(
                f"job {index} crossed replay barrier before its deadline"
            )
        lateness = actual_submit - configured
        if lateness < -0.01 or lateness > max_lateness_s:
            raise RuntimeError(
                f"job {index} missed replay start deadline by "
                f"{lateness:.6f}s"
            )
        normalized_starts.append(actual_submit - offset_s)
    if (
        normalized_starts
        and max(normalized_starts) - min(normalized_starts) > max_skew_s
    ):
        raise RuntimeError("cross-job replay start skew exceeded limit")


def _validate_runner_topology(
    options: RunnerOptions,
    config: SharedVllmConfig,
) -> None:
    if len(options.metrics_urls) != len(config.endpoint_ids):
        raise ValueError(
            "runner requires one metrics URL per configured endpoint"
        )
    if len(set(options.metrics_urls)) != len(options.metrics_urls):
        raise ValueError("runner metrics URLs must be unique")
    configured_metrics = _csv_argument_values(
        config.common_args,
        "--model-metrics-urls",
    )
    if configured_metrics and configured_metrics != options.metrics_urls:
        raise ValueError(
            "runner metrics URLs must match profiler model metrics URLs"
        )
    configured_endpoints = _csv_argument_values(
        config.common_args,
        "--completion-endpoint-urls",
    )
    if configured_endpoints and len(configured_endpoints) != len(
        config.endpoint_ids
    ):
        raise ValueError(
            "completion endpoint count must match endpoint_ids"
        )


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
                record = _run_group(
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
            "commands": [_redact_command(command) for command in commands],
        },
    )
    observer = None
    processes = []
    log_handles = []
    resource_samples: list[dict[str, object]] = []
    credit_samples: list[dict[str, object]] = []
    group_launch_epoch_s = time.time()
    try:
        before = [
            scrape_prometheus_metrics(url)
            for url in options.metrics_urls
        ]
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
            "schema_version": 1,
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
            "job_predicted_work": json.dumps(
                [
                    evidence["predicted_work"]
                    for evidence in job_evidence
                ]
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


def _load_scenario(
    raw: object,
    request_limit: int,
    work_limit: int,
) -> SharedVllmScenario:
    if not isinstance(raw, dict):
        raise ValueError("each scenario must be an object")
    scenario_id = _nonempty_string(raw.get("scenario_id"), "scenario_id")
    if not _SCENARIO_ID.fullmatch(scenario_id):
        raise ValueError("scenario_id contains unsupported characters")
    policy = _nonempty_string(raw.get("policy"), "policy")
    if policy not in POLICIES:
        raise ValueError(f"unknown shared-vLLM policy: {policy}")
    job_count = _positive_integer(raw.get("job_count"), "job_count")
    rows_per_job = _positive_integer(
        raw.get("rows_per_job"),
        "rows_per_job",
    )
    if (
        policy == "static_partition"
        and (job_count > request_limit or job_count > work_limit)
    ):
        raise ValueError("static partition would assign zero capacity")
    weights = _positive_integer_tuple(
        raw.get("weights", [1] * job_count),
        "weights",
        job_count,
    )
    offsets = _nonnegative_float_tuple(
        raw.get("arrival_offsets_s", [0.0] * job_count),
        "arrival_offsets_s",
        job_count,
    )
    return SharedVllmScenario(
        scenario_id=scenario_id,
        policy=policy,
        job_count=job_count,
        rows_per_job=rows_per_job,
        weights=weights,
        arrival_offsets_s=offsets,
    )


def _local_limits(
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
    job_index: int,
) -> tuple[int, int]:
    if scenario.policy != "static_partition":
        return (
            config.request_limit_per_endpoint,
            config.work_limit_per_endpoint,
        )
    return (
        _partition_share(
            config.request_limit_per_endpoint,
            scenario.job_count,
            job_index,
        ),
        _partition_share(
            config.work_limit_per_endpoint,
            scenario.job_count,
            job_index,
        ),
    )


def _partition_share(total: int, count: int, index: int) -> int:
    base, remainder = divmod(total, count)
    return base + (1 if index < remainder else 0)


def _argument_list(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) for item in value
    ):
        raise ValueError(f"{label} must be a list of strings")
    expanded = tuple(_expand_text(item, label) for item in value)
    for item in expanded:
        flag = item.split("=", 1)[0]
        if flag in _RUNNER_OWNED_FLAGS:
            raise ValueError(f"{label} contains runner-owned flag {flag}")
    return expanded


def _argument_value(
    arguments: tuple[str, ...],
    flag: str,
    default: str,
) -> str:
    for index, item in enumerate(arguments):
        if item == flag:
            if index + 1 >= len(arguments):
                raise ValueError(f"{flag} requires a value")
            return arguments[index + 1]
        prefix = f"{flag}="
        if item.startswith(prefix):
            return item[len(prefix) :]
    return default


def _csv_argument_values(
    arguments: tuple[str, ...],
    flag: str,
) -> tuple[str, ...]:
    raw = _argument_value(arguments, flag, "")
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def _expand_text(value: str, label: str) -> str:
    missing = sorted(
        name
        for name in set(_ENV_REFERENCE.findall(value))
        if name not in os.environ
    )
    if missing:
        raise ValueError(
            f"{label} references unset environment variable(s): "
            + ", ".join(missing)
        )
    return _ENV_REFERENCE.sub(
        lambda match: os.environ[match.group(1)],
        value,
    )


def _expand_scalar(value: object, label: str) -> object:
    if not isinstance(value, str):
        return value
    expanded = _expand_text(value, label)
    if _ENV_REFERENCE.fullmatch(value):
        try:
            return json.loads(expanded)
        except json.JSONDecodeError:
            return expanded
    return expanded


def _nonempty_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer")
    return value


def _positive_integer(value: object, label: str) -> int:
    resolved = _integer(value, label)
    if resolved <= 0:
        raise ValueError(f"{label} must be positive")
    return resolved


def _nonnegative_integer(value: object, label: str) -> int:
    resolved = _integer(value, label)
    if resolved < 0:
        raise ValueError(f"{label} must be non-negative")
    return resolved


def _nonnegative_float(value: object, label: str) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be numeric") from exc
    if not math.isfinite(resolved) or resolved < 0:
        raise ValueError(f"{label} must be finite and non-negative")
    return resolved


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        resolved = float(value)
    except (TypeError, ValueError):
        return None
    return resolved if math.isfinite(resolved) else None


def _distribution_fields(
    prefix: str,
    values: list[float],
) -> dict[str, float | str]:
    if not values:
        return {
            f"{prefix}_mean": "",
            f"{prefix}_p95": "",
            f"{prefix}_max": "",
        }
    return {
        f"{prefix}_mean": sum(values) / len(values),
        f"{prefix}_p95": percentile(values, 0.95),
        f"{prefix}_max": max(values),
    }


def _positive_integer_tuple(
    value: object,
    label: str,
    expected: int,
) -> tuple[int, ...]:
    if not isinstance(value, list) or len(value) != expected:
        raise ValueError(f"{label} must contain one value per job")
    return tuple(_positive_integer(item, label) for item in value)


def _nonnegative_float_tuple(
    value: object,
    label: str,
    expected: int,
) -> tuple[float, ...]:
    if not isinstance(value, list) or len(value) != expected:
        raise ValueError(f"{label} must contain one value per job")
    resolved = []
    for item in value:
        if (
            isinstance(item, bool)
            or not isinstance(item, (int, float))
            or not math.isfinite(float(item))
            or item < 0
        ):
            raise ValueError(f"{label} values must be finite and non-negative")
        resolved.append(float(item))
    return tuple(resolved)


class _RayCreditObserver:
    def __init__(
        self,
        address: str,
        namespace: str,
        actor_name: str,
        endpoint_ids: tuple[str, ...],
    ) -> None:
        import ray

        self.ray = ray
        self.namespace = namespace
        self.actor_name = actor_name
        self.endpoint_ids = endpoint_ids
        self.actor = None
        if not ray.is_initialized():
            ray.init(address=address, ignore_reinit_error=True)

    def _resolve_actor(self):
        if self.actor is not None:
            return self.actor
        try:
            self.actor = self.ray.get_actor(
                self.actor_name,
                namespace=self.namespace,
            )
        except ValueError:
            return None
        return self.actor

    def sample(self, origin_epoch_s: float) -> list[dict[str, object]]:
        actor = self._resolve_actor()
        if actor is None:
            return []
        observed_epoch_s = time.time()
        snapshots = self.ray.get(
            [
                actor.snapshot.remote(endpoint_id)
                for endpoint_id in self.endpoint_ids
            ]
        )
        return [
            {
                "schema_version": 1,
                "observed_epoch_s": observed_epoch_s,
                "elapsed_s": observed_epoch_s - origin_epoch_s,
                **_snapshot_mapping(snapshot),
            }
            for snapshot in snapshots
        ]

    def final_snapshots(self) -> list[dict[str, object]]:
        actor = self._resolve_actor()
        if actor is None:
            raise RuntimeError("shared credit actor was never observed")
        snapshots = self.ray.get(
            [
                actor.snapshot.remote(endpoint_id)
                for endpoint_id in self.endpoint_ids
            ]
        )
        return [_snapshot_mapping(snapshot) for snapshot in snapshots]

    def cleanup(self) -> None:
        if self.actor is not None:
            self.ray.kill(self.actor, no_restart=True)


def _snapshot_mapping(snapshot) -> dict[str, object]:
    mapping = asdict(snapshot)
    for key, value in tuple(mapping.items()):
        if isinstance(value, (list, tuple)):
            mapping[key] = json.dumps(value)
    return mapping


def _resource_sample(
    metrics_urls: tuple[str, ...],
    origin_epoch_s: float,
) -> list[dict[str, object]]:
    observed_epoch_s = time.time()
    gpu = gpu_metadata()
    rows = []
    for endpoint_index, metrics_url in enumerate(metrics_urls):
        metrics = scrape_prometheus_metrics(metrics_url)
        rows.append(
            {
                "schema_version": 1,
                "observed_epoch_s": observed_epoch_s,
                "elapsed_s": observed_epoch_s - origin_epoch_s,
                "endpoint_index": endpoint_index,
                "metrics_url": metrics_url,
                "running": metrics.get(
                    "vllm:num_requests_running",
                    "",
                ),
                "waiting": metrics.get(
                    "vllm:num_requests_waiting",
                    "",
                ),
                "kv_usage": metrics.get(
                    "vllm:kv_cache_usage_perc",
                    "",
                ),
                "gpu_metrics_status": gpu["gpu_metrics_status"],
                "gpu_utilization_pct": gpu["gpu_utilization_pct"],
                "gpu_memory_used_mib": gpu["gpu_memory_used_mib"],
                "gpu_power_w": gpu["gpu_power_w"],
            }
        )
    return rows


def _validate_job_evidence(
    options: RunnerOptions,
    scenario: SharedVllmScenario,
    identity: GroupRunIdentity,
    job_index: int,
) -> dict[str, object]:
    run_stem = (
        f"{identity.order_index:03d}_{identity.phase}_"
        f"{identity.repeat_index}_{scenario.scenario_id}"
    )
    job_stem = options.output_dir / "jobs" / f"{run_stem}_job{job_index}"
    summary_rows = _read_csv(job_stem.with_suffix(".runs.csv"))
    request_rows = _read_csv(job_stem.with_suffix(".requests.csv"))
    submission_rows = _read_csv(job_stem.with_suffix(".submissions.csv"))
    if len(summary_rows) != 1 or summary_rows[0].get("status") != "ok":
        raise RuntimeError(f"job {job_index} has no unique successful summary")
    summary = summary_rows[0]
    if int(summary.get("total_rows", -1)) != scenario.rows_per_job:
        raise RuntimeError(f"job {job_index} processed an unexpected row count")
    if len(request_rows) != scenario.rows_per_job:
        raise RuntimeError(f"job {job_index} request trace is not exactly-once")
    if len(submission_rows) != scenario.rows_per_job:
        raise RuntimeError(
            f"job {job_index} submission trace is not exactly-once"
        )
    request_ids = [row.get("request_id", "") for row in request_rows]
    if len(set(request_ids)) != len(request_ids) or "" in request_ids:
        raise RuntimeError(f"job {job_index} has duplicate request IDs")
    if any(not _request_trace_succeeded(row) for row in request_rows):
        raise RuntimeError(f"job {job_index} contains failed requests")
    arrival = [float(row["arrival_epoch_s"]) for row in request_rows]
    completion = [float(row["completion_epoch_s"]) for row in request_rows]
    e2e = [float(row["e2e_s"]) for row in request_rows]
    submission_starts = [
        float(row["submit_epoch_s"]) for row in request_rows
    ]
    slo_met = [
        str(row.get("slo_met", "")).strip().lower() == "true"
        for row in request_rows
    ]
    jct_s = max(completion) - min(arrival)
    completed_in_slo = sum(slo_met)
    predicted_work = sum(
        int(row["prompt_tokens"])
        + int(
            row["client_estimated_output_tokens"]
            or row["estimated_output_tokens"]
        )
        for row in request_rows
    )
    endpoint_counts: dict[str, int] = {}
    for row in request_rows:
        endpoint_id = row["endpoint_id"]
        endpoint_counts[endpoint_id] = (
            endpoint_counts.get(endpoint_id, 0) + 1
        )
    return {
        "jct_s": jct_s,
        "p99_s": percentile(e2e, 0.99),
        "completion_lag_s": max(completion) - max(arrival),
        "slo_violation_ratio": 1.0 - completed_in_slo / len(slo_met),
        "slo_goodput_per_s": completed_in_slo / jct_s,
        "predicted_work": predicted_work,
        "endpoint_counts": endpoint_counts,
        "actor_worker_failures": int(
            summary.get("actor_worker_failures", "0") or 0
        ),
        "replay_configured_start_epoch_s": float(
            summary.get("arrival_replay_start_epoch_s", "0") or 0
        ),
        "replay_observed_start_epoch_s": float(
            summary.get(
                "arrival_replay_observed_start_epoch_s",
                "0",
            )
            or 0
        ),
        "replay_actual_submit_start_epoch_s": min(submission_starts),
    }


def _request_trace_succeeded(row: dict[str, str]) -> bool:
    return (
        row.get("status", "").strip().lower() == "completed"
        and not row.get("error_type", "").strip()
    )


def _validate_final_credit(
    config: SharedVllmConfig,
    snapshots: list[dict[str, object]],
) -> None:
    if len(snapshots) != len(config.endpoint_ids):
        raise RuntimeError("shared credit final snapshot is incomplete")
    for snapshot in snapshots:
        if (
            int(snapshot["active_requests"]) != 0
            or int(snapshot["active_work"]) != 0
            or int(snapshot["waiting_requests"]) != 0
            or int(snapshot["waiting_work"]) != 0
        ):
            raise RuntimeError("shared credit did not return to zero")
        if (
            int(snapshot["max_active_requests_seen"])
            > config.request_limit_per_endpoint
        ):
            raise RuntimeError("shared request limit was exceeded")
        if (
            int(snapshot["max_active_work_seen"])
            > config.work_limit_per_endpoint
        ):
            raise RuntimeError("shared work limit was exceeded")


def _terminate_processes(processes: list[subprocess.Popen]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
    deadline = time.monotonic() + 10.0
    for process in processes:
        if process.poll() is not None:
            continue
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def _write_trace_rows_atomic(
    path: Path,
    rows: list[dict[str, object]],
) -> None:
    if not rows:
        return
    fieldnames = list(rows[0])
    if any(list(row) != fieldnames for row in rows):
        raise ValueError("trace rows have inconsistent schemas")
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _load_group_record(
    path: Path,
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
    identity: GroupRunIdentity,
) -> dict[str, object]:
    record = json.loads(path.read_text(encoding="utf-8"))
    expected = {
        "schema_version": 1,
        "experiment_id": config.experiment_id,
        "scenario_id": scenario.scenario_id,
        "phase": identity.phase,
        "repeat_index": identity.repeat_index,
        "order_index": identity.order_index,
        "policy": scenario.policy,
        "job_count": scenario.job_count,
        "rows_per_job": scenario.rows_per_job,
        "run_instance_id": _run_instance_id(path.parent.parent),
        "incidents": 0,
        "actor_worker_failures": 0,
    }
    for key, value in expected.items():
        if record.get(key) != value:
            raise RuntimeError(
                f"completed group record does not match {key}"
            )
    return record


def _rewrite_group_runs(
    path: Path,
    output_dir: Path,
    completed_runs: list[dict[str, object]],
) -> None:
    records = []
    for completed in sorted(
        completed_runs,
        key=lambda item: int(item["order_index"]),
    ):
        relative = Path(str(completed.get("record_path", "")))
        if (
            not relative.parts
            or relative.parts[0] != "records"
            or ".." in relative.parts
            or relative.is_absolute()
        ):
            raise RuntimeError("manifest contains an unsafe record_path")
        record_path = output_dir / relative
        if not record_path.exists():
            raise RuntimeError("manifest completed record is missing")
        records.append(json.loads(record_path.read_text(encoding="utf-8")))
    if not records:
        return
    fieldnames = list(records[0])
    if any(list(record) != fieldnames for record in records):
        raise RuntimeError("completed group records have mixed schemas")
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    os.replace(temporary, path)


def _group_failure_path(output_dir: Path, run_stem: str) -> Path:
    return output_dir / "traces" / f"{run_stem}.failure.json"


def _group_artifacts_exist(output_dir: Path, run_stem: str) -> bool:
    patterns = (
        ("jobs", f"{run_stem}_job*"),
        ("logs", f"{run_stem}_job*"),
        ("traces", f"{run_stem}.*"),
    )
    return any(
        any((output_dir / child).glob(pattern))
        for child, pattern in patterns
    )


def _coordinator_name(
    experiment_id: str,
    run_instance_id: str,
    run_stem: str,
) -> str:
    raw = f"credit-{experiment_id}-{run_instance_id}-{run_stem}"
    return re.sub(r"[^A-Za-z0-9_.-]", "-", raw)


def _run_instance_id(output_dir: Path) -> str:
    resolved = str(output_dir.resolve())
    digest = hashlib.sha256(resolved.encode("utf-8")).hexdigest()[:12]
    label = re.sub(r"[^A-Za-z0-9_.-]", "-", output_dir.name)
    return f"{label}-{digest}"


def _run_stem(
    scenario: SharedVllmScenario,
    identity: GroupRunIdentity,
) -> str:
    return (
        f"{identity.order_index:03d}_{identity.phase}_"
        f"{identity.repeat_index}_{scenario.scenario_id}"
    )


def _config_fingerprint(config: SharedVllmConfig, schedule) -> str:
    payload = {
        "config": _redacted_config(config),
        "schedule": [asdict(item) for item in schedule],
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _redacted_config(config: SharedVllmConfig) -> dict[str, object]:
    return {
        "experiment_id": config.experiment_id,
        "seed": config.seed,
        "warmup_runs_per_scenario": config.warmup_runs_per_scenario,
        "formal_repeats": config.formal_repeats,
        "endpoint_ids": config.endpoint_ids,
        "request_limit_per_endpoint": config.request_limit_per_endpoint,
        "work_limit_per_endpoint": config.work_limit_per_endpoint,
        "credit_quantum": config.credit_quantum,
        "shared_credit_namespace": config.shared_credit_namespace,
        "gpu_peak_tflops": config.gpu_peak_tflops,
        "mfu_precision": config.mfu_precision,
        "common_args": _redact_command(list(config.common_args)),
        "scenarios": [asdict(item) for item in config.scenarios],
        "service_metadata": dict(config.service_metadata),
    }


def _redact_command(command: list[str]) -> list[str]:
    secret_flags = {
        "--completion-api-key",
        "--database-url",
        "--embedding-api-key",
    }
    redacted = []
    redact_next = False
    for item in command:
        if redact_next:
            redacted.append("***")
            redact_next = False
            continue
        flag, separator, _ = item.partition("=")
        if separator and flag in secret_flags:
            redacted.append(f"{flag}=***")
            continue
        redacted.append(item)
        redact_next = item in secret_flags
    return redacted


def _repository_commit() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=_CODE_ROOT.parent,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _load_resume_manifest(path: Path, expected: dict) -> dict:
    if not path.exists():
        raise ValueError("--resume requires an existing manifest.json")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    for key in (
        "schema_version",
        "experiment_id",
        "config_fingerprint",
        "repository_commit",
        "run_instance_id",
        "redacted_config",
        "schedule",
    ):
        if manifest.get(key) != expected[key]:
            raise ValueError(f"resume manifest does not match {key}")
    if not isinstance(manifest.get("completed_runs"), list):
        raise ValueError("resume manifest has invalid completed_runs")
    if not isinstance(manifest.get("incidents"), list):
        raise ValueError("resume manifest has invalid incidents")
    return manifest


def _write_json_atomic(path: Path, payload: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, path)
