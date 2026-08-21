"""Validate that matched-system release contracts reach the real executors.

The module compares the frozen matrix identity with the native-framework and
Project runner configurations before any cell is dispatched.  It deliberately
does not execute a framework, acquire a lease, or create evidence directories.
"""

from __future__ import annotations

from pathlib import Path

from src.baselines.common.manifests import read_manifest
from src.baselines.text.orchestration.native_multijob import NativeMultiJobConfig
from src.experiments.saor.native_system_matched import (
    MatchedArm,
    MatchedSystemConfig,
    endpoint_auxiliary_url,
    sha256_file,
    validate_executor_job_manifests,
    validate_native_calibration_selection,
)
from src.experiments.shared_vllm.config import SharedVllmConfig


def argument_value(arguments: tuple[str, ...], flag: str) -> str:
    """Return the value following a required Project runner flag."""

    try:
        index = arguments.index(flag)
    except ValueError as exc:
        raise ValueError(f"Project config is missing {flag}") from exc
    if index + 1 >= len(arguments):
        raise ValueError(f"Project config {flag} has no value")
    return arguments[index + 1]


def csv_argument_values(arguments: tuple[str, ...], flag: str) -> tuple[str, ...]:
    """Return a required comma-separated runner flag as a non-empty tuple."""

    values = tuple(
        item.strip() for item in argument_value(arguments, flag).split(",")
        if item.strip()
    )
    if not values:
        raise ValueError(f"Project config {flag} must not be empty")
    return values


def canonical_config_path(
    value: str | Path | None,
    config_path: str | Path | None = None,
) -> str | None:
    """Resolve a path relative to the configuration file that declares it."""

    if value is None:
        return None
    path = Path(value)
    if not path.is_absolute() and config_path is not None:
        path = Path(config_path).parent / path
    return str(path.resolve())


def _project_common_contract(project: SharedVllmConfig) -> dict[str, object]:
    """Extract the Project runner fields that the matched contract freezes."""

    return {
        "protocol": argument_value(project.common_args, "--completion-protocol"),
        "output_cap": int(
            argument_value(project.common_args, "--completion-max-tokens")
        ),
        "organizer": argument_value(project.common_args, "--organizer"),
        "executor": argument_value(project.common_args, "--executor"),
        "endpoint_urls": csv_argument_values(
            project.common_args, "--completion-endpoint-urls"
        ),
        "metrics_urls": csv_argument_values(
            project.common_args, "--model-metrics-urls"
        ),
        "database_url": argument_value(project.common_args, "--database-url"),
        "gpu_peak_tflops_per_gpu": float(argument_value(
            project.common_args, "--gpu-peak-tflops"
        )),
        "mfu_precision": argument_value(project.common_args, "--mfu-precision"),
        "performance_writeback_mode": argument_value(
            project.common_args, "--writeback-mode"
        ),
        "workload_name": argument_value(
            project.common_args, "--source-workload-name"
        ),
        "actor_topology": tuple(sorted({
            "workers": int(argument_value(
                project.common_args, "--actor-workers-per-endpoint"
            )),
            "concurrency": int(argument_value(
                project.common_args, "--ray-actor-max-concurrency"
            )),
            "cpus_per_worker": float(argument_value(
                project.common_args, "--ray-worker-num-cpus"
            )),
        }.items())),
        "batching_contract": tuple(sorted({
            "policy": argument_value(project.common_args, "--batching-policy"),
            "token_budget": int(argument_value(
                project.common_args, "--token-budget"
            )),
            "token_budget_policy": argument_value(
                project.common_args, "--token-budget-policy"
            ),
        }.items())),
    }


def _native_comparisons(
    arm: MatchedArm,
    native: NativeMultiJobConfig,
    native_by_id: dict[str, object],
) -> dict[str, tuple[object, object]]:
    actual = native_by_id.get(arm.arm_id)
    if actual is None:
        raise ValueError(f"native config is missing arm {arm.arm_id}")
    if native.source is None:
        raise ValueError("native matrix config is missing explicit source")

    # NativeMultiJobArm is deliberately duck-typed here only after the ID lookup;
    # the loaded config is the typed boundary and owns these fields.
    expected_source = dict(arm.source)
    validate_executor_job_manifests(
        arm,
        tuple(job.manifest for job in actual.jobs),
        tuple(len(read_manifest(job.manifest)) for job in actual.jobs),
    )
    validate_native_calibration_selection(
        arm,
        adapter=actual.adapter,
        concurrency_per_endpoint=actual.concurrency_per_endpoint,
        batch_size=actual.batch_size,
    )
    return {
        "endpoint_ids": (native.endpoint_ids, arm.endpoint_ids),
        "service_signature": (native.service_signature, arm.service_signature),
        "protocol": (native.protocol, arm.protocol),
        "output_cap": (native.output_cap, arm.output_cap),
        "arrival_offsets_s": (
            tuple(job.offset_s for job in actual.jobs), arm.arrival_offsets_s
        ),
        "job_internal_arrival_contract": (
            native.job_internal_arrival_contract,
            arm.job_internal_arrival_contract,
        ),
        "organizer": (native.organizer, arm.organizer),
        "mfu.status": (native.mfu_status, arm.mfu_contract.status),
        "mfu.gpu_peak_tflops_per_gpu": (
            native.gpu_peak_tflops_per_gpu,
            arm.mfu_contract.gpu_peak_tflops_per_gpu,
        ),
        "mfu.precision": (native.mfu_precision, arm.mfu_contract.precision),
        "mfu.reason": (native.mfu_reason, arm.mfu_contract.reason),
        "performance_writeback_mode": (
            native.performance_writeback_mode,
            arm.performance_writeback_mode,
        ),
        "source.database_url": (
            native.source.database_url, expected_source["database_url"]
        ),
        "source.workload_name": (
            native.source.workload_name, expected_source["workload_name"]
        ),
    }


def _project_calibration_identity(
    project: SharedVllmConfig,
    arm: MatchedArm,
    *,
    matched_config_path: str | Path | None,
    project_config_path: str | Path | None,
) -> tuple[tuple[object, object], tuple[object, object]]:
    actual_path = canonical_config_path(
        project.calibration_contract.path,
        project_config_path,
    ) if project.calibration_contract is not None else None
    expected_path = canonical_config_path(
        arm.calibration_path,
        matched_config_path,
    )
    actual_sha = sha256_file(Path(actual_path)) if actual_path is not None else None
    return (
        (actual_path, expected_path),
        (actual_sha, arm.calibration_sha256),
    )


def _project_comparisons(
    arm: MatchedArm,
    project: SharedVllmConfig,
    project_by_id: dict[str, object],
    common: dict[str, object],
    *,
    matched_config_path: str | Path | None,
    project_config_path: str | Path | None,
) -> dict[str, tuple[object, object]]:
    actual = project_by_id.get(arm.arm_id)
    if actual is None:
        raise ValueError(f"Project config is missing scenario {arm.arm_id}")
    request_limit, work_limit = actual.endpoint_limits(
        project.request_limit_per_endpoint,
        project.work_limit_per_endpoint,
    )
    row_counts = tuple(
        actual.row_count(index) for index in range(actual.job_count)
    )
    validate_executor_job_manifests(
        arm,
        tuple(actual.request_manifests),
        row_counts,
    )
    calibration_path, calibration_sha = _project_calibration_identity(
        project,
        arm,
        matched_config_path=matched_config_path,
        project_config_path=project_config_path,
    )
    expected_source = dict(arm.source)
    return {
        "endpoint_ids": (project.endpoint_ids, arm.endpoint_ids),
        "service_signature": (project.service_signature, arm.service_signature),
        "protocol": (common["protocol"], arm.protocol),
        "output_cap": (common["output_cap"], arm.output_cap),
        "arrival_offsets_s": (actual.arrival_offsets_s, arm.arrival_offsets_s),
        "job_internal_arrival_contract": (
            project.job_internal_arrival_contract,
            arm.job_internal_arrival_contract,
        ),
        "organizer": (common["organizer"], arm.organizer),
        "executor": (common["executor"], arm.project_value("executor")),
        "model_service_scheduler": (
            dict(project.service_signature).get("scheduler"),
            arm.project_value("model_service_scheduler"),
        ),
        "mfu.gpu_peak_tflops_per_gpu": (
            common["gpu_peak_tflops_per_gpu"],
            arm.mfu_contract.gpu_peak_tflops_per_gpu,
        ),
        "mfu.precision": (common["mfu_precision"], arm.mfu_contract.precision),
        "performance_writeback_mode": (
            common["performance_writeback_mode"],
            arm.performance_writeback_mode,
        ),
        "source.database_url": (
            common["database_url"], expected_source["database_url"]
        ),
        "source.workload_name": (
            common["workload_name"], expected_source["workload_name"]
        ),
        "source_row_offsets": (
            actual.source_row_offsets,
            tuple(expected_source.get(
                "source_row_offsets", (0,) * len(actual.request_manifests)
            )),
        ),
        "k_per_endpoint": (request_limit, arm.project_value("k_per_endpoint")),
        "work_limit_per_endpoint": (
            work_limit, arm.project_value("work_limit_per_endpoint")
        ),
        "ready_bytes": (
            project.ready_payload_bytes_limit_per_job,
            arm.project_value("ready_bytes"),
        ),
        "actor_topology": (
            common["actor_topology"], arm.project_value("actor_topology")
        ),
        "batching_contract": (
            common["batching_contract"], arm.project_value("batching_contract")
        ),
        "policy": (actual.policy, arm.project_value("policy")),
        "ready_observation": (
            actual.ready_observation_contract,
            arm.project_value("ready_observation") or "single_head",
        ),
        "debt_caps": (
            actual.debt_cap_fractions, arm.project_value("debt_caps") or ()
        ),
        "rows_per_jobs": (row_counts, tuple(job.rows for job in arm.job_manifests)),
        "calibration_path": calibration_path,
        "calibration_sha256": calibration_sha,
    }


def validate_executor_bindings(
    matched: MatchedSystemConfig,
    native: NativeMultiJobConfig,
    project: SharedVllmConfig,
    *,
    matched_config_path: str | Path | None = None,
    project_config_path: str | Path | None = None,
    runner_metrics_urls: tuple[str, ...] | None = None,
    runner_health_url: str | None = None,
) -> None:
    """Fail before dispatch when a concrete executor drifts from the matrix."""

    native_by_id = {item.arm_id: item for item in native.arms}
    project_by_id = {item.scenario_id: item for item in project.scenarios}
    common = _project_common_contract(project)
    identity = dict(matched.service_identity)
    expected_metadata = tuple(sorted({
        "vllm_version": identity["service"],
        "enforce_eager": identity["enforce_eager"],
        "compilation_mode": identity["compilation_mode"],
        "chunked_prefill": identity["chunked_prefill"],
        "max_num_batched_tokens": identity["max_num_batched_tokens"],
        "max_num_seqs": identity["max_num_seqs"],
        "gpu_memory_utilization": identity["gpu_memory_utilization"],
        "prefix_caching": identity["prefix_caching"],
        "mfu_metrics": identity["mfu_metrics"],
    }.items()))
    expected_metrics = tuple(
        endpoint_auxiliary_url(url, "/metrics") for url in matched.endpoint_urls
    )
    global_comparisons = {
        "native.endpoint_urls": (native.endpoint_urls, matched.endpoint_urls),
        "project.endpoint_urls": (common["endpoint_urls"], matched.endpoint_urls),
        "project.metrics_urls": (common["metrics_urls"], expected_metrics),
        "native.service_identity": (
            native.service_identity, matched.service_identity
        ),
        "project.service_identity": (
            project.service_identity, matched.service_identity
        ),
        "native.service.prefix_caching": (
            native.service_prefix_caching,
            "enabled" if identity["prefix_caching"] else "disabled",
        ),
        "native.service.max_num_seqs": (
            native.service_max_num_seqs, identity["max_num_seqs"]
        ),
        "native.service.max_num_batched_tokens": (
            native.service_max_num_batched_tokens,
            identity["max_num_batched_tokens"],
        ),
        "project.service_metadata": (
            project.service_metadata, expected_metadata
        ),
    }
    if runner_metrics_urls is not None:
        global_comparisons["runner.metrics_urls"] = (
            runner_metrics_urls, expected_metrics
        )
    if runner_health_url is not None:
        global_comparisons["runner.health_url"] = (
            runner_health_url,
            endpoint_auxiliary_url(matched.endpoint_urls[0], "/health"),
        )
    global_drift = [
        name for name, values in global_comparisons.items()
        if values[0] != values[1]
    ]
    if global_drift:
        raise ValueError(
            "matrix executor endpoint contract drift: " + ", ".join(global_drift)
        )
    for arm in matched.arms:
        if arm.kind == "native":
            comparisons = _native_comparisons(arm, native, native_by_id)
        else:
            comparisons = _project_comparisons(
                arm,
                project,
                project_by_id,
                common,
                matched_config_path=matched_config_path,
                project_config_path=project_config_path,
            )
        drift = [
            name for name, values in comparisons.items()
            if values[0] != values[1]
        ]
        if drift:
            raise ValueError(
                f"{arm.arm_id} executor contract drift: {', '.join(drift)}"
            )
