"""CLIP output fields, derived measurements, and result files."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

from ...modalities.image.metrics import IMAGE_METRIC_DEFINITIONS, image_run_derived_metrics
from . import CODE_ROOT

if TYPE_CHECKING:
    from ...modalities.image.execution import EmbeddingCapture
    from .setup import ImageRunSetup
    from .execution import ObservedImageRun

CSV_FIELDS = (
    "arm",
    "baseline_role",
    "implementation_provenance",
    "scheduler_owner",
    "custom_scheduling_code",
    "formal_baseline_eligible",
    "upstream_source",
    "phase",
    "repeat_index",
    "workload_name",
    "source_doc_ids_sha256",
    "expected_source_doc_ids_sha256",
    "source_manifest_match",
    "expected_input_encoded_bytes",
    "rows",
    "unique_images",
    "dataset_passes",
    "batch_size",
    "input_size",
    "cpu_workers",
    "gpu_workers",
    "model_workers",
    "gpus_per_model_worker",
    "source_shards",
    "source_cpu_threads",
    "max_active_batches",
    "ray_cluster_num_cpus",
    "host_cpu_slots_detected",
    "declared_external_cpus",
    "declared_source_cpus",
    "declared_preprocess_cpus",
    "declared_model_cpus",
    "declared_total_cpus",
    "resource_budget_semantics",
    "ray_address_mode",
    "ray_data_actor_pool_mode",
    "formal_start_epoch_s_planned",
    "formal_start_epoch_s_actual",
    "formal_start_lateness_s",
    "torch_intraop_threads_per_worker",
    "torch_interop_threads_per_worker",
    "worker_setup_s",
    "worker_setup_accounting",
    "operator_e2e_s",
    "first_output_s",
    "first_output_semantics",
    "image_derived_metrics_status",
    "post_first_output_s",
    "first_output_fraction_of_e2e",
    "post_first_output_fraction_of_e2e",
    "first_output_cross_scale_semantics",
    "steady_state_min_s",
    "steady_state_duration_gate_met",
    "throughput_cross_scale_semantics",
    "images_per_s",
    "batch_service_p50_s",
    "batch_service_p95_s",
    "batch_service_p99_s",
    "batch_service_semantics",
    "batch_completion_wall_p50_s",
    "batch_completion_wall_p95_s",
    "batch_completion_wall_p99_s",
    "batch_actor_service_p50_s",
    "batch_actor_service_p95_s",
    "batch_actor_service_p99_s",
    "batch_unattributed_wait_p50_s",
    "batch_unattributed_wait_p95_s",
    "batch_unattributed_wait_p99_s",
    "batch_preprocess_p50_s",
    "batch_preprocess_p95_s",
    "batch_preprocess_p99_s",
    "batch_host_copy_p50_s",
    "batch_host_copy_p95_s",
    "batch_host_copy_p99_s",
    "batch_h2d_p50_s",
    "batch_h2d_p95_s",
    "batch_h2d_p99_s",
    "batch_forward_p50_s",
    "batch_forward_p95_s",
    "batch_forward_p99_s",
    "batch_d2h_p50_s",
    "batch_d2h_p95_s",
    "batch_d2h_p99_s",
    "batch_source_next_p50_s",
    "batch_source_next_p95_s",
    "source_next_total_s",
    "batch_driver_materialize_p50_s",
    "batch_driver_materialize_p95_s",
    "driver_materialize_total_s",
    "batch_submit_p50_s",
    "batch_submit_p95_s",
    "submit_total_s",
    "detailed_stage_timing",
    "input_encoded_bytes",
    "avg_encoded_bytes",
    "max_encoded_bytes",
    "telemetry_encoded_bytes",
    "input_tensor_bytes",
    "device_input_bytes",
    "output_embedding_bytes",
    "logical_h2d_effective_gbps",
    "logical_d2h_effective_gbps",
    "submitted_batches",
    "pending_batches_peak",
    "project_execution_mode",
    "hse_encoded_bytes_limit",
    "hse_ready_bytes_limit",
    "hse_ready_work_limit",
    "hse_prepare_inflight_limit",
    "hse_model_inflight_limit",
    "hse_encoded_bytes_peak",
    "hse_ready_bytes_peak",
    "hse_prepare_inflight_peak",
    "hse_model_inflight_peak",
    "batch_prepare_queue_p50_s",
    "batch_prepare_queue_p95_s",
    "batch_ready_residence_p50_s",
    "batch_ready_residence_p95_s",
    "output_rows",
    "exactly_once",
    "embedding_checksum",
    "embedding_sum_all",
    "embedding_digest_xor_rounded5",
    "max_norm_error",
    "cpu_system_mean_pct",
    "cpu_system_peak_pct",
    "cpu_busy_cores_mean",
    "cpu_busy_cores_peak",
    "cpu_per_core_peak_pct",
    "cpu_logical_count",
    "cpu_samples",
    "cpu_core_seconds_estimate",
    "cpu_core_seconds_per_image",
    "images_per_cpu_core_second",
    "host_memory_mean_pct",
    "host_memory_peak_pct",
    "host_memory_available_min_mib",
    "host_disk_read_bytes",
    "host_disk_write_bytes",
    "host_net_recv_bytes",
    "host_net_sent_bytes",
    "host_context_switches",
    "host_interrupts",
    "gpu_util_mean_pct",
    "gpu_util_peak_pct",
    "gpu_active_util_mean_pct",
    "gpu_active_util_peak_pct",
    "gpu_active_device_count",
    "gpu_active_devices_json",
    "gpu_active_power_mean_w",
    "gpu_active_power_peak_w",
    "gpu_energy_estimate_j",
    "gpu_active_sm_clock_mean_mhz",
    "gpu_active_memory_clock_mean_mhz",
    "gpu_active_pcie_generation",
    "gpu_active_pcie_width",
    "gpu_active_pcie_generation_max",
    "gpu_active_pcie_width_max",
    "gpu_memory_peak_mib",
    "gpu_samples",
    "gpu_per_device_json",
    "gpu_seconds",
    "gpu_seconds_per_image",
    "images_per_gpu_s",
    "images_per_joule",
    "joules_per_1k_images",
    "host_disk_read_bytes_per_image",
    "host_disk_write_bytes_per_image",
    "host_net_recv_bytes_per_image",
    "host_net_sent_bytes_per_image",
    "engine_stats_text",
    "engine_stats_semantics",
    "model_revision",
    "processor_revision",
    "dtype",
    "embedding_dimension",
    "embedding_output_contract_requested",
    "embedding_output_contract_effective",
    "embedding_normalization_in_timed_boundary",
    "embedding_normalization_owner",
    "daft_version",
    "ray_version",
    "torch_version",
    "transformers_version",
    "gpu_name",
    "model_flops_per_image",
    "gpu_peak_flops_per_s",
    "estimated_e2e_mfu",
    "server_version",
    "pgvector_version",
    "git_commit",
)


def embedding_output_contract_metadata(
    arm: str,
    requested_contract: str,
) -> dict[str, object]:
    """Describe the output contract that the timed arm actually returns."""
    output_is_normalized = (
        requested_contract == "l2_normalized" or arm != "daft_builtin_embed"
    )
    return {
        "embedding_output_contract_requested": requested_contract,
        "embedding_output_contract_effective": (
            "l2_normalized" if output_is_normalized else "provider_raw"
        ),
        "embedding_normalization_in_timed_boundary": output_is_normalized,
        "embedding_normalization_owner": (
            "baseline_adapter"
            if arm == "daft_builtin_embed" and output_is_normalized
            else "model_actor"
            if output_is_normalized
            else "provider_native_output"
        ),
    }


def percentile(values: tuple[float, ...], fraction: float) -> float | str:
    if not values:
        return ""
    ordered = sorted(values)
    position = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return ordered[position]


def logical_bandwidth_gbps(byte_count: int, durations_s: tuple[float, ...]) -> float | str:
    """Return logical payload bytes / measured stage wall, not a PCIe counter."""
    total_s = sum(durations_s)
    return byte_count / total_s / 1e9 if byte_count > 0 and total_s > 0 else ""


def append_csv(path: Path, row: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, lineterminator="\n")
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def git_commit() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=CODE_ROOT.parent,
        capture_output=True,
        check=False,
        text=True,
    )
    return completed.stdout.strip() if completed.returncode == 0 else "unknown"


def write_embedding_capture(
    args: argparse.Namespace,
    setup: ImageRunSetup,
    capture: EmbeddingCapture,
    output_contract_metadata: dict[str, object],
    *,
    daft_module,
    ray_module,
    torch_module,
    transformers_module,
) -> None:
    target = setup.embedding_target
    sidecar = setup.embedding_sidecar
    if target is None or sidecar is None:
        raise RuntimeError("embedding capture paths are not configured")
    doc_ids, embeddings = capture.finish()
    if target.exists() or sidecar.exists():
        raise SystemExit(
            "ERROR: --save-embeddings target or sidecar already exists; "
            f"refuse overwrite: {target}, {sidecar}"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    import numpy as np

    np.savez_compressed(
        target,
        embeddings=embeddings,
        doc_ids=np.asarray(doc_ids, dtype=str),
    )
    manifest = {
        "arm": args.arm,
        "phase": args.phase,
        "repeat_index": args.repeat_index,
        "model_revision": args.model,
        "processor_revision": (
            "provider_resolved_from_model"
            if args.arm == "daft_builtin_embed"
            else setup.processor
        ),
        "dtype": "provider_default" if args.arm == "daft_builtin_embed" else args.dtype,
        "embedding_dimension": args.embedding_dimension,
        "rows": int(embeddings.shape[0]),
        "dimension": int(embeddings.shape[1]) if embeddings.ndim == 2 else None,
        "diagnostic_capture_enabled": True,
        "timing_valid_for_performance": False,
        **output_contract_metadata,
        "note": (
            "captured embeddings are the post-arm output under the recorded effective "
            "output contract; the parity probe L2-normalizes defensively before "
            "comparison; capture timing is invalid for performance claims"
        ),
        "daft_version": daft_module.__version__,
        "ray_version": ray_module.__version__,
        "torch_version": torch_module.__version__,
        "transformers_version": transformers_module.__version__,
    }
    sidecar.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build_image_result_row(
    args: argparse.Namespace,
    setup: ImageRunSetup,
    observed: ObservedImageRun,
    *,
    daft_module,
    ray_module,
    torch_module,
    transformers_module,
) -> dict[str, object]:
    result = observed.result
    operator_e2e_s = result.total_s + observed.worker_setup_s
    first_output_s = result.first_output_s + observed.worker_setup_s
    total_rows = args.limit * args.dataset_passes
    estimated_e2e_mfu: float | str = ""
    if args.model_flops_per_image and args.gpu_peak_flops_per_s:
        estimated_e2e_mfu = (
            total_rows
            * args.model_flops_per_image
            / (operator_e2e_s * args.gpu_workers * args.gpu_peak_flops_per_s)
        )
    project_metrics = args.arm == "project_ray"
    batch_completion_p50 = percentile(result.batch_completion_wall_s, 0.50)
    batch_completion_p95 = percentile(result.batch_completion_wall_s, 0.95)
    batch_completion_p99 = percentile(result.batch_completion_wall_s, 0.99)
    cpu_core_seconds = (
        float(observed.cpu_metrics["cpu_busy_cores_mean"]) * operator_e2e_s
    )
    gpu_seconds = args.gpu_workers * operator_e2e_s
    gpu_energy_j = float(observed.gpu_metrics["gpu_energy_estimate_j"])
    derived_metrics = image_run_derived_metrics(
        rows=total_rows,
        operator_e2e_s=operator_e2e_s,
        first_output_s=first_output_s,
        cpu_core_seconds=cpu_core_seconds,
        gpu_seconds=gpu_seconds,
        gpu_energy_j=gpu_energy_j,
        host_disk_read_bytes=int(observed.cpu_metrics["host_disk_read_bytes"]),
        host_disk_write_bytes=int(observed.cpu_metrics["host_disk_write_bytes"]),
        host_net_recv_bytes=int(observed.cpu_metrics["host_net_recv_bytes"]),
        host_net_sent_bytes=int(observed.cpu_metrics["host_net_sent_bytes"]),
    )
    cpu_budget = setup.cpu_budget
    return {
        "arm": args.arm,
        "baseline_role": setup.provenance.role,
        "implementation_provenance": setup.provenance.implementation_provenance,
        "scheduler_owner": setup.provenance.scheduler_owner,
        "custom_scheduling_code": setup.provenance.custom_scheduling_code,
        "formal_baseline_eligible": setup.provenance.formal_baseline_eligible,
        "upstream_source": setup.provenance.upstream_source,
        "phase": args.phase,
        "repeat_index": args.repeat_index,
        "workload_name": args.workload_name,
        "source_doc_ids_sha256": setup.source_doc_ids_sha256,
        "expected_source_doc_ids_sha256": args.expected_source_doc_ids_sha256,
        "source_manifest_match": setup.source_manifest_match,
        "expected_input_encoded_bytes": args.expected_input_encoded_bytes or "",
        "rows": total_rows,
        "unique_images": args.limit,
        "dataset_passes": args.dataset_passes,
        "batch_size": args.batch_size,
        "input_size": args.input_size,
        "cpu_workers": "" if args.arm == "daft_builtin_embed" else args.cpu_workers,
        "gpu_workers": args.gpu_workers,
        "model_workers": setup.model_workers,
        "gpus_per_model_worker": setup.gpus_per_model_worker,
        "source_shards": setup.source_shards,
        "source_cpu_threads": setup.source_cpu_threads,
        "max_active_batches": args.max_active_batches if project_metrics else "",
        "ray_cluster_num_cpus": setup.ray_cluster_num_cpus or "",
        "host_cpu_slots_detected": cpu_budget.host_slots,
        "declared_external_cpus": cpu_budget.external_slots,
        "declared_source_cpus": (
            cpu_budget.source_slots if cpu_budget.source_slots is not None else ""
        ),
        "declared_preprocess_cpus": (
            cpu_budget.preprocess_slots
            if cpu_budget.preprocess_slots is not None
            else ""
        ),
        "declared_model_cpus": (
            cpu_budget.model_slots if cpu_budget.model_slots is not None else ""
        ),
        "declared_total_cpus": cpu_budget.declared_total_slots,
        "resource_budget_semantics": cpu_budget.semantics,
        "ray_address_mode": (
            "external_shared" if args.ray_address else "isolated_local"
        ),
        "ray_data_actor_pool_mode": (
            "native_autoscaling_min1_to_configured_max"
            if args.arm == "ray_data_staged"
            and args.ray_data_autoscaling_actor_pools
            else "fixed_size"
            if args.arm == "ray_data_staged"
            else "not_applicable"
        ),
        "formal_start_epoch_s_planned": (
            observed.planned_start_epoch_s if args.formal_ready_file else ""
        ),
        "formal_start_epoch_s_actual": observed.actual_start_epoch_s,
        "formal_start_lateness_s": (
            max(
                0.0,
                observed.actual_start_epoch_s - observed.planned_start_epoch_s,
            )
            if args.formal_ready_file
            else ""
        ),
        "torch_intraop_threads_per_worker": args.torch_intraop_threads,
        "torch_interop_threads_per_worker": args.torch_interop_threads,
        "worker_setup_s": observed.worker_setup_s if project_metrics else "",
        "worker_setup_accounting": (
            "explicit_pre_query_plus_query_wall"
            if project_metrics
            else "folded_into_timed_framework_query_wall"
        ),
        "operator_e2e_s": operator_e2e_s,
        "first_output_s": first_output_s,
        "first_output_semantics": "cold_setup_to_first_complete_arrow_record_batch",
        **derived_metrics,
        "images_per_s": total_rows / operator_e2e_s,
        "batch_service_p50_s": batch_completion_p50,
        "batch_service_p95_s": batch_completion_p95,
        "batch_service_p99_s": batch_completion_p99,
        "batch_service_semantics": (
            "submission_to_result_includes_dependency_queue_actor_and_return"
            if project_metrics
            else "unavailable_engine_internal"
        ),
        "batch_completion_wall_p50_s": batch_completion_p50,
        "batch_completion_wall_p95_s": batch_completion_p95,
        "batch_completion_wall_p99_s": batch_completion_p99,
        "batch_actor_service_p50_s": percentile(result.batch_actor_service_s, 0.50),
        "batch_actor_service_p95_s": percentile(result.batch_actor_service_s, 0.95),
        "batch_actor_service_p99_s": percentile(result.batch_actor_service_s, 0.99),
        "batch_unattributed_wait_p50_s": percentile(
            result.batch_unattributed_wait_s,
            0.50,
        ),
        "batch_unattributed_wait_p95_s": percentile(
            result.batch_unattributed_wait_s,
            0.95,
        ),
        "batch_unattributed_wait_p99_s": percentile(
            result.batch_unattributed_wait_s,
            0.99,
        ),
        "batch_preprocess_p50_s": percentile(result.batch_preprocess_s, 0.50),
        "batch_preprocess_p95_s": percentile(result.batch_preprocess_s, 0.95),
        "batch_preprocess_p99_s": percentile(result.batch_preprocess_s, 0.99),
        "batch_host_copy_p50_s": percentile(result.batch_host_copy_s, 0.50),
        "batch_host_copy_p95_s": percentile(result.batch_host_copy_s, 0.95),
        "batch_host_copy_p99_s": percentile(result.batch_host_copy_s, 0.99),
        "batch_h2d_p50_s": percentile(result.batch_h2d_s, 0.50),
        "batch_h2d_p95_s": percentile(result.batch_h2d_s, 0.95),
        "batch_h2d_p99_s": percentile(result.batch_h2d_s, 0.99),
        "batch_forward_p50_s": percentile(result.batch_forward_s, 0.50),
        "batch_forward_p95_s": percentile(result.batch_forward_s, 0.95),
        "batch_forward_p99_s": percentile(result.batch_forward_s, 0.99),
        "batch_d2h_p50_s": percentile(result.batch_d2h_s, 0.50),
        "batch_d2h_p95_s": percentile(result.batch_d2h_s, 0.95),
        "batch_d2h_p99_s": percentile(result.batch_d2h_s, 0.99),
        "batch_source_next_p50_s": percentile(result.batch_source_next_s, 0.50),
        "batch_source_next_p95_s": percentile(result.batch_source_next_s, 0.95),
        "source_next_total_s": sum(result.batch_source_next_s),
        "batch_driver_materialize_p50_s": percentile(
            result.batch_driver_materialize_s,
            0.50,
        ),
        "batch_driver_materialize_p95_s": percentile(
            result.batch_driver_materialize_s,
            0.95,
        ),
        "driver_materialize_total_s": sum(result.batch_driver_materialize_s),
        "batch_submit_p50_s": percentile(result.batch_submit_s, 0.50),
        "batch_submit_p95_s": percentile(result.batch_submit_s, 0.95),
        "submit_total_s": sum(result.batch_submit_s),
        "detailed_stage_timing": args.detailed_stage_timing,
        "telemetry_encoded_bytes": result.encoded_bytes if project_metrics else "",
        "input_tensor_bytes": result.input_tensor_bytes if project_metrics else "",
        "device_input_bytes": result.device_input_bytes if project_metrics else "",
        "output_embedding_bytes": result.output_bytes if project_metrics else "",
        "logical_h2d_effective_gbps": logical_bandwidth_gbps(
            result.input_tensor_bytes,
            result.batch_h2d_s,
        ),
        "logical_d2h_effective_gbps": logical_bandwidth_gbps(
            result.output_bytes,
            result.batch_d2h_s,
        ),
        "submitted_batches": result.submitted_batches if project_metrics else "",
        "pending_batches_peak": result.pending_batches_peak if project_metrics else "",
        "project_execution_mode": result.execution_mode if project_metrics else "",
        "hse_encoded_bytes_limit": (
            setup.hse_limits.encoded_bytes
            if project_metrics and args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_ready_bytes_limit": (
            setup.hse_limits.ready_bytes
            if project_metrics and args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_ready_work_limit": (
            setup.hse_limits.ready_work
            if project_metrics and args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_prepare_inflight_limit": (
            setup.hse_limits.prepare_inflight
            if project_metrics and args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_model_inflight_limit": (
            setup.hse_limits.model_inflight
            if project_metrics and args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_encoded_bytes_peak": (
            result.encoded_bytes_peak
            if args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_ready_bytes_peak": (
            result.ready_bytes_peak
            if args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_prepare_inflight_peak": (
            result.prepare_inflight_peak
            if args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_model_inflight_peak": (
            result.model_inflight_peak
            if args.project_execution_mode == "hse_static"
            else ""
        ),
        "batch_prepare_queue_p50_s": percentile(
            result.batch_prepare_queue_s,
            0.50,
        ),
        "batch_prepare_queue_p95_s": percentile(
            result.batch_prepare_queue_s,
            0.95,
        ),
        "batch_ready_residence_p50_s": percentile(
            result.batch_ready_residence_s,
            0.50,
        ),
        "batch_ready_residence_p95_s": percentile(
            result.batch_ready_residence_s,
            0.95,
        ),
        "cpu_core_seconds_estimate": cpu_core_seconds,
        "cpu_core_seconds_per_image": cpu_core_seconds / total_rows,
        "gpu_seconds": gpu_seconds,
        "images_per_gpu_s": total_rows / gpu_seconds,
        "images_per_joule": total_rows / gpu_energy_j if gpu_energy_j > 0 else "",
        "engine_stats_text": result.engine_stats,
        "engine_stats_semantics": (
            "hse_stage_broker_stats"
            if result.engine_stats and result.execution_mode == "hse_static"
            else "ray_data_operator_stats"
            if result.engine_stats
            else "unavailable"
        ),
        **result.audit,
        **observed.cpu_metrics,
        **observed.gpu_metrics,
        "model_revision": args.model,
        "processor_revision": (
            "provider_resolved_from_model"
            if args.arm == "daft_builtin_embed"
            else setup.processor
        ),
        "dtype": "provider_default" if args.arm == "daft_builtin_embed" else args.dtype,
        "embedding_dimension": args.embedding_dimension,
        **observed.output_contract_metadata,
        "daft_version": daft_module.__version__,
        "ray_version": ray_module.__version__,
        "torch_version": torch_module.__version__,
        "transformers_version": transformers_module.__version__,
        "gpu_name": torch_module.cuda.get_device_name(0),
        "model_flops_per_image": args.model_flops_per_image or "",
        "gpu_peak_flops_per_s": args.gpu_peak_flops_per_s or "",
        "estimated_e2e_mfu": estimated_e2e_mfu,
        **setup.database_metadata,
        "git_commit": git_commit(),
    }


def build_image_manifest(
    args: argparse.Namespace,
    setup: ImageRunSetup,
    row: dict[str, object],
) -> dict[str, object]:
    provenance = setup.provenance
    return {
        "schema_version": 13,
        "timing_boundary": "per_query_model_worker_setup_to_last_embedding_batch_returned",
        "worker_lifecycle": "per_query_cold_model_worker",
        "ray_framework_startup_included": False,
        "writeback_included": False,
        "preprocessing": (
            "daft_builtin_decode_and_transformers_provider"
            if args.arm == "daft_builtin_embed"
            else "torchvision_tensor_decode_and_processor"
        ),
        "hidden_batching": args.arm == "daft_builtin_embed",
        "baseline_provenance": {
            "role": provenance.role,
            "implementation": provenance.implementation_provenance,
            "scheduler_owner": provenance.scheduler_owner,
            "custom_scheduling_code": provenance.custom_scheduling_code,
            "formal_baseline_eligible": provenance.formal_baseline_eligible,
            "upstream_source": provenance.upstream_source,
        },
        "detailed_stage_timing_intrusive": args.detailed_stage_timing,
        "project_execution_mode": (
            args.project_execution_mode
            if args.arm == "project_ray"
            else "not_applicable"
        ),
        "bandwidth_semantics": "logical_bytes_over_stage_wall_not_pcie_counter",
        "mfu_semantics": (
            "estimated_only_when_verified_flops_and_dtype_peak_are_supplied"
        ),
        "cross_scale_comparison_semantics": {
            "rate_and_unit_resource_metrics": (
                "descriptive comparison allowed after each arm independently reaches "
                "a steady throughput plateau"
            ),
            "absolute_jct_and_first_output": (
                "matched workload scale required for ranking"
            ),
            "first_output_fraction_of_e2e": (
                "streaming/materialization diagnostic only; not normalized latency"
            ),
        },
        "metric_definitions": IMAGE_METRIC_DEFINITIONS,
        "thread_budget_semantics": (
            "explicit_torch_intraop_and_interop_per_worker; "
            "ray_num_cpus_is_admission_not_os_quota"
        ),
        "row": row,
    }
