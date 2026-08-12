#!/usr/bin/env python
"""Run comparable PostgreSQL -> data engine -> CLIP operator-E2E arms.

The arms keep the input table, model, GPU count, output validation, and timing
boundary fixed:

* ``daft_builtin_embed``: Daft built-in decode + ``embed_image`` AI Function;
  Daft owns batching, concurrency, backpressure, and scheduling.
* ``ray_data_staged``: native Ray Data SQL source and ``map_batches`` graph;
  Ray Data owns backpressure and actor scheduling.
* ``daft_native`` / ``daft_ray`` / ``daft_staged``: project-authored UDF
  references retained for mechanism diagnosis, not formal native baselines.
* ``project_ray``: bounded Ray CPU preprocessing actors feeding tensor-only GPU
  actors, with the source kept lazy and streamed from Daft.

This gate excludes pgvector writeback so that it measures the operator execution
path. A later system-E2E layer must add the same sink to every arm.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path


CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.modalities.image import DaftImageSource, ImageSourceConfig  # noqa: E402
from src.modalities.image.source import read_image_source_metadata  # noqa: E402
from src.baselines.image.provenance import (  # noqa: E402
    image_arm_provenance,
    require_formal_arm_allowed,
)
from src.baselines.image.frameworks.daft import (  # noqa: E402
    build_daft_clip_embedder,
    build_daft_staged_clip_pipeline,
    run_daft_builtin_image_embedding,
    run_daft_clip_baseline,
    run_daft_staged_clip_baseline,
)
from src.modalities.image.execution import (  # noqa: E402
    EmbeddingCapture,
    ExecutionResult,
    build_project_ray_worker_pool,
    run_project_ray_pipeline,
    stop_project_ray_worker_pool,
)
from src.modalities.image.staged_execution import run_project_ray_hse_pipeline  # noqa: E402
from src.scheduling.runtime.stage_broker import StageBrokerLimits  # noqa: E402
from src.modalities.image.metrics import (  # noqa: E402
    IMAGE_METRIC_DEFINITIONS,
    image_run_derived_metrics,
)
from src.baselines.image.frameworks.ray_data import (  # noqa: E402
    build_ray_data_clip_pipeline,
    run_ray_data_clip_baseline,
)
from src.modalities.image.resource_budget import build_ray_cpu_budget  # noqa: E402
from src.modalities.image.resource_sampling import NvidiaSmiSampler, SystemCpuSampler  # noqa: E402
from src.infrastructure.runtime_env import ray_runtime_env  # noqa: E402


ARMS = (
    "daft_builtin_embed",
    "daft_native",
    "daft_ray",
    "daft_staged",
    "ray_data_staged",
    "project_ray",
)
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


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm", choices=ARMS, required=True)
    parser.add_argument(
        "--pg-dsn",
        default=os.environ.get("DATABASE_URL") or os.environ.get("PG_DSN", ""),
    )
    parser.add_argument("--model", required=True)
    parser.add_argument("--processor", default="")
    parser.add_argument("--workload-name", default="coco_val2017")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument(
        "--dataset-passes",
        type=int,
        default=1,
        help=(
            "Logical passes over the selected unique images. Repeated rows receive "
            "pass-qualified execution IDs; --limit remains the unique-image count."
        ),
    )
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--warmup-rows", type=int, default=64)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument(
        "--input-size",
        type=int,
        default=224,
        help="Frozen square processor output used for HSE byte/work accounting",
    )
    parser.add_argument("--cpu-workers", type=int, default=4)
    parser.add_argument("--gpu-workers", type=int, default=2)
    parser.add_argument(
        "--daft-model-workers",
        type=int,
        default=0,
        help="Daft UDF actors; 0 means one actor per physical GPU",
    )
    parser.add_argument(
        "--source-shards",
        type=int,
        default=0,
        help="PostgreSQL lazy source shards; 0 follows model worker count",
    )
    parser.add_argument(
        "--source-cpu-threads",
        type=int,
        default=0,
        help="Daft native source/runner threads; 0 follows --cpu-workers",
    )
    parser.add_argument(
        "--max-active-batches",
        type=int,
        default=8,
        help="Project-Ray admission window; ignored by framework-native baseline arms",
    )
    parser.add_argument(
        "--project-execution-mode",
        choices=("direct_dependency", "hse_static"),
        default="direct_dependency",
        help=(
            "Project-Ray flow ownership. hse_static uses a real ready queue and hard "
            "byte/work limits; it does not enable SAOR dynamic decisions."
        ),
    )
    parser.add_argument(
        "--hse-encoded-bytes-limit",
        type=int,
        default=0,
        help="Encoded source bytes held by HSE; 0 derives a conservative workload cap",
    )
    parser.add_argument(
        "--hse-ready-bytes-limit",
        type=int,
        default=0,
        help="Prepared tensor bytes held by HSE; 0 derives from batch/window/FP32 NCHW",
    )
    parser.add_argument(
        "--hse-ready-work-limit",
        type=int,
        default=0,
        help="Prepared model work held by HSE; 0 derives from batch/window/pixels",
    )
    parser.add_argument(
        "--hse-prepare-inflight-limit",
        type=int,
        default=0,
        help="HSE CPU leases; 0 uses --cpu-workers",
    )
    parser.add_argument(
        "--hse-model-inflight-limit",
        type=int,
        default=0,
        help="HSE GPU leases; 0 uses --gpu-workers",
    )
    parser.add_argument(
        "--allow-non-native-diagnostic",
        action="store_true",
        help=(
            "Allow a project-authored Daft reference in a formal-phase diagnostic; "
            "it remains ineligible for baseline ranking"
        ),
    )
    parser.add_argument("--torch-intraop-threads", type=int, default=1)
    parser.add_argument("--torch-interop-threads", type=int, default=1)
    parser.add_argument("--dtype", choices=("float16", "float32", "bfloat16"), default="float16")
    parser.add_argument("--embedding-dimension", type=int, default=512)
    parser.add_argument(
        "--model-flops-per-image",
        type=float,
        default=0.0,
        help="Verified model FLOPs per image; 0 leaves estimated MFU blank",
    )
    parser.add_argument(
        "--gpu-peak-flops-per-s",
        type=float,
        default=0.0,
        help="Per-GPU peak FLOP/s matching dtype; 0 leaves estimated MFU blank",
    )
    parser.add_argument("--phase", choices=("gate", "warmup", "formal"), default="gate")
    parser.add_argument("--repeat-index", type=int, default=0)
    parser.add_argument("--gpu-sample-interval-s", type=float, default=0.5)
    parser.add_argument("--cpu-sample-interval-s", type=float, default=0.25)
    parser.add_argument(
        "--ray-address",
        default="",
        help=(
            "Connect to one externally managed Ray cluster. Required when multiple "
            "independent native image applications share the same physical resources; "
            "empty keeps the isolated single-job behavior."
        ),
    )
    parser.add_argument(
        "--formal-ready-file",
        default="",
        help="Optional per-job readiness marker written after the untimed warm-up.",
    )
    parser.add_argument(
        "--formal-start-barrier-file",
        default="",
        help="Optional JSON barrier containing start_epoch_s for coordinated jobs.",
    )
    parser.add_argument("--formal-start-offset-s", type=float, default=0.0)
    parser.add_argument("--formal-barrier-timeout-s", type=float, default=900.0)
    parser.add_argument(
        "--ray-data-autoscaling-actor-pools",
        action="store_true",
        help=(
            "Use Ray Data's native min=1/max=configured ActorPoolStrategy. "
            "This prevents independent multi-job graphs from gang-reserving all CPU slots."
        ),
    )
    parser.add_argument("--expected-source-doc-ids-sha256", default="")
    parser.add_argument("--expected-input-encoded-bytes", type=int, default=0)
    parser.add_argument(
        "--detailed-stage-timing",
        action="store_true",
        help="Synchronize CUDA stages to measure H2D/forward/D2H; use as a diagnostic arm",
    )
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--out-manifest", required=True)
    parser.add_argument(
        "--save-embeddings",
        default="",
        help=(
            "Diagnostic only: capture validated per-row embeddings to a compressed "
            ".npz plus a sidecar manifest. Supported for daft_builtin_embed and "
            "project_ray gate runs only. Capture adds driver memory/copy overhead, so "
            "its timing is invalid for performance comparison; empty default is a no-op."
        ),
    )
    parser.add_argument(
        "--embedding-output-contract",
        choices=("arm_default", "l2_normalized"),
        default="arm_default",
        help=(
            "Output contract inside the timed operator boundary. arm_default preserves "
            "each implementation's native behavior; l2_normalized charges any required "
            "normalization to that arm and is required for cross-system formal ranking."
        ),
    )
    return parser.parse_args()


def read_database_metadata(
    dsn: str,
    *,
    workload_name: str,
    limit: int,
    offset: int,
    dataset_passes: int = 1,
) -> tuple[frozenset[str], dict[str, object]]:
    return read_image_source_metadata(
        dsn,
        ImageSourceConfig(
            workload_name=workload_name,
            limit=limit,
            offset=offset,
            dataset_passes=dataset_passes,
        ),
    )


def make_source(
    dsn: str,
    workload_name: str,
    limit: int,
    offset: int,
    *,
    source_shards: int,
    dataset_passes: int = 1,
):
    return DaftImageSource().read_sharded(
        dsn,
        ImageSourceConfig(
            workload_name=workload_name,
            limit=limit,
            offset=offset,
            dataset_passes=dataset_passes,
        ),
        shards=source_shards,
    )


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


def main() -> None:
    args = parse_args()
    embedding_target: Path | None = None
    embedding_sidecar: Path | None = None
    provenance = image_arm_provenance(args.arm)
    try:
        require_formal_arm_allowed(
            args.arm,
            phase=args.phase,
            allow_non_native_diagnostic=args.allow_non_native_diagnostic,
        )
    except ValueError as error:
        raise SystemExit(str(error)) from error
    processor = args.processor or args.model
    positive = (
        args.limit,
        args.dataset_passes,
        args.warmup_rows,
        args.batch_size,
        args.input_size,
        args.cpu_workers,
        args.gpu_workers,
        args.max_active_batches,
        args.torch_intraop_threads,
        args.torch_interop_threads,
        args.embedding_dimension,
        args.gpu_sample_interval_s,
        args.cpu_sample_interval_s,
    )
    if not args.pg_dsn:
        raise SystemExit("--pg-dsn is required (or set DATABASE_URL/PG_DSN)")
    if min(positive) <= 0 or args.offset < 0:
        raise SystemExit("row, batch, worker, and dimension values must be positive")
    if min(args.daft_model_workers, args.source_shards, args.source_cpu_threads) < 0:
        raise SystemExit("optional worker and shard overrides must be non-negative")
    if args.arm == "project_ray" and args.max_active_batches < args.gpu_workers:
        raise SystemExit("--max-active-batches must be at least --gpu-workers")
    if args.project_execution_mode != "direct_dependency" and args.arm != "project_ray":
        raise SystemExit("--project-execution-mode applies only to --arm project_ray")
    hse_values = (
        args.hse_encoded_bytes_limit,
        args.hse_ready_bytes_limit,
        args.hse_ready_work_limit,
        args.hse_prepare_inflight_limit,
        args.hse_model_inflight_limit,
    )
    if min(hse_values) < 0:
        raise SystemExit("HSE limit overrides must be non-negative; 0 means derive")
    if args.detailed_stage_timing and args.arm != "project_ray":
        raise SystemExit("--detailed-stage-timing currently supports project_ray only")
    if min(args.model_flops_per_image, args.gpu_peak_flops_per_s) < 0:
        raise SystemExit("FLOP values must be non-negative")
    if bool(args.model_flops_per_image) != bool(args.gpu_peak_flops_per_s):
        raise SystemExit("set both FLOP values or leave both at 0")
    if args.formal_start_offset_s < 0 or args.formal_barrier_timeout_s <= 0:
        raise SystemExit("formal barrier offset must be non-negative and timeout positive")
    if bool(args.formal_ready_file) != bool(args.formal_start_barrier_file):
        raise SystemExit(
            "--formal-ready-file and --formal-start-barrier-file must be set together"
        )
    if args.save_embeddings:
        if args.arm not in ("daft_builtin_embed", "project_ray"):
            raise SystemExit(
                "--save-embeddings supports daft_builtin_embed and project_ray only"
            )
        if args.phase != "gate" or args.dataset_passes != 1:
            raise SystemExit(
                "--save-embeddings is diagnostic-only: use --phase gate and one dataset pass"
            )
        if args.limit > 4096:
            raise SystemExit("--save-embeddings is limited to at most 4096 diagnostic rows")
        embedding_target = Path(args.save_embeddings)
        if embedding_target.suffix != ".npz":
            embedding_target = embedding_target.with_suffix(
                embedding_target.suffix + ".npz"
            )
        embedding_sidecar = Path(str(embedding_target) + ".manifest.json")
        if embedding_target.exists() or embedding_sidecar.exists():
            raise SystemExit(
                "ERROR: --save-embeddings target or sidecar already exists; "
                f"refuse overwrite: {embedding_target}, {embedding_sidecar}"
            )

    import daft
    import ray
    import torch
    import transformers

    formal_ids, database_metadata = read_database_metadata(
        args.pg_dsn,
        workload_name=args.workload_name,
        limit=args.limit,
        offset=args.offset,
        dataset_passes=args.dataset_passes,
    )
    source_doc_ids_sha256 = hashlib.sha256(
        ("\n".join(sorted(formal_ids)) + "\n").encode("utf-8")
    ).hexdigest()
    source_manifest_match = True
    if args.expected_source_doc_ids_sha256:
        source_manifest_match = (
            source_doc_ids_sha256 == args.expected_source_doc_ids_sha256
            and int(database_metadata["input_encoded_bytes"])
            == args.expected_input_encoded_bytes
        )
        if not source_manifest_match:
            raise ValueError("PostgreSQL image source no longer matches the immutable manifest")
    hse_encoded_bytes_limit = args.hse_encoded_bytes_limit or max(
        1,
        int(database_metadata["max_encoded_bytes"])
        * args.batch_size
        * args.max_active_batches
        * 2,
    )
    hse_ready_bytes_limit = args.hse_ready_bytes_limit or (
        args.batch_size
        * args.max_active_batches
        * 3
        * args.input_size
        * args.input_size
        * 4
    )
    hse_ready_work_limit = args.hse_ready_work_limit or (
        args.batch_size
        * args.max_active_batches
        * args.input_size
        * args.input_size
    )
    hse_prepare_inflight_limit = (
        args.hse_prepare_inflight_limit or args.cpu_workers
    )
    hse_model_inflight_limit = args.hse_model_inflight_limit or args.gpu_workers
    hse_limits = StageBrokerLimits(
        encoded_bytes=hse_encoded_bytes_limit,
        ready_bytes=hse_ready_bytes_limit,
        ready_work=hse_ready_work_limit,
        prepare_inflight=hse_prepare_inflight_limit,
        model_inflight=hse_model_inflight_limit,
    )
    warmup_count = min(args.warmup_rows, args.limit)
    warmup_ids, _ = read_database_metadata(
        args.pg_dsn,
        workload_name=args.workload_name,
        limit=warmup_count,
        offset=args.offset,
    )

    worker_pool = None
    embedder = None
    preprocessor = None
    if args.arm in ("daft_native", "daft_ray", "daft_staged"):
        model_workers = args.daft_model_workers or args.gpu_workers
        gpus_per_model_worker = args.gpu_workers / model_workers
        if gpus_per_model_worker > 1:
            raise SystemExit("Daft model workers must be at least the physical GPU count")
    else:
        model_workers = args.gpu_workers
        gpus_per_model_worker = 1.0
    source_shards = args.source_shards or model_workers
    source_cpu_threads = args.source_cpu_threads or args.cpu_workers
    try:
        cpu_budget = build_ray_cpu_budget(
            arm=args.arm,
            source_shards=source_shards,
            preprocess_workers=args.cpu_workers,
            gpu_workers=args.gpu_workers,
            model_workers=model_workers,
            external_source_threads=source_cpu_threads,
        )
    except ValueError as error:
        raise SystemExit(str(error)) from error
    ray_cluster_num_cpus = cpu_budget.cluster_slots
    worker_runtime_env = ray_runtime_env(CODE_ROOT)
    def init_ray() -> None:
        if args.ray_address:
            ray.init(
                address=args.ray_address,
                include_dashboard=False,
                runtime_env=worker_runtime_env,
            )
        else:
            ray.init(
                num_cpus=ray_cluster_num_cpus,
                num_gpus=args.gpu_workers,
                include_dashboard=False,
                runtime_env=worker_runtime_env,
            )

    if args.arm == "daft_builtin_embed":
        init_ray()
        daft.set_runner_ray(noop_if_initialized=True)
    elif args.arm == "daft_native":
        daft.set_runner_native(num_threads=source_cpu_threads)
        embedder = build_daft_clip_embedder(
            model_revision=args.model,
            processor_revision=processor,
            batch_size=args.batch_size,
            model_workers=model_workers,
            gpus_per_worker=gpus_per_model_worker,
            dtype=args.dtype,
            embedding_dimension=args.embedding_dimension,
            torch_intraop_threads=args.torch_intraop_threads,
            torch_interop_threads=args.torch_interop_threads,
        )
    elif args.arm == "daft_ray":
        init_ray()
        daft.set_runner_ray(noop_if_initialized=True)
        embedder = build_daft_clip_embedder(
            model_revision=args.model,
            processor_revision=processor,
            batch_size=args.batch_size,
            model_workers=model_workers,
            gpus_per_worker=gpus_per_model_worker,
            dtype=args.dtype,
            embedding_dimension=args.embedding_dimension,
            torch_intraop_threads=args.torch_intraop_threads,
            torch_interop_threads=args.torch_interop_threads,
        )
    elif args.arm == "daft_staged":
        init_ray()
        daft.set_runner_ray(noop_if_initialized=True)
        preprocessor, embedder = build_daft_staged_clip_pipeline(
            model_revision=args.model,
            processor_revision=processor,
            batch_size=args.batch_size,
            cpu_workers=args.cpu_workers,
            model_workers=model_workers,
            gpus_per_worker=gpus_per_model_worker,
            dtype=args.dtype,
            embedding_dimension=args.embedding_dimension,
            torch_intraop_threads=args.torch_intraop_threads,
            torch_interop_threads=args.torch_interop_threads,
        )
    elif args.arm == "ray_data_staged":
        # Ray Data keeps SQL readers and both callable actor pools live in one
        # streaming graph.  In multi-job mode the external cluster, rather than
        # this adapter, owns the common resource capacity.
        init_ray()
    else:
        init_ray()
        daft.set_runner_native(num_threads=source_cpu_threads)
        worker_pool = build_project_ray_worker_pool(
            model_revision=args.model,
            processor_revision=processor,
            cpu_workers=args.cpu_workers,
            gpu_workers=args.gpu_workers,
            dtype=args.dtype,
            detailed_stage_timing=args.detailed_stage_timing,
            torch_intraop_threads=args.torch_intraop_threads,
            torch_interop_threads=args.torch_interop_threads,
        )

    def execute(
        limit: int,
        expected_ids: frozenset[str],
        *,
        dataset_passes: int = 1,
        embedding_capture: EmbeddingCapture | None = None,
    ) -> ExecutionResult:
        if args.arm == "ray_data_staged":
            dataset = build_ray_data_clip_pipeline(
                database_url=args.pg_dsn,
                source_config=ImageSourceConfig(
                    workload_name=args.workload_name,
                    limit=limit,
                    offset=args.offset,
                    dataset_passes=dataset_passes,
                ),
                source_shards=source_shards,
                processor_revision=processor,
                model_revision=args.model,
                dtype=args.dtype,
                batch_size=args.batch_size,
                cpu_workers=args.cpu_workers,
                gpu_workers=args.gpu_workers,
                autoscaling_actor_pools=args.ray_data_autoscaling_actor_pools,
                torch_intraop_threads=args.torch_intraop_threads,
                torch_interop_threads=args.torch_interop_threads,
            )
            return run_ray_data_clip_baseline(
                dataset,
                expected_doc_ids=expected_ids,
                embedding_dimension=args.embedding_dimension,
            )
        source = make_source(
            args.pg_dsn,
            args.workload_name,
            limit,
            args.offset,
            source_shards=source_shards,
            dataset_passes=dataset_passes,
        )
        if args.arm == "daft_builtin_embed":
            return run_daft_builtin_image_embedding(
                source,
                model_revision=args.model,
                batch_size=args.batch_size,
                expected_doc_ids=expected_ids,
                embedding_dimension=args.embedding_dimension,
                embedding_capture=embedding_capture,
                normalize_output=args.embedding_output_contract == "l2_normalized",
            )
        if args.arm in ("daft_native", "daft_ray"):
            return run_daft_clip_baseline(
                source,
                embedder=embedder,
                expected_doc_ids=expected_ids,
                embedding_dimension=args.embedding_dimension,
            )
        if args.arm == "daft_staged":
            return run_daft_staged_clip_baseline(
                source,
                preprocessor=preprocessor,
                embedder=embedder,
                expected_doc_ids=expected_ids,
                embedding_dimension=args.embedding_dimension,
            )
        project_common = {
            "worker_pool": worker_pool,
            "expected_doc_ids": expected_ids,
            "batch_size": args.batch_size,
            "max_active_batches": args.max_active_batches,
            "embedding_dimension": args.embedding_dimension,
            "embedding_capture": embedding_capture,
        }
        if args.project_execution_mode == "hse_static":
            return run_project_ray_hse_pipeline(
                source,
                **project_common,
                encoded_block_bytes_upper_bound=(
                    int(database_metadata["max_encoded_bytes"]) * args.batch_size
                ),
                limits=hse_limits,
                model_revision=args.model,
                processor_revision=processor,
                model_dtype=args.dtype,
                input_size=args.input_size,
            )
        return run_project_ray_pipeline(source, **project_common)

    execute(warmup_count, warmup_ids)
    if worker_pool is not None:
        stop_project_ray_worker_pool(worker_pool)
    formal_start_epoch_s_planned = 0.0
    if args.formal_ready_file:
        ready_path = Path(args.formal_ready_file)
        ready_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = ready_path.with_suffix(ready_path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "arm": args.arm,
                    "warmup_completed_epoch_s": time.time(),
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(ready_path)
        barrier_path = Path(args.formal_start_barrier_file)
        deadline = time.monotonic() + args.formal_barrier_timeout_s
        while not barrier_path.is_file():
            if time.monotonic() >= deadline:
                raise TimeoutError(f"formal start barrier timed out: {barrier_path}")
            time.sleep(0.05)
        barrier = json.loads(barrier_path.read_text(encoding="utf-8"))
        formal_start_epoch_s_planned = (
            float(barrier["start_epoch_s"]) + args.formal_start_offset_s
        )
        remaining = formal_start_epoch_s_planned - time.time()
        if remaining > 0:
            time.sleep(remaining)
    formal_start_epoch_s_actual = time.time()
    gpu_sampler = NvidiaSmiSampler(
        args.gpu_sample_interval_s,
        active_device_count=args.gpu_workers,
    )
    cpu_sampler = SystemCpuSampler(args.cpu_sample_interval_s)
    gpu_sampler.start()
    cpu_sampler.start()
    if worker_pool is not None:
        setup_started = time.perf_counter()
        worker_pool = build_project_ray_worker_pool(
            model_revision=args.model,
            processor_revision=processor,
            cpu_workers=args.cpu_workers,
            gpu_workers=args.gpu_workers,
            dtype=args.dtype,
            detailed_stage_timing=args.detailed_stage_timing,
            torch_intraop_threads=args.torch_intraop_threads,
            torch_interop_threads=args.torch_interop_threads,
        )
        worker_setup_s = time.perf_counter() - setup_started
    else:
        # Daft creates the model-owning UDF actor lazily inside each query, so
        # its worker/model startup is already included in result.total_s and
        # first_output_s rather than this explicit setup field.
        worker_setup_s = 0.0
    embedding_capture = EmbeddingCapture() if args.save_embeddings else None
    result = execute(
        args.limit,
        formal_ids,
        dataset_passes=args.dataset_passes,
        embedding_capture=embedding_capture,
    )
    output_contract_metadata = embedding_output_contract_metadata(
        args.arm,
        args.embedding_output_contract,
    )
    if args.save_embeddings:
        # Diagnostic only: capture happens while validated batches are consumed.
        # The default path has no capture; this run's timing is not performance-valid.
        assert embedding_capture is not None
        assert embedding_target is not None
        assert embedding_sidecar is not None
        _doc_ids, _emb_arr = embedding_capture.finish()
        if embedding_target.exists() or embedding_sidecar.exists():
            raise SystemExit(
                "ERROR: --save-embeddings target or sidecar already exists; "
                f"refuse overwrite: {embedding_target}, {embedding_sidecar}"
            )
        embedding_target.parent.mkdir(parents=True, exist_ok=True)
        import numpy as _np
        _np.savez_compressed(
            embedding_target,
            embeddings=_emb_arr,
            doc_ids=_np.asarray(_doc_ids, dtype=str),
        )
        _manifest = {
            "arm": args.arm,
            "phase": args.phase,
            "repeat_index": args.repeat_index,
            "model_revision": args.model,
            "processor_revision": (
                "provider_resolved_from_model"
                if args.arm == "daft_builtin_embed"
                else processor
            ),
            "dtype": (
                "provider_default"
                if args.arm == "daft_builtin_embed"
                else args.dtype
            ),
            "embedding_dimension": args.embedding_dimension,
            "rows": int(_emb_arr.shape[0]),
            "dimension": int(_emb_arr.shape[1]) if _emb_arr.ndim == 2 else None,
            "diagnostic_capture_enabled": True,
            "timing_valid_for_performance": False,
            **output_contract_metadata,
            "note": (
                "captured embeddings are the post-arm output under the recorded effective "
                "output contract; the parity probe L2-normalizes defensively before "
                "comparison; capture timing is invalid for performance claims"
            ),
        }
        try:
            import daft as _daft
            import ray as _ray
            import torch as _torch
            import transformers as _transformers
            _manifest.update(
                {
                    "daft_version": _daft.__version__,
                    "ray_version": _ray.__version__,
                    "torch_version": _torch.__version__,
                    "transformers_version": _transformers.__version__,
                }
            )
        except Exception:  # noqa: BLE001
            pass
        embedding_sidecar.write_text(
            json.dumps(_manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    gpu_metrics = gpu_sampler.stop()
    cpu_metrics = cpu_sampler.stop()
    if worker_pool is not None:
        stop_project_ray_worker_pool(worker_pool)

    operator_e2e_s = result.total_s + worker_setup_s
    first_output_s = result.first_output_s + worker_setup_s
    total_rows = args.limit * args.dataset_passes
    estimated_e2e_mfu = ""
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
    cpu_core_seconds = float(cpu_metrics["cpu_busy_cores_mean"]) * operator_e2e_s
    gpu_seconds = args.gpu_workers * operator_e2e_s
    gpu_energy_j = float(gpu_metrics["gpu_energy_estimate_j"])
    derived_metrics = image_run_derived_metrics(
        rows=total_rows,
        operator_e2e_s=operator_e2e_s,
        first_output_s=first_output_s,
        cpu_core_seconds=cpu_core_seconds,
        gpu_seconds=gpu_seconds,
        gpu_energy_j=gpu_energy_j,
        host_disk_read_bytes=int(cpu_metrics["host_disk_read_bytes"]),
        host_disk_write_bytes=int(cpu_metrics["host_disk_write_bytes"]),
        host_net_recv_bytes=int(cpu_metrics["host_net_recv_bytes"]),
        host_net_sent_bytes=int(cpu_metrics["host_net_sent_bytes"]),
    )
    declared_source_cpus = cpu_budget.source_slots
    declared_preprocess_cpus = cpu_budget.preprocess_slots
    declared_model_cpus = cpu_budget.model_slots

    row: dict[str, object] = {
        "arm": args.arm,
        "baseline_role": provenance.role,
        "implementation_provenance": provenance.implementation_provenance,
        "scheduler_owner": provenance.scheduler_owner,
        "custom_scheduling_code": provenance.custom_scheduling_code,
        "formal_baseline_eligible": provenance.formal_baseline_eligible,
        "upstream_source": provenance.upstream_source,
        "phase": args.phase,
        "repeat_index": args.repeat_index,
        "workload_name": args.workload_name,
        "source_doc_ids_sha256": source_doc_ids_sha256,
        "expected_source_doc_ids_sha256": args.expected_source_doc_ids_sha256,
        "source_manifest_match": source_manifest_match,
        "expected_input_encoded_bytes": args.expected_input_encoded_bytes or "",
        "rows": total_rows,
        "unique_images": args.limit,
        "dataset_passes": args.dataset_passes,
        "batch_size": args.batch_size,
        "input_size": args.input_size,
        "cpu_workers": "" if args.arm == "daft_builtin_embed" else args.cpu_workers,
        "gpu_workers": args.gpu_workers,
        "model_workers": model_workers,
        "gpus_per_model_worker": gpus_per_model_worker,
        "source_shards": source_shards,
        "source_cpu_threads": source_cpu_threads,
        "max_active_batches": args.max_active_batches if project_metrics else "",
        "ray_cluster_num_cpus": ray_cluster_num_cpus or "",
        "host_cpu_slots_detected": cpu_budget.host_slots,
        "declared_external_cpus": cpu_budget.external_slots,
        "declared_source_cpus": declared_source_cpus if declared_source_cpus is not None else "",
        "declared_preprocess_cpus": (
            declared_preprocess_cpus if declared_preprocess_cpus is not None else ""
        ),
        "declared_model_cpus": declared_model_cpus if declared_model_cpus is not None else "",
        "declared_total_cpus": cpu_budget.declared_total_slots,
        "resource_budget_semantics": cpu_budget.semantics,
        "ray_address_mode": "external_shared" if args.ray_address else "isolated_local",
        "ray_data_actor_pool_mode": (
            "native_autoscaling_min1_to_configured_max"
            if args.arm == "ray_data_staged" and args.ray_data_autoscaling_actor_pools
            else ("fixed_size" if args.arm == "ray_data_staged" else "not_applicable")
        ),
        "formal_start_epoch_s_planned": (
            formal_start_epoch_s_planned if args.formal_ready_file else ""
        ),
        "formal_start_epoch_s_actual": formal_start_epoch_s_actual,
        "formal_start_lateness_s": (
            max(0.0, formal_start_epoch_s_actual - formal_start_epoch_s_planned)
            if args.formal_ready_file
            else ""
        ),
        "torch_intraop_threads_per_worker": args.torch_intraop_threads,
        "torch_interop_threads_per_worker": args.torch_interop_threads,
        "worker_setup_s": worker_setup_s if project_metrics else "",
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
        # Legacy aliases retained for old summarizers. These are not pure GPU
        # service times; the explicit semantics and replacement fields follow.
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
            hse_limits.encoded_bytes
            if project_metrics and args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_ready_bytes_limit": (
            hse_limits.ready_bytes
            if project_metrics and args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_ready_work_limit": (
            hse_limits.ready_work
            if project_metrics and args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_prepare_inflight_limit": (
            hse_limits.prepare_inflight
            if project_metrics and args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_model_inflight_limit": (
            hse_limits.model_inflight
            if project_metrics and args.project_execution_mode == "hse_static"
            else ""
        ),
        "hse_encoded_bytes_peak": (
            result.encoded_bytes_peak if args.project_execution_mode == "hse_static" else ""
        ),
        "hse_ready_bytes_peak": (
            result.ready_bytes_peak if args.project_execution_mode == "hse_static" else ""
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
        "batch_prepare_queue_p50_s": percentile(result.batch_prepare_queue_s, 0.50),
        "batch_prepare_queue_p95_s": percentile(result.batch_prepare_queue_s, 0.95),
        "batch_ready_residence_p50_s": percentile(result.batch_ready_residence_s, 0.50),
        "batch_ready_residence_p95_s": percentile(result.batch_ready_residence_s, 0.95),
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
        **cpu_metrics,
        **gpu_metrics,
        "model_revision": args.model,
        "processor_revision": (
            "provider_resolved_from_model"
            if args.arm == "daft_builtin_embed"
            else processor
        ),
        "dtype": "provider_default" if args.arm == "daft_builtin_embed" else args.dtype,
        "embedding_dimension": args.embedding_dimension,
        **output_contract_metadata,
        "daft_version": daft.__version__,
        "ray_version": ray.__version__,
        "torch_version": torch.__version__,
        "transformers_version": transformers.__version__,
        "gpu_name": torch.cuda.get_device_name(0),
        "model_flops_per_image": args.model_flops_per_image or "",
        "gpu_peak_flops_per_s": args.gpu_peak_flops_per_s or "",
        "estimated_e2e_mfu": estimated_e2e_mfu,
        **database_metadata,
        "git_commit": git_commit(),
    }
    append_csv(Path(args.out_csv), row)
    manifest = {
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
            args.project_execution_mode if args.arm == "project_ray" else "not_applicable"
        ),
        "bandwidth_semantics": "logical_bytes_over_stage_wall_not_pcie_counter",
        "mfu_semantics": "estimated_only_when_verified_flops_and_dtype_peak_are_supplied",
        "cross_scale_comparison_semantics": {
            "rate_and_unit_resource_metrics": (
                "descriptive comparison allowed after each arm independently reaches "
                "a steady throughput plateau"
            ),
            "absolute_jct_and_first_output": "matched workload scale required for ranking",
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
    manifest_path = Path(args.out_manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, sort_keys=True))
    if ray.is_initialized():
        ray.shutdown()


if __name__ == "__main__":
    main()
