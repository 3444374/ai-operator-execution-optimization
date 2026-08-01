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
import json
import os
import subprocess
import sys
import time
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.image import DaftImageSource, ImageSourceConfig  # noqa: E402
from src.image.baseline_contract import (  # noqa: E402
    image_arm_provenance,
    require_formal_arm_allowed,
)
from src.image.daft_baseline import (  # noqa: E402
    build_daft_clip_embedder,
    build_daft_staged_clip_pipeline,
    run_daft_builtin_image_embedding,
    run_daft_clip_baseline,
    run_daft_staged_clip_baseline,
)
from src.image.execution import (  # noqa: E402
    ExecutionResult,
    build_project_ray_worker_pool,
    run_project_ray_pipeline,
    stop_project_ray_worker_pool,
)
from src.image.ray_data_baseline import (  # noqa: E402
    build_ray_data_clip_pipeline,
    run_ray_data_clip_baseline,
)
from src.image.resource_budget import build_ray_cpu_budget  # noqa: E402
from src.image.resource_sampling import NvidiaSmiSampler, SystemCpuSampler  # noqa: E402
from src.runtime_env import ray_runtime_env  # noqa: E402


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
    "rows",
    "unique_images",
    "dataset_passes",
    "batch_size",
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
    "torch_intraop_threads_per_worker",
    "torch_interop_threads_per_worker",
    "worker_setup_s",
    "worker_setup_accounting",
    "operator_e2e_s",
    "first_output_s",
    "first_output_semantics",
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
    "telemetry_encoded_bytes",
    "input_tensor_bytes",
    "device_input_bytes",
    "output_embedding_bytes",
    "logical_h2d_effective_gbps",
    "logical_d2h_effective_gbps",
    "submitted_batches",
    "pending_batches_peak",
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
    "images_per_gpu_s",
    "images_per_joule",
    "engine_stats_text",
    "engine_stats_semantics",
    "model_revision",
    "processor_revision",
    "dtype",
    "embedding_dimension",
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
        "--detailed-stage-timing",
        action="store_true",
        help="Synchronize CUDA stages to measure H2D/forward/D2H; use as a diagnostic arm",
    )
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--out-manifest", required=True)
    return parser.parse_args()


def read_database_metadata(
    dsn: str,
    *,
    workload_name: str,
    limit: int,
    offset: int,
    dataset_passes: int = 1,
) -> tuple[frozenset[str], dict[str, object]]:
    import psycopg

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT doc_id FROM image_documents "
                "WHERE workload_name = %s ORDER BY doc_id LIMIT %s OFFSET %s",
                (workload_name, limit, offset),
            )
            physical_doc_ids = frozenset(str(row[0]) for row in cursor.fetchall())
            cursor.execute("SHOW server_version")
            server_version = str(cursor.fetchone()[0])
            cursor.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'vector'"
            )
            extension = cursor.fetchone()
            cursor.execute(
                "SELECT COALESCE(SUM(image_bytes), 0), "
                "COALESCE(AVG(image_bytes), 0) FROM ("
                "SELECT image_bytes FROM image_documents "
                "WHERE workload_name = %s ORDER BY doc_id LIMIT %s OFFSET %s"
                ") AS selected_rows",
                (workload_name, limit, offset),
            )
            input_encoded_bytes, avg_encoded_bytes = cursor.fetchone()
    if len(physical_doc_ids) != limit:
        raise ValueError(f"expected {limit} source rows, found {len(physical_doc_ids)}")
    if dataset_passes == 1:
        doc_ids = physical_doc_ids
    else:
        doc_ids = frozenset(
            f"{doc_id}#pass={pass_index}"
            for pass_index in range(1, dataset_passes + 1)
            for doc_id in physical_doc_ids
        )
    return doc_ids, {
        "server_version": server_version,
        "pgvector_version": str(extension[0]) if extension else "not_installed",
        "input_encoded_bytes": int(input_encoded_bytes) * dataset_passes,
        "avg_encoded_bytes": float(avg_encoded_bytes),
    }


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
    if args.detailed_stage_timing and args.arm != "project_ray":
        raise SystemExit("--detailed-stage-timing currently supports project_ray only")
    if min(args.model_flops_per_image, args.gpu_peak_flops_per_s) < 0:
        raise SystemExit("FLOP values must be non-negative")
    if bool(args.model_flops_per_image) != bool(args.gpu_peak_flops_per_s):
        raise SystemExit("set both FLOP values or leave both at 0")

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
    if args.arm == "daft_builtin_embed":
        ray.init(
            num_cpus=ray_cluster_num_cpus,
            num_gpus=args.gpu_workers,
            include_dashboard=False,
            runtime_env=worker_runtime_env,
        )
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
        ray.init(
            num_cpus=ray_cluster_num_cpus,
            num_gpus=args.gpu_workers,
            include_dashboard=False,
            runtime_env=worker_runtime_env,
        )
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
        ray.init(
            num_cpus=ray_cluster_num_cpus,
            num_gpus=args.gpu_workers,
            include_dashboard=False,
            runtime_env=worker_runtime_env,
        )
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
        ray.init(
            # Ray Data keeps SQL readers and both callable actor pools live in
            # one streaming graph. Reserve reader slots explicitly so fixed
            # actor pools cannot starve the source and deadlock the pipeline.
            num_cpus=ray_cluster_num_cpus,
            num_gpus=args.gpu_workers,
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
        return run_project_ray_pipeline(
            source,
            worker_pool=worker_pool,
            expected_doc_ids=expected_ids,
            batch_size=args.batch_size,
            max_active_batches=args.max_active_batches,
            embedding_dimension=args.embedding_dimension,
        )

    execute(warmup_count, warmup_ids)
    if worker_pool is not None:
        stop_project_ray_worker_pool(worker_pool)
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
    result = execute(
        args.limit,
        formal_ids,
        dataset_passes=args.dataset_passes,
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
        "rows": total_rows,
        "unique_images": args.limit,
        "dataset_passes": args.dataset_passes,
        "batch_size": args.batch_size,
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
        "cpu_core_seconds_estimate": cpu_core_seconds,
        "cpu_core_seconds_per_image": cpu_core_seconds / total_rows,
        "gpu_seconds": gpu_seconds,
        "images_per_gpu_s": total_rows / gpu_seconds,
        "images_per_joule": total_rows / gpu_energy_j if gpu_energy_j > 0 else "",
        "engine_stats_text": result.engine_stats,
        "engine_stats_semantics": (
            "ray_data_operator_stats" if result.engine_stats else "unavailable"
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
        "schema_version": 10,
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
        "bandwidth_semantics": "logical_bytes_over_stage_wall_not_pcie_counter",
        "mfu_semantics": "estimated_only_when_verified_flops_and_dtype_peak_are_supplied",
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
