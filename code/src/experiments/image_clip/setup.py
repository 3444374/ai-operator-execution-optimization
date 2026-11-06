"""Validate the image experiment and prepare source and resource declarations."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import NamedTuple

from ...modalities.image import DaftImageSource, ImageSourceConfig
from ...modalities.image.source import read_image_source_metadata
from ...modalities.image.resource_budget import build_ray_cpu_budget
from ...baselines.image.provenance import image_arm_provenance, require_formal_arm_allowed
from ...scheduling.runtime.stage_broker import StageBrokerLimits
from ...infrastructure.runtime_env import ray_runtime_env
from . import CODE_ROOT

ARMS = (
    "daft_builtin_embed",
    "daft_native",
    "daft_ray",
    "daft_staged",
    "ray_data_staged",
    "project_ray",
)


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


class ImageRunSetup(NamedTuple):
    processor: str
    provenance: object
    embedding_target: Path | None
    embedding_sidecar: Path | None
    formal_ids: frozenset[str]
    database_metadata: dict[str, object]
    source_doc_ids_sha256: str
    source_manifest_match: bool
    hse_limits: StageBrokerLimits
    warmup_count: int
    warmup_ids: frozenset[str]
    model_workers: int
    gpus_per_model_worker: float
    source_shards: int
    source_cpu_threads: int
    cpu_budget: object
    ray_cluster_num_cpus: int
    worker_runtime_env: dict[str, object]


def validate_image_run_args(
    args: argparse.Namespace,
) -> tuple[str, Path | None, Path | None, object]:
    provenance = image_arm_provenance(args.arm)
    try:
        require_formal_arm_allowed(
            args.arm,
            phase=args.phase,
            allow_non_native_diagnostic=args.allow_non_native_diagnostic,
        )
    except ValueError as error:
        raise SystemExit(str(error)) from error
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
    embedding_target = None
    embedding_sidecar = None
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
    return args.processor or args.model, embedding_target, embedding_sidecar, provenance


def load_image_run_setup(
    args: argparse.Namespace,
    *,
    processor: str,
    embedding_target: Path | None,
    embedding_sidecar: Path | None,
    provenance: object,
) -> ImageRunSetup:
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
            raise ValueError(
                "PostgreSQL image source no longer matches the immutable manifest"
            )
    hse_limits = StageBrokerLimits(
        encoded_bytes=args.hse_encoded_bytes_limit or max(
            1,
            int(database_metadata["max_encoded_bytes"])
            * args.batch_size
            * args.max_active_batches
            * 2,
        ),
        ready_bytes=args.hse_ready_bytes_limit or (
            args.batch_size
            * args.max_active_batches
            * 3
            * args.input_size
            * args.input_size
            * 4
        ),
        ready_work=args.hse_ready_work_limit or (
            args.batch_size
            * args.max_active_batches
            * args.input_size
            * args.input_size
        ),
        prepare_inflight=(
            args.hse_prepare_inflight_limit or args.cpu_workers
        ),
        model_inflight=args.hse_model_inflight_limit or args.gpu_workers,
    )
    warmup_count = min(args.warmup_rows, args.limit)
    warmup_ids, _ = read_database_metadata(
        args.pg_dsn,
        workload_name=args.workload_name,
        limit=warmup_count,
        offset=args.offset,
    )
    if args.arm in ("daft_native", "daft_ray", "daft_staged"):
        model_workers = args.daft_model_workers or args.gpu_workers
        gpus_per_model_worker = args.gpu_workers / model_workers
        if gpus_per_model_worker > 1:
            raise SystemExit(
                "Daft model workers must be at least the physical GPU count"
            )
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
    return ImageRunSetup(
        processor=processor,
        provenance=provenance,
        embedding_target=embedding_target,
        embedding_sidecar=embedding_sidecar,
        formal_ids=formal_ids,
        database_metadata=database_metadata,
        source_doc_ids_sha256=source_doc_ids_sha256,
        source_manifest_match=source_manifest_match,
        hse_limits=hse_limits,
        warmup_count=warmup_count,
        warmup_ids=warmup_ids,
        model_workers=model_workers,
        gpus_per_model_worker=gpus_per_model_worker,
        source_shards=source_shards,
        source_cpu_threads=source_cpu_threads,
        cpu_budget=cpu_budget,
        ray_cluster_num_cpus=cpu_budget.cluster_slots,
        worker_runtime_env=ray_runtime_env(CODE_ROOT),
    )
