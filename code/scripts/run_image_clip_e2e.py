#!/usr/bin/env python
"""Run comparable PostgreSQL -> Daft -> CLIP image-embedding E2E arms.

The three arms keep the input table, processor, model, batch size, GPU count,
output validation, and timing boundary fixed:

* ``daft_native``: official-style Daft ``@daft.cls`` native GPU UDF.
* ``daft_ray``: the same UDF under Daft's Ray runner.
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
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.image import DaftImageSource, ImageSourceConfig  # noqa: E402
from src.image.daft_baseline import (  # noqa: E402
    build_daft_clip_embedder,
    run_daft_clip_baseline,
)
from src.image.execution import (  # noqa: E402
    ExecutionResult,
    build_project_ray_worker_pool,
    run_project_ray_pipeline,
    stop_project_ray_worker_pool,
)


ARMS = ("daft_native", "daft_ray", "project_ray")
CSV_FIELDS = (
    "arm",
    "phase",
    "repeat_index",
    "workload_name",
    "rows",
    "batch_size",
    "cpu_workers",
    "gpu_workers",
    "source_shards",
    "max_active_batches",
    "worker_setup_s",
    "operator_e2e_s",
    "first_output_s",
    "images_per_s",
    "batch_service_p50_s",
    "batch_service_p95_s",
    "output_rows",
    "exactly_once",
    "embedding_checksum",
    "max_norm_error",
    "gpu_util_mean_pct",
    "gpu_util_peak_pct",
    "gpu_memory_peak_mib",
    "gpu_samples",
    "gpu_per_device_json",
    "model_revision",
    "processor_revision",
    "dtype",
    "embedding_dimension",
    "daft_version",
    "ray_version",
    "torch_version",
    "transformers_version",
    "gpu_name",
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
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--warmup-rows", type=int, default=64)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--cpu-workers", type=int, default=4)
    parser.add_argument("--gpu-workers", type=int, default=2)
    parser.add_argument("--max-active-batches", type=int, default=8)
    parser.add_argument("--dtype", choices=("float16", "float32", "bfloat16"), default="float16")
    parser.add_argument("--embedding-dimension", type=int, default=512)
    parser.add_argument("--phase", choices=("gate", "warmup", "formal"), default="gate")
    parser.add_argument("--repeat-index", type=int, default=0)
    parser.add_argument("--gpu-sample-interval-s", type=float, default=0.5)
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--out-manifest", required=True)
    return parser.parse_args()


class NvidiaSmiSampler:
    """Low-frequency, arm-neutral GPU utilization sampler."""

    def __init__(self, interval_s: float) -> None:
        if interval_s <= 0:
            raise ValueError("GPU sample interval must be positive")
        self.interval_s = interval_s
        self._stop = threading.Event()
        self._samples: list[tuple[int, float, float]] = []
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> dict[str, object]:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_s * 4))
        per_device: dict[int, dict[str, list[float]]] = {}
        for index, utilization, memory_mib in self._samples:
            entry = per_device.setdefault(index, {"utilization": [], "memory_mib": []})
            entry["utilization"].append(utilization)
            entry["memory_mib"].append(memory_mib)
        summaries = {
            str(index): {
                "util_mean_pct": statistics.fmean(values["utilization"]),
                "util_peak_pct": max(values["utilization"]),
                "memory_peak_mib": max(values["memory_mib"]),
                "samples": len(values["utilization"]),
            }
            for index, values in per_device.items()
        }
        all_util = [sample[1] for sample in self._samples]
        all_memory = [sample[2] for sample in self._samples]
        return {
            "gpu_util_mean_pct": statistics.fmean(all_util) if all_util else 0.0,
            "gpu_util_peak_pct": max(all_util, default=0.0),
            "gpu_memory_peak_mib": max(all_memory, default=0.0),
            "gpu_samples": len(self._samples),
            "gpu_per_device_json": json.dumps(summaries, sort_keys=True),
        }

    def _run(self) -> None:
        command = [
            "nvidia-smi",
            "--query-gpu=index,utilization.gpu,memory.used",
            "--format=csv,noheader,nounits",
        ]
        while not self._stop.is_set():
            completed = subprocess.run(
                command,
                capture_output=True,
                check=False,
                text=True,
            )
            if completed.returncode == 0:
                for line in completed.stdout.splitlines():
                    fields = [item.strip() for item in line.split(",")]
                    if len(fields) == 3:
                        self._samples.append(
                            (int(fields[0]), float(fields[1]), float(fields[2]))
                        )
            self._stop.wait(self.interval_s)


def read_database_metadata(
    dsn: str,
    *,
    workload_name: str,
    limit: int,
    offset: int,
) -> tuple[frozenset[str], dict[str, str]]:
    import psycopg

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT doc_id FROM image_documents "
                "WHERE workload_name = %s ORDER BY doc_id LIMIT %s OFFSET %s",
                (workload_name, limit, offset),
            )
            doc_ids = frozenset(str(row[0]) for row in cursor.fetchall())
            cursor.execute("SHOW server_version")
            server_version = str(cursor.fetchone()[0])
            cursor.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'vector'"
            )
            extension = cursor.fetchone()
    if len(doc_ids) != limit:
        raise ValueError(f"expected {limit} source rows, found {len(doc_ids)}")
    return doc_ids, {
        "server_version": server_version,
        "pgvector_version": str(extension[0]) if extension else "not_installed",
    }


def make_source(
    dsn: str,
    workload_name: str,
    limit: int,
    offset: int,
    *,
    source_shards: int,
):
    return DaftImageSource().read_sharded(
        dsn,
        ImageSourceConfig(
            workload_name=workload_name,
            limit=limit,
            offset=offset,
        ),
        shards=source_shards,
    )


def percentile(values: tuple[float, ...], fraction: float) -> float | str:
    if not values:
        return ""
    ordered = sorted(values)
    position = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return ordered[position]


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
    processor = args.processor or args.model
    positive = (
        args.limit,
        args.warmup_rows,
        args.batch_size,
        args.cpu_workers,
        args.gpu_workers,
        args.max_active_batches,
        args.embedding_dimension,
    )
    if not args.pg_dsn:
        raise SystemExit("--pg-dsn is required (or set DATABASE_URL/PG_DSN)")
    if min(positive) <= 0 or args.offset < 0:
        raise SystemExit("row, batch, worker, and dimension values must be positive")
    if args.max_active_batches < args.gpu_workers:
        raise SystemExit("--max-active-batches must be at least --gpu-workers")

    import daft
    import ray
    import torch
    import transformers

    formal_ids, database_metadata = read_database_metadata(
        args.pg_dsn,
        workload_name=args.workload_name,
        limit=args.limit,
        offset=args.offset,
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
    if args.arm == "daft_native":
        daft.set_runner_native(num_threads=args.cpu_workers)
        embedder = build_daft_clip_embedder(
            model_revision=args.model,
            processor_revision=processor,
            batch_size=args.batch_size,
            gpu_workers=args.gpu_workers,
            dtype=args.dtype,
            embedding_dimension=args.embedding_dimension,
        )
    elif args.arm == "daft_ray":
        ray.init(
            num_cpus=max(args.cpu_workers + args.gpu_workers, 4),
            num_gpus=args.gpu_workers,
            include_dashboard=False,
        )
        daft.set_runner_ray(noop_if_initialized=True)
        embedder = build_daft_clip_embedder(
            model_revision=args.model,
            processor_revision=processor,
            batch_size=args.batch_size,
            gpu_workers=args.gpu_workers,
            dtype=args.dtype,
            embedding_dimension=args.embedding_dimension,
        )
    else:
        ray.init(
            num_cpus=max(args.cpu_workers + args.gpu_workers, 4),
            num_gpus=args.gpu_workers,
            include_dashboard=False,
        )
        daft.set_runner_native(num_threads=args.cpu_workers)
        worker_pool = build_project_ray_worker_pool(
            model_revision=args.model,
            processor_revision=processor,
            cpu_workers=args.cpu_workers,
            gpu_workers=args.gpu_workers,
            dtype=args.dtype,
        )

    def execute(limit: int, expected_ids: frozenset[str]) -> ExecutionResult:
        source = make_source(
            args.pg_dsn,
            args.workload_name,
            limit,
            args.offset,
            source_shards=args.gpu_workers,
        )
        if args.arm in ("daft_native", "daft_ray"):
            return run_daft_clip_baseline(
                source,
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
    sampler = NvidiaSmiSampler(args.gpu_sample_interval_s)
    sampler.start()
    if worker_pool is not None:
        setup_started = time.perf_counter()
        worker_pool = build_project_ray_worker_pool(
            model_revision=args.model,
            processor_revision=processor,
            cpu_workers=args.cpu_workers,
            gpu_workers=args.gpu_workers,
            dtype=args.dtype,
        )
        worker_setup_s = time.perf_counter() - setup_started
    else:
        # Daft creates the model-owning UDF actor lazily inside each query, so
        # its worker/model startup is already included in result.total_s and
        # first_output_s rather than this explicit setup field.
        worker_setup_s = 0.0
    result = execute(args.limit, formal_ids)
    gpu_metrics = sampler.stop()
    if worker_pool is not None:
        stop_project_ray_worker_pool(worker_pool)

    operator_e2e_s = result.total_s + worker_setup_s
    first_output_s = result.first_output_s + worker_setup_s

    row: dict[str, object] = {
        "arm": args.arm,
        "phase": args.phase,
        "repeat_index": args.repeat_index,
        "workload_name": args.workload_name,
        "rows": args.limit,
        "batch_size": args.batch_size,
        "cpu_workers": args.cpu_workers,
        "gpu_workers": args.gpu_workers,
        "source_shards": args.gpu_workers,
        "max_active_batches": args.max_active_batches,
        "worker_setup_s": worker_setup_s,
        "operator_e2e_s": operator_e2e_s,
        "first_output_s": first_output_s,
        "images_per_s": args.limit / operator_e2e_s,
        "batch_service_p50_s": percentile(result.batch_service_s, 0.50),
        "batch_service_p95_s": percentile(result.batch_service_s, 0.95),
        **result.audit,
        **gpu_metrics,
        "model_revision": args.model,
        "processor_revision": processor,
        "dtype": args.dtype,
        "embedding_dimension": args.embedding_dimension,
        "daft_version": daft.__version__,
        "ray_version": ray.__version__,
        "torch_version": torch.__version__,
        "transformers_version": transformers.__version__,
        "gpu_name": torch.cuda.get_device_name(0),
        **database_metadata,
        "git_commit": git_commit(),
    }
    append_csv(Path(args.out_csv), row)
    manifest = {
        "schema_version": 1,
        "timing_boundary": "per_query_model_worker_setup_to_last_embedding_batch_returned",
        "worker_lifecycle": "per_query_cold_model_worker",
        "ray_framework_startup_included": False,
        "writeback_included": False,
        "preprocessing": "torchvision_tensor_decode_and_processor",
        "hidden_batching": False,
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
