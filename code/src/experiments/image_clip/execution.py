"""Framework setup, worker lifetime, and observed CLIP execution."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import NamedTuple

from ...baselines.image.frameworks.daft import (
    build_daft_clip_embedder, build_daft_staged_clip_pipeline,
    run_daft_builtin_image_embedding, run_daft_clip_baseline, run_daft_staged_clip_baseline,
)
from ...baselines.image.frameworks.ray_data import build_ray_data_clip_pipeline, run_ray_data_clip_baseline
from ...modalities.image.execution import (
    EmbeddingCapture, ExecutionResult, build_semloom_ray_worker_pool,
    run_semloom_ray_pipeline, stop_semloom_ray_worker_pool,
)
from ...modalities.image.source import ImageSourceConfig
from ...modalities.image.staged_execution import run_semloom_ray_hse_pipeline
from ...modalities.image.resource_sampling import NvidiaSmiSampler, SystemCpuSampler
from .setup import ImageRunSetup, make_source
from .reporting import embedding_output_contract_metadata, write_embedding_capture

class ImageExecution:
    """Own one arm's framework setup and warmup/formal worker lifecycle."""

    def __init__(self, args, setup: ImageRunSetup, *, daft_module, ray_module):
        self.args = args
        self.setup = setup
        self.daft = daft_module
        self.ray = ray_module
        self.worker_pool = None
        self.embedder = None
        self.preprocessor = None
        self._initialize_arm()

    def _init_ray(self) -> None:
        if self.args.ray_address:
            self.ray.init(
                address=self.args.ray_address,
                include_dashboard=False,
                runtime_env=self.setup.worker_runtime_env,
            )
        else:
            self.ray.init(
                num_cpus=self.setup.ray_cluster_num_cpus,
                num_gpus=self.args.gpu_workers,
                include_dashboard=False,
                runtime_env=self.setup.worker_runtime_env,
            )

    def _build_project_workers(self):
        return build_semloom_ray_worker_pool(
            model_revision=self.args.model,
            processor_revision=self.setup.processor,
            cpu_workers=self.args.cpu_workers,
            gpu_workers=self.args.gpu_workers,
            dtype=self.args.dtype,
            detailed_stage_timing=self.args.detailed_stage_timing,
            torch_intraop_threads=self.args.torch_intraop_threads,
            torch_interop_threads=self.args.torch_interop_threads,
        )

    def _initialize_arm(self) -> None:
        args = self.args
        setup = self.setup
        if args.arm == "daft_builtin_embed":
            self._init_ray()
            self.daft.set_runner_ray(noop_if_initialized=True)
        elif args.arm == "daft_native":
            self.daft.set_runner_native(num_threads=setup.source_cpu_threads)
            self.embedder = build_daft_clip_embedder(
                model_revision=args.model,
                processor_revision=setup.processor,
                batch_size=args.batch_size,
                model_workers=setup.model_workers,
                gpus_per_worker=setup.gpus_per_model_worker,
                dtype=args.dtype,
                embedding_dimension=args.embedding_dimension,
                torch_intraop_threads=args.torch_intraop_threads,
                torch_interop_threads=args.torch_interop_threads,
            )
        elif args.arm == "daft_ray":
            self._init_ray()
            self.daft.set_runner_ray(noop_if_initialized=True)
            self.embedder = build_daft_clip_embedder(
                model_revision=args.model,
                processor_revision=setup.processor,
                batch_size=args.batch_size,
                model_workers=setup.model_workers,
                gpus_per_worker=setup.gpus_per_model_worker,
                dtype=args.dtype,
                embedding_dimension=args.embedding_dimension,
                torch_intraop_threads=args.torch_intraop_threads,
                torch_interop_threads=args.torch_interop_threads,
            )
        elif args.arm == "daft_staged":
            self._init_ray()
            self.daft.set_runner_ray(noop_if_initialized=True)
            self.preprocessor, self.embedder = build_daft_staged_clip_pipeline(
                model_revision=args.model,
                processor_revision=setup.processor,
                batch_size=args.batch_size,
                cpu_workers=args.cpu_workers,
                model_workers=setup.model_workers,
                gpus_per_worker=setup.gpus_per_model_worker,
                dtype=args.dtype,
                embedding_dimension=args.embedding_dimension,
                torch_intraop_threads=args.torch_intraop_threads,
                torch_interop_threads=args.torch_interop_threads,
            )
        elif args.arm == "ray_data_staged":
            self._init_ray()
        else:
            self._init_ray()
            self.daft.set_runner_native(num_threads=setup.source_cpu_threads)
            self.worker_pool = self._build_project_workers()

    def stop_project_workers(self) -> None:
        if self.worker_pool is not None:
            stop_semloom_ray_worker_pool(self.worker_pool)

    def start_formal_workers(self) -> float:
        if self.worker_pool is None:
            return 0.0
        started = time.perf_counter()
        self.worker_pool = self._build_project_workers()
        return time.perf_counter() - started

    def execute(
        self,
        limit: int,
        expected_ids: frozenset[str],
        *,
        dataset_passes: int = 1,
        embedding_capture: EmbeddingCapture | None = None,
    ) -> ExecutionResult:
        args = self.args
        setup = self.setup
        if args.arm == "ray_data_staged":
            dataset = build_ray_data_clip_pipeline(
                database_url=args.pg_dsn,
                source_config=ImageSourceConfig(
                    workload_name=args.workload_name,
                    limit=limit,
                    offset=args.offset,
                    dataset_passes=dataset_passes,
                ),
                source_shards=setup.source_shards,
                processor_revision=setup.processor,
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
            source_shards=setup.source_shards,
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
                embedder=self.embedder,
                expected_doc_ids=expected_ids,
                embedding_dimension=args.embedding_dimension,
            )
        if args.arm == "daft_staged":
            return run_daft_staged_clip_baseline(
                source,
                preprocessor=self.preprocessor,
                embedder=self.embedder,
                expected_doc_ids=expected_ids,
                embedding_dimension=args.embedding_dimension,
            )
        project_common = {
            "worker_pool": self.worker_pool,
            "expected_doc_ids": expected_ids,
            "batch_size": args.batch_size,
            "max_active_batches": args.max_active_batches,
            "embedding_dimension": args.embedding_dimension,
            "embedding_capture": embedding_capture,
        }
        if args.project_execution_mode == "hse_static":
            return run_semloom_ray_hse_pipeline(
                source,
                **project_common,
                encoded_block_bytes_upper_bound=(
                    int(setup.database_metadata["max_encoded_bytes"])
                    * args.batch_size
                ),
                limits=setup.hse_limits,
                model_revision=args.model,
                processor_revision=setup.processor,
                model_dtype=args.dtype,
                input_size=args.input_size,
            )
        return run_semloom_ray_pipeline(source, **project_common)


def wait_for_formal_start(args: argparse.Namespace) -> tuple[float, float]:
    planned_epoch_s = 0.0
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
        planned_epoch_s = float(barrier["start_epoch_s"]) + args.formal_start_offset_s
        remaining = planned_epoch_s - time.time()
        if remaining > 0:
            time.sleep(remaining)
    return planned_epoch_s, time.time()


class ObservedImageRun(NamedTuple):
    result: ExecutionResult
    worker_setup_s: float
    planned_start_epoch_s: float
    actual_start_epoch_s: float
    cpu_metrics: dict[str, object]
    gpu_metrics: dict[str, object]
    output_contract_metadata: dict[str, object]


def run_observed_image_execution(
    args: argparse.Namespace,
    setup: ImageRunSetup,
    execution: ImageExecution,
    *,
    daft_module,
    ray_module,
    torch_module,
    transformers_module,
) -> ObservedImageRun:
    execution.execute(setup.warmup_count, setup.warmup_ids)
    execution.stop_project_workers()
    planned_start_epoch_s, actual_start_epoch_s = wait_for_formal_start(args)
    gpu_sampler = NvidiaSmiSampler(
        args.gpu_sample_interval_s,
        active_device_count=args.gpu_workers,
    )
    cpu_sampler = SystemCpuSampler(args.cpu_sample_interval_s)
    gpu_sampler.start()
    cpu_sampler.start()
    worker_setup_s = execution.start_formal_workers()
    capture = EmbeddingCapture() if args.save_embeddings else None
    result = execution.execute(
        args.limit,
        setup.formal_ids,
        dataset_passes=args.dataset_passes,
        embedding_capture=capture,
    )
    output_contract_metadata = embedding_output_contract_metadata(
        args.arm,
        args.embedding_output_contract,
    )
    if capture is not None:
        write_embedding_capture(
            args,
            setup,
            capture,
            output_contract_metadata,
            daft_module=daft_module,
            ray_module=ray_module,
            torch_module=torch_module,
            transformers_module=transformers_module,
        )
    gpu_metrics = gpu_sampler.stop()
    cpu_metrics = cpu_sampler.stop()
    execution.stop_project_workers()
    return ObservedImageRun(
        result=result,
        worker_setup_s=worker_setup_s,
        planned_start_epoch_s=planned_start_epoch_s,
        actual_start_epoch_s=actual_start_epoch_s,
        cpu_metrics=cpu_metrics,
        gpu_metrics=gpu_metrics,
        output_contract_metadata=output_contract_metadata,
    )
