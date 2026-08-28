"""Project image multi-job runner with one shared typed CPU/GPU actor pool.

It compares a fixed per-job active-batch partition against the existing
engine-neutral fair work-credit coordinator.  Source queues stay bounded and
each job retains an independent exactly-once audit and stage telemetry.
"""

from __future__ import annotations

import csv
import json
import math
import queue
import random
import shutil
import subprocess
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, replace
from pathlib import Path

from src.infrastructure.config_env import expand_structure
from src.infrastructure.runner_lease import acquire_host_runner_lease
from src.infrastructure.runtime_env import ray_runtime_env
from src.modalities.image.execution import (
    EmbeddingAudit,
    build_semloom_ray_worker_pool,
    stop_semloom_ray_worker_pool,
)
from src.modalities.image.contracts import (
    build_image_runtime_snapshot,
    build_image_work_descriptor,
    image_work_calibration_signature,
)
from src.modalities.image.resource_sampling import (
    NvidiaSmiSampler,
    RayClusterResourceSampler,
    SystemCpuSampler,
)
from src.modalities.image.source import (
    DaftImageSource,
    ImageSourceConfig,
    read_image_source_metadata,
)
from src.planning.work import WorkDescriptor
from src.scheduling.submission_control.shared_credit import FairEndpointCreditCoordinator
from .manifest import (
    ImageJobManifest,
    load_image_job_manifest,
    validate_image_job_source,
)


@dataclass(frozen=True)
class ProjectImageJob:
    job_id: str
    workload_name: str
    limit: int
    offset: int
    start_offset_s: float
    weight: int = 1


@dataclass(frozen=True)
class ProjectImageScenario:
    scenario_id: str
    policy: str
    jobs: tuple[ProjectImageJob, ...]


@dataclass(frozen=True)
class ProjectImageMultiJobConfig:
    experiment_id: str
    output_root: Path
    database_url: str
    ray_address: str
    job_manifest: ImageJobManifest
    policy_revision: str
    model: str
    processor: str
    dtype: str
    batch_size: int
    source_shards: int
    cpu_workers: int
    gpu_workers: int
    max_active_batches: int
    model_flops_per_image: float
    gpu_peak_flops_per_s: float
    source_queue_batches_per_job: int
    warmup_rows: int
    warmup_repeats: int
    formal_repeats: int
    schedule_seed: int
    gpu_sample_interval_s: float
    cpu_sample_interval_s: float
    scenarios: tuple[ProjectImageScenario, ...]


@dataclass(frozen=True)
class _SourceBatch:
    batch_id: str
    job_id: str
    doc_ids: tuple[str, ...]
    encoded: list[bytes]
    work_units: int
    encoded_bytes: int
    ready_elapsed_s: float
    work_descriptor: WorkDescriptor


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or "REPLACE_ME" in value:
        raise ValueError(f"{field} must be a resolved non-empty string")
    return value.strip()


def _integer(value: object, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{field} must be an integer >= {minimum}")
    return value


def _number(value: object, field: str, *, allow_zero: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0 or (parsed == 0 and not allow_zero):
        raise ValueError(f"{field} must be finite and {'non-negative' if allow_zero else 'positive'}")
    return parsed


def _parse_scenario(raw: object, manifest: ImageJobManifest) -> ProjectImageScenario:
    if not isinstance(raw, dict) or set(raw) != {"id", "policy", "jobs"}:
        raise ValueError("project image scenario fields are invalid")
    scenario_id = _text(raw["id"], "scenario id")
    policy = _text(raw["policy"], f"scenario {scenario_id} policy")
    if policy not in {"static_partition", "proposed"}:
        raise ValueError("project image policy must be static_partition/proposed")
    job_ids = raw["jobs"]
    if (
        not isinstance(job_ids, list)
        or len(job_ids) not in {1, 4}
        or any(not isinstance(item, str) for item in job_ids)
    ):
        raise ValueError(f"scenario {scenario_id} must contain one or four jobs")
    jobs = tuple(
        ProjectImageJob(
            item.job_id,
            item.workload_name,
            item.limit,
            item.offset,
            item.multi_job_start_offset_s,
        )
        for item in manifest.select(tuple(job_ids))
    )
    return ProjectImageScenario(scenario_id, policy, jobs)


def load_project_image_multijob_config(path: str | Path) -> ProjectImageMultiJobConfig:
    payload = expand_structure(
        json.loads(Path(path).read_text(encoding="utf-8")),
        "project_image_multijob_config",
    )
    required = {
        "schema_version", "formal", "experiment_id", "output_root", "database_url",
        "ray_address", "job_manifest", "policy_revision", "model", "processor", "dtype", "batch_size", "source_shards",
        "cpu_workers", "gpu_workers", "max_active_batches", "model_flops_per_image",
        "gpu_peak_flops_per_s",
        "source_queue_batches_per_job", "warmup_rows", "warmup_repeats",
        "formal_repeats", "schedule_seed", "gpu_sample_interval_s",
        "cpu_sample_interval_s", "scenarios",
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise ValueError("project image multi-job config fields are invalid")
    if payload["schema_version"] != 1 or payload["formal"] is not True:
        raise ValueError("project image config requires schema_version=1/formal=true")
    manifest = load_image_job_manifest(_text(payload["job_manifest"], "job_manifest"))
    scenarios_raw = payload["scenarios"]
    if not isinstance(scenarios_raw, list) or not scenarios_raw:
        raise ValueError("scenarios must be non-empty")
    scenarios = tuple(_parse_scenario(item, manifest) for item in scenarios_raw)
    if len({item.scenario_id for item in scenarios}) != len(scenarios):
        raise ValueError("scenario ids must be unique")
    expected_scenarios = {
        "single_short_full_pool",
        "single_long1_full_pool",
        "single_long2_full_pool",
        "single_long3_full_pool",
        "fourjob_static_partition",
        "fourjob_proposed",
    }
    if {item.scenario_id for item in scenarios} != expected_scenarios:
        raise ValueError("project image matrix must keep four single controls plus static/proposed four-job arms")
    for scenario in scenarios:
        if scenario.scenario_id.startswith("single_"):
            expected_policy = "static_partition"
            expected_jobs = {scenario.scenario_id.removeprefix("single_").removesuffix("_full_pool")}
        else:
            expected_policy = (
                "static_partition" if scenario.scenario_id.endswith("static_partition") else "proposed"
            )
            expected_jobs = {"short", "long1", "long2", "long3"}
        if scenario.policy != expected_policy or {job.job_id for job in scenario.jobs} != expected_jobs:
            raise ValueError(f"project image scenario identity changed: {scenario.scenario_id}")
    output_root = Path(_text(payload["output_root"], "output_root"))
    if output_root.exists():
        raise FileExistsError(f"output_root already exists: {output_root}")
    warmups = _integer(payload["warmup_repeats"], "warmup_repeats", minimum=1)
    if warmups != 1:
        raise ValueError("warmup_repeats is frozen at one")
    formal_repeats = _integer(payload["formal_repeats"], "formal_repeats", minimum=1)
    if formal_repeats != 3:
        raise ValueError("formal_repeats is frozen at three")
    max_active = _integer(payload["max_active_batches"], "max_active_batches", minimum=1)
    if any(len(item.jobs) == 4 for item in scenarios) and max_active % 4:
        raise ValueError("four-job static control requires max_active_batches divisible by four")
    dtype = _text(payload["dtype"], "dtype")
    if dtype not in {"float16", "float32", "bfloat16"}:
        raise ValueError("dtype is unsupported")
    frozen_resources = {
        "batch_size": 64,
        "source_shards": 4,
        "cpu_workers": 16,
        "gpu_workers": 2,
        "max_active_batches": 32,
        "source_queue_batches_per_job": 2,
        "warmup_rows": 64,
    }
    resource_mismatches = {
        field: (payload[field], expected)
        for field, expected in frozen_resources.items()
        if payload[field] != expected
    }
    if dtype != "float16" or resource_mismatches:
        raise ValueError(
            f"project image frozen comparison contract changed: dtype={dtype}, "
            f"resources={resource_mismatches}"
        )
    return ProjectImageMultiJobConfig(
        experiment_id=_text(payload["experiment_id"], "experiment_id"),
        output_root=output_root,
        database_url=_text(payload["database_url"], "database_url"),
        ray_address=_text(payload["ray_address"], "ray_address"),
        job_manifest=manifest,
        policy_revision=_text(payload["policy_revision"], "policy_revision"),
        model=_text(payload["model"], "model"),
        processor=_text(payload["processor"], "processor"),
        dtype=dtype,
        batch_size=_integer(payload["batch_size"], "batch_size", minimum=1),
        source_shards=_integer(payload["source_shards"], "source_shards", minimum=1),
        cpu_workers=_integer(payload["cpu_workers"], "cpu_workers", minimum=1),
        gpu_workers=_integer(payload["gpu_workers"], "gpu_workers", minimum=1),
        max_active_batches=max_active,
        model_flops_per_image=_number(payload["model_flops_per_image"], "model_flops_per_image"),
        gpu_peak_flops_per_s=_number(payload["gpu_peak_flops_per_s"], "gpu_peak_flops_per_s"),
        source_queue_batches_per_job=_integer(
            payload["source_queue_batches_per_job"], "source_queue_batches_per_job", minimum=1
        ),
        warmup_rows=_integer(payload["warmup_rows"], "warmup_rows", minimum=1),
        warmup_repeats=warmups,
        formal_repeats=formal_repeats,
        schedule_seed=_integer(payload["schedule_seed"], "schedule_seed", minimum=1),
        gpu_sample_interval_s=_number(payload["gpu_sample_interval_s"], "gpu_sample_interval_s"),
        cpu_sample_interval_s=_number(payload["cpu_sample_interval_s"], "cpu_sample_interval_s"),
        scenarios=scenarios,
    )


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _append_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def _produce(
    config: ProjectImageMultiJobConfig,
    job: ProjectImageJob,
    output: queue.Queue[object],
    start_monotonic: float,
) -> None:
    try:
        remaining = start_monotonic + job.start_offset_s - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        source = DaftImageSource().read_sharded(
            config.database_url,
            ImageSourceConfig(job.workload_name, job.limit, job.offset),
            shards=config.source_shards,
        )
        batches = source.into_batches(config.batch_size).to_arrow_iter(results_buffer_size=2)
        for index, record_batch in enumerate(batches):
            doc_ids = tuple(str(item.as_py()) for item in record_batch["doc_id"])
            encoded = [item.as_py() for item in record_batch["image"]]
            encoded_bytes = sum(len(item) for item in encoded)
            descriptor = build_image_work_descriptor(
                row_count=len(doc_ids),
                encoded_bytes=encoded_bytes,
                model_revision=config.model,
                processor_revision=config.processor,
                dtype=config.dtype,
            )
            output.put(
                _SourceBatch(
                    batch_id=f"{job.job_id}:{index}",
                    job_id=job.job_id,
                    doc_ids=doc_ids,
                    encoded=encoded,
                    work_units=descriptor.primary.units,
                    encoded_bytes=encoded_bytes,
                    ready_elapsed_s=time.monotonic() - start_monotonic,
                    work_descriptor=descriptor,
                )
            )
    except Exception as exc:  # noqa: BLE001
        output.put(exc)
    finally:
        output.put(None)


def _state_row(
    *,
    started: float,
    event: str,
    job_id: str,
    waiting: dict[str, deque[_SourceBatch]],
    active_by_job: dict[str, int],
    active_batches: tuple[_SourceBatch, ...],
    calibration_signature: str,
    max_active_batches: int,
    batch_size: int,
) -> dict[str, object]:
    import ray

    shm = shutil.disk_usage("/dev/shm")
    available = ray.available_resources()
    observed_at_s = time.monotonic()
    snapshot_started = time.perf_counter()
    snapshot = build_image_runtime_snapshot(
        ready=tuple(
            (batch.work_descriptor, started + batch.ready_elapsed_s)
            for batches in waiting.values()
            for batch in batches
        ),
        active=tuple(batch.work_descriptor for batch in active_batches),
        observed_at_s=observed_at_s,
        calibration_signature=calibration_signature,
        max_active_batches=max_active_batches,
        batch_size=batch_size,
    )
    snapshot_build_s = time.perf_counter() - snapshot_started
    snapshot_checked_at_s = time.monotonic()
    snapshot_age_s = snapshot_checked_at_s - snapshot.observed_at_s
    # The scheduler's completion poll is 50 ms, so older state would already
    # belong to a previous control iteration. Observe-only mode records this
    # gate but never changes admission on failure.
    snapshot_max_age_s = 0.05
    return {
        "elapsed_s": observed_at_s - started,
        "event": event,
        "job_id": job_id,
        "active_batches": sum(active_by_job.values()),
        "active_by_job_json": json.dumps(active_by_job, sort_keys=True),
        "ready_batches_by_job_json": json.dumps(
            {key: len(value) for key, value in waiting.items()}, sort_keys=True
        ),
        "ray_available_cpu": float(available.get("CPU", 0.0)),
        "ray_available_gpu": float(available.get("GPU", 0.0)),
        "shm_used_bytes": shm.used,
        "runtime_state_mode": "observe_only",
        "runtime_state_fresh": snapshot.is_fresh(
            now_s=snapshot_checked_at_s,
            max_age_s=snapshot_max_age_s,
        ),
        "runtime_state_age_s": snapshot_age_s,
        "runtime_state_max_age_s": snapshot_max_age_s,
        "runtime_state_calibration_signature": snapshot.calibration_signature,
        "runtime_state_snapshot_json": json.dumps(asdict(snapshot), sort_keys=True),
        "runtime_state_snapshot_build_s": snapshot_build_s,
    }


def run_project_scenario(
    config: ProjectImageMultiJobConfig,
    scenario: ProjectImageScenario,
    *,
    phase: str,
    repeat: int,
    resource_trace_path: Path | None = None,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]]]:
    """Execute one measured scenario after callers have performed an untimed warm-up."""

    import ray

    run_jobs = (
        tuple(replace(job, limit=min(config.warmup_rows, job.limit)) for job in scenario.jobs)
        if phase == "warmup"
        else scenario.jobs
    )
    metadata = {}
    audits = {}
    for job in run_jobs:
        manifest_entry = next(item for item in config.job_manifest.jobs if item.job_id == job.job_id)
        if phase != "warmup":
            ids, job_metadata = validate_image_job_source(config.database_url, manifest_entry)
        else:
            ids, job_metadata = read_image_source_metadata(
                config.database_url,
                ImageSourceConfig(job.workload_name, job.limit, job.offset),
            )
        audits[job.job_id] = EmbeddingAudit(ids, dimension=512)
        metadata[job.job_id] = job_metadata

    setup_started = time.perf_counter()
    pool = build_semloom_ray_worker_pool(
        model_revision=config.model,
        processor_revision=config.processor,
        cpu_workers=config.cpu_workers,
        gpu_workers=config.gpu_workers,
        dtype=config.dtype,
    )
    setup_s = time.perf_counter() - setup_started
    queues = {
        job.job_id: queue.Queue(maxsize=config.source_queue_batches_per_job)
        for job in run_jobs
    }
    waiting = {job.job_id: deque() for job in run_jobs}
    producer_done = {job.job_id: False for job in run_jobs}
    producer_done_elapsed = {job.job_id: None for job in run_jobs}
    producer_error: list[Exception] = []
    active_by_job = {job.job_id: 0 for job in run_jobs}
    first_output = {job.job_id: None for job in run_jobs}
    completion = {job.job_id: None for job in run_jobs}
    completed_rows = {job.job_id: 0 for job in run_jobs}
    first_source_batch = {job.job_id: None for job in run_jobs}
    first_submit = {job.job_id: None for job in run_jobs}
    queue_waits = {job.job_id: [] for job in run_jobs}
    completion_latencies = {job.job_id: [] for job in run_jobs}
    stage_totals = {
        job.job_id: {"encoded_bytes": 0, "preprocess_s": 0.0, "h2d_s": 0.0, "forward_s": 0.0}
        for job in run_jobs
    }
    stage_values = {
        job.job_id: {"preprocess": [], "h2d": [], "forward": []}
        for job in run_jobs
    }
    pending: dict[object, tuple[_SourceBatch, float]] = {}
    traces: list[dict[str, object]] = []
    started = time.monotonic()
    calibration_signature = image_work_calibration_signature(
        model_revision=config.model,
        processor_revision=config.processor,
        dtype=config.dtype,
    )
    threads = [
        threading.Thread(
            target=_produce,
            args=(config, job, queues[job.job_id], started),
            daemon=True,
        )
        for job in run_jobs
    ]
    gpu_sampler = NvidiaSmiSampler(config.gpu_sample_interval_s, active_device_count=config.gpu_workers)
    cpu_sampler = SystemCpuSampler(config.cpu_sample_interval_s)
    ray_sampler = RayClusterResourceSampler(config.gpu_sample_interval_s)
    gpu_sampler.start()
    cpu_sampler.start()
    ray_sampler.start()
    for thread in threads:
        thread.start()

    coordinator = None
    if scenario.policy == "proposed":
        coordinator = FairEndpointCreditCoordinator(
            {"image-pool": (config.max_active_batches, config.max_active_batches * config.batch_size * 224 * 224)},
            quantum=config.batch_size * 224 * 224,
        )
    per_job_limit = config.max_active_batches // len(run_jobs)
    cpu_index = 0
    gpu_index = 0

    try:
        while True:
            for job in run_jobs:
                source_queue = queues[job.job_id]
                while len(waiting[job.job_id]) < config.source_queue_batches_per_job:
                    try:
                        item = source_queue.get_nowait()
                    except queue.Empty:
                        break
                    if item is None:
                        producer_done[job.job_id] = True
                        producer_done_elapsed[job.job_id] = time.monotonic() - started
                    elif isinstance(item, Exception):
                        producer_error.append(item)
                    else:
                        if first_source_batch[job.job_id] is None:
                            first_source_batch[job.job_id] = item.ready_elapsed_s
                        waiting[job.job_id].append(item)
            if producer_error:
                raise producer_error[0]

            submitted = False
            for job in run_jobs:
                if not waiting[job.job_id] or len(pending) >= config.max_active_batches:
                    continue
                batch = waiting[job.job_id][0]
                if coordinator is None:
                    allowed = active_by_job[job.job_id] < per_job_limit
                else:
                    allowed = coordinator.try_acquire(
                        request_id=batch.batch_id,
                        job_id=job.job_id,
                        endpoint_id="image-pool",
                        estimated_work=batch.work_units,
                        weight=job.weight,
                    )
                if not allowed:
                    continue
                waiting[job.job_id].popleft()
                preprocessor = pool.preprocessors[cpu_index % len(pool.preprocessors)]
                gpu_actor = pool.gpu_actors[gpu_index % len(pool.gpu_actors)]
                cpu_index += 1
                gpu_index += 1
                preprocessed = preprocessor.preprocess.remote(batch.doc_ids, batch.encoded)
                reference = gpu_actor.embed.remote(preprocessed)
                pending[reference] = (batch, time.monotonic())
                active_by_job[job.job_id] += 1
                submitted_elapsed = time.monotonic() - started
                queue_waits[job.job_id].append(submitted_elapsed - batch.ready_elapsed_s)
                if first_submit[job.job_id] is None:
                    first_submit[job.job_id] = submitted_elapsed
                traces.append(
                    _state_row(
                        started=started, event="submit", job_id=job.job_id,
                        waiting=waiting, active_by_job=active_by_job,
                        active_batches=tuple(item[0] for item in pending.values()),
                        calibration_signature=calibration_signature,
                        max_active_batches=config.max_active_batches,
                        batch_size=config.batch_size,
                    )
                )
                submitted = True

            if pending:
                ready, _ = ray.wait(list(pending), num_returns=1, timeout=0.05)
                if ready:
                    reference = ready[0]
                    batch, submitted_at = pending.pop(reference)
                    result = ray.get(reference)
                    completion_latencies[batch.job_id].append(time.monotonic() - submitted_at)
                    audits[batch.job_id].add_result(result)
                    completed_rows[batch.job_id] += len(batch.doc_ids)
                    active_by_job[batch.job_id] -= 1
                    if coordinator is not None:
                        coordinator.release(batch.batch_id, job_id=batch.job_id)
                    elapsed = time.monotonic() - started
                    if first_output[batch.job_id] is None:
                        first_output[batch.job_id] = elapsed
                    completion[batch.job_id] = elapsed
                    telemetry = result.telemetry
                    stage_totals[batch.job_id]["encoded_bytes"] += telemetry.encoded_bytes
                    stage_totals[batch.job_id]["preprocess_s"] += telemetry.preprocess_s
                    stage_totals[batch.job_id]["h2d_s"] += telemetry.h2d_s
                    stage_totals[batch.job_id]["forward_s"] += telemetry.forward_s
                    stage_values[batch.job_id]["preprocess"].append(telemetry.preprocess_s)
                    stage_values[batch.job_id]["h2d"].append(telemetry.h2d_s)
                    stage_values[batch.job_id]["forward"].append(telemetry.forward_s)
                    traces.append(
                        _state_row(
                            started=started, event="complete", job_id=batch.job_id,
                            waiting=waiting, active_by_job=active_by_job,
                            active_batches=tuple(item[0] for item in pending.values()),
                            calibration_signature=calibration_signature,
                            max_active_batches=config.max_active_batches,
                            batch_size=config.batch_size,
                        )
                    )
                    continue

            done = all(producer_done.values()) and not pending and all(not value for value in waiting.values())
            if done:
                break
            if not submitted:
                time.sleep(0.01)
    finally:
        gpu = gpu_sampler.stop()
        cpu = cpu_sampler.stop()
        ray_resources = ray_sampler.stop()
        if resource_trace_path is not None:
            ray_sampler.write_csv(resource_trace_path)
        stop_semloom_ray_worker_pool(pool)
        for thread in threads:
            thread.join(timeout=5)

    group_jct_s = time.monotonic() - started
    job_rows = []
    for job in run_jobs:
        audit = audits[job.job_id].finish()
        arrival = job.start_offset_s
        job_jct = float(completion[job.job_id]) - arrival
        job_rows.append(
            {
                "scenario_id": scenario.scenario_id,
                "policy": scenario.policy,
                "phase": phase,
                "repeat": repeat,
                "job_manifest_sha256": config.job_manifest.sha256,
                "policy_revision": config.policy_revision,
                "work_descriptor_calibration_signature": calibration_signature,
                "job_id": job.job_id,
                "job_count": len(run_jobs),
                "rows": job.limit,
                "arrival_offset_s": arrival,
                "completion_elapsed_s": float(completion[job.job_id]),
                "jct_s": job_jct,
                "first_output_s": float(first_output[job.job_id]) - arrival,
                "first_source_batch_s": float(first_source_batch[job.job_id]) - arrival,
                "source_done_s": float(producer_done_elapsed[job.job_id]) - arrival,
                "first_submit_s": float(first_submit[job.job_id]) - arrival,
                "images_per_s": job.limit / job_jct,
                "completed_rows": completed_rows[job.job_id],
                "exactly_once": audit["exactly_once"],
                "input_encoded_bytes": metadata[job.job_id]["input_encoded_bytes"],
                "batch_queue_wait_p50_s": _percentile(queue_waits[job.job_id], 0.50),
                "batch_queue_wait_p95_s": _percentile(queue_waits[job.job_id], 0.95),
                "batch_completion_wall_p50_s": _percentile(
                    completion_latencies[job.job_id], 0.50
                ),
                "batch_completion_wall_p95_s": _percentile(
                    completion_latencies[job.job_id], 0.95
                ),
                "batch_preprocess_p95_s": _percentile(
                    stage_values[job.job_id]["preprocess"], 0.95
                ),
                "batch_h2d_p95_s": _percentile(
                    stage_values[job.job_id]["h2d"], 0.95
                ),
                "batch_forward_p95_s": _percentile(
                    stage_values[job.job_id]["forward"], 0.95
                ),
                **stage_totals[job.job_id],
            }
        )
    rates = [row["images_per_s"] for row in job_rows]
    jain = (sum(rates) ** 2) / (len(rates) * sum(value * value for value in rates))
    total_rows = sum(job.limit for job in run_jobs)
    estimated_mfu = (
        total_rows
        * config.model_flops_per_image
        / (group_jct_s * config.gpu_workers * config.gpu_peak_flops_per_s)
    )
    snapshot_build_values = [
        float(row["runtime_state_snapshot_build_s"])
        for row in traces
    ]
    snapshot_age_values = [
        float(row["runtime_state_age_s"])
        for row in traces
    ]
    snapshot_fresh_count = sum(
        bool(row["runtime_state_fresh"])
        for row in traces
    )
    group = {
        "scenario_id": scenario.scenario_id,
        "policy": scenario.policy,
        "phase": phase,
        "repeat": repeat,
        "job_manifest_sha256": config.job_manifest.sha256,
        "policy_revision": config.policy_revision,
        "work_descriptor_mode": "staged_v1_legacy_equivalent",
        "work_descriptor_calibration_signature": calibration_signature,
        "runtime_state_mode": "observe_only",
        "runtime_state_snapshot_count": len(snapshot_build_values),
        "runtime_state_fresh_ratio": (
            snapshot_fresh_count / len(snapshot_build_values)
            if snapshot_build_values
            else 0.0
        ),
        "runtime_state_age_p95_s": _percentile(snapshot_age_values, 0.95),
        "runtime_state_snapshot_build_mean_s": (
            sum(snapshot_build_values) / len(snapshot_build_values)
            if snapshot_build_values
            else 0.0
        ),
        "runtime_state_snapshot_build_p95_s": _percentile(
            snapshot_build_values, 0.95
        ),
        "policy_implementation": (
            config.policy_revision if scenario.policy == "proposed" else "frozen_static_partition"
        ),
        "job_count": len(run_jobs),
        "rows": total_rows,
        "group_jct_s": group_jct_s,
        "worker_setup_s": setup_s,
        "images_per_s": total_rows / group_jct_s,
        "model_flops_per_image": config.model_flops_per_image,
        "gpu_peak_flops_per_s": config.gpu_peak_flops_per_s,
        "estimated_e2e_mfu": estimated_mfu,
        "jain_images_per_s": jain,
        "static_partition_active_batches_per_job": per_job_limit if coordinator is None else "",
        "shared_active_batch_limit": config.max_active_batches if coordinator is not None else "",
        "custom_scheduling_code": True,
        "scheduler_owner": "project_ray_shared_image_pool",
        **gpu,
        **cpu,
        **ray_resources,
    }
    return group, job_rows, traces


def _scenario_order(config: ProjectImageMultiJobConfig, phase: str, repeat: int) -> tuple[ProjectImageScenario, ...]:
    scenarios = list(config.scenarios)
    random.Random(f"{config.schedule_seed}:{phase}").shuffle(scenarios)
    shift = (repeat - 1) % len(scenarios)
    return tuple(scenarios[shift:] + scenarios[:shift])


def _repository_commit() -> str:
    repository_root = next(
        parent for parent in Path(__file__).resolve().parents if (parent / ".git").exists()
    )
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True,
        cwd=repository_root,
    ).stdout.strip()


def run_project_image_multijob(
    config: ProjectImageMultiJobConfig, *, gate_only: bool = False
) -> int:
    """Run the complete project single/static/shared matrix on an external Ray cluster."""

    import daft
    import ray

    ray.init(address=config.ray_address, runtime_env=ray_runtime_env(Path(__file__).resolve().parents[3]))
    daft.set_runner_native(num_threads=config.source_shards)
    config.output_root.mkdir(parents=True)
    index = {
        "schema_version": 1,
        "experiment_id": config.experiment_id,
        "repository_commit": _repository_commit(),
        "job_manifest": str(config.job_manifest.path),
        "job_manifest_sha256": config.job_manifest.sha256,
        "policy_revision": config.policy_revision,
        "execution_mode": "gate" if gate_only else "formal_matrix",
        "status": "running",
        "runs": [],
    }
    _atomic_json(config.output_root / "matrix_index.json", index)
    try:
        with acquire_host_runner_lease(config.output_root.parent, repository_commit=index["repository_commit"]):
            try:
                phases = (
                    (("gate", 1),)
                    if gate_only
                    else (("warmup", config.warmup_repeats), ("formal", config.formal_repeats))
                )
                for phase, count in phases:
                    for repeat in range(1, count + 1):
                        scheduled = (
                            tuple(
                                scenario
                                for scenario in config.scenarios
                                if scenario.scenario_id.startswith("fourjob_")
                            )
                            if gate_only
                            else _scenario_order(config, phase, repeat)
                        )
                        for scenario in scheduled:
                            group, jobs, traces = run_project_scenario(
                                config,
                                scenario,
                                phase=phase,
                                repeat=repeat,
                                resource_trace_path=(
                                    config.output_root
                                    / "resource_traces"
                                    / f"{phase}_{repeat}_{scenario.scenario_id}.csv"
                                ),
                            )
                            _append_csv(config.output_root / "group_runs.csv", [group])
                            _append_csv(config.output_root / "job_runs.csv", jobs)
                            trace_rows = [
                                {"scenario_id": scenario.scenario_id, "phase": phase, "repeat": repeat, **row}
                                for row in traces
                            ]
                            _append_csv(config.output_root / "control_trace.csv", trace_rows)
                            index["runs"].append(
                                {"scenario_id": scenario.scenario_id, "phase": phase, "repeat": repeat, "status": "passed"}
                            )
                            _atomic_json(config.output_root / "matrix_index.json", index)
            except Exception as exc:
                index["status"] = "failed"
                index["error"] = f"{type(exc).__name__}:{exc}"
                _atomic_json(config.output_root / "matrix_index.json", index)
                return 1
        index["status"] = "passed"
        _atomic_json(config.output_root / "matrix_index.json", index)
        return 0
    finally:
        ray.shutdown()
