"""Static HSE execution path with a real byte-bounded ready-block broker."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass

from ...scheduling.runtime.stage_broker import (
    BoundedStageBroker,
    StageBrokerLimits,
    StageLease,
)
from .execution import (
    EmbeddingAudit,
    EmbeddingCapture,
    ExecutionResult,
    ProjectRayWorkerPool,
)
from .staged import build_encoded_image_block_descriptor


@dataclass
class _PrepareCall:
    lease: StageLease
    payload_ref: object
    actor_index: int


@dataclass
class _ModelCall:
    lease: StageLease
    source_admitted_at_s: float
    actor_index: int


def run_project_ray_hse_pipeline(
    source_df,
    *,
    worker_pool: ProjectRayWorkerPool,
    expected_doc_ids: frozenset[str],
    batch_size: int,
    max_active_batches: int,
    encoded_block_bytes_upper_bound: int,
    limits: StageBrokerLimits,
    model_revision: str,
    processor_revision: str,
    model_dtype: str,
    input_size: int = 224,
    embedding_dimension: int = 512,
    job_id: str = "single-image-job",
    embedding_capture: EmbeddingCapture | None = None,
) -> ExecutionResult:
    """Run static HSE without materializing prepared tensors on the driver.

    This path changes only flow ownership relative to ``run_project_ray_pipeline``:
    worker pools, image semantics, source, and model backend stay identical.
    """
    if min(
        batch_size,
        max_active_batches,
        encoded_block_bytes_upper_bound,
        input_size,
        embedding_dimension,
    ) <= 0:
        raise ValueError("HSE batch, window, and image dimensions must be positive")
    if not worker_pool.preprocessors or not worker_pool.gpu_actors:
        raise ValueError("worker_pool must contain CPU and GPU actors")
    if limits.prepare_inflight > len(worker_pool.preprocessors):
        raise ValueError("prepare inflight cannot exceed the prestarted CPU actor count")
    if limits.model_inflight > len(worker_pool.gpu_actors):
        raise ValueError("model inflight cannot exceed the prestarted GPU actor count")
    if encoded_block_bytes_upper_bound > limits.encoded_bytes:
        raise BufferError(
            "one worst-case HSE source block exceeds encoded_bytes; reduce "
            "batch_size or increase the explicit byte limit"
        )

    import ray

    broker = BoundedStageBroker(limits)
    audit = EmbeddingAudit(
        expected_doc_ids=expected_doc_ids,
        dimension=embedding_dimension,
        capture=embedding_capture,
    )
    prepare_calls: dict[object, _PrepareCall] = {}
    model_calls: dict[object, _ModelCall] = {}
    encoded_payloads: dict[str, list[bytes]] = {}
    prepared_payloads: dict[str, object] = {}
    admitted_at: dict[str, float] = {}
    source_exhausted = False
    source_sequence = 0
    available_cpu_actors = set(range(len(worker_pool.preprocessors)))
    available_gpu_actors = set(range(len(worker_pool.gpu_actors)))
    first_output_s: float | None = None
    batch_completion_wall_s: list[float] = []
    batch_actor_service_s: list[float] = []
    batch_unattributed_wait_s: list[float] = []
    batch_preprocess_s: list[float] = []
    batch_host_copy_s: list[float] = []
    batch_h2d_s: list[float] = []
    batch_forward_s: list[float] = []
    batch_d2h_s: list[float] = []
    batch_source_next_s: list[float] = []
    batch_driver_materialize_s: list[float] = []
    batch_submit_s: list[float] = []
    batch_prepare_queue_s: list[float] = []
    batch_ready_residence_s: list[float] = []
    encoded_bytes = 0
    input_tensor_bytes = 0
    device_input_bytes = 0
    output_bytes = 0
    submitted_batches = 0
    active_blocks_peak = 0
    encoded_bytes_peak = 0
    ready_bytes_peak = 0
    prepare_inflight_peak = 0
    model_inflight_peak = 0
    started = time.perf_counter()
    batches = iter(source_df.into_batches(batch_size).to_arrow_iter(results_buffer_size=2))

    def record_peaks() -> None:
        nonlocal active_blocks_peak, encoded_bytes_peak, ready_bytes_peak
        nonlocal prepare_inflight_peak, model_inflight_peak
        snapshot = broker.snapshot()
        active_blocks_peak = max(active_blocks_peak, snapshot.active_blocks)
        encoded_bytes_peak = max(encoded_bytes_peak, snapshot.encoded_held_bytes)
        ready_bytes_peak = max(ready_bytes_peak, snapshot.ready_held_bytes)
        prepare_inflight_peak = max(prepare_inflight_peak, snapshot.prepare_inflight)
        model_inflight_peak = max(model_inflight_peak, snapshot.model_inflight)

    while not source_exhausted or not broker.is_drained():
        progressed = False

        while broker.snapshot().active_blocks < max_active_batches and not source_exhausted:
            snapshot = broker.snapshot()
            if (
                snapshot.encoded_held_bytes + encoded_block_bytes_upper_bound
                > limits.encoded_bytes
            ):
                break
            source_next_started = time.perf_counter()
            try:
                record_batch = next(batches)
            except StopIteration:
                source_exhausted = True
                break
            batch_source_next_s.append(time.perf_counter() - source_next_started)
            materialize_started = time.perf_counter()
            doc_ids = tuple(str(item.as_py()) for item in record_batch["doc_id"])
            encoded = [item.as_py() for item in record_batch["image"]]
            descriptor = build_encoded_image_block_descriptor(
                job_id=job_id,
                ordered_sequence=source_sequence,
                row_ids=doc_ids,
                encoded_images=encoded,
                model_revision=model_revision,
                processor_revision=processor_revision,
                model_dtype=model_dtype,
                created_at_s=time.perf_counter(),
                input_size=input_size,
                embedding_dimension=embedding_dimension,
            )
            batch_driver_materialize_s.append(time.perf_counter() - materialize_started)
            if descriptor.physical_bytes > encoded_block_bytes_upper_bound:
                raise BufferError(
                    "HSE source block exceeded the immutable database-derived byte "
                    "upper bound"
                )
            if (
                descriptor.ready_bytes_estimate > limits.ready_bytes
                or descriptor.model_work_units > limits.ready_work
            ):
                raise BufferError(
                    "one HSE prepared block exceeds ready byte/work capacity; "
                    "reduce batch_size or increase the explicit limits"
                )
            if not broker.can_accept_encoded(descriptor):
                break
            broker.enqueue_encoded(descriptor)
            encoded_payloads[descriptor.block_id] = encoded
            admitted_at[descriptor.block_id] = time.perf_counter()
            source_sequence += 1
            submitted_batches += 1
            progressed = True

        while True:
            if not available_cpu_actors:
                break
            lease = broker.lease_prepare(now_s=time.perf_counter())
            if lease is None:
                break
            actor_index = min(available_cpu_actors)
            available_cpu_actors.remove(actor_index)
            preprocessor = worker_pool.preprocessors[actor_index]
            encoded = encoded_payloads.pop(lease.descriptor.block_id)
            submit_started = time.perf_counter()
            try:
                descriptor_ref, payload_ref = preprocessor.preprocess_staged.options(
                    num_returns=2
                ).remote(lease.descriptor, encoded)
            except Exception:
                available_cpu_actors.add(actor_index)
                broker.fail_prepare(lease.lease_id, requeue=False)
                raise
            batch_submit_s.append(time.perf_counter() - submit_started)
            batch_prepare_queue_s.append(
                max(0.0, lease.issued_at_s - admitted_at[lease.descriptor.block_id])
            )
            prepare_calls[descriptor_ref] = _PrepareCall(
                lease=lease,
                payload_ref=payload_ref,
                actor_index=actor_index,
            )
            record_peaks()

        while True:
            if not available_gpu_actors:
                break
            lease = broker.lease_model(now_s=time.perf_counter())
            if lease is None:
                break
            payload_ref = prepared_payloads.pop(lease.descriptor.block_id)
            batch_ready_residence_s.append(
                max(
                    0.0,
                    lease.issued_at_s
                    - (lease.descriptor.ready_at_s or lease.issued_at_s),
                )
            )
            actor_index = min(available_gpu_actors)
            available_gpu_actors.remove(actor_index)
            gpu_actor = worker_pool.gpu_actors[actor_index]
            submit_started = time.perf_counter()
            try:
                output_ref = gpu_actor.embed.remote(payload_ref)
            except Exception:
                available_gpu_actors.add(actor_index)
                broker.fail_model(lease.lease_id, requeue=False)
                raise
            submitted_at_s = time.perf_counter()
            batch_submit_s.append(submitted_at_s - submit_started)
            model_calls[output_ref] = _ModelCall(
                lease=lease,
                source_admitted_at_s=admitted_at[lease.descriptor.block_id],
                actor_index=actor_index,
            )
            progressed = True
            record_peaks()

        pending_refs = tuple(prepare_calls) + tuple(model_calls)
        if not pending_refs:
            if source_exhausted and broker.is_drained():
                break
            if not progressed:
                raise RuntimeError("HSE made no progress with unfinished work")
            continue

        ready_refs, _ = ray.wait(list(pending_refs), num_returns=1)
        reference = ready_refs[0]
        now_s = time.perf_counter()
        if reference in prepare_calls:
            call = prepare_calls.pop(reference)
            try:
                prepared = ray.get(reference)
                broker.complete_prepare(call.lease.lease_id, prepared, now_s=now_s)
            except Exception:
                broker.fail_prepare(call.lease.lease_id, requeue=False)
                raise
            finally:
                available_cpu_actors.add(call.actor_index)
            prepared_payloads[prepared.block_id] = call.payload_ref
            progressed = True
            record_peaks()
            continue

        call = model_calls.pop(reference)
        try:
            result = ray.get(reference)
            broker.complete_model(
                call.lease.lease_id,
                output_row_ids=result.doc_ids,
            )
        except Exception:
            broker.fail_model(call.lease.lease_id, requeue=False)
            raise
        finally:
            available_gpu_actors.add(call.actor_index)
        audit.add_result(result)
        completion_wall_s = now_s - call.source_admitted_at_s
        batch_completion_wall_s.append(completion_wall_s)
        batch_actor_service_s.append(result.service_s)
        telemetry = result.telemetry
        batch_preprocess_s.append(telemetry.preprocess_s)
        batch_unattributed_wait_s.append(
            max(0.0, completion_wall_s - telemetry.preprocess_s - result.service_s)
        )
        batch_host_copy_s.append(telemetry.host_copy_s)
        if telemetry.h2d_s > 0:
            batch_h2d_s.append(telemetry.h2d_s)
        if telemetry.forward_s > 0:
            batch_forward_s.append(telemetry.forward_s)
        if telemetry.d2h_s > 0:
            batch_d2h_s.append(telemetry.d2h_s)
        encoded_bytes += telemetry.encoded_bytes
        input_tensor_bytes += telemetry.input_tensor_bytes
        device_input_bytes += telemetry.device_input_bytes
        output_bytes += telemetry.output_bytes
        if first_output_s is None:
            first_output_s = time.perf_counter() - started
        progressed = True
        record_peaks()

    total_s = time.perf_counter() - started
    final_snapshot = broker.snapshot()
    if final_snapshot.completed != submitted_batches or not broker.is_drained():
        raise RuntimeError("HSE broker did not drain exactly once")
    engine_stats = json.dumps(
        {
            "broker": "bounded_stage_broker_v1",
            "encoded_bytes_peak": encoded_bytes_peak,
            "execution_mode": "hse_static",
            "model_inflight_peak": model_inflight_peak,
            "prepare_inflight_peak": prepare_inflight_peak,
            "ready_bytes_peak": ready_bytes_peak,
            "ready_work_limit": limits.ready_work,
        },
        sort_keys=True,
    )
    return ExecutionResult(
        total_s=total_s,
        first_output_s=first_output_s or total_s,
        audit=audit.finish(),
        batch_completion_wall_s=tuple(batch_completion_wall_s),
        batch_actor_service_s=tuple(batch_actor_service_s),
        batch_unattributed_wait_s=tuple(batch_unattributed_wait_s),
        batch_preprocess_s=tuple(batch_preprocess_s),
        batch_host_copy_s=tuple(batch_host_copy_s),
        batch_h2d_s=tuple(batch_h2d_s),
        batch_forward_s=tuple(batch_forward_s),
        batch_d2h_s=tuple(batch_d2h_s),
        batch_source_next_s=tuple(batch_source_next_s),
        batch_driver_materialize_s=tuple(batch_driver_materialize_s),
        batch_submit_s=tuple(batch_submit_s),
        batch_prepare_queue_s=tuple(batch_prepare_queue_s),
        batch_ready_residence_s=tuple(batch_ready_residence_s),
        encoded_bytes=encoded_bytes,
        input_tensor_bytes=input_tensor_bytes,
        device_input_bytes=device_input_bytes,
        output_bytes=output_bytes,
        submitted_batches=submitted_batches,
        pending_batches_peak=active_blocks_peak,
        encoded_bytes_peak=encoded_bytes_peak,
        ready_bytes_peak=ready_bytes_peak,
        prepare_inflight_peak=prepare_inflight_peak,
        model_inflight_peak=model_inflight_peak,
        execution_mode="hse_static",
        engine_stats=engine_stats,
    )
