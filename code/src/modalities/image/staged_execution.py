"""Static HSE execution path with a real byte-bounded ready-block broker."""

from __future__ import annotations

import json
import time
import weakref
from dataclasses import dataclass

from ...scheduling.runtime.stage_dispatch import StageActiveWork, StageModelDispatcher
from ...scheduling.runtime.stage_broker import (
    BoundedStageBroker,
    StageBrokerLimits,
    StageLease,
)
from .execution import (
    EmbeddingAudit,
    EmbeddingCapture,
    ExecutionResult,
    SemLoomRayWorkerPool,
)
from .staged import build_encoded_image_block_descriptor
from .hse_dispatch import bind_hse_dispatcher, image_model_routing_request


@dataclass
class _PrepareCall:
    lease: StageLease
    actor_index: int
    payload_ref: object = None
    reference: object = None


@dataclass
class _ModelCall:
    lease: StageLease
    source_admitted_at_s: float
    actor_index: int
    payload_ref: object
    reference: object = None


@dataclass
class _UnconfirmedRun:
    broker: BoundedStageBroker
    calls: tuple[_PrepareCall | _ModelCall, ...]


# Borrowed pools with unknown work must not start another HSE run. Values keep
# leases/refs, but never the pool, so disposing the owner also removes this record.
_unconfirmed_runs: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
_ABORT_CONFIRMATION_TIMEOUT_S = 30.0
_CAPACITY_WAIT_POLL_S = 1.0


def _settle_aborted_calls(ray, worker_pool, broker, calls) -> None:
    """Fence serial actors once; a failed/late fence leaves its lease charged."""
    barriers = {}
    for call in tuple(calls.values()):
        try:
            # An interrupt can arrive after a broker transition but before the
            # caller removes its old lease record. That work is already local
            # or terminal, so it must not be failed through the stale lease.
            state = broker.state_of(call.lease.descriptor.block_id)
            if state in ("encoded", "ready", "completed", "failed"):
                if state in ("encoded", "ready"):
                    broker.cancel_queued(call.lease.descriptor.block_id)
                calls.pop(call.lease.lease_id)
                call.reference = call.payload_ref = None
                continue
            if call.lease.stage == "prepare":
                actor = worker_pool.preprocessors[call.actor_index]
                reference = actor.ready.remote()
            else:
                actor = worker_pool.gpu_actors[call.actor_index]
                reference = actor.synchronize.remote()
            barriers[reference] = call
        except BaseException:
            continue
    if not barriers:
        return
    try:
        ready, _ = ray.wait(list(barriers), num_returns=len(barriers),
                            timeout=_ABORT_CONFIRMATION_TIMEOUT_S)
    except BaseException:
        return
    for reference in ready:
        try:
            ray.get(reference, timeout=0)
        except BaseException:
            continue
        call = barriers[reference]
        try:
            if call.lease.stage == "prepare":
                broker.fail_prepare(call.lease.lease_id, requeue=False)
            else:
                broker.fail_model(call.lease.lease_id, requeue=False)
            calls.pop(call.lease.lease_id)
            call.reference = call.payload_ref = None
        except BaseException:
            continue


def run_project_ray_hse_pipeline(
    source_df,
    *,
    worker_pool: SemLoomRayWorkerPool,
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
    model_dispatch: StageModelDispatcher | None = None,
) -> ExecutionResult:
    """Run static HSE without materializing prepared tensors on the driver.

    This path changes only flow ownership relative to ``run_semloom_ray_pipeline``:
    worker pools, image semantics, source, and model backend stay identical.
    The pool is borrowed exclusively for this run. On error the owner must
    dispose it before using another execution path; unconfirmed work blocks
    subsequent HSE runs on the same pool and is retained on the exception.
    GPU slots require distinct Python handle objects. This does not identify
    separately deserialized handles or shared physical GPU assignments.
    model_dispatch is opt-in; None preserves static min-free actor selection.
    """
    if worker_pool in _unconfirmed_runs:
        raise RuntimeError("HSE worker pool has unconfirmed work; dispose its actors")
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
    if len({id(actor) for actor in worker_pool.gpu_actors}) != len(worker_pool.gpu_actors):
        raise ValueError("HSE GPU actor handles must be distinct Python objects")
    if limits.prepare_inflight > len(worker_pool.preprocessors):
        raise ValueError("prepare inflight cannot exceed the prestarted CPU actor count")
    if limits.model_inflight > len(worker_pool.gpu_actors):
        raise ValueError("model inflight cannot exceed the prestarted GPU actor count")
    if encoded_block_bytes_upper_bound > limits.encoded_bytes:
        raise BufferError(
            "one worst-case HSE source block exceeds encoded_bytes; reduce "
            "batch_size or increase the explicit byte limit"
        )
    if model_dispatch is not None:
        bind_hse_dispatcher(model_dispatch, worker_pool)

    import ray

    broker = BoundedStageBroker(limits)
    audit = EmbeddingAudit(
        expected_doc_ids=expected_doc_ids,
        dimension=embedding_dimension,
        capture=embedding_capture,
    )
    prepare_calls: dict[object, _PrepareCall] = {}
    model_calls: dict[object, _ModelCall] = {}
    active_calls: dict[str, _PrepareCall | _ModelCall] = {}
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

    try:
        while not source_exhausted or not broker.is_drained():
            progressed = False
            capacity_retry_at_s = None

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
                del record_batch
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
                        "HSE source block exceeded the immutable source-derived byte "
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
                del encoded
                admitted_at[descriptor.block_id] = time.perf_counter()
                source_sequence += 1
                submitted_batches += 1
                progressed = True
                record_peaks()

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
                call = _PrepareCall(lease=lease, actor_index=actor_index)
                active_calls[lease.lease_id] = call
                call.reference, call.payload_ref = preprocessor.preprocess_staged.options(
                    num_returns=2
                ).remote(lease.descriptor, encoded)
                del encoded
                batch_submit_s.append(time.perf_counter() - submit_started)
                batch_prepare_queue_s.append(
                    max(0.0, lease.issued_at_s - admitted_at[lease.descriptor.block_id])
                )
                prepare_calls[call.reference] = call
                del call
                record_peaks()

            while True:
                if not available_gpu_actors:
                    break
                if model_dispatch is None:
                    lease = broker.lease_model(now_s=time.perf_counter())
                    actor_index = min(available_gpu_actors)
                else:
                    candidate = broker.peek_model()
                    if candidate is None:
                        break
                    decision = model_dispatch.choose(
                        candidate, image_model_routing_request(candidate),
                        available_actor_indices=available_gpu_actors,
                        active=tuple(StageActiveWork(call.actor_index, call.lease.descriptor.model_work_units)
                                     for call in active_calls.values() if isinstance(call, _ModelCall)),
                        now_s=time.perf_counter(),
                    )
                    if decision.actor_index is None:
                        capacity_retry_at_s = decision.retry_at_s
                        break
                    actor_index = decision.actor_index
                    lease = broker.lease_model(now_s=time.perf_counter(), block_id=candidate.block_id)
                if lease is None:
                    break
                batch_ready_residence_s.append(
                    max(
                        0.0,
                        lease.issued_at_s
                        - (lease.descriptor.ready_at_s or lease.issued_at_s),
                    )
                )
                available_gpu_actors.remove(actor_index)
                gpu_actor = worker_pool.gpu_actors[actor_index]
                submit_started = time.perf_counter()
                call = _ModelCall(
                    lease=lease,
                    source_admitted_at_s=admitted_at[lease.descriptor.block_id],
                    actor_index=actor_index,
                    payload_ref=prepared_payloads.pop(lease.descriptor.block_id),
                )
                active_calls[lease.lease_id] = call
                call.reference = gpu_actor.embed.remote(call.payload_ref)
                submitted_at_s = time.perf_counter()
                batch_submit_s.append(submitted_at_s - submit_started)
                model_calls[call.reference] = call
                del call
                progressed = True
                record_peaks()

            pending_refs = tuple(prepare_calls) + tuple(model_calls)
            if not pending_refs:
                if source_exhausted and broker.is_drained():
                    break
                if not progressed:
                    if capacity_retry_at_s is not None:
                        time.sleep(min(_CAPACITY_WAIT_POLL_S,
                                       max(0.0, capacity_retry_at_s - time.perf_counter())))
                        continue
                    raise RuntimeError("HSE made no progress with unfinished work")
                continue

            if capacity_retry_at_s is None:
                ready_refs, _ = ray.wait(list(pending_refs), num_returns=1)
            else:
                ready_refs, _ = ray.wait(list(pending_refs), num_returns=1,
                    timeout=min(_CAPACITY_WAIT_POLL_S,
                                max(0.0, capacity_retry_at_s - time.perf_counter())))
                if not ready_refs:
                    continue
            reference = ready_refs[0]
            now_s = time.perf_counter()
            if reference in prepare_calls:
                call = prepare_calls[reference]
                prepared = ray.get(reference)
                broker.complete_prepare(call.lease.lease_id, prepared, now_s=now_s)
                available_cpu_actors.add(call.actor_index)
                prepared_payloads[prepared.block_id] = call.payload_ref
                active_calls.pop(call.lease.lease_id)
                prepare_calls.pop(reference)
                del call
                progressed = True
                record_peaks()
                continue

            call = model_calls[reference]
            result = ray.get(reference)
            broker.complete_model(call.lease.lease_id, output_row_ids=result.doc_ids)
            if model_dispatch is not None:
                completed_actor_index, completed_descriptor = call.actor_index, call.lease.descriptor
            available_gpu_actors.add(call.actor_index)
            completion_wall_s = now_s - call.source_admitted_at_s
            admitted_at.pop(call.lease.descriptor.block_id)
            active_calls.pop(call.lease.lease_id)
            model_calls.pop(reference)
            call.payload_ref = None
            del call
            audit.add_result(result)
            if model_dispatch is not None:
                model_dispatch.observe_completion(completed_actor_index, completed_descriptor,
                                                   service_s=result.service_s, now_s=now_s)
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

    except BaseException as error:
        # Queued blocks have no worker owner; leased work needs a serial actor
        # fence, including device completion after a GPU method exception.
        cleanup_failed = False
        if model_dispatch is not None:
            try:
                for call in tuple(active_calls.values()):
                    if isinstance(call, _ModelCall):
                        model_dispatch.invalidate_actor(call.actor_index)
            except BaseException as feedback_error:
                # Advisory feedback must never prevent the authoritative
                # actor fences or replace the execution's original error.
                error.add_note(f"HSE feedback cleanup raised {type(feedback_error).__name__}")
        try:
            _settle_aborted_calls(ray, worker_pool, broker, active_calls)
            for block_id in (*encoded_payloads, *prepared_payloads):
                if broker.state_of(block_id) in ("encoded", "ready"):
                    broker.cancel_queued(block_id)
            encoded_payloads.clear()
            prepared_payloads.clear()
            prepare_calls.clear()
            model_calls.clear()
        except BaseException as cleanup_error:
            cleanup_failed = True
            error.add_note(f"HSE cleanup raised {type(cleanup_error).__name__}")
        if cleanup_failed or active_calls or not broker.is_drained():
            retained = _UnconfirmedRun(broker, tuple(active_calls.values()))
            _unconfirmed_runs[worker_pool] = retained
            error.hse_unconfirmed_work = retained
            error.add_note(
                f"HSE cleanup unconfirmed: {broker.snapshot().active_blocks} active "
                f"block(s), {len(active_calls)} retained call(s); "
                "leases and refs remain held. Dispose the borrowed worker pool."
            )
        raise

    total_s = time.perf_counter() - started
    final_snapshot = broker.snapshot()
    if final_snapshot.completed != submitted_batches or not broker.is_drained():
        raise RuntimeError("HSE broker did not drain exactly once")
    engine_stats = json.dumps(
        {
            "broker": "bounded_stage_broker_v1",
            "encoded_bytes_peak": encoded_bytes_peak,
            "execution_mode": "hse_static" if model_dispatch is None else "hse_device_dispatch",
            "model_inflight_peak": model_inflight_peak,
            "prepare_inflight_peak": prepare_inflight_peak,
            "ready_bytes_peak": ready_bytes_peak,
            "ready_work_limit": limits.ready_work,
            **({"model_dispatch": model_dispatch.snapshot()} if model_dispatch is not None else {}),
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
        execution_mode="hse_static" if model_dispatch is None else "hse_device_dispatch",
        engine_stats=engine_stats,
    )


# Canonical SemLoom name; retain the historical function as an exact alias so
# existing project_ray callers and evidence remain reproducible.
run_semloom_ray_hse_pipeline = run_project_ray_hse_pipeline
