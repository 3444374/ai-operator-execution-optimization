"""Static HSE execution path with a real byte-bounded ready-block broker."""

from __future__ import annotations

import time
import weakref
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
    SemLoomRayWorkerPool,
)
from .staged import build_encoded_image_block_descriptor, image_transform_signature
from .staged_metrics import StagedImageMetrics


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
    preprocessing_identity: dict[str, object] | None = None,
) -> ExecutionResult:
    """Run static HSE without materializing prepared tensors on the driver.

    This path changes only flow ownership relative to ``run_semloom_ray_pipeline``:
    worker pools, image semantics, source, and model backend stay identical.
    The pool is borrowed exclusively for this run. On error the owner must
    dispose it before using another execution path; unconfirmed work blocks
    subsequent HSE runs on the same pool and is retained on the exception.
    GPU slots require distinct Python handle objects. This does not identify
    separately deserialized handles or shared physical GPU assignments.
    This run closes its source iterator when supported. Source closure does
    not confirm actor or device completion.
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
    pool_identity = getattr(worker_pool, "preprocessing_identity", None)
    if preprocessing_identity is None:
        preprocessing_identity = pool_identity
    elif pool_identity is not None and preprocessing_identity != pool_identity:
        raise ValueError("HSE preprocessing identity does not match the worker pool")
    if preprocessing_identity is not None:
        if (not isinstance(preprocessing_identity, dict)
                or not image_transform_signature(**preprocessing_identity).startswith("image-transform-v2:")
                or preprocessing_identity.get("input_size") != input_size
                or preprocessing_identity.get("representation") != "prepared_fp32_nchw"
                or preprocessing_identity.get("layout") != "NCHW"
                or preprocessing_identity.get("dtype") != "float32"):
            raise ValueError("HSE preprocessing identity does not match its ready representation")

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
    metrics = StagedImageMetrics()
    submitted_batches = 0
    started = time.perf_counter()
    batches = iter(source_df.into_batches(batch_size).to_arrow_iter(results_buffer_size=2))

    def submit_ready_models() -> bool:
        progressed = False
        while True:
            if not available_gpu_actors:
                break
            lease = broker.lease_model(now_s=time.perf_counter())
            actor_index = min(available_gpu_actors)
            if lease is None:
                break
            metrics.batch_ready_residence_s.append(
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
            metrics.batch_submit_s.append(submitted_at_s - submit_started)
            model_calls[call.reference] = call
            del call
            progressed = True
            metrics.record_peaks(broker.snapshot())

        return progressed

    execution_error: BaseException | None = None
    try:
        while not source_exhausted or not broker.is_drained():
            progressed = submit_ready_models()

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
                metrics.batch_source_next_s.append(time.perf_counter() - source_next_started)
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
                    preprocessing_identity=preprocessing_identity,
                )
                metrics.batch_driver_materialize_s.append(time.perf_counter() - materialize_started)
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
                metrics.record_peaks(broker.snapshot())

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
                metrics.batch_submit_s.append(time.perf_counter() - submit_started)
                metrics.batch_prepare_queue_s.append(
                    max(0.0, lease.issued_at_s - admitted_at[lease.descriptor.block_id])
                )
                prepare_calls[call.reference] = call
                del call
                metrics.record_peaks(broker.snapshot())

            progressed = submit_ready_models() or progressed

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
                call = prepare_calls[reference]
                prepared = ray.get(reference)
                broker.complete_prepare(call.lease.lease_id, prepared, now_s=now_s)
                available_cpu_actors.add(call.actor_index)
                prepared_payloads[prepared.block_id] = call.payload_ref
                active_calls.pop(call.lease.lease_id)
                prepare_calls.pop(reference)
                del call
                progressed = True
                metrics.record_peaks(broker.snapshot())
                continue

            call = model_calls[reference]
            result = ray.get(reference)
            broker.complete_model(call.lease.lease_id, output_row_ids=result.doc_ids)
            available_gpu_actors.add(call.actor_index)
            completion_wall_s = now_s - call.source_admitted_at_s
            admitted_at.pop(call.lease.descriptor.block_id)
            active_calls.pop(call.lease.lease_id)
            model_calls.pop(reference)
            call.payload_ref = None
            del call
            audit.add_result(result)
            metrics.record_model_result(result, completion_wall_s)
            if metrics.first_output_s is None:
                metrics.first_output_s = time.perf_counter() - started
            progressed = True
            metrics.record_peaks(broker.snapshot())

    except BaseException as error:
        execution_error = error
    finally:
        try:
            close_source = getattr(batches, "close", None)
            if close_source is not None:
                close_source()
        except BaseException as close_error:
            if execution_error is None:
                execution_error = close_error
            else:
                execution_error.add_note(f"HSE source close raised {type(close_error).__name__}")

    if execution_error is not None:
        error = execution_error
        # Queued blocks have no worker owner; leased work needs a serial actor
        # fence, including device completion after a GPU method exception.
        cleanup_failed = False
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
        raise error

    total_s = time.perf_counter() - started
    final_snapshot = broker.snapshot()
    if final_snapshot.completed != submitted_batches or not broker.is_drained():
        raise RuntimeError("HSE broker did not drain exactly once")
    return metrics.finish(
        total_s=total_s,
        submitted_batches=submitted_batches,
        audit=audit,
        ready_work_limit=limits.ready_work,
    )


# Canonical SemLoom name; retain the historical function as an exact alias so
# existing project_ray callers and evidence remain reproducible.
run_semloom_ray_hse_pipeline = run_project_ray_hse_pipeline
