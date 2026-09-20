"""Incremental image stages over borrowed synchronous Ray actors.

The core owns row admission and result leases. This adapter owns encoded and
prepared blocks until an observed remote terminal, with no retries or actor
termination on query cancellation. All entry points run on the engine owner.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from ...execution_provider.semantic_image import SemanticImagePlan
from ...scheduling.core.session_contract import (
    Acceptance, BackendTask, Submission, TaskKey, Terminal, Uncertain,
)
from ...scheduling.runtime.stage_broker import BoundedStageBroker, StageBrokerLimits
from .contracts import EmbeddingSemantics, ImageEmbeddingResult
from .staged import build_encoded_image_block_descriptor


@dataclass
class _Call:
    task: BackendTask
    handle: str
    block_id: str
    stage: str = "encoded"
    lease: object = None
    reference: object = None
    payload_ref: object = None
    actor_index: int | None = None
    cancelled: bool = False
    uncertain: bool = False
    reported_uncertain: bool = False
    barrier: object = None
    outcome: Terminal | None = None


class RayImageBackend:
    """Bounded CPU prepare -> ready tensor -> GPU model adapter.

    Actors must be synchronous, serial, have no retries/restarts, and implement
    ``ready`` after the work method on the same actor handle. A failed Ray get is
    not sufficient to free resources: a successful subsequent ready call proves
    the previous method has left that actor. An unreachable actor stays unknown.
    The owner stops intake and drains this adapter before destroying its pool.
    """

    def __init__(
        self, *, plan: SemanticImagePlan, worker_pool, limits: StageBrokerLimits,
        max_tasks: int, ray_api=None, observer=None,
        expected_semantics: EmbeddingSemantics | None = None,
    ) -> None:
        if type(max_tasks) is not int or max_tasks < 1:
            raise ValueError("image backend task capacity must be positive")
        if not worker_pool.preprocessors or not worker_pool.gpu_actors:
            raise ValueError("image backend requires prepared CPU and GPU actors")
        if (limits.prepare_inflight > len(worker_pool.preprocessors)
                or limits.model_inflight > len(worker_pool.gpu_actors)):
            raise ValueError("stage concurrency exceeds the borrowed actor pool")
        if limits.ready_bytes < plan.prepared_bytes or limits.ready_work < plan.input_size ** 2:
            raise ValueError("one image cannot fit prepared capacity")
        if ray_api is None:
            import ray as ray_api
        self.plan, self._pool, self._ray = plan, worker_pool, ray_api
        self._broker = BoundedStageBroker(limits)
        self._maximum, self._observer = max_tasks, observer
        self._calls: dict[TaskKey, _Call] = {}
        self._by_block: dict[str, _Call] = {}
        self._free_cpu = set(range(len(worker_pool.preprocessors)))
        self._free_gpu = set(range(len(worker_pool.gpu_actors)))
        self._ordinal = 0
        self._closed = False
        self._semantics = expected_semantics or EmbeddingSemantics(
            model_revision=plan.model_revision,
            processor_revision=plan.processor_revision, dimension=plan.dimension,
        )

    def _observe(self, event: str, call: _Call) -> None:
        if self._observer is not None:
            self._observer(event, call.task.key, self._broker.snapshot())

    def try_submit(self, task: BackendTask, endpoint: str) -> Submission:
        if self._closed or len(self._calls) >= self._maximum or task.key in self._calls:
            return Submission(Acceptance.NOT_ACCEPTED)
        if (task.spec.capability != self.plan.digest or task.spec.operator != "ai_embed"
                or task.spec.work_unit != "pixels" or endpoint != "image"):
            raise ValueError("image backend capability or endpoint mismatch")
        self.plan.validate_input(task.task.payload)
        if (task.task.estimated_work != self.plan.input_size ** 2
                or task.task.max_result_bytes < self.plan.result_bytes):
            raise ValueError("image work or output reservation mismatch")
        identity = f"{task.key.session_id}:{task.key.sequence}:{self._ordinal}"
        descriptor = build_encoded_image_block_descriptor(
            job_id=task.spec.job_id, ordered_sequence=self._ordinal,
            row_ids=(identity,), encoded_images=[task.task.payload],
            model_revision=self.plan.model_revision,
            processor_revision=self.plan.processor_revision,
            model_dtype=self.plan.dtype, created_at_s=time.monotonic(),
            input_size=self.plan.input_size, embedding_dimension=self.plan.dimension,
            decoder="clip_rgb_fp32_v1",
        )
        if descriptor.physical_bytes > self._broker.limits.encoded_bytes:
            raise ValueError("one image cannot fit encoded capacity")
        if not self._broker.can_accept_encoded(descriptor):
            return Submission(Acceptance.NOT_ACCEPTED)
        self._broker.enqueue_encoded(descriptor)
        call = _Call(task, identity, descriptor.block_id)
        self._calls[task.key] = self._by_block[descriptor.block_id] = call
        self._ordinal += 1
        self._observe("accepted", call)
        return Submission(Acceptance.ACCEPTED, call.handle)

    def _finish(self, call: _Call, status: str, result: bytes = b"") -> None:
        call.outcome = Terminal(call.task.key, call.handle, status, result)
        self._observe("remote_terminal" if call.lease else "local_terminal", call)
        call.reference = call.payload_ref = call.barrier = None
        self._observe("object_refs_released", call)

    def _release_actor(self, call: _Call) -> None:
        available = self._free_cpu if call.stage == "preparing" else self._free_gpu
        available.add(call.actor_index)
        call.actor_index = None

    def _fail_finished(self, call: _Call) -> None:
        if call.stage == "preparing":
            self._broker.fail_prepare(call.lease.lease_id, requeue=False)
        else:
            self._broker.fail_model(call.lease.lease_id, requeue=False)
        self._release_actor(call)
        self._finish(call, "cancelled" if call.cancelled else "failed")

    def _mark_unknown(self, call: _Call) -> None:
        call.uncertain = True
        actors = self._pool.preprocessors if call.stage == "preparing" else self._pool.gpu_actors
        try:
            call.barrier = actors[call.actor_index].ready.remote()
        except Exception:
            # Keep the actor, lease and references charged when no confirmation
            # can be requested. A later successful actor terminal can settle it.
            call.barrier = None
        self._observe("remote_unknown", call)

    def _complete(self, call: _Call) -> None:
        try:
            result = self._ray.get(call.reference, timeout=0)
        except Exception:
            self._mark_unknown(call)
            return
        if call.cancelled:
            self._fail_finished(call)
            return
        try:
            if call.stage == "preparing":
                if (result.shape != (1, 3, self.plan.input_size, self.plan.input_size)
                        or result.layout != "NCHW" or result.dtype != "float32"
                        or result.representation != "prepared_fp32_nchw"
                        or result.physical_bytes != self.plan.prepared_bytes):
                    raise ValueError("prepared image does not match its declared representation")
                self._broker.complete_prepare(call.lease.lease_id, result, now_s=time.monotonic())
                self._release_actor(call)
                call.stage, call.lease, call.reference = "ready", None, None
                self._observe("prepared", call)
            else:
                if (type(result) is not ImageEmbeddingResult
                        or result.semantics != self._semantics
                        or result.doc_ids != call.lease.descriptor.row_ids
                        or str(result.embeddings.dtype) != "float32"):
                    raise ValueError("image result identity does not match the leased row")
                packed = self.plan.encode_result(result.embeddings[0])
                self._broker.complete_model(call.lease.lease_id, output_row_ids=result.doc_ids)
                self._release_actor(call)
                self._finish(call, "completed", packed)
        except (ValueError, TypeError, AttributeError, BufferError, IndexError):
            self._fail_finished(call)

    def _submit_stage(self, call: _Call, lease, actor_index: int) -> None:
        call.lease, call.actor_index = lease, actor_index
        call.stage = "preparing" if lease.stage == "prepare" else "modeling"
        try:
            if call.stage == "preparing":
                self._free_cpu.remove(actor_index)
                call.reference, call.payload_ref = (
                    self._pool.preprocessors[actor_index].preprocess_staged.options(num_returns=2)
                    .remote(lease.descriptor, [call.task.task.payload])
                )
            else:
                self._free_gpu.remove(actor_index)
                call.reference = self._pool.gpu_actors[actor_index].embed.remote(call.payload_ref)
            self._observe("stage_submitted", call)
        except Exception:
            # A submission exception can happen after the remote call was sent.
            self._mark_unknown(call)

    def poll(self, handles, max_events: int):
        if type(max_events) is not int or max_events < 1:
            raise ValueError("max_events must be positive")
        registered = set(handles)
        if any((key, call.handle) not in registered for key, call in self._calls.items()):
            raise ValueError("image task has no registered engine owner")
        refs = {
            call.barrier if call.uncertain else call.reference: call
            for call in self._calls.values()
            if (call.barrier if call.uncertain else call.reference) is not None
        }
        if refs:
            ready, _ = self._ray.wait(list(refs), num_returns=min(len(refs), max_events), timeout=0)
            for reference in ready:
                call = refs[reference]
                if call.uncertain:
                    try:
                        self._ray.get(reference, timeout=0)
                    except Exception:
                        call.barrier = None
                    else:
                        self._observe("actor_completion_confirmed", call)
                        self._fail_finished(call)
                else:
                    self._complete(call)
        # Limit each owner tick by event capacity; core ticks continue polling.
        for _ in range(max_events):
            if not self._free_cpu:
                break
            lease = self._broker.lease_prepare(now_s=time.monotonic())
            if lease is None:
                break
            self._submit_stage(self._by_block[lease.descriptor.block_id], lease, min(self._free_cpu))
        for _ in range(max_events):
            if not self._free_gpu:
                break
            lease = self._broker.lease_model(now_s=time.monotonic())
            if lease is None:
                break
            self._submit_stage(self._by_block[lease.descriptor.block_id], lease, min(self._free_gpu))
        events = []
        for key, call in tuple(self._calls.items()):
            if call.outcome is not None:
                events.append(call.outcome)
                self._broker.release_terminal(call.block_id)
                del self._calls[key], self._by_block[call.block_id]
                self._observe("terminal_delivered", call)
            elif call.uncertain and not call.reported_uncertain:
                events.append(Uncertain(key, call.handle, "MODEL_UNAVAILABLE"))
                call.reported_uncertain = True
            if len(events) == max_events:
                break
        return tuple(events)

    def request_cancel(self, key: TaskKey, handle: str | None) -> None:
        call = self._calls.get(key)
        if call is None or call.handle != handle or call.cancelled or call.outcome is not None:
            return
        call.cancelled = True
        self._observe("cancel_requested", call)
        if call.stage in ("encoded", "ready"):
            self._broker.cancel_queued(call.block_id)
            self._finish(call, "cancelled")
        elif call.reference is not None:
            try:
                self._ray.cancel(call.reference, force=False, recursive=False)
                self._observe("cancel_sent", call)
            except Exception:
                self._observe("cancel_send_failed", call)

    def snapshot(self):
        return self._broker.snapshot()

    def close(self) -> bool:
        """Stop intake, request cancellation, and report whether owners drained."""
        self._closed = True
        for key, call in tuple(self._calls.items()):
            self.request_cancel(key, call.handle)
        return not self._calls
