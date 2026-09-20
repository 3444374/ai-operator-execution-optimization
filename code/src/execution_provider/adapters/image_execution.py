"""Assemble typed image stages with the existing shared session engine."""

from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass

from ...modalities.image.operator import (
    ImageComputationUnknown, ImageModelStage, ImagePrepareStage, SynchronousImageReference,
    build_image_worker_pool,
)
from ...modalities.image.incremental_backend import RayImageBackend
from ...modalities.image.contracts import build_image_work_descriptor
from ...scheduling.core.models import EndpointSnapshot, TopologySnapshot
from ...scheduling.core.session import SessionEngine
from ...scheduling.core.session_contract import OfferedTask, SessionLimits, SessionTimeouts, TaskInfo
from ...scheduling.core.session_policy import SessionPolicies
from ...scheduling.core.session_jobs import shared_compute_job_budget
from ...scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from ...scheduling.submission_control.admission import StaticAdmissionController
from ...scheduling.runtime.async_backend import BoundedAsyncBackend
from ...scheduling.runtime.stage_broker import StageBrokerLimits
from ..semantic_image import MAX_IMAGE_BYTES, SemanticImagePlan
from .incremental_execution import IncrementalExecution


@dataclass(frozen=True)
class ImageServiceConfig:
    plan: SemanticImagePlan
    mode: str
    stage_limits: StageBrokerLimits
    cpu_workers: int
    gpu_workers: int
    cpu_threads: int
    timeout_ms: int

    def __post_init__(self):
        if self.mode not in ("reference", "staged"):
            raise ValueError("invalid image service mode")
        if type(self.plan) is not SemanticImagePlan or type(self.stage_limits) is not StageBrokerLimits:
            raise ValueError("invalid typed image configuration")
        for value in (self.cpu_workers, self.gpu_workers, self.cpu_threads, self.timeout_ms):
            if type(value) is not int or value < 1:
                raise ValueError("image service capacities must be positive integers")
        if self.mode == "reference" and (self.cpu_workers != 1 or self.gpu_workers != 1):
            raise ValueError("the synchronous reference uses one CPU and one model instance")
        if self.mode == "staged" and (
            self.stage_limits.encoded_bytes < MAX_IMAGE_BYTES
            or self.stage_limits.ready_bytes < self.plan.prepared_bytes
            or self.stage_limits.ready_work < self.plan.input_size ** 2
            or self.stage_limits.prepare_inflight > self.cpu_workers
            or self.stage_limits.model_inflight > self.gpu_workers
        ):
            raise ValueError("image stage capacity must fit one legal image and the declared workers")

    @property
    def model_id(self):
        return self.plan.model_id

    @classmethod
    def from_record(cls, record):
        fields = set(cls.__dataclass_fields__)
        if type(record) is not dict or set(record) != fields:
            raise ValueError("image configuration fields do not match the schema")
        return cls(**{**record, "plan": SemanticImagePlan.from_record(record["plan"]),
                      "stage_limits": StageBrokerLimits(**record["stage_limits"])})


def build_image_execution(config, *, max_tasks, max_active_requests, input_bytes, result_bytes,
                          max_jobs, observer=None, execute=None, worker_pool=None,
                          ray_api=None, reference=None):
    if execute is not None:
        raise ValueError("image execution requires typed stages, not a chat callback")
    plan = config.plan
    if (type(max_jobs) is not int or max_jobs < 1 or max_tasks < max_jobs
            or input_bytes < max_jobs * MAX_IMAGE_BYTES or result_bytes < max_jobs * plan.result_bytes):
        raise ValueError("each image Job must fit one input and result before workers start")
    if config.mode == "reference" and max_active_requests != 1:
        raise ValueError("synchronous image reference requires one active request")
    timeout = config.timeout_ms / 1000
    limits = SessionLimits(
        held_tasks=max_tasks, input_bytes=input_bytes, result_bytes=result_bytes,
        active_requests=max_active_requests, active_work=max_active_requests * plan.input_size ** 2,
        offer_tasks=max_tasks, item_input_bytes=MAX_IMAGE_BYTES, item_result_bytes=plan.result_bytes,
        metadata_bytes=1024, step_actions=8, wait_timeout_s=timeout, poll_interval_s=0.01,
        timeouts=SessionTimeouts(backend_s=timeout),
    )
    topology = TopologySnapshot((EndpointSnapshot(
        "image", "local-image-workers", "image", "0", True, 0, 0, 0.0, 1.0),), time.monotonic())
    policies = SessionPolicies(StaticAdmissionController(max_active_requests),
                               RoundRobinEndpointRouter(), topology, "image")
    owned_pool = None
    if config.mode == "reference":
        if reference is None:
            from ...modalities.image.clip import configure_torch_thread_pools
            configure_torch_thread_pools(config.cpu_threads, 1)
            reference = SynchronousImageReference(plan, ImagePrepareStage(plan), ImageModelStage(plan))

        async def run_reference(task, endpoint):
            try:
                return await asyncio.to_thread(reference.embed, task.task.payload)
            except ImageComputationUnknown:
                raise
            except Exception:
                # Synchronous callback has returned, so local work is finished.
                # Empty bytes cannot be a legal vector; the typed consumer raises
                # the query error without returning a fabricated embedding.
                return b""

        backend = BoundedAsyncBackend(run_reference, max_tasks=1,
                                      notify=lambda: engine.wake.notify(), isolate_failures=True)
    else:
        if worker_pool is None:
            worker_pool = owned_pool = build_image_worker_pool(
                plan, cpu_workers=config.cpu_workers, gpu_workers=config.gpu_workers,
                cpu_threads=config.cpu_threads, startup_timeout_s=timeout, ray_api=ray_api)
        backend = RayImageBackend(
            plan=plan, worker_pool=worker_pool, limits=config.stage_limits,
            max_tasks=max_active_requests, ray_api=ray_api,
            observer=(lambda event, key, snapshot: observer({
                "event": event, "key": asdict(key), "image_stages": asdict(snapshot)})) if observer else None)

    def observe(event, key):
        if observer:
            observer({"event": event, "key": asdict(key), "usage": asdict(engine.capacity.usage())})

    def close():
        nonlocal owned_pool
        if not backend.close():
            return False
        if owned_pool is not None:
            # Only the service owner, after every query task settled, ends actors.
            from ...modalities.image.execution import stop_semloom_ray_worker_pool
            stop_semloom_ray_worker_pool(owned_pool)
            owned_pool = None
        return True

    try:
        engine = SessionEngine(limits, backend, policies, sink=observe, max_jobs=max_jobs)
        shared_compute_job_budget(engine)
    except BaseException:
        close()
        raise

    def prepare_task(request, sequence):
        plan.validate_input(request.encoded)
        if plan.payload_digest(request.encoded) != request.payload_digest:
            raise ValueError("image payload identity changed before core admission")
        work = build_image_work_descriptor(
            row_count=1, encoded_bytes=len(request.encoded), model_revision=plan.model_revision,
            processor_revision=plan.processor_revision, dtype=plan.dtype,
            input_size=plan.input_size, embedding_dimension=plan.dimension)
        return OfferedTask(sequence, request.encoded, plan.input_size ** 2, plan.result_bytes,
                           info=TaskInfo("pg-image", sequence, "image-pipeline", work))

    return IncrementalExecution(engine, prepare_task, close, timeout + 1, "pixels", shared_compute_job_budget)
