"""Compose existing execution interfaces; wire adapters do not choose policies or work units."""

from dataclasses import dataclass, asdict, replace
from functools import partial
import json
import time
from typing import Callable

from ...planning.work import StageWork, WorkDescriptor
from ...scheduling.core.models import EndpointSnapshot, TopologySnapshot
from ...scheduling.core.session import SessionEngine
from ...scheduling.core.session_jobs import equal_share_job_budget, round_robin_flow
from ...scheduling.core.session_contract import (
    OfferedTask,
    SessionLimits,
    SessionTimeouts,
    TaskInfo,
)
from ...scheduling.core.session_policy import SessionPolicies
from ...scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from ...scheduling.organization.session_window import WorkWindowOrganizer
from ...scheduling.runtime.async_backend import BoundedAsyncBackend
from ...scheduling.submission_control.admission import StaticAdmissionController
from ..completion import CompletionRequest
from ..limits import MAX_INCREMENTAL_TASKS
from ..wire.framing import MAX_FRAME_BYTES
from .async_fixed_model import AsyncFixedModelTransport
from .model_config import MAX_MODEL_RESPONSE_BYTES


@dataclass(frozen=True)
class IncrementalExecution:
    """One owned core and its task/transport hooks; sharing across connections is not implied."""

    engine: SessionEngine
    prepare_task: Callable[[CompletionRequest, int], OfferedTask]
    close: Callable[[], bool]
    drain_timeout_s: float
    work_unit: str = "work_units"
    allocate_job: Callable = equal_share_job_budget

    def open_job(self, label, spec):
        # Resource policy grants a budget; the Engine validates and owns its lifetime.
        budget = self.allocate_job(self.engine)
        job = self.engine.register_job(label, budget)
        try:
            limits = replace(
                self.engine.capacity.limits,
                held_tasks=budget.held_tasks,
                input_bytes=budget.input_bytes,
                result_bytes=budget.result_bytes,
                active_requests=budget.active_requests,
                active_work=budget.active_work,
                offer_tasks=budget.held_tasks,
            )
            return job, self.engine.open(spec, limits, job=job), budget
        except BaseException:
            self.engine.close_job(job)
            raise


def request_count_work(request: CompletionRequest) -> WorkDescriptor:
    return WorkDescriptor((StageWork("model", 1, "work_units"),), "model", "request-count")


def prepare_map_task(request, sequence, *, describe_work=request_count_work):
    # Policy describes demand/locality; the canonical model input remains unchanged.
    payload = json.dumps(
        {
            "model": request.model_id,
            "messages": list(request.canonical_messages),
            **request.generation_constraints,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    work = describe_work(request)
    return OfferedTask(
        sequence,
        payload,
        work.primary.units,
        MAX_MODEL_RESPONSE_BYTES,
        info=TaskInfo("pg-map", sequence, "generate", work),
    )


def build_fixed_model_execution(
    config,
    *,
    execute=None,
    observer=None,
    max_tasks=1,
    max_active_requests=None,
    input_bytes=None,
    result_bytes=None,
    policies=None,
    describe_work=request_count_work,
    active_work=None,
    work_unit="work_units",
    timeouts=None,
    max_jobs=1,
    allocate_job=equal_share_job_budget,
    choose_flow=round_robin_flow,
):
    """Default single-endpoint assembly; supplied policies/work reuse the same core and transport."""
    if type(max_tasks) is not int or not 1 <= max_tasks <= MAX_INCREMENTAL_TASKS:
        raise ValueError("invalid incremental task capacity")
    if max_active_requests is None:
        max_active_requests = max_tasks
    if type(max_active_requests) is not int or max_active_requests < 1:
        raise ValueError("invalid incremental request capacity")
    drain_timeout_s = config.timeout_ms / 1000 + 1
    limits = SessionLimits(
        held_tasks=max_tasks,
        input_bytes=max_tasks * MAX_FRAME_BYTES if input_bytes is None else input_bytes,
        result_bytes=max_tasks * MAX_MODEL_RESPONSE_BYTES if result_bytes is None else result_bytes,
        active_requests=max_active_requests,
        active_work=max_active_requests if active_work is None else active_work,
        offer_tasks=max_tasks,
        item_input_bytes=MAX_FRAME_BYTES,
        item_result_bytes=MAX_MODEL_RESPONSE_BYTES,
        metadata_bytes=1024,
        step_actions=8,
        wait_timeout_s=drain_timeout_s,
        poll_interval_s=0.01,
        timeouts=SessionTimeouts(backend_s=drain_timeout_s) if timeouts is None else timeouts,
    )
    if policies is None:
        topology = TopologySnapshot(
            (
                EndpointSnapshot(
                    "model",
                    config.endpoint_url,
                    "default",
                    "0",
                    True,
                    0,
                    0,
                    0.0,
                    1.0,
                ),
            ),
            time.monotonic(),
        )
        policies = SessionPolicies(
            StaticAdmissionController(max_active_requests),
            RoundRobinEndpointRouter(),
            topology,
            "default",
            organize=WorkWindowOrganizer(
                max_tasks, max_tasks if active_work is None else active_work
            ),
        )
    transport = AsyncFixedModelTransport(config, max_active_requests, observer)

    def observe(event, key):
        if observer:
            observer({"event": event, "key": asdict(key), "usage": asdict(engine.capacity.usage())})

    backend = BoundedAsyncBackend(
        execute or transport.execute,
        max_tasks=max_active_requests,
        notify=lambda: engine.wake.notify(),
        finalize=transport.close,
        isolate_failures=True,
    )
    try:
        engine = SessionEngine(
            limits, backend, policies, sink=observe, max_jobs=max_jobs, choose_flow=choose_flow
        )
        allocate_job(engine)  # Reject impossible resource policies before accepting sockets.
    except BaseException:
        backend.close()
        raise
    return IncrementalExecution(
        engine,
        partial(prepare_map_task, describe_work=describe_work),
        backend.close,
        drain_timeout_s,
        work_unit,
        allocate_job,
    )
