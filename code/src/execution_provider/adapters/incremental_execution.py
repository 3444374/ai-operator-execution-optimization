"""Hold the caller-owned Core, task preparation and service cleanup hooks."""

from dataclasses import dataclass, replace
from typing import Callable

from ...scheduling.core.session import SessionEngine
from ...scheduling.core.session_jobs import equal_share_job_budget
from ...scheduling.core.session_contract import OfferedTask


@dataclass(frozen=True)
class IncrementalExecution:
    """One owned core and its task/transport hooks; sharing across connections is not implied."""

    engine: SessionEngine
    prepare_task: Callable[[object, int], OfferedTask]
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

    def open_query_job(self, label, flow_count):
        """Partition one grant, so dependent flows cannot monopolize each other's storage."""
        if type(flow_count) is not int or flow_count < 1:
            raise ValueError("invalid query flow count")
        budget = self.allocate_job(self.engine)
        base = self.engine.capacity.limits
        held = budget.held_tasks // flow_count
        inputs, results = budget.input_bytes // flow_count, budget.result_bytes // flow_count
        if held < 1 or inputs < base.item_input_bytes or results < base.item_result_bytes:
            raise ValueError("query budget cannot reserve one request and result per flow")
        # More operators change the partition, never the total Job grant or compute share.
        budget = replace(budget, max_sessions=flow_count)
        limits = replace(
            base,
            held_tasks=held,
            input_bytes=inputs,
            result_bytes=results,
            active_requests=budget.active_requests,
            active_work=budget.active_work,
            offer_tasks=held,
        )
        return self.engine.register_job(label, budget), limits, budget
