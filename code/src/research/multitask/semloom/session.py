"""Opt-in shared-compute ceiling policy over SemLoom's basic fixed Job grants."""

from dataclasses import replace
from ....scheduling.core.session_jobs import equal_share_job_budget


def shared_compute_job_budget(engine):
    """Reserve independent storage; eligible Jobs share the existing compute pool.

    These are ceilings, not extra compute grants. SessionCapacity still checks
    the global records before every dispatch; unknown remote work stays charged.
    """
    budget = equal_share_job_budget(engine)
    limits = engine.capacity.limits
    requests = limits.active_requests
    if (budget.held_tasks < requests
            or budget.input_bytes < requests * limits.item_input_bytes
            or budget.result_bytes < requests * limits.item_result_bytes):
        raise ValueError("each shared-compute Job needs storage for full request capacity")
    return replace(budget, active_requests=requests, active_work=limits.active_work)
