"""Declared Core storage, Method residency and IPC capacity for records owners.

Each existing resource owner consumes its own typed limits. Only the legacy
configuration translates one R into all three allocations; explicit budgets
retain independent counts and bytes. No hardware observation or scheduling.
"""
from __future__ import annotations

from dataclasses import dataclass

from src.scheduling.core.session_contract import SessionLimits, SessionTimeouts


@dataclass(frozen=True)
class RecordsCapacityBudget:
    """Explicit finite logical grants from deployment, never inferred free memory."""
    held_tasks: int
    input_bytes: int
    result_bytes: int
    active_requests: int
    method_runs: int
    method_bytes: int

    def __post_init__(self):
        for name, value in vars(self).items():
            if type(value) is not int or value <= 0:
                raise ValueError('resource budget '+name+' must be a positive integer')



@dataclass(frozen=True)
class CoreRecordsCapacity:
    """Per-row logical storage, bounded actions, and poll interval; not RSS caps."""

    item_input_bytes: int = 2 * 1024 * 1024
    item_result_bytes: int = 2 * 1024 * 1024
    metadata_bytes: int = 2048
    step_actions: int = 16
    poll_interval_s: float = 0.01
    budget: RecordsCapacityBudget | None = None

    def __post_init__(self):
        for name in ("item_input_bytes", "item_result_bytes", "metadata_bytes", "step_actions"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(name + " must be a positive integer")
        import math
        if (type(self.poll_interval_s) not in (int, float)
                or not math.isfinite(self.poll_interval_s) or self.poll_interval_s <= 0):
            raise ValueError("poll_interval_s must be finite and positive")
        if type(self.budget) is dict:
            object.__setattr__(self, 'budget', RecordsCapacityBudget(**self.budget))
        if self.budget is not None:
            if type(self.budget) is not RecordsCapacityBudget:
                raise ValueError('budget must be a complete RecordsCapacityBudget or object')
            if (self.budget.input_bytes < self.item_input_bytes
                    or self.budget.result_bytes < self.item_result_bytes):
                raise ValueError('resource byte budgets must fit their declared Core item')

    def limits(self, resources):
        maximum = resources.max_active_batches
        budget = self.budget
        return SessionLimits(
            held_tasks=maximum if budget is None else budget.held_tasks,
            input_bytes=maximum * self.item_input_bytes if budget is None else budget.input_bytes,
            result_bytes=maximum * self.item_result_bytes if budget is None else budget.result_bytes,
            active_requests=maximum if budget is None else budget.active_requests,
            active_work=maximum if budget is None else budget.active_requests, offer_tasks=1,
            item_input_bytes=self.item_input_bytes, item_result_bytes=self.item_result_bytes,
            metadata_bytes=self.metadata_bytes, step_actions=self.step_actions,
            wait_timeout_s=resources.query_timeout_s, poll_interval_s=self.poll_interval_s,
            timeouts=SessionTimeouts(),
        )

    def method_capacity(self, resources, limits):
        from src.semantic_methods.budget import MethodCapacity, row_reservation
        budget = self.budget
        result = (MethodCapacity(resources.max_active_batches, resources.max_active_batches * row_reservation(limits))
                  if budget is None else MethodCapacity(budget.method_runs, budget.method_bytes))
        if result.bytes < row_reservation(limits):
            raise ValueError('method byte budget cannot hold one complete continuation envelope')
        return result

    def method_limits(self, stage_count):
        from src.semantic_methods.continuation import MethodLimits
        size = self.item_input_bytes + stage_count * self.item_result_bytes
        return MethodLimits(self.item_input_bytes, 2 * size, size, stage_count)



def resolve_records_async_limits(resources, capacity, overrides=None):
    """Bind IPC defaults to this owner, retaining independent explicit byte caps.

    Core totals remain R times its per-task allowances. IPC counts and per-item
    allowances default from that declaration; IPC totals cover its own items and
    result slots, rather than reusing a Core total as an IPC per-item allowance.
    """
    from src.serving.backends.vllm_async_engine import AsyncEngineLimits
    from src.serving.backends.vllm_engine import _integer

    if type(capacity) is not CoreRecordsCapacity:
        raise TypeError("Core records capacity must be CoreRecordsCapacity")
    if type(overrides) is AsyncEngineLimits:
        limits = overrides
    else:
        if overrides is not None and type(overrides) is not dict:
            raise TypeError("async_limits must be AsyncEngineLimits or an object")
        if capacity.budget is not None and not {'max_inflight', 'result_slots', 'command_slots',
                'control_slots', 'input_bytes', 'result_bytes'} <= set(overrides or {}):
            raise ValueError('explicit resource budgets require explicit finite IPC counts and byte totals')
        maximum = resources.max_active_batches
        values = dict(max_inflight=maximum, result_slots=maximum, command_slots=maximum,
            item_input_bytes=capacity.item_input_bytes, item_result_bytes=capacity.item_result_bytes,
            control_slots=max(16, maximum * 2))
        values.update(overrides or {})
        for name in ("item_input_bytes", "item_result_bytes", "result_slots"):
            _integer(values[name], name, minimum=1)
        values.setdefault("input_bytes", maximum * values["item_input_bytes"])
        values.setdefault("result_bytes", values["result_slots"] * values["item_result_bytes"])
        limits = AsyncEngineLimits(**values)
    if capacity.budget is None and limits.max_inflight != resources.max_active_batches:
        raise ValueError("async_limits must match the Core owner request limit")
    if capacity.budget is None and limits.result_slots < limits.max_inflight:
        raise ValueError("async_limits need one result slot per Core request")
    return limits



# Keep serialized declarations compatible with the original public import.
RecordsCapacityBudget.__module__ = "src.execution_provider.adapters.lotus_records_core"
CoreRecordsCapacity.__module__ = "src.execution_provider.adapters.lotus_records_core"
