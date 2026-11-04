"""Declared Core storage, Method residency and IPC capacity for records owners.

Each existing resource owner consumes its own typed limits. Only the legacy
configuration translates one R into all three allocations; explicit budgets
retain independent counts and bytes. No hardware observation or scheduling.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace

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
        budget = self.budget
        maximum = resources.request_capacity() if budget is None else budget.active_requests
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
        result = (MethodCapacity(resources.request_capacity(), resources.request_capacity() * row_reservation(limits))
                  if budget is None else MethodCapacity(budget.method_runs, budget.method_bytes))
        if result.bytes < row_reservation(limits):
            raise ValueError('method byte budget cannot hold one complete continuation envelope')
        return result

    def method_limits(self, stage_count):
        from src.semantic_methods.continuation import MethodLimits
        size = self.item_input_bytes + stage_count * self.item_result_bytes
        return MethodLimits(self.item_input_bytes, 2 * size, size, stage_count)

    def with_storage_bytes(self, *, input_bytes, result_bytes, method_bytes):
        """Derive finite count guards from already-assigned payload storage.

        IPC reserves item_result_bytes before every request. MethodDriver uses
        row_reservation for each live continuation; positive MethodLimits give
        its minimum possible reservation. Their byte checks remain independent.
        This conversion does not acquire an exclusive service allocation or
        discover RAM, and does not cover Python/Ray objects or caller audits.
        """
        from src.semantic_methods.budget import row_reservation
        from src.semantic_methods.continuation import MethodLimits

        if self.budget is not None:
            raise ValueError('storage bytes cannot replace an existing resource budget')
        if any(type(value) is not int or value <= 0
               for value in (input_bytes, result_bytes, method_bytes)):
            raise ValueError('assigned storage bytes must be positive integers')
        if input_bytes < self.item_input_bytes or result_bytes < self.item_result_bytes:
            raise ValueError('assigned storage bytes must fit one declared Core item')
        if method_bytes < row_reservation(self.method_limits(1)):
            raise ValueError('method byte budget cannot hold one complete continuation envelope')
        results = result_bytes // self.item_result_bytes
        minimum_method = row_reservation(MethodLimits(1, 1, 1, 1))
        budget = RecordsCapacityBudget(results + 1, input_bytes, result_bytes, results,
            method_bytes // minimum_method + 1, method_bytes)
        return replace(self, budget=budget)



def resolve_records_async_limits(resources, capacity, overrides=None):
    """Bind finite IPC defaults to declared grants; explicit fields take priority.

    Legacy totals retain their R-based translation. With an independent budget,
    request slots come from active_requests, result slots from held_tasks and
    byte totals from their corresponding budgets. These are logical allocations,
    not observed memory or sustainable model capacity. IPC continues enforcing
    its own per-item, result, command and control reservations.
    """
    from src.serving.backends.vllm_async_engine import AsyncEngineLimits
    from src.serving.backends.vllm_protocol import _integer

    if type(capacity) is not CoreRecordsCapacity:
        raise TypeError("Core records capacity must be CoreRecordsCapacity")
    if capacity.budget is None:
        resources.request_capacity()
    if type(overrides) is AsyncEngineLimits:
        limits = overrides
    else:
        if overrides is not None and type(overrides) is not dict:
            raise TypeError("async_limits must be AsyncEngineLimits or an object")
        budget = capacity.budget
        maximum = resources.max_active_batches if budget is None else budget.active_requests
        values = dict(max_inflight=maximum,
            result_slots=maximum if budget is None else budget.held_tasks, command_slots=maximum,
            item_input_bytes=capacity.item_input_bytes, item_result_bytes=capacity.item_result_bytes,
            control_slots=max(16, maximum * 2))
        values.update(overrides or {})
        for name in ("item_input_bytes", "item_result_bytes", "result_slots"):
            _integer(values[name], name, minimum=1)
        values.setdefault("input_bytes", maximum * values["item_input_bytes"]
                          if budget is None else budget.input_bytes)
        values.setdefault("result_bytes", values["result_slots"] * values["item_result_bytes"]
                          if budget is None else budget.result_bytes)
        limits = AsyncEngineLimits(**values)
    if capacity.budget is None and limits.max_inflight != resources.max_active_batches:
        raise ValueError("async_limits must match the Core owner request limit")
    if capacity.budget is None and limits.result_slots < limits.max_inflight:
        raise ValueError("async_limits need one result slot per Core request")
    return limits


def independent_default_capacity(resources, capacity, overrides=None):
    """Use existing finite IPC declarations for ordinary single-stage storage.

    Request permission still belongs to resources.max_active_batches. Result
    reservation bytes bound resident Core tasks and Method envelopes. Their
    count guards sit one above that byte allowance so they cannot become a
    hidden request-window policy. Method sizes use the actual single-stage
    envelope; multi-stage low-level owners retain their explicit declarations.
    These are logical allocations, not available RAM or measured model limits.
    """
    from src.semantic_methods.budget import row_reservation
    from src.serving.backends.vllm_async_engine import AsyncEngineLimits

    if type(capacity) is not CoreRecordsCapacity or capacity.budget is not None:
        raise ValueError('independent defaults require a Core capacity without a complete budget')
    resources.request_capacity()
    if type(overrides) is AsyncEngineLimits:
        limits = overrides
    else:
        if overrides is not None and type(overrides) is not dict:
            raise TypeError('async_limits must be AsyncEngineLimits or an object')
        defaults = AsyncEngineLimits()
        values = asdict(defaults)
        values.update(max_inflight=resources.max_active_batches,
            item_input_bytes=capacity.item_input_bytes, item_result_bytes=capacity.item_result_bytes,
            command_slots=resources.max_active_batches,
            control_slots=max(defaults.control_slots, 2*resources.max_active_batches))
        explicit = overrides or {}
        values.update(explicit)
        for total, item in (('input_bytes', 'item_input_bytes'),
                            ('result_bytes', 'item_result_bytes')):
            if total not in explicit and type(values[item]) is int:
                values[total] = max(values[total], values[item])
        limits = AsyncEngineLimits(**values)
    input_bytes = max(limits.input_bytes, capacity.item_input_bytes)
    result_bytes = max(limits.result_bytes, capacity.item_result_bytes)
    rows = result_bytes // capacity.item_result_bytes
    method_rows = min(rows, input_bytes // capacity.item_input_bytes)
    envelope = row_reservation(capacity.method_limits(1))
    budget = RecordsCapacityBudget(rows+1, input_bytes, result_bytes,
        resources.max_active_batches, method_rows+1, method_rows*envelope)
    return replace(capacity, budget=budget), limits



# Keep serialized declarations compatible with the original public import.
RecordsCapacityBudget.__module__ = "src.execution_provider.adapters.lotus_records_core"
CoreRecordsCapacity.__module__ = "src.execution_provider.adapters.lotus_records_core"
