"""Engine-neutral lifecycle and capacity broker for staged AI blocks.

The broker answers one narrow question: can an encoded block move through
prepare and model stages without losing identity or exceeding declared byte,
work, or inflight limits?  It owns no Ray actors and imports no modality code.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, replace
from typing import Literal

from ...planning.blocks import StageBlockDescriptor
from ...planning.work import RuntimeStateSnapshot, StageStateSnapshot


BlockState = Literal[
    "encoded",
    "preparing",
    "ready",
    "modeling",
    "completed",
    "failed",
]
LeaseStage = Literal["prepare", "model"]


@dataclass(frozen=True)
class StageBrokerLimits:
    """Hard limits applied to one static or dynamically selected safe arm."""

    encoded_bytes: int
    ready_bytes: int
    ready_work: int
    prepare_inflight: int
    model_inflight: int

    def __post_init__(self) -> None:
        values = (
            self.encoded_bytes,
            self.ready_bytes,
            self.ready_work,
            self.prepare_inflight,
            self.model_inflight,
        )
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in values
        ):
            raise ValueError("stage broker limits must be positive integers")


@dataclass(frozen=True)
class StageLease:
    """One non-preemptive permission to execute a block stage."""

    lease_id: str
    stage: LeaseStage
    descriptor: StageBlockDescriptor
    issued_at_s: float


@dataclass(frozen=True)
class StageBrokerSnapshot:
    """Exact application-visible occupancy at one observation instant."""

    encoded_queued: int
    prepare_inflight: int
    ready_queued: int
    model_inflight: int
    completed: int
    failed: int
    encoded_held_bytes: int
    ready_held_bytes: int
    ready_held_work: int
    encoded_bytes_limit: int
    ready_bytes_limit: int
    ready_work_limit: int

    @property
    def active_blocks(self) -> int:
        return (
            self.encoded_queued
            + self.prepare_inflight
            + self.ready_queued
            + self.model_inflight
        )


class BoundedStageBroker:
    """Maintain a fail-closed encoded -> ready -> completed state machine.

    Ready capacity is reserved when a prepare lease is issued, not after its
    output is materialized.  This makes the byte/work bound an invariant even
    when all CPU tasks finish simultaneously.
    """

    def __init__(self, limits: StageBrokerLimits) -> None:
        self._limits = limits
        self._descriptors: dict[str, StageBlockDescriptor] = {}
        self._states: dict[str, BlockState] = {}
        self._encoded_queue: deque[str] = deque()
        self._ready_queue: deque[str] = deque()
        self._prepare_leases: dict[str, StageLease] = {}
        self._model_leases: dict[str, StageLease] = {}
        self._completed_rows: set[str] = set()
        self._admitted_rows: set[str] = set()
        self._encoded_held_bytes = 0
        self._ready_held_bytes = 0
        self._ready_held_work = 0
        self._lease_sequence = 0
        self._calibration_signature: str | None = None

    @property
    def limits(self) -> StageBrokerLimits:
        return self._limits

    def can_accept_encoded(self, descriptor: StageBlockDescriptor) -> bool:
        """Return whether adding this encoded block preserves source memory."""
        self._validate_new_descriptor(descriptor)
        return (
            self._encoded_held_bytes + descriptor.physical_bytes
            <= self._limits.encoded_bytes
        )

    def enqueue_encoded(self, descriptor: StageBlockDescriptor) -> None:
        """Admit one never-before-seen encoded block in global FIFO order."""
        if not self.can_accept_encoded(descriptor):
            raise BufferError("encoded byte limit would be exceeded")
        signature = descriptor.work.calibration_signature
        if self._calibration_signature is None:
            self._calibration_signature = signature
        elif signature != self._calibration_signature:
            raise ValueError("all broker blocks must share one calibration signature")
        self._descriptors[descriptor.block_id] = descriptor
        self._admitted_rows.update(descriptor.row_ids)
        self._states[descriptor.block_id] = "encoded"
        self._encoded_queue.append(descriptor.block_id)
        self._encoded_held_bytes += descriptor.physical_bytes
        self._assert_invariants()

    def lease_prepare(
        self,
        *,
        now_s: float,
        preferred_job_id: str | None = None,
    ) -> StageLease | None:
        """Reserve ready capacity and issue one prepare lease if eligible."""
        if len(self._prepare_leases) >= self._limits.prepare_inflight:
            return None
        block_id = self._eligible_encoded_block(preferred_job_id)
        if block_id is None:
            return None
        descriptor = self._descriptors[block_id]
        if (
            self._ready_held_bytes + descriptor.ready_bytes_estimate
            > self._limits.ready_bytes
            or self._ready_held_work + descriptor.model_work_units
            > self._limits.ready_work
        ):
            return None
        self._encoded_queue.remove(block_id)
        lease = self._new_lease("prepare", descriptor, now_s)
        self._prepare_leases[lease.lease_id] = lease
        self._states[block_id] = "preparing"
        self._ready_held_bytes += descriptor.ready_bytes_estimate
        self._ready_held_work += descriptor.model_work_units
        self._assert_invariants()
        return lease

    def complete_prepare(
        self,
        lease_id: str,
        prepared: StageBlockDescriptor,
        *,
        now_s: float,
    ) -> StageBlockDescriptor:
        """Move a prepared block into the real ready queue atomically."""
        lease = self._get_lease(self._prepare_leases, lease_id, "prepare")
        encoded = lease.descriptor
        self._validate_transition(encoded, prepared)
        if prepared.representation == "encoded":
            raise ValueError("prepare completion must change the representation")
        if prepared.physical_bytes > encoded.ready_bytes_estimate:
            raise BufferError("prepared bytes exceed the reserved ready capacity")
        if prepared.model_work_units > encoded.model_work_units:
            raise BufferError("prepared model work exceeds the reserved work capacity")
        prepared = replace(prepared, ready_at_s=now_s)
        self._prepare_leases.pop(lease_id)
        self._ready_held_bytes -= encoded.ready_bytes_estimate - prepared.physical_bytes
        self._ready_held_work -= encoded.model_work_units - prepared.model_work_units
        self._encoded_held_bytes -= encoded.physical_bytes
        self._descriptors[prepared.block_id] = prepared
        self._states[prepared.block_id] = "ready"
        self._ready_queue.append(prepared.block_id)
        self._assert_invariants()
        return prepared

    def fail_prepare(self, lease_id: str, *, requeue: bool) -> None:
        """Release reservations and either retry the encoded block or fail it."""
        lease = self._pop_lease(self._prepare_leases, lease_id, "prepare")
        descriptor = lease.descriptor
        self._ready_held_bytes -= descriptor.ready_bytes_estimate
        self._ready_held_work -= descriptor.model_work_units
        if requeue:
            descriptor = replace(descriptor, retry_count=descriptor.retry_count + 1)
            self._descriptors[descriptor.block_id] = descriptor
            self._states[descriptor.block_id] = "encoded"
            self._encoded_queue.appendleft(descriptor.block_id)
        else:
            self._states[descriptor.block_id] = "failed"
            self._encoded_held_bytes -= descriptor.physical_bytes
        self._assert_invariants()

    def lease_model(
        self,
        *,
        now_s: float,
        preferred_job_id: str | None = None,
    ) -> StageLease | None:
        """Issue model work only for a fully prepared ready block."""
        if len(self._model_leases) >= self._limits.model_inflight:
            return None
        block_id = self._eligible_ready_block(preferred_job_id)
        if block_id is None:
            return None
        self._ready_queue.remove(block_id)
        descriptor = self._descriptors[block_id]
        lease = self._new_lease("model", descriptor, now_s)
        self._model_leases[lease.lease_id] = lease
        self._states[block_id] = "modeling"
        self._assert_invariants()
        return lease

    def complete_model(
        self,
        lease_id: str,
        *,
        output_row_ids: tuple[str, ...],
    ) -> StageBlockDescriptor:
        """Release ready capacity after validating exactly-once row identity."""
        lease = self._get_lease(self._model_leases, lease_id, "model")
        descriptor = lease.descriptor
        if output_row_ids != descriptor.row_ids:
            raise ValueError("model output row order does not match the leased block")
        duplicates = self._completed_rows.intersection(output_row_ids)
        if duplicates:
            raise ValueError(f"model completion duplicates rows: {sorted(duplicates)[:5]}")
        self._model_leases.pop(lease_id)
        self._completed_rows.update(output_row_ids)
        self._ready_held_bytes -= descriptor.physical_bytes
        self._ready_held_work -= descriptor.model_work_units
        self._states[descriptor.block_id] = "completed"
        self._assert_invariants()
        return descriptor

    def fail_model(self, lease_id: str, *, requeue: bool) -> None:
        """Retry a ready block or release its held capacity on terminal failure."""
        lease = self._pop_lease(self._model_leases, lease_id, "model")
        descriptor = lease.descriptor
        if requeue:
            descriptor = replace(descriptor, retry_count=descriptor.retry_count + 1)
            self._descriptors[descriptor.block_id] = descriptor
            self._states[descriptor.block_id] = "ready"
            self._ready_queue.appendleft(descriptor.block_id)
        else:
            self._states[descriptor.block_id] = "failed"
            self._ready_held_bytes -= descriptor.physical_bytes
            self._ready_held_work -= descriptor.model_work_units
        self._assert_invariants()

    def snapshot(self) -> StageBrokerSnapshot:
        """Return exact counts and held capacities without reading engine state."""
        counts = {state: 0 for state in ("completed", "failed")}
        for state in self._states.values():
            if state in counts:
                counts[state] += 1
        return StageBrokerSnapshot(
            encoded_queued=len(self._encoded_queue),
            prepare_inflight=len(self._prepare_leases),
            ready_queued=len(self._ready_queue),
            model_inflight=len(self._model_leases),
            completed=counts["completed"],
            failed=counts["failed"],
            encoded_held_bytes=self._encoded_held_bytes,
            ready_held_bytes=self._ready_held_bytes,
            ready_held_work=self._ready_held_work,
            encoded_bytes_limit=self._limits.encoded_bytes,
            ready_bytes_limit=self._limits.ready_bytes,
            ready_work_limit=self._limits.ready_work,
        )

    def runtime_snapshot(
        self,
        *,
        observed_at_s: float,
        prepare_service_rate_units_s: float | None = None,
        model_service_rate_units_s: float | None = None,
    ) -> RuntimeStateSnapshot:
        """Build a real two-stage snapshot from queued and leased descriptors."""
        if self._calibration_signature is None:
            raise ValueError("cannot observe an empty broker before its first admission")
        preparing = tuple(lease.descriptor for lease in self._prepare_leases.values())
        modeling = tuple(lease.descriptor for lease in self._model_leases.values())
        encoded = tuple(self._descriptors[item] for item in self._encoded_queue)
        ready = tuple(self._descriptors[item] for item in self._ready_queue)
        return RuntimeStateSnapshot(
            stages=(
                StageStateSnapshot(
                    stage="prepare",
                    active_work=sum(item.prepare_work_units for item in preparing),
                    queued_work=sum(item.prepare_work_units for item in encoded),
                    service_rate_units_s=prepare_service_rate_units_s,
                    oldest_queue_age_s=self._oldest_age(encoded, observed_at_s),
                    observed_at_s=observed_at_s,
                ),
                StageStateSnapshot(
                    stage="model",
                    active_work=sum(item.model_work_units for item in modeling),
                    queued_work=sum(item.model_work_units for item in ready),
                    service_rate_units_s=model_service_rate_units_s,
                    oldest_queue_age_s=self._oldest_ready_age(ready, observed_at_s),
                    observed_at_s=observed_at_s,
                    capacity_work=self._limits.ready_work,
                ),
            ),
            observed_at_s=observed_at_s,
            calibration_signature=self._calibration_signature,
        )

    def is_drained(self) -> bool:
        """Return true when no admitted block remains unfinished."""
        return self.snapshot().active_blocks == 0

    def state_of(self, block_id: str) -> BlockState:
        try:
            return self._states[block_id]
        except KeyError as error:
            raise KeyError(f"unknown block_id: {block_id}") from error

    def _validate_new_descriptor(self, descriptor: StageBlockDescriptor) -> None:
        if descriptor.block_id in self._states:
            raise ValueError(f"duplicate block_id: {descriptor.block_id}")
        duplicates = set(descriptor.row_ids).intersection(self._admitted_rows)
        if duplicates:
            raise ValueError(f"row_ids already admitted: {sorted(duplicates)[:5]}")
        if descriptor.representation != "encoded":
            raise ValueError("new broker admissions must use encoded representation")

    @staticmethod
    def _validate_transition(
        encoded: StageBlockDescriptor,
        prepared: StageBlockDescriptor,
    ) -> None:
        identity = (
            "block_id",
            "job_id",
            "ordered_sequence",
            "row_ids",
            "content_digest",
            "transform_signature",
            "model_signature",
        )
        if any(getattr(encoded, name) != getattr(prepared, name) for name in identity):
            raise ValueError("prepare completion changed immutable block identity")
        if encoded.work != prepared.work:
            raise ValueError("prepare completion changed the calibrated work contract")

    def _eligible_encoded_block(self, preferred_job_id: str | None) -> str | None:
        return self._eligible_block(self._encoded_queue, preferred_job_id)

    def _eligible_ready_block(self, preferred_job_id: str | None) -> str | None:
        return self._eligible_block(self._ready_queue, preferred_job_id)

    def _eligible_block(
        self,
        queue: deque[str],
        preferred_job_id: str | None,
    ) -> str | None:
        if not queue:
            return None
        if preferred_job_id is None:
            return queue[0]
        return next(
            (
                block_id
                for block_id in queue
                if self._descriptors[block_id].job_id == preferred_job_id
            ),
            None,
        )

    def _new_lease(
        self,
        stage: LeaseStage,
        descriptor: StageBlockDescriptor,
        now_s: float,
    ) -> StageLease:
        if not math.isfinite(now_s) or now_s < 0:
            raise ValueError("lease time must be finite and non-negative")
        self._lease_sequence += 1
        return StageLease(
            lease_id=f"{stage}-{self._lease_sequence}-{descriptor.block_id}",
            stage=stage,
            descriptor=descriptor,
            issued_at_s=now_s,
        )

    @staticmethod
    def _get_lease(
        leases: dict[str, StageLease],
        lease_id: str,
        stage: LeaseStage,
    ) -> StageLease:
        try:
            return leases[lease_id]
        except KeyError as error:
            raise KeyError(f"unknown {stage} lease: {lease_id}") from error

    @staticmethod
    def _pop_lease(
        leases: dict[str, StageLease],
        lease_id: str,
        stage: LeaseStage,
    ) -> StageLease:
        try:
            lease = leases.pop(lease_id)
        except KeyError as error:
            raise KeyError(f"unknown {stage} lease: {lease_id}") from error
        return lease

    @staticmethod
    def _oldest_age(
        descriptors: tuple[StageBlockDescriptor, ...],
        observed_at_s: float,
    ) -> float:
        if not descriptors:
            return 0.0
        return max(0.0, observed_at_s - min(item.created_at_s for item in descriptors))

    @staticmethod
    def _oldest_ready_age(
        descriptors: tuple[StageBlockDescriptor, ...],
        observed_at_s: float,
    ) -> float:
        ready_times = tuple(
            item.ready_at_s for item in descriptors if item.ready_at_s is not None
        )
        return max(0.0, observed_at_s - min(ready_times)) if ready_times else 0.0

    def _assert_invariants(self) -> None:
        if not 0 <= self._encoded_held_bytes <= self._limits.encoded_bytes:
            raise AssertionError("encoded byte invariant violated")
        if not 0 <= self._ready_held_bytes <= self._limits.ready_bytes:
            raise AssertionError("ready byte invariant violated")
        if not 0 <= self._ready_held_work <= self._limits.ready_work:
            raise AssertionError("ready work invariant violated")
        if len(self._prepare_leases) > self._limits.prepare_inflight:
            raise AssertionError("prepare inflight invariant violated")
        if len(self._model_leases) > self._limits.model_inflight:
            raise AssertionError("model inflight invariant violated")
        containers = (
            set(self._encoded_queue),
            {lease.descriptor.block_id for lease in self._prepare_leases.values()},
            set(self._ready_queue),
            {lease.descriptor.block_id for lease in self._model_leases.values()},
        )
        union: set[str] = set()
        for container in containers:
            if union.intersection(container):
                raise AssertionError("a block appears in more than one active container")
            union.update(container)
        active_states = {
            block_id
            for block_id, state in self._states.items()
            if state not in ("completed", "failed")
        }
        if union != active_states:
            raise AssertionError("active containers and lifecycle states diverged")
        encoded_expected = sum(
            self._descriptors[block_id].physical_bytes for block_id in self._encoded_queue
        ) + sum(
            lease.descriptor.physical_bytes for lease in self._prepare_leases.values()
        )
        ready_bytes_expected = sum(
            lease.descriptor.ready_bytes_estimate
            for lease in self._prepare_leases.values()
        ) + sum(
            self._descriptors[block_id].physical_bytes for block_id in self._ready_queue
        ) + sum(
            lease.descriptor.physical_bytes for lease in self._model_leases.values()
        )
        ready_work_expected = sum(
            lease.descriptor.model_work_units
            for lease in self._prepare_leases.values()
        ) + sum(
            self._descriptors[block_id].model_work_units for block_id in self._ready_queue
        ) + sum(
            lease.descriptor.model_work_units for lease in self._model_leases.values()
        )
        if encoded_expected != self._encoded_held_bytes:
            raise AssertionError("encoded byte accounting diverged from block ownership")
        if ready_bytes_expected != self._ready_held_bytes:
            raise AssertionError("ready byte accounting diverged from block ownership")
        if ready_work_expected != self._ready_held_work:
            raise AssertionError("ready work accounting diverged from block ownership")
