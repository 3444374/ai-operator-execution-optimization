"""Optional model-stage destination choice using the existing endpoint router.

Capacity is a declared logical request/work ceiling for one execution owner,
never a physical device memory or utilization estimate. Occupancy is supplied
from that owner's live calls; this policy does not reserve or release credits.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

from ...planning.blocks import StageBlockDescriptor
from ..core.models import BatchRequest, EndpointSnapshot, TopologySnapshot
from ..endpoint_routing.policies import LeastWorkEndpointRouter


def _nonnegative_integer(value: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _time(value: float) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value >= 0)


@dataclass(frozen=True)
class StageDestination:
    """Declared endpoint/group labels plus optional observed runtime identity.

    Node-scoped logical accelerator IDs are not physical GPU UUIDs or proof of
    independent cards and HBM. This is selector metadata, not an input cache
    or transfer-cost protocol; the data owner interprets it in its callback.
    """

    endpoint_id: str
    execution_group_id: str
    runtime_actor_id: str | None = None
    node_id: str | None = None
    accelerator_ids: tuple[str, ...] = ()

    def __post_init__(self):
        if any(not isinstance(value, str) or not value
               for value in (self.endpoint_id, self.execution_group_id)):
            raise ValueError("destination identities must be non-empty strings")
        if any(value is not None and (not isinstance(value, str) or not value)
               for value in (self.runtime_actor_id, self.node_id)):
            raise ValueError("observed runtime identities must be non-empty strings")
        if (not isinstance(self.accelerator_ids, tuple)
                or any(not isinstance(value, str) or not value for value in self.accelerator_ids)
                or len(set(self.accelerator_ids)) != len(self.accelerator_ids)):
            raise ValueError("accelerator_ids must be unique non-empty strings")


@dataclass(frozen=True)
class StageCapacity:
    max_active_requests: int
    max_active_work: int

    def __post_init__(self):
        if not all(_nonnegative_integer(value) for value in (
                self.max_active_requests, self.max_active_work)):
            raise ValueError("stage capacity must contain non-negative integers")


@dataclass(frozen=True)
class StageActorCapacity:
    actor_index: int
    destination: StageDestination
    capacity: StageCapacity

    def __post_init__(self):
        if not _nonnegative_integer(self.actor_index):
            raise ValueError("actor_index must be a non-negative integer")
        if min(self.capacity.max_active_requests, self.capacity.max_active_work) <= 0:
            raise ValueError("declared actor capacity must be positive")


@dataclass(frozen=True)
class StageExecutionGroupCapacity:
    execution_group_id: str
    capacity: StageCapacity

    def __post_init__(self):
        if not isinstance(self.execution_group_id, str) or not self.execution_group_id:
            raise ValueError("execution_group_id must be a non-empty string")
        if min(self.capacity.max_active_requests, self.capacity.max_active_work) <= 0:
            raise ValueError("declared group capacity must be positive")


@dataclass(frozen=True)
class StageActiveWork:
    actor_index: int
    work_units: int

    def __post_init__(self):
        if not all(_nonnegative_integer(value) for value in (self.actor_index, self.work_units)):
            raise ValueError("active stage work must contain non-negative integers")


@dataclass(frozen=True)
class StageDispatchDecision:
    actor_index: int | None
    reason: str
    retry_at_s: float | None = None


@dataclass(frozen=True)
class _Rate:
    work_units_s: float
    unit: str
    calibration_signature: str
    model_signature: str
    observed_at_s: float


class StageModelDispatcher:
    """FIFO candidate placement with explicit actor/group ceilings.

    Only fully confirmed, validated completions are fed to observe_completion.
    A single latest sample is used; cumulative token observers cannot describe
    this completed-block work rate. Missing rates trigger deterministic sample
    rotation, not a comparison of seconds with raw work. Owner adapters must
    bind this object to one actual worker mapping and reset rates per run.
    """

    def __init__(
        self, actors: tuple[StageActorCapacity, ...],
        groups: tuple[StageExecutionGroupCapacity, ...], *,
        feedback_ttl_s: float = 30.0,
        service_rate_ttl_s: float = 60.0,
        placement_estimator: Callable[[StageBlockDescriptor, StageDestination, float], object | None] | None = None,
    ):
        if not actors or len({item.actor_index for item in actors}) != len(actors):
            raise ValueError("actors must have unique indices")
        if len({item.destination.endpoint_id for item in actors}) != len(actors):
            raise ValueError("actor endpoint identities must be unique")
        if not groups or len({item.execution_group_id for item in groups}) != len(groups):
            raise ValueError("groups must have unique identities")
        if any(not _time(value) or value <= 0 for value in (
                feedback_ttl_s, service_rate_ttl_s)):
            raise ValueError("feedback lifetimes must be finite and positive")
        self.actors = tuple(sorted(actors, key=lambda item: item.actor_index))
        self.groups = groups
        self._actors = {item.actor_index: item for item in actors}
        self._groups = {item.execution_group_id: item.capacity for item in groups}
        used_groups = {item.destination.execution_group_id for item in actors}
        if used_groups != set(self._groups):
            raise ValueError("declared groups must exactly cover the actor groups")
        for group_id, capacity in self._groups.items():
            members = [item.capacity for item in actors
                       if item.destination.execution_group_id == group_id]
            if (capacity.max_active_requests > sum(item.max_active_requests for item in members)
                    or capacity.max_active_work > sum(item.max_active_work for item in members)):
                raise ValueError("group capacity cannot exceed its declared member total")
        self.feedback_ttl_s = feedback_ttl_s
        self.service_rate_ttl_s = service_rate_ttl_s
        self.placement_estimator = placement_estimator
        self._actor_feedback: dict[int, tuple[StageCapacity, float]] = {}
        self._group_feedback: dict[str, tuple[StageCapacity, float]] = {}
        self._router = LeastWorkEndpointRouter()
        self.reset_service_feedback()

    def reset_service_feedback(self) -> None:
        """Do not carry warmup samples to another formal execution run."""
        self._rates: dict[int, _Rate] = {}
        self._sample_attempts: dict[int, int] = {}
        self._completion_sequence = 0
        self._decisions: Counter[str] = Counter()
        self._locality_observations: Counter[str] = Counter()

    def observe_actor_capacity(self, actor_index: int, capacity: StageCapacity, *,
                               observed_at_s: float) -> bool:
        declared = self._actors[actor_index].capacity
        return self._observe_capacity(self._actor_feedback, actor_index, declared,
                                      capacity, observed_at_s)

    def observe_group_capacity(self, execution_group_id: str, capacity: StageCapacity, *,
                               observed_at_s: float) -> bool:
        return self._observe_capacity(self._group_feedback, execution_group_id,
                                      self._groups[execution_group_id], capacity, observed_at_s)

    @staticmethod
    def _observe_capacity(records, key, declared, capacity, observed_at_s) -> bool:
        if (not _time(observed_at_s)
                or capacity.max_active_requests > declared.max_active_requests
                or capacity.max_active_work > declared.max_active_work
                or (key in records and observed_at_s < records[key][1])):
            return False
        records[key] = (capacity, observed_at_s)
        return True

    def invalidate_actor(self, actor_index: int) -> None:
        """A failed/unknown call provides no successful service observation."""
        self._rates.pop(actor_index, None)

    def observe_completion(self, actor_index: int, descriptor: StageBlockDescriptor, *,
                           service_s: float, now_s: float) -> bool:
        self._actors[actor_index]
        if not _time(now_s):
            return False
        self._completion_sequence += 1
        self._sample_attempts[actor_index] = self._completion_sequence
        self.invalidate_actor(actor_index)
        work = descriptor.work.for_stage("model")
        if work is None or work.units <= 0 or not _time(service_s) or service_s <= 0:
            return False
        rate = work.units / service_s
        if not math.isfinite(rate) or rate <= 0:
            return False
        self._rates[actor_index] = _Rate(rate, work.unit, descriptor.work.calibration_signature,
                                       descriptor.model_signature, now_s)
        return True

    def _capacity(self, records, key, declared, now_s):
        feedback = records.get(key)
        if feedback is not None and 0 <= now_s - feedback[1] < self.feedback_ttl_s:
            return feedback[0], feedback[1] + self.feedback_ttl_s
        return declared, None

    @staticmethod
    def _fits(capacity, usage, work):
        return (usage[0] + 1 <= capacity.max_active_requests
                and usage[1] + work <= capacity.max_active_work)

    def choose(
        self, descriptor: StageBlockDescriptor, request: BatchRequest, *,
        available_actor_indices: set[int], active: tuple[StageActiveWork, ...],
        now_s: float,
    ) -> StageDispatchDecision:
        """Choose only a destination for the owner's current FIFO head.

        Returning None never consumes a lease. retry_at_s is a local expiry
        that can unblock a free actor even while another actor has a slow RPC.
        """
        work = descriptor.work.for_stage("model")
        if (not _time(now_s) or work is None
                or request.estimated_work_units != work.units or request.work_unit != work.unit):
            raise ValueError("routing request must describe comparable model-stage work")
        actor_usage = {index: [0, 0] for index in self._actors}
        group_usage = {key: [0, 0] for key in self._groups}
        for call in active:
            actor = self._actors[call.actor_index]
            for usage in (actor_usage[call.actor_index],
                          group_usage[actor.destination.execution_group_id]):
                usage[0] += 1
                usage[1] += call.work_units
        can_ever_fit = any(self._fits(item.capacity, (0, 0), work.units)
                           and self._fits(self._groups[item.destination.execution_group_id],
                                          (0, 0), work.units) for item in self.actors)
        if not can_ever_fit:
            raise BufferError("FIFO model block exceeds every declared actor/group capacity")
        candidates, expiries = [], []
        for item in self.actors:
            index, group = item.actor_index, item.destination.execution_group_id
            if index not in available_actor_indices:
                continue
            actor_cap, actor_expiry = self._capacity(
                self._actor_feedback, index, item.capacity, now_s)
            group_cap, group_expiry = self._capacity(
                self._group_feedback, group, self._groups[group], now_s)
            if (self._fits(actor_cap, actor_usage[index], work.units)
                    and self._fits(group_cap, group_usage[group], work.units)):
                candidates.append(item)
            elif (self._fits(item.capacity, actor_usage[index], work.units)
                  and self._fits(self._groups[group], group_usage[group], work.units)):
                expiries.extend(value for value in (actor_expiry, group_expiry) if value is not None)
        if not candidates:
            return self._decision(None, "capacity_wait", min(expiries) if expiries else None)
        self._observe_locality(descriptor, candidates, now_s)
        rates = {}
        for item in candidates:
            sample = self._rates.get(item.actor_index)
            if (sample is not None and 0 <= now_s - sample.observed_at_s < self.service_rate_ttl_s
                    and sample.unit == work.unit
                    and sample.calibration_signature == descriptor.work.calibration_signature
                    and sample.model_signature == descriptor.model_signature):
                rates[item.actor_index] = sample.work_units_s
        if len(rates) != len(candidates):
            # Rotate all eligible actors. Rotating only missing-rate actors
            # would starve valid actors when one keeps returning service_s=0.
            item = min(candidates, key=lambda item: (
                self._sample_attempts.get(item.actor_index, -1), item.actor_index))
            return self._decision(item.actor_index, "sample_rotation")
        pool_id = "stage-model"
        topology = TopologySnapshot(tuple(
            EndpointSnapshot(
                endpoint_id=item.destination.endpoint_id,
                url=item.destination.endpoint_id, pool_id=pool_id,
                gpu_id=item.destination.execution_group_id, healthy=True,
                running=actor_usage[item.actor_index][0], waiting=0, kv_usage=None,
                observed_at_s=now_s,
                estimated_active_work=actor_usage[item.actor_index][1],
                service_rate_work_units_s=rates[item.actor_index],
            ) for item in candidates), now_s)
        decision = self._router.route(request, topology, pool_id)
        actor_index = next(item.actor_index for item in candidates
                           if item.destination.endpoint_id == decision.endpoint_id)
        return self._decision(actor_index, "least_work")

    def _observe_locality(self, descriptor, candidates, now_s):
        """Use the data owner's estimate(descriptor, destination, now_s) seam.

        The implemented residency provider returns None: no verified prepared
        content mapping exists at selection. No numeric result protocol is
        invented here. Unknown, unsupported and failed observations have no
        effect on routing or credits; obtaining useful costs remains pending.
        """
        if self.placement_estimator is None:
            return
        for item in candidates:
            try:
                estimate = self.placement_estimator(descriptor, item.destination, now_s)
            except Exception:
                self._locality_observations["lookup_error"] += 1
            else:
                self._locality_observations["unknown" if estimate is None else "unsupported"] += 1

    def _decision(self, actor_index, reason, retry_at_s=None):
        self._decisions[reason] += 1
        return StageDispatchDecision(actor_index, reason, retry_at_s)

    def snapshot(self) -> dict[str, object]:
        """Read-only diagnostics; outstanding occupancy remains owner-derived."""
        return {"policy": "stage_least_work_v1", "decisions": dict(self._decisions),
                "locality_observations": dict(self._locality_observations),
                "sampled_actor_indices": sorted(self._rates),
                "actors": [{"actor_index": item.actor_index,
                            "endpoint_id": item.destination.endpoint_id,
                            "execution_group_id": item.destination.execution_group_id,
                            "runtime_actor_id": item.destination.runtime_actor_id,
                            "node_id": item.destination.node_id,
                            "accelerator_ids": item.destination.accelerator_ids,
                            "max_active_requests": item.capacity.max_active_requests,
                            "max_active_work": item.capacity.max_active_work}
                           for item in self.actors],
                "groups": [{"execution_group_id": item.execution_group_id,
                            "max_active_requests": item.capacity.max_active_requests,
                            "max_active_work": item.capacity.max_active_work} for item in self.groups]}
