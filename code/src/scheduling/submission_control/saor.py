"""Pure Stage-Aware Ordered Release drift-plus-penalty policy."""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Mapping

from ..core.control import CapacityArm


@dataclass(frozen=True)
class SaorJobState:
    job_id: str
    weight: float
    ready_work: int
    active_work: int = 0
    fairness_debt: float = 0.0

    def __post_init__(self) -> None:
        if not self.job_id:
            raise ValueError("job_id must be non-empty")
        if not math.isfinite(self.weight) or self.weight <= 0:
            raise ValueError("job weight must be finite and positive")
        if self.ready_work < 0 or self.active_work < 0:
            raise ValueError("job work must be non-negative")
        if not math.isfinite(self.fairness_debt) or self.fairness_debt < 0:
            raise ValueError("fairness debt must be finite and non-negative")

    @property
    def backlogged(self) -> bool:
        return self.ready_work + self.active_work > 0


@dataclass(frozen=True)
class SaorReleaseCandidate:
    request_id: str
    job_id: str
    endpoint_id: str
    estimated_work: int

    def __post_init__(self) -> None:
        if not self.request_id or not self.job_id or not self.endpoint_id:
            raise ValueError("release identifiers must be non-empty")
        if self.estimated_work <= 0:
            raise ValueError("release work must be positive")


@dataclass(frozen=True)
class SaorAction:
    """One action with service/cost deltas measured against the hold action."""

    action_id: str
    endpoint_id: str
    arm: CapacityArm
    releases: tuple[SaorReleaseCandidate, ...] = ()
    predicted_service_by_job: tuple[tuple[str, float], ...] = ()
    predicted_goodput_delta: float = 0.0
    tail_risk_delta: float = 0.0
    energy_delta: float = 0.0
    switch_cost: float = 0.0

    def __post_init__(self) -> None:
        if not self.action_id or not self.endpoint_id:
            raise ValueError("action identifiers must be non-empty")
        request_ids = tuple(item.request_id for item in self.releases)
        if len(request_ids) != len(set(request_ids)):
            raise ValueError("an action cannot release a request twice")
        if any(item.endpoint_id != self.endpoint_id for item in self.releases):
            raise ValueError("all releases must target the action endpoint")
        service_jobs = tuple(item[0] for item in self.predicted_service_by_job)
        if len(service_jobs) != len(set(service_jobs)):
            raise ValueError("predicted service must contain each Job once")
        if any(
            not job_id
            or not math.isfinite(value)
            or value < 0
            for job_id, value in self.predicted_service_by_job
        ):
            raise ValueError("predicted service must be finite and non-negative")
        for name, value in (
            ("predicted_goodput_delta", self.predicted_goodput_delta),
            ("tail_risk_delta", self.tail_risk_delta),
            ("energy_delta", self.energy_delta),
        ):
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        if not math.isfinite(self.switch_cost) or self.switch_cost < 0:
            raise ValueError("switch_cost must be finite and non-negative")

    @property
    def service_by_job(self) -> dict[str, float]:
        return dict(self.predicted_service_by_job)


@dataclass(frozen=True)
class SaorControlState:
    jobs: tuple[SaorJobState, ...]
    actions: tuple[SaorAction, ...]
    fallback_action: SaorAction
    current_arm: CapacityArm
    observed_at_s: float
    calibration_signature: str
    valid: bool = True

    def __post_init__(self) -> None:
        job_ids = tuple(item.job_id for item in self.jobs)
        action_ids = tuple(item.action_id for item in self.actions)
        if len(job_ids) != len(set(job_ids)):
            raise ValueError("job IDs must be unique")
        if len(action_ids) != len(set(action_ids)):
            raise ValueError("action IDs must be unique")
        if self.fallback_action.releases:
            raise ValueError("fallback action must not release new work")
        known_jobs = set(job_ids)
        jobs_by_id = {job.job_id: job for job in self.jobs}
        for action in (*self.actions, self.fallback_action):
            referenced_jobs = {
                item.job_id for item in action.releases
            } | set(action.service_by_job)
            unknown_jobs = referenced_jobs - known_jobs
            if unknown_jobs:
                raise ValueError(
                    f"action contains unknown jobs: {sorted(unknown_jobs)}"
                )
            released_by_job: dict[str, int] = {}
            for item in action.releases:
                released_by_job[item.job_id] = (
                    released_by_job.get(item.job_id, 0) + item.estimated_work
                )
            if any(
                released_work > jobs_by_id[job_id].ready_work
                for job_id, released_work in released_by_job.items()
            ):
                raise ValueError("action releases more work than the Job has ready")
            if any(
                service
                > jobs_by_id[job_id].ready_work
                + jobs_by_id[job_id].active_work
                for job_id, service in action.predicted_service_by_job
            ):
                raise ValueError("predicted service exceeds available Job work")
        endpoint_ids = {
            action.endpoint_id for action in (*self.actions, self.fallback_action)
        }
        if len(endpoint_ids) != 1:
            raise ValueError("control state actions must target one endpoint")
        if not math.isfinite(self.observed_at_s) or self.observed_at_s < 0:
            raise ValueError("observed_at_s must be finite and non-negative")
        if not self.calibration_signature:
            raise ValueError("calibration_signature must be non-empty")


@dataclass(frozen=True)
class SaorDecision:
    action: SaorAction
    score: float
    reason: str
    action_scores: tuple[tuple[str, float], ...]


class SaorPolicy:
    """Choose the minimum finite-action DPP surrogate without engine imports."""

    def __init__(
        self,
        *,
        v: float,
        eta_f: float,
        tail_weight: float,
        energy_weight: float,
        switch_weight: float,
    ) -> None:
        values = (v, eta_f, tail_weight, energy_weight, switch_weight)
        if any(not math.isfinite(value) or value < 0 for value in values):
            raise ValueError("SAOR weights must be finite and non-negative")
        self.v = v
        self.eta_f = eta_f
        self.tail_weight = tail_weight
        self.energy_weight = energy_weight
        self.switch_weight = switch_weight

    def select(
        self,
        state: SaorControlState,
        *,
        now_s: float,
        max_age_s: float,
        calibration_signature: str,
    ) -> SaorDecision:
        if not math.isfinite(now_s) or now_s < state.observed_at_s:
            raise ValueError("now_s must not precede the observation")
        if not math.isfinite(max_age_s) or max_age_s < 0:
            raise ValueError("max_age_s must be finite and non-negative")
        if not state.valid:
            return self._fallback(state, "invalid_observation")
        if state.calibration_signature != calibration_signature:
            return self._fallback(state, "calibration_signature_mismatch")
        if now_s - state.observed_at_s > max_age_s:
            return self._fallback(state, "stale_observation")
        if not state.actions:
            return self._fallback(state, "no_feasible_action")
        queue_work = {job.job_id: job.ready_work for job in state.jobs}
        fairness_debt = {job.job_id: job.fairness_debt for job in state.jobs}
        active = tuple(job for job in state.jobs if job.backlogged)
        total_weight = sum(job.weight for job in active)
        fairness_share = (
            sum(
                job.fairness_debt * job.weight / total_weight
                for job in active
            )
            if total_weight > 0
            else 0.0
        )
        scored = tuple(
            (
                action.action_id,
                self._score(
                    queue_work,
                    fairness_debt,
                    fairness_share,
                    action,
                ),
            )
            for action in state.actions
        )
        by_id = {action.action_id: action for action in state.actions}
        action_id, score = min(scored, key=lambda item: (item[1], item[0]))
        return SaorDecision(
            by_id[action_id],
            score,
            "minimum_drift_plus_penalty",
            scored,
        )

    def _score(
        self,
        queue_work: Mapping[str, int],
        fairness_debt: Mapping[str, float],
        fairness_share: float,
        action: SaorAction,
    ) -> float:
        service = action.service_by_job
        total_service = sum(service.values())
        queue_term = -sum(
            queue_work[job_id] * completed
            for job_id, completed in service.items()
        )
        fairness_term = fairness_share * total_service - sum(
            fairness_debt[job_id] * completed
            for job_id, completed in service.items()
        )
        cost = (
            -action.predicted_goodput_delta
            + self.tail_weight * action.tail_risk_delta
            + self.energy_weight * action.energy_delta
            + self.switch_weight * action.switch_cost
        )
        return queue_term + self.eta_f * fairness_term + self.v * cost

    @staticmethod
    def _fallback(state: SaorControlState, reason: str) -> SaorDecision:
        return SaorDecision(state.fallback_action, 0.0, reason, ())


def update_fairness_debts(
    jobs: tuple[SaorJobState, ...],
    completed_work_by_job: Mapping[str, int],
) -> tuple[SaorJobState, ...]:
    """Apply one weighted common-backlog fairness-debt update."""

    known = {job.job_id for job in jobs}
    unknown = set(completed_work_by_job) - known
    if unknown:
        raise ValueError(f"completion contains unknown jobs: {sorted(unknown)}")
    if any(
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        for value in completed_work_by_job.values()
    ):
        raise ValueError("completed work must be non-negative integers")
    active = tuple(
        job
        for job in jobs
        if job.backlogged or completed_work_by_job.get(job.job_id, 0) > 0
    )
    total_weight = sum(job.weight for job in active)
    total_completed = sum(
        completed_work_by_job.get(job.job_id, 0) for job in active
    )
    active_ids = {job.job_id for job in active}
    return tuple(
        replace(
            job,
            fairness_debt=max(
                0.0,
                job.fairness_debt
                + job.weight / total_weight * total_completed
                - completed_work_by_job.get(job.job_id, 0),
            ),
        )
        if job.job_id in active_ids and total_weight > 0
        else job
        for job in jobs
    )


def build_single_release_actions(
    *,
    endpoint_id: str,
    arms: tuple[CapacityArm, ...],
    current_arm: CapacityArm,
    ready_heads: tuple[SaorReleaseCandidate, ...],
    active_requests: int,
    active_work: int,
    predicted_incremental_service_by_request: Mapping[str, float],
    predicted_goodput_delta_by_arm: Mapping[CapacityArm, float],
    tail_risk_delta_by_arm: Mapping[CapacityArm, float],
    energy_delta_by_arm: Mapping[CapacityArm, float],
    switch_cost_by_arm: Mapping[CapacityArm, float],
) -> tuple[SaorAction, ...]:
    """Enumerate actions using explicit marginal predictions versus hold."""

    if not endpoint_id or not arms or current_arm not in arms:
        raise ValueError("endpoint and calibrated arms must be valid")
    if active_requests < 0 or active_work < 0:
        raise ValueError("active request/work counts must be non-negative")
    if any(item.endpoint_id != endpoint_id for item in ready_heads):
        raise ValueError("ready heads must target the selected endpoint")
    missing_service = {
        item.request_id for item in ready_heads
    } - set(predicted_incremental_service_by_request)
    if missing_service:
        raise ValueError(
            f"missing service predictions: {sorted(missing_service)}"
        )
    if any(
        not math.isfinite(float(predicted_incremental_service_by_request[item.request_id]))
        or float(predicted_incremental_service_by_request[item.request_id]) < 0
        for item in ready_heads
    ):
        raise ValueError("incremental service predictions must be finite and non-negative")
    predictions = (
        ("goodput delta", predicted_goodput_delta_by_arm),
        ("tail-risk delta", tail_risk_delta_by_arm),
        ("energy delta", energy_delta_by_arm),
        ("switch cost", switch_cost_by_arm),
    )
    for name, values in predictions:
        missing_arms = set(arms) - set(values)
        if missing_arms:
            raise ValueError(f"missing {name} for arms: {sorted(missing_arms)}")
        if any(not math.isfinite(float(values[arm])) for arm in arms):
            raise ValueError(f"{name} predictions must be finite")
    if any(float(switch_cost_by_arm[arm]) < 0 for arm in arms):
        raise ValueError("switch cost predictions must be non-negative")
    actions = [SaorAction("hold", endpoint_id, current_arm)]
    for arm in arms:
        if active_requests + 1 > arm.request_limit:
            continue
        for item in ready_heads:
            if active_work + item.estimated_work > arm.work_limit:
                continue
            actions.append(
                SaorAction(
                    f"{endpoint_id}:{arm.request_limit}:{arm.work_limit}:"
                    f"{item.request_id}",
                    endpoint_id,
                    arm,
                    (item,),
                    ((
                        item.job_id,
                        float(
                            predicted_incremental_service_by_request[
                                item.request_id
                            ]
                        ),
                    ),),
                    predicted_goodput_delta=float(
                        predicted_goodput_delta_by_arm[arm]
                    ),
                    tail_risk_delta=float(tail_risk_delta_by_arm[arm]),
                    energy_delta=float(energy_delta_by_arm[arm]),
                    switch_cost=float(switch_cost_by_arm[arm]),
                )
            )
    return tuple(actions)
