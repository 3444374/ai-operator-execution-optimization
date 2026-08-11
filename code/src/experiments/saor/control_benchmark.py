"""Measure CPU-side control-path overhead without making GPU claims."""

from __future__ import annotations

import math
import time
from dataclasses import asdict, dataclass
from typing import Callable

from src.planning.work import RuntimeStateSnapshot, StageStateSnapshot
from src.scheduling.core.control import CapacityArm
from src.scheduling.submission_control.capacity import BoundedCapacityController
from src.scheduling.submission_control.saor import (
    SaorAction,
    SaorControlState,
    SaorJobState,
    SaorPolicy,
)
from src.scheduling.submission_control.shared_credit import (
    FairEndpointCreditCoordinator,
)


@dataclass(frozen=True)
class ControlBenchmarkRow:
    policy: str
    operation: str
    job_count: int
    iterations: int
    latency_us_p50: float
    latency_us_p95: float
    operations_per_s: float
    claim_scope: str = "cpu_control_path_only"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def run_control_benchmark(
    *,
    job_counts: tuple[int, ...],
    iterations: int,
    warmup_iterations: int,
    repeats: int,
) -> tuple[ControlBenchmarkRow, ...]:
    """Benchmark five existing decision paths under a shared timing contract."""

    if not job_counts or any(value <= 0 for value in job_counts):
        raise ValueError("job_counts must contain positive integers")
    if iterations <= 0 or warmup_iterations < 0 or repeats <= 0:
        raise ValueError("benchmark iteration counts are invalid")
    rows = []
    for job_count in job_counts:
        for policy, operation, factory in _case_factories(job_count):
            operation_call = factory()
            for index in range(warmup_iterations):
                operation_call(index)
            samples_ns = []
            for repeat in range(repeats):
                for index in range(iterations):
                    sequence = repeat * iterations + index + warmup_iterations
                    started_ns = time.perf_counter_ns()
                    operation_call(sequence)
                    samples_ns.append(time.perf_counter_ns() - started_ns)
            total_ns = sum(samples_ns)
            rows.append(
                ControlBenchmarkRow(
                    policy=policy,
                    operation=operation,
                    job_count=job_count,
                    iterations=len(samples_ns),
                    latency_us_p50=_percentile(samples_ns, 0.50) / 1_000.0,
                    latency_us_p95=_percentile(samples_ns, 0.95) / 1_000.0,
                    operations_per_s=(
                        len(samples_ns) * 1_000_000_000.0 / max(1, total_ns)
                    ),
                )
            )
    return tuple(rows)


def _case_factories(
    job_count: int,
) -> tuple[tuple[str, str, Callable[[], Callable[[int], object]]], ...]:
    return (
        ("static", "read_frozen_arm", _static_factory),
        (
            "legacy_threshold",
            "select_capacity_arm",
            _legacy_threshold_factory,
        ),
        (
            "shared_drr",
            "acquire_and_release_credit",
            lambda: _credit_factory(job_count, "drr"),
        ),
        (
            "external_vtc",
            "acquire_and_release_credit",
            lambda: _credit_factory(job_count, "vtc"),
        ),
        ("saor", "select_finite_dpp_action", lambda: _saor_factory(job_count)),
    )


def _static_factory() -> Callable[[int], CapacityArm]:
    arm = CapacityArm(request_limit=128, work_limit=65_536)

    def decide(_: int) -> CapacityArm:
        return arm

    return decide


def _legacy_threshold_factory() -> Callable[[int], object]:
    arm = CapacityArm(request_limit=128, work_limit=65_536)
    controller = BoundedCapacityController(
        (arm,),
        fallback=arm,
        target_service_rate_tokens_s=1.0,
        congestion_kv_usage=1.0,
        consecutive_samples=1,
        cooldown_samples=0,
    )
    snapshot = RuntimeStateSnapshot(
        stages=(
            StageStateSnapshot("organizer", 0, 0, None, 0.0, 0.0),
            StageStateSnapshot("model", 0, 0, 1.0, 0.0, 0.0, arm.work_limit),
        ),
        observed_at_s=0.0,
        calibration_signature="benchmark",
    )

    def decide(_: int) -> object:
        return controller.select(
            snapshot,
            active_requests=0,
            service_waiting_requests=0,
            service_rate_tokens_s=1.0,
            kv_usage=0.0,
            now_s=0.0,
            max_age_s=0.0,
            calibration_signature="benchmark",
        )

    return decide


def _credit_factory(job_count: int, policy: str) -> Callable[[int], bool]:
    endpoint_id = "endpoint-0"
    coordinator = FairEndpointCreditCoordinator(
        {endpoint_id: (1, 1_024)},
        quantum=1_024,
        policy=policy,
    )

    def decide(sequence: int) -> bool:
        job_id = f"job-{sequence % job_count}"
        request_id = f"request-{sequence}"
        granted = coordinator.try_acquire(
            request_id=request_id,
            job_id=job_id,
            endpoint_id=endpoint_id,
            estimated_work=1,
        )
        if not granted:
            raise RuntimeError("benchmark credit request was not granted")
        coordinator.release(request_id, job_id=job_id, actual_work=1)
        return granted

    return decide


def _saor_factory(job_count: int) -> Callable[[int], object]:
    endpoint_id = "endpoint-0"
    arm = CapacityArm(request_limit=max(1, job_count), work_limit=65_536)
    jobs = tuple(
        SaorJobState(
            job_id=f"job-{index}",
            weight=1.0,
            ready_work=1_024,
            fairness_debt=float(index),
        )
        for index in range(job_count)
    )
    actions = tuple(
        SaorAction(
            action_id=f"serve-job-{index}",
            endpoint_id=endpoint_id,
            arm=arm,
            predicted_service_by_job=((f"job-{index}", 1.0),),
        )
        for index in range(job_count)
    )
    state = SaorControlState(
        jobs=jobs,
        actions=actions,
        fallback_action=SaorAction("fallback", endpoint_id, arm),
        current_arm=arm,
        observed_at_s=0.0,
        calibration_signature="benchmark",
    )
    policy = SaorPolicy(
        v=1.0,
        eta_f=1.0,
        tail_weight=1.0,
        energy_weight=0.0,
        switch_weight=1.0,
    )

    def decide(_: int) -> object:
        return policy.select(
            state,
            now_s=0.0,
            max_age_s=0.0,
            calibration_signature="benchmark",
        )

    return decide


def _percentile(values: list[int], quantile: float) -> float:
    if not values or not math.isfinite(quantile) or not 0 <= quantile <= 1:
        raise ValueError("percentile inputs are invalid")
    ordered = sorted(values)
    index = math.ceil(quantile * len(ordered)) - 1
    return float(ordered[max(0, index)])
