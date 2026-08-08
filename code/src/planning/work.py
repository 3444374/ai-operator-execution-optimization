"""Engine-independent staged work and runtime-state contracts.

The contracts make data organization useful to later scheduling: modality
adapters describe stage demand once, while admission/routing can select the
currently constrained stage without importing text, image, Ray, or Daft code.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class StageWork:
    """Estimated demand for one named execution stage."""

    stage: str
    units: int
    unit: str

    def __post_init__(self) -> None:
        if not self.stage or not self.unit:
            raise ValueError("stage and unit must be non-empty")
        if (
            not isinstance(self.units, int)
            or isinstance(self.units, bool)
            or self.units < 0
        ):
            raise ValueError("stage work units must be a non-negative integer")


@dataclass(frozen=True)
class WorkDescriptor:
    """Comparable work, locality, uncertainty, and calibration provenance."""

    stages: tuple[StageWork, ...]
    primary_stage: str
    calibration_signature: str
    locality_key: str = ""
    deadline_s: float | None = None
    lower_primary_units: int | None = None
    upper_primary_units: int | None = None

    def __post_init__(self) -> None:
        if not self.stages:
            raise ValueError("stages must not be empty")
        names = tuple(item.stage for item in self.stages)
        if len(names) != len(set(names)):
            raise ValueError("stage names must be unique")
        if self.primary_stage not in names:
            raise ValueError("primary_stage must identify one stage")
        if not self.calibration_signature:
            raise ValueError("calibration_signature must be non-empty")
        if self.deadline_s is not None and (
            not math.isfinite(self.deadline_s) or self.deadline_s < 0
        ):
            raise ValueError("deadline_s must be finite and non-negative when present")
        primary_units = self.primary.units
        lower = primary_units if self.lower_primary_units is None else self.lower_primary_units
        upper = primary_units if self.upper_primary_units is None else self.upper_primary_units
        if (
            not isinstance(lower, int)
            or isinstance(lower, bool)
            or not isinstance(upper, int)
            or isinstance(upper, bool)
            or lower < 0
            or upper < lower
            or not lower <= primary_units <= upper
        ):
            raise ValueError("primary uncertainty bounds must contain primary work")

    @property
    def primary(self) -> StageWork:
        return next(item for item in self.stages if item.stage == self.primary_stage)

    def for_stage(self, stage: str) -> StageWork | None:
        return next((item for item in self.stages if item.stage == stage), None)


@dataclass(frozen=True)
class StageStateSnapshot:
    """One observation of work at an execution stage."""

    stage: str
    active_work: int
    queued_work: int
    service_rate_units_s: float | None
    oldest_queue_age_s: float
    observed_at_s: float
    capacity_work: int | None = None

    def __post_init__(self) -> None:
        if not self.stage:
            raise ValueError("stage must be non-empty")
        for name, value in (
            ("active_work", self.active_work),
            ("queued_work", self.queued_work),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.capacity_work is not None and (
            not isinstance(self.capacity_work, int)
            or isinstance(self.capacity_work, bool)
            or self.capacity_work <= 0
        ):
            raise ValueError("capacity_work must be positive when present")
        for name, value in (
            ("oldest_queue_age_s", self.oldest_queue_age_s),
            ("observed_at_s", self.observed_at_s),
        ):
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
        if self.service_rate_units_s is not None and (
            not math.isfinite(self.service_rate_units_s)
            or self.service_rate_units_s <= 0
        ):
            raise ValueError("service_rate_units_s must be finite and positive when present")


@dataclass(frozen=True)
class RuntimeStateSnapshot:
    """Atomic multi-stage observation consumed by a state-aware policy."""

    stages: tuple[StageStateSnapshot, ...]
    observed_at_s: float
    calibration_signature: str

    def __post_init__(self) -> None:
        if not self.stages:
            raise ValueError("stages must not be empty")
        names = tuple(item.stage for item in self.stages)
        if len(names) != len(set(names)):
            raise ValueError("runtime stage names must be unique")
        if not math.isfinite(self.observed_at_s) or self.observed_at_s < 0:
            raise ValueError("observed_at_s must be finite and non-negative")
        if not self.calibration_signature:
            raise ValueError("calibration_signature must be non-empty")

    def for_stage(self, stage: str) -> StageStateSnapshot | None:
        return next((item for item in self.stages if item.stage == stage), None)

    def is_fresh(self, *, now_s: float, max_age_s: float) -> bool:
        if not math.isfinite(now_s) or now_s < self.observed_at_s:
            raise ValueError("now_s must be finite and not precede observation")
        if not math.isfinite(max_age_s) or max_age_s < 0:
            raise ValueError("max_age_s must be finite and non-negative")
        return now_s - self.observed_at_s <= max_age_s
