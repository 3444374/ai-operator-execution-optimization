"""Engine-independent control-plane value objects."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class CapacityArm:
    """One offline-calibrated request/work capacity choice."""

    request_limit: int
    work_limit: int

    def __post_init__(self) -> None:
        if self.request_limit <= 0 or self.work_limit <= 0:
            raise ValueError("capacity limits must be positive")
