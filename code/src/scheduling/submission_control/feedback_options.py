"""Sampling and comparison options, independent of feedback controllers."""
from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True)
class ResourceFeedbackConfig:
    """Shared measured-work feedback options."""
    poll_interval_s: float = 0.25
    window_s: float = 1.0
    min_samples: int = 16
    min_remaining: int = 0
    exploration_s: float | None = None
    max_changes: int | None = None
    allow_exploration: bool = False
    min_gain: float = 0.03
    memory_floor_bytes: int = 0
    search_mode: str = "local"
    work_tolerance: float = 0.03
    max_candidate_attempts: int | None = 2
    work_basis: str = "delivered"
    work_comparison: str = "envelope"
    pressure_response: str = "sample"

    def __post_init__(self):
        for name in ("poll_interval_s", "window_s", "exploration_s"):
            value = getattr(self, name)
            if name == "exploration_s" and value is None:
                continue
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(name + " must be finite and positive")
        for name in ("min_samples", "min_remaining", "max_changes"):
            value = getattr(self, name)
            if name == "max_changes" and value is None:
                continue
            if type(value) is not int or value < (0 if name == "min_remaining" else 1):
                raise ValueError(name + " must be a non-negative integer" if name == "min_remaining"
                                 else name + " must be a positive integer")
        if type(self.allow_exploration) is not bool:
            raise ValueError("allow_exploration must be boolean")
        if not 0 <= self.min_gain < 1:
            raise ValueError("min_gain must be between zero and one")
        if type(self.memory_floor_bytes) is not int or self.memory_floor_bytes < 0:
            raise ValueError("memory_floor_bytes must be non-negative")
        if self.search_mode not in ("local", "geometric"):
            raise ValueError("search_mode must be local or geometric")
        if not 0 <= self.work_tolerance < 1:
            raise ValueError("work_tolerance must be between zero and one")
        if self.max_candidate_attempts is not None and (
                type(self.max_candidate_attempts) is not int or not 1 <= self.max_candidate_attempts <= 4):
            raise ValueError("max_candidate_attempts must be None or between one and four")
        if self.work_basis not in ("delivered", "computed"):
            raise ValueError("work_basis must be delivered or computed")
        if self.work_basis == "computed" and self.search_mode != "geometric":
            raise ValueError("computed work requires geometric search")
        if self.work_comparison not in ("envelope", "local_pairs"):
            raise ValueError("work_comparison must be envelope or local_pairs")
        if self.work_comparison == "local_pairs" and self.work_basis != "computed":
            raise ValueError("local pairs require measured computed work")
        if self.pressure_response not in ("sample", "window"):
            raise ValueError("pressure_response must be sample or window")

@dataclass(frozen=True)
class ResourceRequestConfig:
    """Sampling and comparison choices for the independent request policy.

    Durations use driver seconds; min_samples counts completed model receipts.
    No elapsed/change/tail budget applies unless explicitly requested. These
    measurement choices are engineering settings, not hardware capacity facts.
    """
    poll_interval_s: float = .25
    window_s: float = 1.
    max_sample_age_s: float = 1.
    min_samples: int = 16
    measurement_windows: int = 3
    min_gain: float = .03
    min_remaining: int = 0
    exploration_s: float | None = None
    max_changes: int | None = None
    require_memory_feedback: bool = False
    allow_exploration: bool = True

    def __post_init__(self):
        for name in ('poll_interval_s', 'window_s', 'max_sample_age_s', 'exploration_s'):
            value = getattr(self, name)
            if name == 'exploration_s' and value is None:
                continue
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(name+' must be finite and positive')
        if self.poll_interval_s > self.window_s:
            raise ValueError('sampling must be at least as frequent as measurement')
        if self.poll_interval_s > self.max_sample_age_s:
            raise ValueError('sampling must fit the requested feedback age')
        for name, minimum in (('min_samples', 1), ('measurement_windows', 2),
                              ('min_remaining', 0), ('max_changes', 1)):
            value = getattr(self, name)
            if name == 'max_changes' and value is None:
                continue
            if type(value) is not int or value < minimum:
                raise ValueError(name+' must be an integer at least '+str(minimum))
        if (type(self.min_gain) not in (int, float) or not math.isfinite(self.min_gain)
                or not 0 <= self.min_gain < 1):
            raise ValueError('min_gain must be finite and between zero and one')
        for name in ('require_memory_feedback', 'allow_exploration'):
            if type(getattr(self, name)) is not bool:
                raise ValueError(name+' must be boolean')
