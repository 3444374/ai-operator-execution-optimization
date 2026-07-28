"""Compatibility imports for UCB admission policies."""

from .submission_control.ucb import (
    SloRewardInput,
    UcbAdmissionController,
    UcbConfig,
    slo_constrained_reward,
)

__all__ = [
    "SloRewardInput",
    "UcbAdmissionController",
    "UcbConfig",
    "slo_constrained_reward",
]
