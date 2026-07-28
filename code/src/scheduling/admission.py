"""Compatibility imports for admission policies."""

from .submission_control.admission import (
    DynamicAdmissionGate,
    ObservationProvider,
    StaticAdmissionController,
    WindowController,
)

__all__ = [
    "DynamicAdmissionGate",
    "ObservationProvider",
    "StaticAdmissionController",
    "WindowController",
]
