"""Policies that organize rows into model-service requests."""

from .service_quantum import ServiceQuantumSlice, slice_service_quanta

__all__ = ["ServiceQuantumSlice", "slice_service_quanta"]
