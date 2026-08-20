"""Fail-closed vLLM 0.25.1 ``--scheduler-cls`` capability skeleton.

Only ``CustomFCFSScheduler`` is structurally wired: it inherits the frozen
version's ``AsyncScheduler`` unchanged so a future capability run can measure
native/custom parity.  DRR and VTC stay blocked until the installed-source,
identity, and parity gates pass; the tested algorithms live in
``in_engine_scheduler_logic.py`` and are not speculatively spliced into vLLM's
private scheduling loop.
"""

from __future__ import annotations

import importlib.metadata

from vllm.v1.core.sched.async_scheduler import AsyncScheduler

from src.experiments.saor.vllm_0251_source_audit import FROZEN_VLLM_VERSION


def _require_frozen_vllm() -> None:
    installed = importlib.metadata.version("vllm")
    if installed != FROZEN_VLLM_VERSION:
        raise RuntimeError(
            f"scheduler plugin requires vLLM {FROZEN_VLLM_VERSION}, got {installed}"
        )


class CustomFCFSScheduler(AsyncScheduler):
    """Unchanged async scheduler used only as the custom-class parity control."""

    def __init__(self, *args, **kwargs) -> None:
        _require_frozen_vllm()
        super().__init__(*args, **kwargs)


class _BlockedReproductionScheduler(AsyncScheduler):
    reproduction_name = "unconfigured reproduction"

    def __init__(self, *args, **kwargs) -> None:
        _require_frozen_vllm()
        raise RuntimeError(
            f"{self.reproduction_name} is capability-blocked: audit the exact "
            "installed vLLM source, prove Job identity propagation, and pass "
            "native/custom FCFS parity before implementing the private-loop adapter"
        )


class DRRScheduler(_BlockedReproductionScheduler):
    """Reserved class path for the DRR-on-vLLM reproduction."""

    reproduction_name = "DRR-on-vLLM reproduction"


class VTCScheduler(_BlockedReproductionScheduler):
    """Reserved class path for the VTC-on-vLLM reproduction."""

    reproduction_name = "VTC-on-vLLM reproduction"
