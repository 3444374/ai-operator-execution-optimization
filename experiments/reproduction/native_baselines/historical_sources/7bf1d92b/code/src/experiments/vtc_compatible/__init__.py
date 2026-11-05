"""VTC-compatible upstream multi-job workload construction."""

from .workload import (
    OFFICIAL_VTC_ARTIFACT_COMMIT,
    VtcMaterializedRow,
    VtcSourceRow,
    VtcSuiteSpec,
    build_suite,
    runner_environment,
    suite_spec,
)

__all__ = [
    "OFFICIAL_VTC_ARTIFACT_COMMIT",
    "VtcMaterializedRow",
    "VtcSourceRow",
    "VtcSuiteSpec",
    "build_suite",
    "runner_environment",
    "suite_spec",
]
