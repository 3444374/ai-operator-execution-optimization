"""Immutable run settings and pure phase/case/run assessment.

Execution progress never enters measurement-status composition. Diagnostic
reports project the same assessment as formal reports but grant no qualification.
"""
from dataclasses import asdict, dataclass
from typing import Mapping

from .resource_qualification import METRIC_SCHEMA, IMPLEMENTATION_REVISION, compose_status

REQUIRED_PHASES = {
    "stress_large_payload": ("stress", "client_exit"),
    "cancel_and_cleanup": ("cancel", "recovery"),
    "provider_disconnect_and_recovery": ("disconnect", "recovery"),
    "gateway_exit_and_recovery": ("alive", "absent", "recovery"),
}


@dataclass(frozen=True)
class RunSpec:
    mode: str = "formal"
    sample_seconds: float = .02
    baseline_interval_seconds: float = .05
    baseline_timeout_seconds: float = 10.
    baseline_samples: int = 5
    cleanup_interval_seconds: float = .25
    cleanup_timeout_seconds: float = 60.
    cleanup_samples: int = 3
    input_bytes: int = 100000
    output_bytes: int = 65536

    def __post_init__(self):
        if self.mode not in ("formal", "diagnostic"):
            raise ValueError("unknown_run_mode")
        if min(self.sample_seconds, self.baseline_timeout_seconds,
               self.cleanup_timeout_seconds) <= 0:
            raise ValueError("invalid_sampling_duration")
        if min(self.baseline_interval_seconds, self.cleanup_interval_seconds) < 0:
            raise ValueError("invalid_sampling_interval")
        if self.baseline_samples < 2 or self.cleanup_samples < 2:
            raise ValueError("invalid_stability_window")
        if (self.input_bytes, self.output_bytes) != (100000, 65536):
            raise ValueError("workload_bytes_changed")

    @property
    def rounds(self):
        return 1 if self.mode == "diagnostic" else 3

    @property
    def rows_per_round(self):
        return 100 if self.mode == "diagnostic" else 2000

    def manifest(self):
        return {**asdict(self), "rounds": self.rounds, "rows_per_round": self.rows_per_round,
                "metric_schema": METRIC_SCHEMA, "implementation_revision": IMPLEMENTATION_REVISION,
                "required_cases": REQUIRED_PHASES,
                "fault_fixture_protocol": "observe-before-handshake-v1",
                "fault_fixture_barrier_timeout_seconds": 5.,
                "error_contract_revision": "socket-access-v1"}


@dataclass(frozen=True)
class PhaseResult:
    phase: str
    state: str
    measurement_status: str | None = None
    policy_status: str | None = None
    problems: tuple[str, ...] = ()
    failures: tuple[dict, ...] = ()
    safe: bool = False
    artifacts: tuple[str, ...] = ()
    diagnostics: Mapping | None = None

    @property
    def assessment(self):
        if self.state != "completed":
            return None
        return compose_status((self.measurement_status, self.policy_status))


def assess_phases(required, phases):
    """No empty, missing, duplicated, or unfinished phase set can pass."""
    if not required or len(set(required)) != len(required):
        raise ValueError("invalid_required_phases")
    values = {p.phase: p for p in phases}
    problems = [problem for p in phases for problem in p.problems]
    failures = [failure for p in phases for failure in p.failures]
    complete = (len(values) == len(phases) and set(values) == set(required)
                and all(p.state == "completed" for p in phases))
    if not complete:
        state = "running" if any(p.state in ("running", "not_started") for p in phases) else "skipped"
        return {"execution_state": state, "assessment": None, "safe": False,
                "problems": problems + ["required_phase_not_completed"], "failures": failures}
    statuses = [p.assessment for p in phases]
    if failures:
        statuses.append(("valid", "failed"))
    assessment = compose_status(*statuses) if all(statuses) else None
    if assessment is None:
        assessment = ("invalid", "not_evaluated")
        problems.append("illegal_phase_assessment")
    return {"execution_state": "completed", "assessment": dict(zip(
        ("measurement_status", "qualification_status"), assessment)),
        "safe": all(p.safe for p in phases), "problems": problems, "failures": failures}


def project_report(value, mode):
    """Apply qualification eligibility uniformly to phase, case, and run files."""
    assessment = value.get("assessment")
    passed = assessment == {"measurement_status": "valid", "qualification_status": "passed"}
    return {**value, "mode": mode,
            "qualification_status": (assessment["qualification_status"]
                                     if mode == "formal" and assessment else "not_evaluated"),
            "diagnostic_status": ("passed" if passed else "not_passed") if mode == "diagnostic" else None}


def case_report(name, phases, spec):
    value = assess_phases(REQUIRED_PHASES[name], phases)
    return project_report({**value, "case": name, "phases": [asdict(p) for p in phases]}, spec.mode)


def run_report(cases, spec, *, runner_error=None):
    values = dict(cases)
    missing = set(REQUIRED_PHASES) - set(values)
    extra = set(values) - set(REQUIRED_PHASES)
    phases = []
    for name in REQUIRED_PHASES:
        value = values.get(name, {})
        assessment = value.get("assessment") or {}
        phases.append(PhaseResult(name, value.get("execution_state", "skipped"),
            assessment.get("measurement_status"), assessment.get("qualification_status"),
            tuple(value.get("problems", [])), tuple(value.get("failures", [])), value.get("safe", False)))
    result = assess_phases(tuple(REQUIRED_PHASES), phases)
    if missing or extra:
        result["problems"].append("required_case_set_mismatch")
        result["assessment"] = None
        result["safe"] = False
    result = project_report({**result, "cases": values, "metric_schema": METRIC_SCHEMA,
        "implementation_revision": IMPLEMENTATION_REVISION,
        "workload": spec.manifest(), "model_requests": 0,
        "runner_error": runner_error}, spec.mode)
    result["exit_code"] = exit_code(result)
    return result


def exit_code(report):
    if report.get("runner_error"):
        return 3
    if report.get("mode") == "diagnostic":
        return 2  # no formal qualification from a diagnostic
    value = report.get("assessment")
    if value is None or value.get("measurement_status") != "valid":
        return 2
    return 0 if value.get("qualification_status") == "passed" else 1
