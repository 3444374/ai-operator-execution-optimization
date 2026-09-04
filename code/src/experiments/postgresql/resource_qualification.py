"""Pure metric-schema-v2 gate policies for PostgreSQL resource qualification.

Consumes classified ``ResourceTrace`` snapshots from
``observability.process_resources`` and returns structured verdicts. No
process management, no /proc access, no I/O: every function is a pure
mapping so unit tests can drive all red/green/inconclusive paths with
synthetic traces.

Schema registered in experiments/plans/postgresql_semmap_generation_contract.md
§8.4.2 (2026-09-04, frozen before any v2 rerun). Thresholds are unchanged
from the user-confirmed §8.4.1 values; only the measurement implementation
differs from v1.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from src.observability.process_resources.model import (
    FdKind,
    FdIdentity,
    ProcessSnapshot,
    ResourceTrace,
)

METRIC_SCHEMA = "semloom.pg.resource.v2"

MIB = 1024 * 1024

RSS_LIMITS = {
    "backend": (16 * MIB, 8 * MIB),   # (peak_delta_limit, end_delta_limit)
    "gateway": (32 * MIB, 16 * MIB),
}
UDS_CLIENT_PEAK_LIMIT = 1
UDS_ACCEPTED_PEAK_LIMIT = 1
UDS_SESSION_COMBINED_PEAK_LIMIT = 2
UDS_SESSION_COMBINED_END_LIMIT = 0
TOTAL_FD_END_LIMIT = 0
THREAD_END_LIMIT = 0

DIAGNOSTIC_KINDS = (
    FdKind.POSTGRES_CLIENT_SOCKET,
    FdKind.RELATION_FILE,
    FdKind.TOAST_RELATION_FILE,
    FdKind.POSTGRES_TEMP_FILE,
    FdKind.REGULAR_FILE_OTHER,
    FdKind.PIPE,
    FdKind.EVENTFD_OR_ANON_INODE,
    FdKind.SOCKET_OTHER,
)


@dataclass(frozen=True)
class Violation:
    scope: str
    metric: str
    observed: int
    limit: int
    detail: dict = field(default_factory=dict)


@dataclass(frozen=True)
class GateReport:
    metric_schema: str
    measurement_status: str          # valid | inconclusive | invalid
    qualification_status: str        # passed | failed | not_evaluated
    peak_policy: list[Violation]
    cleanup_policy: list[Violation]
    diagnostics: dict

    @property
    def passed(self) -> bool:
        return self.qualification_status == "passed"


def _peak_fd_delta(
    baseline: ProcessSnapshot, trace: ResourceTrace, role: str,
    kinds: frozenset[FdKind],
) -> tuple[int, set[int]]:
    """Return (peak minus baseline count, union of peak fd numbers) per kind set."""
    base_numbers = baseline.fd_numbers(kinds)
    peak_numbers = set(base_numbers)
    for sample in trace.samples:
        point = sample.get(role)
        if point is None:
            continue
        peak_numbers |= point.fd_numbers(kinds)
    return len(peak_numbers) - len(base_numbers), peak_numbers


def _end_snapshot(trace: ResourceTrace, role: str) -> ProcessSnapshot | None:
    last = None
    for sample in trace.samples:
        point = sample.get(role)
        if point is not None:
            last = point
    return last


def evaluate_peak_policy(
    baseline: Mapping[str, ProcessSnapshot], trace: ResourceTrace,
) -> tuple[list[Violation], dict]:
    """Judge the irreversible stress-window peaks exactly once."""
    violations: list[Violation] = []
    diagnostics: dict = {"per_role": {}}

    for role in ("backend", "gateway"):
        base = baseline.get(role)
        if base is None:
            continue
        kind = (
            FdKind.PROVIDER_UDS_CLIENT if role == "backend"
            else FdKind.PROVIDER_UDS_ACCEPTED
        )
        delta, numbers = _peak_fd_delta(base, trace, role, frozenset({kind}))
        diagnostics["per_role"][role] = {
            "provider_uds_peak_fds": sorted(numbers),
            "provider_uds_peak_delta": delta,
        }
        limit = UDS_CLIENT_PEAK_LIMIT if role == "backend" else UDS_ACCEPTED_PEAK_LIMIT
        if delta > limit:
            violations.append(Violation(
                scope=role,
                metric=f"provider_uds_{'client' if role == 'backend' else 'accepted'}_fd_peak_delta",
                observed=delta, limit=limit,
                detail={"peak_fds": sorted(numbers)}))

    combined = 0
    for role in ("backend", "gateway"):
        base = baseline.get(role)
        if base is None:
            continue
        kinds = frozenset({FdKind.PROVIDER_UDS_CLIENT, FdKind.PROVIDER_UDS_ACCEPTED})
        delta, _ = _peak_fd_delta(base, trace, role, kinds)
        combined += delta
    diagnostics["provider_uds_session_fd_peak_delta_combined"] = combined
    if combined > UDS_SESSION_COMBINED_PEAK_LIMIT:
        violations.append(Violation(
            scope="backend+gateway",
            metric="provider_uds_session_fd_peak_delta_combined",
            observed=combined, limit=UDS_SESSION_COMBINED_PEAK_LIMIT))

    for role, (peak_limit, _) in RSS_LIMITS.items():
        base = baseline.get(role)
        if base is None:
            continue
        peak_rss = base.rss_bytes
        for sample in trace.samples:
            point = sample.get(role)
            if point is not None:
                peak_rss = max(peak_rss, point.rss_bytes)
        delta = peak_rss - base.rss_bytes
        diagnostics["per_role"].setdefault(role, {})["rss_peak_delta"] = delta
        if delta > peak_limit:
            violations.append(Violation(
                scope=role, metric="rss_peak_delta",
                observed=delta, limit=peak_limit))

    unknown_combined = 0
    for role in ("backend", "gateway"):
        base = baseline.get(role)
        if base is None:
            continue
        delta, numbers = _peak_fd_delta(base, trace, role, frozenset({FdKind.UNKNOWN}))
        if delta > 0:
            diagnostics["per_role"][role]["unknown_peak_fds"] = sorted(numbers)
        unknown_combined += max(delta, 0)
    diagnostics["unknown_fd_peak_delta_combined"] = unknown_combined

    for role in ("backend", "gateway"):
        base = baseline.get(role)
        if base is None:
            continue
        for kind in DIAGNOSTIC_KINDS:
            delta, numbers = _peak_fd_delta(base, trace, role, frozenset({kind}))
            if delta > 0:
                diagnostics["per_role"][role].setdefault("diagnostic_peak_fds", {})[kind.value] = {
                    "delta": delta, "fds": sorted(numbers)}
    return violations, diagnostics


def evaluate_cleanup_policy(
    baseline: Mapping[str, ProcessSnapshot], trace: ResourceTrace,
) -> tuple[list[Violation], dict]:
    """Judge the post-run end state after the settle window."""
    violations: list[Violation] = []
    diagnostics: dict = {"per_role": {}}

    combined_end = 0
    for role in ("backend", "gateway"):
        base = baseline.get(role)
        end = _end_snapshot(trace, role)
        if base is None or end is None:
            continue
        diagnostics["per_role"][role] = {
            "total_fd_end_delta": end.total_fd_count - base.total_fd_count,
            "thread_end_delta": end.thread_count - base.thread_count,
            "rss_end_delta": end.rss_bytes - base.rss_bytes,
        }
        if end.total_fd_count - base.total_fd_count > TOTAL_FD_END_LIMIT:
            violations.append(Violation(
                scope=role, metric="total_fd_end_delta",
                observed=end.total_fd_count - base.total_fd_count,
                limit=TOTAL_FD_END_LIMIT))
        if end.thread_count - base.thread_count > THREAD_END_LIMIT:
            violations.append(Violation(
                scope=role, metric="thread_end_delta",
                observed=end.thread_count - base.thread_count,
                limit=THREAD_END_LIMIT))
        rss_end_limit = RSS_LIMITS[role][1]
        if end.rss_bytes - base.rss_bytes > rss_end_limit:
            violations.append(Violation(
                scope=role, metric="rss_end_delta",
                observed=end.rss_bytes - base.rss_bytes, limit=rss_end_limit))
        kinds = frozenset({FdKind.PROVIDER_UDS_CLIENT, FdKind.PROVIDER_UDS_ACCEPTED})
        combined_end += end.count(kinds) - base.count(kinds)

    diagnostics["provider_uds_session_fd_end_delta_combined"] = combined_end
    if combined_end > UDS_SESSION_COMBINED_END_LIMIT:
        violations.append(Violation(
            scope="backend+gateway",
            metric="provider_uds_session_fd_end_delta_combined",
            observed=combined_end, limit=UDS_SESSION_COMBINED_END_LIMIT))
    return violations, diagnostics


def build_qualification_report(
    baseline: Mapping[str, ProcessSnapshot], trace: ResourceTrace,
) -> GateReport:
    """Combine peak + cleanup verdicts with fail-closed measurement status."""
    peak_violations, peak_diagnostics = evaluate_peak_policy(baseline, trace)
    cleanup_violations, cleanup_diagnostics = evaluate_cleanup_policy(baseline, trace)
    unknown_peak = peak_diagnostics.get("unknown_fd_peak_delta_combined", 0)
    if unknown_peak > 0:
        measurement_status = "inconclusive"
        qualification_status = "not_evaluated"
    elif not trace.samples:
        measurement_status = "invalid"
        qualification_status = "not_evaluated"
    elif peak_violations or cleanup_violations:
        measurement_status = "valid"
        qualification_status = "failed"
    else:
        measurement_status = "valid"
        qualification_status = "passed"
    return GateReport(
        metric_schema=METRIC_SCHEMA,
        measurement_status=measurement_status,
        qualification_status=qualification_status,
        peak_policy=peak_violations,
        cleanup_policy=cleanup_violations,
        diagnostics={"peak": peak_diagnostics, "cleanup": cleanup_diagnostics},
    )
