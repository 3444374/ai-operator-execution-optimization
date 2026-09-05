"""Pure metric-schema-v2 gate policies for PostgreSQL resource qualification.

Consumes classified ``ResourceTrace`` ticks from
``observability.process_resources`` and returns structured verdicts. No
process management, no /proc access, no I/O: every function is a pure
mapping so unit tests can drive all red/green/inconclusive paths with
synthetic traces.

Schema registered in experiments/plans/postgresql_semmap_generation_contract.md
§8.4.2 (2026-09-04, frozen before any v2 rerun; pre-run static-review
correction pending registration). Thresholds are unchanged from the
user-confirmed §8.4.1 values; only the measurement implementation
differs from v1.

Peak semantics: the maximum SIMULTANEOUS same-tick delta. The union of
fd numbers observed across the run is diagnostic data only and may
never feed a peak threshold — sequential fd reuse must count as one.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from src.observability.process_resources.model import (
    FdKind,
    FdIdentity,
    ProcessSnapshot,
    ResourceTrace,
    SampleTick,
    SnapshotStatus,
)

METRIC_SCHEMA = "semloom.pg.resource.v2.1"
IMPLEMENTATION_REVISION = "phase-lifecycle-2"

MIB = 1024 * 1024

RSS_PEAK_LIMITS = {"backend": 16 * MIB, "gateway": 32 * MIB}
RSS_END_LIMITS = {"backend": 8 * MIB, "gateway": 16 * MIB}
UDS_CLIENT_PEAK_LIMIT = 1
UDS_ACCEPTED_PEAK_LIMIT = 1
UDS_SESSION_COMBINED_PEAK_LIMIT = 2
UDS_SESSION_COMBINED_END_LIMIT = 0
TOTAL_FD_END_LIMIT = 0
THREAD_END_LIMIT = 0

PROVIDER_SESSION_KINDS = frozenset({
    FdKind.PROVIDER_UDS_CONNECTED,
    FdKind.PROVIDER_UDS_LISTENER,
})

DIAGNOSTIC_KINDS = (
    FdKind.POSTGRES_CLIENT_SOCKET,
    FdKind.RELATION_FILE,
    FdKind.TOAST_RELATION_FILE,
    FdKind.POSTGRES_TEMP_FILE,
    FdKind.REGULAR_FILE_OTHER,
    FdKind.PIPE,
    FdKind.EVENTFD_OR_ANON_INODE,
    FdKind.SOCKET_OTHER,
    FdKind.UNBOUND_UNIX_SOCKET,
)

_LEGAL_STATUSES = frozenset({
    ("valid", "passed"),
    ("valid", "failed"),
    ("inconclusive", "not_evaluated"),
    ("invalid", "not_evaluated"),
})


def compose_status(*statuses: tuple[str, str]) -> tuple[str, str] | None:
    """Compose (measurement, qualification) pairs under strict precedence.

    Precedence: invalid > inconclusive > valid-failed > valid-passed. The
    result is always one of the four legal combinations; an input that
    would compose into an illegal pair (e.g. inconclusive+passed) returns
    None so callers can reject it instead of silently coercing.
    """
    if not statuses or any(pair not in _LEGAL_STATUSES for pair in statuses):
        return None
    if any(m == "invalid" for m, _ in statuses):
        return ("invalid", "not_evaluated")
    if any(m == "inconclusive" for m, _ in statuses):
        return ("inconclusive", "not_evaluated")
    if any(q == "failed" for _, q in statuses):
        return ("valid", "failed")
    return ("valid", "passed")


@dataclass(frozen=True)
class Violation:
    scope: str
    metric: str
    observed: object
    limit: object
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
        return (self.measurement_status == "valid"
                and self.qualification_status == "passed")


def _ticks(trace: ResourceTrace) -> tuple[SampleTick, ...]:
    return trace.ticks


def _count(snapshot: ProcessSnapshot, kinds) -> int | None:
    return snapshot.count(kinds)


def evaluate_peak_policy(
    baseline: Mapping[str, ProcessSnapshot], trace: ResourceTrace,
) -> tuple[list[Violation], dict]:
    """Judge the maximum SIMULTANEOUS deltas across same-tick observations.

    backend provider-client peak and gateway provider-accepted peak are
    each max-over-ticks of the per-tick delta; the combined peak is the
    max-over-ticks of the per-tick SUM. Cross-tick per-role maxima are
    never added together.
    """
    violations: list[Violation] = []
    diagnostics: dict = {"per_role": {}, "ticks": len(trace.ticks)}
    if not trace.ticks:
        return violations, diagnostics

    client_kind = FdKind.PROVIDER_UDS_CONNECTED
    accepted_kind = FdKind.PROVIDER_UDS_CONNECTED
    base_backend = baseline.get("backend")
    base_gateway = baseline.get("gateway")
    base_client = _count(base_backend, client_kind) if base_backend else 0
    base_accepted = _count(base_gateway, accepted_kind) if base_gateway else 0

    client_peak = 0
    accepted_peak = 0
    combined_peak = 0
    unknown_peak = 0
    peak_fds: dict[str, set[int]] = {"backend": set(), "gateway": set()}

    for tick in trace.ticks:
        backend = tick.processes.get("backend")
        gateway = tick.processes.get("gateway")
        client_now = _count(backend, client_kind) if backend else 0
        accepted_now = _count(gateway, accepted_kind) if gateway else 0
        if backend is not None and backend.fds is not None:
            peak_fds["backend"].update(
                backend.fd_numbers(client_kind))
        if gateway is not None and gateway.fds is not None:
            peak_fds["gateway"].update(gateway.fd_numbers(accepted_kind))
        client_delta = (client_now - base_client) if client_now is not None else 0
        accepted_delta = (accepted_now - base_accepted) if accepted_now is not None else 0
        client_peak = max(client_peak, client_delta)
        accepted_peak = max(accepted_peak, accepted_delta)
        combined_peak = max(combined_peak, client_delta + accepted_delta)
        for role, snapshot in tick.processes.items():
            if snapshot.status is not SnapshotStatus.VALID:
                continue
            base = baseline.get(role)
            if base is None or base.fds is None or snapshot.fds is None:
                continue
            unknown_peak = max(unknown_peak, len(
                snapshot.unknown_identities() - base.unknown_identities()))

    diagnostics["per_role"]["backend"] = {
        "provider_uds_peak_delta": client_peak,
        "provider_uds_peak_fds_observed": sorted(peak_fds["backend"])}
    diagnostics["per_role"]["gateway"] = {
        "provider_uds_peak_delta": accepted_peak,
        "provider_uds_peak_fds_observed": sorted(peak_fds["gateway"])}
    diagnostics["provider_uds_session_fd_peak_delta_combined"] = combined_peak
    diagnostics["unknown_fd_peak_delta"] = unknown_peak

    if client_peak > UDS_CLIENT_PEAK_LIMIT:
        violations.append(Violation(
            scope="backend", metric="provider_uds_client_fd_peak_delta",
            observed=client_peak, limit=UDS_CLIENT_PEAK_LIMIT,
            detail={"fds_observed": sorted(peak_fds["backend"])}))
    if accepted_peak > UDS_ACCEPTED_PEAK_LIMIT:
        violations.append(Violation(
            scope="gateway", metric="provider_uds_accepted_fd_peak_delta",
            observed=accepted_peak, limit=UDS_ACCEPTED_PEAK_LIMIT,
            detail={"fds_observed": sorted(peak_fds["gateway"])}))
    if combined_peak > UDS_SESSION_COMBINED_PEAK_LIMIT:
        violations.append(Violation(
            scope="backend+gateway",
            metric="provider_uds_session_fd_peak_delta_combined",
            observed=combined_peak, limit=UDS_SESSION_COMBINED_PEAK_LIMIT))

    for role, limit in RSS_PEAK_LIMITS.items():
        base = baseline.get(role)
        peak_rss = base.rss_bytes if base else None
        for tick in trace.ticks:
            snapshot = tick.processes.get(role)
            if snapshot is not None and snapshot.rss_bytes is not None:
                if peak_rss is None:
                    peak_rss = snapshot.rss_bytes
                else:
                    peak_rss = max(peak_rss, snapshot.rss_bytes)
        if peak_rss is not None and base is not None and base.rss_bytes is not None:
            delta = peak_rss - base.rss_bytes
            diagnostics["per_role"].setdefault(role, {})["rss_peak_delta"] = delta
            if delta > limit:
                violations.append(Violation(
                    scope=role, metric="rss_peak_delta",
                    observed=delta, limit=limit))

    for role in baseline:
        base = baseline.get(role)
        if base is None or base.fds is None:
            continue
        for kind in DIAGNOSTIC_KINDS:
            peak = 0
            for tick in trace.ticks:
                snapshot = tick.processes.get(role)
                if snapshot is None or snapshot.fds is None:
                    continue
                now = sum(1 for item in snapshot.fds if item.kind is kind)
                base_count = sum(1 for item in base.fds if item.kind is kind)
                peak = max(peak, now - base_count)
            if peak > 0:
                diagnostics["per_role"].setdefault(
                    role, {}).setdefault("diagnostic_peak_deltas", {})[kind.value] = peak
        # total_fd_peak_delta: registered no-gate diagnostic. Computed from
        # same-tick counts like every other peak, never from fd-number
        # history unions.
        base_total = len(base.fds)
        total_peak = 0
        for tick in trace.ticks:
            snapshot = tick.processes.get(role)
            if snapshot is not None and snapshot.fds is not None:
                total_peak = max(total_peak, len(snapshot.fds) - base_total)
        diagnostics["per_role"].setdefault(
            role, {})["total_fd_peak_delta"] = total_peak
    return violations, diagnostics


def evaluate_cleanup_policy(
    baseline: Mapping[str, ProcessSnapshot], trace: ResourceTrace,
) -> tuple[list[Violation], dict]:
    """Judge the post-run end state after the settle window.

    FD and thread counts must return to baseline EXACTLY: delta != 0 is a
    violation in both directions, because a negative delta also proves the
    resource did not return to the baseline state. A required role absent
    from the final tick is a hole, not a zero: absent roles are flagged so
    every end-state check is backed by an actual observation. New UNKNOWN
    descriptors relative to baseline make the state non-qualifiable the
    same way they do in the peak phase — an unclassified resource may not
    pass by staying out of every classified bucket.
    """
    violations: list[Violation] = []
    diagnostics: dict = {"per_role": {}}
    if not trace.ticks:
        return violations, diagnostics
    final = trace.ticks[-1]

    combined_end = 0
    for role in baseline:
        base = baseline.get(role)
        end = final.processes.get(role)
        if base is None:
            continue
        if end is None:
            # The final tick never observed this role: report the hole
            # instead of silently skipping all of its end-state checks.
            diagnostics["per_role"][role] = {"role_absent_in_final_tick": True}
            violations.append(Violation(
                scope=role, metric="role_missing_in_final_tick",
                observed=None, limit="present"))
            continue
        role_diagnostics: dict = {}
        if end.status is not SnapshotStatus.VALID or base.status is not SnapshotStatus.VALID:
            role_diagnostics["snapshot_status"] = (
                end.status.value, base.status.value)
        else:
            if end.total_fd_count is not None and base.total_fd_count is not None:
                delta = end.total_fd_count - base.total_fd_count
                role_diagnostics["total_fd_end_delta"] = delta
                if delta != TOTAL_FD_END_LIMIT:
                    violations.append(Violation(
                        scope=role, metric="total_fd_end_delta",
                        observed=delta, limit=TOTAL_FD_END_LIMIT))
            if end.fds is not None and base.fds is not None:
                before = {fd.identity for fd in base.fds}
                after = {fd.identity for fd in end.fds}
                if before != after:
                    violations.append(Violation(
                        scope=role, metric="fd_identity_end_mismatch", observed=len(after - before),
                        limit=0, detail={"added": sorted(after - before), "removed": sorted(before - after)}))
            if end.thread_count is not None and base.thread_count is not None:
                delta = end.thread_count - base.thread_count
                role_diagnostics["thread_end_delta"] = delta
                if delta != THREAD_END_LIMIT:
                    violations.append(Violation(
                        scope=role, metric="thread_end_delta",
                        observed=delta, limit=THREAD_END_LIMIT))
            if end.rss_bytes is not None and base.rss_bytes is not None:
                delta = end.rss_bytes - base.rss_bytes
                role_diagnostics["rss_end_delta"] = delta
                if delta > RSS_END_LIMITS[role]:
                    violations.append(Violation(
                        scope=role, metric="rss_end_delta",
                        observed=delta, limit=RSS_END_LIMITS[role]))
            end_provider = _count(end, PROVIDER_SESSION_KINDS) or 0
            base_provider = _count(base, PROVIDER_SESSION_KINDS) or 0
            combined_end += end_provider - base_provider
            role_diagnostics["provider_session_end_delta"] = (
                end_provider - base_provider)
            if end.fds is not None and base.fds is not None:
                new_unknown = sorted(
                    end.unknown_identities() - base.unknown_identities())
                if new_unknown:
                    role_diagnostics["unknown_fd_identities_new"] = new_unknown
        diagnostics["per_role"][role] = role_diagnostics

    diagnostics["provider_uds_session_fd_end_delta_combined"] = combined_end
    if combined_end != UDS_SESSION_COMBINED_END_LIMIT:
        violations.append(Violation(
            scope="backend+gateway",
            metric="provider_uds_session_fd_end_delta_combined",
            observed=combined_end, limit=UDS_SESSION_COMBINED_END_LIMIT))
    return violations, diagnostics


def validate_measurement(baseline, trace, *, required_roles=("backend", "gateway")):
    """Check required data and process identities independently of policy values."""
    if not required_roles or not trace.ticks:
        return "invalid", ["no_required_roles_or_ticks"]
    problems, invalid = [], False
    if set(baseline) != set(required_roles):
        problems.append("baseline_roles_missing_or_extra")
        invalid = True
    for role in required_roles:
        base = baseline.get(role)
        if base is None:
            continue
        if (base.status is not SnapshotStatus.VALID or base.fds is None
                or base.rss_bytes is None or base.thread_count is None
                or base.process_start_time_ticks is None):
            problems.append(f"{role}_baseline_invalid")
            invalid = True
    for index, tick in enumerate(trace.ticks):
        for role in required_roles:
            snap, base = tick.processes.get(role), baseline.get(role)
            if snap is None:
                problems.append(f"tick{index}_{role}_missing")
                continue
            if snap.status is SnapshotStatus.INVALID:
                problems.append(f"tick{index}_{role}_invalid")
                invalid = True
            elif snap.status is SnapshotStatus.PARTIAL:
                problems.append(f"tick{index}_{role}_partial")
            if (snap.fds is None or snap.rss_bytes is None or snap.thread_count is None
                    or snap.process_start_time_ticks is None):
                problems.append(f"tick{index}_{role}_unreadable")
                invalid = True
            if base is not None and snap.process_identity != base.process_identity:
                problems.append(f"tick{index}_{role}_process_replaced")
                invalid = True
        if not tick.unix_table_valid or tick.errors:
            problems.append(f"tick{index}_unix_table_unavailable")
    return ("invalid" if invalid else "inconclusive" if problems else "valid"), problems


def evaluate_session_drain(events: list[dict]) -> tuple[list[Violation], dict]:
    """Judge the gateway session drain from the observer's event log.

    Pure event replay: every session_start must have a matching
    session_end (active sessions == 0). The current gateway/observer
    protocol has no separate task-completion event — the gateway drains
    its session task queue synchronously before session_end, so an
    ended session is the protocol's completion evidence; per-task
    delivery is covered by the case-level task-count/digest correctness
    checks instead. Undelivered tasks would surface as a missing
    session_end (still-open session) here.
    """
    from .provider_session_attribution import session_windows
    violations = []
    try:
        windows = session_windows(events)
    except (ValueError, KeyError, TypeError) as error:
        violations.append(Violation("gateway", "session_events_incomplete", type(error).__name__, "complete"))
        return violations, {"active_sessions": None, "event_integrity": "invalid"}
    starts = {e["session_id"] for e in events if e.get("event") == "session_start"}
    return violations, {"active_sessions": 0, "sessions": len(windows),
                        "session_ids": sorted(starts),
                        "tasks_recorded": sum(e.get("event") == "task" for e in events),
                        "event_integrity": "valid",
                        "success_inferred_from_session_end": False}


def build_qualification_report(
    baseline: Mapping[str, ProcessSnapshot], trace: ResourceTrace,
    *,
    phase: str = "combined",
    required_roles: tuple[str, ...] = ("backend", "gateway"),
) -> GateReport:
    """Compose measurement validity with the phase-appropriate policies.

    ``phase='stress'`` judges peaks only; ``phase='cleanup'`` judges the
    end state only; ``phase='combined'`` judges both. The stress trace's
    final tick is never allowed to act as a cleanup verdict — that
    conflation previously produced contradictory failed-with-no-violations
    reports.
    """
    if phase not in ("stress", "cleanup", "combined"):
        raise ValueError("unknown_policy_phase")
    measurement, problems = validate_measurement(baseline, trace, required_roles=required_roles)
    peak_violations: list[Violation] = []
    cleanup_violations: list[Violation] = []
    diagnostics: dict = {"measurement_problems": problems, "phase": phase}
    # Complete valid observations can prove violations even when another
    # observation leaves the overall measurement inconclusive. Keep those
    # failures; a missing interval cannot erase an observed overload.
    valid_ticks = tuple(t for t in trace.ticks
        if validate_measurement(baseline, ResourceTrace(baseline, (t,)),
                                required_roles=required_roles)[0] == "valid")
    if valid_ticks and phase in ("stress", "combined"):
        peak_violations, peak_diagnostics = evaluate_peak_policy(
            baseline, ResourceTrace(baseline, valid_ticks))
        diagnostics["peak"] = peak_diagnostics
        diagnostics["peak"]["coverage"] = "complete" if measurement == "valid" else "valid_observations_only"
    if trace.ticks and phase in ("cleanup", "combined"):
        cleanup_violations, cleanup_diagnostics = evaluate_cleanup_policy(baseline, trace)
        diagnostics["cleanup"] = cleanup_diagnostics
    unknown_peak = diagnostics.get("peak", {}).get("unknown_fd_peak_delta", 0)
    unknown_cleanup_new = [
        role for role, diag in diagnostics.get("cleanup", {})
        .get("per_role", {}).items()
        if diag.get("unknown_fd_identities_new")]
    if measurement == "invalid":
        composed = ("invalid", "not_evaluated")
    elif measurement == "inconclusive" or unknown_peak > 0 or unknown_cleanup_new:
        composed = ("inconclusive", "not_evaluated")
    elif measurement == "invalid":
        composed = ("invalid", "not_evaluated")
    elif peak_violations or cleanup_violations:
        composed = ("valid", "failed")
    else:
        composed = ("valid", "passed")
    return GateReport(
        metric_schema=METRIC_SCHEMA,
        measurement_status=composed[0],
        qualification_status=composed[1],
        peak_policy=peak_violations,
        cleanup_policy=cleanup_violations,
        diagnostics=diagnostics)
