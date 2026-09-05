"""Capture one owned experiment phase and assess its separate evidence windows.

The injected seams are a sampler, operation, and event reader. All real phases
use this same path; controlled tests replace only external processes/observations.
"""
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import time

from src.observability.process_resources.model import ResourceTrace, SampleTick, SnapshotStatus
from src.observability.process_resources.recorder import (
    acquire_stable_baseline, capture_error, persist_lifecycles,
    persist_operation_outcome, persist_trace, record_operation, write_atomic)
from .provider_session_attribution import (
    attribute_provider_sessions, reclassify_clients, residual_provider_fds, session_windows)
from .resource_qualification import (
    build_qualification_report, compose_status, evaluate_cleanup_policy,
    evaluate_session_drain, validate_measurement)
from .resource_lifecycle import PhaseResult, project_report


def save_json(path, value):
    write_atomic(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def hashes(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_file() and not path.is_symlink() and path.name != "SHA256SUMS.json":
            with path.open("rb") as handle:
                digest = hashlib.file_digest(handle, "sha256").hexdigest()
            result[str(path.relative_to(root))] = digest
    return result


def checkpoint(root, recorded, phase):
    persist_trace(root / "operation", recorded.trace)
    persist_lifecycles(root / "operation", recorded.trace)
    persist_operation_outcome(root / "operation", recorded, phase)


def cleanup_settle(sampler, baseline, spec, events, *, roles, attribution=None, on_interrupt=None):
    """Wait for a stable valid end window, retaining every attempted tick."""
    ticks, problems, stable = [], [], 0
    deadline = time.monotonic() + spec.cleanup_timeout_seconds
    try:
        while time.monotonic() < deadline:
            ns = time.monotonic_ns()
            try:
                tick = sampler.sample_all(ns)
            except Exception as error:
                tick = SampleTick(ns, False, {}, errors=(type(error).__name__,))
            ticks.append(tick)
            current = ResourceTrace(baseline, (tick,))
            measurement, _ = validate_measurement(baseline, current, required_roles=roles)
            violations, _ = evaluate_cleanup_policy(baseline, current)
            try:
                drain, _ = evaluate_session_drain(events())
            except Exception:
                drain = ["events_unreadable"]
            valid = (measurement == "valid" and not violations and not drain
                     and not residual_provider_fds(current, attribution))
            stable = stable + 1 if valid else 0
            if stable >= spec.cleanup_samples:
                return ResourceTrace(baseline, tuple(ticks)), True
            time.sleep(spec.cleanup_interval_seconds)
    except (KeyboardInterrupt, SystemExit):
        if on_interrupt is not None:
            on_interrupt(ResourceTrace(baseline, tuple(ticks)))
        raise
    return ResourceTrace(baseline, tuple(ticks)), False


def execute_phase(*, root, phase, spec, sampler, operation, events,
                  roles=("backend", "gateway"), require_sessions=True,
                  expected_sqlstate=None, expected_tasks=1, expected_sessions=1,
                  expected_digest=None, check_result=None, extra_checks=None):
    """Return a final PhaseResult only after operation and cleanup are persisted."""
    root.mkdir()
    save_json(root / "progress.json", {"phase": phase, "state": "running"})
    baseline = None
    observed = []
    try:
        def quiet(tick):
            violations, _ = evaluate_session_drain(events())
            if violations:
                return False
            if "gateway" in roles:
                from src.observability.process_resources.model import FdKind
                return tick.processes["gateway"].count(FdKind.PROVIDER_UDS_LISTENER) == 1
            return True
        baseline, observed = acquire_stable_baseline(
            sampler, required_roles=roles, required_consecutive=spec.baseline_samples,
            interval_seconds=spec.baseline_interval_seconds,
            timeout_seconds=spec.baseline_timeout_seconds, extra_stability=quiet,
            on_interrupt=lambda ticks: persist_trace(root / "baseline_interrupted", ResourceTrace({}, ticks)))
        persist_trace(root / "baseline", ResourceTrace(
            {} if baseline is None else baseline.baseline, tuple(observed)))
        if baseline is None:
            result = PhaseResult(phase, "completed", "invalid", "not_evaluated",
                                 ("stable_baseline_unavailable",))
            return finish_phase(root, result, spec)
        # Session events before this phase belong to warmup or an earlier phase.
        cursor = len(events())
        phase_events = lambda: events()[cursor:]
        recorded = record_operation(
            sampler, operation, sample_seconds=spec.sample_seconds,
            baseline=baseline.baseline,
            on_checkpoint=lambda value: checkpoint(root, value, phase))
        cleanup, settled = cleanup_settle(
            sampler, baseline.baseline, spec, phase_events, roles=roles,
            on_interrupt=lambda trace: persist_trace(root / "cleanup_interrupted", trace))
        persist_trace(root / "cleanup", cleanup)
        persist_lifecycles(root / "cleanup", cleanup)
        event_values = phase_events()
        save_json(root / "session_events.json", event_values)
        problems, failures = [], []
        attribution = None
        if require_sessions:
            try:
                windows = session_windows(event_values)
                answer = attribute_provider_sessions(
                    backend_pid=baseline.baseline["backend"].pid,
                    baseline=baseline.baseline, trace=recorded.trace, windows=windows)
                attribution = answer.attribution
                problems.extend(answer.problems)
            except (ValueError, KeyError, TypeError) as error:
                problems.append(f"session_event_integrity:{type(error).__name__}")
        save_json(root / "attribution.json", {"attribution": attribution, "problems": problems})
        derived = (recorded.trace if attribution is None else
                   reclassify_clients(recorded.trace, attribution))
        if attribution is not None:
            persist_trace(root / "attributed_operation", derived)
        peak = build_qualification_report(baseline.baseline, derived, phase="stress", required_roles=roles)
        end = build_qualification_report(baseline.baseline, cleanup, phase="cleanup", required_roles=roles)
        statuses = [(peak.measurement_status, peak.qualification_status),
                    (end.measurement_status, end.qualification_status)]
        problems.extend(peak.diagnostics["measurement_problems"])
        problems.extend(end.diagnostics["measurement_problems"])
        if recorded.sampling_error is not None:
            problems.append("operation_sampling_failed")
            statuses.append(("invalid", "not_evaluated"))
        if require_sessions and attribution is None:
            statuses.append(("inconclusive", "not_evaluated"))
        drain, drain_diag = evaluate_session_drain(event_values)
        if drain:
            problems.append("session_drain_invalid")
            statuses.append(("inconclusive", "not_evaluated"))
        residuals = residual_provider_fds(cleanup, attribution)
        if residuals:
            failures.append({"metric": "provider_identity_residual", "resources": residuals})
        if not settled:
            failures.append({"metric": "cleanup_deadline", "stable_samples_required": spec.cleanup_samples})
        observed_state = recorded.operation_error.sqlstate if recorded.operation_error else None
        if expected_sqlstate is not None:
            if observed_state != expected_sqlstate:
                failures.append({"metric": "sqlstate_contract", "expected": expected_sqlstate,
                                 "observed": observed_state})
        elif recorded.operation_error is not None:
            failures.append({"metric": "operation_failed", **asdict(recorded.operation_error)})
        elif check_result is not None:
            failures.extend(check_result(recorded.result))
        if require_sessions:
            tasks = [e for e in event_values if e["event"] == "task"]
            sessions = [e for e in event_values if e["event"] == "session_start"]
            if len(tasks) != expected_tasks or len(sessions) != expected_sessions:
                failures.append({"metric": "event_counts", "tasks": len(tasks), "sessions": len(sessions),
                                 "expected_tasks": expected_tasks, "expected_sessions": expected_sessions})
            if expected_digest is not None and any(e.get("payload_digest") != expected_digest for e in tasks):
                failures.append({"metric": "task_digest"})
            # A session_end records closure, never proves delivery or model success.
            complete = [e for e in event_values if e["event"] == "task_complete"]
            if expected_sqlstate is None and len(complete) != expected_tasks:
                failures.append({"metric": "completion_count", "observed": len(complete), "expected": expected_tasks})
        if extra_checks is not None:
            failures.extend(extra_checks())
        failures.extend(asdict(v) for v in peak.peak_policy + end.cleanup_policy)
        if failures:
            statuses.append(("valid", "failed"))
        status = compose_status(*statuses) or ("invalid", "not_evaluated")
        safe = (status[0] == "valid" and end.passed and settled and not drain and not residuals
                and recorded.sampling_error is None)
        result = PhaseResult(phase, "completed", *status, tuple(problems), tuple(failures), safe,
            tuple(hashes(root)), {"peak": peak.diagnostics, "cleanup": end.diagnostics,
                                 "session_drain": drain_diag, "settled": settled})
        return finish_phase(root, result, spec)
    except (KeyboardInterrupt, SystemExit):
        save_json(root / "interrupted.json", {"phase": phase, "reason": "interrupted"})
        raise
    except Exception as error:
        # Filesystem failures still propagate if the final evidence cannot be saved.
        result = PhaseResult(phase, "completed", "invalid", "not_evaluated",
                             (f"phase_error:{type(error).__name__}",))
        return finish_phase(root, result, spec)


def finish_phase(root, result, spec):
    assessment = result.assessment
    value = {**asdict(result), "assessment": None if assessment is None else dict(zip(
        ("measurement_status", "qualification_status"), assessment))}
    save_json(root / "phase_report.json", project_report(value, spec.mode))
    save_json(root / "progress.json", {"phase": result.phase, "state": result.state})
    save_json(root / "SHA256SUMS.json", hashes(root))
    return result
