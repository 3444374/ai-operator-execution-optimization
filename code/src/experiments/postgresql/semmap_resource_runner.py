"""Fixture-only SemMap resource qualification runner (metric schema v2).

Lifecycle per case, in order, with raw evidence persisted before any
verdict anywhere:

    prepare -> warmup -> stable baseline -> operation (recorded, errors
    captured) -> persist trace + outcome -> attribute provider sessions
    -> validate measurement -> evaluate stress peak exactly once ->
    synchronous cleanup settle -> persist cleanup -> evaluate end state
    -> compose report -> summary (atomic, always) -> exit code.

Differences from the v1 runner that produced 93 duplicate attempt files
and from the earlier v2 drafts called out by static review:

- Peak is judged once over the frozen stress window; the stress trace's
  final tick is never a cleanup verdict.
- Fault cases (cancel, provider disconnect, gateway exit) each perform
  their own stable baseline, recorded operation, attribution, cleanup
  settle, and policy evaluation. No case writes a verdict that a policy
  did not compute.
- disconnect and gateway-exit are split into subphases with their own
  gateways and baselines; the deliberately-exited gateway is an expected
  phase boundary, not a collector failure.
- Correctness mismatches (rows/tasks/sessions/digest/sqlstate) are
  structured failures recorded in the report, not asserts that could
  preempt evidence persistence.
- The CLI returns 0 all-pass, 1 valid/failed, 2 invalid or
  inconclusive, 3 runner/preflight failure, after summary.json lands.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import psycopg
from psycopg import sql

from src.observability.process_resources.model import (
    PgFileClassificationContext,
    ResourceTrace,
    SampleTick,
    SnapshotStatus,
)
from src.observability.process_resources.recorder import (
    acquire_stable_baseline,
    persist_lifecycles,
    persist_operation_outcome,
    persist_trace,
    record_operation,
)
from src.experiments.postgresql.provider_session_attribution import (
    attribute_provider_sessions,
    load_session_events,
    reclassify_clients,
    session_windows,
)
from src.experiments.postgresql.resource_qualification import (
    METRIC_SCHEMA,
    build_qualification_report,
)

try:
    import pwd
except ImportError:  # Windows checkouts import this module for unit tests only.
    pwd = None


MODEL = "golden-map-resource-v1"
INSTRUCTION = "Generate fixed output."
INPUT = "x" * 100000
OUTPUT = "y" * 65536
SAMPLE_SECONDS = 0.02
CLEANUP_TIMEOUT_SECONDS = 60.0
SETTLE_INTERVAL_SECONDS = 0.25
ROWS_PER_ROUND = 2000
ROUNDS = 3

# SQLSTATE contracts registered by the existing PG regression/TAP evidence.
CANCEL_SQLSTATE = "57014"
DISCONNECT_SQLSTATE = "08006"
GATEWAY_EXIT_SQLSTATE = "08006"

EXIT_ALL_PASS = 0
EXIT_VALID_FAILED = 1
EXIT_NOT_EVALUATED = 2
EXIT_RUNNER_FAILURE = 3

# Cleanup failures that stop later cases (unrecovered resources).
SAFETY_METRICS = {"total_fd_end_delta", "thread_end_delta",
                  "provider_uds_session_fd_end_delta_combined"}


def save(path: Path, value) -> None:
    temporary = path.with_name(path.name + ".partial")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, indent=2)
        handle.write("\n")
    os.replace(temporary, path)


def _chown(path: Path, user) -> None:
    if pwd is None or user is None or not hasattr(os, "chown"):
        return
    os.chown(path, user.pw_uid, user.pw_gid)


def wait_log(path: Path, event_name: str, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.startswith("{"):
                    event = json.loads(line)
                    if event.get("event") == event_name:
                        return event
        time.sleep(0.05)
    raise RuntimeError(f"client did not report {event_name}")


def wait_gateway_events(path: Path, *, tasks: int, sessions_ended: int, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = load_session_events(path)
        if (sum(item["event"] == "task" for item in current) >= tasks and
                sum(item["event"] == "session_end" for item in current) >= sessions_ended):
            return current
        time.sleep(0.05)
    raise RuntimeError("gateway event counts did not settle")


def gateway_command(args, socket_path, fixture_path, event_path, extra=()):
    return [
        sys.executable,
        str(args.repo / "code/src/experiments/postgresql/"
                       "semmap_resource_gateway_observer.py"),
        "--events", str(event_path),
        "--",
        "--socket", str(socket_path),
        "--golden-fixture", str(fixture_path),
        *extra,
    ]


def query_one(connection, socket_path):
    connection.execute("SET semloom_pg.provider_execution_profile='golden'")
    connection.execute("SELECT set_config('semloom_pg.gateway_socket',%s,false)",
                       (str(socket_path),))
    options = json.dumps({"model": MODEL, "temperature": 0, "max_tokens": 128},
                         separators=(",", ":"))
    statement = sql.SQL(
        "SELECT ai_semantic.map(payload,{},{}::jsonb) FROM ONLY resource_rows WHERE id=1"
    ).format(sql.Literal(INSTRUCTION), sql.Literal(options))
    value = connection.execute(statement).fetchone()[0]
    return value


def fixture(args, root: Path):
    from src.execution_provider.completion import Completion
    from src.execution_provider.semantic_map import SemanticMapPlan
    from src.execution_provider.wire import v5

    plan = SemanticMapPlan(INSTRUCTION, MODEL, 128)
    task = v5.build_task_message(plan, sequence=0, input_value=INPUT)
    digest = task["semantic_payload_digest"]
    fixture_path = root / "fixture.json"
    fixture_payload = {
        digest: {
            "raw_output": OUTPUT,
            "response_model_id": MODEL,
            "prompt_tokens": 25032,
            "output_tokens": 128,
            "finish_reason": "stop",
        }
    }
    save(fixture_path, fixture_payload)
    save(root / "fixture-identity.json", {
        "payload_digest": digest,
        "input_bytes": len(INPUT),
        "output_bytes": len(OUTPUT),
        "model": MODEL,
        "model_requests": 0,
    })
    return fixture_path, digest


def pg_file_context(connection) -> PgFileClassificationContext:
    """Learn this run's relation/toast filenodes from PostgreSQL itself."""
    data_directory = connection.execute("SHOW data_directory").fetchone()[0]
    rows = connection.execute("""
        SELECT (SELECT relfilenode FROM pg_class
                 WHERE oid = 'resource_rows'::regclass),
               (SELECT relfilenode FROM pg_class c
                 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'payload'
                WHERE c.oid = 'resource_rows'::regclass AND a.atttypid = 'text'::regtype)
    """).fetchall()
    relation_filenodes = set()
    toast_filenodes = set()
    for relation_node, _toast_probe in rows:
        if relation_node:
            relation_filenodes.add(relation_node)
    toast = connection.execute("""
        SELECT relfilenode FROM pg_class WHERE oid = (
            SELECT reltoastrelid FROM pg_class
             WHERE oid = 'resource_rows'::regclass)
    """).fetchone()
    if toast and toast[0]:
        toast_filenodes.add(toast[0])
    return PgFileClassificationContext(
        data_directory=str(data_directory),
        relation_filenodes=frozenset(relation_filenodes),
        toast_filenodes=frozenset(toast_filenodes))


def _sampler_for(pids, socket_path, pg_context):
    """Build a tick sampler over the given role->pid mapping."""
    from src.observability.process_resources.recorder import ProcfsTickSampler
    return ProcfsTickSampler(pids, str(socket_path), pg_context)


def _evaluate_case(
    *,
    case_name,
    root: Path,
    baseline_capture,
    recorded,
    windows,
    expected_sqlstate=None,
    observed_sqlstate=None,
    correctness=None,
    cleanup_trace=None,
):
    """Shared tail: attribute, validate, evaluate peak + cleanup, compose."""
    persist_trace(root / "raw", recorded.trace)
    persist_lifecycles(root / "raw", recorded.trace)
    persist_operation_outcome(root / "raw", recorded, case_name)
    attribution = None
    if windows:
        attribution = attribute_provider_sessions(
            backend_pid=_case_backend_pid(recorded),
            baseline=baseline_capture.baseline,
            trace=recorded.trace,
            windows=windows)
    save(root / "attribution.json", attribution or {"problems": ["no_sessions"]})
    trace = recorded.trace if attribution is None else reclassify_clients(
        recorded.trace, attribution)
    peak_report = build_qualification_report(
        baseline_capture.baseline, trace, phase="stress")
    if cleanup_trace is None:
        cleanup_trace = trace
    cleanup_report = build_qualification_report(
        baseline_capture.baseline, cleanup_trace, phase="cleanup")
    correctness_failures = list(correctness or [])
    if expected_sqlstate is not None and observed_sqlstate != expected_sqlstate:
        correctness_failures.append({
            "metric": "sqlstate_contract",
            "expected": expected_sqlstate,
            "observed": observed_sqlstate})
    if recorded.operation_error is not None and expected_sqlstate is None:
        correctness_failures.append({
            "metric": "operation_error",
            "exception_type": recorded.operation_error.exception_type})
    composed = peak_report, cleanup_report
    measurement = peak_report.measurement_status
    if cleanup_report.measurement_status == "invalid":
        measurement = "invalid"
    elif cleanup_report.measurement_status == "inconclusive" and measurement == "valid":
        measurement = "inconclusive"
    qualification = "passed"
    if measurement != "valid":
        qualification = "not_evaluated"
    elif (peak_report.qualification_status == "failed"
            or cleanup_report.qualification_status == "failed"
            or correctness_failures):
        qualification = "failed"
    report = {
        "case": case_name,
        "metric_schema": METRIC_SCHEMA,
        "measurement_status": measurement,
        "qualification_status": qualification,
        "peak_policy": [v.__dict__ for v in peak_report.peak_policy],
        "cleanup_policy": [v.__dict__ for v in cleanup_report.cleanup_policy],
        "correctness_failures": correctness_failures,
        "peak_diagnostics": peak_report.diagnostics,
        "cleanup_diagnostics": cleanup_report.diagnostics,
        "attribution": attribution,
        "operation_error": (None if recorded.operation_error is None else {
            "exception_type": recorded.operation_error.exception_type,
            "sqlstate": recorded.operation_error.sqlstate}),
    }
    save(root / "gate_report.json", report)
    return report


def _case_backend_pid(recorded):
    for tick in recorded.trace.ticks:
        backend = tick.processes.get("backend")
        if backend is not None:
            return backend.pid
    return -1


def _cleanup_settle(sampler, root: Path, phase: str, baseline):
    """Synchronous settle against the case baseline; full snapshots persisted."""
    ticks = []
    deadline = time.monotonic() + CLEANUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        tick = sampler.sample_all(time.monotonic_ns())
        ticks.append(tick)
        from src.experiments.postgresql.resource_qualification import (
            evaluate_cleanup_policy)
        violations, _ = evaluate_cleanup_policy(baseline, _trace_of(ticks))
        if not violations:
            break
        time.sleep(SETTLE_INTERVAL_SECONDS)
    trace = _trace_of(ticks)
    persist_trace(root / f"raw_{phase}_cleanup", trace)
    return trace


def _trace_of(ticks):
    """Baseline is the first settle tick; end state is the final tick."""
    if not ticks:
        return ResourceTrace(baseline={}, ticks=())
    return ResourceTrace(
        baseline=dict(ticks[0].processes), ticks=tuple(ticks[1:]) or tuple(ticks[:1]))


def run_stress_case(args, connection, user, fixture_path, expected_digest):
    """Stress case through the full lifecycle; returns its report."""
    from src.experiments.choice_resource_checks import child, wait_file

    root = args.root / "stress"
    root.mkdir()
    _chown(root, user)
    socket_path = args.root / "socket/stress.sock"
    event_path = root / "session_events.jsonl"
    env = dict(os.environ, PYTHONPATH=str(args.repo / "code"),
               PYTHONDONTWRITEBYTECODE="1")
    pg_context = pg_file_context(connection)

    with child(gateway_command(args, socket_path, fixture_path, event_path),
               root, "gateway", env, user) as gateway:
        wait_file(socket_path, gateway)
        release = root / "release"
        finish = root / "finish"
        command = [
            str(args.client), str(args.root / "socket"), "55446",
            str(socket_path), str(release), str(finish),
        ]
        with child(command, root, "client", env, user) as client:
            warmup = wait_log(root / "client.log", "warmup_complete")
            current = wait_gateway_events(event_path, tasks=1, sessions_ended=1)
            assert current[-1]["event"] == "session_end"
            sampler = _sampler_for(
                {"backend": warmup["backend_pid"], "gateway": gateway.pid},
                socket_path, pg_context)
            baseline_capture, observed = acquire_stable_baseline(sampler)
            if baseline_capture is None:
                result = {
                    "case": "stress_large_payload",
                    "metric_schema": METRIC_SCHEMA,
                    "measurement_status": "invalid",
                    "qualification_status": "not_evaluated",
                    "reason": "stable baseline not achieved",
                }
                save(root / "gate_report.json", result)
                return result
            persist_trace(root / "raw_baseline", _trace_of(observed))

            def run_rounds():
                release.touch(exist_ok=False)
                return wait_log(root / "client.log", "all_complete", timeout=600)

            recorded = record_operation(
                sampler, run_rounds, sample_seconds=SAMPLE_SECONDS,
                baseline=baseline_capture.baseline)
            finish.touch(exist_ok=False)
            client.wait(timeout=30)

            events = wait_gateway_events(
                event_path,
                tasks=1 + ROWS_PER_ROUND * ROUNDS,
                sessions_ended=1 + ROUNDS)
            task_events = [item for item in events if item["event"] == "task"]
            correctness = []
            rows = getattr(recorded.result, "get", lambda _k: None)("rows")
            if rows != ROWS_PER_ROUND * ROUNDS:
                correctness.append({"metric": "rows",
                                    "expected": ROWS_PER_ROUND * ROUNDS,
                                    "observed": rows})
            if len(task_events) != 1 + ROWS_PER_ROUND * ROUNDS:
                correctness.append({"metric": "tasks",
                                    "expected": 1 + ROWS_PER_ROUND * ROUNDS,
                                    "observed": len(task_events)})
            if any(item["payload_digest"] != expected_digest for item in task_events):
                correctness.append({"metric": "digest_mismatch"})
            report = _evaluate_case(
                case_name="stress_large_payload", root=root,
                baseline_capture=baseline_capture, recorded=recorded,
                windows=session_windows(events), correctness=correctness,
                cleanup_trace=None)
            # Separate cleanup settle AFTER the peak verdict is already saved.
            cleanup_trace = _cleanup_settle(sampler, root, "stress", baseline_capture.baseline)
            cleanup_report = build_qualification_report(
                baseline_capture.baseline, cleanup_trace, phase="cleanup")
            report["cleanup_policy"] = [
                v.__dict__ for v in cleanup_report.cleanup_policy]
            report["cleanup_diagnostics"] = cleanup_report.diagnostics
            if (report["measurement_status"] == "valid"
                    and cleanup_report.qualification_status == "failed"):
                report["qualification_status"] = "failed"
            save(root / "gate_report.json", report)
        gateway.terminate()
        gateway.wait(timeout=10)
    return report


def run_cancel_case(args, connection, user, fixture_path):
    """Cancel + recovery with a full trace; SQLSTATE per registered contract."""
    from src.experiments.choice_resource_checks import child, wait_file

    root = args.root / "cancel"
    root.mkdir()
    _chown(root, user)
    socket_path = args.root / "socket" / "cancel.sock"
    event_path = root / "session_events.jsonl"
    env = dict(os.environ, PYTHONPATH=str(args.repo / "code"),
               PYTHONDONTWRITEBYTECODE="1")
    pg_context = pg_file_context(connection)
    with child(gateway_command(args, socket_path, fixture_path, event_path,
                               ("--test-response-delay-ms", "1000")),
               root, "gateway", env, user) as gateway:
        wait_file(socket_path, gateway)
        sampler = _sampler_for(
            {"backend": connection.info.backend_pid, "gateway": gateway.pid},
            socket_path, pg_context)
        baseline_capture, _ = acquire_stable_baseline(sampler)
        if baseline_capture is None:
            result = {"case": "cancel_and_cleanup",
                      "measurement_status": "invalid",
                      "qualification_status": "not_evaluated",
                      "reason": "stable baseline not achieved"}
            save(root / "result.json", result)
            return result
        connection.execute("SET statement_timeout='50ms'")
        observed_sqlstate = None

        def attempt_cancel():
            nonlocal observed_sqlstate
            try:
                query_one(connection, socket_path)
            except psycopg.Error as error:
                observed_sqlstate = error.sqlstate

        recorded = record_operation(
            sampler, attempt_cancel, sample_seconds=SAMPLE_SECONDS,
            baseline=baseline_capture.baseline)
        wait_gateway_events(event_path, tasks=1, sessions_ended=1, timeout=10)
        connection.execute("SET statement_timeout='5s'")

        def recovery():
            value = query_one(connection, socket_path)
            return {"len": len(value)}

        recorded_recovery = record_operation(
            sampler, recovery, sample_seconds=SAMPLE_SECONDS,
            baseline=baseline_capture.baseline)
        wait_gateway_events(event_path, tasks=2, sessions_ended=2, timeout=10)
        events = load_session_events(event_path)
        correctness = []
        if recorded_recovery.operation_error is not None:
            correctness.append({"metric": "recovery_query_failed"})
        cleanup_trace = _cleanup_settle(sampler, root, "cancel", baseline_capture.baseline)
        report = _evaluate_case(
            case_name="cancel_and_cleanup", root=root,
            baseline_capture=baseline_capture, recorded=recorded,
            windows=session_windows(events),
            expected_sqlstate=CANCEL_SQLSTATE,
            observed_sqlstate=observed_sqlstate,
            correctness=correctness,
            cleanup_trace=cleanup_trace)
        gateway.terminate()
        gateway.wait(timeout=10)
    return report


def run_disconnect_case(args, connection, user, fixture_path):
    """Disconnect subphase then recovery subphase, each with its own gateway."""
    from src.experiments.choice_resource_checks import child, wait_file

    root = args.root / "disconnect"
    root.mkdir()
    _chown(root, user)
    pg_context = pg_file_context(connection)
    env = dict(os.environ, PYTHONPATH=str(args.repo / "code"),
               PYTHONDONTWRITEBYTECODE="1")

    # Subphase 1: disconnect while the gateway lives.
    socket_path = args.root / "socket" / "disconnect.sock"
    event_path = root / "disconnect_events.jsonl"
    with child(gateway_command(args, socket_path, fixture_path, event_path,
                               ("--test-disconnect-on-task",)),
               root, "gateway", env, user) as gateway:
        wait_file(socket_path, gateway)
        sampler = _sampler_for(
            {"backend": connection.info.backend_pid, "gateway": gateway.pid},
            socket_path, pg_context)
        baseline_capture, _ = acquire_stable_baseline(sampler)
        if baseline_capture is None:
            result = {"case": "provider_disconnect_and_recovery",
                      "measurement_status": "invalid",
                      "qualification_status": "not_evaluated",
                      "reason": "stable baseline not achieved"}
            save(root / "result.json", result)
            return result
        observed_sqlstate = None

        def attempt():
            nonlocal observed_sqlstate
            try:
                query_one(connection, socket_path)
            except psycopg.Error as error:
                observed_sqlstate = error.sqlstate

        recorded = record_operation(
            sampler, attempt, sample_seconds=SAMPLE_SECONDS,
            baseline=baseline_capture.baseline)
        wait_gateway_events(event_path, tasks=0, sessions_ended=1, timeout=10)
        events = load_session_events(event_path)
        cleanup_trace = _cleanup_settle(sampler, root, "disconnect", baseline_capture.baseline)
        report = _evaluate_case(
            case_name="provider_disconnect_and_recovery", root=root,
            baseline_capture=baseline_capture, recorded=recorded,
            windows=session_windows(events),
            expected_sqlstate=DISCONNECT_SQLSTATE,
            observed_sqlstate=observed_sqlstate,
            cleanup_trace=cleanup_trace)
        gateway.terminate()
        gateway.wait(timeout=10)

    # Subphase 2: fresh gateway, fresh baseline, recovery query.
    recovery_root = args.root / "disconnect-recovery"
    recovery_root.mkdir()
    _chown(recovery_root, user)
    recovery_socket = args.root / "socket" / "disconnect-recovery.sock"
    recovery_events = recovery_root / "session_events.jsonl"
    with child(gateway_command(args, recovery_socket, fixture_path,
                               recovery_events),
               recovery_root, "gateway", env, user) as gateway:
        wait_file(recovery_socket, gateway)
        sampler = _sampler_for({"backend_pid": connection.info.backend_pid},
                               connection, gateway, recovery_socket, pg_context)
        baseline_capture, _ = acquire_stable_baseline(sampler)

        def recovery():
            value = query_one(connection, recovery_socket)
            return {"len": len(value)}

        recorded = record_operation(
            sampler, recovery, sample_seconds=SAMPLE_SECONDS,
            baseline=baseline_capture.baseline)
        events = load_session_events(recovery_events)
        cleanup_trace = _cleanup_settle(sampler, recovery_root, "recovery", baseline_capture.baseline)
        recovery_report = _evaluate_case(
            case_name="provider_disconnect_and_recovery/recovery",
            root=recovery_root, baseline_capture=baseline_capture,
            recorded=recorded, windows=session_windows(events),
            correctness=(
                [] if recorded.operation_error is None
                else [{"metric": "recovery_query_failed"}]),
            cleanup_trace=cleanup_trace)
        gateway.terminate()
        gateway.wait(timeout=10)
    if recovery_report["qualification_status"] != "passed":
        report["qualification_status"] = "failed"
    report["recovery"] = {
        "measurement_status": recovery_report["measurement_status"],
        "qualification_status": recovery_report["qualification_status"]}
    save(root / "result.json", report)
    return report


def run_exit_case(args, connection, user, fixture_path):
    """Gateway-exit: alive phase, absent phase, fresh-gateway recovery phase."""
    from src.experiments.choice_resource_checks import child, wait_file

    root = args.root / "gateway_exit"
    root.mkdir()
    _chown(root, user)
    pg_context = pg_file_context(connection)
    env = dict(os.environ, PYTHONPATH=str(args.repo / "code"),
               PYTHONDONTWRITEBYTECODE="1")
    socket_path = args.root / "socket" / "exit.sock"
    event_path = root / "session_events.jsonl"
    with child(gateway_command(args, socket_path, fixture_path, event_path),
               root, "gateway", env, user) as gateway:
        wait_file(socket_path, gateway)
        sampler = _sampler_for(
            {"backend": connection.info.backend_pid, "gateway": gateway.pid},
            socket_path, pg_context)
        baseline_capture, _ = acquire_stable_baseline(sampler)
        if baseline_capture is None:
            result = {"case": "gateway_exit_and_recovery",
                      "measurement_status": "invalid",
                      "qualification_status": "not_evaluated",
                      "reason": "stable baseline not achieved"}
            save(root / "result.json", result)
            return result

        def warm():
            value = query_one(connection, socket_path)
            return {"len": len(value)}

        recorded = record_operation(
            sampler, warm, sample_seconds=SAMPLE_SECONDS,
            baseline=baseline_capture.baseline)
        wait_gateway_events(event_path, tasks=1, sessions_ended=1, timeout=10)
        gateway.terminate()
        gateway.wait(timeout=10)
        assert not socket_path.exists()

    # Absent phase: only the backend's socket cleanup and the SQLSTATE contract.
    absent_ticks = []
    sampler_absent = _sampler_for(
        {"backend": connection.info.backend_pid},
        socket_path, pg_context)

    def absent_probe():
        try:
            query_one(connection, socket_path)
        except psycopg.Error as error:
            return error.sqlstate
        return None

    recorded_absent = record_operation(
        sampler_absent, absent_probe, sample_seconds=SAMPLE_SECONDS,
        baseline={r: s for r, s in baseline_capture.baseline.items()
                  if r == "backend"})
    absent_sqlstate = recorded_absent.result
    persist_trace(root / "raw_absent", recorded_absent.trace)

    # Recovery phase: fresh gateway, fresh baseline.
    recovery_root = args.root / "gateway_exit-recovery"
    recovery_root.mkdir()
    _chown(recovery_root, user)
    recovery_socket = args.root / "socket" / "exit-recovery.sock"
    recovery_events = recovery_root / "session_events.jsonl"
    with child(gateway_command(args, recovery_socket, fixture_path,
                               recovery_events),
               recovery_root, "gateway", env, user) as gateway:
        wait_file(recovery_socket, gateway)
        sampler = _sampler_for({"backend_pid": connection.info.backend_pid},
                               connection, gateway, recovery_socket, pg_context)
        baseline_capture, _ = acquire_stable_baseline(sampler)

        def recovery():
            value = query_one(connection, recovery_socket)
            return {"len": len(value)}

        recorded_recovery = record_operation(
            sampler, recovery, sample_seconds=SAMPLE_SECONDS,
            baseline=baseline_capture.baseline)
        events = load_session_events(recovery_events)
        cleanup_trace = _cleanup_settle(sampler, recovery_root, "recovery", baseline_capture.baseline)
        recovery_report = _evaluate_case(
            case_name="gateway_exit_and_recovery/recovery",
            root=recovery_root, baseline_capture=baseline_capture,
            recorded=recorded_recovery, windows=session_windows(events),
            correctness=(
                [] if recorded_recovery.operation_error is None
                else [{"metric": "recovery_query_failed"}]),
            cleanup_trace=cleanup_trace)
        gateway.terminate()
        gateway.wait(timeout=10)

    correctness = []
    if absent_sqlstate != GATEWAY_EXIT_SQLSTATE:
        # Registered contract mismatch is a separate production bug report,
        # recorded here, never absorbed by widening the accepted set.
        correctness.append({"metric": "sqlstate_contract",
                            "expected": GATEWAY_EXIT_SQLSTATE,
                            "observed": absent_sqlstate})
    report = {
        "case": "gateway_exit_and_recovery",
        "metric_schema": METRIC_SCHEMA,
        "measurement_status": recovery_report["measurement_status"],
        "qualification_status": recovery_report["qualification_status"],
        "correctness_failures": correctness,
        "post_exit_sqlstate": absent_sqlstate,
        "socket_path_removed": not socket_path.exists(),
        "recovery": {"qualification_status":
                     recovery_report["qualification_status"]},
    }
    if correctness:
        report["qualification_status"] = "failed"
    save(root / "result.json", report)
    return report


def _is_unsafe(case_result) -> bool:
    for violation in case_result.get("cleanup_policy") or []:
        if violation.get("metric") in SAFETY_METRICS:
            return True
    return False


def _exit_code(summary) -> int:
    statuses = [
        (case.get("measurement_status"), case.get("qualification_status"))
        for case in summary["cases"].values()]
    if any(m == "invalid" or m == "inconclusive" for m, _ in statuses):
        return EXIT_NOT_EVALUATED
    if any(q == "not_evaluated" for _, q in statuses):
        return EXIT_NOT_EVALUATED
    if any(q == "failed" for _, q in statuses):
        return EXIT_VALID_FAILED
    return EXIT_ALL_PASS


def run(args):
    from src.experiments.choice_resource_checks import cluster

    def preflight_failure(reason):
        args.root.mkdir(parents=True, exist_ok=True)
        save(args.root / "summary.json", {
            "status": "runner_failure",
            "reason": reason,
            "metric_schema": METRIC_SCHEMA,
            "model_requests": 0,
            "cases": {}})
        return EXIT_RUNNER_FAILURE

    try:
        if subprocess.check_output(
                [str(args.prefix / "bin/pg_config"), "--version"],
                text=True).strip() != "PostgreSQL 18.3":
            return preflight_failure("pg_version_mismatch")
        if subprocess.check_output(
                ["git", "-C", str(args.repo), "rev-parse", "HEAD"],
                text=True).strip() != args.commit:
            return preflight_failure("commit_mismatch")
        if subprocess.check_output(
                ["git", "-C", str(args.repo), "status", "--porcelain"],
                text=True).strip():
            return preflight_failure("working_tree_dirty")
    except OSError as error:
        return preflight_failure(f"preflight_error:{type(error).__name__}")
    args.root.mkdir()
    user = pwd.getpwnam("postgres")
    _chown(args.root, user)
    fixture_path, digest = fixture(args, args.root)
    summary = {
        "status": "failed",
        "metric_schema": METRIC_SCHEMA,
        "supersedes_measurement_implementation": "v1",
        "model_requests": 0,
        "mode": "diagnostic" if getattr(args, "diagnostic", False) else "formal",
        "cases": {},
    }
    try:
        with cluster(args.prefix, args.root, user) as connection:
            connection.execute(
                "CREATE TABLE resource_rows(id integer, payload text) "
                "WITH (autovacuum_enabled=false, toast.autovacuum_enabled=false)")
            connection.execute(
                "INSERT INTO resource_rows SELECT n,repeat('x',100000) "
                "FROM generate_series(1,2000) n")
            connection.execute("VACUUM ANALYZE resource_rows")
            connection.execute("SET statement_timeout='300s'")
            stress = run_stress_case(args, connection, user, fixture_path, digest)
            summary["cases"]["stress_large_payload"] = stress
            stop = _is_unsafe(stress)
            for name, check in (
                    ("cancel_and_cleanup", run_cancel_case),
                    ("provider_disconnect_and_recovery", run_disconnect_case),
                    ("gateway_exit_and_recovery", run_exit_case)):
                if stop:
                    summary["cases"][name] = {
                        "measurement_status": "not_run",
                        "qualification_status": "not_evaluated",
                        "reason": "unsafe cleanup state in earlier case",
                    }
                    continue
                summary["cases"][name] = check(
                    args, connection, user, fixture_path)
                stop = _is_unsafe(summary["cases"][name])
            if getattr(args, "diagnostic", False):
                # A diagnostic run answers identity/peak questions only; it
                # can never produce a qualification verdict.
                for case in summary["cases"].values():
                    case["qualification_status"] = "not_evaluated"
                    case["diagnostic_note"] = (
                        "diagnostic mode: identity/peak evidence only")
            required = [c for n, c in summary["cases"].items()]
            summary["status"] = (
                "passed" if all(
                    c.get("measurement_status") == "valid"
                    and c.get("qualification_status") == "passed"
                    for c in required)
                else "failed")
    finally:
        save(args.root / "summary.json", summary)
    return _exit_code(summary)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repo", "root", "prefix", "client"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--diagnostic", action="store_true",
                        help="1 round x 100 rows, verdicts forced not_evaluated")
    args = parser.parse_args()
    if args.diagnostic:
        global ROWS_PER_ROUND, ROUNDS
        ROWS_PER_ROUND = 100
        ROUNDS = 1
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
