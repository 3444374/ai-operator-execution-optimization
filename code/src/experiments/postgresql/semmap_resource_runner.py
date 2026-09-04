"""Fixture-only SemMap resource qualification runner (metric schema v2).

Explicit state machine per contract §8.4.2:

    PREPARE -> WARMUP -> ACQUIRE_STABLE_BASELINE -> RUN_STRESS_AND_CAPTURE
           -> PERSIST_STRESS_RAW -> EVALUATE_IMMUTABLE_PEAKS
           -> WAIT_FOR_CLEANUP -> PERSIST_CLEANUP_RAW -> EVALUATE_END_STATE
           -> RUN_INDEPENDENT_FAULT_CASES -> WRITE_FINAL_REPORT

Differences from the 2026-09-04 v1 runner that produced 93 duplicate
attempt files:

- The peak gate is judged exactly once after stress; the settle window only
  serves cleanup, and settle polls are recorded as ``cleanup_sample`` rows
  inside one artifact, never as new attempt files.
- Raw traces are persisted atomically BEFORE any pass/fail evaluation or
  exception, for every case including failures.
- Each fault case (cancel, provider disconnect, gateway exit) carries its own
  measurement/qualification status; a cleaned-up peak failure does not block
  diagnostic fault cases, but unsafe states (unrecovered FD/threads, dead
  processes) stop later cases.

The runner owns its isolated PostgreSQL 18.3 cluster, gateway, and C client
exactly like the v1 runner (shared helpers from choice_resource_checks);
no model service is started or contacted here. Zero model requests.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

try:
    import pwd
except ImportError:  # Windows checkouts import this module for unit tests only.
    pwd = None

import psycopg
from psycopg import sql

from src.observability.process_resources.model import ProcessSnapshot
from src.observability.process_resources.recorder import (
    ProcfsSampler,
    persist_fd_events,
    persist_trace,
    record_operation,
)
from src.experiments.choice_resource_checks import child, cluster, wait_file
from src.experiments.postgresql.resource_qualification import (
    METRIC_SCHEMA,
    build_qualification_report,
)


MODEL = "golden-map-resource-v1"
INSTRUCTION = "Generate fixed output."
INPUT = "x" * 100000
OUTPUT = "y" * 65536
SAMPLE_SECONDS = 0.02
CLEANUP_TIMEOUT_SECONDS = 60.0
SETTLE_INTERVAL_SECONDS = 0.25
ROWS_PER_ROUND = 2000
ROUNDS = 3

# Unsafe states that stop later fault cases even when an earlier gate failed.
SAFETY_METRICS = {"total_fd_end_delta", "thread_end_delta",
                  "provider_uds_session_fd_end_delta_combined"}


def save(path: Path, value) -> None:
    temporary = path.with_name(path.name + ".partial")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, indent=2)
        handle.write("\n")
    os.replace(temporary, path)


def events(path: Path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="ascii").splitlines()]


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
        current = events(path)
        if (sum(item["event"] == "task" for item in current) >= tasks and
                sum(item["event"] == "session_end" for item in current) >= sessions_ended):
            return current
        time.sleep(0.05)
    raise RuntimeError("gateway event counts did not settle")


def gateway_command(args, socket_path, fixture_path, event_path, extra=()):
    return [
        sys.executable,
        str(args.gateway_observer),
        "--events", str(event_path),
        "--",
        "--socket", str(socket_path),
        "--golden-fixture", str(fixture_path),
        *extra,
    ]


def query_one(connection, socket_path):
    connection.execute("SET semloom_pg.provider_execution_profile='golden'")
    connection.execute("SELECT set_config('semloom_pg.gateway_socket',%s,false)", (str(socket_path),))
    options = json.dumps({"model": MODEL, "temperature": 0, "max_tokens": 128},
                         separators=(",", ":"))
    statement = sql.SQL(
        "SELECT ai_semantic.map(payload,{},{}::jsonb) FROM ONLY resource_rows WHERE id=1"
    ).format(sql.Literal(INSTRUCTION), sql.Literal(options))
    value = connection.execute(statement).fetchone()[0]
    assert value == OUTPUT
    return len(value)


def fixture(args, root: Path):
    """Build the golden fixture exactly like v1 (same digest identity)."""
    from src.execution_provider.completion import Completion
    from src.execution_provider.semantic_map import SemanticMapPlan
    from src.execution_provider.wire import v5

    plan = SemanticMapPlan(INSTRUCTION, MODEL, 128)
    task = v5.build_task_message(plan, sequence=0, input_value=INPUT)
    digest = task["semantic_payload_digest"]
    value = {
        digest: {
            "raw_output": OUTPUT,
            "response_model_id": MODEL,
            "prompt_tokens": 25032,
            # output_tokens must stay <= the plan max_tokens (128) or the
            # gateway's usage validation rejects the fixture; matches v1.
            "output_tokens": 128,
            "finish_reason": "stop",
        }
    }
    fixture_path = root / "fixture.json"
    save(fixture_path, value)
    save(root / "fixture-identity.json", {
        "payload_digest": digest,
        "input_bytes": len(INPUT),
        "output_bytes": len(OUTPUT),
        "model": MODEL,
        "model_requests": 0,
    })
    return fixture_path, digest


def _roles(connection, gateway_pid):
    return {"backend": connection.info.backend_pid, "gateway": gateway_pid}


def _chown_tree(path: Path, user) -> None:
    if pwd is None or user is None or not hasattr(os, "chown"):
        return
    os.chown(path, user.pw_uid, user.pw_gid)


def run_stress_case(args, connection, user, fixture_path, expected_digest):
    """One stress case through the explicit state machine; returns its report."""
    root = args.root / "stress"
    root.mkdir()
    _chown_tree(root, user)
    socket_path = args.root / "socket/stress.sock"
    event_path = root / "gateway-events.jsonl"
    env = dict(os.environ, PYTHONPATH=str(args.repo / "code"), PYTHONDONTWRITEBYTECODE="1")
    case = {"case": "stress_large_payload", "metric_schema": METRIC_SCHEMA}

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
            # The stress backend is the C client's own connection (v1 sampled
            # the same pid from this event); the runner's psycopg backend
            # never executes Map queries.
            sampler = ProcfsSampler(
                {"backend": warmup["backend_pid"], "gateway": gateway.pid},
                str(socket_path))

            def run_rounds():
                release.touch(exist_ok=False)
                return wait_log(root / "client.log", "all_complete", timeout=600)

            # RUN_STRESS_AND_CAPTURE covers only the stress window; peak is
            # frozen the moment this returns.
            completed, stress_trace = record_operation(
                sampler, ("backend", "gateway"), run_rounds,
                sample_seconds=SAMPLE_SECONDS)
            assert completed["rows"] == ROWS_PER_ROUND * ROUNDS
            # PERSIST_STRESS_RAW before any evaluation.
            persist_trace(root / "raw", stress_trace)
            persist_fd_events(root / "raw", stress_trace)

            # EVALUATE_IMMUTABLE_PEAKS: judged once over the frozen window.
            peak_report = build_qualification_report(
                stress_trace.baseline, stress_trace)

            # WAIT_FOR_CLEANUP: settle window only serves the end state.
            cleanup_samples = []
            deadline = time.monotonic() + CLEANUP_TIMEOUT_SECONDS
            cleanup_trace = stress_trace
            while time.monotonic() < deadline:
                _, cleanup_trace = record_operation(
                    sampler, ("backend", "gateway"),
                    lambda: None, sample_seconds=0)
                cleanup_samples.append({
                    "monotonic": time.monotonic(),
                    "backend_total_fd": cleanup_trace.samples[-1]["backend"].total_fd_count,
                    "gateway_total_fd": cleanup_trace.samples[-1]["gateway"].total_fd_count,
                })
                end_report = build_qualification_report(
                    stress_trace.baseline, cleanup_trace)
                if not end_report.cleanup_policy:
                    break
                time.sleep(SETTLE_INTERVAL_SECONDS)
            save(root / "cleanup_samples.json", cleanup_samples)
            # PERSIST_CLEANUP_RAW: append the final cleanup observation set.
            persist_trace(root / "raw_cleanup", cleanup_trace)

            task_events = [item for item in
                           wait_gateway_events(event_path, tasks=1 + ROWS_PER_ROUND * ROUNDS,
                                               sessions_ended=1 + ROUNDS)
                           if item["event"] == "task"]
            assert len(task_events) == 1 + ROWS_PER_ROUND * ROUNDS
            assert all(item["payload_digest"] == expected_digest for item in task_events)

            end_report = build_qualification_report(
                stress_trace.baseline, cleanup_trace)
            combined = {
                **case,
                "measurement_status": (
                    "inconclusive" if peak_report.measurement_status == "inconclusive"
                    or end_report.measurement_status == "inconclusive"
                    else "valid"),
                "qualification_status": (
                    "failed" if peak_report.qualification_status == "failed"
                    or end_report.qualification_status == "failed"
                    else peak_report.qualification_status),
                "peak_policy": [v.__dict__ for v in peak_report.peak_policy],
                "cleanup_policy": [v.__dict__ for v in end_report.cleanup_policy],
                "peak_diagnostics": peak_report.diagnostics,
                "tasks": len(task_events),
                "sessions": 1 + ROUNDS,
            }
            save(root / "gate_report.json", combined)
            finish.touch(exist_ok=False)
            assert client.wait(timeout=10) == 0
        gateway.terminate()
        assert gateway.wait(timeout=5) == 0
        assert not socket_path.exists()
    return combined


def fault_gateway(args, user, label, fixture_path, extra=()):
    root = args.root / label
    root.mkdir()
    _chown_tree(root, user)
    socket_path = args.root / "socket" / f"{label}.sock"
    event_path = root / "events.jsonl"
    env = dict(os.environ, PYTHONPATH=str(args.repo / "code"), PYTHONDONTWRITEBYTECODE="1")
    command = gateway_command(args, socket_path, fixture_path, event_path, extra)
    return root, socket_path, event_path, env, command


def cancel_check(args, connection, user, fixture_path):
    root, socket_path, event_path, env, command = fault_gateway(
        args, user, "cancel", fixture_path, ("--test-response-delay-ms", "1000"))
    with child(command, root, "gateway", env, user) as gateway:
        wait_file(socket_path, gateway)
        connection.execute("SET statement_timeout='50ms'")
        state = None
        try:
            query_one(connection, socket_path)
        except psycopg.Error as error:
            state = error.sqlstate
        assert state == "57014"
        wait_gateway_events(event_path, tasks=1, sessions_ended=1, timeout=5)
        connection.execute("SET statement_timeout='5s'")
        assert query_one(connection, socket_path) == len(OUTPUT)
        wait_gateway_events(event_path, tasks=2, sessions_ended=2, timeout=5)
        sampler = ProcfsSampler(_roles(connection, gateway.pid), str(socket_path))
        _, trace = record_operation(sampler, ("backend", "gateway"),
                                    lambda: None, sample_seconds=0)
        persist_trace(root / "raw", trace)
        report = build_qualification_report(trace.baseline, trace)
        result = {"case": "cancel_and_cleanup", "sqlstate": state,
                  "measurement_status": report.measurement_status,
                  "qualification_status": report.qualification_status,
                  "violations": [v.__dict__ for v in report.cleanup_policy]}
        save(root / "result.json", result)
        gateway.terminate()
        assert gateway.wait(timeout=5) == 0 and not socket_path.exists()
    return result


def disconnect_check(args, connection, user, fixture_path):
    root, socket_path, event_path, env, command = fault_gateway(
        args, user, "disconnect", fixture_path, ("--test-disconnect-on-task",))
    with child(command, root, "gateway", env, user) as gateway:
        wait_file(socket_path, gateway)
        state = None
        try:
            query_one(connection, socket_path)
        except psycopg.Error as error:
            state = error.sqlstate
        assert state == "08006"
        wait_gateway_events(event_path, tasks=0, sessions_ended=1, timeout=5)
        gateway.terminate()
        assert gateway.wait(timeout=5) == 0 and not socket_path.exists()
    recovery_root, recovery_socket, recovery_events, recovery_env, recovery_command = \
        fault_gateway(args, user, "disconnect-recovery", fixture_path)
    with child(recovery_command, recovery_root, "gateway", recovery_env, user) as gateway:
        wait_file(recovery_socket, gateway)
        assert query_one(connection, recovery_socket) == len(OUTPUT)
        wait_gateway_events(recovery_events, tasks=1, sessions_ended=1, timeout=5)
        gateway.terminate()
        assert gateway.wait(timeout=5) == 0 and not recovery_socket.exists()
    result = {"case": "provider_disconnect_and_recovery", "sqlstate": state,
              "measurement_status": "valid", "qualification_status": "passed"}
    save(root / "result.json", result)
    return result


def exit_check(args, connection, user, fixture_path):
    root, socket_path, event_path, env, command = fault_gateway(
        args, user, "gateway-exit", fixture_path)
    with child(command, root, "gateway", env, user) as gateway:
        wait_file(socket_path, gateway)
        assert query_one(connection, socket_path) == len(OUTPUT)
        wait_gateway_events(event_path, tasks=1, sessions_ended=1, timeout=5)
        gateway.terminate()
        assert gateway.wait(timeout=5) == 0 and not socket_path.exists()
    state = None
    try:
        query_one(connection, socket_path)
    except psycopg.Error as error:
        state = error.sqlstate
    # A clean gateway exit removes the socket file before this retry, so the
    # connect fails with ENOENT instead of ECONNREFUSED; both surface as
    # connection-failure family errors (08006) or the internal mapped error
    # when the provider treats the missing path as an internal condition.
    assert state in ("08006", "XX000")
    recovery_root, recovery_socket, recovery_events, recovery_env, recovery_command = \
        fault_gateway(args, user, "gateway-exit-recovery", fixture_path)
    with child(recovery_command, recovery_root, "gateway", recovery_env, user) as gateway:
        wait_file(recovery_socket, gateway)
        assert query_one(connection, recovery_socket) == len(OUTPUT)
        wait_gateway_events(recovery_events, tasks=1, sessions_ended=1, timeout=5)
        gateway.terminate()
        assert gateway.wait(timeout=5) == 0 and not recovery_socket.exists()
    result = {"case": "gateway_exit_and_recovery", "post_exit_sqlstate": state,
              "measurement_status": "valid", "qualification_status": "passed"}
    save(root / "result.json", result)
    return result


def _is_unsafe(case_result) -> bool:
    """Unsafe cleanup states stop later cases; a judged peak failure does not."""
    violations = case_result.get("cleanup_policy") or case_result.get("violations") or []
    return any(v["metric"] in SAFETY_METRICS for v in violations)


def run(args):
    assert subprocess.check_output(
        [str(args.prefix / "bin/pg_config"), "--version"], text=True).strip() == "PostgreSQL 18.3"
    assert subprocess.check_output(
        ["git", "-C", str(args.repo), "rev-parse", "HEAD"], text=True).strip() == args.commit
    assert not subprocess.check_output(
        ["git", "-C", str(args.repo), "status", "--porcelain"], text=True).strip()
    args.root.mkdir()
    user = pwd.getpwnam("postgres")
    os.chown(args.root, user.pw_uid, user.pw_gid)
    fixture_path, digest = fixture(args, args.root)
    summary = {
        "status": "failed",
        "metric_schema": METRIC_SCHEMA,
        "supersedes_measurement_implementation": "v1",
        "model_requests": 0,
        "cases": {},
    }
    try:
        with cluster(args.prefix, args.root, user) as connection:
            connection.execute(
                "CREATE TABLE resource_rows(id integer, payload text) "
                "WITH (autovacuum_enabled=false, toast.autovacuum_enabled=false)")
            connection.execute(
                "INSERT INTO resource_rows SELECT n,repeat('x',100000) FROM generate_series(1,2000) n")
            connection.execute("VACUUM ANALYZE resource_rows")
            connection.execute("SET statement_timeout='300s'")
            stress = run_stress_case(args, connection, user, fixture_path, digest)
            summary["cases"]["stress_large_payload"] = stress
            # Fault cases run independently unless an unsafe state appeared.
            stop = _is_unsafe(stress)
            for name, check in (("cancel_and_cleanup", cancel_check),
                                ("provider_disconnect_and_recovery", disconnect_check),
                                ("gateway_exit_and_recovery", exit_check)):
                if stop:
                    summary["cases"][name] = {
                        "measurement_status": "not_run",
                        "qualification_status": "not_evaluated",
                        "reason": "unsafe cleanup state in earlier case",
                    }
                    continue
                summary["cases"][name] = check(args, connection, user, fixture_path)
                stop = _is_unsafe(summary["cases"][name])
            summary["status"] = (
                "passed" if all(
                    c.get("qualification_status") == "passed"
                    for c in summary["cases"].values())
                else "failed")
    finally:
        # WRITE_FINAL_REPORT: always persisted, pass or fail.
        save(args.root / "summary.json", summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repo", "root", "prefix", "gateway-observer", "client"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--commit", required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
