#!/usr/bin/env python3
"""Run fixture-only SemMap resource, cancellation, disconnect, and exit checks."""

import argparse
import json
import os
from pathlib import Path
import pwd
import statistics
import subprocess
import sys
import threading
import time

import psycopg
from psycopg import sql
import psutil

from src.execution_provider.completion import Completion
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.wire import v5
from src.experiments.choice_resource_checks import child, cluster, wait_file


MODEL = "golden-map-resource-v1"
INSTRUCTION = "Generate fixed output."
INPUT = "x" * 100000
OUTPUT = "y" * 65536
SAMPLE_SECONDS = 0.02
MIB = 1024 * 1024


def save(path: Path, value) -> None:
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2)
        handle.write("\n")


def events(path: Path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="ascii").splitlines()]


def sample(process: psutil.Process):
    return {
        "rss_bytes": process.memory_info().rss,
        "fd": process.num_fds(),
        "threads": process.num_threads(),
    }


def settled(processes):
    values = []
    for _ in range(7):
        values.append({name: sample(process) for name, process in processes.items()})
        time.sleep(0.05)
    return {
        name: {
            field: int(statistics.median(point[name][field] for point in values))
            for field in ("rss_bytes", "fd", "threads")
        }
        for name in processes
    }


def observe(processes, operation):
    samples = []
    failure = []
    stopping = threading.Event()

    def collect():
        while not stopping.is_set():
            try:
                samples.append({
                    "monotonic": time.monotonic(),
                    "processes": {name: sample(process) for name, process in processes.items()},
                })
            except (OSError, psutil.Error) as error:
                failure.append(type(error).__name__)
                return
            stopping.wait(SAMPLE_SECONDS)

    worker = threading.Thread(target=collect)
    worker.start()
    try:
        result = operation()
    finally:
        stopping.set()
        worker.join()
    assert not failure and samples
    return result, samples


def wait_log(path: Path, event_name: str, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            parsed = []
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.startswith("{"):
                    parsed.append(json.loads(line))
            for event in parsed:
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


def fixture(args, root):
    plan = SemanticMapPlan(INSTRUCTION, MODEL, 128)
    task = v5.build_task_message(plan, sequence=0, input_value=INPUT)
    digest = task["semantic_payload_digest"]
    value = {
        digest: {
            "raw_output": OUTPUT,
            "response_model_id": MODEL,
            "prompt_tokens": 25032,
            "output_tokens": 128,
            "finish_reason": "stop",
        }
    }
    fixture_path = root / "fixture.json"
    save(fixture_path, value)
    save(root / "fixture-identity.json", {
        "payload_digest": digest,
        "input_bytes": len(INPUT.encode()),
        "output_bytes": len(OUTPUT.encode()),
        "model": MODEL,
        "model_requests": 0,
    })
    return fixture_path, digest


def gateway_command(args, socket_path, fixture_path, event_path, extra=()):
    return [
        sys.executable,
        str(args.gateway_observer),
        "--events",
        str(event_path),
        "--",
        "--socket",
        str(socket_path),
        "--golden-fixture",
        str(fixture_path),
        *extra,
    ]


def query_one(connection, socket_path):
    connection.execute("SET semloom_pg.provider_execution_profile='golden'")
    connection.execute("SELECT set_config('semloom_pg.gateway_socket',%s,false)", (str(socket_path),))
    options = json.dumps({"model": MODEL, "temperature": 0, "max_tokens": 128}, separators=(",", ":"))
    statement = sql.SQL(
        "SELECT ai_semantic.map(payload,{},{}::jsonb) FROM ONLY resource_rows WHERE id=1"
    ).format(sql.Literal(INSTRUCTION), sql.Literal(options))
    value = connection.execute(statement).fetchone()[0]
    assert value == OUTPUT
    return len(value)


def check_stress_limits(baseline, ending, samples):
    peaks = {
        name: {
            field: max(point["processes"][name][field] for point in samples)
            for field in ("rss_bytes", "fd", "threads")
        }
        for name in baseline
    }
    assert peaks["gateway"]["rss_bytes"] - baseline["gateway"]["rss_bytes"] <= 32 * MIB
    assert ending["gateway"]["rss_bytes"] - baseline["gateway"]["rss_bytes"] <= 16 * MIB
    assert peaks["backend"]["rss_bytes"] - baseline["backend"]["rss_bytes"] <= 16 * MIB
    assert ending["backend"]["rss_bytes"] - baseline["backend"]["rss_bytes"] <= 8 * MIB
    baseline_uds = baseline["gateway"]["fd"] + baseline["backend"]["fd"]
    peak_uds = peaks["gateway"]["fd"] + peaks["backend"]["fd"]
    ending_uds = ending["gateway"]["fd"] + ending["backend"]["fd"]
    assert peak_uds - baseline_uds <= 2
    assert ending_uds - baseline_uds == 0
    assert ending["gateway"]["threads"] == baseline["gateway"]["threads"]
    assert ending["backend"]["threads"] == baseline["backend"]["threads"]
    return peaks


def stress(args, connection, user, fixture_path, expected_digest):
    root = args.root / "stress"
    root.mkdir()
    os.chown(root, user.pw_uid, user.pw_gid)
    socket_path = args.root / "socket/stress.sock"
    event_path = root / "gateway-events.jsonl"
    env = dict(os.environ, PYTHONPATH=str(args.repo / "code"), PYTHONDONTWRITEBYTECODE="1")
    with child(gateway_command(args, socket_path, fixture_path, event_path), root, "gateway", env, user) as gateway:
        wait_file(socket_path, gateway)
        release = root / "release"
        finish = root / "finish"
        command = [
            str(args.client),
            str(args.root / "socket"),
            "55446",
            str(socket_path),
            str(release),
            str(finish),
        ]
        with child(command, root, "client", env, user) as client:
            warmup = wait_log(root / "client.log", "warmup_complete")
            current = wait_gateway_events(event_path, tasks=1, sessions_ended=1)
            assert current[-1]["event"] == "session_end"
            processes = {
                "backend": psutil.Process(warmup["backend_pid"]),
                "gateway": psutil.Process(gateway.pid),
                "client": psutil.Process(client.pid),
            }
            baseline = settled(processes)
            save(root / "baseline.json", baseline)

            def run_rounds():
                release.touch(exist_ok=False)
                return wait_log(root / "client.log", "all_complete", timeout=600)

            completed, samples = observe(processes, run_rounds)
            assert completed["rows"] == 6000
            current = wait_gateway_events(event_path, tasks=6001, sessions_ended=4)
            task_events = [item for item in current if item["event"] == "task"]
            assert len(task_events) == 6001
            assert all(item["payload_digest"] == expected_digest for item in task_events)

            deadline = time.monotonic() + 60
            ending = None
            peaks = None
            last_error = None
            while time.monotonic() < deadline:
                ending = settled(processes)
                try:
                    peaks = check_stress_limits(baseline, ending, samples)
                    break
                except AssertionError as error:
                    last_error = str(error)
                    time.sleep(0.25)
            if peaks is None:
                raise AssertionError(f"resource recovery did not meet limits: {last_error}")
            save(root / "measurements.json", {
                "baseline": baseline,
                "ending": ending,
                "peaks": peaks,
                "samples": samples,
                "tasks": len(task_events),
                "sessions": 4,
            })
            finish.touch(exist_ok=False)
            assert client.wait(timeout=10) == 0
        gateway.terminate()
        assert gateway.wait(timeout=5) == 0
        assert not socket_path.exists()
    return {"tasks": 6000, "warmup_tasks": 1, "rounds": 3, "rows_per_round": 2000}


def fault_gateway(args, user, label, fixture_path, extra=()):
    root = args.root / label
    root.mkdir()
    os.chown(root, user.pw_uid, user.pw_gid)
    socket_path = args.root / "socket" / f"{label}.sock"
    event_path = root / "events.jsonl"
    env = dict(os.environ, PYTHONPATH=str(args.repo / "code"), PYTHONDONTWRITEBYTECODE="1")
    command = gateway_command(args, socket_path, fixture_path, event_path, extra)
    return root, socket_path, event_path, env, command


def cancel_check(args, connection, user, fixture_path):
    root, socket_path, event_path, env, command = fault_gateway(
        args, user, "cancel", fixture_path, ("--test-response-delay-ms", "1000")
    )
    with child(command, root, "gateway", env, user) as gateway:
        wait_file(socket_path, gateway)
        processes = {"backend": psutil.Process(connection.info.backend_pid), "gateway": psutil.Process(gateway.pid)}
        baseline = settled(processes)
        connection.execute("SET statement_timeout='50ms'")
        state = None
        try:
            query_one(connection, socket_path)
        except psycopg.Error as error:
            state = error.sqlstate
        assert state == "57014"
        wait_gateway_events(event_path, tasks=1, sessions_ended=1, timeout=5)
        connection.execute("SET statement_timeout='5s'")
        assert query_one(connection, socket_path) == 65536
        wait_gateway_events(event_path, tasks=2, sessions_ended=2, timeout=5)
        ending = settled(processes)
        assert ending["backend"]["fd"] == baseline["backend"]["fd"]
        assert ending["gateway"]["fd"] == baseline["gateway"]["fd"]
        save(root / "result.json", {"sqlstate": state, "recovery": "passed", "baseline": baseline, "ending": ending})
        gateway.terminate()
        assert gateway.wait(timeout=5) == 0 and not socket_path.exists()


def disconnect_check(args, connection, user, fixture_path):
    root, socket_path, event_path, env, command = fault_gateway(
        args, user, "disconnect", fixture_path, ("--test-disconnect-on-task",)
    )
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
    recovery_root, recovery_socket, recovery_events, recovery_env, recovery_command = fault_gateway(
        args, user, "disconnect-recovery", fixture_path
    )
    with child(recovery_command, recovery_root, "gateway", recovery_env, user) as gateway:
        wait_file(recovery_socket, gateway)
        assert query_one(connection, recovery_socket) == 65536
        wait_gateway_events(recovery_events, tasks=1, sessions_ended=1, timeout=5)
        gateway.terminate()
        assert gateway.wait(timeout=5) == 0 and not recovery_socket.exists()
    save(root / "result.json", {"sqlstate": state, "recovery": "passed"})


def exit_check(args, connection, user, fixture_path):
    root, socket_path, event_path, env, command = fault_gateway(args, user, "gateway-exit", fixture_path)
    with child(command, root, "gateway", env, user) as gateway:
        wait_file(socket_path, gateway)
        assert query_one(connection, socket_path) == 65536
        wait_gateway_events(event_path, tasks=1, sessions_ended=1, timeout=5)
        gateway.terminate()
        assert gateway.wait(timeout=5) == 0 and not socket_path.exists()
    state = None
    try:
        query_one(connection, socket_path)
    except psycopg.Error as error:
        state = error.sqlstate
    assert state == "08006"
    recovery_root, recovery_socket, recovery_events, recovery_env, recovery_command = fault_gateway(
        args, user, "gateway-exit-recovery", fixture_path
    )
    with child(recovery_command, recovery_root, "gateway", recovery_env, user) as gateway:
        wait_file(recovery_socket, gateway)
        assert query_one(connection, recovery_socket) == 65536
        wait_gateway_events(recovery_events, tasks=1, sessions_ended=1, timeout=5)
        gateway.terminate()
        assert gateway.wait(timeout=5) == 0 and not recovery_socket.exists()
    save(root / "result.json", {"post_exit_sqlstate": state, "recovery": "passed"})


def run(args):
    assert subprocess.check_output([str(args.prefix / "bin/pg_config"), "--version"], text=True).strip() == "PostgreSQL 18.3"
    assert subprocess.check_output(["git", "-C", str(args.repo), "rev-parse", "HEAD"], text=True).strip() == args.commit
    assert not subprocess.check_output(["git", "-C", str(args.repo), "status", "--porcelain"], text=True).strip()
    args.root.mkdir()
    user = pwd.getpwnam("postgres")
    os.chown(args.root, user.pw_uid, user.pw_gid)
    fixture_path, digest = fixture(args, args.root)
    summary = {"status": "failed", "model_requests": 0}
    try:
        with cluster(args.prefix, args.root, user) as connection:
            connection.execute(
                "CREATE TABLE resource_rows(id integer, payload text) "
                "WITH (autovacuum_enabled=false, toast.autovacuum_enabled=false)"
            )
            connection.execute(
                "INSERT INTO resource_rows SELECT n,repeat('x',100000) FROM generate_series(1,2000) n"
            )
            connection.execute("VACUUM ANALYZE resource_rows")
            connection.execute("SET statement_timeout='300s'")
            stress_result = stress(args, connection, user, fixture_path, digest)
            cancel_check(args, connection, user, fixture_path)
            disconnect_check(args, connection, user, fixture_path)
            exit_check(args, connection, user, fixture_path)
        summary = {
            "status": "passed",
            "model_requests": 0,
            "stress": stress_result,
            "input_bytes_per_task": 100000,
            "output_bytes_per_task": 65536,
            "cancel": "passed",
            "disconnect": "passed",
            "gateway_exit": "passed",
        }
    finally:
        save(args.root / "summary.json", summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repo", "root", "prefix", "gateway-observer", "client"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--commit", required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
