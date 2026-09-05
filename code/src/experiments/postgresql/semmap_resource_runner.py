"""Fixture-only PG18.3 resource checks with owned roots and complete phase evidence.

Create an exclusive run root, preflight/build, execute explicit phases, compose
one final report, and release only owned processes. No production code or real
model configuration is changed. See resource_lifecycle for mode/exit semantics.
"""
from contextlib import contextmanager
from dataclasses import asdict
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from src.observability.process_resources.recorder import ProcfsTickSampler
from src.observability.process_resources.model import PgFileClassificationContext
from .provider_session_attribution import load_session_events
from .resource_qualification import evaluate_session_drain
from .resource_lifecycle import RunSpec, PhaseResult, REQUIRED_PHASES, case_report, run_report
from .resource_phase import execute_phase, finish_phase, hashes, save_json

MODEL = "golden-map-resource-v1"
INSTRUCTION = "Generate fixed output."
INPUT = "x" * 100000
OUTPUT = "y" * 65536
CANCEL_SQLSTATE = "57014"
DISCONNECT_SQLSTATE = "08006"
GATEWAY_EXIT_SQLSTATE = "08006"


def preflight(args):
    version = subprocess.check_output([str(args.prefix / "bin/pg_config"), "--version"], text=True).strip()
    if version != "PostgreSQL 18.3":
        raise ValueError("pg_version_mismatch")
    actual = subprocess.check_output(["git", "-C", str(args.repo), "rev-parse", "HEAD"], text=True).strip()
    if actual != args.commit:
        raise ValueError("commit_mismatch")
    if subprocess.check_output(["git", "-C", str(args.repo), "status", "--porcelain"], text=True).strip():
        raise ValueError("working_tree_dirty")
    if Path(__file__).resolve() != (args.repo / "code/src/experiments/postgresql/semmap_resource_runner.py").resolve():
        raise ValueError("runner_source_mismatch")
    return {"commit": actual, "postgresql": version, "source_clean": True}


def build_client(client_source, target, pg_prefix):
    compiled = subprocess.run([
        "cc", "-O2", "-Wall", "-Werror", f"-I{pg_prefix / 'include'}",
        str(client_source), "-o", str(target), f"-L{pg_prefix / 'lib'}", "-lpq"],
        capture_output=True, text=True)
    # Only stable diagnostics enter portable evidence. Compilation stderr can
    # contain private absolute paths; never copy it into the public report.
    save_json(target.with_suffix(".build.json"), {"exit_code": compiled.returncode,
              "source_sha256": hashlib.sha256(client_source.read_bytes()).hexdigest(),
              "stderr_present": bool(compiled.stderr)})
    if compiled.returncode:
        raise RuntimeError("client_build_failed")


def fixture(root):
    from src.execution_provider.semantic_map import SemanticMapPlan
    from src.execution_provider.wire import v5
    task = v5.build_task_message(SemanticMapPlan(INSTRUCTION, MODEL, 128),
                                 sequence=0, input_value=INPUT)
    digest = task["semantic_payload_digest"]
    path = root / "fixture.json"
    save_json(path, {digest: {"raw_output": OUTPUT, "response_model_id": MODEL,
              "prompt_tokens": 25032, "output_tokens": 128, "finish_reason": "stop"}})
    save_json(root / "fixture_identity.json", {"payload_digest": digest,
              "input_bytes": len(INPUT), "output_bytes": len(OUTPUT),
              "output_sha256": hashlib.sha256(OUTPUT.encode()).hexdigest(), "model_requests": 0})
    return path, digest


def query_one(connection, socket_path):
    from psycopg import sql
    connection.execute("SET semloom_pg.provider_execution_profile='golden'")
    connection.execute("SELECT set_config('semloom_pg.gateway_socket',%s,false)", (str(socket_path),))
    options = json.dumps({"model": MODEL, "temperature": 0, "max_tokens": 128}, separators=(",", ":"))
    statement = sql.SQL("SELECT ai_semantic.map(payload,{},{}::jsonb) FROM ONLY resource_rows WHERE id=1").format(
        sql.Literal(INSTRUCTION), sql.Literal(options))
    rows = connection.execute(statement).fetchall()
    value = rows[0][0] if len(rows) == 1 and len(rows[0]) == 1 else None
    return {"rows": len(rows), "shape_valid": len(rows) == 1 and len(rows[0]) == 1,
            "is_null": value is None, "output_matches": value == OUTPUT,
            "output_sha256": hashlib.sha256(value.encode()).hexdigest() if isinstance(value, str) else None}


def check_output(value):
    if not isinstance(value, dict) or value.get("rows") != 1 or not value.get("shape_valid") or not value.get("output_matches") or value.get("is_null"):
        return [{"metric": "fixture_output_mismatch"}]
    return []


def pg_file_context(connection):
    data = connection.execute("SHOW data_directory").fetchone()[0]
    relation, toast = connection.execute("""
        SELECT c.relfilenode, t.relfilenode FROM pg_class c
        LEFT JOIN pg_class t ON t.oid=c.reltoastrelid
        WHERE c.oid='resource_rows'::regclass
    """).fetchone()
    return PgFileClassificationContext(data, frozenset([relation]), frozenset([toast] if toast else []))


def wait_log(path, event, timeout, process=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            for line in path.read_text().splitlines():
                if line.startswith("{"):
                    value = json.loads(line)
                    if value.get("event") == event:
                        return value
        if process is not None and process.poll() is not None:
            raise RuntimeError("client_exited_before_event")
        time.sleep(.01)
    raise TimeoutError("client_event_timeout")


def wait_drain(events, timeout=10.):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        values = events()
        violations, _ = evaluate_session_drain(values)
        if values and not violations:
            return
        time.sleep(.02)
    raise TimeoutError("warmup_session_drain_timeout")


@contextmanager
def gateway(args, root, name, fixture_path, user, extra=()):
    from .runtime_helpers import owned_child_process, wait_for_path
    root.mkdir()
    if user is not None:
        os.chown(root, user.pw_uid, user.pw_gid)
    socket_path = args.root / "socket" / (name + ".sock")
    event_path = root / "session_events.jsonl"
    env = dict(os.environ, PYTHONPATH=str(args.repo / "code"), PYTHONDONTWRITEBYTECODE="1")
    command = [sys.executable, str(args.repo / "code/src/experiments/postgresql/semmap_resource_gateway_observer.py"),
               "--events", str(event_path), "--", "--socket", str(socket_path),
               "--golden-fixture", str(fixture_path), *extra]
    with owned_child_process(command, root, "gateway", env, user) as process:
        wait_for_path(socket_path, process)
        yield process, socket_path, lambda: load_session_events(event_path)


def phase(args, spec, root, name, connection, gateway_process, socket_path,
          events, digest, **kwargs):
    pids = {"backend": connection.info.backend_pid}
    if gateway_process is not None:
        pids["gateway"] = gateway_process.pid
    sampler = ProcfsTickSampler(pids, str(socket_path), pg_file_context(connection))
    return execute_phase(root=root / name, phase=name, spec=spec, sampler=sampler,
        operation=lambda: query_one(connection, socket_path), events=events,
        roles=tuple(pids), expected_digest=digest, check_result=check_output, **kwargs)


def skipped(name, reason):
    return PhaseResult(name, "skipped", problems=(reason,))


def stress_case(args, spec, connection, user, fixture_path, digest):
    from .runtime_helpers import owned_child_process
    root = args.root / "stress_large_payload"
    root.mkdir()
    with gateway(args, root / "gateway", "stress", fixture_path, user) as (process, socket_path, events):
        client_root = root / "client"
        client_root.mkdir()
        release, finish = client_root / "release", client_root / "finish"
        env = dict(os.environ, LD_LIBRARY_PATH=str(args.prefix / "lib"))
        command = [str(args.root / "build/resource_client_v3"), str(args.root / "socket"), "55446",
                   str(socket_path), str(release), str(finish), str(spec.rounds), str(spec.rows_per_round)]
        with owned_child_process(command, client_root, "client", env, user) as client:
            warm = wait_log(client_root / "client.log", "warmup_complete", 30, client)
            wait_drain(events)
            sampler = ProcfsTickSampler({"backend": warm["backend_pid"], "gateway": process.pid},
                                       str(socket_path), pg_file_context(connection))
            def operation():
                release.touch()
                return wait_log(client_root / "client.log", "all_complete", 600, client)
            def correctness(result):
                expected = {"rows": spec.rounds * spec.rows_per_round,
                            "rounds": spec.rounds, "rows_per_round": spec.rows_per_round}
                return [{"metric": "client_workload", "expected": expected, "observed": result}] if not isinstance(result, dict) or any(result.get(k) != v for k,v in expected.items()) else []
            # Client remains connected throughout execute_phase's operation and
            # cleanup. Only its finish barrier permits PQfinish/backend exit.
            result = execute_phase(root=root / "stress", phase="stress", spec=spec,
                sampler=sampler, operation=operation, events=events,
                expected_tasks=spec.rounds * spec.rows_per_round, expected_sessions=spec.rounds,
                expected_digest=digest, check_result=correctness)
            exit_root = root / "client_exit"
            exit_root.mkdir()
            failure = None
            try:
                finish.touch()
                client.wait(timeout=30)
            except Exception as error:
                failure = type(error).__name__
            observed_code = client.poll()
            save_json(exit_root / "outcome.json", {
                "returncode": observed_code, "error_type": failure,
                "released_after_cleanup": True})
            exit_result = PhaseResult(
                "client_exit", "completed",
                "invalid" if failure else "valid",
                "not_evaluated" if failure else "passed" if observed_code == 0 else "failed",
                ("client_exit_unobserved",) if failure else (),
                ({"metric": "client_exit", "observed": observed_code},) if observed_code != 0 else (),
                failure is None and observed_code == 0, ("outcome.json",))
            finish_phase(exit_root, exit_result, spec)
            return [result, exit_result]


def cancel_case(args, spec, connection, user, fixture_path, digest):
    root = args.root / "cancel_and_cleanup"
    root.mkdir()
    with gateway(args, root / "gateway", "cancel", fixture_path, user,
                 ("--test-response-delay-ms", "1000")) as (process, socket_path, events):
        connection.execute("SET statement_timeout='50ms'")
        result = phase(args, spec, root, "cancel", connection, process, socket_path, events, digest,
                       expected_sqlstate=CANCEL_SQLSTATE)
        connection.execute("SET statement_timeout='5s'")
        recovery = (phase(args, spec, root, "recovery", connection, process, socket_path, events, digest)
                    if result.safe else skipped("recovery", "cancel_state_unsafe_or_unknown"))
        return [result, recovery]


def disconnect_case(args, spec, connection, user, fixture_path, digest):
    root = args.root / "provider_disconnect_and_recovery"
    root.mkdir()
    with gateway(args, root / "old_gateway", "disconnect", fixture_path, user,
                 ("--test-disconnect-on-task",)) as (process, socket_path, events):
        result = phase(args, spec, root, "disconnect", connection, process, socket_path, events, digest,
                       expected_sqlstate=DISCONNECT_SQLSTATE, expected_tasks=0)
    if not result.safe:
        return [result, skipped("recovery", "disconnect_state_unsafe_or_unknown")]
    with gateway(args, root / "new_gateway", "disconnect-recovery", fixture_path, user) as (process, socket_path, events):
        recovery = phase(args, spec, root, "recovery", connection, process, socket_path, events, digest)
    return [result, recovery]


def exit_case(args, spec, connection, user, fixture_path, digest):
    root = args.root / "gateway_exit_and_recovery"
    root.mkdir()
    with gateway(args, root / "old_gateway", "exit", fixture_path, user) as (process, socket_path, events):
        alive = phase(args, spec, root, "alive", connection, process, socket_path, events, digest)
        if not alive.safe:
            return [alive, skipped("absent", "alive_state_unknown"), skipped("recovery", "alive_state_unknown")]
        process.terminate()
        process.wait(timeout=10)
        old_returncode = process.returncode
    absent = phase(args, spec, root, "absent", connection, None, socket_path, lambda: [], digest,
        require_sessions=False, expected_sqlstate=GATEWAY_EXIT_SQLSTATE,
        extra_checks=lambda: ([{"metric": "gateway_not_absent"}]
                              if old_returncode is None or socket_path.exists() else []))
    if not absent.safe:
        return [alive, absent, skipped("recovery", "absent_state_unsafe_or_unknown")]
    with gateway(args, root / "new_gateway", "exit-recovery", fixture_path, user) as (process, recovery_socket, events):
        recovery = phase(args, spec, root, "recovery", connection, process, recovery_socket, events, digest)
    return [alive, absent, recovery]


def execute_cases(args, spec):
    import pwd
    from .runtime_helpers import isolated_pg18_cluster
    user = pwd.getpwnam("postgres")
    os.chown(args.root, user.pw_uid, user.pw_gid)
    fixture_path, digest = fixture(args.root)
    with isolated_pg18_cluster(args.prefix, args.root, user) as connection:
        connection.execute("CREATE TABLE resource_rows(id integer, payload text) WITH (autovacuum_enabled=false, toast.autovacuum_enabled=false)")
        connection.execute(f"INSERT INTO resource_rows SELECT n,repeat('x',100000) FROM generate_series(1,{spec.rows_per_round}) n")
        connection.execute("VACUUM ANALYZE resource_rows")
        connection.execute("SET statement_timeout='300s'")
        safe = True
        for name, check in zip(REQUIRED_PHASES, (stress_case, cancel_case, disconnect_case, exit_case)):
            if safe:
                phases = check(args, spec, connection, user, fixture_path, digest)
            else:
                phases = [skipped(p, "prior_case_unsafe_or_unknown") for p in REQUIRED_PHASES[name]]
            safe = all(p.safe for p in phases)
            yield name, phases


def run(args):
    spec = RunSpec("diagnostic" if args.diagnostic else "formal")
    try:
        # Atomic ownership. Existing roots are read-only to this invocation.
        args.root.mkdir(parents=False, exist_ok=False)
    except OSError:
        print("run_root_unavailable: choose a new result directory", file=sys.stderr)
        return 3
    cases = {}
    error = None
    interrupted = None
    try:
        (args.root / "build").mkdir()
        save_json(args.root / "manifest.json", {"state": "started", "spec": spec.manifest(),
                  "requested_commit": args.commit})
        save_json(args.root / "source_identity.json", preflight(args))
        build_client(args.repo / "code/src/experiments/postgresql/resource_client_v3.c",
                     args.root / "build/resource_client_v3", args.prefix)
        for name, phases in execute_cases(args, spec):
            report = case_report(name, phases, spec)
            case_root = args.root / name
            case_root.mkdir(exist_ok=True)
            save_json(case_root / "case_report.json", report)
            cases[name] = report
            save_json(args.root / "progress.json", {"completed_cases": list(cases)})
    except BaseException as failure:
        error = {"type": type(failure).__name__, "reason": "runner_failure",
                 "sqlstate": getattr(failure, "sqlstate", None)}
        if isinstance(failure, (KeyboardInterrupt, SystemExit)):
            interrupted = failure
    finally:
        report = run_report(cases, spec, runner_error=error)
        save_json(args.root / "summary.json", report)
        save_json(args.root / "SHA256SUMS.json", hashes(args.root))
    if interrupted is not None:
        raise interrupted
    return report["exit_code"]


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repo", "root", "prefix"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--diagnostic", action="store_true", help="Real 1x100 fixture workload; no formal qualification")
    return parser.parse_args(argv)


def main(argv=None):
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
