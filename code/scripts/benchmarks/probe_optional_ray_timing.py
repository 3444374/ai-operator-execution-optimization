#!/usr/bin/env python3
"""Four bounded Ray Data fixture graphs, never a model or an existing Ray cluster.

Without --execute this prints the plan. Execution requires a new outside-Git
directory. A closed ledger may supply only its first 2,520 (sequence, phase,
kind) values; source URLs, bodies, timestamps and identity markers are not copied.
The existing 192 initialization samples are evidence inputs, not repeated here.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import contextmanager
import hashlib
import importlib.util
from importlib.metadata import version
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid


CODE = Path(__file__).resolve().parents[2]
REPOSITORY = CODE.parent
if str(CODE) not in sys.path:
    sys.path.insert(0, str(CODE))

from src.baselines.common.private_artifacts import (
    new_private_directory, require_outside_git, write_private_json,
)
from src.baselines.common.redact import redact_text


CASES = (("forward-0", 0), ("forward-2520", 2520),
         ("reverse-2520", 2520), ("reverse-0", 0))
MODEL_REQUESTS = 0
HTTP_LIMIT = 8
REPLY = b'{"fixture":true}'


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            value.update(chunk)
    return value.hexdigest()


def ast_symbols(text):
    return {node.name: hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()
            for node in ast.parse(text).body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}


def graph_calls(text):
    """Compare the original graph's actual vendor calls through timing wrappers."""
    function = next((node for node in ast.parse(text).body
                     if isinstance(node, ast.FunctionDef) and node.name == "_ray_rows"), None)
    if function is None:
        return None
    signatures = []
    names = {"options.validate_backend", "ray.is_initialized", "ray.data.from_items",
             "HttpRequestProcessorConfig", "build_processor", "stage.map_batches_kwargs.update",
             "processor", "result.iter_rows", "_outcome_from_wire", "close", "executor.shutdown"}

    class Calls(ast.NodeVisitor):
        def visit_If(self, node):
            if isinstance(node.test, ast.Name) and node.test.id == "detailed":
                # The diagnostic explicitly enables this existing adapter branch.
                for child in node.body:
                    self.visit(child)
            else:
                self.generic_visit(node)

        def visit_Call(self, node):
            if (ast.unparse(node.func) == "actions.append" and len(node.args) == 1 and
                    isinstance(node.args[0], ast.Tuple) and len(node.args[0].elts) == 2 and
                    isinstance(node.args[0].elts[1], ast.Name) and node.args[0].elts[1].id == "close"):
                # The adapter passes its unchanged iterator close into observed cleanup.
                signatures.append(ast.dump(ast.Call(func=ast.Name(id="close", ctx=ast.Load()),
                                                      args=[], keywords=[]), include_attributes=False))
            if ast.unparse(node.func) in names:
                signatures.append(ast.dump(node, include_attributes=False))
            self.generic_visit(node)

    Calls().visit(function)
    return signatures


def source_mapping(handoff_path=None):
    """Record source identity without claiming an instrumented file is identical."""
    sources = {
        "pre_send_ledger.py": CODE / "scripts/benchmarks/sembench_pre_send_ledger.py",
        "send_counts.py": CODE / "src/experiments/send_counts.py",
        "native_calls.py": CODE / "src/baselines/text/frameworks/native_calls.py",
    }
    result = {name: {"working_sha256": digest(path),
                     "working_symbol_ast_sha256": ast_symbols(path.read_text())}
              for name, path in sources.items()}
    result["native_calls.py"]["working_vendor_graph_calls"] = graph_calls(sources["native_calls.py"].read_text())
    try:
        result["public_head"] = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPOSITORY, text=True).strip()
        for name, path in sources.items():
            head = subprocess.check_output(["git", "show", "HEAD:" + str(path.relative_to(REPOSITORY))],
                                           cwd=REPOSITORY, text=True)
            result[name]["head_sha256"] = hashlib.sha256(head.encode()).hexdigest()
            result[name]["head_symbol_ast_sha256"] = ast_symbols(head)
            if name == "native_calls.py":
                result[name]["head_vendor_graph_calls"] = graph_calls(head)
    except subprocess.CalledProcessError:
        # A staged source-only bundle can still record every actual working file.
        result["public_head"] = "unavailable: source bundle has no Git metadata"
    if handoff_path is None:
        result["retained_mapping"] = "unavailable: --handoff was not supplied"
        return result
    handoff = json.loads(Path(handoff_path).read_text())
    result["handoff_sha256"] = digest(handoff_path)
    result["existing_initialization_samples"] = handoff["replay"]["sample_count"]
    result["retained_harness"] = {}
    for name, record in handoff["actual_ray_helpers"].items():
        if digest(record["path"]) != record["sha256"]:
            raise RuntimeError("retained source SHA differs: " + name)
        result["retained_harness"][name] = dict(sha256=record["sha256"],
            role="identity verified; this closed helper is not invoked")
    result["retained_sample_evidence"] = [dict(sha256=record["sha256"], bytes=record["bytes"])
        for record in handoff.get("existing_evidence", [])
        if Path(record["path"]).name == "ray-budget-diagnostic.json"]
    records = {name: handoff["actual_ray_helpers"][name]
               for name in ("pre_send_ledger.py", "send_counts.py")}
    records["native_calls.py"] = handoff["retained_ray_source"][
        "code/src/baselines/text/frameworks/native_calls.py"]
    for name, record in records.items():
        path = Path(record["path"])
        if digest(path) != record["sha256"]:
            raise RuntimeError("retained source SHA differs: " + name)
        retained = ast_symbols(path.read_text())
        current = result[name]["working_symbol_ast_sha256"]
        head = result[name].get("head_symbol_ast_sha256", {})
        result[name].update(retained_sha256=record["sha256"], retained_symbol_ast_sha256=retained,
            retained_matches_head_symbols=sorted(key for key in retained if retained[key] == head.get(key)),
            retained_matches_working_symbols=sorted(key for key in retained if retained[key] == current.get(key)))
        if name == "native_calls.py":
            calls = graph_calls(path.read_text())
            result[name]["retained_vendor_graph_calls"] = calls
            result[name]["working_vendor_calls_match_retained"] = (
                calls is not None and calls == result[name]["working_vendor_graph_calls"])
    result["mapping_limits"] = [
        "Private reserve/activate contain a different task-completion budget window.",
        "Private NativeCallExecutor accepts timeout_s=None; this bounded fixture uses a finite timeout.",
        "Instrumented initialize/history/HTTP/graph functions have new timing boundaries; file SHAs differ.",
        "Original graph claim is limited to the retained _ray_rows call sequence and vendor settings.",
    ]
    return result


def verify_graph_mapping(mapping):
    graph = mapping["native_calls.py"]
    reference = graph.get("retained_vendor_graph_calls", graph.get("head_vendor_graph_calls"))
    if reference is None or graph["working_vendor_graph_calls"] != reference:
        raise RuntimeError("current vendor graph calls/settings do not map to retained source or public HEAD")


def verify_working_sources(mapping):
    for name, path in (("pre_send_ledger.py", CODE / "scripts/benchmarks/sembench_pre_send_ledger.py"),
                       ("send_counts.py", CODE / "src/experiments/send_counts.py"),
                       ("native_calls.py", CODE / "src/baselines/text/frameworks/native_calls.py")):
        if digest(path) != mapping[name]["working_sha256"]:
            raise RuntimeError("probe source changed after mapping: " + name)


def verified_source_mapping_json(path):
    """Use locally verified provenance in a source-only bundle, without private reads."""
    payload = json.loads(Path(path).read_text())
    mapping = payload.get("source_mapping", payload)
    verify_working_sources(mapping)
    verify_graph_mapping(mapping)
    return dict(mapping, source_mapping_json_sha256=digest(path),
                mapping_source="local_verified_handoff", private_helpers_rechecked_here=False)


def prefix_rows(source=None):
    """Bounded, immutable read; never open the source in SQLite write mode."""
    if source is None:
        return [(index, "software", "fixture") for index in range(1, 2521)], {
            "kind": "synthetic phase/kind rows", "original_ledger_read": False}
    source = Path(source).resolve(strict=True)
    before = digest(source)
    for suffix in ("-wal", "-journal"):
        if Path(str(source) + suffix).exists():
            raise ValueError("prefix source must be a closed ledger without write sidecars")
    with sqlite3.connect(source.as_uri() + "?mode=ro&immutable=1", uri=True) as db:
        rows = list(db.execute("SELECT sequence,phase,kind FROM sends ORDER BY sequence LIMIT 2520"))
    if len(rows) != 2520 or [row[0] for row in rows] != list(range(1, 2521)):
        raise ValueError("prefix source needs 2520 contiguous retained rows")
    if digest(source) != before:
        raise RuntimeError("closed ledger changed during its immutable read")
    return rows, {"kind": "retained sequence/phase/kind only", "sha256": before,
                  "original_ledger_read": True, "original_ledger_unchanged": True}


@contextmanager
def environment(values):
    previous = {key: os.environ.get(key) for key in values}
    try:
        for key, value in values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def ledger_snapshot(root, counters):
    with sqlite3.connect(Path(root) / "sends.sqlite") as db:
        db.execute("BEGIN IMMEDIATE")
        values = counters.snapshot(db)
        return dict(rows=db.execute("SELECT count(*) FROM sends").fetchone()[0],
                    sequence_max=db.execute("SELECT coalesce(max(sequence),0) FROM sends").fetchone()[0],
                    history_matches_counters=counters._history(db) == values,
                    counters={key: list(value) for key, value in values.items()})


def seed_ledger(root, rows):
    """Use the actual initializer, then append synthetic identities to a new DB."""
    started = time.monotonic_ns()
    counters = load_module("send_counts", CODE / "src/experiments/send_counts.py")
    with environment({"MODELRUN_ROOT": root, "SEMLOOM_DETAILED_TIMING_DIR": None}):
        ledger = load_module("_probe_seed_ledger", CODE / "scripts/benchmarks/sembench_pre_send_ledger.py")
        ledger.initialize()
        with sqlite3.connect(root / "sends.sqlite") as db:
            db.executemany("INSERT INTO sends(sequence,phase,kind,returned_ns) VALUES(?,?,?,0)", rows)
        snapshot = ledger_snapshot(root, counters)
    if not snapshot["history_matches_counters"] or snapshot["rows"] != len(rows):
        raise RuntimeError("seed counters do not match the retained prefix")
    return dict(started_ns=started, ended_ns=time.monotonic_ns(), snapshot=snapshot,
                scope="pre-probe seed; excluded from all query timings")


def plan(*, expected_pyarrow_version="24.0.0"):
    return dict(schema="semloom.optional_ray_fixture_plan.v1", cases=[
        dict(case_id=case, prefix_rows=count, complete_http_calls=2) for case, count in CASES],
        model_requests=MODEL_REQUESTS, maximum_http_posts=HTTP_LIMIT,
        ray_version="2.56.1", pyarrow_version=expected_pyarrow_version, cpu_slots=4, gpu_slots=0,
        graph_timeout_seconds=60, total_timeout_seconds=180,
        source="NativeCallExecutor('ray-data'); original _ray_rows vendor API graph",
        order="forward then reverse", owner_lifecycle="independent new owner per graph",
        interpretation="history size vs order under new owners; does not isolate repeated-owner effects",
        unknown=["Ray scheduler queue wait", "actor-ready instant before its first adapter callback"],
        stop_conditions=["external POST or model configuration", "existing Ray connection requested",
            "more than two POSTs in one graph or eight overall", "history/counter mismatch",
            "source ledger SHA changes", "graph timeout", "unconfirmed cleanup", "existing output path"])


SITE = '''"""Required fixture accounting; Python must not ignore a failed hook."""
import os,sys,time
try:
 if os.environ.get('SEMLOOM_RAY_FIXTURE_CHILD')=='1':
  entered=time.monotonic_ns()
  from src.experiments.execution_timing import get_timing
  timing=get_timing()
  timing.mark('probe.python_startup_hook.enter',monotonic_ns=entered,worker_pid=os.getpid(),query_id=os.environ['SEMLOOM_PROBE_CASE'])
  with timing.span('probe.python_startup_hook',worker_pid=os.getpid(),query_id=os.environ['SEMLOOM_PROBE_CASE']):
   from pre_send_ledger import install
   install()
except BaseException as error:
 try:os.write(2,('required fixture bootstrap failed: '+type(error).__name__+'\\n').encode())
 finally:os._exit(78)
'''


def stage_case(root, case_id, rows, run_id, *, ray_temp_dir=None):
    root.mkdir(mode=0o700)
    validation = root / "validation"
    validation.mkdir(mode=0o700)
    sources = {"pre_send_ledger.py": CODE / "scripts/benchmarks/sembench_pre_send_ledger.py",
               "send_counts.py": CODE / "src/experiments/send_counts.py",
               "execution_timing.py": CODE / "src/experiments/execution_timing.py",
               "buffered_events.py": CODE / "src/experiments/buffered_events.py",
               "ray_map_transport.py": CODE / "src/execution_provider/adapters/ray_map_transport.py"}
    for name, path in sources.items():
        (validation / name).write_bytes(path.read_bytes())
    (validation / "sitecustomize.py").write_text(SITE)
    files = {path.name: digest(path) for path in validation.iterdir()}
    write_private_json(root / "harness-identity.json", {"files": files})
    software = sum(phase == "software" for _, phase, _ in rows)
    write_private_json(root / "state.json", {
        "minimal_sync_public_cli_software": {"max_logical": 0, "max_posts": software + 2},
        "matrix": {"stages": {phase: [0, 0, 0] for phase in ("quality", "single", "dual")},
                   "hard_logical_total": 0, "hard_transmission_total": 0}})
    seed = seed_ledger(root, rows)
    config = dict(case_id=case_id, prefix_rows=len(rows), run_id=run_id, seed=seed,
                  code_root=str(CODE), files=files, ray_temp_dir=str(ray_temp_dir or root / "ray-runtime"))
    write_private_json(root / "case.json", config)
    return config


def _fixture(ledger, timing):
    """One localhost server with at most two accepted, durably reserved bodies."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    received, failures = [], []
    lock = threading.Lock()
    attempts = [0]

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_POST(self):
            entered = time.monotonic_ns()
            with lock:
                attempts[0] += 1
                allowed = attempts[0] <= 2
            try:
                if not allowed or self.path != "/fixture":
                    raise RuntimeError("fixture receive limit or route exceeded")
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 1024:
                    raise ValueError("fixture requires a bounded complete request body")
                body = self.rfile.read(size)
                if len(body) != size:
                    raise ValueError("fixture request body was incomplete")
                sequence = ledger.claim_fixture(body)
                with lock:
                    received.append(dict(sequence=sequence, received_ns=entered,
                                         body_sha256=hashlib.sha256(body).hexdigest()))
                timing.mark("probe.fixture.received", monotonic_ns=entered, request_id=sequence,
                            query_id=os.environ["SEMLOOM_PROBE_CASE"])
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(REPLY)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(REPLY)
                self.wfile.flush()
            except BaseException as error:
                with lock:
                    failures.append(error)
                self.send_error(500, "fixture accounting failed")

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, name="fixture-http", daemon=True)
    thread.start()
    return server, thread, received, failures, attempts


def summarize_bootstrap(root, query_start, query_end, driver_domain):
    """Compare clocks only after the existing Linux clock-domain verification."""
    spans = {}
    for path in sorted((root / "timings").glob("timing-*.jsonl")):
        with path.open() as stream:
            for line in stream:
                event = json.loads(line)
                if event["operation"] not in ("probe.python_startup_hook", "probe.python_startup_hook.enter"):
                    continue
                key = event["process_instance"]
                row = spans.setdefault(key, dict(pid=event["pid"], process_instance=event["process_instance"],
                    clock_domain=event["clock_domain"], clock_verified=event["clock_verified"],
                    actor_id="unavailable: bootstrap is a process hook, not an actor callback"))
                field = ("entry_ns" if event["operation"].endswith(".enter") else
                         "started_ns" if event["event"] == "span_started" else "ended_ns")
                row[field] = event["monotonic_ns"]
                if len(spans) > 256:
                    raise RuntimeError("unexpected bootstrap count exceeds diagnostic bound")
    for row in spans.values():
        start, end = row.get("entry_ns", row.get("started_ns")), row.get("ended_ns")
        row["scope"] = "captured Python hook entry to helper ready; process creation/pre-hook interpreter startup excluded"
        if start is None or end is None:
            row["status"] = "unfinished: worker may have exited before writer drain"
            continue
        row["hook_seconds"] = (end-start)/1e9
        if row["clock_verified"] and row["clock_domain"] == driver_domain:
            row.update(status="completed",
                before_query_hook_seconds=max(0, min(end, query_start)-start)/1e9,
                relation="ready_before_query" if end <= query_start else
                         "started_after_query" if start >= query_start else "overlaps_query_start")
            row["inside_query_hook_seconds"] = (max(0, min(end, query_end)-max(start, query_start))/1e9
                if type(query_end) is int else "unavailable: no native EOF observed")
        else:
            row["status"] = "cross-process comparison unavailable: unverified or different clock domain"
    return list(spans.values())


def writer_coverage(root, owner_pid):
    """An exit status does not establish that a process drained its event writer."""
    coverage = []
    for path in sorted((root / "timings").glob("timing-*.jsonl")):
        count, first, malformed = 0, None, 0
        with path.open() as stream:
            for line in stream:
                count += 1
                try:
                    event = json.loads(line)
                    first = first or event
                except ValueError:
                    malformed += 1
                if count > 65536:
                    raise RuntimeError("timing file exceeds bounded fixture event history")
        marker = path.with_suffix(".closed.json")
        closed = json.loads(marker.read_text()) if marker.exists() else {}
        snapshot = closed.get("writer") or {}
        complete = bool(first and not malformed and closed.get("status") == "closed" and
            closed.get("process_instance") == first.get("process_instance") and
            closed.get("pid") == first.get("pid") and snapshot.get("closed") is True and
            snapshot.get("error_type") is None and snapshot.get("pending_bytes") == 0 and
            snapshot.get("accepted_events") == snapshot.get("written_events") == count)
        coverage.append(dict(file=path.name, pid=first.get("pid") if first else None,
            events=count, malformed_lines=malformed, writer_complete=complete,
            reason="verified close marker and exact written count" if complete else
                   "unknown/incomplete: missing or inconsistent close marker; no terminal event inferred"))
    return dict(processes=coverage, driver_verified=any(
        row["pid"] == owner_pid and row["writer_complete"] for row in coverage),
        workers_with_unverified_close=sum(row["pid"] != owner_pid and not row["writer_complete"] for row in coverage))


def finish_case_evidence(root, record, timing, close_writer, *, ray, cleanup_started,
                         cleanup_ended, primary=None, cleanup_error=None):
    """Keep close and evidence attempts independent; never replace an execution error."""
    failures = [("resource_cleanup", cleanup_error)] if cleanup_error is not None else []

    def attempt(stage, call):
        try:
            return call()
        except BaseException as error:
            failures.append((stage, error))
            return None

    record["owner_cleanup_seconds"] = (cleanup_ended-cleanup_started)/1e9
    disconnected = attempt("ray_disconnect_check", lambda: ray is None or not ray.is_initialized())
    record["owned_ray_disconnected"] = disconnected if type(disconnected) is bool else "unknown"
    record["resource_cleanup_confirmed"] = cleanup_error is None and disconnected is True
    attempt("cleanup_timing", lambda: timing.record_interval("probe.owner_cleanup", cleanup_started, cleanup_ended,
        status="error" if cleanup_error else "completed", scope="owner teardown after native graph EOF",
        error_type=type(cleanup_error).__name__ if cleanup_error else None))
    attempt("timing_close", close_writer)
    if "query_start_ns" in record:
        bootstrap = attempt("bootstrap_summary", lambda: summarize_bootstrap(root, record["query_start_ns"],
                            record.get("query_eof_ns"), timing.clock_domain))
        if bootstrap is not None:
            record["bootstrap"] = bootstrap
    coverage = attempt("writer_coverage", lambda: writer_coverage(root, os.getpid()))
    record["timing_writer_coverage"] = coverage or dict(driver_verified=False,
        reason="unknown: writer coverage could not be checked")
    if not record["timing_writer_coverage"]["driver_verified"]:
        failures.append(("writer_coverage", RuntimeError("driver timing writer has no verified close marker")))
    record["cleanup_confirmed"] = record["resource_cleanup_confirmed"] and not failures
    first = primary or (failures[0][1] if failures else None)
    if first is not None:
        record["status"] = "failed"
    if failures:
        for field, resource in (("cleanup_errors", True), ("evidence_errors", False)):
            record[field] = [dict(stage=stage, error_type=type(error).__name__, error=redact_text(str(error)))
                for stage, error in failures if (stage in ("resource_cleanup", "ray_disconnect_check")) == resource]
        if primary is None:
            record.update(status="failed", error_type=type(first).__name__, error=redact_text(str(first)))
        for stage, error in failures:
            if error is not first:
                first.add_note("Fixture " + stage + " also failed: " + type(error).__name__)
    try:
        write_private_json(root / "result.json", record)
    except BaseException as observation:
        if first is None:
            first = observation
        else:
            first.add_note("Fixture result recording also failed: " + type(observation).__name__)
    if primary is None and first is not None:
        raise first


def stop_owned_process_group(process, primary):
    """Only the PGID created by this Popen(start_new_session=True) is addressed."""
    status = dict(process_group=process.pid, termination_requested=False, group_exit_confirmed=False)
    errors = []

    def send(sig):
        try:
            os.killpg(process.pid, sig)
            status["termination_requested"] = True
        except ProcessLookupError:
            status["group_exit_confirmed"] = True
        except BaseException as error:
            errors.append(error)

    send(signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        send(signal.SIGKILL)
        try:
            process.wait(timeout=5)
        except BaseException as error:
            errors.append(error)
    except BaseException as error:
        errors.append(error)
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        status["group_exit_confirmed"] = True
    except BaseException as error:
        errors.append(error)
    else:
        # The leader may already be reaped while its owned children remain.
        send(signal.SIGKILL)
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            status["group_exit_confirmed"] = True
        except BaseException as error:
            errors.append(error)
    if not status["group_exit_confirmed"]:
        errors.append(RuntimeError("owned process-group exit is unconfirmed"))
    status["errors"] = [dict(error_type=type(error).__name__, error=redact_text(str(error))) for error in errors]
    for error in errors:
        primary.add_note("Owned child cleanup also failed: " + type(error).__name__)
    return status


def supervise_owner(process, reader, reader_errors, *, timeout, case_id):
    """Return the first failure after attempting owned cleanup and log drainage."""
    primary, cleanup = None, None
    try:
        process.wait(timeout=timeout)
        if process.returncode != 0:
            raise RuntimeError("fixture owner failed; see retained " + case_id + "/owner.log")
    except BaseException as error:
        primary = error
        cleanup = stop_owned_process_group(process, primary)
    try:
        reader.join(5)
        if reader.is_alive():
            reader_errors.append(RuntimeError("fixture subprocess log stream did not close"))
    except BaseException as error:
        reader_errors.append(error)
    if reader_errors:
        if primary is None:
            primary = reader_errors[0]
            cleanup = stop_owned_process_group(process, primary)
            try:
                reader.join(5)
            except BaseException as error:
                primary.add_note("Child log join also failed: " + type(error).__name__)
        for error in reader_errors:
            if error is not primary:
                primary.add_note("Child log observation also failed: " + type(error).__name__)
    if cleanup is not None:
        try:
            cleanup["log_stream_closed"] = not reader.is_alive()
        except BaseException as error:
            cleanup["log_stream_closed"] = "unknown"
            primary.add_note("Child log state check also failed: " + type(error).__name__)
    return primary, cleanup


def read_owner_result(root, case_id, supervision_error=None):
    """Retain a prior owner failure when its final evidence is missing or damaged."""
    try:
        result = json.loads((root / "result.json").read_text())
        if not isinstance(result, dict):
            raise ValueError("owner result must be a JSON object")
    except BaseException as error:
        result = dict(case_id=case_id, status="failed", http_posts=None, cleanup_confirmed=False,
            supervisor_cleanup_status="unknown", read_error=dict(error_type=type(error).__name__,
                error=redact_text(str(error))), reason="owner result unreadable; request count and cleanup unknown")
        if supervision_error is not None:
            supervision_error.add_note("Owner result read also failed: " + type(error).__name__)
            return result, supervision_error
        return result, error
    return result, supervision_error


def run_case(root):
    """Child entry; sitecustomize has already installed the actual public helper."""
    root = root.resolve(strict=True)
    require_outside_git(root)
    config = json.loads((root / "case.json").read_text())
    if (os.environ.get("SEMLOOM_RAY_FIXTURE_CHILD") != "1" or
            os.environ.get("MODELRUN_ROOT") != str(root)):
        raise RuntimeError("fixture child requires its staged, isolated environment")
    from src.experiments.execution_timing import close_timing, get_timing
    import pre_send_ledger as ledger
    import send_counts as counters
    timing = get_timing()
    ledger.assert_hooks()
    server = thread = ray = executor = None
    primary = None
    record = dict(case_id=config["case_id"], prefix_rows=config["prefix_rows"],
                  models_started=0, owner_pid=os.getpid(), status="started", seed=config["seed"],
                  clock_domain=timing.clock_domain, clock_verified=timing.clock_verified)
    try:
        server, thread, received, failures, attempts = _fixture(ledger, timing)
        os.environ["MODELRUN_FIXTURE_URL"] = "http://127.0.0.1:" + str(server.server_port) + "/fixture"
        with timing.bind(query_id=config["case_id"], owner_id=config["case_id"], backend="ray-data"):
            owner_startup = time.monotonic_ns()
            with timing.span("probe.owner_startup", scope="caller-owned Ray init; outside query"):
                import ray
                if ray.__version__ != "2.56.1" or ray.is_initialized():
                    raise RuntimeError("requires a fresh caller-owned Ray 2.56.1 runtime")
                ray.init(address="local", num_cpus=4, num_gpus=0, include_dashboard=False,
                         log_to_driver=False, _temp_dir=config["ray_temp_dir"],
                         object_store_memory=128 * 1024 * 1024)
            record["owner_ray_startup_seconds"] = (time.monotonic_ns()-owner_startup)/1e9
            from src.baselines.text.frameworks.native_calls import NativeCall, NativeCallExecutor
            from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
            executor = NativeCallExecutor("ray-data", NativeGraphOptions(concurrency=8,
                ray_actors=2, ray_async_batches_per_actor=2, ray_batch_rows=1, source_blocks=2))
            ledger.activate("software", config["case_id"], time.time(), 90, 5)
            ledger.unit(config["case_id"])
            before = ledger_snapshot(root, counters)
            calls = tuple(NativeCall(config["case_id"] + ":" + str(index), index,
                json.dumps({"fixture_row": index, "fixture_case": config["case_id"]}).encode(),
                os.environ["MODELRUN_FIXTURE_URL"], (("Content-Type", "application/json"),), 10)
                for index in range(2))
            seen = []
            query_start = time.monotonic_ns()
            record["query_start_ns"] = query_start
            timing.mark("probe.query.start", monotonic_ns=query_start)
            with timing.span("probe.query", scope="first graph operation through observed native EOF"):
                for result in executor.iter_batch(calls, timeout_s=60):
                    if (result.transport_error or result.execution_error or result.response is None or
                            result.response.status_code != 200 or result.response.body != REPLY):
                        raise RuntimeError("native fixture result differs or has a transport error")
                    seen.append((result.call_id, result.row_id))
                    if len(seen) == 2:
                        record["result_usable_ns"] = time.monotonic_ns()
                        timing.mark("probe.results_usable", monotonic_ns=record["result_usable_ns"])
            query_end = time.monotonic_ns()
            timing.mark("probe.query.eof", monotonic_ns=query_end)
            record.update(query_start_ns=query_start, query_eof_ns=query_end,
                          query_seconds=(query_end-query_start)/1e9,
                          result_usable_seconds=(record["result_usable_ns"]-query_start)/1e9,
                          query_to_first_fixture_seconds=(min(row["received_ns"] for row in received)-query_start)/1e9,
                          close_report=executor.last_close_report, fixture_receives=received,
                          http_posts=attempts[0], before=before, after=ledger_snapshot(root, counters))
            expected = {(call.call_id, call.row_id) for call in calls}
            if len(seen) != 2 or set(seen) != expected or attempts[0] != 2 or len(received) != 2:
                raise RuntimeError("fixture must receive exactly two distinct complete calls")
            if failures:
                raise failures[0]
            if not executor.last_close_report or not executor.last_close_report.get("clean"):
                raise RuntimeError("native graph cleanup was not confirmed")
            after = record["after"]
            expected_counters = dict(before["counters"])
            pair = before["counters"]["software"]
            expected_counters["software"] = [pair[0] + 2, pair[1]]
            if (not after["history_matches_counters"] or after["rows"] != before["rows"] + 2 or
                    after["sequence_max"] != before["sequence_max"] + 2 or
                    after["counters"] != expected_counters):
                raise RuntimeError("graph history/counter increment differs from exactly two fixture sends")
            with sqlite3.connect(root / "sends.sqlite") as db:
                suffix = list(db.execute("SELECT kind,status,error_type FROM sends WHERE sequence>? ORDER BY sequence",
                                         (config["prefix_rows"],)))
                claims = db.execute("SELECT count(*) FROM fixture_received").fetchone()[0]
            if suffix != [("fixture", 200, ""), ("fixture", 200, "")] or claims != 2:
                raise RuntimeError("terminal fixture reserves or received claims are incomplete")
            record["status"] = "completed"
    except BaseException as error:
        primary = error
        record.update(status="failed", error_type=type(error).__name__, error=redact_text(str(error)))
        raise
    finally:
        cleanup_started = time.monotonic_ns()
        cleanup_error = None
        if server is not None:
            record.update(http_posts=attempts[0], fixture_receives=received)
        for call in ((executor.close if executor is not None else lambda: None),
                     (server.shutdown if server is not None else lambda: None),
                     (server.server_close if server is not None else lambda: None),
                     (lambda: thread.join(2) if thread is not None else None),
                     (ray.shutdown if ray is not None else lambda: None)):
            try:
                call()
            except BaseException as error:
                cleanup_error = cleanup_error or error
        cleanup_ended = time.monotonic_ns()
        finish_case_evidence(root, record, timing, close_timing, ray=ray,
            cleanup_started=cleanup_started, cleanup_ended=cleanup_ended,
            primary=primary, cleanup_error=cleanup_error)


def execute(args):
    if sys.platform != "linux" or not Path("/proc/self/stat").exists():
        raise RuntimeError("fixture execution requires Linux /proc; do not install Ray into another environment")
    if any(os.environ.get(name) for name in ("RAY_ADDRESS", "MODELRUN_ROOT", "MODELRUN_MODEL_ENDPOINTS")):
        raise RuntimeError("refuse inherited Ray address or model-run configuration")
    packages = {name: version(name) for name in ("ray", "pyarrow", "aiohttp")}
    if packages["ray"] != "2.56.1" or packages["pyarrow"] != args.expected_pyarrow_version:
        raise RuntimeError("fixture requires existing Ray 2.56.1 / Arrow " + args.expected_pyarrow_version +
                           "; actual=" + json.dumps(packages) + "; no dependencies are installed")
    rows, prefix = prefix_rows(args.prefix_source)
    if args.source_mapping_json is not None:
        mapping = verified_source_mapping_json(args.source_mapping_json)
    else:
        mapping = source_mapping(args.handoff)
        verify_graph_mapping(mapping)
    ray_base = (args.ray_temp_base or args.output.parent).resolve()
    require_outside_git(ray_base)
    if not ray_base.is_dir() or len(str(ray_base).encode()) > 26:
        raise ValueError("--ray-temp-base must be an existing short private directory (at most 26 bytes); "
                         "use the target data disk to leave room for Ray's Unix socket names")
    new_private_directory(args.output)
    run_id = uuid.uuid4().hex
    from src.execution_provider.adapters.ray_map_transport import _clock_domain
    supervisor_domain = _clock_domain()
    report = dict(plan(expected_pyarrow_version=args.expected_pyarrow_version), run_id=run_id,
                  packages=packages, source_mapping=mapping,
                  prefix_source=prefix, cases=[], models_started=0, http_posts=0, status="started",
                  supervisor_clock_domain=supervisor_domain, launched_owners=[])
    write_private_json(args.output / "plan.json", report)
    deadline = time.monotonic() + 180
    primary = None
    try:
        for case_id, count in CASES:
            if time.monotonic() >= deadline:
                raise TimeoutError("total fixture budget elapsed before next owner launch")
            verify_working_sources(mapping)
            root = (args.output / case_id).resolve()
            ray_temp = ray_base / ("rt-" + uuid.uuid4().hex[:8])
            new_private_directory(ray_temp)
            stage_case(root, case_id, rows[:count], run_id, ray_temp_dir=ray_temp)
            child_env = dict(os.environ, MODELRUN_ROOT=str(root), MODELRUN_MODEL_ENDPOINTS="[]",
                MODELRUN_ACCOUNT="1", SEMLOOM_RAY_FIXTURE_CHILD="1", SEMLOOM_PROBE_CASE=case_id,
                SEMLOOM_DETAILED_TIMING_DIR=str(root / "timings"), SEMLOOM_TIMING_RUN_ID=run_id,
                RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY="2", PYTHONDONTWRITEBYTECODE="1",
                CUDA_VISIBLE_DEVICES="",
                PYTHONPATH=os.pathsep.join((str(root / "validation"), str(CODE))))
            launched = time.monotonic_ns()
            with (root / "owner.log").open("x") as log:
                process = subprocess.Popen([sys.executable, "-B", str(Path(__file__).resolve()), "--_case", str(root)],
                    cwd=REPOSITORY, env=child_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    start_new_session=True)
                report["launched_owners"].append(dict(case_id=case_id, pid=process.pid,
                    process_group=process.pid, launch_ns=launched, owned_ray_temp_dir=str(ray_temp)))
                discarded = [0]
                reader_errors = []
                def drain():
                    written = 0
                    try:
                        while chunk := process.stdout.readline(8192):
                            value = redact_text(chunk.decode(errors="replace"))
                            try:
                                if written + len(value.encode()) <= 1024 * 1024 and not reader_errors:
                                    log.write(value)
                                    written += len(value.encode())
                                else:
                                    discarded[0] += len(chunk)
                            except BaseException as error:
                                if not reader_errors:
                                    reader_errors.append(error)
                                discarded[0] += len(chunk)
                    except BaseException as error:
                        reader_errors.append(error)
                reader = threading.Thread(target=drain, name="fixture-log", daemon=True)
                try:
                    reader.start()
                except BaseException as error:
                    report["http_posts"] = None
                    report["launched_owners"][-1]["group_cleanup"] = stop_owned_process_group(process, error)
                    raise
                supervision_error, group_cleanup = supervise_owner(process, reader, reader_errors,
                    timeout=max(.001, min(90, deadline-time.monotonic())), case_id=case_id)
                if group_cleanup is not None:
                    report["launched_owners"][-1]["group_cleanup"] = group_cleanup
            result, owner_error = read_owner_result(root, case_id, supervision_error)
            if owner_error is not None and group_cleanup is None:
                group_cleanup = stop_owned_process_group(process, owner_error)
                report["launched_owners"][-1]["group_cleanup"] = group_cleanup
            result.update(process_launch_ns=launched, log_discarded_bytes=discarded[0])
            if supervision_error is not None:
                result["supervision_error"] = dict(error_type=type(supervision_error).__name__,
                                                    error=redact_text(str(supervision_error)))
                if (not group_cleanup or not group_cleanup["group_exit_confirmed"] or
                        result.get("resource_cleanup_confirmed") is not True):
                    result.update(http_posts_observed=result.get("http_posts"), http_posts=None,
                                  cleanup_confirmed=False, supervisor_cleanup_status="unknown")
            if (result.get("clock_verified") and result.get("clock_domain") == supervisor_domain
                    and type(result.get("query_start_ns")) is int):
                result["process_launch_to_query_seconds"] = (result["query_start_ns"]-launched)/1e9
            else:
                result["process_launch_to_query_seconds"] = "unavailable: startup/query clocks are not verified equal"
            report["cases"].append(result)
            if result.get("http_posts") is None:
                report["http_posts"] = None
            else:
                report["http_posts"] += result["http_posts"]
            if owner_error is not None:
                raise owner_error
            if result["status"] != "completed" or not result["cleanup_confirmed"] or report["http_posts"] > HTTP_LIMIT:
                raise RuntimeError("fixture failed its result, request-count, or cleanup invariant")
            print(json.dumps({"case": case_id, "http_posts": result["http_posts"], "status": result["status"]}), flush=True)
        if report["http_posts"] != HTTP_LIMIT:
            raise RuntimeError("four complete graphs must receive exactly eight HTTP requests")
        verify_working_sources(mapping)
        report["status"] = "completed"
    except BaseException as error:
        primary = error
        report.update(status="failed", error_type=type(error).__name__, error=redact_text(str(error)))
        raise
    finally:
        if args.prefix_source is not None:
            try:
                unchanged = digest(args.prefix_source) == prefix["sha256"]
            except BaseException as observation:
                unchanged = False
                if primary is not None:
                    primary.add_note("Source ledger verification also failed: " + type(observation).__name__)
            report["prefix_source"]["original_ledger_unchanged"] = unchanged
            if not unchanged:
                report["status"] = "failed"
                if primary is None:
                    primary = RuntimeError("original closed ledger changed; stop and preserve evidence")
        try:
            write_private_json(args.output / "result.json", report)
        except BaseException as observation:
            if primary is None:
                raise
            primary.add_note("Fixture result recording also failed: " + type(observation).__name__)
        if primary is not None and sys.exception() is None:
            raise primary
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="run exactly the four planned CPU/localhost graphs")
    parser.add_argument("--output", type=Path, help="new private output directory, outside every Git checkout")
    parser.add_argument("--prefix-source", type=Path, help="closed ledger, opened immutable/read-only")
    parser.add_argument("--expected-pyarrow-version", default="24.0.0",
                        help="exact existing Arrow version to verify; default follows the checked model driver")
    provenance = parser.add_mutually_exclusive_group()
    provenance.add_argument("--handoff", type=Path, help="private retained-source and 192-sample handoff metadata")
    provenance.add_argument("--source-mapping-json", type=Path,
                            help="locally verified SHA/AST summary; recheck current public source SHAs without private helper reads")
    parser.add_argument("--ray-temp-base", type=Path,
                        help="existing short private data-disk directory for new owned Ray session directories")
    parser.add_argument("--_case", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args._case is not None:
        run_case(args._case)
        return 0
    if not args.execute:
        print(json.dumps(plan(expected_pyarrow_version=args.expected_pyarrow_version), indent=2))
        return 0
    if args.output is None:
        parser.error("--execute requires --output")
    return execute(args)


if __name__ == "__main__":
    raise SystemExit(main())
