"""Formal Movie Q3 boundary around the existing author-owned Filter carrier.

The controller owns the normal host lease, permanent query claim and clocks.
IMLane DBEnd/Coordinator/Lane/CPython retain their existing scheduling and close
protocol. This adapter adds no model pool, task scheduler or retry path.
"""

from dataclasses import asdict
import inspect
import os
from pathlib import Path
import signal
from subprocess import PIPE, Popen
import threading
import time
from types import SimpleNamespace

from integrations.imlane_records import owner, verify, verify_filter
from integrations.imlane_records.chain_runtime import AuthorRunGuard, check_external_lease
from integrations.imlane_records.client import RecordsBridge
from integrations.imlane_records.filter_host import FilterResults
from integrations.imlane_records.filter_workload import PROTOCOL, validate_terminal
from src.baselines.common.private_artifacts import require_outside_git, write_private_json
from src.baselines.common.redact import redact_text
from src.baselines.text.movie_records import MovieRecordSource
from src.experiments.cell_budget import ClaimedUnit
from src.experiments.lotus_records_movie import run_movie_query
from src.infrastructure import runner_lease
from src.semantic_methods.lotus.records import CallLedger, identity


BUDGET_SCOPE = "whole-query reservation; per-call actual usage from backend ledger"


def _binding(source, method, generation, engine, resources, query_id, profile, context):
    if type(source) is not MovieRecordSource:
        raise ValueError("the formal Movie adapter requires MovieRecordSource")
    if type(profile) is not dict or profile != asdict(resources):
        raise ValueError("IMLane profile must equal the resolved records resources")
    if type(context) is not dict:
        raise ValueError("the controller must supply the already-claimed query context")
    root = Path(context["output_dir"])
    require_outside_git(root)
    owned = root / "imlane-owner"
    if (root.is_symlink() or not root.is_dir() or owned.exists() or owned.is_symlink()
            or (root / "query.json").exists()):
        raise ValueError("IMLane requires a fresh private owner directory and query report")
    runtime, outer, budget, claim = (context[key] for key in (
        "runtime_binding", "owner_context", "budget", "claimed_budget"))
    if not isinstance(claim, ClaimedUnit) or claim._pid != os.getpid():
        raise ValueError("the query claim must belong to this controller worker process")
    expected = source.expected(query_id)
    if (not expected or budget["unit_id"] != query_id or claim.reservation.unit_id != query_id
            or claim.reservation.requests != len(expected) or engine.max_model_calls != len(expected)):
        raise ValueError("the existing query claim and engine cap must equal all declared occurrences")
    artifact_root = Path(runtime["imlane_artifact_root"])
    lease_path = artifact_root / runner_lease.HOST_SCOPE_DIR_NAME / runner_lease.LEASE_NAME
    if (Path(outer["artifact_root"]).resolve() != artifact_root.resolve()
            or Path(outer["lease_path"]).absolute() != lease_path.absolute()):
        raise ValueError("the controller lease must bind the normal IMLane artifact root")
    value = dict(schema="imlane.filter.run.v1", mode="local-model",
        method=asdict(method), generation=asdict(generation), engine=asdict(engine),
        resources=asdict(resources), source_manifest=str(source.manifest_path),
        query_ids=[query_id], max_total_calls=len(expected),
        preflight_report=runtime["runtime_preflight_report"],
        author_root=runtime["imlane_author_root"], method_python=runtime["imlane_method_python"],
        bridge_binary=runtime["imlane_bridge_binary"], artifact_root=str(artifact_root),
        lease_sha256=outer["lease_sha256"], owner_resource_limits=outer["owner_resource_limits"],
        gpu_idle_max_mib=outer["gpu_idle_max_mib"],
        model_environment=dict(CUDA_VISIBLE_DEVICES=runtime["cuda_visible_devices"]))
    for name in ("author_root", "method_python", "bridge_binary", "artifact_root"):
        path = Path(value[name])
        require_outside_git(path)
        if not path.is_absolute():
            raise ValueError("author runtime paths must be absolute")
    check_external_lease(value)
    components = verify_filter.configuration(value)
    return owned, value, components, expected


def _open_filter(args, value, components, query_id, report):
    """The same scope/bridge/result path as verify_filter.run_one, kept lazy."""
    method, engine, generation, resources, source = components
    deadline = time.monotonic() + resources.query_timeout_s
    args.filter_config = args.output / "worker-0.json"
    args.cuda_visible_devices = value["model_environment"]["CUDA_VISIBLE_DEVICES"]
    udf = verify_filter.OWNED / "filter_udf.py"
    scope = verify_filter.FilterScope(args, "cpython-filter", owner, report, kernel=udf)
    control = scope.root / "query-control"
    control.mkdir()
    scan = source.scan_spec(method)
    write_private_json(args.filter_config, dict(method=asdict(method), engine=asdict(engine),
        generation=asdict(generation), resources=asdict(resources), query_id=query_id,
        deadline_monotonic=deadline, control_dir=str(control), scan=scan.describe(),
        model_environment=value["model_environment"]))
    scope.__enter__()
    process = Popen([str(args.bridge_binary), str(scope.port), str(udf), "infer_records", PROTOCOL],
        env=scope.env, cwd=scope.root, stdin=PIPE, stdout=PIPE, stderr=PIPE, start_new_session=True)
    scope.track_root(process)
    writer = verify._capture_stderr(process.stderr, scope.root / "bridge-stderr.log", report)
    scope.children.append((process, writer))

    def finish_owner():
        scope.close()
        return owner.resource_state(scope)

    def abort_owner():
        state = finish_owner()
        process.wait(timeout=3)
        if state["resources_confirmed"] is not True:
            raise RuntimeError("Filter scope resources remain unconfirmed")

    client = RecordsBridge(process, abort_owner=abort_owner,
        timeout_s=min(60, resources.query_timeout_s), protocol=PROTOCOL)
    rows = (dict(identity(query_id, row["row_id"], row["source_position"]),
                 **{name: row[name] for name in scan.payload_columns})
            for row in source.rows() if scan.predicate is None or row[scan.predicate[0]] == scan.predicate[1])
    return FilterResults(rows, client, query_id=query_id, max_calls=engine.max_model_calls,
        columns=scan.payload_columns, cleanup_scope=args.stop_budget.cleanup,
        control_dir=control, deadline=deadline, close_owner=finish_owner,
        block_rows=resources.prepare_batch_rows, max_pending=resources.max_active_batches)


def _validate_calls(report, expected):
    """Keep actual formatter SHA and authoritative token IDs from the carrier."""
    declared = {row["call_id"]: row for row in expected}
    for row in report["calls"]["records"]:
        validate_terminal(row, declared[row["call_id"]], engine_uid=row.get("engine_uid"))
    close = report.get("backend_close") or {}
    if close.get("status") != "closed":
        raise RuntimeError("the author/model close receipt remains unconfirmed")


def run(source, checkout, method, generation, engine, resources, *, query_id, profile,
        observer=None, context=None):
    """Run one already-reserved query; never claim again or release the lease."""
    observer_errors = []

    def emit(event):
        if observer is not None:
            try:
                observer(event)
            except BaseException as error:
                # A failed clock must not replace an already-known call ledger
                # when the shared query consumer emits its final cleanup event.
                observer_errors.append((event, error))
    report = dict(schema="semloom.imlane_movie_formal.v1", status="failed", query_id=query_id,
        backend="imlane", budget_scope=BUDGET_SCOPE, external_host_lease_released_by_worker=False,
        scheduler_owner="author IMLane DBEnd/Coordinator/Lane/CPython",
        author_commit=owner.AUTHOR, author_scheduling_modified=False,
        model_lifetime="one private engine per query owner; no retry or reuse",
        chat_template_scope="untemplated LOTUS messages; template applied once by existing vLLM chat",
        independent_descendant_audit="outer controller plus author scope birth identities")
    author_report = dict(cleanup=[], queries=[], passed=True)
    primary = args = guard = None
    previous = {}
    started = time.monotonic()

    def failed(error, label):
        nonlocal primary
        if primary is None:
            primary = error
        elif primary is not error:
            primary.add_note(label + ": " + type(error).__name__)
        report["status"] = "failed"

    try:
        expected = source.expected(query_id)
        missing = CallLedger(expected, max_calls=max(1, len(expected)))
        missing.mark_unconfirmed("QueryNotStarted")
        report["calls"] = missing.report()
        if "observer" not in inspect.signature(run_movie_query).parameters:
            raise RuntimeError("the controller's Movie observer hook must be integrated first")
        root, value, components, expected = _binding(
            source, method, generation, engine, resources, query_id, profile, context)
        root.mkdir(mode=0o700, exist_ok=False)
        args = SimpleNamespace(output=root, scopes=[], stop_budget=owner.StopBudget(threading.Event()),
            owner_resource_limits=value["owner_resource_limits"],
            **{key: Path(value[key]) for key in ("author_root", "method_python", "bridge_binary", "artifact_root")})
        previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        for number in previous:
            signal.signal(number, args.stop_budget.signal)
        claim = context["claimed_budget"]
        with claim._lock:
            if getattr(claim, "_imlane_movie_owner_entered", False):
                raise ValueError("this query claim already entered an IMLane owner; retries are forbidden")
            claim._imlane_movie_owner_entered = True
        guard = AuthorRunGuard(value, args)

        def execute():
            if observer_errors:
                raise observer_errors[0][1]
            guard()
            emit("author_start")
            if observer_errors:
                raise observer_errors[0][1]
            return _open_filter(args, value, components, query_id, author_report)

        result = run_movie_query(source, checkout, components[0], generation, execute,
                                 query_id=query_id, observer=emit)
        report.update(result)
        _validate_calls(report, expected)
    except BaseException as error:
        report.update(getattr(error, "query_report", {}))
        failed(error, "query failure")
    finally:
        if args is not None:
            for scope in reversed(args.scopes):
                try:
                    scope.close()
                except BaseException as error:
                    failed(error, "author cleanup also failed")
            author_report["scope_resources"] = [owner.resource_state(scope) for scope in args.scopes]
            verify_filter.apply_final_verdict(author_report, stopped=args.stop_budget.stopped.is_set())
            if (author_report["passed"] is not True
                    or any(row["resources_confirmed"] is not True for row in author_report["scope_resources"])):
                failed(RuntimeError("author resource or cleanup evidence remains unconfirmed"), "author verdict")
        report["author_owner"] = author_report
        if guard is not None:
            try:
                report["physical_exit"] = guard.finish()
                if (report["physical_exit"].get("gpu_exit_confirmed") is not True
                        or report["physical_exit"].get("external_lease_valid") is not True):
                    raise RuntimeError("independent GPU exit or the external lease remains unconfirmed")
                if (author_report["scope_resources"]
                        and (report.get("backend_close") or {}).get("status") == "closed"
                        and all(row["resources_confirmed"] is True for row in author_report["scope_resources"])):
                    emit("owner_closed")
            except BaseException as error:
                failed(error, "physical exit check also failed")
        for number, handler in previous.items():
            signal.signal(number, handler)
        for event, error in observer_errors:
            report.setdefault("observer_errors", []).append(dict(event=event,
                error_type=type(error).__name__, error=redact_text(str(error))))
            failed(error, "observer " + event + " also failed")
        report["adapter_elapsed_seconds"] = time.monotonic() - started
        if primary is not None:
            report.update(status="failed", error_type=type(primary).__name__, error=redact_text(str(primary)))
            primary.query_report = report
    if primary is not None:
        raise primary
    return report
