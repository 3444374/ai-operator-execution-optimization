"""Native backend query/owner lifetimes and return clocks, independent of Movie inputs.

Adapters own scheduling and model lifetimes. This module observes the supplied
query driver; it does not create model work, leases, retries or budgets.
"""
from pathlib import Path
import time
import traceback
import uuid
from src.baselines.common.private_artifacts import new_private_directory, publish_private_json, write_private_json
from src.baselines.common.redact import redact_json_values

RESIDENT = frozenset({"lotus-native", "semloom-core", "imlane"})
BACKENDS = RESIDENT | {"ray-data", "daft", "imlane"}
LIFECYCLES = {"resident", "isolated", "runtime_reuse_only", "native_single_thread_session"}
DAFT_SESSION_PROFILE = dict(native_threads=1, use_process=False, max_concurrency=None)
# This lists implemented adapter paths, not every capability of each vendor.


def _failure(error):
    return redact_json_values(dict(error_type=type(error).__name__, error=str(error),
        traceback="".join(traceback.format_exception(type(error), error, error.__traceback__)),
        notes=list(getattr(error, "__notes__", ()))))


def _lifecycle_timing(report):
    """Compare recorded phases without treating driver start as model loading."""
    def total(values):
        return None if any(v is None for v in values) else sum(values)
    owners=[]
    for owner in report["owners"]:
        first=next((q for q in report["queries"] if q["query_id"]==owner["first_query_id"]),None)
        ready=owner.get("ready")
        ready=ready if isinstance(ready,dict) else {}
        runtime_only=ready.get("no_session_model") is True
        begin=None if first is None else min(owner["owner_start_ns"],first["query_start_ns"])
        def cold(name):
            end=None if first is None else first.get(name)
            return None if begin is None or end is None else (end-begin)/1e9
        overlap=None
        if first is not None and first.get("query_api_return_ns") is not None and owner.get("owner_ready_ns") is not None:
            overlap=max(0,min(owner["owner_ready_ns"],first["query_api_return_ns"])
                        -max(owner["owner_start_ns"],first["query_start_ns"]))/1e9
        owners.append(dict(owner_id=owner["owner_id"],first_query_id=owner["first_query_id"],
            owner_startup_scope=(None if not ready else "driver_runtime_only" if runtime_only else "backend_start"),
            model_initialization_scope=ready.get("model_startup_scope",None if not ready else "backend_start"),
            startup_seconds=owner.get("startup_seconds"),shutdown_seconds=owner.get("shutdown_seconds"),
            startup_already_in_query_api_seconds=overlap,
            cold_first_result_seconds=cold("query_result_ready_ns"),
            cold_first_api_return_seconds=cold("query_api_return_ns")))
    native_starts=[]
    for query in report["queries"]:
        backend=query.get("backend_report")
        close=backend.get("backend_close") if isinstance(backend,dict) else None
        records=close.get("engine_reports") if isinstance(close,dict) else None
        measured=[]
        for record in records if isinstance(records,list) else ():
            if not isinstance(record,dict):continue
            ready=record.get("engine_ready")
            uid=ready.get("engine_uid") if isinstance(ready,dict) else None
            seconds=record.get("startup_seconds")
            measured.append(dict(engine_uid=uid,startup_seconds=(seconds
                if uid and type(seconds) in (int,float) and seconds>=0 else None)))
        native_starts.append(dict(query_id=query["query_id"],reports=measured,
            source="backend_close.engine_reports; private process startup includes readiness IPC",
            missing_reason=None if measured else "backend did not expose a native constructor span"))
    return dict(owners=owners,native_query_model_startup=native_starts,
        owner_startup_seconds_sum=total([o["startup_seconds"] for o in owners]),
        owner_shutdown_seconds_sum=total([o["shutdown_seconds"] for o in owners]),
        owner_startup_already_in_query_api_seconds=total([o["startup_already_in_query_api_seconds"] for o in owners]),
        query_api_return_seconds_sum=total([q["query_api_return_seconds"] for q in report["queries"]]),
        query_api_timing_count=sum(q["query_api_return_ns"] is not None for q in report["queries"]),
        query_api_scope="query_start to immediate driver.query normal return; checkpoint and owner close separate",
        cold_first_scope="earlier owner/query start to first result or driver return; preflight before owner start separate",
        overlap_rule="owner startup may already be in query API time; components are not blindly additive",
        model_load_count_observed=report["model_load_count_observed"])


def run_sequence(queries, factory, output, *, backend, lifecycle="resident", observer=None, clock=None,
                 setup=None, checkpoint=None):
    """Run one ready owner or consecutive isolated owners, stopping on any failure.

    ``factory(directory, first_query)`` allocates no resources in its constructor. Its
    driver supplies start(), query(spec, emit), and close(). query must emit
    result_ready once after the complete result, before quality evaluation.
    Only the driver closes resources it created. Caller-owned leases and
    supervisors are never closed here. All durable evidence is redacted.
    An optional synchronous lifecycle checkpoint runs after a complete query is
    durably published, before advancing or closing its owner. Its failure stops
    the session; diagnostic observer failures remain noncritical.
    """
    if backend not in BACKENDS or lifecycle not in LIFECYCLES:
        raise ValueError("unsupported backend or lifecycle")
    if lifecycle == "native_single_thread_session" and backend != "daft":
        raise ValueError("native_single_thread_session requires daft")
    if lifecycle == "runtime_reuse_only" and backend != "ray-data":
        raise ValueError("runtime_reuse_only is implemented only for ray-data")
    if lifecycle == "resident" and backend not in RESIDENT:
        raise ValueError(backend + " has no cross-query model reuse implemented by this adapter; select isolated")
    queries = tuple(dict(query) for query in queries)
    ids = [query["query_id"] for query in queries]
    if (not queries or len(ids) != len(set(ids)) or any(type(q) is not str or not q for q in ids)
            or any(query.get("phase") not in {"warmup", "measured"} for query in queries)):
        raise ValueError("declare unique nonempty query IDs and explicit warmup/measured phases")
    now = time.monotonic_ns if clock is None else clock
    output = Path(output)
    new_private_directory(output)
    session_id = uuid.uuid4().hex
    report = dict(schema="semloom.movie_records_session.v1", session_id=session_id,
        backend=backend, lifecycle=lifecycle, status="failed", performance_claim=False,
        cross_query_model_reuse_requested=lifecycle in {"resident", "native_single_thread_session"},
        model_load_count_observed=None, model_load_count_reason="requires model/runtime observation",
        automatic_retries=0, session_start_ns=now(), events=[], observer_errors=[], owners=[], queries=[],
        setup=setup, application_scope="sequence after preflight through final lifecycle event; final session.json publication and caller return separate",
        skipped_queries=[], clock="time.monotonic_ns, worker process", warmup_implicit=False,
        query_scope="fresh source creation/read/preflight, native prepare/operator, complete projection/aggregation",
        post_result_scope="quality evaluation and query iterator/client cleanup; final owner shutdown separate")
    write_private_json(output / "session-start.json", redact_json_values(dict(
        schema=report["schema"], session_id=session_id, backend=backend, lifecycle=lifecycle,
        queries=queries, session_start_ns=report["session_start_ns"])))
    primary = driver = owner = None

    def emit(name, *, query=None):
        owner_id = (query.get("owner_id") if query is not None else
                    None if owner is None else owner["owner_id"])
        event = dict(event=name, monotonic_ns=now(), session_id=session_id,
                     owner_id=owner_id,
                     query_id=None if query is None else query["query_id"])
        report["events"].append(event)
        if query is not None:
            query["events"].append(event)
            if name == "result_ready":
                if query.get("query_result_ready_ns") is not None:
                    raise ValueError("backend emitted duplicate complete-result events")
                query["query_result_ready_ns"] = event["monotonic_ns"]
        if observer is not None:
            try:
                observer(dict(event))
            except Exception as error:
                report["observer_errors"].append(_failure(error))
        return event["monotonic_ns"]

    def open_owner(spec):
        nonlocal driver, owner
        owner = dict(owner_id=uuid.uuid4().hex, status="starting", start_invocations=1,
                     first_query_id=spec["query_id"])
        report["owners"].append(owner)
        # Constructor precedes start but remains inside the reported startup.
        owner["owner_start_ns"] = emit("session_owner_start")
        directory = output / ("owner-" + str(len(report["owners"]) - 1))
        try:
            driver = factory(directory, spec)
            owner["ready"] = driver.start()
        except BaseException as error:
            owner.update(status="failed", failure=_failure(error))
            raise
        owner["owner_ready_ns"] = emit("session_owner_ready")
        owner.update(status="ready", startup_seconds=(owner["owner_ready_ns"] - owner["owner_start_ns"]) / 1e9)
        # Publish the original readiness receipt before any query, for the
        # independent parent monitor. This is evidence, not an ownership bypass.
        publish_private_json(output / ("owner-ready-" + str(len(report["owners"]) - 1) + ".json"),
                           redact_json_values(dict(session_id=session_id, **owner)))

    def close_owner():
        nonlocal driver
        if driver is None:
            return
        owner["owner_close_start_ns"] = emit("session_owner_close_start")
        try:
            owner["close"] = driver.close()
            if (not isinstance(owner["close"], dict) or owner["close"].get("status") != "closed"
                    or owner["close"].get("cleanup_errors")):
                raise RuntimeError("session owner cleanup remains unconfirmed")
            owner["status"] = "closed"
        except BaseException as error:
            owner.update(status="unknown", failure=_failure(error))
            raise
        finally:
            terminal = emit("session_owner_closed" if owner["status"] == "closed"
                            else "session_owner_close_unconfirmed")
            owner["owner_close_terminal_ns"] = terminal
            if owner["status"] == "closed":
                owner["owner_closed_ns"] = terminal
            owner["shutdown_seconds"] = (terminal - owner["owner_close_start_ns"]) / 1e9
            driver = None

    try:
        emit("session_start")
        if lifecycle != "isolated":
            open_owner(queries[0])
            emit("session_ready")
        for index, spec in enumerate(queries):
            query = dict(query_id=spec["query_id"], task=spec.get("task"), phase=spec["phase"],
                         status="failed", events=[], query_start_ns=now(), query_result_ready_ns=None,
                         query_api_return_ns=None,
                         owner_id=owner["owner_id"] if lifecycle != "isolated" else None)
            report["queries"].append(query)
            query_failure = None
            try:
                emit("query_start", query=query)
                if lifecycle == "isolated":
                    open_owner(spec)
                query["owner_id"] = owner["owner_id"]
                query["backend_report"] = driver.query(spec, lambda event: emit(event, query=query))
                query["query_api_return_ns"] = now()
                if query["backend_report"].get("status") != "completed":
                    raise RuntimeError("backend did not complete the query")
                if query["query_result_ready_ns"] is None:
                    raise ValueError("backend omitted the complete-result boundary")
                query["status"] = "completed"
            except BaseException as error:
                query_failure = error
                query["failure"] = _failure(error)
                query["backend_report"] = getattr(error, "query_report", query.get("backend_report"))
                raise
            finally:
                query["query_terminal_ns"] = emit("query_end", query=query)
                terminal = query["query_result_ready_ns"] or query["query_terminal_ns"]
                query["full_query_seconds"] = (terminal - query["query_start_ns"]) / 1e9
                query["complete_result_observed"] = query["query_result_ready_ns"] is not None
                query["post_result_seconds"] = (query["query_terminal_ns"] - terminal) / 1e9
                query["query_api_return_seconds"] = (None if query["query_api_return_ns"] is None else
                    (query["query_api_return_ns"]-query["query_start_ns"])/1e9)
                # Preserve primary failure evidence before final owner cleanup.
                try:
                    publish_private_json(output / ("query-" + str(index) + ".json"), redact_json_values(query))
                except BaseException as recording:
                    if query_failure is None:
                        raise
                    query_failure.add_note("query recording also failed: " + type(recording).__name__)
            if checkpoint is not None:
                emit("query_checkpoint_start", query=query)
                try:
                    query["owner_checkpoint"] = checkpoint(dict(session_id=session_id,
                        query_id=query["query_id"], owner_id=query["owner_id"], query_index=index))
                except BaseException as error:
                    query["checkpoint_failure"] = _failure(error)
                    emit("query_checkpoint_failed", query=query)
                    raise
                emit("query_checkpoint_end", query=query)
            if lifecycle == "isolated":
                close_owner()
        report["status"] = "completed"
    except BaseException as error:
        primary = error
        report["failure"] = _failure(error)
    finally:
        try:
            close_owner()
        except BaseException as cleanup:
            report["cleanup_failure"] = _failure(cleanup)
            if primary is None:
                primary = cleanup
                report["failure"] = _failure(cleanup)
            else:
                primary.add_note("session cleanup also failed: " + type(cleanup).__name__)
        report["skipped_queries"] = ids[len(report["queries"]):]
        report["owner_start_count"] = len(report["owners"])
        if primary is not None:
            report["status"] = "failed"
        report["session_end_ns"] = emit("session_closed" if primary is None else "session_failed")
        report["application_e2e_s"] = (report["session_end_ns"] - report["session_start_ns"]) / 1e9
        report["lifecycle_timing"] = _lifecycle_timing(report)
        if backend == "imlane" and lifecycle == "resident" and report["owners"]:
            first_owner = report["owners"][0]
            first_query = report["queries"][0] if report["queries"] else {}
            result_ns = first_query.get("query_result_ready_ns")
            report["resident_timing"] = dict(
                startup_seconds=first_owner.get("startup_seconds"),
                cold_first_result_seconds=(None if result_ns is None else
                    (result_ns - first_owner["owner_start_ns"]) / 1e9),
                first_ready_model_query_seconds=first_query.get("full_query_seconds"),
                steady_queries=[dict(query_id=query["query_id"], phase=query["phase"],
                    full_query_seconds=query.get("full_query_seconds")) for query in report["queries"][1:]],
                close_seconds=first_owner.get("shutdown_seconds"),
                cold_scope="owner start through first complete result; includes startup and readiness publication",
                steady_scope="complete later queries; owner startup and final close excluded")
        if setup is not None and setup.get("worker_entry_ns") is not None:
            report["worker_e2e_s"] = (report["session_end_ns"] - setup["worker_entry_ns"]) / 1e9
        try:
            write_private_json(output / "session.json", redact_json_values(report))
        except BaseException as recording:
            if primary is None:
                primary = recording
                report["status"] = "failed"
            else:
                primary.add_note("session recording also failed: " + type(recording).__name__)
        if primary is not None:
            primary.session_report = report
    if primary is not None:
        raise primary
    return report
