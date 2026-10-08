"""Record a query submitted after its source and reusable runtime are ready."""
from contextlib import closing, contextmanager, ExitStack
import json
import hashlib
from pathlib import Path
import time

from src.baselines.common.private_artifacts import write_private_json
from .map_query_recording import record_execution
from .cell_evidence import CellErrors


def proxy_http_peak(traces):
    """Count actual upstream intervals on the common proxy's monotonic clock."""
    points = []
    for row in traces:
        start = row['upstream_dispatch_started_monotonic_ns']
        end = row['upstream_response_body_read_completed_monotonic_ns']
        if start is None or end is None or end < start:
            raise ValueError('completed proxy request lacks an ordered upstream interval')
        points.extend(((start, 1), (end, -1)))
    active = peak = 0
    for _, delta in sorted(points):
        active += delta
        if active < 0:
            raise ValueError('proxy HTTP responsibilities are not ordered')
        peak = max(active, peak)
    if active:
        raise ValueError('proxy HTTP responsibilities did not settle')
    return peak


def record_prepared_execution(directory, open_rows, *, backend_ready_ns,
                              preparation_started_ns=None, clock=time.monotonic_ns,
                              **options):
    """Keep the actual API entry distinct from recorder setup and result EOF.

    The caller completes preparation before calling this function. The supplied
    context manager must enter the native query API without loading source data
    or initializing a reusable runtime. Query-owned work remains in that API.
    """
    if type(backend_ready_ns) is not int or backend_ready_ns <= 0:
        raise ValueError('a positive prepared-runtime timestamp is required')
    if preparation_started_ns is not None and (
            type(preparation_started_ns) is not int or
            not 0 < preparation_started_ns <= backend_ready_ns):
        raise ValueError('preparation start must precede prepared-runtime readiness')
    directory = Path(directory)
    submitted = None
    execution = None

    @contextmanager
    def submit():
        nonlocal submitted
        submitted = clock()
        with open_rows() as rows:
            yield rows

    errors = CellErrors()
    execution = errors.attempt('query', lambda:record_execution(directory, submit, clock=clock, **options))
    saved = directory / 'execution.json'
    if execution is None and saved.is_file():
        execution = errors.attempt('timing_read', lambda:json.loads(saved.read_text()))
    digest = errors.attempt('timing_source_hash', lambda:hashlib.sha256(saved.read_bytes()).hexdigest()) if saved.is_file() else None
    timing = dict(ready_timing_schema='semloom.ready_query.v1',
                      t_backend_ready_ns=backend_ready_ns, t_submit_ns=submitted,
                      ready_query_seconds=None,
                      ready_query_timing_scope='native SQL/API entry after preparation through unified consumer EOF/error; includes result recording',
                      preparation_seconds=((backend_ready_ns-preparation_started_ns)/1e9
                                           if preparation_started_ns is not None else None),
                      query_execution_sha256=digest)
    if execution is not None:
        def interval():
            terminal = execution['t_query_terminal_ns']
            if submitted is None or terminal is None:
                return None
            if not backend_ready_ns <= submitted <= terminal:
                raise ValueError('prepared query timestamps are not ordered')
            return (terminal-submitted)/1e9
        timing['ready_query_seconds'] = errors.attempt('timing_order', interval)
    timing['ready_timing_errors'] = errors.details
    if execution is not None:
        execution.update(timing)
    errors.attempt('timing_write', lambda:write_private_json(directory/'ready-timing.json', timing))
    errors.raise_if_failed()
    return execution


def record_prepared_pg_query(connection, query, directory, *,
                             preparation_started_ns=None, clock=time.monotonic_ns,
                             query_timeout_s=None, **options):
    """The connection and cursor exist before the timed cursor.stream API entry."""
    preparation = ExitStack()
    errors = CellErrors()
    result = None
    try:
        cursor = preparation.enter_context(connection.cursor())
        ready = clock()

        @contextmanager
        def open_rows():
            # The recorder owns cursor exit after submission, including failures.
            with preparation.pop_all():
                with closing(cursor.stream(query)) as rows:
                    yield rows

        result = record_prepared_execution(directory, open_rows,
            backend_ready_ns=ready, preparation_started_ns=preparation_started_ns,
            clock=clock, query_timeout_s=query_timeout_s,
            cancel_query=(lambda: connection.cancel_safe(timeout=1)) if query_timeout_s else None,
            **options)
    except BaseException as failure:
        errors.record('prepared_query', failure)
    finally:
        errors.attempt('cursor_cleanup_before_submission', preparation.close)
    errors.raise_if_failed()
    return result
