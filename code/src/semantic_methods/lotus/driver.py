"""Finite external LOTUS input and result consumption around the existing MethodDriver."""

import math
import time

from ...scheduling.core.session_contract import SessionSpec, TaskProfile
from ..budget import MethodBudgetPool
from ..driver import MethodDriver, RowIdentity


def iter_two_map_rows(execution, method, rows, *, query_id, operator_id, limits,
                      capacity, max_rows=4096, cancelled=None, timeout_s=120):
    """Yield original row identities in available order; caller owns execution cleanup.

    rows contains already authorized external input bytes. The method input and state
    limits cover both stages. Closing the iterator closes its consumer and fixed grant;
    the execution owner still drains remote work and closes its backend.
    """
    if type(max_rows) is not int or max_rows < 1:
        raise ValueError("LOTUS input row limit must be positive")
    if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 120:
        raise ValueError("LOTUS chain timeout must be at most 120 seconds")
    pool = MethodBudgetPool(capacity)
    job, session, _budget = execution.open_job(query_id,
        SessionSpec(query_id, operator_id, "text-completion", work_unit=execution.work_unit,
                    task_profiles=(TaskProfile("lotus-map", "text-completion"),)))
    driver = None
    primary = None
    cleanup_errors = []
    try:
        grant = pool.allocate(capacity)
        try:
            driver = MethodDriver(session, method, limits, grant)
        except BaseException:
            try:
                grant.close()
            except BaseException as error:
                cleanup_errors.append(("grant", error))
            raise
        source = iter(rows)
        empty = object()
        pending = empty
        ended = False
        sequence = 0
        started = time.monotonic()
        while not driver.finished:
            if cancelled and cancelled():
                session.request_cancel()
                raise RuntimeError("LOTUS chain cancelled")
            if time.monotonic() - started >= timeout_s:
                session.request_cancel()
                raise TimeoutError("LOTUS chain deadline exceeded")
            if not ended and pending is empty:
                try:
                    pending = next(source)
                except StopIteration:
                    ended = True
                    driver.end_input()
                else:
                    if sequence == max_rows:
                        raise ValueError("LOTUS external input exceeds declared row limit")
            if pending is not empty and driver.offer_row(RowIdentity(sequence, operator_id), pending):
                pending = empty
                sequence += 1
            cleanup = execution.engine.advance()
            progress = driver.advance(session.limits.held_tasks)
            for result in driver.results(capacity.runs):
                try:
                    yield result
                finally:
                    driver.release_result(result.row)
            if not (progress.has_immediate_work or cleanup.has_immediate_work):
                timeout = session.limits.poll_interval_s
                deadlines = [d for d in (cleanup.next_deadline, progress.next_deadline) if d is not None]
                if deadlines:
                    timeout = min(timeout, max(0, min(deadlines) - execution.engine.clock()))
                execution.engine.wake.wait(progress.generation, timeout)
    except BaseException as error:
        primary = error
        raise
    finally:
        consumer = ("driver", driver.close) if driver is not None else ("session", session.close_consumer)
        for phase, close in (consumer, ("job", lambda: execution.engine.close_job(job))):
            try:
                close()
            except BaseException as error:
                cleanup_errors.append((phase, error))
        if cleanup_errors:
            failure = primary if primary is not None else cleanup_errors[0][1]
            failure.lotus_cleanup_errors = tuple(cleanup_errors)
            for phase, error in cleanup_errors:
                if error is not failure:
                    failure.add_note("LOTUS chain " + phase + " cleanup also failed: " + type(error).__name__)
            if primary is None:
                raise failure
