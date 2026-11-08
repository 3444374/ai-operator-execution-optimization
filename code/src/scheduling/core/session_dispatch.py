"""Submit selected session records and retain uncertain backend responsibility.

The existing SessionEngine and flow keep all task, lease and cursor state.
Single requests and atomic groups share this handoff implementation.
"""
from __future__ import annotations

from dataclasses import replace

from .session_capacity import TaskRecord
from .session_contract import (
    Acceptance, BackendTask, BatchKey, BatchMember, BatchSubmission,
    State, Submission, UINT64_MAX,
)
from .session_policy import _batch_dispatch_snapshot


def _dispatch(session, record: TaskRecord, now: float) -> bool:
    engine = session.engine
    if session._pending_members and getattr(engine.backend, "supports_batch_submit", False):
        return _dispatch_batch(session, now)
    if not engine.capacity.can_dispatch(record, session.limits):
        session._capacity_waiting = record.key
        if engine.observe_capacity_blocks:
            engine._emit("dispatch_capacity_blocked", record.key)
        return False
    session._capacity_waiting = None
    endpoint = engine.policies.select(
        record, tuple(r for r in engine.capacity.records.values() if r.compute), now
    )
    if endpoint is None:
        return False
    record.endpoint = endpoint
    if engine.credit:
        record.credit = engine.credit.try_acquire(
            request_id=engine._request_id(record),
            job_id=session.spec.job_id,
            endpoint_id=endpoint,
            estimated_work=record.task.estimated_work,
        )
        if not record.credit:
            # A denied waiter is transient; no hidden unbounded ready queue remains.
            engine.credit.cancel_waiter(engine._request_id(record), job_id=session.spec.job_id)
            return False
    if session._cancel.is_set():
        if record.credit:
            record.credit = False
            engine.credit.release(engine._request_id(record), job_id=session.spec.job_id)
        session.state = State.CANCELLED
        return False
    queued_since = record.since
    record.compute = True
    record.phase = "INFLIGHT"
    record.since = now
    try:
        outcome = engine.backend.try_submit(
            BackendTask(record.key, record.spec, record.task, record.member), endpoint
        )
    except Exception:
        outcome = Submission(Acceptance.UNKNOWN)
    if (
        type(outcome) is not Submission
        or type(outcome.acceptance) is not Acceptance
        or (outcome.acceptance == Acceptance.NOT_ACCEPTED and outcome.handle is not None)
        or (
            outcome.handle is not None
            and (
                type(outcome.handle) is not str
                or not outcome.handle
                or len(outcome.handle.encode()) > session.limits.metadata_bytes
            )
        )
    ):
        outcome = Submission(Acceptance.UNKNOWN)
    if outcome.acceptance == Acceptance.NOT_ACCEPTED:
        record.compute = False
        record.phase = "QUEUED"
        record.since = queued_since
        if record.credit:
            record.credit = False
            engine.credit.release(engine._request_id(record), job_id=session.spec.job_id)
        return False
    record.handle = outcome.handle
    if outcome.acceptance == Acceptance.UNKNOWN or outcome.handle is None:
        record.phase = "UNCERTAIN"
        session._fail("backend acceptance unknown")
    engine._emit("submitted", record.key)
    session._pending_members = session._pending_members[1:]
    return True


def _dispatch_batch(session, now):
    """Optional atomic transport handoff over existing per-member reservations."""
    engine = session.engine
    if engine.credit is not None:
        raise ValueError("atomic transport batches do not support shared-credit routing")
    records = tuple(engine.capacity.records[key] for key in session._pending_members)
    reserved = []
    snapshot = _batch_dispatch_snapshot(
        engine.capacity, engine.policies, session_id=session.session_id, limits=session.limits,
        job_id=session.spec.job_id, member_count=len(session._pending_members), now=now)
    if snapshot is not None:
        bounds, occupied_endpoint, oldest = snapshot
    try:
        for record in records:
            fits = (engine.capacity.can_dispatch(record, session.limits) if snapshot is None else
                    all(count < limit.active_requests
                        and work + record.task.estimated_work <= limit.active_work
                        for count, work, limit in bounds))
            if not fits:
                session._capacity_waiting = record.key
                return False
            if snapshot is None:
                endpoint = engine.policies.select(record,
                    tuple(r for r in engine.capacity.records.values() if r.compute), now)
            else:
                if not engine.policies.admission.decide(
                        bounds[0][0], hol_age_s=oldest).allowed:
                    return False
                endpoint = engine.policies._route(record, (occupied_endpoint,))
            if endpoint is None:
                return False
            record.compute, record.endpoint = True, endpoint
            reserved.append((record, record.since))
            if snapshot is not None:
                for bound in bounds:
                    bound[0] += 1
                    bound[1] += record.task.estimated_work
                oldest = max(oldest, now - record.since)
                occupied_endpoint = replace(occupied_endpoint,
                    running=occupied_endpoint.running + 1,
                    estimated_active_work=occupied_endpoint.estimated_active_work
                        + record.task.estimated_work)
        if len({r.endpoint for r in records}) != 1:
            raise ValueError("a transport batch requires one endpoint")
        if session._cancel.is_set():
            return False
        for record in records:
            record.phase, record.since = "INFLIGHT", now
        try:
            outcome = engine.backend.try_submit_batch(tuple(
                BackendTask(r.key, r.spec, r.task, r.member) for r in records), records[0].endpoint)
        except Exception:
            outcome = BatchSubmission(Acceptance.UNKNOWN)
        if (type(outcome) is not BatchSubmission or type(outcome.acceptance) is not Acceptance
                or type(outcome.handles) is not tuple
                or outcome.acceptance == Acceptance.ACCEPTED and
                (len(outcome.handles) != len(records) or any(type(h) is not str or not h
                    or len(h.encode()) > session.limits.metadata_bytes for h in outcome.handles))
                or outcome.acceptance != Acceptance.ACCEPTED and outcome.handles):
            outcome = BatchSubmission(Acceptance.UNKNOWN)
        if outcome.acceptance == Acceptance.NOT_ACCEPTED:
            return False
        reserved.clear()  # All member compute responsibilities transferred.
        session._capacity_waiting = None
        session._pending_members = ()
        for i, record in enumerate(records):
            record.handle = outcome.handles[i] if outcome.handles else None
            if outcome.acceptance == Acceptance.UNKNOWN:
                record.phase = "UNCERTAIN"
            engine._emit("submitted", record.key)
        if outcome.acceptance == Acceptance.UNKNOWN:
            session._fail("backend batch acceptance unknown")
        return True
    finally:
        for record, since in reserved:
            record.compute, record.endpoint, record.phase = False, None, "QUEUED"
            record.since = since


def _next_record(session, queued: tuple[TaskRecord, ...]) -> TaskRecord | None:
    if session.engine.policies.organize is None:
        return session.engine.policies.select_task(queued)
    if not session._pending_members:
        group = session.engine.policies.select_batch(queued)
        if not group:
            return None
        if session._next_batch > UINT64_MAX:
            raise OverflowError("batch identity exhausted")
        batch = BatchKey(session.session_id, session._next_batch)
        members = tuple(BatchMember(batch, i, len(group)) for i in range(len(group)))
        keys = tuple(r.key for r in group)
        # Save only keys: payloads and reservations remain in the single task table.
        for record, member in zip(group, members):
            record.member = member
        session._pending_members = keys
        session._next_batch += 1
    return session.engine.capacity.records[session._pending_members[0]]
