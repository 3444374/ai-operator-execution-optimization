"""Sampling, trace building, stable baseline, and atomic persistence.

The recorder is passive: it samples, records, and serializes traces. It never
decides pass/fail and owns no thresholds. An operation that raises still
yields its trace through RecordedOperation so evidence can hit disk before
any verdict is reported.
"""
from __future__ import annotations

import gzip
import json
import os
import threading
import time
from pathlib import Path
from typing import Callable, Mapping, Protocol

from .linux_procfs import sample_tick, unix_socket_table, process_start_time_ticks
from .model import (
    CapturedError,
    FdIdentity,
    PgFileClassificationContext,
    ProcessSnapshot,
    RecordedOperation,
    ResourceTrace,
    SampleTick,
    SnapshotStatus,
    StableBaseline,
)


class Clock(Protocol):
    """Minimal monotonic clock seam for tests."""

    def monotonic_ns(self) -> int: ...


class SystemClock:
    def monotonic_ns(self) -> int:
        return time.monotonic_ns()


class TickSampler(Protocol):
    """One call observes every role under a single shared /proc view."""

    def sample_all(self, monotonic_ns: int) -> SampleTick: ...


class ProcfsTickSampler:
    """Sample the live roles; one /proc/net/unix read per tick."""

    def __init__(
        self,
        pids: Mapping[str, int],
        provider_socket_path: str | None,
        pg_context: PgFileClassificationContext | None = None,
    ):
        self._pids = dict(pids)
        self._provider_socket_path = provider_socket_path
        self._pg_context = pg_context
        self._start_times = {
            role: process_start_time_ticks(pid)
            for role, pid in self._pids.items()}

    def sample_all(self, monotonic_ns: int) -> SampleTick:
        return sample_tick(
            self._pids,
            monotonic_ns=monotonic_ns,
            provider_socket_path=self._provider_socket_path,
            pg_context=self._pg_context,
            expected_start_times=self._start_times)


def _serialize_fd(item: FdIdentity) -> dict:
    return {
        "fd": item.fd,
        "target": item.target,
        "kind": item.kind.value,
        "inode": item.inode,
        "unix_path": item.unix_path,
    }


def serialize_snapshot(role: str, snapshot: ProcessSnapshot) -> dict:
    return {
        "role": role,
        "pid": snapshot.pid,
        "process_start_time_ticks": snapshot.process_start_time_ticks,
        "monotonic_ns": snapshot.monotonic_ns,
        "status": snapshot.status.value,
        "errors": list(snapshot.errors),
        "rss_bytes": snapshot.rss_bytes,
        "thread_count": snapshot.thread_count,
        "total_fd_count": snapshot.total_fd_count,
        "fds": None if snapshot.fds is None else [
            _serialize_fd(item) for item in snapshot.fds],
    }


def serialize_tick(tick: SampleTick) -> list[str]:
    lines = []
    for role in sorted(tick.processes):
        lines.append(json.dumps({
            "kind": "tick",
            "unix_table_valid": tick.unix_table_valid,
            **serialize_snapshot(role, tick.processes[role]),
        }))
    return lines


def _write_gzip_atomic(path: Path, lines: list[str]) -> None:
    temporary = path.with_name(path.name + ".partial")
    with gzip.open(temporary, "wt", encoding="utf-8", newline="\n") as handle:
        for line in lines:
            handle.write(line)
            handle.write("\n")
    os.replace(temporary, path)


def write_atomic(path: Path, payload: str) -> None:
    temporary = path.with_name(path.name + ".partial")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def acquire_stable_baseline(
    sampler: TickSampler,
    *,
    required_consecutive: int = 5,
    interval_seconds: float = 0.05,
    timeout_seconds: float = 10.0,
    clock: Clock | None = None,
    extra_stability: Callable[[SampleTick], bool] | None = None,
) -> tuple[StableBaseline | None, list[SampleTick]]:
    """Accept a baseline only after the FD identity set stopped changing.

    Stability requires every observed role to stay alive with an unchanged
    process start time, be VALID, keep an identical FD identity set (fd
    number AND target), and satisfy ``extra_stability`` (e.g. zero active
    provider sessions). Count equality is never sufficient: fd replacement
    keeps the count but changes the identity set.
    """
    clock = clock or SystemClock()
    deadline = time.monotonic() + timeout_seconds
    observed: list[SampleTick] = []
    stable_run: list[SampleTick] = []
    while time.monotonic() < deadline:
        tick = sampler.sample_all(clock.monotonic_ns())
        observed.append(tick)

        def identity_set(snapshot: ProcessSnapshot) -> tuple | None:
            if snapshot.fds is None:
                return None
            return tuple(sorted((item.fd, item.target) for item in snapshot.fds))

        def stable_against(previous: SampleTick, current: SampleTick) -> bool:
            for role in current.processes:
                before = previous.processes.get(role)
                after = current.processes.get(role)
                if before is None or after is None:
                    return False
                if after.status is not SnapshotStatus.VALID:
                    return False
                if before.process_start_time_ticks != after.process_start_time_ticks:
                    return False
                if identity_set(before) != identity_set(after):
                    return False
            if extra_stability is not None and not extra_stability(tick):
                return False
            return True

        if stable_run and stable_against(stable_run[-1], tick):
            stable_run.append(tick)
        else:
            stable_run = [tick]
        if len(stable_run) >= required_consecutive:
            baseline = {
                role: stable_run[-1].processes[role]
                for role in sorted(stable_run[-1].processes)}
            return StableBaseline(ticks=tuple(stable_run), baseline=baseline), observed
        time.sleep(interval_seconds)
    return None, observed


def record_operation(
    sampler: TickSampler,
    operation: Callable[[], object],
    *,
    sample_seconds: float,
    baseline: Mapping[str, ProcessSnapshot] | None = None,
    clock: Clock | None = None,
) -> RecordedOperation:
    """Run ``operation`` while sampling; return outcome AND trace together.

    The operation error is captured, never re-raised, so the caller can
    persist the trace before reporting the failure. Sampling errors are
    captured the same way. A final sample is taken after the operation so
    the trace's last tick reflects the post-operation state.
    """
    clock = clock or SystemClock()
    ticks: list[SampleTick] = []
    stopping = threading.Event()
    sampling_error: CapturedError | None = None

    def first_snapshot() -> dict[str, ProcessSnapshot]:
        tick = sampler.sample_all(clock.monotonic_ns())
        ticks.append(tick)
        return dict(tick.processes)

    starting = first_snapshot()
    if baseline is None:
        baseline = starting

    def collect() -> None:
        while not stopping.is_set():
            try:
                ticks.append(sampler.sample_all(clock.monotonic_ns()))
            except OSError as error:
                nonlocal sampling_error
                sampling_error = CapturedError(
                    exception_type=type(error).__name__, sqlstate=None,
                    message=str(error)[:200], phase="sampling")
                return
            stopping.wait(sample_seconds)

    operation_error: CapturedError | None = None
    result = None
    worker = threading.Thread(target=collect)
    worker.start()
    try:
        result = operation()
    except Exception as error:  # noqa: BLE001 - captured, sanitized below
        sqlstate = getattr(error, "sqlstate", None)
        operation_error = CapturedError(
            exception_type=type(error).__name__,
            sqlstate=str(sqlstate) if sqlstate else None,
            message=str(error)[:200], phase="operation")
    finally:
        stopping.set()
        worker.join()
    ticks.append(sampler.sample_all(clock.monotonic_ns()))
    trace = ResourceTrace(baseline=baseline, ticks=tuple(ticks))
    return RecordedOperation(
        result=result, operation_error=operation_error,
        sampling_error=sampling_error, trace=trace)


def fd_lifecycles(trace: ResourceTrace) -> list[dict]:
    """Per-(role, fd, target) lifecycle records for diagnostics only.

    Never used for peak math: sequential fd reuse must not be summed.
    """
    records: dict[tuple[str, int, str], dict] = {}
    ordered: list[tuple[int, Mapping[str, ProcessSnapshot]]] = [
        (-1, dict(trace.baseline))] if trace.baseline else []
    ordered.extend((index, dict(tick.processes)) for index, tick in enumerate(trace.ticks))
    for position, processes in ordered:
        for role in sorted(processes):
            snapshot = processes[role]
            if snapshot.fds is None:
                continue
            for item in snapshot.fds:
                key = (role, item.fd, item.target)
                record = records.setdefault(key, {
                    "role": role, "fd": item.fd, "target": item.target,
                    "kind": item.kind.value, "inode": item.inode,
                    "first_seen_tick": position,
                    "last_seen_tick": position})
                record["last_seen_tick"] = position
    return [records[key] for key in sorted(records, key=lambda k: (k[0], k[1]))]


def persist_trace(directory: Path, trace: ResourceTrace) -> None:
    """Write baseline plus ticks as gzip JSONL, streamed to a partial file."""
    directory.mkdir(parents=True, exist_ok=True)
    lines = []
    for role in sorted(trace.baseline):
        lines.append(json.dumps({
            "kind": "baseline",
            **serialize_snapshot(role, trace.baseline[role])}))
    for tick in trace.ticks:
        lines.extend(serialize_tick(tick))
    _write_gzip_atomic(directory / "process_samples.jsonl.gz", lines)


def persist_lifecycles(directory: Path, trace: ResourceTrace) -> None:
    """Write the fd lifecycle diagnostic stream."""
    directory.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(item) for item in fd_lifecycles(trace)]
    _write_gzip_atomic(directory / "fd_lifecycles.jsonl.gz", lines)


def persist_operation_outcome(
    directory: Path, recorded: RecordedOperation, phase: str,
) -> None:
    """Persist the sanitized operation outcome beside its trace."""
    directory.mkdir(parents=True, exist_ok=True)
    payload = {"phase": phase, "result_summary": None}
    if recorded.operation_error is not None:
        payload["operation_error"] = {
            "exception_type": recorded.operation_error.exception_type,
            "sqlstate": recorded.operation_error.sqlstate,
            "message": recorded.operation_error.message,
            "phase": recorded.operation_error.phase}
    if recorded.sampling_error is not None:
        payload["sampling_error"] = {
            "exception_type": recorded.sampling_error.exception_type,
            "message": recorded.sampling_error.message}
    if isinstance(recorded.result, dict):
        payload["result_summary"] = {
            key: recorded.result[key] for key in recorded.result
            if isinstance(recorded.result[key], (int, str, bool, float, type(None)))}
    elif isinstance(recorded.result, (int, str, bool, float, type(None))):
        payload["result_summary"] = recorded.result
    write_atomic(directory / "operation_outcome.json",
                 json.dumps(payload, indent=2) + "\n")
