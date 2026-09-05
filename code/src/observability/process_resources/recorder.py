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
from dataclasses import asdict, replace
from statistics import median
from pathlib import Path
from typing import Callable, Mapping, Protocol

from .linux_procfs import sample_tick, process_start_time_ticks
from src.baselines.common.redact import redact_text
from .model import (
    CapturedError,
    FdIdentity,
    FdKind,
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
        "observed_start_ns": snapshot.observed_start_ns,
        "observed_end_ns": snapshot.observed_end_ns,
        "fd_attempts": [{"started_ns": a.started_ns, "ended_ns": a.ended_ns,
                         "errors": list(a.errors),
                         "fds": None if a.fds is None else [_serialize_fd(f) for f in a.fds]}
                        for a in snapshot.fd_attempts],
        "fds": None if snapshot.fds is None else [
            _serialize_fd(item) for item in snapshot.fds],
    }


def serialize_tick(tick: SampleTick) -> list[str]:
    lines = []
    for role in sorted(tick.processes):
        lines.append(json.dumps({
            "kind": "tick",
            "unix_table_valid": tick.unix_table_valid,
            "tick_start_ns": tick.monotonic_ns, "tick_end_ns": tick.ended_ns,
            "tick_errors": list(tick.errors),
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


def capture_error(error: BaseException, phase: str) -> CapturedError:
    """Persist reason codes, never exception bodies or connection parameters."""
    state = getattr(error, "sqlstate", None)
    return CapturedError(type(error).__name__, str(state) if state else None,
                         f"{phase}_error", phase)


def acquire_stable_baseline(
    sampler: TickSampler, *, required_consecutive=5, interval_seconds=.05,
    timeout_seconds=10., clock=None, extra_stability=None,
    required_roles=("backend", "gateway"), on_interrupt=None,
):
    """Require readable, quiet, identity/thread-stable roles; use median RSS."""
    if not required_roles or required_consecutive < 2:
        raise ValueError("invalid_baseline_config")
    clock = clock or SystemClock()
    deadline = time.monotonic() + timeout_seconds
    observed, stable_run = [], []

    def signature(tick):
        if not tick.unix_table_valid or set(tick.processes) != set(required_roles):
            return None
        result = []
        for role in required_roles:
            snap = tick.processes[role]
            if (snap.status is not SnapshotStatus.VALID or snap.fds is None
                    or snap.rss_bytes is None or snap.thread_count is None
                    or snap.process_start_time_ticks is None
                    or snap.count(FdKind.PROVIDER_UDS_CONNECTED)):
                return None
            result.append((role, snap.process_identity, snap.thread_count,
                           tuple(sorted(f.identity for f in snap.fds))))
        if extra_stability is not None and not extra_stability(tick):
            return None
        return tuple(result)

    previous = None
    try:
        while time.monotonic() < deadline:
            ns = clock.monotonic_ns()
            try:
                tick = sampler.sample_all(ns)
                current = signature(tick)
            except Exception as error:
                tick = SampleTick(ns, False, {}, errors=(type(error).__name__,))
                current = None
            observed.append(tick)
            stable_run = stable_run + [tick] if current is not None and current == previous else ([tick] if current is not None else [])
            previous = current
            if len(stable_run) >= required_consecutive:
                baseline = {
                    role: replace(stable_run[-1].processes[role], rss_bytes=int(median(
                        t.processes[role].rss_bytes for t in stable_run)))
                    for role in required_roles}
                return StableBaseline(tuple(stable_run), baseline), observed
            time.sleep(interval_seconds)
    except (KeyboardInterrupt, SystemExit):
        if on_interrupt is not None:
            on_interrupt(tuple(observed))
        raise
    return None, observed


def record_operation(
    sampler: TickSampler, operation, *, sample_seconds, baseline=None,
    clock=None, on_checkpoint=None,
) -> RecordedOperation:
    """Capture first/middle/final errors and checkpoint before propagating interrupts.

    The initial successful sample is the ready barrier. An unreadable initial
    sample prevents the operation. Finalization does not discard earlier ticks.
    """
    if sample_seconds < 0:
        raise ValueError("negative_sample_interval")
    clock = clock or SystemClock()
    started = clock.monotonic_ns()
    ticks, errors, interrupts = [], [], []
    stopping = threading.Event()

    def sample(phase):
        try:
            ticks.append(sampler.sample_all(clock.monotonic_ns()))
            return True
        except BaseException as error:
            errors.append(capture_error(error, phase))
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                interrupts.append(error)
            return False

    result = None
    operation_error = None
    ready = sample("sampling_first")
    if ready:
        first = ticks[0]
        required = set(baseline) if baseline is not None else set(first.processes)
        ready = (bool(required) and set(first.processes) == required
                 and first.unix_table_valid and not first.errors
                 and all(s.status is SnapshotStatus.VALID and s.fds is not None
                         and s.rss_bytes is not None and s.thread_count is not None
                         and s.process_start_time_ticks is not None
                         and (baseline is None or s.process_identity == baseline[r].process_identity)
                         for r, s in first.processes.items()))
        if not ready:
            errors.append(CapturedError("ObservationUnavailable", None,
                                        "sampling_first_invalid", "sampling_first"))
    actual_baseline = baseline if baseline is not None else (dict(ticks[0].processes) if ticks else {})

    def collect():
        while not stopping.wait(max(sample_seconds, .0001)):
            if not sample("sampling_middle"):
                return

    worker = threading.Thread(target=collect, name="resource-sampler")
    if ready:
        worker.start()
        try:
            result = operation()
        except BaseException as error:
            operation_error = capture_error(error, "operation")
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                interrupts.append(error)
        finally:
            stopping.set()
            worker.join()
            sample("sampling_final")
    recorded = RecordedOperation(
        result, operation_error, errors[0] if errors else None,
        ResourceTrace(actual_baseline, tuple(ticks)), started,
        clock.monotonic_ns(), tuple(errors))
    if on_checkpoint is not None:
        on_checkpoint(recorded)
    if interrupts:
        raise interrupts[0]
    return recorded


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
    def records():
        for role in sorted(trace.baseline):
            yield json.dumps({"kind": "baseline", **serialize_snapshot(role, trace.baseline[role])})
        for tick in trace.ticks:
            if not tick.processes:
                yield json.dumps({"kind": "empty_tick", "monotonic_ns": tick.monotonic_ns,
                                  "tick_errors": tick.errors, "unix_table_valid": tick.unix_table_valid})
            yield from serialize_tick(tick)
    _write_gzip_atomic(directory / "process_samples.jsonl.gz", records())


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
    payload = {"phase": phase, "result_summary": None,
               "started_ns": recorded.started_ns, "ended_ns": recorded.ended_ns,
               "sample_count": len(recorded.trace.ticks),
               "sampling_errors": [asdict(e) for e in recorded.sampling_errors]}
    if recorded.operation_error is not None:
        payload["operation_error"] = {
            "exception_type": recorded.operation_error.exception_type,
            "sqlstate": recorded.operation_error.sqlstate,
            "message": redact_text(recorded.operation_error.message),
            "phase": recorded.operation_error.phase}
    if recorded.sampling_error is not None:
        payload["sampling_error"] = {
            "exception_type": recorded.sampling_error.exception_type,
            "message": redact_text(recorded.sampling_error.message)}
    if isinstance(recorded.result, dict):
        payload["result_summary"] = {
            key: recorded.result[key] for key in recorded.result
            if isinstance(recorded.result[key], (int, str, bool, float, type(None)))}
    elif isinstance(recorded.result, (int, str, bool, float, type(None))):
        payload["result_summary"] = recorded.result
    write_atomic(directory / "operation_outcome.json",
                 redact_text(json.dumps(payload, indent=2)) + "\n")
