"""Periodic process-resource recorder with FD-change capture and atomic output.

The recorder is passive: it samples, records, and serializes traces. It never
decides pass/fail and owns no thresholds. Artifacts are written atomically
so that raw evidence exists before any policy evaluation can raise.
"""
from __future__ import annotations

import gzip
import json
import os
import threading
import time
from pathlib import Path
from typing import Callable, Mapping, Protocol

from .linux_procfs import snapshot_process
from .model import FdIdentity, FdKind, ProcessSnapshot, ResourceTrace


class Clock(Protocol):
    """Minimal monotonic clock seam for tests."""

    def monotonic_ns(self) -> int: ...


class SystemClock:
    def monotonic_ns(self) -> int:
        return time.monotonic_ns()


class ProcessSampler(Protocol):
    """Minimal sampler seam so unit tests can inject synthetic snapshots."""

    def __call__(self, role: str, monotonic_ns: int) -> ProcessSnapshot: ...


class ProcfsSampler:
    """Snapshot live processes; provider-path descriptors classified per role."""

    def __init__(self, pids: Mapping[str, int], provider_socket_path: str | None):
        self._pids = dict(pids)
        self._provider_socket_path = provider_socket_path

    def __call__(self, role: str, monotonic_ns: int) -> ProcessSnapshot:
        gateway_overrides = {
            FdKind.PROVIDER_UDS_ACCEPTED: FdKind.PROVIDER_UDS_ACCEPTED,
        }
        backend_overrides = {
            FdKind.PROVIDER_UDS_ACCEPTED: FdKind.PROVIDER_UDS_CLIENT,
        }
        overrides = backend_overrides if role == "backend" else gateway_overrides
        return snapshot_process(
            self._pids[role],
            monotonic_ns=monotonic_ns,
            provider_socket_path=self._provider_socket_path,
            role_overrides=overrides,
        )


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
        "monotonic_ns": snapshot.monotonic_ns,
        "rss_bytes": snapshot.rss_bytes,
        "thread_count": snapshot.thread_count,
        "total_fd_count": snapshot.total_fd_count,
        "fds": [_serialize_fd(item) for item in snapshot.fds],
    }


def write_atomic(path: Path, payload: str) -> None:
    temporary = path.with_name(path.name + ".partial")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def record_operation(
    sampler: ProcessSampler,
    roles: tuple[str, ...],
    operation: Callable[[], object],
    *,
    sample_seconds: float,
    clock: Clock | None = None,
) -> tuple[object, ResourceTrace]:
    """Run ``operation`` while sampling every role; return result and trace.

    Sampling covers the whole operation; baseline is the first sample of the
    run and is captured before the operation starts so peak deltas always
    have a pre-operation reference.
    """
    clock = clock or SystemClock()
    samples: list[Mapping[str, ProcessSnapshot]] = []
    failures: list[str] = []
    baseline: dict[str, ProcessSnapshot] = {}
    stopping = threading.Event()

    def take(point_ns: int) -> dict[str, ProcessSnapshot]:
        return {role: sampler(role, point_ns) for role in roles}

    baseline = take(clock.monotonic_ns())

    def collect() -> None:
        while not stopping.is_set():
            try:
                samples.append(take(clock.monotonic_ns()))
            except OSError as error:
                failures.append(type(error).__name__)
                return
            stopping.wait(sample_seconds)

    worker = threading.Thread(target=collect)
    worker.start()
    try:
        result = operation()
    finally:
        stopping.set()
        worker.join()
    if failures or not samples:
        raise OSError(f"resource sampling failed: {failures or 'no samples'}")
    return result, ResourceTrace(baseline=baseline, samples=tuple(samples))


def persist_trace(directory: Path, trace: ResourceTrace) -> None:
    """Write one JSONL.gz artifact containing baseline and all samples."""
    directory.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps({
        "kind": "baseline",
        **serialize_snapshot(role, snapshot),
    }) for role, snapshot in sorted(trace.baseline.items())]
    for sample in trace.samples:
        for role in sorted(sample):
            lines.append(json.dumps({
                "kind": "sample",
                **serialize_snapshot(role, sample[role]),
            }))
    payload = "\n".join(lines) + "\n"
    temporary = directory / "process_samples.jsonl.gz.partial"
    with gzip.open(temporary, "wt", encoding="utf-8", newline="\n") as handle:
        handle.write(payload)
    os.replace(temporary, directory / "process_samples.jsonl.gz")


def peaks(trace: ResourceTrace) -> dict[str, ProcessSnapshot]:
    """Per-role snapshot holding the maximum observed value of every field."""
    combined: dict[str, ProcessSnapshot] = {}
    for role, base in trace.baseline.items():
        rss = base.rss_bytes
        threads = base.thread_count
        fd_identities: dict[int, FdIdentity] = {item.fd: item for item in base.fds}
        for sample in trace.samples:
            point = sample.get(role)
            if point is None:
                continue
            rss = max(rss, point.rss_bytes)
            threads = max(threads, point.thread_count)
            for item in point.fds:
                fd_identities.setdefault(item.fd, item)
        combined[role] = ProcessSnapshot(
            monotonic_ns=base.monotonic_ns,
            rss_bytes=rss,
            thread_count=threads,
            fds=tuple(fd_identities[fd] for fd in sorted(fd_identities)),
        )
    return combined


def ending(trace: ResourceTrace) -> dict[str, ProcessSnapshot]:
    """Last observed snapshot per role."""
    last: dict[str, ProcessSnapshot] = {}
    for sample in trace.samples:
        for role, point in sample.items():
            last[role] = point
    return last
