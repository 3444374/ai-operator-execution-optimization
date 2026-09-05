"""Immutable process-resource observation model shared by qualification runners.

Values only: no thresholds, no Linux-specific collection, no PostgreSQL policy.
Experiment-specific limit policies consume these snapshots elsewhere.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping


class FdKind(Enum):
    """Classification of one file descriptor target.

    The generic /proc collector distinguishes provider-path listeners and
    provider-path connected sockets by bound path only. An *unbound* AF_UNIX
    client (the backend provider end and the backend's own libpq link) has
    no pathname and is kept as UNBOUND_UNIX_SOCKET; experiment-specific
    attribution decides which of those are provider clients.

    POSTGRES_CLIENT_SOCKET remains reserved for cooperative in-process
    samplers (getsockname); external observers cannot produce it.

    UNKNOWN must stay the default for unrecognized targets so that
    fail-closed policies can reject unclassified observations instead of
    silently dropping them from provider-UDS metrics.
    """

    PROVIDER_UDS_LISTENER = "provider_uds_listener"
    PROVIDER_UDS_CONNECTED = "provider_uds_connected"
    UNBOUND_UNIX_SOCKET = "unbound_unix_socket"
    POSTGRES_CLIENT_SOCKET = "postgres_client_socket"
    RELATION_FILE = "relation_file"
    TOAST_RELATION_FILE = "toast_relation_file"
    POSTGRES_TEMP_FILE = "postgres_temp_file"
    REGULAR_FILE_OTHER = "regular_file_other"
    PIPE = "pipe"
    EVENTFD_OR_ANON_INODE = "eventfd_or_anon_inode"
    SOCKET_OTHER = "socket_other"
    UNKNOWN = "unknown"


class SnapshotStatus(Enum):
    """Explicit validity of one process snapshot.

    A snapshot may never use zeros or empty sets to mean "could not read":
    unreadable data stays ``None`` and the status says why.
    """

    VALID = "valid"
    PARTIAL = "partial"
    INVALID = "invalid"


@dataclass(frozen=True)
class FdIdentity:
    """One observed descriptor with its resolved target classification."""

    fd: int
    target: str
    kind: FdKind
    inode: int | None = None
    unix_path: str | None = None

    @property
    def identity(self) -> tuple:
        return (self.fd, self.inode, self.target)


@dataclass(frozen=True)
class UnixSocketInfo:
    path: str | None
    flags: int
    state: int
    socket_type: int


@dataclass(frozen=True)
class FdSnapshotAttempt:
    started_ns: int
    ended_ns: int
    fds: tuple[FdIdentity, ...] | None
    errors: tuple[str, ...]


@dataclass(frozen=True)
class PgFileClassificationContext:
    """Filenodes the runner learned from PostgreSQL for exact classification.

    Relation/toast classification matches these filenodes under the run's
    own data directory instead of guessing from numeric basenames.
    """

    data_directory: str
    relation_filenodes: frozenset[int] = frozenset()
    toast_filenodes: frozenset[int] = frozenset()


@dataclass(frozen=True)
class ProcessSnapshot:
    """One point-in-time observation of a single process.

    ``rss_bytes``/``thread_count``/``fds`` are ``None`` exactly when they
    could not be read; absent optionality means the value was observed.
    """

    pid: int
    process_start_time_ticks: int | None
    monotonic_ns: int
    status: SnapshotStatus
    errors: tuple[str, ...] = ()
    rss_bytes: int | None = None
    thread_count: int | None = None
    fds: tuple[FdIdentity, ...] | None = None
    observed_start_ns: int | None = None
    observed_end_ns: int | None = None
    fd_attempts: tuple[FdSnapshotAttempt, ...] = ()

    @property
    def process_identity(self) -> tuple:
        return (self.pid, self.process_start_time_ticks)

    @property
    def total_fd_count(self) -> int | None:
        return None if self.fds is None else len(self.fds)

    def count(self, kinds: FdKind | frozenset[FdKind]) -> int | None:
        if self.fds is None:
            return None
        if isinstance(kinds, FdKind):
            kinds = frozenset({kinds})
        return sum(1 for item in self.fds if item.kind in kinds)

    def fd_numbers(self, kinds: FdKind | frozenset[FdKind]) -> frozenset[int]:
        if isinstance(kinds, FdKind):
            kinds = frozenset({kinds})
        return frozenset(
            item.fd for item in (self.fds or ()) if item.kind in kinds)

    def unknown_identities(self) -> frozenset[tuple[int, str]]:
        """(fd, target) pairs of unclassified descriptors.

        End-state policies compare identity sets, not counts: an UNKNOWN
        replacing a closed classified fd keeps the count delta at zero
        while the process state is demonstrably NOT the baseline state.
        """
        return frozenset(
            (item.fd, item.target) for item in (self.fds or ())
            if item.kind is FdKind.UNKNOWN)


@dataclass(frozen=True)
class SampleTick:
    """Sequential observation batch, not an atomic cross-process snapshot.

    A tick reads ``/proc/net/unix`` once and then observes each role, so
    socket-table lookups cannot disagree between roles within the tick.
    """

    monotonic_ns: int
    unix_table_valid: bool
    processes: Mapping[str, ProcessSnapshot]
    ended_ns: int | None = None
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class StableBaseline:
    """A baseline accepted only after the identity set stopped changing."""

    ticks: tuple[SampleTick, ...]
    baseline: Mapping[str, ProcessSnapshot]

    @property
    def rss_median(self) -> dict[str, int]:
        medians: dict[str, int] = {}
        for role, snapshot in self.baseline.items():
            values = sorted(
                tick.processes[role].rss_bytes or 0
                for tick in self.ticks
                if tick.processes.get(role) is not None
                and tick.processes[role].rss_bytes is not None)
            if values:
                medians[role] = values[len(values) // 2]
        return medians


@dataclass(frozen=True)
class CapturedError:
    """Sanitized operation error: type, SQLSTATE, and a stable message."""

    exception_type: str
    sqlstate: str | None
    message: str
    phase: str


@dataclass(frozen=True)
class RecordedOperation:
    """Operation outcome plus the trace, preserved even when it raised."""

    result: object | None
    operation_error: CapturedError | None
    sampling_error: CapturedError | None
    trace: "ResourceTrace"
    started_ns: int | None = None
    ended_ns: int | None = None
    sampling_errors: tuple[CapturedError, ...] = ()


@dataclass(frozen=True)
class ResourceTrace:
    """Baseline plus sampled ticks for every observed process role.

    ``fd_correlation_evidence`` records post-hoc reclassification audits
    (e.g. provider client attribution); empty for raw recorder output.
    """

    baseline: Mapping[str, ProcessSnapshot]
    ticks: tuple[SampleTick, ...] = ()

    @property
    def samples(self) -> tuple[Mapping[str, ProcessSnapshot], ...]:
        return tuple(dict(tick.processes) for tick in self.ticks)

    fd_correlation_evidence: Mapping[int, Mapping[str, object]] = field(
        default_factory=dict)
