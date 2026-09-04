"""Immutable process-resource observation model shared by qualification runners.

Values only: no thresholds, no Linux-specific collection, no PostgreSQL policy.
Experiment-specific limit policies consume these snapshots elsewhere.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping


class FdKind(Enum):
    """Classification of one file descriptor target.

    UNKNOWN must stay the default for unrecognized targets so that
    fail-closed policies can reject unclassified observations instead of
    silently dropping them from provider-UDS metrics.
    """

    PROVIDER_UDS_LISTENER = "provider_uds_listener"
    PROVIDER_UDS_CLIENT = "provider_uds_client"
    PROVIDER_UDS_ACCEPTED = "provider_uds_accepted"
    POSTGRES_CLIENT_SOCKET = "postgres_client_socket"
    RELATION_FILE = "relation_file"
    TOAST_RELATION_FILE = "toast_relation_file"
    POSTGRES_TEMP_FILE = "postgres_temp_file"
    REGULAR_FILE_OTHER = "regular_file_other"
    PIPE = "pipe"
    EVENTFD_OR_ANON_INODE = "eventfd_or_anon_inode"
    SOCKET_OTHER = "socket_other"
    UNKNOWN = "unknown"


PROVIDER_UDS_KINDS = frozenset({
    FdKind.PROVIDER_UDS_LISTENER,
    FdKind.PROVIDER_UDS_CLIENT,
    FdKind.PROVIDER_UDS_ACCEPTED,
})


@dataclass(frozen=True)
class FdIdentity:
    """One observed descriptor with its resolved target classification."""

    fd: int
    target: str
    kind: FdKind
    inode: int | None = None
    unix_path: str | None = None


@dataclass(frozen=True)
class ProcessSnapshot:
    """One point-in-time observation of a single process."""

    monotonic_ns: int
    rss_bytes: int
    thread_count: int
    fds: tuple[FdIdentity, ...]

    @property
    def total_fd_count(self) -> int:
        return len(self.fds)

    def count(self, kinds: FdKind | frozenset[FdKind]) -> int:
        if isinstance(kinds, FdKind):
            kinds = frozenset({kinds})
        return sum(1 for item in self.fds if item.kind in kinds)

    def fd_numbers(self, kinds: FdKind | frozenset[FdKind]) -> frozenset[int]:
        if isinstance(kinds, FdKind):
            kinds = frozenset({kinds})
        return frozenset(item.fd for item in self.fds if item.kind in kinds)


@dataclass(frozen=True)
class ResourceTrace:
    """Baseline plus sampled observations for every observed process role."""

    baseline: Mapping[str, ProcessSnapshot]
    samples: tuple[Mapping[str, ProcessSnapshot], ...]
