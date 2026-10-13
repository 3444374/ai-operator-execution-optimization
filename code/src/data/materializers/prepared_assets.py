"""Query-local reuse of immutable prepared bytes in a finite demand window.

This is a data-organization prototype, with no scheduler or model-result cache.
The producer declares already permitted consumers and a deterministic transform.
An acquired consumer keeps its buffer until its own read ends; model completion
and result consumption have separate owners. Dropping conditional future demand
never releases another consumer's buffer. Host reservations
cover prepared bytes and declared preparation scratch, not process RSS, source
buffers, serialized requests, pinned memory, or device allocations.
"""

from __future__ import annotations

import hashlib
from collections import Counter, OrderedDict
from dataclasses import dataclass
from typing import Callable
import math

from ...planning.work import WorkDescriptor


@dataclass(frozen=True)
class PreparedAssetKey:
    content_digest: str
    transform_signature: str

    def __post_init__(self):
        if (type(self.content_digest) is not str or len(self.content_digest) != 64
                or any(c not in "0123456789abcdef" for c in self.content_digest)
                or type(self.transform_signature) is not str or not self.transform_signature):
            raise ValueError("prepared asset requires a content digest and transform signature")


def prepared_asset_key(encoded: bytes, transform_signature: str) -> PreparedAssetKey:
    """Separate physical preparation identity from row, call, stage and Job IDs."""
    if type(encoded) is not bytes or not encoded:
        raise ValueError("prepared asset source must be non-empty immutable bytes")
    return PreparedAssetKey(hashlib.sha256(encoded).hexdigest(), transform_signature)


@dataclass(frozen=True)
class PreparedSourceMetadata:
    """Producer evidence from an immutable manifest and a single-image header.

    Stage work, locality, uncertainty and calibration remain in the existing
    WorkDescriptor. An image header supplies logical RGB work, not allocator
    peaks or visual tokens. This metadata never enters a model call.
    """

    object_id: str
    content_version: str
    work: WorkDescriptor
    provenance: str

    def __post_init__(self):
        PreparedAssetKey(self.content_version, "source-version-check")
        if any(type(value) is not str or not value for value in (self.object_id, self.provenance)):
            raise ValueError("source metadata requires an object identity and evidence origin")
        if type(self.work) is not WorkDescriptor:
            raise TypeError("source metadata references the existing WorkDescriptor")


@dataclass(frozen=True)
class AssetUse:
    consumer_id: str
    key: PreparedAssetKey
    source: PreparedSourceMetadata | None = None
    ready_after: str | None = None

    def __post_init__(self):
        if type(self.consumer_id) is not str or not self.consumer_id:
            raise ValueError("asset consumer identity must be non-empty text")
        if type(self.key) is not PreparedAssetKey:
            raise TypeError("asset use requires a prepared asset key")
        if self.source is not None and (
                type(self.source) is not PreparedSourceMetadata
                or self.source.content_version != self.key.content_digest):
            raise ValueError("source evidence differs from the prepared asset version")
        if self.ready_after is not None and (type(self.ready_after) is not str or not self.ready_after):
            raise ValueError("conditional demand requires an explicit producer stage")


@dataclass(frozen=True)
class PreparedAssetLease:
    consumer_id: str
    key: PreparedAssetKey
    payload: bytes


@dataclass(frozen=True)
class PreparedAssetSnapshot:
    pending_consumers: int
    held_consumers: int
    resident_assets: int
    prepared_bytes: int
    preparation_reserved_bytes: int
    host_reservation_peak_bytes: int
    prepare_calls: int
    reuse_hits: int
    evictions: int
    resident_byte_seconds: float | None = None


class PreparedAssetWindow:
    """Own one finite window; individual model calls remain the producer's work.

    Only idle buffers can be evicted. The last actual consumer releases an asset
    immediately. A cache miss reserves the output upper bound plus temporary
    scratch before preparation. Callbacks run synchronously on one owner thread
    and must honor that declaration; there is no background preparation queue.
    """

    def __init__(self, uses: tuple[AssetUse, ...], *, max_consumers: int, host_bytes: int,
                 clock: Callable[[], float] | None = None):
        if any(type(v) is not int or v < 1 for v in (max_consumers, host_bytes)):
            raise ValueError("asset window capacities must be positive integers")
        if type(uses) is not tuple or not 1 <= len(uses) <= max_consumers:
            raise ValueError("asset demand must be a finite tuple within consumer capacity")
        if any(type(use) is not AssetUse for use in uses):
            raise TypeError("asset demand requires AssetUse values")
        if len({use.consumer_id for use in uses}) != len(uses):
            raise ValueError("asset consumer identities must be distinct")
        self._pending = {use.consumer_id: use.key for use in uses}
        self._gates = {use.consumer_id: use.ready_after for use in uses if use.ready_after is not None}
        self._remaining = Counter(use.key for use in uses)
        self._held: dict[str, PreparedAssetLease] = {}
        self._assets: OrderedDict[PreparedAssetKey, bytes] = OrderedDict()
        self._host_bytes = host_bytes
        self._bytes = self._reserved = self._peak = self._calls = self._hits = self._evictions = 0
        if clock is not None and not callable(clock):
            raise TypeError("asset residency clock must be callable")
        self._clock = clock
        self._last_time = None
        self._byte_seconds = 0.0
        self._account_time()

    def acquire(self, consumer_id: str, *, prepare: Callable[[], bytes],
                prepared_bytes_upper_bound: int, scratch_bytes_upper_bound: int = 0
                ) -> PreparedAssetLease | None:
        """Return None under buffer pressure, without invoking preparation."""
        self._check_owner_callback()
        self._account_time()
        if (type(prepared_bytes_upper_bound) is not int or prepared_bytes_upper_bound < 1
                or type(scratch_bytes_upper_bound) is not int or scratch_bytes_upper_bound < 0):
            raise ValueError("preparation requires positive output and non-negative scratch bytes")
        if consumer_id not in self._pending:
            raise ValueError("asset consumer is unknown, acquired or already settled")
        if consumer_id in self._gates:
            raise ValueError("asset demand is not ready until its producer stage resolves")
        key = self._pending[consumer_id]
        if key in self._assets:
            payload = self._assets[key]
            if len(payload) > prepared_bytes_upper_bound:
                raise BufferError("cached asset exceeds this consumer's declared output size")
            self._hits += 1
            self._assets.move_to_end(key)
        else:
            required = prepared_bytes_upper_bound + scratch_bytes_upper_bound
            if required > self._host_bytes:
                raise BufferError("one asset preparation cannot fit the host capacity")
            active = {lease.key for lease in self._held.values()}
            for idle_key in tuple(self._assets):
                if self._bytes + required <= self._host_bytes:
                    break
                if idle_key not in active:
                    self._bytes -= len(self._assets.pop(idle_key))
                    self._evictions += 1
            if self._bytes + required > self._host_bytes:
                return None
            self._peak = max(self._peak, self._bytes + required)
            self._calls += 1
            self._reserved = required
            try:
                payload = prepare()
                if type(payload) is not bytes or not payload:
                    raise TypeError("prepared asset must be non-empty immutable bytes")
                if len(payload) > prepared_bytes_upper_bound:
                    raise BufferError("prepared asset exceeds its declared output size")
            finally:
                self._reserved = 0
                self._account_time()
            self._assets[key] = payload
            self._bytes += len(payload)
        lease = PreparedAssetLease(consumer_id, key, payload)
        del self._pending[consumer_id]
        self._held[consumer_id] = lease
        return lease

    def release(self, lease: PreparedAssetLease) -> None:
        """End this buffer read, independently of model end or result consumption.

        An exporter may release after copying into a separately owned string.
        That string/request stays live under its real owner; this is no receipt
        that the model, transport, SDK replay or output consumer has stopped.
        """
        self._check_owner_callback()
        self._account_time()
        if self._held.get(lease.consumer_id) is not lease:
            raise ValueError("asset lease is not held by this window")
        del self._held[lease.consumer_id]
        self._settle(lease.key)

    def cancel_pending(self, consumer_id: str) -> None:
        """Remove only unacquired demand; unknown remote work stays held."""
        self._check_owner_callback()
        self._account_time()
        if consumer_id not in self._pending:
            raise ValueError("only an unacquired asset consumer can be cancelled")
        self._gates.pop(consumer_id, None)
        self._settle(self._pending.pop(consumer_id))

    def resolve_stage(self, stage_id: str, required_consumers: tuple[str, ...]) -> None:
        """Use a producer's actual stage output to settle conditional demand.

        All declared candidates at this barrier are resolved atomically. No
        downstream preparation/call is authorized before this receipt. The
        prototype neither predicts predicates nor changes order/readiness.
        """
        self._check_owner_callback()
        self._account_time()
        candidates = {consumer for consumer, stage in self._gates.items() if stage == stage_id}
        if type(stage_id) is not str or not stage_id or not candidates:
            raise ValueError("unknown or already resolved producer stage")
        if (type(required_consumers) is not tuple
                or any(type(consumer) is not str for consumer in required_consumers)
                or len(set(required_consumers)) != len(required_consumers)
                or not set(required_consumers) <= candidates):
            raise ValueError("required consumers must belong to this unresolved stage")
        for consumer in candidates:
            del self._gates[consumer]
            if consumer not in required_consumers:
                self._settle(self._pending.pop(consumer))

    def _account_time(self) -> None:
        if self._clock is None:
            return
        now = self._clock()
        if (type(now) not in (int, float) or not math.isfinite(now)
                or self._last_time is not None and now < self._last_time):
            raise ValueError("asset residency clock must be finite and monotonic")
        if self._last_time is not None:
            self._byte_seconds += self._bytes * (now - self._last_time)
        self._last_time = now

    def _check_owner_callback(self) -> None:
        if self._reserved:
            raise RuntimeError("preparation callbacks cannot mutate their asset window")

    def _settle(self, key: PreparedAssetKey) -> None:
        self._remaining[key] -= 1
        if self._remaining[key] == 0:
            del self._remaining[key]
            payload = self._assets.pop(key, None)
            if payload is not None:
                self._bytes -= len(payload)

    def snapshot(self) -> PreparedAssetSnapshot:
        self._account_time()
        return PreparedAssetSnapshot(len(self._pending), len(self._held), len(self._assets),
                                     self._bytes, self._reserved, self._peak, self._calls,
                                     self._hits, self._evictions,
                                     self._byte_seconds if self._clock is not None else None)
