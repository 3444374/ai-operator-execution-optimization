"""Actor-owned input reuse; these byte counts are not physical GPU memory.

This cache owns only retained input objects. Model weights, forward scratch,
Torch allocator reservations and uncached inputs remain outside its budget.
The owner confirms device completion before releasing a lease; cancellation
alone is never completion. No scheduler, transport or peer-to-peer copy lives
here.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable

import numpy as np

from ...data.materializers.prepared_assets import PreparedAssetKey


def estimate(descriptor, destination, now_s) -> None:
    """Conform to the shared HSE locality callback without guessing a cost.

    Existing StageBlockDescriptor identifies encoded bytes plus row IDs, while
    this cache identifies actual prepared tensor contents. There is no verified
    mapping at selection time. Reading a tensor just to estimate its placement
    would add materialization and hashing to the scheduler, so return unknown.
    """
    return None


def prepared_tensor_key(payload: np.ndarray, *, processor_revision: str,
                        device_dtype: str) -> PreparedAssetKey:
    """Identify actual prepared contents independently of row and call IDs."""
    if not processor_revision or not device_dtype:
        raise ValueError("input reuse requires processor and target dtype identities")
    if payload.ndim != 4 or not payload.size or payload.dtype.hasobject:
        raise ValueError("input reuse requires a non-empty numeric NCHW tensor")
    pixels = np.ascontiguousarray(payload)
    representation = json.dumps({
        "schema": "prepared-tensor-v1", "shape": pixels.shape, "layout": "NCHW",
        "dtype": pixels.dtype.str, "processor_revision": processor_revision,
        "device_dtype": device_dtype,
    }, sort_keys=True, separators=(",", ":"))
    return PreparedAssetKey(
        hashlib.sha256(memoryview(pixels).cast("B")).hexdigest(), representation)


@dataclass(frozen=True)
class InputTransferCost:
    reuse_available: bool
    host_reload_bytes: int
    device_copy_bytes: int
    transfer_s: float | None = None
    p2p_supported: bool = False


@dataclass(frozen=True)
class ResidentInput:
    key: PreparedAssetKey
    logical_bytes: int
    references: int
    reusable: bool


@dataclass(frozen=True)
class InputResidencySnapshot:
    cache_id: str
    location: str
    budget_bytes: int
    max_entries: int
    resident_bytes: int
    reserved_bytes: int
    logical_bytes_peak: int
    entries: tuple[ResidentInput, ...]
    held_references: int
    unknown_references: int
    hits: int
    misses: int
    evictions: int


@dataclass(frozen=True)
class InputLease:
    key: PreparedAssetKey
    payload: object
    logical_bytes: int
    cache_hit: bool


@dataclass
class _Entry:
    payload: object
    logical_bytes: int
    references: int = 0
    reusable: bool = True


class DeviceInputCache:
    """A serial actor's bounded LRU inputs, with completion-owned references.

    ``location`` is an actor-local label, not proof of a physical GPU. A fresh
    cache_id distinguishes a replacement actor. A miss that cannot fit returns
    None before calling the loader, allowing the original uncached path.
    A loader exception keeps its reservation until a successful device fence.
    """

    def __init__(self, *, budget_bytes: int, location: str, max_entries: int = 8):
        if type(budget_bytes) is not int or budget_bytes < 0 or not location:
            raise ValueError("input cache requires non-negative bytes and a location")
        if type(max_entries) is not int or max_entries < 1:
            raise ValueError("input cache entry capacity must be a positive integer")
        self._budget, self._location = budget_bytes, location
        self._max_entries = max_entries
        self._cache_id = uuid.uuid4().hex
        self._entries: OrderedDict[PreparedAssetKey, _Entry] = OrderedDict()
        self._held: dict[int, InputLease] = {}
        self._unknown: set[int] = set()
        self._bytes = self._reserved = self._hits = self._misses = self._evictions = 0
        self._peak = 0
        self._loading = False

    def acquire(self, key: PreparedAssetKey, *, logical_bytes: int,
                load: Callable[[], object]) -> InputLease | None:
        self._check_bytes(key, logical_bytes)
        self._check_mutation()
        if self._unknown or self._reserved:
            raise RuntimeError("input completion is unknown; confirm synchronization first")
        entry = self._entries.get(key)
        hit = entry is not None and entry.reusable
        if hit:
            if logical_bytes != entry.logical_bytes:
                raise ValueError("resident input byte count does not match its identity")
            self._hits += 1
            self._entries.move_to_end(key)
        else:
            self._misses += 1
            if entry is not None or logical_bytes > self._budget:
                return None
            idle = [k for k, v in self._entries.items() if not v.references]
            reclaimable = sum(self._entries[k].logical_bytes for k in idle)
            if (self._bytes + logical_bytes - reclaimable > self._budget
                    or len(self._entries) - len(idle) >= self._max_entries):
                return None
            for idle_key in idle:
                if (self._bytes + logical_bytes <= self._budget
                        and len(self._entries) < self._max_entries):
                    break
                self._evict(idle_key)
            self._reserved, self._loading = logical_bytes, True
            self._peak = max(self._peak, self._bytes + self._reserved)
            try:
                payload = load()
                if payload is None:
                    raise ValueError("input loader must return an owned object")
            finally:
                # On an exception no payload is published and the reservation
                # stays held: the owner must fence potentially submitted work.
                self._loading = False
            entry = _Entry(payload, logical_bytes)
            self._entries[key] = entry
            self._bytes += logical_bytes
            self._reserved = 0
        lease = InputLease(key, entry.payload, logical_bytes, hit)
        entry.references += 1
        self._held[id(lease)] = lease
        return lease

    def release(self, lease: InputLease, *, retain: bool = True) -> None:
        """The owner calls this only after authoritative device completion."""
        self._check_mutation()
        self._check_lease(lease)
        if id(lease) in self._unknown:
            raise RuntimeError("unknown input requires synchronization confirmation")
        entry = self._entries[lease.key]
        del self._held[id(lease)]
        entry.references -= 1
        entry.reusable &= retain
        if not entry.references and not entry.reusable:
            self._evict(lease.key)

    def mark_unknown(self, lease: InputLease) -> None:
        self._check_mutation()
        self._check_lease(lease)
        self._unknown.add(id(lease))
        self._entries[lease.key].reusable = False

    def confirm_synchronized(self) -> None:
        """Call only after this cache's device fence succeeds."""
        self._check_mutation()
        for token in tuple(self._unknown):
            self._unknown.remove(token)
            self.release(self._held[token], retain=False)
        self._reserved = 0

    def clear(self) -> None:
        """Stop reuse; active or unknown users continue to hold their objects."""
        self._check_mutation()
        for key, entry in tuple(self._entries.items()):
            entry.reusable = False
            if not entry.references:
                self._evict(key)

    def transfer_cost(self, key: PreparedAssetKey, *, host_bytes: int,
                      device_bytes: int) -> InputTransferCost:
        self._check_bytes(key, device_bytes)
        if type(host_bytes) is not int or host_bytes < 1:
            raise ValueError("host input bytes must be a positive integer")
        entry = self._entries.get(key)
        hit = (entry is not None and entry.reusable and not self._reserved
               and not self._unknown and entry.logical_bytes == device_bytes)
        return InputTransferCost(hit, 0 if hit else host_bytes, 0 if hit else device_bytes)

    def snapshot(self) -> InputResidencySnapshot:
        return InputResidencySnapshot(
            self._cache_id, self._location, self._budget, self._max_entries,
            self._bytes, self._reserved, self._peak,
            tuple(ResidentInput(k, v.logical_bytes, v.references, v.reusable)
                  for k, v in self._entries.items()), len(self._held), len(self._unknown),
            self._hits, self._misses, self._evictions)

    @staticmethod
    def _check_bytes(key, logical_bytes):
        if type(key) is not PreparedAssetKey:
            raise TypeError("input reuse requires a PreparedAssetKey")
        if type(logical_bytes) is not int or logical_bytes < 1:
            raise ValueError("input bytes must be a positive integer")

    def _check_mutation(self):
        if self._loading:
            raise RuntimeError("input loader cannot mutate its cache")

    def _check_lease(self, lease):
        if type(lease) is not InputLease or self._held.get(id(lease)) is not lease:
            raise ValueError("input lease is not held by this cache")

    def _evict(self, key):
        self._bytes -= self._entries.pop(key).logical_bytes
        self._evictions += 1
