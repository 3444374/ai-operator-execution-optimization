"""Bounded, owner-thread retention of immutable media for declared consumers.

Diagnostic frontend helper, before TaskPreparation/admission. The bound covers
retained URI payloads owned here. LOTUS arrays/messages, encoding temporaries,
SDK copies and Python metadata are separate owners; this is not an RSS bound.
No model answers, model concurrency, or semantic decisions are cached.
"""

import hashlib
import json

from src.semantic_methods.budget import MethodBudgetPool, MethodCapacity


def media_key(source_sha256, profile):
    required = {'media_recipe', 'model', 'generation', 'service_preprocess'}
    if (type(source_sha256) is not str or len(source_sha256) != 64
            or any(c not in '0123456789abcdef' for c in source_sha256)
            or type(profile) is not dict or not required <= set(profile)
            or any(not profile[k] for k in required)):
        raise ValueError('complete source and representation compatibility identity required')
    # Freeze caller dictionaries into a value identity. Filenames and predicates
    # are intentionally absent; the representation contains neither one.
    raw = json.dumps(dict(source_sha256=source_sha256, profile=profile),
                     ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(raw.encode()).hexdigest()


class SharedMediaRepresentation:
    """Keep a representation through its last declared terminal consumer.

    Callers declare finite consumer-edge IDs from their verified logical plan.
    borrow validates source bytes on hits as well as misses. finish is legal only
    once the consumer has returned/failed terminally and no async work borrows it.
    close never treats an outstanding consumer as terminal.
    """

    def __init__(self, plan, *, capacity_bytes, max_entries, max_item_bytes, max_consumers, retention_enabled=True):
        if (any(type(x) is not int or x <= 0 for x in (capacity_bytes, max_entries, max_item_bytes, max_consumers))
                or not plan or len(plan) > max_entries):
            raise ValueError('positive finite retention bounds and plan required')
        if type(retention_enabled) is not bool:
            raise ValueError('retention selection must be explicit boolean')
        self.retention_enabled = retention_enabled
        self.pending = {}
        for key, consumers in plan.items():
            edges = consumers
            if (type(edges) is not tuple or not edges or len(edges) > max_consumers
                    or len(edges) != len(set(edges))
                    or any(type(c) is not str or not c or len(c.encode()) > 256 for c in edges)):
                raise ValueError('unique declared consumer edges required')
            self.pending[key] = set(edges)
        if sum(map(len, self.pending.values())) > max_consumers:
            raise ValueError('consumer plan exceeds its metadata bound')
        self.pool = MethodBudgetPool(MethodCapacity(max_entries, capacity_bytes))
        self.budget = self.pool.allocate(MethodCapacity(max_entries, capacity_bytes))
        self.budget.claim()
        self.max_item_bytes = max_item_bytes
        self.values, self.active, self.finished = {}, set(), set()
        self.closed = False
        self.hits = self.misses = self.capacity_bypasses = self.no_future_bypasses = self.disabled_bypasses = self.peak_bytes = 0

    def borrow(self, raw, source_sha256, profile, consumer, producer):
        if self.closed or type(raw) is not bytes or hashlib.sha256(raw).hexdigest() != source_sha256:
            raise ValueError('closed owner or changed source bytes')
        key = media_key(source_sha256, profile)
        edge = key, consumer
        if key not in self.pending or consumer not in self.pending[key] or edge in self.active or edge in self.finished:
            raise ValueError('unknown, repeated or terminal consumer edge')
        # The original image producer remains the owner of encoding temporaries.
        # Admission here retains only complete immutable ASCII URI values.
        if key in self.values:
            value = self.values[key]
            self.hits += 1
        else:
            value = producer(raw)
            if (type(value) is not str or not value.isascii()
                    or not value.startswith('data:image/png;base64,') or len(value) > self.max_item_bytes):
                raise ValueError('incompatible or oversized immutable representation')
            self.misses += 1
            if len(self.pending[key]) <= 1:
                self.no_future_bypasses += 1
            elif not self.retention_enabled:
                self.disabled_bypasses += 1
            elif self.budget.reserve(len(value)):
                self.values[key] = value
                self.peak_bytes = max(self.peak_bytes, self.budget.bytes)
            else:
                self.capacity_bypasses += 1
        self.active.add(edge)
        return value

    def finish(self, source_sha256, profile, consumer):
        if self.closed:
            raise RuntimeError('retention owner is closed')
        key = media_key(source_sha256, profile)
        edge = key, consumer
        if edge not in self.active:
            raise ValueError('consumer has no active representation borrow')
        self.active.remove(edge)
        self.finished.add(edge)
        self.pending[key].remove(consumer)
        if not self.pending[key] and key in self.values:
            self.budget.release(len(self.values.pop(key)))

    def abandon_unstarted(self, source_sha256, profile, consumer):
        """Plan cancellation may remove only an edge that never borrowed data."""
        key = media_key(source_sha256, profile)
        edge = key, consumer
        if (self.closed or edge in self.active or edge in self.finished
                or key not in self.pending or consumer not in self.pending[key]):
            raise ValueError('only an unstarted planned edge may be abandoned')
        self.pending[key].remove(consumer)
        self.finished.add(edge)
        if not self.pending[key] and key in self.values:
            self.budget.release(len(self.values.pop(key)))

    def snapshot(self):
        return dict(retained_bytes=self.budget.bytes, retained_entries=self.budget.runs,
                    active_borrows=len(self.active), pending_consumers=sum(map(len, self.pending.values())),
                    peak_retained_bytes=self.peak_bytes, cache_hits=self.hits,
                    encodings=self.misses, capacity_bypasses=self.capacity_bypasses,
                    no_future_consumer_bypasses=self.no_future_bypasses,
                    retention_disabled_bypasses=self.disabled_bypasses,
                    capacity_bytes=self.budget.capacity.bytes, max_item_bytes=self.max_item_bytes,
                    byte_scope='owner-retained ASCII URI payloads; excludes frontend/encoder/SDK/metadata/RSS')

    def close(self):
        if self.closed:
            return
        if self.active or any(self.pending.values()) or self.values:
            raise RuntimeError('retention still has active or future consumer owners')
        self.budget.close()
        self.closed = True
