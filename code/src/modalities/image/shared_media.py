"""Bounded, owner-thread retention of immutable media for declared consumers.

Diagnostic frontend helper, before TaskPreparation/admission. The bound covers
retained URI payloads owned here. LOTUS arrays/messages, encoding temporaries,
SDK copies and Python metadata are separate owners; this is not an RSS bound.
No model answers, model concurrency, or semantic decisions are cached.
"""

import hashlib
import json
import time
import uuid

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

    def __init__(self, plan, *, capacity_bytes, max_entries, max_item_bytes, max_consumers,
                 retention_enabled=True, observer=None, retention_policy=None):
        if (any(type(x) is not int or x <= 0 for x in (capacity_bytes, max_entries, max_item_bytes, max_consumers))
                or not plan or len(plan) > max_entries):
            raise ValueError('positive finite retention bounds and plan required')
        if type(retention_enabled) is not bool:
            raise ValueError('retention selection must be explicit boolean')
        if observer is not None and not callable(observer):
            raise TypeError('representation observer must be callable')
        if retention_policy not in (None, 'consumer_rebuild_value', 'lru'):
            raise ValueError('unknown representation retention policy')
        if retention_policy is not None and not retention_enabled:
            raise ValueError('a retention policy requires enabled retention')
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
        self._observer, self._observation_errors, self._observation_sequence = observer, 0, 0
        self._owner_uid = uuid.uuid4().hex if observer is not None else None
        self.retention_policy = retention_policy
        self._rebuild_ns = {}
        self._last_access, self._access_sequence = {}, 0
        self._evictions = 0
        self._observe('owner_open', capacity_bytes=capacity_bytes, max_entries=max_entries,
                      declared_consumers=sum(map(len, self.pending.values())), retention_enabled=retention_enabled,
                      policy=retention_policy or 'keep_until_last_consumer')

    def _observe(self, action, *, key=None, consumer=None, **fields):
        if self._observer is None:
            return
        self._observation_sequence += 1
        try:
            self._observer(dict(event='representation_lifecycle', action=action,
                owner_uid=self._owner_uid, owner_sequence=self._observation_sequence,
                observation_ns=time.monotonic_ns(), representation_key=key,
                consumer_ref=None if consumer is None else hashlib.sha256(consumer.encode()).hexdigest(),
                retained_bytes=self.budget.bytes, retained_entries=self.budget.runs,
                active_borrows=len(self.active),
                pending_consumers=None if key is None else len(self.pending[key]), **fields))
        except Exception:
            self._observation_errors += 1

    def _release(self, key, reason):
        value = self.values.pop(key)
        self.budget.release(len(value))
        self._rebuild_ns.pop(key, None)
        self._last_access.pop(key, None)
        self._observe('retained_release', key=key, payload_bytes=len(value),
                      storage_id=str(id(value)), reason=reason)

    def _retain(self, key, value, rebuild_ns):
        if self.budget.reserve(len(value)):
            return True
        if self.retention_policy is None:
            return False
        from src.scheduling.organization.representation_retention import RetentionCandidate, consumer_value_victims, lru_victims
        active = {k: sum(1 for edge in self.active if edge[0] == k) for k in self.values}
        retained = tuple(RetentionCandidate(k, len(v), len(self.pending[k]) - active[k],
                                            self._rebuild_ns[k], active[k], self._last_access.get(k, 0))
                         for k, v in self.values.items())
        incoming = RetentionCandidate(key, len(value),
            len(self.pending[key]) - 1 - sum(1 for edge in self.active if edge[0] == key), rebuild_ns)
        choose = consumer_value_victims if self.retention_policy == 'consumer_rebuild_value' else lru_victims
        victims = choose(incoming, retained, capacity_bytes=self.budget.capacity.bytes,
                         max_entries=self.budget.capacity.runs)
        self._observe('retention_choice', key=key, victim_keys=list(victims),
                      estimated_reuse_ns=incoming.value, rebuild_ns=rebuild_ns,
                      policy=self.retention_policy)
        if not victims:
            return False
        for victim in victims:
            self._release(victim, self.retention_policy + '_choice')
            self._evictions += 1
        return self.budget.reserve(len(value))

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
            hit = True
        else:
            hit = False
            self._observe('prepare_start', key=key, consumer=consumer, source_bytes=len(raw))
            timed = self._observer is not None or self.retention_policy is not None
            started = time.monotonic_ns() if timed else 0
            try:
                value = producer(raw)
            except BaseException as error:
                self._observe('prepare_error', key=key, consumer=consumer, error_type=type(error).__name__)
                raise
            rebuild_ns = time.monotonic_ns() - started if timed else 0
            if (type(value) is not str or not value.isascii()
                    or not value.startswith('data:image/png;base64,') or len(value) > self.max_item_bytes):
                self._observe('prepare_error', key=key, consumer=consumer, error_type='InvalidRepresentation')
                raise ValueError('incompatible or oversized immutable representation')
            self._observe('prepare_end', key=key, consumer=consumer, payload_bytes=len(value),
                          storage_id=str(id(value)), rebuild_ns=rebuild_ns)
            self.misses += 1
            future = (len(self.pending[key]) - 1 - sum(1 for active_key, _ in self.active if active_key == key)
                      if self.retention_policy is not None else 0)
            if len(self.pending[key]) <= 1 or self.retention_policy is not None and future == 0:
                self.no_future_bypasses += 1
                self._observe('retention_bypass', key=key, reason='no_future_consumer')
            elif not self.retention_enabled:
                self.disabled_bypasses += 1
                self._observe('retention_bypass', key=key, reason='disabled')
            elif self._retain(key, value, rebuild_ns):
                self.values[key] = value
                self._rebuild_ns[key] = rebuild_ns
                self.peak_bytes = max(self.peak_bytes, self.budget.bytes)
                self._observe('retained_acquire', key=key, payload_bytes=len(value), storage_id=str(id(value)))
            else:
                self.capacity_bypasses += 1
                self._observe('retention_bypass', key=key, reason='capacity')
        self.active.add(edge)
        if key in self.values and self.retention_policy == 'lru':
            self._access_sequence += 1
            self._last_access[key] = self._access_sequence
        self._observe('borrow', key=key, consumer=consumer, cache_hit=hit,
                      storage_id=str(id(value)), payload_bytes=len(value), retained=key in self.values)
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
        self._observe('consumer_terminal', key=key, consumer=consumer)
        if not self.pending[key] and key in self.values:
            self._release(key, 'last_consumer_terminal')

    def abandon_unstarted(self, source_sha256, profile, consumer):
        """Plan cancellation may remove only an edge that never borrowed data."""
        key = media_key(source_sha256, profile)
        edge = key, consumer
        if (self.closed or edge in self.active or edge in self.finished
                or key not in self.pending or consumer not in self.pending[key]):
            raise ValueError('only an unstarted planned edge may be abandoned')
        self.pending[key].remove(consumer)
        self.finished.add(edge)
        self._observe('consumer_abandoned', key=key, consumer=consumer)
        if not self.pending[key] and key in self.values:
            self._release(key, 'last_unstarted_consumer_abandoned')

    def snapshot(self):
        return dict(retained_bytes=self.budget.bytes, retained_entries=self.budget.runs,
                    active_borrows=len(self.active), pending_consumers=sum(map(len, self.pending.values())),
                    peak_retained_bytes=self.peak_bytes, cache_hits=self.hits,
                    encodings=self.misses, capacity_bypasses=self.capacity_bypasses,
                    no_future_consumer_bypasses=self.no_future_bypasses,
                    retention_disabled_bypasses=self.disabled_bypasses,
                    capacity_bytes=self.budget.capacity.bytes, max_item_bytes=self.max_item_bytes,
                    byte_scope='owner-retained ASCII URI payloads; excludes frontend/encoder/SDK/metadata/RSS',
                    **(dict(retention_policy=self.retention_policy, retention_evictions=self._evictions)
                       if self.retention_policy is not None else {}),
                    **(dict(observation_errors=self._observation_errors, observation_events=self._observation_sequence)
                       if self._observer is not None else {}))

    def close(self):
        if self.closed:
            return
        if self.active or any(self.pending.values()) or self.values:
            raise RuntimeError('retention still has active or future consumer owners')
        self.budget.close()
        self.closed = True
        self._observe('owner_close', declared_pending=sum(map(len, self.pending.values())),
                      observation_errors=self._observation_errors)
