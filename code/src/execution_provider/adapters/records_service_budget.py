"""Exclusive, process-local grants for ordinary async payload envelopes.

The service supplies the total. This allocator neither discovers RAM nor owns
request admission. Core and IPC continue enforcing their existing reservations.
Python/Ray overhead, caller relations/audits and model/KV memory are not covered.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
import threading
import uuid
import weakref
from typing import TYPE_CHECKING

from .records_capacity import CoreRecordsCapacity, resolve_records_async_limits

if TYPE_CHECKING:
    from src.serving.backends.vllm_async_engine import AsyncEngineLimits


def _positive_bytes(value, name):
    if type(value) is not int or value <= 0:
        raise ValueError(name + ' must be positive integer payload bytes')
    return value


@dataclass(frozen=True)
class RecordsPayloadPlan:
    capacity: CoreRecordsCapacity
    ipc_limits: AsyncEngineLimits
    components: tuple[tuple[str, int], ...]

    @property
    def bytes(self):
        return sum(size for _, size in self.components)

    def describe(self):
        return dict(capacity=asdict(self.capacity), ipc_limits=asdict(self.ipc_limits),
                    components=dict(self.components), bytes=self.bytes,
                    scope='ordinary async declared payload envelopes; not RAM or GPU capacity')

    @property
    def signature(self):
        return hashlib.sha256(json.dumps(self.describe(), sort_keys=True,
            separators=(',', ':')).encode()).hexdigest()


def _plan(shape, requests):
    from src.semantic_methods.budget import row_reservation
    from src.semantic_methods.lotus.records import RecordsResources

    capacity = shape.with_storage_bytes(input_bytes=requests * shape.item_input_bytes,
        result_bytes=requests * shape.item_result_bytes,
        method_bytes=row_reservation(shape.method_limits(1)))
    limits = resolve_records_async_limits(RecordsResources(max_active_batches=None), capacity)
    budget = capacity.budget
    parts = [('core_input', budget.input_bytes), ('core_result', budget.result_bytes),
             ('core_metadata', budget.held_tasks * shape.metadata_bytes),
             ('source_pending', shape.item_input_bytes),
             ('method_state', budget.method_bytes)]
    for endpoint in ('parent', 'child'):
        parts.extend((('ipc_' + endpoint + '_input', limits.input_bytes),
                      ('ipc_' + endpoint + '_result', limits.result_bytes),
                      ('ipc_' + endpoint + '_control', limits.control_slots * limits.control_frame_bytes)))
    parts.extend((('actor_prepared', requests * limits.item_input_bytes),
                  ('actor_results', requests * limits.item_result_bytes),
                  ('transport_prepared', requests * limits.item_input_bytes),
                  ('transport_results', budget.result_bytes),
                  ('closing_results', budget.result_bytes)))
    return RecordsPayloadPlan(capacity, limits, tuple(parts))


class RecordsPayloadPool:
    """One service's finite logical storage; live grants cannot overlap."""

    def __init__(self, total_bytes):
        self._total = _positive_bytes(total_bytes, 'service total')
        self._pid = os.getpid()
        self._lock = threading.RLock()
        self._grants = {}
        self._closing = False

    def __getstate__(self):
        raise TypeError('payload pool and grant capabilities cannot be serialized or copied')

    @property
    def total_bytes(self):
        return self._total

    def _process(self):
        if os.getpid() != self._pid:
            raise RuntimeError('payload grants belong to their original service process')

    def _select(self, shape, maximum):
        self._process()
        if self._closing:
            raise RuntimeError('payload service is closing')
        if type(shape) is not CoreRecordsCapacity or shape.budget is not None:
            raise ValueError('payload allocation requires a per-item shape without an existing budget')
        if maximum is not None:
            _positive_bytes(maximum, 'allocation maximum')
        available = self.total_bytes - sum(g.plan.bytes for g in self._grants.values())
        available = available if maximum is None else min(available, maximum)
        # Cost is monotone: fixed continuation storage, item-sized slots and
        # existing IPC control-frame reserves. No new request-count ceiling.
        lower, upper, best = 1, available // (shape.item_input_bytes + shape.item_result_bytes), None
        while lower <= upper:
            count = (lower + upper) // 2
            candidate = _plan(shape, count)
            if candidate.bytes <= available:
                best, lower = candidate, count + 1
            else:
                upper = count - 1
        if best is None:
            raise ValueError('service payload bytes cannot hold a complete ordinary async allocation')
        return best

    def preview(self, shape, *, max_bytes=None):
        self._process()
        with self._lock:
            return self._select(shape, max_bytes)

    def allocate(self, shape, *, max_bytes=None):
        self._process()
        with self._lock:
            grant = RecordsPayloadGrant(self, self._select(shape, max_bytes))
            self._grants[grant.allocation_id] = grant
            return grant

    def snapshot(self):
        self._process()
        with self._lock:
            used = sum(g.plan.bytes for g in self._grants.values())
            return dict(total_bytes=self.total_bytes, allocated_bytes=used,
                available_bytes=self.total_bytes - used, allocations=len(self._grants),
                status=('pending' if self._grants else 'closed') if self._closing else 'open',
                grants=[g.describe() for g in self._grants.values()])

    @staticmethod
    def _runtime_observation(owner):
        """Observe an already-connected runtime without importing or starting Ray."""
        import sys

        ray = owner._actors.ray
        try:
            if (ray is None or sys.modules.get('ray') is not ray
                    or not callable(getattr(ray, 'is_initialized', None))
                    or ray.is_initialized() is not True
                    or not callable(getattr(ray, 'get_runtime_context', None))):
                return None
            context = ray.get_runtime_context()
            identity = dict(job_id=context.get_job_id(), node_id=context.get_node_id())
            if any(type(value) is not str or not value.strip() for value in identity.values()):
                return None
            return ray, identity
        except Exception:
            return None

    def _recovery_owner(self, allocation_id):
        self._process()
        if type(allocation_id) is not str or not allocation_id:
            raise ValueError('recovery requires a registered allocation ID')
        with self._lock:
            grant = self._grants.get(allocation_id)
            if grant is None:
                raise ValueError('unregistered service payload allocation')
            self._registered(grant)
            owner = grant._owner
            if owner is None:
                raise RuntimeError('unbound allocation requires its original grant cancel operation')
            owner._control()
            return grant, owner

    def retry_cleanup(self, allocation_id, *, timeout=None):
        """Retry the original owner's close; retained results are not transferred."""
        import math

        grant, owner = self._recovery_owner(allocation_id)
        if timeout is not None:
            try:
                valid = type(timeout) in (int, float) and math.isfinite(timeout) and timeout > 0
            except OverflowError:
                valid = False
            if not valid:
                raise ValueError('cleanup timeout must be finite and positive')
        actors = owner._actors
        if (actors.ray is not None or actors.model_actor is not None
                or actors.prepare_actors or actors.model_ready is not None):
            observed = self._runtime_observation(owner)
            if (grant._runtime_identity is None or observed is None
                    or observed[0] is not grant._runtime_module
                    or observed[1] != grant._runtime_identity):
                raise RuntimeError('original Ray runtime identity is unavailable or changed; recovery cannot reconnect it')
        return owner.close(timeout=timeout)

    def take_unconsumed_results(self, allocation_id):
        """Explicitly transfer the original owner's local results to the caller."""
        _, owner = self._recovery_owner(allocation_id)
        return owner.take_unconsumed_results()

    def close(self):
        self._process()
        with self._lock:
            self._closing = True
            return self.snapshot()

    def _registered(self, grant):
        self._process()
        if self._grants.get(grant.allocation_id) is not grant:
            raise ValueError('unregistered service payload grant')


class RecordsPayloadGrant:
    """A single actual model owner retains this allocation until safe return."""

    def __init__(self, pool, plan):
        self._pool, self._plan = pool, plan
        self._allocation_id = uuid.uuid4().hex
        self._owner = self._released_owner = None
        self._released = False
        self._identity = None
        self._runtime_module = self._runtime_identity = None

    def __getstate__(self):
        raise TypeError('payload pool and grant capabilities cannot be serialized or copied')

    @property
    def plan(self):
        return self._plan

    @property
    def allocation_id(self):
        return self._allocation_id

    def describe(self):
        self._pool._process()
        with self._pool._lock:
            return dict(allocation_id=self.allocation_id, bytes=self.plan.bytes,
                plan_signature=self.plan.signature,
                status='released' if self._released else 'bound' if self._owner is not None else 'allocated',
                model_identity=None if self._identity is None else dict(self._identity))

    def cancel(self):
        """Return only an allocation which has never adopted a model owner."""
        self._pool._process()
        with self._pool._lock:
            if self._released:
                return False
            self._pool._registered(self)
            if self._owner is not None:
                raise RuntimeError('bound payload grant requires its actual owner cleanup')
            del self._pool._grants[self.allocation_id]
            self._released = True
            return True

    def bind(self, owner):
        from .lotus_records_core import SemLoomRecordsCore

        self._pool._process()
        owner._control()
        with self._pool._lock:
            self._pool._registered(self)
            if (type(owner) is not SemLoomRecordsCore or self._owner is not None
                    or getattr(owner, '_service_allocation', None) is not None
                    or owner._actors.model_actor is not None or owner.model_ready is not None
                    or owner.capacity != self.plan.capacity):
                raise ValueError('payload grant requires one matching unstarted ordinary owner')
            actual, planned = asdict(owner._actors.limits), asdict(self.plan.ipc_limits)
            actual.pop('feedback_interval_s')
            planned.pop('feedback_interval_s')
            if actual != planned or owner.resources.max_active_batches != self.plan.capacity.budget.active_requests:
                raise ValueError('owner limits differ from its service payload allocation')
            self._owner = owner
            owner._service_allocation = self

    def observe_ready(self, owner):
        self._pool._process()
        owner._control()
        with self._pool._lock:
            self._pool._registered(self)
            if owner is not self._owner:
                raise ValueError('payload grant belongs to another model owner')
            ready = owner.model_ready
            names = ('engine_uid', 'producer_owner_uid')
            if type(ready) is not dict or any(type(ready.get(n)) is not str or not ready[n] for n in names):
                raise ValueError('payload allocation needs actual model and producer ready identities')
            identity = {n: ready[n] for n in names}
            if self._identity is not None and self._identity != identity:
                raise ValueError('payload allocation cannot replace its actual model or producer identity')
            observed = self._pool._runtime_observation(owner)
            if self._identity is None:
                if observed is not None:
                    self._runtime_module, self._runtime_identity = observed
            elif self._runtime_identity is not None and (observed is None
                    or observed[0] is not self._runtime_module or observed[1] != self._runtime_identity):
                raise ValueError('payload allocation cannot replace its original Ray runtime identity')
            self._identity = identity

    def settle(self, owner):
        """Use the actual owner's existing close proof; unknown never returns bytes."""
        self._pool._process()
        owner._control()
        with self._pool._lock:
            if self._released:
                if self._released_owner is None or self._released_owner() is not owner:
                    raise ValueError('payload grant belongs to another model owner')
                return True
            self._pool._registered(self)
            if owner is not self._owner:
                raise ValueError('payload grant belongs to another model owner')
            report = owner.last_close_report or {}
            model = report.get('model_owner') or {}
            usage = None if owner.core is None else asdict(owner.core.capacity.usage())
            rpc = owner._actors._data_rpc_progress()
            feedback = owner._feedback
            if feedback is not None:
                snapshot_report = getattr(feedback, 'snapshot_rpc_report', None)
                if not callable(snapshot_report):
                    return False
                try:
                    if snapshot_report().get('snapshot_rpc_terminal_evidence_complete') is not True:
                        return False
                except Exception:
                    return False
            closed_identity = model.get('model_ready') or {}
            if (not owner._closing or report.get('status') != 'closed'
                    or model.get('status') != 'closed'
                    or model.get('data_rpc_pending') != 0 or model.get('data_rpc_unconfirmed') != 0
                    or rpc.pending or rpc.unconfirmed
                    or report.get('transport_closed') is not True or report.get('retained_remote_inputs') != 0
                    or owner.transport is not None and owner.transport.retained_refs()
                    or report.get('feedback_sampler_stopped') is not True
                    or self._identity is not None and any(closed_identity.get(n) != value
                        for n, value in self._identity.items())
                    or usage is not None and any(usage.values())
                    or owner._active is not None and owner._active.unconsumed_call_results):
                return False
            del self._pool._grants[self.allocation_id]
            self._released, self._released_owner, self._owner = True, weakref.ref(owner), None
            return True


def create_service_records_core(method, engine, generation, resources, pool, maximum, **options):
    """Bind one static service allocation before the existing factory can start work."""
    from src.semantic_methods.lotus.records import RecordsResources
    from .lotus_records_core import create_records_core

    if type(pool) is not RecordsPayloadPool:
        raise TypeError('service_budget must be a process-local RecordsPayloadPool')
    resources = (RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=None)
                 if resources is None else resources)
    if (type(resources) is not RecordsResources or resources.max_active_batches is not None
            or resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1):
        raise ValueError('service allocation requires ordinary resources with max_active_batches=None')
    if (options['async_limits'] is not None or options['prepared_batching'] is not None
            or options['batch_concurrency'] is not None or options['batch_feedback'] is not None
            or options['rolling_refill'] is not False
            or options['experimental_skip_token_packing'] is not False
            or options['interleaved_preparation'] is not False
            or options['event_driven_preparation'] is not False):
        raise ValueError('service payload grants currently support ordinary async requests without IPC overrides')
    profile = options['profile']
    if profile is not None and type(profile) is not dict:
        raise ValueError('service payload grants require an explicit ordinary async profile')
    profile = {} if profile is None else dict(profile)
    profile.setdefault('mode', 'async')
    if profile.get('mode') != 'async':
        raise ValueError('service payload grants require async execution')
    if profile.get('admission') is None and 'max_inflight' not in profile:
        profile['max_inflight'] = 'auto'
    if (type(profile.get('admission')) is dict
            and profile['admission'].get('kind') == 'legacy_resource_feedback'):
        raise ValueError('historical feedback profiles retain their own explicit resource declarations')
    shape = CoreRecordsCapacity() if options['capacity'] is None else options['capacity']
    grant = pool.allocate(shape, max_bytes=maximum)
    try:
        options.update(profile=profile, capacity=grant.plan.capacity, async_limits=grant.plan.ipc_limits)
        # The existing factory only constructs local owners; model allocation
        # is deferred to start/evaluate after the grant has adopted that owner.
        owner = create_records_core(method, engine, generation, resources, **options)
        grant.bind(owner)
    except BaseException:
        grant.cancel()
        raise
    owner.execution_profile.update(service_payload_plan=grant.plan.describe(),
        resource_budget_source='exclusive service-process declared payload allocation; not measured RAM or model capacity')
    return owner
