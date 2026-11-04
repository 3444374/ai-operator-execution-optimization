"""Exclusive process-local grants for ordinary and rolling async payloads.

The service supplies the total. This allocator neither discovers RAM nor owns
request admission. Core and IPC continue enforcing their existing reservations.
Rolling delivery arrays have independent grants. Whole-query source/expected
relations, Python/Ray overhead, caller audit copies and model/KV are not covered.
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
from .records_runtime_identity import observe_ray_runtime

if TYPE_CHECKING:
    from src.serving.backends.vllm_async_engine import AsyncEngineLimits
    from .records_service_delivery import OrdinaryDeliveryDeclaration, OrdinaryDeliverySession


def _positive_bytes(value, name):
    if type(value) is not int or value <= 0:
        raise ValueError(name + ' must be positive integer payload bytes')
    return value


@dataclass(frozen=True)
class RecordsPayloadPlan:
    capacity: CoreRecordsCapacity
    ipc_limits: AsyncEngineLimits
    components: tuple[tuple[str, int], ...]
    delivery: OrdinaryDeliveryDeclaration | OrdinaryDeliverySession | None = None

    @property
    def bytes(self):
        return sum(size for _, size in self.components)

    def describe(self):
        value = dict(capacity=asdict(self.capacity), ipc_limits=asdict(self.ipc_limits),
                    components=dict(self.components), bytes=self.bytes,
                    scope='ordinary async declared payload envelopes; not RAM or GPU capacity')
        if self.delivery is not None:
            value.update(delivery=self.delivery.describe(),
                scope='rolling ordinary Filter instance and complete unconsumed delivery envelopes; excludes full source/expected relations, RSS and GPU capacity')
        return value

    @property
    def signature(self):
        return hashlib.sha256(json.dumps(self.describe(), sort_keys=True,
            separators=(',', ':')).encode()).hexdigest()


def _plan(shape, requests, delivery=None):
    from src.semantic_methods.budget import row_reservation
    from src.semantic_methods.lotus.records import RecordsResources

    capacity = shape.with_storage_bytes(input_bytes=requests * shape.item_input_bytes,
        result_bytes=requests * shape.item_result_bytes,
        method_bytes=(requests if delivery is not None else 1)*row_reservation(shape.method_limits(1)))
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
    if delivery is not None:
        # Each name is a concrete retained or API-workspace role, not a request
        # policy multiplier. Receipt storage remains independent of Core R.
        roles = ('raw_receipts', 'parsed_calls', 'adoption_calls', 'failure_raw_results',
                 'error_calls', 'chain_close_calls',
                 'cached_stream_report', 'ordinary_projection', 'merged_getter_work',
                 'consumer_snapshot', 'transfer_work')
        parts.extend(('delivery_'+role, delivery.array_bytes) for role in roles)
        declarations = delivery.deliveries if hasattr(delivery,'deliveries') else (delivery,)
        item = max((max(v.item_upper_bounds,default=0) for v in declarations),default=0)
        parts.extend((('delivery_codec_text',item),('delivery_codec_bytes',item)))
    return RecordsPayloadPlan(capacity, limits, tuple(parts), delivery)


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

    def _select(self, shape, maximum, delivery=None):
        self._process()
        if self._closing:
            raise RuntimeError('payload service is closing')
        if type(shape) is not CoreRecordsCapacity or shape.budget is not None:
            raise ValueError('payload allocation requires a per-item shape without an existing budget')
        if maximum is not None:
            _positive_bytes(maximum, 'allocation maximum')
        if delivery is not None:
            from .records_service_delivery import OrdinaryDeliveryDeclaration, OrdinaryDeliverySession
            if type(delivery) not in (OrdinaryDeliveryDeclaration,OrdinaryDeliverySession):
                raise ValueError('rolling delivery requires actual immutable ordinary query declarations')
            declarations = delivery.deliveries if type(delivery) is OrdinaryDeliverySession else (delivery,)
            if any(v.item_result_bytes != shape.item_result_bytes for v in declarations):
                raise ValueError('delivery model result bytes differ from the actual instance shape')
        available = self.total_bytes - sum(g.plan.bytes for g in self._grants.values())
        available = available if maximum is None else min(available, maximum)
        # Cost is monotone: fixed continuation storage, item-sized slots and
        # existing IPC control-frame reserves. No new request-count ceiling.
        lower, upper, best = 1, available // (shape.item_input_bytes + shape.item_result_bytes), None
        while lower <= upper:
            count = (lower + upper) // 2
            candidate = _plan(shape, count, delivery)
            if candidate.bytes <= available:
                best, lower = candidate, count + 1
            else:
                upper = count - 1
        if best is None:
            raise ValueError('service payload bytes cannot hold a complete ordinary async allocation')
        return best

    def preview(self, shape, *, max_bytes=None, delivery=None):
        self._process()
        with self._lock:
            return self._select(shape, max_bytes, delivery)

    def allocate(self, shape, *, max_bytes=None, delivery=None):
        self._process()
        with self._lock:
            grant = RecordsPayloadGrant(self, self._select(shape, max_bytes, delivery))
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
    def _producer(owner):
        from .lotus_records_core import SemLoomRecordsCore
        from .lotus_records_batches import PreparedRecordsCore
        if type(owner) is SemLoomRecordsCore:
            return owner
        if type(owner) is PreparedRecordsCore:
            return owner.owner
        raise ValueError('service payload owner has no supported actual producer')

    @staticmethod
    def _runtime_observation(owner):
        """Observe an already-connected runtime without importing or starting Ray."""
        producer = RecordsPayloadPool._producer(owner)
        return None if producer is None else RecordsPayloadPool._ray_runtime_observation(producer._actors.ray)

    _ray_runtime_observation = staticmethod(observe_ray_runtime)

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
        producer = self._producer(owner)
        if not grant._owns_producer(producer):
            raise RuntimeError('registered service producer was replaced')
        if owner.cleanup_confirmed():
            # Rolling close also transfers its retained full-query arrays. A
            # prior terminal cleanup does not prove that transfer succeeded.
            return owner.close(timeout=timeout) if grant.plan.delivery is not None else owner.last_close_report
        if producer is None:
            return owner.close(timeout=timeout)
        actors = producer._actors
        if (actors.ray is not None or actors.model_actor is not None
                or actors.prepare_actors or actors.model_ready is not None):
            observed = self._runtime_observation(owner)
            original = ((grant._runtime_module, grant._runtime_identity)
                        if grant._runtime_identity is not None else getattr(owner, '_model_runtime', None))
            if (original is None or observed is None
                    or observed[0] is not original[0] or observed[1] != original[1]):
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
        self._producer = None
        self._retirement = self._retirement_values = None
        self._retiring_producer_ref = None

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
        from .lotus_records_batches import PreparedRecordsCore

        self._pool._process()
        owner._control()
        with self._pool._lock:
            self._pool._registered(self)
            rolling = type(owner) is PreparedRecordsCore
            if (type(owner) not in (SemLoomRecordsCore,PreparedRecordsCore) or self._owner is not None
                    or getattr(owner, '_service_allocation', None) is not None
                    or (owner.owner is not None if rolling else owner._actors.model_actor is not None)
                    or owner.model_ready is not None or rolling != (self.plan.delivery is not None)
                    or owner.capacity != self.plan.capacity):
                raise ValueError('payload grant requires one matching unstarted ordinary or rolling owner')
            actual, planned = asdict(owner._limits if rolling else owner._actors.limits), asdict(self.plan.ipc_limits)
            actual.pop('feedback_interval_s')
            planned.pop('feedback_interval_s')
            if actual != planned or owner.resources.max_active_batches != self.plan.capacity.budget.active_requests:
                raise ValueError('owner limits differ from its service payload allocation')
            self._owner = owner
            self._producer = None if rolling else owner
            owner._service_allocation = self

    def adopt_producer(self, owner, producer):
        """Retain the actual constructed chain before checking its unstarted declarations."""
        from .lotus_records_batches import PreparedRecordsCore
        from .lotus_record_chain_core import SemLoomRecordChainCore
        self._pool._process()
        owner._control()
        with self._pool._lock:
            self._pool._registered(self)
            if (owner is not self._owner or type(owner) is not PreparedRecordsCore
                    or type(producer) is not SemLoomRecordChainCore or owner.owner is not producer
                    or self._retirement is not None
                    or self._producer is not None and self._producer is not producer):
                raise ValueError('service allocation cannot adopt a foreign or replacement producer')
            self._producer = producer
            actual, planned = asdict(producer._actors.limits), asdict(self.plan.ipc_limits)
            actual.pop('feedback_interval_s')
            planned.pop('feedback_interval_s')
            if (producer.capacity != self.plan.capacity or producer.resources != owner.resources
                    or producer.engine != owner.engine or producer._actors.generation != owner.generation
                    or actual != planned or producer.execution != 'record_local_sections'
                    or not producer._actors._prepared_only or producer._actors.model_actor is not None
                    or producer._actors.model_ready is not None):
                raise ValueError('actual unstarted rolling producer differs from its service allocation')

    def retire_producer(self, owner, producer):
        """Record terminal producer proof before its query payload references are dropped.

        No bytes return here. The wrapper must transfer its last results and drop
        its chain and active iterator before settle can return the allocation.
        """
        from .lotus_records_batches import PreparedRecordsCore
        from .lotus_record_chain_core import SemLoomRecordChainCore

        self._pool._process()
        owner._control()
        with self._pool._lock:
            self._pool._registered(self)
            if (self.plan.delivery is None or owner is not self._owner
                    or type(owner) is not PreparedRecordsCore
                    or type(producer) is not SemLoomRecordChainCore
                    or owner.owner is not producer or not self._owns_producer(producer)):
                raise ValueError('service producer retirement requires its actual registered wrapper and chain')
            if not owner._closed or not owner.cleanup_confirmed() or owner.has_unconsumed_results():
                raise RuntimeError('live work or untransferred results still retain the service allocation')
            ready = ((producer.last_close_report or {}).get('model_owner') or {}).get('model_ready') or {}
            if self._identity is not None and any(ready.get(n) != value for n,value in self._identity.items()):
                raise RuntimeError('terminal model identity differs from the registered service producer')
            if self._retirement is not None:
                marker = getattr(owner, '_service_retirement', None)
                if not self._retirement_valid() or not (marker is None or marker is self._retirement):
                    raise RuntimeError('the original producer retirement proof changed')
                self._producer = None
                return self._retirement
            certificate = dict(allocation_id=self.allocation_id,
                plan_signature=self.plan.signature, producer_id=hex(id(producer)),
                model_identity=None if self._identity is None else dict(self._identity),
                cleanup_confirmed=True)
            # Keep no chain or N-result array in this proof. Only the registered
            # wrapper can finish transferring its own references after this call.
            self._retirement_values = (self.allocation_id, self.plan.signature,
                certificate['producer_id'], None if self._identity is None else tuple(sorted(self._identity.items())))
            self._retiring_producer_ref = weakref.ref(producer)
            self._retirement = certificate
            self._producer = None
            return certificate

    def _owns_producer(self, producer):
        if producer is self._producer:
            return True
        return (self._producer is None and self._retiring_producer_ref is not None
                and self._retiring_producer_ref() is producer and self._retirement_valid())

    def _retirement_valid(self):
        certificate = self._retirement
        if (type(certificate) is not dict or set(certificate) != {
                    'allocation_id', 'plan_signature', 'producer_id', 'model_identity', 'cleanup_confirmed'}
                or certificate.get('cleanup_confirmed') is not True):
            return False
        identity = certificate.get('model_identity')
        if identity is not None and (type(identity) is not dict
                or any(type(k) is not str or type(v) is not str for k,v in identity.items())):
            return False
        values = (certificate.get('allocation_id'), certificate.get('plan_signature'),
            certificate.get('producer_id'), None if identity is None else tuple(sorted(identity.items())))
        return (all(type(v) is str for v in values[:3])
                and values == self._retirement_values)

    def _retirement_confirmed(self, owner):
        wrapper_done = (self._retirement_valid() and owner._service_retirement is self._retirement
                and owner.owner is None and owner._active is None
                and type(getattr(owner, '_retired_report', None)) is dict
                and owner._retired_report.get('payload_transferred') is True)
        if not wrapper_done or self._retiring_producer_ref is None:
            return False
        producer = self._retiring_producer_ref()
        if producer is None:
            return True
        # A private cycle may outlive the wrapper until Python next runs GC.
        # Its payload arrays must be explicitly retired before bytes can return.
        return producer._service_payloads_retired_confirmed() is True

    def observe_ready(self, owner):
        self._pool._process()
        owner._control()
        with self._pool._lock:
            self._pool._registered(self)
            if owner is not self._owner:
                raise ValueError('payload grant belongs to another model owner')
            if self._pool._producer(owner) is not self._producer:
                raise ValueError('actual service producer was replaced before ready')
            ready = owner.model_ready
            names = ('engine_uid', 'producer_owner_uid')
            if type(ready) is not dict or any(type(ready.get(n)) is not str or not ready[n] for n in names):
                raise ValueError('payload allocation needs actual model and producer ready identities')
            identity = {n: ready[n] for n in names}
            if self._identity is not None and self._identity != identity:
                raise ValueError('payload allocation cannot replace its actual model or producer identity')
            observed = self._pool._runtime_observation(owner)
            if getattr(owner, '_model_runtime_capture_attempted', False):
                original = owner._model_runtime
                if original is None:
                    observed = None
                elif (observed is None or observed[0] is not original[0] or observed[1] != original[1]):
                    raise ValueError('payload allocation changed its startup Ray runtime identity')
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
            if self.plan.delivery is not None:
                producer = self._pool._producer(owner)
                if producer is not self._producer:
                    return False
                try:
                    if not owner._closed or not owner.cleanup_confirmed() or owner.has_unconsumed_results():
                        return False
                    if self._retirement is not None:
                        if not self._retirement_confirmed(owner):
                            return False
                    elif producer is not None or self._identity is not None:
                        # A terminal chain still owns full-query payload arrays.
                        # Its cleanup proof alone cannot refund delivery storage.
                        return False
                except Exception:
                    return False
                del self._pool._grants[self.allocation_id]
                self._released, self._released_owner, self._owner = True, weakref.ref(owner), None
                self._producer = None
                self._retiring_producer_ref = None
                return True
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
            self._producer = None
            return True


def create_service_records_core(method, engine, generation, resources, pool, maximum, **options):
    """Bind one static service allocation before the existing factory can start work."""
    from .lotus_records_core import create_records_core
    from .records_service_config import service_record_options

    if type(pool) is not RecordsPayloadPool:
        raise TypeError('service_budget must be a process-local RecordsPayloadPool')
    resources, profile, shape = service_record_options(resources, options)
    delivery = options.pop('service_delivery',None)
    grant = pool.allocate(shape, max_bytes=maximum, delivery=delivery)
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
        service_payload_bytes=pool.total_bytes, service_allocation_acquired=True,
        resource_budget_source='exclusive service-process declared payload allocation; not measured RAM or model capacity')
    return owner
