"""Connect one reviewed request policy to actual records ownership and sources."""
from dataclasses import asdict, replace
import time

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.core.session import SessionEngine
from src.scheduling.submission_control.adaptive import ResourceRequestConfig
from src.scheduling.submission_control.admission import StaticAdmissionController
from src.scheduling.submission_control.request_scan import ResourceRequestController
from src.scheduling.submission_control.resource_probe import ResourceRequestPermit
from src.scheduling.submission_control.stage_work import SupplyState
from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider

from .lotus_chain_token_batches import BatchSupplyFeedback
from .records_native_sampler import RecordsNativeSampler
from .records_native_source import NativeSourceClock, backend_manages_request_memory
from .records_memory_pressure import MemoryPressureBinding


def applied_request_limit(owner):
    """Existing enforced storage/request declarations, never physical capacity."""
    if not isinstance(owner.core, SessionEngine):
        raise ValueError('request policy requires the applied Core resource owner')
    core, ipc = owner.core.capacity.limits, owner._actors.limits
    return min(owner.resources.max_active_batches, core.active_requests, core.held_tasks,
        core.result_bytes // core.item_result_bytes, ipc.max_inflight, ipc.result_slots,
        ipc.result_bytes // ipc.item_result_bytes)


class _RequestSource:
    """Source conversion and policy input shared by the two existing adapters."""
    def _sample_age_limit(self):
        return self.options.max_sample_age_s

    def _source_clock_domain(self):
        return 'driver.time.monotonic seconds; conservative native source age'

    def _source_observed_at(self, native, packet, issued_at_s, received_at_s):
        return self.native_source_clock.observed_at(native, packet,
            issued_at_s=issued_at_s, received_at_s=received_at_s)

    def _bind_request_policy(self, session_id, total):
        ready = self.owner._actors.model_ready
        if not backend_manages_request_memory(ready):
            raise ValueError('resource request feedback requires the actual backend resource owner ready')
        self.native_source_clock = NativeSourceClock(ready['engine_uid'], session_id)
        self.memory_pressure_binding = (MemoryPressureBinding(ready['engine_uid'], session_id,
            started_at_s=self.started, max_age_s=self.options.max_sample_age_s)
            if self.options.require_memory_feedback else None)
        self._control_source = None
        self.controller = ResourceRequestController(applied_request_limit(self.owner), self.options)
        self.controller.reset(1, total, resource_identity=(ready['engine_uid'], session_id))

    def _latest(self, inflight):
        observation, self._control_source = self.provider.latest_with_source(inflight)
        return observation

    def _observes_resources(self):
        return self.options.allow_exploration or self.options.require_memory_feedback

    def _read_resource(self, packet, **timing):
        binding = getattr(self, 'memory_pressure_binding', None)
        if binding is not None:
            binding.observe(packet, requested_at_s=timing['requested_at_s'], received_at_s=timing['received_at_s'])
        return super()._read_resource(packet, **timing)

    def _memory_report(self):
        binding = getattr(self, 'memory_pressure_binding', None)
        return dict(enabled=False) if binding is None else binding.report()

    def _decide_request(self, observation, supply):
        ready = self.owner._actors.model_ready
        source = self._control_source
        bound = (source is not None and source.source_uid == ready['engine_uid']
                 and source.source_session_id == self.session_id)
        permit = ResourceRequestPermit(ready['engine_uid'], self.session_id,
            observation.sample_sequence, source.source_sequence if bound else None,
            observed_at_s=observation.observed_at_s,
            request_limit=applied_request_limit(self.owner), local_limits_enforced=True,
            backend_owns_memory=backend_manages_request_memory(ready),
            **({} if getattr(self, 'memory_pressure_binding', None) is None else
               self.memory_pressure_binding.permit_fields(ready['engine_uid'], self.session_id)))
        return self.controller.update_supply(observation, supply, resource_permit=permit,
            physical_samples=tuple(self.resource_samples))


class ResourceRequestSupplyFeedback(_RequestSource, BatchSupplyFeedback):
    """Rolling token organization with the same receipts and one request policy."""
    report_field = 'request_supply_feedback'
    request_control = True

    @property
    def options(self):
        return self.config

    def _new_controller(self):
        # bind() supplies the real Core session before any policy action.
        return None

    def _reset_controller(self, stream):
        total = len(stream.run.current)*len(stream._stage_ids)
        if total:
            self._bind_request_policy(stream.session.session_id, total)
        else:
            self.native_source_clock = self.memory_pressure_binding = self._control_source = None
        self._set_window(1)

    def _inflight(self, batches, requests):
        return requests

    def _window(self):
        return self.owner._token_organizer.request_window

    def _set_window(self, value):
        self.owner._token_organizer.request_window = value

    def _decide(self, observation, supply, live, queued, admitted):
        return self._decide_request(observation, supply)

    def _report_identity(self):
        return dict(kind='resource_request_feedback', initial_requests=1,
            max_requests=applied_request_limit(self.owner), cross_query_hints_supported=False,
            memory_feedback=self._memory_report())


class CoreResourceRequestFeedback(_RequestSource, RecordsNativeSampler):
    """Ordinary Core uses its existing gate; observation never owns task credit."""
    uses_computed_work = True

    def __init__(self, owner, admission):
        super().__init__(owner)
        self.base_options = self.options = ResourceRequestConfig(**{
            k: v for k, v in admission.items() if k != 'kind'})
        self.gate = StaticAdmissionController(1)
        self._stream = None
        self.controller = None
        self.transition_keys = set()
        self.submitted = 0
        self._sink_installed = False
        self._total = 0
        self.next_poll = 0.

    @property
    def stream(self):
        return self._stream

    @stream.setter
    def stream(self, stream):
        self._stream = stream
        if stream is None:
            return
        self._reset_sampling_phase(stream.session.session_id)
        if not self._total:
            return
        self._bind_request_policy(stream.session.session_id, self._total)
        if not self._sink_installed:
            previous = self.owner.core.sink
            def event(action, key):
                if previous is not None:
                    previous(action, key)
                if action == 'submitted' and key.session_id == self.session_id:
                    self.submitted += 1
            self.owner.core.sink = event
            self._sink_installed = True
        self._start_sampling(self._total, NonBlockingMetricsObservationProvider)

    def begin(self, total, initial, *, allow_exploration=None):
        if initial != 1:
            raise ValueError('resource requests start at one without cross-query hints')
        self.options = (self.base_options if allow_exploration is None else
                        replace(self.base_options, allow_exploration=allow_exploration))
        self._total, self._stream = total, None
        self.native_source_clock = self.memory_pressure_binding = self._control_source = None
        self.transition_keys.clear()
        self.submitted = 0
        self.controller = None
        self.gate.limit = 1
        self.next_poll = 0.

    def observe(self, stream):
        if self._stream is not stream or self.controller is None:
            return
        now = time.monotonic()
        if self.closed or now < self.next_poll:
            return
        self.next_poll = now + self.options.poll_interval_s
        records = tuple(r for r in self.owner.core.capacity.records.values()
                        if r.key.session_id == self.session_id)
        queued = tuple(r for r in records if r.phase == 'QUEUED')
        admitted = {r.key for r in records if r.compute or r.member is not None}
        self.transition_keys.intersection_update(admitted)
        inflight = self.owner.core.capacity.usage().active_requests
        # Snapshot receipt work before the observation's clock capture. The
        # retained results remain owned by the original transport/consumer.
        work = stream.completed_model_work()
        try:
            observation = self._latest(inflight)
        except ValueError:
            self.errors += 1
            self._control_source = None
            observation = AdmissionObservation(time.monotonic(), False, inflight, None, None, None)
        unread = max(0, len(stream.expected)-len(stream.offered)) if not stream.source_eof else 0
        ready_results = sum(r.phase in ('READY', 'LEASED') for r in records)
        # Native freshness describes its accepted source. Missing receipt work
        # pauses performance scoring without suppressing that source's safety.
        observation = replace(observation,
            pending_items=len(queued)+unread, transition_pending=len(self.transition_keys),
            consumer_backpressure=ready_results >= self.gate.limit, **work)
        usage, limits = self.owner.core.capacity.usage(), self.owner.core.capacity.limits
        blocked = 'member_R' if inflight >= limits.active_requests else None
        if len(admitted) > inflight:
            blocked = 'pending_transport_admission'
        if not queued and blocked is None and stream.pending is not None:
            from src.scheduling.core.session_capacity import SessionCapacity
            blocked = SessionCapacity.storage_block(usage, limits, stream.pending)
        supply = SupplyState(observation.observed_at_s, len(queued), len(stream.offered), self.submitted,
            0, upstream_pending=not stream.source_eof, exhausted=stream.source_eof and not records,
            capacity_blocked=blocked, local_capacity=dict(usage=asdict(usage), limits=asdict(limits)),
            source_exhausted=stream.source_eof, active_requests=inflight)
        decision = self._decide_request(observation, supply)
        if decision.window != self.gate.limit:
            self.transition_keys = admitted
            self.gate.limit = decision.window

    def pause(self):
        if self.provider is not None:
            self.provider.pause()

    def stop(self):
        self._close_sampling()

    def finish(self, stream):
        if self._stream is not stream:
            return getattr(stream, '_admission_report', None)
        self.pause()
        rpc = self.snapshot_rpc_report()
        policy = (dict(strategy='resource_request_v1', initial_window=1, window=1,
                       can_continue=False, empty_query=True, cross_query_hints_supported=False)
                  if self.controller is None else self.controller.report())
        stream._admission_report = dict(policy, **rpc,
            sampling_paused=True,
            source_stale_after_s=self.options.max_sample_age_s,
            source_clock_domain=self._source_clock_domain(), physical_capacity_known=False,
            memory_feedback=self._memory_report())
        self._stream = None
        return stream._admission_report
