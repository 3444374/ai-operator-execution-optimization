"""Native records feedback sampling, independent of query and window policy.

The existing owner supplies one model actor. Snapshot RPC retention, cancellation,
source qualification and audit transfer remain on the same retained state.
"""
from concurrent.futures import CancelledError
import math
from threading import Event, Lock
import time
import weakref

from .records_feedback_observation import (
    audit_integer, qualify_resource_observation, resource_probe_view,
    snapshot_response_header,
)


class RecordsNativeSampler:
    """One retained snapshot RPC and its source/receipt state, without decisions."""

    def __init__(self, owner):
        self.owner = owner
        self.provider = self.ref = self.future = None
        self.session_id = None
        self.last_native = None
        self.resource_samples = []
        self.resource_audit = []
        self.resource_rpc_audit = []
        self.resource_rpc_counts = dict(total=0, native_eligible=0, actor_unbound=0,
            session_mismatch=0, native_missing=0, CPU_qualified=0, CPU_rejected=0, evicted=0, errors=0)
        self.requests = self.errors = 0
        self.closed = False
        self.snapshot_rpc_receipts = []
        self._snapshot_request = None
        self._snapshot_lock = Lock()
        self._snapshot_ready = None
        self._collected_ready = None

    def _reset_sampling_phase(self, session_id):
        with self._snapshot_lock:
            self.session_id = session_id
            self.last_native = None
            self.resource_samples = []
            self.resource_rpc_audit = []
            self.resource_rpc_counts = dict.fromkeys(self.resource_rpc_counts, 0)
            self.started = time.monotonic()

    def _start_sampling(self, expected_count, provider_type):
        if self.provider is None and expected_count:
            self._snapshot_ready = Event()
            self.provider = provider_type(self.sample,
                poll_interval_s=self.options.poll_interval_s,
                stale_after_s=self._sample_age_limit(),
                close_timeout_s=self.options.poll_interval_s,
                ready_event=self._snapshot_ready, collect_ready=self.collect_ready)
        elif self.provider is not None:
            self.provider.resume()

    def _close_sampling(self):
        with self._snapshot_lock:
            self.closed = True
        if self.provider is not None:
            self.provider.close()
        self._reap_stopped_snapshot(cancel=True)

    def _sample_age_limit(self):
        return self.options.window_s

    def _source_observed_at(self, native, packet, issued_at_s, received_at_s):
        return native.get('observed_at_s', -1)

    def _source_clock_domain(self):
        return 'native-engine NativeStats.clock seconds'

    @staticmethod
    def _resource_view(packet, *, request_index, requested_at_s, received_at_s):
        return resource_probe_view(packet, request_index=request_index,
            requested_at_s=requested_at_s, received_at_s=received_at_s)

    def _read_resource(self, packet, *, request_index, requested_at_s, received_at_s):
        view = self._resource_view(packet, request_index=request_index,
            requested_at_s=requested_at_s, received_at_s=received_at_s)
        return qualify_resource_observation(packet, view, window_s=self._sample_age_limit())

    @staticmethod
    def _audit_integer(value, *, minimum=0):
        return audit_integer(value, minimum=minimum)

    def _audit_resource_rpc(self, row, *, native, disposition, reason, request_index,
                            request_session_id, response_session_id, requested_at_s, received_at_s):
        # Only fixed metadata fields leave this boundary; never retain the RPC
        # payload, arbitrary metric dictionaries, prompt or model output.
        source = row['host_cpu_source']
        cpu_reason = row['host_cpu_reject_reason']
        allowed_reasons = {'cpu_interval_missing_or_invalid', 'cpu_interval_nonpositive',
            'cpu_interval_exceeds_feedback_window', 'cpu_value_unavailable',
            'cpu_percent_baseline_priming', 'cpu_counter_clock_invalid',
            'cpu_percent_nonpositive_interval', 'cpu_counter_shape_changed',
            'cpu_counter_regressed', 'cpu_counter_delta_invalid'}
        if cpu_reason is not None and cpu_reason not in allowed_reasons:
            cpu_reason = 'cpu_source_unavailable'
        invalid = []
        def integer(name, value, *, minimum=0):
            bounded = self._audit_integer(value, minimum=minimum)
            if value is not None and bounded is None:
                invalid.append(name)
            return bounded
        def clock(name, value, *, signed=False):
            if type(value) is int:
                return integer(name, value, minimum=-(1 << 64)+1 if signed else 0)
            if type(value) is float and math.isfinite(value) and (signed or value >= 0):
                return value
            if value is not None:
                invalid.append(name)
            return None
        resource_uid = row['view']['engine_uid']
        if not (type(resource_uid) is str and len(resource_uid) == 32
                and all(c in '0123456789abcdef' for c in resource_uid)):
            if resource_uid is not None:
                invalid.append('resource_engine_uid')
            resource_uid = None
        cpu_interval = clock('host_cpu_raw_interval_s', row['host_cpu_raw_interval_s'], signed=True)
        if cpu_interval is None and cpu_reason is None:
            cpu_reason = 'cpu_interval_missing_or_invalid'
        with self._snapshot_lock:
            attempt = dict(request_index=integer('request_index', request_index),
                request_session_id=integer('request_session_id', request_session_id, minimum=-1),
                current_session_id=integer('current_session_id', self.session_id, minimum=-1),
                response_session_id=integer('response_session_id', response_session_id, minimum=-1),
                requested_at_s=clock('requested_at_s', requested_at_s),
                received_at_s=clock('received_at_s', received_at_s),
                request_clock_domain='driver.time.monotonic seconds',
                resource_counter_clock_domain='model actor time.monotonic_ns',
                native_clock_domain='native-engine NativeStats.clock seconds',
                frame_disposition=disposition, native_reason=reason,
                actor_unbound=response_session_id == -1,
                native_frame_present=native is not None,
                native_validation_accepted=disposition == 'eligible_for_provider',
                resource_engine_uid=resource_uid,
                resource_session_id=integer('resource_session_id', row['view']['session_id'], minimum=-1),
                host_cpu_raw_interval_s=cpu_interval,
                host_cpu_qualified=cpu_reason is None,
                host_cpu_reject_reason=cpu_reason,
                host_cpu_counter_sequence=integer('host_cpu_counter_sequence', source.get('cpu_counter_sample_sequence')),
                host_cpu_read_start_ns=integer('host_cpu_read_start_ns', source.get('cpu_counter_read_start_monotonic_ns')),
                host_cpu_read_end_ns=integer('host_cpu_read_end_ns', source.get('cpu_counter_read_end_monotonic_ns')),
                audit_only=True, invalid_fields=tuple(invalid))
            counts = self.resource_rpc_counts
            counts['total'] += 1
            counts['native_eligible'] += attempt['native_validation_accepted']
            counts['actor_unbound'] += attempt['actor_unbound']
            counts['session_mismatch'] += disposition == 'stale_session'
            counts['native_missing'] += native is None
            counts['CPU_qualified' if attempt['host_cpu_qualified'] else 'CPU_rejected'] += 1
            counts['evicted'] += len(self.resource_rpc_audit) == 64
            self.resource_rpc_audit = (self.resource_rpc_audit + [attempt])[-64:]
        return attempt

    def sample(self):
        if self.closed or self.session_id is None:
            return None
        with self._snapshot_lock:
            collected, self._collected_ready = self._collected_ready, None
        if collected is not None:
            session, snapshot = collected
            return snapshot if session == self.session_id else None
        try:
            if self.ref is None and self._snapshot_request is None:
                remote = self.owner._actors.model_actor.snapshot.remote
                ready = getattr(self.owner._actors, 'model_ready', None) or {}
                uid = ready.get('engine_uid')
                issued_at, clock_error = self._snapshot_clock()
                with self._snapshot_lock:
                    self.sample_session = self.session_id
                    self.requests += 1
                    self.errors += int(clock_error is not None)
                    self._snapshot_request = dict(request_index=self.requests,
                        request_session_id=self.sample_session, issued_at_s=issued_at,
                        issued_at_error_type=clock_error,
                        expected_engine_uid=uid if type(uid) is str else None,
                        cancel_requested=False, cancel_reason=None, cancel_error_type=None,
                        future_access_error_type=None, dispatch_error_type=None, poll_error_type=None,
                        client_future_cancelled=False)
                ref = remote(observe_resources=True) if self._observes_resources() else remote()
                with self._snapshot_lock:
                    self.ref = ref
                self.expiry = time.monotonic()+self._sample_age_limit()
                self.expired = False
            if self.ref is None:
                return None  # Dispatch outcome without a handle remains unknown.
            acquired = None
            if self.future is None:
                future = self.ref.future()
                with self._snapshot_lock:
                    self.future = future
                acquired = future
            snapshot = self._consume_snapshot(cancel_pending=True)
            if acquired is not None and self.future is acquired:
                self._watch_snapshot_ready(acquired)
            return snapshot
        except Exception as error:
            with self._snapshot_lock:
                self.errors += 1
                if self._snapshot_request is not None:
                    field = 'dispatch_error_type' if self.ref is None else 'future_access_error_type'
                    self._snapshot_request = dict(self._snapshot_request, **{field:type(error).__name__})
            return None

    def _watch_snapshot_ready(self, future):
        if self._snapshot_ready is None:
            return
        index = self._snapshot_request['request_index']
        owner = weakref.ref(self)
        def notify(completed):
            feedback = owner()
            if feedback is not None:
                feedback._snapshot_ready.set()
        try:
            future.add_done_callback(notify)
        except Exception as error:
            self._note_future_timing(index, future_wakeup_error_type=type(error).__name__)

    def collect_ready(self):
        # Only the provider thread consumes. A notification never issues a new
        # RPC, and local Future cancellation still retains the original ref.
        if self.closed or self.future is None:
            return None
        session = self.sample_session
        snapshot = self._consume_snapshot()
        with self._snapshot_lock:
            if self.ref is not None:
                return None
            # The next regular tick spends the turn that used to collect this
            # result. Publishing early therefore does not increase RPC cadence.
            self._collected_ready = (session, snapshot)
        return snapshot

    def _cancel_snapshot(self, reason):
        with self._snapshot_lock:
            ref = self.ref
            if ref is None or self._snapshot_request['cancel_requested']:
                return
            self._snapshot_request = dict(self._snapshot_request, cancel_requested=True, cancel_reason=reason)
        try:
            self.owner._actors.ray.cancel(ref, force=False)
        except Exception as error:
            with self._snapshot_lock:
                self.errors += 1
                self._snapshot_request = dict(self._snapshot_request, cancel_error_type=type(error).__name__)

    @staticmethod
    def _snapshot_clock():
        # Receipt timestamps are optional evidence, not a prerequisite for
        # collecting a known RPC result or for the owner's required cleanup.
        try:
            observed = time.monotonic()
            if type(observed) not in (int, float) or not math.isfinite(observed):
                raise ValueError('receipt clock is not finite')
            return observed, None
        except Exception as error:
            return None, type(error).__name__

    def _note_future_timing(self, index, **values):
        with self._snapshot_lock:
            request = self._snapshot_request
            if request is not None and request['request_index'] == index:
                self._snapshot_request = dict(request, **values)

    def _observe_future_ready(self, done):
        # Observation only: no wakeup, extra RPC, result read or ownership release.
        # A callback observes local readiness; it is not remote terminal proof.
        try:
            enabled = callable(getattr(self.provider, 'observer', None))
        except Exception:
            return
        if not enabled:
            return
        polled, poll_error = self._snapshot_clock()
        with self._snapshot_lock:
            request = self._snapshot_request
            if request is None:
                return
            request = dict(request, future_poll_observed_at_s=polled,
                future_poll_clock_error_type=poll_error, future_polled_done=done,
                future_poll_count=request.get('future_poll_count', 0)+1)
            if not done:
                request['future_last_pending_poll_observed_at_s'] = polled
            self._snapshot_request = request
            if request.get('future_timing_enabled'):
                return
            index, future = request['request_index'], self.future
            self._snapshot_request = dict(request, future_timing_enabled=True,
                future_timing_clock='driver.time.monotonic seconds',
                future_ready_observed_at_s=None, future_ready_clock_error_type=None,
                future_observer_error_type=None)
        registered, clock_error = self._snapshot_clock()
        try:
            self._note_future_timing(index, future_registered_at_s=registered,
                future_registration_clock_error_type=clock_error,
                future_done_before_registration=done)
            owner = weakref.ref(self)
            def notified(completed):
                feedback = owner()
                if feedback is not None:
                    observed, error = feedback._snapshot_clock()
                    feedback._note_future_timing(index, future_ready_observed_at_s=observed,
                        future_ready_clock_error_type=error)
            future.add_done_callback(notified)
        except Exception as error:
            self._note_future_timing(index, future_observer_error_type=type(error).__name__)

    @staticmethod
    def _snapshot_header(value):
        return snapshot_response_header(value)

    def _finish_snapshot(self, status, disposition, *, value=None, error=None):
        observed_at, clock_error = self._snapshot_clock()
        header = self._snapshot_header(value)
        with self._snapshot_lock:
            self.errors += int(clock_error is not None)+int(header['response_source_observed_at_error_type'] is not None)
            self.snapshot_rpc_receipts.append(dict(self._snapshot_request,
                rpc_terminal_status=status, future_done_confirmed=True,
                terminal_observed_at_s=observed_at, terminal_observed_at_error_type=clock_error,
                rpc_error_type=error, frame_disposition=disposition, **header))
            self.ref = self.future = self._snapshot_request = None

    def _consume_snapshot(self, *, cancel_pending=False):
        from src.scheduling.runtime.observations import ServiceMetricsSnapshot
        try:
            done = self.future.done()
        except Exception as error:
            with self._snapshot_lock:
                self.errors += 1
                self._snapshot_request = dict(self._snapshot_request, poll_error_type=type(error).__name__)
            return None
        self._observe_future_ready(done)
        if not done:
            if cancel_pending and self.closed:
                self._cancel_snapshot('close')
            elif cancel_pending and not self.expired and time.monotonic() >= self.expiry:
                self.expired = True
                self._cancel_snapshot('expired')
            # Cancel requested/failed is not terminal proof. The same ref owns
            # this slot; duplicate native sequences keep their original age.
            with self._snapshot_lock:
                native = self.last_native
                return (native if not self.closed and native is not None
                        and native.source_session_id == self.session_id else None)
        with self._snapshot_lock:
            index = self._snapshot_request['request_index']
            request_session_id = self._snapshot_request['request_session_id']
            timing = self._snapshot_request.get('future_timing_enabled', False)
            issued_at_s = self._snapshot_request.get('issued_at_s')
        if timing:
            observed, error = self._snapshot_clock()
            self._note_future_timing(index, future_collect_started_at_s=observed,
                future_collect_clock_error_type=error)
        try:
            value = self.future.result()
        except CancelledError:
            # A cancelled local read Future is not the ObjectRef's remote
            # return/exception. Keep the same RPC ownership explicitly unknown.
            with self._snapshot_lock:
                self._snapshot_request = dict(self._snapshot_request, client_future_cancelled=True)
            if timing:
                observed, error = self._snapshot_clock()
                self._note_future_timing(index, future_result_observed_at_s=observed,
                    future_result_clock_error_type=error)
            return None
        except Exception as error:
            if timing:
                observed, clock_error = self._snapshot_clock()
                self._note_future_timing(index, future_result_observed_at_s=observed,
                    future_result_clock_error_type=clock_error)
            if not self.closed:
                self.errors += 1
            self._finish_snapshot('raised', 'rpc_error', error=type(error).__name__)
            return None
        received_at_s, received_clock_error = self._snapshot_clock()
        if timing:
            self._note_future_timing(index, future_result_observed_at_s=received_at_s,
                future_result_clock_error_type=received_clock_error)
        snapshot, disposition, reason = None, 'invalid_frame', 'invalid_native_frame'
        native, response_session = None, None
        try:
            if isinstance(value, dict):
                response_session = value.get('session_id')
                ipc = value.get('ipc')
                native = ipc.get('native_feedback') if isinstance(ipc, dict) else None
            if self.closed:
                disposition, reason = 'closed', 'feedback_closed'
            elif self.expired:
                disposition, reason = 'expired', 'expired_RPC'
            elif self.sample_session != self.session_id or value.get('session_id') != self.session_id:
                disposition = 'stale_session'
                reason = ('sampling_session_changed' if self.sample_session != self.session_id
                          else 'session_mismatch')
            elif native is None:
                disposition, reason = 'missing_native', 'native_frame_missing'
            elif native.get('engine_uid') != self.owner._actors.model_ready['engine_uid']:
                disposition, reason = 'wrong_engine', 'engine_uid_mismatch'
            else:
                source_at = self._source_observed_at(native, value, issued_at_s, received_at_s)
                if source_at < self.started:
                    disposition, reason = 'stale_source', 'source_predates_query'
                else:
                    snapshot = ServiceMetricsSnapshot(native['running'], native['waiting'], native['kv_usage'],
                    source_uid=native['engine_uid'], source_session_id=response_session,
                    received_at_s=received_at_s, control_request_index=index,
                    source_clock_domain=self._source_clock_domain(),
                    received_clock_domain='driver.time.monotonic seconds',
                    completed_tokens_total=native['generation_tokens_total'],
                    preemptions_total=native['preemptions_total'], source_sequence=native['sequence'],
                    source_observed_at_s=source_at)
                    disposition, reason = 'eligible_for_provider', 'RPC_sample_valid'
        except Exception:
            self.errors += 1
            disposition, reason = 'invalid_frame', 'invalid_native_frame'
        self._finish_snapshot('returned', disposition, value=value)
        resource_attempt = resource_read = None
        if self._observes_resources():
            try:
                resource_read = self._read_resource(value.get('resources') if isinstance(value, dict) else None,
                    request_index=index, requested_at_s=issued_at_s, received_at_s=received_at_s)
                resource_attempt = self._audit_resource_rpc(resource_read[1], native=native,
                    disposition=disposition, reason=reason, request_index=index,
                    request_session_id=request_session_id,
                    response_session_id=response_session if type(response_session) is int else None,
                    requested_at_s=issued_at_s, received_at_s=received_at_s)
            except Exception:
                with self._snapshot_lock:
                    self.resource_rpc_counts['total'] += 1
                    self.resource_rpc_counts['errors'] += 1
                # Diagnostic failure cannot change the known RPC or native eligibility.
        try:
            audit = getattr(self.provider, 'observe_native', None)
            if callable(audit):
                audit(native, session_id=response_session, received_at_s=received_at_s,
                    accepted=snapshot is not None, reason=reason, control_request_index=index,
                    source_clock_domain='native-engine NativeStats.clock seconds',
                    received_clock_domain='driver.time.monotonic seconds',
                    **(dict(resource_attempt=dict(resource_attempt)) if resource_attempt is not None else {}))
        except Exception:
            pass  # Optional audit cannot change the RPC receipt or valid feedback.
        if snapshot is not None:
            with self._snapshot_lock:
                # Audit runs outside the ownership lock and may span bind().
                # A known old RPC return must not seed the next Core session.
                if self.closed or snapshot.source_session_id != self.session_id:
                    return None
                self.last_native = snapshot
                if self._observes_resources():
                    view, resource = resource_read or self._read_resource(value.get('resources'), request_index=index,
                        requested_at_s=issued_at_s, received_at_s=received_at_s)
                    self.resource_samples = (self.resource_samples + [view])[-2:]
                    self.resource_audit = (self.resource_audit + [resource])[-64:]
        return snapshot

    def _reap_stopped_snapshot(self, *, cancel=False):
        # The existing provider is the single writer while sampling. Never
        # race it or wait for RPC completion on the query/close thread.
        if self.provider is not None and self.provider.is_running:
            return
        if self.future is not None:
            self._consume_snapshot()
        if cancel and self.ref is not None:
            self._cancel_snapshot('close')
            if self.future is not None:
                self._consume_snapshot()  # Collect an immediately terminal cancel.
