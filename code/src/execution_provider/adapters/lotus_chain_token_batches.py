"""Prepared-request organization for the existing Core/async owner, not an executor."""
from dataclasses import asdict
from concurrent.futures import CancelledError
import json
import math
from threading import Event, Lock
import time
import weakref

from src.modalities.text.tokenization import chat_token_count
from src.scheduling.organization.batching import PendingBatchBuilder, RowArrival
from src.serving.backends.async_engine_process import _frame
from src.serving.backends.vllm_engine import IDENTITY_FIELDS, _prepared


class PreparedTokenOrganizer:
    def __init__(self, owner, tokenizer, concurrency):
        self.owner, self.tokenizer, self.concurrency = owner, tokenizer, concurrency
        self._estimates = {}
        self._header_estimate = self._header_context = None
        self.selected = self.peak_batches = self.peak_requests = 0
        self.selected_seal_reason_counts = dict(token_budget=0, request_capacity=0,
            frame_bytes=0, compatibility=0, ready_frontier_exhausted=0)
        self.request_window = owner.resources.max_active_batches
        self.history = []

    def usage(self):
        active = [r for r in self.owner.core.capacity.records.values() if r.compute]
        batches = {r.member.batch_key for r in active if r.member is not None}
        self.peak_batches = max(self.peak_batches, len(batches))
        self.peak_requests = max(self.peak_requests, len(active))
        return len(batches), len(active)

    def _header_sizes(self, session_id, signature, limits):
        context = (session_id, self.owner.deadline, signature, limits.input_bytes)
        if context != self._header_context:
            generation = json.loads(signature)
            # Merging two compact JSON objects adds one comma and removes two
            # braces. Subtract this metadata object's size minus one from the
            # validated submit size to recover the unchanged IPC member size.
            extra = len(_frame(dict(op='submit', generation=generation,
                deadline=self.owner.deadline), limits.input_bytes))-5
            header = len(_frame(dict(op='submit_batch', requests=[], generation=generation,
                deadline=self.owner.deadline), limits.input_bytes))-4
            rpc_header = len(_frame(dict(calls=[], batch_key=[session_id, (1 << 64)-1],
                deadline=self.owner.deadline), limits.input_bytes))-4
            self._header_estimate = extra, header, rpc_header
            self._header_context = context
        return self._header_estimate

    def __call__(self, candidates):
        owner = self.owner
        active_batches, active_requests = self.usage()
        # Rolling mode uses the existing per-request compute and resident grants.
        # Every new batch still needs live, reserved member tasks; B only packs
        # this handoff and a slow peer does not retain the whole supply window.
        if not owner.rolling_refill and active_batches >= self.concurrency:
            return ()
        limits = owner._actors.limits
        available = min(owner.resources.max_active_batches-active_requests, limits.max_inflight,
                        limits.result_slots, limits.result_bytes//limits.item_result_bytes)
        if owner.rolling_refill:
            # Core calls organize only after the prior immutable selection has
            # been accepted/drained. Selection acquires individual permits;
            # a later shrink stops new selections without cancelling old ones.
            available = min(available, self.request_window-active_requests)
        if available <= 0:
            return ()
        live = owner.core.capacity.records
        self._estimates = {k: v for k, v in self._estimates.items() if k in live}
        if not live:
            self._header_estimate = self._header_context = None
        skip_tokens = getattr(owner, 'experimental_skip_token_packing', False)
        builder = PendingBatchBuilder(available if skip_tokens else None,
                                      0 if skip_tokens else owner.prepared_batching.token_budget)
        chosen, compatibility, wire_bytes, reason = [], None, 0, 'ready_frontier_exhausted'
        for candidate in candidates:
            if candidate.key not in self._estimates:
                value = json.loads(candidate.task.payload)
                row, generation = value['row'], value['generation']
                _, conversations = _prepared([row])
                prompt = chat_token_count(self.tokenizer, conversations[0])
                snapshot = {name: row[name] for name in (*IDENTITY_FIELDS, 'messages_json')}
                item = dict(request_id='f'*32+':'+str((1 << 63)-1), row=snapshot)
                single = len(_frame(dict(op='submit', **item, generation=generation,
                    deadline=owner.deadline), limits.item_input_bytes))-4
                key = tuple(row[n] for n in ('query_id', 'algorithm_stage', 'model_role')) + (json.dumps(generation, sort_keys=True),)
                extra, _, _ = self._header_sizes(candidate.key.session_id, key[-1], limits)
                size = single-extra
                if size > limits.input_bytes:
                    raise ValueError('async engine IPC item exceeds its byte capacity')
                rpc_item = len(_frame(dict(row=row, generation=generation,
                    maximum=candidate.task.max_result_bytes,
                    request_key=[candidate.key.session_id, candidate.key.sequence]), limits.input_bytes))-4
                self._estimates[candidate.key] = (row['call_id'], prompt, generation['max_tokens'], key,
                    size, rpc_item)
            call_id, prompt, output, key, size, rpc_item = self._estimates[candidate.key]
            extra, header, rpc_header = self._header_sizes(candidate.key.session_id, key[-1], limits)
            if size+extra > limits.item_input_bytes or max(size, rpc_item) > limits.input_bytes:
                raise ValueError('async engine IPC item exceeds its byte capacity')
            arrival = RowArrival(call_id, 0., prompt, output, '', candidate.key)
            if chosen and key != compatibility:
                reason = 'compatibility'; break
            if builder.would_exceed_token_budget(arrival):
                reason = 'token_budget'; break
            proposed = header+size if not chosen else wire_bytes+1+size
            rpc_proposed = rpc_header+rpc_item if not chosen else rpc_bytes+1+rpc_item
            if max(proposed, rpc_proposed) > min(limits.input_bytes, (1 << 32)-1):
                if not chosen:
                    raise BufferError('complete prepared request exceeds the batch frame allowance')
                reason = 'frame_bytes'; break
            compatibility, wire_bytes, rpc_bytes = key, proposed, rpc_proposed
            chosen.append(candidate.key)
            full = builder.add(arrival)
            if full and not skip_tokens:
                reason = 'token_budget'; break
            if len(chosen) == available:
                reason = 'request_capacity'; break
        if not chosen:
            return ()
        batch = builder.close()
        self.selected += 1
        self.selected_seal_reason_counts[reason] += 1
        self.history = (self.history + [dict(members=len(chosen), tokens=batch.estimated_total_tokens,
            reason=reason, ipc_frame_bytes_upper_bound=wire_bytes, data_rpc_bytes_upper_bound=rpc_bytes,
            **(dict(C_request=self.request_window, requests_before=active_requests)
               if owner.request_feedback is not None else {}))])[-64:]
        return tuple(chosen)

    def report(self):
        batches, requests = self.usage()
        rolling = (dict(rolling_refill=True, C_request=self.request_window,
                        admission_unit='single_model_requests') if self.owner.rolling_refill else {})
        return dict(policy=asdict(self.owner.prepared_batching), concurrency=self.concurrency,
            concurrency_unit='application transport batches with unsettled compute members',
            request_limit=self.owner.resources.max_active_batches, active_batches=batches, active_requests=requests,
            peak_batches=self.peak_batches, peak_requests=self.peak_requests, selected_batches=self.selected,
            selected_seal_reason_counts=dict(self.selected_seal_reason_counts),
            recent_batches=list(self.history), row_cap=None, native_batching='engine-owned; independent', **rolling,
            **(dict(experimental_skip_token_packing=True)
               if getattr(self.owner, 'experimental_skip_token_packing', False) else {}))


class BatchSupplyFeedback:
    """Observe the existing owner and change only its organizer's batch window."""
    report_field = 'batch_supply_feedback'
    request_control = False

    def __init__(self, owner, config):
        self.owner, self.config = owner, config
        self.provider = self.controller = self.ref = self.future = None
        self.session_id = None
        self.last_native = None
        self.resource_samples = []
        self.resource_audit = []
        self.resource_rpc_audit = []
        self.resource_rpc_counts = dict(total=0, native_eligible=0, actor_unbound=0,
            session_mismatch=0, native_missing=0, CPU_qualified=0, CPU_rejected=0, evicted=0, errors=0)
        self.prepared = self.submitted = self.requests = self.errors = 0
        self.closed = False
        self.phases = []
        self.transition = set()
        self.next_poll = 0.
        self.snapshot_rpc_receipts = []
        self._snapshot_request = None
        self._snapshot_lock = Lock()
        self._snapshot_ready = None
        self._collected_ready = None

    @property
    def options(self):
        return self.config.options

    def _new_controller(self):
        from src.scheduling.submission_control.stage_work import SupplyFeedbackController
        return SupplyFeedbackController(self.config)

    def _inflight(self, batches, requests):
        return batches

    def _window(self):
        return self.owner._token_organizer.concurrency

    def _set_window(self, value):
        self.owner._token_organizer.concurrency = value

    def _report_identity(self):
        return dict(kind='fixed_B_batch_supply_feedback', initial_batches=1,
                    max_batches=self.config.max_batches)

    def _reset_controller(self, stream):
        self.controller.reset(1, len(stream.run.current)*len(stream._stage_ids))
        self._set_window(1)

    def _observes_resources(self):
        return (self.request_control and self.options.search_policy == 'ascending'
                and self.options.allow_exploration)

    @staticmethod
    def _resource_view(packet, *, request_index, requested_at_s, received_at_s):
        from src.scheduling.submission_control.resource_probe import ResourceProbeView
        packet = packet if isinstance(packet, dict) else {}
        sample = packet.get('sample')
        sample = sample if isinstance(sample, dict) else {}
        values, sources = sample.get('values'), sample.get('sources')
        values = values if isinstance(values, dict) else {}
        sources = sources if isinstance(sources, dict) else {}
        host = sources.get('host')
        host = host if isinstance(host, dict) else {}
        devices = packet.get('gpu_uuids') or ()
        devices = (tuple(devices) if isinstance(devices, (tuple, list))
                   and all(isinstance(uid, str) for uid in devices) else ())
        def number(value):
            return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None
        return ResourceProbeView(packet.get('engine_uid'), packet.get('session_id'), request_index,
            requested_at_s, received_at_s, number(packet.get('source_elapsed_s')), devices,
            # The shared reader aggregates multiple devices. Its mean cannot
            # establish that every device is idle; this owner uses one GPU.
            number(values.get('gpu_utilization_pct')) if len(devices) == 1 else None,
            number(host.get('cpu_allocation_utilization_pct')
                   if host.get('cpu_observation_scope') == 'current_cgroup_per_effective_CPU'
                   else values.get('host_cpu_per_core_max_pct'))
            if host.get('cpu_percent_interval_s') is not None else None)

    def _read_resource(self, packet, *, request_index, requested_at_s, received_at_s):
        view = self._resource_view(packet, request_index=request_index,
            requested_at_s=requested_at_s, received_at_s=received_at_s)
        # Retain source evidence even when the CPU interval is not
        # eligible for the unchanged feedback measurement window.
        packet = packet if isinstance(packet, dict) else {}
        sample = packet.get('sample')
        sample = sample if isinstance(sample, dict) else {}
        sources = sample.get('sources')
        sources = sources if isinstance(sources, dict) else {}
        host = sources.get('host')
        host = host if isinstance(host, dict) else {}
        interval = host.get('cpu_percent_interval_s')
        raw_interval = interval if type(interval) in (int, float) and math.isfinite(interval) else None
        cpu_reject = host.get('cpu_metrics_status')
        if cpu_reject == 'observed':
            cpu_reject = None
        if raw_interval is None:
            cpu_reject = cpu_reject or 'cpu_interval_missing_or_invalid'
        elif raw_interval <= 0:
            cpu_reject = 'cpu_interval_nonpositive'
        elif raw_interval > self.options.window_s:
            cpu_reject = 'cpu_interval_exceeds_feedback_window'
        else:
            cpu_reject = None if view.host_peak_cpu_pct is not None else (cpu_reject or 'cpu_value_unavailable')
        if raw_interval is None or not 0 < raw_interval <= self.options.window_s:
            from dataclasses import replace
            view = replace(view, host_peak_cpu_pct=None)
            interval = None
        return view, dict(view=asdict(view),
            status=packet.get('status', 'unavailable'), missing=sample.get('missing') or {},
            error_type=packet.get('error_type'), observed_values=sample.get('values') or {},
            host_cpu_interval_s=interval,
            host_cpu_raw_interval_s=raw_interval, host_cpu_reject_reason=cpu_reject,
            host_cpu_interval_type=type(host.get('cpu_percent_interval_s')).__name__,
            host_cpu_source={name: host[name] for name in ('cpu_metrics_status',
                'cpu_baseline_scope', 'cpu_counter_sample_sequence', 'cpu_sample_thread_id',
                'cpu_observation_scope', 'cpu_effective_slots', 'cpu_quota_slots',
                'cpu_allocation_busy_cores', 'cpu_allocation_utilization_pct', 'cpu_allocation_status',
                'cpu_counter_read_start_monotonic_ns', 'cpu_counter_read_end_monotonic_ns')
                if name in host and (type(host[name]) in (str, int) or
                    (type(host[name]) is float and math.isfinite(host[name])))},
            busy_period_scope='NVML internal period; distinct RPC reads need not be independent samples')

    @staticmethod
    def _audit_integer(value, *, minimum=0):
        return value if type(value) is int and minimum <= value < 1 << 64 else None

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

    def _decide(self, observation, supply, live, queued, admitted):
        return self.controller.update_supply(observation, supply)

    def event(self, action, key):
        if action == 'submitted':
            self.submitted += 1

    def ready(self, event):
        if event.action == 'Core_offer_accepted':
            self.prepared += 1

    def bind(self, stream):
        from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider
        with self._snapshot_lock:
            self.session_id = stream.session.session_id
            self.last_native = None
            self.resource_samples = []
            self.resource_rpc_audit = []
            self.resource_rpc_counts = dict.fromkeys(self.resource_rpc_counts, 0)
            self.started = time.monotonic()
        self.prepared = self.submitted = 0
        self.transition.clear()
        self.controller = self._new_controller()
        # Declaration bounds possible calls; no token reference or gold is read.
        self._reset_controller(stream)
        if self.provider is None and self.owner._expected_count:
            self._snapshot_ready = Event()
            self.provider = NonBlockingMetricsObservationProvider(self.sample,
                poll_interval_s=self.options.poll_interval_s,
                stale_after_s=self.options.window_s,
                close_timeout_s=self.options.poll_interval_s,
                ready_event=self._snapshot_ready, collect_ready=self.collect_ready)
        elif self.provider is not None:
            self.provider.resume()

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
                self.expiry = time.monotonic()+self.options.window_s
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
        ipc = value.get('ipc') if isinstance(value, dict) else None
        native = ipc.get('native_feedback') if isinstance(ipc, dict) else None
        native = native if isinstance(native, dict) else {}
        session = value.get('session_id') if isinstance(value, dict) else None
        uid, sequence, at = (native.get(n) for n in ('engine_uid', 'sequence', 'observed_at_s'))
        clock_error = None
        if type(at) in (int, float):
            try:
                if not math.isfinite(at):raise ValueError('non-finite source timestamp')
            except (OverflowError, ValueError) as error:
                at, clock_error = None, type(error).__name__
        else:
            at = None
        return dict(response_session_id=session if type(session) is int else None,
            response_engine_uid=uid if type(uid) is str else None,
            response_source_sequence=sequence if type(sequence) is int else None,
            response_source_observed_at_s=at, response_source_observed_at_error_type=clock_error)

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
            elif native.get('observed_at_s', -1) < self.started:
                disposition, reason = 'stale_source', 'source_predates_query'
            else:
                snapshot = ServiceMetricsSnapshot(native['running'], native['waiting'], native['kv_usage'],
                    source_uid=native['engine_uid'], source_session_id=response_session,
                    received_at_s=received_at_s, control_request_index=index,
                    source_clock_domain='native-engine NativeStats.clock seconds',
                    received_clock_domain='driver.time.monotonic seconds',
                    completed_tokens_total=native['generation_tokens_total'],
                    preemptions_total=native['preemptions_total'], source_sequence=native['sequence'],
                    source_observed_at_s=native['observed_at_s'])
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

    def update(self, stream):
        from dataclasses import replace
        from src.planning.work import StageWork
        from src.scheduling.core.models import AdmissionObservation
        from src.scheduling.submission_control.stage_work import SupplyState
        now = time.monotonic()
        if self.closed or self.controller is None or now < self.next_poll:
            return
        self.next_poll = now+self.options.poll_interval_s
        records = self.owner.core.capacity.records
        live = [r for r in records.values() if r.key.session_id == self.session_id]
        queued = [r for r in live if r.phase == 'QUEUED']
        batches, requests = self.owner._token_organizer.usage()
        inflight = self._inflight(batches, requests)
        with self.owner._receipt_lock:
            calls = [v for k, v in self.owner._raw_receipts.items()
                     if k.session_id == self.session_id and v.get('status') == 'succeeded']
            work = {}
            for target, source in [('completed_input_work_total', 'prompt_tokens'),
                    ('completed_output_work_total', 'completion_tokens'),
                    ('completed_computed_input_work_total', 'prompt_computed_tokens'),
                    ('completed_cached_input_work_total', 'prompt_cached_tokens')]:
                values = [v.get(source) for v in calls]
                work[target] = sum(values) if all(type(v) is int for v in values) else None
        # Timestamp after local counter capture, never count a later receipt
        # against an earlier window end. Invalid metric clocks remain unknown.
        try:
            o = (self.provider.latest(inflight) if self.provider is not None else
                 AdmissionObservation(time.monotonic(), False, inflight, None, None, None))
        except ValueError:
            self.errors += 1
            o = AdmissionObservation(time.monotonic(), False, inflight, None, None, None)
        admitted = {r.key for r in live if r.compute or self.request_control
                    and r.phase == 'QUEUED' and r.member is not None}
        self.transition.intersection_update(admitted)
        unread = max(0, self.owner._expected_count-len(stream.offered)) if not stream._phase_eof else 0
        o = replace(o, completed_items_total=len(calls), pending_items=len(queued)+unread,
                    transition_pending=len(self.transition), **work)
        if stream.driver.ready_observer_error is not None:
            o = replace(o, fresh=False)
        usage = self.owner.core.capacity.usage()
        limits = self.owner.core.capacity.limits
        # QUEUED tasks already own their input/result storage. Do not charge it
        # again, or mistake a full resident table for a dispatch constraint.
        blocked = 'member_R' if requests >= limits.active_requests else None
        pending = len(admitted)-requests
        if self.request_control and pending:
            blocked = 'pending_transport_admission'
        if not queued and blocked is None:
            from src.scheduling.core.session_capacity import SessionCapacity
            for row in stream.driver._rows.values():
                if row.run.pending is not None:
                    blocked = SessionCapacity.storage_block(usage, limits, row.run.pending.offered(0))
                    if blocked is not None:
                        break
        units = {}
        for record in queued:
            for stage in record.task.info.work.stages:
                key = (stage.stage, stage.unit)
                units[key] = units.get(key, 0)+stage.units
        state = SupplyState(o.observed_at_s, len(queued), self.prepared, self.submitted, batches,
            upstream_pending=not stream._phase_eof or bool(stream.driver.active_rows),
            exhausted=stream._phase_eof and not stream.driver.active_rows,
            capacity_blocked=blocked, ready_work=tuple(StageWork(k[0], v, k[1]) for k, v in sorted(units.items())),
            local_capacity=dict(usage=asdict(usage), limits=asdict(limits),
                resident_rows=stream.driver.active_rows,
                **(dict(pending_admission_requests=pending, charged_requests=len(admitted),
                        C_request=self._window()) if self.request_control else {}),
                local_capture_start_s=now-self.started, local_capture_end_s=o.observed_at_s-self.started),
            source_exhausted=stream._phase_eof,
            active_requests=requests if self.request_control else None)
        decision = self._decide(o, state, live, queued, admitted)
        if decision.window != self._window():
            self.transition = admitted
            self._set_window(decision.window)

    def end_phase(self):
        if self.controller is not None:
            report = self.controller.report()
            self.phases.append(dict(report, session_id=self.session_id,
                                    phase_closed=True, search_can_continue_next_query=report['can_continue'],
                                    can_continue=False))
            self.controller = None
        if self.provider is not None:
            self.provider.pause()

    def close(self):
        with self._snapshot_lock:
            self.closed = True
        if self.provider is not None:
            self.provider.close()
        self._reap_stopped_snapshot(cancel=True)
        self.end_phase()

    def report(self):
        if self.closed:
            self._reap_stopped_snapshot()
        # The sampler may outlive provider.close(). Copy ownership together;
        # RPC dispatch, result access and provider joins never hold this lock.
        with self._snapshot_lock:
            stopped = self.provider is None or not self.provider.is_running
            request = None if self._snapshot_request is None else dict(self._snapshot_request)
            receipts = list(self.snapshot_rpc_receipts)
            requests, errors, closed = self.requests, self.errors, self.closed
            ref_retained, future_retained = self.ref is not None, self.future is not None
            resource_attempts = [dict(row) for row in self.resource_rpc_audit]
            resource_counts = {name:self._audit_integer(value) for name, value in self.resource_rpc_counts.items()}
        pending = (None if request is None else
                   dict(request, rpc_terminal_status='unknown',
                        unresolved_reason=('client_future_cancelled_no_rpc_result'
                            if request['client_future_cancelled'] else 'terminal_not_collected')))
        complete = (closed and stopped and pending is None and not ref_retained
                    and not future_retained and len(receipts) == requests)
        return dict(**self._report_identity(), fixed_B=self.owner.prepared_batching.token_budget,
            phases=list(self.phases), snapshot_requests=requests, sample_errors=errors,
            snapshot_ref_retained=ref_retained,
            sampler_stopped=stopped, snapshot_rpc_cleanup_status=(
                'open' if not closed else 'closed' if complete else 'unknown'),
            snapshot_rpc_terminal_evidence_complete=complete,
            snapshot_rpc_receipts=[dict(row) for row in receipts],
            snapshot_rpc_pending=pending,
            snapshot_rpc_clock='driver monotonic seconds; response source timestamp is engine-supplied',
            resource_observation=dict(enabled=self._observes_resources(), recent_samples=list(self.resource_audit),
                recent_RPC_attempts=resource_attempts, RPC_counts=resource_counts,
                RPC_audit_session_id=self._audit_integer(self.session_id, minimum=-1), RPC_audit_capacity=64,
                RPC_audit_scope='current bound Core session; diagnostics only, not policy input',
                ownership='same single snapshot RPC; physical reader on model actor allocation',
                estimated_request_cost_required=False, real_performance_benefit_proven=False,
                action_scope='request permits only; no CPU/GPU placement, I/O scheduling or dynamic B'),
            measured_interfaces=['Core demand/StageWork/credits', 'method-ready/offer counts',
                                 'confirmed member work receipts', 'native running/waiting/KV/preemptions'] +
                (['optional actor-allocated NVML busy and host CPU with explicit missing coverage']
                 if self._observes_resources() else []),
            unavailable_interfaces=['MFU/HBM', 'per-request physical cost',
                'driver-host CPU if driver and model actor differ',
                'I/O or link bandwidth saturation', 'stage-specific physical demand',
                'usable host/device capacity for request admission'] +
                ([] if self._observes_resources() else ['CPU utilization', 'GPU utilization', 'physical free memory']),
            startup_capacity=dict(member_R=self.owner.resources.max_active_batches,
                declared_native_sequences=self.owner.engine.max_num_seqs,
                declared_native_tokens=self.owner.engine.max_num_batched_tokens,
                observed_native_capacity=(self.owner._actors.model_ready or {}).get('native_capacity'),
                observed_IPC=(self.owner._actors.model_ready or {}).get('limits'),
                IPC=asdict(self.owner._actors.limits)),
            no_capacity_deleted=True, online_cost_in_query=True, resource_benefit_proven=False)


class RequestSupplyFeedback(BatchSupplyFeedback):
    """Reuse the single-RPC sampler and demand gates; control request permits.

    No batch slot is retained by a slow peer. Already selected immutable groups
    own their member permits until accepted/settled, including after a shrink.
    Core's independent R/storage/transport bounds remain in force.
    """
    report_field = 'request_supply_feedback'
    request_control = True

    @property
    def options(self):
        return self.config

    def _new_controller(self):
        if self.config.search_policy == 'ascending':
            from src.scheduling.submission_control.request_scan import AscendingRequestController
            return AscendingRequestController(self.owner.resources.max_active_batches, self.config)
        from src.scheduling.submission_control.stage_work import RequestSupplyFeedbackController
        return RequestSupplyFeedbackController(self.owner.resources.max_active_batches, self.config)

    def _reset_controller(self, stream):
        if self.config.search_policy != 'ascending':
            return super()._reset_controller(stream)
        from src.scheduling.runtime.calibration import identity_signature
        ready = self.owner._actors.model_ready or {}
        uid = ready.get('engine_uid')
        # Scope is this actual resident owner, not a disk/global learned policy.
        # Exact method/generation/B/limits bind applicability; fresh measured
        # descriptors still validate changed work/cache mix in the controller.
        self._hint_identity = (None if not uid else identity_signature(dict(
            hardware=dict(owner_engine_uid=uid, native_capacity=ready.get('native_capacity')),
            model=asdict(self.owner.engine),
            generation=dict(stages=[asdict(self.owner._stage_bindings[s].generation) for s in stream._stage_ids]),
            execution=dict(options=asdict(self.config), B=asdict(self.owner.prepared_batching),
                           capacity=asdict(self.owner.core.capacity.limits),
                           IPC=asdict(self.owner._actors.limits)),
            workload=dict(stages=[dict(stage_id=s, method=asdict(self.owner._stage_bindings[s].method),
                                      model_role=self.owner._stage_bindings[s].model_role) for s in stream._stage_ids]),
            measurement=dict(work_unit='complete_model_request', policy=self.controller.strategy))))
        hint = (None if self._hint_identity is None else
                self.owner._request_scan_hints.get(self._hint_identity))
        window = 1 if hint is None else hint['selected_window']
        self.controller.reset(window, len(stream.run.current)*len(stream._stage_ids), search_hint=hint)
        self._set_window(window)

    def _decide(self, o, supply, live, queued, admitted):
        if self.config.search_policy != 'ascending':
            return super()._decide(o, supply, live, queued, admitted)
        from src.scheduling.submission_control.resource_step import NativeKVFrame, RequestKVWork, propose_kv_window
        ready = self.owner._actors.model_ready or {}
        capacity = ready.get('native_capacity') or {}
        native = self.last_native
        frame = None if native is None else NativeKVFrame(
            native.source_uid, native.source_session_id, native.source_sequence,
            native.source_observed_at_s, native.received_at_s,
            capacity.get('kv_cache_blocks'), capacity.get('kv_cache_block_tokens'), native.kv_usage,
            # Existing receipts do not prove the KV layout/cost model or a
            # cross-host clock mapping. Missing proof stays unknown. In
            # particular NVML free bytes cannot fill either field.
            clock_domain=capacity.get('mapped_resource_clock_domain'),
            cost_model=capacity.get('kv_cost_model'))
        estimates = self.owner._token_organizer._estimates
        def work(record):
            estimate = estimates.get(record.key)
            return RequestKVWork((record.key.session_id, record.key.sequence),
                None if estimate is None else estimate[1], None if estimate is None else estimate[2])
        # Never rescan input or manufacture mean costs to force a jump. Only
        # actual prepared/tokenized requests qualify; an uncosted prefix takes
        # the geometric fallback while native pressure still governs shrinking.
        step = propose_kv_window(frame, now_s=o.observed_at_s, max_age_s=self.options.window_s,
            expected_engine_uid=ready.get('engine_uid'), expected_session_id=self.session_id,
            current_window=self._window(), hard_limit=self.owner.resources.max_active_batches,
            ready=tuple(work(r) for r in queued if r.key not in admitted),
            committed=tuple(work(r) for r in live if r.key in admitted))
        from src.scheduling.submission_control.resource_probe import resource_probe_opportunity
        preemptions = self.controller._preemptions
        opportunity = resource_probe_opportunity(o, supply, current_window=self._window(),
            hard_limit=self.owner.resources.max_active_batches, max_age_s=self.options.window_s,
            expected_engine_uid=ready.get('engine_uid'), expected_session_id=self.session_id,
            physical_samples=tuple(self.resource_samples),
            preemption_increased=(o.preemptions_total is not None and preemptions is not None
                                  and o.preemptions_total > preemptions),
            low_kv_usage=self.controller.config.low_load_kv_usage)
        return self.controller.update_supply(o, supply, resource_step=step, resource_opportunity=opportunity)

    def end_phase(self):
        if (self.config.search_policy == 'ascending' and self.controller is not None
                and self._hint_identity is not None):
            hint = self.controller.export_hint()
            if hint is not None:
                self.owner._request_scan_pending[self._hint_identity] = hint
        super().end_phase()

    def _inflight(self, batches, requests):
        return requests

    def _window(self):
        return self.owner._token_organizer.request_window

    def _set_window(self, value):
        self.owner._token_organizer.request_window = value

    def _report_identity(self):
        initial = (self.phases[0]['initial_window'] if self.phases else
                   getattr(self.controller, '_initial_window', 1))
        return dict(kind='fixed_B_request_supply_feedback', initial_requests=initial,
            declared_cold_initial_requests=1,
            max_requests=self.owner.resources.max_active_batches,
            admission_unit='single_model_requests',
            shrink_policy='stop new selections; drain already admitted members without cancelling',
            pending_selection_permits='charged once per member; not native inflight', C_batch=None)
