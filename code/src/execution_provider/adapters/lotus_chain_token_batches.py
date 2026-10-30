"""Prepared-request organization for the existing Core/async owner, not an executor."""
from dataclasses import asdict
from concurrent.futures import CancelledError
import json
import math
from threading import Lock
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
        self.selected = self.peak_batches = self.peak_requests = 0
        self.history = []

    def usage(self):
        active = [r for r in self.owner.core.capacity.records.values() if r.compute]
        batches = {r.member.batch_key for r in active if r.member is not None}
        self.peak_batches = max(self.peak_batches, len(batches))
        self.peak_requests = max(self.peak_requests, len(active))
        return len(batches), len(active)

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
        if available <= 0:
            return ()
        live = owner.core.capacity.records
        self._estimates = {k: v for k, v in self._estimates.items() if k in live}
        builder = PendingBatchBuilder(None, owner.prepared_batching.token_budget)
        chosen, compatibility, wire_bytes, reason = [], None, 0, 'ready_frontier_exhausted'
        for candidate in candidates:
            if candidate.key not in self._estimates:
                value = json.loads(candidate.task.payload)
                row, generation = value['row'], value['generation']
                _, conversations = _prepared([row])
                prompt = chat_token_count(self.tokenizer, conversations[0])
                snapshot = {name: row[name] for name in (*IDENTITY_FIELDS, 'messages_json')}
                item = dict(request_id='f'*32+':'+str((1 << 63)-1), row=snapshot)
                _frame(dict(op='submit', **item, generation=generation, deadline=owner.deadline), limits.item_input_bytes)
                size = len(_frame(item, limits.input_bytes))-4
                header = len(_frame(dict(op='submit_batch', requests=[], generation=generation,
                    deadline=owner.deadline), limits.input_bytes))-4
                rpc_item = len(_frame(dict(row=row, generation=generation,
                    maximum=candidate.task.max_result_bytes,
                    request_key=[candidate.key.session_id, candidate.key.sequence]), limits.input_bytes))-4
                rpc_header = len(_frame(dict(calls=[], batch_key=[candidate.key.session_id, (1 << 64)-1],
                    deadline=owner.deadline), limits.input_bytes))-4
                key = tuple(row[n] for n in ('query_id', 'algorithm_stage', 'model_role')) + (json.dumps(generation, sort_keys=True),)
                self._estimates[candidate.key] = (row['call_id'], prompt, generation['max_tokens'], key,
                    size, header, rpc_item, rpc_header)
            call_id, prompt, output, key, size, header, rpc_item, rpc_header = self._estimates[candidate.key]
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
            if full:
                reason = 'token_budget'; break
            if len(chosen) == available:
                reason = 'request_capacity'; break
        if not chosen:
            return ()
        batch = builder.close()
        self.selected += 1
        self.history = (self.history + [dict(members=len(chosen), tokens=batch.estimated_total_tokens,
            reason=reason, ipc_frame_bytes_upper_bound=wire_bytes, data_rpc_bytes_upper_bound=rpc_bytes)])[-64:]
        return tuple(chosen)

    def report(self):
        batches, requests = self.usage()
        rolling = (dict(rolling_refill=True, C_request=self.owner.resources.max_active_batches,
                        admission_unit='single_model_requests') if self.owner.rolling_refill else {})
        return dict(policy=asdict(self.owner.prepared_batching), concurrency=self.concurrency,
            concurrency_unit='application transport batches with unsettled compute members',
            request_limit=self.owner.resources.max_active_batches, active_batches=batches, active_requests=requests,
            peak_batches=self.peak_batches, peak_requests=self.peak_requests, selected_batches=self.selected,
            recent_batches=list(self.history), row_cap=None, native_batching='engine-owned; independent', **rolling)


class BatchSupplyFeedback:
    """Observe the existing owner and change only its organizer's batch window."""
    def __init__(self, owner, config):
        self.owner, self.config = owner, config
        self.provider = self.controller = self.ref = self.future = None
        self.session_id = None
        self.last_native = None
        self.prepared = self.submitted = self.requests = self.errors = 0
        self.closed = False
        self.phases = []
        self.transition = set()
        self.next_poll = 0.
        self.snapshot_rpc_receipts = []
        self._snapshot_request = None
        self._snapshot_lock = Lock()

    def event(self, action, key):
        if action == 'submitted':
            self.submitted += 1

    def ready(self, event):
        if event.action == 'Core_offer_accepted':
            self.prepared += 1

    def bind(self, stream):
        from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider
        from src.scheduling.submission_control.stage_work import SupplyFeedbackController
        self.session_id = stream.session.session_id
        self.last_native = None
        self.started = time.monotonic()
        self.prepared = self.submitted = 0
        self.transition.clear()
        self.controller = SupplyFeedbackController(self.config)
        # Declaration bounds possible calls; no token reference or gold is read.
        self.controller.reset(1, len(stream.run.current)*len(stream._stage_ids))
        self.owner._token_organizer.concurrency = 1
        if self.provider is None and self.owner._expected_count:
            self.provider = NonBlockingMetricsObservationProvider(self.sample,
                poll_interval_s=self.config.options.poll_interval_s,
                stale_after_s=self.config.options.window_s,
                close_timeout_s=self.config.options.poll_interval_s)
        elif self.provider is not None:
            self.provider.resume()

    def sample(self):
        if self.closed or self.session_id is None:
            return None
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
                ref = remote()
                with self._snapshot_lock:
                    self.ref = ref
                self.expiry = time.monotonic()+self.config.options.window_s
                self.expired = False
            if self.ref is None:
                return None  # Dispatch outcome without a handle remains unknown.
            if self.future is None:
                future = self.ref.future()
                with self._snapshot_lock:
                    self.future = future
            return self._consume_snapshot(cancel_pending=True)
        except Exception as error:
            with self._snapshot_lock:
                self.errors += 1
                if self._snapshot_request is not None:
                    field = 'dispatch_error_type' if self.ref is None else 'future_access_error_type'
                    self._snapshot_request = dict(self._snapshot_request, **{field:type(error).__name__})
            return None

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
            return None if self.closed else self.last_native
        with self._snapshot_lock:
            index = self._snapshot_request['request_index']
            timing = self._snapshot_request.get('future_timing_enabled', False)
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
        try:
            audit = getattr(self.provider, 'observe_native', None)
            if callable(audit):
                audit(native, session_id=response_session, received_at_s=received_at_s,
                    accepted=snapshot is not None, reason=reason, control_request_index=index,
                    source_clock_domain='native-engine NativeStats.clock seconds',
                    received_clock_domain='driver.time.monotonic seconds')
        except Exception:
            pass  # Optional audit cannot change the RPC receipt or valid feedback.
        if snapshot is not None:
            self.last_native = snapshot
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
        self.next_poll = now+self.config.options.poll_interval_s
        records = self.owner.core.capacity.records
        live = [r for r in records.values() if r.key.session_id == self.session_id]
        queued = [r for r in live if r.phase == 'QUEUED']
        batches, requests = self.owner._token_organizer.usage()
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
            o = (self.provider.latest(batches) if self.provider is not None else
                 AdmissionObservation(time.monotonic(), False, batches, None, None, None))
        except ValueError:
            self.errors += 1
            o = AdmissionObservation(time.monotonic(), False, batches, None, None, None)
        self.transition = {k for k in self.transition if k in records and records[k].compute}
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
                local_capture_start_s=now-self.started, local_capture_end_s=o.observed_at_s-self.started),
            source_exhausted=stream._phase_eof)
        decision = self.controller.update_supply(o, state)
        if decision.window != self.owner._token_organizer.concurrency:
            self.transition = {r.key for r in live if r.compute}
            self.owner._token_organizer.concurrency = decision.window

    def end_phase(self):
        if self.controller is not None:
            self.phases.append(dict(self.controller.report(), session_id=self.session_id,
                                    phase_closed=True, can_continue=False))
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
        pending = (None if request is None else
                   dict(request, rpc_terminal_status='unknown',
                        unresolved_reason=('client_future_cancelled_no_rpc_result'
                            if request['client_future_cancelled'] else 'terminal_not_collected')))
        complete = (closed and stopped and pending is None and not ref_retained
                    and not future_retained and len(receipts) == requests)
        return dict(kind='fixed_B_batch_supply_feedback', initial_batches=1,
            max_batches=self.config.max_batches, fixed_B=self.owner.prepared_batching.token_budget,
            phases=list(self.phases), snapshot_requests=requests, sample_errors=errors,
            snapshot_ref_retained=ref_retained,
            sampler_stopped=stopped, snapshot_rpc_cleanup_status=(
                'open' if not closed else 'closed' if complete else 'unknown'),
            snapshot_rpc_terminal_evidence_complete=complete,
            snapshot_rpc_receipts=[dict(row) for row in receipts],
            snapshot_rpc_pending=pending,
            snapshot_rpc_clock='driver monotonic seconds; response source timestamp is engine-supplied',
            measured_interfaces=['Core demand/StageWork/credits', 'method-ready/offer counts',
                                 'confirmed member work receipts', 'native running/waiting/KV/preemptions'],
            unavailable_interfaces=['CPU utilization', 'GPU utilization', 'physical free memory', 'MFU/HBM'],
            startup_capacity=dict(member_R=self.owner.resources.max_active_batches,
                declared_native_sequences=self.owner.engine.max_num_seqs,
                declared_native_tokens=self.owner.engine.max_num_batched_tokens,
                observed_native_capacity=None,
                observed_IPC=(self.owner._actors.model_ready or {}).get('limits'),
                IPC=asdict(self.owner._actors.limits)),
            no_capacity_deleted=True, online_cost_in_query=True, resource_benefit_proven=False)
