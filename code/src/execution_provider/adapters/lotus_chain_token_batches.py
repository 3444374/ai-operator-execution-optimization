"""Prepared-request organization for the existing Core/async owner, not an executor."""
from dataclasses import asdict
import json
import time

from src.modalities.text.tokenization import chat_token_count, chat_token_ids
from src.scheduling.organization.batching import PendingBatchBuilder, RowArrival
from src.serving.backends.async_engine_wire import _frame
from src.serving.backends.vllm_protocol import IDENTITY_FIELDS, _prepared

from .records_feedback_observation import audit_integer, snapshot_rpc_report
from .records_native_sampler import RecordsNativeSampler


class _PreparedPrefix:
    """Incremental organization prediction; no task leases or capacity grants."""

    def __init__(self, organizer, available):
        self.organizer, self.available = organizer, available
        self.sequence = organizer._prefix_sequence
        organizer._prefix_sequence += 1
        self.skip_tokens = organizer.owner.prepared_batching is None
        self.builder = PendingBatchBuilder(available if self.skip_tokens else None,
            0 if self.skip_tokens else organizer.owner.prepared_batching.token_budget)
        self.chosen = []
        self.compatibility = None
        self.wire_bytes = self.rpc_bytes = 0
        self.reason, self.closed = 'ready_frontier_exhausted', False

    def consider(self, candidate):
        organizer, limits = self.organizer, self.organizer.owner._actors.limits
        call_id, prompt, output, key, size, rpc_item = organizer._estimate(candidate)
        extra, header, rpc_header = organizer._header_sizes(candidate.key.session_id, key[-1], limits)
        if size+extra > limits.item_input_bytes or max(size, rpc_item) > limits.input_bytes:
            raise ValueError('async engine IPC item exceeds its byte capacity')
        arrival = RowArrival(call_id, 0., prompt, output, '', candidate.key)
        if self.chosen and key != self.compatibility:
            self.reason, self.closed = 'compatibility', True
            return
        if self.builder.would_exceed_token_budget(arrival):
            self.reason, self.closed = 'token_budget', True
            return
        proposed = header+size if not self.chosen else self.wire_bytes+1+size
        rpc_proposed = rpc_header+rpc_item if not self.chosen else self.rpc_bytes+1+rpc_item
        if max(proposed, rpc_proposed) > min(limits.input_bytes, (1 << 32)-1):
            if not self.chosen:
                raise BufferError('complete prepared request exceeds the batch frame allowance')
            self.reason, self.closed = 'frame_bytes', True
            return
        self.compatibility, self.wire_bytes, self.rpc_bytes = key, proposed, rpc_proposed
        self.chosen.append(candidate.key)
        full = self.builder.add(arrival)
        if full and not self.skip_tokens:
            self.reason, self.closed = 'token_budget', True
        elif len(self.chosen) == self.available:
            self.reason, self.closed = 'request_capacity', True


class PreparedTokenOrganizer:
    def __init__(self, owner, tokenizer, concurrency, *, prepared_order=None):
        self.owner, self.tokenizer, self.concurrency = owner, tokenizer, concurrency
        self._estimates = {}
        self._token_prefixes = {}
        self.prepared_order = prepared_order
        self._header_estimate = self._header_context = None
        self.selected = self.peak_batches = self.peak_requests = 0
        self.selected_seal_reason_counts = dict(token_budget=0, request_capacity=0,
            frame_bytes=0, compatibility=0, ready_frontier_exhausted=0)
        self.request_window = getattr(owner, 'request_window', owner.resources.max_active_batches)
        self.history = []
        self._prefix = self._prefix_context = None
        self._frontier, self._frontier_set = [], set()
        self._prefix_sequence = 0

    def release_query_payloads(self):
        """Release completed query candidates while retaining batch statistics."""
        self._prefix = self._prefix_context = None
        self._estimates, self._frontier, self._frontier_set = {}, [], set()
        self._token_prefixes = {}
        self._header_estimate = self._header_context = None

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

    def _estimate(self, candidate):
        limits = self.owner._actors.limits
        if candidate.key not in self._estimates:
            value = json.loads(candidate.task.payload)
            row, generation = value['row'], value['generation']
            _, conversations = _prepared([row])
            if self.prepared_order is None or self.prepared_order.mode == 'length':
                prompt = chat_token_count(self.tokenizer, conversations[0])
                if self.prepared_order is not None:
                    self._token_prefixes[candidate.key] = ()
            else:
                tokens = chat_token_ids(self.tokenizer, conversations[0])
                prompt = len(tokens)
                self._token_prefixes[candidate.key] = tuple(tokens[:self.prepared_order.prefix_token_limit])
            from src.serving.backends.model_context import model_context_limit
            if prompt + generation['max_tokens'] > model_context_limit(self.owner.engine, self.owner._actors.model_ready):
                raise ValueError('prepared prompt and output cap exceed effective model context')
            snapshot = {name: row[name] for name in (*IDENTITY_FIELDS, 'messages_json')}
            item = dict(request_id='f'*32+':'+str((1 << 63)-1), row=snapshot)
            single = len(_frame(dict(op='submit', **item, generation=generation,
                deadline=self.owner.deadline), limits.item_input_bytes))-4
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
        return self._estimates[candidate.key]

    def _available(self, active_requests, active_batches=0):
        owner, limits = self.owner, self.owner._actors.limits
        if not owner.rolling_refill and active_batches >= self.concurrency:
            return 0
        available = min(owner.resources.max_active_batches-active_requests, limits.max_inflight,
                        limits.result_slots, limits.result_bytes//limits.item_result_bytes)
        if owner.rolling_refill:
            available = min(available, self.request_window-active_requests)
        return max(0, available)

    def _context(self, available):
        owner, limits = self.owner, self.owner._actors.limits
        return (available, owner.deadline,
                None if owner.prepared_batching is None else owner.prepared_batching.token_budget,
                limits.input_bytes, limits.item_input_bytes)

    def _plan(self, candidates, available):
        prefix = _PreparedPrefix(self, available)
        if self.prepared_order is not None:
            from src.scheduling.organization.prepared_order import RequestOrganizationProfile, order_prepared_window
            # All members are already accepted complete tasks. No new input is
            # pulled and the original payload/generation objects are unchanged.
            candidates = tuple(candidates[:self.prepared_order.candidate_limit])
            profiles = []
            by_key = {candidate.key: candidate for candidate in candidates}
            for candidate in candidates:
                _, prompt, output, compatibility, _, _ = self._estimate(candidate)
                profiles.append(RequestOrganizationProfile(candidate.key, compatibility, prompt, output,
                                                            self._token_prefixes[candidate.key]))
            candidates = tuple(by_key[key] for key in order_prepared_window(profiles, self.prepared_order))
        for candidate in candidates:
            prefix.consider(candidate)
            if prefix.closed:
                break
        return prefix

    def refresh_prefix(self, candidates, active_requests):
        """Reconcile a prediction after authoritative Core progress, not per row."""
        live = self.owner.core.capacity.records
        self._estimates = {key: value for key, value in self._estimates.items() if key in live}
        self._token_prefixes = {key: value for key, value in self._token_prefixes.items() if key in live}
        if not live:
            self._header_estimate = self._header_context = None
        available = self._available(active_requests)
        keys = [candidate.key for candidate in candidates]
        context = self._context(available)
        if self._prefix is not None and context == self._prefix_context and keys == self._frontier:
            return
        self._frontier, self._frontier_set = keys, set(keys)
        self._prefix_context = context
        self._prefix = self._plan(candidates, available) if available else None

    def append_ready(self, candidates):
        """Append accepted keys; a research order recomputes its finite window."""
        for candidate in candidates:
            if candidate.key in self._frontier_set:
                continue
            self._frontier.append(candidate.key)
            self._frontier_set.add(candidate.key)
            if self.prepared_order is None and self._prefix is not None and not self._prefix.closed:
                self._prefix.consider(candidate)
        if self.prepared_order is not None and self._prefix_context is not None:
            from src.scheduling.core.session_policy import TaskCandidate
            records = self.owner.core.capacity.records
            frontier = tuple(TaskCandidate(key, records[key].task, records[key].spec)
                             for key in self._frontier)
            self._prefix = self._plan(frontier, self._prefix_context[0]) if self._prefix_context[0] else None

    def dispatch_token(self, can_prepare, active_requests):
        prefix = self._prefix
        if prefix is None or not prefix.chosen:
            return None
        if prefix.closed or not can_prepare or active_requests == 0:
            return (prefix.sequence, len(prefix.chosen), prefix.reason, self._prefix_context)
        ready, version = self.owner.transport.prepared_supply_opportunity()
        if ready:
            return (prefix.sequence, len(prefix.chosen), prefix.reason, self._prefix_context, version)
        return None

    def _defer_partial(self, active_requests, can_prepare):
        if not active_requests or not can_prepare:
            return False
        if getattr(self.owner, '_event_driven_preparation', False):
            return not self.owner.transport.prepared_supply_opportunity()[0]
        return True

    def __call__(self, candidates):
        owner = self.owner
        active_batches, active_requests = self.usage()
        available = self._available(active_requests, active_batches)
        if not available:
            return ()
        live = owner.core.capacity.records
        self._estimates = {k: v for k, v in self._estimates.items() if k in live}
        self._token_prefixes = {k: v for k, v in self._token_prefixes.items() if k in live}
        if not live:
            self._header_estimate = self._header_context = None
        incremental = getattr(owner, '_event_driven_preparation', False)
        context = self._context(available)
        if (incremental and self._prefix is not None and context == self._prefix_context
                and tuple(self._frontier) == tuple(candidate.key for candidate in candidates)):
            prefix = self._prefix
        else:
            prefix = self._plan(candidates, available)
        if not prefix.chosen:
            return ()
        if (getattr(owner, '_interleaved_preparation', False)
                and prefix.reason == 'ready_frontier_exhausted'
                and self._defer_partial(active_requests, getattr(owner._active, 'can_prepare', False))):
            if incremental:
                self._prefix, self._prefix_context = prefix, context
                self._frontier = [candidate.key for candidate in candidates]
                self._frontier_set = set(self._frontier)
            return ()
        batch = prefix.builder.close()
        self.selected += 1
        self.selected_seal_reason_counts[prefix.reason] += 1
        self.history = (self.history + [dict(members=len(prefix.chosen), tokens=batch.estimated_total_tokens,
            reason=prefix.reason, ipc_frame_bytes_upper_bound=prefix.wire_bytes,
            data_rpc_bytes_upper_bound=prefix.rpc_bytes,
            **(dict(C_request=self.request_window, requests_before=active_requests)
               if owner.request_feedback is not None else {}))])[-64:]
        if incremental:
            self._prefix = self._prefix_context = None
            self._frontier, self._frontier_set = [], set()
        return tuple(prefix.chosen)

    def report(self):
        batches, requests = self.usage()
        rolling = (dict(rolling_refill=True, C_request=self.request_window,
                        admission_unit='single_model_requests') if self.owner.rolling_refill else {})
        return dict(policy=None if self.owner.prepared_batching is None else asdict(self.owner.prepared_batching),
            **(dict(prepared_order=asdict(self.prepared_order),
                    order_scope='accepted complete requests; prefix and length proxies, not actual GPU batches')
               if self.prepared_order is not None else {}),
            concurrency=self.concurrency,
            concurrency_unit='application transport batches with unsettled compute members',
            request_limit=self.owner.resources.max_active_batches, active_batches=batches, active_requests=requests,
            peak_batches=self.peak_batches, peak_requests=self.peak_requests, selected_batches=self.selected,
            selected_seal_reason_counts=dict(self.selected_seal_reason_counts),
            recent_batches=list(self.history), row_cap=None, native_batching='engine-owned; independent', **rolling,
            **(dict(interleaved_preparation=True) if getattr(self.owner, '_interleaved_preparation', False) else {}),
            **(dict(event_driven_preparation=True, partial_frontier_flush='data_rpc_opportunity')
               if getattr(self.owner, '_event_driven_preparation', False) else {}))


class BatchSupplyFeedback(RecordsNativeSampler):
    """Observe the existing owner and change only its organizer's batch window."""
    report_field = 'batch_supply_feedback'
    request_control = False

    def __init__(self, owner, config):
        self.config = config
        super().__init__(owner)
        self.controller = None
        self.prepared = self.submitted = 0
        self.phases = []
        self.transition = set()
        self.next_poll = 0.

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
        return False


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
        self._reset_sampling_phase(stream.session.session_id)
        self.prepared = self.submitted = 0
        self.transition.clear()
        self.controller = self._new_controller()
        # Declaration bounds possible calls; no token reference or gold is read.
        self._reset_controller(stream)
        self._start_sampling(self.owner._expected_count, NonBlockingMetricsObservationProvider)

    def _latest(self, inflight):
        return self.provider.latest(inflight)


    def update(self, stream):
        from dataclasses import replace
        from src.planning.work import StageWork
        from src.scheduling.core.models import AdmissionObservation
        from src.scheduling.submission_control.feedback_work import SupplyState
        now = time.monotonic()
        if self.controller is None or not self._control_due(now):
            return
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
            o = (self._latest(inflight) if self.provider is not None else
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
        self._close_sampling()
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
            resource_counts = {name:audit_integer(value) for name, value in self.resource_rpc_counts.items()}
        snapshot = snapshot_rpc_report(request=request, receipts=receipts, requests=requests,
            errors=errors, closed=closed, ref_retained=ref_retained, future_retained=future_retained,
            sampler_stopped=stopped)
        return dict(**self._report_identity(),
            fixed_B=None if self.owner.prepared_batching is None else self.owner.prepared_batching.token_budget,
            phases=list(self.phases), **snapshot,
            resource_observation=dict(enabled=self._observes_resources(), recent_samples=list(self.resource_audit),
                recent_RPC_attempts=resource_attempts, RPC_counts=resource_counts,
                RPC_audit_session_id=audit_integer(self.session_id, minimum=-1), RPC_audit_capacity=64,
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
