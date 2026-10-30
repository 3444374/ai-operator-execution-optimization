"""Prepared-request organization for the existing Core/async owner, not an executor."""
from dataclasses import asdict
import json
import time

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
        if active_batches >= self.concurrency:
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
        return dict(policy=asdict(self.owner.prepared_batching), concurrency=self.concurrency,
            concurrency_unit='application transport batches with unsettled compute members',
            request_limit=self.owner.resources.max_active_batches, active_batches=batches, active_requests=requests,
            peak_batches=self.peak_batches, peak_requests=self.peak_requests, selected_batches=self.selected,
            recent_batches=list(self.history), row_cap=None, native_batching='engine-owned; independent')


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
        from src.scheduling.runtime.observations import ServiceMetricsSnapshot
        if self.closed or self.session_id is None:
            return None
        finished = False
        try:
            if self.ref is None:
                self.sample_session = self.session_id
                self.ref = self.owner._actors.model_actor.snapshot.remote()
                self.requests += 1
                self.expiry = time.monotonic()+self.config.options.window_s
                self.expired = False
            if self.future is None:
                self.future = self.ref.future()
            if not self.future.done():
                if not self.expired and time.monotonic() >= self.expiry:
                    self.expired = True
                    try:
                        self.owner._actors.ray.cancel(self.ref, force=False)
                    except Exception:
                        self.errors += 1
                    # A failed cancel does not prove completion. Keep this RPC
                    # until its future settles, as the original Core sampler does.
                # Duplicate source sequence keeps its original timestamp in the
                # provider; it is never relabelled as fresh or idle.
                return self.last_native
            finished = True
            value = self.future.result()
            try:received_at_s = time.monotonic()
            except Exception:received_at_s = None
            self.ref = self.future = None
            native = (value.get('ipc') or {}).get('native_feedback')
            reason = ('expired_RPC' if self.expired else 'feedback_closed' if self.closed else
                'sampling_session_changed' if self.sample_session != self.session_id else
                'session_mismatch' if value.get('session_id') != self.session_id else
                'native_frame_missing' if native is None else
                'engine_uid_mismatch' if native.get('engine_uid') != self.owner._actors.model_ready['engine_uid'] else
                'source_predates_query' if native.get('observed_at_s', -1) < self.started else 'RPC_sample_valid')
            audit=getattr(self.provider,'observe_native',None)
            if callable(audit):
                try:audit(native,session_id=value.get('session_id'),received_at_s=received_at_s,
                    accepted=reason=='RPC_sample_valid',reason=reason)
                except Exception:pass
            if reason != 'RPC_sample_valid':
                return None
            self.last_native = ServiceMetricsSnapshot(native['running'], native['waiting'], native['kv_usage'],
                source_uid=native['engine_uid'], source_session_id=value['session_id'], received_at_s=received_at_s,
                completed_tokens_total=native['generation_tokens_total'],
                preemptions_total=native['preemptions_total'], source_sequence=native['sequence'],
                source_observed_at_s=native['observed_at_s'])
            return self.last_native
        except Exception:
            self.errors += 1
            if finished:
                self.ref = self.future = None
            return None

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
        self.closed = True
        if self.provider is not None:
            self.provider.close()
        if self.ref is not None:
            try:
                self.owner._actors.ray.cancel(self.ref, force=False)
            except Exception:
                self.errors += 1
        self.end_phase()

    def report(self):
        return dict(kind='fixed_B_batch_supply_feedback', initial_batches=1,
            max_batches=self.config.max_batches, fixed_B=self.owner.prepared_batching.token_budget,
            phases=list(self.phases), snapshot_requests=self.requests, sample_errors=self.errors,
            snapshot_ref_retained=self.ref is not None,
            sampler_stopped=self.provider is None or not self.provider.is_running,
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
