"""Prepared-request organization for the existing Core/async owner, not an executor."""
from dataclasses import asdict
import json

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
