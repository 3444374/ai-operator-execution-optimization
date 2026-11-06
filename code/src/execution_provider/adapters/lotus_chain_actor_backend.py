"""Prepared F/M continuations through the existing Core transport and async actor.

The caller owns actor allocation, deadlines, close and durable receipt storage.
No second admission counter or inference queue is introduced here.
"""

from copy import deepcopy
from concurrent.futures import Future, InvalidStateError
from dataclasses import asdict
import asyncio
import json
import math

from src.scheduling.core.session_contract import Acceptance, BatchSubmission, TaskKey, Terminal
from src.serving.backends.vllm_protocol import GenerationSpec, IDENTITY_FIELDS, _prepared
from .records_model_adapter import _check_result_capacity
from .records_transport import _RecordTransport, _await_result


class LotusChainActorBackend(_RecordTransport):
    def __init__(self, actors, maximum, notify, *, deadline_monotonic, on_result, token_budget=None, bulk=False,
                 handoff_progress=False):
        if deadline_monotonic is not None and (not math.isfinite(deadline_monotonic) or deadline_monotonic <= 0):
            raise ValueError("request deadline must be finite and positive or None")
        if not callable(on_result):
            raise TypeError("an owner-held receipt sink is required")
        if handoff_progress and not bulk:
            raise ValueError('handoff progress requires prepared bulk transport')
        if token_budget is not None:
            from .lotus_chain_token_budget import ChainTokenBudget
            if type(token_budget) is not ChainTokenBudget or token_budget.engine != actors.engine:
                raise ValueError("the token configuration must belong to the model owner")
        self.token_budget, self.on_result = token_budget, on_result
        super().__init__(actors, maximum, notify, finalize=self._finish_batches if bulk else None)
        self.deadline = deadline_monotonic
        self.supports_batch_submit = bulk
        self._batch_poll_ack = bulk and getattr(actors, 'supports_prepared_batch_ack', False)
        self._bulk = {}
        self.data_batch_rpcs = self.peak_result_frame_bytes = 0
        self._batch_submits_started = 0
        self._batch_ack_counts = dict(accepted=0, rejected=0, unknown=0)
        self._observe_handoff = handoff_progress
        self._handoff_notify = notify
        self._handoff_pending = self._handoff_generation = 0
        self._handoff_stalled = False
        self._handoff_observer_error = None
        self._rpc_subscription = None
        if handoff_progress:
            try:
                self._rpc_subscription = actors._bind_rpc_progress(notify)
            except BaseException as error:
                if not self.backend.close():
                    error.add_note('new handoff backend cleanup remains unconfirmed')
                raise

    def _publish_handoff(self):
        try:
            self._handoff_notify()
        except Exception as error:
            self._handoff_observer_error = type(error).__name__

    def prepared_supply_opportunity(self):
        """Cheap hint, including host handoffs not yet registered at the RPC gate.

        The two snapshots can race; neither grants compute or RPC capacity.
        Existing Core and RPC gates remain the final admission checks.
        """
        rpc = self.actors._data_rpc_progress()
        with self._lock:
            unregistered = max(0, self._handoff_pending-rpc.batch_submits_pending)
            available = (self._observe_handoff and not self._handoff_stalled
                and not rpc.unconfirmed and rpc.workers-rpc.pending-unregistered > 0)
            return available, (self._handoff_generation, rpc.generation)

    def batch_handoff_report(self):
        """First actor ACK per logical submit, separate from per-member results.

        Owner backpressure retries and result polling are not new packets.
        Unknown means no recognized first ACK was observed; it does not retire
        any request responsibility or imply an unknown model terminal.
        """
        with self._lock:
            counts = dict(self._batch_ack_counts)
            return dict(submit_operations_started=self._batch_submits_started,
                actor_ack_counts=counts,
                pending_actor_acks=self._batch_submits_started-sum(counts.values()))

    def _count_batch_ack(self, outcome, context=None):
        with self._lock:
            self._batch_ack_counts[outcome] += 1
            if self._observe_handoff and context.pop('handoff_pending', False):
                self._handoff_pending -= 1
                self._handoff_stalled |= outcome != 'accepted'
                self._handoff_generation += 1
        if self._observe_handoff:
            self._publish_handoff()

    def recover_actor_receipts(self, receipts):
        errors = super().recover_actor_receipts(receipts)
        if self.supports_batch_submit:
            with self._lock:
                recovered = dict(self._outcomes)
            for context in tuple(self._bulk.values()):
                for key, future in tuple(context['futures'].items()):
                    if key in recovered and not future.done():
                        try:
                            future.set_result(recovered[key])
                        except InvalidStateError:
                            pass  # The ordinary result poll won the same transfer.
        return errors

    async def _finish_batches(self):
        tasks = [c['driver'] for c in self._bulk.values() if 'driver' in c]
        if tasks:
            await asyncio.gather(*tasks)
        for group, context in tuple(self._bulk.items()):
            if context['acks']:
                await self._poll_batch(group, context)
        # Backend close calls this only after all member executions stopped.
        self._bulk.clear()
        if self._rpc_subscription is not None:
            self.actors._unbind_rpc_progress(self._rpc_subscription)

    def _retire_batch(self, group, context):
        if context['driver_done'] and not context['futures'] and not context['acks']:
            self._bulk.pop(group, None)

    async def _poll_batch(self, group, context):
        key = (group.session_id, group.sequence)
        acknowledgements = tuple(context['acks'])
        if acknowledgements:
            ref = self.actors.wait_prepared_batch(key, acknowledgements)
        else:
            ref = (self.actors.wait_prepared_batch(key)
                   if callable(getattr(self.actors, 'wait_prepared_batch', None))
                   else self.actors.poll_prepared_batch(key))
        raw = await _await_result(ref)
        self.peak_result_frame_bytes = max(self.peak_result_frame_bytes, len(raw))
        response = json.loads(raw)
        # Keep the ACK intent if the RPC/response fails. Its original receipt
        # already lives in the owner sink; replay never resubmits model work.
        for member in acknowledgements:
            context['acks'].pop(member, None)
        return response

    def try_submit_batch(self, tasks, endpoint):
        return self._try_submit_batch(tasks, endpoint)

    def _try_submit_batch(self, tasks, endpoint, *, values=None):
        if not self.supports_batch_submit or not tasks or any(t.member is None for t in tasks):
            raise ValueError('bulk handoff requires an organized prepared batch')
        group = tasks[0].member.batch_key
        if (group in self._bulk or any(t.member.batch_key != group for t in tasks)
                or tuple(t.member.index for t in tasks) != tuple(range(len(tasks)))):
            raise ValueError('bulk handoff changed ordered membership')
        if values is None:
            values = [self._request(t) for t in tasks]
        context = dict(tasks={t.key: t for t in tasks}, values={t.key: v for t, v in zip(tasks, values)},
                       members=frozenset(t.key for t in tasks), futures={t.key: Future() for t in tasks},
                       started=False, driver_done=False, accepted=False, acks={})
        self._bulk[group] = context
        with self._lock:
            if self._observe_handoff:
                context['handoff_pending'] = True
                self._handoff_pending += 1
                self._handoff_generation += 1
            for task, value in zip(tasks, values):
                self._requests[task.key] = {n: value['row'][n] for n in IDENTITY_FIELDS}, task.task.max_result_bytes
                self._refs[task.key] = (value['row'],)
        outcome = self.backend.try_submit_batch(tasks, endpoint)
        if outcome.acceptance == Acceptance.NOT_ACCEPTED:
            del self._bulk[group]
            with self._lock:
                if context.pop('handoff_pending', False):
                    self._handoff_pending -= 1
                    self._handoff_generation += 1
                for task in tasks:
                    self._requests.pop(task.key, None); self._refs.pop(task.key, None)
            if self._observe_handoff:
                self._publish_handoff()
        return outcome

    async def _drive_batch(self, group, context):
        ack_pending = False
        try:
            calls = [dict(row=context['values'][t.key]['row'], generation=context['values'][t.key]['generation'],
                          maximum=t.task.max_result_bytes, request_key=[t.key.session_id, t.key.sequence])
                     for t in context['tasks'].values()]
            key = (group.session_id, group.sequence)
            with self._lock:
                self._batch_submits_started += 1
            ack_pending = True
            ref = self.actors.submit_prepared_batch(calls, key, self.deadline)
            with self._lock:
                self.data_batch_rpcs += 1
            calls = None
            context['tasks'].clear()
            context['values'].clear()
            response = await _await_result(ref)
            status = response.get('status') if type(response) is dict else None
            ack_pending = False
            self._count_batch_ack(status if type(status) is str and status in ('accepted', 'rejected')
                                  else 'unknown', context)
            context['accepted'] = status == 'accepted'
            pending = set(context['members'])
            while pending or self._batch_poll_ack and (context['futures'] or context['acks']):
                pending = {member for member in pending if (future := context['futures'].get(member)) is not None
                           and not future.done()}
                if not pending and not self._batch_poll_ack:
                    break
                if not pending and not context['acks']:
                    if not context['futures']:
                        break
                    # Raw result readiness is not a sink-transfer proof. Let
                    # the existing member coroutines finish before final ACK.
                    await asyncio.sleep(self.poll_interval_s)
                    continue
                if response['status'] == 'accepted':
                    response = await self._poll_batch(group, context)
                for receipt in response.get('results', ()):
                    task_key = TaskKey(*receipt['request_key'])
                    if task_key not in context['members']:
                        raise ValueError('bulk result belongs to another group')
                    if not self._batch_poll_ack and task_key not in pending and task_key not in context['futures']:
                        # A lost ACK can hold a later bounded page behind this
                        # receipt. Retry only after audit/sink transfer removed
                        # its future, never just because the future is ready.
                        self.actors.acknowledge_core(task_key)
                    if task_key in pending:
                        future = context['futures'].get(task_key)
                        try:
                            if future is not None:
                                future.set_result(json.dumps(receipt['result'],
                                    ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode())
                        except InvalidStateError:
                            pass  # A validated close receipt may already have transferred it.
                        pending.remove(task_key)
                if not pending and not self._batch_poll_ack:
                    break
                if pending and response.get('closed') and type(response.get('more_results')) is not bool:
                    raise ValueError('closed bulk reply has no bounded-page frontier')
                if pending and (response.get('closed') and not response['more_results']
                        or response.get('status') == 'rejected'):
                    raise RuntimeError('bulk producer closed without all member receipts')
                raw = receipt = future = None
                response = dict(status='accepted')
                await asyncio.sleep(self.poll_interval_s)
        except BaseException as error:
            for future in tuple(context['futures'].values()):
                if not future.done():
                    try:
                        future.set_exception(error)
                    except InvalidStateError:
                        pass
        finally:
            if ack_pending:
                self._count_batch_ack('unknown', context)
            # A fast RPC may finish before the backend schedules every local
            # member. Keep only their charged result futures until transfer.
            context['driver_done'] = True
            self._retire_batch(group, context)

    def _request(self, task):
        value = json.loads(task.task.payload)
        row = value["row"]
        _prepared([row])
        _check_result_capacity(row, task.task.max_result_bytes)
        if row["algorithm_stage"] != task.task.profile_name:
            raise ValueError("prepared method stage changed its Core profile")
        generation = GenerationSpec.from_dict(value["generation"])
        if self.token_budget is None:
            if "token_cost" in value or task.task.estimated_work != 1:
                raise ValueError("request-count mode does not accept token reservations")
        else:
            self.token_budget.validate(row, generation, value["token_cost"])
            if task.task.estimated_work != value["token_cost"]["reserved_tokens"]:
                raise ValueError("Core reservation changed the prepared token estimate")
        return value

    def try_submit(self, task, endpoint):
        # Reject malformed estimates before transport/actor ownership is accepted.
        self._request(task)
        return self.backend.try_submit(task, endpoint)

    def _submit_task(self, task, timings, prepare_method):
        if self.supports_batch_submit:
            context = self._bulk[task.member.batch_key]
            if not context['started']:
                context['started'] = True
                context['driver'] = asyncio.create_task(self._drive_batch(task.member.batch_key, context))
            future = context['futures'][task.key]
            class Result:
                def future(self):
                    return future
            return Result()
        value = self._request(task)
        row = value["row"]
        expected = {name: row[name] for name in IDENTITY_FIELDS}
        with self._lock:
            self._requests[task.key] = expected, task.task.max_result_bytes
            self._refs[task.key] = (row,)
        result = self.actors.submit_prepared_core([row], self.deadline, task.task.max_result_bytes,
            task.key, value["generation"], value.get("token_cost"))
        with self._lock:
            self._refs[task.key] = (row, result)
        return result

    def _audit_result(self, expected, value):
        from .lotus_chain_token_budget import audit_chain_call
        audit_chain_call(expected, value)

    def _result_transferred(self, task, value):
        request = json.loads(task.task.payload)
        receipt = dict(deepcopy(value), parent_call_ids=list(request["parent_call_ids"]),
                       core_task_key=dict(session_id=task.key.session_id, sequence=task.key.sequence))
        if "token_cost" in request:
            receipt["token_estimate"] = deepcopy(request["token_cost"])
        if task.member is not None:
            receipt['transport_batch'] = asdict(task.member)
        self.on_result(task.key, receipt)
        if task.member is not None and not self._batch_poll_ack:
            context = self._bulk.get(task.member.batch_key)
            if context is not None:
                context['futures'].pop(task.key, None)
                self._retire_batch(task.member.batch_key, context)

    def _acknowledge_result(self, task):
        if not self._batch_poll_ack:
            return super()._acknowledge_result(task)
        context = self._bulk[task.member.batch_key]
        if context['accepted']:
            context['acks'][task.key] = None

    async def _execute(self, task, endpoint):
        try:
            return await super()._execute(task, endpoint)
        finally:
            if self._batch_poll_ack:
                context = self._bulk.get(task.member.batch_key)
                if context is not None:
                    context['futures'].pop(task.key, None)
                    self._retire_batch(task.member.batch_key, context)

    def poll(self, handles, max_events):
        events = self.backend.poll(handles, max_events)
        for event in events:
            if type(event) is Terminal:
                self.acknowledge(event.key)
        return events

    def request_cancel(self, key, handle):
        self.backend.request_cancel(key, handle)

    def close(self, timeout=5):
        """Only stop local delivery; the caller must close the same actor owner."""
        return self.backend.close(timeout)

    def snapshot(self):
        return dict(retained_refs=self.retained_refs(),
            retained_results=[dict(core_task_key=dict(session_id=key.session_id, sequence=key.sequence),
                                   result=json.loads(value)) for key,value in self.failure_receipts()])


class _SectionTransport(LotusChainActorBackend):
    """Register the existing transport's exact prepared calls before its RPC."""

    def __init__(self, owner, maximum):
        self.owner = owner
        self._audit_pending = {}
        super().__init__(owner._actors, maximum, lambda: owner.core.wake.notify(),
                         deadline_monotonic=owner.deadline, on_result=owner._record_local_receipt,
                         token_budget=owner.token_budget, bulk=owner._async_token_batches,
                         handoff_progress=getattr(owner, '_event_driven_preparation', False))
        self.poll_interval_s = owner.capacity.poll_interval_s

    def try_submit_batch(self, tasks, endpoint):
        values = []
        for task in tasks:
            value = self._request(task)
            self.owner._register_local_task(task, value)
            values.append(value)
        # Task bytes and the stage binding are immutable; the owner registration
        # above only reads this parse. Reuse it within this handoff, not a cache.
        outcome = super()._try_submit_batch(tasks, endpoint, values=values)
        if outcome.acceptance == Acceptance.NOT_ACCEPTED:
            with self.owner._receipt_lock:
                for task in tasks:
                    self.owner._contexts.pop(task.key, None)
        return outcome

    def _submit_task(self, task, timings, prepare_method):
        self.owner._register_local_task(task, self._request(task))
        return super()._submit_task(task, timings, prepare_method)

    def recover_actor_receipts(self, receipts):
        errors = super().recover_actor_receipts(receipts)
        # A failed bulk reply may have ended every local result reader. Core
        # can then ACK retirement and remove these transport receipts without
        # _result_transferred running. Transfer the validated audit first.
        return errors + self.transfer_audit_receipts()

    def transfer_audit_receipts(self):
        for key, payload in self.failure_receipts():
            try:
                self.on_result(key, json.loads(payload))
            except BaseException as error:
                with self._lock:
                    pending = self._audit_pending.setdefault(key, dict(ack_deferred=False))
                    pending['error'] = 'actor_close_receipt_sink: '+type(error).__name__
            else:
                with self._lock:
                    pending = self._audit_pending.pop(key, None)
                if pending is not None and pending['ack_deferred']:
                    super().acknowledge(key)
        with self._lock:
            return [v['error'] for v in self._audit_pending.values()]

    def acknowledge(self, key):
        with self._lock:
            pending = self._audit_pending.get(key)
            if pending is not None:
                # Core may retire compute while the same bounded result and
                # registered byte/identity context still await the audit sink.
                pending['ack_deferred'] = True
                return
        super().acknowledge(key)

    def audit_handoff_report(self):
        with self._lock:
            return dict(pending_receipts=len(self._audit_pending),
                deferred_core_acks=sum(v['ack_deferred'] for v in self._audit_pending.values()),
                retained_result_bytes=sum(len(self._outcomes[key]) for key in self._audit_pending),
                registered_requests=sum(key in self._requests for key in self._audit_pending),
                scope='encoded retained audit receipts; not compute ownership or RSS')
