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
from src.serving.backends.vllm_engine import GenerationSpec, IDENTITY_FIELDS, _prepared
from .lotus_records_core import _RecordTransport, _check_result_capacity


class LotusChainActorBackend(_RecordTransport):
    def __init__(self, actors, maximum, notify, *, deadline_monotonic, on_result, token_budget=None, bulk=False):
        if not math.isfinite(deadline_monotonic) or deadline_monotonic <= 0:
            raise ValueError("a finite request deadline is required")
        if not callable(on_result):
            raise TypeError("an owner-held receipt sink is required")
        if token_budget is not None:
            from .lotus_chain_token_budget import ChainTokenBudget
            if type(token_budget) is not ChainTokenBudget or token_budget.engine != actors.engine:
                raise ValueError("the token configuration must belong to the model owner")
        self.token_budget, self.on_result = token_budget, on_result
        super().__init__(actors, maximum, notify, finalize=self._finish_batches if bulk else None)
        self.deadline = deadline_monotonic
        self.supports_batch_submit = bulk
        self._bulk = {}
        self.data_batch_rpcs = self.peak_result_frame_bytes = 0

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
        # Backend close calls this only after all member executions stopped.
        self._bulk.clear()

    def _retire_batch(self, group, context):
        if context['driver_done'] and not context['futures']:
            self._bulk.pop(group, None)

    def try_submit_batch(self, tasks, endpoint):
        if not self.supports_batch_submit or not tasks or any(t.member is None for t in tasks):
            raise ValueError('bulk handoff requires an organized prepared batch')
        group = tasks[0].member.batch_key
        if (group in self._bulk or any(t.member.batch_key != group for t in tasks)
                or tuple(t.member.index for t in tasks) != tuple(range(len(tasks)))):
            raise ValueError('bulk handoff changed ordered membership')
        values = [self._request(t) for t in tasks]
        context = dict(tasks={t.key: t for t in tasks}, values={t.key: v for t, v in zip(tasks, values)},
                       members=frozenset(t.key for t in tasks), futures={t.key: Future() for t in tasks},
                       started=False, driver_done=False)
        self._bulk[group] = context
        with self._lock:
            for task, value in zip(tasks, values):
                self._requests[task.key] = {n: value['row'][n] for n in IDENTITY_FIELDS}, task.task.max_result_bytes
                self._refs[task.key] = (value['row'],)
        outcome = self.backend.try_submit_batch(tasks, endpoint)
        if outcome.acceptance == Acceptance.NOT_ACCEPTED:
            del self._bulk[group]
            with self._lock:
                for task in tasks:
                    self._requests.pop(task.key, None); self._refs.pop(task.key, None)
        return outcome

    async def _drive_batch(self, group, context):
        try:
            calls = [dict(row=context['values'][t.key]['row'], generation=context['values'][t.key]['generation'],
                          maximum=t.task.max_result_bytes, request_key=[t.key.session_id, t.key.sequence])
                     for t in context['tasks'].values()]
            key = (group.session_id, group.sequence)
            ref = self.actors.submit_prepared_batch(calls, key, self.deadline)
            self.data_batch_rpcs += 1
            calls = None
            context['tasks'].clear()
            context['values'].clear()
            response = await asyncio.wrap_future(ref.future())
            pending = set(context['members'])
            while pending:
                pending = {member for member in pending if (future := context['futures'].get(member)) is not None
                           and not future.done()}
                if not pending:
                    break
                if response['status'] == 'accepted':
                    ref = self.actors.poll_prepared_batch(key)
                    raw = await asyncio.wrap_future(ref.future())
                    self.peak_result_frame_bytes = max(self.peak_result_frame_bytes, len(raw))
                    response = json.loads(raw)
                for receipt in response.get('results', ()):
                    task_key = TaskKey(*receipt['request_key'])
                    if task_key not in context['members']:
                        raise ValueError('bulk result belongs to another group')
                    if task_key not in pending and task_key not in context['futures']:
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
                if not pending:
                    break
                if response.get('closed') and type(response.get('more_results')) is not bool:
                    raise ValueError('closed bulk reply has no bounded-page frontier')
                if (response.get('closed') and not response['more_results']
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
        if task.member is not None:
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
