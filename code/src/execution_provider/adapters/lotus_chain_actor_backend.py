"""Prepared F/M continuations through the existing Core transport and async actor.

The caller owns actor allocation, deadlines, close and durable receipt storage.
No second admission counter or inference queue is introduced here.
"""

from copy import deepcopy
import json
import math

from src.scheduling.core.session_contract import Terminal
from src.serving.backends.vllm_engine import GenerationSpec, IDENTITY_FIELDS, _prepared
from .lotus_records_core import _RecordTransport, _check_result_capacity


class LotusChainActorBackend(_RecordTransport):
    def __init__(self, actors, maximum, notify, *, deadline_monotonic, on_result, token_budget=None):
        if not math.isfinite(deadline_monotonic) or deadline_monotonic <= 0:
            raise ValueError("a finite request deadline is required")
        if not callable(on_result):
            raise TypeError("an owner-held receipt sink is required")
        if token_budget is not None:
            from .lotus_chain_token_budget import ChainTokenBudget
            if type(token_budget) is not ChainTokenBudget or token_budget.engine != actors.engine:
                raise ValueError("the token configuration must belong to the model owner")
        self.token_budget, self.on_result = token_budget, on_result
        super().__init__(actors, maximum, notify)
        self.deadline = deadline_monotonic

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
        self.on_result(task.key, receipt)

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
