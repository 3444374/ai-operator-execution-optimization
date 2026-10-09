"""The uncached LOTUS batch selects its executor before native request scheduling."""

from contextlib import contextmanager
from types import MethodType
import time
import math

from ...execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task
from ...scheduling.core.session_contract import State
from ...execution_provider.adapters.full_response import decode_full_response
from .sdk import check_model_config, lotus_response, prepare_call, validate_source


class LotusBatchExecutor:
    """A bounded synchronous LOTUS consumer over the existing shared task Engine.

    The caller owns and closes execution. This adapter opens and closes one flow per
    uncached batch; cached values and the full-frame result wait remain in LOTUS.
    """

    def __init__(self, execution, model_config, *, query_id, operator_id,
                 max_batch_rows=4096, max_batch_result_bytes=16777216,
                 on_response=None, on_batch=None, cancelled=None, timeout_s=120):
        if any(type(v) is not int or v <= 0 for v in (max_batch_rows, max_batch_result_bytes)):
            raise ValueError("LOTUS batch limits must be positive integers")
        if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 120:
            raise ValueError("LOTUS batch timeout must be at most 120 seconds")
        self.execution, self.model_config = execution, model_config
        self.query_id, self.operator_id = query_id, operator_id
        self.max_batch_rows, self.max_batch_result_bytes = max_batch_rows, max_batch_result_bytes
        self.on_response, self.on_batch, self.cancelled = on_response, on_batch, cancelled
        self.timeout_s, self.last_responses = timeout_s, ()

    def __call__(self, lm, uncached_data, all_kwargs, show_progress_bar, progress_bar_desc):
        from openai import OpenAIError
        if len(uncached_data) > self.max_batch_rows:
            raise ValueError("LOTUS uncached batch exceeds declared row limit")
        self.last_responses = ()
        if not uncached_data:
            return []
        # Observe the original whole uncached batch before any physical offer or wait.
        if self.on_batch:
            self.on_batch(uncached_data, all_kwargs)
        flow = NativeTaskSession(self.execution, self.query_id, self.operator_id)
        count = len(uncached_data)
        responses, full = [None] * count, [None] * count
        next_input = completed = result_bytes = 0
        pending = ()
        sealed = False
        clean = False
        started = time.monotonic()
        try:
            while True:
                if self.cancelled and self.cancelled():
                    flow.request_cancel()
                    raise RuntimeError("LOTUS batch cancelled")
                if time.monotonic() - started >= self.timeout_s:
                    flow.request_cancel()
                    raise TimeoutError("LOTUS batch deadline exceeded")
                if not pending and next_input < count:
                    size = min(flow.limits.offer_tasks, flow.limits.held_tasks, count - next_input)
                    offered = []
                    for sequence in range(next_input, next_input + size):
                        call = prepare_call(lm, uncached_data[sequence][0], all_kwargs)
                        check_model_config(call, self.model_config)
                        offered.append(prepare_native_task(call.payload, sequence, row_sequence=sequence,
                            call_id=self.operator_id, max_result_bytes=flow.limits.item_result_bytes))
                    pending = tuple(offered)
                if pending:
                    outcome = flow.offer(pending)
                    if outcome.accepted_prefix_count:
                        next_input += outcome.accepted_prefix_count
                        pending = pending[outcome.accepted_prefix_count:]
                    elif outcome.status != "BACKPRESSURE":
                        raise RuntimeError("LOTUS task offer rejected: " + str(outcome.reason))
                if next_input == count and not sealed:
                    flow.end_input()
                    sealed = True
                progress = flow.advance(flow.limits.held_tasks)
                try:
                    for delivery in progress.deliveries:
                        index = delivery.key.sequence
                        if delivery.status != "completed":
                            raise RuntimeError("LOTUS model transport did not complete")
                        if not 0 <= index < count or responses[index] is not None:
                            raise RuntimeError("LOTUS completion association differs")
                        raw_response = decode_full_response(delivery.result)
                        if self.on_response:
                            self.on_response(index, raw_response)
                        result_bytes += len(delivery.result)
                        if result_bytes > self.max_batch_result_bytes:
                            error = ValueError("LOTUS retained batch responses exceed declared byte limit")
                            error.full_response = raw_response
                            raise error
                        full[index] = raw_response
                        try:
                            responses[index] = lotus_response(full[index])
                        except OpenAIError as error:
                            responses[index] = error
                        completed += 1
                finally:
                    if progress.deliveries:
                        flow.release(tuple(d.lease_id for d in progress.deliveries))
                if progress.state in (State.FAILED, State.CANCELLED):
                    raise RuntimeError(progress.error or "LOTUS execution stopped")
                if progress.state == State.FINISHED:
                    if completed != count:
                        raise RuntimeError("LOTUS batch completion count differs")
                    clean = True
                    return responses
                flow.wait(progress)
        finally:
            self.last_responses = tuple(full)
            flow.close(clean=clean)


@contextmanager
def lotus_executor(lm, executor=None):
    """Select per LM instance; None runs the original LOTUS/LiteLLM implementation."""
    if "_process_uncached_messages" in lm.__dict__:
        raise ValueError("LOTUS instance already has an uncached-batch override")
    if executor is None:
        yield lm
        return
    validate_source()
    def selected(instance, uncached_data, all_kwargs, show_progress_bar, progress_bar_desc):
        return executor(instance, uncached_data, all_kwargs, show_progress_bar, progress_bar_desc)
    lm._process_uncached_messages = MethodType(selected, lm)
    try:
        yield lm
    finally:
        del lm._process_uncached_messages
