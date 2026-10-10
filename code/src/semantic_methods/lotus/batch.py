"""The uncached LOTUS batch selects its executor before native request scheduling."""

from contextlib import contextmanager
from types import MethodType
import time
import math

from ...execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task
from ...scheduling.core.session_contract import State
from ...execution_provider.adapters.full_response import decode_full_response
from .sdk import check_model_config, lotus_response, prepare_call, validate_batch, validate_source


class LotusBatchExecutor:
    """A bounded synchronous LOTUS consumer over the existing shared task Engine.

    The caller owns and closes execution. This adapter opens and closes one flow per
    uncached batch; cached values and the full-frame result wait remain in LOTUS.
    """

    def __init__(self, execution, model_config, *, query_id, operator_id,
                 max_batch_rows=4096, max_batch_result_bytes=16777216,
                 on_response=None, on_batch=None, cancelled=None, timeout_s=120,
                 on_prepared=None, prevalidate_batch=False, retain_full_responses=True):
        if any(type(v) is not int or v <= 0 for v in (max_batch_rows, max_batch_result_bytes)):
            raise ValueError("LOTUS batch limits must be positive integers")
        if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 120:
            raise ValueError("LOTUS batch timeout must be at most 120 seconds")
        if type(prevalidate_batch) is not bool or type(retain_full_responses) is not bool:
            raise ValueError("LOTUS preparation and retention choices must be boolean")
        self.execution, self.model_config = execution, model_config
        self.query_id, self.operator_id = query_id, operator_id
        self.max_batch_rows, self.max_batch_result_bytes = max_batch_rows, max_batch_result_bytes
        self.on_response, self.on_batch, self.cancelled = on_response, on_batch, cancelled
        self.on_prepared = on_prepared
        self.prevalidate_batch, self.retain_full_responses = prevalidate_batch, retain_full_responses
        self.timeout_s, self.last_responses = timeout_s, ()
        self.last_cleanup_errors = ()

    def __call__(self, lm, uncached_data, all_kwargs, show_progress_bar, progress_bar_desc):
        from openai import OpenAIError
        if len(uncached_data) > self.max_batch_rows:
            raise ValueError("LOTUS uncached batch exceeds declared row limit")
        self.last_responses = ()
        self.last_cleanup_errors = ()
        if not uncached_data:
            return []
        count = len(uncached_data)
        flow = None
        full = []
        next_input = completed = result_bytes = 0
        pending = ()
        sealed = False
        clean = False
        deadline = time.monotonic() + self.timeout_s
        primary = None
        def check_stop():
            if self.cancelled and self.cancelled():
                if flow is not None:
                    flow.request_cancel()
                raise RuntimeError("LOTUS batch cancelled")
            if time.monotonic() >= deadline:
                if flow is not None:
                    flow.request_cancel()
                raise TimeoutError("LOTUS batch deadline exceeded")
        try:
            check_stop()
            # Observe the original whole uncached batch before any physical offer or wait.
            if self.on_batch:
                self.on_batch(uncached_data, all_kwargs)
            check_stop()
            first_call = validate_batch(lm, uncached_data, all_kwargs,
                check_stop=check_stop) if self.prevalidate_batch else None
            check_stop()
            flow = NativeTaskSession(self.execution, self.query_id, self.operator_id)
            responses = [None] * count
            full = [None] * count if self.retain_full_responses else []
            while True:
                check_stop()
                if not pending and next_input < count:
                    size = min(flow.limits.offer_tasks, flow.limits.held_tasks, count - next_input)
                    offered = []
                    for sequence in range(next_input, next_input + size):
                        check_stop()
                        call = first_call if sequence == 0 and first_call is not None else prepare_call(lm, uncached_data[sequence][0], all_kwargs)
                        first_call = None
                        check_model_config(call, self.model_config)
                        if self.on_prepared:
                            self.on_prepared(sequence, call)
                        offered.append(prepare_native_task(call.payload, sequence, row_sequence=sequence,
                            call_id=self.operator_id, max_result_bytes=flow.limits.item_result_bytes))
                    pending = tuple(offered)
                if pending:
                    check_stop()
                    outcome = flow.offer(pending)
                    if outcome.accepted_prefix_count:
                        next_input += outcome.accepted_prefix_count
                        pending = pending[outcome.accepted_prefix_count:]
                    elif outcome.status != "BACKPRESSURE":
                        raise RuntimeError("LOTUS task offer rejected: " + str(outcome.reason))
                if next_input == count and not sealed:
                    flow.end_input()
                    sealed = True
                check_stop()
                progress = flow.advance(flow.limits.held_tasks)
                delivery_error = None
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
                        if self.retain_full_responses:
                            full[index] = raw_response
                        try:
                            responses[index] = lotus_response(raw_response)
                        except OpenAIError as error:
                            responses[index] = error
                        completed += 1
                except BaseException as error:
                    delivery_error = error
                    raise
                finally:
                    try:
                        flow.release(tuple(d.lease_id for d in progress.deliveries))
                    except BaseException as error:
                        self.last_cleanup_errors += (("release", error),)
                        if delivery_error is None:
                            raise
                        delivery_error.add_note("LOTUS result release also failed: " + type(error).__name__)
                if progress.state in (State.FAILED, State.CANCELLED):
                    raise RuntimeError(progress.error or "LOTUS execution stopped")
                if progress.state == State.FINISHED:
                    if completed != count:
                        raise RuntimeError("LOTUS batch completion count differs")
                    clean = True
                    return responses
                flow.wait(progress)
        except BaseException as error:
            primary = error
            raise
        finally:
            self.last_responses = tuple(full)
            try:
                if flow is not None:
                    flow.close(clean=clean)
            except BaseException as error:
                self.last_cleanup_errors += (("close", error),)
                if primary is None:
                    raise
                primary.add_note("LOTUS flow close also failed: " + type(error).__name__)


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
