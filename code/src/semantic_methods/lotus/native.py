"""LOTUS method batches executed by Daft Native or Ray Data, retaining its SDK."""
from __future__ import annotations

import math
import time

from src.baselines.text.frameworks.native_calls import NativeCall, raise_execution_error
from src.execution_provider.adapters.full_response import encode_full_response
from .sdk import LotusResponseReplay, check_model_config, parse_lotus_response, prepare_call, validate_batch
from .limits import LotusCallLimits


def parse_native_response(lm, messages, all_kwargs, prepared, outcome, *, replay=None):
    """Compatibility entry; every replacement batch uses the same SDK replay."""
    raise_execution_error(outcome)
    if replay is None:
        return parse_lotus_response(lm, messages, all_kwargs, prepared,
                                    outcome.response, outcome.transport_error)
    return parse_lotus_response(lm, messages, all_kwargs, prepared,
                                outcome.response, outcome.transport_error, replay=replay)


class LotusNativeBatchExecutor:
    """Replace only the uncached request pool; LM and sem_map stay in LOTUS."""
    def __init__(self, native, model_config, *, query_id, operator_id,
                 max_batch_rows=4096, max_batch_result_bytes=16777216,
                 on_response=None, on_batch=None, on_prepared=None,
                 cancelled=None, timeout_s=120, retain_full_responses=True, follow_redirects=True,
                 call_limits=None):
        if any(type(value) is not int or value <= 0 for value in (max_batch_rows, max_batch_result_bytes)):
            raise ValueError("LOTUS native batch limits must be positive integers")
        if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 120:
            raise ValueError("LOTUS native batch timeout must be at most 120 seconds")
        if type(retain_full_responses) is not bool or type(follow_redirects) is not bool:
            raise ValueError("LOTUS raw response retention choice must be boolean")
        self.native, self.model_config = native, model_config
        if call_limits is not None and type(call_limits) is not LotusCallLimits:
            raise TypeError('LOTUS call_limits requires LotusCallLimits')
        self.call_limits = call_limits or LotusCallLimits(model_config.timeout_ms / 1000,
            max_batch_rows=max_batch_rows, max_batch_response_bytes=max_batch_result_bytes, batch_timeout_s=timeout_s)
        self.call_limits.check_model(model_config)
        self.query_id, self.operator_id = query_id, operator_id
        self.max_batch_rows = self.call_limits.max_batch_rows
        self.max_batch_result_bytes = self.call_limits.max_batch_response_bytes
        self.on_response, self.on_batch, self.on_prepared = on_response, on_batch, on_prepared
        self.cancelled, self.timeout_s = cancelled, self.call_limits.batch_timeout_s
        self.retain_full_responses = retain_full_responses
        self.follow_redirects = follow_redirects
        self.last_responses, self.last_cleanup_errors = (), ()
        self.last_close_report = None
        self._batch_number = 0
        self.last_preflight = None

    def __call__(self, lm, uncached_data, all_kwargs, show_progress_bar, progress_bar_desc):
        self.last_responses, self.last_cleanup_errors = (), ()
        self.last_close_report = None
        native_rows = getattr(self.native, 'max_batch_rows', self.max_batch_rows)
        native_bytes = getattr(self.native, 'max_batch_bytes', self.call_limits.max_batch_input_bytes)
        self.last_preflight = dict(target_rows=len(uncached_data), native_max_batch_rows=native_rows,
                                   effective_max_batch_rows=min(native_rows, self.max_batch_rows),
                                   native_max_batch_input_bytes=native_bytes,
                                   call_limits=self.call_limits.describe())
        self.call_limits.preflight_rows(len(uncached_data), native_rows=native_rows)
        if not uncached_data:
            return []
        from openai import OpenAIError
        if all_kwargs.get("num_retries") != 0 or all_kwargs.get("max_retries") != 0:
            raise ValueError("LOTUS native execution requires explicit zero retries")
        import litellm
        if litellm.cache is not None:
            raise ValueError("LOTUS native execution requires an independently specified LiteLLM cache policy")
        deadline = time.monotonic() + self.timeout_s

        def check_stop():
            if self.cancelled and self.cancelled():
                raise InterruptedError("LOTUS native batch cancelled")
            if time.monotonic() >= deadline:
                raise TimeoutError("LOTUS native batch deadline exceeded")

        check_stop()
        if self.on_batch:
            self.on_batch(uncached_data, all_kwargs)
        first = validate_batch(lm, uncached_data, all_kwargs, check_stop=check_stop)
        self._batch_number += 1
        prepared_calls = []
        calls = []
        input_bytes = 0
        for index, (messages, _) in enumerate(uncached_data):
            check_stop()
            prepared = first if index == 0 else prepare_call(lm, messages, all_kwargs)
            check_model_config(prepared, self.model_config)
            self.call_limits.check_prepared(prepared)
            input_bytes += len(prepared.payload)
            if input_bytes > min(self.call_limits.max_batch_input_bytes,
                                 getattr(self.native, 'max_batch_bytes', self.call_limits.max_batch_input_bytes)):
                raise ValueError('LOTUS prepared batch input exceeds the selected executor byte capacity')
            if self.on_prepared:
                self.on_prepared(index, prepared)
            prepared_calls.append(prepared)
            calls.append(NativeCall(call_id=f"{self.query_id}:{self.operator_id}:{self._batch_number}:{index}",
                row_id=index, payload=prepared.payload, endpoint=prepared.api_base + "/chat/completions",
                headers=(("Content-Type", "application/json"), ("Authorization", "Bearer " + prepared.api_key)),
                timeout_s=prepared.timeout_s, follow_redirects=self.follow_redirects,
                max_response_bytes=self.call_limits.max_response_bytes))
        check_stop()
        responses = [None] * len(calls)
        full = [None] * len(calls) if self.retain_full_responses else []
        received = set()
        result_bytes = 0
        iterator = None
        primary = None
        replay = LotusResponseReplay()
        try:
            iterator = self.native.iter_batch(tuple(calls), cancelled=self.cancelled,
                                              timeout_s=deadline - time.monotonic())
            for outcome in iterator:
                check_stop()
                index = outcome.row_id
                if (not 0 <= index < len(calls) or index in received
                        or outcome.call_id != calls[index].call_id):
                    raise ValueError("LOTUS native response changed its occurrence identity")
                received.add(index)
                raw = outcome.response
                if raw is not None:
                    if self.on_response:
                        self.on_response(index, raw)
                    result_bytes += len(encode_full_response(raw))
                    if result_bytes > self.max_batch_result_bytes:
                        error = ValueError("LOTUS retained batch responses exceed declared byte limit")
                        error.full_response = raw
                        raise error
                    if self.retain_full_responses:
                        full[index] = raw
                try:
                    responses[index] = parse_native_response(lm, uncached_data[index][0], all_kwargs,
                                                             prepared_calls[index], outcome, replay=replay)
                except OpenAIError as error:
                    # LM's original cache/stat/order/choice logic owns this error value.
                    responses[index] = error
                check_stop()
            if len(received) != len(calls):
                raise ValueError("LOTUS native batch response is incomplete")
            return responses
        except BaseException as error:
            primary = error
            raise
        finally:
            self.last_responses = tuple(full)
            try:
                if iterator is not None:
                    iterator.close()
            except BaseException as error:
                self.last_cleanup_errors = (("close", error),)
                if primary is None:
                    raise
                primary.add_note("LOTUS native graph close also failed: " + type(error).__name__)
            finally:
                self.last_close_report = self.native.last_close_report
                seen = {id(error) for _, error in self.last_cleanup_errors}
                self.last_cleanup_errors += tuple((stage, error) for stage, error in
                    getattr(self.native, "last_cleanup_errors", ()) if id(error) not in seen)
                try:
                    replay.close()
                except BaseException as error:
                    self.last_cleanup_errors += (("parser_close", error),)
                    if primary is None and len(self.last_cleanup_errors) == 1:
                        raise
                    if primary is not None:
                        primary.add_note("LOTUS SDK client close also failed: " + type(error).__name__)
