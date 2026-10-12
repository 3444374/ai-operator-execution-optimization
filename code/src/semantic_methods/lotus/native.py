"""LOTUS method batches executed by Daft Native or Ray Data, retaining its SDK."""
from __future__ import annotations

import json
import math
import time

from src.baselines.text.frameworks.native_calls import NativeCall
from src.execution_provider.adapters.full_response import encode_full_response
from .sdk import check_model_config, prepare_call, validate_batch, validate_source


def parse_native_response(lm, messages, all_kwargs, prepared, outcome):
    """Run the original pinned SDK parser through a network-free response client.

    Its send override avoids experiment observers that intercept httpx.Client.send.
    A MockTransport also rejects unexpected access; no second request is submitted.
    """
    validate_source()
    import httpx
    from openai import OpenAI
    from litellm import completion

    sent = []

    def replay(request):
        if (request.method != "POST" or str(request.url) != prepared.api_base + "/chat/completions"
                or json.loads(request.read()) != json.loads(prepared.payload)
                or request.headers.get("authorization") != "Bearer " + prepared.api_key):
            raise ValueError("LOTUS SDK parser request differs from its prepared complete call")
        sent.append(request)
        if len(sent) != 1:
            raise ValueError("LOTUS SDK parser attempted to retry its response")
        if outcome.transport_error:
            error_type = outcome.transport_error.partition(":")[0]
            if error_type in ("TimeoutError", "ConnectionTimeoutError", "SocketTimeoutError", "ServerTimeoutError"):
                raise httpx.ReadTimeout(outcome.transport_error, request=request)
            raise httpx.ConnectError(outcome.transport_error, request=request)
        full = outcome.response
        return httpx.Response(full.status_code, headers=list(full.headers), content=full.body,
            request=request, extensions={"http_version": full.http_version.encode()})

    class ResponseClient(httpx.Client):
        def send(self, request, *args, **kwargs):
            return replay(request)

    with ResponseClient(transport=httpx.MockTransport(replay)) as http:
        client = OpenAI(api_key=prepared.api_key, base_url=prepared.api_base,
                        timeout=prepared.timeout_s, max_retries=0, http_client=http)
        try:
            return completion(model=lm.model, messages=messages, drop_params=True,
                              client=client, **all_kwargs)
        except Exception as error:
            error.full_response = outcome.response
            error.native_transport_error = outcome.transport_error
            raise
        finally:
            client.close()


class LotusNativeBatchExecutor:
    """Replace only the uncached request pool; LM and sem_map stay in LOTUS."""
    def __init__(self, native, model_config, *, query_id, operator_id,
                 max_batch_rows=4096, max_batch_result_bytes=16777216,
                 on_response=None, on_batch=None, on_prepared=None,
                 cancelled=None, timeout_s=120, retain_full_responses=True, follow_redirects=True):
        if any(type(value) is not int or value <= 0 for value in (max_batch_rows, max_batch_result_bytes)):
            raise ValueError("LOTUS native batch limits must be positive integers")
        if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 120:
            raise ValueError("LOTUS native batch timeout must be at most 120 seconds")
        if type(retain_full_responses) is not bool or type(follow_redirects) is not bool:
            raise ValueError("LOTUS raw response retention choice must be boolean")
        self.native, self.model_config = native, model_config
        self.query_id, self.operator_id = query_id, operator_id
        self.max_batch_rows, self.max_batch_result_bytes = max_batch_rows, max_batch_result_bytes
        self.on_response, self.on_batch, self.on_prepared = on_response, on_batch, on_prepared
        self.cancelled, self.timeout_s = cancelled, timeout_s
        self.retain_full_responses = retain_full_responses
        self.follow_redirects = follow_redirects
        self.last_responses, self.last_cleanup_errors = (), ()
        self.last_close_report = None
        self._batch_number = 0

    def __call__(self, lm, uncached_data, all_kwargs, show_progress_bar, progress_bar_desc):
        from openai import OpenAIError
        self.last_responses, self.last_cleanup_errors = (), ()
        self.last_close_report = None
        if len(uncached_data) > self.max_batch_rows:
            raise ValueError("LOTUS uncached batch exceeds declared row limit")
        if not uncached_data:
            return []
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
        for index, (messages, _) in enumerate(uncached_data):
            check_stop()
            prepared = first if index == 0 else prepare_call(lm, messages, all_kwargs)
            check_model_config(prepared, self.model_config)
            if self.on_prepared:
                self.on_prepared(index, prepared)
            prepared_calls.append(prepared)
            calls.append(NativeCall(call_id=f"{self.query_id}:{self.operator_id}:{self._batch_number}:{index}",
                row_id=index, payload=prepared.payload, endpoint=prepared.api_base + "/chat/completions",
                headers=(("Content-Type", "application/json"), ("Authorization", "Bearer " + prepared.api_key)),
                timeout_s=prepared.timeout_s, follow_redirects=self.follow_redirects))
        check_stop()
        responses = [None] * len(calls)
        full = [None] * len(calls) if self.retain_full_responses else []
        received = set()
        result_bytes = 0
        iterator = None
        primary = None
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
                                                             prepared_calls[index], outcome)
                except OpenAIError as error:
                    # LM's original cache/stat/order/choice logic owns this error value.
                    responses[index] = error
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
