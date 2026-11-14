"""Native LOTUS service calls, observation and prepaid query ownership.

The original SDK owns batching and parsing. These helpers configure its LM and
observe transport attempts without starting a service or adding model work.
"""

from contextlib import contextmanager
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import threading
import time

from src.infrastructure.private_artifacts import write_private_json
from src.infrastructure.redact import redact_text


def make_lm(config, *, max_ctx_len=None, tokenizer=None, api_key=None):
    from lotus.models import LM
    from src.serving.backends.vllm_protocol import GenerationSpec

    generation = GenerationSpec.from_dict(config["generation"])
    options = dict(top_p=generation.top_p, stop=list(generation.stop),
        presence_penalty=generation.presence_penalty, frequency_penalty=generation.frequency_penalty,
        api_base=config["api_base"].rstrip("/"), api_key=os.environ[config["api_key_env"]] if api_key is None else api_key,
        timeout=config["query_timeout_s"], num_retries=0, max_retries=0)
    if generation.seed is not None:
        options["seed"] = generation.seed
    if max_ctx_len is not None:
        options["max_ctx_len"] = max_ctx_len
    if tokenizer is not None:
        options["tokenizer"] = tokenizer
    return LM(config["model_name"], temperature=generation.temperature, max_tokens=generation.max_tokens,
        max_batch_size=config["native_lm_max_batch_size"], **options)


@contextmanager
def native_query_settings(lm):
    """Configure one isolated native query, including the SDK's own worker threads.

    ContextVar overrides alone do not enter LOTUS's groupby ThreadPoolExecutor.
    Keep the original configure() globals visible there and restore them after
    the query, while also overriding any outer context in the calling thread.
    """
    import lotus

    previous = {name: object.__getattribute__(lotus.settings, name) for name in ('lm', 'enable_cache')}
    lotus.settings.configure(lm=lm, enable_cache=False)
    try:
        with lotus.settings.context(lm=lm, enable_cache=False):
            yield
    finally:
        lotus.settings.configure(**previous)


def _audit_value(value):
    """Hash inline image bytes in evidence, leaving the actual SDK POST untouched."""
    if isinstance(value, dict):
        if value.get("type") == "image_url":
            image = value["image_url"]
            url = image["url"]
            encoded = url.encode()
            return dict(type="image_url", image_url={k: v for k, v in image.items() if k != "url"},
                url_sha256=hashlib.sha256(encoded).hexdigest(), url_bytes=len(encoded),
                representation="data:image/png;base64" if url.startswith("data:image/png;base64,") else "other")
        return {key: _audit_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_audit_value(item) for item in value]
    return value


class NativeServiceCalls:
    def __init__(self, writer, model_name):
        self.writer, self.model = writer, model_name.removeprefix("openai/")
        self.lock = threading.Lock()
        self.requests, self.succeeded, self.failed, self.finished = set(), set(), set(), set()
        self.lm_calls = []
        self._active_lm_calls = {}
        self.response_ids = {}
        self.sdk_batches = []
        self._responses = []

    def record(self, event):
        attempt, kind = event.get("attempt"), event["event"]
        with self.lock:
            if kind == "request":
                self.requests.add(attempt)
            elif kind == "http_response":
                response = event["response"]
                if type(response) is dict:
                    self.response_ids[attempt] = response.get('id')
                if (event["status"] == 200 and type(response) is dict
                        and response.get("model") == self.model and response.get("choices")):
                    self.succeeded.add(attempt)
                else:
                    self.failed.add(attempt)
            elif kind == "http_finished":
                self.finished.add(attempt)
                if event.get("error_type"):
                    self.failed.add(attempt)
        self.writer.record(_audit_value(event))

    def record_response(self, event, content):
        """Retain the already-read immutable response bytes, without parsing or encoding."""
        with self.lock:
            self._responses.append((event, content))

    def _flush_responses(self):
        with self.lock:
            pending, self._responses = self._responses, []
        failure = None
        for event, content in pending:
            try:
                try:
                    value = json.loads(content)
                except ValueError:
                    value = None
                self.record(dict(event, response=value))
            except BaseException as error:
                if failure is None:
                    failure = error
        if failure is not None:
            raise failure

    @contextmanager
    def observe_lm(self, lm, *, kind=None):
        lm_type = type(lm)
        original = lm_type.__call__
        native_uncached = lm._process_uncached_messages
        missing = object()
        previous_uncached = lm.__dict__.get('_process_uncached_messages', missing)
        current_call = threading.local()

        def ordered_responses(data, *args, **kwargs):
            responses = native_uncached(data, *args, **kwargs)
            # The native SDK consumes these responses in uncached input order.
            # Retain only IDs/counts here; reconciliation and writing follow sink.
            with self.lock:
                self.sdk_batches.append((current_call.index, len(data),
                    tuple(getattr(response, 'id', None) for response in responses)))
            return responses

        def observed(current, messages, *args, **kwargs):
            if current is not lm:
                raise RuntimeError("the original " + (kind or "visual") + " query invoked another model")
            stage = dict(input_occurrences=len(messages),
                kind=("extract" if kwargs.get("response_format") == {"type": "json_object"} else kind or "filter_or_join"))
            with self.lock:
                before = len(self.requests)
                index = len(self.lm_calls)
                self.lm_calls.append(stage)
                if self._active_lm_calls:
                    for active in (*self._active_lm_calls.values(), stage):
                        active['post_attempts_unavailable_reason'] = 'overlapping native LM calls; global request delta is not per-call attribution'
                self._active_lm_calls[index] = stage
            current_call.index = index
            try:
                return original(current, messages, *args, **kwargs)
            finally:
                with self.lock:
                    stage["post_attempts"] = (None if 'post_attempts_unavailable_reason' in stage
                                              else len(self.requests) - before)
                    del self._active_lm_calls[index]

        lm_type.__call__ = observed
        lm._process_uncached_messages = ordered_responses
        try:
            yield
        finally:
            lm_type.__call__ = original
            if previous_uncached is missing:
                del lm._process_uncached_messages
            else:
                lm._process_uncached_messages = previous_uncached

    def record_sdk_responses(self):
        """Link SDK return slots to physical POSTs without matching prompt text."""
        self._flush_responses()
        with self.lock:
            batches = list(self.sdk_batches)
            ids = {attempt: value for attempt, value in self.response_ids.items() if attempt in self.succeeded}
            succeeded = len(self.succeeded)
        http_counts = Counter(ids.values())
        sdk_counts = Counter(value for _, _, values in batches for value in values)
        attempts = {value: attempt for attempt, value in ids.items() if value is not None and http_counts[value] == 1}
        linked = unlinked = 0
        complete = len(batches) == len(self.lm_calls)
        for call, count, values in sorted(batches):
            complete = complete and count == len(values) == self.lm_calls[call]['input_occurrences']
            for position, value in enumerate(values):
                attempt = attempts.get(value) if value is not None and sdk_counts[value] == 1 else None
                linked += attempt is not None
                unlinked += attempt is None
                self.writer.record(dict(event='native_sdk_response',lm_call_index=call,
                    uncached_position=position,response_id=value,http_attempt=attempt,
                    monotonic_ns=time.monotonic_ns(),phase='post_query_audit'))
        available = complete and not unlinked and linked == succeeded
        return dict(scope='original LOTUS uncached response order linked by unique HTTP completion ID; '
                         'no input-text or completion-order inference',
            available=available,completed_batches=len(batches),native_result_slots_complete=complete,
            expected_occurrences=sum(stage['input_occurrences'] for stage in self.lm_calls),
            successful_http_responses=succeeded,
            linked_occurrences=linked,unlinked_occurrences=unlinked,
            unavailable_reason=None if available else
                'missing/duplicate completion IDs or incomplete native SDK result slots')

    def snapshot(self):
        self._flush_responses()
        with self.lock:
            return dict(attempts=len(self.requests), succeeded=len(self.succeeded), failed=len(self.failed),
                unknown=len(self.requests - self.succeeded - self.failed),
                unfinished=len(self.requests - self.finished))


def run_prepaid_query(config, checkout, directory, query, *, query_role, **inputs):
    from .attempt_ledger import AttemptBudget
    from .cell_budget import CellBudgetLedger

    if not os.environ.get(config["api_key_env"]):
        raise ValueError("set the declared api_key_env before claiming a " + query_role + " query unit")
    value = config["budget"]
    ledger = CellBudgetLedger(Path(value["path"]), AttemptBudget(value["id"], value["limit"]))
    try:
        claim = ledger.claim_unit(value["unit_id"])
        claim.bind_execution()
        report = query(config, checkout, directory, budget=claim, **inputs)
    except BaseException as error:
        report = getattr(error, "query_report", dict(status="failed", error_type=type(error).__name__,
            error=redact_text(str(error)), model_calls=0))
        report["permanent_budget"] = ledger.snapshot()
        write_private_json(Path(directory) / "query.json", report)
        raise
    report["permanent_budget"] = ledger.snapshot()
    write_private_json(Path(directory) / "query.json", report)
    return report
