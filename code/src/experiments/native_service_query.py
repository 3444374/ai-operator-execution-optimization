"""Native LOTUS service calls, observation and prepaid query ownership.

The original SDK owns batching and parsing. These helpers configure its LM and
observe transport attempts without starting a service or adding model work.
"""

from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import threading

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


def _audit_value(value):
    """Hash inline image bytes in evidence, leaving the actual SDK POST untouched."""
    if isinstance(value, dict):
        if value.get("type") == "image_url":
            image = value["image_url"]
            url = image["url"]
            return dict(type="image_url", image_url={k: v for k, v in image.items() if k != "url"},
                url_sha256=hashlib.sha256(url.encode()).hexdigest(), url_bytes=len(url.encode()),
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

    def record(self, event):
        attempt, kind = event.get("attempt"), event["event"]
        with self.lock:
            if kind == "request":
                self.requests.add(attempt)
            elif kind == "http_response":
                response = event["response"]
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

    @contextmanager
    def observe_lm(self, lm, *, kind=None):
        lm_type = type(lm)
        original = lm_type.__call__

        def observed(current, messages, *args, **kwargs):
            if current is not lm:
                raise RuntimeError("the original " + (kind or "visual") + " query invoked another model")
            before = len(self.requests)
            stage = dict(input_occurrences=len(messages),
                kind=kind or ("extract" if kwargs.get("response_format") == {"type": "json_object"} else "filter_or_join"))
            self.lm_calls.append(stage)
            try:
                return original(current, messages, *args, **kwargs)
            finally:
                stage["post_attempts"] = len(self.requests) - before

        lm_type.__call__ = observed
        try:
            yield
        finally:
            lm_type.__call__ = original

    def snapshot(self):
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
