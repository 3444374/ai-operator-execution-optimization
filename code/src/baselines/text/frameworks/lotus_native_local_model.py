"""Original SemBench Movie task / LOTUS LM over a query-owned local vLLM HTTP service.

LOTUS and LiteLLM retain scheduling, concurrency, accounting and parsing. The
batch_completion wrapper only observes its ordered return; it calls the original
once with identical arguments and returns the same object. No uncached LM hook,
Core, Ray, private request pool or embedded-engine substitution is used.

The controller owns the host lease, permanent query claim, wall/resource limits
and independent GPU-exit check. This adapter spends that claim before each POST
and closes only its own HTTP client and observed, birth-matched server processes.
Each query is cold: model/service startup stays inside query latency. HTTP and
embedded chat are distinct protocols; runtime model/quality qualification remains
necessary. CPU tests use an HTTP transport fixture and never load a model.
"""

from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import threading
import time
import uuid

from src.baselines.common.private_artifacts import (
    content_digest, file_identity, json_metric, new_private_directory,
    require_outside_git, write_private_json,
)
from src.baselines.common.redact import redact_argument_list, redact_json_values, redact_text
from src.baselines.text.frameworks.lotus_pg import observe_filter_rows
from src.baselines.text.movie_records import MovieRecordSource, validate_movie_method
from src.baselines.text.sembench_movie import (
    classification_audit, evaluate_original, run_original_lotus_query, verify_checkout,
)
from src.experiments.cell_budget import ClaimedUnit
from src.experiments.native_http_observer import _observe_native_httpx_client
from src.semantic_methods.lotus.engine import (
    lm_report, make_lm, validate_filter_source, validate_generation,
)
from src.semantic_methods.lotus.records import CallLedger, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, VLLM_VERSION, _prepared


class LocalService:
    """One native server process tree, without inference scheduling or retries."""

    def __init__(self, engine, model, resources, directory, cuda_device):
        self.engine, self.model, self.resources = engine, model, resources
        self.directory, self.cuda_device = Path(directory), cuda_device
        self.process = self.created = self.port = None
        self.owned = set()
        self.ready_report = self.last_close_report = None
        self.engine_uid = uuid.uuid4().hex
        self._stop = threading.Event()
        self._monitor = None
        self._monitor_errors = []
        self._owned_lock = threading.RLock()

    def command(self):
        e = self.engine
        command = [e.model_python, "-m", "vllm.entrypoints.openai.api_server",
            "--host", "127.0.0.1", "--port", str(self.port),
            "--model", e.model_path, "--tokenizer", e.tokenizer_path or e.model_path,
            "--served-model-name", self.model, "--dtype", e.dtype,
            "--max-model-len", str(e.max_model_len), "--max-num-seqs", str(e.max_num_seqs),
            "--max-num-batched-tokens", str(e.max_num_batched_tokens),
            "--gpu-memory-utilization", str(e.gpu_memory_utilization), "--seed", str(e.seed),
            "--tensor-parallel-size", "1", "--pipeline-parallel-size", "1",
            "--distributed-executor-backend", "uni", "--generation-config", "vllm",
            "--no-enable-log-requests"]
        if e.chat_template is not None:
            command += ["--chat-template", e.chat_template]
        return command

    def _artifacts(self):
        tokenizer = Path(self.engine.tokenizer_path or self.engine.model_path)
        result = {name: file_identity(path) for name, path in (
            ("model_config", Path(self.engine.model_path) / "config.json"),
            ("tokenizer_config", tokenizer / "tokenizer_config.json"),
            ("tokenizer", tokenizer / "tokenizer.json"))}
        template = self.engine.chat_template
        if template is None:
            template = json.loads((tokenizer / "tokenizer_config.json").read_text())["chat_template"]
        else:
            try:
                if Path(template).is_file():
                    template = Path(template).read_text()
            except OSError:
                pass  # An inline Jinja template may exceed a filesystem name limit.
        if type(template) is not str or not template:
            raise ValueError("the local HTTP service requires one explicit text chat template")
        result["chat_template_sha256"] = hashlib.sha256(template.encode()).hexdigest()
        return result

    def _observe(self):
        from src.experiments.lotus_records_supervisor import _observe
        with self._owned_lock:
            _observe(self.owned, self.process.pid, self.created)

    def _listen_identity(self):
        """Never contact a listener unless its process birth belongs to this owner."""
        import psutil
        self._observe()
        listeners = [row for row in psutil.net_connections(kind="tcp")
                     if row.status == psutil.CONN_LISTEN and row.laddr.port == self.port]
        if not listeners:
            return None
        identities = []
        for row in listeners:
            if row.pid is None or row.laddr.ip != "127.0.0.1":
                raise RuntimeError("local service port is occupied by an unverified listener")
            process = psutil.Process(row.pid)
            identity = (row.pid, process.create_time())
            with self._owned_lock:
                if identity not in self.owned:
                    raise RuntimeError("local service port is occupied by a foreign process")
            identities.append(dict(pid=identity[0], create_time=identity[1]))
        # The listener may be a child, but the launched parent's exact model,
        # template and capacity arguments must still match the owned command.
        actual = psutil.Process(self.process.pid).cmdline()
        if actual[1:] != self.command()[1:]:
            raise RuntimeError("owned service command/model/template/capacity changed")
        return identities

    def _watch(self):
        while not self._stop.wait(0.05):
            try:
                self._observe()
            except BaseException as error:
                self._monitor_errors.append(type(error).__name__)
                return

    def verify_listener(self):
        if self.process.poll() is not None or self._monitor_errors or not self._listen_identity():
            raise RuntimeError("native HTTP listener is no longer a verified live owned service")

    def start(self):
        import httpx
        import psutil
        started = time.monotonic()
        artifacts = self._artifacts()
        # Release-before-bind is intentionally fail-closed: readiness below
        # verifies the actual listener's birth identity before any HTTP access.
        with socket.socket() as selection:
            selection.bind(("127.0.0.1", 0))
            self.port = selection.getsockname()[1]
        self.endpoint = f"http://127.0.0.1:{self.port}/v1/chat/completions"
        environment = dict(os.environ, CUDA_VISIBLE_DEVICES=self.cuda_device, CUDA_DEVICE_ORDER="PCI_BUS_ID",
            HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1", HF_HUB_DISABLE_TELEMETRY="1",
            VLLM_NO_USAGE_STATS="1", DO_NOT_TRACK="1")
        self.process = subprocess.Popen(self.command(), stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True, env=environment)
        self.created = psutil.Process(self.process.pid).create_time()
        self.owned.add((self.process.pid, self.created))
        self._monitor = threading.Thread(target=self._watch, daemon=True, name="lotus-local-owner-observer")
        self._monitor.start()
        deadline = started + self.resources.startup_timeout_s
        with httpx.Client(trust_env=False, follow_redirects=False) as health:
            while time.monotonic() < deadline:
                if self.process.poll() is not None or self._monitor_errors:
                    raise RuntimeError("owned local model service failed during startup")
                listener = self._listen_identity()
                if listener:
                    try:
                        response = health.get(self.endpoint.removesuffix("chat/completions") + "models",
                                              timeout=min(1.0, max(0.001, deadline - time.monotonic())))
                    except httpx.TransportError:
                        response = None
                    if response is not None and response.status_code == 200:
                        if [row.get("id") for row in response.json().get("data", [])] != [self.model]:
                            raise RuntimeError("owned HTTP service exposes a different model identity")
                        listener = self._listen_identity()
                        if not listener or artifacts != self._artifacts():
                            raise RuntimeError("owned listener or model/template artifacts changed at readiness")
                        self.ready_report = dict(engine_uid=self.engine_uid, endpoint=self.endpoint, port=self.port,
                            pid=self.process.pid, create_time=self.created, listeners=listener,
                            ready_monotonic_ns=time.monotonic_ns(), startup_seconds=time.monotonic() - started,
                            command=redact_argument_list(self.command()), artifacts=artifacts,
                            cuda_visible_devices=self.cuda_device, cuda_device_order="PCI_BUS_ID",
                            model_config=asdict(self.engine), vllm_version=VLLM_VERSION,
                            template_application="untemplated conversation POST; server chat applies template once",
                            template_application_scope="protocol/source contract; real model token parity pending")
                        return self.ready_report
                time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        raise TimeoutError("owned local model HTTP service startup deadline expired")

    def close(self):
        from src.experiments.lotus_records_supervisor import _live, _signal, _wait
        if self.last_close_report is not None:
            return self.last_close_report
        self._stop.set()
        if self._monitor is not None:
            self._monitor.join(timeout=1)
        errors = list(self._monitor_errors)
        signals, exits = [], []
        exited_before_close = None if self.process is None else self.process.poll()
        owner_failed = self.ready_report is not None and exited_before_close is not None
        if self.process is not None:
            if self.created is not None:
                try:
                    self._observe()
                    for value, seconds in ((signal.SIGTERM, 3), (signal.SIGKILL, 2)):
                        if not _live(self.owned):
                            break
                        signals.extend(_signal(self.owned, self.process.pid, self.created, value))
                        _wait(self.owned, self.process.pid, self.created, time.monotonic() + seconds,
                              exit_events=exits, phase=signal.Signals(value).name)
                except BaseException as error:
                    errors.append("process_cleanup: " + type(error).__name__)
            else:
                errors.append("process_identity_unavailable")
            # The Popen handle is still owned if process-table observation failed.
            if self.process.poll() is None:
                try:
                    self.process.kill()
                except BaseException as error:
                    errors.append("direct_service_kill: " + type(error).__name__)
            try:
                self.process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                errors.append("direct_service_process_still_running")
        live = _live(self.owned)
        confirmed = (self.process is None or self.created is not None
                     and self.process.poll() is not None and not live and not errors)
        self.last_close_report = dict(status="failed" if owner_failed else "closed" if confirmed else "unknown",
            engine_uid=self.engine_uid,
            process_exit_confirmed=confirmed, gpu_exit_confirmed=None,
            cleanup_scope="only query-owned birth-matched observed OS processes; GPU check belongs to controller",
            owned_processes=[dict(pid=p, create_time=c) for p, c in sorted(self.owned)],
            signal_events=signals, exit_events=exits, cleanup_errors=errors,
            ready=self.ready_report, endpoint=getattr(self, "endpoint", None),
            unexpected_exit_before_close=owner_failed, returncode_before_close=exited_before_close,
            returncode=None if self.process is None else self.process.returncode)
        write_private_json(self.directory / "owner-close.json", redact_json_values(self.last_close_report))
        return self.last_close_report


class _Calls:
    """Observe native input/return order and independent wire receipts."""

    def __init__(self, expected, budget, generation, model, max_calls):
        self.expected, self.budget, self.generation, self.model = expected, budget, generation, model
        self.max_calls = max_calls
        self.prepared = []
        self.prompts_observed = False
        self.returned = None
        self.events = {}
        self.batch_invocations = 0
        self.engine_uid = None
        self._lock = threading.Lock()
        self._spends = 0
        self.before_post = None
        self._last_ledger = CallLedger(expected, max_calls=max(1, len(expected)))

    def prompts(self, rows):
        if self.prompts_observed or [row["row_id"] for row in rows] != [row["row_id"] for row in self.expected]:
            raise ValueError("native Filter input order differs from declared occurrences")
        self.prepared, _ = _prepared([dict(identity, messages_json=json.dumps(row["messages"], ensure_ascii=False))
            for identity, row in zip(self.expected, rows, strict=True)])
        self.prompts_observed = True

    def reserve(self, digest):
        if self.before_post is not None:
            self.before_post()
        with self._lock:
            if self._spends >= len(self.expected) or self.max_calls is not None and self._spends >= self.max_calls:
                raise RuntimeError("native HTTP model call upper bound exhausted")
            attempt = self.budget.reserve(digest)
            self._spends += 1
            return attempt

    def record(self, event):
        with self._lock:
            row = self.events.setdefault(event["attempt"], {})
            row[event["event"]] = event
        if event["event"] == "request":
            body = event["body"]
            g = self.generation
            expected = dict(temperature=g.temperature, top_p=g.top_p, stop=list(g.stop),
                presence_penalty=g.presence_penalty, frequency_penalty=g.frequency_penalty)
            if g.seed is not None:
                expected["seed"] = g.seed
            if (body.get("model") != self.model or body.get("n", 1) != 1
                    or body.get("stream", False) is not False
                    or body.get("return_token_ids") is not True
                    or body.get("max_tokens", body.get("max_completion_tokens")) != g.max_tokens
                    or any(body.get(name) != value for name, value in expected.items())
                    or content_digest(body.get("messages")) not in {r["messages_sha256"] for r in self.prepared}):
                raise ValueError("native HTTP body differs from the declared conversation/generation")

    @contextmanager
    def observe_batch(self):
        module = importlib.import_module("lotus.models.lm")
        original = module.batch_completion
        if getattr(original, "_semloom_native_observer", False):
            raise RuntimeError("nested native batch observers are unsupported")
        def observed(*args, **kwargs):
            self.batch_invocations += 1
            if self.batch_invocations != 1 or not self.prompts_observed:
                raise ValueError("native ordinary Filter requires exactly one LM batch_completion invocation")
            result = original(*args, **kwargs)
            self.returned = result
            return result
        observed._semloom_native_observer = True
        module.batch_completion = observed
        try:
            yield
        finally:
            module.batch_completion = original

    def receipts(self):
        rows = []
        for attempt, events in sorted(self.events.items()):
            request = events.get("request", {})
            body = request.get("body", {})
            response = events.get("http_response", {})
            rows.append(dict(attempt=attempt, messages_sha256=content_digest(body.get("messages")),
                request_bytes_sha256=request.get("request_bytes_sha256"),
                http_started="http_started" in events, http_status=response.get("status"),
                response=response.get("response"), error_type=events.get("http_finished", {}).get("error_type")))
        return rows

    def ledger(self, failure):
        ledger = CallLedger(self.expected, max_calls=max(1, len(self.expected)))
        self._last_ledger = ledger
        receipts = self.receipts()
        by_id = {}
        for receipt in receipts:
            value = receipt["response"]
            response_id = value.get("id") if type(value) is dict else None
            if type(response_id) is str and response_id:
                by_id.setdefault(response_id, []).append(receipt)
        protocol_errors = []
        returned = self.returned if isinstance(self.returned, (list, tuple)) else []
        if self.returned is not None and len(returned) != len(self.expected):
            protocol_errors.append("native batch return cardinality differs")
        for ordinal, identity in enumerate(self.expected):
            prepared = self.prepared[ordinal] if ordinal < len(self.prepared) else identity
            value = returned[ordinal] if ordinal < len(returned) else None
            matches = by_id.get(getattr(value, "id", None), [])
            if not matches:
                # A systemic SDK failure may omit an ordered return. Retain a
                # unique conversation receipt; duplicates stay unknown, with
                # their original wire receipts retained separately below.
                same = [r for r in receipts if r["messages_sha256"] == prepared.get("messages_sha256")]
                duplicates = sum(r.get("messages_sha256") == prepared.get("messages_sha256") for r in self.prepared)
                matches = same if len(same) == 1 and duplicates == 1 else []
            receipt = matches[0] if len(matches) == 1 else None
            row = dict(prepared, status="unknown", submission_confirmed=False,
                prompt_tokens=None, completion_tokens=None, total_tokens=None,
                error_type=type(failure).__name__ if failure else "NativeReceiptUnconfirmed")
            if receipt is not None:
                row.update(attempt=receipt["attempt"], http_status=receipt["http_status"],
                           submission_confirmed=receipt["http_status"] is not None)
                payload = receipt["response"]
                valid_token_ids = False
                if type(payload) is dict:
                    usage = payload.get("usage")
                    usage = usage if type(usage) is dict else {}
                    for name in ("prompt_tokens", "completion_tokens", "total_tokens"):
                        count = usage.get(name)
                        if type(count) is int and count >= 0:
                            row[name] = count
                    row["engine_request_id"] = payload.get("id")
                    choices = payload.get("choices", [])
                    choice = choices[0] if type(choices) is list and len(choices) == 1 and type(choices[0]) is dict else {}
                    message = choice.get("message")
                    message = message if type(message) is dict else {}
                    row.update(text=message.get("content"), finish_reason=choice.get("finish_reason"))
                    prompt_ids, output_ids = payload.get("prompt_token_ids"), choice.get("token_ids")
                    valid_token_ids = (type(prompt_ids) is list and bool(prompt_ids)
                        and type(output_ids) is list
                        and all(type(token) is int and token >= 0 for token in prompt_ids + output_ids)
                        and len(prompt_ids) == row["prompt_tokens"]
                        and len(output_ids) == row["completion_tokens"]
                        and len(output_ids) <= self.generation.max_tokens)
                valid_usage = (row["prompt_tokens"] is not None and row["completion_tokens"] is not None
                    and row["total_tokens"] == row["prompt_tokens"] + row["completion_tokens"])
                native_matches = True
                if getattr(value, "id", None) is not None:
                    choices = getattr(value, "choices", [])
                    usage = getattr(value, "usage", None)
                    native_matches = (len(choices) == 1
                        and getattr(choices[0].message, "content", None) == row.get("text")
                        and choices[0].finish_reason == row.get("finish_reason")
                        and all(getattr(usage, name, None) == row[name]
                                for name in ("prompt_tokens", "completion_tokens", "total_tokens")))
                if (receipt["messages_sha256"] == prepared.get("messages_sha256")
                        and receipt["http_status"] == 200 and valid_usage and valid_token_ids
                        and native_matches
                        and payload.get("model") == self.model
                        and type(row.get("engine_request_id")) is str and row["engine_request_id"]
                        and type(row.get("text")) is str and row.get("finish_reason") in ("stop", "length")):
                    row.update(status="succeeded", engine_uid=self.engine_uid)
                    row.update(prompt_token_ids=list(prompt_ids), output_token_ids=list(output_ids),
                               token_ids_source="vllm_http_response")
                    row.pop("error_type")
                elif receipt["http_status"] is not None:
                    row.update(status="failed", error_type="NativeHTTPProtocolError")
            if row["status"] != "succeeded" and (failure is None or receipt is not None):
                protocol_errors.append("native call lacks a valid authoritative HTTP receipt")
            ledger.record(row)
        return ledger.report(), receipts, protocol_errors


def _client_and_lm(method, generation, resources, profile, endpoint):
    import httpx
    from openai import OpenAI
    # One owned client supports the original LiteLLM threads. No connection cap
    # adds a second concurrency policy to LM.max_batch_size.
    lm = make_lm(method, generation)
    client = OpenAI(base_url=endpoint.removesuffix("/chat/completions"), api_key="local-fixture",
        max_retries=0, timeout=resources.query_timeout_s,
        http_client=httpx.Client(transport=httpx.HTTPTransport(retries=0), trust_env=False,
                                limits=httpx.Limits(max_connections=None, max_keepalive_connections=None)))
    lm.max_batch_size = profile["native_lm_max_batch_size"]
    lm.kwargs.update(api_base=endpoint.removesuffix("/chat/completions"), api_key="local-fixture",
        num_retries=0, max_retries=0, timeout=resources.query_timeout_s, n=1, client=client,
        extra_body={"return_token_ids": True})
    return client, lm


def run(source, checkout, method, generation, engine, resources, *,
        query_id, profile, observer=None, context=None):
    """Execute the selected unchanged original Movie query, with controller-owned budget/lease."""
    report = dict(schema="semloom.lotus_native_local_model.v1", status="failed", query_id=query_id,
        production_core_used=False, scheduler_owner="Original LOTUS LM/LiteLLM batch_completion",
        native_scheduling_retained=True, observational_batch_wrapper=True,
        uncached_hook_replaced=False, upstream_accessor_used=True, model_calls_are_not_output_rows=True,
        protocol="query-owned localhost OpenAI chat HTTP; distinct from embedded LLM.chat",
        lifecycle="fresh service/model per query; startup included; resident latency unavailable",
        resident_query_seconds=None, evaluation_in_query_seconds=False,
        budget_digest_scope="SHA256 of actual complete outgoing HTTP body, once before every POST",
        http_observation_timing_scope="budget_reserve_ns includes per-POST owned-listener verification",
        template_applications_in_adapter=0, template_applications_in_server=1,
        template_evidence_scope="untemplated conversations at wire; real model/token parity pending")
    started = time.monotonic()
    service = client = lm = calls = None
    primary = None
    def emit(event):
        if observer is not None:
            observer(event)
    try:
        os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "true"
        import lotus
        import pandas as pd
        from lotus.types import SerializationFormat
        if type(source) is not MovieRecordSource:
            raise TypeError("native Movie tasks require MovieRecordSource")
        if type(engine) is not EngineSpec or type(generation) is not GenerationSpec or type(resources) is not RecordsResources:
            raise TypeError("native Movie tasks require typed engine/generation/resources")
        task = source.task
        validate_movie_method(method, task.name)
        validate_generation(method, generation)
        resources.describe()
        engine.__post_init__()
        if generation.max_tokens > engine.max_model_len:
            raise ValueError("native max_tokens exceeds max_model_len")
        if not method.model_name.startswith("openai/") or not method.model_name[len("openai/"):]:
            raise ValueError("native local HTTP requires an explicit openai/ model identity")
        if (type(profile) is not dict or set(profile) != {"native_lm_max_batch_size"}
                or type(profile["native_lm_max_batch_size"]) is not int or profile["native_lm_max_batch_size"] < 1):
            raise ValueError("native profile requires only a positive native_lm_max_batch_size")
        if context is None or type(context) is not dict:
            raise ValueError("native local model requires the controller context")
        budget = context.get("claimed_budget")
        if not isinstance(budget, ClaimedUnit) or budget.reservation.unit_id != query_id or budget.attempts:
            raise ValueError("the current query's fresh controller-claimed budget is required")
        emit("preflight_start")
        report.update(source=source.preflight(method), task=task.describe(), method=method.describe(), generation=asdict(generation),
                      method_source=validate_filter_source(), profile=dict(profile),
                      resources=resources.describe())
        upstream = verify_checkout(checkout)
        expected = source.expected(query_id)
        calls = _Calls(expected, budget, generation, method.model_name[len("openai/"):], engine.max_model_calls)
        if ((expected and budget.remaining != len(expected))
                or engine.max_model_calls is not None and engine.max_model_calls < len(expected)):
            raise ValueError("native complete task exceeds its claimed model-call upper bound")
        if lotus.settings.serialization_format != SerializationFormat.DEFAULT:
            raise ValueError("native Movie tasks require the original DEFAULT serialization")
        directory = Path(context["output_dir"]) / "lotus-native-http"
        require_outside_git(directory)
        lease = context["owner_context"]
        if file_identity(lease["lease_path"])["sha256"] != lease["lease_sha256"]:
            raise ValueError("external host lease identity changed")
        runtime = context["runtime_binding"]
        if any(runtime[name] != getattr(engine, name) for name in ("driver_python", "model_python", "model_path")):
            raise ValueError("native engine differs from the controller runtime binding")
        if runtime.get("tokenizer_path", engine.model_path) != (engine.tokenizer_path or engine.model_path):
            raise ValueError("native tokenizer differs from the controller runtime binding")
        device = runtime["cuda_visible_devices"]
        if (type(device) is not str or re.fullmatch(
                r"(?:[0-9]+|GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})", device) is None):
            raise ValueError("native local model requires one declared physical CUDA index or GPU UUID")
        if expected:
            report["environment"] = engine.validate_driver_environment()
        new_private_directory(directory)
        emit("preflight_end")
        endpoint = "http://127.0.0.1:0/v1/chat/completions"
        if expected:
            service = LocalService(engine, calls.model, resources, directory, device)
            emit("owner_start")
            report["owner_ready"] = service.start()
            calls.engine_uid = report["owner_ready"]["engine_uid"]
            calls.before_post = service.verify_listener
            endpoint = service.endpoint
        else:
            report["owner_ready"] = dict(no_inference=True, reason="zero predicate-eligible rows; no service required")
            report["backend_close"] = dict(status="closed", process_exit_confirmed=True,
                                           gpu_exit_confirmed=None, owned_processes=[])
        emit("owner_ready")
        client, lm = _client_and_lm(method, generation, resources, profile, endpoint)
        decisions = []
        def load():
            emit("source_read_start")
            rows = source.rows()
            frame = pd.DataFrame([dict(reviewId=r["reviewId"], id=r["id"], reviewText=r["reviewText"],
                                       _execution_row_id=r["row_id"]) for r in rows],
                                 columns=["reviewId", "id", "reviewText", "_execution_row_id"])
            emit("source_read_end")
            return frame
        def record(values):
            decisions.extend(dict(row_id=row_id, decision=decision) for row_id, decision in values)
        with lotus.settings.context(lm=lm, enable_cache=False):
            with observe_filter_rows(record, calls.prompts), calls.observe_batch():
                with _observe_native_httpx_client(calls, calls.record, endpoint,
                                                  timeout_s=resources.query_timeout_s):
                    result = run_original_lotus_query(upstream, task.query, load)
        # The timestamp is immediately after the original complete task result returns.
        report.update(result=task.encode_result(result), decisions=decisions,
                      query_seconds=time.monotonic() - started)
        emit("result_ready")
        ledger, receipts, errors = calls.ledger(None)
        if errors or len(decisions) != len(expected):
            raise ValueError("native complete result/call/decision protocol differs: " + "; ".join(errors))
        task.validate_result(report["result"], source.rows(), decisions)
        emit("evaluation_start")
        evaluated = time.monotonic()
        evaluation_error = None
        try:
            labeled = source.labeled_frame()
            quality = evaluate_original(upstream, task.query, result, labeled)
            labels = dict(zip((r["row_id"] for r in source.rows()), labeled["scoreSentiment"], strict=True))
            quality["classification_audit"] = classification_audit(
                {r["row_id"]: labels[r["row_id"]] for r in expected},
                [(r["row_id"], "POSITIVE" if r["decision"] else "NEGATIVE") for r in decisions])
            report.update(quality=json_metric(quality), status="completed")
        except BaseException as error:
            evaluation_error = error
            raise
        finally:
            report["evaluation_seconds"] = time.monotonic() - evaluated
            try:
                emit("evaluation_end")
            except BaseException as error:
                if evaluation_error is None:
                    raise
                evaluation_error.add_note("native evaluation_end observer also failed: " + type(error).__name__)
    except BaseException as error:
        primary = error
    finally:
        for action in ([client.close] if client is not None else []):
            try:
                action()
            except BaseException as error:
                if primary is None:
                    primary = error
                else:
                    primary.add_note("native client cleanup also failed: " + type(error).__name__)
        if service is not None:
            try:
                report["backend_close"] = service.close()
                if report["backend_close"]["process_exit_confirmed"] is not True:
                    raise RuntimeError("native HTTP owner process exit remains unconfirmed")
                if report["backend_close"]["status"] != "closed":
                    raise RuntimeError("native HTTP owner exited unexpectedly before cleanup")
            except BaseException as error:
                report["backend_close"] = getattr(service, "last_close_report", report.get("backend_close"))
                if primary is None:
                    primary = error
                else:
                    primary.add_note("native owner cleanup also failed: " + type(error).__name__)
        if calls is not None:
            try:
                report["calls"], report["http_receipts"], report["call_protocol_errors"] = calls.ledger(primary)
            except BaseException as error:
                # Keep every validated prefix receipt even if observation itself
                # breaks. Unassociated wire evidence stays separately available.
                calls._last_ledger.mark_unconfirmed(type(error).__name__)
                report["calls"] = calls._last_ledger.report()
                report["http_receipts"] = calls.receipts()
                report["call_accounting_error"] = type(error).__name__
                if primary is None:
                    primary = error
                else:
                    primary.add_note("native call accounting also failed: " + type(error).__name__)
            report.update(budget_attempts_consumed=calls._spends,
                http_send_attempts=sum(r["http_started"] for r in report["http_receipts"]),
                native_batch_invocations=calls.batch_invocations)
        if lm is not None:
            report["lm"] = lm_report(lm)
        report["elapsed_seconds"] = time.monotonic() - started
        try:
            emit("owner_closed")
        except BaseException as error:
            if primary is None:
                primary = error
            else:
                primary.add_note("native owner_closed observer failed: " + type(error).__name__)
        if primary is not None:
            report.update(status="failed", error_type=type(primary).__name__, error=redact_text(str(primary)))
            primary.query_report = report
    if primary is not None:
        raise primary
    return report
