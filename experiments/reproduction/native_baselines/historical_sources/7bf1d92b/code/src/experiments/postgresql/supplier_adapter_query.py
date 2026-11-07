"""Original LOTUS methods with four explicitly selected executor owners."""

import base64
from collections import Counter
from contextlib import contextmanager, ExitStack
from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
import threading
import time

from src.baselines.common.private_artifacts import (
    content_digest,
    new_private_directory,
    open_private_text,
    write_private_json,
)
from src.baselines.common.redact import redact_json_values
from src.execution_provider.adapters.full_response import encode_full_response
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.experiments.buffered_events import BufferedEvents
from src.experiments.execution_timing import timed_call, timing_span
from src.experiments.process_sampling import ProcessSampler
from src.scheduling.core.session_contract import Usage, SessionTimeouts
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from .cell_evidence import CellErrors
from .native_adapter_metrics import summarize_calls, executor_phase_observations
from .native_adapter_query import (
    call_clock_domain,
    _runtime,
    load_bounded_rows,
    resolve_adapter_limits,
)
from .ready_query_recording import record_prepared_execution, proxy_http_peak
from .sembench_queries import LOTUS_MOVIE_ARMS

SUPPLIER_ARMS = LOTUS_MOVIE_ARMS
NATIVE_SUPPLIER_BACKENDS = {
    "lotus-method-native-daft": "daft",
    "lotus-method-native-ray-data": "ray-data",
}
LOTUS_OBSERVATION_MODES = ("qualification", "performance")


def validate_lotus_observation_mode(mode):
    if mode not in LOTUS_OBSERVATION_MODES:
        raise ValueError("LOTUS observation mode must be qualification or performance")
    return mode


def _record_supplier_http(stream, body, response, status):
    """Keep original response bytes for the supplier's own parser."""
    record = dict(request=json.loads(body), http_status=status)
    try:
        record["response"] = json.loads(response)
    except (json.JSONDecodeError, UnicodeDecodeError):
        record.update(response=None, response_bytes_base64=base64.b64encode(response).decode())
    stream.write(json.dumps(record, ensure_ascii=False) + "\n")


def _supplier_runtime(arm, owner):
    shared = owner.group.runtime
    backend = NATIVE_SUPPLIER_BACKENDS.get(arm)
    runtime = (
        dict(getattr(owner.group, "daft_runtime", None) or shared)
        if backend == "daft"
        else dict(shared)
    )
    if backend is not None:
        runtime.update(
            owner={"daft": "Daft Native", "ray-data": "Ray Data"}[backend],
            execution_backend=backend,
            shared_runtime_owner=shared.get("owner"),
        )
    elif arm == "lotus-adapted-native":
        runtime.update(owner="LOTUS/LiteLLM", shared_runtime_owner=shared.get("owner"))
    return runtime


class MethodObservations:
    def __init__(self, writer, raw, unit_id):
        self.writer, self.raw, self.unit_id = writer, raw, unit_id
        self.calls = {}
        self.domain = call_clock_domain()

    def record(self, event):
        value = dict(event)
        value.setdefault("monotonic_ns", time.monotonic_ns())
        self.writer.record(value)

    def ready(
        self,
        row_id,
        stage,
        payload,
        *,
        response_representation,
        call_id="lotus-map",
        native_ready_ns=None,
        ready_ns=None,
        readiness_scope=None,
    ):
        identity = (row_id, str(stage), call_id)
        if identity in self.calls:
            raise ValueError("method generated a duplicate complete call")
        call = dict(
            row_id=row_id,
            stage_id=str(stage),
            stage_ordinal=stage,
            call_id=call_id,
            model_role="main",
            request_values_sha256=None if payload is None else content_digest(json.loads(payload)),
            response_representation=response_representation,
        )
        if payload is None:
            call["request_values_scope"] = (
                "not constructed before the native SDK; actual outbound values are retained by the common proxy"
            )
        if native_ready_ns is not None:
            call["native_ready_ns"] = native_ready_ns
            call["readiness_scope"] = (
                "complete call received by the trusted batch callback; native steady clock retained separately"
            )
        if readiness_scope is not None:
            call["readiness_scope"] = readiness_scope
        self.calls[identity] = call
        self.record(
            dict(
                event="task_ready",
                clock_domain=self.domain,
                monotonic_ns=time.monotonic_ns() if ready_ns is None else ready_ns,
                **call,
            )
        )

    def received(self, row_id, stage, body, *, when=None, call_id="lotus-map"):
        self.record(
            dict(
                event="caller_response",
                row_id=row_id,
                stage_id=str(stage),
                call_id=call_id,
                monotonic_ns=time.monotonic_ns() if when is None else when,
                clock_domain=self.domain,
                response_bytes_sha256=hashlib.sha256(body).hexdigest(),
            )
        )

    def save_full(self, row_id, stage, full):
        self.raw.write(
            json.dumps(
                dict(
                    row_id=row_id,
                    stage_id=str(stage),
                    response_wire=base64.b64encode(encode_full_response(full)).decode(),
                )
            )
            + "\n"
        )


class _LotusBatchObservation:
    """Measure the original LM's complete batch return; native pools remain native."""

    def __init__(
        self,
        lm,
        rows,
        observations,
        executor=None,
        *,
        executor_label=None,
        call_id="lotus-map",
        observation_mode="qualification",
    ):
        self.observation_mode = validate_lotus_observation_mode(observation_mode)
        self.original = lm._process_uncached_messages
        self.executor, self.rows, self.observations = executor, rows, observations
        self.stage = 0
        self.call_id = call_id
        self.executor_label = executor_label or ("native" if executor is None else "semloom")

    def __call__(self, lm, data, kwargs, show_progress_bar, description):
        ready_ns = time.monotonic_ns()
        stage = self.stage
        self.stage += 1
        if len(data) != len(self.rows):
            raise ValueError("cache-disabled LOTUS batch differs from the source row set")

        def prepared(index, call):
            self.observations.ready(
                self.rows[index]["row_id"],
                stage,
                call.payload,
                call_id=self.call_id,
                response_representation="complete native SDK ModelResponse values",
                ready_ns=ready_ns,
                readiness_scope="complete messages and effective parameters received by the original uncached batch entry; HTTP encoding follows",
            )
            self.observations.record(
                dict(
                    event="lotus_http_call_prepared",
                    row_id=self.rows[index]["row_id"],
                    stage_id=str(stage),
                    clock_domain=self.observations.domain,
                    scope="expected native HTTP encoding"
                    if self.executor is None
                    else "HTTP encoding used by execution",
                )
            )

        if self.executor is None:
            if self.observation_mode == "qualification":
                from src.semantic_methods.lotus.sdk import prepare_call

                for index, item in enumerate(data):
                    prepared(index, prepare_call(lm, item[0], kwargs))
            else:
                for row in self.rows:
                    self.observations.ready(
                        row["row_id"],
                        stage,
                        None,
                        call_id=self.call_id,
                        response_representation="complete native SDK ModelResponse values",
                        ready_ns=ready_ns,
                        readiness_scope="complete messages and effective parameters received by the original uncached batch entry; HTTP encoding follows",
                    )
            results = self.original(data, kwargs, show_progress_bar, description)
        else:
            self.executor.on_prepared = prepared
            self.executor.on_response = (
                (
                    lambda index, full: self.observations.save_full(
                        self.rows[index]["row_id"], stage, full
                    )
                )
                if self.observation_mode == "qualification"
                else None
            )
            results = self.executor(lm, data, kwargs, show_progress_bar, description)
        if len(results) != len(self.rows):
            raise ValueError("LOTUS native batch returned an incomplete response list")
        returned = time.monotonic_ns()
        self.observations.record(
            dict(
                event="lotus_batch_return",
                monotonic_ns=returned,
                stage_id=str(stage),
                response_count=len(results),
                executor=self.executor_label,
                scope="complete SDK response list returned before observation serialization and LOTUS statistics",
            )
        )
        for row, response in zip(self.rows, results):
            if isinstance(response, Exception):
                self.observations.record(
                    dict(
                        event="caller_error",
                        row_id=row["row_id"],
                        stage_id=str(stage),
                        call_id=self.call_id,
                        error_type=type(response).__name__,
                        monotonic_ns=returned,
                    )
                )
                continue
            if not callable(getattr(response, "model_dump", None)):
                raise ValueError("LOTUS returned a failed complete SDK response")
            response_values = response.model_dump()
            body = json.dumps(
                response_values, sort_keys=True, ensure_ascii=False, allow_nan=False
            ).encode()
            self.observations.received(
                row["row_id"], stage, body, when=returned, call_id=self.call_id
            )
            if self.observation_mode == "qualification":
                self.observations.raw.write(
                    json.dumps(
                        dict(
                            row_id=row["row_id"],
                            stage_id=str(stage),
                            response_representation="complete native SDK ModelResponse values",
                            response_values=response_values,
                        ),
                        ensure_ascii=False,
                        allow_nan=False,
                    )
                    + "\n"
                )
        return results


@timed_call("lotus.lm_prepare")
def prepare_lotus_lm(plan, model, options, tokenizer_path, *, sembench=False):
    os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
    from lotus.models import LM
    from src.semantic_methods.lotus.sdk import validate_source

    validate_source()
    token = model.bearer_token or "local-fixture"
    tokenizer = None
    if tokenizer_path is not None:
        from tokenizers import Tokenizer

        tokenizer = Tokenizer.from_file(str(tokenizer_path))
    generation = {"stop": ["\n"]} if sembench else {}
    return LM(
        "openai/" + model.model_id,
        api_base=model.endpoint_url.removesuffix("/chat/completions"),
        api_key=token,
        max_batch_size=options.concurrency,
        temperature=0,
        top_p=1,
        max_tokens=plan.max_tokens,
        num_retries=0,
        max_retries=0,
        timeout=model.timeout_ms / 1000,
        tokenizer=tokenizer,
        **generation,
    )


@contextmanager
def _lotus_rows(
    stack,
    arm,
    values,
    plan,
    model,
    execution,
    observations,
    options,
    unit_id,
    stop,
    tokenizer_path,
    *,
    lm=None,
    query_timeout_s=120,
    sembench_result=None,
    lotus_observation_mode="qualification",
):
    import lotus
    import pandas as pd
    from src.semantic_methods.lotus.batch import LotusBatchExecutor, lotus_executor

    configured = replace(model, bearer_token=model.bearer_token or "local-fixture")
    if lm is None:
        lm = prepare_lotus_lm(
            plan, model, options, tokenizer_path, sembench=sembench_result is not None
        )
    frame = (
        pd.DataFrame({"row_id": [v["row_id"] for v in values], "text": [v["text"] for v in values]})
        if sembench_result is None
        else None
    )
    operator = "lotus-map" if sembench_result is None else "lotus-filter"
    stack.enter_context(lotus.settings.context(lm=lm, enable_cache=False))
    selected = (
        None
        if execution is None
        else LotusBatchExecutor(
            execution,
            configured,
            query_id=unit_id,
            operator_id=operator,
            cancelled=stop.is_set,
            timeout_s=query_timeout_s,
            prevalidate_batch=True,
            retain_full_responses=False,
        )
    )
    backend = NATIVE_SUPPLIER_BACKENDS.get(arm)
    if backend is not None:
        from src.baselines.text.frameworks.native_calls import NativeCallExecutor
        from src.semantic_methods.lotus.native import LotusNativeBatchExecutor

        selected = LotusNativeBatchExecutor(
            NativeCallExecutor(backend, options),
            configured,
            query_id=unit_id,
            operator_id=operator,
            cancelled=stop.is_set,
            timeout_s=query_timeout_s,
            retain_full_responses=False,
        )
    observer = _LotusBatchObservation(
        lm,
        values if sembench_result is None else sembench_result.model_rows,
        observations,
        selected,
        executor_label=backend,
        call_id=operator,
        observation_mode=lotus_observation_mode,
    )
    stack.enter_context(lotus_executor(lm, observer))

    def execute():
        if sembench_result is not None:
            yield from sembench_result.execute()
            return
        result = frame.sem_map(
            plan.instruction + "\nInput: {text}", suffix="_answer", return_raw_outputs=True
        )
        yield from zip(result["row_id"], result["_answer"])

    yield (
        execute,
        dict(
            supplier="LOTUS1.2.4",
            source_commit="b1a85fd7a66fabed8a1585d44d7597d592b4433f",
            cache_policy="disabled",
            method_roles=["main"],
            two_map=False,
            observation_mode=lotus_observation_mode,
            call_values_source=(
                "execution-prepared payload"
                if selected is not None
                else "expected native HTTP encoding"
                if lotus_observation_mode == "qualification"
                else "unavailable before native SDK encoding; actual HTTP values retained by proxy"
            ),
            observation_timing="all in-query observation work remains in measured query time; no estimated subtraction",
            caller_scope="original LM receives full uncached SDK response list; whole batch wait retained",
            native_supply="DataFrame formatting and complete uncached batch; native pool retained only on native arm",
            executor=backend or ("semloom" if execution is not None else "LOTUS/LiteLLM"),
            original_method_preserved=True,
            original_query=(
                None
                if sembench_result is None
                else dict(
                    benchmark="SemBench Movie",
                    query_id=sembench_result.query.query_id,
                    commit="c814e3807e72d4cf876b852b17e77f3cc94575c2",
                    operator="sem_filter",
                    relational_result="COUNT"
                    if sembench_result.query.query_id == 3
                    else "LIMIT5 reviewId",
                )
            ),
            execution_kind="native_api"
            if backend is not None
            else "supplier_native"
            if execution is None
            else "semloom",
        ),
    )


@timed_call("semloom.executor_drain")
def _drain_execution(execution, *, close=True):
    until = time.monotonic() + execution.drain_timeout_s
    while execution.engine.capacity.usage() != Usage() and time.monotonic() < until:
        execution.engine.advance()
        time.sleep(0.01)
    if (
        execution.engine.capacity.usage() != Usage()
        or execution.engine.jobs.jobs
        or (close and not execution.close(execution.drain_timeout_s))
    ):
        raise RuntimeError("supplier execution retains unresolved model resources")


def run_supplier_query(
    arm,
    *,
    load_source=None,
    plan,
    model,
    ledger,
    unit_id,
    root,
    options,
    physical=None,
    ray_temp_root=None,
    ray_address=None,
    query_timeout_s=120,
    tokenizer_path=None,
    reference_outputs=None,
    allowed_outputs=None,
    preparation_started_ns=None,
    owner=None,
    max_held_tasks=None,
    sembench_query=None,
    lotus_observation_mode="qualification",
):
    if arm not in SUPPLIER_ARMS:
        raise ValueError("supplier arm has not been integrated")
    if query_timeout_s is None and sembench_query is None:
        raise ValueError("completion deadlines require an explicit original Movie query")
    validate_lotus_observation_mode(lotus_observation_mode)
    if sembench_query is not None:
        from .sembench_queries import MovieQuery, MovieQueryResult, LOTUS_MOVIE_ARMS

        if (
            type(sembench_query) is not MovieQuery
            or arm not in LOTUS_MOVIE_ARMS
            or load_source is not None
            or reference_outputs is not None
            or allowed_outputs is not None
            or plan.max_tokens != 8
        ):
            raise ValueError(
                "original Movie queries require their selected relation and fixed Filter generation"
            )
        if owner is not None and owner.group.sembench_dataset != sembench_query.dataset:
            raise ValueError(
                "persistent LOTUS owner was not prepared for this original Movie dataset"
            )
    use_core = arm == "lotus-method-semloom"
    if use_core and physical is None:
        raise ValueError("supplier SemLoom arm requires its Daft/Ray transport")
    if not use_core and physical is not None:
        raise ValueError("Ray Map physical options belong to SemLoom only")
    if physical is not None and (
        physical.window_bytes < MAX_FRAME_BYTES + 24
        or max(physical.workers, physical.batch_rows) > options.concurrency
    ):
        raise ValueError(
            "supplier SemLoom arm requires declared payload batches fitting a complete legal task"
        )
    if plan.model_id != model.model_id:
        raise ValueError("method and service model identities differ")
    held_tasks = resolve_adapter_limits(options, max_held_tasks)
    invoked = time.monotonic_ns()
    started = invoked if preparation_started_ns is None else preparation_started_ns
    if type(started) is not int or not 0 < started <= invoked:
        raise ValueError("application preparation time must precede executor entry")
    root = Path(root)
    new_private_directory(root)
    errors = CellErrors()
    stop = threading.Event()
    lock = threading.Lock()
    count = 0
    execution = None
    reserved = False
    sembench_result = None
    predictions = {}
    summary = dict(
        schema="semloom.supplier_adapter_query.v1",
        status="failed",
        arm=arm,
        unit_id=unit_id,
        performance_qualified=False,
        query_preparation_started_ns=started,
        source_identity="finite external raw input; no PG multi-Map",
        options=asdict(options),
        semloom_capacity=dict(held_tasks=held_tasks, active_requests=options.concurrency)
        if use_core
        else None,
        lotus_observation_mode=lotus_observation_mode,
        request_budget_owner="common proxy before upstream POST",
        stages={},
    )
    values = []
    maximum = 0

    def before(route, body):
        nonlocal count
        with lock:
            if stop.is_set():
                raise RuntimeError("supplier query stopped further model forwarding")
            value = json.loads(body)
            if value.get("model") != model.model_id or value.get("stream", False):
                raise ValueError("supplier changed single-model nonstreaming semantics")
            if count == maximum:
                raise ValueError("supplier exceeded its declared complete-call count")
            shared.reserve(hashlib.sha256(body).hexdigest())
            count += 1

    def after(route, body, response, status):
        try:
            with lock:
                upstream_raw.write(
                    json.dumps(
                        dict(
                            request_values_sha256=content_digest(json.loads(body)),
                            request_body_sha256=hashlib.sha256(body).hexdigest(),
                            http_status=status,
                            response_body_sha256=hashlib.sha256(response).hexdigest(),
                            response_body_base64=base64.b64encode(response).decode(),
                        )
                    )
                    + "\n"
                )
            with lock:
                _record_supplier_http(protocols, body, response, status)
        except BaseException as failure:
            stop.set()
            errors.record("http_failure", failure)
            raise

    try:
        source_started = time.monotonic_ns()
        with timing_span("query.source_load"):
            if sembench_query is None:
                values = load_bounded_rows(load_source)
            else:
                summary["sembench_preflight"] = sembench_query.dataset.preflight()
                values = sembench_query.dataset.load_rows()
                sembench_query.check_executor_shape(arm, values)
                sembench_result = MovieQueryResult(sembench_query, values, root)
                summary["source_identity"] = (
                    "selected original SemBench Movie finite relation; external source"
                )
        summary["stages"]["source_seconds"] = (time.monotonic_ns() - source_started) / 1e9
        row_ids = {v["row_id"] for v in values}
        if reference_outputs is not None and (
            not isinstance(reference_outputs, dict)
            or set(reference_outputs) != row_ids
            or any(not isinstance(v, str) for v in reference_outputs.values())
        ):
            raise ValueError("evaluation references must match all external row occurrences")
        maximum = len(values) if sembench_result is None else len(sembench_result.model_rows)
        if maximum:
            ledger.reserve_unit(unit_id, maximum)
            reserved = True
            shared = ledger.claim_shared_unit(unit_id)
        with ExitStack() as stack:
            writer = stack.enter_context(BufferedEvents(root / "method-events.jsonl"))
            raw = stack.enter_context(open_private_text(root / "complete-responses.jsonl"))
            upstream_raw = stack.enter_context(
                open_private_text(root / "upstream-response-bodies.jsonl")
            )
            protocols = stack.enter_context(open_private_text(root / "protocols.jsonl"))
            observations = MethodObservations(writer, raw, unit_id)
            if owner is not None:
                physical, runtime = owner.physical, _supplier_runtime(arm, owner)
                summary["runtime"] = runtime
            elif use_core:
                physical, runtime = _runtime(
                    stack, "fixed-map-semloom", options, physical, ray_temp_root, ray_address
                )
                summary["runtime"] = runtime
            elif arm in NATIVE_SUPPLIER_BACKENDS:
                runtime_arm = {"daft": "fixed-map-native-daft", "ray-data": "fixed-map-native-ray"}[
                    NATIVE_SUPPLIER_BACKENDS[arm]
                ]
                _, runtime = _runtime(stack, runtime_arm, options, None, ray_temp_root, ray_address)
                summary["runtime"] = runtime
            if owner is None:
                gateway = stack.enter_context(
                    ObservationGateway(
                        routes=(GatewayRoute(unit_id, "model", model.endpoint_url),),
                        trace_path=root / "http-trace.jsonl",
                        request_timeout_s=model.timeout_ms / 1000
                        if query_timeout_s is None
                        else min(query_timeout_s, model.timeout_ms / 1000),
                        preserve_http=True,
                        before_forward=before,
                        after_forward=after,
                    )
                )
                routed = replace(model, endpoint_url=gateway.endpoint_url(unit_id, "model"))
            else:
                routed = owner.model

            def core_record(event):
                observations.record(
                    dict(event, event="backend_" + event["event"])
                    if event.get("event") in ("http_started", "http_finished")
                    else event
                )

            if owner is not None:
                stack.enter_context(owner.query(unit_id, root, before, after, core_record))
                execution = owner.execution
            elif use_core:
                token = routed.bearer_token or "local-fixture"
                with timing_span("semloom.executor_create"):
                    execution = build_native_execution(
                        replace(routed, bearer_token=token),
                        physical=physical,
                        max_tasks=held_tasks,
                        max_active_requests=options.concurrency,
                        observer=core_record,
                        timeouts=SessionTimeouts(
                            backend_s=120 if query_timeout_s is None else max(45, query_timeout_s)
                        ),
                    )

                def close_execution():
                    nonlocal execution
                    summary["core_cleanup"] = dict(
                        final_core_usage=asdict(execution.engine.capacity.usage())
                    )
                    errors.attempt("execution_drain", lambda: _drain_execution(execution))
                    summary["core_cleanup"]["final_core_usage"] = asdict(
                        execution.engine.capacity.usage()
                    )
                    execution = None

                stack.callback(close_execution)
            execute, identity = stack.enter_context(
                _lotus_rows(
                    stack,
                    arm,
                    values,
                    plan,
                    routed,
                    execution,
                    observations,
                    options,
                    unit_id,
                    stop,
                    tokenizer_path,
                    lm=owner.lotus_lm() if owner is not None else None,
                    query_timeout_s=query_timeout_s,
                    sembench_result=sembench_result,
                    lotus_observation_mode=lotus_observation_mode,
                )
            )
            summary["identity"] = identity
            ready = time.monotonic_ns()
            if count:
                raise ValueError("supplier preparation called the model before query submission")

            @contextmanager
            def open_rows():
                def rows():
                    for row_id, output in execute():
                        if row_id in predictions or (
                            sembench_result is None and row_id not in row_ids
                        ):
                            raise ValueError("supplier result lost source occurrence association")
                        if allowed_outputs is not None and output not in allowed_outputs:
                            stop.set()
                            raise ValueError("supplier returned an undeclared application value")
                        predictions[row_id] = output
                        yield row_id, output

                stream = rows()
                try:
                    yield stream
                finally:
                    stream.close()

            def cancel():
                stop.set()

            with ProcessSampler(
                root / "query-rss.jsonl", {"supplier_driver": os.getpid()}, include_children=True
            ) as sampler:
                summary["execution"] = record_prepared_execution(
                    root / "q0",
                    open_rows,
                    backend_ready_ns=ready,
                    preparation_started_ns=started,
                    max_rows=len(values),
                    max_result_bytes=len(values) * 70000,
                    flush_rows=64,
                    query_timeout_s=query_timeout_s,
                    cancel_query=cancel,
                )
            summary["resources"] = sampler.summary()
        traces = [json.loads(line) for line in (root / "http-trace.jsonl").read_text().splitlines()]
        expected_results = (
            len(values) if sembench_result is None else len(sembench_result.result_rows)
        )
        if (
            count != maximum
            or len(predictions) != expected_results
            or len(traces) != count
            or any(
                t["status"] != "completed" or t["request_body_sha256"] != t["forwarded_body_sha256"]
                for t in traces
            )
        ):
            raise ValueError("supplier task, POST or row counts differ")
        events = [
            json.loads(line) for line in (root / "method-events.jsonl").read_text().splitlines()
        ]
        calls = list(observations.calls.values())
        write_private_json(root / "calls.json", calls)
        if len(calls) != maximum:
            raise ValueError("LOTUS logical call count differs from its original query")
        protocol_records = [
            json.loads(line) for line in (root / "protocols.jsonl").read_text().splitlines()
        ]
        decoded = [p["response"] for p in protocol_records if isinstance(p["response"], dict)]
        unknown = len(protocol_records) - len(decoded)
        qualified = all(p["http_status"] == 200 for p in protocol_records) and all(
            v.get("model") == model.model_id
            and len(v.get("choices", [])) == 1
            and v["choices"][0].get("finish_reason") == "stop"
            for v in decoded
        )
        summary["native_http_audit"] = dict(
            http_status_counts=dict(Counter(str(p["http_status"]) for p in protocol_records)),
            readable_json_responses=len(decoded),
            qualification_scope="after native result consumption",
            declared_single_stop_answer=qualified if unknown == 0 else None,
            observation_policy="record raw response; supplier parser owns response acceptance",
        )
        if calls:
            missing_values = sum(c["request_values_sha256"] is None for c in calls)
            if missing_values:
                if (
                    lotus_observation_mode != "performance"
                    or arm != "lotus-adapted-native"
                    or missing_values != len(calls)
                    or len(calls) != maximum
                ):
                    raise ValueError(
                        "supplier lacks its declared method-prepared complete call values"
                    )
                summary["method_http_values_audit"] = dict(
                    status="unavailable",
                    logical_calls=len(calls),
                    reason="performance observation omits expected native HTTP construction; actual POST values and counts remain recorded",
                )
            else:
                if Counter(c["request_values_sha256"] for c in calls) != Counter(
                    content_digest(p["request"]) for p in protocol_records
                ):
                    raise ValueError(
                        "supplier actual HTTP values differ from method-prepared complete calls"
                    )
                summary["method_http_values_audit"] = dict(
                    status="passed", logical_calls=len(calls)
                )
        summary.update(
            status="passed",
            rows=len(values),
            actual_posts=count,
            preparation_model_posts=0,
            full_query_seconds=(summary["execution"]["t_query_terminal_ns"] - started) / 1e9,
            method_calls=calls,
            observed_peak_http=proxy_http_peak(traces),
            executor_phases=executor_phase_observations(events),
        )
        if calls:
            summary["call_timing"] = summarize_calls(events, calls)
        else:
            summary["call_timing"] = dict(
                status="unavailable", reason="query required no model calls"
            )
        if sembench_result is not None:
            evaluated = time.monotonic_ns()
            summary["quality"] = sembench_result.quality()
            summary["stages"]["evaluation_seconds"] = (time.monotonic_ns() - evaluated) / 1e9
            summary["sembench"] = dict(
                query_id=sembench_query.query_id,
                dataset_role=sembench_query.dataset.role,
                relation_rows=len(values),
                model_rows=maximum,
                result_rows=expected_results,
                evaluator="unchanged original MovieEvaluator and gold SQL; after query consumer EOF",
                manifest_content_digest=sembench_query.dataset.expected_manifest_digest,
                comparison_prerequisites=[
                    "common response/aggregate resource policy",
                    "matched response interpretation and error qualification",
                    "separate HTTP client and SDK conversion cost evidence",
                    "native early-close additional errors remain under independent repair",
                ],
                timing_scope="query timing excludes post-query official evaluation; preparation includes source verification",
            )
        elif reference_outputs is not None:
            if set(reference_outputs) != set(predictions):
                raise ValueError("reference rows differ from supplier results")
            correct = sum(predictions[k] == v for k, v in reference_outputs.items())
            summary["quality"] = dict(
                rows=len(values), correct=correct, accuracy=correct / len(values)
            )
        else:
            summary["quality"] = dict(status="unavailable", reason="no reference labels supplied")
    except BaseException as failure:
        stop.set()
        errors.record("query", failure)
    finally:
        if execution is not None and owner is None:
            errors.attempt("execution_drain", lambda: _drain_execution(execution))
        if reserved:
            errors.attempt("budget_close", lambda: ledger.close_shared_unit(unit_id))
        if errors.first is not None:
            summary["status"] = "failed"
        summary.update(errors=errors.details, attempted_posts=count, ended_ns=time.monotonic_ns())
        errors.attempt(
            "summary_write",
            lambda: write_private_json(root / "summary.json", redact_json_values(summary)),
        )
    errors.raise_if_failed()
    return summary
