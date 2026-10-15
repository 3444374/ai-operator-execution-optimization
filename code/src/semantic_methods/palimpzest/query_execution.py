"""Pinned PZ DAG with query-owned ready calls and original SDK/operator continuations."""

from collections import Counter, deque
from concurrent.futures import Future
from contextvars import ContextVar, copy_context
from copy import copy
from dataclasses import dataclass
import hashlib
import ipaddress
import json
import math
import sys
import threading
import time
from types import MethodType
from urllib.parse import urlsplit

from src.execution_provider.adapters.full_response import encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.baselines.text.frameworks.native_calls import outcome_kind
from .backend import PalimpzestAdapterError
from .native_control import NativeAdaptedExecutor
from .prepared_progress import FiniteBatchProgress
from .plan import FrozenPlan, _snapshot
from .prepared_sdk import (
    GenerationAttempt, PreparedRequestRoute, PreparedSDK, clone_function, validate_prepared_runtime,
)
from .source import validate_source


def freeze_existing_plan(plan):
    """Freeze a caller-selected plan without selecting it again or running sampling."""
    validate_source()
    snapshot = _snapshot(plan)
    return FrozenPlan(plan, plan.plan_id, hashlib.sha256(snapshot).hexdigest(), snapshot,
                      tuple(getattr(op, "datasource", None) for op in plan))


def validate_frozen_plan(frozen):
    validate_source()
    if type(frozen) is not FrozenPlan:
        raise TypeError("PZ prepared execution requires an original FrozenPlan")
    sources = tuple(getattr(op, "datasource", None) for op in frozen.plan)
    if (frozen.plan.plan_id != frozen.plan_id or _snapshot(frozen.plan) != frozen._snapshot_bytes
            or len(sources) != len(frozen._sources)
            or any(actual is not expected for actual, expected in zip(sources, frozen._sources))):
        raise PalimpzestAdapterError("PZ selected plan changed after freezing")
    return frozen.plan


def validate_prepared_plan(plan):
    """Reject unsupported methods before any model submission; never simplify a plan."""
    from palimpzest.constants import Cardinality, Model
    from palimpzest.core.data.iter_dataset import MemoryDataset
    from palimpzest.query.generators.generators import Generator
    from palimpzest.query.operators.aggregate import CountAggregateOp
    from palimpzest.query.operators.convert import LLMConvertBonded
    from palimpzest.query.operators.filter import LLMFilter, NonLLMFilter
    from palimpzest.query.operators.project import ProjectOp
    from palimpzest.query.operators.scan import MarshalAndScanDataOp
    from palimpzest.query.optimizer.plan import PhysicalPlan
    allowed = (MarshalAndScanDataOp, NonLLMFilter, ProjectOp, CountAggregateOp, LLMFilter, LLMConvertBonded)
    def tree(node):
        if type(node) is not PhysicalPlan or len(node.subplans) > 1:
            raise PalimpzestAdapterError("PZ prepared slice supports only an original unary PhysicalPlan")
        for child in node.subplans:
            tree(child)
    tree(plan)
    operators, model_names = [], set()
    for topo_idx, operator in enumerate(plan):
        if type(operator) not in allowed:
            raise PalimpzestAdapterError("unsupported PZ physical operator: " + type(operator).__name__)
        if type(operator) is MarshalAndScanDataOp and type(operator.datasource) is not MemoryDataset:
            raise PalimpzestAdapterError("PZ prepared scan currently requires the fixed MemoryDataset")
        if type(operator) is LLMConvertBonded and (
                operator.cardinality != Cardinality.ONE_TO_ONE or len(operator.generated_fields) != 1):
            raise PalimpzestAdapterError("PZ prepared convert requires one generated field and ONE_TO_ONE")
        if type(operator) in (LLMFilter, LLMConvertBonded):
            generator = operator.generator
            if (type(generator) is not Generator or operator.model != Model.VLLM_QWEN_1_5_0_5B_CHAT
                    or generator.model != operator.model or generator.model_name != operator.model.value
                    or generator.cardinality != Cardinality.ONE_TO_ONE
                    or generator.prompt_strategy.is_audio_prompt() or generator.prompt_strategy.is_image_prompt()):
                raise PalimpzestAdapterError("PZ prepared methods require one fixed original nonstream text model")
            model_names.add(generator.model_name)
        operators.append(dict(occurrence=f"{topo_idx}-{operator.get_full_op_id()}",
                              operator=type(operator).__name__))
    if len(model_names) != 1:
        raise PalimpzestAdapterError("PZ prepared plan requires exactly one model identity")
    _snapshot(plan)  # Includes the immutable ordinary-predicate contract.
    return dict(operators=operators, model=next(iter(model_names)), work_unit="request-count",
                semantic_request_per_input=1, unsupported=["join", "distinct", "LIMIT", "semantic aggregate",
                    "multi-field/followup convert", "ONE_TO_MANY", "sampling/validation phase"])


@dataclass(frozen=True)
class QueryExecutionLimits:
    """Deployment memory envelope; backend request concurrency is configured separately.

    pz_workers records the unchanged native PZ parameter. max_pending_calls bounds
    query-local original stacks and prepared Requests and may exceed that parameter.
    It is not a shared backend concurrency policy or an autoscaling controller.
    """
    pz_workers: int
    max_pending_calls: int
    max_batch_rows: int
    max_input_bytes: int = 32 * 1024**2
    max_batch_bytes: int = 16 * 1024**2
    max_request_bytes: int = 1024**2
    max_response_bytes: int = 1024**2
    max_retained_response_bytes: int = 32 * 1024**2
    batch_timeout_s: float = 120.0
    close_timeout_s: float = 120.0

    def __post_init__(self):
        counts = (self.pz_workers, self.max_pending_calls, self.max_batch_rows, self.max_input_bytes,
                  self.max_batch_bytes, self.max_request_bytes, self.max_response_bytes,
                  self.max_retained_response_bytes)
        if any(type(value) is not int or value < 1 for value in counts):
            raise ValueError("PZ prepared row/byte allowances must be positive integers")
        if not self.max_batch_rows <= self.max_pending_calls:
            raise ValueError("PZ ready snapshot must fit its declared preparation-memory window")
        if (self.max_request_bytes > 8 * 1024**2 or self.max_response_bytes > 8 * 1024**2
                or self.max_request_bytes > self.max_batch_bytes or self.max_batch_bytes > self.max_input_bytes
                or self.max_pending_calls * self.max_response_bytes > self.max_retained_response_bytes):
            raise ValueError("PZ prepared byte allowances are inconsistent")
        for value in (self.batch_timeout_s, self.close_timeout_s):
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 120:
                raise ValueError("PZ prepared deadlines must be at most 120 seconds")


class _StackCancelled(BaseException):
    pass


class _DeferredFuture(Future):
    def __init__(self, operator, argument, occurrence, sequence):
        super().__init__()
        self.operator, self.argument = operator, argument
        self.occurrence, self.sequence = occurrence, sequence
        self.stack = self.call = self.identity = None
        self.call_id = self.row_id = None
        self.started_at = self.ready_at = self.resumed_at = None


class _QueryExecutor:
    def __init__(self, owner, max_workers):
        if max_workers != owner.limits.pz_workers:
            raise PalimpzestAdapterError("PZ original executor configuration changed")
        self.owner = owner

    def __enter__(self):
        return self

    def __exit__(self, kind, error, traceback):
        try:
            self.owner.close(method_complete=error is None)
        except BaseException as cleanup:
            if error is None:
                raise
            error.add_note("PZ prepared cleanup also failed: " + type(cleanup).__name__)

    def submit(self, operator, argument):
        return self.owner.submit(operator, argument)


class PreparedExecutionStrategy:
    """Single query; original PZ DAG code, backend scheduling of complete ready calls.

    PZ retains its source/input/future queues. Deferred futures hold the same operator
    inputs as native submitted futures. Only activated SDK stacks and introduced wire
    values are bounded here; this is not an RSS or source-materialization bound.
    The executor remains caller-owned, including any unconfirmed remote work.
    """

    def __init__(self, native_strategy, model_config, *, original_completion, request_kwargs,
                 validate_response, executor, limits, query_id, observer=None, cancelled=None, record=None,
                 progress=None, request_route=None):
        from palimpzest.query.execution.parallel_execution_strategy import ParallelExecutionStrategy
        if type(native_strategy) is not ParallelExecutionStrategy:
            raise TypeError("PZ prepared execution requires the original ParallelExecutionStrategy")
        if type(model_config) is not FixedModelConfig or type(limits) is not QueryExecutionLimits:
            raise TypeError("PZ prepared execution requires typed fixed model configuration and limits")
        if (native_strategy.max_workers != limits.pz_workers
                or progress is None and not callable(getattr(executor, "iter_batch", None))
                or not all(callable(value) for value in (original_completion, request_kwargs, validate_response))):
            raise ValueError("PZ prepared executor/callbacks differ from the declared native worker contract")
        if (limits.max_batch_rows > executor.max_batch_rows or limits.max_batch_bytes > executor.max_batch_bytes
                or limits.max_batch_rows * limits.max_response_bytes > executor.max_batch_bytes):
            raise ValueError("PZ prepared batches exceed the existing executor byte/row contract")
        endpoint = urlsplit(model_config.endpoint_url)
        try:
            loopback = ipaddress.ip_address(endpoint.hostname).is_loopback
        except ValueError:
            loopback = endpoint.hostname == "localhost"
        if (not loopback or endpoint.scheme != "http" or endpoint.query
                or endpoint.path != "/v1/chat/completions" or model_config.choice_format is not None
                or model_config.timeout_ms > 120000 or type(query_id) is not str
                or not query_id or len(query_id.encode()) > 1024):
            raise ValueError("PZ prepared execution requires the verified finite local model profile")
        self.native_strategy, self.model_config, self.executor, self.limits = native_strategy, model_config, executor, limits
        if request_route is not None and (type(request_route) is not PreparedRequestRoute
                or request_route._registered is not True
                or request_route.route.upstream_url != model_config.endpoint_url
                or request_route.route.job_id != query_id):
            raise ValueError("PZ request route differs from the canonical model or query registration")
        self.request_route = request_route
        self._callbacks = original_completion, request_kwargs, validate_response
        self.query_id, self.observer, self.cancelled, self.record = query_id, observer, cancelled, record
        self.current_invocation = ContextVar("pz-prepared-invocation", default=None)
        self.current_attempt = ContextVar("pz-prepared-generation", default=None)
        self._ready, self._live = deque(), {}
        self.progress = progress if progress is not None else FiniteBatchProgress(executor)
        if not all(callable(getattr(self.progress, name, None)) for name in ("start", "poll", "finish")):
            raise TypeError("PZ prepared progress requires start/poll/finish")
        self._sdk = self._fatal = self._thread_id = None
        self._started = self._closing = self._closed = self._unconfirmed = False
        self._sequence = self._prepared = self._outcomes = self._completed = 0
        self._input_bytes = self._peak_input_bytes = self._peak_live = 0
        self._prepare_secs = self._finish_secs = 0.0
        self._batches = Counter()
        self._occurrences, self._semantic, self._expected = {}, set(), {}
        self.last_close_report = None

    def fail(self, error):
        if self._fatal is None:
            self._fatal = error if isinstance(error, PalimpzestAdapterError) else PalimpzestAdapterError(
                "PZ prepared execution contract failed: " + type(error).__name__)

    def check_open(self):
        if threading.get_ident() != self._thread_id:
            raise PalimpzestAdapterError("PZ original continuations must run on their query thread")
        if self._unconfirmed and self._fatal is None:
            self.fail(PalimpzestAdapterError("PZ backend retains unconfirmed work; further admission stopped"))
        if self._fatal is not None:
            raise self._fatal
        if self._closing or self._stop_requested():
            raise PalimpzestAdapterError("PZ prepared execution stopped before submission")

    def _stop_requested(self):
        return self._fatal is not None or self._unconfirmed or bool(self.cancelled and self.cancelled())

    def _emit(self, event, **values):
        if self.record is not None:
            try:
                self.record(dict(event=event, query_id=self.query_id, **values))
            except BaseException as error:
                self.fail(error)
                raise self._fatal from error

    def submit(self, operator, argument):
        self.check_open()
        occurrence = self._occurrences.get(id(operator))
        if occurrence is None:
            raise PalimpzestAdapterError("PZ submitted a callable outside its selected physical plan")
        future = _DeferredFuture(operator, argument, occurrence, self._sequence)
        self._sequence += 1
        if id(operator) not in self._semantic:
            future.set_running_or_notify_cancel()
            try:
                future.set_result(operator(argument))
            except BaseException as error:
                future.set_exception(error)
            future.operator = future.argument = None
        return future

    def _activate(self, futures):
        import greenlet
        for future in futures:
            if len(self._live) >= self.limits.max_pending_calls:
                break
            if future.done() or future.stack is not None:
                continue
            self.check_open()
            future.set_running_or_notify_cancel()
            future.started_at = time.monotonic()
            token = self.current_invocation.set(future)
            future.stack = greenlet.greenlet(lambda future=future: future.operator(future.argument))
            future.stack.gr_context = copy_context()
            self.current_invocation.reset(token)
            self._live[future.sequence] = future
            self._peak_live = max(self._peak_live, len(self._live))
            self._resume(future)

    def begin_generation(self, candidate, fields):
        self.check_open()
        future = self.current_invocation.get()
        if future is None or future.identity is not None or type(fields) is not dict or len(fields) != 1:
            raise PalimpzestAdapterError("PZ prepared method attempted an unsupported generation step")
        future.identity = dict(query_id=self.query_id, plan_fingerprint=self._fingerprint,
            physical_operator=future.occurrence, input_occurrence=future.sequence,
            source_indices=list(candidate.source_indices), record_id=candidate.id,
            parent_ids=candidate.parent_ids, cardinality_idx=candidate.cardinality_idx,
            generation_step=0, attempt=0)
        digest = hashlib.sha256(json.dumps(future.identity, sort_keys=True).encode()).hexdigest()
        call_id = "pz:" + digest
        self._emit("pz_generation_identity", call_id=call_id, identity=future.identity)
        return GenerationAttempt(call_id, future.sequence, tuple(candidate.source_indices))

    def prepared(self, call, request, native_sender):
        import greenlet
        future = self.current_invocation.get()
        if (future is None or future.call is not None
                or self._input_bytes + len(call.payload) > self.limits.max_input_bytes):
            error = PalimpzestAdapterError("PZ prepared request exceeds its owned continuation allowance")
            self.fail(error)
            raise error
        future.call, future.ready_at = call, time.monotonic()
        future.call_id, future.row_id = call.call_id, call.row_id
        self._prepare_secs += future.ready_at - future.started_at
        self._input_bytes += len(call.payload)
        self._peak_input_bytes = max(self._peak_input_bytes, self._input_bytes)
        self._prepared += 1
        if isinstance(self.executor, NativeAdaptedExecutor):
            self.executor.bind_sdk_request(call.call_id, request, native_sender)
        self._ready.append(future)
        return greenlet.getcurrent().parent.switch()

    def _resume(self, future, outcome=None, error=None):
        try:
            if outcome is not None or error is not None:
                future.resumed_at = time.monotonic()
            if error is not None:
                result = future.stack.throw(error)
            else:
                result = future.stack.switch() if outcome is None else future.stack.switch(outcome)
            if not future.stack.dead:
                if future.call is None:
                    raise PalimpzestAdapterError("PZ original stack suspended outside the SDK send boundary")
                return
            if future.identity is not None:
                children = [dict(output_child=index, record_id=record.id, parent_ids=record.parent_ids,
                                 source_indices=list(record.source_indices), cardinality_idx=record.cardinality_idx)
                            for index, record in enumerate(result.data_records)]
                self._emit("pz_operator_finished", call_id="pz:" + hashlib.sha256(
                    json.dumps(future.identity, sort_keys=True).encode()).hexdigest(),
                    identity=future.identity, output_children=children)
            future.set_result(result)
        except BaseException as failure:
            if isinstance(failure, PalimpzestAdapterError):
                self.fail(failure)
            if not future.done():
                # Future.result() supplies the consumer traceback. Keeping the
                # dead original stack here would retain its prepared Request.
                future.set_exception(failure.with_traceback(None))
        finally:
            if future.stack.dead:
                if future.resumed_at is not None:
                    self._finish_secs += time.monotonic() - future.resumed_at
                self._completed += 1
                self._live.pop(future.sequence, None)
                if future.call is not None:
                    self._input_bytes -= len(future.call.payload)
                # Original PZ may keep completed futures in its queues. Retain
                # only the small late-outcome identity, not a finished SDK stack.
                future.call = future.stack = future.operator = future.argument = None

    def _dispatch(self):
        if not self.progress.accepts_ready or not self._ready or self._stop_requested():
            return
        batch, size = [], 0
        for future in self._ready:
            if len(batch) == self.limits.max_batch_rows:
                break
            if size + len(future.call.payload) > self.limits.max_batch_bytes:
                break
            batch.append(future)
            size += len(future.call.payload)
        calls = tuple(future.call for future in batch)
        accepted = self.progress.start(calls, identities={future.call.call_id: future.identity for future in batch},
                                       cancelled=self._stop_requested, timeout_s=self.limits.batch_timeout_s)
        if type(accepted) is not int or not 0 <= accepted <= len(batch):
            raise PalimpzestAdapterError("PZ progress changed its accepted-prefix contract")
        if accepted:
            for future in batch[:accepted]:
                if self._ready.popleft() is not future or future.call.call_id in self._expected:
                    raise PalimpzestAdapterError("PZ progress repeated an accepted ready occurrence")
                self._expected[future.call.call_id] = future
            self._batches[accepted] += 1

    def _event(self, timeout):
        event = self.progress.poll(timeout)
        if event is None:
            return
        kind, value = event
        if kind == "outcome":
            future = self._expected.pop(value.call_id, None)
            if (future is None or value.row_id != future.row_id
                    or outcome_kind(value) == "execution_error"
                    or value.response is not None and len(encode_full_response(value.response)) > self.limits.max_response_bytes):
                self._unconfirmed = True
                self.fail(PalimpzestAdapterError("PZ backend changed its finite complete-call result contract"))
                return
            self._outcomes += 1
            if future.done():
                self._emit("pz_late_backend_outcome", call_id=value.call_id,
                           outcome_kind=outcome_kind(value))
            else:
                self._resume(future, outcome=value)
            # First let the original SDK/Generator account this provider failure
            # and retain its default. Unknown work then stops the whole query's
            # later admission, rather than being interpreted as a reusable False.
            if value.transport_error:
                self._unconfirmed = True
                self.fail(PalimpzestAdapterError("PZ transport completion is unconfirmed; further admission stopped"))
            if value.response is not None and 300 <= value.response.status_code < 400:
                self.fail(PalimpzestAdapterError("PZ nonredirect loopback profile does not support HTTP redirects"))
        elif kind == "error":
            self.fail(value)
        elif kind == "eof":
            if self.progress.finite_batches and self._expected:
                self._unconfirmed = True
                self.fail(PalimpzestAdapterError("PZ backend ended before all prepared calls completed"))
            if value and value.get("unconfirmed_work"):
                self._unconfirmed = True
                self.fail(PalimpzestAdapterError("PZ backend close retains unconfirmed work; further admission stopped"))

    def wait(self, futures, timeout):
        futures = tuple(futures)
        if not futures:
            return set(), set()
        deadline = time.monotonic() + timeout
        while True:
            self.check_open()
            self._activate(futures)
            self._dispatch()
            done = {future for future in futures if future.done()}
            if len(done) == len(futures) or time.monotonic() >= deadline:
                return done, set(futures) - done
            if not self.progress.active and not self._ready and not self._live:
                raise PalimpzestAdapterError("PZ deferred futures cannot make progress")
            self._event(max(0, deadline - time.monotonic()))

    def execute_plan(self, plan):
        from palimpzest.query.execution.parallel_execution_strategy import ParallelExecutionStrategy
        from palimpzest.query.optimizer.plan import PhysicalPlan
        if self._started:
            raise PalimpzestAdapterError("PZ prepared strategy is single-query and cannot be reused")
        validate_prepared_runtime()
        description = validate_prepared_plan(plan)
        self._fingerprint = freeze_existing_plan(plan).fingerprint
        self._thread_id, self._started = threading.get_ident(), True
        self._sdk = PreparedSDK(self, self.model_config, *self._callbacks, request_route=self.request_route)
        def private_tree(node):
            operator = copy(node.operator)
            if hasattr(operator, "generator"):
                if operator.generator.api_base != self.model_config.endpoint_url.removesuffix("/chat/completions"):
                    raise PalimpzestAdapterError("PZ original Generator API base differs from the selected local model")
                operator.generator = self._sdk.generator(operator.generator)
                self._semantic.add(id(operator))
            return PhysicalPlan(operator, [private_tree(child) for child in node.subplans], node.plan_cost)
        try:
            private_plan = private_tree(plan)
            if _snapshot(private_plan) != _snapshot(plan):
                raise PalimpzestAdapterError("PZ query-private operator copy changed the selected plan")
            self._occurrences = {id(op): item["occurrence"] for op, item in
                                 zip(private_plan, description["operators"], strict=True)}
            strategy = copy(self.native_strategy)
            strategy.execute_plan = MethodType(clone_function(ParallelExecutionStrategy.execute_plan), strategy)
            strategy._execute_plan = MethodType(clone_function(ParallelExecutionStrategy._execute_plan,
                ThreadPoolExecutor=lambda max_workers: _QueryExecutor(self, max_workers)), strategy)
            strategy._process_future_results = MethodType(clone_function(
                ParallelExecutionStrategy._process_future_results, wait=self.wait), strategy)
            return strategy.execute_plan(plan=private_plan)
        finally:
            if not self._closed:
                primary = sys.exception()
                try:
                    self.close(method_complete=False)
                except BaseException as cleanup:
                    if primary is None:
                        raise
                    if cleanup is not primary:
                        primary.add_note("PZ prepared final cleanup also failed: " + type(cleanup).__name__)

    def close(self, *, method_complete=False):
        if self._closed:
            return
        self._closing = True
        deadline = time.monotonic() + self.limits.close_timeout_s
        # Only a failing/cancelled method may leave stacks pending at executor exit.
        if self._live and self._fatal is None:
            self.fail(PalimpzestAdapterError("PZ method exited with unfinished original operator stacks"))
        if self.progress.finite_batches:
            while self.progress.active and time.monotonic() < deadline:
                self._event(min(0.05, max(0, deadline - time.monotonic())))
        progress_report = self.progress.finish(method_complete=method_complete and not self._live and self._fatal is None,
                                               timeout_s=max(0, deadline - time.monotonic()))
        if self.progress.active or progress_report.get("unconfirmed_work"):
            self._unconfirmed = True
            self.fail(PalimpzestAdapterError("PZ progress finish retains unconfirmed work; query completion rejected"))
        for future in tuple(self._live.values()):
            if self.progress.finite_batches:
                self._unconfirmed |= future.call_id in self._expected
            self._resume(future, error=_StackCancelled("PZ method cancelled; backend ownership recorded separately"))
        self._ready.clear()
        if not self.progress.active and self._sdk is not None:
            self._sdk.close()
        self._closed = not self.progress.active
        self.last_close_report = self.snapshot()
        if self._fatal is not None:
            raise self._fatal
        if not self._closed:
            raise PalimpzestAdapterError("PZ prepared close retains backend iterator ownership")

    def snapshot(self):
        return dict(schema="semloom.palimpzest_prepared_execution.v1", source_dag_owner="original PZ parallel DAG code",
            supplier="query-private deferred Future and original same-thread operator stack",
            pz_threadpool_created=False, shared_pz_or_sdk_patches=False, work_unit="request-count",
            generators_completed=self._completed, prepared_calls=self._prepared, outcomes_received=self._outcomes,
            active_stacks=len(self._live), peak_active_stacks=self._peak_live,
            retained_request_bytes=self._input_bytes, peak_retained_request_bytes=self._peak_input_bytes,
            batch_size_counts=dict(self._batches), preparation_seconds=self._prepare_secs,
            finish_seconds=self._finish_secs, iterator_reader_stopped=not self.progress.active,
            progress_active=self.progress.active, progress_type=type(self.progress).__name__,
            unconfirmed_work=self._unconfirmed, closed=self._closed, reuse_allowed=False,
            fatal_error_type=None if self._fatal is None else type(self._fatal).__name__,
            remote_http_stopped="unavailable", actual_http_attempts="separate actual POST observer required",
            preparation_memory_slots=self.limits.max_pending_calls,
            native_pz_workers_identity=self.limits.pz_workers,
            backend_concurrency_owner="selected backend; configured independently of preparation memory",
            finite_batch_lifecycle=self.progress.finite_batches,
            accepts_ready_during_active=not self.progress.finite_batches,
            http_profile="explicit nonredirect loopback profile; redirects unsupported",
            original_sdk_default_redirect_semantics_qualified=False,
            request_route=None if self.request_route is None else self.request_route.describe(),
            executor_close_report=getattr(self.executor, "last_close_report", None))
