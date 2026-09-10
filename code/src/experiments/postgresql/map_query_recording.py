"""Record a bounded query before evaluation; preserve partial results and failures.

The caller owns SQL, connections, timeouts, model budgets, and service cleanup.
This recorder adds no retry, scheduler, semantic rewrite, or transaction commit.
"""

from contextlib import closing, contextmanager
from contextvars import ContextVar
import asyncio
import json
import hashlib
import math
from pathlib import Path
import re
import time

from src.baselines.common.private_artifacts import (
    content_digest, new_private_directory, open_private_text, write_private_json,
)
from src.baselines.common.redact import redact_text
from src.execution_provider.semantic_map import canonical_messages, SemanticMapPlan
from src.execution_provider.wire.map_codec import semantic_payload_digest
from src.baselines.text.map_inputs import text_rows_by_id
from .query_deadline import QueryDeadline


_failure_observer = ContextVar('query_failure_observer', default=None)


@contextmanager
def query_failure_scope():
    """Report the first query failure before an adapter starts resource cleanup.

    Place this inside resource contexts around entry operations or iteration.
    Uninstrumented context managers can only be timed when they return/raise.
    """
    try:
        yield
    except GeneratorExit:
        raise
    except BaseException as failure:
        observer = _failure_observer.get()
        if observer is not None:
            observer(failure)
        raise


def _error_details(error):
    state = getattr(error, "sqlstate", None)
    return {
        "type": type(error).__name__,
        "sqlstate": state if isinstance(state, str) and re.fullmatch(r"[0-9A-Z]{5}", state) else None,
        "message": redact_text(str(error))[:4096],
    }


class _Recording:
    def __init__(self, directory, max_rows, max_result_bytes, flush_rows, metadata, clock):
        if any(type(v) is not int or v < 1 for v in (max_rows, max_result_bytes, flush_rows)):
            raise ValueError("positive result and flush limits are required")
        self.directory, self.clock = Path(directory), clock
        self.max_rows, self.max_bytes, self.flush_rows = max_rows, max_result_bytes, flush_rows
        self.recorded = self.received = self.size = self.recording_ns = 0
        self.phase, self.error = "recording", None
        self.failure = None
        self.errors = {name: None for name in ("query_error", "cleanup_error", "recording_error")}
        self.release = self.first = self.last = self.terminal = self.durable = self.cleanup = None
        self.cancel_triggered = None
        self.stream_entered = None
        self.query_status = "not_started"
        self._context = self.stream = None
        self._hash = hashlib.sha256()
        new_private_directory(self.directory)
        self.started = clock()
        write_private_json(self.directory / "started.json", {
            "schema": "semloom.query_recording.v1", "started_ns": self.started,
            "max_rows": max_rows, "max_result_bytes": max_result_bytes,
            "flush_rows": flush_rows, "metadata": metadata,
        })

    def open_results(self):
        context = open_private_text(self.directory / "results.jsonl")
        self.stream = context.__enter__()
        self._context = context
        self.release = self.clock()
        self.phase, self.query_status = "stream_entry", "running"

    def row(self, row):
        received = self.clock()
        self.received += 1
        self.first = received if self.first is None else self.first
        self.last = received
        self.phase = "recording"
        before = self.clock()
        payload = json.dumps({"received_ns": received, "row": row},
                             ensure_ascii=False, allow_nan=False) + "\n"
        encoded = payload.encode("utf-8")
        if self.recorded == self.max_rows or self.size + len(encoded) > self.max_bytes:
            raise ValueError("recorded result limit exceeded")
        self.stream.write(payload)
        if (self.recorded + 1) % self.flush_rows == 0:
            self.stream.flush()
        self._hash.update(encoded)
        self.recorded += 1
        self.size += len(encoded)
        self.recording_ns += self.clock() - before
        self.phase = "execute_or_stream"

    def query_terminal(self, status):
        self.terminal, self.query_status = self.clock(), status
        self.phase = "stream_cleanup"

    def failed(self, failure, *, phase=None):
        if failure is self.failure:
            return
        phase = phase or self.phase
        is_query = phase in ("execute_or_stream", "stream_entry")
        kind = ("query_error" if is_query else
                "cleanup_error" if phase == "stream_cleanup" else "recording_error")
        detail = dict(_error_details(failure), observed_ns=self.clock())
        if is_query and self.cancel_triggered is None and (
            isinstance(failure, asyncio.CancelledError) or detail["sqlstate"] == "57014"
        ):
            self.cancel_triggered = detail["observed_ns"]
        if self.errors[kind] is None:
            self.errors[kind] = detail
        if self.failure is None:
            self.failure = failure
            self.error, self.error_phase = detail, phase
        if is_query and self.terminal is None:
            self.query_terminal("failed")
        elif self.query_status == "running":
            self.query_status = "consumer_aborted"

    def raise_if_failed(self):
        if self.failure is not None:
            raise self.failure

    def finish(self):
        try:
            if self._context is not None:
                self._context.__exit__(None, None, None)
                self.durable = self.clock()
        except BaseException as failure:
            self.failed(failure, phase="recording_flush")
        finally:
            ended = self.clock()
            self.execution = {
                "schema": "semloom.query_execution.v1", "timing_schema": "semloom.query_timing.v3",
                "status": "failed" if self.error else "completed",
                "started_ns": self.started, "ended_ns": ended,
                "elapsed_seconds": (ended - self.started) / 1_000_000_000,
                "recorded_rows": self.recorded, "received_rows": self.received,
                "recorded_bytes": self.size,
                "results_sha256": self._hash.hexdigest() if self.durable is not None else None,
                "error_phase": self.error_phase if self.error else None, "error": self.error,
                **self.errors,
                "query_status": self.query_status, "t_release_ns": self.release,
                "t_first_row_ns": self.first, "t_last_row_ns": self.last,
                "t_query_terminal_ns": self.terminal, "t_results_durable_ns": self.durable,
                "t_stream_cleanup_ns": self.cleanup,
                "t_stream_entered_ns": self.stream_entered,
                "entry_observation_scope": "context entry return/error; adapters report first failures before internal cleanup",
                "t_cancel_triggered_ns": self.cancel_triggered,
                "query_jct_seconds": ((self.terminal - self.release) / 1_000_000_000
                                      if self.terminal is not None and self.release is not None else None),
                "inline_recording_seconds": self.recording_ns / 1_000_000_000,
                "timing_scope": "before dispatch through stream consumption and recorder flush",
                "query_timing_scope": "application release through observed EOF/error; includes inline recording interference",
                "cleanup_scope": "cursor/iterator only; runner records external resource cleanup separately",
                "partial_results_are_provisional": self.error is not None,
            }
            try:
                write_private_json(self.directory / "execution.json", self.execution)
            except BaseException as failure:
                self.failed(failure, phase="recording_summary")
                # A failed summary must never be mistaken for a completed record.
                # Preserve a separate fallback when the original target alone failed.
                self.execution.update(status="failed", error=self.error,
                                      error_phase=self.error_phase, partial_results_are_provisional=True, **self.errors)
                try:
                    write_private_json(self.directory / "recording-failure.json", self.execution)
                except BaseException:
                    pass  # The primary exception still reaches the owning runner.


def evaluate_recording(directory: Path, evaluator, *, mode="materialized", clock=time.monotonic_ns):
    """Evaluate a completed record separately; stream mode must consume all rows."""
    if mode not in ("materialized", "stream"):
        raise ValueError("unknown evaluation mode")
    directory = Path(directory)
    execution = json.loads((directory / "execution.json").read_text())
    if execution["status"] != "completed":
        raise ValueError("cannot qualify an incomplete execution")
    if (directory / "evaluation.json").exists():
        raise FileExistsError("evaluation already recorded")
    started = clock()
    count = size = 0
    digest = hashlib.sha256()
    failure = None
    try:
        with (directory / "results.jsonl").open("rb") as stream:
            def rows():
                nonlocal count, size
                for line in stream:
                    digest.update(line)
                    size += len(line)
                    count += 1
                    if count > execution['recorded_rows'] or size > execution['recorded_bytes']:
                        raise ValueError('recorded result count/bytes exceeds completed execution')
                    yield json.loads(line)["row"]
            materialized = list(rows()) if mode == "materialized" else None
            def verify():
                if count != execution['recorded_rows']:
                    raise ValueError('evaluator did not consume the complete recorded result')
                if size != execution['recorded_bytes']:
                    raise ValueError('recorded result byte count differs')
                expected = execution.get('results_sha256')
                if not isinstance(expected, str) or digest.hexdigest() != expected:
                    raise ValueError('recorded results changed before evaluation')
                if execution.get('query_status') != 'completed':
                    raise ValueError('cannot qualify an incomplete query')
            if materialized is not None:
                verify()
            result = evaluator(materialized if materialized is not None else rows())
            if count != execution["recorded_rows"] or stream.read(1):
                raise ValueError("evaluator did not consume the complete recorded result")
            verify()
        json.dumps(result, ensure_ascii=False, allow_nan=False)
        report = {"status": "completed", "result": result}
    except BaseException as error:
        failure = error
        report = {"status": "failed", "error": _error_details(error)}
    finally:
        report.update(started_ns=started, ended_ns=clock(), mode=mode, consumed_rows=count,
                      memory_scope="post-query evaluation, separate from query/consumer sampling")
        try:
            write_private_json(directory / "evaluation.json", report)
        except BaseException as error:
            report.update(status="failed", recording_error=_error_details(error))
            failure = failure or error
            try:
                write_private_json(directory / "evaluation-failure.json", report)
            except BaseException:
                pass
    if failure is not None:
        raise failure
    return report


def record_execution(directory: Path, open_rows, *, max_rows: int, max_result_bytes: int,
                     metadata=None, evaluator=None, clock=time.monotonic_ns,
                     flush_rows=1, evaluation_mode="materialized", query_timeout_s=None, cancel_query=None):
    """Record one bounded iterator; retain legacy elapsed and distinct query timing."""
    if evaluation_mode not in ("materialized", "stream"):
        raise ValueError("unknown evaluation mode")
    deadline = QueryDeadline(query_timeout_s, cancel_query,
                             lambda: setattr(recording, "cancel_triggered", clock()))
    recording = _Recording(directory, max_rows, max_result_bytes, flush_rows, metadata, clock)
    deadline_started = False
    token = _failure_observer.set(recording.failed)
    try:
        recording.open_results()
        deadline.start()
        deadline_started = True
        with open_rows() as rows:
            recording.stream_entered, recording.phase = clock(), "execute_or_stream"
            try:
                for row in rows:
                    deadline.check()
                    recording.row(row)
                deadline.check()
                recording.query_terminal("completed")
            except BaseException as failure:
                recording.failed(failure)
                recording.phase = "stream_cleanup"
                raise
            finally:
                try:
                    deadline.stop()
                except BaseException as failure:
                    recording.failed(failure, phase="stream_cleanup")
    except BaseException as failure:
        recording.failed(failure)
    finally:
        if deadline_started:
            try:
                deadline.stop()
            except BaseException as failure:
                recording.failed(failure, phase="stream_cleanup")
        recording.cleanup = clock()
        try:
            recording.finish()
        finally:
            _failure_observer.reset(token)
    recording.raise_if_failed()
    if evaluator is not None:
        evaluate_recording(directory, evaluator, mode=evaluation_mode, clock=clock)
    return recording.execution


async def record_async_execution(directory: Path, open_rows, *, max_rows: int, max_result_bytes: int,
                                 metadata=None, evaluator=None, clock=time.monotonic_ns,
                                 flush_rows=1, evaluation_mode="materialized", query_timeout_s=None):
    """Use the same recording contract with a bounded asynchronous direct producer."""
    if evaluation_mode not in ("materialized", "stream"):
        raise ValueError("unknown evaluation mode")
    if query_timeout_s is not None and (type(query_timeout_s) not in (int, float)
                                       or not math.isfinite(query_timeout_s) or query_timeout_s <= 0):
        raise ValueError("query timeout must be positive")
    recording = _Recording(directory, max_rows, max_result_bytes, flush_rows, metadata, clock)
    deadline = asyncio.timeout(query_timeout_s)
    def observe_failure(failure):
        if deadline.expired() and isinstance(failure,asyncio.CancelledError):
            if recording.cancel_triggered is None:
                recording.cancel_triggered=clock()
            recording.failed(TimeoutError('query deadline exceeded'))
        else:
            recording.failed(failure)
    token = _failure_observer.set(observe_failure)
    try:
        recording.open_results()
        async with deadline:
            async with open_rows() as rows:
                recording.stream_entered, recording.phase = clock(), "execute_or_stream"
                try:
                    async for row in rows:
                        recording.row(row)
                    recording.query_terminal("completed")
                    deadline.reschedule(None)
                except BaseException as failure:
                    if deadline.expired() and isinstance(failure, asyncio.CancelledError):
                        if recording.cancel_triggered is None:
                            recording.cancel_triggered = clock()
                        if recording.failure is None:
                            recording.failed(TimeoutError("query deadline exceeded"))
                    else:
                        recording.failed(failure)
                    if not deadline.expired():
                        deadline.reschedule(None)
                    recording.phase = "stream_cleanup"
                    raise
    except BaseException as failure:
        if deadline.expired() and recording.failure is None:
            recording.cancel_triggered = clock()
            recording.failed(TimeoutError("query deadline exceeded"))
        elif not (deadline.expired() and isinstance(failure, TimeoutError)
                  and isinstance(recording.failure, TimeoutError)):
            recording.failed(failure)
    finally:
        recording.cleanup = clock()
        try:
            recording.finish()
        finally:
            _failure_observer.reset(token)
    recording.raise_if_failed()
    if evaluator is not None:
        evaluate_recording(directory, evaluator, mode=evaluation_mode, clock=clock)
    return recording.execution


def record_pg_query(connection, query, directory, *, max_rows, max_result_bytes,
                    evaluator=None, metadata=None, flush_rows=1, evaluation_mode="materialized",
                    query_timeout_s=None):
    """Stream an explicit SQL statement; caller owns finite timeout and transaction."""
    @contextmanager
    def open_rows():
        with connection.cursor() as cursor:
            with closing(cursor.stream(query)) as rows:
                yield rows

    return record_execution(Path(directory), open_rows, max_rows=max_rows,
                            max_result_bytes=max_result_bytes, evaluator=evaluator, metadata=metadata,
                            flush_rows=flush_rows, evaluation_mode=evaluation_mode,
                            query_timeout_s=query_timeout_s,
                            cancel_query=(lambda: connection.cancel_safe(timeout=1)) if query_timeout_s else None)


def verify_map_completions(inputs, predictions, completions, *, plan: SemanticMapPlan,
                           sequence_ids):
    """Check non-NULL single-Map rows by independent input sequence and digest.

    sequence_ids must describe the actual producer sequence (e.g. a captured input
    binding). This function does not assume SQL without ORDER BY has a fixed order.
    """
    expected = text_rows_by_id(inputs)
    output = text_rows_by_id(predictions)
    if set(output) != set(expected) or len(sequence_ids) != len(expected) or set(sequence_ids) != set(expected):
        raise ValueError("Map result or sequence ID set mismatch")
    seen = set()
    for completion in completions:
        sequence = completion["sequence"]
        if type(sequence) is not int or not 0 <= sequence < len(sequence_ids) or sequence in seen:
            raise ValueError("invalid or duplicate completion sequence")
        seen.add(sequence)
        identity = sequence_ids[sequence]
        text = expected[identity]
        digest = semantic_payload_digest(
            semantic_spec_sha256=plan.digest, input_value=text,
            canonical_messages_utf8=canonical_messages(plan.instruction, text),
        )
        if completion["payload_digest"] != digest or completion["raw_output"] != output[identity]:
            raise ValueError("Map payload or output association mismatch")
        if completion["response_model_id"] != plan.model_id or completion["finish_reason"] != "stop":
            raise ValueError("Map completion identity or finish contract mismatch")
    if len(seen) != len(expected):
        raise ValueError("missing Map completions")
    return {"matched_rows": len(seen), "prediction_values_sha256": content_digest(output)}


def public_execution_summary(directory: Path):
    """Export only measured status/counts/times; never copy raw exceptions or rows."""
    execution = json.loads((directory / "execution.json").read_text())
    if execution.get("schema") != "semloom.query_execution.v1":
        raise ValueError("not a query execution record")
    evaluation_path = directory / "evaluation.json"
    evaluation_status = json.loads(evaluation_path.read_text())["status"] if evaluation_path.exists() else "not_run"
    return {
        "schema": "semloom.query.public_summary.v1",
        "execution_status": execution["status"], "evaluation_status": evaluation_status,
        **{key: execution[key] for key in ("started_ns", "ended_ns", "elapsed_seconds", "recorded_rows")},
        "error_phase": execution["error_phase"],
        "error_type": execution["error"]["type"] if execution["error"] else None,
        "timing_schema": execution.get("timing_schema"),
        "errors": {name: ({"type": execution[name]["type"], "sqlstate": execution[name]["sqlstate"],
                           "observed_ns": execution[name]["observed_ns"]} if execution.get(name) else None)
                   for name in ("query_error", "cleanup_error", "recording_error")},
        **{key: execution.get(key) for key in ("received_rows", "query_status", "t_release_ns",
           "t_first_row_ns", "t_last_row_ns", "t_query_terminal_ns", "t_results_durable_ns",
           "t_stream_cleanup_ns", "t_cancel_triggered_ns", "query_jct_seconds", "inline_recording_seconds")},
        "performance_qualified": False,
    }
