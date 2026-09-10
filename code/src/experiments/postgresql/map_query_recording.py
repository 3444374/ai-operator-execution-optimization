"""Record a bounded query before evaluation; preserve partial results and failures.

The caller owns SQL, connections, timeouts, model budgets, and service cleanup.
This recorder adds no retry, scheduler, semantic rewrite, or transaction commit.
"""

from contextlib import closing, contextmanager
import json
import hashlib
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
        self.release = self.first = self.last = self.terminal = self.durable = self.cleanup = None
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
        self.phase, self.query_status = "execute_or_stream", "running"

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

    def failed(self, failure):
        self.error = _error_details(failure)
        self.error_phase = self.phase
        if self.phase == "execute_or_stream" and self.terminal is None:
            self.query_terminal("failed")
        elif self.query_status == "running":
            self.query_status = "consumer_aborted"
        self.cleanup = self.clock()

    def finish(self):
        try:
            if self._context is not None:
                self._context.__exit__(None, None, None)
                self.durable = self.clock()
        except BaseException as failure:
            self.error = _error_details(failure)
            self.error_phase = "recording_flush"
            raise
        finally:
            ended = self.clock()
            self.execution = {
                "schema": "semloom.query_execution.v1", "timing_schema": "semloom.query_timing.v2",
                "status": "failed" if self.error else "completed",
                "started_ns": self.started, "ended_ns": ended,
                "elapsed_seconds": (ended - self.started) / 1_000_000_000,
                "recorded_rows": self.recorded, "received_rows": self.received,
                "recorded_bytes": self.size,
                "results_sha256": self._hash.hexdigest() if self.durable is not None else None,
                "error_phase": self.error_phase if self.error else None, "error": self.error,
                "query_status": self.query_status, "t_release_ns": self.release,
                "t_first_row_ns": self.first, "t_last_row_ns": self.last,
                "t_query_terminal_ns": self.terminal, "t_results_durable_ns": self.durable,
                "t_stream_cleanup_ns": self.cleanup,
                "query_jct_seconds": ((self.terminal - self.release) / 1_000_000_000
                                      if self.terminal is not None and self.release is not None else None),
                "inline_recording_seconds": self.recording_ns / 1_000_000_000,
                "timing_scope": "before dispatch through stream consumption and recorder flush",
                "query_timing_scope": "application release through observed EOF/error; includes inline recording interference",
                "cleanup_scope": "cursor/iterator only; runner records external resource cleanup separately",
                "partial_results_are_provisional": self.error is not None,
            }
            write_private_json(self.directory / "execution.json", self.execution)


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
    count = 0
    digest = hashlib.sha256()
    try:
        with (directory / "results.jsonl").open("rb") as stream:
            def rows():
                nonlocal count
                for line in stream:
                    digest.update(line)
                    count += 1
                    yield json.loads(line)["row"]
            result = evaluator(list(rows()) if mode == "materialized" else rows())
            if count != execution["recorded_rows"] or stream.read(1):
                raise ValueError("evaluator did not consume the complete recorded result")
            expected = execution.get("results_sha256")
            if expected is not None and digest.hexdigest() != expected:
                raise ValueError("recorded results changed before evaluation")
        json.dumps(result, ensure_ascii=False, allow_nan=False)
        report = {"status": "completed", "result": result}
    except BaseException as failure:
        report = {"status": "failed", "error": _error_details(failure)}
        raise
    finally:
        report.update(started_ns=started, ended_ns=clock(), mode=mode, consumed_rows=count,
                      memory_scope="post-query evaluation, separate from query/consumer sampling")
        write_private_json(directory / "evaluation.json", report)
    return report


def record_execution(directory: Path, open_rows, *, max_rows: int, max_result_bytes: int,
                     metadata=None, evaluator=None, clock=time.monotonic_ns,
                     flush_rows=1, evaluation_mode="materialized"):
    """Record one bounded iterator; retain legacy elapsed and distinct query timing."""
    if evaluation_mode not in ("materialized", "stream"):
        raise ValueError("unknown evaluation mode")
    recording = _Recording(directory, max_rows, max_result_bytes, flush_rows, metadata, clock)
    try:
        recording.open_results()
        with open_rows() as rows:
            for row in rows:
                recording.row(row)
            recording.query_terminal("completed")
        recording.cleanup = clock()
    except BaseException as failure:
        recording.failed(failure)
        raise
    finally:
        recording.finish()
    if evaluator is not None:
        evaluate_recording(directory, evaluator, mode=evaluation_mode, clock=clock)
    return recording.execution


async def record_async_execution(directory: Path, open_rows, *, max_rows: int, max_result_bytes: int,
                                 metadata=None, evaluator=None, clock=time.monotonic_ns,
                                 flush_rows=1, evaluation_mode="materialized"):
    """Use the same recording contract with a bounded asynchronous direct producer."""
    if evaluation_mode not in ("materialized", "stream"):
        raise ValueError("unknown evaluation mode")
    recording = _Recording(directory, max_rows, max_result_bytes, flush_rows, metadata, clock)
    try:
        recording.open_results()
        async with open_rows() as rows:
            async for row in rows:
                recording.row(row)
            recording.query_terminal("completed")
        recording.cleanup = clock()
    except BaseException as failure:
        recording.failed(failure)
        raise
    finally:
        recording.finish()
    if evaluator is not None:
        evaluate_recording(directory, evaluator, mode=evaluation_mode, clock=clock)
    return recording.execution


def record_pg_query(connection, query, directory, *, max_rows, max_result_bytes,
                    evaluator=None, metadata=None, flush_rows=1, evaluation_mode="materialized"):
    """Stream an explicit SQL statement; caller owns finite timeout and transaction."""
    @contextmanager
    def open_rows():
        with connection.cursor() as cursor:
            with closing(cursor.stream(query)) as rows:
                yield rows

    return record_execution(Path(directory), open_rows, max_rows=max_rows,
                            max_result_bytes=max_result_bytes, evaluator=evaluator, metadata=metadata,
                            flush_rows=flush_rows, evaluation_mode=evaluation_mode)


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
        **{key: execution.get(key) for key in ("received_rows", "query_status", "t_release_ns",
           "t_first_row_ns", "t_last_row_ns", "t_query_terminal_ns", "t_results_durable_ns",
           "t_stream_cleanup_ns", "query_jct_seconds", "inline_recording_seconds")},
        "performance_qualified": False,
    }
