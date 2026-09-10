"""Record a bounded query before evaluation; preserve partial results and failures.

The caller owns SQL, connections, timeouts, model budgets, and service cleanup.
This recorder adds no retry, scheduler, semantic rewrite, or transaction commit.
"""

from contextlib import closing, contextmanager
import json
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


def record_execution(directory: Path, open_rows, *, max_rows: int, max_result_bytes: int,
                     metadata=None, evaluator=None, clock=time.monotonic_ns):
    """Consume one caller-owned iterator and save execution before any evaluator."""
    if any(type(value) is not int or value < 1 for value in (max_rows, max_result_bytes)):
        raise ValueError("positive result limits are required")
    new_private_directory(directory)
    started = clock()
    write_private_json(directory / "started.json", {
        "schema": "semloom.query_recording.v1", "started_ns": started,
        "max_rows": max_rows, "max_result_bytes": max_result_bytes, "metadata": metadata,
    })
    count = size = 0
    phase, error = "recording", None
    try:
        with open_private_text(directory / "results.jsonl") as stream:
            phase = "execute_or_stream"
            with open_rows() as rows:
                iterator = iter(rows)
                while True:
                    phase = "execute_or_stream"
                    try:
                        row = next(iterator)
                    except StopIteration:
                        break
                    phase = "recording"
                    payload = json.dumps({"received_ns": clock(), "row": row},
                                         ensure_ascii=False, allow_nan=False) + "\n"
                    encoded_size = len(payload.encode("utf-8"))
                    if count == max_rows or size + encoded_size > max_result_bytes:
                        raise ValueError("recorded result limit exceeded")
                    stream.write(payload)
                    stream.flush()
                    count += 1
                    size += encoded_size
                phase = "stream_cleanup"
    except BaseException as failure:
        error = _error_details(failure)
        raise
    finally:
        ended = clock()
        execution = {
            "schema": "semloom.query_execution.v1",
            "status": "failed" if error else "completed",
            "started_ns": started, "ended_ns": ended,
            "elapsed_seconds": (ended-started)/1_000_000_000,
            "recorded_rows": count, "recorded_bytes": size,
            "error_phase": phase if error else None, "error": error,
            "timing_scope": "before dispatch through stream consumption and recorder flush",
            "partial_results_are_provisional": error is not None,
        }
        write_private_json(directory / "execution.json", execution)
    if evaluator is not None:
        try:
            with (directory / "results.jsonl").open(encoding="utf-8") as stream:
                result = evaluator([json.loads(line)["row"] for line in stream])
            write_private_json(directory / "evaluation.json", {"status": "completed", "result": result})
        except BaseException as failure:
            # Execution remains completed even if its independent evaluation fails.
            if not (directory / "evaluation.json").exists():
                write_private_json(directory / "evaluation.json", {"status": "failed", "error": _error_details(failure)})
            raise
    return execution


def record_pg_query(connection, query, directory, *, max_rows, max_result_bytes,
                    evaluator=None, metadata=None):
    """Stream an explicit SQL statement; caller must configure its finite timeout."""
    @contextmanager
    def open_rows():
        with connection.cursor() as cursor:
            with closing(cursor.stream(query)) as rows:
                yield rows

    return record_execution(Path(directory), open_rows, max_rows=max_rows,
                            max_result_bytes=max_result_bytes, evaluator=evaluator, metadata=metadata)


def verify_map_completions(inputs, predictions, completions, *, plan: SemanticMapPlan,
                           sequence_ids):
    """Join by declared input sequence and semantic digest, never event arrival order.

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
        "performance_qualified": False,
    }
