"""Deterministic no-model records UDF and explicit protocol fault fixtures."""
import json
import os
import time
import uuid

import pyarrow as pa

_uid = uuid.uuid4().hex
_batches = 0
INPUT = pa.schema([(name, dtype) for name, dtype in (
    ("call_id", pa.string()), ("row_id", pa.int64()), ("query_id", pa.string()),
    ("stage_id", pa.string()), ("record_json", pa.string()))])
OUTPUT = pa.schema([("call_id", pa.string()), ("row_id", pa.int64()), ("outcome_json", pa.string())])


def infer_records(table):
    global _batches
    if (table.column_names != INPUT.names or table.schema.metadata or
            any(field.type != INPUT.field(i).type or field.metadata for i, field in enumerate(table.schema)) or
            not 0 < len(table) <= 16):
        raise ValueError("echo requires the fixed nonempty records schema")
    table.validate(full=True)
    rows = table.to_pylist()
    stage = (rows[0]["query_id"], rows[0]["stage_id"])
    seen = set()
    records = []
    for row in rows:
        if any(row[name] is None for name in INPUT.names):
            raise ValueError("NULL echo field")
        if any(not row[name] for name in ("call_id", "query_id", "stage_id")):
            raise ValueError("empty echo identity")
        if row["call_id"] in seen or row["row_id"] < 0 or (row["query_id"], row["stage_id"]) != stage:
            raise ValueError("echo occurrence/query/stage differs")
        record = json.loads(row["record_json"])
        if not isinstance(record, dict) or set(record) != {"text"} or not isinstance(record["text"], str):
            raise ValueError("echo accepts only a text metadata field")
        seen.add(row["call_id"])
        records.append(record)
    _batches += 1
    out = []
    for source, record in zip(rows, records, strict=True):
        value = dict(query_id=source["query_id"], stage_id=source["stage_id"], status="ok",
                     result=dict(record=record, _occurrence_id=source["call_id"], _position=source["row_id"]),
                     error_type=None, error=None, model_entry_attempted=False, completion_known=True,
                     worker=dict(worker_pid=os.getpid(), worker_uid=_uid, completed_batches=_batches,
                                 cuda_visible_devices=os.environ.get("CUDA_VISIBLE_DEVICES", "")))
        out.append(dict(call_id=source["call_id"], row_id=source["row_id"],
                        outcome_json=json.dumps(value, ensure_ascii=False, allow_nan=False)))
    return pa.Table.from_pylist(out, schema=OUTPUT)


def reversed_records(table):
    return infer_records(table).take(pa.array(list(reversed(range(len(table))))))


def missing_record(table):
    return infer_records(table).slice(0, len(table) - 1)


def error_records(table):
    rows = infer_records(table).to_pylist()
    for row in rows:
        value = json.loads(row["outcome_json"])
        value.update(status="error", result=None, error_type="EchoFailure", error="injected echo error",
                     completion_known=False)
        row["outcome_json"] = json.dumps(value)
    return pa.Table.from_pylist(rows, schema=OUTPUT)


def malformed_records(table):
    rows = infer_records(table).to_pylist()
    value = json.loads(rows[0]["outcome_json"])
    value["error"] = "success must not carry an error"
    rows[0]["outcome_json"] = json.dumps(value)
    return pa.Table.from_pylist(rows, schema=OUTPUT)


def oversized_records(table):
    rows = infer_records(table).to_pylist()
    rows[0]["outcome_json"] = "x" * 1048577
    return pa.Table.from_pylist(rows, schema=OUTPUT)


def raw_exception(table):
    raise RuntimeError("injected raw Python exception before output semaphore")


def slow_records(table):
    time.sleep(2)
    return infer_records(table)
