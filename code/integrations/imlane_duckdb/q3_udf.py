"""Movie Q3 Arrow kernel for the author's DuckDB/CPython executor.

The author converts four SQL VARCHAR arguments to c0..c3. Its result converter
reads the first column as raw numeric storage, so this kernel returns int8,
not Arrow's bit-packed bool. DuckDB and IMLane retain batching and scheduling.

SEMLOOM_IMLANE_Q3_CONFIG names a private JSON file with provider_path (a Python
file), provider_function, provider_mode and receipt_dir. The synchronous
provider receives one messages_batch and returns one list[str]. Configuration
and the callable are loaded once per executor/configuration; use a new executor
to change them. A software fixture must be explicitly labeled software_fixture.
The actual model provider and its resource lifecycle remain unbound/pending.
"""

from functools import cache
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import uuid

import pyarrow as pa

from src.baselines.text.sembench_movie_native import q3_decision, q3_messages
from src.infrastructure.private_artifacts import (
    content_digest, publish_private_json, require_outside_git,
)


CONFIG_ENV = "SEMLOOM_IMLANE_Q3_CONFIG"
INPUT = pa.schema([(f"c{i}", pa.string()) for i in range(4)])
OUTPUT = pa.schema([pa.field("positive", pa.int8(), nullable=False)])
IDENTITY = ("occurrence_id", "id", "reviewId", "reviewText")


def _input_rows(table):
    if not isinstance(table, pa.Table):
        raise TypeError("IMLane Q3 requires a pyarrow.Table")
    table.validate(full=True)
    if (table.column_names != INPUT.names
            or any(field.type != INPUT.field(i).type for i, field in enumerate(table.schema))
            or any(column.null_count for column in table.columns)):
        raise ValueError("IMLane Q3 requires four non-NULL UTF8 columns c0..c3")
    rows = [dict(zip(IDENTITY, values, strict=True))
            for values in zip(*(column.to_pylist() for column in table.columns), strict=True)]
    identities = [row["occurrence_id"] for row in rows]
    if (any(not identity for identity in identities) or len(set(identities)) != len(identities)
            or any(row["id"] != "taken_3" for row in rows)):
        raise ValueError("IMLane Q3 requires distinct occurrences after the taken_3 filter")
    return rows


@cache
def _configuration(config_path):
    path = Path(config_path)
    require_outside_git(path)
    config = json.loads(path.read_text(encoding="utf-8"))
    directory = Path(config["receipt_dir"]).resolve()
    require_outside_git(directory)
    if not directory.is_dir():
        raise ValueError("IMLane Q3 requires an existing private receipt directory")
    mode = config["provider_mode"]
    if not isinstance(mode, str) or not mode:
        raise ValueError("IMLane Q3 requires an explicit provider_mode")
    source = Path(config["provider_path"]).resolve()
    source_bytes = source.read_bytes()
    digest = hashlib.sha256(source_bytes).hexdigest()
    name = "_imlane_duckdb_q3_provider_" + hashlib.sha256(str(path).encode()).hexdigest()
    spec = importlib.util.spec_from_file_location(name, source)
    if spec is None or spec.loader is None:
        raise ImportError("IMLane Q3 provider_path must identify a Python module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        exec(compile(source_bytes, str(source), "exec"), module.__dict__)
        provider = getattr(module, config["provider_function"])
        if not callable(provider):
            raise TypeError("IMLane Q3 provider_function must be callable")
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return provider, directory, mode, digest


def _output(decisions):
    return pa.Table.from_arrays([pa.array(decisions, type=pa.int8())], schema=OUTPUT)


def infer_q3(table):
    """Return one decision per row and preserve one batch-provider invocation."""
    rows = _input_rows(table)
    if not rows:
        return _output([])
    config_path = os.environ.get(CONFIG_ENV)
    if not config_path:
        raise RuntimeError("IMLane Q3 batch provider is unconfigured; model binding is pending")
    provider, directory, mode, provider_digest = _configuration(str(Path(config_path).resolve()))
    messages_batch = [q3_messages(row["reviewText"]) for row in rows]
    sources = [dict(occurrence_id=row["occurrence_id"], id=row["id"], reviewId=row["reviewId"],
                    messages_sha256=content_digest(messages))
               for row, messages in zip(rows, messages_batch, strict=True)]
    call = dict(logical_rows=len(rows), status="unknown", returned=False,
                started_monotonic_ns=time.monotonic_ns())
    receipt = dict(schema="semloom.imlane.duckdb.q3.batch.v1", provider_mode=mode,
                   provider_source_sha256=provider_digest, worker_pid=os.getpid(),
                   logical_rows=len(rows), physical_model_attempts=None,
                   provider_calls=[call], sources=sources,
                   status="unknown", predictions=[])
    destination = directory / ("q3-batch-" + uuid.uuid4().hex + ".json")
    replies = None
    try:
        replies = provider(messages_batch)
        call.update(returned=True, finished_monotonic_ns=time.monotonic_ns())
        if not isinstance(replies, list) or any(not isinstance(reply, str) for reply in replies):
            raise TypeError("IMLane Q3 batch provider must return list[str]")
        if len(replies) != len(rows):
            raise ValueError("IMLane Q3 batch provider changed row cardinality")
        decisions = [int(q3_decision(reply)) for reply in replies]
        receipt["predictions"] = [dict(source, generated_text=reply, decision=decision)
                                  for source, reply, decision in zip(sources, replies, decisions, strict=True)]
        output = _output(decisions)
        call["status"] = receipt["status"] = "succeeded"
    except BaseException as error:
        receipt.update(status="failed", error_type=type(error).__name__)
        call.setdefault("finished_monotonic_ns", time.monotonic_ns())
        if isinstance(replies, list) and all(isinstance(reply, str) for reply in replies):
            receipt["unassociated_reply_texts"] = replies
        try:
            publish_private_json(destination, receipt)
        except Exception as audit_error:
            error.add_note("IMLane Q3 receipt publication failed: " + type(audit_error).__name__)
        raise
    publish_private_json(destination, receipt)
    return output
