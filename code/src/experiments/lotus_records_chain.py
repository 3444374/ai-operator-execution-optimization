"""Public finite ordinary-record chains using the native IMLane author backend."""

import argparse
from dataclasses import dataclass
import json
import math
from pathlib import Path
import sys
import time

from integrations.imlane_records.chain_runtime import digest, read_json, read_owner, preflight_owner, AuthorRunGuard
from src.baselines.common.private_artifacts import new_private_directory, require_outside_git, write_private_json
from src.baselines.common.redact import redact_text
from src.semantic_methods.lotus.records import RecordsResources
from src.semantic_methods.lotus.record_chain import (
    LotusRecordChain, RecordOccurrence, _ordered_columns, records_to_frame, validate_chain_source,
)
from src.serving.backends.vllm_engine import EngineSpec

BACKENDS = ("imlane-author-local-model-chain",)
_DTYPES = {"object", "string", "bool", "boolean", "int64", "Int64", "uint64", "UInt64", "float64", "Float64"}


@dataclass(frozen=True)
class RelationInput:
    records: tuple
    columns: tuple
    source_index: object
    empty_prototype: object


def read_config(path):
    import lotus

    value = read_json(path)
    required = {"schema", "backend", "chain", "engine", "resources", "source_sha256", "owner_sha256"}
    if (type(value) is not dict or set(value) != required
            or value["schema"] != "semloom.records_chain_run.v1" or value["backend"] not in BACKENDS):
        raise ValueError("unsupported ordinary-record chain configuration")
    for name in ("source_sha256", "owner_sha256"):
        if type(value[name]) is not str or len(value[name]) != 64 or any(c not in "0123456789abcdef" for c in value[name]):
            raise ValueError("source and owner configuration SHA256 identities are required")
    raw_chain = value["chain"]
    if (type(raw_chain) is not dict or set(raw_chain) - {"stages", "query_id", "relation_barriers"}
            or not {"stages", "query_id"} <= set(raw_chain)
            or type(raw_chain["stages"]) is not list or not 1 <= len(raw_chain["stages"]) <= 16):
        raise ValueError("the public profile supports one finite chain of 1..16 stages")
    with lotus.settings.context(enable_cache=False):
        chain = LotusRecordChain.from_dict(value["chain"])
    engine, resources = EngineSpec.from_dict(value["engine"]), RecordsResources(**value["resources"])
    if (resources.cpu_workers != 1 or resources.gpu_workers != 1
            or resources.prepare_batch_rows > 16 or resources.model_batch_rows > 16
            or resources.max_active_batches > 2):
        raise ValueError("the IMLane selector requires one executor/GPU, batch rows 1..16 and pending at most two")
    if (engine.max_model_calls is None or engine.max_model_calls > 4096
            or len({stage.method.model_name for stage in chain.stages}) != 1
            or any(stage.generation.max_tokens > engine.max_model_len for stage in chain.stages)):
        raise ValueError("one deployment and a finite compatible engine lifetime allowance are required")
    return value, chain, engine, resources


def read_relation(path, *, expected_sha256=None):
    import pandas as pd

    value = read_json(path, expected_sha256=expected_sha256)
    required = {"schema", "records", "columns", "source_index", "empty_prototype"}
    if (type(value) is not dict or set(value) != required or value["schema"] != "semloom.record_relation.v1"
            or type(value["records"]) is not list or len(value["records"]) > 4096):
        raise ValueError("a finite declared ordinary-record relation is required")
    records = tuple(RecordOccurrence.from_dict(row) for row in value["records"])
    if (len({r.row_id for r in records}) != len(records)
            or len({r.source_position for r in records}) != len(records)
            or [r.source_position for r in records] != sorted(r.source_position for r in records)
            or any(r.index_updates for r in records)):
        raise ValueError("source occurrences must be unique, ordered and have no pending derived writes")
    columns = tuple(_ordered_columns(records, value["columns"]))
    index = value["source_index"]
    if (type(index) is not dict or set(index) != {"dtype", "name"}
            or type(index["dtype"]) is not str or index["dtype"] not in {"int64", "object", "string"}
            or index["name"] is not None and type(index["name"]) not in (str, int)):
        raise ValueError("a single explicit int64/object/string Index is required")
    original = pd.Index([r.source_index for r in records], dtype=index["dtype"], name=index["name"])
    if [(type(v), v) for v in original.tolist()] != [(type(r.source_index), r.source_index) for r in records]:
        raise ValueError("Index metadata must not convert occurrence labels")
    fields = value["empty_prototype"]
    if (type(fields) is not list
            or any(type(f) is not dict or set(f) != {"name", "dtype"} or type(f["name"]) is not str
                   or type(f["dtype"]) is not str or f["dtype"] not in _DTYPES for f in fields)
            or len({f["name"] for f in fields}) != len(fields)):
        raise ValueError("the caller must explicitly declare the final empty relation column types")
    prototype = pd.DataFrame({f["name"]: pd.Series(dtype=f["dtype"]) for f in fields}, index=original[:0])
    return RelationInput(records, columns, original, prototype)


def preflight(config, relation, chain, engine, resources, owner):
    import lotus

    upper = len(relation.records) * len(chain.stages)
    if upper > engine.max_model_calls or upper > owner["budget"]["limit"]:
        raise ValueError("the complete chain exceeds its declared permanent allowance")
    with lotus.settings.context(enable_cache=False):
        method_source = validate_chain_source()
        if set(chain.methods[0].columns) - set(relation.columns):
            raise ValueError("the first method references an undeclared source column")
        chain.methods[0].prepare_rows(relation.records, query_id=chain.query_id)
    environment = preflight_owner(owner, engine)
    return dict(status="preflight_only", inference_calls=0, backend=config["backend"], query_id=chain.query_id,
        max_possible_calls=upper, permanent_unit_requests_minimum=max(1, upper), method_source=method_source,
        author_environment=environment, native_batch_owner="author DBEnd/Coordinator/Lane",
        source_occurrences=len(relation.records), source_columns=list(relation.columns),
        empty_schema_policy="caller-declared final prototype; never inferred from empty rows",
        permanent_unit_claimed=False, external_host_lease_released_by_worker=False)


def _make_backend(**kwargs):
    from integrations.imlane_records.chain_backend import IMLaneChainBackend
    return IMLaneChainBackend(**kwargs)


def _frame_result(frame):
    import numpy as np
    import pandas as pd

    def value(item):
        if item is None or item is pd.NA or item is pd.NaT:
            return None
        if isinstance(item, np.generic):
            return value(item.item())
        if type(item) is float:
            return item if math.isfinite(item) else None
        if type(item) in (str, int, bool):
            return item
        if isinstance(item, (list, tuple)):
            return [value(part) for part in item]
        if type(item) is dict and all(type(key) is str for key in item):
            return {key: value(part) for key, part in item.items()}
        raise ValueError("the result contains a value outside the finite JSON profile")

    return dict(columns=list(frame.columns), index=[value(label) for label in frame.index.tolist()],
        data=[[value(item) for item in row] for row in frame.itertuples(index=False, name=None)],
        dtypes=[str(dtype) for dtype in frame.dtypes],
        index_dtype=str(frame.index.dtype), index_name=value(frame.index.name))


def run_query(config, relation, chain, engine, resources, author_args, model_environment, *, cancelled=None):
    """Consume the existing native chain exactly once; never replay model replies."""
    import lotus

    report = dict(schema="semloom.records_chain_query.v1", backend=config["backend"], query_id=chain.query_id,
        status="failed", calls=[], production_core_used=False, performance_claim=False,
        query_timing_scope="native backend, complete relation traversal, final assembly and close",
        scheduler_owner="author DBEnd/Coordinator/Lane", model_calls_are_not_output_rows=True)
    stream = backend = primary = None
    started = time.monotonic()
    try:
        with lotus.settings.context(enable_cache=False):
            backend = _make_backend(engine=engine, resources=resources, author_args=author_args,
                                    model_environment=model_environment, cancelled=cancelled)
            stream = backend.evaluate(iter(relation.records), chain, expected=relation.records, columns=relation.columns)
            rows = list(stream)
            if stream.completed is not True:
                raise RuntimeError("the native relation did not establish complete successful EOF")
            frame = records_to_frame(rows, columns=stream.columns, source_index=relation.source_index,
                                     empty_prototype=relation.empty_prototype)
            result = _frame_result(frame)
            report.update(status="completed", result=result, output_records=[row.describe() for row in rows])
    except BaseException as error:
        primary = error
        try:
            report["calls"] = list(getattr(error, "chain_calls", []))
        except BaseException as metadata:
            error.add_note("chain failure accounting snapshot also failed: " + type(metadata).__name__)
    finally:
        def failed(error, label):
            nonlocal primary
            report["status"] = "failed"
            if primary is None:
                primary = error
            elif error is not primary:
                primary.add_note(label + ": " + type(error).__name__)
        if stream is not None:
            try:
                report["backend_close"] = stream.close()
                if (report["backend_close"].get("status") != "closed"
                        or report["backend_close"].get("cleanup_errors")):
                    raise RuntimeError("native chain cleanup remains unconfirmed")
            except BaseException as error:
                failed(error, "native chain close also failed")
            for name, target in (("calls", "calls"), ("last_close_report", "backend_close"),
                                 ("phase_history", "phase_history"), ("batch_stage_stats", "stage_stats"),
                                 ("unassociated_results", "unassociated_results")):
                try:
                    report[target] = getattr(stream, name)
                except BaseException as error:
                    report.setdefault("metadata_errors", []).append(dict(field=name, error_type=type(error).__name__))
                    failed(error, "native chain metadata also failed")
            try:
                report["raw_result_batches"] = [dict(batch_key=list(key), records=records)
                                               for key, records in stream.raw_result_batches.items()]
            except BaseException as error:
                failed(error, "native chain raw accounting also failed")
        report["elapsed_seconds"] = time.monotonic() - started
        if primary is not None:
            report.update(status="failed", error_type=type(primary).__name__, error=redact_text(str(primary)))
            primary.query_report = report
    if primary is not None:
        raise primary
    return report


def run_worker(config, relation, chain, engine, resources, root, owner):
    from .imlane_owned_query import run_owned_query

    return run_owned_query(root, owner, query_id=chain.query_id, backend=config["backend"],
        required_requests=max(1, len(relation.records) * len(chain.stages)),
        preflight=lambda: preflight(config, relation, chain, engine, resources, owner),
        execute=lambda args, guard: run_query(config, relation, chain, engine, resources,
            args, owner["model_environment"], cancelled=guard),
        guard_factory=AuthorRunGuard, write_report=write_private_json)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    for name in ("chain-check", "chain-run"):
        command = commands.add_parser(name)
        for field in ("config", "source-json", "owner-config"):
            command.add_argument("--" + field, required=True)
        if name == "chain-run":
            command.add_argument("--output-dir", required=True)
            command.add_argument("--execute-model", action="store_true")
            command.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    config, chain, engine, resources = read_config(args.config)
    if digest(args.source_json) != config["source_sha256"] or digest(args.owner_config) != config["owner_sha256"]:
        raise ValueError("source or owner context changed")
    relation = read_relation(args.source_json, expected_sha256=config["source_sha256"])
    owner = read_owner(args.owner_config, expected_sha256=config["owner_sha256"])
    checked = preflight(config, relation, chain, engine, resources, owner)
    if args.action == "chain-check" or not args.execute_model:
        print(json.dumps(checked, ensure_ascii=False))
        return 0
    root = Path(args.output_dir).resolve()
    require_outside_git(root)
    if args.worker:
        run_worker(config, relation, chain, engine, resources, root, owner)
    else:
        from .lotus_records_supervisor import supervise
        new_private_directory(root)
        write_private_json(root / "preflight.json", checked)
        command = [sys.executable, str(Path(__file__).parents[2] / "scripts/experiments/run_lotus_records_movie.py"),
                   "chain-run", "--config", str(Path(args.config).resolve()),
                   "--source-json", str(Path(args.source_json).resolve()),
                   "--owner-config", str(Path(args.owner_config).resolve()), "--output-dir", str(root),
                   "--execute-model", "--worker"]
        supervise(command, root, timeout_s=resources.startup_timeout_s + resources.query_timeout_s + 30)
    return 0
