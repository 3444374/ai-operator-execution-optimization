"""Finite generic Filter cascades through the existing native IMLane backend."""

import argparse
from dataclasses import replace
from copy import deepcopy
import json
from pathlib import Path
import sys
import time

from integrations.imlane_records.chain_runtime import (
    AuthorRunGuard, read_json, read_owner, preflight_owner,
)
from src.baselines.common.private_artifacts import new_private_directory, require_outside_git, write_private_json
from src.baselines.common.redact import redact_text
from src.experiments.lotus_records_chain import read_relation, _frame_result
from src.semantic_methods.lotus.engine import bind_filter_method, make_lm, validate_generation
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


BACKEND = "imlane-author-local-model-cascade"


def read_config(path):
    import lotus
    from lotus.types import CascadeArgs
    from src.semantic_methods.lotus.cascade import cascade_identity, _method_columns

    value = read_json(path)
    required = {"schema", "backend", "query_id", "method", "helper_model_name", "engines",
                "generations", "cascade_args", "resources", "max_calls", "source_sha256", "owner_sha256"}
    if (type(value) is not dict or set(value) != required
            or value["schema"] != "semloom.records_cascade_run.v1" or value["backend"] != BACKEND):
        raise ValueError("an explicit generic IMLane cascade profile is required")
    for name in ("source_sha256", "owner_sha256"):
        digest = value[name]
        if type(digest) is not str or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("source and owner SHA256 identities are required")
    if type(value["method"]) is not dict or "instruction" not in value["method"]:
        raise ValueError("the generic cascade requires an explicit caller instruction")
    with lotus.settings.context(enable_cache=False):
        method = bind_filter_method(FilterMethod.from_dict(value["method"]))
    if method.resolved_input_columns != _method_columns(method.instruction):
        raise ValueError("cascade method columns must retain the original instruction order")
    helper = FilterMethod(value["helper_model_name"], method.instruction,
                          default=method.default, system_prompt=method.system_prompt)
    helper.describe()
    for name in ("engines", "generations"):
        if type(value[name]) is not dict or set(value[name]) != {"main", "helper"}:
            raise ValueError(name + " must declare exactly main and helper")
    generations = {role: GenerationSpec.from_dict(raw) for role, raw in value["generations"].items()}
    if (generations["main"].logprobs is not None or generations["helper"].logprobs is None
            or not 1 <= generations["helper"].logprobs <= 20):
        raise ValueError("main logprobs must be None and helper top logprobs must be 1..20")
    for role, binding in (("main", method), ("helper", helper)):
        validate_generation(binding, replace(generations[role], logprobs=None))
    raw = value["cascade_args"]
    if type(raw) is not dict or set(raw) - set(CascadeArgs.model_fields):
        raise ValueError("cascade_args must contain only original CascadeArgs fields")
    args = CascadeArgs.model_validate_json(json.dumps(raw), strict=True)
    cascade_identity(value["query_id"], "preflight", 0, 0, model_role="helper", stage="helper_score")
    if type(value["max_calls"]) is not int or not 1 <= value["max_calls"] <= 12288:
        raise ValueError("the finite global max_calls must be 1..12288")
    resources = RecordsResources(**value["resources"])
    if (resources.cpu_workers != 1 or resources.gpu_workers != 1 or resources.prepare_batch_rows > 16
            or resources.model_batch_rows > 16 or resources.max_active_batches > 2):
        raise ValueError("IMLane needs one executor/GPU, 1..16 rows and pending at most two")
    engines = {role: EngineSpec.from_dict(raw) for role, raw in value["engines"].items()}
    if any(spec.max_model_calls is None or spec.max_model_calls > 4096 for spec in engines.values()):
        raise ValueError("both model lifetimes need explicit caps at most 4096")
    if Path(engines["main"].model_path).resolve() == Path(engines["helper"].model_path).resolve():
        raise ValueError("distinct explicit helper and main deployment paths are required")
    if any(generations[role].max_tokens > engines[role].max_model_len for role in engines):
        raise ValueError("cascade max_tokens exceeds its deployment max_model_len")
    return value, method, generations, resources, engines, args


def _methods(method, helper_model, args):
    return dict(main=method, helper=bind_filter_method(FilterMethod(helper_model,
        args.helper_filter_instruction or method.instruction, default=method.default,
        system_prompt=method.system_prompt, output_tokens=method.output_tokens)))


def _frame(relation, methods):
    import csv
    import pandas as pd
    from src.baselines.text.cascade_records_source import MAX_FIELD_CHARS

    columns = tuple(dict.fromkeys((*methods["main"].resolved_input_columns,
                                   *methods["helper"].resolved_input_columns)))
    if not set(columns) <= set(relation.columns):
        raise ValueError("source must cover both roles' original text columns")
    if csv.field_size_limit() < MAX_FIELD_CHARS or any(len(name) > MAX_FIELD_CHARS for name in columns):
        raise ValueError("the finite cascade CSV field allowance differs")
    rows = []
    for row in relation.records:
        if row.source_position > 2**63 - 1:
            raise ValueError("native cascade CSV positions must fit int64")
        row.row_id.encode("utf-8")
        if any(name not in row.values or type(row.values[name]) is not str for name in columns):
            raise ValueError("both cascade roles require present text source values")
        if len(row.row_id) > MAX_FIELD_CHARS or any(len(row.values[name]) > MAX_FIELD_CHARS for name in columns):
            raise ValueError("the finite cascade CSV field allowance is exceeded; input is not truncated")
        rows.append(dict(row_id=row.row_id, source_position=row.source_position,
                         **{name: row.values[name] for name in columns}))
    return pd.DataFrame(rows, columns=["row_id", "source_position", *columns]), columns


def _models(methods, generations, engines):
    from tokenizers import Tokenizer

    result = {}
    for role, method in methods.items():
        lm = make_lm(method, replace(generations[role], logprobs=None))
        spec = engines[role]
        lm.tokenizer = Tokenizer.from_file(str(Path(spec.tokenizer_path or spec.model_path) / "tokenizer.json"))
        for label in ("True", "False"):
            tokens = lm.tokenizer.encode(label, add_special_tokens=False).ids
            if len(tokens) != 1 or lm.encode_text(label) != tokens:
                raise ValueError("original LM and deployed tokenizer label IDs differ")
        if role == "helper":
            lm.kwargs["top_logprobs"] = generations[role].logprobs
        result[role] = lm
    return result


def _method_preflight(config, relation, method, generations, engines, args):
    import lotus
    from src.semantic_methods.lotus.cascade import validate_cascade, validate_cascade_source

    methods = _methods(method, config["helper_model_name"], args)
    frame, columns = _frame(relation, methods)
    models = _models(methods, generations, engines)
    with lotus.settings.context(enable_cache=False):
        checked, upper, effective = validate_cascade(frame, models["main"], models["helper"], args,
            instruction=method.instruction, max_rows=4096, max_calls=config["max_calls"])
    sample = int(len(frame) * checked.sampling_percentage) if checked.filter_pos_cascade_threshold is None else 0
    limits = dict(helper=len(frame), main=max(len(frame), sample))
    if any(engines[role].max_model_calls < limit for role, limit in limits.items()):
        raise ValueError("engine lifetime cap cannot cover every possible logical stage")
    return dict(max_possible_calls=upper, max_stage_calls=limits, input_columns=list(columns),
        effective_generation=effective, cascade_args=checked.model_dump(mode="json"),
        method_source=validate_cascade_source())


def preflight(config, relation, method, generations, resources, engines, args, owner):
    checked = _method_preflight(config, relation, method, generations, engines, args)
    if checked["max_possible_calls"] > owner["budget"]["limit"]:
        raise ValueError("the global cascade exceeds its permanent budget allowance")
    environment = preflight_owner(owner, engines["main"], chain=False, cascade=True)
    helper_environment = engines["helper"].validate_driver_environment()
    return dict(checked, status="preflight_only", inference_calls=0, backend=BACKEND,
        query_id=config["query_id"], source_occurrences=len(relation.records),
        permanent_unit_requests_minimum=max(1, checked["max_possible_calls"]),
        owner_environment=environment, helper_engine_environment=helper_environment,
        permanent_unit_claimed=False, external_host_lease_released_by_worker=False,
        scheduler_owner="author DBEnd/Coordinator/Lane")


def _make_backend(source, **kwargs):
    from integrations.imlane_records.cascade_host import IMLaneCascadeBackend
    return IMLaneCascadeBackend(source, **kwargs)


def run_query(config, relation, method, generations, resources, engines, args,
              author_args, model_environment, *, cancelled=None):
    import lotus
    from src.baselines.text.cascade_records_source import MaterializedCascadeSource
    from src.semantic_methods.lotus.cascade import run_cascade
    from src.semantic_methods.lotus.record_chain import records_to_frame

    report = dict(status="failed", query_id=config["query_id"], calls=None)
    backend = primary = None
    started = time.monotonic()
    try:
        with lotus.settings.context(enable_cache=False):
            methods = _methods(method, config["helper_model_name"], args)
            frame, columns = _frame(relation, methods)
            models = _models(methods, generations, engines)
            source = MaterializedCascadeSource(author_args.output, relation.records,
                                               method=method, payload_columns=columns)
            prepared_at = time.monotonic()
            backend = _make_backend(source, engines=engines, resources=resources,
                author_args=author_args, model_environment=model_environment, cancelled=cancelled)
            report = run_cascade(frame, models["main"], models["helper"], args, backend,
                instruction=method.instruction, default=method.default, system_prompt=method.system_prompt,
                query_id=config["query_id"], max_rows=4096, max_calls=config["max_calls"])
            expected = {(row.row_id, row.source_position) for row in relation.records}
            decisions = report["decisions"]
            by_key = {(row["row_id"], row["source_position"]): row["decision"] for row in decisions}
            if (len(by_key) != len(decisions) or set(by_key) != expected
                    or any(type(value) is not bool for value in by_key.values())):
                raise ValueError("cascade decisions do not cover the original occurrences")
            kept = [row for row in relation.records if by_key[(row.row_id, row.source_position)]]
            final = records_to_frame(kept, columns=relation.columns, source_index=relation.source_index,
                                     empty_prototype=relation.empty_prototype)
            report.update(result=_frame_result(final), output_records=[row.describe() for row in kept],
                source_materialization_seconds=prepared_at-started,
                source=source.preflight(method))
    except BaseException as error:
        primary = error
        try:
            attached = getattr(error, "query_report", None)
            if isinstance(attached, dict):
                report = attached
        except BaseException as metadata:
            error.add_note("cascade error accounting also failed: " + type(metadata).__name__)
    finally:
        def failed(error, label):
            nonlocal primary
            report["status"] = "failed"
            if primary is None:
                primary = error
            elif primary is not error:
                primary.add_note(label + ": " + type(error).__name__)
        if backend is not None:
            try:
                closed = backend.close()
                report["owner_close"] = deepcopy(closed)
                if type(closed) is not list or any(type(row) is not dict or row.get("status") != "closed"
                        or row.get("stage_process_exit_confirmed") is not True for row in closed):
                    raise RuntimeError("cascade author or model stage close remains unconfirmed")
            except BaseException as error:
                failed(error, "cascade owner close also failed")
            for attribute, field in (("close_reports", "backend_close"), ("author_report", "author_runtime")):
                try:
                    report[field] = deepcopy(getattr(backend, attribute))
                except BaseException as error:
                    failed(error, "cascade metadata " + field + " also failed")
        report.update(public_profile="semloom.records_cascade_run.v1", backend=BACKEND,
            scheduler_owner="author DBEnd/Coordinator/Lane", production_core_used=False,
            performance_claim=False, elapsed_seconds=time.monotonic()-started)
        if primary is not None:
            report.update(status="failed", error_type=type(primary).__name__, error=redact_text(str(primary)))
            primary.query_report = report
    if primary is not None:
        raise primary
    return report


def run_worker(config, relation, method, generations, resources, engines, args, root, owner):
    from .imlane_owned_query import run_owned_query

    # This pure method gate also provides the original global worst-case bound.
    checked = _method_preflight(config, relation, method, generations, engines, args)
    return run_owned_query(root, owner, query_id=config["query_id"], backend=BACKEND,
        required_requests=max(1, checked["max_possible_calls"]),
        preflight=lambda: preflight(config, relation, method, generations, resources, engines, args, owner),
        execute=lambda author, guard: run_query(config, relation, method, generations, resources,
            engines, args, author, owner["model_environment"], cancelled=guard),
        guard_factory=AuthorRunGuard, write_report=write_private_json)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    for name in ("record-cascade-check", "record-cascade-run"):
        command = commands.add_parser(name)
        for field in ("config", "source-json", "owner-config"):
            command.add_argument("--"+field, required=True)
        if name.endswith("-run"):
            command.add_argument("--output-dir", required=True)
            command.add_argument("--execute-model", action="store_true")
            command.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    command = parser.parse_args(argv)
    config, method, generations, resources, engines, args = read_config(command.config)
    relation = read_relation(command.source_json, expected_sha256=config["source_sha256"])
    owner = read_owner(command.owner_config, expected_sha256=config["owner_sha256"])
    checked = preflight(config, relation, method, generations, resources, engines, args, owner)
    if command.action.endswith("-check") or not command.execute_model:
        print(json.dumps(checked, ensure_ascii=False))
        return 0
    root = Path(command.output_dir).resolve()
    require_outside_git(root)
    if command.worker:
        run_worker(config, relation, method, generations, resources, engines, args, root, owner)
    else:
        from .lotus_records_supervisor import supervise
        new_private_directory(root)
        write_private_json(root/"preflight.json", checked)
        worker = [sys.executable, str(Path(__file__).parents[2]/"scripts/experiments/run_lotus_records_movie.py"),
            "record-cascade-run", "--config", str(Path(command.config).resolve()),
            "--source-json", str(Path(command.source_json).resolve()),
            "--owner-config", str(Path(command.owner_config).resolve()), "--output-dir", str(root),
            "--execute-model", "--worker"]
        supervise(worker, root, timeout_s=resources.startup_timeout_s+resources.query_timeout_s+30)
    return 0
