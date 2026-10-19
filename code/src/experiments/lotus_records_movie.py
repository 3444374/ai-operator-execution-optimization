"""Finite ordinary Movie Q3 records execution and original SemBench evaluation."""

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys
import time

from src.baselines.common.private_artifacts import json_metric, new_private_directory, write_private_json
from src.baselines.common.redact import redact_text
from src.baselines.text.movie_records import (
    MovieRecordSource, RECORD_COLUMNS, prepare_movie_source, validate_movie_method,
)
from src.baselines.text.sembench_movie import (
    classification_audit, evaluate_original, run_original_lotus_query, verify_checkout,
)
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordsResources


BACKENDS = ("ray-data-local-model", "semloom-local-model-static-reference", "daft-native-local-model",
            "semloom-local-model-core")


def _fixed_method(method):
    validate_movie_method(method)


def _finish(iterator, primary):
    try:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()
    except BaseException as error:
        if primary is None:
            raise
        primary.add_note("records result close also failed: " + type(error).__name__)


def merge_unconsumed_call_results(report, expected, close_report, *, primary=None):
    """Merge terminal close receipts into accounting, without interpreting them.

    Only IDs marked here as runner-created missing-call placeholders may later
    receive a real terminal result. A backend's own unknown remains authoritative.
    Bad receipts do not discard valid receipts on either side of them.
    """
    previous = CallLedger(expected, max_calls=max(1, len(expected)))
    for row in report["calls"]["records"]:
        previous.record(row)
    rows = previous.report()["records"]
    declared = {row["call_id"]: row for row in expected}
    placeholder_ids = set(report.get("runner_unconfirmed_call_ids", []))
    placeholders = {row["call_id"]: row for row in rows
                    if row["call_id"] in placeholder_ids and row["status"] == "unknown"
                    and row["submission_confirmed"] is False
                    and row["prompt_tokens"] is None and row["completion_tokens"] is None}
    ledger = CallLedger(expected, max_calls=max(1, len(expected)))
    known = {}
    for row in rows:
        if row["call_id"] not in placeholders:
            ledger.record(row)
            known[row["call_id"]] = row
    errors = []
    receipts = [] if close_report is None else close_report.get("unconsumed_call_results", [])
    if type(receipts) is not list:
        errors.append(ValueError("unconsumed call results must be a list"))
        receipts = []
    for receipt in receipts:
        try:
            call_id = receipt.get("call_id") if type(receipt) is dict else None
            if type(call_id) is not str or call_id not in declared:
                raise ValueError("close receipt belongs to an undeclared call identity")
            checked = CallLedger([declared[call_id]], max_calls=1)
            checked.record(receipt)
            row = checked.report()["records"][0]
            if call_id in known:
                if row != known[call_id]:
                    raise ValueError("close receipt conflicts with an existing call result")
                continue
            ledger.record(row)
            known[call_id] = row
            placeholders.pop(call_id, None)
        except BaseException as error:
            errors.append(error)
    for row in placeholders.values():
        ledger.record(row)
    missing = ledger.report()["missing_call_ids"]
    failure = primary if primary is not None else (errors[0] if errors else None)
    if failure is not None:
        ledger.mark_unconfirmed(type(failure).__name__)
        placeholder_ids = set(placeholders) | set(missing)
        report["status"] = "failed"
    else:
        placeholder_ids = set(placeholders)
    report["calls"] = ledger.report()
    report["runner_unconfirmed_call_ids"] = [row["call_id"] for row in expected
                                             if row["call_id"] in placeholder_ids]
    for error in errors:
        detail = dict(error_type=type(error).__name__, error=redact_text(str(error)))
        recorded = report.setdefault("close_receipt_errors", [])
        if detail not in recorded:
            recorded.append(detail)
        if failure is not None:
            note = "records close receipt rejected: " + detail["error"]
            if note not in getattr(failure, "__notes__", []):
                failure.add_note(note)
    if errors and primary is None:
        raise errors[0]


def run_movie_query(source, checkout, method, generation, execute, *, query_id):
    """Execute once; only compact final engine results may collect at the driver.

    execute is a lazy backend factory. Preflight and LM policy validation happen
    before calling it. A failure carries query_report, including every known call.
    """
    import lotus
    import pandas as pd
    from src.semantic_methods.lotus.engine import (
        interpret_results, lm_report, make_lm, validate_filter_source, validate_lm,
    )
    _fixed_method(method)
    inputs = source.preflight(method)
    upstream = verify_checkout(checkout)
    method_source = validate_filter_source()
    expected = source.expected(query_id)
    ledger = CallLedger(expected, max_calls=max(1, len(expected)))
    lm = make_lm(method, generation)
    report = dict(schema="semloom.movie_records_query.v1", query_id=query_id,
                  status="failed", source=inputs, method=asdict(method),
                  generation=asdict(generation), method_source=method_source,
                  production_core_used=False, model_calls_are_not_output_rows=True,
                  query_timing_scope="backend construction, scan, prepare, model, native interpretation and COUNT",
                  evaluation_in_query_seconds=False, upstream_accessor_used_in_records=False)
    iterator = None
    primary = None
    started = time.monotonic()
    try:
        with lotus.settings.context(lm=lm, enable_cache=False):
            validate_lm(lm, method, generation)
            iterator = iter(execute())
            report["production_core_used"] = getattr(iterator, "production_core_connected", False) is True
            raw = []
            for value in iterator:
                ledger.record(value)
                raw.append(value)
            missing = ledger.report()["counts"]["missing"]
            if missing:
                raise ValueError("records execution omitted declared calls")
            by_call = {row["call_id"]: row for row in raw}
            ordered = [by_call[row["call_id"]] for row in expected]
            decisions = interpret_results(ordered, lm, method)
            count = sum(row["decision"] for row in decisions)
            result = pd.DataFrame({"positive_review_cnt": [count]})
            report.update(result={"positive_review_cnt": count}, decisions=decisions,
                          query_seconds=time.monotonic() - started)
            evaluated_at = time.monotonic()
            labeled = source.labeled_frame()
            quality = evaluate_original(upstream, 3, result, labeled)
            rows = source.rows()
            labels = dict(zip((row["row_id"] for row in rows), labeled["scoreSentiment"], strict=True))
            quality["classification_audit"] = classification_audit(
                {row["row_id"]: labels[row["row_id"]] for row in expected},
                [(row["row_id"], "POSITIVE" if row["decision"] else "NEGATIVE") for row in decisions],
            )
            report.update(quality=json_metric(quality),
                          evaluation_seconds=time.monotonic() - evaluated_at,
                          status="completed")
    except BaseException as error:
        primary = error
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
    finally:
        try:
            if iterator is not None:
                _finish(iterator, primary)
        except BaseException as error:
            primary = error
            report.update(status="failed", error_type=type(error).__name__,
                          error=redact_text(str(error)))
        report["calls"] = ledger.report()
        try:
            report["backend_close"] = getattr(iterator, "last_close_report", None)
            merge_unconsumed_call_results(report, expected, report["backend_close"], primary=primary)
        except BaseException as error:
            if primary is None:
                primary = error
                report.update(error_type=type(error).__name__, error=redact_text(str(error)))
            else:
                primary.add_note("records close accounting also failed: " + type(error).__name__)
            merge_unconsumed_call_results(report, expected, None, primary=primary)
        report["lm"] = lm_report(lm)
        report["elapsed_seconds"] = time.monotonic() - started
        if primary is not None:
            report["status"] = "failed"
            primary.query_report = report
    if primary is not None:
        raise primary
    return report


def fixed_response_qualification(source, checkout, method, generation, responses, *, query_id):
    """Compare a small software fixture with unchanged native LOTUS Q3, without I/O calls."""
    import lotus
    import pandas as pd
    from src.baselines.text.frameworks.lotus_pg import observe_filter_rows
    from src.semantic_methods.lotus.engine import (
        LotusFilterMethod, engine_response, lm_report, make_lm, validate_filter_source,
    )

    _fixed_method(method)
    source.preflight(method)
    validate_filter_source()
    expected = source.expected(query_id)
    if len(responses) != len(expected):
        raise ValueError("fixed responses must cover every eligible occurrence")
    by_row = {}
    for row in responses:
        if row["row_id"] in by_row:
            raise ValueError("duplicate fixed-response occurrence")
        by_row[row["row_id"]] = row
    if set(by_row) != {row["row_id"] for row in expected}:
        raise ValueError("fixed responses have foreign or missing occurrences")
    raw = []
    for identity in expected:
        value = dict(by_row[identity["row_id"]])
        raw.append(dict(value, **identity, status="succeeded", submission_confirmed=True,
                        engine_uid="software-fixture", engine_request_id=identity["call_id"],
                        finish_reason="stop", logprobs_json="null",
                        total_tokens=value["prompt_tokens"] + value["completion_tokens"],
                        usage_source="configured software fixture; no inference"))
    source_rows = {row["row_id"]: row for row in source.rows()}
    preparer = LotusFilterMethod(method)
    prepared = preparer.prepare_rows([dict(source_rows[row["row_id"]], **row) for row in expected])
    report = run_movie_query(source, checkout, method, generation, lambda: iter(raw), query_id=query_id)

    try:
        reference = make_lm(method, generation)
        captured, native_decisions, native_prompts = [], [], []
        def fixed(uncached, kwargs, *_):
            captured.append(dict(messages=[message for message, _ in uncached], kwargs=kwargs))
            if len(uncached) != len(raw):
                raise ValueError("native Q3 requested an unexpected fixture call shape")
            return [engine_response(row, reference) for row in raw]
        reference._process_uncached_messages = fixed
        frame = pd.DataFrame(source.rows(), columns=RECORD_COLUMNS).rename(
            columns={"row_id": "_execution_row_id"})
        with lotus.settings.context(lm=reference, enable_cache=False):
            with observe_filter_rows(native_decisions.extend, native_prompts.extend):
                result = run_original_lotus_query(checkout, 3, lambda: frame)
        native_messages = [message for call in captured for message in call["messages"]]
        if (len(captured) != 1
                or native_messages != [json.loads(row["messages_json"]) for row in prepared]):
            raise ValueError("records preparation differs from original LOTUS messages")
        observed = [(row["row_id"], row["decision"]) for row in report["decisions"]]
        if native_decisions != observed or int(result.iloc[0, 0]) != report["result"]["positive_review_cnt"]:
            raise ValueError("records Filter decisions or COUNT differ from original LOTUS")
        if lm_report(reference)["stats"] != report["lm"]["stats"]:
            raise ValueError("records physical/virtual usage differs from original LM accounting")
        report.update(execution_kind="fixed-response software qualification", inference_calls=0,
                      native_reference=dict(messages_equal=True, decisions_equal=True, count_equal=True,
                                            lm_stats_equal=True, effective_kwargs=captured[0]["kwargs"]))
    except BaseException as error:
        report.update(status="failed", error_type=type(error).__name__, error=redact_text(str(error)),
                      inference_calls=0, execution_kind="fixed-response software qualification")
        error.query_report = report
        raise
    return report


def read_config(path):
    from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec

    value = json.loads(Path(path).read_text())
    allowed = {"schema", "backend", "query_id", "method", "generation", "engine", "resources"}
    if (type(value) is not dict or set(value) - allowed
            or value.get("schema") != "semloom.movie_records_run.v1"
            or value.get("backend") not in BACKENDS):
        raise ValueError("unsupported ordinary Movie records configuration")
    method = FilterMethod.from_dict(value["method"])
    _fixed_method(method)
    generation = GenerationSpec.from_dict(value.get("generation", {}))
    resources = RecordsResources(**value.get("resources", {}))
    if value["backend"] == "semloom-local-model-core" and (
            resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1):
        raise ValueError("Core records currently requires prepare_batch_rows=model_batch_rows=1")
    engine = EngineSpec.from_dict(value["engine"])
    query_id = value["query_id"]
    # Validate query identity even for empty source relations.
    from src.semantic_methods.lotus.records import identity
    identity(query_id, "preflight", 0)
    return value, method, generation, resources, engine


def model_preflight(source, checkout, method, generation, engine):
    import lotus
    from src.semantic_methods.lotus.engine import make_lm, validate_filter_source, validate_lm

    _fixed_method(method)
    inputs = source.preflight(method)
    verify_checkout(checkout)
    method_source = validate_filter_source()
    lm = make_lm(method, generation)
    with lotus.settings.context(lm=lm, enable_cache=False):
        validate_lm(lm, method, generation)
    environment = engine.validate_driver_environment()
    return dict(status="preflight_only", inference_calls=0, source=inputs,
                method_source=method_source, engine_environment=environment)


def _model_worker(config, source, checkout, method, generation, resources, engine, root):
    ray = None
    owner = None
    failure = None
    report = {}
    runtime_started = time.monotonic()
    try:
        if config["backend"] != "daft-native-local-model":
            import ray

            ray.init(address="local", num_cpus=resources.cpu_workers + resources.gpu_workers + 1,
                     num_gpus=1, include_dashboard=False,
                     runtime_env={"env_vars": {"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                                                 "LITELLM_LOCAL_MODEL_COST_MAP": "True",
                                                 "VLLM_WORKER_MULTIPROC_METHOD": "spawn"}})
        runtime_seconds = time.monotonic() - runtime_started
        def execute():
            nonlocal owner
            if config["backend"] == "daft-native-local-model":
                from src.baselines.text.frameworks.daft_lotus_records import evaluate
                return evaluate(source, method, engine, generation, resources, query_id=config["query_id"])
            if config["backend"] == "ray-data-local-model":
                from src.baselines.text.frameworks.ray_data_lotus_records import evaluate
                return evaluate(source, method, engine, generation, resources, query_id=config["query_id"])
            if config["backend"] == "semloom-local-model-core":
                from src.execution_provider.adapters.lotus_records_core import SemLoomRecordsCore
                owner = SemLoomRecordsCore(method, engine, generation, resources)
                return owner.evaluate(source, query_id=config["query_id"])
            from src.execution_provider.adapters.lotus_records_static import SemLoomRecordsStaticReference
            owner = SemLoomRecordsStaticReference(method, engine, generation, resources)
            return owner.evaluate(source, query_id=config["query_id"])
        report = run_movie_query(source, checkout, method, generation, execute, query_id=config["query_id"])
        report.update(backend=config["backend"], runtime_setup_seconds=runtime_seconds,
                      execution_kind="synchronous local-model qualification", performance_claim=False)
    except BaseException as error:
        failure = error
        report = getattr(error, "query_report", dict(status="failed", error_type=type(error).__name__,
                                                     error=redact_text(str(error))))
    finally:
        try:
            if owner is not None:
                report["owner_close"] = owner.close()
                if report["owner_close"].get("cleanup_errors"):
                    raise RuntimeError("static owner cleanup remains unconfirmed")
        except BaseException as cleanup:
            report.update(status="failed", close_error_type=type(cleanup).__name__)
            if failure is None:
                failure = cleanup
            else:
                failure.add_note("static owner close also failed: " + type(cleanup).__name__)
        finally:
            report.setdefault("production_core_used", False)
            try:
                report["production_core_used"] = report["production_core_used"] is True or (
                    getattr(owner, "production_core_connected", False) is True)
                if (owner is not None and type(report.get("calls")) is dict
                        and report["calls"].get("schema") == "semloom.lotus_call_ledger.v1"):
                    late_close = getattr(owner, "last_query_report", None)
                    if late_close is None:
                        late_close = report.get("owner_close", {}).get("last_query_report")
                    if late_close is not None:
                        report["backend_close"] = late_close
                        merge_unconsumed_call_results(report, source.expected(config["query_id"]),
                                                      late_close, primary=failure)
            except BaseException as cleanup:
                report.update(status="failed", close_receipt_error_type=type(cleanup).__name__)
                if failure is None:
                    failure = cleanup
                    report.update(error_type=type(cleanup).__name__, error=redact_text(str(cleanup)))
                else:
                    failure.add_note("owner close accounting also failed: " + type(cleanup).__name__)
        try:
            if ray is not None and ray.is_initialized():
                ray.shutdown()
        except BaseException as cleanup:
            report.update(status="failed", ray_close_error_type=type(cleanup).__name__)
            if failure is None:
                failure = cleanup
            else:
                failure.add_note("Ray close also failed: " + type(cleanup).__name__)
        report["independent_gpu_process_exit_check"] = False
        try:
            write_private_json(root / "query.json", report)
        except BaseException as recording:
            if failure is None:
                failure = recording
            else:
                failure.add_note("query report also failed: " + type(recording).__name__)
    if failure is not None:
        failure.query_report = report
        raise failure


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--input-csv", required=True)
    prepare.add_argument("--input-sha256", required=True)
    prepare.add_argument("--max-rows", type=int, required=True)
    prepare.add_argument("--role", choices=("software_fixture", "sembench_movie_subset"), required=True)
    prepare.add_argument("--output-dir", required=True)
    for name in ("check", "run", "fixture"):
        command = commands.add_parser(name)
        command.add_argument("--source-manifest", required=True)
        command.add_argument("--sembench-checkout", required=True)
        command.add_argument("--config", required=name != "fixture")
        if name != "check":
            command.add_argument("--output-dir", required=True)
        if name == "run":
            command.add_argument("--execute-model", action="store_true")
            command.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
        if name == "fixture":
            command.add_argument("--responses", required=True)
            command.add_argument("--model-name", default="openai/local-fixture")
    args = parser.parse_args(argv)
    if args.action == "prepare":
        print(prepare_movie_source(args.input_csv, args.output_dir,
                                   expected_csv_sha256=args.input_sha256,
                                   max_rows=args.max_rows, role=args.role))
        return 0
    source = MovieRecordSource.from_manifest(args.source_manifest)
    if args.action == "fixture":
        from src.serving.backends.vllm_engine import GenerationSpec
        method, generation = FilterMethod(args.model_name), GenerationSpec()
        root = Path(args.output_dir).resolve()
        new_private_directory(root)
        try:
            report = fixed_response_qualification(source, args.sembench_checkout, method, generation,
                                                 json.loads(Path(args.responses).read_text()), query_id="movie-q3-fixture")
        except BaseException as error:
            report = getattr(error, "query_report", dict(status="failed", error_type=type(error).__name__,
                                                         error=redact_text(str(error)), inference_calls=0))
            try:
                write_private_json(root / "query.json", report)
            except BaseException as recording:
                error.add_note("fixture report also failed: " + type(recording).__name__)
            raise
        write_private_json(root / "query.json", report)
        print(json.dumps(dict(status=report["status"], inference_calls=0, result=report["result"])))
        return 0
    config, method, generation, resources, engine = read_config(args.config)
    preflight = model_preflight(source, args.sembench_checkout, method, generation, engine)
    if args.action == "check" or not args.execute_model:
        print(json.dumps(preflight, ensure_ascii=False))
        return 0
    root = Path(args.output_dir).resolve()
    if args.worker:
        _model_worker(config, source, args.sembench_checkout, method, generation, resources, engine, root)
    else:
        from .lotus_records_supervisor import supervise
        new_private_directory(root)
        write_private_json(root / "preflight.json", preflight)
        command = [sys.executable, str(Path(__file__).parents[2] / "scripts/experiments/run_lotus_records_movie.py"),
                   "run", "--source-manifest", str(source.manifest_path),
                   "--sembench-checkout", str(Path(args.sembench_checkout).resolve()),
                   "--config", str(Path(args.config).resolve()), "--output-dir", str(root),
                   "--execute-model", "--worker"]
        supervise(command, root, timeout_s=resources.startup_timeout_s + resources.query_timeout_s + 30)
    return 0
