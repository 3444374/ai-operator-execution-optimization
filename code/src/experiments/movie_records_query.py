"""Shared Movie query evaluation, call receipts and fixed-response comparison."""
from dataclasses import asdict
import json
import time

from src.infrastructure.private_artifacts import json_metric
from src.infrastructure.redact import redact_text
from src.baselines.text.movie_records import RECORD_COLUMNS, validate_movie_method
from src.baselines.text.sembench_movie import (
    classification_audit, evaluate_original, run_original_lotus_query, verify_checkout,
)
from src.semantic_methods.lotus.records import CallLedger



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



def run_movie_query(source, checkout, method, generation, execute, *, query_id, observer=None):
    """Execute once; only compact final engine results may collect at the driver.

    execute is a lazy backend factory. Preflight and LM policy validation happen
    before calling it. A failure carries query_report, including every known call.
    """
    import lotus
    from src.semantic_methods.lotus.engine import (
        interpret_results, lm_report, make_lm, validate_filter_source, validate_lm,
    )
    emit = observer if observer is not None else lambda _event: None
    emit("query_source_start")
    task = source.task
    validate_movie_method(method, task.name)
    inputs = source.preflight(method)
    upstream = verify_checkout(checkout)
    method_source = validate_filter_source()
    expected = source.expected(query_id)
    emit("query_source_ready")
    ledger = CallLedger(expected, max_calls=max(1, len(expected)))
    lm = make_lm(method, generation)
    report = dict(schema="semloom.movie_records_query.v1", query_id=query_id,
                  status="failed", source=inputs, task=task.describe(), method=asdict(method),
                  generation=asdict(generation), method_source=method_source,
                  production_core_used=False, model_calls_are_not_output_rows=True,
                  query_timing_scope="backend construction, scan, prepare, model, native interpretation and task projection/aggregation",
                  evaluation_in_query_seconds=False, upstream_accessor_used_in_records=False)
    iterator = None
    primary = None
    started = time.monotonic()
    try:
        with lotus.settings.context(lm=lm, enable_cache=False):
            validate_lm(lm, method, generation)
            iterator = iter(execute())
            report["production_core_used"] = getattr(iterator, "production_core_connected", False) is True
            if report["production_core_used"]:
                profile = getattr(iterator, "execution_profile", None)
                if profile is not None:
                    report["core_execution"] = profile
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
            result = task.result_frame(source.rows(), decisions)
            report.update(result=task.encode_result(result), decisions=decisions,
                          query_seconds=time.monotonic() - started)
            emit("result_ready")
            evaluated_at = time.monotonic()
            emit("evaluation_start")
            labeled = source.labeled_frame()
            quality = evaluate_original(upstream, task.query, result, labeled)
            rows = source.rows()
            labels = dict(zip((row["row_id"] for row in rows), labeled["scoreSentiment"], strict=True))
            quality["classification_audit"] = classification_audit(
                {row["row_id"]: labels[row["row_id"]] for row in expected},
                [(row["row_id"], "POSITIVE" if row["decision"] else "NEGATIVE") for row in decisions],
            )
            report.update(quality=json_metric(quality),
                          evaluation_seconds=time.monotonic() - evaluated_at,
                          status="completed")
            emit("evaluation_end")
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
            receipts = ([] if report["backend_close"] is None else
                        report["backend_close"].get("unconsumed_call_results", []))
            # This snapshot was just made from the original validated ledger.
            # With no new terminal, replaying every record adds no accounting.
            if (primary is None and report["status"] == "completed"
                    and report["calls"]["counts"]["succeeded"] == len(expected)
                    and type(receipts) is list and not receipts):
                report["runner_unconfirmed_call_ids"] = []
            else:
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
        try:
            emit("query_cleanup_end")
        except BaseException as observation:
            report.setdefault("observer_errors", []).append(redact_text(str(observation)))
            if primary is None:
                primary = observation
            else:
                primary.add_note("cleanup observation also failed: " + type(observation).__name__)
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
    from src.semantic_methods.lotus.engine import observe_filter_rows
    from src.semantic_methods.lotus.engine import (
        LotusFilterMethod, bind_filter_method, engine_response, lm_report, make_lm, validate_filter_source,
    )

    task = source.task
    validate_movie_method(method, task.name)
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
    preparer = LotusFilterMethod(bind_filter_method(method))
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
                result = run_original_lotus_query(checkout, task.query, lambda: frame)
        native_messages = [message for call in captured for message in call["messages"]]
        if (len(captured) != 1
                or native_messages != [json.loads(row["messages_json"]) for row in prepared]):
            raise ValueError("records preparation differs from original LOTUS messages")
        observed = [(row["row_id"], row["decision"]) for row in report["decisions"]]
        if native_decisions != observed or task.encode_result(result) != report["result"]:
            raise ValueError("records Filter decisions or task projection differ from original LOTUS")
        if lm_report(reference)["stats"] != report["lm"]["stats"]:
            raise ValueError("records physical/virtual usage differs from original LM accounting")
        report.update(execution_kind="fixed-response software qualification", inference_calls=0,
                      native_reference=dict(messages_equal=True, decisions_equal=True, result_equal=True,
                                            count_equal=True if task.query == 3 else None,
                                            lm_stats_equal=True, effective_kwargs=captured[0]["kwargs"]))
    except BaseException as error:
        report.update(status="failed", error_type=type(error).__name__, error=redact_text(str(error)),
                      inference_calls=0, execution_kind="fixed-response software qualification")
        error.query_report = report
        raise
    return report
