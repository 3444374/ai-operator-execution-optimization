"""Movie Q3 helper-cascade callable for integration into the owner-managed CLI."""

import time

from src.baselines.common.private_artifacts import json_metric
from src.baselines.common.redact import redact_text
from src.baselines.text.sembench_movie import classification_audit, evaluate_original, verify_checkout
from src.semantic_methods.lotus.cascade import run_cascade
from src.semantic_methods.lotus.records import FilterMethod, MOVIE_Q3_INSTRUCTION


def run_movie_cascade_query(source, checkout, main_lm, helper_lm, cascade_args, evaluate, *,
                            query_id, max_calls):
    """Preserve Movie predicate, COUNT and original evaluator; no physical executor here."""
    import pandas as pd
    inputs = source.preflight(FilterMethod(main_lm.model))
    upstream = verify_checkout(checkout)
    frame = pd.DataFrame([
        dict(row_id=row["row_id"], source_position=row["source_position"], reviewText=row["reviewText"])
        for row in source.rows() if row["id"] == "taken_3"
    ], columns=["row_id", "source_position", "reviewText"])
    started = time.monotonic()
    report = {}
    try:
        report = run_cascade(frame, main_lm, helper_lm, cascade_args, evaluate,
                             instruction=MOVIE_Q3_INSTRUCTION, query_id=query_id,
                             max_rows=max(1, inputs["rows"]), max_calls=max_calls)
        report["positive_review_cnt"] = sum(row["decision"] for row in report["decisions"])
        result = pd.DataFrame({"positive_review_cnt": [report["positive_review_cnt"]]})
        report.update(query_seconds=time.monotonic() - started, source=inputs,
            query_timing_scope="method preflight, backend stages, original control, parsing and COUNT",
            evaluation_in_query_seconds=False,
            source_preflight_and_initial_occurrence_read_in_query_seconds=False,
            backend_close=getattr(evaluate, "close_reports", None),
            production_core_used=getattr(evaluate, "production_core_connected", False) is True)
        evaluated_at = time.monotonic()
        labels = source.labeled_frame()
        quality = evaluate_original(upstream, 3, result, labels)
        source_rows = source.rows()
        references = dict(zip((row["row_id"] for row in source_rows), labels["scoreSentiment"], strict=True))
        quality["classification_audit"] = classification_audit(
            {row["row_id"]: references[row["row_id"]] for row in report["decisions"]},
            [(row["row_id"], "POSITIVE" if row["decision"] else "NEGATIVE") for row in report["decisions"]],
        )
        report.update(quality=json_metric(quality), evaluation_seconds=time.monotonic() - evaluated_at)
    except BaseException as error:
        report = getattr(error, "query_report", report)
        report.update(status="failed", query_id=query_id, source=inputs,
                      error_type=type(error).__name__, error=redact_text(str(error)),
                      elapsed_seconds=time.monotonic() - started,
                      backend_close=getattr(evaluate, "close_reports", None),
                      production_core_used=getattr(evaluate, "production_core_connected", False) is True)
        error.query_report = report
        raise
    report["elapsed_seconds"] = time.monotonic() - started
    return report
