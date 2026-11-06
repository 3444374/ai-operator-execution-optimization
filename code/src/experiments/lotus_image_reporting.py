"""Persist retrieval evidence before releasing its exported observation references."""

import json
import time

from src.infrastructure.redact import redact_text
from src.semantic_methods.lotus.image_rm import RMObservation


def retain_failure(report, error, failure, scope, *, cleanup=False):
    from src.observability.execution_timing import is_timing_error
    if error is None or (cleanup and is_timing_error(error) and not is_timing_error(failure)):
        if error is not None:
            failure.add_note("Prior image timing failure: " + type(error).__name__)
        report.update(status="failed", error_type=type(failure).__name__, reason=redact_text(str(failure)))
        return failure
    error.add_note(scope + " also raised " + type(failure).__name__)
    return error


def write_retrieval_evidence(output_dir, report, rm, execution_records, error):
    """Keep failed writes and unexported observations with their current owners."""
    sink = time.monotonic_ns()
    if rm is not None:
        report["rm_calls"] = list(rm.observation.calls)
    if execution_records is not None:
        report["execution_observations"] = list(execution_records)
    if rm is not None and rm.observation.vectors:
        import numpy as np
        try:
            vectors = dict(rm.observation.vectors)
            np.savez_compressed(output_dir / "rm-vectors.npz", **vectors)
            if isinstance(rm.observation, RMObservation):
                rm.observation.release_exported(vectors=vectors)
        except BaseException as exc:
            report.setdefault("evidence_write_errors", []).append("vectors: " + type(exc).__name__)
            error = retain_failure(report, error, exc, "vector evidence write")
    report_path = output_dir / "result.json"
    report_written = False
    try:
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        report_written = True
    except BaseException as exc:
        report.setdefault("evidence_write_errors", []).append("result: " + type(exc).__name__)
        error = retain_failure(report, error, exc, "result evidence write")
    try:
        (output_dir / "evidence-write.json").write_text(json.dumps({
            "elapsed_s": (time.monotonic_ns() - sink) / 1e9,
            "errors": report.get("evidence_write_errors", []),
        }) + "\n")
    except BaseException as exc:
        report.setdefault("evidence_write_errors", []).append("receipt: " + type(exc).__name__)
        error = retain_failure(report, error, exc, "receipt evidence write")
        report_written = False
        try:
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            report_written = True
        except BaseException as update_error:
            error.add_note("updated result evidence write also raised " + type(update_error).__name__)
    if report_written:
        if rm is not None and isinstance(rm.observation, RMObservation):
            rm.observation.release_exported(calls=report["rm_calls"])
        if isinstance(execution_records, list):
            exported = report["execution_observations"]
            if len(exported) <= len(execution_records) and all(
                    left is right for left, right in zip(exported, execution_records)):
                del execution_records[:len(exported)]
    return error
