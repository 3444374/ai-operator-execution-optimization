"""One unchanged LOTUS accessor over the existing native local model owner.

The caller binds the stage and invokes the original accessor. LOTUS retains
formatting, parsing, batching and ordered returns; this adapter owns only its
cold model lifetime or explicitly borrows a caller-owned resident model.
"""

from contextlib import contextmanager
from copy import deepcopy
import time

from src.semantic_methods.lotus.engine import lm_report
from src.semantic_methods.lotus.record_method import LotusRecordMethod
from src.semantic_methods.lotus.record_types import stage_identity
from src.semantic_methods.lotus.records import CallLedger, identity
from src.semantic_methods.lotus.filter_answers import LOTUS_NATIVE


@contextmanager
def _observe_messages(lm, records, capture):
    """Bind actual native messages once inside the owner's isolated SDK scope."""
    lm_type, called = type(lm), False
    original = lm_type.__call__

    def observed(current, messages, *args, **kwargs):
        nonlocal called
        if current is not lm:
            raise ValueError("native stage invoked another LM")
        if called:
            raise RuntimeError("native stage invoked its LM more than once")
        called = True
        if len(messages) != len(records):
            raise ValueError("native LM message count differs from its source occurrences")
        capture([dict(row_id=record.row_id, messages=message)
            for record, message in zip(records, messages, strict=True)])
        return original(current, messages, *args, **kwargs)

    lm_type.__call__ = observed
    try:
        yield
        if records and not called:
            raise RuntimeError("native accessor did not invoke its bound LM")
    finally:
        lm_type.__call__ = original


def finish_accounting(report, *, require_complete=True):
    """Materialize resident statistics after full sink, before owner close."""
    binding = report.pop('_native_binding')
    lm = report.pop('_native_lm')
    ready = report.pop('_native_ready')
    try:
        report.update(binding.report())
        report['lm'] = lm_report(lm)
        if ready is not None:
            report['owner_ready'] = deepcopy(ready)
        counts = report['calls']['counts']
        if require_complete and (counts['succeeded'] != len(report['stage_calls']) or counts['missing']):
            raise RuntimeError('native accessor did not return every declared model call')
        report['accounting_phase'] = 'after_full_sink' if require_complete else 'failure'
        if require_complete:
            report['status'] = 'completed'
    except BaseException as error:
        report['accounting_error_type'] = type(error).__name__
        error.operator_report = report
        raise


def execute_native(frame, stage, records, *, invoke, engine, resources,
                   query_id, profile, budget, shared_owner=None, defer_accounting=False):
    """Execute a bound Filter/Map/Extract without replacing its native graph.

    ``records`` correspond to ``frame`` occurrences in source order. The fresh
    allowance is supplied by the caller; the existing provider spends it before
    transferring each actual request to IPC. No model or network is allocated
    for an empty input. A borrowed owner is never closed by this stage, including
    on failure; its caller must close it after the complete original query.
    A resident caller may defer complete statistics and consume them exactly
    once through finish_accounting, after its full sink and before owner close.
    """
    if defer_accounting and shared_owner is None:
        raise ValueError('deferred accounting requires a caller-owned resident model')
    if stage.kind == "filter" and stage.method.answer_parser != LOTUS_NATIVE:
        raise ValueError("lotus-native requires the unmodified LOTUS answer parser")
    started = time.monotonic()
    report = dict(schema="semloom.lotus_native_stage.v1", status="failed",
        query_id=query_id, stage=stage.describe(), protocol="local_async_model_IPC",
        production_core_used=False, original_operators_used=True,
        scheduler_owner="original LOTUS LM / LiteLLM batch_completion",
        lifecycle=("borrowed native model; caller owns final close" if shared_owner is not None
                   else "cold local model; startup and close included"))
    owner = shared_owner
    binding = method = result = expected = None
    failure = None
    try:
        if type(profile) is not dict or set(profile) != {"native_lm_max_batch_size"}:
            raise ValueError("native profile requires only native_lm_max_batch_size")
        concurrency = profile["native_lm_max_batch_size"]
        if type(concurrency) is not int or concurrency < 1:
            raise ValueError("native_lm_max_batch_size must be positive")
        if shared_owner is not None and (owner.engine != engine or owner.generation != stage.generation
                or owner.resources != resources or owner.concurrency != concurrency):
            raise ValueError("native stage differs from the resident model binding")
        stage_expected = [stage_identity(query_id, record, stage.stage_id,
            model_role=stage.model_role) for record in records]
        # The native owner retains its original CallLedger identity contract.
        # Finite-stage IDs are separate provenance, not substitutes in that ledger.
        expected = [identity(query_id, record.row_id, record.source_position,
            model_role=stage.model_role, algorithm_stage=stage.stage_id) for record in records]
        report["stage_calls"] = stage_expected
        report["stage_call_ids"] = {native["call_id"]: finite["call_id"]
            for native, finite in zip(expected, stage_expected, strict=True)}
        if not defer_accounting:
            ledger = CallLedger(expected, max_calls=max(1, len(expected)))
            ledger.mark_unconfirmed("NativeStageDidNotReturnEveryOccurrence")
            report["calls"] = ledger.report()
        if budget.remaining < len(expected):
            raise ValueError("native stage requires its complete declared call allowance")
        import lotus
        from .lotus_native_local import LocalNativeModel

        with lotus.settings.context(enable_cache=False):
            method = LotusRecordMethod(stage)
            method.lm.max_batch_size = concurrency
            if shared_owner is None:
                owner = LocalNativeModel(engine, stage.generation, resources, concurrency)
            with owner.query(method.lm, expected, budget, query_id=query_id) as binding:
                if owner.ready_report is not None:
                    report["model_ready_observed_ns"] = time.monotonic_ns()
                    if not defer_accounting:
                        report["owner_ready"] = deepcopy(owner.ready_report)
                with lotus.settings.context(lm=method.lm):
                    with _observe_messages(method.lm, records, binding.capture_prompts):
                        result = invoke()
                if not defer_accounting:
                    report.update(binding.report())
                    counts = report["calls"]["counts"]
                    if counts["succeeded"] != len(expected) or counts["missing"]:
                        raise RuntimeError("native accessor did not return every declared model call")
        report["status"] = "native_returned" if defer_accounting else "completed"
    except BaseException as error:
        failure = error
    finally:
        if owner is not None and shared_owner is None:
            try:
                report["backend_close"] = owner.close(**({} if resources.query_timeout_s is None else dict(timeout=resources.query_timeout_s)))
                if report["backend_close"]["status"] != "closed" and failure is None:
                    failure = RuntimeError("native model physical cleanup is unconfirmed")
            except BaseException as error:
                report["backend_close"] = deepcopy(owner.last_close_report) or dict(
                    status="unknown", process_exit_confirmed=False,
                    cleanup_errors=[type(error).__name__])
                if failure is None:
                    failure = error
                else:
                    failure.add_note("native model close failed: " + type(error).__name__)
        if defer_accounting:
            if binding is not None:
                report.update(_native_binding=binding, _native_lm=method.lm, _native_ready=owner.ready_report)
                if failure is not None:
                    try:
                        finish_accounting(report, require_complete=False)
                    except BaseException as error:
                        failure.add_note('native stage failure accounting also failed: ' + type(error).__name__)
            elif expected is not None:
                ledger = CallLedger(expected, max_calls=max(1, len(expected)))
                ledger.mark_unconfirmed("NativeStageDidNotReturnEveryOccurrence")
                report['calls'] = ledger.report()
        elif binding is not None:
            try:
                final = report.get("backend_close", {}).get("native_final_accounting")
                report.update({key: value for key, value in final.items()
                    if key not in ("schema", "query_id")} if final is not None else binding.report())
            except BaseException as error:
                report["accounting_error_type"] = type(error).__name__
                if failure is None:
                    failure = error
                else:
                    failure.add_note("native model accounting failed: " + type(error).__name__)
        if method is not None and not defer_accounting:
            report["lm"] = lm_report(method.lm)
        report["operator_seconds"] = time.monotonic() - started
    if failure is not None:
        report["status"] = "failed"
        failure.operator_report = report
        raise failure
    return dict(report, frame=result)
