"""One finite author query: existing lease/guard, permanent unit and final receipts.

The caller supplies semantic preflight and execution. No task or stage scheduling
is performed here; both public record profiles use the existing author backend.
"""

from pathlib import Path
import signal
import threading
from types import SimpleNamespace

from src.baselines.common.private_artifacts import require_outside_git
from src.baselines.common.redact import redact_text


def run_owned_query(root, owner, *, query_id, backend, required_requests, preflight,
                    execute, guard_factory, write_report):
    from integrations.imlane_records.owner import StopBudget
    from src.experiments.attempt_ledger import AttemptBudget
    from src.experiments.cell_budget import CellBudgetLedger

    if type(required_requests) is not int or required_requests < 1:
        raise ValueError("a positive permanent unit allowance is required")
    require_outside_git(root)
    if not root.is_dir() or any((root / name).exists() for name in ("query.json", "relation-control", "phase-0", "cascade-source.csv",
            "cascade-helper_score", "cascade-threshold_sample", "cascade-fallback")):
        raise ValueError("the author worker requires a new private run directory")
    args = SimpleNamespace(output=root, scopes=[], stop_budget=StopBudget(threading.Event()),
        owner_resource_limits=owner["owner_resource_limits"],
        **{key: Path(owner[key]) for key in ("author_root", "bridge_binary", "artifact_root", "method_python")})
    primary = ledger = guard = None
    report = dict(status="failed", calls=[], query_id=query_id, backend=backend)
    previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
    try:
        for number in previous:
            signal.signal(number, args.stop_budget.signal)
        preflight()
        guard = guard_factory(owner, args)
        budget = owner["budget"]
        ledger = CellBudgetLedger(Path(budget["path"]), AttemptBudget(budget["id"], budget["limit"]))
        claim = ledger.claim_unit(budget["unit_id"])
        upper = required_requests
        if claim.reservation.requests < upper:
            raise ValueError("the already-reserved unit cannot cover this complete query")
        report = execute(args, guard)
    except BaseException as error:
        primary = error
        report = getattr(error, "query_report", report)
    finally:
        if guard is not None:
            try:
                report["physical_exit"] = guard.finish()
                if (report["physical_exit"]["gpu_exit_confirmed"] is not True
                        or report["physical_exit"]["external_lease_valid"] is not True):
                    raise RuntimeError("the query's independent GPU exit or external lease remains unconfirmed")
            except BaseException as error:
                report["status"] = "failed"
                if primary is None:
                    primary = error
                else:
                    primary.add_note("physical exit check also failed: " + type(error).__name__)
        if ledger is not None:
            try:
                report["permanent_budget"] = ledger.snapshot()
            except BaseException as error:
                report["status"] = "failed"
                if primary is None:
                    primary = error
                else:
                    primary.add_note("budget receipt also failed: " + type(error).__name__)
        report.update(budget_scope="existing unit charged once for worst-case query; no refunds or automatic retries",
                      external_host_lease_released_by_worker=False, independent_descendant_audit="outer supervisor")
        if primary is not None:
            report.update(status="failed", error_type=type(primary).__name__, error=redact_text(str(primary)))
            primary.query_report = report
        try:
            write_report(root / "query.json", report)
        except BaseException as error:
            report.update(status="failed", report_error_type=type(error).__name__)
            if primary is None:
                primary = error
            else:
                primary.add_note("query report also failed: " + type(error).__name__)
        for number, handler in previous.items():
            signal.signal(number, handler)
    if primary is not None:
        primary.query_report = report
        raise primary
    return report
