"""Finite Q3 campaign orchestration; native backends retain all task scheduling.

Checking never allocates a budget or starts an execution framework. Model work
requires a separately bound authorization, a fresh ledger and the normal host
lease. Worker processes claim one complete unit before constructing any owner.
"""

from collections import Counter
import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import statistics
import sys
import time

from src.baselines.common.private_artifacts import new_private_directory, require_outside_git, write_private_json
from src.baselines.common.redact import redact_json_values, redact_text
from src.baselines.text.movie_records import MovieRecordSource
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec

REPO = Path(__file__).resolve().parents[3]
CONFIG_ROOT = REPO / "deploy/autodl/sembench_records"
ADAPTERS = {
    "lotus-native": "src.baselines.text.frameworks.lotus_native_local_model",
    "imlane": "src.experiments.imlane_movie_formal",
}


def runtime_files():
    return {str(p.relative_to(REPO)) for directory in ("code/src", "code/integrations", "code/scripts")
        for p in (REPO / directory).rglob("*") if p.is_file() and p.suffix in (".py", ".cpp", ".hpp")}


def read(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 8 * 1024**2:
        raise ValueError("a bounded regular JSON file is required")
    def reject(value):
        raise ValueError("nonfinite JSON: " + value)
    return json.loads(path.read_text(), parse_constant=reject)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def check_package(root):
    """Pure file/source/budget binding check, without runtime/model construction."""
    root = Path(root).resolve()
    require_outside_git(root)
    inventory = read(root / "integrity.json")
    if any(Path(n).is_absolute() or ".." in Path(n).parts for n in inventory):
        raise ValueError("package inventory paths must stay inside the package")
    actual = {str(p.relative_to(root)) for p in root.rglob("*") if p.is_file() and p.name != "integrity.json"}
    if set(inventory) != actual or any(sha(root / n) != h for n, h in inventory.items()):
        raise ValueError("preparation package file inventory/SHA256 changed")
    locked = read(CONFIG_ROOT / "source.lock.json")
    if (set(locked["runtime_files_sha256"]) != runtime_files()
            or any(sha(REPO / n) != h for n, h in locked["runtime_files_sha256"].items())):
        raise ValueError("runtime source changed after preparation")
    identity = read(root / "package-identity.json")
    if (identity["runtime_source_lock_sha256"] != sha(CONFIG_ROOT / "source.lock.json")
            or any(sha(CONFIG_ROOT / n) != h for n, h in identity["preparation_files_sha256"].items())):
        raise ValueError("preparer source/config identity changed")
    schedule, runtime = read(root / "schedule.json"), read(root / "runtime-binding.json")
    expected, all_calls = set(), set()
    source = MovieRecordSource.from_manifest(root / "input/manifest.json")
    for summary in schedule["units"]:
        unit = read(root / summary["manifest"])
        if (sha(root / summary["manifest"]) != summary["sha256"] or summary["query_id"] in expected
                or unit["query_id"] != summary["query_id"]):
            raise ValueError("query identity changed or was reused")
        expected.add(summary["query_id"])
        calls = unit["stages"][0]["calls"]
        if (calls != source.expected(unit["query_id"]) or len(calls) != summary["max_calls"]
                or all_calls.intersection(c["call_id"] for c in calls)
                or unit["source_manifest_sha256"] != sha(source.manifest_path)
                or unit["model_call_upper_bound"] != summary["max_calls"]):
            raise ValueError("per-stage call upper bound differs")
        all_calls.update(c["call_id"] for c in calls)
        for cfg in unit["configurations"]:
            if sha(root / cfg["path"]) != cfg["sha256"]:
                raise ValueError("candidate config changed")
    total = sum(u["max_calls"] for u in schedule["units"])
    if (total != schedule["max_model_calls"] or total != 4200 or len(expected) != 35
            or schedule["max_round_wall_s"] != 30150):
        raise ValueError("finite matrix/budget changed; requires a new plan")
    return schedule, runtime


def check(root):
    schedule, runtime = check_package(root)
    blockers = []
    for backend, module in ADAPTERS.items():
        try:
            if not callable(getattr(importlib.import_module(module), "run", None)):
                blockers.append(backend + " adapter has no run entry")
        except ModuleNotFoundError as error:
            blockers.append(backend + " adapter/dependency missing: " + str(error.name))
    if runtime["binding_status"] != "current_runtime_qualified":
        blockers.append("runtime binding is not current_runtime_qualified")
    return dict(status="blocked" if blockers else "structurally_ready_pending_authorization",
        blockers=blockers, actual_model_calls=0, budget_claims=0, max_model_calls=schedule["max_model_calls"],
        units=len(schedule["units"]), formal_execution_authorized=False)


def validate_runtime_qualification(path):
    """An old status-only or CPU report cannot qualify the new executable source."""
    report = read(path)
    locked, plan = read(CONFIG_ROOT / "source.lock.json"), read(CONFIG_ROOT / "comparison.json")
    backends = {"lotus-native", "semloom-core", "ray-data", "daft", "imlane"}
    if (report.get("schema") != "semloom.sembench_records_runtime_qualification.v1"
            or report.get("status") != "ok" or report.get("qualification_kind") != "real_model"
            or report.get("runtime_source_lock_sha256") != sha(CONFIG_ROOT / "source.lock.json")
            or report.get("model_identity_evidence_sha256") != locked["model_identity_evidence_sha256"]
            or report.get("sembench_commit") != plan["sembench"]["commit"]
            or report.get("input_csv_sha256") != plan["input"]["csv_sha256"]
            or set(report.get("backends", {})) != backends):
        raise ValueError("current-source real-model qualification identity required; CPU/old reports are insufficient")
    for backend, evidence in report["backends"].items():
        if (evidence.get("status") != "passed" or type(evidence.get("real_model_calls")) is not int
                or evidence["real_model_calls"] <= 0 or evidence.get("physical_exit_confirmed") is not True
                or evidence.get("quality_comparable") is not True
                or sha(evidence["evidence_path"]) != evidence.get("evidence_sha256")):
            raise ValueError(backend + " requires positive real calls, bound evidence, comparable quality and physical exit")
    return report


def validate_authorization(root, authorization):
    schedule, runtime = check_package(root)
    value = read(authorization)
    required = {"schema", "approved", "budget_id", "limit_calls", "max_wall_s",
        "package_integrity_sha256", "runtime_source_lock_sha256", "runtime_preflight_sha256"}
    if (set(value) != required or value["schema"] != "semloom.sembench_records_authorization.v1"
            or value["approved"] is not True
            or type(value["limit_calls"]) is not int or value["limit_calls"] != schedule["max_model_calls"]
            or type(value["max_wall_s"]) is not int or value["max_wall_s"] != schedule["max_round_wall_s"]
            or value["package_integrity_sha256"] != sha(Path(root) / "integrity.json")
            or value["runtime_source_lock_sha256"] != sha(CONFIG_ROOT / "source.lock.json")
            or value["runtime_preflight_sha256"] != sha(runtime["runtime_preflight_report"])):
        raise ValueError("formal authorization must exactly bind the reviewed package/source/runtime/budget")
    AttemptBudget(value["budget_id"], value["limit_calls"])
    validate_runtime_qualification(runtime["runtime_preflight_report"])
    if check(root)["blockers"]:
        raise ValueError("formal adapters/runtime are not ready")
    return value


class QueryClock:
    def __init__(self):
        self.events = []

    def __call__(self, event):
        self.events.append(dict(event=event, monotonic_ns=time.monotonic_ns()))

    def result(self, invoked_ns, finished_ns):
        found = [e["monotonic_ns"] for e in self.events if e["event"] == "result_ready"]
        if len(found) != 1 or not invoked_ns <= found[0] <= finished_ns:
            raise ValueError("one complete pre-evaluation result_ready timestamp is required")
        return dict(query_result_seconds=(found[0] - invoked_ns) / 1e9,
            application_e2e_seconds=(finished_ns - invoked_ns) / 1e9, events=self.events,
            scope="worker invocation through full COUNT; application includes worker-owned evaluation/close",
            resident_query_seconds=None)


def validate_result(report, expected):
    if report.get("status") != "completed":
        raise ValueError("query did not complete")
    if report.get("inference_calls") == 0 or report.get("execution_kind") == "fixed-response software qualification":
        raise ValueError("CPU/preflight evidence cannot qualify a model execution")
    ledger = CallLedger(expected, max_calls=len(expected))
    for row in report["calls"]["records"]:
        ledger.record(row)
    calls = ledger.report()
    if calls["counts"]["succeeded"] != len(expected) or calls["counts"]["missing"] or calls["usage"]["total_tokens"] is None:
        raise ValueError("query has failed/unknown/missing calls or incomplete usage")
    if any(type(row.get("text")) is not str or row.get("finish_reason") not in ("stop", "length")
            or row.get("total_tokens") != row["prompt_tokens"] + row["completion_tokens"] for row in calls["records"]):
        raise ValueError("successful calls require real terminal text/finish and consistent usage")
    if any(type(row.get("messages_sha256")) is not str or len(row["messages_sha256"]) != 64
            or any(c not in "0123456789abcdef" for c in row["messages_sha256"]) for row in calls["records"]):
        raise ValueError("actual per-call untemplated message identities are required")
    decisions = report["decisions"]
    if (len(decisions) != len(expected) or len({r["row_id"] for r in decisions}) != len(expected)
            or {r["row_id"] for r in decisions} != {r["row_id"] for r in expected}
            or any(type(r["decision"]) is not bool for r in decisions)
            or report["result"]["positive_review_cnt"] != sum(r["decision"] for r in decisions)
            or "original_metric" not in report["quality"]):
        raise ValueError("complete row decisions, COUNT and original evaluation required")
    return dict(decisions=sorted((r["row_id"], r["decision"]) for r in decisions),
        messages=sorted((r["row_id"], r["messages_sha256"]) for r in calls["records"]),
        result=report["result"], original_metric=report["quality"]["original_metric"])


def parser_audit(report, method, generation):
    """Audit default use after the unchanged LOTUS CoT preprocessor, outside timing."""
    from lotus.sem_ops.postprocessors import get_cot_postprocessor
    from src.semantic_methods.lotus.engine import make_lm
    rows = report["calls"]["records"]
    outputs, _ = get_cot_postprocessor(make_lm(method, generation))([r["text"] for r in rows])
    positive, negative = (token.lower() for token in method.output_tokens)
    defaults, ambiguous = [], []
    for row, answer in zip(rows, outputs, strict=True):
        lowered = answer.lower() if answer is not None else ""
        if positive not in lowered and negative not in lowered:
            defaults.append(row["call_id"])
        elif positive in lowered and negative in lowered:
            ambiguous.append(row["call_id"])
    return dict(original_parser_default=method.default, invalid_format_default_count=len(defaults),
        default_call_ids=defaults, both_tokens_count=len(ambiguous), both_tokens_call_ids=ambiguous,
        original_positive_token_precedence_preserved=True, altered_decisions=False)


def select_profiles(calibration):
    selected = {}
    for row in calibration:
        key = row["backend"]
        score = (row["cold_complete_seconds"], row["profile"])
        if key not in selected or score < selected[key][0]:
            selected[key] = (score, row["profile"])
    if len(selected) != 5 or len(calibration) != 10 or any(
            Counter(r["profile"] for r in calibration if r["backend"] == b) != {"p1": 1, "p2": 1} for b in selected):
        raise ValueError("all five backends require two completed calibration points")
    return {b: value[1] for b, value in selected.items()}


def worker(root, unit_path, config_path, context_path, output):
    """One already-reserved unit, claimed exactly once before any model owner."""
    from src.experiments.lotus_records_movie import _model_worker, read_config, model_preflight
    root, output = Path(root), Path(output)
    unit, config, context = read(unit_path), read(config_path), read(context_path)
    clock = QueryClock()
    clock("worker_entry")
    primary = None
    try:
        check_package(root)
        auth = validate_authorization(root, context["authorization_path"])
        if sha(context["authorization_path"]) != context["authorization_sha256"]:
            raise ValueError("worker authorization changed")
        from integrations.imlane_records.chain_runtime import check_external_lease
        check_external_lease(context["owner_context"])
        if (Path(unit_path).resolve().parent != root.resolve() / "units"
                or Path(config_path).resolve().parent != root.resolve() / "configs"
                or not any((root / c["path"]).resolve() == Path(config_path).resolve()
                    and c["sha256"] == sha(config_path) for c in unit["configurations"])):
            raise ValueError("worker unit/config must be the bound prepared candidates")
        budget = context["budget"]
        if (budget["id"] != auth["budget_id"] or budget["limit"] != auth["limit_calls"]
                or budget["unit_id"] != unit["query_id"]):
            raise ValueError("worker budget differs from authorization/query")
        ledger = CellBudgetLedger(Path(budget["path"]), AttemptBudget(budget["id"], budget["limit"]))
        claim = ledger.claim_unit(unit["query_id"])
        if claim.reservation.requests != unit["model_call_upper_bound"]:
            raise ValueError("query claim differs from complete stage upper bound")
        context["claimed_budget"] = claim
        context["output_dir"] = output
        source = MovieRecordSource.from_manifest(root / "input/manifest.json")
        runtime = read(root / "runtime-binding.json")
        from integrations.imlane_records.resource_limits import gpu_snapshot
        assigned = gpu_snapshot(runtime["cuda_visible_devices"])
        if (assigned["uuid"] != context["assigned_gpu_uuid"] or assigned["compute_processes"]
                or assigned["memory_mib"] > 1):
            raise RuntimeError("worker assigned GPU identity/idleness changed before owner allocation")
        runtime = dict(runtime, cuda_visible_devices=assigned["uuid"])
        os.environ["CUDA_VISIBLE_DEVICES"] = assigned["uuid"]
        os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
        write_private_json(output / "worker-binding.json", dict(
            effective_cuda_visible_devices=assigned["uuid"], query_id=unit["query_id"],
            claimed_requests=claim.reservation.requests, model_calls_started=0,
            runtime_source_lock_sha256=sha(CONFIG_ROOT / "source.lock.json")))
        checkout = context["sembench_checkout"]
        if unit["backend"] in ADAPTERS:
            engine = EngineSpec.from_dict(config.get("engine", config.get("requested_engine")))
            method = FilterMethod.from_dict(config["method"])
            generation = GenerationSpec.from_dict(config["generation"])
            resources = RecordsResources(**config["resources"]) if "resources" in config else RecordsResources(
                startup_timeout_s=300, query_timeout_s=480)
            context["runtime_binding"] = runtime
            adapter = importlib.import_module(ADAPTERS[unit["backend"]])
            report = adapter.run(source, checkout, method, generation, engine, resources,
                query_id=unit["query_id"], profile=config.get("native_options", config["resources"] if "resources" in config else {}),
                observer=clock, context=context)
            if not (output / "query.json").exists():
                write_private_json(output / "query.json", redact_json_values(report))
        else:
            value, method, generation, resources, engine = read_config(config_path)
            model_preflight(source, checkout, method, generation, engine)
            _model_worker(value, source, checkout, method, generation, resources, engine, output, observer=clock)
        clock("worker_finished")
    except BaseException as error:
        primary = error
        if not (output / "query.json").exists():
            report = getattr(error, "query_report", dict(status="failed", error_type=type(error).__name__, error=redact_text(str(error))))
            write_private_json(output / "query.json", redact_json_values(report))
    finally:
        write_private_json(output / "clock.json", clock.events)
    if primary is not None:
        raise primary


def run_campaign(root, authorization, output):
    """Fresh serial campaign; no resume, automatic retry or old-ledger mutation."""
    from integrations.imlane_records.resource_limits import gpu_snapshot, gpu_exited
    from src.infrastructure import runner_lease
    from src.experiments.lotus_records_supervisor import supervise
    from .sembench_records_formal_resources import ResourceMonitor

    root, output = Path(root).resolve(), Path(output).resolve()
    if output.is_relative_to(root) or root.is_relative_to(output):
        raise ValueError("campaign output must be separate from the immutable preparation package")
    auth = validate_authorization(root, authorization)
    schedule, runtime = check_package(root)
    if not hasattr(os, "sched_getaffinity") or os.sched_getaffinity(0) != set(range(8)):
        raise ValueError("launch formal controller with exactly the declared CPU affinity slots0-7")
    # Target host identity is checked by the current runtime report; no model allocation here.
    before = gpu_snapshot(runtime["cuda_visible_devices"])
    if before["compute_processes"] or before["memory_mib"] > 1:
        raise RuntimeError("assigned GPU is not idle; foreign/retired owners must remain untouched")
    new_private_directory(output)
    started = time.monotonic()
    deadline = started + auth["max_wall_s"]
    ledger = CellBudgetLedger.create(output / "budget.sqlite", AttemptBudget(auth["budget_id"], auth["limit_calls"]),
        deadline_utc=time.time() + auth["max_wall_s"])
    try:
        lease = runner_lease.acquire_runner_lease(Path(runtime["imlane_artifact_root"]) / runner_lease.HOST_SCOPE_DIR_NAME,
            config_fingerprint=runner_lease.HOST_SCOPE_FINGERPRINT, repository_commit=read(CONFIG_ROOT / "comparison.json")["source_commit"],
            recover_stale=False)
    except BaseException as error:
        write_private_json(output / "preflight-failure.json", dict(status="failed_before_owner",
            error_type=type(error).__name__, error=redact_text(str(error)), permanent_budget=ledger.snapshot(),
            queries_started=0, lease_acquired=False))
        raise
    complete, calibration, selected, reference, physical_confirmed = [], [], None, None, True
    report = dict(schema="semloom.sembench_records_formal.v1", status="failed", queries=complete,
        no_automatic_retries=True, resident_results=None, p99=None, p99_99=None)
    try:
        for summary in schedule["units"]:
            check_package(root)
            unit = read(root / summary["manifest"])
            if unit["phase"] == "measured" and selected is None:
                selected = select_profiles(calibration)
                write_private_json(output / "calibration-selection.json", selected)
            profile = unit["selected_profile"] or selected[unit["backend"]]
            chosen = next(c for c in unit["configurations"] if c["profile"] == profile)
            remaining = deadline - time.monotonic()
            if remaining < schedule["serial_unit_ceiling_s"] + 30:
                raise TimeoutError("remaining global budget cannot cover the next complete unit")
            gpu_before = gpu_snapshot(runtime["cuda_visible_devices"])
            if not gpu_exited(before, gpu_before):
                physical_confirmed = False
                raise RuntimeError("GPU ownership/idleness drift before query")
            unit_root = output / unit["query_id"]
            new_private_directory(unit_root)
            ledger.reserve_unit(unit["query_id"], unit["model_call_upper_bound"])
            context = dict(budget=dict(path=str(output / "budget.sqlite"), id=auth["budget_id"],
                limit=auth["limit_calls"], unit_id=unit["query_id"]),
                owner_context=dict(lease_path=str(lease.path), lease_sha256=sha(lease.path),
                    artifact_root=runtime["imlane_artifact_root"],
                    owner_resource_limits=dict(cpu_cores=8, rss_bytes=21474836480, shm_bytes=536870912),
                    gpu_idle_max_mib=1), profile_id=profile,
                authorization_path=str(Path(authorization).resolve()), authorization_sha256=sha(authorization),
                assigned_gpu_uuid=gpu_before["uuid"])
            # The original pinned checkout is explicit in the source preparation context.
            context["sembench_checkout"] = read(root / "package-identity.json")["sembench_checkout"]
            write_private_json(unit_root / "context.json", context)
            argv = [runtime["driver_python"], str(REPO / "code/scripts/experiments/run_sembench_records_formal.py"),
                "worker", "--package", str(root), "--unit", str(root / summary["manifest"]),
                "--config", str(root / chosen["path"]), "--context", str(unit_root / "context.json"),
                "--output-dir", str(unit_root), "--execute-model"]
            physical_confirmed = False
            monitor = ResourceMonitor(read(CONFIG_ROOT / "comparison.json")["shared_resources"])
            def bounded_monitor(identities):
                if time.monotonic() >= deadline - 30:
                    raise TimeoutError("global deadline entered the reserved physical-cleanup window")
                monitor(identities)
            invoked = time.monotonic_ns()
            supervised = None
            try:
                supervised = supervise(argv, unit_root, timeout_s=schedule["serial_unit_ceiling_s"],
                    grace_s=15, natural_exit_grace_s=1, monitor=bounded_monitor)
            finally:
                physical = dict(gpu_before=gpu_before, gpu_exit_confirmed=False, resources=None)
                try:
                    gpu_after = gpu_snapshot(runtime["cuda_visible_devices"])
                    resources = monitor.finish()
                    physical.update(gpu_after=gpu_after, resources=resources,
                        gpu_exit_confirmed=gpu_exited(gpu_before, gpu_after))
                    physical_confirmed = (physical["gpu_exit_confirmed"] and resources["passed"] is True
                        and supervised is not None and supervised["observed_owned_exit_confirmed"] is True)
                except BaseException as cleanup:
                    physical.update(error_type=type(cleanup).__name__, error=redact_text(str(cleanup)))
                    physical_confirmed = False
                write_private_json(unit_root / "physical.json", physical)
            finished = time.monotonic_ns()
            if not physical_confirmed or resources["passed"] is not True or supervised["observed_owned_exit_confirmed"] is not True:
                raise RuntimeError("query physical/resource responsibility remains unresolved")
            result = read(unit_root / "query.json")
            source = MovieRecordSource.from_manifest(root / "input/manifest.json")
            signature = validate_result(result, source.expected(unit["query_id"]))
            effective_config = read(root / chosen["path"])
            write_private_json(unit_root / "quality-audit.json", parser_audit(result,
                FilterMethod.from_dict(effective_config["method"]),
                GenerationSpec.from_dict(effective_config["generation"])))
            if reference is None:
                if unit["backend"] != "lotus-native":
                    raise ValueError("the first native LOTUS calibration must establish the real-model reference")
                reference = signature
            elif signature != reference:
                raise ValueError("real-model row/message/COUNT/quality differs; execution-only comparison stops")
            clock = QueryClock()
            clock.events = read(unit_root / "clock.json")
            timing = clock.result(invoked, finished)
            write_private_json(unit_root / "timing.json", timing)
            row = dict(query_id=unit["query_id"], backend=unit["backend"], phase=unit["phase"], block=unit["block"],
                profile=profile, cold_complete_seconds=timing["application_e2e_seconds"],
                query_result_seconds=timing["query_result_seconds"], actual_model_calls=len(result["calls"]["records"]),
                input_rows=len(source.rows()),
                filter_rows=len(result["calls"]["records"]), result_rows=1,
                successful_calls_per_query_second=len(result["calls"]["records"]) / timing["query_result_seconds"],
                result_sha256=sha(unit_root / "query.json"))
            complete.append(row)
            if unit["phase"] == "calibration":
                calibration.append(row)
        report.update(status="completed", selected_profiles=selected, measured_n_per_backend=5,
            median_cold_seconds={b: statistics.median(r["cold_complete_seconds"] for r in complete
                if r["backend"] == b and r["phase"] == "measured") for b in ADAPTERS.keys() | {"semloom-core", "ray-data", "daft"}})
    except BaseException as error:
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        raise
    finally:
        try:
            final_gpu = gpu_snapshot(runtime["cuda_visible_devices"])
            physical_confirmed = physical_confirmed and gpu_exited(before, final_gpu)
            report.update(final_gpu=final_gpu, permanent_budget=ledger.snapshot())
        except BaseException as cleanup:
            physical_confirmed = False
            report.update(cleanup_error_type=type(cleanup).__name__, cleanup_error=redact_text(str(cleanup)))
        elapsed = time.monotonic() - started
        report.update(elapsed_seconds=elapsed, deadline_overrun_seconds=max(0, elapsed - auth["max_wall_s"]),
            cleanup_reserve_seconds=30, physical_exit_confirmed=physical_confirmed,
            lease_status="release_pending" if physical_confirmed else "held_unresolved")
        write_private_json(output / "campaign.json", redact_json_values(report))
        if physical_confirmed:
            lease.release()
            write_private_json(output / "lease-released.json", dict(released=True, after_campaign_evidence=True))
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    for action in ("check", "run", "worker"):
        command = sub.add_parser(action)
        command.add_argument("--package", type=Path, required=True)
        if action != "check":
            command.add_argument("--output-dir", type=Path, required=True)
            command.add_argument("--execute-model", action="store_true")
        if action == "run":
            command.add_argument("--authorization", type=Path, required=True)
        if action == "worker":
            for name in ("unit", "config", "context"):
                command.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args(argv)
    if args.action == "check" or not getattr(args, "execute_model", False):
        result = check(args.package)
        print(json.dumps(result))
        return 0 if not result["blockers"] else 2
    if args.action == "worker":
        worker(args.package, args.unit, args.config, args.context, args.output_dir)
    else:
        result = run_campaign(args.package, args.authorization, args.output_dir)
        print(json.dumps(dict(status=result["status"], queries=len(result["queries"]))))
    return 0
