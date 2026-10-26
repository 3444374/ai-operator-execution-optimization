"""Movie/Core workload adaptation for shared runtime calibration.

Only plan identity and complete-query receipts belong here. Signature, selection,
cache and bounded scan orchestration live in scheduling.runtime.calibration.
"""

from copy import deepcopy
import math

from src.scheduling.runtime.calibration import (
    Measurement, identity_signature as core_identity_signature, measurement_record,
    resolve_calibration as resolve_core_selection, select_calibration,
)

_COST_NAMES = ("declared_model_calls", "recorded_model_calls", "warmup_recorded_model_calls",
               "succeeded_model_calls", "failed_model_calls", "unknown_model_calls")


def _positive_integer(value, label):
    if type(value) is not int or value < 1:
        raise ValueError(label + " must be a positive integer")
    return value


def _seconds(value, label, *, allow_zero=False):
    if (type(value) not in (int, float) or not math.isfinite(value)
            or value < 0 or not allow_zero and value == 0):
        raise ValueError(label + " must be finite and " + ("nonnegative" if allow_zero else "positive"))
    return value


def _schedule(checked):
    return [dict(task=query["task"], manifest_sha256=query["manifest_sha256"],
                 phase=query["phase"], expected_calls=query["expected_calls"])
            for query in checked["queries"]]


def build_core_identity(plan, checked, *, hardware, model, software):
    """Bind fixed execution and checked input bytes; ignore IDs and scanned knobs.

    hardware comes from existing host checks (selected devices and resource caps).
    model revisions identify actual weights/tokenizer bytes, never just a path.
    checked is the existing check_plan(plan) receipt, without running inference.
    """
    for value, fields in (
        (hardware, ("machine_id", "gpu_ids", "gpu_driver", "cpu_limit", "resource_limits")),
        (model, ("model_revision", "tokenizer_revision")),
        (software, ("source_revision", "versions")),
    ):
        if type(value) is not dict or any(not value.get(key) for key in fields):
            raise ValueError("observed Core identity is missing " + ", ".join(fields))
    _positive_integer(hardware["cpu_limit"], "hardware.cpu_limit")
    if plan["backend"] != "semloom-core" or plan["profile"].get("mode") != "async":
        raise ValueError("offline Core selection requires the existing async Core path")
    if (plan["resources"]["prepare_batch_rows"] != 1
            or plan["resources"]["model_batch_rows"] != 1):
        raise ValueError("Core calibration retains prepare/model batch rows of one")
    if (len(plan["queries"]) != len(checked["queries"])
            or any(any(query[key] != declaration[key] for key in ("query_id", "task", "manifest", "phase"))
                   for query, declaration in zip(checked["queries"], plan["queries"]))):
        raise ValueError("checked inputs differ from the Core plan")
    schedule = _schedule(checked)
    if (not schedule or not any(q["phase"] == "measured" for q in schedule)
            or any(q["phase"] not in {"warmup", "measured"} for q in schedule)
            or sum(q["expected_calls"] for q in schedule if q["phase"] == "measured") < 1):
        raise ValueError("explicit warmup/measured query schedule required")
    identity = dict(
        hardware=deepcopy(hardware), model=dict(deepcopy(model), name=plan["model_name"]),
        generation=deepcopy(plan["generation"]),
        execution=dict(software=deepcopy(software), lifecycle=plan["lifecycle"],
            engine={key: deepcopy(value) for key, value in plan["engine"].items() if key != "max_model_calls"},
            resources={key: value for key, value in plan["resources"].items() if key != "cpu_workers"},
            profile={key: value for key, value in plan["profile"].items() if key != "max_inflight"}),
        workload=dict(queries=schedule),
        measurement=dict(objective="sum_measured_full_query_seconds", warmup="explicit_plan_queries", work_unit="model_calls"),
    )
    core_identity_signature(identity)
    return identity


def _candidate_key(candidate, identity):
    if type(candidate) is not dict or set(candidate) != {"max_inflight", "cpu_workers"}:
        raise ValueError("candidate needs max_inflight and cpu_workers only")
    maximum = _positive_integer(candidate["max_inflight"], "max_inflight")
    workers = _positive_integer(candidate["cpu_workers"], "cpu_workers")
    execution = identity["execution"]
    if maximum > min(execution["engine"]["max_num_seqs"], execution["resources"]["max_active_batches"]):
        raise ValueError("candidate exceeds declared Core/model capacity")
    # Match the existing session's Ray allocation: prepare + model + driver.
    if workers + execution["resources"]["gpu_workers"] + 1 > identity["hardware"]["cpu_limit"]:
        raise ValueError("candidate exceeds the observed CPU allocation")
    return maximum, workers


def core_candidate_plan(plan, checked, candidate, *, identity, sample_id=None):
    """Build a plan for the existing run_movie_session, or materialize a cache hit.

    Assign sample_id before the caller checks/claims each new scan unit. No files,
    claims, model ownership or query execution are created by this function.
    """
    current = build_core_identity(plan, checked, hardware=identity["hardware"], model=identity["model"],
                                  software=identity["execution"]["software"])
    if core_identity_signature(current) != core_identity_signature(identity):
        raise ValueError("plan differs from the Core calibration identity")
    maximum, workers = _candidate_key(candidate, identity)
    result = deepcopy(plan)
    result["profile"]["max_inflight"] = maximum
    result["resources"]["cpu_workers"] = workers
    if sample_id is not None:
        if type(sample_id) is not str or not sample_id.strip():
            raise ValueError("sample_id must be nonempty")
        for query in result["queries"]:
            query["query_id"] = sample_id + ":" + query["query_id"]
    return result


def core_measurement(plan, checked, report, *, identity, repeat_index, resources_passed):
    """Reduce an existing session receipt, retaining failed/unknown sample costs.

    resources_passed is the existing supervisor's complete resource/cleanup
    verdict, not a second profiler or a check inferred from a successful query.
    """
    repeat_index = _positive_integer(repeat_index, "repeat_index")
    if type(resources_passed) is not bool:
        raise ValueError("resources_passed requires the existing supervisor verdict")
    candidate = dict(max_inflight=plan["profile"]["max_inflight"], cpu_workers=plan["resources"]["cpu_workers"])
    core_candidate_plan(plan, checked, candidate, identity=identity)
    reasons = []
    if report.get("backend") != "semloom-core" or report.get("lifecycle") != plan["lifecycle"]:
        raise ValueError("session receipt is not this Core execution path")
    if report.get("status") != "completed":
        reasons.append("session_not_completed")
    if not resources_passed:
        reasons.append("resource_or_cleanup_check_failed")
    queries = report.get("queries", [])
    if len(queries) != len(checked["queries"]):
        reasons.append("incomplete_query_schedule")
    costs = dict(declared_model_calls=sum(q["expected_calls"] for q in checked["queries"]),
                 recorded_model_calls=0, warmup_recorded_model_calls=0,
                 succeeded_model_calls=0, failed_model_calls=0, unknown_model_calls=0)
    measured, warmup = [], []
    for expected, query in zip(checked["queries"], queries):
        if any(query.get(key) != expected[key] for key in ("query_id", "task", "phase")):
            raise ValueError("session query identity/order differs from the checked plan")
        raw = query.get("backend_report") or {}
        counts = raw.get("calls", {}).get("counts", {})
        for name in ("recorded", "succeeded", "failed", "unknown"):
            count = counts.get(name, 0)
            if type(count) is not int or count < 0:
                raise ValueError("invalid observed model-call count")
            costs[name + "_model_calls"] += count
        if expected["phase"] == "warmup":
            costs["warmup_recorded_model_calls"] += counts.get("recorded", 0)
        if (any(counts.get(key) != expected["expected_calls"] for key in ("expected", "recorded", "succeeded"))
                or any(counts.get(key) != 0 for key in ("missing", "failed", "unknown", "submission_unconfirmed"))):
            reasons.append("incomplete_or_unsuccessful_calls")
        actual = raw.get("core_execution", {})
        if actual.get("mode") != "async" or actual.get("max_inflight") != candidate["max_inflight"]:
            reasons.append("execution_profile_not_confirmed")
        if (raw.get("status") != "completed" or query.get("status") != "completed"
                or query.get("complete_result_observed") is not True):
            reasons.append("complete_result_not_confirmed")
        duration = query.get("full_query_seconds")
        if duration is None:
            reasons.append("missing_query_time")
        else:
            _seconds(duration, "full_query_seconds")
            (warmup if expected["phase"] == "warmup" else measured).append(duration)
    wall = report.get("application_e2e_s")
    if wall is None:
        reasons.append("missing_session_time")
    else:
        _seconds(wall, "application_e2e_s", allow_zero=True)
    return dict(schema="semloom.core_calibration_sample.v1", identity_sha256=core_identity_signature(identity),
        candidate=candidate, repeat_index=repeat_index, status="failed" if reasons else "ok",
        reasons=sorted(set(reasons)), resources_passed=resources_passed,
        full_query_s=sum(measured) if not reasons else None,
        measured_query_s=measured, warmup_query_s=warmup, session_wall_s=wall, **costs)

def core_runtime_measurement(row):
    """Convert one existing Core receipt for a public measure callback or CLI."""
    if row.get("schema") != "semloom.core_calibration_sample.v1":
        raise ValueError("a Core session measurement receipt is required")
    if any(type(row[name]) is not int or row[name] < 0 for name in _COST_NAMES):
        raise ValueError("sample call costs must be nonnegative integer counts")
    if row["session_wall_s"] is not None:
        _seconds(row["session_wall_s"], "session_wall_s", allow_zero=True)
    valid = (row["status"] == "ok" and not row.get("reasons")
        and row["failed_model_calls"] == row["unknown_model_calls"] == 0
        and row["succeeded_model_calls"] == row["recorded_model_calls"] == row["declared_model_calls"])
    return Measurement(row["full_query_s"] if valid else None,
        "ok" if valid else "unknown" if row["unknown_model_calls"] else "failed",
        row["resources_passed"], row["recorded_model_calls"],
        row["warmup_recorded_model_calls"], row["unknown_model_calls"])


def build_core_selection(identity, candidates, measurements, *, scan_wall_s,
                         minimum_repeats=3, near_peak_ratio=0.97):
    """Adapt Movie receipts to the shared runtime selector; no second policy."""
    for candidate in candidates:
        _candidate_key(candidate, identity)
    samples = []
    for row in measurements:
        if row["identity_sha256"] != core_identity_signature(identity):
            raise ValueError("measurement has a different Core identity")
        _candidate_key(row["candidate"], identity)
        samples.append(measurement_record(identity, row["candidate"], row["repeat_index"],
            core_runtime_measurement(row), reserved_work=row["declared_model_calls"]))
    return select_calibration(identity, candidates, samples, repetitions=minimum_repeats,
                              near_peak_ratio=near_peak_ratio, scan_wall_s=scan_wall_s)
