"""Movie/Core workload adaptation for shared runtime calibration.

Only plan identity and complete-query receipts belong here. Signature, selection,
cache and bounded scan orchestration live in scheduling.runtime.calibration.
"""

from copy import deepcopy
from dataclasses import asdict, replace
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


def _records_capacity_policy(plan):
    """Normalize fixed declarations, retaining limits that vary across the scan.

    A count equal to today's C is not equivalent to a count derived from C.
    Only drop an override equal to its default throughout the eligible C range.
    The existing resolver's defaults are monotone in owner R, so endpoints suffice.
    """
    from src.experiments.movie_records_config import records_runtime
    configured = records_runtime(plan)
    raw_capacity = plan.get('core_capacity', {})
    derived = (configured.capacity.budget is not None and type(raw_capacity) is dict
               and raw_capacity.get('budget') is None)
    if derived:
        return dict(version=2,
            core=dict(asdict(configured.capacity), poll_interval_s=float(configured.capacity.poll_interval_s)),
            IPC_overrides=dict(asdict(configured.async_limits),
                feedback_interval_s=float(configured.async_limits.feedback_interval_s)),
            source=configured.profile['resource_budget_source'])
    if not set(plan) & {"core_capacity", "async_limits"}:
        return None
    from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity, resolve_records_async_limits
    from src.execution_provider.adapters.records_profile import resolve_core_profile
    from src.semantic_methods.lotus.records import RecordsResources
    from src.serving.backends.vllm_protocol import EngineSpec

    for name in ("core_capacity", "async_limits"):
        if type(plan.get(name, {})) is not dict:
            raise ValueError(name + " must be an object")
    capacity = CoreRecordsCapacity(**plan.get("core_capacity", {}))
    resources = RecordsResources(**plan["resources"])
    engine = EngineSpec.from_dict(plan["engine"])
    resolved = resolve_core_profile(engine, resources, plan["profile"],
        rolling_refill=plan.get("rolling_refill", False), capacity=capacity)
    owner = resolved.get("owner_max_inflight", resolved["max_inflight"])
    overrides = plan.get("async_limits", {})
    actual = asdict(resolve_records_async_limits(
        replace(resources, max_active_batches=owner), capacity, overrides))
    actual["feedback_interval_s"] = float(actual["feedback_interval_s"])
    if capacity.budget is not None:
        return dict(core=dict(asdict(capacity), poll_interval_s=float(capacity.poll_interval_s)),
                    IPC_overrides=actual)
    upper = min(resources.max_active_batches, engine.max_num_seqs) \
        if engine.max_num_seqs is not None else resources.max_active_batches
    bounds = (owner, owner) if "admission" in resolved else (1, upper)
    defaults = []
    for maximum in bounds:
        values = asdict(resolve_records_async_limits(
            replace(resources, max_active_batches=maximum), CoreRecordsCapacity()))
        # Core may allow a smaller result than IPC's minimum, with an explicit
        # valid IPC override. Comparing defaults must not reject that legal pair.
        values.update(item_input_bytes=capacity.item_input_bytes, item_result_bytes=capacity.item_result_bytes)
        values["input_bytes"] = maximum * overrides.get("item_input_bytes", capacity.item_input_bytes)
        values["result_bytes"] = overrides.get("result_slots", maximum) * overrides.get(
            "item_result_bytes", capacity.item_result_bytes)
        defaults.append(values)
    normalized = {key: actual[key] for key in overrides
                  if any(values[key] != actual[key] for values in defaults)}
    core = dict(asdict(capacity), poll_interval_s=float(capacity.poll_interval_s))
    if capacity == CoreRecordsCapacity() and not normalized:
        return None  # Keep the pre-capacity identity for the equivalent default policy.
    return dict(core=core, IPC_overrides=normalized)


def _core_identity_fields(plan, checked, *, hardware, model, software):
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
    _seconds(hardware["cpu_limit"], "hardware.cpu_limit")
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
    capacity = _records_capacity_policy(plan)
    if capacity is not None:
        key = 'records_capacity_policy_v2' if capacity.get('version') == 2 else 'records_capacity_policy_v1'
        identity['execution'][key] = capacity
        if capacity['core'].get('budget') is not None:
            from src.semantic_methods.lotus.prepared_batching import PreparedBatching
            batching = plan.get('prepared_batching')
            batching = None if batching is None else asdict(PreparedBatching(**batching))
            identity['execution']['resources']['max_active_batches'] = capacity['core']['budget']['active_requests']
            identity['execution']['record_organization_v1'] = dict(
                prepared_batching=batching,
                rolling_refill=plan.get('rolling_refill', False))
    if 'preparation_mode' in plan:
        from src.experiments.movie_records_config import record_batching_options, records_runtime
        from src.execution_provider.adapters.records_profile import resolve_record_batching
        preparation = resolve_record_batching(records_runtime(plan).profile, **record_batching_options(plan))
        flags = {name: preparation[name] for name in ('interleaved_preparation', 'event_driven_preparation')
                 if preparation is not None and name in preparation}
        if flags:
            identity['execution']['record_preparation_v1'] = flags
    return identity


def build_core_identity(plan, checked, *, hardware, model, software, model_ready=None):
    identity = _core_identity_fields(plan, checked, hardware=hardware, model=model, software=software)
    from src.serving.backends.vllm_protocol import EngineSpec
    engine = EngineSpec.from_dict(plan['engine'])
    if engine.max_model_len is None:
        from src.serving.backends.model_context import model_context_identity
        identity['execution']['model_context'] = model_context_identity(engine, model_ready)
    core_identity_signature(identity)
    return identity


def core_context_plan(plan, checked, candidate, *, hardware, model, software):
    """Prepare the first declared sample, without claiming an observed identity."""
    from src.serving.backends.vllm_protocol import EngineSpec
    if EngineSpec.from_dict(plan['engine']).max_model_len is not None:
        raise ValueError('context confirmation is only needed for omitted context')
    fields = _core_identity_fields(plan, checked, hardware=hardware, model=model, software=software)
    _candidate_key(candidate, fields)
    return _apply_candidate(plan, candidate)


def core_model_ready(plan, ready, *, expected=None):
    """Check the executing owner; another owner's observation is only an expectation."""
    from src.serving.backends.vllm_protocol import EngineSpec
    from src.serving.backends.model_context import checked_model_context, model_context_identity
    engine = EngineSpec.from_dict(plan['engine'])
    if type(ready) is dict and (ready.get('closed') is True or ready.get('status') in {'failed', 'unknown'}):
        raise RuntimeError('executing model is not ready')
    checked_model_context(engine, ready)
    if expected is not None and model_context_identity(engine, ready) != model_context_identity(engine, expected):
        raise ValueError('executing model context differs from the calibration observation')
    return deepcopy(ready)


def core_session_model_ready(plan, report, *, expected=None):
    """Bind complete query calls to their actual session owner's ready receipt."""
    owners = report.get('owners')
    if not isinstance(owners, list) or not owners:
        raise RuntimeError('Core context confirmation requires session owners')
    ready_by_owner = {}
    first = None
    def empty_query(query):
        raw = query.get('backend_report') or {}
        calls = raw.get('calls', {})
        counts = calls.get('counts', {})
        return (query.get('status') == raw.get('status') == 'completed'
            and query.get('complete_result_observed') is True and calls.get('records') == []
            and all(type(counts.get(k)) is int and counts[k] == 0
                for k in ('expected', 'recorded', 'succeeded', 'failed', 'unknown', 'missing', 'submission_unconfirmed')))
    for owner in owners:
        if type(owner) is not dict:
            raise ValueError('Core context confirmation requires owner records')
        owner_id = owner.get('owner_id')
        if type(owner_id) is not str or not owner_id or owner_id in ready_by_owner:
            raise ValueError('Core context confirmation requires unique owner identities')
        observed = owner.get('ready')
        if type(observed) is dict and observed.get('no_session_model') is True:
            observed = observed.get('query_model_ready')
            if observed is None:
                queries = [q for q in report.get('queries', []) if q.get('owner_id') == owner_id]
                if not queries or not all(empty_query(q) for q in queries):
                    raise RuntimeError('deferred model context was not confirmed for a nonempty query')
                ready_by_owner[owner_id] = None
                continue
        ready = core_model_ready(plan, observed, expected=expected if first is None else first)
        ready_by_owner[owner_id] = ready
        if first is None:
            first = ready
    for query in report.get('queries', []):
        if query.get('owner_id') not in ready_by_owner:
            raise ValueError('query is missing its actual model owner')
        ready = ready_by_owner.get(query.get('owner_id'))
        if ready is None:
            continue  # Only a completed zero-call deferred query was accepted above.
        calls = (query.get('backend_report') or {}).get('calls', {})
        records = calls.get('records', [])
        recorded = calls.get('counts', {}).get('recorded', 0)
        if type(recorded) is not int or recorded < 0 or len(records) != recorded:
            raise ValueError('context confirmation requires every recorded model call')
        if any(row.get('engine_uid') != ready['engine_uid'] for row in records):
            raise ValueError('query call changed the executing model identity')
    if first is None:
        raise RuntimeError('Core context confirmation did not observe a model')
    return first


def core_context_failure(checked, report):
    """Retain paid call counts when no observed calibration identity can be built."""
    costs = dict(declared_model_calls=sum(q['expected_calls'] for q in checked['queries']),
        recorded_model_calls=0, warmup_recorded_model_calls=0,
        succeeded_model_calls=0, failed_model_calls=0, unknown_model_calls=0)
    for query in report.get('queries', []):
        counts = (query.get('backend_report') or {}).get('calls', {}).get('counts', {})
        for name in ('recorded', 'succeeded', 'failed', 'unknown'):
            value = counts.get(name, 0)
            if type(value) is not int or value < 0:
                raise ValueError('invalid observed call count during context confirmation')
            costs[name + '_model_calls'] += value
        if query['phase'] == 'warmup':
            costs['warmup_recorded_model_calls'] += counts.get('recorded', 0)
    return dict(schema='semloom.core_context_failure.v1', status='failed',
        identity_sha256=None, reasons=['executing_model_context_not_confirmed'], **costs)


def _candidate_key(candidate, identity):
    if type(candidate) is not dict or set(candidate) != {"max_inflight", "cpu_workers"}:
        raise ValueError("candidate needs max_inflight and cpu_workers only")
    maximum = _positive_integer(candidate["max_inflight"], "max_inflight")
    workers = _positive_integer(candidate["cpu_workers"], "cpu_workers")
    execution = identity["execution"]
    capacity = execution["resources"]["max_active_batches"]
    policy = execution.get('records_capacity_policy_v2') or execution.get('records_capacity_policy_v1')
    native_sequences = execution["engine"].get("max_num_seqs")
    if native_sequences is not None and (policy is None or policy['core'].get('budget') is None):
        capacity = min(native_sequences, capacity)
    if maximum > capacity:
        raise ValueError("candidate exceeds declared Core/model capacity")
    # Match the existing session's Ray allocation: prepare + model + driver.
    if workers + execution["resources"]["gpu_workers"] + 1 > identity["hardware"]["cpu_limit"]:
        raise ValueError("candidate exceeds the observed CPU allocation")
    if policy is not None:
        from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity, resolve_records_async_limits
        from src.semantic_methods.lotus.records import RecordsResources

        owner = (policy['core']['budget']['active_requests'] if policy['core'].get('budget') is not None
                 else execution["resources"]["max_active_batches"] if execution["profile"].get("admission") else maximum)
        resources = RecordsResources(**dict(execution["resources"], cpu_workers=workers, max_active_batches=owner))
        resolve_records_async_limits(resources, CoreRecordsCapacity(**policy["core"]), policy["IPC_overrides"])
    return maximum, workers


def _apply_candidate(plan, candidate, sample_id=None):
    result = deepcopy(plan)
    result['profile']['max_inflight'] = candidate['max_inflight']
    result['resources']['cpu_workers'] = candidate['cpu_workers']
    if sample_id is not None:
        if type(sample_id) is not str or not sample_id.strip():
            raise ValueError('sample_id must be nonempty')
        for query in result['queries']:
            query['query_id'] = sample_id + ':' + query['query_id']
    return result


def core_candidate_plan(plan, checked, candidate, *, identity, sample_id=None, model_ready=None):
    """Build a plan for the existing run_movie_session, or materialize a cache hit.

    Assign sample_id before the caller checks/claims each new scan unit. No files,
    claims, model ownership or query execution are created by this function.
    """
    current = build_core_identity(plan, checked, hardware=identity["hardware"], model=identity["model"],
                                  software=identity["execution"]["software"], model_ready=model_ready)
    if core_identity_signature(current) != core_identity_signature(identity):
        raise ValueError("plan differs from the Core calibration identity")
    _candidate_key(candidate, identity)
    return _apply_candidate(plan, candidate, sample_id)


def core_measurement(plan, checked, report, *, identity, repeat_index, resources_passed, model_ready=None):
    """Reduce an existing session receipt, retaining failed/unknown sample costs.

    resources_passed is the existing supervisor's complete resource/cleanup
    verdict, not a second profiler or a check inferred from a successful query.
    """
    repeat_index = _positive_integer(repeat_index, "repeat_index")
    if type(resources_passed) is not bool:
        raise ValueError("resources_passed requires the existing supervisor verdict")
    candidate = dict(max_inflight=plan["profile"]["max_inflight"], cpu_workers=plan["resources"]["cpu_workers"])
    core_candidate_plan(plan, checked, candidate, identity=identity, model_ready=model_ready)
    reasons = []
    if identity['execution'].get('model_context') is not None:
        try:
            core_session_model_ready(plan, report, expected=model_ready)
        except (ValueError, RuntimeError):
            reasons.append('executing_model_context_not_confirmed')
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
