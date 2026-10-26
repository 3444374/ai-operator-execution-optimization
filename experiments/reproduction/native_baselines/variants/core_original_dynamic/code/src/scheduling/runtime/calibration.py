"""Bounded offline calibration shared by ordinary and experimental callers.

Callbacks own workload semantics, resource supervision and cleanup. This module
only measures declared candidates, chooses among them and persists a reusable
configuration. It has no model, database or experiment dependencies.
"""

from copy import deepcopy
from dataclasses import asdict, dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import tempfile
import time
from typing import Callable


def _digest(value):
    # Same canonical JSON bytes as the existing content_digest utility.
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _positive_int(value, name):
    if type(value) is not int or value < 1:
        raise ValueError(name + " must be a positive integer")
    return value


def _seconds(value, name, *, zero=False):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or not zero and value == 0:
        raise ValueError(name + " must be finite and " + ("nonnegative" if zero else "positive"))
    return value


def identity_signature(identity):
    """Bind observed platform/model/generation/execution/workload/measurement."""
    groups = {"hardware", "model", "generation", "execution", "workload", "measurement"}
    if (type(identity) is not dict or set(identity) != groups
            or any(type(value) is not dict or not value for value in identity.values())):
        raise ValueError("calibration identity requires all six nonempty identity groups")
    if not isinstance(identity["measurement"].get("work_unit"), str) or not identity["measurement"]["work_unit"]:
        raise ValueError("measurement.work_unit must name the scan budget unit")
    return _digest(dict(schema="semloom.runtime_calibration_identity.v1", **identity))


def select_near_peak(scores, *, ratio):
    """Return the smallest candidate within ratio of the measured peak score."""
    if not scores:
        raise ValueError("no eligible measured candidates")
    peak = max(scores.values())
    acceptable = sorted(key for key, score in scores.items() if score >= ratio * peak)
    if not acceptable:
        raise ValueError("no candidate satisfies the near-peak rule")
    return acceptable[0], acceptable


@dataclass(frozen=True)
class Measurement:
    """One complete workload measurement; elapsed_s follows the signed objective."""

    elapsed_s: float | None
    status: str
    resources_passed: bool
    work: int | None
    warmup_work: int = 0
    unknown_work: int = 0

    def __post_init__(self):
        if self.status not in {"ok", "failed", "unknown"} or type(self.resources_passed) is not bool:
            raise ValueError("explicit measurement status and resource verdict required")
        if self.elapsed_s is not None:
            _seconds(self.elapsed_s, "elapsed_s")
        for name in ("work", "warmup_work", "unknown_work"):
            value = getattr(self, name)
            if name == "work" and value is None:
                continue
            if type(value) is not int or value < 0:
                raise ValueError(name + " must be a nonnegative integer")
        if self.work is not None and max(self.warmup_work, self.unknown_work) > self.work:
            raise ValueError("warmup/unknown work exceeds reported work")


@dataclass(frozen=True)
class ScanBudget:
    timeout_s: float
    max_work: int

    def __post_init__(self):
        _seconds(self.timeout_s, "timeout_s")
        _positive_int(self.max_work, "max_work")


@dataclass(frozen=True)
class IncreasingSearch:
    """Grow one declared knob; named limits are bounds, never measured plateaus.

    Repeated min/max latency ranges form a conservative *observed* noise band,
    not a statistical confidence interval or a global optimum guarantee.
    """

    parameter: str
    capacity_limits: dict[str, int]
    resource_limits: dict[str, int] = field(default_factory=dict)
    growth: int = 2
    min_improvement: float = 0.03
    confirmations: int = 2

    def __post_init__(self):
        if not isinstance(self.parameter, str) or not self.parameter:
            raise ValueError("search needs a named parameter")
        if not self.capacity_limits and not self.resource_limits:
            raise ValueError("increasing search requires named finite limits")
        for limits in (self.capacity_limits, self.resource_limits):
            if type(limits) is not dict or any(not isinstance(k, str) or not k for k in limits):
                raise ValueError("search limits require names")
            for value in limits.values():
                _positive_int(value, "search limit")
        if _positive_int(self.growth, "growth") < 2 or _positive_int(self.confirmations, "confirmations") < 2:
            raise ValueError("growth and plateau confirmations must be at least two")
        if type(self.min_improvement) not in (int, float) or not 0 < self.min_improvement < 1:
            raise ValueError("min_improvement must be in (0, 1)")


def search_candidates(seed, search):
    """Expand only the possible finite reservation range; do not run probes."""
    if not isinstance(search, IncreasingSearch) or search.parameter not in seed:
        raise ValueError("search parameter must be present in its seed")
    cap = min((*search.capacity_limits.values(), *search.resource_limits.values()))
    value = _positive_int(seed[search.parameter], "search start")
    if value > cap:
        raise ValueError("search start exceeds its declared limits")
    candidates = []
    while True:
        candidates.append(dict(deepcopy(seed), **{search.parameter: value}))
        if value == cap:
            return candidates
        value = min(cap, value * search.growth)


def _key(candidate, parameters):
    if type(candidate) is not dict or set(candidate) != set(parameters):
        raise ValueError("all candidates require the same parameter names")
    return tuple(_positive_int(candidate[name], name) for name in parameters)


def _policy(candidates, repetitions, near_peak_ratio, search=None):
    _positive_int(repetitions, "repetitions")
    if type(near_peak_ratio) not in (int, float) or not 0 < near_peak_ratio <= 1:
        raise ValueError("near_peak_ratio must be in (0, 1]")
    if not candidates or not candidates[0] or any(type(name) is not str for name in candidates[0]):
        raise ValueError("declare nonempty named integer candidates")
    if search is not None:
        if len(candidates) != 1 or repetitions < 3:
            raise ValueError("increasing search needs one seed and at least three repeats")
        candidates = search_candidates(candidates[0], search)
    parameters = list(candidates[0])
    keys = [_key(candidate, parameters) for candidate in candidates]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate candidate")
    policy = dict(parameters=parameters, candidates=[dict(zip(parameters, key)) for key in sorted(keys)],
                  repetitions=repetitions, near_peak_ratio=near_peak_ratio)
    if search is not None:
        policy["search"] = asdict(search)
    return policy


def _search_evidence(policy, samples):
    """Describe measured growth, regression and plateau; never infer beyond a cap."""
    search = IncreasingSearch(**policy["search"])
    points, best, quiet, stop, hits = [], None, 0, None, []
    for candidate in policy["candidates"]:
        rows = [row["measurement"] for row in samples if row["candidate"] == candidate]
        if (len(rows) != policy["repetitions"] or any(row["status"] != "ok"
                or not row["resources_passed"] or row["work"] is None
                or row["unknown_work"] or row["elapsed_s"] is None for row in rows)):
            break
        values = [row["elapsed_s"] for row in rows]
        point = dict(candidate=deepcopy(candidate), median_s=statistics.median(values),
                     observed_min_s=min(values), observed_max_s=max(values), repeats=len(rows))
        regression = False
        if best is not None:
            optimistic_gain = 1 - point["observed_min_s"] / best["observed_max_s"]
            point["optimistic_relative_gain"] = optimistic_gain
            quiet = quiet + 1 if optimistic_gain <= search.min_improvement else 0
            regression = point["observed_min_s"] > best["observed_max_s"] * (1 + search.min_improvement)
        if best is None or point["median_s"] < best["median_s"]:
            best = point
        points.append(point)
        value = candidate[search.parameter]
        hits = [dict(kind=kind, name=name, value=limit)
                for kind, limits in (("capacity_limit", search.capacity_limits),
                                     ("resource_limit", search.resource_limits))
                for name, limit in limits.items() if value == limit]
        # Even flat observations at the final boundary cannot certify headroom.
        if hits:
            stop = "capacity_limit" if any(h["kind"] == "capacity_limit" for h in hits) else "resource_limit"
        elif regression:
            stop = "regression"
        elif quiet >= search.confirmations:
            stop = "plateau"
        if stop is not None:
            break
    return dict(stop_reason=stop, platform_confirmed=stop == "plateau", limits_hit=hits,
        consecutive_nonimproving_points=quiet, completed_points=len(points), points=points,
        lowest_median_candidate=None if best is None else best["candidate"],
        rule="observed min/max gain envelope; local measured plateau, not confidence interval/global optimum")


def measurement_record(identity, candidate, repeat_index, measurement, *, reserved_work):
    if not isinstance(measurement, Measurement):
        raise TypeError("measure callback must return Measurement")
    return dict(identity_sha256=identity_signature(identity), candidate=deepcopy(candidate),
                repeat_index=_positive_int(repeat_index, "repeat_index"),
                reserved_work=_positive_int(reserved_work, "reserved_work"), measurement=asdict(measurement))


def _scan_cost(samples, wall_s):
    return dict(wall_s=_seconds(wall_s, "scan_wall_s", zero=True), samples=len(samples),
        reserved_work=sum(row["reserved_work"] for row in samples),
        reported_work=sum(row["measurement"]["work"] or 0 for row in samples),
        unreported_reserved_work=sum(row["reserved_work"] for row in samples if row["measurement"]["work"] is None),
        warmup_work=sum(row["measurement"]["warmup_work"] for row in samples),
        unknown_work=sum(row["measurement"]["unknown_work"] for row in samples), target_measurement_included=False)


def select_calibration(identity, candidates, samples, *, repetitions=3, near_peak_ratio=0.97, scan_wall_s,
                       search=None):
    """The sole median/near-peak selector, also used for precollected measurements."""
    signature = identity_signature(identity)
    policy = _policy(candidates, repetitions, near_peak_ratio, search)
    parameters = policy["parameters"]
    grouped = {_key(candidate, parameters): {} for candidate in policy["candidates"]}
    for row in samples:
        if row["identity_sha256"] != signature:
            raise ValueError("measurement has a different calibration identity")
        key = _key(row["candidate"], parameters)
        if key not in grouped:
            raise ValueError("measurement candidate was not declared")
        repeat = _positive_int(row["repeat_index"], "repeat_index")
        if repeat in grouped[key]:
            raise ValueError("duplicate candidate repeat")
        _positive_int(row["reserved_work"], "reserved_work")
        grouped[key][repeat] = (Measurement(**row["measurement"]), row["reserved_work"])
    scores, cells = {}, []
    for key, repeats in sorted(grouped.items()):
        valid = all(m.status == "ok" and m.resources_passed and m.elapsed_s is not None
                    and m.work is not None and m.work <= reserved and m.unknown_work == 0
                    for m, reserved in repeats.values())
        reason = "failed_unknown_or_resource_check" if not valid else "insufficient_repeats" if len(repeats) < repetitions else None
        median = statistics.median(m.elapsed_s for m, _ in repeats.values()) if reason is None else None
        if reason is None:
            scores[key] = 1 / median
        cells.append(dict(candidate=dict(zip(parameters, key)), repeats=len(repeats), eligible=reason is None,
                          reason=reason, elapsed_s_median=median))
    selected, acceptable = select_near_peak(scores, ratio=near_peak_ratio)
    return dict(schema="semloom.runtime_calibration.v1", identity=deepcopy(identity), identity_sha256=signature,
        policy=policy, selection=dict(zip(parameters, selected)), performance_tuned=True, scope="measured_candidates_only",
        evidence=dict(candidates=cells, acceptable_candidates=[dict(zip(parameters, key)) for key in acceptable],
                      samples_sha256=_digest(samples)), scan_cost=_scan_cost(samples, scan_wall_s))


def resolve_calibration(path, identity, *, policy=None):
    """Read a selection only. Missing/mismatched identity or policy needs a scan."""
    signature, path = identity_signature(identity), Path(path)
    if not path.exists():
        return dict(status="needs_scan", reason="missing_selection", identity_sha256=signature, performance_tuned=False)
    raw = path.read_bytes()
    payload = json.loads(raw)
    if (payload.get("schema") != "semloom.runtime_calibration.v1"
            or identity_signature(payload["identity"]) != payload["identity_sha256"]):
        raise ValueError("invalid runtime calibration identity/schema")
    if payload["identity_sha256"] != signature or policy is not None and payload["policy"] != policy:
        return dict(status="needs_scan", reason="identity_or_policy_mismatch", identity_sha256=signature, performance_tuned=False)
    stored = payload["policy"]
    search = IncreasingSearch(**stored["search"]) if "search" in stored else None
    seeds = stored["candidates"][:1] if search is not None else stored["candidates"]
    if stored != _policy(seeds, stored["repetitions"], stored["near_peak_ratio"], search):
        raise ValueError("invalid calibration policy")
    if search is not None:
        original = select_calibration(payload["identity"], seeds, payload["search_samples"],
            repetitions=stored["repetitions"], near_peak_ratio=stored["near_peak_ratio"],
            scan_wall_s=payload["scan_cost"]["wall_s"], search=search)
        observed = _search_evidence(stored, payload["search_samples"])
        if (original["selection"] != payload["selection"] or original["evidence"] != payload["evidence"]
                or observed != payload["search_evidence"] or observed["stop_reason"] is None):
            raise ValueError("increasing-search evidence differs from its measured selection")
    declared = {_key(candidate, stored["parameters"]) for candidate in stored["candidates"]}
    scores = {}
    for cell in payload["evidence"]["candidates"]:
        key = _key(cell["candidate"], stored["parameters"])
        if key not in declared:
            raise ValueError("evidence candidate was not declared")
        if cell["eligible"] is True:
            if cell["repeats"] < stored["repetitions"]:
                raise ValueError("insufficient measured repeats")
            scores[key] = 1 / _seconds(cell["elapsed_s_median"], "elapsed_s_median")
    selected, _ = select_near_peak(scores, ratio=stored["near_peak_ratio"])
    if (_key(payload["selection"], stored["parameters"]) != selected
            or payload.get("performance_tuned") is not True or payload.get("scope") != "measured_candidates_only"):
        raise ValueError("selection is not the measured near-peak candidate")
    result = dict(status="ready", candidate=payload["selection"], identity_sha256=signature,
                selection_path=str(path.resolve()), selection_sha256=hashlib.sha256(raw).hexdigest(),
                performance_tuned=True, scope=payload["scope"], calibration_cost=payload["scan_cost"])
    if search is not None:
        result["search"] = payload["search_evidence"]
    return result


def _save(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, allow_nan=False, indent=2)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class CalibrationScanError(RuntimeError):
    def __init__(self, reason, report):
        super().__init__(reason)
        self.report = report


def calibrate(identity, candidates, *, cache_dir, measure: Callable | None = None,
              budget: ScanBudget | None = None, work_per_sample=1, repetitions=3,
              near_peak_ratio=0.97, clock=time.monotonic, search: IncreasingSearch | None = None,
              stop_on_failure=False):
    """Resolve or run a bounded offline scan; callbacks must honor deadline/work_limit.

    measure(config, repeat_index=..., deadline_monotonic=..., work_limit=...)
    returns Measurement only after its owned resources have stopped borrowing.
    Unknown cleanup should raise; it stops the scan rather than starting a peer.
    No background threads, forced cancellation, model retry or ownership transfer.
    """
    started = clock()
    identity, candidates, search = deepcopy(identity), deepcopy(candidates), deepcopy(search)
    signature = identity_signature(identity)
    policy = _policy(candidates, repetitions, near_peak_ratio, search)
    path = Path(cache_dir) / (_digest(dict(identity_sha256=signature, policy=policy)) + ".json")
    resolved = resolve_calibration(path, identity, policy=policy)
    if resolved["status"] == "ready" or measure is None:
        return dict(resolved, cache_hit=resolved["status"] == "ready", scan_cost=_scan_cost([], 0), resolution_wall_s=clock() - started)
    if not isinstance(budget, ScanBudget):
        raise ValueError("a bounded ScanBudget is required before measuring")
    _positive_int(work_per_sample, "work_per_sample")
    if search is None and len(candidates) * repetitions * work_per_sample > budget.max_work:
        raise ValueError("declared scan exceeds max_work")
    samples, scan_started = [], clock()
    deadline = started + budget.timeout_s

    def failed(reason, stop_reason=None):
        report = dict(status="scan_failed", identity_sha256=signature,
            samples=deepcopy(samples), scan_cost=_scan_cost(samples, clock() - scan_started),
            resolution_wall_s=clock() - started, performance_tuned=False)
        if search is not None:
            report["search"] = dict(_search_evidence(policy, samples),
                                    stop_reason=stop_reason, platform_confirmed=False)
        return CalibrationScanError(reason, report)

    def measure_one(candidate, repeat):
        if clock() >= deadline:
            raise failed("calibration deadline reached", "time_budget")
        try:
            result = measure(deepcopy(candidate), repeat_index=repeat, deadline_monotonic=deadline, work_limit=work_per_sample)
            record = measurement_record(identity, candidate, repeat, result, reserved_work=work_per_sample)
        except BaseException as error:
            samples.append(measurement_record(identity, candidate, repeat,
                Measurement(None, "unknown", False, None), reserved_work=work_per_sample))
            failure = failed("measurement callback did not confirm completion", "unconfirmed_callback")
            if isinstance(error, Exception):
                raise failure from error
            error.calibration_report = failure.report
            raise
        samples.append(record)
        if result.work is not None and result.work > work_per_sample:
            raise failed("measurement exceeded its work_limit", "work_budget")
        if clock() > deadline:
            raise failed("calibration callback exceeded deadline", "time_budget")
        if (search is not None or stop_on_failure) and (result.status != "ok" or result.elapsed_s is None or not result.resources_passed
                                  or result.work is None or result.unknown_work):
            reason = "resource_limit" if not result.resources_passed else "measurement_failed"
            raise failed("probe failed; no further probe or automatic target", reason)

    if search is None:
        for repeat in range(1, repetitions + 1):
            for candidate in candidates:
                measure_one(candidate, repeat)
    else:
        for candidate in policy["candidates"]:
            if (len(samples) + repetitions) * work_per_sample > budget.max_work:
                raise failed("scan work budget cannot cover the next complete point", "work_budget")
            for repeat in range(1, repetitions + 1):
                measure_one(candidate, repeat)
            if _search_evidence(policy, samples)["stop_reason"] is not None:
                break
    try:
        payload = select_calibration(identity, candidates, samples, repetitions=repetitions,
            near_peak_ratio=near_peak_ratio, scan_wall_s=clock() - scan_started, search=search)
    except ValueError as error:
        raise failed("no usable calibration selection") from error
    if clock() > deadline:
        raise failed("calibration deadline reached before publishing", "time_budget")
    if search is not None:
        payload.update(search_evidence=_search_evidence(policy, samples), search_samples=deepcopy(samples))
    try:
        _save(path, payload)
        resolved = resolve_calibration(path, identity, policy=policy)
    except (OSError, ValueError) as error:
        raise failed("calibration cache could not be published") from error
    return dict(resolved, cache_hit=False, scan_cost=payload["scan_cost"], resolution_wall_s=clock() - started)


def run_calibrated(identity, candidates, execute: Callable, *, clock=time.monotonic, **calibration_options):
    """Give the measured configuration to actual execution, with separate timings."""
    started = clock()
    resolution = calibrate(identity, candidates, clock=clock, **calibration_options)
    if resolution["status"] != "ready":
        return dict(status="needs_scan", calibration=resolution, target_wall_s=None, total_wall_s=clock() - started)
    target_started = clock()
    try:
        result = execute(deepcopy(resolution["candidate"]))
    except BaseException as error:
        error.calibration_report = dict(status="execution_failed", calibration=resolution,
            target_wall_s=clock() - target_started, total_wall_s=clock() - started)
        raise
    return dict(status="executed", result=result, calibration=resolution,
                target_wall_s=clock() - target_started, total_wall_s=clock() - started)


def run_query(identity, candidates, execute: Callable | None = None, *, default, cache_dir,
              mode="query", measure: Callable | None = None, budget: ScanBudget | None = None,
              work_per_sample=1, target_work=None, repetitions=3, near_peak_ratio=0.97,
              search: IncreasingSearch | None = None, request_started_monotonic=None,
              clock=time.monotonic):
    """Ordinary query policy, sharing the existing calibration/cache machinery.

    query (default): exact cache or explicit default; never call measure.
    offline: explicit bounded calibration only, without a target query.
    online: explicit scan+target budget; target also receives the deadline and
      work_limit. Callbacks honor these cooperatively; an outer supervisor may
      impose a hard stop. No retry/fallback after uncertain measurement work.

    Target callbacks take execute(configuration); in online mode they take
    execute(configuration, *, deadline_monotonic, work_limit). Full-query time
    remains the signed measurement objective, not inference-only throughput.
    Request wait is reported only with a same-clock caller start anchor; without
    it, outside owner startup/warmup is unobserved, never invented as zero.
    """
    started = clock()
    if mode not in {"query", "offline", "online"}:
        raise ValueError("query policy mode must be query, offline or online")
    identity, candidates, default, search = map(deepcopy, (identity, candidates, default, search))
    identity_signature(identity)
    policy = _policy(candidates, repetitions, near_peak_ratio, search)
    _key(default, policy["parameters"])
    if search is not None and default[search.parameter] > min(
            (*search.capacity_limits.values(), *search.resource_limits.values())):
        raise ValueError("explicit default exceeds the declared search capacity")
    if mode != "offline" and not callable(execute):
        raise TypeError("query execution callback is required")
    if request_started_monotonic is not None:
        _seconds(request_started_monotonic, "request_started_monotonic", zero=True)
        if request_started_monotonic > started:
            raise ValueError("request start must precede this invocation in the same clock")
    common = dict(cache_dir=cache_dir, repetitions=repetitions,
                  near_peak_ratio=near_peak_ratio, search=search, clock=clock)
    report = dict(mode=mode, status="failed", result=None, configuration=None,
                  configuration_source=None, performance_tuned=False, calibration=None,
                  target_wall_s=None, target_work_limit=None,
                  timing_scope="policy invocation through callback return; outside work requires caller start",
                  caller_start_provided=request_started_monotonic is not None)
    try:
        if mode == "offline":
            resolution = calibrate(identity, candidates, measure=measure, budget=budget,
                                    work_per_sample=work_per_sample, **common)
            report.update(status="calibrated" if resolution["status"] == "ready" else "needs_scan",
                          calibration=resolution, configuration=resolution.get("candidate"),
                          configuration_source="offline_calibration" if resolution["status"] == "ready" else None,
                          performance_tuned=resolution["performance_tuned"])
        elif mode == "online":
            if not isinstance(budget, ScanBudget) or not callable(measure):
                raise ValueError("online mode requires explicit measurement and a scan+target budget")
            work = _positive_int(work_per_sample if target_work is None else target_work, "target_work")
            available = budget.max_work - work
            if available < 0:
                raise ValueError("online budget cannot cover its reserved target work")
            deadline = (started if request_started_monotonic is None else request_started_monotonic) + budget.timeout_s
            remaining = deadline - clock()
            if remaining <= 0:
                raise TimeoutError("online request deadline reached before calibration")
            report.update(target_work_limit=work, deadline_monotonic=deadline,
                          online_work_budget=budget.max_work, scan_work_budget=available,
                          work_budget_scope="this invocation's measurement and target work; caller-owned warmup must be separately accounted",
                          deadline_enforcement="cooperative callbacks plus before/after checks; not a hard process timeout")

            def bounded_measure(configuration, **options):
                options["deadline_monotonic"] = min(deadline, options["deadline_monotonic"])
                return measure(configuration, **options)

            def bounded_execute(configuration):
                if clock() >= deadline:
                    raise TimeoutError("online deadline reached before target execution")
                report["configuration"] = deepcopy(configuration)
                result = execute(configuration, deadline_monotonic=deadline, work_limit=work)
                if clock() > deadline:
                    error = TimeoutError("online target returned after its deadline")
                    error.completed_result = result
                    raise error
                return result

            value = run_calibrated(identity, candidates, bounded_execute,
                measure=bounded_measure if available else None,
                budget=ScanBudget(remaining, available) if available else None,
                work_per_sample=work_per_sample, stop_on_failure=True, **common)
            resolution = value["calibration"]
            report.update(value, configuration=resolution.get("candidate"),
                configuration_source=("calibration_cache" if resolution["cache_hit"] else "measured_online")
                    if resolution["status"] == "ready" else None,
                performance_tuned=resolution["performance_tuned"])
            if value["status"] == "needs_scan" and not available:
                report["stop_reason"] = "work_budget"
        else:
            # Cache availability is optional performance state for an ordinary
            # query. Invalid/unreadable receipts cannot authorize their settings.
            try:
                resolution = calibrate(identity, candidates, measure=None, **common)
            except (OSError, ValueError, KeyError, TypeError) as error:
                resolution = dict(status="needs_scan", reason="unusable_selection",
                    lookup_error_type=type(error).__name__, performance_tuned=False,
                    cache_hit=False, scan_cost=_scan_cost([], 0))
            hit = resolution["status"] == "ready"
            configuration = resolution["candidate"] if hit else default
            report.update(calibration=resolution, configuration=deepcopy(configuration),
                          configuration_source="calibration_cache" if hit else "explicit_default",
                          performance_tuned=hit)
            target_started = clock()
            try:
                report.update(result=execute(deepcopy(configuration)), status="executed")
            finally:
                report["target_wall_s"] = clock() - target_started
    except BaseException as error:
        # Preserve original result/usage/error evidence; do not turn a failed or
        # unknown online scan into a second execution on the default profile.
        if hasattr(error, "calibration_report"):
            report["calibration_failure"] = error.calibration_report
        elif hasattr(error, "report"):
            report["calibration_failure"] = error.report
        if hasattr(error, "completed_result"):
            report["completed_result"] = error.completed_result
        error.query_policy_report = report
        raise
    finally:
        ended = clock()
        report["total_wall_s"] = ended - started
        report["request_wait_s"] = (None if request_started_monotonic is None
                                    else ended - request_started_monotonic)
    return report


def run_admission_query(identity, execute, *, initial_window, maximum, cache_dir,
                        reuse=True, clock=time.monotonic, request_started_monotonic=None,
                        options=None):
    """Execute once, optionally reusing a precisely matched admission hint.

    execute(window, *, allow_exploration) returns a completed-query receipt with
    admission_feedback and explicit reuse_eligible (all calls accounted and query
    borrowing released). The model may remain resident. The receipt is a local
    one-pass hint, never a fixed-C complete-query calibration measurement.
    Geometric options additionally pass search_hint. Only finite point progress
    is reused; reference rates are measured anew. Ordinary run_query defaults
    and its offline scan/cache remain unchanged.
    """
    started=clock();signature=identity_signature(identity)
    _positive_int(initial_window,'initial_window');_positive_int(maximum,'maximum')
    if initial_window>maximum or type(reuse) is not bool:
        raise ValueError('initial window must fit capacity; reuse is boolean')
    if request_started_monotonic is not None:
        _seconds(request_started_monotonic,'request_started_monotonic',zero=True)
        if request_started_monotonic>started:
            raise ValueError('request start must precede invocation in the same clock')
    from ..submission_control.adaptive import ResourceFeedbackConfig, _GeometricSearch
    if options is not None and not isinstance(options,ResourceFeedbackConfig):
        raise TypeError('options must be ResourceFeedbackConfig')
    geometric=options is not None and options.search_mode=='geometric'
    strategy='onepass_geometric_v1' if geometric else 'onepass_performance_v3'
    policy=dict(strategy=strategy,initial_window=initial_window,maximum=maximum)
    if options is not None:
        policy['options']=asdict(options)
    path=Path(cache_dir)/('admission-'+_digest(dict(identity=signature,policy=policy))+'.json')
    cached=None;lookup_error=None
    if reuse:
        try:
            if path.exists():
                cached=json.loads(path.read_text())
                if (cached['schema']!='semloom.runtime_admission_hint.v1' or cached['identity_sha256']!=signature
                        or cached['policy']!=policy or type(cached['window']) is not int
                        or not 1<=cached['window']<=maximum or cached['reuse_eligible'] is not True
                        or type(cached['evidence']) is not dict):
                    raise ValueError('incompatible admission hint')
                if geometric:
                    state=cached['evidence']['search_state']
                    if state['origin']!=initial_window or type(cached['evidence']['can_continue']) is not bool:
                        raise ValueError('incompatible geometric origin')
                    _GeometricSearch(initial_window,maximum,options.max_candidate_attempts,state)
        except (OSError,ValueError,KeyError,TypeError) as error:
            cached=None;lookup_error=type(error).__name__
    window=initial_window if cached is None else cached['window']
    permitted=options is None or options.allow_exploration
    continuation=permitted and cached is not None and cached['evidence'].get(
        'can_continue' if geometric else 'exploration_censored') is True
    report=dict(configuration=dict(max_inflight=window),cache_hit=cached is not None,
        configuration_source='explicit_initial' if cached is None else 'exact_admission_hint',
        scan_cost=dict(wall_s=0.,samples=0),lookup_error=lookup_error,
        timing_scope='policy invocation through complete callback; caller anchor covers outside startup/queue',
        historical_evidence=None if cached is None else cached['evidence'])
    report['bounded_continuation']=continuation
    try:
        arguments=dict(allow_exploration=permitted and (cached is None or continuation))
        if geometric:
            arguments['search_hint']=None if cached is None else deepcopy(cached['evidence']['search_state'])
        value=execute(window,**arguments)
        report['result']=value
        feedback=value.get('admission_feedback') or {}
        report['online_exploration_s']=feedback.get('exploration_wall_s')
        report['online_exploration_already_in_total']=True
        if (value.get('status')=='completed' and value.get('reuse_eligible') is True
                and feedback.get('strategy')==strategy):
            pressure=any(row['reason']=='resource_pressure' for row in feedback['history'])
            selected=(feedback['window'] if pressure and not feedback['unconfirmed_trial']
                      else feedback.get('recommended_window'))
            if selected is None and not feedback['changes']:
                selected=window
            terminal_stop=feedback.get('stop_reason') in ('capacity_limit','local_plateau',
                'performance_rollback','waiting_regression','inconclusive_noise')
            new_incomparable=(continuation and feedback.get('invalidated_comparisons',0)>0
                and any(cached['evidence'].get(key)!=feedback.get(key) for key in
                        ('stop_reason','incomplete_reason','invalidated_comparisons')))
            unchanged=(feedback.get('search_state')==cached['evidence'].get('search_state')
                       and feedback.get('can_continue')==cached['evidence'].get('can_continue')) if geometric and cached else False
            if cached is not None and selected==window and (unchanged if geometric else
                    (not continuation or not (terminal_stop or new_incomparable))):
                report['reused_hint_unmodified']=True
            elif type(selected) is int and 1<=selected<=maximum:
                payload=dict(schema='semloom.runtime_admission_hint.v1',identity_sha256=signature,
                    policy=policy,window=selected,reuse_eligible=True,
                    evidence=dict(scope=feedback['recommendation_scope'],accepted_steps=feedback['accepted_steps'],
                        unconfirmed_trial=feedback['unconfirmed_trial'],strategy=feedback['strategy'],
                        stop_reason=feedback.get('stop_reason'),
                        exploration_censored=feedback.get('exploration_censored',False),
                        incomplete_reason=feedback.get('incomplete_reason'),
                        invalidated_comparisons=feedback.get('invalidated_comparisons',0),
                        source='successful one-pass query, not a fixed full-query measurement'))
                if geometric:
                    payload['evidence'].update(search_state=deepcopy(feedback['search_state']),
                        can_continue=feedback['can_continue'])
                try:
                    _save(path,payload)
                    report['saved_hint']=dict(window=selected,path=str(path),scope=payload['evidence']['scope'])
                except OSError as error:
                    report['cache_write_warning']=type(error).__name__
        return report
    except BaseException as error:
        error.admission_query_report=report
        raise
    finally:
        ended=clock();report['total_wall_s']=ended-started
        report['request_wait_s']=None if request_started_monotonic is None else ended-request_started_monotonic
