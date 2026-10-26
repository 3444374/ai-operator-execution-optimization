"""Engine-independent AIMD admission controllers.

Ref: Clipper (NSDI 2017) provides the AIMD pattern. The asymmetric +2/x0.5
defaults follow the CONCUR-style candidate recorded in the project experiment
plan; they are hypotheses to test, not universal constants.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from ..core.models import (
    AdmissionObservation,
    ControlDiagnostics,
    WindowDecision,
)


@dataclass(frozen=True)
class AimdConfig:
    min_window: int = 4
    max_window: int = 16
    additive_increase: int = 2
    multiplicative_decrease: float = 0.5
    congestion_kv_usage: float = 0.85
    low_load_kv_usage: float = 0.50
    low_load_running: int = 64

    def __post_init__(self) -> None:
        if self.min_window <= 0 or self.max_window < self.min_window:
            raise ValueError("window bounds must satisfy 0 < min_window <= max_window")
        if self.additive_increase <= 0:
            raise ValueError("additive_increase must be positive")
        if not 0.0 < self.multiplicative_decrease < 1.0:
            raise ValueError("multiplicative_decrease must be between 0 and 1")
        if not 0.0 <= self.low_load_kv_usage < self.congestion_kv_usage <= 1.0:
            raise ValueError("KV thresholds must define a valid deadband")
        if self.low_load_running <= 0:
            raise ValueError("low_load_running must be positive")


class AimdAdmissionController:
    def __init__(
        self,
        config: AimdConfig | None = None,
        initial_window: int | None = None,
    ):
        self.config = config or AimdConfig()
        initial = self.config.min_window if initial_window is None else initial_window
        if not self.config.min_window <= initial <= self.config.max_window:
            raise ValueError("initial_window must be within configured bounds")
        self.current_window = initial

    def update(self, observation: AdmissionObservation) -> WindowDecision:
        if not observation.fresh:
            return self._hold("stale_observation")
        if not observation.has_service_metrics:
            return self._hold("missing_metrics")
        return self._apply_signals(
            running=float(observation.running),
            waiting_congested=observation.waiting > 0,
            waiting_clear=observation.waiting == 0,
            kv_usage=float(observation.kv_usage),
        )

    def _apply_signals(
        self,
        *,
        running: float,
        waiting_congested: bool,
        waiting_clear: bool,
        kv_usage: float,
        diagnostics: ControlDiagnostics | None = None,
    ) -> WindowDecision:
        if waiting_congested or kv_usage >= self.config.congestion_kv_usage:
            reason = "queue_congestion" if waiting_congested else "kv_congestion"
            updated = max(
                self.config.min_window,
                int(self.current_window * self.config.multiplicative_decrease),
            )
            if updated == self.current_window:
                return self._hold("at_minimum", diagnostics)
            self.current_window = updated
            return WindowDecision(updated, "decrease", reason, diagnostics or ControlDiagnostics())

        low_load = (
            waiting_clear
            and kv_usage <= self.config.low_load_kv_usage
            and running < self.config.low_load_running
        )
        if low_load:
            updated = min(
                self.config.max_window,
                self.current_window + self.config.additive_increase,
            )
            if updated == self.current_window:
                return self._hold("at_maximum", diagnostics)
            self.current_window = updated
            return WindowDecision(updated, "increase", "low_load", diagnostics or ControlDiagnostics())
        return self._hold("deadband", diagnostics)

    def _hold(
        self,
        reason: str,
        diagnostics: ControlDiagnostics | None = None,
    ) -> WindowDecision:
        return WindowDecision(
            self.current_window,
            "hold",
            reason,
            diagnostics or ControlDiagnostics(),
        )


class EwmaAimdAdmissionController(AimdAdmissionController):
    def __init__(
        self,
        config: AimdConfig | None = None,
        initial_window: int | None = None,
        *,
        alpha: float = 0.3,
        smoothed_waiting_threshold: float = 0.5,
    ):
        super().__init__(config, initial_window)
        if not 0.0 < alpha <= 1.0:
            raise ValueError("alpha must be between 0 and 1")
        if smoothed_waiting_threshold <= 0:
            raise ValueError("smoothed_waiting_threshold must be positive")
        self.alpha = alpha
        self.smoothed_waiting_threshold = smoothed_waiting_threshold
        self._running: float | None = None
        self._waiting: float | None = None
        self._kv_usage: float | None = None

    def update(self, observation: AdmissionObservation) -> WindowDecision:
        if not observation.fresh:
            return self._hold("stale_observation", self._diagnostics())
        if not observation.has_service_metrics:
            return self._hold("missing_metrics", self._diagnostics())

        self._running = self._smooth(self._running, float(observation.running))
        self._waiting = self._smooth(self._waiting, float(observation.waiting))
        self._kv_usage = self._smooth(self._kv_usage, float(observation.kv_usage))
        diagnostics = self._diagnostics()
        return self._apply_signals(
            running=self._running,
            waiting_congested=self._waiting >= self.smoothed_waiting_threshold,
            waiting_clear=self._waiting < self.smoothed_waiting_threshold,
            kv_usage=self._kv_usage,
            diagnostics=diagnostics,
        )

    def _smooth(self, previous: float | None, sample: float) -> float:
        if previous is None:
            return sample
        return self.alpha * sample + (1.0 - self.alpha) * previous

    def _diagnostics(self) -> ControlDiagnostics:
        return ControlDiagnostics(
            smoothed_running=self._running,
            smoothed_waiting=self._waiting,
            smoothed_kv_usage=self._kv_usage,
        )


@dataclass(frozen=True)
class HolAgeAimdConfig:
    min_window: int = 4
    max_window: int = 16
    additive_increase: int = 2
    multiplicative_decrease: float = 0.5
    congestion_hol_age_s: float = 2.0
    low_load_hol_age_s: float = 0.5

    def __post_init__(self) -> None:
        if self.min_window <= 0 or self.max_window < self.min_window:
            raise ValueError("window bounds must satisfy 0 < min_window <= max_window")
        if self.additive_increase <= 0:
            raise ValueError("additive_increase must be positive")
        if not 0.0 < self.multiplicative_decrease < 1.0:
            raise ValueError("multiplicative_decrease must be between 0 and 1")
        if (
            not math.isfinite(self.low_load_hol_age_s)
            or not math.isfinite(self.congestion_hol_age_s)
            or not 0.0
            <= self.low_load_hol_age_s
            < self.congestion_hol_age_s
        ):
            raise ValueError("HOL-age thresholds must define a valid deadband")


class HolAgeAimdAdmissionController:
    """AIMD keyed on Ray-side head-of-line age rather than vLLM queue depth.

    The service-metric controllers (AIMD/EWMA/PID) read
    ``vllm:num_requests_waiting``, which stays at 0 when requests queue on the
    Ray side, so they never decrease and saturate to ``max_window``. Head-of-line
    age (the age of the oldest in-flight submission) is observable directly
    from the scheduler. It includes normal model service time and therefore is
    not a pure Ray queue-delay signal. This controller is a diagnostic
    candidate whose thresholds must be calibrated against normal service
    latency; it does not by itself establish a production congestion signal.
    """

    def __init__(
        self,
        config: HolAgeAimdConfig | None = None,
        initial_window: int | None = None,
    ):
        self.config = config or HolAgeAimdConfig()
        initial = self.config.min_window if initial_window is None else initial_window
        if not self.config.min_window <= initial <= self.config.max_window:
            raise ValueError("initial_window must be within configured bounds")
        self.current_window = initial

    def update(self, observation: AdmissionObservation) -> WindowDecision:
        if not observation.fresh:
            return self._hold("stale_observation")
        hol_age_s = observation.hol_age_s
        if hol_age_s is None:
            return self._hold("missing_hol_age")
        if hol_age_s >= self.config.congestion_hol_age_s:
            updated = max(
                self.config.min_window,
                int(self.current_window * self.config.multiplicative_decrease),
            )
            if updated == self.current_window:
                return self._hold("at_minimum")
            self.current_window = updated
            return WindowDecision(updated, "decrease", "hol_age_congestion")
        if hol_age_s <= self.config.low_load_hol_age_s:
            updated = min(
                self.config.max_window,
                self.current_window + self.config.additive_increase,
            )
            if updated == self.current_window:
                return self._hold("at_maximum")
            self.current_window = updated
            return WindowDecision(updated, "increase", "hol_age_low_load")
        return self._hold("deadband")

    def _hold(self, reason: str) -> WindowDecision:
        return WindowDecision(self.current_window, "hold", reason, ControlDiagnostics())


@dataclass(frozen=True)
class ResourceFeedbackConfig:
    """Optional one-pass admission policy; values are candidates, not tuned defaults."""
    poll_interval_s: float = 0.25
    window_s: float = 1.0
    min_samples: int = 16
    min_remaining: int = 32
    exploration_s: float = 10.0
    max_changes: int = 8
    allow_exploration: bool = False
    min_gain: float = 0.03
    memory_floor_bytes: int = 0
    search_mode: str = "local"
    work_tolerance: float = 0.03
    max_candidate_attempts: int = 2

    def __post_init__(self):
        for name in ("poll_interval_s", "window_s", "exploration_s"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(name + " must be finite and positive")
        for name in ("min_samples", "min_remaining", "max_changes"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(name + " must be a positive integer")
        if type(self.allow_exploration) is not bool:
            raise ValueError("allow_exploration must be boolean")
        if not 0 <= self.min_gain < 1:
            raise ValueError("min_gain must be between zero and one")
        if type(self.memory_floor_bytes) is not int or self.memory_floor_bytes < 0:
            raise ValueError("memory_floor_bytes must be non-negative")
        if self.search_mode not in ("local", "geometric"):
            raise ValueError("search_mode must be local or geometric")
        if not 0 <= self.work_tolerance < 1:
            raise ValueError("work_tolerance must be between zero and one")
        if type(self.max_candidate_attempts) is not int or not 1 <= self.max_candidate_attempts <= 4:
            raise ValueError("max_candidate_attempts must be between one and four")


class _GeometricSearch:
    """Finite candidate progress, not an executor or a store of reusable rates."""
    resolved = {"gain", "no_gain", "regression", "pressure"}
    outcomes = resolved | {"pending", "noise", "incomparable"}

    def __init__(self, initial, maximum, attempts, hint=None):
        self.maximum, self.limit = maximum, attempts
        self.origin = initial if hint is None else hint["origin"]
        if type(self.origin) is not int or not 1 <= self.origin <= maximum:
            raise ValueError("invalid geometric origin")
        self.points = {}
        value = self.origin
        while value < maximum:
            value = min(maximum, value * 2)
            self.points[value] = dict(attempts=0, outcome="pending")
        self.ceiling, self.here = maximum, set()
        if hint is not None:
            if (hint["schema"] != "semloom.admission_search.v1"
                    or hint["maximum"] != maximum or hint["attempt_limit"] != attempts):
                raise ValueError("incompatible geometric search")
            ceiling, rows = hint["ceiling"], hint["points"]
            if type(ceiling) is not int or not 1 <= ceiling <= maximum or len(rows) != len(self.points):
                raise ValueError("invalid geometric progress")
            restored = {}
            for row in rows:
                key, count, outcome = row["window"], row["attempts"], row["outcome"]
                if (type(key) is not int or key not in self.points or key in restored
                        or type(count) is not int or not 0 <= count <= attempts or outcome not in self.outcomes
                        or count == 0 and outcome != "pending"):
                    raise ValueError("invalid geometric point")
                restored[key] = dict(attempts=count, outcome=outcome)
            self.points, self.ceiling = restored, ceiling

    def available(self):
        return [key for key, row in self.points.items() if key <= self.ceiling
                and row["outcome"] not in self.resolved and row["attempts"] < self.limit]

    def next(self):
        values = [key for key in self.available() if key not in self.here]
        # Cover unvisited points before retrying unresolved ones on a later query.
        return min(values, key=lambda key: (self.points[key]["attempts"] > 0, key)) if values else None

    def begin(self, window):
        self.points[window]["attempts"] += 1
        self.points[window]["outcome"] = "pending"
        self.here.add(window)

    def finish(self, window, outcome):
        if window in self.here:
            self.points[window]["outcome"] = outcome

    def stop_reason(self):
        if self.available():
            return "continuation_required"
        if any(row["outcome"] not in self.resolved for key, row in self.points.items() if key <= self.ceiling):
            return "search_exhausted"
        return "resource_limited" if self.ceiling < self.maximum else "sampled_range_complete"

    def snapshot(self):
        return dict(schema="semloom.admission_search.v1", origin=self.origin, maximum=self.maximum,
            attempt_limit=self.limit, ceiling=self.ceiling,
            points=[dict(window=key, **row) for key, row in self.points.items()])


class ResourceFeedbackController(AimdAdmissionController):
    """One-pass performance trials on the existing admission window.

    Windows measure delivered work, not isolated model throughput. Observed
    min/max bands are conservative local evidence, never confidence intervals.
    """

    def __init__(self, maximum, initial_window, options, *, clock=None):
        import time
        super().__init__(AimdConfig(min_window=1, max_window=maximum,
            low_load_running=maximum), initial_window)
        self.options, self.clock = options, clock or time.monotonic
        self.reset(initial_window, 0)

    def reset(self, initial_window, total_items, *, search_hint=None):
        self.current_window = initial_window
        self.started = self._window_at = self.clock()
        self._anchor = self._reference = self._probe_at = None
        self._confirmed_window = initial_window
        self._preemptions = None
        self._needs_settle = False
        self._pressure_windows = self._cooldown = 0
        self._changes = self._decisions = self._accepted_steps = 0
        self._growth_stopped = False
        self._stop_reason = None
        self._discard_first = True
        self._windows = []
        self.total_items = total_items
        self.history, self.measurements, self.evaluations = [], [], []
        self.reasons = {}
        self.control_seconds = self.exploration_wall_s = self.estimated_trial_loss_s = 0.0
        self._probe_loss_base = 0.0
        self._search = (_GeometricSearch(initial_window, self.config.max_window,
            self.options.max_candidate_attempts, search_hint) if self.options.search_mode == "geometric" else None)
        if search_hint is not None and self._search is None:
            raise ValueError("search_hint requires geometric mode")

    def _mark(self, o):
        self._anchor = (o.observed_at_s, o.completed_items_total,
                        o.completed_input_work_total, o.completed_output_work_total)

    def _end_probe(self, now):
        if self._probe_at is not None:
            self.exploration_wall_s += max(0.0, now - self._probe_at)
            self._probe_at = None

    def _change(self, window, reason, o):
        previous, self.current_window = self.current_window, window
        self._changes += 1
        self._needs_settle, self._cooldown = True, 1
        self._windows, self._anchor = [], None
        self.history = (self.history + [dict(at_s=o.observed_at_s-self.started,
            before=previous, window=window, reason=reason, inflight=o.inflight,
            kv_usage=o.kv_usage, waiting=o.waiting)])[-64:]
        return WindowDecision(window, "increase" if window > previous else "decrease", reason)

    def _stop(self, reason, o, *, rollback=False):
        if not self._growth_stopped:
            self._stop_reason = reason
        self._growth_stopped = True
        self._end_probe(o.observed_at_s)
        if rollback and self._reference is not None and self.current_window != self._reference["window"]:
            return self._change(self._reference["window"], reason, o)
        return self._hold(reason)

    def sampling_interval_s(self, pending_items):
        has_candidate = bool(self._search.available()) if self._search is not None else self.current_window < self.config.max_window
        exploring = (self.options.allow_exploration and not self._growth_stopped
            and (self._probe_at is not None or has_candidate)
            and pending_items is not None and pending_items > self.options.min_remaining
            and self.clock()-self.started < self.options.exploration_s
            and self._changes < self.options.max_changes)
        return self.options.poll_interval_s if exploring else max(self.options.poll_interval_s,self.options.window_s)

    def update(self, observation):
        start = self.clock()
        try:
            decision = self._update(observation)
            self.reasons[decision.reason] = self.reasons.get(decision.reason, 0)+1
            return decision
        finally:
            self._decisions += 1
            self.control_seconds += max(0.0, self.clock()-start)

    def _next_trial(self, o):
        point = self._reference
        higher = (self._search.next() if self._search is not None else
                  min(self.config.max_window,self.current_window+self.config.additive_increase))
        if higher is None:
            return self._stop(self._search.stop_reason(),o)
        if higher == self.current_window:
            return self._stop("capacity_limit",o)
        # Reserve a possible rollback; resource-pressure reductions are independent.
        if self._changes+1 >= self.options.max_changes:
            return self._stop("exploration_budget",o)
        remaining = max(0,self.total_items-o.completed_items_total)
        remaining_s = remaining/point["rate"]
        possible_saving = remaining_s*max(0.,1-self.current_window/higher)
        ideal_rate = point["rate"]*higher/self.current_window
        trial_s = ((3 if self._search is not None else 2)*max(self.options.window_s,self.options.min_samples/ideal_rate)
                   + max(self.options.window_s,o.hol_age_s or 0)+self.options.window_s)
        # Measurement consumes wall time, but continues useful original work.
        # Reserve that time, then value only the work left after reference-rate
        # progress. Controller CPU is observed; other future losses are unknown.
        after_trial = max(0.,remaining-point["rate"]*trial_s)
        later_saving = after_trial/point["rate"]*max(0.,1-self.current_window/higher)
        control_estimate = (self.control_seconds/max(1,self._decisions)
                            * math.ceil(trial_s/self.options.poll_interval_s))
        self.evaluations = (self.evaluations + [dict(kind="probe_cost_estimate",
            window=self.current_window, candidate=higher, remaining_items=remaining,
            estimated_remaining_s=remaining_s, optimistic_saved_s=possible_saving,
            trial_allowance_s=trial_s, remaining_after_reference_progress=after_trial,
            optimistic_saved_after_trial_s=later_saving,incremental_controller_estimate_s=control_estimate,
            unpredicted_trial_costs="transition/IPC/observation losses; full query still measures them",
            assumption="reference-rate useful progress during trial, then ideal scaling; not measured benefit")])[-64:]
        if self.options.exploration_s-(o.observed_at_s-self.started) < trial_s:
            return self._stop("exploration_budget",o)
        if after_trial <= self.options.min_remaining or (higher > self.current_window and later_saving <= control_estimate):
            return self._stop("insufficient_remaining_benefit",o)
        self._probe_loss_base = self.estimated_trial_loss_s
        self._probe_at = o.observed_at_s
        if self._search is not None:
            self._search.begin(higher)
        return self._change(higher,"performance_probe",o)

    def _point(self):
        import statistics
        rows = self._windows
        return dict(window=self.current_window, rate=(sum(r[4] for r in rows)/sum(r[5] for r in rows)
                if self._search is not None else statistics.median(r[0] for r in rows)),
            rate_min=min(r[0] for r in rows),rate_max=max(r[0] for r in rows),
            input_min=min(r[1] for r in rows),input_max=max(r[1] for r in rows),
            output_min=min(r[2] for r in rows),output_max=max(r[2] for r in rows),
            hol_max=max((r[3] for r in rows if r[3] is not None),default=None),samples=len(rows),
            completed=sum(r[4] for r in rows),elapsed_s=sum(r[5] for r in rows))

    def _invalidate_work_comparison(self, point, o):
        previous = self._reference
        self.evaluations = (self.evaluations + [dict(kind="incomparable_work_mix",
            reference=dict(previous),candidate=dict(point),comparison_valid=False)])[-64:]
        self.estimated_trial_loss_s = self._probe_loss_base
        if self._search is not None:
            self._search.finish(point["window"],"incomparable")
        self._end_probe(o.observed_at_s)
        self._confirmed_window = previous["window"]
        self._reference, self._anchor, self._windows = None, None, []
        # Keep original elapsed/change budgets. Fresh work at the safe window
        # must establish a new reference before another trial can start.
        return (self._change(self._confirmed_window,"work_mix_changed",o)
                if self.current_window != self._confirmed_window else self._hold("work_mix_changed"))

    def _compare(self, point, o):
        previous = self._reference
        margin = self.options.min_gain
        work_margin = self.options.work_tolerance if self._search is not None else margin
        for name in ("input","output"):
            if (point[name+"_min"] < previous[name+"_min"]*(1-work_margin)
                    or point[name+"_max"] > previous[name+"_max"]*(1+work_margin)):
                return self._invalidate_work_comparison(point,o)
        lower = point["rate_min"]/previous["rate_max"]-1
        upper = point["rate_max"]/previous["rate_min"]-1
        saved = max(0,self.total_items-o.completed_items_total)*(1/previous["rate"]-1/point["rate"])
        self.evaluations = (self.evaluations + [dict(kind="observed_gain_band",
            window=point["window"],reference_window=previous["window"],
            conservative_gain=lower,optimistic_gain=upper,estimated_remaining_saved_s=saved,
            estimated_stable_trial_loss_s=self.estimated_trial_loss_s,
            rule="observed windows, not a full-query counterfactual")])[-64:]
        if lower > margin:
            if self._search is not None:
                self._search.finish(point["window"],"gain")
            self._accepted_steps += 1
            self._reference = point
            self._confirmed_window = point["window"]
            self._end_probe(o.observed_at_s)
            # Already incurred costs are sunk: keep a faster verified window,
            # even when too little work remains to recover earlier losses.
            return self._next_trial(o)
        if upper < -margin:
            if self._search is not None:
                return self._finish_geometric_trial(point,o,"regression")
            return self._stop("performance_rollback",o,rollback=True)
        if (point["hol_max"] is not None and previous["hol_max"] is not None
                and point["hol_max"] > previous["hol_max"]*(1+margin) and upper <= margin):
            if self._search is not None:
                return self._finish_geometric_trial(point,o,"regression")
            return self._stop("waiting_regression",o,rollback=True)
        if upper <= margin:
            if self._search is not None:
                return self._finish_geometric_trial(point,o,"no_gain")
            return self._stop("local_plateau",o,rollback=True)
        if len(self._windows) >= 4:
            if self._search is not None:
                return self._finish_geometric_trial(point,o,"noise")
            return self._stop("inconclusive_noise",o,rollback=True)
        return self._hold("confirming_performance")

    def _finish_geometric_trial(self, point, o, outcome):
        self._search.finish(point["window"],outcome)
        self._end_probe(o.observed_at_s)
        self._reference, self._anchor, self._windows = None, None, []
        # Rebuild the confirmed reference from this query before the next point.
        return self._change(self._confirmed_window,"trial_"+outcome,o)

    def _update(self, o):
        if not o.fresh or not o.has_service_metrics:
            if self._probe_at is not None and o.observed_at_s-self.started >= self.options.exploration_s:
                return self._stop("exploration_budget",o,rollback=True)
            return self._hold("missing_or_stale_feedback")
        now = o.observed_at_s
        if self._preemptions is None:
            self._preemptions = o.preemptions_total
        if now-self._window_at < self.options.window_s:
            return self._hold("collecting_window")
        self._window_at = now
        preempted = (o.preemptions_total is not None and self._preemptions is not None
                     and o.preemptions_total > self._preemptions)
        self._preemptions = o.preemptions_total
        elapsed = 0 if self._anchor is None else now-self._anchor[0]
        count = o.completed_items_total
        rate = (None if not elapsed or count is None or self._anchor[1] is None
                else (count-self._anchor[1])/elapsed)
        pressured = o.memory_pressure is True or preempted or o.kv_usage >= self.config.congestion_kv_usage
        if o.waiting and self._reference is not None and rate is not None and rate < self._reference["rate"]:
            pressured = True
        self._pressure_windows = self._pressure_windows+1 if pressured else 0
        if pressured:
            if self._pressure_windows < 2 and not preempted and o.memory_pressure is not True:
                return self._hold("confirming_pressure")
            self._end_probe(now)
            self._reference = None
            lower = max(1,int(self.current_window*self.config.multiplicative_decrease))
            self._confirmed_window = lower
            if self._search is not None:
                self._search.finish(self.current_window,"pressure")
                self._search.ceiling = min(self._search.ceiling,max(1,self.current_window-1))
                self._growth_stopped, self._stop_reason = True, "resource_pressure"
            return self._change(lower,"resource_pressure",o) if lower < self.current_window else self._hold("at_minimum")
        if (self.options.allow_exploration and not self._growth_stopped and (now-self.started >= self.options.exploration_s
                or self._changes >= self.options.max_changes)):
            return self._stop("exploration_budget",o,rollback=True)
        if (self._search is not None and self.options.allow_exploration and
                (self.total_items < self.options.min_remaining or
                 o.pending_items is not None and o.pending_items <= self.options.min_remaining)):
            return self._stop("short_tail_or_fixed",o,rollback=True)
        if o.transition_pending or self._needs_settle:
            self._mark(o)
            self._needs_settle = bool(o.transition_pending)
            return self._hold("transition" if o.transition_pending else "transition_settled")
        if self._cooldown:
            self._cooldown -= 1
            self._mark(o)
            return self._hold("cooldown")
        if not self.options.allow_exploration:
            return self._hold("short_tail_or_fixed")
        if self.total_items < self.options.min_remaining or o.pending_items is None or o.pending_items <= self.options.min_remaining:
            return self._stop("short_tail_or_fixed",o,rollback=True)
        if self._growth_stopped:
            return self._hold(self._stop_reason or "exploration_stopped")
        if self.options.memory_floor_bytes and o.memory_pressure is None:
            return self._hold("missing_memory_feedback")
        if o.consumer_backpressure:
            self._windows=[];self._mark(o)
            return self._hold("consumer_backpressure")
        if self._search is None and self._reference is None and self.current_window == self.config.max_window:
            return self._stop("capacity_limit",o)
        work = o.completed_input_work_total,o.completed_output_work_total
        if count is None or any(v is None for v in work):
            self._anchor=None;self._windows=[]
            return self._hold("missing_work_feedback")
        if self._anchor is None or any(v is None for v in self._anchor[1:]):
            self._mark(o)
            return self._hold("priming_measurement")
        delta = count-self._anchor[1]
        if delta < 0 or any(a < b for a,b in zip(work,self._anchor[2:])):
            return self._stop("nonmonotonic_work",o,rollback=True)
        if delta < self.options.min_samples:
            return self._hold("insufficient_progress")
        row = (delta/elapsed,(work[0]-self._anchor[2])/delta,
               (work[1]-self._anchor[3])/delta,o.hol_age_s,delta,elapsed)
        self._mark(o)
        if self._discard_first:
            self._discard_first=False
            return self._hold("initial_progress_window")
        if o.waiting or o.kv_usage > self.config.low_load_kv_usage:
            self._windows=[]
            return self._hold("resource_deadband")
        self._windows.append(row)
        if self._probe_at is not None and self._reference is not None:
            self.estimated_trial_loss_s += max(0.0,elapsed-delta/self._reference["rate"])
        if len(self._windows) < (3 if self._search is not None else 2):
            return self._hold("confirming_performance")
        point = self._point()
        self.measurements=(self.measurements+[point])[-64:]
        if self._reference is None:
            work_margin = self.options.work_tolerance if self._search is not None else self.options.min_gain
            if any(point[name+"_max"] > point[name+"_min"]*(1+work_margin)
                   for name in ("input","output")):
                self._windows=self._windows[-1:]
                return self._hold("unstable_reference_work")
            self._reference=point
            self._confirmed_window=point["window"]
            return self._next_trial(o)
        return self._compare(point,o)

    def report(self):
        short = self.total_items <= self.options.min_remaining
        stop = self._stop_reason or ("short_query" if short else "capacity_limit"
            if self._search is None and self.current_window == self.config.max_window and self._probe_at is None else None)
        censored = self.options.allow_exploration and not short and (
            self._probe_at is not None or stop in (None,"exploration_budget","insufficient_remaining_benefit")
            or stop == "short_tail_or_fixed" and self._changes > 0)
        invalidated=sum(row["kind"] == "incomparable_work_mix" for row in self.evaluations)
        search = self._search
        if search is not None:
            censored = (not short and (self.options.allow_exploration or any(
                row["attempts"] for row in search.points.values())) and
                (bool(search.available()) or search.stop_reason() != "sampled_range_complete"))
        return dict(kind="resource_feedback",strategy="onepass_geometric_v1" if search is not None
            else "onepass_performance_v3",window=self.current_window,
            capacity=self.config.max_window,total_items=self.total_items,changes=self._changes,
            decision_calls=self._decisions,control_seconds=self.control_seconds,reasons=dict(self.reasons),
            history=list(self.history),measurements=list(self.measurements),evaluations=list(self.evaluations),
            accepted_steps=self._accepted_steps,
            recommended_window=self._confirmed_window,
            unconfirmed_trial=self._probe_at is not None,
            stop_reason=stop,exploration_censored=bool(censored),
            incomplete_reason="work_mix_changed" if censored and invalidated else stop if censored else None,
            search_state=None if search is None else search.snapshot(),
            can_continue=self.options.allow_exploration and not short and bool(search.available())
                if search is not None else bool(censored),
            invalidated_comparisons=invalidated,
            exploration_wall_s=self.exploration_wall_s+(0 if self._probe_at is None else max(0,self.clock()-self._probe_at)),
            exploration_already_in_query_wall=True,estimated_stable_trial_loss_s=self.estimated_trial_loss_s,
            recommendation_scope="same-query stable delivery windows; not a measured fixed full-query cache entry",
            learned_global_optimum=False)
