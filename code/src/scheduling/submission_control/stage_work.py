"""Engine-independent stage-work and demand-gated online feedback candidates.

The legacy work controller uses its explicit calibrated action set. The supply
controller reuses measured-work trials without task calibration or hardware
utilization as evidence of spare capacity. Neither establishes resource benefit.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field, replace

from ...planning.work import RuntimeStateSnapshot
from ...planning.work import StageWork
from .adaptive import ResourceFeedbackConfig, ResourceFeedbackController


@dataclass(frozen=True)
class WorkCreditDecision:
    work_limit: int
    action: str
    reason: str


class BoundedStageWorkController:
    """Adjust one stage's work credit by at most one calibrated step."""

    def __init__(
        self,
        candidates: tuple[int, ...],
        *,
        fallback_work: int,
        controlled_stage: str,
        upstream_stage: str,
        low_active_fraction: float = 0.8,
        congestion_queue_fraction: float = 0.1,
        congestion_age_s: float = 1.0,
    ) -> None:
        ordered = tuple(sorted(set(candidates)))
        if not ordered or any(item <= 0 for item in ordered):
            raise ValueError("candidates must contain positive work limits")
        if fallback_work not in ordered:
            raise ValueError("fallback_work must be one of candidates")
        if not controlled_stage or not upstream_stage:
            raise ValueError("stage names must be non-empty")
        if not 0 < low_active_fraction < 1:
            raise ValueError("low_active_fraction must be in (0, 1)")
        if not 0 <= congestion_queue_fraction < 1:
            raise ValueError("congestion_queue_fraction must be in [0, 1)")
        if not math.isfinite(congestion_age_s) or congestion_age_s <= 0:
            raise ValueError("congestion_age_s must be finite and positive")
        self.candidates = ordered
        self.fallback_work = fallback_work
        self.controlled_stage = controlled_stage
        self.upstream_stage = upstream_stage
        self.low_active_fraction = low_active_fraction
        self.congestion_queue_fraction = congestion_queue_fraction
        self.congestion_age_s = congestion_age_s
        self._index = ordered.index(fallback_work)

    @property
    def current_work(self) -> int:
        return self.candidates[self._index]

    def select(
        self,
        snapshot: RuntimeStateSnapshot,
        *,
        now_s: float,
        max_age_s: float,
        calibration_signature: str,
    ) -> WorkCreditDecision:
        if (
            snapshot.calibration_signature != calibration_signature
            or not snapshot.is_fresh(now_s=now_s, max_age_s=max_age_s)
        ):
            return self._fallback("stale_or_signature_mismatch")
        controlled = snapshot.for_stage(self.controlled_stage)
        upstream = snapshot.for_stage(self.upstream_stage)
        if controlled is None or upstream is None or controlled.capacity_work is None:
            return self._fallback("missing_stage_or_capacity")

        congested = (
            controlled.queued_work
            >= controlled.capacity_work * self.congestion_queue_fraction
            and controlled.oldest_queue_age_s >= self.congestion_age_s
        )
        if congested:
            return self._move(-1, "service_queue_congestion")

        underfilled = (
            upstream.queued_work > 0
            and controlled.queued_work == 0
            and controlled.active_work
            < controlled.capacity_work * self.low_active_fraction
        )
        if underfilled:
            return self._move(1, "upstream_work_available")
        return WorkCreditDecision(self.current_work, "hold", "deadband")

    def _fallback(self, reason: str) -> WorkCreditDecision:
        self._index = self.candidates.index(self.fallback_work)
        return WorkCreditDecision(self.current_work, "fallback", reason)

    def _move(self, delta: int, reason: str) -> WorkCreditDecision:
        updated = min(max(self._index + delta, 0), len(self.candidates) - 1)
        action = "hold" if updated == self._index else (
            "increase" if delta > 0 else "decrease"
        )
        self._index = updated
        return WorkCreditDecision(self.current_work, action, reason)


@dataclass(frozen=True)
class SupplyFeedbackConfig:
    """Explicit operational ceiling; no task reference or historical optimum."""
    max_batches: int
    options: ResourceFeedbackConfig = field(default_factory=lambda: ResourceFeedbackConfig(
        allow_exploration=True, search_mode="geometric", work_basis="computed",
        work_comparison="local_pairs", max_candidate_attempts=None))

    def __post_init__(self):
        if type(self.max_batches) is not int or self.max_batches < 1:
            raise ValueError("max_batches must be a positive operational ceiling")
        if type(self.options) is not ResourceFeedbackConfig:
            raise TypeError("options must be ResourceFeedbackConfig")
        if self.options.poll_interval_s > self.options.window_s:
            raise ValueError("poll interval must fit the measurement window")

    @classmethod
    def from_dict(cls, value):
        if (type(value) is not dict or 'max_batches' not in value
                or set(value)-{'max_batches', 'options'}):
            raise ValueError('batch_feedback requires max_batches and optional options')
        config = cls(value['max_batches'])
        if 'options' not in value:
            return config
        if type(value['options']) is not dict:
            raise ValueError('batch_feedback options must be an object')
        try:
            options = replace(config.options, **value['options'])
        except TypeError as error:
            raise ValueError('unsupported batch_feedback option') from error
        return cls(config.max_batches, options)


@dataclass(frozen=True)
class SupplyState:
    """Control-thread demand at the end of the same native measurement window.

    Counts describe actual prepared/admitted work, not potential future model
    calls. StageWork retains declared units, not calibrated hardware cost.
    """
    observed_at_s: float
    ready_items: int
    prepared_total: int
    submitted_total: int
    active_batches: int
    upstream_pending: bool
    exhausted: bool
    capacity_blocked: str | None = None
    ready_work: tuple[StageWork, ...] = ()
    local_capacity: dict = field(default_factory=dict)
    source_exhausted: bool = False

    def __post_init__(self):
        if not math.isfinite(self.observed_at_s) or self.observed_at_s < 0:
            raise ValueError("supply timestamp must be finite and non-negative")
        for name in ("ready_items", "prepared_total", "submitted_total", "active_batches"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(name + " must be a non-negative integer")
        if any(type(v) is not bool for v in (self.upstream_pending, self.exhausted, self.source_exhausted)):
            raise TypeError("supply state flags must be boolean")


class SupplyFeedbackController(ResourceFeedbackController):
    """Gate the existing work-aware trials on actual demand and capacity.

    The controlled window is application batches, never member/request C.
    A bounded online probe is not a claim of idle hardware or a global optimum.
    """
    def __init__(self, config, *, clock=None):
        self.supply_config = config
        super().__init__(config.max_batches, 1, config.options, clock=clock)
        self.config = replace(self.config, additive_increase=1)

    def reset(self, initial_window, total_items, *, search_hint=None):
        if initial_window != 1 or search_hint is not None:
            raise ValueError('supply feedback starts at one without a historical search hint')
        super().reset(1, total_items)
        self.supply_history = []
        self.supply_changes = []
        self.supply_states = {}
        self._supply_anchor = None
        self._delivery_anchor = None
        self._last_native_at = None
        self._pending_feedback_gap = False
        self.feedback_gaps = 0
        self._current_supply = self._measurement_supply = self._measurement_delivery = None

    def _mark(self, observation):
        super()._mark(observation)
        self._measurement_supply, self._measurement_delivery = self._current_supply, observation

    def _invalidate_feedback_gap(self, observation):
        self.feedback_gaps += 1
        self.evaluations = (self.evaluations + [dict(kind='feedback_gap',
            at_s=observation.observed_at_s-self.started, window=self.current_window,
            comparison_valid=False, rule='remeasure the safe window after fresh feedback resumes')])[-64:]
        if self._probe_at is not None and self._search is not None:
            candidate = (self._probe_candidate['window'] if self._probe_candidate is not None
                         else self.current_window)
            self._search.interrupt(candidate)
            self.evaluations[-1].update(interrupted_candidate=candidate,
                attempts=self._search.points[candidate]['attempts'],
                retry_eligible=candidate in self._search.available())
        self._end_probe(observation.observed_at_s)
        self._reference, self._anchor, self._windows = None, None, []
        self._cached_context = None
        if self._search is not None:
            self._search.verified_window = None
        self._discard_first = True
        self._window_at = observation.observed_at_s
        # Keep elapsed time, attempt/change budgets and real stop conditions.
        # The gap invalidates evidence, not the possibility of future evidence.
        if self.current_window != self._confirmed_window:
            return self._change(self._confirmed_window, 'feedback_gap', observation)
        self._mark(observation)
        return self._hold('feedback_gap')

    def update_supply(self, observation, supply):
        o = observation
        self._current_supply = supply
        state, decision = "measuring", None
        rates = None
        native_valid = (o.fresh and o.has_service_metrics and o.sample_age_s is not None
                        and o.sample_age_s <= self.options.window_s)
        native_at = None if not native_valid else o.observed_at_s-o.sample_age_s
        feedback_gap = (native_valid and (self._pending_feedback_gap
                        or self._last_native_at is not None
                        and not 0 <= native_at-self._last_native_at <= self.options.window_s))
        if native_valid:
            self._last_native_at = native_at
            self._pending_feedback_gap = feedback_gap
        previous = self._measurement_supply if self._anchor is not None else self._supply_anchor
        delivery = self._measurement_delivery if self._anchor is not None else self._delivery_anchor
        if previous is not None:
            elapsed = supply.observed_at_s - previous.observed_at_s
            if elapsed > 0 and (supply.prepared_total >= previous.prepared_total
                                and supply.submitted_total >= previous.submitted_total):
                rates = dict(window_start_s=previous.observed_at_s-self.started,
                             window_end_s=supply.observed_at_s-self.started,
                             native_point_in_window=(native_at is not None
                                and previous.observed_at_s <= native_at <= supply.observed_at_s),
                             prepared_items_s=(supply.prepared_total-previous.prepared_total)/elapsed,
                             submitted_items_s=(supply.submitted_total-previous.submitted_total)/elapsed)
                for field_name in ('completed_items_total', 'completed_input_work_total',
                        'completed_output_work_total', 'completed_computed_input_work_total',
                        'completed_cached_input_work_total'):
                    a, b = getattr(delivery, field_name), getattr(o, field_name)
                    rates[field_name.removesuffix('_total')+'_s'] = ((b-a)/elapsed
                        if a is not None and b is not None and b >= a else None)
        # Complete EOF can be proven locally even when the engine has no stats.
        if supply.exhausted and not o.inflight and not supply.ready_items:
            state = "work_exhausted"
            decision = self._hold(state)
        elif native_valid and (o.memory_pressure is True or o.kv_usage >= self.config.congestion_kv_usage
                or o.preemptions_total is not None and self._preemptions is not None
                and o.preemptions_total > self._preemptions):
            state = "resource_pressure"
        elif feedback_gap:
            state = 'insufficient_information'
            decision = self._invalidate_feedback_gap(o)
            self._pending_feedback_gap = False
        elif (not o.fresh or not o.has_service_metrics or o.sample_age_s is None
              or o.sample_age_s > self.options.window_s
              or not 0 <= supply.observed_at_s-o.observed_at_s <= self.options.poll_interval_s):
            state = "insufficient_information"
            # A repeated delivery is not a new observation. Keep the last valid
            # measurement anchor while polling; invalidate genuinely old data.
            if o.sample_age_s is not None and o.sample_age_s > self.options.window_s:
                self._anchor, self._windows = None, []
            decision = self._hold(state)
        elif o.waiting:
            state = "backend_queued"
            self._anchor, self._windows = None, []
            decision = self._hold(state)
        elif supply.capacity_blocked:
            state = "configuration_boundary"
            self._anchor, self._windows = None, []
            decision = self._hold(state)
        elif not supply.ready_items and supply.source_exhausted:
            state = "work_tail_or_dependency_wait"
            self._anchor, self._windows = None, []
            decision = self._hold(state)
        # Ready work can remain while compute batches hand over. Include that
        # dispatch gap in this candidate's measured wall time; an instantaneous
        # active-batch count does not establish upstream supply starvation.
        elif not supply.ready_items:
            state = "upstream_supply_limited"
            self._anchor, self._windows = None, []
            reason = ('preparation_below_completion' if rates is not None
                      and rates.get('completed_items_s') is not None
                      and rates['prepared_items_s'] < rates['completed_items_s'] else
                      state)
            decision = self._hold(reason)
        if decision is None:
            decision = super().update(o)
            if decision.reason in ("capacity_limit", "sampled_range_complete"):
                state = "configuration_boundary"
            elif decision.reason in ("local_plateau", "performance_rollback", "waiting_regression",
                                     "trial_no_gain", "trial_regression"):
                state = "measured_no_gain"
            elif decision.reason in ('missing_work_feedback', 'insufficient_progress',
                                     'incomparable_work_mix', 'inconclusive_noise', 'missing_memory_feedback'):
                state = 'insufficient_information'
            elif self.evaluations and self.evaluations[-1].get('outcome') in ('no_gain', 'regression'):
                state = "measured_no_gain"
        elif native_valid and state != 'resource_pressure':
            # Low-pressure frames count even while no measurement is eligible.
            self._pressure_windows = 0
            self._preemptions = o.preemptions_total
        self.supply_states[state] = self.supply_states.get(state, 0)+1
        if (decision.action != 'hold' or self._supply_anchor is None
                or supply.observed_at_s-self._supply_anchor.observed_at_s >= self.options.window_s):
            self.supply_history = (self.supply_history + [dict(at_s=supply.observed_at_s-self.started,
                state=state, action=decision.action, reason=decision.reason, window=decision.window,
                ready_items=supply.ready_items, active_batches=supply.active_batches,
                upstream_pending=supply.upstream_pending, exhausted=supply.exhausted,
                capacity_blocked=supply.capacity_blocked, supply_rates=rates,
                local_capacity=supply.local_capacity,
                ready_work=[asdict(w) for w in supply.ready_work],
                running=o.running, waiting=o.waiting, kv_usage=o.kv_usage,
                sample_age_s=o.sample_age_s)])[-64:]
            if decision.action != 'hold':
                self.supply_changes = (self.supply_changes + [self.supply_history[-1]])[-64:]
            self._supply_anchor = supply
            self._delivery_anchor = o
        return decision

    def report(self):
        report = super().report()
        if report['stop_reason'] == 'adjacent_trial_unavailable' and self._search is not None:
            adjacent = min((key for key in self._search.points
                            if self.current_window < key <= self._search.ceiling), default=None)
            if (adjacent is not None and self._search.limit is not None
                    and self._search.points[adjacent]['outcome'] not in self._search.resolved
                    and self._search.points[adjacent]['attempts'] >= self._search.limit):
                report.update(exploration_censored=True, incomplete_reason='candidate_attempt_budget',
                              can_continue=False)
        return dict(report, control_unit="application_batches", initial_window=1,
                    feedback_gaps=self.feedback_gaps,
                    feedback_gap_pending=self._pending_feedback_gap,
                    exploration_limits=dict(elapsed_s=self.options.exploration_s,
                        changes=self.options.max_changes, attempts_per_candidate=self.options.max_candidate_attempts,
                        min_remaining=self.options.min_remaining),
                    demand_windows=list(self.supply_history), B_adjusted=False,
                    demand_changes=list(self.supply_changes), demand_states=dict(self.supply_states),
                    demand_decision_calls=sum(self.supply_states.values()),
                    control_seconds_scope='existing trial-controller calls only; gating/sampling stay in outer query wall',
                    CPU_GPU_utilization_and_physical_memory="not sampled; unknown",
                    capacity_is_performance_platform=False)
