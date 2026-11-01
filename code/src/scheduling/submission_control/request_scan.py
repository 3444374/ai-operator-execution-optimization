"""Demand-gated ascending request search over original, useful work.

The reference policy remains available for reproducing its measurements. This
policy shares its accounting, settling and work matching, not its A-B-A rule.
Local comparisons are operational evidence, not a causal hardware cost model.
"""
from copy import deepcopy
from dataclasses import asdict
import math

from .adaptive import _GeometricSearch, _local_pair_evidence
from .stage_work import SupplyFeedbackController, default_request_feedback_options
from ..runtime.calibration import select_near_peak


class AscendingRequestController(SupplyFeedbackController):
    control_unit = 'single_model_requests'
    strategy = 'ascending_request_work_v1'

    def __init__(self, max_requests, options=None, *, clock=None):
        if type(max_requests) is not int or max_requests < 1:
            raise ValueError('max_requests must be a positive operational capacity')
        options = default_request_feedback_options() if options is None else options
        if (options.search_mode != 'geometric' or options.work_basis != 'computed'
                or options.work_comparison != 'local_pairs'
                or options.poll_interval_s > options.window_s):
            raise ValueError('ascending requests require geometric computed local-pair evidence')
        self._initialize_supply(max_requests, options, clock)

    def reset(self, initial_window, total_items, *, search_hint=None):
        if type(initial_window) is not int or not 1 <= initial_window <= self.config.max_window:
            raise ValueError('initial request quota must fit its declared capacity')
        state = None
        if search_hint is None:
            if initial_window != 1:
                raise ValueError('a non-one start requires a validated ascending hint')
        else:
            if (type(search_hint) is not dict or search_hint.get('schema') != 'semloom.request_scan.v1'
                    or search_hint.get('strategy') != self.strategy
                    or search_hint.get('selected_window') != initial_window
                    or search_hint.get('options') != asdict(self.options)
                    or not 1 <= initial_window <= self.config.max_window
                    or search_hint.get('terminal') not in (None, 'local_plateau',
                        'performance_regression', 'capacity_limit')
                    or type(search_hint.get('search_state')) is not dict):
                raise ValueError('incompatible ascending request hint')
            state = deepcopy(search_hint['search_state'])
            # Validate the nested finite progress with the existing contract.
            progress = _GeometricSearch(1, self.config.max_window, self.options.max_candidate_attempts, state)
            if progress.work_context is None or progress.verified_window != initial_window:
                raise ValueError('hint needs a qualified work context and selected quota')
            condition = search_hint.get('condition_evidence')
            if condition is not None and (type(condition) is not dict
                    or condition.get('scope') != 'invalidation_only_not_a_current_query_score'
                    or any(type(condition.get(k)) not in (int, float)
                           or not math.isfinite(condition[k]) or condition[k] <= 0 for k in ('rate_min', 'rate_max'))
                    or condition['rate_min'] > condition['rate_max']
                    or condition.get('resource_condition') is not None and not isinstance(condition['resource_condition'], dict)):
                raise ValueError('invalid prior query condition evidence')
            progress.resume_pressure()
            state = progress.snapshot()
        self._reset_supply(initial_window, total_items, search_hint=state)
        self._initial_window = initial_window
        self._terminal = None
        self._hint_terminal = None if search_hint is None else search_hint['terminal']
        self._reused = search_hint is not None
        self._tested = {}
        self._resource_step = None
        self._resource_opportunity = None
        self._prior_condition = None if search_hint is None else deepcopy(search_hint.get('condition_evidence'))
        self._last_resource_condition = (None if self._prior_condition is None else
                                         deepcopy(self._prior_condition.get('resource_condition')))
        self._pressure_active = False
        self.step_decisions = []
        self.work_epoch = 0
        self._confirming = False
        self._state_validation = self._reused

    def update_supply(self, observation, supply, *, resource_step=None, resource_opportunity=None):
        self._resource_step = resource_step
        self._resource_opportunity = resource_opportunity
        return super().update_supply(observation, supply)

    def _resource_condition(self):
        step = self._resource_step
        if (step is not None and type(step.target_window) is int
                and self._confirmed_window < step.target_window <= self.config.max_window):
            return dict(kind='costed_resource_opportunity', target=step.target_window,
                engine_uid=step.evidence.get('engine_uid'), resource=step.evidence.get('resource'),
                cost_model=step.evidence.get('cost_model'))
        opportunity = self._resource_opportunity
        return (deepcopy(opportunity.condition) if opportunity is not None
                and opportunity.kind != 'none' else None)

    def _resource_changed(self):
        current, previous = self._resource_condition(), self._last_resource_condition
        if current is None or current == previous:
            return False
        # A weaker observation is not a reason to raise concurrency. A newly
        # observed opportunity, more costed ready work, or a stronger physical
        # idle signal can reopen a local trial. Repeated polls cannot.
        if current['kind'] == 'native_headroom':
            return previous is None
        if current['kind'] == 'costed_resource_opportunity' and previous is not None:
            if previous.get('kind') == current['kind']:
                target = previous.get('target')
                return type(target) is not int or current['target'] > target
        return True

    def _historical_performance_changed(self, point):
        """Old bands invalidate conditions; they never score a new winner."""
        previous = self._prior_condition
        if previous is None:
            return None
        return (point['rate_min'] > previous['rate_max']*(1+self.options.min_gain)
                or point['rate_max'] < previous['rate_min']*(1-self.options.min_gain))

    def _measurement_enabled(self):
        # A held selection is monitored on fresh original work, without a timer
        # that automatically reopens exploration for each query or each poll.
        return not self._growth_stopped or self._terminal is not None

    def _clear_point(self):
        self._windows = []
        # Keep the last counter/clock anchor: the next window is disjoint.

    def _new_epoch(self, point, reason, *, rebase=True):
        self.work_epoch += 1
        self._tested = {}
        if rebase:
            self._search.rebase_work(self._work_context(point))
        self._terminal = self._hint_terminal = None
        self._growth_stopped, self._stop_reason = False, None
        self.evaluations.append(dict(kind=reason, work_epoch=self.work_epoch,
            comparison_valid=False, rule='new rates measured here; historical rates are not reused'))

    def _open_resource_epoch(self, point):
        self._new_epoch(point, 'new_resource_opportunity', rebase=False)
        for window, row in self._search.points.items():
            if (window > self._confirmed_window and row['outcome'] != 'pressure'
                    and (self._search.limit is None or row['attempts'] < self._search.limit)):
                row['outcome'] = 'pending'
        # A changed condition reopens eligible measurements, never their spent
        # attempt budget. Old no-gain/gain scores are not new-condition scores.

    def _invalidate_feedback_gap(self, o):
        self._terminal = self._hint_terminal = None
        self._tested = {}
        self._confirming = False
        if self._stop_reason in ('local_plateau', 'performance_regression', 'capacity_limit'):
            self._growth_stopped, self._stop_reason = False, None
        return super()._invalidate_feedback_gap(o)

    def _pressure_event(self, o):
        self._terminal = self._hint_terminal = None
        self._tested = {}
        self._confirming = False
        if not self._pressure_active:
            self.work_epoch += 1
        self._pressure_active = True

    def _update(self, o):
        decision = super()._update(o)
        if (self._pressure_active and o.fresh and o.has_service_metrics
                and self._search.pressure_recovery is None and not o.waiting
                and o.kv_usage <= self.config.low_load_kv_usage and o.memory_pressure is not True):
            self._pressure_active = False
        return decision

    def _accept_measurement(self, point, o):
        if self._probe_candidate is not None:
            candidate, self._probe_candidate = self._probe_candidate, None
            self._reference = point
            return self._compare(candidate, o)
        if self._reference is None:
            context = self._work_context(point)
            changed = self._cached_context is not None and not self._same_work(self._cached_context, context)
            performance_changed = (self._state_validation and self._hint_terminal is not None
                                   and self._historical_performance_changed(point) is not False)
            resource_changed = self._state_validation and self._hint_terminal is not None and self._resource_changed()
            self._cached_context = None
            if changed:
                self._new_epoch(point, 'cached_work_changed')
            elif performance_changed:
                self._new_epoch(point, 'cached_performance_changed' if self._prior_condition is not None
                                else 'cached_condition_unverified')
            elif resource_changed:
                self._open_resource_epoch(point)
            self._reference = point
            self._confirmed_window = point['window']
            self._tested[point['window']] = point
            self._search.work_context = context
            self._search.verified_window = point['window']
            self._clear_point()
            if self._state_validation:
                self._state_validation = False
                if changed:
                    # Revalidate locally at a lower quota instead of treating a
                    # new work mix as proof for either the old rates or C=1.
                    return self._probe(max(1, self.current_window//2), o, 'work_changed_probe')
                if performance_changed or resource_changed:
                    self._last_resource_condition = self._resource_condition()
                    return self._next_trial(o)
                if self._hint_terminal is not None:
                    self._terminal = self._hint_terminal
                    self._growth_stopped, self._stop_reason = True, self._terminal
                    return self._hold('reused_selection_validated')
            return self._next_trial(o)
        if self._terminal is not None and self._probe_at is None:
            previous = self._reference
            evidence = _local_pair_evidence(previous, point, self.options.min_gain)
            changed = not self._same_work(self._work_context(previous), self._work_context(point))
            self._clear_point()
            self._reference = point
            resources_changed = self._resource_changed()
            if changed or evidence['outcome'] in ('gain', 'regression') or resources_changed:
                resource_only = resources_changed and not changed and evidence['outcome'] not in ('gain', 'regression')
                if resource_only:
                    self._open_resource_epoch(point)
                else:
                    self._new_epoch(point, 'work_changed' if changed else 'performance_changed')
                self._tested[point['window']] = point
                self._search.work_context = self._work_context(point)
                self._search.verified_window = point['window']
                if resource_only:
                    self._last_resource_condition = self._resource_condition()
                    return self._next_trial(o)
                lower = max(1, self.current_window//2)
                if lower != self.current_window:
                    return self._probe(lower, o, 'condition_changed_probe')
                return self._next_trial(o)
            return self._hold('selection_monitored' if evidence['outcome'] is not None else 'monitor_incomparable')
        if self._probe_at is None and point['window'] == self._reference['window']:
            self._reference = point
            self._clear_point()
            return self._next_trial(o)
        return self._compare(point, o)

    def _probe(self, target, o, reason='performance_probe'):
        if target == self.current_window:
            return self._next_trial(o) if target < self.config.max_window else self._hold('capacity_limit')
        if type(target) is not int or not 1 <= target <= self._search.ceiling:
            return self._hold('candidate_unavailable')
        if target in self._search.points and target not in self._search.available():
            return self._hold('candidate_unavailable')
        decision = self._trial_budget(target, o)
        if decision is not None:
            return decision
        if target not in self._search.points:
            # Resource-selected points use the existing finite progress schema;
            # no fabricated geometric outcome is attached to a skipped point.
            self._search.points[target] = dict(attempts=0, outcome='pending')
            self._search.recovery_points.add(target)
        self._search.begin(target)
        self._probe_at = o.observed_at_s
        self._probe_loss_base = self.estimated_trial_loss_s
        self._confirming = False
        return self._change(target, reason, o)

    def _next_trial(self, o):
        if self._terminal is not None:
            return self._hold(self._terminal)
        if self.current_window >= self.config.max_window:
            return self._select(o, 'capacity_limit')
        higher = [v for v in self._search.available() if v > self._confirmed_window]
        target = min(higher) if higher else None
        step = self._resource_step
        opportunity = self._resource_opportunity
        source = 'geometric_fallback'
        reason = 'resource_estimate_unavailable' if step is None else step.reason
        if (step is not None and type(step.target_window) is int
                and self._confirmed_window < step.target_window <= self._search.ceiling
                and (step.target_window not in self._search.points or step.target_window in higher)):
            target = step.target_window
            source = 'costed_resource_headroom'
        elif opportunity is not None and opportunity.kind != 'none' and higher:
            target = sorted(higher)[min(opportunity.extra_geometric_steps, len(higher)-1)]
            source, reason = 'resource_feedback_probe', opportunity.reason
        self.step_decisions = (self.step_decisions + [dict(at_s=o.observed_at_s-self.started,
            from_window=self._confirmed_window, target=target, source=source, reason=reason,
            evidence=None if step is None else deepcopy(step.evidence), work_epoch=self.work_epoch)])[-64:]
        if opportunity is not None:
            self.step_decisions[-1]['resource_observation'] = deepcopy(opportunity.evidence)
        if step is not None:
            self.step_decisions[-1]['requested_resource_target'] = step.target_window
            self.step_decisions[-1]['resource_target_accepted'] = source == 'costed_resource_headroom'
        if target is None:
            return self._select(o, 'capacity_limit' if self._confirmed_window == self.config.max_window
                                else 'candidate_evidence_exhausted')
        condition = self._resource_condition()
        if condition is not None:
            self._last_resource_condition = condition
        return self._probe(target, o)

    def _compare(self, point, o):
        previous = self._reference
        evidence = _local_pair_evidence(previous, point, self.options.min_gain)
        before = (None if self._probe_reference is None else
                  _local_pair_evidence(self._probe_reference, point, self.options.min_gain))
        self.evaluations = (self.evaluations + [dict(kind='local_pair_evidence',
            reference=deepcopy(previous), candidate=deepcopy(point), evidence=evidence,
            earlier_reference_evidence=before, comparison_valid=evidence['outcome'] is not None,
            work_epoch=self.work_epoch)])[-64:]
        outcome = evidence['outcome']
        if outcome == 'gain' and not self._confirming:
            # This is the critical difference from reference-policy A-B-A:
            # measured gain is accepted before considering the next point.
            lower_won = point['window'] < previous['window']
            self._accept_gain(point, o)
            if lower_won:
                return (self._select(o, 'performance_regression') if point['window'] == 1 else
                        self._probe(max(1, point['window']//2), o, 'condition_changed_probe'))
            return self._next_trial(o)
        if not self._confirming:
            if outcome is None and len(self._windows) < 6:
                return self._hold('collecting_local_pairs')
            # A flat/lower/mixed result requires a fresh local check, not a
            # return after every successful increase. Keep original work useful.
            if not self._within_change_budget(2):
                return self._stop('exploration_budget', o, rollback=True)
            self._confirming = True
            self._probe_reference = previous
            self._probe_candidate, self._reference = point, None
            self.evaluations.append(dict(kind='local_confirmation', outcome=outcome,
                comparison_valid=False, work_epoch=self.work_epoch))
            return self._change(self._confirmed_window, 'local_confirmation', o)
        if outcome is None and len(self._windows) < 6:
            self._probe_candidate, self._reference = point, None
            return self._hold('collecting_local_pairs')
        conflicts = before is not None and (before['outcome'] in ('gain', 'regression', 'no_gain')
            and outcome in ('gain', 'regression', 'no_gain') and before['outcome'] != outcome
            or outcome == 'gain' and any(p['gain'] < -self.options.min_gain for p in before['not_heavier_pairs'])
            or outcome == 'regression' and any(p['gain'] > self.options.min_gain for p in before['not_lighter_pairs']))
        if outcome == 'gain' and not conflicts:
            self._accept_gain(point, o)
            # A conditional recheck left admission at the lower point. Returning
            # to the accepted point is actuation, not a fresh reference replay.
            self._clear_point()
            return self._change(point['window'], 'performance_accept', o)
        if outcome in ('no_gain', 'regression') and not conflicts:
            self._tested[previous['window']] = previous
            self._tested[point['window']] = point
            self._search.finish(point['window'], outcome)
            self._end_probe(o.observed_at_s)
            self._confirming = False
            return self._select(o, 'local_plateau' if outcome == 'no_gain' else 'performance_regression')
        self._search.finish(point['window'], 'noise' if conflicts else 'incomparable')
        self._end_probe(o.observed_at_s)
        self._confirming = False
        changed = not self._same_work(self._work_context(previous), self._work_context(point))
        self._new_epoch(previous, 'reference_order_conflict' if conflicts else 'incomparable_work_mix', rebase=changed)
        self._reference = previous
        self._tested[previous['window']] = previous
        self._confirmed_window = previous['window']
        # The fresh safe reference is a qualified *starting* point even though
        # its candidate is unresolved. Preserve continuation, not a winner or
        # the conflicting candidate's rate. Same-work noise keeps attempt cost.
        self._search.work_context = self._work_context(previous)
        self._search.verified_window = previous['window']
        self._clear_point()
        # No plateau is certified. Reopen on fresh evidence at this safe point;
        # the already-accounted elapsed/changes/attempt costs are retained.
        return self._hold('incomparable_work_mix')

    def _accept_gain(self, point, o):
        self._search.finish(point['window'], 'gain')
        self._search.verified_window = point['window']
        self._search.work_context = self._work_context(point)
        self._tested[point['window']] = point
        self._accepted_steps += 1
        self._confirmed_window = point['window']
        self._reference = point
        self._end_probe(o.observed_at_s)
        self._confirming = False
        self._clear_point()

    def _select(self, o, reason):
        peak = self._tested.get(self._confirmed_window, self._reference)
        scores = {self._confirmed_window: 1.0}
        for window, point in self._tested.items():
            if window == self._confirmed_window:
                continue
            matched = _local_pair_evidence(peak, point, self.options.min_gain)
            equal = [pair for pair in matched['not_lighter_pairs'] if pair['equal_work']]
            if len(equal) >= 2:
                scores[window] = min(pair['gain']+1 for pair in equal)
        selected, eligible = select_near_peak(scores, ratio=1-self.options.min_gain)
        self.evaluations.append(dict(kind='near_peak_selection', scores=scores, selected=selected,
            eligible=eligible, ratio=1-self.options.min_gain, work_epoch=self.work_epoch,
            scope='disjoint equal-work local pairs relative to confirmed point; only measured quotas'))
        self._confirmed_window = selected
        self._reference = self._tested.get(selected, peak)
        self._search.verified_window = selected
        self._search.work_context = self._work_context(self._reference)
        self._terminal = reason
        condition = self._resource_condition()
        if condition is not None:
            self._last_resource_condition = condition
        self._growth_stopped, self._stop_reason = True, reason
        self._clear_point()
        return (self._change(selected, reason, o) if self.current_window != selected else self._hold(reason))

    def export_hint(self):
        if (self._state_validation or self._search.verified_window != self._confirmed_window
                or self._search.work_context is None):
            return None
        # An interrupted larger candidate is resumable. Rates and phase/native
        # clocks are deliberately absent; the next query must measure its work.
        return dict(schema='semloom.request_scan.v1', strategy=self.strategy,
            options=asdict(self.options),
            selected_window=self._confirmed_window, search_state=deepcopy(self._search.snapshot()),
            terminal=self._terminal if self._terminal in ('local_plateau', 'performance_regression', 'capacity_limit') else None,
            work_context=deepcopy(self._search.work_context),
            condition_evidence=dict(scope='invalidation_only_not_a_current_query_score',
                rate_min=self._tested[self._confirmed_window]['rate_min'],
                rate_max=self._tested[self._confirmed_window]['rate_max'],
                resource_condition=deepcopy(self._last_resource_condition)))

    def report(self):
        value = super().report()
        measured_stop = self._terminal in ('local_plateau', 'performance_regression')
        bounded_stop = measured_stop or self._terminal == 'capacity_limit'
        value.update(strategy=self.strategy, initial_window=self._initial_window,
            state_reused=self._reused, fresh_state_validation_pending=self._state_validation,
            comparable_selection=not self._state_validation and self._search.verified_window == self._confirmed_window,
            work_epoch=self.work_epoch, step_decisions=deepcopy(self.step_decisions),
            stop_reason=self._terminal or self._stop_reason, exploration_censored=not measured_stop,
            incomplete_reason=None if measured_stop else self._terminal or self._stop_reason,
            can_continue=self.options.allow_exploration and not bounded_stop and bool(self._search.available()),
            reference_rechecks=sum(row['kind'] == 'local_confirmation' for row in self.evaluations),
            resource_step_scope='source-qualified resource opportunities drive tentative geometric trials; exact cost is optional',
            recommendation_scope='measured comparable quotas; not a global optimum or hardware saturation proof')
        return deepcopy(value)
