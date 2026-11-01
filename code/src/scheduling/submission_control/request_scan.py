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
        state = frontier = lower_pending = None
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
            frontier = None if condition is None else condition.get('resource_frontier')
            if frontier is not None and (type(frontier) is not dict
                    or set(frontier) != {'ready_items', 'window', 'range_end'}
                    or any(type(v) is not int for v in frontier.values())
                    or frontier['ready_items'] < 0
                    or not 1 <= frontier['window'] <= frontier['range_end'] <= self.config.max_window
                    or frontier['window'] not in progress.points
                    or progress.points[frontier['window']]['attempts'] == 0
                       and progress.points[frontier['window']]['outcome'] != 'pressure'):
                raise ValueError('invalid prior resource frontier')
            if frontier is not None and progress.points[frontier['window']]['attempts'] == 0:
                # Legacy pressure bookkeeping can retain the point but has
                # invalidated this resource condition; do not reuse its range.
                frontier = None
            lower_pending = search_hint.get('local_probe_pending')
            if lower_pending is not None and (type(lower_pending) is not int
                    or not 1 <= lower_pending < initial_window
                    or lower_pending not in progress.points
                    or search_hint['terminal'] is not None):
                raise ValueError('invalid pending local probe')
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
        self._resource_frontier = deepcopy(frontier)
        self._pressure_active = False
        self.step_decisions = []
        self.work_epoch = 0
        self._confirming = False
        self._state_validation = self._reused
        self._validation_trigger = 'query_hint' if self._reused else None
        self._resume_lower_window = lower_pending
        self._query_pause = None

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
        if opportunity is None or opportunity.kind == 'none':
            return None
        condition = deepcopy(opportunity.condition)
        if condition['kind'] in ('idle_device_headroom', 'physical_busy'):
            condition['observed_window'] = opportunity.evidence.get('observed_request_window')
        return condition

    def _qualified_resource_changed(self):
        current, previous = self._resource_condition(), self._last_resource_condition
        if current is None or current == previous:
            return False
        if (previous is not None and current['kind'] in ('idle_device_headroom', 'physical_busy')
                and previous.get('kind') in ('idle_device_headroom', 'physical_busy')
                and current.get('observed_window') != previous.get('observed_window')):
            # A control action changes utilization too. First establish a fresh
            # baseline at the held quota, not a false external resource change.
            return False
        # A weaker observation is not a reason to raise concurrency. A newly
        # observed opportunity, more costed ready work, or a stronger physical
        # idle signal can reopen a local trial. Repeated polls cannot.
        if current['kind'] == 'physical_busy':
            return False
        if current['kind'] == 'native_headroom':
            return previous is None
        if (current['kind'] == 'idle_device_headroom' and previous is not None
                and previous.get('kind') == current['kind']
                and previous.get('gpu_uuids') == current.get('gpu_uuids')):
            before, after = previous.get('idle_share'), current.get('idle_share')
            return before is None or after is not None and after > before*(1+self.options.min_gain)
        if current['kind'] == 'costed_resource_opportunity' and previous is not None:
            if previous.get('kind') == current['kind']:
                target = previous.get('target')
                return type(target) is not int or current['target'] > target
        return True

    def _supply_frontier(self):
        if self._resource_frontier is not None:
            return self._resource_frontier
        # Older in-memory hints did not retain supply context. Their measured
        # points still bound the tried range; do not call it untested work.
        tried = max([self._confirmed_window] + [window for window, row in self._search.points.items()
                                               if row['attempts']])
        return dict(ready_items=0, window=tried, range_end=tried)

    def _resource_change(self):
        if self._qualified_resource_changed():
            return dict(trigger='qualified_resource_change', after_window=None)
        opportunity, supply = self._resource_opportunity, self._current_supply
        if opportunity is None or opportunity.kind == 'none' or supply is None:
            return None
        frontier = self._supply_frontier()
        end = min(self._search.ceiling, self._confirmed_window+supply.ready_items)
        # C itself, a coverage gap, a queue dip and restoration of an already
        # tried ready range are not new supply. Both demand and a legal trial
        # beyond the previously attempted frontier must have increased.
        if supply.ready_items <= frontier['ready_items'] or end <= frontier['range_end']:
            return None
        target = opportunity.probe_target_window
        if target is None:
            target = next((window for window in sorted(self._search.points)
                if frontier['window'] < window <= end
                and self._search.points[window]['outcome'] != 'pressure'
                and (self._search.limit is None or self._search.points[window]['attempts'] < self._search.limit)), None)
        if (type(target) is int and frontier['window']*(1+self.options.min_gain) < target <= end
                and (target not in self._search.points or self._search.points[target]['outcome'] != 'pressure'
                     and (self._search.limit is None or self._search.points[target]['attempts'] < self._search.limit))):
            return dict(trigger='ready_frontier_expanded', after_window=frontier['window'])
        return None

    def _remember_resource_condition(self):
        # Only a genuinely changed qualified condition advances this watermark;
        # the same predicate governs reopening and remembering an actual trial.
        if self._qualified_resource_changed():
            self._last_resource_condition = self._resource_condition()

    def _observe_reference_condition(self, window):
        condition = self._resource_condition()
        previous = self._last_resource_condition
        if (condition is not None and condition['kind'] in ('physical_busy', 'idle_device_headroom')
                and condition.get('observed_window') == window
                and (condition['kind'] == 'physical_busy' or previous is not None
                     and previous.get('kind') in ('physical_busy', 'idle_device_headroom')
                     and previous.get('observed_window') != window)):
            self._last_resource_condition = condition

    def _record_resource_trial(self, window, before):
        condition = self._resource_condition()
        if condition is None:
            return
        same_condition = not self._qualified_resource_changed()
        previous = self._resource_frontier if same_condition else None
        ready = self._current_supply.ready_items
        end = min(self.config.max_window, before+ready)
        # Ordinary geometric trials may exceed the immediately ready prefix.
        # Only a ready-backed part is a tested resource/supply opportunity.
        if window > end:
            return
        self._resource_frontier = dict(ready_items=max(ready, 0 if previous is None else previous['ready_items']),
            window=max(window, 1 if previous is None else previous['window']),
            range_end=max(end, 1 if previous is None else previous['range_end']))
        self._remember_resource_condition()

    def _historical_performance_changed(self, point):
        """The validated band invalidates conditions, never scores a winner."""
        previous = self._prior_condition
        if previous is None:
            return None
        return (point['rate_min'] > previous['rate_max']*(1+self.options.min_gain)
                or point['rate_max'] < previous['rate_min']*(1-self.options.min_gain))

    def _measurement_enabled(self):
        # A held selection is monitored on fresh original work, without a timer
        # that automatically reopens exploration for each query or each poll.
        return self._query_pause is None and (self._state_validation
            or not self._growth_stopped or self._terminal is not None)

    def _stop(self, reason, o, *, rollback=False):
        # Query limits suspend execution, not the unresolved validation intent.
        # _end_probe in the shared stop path must not erase its direction.
        self._resume_lower_window = self._pending_lower_window()
        self._query_pause = reason
        if reason == 'nonmonotonic_work':
            self._search.verified_window = None
            self._state_validation = True
            self._validation_trigger = 'invalid_work'
        return super()._stop(reason, o, rollback=rollback)

    def _clear_point(self):
        self._windows = []
        # Keep the last counter/clock anchor: the next window is disjoint.

    def _new_epoch(self, point, reason, *, rebase=True):
        self.work_epoch += 1
        self._tested = {}
        if rebase:
            self._resume_lower_window = None
            self._prior_condition = None
            self._search.work_context = self._work_context(point)
            self._search.verified_window = None
            for row in self._search.points.values():
                if row['outcome'] != 'pressure':
                    row['outcome'] = 'pending'
            # Invalid scores do not refund already spent per-candidate attempts.
            self._resource_frontier = None
            self._last_resource_condition = None
        self._terminal = self._hint_terminal = None
        self._growth_stopped, self._stop_reason = False, None
        self.evaluations.append(dict(kind=reason, work_epoch=self.work_epoch,
            comparison_valid=False, rule='new rates measured here; historical rates are not reused'))

    def _open_resource_epoch(self, point, change):
        self._new_epoch(point, 'new_resource_opportunity', rebase=False)
        self.evaluations[-1]['resource_trigger'] = change['trigger']
        for window, row in self._search.points.items():
            if (window > self._confirmed_window and row['outcome'] != 'pressure'
                    and (self._search.limit is None or row['attempts'] < self._search.limit)):
                row['outcome'] = 'pending'
        # A changed condition reopens eligible measurements, never their spent
        # attempt budget. Old no-gain/gain scores are not new-condition scores.

    def _invalidate_feedback_gap(self, o):
        self._resume_lower_window = self._pending_lower_window()
        safe = next((p for p in (self._reference, self._probe_reference,
                     self._tested.get(self._confirmed_window))
                     if p is not None and p['window'] == self._confirmed_window), None)
        if safe is not None and self._prior_condition is None:
            self._prior_condition = dict(rate_min=safe['rate_min'], rate_max=safe['rate_max'])
        elif safe is None and not self._state_validation:
            self._prior_condition = None
        self._state_validation = self._search.work_context is not None
        self._validation_trigger = 'native_gap' if self._state_validation else None
        self._hint_terminal = self._terminal or self._hint_terminal
        self._terminal = None
        self._tested = {}
        self._confirming = False
        # Every monitored selection (including exhausted evidence) can rebuild
        # its reference. An actual per-query pause still obeys its spent limits.
        if self._query_pause is None:
            self._growth_stopped, self._stop_reason = False, None
        return super()._invalidate_feedback_gap(o)

    def _pressure_event(self, o):
        self._terminal = self._hint_terminal = None
        self._tested = {}
        self._confirming = False
        # Pressure establishes a different safe quota. Its reference must be
        # rebuilt; a rate band measured at the old quota cannot validate it.
        self._state_validation = False
        self._validation_trigger = None
        self._prior_condition = None
        self._resource_frontier = None
        self._last_resource_condition = None
        self._resume_lower_window = None
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
            prior_work = self._cached_context or self._search.work_context
            changed = prior_work is not None and not self._same_work(prior_work, context)
            performance_changed = (self._state_validation
                                   and self._historical_performance_changed(point) is not False)
            change = self._resource_change() if self._state_validation else None
            self._cached_context = None
            reason = ('cached_work_changed' if changed else
                      'cached_performance_changed' if performance_changed and self._prior_condition is not None else
                      'cached_condition_unverified' if performance_changed else None)
            return self._resume_reference(point, o, reason=reason, resource_change=change,
                terminal=self._hint_terminal if self._state_validation else None,
                held_reason='reused_selection_validated')
        if self._terminal is not None and self._probe_at is None:
            previous = self._reference
            evidence = _local_pair_evidence(previous, point, self.options.min_gain)
            changed = not self._same_work(self._search.work_context, self._work_context(point))
            reason = ('work_changed' if changed else
                      'performance_changed' if self._historical_performance_changed(point) is True else None)
            return self._resume_reference(point, o, reason=reason, resource_change=self._resource_change(),
                terminal=self._terminal,
                held_reason='selection_monitored' if evidence['outcome'] is not None else 'monitor_incomparable')
        if self._probe_at is None and point['window'] == self._reference['window']:
            self._reference = point
            self._clear_point()
            return self._next_trial(o)
        return self._compare(point, o)

    def _resume_reference(self, point, o, *, reason, resource_change, terminal, held_reason):
        """One recovery transition for imported, interrupted and held evidence."""
        if self._resume_lower_window is not None:
            # Finishing an interrupted local validation takes precedence over
            # acting on a new upward opportunity. Only direction is retained.
            resource_change = None
        if reason is not None:
            self._new_epoch(point, reason)
        elif resource_change is not None:
            self._open_resource_epoch(point, resource_change)
        new_condition = reason is not None or resource_change is not None or self._prior_condition is None
        self._reference = point
        self._confirmed_window = point['window']
        self._tested[point['window']] = point
        if new_condition:
            self._search.work_context = self._work_context(point)
            self._prior_condition = dict(rate_min=point['rate_min'], rate_max=point['rate_max'])
        self._search.verified_window = point['window']
        self._clear_point()
        self._state_validation = False
        self._validation_trigger = None
        self._observe_reference_condition(point['window'])
        if reason is not None:
            lower = max(1, self.current_window//2)
            return (self._try_lower(lower, o, 'condition_changed_probe') if lower != self.current_window
                    else self._next_trial(o))
        if self._resume_lower_window is not None:
            return self._try_lower(self._resume_lower_window, o, 'local_probe_resume')
        if resource_change is not None:
            return self._next_trial(o, after_window=resource_change['after_window'])
        if terminal is not None:
            self._terminal = terminal
            self._growth_stopped, self._stop_reason = True, terminal
            return self._hold(held_reason)
        return self._next_trial(o)

    def _set_lower_intent(self, target):
        self._resume_lower_window = target
        if target not in self._search.points:
            # A declared, unstarted validation has no charged attempt or score,
            # just like an untried geometric point. It can survive a budget veto.
            self._search.points[target] = dict(attempts=0, outcome='pending')
            self._search.recovery_points.add(target)

    def _try_lower(self, target, o, reason):
        self._set_lower_intent(target)
        return self._probe(target, o, reason)

    def _pending_lower_window(self):
        if self._probe_at is not None:
            candidate = (self._probe_candidate['window'] if self._probe_candidate is not None
                         else self.current_window)
            if candidate < self._confirmed_window:
                return candidate
        return self._resume_lower_window

    def _local_probe_blocked_by(self):
        target = self._pending_lower_window()
        if target is None:
            return None
        if self._query_pause is not None:
            return self._query_pause
        if target > self._search.ceiling:
            return 'pressure_ceiling'
        row = self._search.points.get(target)
        if row is not None and self._search.limit is not None and row['attempts'] >= self._search.limit:
            return 'candidate_attempt_limit'
        if row is not None and row['outcome'] in self._search.resolved:
            return 'candidate_evidence_resolved'
        return None

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

    def _next_trial(self, o, *, after_window=None):
        if self._resume_lower_window is not None:
            return self._try_lower(self._resume_lower_window, o, 'local_probe_resume')
        if self._terminal is not None:
            return self._hold(self._terminal)
        if self.current_window >= self.config.max_window:
            return self._select(o, 'capacity_limit')
        floor = max(self._confirmed_window, after_window or 0)
        higher = [v for v in self._search.available() if v > floor]
        target = min(higher) if higher else None
        step = self._resource_step
        opportunity = self._resource_opportunity
        source = 'geometric_fallback'
        reason = 'resource_estimate_unavailable' if step is None else step.reason
        if (step is not None and type(step.target_window) is int
                and floor < step.target_window <= self._search.ceiling
                and (step.target_window not in self._search.points or step.target_window in higher)):
            target = step.target_window
            source = 'costed_resource_headroom'
        elif opportunity is not None and opportunity.kind != 'none':
            requested = opportunity.probe_target_window
            if (type(requested) is int and floor < requested <= self._search.ceiling
                    and (requested not in self._search.points or requested in higher)):
                target = requested
            if target is not None:
                source, reason = 'resource_feedback_probe', opportunity.reason
        self.step_decisions = (self.step_decisions + [dict(at_s=o.observed_at_s-self.started,
            from_window=self._confirmed_window, target=target, source=source, reason=reason,
            evidence=None if step is None else deepcopy(step.evidence), work_epoch=self.work_epoch)])[-64:]
        if opportunity is not None:
            self.step_decisions[-1]['resource_observation'] = deepcopy(opportunity.evidence)
            self.step_decisions[-1]['requested_probe_target'] = opportunity.probe_target_window
            self.step_decisions[-1]['probe_target_accepted'] = (source == 'resource_feedback_probe'
                and opportunity.probe_target_window is not None and target == opportunity.probe_target_window)
        if step is not None:
            self.step_decisions[-1]['requested_resource_target'] = step.target_window
            self.step_decisions[-1]['resource_target_accepted'] = source == 'costed_resource_headroom'
        if target is None:
            # An exhausted opportunity is still observed. Do not repeatedly
            # reopen the same condition, invent a trial or refund its budget.
            self._remember_resource_condition()
            return self._select(o, 'capacity_limit' if self._confirmed_window == self.config.max_window
                                else 'candidate_evidence_exhausted')
        before = self.current_window
        decision = self._probe(target, o)
        self.step_decisions[-1]['trial_actuated'] = self._probe_at is not None and self.current_window == target
        if self.step_decisions[-1]['trial_actuated']:
            self._record_resource_trial(target, before)
        return decision

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
                        self._try_lower(max(1, point['window']//2), o, 'condition_changed_probe'))
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
            lower_won = point['window'] < previous['window']
            self._accept_gain(point, o)
            if lower_won:
                if point['window'] == 1:
                    return self._select(o, 'performance_regression')
                self._set_lower_intent(max(1, point['window']//2))
            # A conditional recheck left admission at the lower point. Returning
            # to the accepted point is actuation, not a fresh reference replay.
            self._clear_point()
            return self._change(point['window'], 'performance_accept', o)
        if outcome in ('no_gain', 'regression') and not conflicts:
            self._tested[previous['window']] = previous
            self._tested[point['window']] = point
            self._search.finish(point['window'], outcome)
            if point['window'] == self._resume_lower_window:
                self._resume_lower_window = None
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
        if changed:
            # The interrupted local trial belongs to the old work condition.
            # Its replacement must first qualify a fresh safe reference, not
            # silently fall through to an unrelated upward scan.
            self._reference = None
            self._search.verified_window = None
            self._state_validation = True
            self._validation_trigger = 'work_change'
        # No plateau is certified. Reopen on fresh evidence at this safe point;
        # the already-accounted elapsed/changes/attempt costs are retained.
        return self._hold('incomparable_work_mix')

    def _accept_gain(self, point, o):
        if point['window'] == self._resume_lower_window:
            self._resume_lower_window = None
        self._search.finish(point['window'], 'gain')
        self._search.verified_window = point['window']
        self._search.work_context = self._work_context(point)
        self._prior_condition = dict(rate_min=point['rate_min'], rate_max=point['rate_max'])
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
        self._prior_condition = dict(rate_min=self._reference['rate_min'], rate_max=self._reference['rate_max'])
        self._terminal = reason
        self._growth_stopped, self._stop_reason = True, reason
        self._clear_point()
        return (self._change(selected, reason, o) if self.current_window != selected else self._hold(reason))

    def export_hint(self):
        if (self._state_validation or self._search.verified_window != self._confirmed_window
                or self._search.work_context is None):
            return None
        # An interrupted larger candidate is resumable. Rates and phase/native
        # clocks are deliberately absent; the next query must measure its work.
        condition = self._prior_condition or self._tested[self._confirmed_window]
        return dict(schema='semloom.request_scan.v1', strategy=self.strategy,
            options=asdict(self.options),
            selected_window=self._confirmed_window, search_state=deepcopy(self._search.snapshot()),
            terminal=self._terminal if self._terminal in ('local_plateau', 'performance_regression', 'capacity_limit') else None,
            work_context=deepcopy(self._search.work_context),
            local_probe_pending=self._pending_lower_window(),
            condition_evidence=dict(scope='invalidation_only_not_a_current_query_score',
                rate_min=condition['rate_min'], rate_max=condition['rate_max'],
                resource_condition=deepcopy(self._last_resource_condition),
                resource_frontier=deepcopy(self._resource_frontier)))

    def report(self):
        value = super().report()
        measured_stop = self._terminal in ('local_plateau', 'performance_regression')
        bounded_stop = measured_stop or self._terminal == 'capacity_limit'
        value.update(strategy=self.strategy, initial_window=self._initial_window,
            state_reused=self._reused, fresh_state_validation_pending=self._state_validation,
            validation_trigger=self._validation_trigger, resource_frontier=deepcopy(self._resource_frontier),
            local_probe_pending=self._pending_lower_window(),
            query_exploration_pause=self._query_pause,
            local_probe_blocked_by=self._local_probe_blocked_by(),
            continuation_state=('query_paused' if self._query_pause is not None else
                'reference_pending' if self._state_validation else
                'local_probe_blocked' if self._local_probe_blocked_by() is not None else
                'local_probe_pending' if self._pending_lower_window() is not None else
                'monitoring' if self._terminal is not None else 'searching'),
            comparable_selection=not self._state_validation and self._search.verified_window == self._confirmed_window,
            work_epoch=self.work_epoch, step_decisions=deepcopy(self.step_decisions),
            stop_reason=self._query_pause or self._terminal or self._stop_reason, exploration_censored=not measured_stop,
            incomplete_reason=None if measured_stop else self._terminal or self._stop_reason,
            can_continue=self.options.allow_exploration and not bounded_stop and bool(self._search.available()),
            reference_rechecks=sum(row['kind'] == 'local_confirmation' for row in self.evaluations),
            resource_step_scope='qualified idle share chooses a bounded tentative request target; exact cost is optional',
            recommendation_scope='measured comparable quotas; not a global optimum or hardware saturation proof')
        return deepcopy(value)
