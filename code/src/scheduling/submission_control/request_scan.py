"""Resource-directed request trials over original, useful work.

Local comparisons are operational evidence, not a causal hardware cost model.
"""
from copy import deepcopy
from dataclasses import asdict
import math

from .adaptive import _local_pair_evidence
from .resource_probe import request_supply_blocked
from ..runtime.calibration import select_near_peak


class ResourceRequestController:
    """Resource-directed trials with separate, query-local work comparisons.

    This policy does not inherit AIMD. The existing Core continues to own
    accepted work, bytes, cancellation and settlement. A smaller window only
    stops new selections. The adapter must supply a validated operational
    permit for each frame; this class does not discover hardware capacity.
    """
    control_unit = 'single_model_requests'
    strategy = 'resource_request_v1'
    _trace_limit = 64  # Diagnostic retention only; never a search/request limit.

    def __init__(self, max_requests, options=None, *, clock=None):
        import time
        from .adaptive import ResourceRequestConfig
        if type(max_requests) is not int or max_requests < 1:
            raise ValueError('max_requests must be the positive applied owner maximum')
        self.options = ResourceRequestConfig() if options is None else options
        if type(self.options) is not ResourceRequestConfig:
            raise TypeError('resource requests require ResourceRequestConfig')
        self.maximum, self.clock = max_requests, clock or time.monotonic
        self._reset(1, 0)

    def reset(self, initial_window, total_items, *, search_hint=None, resource_identity=None):
        if resource_identity is None:
            raise ValueError('resource requests need an explicitly bound engine and session')
        self._reset(initial_window, total_items, search_hint=search_hint, resource_identity=resource_identity)

    def _reset(self, initial_window, total_items, *, search_hint=None, resource_identity=None):
        if type(initial_window) is not int or initial_window != 1 or search_hint is not None:
            raise ValueError('resource requests start at one without cross-query hints')
        if type(total_items) is not int or total_items < 0:
            raise ValueError('total_items must be a non-negative declared work count')
        if resource_identity is not None and (
                type(resource_identity) is not tuple or len(resource_identity) != 2
                or not isinstance(resource_identity[0], str) or not resource_identity[0]
                or type(resource_identity[1]) is not int or resource_identity[1] < 0):
            raise ValueError('resource identity requires the validated engine and session')
        self.resource_identity = resource_identity
        self.current_window = self._initial_window = self._fallback_window = initial_window
        self.total_items, self.started = total_items, self.clock()
        self._sequence = self._native_sequence = self._last_sample_at = None
        self._last_control_at = self._safety_sequence = None
        self._guard_seen_sequence = None
        self._guard_seen_payload = None
        self._guard_seen_rejected = False
        self._safety_payload = None
        self._preemptions = self._rejections = None
        self._counter_source_valid = self._work_source_valid = True
        self._native_memory_bound = self._guard_memory_bound = False
        self._rejection_source_kind = None
        self._preemption_baseline_pending = False
        self._preemption_baseline_count = None
        self._work_watermark = self._pending_work_context = self._last_point = None
        self._work_known_watermark = [None]*5
        self._anchor = self._reference = self._trial = self._pending = None
        self._verified_window = self._measured_window = None
        self._last_verified_window = None
        self._windows = []
        self._settling = False
        self._changes = self._accepted_steps = 0
        self._pressure_ceiling = self.maximum
        self._recovery_needed = False
        self._recovery_count = None
        self._stop_reason = None
        self._exploration_stop_reason = None
        self._early_trial_available = True
        self._direction = 'up'
        self._initial_lower_scan = False
        self._censored_frontier = None
        self.history, self.measurements, self.evaluations, self.step_decisions = [], [], [], []
        self.reasons = {}

    def _record(self, collection, row):
        collection.append(row)
        del collection[:-self._trace_limit]

    def sampling_interval_s(self, pending_items):
        # Safety sampling continues at the same cadence after exploration ends.
        return self.options.poll_interval_s

    def export_hint(self):
        return None

    def update(self, observation):
        raise RuntimeError('resource requests need update_supply with a validated operational permit')

    def _decision(self, reason, target=None, observation=None):
        from ..core.models import WindowDecision
        self.reasons[reason] = self.reasons.get(reason, 0)+1
        if target is None or target == self.current_window:
            return WindowDecision(self.current_window, 'hold', reason)
        if type(target) is not int or not 1 <= target <= self.maximum:
            raise ValueError('a request action must fit the applied owner maximum')
        before, self.current_window = self.current_window, target
        self._changes += 1
        self._settling = True
        self._anchor, self._windows = None, []
        self._record(self.history, dict(before=before, window=target, reason=reason,
            at_s=observation.observed_at_s-self.started, phase=self._changes,
            completed_items_total=observation.completed_items_total,
            existing_work='drains under its original Core ownership'))
        return WindowDecision(target, 'increase' if target > before else 'decrease', reason)

    def _permit_status(self, o, s, permit):
        from .resource_probe import ResourceRequestPermit
        if (not isinstance(permit, ResourceRequestPermit) or self.resource_identity is None
                or (permit.engine_uid, permit.session_id) != self.resource_identity):
            return 'unbound'
        if (self._last_control_at is not None and o.observed_at_s < self._last_control_at
                or not 0 <= s.observed_at_s-o.observed_at_s <= self.options.max_sample_age_s):
            return 'unknown_clock'
        if not 0 <= o.observed_at_s-permit.observed_at_s <= self.options.max_sample_age_s:
            return 'stale_operational_permit'
        return 'bound'

    def _guard_status(self, o, permit):
        if permit.safety_sequence is None:
            return 'native' if self._guard_seen_sequence is None else 'missing_bound_safety_sample'
        if self._guard_seen_sequence is not None and permit.safety_sequence < self._guard_seen_sequence:
            return 'out_of_order_safety_sample'
        if permit.safety_sequence == self._guard_seen_sequence:
            if self._guard_seen_rejected:
                return 'rejected_safety_sample'
            if self._safety_values(permit) != self._guard_seen_payload:
                return 'conflicting_safety_sample'
        if not 0 <= o.observed_at_s-permit.safety_observed_at_s <= self.options.max_sample_age_s:
            return 'expired_safety_sample'
        if permit.safety_sequence is not None and self._safety_sequence is not None:
            if permit.safety_sequence < self._safety_sequence:
                return 'out_of_order_safety_sample'
            if permit.safety_sequence == self._safety_sequence and self._safety_values(permit) != self._safety_payload:
                return 'conflicting_safety_sample'
            if permit.safety_sequence == self._safety_sequence:
                return 'repeat'
        return 'new'

    @staticmethod
    def _safety_values(permit):
        return (permit.rejected_requests_total, permit.memory_pressure,
                permit.safety_observed_at_s)

    def _frame_status(self, o, s, permit):
        if (type(o.sample_sequence) is not int or permit.sample_sequence != o.sample_sequence
                or permit.native_sequence is None or o.sample_age_s is None
                or not 0 <= o.sample_age_s <= self.options.max_sample_age_s
                or not 0 <= s.observed_at_s-o.observed_at_s <= self.options.max_sample_age_s-o.sample_age_s):
            return 'unknown'
        if (self._native_sequence is not None and permit.native_sequence < self._native_sequence
                or self._sequence is not None and o.sample_sequence < self._sequence):
            return 'out_of_order'
        if (permit.native_sequence == self._native_sequence or o.sample_sequence == self._sequence):
            return ('expired_repeat' if self._last_sample_at is not None
                    and o.observed_at_s-self._last_sample_at > self.options.max_sample_age_s else 'repeat')
        return 'new' if o.fresh is True else 'unknown'

    def _interrupt(self, reason, o):
        target = min(self.current_window, self._fallback_window)
        active = self._trial is not None or self._pending is not None
        if active:
            self._early_trial_available = False
        self._trial = self._pending = self._reference = self._anchor = None
        self._pending_work_context = None
        self._windows = []
        self._verified_window = self._measured_window = None
        self._stop_reason = None
        self._direction = 'down' if self._initial_lower_scan else 'up'
        if active:
            self._record(self.evaluations, dict(kind='interrupted_trial', reason=reason,
                comparison_valid=False, actual_window=self.current_window))
        return self._decision(reason, target if active else None, o)

    def _pause_performance(self, reason):
        """Optional work data does not revoke a current resource permission.

        Keep an unconfirmed resource trial reversible on its original fallback.
        Stop scoring and further exploration until actual work data resumes.
        Safety feedback and explicit budgets continue to act independently.
        """
        self._pending = self._pending_work_context = self._reference = self._anchor = None
        self._windows = []
        self._verified_window = self._measured_window = None
        self._stop_reason = None
        return self._decision(reason)

    def _stop_exploration(self, reason, o):
        """End an unfinished trial without erasing a completed comparison."""
        active = self._trial is not None or self._pending is not None
        target = min(self.current_window, self._fallback_window)
        self._exploration_stop_reason = reason
        self._trial = self._pending = self._pending_work_context = None
        self._anchor, self._windows = None, []
        if active:
            self._early_trial_available = False
            if self._reference is not None and self._reference['window'] != target:
                self._reference = None
        return self._decision(reason, target if active else None, o)

    def _pressure(self, o, permit, *, permit_current=True):
        previous = self.current_window
        target = max(1, min(previous//2, permit.request_limit if permit_current else self.maximum))
        self._pressure_ceiling = min(self._pressure_ceiling, max(1, previous-1))
        self._fallback_window = target
        self._trial = self._pending = self._reference = self._anchor = None
        self._pending_work_context = None
        self._verified_window = self._measured_window = None
        self._windows = []
        self._stop_reason = None
        self._recovery_needed, self._recovery_count = True, o.completed_items_total
        self._early_trial_available = False
        self._direction = 'up'
        self._initial_lower_scan = False
        self._censored_frontier = None
        self._record(self.evaluations, dict(kind='resource_pressure', window=previous,
            preemptions_total=o.preemptions_total, memory_pressure=o.memory_pressure,
            rejected_requests_total=permit.rejected_requests_total,
            operational_request_limit=permit.request_limit if permit_current else None, comparison_valid=False,
            response='halve future selections within the current permit; retain all existing work'))
        return self._decision('resource_pressure', target, o)

    def _budget_reason(self, o):
        if not self.options.allow_exploration:
            return 'exploration_disabled'
        if (self.options.exploration_s is not None
                and o.observed_at_s-self.started >= self.options.exploration_s):
            return 'exploration_budget'
        reserve = (int(self._pending['target'] != self.current_window) if self._pending is not None
                   else 1 if self._trial is not None else 0 if self._stop_reason is not None else 2)
        if self.options.max_changes is not None and self._changes+reserve > self.options.max_changes:
            return 'change_budget'
        if o.pending_items is None:
            return 'pending_work_unknown'
        if o.pending_items <= self.options.min_remaining:
            return 'no_pending_work' if not self.options.min_remaining else 'requested_short_tail'
        return None

    def _can_raise(self, target, o, s, permit):
        if target <= self.current_window:
            return True
        # A measured reference return needs actual operational permission, not
        # an idle-resource heuristic. Native queue occupancy alone is not an
        # allocation failure. update_supply has already checked fresh safety,
        # work identity and monotonic counters before any pending selection.
        return (o.fresh is True and not o.transition_pending
            and o.inflight == self.current_window and s.active_requests == self.current_window
            and not o.consumer_backpressure and not request_supply_blocked(s)
            and permit.local_limits_enforced and permit.backend_owns_memory
            and o.pending_items is not None and target <= min(self.maximum,
            permit.request_limit, self.current_window+s.ready_items,
            self.current_window+o.pending_items))

    def _select(self, selection, o, s, permit):
        target = selection['target']
        if not self._can_raise(target, o, s, permit):
            if self._pending is None:
                self._pending_work_context = deepcopy(self._last_point)
            self._pending = selection
            return self._decision('selection_waiting_for_supply')
        # Do not advance selection/verification before the actual request action.
        higher_gain = (selection['gain'] and self._trial is not None
                       and self._trial['candidate'] > self._trial['before'])
        decision = self._decision(selection['reason'], target, o)
        self._pending = self._trial = self._pending_work_context = None
        self._reference = None if selection.get('remeasure_reference') else selection['point']
        self._fallback_window = target
        self._verified_window = target if selection['verified'] else None
        if selection['verified']:
            self._last_verified_window = target
        self._measured_window = target if selection['point'] is not None else None
        self._accepted_steps += int(selection['gain'])
        if higher_gain:
            self._pressure_ceiling = self.maximum
        if selection.get('finish_initial_lower_scan'):
            self._initial_lower_scan = False
            self._direction = 'up'
        self._censored_frontier = deepcopy(selection.get('censored_frontier'))
        self._stop_reason = selection['stop']
        return decision

    def _completed_work(self, o):
        names = ('completed_items_total', 'completed_input_work_total', 'completed_output_work_total',
                 'completed_computed_input_work_total', 'completed_cached_input_work_total')
        counters = tuple(getattr(o, name) for name in names)
        if not self._work_source_valid:
            return 'completed_work_source_unavailable', None
        # A missing optional field cannot hide a known reversal in another.
        # These per-field high values only check monotonicity. Keep the last
        # complete tuple separately so work deltas share one capture interval.
        for value, previous in zip(counters, self._work_known_watermark):
            if type(value) is int and value >= 0 and previous is not None and value < previous:
                self._work_source_valid = False
                return 'completed_work_counter_reversed', None
        for index, value in enumerate(counters):
            if type(value) is int and value >= 0:
                self._work_known_watermark[index] = value
        if any(type(value) is not int or value < 0 for value in counters):
            return 'completed_work_unknown', None
        descriptor = None
        if self._work_watermark is not None:
            delta = tuple(after-before for after, before in zip(counters, self._work_watermark))
            if any(value < 0 for value in delta):
                self._work_source_valid = False
                return 'completed_work_counter_reversed', None
            if (not delta[0] and any(delta[1:]) or delta[3]+delta[4] != delta[1]):
                self._work_source_valid = False
                return 'completed_work_description_invalid', None
            if delta[0]:
                descriptor = dict(zip(('input', 'output', 'computed', 'cached'),
                                      (value/delta[0] for value in delta[1:])))
        self._work_watermark = counters
        return None, descriptor

    def _pending_work_changed(self, descriptor):
        context = self._pending_work_context
        return descriptor is not None and context is not None and any(
            not min(w[name] for w in context['windows']) <= descriptor[name]
                <= max(w[name] for w in context['windows'])
            for name in ('input', 'output', 'computed', 'cached'))

    def _consume_safety(self, o, p, native_status, *, permit_current, guard_status):
        """Safety source progress is independent of native performance progress."""
        if p.safety_sequence is not None:
            if self._guard_seen_sequence is None or p.safety_sequence > self._guard_seen_sequence:
                self._guard_seen_sequence = p.safety_sequence
                self._guard_seen_payload = self._safety_values(p)
                self._guard_seen_rejected = guard_status not in ('new', 'repeat')
            elif p.safety_sequence == self._guard_seen_sequence and guard_status not in ('new', 'repeat'):
                self._guard_seen_rejected = True
            if p.memory_pressure is not None:
                self._guard_memory_bound = True
        if native_status == 'new' and o.memory_pressure is not None:
            self._native_memory_bound = True
        independent_new = guard_status == 'new'
        memory_event = (independent_new and p.memory_pressure is True
                        or native_status == 'new' and o.memory_pressure is True)
        native_memory = o.memory_pressure if native_status in ('new', 'repeat') else None
        guard_memory = p.memory_pressure if guard_status in ('new', 'repeat') else None
        preempted = rejected = False
        if native_status == 'new':
            value = o.preemptions_total
            if value is not None:
                if self._preemptions is None and value > 0:
                    self._preemption_baseline_pending = True
                    self._preemption_baseline_count = o.completed_items_total
                preempted = self._preemptions is not None and value > self._preemptions
                if self._preemptions is not None and value < self._preemptions:
                    self._counter_source_valid = False
                self._preemptions = max(self._preemptions or 0, value)
            self._sequence, self._native_sequence = o.sample_sequence, p.native_sequence
            self._last_sample_at = o.observed_at_s-o.sample_age_s
        elif (native_status == 'repeat' and o.preemptions_total is not None
                and self._preemptions is not None and o.preemptions_total != self._preemptions):
            self._counter_source_valid = False
        rejection_new = independent_new if p.safety_sequence is not None else native_status == 'new'
        if rejection_new and p.rejected_requests_total is not None:
            kind = 'safety' if p.safety_sequence is not None else 'native'
            if self._rejection_source_kind is not None and self._rejection_source_kind != kind:
                self._counter_source_valid = False
            self._rejection_source_kind = kind
            value = p.rejected_requests_total
            rejected = value > (self._rejections or 0)
            if self._rejections is not None and value < self._rejections:
                self._counter_source_valid = False
            self._rejections = max(self._rejections or 0, value)
        elif (guard_status in ('new', 'repeat', 'native')
                and p.rejected_requests_total is not None
                and (self._rejections is None or p.rejected_requests_total != self._rejections)):
            self._counter_source_valid = False
        if independent_new:
            self._safety_sequence = p.safety_sequence
            self._safety_payload = self._safety_values(p)
        self._last_control_at = o.observed_at_s
        pressure = memory_event or preempted or rejected or permit_current and p.request_limit < self.current_window
        unknown = (not permit_current or guard_status not in ('new', 'repeat', 'native')
                   or not self._counter_source_valid or o.preemptions_total is None
                   or not p.backend_owns_memory or not p.local_limits_enforced
                   or (p.rejection_feedback_required or self._rejection_source_kind is not None)
                       and p.rejected_requests_total is None
                   or self._native_memory_bound and native_memory is None
                   or self._guard_memory_bound and guard_memory is None
                   or self.options.require_memory_feedback
                       and native_memory is not False and guard_memory is not False)
        return pressure, ('safety_feedback_'+guard_status if guard_status not in ('new', 'repeat', 'native') else
                          'required_safety_information_unknown' if unknown else
                          'memory_pressure_active' if native_memory is True or guard_memory is True else None)

    def _point(self):
        rows = deepcopy(self._windows)
        return dict(window=self.current_window, windows=rows,
            rate=sum(r['completed'] for r in rows)/sum(r['elapsed_s'] for r in rows),
            rate_min=min(r['rate'] for r in rows), rate_max=max(r['rate'] for r in rows),
            completed_begin=rows[0]['completed_begin'], completed_end=rows[-1]['completed_end'],
            control_phase_id=self._changes,
            scope='query-local delivered useful work; not model-only or causal GPU throughput')

    def _measure(self, o, s, permit):
        names = ('completed_items_total', 'completed_input_work_total', 'completed_output_work_total',
                 'completed_computed_input_work_total', 'completed_cached_input_work_total')
        counters = tuple(getattr(o, name) for name in names)
        if any(type(value) is not int or value < 0 for value in counters):
            return self._interrupt('completed_work_unknown', o)
        now = o.observed_at_s
        if (o.transition_pending or o.inflight != self.current_window
                or s.active_requests != self.current_window):
            self._anchor, self._windows = None, []
            reason = ('draining_previous_window' if o.inflight > self.current_window
                      or s.active_requests is not None and s.active_requests > self.current_window
                      else 'window_not_yet_saturated')
            return self._decision('transition' if o.transition_pending else reason)
        if self._settling:
            self._anchor, self._windows = (now, counters), []
            self._settling = False
            return self._decision('transition_settled')
        if o.consumer_backpressure or request_supply_blocked(s) or not s.ready_items:
            self._anchor, self._windows = None, []
            return self._decision('downstream_measurement_interrupted')
        if self._anchor is None:
            self._anchor = (now, counters)
            return self._decision('priming_measurement')
        elapsed = now-self._anchor[0]
        deltas = tuple(after-before for after, before in zip(counters, self._anchor[1]))
        if elapsed < 0 or any(value < 0 for value in deltas):
            return self._interrupt('completed_work_counter_reversed', o)
        if elapsed < self.options.window_s or deltas[0] < self.options.min_samples:
            return self._decision('collecting_completed_work')
        count = deltas[0]
        if deltas[1]+deltas[2] == 0 or deltas[3]+deltas[4] != deltas[1]:
            return self._interrupt('completed_work_description_invalid', o)
        row = dict(rate=count/elapsed, completed=count, elapsed_s=elapsed,
            input=deltas[1]/count, output=deltas[2]/count,
            computed=deltas[3]/count, cached=deltas[4]/count,
            completed_begin=self._anchor[1][0], completed_end=counters[0],
            begin_s=self._anchor[0]-self.started, end_s=now-self.started)
        self._windows.append(row)
        self._anchor = (now, counters)
        if len(self._windows) < self.options.measurement_windows:
            return self._decision('collecting_independent_windows')
        point = self._point()
        self._last_point = deepcopy(point)
        self._record(self.measurements, deepcopy(point))
        self._windows = []
        self._measured_window = self.current_window
        if self._trial is None:
            # Held-work changes reopen measurements, never import old scores.
            old = self._reference
            work_changed = old is not None and any(
                max(w[name] for w in old['windows']) < min(w[name] for w in point['windows'])
                or max(w[name] for w in point['windows']) < min(w[name] for w in old['windows'])
                for name in ('input', 'computed', 'output'))
            held_changed = (self._stop_reason in ('local_plateau', 'performance_regression', 'operational_limit',
                                     'minimum_measured_window', 'comparison_censored')
                    and old is not None and (work_changed
                        or point['rate_min'] > old['rate_max']*(1+self.options.min_gain)
                        or point['rate_max'] < old['rate_min']*(1-self.options.min_gain)))
            if held_changed:
                if self._censored_frontier is None:
                    self._stop_reason = None
                self._verified_window = None
                self._direction = 'down' if self._initial_lower_scan else 'up'
                self._record(self.evaluations, dict(kind='held_performance_changed', comparison_valid=False))
            if self._stop_reason == 'work_not_comparable':
                self._stop_reason = None
                self._direction = 'down' if self._initial_lower_scan else 'up'
            # Keep the held choice's original comparison context until a real
            # change is detected. Rolling mixed ranges must not absorb it.
            if old is None or self._stop_reason is None or held_changed:
                self._reference = point
            self._fallback_window = self.current_window
            return self._decision('held_window_measured')
        if self._reference is None:
            if self._trial['initial_accelerated']:
                self._initial_lower_scan = True
                self._direction = 'down'
            self._reference, self._fallback_window, self._trial = point, self.current_window, None
            return self._decision('resource_trial_measured_without_gain_claim')
        before = self._reference
        evidence = _local_pair_evidence(before, point, self.options.min_gain)
        self._record(self.evaluations, dict(kind='work_comparison', reference=deepcopy(before),
            candidate=deepcopy(point), evidence=evidence, comparison_valid=evidence['outcome'] is not None))
        outcome = evidence['outcome']
        if outcome is None:
            return self._select(dict(target=before['window'], point=None,
                reason='work_not_comparable', verified=False, gain=False,
                stop='work_not_comparable'), o, s, permit)
        higher = point['window'] > before['window']
        choose_candidate = outcome == 'gain' or outcome == 'no_gain' and not higher
        chosen = point if choose_candidate else before
        finishing_lower = not higher and outcome == 'regression' and self._initial_lower_scan
        continuing_lower = not higher and outcome in ('gain', 'no_gain') and self._initial_lower_scan
        frontier = None
        if continuing_lower:
            reason = 'measured_gain' if outcome == 'gain' else 'smaller_window_without_rate_loss'
            stop = None
        elif finishing_lower:
            reason = 'performance_regression'
            stop = 'performance_regression' if o.waiting else None
        elif higher and outcome in ('no_gain', 'regression') and self._trial['clip_causes']:
            reason, stop = 'comparison_censored', 'comparison_censored'
            frontier = dict(tested_end=max(before['window'], point['window']),
                clip_causes=list(self._trial['clip_causes']),
                selected_window=chosen['window'], source=deepcopy(self._trial['source']),
                work=deepcopy(chosen['windows']))
        elif outcome == 'no_gain':
            reason, stop = 'local_plateau', 'local_plateau'
        elif outcome == 'regression':
            reason = 'performance_regression'
            stop = ('operational_limit' if not higher and self._trial['at_applied_ceiling']
                    else 'performance_regression')
        else:
            reason, stop = 'measured_gain', None
        if stop == 'operational_limit':
            frontier = dict(tested_end=max(before['window'], point['window']),
                clip_causes=['owner'], selected_window=chosen['window'],
                source=deepcopy(self._trial['source']), work=deepcopy(chosen['windows']))
        return self._select(dict(target=chosen['window'], point=chosen, reason=reason,
            verified=True, gain=outcome == 'gain', stop=stop,
            finish_initial_lower_scan=finishing_lower,
            remeasure_reference=chosen is before and stop is None,
            censored_frontier=frontier), o, s, permit)

    def update_supply(self, observation, supply, *, resource_permit=None, physical_samples=(),
                      resource_step=None, resource_opportunity=None):
        """Use raw qualified inputs; a legacy opportunity cannot grant permission."""
        o, s, p = observation, supply, resource_permit
        permit_status = self._permit_status(o, s, p)
        if permit_status in ('unbound', 'unknown_clock'):
            return self._interrupt('safety_feedback_'+permit_status, o)
        status = self._frame_status(o, s, p)
        guard_status = self._guard_status(o, p)
        pressured, safety_reason = self._consume_safety(o, p, status,
            permit_current=permit_status == 'bound', guard_status=guard_status)
        # Receipt progress has its own query-local source. Record a known
        # reversal even when the same update also requires immediate pressure
        # protection; missing work in a protection-only frame is just unknown.
        work_reason, descriptor = self._completed_work(o)
        if pressured:
            return self._pressure(o, p, permit_current=permit_status == 'bound')
        if safety_reason is not None:
            return self._interrupt(safety_reason, o)
        if work_reason is not None and not self._work_source_valid:
            return self._interrupt(work_reason, o)
        if self._pending is not None and self._pending_work_changed(descriptor):
            return self._interrupt('pending_work_changed', o)
        if status not in ('new', 'repeat'):
            return self._interrupt('safety_feedback_'+status, o)
        if (not o.has_service_metrics
                or any(type(value) is not int for value in (o.inflight, o.running, o.waiting))
                or type(o.kv_usage) not in (int, float)):
            return self._interrupt('native_metrics_unknown', o)
        budget = self._budget_reason(o)
        if budget is not None:
            if work_reason is not None:
                self._pause_performance(work_reason)
            return self._stop_exploration(budget, o)
        self._exploration_stop_reason = None
        if work_reason is not None and not (self._early_trial_available
                and self._reference is None and self._trial is None and self._pending is None):
            return self._pause_performance(work_reason)
        if status == 'repeat':
            return self._decision('repeated_native_sample')
        if self._preemption_baseline_pending:
            count = o.completed_items_total
            if self._preemption_baseline_count is None:
                self._preemption_baseline_count = count
            if count is None or self._preemption_baseline_count is None or count <= self._preemption_baseline_count:
                return self._decision('preemption_baseline_pending')
            self._preemption_baseline_pending = False
        if self._recovery_needed:
            count = o.completed_items_total
            if self._recovery_count is None:
                self._recovery_count = count
            if count is None or self._recovery_count is None or count <= self._recovery_count:
                return self._decision('waiting_for_useful_recovery_progress')
            self._recovery_needed = False
            # New useful work after a clear frame permits a new reversible
            # trial, including recovery from pressure at the minimum quota.
            self._pressure_ceiling = max(self._pressure_ceiling,
                min(self.maximum, self.current_window+1))
        if self._pending is not None:
            return self._select(self._pending, o, s, p)
        if self._trial is not None or self._settling:
            return self._measure(o, s, p)
        from .resource_probe import resource_request_opportunity
        identity = self.resource_identity or (None, None)
        opportunity = resource_request_opportunity(o, s, current_window=self.current_window,
            hard_limit=self.maximum, max_age_s=self.options.max_sample_age_s,
            expected_engine_uid=identity[0], expected_session_id=identity[1], permit=p,
            physical_samples=physical_samples, resource_step=resource_step,
            require_memory_feedback=self.options.require_memory_feedback)
        end = min(self.maximum, self._pressure_ceiling, p.request_limit)
        if self._censored_frontier is not None:
            frontier = self._censored_frontier
            measurable_end = min(opportunity.evidence.get('measurable_end', 0), end)
            costed_known = ('costed_ready_prefix' not in frontier['clip_causes']
                            or 'costed_resource_step' in opportunity.evidence)
            if (opportunity.probe_target_window is not None and costed_known
                    and measurable_end > frontier['tested_end']):
                self._record(self.evaluations, dict(kind='measurable_range_expanded',
                    previous=deepcopy(frontier), measurable_end=measurable_end,
                    comparison_valid=False))
                self._censored_frontier = None
                self._reference = self._anchor = None
                self._windows = []
                self._verified_window = self._measured_window = None
                self._stop_reason = None
                self._direction = 'down' if self._initial_lower_scan else 'up'
                self._settling = True
                return self._measure(o, s, p)
            self._stop_reason = self._stop_reason or 'comparison_censored'
        if self._reference is None and not self._early_trial_available:
            return self._measure(o, s, p)
        if self._stop_reason is not None:
            return self._measure(o, s, p)
        target = opportunity.probe_target_window
        if target is not None:
            target = min(target, end)
            if not self._early_trial_available:
                # Resource positioning can accelerate the first runnable trial.
                # Subsequent comparisons keep geometric spacing; utilization
                # cannot repeatedly skip the intervening measured positions.
                target = min(target, self.current_window*2)
        if self._direction == 'down':
            target = None
            if (self._reference is not None and s.ready_items and not o.transition_pending
                    and not o.consumer_backpressure and not request_supply_blocked(s)):
                if self.current_window == 1:
                    self._initial_lower_scan = False
                    self._stop_reason = 'minimum_measured_window'
                    return self._measure(o, s, p)
                target = max(1, self.current_window//2)
        if (target is None or target <= self.current_window) and self._reference is not None:
            # At an applied ceiling, test a smaller position with original work.
            # A declared maximum alone never certifies a performance plateau.
            if ((self.current_window >= end or o.waiting) and self.current_window > 1
                    and not o.consumer_backpressure and not request_supply_blocked(s)
                    and s.ready_items and not o.transition_pending):
                target = max(1, self.current_window//2)
                self._direction = 'down'
        if target is not None and target != self.current_window:
            initial_accelerated = self._early_trial_available and target > self.current_window*2
            self._early_trial_available = False
            clip_causes = list(opportunity.evidence.get('clip_causes', ()))
            if end < min(self.maximum, self.current_window*2):
                clip_causes.append('pressure' if end == self._pressure_ceiling else 'owner')
            self._trial = dict(before=self.current_window, candidate=target,
                initial_accelerated=initial_accelerated, clip_causes=sorted(set(clip_causes)),
                source=deepcopy(opportunity.evidence.get('source')),
                at_applied_ceiling=self.current_window >= min(self.maximum, p.request_limit))
            self._record(self.step_decisions, dict(before=self.current_window, target=target,
                reason=opportunity.reason, evidence=deepcopy(opportunity.evidence),
                resource_suggested_target=opportunity.probe_target_window,
                performance_reference_present=self._reference is not None,
                verified_gain=False))
            return self._decision('resource_trial' if self._reference is None else 'performance_trial', target, o)
        return self._measure(o, s, p)

    def report(self):
        return dict(strategy=self.strategy, control_unit=self.control_unit,
            initial_window=self._initial_window, maximum=self.maximum, window=self.current_window,
            current_admission_window=self.current_window, recommended_window=self._verified_window,
            measured_window=self._measured_window, verified_performance_window=self._verified_window,
            accepted_steps=self._accepted_steps, changes=self._changes, reasons=dict(self.reasons),
            stop_reason=self._stop_reason or self._exploration_stop_reason,
            performance_stop_reason=self._stop_reason, exploration_stop_reason=self._exploration_stop_reason,
            last_verified_performance_window=self._last_verified_window,
            active_unverified_trial=self._trial is not None,
            counter_source_valid=self._counter_source_valid, work_source_valid=self._work_source_valid,
            can_continue=False,
            pending_selection=deepcopy(self._pending),
            smaller_positions_pending=self._initial_lower_scan,
            censored_frontier=deepcopy(self._censored_frontier),
            physical_capacity_known=False, learned_global_optimum=False,
            local_plateau_confirmed=self._stop_reason == 'local_plateau' and self._verified_window is not None,
            exploration_censored=self._stop_reason != 'local_plateau',
            cross_query_hints_supported=False, options=asdict(self.options),
            history=deepcopy(self.history), measurements=deepcopy(self.measurements),
            evaluations=deepcopy(self.evaluations), resource_trials=deepcopy(self.step_decisions),
            diagnostic_record_limit=self._trace_limit,
            performance_scope='disjoint query-local work matches; full-query benefit remains unverified')
