"""CPU event sequences for resource trials and separate performance evidence."""

from dataclasses import replace
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.adaptive import ResourceRequestConfig
from src.scheduling.submission_control.request_scan import ResourceRequestController
from src.scheduling.submission_control.resource_probe import ResourceRequestPermit, ResourceProbeView
from src.scheduling.submission_control.stage_work import SupplyState


class ResourceTrace:
    def __init__(self, maximum=2000, options=None):
        self.now = 10.
        self.sequence = 0
        self.completed = self.input = self.output = self.computed = self.cached = 0
        self.controller = ResourceRequestController(maximum, options, clock=lambda: self.now)
        self.controller.reset(1, 100000, resource_identity=('CPU-engine', 7))
        self.maximum = maximum
        self.permit = None
        self.guard_times = {}

    def tick(self, count=0, *, elapsed=.25, work=(100, 4, 80, 20),
             observation=None, supply=None, permit=None, physical=False, resource_step=None,
             physical_count=2):
        self.now += elapsed
        self.sequence += 1
        self.completed += count
        for name, value in zip(('input', 'output', 'computed', 'cached'), work):
            setattr(self, name, getattr(self, name)+count*value)
        window = self.controller.current_window
        o = AdmissionObservation(self.now, True, window, window, 0, .90,
            sample_age_s=0., sample_sequence=self.sequence, preemptions_total=0,
            completed_items_total=self.completed, pending_items=10000,
            completed_input_work_total=self.input, completed_output_work_total=self.output,
            completed_computed_input_work_total=self.computed,
            completed_cached_input_work_total=self.cached)
        s = SupplyState(self.now, 5000, self.completed+5000, self.completed+window,
            1, True, False, active_requests=window)
        p = ResourceRequestPermit('CPU-engine', 7, self.sequence, self.sequence, self.now,
            self.maximum, local_limits_enforced=True, backend_owns_memory=True)
        if permit is not False:
            permit = dict(permit or {})
            if permit.get('safety_sequence') is not None:
                capture = permit.get('safety_observed_at_s', self.now)
                permit.setdefault('safety_observed_at_s', self.guard_times.setdefault(permit['safety_sequence'], capture))
        o, s, p = (replace(o, **(observation or {})), replace(s, **(supply or {})),
                   None if permit is False else replace(p, **(permit or {})))
        views = ()
        if physical:
            busy = (20., 20.) if physical is True else physical
            views = tuple(ResourceProbeView('CPU-engine', 7, self.sequence*2+i,
                self.now-.20+i*.05, self.now-.10+i*.05, .01,
                ('GPU-cpu-fixture',), *busy) for i in range(physical_count))
        self.permit = p
        step = resource_step(o, s, p) if callable(resource_step) else resource_step
        return self.controller.update_supply(o, s, resource_permit=p, physical_samples=views,
            resource_step=step)

    def until(self, predicate, *, count=40, limit=120, **kwargs):
        for _ in range(limit):
            before = self.controller.current_window
            result = self.tick(count(before) if callable(count) else count, elapsed=1., **kwargs)
            if predicate(self.controller.report(), result):
                return result
        raise AssertionError(self.controller.report())


class ResourceRequestTests(unittest.TestCase):
    def test_resource_trial_can_jump_before_any_reference_or_completed_work(self):
        trace = ResourceTrace()
        decision = trace.tick(physical=True)
        self.assertGreater(decision.window, 128)
        self.assertLessEqual(decision.window, 2000)
        report = trace.controller.report()
        self.assertEqual(report['accepted_steps'], 0)
        self.assertIsNone(report['verified_performance_window'])
        self.assertIsNone(report['measured_window'])
        self.assertIsNone(trace.controller.export_hint())

    def test_fifty_percent_device_or_host_busy_does_not_disqualify_a_resource_trial(self):
        for busy in ((49., 49.), (50., 50.), (90., 70.)):
            with self.subTest(busy=busy):
                trace = ResourceTrace()
                self.assertGreater(trace.tick(physical=busy).window, 128)

    def test_KV_ninety_nine_is_not_allocation_failure(self):
        trace = ResourceTrace()
        self.assertGreater(trace.tick(physical=True, observation={'kv_usage': .99}).window, 128)
        self.assertNotIn('resource_pressure', trace.controller.report()['reasons'])

    def test_current_owner_limit_and_actual_ready_prefix_bound_the_trial(self):
        trace = ResourceTrace()
        result = trace.tick(physical=True, permit={'request_limit': 300}, supply={'ready_items': 299})
        self.assertGreater(result.window, 128)
        self.assertLessEqual(result.window, 300)
        self.assertFalse(trace.controller.report()['physical_capacity_known'])

    def test_no_physical_evidence_uses_only_a_reversible_native_trial(self):
        trace = ResourceTrace()
        result = trace.tick()
        self.assertEqual(result.window, 2)
        self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_unbound_unknown_or_foreign_safety_information_cannot_authorize_growth(self):
        for changes in (False, {'engine_uid': 'foreign'}, {'session_id': 8},
                        {'sample_sequence': 2}, {'observed_at_s': 0.},
                        {'local_limits_enforced': False}, {'backend_owns_memory': False},
                        {'rejection_feedback_required': True}, {'request_limit': 1}):
            with self.subTest(changes=changes):
                trace = ResourceTrace()
                self.assertEqual(trace.tick(permit=changes, physical=True).window, 1)
                self.assertEqual(trace.controller.report()['changes'], 0)

    def test_missing_preemption_or_requested_memory_signal_is_unknown(self):
        trace = ResourceTrace()
        self.assertEqual(trace.tick(observation={'preemptions_total': None}, physical=True).window, 1)
        trace = ResourceTrace(options=ResourceRequestConfig(require_memory_feedback=True))
        self.assertEqual(trace.tick(physical=True).window, 1)
        self.assertGreater(trace.tick(observation={'memory_pressure': False}, physical=True).window, 128)

    def test_no_legal_ready_work_no_saturation_or_downstream_room_means_no_trial(self):
        cases = (({}, {'ready_items': 0}), ({'inflight': 0}, {'active_requests': 0}),
                 ({'waiting': 1}, {}), ({'consumer_backpressure': True}, {}),
                 ({}, {'capacity_blocked': 'result_bytes'}))
        for observation, supply in cases:
            with self.subTest(observation=observation, supply=supply):
                trace = ResourceTrace()
                self.assertEqual(trace.tick(observation=observation, supply=supply, physical=True).window, 1)

    def test_explicit_pressure_preempts_the_measurement_clock_and_is_not_reapplied(self):
        for signal in ({'preemptions_total': 1}, {'memory_pressure': True}):
            with self.subTest(signal=signal):
                trace = ResourceTrace()
                before = trace.tick(physical=True).window
                decision = trace.tick(elapsed=.001, observation=signal)
                self.assertLess(decision.window, before)
                self.assertEqual(decision.reason, 'resource_pressure')
                changes = trace.controller.report()['changes']
                result = trace.tick(elapsed=.001, observation=dict(signal, sample_sequence=2),
                    permit={'sample_sequence': 2, 'native_sequence': 2})
                self.assertEqual(result.window, decision.window)
                self.assertEqual(trace.controller.report()['changes'], changes)
                self.assertEqual(trace.controller.sampling_interval_s(0), .25)

    def test_backend_rejection_is_explicit_pressure_not_a_KV_proxy(self):
        trace = ResourceTrace()
        trace.tick(physical=True, permit={'rejection_feedback_required': True, 'rejected_requests_total': 0})
        result = trace.tick(elapsed=.001,
            permit={'rejection_feedback_required': True, 'rejected_requests_total': 1})
        self.assertEqual(result.reason, 'resource_pressure')
        self.assertEqual(trace.controller.report()['accepted_steps'], 0)

    def test_cached_native_sample_cannot_be_relabelled_as_new_by_an_RPC_poll(self):
        trace = ResourceTrace()
        trace.tick(physical=True)
        changes = trace.controller.report()['changes']
        for _ in range(8):
            trace.tick(40, elapsed=1., permit={'native_sequence': 1})
        report = trace.controller.report()
        self.assertLessEqual(report['changes'], changes+1)
        self.assertEqual(report['window'], 1)
        self.assertIsNone(report['measured_window'])
        self.assertEqual(report['accepted_steps'], 0)

    def test_expired_or_unknown_feedback_rolls_back_only_the_unverified_increase(self):
        for signal in ({'fresh': False}, {'sample_age_s': 2.}, {'preemptions_total': None}):
            with self.subTest(signal=signal):
                trace = ResourceTrace()
                trace.tick(physical=True)
                result = trace.tick(10000, observation=signal)
                self.assertEqual(result.window, 1)
                self.assertEqual(trace.controller.report()['accepted_steps'], 0)
                self.assertIsNone(trace.controller.export_hint())

    def test_gain_is_recorded_only_after_disjoint_actual_work_windows(self):
        trace = ResourceTrace(maximum=16)
        trace.until(lambda r, _: r['accepted_steps'] > 0, count=lambda c: 40*c)
        report = trace.controller.report()
        self.assertIsNotNone(report['verified_performance_window'])
        for evaluation in report['evaluations']:
            if evaluation['kind'] != 'work_comparison':
                continue
            self.assertNotEqual(evaluation['reference']['window'], evaluation['candidate']['window'])
            self.assertLessEqual(evaluation['reference']['completed_end'], evaluation['candidate']['completed_begin'])
        self.assertIsNone(trace.controller.export_hint())

    def test_equal_work_and_rate_can_select_a_smaller_measured_window(self):
        trace = ResourceTrace(maximum=8)
        trace.until(lambda r, _: r['stop_reason'] == 'local_plateau')
        report = trace.controller.report()
        self.assertEqual(report['window'], 2)
        self.assertEqual(report['verified_performance_window'], 2)
        self.assertFalse(report['learned_global_optimum'])

    def test_incomparable_work_does_not_turn_apparent_speed_into_verified_gain(self):
        trace = ResourceTrace(maximum=8)
        trace.until(lambda r, _: r['measured_window'] == 2)
        for _ in range(18):
            quota = trace.controller.current_window
            trace.tick(40*quota, elapsed=1., work=(100, 4, 80, 20) if quota == 2 else (200, 8, 40, 160))
        self.assertEqual(trace.controller.report()['accepted_steps'], 0)
        self.assertIsNone(trace.controller.export_hint())

    def test_empty_or_short_query_does_not_invent_a_reference_or_horizon(self):
        for changes in ({'pending_items': 0}, {'pending_items': None}):
            with self.subTest(changes=changes):
                trace = ResourceTrace()
                for _ in range(4): trace.tick(observation=changes, physical=True)
                report = trace.controller.report()
                self.assertEqual(report['window'], 1)
                self.assertIsNone(report['verified_performance_window'])
                self.assertIsNone(trace.controller.export_hint())

    def test_explicit_query_budget_does_not_disable_later_pressure_sampling(self):
        trace = ResourceTrace(options=ResourceRequestConfig(exploration_s=.1))
        self.assertEqual(trace.tick(physical=True).window, 1)
        result = trace.tick(observation={'memory_pressure': True})
        self.assertEqual(result.reason, 'resource_pressure')
        self.assertEqual(trace.controller.sampling_interval_s(0), .25)

    def test_configuration_has_no_hidden_scan_or_capacity_budget(self):
        config = ResourceRequestConfig()
        self.assertIsNone(config.exploration_s)
        self.assertIsNone(config.max_changes)
        self.assertEqual(config.min_remaining, 0)
        for key, value in (('min_samples', True), ('window_s', float('nan')),
                           ('min_gain', True), ('measurement_windows', 1),
                           ('require_memory_feedback', 1)):
            with self.subTest(key=key), self.assertRaises(ValueError):
                replace(config, **{key: value})

    def test_no_historical_AIMD_parent_or_threshold_attributes_are_used(self):
        from src.scheduling.submission_control.adaptive import AimdAdmissionController
        controller = ResourceTrace().controller
        self.assertNotIsInstance(controller, AimdAdmissionController)
        self.assertFalse(hasattr(controller.options, 'congestion_kv_usage'))
        self.assertFalse(hasattr(controller.options, 'low_load_kv_usage'))

    def test_source_and_setting_types_are_validated_before_use(self):
        permit = ResourceTrace()
        permit.tick()
        for key, value in (('sample_sequence', 0), ('native_sequence', True),
                           ('request_limit', -1), ('request_limit', True),
                           ('rejected_requests_total', True), ('backend_owns_memory', 1)):
            with self.subTest(key=key), self.assertRaises(ValueError):
                replace(permit.permit, **{key: value})
        controller = ResourceTrace().controller
        with self.assertRaises(ValueError): controller.reset(2, 100)
        with self.assertRaises(ValueError): controller.reset(1, 100, search_hint={})

    def test_lower_trial_cannot_measure_the_remaining_requests_from_a_larger_window(self):
        trace = ResourceTrace(maximum=4)
        trace.tick(physical=(0., 0.))
        trace.until(lambda r, _: r['measured_window'] == 4, count=160)
        trace.until(lambda r, _: r['window'] == 2, count=160)
        measurements = len(trace.controller.measurements)
        for _ in range(6):
            result = trace.tick(160, elapsed=1., observation={'inflight': 4}, supply={'active_requests': 4})
            self.assertEqual(result.reason, 'draining_previous_window')
        self.assertEqual(len(trace.controller.measurements), measurements)
        self.assertEqual(trace.controller.report()['accepted_steps'], 0)

    def blocked_return(self):
        trace = ResourceTrace(maximum=4)
        trace.tick(physical=(0., 0.))
        trace.until(lambda r, _: r['measured_window'] == 4, count=lambda c: 40*c)
        trace.until(lambda r, _: r['window'] == 2, count=lambda c: 40*c)
        trace.until(lambda r, _: r['pending_selection'] is not None,
                    count=80, supply={'ready_items': 1})
        return trace

    def test_denied_upward_return_keeps_the_actual_phase_and_retries_without_recursion(self):
        trace = self.blocked_return()
        changes = trace.controller.report()['changes']
        for _ in range(20):
            decision = trace.tick(80, elapsed=1., supply={'ready_items': 1})
            self.assertEqual(decision.window, 2)
            self.assertEqual(trace.controller.report()['changes'], changes)
            self.assertIsNone(trace.controller.report()['verified_performance_window'])
        decision = trace.tick(80, elapsed=1.)
        self.assertEqual(decision.window, 4)
        self.assertEqual(trace.controller.report()['verified_performance_window'], 4)
        self.assertIsNone(trace.controller.report()['pending_selection'])
        self.assertIsNone(trace.controller.export_hint())

    def test_explicit_pressure_always_preempts_a_waiting_reference_return(self):
        trace = self.blocked_return()
        result = trace.tick(0, elapsed=.001, observation={'preemptions_total': 1}, supply={'ready_items': 1})
        self.assertEqual((result.window, result.reason), (1, 'resource_pressure'))
        self.assertIsNone(trace.controller.report()['pending_selection'])
        self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_unknown_or_tail_feedback_never_forces_an_upward_reference_return(self):
        for signal in ({'preemptions_total': None}, {'fresh': False}, {'pending_items': 0}):
            with self.subTest(signal=signal):
                trace = self.blocked_return()
                self.assertEqual(trace.tick(1000, observation=signal).window, 2)
                self.assertIsNone(trace.controller.report()['pending_selection'])
                self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_pressure_recovery_requires_new_original_progress(self):
        trace = ResourceTrace()
        trace.tick(physical=True)
        trace.tick(observation={'preemptions_total': 1})
        before = trace.controller.current_window
        for _ in range(4):
            result = trace.tick(observation={'preemptions_total': 1}, physical=True)
            self.assertEqual(result.window, before)
        trace.until(lambda r, _: r['window'] > before, count=100,
                    observation={'preemptions_total': 1}, physical=True)

    def test_counter_rollback_and_out_of_order_source_cannot_certify_a_gain(self):
        trace = ResourceTrace()
        trace.tick(physical=True, observation={'preemptions_total': 3})
        result = trace.tick(1000, observation={'preemptions_total': 2})
        self.assertEqual(result.window, 1)
        self.assertEqual(trace.controller.report()['accepted_steps'], 0)
        trace = ResourceTrace()
        trace.tick(physical=True)
        trace.tick(40)
        result = trace.tick(1000, permit={'native_sequence': 1})
        self.assertEqual(result.window, 1)
        self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_miswired_generic_gate_and_unbound_query_are_rejected_explicitly(self):
        controller = ResourceTrace().controller
        with self.assertRaisesRegex(ValueError, 'explicitly bound'):
            controller.reset(1, 100)
        with self.assertRaisesRegex(RuntimeError, 'update_supply'):
            controller.update(None)

    def test_feedback_freshness_is_independent_of_the_throughput_measurement_window(self):
        trace = ResourceTrace(options=ResourceRequestConfig(window_s=30., max_sample_age_s=1.))
        trace.tick(physical=True)
        result = trace.tick(observation={'memory_pressure': True, 'sample_age_s': 2.})
        self.assertEqual(result.window, 1)
        self.assertNotEqual(result.reason, 'resource_pressure')
        trace = ResourceTrace(options=ResourceRequestConfig(window_s=30., max_sample_age_s=1.))
        before = trace.tick(physical=True).window
        result = trace.tick(elapsed=.001, observation={'preemptions_total': 1})
        self.assertLess(result.window, before)
        self.assertEqual(result.reason, 'resource_pressure')

    def test_a_faster_lower_trial_continues_downward_on_original_work(self):
        trace = ResourceTrace(maximum=16)
        trace.tick(physical=(0., 0.))
        trace.until(lambda r, _: r['stop_reason'] == 'minimum_measured_window',
                    count=lambda c: 1600//c)
        report = trace.controller.report()
        self.assertEqual(report['window'], 1)
        self.assertEqual(report['verified_performance_window'], 1)
        self.assertEqual([row['window'] for row in report['history']], [16, 8, 4, 2, 1])
        self.assertFalse(report['learned_global_optimum'])

    def test_Core_keeps_existing_tasks_and_bytes_until_their_actual_settlement(self):
        from src.scheduling.submission_control.admission import StaticAdmissionController
        from tests.scheduling.test_incremental_session import setup, task
        trace = ResourceTrace(maximum=4)
        trace.tick(physical=(0., 0.))
        controller = trace.controller
        engine, session, backend, clock = setup(held_tasks=5, input_bytes=20, result_bytes=20,
            active_requests=4, active_work=4, offer_tasks=5, step_actions=64)

        class SelectedWindow:
            def decide(self, inflight, **kwargs):
                return StaticAdmissionController(controller.current_window).decide(inflight, **kwargs)

        engine.policies = replace(engine.policies, admission=SelectedWindow())
        self.assertEqual(session.offer(tuple(task(index) for index in range(5))).accepted_prefix_count, 5)
        session.seal()
        session.advance(5)
        before = engine.capacity.usage()
        self.assertEqual(before.active_requests, 4)
        trace.tick(observation={'preemptions_total': 1})
        session.advance(5)
        self.assertEqual(engine.capacity.usage(), before)
        self.assertEqual(backend.cancelled, [])
        delivered = []
        while backend.pending:
            for key in tuple(backend.pending): backend.complete(key)
            clock.now += .1
            values = session.advance(5).deliveries
            delivered.extend(values)
            # Delivery still owns its input/result reservation until release.
            self.assertGreaterEqual(engine.capacity.usage().held_tasks, len(values))
            session.release(tuple(value.lease_id for value in values))
        self.assertEqual({value.key.sequence for value in delivered}, set(range(5)))
        self.assertEqual(len(delivered), 5)
        usage = engine.capacity.usage()
        self.assertEqual((usage.held_tasks, usage.input_bytes, usage.result_bytes, usage.active_requests), (0, 0, 0, 0))
        self.assertEqual(backend.cancelled, [])
        session.close()

    def test_uncertain_Core_results_do_not_refund_occupancy_when_the_window_shrinks(self):
        from src.scheduling.core.session_contract import Uncertain
        from tests.scheduling.test_incremental_session import setup, task
        trace = ResourceTrace(maximum=4)
        trace.tick(physical=(0., 0.))
        engine, session, backend, _clock = setup(held_tasks=4, input_bytes=16, result_bytes=16,
            active_requests=4, active_work=4, offer_tasks=4, step_actions=64)
        session.offer(tuple(task(index) for index in range(4)))
        session.advance(4)
        key = next(iter(backend.pending))
        backend.events.append(Uncertain(key, backend.pending[key][0]))
        engine.reap(4)
        before = engine.capacity.usage()
        self.assertEqual(before.active_requests, 4)
        trace.tick(observation={'preemptions_total': 1})
        self.assertEqual(engine.capacity.usage(), before)
        session.close()
        self.assertEqual(engine.capacity.usage().active_requests, 4)
        for key in tuple(backend.pending): backend.complete(key)
        engine.reap(4)
        usage = engine.capacity.usage()
        self.assertEqual((usage.active_requests, usage.held_tasks, usage.input_bytes, usage.result_bytes), (0, 0, 0, 0))

    def test_held_work_changes_invalidate_the_previous_performance_selection(self):
        trace = ResourceTrace(maximum=8)
        trace.until(lambda r, _: r['stop_reason'] == 'local_plateau')
        self.assertEqual(trace.controller.report()['verified_performance_window'], 2)
        trace.until(lambda r, _: any(row['kind'] == 'held_performance_changed' for row in r['evaluations']),
                    work=(200, 8, 160, 40))
        self.assertIsNone(trace.controller.report()['verified_performance_window'])
        self.assertIsNone(trace.controller.export_hint())

    def test_a_larger_actual_operational_permission_reopens_a_censored_scan(self):
        trace = ResourceTrace(maximum=24)
        trace.tick(physical=(0., 0.), permit={'request_limit': 8})
        trace.until(lambda r, _: r['stop_reason'] == 'operational_limit', count=lambda c: 40*c,
                    permit={'request_limit': 8})
        self.assertEqual(trace.controller.current_window, 8)
        trace.until(lambda r, _: r['window'] > 8, count=lambda c: 40*c, physical=True)
        self.assertFalse(trace.controller.report()['physical_capacity_known'])

    def test_pressure_at_the_minimum_does_not_leave_a_permanent_one_request_ceiling(self):
        trace = ResourceTrace(maximum=8)
        trace.tick(observation={'memory_pressure': True})
        trace.until(lambda r, _: r['window'] > 1, count=40,
                    observation={'memory_pressure': False}, physical=True, limit=20)
        self.assertEqual(trace.controller.report()['accepted_steps'], 0)

    def test_actual_pending_work_arriving_after_unknown_feedback_can_start_a_trial(self):
        trace = ResourceTrace()
        trace.tick(observation={'pending_items': None}, physical=True)
        self.assertGreater(trace.tick(physical=True).window, 128)
        self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_requested_wall_budget_can_end_a_trial_even_before_another_native_sample(self):
        trace = ResourceTrace(options=ResourceRequestConfig(exploration_s=.5))
        trace.tick(physical=True)
        result = trace.tick(elapsed=.30, permit={'native_sequence': 1})
        self.assertEqual((result.window, result.reason), (1, 'exploration_budget'))

    def test_new_safety_or_permission_information_precedes_native_sample_deduplication(self):
        cases = (({'memory_pressure': True}, {}), ({}, {'request_limit': 1}),
                 ({}, {'backend_owns_memory': False}),
                 ({}, {'rejection_feedback_required': True, 'rejected_requests_total': 1}))
        for observation, permit in cases:
            with self.subTest(observation=observation, permit=permit):
                trace = ResourceTrace()
                before = trace.tick(physical=True).window
                result = trace.tick(elapsed=.001, observation=observation,
                                    permit=dict(permit, native_sequence=1))
                self.assertLess(result.window, before)
                self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_first_positive_preemption_counter_needs_a_query_baseline_and_new_progress(self):
        trace = ResourceTrace()
        for _ in range(3):
            result = trace.tick(physical=True, observation={'preemptions_total': 7})
            self.assertEqual(result.window, 1)
        result = trace.tick(1, physical=True, observation={'preemptions_total': 7})
        self.assertGreater(result.window, 128)
        self.assertEqual(trace.controller.report()['accepted_steps'], 0)

    def test_pending_return_does_not_accept_missing_or_reset_completed_work(self):
        for values in ({'completed_input_work_total': None},
                       dict.fromkeys(('completed_items_total', 'completed_input_work_total',
                            'completed_output_work_total', 'completed_computed_input_work_total',
                            'completed_cached_input_work_total'), 0)):
            with self.subTest(values=values):
                trace = self.blocked_return()
                self.assertEqual(trace.tick(80, observation=values).window, 2)
                self.assertIsNone(trace.controller.report()['verified_performance_window'])
                self.assertIsNone(trace.controller.report()['pending_selection'])

    def test_pending_return_is_cancelled_when_waiting_work_changes(self):
        trace = self.blocked_return()
        trace.tick(80, work=(200, 8, 40, 160), supply={'ready_items': 1})
        self.assertIsNone(trace.controller.report()['pending_selection'])
        self.assertIsNone(trace.controller.report()['verified_performance_window'])
        self.assertEqual(trace.controller.current_window, 2)

    def test_query_tail_stops_new_trials_without_erasing_a_completed_comparison(self):
        trace = ResourceTrace(maximum=8)
        trace.until(lambda r, _: r['stop_reason'] == 'local_plateau')
        result = trace.tick(observation={'pending_items': 0})
        report = trace.controller.report()
        self.assertEqual(result.window, 2)
        self.assertEqual(report['verified_performance_window'], 2)
        self.assertTrue(report['local_plateau_confirmed'])
        self.assertEqual(report['exploration_stop_reason'], 'no_pending_work')

    def test_change_budget_reserves_only_the_actions_remaining_in_an_existing_trial(self):
        trace = ResourceTrace(maximum=16, options=ResourceRequestConfig(max_changes=4))
        trace.until(lambda r, _: r['stop_reason'] == 'change_budget', count=lambda c: 40*c)
        report = trace.controller.report()
        self.assertEqual(report['window'], 8)
        self.assertEqual(report['verified_performance_window'], 8)
        self.assertGreaterEqual(report['accepted_steps'], 2)
        self.assertLessEqual(report['changes'], 4)

    def test_still_recent_consumed_native_frame_is_held_without_becoming_a_new_sample(self):
        trace = ResourceTrace()
        before = trace.tick(physical=True).window
        result = trace.tick(elapsed=.1,
            observation={'fresh': False, 'sample_sequence': 1, 'sample_age_s': .1},
            permit={'sample_sequence': 1, 'native_sequence': 1})
        self.assertEqual((result.window, result.reason), (before, 'repeated_native_sample'))
        self.assertIsNone(trace.controller.report()['measured_window'])

    def test_independent_memory_guard_can_react_without_any_new_native_frame(self):
        trace = ResourceTrace()
        before = trace.tick(physical=True).window
        values = {'fresh': False, 'sample_sequence': None, 'sample_age_s': None,
                  'running': None, 'waiting': None, 'kv_usage': None, 'preemptions_total': None}
        permit = {'sample_sequence': None, 'native_sequence': None,
                  'safety_sequence': 20, 'memory_pressure': True}
        result = trace.tick(elapsed=.001, observation=values, permit=permit)
        self.assertEqual((result.window, result.reason), (1, 'resource_pressure'))
        changes = trace.controller.report()['changes']
        for _ in range(3): trace.tick(elapsed=.001, observation=values, permit=permit)
        self.assertEqual(trace.controller.report()['changes'], changes)

    def test_rejection_guard_is_fresh_on_its_own_source_without_refreshing_native_metrics(self):
        trace = ResourceTrace()
        before = trace.tick(physical=True, permit={'safety_sequence': 10,
            'rejection_feedback_required': True, 'rejected_requests_total': 0}).window
        result = trace.tick(elapsed=.001, observation={'fresh': False, 'sample_sequence': 1},
            permit={'sample_sequence': 1, 'native_sequence': 1, 'safety_sequence': 11,
                    'rejection_feedback_required': True, 'rejected_requests_total': 1})
        self.assertEqual((result.window, result.reason), (1, 'resource_pressure'))
        self.assertIsNone(trace.controller.report()['measured_window'])

    def test_unknown_required_guard_or_lost_permission_is_checked_before_cached_native_metrics(self):
        cases = ({'safety_sequence': 11, 'memory_pressure': None},
                 {'safety_sequence': 11, 'memory_pressure': False, 'backend_owns_memory': False},
                 {'safety_sequence': 11, 'memory_pressure': False, 'local_limits_enforced': False})
        for changes in cases:
            with self.subTest(changes=changes):
                trace = ResourceTrace(options=ResourceRequestConfig(require_memory_feedback=True))
                trace.tick(physical=True, permit={'safety_sequence': 10, 'memory_pressure': False})
                result = trace.tick(elapsed=.001, observation={'fresh': False, 'sample_sequence': 1},
                    permit=dict(changes, sample_sequence=1, native_sequence=1))
                self.assertEqual(result.window, 1)
                self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_an_independent_guard_sequence_cannot_refresh_its_age_with_RPC_time(self):
        trace = ResourceTrace(options=ResourceRequestConfig(require_memory_feedback=True))
        trace.tick(physical=True, permit={'safety_sequence': 10, 'memory_pressure': False})
        result = trace.tick(elapsed=1.01, permit={'safety_sequence': 10, 'memory_pressure': False})
        self.assertEqual(result.window, 1)
        self.assertIn('expired_safety_sample', result.reason)

    def test_native_cache_lifetime_starts_at_its_actual_source_age(self):
        trace = ResourceTrace()
        trace.tick(physical=True, observation={'sample_age_s': .8})
        result = trace.tick(elapsed=.3, permit={'native_sequence': 1})
        self.assertEqual(result.window, 1)
        self.assertIn('expired_repeat', result.reason)

    def test_same_guard_identity_with_different_contents_is_unknown_not_a_new_read(self):
        trace = ResourceTrace(options=ResourceRequestConfig(require_memory_feedback=True))
        trace.tick(physical=True, permit={'safety_sequence': 10, 'memory_pressure': False})
        result = trace.tick(elapsed=.001, permit={'safety_sequence': 10, 'memory_pressure': True})
        self.assertEqual(result.window, 1)
        self.assertIn('conflicting_safety_sample', result.reason)

    def test_monotonic_counter_violation_cannot_be_repaired_by_catching_up_with_old_numbers(self):
        trace = self.blocked_return()
        names = ('completed_items_total', 'completed_input_work_total', 'completed_output_work_total',
                 'completed_computed_input_work_total', 'completed_cached_input_work_total')
        trace.tick(observation=dict.fromkeys(names, 0))
        for _ in range(8):
            self.assertEqual(trace.tick(80, elapsed=1., physical=True).window, 2)
        self.assertFalse(trace.controller.report()['work_source_valid'])
        self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_a_finished_first_resource_trial_is_not_rolled_back_by_a_spent_action_budget(self):
        trace = ResourceTrace(maximum=16, options=ResourceRequestConfig(max_changes=2))
        trace.until(lambda r, _: r['stop_reason'] == 'change_budget', count=40)
        report = trace.controller.report()
        self.assertEqual(report['window'], 2)
        self.assertEqual(report['measured_window'], 2)
        self.assertEqual(report['changes'], 1)
        self.assertIsNone(report['verified_performance_window'])

    def test_a_stale_auxiliary_guard_cannot_hide_new_native_preemption(self):
        trace = ResourceTrace(maximum=16)
        trace.until(lambda r, _: r['accepted_steps'] > 0, count=lambda c: 40*c)
        trace.tick(observation={'waiting': 1}, permit={'safety_sequence': 10, 'memory_pressure': False})
        before = trace.controller.current_window
        result = trace.tick(elapsed=1.01, observation={'preemptions_total': 1},
                            permit={'safety_sequence': 10, 'memory_pressure': False})
        self.assertEqual((result.window, result.reason), (before//2, 'resource_pressure'))

    def test_new_native_preemption_does_not_require_a_new_operational_permit(self):
        trace = ResourceTrace(maximum=16)
        trace.until(lambda r, _: r['accepted_steps'] > 0, count=lambda c: 40*c)
        trace.tick(observation={'waiting': 1})
        before = trace.controller.current_window
        result = trace.tick(elapsed=.001, observation={'preemptions_total': 1},
                            permit={'observed_at_s': 0., 'request_limit': 0})
        self.assertEqual((result.window, result.reason), (before//2, 'resource_pressure'))

    def test_stale_independent_rejection_is_not_relabelled_with_fresh_native_metrics(self):
        trace = ResourceTrace()
        trace.tick(physical=True)
        result = trace.tick(permit={'observed_at_s': 0., 'rejected_requests_total': 1,
            'safety_sequence': 10, 'safety_observed_at_s': 0.})
        self.assertEqual(result.window, 1)
        self.assertNotEqual(result.reason, 'resource_pressure')

    def test_an_observed_independent_guard_cannot_disappear_into_native_fallback(self):
        trace = ResourceTrace(maximum=16)
        trace.tick(physical=(0., 0.), permit={'safety_sequence': 10, 'memory_pressure': False})
        trace.tick(permit={'safety_sequence': 11, 'memory_pressure': True})
        before = trace.controller.current_window
        for _ in range(16):
            result = trace.tick(80, elapsed=1., physical=True)
            self.assertLessEqual(result.window, before)
        self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_changing_only_receiver_identity_cannot_replay_one_native_pressure_value(self):
        trace = ResourceTrace()
        trace.tick(physical=True)
        trace.tick(elapsed=.001, observation={'memory_pressure': True}, permit={'native_sequence': 1})
        changes = trace.controller.report()['changes']
        for _ in range(4):
            trace.tick(elapsed=.001, observation={'memory_pressure': True}, permit={'native_sequence': 1})
        self.assertEqual(trace.controller.report()['changes'], changes)

    def test_pressure_cannot_hide_a_same_frame_completed_counter_reversal(self):
        trace = ResourceTrace(maximum=16)
        trace.until(lambda r, _: r['accepted_steps'] > 0, count=lambda c: 40*c)
        before = trace.controller.current_window
        names = ('completed_items_total', 'completed_input_work_total', 'completed_output_work_total',
                 'completed_computed_input_work_total', 'completed_cached_input_work_total')
        values = dict.fromkeys(names, 0)
        values['memory_pressure'] = True
        result = trace.tick(elapsed=.001, observation=values)
        self.assertEqual((result.window, result.reason), (before//2, 'resource_pressure'))
        self.assertFalse(trace.controller.report()['work_source_valid'])
        for _ in range(16):
            self.assertEqual(trace.tick(100, elapsed=1., physical=True).window, result.window)

    def test_protection_only_frames_with_no_work_fields_do_not_invent_a_reversal(self):
        trace = ResourceTrace(maximum=16)
        trace.until(lambda r, _: r['accepted_steps'] > 0, count=lambda c: 40*c)
        before = trace.controller.current_window
        names = ('completed_items_total', 'completed_input_work_total', 'completed_output_work_total',
                 'completed_computed_input_work_total', 'completed_cached_input_work_total')
        values = dict.fromkeys(names, None)
        values.update(fresh=False, sample_sequence=None, sample_age_s=None,
                      running=None, waiting=None, kv_usage=None, preemptions_total=None)
        result = trace.tick(elapsed=.001, observation=values, permit={'sample_sequence': None,
            'native_sequence': None, 'safety_sequence': 20, 'memory_pressure': True})
        self.assertEqual((result.window, result.reason), (before//2, 'resource_pressure'))
        self.assertTrue(trace.controller.report()['work_source_valid'])

    def test_normal_saturation_at_the_applied_request_maximum_remains_measurable(self):
        trace = ResourceTrace(maximum=4)
        trace.tick(physical=(0., 0.))
        trace.until(lambda r, _: r['measured_window'] == 4, count=lambda c: 40*c,
                    supply={'capacity_blocked': 'member_R'}, limit=12)
        trace.until(lambda r, _: r['window'] == 2, count=160,
                    supply={'capacity_blocked': 'member_R'}, limit=4)

    def test_receiver_changes_do_not_replay_a_genuinely_observed_native_pressure_sample(self):
        trace = ResourceTrace()
        before = trace.tick(physical=True).window
        result = trace.tick(elapsed=.001, observation={'memory_pressure': True})
        self.assertEqual(result.window, 1)
        changes = trace.controller.report()['changes']
        for index in range(4):
            trace.tick(elapsed=.001, observation={'memory_pressure': True, 'sample_age_s': .001*(index+1)},
                       permit={'native_sequence': 2})
        self.assertEqual(trace.controller.report()['changes'], changes)

    def test_new_permission_and_cached_guard_keep_their_own_source_times(self):
        trace = ResourceTrace(maximum=16)
        before = trace.tick(physical=(0., 0.), permit={'request_limit': 4,
            'safety_sequence': 10, 'memory_pressure': False}).window
        result = trace.tick(elapsed=.001, permit={'request_limit': 16,
            'safety_sequence': 10, 'memory_pressure': False})
        self.assertEqual(result.window, before)
        self.assertNotIn('conflicting', result.reason)

    def test_first_old_guard_cannot_be_made_fresh_by_a_current_permission(self):
        trace = ResourceTrace()
        result = trace.tick(physical=True, permit={'safety_sequence': 10,
            'safety_observed_at_s': 0., 'memory_pressure': False})
        self.assertEqual(result.window, 1)
        self.assertIn('expired_safety_sample', result.reason)
        self.assertEqual(trace.tick(physical=True).window, 1)

    def test_fresh_guard_pressure_does_not_require_current_operational_permission(self):
        trace = ResourceTrace()
        before = trace.tick(physical=True).window
        result = trace.tick(elapsed=.001, permit={'observed_at_s': 0., 'safety_sequence': 10,
            'memory_pressure': True})
        self.assertEqual((result.window, result.reason), (1, 'resource_pressure'))

    def test_queued_native_work_can_trigger_a_smaller_trial_and_a_measured_reference_return(self):
        trace = ResourceTrace(maximum=16)
        trace.tick(physical=(20., 20.))
        trace.until(lambda r, _: r['stop_reason'] == 'performance_regression',
                    count=lambda c: 40*c, observation={'waiting': 1})
        report = trace.controller.report()
        self.assertEqual(report['verified_performance_window'], 13)
        self.assertEqual([row['window'] for row in report['history']], [13, 6, 13])
        self.assertNotEqual(report['stop_reason'], 'operational_limit')

    def test_observed_memory_and_rejection_channels_cannot_become_optional_when_values_disappear(self):
        for channel in ('native_memory', 'guard_memory', 'native_rejection', 'guard_rejection'):
            with self.subTest(channel=channel):
                trace = ResourceTrace(maximum=16)
                independent = channel.startswith('guard')
                memory = channel.endswith('memory')
                initial = {'safety_sequence': 10} if independent else {}
                if memory and independent: initial['memory_pressure'] = False
                if not memory: initial['rejected_requests_total'] = 0
                trace.tick(physical=(0., 0.), permit=initial)
                pressure = {'safety_sequence': 11} if independent else {}
                observation = {}
                if memory:
                    (pressure if independent else observation)['memory_pressure'] = True
                else:
                    pressure['rejected_requests_total'] = 1
                result = trace.tick(observation=observation, permit=pressure)
                self.assertEqual(result.reason, 'resource_pressure')
                before = result.window
                for index in range(12):
                    missing = {'safety_sequence': 12+index} if independent else {}
                    result = trace.tick(80, elapsed=1., permit=missing, physical=True)
                    self.assertLessEqual(result.window, before)
                self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_a_rejected_guard_identity_cannot_be_retimed_or_waited_into_a_new_read(self):
        for future in (False, True):
            with self.subTest(future=future):
                trace = ResourceTrace(maximum=16)
                source_time = 10.75 if future else 0.
                result = trace.tick(physical=True, permit={'safety_sequence': 10,
                    'safety_observed_at_s': source_time, 'memory_pressure': False})
                self.assertEqual(result.window, 1)
                result = trace.tick(elapsed=.6, physical=True, permit={'safety_sequence': 10,
                    'safety_observed_at_s': source_time if future else trace.now+.6,
                    'memory_pressure': False})
                self.assertEqual(result.window, 1)
                self.assertGreater(trace.tick(physical=True, permit={'safety_sequence': 11,
                    'memory_pressure': False}).window, 1)

    def test_missing_optional_performance_counters_do_not_cancel_a_resource_permitted_run(self):
        trace = ResourceTrace()
        before = trace.tick(physical=True).window
        for _ in range(6):
            result = trace.tick(40, elapsed=1., observation={'completed_computed_input_work_total': None})
            self.assertEqual(result.window, before)
        report = trace.controller.report()
        self.assertIsNone(report['verified_performance_window'])
        self.assertIsNone(report['measured_window'])
        self.assertEqual(report['accepted_steps'], 0)
        self.assertIsNone(trace.controller.export_hint())
        result = trace.tick(observation={'preemptions_total': None})
        self.assertEqual(result.window, 1)

    def test_optional_work_feedback_is_not_required_for_the_first_resource_trial(self):
        trace = ResourceTrace()
        result = trace.tick(40, physical=True, observation={'completed_computed_input_work_total': None})
        self.assertGreater(result.window, 128)
        self.assertEqual(trace.controller.report()['accepted_steps'], 0)
        self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_each_observed_memory_source_must_clear_even_when_the_other_is_fresh_and_clear(self):
        trace = ResourceTrace(maximum=16)
        trace.tick(physical=(0., 0.), observation={'memory_pressure': False},
            permit={'safety_sequence': 10, 'memory_pressure': False})
        result = trace.tick(observation={'memory_pressure': True},
            permit={'safety_sequence': 11, 'memory_pressure': False})
        before = result.window
        for index in range(12):
            self.assertLessEqual(trace.tick(80, elapsed=1., physical=True,
                permit={'safety_sequence': 12+index, 'memory_pressure': False}).window, before)

    def test_fixture_cache_retains_an_explicit_real_guard_source_time(self):
        trace = ResourceTrace(maximum=16)
        first_time = trace.now+.25-.5
        before = trace.tick(physical=True, permit={'safety_sequence': 10,
            'safety_observed_at_s': first_time, 'memory_pressure': False}).window
        result = trace.tick(elapsed=.1, permit={'safety_sequence': 10, 'memory_pressure': False})
        self.assertEqual(trace.permit.safety_observed_at_s, first_time)
        self.assertEqual(result.window, before)
        self.assertNotIn('conflicting', result.reason)

    def test_a_missing_work_field_cannot_hide_another_known_counter_reversal(self):
        fields = ('completed_items_total', 'completed_input_work_total', 'completed_output_work_total',
                  'completed_computed_input_work_total', 'completed_cached_input_work_total')
        for index, field in enumerate(fields):
            with self.subTest(field=field):
                trace = ResourceTrace(maximum=16)
                trace.until(lambda r, _: r['window'] == 8 and r['verified_performance_window'] == 4,
                            count=lambda c: 40*c)
                result = trace.tick(40, observation={field: 0, fields[(index+1)%len(fields)]: None})
                self.assertEqual((result.window, result.reason), (4, 'completed_work_counter_reversed'))
                self.assertFalse(trace.controller.report()['work_source_valid'])
                self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_partial_known_watermarks_do_not_make_a_valid_complete_span_inconsistent(self):
        trace = ResourceTrace(maximum=16)
        before = trace.tick(physical=True).window
        self.assertEqual(trace.tick(40, observation={'completed_computed_input_work_total': None}).window, before)
        self.assertEqual(trace.tick(40, observation={'completed_input_work_total': None}).window, before)
        trace.until(lambda r, _: r['measured_window'] == before, count=40)
        report = trace.controller.report()
        self.assertTrue(report['work_source_valid'])
        self.assertEqual(report['accepted_steps'], 0)
        self.assertIsNone(report['verified_performance_window'])


if __name__ == '__main__':
    unittest.main()
