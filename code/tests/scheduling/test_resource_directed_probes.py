"""Large trials use actual ready permission and source evidence, never literal C tiers."""
import unittest
from dataclasses import replace

from src.scheduling.submission_control.feedback_options import ResourceRequestConfig
from src.scheduling.submission_control.admission import StaticAdmissionController
from tests.scheduling.test_incremental_session import setup, task
from tests.scheduling.test_resource_request_controller import ResourceTrace


class ResourceDirectedProbeTests(unittest.TestCase):
    def test_one_fresh_resource_view_can_reach_distinct_permitted_frontiers_without_completions(self):
        for maximum in (64, 256, 4096):
            with self.subTest(maximum=maximum):
                trace = ResourceTrace(maximum=maximum)
                decision = trace.tick(physical=(0., 0.), physical_count=1)
                self.assertEqual(decision.window, maximum)
                report = trace.controller.report()
                self.assertEqual(trace.completed, 0)
                self.assertEqual(report['accepted_steps'], 0)
                self.assertIsNone(report['verified_performance_window'])
                self.assertFalse(report['physical_capacity_known'])

    def test_observed_spare_share_positions_the_trial_continuously(self):
        targets = []
        for busy in (0., 20., 70.):
            trace = ResourceTrace(maximum=1000)
            targets.append(trace.tick(physical=(busy, busy), physical_count=1).window)
        self.assertEqual(targets, [1000, 800, 300])

    def test_resources_can_extend_a_native_trial_while_the_old_first_member_is_unfinished(self):
        trace = ResourceTrace(maximum=512)
        self.assertEqual(trace.tick().window, 2)
        decision = trace.tick(physical=(0., 0.), physical_count=1,
            observation={'transition_pending': 1})
        self.assertEqual(decision.window, 512)
        self.assertEqual(trace.completed, 0)
        report = trace.controller.report()
        self.assertEqual([r['window'] for r in report['history']], [2, 512])
        self.assertEqual(report['accepted_steps'], 0)
        self.assertTrue(report['active_unverified_trial'])

    def test_optional_work_measurement_is_not_a_resource_probe_permission(self):
        trace = ResourceTrace(maximum=512)
        trace.tick()
        result = trace.tick(physical=(0., 0.), physical_count=1,
            observation={'transition_pending': 1, 'completed_computed_input_work_total': None})
        self.assertEqual(result.window, 512)
        report = trace.controller.report()
        self.assertEqual(report['accepted_steps'], 0)
        self.assertIsNone(report['verified_performance_window'])
        self.assertIsNone(report['measured_window'])

    def measured_reference(self):
        trace = ResourceTrace(maximum=512)
        self.assertEqual(trace.tick().window, 2)
        trace.until(lambda report, decision:
            decision.reason == 'resource_trial_measured_without_gain_claim')
        self.assertEqual(trace.controller.report()['measured_window'], 2)
        return trace

    def test_later_resource_trial_is_not_clamped_to_twice_a_measured_reference(self):
        trace = self.measured_reference()
        self.assertEqual(trace.tick(physical=(0., 0.), physical_count=1).window, 512)
        self.assertEqual(trace.controller.report()['accepted_steps'], 0)

    def test_completed_query_work_regression_returns_to_measured_reference(self):
        trace = self.measured_reference()
        started = trace.controller.started
        trace.tick(physical=(0., 0.), physical_count=1)
        trace.until(lambda report, decision: decision.reason == 'performance_regression', count=20)
        report = trace.controller.report()
        self.assertEqual(report['window'], 2)
        self.assertEqual(report['accepted_steps'], 0)
        self.assertEqual(trace.controller.started, started)
        self.assertEqual(trace.controller._work_watermark[0], trace.completed)
        self.assertEqual([row['window'] for row in report['history']], [2, 512, 2])

    def test_pressure_rolls_back_an_extended_unverified_trial_to_its_original_fallback(self):
        trace = ResourceTrace(maximum=512)
        trace.tick()
        trace.tick(physical=(0., 0.), physical_count=1, observation={'transition_pending': 1})
        self.assertEqual(trace.tick(observation={'memory_pressure': True}).window, 1)
        self.assertEqual(trace.controller.report()['accepted_steps'], 0)

    def test_resource_growth_cannot_resume_while_old_work_is_still_above_the_shrunken_window(self):
        trace = ResourceTrace(maximum=512)
        trace.tick(physical=(0., 0.), physical_count=1)
        self.assertEqual(trace.tick(observation={'memory_pressure': True}).window, 1)
        result = trace.tick(1, physical=(0., 0.), physical_count=1,
            observation={'inflight': 512, 'transition_pending': 512, 'memory_pressure': False},
            supply={'active_requests': 512})
        self.assertEqual(result.window, 1)

    def test_repeated_pressure_narrows_the_failed_range_instead_of_retrying_the_same_large_window(self):
        trace = ResourceTrace(maximum=1024)
        self.assertEqual(trace.tick(physical=(0., 0.), physical_count=1).window, 1024)
        candidates = []
        for _ in range(3):
            self.assertEqual(trace.tick(observation={'memory_pressure': True}).window, 1)
            candidate = trace.tick(1, observation={'memory_pressure': False},
                physical=(0., 0.), physical_count=1).window
            candidates.append(candidate)
            self.assertIn('pressure', trace.controller._trial['clip_causes'])
        self.assertEqual(candidates, [512, 256, 128])
        self.assertEqual(trace.completed, 3)
        self.assertEqual(trace.controller.report()['accepted_steps'], 0)

    def test_unknown_required_memory_cannot_be_overridden_by_idle_device_data(self):
        trace = ResourceTrace(options=ResourceRequestConfig(require_memory_feedback=True))
        self.assertEqual(trace.tick(physical=(0., 0.), physical_count=1).window, 1)

    def test_request_ready_and_pending_permission_still_bound_large_trials(self):
        for kwargs, expected in (({'permit': {'request_limit': 16}}, 16),
                                 ({'supply': {'ready_items': 3}}, 4),
                                 ({'observation': {'pending_items': 5}}, 6)):
            with self.subTest(kwargs=kwargs):
                trace = ResourceTrace(maximum=4096)
                self.assertEqual(trace.tick(physical=(0., 0.), physical_count=1, **kwargs).window, expected)

    def test_a_short_query_never_turns_an_unmeasured_large_trial_into_a_winner(self):
        trace = ResourceTrace(maximum=256)
        trace.tick(physical=(0., 0.), physical_count=1)
        trace.tick(observation={'pending_items': 0}, supply={'exhausted': True, 'ready_items': 0})
        report = trace.controller.report()
        self.assertIsNone(report['verified_performance_window'])
        self.assertIsNone(report['recommended_window'])
        self.assertEqual(report['accepted_steps'], 0)

    def test_a_previous_window_resource_view_is_not_reused_for_another_jump(self):
        trace = ResourceTrace(maximum=512)
        trace.tick()
        result = trace.tick(elapsed=.01, physical=(0., 0.), physical_count=1,
            observation={'transition_pending': 1})
        self.assertEqual(result.window, 2)
        self.assertEqual(trace.controller.report()['changes'], 1)
        self.assertEqual(trace.tick(elapsed=.3, physical=(0., 0.), physical_count=1,
            observation={'transition_pending': 1}).window, 512)
        self.assertEqual(trace.completed, 0)

    def test_a_new_resource_RPC_cannot_relabel_a_repeated_native_sample(self):
        trace = ResourceTrace(maximum=512)
        first = trace.tick(physical=(90., 90.), physical_count=1).window
        self.assertEqual(trace.tick(physical=(0., 0.), physical_count=1,
            observation={'sample_sequence': 1, 'running': 1, 'sample_age_s': .25},
            permit={'sample_sequence': 1, 'native_sequence': 1}).window, first)
        self.assertEqual(trace.controller.report()['changes'], 1)
        self.assertEqual(trace.tick(physical=(0., 0.), physical_count=1).window, 512)

    def test_real_core_enforces_the_large_window_and_pressure_does_not_release_owned_requests(self):
        trace = ResourceTrace(maximum=64)
        core, session, backend, clock = setup(held_tasks=128, input_bytes=512, result_bytes=512,
            active_requests=64, active_work=64, offer_tasks=128, step_actions=128)
        class Window:
            def decide(self, inflight, **kwargs):
                return StaticAdmissionController(trace.controller.current_window).decide(inflight, **kwargs)
        core.policies = replace(core.policies, admission=Window())
        self.assertEqual(session.offer(tuple(task(i) for i in range(128))).accepted_prefix_count, 128)
        session.seal()
        session.advance(128)
        self.assertEqual(core.capacity.usage().active_requests, 1)
        self.assertEqual(trace.tick(physical=(0., 0.), physical_count=1).window, 64)
        clock.now = max(clock.now, session._retry_at)
        session.advance(128)
        self.assertEqual(len(backend.pending), 64)
        owned = core.capacity.usage()
        self.assertEqual(owned.active_requests, 64)
        self.assertEqual(trace.tick(observation={'memory_pressure': True}).window, 1)
        session.advance(128)
        self.assertEqual(core.capacity.usage(), owned)
        self.assertEqual(backend.cancelled, [])
        session.close()
        for key in tuple(backend.pending):
            backend.complete(key)
        core.reap(128)
        self.assertEqual(core.capacity.usage().held_tasks, 0)
