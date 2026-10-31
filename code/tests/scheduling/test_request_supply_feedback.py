"""Request-unit demand gating; deterministic replays, not hardware evidence."""
from dataclasses import replace
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.stage_work import RequestSupplyFeedbackController, SupplyState


class RequestSupplyFeedbackTests(unittest.TestCase):
    def setUp(self):
        self.now = 0.
        self.c = RequestSupplyFeedbackController(8, clock=lambda: self.now)
        self.c.reset(1, 10000)

    def tick(self, completed=0, **changes):
        self.now += 1
        o = AdmissionObservation(self.now, True, self.c.current_window,
            self.c.current_window, 0, .1, sample_age_s=0,
            completed_items_total=completed, pending_items=1000, preemptions_total=0,
            completed_input_work_total=completed*100, completed_output_work_total=completed*4,
            completed_computed_input_work_total=completed*80,
            completed_cached_input_work_total=completed*20)
        s = SupplyState(self.now, 100, completed+100, completed+5, 1, True, False,
                        active_requests=self.c.current_window)
        obs = {k: v for k, v in changes.items() if k in o.__dataclass_fields__}
        state = {k: v for k, v in changes.items() if k in s.__dataclass_fields__ and k not in obs}
        return self.c.update_supply(replace(o, **obs), replace(s, **state))

    def test_default_request_scan_measures_before_growth_and_keeps_costs(self):
        decisions = [self.tick(n*50) for n in range(1, 6)]
        self.assertEqual([d.reason for d in decisions], ['priming_measurement',
            'initial_progress_window', 'confirming_performance', 'confirming_performance',
            'performance_probe'])
        self.assertEqual(self.c.current_window, 2)
        report = self.c.report()
        self.assertEqual(report['control_unit'], 'single_model_requests')
        self.assertFalse(report['B_adjusted'])
        self.assertEqual(report['accepted_steps'], 0)
        self.assertEqual(report['exploration_limits'], dict(elapsed_s=None, changes=None,
            attempts_per_candidate=None, min_remaining=0))
        self.assertEqual(report['demand_changes'][-1]['active_requests'], 1)
        self.assertEqual(report['demand_changes'][-1]['active_batches'], 1)
        self.tick(300, memory_pressure=True)
        self.assertEqual(self.c.current_window, 1)
        self.assertGreater(self.c.report()['exploration_wall_s'], 0)

    def test_missing_duplicate_queue_supply_and_tail_do_not_prove_spare_resources(self):
        for change, reason in [(dict(fresh=False), 'insufficient_information'),
                (dict(sample_age_s=2), 'insufficient_information'),
                (dict(waiting=2), 'backend_queued'),
                (dict(ready_items=0), 'upstream_supply_limited'),
                (dict(ready_items=0, source_exhausted=True), 'work_tail_or_dependency_wait'),
                (dict(capacity_blocked='result_bytes'), 'configuration_boundary'),
                (dict(capacity_blocked='pending_transport_admission'), 'submission_backpressure')]:
            with self.subTest(change=change):
                self.assertEqual(self.tick(20, **change).reason, reason)
                self.assertEqual(self.c.current_window, 1)

    def test_invalid_start_or_historical_hint_is_rejected(self):
        for start, hint in [(2, None), (1, (1, 2))]:
            with self.assertRaises(ValueError):
                self.c.reset(start, 100, search_hint=hint)
