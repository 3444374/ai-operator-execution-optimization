"""Source recency and delivered-work measurement use separate clocks.

The saved-input candidate replay is off-policy CPU evidence, not a new query.
"""
from dataclasses import asdict, replace
import gzip
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.core.session_contract import Uncertain
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig, ResourceFeedbackController
from src.scheduling.submission_control.admission import DynamicAdmissionGate
from tests.scheduling.test_incremental_session import setup, task


FIXTURE = Path(__file__).with_name('fixtures')/'geometric_feedback_saved_input.json.gz'


def saved_replay(data, *, source_stale_after_s=None):
    origin = data['origin']
    clock = dict(now=origin['started'])
    # These selectors belonged to the retired request adapter, not this controller.
    options = {key: value for key, value in origin['options'].items()
               if key not in {'search_policy', 'physical_probe_policy'}}
    controller = ResourceFeedbackController(origin['maximum'], origin['initial'],
        ResourceFeedbackConfig(**options), clock=lambda: clock['now'],
        source_stale_after_s=source_stale_after_s)
    controller.reset(origin['initial'], origin['total'])
    decisions = []
    for row in data['records']:
        observation = AdmissionObservation(**row['observation'])
        clock['now'] = observation.observed_at_s
        decision = controller.update(observation)
        decisions.append((decision.reason, controller.current_window))
    return decisions, controller.report()


class ResourceFeedbackSourceClockTests(unittest.TestCase):
    def test_saved_original_inputs_and_unbound_decisions_match_exactly(self):
        data = json.loads(gzip.decompress(FIXTURE.read_bytes()))
        records = json.dumps(data['records'], sort_keys=True, separators=(',', ':')).encode()
        self.assertEqual(hashlib.sha256(records).hexdigest(), data['records_sha256'])
        self.assertEqual(len(data['records']), 474)
        decisions, report = saved_replay(data)
        self.assertEqual(decisions, [(row['reason'], row['window']) for row in data['records']])
        for key, value in data['expected_unbound_report'].items():
            self.assertEqual(report[key], value, key)

    def test_saved_failure_uses_actual_provider_recency_without_rewriting_inputs_or_budget(self):
        data = json.loads(gzip.decompress(FIXTURE.read_bytes()))
        before = json.dumps(data, sort_keys=True)
        first, report = saved_replay(data, source_stale_after_s=data['source_stale_after_s'])
        second, again = saved_replay(data, source_stale_after_s=data['source_stale_after_s'])
        self.assertEqual(first, second)
        for key in data['expected_unbound_report']:
            self.assertEqual(report[key], again[key], key)
        valid = [i for i, row in enumerate(data['records']) if row['observation']['fresh']
            and AdmissionObservation(**row['observation']).has_service_metrics]
        self.assertEqual(len(valid), 248)
        self.assertTrue(all(first[i][0] != 'missing_or_stale_feedback' for i in valid))
        self.assertGreater(report['changes'], 0)
        self.assertGreater(len(report['measurements']), 0)
        self.assertTrue(all(1 <= window <= data['origin']['maximum'] for _, window in first))
        self.assertEqual(json.dumps(data, sort_keys=True), before)
        self.assertEqual(data['origin']['options']['window_s'], .01)
        self.assertEqual(data['origin']['options']['exploration_s'], 4)

    def controller(self, *, source_age=.5, pressure_response='sample'):
        return ResourceFeedbackController(8, 8,
            ResourceFeedbackConfig(window_s=.01, poll_interval_s=.003,
                                   pressure_response=pressure_response),
            clock=lambda: 0., source_stale_after_s=source_age)

    def observation(self, at=10., **kwargs):
        value = AdmissionObservation(at, True, 8, 8, 0, .1, sample_age_s=.02,
            sample_sequence=1, preemptions_total=0, pending_items=100)
        return replace(value, **kwargs)

    def test_bound_fresh_pressure_precedes_the_measurement_window_and_replays_once(self):
        controller = self.controller()
        first = controller.update(self.observation(memory_pressure=True))
        self.assertEqual((first.window, first.reason), (4, 'resource_pressure'))
        replay = controller.update(self.observation(at=10.1, sample_age_s=.12, memory_pressure=True))
        self.assertEqual(replay.window, 4)
        self.assertEqual(controller.report()['changes'], 1)
        fresh = controller.update(self.observation(at=10.2, sample_sequence=2, memory_pressure=True))
        self.assertEqual((fresh.window, fresh.reason), (2, 'resource_pressure'))

    def test_preemption_increment_does_not_wait_for_complete_work_measurements(self):
        controller = self.controller()
        controller.update(self.observation(preemptions_total=20))
        decision = controller.update(self.observation(at=10.003, sample_sequence=2,
            preemptions_total=21, completed_items_total=None))
        self.assertEqual((decision.window, decision.reason), (4, 'resource_pressure'))
        self.assertEqual(controller.report()['measurements'], [])

    def test_expired_missing_or_already_delivered_sources_cannot_assert_new_pressure(self):
        for changes in ({'sample_age_s': .501}, {'fresh': False},
                        {'running': None, 'waiting': None, 'kv_usage': None}):
            with self.subTest(changes=changes):
                controller = self.controller()
                decision = controller.update(self.observation(memory_pressure=True, **changes))
                self.assertEqual(decision.window, 8)
                self.assertEqual(controller.report()['changes'], 0)

    def test_unbound_geometric_callers_keep_the_original_age_rule(self):
        controller = ResourceFeedbackController(8, 8, ResourceFeedbackConfig(
            window_s=.01, search_mode='geometric'), clock=lambda: 0.)
        decision = controller.update(self.observation(memory_pressure=True))
        self.assertEqual((decision.window, decision.reason), (8, 'missing_or_stale_feedback'))

    def test_window_response_and_option_signatures_do_not_change(self):
        options = ResourceFeedbackConfig(window_s=.01, pressure_response='window')
        before = asdict(options)
        bound = ResourceFeedbackController(8, 8, options, clock=lambda: 0., source_stale_after_s=.5)
        unbound = ResourceFeedbackController(8, 8, options, clock=lambda: 0.)
        observations = [self.observation(at=at, sample_sequence=i, memory_pressure=True)
            for i, at in enumerate((.005, .01, .02), 1)]
        self.assertEqual([bound.update(o) for o in observations], [unbound.update(o) for o in observations])
        self.assertEqual(asdict(options), before)

    def test_source_age_limit_accepts_zero_and_rejects_invalid_declarations(self):
        for value in (None, 0, .5):
            self.assertEqual(self.controller(source_age=value).source_stale_after_s, value)
        for value in (True, -1, '0.5', float('nan'), float('inf'), 10**1000):
            with self.subTest(type=type(value).__name__), self.assertRaises(ValueError):
                self.controller(source_age=value)

    def test_unknown_Core_work_keeps_its_original_ownership_when_source_pressure_reduces(self):
        engine, session, backend, clock = setup(held_tasks=5, input_bytes=20,
            result_bytes=20, active_requests=4, active_work=4, offer_tasks=5, step_actions=64)
        controller = ResourceFeedbackController(4, 4, ResourceFeedbackConfig(window_s=.01),
            clock=clock, source_stale_after_s=.5)
        observation = self.observation()
        provider = SimpleNamespace(latest=lambda inflight, **_: replace(observation, inflight=inflight))
        gate = DynamicAdmissionGate(controller, provider, clock=clock)
        engine.policies = replace(engine.policies, admission=gate)
        session.offer(tuple(task(i) for i in range(5)))
        session.seal()
        session.advance(5)
        key = next(iter(backend.pending))
        backend.events.append(Uncertain(key, backend.pending[key][0]))
        engine.reap(1)
        self.assertEqual(engine.capacity.records[key].phase, 'UNCERTAIN')
        before = engine.capacity.usage()
        record = engine.capacity.records[key]
        cancellations = tuple(backend.cancelled)
        clock.now = 10.003
        observation = self.observation(at=clock.now, sample_sequence=2, preemptions_total=1)
        self.assertFalse(gate.decide(before.active_requests).allowed)
        self.assertEqual(controller.current_window, 2)
        self.assertEqual(engine.capacity.usage(), before)
        self.assertIs(engine.capacity.records[key], record)
        self.assertTrue(record.compute)
        self.assertEqual(tuple(backend.cancelled), cancellations)


if __name__ == '__main__':
    unittest.main()
