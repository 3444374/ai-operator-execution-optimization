"""Admission uses integer request counts and validates before policy side effects."""
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from src.scheduling.core.models import AdmissionObservation, WindowDecision
from src.scheduling.submission_control.admission import DynamicAdmissionGate, StaticAdmissionController


class AdmissionCountTests(unittest.TestCase):
    def gate(self, *, current=2, result=None, **options):
        controller = SimpleNamespace(current_window=current,
            update=Mock(return_value=result or WindowDecision(2, 'hold', 'fixture')))
        provider = SimpleNamespace(latest=Mock(return_value=AdmissionObservation(1., True, 0, 1, 0, .1)))
        return DynamicAdmissionGate(controller, provider, **options), controller, provider

    def test_static_limit_and_dynamic_initial_window_reject_noninteger_counts(self):
        for value in (True, 1.5, 2.0, '2', None, float('nan')):
            with self.subTest(kind='static', value=value), self.assertRaises(ValueError):
                StaticAdmissionController(value)
            with self.subTest(kind='dynamic', value=value), self.assertRaises(ValueError):
                self.gate(current=value)

    def test_invalid_inflight_is_rejected_before_observer_or_policy(self):
        for value in (True, .5, 2.0, '0', None, float('nan'), -1):
            for kind in ('static', 'dynamic'):
                gate, controller, provider = self.gate()
                if kind == 'static':
                    gate = StaticAdmissionController(2)
                with self.subTest(kind=kind, value=value), self.assertRaises(ValueError):
                    gate.decide(value)
                controller.update.assert_not_called()
                provider.latest.assert_not_called()

    def test_invalid_policy_output_cannot_replace_limit_or_emit_trace(self):
        for value in (True, 1.5, 2.0, None, float('nan'), 0):
            trace = Mock()
            decision = SimpleNamespace(window=value, reason='fixture', action='hold')
            gate, _, _ = self.gate(result=decision, trace_sink=trace)
            with self.subTest(value=value), self.assertRaises(ValueError):
                gate.decide(0)
            self.assertEqual(gate.limit, 2)
            trace.assert_not_called()

    def test_cadence_rejects_boolean_nonnumeric_or_nonfinite_values(self):
        for value in (True, False, '0', None, float('nan'), float('inf'), 10**400, -1):
            with self.subTest(kind=type(value).__name__), self.assertRaises(ValueError):
                self.gate(min_update_interval_s=value)


if __name__ == '__main__':
    unittest.main()
