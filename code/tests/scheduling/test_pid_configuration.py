"""Reject invalid PID configuration before it can emit fractional windows."""
import hashlib
import json
from pathlib import Path
import unittest

from src.scheduling.submission_control.pid import PidAdmissionController, PidConfig


class PidConfigurationTests(unittest.TestCase):
    def test_count_fields_reject_booleans_and_nonintegers(self):
        for name in ('min_window', 'max_window'):
            for bad in (True, 1.5, 2.0, '2', None, float('nan'), 10**400):
                options = dict(min_window=1, max_window=4)
                options[name] = bad
                with self.subTest(field=name, value=bad), self.assertRaises(ValueError):
                    PidConfig(**options)

    def test_PID_numeric_fields_reject_booleans_nonnumeric_and_unrepresentable(self):
        for name in ('target_waiting', 'proportional_gain', 'integral_gain',
                     'derivative_gain', 'integral_limit'):
            for bad in (True, '1', None, 10**400):
                with self.subTest(field=name, kind=type(bad).__name__), self.assertRaises(ValueError):
                    PidConfig(**{name:bad})

    def test_initial_window_is_an_integer_count(self):
        config = PidConfig(min_window=1, max_window=4)
        for bad in (True, 2.0, 2.5, '2', float('nan')):
            with self.subTest(value=bad), self.assertRaises(ValueError):
                PidAdmissionController(config, initial_window=bad)

    def test_historical_PID_keeps_original_bytes_and_projection_identity(self):
        root = Path(__file__).resolve().parents[3]
        directory = root/'experiments/reproduction/native_baselines'
        logical = 'code/src/scheduling/submission_control/pid.py'
        original = 'historical_sources/bfb9100e/'+logical
        expected = 'b9a4c4b4fc34f7a6d39959acc85c449ac276b8beb7884ebd7e776a2caa2a41e7'
        self.assertEqual(hashlib.sha256((directory/original).read_bytes()).hexdigest(), expected)
        manifest = json.loads((directory/'MANIFEST.json').read_text())
        for spec in manifest['variants'].values():
            self.assertEqual(spec['code_files_sha256'][logical], expected)
            self.assertEqual(spec['overrides'][logical], original)


if __name__ == '__main__':
    unittest.main()
