"""New rolling state must not alias old hints or invalidate ordinary caches."""
from dataclasses import asdict, replace
import json
from pathlib import Path
import tempfile
import unittest

from src.scheduling.runtime.calibration import _digest, identity_signature, run_admission_query
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig
from src.scheduling.submission_control.stage_work import default_request_feedback_options
from tests.scheduling.test_ascending_request_scan import ScanTrace


class RequestScanCompatibilityTests(unittest.TestCase):
    def test_reference_policy_uses_the_existing_disk_cache_key_and_reads_its_hint(self):
        identity = {name: {'version': 'CPU-fixture'} for name in
                    ('hardware', 'model', 'generation', 'execution', 'workload', 'measurement')}
        identity['measurement']['work_unit'] = 'model_calls'
        options = ResourceFeedbackConfig()
        old_options = asdict(options)
        old_options.pop('search_policy')
        policy = dict(strategy='onepass_performance_v3', initial_window=1, maximum=8, options=old_options)
        payload = dict(schema='semloom.runtime_admission_hint.v1', identity_sha256=identity_signature(identity),
            policy=policy, window=3, reuse_eligible=True, evidence=dict(exploration_censored=False))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/('admission-'+_digest(dict(identity=identity_signature(identity), policy=policy))+'.json')
            original = json.dumps(payload).encode()
            path.write_bytes(original)
            seen = []
            def execute(window, **kwargs):
                seen.append((window, kwargs['allow_exploration']))
                return dict(status='completed', reuse_eligible=False)
            value = run_admission_query(identity, execute, initial_window=1, maximum=8,
                                         cache_dir=directory, options=options)
            self.assertTrue(value['cache_hit'])
            self.assertEqual(seen, [(3, False)])
            self.assertEqual(path.read_bytes(), original)

    def test_new_state_requires_fresh_query_validation_and_exact_policy_options(self):
        trace = ScanTrace()
        trace.run_until(lambda r, _: r['recommended_window'] >= 2)
        hint = trace.controller.export_hint()
        reused = ScanTrace(hint=hint)
        self.assertIsNone(reused.controller.export_hint())
        self.assertFalse(reused.controller.report()['comparable_selection'])
        changed = default_request_feedback_options()
        reused.controller.options = replace(changed, min_gain=.05)
        with self.assertRaisesRegex(ValueError, 'incompatible'):
            reused.controller.reset(hint['selected_window'], 1000, search_hint=hint)

    def test_mutating_report_or_exported_hint_does_not_change_live_progress(self):
        trace = ScanTrace()
        trace.run_until(lambda r, _: r['recommended_window'] >= 2)
        original = trace.controller.export_hint()
        external = trace.controller.export_hint()
        external['search_state']['work_context']['input_min'] = -1
        report = trace.controller.report()
        report['measurements'][0]['input_min'] = -1
        report['history'][0]['window'] = -1
        self.assertEqual(trace.controller.export_hint(), original)
        self.assertGreater(trace.controller.report()['measurements'][0]['input_min'], 0)
        self.assertGreater(trace.controller.report()['history'][0]['window'], 0)


if __name__ == '__main__':
    unittest.main()
