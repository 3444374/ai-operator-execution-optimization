"""Retired prototype recovery and current export checks; no model or service."""
import ast
import hashlib
import json
from pathlib import Path
import sys
import unittest

from tests.scheduling.test_record_feedback_variants import SourceView


ROOT = Path(__file__).resolve().parents[3]
ARCHIVE = ROOT/'experiments/reproduction/native_baselines'
PREFIX = 'historical_sources/bfb9100e/'
ORIGINALS = {
    'code/src/scheduling/submission_control/ucb.py': 'ee9b129e0b721bec4c998fb573899c1ec16bcc82073c527c75325073ca7c757e',
    'code/src/scheduling/__init__.py': '6e6e75fdf724d60925b3006d2a70cd9d49288d6fbbb9035e7232d43d4fc5db4e',
    'code/tests/scheduling/test_adaptive_admission.py': '306cca79bfbebc100476762c5bf8ed0ec57c81d3e6de1e2cc721065aa5054e68',
}


class ArchivedControlSourceTests(unittest.TestCase):
    def test_UCB_is_absent_from_current_runtime_and_historical_bytes_stay_exact(self):
        from src import scheduling
        self.assertFalse((ROOT/'code/src/scheduling/submission_control/ucb.py').exists())
        for name in ('UcbConfig', 'UcbAdmissionController', 'SloRewardInput', 'slo_constrained_reward'):
            self.assertNotIn(name, scheduling.__all__)
            with self.assertRaises(AttributeError):
                getattr(scheduling, name)
        manifest = json.loads((ARCHIVE/'MANIFEST.json').read_text())
        for logical, expected in ORIGINALS.items():
            self.assertEqual(hashlib.sha256((ARCHIVE/(PREFIX+logical)).read_bytes()).hexdigest(), expected)
            if logical.startswith('code/src/'):
                for spec in manifest['variants'].values():
                    self.assertEqual(spec['overrides'][logical], PREFIX+logical)
                    self.assertEqual(spec['code_files_sha256'][logical], expected)

    def test_all_five_original_UCB_checks_run_against_the_original_module(self):
        view = SourceView(ROOT, 'archived_UCB', 'native_isolated_and_imlane')
        try:
            module = view.module('scheduling.submission_control.ucb')
            path = ARCHIVE/(PREFIX+'code/tests/scheduling/test_adaptive_admission.py')
            original = ast.parse(path.read_text())
            case = next(n for n in original.body if isinstance(n, ast.ClassDef)
                        and n.name == 'UcbAdmissionControllerTests')
            namespace = dict(unittest=unittest, __name__='_archived_UCB_tests',
                **{name:getattr(module, name) for name in ('UcbConfig', 'UcbAdmissionController',
                    'SloRewardInput', 'slo_constrained_reward')})
            exec(compile(ast.Module(body=[case], type_ignores=[]), str(path), 'exec'), namespace)
            result = unittest.TestResult()
            unittest.defaultTestLoader.loadTestsFromTestCase(namespace[case.name]).run(result)
            self.assertEqual(result.testsRun, 5)
            self.assertEqual(result.failures, [])
            self.assertEqual(result.errors, [])
        finally:
            sys.meta_path.remove(view)
            for name in tuple(sys.modules):
                if name == view.label or name.startswith(view.label+'.'):
                    del sys.modules[name]


if __name__ == '__main__':
    unittest.main()
