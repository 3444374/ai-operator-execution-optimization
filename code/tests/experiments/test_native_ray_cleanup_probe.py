"""Reject standalone diagnostic cases before any SDK, fixture or output starts."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[2] / 'scripts/benchmarks/probe_native_ray_cleanup.py'
spec = importlib.util.spec_from_file_location('ray_cleanup_probe', SCRIPT)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class CaseControllerTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.lease_path = self.root / '.host-runner-scope/.runner-lease.json'
        self.lease_path.parent.mkdir()
        self.parent = dict(pid=4321, start_ticks='12345', state='S', ppid=1)
        self.lease = dict(pid=4321, process_start_id='12345', owner_token='local-test-token',
                          repository_commit=probe.COMMIT,
                          config_fingerprint='host-exclusive-experiment-runner-v1')
        self.lease_path.write_text(json.dumps(self.lease))
        self.args = SimpleNamespace(artifact_root=self.root)

    def check(self):
        with patch.object(probe, 'identity', return_value=self.parent), patch.dict(
                probe.os.environ, {probe.CONTROLLER_TOKEN_ENV: 'local-test-token'}, clear=True):
            probe.require_case_controller(self.args)

    def test_live_matching_controller_qualifies_without_sdk(self):
        self.check()

    def test_missing_changed_or_stale_owner_is_rejected(self):
        for field, value in (('pid', 4322), ('process_start_id', 'new-birth'),
                             ('owner_token', 'other-owner'), ('repository_commit', 'other-source'),
                             ('config_fingerprint', 'other-scope')):
            with self.subTest(field=field):
                changed = {**self.lease, field: value}
                self.lease_path.write_text(json.dumps(changed))
                with self.assertRaises(RuntimeError):
                    self.check()
        self.lease_path.write_text(json.dumps(self.lease))
        for state in ('Z', 'unknown'):
            with self.subTest(state=state), patch.dict(self.parent, state=state):
                with self.assertRaises(RuntimeError):
                    self.check()
        for parent in (None, {**self.parent, 'start_ticks': None}):
            with self.subTest(parent=parent), patch.object(probe, 'identity', return_value=parent), patch.dict(
                    probe.os.environ, {probe.CONTROLLER_TOKEN_ENV: 'local-test-token'}, clear=True):
                with self.assertRaises(RuntimeError):
                    probe.require_case_controller(self.args)

    def test_missing_token_lease_root_and_invalid_lease_are_rejected(self):
        with patch.dict(probe.os.environ, {}, clear=True), self.assertRaises(RuntimeError):
            probe.require_case_controller(self.args)
        with patch.dict(probe.os.environ, {probe.CONTROLLER_TOKEN_ENV: 'local-test-token'}, clear=True):
            with self.assertRaises(RuntimeError):
                probe.require_case_controller(SimpleNamespace(artifact_root=None))
            for content in ('[]', '{'):
                with self.subTest(content=content):
                    self.lease_path.write_text(content)
                    with self.assertRaises(RuntimeError):
                        probe.require_case_controller(self.args)
            self.lease_path.unlink()
            with self.assertRaises(RuntimeError):
                probe.require_case_controller(self.args)

    def test_default_plan_and_standalone_case_create_nothing(self):
        before = sorted(p.relative_to(self.root).as_posix() for p in self.root.rglob('*'))
        plan = subprocess.run([sys.executable, '-B', str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(plan.returncode, 0, plan.stderr)
        self.assertEqual(json.loads(plan.stdout)['maximum_fixture_posts'], 8)
        rejected = subprocess.run([sys.executable, '-B', str(SCRIPT), '--case', 'off-eof',
                                   '--output', str(self.root / 'unused')], capture_output=True, text=True)
        self.assertEqual(rejected.returncode, 2)
        self.assertIn('shared-lease controller', rejected.stderr)
        self.assertEqual(sorted(p.relative_to(self.root).as_posix() for p in self.root.rglob('*')), before)

    def test_combining_case_and_execute_is_rejected(self):
        result = subprocess.run([sys.executable, '-B', str(SCRIPT), '--execute', '--case', 'off-eof'],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn('not allowed with argument', result.stderr)


if __name__ == '__main__':
    unittest.main()
