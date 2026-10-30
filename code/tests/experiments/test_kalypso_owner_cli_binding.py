"""Existing owner command data and standard CLI resolution; no service/model."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.experiments.kalypso_query_lifecycle import owner_cli_bindings


ROOT=Path(__file__).resolve().parents[3]


class OwnerCommandBindingTests(unittest.TestCase):
    def config(self, root):
        return dict(native_launch='author_defaults',model='CPU-fixture',port=18103,
            baseline_source=str(ROOT),install_root=str(root/'author environment'),
            driver_python=sys.executable,native_cpu_evidence=str(root/'fresh CPU references'))

    def test_bindings_are_command_data_without_launch_or_supervision_side_effects(self):
        with tempfile.TemporaryDirectory() as directory:
            cfg=self.config(Path(directory));original=copy.deepcopy(cfg);env=dict(os.environ)
            with patch.object(subprocess,'Popen',side_effect=AssertionError('binding launched a process')):
                value=owner_cli_bindings(cfg)
            self.assertEqual(cfg,original);self.assertEqual(dict(os.environ),env)
            self.assertEqual(value['server'][:2],[str(Path(cfg['install_root'])/'venv/bin/python'),'-B'])
            self.assertEqual(value['cpu'][:2],value['server'][:2])
            self.assertEqual(value['query'][:2],[sys.executable,'-B'])
            self.assertTrue(value['server'][2].endswith('/run_kalypso_native_server.py'))
            self.assertTrue(value['cpu'][2].endswith('/check_kalypso_native_cpu.py'))
            self.assertTrue(value['query'][2].endswith('/run_kalypso_sembench_query.py'))
            self.assertEqual(value['observer_module'],str(ROOT/'code/src/baselines/text/frameworks/kalypso_native/native_server.py'))
            self.assertEqual(value['cpu_reference_root'],cfg['native_cpu_evidence'])
            self.assertEqual(len(value['server']),3)
            json.dumps(value)

    def test_standard_entries_resolve_from_author_cwd_with_the_returned_pythonpath(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);cfg=self.config(root)
            python=Path(cfg['install_root'])/'venv/bin/python';python.parent.mkdir(parents=True)
            python.symlink_to(sys.executable)
            value=owner_cli_bindings(cfg)
            for role in ('server','cpu','query'):
                with self.subTest(role=role):
                    result=subprocess.run(value[role]+['--help'],cwd=root,
                        env=dict(os.environ,PYTHONPATH=value['pythonpath'],CUDA_VISIBLE_DEVICES='',PYTHONDONTWRITEBYTECODE='1'),
                        capture_output=True,text=True,timeout=10)
                    self.assertEqual(result.returncode,0,result.stderr)
                    self.assertIn('--config',result.stdout)

    def test_stale_overrides_or_incomplete_git_checkout_fail_before_a_command_can_run(self):
        with tempfile.TemporaryDirectory() as directory:
            cfg=self.config(Path(directory))
            with self.assertRaisesRegex(ValueError,'fresh'):owner_cli_bindings(dict(cfg,engine={}))
            with self.assertRaisesRegex(ValueError,'incomplete'):
                owner_cli_bindings(dict(cfg,baseline_source=directory))


if __name__=='__main__':
    unittest.main()
