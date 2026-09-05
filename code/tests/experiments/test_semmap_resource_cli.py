"""Command-line parsing and nonzero refusal preserve existing evidence."""
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest
from src.experiments.postgresql.semmap_resource_runner import parse_args
from src.experiments.postgresql.resource_lifecycle import RunSpec


class CliTests(unittest.TestCase):
    def test_diagnostic_parse_has_no_global_effect(self):
        base=['--repo','.','--root','new','--prefix','pg','--commit','abc']
        diagnostic=parse_args(base+['--diagnostic'])
        formal=parse_args(base)
        self.assertTrue(diagnostic.diagnostic)
        self.assertFalse(formal.diagnostic)
        self.assertEqual(RunSpec('formal').rounds,3)
        self.assertFalse(hasattr(formal,'client'))
        configured=parse_args(base+['--pg-user','fixture_owner','--pg-port','55499'])
        self.assertEqual((formal.pg_user,formal.pg_port),('postgres',55446))
        self.assertEqual((configured.pg_user,configured.pg_port),('fixture_owner',55499))

    def test_real_entry_refuses_existing_root_without_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            sentinel=root/'summary.json'
            sentinel.write_text('unchanged')
            command=[sys.executable,'code/scripts/experiments/run_semmap_resource_checks.py',
                '--repo','.', '--root',str(root),'--prefix','missing','--commit','abc']
            result=subprocess.run(command,capture_output=True,text=True)
            self.assertEqual(result.returncode,3,result.stderr)
            self.assertEqual(sentinel.read_text(),'unchanged')
            self.assertEqual(list(root.iterdir()),[sentinel])


if __name__=='__main__': unittest.main()
