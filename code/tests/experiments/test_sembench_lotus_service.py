"""One LOTUS service entry across preserved visual and aggregate profiles."""

from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from tests.experiments import test_sembench_lotus_image_query as visual_fixture
from tests.experiments import test_sembench_lotus_mmqa_aggregate as aggregate_fixture


@unittest.skipUnless(visual_fixture.CHECKOUT and visual_fixture.HAS_SDK and aggregate_fixture.HAS_SDK,
    'requires the pinned public queries and original SDK')
class NativeLotusServiceEntryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        visual_fixture.NativeLotusImageQueryTests.setUpClass()
        aggregate_fixture.NativeMMQAAggregateTests.setUpClass()

    def fixture(self, aggregate):
        case = (aggregate_fixture.NativeMMQAAggregateTests() if aggregate
                else visual_fixture.NativeLotusImageQueryTests())
        case.setUp()
        self.addCleanup(case.doCleanups)
        return case

    def test_both_profile_schemas_use_the_same_entry_without_model_or_claim(self):
        self.assertIs(visual_fixture.NativeLotusImageQueryTests.entry, aggregate_fixture.NativeMMQAAggregateTests.entry)
        for aggregate in (False, True):
            with self.subTest(aggregate=aggregate):
                case = self.fixture(aggregate)
                config, _ = case.config() if aggregate else case.config('mmqa', 'Q2b')
                data = case.source(selected=1) if aggregate else case.mmqa_inputs()['data_dir']
                ledger, unit = case.ledger(config, 1 if aggregate else 8)
                config, path = case.config(budget=unit) if aggregate else case.config('mmqa', 'Q2b', budget=unit)
                output = case.root/'must-not-create-output'
                for action in ('check', 'run'):
                    args = [action, '--config', str(path), '--sembench-checkout', str(case.checkout), '--data-dir', str(data)]
                    if action == 'run':
                        args += ['--output-dir', str(output)]
                    with redirect_stdout(io.StringIO()) as stdout:
                        self.assertEqual(case.entry.main(args), 0)
                    report = json.loads(stdout.getvalue())
                    self.assertEqual(report['model_calls'], 0)
                self.assertFalse(output.exists())
                self.assertEqual(ledger.snapshot()['units'][0]['claimed'], 0)

    def test_bad_profile_fields_and_missing_cli_inputs_fail_before_lm_setup(self):
        for aggregate in (False, True):
            with self.subTest(aggregate=aggregate):
                case = self.fixture(aggregate)
                config, path = case.config() if aggregate else case.config('mmqa', 'Q2b')
                for changes in (dict(unknown_query_runner_option=1), dict(native_lm_max_batch_size=0),
                                dict(generation=dict(config['generation'], response_format={'type': 'json_object'}))):
                    bad = case.root/'bad-config.json'
                    bad.write_text(json.dumps(dict(config, **changes)))
                    with patch.object(case.entry, '_make_lm', side_effect=AssertionError('bad configuration set up an LM')), \
                            self.assertRaises(ValueError):
                        case.entry.configuration(bad)
                if aggregate:
                    with patch.object(case.entry, '_make_lm', side_effect=AssertionError('missing data set up an LM')), \
                            redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as stopped:
                        case.entry.main(['check', '--config', str(path), '--sembench-checkout', str(case.checkout)])
                    self.assertEqual(stopped.exception.code, 2)

    def test_canonical_help_does_not_import_models_or_frameworks(self):
        code = Path(__file__).resolve().parents[2]
        program = '''
import importlib.abc,sys
sys.path.insert(0,sys.argv[1])
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,*unused):
        if fullname.split('.')[0] in ('lotus','ray','daft','vllm','torch'):
            raise AssertionError('help imported a model/framework: '+fullname)
sys.meta_path.insert(0,Guard())
from src.experiments.sembench_lotus_query import main
main(['--help'])
'''
        result = subprocess.run([sys.executable, '-B', '-c', program, str(code)],
            capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('check', result.stdout)
        self.assertIn('run', result.stdout)


if __name__ == '__main__':
    unittest.main()
