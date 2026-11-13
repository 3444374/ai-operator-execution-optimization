"""Native Ray API fixtures only: no Ray runtime, model or external service."""

import json
from contextlib import redirect_stdout
from io import StringIO
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import pandas as pd

from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.sembench_native_ray import run_query
from tests.baselines.text.ray_native_fixtures import recorded_processor


class RunnerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ledger = CellBudgetLedger.create(self.root / 'budget.sqlite',
            AttemptBudget('fixture', 2), deadline_utc=time.time() + 120)
        self.ledger.reserve_unit('query', 2)
        self.config = dict(model='fixture-model', model_python=sys.executable,
            engine_kwargs={}, processor_options={}, runtime_options={},
            sampling_params=dict(n=1, max_tokens=32, temperature=0,
                structured_outputs={'json_object': True}),
            budget=dict(path=str(self.ledger.path), id='fixture', limit=2, unit_id='query'))

    def task(self, state, *, count=2, parsing_error=False, evaluation_error=False):
        class Task:
            def read_source(current):
                self.assertFalse(state.initialized)
                state.events.append('source')
                return object()
            def requests(current, source):
                return [dict(call_id=str(index), messages=[dict(role='user', content='fixture')])
                        for index in range(count)]
            def finish(current, source, predictions):
                self.assertTrue(state.initialized)
                self.assertEqual(sorted(row['call_id'] for row in predictions), ['0', '1'])
                state.events.append('sink')
                if parsing_error:
                    raise ValueError('strict Boolean parsing failed')
                return pd.DataFrame({'count': [1]})
            def verify_source(current, source):
                self.assertFalse(state.initialized)
                state.events.append('verify')
            def describe(current, source):
                return dict(profile='project-boolean-fixture')
            def evaluate(current, source, result, *, checkout):
                self.assertFalse(state.initialized)
                state.events.append('evaluate')
                if evaluation_error:
                    raise ValueError('official evaluator failed')
                return dict(score=1)
        return Task()

    def run_fixture(self, state, **task_options):
        return run_query(self.task(state, **task_options), self.config,
                         self.root / 'query', checkout='unused-fixture-checkout')

    def test_native_processor_and_complete_sink_before_close(self):
        with recorded_processor({'0': '{"keep":true}', '1': '{"keep":false}'}) as state:
            report = self.run_fixture(state)
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(report['physical_attempts_reserved'], 2)
        self.assertEqual(state.events, ['source', 'init', 'materialize', 'take_all',
                                        'sink', 'close', 'verify', 'evaluate'])
        self.assertEqual(state.requests[0]['sampling_params'], self.config['sampling_params'])
        self.assertIn('ray_native_observer.setup', state.hook)
        self.assertEqual(report['native']['ray_driver_disconnected'], True)
        self.assertIsNone(report['physical_gpu_exit_confirmed'])
        self.assertGreaterEqual(report['sink_ready_ns'], report['source_started_ns'])
        self.assertEqual(report['result']['data'], [[1]])

    def test_parse_failure_keeps_raw_predictions_without_successful_sink(self):
        with recorded_processor({'0': 'invalid', '1': '{"keep":false}'}) as state:
            with self.assertRaisesRegex(ValueError, 'strict Boolean') as raised:
                self.run_fixture(state, parsing_error=True)
        report = raised.exception.query_report
        self.assertEqual(report['status'], 'failed')
        self.assertIsNone(report['sink_ready_ns'])
        self.assertNotIn('quality', report)
        native = json.loads((self.root / 'query/native-predictions.json').read_text())
        self.assertEqual(len(native['batches'][0]['predictions']), 2)
        self.assertEqual(report['physical_attempts_reserved'], 2)
        self.assertIn('close', state.events)

    def test_close_failure_keeps_complete_result_and_original_failure(self):
        with recorded_processor({'0': '{"keep":true}', '1': '{"keep":false}'},
                                close_error=RuntimeError('native close failed')) as state:
            with self.assertRaisesRegex(RuntimeError, 'native close failed') as raised:
                self.run_fixture(state)
        report = raised.exception.query_report
        self.assertIsNotNone(report['sink_ready_ns'])
        self.assertEqual(report['result']['data'], [[1]])
        self.assertEqual(report['status'], 'failed')
        self.assertNotIn('quality', report)

    def test_post_sink_evaluator_failure_keeps_complete_result(self):
        with recorded_processor({'0': '{"keep":true}', '1': '{"keep":false}'}) as state:
            with self.assertRaisesRegex(ValueError, 'official evaluator failed') as raised:
                self.run_fixture(state, evaluation_error=True)
        self.assertEqual(raised.exception.query_report['result']['data'], [[1]])
        self.assertEqual(raised.exception.query_report['status'], 'failed')

    def test_insufficient_permanent_budget_stops_before_native_startup(self):
        with recorded_processor({}) as state:
            with self.assertRaises(BudgetExhausted) as raised:
                self.run_fixture(state, count=3)
        self.assertEqual(state.events, ['source'])
        self.assertEqual(raised.exception.query_report['physical_attempts_reserved'], 0)

    def test_invalid_generation_stops_before_source_or_budget_claim(self):
        before = self.ledger.snapshot()
        with recorded_processor({}) as state:
            for sampling in (dict(n=True, structured_outputs={'json_object': True}),
                             dict(n=2, structured_outputs={'json_object': True}), dict(n=1)):
                with self.subTest(sampling=sampling), self.assertRaises(ValueError):
                    run_query(self.task(state), dict(self.config, sampling_params=sampling),
                              self.root / 'must-not-exist', checkout='unused')
        self.assertEqual(state.events, [])
        self.assertFalse((self.root / 'must-not-exist').exists())
        self.assertEqual(self.ledger.snapshot(), before)

    def test_post_evaluation_persistence_failure_keeps_result_without_success_quality(self):
        from src.experiments import sembench_native_ray as entry

        original = entry.write_private_json
        def write(path, value):
            if path.name == 'requests.json':
                raise OSError('software evidence write failed')
            return original(path, value)
        with recorded_processor({'0': '{"keep":true}', '1': '{"keep":false}'}) as state:
            with patch.object(entry, 'write_private_json', side_effect=write):
                with self.assertRaisesRegex(OSError, 'software evidence write failed') as raised:
                    self.run_fixture(state)
        report = raised.exception.query_report
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['result']['data'], [[1]])
        self.assertNotIn('quality', report)
        self.assertEqual(report['physical_attempts_reserved'], 2)

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_neutral_movie_task_and_official_evaluator(self):
        from src.baselines.text.sembench_native_text import MovieFilterTask

        source = self.root / 'Reviews.csv'
        pd.DataFrame([
            dict(reviewId='outside', id='other', reviewText='Good', scoreSentiment='POSITIVE'),
            dict(reviewId='duplicate', id='taken_3', reviewText='Good', scoreSentiment='POSITIVE'),
            dict(reviewId='duplicate', id='taken_3', reviewText='Bad', scoreSentiment='NEGATIVE'),
        ]).to_csv(source, index=False)
        task = MovieFilterTask(source, query='Q3', execution_id='native')
        with recorded_processor({'native-row-1': '{"keep":true}',
                                 'native-row-2': '{"keep":false}'}) as state:
            report = run_query(task, self.config, self.root / 'query',
                               checkout=os.environ['SEMBENCH_CHECKOUT'])
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(report['result']['data'], [[1]])
        self.assertEqual(report['task']['model_columns'], ['reviewText'])
        self.assertEqual(report['task']['source']['rows'], 3)
        self.assertEqual(report['task']['source']['eligible_rows'], 2)
        self.assertIn('original_metric', report['quality'])
        self.assertEqual([json.loads(row['messages'][1]['content'])['text']
                          for row in state.requests], ['Good', 'Bad'])

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_CLI_checks_without_claiming_budget_or_starting_native_execution(self):
        from src.experiments import sembench_native_ray as entry

        source = self.root / 'Reviews.csv'
        pd.DataFrame([dict(reviewId='row', id='taken_3', reviewText='Good',
                           scoreSentiment='POSITIVE')]).to_csv(source, index=False)
        config = dict(self.config, schema='semloom.sembench_native_ray.v1',
            sembench_checkout=os.environ['SEMBENCH_CHECKOUT'], reviews_csv=str(source), query='Q3', query_id='CLI')
        path = self.root / 'config.json'
        path.write_text(json.dumps(config))
        before = self.ledger.snapshot()
        with patch.object(entry, 'run_query', side_effect=AssertionError('native execution forbidden')):
            for action in ('check', 'run'):
                with self.subTest(action=action), redirect_stdout(StringIO()) as output:
                    self.assertEqual(entry.main([action, '--config', str(path)]), 0)
                self.assertFalse(json.loads(output.getvalue())['execution_enabled'])
        self.assertEqual(self.ledger.snapshot(), before)

    def test_fresh_subprocess_forbids_lotus_imports(self):
        script = '''
import importlib.abc
import os
import sys
import unittest
def forbidden(name):
    return name == 'lotus' or name.startswith(('lotus.', 'src.semantic_methods.lotus', 'semantic_methods.lotus'))
class BlockLotus(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if forbidden(fullname):
            raise AssertionError('forbidden LOTUS import: ' + fullname)
sys.meta_path.insert(0, BlockLotus())
from tests.experiments.test_sembench_native_ray import RunnerTests
suite = unittest.TestSuite([RunnerTests('test_native_processor_and_complete_sink_before_close')])
if os.environ.get('SEMBENCH_CHECKOUT'):
    suite.addTest(RunnerTests('test_neutral_movie_task_and_official_evaluator'))
result = unittest.TextTestRunner().run(suite)
assert not any(forbidden(name) for name in sys.modules)
raise SystemExit(not result.wasSuccessful())
'''
        outcome = subprocess.run([sys.executable, '-B', '-c', script], env=dict(os.environ),
            text=True, capture_output=True, timeout=30)
        self.assertEqual(outcome.returncode, 0, outcome.stdout + outcome.stderr)


if __name__ == '__main__':
    unittest.main()
