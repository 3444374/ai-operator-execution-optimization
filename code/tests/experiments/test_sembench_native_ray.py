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
            def prepare_evaluation(current):
                return None
            def read_source(current):
                self.assertFalse(state.initialized)
                state.events.append('source')
                return object()
            def requests(current, source):
                return [dict(call_id=str(index), messages=[dict(role='user', content='fixture')])
                        for index in range(count)]
            def model_request_count(current, source):
                return count
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
            def evaluate(current, source, result, *, checkout, evaluation=None):
                self.assertFalse(state.initialized)
                state.events.append('evaluate')
                if evaluation_error:
                    raise ValueError('official evaluator failed')
                return dict(score=1)
        return Task()

    def run_fixture(self, state, **task_options):
        return run_query(self.task(state, **task_options), self.config,
                         self.root / 'query', checkout='unused-fixture-checkout')

    def join_source(self):
        source = self.root / 'Reviews.csv'
        pd.DataFrame(dict(id=['ant_man_and_the_wasp_quantumania'] * 3,
            reviewId=['duplicate', 'duplicate', 'other'], reviewText=['good first', 'bad', 'good second'],
            scoreSentiment=['POSITIVE', 'NEGATIVE', 'POSITIVE'])).to_csv(source, index=False)
        return source

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

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_empty_source_keeps_ready_not_applicable_without_starting_native_execution(self):
        from src.baselines.text.frameworks.ray_data_sembench_native import _NativeQueryOwner
        from src.baselines.text.sembench_native_text import movie_text_task

        source = self.root / 'Reviews.csv'
        pd.DataFrame([dict(reviewId='unselected', id='other', reviewText='Good',
                           scoreSentiment='POSITIVE')]).to_csv(source, index=False)
        task = movie_text_task(source, 'Q3', 'empty')
        with patch.object(_NativeQueryOwner, '_start', side_effect=AssertionError('native startup forbidden')):
            report = run_query(task, self.config, self.root / 'query', checkout=os.environ['SEMBENCH_CHECKOUT'])
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(report['result']['data'], [[0]])
        self.assertEqual(report['logical_requests'], 0)
        self.assertEqual(report['physical_attempts_reserved'], 0)
        self.assertIsNone(report['model_ready_before_source'])
        self.assertIn('not started', report['model_ready_before_source_reason'])

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
    def test_map_tasks_reuse_the_native_processor_and_complete_official_sinks(self):
        from src.baselines.text.sembench_native_text import movie_text_task

        for query in ('Q8', 'Q9'):
            with self.subTest(query=query):
                root = self.root / query
                root.mkdir()
                source = root / 'Reviews.csv'
                movie = 'taken_3' if query == 'Q8' else 'ant_man_and_the_wasp_quantumania'
                pd.DataFrame([
                    dict(reviewId='outside', id='other', reviewText='Outside', scoreSentiment='POSITIVE', originalScore='4/5'),
                    dict(reviewId='good', id=movie, reviewText='Good', scoreSentiment='POSITIVE', originalScore='5/5'),
                    dict(reviewId='bad', id=movie, reviewText='Bad', scoreSentiment='NEGATIVE', originalScore='1/5'),
                ]).to_csv(source, index=False)
                ledger = CellBudgetLedger.create(root / 'budget.sqlite',
                    AttemptBudget(query, 2), deadline_utc=time.time() + 120)
                ledger.reserve_unit('query', 2)
                config = dict(self.config, budget=dict(path=str(ledger.path), id=query, limit=2, unit_id='query'))
                task = movie_text_task(source, query, 'native', ranking='map' if query == 'Q9' else None)
                values = ({'sentiment': 'POSITIVE'}, {'sentiment': 'NEGATIVE'}) if query == 'Q8' else ({'score': 5}, {'score': 1})
                with recorded_processor({f'native-row-{index}': json.dumps(value)
                                         for index, value in zip((1, 2), values)}) as state:
                    report = run_query(task, config, root / 'query', checkout=os.environ['SEMBENCH_CHECKOUT'])
                expected = [['NEGATIVE', 1], ['POSITIVE', 1]] if query == 'Q8' else [['good', 5.0], ['bad', 1.0]]
                self.assertEqual(report['result']['data'], expected)
                self.assertEqual(report['status'], 'completed')
                self.assertEqual(report['baseline_identity'], 'ray-native-project-json')
                self.assertEqual(report['physical_attempts_reserved'], 2)
                self.assertIn('original_metric', report['quality'])
                metric = report['quality']['original_metric']
                self.assertAlmostEqual(metric['absolute_error'] if query == 'Q8' else metric['spearman_correlation'],
                                       0 if query == 'Q8' else 1)
                self.assertNotIn('originalScore', json.dumps(state.requests))
                self.assertGreaterEqual(report['evaluation_started_ns'], report['sink_ready_ns'])

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_Q10_scores_all_reviews_then_groups_movies_before_official_evaluation(self):
        from src.baselines.text.sembench_native_text import movie_text_task

        source, movies = self.root / 'Reviews.csv', self.root / 'Movies.csv'
        pd.DataFrame(dict(id=['B', 'A', 'A'], reviewText=['Bad B', 'Good A', 'Bad A'])).to_csv(source, index=False)
        pd.DataFrame(dict(id=['A', 'B'], audienceScore=[100, 20])).to_csv(movies, index=False)
        ledger = CellBudgetLedger.create(self.root / 'q10-budget.sqlite',
            AttemptBudget('Q10', 3), deadline_utc=time.time() + 120)
        ledger.reserve_unit('query', 3)
        config = dict(self.config, budget=dict(path=str(ledger.path), id='Q10', limit=3, unit_id='query'))
        task = movie_text_task(source, 'Q10', 'native', movies_csv=movies)
        with recorded_processor({f'native-row-{index}': json.dumps({'score': value})
                                 for index, value in enumerate((1, 5, 1))}) as state:
            report = run_query(task, config, self.root / 'query', checkout=os.environ['SEMBENCH_CHECKOUT'])
        self.assertEqual(report['result']['data'], [['A', 3.0], ['B', 1.0]])
        self.assertEqual(report['physical_attempts_reserved'], 3)
        self.assertAlmostEqual(report['quality']['original_metric']['spearman_correlation'], 1)
        self.assertNotIn('audienceScore', json.dumps(state.requests))
        self.assertEqual([json.loads(row['messages'][1]['content'])['text']
                          for row in state.requests], ['Bad B', 'Good A', 'Bad A'])
        self.assertGreaterEqual(report['evaluation_started_ns'], report['sink_ready_ns'])

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_join_tasks_predict_all_pairs_before_original_ID_filter_and_sink(self):
        from src.baselines.text.sembench_native_text import movie_text_task

        source = self.join_source()
        movie = 'ant_man_and_the_wasp_quantumania'
        for query in ('Q5', 'Q6', 'Q7'):
            with self.subTest(query=query):
                root = self.root / query
                root.mkdir()
                ledger = CellBudgetLedger.create(root / 'budget.sqlite',
                    AttemptBudget(query, 9), deadline_utc=time.time() + 120)
                ledger.reserve_unit('query', 9)
                config = dict(self.config, budget=dict(path=str(ledger.path), id=query, limit=9, unit_id='query'))
                task = movie_text_task(source, query, 'native')
                def reply(row, request):
                    pair = json.loads(request['messages'][1]['content'])
                    same = pair['left_text'].startswith('good') == pair['right_text'].startswith('good')
                    return json.dumps({'keep': same if query == 'Q5' else not same})
                with recorded_processor(reply) as state:
                    report = run_query(task, config, root / 'query', checkout=os.environ['SEMBENCH_CHECKOUT'])
                self.assertEqual(report['result']['data'], [[movie, 'duplicate', 'other'], [movie, 'other', 'duplicate']])
                self.assertEqual(report['physical_attempts_reserved'], 9)
                self.assertEqual(len(state.requests), 9)
                ids = {row['call_id'] for row in state.requests}
                self.assertIn('native-left-0-right-0', ids)
                self.assertIn('native-left-0-right-1', ids)
                self.assertEqual(report['quality']['original_metric']['f1_score'], 1)
                self.assertTrue(all(set(json.loads(row['messages'][1]['content'])) == {'left_text', 'right_text'}
                                    for row in state.requests))

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_join_budget_rejection_precedes_pair_construction_and_native_start(self):
        from src.baselines.text.sembench_native_text import movie_text_task

        task = movie_text_task(self.join_source(), 'Q5', 'native')
        with patch.object(type(task), 'requests', side_effect=AssertionError('pairs must not be constructed')) as requests:
            with recorded_processor({}) as state:
                with self.assertRaises(BudgetExhausted) as raised:
                    run_query(task, self.config, self.root / 'query', checkout=os.environ['SEMBENCH_CHECKOUT'])
        requests.assert_not_called()
        self.assertEqual(state.events, [])
        self.assertEqual(raised.exception.query_report['physical_attempts_reserved'], 0)

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_CLI_counts_join_pairs_for_all_three_original_tasks(self):
        from src.experiments import sembench_native_ray as entry

        base = dict(self.config, schema='semloom.sembench_native_ray.v1',
            sembench_checkout=os.environ['SEMBENCH_CHECKOUT'], reviews_csv=str(self.join_source()), query_id='CLI')
        path = self.root / 'join-config.json'
        with patch.object(entry, 'run_query', side_effect=AssertionError('native execution forbidden')):
            for query in ('Q5', 'Q6', 'Q7'):
                path.write_text(json.dumps(dict(base, query=query, budget=dict(self.config['budget'], limit=9))))
                with self.subTest(query=query), redirect_stdout(StringIO()) as output:
                    self.assertEqual(entry.main(['check', '--config', str(path)]), 0)
                self.assertEqual(json.loads(output.getvalue())['task']['expected_model_calls'], 9)
            path.write_text(json.dumps(dict(base, query='Q5', budget=dict(self.config['budget'], limit=8))))
            with self.assertRaisesRegex(ValueError, 'eligible source'):
                entry.main(['check', '--config', str(path)])

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

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_CLI_requires_Q10_gold_and_rejects_gold_for_other_tasks(self):
        from src.experiments import sembench_native_ray as entry

        source, movies = self.root / 'Reviews.csv', self.root / 'Movies.csv'
        pd.DataFrame([dict(id='A', reviewText='Good')]).to_csv(source, index=False)
        pd.DataFrame([dict(id='A', audienceScore=100)]).to_csv(movies, index=False)
        base = dict(self.config, schema='semloom.sembench_native_ray.v1',
            sembench_checkout=os.environ['SEMBENCH_CHECKOUT'], reviews_csv=str(source), query_id='CLI')
        path = self.root / 'q10-config.json'
        before = self.ledger.snapshot()
        path.write_text(json.dumps(dict(base, query='Q10', movies_csv=str(movies))))
        with patch.object(entry, 'run_query', side_effect=AssertionError('native execution forbidden')):
            with redirect_stdout(StringIO()) as output:
                self.assertEqual(entry.main(['check', '--config', str(path)]), 0)
            self.assertEqual(json.loads(output.getvalue())['task']['query'], 'Q10')
            for task in (dict(query='Q10'), dict(query='Q10', movies_csv=str(movies), ranking='map'),
                         dict(query='Q3', movies_csv=str(movies)), dict(query='Q8', movies_csv=str(movies)),
                         dict(query='Q9', ranking='map', movies_csv=str(movies))):
                path.write_text(json.dumps(dict(base, **task)))
                with self.subTest(task=task), self.assertRaises(ValueError):
                    entry.main(['check', '--config', str(path)])
        self.assertEqual(self.ledger.snapshot(), before)
        from src.baselines.text.sembench_native_text import movie_text_task
        missing = movie_text_task(source, 'Q10', 'missing-gold', movies_csv=self.root / 'missing-Movies.csv')
        with recorded_processor({}) as state:
            with self.assertRaises(FileNotFoundError) as raised:
                run_query(missing, self.config, self.root / 'missing-gold-run',
                          checkout=os.environ['SEMBENCH_CHECKOUT'])
        self.assertEqual(state.events, [])
        self.assertIsNone(raised.exception.query_report['source_started_ns'])
        self.assertEqual(self.ledger.snapshot(), before)

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_CLI_dispatches_maps_and_requires_the_explicit_Q9_map_profile(self):
        from src.experiments import sembench_native_ray as entry

        source = self.root / 'Reviews.csv'
        pd.DataFrame([dict(reviewId='row', id='ant_man_and_the_wasp_quantumania', reviewText='Good',
                           scoreSentiment='POSITIVE', originalScore='5/5')]).to_csv(source, index=False)
        base = dict(self.config, schema='semloom.sembench_native_ray.v1',
            sembench_checkout=os.environ['SEMBENCH_CHECKOUT'], reviews_csv=str(source), query_id='CLI')
        path = self.root / 'config.json'
        before = self.ledger.snapshot()
        with patch.object(entry, 'run_query', side_effect=AssertionError('native execution forbidden')):
            for task in (dict(query='Q8'), dict(query='Q9', ranking='map')):
                path.write_text(json.dumps(dict(base, **task)))
                with self.subTest(task=task), redirect_stdout(StringIO()) as output:
                    self.assertEqual(entry.main(['check', '--config', str(path)]), 0)
                self.assertEqual(json.loads(output.getvalue())['task']['query'], task['query'])
            for task in (dict(query='Q9'), dict(query='Q9', ranking='topk'), dict(query='Q8', ranking='map')):
                path.write_text(json.dumps(dict(base, **task)))
                with self.subTest(task=task), self.assertRaises(ValueError):
                    entry.main(['check', '--config', str(path)])
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
    suite.addTest(RunnerTests('test_map_tasks_reuse_the_native_processor_and_complete_official_sinks'))
    suite.addTest(RunnerTests('test_Q10_scores_all_reviews_then_groups_movies_before_official_evaluation'))
    suite.addTest(RunnerTests('test_join_tasks_predict_all_pairs_before_original_ID_filter_and_sink'))
result = unittest.TextTestRunner().run(suite)
assert not any(forbidden(name) for name in sys.modules)
raise SystemExit(not result.wasSuccessful())
'''
        outcome = subprocess.run([sys.executable, '-B', '-c', script], env=dict(os.environ),
            text=True, capture_output=True, timeout=30)
        self.assertEqual(outcome.returncode, 0, outcome.stdout + outcome.stderr)


if __name__ == '__main__':
    unittest.main()
