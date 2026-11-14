"""Native Ray API fixtures only: no Ray runtime, model or external service."""

import json
import base64
import asyncio
from contextlib import redirect_stdout
from io import BytesIO, StringIO
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.sembench_native_ray import run_query
from tests.baselines.text.ray_native_fixtures import recorded_processor


class SoftwareServeSession:
    """One fake model plus the real finite observer, without importing Ray."""
    startup_calls = 0
    unknown = mismatch = close_failure = False

    def __init__(self, **options):
        from src.baselines.text.frameworks.ray_native_observer import install
        from src.experiments.shared_request_budget import SharedClaimedUnit
        from src.infrastructure.private_artifacts import write_private_json

        self.spec = json.loads(options['model_runtime_env']['env_vars']['SEMLOOM_RAY_NATIVE_OBSERVER'])
        self.root = Path(self.spec['output'])
        self.unit = SharedClaimedUnit(Path(self.spec['path']),
            AttemptBudget(self.spec['id'], self.spec['limit']), self.spec['unit_id'])
        self.events, self.inputs = [], []
        self.report = dict(events={}, batches=[], deployment_startup_seconds=0.001,
                           actual_native_Daft_or_Ray=False)
        fixture = self
        def emit(event, **data):
            with (self.root/'native-events-software.jsonl').open('a') as stream:
                stream.write(json.dumps(dict(event=event, monotonic_ns=time.monotonic_ns(), **data))+'\n')
        class Model:
            def __init__(self):
                pass
            async def generate(self, prompt, sampling_params, request_id):
                text = json.loads(prompt[-1]['content'])['text']
                yield SimpleNamespace(finished=not fixture.unknown, prompt_token_ids=[1], outputs=[
                    SimpleNamespace(text=json.dumps(dict(keep='good' in text)), token_ids=[2], finish_reason='stop')])
        install(Model, self.unit, emit,
            lambda attempt, value: write_private_json(self.root/('native-output-'+str(attempt)+'.json'), value))
        self.Model, self.emit = Model, emit

    def start(self):
        self.events.append('start')
        self.emit('native_observer_installed', versions={'ray': 'software'}, python=sys.executable, prefix=sys.prefix)
        self.model = self.Model()
        for index in range(self.startup_calls):
            asyncio.run(self.generate(dict(call_id='startup-'+str(index), messages=[dict(
                role='user', content=json.dumps(dict(text='startup')))])))
        self.report['events']['deployment_ready_ns'] = time.monotonic_ns()

    async def generate(self, request):
        key = 'chatcmpl-'+request['call_id']
        async for output in self.model.generate(request['messages'], SimpleNamespace(n=1), key):
            text = output.outputs[0].text
        return dict(call_id=request['call_id'], generated_text=text,
                    response_id='wrong' if self.mismatch else key)

    def predict(self, requests, *, sampling_params):
        self.events.append('predict')
        self.inputs.append(requests)
        predictions = [asyncio.run(self.generate(request)) for request in requests]
        batch = dict(predictions=predictions[::-1])
        self.report['batches'].append(batch)
        return batch

    def close(self):
        import sqlite3
        with sqlite3.connect(Path(self.spec['path']).as_uri()+'?mode=ro', uri=True) as db:
            self.closed_unit = bool(db.execute('select 1 from closed_shared_units where unit_id=?',
                                              (self.spec['unit_id'],)).fetchone())
        self.events.append('close')
        if self.close_failure:
            raise ValueError('software owned close failed')


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
            @staticmethod
            def encode_result(result):
                return result.to_dict(orient='split')
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

    def mmqa_source(self):
        pd.DataFrame(dict(Title=['Film', 'Film', 'Other'], Role=['Other', 'Bob Whitewood', 'Other'])).to_csv(
            self.root / 'ben_piazza.csv', index=False)
        pd.DataFrame(dict(title=['Film', 'Other'], text=['first director', 'second director'])).to_csv(
            self.root / 'ben_piazza_text_data.csv', index=False)
        pd.DataFrame(dict(title=['Mean Girls', 'Ignored', 'Allied'], text=['first genres', 'unused', 'second genres'])).to_csv(
            self.root / 'lizzy_caplan_text_data.csv', index=False)
        return self.root

    def animals_source(self):
        from PIL import Image
        from src.baselines.image import sembench_animals as animals
        from src.infrastructure.private_artifacts import content_digest

        rows = []
        for index, color in enumerate(('red', 'blue')):
            path = self.root / ('image-' + str(index) + '.png')
            Image.new('RGB', (3, 2), color).save(path)
            rows.append(dict(row_id='animals-row-' + str(index), ImagePath=str(path),
                Species='ZEBRA' if index == 0 else 'IMPALA', City='City' + str(index),
                StationID='Station' + str(index), image=animals.image_identity(path)))
        value = dict(schema=animals.MANIFEST_SCHEMA, role='software_fixture',
            sembench_commit=animals.SEMBENCH_COMMIT, rows=rows)
        value['sha256'] = content_digest(value)
        manifest = self.root / 'animals.json'
        manifest.write_text(json.dumps(value))
        return dict(scenario='animals', manifest=str(manifest), manifest_digest=value['sha256'],
            query='Q1', query_id='animals', sembench_checkout=os.environ['SEMBENCH_CHECKOUT'])

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
    def test_MMQA_extract_preserves_Series_and_table_sinks_including_close_failure(self):
        from src.baselines.text.sembench_native_text import task_from_config

        data = self.mmqa_source()
        for query, close_error in (('Q1', None), ('Q4', None), ('Q1', RuntimeError('native close failed'))):
            with self.subTest(query=query, close_error=close_error):
                case = query + ('-close' if close_error else '')
                root = self.root / case
                root.mkdir()
                ledger = CellBudgetLedger.create(root / 'budget.sqlite',
                    AttemptBudget(case, 2), deadline_utc=time.time() + 120)
                ledger.reserve_unit('query', 2)
                config = dict(self.config, budget=dict(path=str(ledger.path), id=case, limit=2, unit_id='query'))
                task = task_from_config(dict(scenario='mmqa', data_dir=str(data), query=query, query_id='extract'))
                replies = ({'extract-row-0': '{"director":"Michael Ritchie"}', 'extract-row-1': '{"director":"Other"}'}
                    if query == 'Q1' else {'extract-row-0': '{"genres":"comedy"}', 'extract-row-2': '{"genres":"drama, romance"}'})
                with recorded_processor(replies, close_error=close_error):
                    if close_error:
                        with self.assertRaisesRegex(RuntimeError, 'native close failed') as raised:
                            run_query(task, config, root / 'query', checkout=os.environ['SEMBENCH_CHECKOUT'])
                        report = raised.exception.query_report
                        self.assertEqual(report['status'], 'failed')
                        self.assertNotIn('quality', report)
                    else:
                        report = run_query(task, config, root / 'query', checkout=os.environ['SEMBENCH_CHECKOUT'])
                        self.assertEqual(report['quality']['original_metric']['precision'], 1)
                if query == 'Q1':
                    self.assertEqual(report['result'], dict(kind='series', name='director', index=[1],
                        data=['Michael Ritchie'], dtype='object'))
                else:
                    self.assertEqual(report['result']['data'], [['comedy', 'Mean Girls'], ['drama', 'Allied'], ['romance', 'Allied']])
                self.assertEqual(report['physical_attempts_reserved'], 2)
                self.assertIsNotNone(report['sink_ready_ns'])

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_CLI_selects_MMQA_source_fields_without_movie_configuration(self):
        from src.experiments import sembench_native_ray as entry

        base = dict(self.config, schema='semloom.sembench_native_ray.v1', scenario='mmqa',
            data_dir=str(self.mmqa_source()), query_id='CLI', sembench_checkout=os.environ['SEMBENCH_CHECKOUT'])
        path = self.root / 'mmqa-config.json'
        before = self.ledger.snapshot()
        with patch.object(entry, 'run_query', side_effect=AssertionError('native execution forbidden')):
            for query in ('Q1', 'Q4'):
                path.write_text(json.dumps(dict(base, query=query)))
                with self.subTest(query=query), redirect_stdout(StringIO()) as output:
                    self.assertEqual(entry.main(['check', '--config', str(path)]), 0)
                self.assertEqual(json.loads(output.getvalue())['task']['scenario'], 'mmqa')
        self.assertEqual(self.ledger.snapshot(), before)

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_Animals_PNG_content_reaches_the_native_processor_and_original_sink(self):
        from PIL import Image
        from src.baselines.text.sembench_native_text import task_from_config

        settings = self.animals_source()
        for index, stage in enumerate((True, {}, {'enabled': True})):
            with self.subTest(stage=stage):
                root = self.root / ('enabled-' + str(index))
                root.mkdir()
                ledger = CellBudgetLedger.create(root / 'budget.sqlite',
                    AttemptBudget('images-' + str(index), 2), deadline_utc=time.time() + 120)
                ledger.reserve_unit('query', 2)
                config = dict(self.config, processor_options=dict(prepare_image_stage=stage),
                    budget=dict(path=str(ledger.path), id='images-' + str(index), limit=2, unit_id='query'))
                with recorded_processor(lambda row, request: json.dumps({'keep': row['call_id'].endswith('-0')})) as state:
                    report = run_query(task_from_config(settings), config, root / 'query', checkout=settings['sembench_checkout'])
                self.assertEqual(report['status'], 'completed')
                self.assertEqual(report['result']['data'], [[1]])
                self.assertEqual(report['quality']['original_metric']['absolute_error'], 0)
                self.assertEqual(report['physical_attempts_reserved'], 2)
                original = json.loads((root / 'query/requests.json').read_text())
                for row, saved in zip(state.requests, original, strict=True):
                    self.assertEqual(row['messages'], saved['messages'])
                    parts = row['messages'][1]['content']
                    raw = base64.b64decode(parts[1]['image_url']['url'].split(',', 1)[1])
                    self.assertTrue(raw.startswith(b'\x89PNG\r\n\x1a\n'))
                    with Image.open(BytesIO(raw)) as image, Image.open(self.root / ('image-' + row['call_id'][-1] + '.png')) as source:
                        self.assertEqual(image.size, (3, 2))
                        self.assertEqual(image.convert('RGB').tobytes(), source.convert('RGB').tobytes())
                    self.assertNotIn('Species', json.dumps(row['messages']))
                self.assertEqual(state.configs[0]['prepare_image_stage'], stage)

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_Animals_missing_or_disabled_image_stage_stops_before_native_start(self):
        from src.baselines.text.sembench_native_text import task_from_config

        settings = self.animals_source()
        for index, options in enumerate(({}, {'prepare_image_stage': False}, {'prepare_image_stage': {'enabled': False}})):
            with self.subTest(options=options):
                root = self.root / ('disabled-' + str(index))
                root.mkdir()
                ledger = CellBudgetLedger.create(root / 'budget.sqlite',
                    AttemptBudget('disabled-' + str(index), 2), deadline_utc=time.time() + 120)
                ledger.reserve_unit('query', 2)
                config = dict(self.config, processor_options=options,
                    budget=dict(path=str(ledger.path), id='disabled-' + str(index), limit=2, unit_id='query'))
                with recorded_processor({}) as state:
                    with self.assertRaisesRegex(ValueError, 'prepare_image_stage') as raised:
                        run_query(task_from_config(settings), config, root / 'query', checkout=settings['sembench_checkout'])
                self.assertEqual(state.events, [])
                self.assertEqual(raised.exception.query_report['physical_attempts_reserved'], 0)

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind the fixed official SemBench checkout')
    def test_CLI_checks_known_image_stage_without_requests_budget_or_output(self):
        from src.experiments import sembench_native_ray as entry
        from src.baselines.text import sembench_native_text as tasks

        settings = self.animals_source()
        task = tasks.task_from_config(settings)
        base = dict(self.config, schema='semloom.sembench_native_ray.v1', **settings)
        path, output_dir = self.root / 'image-config.json', self.root / 'not-created'
        before = self.ledger.snapshot()
        with patch.object(tasks, 'task_from_config', return_value=task), \
                patch.object(type(task), 'requests', side_effect=AssertionError('check must not expand requests')) as requests, \
                patch.object(entry, 'run_query', side_effect=AssertionError('native execution forbidden')):
            for stage in (True, {}):
                path.write_text(json.dumps(dict(base, processor_options=dict(prepare_image_stage=stage))))
                with self.subTest(stage=stage), redirect_stdout(StringIO()) as output:
                    self.assertEqual(entry.main(['check', '--config', str(path), '--output-dir', str(output_dir)]), 0)
                self.assertFalse(json.loads(output.getvalue())['execution_enabled'])
            for options in ({}, {'prepare_image_stage': False}, {'prepare_image_stage': {'enabled': False}}):
                path.write_text(json.dumps(dict(base, processor_options=options)))
                with self.subTest(options=options), self.assertRaisesRegex(ValueError, 'prepare_image_stage'):
                    entry.main(['check', '--config', str(path), '--output-dir', str(output_dir)])
        requests.assert_not_called()
        self.assertEqual(self.ledger.snapshot(), before)
        self.assertFalse(output_dir.exists())

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
from tests.experiments.test_sembench_native_ray import RunnerTests, ServeSessionTests, NativeMultistageTests
suite = unittest.TestSuite([RunnerTests('test_native_processor_and_complete_sink_before_close'),
    NativeMultistageTests('test_offline_dependent_second_batch_preserves_occurrences_and_final_sink_before_close'),
    NativeMultistageTests('test_serve_two_dependent_queries_use_one_model_owner_and_audit_every_batch')])
if os.environ.get('SEMBENCH_CHECKOUT'):
    suite.addTest(RunnerTests('test_neutral_movie_task_and_official_evaluator'))
    suite.addTest(RunnerTests('test_map_tasks_reuse_the_native_processor_and_complete_official_sinks'))
    suite.addTest(RunnerTests('test_Q10_scores_all_reviews_then_groups_movies_before_official_evaluation'))
    suite.addTest(RunnerTests('test_join_tasks_predict_all_pairs_before_original_ID_filter_and_sink'))
    suite.addTest(RunnerTests('test_MMQA_extract_preserves_Series_and_table_sinks_including_close_failure'))
    suite.addTest(RunnerTests('test_Animals_PNG_content_reaches_the_native_processor_and_original_sink'))
    suite.addTest(ServeSessionTests('test_fresh_movie_queries_use_one_owner_and_original_scoring'))
result = unittest.TextTestRunner().run(suite)
assert not any(forbidden(name) for name in sys.modules)
raise SystemExit(not result.wasSuccessful())
'''
        outcome = subprocess.run([sys.executable, '-B', '-c', script], env=dict(os.environ),
            text=True, capture_output=True, timeout=30)
        self.assertEqual(outcome.returncode, 0, outcome.stdout + outcome.stderr)


class ServeSessionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.owners, self.reads = [], []

    def config(self, *, limit=4, prefix=0):
        ledger = CellBudgetLedger.create(self.root/'budget.sqlite',
            AttemptBudget('serve-fixture', limit), deadline_utc=time.time()+120)
        if prefix:
            ledger.reserve_unit('prior-unspent', prefix)
        ledger.reserve_unit('session', limit-prefix)
        return dict(schema='semloom.sembench_native_ray_serve_session.v2', model='fixture-model',
            model_source='unused-local-model', model_python=sys.executable,
            startup_timeout_s=10, session_timeout_s=45, cleanup_s=3,
            app_name='owned-fixture', deployment_name='model-fixture', query_id='session',
            engine_kwargs={}, processor_options={}, runtime_options={},
            sampling_params=dict(n=1, max_tokens=32, structured_outputs={'json_object': True}),
            budget=dict(path=str(ledger.path), id='serve-fixture', limit=limit, unit_id='session'))

    def owner(self, **kwargs):
        owner = SoftwareServeSession(**kwargs)
        self.owners.append(owner)
        return owner

    def tasks(self, config):
        test = self
        class Task:
            def prepare_evaluation(self): return None
            def read_source(self):
                test.reads.append(config['query_id'])
                return object()
            def model_request_count(self, source): return 2
            def requests(self, source):
                return [dict(call_id=config['query_id']+'-'+str(index), messages=[dict(
                    role='user', content=json.dumps(dict(text=text)))]) for index, text in enumerate(('good','bad'))]
            def finish(self, source, predictions):
                return pd.DataFrame(dict(count=[sum(json.loads(row['generated_text'])['keep'] for row in predictions)]))
            def verify_source(self, source): pass
            def describe(self, source): return dict(profile='software-neutral-task')
            def evaluate(self, source, result, **kwargs): return dict(score=1)
            def encode_result(self, result): return result.to_dict(orient='split')
        return Task()

    def run_session(self, config, **kwargs):
        from src.experiments.sembench_native_ray import run_serve_session
        return run_serve_session(config, self.root/'session', checkout='unused-fixture',
            session_factory=self.owner, task_factory=self.tasks, **kwargs)

    def test_two_fresh_complete_queries_share_one_owner_and_nonzero_budget_interval(self):
        report = self.run_session(self.config(limit=6, prefix=2))
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(len(self.owners), 1)
        self.assertEqual(self.owners[0].events, ['start','predict','predict','close'])
        self.assertTrue(self.owners[0].closed_unit)
        self.assertEqual(self.reads, ['session-round-1','session-round-2'])
        self.assertEqual([q['native_observation']['attempts'] for q in report['queries']], [[3,4],[5,6]])
        self.assertEqual(report['physical_attempts_reserved'], 4)
        self.assertIsNone(report['warm_qualified'])
        self.assertIsNone(report['target_native_model_qualified'])

    def test_native_startup_work_is_charged_separately_and_never_refunded(self):
        with patch.object(SoftwareServeSession, 'startup_calls', 1):
            report = self.run_session(self.config(limit=5))
        self.assertEqual(report['native_startup_attempts']['attempts'], [1])
        self.assertEqual([q['native_observation']['attempts'] for q in report['queries']], [[2,3],[4,5]])
        self.assertEqual(report['physical_attempts_reserved'], 5)

    def test_startup_can_exhaust_query_allowance_before_any_requests_are_constructed(self):
        config = self.config(limit=2)
        original = self.tasks
        def no_requests(value):
            task = original(value)
            task.requests = lambda source: self.fail('budget failure constructed the query requests')
            return task
        from src.experiments.sembench_native_ray import run_serve_session
        with patch.object(SoftwareServeSession, 'startup_calls', 1), self.assertRaises(BudgetExhausted) as caught:
            run_serve_session(config, self.root/'session', checkout='unused',
                session_factory=self.owner, task_factory=no_requests)
        self.assertEqual(self.owners[0].events, ['start','close'])
        self.assertEqual(caught.exception.query_report['physical_attempts_reserved'], 1)
        self.assertEqual(len(self.reads), 1)

    def test_unconfirmed_or_mismatched_responses_stop_before_the_next_query(self):
        for field in ('unknown', 'mismatch'):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temporary:
                self.root = Path(temporary)
                self.owners, self.reads = [], []
                with patch.object(SoftwareServeSession, field, True), self.assertRaises(RuntimeError) as caught:
                    self.run_session(self.config())
                report = caught.exception.query_report
                self.assertEqual(report['status'], 'failed')
                self.assertEqual(len(self.reads), 1)
                self.assertIsNotNone(report['queries'][0]['result'])
                self.assertNotIn('quality', report['queries'][0])
                self.assertTrue(self.owners[0].closed_unit)
                self.assertIsNone(report['warm_qualified'])

    @staticmethod
    def identity(*, changed=False):
        return dict(observed_monotonic_ns=time.monotonic_ns(), node_id='fixture-node',
            model_actor_id='fixture-actor', model_actor_pid=10, model_actor_create_time=1.0,
            model_processes=[dict(pid=21 if changed else 20, create_time=2.0)])

    def test_changed_model_child_stops_before_the_second_fresh_source(self):
        calls = []
        def snapshot():
            calls.append(None)
            return self.identity(changed=len(calls)>=4)
        with self.assertRaisesRegex(RuntimeError, 'owner changed'):
            self.run_session(self.config(), identity_snapshot=snapshot)
        self.assertEqual(len(self.reads), 1)
        self.assertEqual(self.owners[0].events, ['start','predict','close'])

    def test_a_new_constructor_started_after_sink_stops_before_the_second_source(self):
        from src.experiments.sembench_native_ray import run_serve_session
        def factory(value):
            task = self.tasks(value)
            original = task.evaluate
            def evaluate(*args, **kwargs):
                result = original(*args, **kwargs)
                if value['query_id'].endswith('-1'):
                    self.owners[0].emit('native_constructor_started')
                return result
            task.evaluate = evaluate
            return task
        with self.assertRaisesRegex(RuntimeError, 'constructed a model between'):
            run_serve_session(self.config(), self.root/'session', checkout='unused',
                session_factory=self.owner, task_factory=factory)
        self.assertEqual(len(self.reads), 1)
        self.assertEqual(self.owners[0].events, ['start','predict','close'])

    def test_identity_evidence_requires_model_children_and_fresh_observation(self):
        from src.experiments.sembench_native_ray import _owner_snapshot
        for value in (dict(model_actor_pid=10), self.identity()):
            with self.subTest(value=value):
                _, proof = _owner_snapshot(lambda: value)
                self.assertIsNone(proof)
        report = self.run_session(self.config(), identity_snapshot=self.identity)
        # Conditional snapshot comparison only; these are fixture identities,
        # and target native/model qualification remains explicitly unknown.
        self.assertTrue(report['model_owner_reused'])
        self.assertIsNone(report['target_native_model_qualified'])

    def test_two_empty_queries_do_not_qualify_warm_model_work(self):
        from src.experiments.sembench_native_ray import run_serve_session

        def factory(value):
            task = self.tasks(value)
            task.model_request_count = lambda source: 0
            task.requests = lambda source: []
            task.finish = lambda source, predictions: pd.DataFrame(dict(count=[0]))
            return task
        report = run_serve_session(self.config(), self.root/'empty-session', checkout='software',
            session_factory=self.owner, task_factory=factory, identity_snapshot=self.identity)
        self.assertEqual(report['physical_attempts_reserved'], 0)
        self.assertIsNone(report['model_owner_reused'])
        self.assertIsNone(report['warm_qualified'])

    def test_primary_failure_survives_owned_close_failure_and_completed_sink_is_kept(self):
        with patch.object(SoftwareServeSession, 'mismatch', True), \
                patch.object(SoftwareServeSession, 'close_failure', True), self.assertRaises(RuntimeError) as caught:
            self.run_session(self.config())
        self.assertIn('response IDs', str(caught.exception))
        self.assertTrue(any('close' in note for note in caught.exception.__notes__))
        self.assertIsNotNone(caught.exception.query_report['queries'][0]['result'])
        saved = json.loads((self.root/'session/round-1/query.json').read_text())
        self.assertEqual(saved['status'], 'failed')
        self.assertNotIn('quality', saved)

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind fixed official SemBench')
    def test_fresh_movie_queries_use_one_owner_and_original_scoring(self):
        from src.baselines.text.sembench_native_text import task_from_config
        from src.experiments.sembench_native_ray import run_serve_session
        source = self.root/'Reviews.csv'
        rows = pd.DataFrame(dict(id=['taken_3','taken_3'], reviewId=[1,2], reviewText=['good first','bad second'],
                                 scoreSentiment=['POSITIVE','NEGATIVE']))
        rows.to_csv(source, index=False)
        config = dict(self.config(), reviews_csv=str(source), query='Q3')
        reads = []
        def factory(value):
            task = task_from_config(value)
            class Task:
                def __getattr__(self, name): return getattr(task, name)
                def read_source(self):
                    reads.append(value['query_id'])
                    return task.read_source()
                def evaluate(self, source_value, result, **kwargs):
                    quality = task.evaluate(source_value, result, **kwargs)
                    if value['query_id'].endswith('-1'):
                        rows.loc[1, 'reviewText'] = 'good second'
                        rows.to_csv(source, index=False)
                    return quality
            return Task()
        report = run_serve_session(config, self.root/'session', checkout=os.environ['SEMBENCH_CHECKOUT'],
            session_factory=self.owner, task_factory=factory)
        self.assertEqual(reads, ['session-round-1','session-round-2'])
        self.assertEqual([q['result']['data'] for q in report['queries']], [[[1]],[[2]]])
        self.assertEqual([q['quality']['original_metric']['absolute_error'] for q in report['queries']], [0,1])
        self.assertEqual(len(self.owners), 1)
        self.assertTrue(self.owners[0].closed_unit)

    @unittest.skipUnless(os.environ.get('SEMBENCH_CHECKOUT'), 'bind fixed official SemBench')
    def test_CLI_profile_is_explicit_and_check_allocates_no_owner_or_budget(self):
        from src.experiments import sembench_native_ray as entry
        source = self.root/'Reviews.csv'
        pd.DataFrame([dict(id='taken_3', reviewId=1, reviewText='good', scoreSentiment='POSITIVE')]).to_csv(source,index=False)
        config = dict(self.config(), reviews_csv=str(source), query='Q3',
                      sembench_checkout=os.environ['SEMBENCH_CHECKOUT'])
        path = self.root/'config.json'
        path.write_text(json.dumps(config))
        before = Path(config['budget']['path']).read_bytes()
        output = self.root/'not-created'
        with patch.object(entry, 'run_serve_session', side_effect=AssertionError('check executed Serve')):
            with redirect_stdout(StringIO()) as stream:
                self.assertEqual(entry.main(['serve-session','--config',str(path),'--output-dir',str(output)]),0)
            self.assertFalse(json.loads(stream.getvalue())['execution_enabled'])
            with self.assertRaisesRegex(ValueError, 'explicit serve-session'):
                entry.main(['run','--config',str(path)])
        self.assertFalse(output.exists())
        self.assertEqual(Path(config['budget']['path']).read_bytes(), before)


class DependentTableTask:
    """Two real source-dependent batches, with no service or scheduling policy."""
    def __init__(self, path, execution_id, events):
        self.path, self.execution_id, self.events = Path(path), execution_id, events

    def prepare_evaluation(self):
        return None

    def read_source(self):
        from io import BytesIO
        self.events.append('source')
        raw = self.path.read_bytes()
        return dict(raw=raw, frame=pd.read_csv(BytesIO(raw)))

    def model_request_count(self, source):
        return len(source['frame'])  # Initial lower bound; second batch depends on predictions.

    def requests(self, source):
        raise AssertionError('dependent query must use its query generator')

    def finish(self, source, predictions):
        raise AssertionError('dependent query owns its complete relation in query_batches')

    def query_batches(self, source):
        from src.baselines.text.sembench_native_text import TextFilterTask
        predicate = TextFilterTask('Keep the supplied good text.')
        frame = source['frame']
        first = [dict(call_id=self.execution_id+'-first-'+str(index),
            messages=predicate.messages(text)) for index, text in enumerate(frame['text'])]
        try:
            decisions = predicate.decisions([row['call_id'] for row in first], (yield first))
            positions = [index for index, keep in enumerate(decisions) if keep]
            self.events.append('first-finished')
            second = [dict(call_id=self.execution_id+'-second-'+str(index),
                messages=predicate.messages(frame.iloc[index]['text']+' dependent')) for index in positions]
            self.events.append('second-built')
            following = predicate.decisions([row['call_id'] for row in second], (yield second))
            self.events.append('sink')
            return pd.DataFrame(dict(id=[frame.iloc[index]['id'] for index in positions], keep=following))
        finally:
            self.events.append('generator-close')

    def verify_source(self, source):
        assert self.path.read_bytes() == source['raw']
        self.events.append('verify')

    def describe(self, source):
        return dict(role='controlled dependent-table software fixture', source_rows=len(source['frame']))

    def evaluate(self, source, result, **kwargs):
        self.events.append('evaluate')
        return dict(role='fixture relation check; not a benchmark/model score', rows=len(result))

    def encode_result(self, result):
        return result.to_dict(orient='split')


class NativeMultistageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root/'source.csv'
        pd.DataFrame(dict(id=['duplicate','other','duplicate'], text=['good first','bad','good second'])).to_csv(
            self.source, index=False)
        self.owners = []

    def config(self, limit=5, *, serve=False):
        ledger = CellBudgetLedger.create(self.root/'budget.sqlite', AttemptBudget('multi-fixture',limit),
                                        deadline_utc=time.time()+120)
        ledger.reserve_unit('query', limit)
        config = dict(model='fixture-model', model_python=sys.executable, engine_kwargs={},
            processor_options={}, runtime_options={}, sampling_params=dict(n=1,structured_outputs={'json_object':True}),
            budget=dict(path=str(ledger.path),id='multi-fixture',limit=limit,unit_id='query'))
        if serve:
            config.update(schema='semloom.sembench_native_ray_serve_session.v2', model_source='unused',
                          app_name='owned-fixture',deployment_name='model-fixture',query_id='session')
        return config

    @staticmethod
    def reply(row, request):
        payload = json.loads(request['messages'][-1]['content'])
        return json.dumps(dict(keep='good' in payload['text']))

    def serve_owner(self, **options):
        owner = SoftwareServeSession(**options)
        self.owners.append(owner)
        return owner

    def test_offline_dependent_second_batch_preserves_occurrences_and_final_sink_before_close(self):
        config = self.config()
        with recorded_processor(self.reply) as state:
            task = DependentTableTask(self.source, 'multi', state.events)
            report = run_query(task, config, self.root/'query', checkout='unused-fixture')
        self.assertEqual(report['request_batch_counts'], [3,2])
        self.assertEqual(report['logical_requests'], 5)
        self.assertEqual(report['physical_attempts_reserved'], 5)
        self.assertEqual(report['result']['data'], [['duplicate',True],['duplicate',True]])
        self.assertEqual(len(state.configs), 2)
        self.assertEqual(state.events.count('init'), 1)
        self.assertLess(state.events.index('first-finished'), state.events.index('second-built'))
        self.assertLess(state.events.index('sink'), state.events.index('close'))
        self.assertLess(state.events.index('close'), state.events.index('verify'))
        self.assertIs(report['model_ready_before_source'], False)
        requests = json.loads((self.root/'query/requests.json').read_text())
        self.assertEqual([row['call_id'] for row in requests], ['multi-first-0','multi-first-1','multi-first-2',
                                                              'multi-second-0','multi-second-2'])
        self.assertIn('good second dependent', state.requests[-1]['messages'][-1]['content'])

    def test_offline_empty_dependent_batch_does_not_start_another_native_graph(self):
        pd.DataFrame(dict(id=['a','b','c'],text=['bad first','bad second','bad third'])).to_csv(self.source,index=False)
        config = self.config(limit=3)
        with recorded_processor(self.reply) as state:
            report = run_query(DependentTableTask(self.source,'empty',state.events),config,self.root/'query',checkout='fixture')
        self.assertEqual(report['request_batch_counts'], [3,0])
        self.assertEqual(report['physical_attempts_reserved'], 3)
        self.assertEqual(report['result']['data'], [])
        self.assertEqual(report['result']['columns'], ['id','keep'])
        self.assertEqual(len(state.configs), 1)
        native = json.loads((self.root/'query/native-predictions.json').read_text())
        self.assertEqual([len(batch['predictions']) for batch in native['batches']], [3,0])
        self.assertLess(state.events.index('sink'), state.events.index('close'))

    def test_offline_second_stage_budget_rejection_keeps_constructed_requests_without_submission(self):
        config = self.config(limit=4)
        with recorded_processor(self.reply) as state:
            with self.assertRaisesRegex(BudgetExhausted, 'next complete native batch') as caught:
                run_query(DependentTableTask(self.source,'limited',state.events),config,self.root/'query',checkout='fixture')
        report = caught.exception.query_report
        self.assertEqual(report['request_batch_counts'], [3,2])
        self.assertEqual(report['physical_attempts_reserved'], 3)
        self.assertEqual(len(state.requests), 3)
        self.assertEqual(len(json.loads((self.root/'query/requests.json').read_text())), 5)
        self.assertIsNone(report['sink_ready_ns'])
        self.assertNotIn('quality', report)
        self.assertIn('generator-close', state.events)
        self.assertIn('close', state.events)

    def test_dependent_pair_budget_precedes_message_materialization(self):
        config = self.config(limit=4)
        class Pairs:
            def __len__(self):
                return 1000000
            def __iter__(self):
                raise AssertionError('budget rejection expanded dependent pairs')
        with recorded_processor(self.reply) as state:
            task = DependentTableTask(self.source, 'deferred', state.events)
            original = task.query_batches
            def deferred(source):
                program = original(source)
                try:
                    predictions = yield next(program)
                    program.send(predictions)
                    yield Pairs()
                finally:
                    program.close()
            task.query_batches = deferred
            with self.assertRaisesRegex(BudgetExhausted, 'next complete native batch') as caught:
                run_query(task, config, self.root/'query', checkout='fixture')
        self.assertEqual(caught.exception.query_report['request_batch_counts'], [3,1000000])
        self.assertEqual(caught.exception.query_report['logical_requests'], 3)
        self.assertEqual(caught.exception.query_report['physical_attempts_reserved'], 3)
        self.assertEqual(len(state.requests), 3)
        self.assertIn('generator-close', state.events)

    def test_offline_unknown_batch_blocks_next_submission_and_final_unknown_preserves_sink(self):
        for unknown_batch in (1,2):
            with self.subTest(unknown_batch=unknown_batch), tempfile.TemporaryDirectory() as temporary:
                self.root = Path(temporary)
                config = self.config()
                materialized = []
                def corrupt_terminal(phase):
                    if phase != 'materialize': return
                    materialized.append(phase)
                    if len(materialized) != unknown_batch: return
                    path = self.root/'query/native-events-software.jsonl'
                    rows = [json.loads(line) for line in path.read_text().splitlines()]
                    target = 1 if unknown_batch == 1 else 4
                    for row in rows:
                        if row['event']=='native_generate_terminal' and row['attempt']==target:
                            row['event']='native_generate_unconfirmed'
                    path.write_text(''.join(json.dumps(row)+'\n' for row in rows))
                with recorded_processor(self.reply, advance=corrupt_terminal) as state:
                    with self.assertRaisesRegex(RuntimeError, 'unconfirmed|confirmed terminals') as caught:
                        run_query(DependentTableTask(self.source,'unknown',state.events),config,self.root/'query',checkout='fixture')
                report = caught.exception.query_report
                self.assertEqual(len(state.requests), 3 if unknown_batch==1 else 5)
                self.assertEqual(report['physical_attempts_reserved'], 3 if unknown_batch==1 else 5)
                self.assertEqual(report['request_batch_counts'], [3,2])
                self.assertNotIn('quality', report)
                if unknown_batch==1:
                    self.assertIsNone(report['sink_ready_ns'])
                else:
                    self.assertIsNotNone(report['sink_ready_ns'])
                    self.assertEqual(report['result']['data'], [['duplicate',True],['duplicate',True]])

    def test_offline_second_native_exception_closes_owner_and_keeps_primary(self):
        calls = []
        failure = RuntimeError('second native graph failed')
        def transform(rows):
            calls.append(1)
            if len(calls)==2: raise failure
            return rows
        config = self.config()
        with recorded_processor(self.reply, transform=transform, close_error=ValueError('close also failed')) as state:
            with self.assertRaises(RuntimeError) as caught:
                run_query(DependentTableTask(self.source,'error',state.events),config,self.root/'query',checkout='fixture')
        self.assertIs(caught.exception,failure)
        self.assertTrue(any('close failed' in note for note in failure.__notes__))
        self.assertEqual(failure.query_report['physical_attempts_reserved'],5)
        self.assertIsNone(failure.query_report['sink_ready_ns'])
        self.assertEqual(len(json.loads((self.root/'query/requests.json').read_text())),5)

    def test_serve_two_dependent_queries_use_one_model_owner_and_audit_every_batch(self):
        from src.experiments import sembench_native_ray as entry
        events=[]
        original_read = entry.read_json
        def post_sink_read(path):
            if Path(path).name.startswith('native-output-'):
                attempt = int(Path(path).stem.removeprefix('native-output-'))
                self.assertGreaterEqual(events.count('sink'), 1 if attempt <= 5 else 2)
            return original_read(path)
        with patch.object(entry, 'read_json', side_effect=post_sink_read):
            report = entry.run_serve_session(self.config(limit=10,serve=True),self.root/'session',checkout='fixture',
                session_factory=self.serve_owner,
                task_factory=lambda config:DependentTableTask(self.source,config['query_id'],events))
        self.assertEqual(report['status'],'completed')
        self.assertEqual(len(self.owners),1)
        self.assertEqual(self.owners[0].events,['start','predict','predict','predict','predict','close'])
        self.assertTrue(self.owners[0].closed_unit)
        self.assertEqual([query['request_batch_counts'] for query in report['queries']],[[3,2],[3,2]])
        self.assertEqual([query['native_observation']['attempts'] for query in report['queries']],
                         [list(range(1,6)),list(range(6,11))])
        self.assertEqual([len(query['native_observation']['response_associations']) for query in report['queries']],[5,5])
        self.assertEqual(report['physical_attempts_reserved'],10)
        self.assertIsNone(report['warm_qualified'])
        for index,query in enumerate(report['queries'],1):
            self.assertEqual(query['result']['data'],[['duplicate',True],['duplicate',True]])
            self.assertLessEqual(query['sink_ready_ns'],query['post_sink_audit_started_ns'])
            requests=json.loads((self.root/'session'/('round-'+str(index))/'requests.json').read_text())
            self.assertEqual(len(requests),5)

    def test_serve_second_stage_unknown_keeps_final_sink_and_native_exception_keeps_primary(self):
        from src.experiments.sembench_native_ray import run_serve_session
        for mode in ('unknown', 'exception'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                self.root = Path(temporary)
                self.owners = []
                failure = RuntimeError('second Serve batch failed')
                def owner_factory(**options):
                    owner = self.serve_owner(**options)
                    original = owner.predict
                    def predict(rows, **arguments):
                        if len(owner.inputs) == 1:
                            if mode == 'exception':
                                raise failure
                            owner.unknown = True
                        return original(rows, **arguments)
                    owner.predict = predict
                    return owner
                with self.assertRaises(RuntimeError) as caught:
                    run_serve_session(self.config(limit=10, serve=True), self.root/'session', checkout='fixture',
                        session_factory=owner_factory,
                        task_factory=lambda config: DependentTableTask(self.source, config['query_id'], []))
                report = caught.exception.query_report
                self.assertEqual(len(report['queries']), 1)
                self.assertTrue(self.owners[0].closed_unit)
                self.assertEqual(report['queries'][0]['request_batch_counts'], [3,2])
                self.assertNotIn('quality', report['queries'][0])
                self.assertIsNone(report['warm_qualified'])
                if mode == 'unknown':
                    self.assertEqual(report['physical_attempts_reserved'], 5)
                    self.assertEqual(report['queries'][0]['result']['data'], [['duplicate',True],['duplicate',True]])
                else:
                    self.assertIs(caught.exception, failure)
                    self.assertEqual(report['physical_attempts_reserved'], 3)
                    self.assertIsNone(report['queries'][0]['result'])

    def test_serve_unknown_or_insufficient_second_stage_never_submits_dependent_batch_or_next_query(self):
        from src.experiments.sembench_native_ray import run_serve_session
        for mode in ('unknown','budget'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                self.root=Path(temporary)
                self.owners=[]
                config=self.config(limit=10 if mode=='unknown' else 4,serve=True)
                with patch.object(SoftwareServeSession,'unknown',mode=='unknown'):
                    with self.assertRaises((RuntimeError,BudgetExhausted)) as caught:
                        run_serve_session(config,self.root/'session',checkout='fixture',session_factory=self.serve_owner,
                            task_factory=lambda config:DependentTableTask(self.source,config['query_id'],[]))
                report=caught.exception.query_report
                self.assertEqual(self.owners[0].events,['start','predict','close'])
                self.assertEqual(len(report['queries']),1)
                self.assertEqual(report['queries'][0]['request_batch_counts'],[3,2])
                self.assertEqual(report['physical_attempts_reserved'],3)
                self.assertIsNone(report['queries'][0]['result'])
                self.assertTrue(self.owners[0].closed_unit)
                self.assertIsNone(report['warm_qualified'])


if __name__ == '__main__':
    unittest.main()
