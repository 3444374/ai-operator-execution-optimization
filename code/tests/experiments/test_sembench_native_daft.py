"""Independent task/sink with a real supervised software child, no native SDK/model."""

from contextlib import redirect_stdout
from copy import deepcopy
import csv
import importlib.util
from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from src.experiments import native_daft_query, sembench_native_daft as driver
from src.infrastructure.private_artifacts import new_private_directory, read_json, write_private_json


CHECKOUT = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')
CODE = str(Path(__file__).resolve().parents[2])


IMPORT_GUARD = r'''
import importlib.abc,socket,sys
forbidden=[]
class NoSDK(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,*args):
  if (fullname.split('.')[0] in {'lotus','daft','vllm','transformers'}
      or fullname=='src.baselines.text.sembench_text'
      or fullname=='src.semantic_methods.lotus' or fullname.startswith('src.semantic_methods.lotus.')):
   forbidden.append(fullname)
   raise AssertionError('independent software fixture imported forbidden SDK/semantics: '+fullname)
sys.meta_path.insert(0,NoSDK())
def offline(event,args):
 if event in ('socket.connect','socket.bind') and args[0].family in (socket.AF_INET,socket.AF_INET6):
  raise RuntimeError('software fixture forbids INET connect/bind')
sys.addaudithook(offline)
'''


CPU_WORKER = IMPORT_GUARD + r'''
import json,time
from pathlib import Path
from src.experiments.native_daft_query import run_worker
from src.infrastructure.private_artifacts import read_json,write_private_json
config,directory,mode=read_json(sys.argv[1]),Path(sys.argv[2]),sys.argv[3]
def execute(rows,**options):
 write_private_json(directory/'fixture-start.json',dict(role='software predictions; no native SDK/model',started_ns=time.monotonic_ns()))
 predictions=[]
 for row in rows:
  options['before_generate'](row['call_id'],'software-'+row['call_id'],row['prompt'])
  messages=json.loads(row['prompt'])
  payload=json.loads(messages[1]['content'])
  if mode in ('pair-same','pair-opposite'):
   same=payload['left_text'].startswith('good')==payload['right_text'].startswith('good')
   generated=json.dumps({'keep':same if mode=='pair-same' else not same})
  else:
   text=payload['text']
   if mode=='sentiment':generated=json.dumps({'sentiment':'POSITIVE' if text.startswith('good') else 'NEGATIVE'})
   elif mode=='score':generated=json.dumps({'score':5 if text.startswith('good') else 1})
   elif mode in ('director','director-close-error'):generated=json.dumps({'director':'Michael Ritchie' if text.startswith('first') else 'Other'})
   elif mode=='genres':generated=json.dumps({'genres':'comedy' if text.startswith('first') else 'drama, romance'})
   else:generated=json.dumps({'keep':text.startswith('good')}) if mode!='invalid' else '{"keep":"true"}'
  predictions.append(dict(call_id=row['call_id'],generated_text=generated))
 if mode=='missing':predictions.pop()
 result=dict(predictions=predictions,metadata=dict(result_ready_monotonic_ns=time.monotonic_ns(),
  native_api=dict(role='software fixture',generate_calls=len(rows),accepted=len(rows),final_events=len(rows),unresolved=0,errors=[]),model_close_receipt=None))
 options['on_result_ready'](result)
 close=dict(status='closed',role='software fixture, not a model shutdown',close_started_monotonic_ns=time.monotonic_ns())
 write_private_json(directory/'fixture-close.json',close)
 result['metadata']['model_close_receipt']=close
 if mode in ('close-error','director-close-error'):
  error=RuntimeError('software worker close failure')
  error.model_close_report=dict(close,status='unknown')
  raise error
 return result
try:run_worker(config,directory,execute_query=execute)
finally:write_private_json(directory/'fixture-imports.json',dict(forbidden_import_attempts=forbidden,real_native_SDK=False,actual_models=0))
'''


class SoftwareSupervisor:
    """Real prepared-worker ACK owner; observe one known software child only."""
    def __init__(self, mode='success'):
        self.mode, self.calls = mode, []

    def __call__(self, config, directory, **options):
        from src.experiments import lotus_records_supervisor as processes
        self.calls.append((Path(config), Path(directory)))
        def launch(command, output, **kwargs):
            # The host sandbox denies global psutil/sysctl process enumeration.
            # This fixture observes its Popen child, not host/GPU/tree cleanup.
            new_private_directory(output)
            fixture_command = [sys.executable, '-B', '-c', CPU_WORKER, str(config), str(directory), self.mode]
            process = subprocess.Popen(fixture_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            deadline = time.monotonic() + kwargs['timeout_s']
            try:
                while True:
                    kwargs['monitor'](())
                    if process.poll() is not None:
                        if process.returncode:
                            raise RuntimeError('CPU software worker failed; see fixture-worker.log')
                        return dict(status='CPU software child exited', scope='one known Popen child; tree/GPU unverified',
                                    known_child_exit_confirmed=True, worker_exit=process.returncode)
                    if time.monotonic() >= deadline:
                        raise TimeoutError('CPU software child exceeded its finite allowance')
                    time.sleep(.005)
            finally:
                try:
                    process.wait(timeout=kwargs['grace_s'])
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=kwargs['grace_s'])
                raw, _ = process.communicate(timeout=kwargs['grace_s'])
                from src.infrastructure.redact import redact_text
                (Path(directory) / 'fixture-worker.log').write_text(redact_text(raw))
        with patch.object(processes, 'supervise', side_effect=launch):
            return native_daft_query.supervised_run(config, directory, **options)


class SoftwareTokenizer:
    def __init__(self):
        self.messages = []

    def apply_chat_template(self, messages, *, tokenize, add_generation_prompt):
        assert tokenize is False and add_generation_prompt is True
        self.messages.append(deepcopy(messages))
        return json.dumps(messages, ensure_ascii=False)


@unittest.skipUnless(CHECKOUT and all(importlib.util.find_spec(name) for name in ('pandas','duckdb','cdlib')),
                     'requires the pinned SemBench checkout and existing CPU evaluator dependencies')
class IndependentDaftQueryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.csv = self.root / 'Reviews.csv'
        rows = [('taken_3','r0','good duplicate','POSITIVE'), ('taken_3','r1','bad','NEGATIVE'),
                ('other','r2','good other','POSITIVE'), ('taken_3','r3','good duplicate','POSITIVE'),
                ('other','r4','good four','POSITIVE'), ('other','r5','good five','POSITIVE'),
                ('other','r6','good six','POSITIVE'), ('taken_3','r7','','NEGATIVE')]
        self.write_rows(rows)

    def write_rows(self, rows):
        with self.csv.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(('id','reviewId','reviewText','scoreSentiment'))
            writer.writerows(rows)

    def write_join_rows(self):
        self.write_rows([
            ('ant_man_and_the_wasp_quantumania', 'duplicate', 'good first', 'POSITIVE'),
            ('ant_man_and_the_wasp_quantumania', 'duplicate', 'bad', 'NEGATIVE'),
            ('ant_man_and_the_wasp_quantumania', 'other', 'good second', 'POSITIVE')])

    def mmqa_config(self, query='Q1'):
        import pandas as pd

        pd.DataFrame(dict(Title=['Film', 'Film', 'Other'], Role=['Other', 'Bob Whitewood', 'Other'])).to_csv(
            self.root / 'ben_piazza.csv', index=False)
        pd.DataFrame(dict(title=['Film', 'Other'], text=['first director', 'second director'])).to_csv(
            self.root / 'ben_piazza_text_data.csv', index=False)
        pd.DataFrame(dict(title=['Mean Girls', 'Ignored', 'Allied'], text=['first genres', 'unused', 'second genres'])).to_csv(
            self.root / 'lizzy_caplan_text_data.csv', index=False)
        config = self.config(query)
        del config['reviews_csv']
        config.update(scenario='mmqa', data_dir=str(self.root))
        return config

    def config(self, query='Q3', *, ranking=None, movies_csv=None):
        config = dict(schema=driver.SCHEMA, sembench_checkout=CHECKOUT, reviews_csv=str(self.csv),
            query=query, query_id='native-daft-' + query, model='software-no-model',
            sampling_params=dict(n=1, response_format=dict(type='json_object'), max_tokens=32, temperature=0),
            engine_kwargs={}, provider_options={}, query_timeout_s=15, cleanup_s=4,
            stop_grace_s=.5, max_model_calls=8)
        if ranking is not None:
            config['ranking'] = ranking
        if movies_csv is not None:
            config['movies_csv'] = str(movies_csv)
        return config

    def invoke(self, query='Q3', mode='success', *, ranking=None, movies_csv=None):
        self.tokenizer, self.supervisor = SoftwareTokenizer(), SoftwareSupervisor(mode)
        self.output = self.root / ('run-' + query + '-' + mode)
        return driver.run_query(self.config(query, ranking=ranking, movies_csv=movies_csv), self.output, model_python=sys.executable,
                                tokenizer=self.tokenizer, supervise=self.supervisor)

    def test_one_filter_family_reaches_complete_officially_scored_sinks_before_ACK_close(self):
        expected = {'Q1':[['r0'],['r2'],['r3'],['r4'],['r5']], 'Q2':[['r0'],['r3']],
                    'Q3':[[2]], 'Q4':[[.5]]}
        for query in expected:
            with self.subTest(query=query):
                report = self.invoke(query)
                self.assertEqual(report['status'], 'completed')
                self.assertEqual(report['result']['data'], expected[query])
                requests = 8 if query == 'Q1' else 4
                self.assertEqual(report['physical_attempts_reserved'], requests)
                self.assertEqual(report['logical_model_requests'], requests)
                self.assertEqual(report['task']['comparison_identity'], '统一语义工程对照')
                self.assertFalse(report['author_original_semantic_method'])
                self.assertEqual(report['query_clock_version'], 'native_daft.source_to_sink.v1')
                self.assertEqual(report['source_to_sink_seconds'],
                    (report['sink_ready_ns'] - report['source_start_ns']) / 1e9)
                worker = self.output / 'native-worker'
                close, terminal = read_json(worker / 'fixture-close.json'), read_json(worker / 'worker.json')
                self.assertLessEqual(report['preflight_finished_ns'], report['source_start_ns'])
                self.assertLessEqual(report['sink_ready_ns'], terminal['result_release']['released_monotonic_ns'])
                self.assertLessEqual(terminal['result_release']['released_monotonic_ns'], close['close_started_monotonic_ns'])
                self.assertLessEqual(close['close_started_monotonic_ns'], report['source_audit_started_ns'])
                self.assertLessEqual(report['sink_ready_ns'], report['evaluation_started_ns'])
                self.assertTrue(terminal['result_release']['confirmed'])
                self.assertEqual(read_json(worker / 'fixture-imports.json')['forbidden_import_attempts'], [])
                self.assertTrue(report['supervision']['known_child_exit_confirmed'])
                self.assertEqual(report['supervision']['worker_exit'], 0)
                self.assertIsNone(report['physical_gpu_exit_confirmed'])
                metric = report['quality']['original_metric']
                self.assertEqual(metric['f1_score'] if query in ('Q1','Q2') else metric['absolute_error'],
                                 1 if query in ('Q1','Q2') else 0)
                self.assertNotIn('scoreSentiment', json.dumps(self.tokenizer.messages))
                self.assertNotIn('POSITIVE', json.dumps(self.tokenizer.messages))
                prompts = read_json(self.output / 'prepared-prompts.json')['requests']
                self.assertEqual(len(prompts), requests)
                self.assertEqual(len({row['call_id'] for row in prompts}), requests)

    def test_map_tasks_reuse_the_same_worker_and_original_scored_sinks(self):
        report = self.invoke('Q8', 'sentiment')
        self.assertEqual(report['result']['data'], [['NEGATIVE', 2], ['POSITIVE', 2]])
        self.assertEqual(report['quality']['original_metric']['absolute_error'], 0)
        self.assertEqual(report['baseline_identity'], 'daft-native-project-json')
        self.assertEqual(report['physical_attempts_reserved'], 4)
        self.assertTrue(report['result_release']['confirmed'])
        with self.csv.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(('id', 'reviewId', 'reviewText', 'scoreSentiment', 'originalScore'))
            writer.writerows([
                ('other', 'outside', 'good outside', 'POSITIVE', '4/5'),
                ('ant_man_and_the_wasp_quantumania', 'good', 'good score', 'POSITIVE', '5/5'),
                ('ant_man_and_the_wasp_quantumania', 'bad', 'bad score', 'NEGATIVE', '1/5'),
            ])
        report = self.invoke('Q9', 'score', ranking='map')
        self.assertEqual(report['result']['data'], [['good', 5.0], ['bad', 1.0]])
        self.assertAlmostEqual(report['quality']['original_metric']['spearman_correlation'], 1)
        self.assertEqual(report['physical_attempts_reserved'], 2)
        terminal = read_json(self.output / 'native-worker/worker.json')
        self.assertLessEqual(report['sink_ready_ns'], terminal['result_release']['released_monotonic_ns'])
        self.assertNotIn('originalScore', json.dumps(self.tokenizer.messages))

    def test_Q10_scores_all_reviews_then_groups_movies_before_official_evaluation(self):
        with self.csv.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(('id', 'reviewText'))
            writer.writerows([('B', 'bad B'), ('A', 'good A'), ('A', 'bad A')])
        movies = self.root / 'Movies.csv'
        with movies.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(('id', 'audienceScore'))
            writer.writerows([('A', 100), ('B', 20)])
        report = self.invoke('Q10', 'score', movies_csv=movies)
        self.assertEqual(report['result']['data'], [['A', 3.0], ['B', 1.0]])
        self.assertEqual(report['physical_attempts_reserved'], 3)
        self.assertAlmostEqual(report['quality']['original_metric']['spearman_correlation'], 1)
        self.assertNotIn('audienceScore', json.dumps(self.tokenizer.messages))
        terminal = read_json(self.output / 'native-worker/worker.json')
        self.assertLessEqual(report['sink_ready_ns'], terminal['result_release']['released_monotonic_ns'])
        self.assertLessEqual(report['sink_ready_ns'], report['evaluation_started_ns'])

    def test_join_tasks_predict_all_pairs_before_original_ID_filter_and_sink(self):
        self.write_join_rows()
        movie = 'ant_man_and_the_wasp_quantumania'
        for query in ('Q5', 'Q6', 'Q7'):
            with self.subTest(query=query):
                config = dict(self.config(query), max_model_calls=9)
                output = self.root / query
                supervise = SoftwareSupervisor('pair-same' if query == 'Q5' else 'pair-opposite')
                tokenizer = SoftwareTokenizer()
                report = driver.run_query(config, output, model_python=sys.executable,
                                          tokenizer=tokenizer, supervise=supervise)
                self.assertEqual(report['result']['data'], [[movie, 'duplicate', 'other'], [movie, 'other', 'duplicate']])
                self.assertEqual(report['physical_attempts_reserved'], 9)
                self.assertEqual(len(tokenizer.messages), 9)
                prepared = read_json(output / 'prepared-prompts.json')['requests']
                self.assertIn('native-daft-' + query + '-left-0-right-0', {row['call_id'] for row in prepared})
                self.assertIn('native-daft-' + query + '-left-0-right-1', {row['call_id'] for row in prepared})
                self.assertEqual(report['quality']['original_metric']['f1_score'], 1)
                terminal = read_json(output / 'native-worker/worker.json')
                self.assertLessEqual(report['sink_ready_ns'], terminal['result_release']['released_monotonic_ns'])

    def test_join_budget_rejection_precedes_pair_construction_and_native_start(self):
        self.write_join_rows()
        task = driver.task_from_config(self.config('Q5'))
        supervise = SoftwareSupervisor()
        with patch.object(driver, 'task_from_config', return_value=task), \
                patch.object(type(task), 'requests', side_effect=AssertionError('pairs must not be constructed')) as requests:
            with self.assertRaisesRegex(ValueError, 'finite native budget') as raised:
                driver.run_query(self.config('Q5'), self.root / 'query', model_python=sys.executable,
                                 tokenizer=SoftwareTokenizer(), supervise=supervise)
        requests.assert_not_called()
        self.assertEqual(supervise.calls, [])
        self.assertEqual(raised.exception.query_report['physical_attempts_reserved'], 0)

    def test_CLI_counts_join_pairs_for_all_three_original_tasks(self):
        self.write_join_rows()
        path = self.root / 'join-config.json'
        with patch.object(driver, 'run_query', side_effect=AssertionError('native execution forbidden')):
            for query in ('Q5', 'Q6', 'Q7'):
                path.write_text(json.dumps(dict(self.config(query), max_model_calls=9)))
                with self.subTest(query=query), redirect_stdout(StringIO()) as stream:
                    self.assertEqual(driver.main(['check', '--config', str(path)]), 0)
                self.assertEqual(json.loads(stream.getvalue())['task']['expected_model_calls'], 9)
            path.write_text(json.dumps(self.config('Q5')))
            with self.assertRaisesRegex(ValueError, 'finite native budget'):
                driver.main(['check', '--config', str(path)])

    def test_MMQA_extract_preserves_Series_and_table_sinks_including_close_failure(self):
        for query, mode in (('Q1', 'director'), ('Q4', 'genres'), ('Q1', 'director-close-error')):
            with self.subTest(query=query, mode=mode):
                config = self.mmqa_config(query)
                output = self.root / mode
                options = dict(model_python=sys.executable, tokenizer=SoftwareTokenizer(), supervise=SoftwareSupervisor(mode))
                if mode == 'director-close-error':
                    with self.assertRaises(RuntimeError) as raised:
                        driver.run_query(config, output, **options)
                    report = raised.exception.query_report
                    self.assertEqual(report['status'], 'failed')
                    self.assertNotIn('quality', report)
                else:
                    report = driver.run_query(config, output, **options)
                    self.assertEqual(report['quality']['original_metric']['precision'], 1)
                if query == 'Q1':
                    self.assertEqual(report['result'], dict(kind='series', name='director', index=[1],
                        data=['Michael Ritchie'], dtype='object'))
                else:
                    self.assertEqual(report['result']['data'], [['comedy', 'Mean Girls'], ['drama', 'Allied'], ['romance', 'Allied']])
                self.assertEqual(report['physical_attempts_reserved'], 2)
                terminal = read_json(output / 'native-worker/worker.json')
                self.assertLessEqual(report['sink_ready_ns'], terminal['result_release']['released_monotonic_ns'])

    def test_encoder_failure_keeps_primary_error_raw_Series_and_report(self):
        for primary_failure in (None, RuntimeError('official score failed')):
            with self.subTest(primary_failure=primary_failure):
                config = self.mmqa_config()
                task = driver.task_from_config(config)
                output = self.root / ('encoder-only' if primary_failure is None else 'encoder-and-score')
                encoding_failure = TypeError('Series encoding failed')
                def evaluate(*args, **kwargs):
                    if primary_failure is not None:
                        raise primary_failure
                    return dict(original_metric=dict(precision=1))
                with patch.object(driver, 'task_from_config', return_value=task), \
                        patch.object(type(task), 'evaluate', side_effect=evaluate), \
                        patch.object(type(task), 'encode_result', side_effect=encoding_failure):
                    with self.assertRaises(type(primary_failure or encoding_failure)) as raised:
                        driver.run_query(config, output, model_python=sys.executable,
                            tokenizer=SoftwareTokenizer(), supervise=SoftwareSupervisor('director'))
                self.assertIs(raised.exception, primary_failure or encoding_failure)
                report = raised.exception.query_report
                self.assertEqual(report['status'], 'failed')
                self.assertNotIn('quality', report)
                self.assertEqual(report['result_encoding_error_type'], 'TypeError')
                self.assertEqual(raised.exception.query_result.name, 'director')
                self.assertEqual(raised.exception.query_result.tolist(), ['Michael Ritchie'])
                self.assertEqual(read_json(output / 'driver.json')['status'], 'failed')
                self.assertTrue((output / 'native-worker/result-ready.json').exists())

    def test_CLI_selects_MMQA_source_fields_without_movie_configuration(self):
        path = self.root / 'mmqa-config.json'
        with patch.object(driver, 'run_query', side_effect=AssertionError('native execution forbidden')):
            for query in ('Q1', 'Q4'):
                path.write_text(json.dumps(self.mmqa_config(query)))
                with self.subTest(query=query), redirect_stdout(StringIO()) as stream:
                    self.assertEqual(driver.main(['check', '--config', str(path)]), 0)
                self.assertEqual(json.loads(stream.getvalue())['task']['scenario'], 'mmqa')

    def test_zero_occurrences_keep_the_real_empty_sink_without_a_worker(self):
        self.write_rows([('other','outside','good outside','POSITIVE')])
        supervise = SoftwareSupervisor()
        output = self.root / 'empty'
        report = driver.run_query(self.config('Q3'), output, model_python=sys.executable,
                                  tokenizer=SoftwareTokenizer(), supervise=supervise)
        self.assertEqual(report['result']['data'], [[0]])
        self.assertEqual(report['physical_attempts_reserved'], 0)
        self.assertIsNone(report['model_ready_before_source'])
        self.assertIsNone(report['model_startup_seconds'])
        self.assertEqual(supervise.calls, [])
        self.assertFalse((output / 'native-worker').exists())
        self.assertGreaterEqual(report['source_to_sink_seconds'], 0)

    def test_strict_parser_and_occurrence_failure_remain_failed_and_release_the_worker(self):
        for mode in ('invalid','missing'):
            with self.subTest(mode=mode), self.assertRaises(ValueError) as error:
                self.invoke(mode=mode)
            report = error.exception.query_report
            self.assertEqual(report['status'], 'failed')
            self.assertIsNone(report['sink_ready_ns'])
            self.assertNotIn('quality', report)
            self.assertEqual(report['physical_attempts_reserved'], 4)
            self.assertTrue((self.output / 'native-worker/result-release.json').exists())

    def test_close_failure_preserves_completed_sink_without_claiming_success(self):
        with self.assertRaises(RuntimeError) as error:
            self.invoke(mode='close-error')
        report = error.exception.query_report
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['result']['data'], [[2]])
        self.assertIsNotNone(report['source_to_sink_seconds'])
        self.assertNotIn('quality', report)
        self.assertEqual(report['physical_attempts_reserved'], 4)

    def test_budget_and_configuration_rejections_precede_native_work(self):
        config = self.config()
        for change in (dict(schema='semloom.native_daft_sembench.v1'),
                       dict(sampling_params=dict(n=1)),
                       dict(sampling_params=dict(n=True,response_format=dict(type='json_object'))),
                       dict(provider_options=dict(num_gpus=1))):
            with self.subTest(change=change), self.assertRaises(ValueError):
                driver.check(dict(config, **change))
        config['max_model_calls'] = 1
        supervise = SoftwareSupervisor()
        with self.assertRaisesRegex(ValueError, 'finite native budget') as error:
            driver.run_query(config, self.root / 'budget', model_python=sys.executable,
                             tokenizer=SoftwareTokenizer(), supervise=supervise)
        self.assertEqual(error.exception.query_report['physical_attempts_reserved'], 0)
        self.assertEqual(supervise.calls, [])

    def test_report_write_failure_keeps_the_completed_sink_and_original_exception(self):
        original = driver._save_artifact
        failure = OSError('software final report write failed')
        def write(path, value, **options):
            if path.name == 'driver.json':
                raise failure
            return original(path, value, **options)
        with patch.object(driver, '_save_artifact', side_effect=write):
            with self.assertRaises(OSError) as raised:
                self.invoke()
        self.assertIs(raised.exception, failure)
        report = raised.exception.query_report
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['result']['data'], [[2]])
        self.assertNotIn('quality', report)
        self.assertEqual(report['physical_attempts_reserved'], 4)

    def test_CLI_run_without_execution_flag_checks_only_and_allocates_no_output(self):
        config = self.root / 'config.json'; write_private_json(config, self.config())
        output = self.root / 'not-created'
        for action in ('check','run'):
            with self.subTest(action=action), redirect_stdout(StringIO()) as stream:
                self.assertEqual(driver.main([action,'--config',str(config),'--output-dir',str(output)]), 0)
            self.assertFalse(json.loads(stream.getvalue())['execution_enabled'])
        self.assertFalse(output.exists())

    def test_CLI_requires_Q10_gold_and_rejects_gold_for_other_tasks(self):
        with self.csv.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(('id', 'reviewText'))
            writer.writerow(('A', 'good'))
        movies = self.root / 'Movies.csv'
        with movies.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(('id', 'audienceScore'))
            writer.writerow(('A', 100))
        path = self.root / 'q10-config.json'
        path.write_text(json.dumps(self.config('Q10', movies_csv=movies)))
        with patch.object(driver, 'run_query', side_effect=AssertionError('native execution forbidden')):
            with redirect_stdout(StringIO()) as stream:
                self.assertEqual(driver.main(['check', '--config', str(path)]), 0)
            self.assertEqual(json.loads(stream.getvalue())['task']['query'], 'Q10')
            for config in (self.config('Q10'), self.config('Q10', ranking='map', movies_csv=movies),
                           self.config('Q3', movies_csv=movies), self.config('Q8', movies_csv=movies),
                           self.config('Q9', ranking='map', movies_csv=movies)):
                path.write_text(json.dumps(config))
                with self.subTest(query=config['query']), self.assertRaises(ValueError):
                    driver.main(['check', '--config', str(path)])
        supervise = SoftwareSupervisor()
        output = self.root / 'missing-gold-run'
        with self.assertRaises(FileNotFoundError) as raised:
            driver.run_query(self.config('Q10', movies_csv=self.root / 'missing-Movies.csv'), output,
                model_python=sys.executable, tokenizer=SoftwareTokenizer(), supervise=supervise)
        self.assertEqual(supervise.calls, [])
        self.assertIsNone(raised.exception.query_report['source_start_ns'])
        self.assertFalse((output / 'physical-budget.sqlite').exists())

    def test_CLI_dispatches_maps_and_requires_the_explicit_Q9_map_profile(self):
        path = self.root / 'map-config.json'
        output = self.root / 'not-created'
        with self.csv.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(('id', 'reviewId', 'reviewText', 'scoreSentiment', 'originalScore'))
            writer.writerow(('taken_3', 'row', 'good', 'POSITIVE', '5/5'))
        with patch.object(driver, 'run_query', side_effect=AssertionError('native execution forbidden')):
            for query, ranking in (('Q8', None), ('Q9', 'map')):
                config = self.config(query, ranking=ranking)
                path.write_text(json.dumps(config))
                with self.subTest(query=query), redirect_stdout(StringIO()) as stream:
                    self.assertEqual(driver.main(['check', '--config', str(path)]), 0)
                self.assertEqual(json.loads(stream.getvalue())['task']['query'], query)
            for query, ranking in (('Q9', None), ('Q9', 'topk'), ('Q8', 'map')):
                path.write_text(json.dumps(self.config(query, ranking=ranking)))
                with self.subTest(query=query, ranking=ranking), self.assertRaises(ValueError):
                    driver.main(['check', '--config', str(path)])
        self.assertFalse(output.exists())

    def test_fresh_driver_and_worker_forbid_LOTUS_through_the_complete_official_score(self):
        program = IMPORT_GUARD + r'''
import json
from src.experiments.sembench_native_daft import run_query
from src.infrastructure.private_artifacts import read_json
from tests.experiments.test_sembench_native_daft import SoftwareTokenizer,SoftwareSupervisor
report=run_query(read_json(sys.argv[1]),sys.argv[2],model_python=sys.executable,
 tokenizer=SoftwareTokenizer(),supervise=SoftwareSupervisor(sys.argv[3]))
print(json.dumps(dict(status=report['status'],result=report['result'],quality=report['quality'],
 forbidden_import_attempts=forbidden,loaded_forbidden=[name for name in sys.modules if name=='lotus'
 or name.startswith('lotus.') or name=='src.baselines.text.sembench_text'
 or name=='src.semantic_methods.lotus' or name.startswith('src.semantic_methods.lotus.')],
 actual_models=0,native_SDK_executed=False)))
'''
        for mode, settings, expected, metric, score in (
                ('success', self.config(), [[2]], 'absolute_error', 0),
                ('director', self.mmqa_config(), ['Michael Ritchie'], 'precision', 1)):
            with self.subTest(mode=mode):
                config, output = self.root / (mode+'-config.json'), self.root / (mode+'-isolated')
                write_private_json(config, settings)
                result = subprocess.run([sys.executable,'-B','-c',program,str(config),str(output),mode],
                    capture_output=True,text=True,env=dict(os.environ,PYTHONPATH=CODE),timeout=45)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                value = json.loads(result.stdout.splitlines()[-1])
                self.assertEqual(value['status'], 'completed')
                self.assertEqual(value['forbidden_import_attempts'], [])
                self.assertEqual(value['loaded_forbidden'], [])
                self.assertEqual(value['result']['data'], expected)
                self.assertEqual(value['quality']['original_metric'][metric], score)


if __name__ == '__main__':
    unittest.main()
