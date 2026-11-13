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
  if (fullname.split('.')[0] in {'lotus','daft','vllm','torch','transformers'}
      or fullname=='src.experiments.native_daft_sembench'
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
    def __init__(self, mode='success', *, program=CPU_WORKER):
        self.mode, self.calls, self.program = mode, [], program

    def __call__(self, config, directory, **options):
        from src.experiments import lotus_records_supervisor as processes
        self.calls.append((Path(config), Path(directory)))
        def launch(command, output, **kwargs):
            # The host sandbox denies global psutil/sysctl process enumeration.
            # This fixture observes its Popen child, not host/GPU/tree cleanup.
            new_private_directory(output)
            fixture_command = [sys.executable, '-B', '-c', self.program, str(config), str(directory), self.mode]
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

    def animals_config(self):
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
        config = self.config('Q1')
        del config['reviews_csv']
        config.update(scenario='animals', manifest=str(manifest), manifest_digest=value['sha256'])
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

    def test_Animals_content_parts_are_rejected_by_check_and_string_worker_boundary(self):
        config = self.animals_config()
        task = driver.task_from_config(config)
        with patch.object(driver, 'task_from_config', return_value=task), \
                patch.object(type(task), 'requests', side_effect=AssertionError('check must not expand requests')) as requests:
            with self.assertRaisesRegex(ValueError, 'image content'):
                driver.check(config)
        requests.assert_not_called()
        tokenizer, supervise = SoftwareTokenizer(), SoftwareSupervisor()
        with patch.object(tokenizer, 'apply_chat_template', side_effect=AssertionError('image must not be templated')) as render:
            with self.assertRaisesRegex(ValueError, 'non-text message parts') as raised:
                driver.run_query(config, self.root / 'image-rejected', model_python=sys.executable,
                    tokenizer=tokenizer, supervise=supervise)
        render.assert_not_called()
        self.assertEqual(supervise.calls, [])
        self.assertEqual(raised.exception.query_report['physical_attempts_reserved'], 0)

    def test_string_renderer_checks_the_entire_batch_before_any_template_call(self):
        tokenizer = SoftwareTokenizer()
        requests = [dict(call_id='text', messages=[dict(role='user', content='plain text')]),
                    dict(call_id='parts', messages=[dict(role='user', content=[dict(type='text', text='array')])])]
        prepared = driver._render(requests, tokenizer)
        self.assertEqual(tokenizer.messages, [row['messages'] for row in requests])
        self.assertEqual([json.loads(row['prompt']) for row in prepared], tokenizer.messages)
        requests.append(dict(call_id='image', messages=[dict(role='user', content=[
            dict(type='image_url', image_url=dict(url='data:image/png;base64,unused-software-image'))])]))
        with patch.object(tokenizer, 'apply_chat_template', side_effect=AssertionError('no partial batch rendering')) as render:
            with self.assertRaisesRegex(ValueError, 'non-text message parts'):
                driver._render(requests, tokenizer)
        render.assert_not_called()

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


ATTACHED_WORKER = IMPORT_GUARD + r'''
import asyncio,json,time
import httpx
from pathlib import Path
from src.experiments.native_daft_query import run_worker
from src.baselines.text.frameworks.daft_sembench_native import effective_openai_messages
from src.infrastructure.private_artifacts import read_json,write_private_json
config,directory,mode=read_json(sys.argv[1]),Path(sys.argv[2]),sys.argv[3]
attempts=0
async def send(client,request,*args,**kwargs):
 global attempts
 attempts+=1
 assert str(request.url)=='http://localhost:18473/v1/chat/completions'
 assert request.headers['authorization']=='Bearer software-key'
 if mode=='failure' or mode=='unknown-recovered' and attempts==1:
  raise httpx.ReadTimeout('software API response unknown',request=request)
 if mode=='retry' and attempts==1:
  return httpx.Response(503,request=request,json=dict(error='software native retry fixture'))
 body=json.loads(request.content)
 content=body['messages'][1]['content']
 text=json.loads(content if isinstance(content,str) else content[0]['text'])['text']
 generated=json.dumps(dict(keep=text.startswith('good')))
 response=dict(id='software-'+directory.parent.name+'-'+str(attempts),model=body['model'],
  choices=[dict(index=0,message=dict(role='assistant',content=generated),finish_reason='stop')],
  usage=dict(prompt_tokens=1,completion_tokens=1,total_tokens=2))
 return httpx.Response(200,request=request,json=response)
httpx.AsyncClient.send=send

def execute(rows,*,model,sampling_params,provider_options,on_result_ready):
 async def all_rows():
  predictions=[]
  async with httpx.AsyncClient(headers={'authorization':'Bearer '+provider_options['api_key']}) as client:
   for row in rows:
    body=dict(sampling_params,model=model,messages=row['messages'] if mode=='raw' else effective_openai_messages(row['messages']))
    for retry in range(2):
     try:
      response=await client.post(provider_options['base_url'].rstrip('/')+'/chat/completions',json=body)
      if response.status_code==503 and retry==0:continue
      response.raise_for_status()
      break
     except httpx.ReadTimeout:
      if mode!='unknown-recovered' or retry:raise
    predictions.append(dict(call_id=row['call_id'],generated_text=response.json()['choices'][0]['message']['content']))
  return list(reversed(predictions))
 result=dict(predictions=asyncio.run(all_rows()),metadata=dict(
  result_ready_monotonic_ns=time.monotonic_ns(),model_close_receipt=None,model_process_owned=False,
  message_encoding='daft.openai.system_single_user.parts.v1',target_renderer_equivalence_verified=None))
 on_result_ready(result)
 return result
try:run_worker(config,directory,execute_query=execute)
finally:write_private_json(directory/'fixture-imports.json',dict(forbidden_import_attempts=forbidden,real_native_SDK=False,actual_models=0))
'''


@unittest.skipUnless(CHECKOUT and all(importlib.util.find_spec(name) for name in ('pandas','duckdb','cdlib')),
                     'requires pinned SemBench and existing CPU scorer dependencies')
class AttachedDaftSessionTests(unittest.TestCase):
    setUp = IndependentDaftQueryTests.setUp
    write_rows = IndependentDaftQueryTests.write_rows

    def config(self):
        config = IndependentDaftQueryTests.config(self)
        config.pop('max_model_calls')
        return dict(config, schema=driver.ATTACHED_SCHEMA, max_http_attempts=8, session_timeout_s=45,
            native_openai=dict(base_url_env='CPU_DAFT_SESSION_BASE', api_key_env='CPU_DAFT_SESSION_KEY'),
            provider_options=dict(max_retries=0))

    def invoke(self, *, mode='success', config=None, identity_probe=None):
        self.output = self.root / ('session-' + mode)
        self.supervisor = SoftwareSupervisor(mode, program=ATTACHED_WORKER)
        with patch.dict(os.environ, CPU_DAFT_SESSION_BASE='http://localhost:18473/v1',
                        CPU_DAFT_SESSION_KEY='software-key'):
            return driver.run_attached_session(config or self.config(), self.output,
                model_python=sys.executable, supervise=self.supervisor, identity_probe=identity_probe)

    @staticmethod
    def identity(pid=222):
        return dict(observed_monotonic_ns=time.monotonic_ns(), host_id='software-host',
            service_pid=111, service_create_time=10., model_processes=[dict(pid=pid,create_time=20.)])

    def test_two_fresh_queries_share_one_permanent_budget_with_ACK_and_fresh_owner_proof(self):
        snapshots = []
        def probe():
            snapshots.append(len(snapshots))
            if len(snapshots) == 3:
                with self.csv.open('a') as stream:
                    stream.write('taken_3,new,good new,POSITIVE\n')
            return self.identity()
        report = self.invoke(config=dict(self.config(), max_http_attempts=9), identity_probe=probe)
        self.assertEqual(report['status'], 'completed')
        self.assertTrue(report['model_owner_reused'])
        self.assertTrue(report['warm_qualified'])
        self.assertFalse(report['target_native_model_qualified'])
        self.assertIsNone(report['service_ready_before_session'])
        self.assertIsNone(report['model_startup_seconds'])
        self.assertFalse(report['remote_service_shutdown_owned'])
        self.assertFalse(report['HTTP_JSONL_evidence_used'])
        self.assertIn('worker.json', report['HTTP_evidence_source'])
        self.assertEqual([row['result']['data'] for row in report['queries']], [[[2]], [[3]]])
        self.assertEqual([row['physical_HTTP_attempts'] for row in report['queries']], [4,5])
        self.assertEqual(report['physical_HTTP_attempts'], 9)
        self.assertEqual(len(report['budget']['units']), 1)
        workers = [read_json(path/'worker.json') for _, path in self.supervisor.calls]
        for row, terminal in zip(report['queries'], workers):
            self.assertEqual(row['quality']['original_metric']['absolute_error'], 0)
            self.assertEqual(row['HTTP_audit']['unresolved_attempts'], 0)
            self.assertFalse(row['HTTP_audit']['raw_HTTP_call_id_association_available'])
            self.assertLessEqual(row['sink_ready_ns'], terminal['result_release']['released_monotonic_ns'])
            self.assertLessEqual(row['sink_ready_ns'], row['source_audit_started_ns'])
            self.assertTrue(terminal['result_release']['confirmed'])
            self.assertFalse(terminal['model_process_owned'])
            self.assertFalse(terminal['remote_service_shutdown_owned'])
        packets = [read_json(path)['request_budget'] for path, _ in self.supervisor.calls]
        self.assertEqual(packets[0], packets[1])
        self.assertLess(report['queries'][0]['client_worker_exit_observed_ns'], report['queries'][1]['source_start_ns'])
        from src.experiments.shared_request_budget import SharedClaimedUnit
        from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
        unit = SharedClaimedUnit(self.output/'physical-budget.sqlite',
            AttemptBudget(self.config()['query_id'], 9), 'session')
        with self.assertRaisesRegex(BudgetExhausted, 'closed'):
            unit.reserve('0'*64)

    def test_missing_stale_or_endpoint_only_probe_keeps_warm_unknown(self):
        for index, probe in enumerate((None, lambda: dict(self.identity(), observed_monotonic_ns=0),
                                       lambda: dict(endpoint='http://localhost:18473/v1'))):
            with self.subTest(index=index):
                self.root = Path(self.temporary.name) / str(index)
                self.root.mkdir()
                report = self.invoke(identity_probe=probe)
                self.assertEqual(len(report['queries']), 2)
                self.assertIsNone(report['model_owner_reused'])
                self.assertIsNone(report['warm_qualified'])

    def test_owner_change_stops_before_second_worker_and_retains_first_complete_result(self):
        calls = []
        def probe():
            calls.append(1)
            return self.identity(222 if len(calls) < 3 else 333)
        with self.assertRaisesRegex(RuntimeError, 'owner changed') as caught:
            self.invoke(identity_probe=probe)
        report = caught.exception.query_report
        self.assertEqual(len(self.supervisor.calls), 1)
        self.assertEqual(report['queries'][0]['result']['data'], [[2]])
        self.assertIsNone(report['queries'][1]['source_start_ns'])
        self.assertFalse(report['model_owner_reused'])
        self.assertIsNone(report['warm_qualified'])
        self.assertEqual(report['physical_HTTP_attempts'], 4)

    def test_retry_attempts_are_charged_and_exhaustion_never_starts_another_worker(self):
        report = self.invoke(mode='retry', config=dict(self.config(), max_http_attempts=10))
        self.assertEqual([row['physical_HTTP_attempts'] for row in report['queries']], [5,5])
        self.assertEqual(report['physical_HTTP_attempts'], 10)
        self.assertTrue(all(row['HTTP_audit']['successful_responses'] == 4 for row in report['queries']))
        with self.assertRaisesRegex(ValueError, 'exhausted') as caught:
            self.invoke(mode='exhausted', config=dict(self.config(), max_http_attempts=4))
        self.assertEqual(len(self.supervisor.calls), 1)
        self.assertEqual(caught.exception.query_report['physical_HTTP_attempts'], 4)
        self.assertEqual(caught.exception.query_report['queries'][0]['result']['data'], [[2]])

    def test_failed_or_unknown_HTTP_stops_next_round_and_preserves_raw_evidence(self):
        for mode in ('failure', 'unknown-recovered', 'raw'):
            with self.subTest(mode=mode), self.assertRaises((RuntimeError,ValueError)) as caught:
                self.invoke(mode=mode)
            report = caught.exception.query_report
            self.assertEqual(len(self.supervisor.calls), 1)
            self.assertEqual(report['status'], 'failed')
            self.assertIsNone(report['warm_qualified'])
            row = report['queries'][0]
            self.assertNotIn('quality', row)
            terminal = read_json(self.output/'query-1/native-worker/worker.json')
            self.assertEqual(terminal['native_api'], row['native_api'])
            self.assertTrue(terminal['native_api']['requests'])
            self.assertTrue((self.output/'query-1/prepared-messages.json').exists())
            if mode == 'failure':
                self.assertEqual(row['physical_HTTP_attempts'], 1)
                self.assertEqual(row['native_api']['unresolved'], 1)
                self.assertNotIn('result', row)
            else:
                self.assertEqual(row['result']['data'], [[2]])
                self.assertTrue(row['result_release']['confirmed'])
                self.assertEqual(row['physical_HTTP_attempts'], 5 if mode == 'unknown-recovered' else 4)
                if mode == 'unknown-recovered':
                    self.assertEqual(row['native_api']['unresolved'], 1)

    def test_post_sink_encoding_or_persistence_failures_keep_primary_and_complete_raw_sink(self):
        failure = RuntimeError('official evaluation failed')
        from src.baselines.text.sembench_native_text import MovieFilterTask
        with patch.object(MovieFilterTask, 'evaluate', side_effect=failure), \
                patch.object(MovieFilterTask, 'encode_result', side_effect=TypeError('software encode failure')):
            with self.assertRaises(RuntimeError) as caught:
                self.invoke(mode='encoding')
        self.assertIs(caught.exception, failure)
        report = failure.query_report
        self.assertEqual(len(self.supervisor.calls), 1)
        self.assertEqual(failure.session_results[0].iloc[0,0], 2)
        self.assertEqual(report['queries'][0]['result_encoding_error_type'], 'TypeError')
        self.assertNotIn('quality', report['queries'][0])
        self.assertTrue(report['queries'][0]['result_release']['confirmed'])
        self.assertEqual(report['physical_HTTP_attempts'], 4)
        original = driver._save_artifact
        write_failure = OSError('software per-query report write failed')
        def save(path, value, **options):
            if path.name == 'driver.json':
                raise write_failure
            return original(path, value, **options)
        with patch.object(driver, '_save_artifact', side_effect=save):
            with self.assertRaises(OSError) as caught:
                self.invoke(mode='write-failure')
        self.assertIs(caught.exception, write_failure)
        report = write_failure.query_report
        self.assertEqual(len(self.supervisor.calls), 1)
        self.assertEqual(report['queries'][0]['result']['data'], [[2]])
        self.assertEqual(report['queries'][0]['status'], 'failed')
        self.assertNotIn('quality', report['queries'][0])

    def test_finite_allowance_is_checked_before_requests_and_missing_gold_precedes_budget(self):
        from src.baselines.text.sembench_native_text import MovieFilterTask
        with patch.object(MovieFilterTask, 'requests', side_effect=AssertionError('no request allocation')) as requests:
            with self.assertRaisesRegex(ValueError, 'remaining HTTP allowance') as caught:
                self.invoke(mode='too-small', config=dict(self.config(), max_http_attempts=3))
        requests.assert_not_called()
        self.assertEqual(self.supervisor.calls, [])
        self.assertEqual(caught.exception.query_report['physical_HTTP_attempts'], 0)
        config = dict(self.config(), query='Q10', movies_csv=str(self.root/'missing-Movies.csv'))
        with self.assertRaises(FileNotFoundError) as caught:
            self.invoke(mode='missing-gold', config=config)
        self.assertEqual(self.supervisor.calls, [])
        self.assertFalse((self.output/'physical-budget.sqlite').exists())
        self.assertEqual(caught.exception.query_report['queries'], [])

    def test_attached_CLI_checks_explicit_schema_without_worker_or_budget_and_requires_same_layout(self):
        path = self.root/'attached-config.json'
        write_private_json(path, self.config())
        output = self.root/'unused'
        with patch.dict(os.environ, CPU_DAFT_SESSION_BASE='http://localhost:18473/v1',
                        CPU_DAFT_SESSION_KEY='software-key'), \
                patch.object(driver, 'run_attached_session', side_effect=AssertionError('no native execution')):
            for action in ('check','attached-session'):
                with self.subTest(action=action), redirect_stdout(StringIO()) as stream:
                    self.assertEqual(driver.main([action,'--config',str(path),'--output-dir',str(output)]), 0)
                self.assertEqual(json.loads(stream.getvalue())['schema'], driver.ATTACHED_SCHEMA)
            for change in (dict(max_http_attempts=7), dict(engine_kwargs=dict(tensor_parallel_size=1)),
                           dict(sampling_params=dict(n=1,response_format=dict(type='json_object'),stream=True))):
                with self.subTest(change=change), self.assertRaises(ValueError):
                    driver.check_attached_session(dict(self.config(), **change))
        self.assertFalse(output.exists())
        rows = [dict(call_id=str(i), messages=[dict(role='system',content=system),dict(role='user',content='text')])
                for i, system in enumerate(('zebra','impala'))]
        with self.assertRaisesRegex(ValueError, 'one static system'):
            driver._attached_layout(rows)

    def test_fresh_attached_driver_and_actual_worker_forbid_LOTUS_and_engines_through_official_score(self):
        program = IMPORT_GUARD + r'''
import json
from src.experiments.sembench_native_daft import run_attached_session
from src.infrastructure.private_artifacts import read_json
from tests.experiments.test_sembench_native_daft import SoftwareSupervisor,ATTACHED_WORKER
report=run_attached_session(read_json(sys.argv[1]),sys.argv[2],model_python=sys.executable,
 supervise=SoftwareSupervisor(program=ATTACHED_WORKER))
assert not forbidden
assert not [name for name in sys.modules if name.startswith(('lotus','daft','vllm','torch','transformers'))
 or name=='src.experiments.native_daft_sembench' or name.startswith('src.semantic_methods.lotus')]
print(json.dumps(dict(status=report['status'],scores=[q['quality']['original_metric'] for q in report['queries']],
 forbidden_import_attempts=forbidden,actual_models=0,native_SDK_executed=False)))
'''
        config = self.root/'fresh.json'
        write_private_json(config, self.config())
        result = subprocess.run([sys.executable,'-B','-c',program,str(config),str(self.root/'fresh')],
            capture_output=True,text=True,env=dict(os.environ,PYTHONPATH=CODE,
                CPU_DAFT_SESSION_BASE='http://localhost:18473/v1',CPU_DAFT_SESSION_KEY='software-key'),timeout=45)
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        value = json.loads(result.stdout.splitlines()[-1])
        self.assertEqual(value['status'], 'completed')
        self.assertEqual([score['absolute_error'] for score in value['scores']], [0,0])
        for index in (1,2):
            imports = read_json(self.root/'fresh'/('query-'+str(index))/'native-worker/fixture-imports.json')
            self.assertEqual(imports['forbidden_import_attempts'], [])
            self.assertEqual(imports['actual_models'], 0)


if __name__ == '__main__':
    unittest.main()
