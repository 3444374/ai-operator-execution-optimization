"""Real CPU child-process handoff and original LOTUS queries; model replies are fixtures."""

from copy import deepcopy
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

from src.experiments import native_daft_query as worker, native_daft_sembench as driver
from src.infrastructure.private_artifacts import read_json, write_private_json
from tests.experiments import test_native_daft_query as worker_fixture
from tests.experiments import test_sembench_lotus_mmqa_aggregate as aggregate_fixture


CHILD = r'''
import importlib.abc,json,sys,time
from pathlib import Path
sys.path.insert(0,sys.argv[4])
class NoEngine(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,*args):
  if fullname.split('.')[0] in {'daft','vllm','torch','transformers','lotus'}: raise AssertionError('CPU worker imported real engine SDK: '+fullname)
sys.meta_path.insert(0,NoEngine())
from src.experiments.native_daft_query import run_worker
from src.infrastructure.private_artifacts import read_json,write_private_json
config,directory,mode=read_json(sys.argv[1]),Path(sys.argv[2]),sys.argv[3]
def execute(rows,**options):
 write_private_json(directory/'model-construct.json',dict(constructed_ns=time.monotonic_ns()))
 predictions=[]
 for row in rows:
  options['before_generate'](row['call_id'],'CPU-'+row['call_id'],row['prompt'])
  messages=json.loads(row['prompt']) if row['prompt'].startswith('[') else [dict(content=row['prompt'])]
  prompt=messages[0]['content']
  if mode=='fail-reduction' and '\n\tSource ' in prompt:
   error=ValueError('CPU reduction failed')
   error.native_query_report=dict(predictions=predictions,metadata=dict(native_api={},model_close_receipt=dict(status='closed')))
   raise error
  answer='Another Actor' if mode=='wrong_actor' else 'Lizzy Caplan'
  if mode=='long_summaries' and ('\n\tDocument ' in prompt or prompt.count('\n\tSource ')>2): answer+=' '+'detail '*90
  predictions.append(dict(call_id=row['call_id'],generated_text=answer))
 result=dict(predictions=predictions,metadata=dict(result_ready_monotonic_ns=time.monotonic_ns(),model_close_receipt=None,native_api=dict(generate_calls=len(rows),accepted=len(rows),final_events=len(rows),unresolved=0,errors=[])))
 options['on_result_ready'](result)
 close=dict(status='closed',close_started_monotonic_ns=time.monotonic_ns())
 write_private_json(directory/'model-close.json',close)
 result['metadata']['model_close_receipt']=close
 if mode=='close-error':
  error=RuntimeError('CPU native close failed')
  error.model_close_report=dict(close,status='unknown')
  raise error
 return result
run_worker(config,directory,execute_query=execute)
'''


class ChildSupervisor:
    def __init__(self, mode='compact'):
        self.mode, self.calls = mode, []

    def __call__(self, config_path, directory, **kwargs):
        config = read_json(config_path)
        self.calls.append((config, Path(directory)))
        def launch(command, output, **options):
            # One known CPU child: process-tree/GPU supervision is a separate target qualification.
            process = subprocess.Popen([sys.executable, '-B', '-c', CHILD, str(config_path), str(directory),
                self.mode, str(Path(__file__).resolve().parents[2])], stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True)
            until = time.monotonic() + options['timeout_s']
            try:
                while True:
                    options['monitor'](())
                    if process.poll() is not None:
                        if process.returncode:
                            raise RuntimeError('CPU prepared worker failed')
                        return dict(status='CPU child exited', scope='one known software child; no host/GPU qualification')
                    if time.monotonic() >= until:
                        raise TimeoutError('CPU child fixture exceeded its finite allowance')
                    time.sleep(.005)
            finally:
                try:
                    process.wait(timeout=options['grace_s'])
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=options['grace_s'])
                raw, _ = process.communicate(timeout=options['grace_s'])
                from src.infrastructure.redact import redact_text
                Path(directory, 'CPU-worker.log').write_text(redact_text(raw))
        with patch('src.experiments.lotus_records_supervisor.supervise', side_effect=launch):
            return worker.supervised_run(config_path, directory, **kwargs)


class ResultReleaseTests(unittest.TestCase):
    def setUp(self):
        self.fixture = worker_fixture.NativeDaftWorkerTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root

    def invoke(self, callback, *, release=True):
        config = dict(self.fixture.config, query_timeout_s=5, cleanup_s=2, stop_grace_s=.2)
        if release:
            config['wait_for_result_release'] = True
        path = self.root/'config.json'
        write_private_json(path, config)
        return ChildSupervisor()(path, self.root/'worker', model_python=sys.executable, on_result_ready=callback)

    def test_real_child_waits_for_full_parent_sink_before_starting_close(self):
        observed = {}
        def consume(raw):
            self.assertFalse((self.root/'worker/model-close.json').exists())
            observed['sink_ready_ns'] = time.monotonic_ns()
        self.invoke(consume)
        close = read_json(self.root/'worker/model-close.json')
        self.assertLessEqual(observed['sink_ready_ns'], close['close_started_monotonic_ns'])
        terminal = read_json(self.root/'worker/worker.json')
        self.assertTrue(terminal['result_release']['confirmed'])
        self.assertLessEqual(observed['sink_ready_ns'], terminal['result_release']['released_monotonic_ns'])

    def test_consumer_failure_is_primary_and_still_publishes_release(self):
        failure = ValueError('CPU original parser failed')
        def consume(raw):
            raise failure
        with self.assertRaises(ValueError) as caught:
            self.invoke(consume)
        self.assertIs(caught.exception, failure)
        self.assertTrue((self.root/'worker/result-release.json').exists())
        self.assertTrue((self.root/'worker/result-ready.json').exists())

    def test_legacy_worker_defaults_keep_no_release_gate(self):
        self.invoke(lambda raw: None, release=False)
        terminal = read_json(self.root/'worker/worker.json')
        self.assertNotIn('result_release', terminal)
        self.assertFalse((self.root/'worker/result-release.json').exists())
        self.assertNotIn('result_release_required', read_json(self.root/'worker/deadline.json'))

    def test_release_wait_is_bounded_and_detects_a_dead_parent(self):
        with patch.object(worker.time, 'monotonic', return_value=2), patch.object(worker.os, 'getppid', return_value=7):
            with self.assertRaises(TimeoutError):
                worker._wait_for_result_release(self.root, 1, 1, 7)
            with self.assertRaises(InterruptedError):
                worker._wait_for_result_release(self.root, 1, 9, 8)

    def test_cleanup_begins_at_release_without_resetting_the_query_deadline(self):
        now = [0.]
        path = self.root/'ready.json'
        def consume(raw):
            now[0] = 20.
        deadline = worker.QueryDeadline(path, query_s=30, cleanup_s=5, stop_grace_s=1,
            clock=lambda: now[0], on_result_ready=consume, wait_for_result_release=True)
        write_private_json(path, dict(schema='semloom.native_daft_map.result_ready.v1', result_ready_monotonic_ns=1000000000))
        now[0] = 2.
        deadline(())
        self.assertEqual(deadline.report()['query_deadline_monotonic'], 30.)
        self.assertEqual(deadline.report()['cleanup_deadline_monotonic'], 25.)
        now[0] = 24.
        with self.assertRaisesRegex(TimeoutError, 'post-result close'):
            deadline(())

    def test_timely_final_sink_has_its_full_separate_cleanup_allowance(self):
        now = [0.]
        path = self.root/'ready.json'
        deadline = worker.QueryDeadline(path, query_s=30, cleanup_s=5, stop_grace_s=1,
            clock=lambda: now[0], on_result_ready=lambda raw: None, wait_for_result_release=True)
        write_private_json(path, dict(schema='semloom.native_daft_map.result_ready.v1',
                                     result_ready_monotonic_ns=29_000_000_000))
        now[0] = 29.
        deadline(())
        self.assertEqual(deadline.report()['query_deadline_monotonic'], 30.)
        self.assertEqual(deadline.report()['cleanup_deadline_monotonic'], 34.)
        for now[0] in (31., 32.999):
            deadline(())
        now[0] = 33.
        with self.assertRaisesRegex(TimeoutError, 'post-result close'):
            deadline(())

    def test_late_consumer_release_does_not_gain_a_new_query_deadline(self):
        now = [0.]
        path = self.root/'ready.json'
        def consume(raw):
            now[0] = 30.1
        deadline = worker.QueryDeadline(path, query_s=30, cleanup_s=5, stop_grace_s=1,
            clock=lambda: now[0], on_result_ready=consume, wait_for_result_release=True)
        write_private_json(path, dict(schema='semloom.native_daft_map.result_ready.v1',
                                     result_ready_monotonic_ns=29_000_000_000))
        now[0] = 29.
        with self.assertRaisesRegex(TimeoutError, 'complete query exceeded'):
            deadline(())
        self.assertEqual(deadline.report()['query_deadline_monotonic'], 30.)

    def test_callback_error_survives_failed_release_publication(self):
        path = self.root/'ready.json'
        write_private_json(path, dict(schema='semloom.native_daft_map.result_ready.v1', result_ready_monotonic_ns=1))
        failure = ValueError('CPU original consumer failed')
        def fail(raw):
            raise failure
        deadline = worker.QueryDeadline(path, query_s=20, cleanup_s=5, stop_grace_s=1,
            on_result_ready=fail, wait_for_result_release=True)
        with patch.object(worker, 'publish_private_json', side_effect=OSError('CPU ACK disk failure')):
            with self.assertRaises(ValueError) as caught:
                deadline(())
        self.assertIs(caught.exception, failure)
        self.assertIsNone(deadline.report()['result_release_monotonic'])
        self.assertTrue(any('write failed' in note for note in failure.__notes__))

    def test_worker_missing_release_fails_boundedly_and_keeps_unknown_close(self):
        config = dict(self.fixture.config, query_timeout_s=.01, wait_for_result_release=True)
        directory = self.root/'unreleased'
        def execute(rows, **options):
            options['on_result_ready'](dict(predictions=[dict(call_id=row['call_id'],generated_text='CPU') for row in rows],
                metadata=dict(result_ready_monotonic_ns=time.monotonic_ns(),native_api={},model_close_receipt=None)))
        with self.assertRaises(TimeoutError):
            worker.run_worker(config, directory, execute_query=execute)
        report = read_json(directory/'worker.json')
        self.assertEqual(report['status'], 'failed')
        self.assertFalse(report['result_release']['confirmed'])
        self.assertIsNone(report['model_close_receipt'])
        self.assertTrue((directory/'result-ready.json').exists())


class ChatTokenizer:
    def __init__(self):
        self.messages = []

    def apply_chat_template(self, messages, *, tokenize, add_generation_prompt):
        assert tokenize is False and add_generation_prompt is True
        self.messages.append(deepcopy(messages))
        return json.dumps(messages, ensure_ascii=False)


@unittest.skipUnless(aggregate_fixture.CHECKOUT and aggregate_fixture.HAS_SDK,
                     'requires pinned original SemBench and real LOTUS SDK, no model')
class NativeDaftFullQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        aggregate_fixture.NativeMMQAAggregateTests.setUpClass()

    def setUp(self):
        self.source = aggregate_fixture.NativeMMQAAggregateTests()
        self.source.setUp()
        self.addCleanup(self.source.doCleanups)
        self.model = self.source.directory('software-model')
        shutil.copyfile(self.source.tokenizer_path, self.model/'tokenizer.json')

    def configure(self, service, data, *, cap=40):
        return dict(schema='semloom.native_daft_sembench.v1', scenario='mmqa', query='Q5',
            query_id='CPU-daft-original', sembench_checkout=str(self.source.checkout), data_dir=str(data),
            model_name=service['model_name'], model=str(self.model),
            sampling_params=dict(service['generation'], n=1), provider_options={},
            engine_kwargs=dict(max_model_len=service['native_lm_max_ctx_len'],
                tokenizer_revision=service['token_counter']['tokenizer_revision']),
            query_timeout_s=40, cleanup_s=3, stop_grace_s=.5, max_model_calls=cap,
            native_lm_max_ctx_len=service['native_lm_max_ctx_len'],
            token_counter=dict(service['token_counter'], path=str(self.model/'tokenizer.json')))

    def invoke(self, config, *, mode='compact'):
        self.last = self.source.root/('native-'+str(next(self.source.serial)))
        self.supervisor, self.tokenizer = ChildSupervisor(mode), ChatTokenizer()
        return driver.run_query(config, self.last, model_python=sys.executable,
                                tokenizer=self.tokenizer, supervise=self.supervisor)

    def test_original_single_and_summary_dependent_hierarchy_matches_service_messages_tokens_and_score(self):
        from lotus.models import LM
        count, counts = LM.count_tokens, []
        def observed(lm, value):
            total = count(lm, value)
            counts.append((time.monotonic_ns(), deepcopy(value), total))
            return total
        cases = ((self.source.config()[0], 'compact'), (self.source.multi_config()[0], 'compact'),
                 (self.source.multi_config()[0], 'long_summaries'), (self.source.config()[0], 'wrong_actor'))
        for service, mode in cases:
            with self.subTest(mode=mode, context=service['native_lm_max_ctx_len']):
                data = self.source.source()
                replies = aggregate_fixture.NativeAggregateReplies(self, mode=mode)
                with patch.object(LM, 'count_tokens', observed):
                    reference, _, _ = self.source.invoke(service, data, replies, allowance=40)
                expected_counts = [(value, total) for stamp, value, total in counts
                                   if reference['source_started_ns'] <= stamp <= reference['sink_ready_ns']]
                counts.clear()
                with patch.object(LM, 'count_tokens', observed):
                    report = self.invoke(self.configure(service, data), mode=mode)
                actual_counts = [(value, total) for stamp, value, total in counts
                                 if report['source_start_ns'] <= stamp <= report['sink_ready_ns']]
                counts.clear()
                self.assertEqual(report['result'], reference['result'])
                self.assertEqual(report['quality'], reference['quality'])
                self.assertEqual(actual_counts, expected_counts)
                self.assertEqual(self.tokenizer.messages, [request['body']['messages'] for request in replies.requests])
                self.assertEqual(report['physical_attempts_reserved'], reference['model_calls'])
                self.assertTrue(report['query_thread_closed'])
                self.assertEqual(report['query_clock_version'], 'lotus_daft.source_to_sink.v1')
                self.assertFalse(report['lotus_lm']['model_tokenizer_verified'])
                ids = []
                previous_close = None
                for index, (worker_config, directory) in enumerate(self.supervisor.calls):
                    packet = read_json(worker_config['requests_path'])
                    ids.extend(row['call_id'] for row in packet['requests'])
                    self.assertTrue(all(row['call_id'].startswith(f'CPU-daft-original-batch-{index}-row-') for row in packet['requests']))
                    self.assertEqual(worker_config['sampling_params']['max_tokens'], service['generation']['max_tokens'])
                    self.assertEqual(worker_config['sampling_params']['stop'], [])
                    construct = read_json(directory/'model-construct.json')['constructed_ns']
                    if previous_close is not None:
                        self.assertLessEqual(previous_close, construct)
                    previous_close = read_json(directory/'model-close.json')['close_started_monotonic_ns']
                self.assertEqual(len(set(ids)), len(ids))
                self.assertLessEqual(report['sink_ready_ns'], previous_close)
                if mode == 'long_summaries':
                    self.assertGreater(len(report['native_batches']), 2)

    def test_budget_stops_real_summary_work_and_retains_closed_prior_batches(self):
        service, _ = self.source.multi_config()
        data = self.source.source()
        minimum = self.source.entry.check(service, self.source.checkout, data_dir=data)['work']['minimum_model_calls']
        config = self.configure(service, data, cap=minimum)
        from src.experiments.attempt_ledger import BudgetExhausted
        with self.assertRaises(BudgetExhausted):
            self.invoke(config, mode='long_summaries')
        report = read_json(self.last/'driver.json')
        self.assertEqual(report['status'], 'failed')
        self.assertIsNone(report['source_to_sink_seconds'])
        self.assertNotIn('quality', report)
        self.assertLessEqual(report['physical_attempts_reserved'], minimum)
        self.assertTrue(report['query_thread_closed'])
        self.assertTrue(all((directory/'result-release.json').exists() for _, directory in self.supervisor.calls))

    def test_reduction_failure_keeps_native_artifacts_without_success_sink(self):
        service, _ = self.source.multi_config()
        with self.assertRaises(RuntimeError):
            self.invoke(self.configure(service, self.source.source()), mode='fail-reduction')
        report = read_json(self.last/'driver.json')
        self.assertIsNone(report['source_to_sink_seconds'])
        self.assertNotIn('quality', report)
        self.assertTrue(report['query_thread_closed'])
        self.assertTrue((self.supervisor.calls[-1][1]/'failed-result.json').exists())

    def test_final_close_failure_keeps_the_complete_original_result(self):
        service, _ = self.source.config()
        with self.assertRaises(RuntimeError):
            self.invoke(self.configure(service, self.source.source()), mode='close-error')
        report = read_json(self.last/'driver.json')
        self.assertEqual(report['status'], 'failed')
        self.assertIn('result', report)
        self.assertTrue((self.last/'result-ready.json').exists())
        self.assertNotIn('quality', report)
        self.assertTrue(report['query_thread_closed'])

    def test_original_query_error_is_primary_when_native_close_also_fails(self):
        service, _ = self.source.config()
        failure = ValueError('CPU original query postprocessing failed')
        factory = driver._query
        def query(config, generation):
            task = factory(config, generation)
            execute = task.bind_original_execution()
            def failing():
                execute()
                raise failure
            task.bind_original_execution = lambda: failing
            return task
        with patch.object(driver, '_query', side_effect=query), self.assertRaises(ValueError) as caught:
            self.invoke(self.configure(service, self.source.source()), mode='close-error')
        self.assertIs(caught.exception, failure)
        report = read_json(self.last/'driver.json')
        self.assertNotIn('quality', report)
        self.assertTrue(report['query_thread_closed'])
        self.assertTrue((self.supervisor.calls[0][1]/'result-release.json').exists())

    def test_post_sink_source_audit_failure_keeps_complete_result_without_quality(self):
        service, _ = self.source.config()
        failure = ValueError('CPU post-sink source audit failed')
        factory = driver._query
        def query(config, generation):
            task = factory(config, generation)
            verify = task.verify_source
            calls = []
            def audit():
                calls.append(True)
                if len(calls) == 2:
                    raise failure
                return verify()
            task.verify_source = audit
            return task
        with patch.object(driver, '_query', side_effect=query), self.assertRaises(ValueError) as caught:
            self.invoke(self.configure(service, self.source.source()))
        self.assertIs(caught.exception, failure)
        report = read_json(self.last/'driver.json')
        self.assertIn('result', report)
        self.assertNotIn('quality', report)
        self.assertEqual(read_json(self.last/'result-ready.json')['result'], report['result'])

    def test_unknown_work_requires_minimum_and_bound_counter_before_any_worker(self):
        service, _ = self.source.multi_config()
        config = self.configure(service, self.source.source(), cap=1)
        with self.assertRaisesRegex(ValueError, 'known minimum'):
            self.invoke(config)
        self.assertEqual(self.supervisor.calls, [])
        config['token_counter']['sha256'] = '0'*64
        with self.assertRaisesRegex(ValueError, 'SHA256'):
            self.invoke(config)
        self.assertEqual(self.supervisor.calls, [])

    def test_auto_tokenizer_uses_the_declared_native_override_and_revision(self):
        service, _ = self.source.config()
        config = self.configure(service, self.source.source())
        override = self.source.directory('software-tokenizer-override')
        shutil.copyfile(self.model/'tokenizer.json', override/'tokenizer.json')
        config['engine_kwargs']['tokenizer'] = str(override)
        config['token_counter']['path'] = str(override/'tokenizer.json')
        tokenizer = ChatTokenizer()
        with patch('transformers.AutoTokenizer.from_pretrained', return_value=tokenizer) as load:
            report = driver.run_query(config, self.source.root/'auto-tokenizer', model_python=sys.executable,
                                      supervise=ChildSupervisor())
        self.assertEqual(report['status'], 'completed')
        load.assert_called_once_with(str(override), local_files_only=True, trust_remote_code=False,
                                     revision=config['token_counter']['tokenizer_revision'])

    def test_query_thread_start_failure_stays_primary_without_any_worker(self):
        service, _ = self.source.config()
        failure = OSError('CPU query thread creation failed')
        with patch.object(driver.Thread, 'start', side_effect=failure):
            with self.assertRaises(OSError) as caught:
                self.invoke(self.configure(service, self.source.source()))
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.supervisor.calls, [])
        report = read_json(self.last/'driver.json')
        self.assertEqual(report['status'], 'failed')
        self.assertTrue(report['query_thread_closed'])
        self.assertNotIn('quality', report)

    def test_post_sink_budget_close_failure_keeps_result_and_primary_without_quality(self):
        service, _ = self.source.config()
        failure = OSError('CPU budget close failed')
        with patch.object(driver.CellBudgetLedger, 'close_shared_unit', side_effect=failure):
            with self.assertRaises(OSError) as caught:
                self.invoke(self.configure(service, self.source.source()))
        self.assertIs(caught.exception, failure)
        report = read_json(self.last/'driver.json')
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['error_type'], 'OSError')
        self.assertIn('result', report)
        self.assertEqual(read_json(self.last/'result-ready.json')['result'], report['result'])
        self.assertNotIn('quality', report)
        self.assertTrue(report['query_thread_closed'])

    def test_original_empty_filter_never_launches_a_native_worker(self):
        import pandas as pd
        from tests.experiments.test_sembench_text_original import OriginalSemBenchTextTests
        source = OriginalSemBenchTextTests()
        source.setUp()
        self.addCleanup(source.doCleanups)
        path = source.data/'Reviews.csv'
        pd.read_csv(path).iloc[:0].to_csv(path, index=False)
        query = source._query('movie', '1')
        config = dict(schema='semloom.native_daft_sembench.v1', scenario='movie', query='1',
            query_id=query.query_id, sembench_checkout=str(query.checkout), data_dir=str(source.data),
            model_name=query.model_name, model='CPU-only', sampling_params=query.generation.kwargs(),
            engine_kwargs={}, provider_options={}, query_timeout_s=30, cleanup_s=3, stop_grace_s=.5,
            max_model_calls=1)
        supervisor = ChildSupervisor()
        report = driver.run_query(config, source.data/'empty-query', model_python=sys.executable,
                                  tokenizer=ChatTokenizer(), supervise=supervisor)
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(report['result']['data'], [])
        self.assertEqual(report['physical_attempts_reserved'], 0)
        self.assertEqual(report['logical_requests'], 0)
        self.assertEqual(supervisor.calls, [])
        self.assertTrue(report['query_thread_closed'])

    def test_intermediate_close_still_consumes_the_original_query_deadline(self):
        service, _ = self.source.multi_config()
        config = self.configure(service, self.source.source())
        config['query_timeout_s'] = 30
        supervisor = ChildSupervisor()
        offset = [0.]
        clock = time.monotonic
        def supervise(*args, **kwargs):
            receipt = supervisor(*args, **kwargs)
            # The first worker is closed and the next real LM batch is pending.
            offset[0] = 31.
            return receipt
        path = self.source.root/'intermediate-deadline'
        with patch.object(driver.time, 'monotonic', side_effect=lambda: clock()+offset[0]):
            with self.assertRaisesRegex(TimeoutError, 'absolute allowance'):
                driver.run_query(config, path, model_python=sys.executable,
                                 tokenizer=ChatTokenizer(), supervise=supervise)
        self.assertEqual(len(supervisor.calls), 1)
        report = read_json(path/'driver.json')
        self.assertEqual(report['status'], 'failed')
        self.assertIsNone(report['source_to_sink_seconds'])
        self.assertNotIn('quality', report)
        self.assertTrue(report['query_thread_closed'])


if __name__ == '__main__':
    unittest.main()
