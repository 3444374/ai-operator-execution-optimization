"""Cold-scope input, native readiness and saved IPC evidence; no models."""

import csv
from copy import deepcopy
from pathlib import Path
import os
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch

from src.experiments.native_imlane_model_query import (
    QueryWindow, audit_model_result, configuration, run_worker, supervised_run,
)
from src.experiments.native_imlane_query import materialize_input, prepare_input
from src.baselines.text.sembench_movie_native import load_q3_source, q3_messages
from src.infrastructure.private_artifacts import content_digest, read_json, write_private_json


class NativeIMLaneModelWindowTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        self.now = 100.0
        self.window = QueryWindow(20, 10, 2, clock=lambda: self.now)

    def native(self, ready_ns, **kwargs):
        value = dict(status='query_returned', result_ready_monotonic_ns=ready_ns,
                     result_clock='CLOCK_MONOTONIC', positive_review_cnt=7)
        value.update(kwargs)
        write_private_json(self.root / 'query.json', value)

    def test_ready_timestamp_starts_one_cleanup_window_even_if_observed_later(self):
        self.native(110_000_000_000)
        self.now = 117
        self.window.observe(self.root / 'query.json', self.root / 'ready.json')
        self.assertEqual(self.window.begin_cleanup(), 120)
        self.assertEqual(read_json(self.root / 'ready.json')['positive_review_cnt'], 7)
        self.window.observe(self.root / 'query.json', self.root / 'ready.json')
        self.window.check()
        self.now = 118
        with self.assertRaises(TimeoutError):
            self.window.check()
        self.assertEqual(self.window.begin_cleanup(), 120)

    def test_complete_relation_window_publishes_once_before_any_later_audit(self):
        from src.experiments.native_imlane_model_query import _ResultWindow

        window = _ResultWindow(self.root, 20, 10, 2, clock=lambda: self.now)
        self.now = 110
        window.accept_ready(110_000_000_000, clock='CLOCK_MONOTONIC')
        observed = read_json(self.root / 'result-ready.json')
        self.assertEqual(observed['result_ready_monotonic_ns'], 110_000_000_000)
        self.assertEqual(observed['clock'], 'CLOCK_MONOTONIC')
        self.now = 115
        window.accept_ready(115_000_000_000, clock='CLOCK_MONOTONIC')
        self.assertEqual(read_json(self.root / 'result-ready.json'), observed)
        self.assertEqual(window.begin_cleanup(), 120)

    def test_old_driver_clock_future_clock_and_late_result_do_not_extend_query(self):
        for stamp, clock, now, error in (
            (None, 'CLOCK_MONOTONIC', 117, ValueError),
            (99_000_000_000, 'CLOCK_MONOTONIC', 117, ValueError),
            (119_000_000_000, 'CLOCK_MONOTONIC', 117, ValueError),
            (110_000_000_000, 'unspecified-clock', 117, ValueError),
            (121_000_000_000, 'CLOCK_MONOTONIC', 122, TimeoutError),
        ):
            with self.subTest(stamp=stamp, clock=clock):
                path = self.root / 'query.json'
                path.unlink(missing_ok=True)
                self.native(stamp, result_clock=clock)
                self.now = now
                window = QueryWindow(20, 10, 2, clock=lambda: 100)
                window.clock = lambda: self.now
                with self.assertRaises(error):
                    window.observe(path, self.root / 'ready.json')
                self.assertIsNone(window.ready_ns)

    def test_failure_cleanup_deadline_is_fixed_and_late_work_gets_no_new_query_window(self):
        self.now = 105
        self.assertEqual(self.window.begin_cleanup(), 115)
        self.now = 109
        self.assertEqual(self.window.begin_cleanup(), 115)
        self.window = QueryWindow(20, 10, 2, clock=lambda: 100)
        self.window.clock = lambda: 140
        self.assertEqual(self.window.begin_cleanup(), 130)

    def test_no_ready_has_original_query_deadline_and_invalid_budget_rejected(self):
        self.window.observe(self.root / 'absent.json', self.root / 'ready.json')
        self.now = 120
        with self.assertRaises(TimeoutError):
            self.window.check()
        for values in ((20, 2, 2), (20, 0, 1), (float('inf'), 10, 1), (True, 10, 1)):
            with self.subTest(values=values), self.assertRaises(ValueError):
                QueryWindow(*values)

    def test_outer_supervisor_uses_native_ready_and_reserves_force_inside_same_deadline(self):
        output, supervision = self.root / 'worker', self.root / 'supervision'
        monitor = Mock()

        def supervise(command, directory, **kwargs):
            Path(directory).mkdir()
            output.mkdir()
            write_private_json(output / 'result-ready.json', dict(
                schema='semloom.imlane.duckdb.q3.model_result_ready.v1',
                result_ready_monotonic_ns=110_000_000_000, clock='CLOCK_MONOTONIC'))
            self.assertEqual((kwargs['timeout_s'], kwargs['grace_s'], kwargs['natural_exit_grace_s']), (30, 2, 1))
            self.now = 117
            kwargs['monitor'](())
            self.now = 118
            kwargs['monitor'](())

        with patch('src.experiments.native_imlane_model_query.QueryWindow', return_value=self.window), \
                patch('src.experiments.lotus_records_supervisor.supervise', side_effect=supervise):
            with self.assertRaises(TimeoutError):
                supervised_run(['CPU-worker'], output, supervision, query_seconds=20,
                    cleanup_seconds=10, stop_grace_seconds=2, monitor=monitor)
        self.assertEqual(monitor.call_count, 2)
        self.assertEqual(read_json(supervision / 'native-deadline.json')['cleanup_deadline_monotonic'], 120)

    def test_outer_supervision_artifacts_cannot_precreate_worker_output(self):
        output = self.root / 'worker'
        for supervision in (output, output / 'supervision'):
            with self.subTest(supervision=supervision), self.assertRaises(ValueError):
                supervised_run(['CPU-worker'], output, supervision)

    def test_outer_supervisor_observes_early_failure_cleanup_without_any_ready_result(self):
        output, supervision = self.root / 'worker', self.root / 'supervision'

        def supervise(command, directory, **kwargs):
            Path(directory).mkdir()
            output.mkdir()
            write_private_json(output / 'cleanup-start.json', dict(
                schema='semloom.imlane.duckdb.q3.cleanup_started.v1',
                cleanup_started_monotonic_ns=105_000_000_000,
                cleanup_deadline_monotonic=115, clock='CLOCK_MONOTONIC'))
            self.now = 112
            kwargs['monitor'](())
            self.now = 113
            kwargs['monitor'](())

        with patch('src.experiments.native_imlane_model_query.QueryWindow', return_value=self.window), \
                patch('src.experiments.lotus_records_supervisor.supervise', side_effect=supervise):
            with self.assertRaises(TimeoutError):
                supervised_run(['CPU-worker'], output, supervision, query_seconds=20,
                    cleanup_seconds=10, stop_grace_seconds=2)
        value = read_json(supervision / 'native-deadline.json')
        self.assertIsNone(value['result_ready_monotonic_ns'])
        self.assertEqual(value['cleanup_deadline_monotonic'], 115)

    def test_failure_marker_publication_error_does_not_skip_or_restart_cleanup(self):
        self.window.cleanup_path = self.root / 'cleanup-start.json'
        self.now = 105
        failure = OSError('CPU marker unavailable')
        with patch('src.experiments.native_imlane_model_query.publish_private_json', side_effect=failure) as write:
            self.assertEqual(self.window.begin_cleanup(), 115)
            self.now = 109
            self.assertEqual(self.window.begin_cleanup(), 115)
        write.assert_called_once()
        self.assertIs(self.window.cleanup_publication_error, failure)

    def test_later_ready_cannot_restart_an_existing_failure_cleanup_allowance(self):
        self.now = 105
        self.assertEqual(self.window.begin_cleanup(), 115)
        self.now = 112
        self.window.accept_ready(110_000_000_000, clock='CLOCK_MONOTONIC')
        self.assertEqual(self.window.cleanup_deadline, 115)

    def test_fractional_clock_failure_marker_round_trips_without_adding_time_tolerance(self):
        for position in range(12):
            with self.subTest(position=position):
                clock = [5_000_000.0]
                path = self.root / ('cleanup-' + str(position) + '.json')
                child = QueryWindow(20, 10, 2, clock=lambda: clock[0], cleanup_path=path)
                parent = QueryWindow(20, 10, 2, clock=lambda: clock[0])
                clock[0] += .01010101 * (position + 1)
                expected = child.begin_cleanup()
                parent.accept_cleanup(read_json(path))
                self.assertEqual(parent.cleanup_deadline, expected)


class NativeIMLaneModelAuditTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        self.csv = self.root / 'Reviews.csv'
        with self.csv.open('w', newline='') as stream:
            out = csv.DictWriter(stream, fieldnames=('id', 'reviewId', 'reviewText', 'scoreSentiment'))
            out.writeheader()
            out.writerows([
                dict(id='other', reviewId='same', reviewText='', scoreSentiment='NEGATIVE'),
                dict(id='taken_3', reviewId='same', reviewText='好\n"review"', scoreSentiment='POSITIVE'),
                dict(id='taken_3', reviewId='same', reviewText='bad', scoreSentiment='NEGATIVE')])
        self.input = prepare_input(self.csv, self.root / 'input', provider_mode='software_ipc')
        source = load_q3_source(self.csv)
        self.rows = source['rows'][1:]
        self.calls = self.root / 'model-calls'
        self.calls.mkdir()
        self.receipts = self.root / 'udf-receipts'
        self.receipts.mkdir()
        self.sampling = dict(temperature=0, top_p=1, max_tokens=8192, stop=None)
        write_private_json(self.root / 'model-provider.json', dict(provider_mode='software_ipc',
            model='CPU-model', sampling_params=self.sampling))
        replies = ['Answer: True', 'Answer: False']
        predictions = [dict(occurrence_id=row['occurrence_id'], id=row['id'], reviewId=row['reviewId'],
            messages_sha256=content_digest(q3_messages(row['reviewText'])), generated_text=text,
            decision=int(i == 0)) for i, (row, text) in enumerate(zip(self.rows, replies, strict=True))]
        write_private_json(self.receipts / 'q3-batch-cpu.json', dict(
            schema='semloom.imlane.duckdb.q3.batch.v1', provider_mode='software_ipc', status='succeeded',
            worker_pid=1729, logical_rows=2, sources=[{key: value for key, value in p.items()
                                     if key not in {'generated_text', 'decision'}} for p in predictions],
            predictions=predictions, provider_calls=[dict(logical_rows=2, status='succeeded', returned=True,
                started_monotonic_ns=100, finished_monotonic_ns=200)]))
        write_private_json(self.root / 'query.json', dict(status='query_returned', native_ai_query_returns=1,
            source_rows=3, eligible_rows=2, positive_review_cnt=1))
        requests = []
        for i, (row, reply) in enumerate(zip(self.rows, replies, strict=True)):
            payload = dict(self.sampling, model='CPU-model', n=1,
                           messages=q3_messages(row['reviewText']))
            requests.append(dict(position=i, attempted=True, status='succeeded',
                messages_sha256=content_digest(payload['messages']), requested_payload=payload,
                request_payload_sha256=content_digest(payload), truncated=False, response_id='CPU-' + str(i),
                usage=dict(completion_tokens=2), response=dict(position=i, request_id='CPU-' + str(i),
                    attempted=True, status='succeeded', generated_text=reply, finish_reason='stop',
                    requested_max_tokens=8192, resolved_max_tokens=8192,
                    usage=dict(completion_tokens=2))))
        self.terminal = self.calls / 'model-batch-cpu.terminal.json'
        write_private_json(self.calls / 'model-batch-cpu.started.json', dict(status='unknown'))
        self.replace_terminal(dict(schema='semloom.imlane.duckdb.q3.model_batch.v2',
            provider_mode='software_ipc', status='succeeded', client_closed=True,
            ipc_attempted=True, engine_generate_invocations=2, calls=requests, worker_pid=1729,
            started_monotonic_ns=110, finished_monotonic_ns=190))

    def audit(self):
        with patch('src.baselines.text.sembench_movie.evaluate_original', return_value={'CPU': True}):
            return audit_model_result(self.root, 'CPU-checkout')

    def replace_terminal(self, value):
        request = dict(schema='semloom.imlane.duckdb.q3.model_ipc.v1', batch_id='cpu',
            model='CPU-model', sampling_params=dict(self.sampling, n=1),
            messages_batch=[c['requested_payload']['messages'] for c in value['calls']],
            deadline_monotonic=300)
        response = dict(schema=request['schema'], batch_id='cpu', model='CPU-model', status='succeeded',
                        calls=[c['response'] for c in value['calls']])
        value.update(batch_id='cpu', request=request, request_sha256=content_digest(request),
                     response=response, response_sha256=content_digest(response))
        self.terminal.unlink(missing_ok=True)
        write_private_json(self.terminal, value)

    def test_real_input_export_never_contains_software_gold_replies(self):
        self.assertFalse((self.root / 'input/replies.json').exists())
        with (self.root / 'input/input.csv').open(newline='') as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows, load_q3_source(self.csv)['rows'])
        self.assertNotIn('model_requests', self.input)

    def test_same_saved_ipc_and_udf_responses_use_original_evaluator_without_model(self):
        result = self.audit()
        self.assertEqual(result['ipc']['batch_rpc_invocations'], 1)
        self.assertEqual(result['quality']['classification']['true_positive'], 1)
        self.assertEqual(result['quality']['classification']['true_negative'], 1)
        self.assertFalse(result['comparison_qualified'])
        self.assertIsNone(result['ipc']['physical_model_attempts'])

    def test_started_without_terminal_leaves_attempts_unknown(self):
        self.terminal.unlink()
        with self.assertRaisesRegex(ValueError, 'unknown'):
            self.audit()

    def test_close_unknown_wrong_generation_or_changed_response_is_not_complete(self):
        original = read_json(self.terminal)
        for change in ('close', 'generation', 'response'):
            value = deepcopy(original)
            if change == 'close':
                value['client_closed'] = None
            elif change == 'generation':
                value['calls'][0]['requested_payload']['max_tokens'] = 8
                value['calls'][0]['request_payload_sha256'] = content_digest(value['calls'][0]['requested_payload'])
            else:
                value['calls'][0]['response']['generated_text'] = 'Answer: False'
            self.replace_terminal(value)
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.audit()

    def test_length_text_is_evaluated_and_truncation_is_reported_separately(self):
        value = read_json(self.terminal)
        value['calls'][0]['truncated'] = True
        value['calls'][0]['response']['finish_reason'] = 'length'
        self.replace_terminal(value)
        result = self.audit()
        self.assertEqual(result['ipc']['truncated'], 1)
        self.assertEqual(result['native_result']['positive_review_cnt'], 1)

    def test_duplicate_messages_cannot_exchange_occurrence_responses_behind_same_count(self):
        with self.csv.open(newline='') as stream:
            values = list(csv.DictReader(stream))
        values[2]['reviewText'] = values[1]['reviewText']
        with self.csv.open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=values[0])
            writer.writeheader()
            writer.writerows(values)
        # Refresh the explicit input after changing the fixture, preserving all
        # occurrences. Both requests now have identical model messages.
        import shutil
        shutil.rmtree(self.root / 'input')
        prepare_input(self.csv, self.root / 'input', provider_mode='software_ipc')
        path = self.receipts / 'q3-batch-cpu.json'
        udf = read_json(path)
        udf['sources'][1]['messages_sha256'] = udf['sources'][0]['messages_sha256']
        udf['predictions'][1]['messages_sha256'] = udf['sources'][0]['messages_sha256']
        model = read_json(self.terminal)
        model['calls'][1]['messages_sha256'] = model['calls'][0]['messages_sha256']
        model['calls'][1]['requested_payload']['messages'] = deepcopy(model['calls'][0]['requested_payload']['messages'])
        model['calls'][1]['request_payload_sha256'] = content_digest(model['calls'][1]['requested_payload'])
        self.replace_terminal(model)
        path.unlink()
        write_private_json(path, udf)
        self.audit()
        udf['predictions'][0]['generated_text'], udf['predictions'][1]['generated_text'] = (
            udf['predictions'][1]['generated_text'], udf['predictions'][0]['generated_text'])
        path.unlink()
        write_private_json(path, udf)
        with self.assertRaisesRegex(ValueError, 'response position'):
            self.audit()

    def test_large_retained_software_replies_are_not_limited_as_small_config_files(self):
        text = 'Answer: True ' + 'x' * (9 * 1024**2)
        path = self.receipts / 'q3-batch-cpu.json'
        udf = read_json(path)
        udf['predictions'][0]['generated_text'] = text
        path.unlink()
        write_private_json(path, udf)
        model = read_json(self.terminal)
        model['calls'][0]['response']['generated_text'] = text
        self.replace_terminal(model)
        self.assertGreater(path.stat().st_size, 8 * 1024**2)
        self.assertGreater(self.terminal.stat().st_size, 8 * 1024**2)
        result = self.audit()
        self.assertEqual(result['native_result']['positive_review_cnt'], 1)

    def test_model_config_is_explicit_and_does_not_add_generation_defaults(self):
        value = dict(schema='semloom.imlane.duckdb.q3.model_run.v2', provider_mode='model',
            reviews_csv=str(self.csv), model_service={}, sampling_params=self.sampling,
            request_timeout_s=60)
        self.assertEqual(configuration(value), value)
        with self.assertRaises(ValueError):
            configuration(dict(value, provider_mode='software_fixture'))
        with self.assertRaises(ValueError):
            configuration(dict(value, legacy_concurrency=8))


class NativeIMLaneModelWorkerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.csv = self.root / 'Reviews.csv'
        self.csv.write_text('id,reviewId,reviewText,scoreSentiment\ntaken_3,1,good,POSITIVE\n')
        self.config = dict(schema='semloom.imlane.duckdb.q3.model_run.v2', provider_mode='software_ipc',
            reviews_csv=str(self.csv), model_service=dict(model_python=sys.executable,
                model=str(self.root), served_model_name='CPU-model',
                dtype='bfloat16', engine_options={}),
            sampling_params=dict(temperature=0, top_p=1, max_tokens=8192, stop=None),
            request_timeout_s=60)
        write_private_json(self.root / 'config.json', self.config)
        self.args = SimpleNamespace(config=self.root / 'config.json', output=self.root / 'run',
            author_root=self.root / 'author', method_python=Path(sys.executable),
            driver=self.root / 'q3', sembench_checkout=self.root / 'checkout', kind='thread',
            threads=None, executor_count=None, query_seconds=1200, cleanup_seconds=120,
            stop_grace_seconds=8)
        self.events = []
        self.service = MagicMock()
        self.service.socket_path = Path('/tmp/software-model/rpc.sock')
        self.service.served_model_name = 'CPU-model'
        self.service.ready_info = dict(max_model_len=32768)
        self.service.report = dict(status='configured', software_fixture=True)
        self.service.__enter__.side_effect = lambda: self.events.append('model-start') or self.service
        self.service.close.side_effect = lambda **unused: self.events.append('model-close')

        def leave(kind, error, traceback):
            self.service.close()
            return False

        self.service.__exit__.side_effect = leave
        self.factory = Mock(return_value=self.service)
        self.scope = SimpleNamespace(env={}, children=[], track_root=Mock(), check=Mock())

        def scope_factory(args, report, **unused):
            self.scope.env = args.workload_environment
            manager = MagicMock()
            manager.__enter__.side_effect = lambda: self.events.append('author-start') or self.scope
            manager.__exit__.side_effect = lambda *a: self.events.append('author-close') or False
            return manager

        self.process = Mock(poll=Mock(return_value=0))
        self.writer = Mock()

        def spawn(command, environment, log):
            self.events.append('native-query')
            write_private_json(self.args.output / 'query.json', dict(status='query_returned',
                result_ready_monotonic_ns=time.monotonic_ns(), result_clock='CLOCK_MONOTONIC',
                positive_review_cnt=1, query_clock_version='imlane.q3.source_to_sink.v1'))
            self.assertEqual(environment['SEMLOOM_IMLANE_Q3_MODEL_CONFIG'],
                             str(self.args.output / 'model-provider.json'))
            self.assertEqual(command[command.index('--kind') + 1], 'thread')
            return self.process, self.writer

        self.audit = Mock(return_value=dict(native_result=dict(positive_review_cnt=1),
            predictions=[{}], ipc=dict(batch_rpc_invocations=1), quality=dict(CPU=True)))
        for guard in (
            patch('integrations.imlane_duckdb.runtime.DuckDBScope', side_effect=scope_factory),
            patch('integrations.imlane_records.owner.spawn', side_effect=spawn),
            patch('integrations.imlane_records.owner.resource_state', return_value={'resources_confirmed': True}),
            patch('src.experiments.native_imlane_model_query.audit_model_result', self.audit),
            patch('subprocess.Popen', side_effect=AssertionError('real process forbidden')),
        ):
            guard.start()
            self.addCleanup(guard.stop)

    def test_model_owner_and_author_owner_close_after_native_result_without_repeating_graph(self):
        with patch.dict(os.environ, {'VLLM_API_KEY': 'CPU-inherited-key'}):
            report = run_worker(self.args, service_factory=self.factory)
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(self.events[:5], ['model-start', 'author-start', 'native-query',
                                         'author-close', 'model-close'])
        self.audit.assert_called_once()
        self.assertNotIn('api_key_env', read_json(self.args.output / 'model-provider.json'))
        self.assertTrue((self.args.output / 'result-ready.json').exists())
        self.assertEqual(report['model_requests'], 0)
        self.assertIsNone(report['physical_gpu_exit_confirmed'])
        provider = read_json(self.args.output / 'model-provider.json')
        self.assertEqual(provider['sampling_params']['max_tokens'], 8192)
        self.assertIsNone(provider['sampling_params']['stop'])
        self.assertTrue(self.service.begin_cleanup.called)

    def test_ready_query_clock_contains_fresh_source_and_stops_before_audit_and_close(self):
        source_started = []

        def prepare(*args, **kwargs):
            self.events.append('source-read')
            source_started.append(time.monotonic_ns())
            return materialize_input(*args, **kwargs)

        with patch('src.experiments.native_imlane_model_query.materialize_input', side_effect=prepare):
            report = run_worker(self.args, service_factory=self.factory)
        self.assertEqual(self.events[:4], ['model-start', 'author-start', 'source-read', 'native-query'])
        self.assertEqual(report['query_clock_version'], 'imlane.q3.source_to_sink.v2')
        self.assertEqual(report['native_query_clock_version'], 'imlane.q3.source_to_sink.v1')
        ready = read_json(self.args.output / 'query.json')['result_ready_monotonic_ns']
        self.assertLessEqual(report['source_started_ns'], source_started[0])
        self.assertLessEqual(source_started[0], ready)
        self.assertLessEqual(ready, report['owned_cleanup_completed_monotonic_ns'])
        self.assertLessEqual(ready, report['audit_started_monotonic_ns'])
        self.assertLessEqual(report['owned_cleanup_completed_monotonic_ns'], report['input_audit_started_monotonic_ns'])
        self.assertLessEqual(report['input_audit_ended_monotonic_ns'], report['audit_started_monotonic_ns'])
        self.assertEqual(report['ready_source_to_sink_seconds'],
                         (ready - report['source_started_ns']) / 1e9)
        self.assertGreaterEqual(report['cold_worker_entry_to_sink_seconds'],
                                report['ready_source_to_sink_seconds'])

    def test_old_driver_does_not_acquire_new_clock_version(self):
        import src.experiments.native_imlane_model_query as worker
        original = worker.read_json

        def read(path, **kwargs):
            value = original(path, **kwargs)
            if Path(path).name == 'query.json':
                value.pop('query_clock_version', None)
            return value

        with patch.object(worker, 'read_json', side_effect=read):
            report = run_worker(self.args, service_factory=self.factory)
        self.assertIsNone(report['query_clock_version'])
        self.assertIsNone(report['native_query_clock_version'])
        self.assertIn('ready_source_to_sink_seconds', report)

    def test_source_clock_keeps_real_materialization_and_excludes_deferred_identity_cost(self):
        import src.experiments.native_imlane_model_query as worker
        import src.experiments.native_imlane_query as packet
        from integrations.imlane_records import owner

        now = [1000.0]
        window = QueryWindow(1200, 120, 8, clock=lambda: now[0], cleanup_path=self.args.output/'cleanup-start.json')
        read, export, describe, identity, write = (packet.read_q3_source, packet._export_source,
            packet.describe_q3_source, packet.file_identity, packet.write_private_json)
        spawn = owner.spawn.side_effect
        self.service.__enter__.side_effect = lambda: now.__setitem__(0, now[0]+90) or self.service

        def cost(function, seconds):
            def wrapped(*args, **kwargs):
                result = function(*args, **kwargs)
                now[0] += seconds
                return result
            return wrapped

        def driver(*args, **kwargs):
            self.assertTrue((self.args.output/'input/input.csv').exists())
            self.assertFalse((self.args.output/'input/packet.json').exists())
            now[0] += 5
            return spawn(*args, **kwargs)

        with patch.object(worker, 'QueryWindow', return_value=window), \
                patch.object(worker.time, 'monotonic', side_effect=lambda: now[0]), \
                patch.object(worker.time, 'monotonic_ns', side_effect=lambda: int(now[0]*1e9)), \
                patch.object(packet, 'read_q3_source', side_effect=cost(read, 2)), \
                patch.object(packet, '_export_source', side_effect=cost(export, 3)), \
                patch.object(packet, 'describe_q3_source', side_effect=cost(describe, 7)), \
                patch.object(packet, 'file_identity', side_effect=cost(identity, 11)), \
                patch.object(packet, 'write_private_json', side_effect=cost(write, 13)), \
                patch.object(owner, 'spawn', side_effect=driver):
            report = run_worker(self.args, service_factory=self.factory)
        self.assertEqual(report['query_clock_version'], 'imlane.q3.source_to_sink.v2')
        self.assertEqual(report['native_query_clock_version'], 'imlane.q3.source_to_sink.v1')
        self.assertEqual(report['ready_source_to_sink_seconds'], 10)
        self.assertEqual(report['cold_worker_entry_to_sink_seconds'], 100)
        self.assertEqual(report['input_audit_ended_monotonic_ns']-report['input_audit_started_monotonic_ns'], 31_000_000_000)
        self.assertLessEqual(report['owned_cleanup_completed_monotonic_ns'], report['input_audit_started_monotonic_ns'])
        self.assertLessEqual(report['input_audit_ended_monotonic_ns'], report['audit_started_monotonic_ns'])

    def test_post_result_packet_failure_is_not_retried_or_reported_as_a_complete_query(self):
        failure = OSError('CPU packet publication failed')
        with patch('src.experiments.native_imlane_model_query.publish_input_packet', side_effect=failure) as publish:
            with self.assertRaises(OSError) as caught:
                run_worker(self.args, service_factory=self.factory)
        self.assertIs(caught.exception, failure)
        self.assertEqual(publish.call_count, 1)
        self.audit.assert_not_called()
        self.assertIn('model-close', self.events)
        self.assertTrue((self.args.output/'result-ready.json').exists())
        report = read_json(self.args.output/'worker.json')
        self.assertEqual(report['status'], 'failed')
        self.assertIn('input_audit_ended_monotonic_ns', report)

    def test_native_failure_keeps_packet_evidence_after_cleanup(self):
        failure = RuntimeError('CPU native query failed before COUNT')
        with patch('integrations.imlane_records.owner.spawn', side_effect=failure):
            with self.assertRaises(RuntimeError) as caught:
                run_worker(self.args, service_factory=self.factory)
        self.assertIs(caught.exception, failure)
        self.audit.assert_not_called()
        self.assertIn('model-close', self.events)
        self.assertTrue((self.args.output/'input/packet.json').exists())
        self.assertFalse((self.args.output/'result-ready.json').exists())
        self.assertEqual(read_json(self.args.output/'worker.json')['error_type'], 'RuntimeError')

    def test_packet_failure_after_native_failure_keeps_the_native_error_and_one_attempt(self):
        failure = RuntimeError('CPU native query failed before COUNT')
        with patch('integrations.imlane_records.owner.spawn', side_effect=failure), \
                patch('src.experiments.native_imlane_model_query.publish_input_packet',
                      side_effect=OSError('CPU packet failure')) as publish:
            with self.assertRaises(RuntimeError) as caught:
                run_worker(self.args, service_factory=self.factory)
        self.assertIs(caught.exception, failure)
        self.assertEqual(publish.call_count, 1)
        self.assertIn('input audit publication also failed: OSError', failure.__notes__)
        self.assertEqual(read_json(self.args.output/'worker.json')['error_type'], 'RuntimeError')

    def test_public_software_mode_cannot_accidentally_start_a_real_model(self):
        with patch('integrations.imlane_duckdb.model_service.ModelService') as real:
            with self.assertRaisesRegex(ValueError, 'no-model test service factory'):
                run_worker(self.args)
        real.assert_not_called()
        self.assertEqual(read_json(self.args.output / 'worker.json')['status'], 'failed')

    def test_bad_generation_is_rejected_before_service_enters(self):
        self.config['sampling_params']['max_tokens'] = 0
        (self.root / 'config.json').unlink()
        write_private_json(self.root / 'config.json', self.config)
        with self.assertRaises(ValueError):
            run_worker(self.args, service_factory=self.factory)
        self.service.__enter__.assert_not_called()
        self.audit.assert_not_called()
        self.assertEqual(read_json(self.args.output / 'worker.json')['status'], 'failed')

    def test_retired_http_configuration_cannot_launch_a_service(self):
        self.config['api_key_env'] = None
        (self.root / 'config.json').unlink()
        write_private_json(self.root / 'config.json', self.config)
        with self.assertRaises(ValueError):
            run_worker(self.args, service_factory=self.factory)
        self.factory.assert_not_called()

    def test_primary_query_error_survives_service_cleanup_error(self):
        failure = RuntimeError('CPU original native failure')
        self.scope.check.side_effect = [None, failure]
        self.service.__exit__.side_effect = lambda *a: False
        self.service.close.side_effect = RuntimeError('CPU close failed')
        with self.assertRaises(RuntimeError) as caught:
            run_worker(self.args, service_factory=self.factory)
        self.assertIs(caught.exception, failure)
        self.assertIn('model service cleanup also failed', failure.__notes__[0])
        self.assertEqual(read_json(self.args.output / 'worker.json')['status'], 'failed')
        self.assertTrue((self.args.output / 'query.json').exists())

    def test_author_startup_exhausting_query_deadline_cannot_launch_native_work(self):
        clock = [time.monotonic()]
        window = QueryWindow(1200, 120, 8, clock=lambda: clock[0])
        manager = MagicMock()

        def ready_after_deadline():
            clock[0] = window.query_deadline + 1
            return self.scope

        manager.__enter__.side_effect = ready_after_deadline
        manager.__exit__.return_value = False
        with patch('src.experiments.native_imlane_model_query.QueryWindow', return_value=window), \
                patch('integrations.imlane_duckdb.runtime.DuckDBScope', return_value=manager):
            with self.assertRaises(TimeoutError):
                run_worker(self.args, service_factory=self.factory)
        self.assertNotIn('native-query', self.events)
        self.assertFalse((self.args.output / 'result-ready.json').exists())

    def test_failed_model_batch_stops_waiting_for_an_author_scalar_and_retains_failure(self):
        def failed_batch(command, environment, log):
            write_private_json(self.args.output / 'model-calls/model-batch-error.terminal.json',
                dict(status='failed', client_closed=True, error_type='ModelBatchError'))
            return self.process, self.writer

        with patch('integrations.imlane_records.owner.spawn', side_effect=failed_batch):
            with self.assertRaisesRegex(RuntimeError, 'model-provider batch failed'):
                run_worker(self.args, service_factory=self.factory)
        self.process.poll.assert_not_called()
        report = read_json(self.args.output / 'worker.json')
        self.assertEqual(report['model_batch_failure']['error_type'], 'ModelBatchError')
        self.assertEqual(report['status'], 'failed')
        self.assertFalse((self.args.output / 'result-ready.json').exists())

    def test_cleanup_failure_after_query_preserves_ready_result_and_fails_worker(self):
        self.service.__exit__.side_effect = lambda *a: False
        self.service.close.side_effect = RuntimeError('CPU final close failed')
        with self.assertRaisesRegex(RuntimeError, 'CPU final close failed'):
            run_worker(self.args, service_factory=self.factory)
        report = read_json(self.args.output / 'worker.json')
        self.assertEqual(report['status'], 'failed')
        self.assertTrue((self.args.output / 'result-ready.json').exists())

    def test_startup_failure_preserves_primary_even_if_early_cleanup_marker_cannot_publish(self):
        failure = RuntimeError('CPU startup failure')
        self.service.__enter__.side_effect = failure
        with patch('src.experiments.native_imlane_model_query.publish_private_json',
                   side_effect=OSError('CPU marker unavailable')):
            with self.assertRaises(RuntimeError) as caught:
                run_worker(self.args, service_factory=self.factory)
        self.assertIs(caught.exception, failure)
        self.service.close.assert_called_once()
        self.assertIn('failure cleanup marker publication also failed', failure.__notes__[0])
        self.assertEqual(read_json(self.args.output / 'worker.json')['deadline']['cleanup_marker_error_type'], 'OSError')


if __name__ == '__main__':
    unittest.main()
