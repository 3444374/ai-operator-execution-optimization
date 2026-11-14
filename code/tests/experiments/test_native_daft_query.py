"""Native worker artifacts and absolute post-result deadlines, no model."""

from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from src.experiments.native_daft_query import QueryDeadline, run_worker, supervised_run
from src.infrastructure.private_artifacts import publish_private_json, read_json, write_private_json


class NativeDaftWorkerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        from src.infrastructure.private_artifacts import file_sha256
        self.requests=[dict(call_id=str(i),prompt='complete original prompt') for i in range(2)]
        packet=self.root/'prompts.json'
        write_private_json(packet,dict(schema='semloom.native_daft_prompts.v1',requests=self.requests))
        budget=dict(path=str(self.root/'budget.sqlite'),budget_id='CPU-worker',limit=2,unit_id='query')
        ledger=CellBudgetLedger.create(Path(budget['path']),AttemptBudget(budget['budget_id'],2),deadline_utc=None)
        ledger.reserve_unit('query',2);ledger.claim_shared_unit('query')
        self.config = dict(schema='semloom.native_daft_map.v1', requests_path=str(packet),
            requests_sha256=file_sha256(packet),request_budget=budget,model='CPU-only',
            sampling_params=dict(temperature=0, max_tokens=8192, stop=[]), engine_kwargs={},
            provider_options={}, query_timeout_s=1200, cleanup_s=120, stop_grace_s=8)

    def ready(self, path, second):
        publish_private_json(path, dict(schema='semloom.native_daft_map.result_ready.v1',
                                       result_ready_monotonic_ns=int(second * 1e9)))

    def test_retired_q3_configuration_is_rejected_before_model_or_output_allocation(self):
        from src.experiments.native_daft_query import configuration
        legacy=dict(self.config,schema='semloom.native_daft_q3.v1')
        with self.assertRaisesRegex(ValueError,'original query task layer'):configuration(legacy)

    def test_cleanup_starts_at_actual_result_ready_including_forced_stop(self):
        now = [0.]
        path = self.root / 'result-ready.json'
        sampled = Mock()
        deadline = QueryDeadline(path, query_s=1200, cleanup_s=120, stop_grace_s=8,
                                 monitor=sampled, clock=lambda: now[0])
        self.ready(path, 60)
        now[0] = 100  # The observer may notice READY later than it was written.
        deadline(('owned fixture identity',))
        self.assertEqual(deadline.report()['cleanup_deadline_monotonic'], 180)
        now[0] = 171.999
        deadline(())
        now[0] = 172
        with self.assertRaisesRegex(TimeoutError, 'post-result close'):
            deadline(())
        self.assertEqual(deadline.report()['forced_stop_reserved_s'], 8)
        self.assertEqual(sampled.call_count, 3)

    def test_query_deadline_is_not_reduced_by_cleanup_reservation(self):
        now = [0.]
        deadline = QueryDeadline(self.root / 'not-ready.json', query_s=1200,
            cleanup_s=120, stop_grace_s=8, clock=lambda: now[0])
        now[0] = 1199.9
        deadline(())
        now[0] = 1200
        with self.assertRaisesRegex(TimeoutError, 'complete query'):
            deadline(())

    def test_native_readiness_callback_runs_once_during_monitoring_before_close_wait(self):
        path = self.root / 'native-ready.json'
        events = []
        now = [100]
        deadline = QueryDeadline(path, query_s=1200, cleanup_s=120, stop_grace_s=8,
            clock=lambda: now[0], on_result_ready=lambda raw: events.append(raw['result_ready_monotonic_ns']))
        self.ready(path, 101)
        now[0] = 102
        deadline(())
        deadline(())
        self.assertEqual(events, [101000000000])

    def test_late_native_result_is_not_delivered_to_semantic_sink(self):
        path = self.root / 'late-callback.json'
        callback = Mock()
        deadline = QueryDeadline(path, query_s=1, cleanup_s=120, stop_grace_s=8,
            clock=lambda: 0, on_result_ready=callback)
        self.ready(path, 2)
        with self.assertRaises(TimeoutError):
            deadline(())
        callback.assert_not_called()

    def test_invalid_prepared_packet_stops_before_native_model_entry(self):
        from src.infrastructure.private_artifacts import file_sha256
        packet = self.root / 'invalid-prompts.json'
        write_private_json(packet, dict(schema='semloom.native_daft_prompts.v1', requests=[
            dict(call_id='duplicate', prompt='first'), dict(call_id='duplicate', prompt='second')]))
        config = dict(self.config)
        config.update(schema='semloom.native_daft_map.v1', requests_path=str(packet),
            requests_sha256=file_sha256(packet), request_budget=dict(path='unused',
                budget_id='CPU', limit=2, unit_id='CPU'))
        execute = Mock()
        with self.assertRaisesRegex(ValueError, 'unique occurrence'):
            run_worker(config, self.root / 'duplicate', execute_query=execute)
        config['requests_sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'digest changed'):
            run_worker(config, self.root / 'changed', execute_query=execute)
        execute.assert_not_called()

    def test_result_published_after_query_deadline_does_not_bypass_it(self):
        now = [0.]
        path = self.root / 'late-ready.json'
        deadline = QueryDeadline(path, query_s=1200, cleanup_s=120, stop_grace_s=8,
                                 clock=lambda: now[0])
        self.ready(path, 1201)
        now[0] = 1201
        with self.assertRaisesRegex(TimeoutError, 'result exceeded'):
            deadline(())

    def execute(self, rows, **options):
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows,self.requests)
        self.assertEqual(options['sampling_params'], self.config['sampling_params'])
        self.assertEqual(options['close_timeout_s'], 112)
        self.assertNotIn('on_native_event', options)
        for row in rows:options['before_generate'](row['call_id'],'CPU-'+row['call_id'],row['prompt'])
        result = dict(predictions=[dict(call_id=row['call_id'],generated_text='True') for row in rows],
            metadata=dict(result_ready_monotonic_ns=time.monotonic_ns(),model_close_receipt=None,native_api={}))
        options['on_result_ready'](result)
        self.assertTrue((self.directory / 'result-ready.json').is_file())
        self.assertFalse((self.directory / 'worker.json').exists())
        return result

    def test_worker_publishes_complete_predictions_before_close_and_separate_terminal(self):
        self.directory = self.root / 'normal'
        def execute(rows, **options):
            result = self.execute(rows, **options)
            result['metadata']['model_close_receipt'] = dict(status='closed', captured_models=1)
            return result
        report = run_worker(self.config, self.directory, execute_query=execute)
        ready = read_json(self.directory / 'result-ready.json')
        self.assertEqual(len(ready['result']['predictions']),2)
        self.assertIsNone(ready['result']['metadata']['model_close_receipt'])
        self.assertEqual(report['model_close_receipt']['status'], 'closed')
        self.assertIsNone(report['physical_gpu_exit_confirmed'])
        self.assertNotIn('predictions', report)  # One saved prediction artifact.

    def test_close_failure_keeps_ready_predictions_and_failed_terminal(self):
        self.directory = self.root / 'failed'
        failure = RuntimeError('CPU close failed after a complete result')
        failure.model_close_report = dict(status='unknown')
        def execute(rows, **options):
            self.execute(rows, **options)
            raise failure
        with self.assertRaises(RuntimeError) as caught:
            run_worker(self.config, self.directory, execute_query=execute)
        self.assertIs(caught.exception, failure)
        self.assertEqual(len(read_json(self.directory / 'result-ready.json')['result']['predictions']),2)
        terminal = read_json(self.directory / 'worker.json')
        self.assertEqual(terminal['status'], 'failed')
        self.assertEqual(terminal['model_close_receipt']['status'], 'unknown')

    def test_supervisor_total_and_grace_share_one_cleanup_allowance(self):
        config = self.root / 'config.json'
        write_private_json(config, self.config)
        with patch('src.experiments.lotus_records_supervisor.supervise', return_value={'status': 'closed'}) as supervise:
            result = supervised_run(config, self.root / 'supervised', model_python='CPU-python')
        self.assertEqual(result['status'], 'closed')
        options = supervise.call_args.kwargs
        self.assertEqual(options['timeout_s'], 1320)
        self.assertEqual(options['grace_s'], 8)
        self.assertEqual(options['natural_exit_grace_s'], 4)
        self.assertIsInstance(options['monitor'], QueryDeadline)
        command = supervise.call_args.args[0]
        self.assertEqual(command[:5], ['CPU-python', '-u', '-B', '-m', 'src.experiments.native_daft_query'])
        self.assertTrue((self.root / 'supervised/deadline.json').exists())

    def test_failed_collection_keeps_partial_predictions_without_publishing_ready(self):
        self.directory = self.root / 'failed-count'
        failure = RuntimeError('CPU native collection failed after partial output')
        failure.native_query_report = dict(predictions=[dict(generated_text='True')],
            metadata=dict(model_close_receipt={'status': 'closed'}))
        def execute(rows, **options):
            raise failure
        with self.assertRaises(RuntimeError) as caught:
            run_worker(self.config, self.directory, execute_query=execute)
        self.assertIs(caught.exception, failure)
        self.assertFalse((self.directory / 'result-ready.json').exists())
        saved = read_json(self.directory / 'failed-result.json')
        self.assertEqual(saved['schema'], 'semloom.native_daft_map.failed_result.v1')
        self.assertEqual(saved['result']['predictions'], [{'generated_text': 'True'}])
        terminal = read_json(self.directory / 'worker.json')
        self.assertEqual(terminal['status'], 'failed')
        self.assertEqual(terminal['result_artifact'], 'failed-result.json')
        self.assertEqual(terminal['model_close_receipt'], {'status': 'closed'})
        self.assertFalse(terminal['result_ready_recorded'])

    def test_artifact_write_failure_preserves_primary_error_and_supervision_report(self):
        config = self.root / 'config.json'
        write_private_json(config, self.config)
        for operation in ('worker', 'supervisor'):
            with self.subTest(operation=operation):
                failure = RuntimeError('CPU original ' + operation + ' failure')
                failure.report = {'cleanup': 'unconfirmed CPU fixture'}
                with patch('src.experiments.native_daft_query.write_private_json', side_effect=OSError('CPU disk full')):
                    with self.assertRaises(RuntimeError) as caught:
                        if operation == 'worker':
                            run_worker(self.config, self.root / operation,
                                       execute_query=Mock(side_effect=failure))
                        else:
                            with patch('src.experiments.lotus_records_supervisor.supervise', side_effect=failure):
                                supervised_run(config, self.root / operation, model_python='CPU-python')
                self.assertIs(caught.exception, failure)
                self.assertEqual(caught.exception.report, {'cleanup': 'unconfirmed CPU fixture'})
                self.assertTrue(any('write failed' in note for note in failure.__notes__))

    def test_caller_handled_exception_does_not_hide_a_new_artifact_failure(self):
        config = self.root / 'config.json'
        write_private_json(config, self.config)
        for operation in ('worker', 'supervisor'):
            with self.subTest(operation=operation):
                try:
                    raise RuntimeError('unrelated caller exception')
                except RuntimeError:
                    with patch('src.experiments.native_daft_query.write_private_json', side_effect=OSError('CPU new write failure')):
                        with self.assertRaisesRegex(OSError, 'new write failure'):
                            if operation == 'worker':
                                execute = Mock(return_value={'metadata': {'model_close_receipt': {'status': 'closed'},'native_api':{}}})
                                run_worker(self.config, self.root / operation, execute_query=execute)
                            else:
                                with patch('src.experiments.lotus_records_supervisor.supervise', return_value={'status': 'closed'}):
                                    supervised_run(config, self.root / operation, model_python='CPU-python')


if __name__ == '__main__':
    unittest.main()
