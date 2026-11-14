"""Two original queries share real Arrow/provider accounting and a software owner."""
import time
import unittest
from unittest.mock import patch

from src.experiments import native_imlane_sembench as entry
from src.infrastructure.private_artifacts import read_json
from tests.experiments import test_native_imlane_aggregate as carrier


@unittest.skipUnless(carrier.fixture.CHECKOUT and carrier.fixture.HAS_SDK, 'requires pinned SemBench/LOTUS SDK')
class ModelSessionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        carrier.NativeAggregateTests.setUpClass()

    def setUp(self):
        self.native = carrier.NativeAggregateTests()
        self.native.setUp()
        self.addCleanup(self.native.doCleanups)

    def config(self, *, multi=False):
        _, config, _ = self.native.config(multi=multi)
        return dict(config, schema=entry.QUERY_SCHEMA, lifecycle='model-session')

    def test_two_fresh_full_queries_keep_one_owner_sql_lifecycle_and_continuous_attempts(self):
        import pandas as pd
        config = self.config(multi=True)
        reads = []
        read_csv = pd.read_csv
        def source(*args, **kwargs):
            reads.append(time.monotonic_ns())
            return read_csv(*args, **kwargs)
        with patch.object(pd, 'read_csv', side_effect=source):
            report = self.native.invoke(config)
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(len(self.native.last.services), 1)
        self.assertTrue(report['warm_qualified'])
        self.assertTrue(report['model_owner_reused'])
        self.assertEqual(len(report['model_owner_observations']), 5)
        first, second = report['queries']
        self.assertEqual([first['phase'], second['phase']], ['ready-first', 'same-owner-repeat'])
        self.assertGreater(first['model_calls'], 1)
        self.assertEqual(report['budget']['consumed_requests'], first['model_calls']+second['model_calls'])
        calls = [call for value in (first,second) for call in value['ipc']['calls']]
        self.assertEqual(sorted(call['response']['budget_attempt'] for call in calls), list(range(1,len(calls)+1)))
        self.assertEqual(len({call['call_id'] for call in calls}),len(calls))
        for value in (first,second):
            self.assertTrue(any(value['query_started_monotonic_ns'] <= stamp <= value['result_ready_monotonic_ns'] for stamp in reads))
            self.assertEqual(value['source_to_sink_seconds'],(value['result_ready_monotonic_ns']-value['query_started_monotonic_ns'])/1e9)
            self.assertEqual(value['deadline']['query_deadline_monotonic'],report['deadline']['query_deadline_monotonic'])
            self.assertIn('quality',value)
            self.assertTrue((self.native.last.output/('round-'+str(value['round']))/'result-ready.json').exists())
            self.assertTrue(all(batch['driver_exit_observed_monotonic_ns']<=value['result_ready_monotonic_ns'] for batch in value['prediction_batches']))
        self.assertLess(first['result_ready_monotonic_ns'],second['query_started_monotonic_ns'])
        self.assertEqual(read_json(self.native.last.output/'result-ready.json')['result_ready_monotonic_ns'],second['result_ready_monotonic_ns'])
        events = self.native.last.events
        self.assertEqual(sum(name=='model-close' for name,_ in events),1)
        self.assertTrue(all(stamp>=second['result_ready_monotonic_ns'] for name,stamp in events if name in ('model-close','model-cleanup')))
        self.assertIsNone(report['ready_source_to_sink_seconds'])

    def test_changed_owner_stops_before_another_query_and_retains_first_complete_sink(self):
        with self.assertRaisesRegex(RuntimeError, 'model owner changed'):
            self.native.invoke(self.config(),owner_change_at=3)
        report = read_json(self.native.last.output/'worker.json')
        self.assertEqual(report['status'],'failed')
        self.assertFalse(report['model_owner_reused'])
        self.assertFalse(report['warm_qualified'])
        self.assertEqual(len(report['queries']),1)
        self.assertIsNotNone(report['queries'][0]['result'])
        self.assertFalse((self.native.last.output/'round-2').exists())
        self.assertFalse((self.native.last.output/'result-ready.json').exists())
        self.assertEqual(sum(name=='model-close' for name,_ in self.native.last.events),1)

    def test_first_post_sink_error_preserves_result_and_primary_during_close_failure(self):
        with self.assertRaisesRegex(ValueError,'saved receipt audit failed') as caught:
            self.native.invoke(self.config(),audit_failure=True,close_failure=True)
        self.assertTrue(any('close failed' in note for note in caught.exception.__notes__))
        report = read_json(self.native.last.output/'worker.json')
        self.assertEqual(len(report['queries']),1)
        self.assertIsNotNone(report['queries'][0]['result'])
        self.assertFalse(report['warm_qualified'])
        self.assertFalse((self.native.last.output/'round-2').exists())
        self.assertTrue((self.native.last.output/'full-sink-observed.json').exists())

    def test_second_query_cannot_reset_the_budget_after_a_complete_first_query(self):
        config=self.config(multi=True)
        config['max_model_calls']=entry.check(config)['source']['minimum_model_calls']*2
        prepare=entry.prepare_query
        def next_query(value, **kwargs):
            if kwargs.get('execution_id', '').endswith('-round-2'):
                self.native.last.replies.mode='long_summaries'
            return prepare(value, **kwargs)
        with patch.object(entry,'prepare_query',side_effect=next_query),self.assertRaises(entry.BudgetExhausted):
            self.native.invoke(config)
        report=read_json(self.native.last.output/'worker.json')
        self.assertEqual([value['status'] for value in report['queries']],['completed','failed'])
        self.assertIsNotNone(report['queries'][0]['result'])
        self.assertFalse(report['warm_qualified'])
        self.assertLessEqual(report['budget']['consumed_requests'],config['max_model_calls'])
        self.assertFalse((self.native.last.output/'result-ready.json').exists())
        self.assertEqual(sum(name=='model-close' for name,_ in self.native.last.events),1)

    def test_model_close_failure_revokes_warm_qualification_but_keeps_both_sinks(self):
        with self.assertRaisesRegex(RuntimeError,'CPU model close failed'):
            self.native.invoke(self.config(),close_failure=True)
        report=read_json(self.native.last.output/'worker.json')
        self.assertEqual([value['status'] for value in report['queries']],['completed','completed'])
        self.assertTrue(report['model_owner_reused'])
        self.assertFalse(report['warm_qualified'])

    def test_persistence_failure_revokes_qualification_in_both_failure_reports(self):
        write=carrier.relation.write_private_json
        def fail_sink(path,value):
            if path.name=='full-sink-observed.json':
                raise OSError('CPU session sink persistence failed')
            return write(path,value)
        with patch.object(carrier.relation,'write_private_json',side_effect=fail_sink),self.assertRaisesRegex(OSError,'sink persistence failed'):
            self.native.invoke(self.config())
        for name in ('author.json','worker.json'):
            report=read_json(self.native.last.output/name)
            self.assertEqual(report['status'],'failed')
            self.assertTrue(report['model_owner_reused'])
            self.assertFalse(report['warm_qualified'])
            self.assertEqual(len(report['queries']),2)

    def test_failed_final_worker_publication_retains_results_without_warm_qualification(self):
        write=entry.write_private_json
        def fail_worker(path,value):
            if path.name=='worker.json':
                raise OSError('CPU final worker report failed')
            return write(path,value)
        with patch.object(entry,'write_private_json',side_effect=fail_worker),self.assertRaisesRegex(OSError,'final worker report failed') as caught:
            self.native.invoke(self.config())
        report=caught.exception.query_report
        self.assertEqual(report['status'],'failed')
        self.assertFalse(report['warm_qualified'])
        self.assertTrue(report['model_owner_reused'])
        self.assertEqual([value['status'] for value in report['queries']],['completed','completed'])
        self.assertIsNotNone(caught.exception.query_result)
        self.assertFalse((self.native.last.output/'worker.json').exists())

    def test_second_prepare_exhaustion_is_timeout_and_preserves_first_sink(self):
        config=self.config()
        prepare,window,real=entry.prepare_query,entry._ResultWindow,time.monotonic
        advance=[0]
        def now():return real()+advance[0]
        def timed_window(*args,**kwargs):return window(*args,clock=now,**kwargs)
        def source(value, **kwargs):
            query=prepare(value, **kwargs)
            if kwargs.get('execution_id', '').endswith('-round-2'):
                advance[0]=config['query_timeout_s']+1
            return query
        with patch.object(entry.time,'monotonic',side_effect=now),patch.object(entry,'_ResultWindow',side_effect=timed_window), \
                patch.object(entry,'prepare_query',side_effect=source),self.assertRaises(TimeoutError):
            self.native.invoke(config)
        report=read_json(self.native.last.output/'worker.json')
        self.assertEqual(len(report['queries']),1)
        self.assertIsNotNone(report['queries'][0]['result'])
        self.assertFalse((self.native.last.output/'round-2').exists())
        self.assertEqual(sum(name=='model-close' for name,_ in self.native.last.events),1)

    def test_round_occurrences_do_not_change_the_valid_permanent_budget_id(self):
        config=dict(self.config(),query_id='q'*128)
        report=self.native.invoke(config)
        self.assertTrue(report['warm_qualified'])
        self.assertEqual(report['budget']['budget_id'],config['query_id'])
        for index,value in enumerate(report['queries'],1):
            self.assertEqual(value['source']['query_id'],config['query_id']+'-round-'+str(index))

    def test_total_allowance_covers_both_queries_and_lifecycle_is_explicit(self):
        config = self.config()
        checked = entry.check(config)
        self.assertEqual(checked['query_executions'],2)
        self.assertEqual(checked['model_calls'],0)
        with self.assertRaisesRegex(ValueError,'minimum for every declared execution'):
            entry.prepare_query(dict(config,max_model_calls=1))
        for change in (dict(lifecycle='warm'),dict(schema=entry.TEXT_SCHEMA)):
            with self.subTest(change=change),self.assertRaisesRegex(ValueError,'complete original-query'):
                entry.configuration(dict(config,**change))
