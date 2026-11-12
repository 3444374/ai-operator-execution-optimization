import unittest
from unittest.mock import patch

from src.observability.native_query_timing import (ObservationStore,model_API_samples,
                             offline_scan_total,query_timing,timed_driver_type)


class Clock:
    def __init__(self,*values):self.values=iter(values)
    def __call__(self):
        value=next(self.values)
        if isinstance(value,Exception):raise value
        return value


class QueryTests(unittest.TestCase):
    def test_callback_result_and_policy_cleanup_preserved(self):
        result={'result':[1,2],'calls':{'counts':{'succeeded':2}}};events=[]
        class Base:
            def query(self,spec,emit):
                events.extend(['lookup','scan','model','cleanup','hint_save']);return result
        store=ObservationStore(Clock(100,900))
        value=timed_driver_type(Base,store)().query({'query_id':'q'},None)
        self.assertIs(value,result);self.assertEqual(events,['lookup','scan','model','cleanup','hint_save'])
        self.assertEqual(value['comparison_timing']['API_returned_ns'],900)

    def test_isolated_initialization_not_omitted(self):
        q=dict(query_start_ns=1_000_000_000,query_result_ready_ns=12_000_000_000,
               query_terminal_ns=14_000_000_000,backend_report={'comparison_timing':dict(complete=True,
               driver_entered_ns=11_000_000_000,API_returned_ns=13_000_000_000)})
        timing=query_timing(q)
        self.assertEqual(timing['API_return_s'],12.)
        self.assertEqual(timing['driver_only_s'],2.)

    def test_primary_error_identity_and_evidence(self):
        error=RuntimeError('original');error.query_report={'status':'failed'}
        class Base:
            def query(self,spec,emit):raise error
        store=ObservationStore(Clock(100,900))
        with self.assertRaises(RuntimeError) as caught:timed_driver_type(Base,store)().query({'query_id':'q'},None)
        self.assertIs(caught.exception,error)
        self.assertEqual(error.query_report['comparison_timing']['status'],'raised')

    def test_clock_failure_does_not_kill_result(self):
        class Base:
            def query(self,*args):return {'result':42}
        store=ObservationStore(Clock(OSError('clock'),900))
        report=timed_driver_type(Base,store)().query({'query_id':'q'},None)
        self.assertEqual(report['result'],42);self.assertFalse(report['comparison_timing']['complete'])
        self.assertEqual(store.warnings,['OSError'])

    def test_no_exact_clock_never_uses_terminal(self):
        timing=query_timing(dict(query_start_ns=100,query_result_ready_ns=500,query_terminal_ns=900))
        self.assertIsNone(timing['API_return_s']);self.assertEqual(timing['session_terminal_s'],.0000008)

    def test_bad_clock_order_is_missing_not_clamped(self):
        timing=query_timing(dict(query_start_ns=500,query_result_ready_ns=800,query_terminal_ns=900,
            backend_report={'comparison_timing':dict(complete=True,driver_entered_ns=100,API_returned_ns=900)}))
        self.assertIsNone(timing['API_return_s']);self.assertIn('ordering',timing['reason'])




class MemorySinkTests(unittest.TestCase):
    def query(self):
        return dict(status='completed',query_start_ns=1_000_000_000,
            query_result_ready_ns=22_000_000_000,query_terminal_ns=30_000_000_000,
            backend_report=dict(status='completed',
                memory_sink_timing=dict(schema='semloom.kalypso_memory_sink.v1',
                    memory_sink_ready_ns=4_000_000_000,sink_scope='complete original relation'),
                comparison_timing=dict(complete=True,driver_entered_ns=2_000_000_000,
                    API_returned_ns=29_000_000_000)))

    def test_versioned_source_excludes_preflight_and_keeps_legacy_caller_interval(self):
        q=self.query()
        q['backend_report']['memory_sink_timing'].update(
            query_clock_version='sembench.source_to_sink.v2',source_started_ns=3_000_000_000)
        result=query_timing(q)
        self.assertEqual(result['source_to_sink_seconds'],1)
        self.assertEqual(result['caller_to_memory_sink_seconds'],3)
        self.assertEqual(result['source_started_ns'],3_000_000_000)
        self.assertEqual(result['query_clock_version'],'sembench.source_to_sink.v2')

    def test_versioned_source_never_falls_back_to_submission_when_missing_or_invalid(self):
        for source in (None,True,0,1_500_000_000,5_000_000_000):
            with self.subTest(source=source):
                q=self.query()
                q['backend_report']['memory_sink_timing'].update(
                    query_clock_version='sembench.source_to_sink.v2',source_started_ns=source)
                self.assertIsNone(query_timing(q)['source_to_sink_seconds'])

    def test_unknown_version_never_relabels_the_legacy_interval(self):
        q=self.query()
        q['backend_report']['memory_sink_timing']['query_clock_version']='unknown'
        result=query_timing(q)
        self.assertIsNone(result['source_to_sink_seconds'])
        self.assertIn('unrecognized',result['source_to_sink_unavailable_reason'])

    def test_audit_and_event_delivery_do_not_extend_memory_sink(self):
        q=self.query()
        first=query_timing(q)
        q['query_result_ready_ns']+=4_000_000_000
        q['events']=[dict(event='memory_sink_ready',monotonic_ns=10_000_000_000)]
        later=query_timing(q)
        self.assertEqual(first['source_to_sink_seconds'],3.)
        self.assertEqual(later['source_to_sink_seconds'],3.)
        self.assertEqual(first['answer_ready_s'],21.)
        self.assertEqual(later['answer_ready_s'],25.)
        self.assertEqual(later['API_return_s'],28.)

    def test_sink_is_independent_of_optional_API_return_receipt(self):
        q=self.query()
        del q['backend_report']['comparison_timing']
        result=query_timing(q)
        self.assertEqual(result['source_to_sink_seconds'],3.)
        self.assertIsNone(result['API_return_s'])
        self.assertIsNone(result['source_to_sink_unavailable_reason'])

    def test_no_sink_never_falls_back_to_legacy_or_terminal(self):
        for report in (None,{},dict(memory_sink_timing={'schema':'unrecognized'})):
            with self.subTest(report=report):
                q=self.query();q['backend_report']=report
                result=query_timing(q)
                self.assertIsNone(result['source_to_sink_seconds'])
                self.assertIsNone(result['memory_sink_ready_ns'])
                self.assertIn('not substituted',result['source_to_sink_unavailable_reason'])
                self.assertEqual(result['answer_ready_s'],21.)

    def test_invalid_sink_boundaries_are_unavailable_not_clamped(self):
        for sink in (None,True,-1,500_000_000,1_500_000_000,23_000_000_000,31_000_000_000):
            with self.subTest(sink=sink):
                q=self.query()
                q['backend_report']['memory_sink_timing']['memory_sink_ready_ns']=sink
                result=query_timing(q)
                self.assertIsNone(result['source_to_sink_seconds'])
                self.assertTrue(result['source_to_sink_unavailable_reason'])

    def test_failed_audit_keeps_observed_sink_without_claiming_success(self):
        q=self.query();q['status']='failed';q['query_result_ready_ns']=None
        q['backend_report']['status']='failed'
        result=query_timing(q)
        self.assertEqual(result['source_to_sink_seconds'],3.)
        self.assertIsNone(result['answer_ready_s'])
        self.assertEqual(q['status'],'failed')

    def test_attached_failure_keeps_timing_and_original_exception(self):
        from src.experiments import kalypso_query_lifecycle as attached
        from src.experiments import native_query_lifecycle as sessions
        q=self.query();q['status']='failed';q['query_result_ready_ns']=None
        q['backend_report']['status']='failed'
        failure=RuntimeError('post-sink audit failed')
        failure.session_report=dict(status='failed',queries=[q])
        with patch.object(attached,'server_argv'), \
             patch.object(attached.native,'load_dependencies'), \
             patch.object(sessions,'run_sequence',side_effect=failure):
            with self.assertRaises(RuntimeError) as caught:
                attached.run_attached({},dict(queries=[dict(task='movie-q2')]),'unused-fixture-output')
        self.assertIs(caught.exception,failure)
        self.assertEqual(q['complete_query_timing']['source_to_sink_seconds'],3.)
        self.assertEqual(failure.session_report['status'],'failed')


class UnitTests(unittest.TestCase):
    def rows(self,status='succeeded'):
        return [dict(call_id=str(i),row_id=str(i),engine_uid='e',model_start_monotonic=1.,
                     model_end_monotonic=2.,status=status) for i in range(4)]

    def test_chat_batch_not_copied_per_row(self):
        samples=model_API_samples('ray-data',dict(calls=dict(records=self.rows())))
        self.assertEqual(len(samples),1);self.assertEqual(samples[0]['records'],4)

    def test_independent_Core_calls_not_merged_by_same_clock(self):
        samples=model_API_samples('semloom-core',dict(calls=dict(records=self.rows())))
        self.assertEqual(len(samples),4);self.assertTrue(all(x['records']==1 for x in samples))

    def test_missing_batch_identity_never_invents_row_API_units(self):
        rows=self.rows()
        for row in rows:row.pop('model_start_monotonic');row.pop('model_end_monotonic')
        samples=model_API_samples('daft',dict(calls=dict(records=rows)))
        self.assertEqual(sum(x['countable_API_unit'] for x in samples),0)
        self.assertEqual(sum(x['records'] for x in samples),4)

    def test_failed_and_unknown_API_units_kept(self):
        rows=self.rows();rows[0]['status']='unknown';rows[1]['status']='failed'
        samples=model_API_samples('semloom-core',dict(calls=dict(records=rows)))
        self.assertEqual(len(samples),4);self.assertEqual({x['status'] for x in samples},{'succeeded','failed','unknown'})

    def test_HTTP_and_LM_batch_are_different_units(self):
        report=dict(http_receipts=[dict(attempt=1,http_started=True,http_status=200,
            external_HTTP_timing=dict(complete=True,started_ns=100,finished_ns=300,elapsed_s=.0000002))],
            comparison_timing=dict(native_batch_API=[dict(input_records=4,elapsed_s=1.)]))
        samples=model_API_samples('lotus-native',report)
        self.assertEqual(len(samples),1);self.assertEqual(samples[0]['unit'],'HTTP_POST')

    def test_local_native_uses_actual_independent_model_receipts(self):
        report = dict(protocol='local_async_model_IPC', native_transport_receipts=[
                          dict(row, submission_confirmed=True) for row in self.rows()],
                      calls=dict(records=[]))
        samples = model_API_samples('lotus-native', report)
        self.assertEqual(len(samples), 4)
        self.assertTrue(all(row['unit'] == 'single_generate' and row['records'] == 1 for row in samples))
        self.assertTrue(all(row['elapsed_s'] == 1 for row in samples))

    def test_local_native_keeps_presubmit_rejections_without_counting_model_API_calls(self):
        for submitted in (False, None):
            report = dict(protocol='local_async_model_IPC', native_transport_receipts=[
                dict(self.rows('failed')[0], submission_confirmed=submitted)])
            sample = model_API_samples('lotus-native', report)[0]
            self.assertFalse(sample['countable_API_unit'])
            self.assertEqual(sample['status'], 'not_sent' if submitted is False else 'unknown')
            self.assertIsNone(sample['elapsed_s'])
            self.assertIsNone(sample['start_ns'])

    def test_local_native_unconfirmed_unknown_is_not_reported_as_unsent(self):
        report = dict(protocol='local_async_model_IPC', native_transport_receipts=[
            dict(self.rows('unknown')[0], submission_confirmed=False)])
        sample = model_API_samples('lotus-native', report)[0]
        self.assertEqual(sample['status'], 'unknown')
        self.assertFalse(sample['countable_API_unit'])
        self.assertIsNone(sample['elapsed_s'])

    def test_duplicate_offline_event_charged_once(self):
        event=dict(event_id='scan',actual_scan_calls=10,actual_scan_wall_s=2.)
        self.assertEqual(offline_scan_total([event,dict(event)]),dict(events=1,calls=10,wall_s=2.))

    def test_conflicting_offline_identity_rejected(self):
        event=dict(event_id='scan',actual_scan_calls=10,actual_scan_wall_s=2.)
        with self.assertRaises(ValueError):offline_scan_total([event,dict(event,actual_scan_wall_s=3.)])


if __name__=='__main__':unittest.main(verbosity=2)
