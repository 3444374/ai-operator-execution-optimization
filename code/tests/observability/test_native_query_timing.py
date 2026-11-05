import unittest

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

    def test_duplicate_offline_event_charged_once(self):
        event=dict(event_id='scan',actual_scan_calls=10,actual_scan_wall_s=2.)
        self.assertEqual(offline_scan_total([event,dict(event)]),dict(events=1,calls=10,wall_s=2.))

    def test_conflicting_offline_identity_rejected(self):
        event=dict(event_id='scan',actual_scan_calls=10,actual_scan_wall_s=2.)
        with self.assertRaises(ValueError):offline_scan_total([event,dict(event,actual_scan_wall_s=3.)])


if __name__=='__main__':unittest.main(verbosity=2)
