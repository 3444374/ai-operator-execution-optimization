import unittest

from src.experiments.postgresql.native_adapter_metrics import (
    compare_methods, sample_distribution, summarize_calls, summarize_queries, executor_phase_observations,
)


class NativeAdapterMetricsTests(unittest.TestCase):
    def call(self, call_id='a', stage='first', ordinal=0):
        return dict(call_id=call_id, row_id='same-text-occurrence', stage_id=stage,
                    stage_ordinal=ordinal, model_role='main', request_values_sha256='a'*64)

    def event(self, call, name, seconds, **extra):
        return dict(event=name, call_id=call['call_id'], row_id=call['row_id'],
                    stage_id=call['stage_id'], monotonic_ns=int(seconds*1e9),
                    clock_domain='fixture-shared-clock', **extra)

    def events(self, call, offset=0):
        return [self.event(call, 'task_ready', 1+offset, request_values_sha256='a'*64),
                self.event(call, 'executor_submit', 3+offset),
                self.event(call, 'http_started', 5+offset),
                self.event(call, 'http_body_read', 8+offset, response_bytes_sha256='b'*64),
                self.event(call, 'caller_response', 9+offset, response_bytes_sha256='b'*64),
                self.event(call, 'result_release', 10+offset)]

    def test_readiness_queue_and_full_caller_response_are_separate(self):
        call=self.call()
        value=summarize_calls(list(reversed(self.events(call))), [call])
        self.assertEqual(value['request_e2e']['samples'], [8])
        self.assertEqual([value['calls'][0][name+'_seconds'] for name in (
            'ready_to_submit','submit_to_http','http_roundtrip','return_to_caller','result_retention')], [2,2,3,1,1])
        self.assertEqual(value['model_inference_seconds']['status'], 'unavailable')

    def test_successor_preparation_wait_is_observed(self):
        first,second=self.call('same-logical-call'),self.call('same-logical-call','second',1)
        value=summarize_calls(self.events(first)+self.events(second,10), [first,second])
        self.assertEqual(value['successor_waits'][0]['predecessor_response_to_successor_ready_seconds'],2)
        self.assertEqual(value['request_e2e']['count'],2)

    def test_unknown_stage_clock_duplicate_and_wrong_response_are_rejected(self):
        call=self.call()
        changed=self.events(call);changed[3]['response_bytes_sha256']='c'*64
        different=self.events(call);different[2]['clock_domain']='other'
        for events in [changed,different,self.events(call)+[self.events(call)[0]],self.events(call)[:-2]]:
            with self.subTest(events=events),self.assertRaises(ValueError):
                summarize_calls(events,[call])

    def test_small_query_samples_report_maximum_rank(self):
        records=[dict(status='passed',full_query_seconds=v+1,
                      execution=dict(ready_query_seconds=v)) for v in [3,1,2]]
        value=summarize_queries(records)
        self.assertEqual(value['ready_query']['count'],3)
        self.assertEqual(value['ready_query']['p99_99'],3)
        self.assertTrue(value['ready_query']['p99_99_is_sample_maximum'])
        self.assertFalse(value['ready_query']['independent_population_tail_estimate'])
        with self.assertRaises(ValueError):summarize_queries([dict(status='failed')])

    def test_empty_observations_are_unavailable_and_invalid_latencies_fail(self):
        value=sample_distribution([],unit='seconds')
        self.assertIsNone(value['p99'])
        for samples in [[-1],[float('inf')],[True]]:
            with self.assertRaises(ValueError):sample_distribution(samples,unit='seconds')

    def test_equal_prompts_do_not_merge_distinct_call_occurrences(self):
        first,second=self.call(),self.call('b')
        self.assertEqual(compare_methods([first,second],[second,first])['logical_calls'],2)
        for calls in [[first],[first,dict(second,model_role='helper')],[first,first]]:
            with self.assertRaises(ValueError):compare_methods([first,second],calls)

    def test_executor_spans_keep_queue_work_and_resume_separate(self):
        value=executor_phase_observations([dict(event='ray_work',stage='payload_next',status='completed',
                                                queue_ns=10,work_ns=20,resume_ns=30,elapsed_ns=60)])
        spans=value['representation_and_transfer']['payload_next']
        self.assertEqual(spans['work']['samples'],[20/1e9])
        self.assertEqual(spans['elapsed']['samples'],[60/1e9])
        self.assertEqual(value['organization']['status'],'unavailable')


if __name__ == '__main__':
    unittest.main()
