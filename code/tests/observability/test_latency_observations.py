"""Existing a244 latency-observation regressions; no query-cost dependency."""
import unittest
from src.observability.metrics.statistics import latency_observations


class LatencyObservationTests(unittest.TestCase):
    def row(self,i,seconds,**extra):
        return dict(id=str(i),elapsed_s=seconds,status='succeeded',phase='reuse',**extra)

    def test_small_query_sample_reports_maximum_not_population_p99(self):
        r=latency_observations([self.row(1,2),self.row(2,5),self.row(3,3)])
        self.assertEqual((r['median_s'],r['maximum_s']),(3,5))
        self.assertIsNone(r['empirical_p99_s']);self.assertFalse(r['population_P99_established'])
        self.assertEqual(r['slowest'][0]['id'],'2')

    def test_empirical_rank_has_sample_size_and_no_population_claim(self):
        r=latency_observations([self.row(i,i) for i in range(1,201)])
        self.assertEqual((r['completed_units'],r['empirical_p99_s'],r['empirical_p99_rank']),(200,198,198))
        self.assertFalse(r['population_P99_established'])

    def test_longest_request_need_not_be_last_to_complete(self):
        rows=[self.row('long',.410826,start_s=1.210174,end_s=1.621,clock_domain='same-clock',clock_domain_verified=True),
              self.row('last',.144167,start_s=16.769833,end_s=16.914,clock_domain='same-clock',clock_domain_verified=True)]
        r=latency_observations(list(reversed(rows)))
        self.assertEqual(r['slowest'][0]['id'],'long');self.assertEqual(r['last_completed']['id'],'last')

    def test_missing_clock_or_phase_does_not_fabricate_queue_or_inference_time(self):
        r=latency_observations([self.row(1,1,end_s=4)])
        self.assertIsNone(r['last_completed']);self.assertFalse(r['clock_correlation_available'])
        self.assertNotIn('pure_GPU_inference_s',r)
        r=latency_observations([self.row(1,1,end_s=4,clock_domain='unverified-label')])
        self.assertFalse(r['clock_correlation_available'])

    def test_failures_unknown_and_missing_samples_are_not_silently_dropped(self):
        rows=[self.row(i,i) for i in range(100)]
        rows.extend([dict(id='failed',elapsed_s=8,status='failed'),dict(id='unknown',elapsed_s=None,status='unknown')])
        r=latency_observations(rows)
        self.assertEqual(r['incomplete_observations'],2);self.assertIsNone(r['empirical_p99_s'])

    def test_four_copied_batch_rows_count_as_one_interval(self):
        rows=[self.row(i,2,copied_batch_interval=True,batch='batch-1') for i in range(4)]
        r=latency_observations(rows,sample_unit_key='batch')
        self.assertEqual((r['observations'],r['completed_units']),(4,1))
        self.assertIsNone(latency_observations(rows)['empirical_p99_s'])
