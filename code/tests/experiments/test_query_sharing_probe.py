"""Independent trace checks and falsification cases, with no model/backend I/O."""

import unittest

from src.experiments.query_sharing_probe import Query, run
from src.experiments.query_sharing_analysis import audit


class QuerySharingProbeTests(unittest.TestCase):
    def audit(self, result):
        submits, terminals, active = set(), set(), {}
        peaks = 0
        for event in sorted(result['trace'], key=lambda e: (e['t'], e['event'] != 'service_finish')):
            key = (event['key']['session_id'], event['key']['sequence'])
            if event['event'] == 'submitted':
                self.assertNotIn(key, submits)
                submits.add(key)
                active[key] = event
                peaks = max(peaks, len(active))
                self.assertLessEqual(len(active), result['admission'])
            elif event['event'] == 'service_finish':
                self.assertIn(key, active)
                self.assertNotIn(key, terminals)
                terminals.add(key)
                del active[key]
        self.assertEqual(submits, terminals)
        self.assertEqual(len(submits), sum(q['rows'] for q in result['queries']))
        self.assertEqual(len(result['jobs']), len(result['queries']))
        self.assertTrue(all(v == 0 for v in result['final_usage'].values()))
        self.assertEqual(peaks, result['peaks']['active_requests'])
        for endpoint in ('e0', 'e1'):
            starts = [e['t'] for e in result['trace'] if e['endpoint'] == endpoint and e['event'] == 'service_start']
            finishes = [e['t'] for e in result['trace'] if e['endpoint'] == endpoint and e['event'] == 'service_finish']
            self.assertTrue(all(b >= a for a, b in zip(finishes, starts[1:])))

    def test_real_core_uses_independent_registered_queries_and_no_future_arrival(self):
        queries = (Query('long', 64, 0), Query('short', 4, .5))
        results = [run(queries, arm, admission=2) for arm in ('strong_static', 'bounded_short')]
        for result in results:
            self.audit(result)
            short_id = result['jobs']['short']['job_id']
            self.assertTrue(all(e['t'] >= .5 for e in result['trace'] if e['job_id'] == short_id))
        self.assertLess(results[1]['jobs']['short']['jct_s'], results[0]['jobs']['short']['jct_s'])
        self.assertEqual(results[1]['group_jct_s'], results[0]['group_jct_s'])

    def test_work_conserving_control_borrows_idle_compute(self):
        result = run((Query('long', 64, 0), Query('short', 4, 40)), 'work_conserving')
        self.audit(result)
        self.assertEqual(result['jobs']['long']['jct_s'], 32)
        self.assertEqual(result['peaks']['active_requests'], 4)

    def test_nonpreemptive_large_work_can_erase_every_short_job_gain(self):
        queries = (Query('long', 2, 0, 20, 20), Query('short', 1, .5))
        results = [run(queries, arm, admission=2) for arm in ('strong_static', 'bounded_short')]
        for result in results:
            self.audit(result)
        self.assertEqual(results[0]['jobs']['short']['jct_s'], results[1]['jobs']['short']['jct_s'])

    def test_bad_estimates_are_not_replaced_with_offline_service_times(self):
        queries = (Query('long', 7, 0), Query('short', 3, .5, 5, 1))
        static = run(queries, 'strong_static', admission=2)
        candidate = run(queries, 'bounded_short', admission=2)
        self.audit(candidate)
        self.assertGreater(candidate['jobs']['long']['jct_s'], 1.05 * static['jobs']['long']['jct_s'])
        short = candidate['jobs']['short']['job_id']
        self.assertTrue(all(e['estimated_work'] == 1 for e in candidate['trace'] if e['job_id'] == short))

    def test_independent_audit_rejects_duplicate_submission_evidence(self):
        result = run((Query('short', 2, 0),), 'bounded_short', admission=2)
        audit(result)
        result['trace'].append(next(e for e in result['trace'] if e['event'] == 'submitted').copy())
        with self.assertRaises(AssertionError):
            audit(result)

    def test_independent_audit_rejects_capacity_sample_without_job_ownership(self):
        result = run((Query('short', 2, 0),), 'bounded_short', admission=2)
        result['resources'][0]['active_requests'] = 4
        with self.assertRaises(AssertionError):
            audit(result)


if __name__ == '__main__':
    unittest.main()
