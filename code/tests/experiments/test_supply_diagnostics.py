import unittest

from src.experiments.supply_diagnostics import diagnose_supply


def events():
    base = dict(run_id='run', observer_session_id='observer', pid=3, clock_domain='a' * 64,
                engine_uid='e' * 32, core_uid='c' * 32)
    row = dict(driver_uid='d' * 32, method_row_ref='row-hash', stage_index=0, session_id=1)
    task = dict(session_id=1, sequence=0)
    values = [
        dict(event='method_ready_boundary', action='method_ready', monotonic_ns=10, **row),
        dict(event='native_queue_snapshot', pid=5, source_sequence=1, running=0, waiting=0, monotonic_ns=15),
        dict(event='method_ready_boundary', action='Core_offer_accepted', monotonic_ns=20, key=task, **row),
        dict(event='Core_dispatch_capacity_blocked', monotonic_ns=22, key=task),
        dict(event='native_queue_snapshot', pid=5, source_sequence=2, running=0, waiting=0, monotonic_ns=25),
        dict(event='Core_submitted', monotonic_ns=30, key=task, record_phase='INFLIGHT'),
    ]
    return [dict(base, **value) for value in values]


class SupplyTests(unittest.TestCase):
    def test_true_id_and_clock_join_reports_leads_not_physical_starvation(self):
        report = diagnose_supply(events(), capture_issues=[])
        self.assertEqual(report['capture_status'], 'closed_without_recorded_loss')
        self.assertEqual(report['summary']['closed_wait_intervals'], 2)
        self.assertEqual(report['summary']['matched_snapshot_points'], 2)
        self.assertTrue(all(row['physical_starvation'] == 'unknown' for row in report['findings']))
        self.assertEqual([v['duration_ns'] for v in report['waits']], [10, 10])
        self.assertEqual(len(report['waits'][1]['observed_capacity_block_events']), 1)

    def test_different_or_missing_host_clock_does_not_join_remote_points(self):
        for domain in ('b' * 64, None):
            values = events()
            for row in values:
                if row['event'] == 'native_queue_snapshot': row['clock_domain'] = domain
            report = diagnose_supply(values, capture_issues=[])
            self.assertEqual(report['summary']['matched_snapshot_points'], 0)
            self.assertIn('no_matching_engine_clock_and_wait_interval', report['capture_issues'])

    def test_wrong_engine_or_run_cannot_supply_a_ready_row(self):
        for changed in (dict(engine_uid='z' * 32), dict(run_id='other-run')):
            values = events()
            for row in values:
                if row['event'] == 'native_queue_snapshot': row.update(changed)
            self.assertEqual(diagnose_supply(values, capture_issues=[])['findings'], [])

    def test_core_identity_prevents_same_session_and_sequence_cross_join(self):
        first, second = events(), events()
        for row in second:
            row['core_uid'] = 'f' * 32
            if 'driver_uid' in row: row['driver_uid'] = 'g' * 32
            row['monotonic_ns'] += 100
            if 'source_sequence' in row: row['source_sequence'] += 2
        report = diagnose_supply(first + second, capture_issues=[])
        self.assertEqual(report['capture_status'], 'closed_without_recorded_loss')
        self.assertEqual([row['duration_ns'] for row in report['waits']], [10, 10, 10, 10])

    def test_conflicting_duplicate_queue_snapshots_do_not_choose_a_favorable_variant(self):
        values = events(); duplicate = dict(values[1], running=8)
        values.append(duplicate)
        report = diagnose_supply(values, capture_issues=[])
        self.assertEqual(report['summary']['matched_snapshot_points'], 1)
        self.assertIn('duplicate_native_queue_sequence', report['capture_issues'])

    def test_waiting_work_is_a_separate_signal_and_sparse_samples_are_not_interpolated(self):
        values = events(); values[4].update(running=4, waiting=3)
        report = diagnose_supply(values, capture_issues=[])
        self.assertEqual(report['summary']['phenomena'],
            dict(ready_with_reported_empty_engine_queue=1, engine_reports_waiting_work=1))
        self.assertNotIn('idle_duration_ns', str(report))

    def test_unfinished_ready_wait_is_not_extended_to_an_arbitrary_capture_end(self):
        values = events()[:2]
        report = diagnose_supply(values, capture_issues=[])
        self.assertEqual(len(report['unresolved_waits']), 1)
        self.assertEqual(report['findings'], [])

    def test_future_buffer_timestamp_and_unconfirmed_sink_remain_incomplete(self):
        values = events(); values[1]['observation_ns'] = 1000
        report = diagnose_supply(values, capture_issues=['sink_loss'])
        self.assertIn('event_identity_or_clock_missing', report['capture_issues'])
        self.assertEqual(report['capture_status'], 'incomplete')


if __name__ == '__main__':
    unittest.main()
