"""Kernel memory intervals and source identity; temporary fixtures only."""
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
import unittest

from src.observability.metrics import memory_pressure as metrics
from tests.observability.memory_pressure_fixture import KernelMemoryFixture


class KernelMemoryPressureTests(unittest.TestCase):
    def test_initial_read_is_unknown_and_clean_interval_is_not_capacity(self):
        with KernelMemoryFixture() as kernel:
            first, second = kernel.source(), kernel.source()
            self.assertIsNone(first['pressure'])
            self.assertEqual(first['status'], 'baseline_priming')
            self.assertIs(second['pressure'], False)
            self.assertEqual(second['status'], 'observed')
            self.assertEqual(second['source_sequence'], 2)
            self.assertEqual(second['interval_s'], .25)
            self.assertNotIn('available_bytes', second)
            self.assertNotIn('request_limit', second)
            self.assertNotIn('RSS_limit', second)

    def test_each_real_reclaim_or_stall_counter_can_signal_pressure(self):
        for kind in ('low','high','max','oom','oom_kill','oom_group_kill','some','full'):
            with self.subTest(kind=kind), KernelMemoryFixture() as kernel:
                kernel.source()
                values = {kind: 1}
                if kind == 'full':values['some'] = 1
                kernel.write(kernel.leaf, **values)
                self.assertIs(kernel.source()['pressure'], True)
                self.assertIs(kernel.source()['pressure'], False)

    def test_shared_ancestor_pressure_is_kept_separate_from_exclusive_ownership(self):
        with KernelMemoryFixture() as kernel:
            kernel.source()
            kernel.write(kernel.mount, high=1)
            frame = kernel.source()
            self.assertIs(frame['pressure'], True)
            self.assertEqual(frame['increments'][0]['events_high'], 0)
            self.assertEqual(frame['increments'][1]['events_high'], 1)

    def test_actual_root_without_events_can_still_report_psi(self):
        with KernelMemoryFixture() as kernel:
            (kernel.mount/'memory.events').unlink()
            kernel.source()
            kernel.write(kernel.mount, some=2)
            (kernel.mount/'memory.events').unlink()
            self.assertIs(kernel.source()['pressure'], True)

    def test_missing_leaf_event_or_psi_file_never_becomes_zero(self):
        for name in ('memory.events','memory.pressure'):
            with self.subTest(name=name), KernelMemoryFixture() as kernel:
                kernel.source()
                (kernel.leaf/name).unlink()
                self.assertIsNone(kernel.source()['pressure'])
                kernel.write(kernel.leaf)
                self.assertEqual(kernel.source()['status'], 'baseline_priming')

    def test_malformed_duplicate_negative_and_incomplete_counters_stay_unknown(self):
        cases = [('memory.events','high -1\n'),('memory.events','high 1\nhigh 2\n'),
            ('memory.pressure','some total=0\n'),('memory.pressure','some total=1\nfull total=2\n'),
            ('memory.pressure','some total=1 total=2\nfull total=0\n')]
        for name, data in cases:
            with self.subTest(data=data), KernelMemoryFixture() as kernel:
                (kernel.leaf/name).write_text(data)
                self.assertIsNone(kernel.source()['pressure'])

    def test_counter_reversal_changes_source_epoch_and_remains_unknown(self):
        with KernelMemoryFixture() as kernel:
            kernel.write(kernel.leaf, high=2)
            first = kernel.source()
            kernel.write(kernel.leaf, high=1)
            reversed_frame = kernel.source()
            self.assertIsNone(reversed_frame['pressure'])
            self.assertEqual(reversed_frame['status'], 'counter_reversed_or_shape_changed')
            self.assertNotEqual(first['source_uid'], reversed_frame['source_uid'])

    def test_recreated_cgroup_changes_identity_before_reporting_clean(self):
        with KernelMemoryFixture() as kernel:
            first = kernel.source()
            kernel.leaf.rename(kernel.mount/'retired')
            kernel.leaf.mkdir()
            kernel.write(kernel.leaf)
            (kernel.leaf/'memory.max').write_text('max')
            frame = kernel.source()
            self.assertIsNone(frame['pressure'])
            self.assertEqual(frame['status'], 'membership_changed')
            self.assertNotEqual(first['source_uid'], frame['source_uid'])
            self.assertNotEqual(first['hierarchy_uid'], frame['hierarchy_uid'])

    def test_membership_move_during_read_is_unknown(self):
        with KernelMemoryFixture() as kernel:
            with patch.object(metrics,'controller_hierarchy', side_effect=[
                    (2,(kernel.leaf,kernel.mount)),(2,(kernel.mount,))]):
                self.assertIsNone(kernel.source()['pressure'])

    def test_unsupported_hierarchy_and_read_errors_do_not_use_host_ram(self):
        with KernelMemoryFixture() as kernel:
            for version in (0,1):
                with self.subTest(version=version), patch.object(metrics,'controller_hierarchy',return_value=(version,())):
                    self.assertIsNone(kernel.source()['pressure'])
            with patch.object(metrics,'controller_hierarchy',side_effect=PermissionError('CPU fixture')):
                self.assertIsNone(kernel.source()['pressure'])

    def test_invalid_or_overlapping_clock_does_not_qualify(self):
        with KernelMemoryFixture() as kernel:
            kernel.source()
            for value in (False,float('nan'),float('inf'),-1,0):
                with self.subTest(value=value):
                    kernel.source.clock=lambda: value
                    self.assertIsNone(kernel.source()['pressure'])

    def test_concurrent_reads_share_one_locked_baseline(self):
        with KernelMemoryFixture() as kernel, ThreadPoolExecutor(max_workers=4) as threads:
            values=list(threads.map(lambda _:kernel.source(),range(12)))
            self.assertEqual(sorted(v['source_sequence'] for v in values),list(range(1,13)))
            self.assertEqual(sum(v['status']=='baseline_priming' for v in values),1)
            self.assertEqual(sum(v['pressure'] is False for v in values),11)
