"""Sample only supervisor-owned births and explicitly declared limits; no GPU."""
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.observability.process_resources import supervised


# Captured startup observations are trace values, never resource ceilings.
OBSERVED_AUTHOR_SHM_BYTES = 536969216
OBSERVED_MODEL_READY_SHM_BYTES = 4096
OBSERVED_STARTUP_SHM_BYTES = OBSERVED_AUTHOR_SHM_BYTES + OBSERVED_MODEL_READY_SHM_BYTES
FIXTURE_PAGE_BYTES = 4096
FIXTURE_AVAILABLE_SHM_BYTES = 2 * 1024**3


class SupervisedResourcesTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.limits = dict(cpu_affinity_slots=[1, 3],
            max_sampled_owned_rss_bytes=1000, max_owned_shm_bytes=100)

    def process(self, *, born=1, status='running', rss=64, affinity=(1, 3)):
        return SimpleNamespace(create_time=lambda: born, status=lambda: status,
            memory_info=lambda: SimpleNamespace(rss=rss), cpu_affinity=lambda: affinity)

    def test_reused_pid_and_zombie_do_not_contribute_to_sampled_rss(self):
        processes = {10: self.process(), 11: self.process(born=9, rss=900),
            12: self.process(status=supervised.psutil.STATUS_ZOMBIE, rss=900)}
        with patch.object(supervised.ResourceMonitor, '_shm_used', return_value=0), \
                patch.object(supervised.psutil, 'Process', side_effect=processes.__getitem__):
            monitor = supervised.ResourceMonitor(self.limits, shm_root=self.root)
            monitor({(10, 1), (11, 1), (12, 1)})
            report = monitor.finish()
        self.assertTrue(report['passed'])
        self.assertEqual(report['samples'][0]['observed_rss_sum_bytes'], 64)
        self.assertFalse(report['kernel_memory_limit_enforced'])
        self.assertFalse(report['unobserved_detached_process_exit_confirmed'])

    def test_declared_rss_or_cpu_affinity_violation_is_preserved(self):
        for process in (self.process(rss=1001), self.process(affinity=(1, 7))):
            with self.subTest(process=process), \
                    patch.object(supervised.ResourceMonitor, '_shm_used', return_value=0), \
                    patch.object(supervised.psutil, 'Process', return_value=process):
                monitor = supervised.ResourceMonitor(self.limits, shm_root=self.root)
                with self.assertRaisesRegex(RuntimeError, 'resource/affinity ceiling'):
                    monitor({(10, 1)})
                self.assertFalse(monitor.finish()['passed'])

    def test_host_shm_delta_and_new_names_remain_distinct_from_owned_rss(self):
        with patch.object(supervised.ResourceMonitor, '_shm_used', side_effect=[0, 101, 0]):
            monitor = supervised.ResourceMonitor(self.limits, shm_root=self.root)
            with self.assertRaisesRegex(RuntimeError, 'resource/affinity ceiling'):
                monitor(set())
            (self.root / 'leftover').touch()
            report = monitor.finish()
        self.assertEqual(report['samples'][0]['host_shm_used_delta_bytes'], 101)
        self.assertEqual(report['remaining_new_shm_names'], ['leftover'])
        self.assertFalse(report['passed'])

    def test_startup_observations_and_later_growth_fit_physical_available_shm(self):
        baseline = 2 * FIXTURE_PAGE_BYTES
        # Replay captured startup, then synthetic multi-page and 8 MiB growth.
        deltas = [OBSERVED_MODEL_READY_SHM_BYTES, OBSERVED_STARTUP_SHM_BYTES,
                  OBSERVED_STARTUP_SHM_BYTES + 3 * FIXTURE_PAGE_BYTES,
                  OBSERVED_STARTUP_SHM_BYTES + 3 * FIXTURE_PAGE_BYTES + 8 * 1024**2]
        trace = [baseline, *(baseline + delta for delta in deltas), baseline]
        limits = dict(self.limits, max_owned_shm_bytes=FIXTURE_AVAILABLE_SHM_BYTES)
        self.assertEqual(OBSERVED_AUTHOR_SHM_BYTES, 4 * (128 * 1024**2 + 2 * 12 * 1024))
        self.assertEqual(OBSERVED_STARTUP_SHM_BYTES, 536973312)
        with patch.object(supervised.ResourceMonitor, '_shm_used', side_effect=trace):
            monitor = supervised.ResourceMonitor(limits, shm_root=self.root, sample_seconds=0)
            for _ in deltas:
                monitor(set())
            report = monitor.finish()
        self.assertTrue(report['passed'])
        self.assertEqual([sample['host_shm_used_delta_bytes'] for sample in report['samples']], deltas)
        self.assertEqual(report['failures'], [])
        self.assertEqual(report['remaining_new_shm_names'], [])
        self.assertEqual(report['final_host_shm_used_delta_bytes'], 0)

    def test_physical_available_shm_accepts_exact_and_rejects_one_byte_or_one_page_more(self):
        baseline = 3 * FIXTURE_PAGE_BYTES
        limits = dict(self.limits, max_owned_shm_bytes=FIXTURE_AVAILABLE_SHM_BYTES)
        # One byte checks the comparator; the captured host measurement uses pages.
        for excess in (0, 1, FIXTURE_PAGE_BYTES):
            peak = FIXTURE_AVAILABLE_SHM_BYTES + excess
            with self.subTest(excess=excess), \
                    patch.object(supervised.ResourceMonitor, '_shm_used',
                                 side_effect=[baseline, baseline + peak, baseline]):
                monitor = supervised.ResourceMonitor(limits, shm_root=self.root, sample_seconds=0)
                if excess:
                    with self.assertRaisesRegex(RuntimeError, 'resource/affinity ceiling'):
                        monitor(set())
                else:
                    monitor(set())
                report = monitor.finish()
            self.assertEqual(report['passed'], excess == 0)
            self.assertEqual(report['samples'][0]['host_shm_used_delta_bytes'], peak)
            self.assertEqual(report['final_host_shm_used_delta_bytes'], 0)

    def test_shm_guard_failure_survives_later_clean_samples_and_final_cleanup(self):
        baseline = FIXTURE_PAGE_BYTES
        limits = dict(self.limits, max_owned_shm_bytes=FIXTURE_AVAILABLE_SHM_BYTES)
        trace = [baseline, baseline + FIXTURE_AVAILABLE_SHM_BYTES + FIXTURE_PAGE_BYTES, baseline, baseline]
        with patch.object(supervised.ResourceMonitor, '_shm_used', side_effect=trace):
            monitor = supervised.ResourceMonitor(limits, shm_root=self.root, sample_seconds=0)
            with self.assertRaisesRegex(RuntimeError, 'resource/affinity ceiling'):
                monitor(set())
            monitor(set())
            report = monitor.finish()
        self.assertEqual(report['samples'][-1]['host_shm_used_delta_bytes'], 0)
        self.assertEqual(report['final_host_shm_used_delta_bytes'], 0)
        self.assertEqual(report['remaining_new_shm_names'], [])
        self.assertEqual(report['failures'], ['sampled resource/affinity ceiling exceeded'])
        self.assertFalse(report['passed'])

    def test_final_host_shm_residue_fails_without_new_names_or_a_peak_violation(self):
        baseline = 4 * FIXTURE_PAGE_BYTES
        limits = dict(self.limits, max_owned_shm_bytes=FIXTURE_AVAILABLE_SHM_BYTES)
        trace = [baseline, baseline + FIXTURE_PAGE_BYTES, baseline + FIXTURE_PAGE_BYTES]
        with patch.object(supervised.ResourceMonitor, '_shm_used', side_effect=trace):
            monitor = supervised.ResourceMonitor(limits, shm_root=self.root, sample_seconds=0)
            monitor(set())
            report = monitor.finish()
        self.assertEqual(report['remaining_new_shm_names'], [])
        self.assertEqual(report['failures'], [])
        self.assertEqual(report['final_host_shm_used_delta_bytes'], FIXTURE_PAGE_BYTES)
        self.assertFalse(report['passed'])


if __name__ == '__main__':
    unittest.main()
