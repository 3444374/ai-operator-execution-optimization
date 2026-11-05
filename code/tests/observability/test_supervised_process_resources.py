"""Sample only supervisor-owned births and explicitly declared limits; no GPU."""
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.observability.process_resources import supervised


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


if __name__ == '__main__':
    unittest.main()
