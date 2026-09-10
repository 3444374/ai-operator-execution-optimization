"""Memory evidence must be attributed, complete and bounded, including every node."""
import json
import unittest
from src.experiments.postgresql.window_memory import verify_window_memory


class WindowMemoryTests(unittest.TestCase):
    def value(self,**updates):
        return dict(dict(version=1,backend_pid=17,memory_id=1,retained_rows=0,pending_rows=0,retained_bytes=0,
            retained_limit=200,staging_limit=300,peak_retained_bytes=180,peak_staging_bytes=90,
            peak_conversion_bytes=20,peak_receive_bytes=30,peak_retained_rows=2,memory_waits=1),**updates)

    def audit(self,*values):
        return verify_window_memory(['LOG: SEMLOOM_WINDOW_MEMORY '+json.dumps(v) for v in values],
                                    backend_pid=17,retained_limit=200,staging_limit=300,window=4)

    def test_multiple_nodes_report_sum_and_ignore_other_backends(self):
        report=self.audit(self.value(),self.value(memory_id=2),self.value(backend_pid=18))
        self.assertEqual(report['operators'],2)
        self.assertEqual(report['sum_retained_limits'],400)
        self.assertEqual(report['sum_retained_peaks'],360)

    def test_missing_duplicate_over_budget_and_unreleased_observations_fail(self):
        with self.assertRaises(ValueError):self.audit()
        with self.assertRaises(ValueError):self.audit(self.value(),self.value())
        for field,value in (('peak_retained_bytes',201),('peak_staging_bytes',301),
                            ('peak_conversion_bytes',1048577),('retained_rows',1),('pending_rows',1),
                            ('retained_bytes',1),('peak_retained_rows',5),('retained_limit',201)):
            with self.subTest(field=field),self.assertRaises(ValueError):self.audit(self.value(**{field:value}))
