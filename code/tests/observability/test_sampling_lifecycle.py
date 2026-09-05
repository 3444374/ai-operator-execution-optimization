"""Behavior of incomplete sampling and stable baseline acquisition."""
from dataclasses import replace
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.observability.process_resources.model import (
    FdIdentity, FdKind, ProcessSnapshot, SampleTick, SnapshotStatus)
from src.observability.process_resources.recorder import (
    acquire_stable_baseline, persist_operation_outcome, record_operation)


def tick(ns=1, threads=1):
    return SampleTick(ns, True, {
        role: ProcessSnapshot(pid, pid, ns, SnapshotStatus.VALID,
                              rss_bytes=1024, thread_count=threads, fds=())
        for role, pid in (("backend", 1), ("gateway", 2))})


class SamplingLifecycleTests(unittest.TestCase):
    def test_first_sample_exception_preserves_outcome_without_running_operation(self):
        class Sampler:
            def sample_all(self, ns):
                raise ValueError("sensitive detail")
        calls = []
        result = record_operation(Sampler(), lambda: calls.append(1), sample_seconds=.01)
        self.assertEqual(calls, [])
        self.assertIsNotNone(result.sampling_error)
        self.assertEqual(result.trace.ticks, ())

    def test_final_sample_exception_preserves_operation_and_earlier_ticks(self):
        class Sampler:
            failed = False
            def sample_all(self, ns):
                if self.failed:
                    raise ValueError("final error")
                return tick(ns)
        sampler = Sampler()
        def operation():
            sampler.failed = True
            return 42
        result = record_operation(sampler, operation, sample_seconds=.01)
        self.assertEqual(result.result, 42)
        self.assertTrue(result.trace.ticks)
        self.assertIsNotNone(result.sampling_error)

    def test_thread_changes_do_not_form_a_baseline(self):
        class Sampler:
            n = 0
            def sample_all(self, ns):
                self.n += 1
                return tick(ns, self.n)
        baseline, _ = acquire_stable_baseline(
            Sampler(), interval_seconds=0, timeout_seconds=.02)
        self.assertIsNone(baseline)

    def test_active_provider_socket_is_not_quiet_baseline(self):
        value = tick()
        gateway = replace(value.processes["gateway"], fds=(
            FdIdentity(9, "socket:[9]", FdKind.PROVIDER_UDS_CONNECTED, 9),))
        class Sampler:
            def sample_all(self, ns):
                return replace(value, processes={**value.processes, "gateway": gateway})
        baseline, _ = acquire_stable_baseline(
            Sampler(), interval_seconds=0, timeout_seconds=.02)
        self.assertIsNone(baseline)

    def test_persisted_error_uses_stable_code_without_exception_body(self):
        class Sampler:
            def sample_all(self, ns):
                return tick(ns)
        def operation():
            raise RuntimeError("private-sentinel-body")
        result = record_operation(Sampler(), operation, sample_seconds=.01)
        with tempfile.TemporaryDirectory() as directory:
            persist_operation_outcome(Path(directory), result, "test")
            body = (Path(directory) / "operation_outcome.json").read_text()
        self.assertNotIn("private-sentinel-body", body)

    def test_invalid_first_snapshot_does_not_execute_workload(self):
        from unittest.mock import Mock
        for bad in (replace(tick(),unix_table_valid=False),
                    replace(tick(),processes={}),
                    replace(tick(),processes={r:replace(v,status=SnapshotStatus.PARTIAL) for r,v in tick().processes.items()})):
            sampler=Mock()
            sampler.sample_all.return_value=bad
            calls=[]
            result=record_operation(sampler,lambda:calls.append(1),sample_seconds=.001)
            self.assertEqual(calls,[])
            self.assertIsNotNone(result.sampling_error)

    def test_middle_non_oserror_stops_sampling_and_cannot_disappear(self):
        import time
        class Sampler:
            n=0
            def sample_all(self,ns):
                self.n+=1
                if self.n==3:
                    raise ValueError("middle")
                return tick(ns)
        result=record_operation(Sampler(),lambda:time.sleep(.02),sample_seconds=.001)
        self.assertEqual(result.sampling_error.phase,"sampling_middle")
        self.assertTrue(result.trace.ticks)

    def test_operation_interrupt_checkpoints_then_propagates(self):
        from unittest.mock import Mock
        sampler=Mock()
        sampler.sample_all.side_effect=lambda ns:tick(ns)
        saved=[]
        def interrupt():
            raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            record_operation(sampler,interrupt,sample_seconds=.001,on_checkpoint=saved.append)
        self.assertEqual(len(saved),1)
        self.assertTrue(saved[0].trace.ticks)


if __name__ == "__main__":
    unittest.main()
