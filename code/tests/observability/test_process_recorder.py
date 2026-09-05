"""Recorder tests: stable baseline, failure-safe recording, atomic persistence."""
import gzip
import json
import tempfile
import unittest
from pathlib import Path

from src.observability.process_resources.model import (
    FdIdentity, FdKind, ProcessSnapshot, ResourceTrace, SampleTick,
    SnapshotStatus)
from src.observability.process_resources.recorder import (
    acquire_stable_baseline,
    fd_lifecycles,
    persist_lifecycles,
    persist_operation_outcome,
    persist_trace,
    record_operation,
    write_atomic,
)


def _snap(pid, ns, fds, threads=1):
    return ProcessSnapshot(
        pid=pid, process_start_time_ticks=pid, monotonic_ns=ns,
        status=SnapshotStatus.VALID, rss_bytes=1_000_000,
        thread_count=threads, fds=tuple(fds))


def _fd(fd, target="pipe:[1]", kind=FdKind.PIPE):
    return FdIdentity(fd=fd, target=target, kind=kind)


class FakeTickSampler:
    """Deterministic sampler cycling a scripted tick sequence forever."""

    def __init__(self, ticks):
        self._ticks = list(ticks)
        self._index = 0

    def sample_all(self, monotonic_ns):
        tick = self._ticks[self._index % len(self._ticks)]
        self._index += 1
        return tick


def _tick(ns, backend_fds=(), gateway_fds=()):
    return SampleTick(
        monotonic_ns=ns, unix_table_valid=True,
        processes={"backend": _snap(1, ns, backend_fds),
                   "gateway": _snap(2, ns, gateway_fds)})


class StableBaselineTests(unittest.TestCase):
    def test_five_identical_ticks_accept_baseline(self):
        ticks = [_tick(n) for n in range(5)]
        baseline, observed = acquire_stable_baseline(
            FakeTickSampler(ticks), required_consecutive=5,
            interval_seconds=0, timeout_seconds=1)
        self.assertIsNotNone(baseline)
        self.assertEqual(baseline.baseline["backend"].pid, 1)

    def test_fd_identity_replacement_is_not_stable(self):
        # Count identical, identity different: must never stabilize.
        changing = [_tick(0, backend_fds=(_fd(18, "socket:[1]", FdKind.SOCKET_OTHER),)),
                    _tick(1, backend_fds=(_fd(18, "socket:[2]", FdKind.SOCKET_OTHER),))]
        baseline, _ = acquire_stable_baseline(
            FakeTickSampler(changing), required_consecutive=5,
            interval_seconds=0, timeout_seconds=0.3)
        self.assertIsNone(baseline, "fd replacement must break stability")

    def test_constant_tick_sequence_is_stable(self):
        # A single constant tick cycled forever is a legitimately stable
        # process; the baseline must be accepted, not time out.
        baseline, _ = acquire_stable_baseline(
            FakeTickSampler([_tick(0)]), required_consecutive=5,
            interval_seconds=0, timeout_seconds=1)
        self.assertIsNotNone(baseline)

    def test_endlessly_changing_ticks_time_out(self):
        ticks = [_tick(n, backend_fds=(_fd(18, f"socket:[{n}]", FdKind.SOCKET_OTHER),))
                 for n in range(50)]
        baseline, observed = acquire_stable_baseline(
            FakeTickSampler(ticks), required_consecutive=5,
            interval_seconds=0, timeout_seconds=0.2)
        self.assertIsNone(baseline)
        self.assertTrue(observed)


class RecordOperationTests(unittest.TestCase):
    def test_operation_failure_still_returns_trace(self):
        def failing():
            raise RuntimeError("sentinel")
        recorded = record_operation(
            FakeTickSampler([_tick(0)]), failing, sample_seconds=0)
        self.assertIsNotNone(recorded.trace)
        self.assertIsNotNone(recorded.operation_error)
        self.assertEqual(recorded.operation_error.exception_type, "RuntimeError")
        self.assertEqual(recorded.operation_error.message, "operation_error")

    def test_success_returns_result_and_final_sample(self):
        recorded = record_operation(
            FakeTickSampler([_tick(0)]), lambda: 42, sample_seconds=0)
        self.assertEqual(recorded.result, 42)
        self.assertIsNone(recorded.operation_error)
        self.assertGreaterEqual(len(recorded.trace.ticks), 2,
                                "pre-sample plus post-operation sample")


class PersistenceTests(unittest.TestCase):
    def test_persist_trace_writes_gzip_jsonl_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            trace = ResourceTrace(
                baseline={"backend": _snap(1, 0, (_fd(3),))},
                ticks=(_tick(1), _tick(2)))
            persist_trace(target, trace)
            artifact = target / "process_samples.jsonl.gz"
            self.assertTrue(artifact.exists())
            self.assertFalse(list(target.glob("*.partial")))
            with gzip.open(artifact, "rt", encoding="utf-8") as handle:
                lines = [json.loads(line) for line in handle]
            self.assertEqual(lines[0]["kind"], "baseline")
            self.assertEqual(lines[-1]["kind"], "tick")
            self.assertEqual(lines[0]["status"], "valid")
            self.assertEqual(lines[0]["fds"][0]["kind"], "pipe")

    def test_persist_lifecycles_records_reuse_separately(self):
        trace = ResourceTrace(
            baseline={"backend": _snap(1, 0, ())},
            ticks=(_tick(1, backend_fds=(_fd(18, "socket:[1]", FdKind.SOCKET_OTHER),)),
                   _tick(2),
                   _tick(3, backend_fds=(_fd(18, "socket:[2]", FdKind.SOCKET_OTHER),))))
        records = fd_lifecycles(trace)
        self.assertEqual(len(records), 2,
                         "same fd number with a different target is two lifecycles")
        with tempfile.TemporaryDirectory() as directory:
            persist_lifecycles(Path(directory), trace)
            self.assertTrue((Path(directory) / "fd_lifecycles.jsonl.gz").exists())

    def test_persist_operation_outcome_sanitizes(self):
        from src.observability.process_resources.model import CapturedError
        from src.observability.process_resources.model import RecordedOperation
        recorded = RecordedOperation(
            result=None,
            operation_error=CapturedError(
                exception_type="RuntimeError", sqlstate=None,
                message="secret-path/detail", phase="operation"),
            sampling_error=None,
            trace=ResourceTrace(baseline={"backend": _snap(1, 0, ())}, ticks=()))
        with tempfile.TemporaryDirectory() as directory:
            persist_operation_outcome(Path(directory), recorded, "stress")
            payload = json.loads(
                (Path(directory) / "operation_outcome.json").read_text(encoding="utf-8"))
        self.assertEqual(payload["operation_error"]["exception_type"], "RuntimeError")
        self.assertEqual(payload["phase"], "stress")

    def test_write_atomic_replaces_existing(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "x.json"
            write_atomic(path, "one")
            write_atomic(path, "two")
            self.assertEqual(path.read_text(encoding="utf-8"), "two")


if __name__ == "__main__":
    unittest.main()
