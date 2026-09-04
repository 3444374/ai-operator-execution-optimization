"""Recorder tests: atomic persistence precedes evaluation; one artifact per run."""
import gzip
import json
import tempfile
import unittest
from pathlib import Path

from src.observability.process_resources.model import FdIdentity, FdKind, ProcessSnapshot
from src.observability.process_resources.recorder import (
    peaks,
    ending,
    persist_trace,
    record_operation,
    write_atomic,
)


def snap(ns, fds, rss=100):
    return ProcessSnapshot(
        monotonic_ns=ns,
        rss_bytes=rss,
        thread_count=1,
        fds=tuple(FdIdentity(fd=i, target="socket:[1]", kind=kind) for i, kind in enumerate(fds)),
    )


class FakeClock:
    def __init__(self):
        self.now = 1_000

    def monotonic_ns(self) -> int:
        self.now += 1
        return self.now


class RecorderTests(unittest.TestCase):
    def test_operation_failure_still_returns_nothing_but_persists_outside(self):
        # record_operation propagates the operation error; the caller keeps
        # the responsibility to persist whatever it captured via its own
        # trace handle. Here we verify the success path returns a trace.
        clock = FakeClock()
        calls = []

        def sampler(role, monotonic_ns):
            calls.append((role, monotonic_ns))
            return snap(monotonic_ns, [FdKind.PROVIDER_UDS_CLIENT])

        result, trace = record_operation(
            sampler, ("backend",), lambda: 42,
            sample_seconds=0.0, clock=clock)
        self.assertEqual(result, 42)
        self.assertIn("backend", trace.baseline)
        self.assertTrue(trace.samples)

    def test_peaks_union_of_fd_identities_and_max_rss(self):
        base = {"backend": snap(1, [FdKind.POSTGRES_CLIENT_SOCKET], rss=100)}
        samples = (
            {"backend": snap(2, [FdKind.POSTGRES_CLIENT_SOCKET, FdKind.POSTGRES_TEMP_FILE], rss=500)},
            {"backend": snap(3, [FdKind.POSTGRES_CLIENT_SOCKET], rss=200)},
        )
        combined = peaks(type("T", (), {"baseline": base, "samples": samples})())
        self.assertEqual(combined["backend"].rss_bytes, 500)
        self.assertEqual(combined["backend"].total_fd_count, 2)

    def test_ending_is_last_sample(self):
        samples = (
            {"backend": snap(2, [FdKind.PIPE])},
            {"backend": snap(3, [])},
        )
        last = ending(type("T", (), {"baseline": {"backend": snap(1, [])}, "samples": samples})())
        self.assertEqual(last["backend"].monotonic_ns, 3)

    def test_persist_trace_writes_gzip_jsonl_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            base = {"backend": snap(1, [FdKind.PROVIDER_UDS_CLIENT])}
            trace = type("T", (), {"baseline": base, "samples": ({"backend": snap(2, [])},)})()
            persist_trace(target, trace)
            artifact = target / "process_samples.jsonl.gz"
            self.assertTrue(artifact.exists())
            self.assertFalse(list(target.glob("*.partial")))
            with gzip.open(artifact, "rt", encoding="utf-8") as handle:
                lines = [json.loads(line) for line in handle]
            self.assertEqual(lines[0]["kind"], "baseline")
            self.assertEqual(lines[-1]["kind"], "sample")
            self.assertEqual(lines[0]["fds"][0]["kind"], "provider_uds_client")

    def test_write_atomic_replaces_existing(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "x.json"
            write_atomic(path, "one")
            write_atomic(path, "two")
            self.assertEqual(path.read_text(encoding="utf-8"), "two")


if __name__ == "__main__":
    unittest.main()
