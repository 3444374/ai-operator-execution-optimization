from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.adaptive_admission import AimdAdmissionController  # noqa: E402
from src.scheduling.admission import DynamicAdmissionGate  # noqa: E402
from src.scheduling.models import (  # noqa: E402
    BatchRequest,
    CollectedSubmission,
    EndpointSnapshot,
    PayloadEnvelope,
    SubmissionCompletion,
    TopologySnapshot,
)
from src.scheduling.observations import (  # noqa: E402
    CachedMetricsObservationProvider,
    ServiceMetricsSnapshot,
)
from src.scheduling.routing import RoundRobinEndpointRouter  # noqa: E402
from src.scheduling.scheduler import SynchronousScheduler  # noqa: E402


class FakeClock:
    def __init__(self, current: float = 0.0):
        self.current = current

    def __call__(self) -> float:
        return self.current


class CachedObservationProviderTests(unittest.TestCase):
    def test_provider_samples_at_interval_and_marks_cached_value_stale(self) -> None:
        clock = FakeClock(1.0)
        samples = []

        def sample():
            samples.append(clock.current)
            return ServiceMetricsSnapshot(running=10, waiting=0, kv_usage=0.2)

        provider = CachedMetricsObservationProvider(
            sample,
            min_sample_interval_s=0.25,
            clock=clock,
        )

        first = provider.latest(inflight=2)
        clock.current = 1.1
        cached = provider.latest(inflight=3)
        clock.current = 1.3
        refreshed = provider.latest(inflight=4)

        self.assertEqual(samples, [1.0, 1.3])
        self.assertTrue(first.fresh)
        self.assertFalse(cached.fresh)
        self.assertTrue(refreshed.fresh)
        self.assertEqual(cached.inflight, 3)
        self.assertEqual(cached.waiting, 0)

    def test_scrape_failure_produces_fresh_missing_observation(self) -> None:
        provider = CachedMetricsObservationProvider(lambda: None, clock=FakeClock())

        observation = provider.latest(inflight=1)

        self.assertTrue(observation.fresh)
        self.assertIsNone(observation.running)
        self.assertIsNone(observation.waiting)
        self.assertIsNone(observation.kv_usage)


class DynamicAdmissionGateTests(unittest.TestCase):
    def test_gate_updates_window_and_records_typed_trace(self) -> None:
        snapshots = iter(
            [
                ServiceMetricsSnapshot(10, 0, 0.2),
                ServiceMetricsSnapshot(10, 2, 0.2),
            ]
        )
        clock = FakeClock(1.0)
        provider = CachedMetricsObservationProvider(
            lambda: next(snapshots),
            min_sample_interval_s=0.0,
            clock=clock,
        )
        traces = []
        gate = DynamicAdmissionGate(
            AimdAdmissionController(initial_window=4),
            provider,
            trace_sink=traces.append,
        )

        increased = gate.decide(inflight=3)
        clock.current = 2.0
        decreased = gate.decide(inflight=5)

        self.assertTrue(increased.allowed)
        self.assertEqual(increased.limit, 6)
        self.assertFalse(decreased.allowed)
        self.assertEqual(decreased.limit, 4)
        self.assertEqual([item.controller_action for item in traces], ["increase", "decrease"])
        self.assertEqual([item.inflight for item in traces], [3, 5])
        self.assertEqual(traces[1].waiting, 2)

    def test_gate_does_not_reapply_controller_to_cached_observation(self) -> None:
        clock = FakeClock(1.0)
        provider = CachedMetricsObservationProvider(
            lambda: ServiceMetricsSnapshot(10, 0, 0.2),
            min_sample_interval_s=1.0,
            clock=clock,
        )
        gate = DynamicAdmissionGate(AimdAdmissionController(initial_window=4), provider)

        first = gate.decide(inflight=0)
        clock.current = 1.1
        cached = gate.decide(inflight=0)

        self.assertEqual(first.limit, 6)
        self.assertEqual(cached.limit, 6)
        self.assertEqual(cached.reason, "stale_observation")

    def test_scheduler_never_exceeds_window_before_dynamic_downshift(self) -> None:
        class AdmissionClock:
            def __init__(self):
                self.calls = 0

            def __call__(self):
                self.calls += 1
                if self.calls == 1:
                    return 0.0
                if self.calls <= 6:
                    return 0.1
                return 0.3

        class ImmediateAdapter:
            def submit(self, envelope, endpoint_id):
                return envelope.request.request_id

            def wait_one(self, pending):
                handle, envelope = pending[0]
                return CollectedSubmission(
                    handle,
                    SubmissionCompletion(handle, "completed", envelope.payload),
                    0.0,
                    0.0,
                )

        snapshots = iter(
            [
                ServiceMetricsSnapshot(10, 0, 0.2),
                ServiceMetricsSnapshot(10, 2, 0.2),
            ]
        )
        gate = DynamicAdmissionGate(
            AimdAdmissionController(initial_window=4),
            CachedMetricsObservationProvider(
                lambda: next(snapshots),
                min_sample_interval_s=0.25,
                clock=AdmissionClock(),
            ),
        )
        topology = TopologySnapshot(
            (
                EndpointSnapshot(
                    "e1",
                    "ray://e1",
                    "default",
                    "0",
                    True,
                    0,
                    0,
                    None,
                    0.0,
                ),
            ),
            0.0,
        )
        envelopes = [
            PayloadEnvelope(
                BatchRequest(
                    f"r{index}",
                    "job",
                    "ai_complete",
                    1,
                    10,
                    5,
                    "",
                    0.0,
                    0.0,
                    f"p{index}",
                ),
                f"payload-{index}",
            )
            for index in range(8)
        ]

        result = SynchronousScheduler(
            gate,
            RoundRobinEndpointRouter(),
            ImmediateAdapter(),
            "default",
        ).run(envelopes, topology)

        self.assertEqual(len(result.completions), 8)
        self.assertEqual(result.max_inflight_seen, 6)
        self.assertEqual(result.applied_limit, 4)


if __name__ == "__main__":
    unittest.main()
