from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts import postgres_ai_operator_profile as profile  # noqa: E402
from src.scheduling.models import SubmissionCompletion  # noqa: E402
from src.scheduling.scheduler import SchedulerResult  # noqa: E402


class SchedulingProfileHelperTests(unittest.TestCase):
    def test_batch_envelopes_preserve_arrow_payload_and_compute_cost(self) -> None:
        batch = pa.table(
            {
                "doc_id": [1, 2],
                "prompt_tokens": [10, 20],
                "prefix_key": ["shared", "shared"],
                "arrival_time_s": [1.5, 2.0],
            }
        )

        envelopes = profile._batch_envelopes(
            [batch],
            job_id="job-1",
            operator="ai_complete",
            completion_max_tokens=8,
        )

        self.assertEqual(len(envelopes), 1)
        envelope = envelopes[0]
        self.assertIs(envelope.payload, batch)
        self.assertEqual(envelope.request.request_id, "job-1:batch:0")
        self.assertEqual(envelope.request.payload_id, "job-1:batch:0")
        self.assertEqual(envelope.request.row_count, 2)
        self.assertEqual(envelope.request.prompt_tokens, 30)
        self.assertEqual(envelope.request.estimated_output_tokens, 16)
        self.assertEqual(envelope.request.prefix_key, "shared")
        self.assertEqual(envelope.request.first_arrival_s, 1.5)
        self.assertEqual(envelope.request.oldest_arrival_s, 1.5)

    def test_endpoint_topology_pairs_ids_and_urls_in_default_pool(self) -> None:
        topology = profile._endpoint_topology(
            endpoint_ids=["endpoint-0", "endpoint-1"],
            endpoint_urls=["http://one", "http://two"],
        )

        self.assertEqual(
            [(item.endpoint_id, item.url) for item in topology.endpoints],
            [("endpoint-0", "http://one"), ("endpoint-1", "http://two")],
        )
        self.assertTrue(all(item.pool_id == "default" for item in topology.endpoints))
        self.assertTrue(all(item.gpu_id == "0" for item in topology.endpoints))
        self.assertTrue(all(item.healthy for item in topology.endpoints))

    def test_endpoint_topology_rejects_mismatched_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "same length"):
            profile._endpoint_topology(["endpoint-0"], [])

    def test_scheduler_metrics_preserve_existing_profiler_schema(self) -> None:
        result = SchedulerResult(
            completions=(SubmissionCompletion("request-1", "completed", result={"ok": True}),),
            operator_invocations=1,
            max_inflight_seen=1,
            applied_limit=4,
            bounded_wait_s=0.2,
            avg_bounded_wait_s=0.2,
            fanin_s=0.1,
            submit_s=0.05,
        )

        metrics = profile._scheduler_metrics(result)

        self.assertEqual(
            set(metrics),
            {
                "operator_invocations",
                "max_inflight",
                "bounded_wait_s",
                "avg_bounded_wait_s",
                "fanin_s",
                "submit_s",
                "adaptive_downshifts",
                "adaptive_upshifts",
                "adaptive_limit_mean",
            },
        )
        self.assertEqual(metrics["operator_invocations"], 1)
        self.assertEqual(metrics["max_inflight"], 1)
        self.assertEqual(metrics["adaptive_downshifts"], 0)
        self.assertEqual(metrics["adaptive_upshifts"], 0)
        self.assertEqual(metrics["adaptive_limit_mean"], 4)


if __name__ == "__main__":
    unittest.main()
