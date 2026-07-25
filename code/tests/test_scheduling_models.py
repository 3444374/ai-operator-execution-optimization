from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.models import (  # noqa: E402
    BatchRequest,
    EndpointSnapshot,
    PayloadEnvelope,
    TopologySnapshot,
)


class SchedulingModelTests(unittest.TestCase):
    def test_batch_request_rejects_non_positive_row_count(self) -> None:
        with self.assertRaisesRegex(ValueError, "row_count must be positive"):
            BatchRequest(
                request_id="r1",
                job_id="j1",
                operator="ai_complete",
                row_count=0,
                prompt_tokens=10,
                estimated_output_tokens=5,
                prefix_key="",
                first_arrival_s=1.0,
                oldest_arrival_s=1.0,
                payload_id="p1",
            )

    def test_payload_envelope_keeps_payload_out_of_request_metadata(self) -> None:
        request = BatchRequest(
            request_id="r1",
            job_id="j1",
            operator="ai_complete",
            row_count=2,
            prompt_tokens=10,
            estimated_output_tokens=6,
            prefix_key="prefix",
            first_arrival_s=2.0,
            oldest_arrival_s=1.0,
            payload_id="p1",
        )
        payload = object()

        envelope = PayloadEnvelope(request=request, payload=payload)

        self.assertEqual(request.estimated_total_tokens, 16)
        self.assertIs(envelope.payload, payload)

    def test_topology_snapshot_rejects_duplicate_endpoint_ids(self) -> None:
        endpoint = EndpointSnapshot(
            endpoint_id="e1",
            url="http://localhost:8000/v1/completions",
            pool_id="default",
            gpu_id="0",
            healthy=True,
            running=0,
            waiting=0,
            kv_usage=0.0,
            observed_at_s=1.0,
        )

        with self.assertRaisesRegex(ValueError, "endpoint_id values must be unique"):
            TopologySnapshot(endpoints=(endpoint, endpoint), observed_at_s=1.0)


if __name__ == "__main__":
    unittest.main()
