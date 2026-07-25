from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.organizers import DaftOrganizer, OrganizerConfig  # noqa: E402
from src.scheduling.admission import StaticAdmissionController  # noqa: E402
from src.scheduling.models import (  # noqa: E402
    BatchRequest,
    EndpointSnapshot,
    PayloadEnvelope,
    TopologySnapshot,
)
from src.scheduling.ray_adapter import RaySubmissionAdapter  # noqa: E402
from src.scheduling.routing import RoundRobinEndpointRouter  # noqa: E402
from src.scheduling.scheduler import SynchronousScheduler  # noqa: E402


class DaftRayContractTests(unittest.TestCase):
    def test_daft_arrow_batches_execute_through_ray_adapter(self) -> None:
        import ray

        table = pa.table(
            {
                "doc_id": [1, 2, 3, 4],
                "tenant_id": [1, 1, 1, 1],
                "category": ["a", "a", "b", "b"],
                "text": ["one", "two", "three", "four"],
                "prompt_tokens": [1, 1, 1, 1],
                "target_output_tokens": [1, 1, 1, 1],
                "arrival_time_s": [0.0, 0.1, 0.2, 0.3],
                "session_id": ["s1", "s1", "s2", "s2"],
                "prefix_key": ["", "", "", ""],
            }
        )
        organized = DaftOrganizer(
            OrganizerConfig(batch_size=2, runner="native")
        ).organize(table)

        @ray.remote
        def execute(payload, endpoint_id):
            return {
                "rows": payload.num_rows,
                "endpoint_id": endpoint_id,
            }

        envelopes = [
            PayloadEnvelope(
                BatchRequest(
                    request_id=f"r{index}",
                    job_id="j1",
                    operator="ai_complete",
                    row_count=batch.num_rows,
                    prompt_tokens=batch.num_rows,
                    estimated_output_tokens=batch.num_rows,
                    prefix_key="",
                    first_arrival_s=0.0,
                    oldest_arrival_s=0.0,
                    payload_id=f"p{index}",
                ),
                batch,
            )
            for index, batch in enumerate(organized.batches)
        ]
        topology = TopologySnapshot(
            (
                EndpointSnapshot(
                    "e1",
                    "http://localhost:8000/v1/completions",
                    "default",
                    "0",
                    True,
                    0,
                    0,
                    0.0,
                    1.0,
                ),
            ),
            1.0,
        )

        ray.init(ignore_reinit_error=True, num_cpus=1)
        try:
            result = SynchronousScheduler(
                StaticAdmissionController(2),
                RoundRobinEndpointRouter(),
                RaySubmissionAdapter(
                    ray,
                    {"e1": lambda payload: execute.remote(payload, "e1")},
                ),
                "default",
            ).run(envelopes, topology)
        finally:
            ray.shutdown()

        self.assertEqual([item.result["rows"] for item in result.completions], [2, 2])
        self.assertEqual(
            [item.result["endpoint_id"] for item in result.completions],
            ["e1", "e1"],
        )


if __name__ == "__main__":
    unittest.main()
