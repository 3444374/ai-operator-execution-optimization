"""Read retained request observations without starting an HTTP proxy."""

import unittest

from src.experiments.saor.native_system_observation import (
    JobObservationContract,
    summarize_gateway_rows,
)


class GatewayTraceSummaryTests(unittest.TestCase):
    def test_existing_trace_summary_remains_available_without_a_proxy(self):
        row = dict(
            gateway_request_id=0,
            job_id="job0",
            endpoint_id="endpoint-0",
            retry_count=0,
            request_body_sha256="a" * 64,
            forwarded_body_sha256="a" * 64,
            status="completed",
            upstream_status=200,
            received_epoch_s=1.0,
            upstream_start_epoch_s=1.1,
            upstream_response_epoch_s=1.3,
            response_completed_epoch_s=1.4,
            dispatch_delay_s=0.1,
            actual_prompt_tokens=11,
            actual_output_tokens=7,
            actual_total_tokens=18,
        )
        summary = summarize_gateway_rows([row], (
            JobObservationContract("job0", "fixture", 1.0, 60.0),
        ))
        self.assertEqual(summary["status"], "passed")
        self.assertEqual(summary["gateway_integrity"]["request_count"], 1)


if __name__ == "__main__":
    unittest.main()
