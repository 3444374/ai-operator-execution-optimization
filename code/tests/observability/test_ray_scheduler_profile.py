"""Current Ray scheduler/metrics behavior retained from the retired SQL profiler harness."""
import unittest
import pyarrow as pa
from src.observability.profiling import ray_scheduler as profile_ray_scheduler
from src.observability.profiling import batch_envelopes as profile_batches
from src.scheduling.core.models import SubmissionCompletion
from src.scheduling.core.scheduler import SchedulerResult
from src.scheduling.submission_control.admission import StaticAdmissionController

class _ImmediateRef:
    def __init__(self, result: object):
        self.result = result

class _ImmediateRay:
    @staticmethod
    def wait(handles, num_returns, timeout=None):
        del timeout
        return handles[:num_returns], handles[num_returns:]

    @staticmethod
    def get(handle):
        if isinstance(handle, list):
            return [item.result for item in handle]
        return handle.result

class _RecordingRemote:
    def __init__(self):
        self.calls = []

    def remote(self, *args):
        self.calls.append(args)
        return _ImmediateRef({"call_index": len(self.calls) - 1})


class RaySchedulerProfileTests(unittest.TestCase):
    def test_endpoint_topology_pairs_ids_and_urls_in_default_pool(self) -> None:
        topology = profile_ray_scheduler._endpoint_topology(
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
            profile_ray_scheduler._endpoint_topology(["endpoint-0"], [])


    def test_endpoint_topology_preserves_pool_and_gpu_assignments(self) -> None:
        topology = profile_ray_scheduler._endpoint_topology(
            ["endpoint-0", "endpoint-1"],
            ["http://one", "http://two"],
            pool_ids=["short", "long"],
            gpu_ids=["0", "1"],
        )

        self.assertEqual(
            [(item.pool_id, item.gpu_id) for item in topology.endpoints],
            [("short", "0"), ("long", "1")],
        )


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

        metrics = profile_ray_scheduler._scheduler_metrics(result)

        self.assertEqual(
            set(metrics),
            {
                "operator_invocations",
                "max_inflight",
                "max_active_work_per_endpoint_seen",
                "max_ready_requests_seen",
                "max_ready_work_seen",
                "max_ready_payload_bytes_seen",
                "ready_requests_transition_samples",
                "ready_work_transition_samples",
                "ready_payload_bytes_transition_samples",
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


    def test_bounded_ready_window_is_derived_from_existing_k_and_w(self) -> None:
        self.assertEqual(
            profile_ray_scheduler._shared_ready_window_limits(
                256,
                ["endpoint-0", "endpoint-1"],
                {
                    "policy": "fifo",
                    "work_limit": 65536,
                    "ready_observation_contract": (
                        "bounded_concrete_pre_registration"
                    ),
                    "ready_payload_bytes_limit": 1048576,
                },
            ),
            (256, 131072, 1048576),
        )
        self.assertEqual(
            profile_ray_scheduler._shared_ready_window_limits(
                256,
                ["endpoint-0", "endpoint-1"],
                {"policy": "saor_bounded_priority", "work_limit": 65536},
            ),
            (1, None, None),
        )


    def test_arrow_batch_requests_preserve_costs_locality_and_arrivals(self) -> None:
        values = {"prompt_tokens": [3, 5], "target_output_tokens": [2, 9],
                  "prefix_key": ["shared", "shared"], "preferred_endpoint_id": ["e0", "e0"],
                  "arrival_time_s": [2.0, 1.0]}
        batch = pa.table(values)
        mixed = pa.table({**values, "prefix_key": ["left", "right"],
                          "preferred_endpoint_id": ["e0", "e1"]})
        for mode, output_tokens in (("prompt_only", 0), ("fixed_output_cap", 16),
                                    ("trace_target_output", 10)):
            with self.subTest(mode=mode):
                envelopes = profile_batches._batch_envelopes(
                    [batch, mixed], job_id="job", operator="ai_complete",
                    completion_max_tokens=8, output_cost_mode=mode,
                    batch_index_start=4, prompt_token_overhead_per_request=2)
                self.assertEqual([e.request.request_id for e in envelopes],
                                 ["job:batch:4", "job:batch:5"])
                self.assertIs(envelopes[0].payload, batch)
                request = envelopes[0].request
                self.assertEqual((request.row_count, request.prompt_tokens,
                                  request.estimated_output_tokens), (2, 12, output_tokens))
                self.assertEqual((request.prefix_key, request.preferred_endpoint_id), ("shared", "e0"))
                self.assertEqual((request.first_arrival_s, request.oldest_arrival_s), (1.0, 1.0))
                self.assertEqual((request.planning_batch_id, request.payload_id),
                                 ("job:batch:4", "job:batch:4"))
                self.assertEqual(request.estimated_payload_bytes, batch.nbytes)
                self.assertEqual((request.service_quantum_index, request.service_quantum_oversized),
                                 (-1, False))
                self.assertEqual((envelopes[1].request.prefix_key,
                                  envelopes[1].request.preferred_endpoint_id), ("", ""))

    def test_run_scheduler_consumes_a_single_pass_lazy_iterable(self) -> None:
        batch = pa.table({"doc_id": [1], "prompt_tokens": [3]})
        envelope = profile_batches._batch_envelopes(
            [batch],
            job_id="job",
            operator="ai_embed",
            completion_max_tokens=0,
        )[0]
        consumed = []

        def envelopes():
            consumed.append("started")
            yield envelope

        remote = _RecordingRemote()
        topology = profile_ray_scheduler._endpoint_topology(
            ["endpoint-0"],
            ["ray://task/0"],
        )

        lifecycle_events = []
        results, metrics = profile_ray_scheduler._run_scheduler_with_options(
            _ImmediateRay,
            envelopes(),
            topology,
            {"endpoint-0": lambda payload: remote.remote(payload)},
            StaticAdmissionController(1),
            profile_ray_scheduler._SchedulerOptions(
                submission_lifecycle_sink=lifecycle_events,
            ),
        )

        self.assertEqual(consumed, ["started"])
        self.assertEqual(results, [{"call_index": 0}])
        self.assertEqual(metrics["operator_invocations"], 1)
        self.assertEqual(
            [event.submission_id for event in lifecycle_events],
            ["job:batch:0"],
        )
