from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

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


class _ImmediateRef:
    def __init__(self, result: object):
        self.result = result


class _ImmediateRay:
    @staticmethod
    def wait(handles, num_returns):
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


class StaticTaskSchedulingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.batches = [
            pa.table({"doc_id": [1], "prompt_tokens": [10]}),
            pa.table({"doc_id": [2], "prompt_tokens": [20]}),
        ]

    def _submit(self, remote, **overrides):
        arguments = {
            "ray_module": _ImmediateRay,
            "remote_embed": remote,
            "batches": self.batches,
            "max_inflight": 2,
            "operator": "ai_embed",
            "embedding_dim": 16,
            "model_backend": "fake",
            "endpoint_urls": [],
            "model_name": "model",
            "api_key": None,
            "timeout_s": 5.0,
            "completion_max_tokens": 8,
            "adaptive_config": None,
        }
        arguments.update(overrides)
        return profile.submit_ray_tasks(**arguments)

    def test_static_task_path_delegates_to_shared_scheduler(self) -> None:
        remote = _RecordingRemote()
        expected = ([{"ok": True}], {"operator_invocations": 1})

        with patch.object(profile, "_run_static_scheduler", return_value=expected) as run:
            actual = self._submit(remote)

        self.assertEqual(actual, expected)
        run.assert_called_once()

    def test_fake_task_submitter_preserves_operator_arguments(self) -> None:
        remote = _RecordingRemote()

        results, metrics = self._submit(remote)

        self.assertEqual(results, [{"call_index": 0}, {"call_index": 1}])
        self.assertEqual(
            [(call[0], call[1]) for call in remote.calls],
            [(self.batches[0], 16), (self.batches[1], 16)],
        )
        self.assertEqual(metrics["operator_invocations"], 2)
        self.assertEqual(metrics["max_inflight"], 2)
        self.assertEqual(metrics["adaptive_downshifts"], 0)

    def test_http_task_submitters_route_across_endpoint_urls(self) -> None:
        remote = _RecordingRemote()

        self._submit(
            remote,
            model_backend="compatible_http",
            endpoint_urls=["http://one", "http://two"],
        )

        self.assertEqual(
            [call[1] for call in remote.calls],
            ["http://one", "http://two"],
        )
        self.assertTrue(all(call[2:] == ("model", None, 5.0) for call in remote.calls))

    def test_adaptive_task_path_remains_isolated_from_static_scheduler(self) -> None:
        remote = _RecordingRemote()
        adaptive_config = {}

        with patch.object(profile, "_run_static_scheduler") as run:
            self._submit(remote, adaptive_config=adaptive_config)

        run.assert_not_called()


class _RecordingActor:
    def __init__(self):
        self.execute_batch = _RecordingRemote()


class StaticActorSchedulingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.batches = [
            pa.table({"doc_id": [1], "prompt_tokens": [10]}),
            pa.table({"doc_id": [2], "prompt_tokens": [20]}),
            pa.table({"doc_id": [3], "prompt_tokens": [30]}),
        ]

    def _submit(self, actors, adaptive_config=None):
        return profile.submit_with_backpressure(
            ray_module=_ImmediateRay,
            actors=actors,
            batches=self.batches,
            max_inflight=2,
            method_name="execute_batch",
            adaptive_config=adaptive_config,
        )

    def test_static_actor_path_delegates_to_shared_scheduler(self) -> None:
        actors = [_RecordingActor()]
        expected = ([{"ok": True}], {"operator_invocations": 1})

        with patch.object(profile, "_run_static_scheduler", return_value=expected) as run:
            actual = self._submit(actors)

        self.assertEqual(actual, expected)
        run.assert_called_once()

    def test_actor_submitters_round_robin_across_actor_pool(self) -> None:
        actors = [_RecordingActor(), _RecordingActor()]

        results, metrics = self._submit(actors)

        self.assertEqual(
            [call[0] for call in actors[0].execute_batch.calls],
            [self.batches[0], self.batches[2]],
        )
        self.assertEqual(
            [call[0] for call in actors[1].execute_batch.calls],
            [self.batches[1]],
        )
        self.assertEqual(len(results), 3)
        self.assertEqual(metrics["operator_invocations"], 3)
        self.assertEqual(metrics["max_inflight"], 2)

    def test_static_actor_path_requires_at_least_one_actor(self) -> None:
        with self.assertRaisesRegex(ValueError, "actors must not be empty"):
            self._submit([])

    def test_adaptive_actor_path_remains_isolated_from_static_scheduler(self) -> None:
        actors = [_RecordingActor()]

        with patch.object(profile, "_run_static_scheduler") as run:
            self._submit(actors, adaptive_config={})

        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
