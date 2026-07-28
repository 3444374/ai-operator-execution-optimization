from __future__ import annotations

import sys
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts import postgres_ai_operator_profile as profile  # noqa: E402
from src.organizers import DaftOrganizer, OrganizerConfig  # noqa: E402
from src.scheduling.admission import StaticAdmissionController  # noqa: E402
from src.scheduling.lifecycle import MonotonicEpochClock  # noqa: E402
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
    def test_output_aware_bfd_executes_through_task_and_actor(self) -> None:
        import ray

        table = pa.table(
            {
                "doc_id": [1, 2, 3, 4, 5],
                "prompt_tokens": [6, 5, 4, 3, 2],
                "target_output_tokens": [0, 0, 0, 0, 0],
                "prefix_key": ["", "", "", "", ""],
            }
        )
        organized = DaftOrganizer(
            OrganizerConfig(
                batch_size=3,
                runner="native",
                batching_policy="best_fit_token_budget",
                token_budget=10,
                output_cost_mode="prompt_only",
            )
        ).organize(table)
        job_start_epoch_s = time.time() - 1.0
        ready_epoch_s = time.time()
        envelopes, seeds = profile._offline_batch_envelopes(
            organized.batches,
            job_id="bfd-contract",
            operator="ai_complete",
            completion_max_tokens=8,
            output_cost_mode="prompt_only",
            batch_index_start=0,
            job_start_epoch_s=job_start_epoch_s,
            ready_epoch_s=ready_epoch_s,
        )

        @ray.remote
        def execute_task(
            payload,
            endpoint_url,
            model_name,
            api_key,
            timeout_s,
            completion_max_tokens,
        ):
            del endpoint_url, model_name, api_key, timeout_s
            service_start_epoch_s = time.time()
            doc_ids = payload.column("doc_id").to_pylist()
            return {
                "doc_id": doc_ids,
                "output_text": [
                    f"task-{doc_id}" for doc_id in doc_ids
                ],
                "service_start_epoch_s": service_start_epoch_s,
                "service_end_epoch_s": time.time(),
                "completion_max_tokens": completion_max_tokens,
            }

        @ray.remote
        class ExecuteActor:
            def execute_batch(self, payload):
                service_start_epoch_s = time.time()
                doc_ids = payload.column("doc_id").to_pylist()
                return {
                    "doc_id": doc_ids,
                    "output_text": [
                        f"actor-{doc_id}" for doc_id in doc_ids
                    ],
                    "service_start_epoch_s": service_start_epoch_s,
                    "service_end_epoch_s": time.time(),
                }

        ray.init(ignore_reinit_error=True, num_cpus=2)
        try:
            task_events = []
            task_results, _ = profile.submit_ray_tasks(
                ray_module=ray,
                remote_embed=execute_task,
                batches=[],
                max_inflight=2,
                operator="ai_complete",
                embedding_dim=0,
                model_backend="vllm",
                endpoint_urls=["contract://local"],
                model_name="contract-only",
                api_key=None,
                timeout_s=1.0,
                completion_max_tokens=8,
                replay_envelopes=envelopes,
                submission_lifecycle_sink=task_events,
            )
            actor_events = []
            actor_results, _ = profile.submit_with_backpressure(
                ray_module=ray,
                actors=[ExecuteActor.remote()],
                batches=[],
                max_inflight=2,
                method_name="execute_batch",
                replay_envelopes=envelopes,
                submission_lifecycle_sink=actor_events,
            )
        finally:
            ray.shutdown()

        task_groups = [item["doc_id"] for item in task_results]
        actor_groups = [item["doc_id"] for item in actor_results]
        self.assertEqual(task_groups, [[1, 3], [2, 4, 5]])
        self.assertEqual(actor_groups, task_groups)
        self.assertEqual(
            sorted(doc_id for group in task_groups for doc_id in group),
            [1, 2, 3, 4, 5],
        )
        task_request_rows = profile._build_profiler_request_rows(
            seeds,
            task_events,
            task_results,
            operator="ai_complete",
            slo_target_s=None,
        )
        actor_request_rows = profile._build_profiler_request_rows(
            seeds,
            actor_events,
            actor_results,
            operator="ai_complete",
            slo_target_s=None,
        )
        for rows in (task_request_rows, actor_request_rows):
            self.assertEqual(len(rows), 5)
            self.assertTrue(
                all(
                    row.request_time_origin == "offline_job_start"
                    for row in rows
                )
            )

    def test_arrival_replay_executes_exactly_once_through_task_and_actor(self) -> None:
        import ray

        table = pa.table(
            {
                "doc_id": [1, 2, 3, 4],
                "prompt_tokens": [10, 10, 10, 10],
                "arrival_time_s": [0.0, 0.0, 20.0, 100.0],
                "prefix_key": ["shared", "shared", "shared", "other"],
            }
        )
        daft_tables = DaftOrganizer(
            OrganizerConfig(batch_size=4, runner="native")
        ).organize(table).batches
        replay_args = SimpleNamespace(
            batching_policy="fixed_rows",
            arrival_time_scale=0.001,
            completion_max_tokens=0,
            flush_max_wait_ms=50.0,
            flush_policy="fixed_timeout",
            flush_timeout_ms=60.0,
            ray_batch_rows=4,
            strategy="coalesced",
            submission_granularity="batch",
            token_budget=0,
        )

        @ray.remote
        def execute_task(
            payload,
            endpoint_url,
            model_name,
            api_key,
            timeout_s,
            completion_max_tokens,
        ):
            del endpoint_url, model_name, api_key, timeout_s, completion_max_tokens
            service_start_epoch_s = time.time()
            doc_ids = payload.column("doc_id").to_pylist()
            return {
                "doc_id": doc_ids,
                "output_text": [f"task-{doc_id}" for doc_id in doc_ids],
                "service_start_epoch_s": service_start_epoch_s,
                "service_end_epoch_s": time.time(),
                "executor": "task",
            }

        @ray.remote
        class ExecuteActor:
            def execute_batch(self, payload):
                service_start_epoch_s = time.time()
                doc_ids = payload.column("doc_id").to_pylist()
                return {
                    "doc_id": doc_ids,
                    "output_text": [f"actor-{doc_id}" for doc_id in doc_ids],
                    "service_start_epoch_s": service_start_epoch_s,
                    "service_end_epoch_s": time.time(),
                    "executor": "actor",
                }

        def replay(trace, lifecycle_seeds, epoch_clock):
            return profile._arrival_replay_envelopes(
                daft_tables,
                replay_args,
                job_id="arrival-contract",
                operator="ai_complete",
                service_observation=lambda: None,
                trace_sink=trace,
                lifecycle_seed_sink=lifecycle_seeds,
                epoch_clock=epoch_clock,
            )

        ray.init(ignore_reinit_error=True, num_cpus=2)
        try:
            task_trace = []
            task_seeds = []
            task_submission_events = []
            task_epoch_clock = MonotonicEpochClock()
            task_results, task_metrics = profile.submit_ray_tasks(
                ray_module=ray,
                remote_embed=execute_task,
                batches=[],
                max_inflight=2,
                operator="ai_complete",
                embedding_dim=0,
                model_backend="vllm",
                endpoint_urls=["contract://local"],
                model_name="contract-only",
                api_key=None,
                timeout_s=1.0,
                completion_max_tokens=0,
                replay_envelopes=replay(
                    task_trace,
                    task_seeds,
                    task_epoch_clock,
                ),
                submission_lifecycle_sink=task_submission_events,
                epoch_clock=task_epoch_clock,
            )

            actor_trace = []
            actor_seeds = []
            actor_submission_events = []
            actor_epoch_clock = MonotonicEpochClock()
            actor_results, actor_metrics = profile.submit_with_backpressure(
                ray_module=ray,
                actors=[ExecuteActor.remote()],
                batches=[],
                max_inflight=2,
                method_name="execute_batch",
                replay_envelopes=replay(
                    actor_trace,
                    actor_seeds,
                    actor_epoch_clock,
                ),
                submission_lifecycle_sink=actor_submission_events,
                epoch_clock=actor_epoch_clock,
            )
        finally:
            ray.shutdown()

        expected_groups = [[1, 2, 3], [4]]
        self.assertEqual(
            [item["doc_id"] for item in task_results],
            expected_groups,
        )
        self.assertEqual(
            [item["doc_id"] for item in actor_results],
            expected_groups,
        )
        self.assertEqual(task_metrics["operator_invocations"], 2)
        self.assertEqual(actor_metrics["operator_invocations"], 2)
        self.assertEqual(
            sorted(doc_id for item in task_results for doc_id in item["doc_id"]),
            [1, 2, 3, 4],
        )
        self.assertEqual(
            sorted(doc_id for item in actor_results for doc_id in item["doc_id"]),
            [1, 2, 3, 4],
        )
        task_request_rows = profile._build_profiler_request_rows(
            task_seeds,
            task_submission_events,
            task_results,
            operator="ai_complete",
            slo_target_s=None,
        )
        actor_request_rows = profile._build_profiler_request_rows(
            actor_seeds,
            actor_submission_events,
            actor_results,
            operator="ai_complete",
            slo_target_s=None,
        )
        for request_rows, submission_events in (
            (task_request_rows, task_submission_events),
            (actor_request_rows, actor_submission_events),
        ):
            self.assertEqual(len(request_rows), 4)
            self.assertEqual(
                len({row.request_id for row in request_rows}),
                4,
            )
            self.assertEqual(
                {row.doc_id for row in request_rows},
                {"1", "2", "3", "4"},
            )
            self.assertTrue(
                {row.submission_id for row in request_rows}.issubset(
                    {event.submission_id for event in submission_events}
                )
            )
            self.assertTrue(
                all(row.e2e_s >= 0 for row in request_rows)
            )
            self.assertTrue(
                all(
                    row.latency_granularity == "submission"
                    for row in request_rows
                )
            )
        self.assertGreaterEqual(task_trace[-1].elapsed_s, 0.075)
        self.assertLess(task_trace[-1].elapsed_s, 5.0)
        self.assertGreaterEqual(actor_trace[-1].elapsed_s, 0.075)
        self.assertLess(actor_trace[-1].elapsed_s, 5.0)

    def test_request_replay_executes_one_row_per_ray_submission(self) -> None:
        import ray

        table = pa.table(
            {
                "doc_id": [1, 2, 3],
                "prompt_tokens": [10, 10, 10],
                "arrival_time_s": [0.0, 0.0, 0.0],
                "prefix_key": ["shared", "shared", "shared"],
            }
        )
        replay_args = SimpleNamespace(
            batching_policy="fixed_rows",
            arrival_time_scale=0.001,
            completion_max_tokens=0,
            flush_max_wait_ms=50.0,
            flush_policy="fixed_timeout",
            flush_timeout_ms=60.0,
            ray_batch_rows=8,
            strategy="coalesced",
            submission_granularity="request",
            token_budget=0,
        )
        envelopes = profile._arrival_replay_envelopes(
            [table],
            replay_args,
            job_id="request-contract",
            operator="ai_complete",
            service_observation=lambda: None,
            trace_sink=[],
            lifecycle_seed_sink=[],
            epoch_clock=MonotonicEpochClock(),
        )

        @ray.remote
        def execute_task(
            payload,
            endpoint_url,
            model_name,
            api_key,
            timeout_s,
            completion_max_tokens,
        ):
            del (
                endpoint_url,
                model_name,
                api_key,
                timeout_s,
                completion_max_tokens,
            )
            doc_ids = payload.column("doc_id").to_pylist()
            return {
                "doc_id": doc_ids,
                "output_text": [f"task-{doc_id}" for doc_id in doc_ids],
                "service_start_epoch_s": time.time(),
                "service_end_epoch_s": time.time(),
            }

        ray.init(ignore_reinit_error=True, num_cpus=2)
        try:
            results, metrics = profile.submit_ray_tasks(
                ray_module=ray,
                remote_embed=execute_task,
                batches=[],
                max_inflight=2,
                operator="ai_complete",
                embedding_dim=0,
                model_backend="vllm",
                endpoint_urls=["contract://local"],
                model_name="contract-only",
                api_key=None,
                timeout_s=1.0,
                completion_max_tokens=0,
                replay_envelopes=envelopes,
            )
        finally:
            ray.shutdown()

        self.assertEqual(
            sorted(item["doc_id"] for item in results),
            [[1], [2], [3]],
        )
        self.assertEqual(metrics["operator_invocations"], 3)

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

    def test_daft_batches_execute_through_profiler_task_and_actor_paths(self) -> None:
        import ray

        table = pa.table(
            {
                "doc_id": [1, 2, 3, 4],
                "prompt_tokens": [10, 20, 30, 40],
            }
        )
        batches = DaftOrganizer(
            OrganizerConfig(batch_size=2, runner="native")
        ).organize(table).batches

        @ray.remote
        def execute_task(payload, embedding_dim):
            return {"rows": payload.num_rows, "embedding_dim": embedding_dim}

        @ray.remote
        class ExecuteActor:
            def __init__(self, label):
                self.label = label

            def execute_batch(self, payload):
                return {
                    "rows": payload.num_rows,
                    "executor": "actor",
                    "pool": self.label,
                }

        ray.init(ignore_reinit_error=True, num_cpus=1)
        try:
            task_results, task_metrics = profile.submit_ray_tasks(
                ray_module=ray,
                remote_embed=execute_task,
                batches=batches,
                max_inflight=2,
                operator="ai_embed",
                embedding_dim=16,
                model_backend="fake",
                endpoint_urls=[],
                model_name="unused",
                api_key=None,
                timeout_s=1.0,
                completion_max_tokens=0,
            )
            actor_results, actor_metrics = profile.submit_with_backpressure(
                ray_module=ray,
                actors=[
                    ExecuteActor.remote("short"),
                    ExecuteActor.remote("long"),
                ],
                batches=batches,
                max_inflight=2,
                method_name="execute_batch",
                routing_config=profile._build_routing_config(
                    endpoint_count=2,
                    endpoint_routing="least_queued",
                    pool_routing="request_cost",
                    pool_ids_text="short,long",
                    gpu_ids_text="0,0",
                    long_request_tokens=50,
                ),
            )
        finally:
            ray.shutdown()

        self.assertEqual(
            task_results,
            [
                {"rows": 2, "embedding_dim": 16},
                {"rows": 2, "embedding_dim": 16},
            ],
        )
        self.assertEqual(
            actor_results,
            [
                {"rows": 2, "executor": "actor", "pool": "short"},
                {"rows": 2, "executor": "actor", "pool": "long"},
            ],
        )
        self.assertEqual(task_metrics["operator_invocations"], 2)
        self.assertEqual(actor_metrics["operator_invocations"], 2)


if __name__ == "__main__":
    unittest.main()
