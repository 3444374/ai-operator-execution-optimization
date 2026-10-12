"""Actual Ray HTTP actors and Daft/Arrow payloads against independent loopback fixtures."""

from collections import Counter
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest

from src.execution_provider.adapters.model_replicas import replica_usage
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.scheduling.core.session_contract import State, Usage
from tests.postgres import test_semloom_model_replicas as fixtures


_AVAILABLE = all(importlib.util.find_spec(name) is not None for name in ("ray", "daft", "pyarrow"))


@unittest.skipUnless(_AVAILABLE and os.environ.get("SEMLOOM_REPLICA_RAY_TEST") == "1",
                     "requires the explicitly enabled actual Ray/Daft CPU fixture")
class ModelReplicaRayIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import ray
        if ray.is_initialized():
            raise RuntimeError("actual replica fixture must own its isolated Ray cluster")
        os.environ.pop("RAY_ADDRESS", None)
        cls.ray = ray
        cls.temp_path = tempfile.mkdtemp(prefix="rpr-", dir=os.environ.get("SEMLOOM_REPLICA_RAY_TMP", "/tmp"))
        cpus = os.environ.get("SEMLOOM_REPLICA_CPUSET")
        if cpus:
            expected = {int(value) for value in cpus.split(",")}
            if set(os.sched_getaffinity(0)) != expected:
                raise RuntimeError("actual replica fixture CPU allocation differs")
        try:
            cls.ray.init(num_cpus=8, num_gpus=0, include_dashboard=False,
                         object_store_memory=256 * 1024 * 1024, _node_ip_address="127.0.0.1",
                         _temp_dir=cls.temp_path,
                         runtime_env={"env_vars": {"PYTHONPATH": str(Path(__file__).resolve().parents[2]),
                                                   "CUDA_VISIBLE_DEVICES": "", "OMP_NUM_THREADS": "1"}})
        except BaseException:
            cls.ray.shutdown()
            raise
        cls.address = ray.get_runtime_context().gcs_address

    @classmethod
    def tearDownClass(cls):
        cls.ray.shutdown()
        # Keep this run's Ray logs for diagnosis; only its processes are stopped.

    def setUp(self):
        self.fixture = fixtures.ModelReplicaTests("test_real_urls_receive_exact_occurrences_and_complete_responses_keep_identity")
        self.fixture.setUp()

    def tearDown(self):
        self.fixture.tearDown()

    def physical(self, replicas, backend):
        return {replica.endpoint_id: RayMapConfig(self.address, 1, 1, MAX_FRAME_BYTES + 24,
            2 * MAX_FRAME_BYTES, payload_backend=backend, response_mode="full") for replica in replicas.replicas}

    def test_actual_payload_backends_and_two_ray_actors_keep_capacity_calls_and_full_responses(self):
        for backend in ("daft", "arrow"):
            with self.subTest(payload_backend=backend):
                a, b = self.fixture.site("a", hold=True), self.fixture.site("b", hold=True)
                replicas = fixtures.directory((a, b), capacities=((1, 1), (1, 1)))
                events = []
                execution = self.fixture.execution(replicas, physical=self.physical(replicas, backend),
                                                  max_tasks=6, observer=events.append, timeout=5000)
                flow = self.fixture.flow(execution)
                tasks = tuple(fixtures.task(i) for i in range(6))
                flow.offer(tasks); flow.end_input()
                self.fixture.until(execution, lambda: len(a.requests) == len(b.requests) == 1, timeout=15)
                self.assertEqual(replica_usage(execution), dict(
                    a=dict(active_requests=1, active_work=1, uncertain_requests=0),
                    b=dict(active_requests=1, active_work=1, uncertain_requests=0)))
                self.assertEqual(execution.engine.capacity.usage().active_requests, 2)
                a.released.set(); b.released.set()
                results = self.fixture.collect(flow, 6)
                self.assertEqual(Counter(p[1] for p in a.requests + b.requests), Counter(t.payload for t in tasks))
                for sequence, (delivery, response) in results.items():
                    self.assertEqual(delivery.info, tasks[sequence].info)
                    self.assertEqual(response.status_code, 200)
                    self.assertIn(b'"vendor_extension"', response.body)
                    self.assertEqual([v for k, v in response.headers if k.lower() == "x-duplicate"], ["first", "second"])
                self.assertEqual((a.peak, b.peak), (1, 1))
                self.assertEqual(execution.engine.capacity.usage(), Usage())
                flow.close(clean=True)
                self.assertTrue(execution.close())
                pools = [event for event in events if event["event"] == "ray_worker_pool"]
                self.assertEqual({e["endpoint_id"] for e in pools}, {"a", "b"})
                self.assertEqual(len({actor for e in pools for actor in e["actor_ids"]}), 2)
                finished = [event for event in events if event["event"] == "ray_transport_closed"]
                self.assertEqual(len(finished), 2)
                self.assertTrue(all(e["confirmed"] and e["object_bytes"] == 0 for e in finished))
                self.assertTrue(self.ray.is_initialized())

    def test_actual_daft_ray_cancel_retains_slow_replica_and_another_query_then_reuses_execution(self):
        a, b = self.fixture.site("a", hold=True), self.fixture.site("b")
        replicas = fixtures.directory((a, b), capacities=((1, 1), (1, 1)))
        events = []
        execution = self.fixture.execution(replicas, physical=self.physical(replicas, "daft"),
                                          max_tasks=8, max_jobs=2, timeout=5000, observer=events.append)
        first, other = self.fixture.flow(execution, "first-query"), self.fixture.flow(execution, "other-query")
        first.offer((fixtures.task(0, tag="cancelled"),)); other.offer((fixtures.task(0, tag="other"),))
        try:
            self.fixture.until(execution, lambda: len(a.requests) == len(b.requests) == 1, timeout=15)
        except AssertionError:
            self.fail(json.dumps(dict(posts=[len(a.requests), len(b.requests)],
                records=[dict(session=r.key.session_id, sequence=r.key.sequence, endpoint=r.endpoint,
                              phase=r.phase, compute=r.compute) for r in execution.engine.capacity.records.values()],
                sessions=[dict(state=f.session.state.value, error=f.session.error) for f in (first, other)],
                events=[e for e in events if e['event'] in ('ray_execution_error', 'http_error',
                                                         'replica_transport_started', 'uncertain')]), sort_keys=True))
        first.request_cancel()
        self.assertEqual(first.advance(1).state, State.CANCELLED)
        self.assertEqual(first.close().uncertain_requests, 1)
        self.assertFalse(execution.close())
        self.assertEqual(replica_usage(execution)["a"]["active_requests"], 1)
        self.fixture.collect(other, 1)
        other.offer((fixtures.task(1, tag="continuation"),))
        self.fixture.collect(other, 1)
        other.close()
        self.assertEqual(replica_usage(execution)["a"]["active_requests"], 1)
        a.released.set()
        self.fixture.until(execution, lambda: not execution.engine.capacity.records)
        self.assertEqual((len(a.requests), len(b.requests)), (1, 2))
        next_query = self.fixture.flow(execution, "next-query")
        next_query.offer((fixtures.task(0, tag="fresh"),)); next_query.end_input()
        self.fixture.collect(next_query, 1)
        next_query.close(clean=True)
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertTrue(execution.close())
        self.assertEqual(len(a.requests) + len(b.requests), 4)


if __name__ == "__main__":
    unittest.main()
