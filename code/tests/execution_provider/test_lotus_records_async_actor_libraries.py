"""Opt-in real threaded Ray actor + CPU model child, with zero logical GPUs."""

from dataclasses import asdict
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.execution_provider.test_lotus_records_async_actor import CPUActor, prepared


class RayCPUActor(CPUActor):
    def ready(self, deadline):
        # Both interpreters deliberately use the existing CPU venv in this
        # fixture; no real vLLM runtime or environment qualification is claimed.
        with patch.object(EngineSpec, "validate_driver_environment", return_value={}):
            report = super().ready(deadline)
        return dict(report, actor_pid=os.getpid(), fixture_child_pid=self.engine.process.pid,
                    actual_ray_gpus=0, model_runtime="fixed CPU fixture")


@unittest.skipUnless(os.environ.get("RUN_ASYNC_RECORDS_CPU_LIBRARIES") == "1",
                     "explicit zero-GPU Ray/IPC CPU-library qualification required")
class AsyncActorLibrariesTests(unittest.TestCase):
    def test_actual_threaded_actor_late_supply_cancel_scope_and_actor_death_child_eof(self):
        import psutil
        import ray

        self.assertEqual(ray.__version__, "2.56.1")
        self.assertFalse(ray.is_initialized(), "the test owns only its fresh local Ray runtime")
        limits = AsyncEngineLimits(max_inflight=2, result_slots=2, command_slots=2,
            input_bytes=8192, item_input_bytes=4096, result_bytes=8192, item_result_bytes=4096)
        spec = EngineSpec("/tmp/fixture-model", sys.executable, sys.executable)
        actors, children = [], []

        def live(pid, created):
            try:
                process = psutil.Process(pid)
                return process.create_time() == created and process.status() != psutil.STATUS_ZOMBIE
            except psutil.NoSuchProcess:
                return False

        with tempfile.TemporaryDirectory(prefix="asyncray-", dir="/tmp") as runtime:
            try:
                ray.init(address="local", num_cpus=2, num_gpus=0, include_dashboard=False,
                    object_store_memory=80 * 1024**2, _temp_dir=runtime,
                    _node_ip_address="127.0.0.1",
                    runtime_env={"env_vars": {"PYTHONPATH": str(Path(__file__).resolve().parents[2]),
                        "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1"}})

                def start():
                    actor = ray.remote(RayCPUActor).options(num_cpus=1, num_gpus=0,
                        max_concurrency=4, max_restarts=0, max_task_retries=0).remote(
                            asdict(spec), asdict(GenerationSpec()), 1, asdict(limits))
                    actors.append(actor)
                    ready = ray.get(actor.ready.remote(time.monotonic() + 20), timeout=25)
                    pid = ready["fixture_child_pid"]
                    children.append((pid, psutil.Process(pid).create_time()))
                    self.assertEqual(ready["actual_ray_gpus"], 0)
                    return actor

                def accepted(actor, count):
                    deadline = time.monotonic() + 5
                    while ray.get(actor.snapshot.remote(), timeout=2)["ipc"]["accepted_requests"] != count:
                        if time.monotonic() >= deadline:
                            self.fail("real threaded actor did not accept fixture input")
                        time.sleep(0.01)

                actor = start()
                first = actor.run_core.remote([prepared(0, "hold")], time.monotonic() + 10, 4096, (0, 0))
                accepted(actor, 1)
                second = actor.run_core.remote([prepared(1)], time.monotonic() + 10, 4096, (0, 1))
                self.assertEqual(json.loads(ray.get(second, timeout=5))["row_id"], "row-1")
                self.assertEqual(ray.wait([first], timeout=0)[0], [])
                self.assertFalse(ray.get(actor.abort_core.remote((0, 0)), timeout=2)["engine_terminal_confirmed"])
                self.assertEqual(json.loads(ray.get(first, timeout=5))["prompt_tokens"], 11)
                self.assertEqual(ray.get([actor.acknowledge_core.remote((0, i)) for i in (0, 1)]), [True, True])

                current = actor.run_core.remote([prepared(0, "hold")], time.monotonic() + 10, 4096, (1, 0))
                accepted(actor, 1)
                self.assertFalse(ray.get(actor.abort_core.remote((0, 0)), timeout=2)["requested"])
                self.assertEqual(ray.wait([current], timeout=0)[0], [])
                ray.get(actor.abort_core.remote((1, 0)), timeout=2)
                self.assertEqual(json.loads(ray.get(current, timeout=5))["prompt_tokens"], 11)
                ray.get(actor.acknowledge_core.remote((1, 0)))
                closed = ray.get(actor.close.remote(3), timeout=5)
                self.assertTrue(closed["ipc_close"]["process_exit_confirmed"])
                self.assertIsNone(closed["gpu_exit_confirmed"])
                ray.kill(actor, no_restart=True)

                dying = start()
                pending = dying.run_core.remote([prepared(0, "hold")], time.monotonic() + 20, 4096, (0, 0))
                accepted(dying, 1)
                child = children[-1]
                ray.kill(dying, no_restart=True)
                with self.assertRaises(ray.exceptions.RayActorError):
                    ray.get(pending, timeout=5)
                deadline = time.monotonic() + 5
                while live(*child) and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertFalse(live(*child), "owned CPU child must close after actor IPC EOF")
            finally:
                for actor in actors:
                    try:
                        ray.kill(actor, no_restart=True)
                    except Exception:
                        pass
                for pid, created in children:
                    if live(pid, created):
                        process = psutil.Process(pid)
                        process.terminate()
                        try:
                            process.wait(2)
                        except psutil.TimeoutExpired:
                            process.kill()
                            process.wait(2)
                ray.shutdown()


if __name__ == "__main__":
    unittest.main()
