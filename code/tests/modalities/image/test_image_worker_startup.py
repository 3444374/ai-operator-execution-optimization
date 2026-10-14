"""Worker-factory failures; real Ray exit evidence is opt-in and CPU-only."""

import datetime
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from src.modalities.image import clip, execution


class _FakeRay(ModuleType):
    def __init__(self, *, create_error_at=None, get_error_at=None, ready_error=False, mismatch=False, kill_errors=None):
        super().__init__("ray")
        self.created, self.killed, self.options = [], [], []
        self.create_error_at, self.get_error_at = create_error_at, get_error_at
        self.ready_error, self.mismatch = ready_error, mismatch
        self.kill_errors = kill_errors or {}
        self.first_error = RuntimeError("fixture worker startup failure")
        self.get_calls = 0

    def remote(self, **options):
        self.options.append(options)
        def wrap(cls):
            def create(*args, **kwargs):
                if len(self.created) + 1 == self.create_error_at:
                    raise self.first_error
                actor = SimpleNamespace(name=f"{cls.__name__}:{len(self.created)}")
                def ready():
                    if self.ready_error: raise self.first_error
                    return actor
                actor.ready = SimpleNamespace(remote=ready)
                self.created.append(actor)
                return actor
            return SimpleNamespace(remote=create)
        return wrap

    def get(self, references):
        self.get_calls += 1
        if self.get_calls == self.get_error_at: raise self.first_error
        return [{"torch_intraop_threads": 9 if self.mismatch else 1, "torch_interop_threads": 1}
                for _ in references]

    def kill(self, actor, *, no_restart):
        assert no_restart is True
        self.killed.append(actor)
        if len(self.killed) in self.kill_errors: raise self.kill_errors[len(self.killed)]


class ImageWorkerStartupTest(unittest.TestCase):
    def build(self, ray):
        with patch.dict(sys.modules, {"ray": ray}):
            return execution.build_semloom_ray_worker_pool(
                model_revision="fixture", processor_revision="fixture", cpu_workers=2,
                gpu_workers=2, dtype="float16",
            )

    def test_partial_creation_ready_submission_and_readiness_get_retain_all_known_handles(self):
        for options in ({"create_error_at": 2}, {"create_error_at": 4}, {"ready_error": True},
                        {"get_error_at": 1}, {"get_error_at": 2}):
            with self.subTest(options=options):
                ray = _FakeRay(**options)
                with self.assertRaises(RuntimeError) as raised: self.build(ray)
                self.assertIs(raised.exception, ray.first_error)
                retained = raised.exception.image_worker_pool_startup
                self.assertEqual(retained.actor_handles, tuple(ray.created))
                self.assertEqual(ray.killed, ray.created)
                self.assertEqual(retained.termination_request_outcomes, ("requested",) * len(ray.created))
                self.assertEqual(retained.physical_exit_status, "unconfirmed")
                self.assertFalse(hasattr(retained, "preprocessors"))
                self.assertFalse(hasattr(retained, "gpu_actors"))

    def test_cleanup_errors_do_not_replace_startup_error_or_skip_other_handles(self):
        ray = _FakeRay(get_error_at=1, kill_errors={1: OSError("fixture kill error"), 3: ValueError("fixture kill error")})
        with self.assertRaises(RuntimeError) as raised: self.build(ray)
        self.assertIs(raised.exception, ray.first_error)
        self.assertEqual(ray.killed, ray.created)
        retained = raised.exception.image_worker_pool_startup
        self.assertEqual(retained.termination_request_outcomes,
                         ("request_error:OSError", "requested", "request_error:ValueError", "requested"))
        self.assertEqual(retained.physical_exit_status, "unconfirmed")
        self.assertTrue(any("OSError" in note for note in raised.exception.__notes__))

    def test_thread_contract_mismatch_is_also_a_startup_failure(self):
        ray = _FakeRay(mismatch=True)
        with self.assertRaisesRegex(RuntimeError, "thread contract mismatch") as raised: self.build(ray)
        self.assertEqual(ray.killed, ray.created)
        self.assertEqual(len(raised.exception.image_worker_pool_startup.actor_handles), 4)

    def test_interrupt_keeps_first_error_and_created_handles(self):
        ray = _FakeRay(create_error_at=2)
        ray.first_error = KeyboardInterrupt("fixture interrupt")
        with self.assertRaises(KeyboardInterrupt) as raised: self.build(ray)
        self.assertIs(raised.exception, ray.first_error)
        self.assertEqual(ray.killed, ray.created)
        self.assertEqual(len(raised.exception.image_worker_pool_startup.actor_handles), 1)

    def test_success_preserves_pool_actor_order_and_native_resource_requests(self):
        ray = _FakeRay()
        pool = self.build(ray)
        self.assertIsInstance(pool, execution.ProjectRayWorkerPool)
        self.assertEqual(pool.preprocessors, tuple(ray.created[:2]))
        self.assertEqual(pool.gpu_actors, tuple(ray.created[2:]))
        self.assertEqual(ray.killed, [])
        self.assertEqual(ray.options, [{"num_cpus": 1, "max_restarts": 0, "max_task_retries": 0},
                                     {"num_cpus": 1, "num_gpus": 1, "max_restarts": 0, "max_task_retries": 0}])

    def test_normal_pool_stop_attempts_all_handles_and_keeps_first_termination_error(self):
        ray = _FakeRay()
        pool = self.build(ray)
        first_error = OSError("fixture first kill error")
        ray.kill_errors = {1: first_error, 3: ValueError("fixture second kill error")}
        with patch.dict(sys.modules, {"ray": ray}):
            with self.assertRaises(OSError) as raised: execution.stop_semloom_ray_worker_pool(pool)
        self.assertIs(raised.exception, first_error)
        self.assertEqual(ray.killed, ray.created)
        self.assertEqual(raised.exception.image_actor_termination_outcomes,
                         ("request_error:OSError", "requested", "request_error:ValueError", "requested"))


def _process_start_ticks(pid):
    try:
        return int((Path("/proc") / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()[19])
    except FileNotFoundError:
        return None


def _write_worker_identity(directory, role):
    pid = os.getpid()
    (Path(directory) / f"{role}-{pid}.json").write_text(json.dumps(
        {"role": role, "pid": pid, "start_ticks": _process_start_ticks(pid)}, sort_keys=True))


class _NoModelPreprocessor:
    def __init__(self, revision, *, torch_intraop_threads, torch_interop_threads):
        _write_worker_identity(revision, "preprocess")
        import torch
        torch.set_num_threads(torch_intraop_threads)
        torch.set_num_interop_threads(torch_interop_threads)


class _NoModelActor:
    def __init__(self, revision, **kwargs):
        _write_worker_identity(revision, "model_cpu_fixture")

    def ready(self):
        raise RuntimeError("intentional no-model readiness failure")


class ImageWorkerStartupRealRayTest(unittest.TestCase):
    @unittest.skipUnless(os.environ.get("SEMLOOM_IMAGE_WORKER_SDK_TESTS") == "1",
                         "opt-in fixed Ray CPU process-exit evidence; shared window required")
    def test_real_ray_factory_failure_records_process_exit_before_client_shutdown(self):
        import ray

        if not Path("/proc/self/stat").is_file(): self.skipTest("requires Linux PID/start-tick evidence")
        if ray.is_initialized(): self.skipTest("preserve an existing Ray client")
        self.assertEqual(ray.__version__, "2.56.1")
        # Test substitutes both model implementations and changes the model
        # actor's GPU request to zero. It tests native Ray process termination,
        # never GPU/device completion or the real CLIP workload.
        report = {"ray_version": ray.__version__, "fixture": "substituted_cpu_actors_no_model",
                  "num_cpus": 2, "num_gpus": 0, "status": "incomplete"}
        native_remote, native_get = ray.remote, ray.get
        def cpu_only_remote(**options):
            if "num_gpus" in options: options["num_gpus"] = 0
            return native_remote(**options)
        def bounded_get(references, **kwargs):
            kwargs.setdefault("timeout", 30)
            return native_get(references, **kwargs)
        with tempfile.TemporaryDirectory() as directory:
            try:
                ray.init(num_cpus=2, num_gpus=0, include_dashboard=False, log_to_driver=False,
                         runtime_env={"env_vars": {"PYTHONPATH": str(Path(__file__).resolve().parents[3])}})
                with patch.object(clip, "FastClipImagePreprocessor", _NoModelPreprocessor), \
                     patch.object(clip, "ClipTensorActor", _NoModelActor), \
                     patch.object(ray, "remote", cpu_only_remote), patch.object(ray, "get", bounded_get):
                    with self.assertRaises(Exception) as raised:
                        execution.build_semloom_ray_worker_pool(
                            model_revision=directory, processor_revision=directory,
                            cpu_workers=1, gpu_workers=1, dtype="float16",
                        )
                report["initial_error_type"] = type(raised.exception).__name__
                self.assertIn("intentional no-model readiness failure", str(raised.exception))
                failure = raised.exception.image_worker_pool_startup
                report["termination_request_outcomes"] = failure.termination_request_outcomes
                identities = [json.loads(path.read_text()) for path in sorted(Path(directory).glob("*.json"))]
                report["created_workers"] = identities
                self.assertEqual({value["role"] for value in identities}, {"preprocess", "model_cpu_fixture"})
                self.assertEqual(len(identities), 2)
                self.assertTrue(all(type(value["start_ticks"]) is int for value in identities))
                deadline = time.monotonic() + 30
                while True:
                    observed = [{**value, "observed_start_ticks": _process_start_ticks(value["pid"])}
                                for value in identities]
                    report["exit_evidence_before_ray_shutdown"] = observed
                    exited = all(value["observed_start_ticks"] != value["start_ticks"] for value in observed)
                    if exited or time.monotonic() >= deadline: break
                    time.sleep(0.05)
                report["physical_cpu_process_exit_confirmed"] = exited
                self.assertTrue(exited, "native Ray termination requests lacked process-exit evidence")
                report["status"] = "cpu_process_exit_observed"
            finally:
                report["recorded_at_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                print("IMAGE_WORKER_EXIT_EVIDENCE " + json.dumps(report, sort_keys=True))
                if ray.is_initialized(): ray.shutdown()


if __name__ == "__main__":
    unittest.main()
