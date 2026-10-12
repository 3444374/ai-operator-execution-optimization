"""Synthetic process/GPU observations verify identity failures without launching a model."""

from dataclasses import replace
import unittest

from src.execution_provider.adapters.model_replicas import ModelReplica, ModelReplicaSet
from src.execution_provider.adapters.replica_identity import GpuProcess, ReplicaProcess, verify_replica_processes


def fixture(*, same_device=False):
    a = ModelReplica("a", "http://127.0.0.1:8000/v1/chat/completions", "machine-a", ("GPU-a",), "a" * 64, 2, 2)
    b = replace(a, endpoint_id="b", endpoint_url="http://127.0.0.1:8001/v1/chat/completions",
                device_ids=("GPU-a" if same_device else "GPU-b",))
    directory = ModelReplicaSet("fixture", (a, b), 4, 4)
    argv = ("python", "-m", "vllm.entrypoints.openai.api_server", "--model", "/private/model",
            "--served-model-name", "fixture", "--tensor-parallel-size", "1", "--dtype", "bfloat16",
            "--port", "8000", "--host", "127.0.0.1")
    second = tuple("8001" if token == "8000" else token for token in argv)
    processes = (ReplicaProcess(100, 1, "1000", argv, (8000,)),
                 ReplicaProcess(101, 100, "1001", ("model-engine",)),
                 ReplicaProcess(200, 1, "2000", second, (8001,)),
                 ReplicaProcess(201, 200, "2001", ("model-engine",)))
    sidecars = {name: dict(schema_version=1, pid=pid, port=port, process_start_time_ticks=started,
                          package_version="fixture-version")
                for name, pid, port, started in (("a", 100, 8000, "1000"), ("b", 200, 8001, "2000"))}
    gpus = (GpuProcess(101, "GPU-a"), GpuProcess(201, "GPU-a" if same_device else "GPU-b"))
    return directory, sidecars, processes, gpus


class ReplicaIdentityTests(unittest.TestCase):
    def test_two_tp1_processes_are_bound_to_distinct_real_gpu_descendants(self):
        directory, sidecars, processes, gpus = fixture()
        result = verify_replica_processes(directory, sidecars, processes, gpus)
        self.assertEqual((result["replica_count"], result["gpu_count"], result["independent_devices"]), (2, 2, True))
        self.assertEqual([r["gpu_process_ids"] for r in result["replicas"]], [[101], [201]])
        self.assertEqual([r["device_ids"] for r in result["replicas"]], [["GPU-a"], ["GPU-b"]])
        self.assertEqual(result["replica_set_sha256"], directory.sha256)
        self.assertEqual(result["model_content_identity"], "declared; separate content audit required")
        self.assertNotIn("/private/model", str(result))
        self.assertNotIn(directory.replicas[0].endpoint_url, str(result))

    def test_single_replica_reports_one_gpu_instead_of_worker_count(self):
        directory, sidecars, processes, gpus = fixture()
        one = replace(directory, replicas=directory.replicas[:1], max_active_requests=2, max_active_work=2)
        result = verify_replica_processes(one, {"a": sidecars["a"]}, processes, gpus)
        self.assertEqual((result["replica_count"], result["gpu_count"]), (1, 1))

    def test_ray_http_actors_and_declared_gpu_names_do_not_prove_model_replicas(self):
        directory, sidecars, processes, gpus = fixture()
        for invalid in ((), (replace(processes[0], argv=("python", "ray::_HttpActor")),) + processes[1:]):
            with self.subTest(processes=len(invalid)), self.assertRaises(ValueError):
                verify_replica_processes(directory, sidecars, invalid, gpus)
        with self.assertRaisesRegex(ValueError, "actual model GPU"):
            verify_replica_processes(directory, sidecars, processes, ())

    def test_stale_pid_missing_sidecar_and_unowned_listener_are_rejected(self):
        directory, sidecars, processes, gpus = fixture()
        for invalid in ({"a": sidecars["a"]},
                        dict(sidecars, a=dict(sidecars["a"], process_start_time_ticks="stale")),
                        dict(sidecars, a=dict(sidecars["a"], port=8001)),
                        dict(sidecars, a=dict(sidecars["a"], pid=True))):
            with self.subTest(sidecars=invalid), self.assertRaises(ValueError):
                verify_replica_processes(directory, invalid, processes, gpus)
        with self.assertRaisesRegex(ValueError, "listener"):
            verify_replica_processes(directory, sidecars,
                                    (replace(processes[0], listening_ports=()),) + processes[1:], gpus)

    def test_device_mismatch_or_shared_device_cannot_be_reported_as_two_independent_gpus(self):
        directory, sidecars, processes, gpus = fixture()
        with self.assertRaisesRegex(ValueError, "declared devices"):
            verify_replica_processes(directory, sidecars, processes, (GpuProcess(101, "GPU-b"),) + gpus[1:])
        directory, sidecars, processes, gpus = fixture(same_device=True)
        with self.assertRaisesRegex(ValueError, "without other compute owners"):
            verify_replica_processes(directory, sidecars, processes, gpus)
        shared = verify_replica_processes(directory, sidecars, processes, gpus, independent_devices=False)
        self.assertEqual((shared["replica_count"], shared["gpu_count"], shared["independent_devices"]), (2, 1, False))

    def test_foreign_gpu_processes_cannot_hide_in_independent_replica_capacity(self):
        directory, sidecars, processes, gpus = fixture()
        with self.assertRaisesRegex(ValueError, "without other compute owners"):
            verify_replica_processes(directory, sidecars, processes, gpus + (GpuProcess(999, "GPU-a"),))

    def test_distinct_ports_cannot_alias_one_model_process_tree(self):
        directory, sidecars, processes, gpus = fixture()
        joined = processes[:2] + (replace(processes[2], parent_pid=100), processes[3])
        with self.assertRaises(ValueError):
            verify_replica_processes(directory, sidecars, joined, gpus)

    def test_wrong_model_tensor_parallel_or_serving_arguments_are_rejected(self):
        directory, sidecars, processes, gpus = fixture()
        for flag, value in (("--served-model-name", "different"), ("--tensor-parallel-size", "2"),
                            ("--dtype", "float16")):
            argv = list(processes[2].argv)
            argv[argv.index(flag) + 1] = value
            with self.subTest(flag=flag), self.assertRaises(ValueError):
                verify_replica_processes(directory, sidecars,
                                        processes[:2] + (replace(processes[2], argv=tuple(argv)), processes[3]), gpus)
        no_tp = tuple(token for token in processes[2].argv if token not in ("--tensor-parallel-size", "1"))
        with self.assertRaises(ValueError):
            verify_replica_processes(directory, sidecars,
                                    processes[:2] + (replace(processes[2], argv=no_tp), processes[3]), gpus)
        with self.assertRaisesRegex(ValueError, "package versions"):
            verify_replica_processes(directory, dict(sidecars, b=dict(sidecars["b"], package_version="different")),
                                    processes, gpus)

    def test_remote_url_requires_running_the_probe_on_the_endpoint_machine(self):
        directory, sidecars, processes, gpus = fixture()
        remote = replace(directory, replicas=(replace(directory.replicas[0],
            endpoint_url="https://example.invalid:8000/v1/chat/completions"), directory.replicas[1]))
        with self.assertRaisesRegex(ValueError, "endpoint-machine loopback"):
            verify_replica_processes(remote, sidecars, processes, gpus)


if __name__ == "__main__":
    unittest.main()
