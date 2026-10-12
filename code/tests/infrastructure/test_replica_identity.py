"""Synthetic process/GPU observations verify identity failures without launching a model."""

from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock

from src.execution_provider.adapters.model_replicas import ModelReplica, ModelReplicaSet
from src.execution_provider.adapters import replica_identity
from src.execution_provider.adapters.replica_identity import (
    GpuProcess, ReplicaListener, ReplicaProcess, _listeners, verify_replica_processes,
)


def fixture(*, same_device=False):
    a = ModelReplica("a", "http://127.0.0.1:8000/v1/chat/completions", "machine-a", ("GPU-a",), "a" * 64, 2, 2)
    b = replace(a, endpoint_id="b", endpoint_url="http://127.0.0.1:8001/v1/chat/completions",
                device_ids=("GPU-a" if same_device else "GPU-b",))
    directory = ModelReplicaSet("fixture", (a, b), 4, 4)
    argv = ("python", "-m", "vllm.entrypoints.openai.api_server", "--model", "/private/model",
            "--served-model-name", "fixture", "--tensor-parallel-size", "1", "--dtype", "bfloat16",
            "--port", "8000", "--host", "127.0.0.1")
    second = tuple("8001" if token == "8000" else token for token in argv)
    processes = (ReplicaProcess(100, 1, "1000", argv, (8000,), (ReplicaListener("ipv4", "127.0.0.1", 8000),)),
                 ReplicaProcess(101, 100, "1001", ("model-engine",)),
                 ReplicaProcess(200, 1, "2000", second, (8001,), (ReplicaListener("ipv4", "127.0.0.1", 8001),)),
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
        self.assertEqual(result["schema_version"], 2)
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
                                    (replace(processes[0], listening_ports=(), listening_sockets=()),) + processes[1:], gpus)

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

    def test_raw_proc_wrong_address_and_ipv6_only_do_not_qualify_an_ipv4_url(self):
        directory, sidecars, processes, gpus = fixture()
        one = replace(directory, replicas=directory.replicas[:1], max_active_requests=2, max_active_work=2)
        for family, address, expected in (("tcp", "0200007F", "127.0.0.2"),
                                           ("tcp6", "00000000000000000000000001000000", "::1")):
            with self.subTest(family=family), tempfile.TemporaryDirectory() as root:
                root = Path(root); (root / "net").mkdir()
                for name in ("tcp", "tcp6"):
                    row = (f"0: {address}:1F40 00000000:0000 0A 00000000:00000000 "
                           "00:00000000 00000000 1000 0 12345 1\n") if name == family else ""
                    (root / "net" / name).write_text("header\n" + row)
                observed = _listeners(root)["12345"]
                self.assertEqual((observed.address, observed.port), (expected, 8000))
                process = replace(processes[0], listening_sockets=(observed,))
                with self.assertRaisesRegex(ValueError, "address/family listener"):
                    verify_replica_processes(one, {"a": sidecars["a"]}, (process, processes[1]), gpus[:1])

    def test_explicit_address_and_wildcards_require_the_same_address_family(self):
        directory, sidecars, processes, gpus = fixture()
        one = replace(directory, replicas=directory.replicas[:1], max_active_requests=2, max_active_work=2)
        for url, listener, accepted in (
                ("http://127.0.0.1:8000/v1/chat/completions", ReplicaListener("ipv4", "127.0.0.1", 8000), True),
                ("http://127.0.0.1:8000/v1/chat/completions", ReplicaListener("ipv4", "0.0.0.0", 8000), True),
                ("http://[::1]:8000/v1/chat/completions", ReplicaListener("ipv6", "::1", 8000), True),
                ("http://[::1]:8000/v1/chat/completions", ReplicaListener("ipv6", "::", 8000), True),
                ("http://127.0.0.1:8000/v1/chat/completions", ReplicaListener("ipv6", "::", 8000), False),
                ("http://[::1]:8000/v1/chat/completions", ReplicaListener("ipv4", "0.0.0.0", 8000), False),
                ("http://127.0.0.1:8000/v1/chat/completions", ReplicaListener("ipv4", "127.0.0.1", 8001), False)):
            candidate = replace(one, replicas=(replace(one.replicas[0], endpoint_url=url),))
            observed = (replace(processes[0], listening_sockets=(listener,)), processes[1])
            with self.subTest(url=url, listener=listener):
                if accepted:
                    report = verify_replica_processes(candidate, {"a": sidecars["a"]}, observed, gpus[:1])
                    self.assertEqual(report["replicas"][0]["listener_bindings"][0]["address"], listener.address)
                else:
                    with self.assertRaisesRegex(ValueError, "listener"):
                        verify_replica_processes(candidate, {"a": sidecars["a"]}, observed, gpus[:1])

    def test_port_only_or_another_process_listener_cannot_qualify_a_model_tree(self):
        directory, sidecars, processes, gpus = fixture()
        one = replace(directory, replicas=directory.replicas[:1], max_active_requests=2, max_active_work=2)
        missing = replace(processes[0], listening_sockets=())
        foreign = ReplicaProcess(999, 1, "9990", ("unrelated",), (), processes[0].listening_sockets)
        with self.assertRaisesRegex(ValueError, "listener"):
            verify_replica_processes(one, {"a": sidecars["a"]}, (missing, processes[1], foreign), gpus[:1])
        descendant = replace(processes[1], listening_sockets=processes[0].listening_sockets)
        report = verify_replica_processes(one, {"a": sidecars["a"]}, (missing, descendant), gpus[:1])
        self.assertEqual(report["replicas"][0]["listener_bindings"][0]["pid"], 101)

    def test_localhost_requires_all_current_loopback_resolutions_to_match_the_model(self):
        directory, sidecars, processes, gpus = fixture()
        one = replace(directory, replicas=(replace(directory.replicas[0],
            endpoint_url="http://localhost:8000/v1/chat/completions"),), max_active_requests=2, max_active_work=2)
        for addresses in (None, (), ("192.0.2.1",), ("127.0.0.1", "::1")):
            with self.subTest(addresses=addresses), self.assertRaises(ValueError):
                verify_replica_processes(one, {"a": sidecars["a"]}, processes[:2], gpus[:1],
                                        localhost_addresses=addresses)
        dual = replace(processes[0], listening_sockets=processes[0].listening_sockets +
                       (ReplicaListener("ipv6", "::1", 8000),))
        report = verify_replica_processes(one, {"a": sidecars["a"]}, (dual, processes[1]), gpus[:1],
                                         localhost_addresses=("127.0.0.1", "::1"))
        self.assertEqual(len(report["replicas"][0]["listener_bindings"]), 2)

    def test_full_local_probe_correlates_proc_socket_inode_and_rechecks_listener(self):
        directory, sidecars, processes, gpus = fixture()
        one = replace(directory, replicas=directory.replicas[:1], max_active_requests=2, max_active_work=2)
        with tempfile.TemporaryDirectory() as root:
            root = Path(root); (root / "net").mkdir()
            row = "0: 0100007F:1F40 00000000:0000 0A 00000000:00000000 00:00000000 00000000 1000 0 12345 1\n"
            (root / "net/tcp").write_text("header\n" + row); (root / "net/tcp6").write_text("header\n")
            for process in processes[:2]:
                path = root / str(process.pid); path.mkdir(); (path / "fd").mkdir()
                (path / "stat").write_text(f"{process.pid} (model process) " +
                    " ".join(["S", str(process.parent_pid)] + ["0"] * 17 + [process.start_ticks]))
                (path / "cmdline").write_bytes(b"\0".join(item.encode() for item in process.argv) + b"\0")
            (root / "100/fd/3").symlink_to("socket:[12345]")
            sidecar = root / "sidecar.json"; sidecar.write_text(json.dumps(sidecars["a"]))
            def path(value):
                return root if str(value) == "/proc" else Path(value)
            with mock.patch.object(replica_identity, "Path", path), \
                    mock.patch.object(replica_identity.subprocess, "run",
                                      return_value=SimpleNamespace(stdout="101, GPU-a\n")):
                report = replica_identity.inspect_local_model_replicas(one, {"a": sidecar})
                self.assertEqual(report["replicas"][0]["listener_bindings"][0]["inode"], "12345")
                named = replace(one, replicas=(replace(one.replicas[0],
                    endpoint_url="http://localhost:8000/v1/chat/completions"),))
                ipv4 = (replica_identity.socket.AF_INET, replica_identity.socket.SOCK_STREAM,
                        replica_identity.socket.IPPROTO_TCP, "", ("127.0.0.1", 0))
                ipv6 = (replica_identity.socket.AF_INET6, replica_identity.socket.SOCK_STREAM,
                        replica_identity.socket.IPPROTO_TCP, "", ("::1", 0, 0, 0))
                with mock.patch.object(replica_identity.socket, "getaddrinfo", return_value=[ipv4]):
                    self.assertEqual(replica_identity.inspect_local_model_replicas(named, {"a": sidecar})
                                     ["replica_count"], 1)
                with mock.patch.object(replica_identity.socket, "getaddrinfo", return_value=[ipv4, ipv6]):
                    with self.assertRaisesRegex(ValueError, "address/family listener"):
                        replica_identity.inspect_local_model_replicas(named, {"a": sidecar})
                observed_listeners = _listeners(root)
                with mock.patch.object(replica_identity, "_listeners", side_effect=[observed_listeners, {}]):
                    with self.assertRaisesRegex(ValueError, "listener changed"):
                        replica_identity.inspect_local_model_replicas(one, {"a": sidecar})


if __name__ == "__main__":
    unittest.main()
