"""vLLM launch declarations and resource reports using synthetic GPU identities."""

import copy
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.services import launch_vllm_with_identity as identity_launcher
from src.serving.vllm_resources import (
    VllmResources,
    validate_resource_identity,
    verify_resource_argv,
)


class VllmResourceIdentityTests(unittest.TestCase):
    def report(self, *, count="auto", devices=("GPU-a",), tp=1):
        _, argv, report = VllmResources(count, devices, tp).resolve(
            ["--model", "fixture", "--port", "8000"], environ={}, probe=lambda uid: uid
        )
        return argv, json.loads(json.dumps(report))

    def test_real_producer_json_round_trip_accepts_auto_explicit_and_topology(self):
        for count, devices, tp in (
            ("auto", ("GPU-a",), 1),
            (1, ("GPU-a",), 1),
            ("auto", "GPU-a", 1),
            ("auto", "GPU-a,GPU-b", 1),
            ("auto", ("GPU-a", "GPU-b", "GPU-c", "GPU-d"), 2),
        ):
            with self.subTest(count=count, devices=devices, tp=tp):
                argv, report = self.report(count=count, devices=devices, tp=tp)
                with patch("subprocess.run", side_effect=AssertionError("reader probed hardware")):
                    validate_resource_identity(report["resources_requested"], report["resources_resolved"])
                    verify_resource_argv(
                        ["/venv/bin/python", "-m", "vllm.entrypoints.openai.api_server", *argv],
                        report["resources_resolved"],
                    )
                self.assertEqual(report["resources_requested"]["gpu_count"], count)

    def test_invalid_resource_types_counts_allocation_and_ranks_fail(self):
        _, good = self.report()
        mutations = [
            ("resources_requested", "gpu_count", True),
            ("resources_requested", "tensor_parallel_size", 1.0),
            ("resources_requested", "allowed_gpus", None),
            ("resources_resolved", "resolved_count", True),
            ("resources_resolved", "resolved_count", 0),
            ("resources_resolved", "data_parallel_size", 2),
            ("resources_resolved", "tensor_parallel_size", 257),
            ("resources_resolved", "pipeline_parallel_size", True),
            ("resources_resolved", "selected_gpu_uuids", ["GPU-z"]),
            ("resources_resolved", "allowed_gpu_uuids", ["GPU-a", "GPU-a"]),
            ("resources_resolved", "rank_gpu_uuids", [["GPU-z"]]),
            ("resources_resolved", "allocation_source", "all_host"),
            ("resources_resolved", "version", "0.25.2"),
        ]
        for group, name, value in mutations:
            bad = copy.deepcopy(good)
            bad[group][name] = value
            with self.subTest(group=group, name=name), self.assertRaises(ValueError):
                validate_resource_identity(bad["resources_requested"], bad["resources_resolved"])
        bad = copy.deepcopy(good)
        bad["resources_resolved"]["unknown"] = 1
        with self.assertRaises(ValueError):
            validate_resource_identity(bad["resources_requested"], bad["resources_resolved"])

    def test_inherited_mask_declaration_preserves_order_without_reprobing(self):
        _, argv, report = VllmResources(allowed_gpus=("GPU-a", "GPU-b", "GPU-c")).resolve(
            [], environ={"CUDA_VISIBLE_DEVICES": "GPU-c,GPU-b"}, probe=lambda uid: uid
        )
        report = json.loads(json.dumps(report))
        validate_resource_identity(report["resources_requested"], report["resources_resolved"])
        verify_resource_argv(argv, report["resources_resolved"])
        report["resources_resolved"]["allowed_gpu_uuids"].reverse()
        with self.assertRaises(ValueError):
            validate_resource_identity(report["resources_requested"], report["resources_resolved"])

    def test_live_argv_drift_duplicate_alias_and_rank_overrides_fail(self):
        argv, report = self.report()
        resolved = report["resources_resolved"]
        for suffix in (
            ["--data-parallel-size", "1"],
            ["--node-rank", "0"],
            ["-tp", "1"],
            ["--tensor_parallel_size", "1"],
            ["--data-parallel-rank=0"],
            ["--config", "fixture.yaml"],
        ):
            with self.subTest(suffix=suffix), self.assertRaises(ValueError):
                verify_resource_argv([*argv, *suffix], resolved)
        drift = list(argv)
        drift[drift.index("--data-parallel-size") + 1] = "2"
        with self.assertRaises(ValueError):
            verify_resource_argv(drift, resolved)
        with self.assertRaises(ValueError):
            verify_resource_argv(argv[:-2], resolved)



class VllmLauncherIdentityTests(unittest.TestCase):
    def test_identity_launcher_seals_venv_before_exact_server_exec(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "site-packages" / "vllm"
            package.mkdir(parents=True)
            origin = package / "__init__.py"
            origin.write_text("", encoding="utf-8")
            sidecar = root / "runtime.json"
            with patch.object(
                identity_launcher,
                "_args",
                return_value=SimpleNamespace(
                    identity_output=sidecar,
                    resource_config=None,
                    port=8000,
                    server_args=["--model", "/model", "--port", "8000"],
                ),
            ), patch.object(
                identity_launcher.importlib.util,
                "find_spec",
                return_value=SimpleNamespace(origin=str(origin)),
            ), patch.object(
                identity_launcher.importlib.metadata,
                "version",
                return_value="0.25.1",
            ), patch.object(
                identity_launcher,
                "_process_start_time_ticks",
                return_value="12345",
            ), patch.object(
                identity_launcher.os,
                "execv",
                side_effect=RuntimeError("exec captured"),
            ) as execute:
                with self.assertRaisesRegex(RuntimeError, "exec captured"):
                    identity_launcher.main()
            sealed = json.loads(sidecar.read_text(encoding="utf-8"))
            self.assertEqual(set(sealed), {
                "schema_version", "pid", "process_start_time_ticks", "port",
                "python_executable_argv0", "sys_prefix", "package_root", "package_version",
            })
            self.assertEqual(sealed["process_start_time_ticks"], "12345")
            self.assertEqual(sealed["python_executable_argv0"], sys.executable)
            self.assertEqual(sealed["sys_prefix"], sys.prefix)
            self.assertEqual(sealed["package_root"], str(package.resolve()))
            execute.assert_called_once_with(
                sys.executable,
                [
                    sys.executable,
                    "-m",
                    "vllm.entrypoints.openai.api_server",
                    "--model", "/model", "--port", "8000",
                ],
            )
