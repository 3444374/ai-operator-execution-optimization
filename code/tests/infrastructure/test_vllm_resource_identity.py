"""Launcher declarations and legacy live preflight must agree without GPU probes."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.infrastructure.vllm_preflight import _load_runtime_identities
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

    def sidecar(self, report=None):
        return dict(
            schema_version=1,
            pid=123,
            process_start_time_ticks="456",
            port=8000,
            python_executable_argv0="/venv/bin/python",
            sys_prefix="/venv",
            package_root="/venv/lib/vllm",
            package_version="0.25.1",
            **(report or {}),
        )

    def load(self, value):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.json"
            path.write_text(json.dumps(value))
            return _load_runtime_identities((path,))

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
                    loaded = self.load(self.sidecar(report))["8000"]
                    verify_resource_argv(
                        ["/venv/bin/python", "-m", "vllm.entrypoints.openai.api_server", *argv],
                        loaded["resources_resolved"],
                    )
                self.assertEqual(loaded["resources_requested"]["gpu_count"], count)

    def test_legacy_exact_schema_and_unknown_or_partial_resource_fields(self):
        self.assertNotIn("resources_requested", self.load(self.sidecar())["8000"])
        _, report = self.report()
        invalid = [
            dict(self.sidecar(), extra=True),
            dict(self.sidecar(), resources_requested=report["resources_requested"]),
            dict(self.sidecar(), resources_resolved=report["resources_resolved"]),
            dict(self.sidecar(), resources_requested=None, resources_resolved=None),
        ]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises((ValueError, RuntimeError)):
                self.load(value)

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
            self.load(self.sidecar(bad))

    def test_inherited_mask_declaration_preserves_order_without_reprobing(self):
        _, argv, report = VllmResources(allowed_gpus=("GPU-a", "GPU-b", "GPU-c")).resolve(
            [], environ={"CUDA_VISIBLE_DEVICES": "GPU-c,GPU-b"}, probe=lambda uid: uid
        )
        report = json.loads(json.dumps(report))
        self.load(self.sidecar(report))
        verify_resource_argv(argv, report["resources_resolved"])
        report["resources_resolved"]["allowed_gpu_uuids"].reverse()
        with self.assertRaises(ValueError):
            self.load(self.sidecar(report))

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

    def test_resource_package_version_and_duplicate_port_fail(self):
        _, report = self.report()
        value = self.sidecar(report)
        value["package_version"] = "0.25.0"
        with self.assertRaises(RuntimeError):
            self.load(value)
        with tempfile.TemporaryDirectory() as directory:
            paths = tuple(Path(directory) / name for name in ("a.json", "b.json"))
            for path in paths:
                path.write_text(json.dumps(self.sidecar(report)))
            with self.assertRaisesRegex(RuntimeError, "duplicate"):
                _load_runtime_identities(paths)


if __name__ == "__main__":
    unittest.main()
