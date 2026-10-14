from __future__ import annotations

import builtins
import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
SCRIPT = CODE_ROOT / "scripts" / "profiling" / "validate_clip_input_residency.py"
SPEC = importlib.util.spec_from_file_location("clip_input_residency_validation", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ClipInputResidencyValidationTests(unittest.TestCase):
    def wrap_argv(self, *base_argv, **options):
        argv = ["wrap", "--base-cli", "/path/to/cli.py", "--cache-bytes", "0",
                "--observations", "/path/to/new-observations.json"]
        argv.extend(options.get("wrapper", []))
        return [*argv, "--", *base_argv]

    def test_explicit_semloom_arm_and_zero_cache_are_accepted_without_sdk_import(self):
        args = MODULE.parse_args(self.wrap_argv("--arm", "project_ray", "--dataset-passes", "2"))
        self.assertEqual(args.base_argv, ["--arm", "project_ray", "--dataset-passes", "2"])
        self.assertEqual((args.cache_bytes, args.cache_entries), (0, 8))

    def test_native_arms_cannot_be_wrapped(self):
        for arm in ("daft_native", "daft_builtin_embed", "ray_data_staged", "imlane"):
            with self.subTest(arm=arm), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    MODULE.parse_args(self.wrap_argv("--arm", arm))

    def test_missing_duplicate_and_abbreviated_arm_are_rejected(self):
        invalid = ([], ["--arm", "project_ray", "--arm=daft_native"],
                   ["--arm", "project_ray", "--ar", "daft_native"])
        for argv in invalid:
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    MODULE.parse_args(self.wrap_argv(*argv))

    def test_gpu_count_and_intrusive_stage_timing_are_rejected(self):
        for extra in (["--gpu-workers", "2"], ["--detailed-stage-timing"]):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    MODULE.parse_args(self.wrap_argv("--arm", "project_ray", *extra))

    def test_cache_fields_reject_negative_bytes_and_zero_entries(self):
        for extra in (["--cache-bytes", "-1"], ["--cache-entries", "0"]):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    MODULE.parse_args(self.wrap_argv("--arm=project_ray", wrapper=extra))

    def test_timeout_and_tolerances_are_finite(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            MODULE.parse_args(self.wrap_argv("--arm=project_ray", wrapper=["--timeout-s", "nan"]))
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            MODULE.parse_args(["qualify", "--model", "/model", "--image-root", "/images",
                               "--image-list", "/manifest", "--observations", "/new.json", "--rtol", "inf"])

    def test_help_and_preview_do_not_import_ray_torch_or_transformers(self):
        actual_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name.split(".", 1)[0] in ("torch", "ray", "transformers"):
                raise AssertionError("preview tried to import a device/model SDK")
            return actual_import(name, *args, **kwargs)

        with patch("builtins.__import__", guarded_import), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as stopped:
                MODULE.main(["--help"])
            self.assertEqual(stopped.exception.code, 0)
            MODULE.main(self.wrap_argv("--arm=project_ray", wrapper=["--preview"]))
            MODULE.main(["qualify", "--model", "/model", "--image-root", "/images", "--image-list", "/manifest",
                         "--observations", "/new.json", "--preview"])
            MODULE.main(["topology", "--output", "/new.json", "--preview"])

    def test_output_contract_checks_full_512_dimensions_current_ids_and_normalization(self):
        ids = tuple(str(index) for index in range(64))
        matrix = np.zeros((64, 512), dtype=np.float32)
        matrix[:, 0] = 1.0
        result = SimpleNamespace(doc_ids=ids, embeddings=matrix,
                                 semantics=SimpleNamespace(projected=True, normalized=True, dtype="float32"))
        self.assertEqual(MODULE.validate_output(result, ids)["shape"], [64, 512])
        for replacement in (SimpleNamespace(**{**vars(result), "doc_ids": ids[::-1]}),
                            SimpleNamespace(**{**vars(result), "embeddings": matrix[:, :16]}),
                            SimpleNamespace(**{**vars(result), "embeddings": matrix * 2}),
                            SimpleNamespace(**{**vars(result), "embeddings": matrix.astype(np.float16)})):
            with self.subTest(replacement=replacement), self.assertRaises(AssertionError):
                MODULE.validate_output(replacement, ids)

    def test_snapshot_serializes_only_event_fields_and_does_not_synchronize(self):
        sys.path.insert(0, str(CODE_ROOT))
        from src.modalities.image.input_residency import DeviceInputCache
        actor_type = MODULE.make_measured_actor(0, cuda_events=True)
        actor = actor_type.__new__(actor_type)
        actor._device = "cuda"
        actor._torch = SimpleNamespace(cuda=SimpleNamespace(**{
            name: lambda _device: 0 for name in (
                "memory_allocated", "memory_reserved", "max_memory_allocated", "max_memory_reserved")}))
        actor._inputs = DeviceInputCache(budget_bytes=0, location="fixture")
        actor._initial_memory, actor._forward_calls = {}, 1
        actor._cancel_flag_observed, actor._input_completion_event = False, None
        actor.ready = lambda: {"fixture_only": True}
        start = SimpleNamespace(elapsed_time=lambda _end: 12.5)
        done = SimpleNamespace(query=lambda: True)
        pending = SimpleNamespace(query=lambda: False)
        actor._records = [{"row_count": 64, "service_s": 0.2, "clip_forward_events": (start, done)},
                          {"row_count": 64, "as_tensor_events": (start, pending)}]
        snapshot = actor.residency_measurement_snapshot()
        self.assertEqual(snapshot["calls"][0]["clip_forward_cuda_s"], 0.0125)
        self.assertIsNone(snapshot["calls"][1]["as_tensor_cuda_s"])
        self.assertFalse(snapshot["calls"][1]["as_tensor_event_done"])
        json.dumps(snapshot)

    def test_source_path_escape_is_rejected_before_read(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "images.json"
            manifest.write_text(json.dumps(["../outside.jpg"] * 64))
            with self.assertRaisesRegex(ValueError, "relative JPEG"):
                MODULE._read_qualification_images(SimpleNamespace(image_list=manifest, image_root=root))

    def test_qualification_rejects_smaller_input_instead_of_changing_task(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "images.json"
            manifest.write_text(json.dumps(["one.jpg"] * 63))
            with self.assertRaisesRegex(ValueError, "at least 64"):
                MODULE._read_qualification_images(SimpleNamespace(image_list=manifest, image_root=root))

    def test_topology_one_gpu_reports_pending_and_no_peer_transport(self):
        fake_torch = SimpleNamespace(cuda=SimpleNamespace(device_count=lambda: 1))
        completed = SimpleNamespace(stdout="single-device topology\n")
        result = {}
        with patch.dict(sys.modules, {"torch": fake_torch}), patch.object(MODULE.subprocess, "run", return_value=completed):
            MODULE.run_topology(result)
        self.assertEqual((result["status"], result["two_devices_required"]), ("pending", True))
        self.assertEqual(result["directional_peer_access"], [])
        self.assertFalse(result["semloom_peer_copy_implemented"])

    def test_topology_checks_both_directions_without_claiming_copy(self):
        pairs = []
        fake_torch = SimpleNamespace(cuda=SimpleNamespace(
            device_count=lambda: 2,
            can_device_access_peer=lambda source, destination: pairs.append((source, destination)) or True))
        result = {}
        with patch.dict(sys.modules, {"torch": fake_torch}), patch.object(
                MODULE.subprocess, "run", return_value=SimpleNamespace(stdout="two-device topology\n")):
            MODULE.run_topology(result)
        self.assertEqual(pairs, [(0, 1), (1, 0)])
        self.assertFalse(result["semloom_peer_copy_implemented"])
        self.assertEqual(result["fallback"], "host_reload_on_destination")


if __name__ == "__main__":
    unittest.main()
