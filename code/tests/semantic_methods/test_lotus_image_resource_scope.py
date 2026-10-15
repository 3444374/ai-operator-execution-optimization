"""The public resource entry admits only its source-qualified single-card scope."""
import os
import unittest
from unittest.mock import Mock, patch

from src.modalities.image.resource_budget import ImageWorkerResources


class ImageResourceScopeTests(unittest.TestCase):
    @patch.dict(os.environ, {"NVIDIA_TF32_OVERRIDE": "0"}, clear=True)
    @patch("src.modalities.image.resource_budget.available_cpu_slots", return_value=8)
    def test_auto_and_explicit_one_keep_the_same_allowed_gpu_and_native_slots(self, _slots):
        for arm, pipeline, slots in (("semloom_static", "fused", 3),
                                     ("ray_data", "fused", 2),
                                     ("ray_data", "staged", 3),
                                     ("daft_native", "fused", 2)):
            plans = []
            for count in ("auto", 1):
                resources = ImageWorkerResources(gpu_workers=count, allowed_gpus=("GPU-a",))
                plan = resources.resolve(arm=arm, pipeline=pipeline, environ={}, probe=lambda token: token)
                self.assertEqual(resources.ray_slots(plan, arm, pipeline), slots)
                plans.append(plan.selected_gpu_uuids)
            self.assertEqual(plans, [("GPU-a",), ("GPU-a",)])

    def test_unqualified_precision_or_execution_scope_fails_before_any_gpu_probe(self):
        cases = [({}, {}, "semloom_static", "NVIDIA_TF32_OVERRIDE"),
                 ({"NVIDIA_TF32_OVERRIDE": "1"}, {}, "ray_data", "NVIDIA_TF32_OVERRIDE"),
                 ({"NVIDIA_TF32_OVERRIDE": "0"}, {}, "semloom_hse", "HSE"),
                 ({"NVIDIA_TF32_OVERRIDE": "0"}, {}, "external", "HSE"),
                 ({"NVIDIA_TF32_OVERRIDE": "0"}, {"cpu_workers": 2}, "semloom_static", "one CPU"),
                 ({"NVIDIA_TF32_OVERRIDE": "0"}, {"prepare_inflight": 2}, "semloom_static", "stage depths"),
                 ({"NVIDIA_TF32_OVERRIDE": "0"}, {"model_inflight": 2}, "ray_data", "stage depths")]
        for environment, values, arm, reason in cases:
            probe = Mock()
            with self.subTest(arm=arm, values=values, environment=environment), \
                    patch.dict(os.environ, environment, clear=True), self.assertRaisesRegex(ValueError, reason):
                ImageWorkerResources(allowed_gpus=("GPU-a",), **values).resolve(arm=arm, environ={}, probe=probe)
            probe.assert_not_called()

    @patch.dict(os.environ, {"NVIDIA_TF32_OVERRIDE": "0"}, clear=True)
    @patch("src.modalities.image.resource_budget.available_cpu_slots", return_value=8)
    def test_multiple_devices_and_inadequate_cpu_or_active_capacity_are_rejected(self, _slots):
        cases = [(dict(allowed_gpus=("GPU-a", "GPU-b")), {}, "one GPU"),
                 (dict(allowed_gpus=("GPU-a",), ray_num_cpus=2), {}, "source/consumer"),
                 (dict(allowed_gpus=("GPU-a",), ray_num_cpus=9), {}, "CPU allocation"),
                 (dict(allowed_gpus=("GPU-a",)), {"max_active_batches": 0}, "active_batches")]
        for values, options, reason in cases:
            with self.subTest(values=values, options=options), self.assertRaisesRegex(ValueError, reason):
                ImageWorkerResources(**values).resolve(arm="semloom_static", environ={},
                                                       probe=lambda token: token, **options)

    def test_invalid_resource_values_are_rejected_before_launch(self):
        for values in ({"gpu_workers": True}, {"cpu_workers": False}, {"prepare_inflight": 0},
                       {"model_inflight": "AUTO"}, {"ray_num_cpus": 1.5}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                ImageWorkerResources(**values)
