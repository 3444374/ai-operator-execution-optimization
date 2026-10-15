"""CPU-only deployment boundaries and pinned native launch plans."""
from dataclasses import replace
import os
import unittest
from unittest.mock import Mock, patch

from src.infrastructure.gpu_allocation import resolve_gpu_allocation
from src.serving.vllm_resources import VllmResources, verify_parsed_launch
from src.modalities.image.resource_budget import ImageWorkerResources
from src.modalities.image.worker_identity import assigned_gpu_uuid
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions


class GpuAllocationTests(unittest.TestCase):
    def test_auto_discovers_only_the_explicit_deployment_and_keeps_order(self):
        probe = Mock(side_effect=lambda token: {"3": "GPU-b", "1": "GPU-a"}[token])
        plan = resolve_gpu_allocation("auto", ["3", "1"], environ={}, probe=probe)
        self.assertEqual(plan.selected_gpu_uuids, ("GPU-b", "GPU-a"))
        self.assertEqual(probe.call_args_list, [(("3",),), (("1",),)])
        self.assertEqual(plan.report()["requested_count"], "auto")
        self.assertEqual(plan.count, 2)

    def test_missing_allocation_never_queries_or_assumes_all_host_gpus(self):
        probe = Mock()
        for env in ({}, {"NVIDIA_VISIBLE_DEVICES": "all"}):
            with self.subTest(env=env), self.assertRaisesRegex(ValueError, "require"):
                resolve_gpu_allocation(environ=env, probe=probe)
        probe.assert_not_called()

    def test_masks_intersect_and_outside_uuid_is_not_probed(self):
        probe = Mock(side_effect=lambda uid: uid)
        plan = resolve_gpu_allocation(1, ["GPU-a", "GPU-b", "GPU-c"],
            environ={"CUDA_VISIBLE_DEVICES": "GPU-c,GPU-b", "NVIDIA_VISIBLE_DEVICES": "GPU-b"}, probe=probe)
        self.assertEqual(plan.selected_gpu_uuids, ("GPU-b",))
        probe.assert_called_once_with("GPU-b")
        with self.assertRaisesRegex(ValueError, "exceeds"):
            resolve_gpu_allocation(2, ["GPU-a", "GPU-b"],
                environ={"CUDA_VISIBLE_DEVICES": "GPU-b"}, probe=lambda uid: uid)
        probe.reset_mock()
        with self.assertRaisesRegex(ValueError, "requires UUID"):
            resolve_gpu_allocation("auto", ["0", "1"], environ={"CUDA_VISIBLE_DEVICES": "GPU-a"}, probe=probe)
        probe.assert_not_called()

    def test_gpu_ids_and_uuid_cuda_masks_are_supported_explicit_allocations(self):
        for env, expected_source in (({"GPU_IDS": "2,0"}, "GPU_IDS"),
                                     ({"CUDA_VISIBLE_DEVICES": "GPU-b,GPU-a"}, "CUDA_VISIBLE_DEVICES")):
            plan = resolve_gpu_allocation(environ=env,
                probe=lambda token: {"2": "GPU-b", "0": "GPU-a"}.get(token, token))
            self.assertEqual(plan.selected_gpu_uuids, ("GPU-b", "GPU-a"))
            self.assertEqual(plan.allocation_source, expected_source)

    def test_ambiguous_cuda_ordinals_invalid_or_duplicate_physical_ids_fail(self):
        probe = Mock(return_value="GPU-a")
        for env in ({"CUDA_VISIBLE_DEVICES": "0,1"}, {"NVIDIA_VISIBLE_DEVICES": "0"}):
            with self.subTest(env=env), self.assertRaisesRegex(ValueError, "UUID"):
                resolve_gpu_allocation(allowed_gpus=["0"], environ=env, probe=probe)
        probe.assert_not_called()
        for values in ([], ["all"], ["-1"], ["0", "0"], ["MIG-a"], [True], "0, 1"):
            with self.subTest(values=values), self.assertRaises(ValueError):
                resolve_gpu_allocation(allowed_gpus=values, environ={}, probe=probe)
        with self.assertRaisesRegex(ValueError, "duplicate physical"):
            resolve_gpu_allocation(allowed_gpus=["0", "GPU-a"], environ={}, probe=probe)

    def test_oversized_invalid_and_zero_device_counts_are_rejected(self):
        for count in (True, False, 0, -1, 257, 1.0, "2", "AUTO", None):
            with self.subTest(count=count), self.assertRaises(ValueError):
                resolve_gpu_allocation(count, ["GPU-a"], environ={}, probe=lambda x: x)
        with self.assertRaisesRegex(ValueError, "exceeds"):
            resolve_gpu_allocation(2, ["GPU-a"], environ={}, probe=lambda x: x)

    def test_visibility_is_restored_on_success_and_cancellation(self):
        plan = resolve_gpu_allocation(1, ["GPU-b", "GPU-a"], environ={}, probe=lambda x: x)
        with patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "GPU-a"}):
            with self.assertRaises(KeyboardInterrupt):
                with plan.visibility():
                    self.assertEqual(os.environ["CUDA_VISIBLE_DEVICES"], "GPU-b")
                    raise KeyboardInterrupt()
            self.assertEqual(os.environ["CUDA_VISIBLE_DEVICES"], "GPU-a")

    def test_worker_checks_actual_gpu_before_model_load(self):
        from types import SimpleNamespace
        torch = SimpleNamespace(cuda=SimpleNamespace(device_count=lambda: 1,
            get_device_properties=lambda device: SimpleNamespace(uuid="GPU-b")))
        self.assertEqual(assigned_gpu_uuid(torch, "cuda", ("GPU-b",)), "GPU-b")
        with self.assertRaisesRegex(ValueError, "outside"):
            assigned_gpu_uuid(torch, "cuda", ("GPU-a",))
        torch.cuda.device_count = lambda: 2
        with self.assertRaisesRegex(ValueError, "exactly one"):
            assigned_gpu_uuid(torch, "cuda", ("GPU-a", "GPU-b"))


class NativeResourcePlanTests(unittest.TestCase):
    def test_effective_vendor_parser_namespace_must_match_local_topology_and_endpoint(self):
        from types import SimpleNamespace
        parsed = SimpleNamespace(data_parallel_size=2, tensor_parallel_size=1, pipeline_parallel_size=1,
            data_parallel_backend="mp", distributed_executor_backend="mp", nnodes=1, node_rank=0,
            headless=False, data_parallel_rank=None, data_parallel_start_rank=None, data_parallel_size_local=None,
            data_parallel_hybrid_lb=False, data_parallel_external_lb=False, data_parallel_multi_port_external_lb=False,
            enable_expert_parallel=False, enable_elastic_ep=False, device_ids=None,
            decode_context_parallel_size=1, prefill_context_parallel_size=1, port=8000, uds=None, grpc=False)
        resolved = dict(data_parallel_size=2, tensor_parallel_size=1)
        verify_parsed_launch(parsed, resolved, 8000)
        for name, value in (("data_parallel_rank", 0), ("nnodes", 2), ("device_ids", [3]),
                            ("data_parallel_external_lb", True), ("port", 8001), ("headless", True)):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "effective"):
                verify_parsed_launch(SimpleNamespace(**dict(vars(parsed), **{name: value})), resolved, 8000)

    def test_one_two_and_four_gpu_runs_use_same_vllm_logical_endpoint_and_native_dp(self):
        for count in (1, 2, 4):
            allowed = ["GPU-" + str(i) for i in range(count)]
            plan, argv, report = VllmResources(allowed_gpus=allowed).resolve(
                ["--model", "fixture", "--port", "8000"], environ={}, probe=lambda x: x)
            self.assertEqual(plan.count, count)
            self.assertEqual(argv[argv.index("--data-parallel-size") + 1], str(count))
            self.assertEqual(report["resources_resolved"]["load_balancing"], "vllm_internal")
            self.assertEqual(report["resources_resolved"]["rank_gpu_uuids"], [(x,) for x in allowed])
            self.assertEqual(argv.count("--port"), 1)

    def test_tp_is_explicit_and_incompatible_topologies_fail_before_probe(self):
        probe = Mock(side_effect=lambda x: x)
        config = VllmResources(allowed_gpus=["GPU-a", "GPU-b", "GPU-c", "GPU-d"], tensor_parallel_size=2)
        _, _, report = config.resolve([], environ={}, probe=probe)
        self.assertEqual(report["resources_resolved"]["data_parallel_size"], 2)
        self.assertEqual(report["resources_resolved"]["rank_gpu_uuids"], [("GPU-a", "GPU-b"), ("GPU-c", "GPU-d")])
        with self.assertRaisesRegex(ValueError, "divisible"):
            replace(config, gpu_count=3).resolve([], environ={}, probe=probe)
        probe.reset_mock()
        for argv in (["--data-parallel-size=2"], ["-tp", "2"], ["--headless"], ["--data-parallel-backend", "ray"],
                     ["-dpn", "0"], ["-dph"], ["-dpe"], ["-dpm"], ["-tp2"], ["--device-ids", "GPU-z"],
                     ["--data_parallel_rank", "0"], ["--config", "fixture.yaml"]):
            with self.subTest(argv=argv), self.assertRaisesRegex(ValueError, "owns native"):
                config.resolve(argv, environ={}, probe=probe)
        with self.assertRaisesRegex(ValueError, "environment overrides"):
            config.resolve([], environ={"VLLM_DP_RANK": "1"}, probe=probe)
        probe.assert_not_called()

    @patch("src.modalities.image.resource_budget.available_cpu_slots", return_value=16)
    def test_image_worker_count_and_preparation_depth_are_independent(self, _):
        resources = ImageWorkerResources(allowed_gpus=["GPU-a", "GPU-b"], cpu_workers=3, ray_num_cpus=8,
                                         prepare_inflight=2, model_inflight=2)
        plan = resources.resolve(arm="semloom_hse", max_active_batches=4, environ={}, probe=lambda x: x)
        self.assertEqual(plan.count, 2)
        self.assertEqual((resources.cpu_workers, resources.prepare_inflight), (3, 2))
        with self.assertRaisesRegex(ValueError, "depths"):
            replace(resources, model_inflight=3).resolve(arm="semloom_hse", max_active_batches=4, environ={}, probe=lambda x: x)
        with self.assertRaisesRegex(ValueError, "CPU slots"):
            replace(resources, ray_num_cpus=4).resolve(arm="semloom_hse", environ={}, probe=lambda x: x)
        for arm in ("stock", "direct", "external", "imlane"):
            with self.subTest(arm=arm), self.assertRaisesRegex(ValueError, "not multi-device"):
                resources.resolve(arm=arm, environ={}, probe=Mock())

    def test_native_http_range_guard_uses_maximum_without_equating_gpus_and_actors(self):
        options = NativeGraphOptions(concurrency=8, num_threads=5, ray_min_actors=1, ray_actors=4)
        self.assertEqual(options.ray_actor_pool, (1, 4))
        self.assertEqual(options.ray_active_http_upper_bound, 8)
        options.validate_backend("ray-data")
        with self.assertRaisesRegex(ValueError, "upper bound"):
            replace(options, concurrency=7).validate_backend("ray-data")
        with self.assertRaisesRegex(ValueError, "spare CPU"):
            replace(options, num_threads=4).validate_backend("ray-data")
        with self.assertRaisesRegex(ValueError, "minimum"):
            replace(options, ray_min_actors=5)
        self.assertEqual(NativeGraphOptions().ray_actor_pool, (2, 2))


if __name__ == "__main__":
    unittest.main()
