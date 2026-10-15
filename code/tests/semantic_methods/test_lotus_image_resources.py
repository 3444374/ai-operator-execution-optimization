"""CPU graph/actor doubles exercise resource wiring and complete ordered output."""
import asyncio
import json
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import numpy as np
from PIL import Image

from src.semantic_methods.lotus.image_backend import SemLoomImageExecutor, RayDataImageExecutor


IDENTITY = dict(processor_revision="fixture", decoder="pillow_convert_rgb", input_size=224,
    representation="prepared_fp32_nchw", layout="NCHW", dtype="float32", decoder_version="fixture",
    processor_version="fixture", execution_variant="clip_slow_pil")


def matrix(ids):
    values = np.zeros((len(ids), 512), dtype=np.float32)
    for index, doc_id in enumerate(ids):
        values[index, ord(doc_id) - ord("a")] = 1
    return values


def ray_actors(*, cpu_identities=None, gpu_ids=("GPU-a", "GPU-b")):
    handles, options = [], []
    cpu_values = iter(cpu_identities or [IDENTITY] * 3)
    gpu_values = iter(gpu_ids)

    def remote(**resources):
        def actor(cls):
            def create(*args, **kwargs):
                options.append((cls.__name__, resources, kwargs))
                ready = (dict(preprocessing_identity=next(cpu_values)) if cls.__name__ == "SlowClipPrepare"
                         else dict(resource_identity=dict(gpu_uuid=next(gpu_values))))
                ready.update(torch_intraop_threads=1, torch_interop_threads=1)
                handle = SimpleNamespace(ready=SimpleNamespace(remote=lambda: ready))
                handles.append(handle)
                return handle
            return SimpleNamespace(remote=create)
        return actor
    return SimpleNamespace(is_initialized=lambda: True, remote=remote, get=lambda values: values, kill=Mock()), handles, options


class SemLoomResourceTests(unittest.TestCase):
    def startup(self, ray):
        with patch.dict(sys.modules, {"ray": ray}), \
                patch("src.semantic_methods.lotus.image_backend.ClipImagePreprocessor",
                      return_value=SimpleNamespace(preprocessing_identity=lambda: IDENTITY)):
            return SemLoomImageExecutor(model="fixture", cpu_workers=3, gpu_workers=2,
                prepare_inflight=2, model_inflight=2, max_active_batches=4,
                allowed_gpu_uuids=("GPU-a", "GPU-b"))

    def test_all_workers_are_reused_and_ready_depth_does_not_equal_gpu_count(self):
        ray, handles, options = ray_actors()
        executor = self.startup(ray)
        self.assertEqual((len(executor.pool.preprocessors), len(executor.pool.gpu_actors)), (3, 2))
        self.assertEqual(len(executor.worker_readiness), 5)
        self.assertTrue(all(values[1].get("max_task_retries") == 0 for values in options))
        self.assertEqual([values[2]["allowed_gpu_uuids"] for values in options[3:]], [("GPU-a", "GPU-b")] * 2)

        def pipeline(source, **kwargs):
            limits = kwargs["limits"]
            self.assertEqual((limits.prepare_inflight, limits.model_inflight), (2, 2))
            self.assertEqual(limits.ready_bytes, 32 * 3 * 224 * 224 * 4 * 4)
            kwargs["embedding_capture"].add(("c",), matrix(("c",)))
            kwargs["embedding_capture"].add(("b", "a"), matrix(("b", "a")))
            return SimpleNamespace(encoded_bytes_peak=0, ready_bytes_peak=0, batch_preprocess_s=(),
                batch_host_copy_s=(), batch_actor_service_s=(), batch_submit_s=(), device_input_bytes=0)

        with patch.dict(sys.modules, {"daft": SimpleNamespace(from_pydict=lambda value: value)}), \
                patch("src.modalities.image.staged_execution.run_semloom_ray_hse_pipeline", side_effect=pipeline) as run:
            for _ in range(2):
                result = executor.embed_batch(("a", "b", "c"), [b"a", b"b", b"c"])
                np.testing.assert_array_equal(result.embeddings, matrix(("a", "b", "c")))
            self.assertEqual(run.call_count, 2)
        self.assertEqual(len(handles), 5)

    def test_every_preprocessor_and_physical_model_owner_is_checked(self):
        for ray, handles, _ in (ray_actors(cpu_identities=[IDENTITY, IDENTITY, dict(IDENTITY, decoder_version="changed")]),
                                ray_actors(gpu_ids=("GPU-a", "GPU-a")),
                                ray_actors(gpu_ids=("GPU-a", "GPU-outside"))):
            with self.subTest(), self.assertRaises(ValueError) as caught:
                self.startup(ray)
            self.assertEqual(ray.kill.call_count, 5)
            self.assertEqual(caught.exception.image_worker_pool_startup.actor_handles, tuple(handles))

    def test_failure_or_cancel_cannot_resubmit_unknown_model_work(self):
        for error in (ValueError("model failed"), asyncio.CancelledError("cancelled")):
            ray, _, _ = ray_actors()
            executor = self.startup(ray)
            with patch.dict(sys.modules, {"daft": SimpleNamespace(from_pydict=lambda value: value)}), \
                    patch("src.modalities.image.staged_execution.run_semloom_ray_hse_pipeline", side_effect=error) as run:
                with self.assertRaises(type(error)):
                    executor.embed_batch(("a",), [b"a"])
                with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                    executor.embed_batch(("a",), [b"a"])
            self.assertEqual(run.call_count, 1)


class RayDataResourceTests(unittest.TestCase):
    def graph(self, batches):
        ray, data = ModuleType("ray"), ModuleType("ray.data")
        ray.data, ray.__path__, ray.is_initialized = data, [], lambda: True
        data.ActorPoolStrategy = lambda **kwargs: kwargs
        graph = Mock()
        graph.map_batches.return_value = graph
        graph.iter_batches.side_effect = lambda **kwargs: iter(batches)
        data.from_items = Mock(return_value=graph)
        return {"ray": ray, "ray.data": data}, graph

    def test_one_and_two_device_graphs_keep_complete_reordered_tail_batches(self):
        batches = [dict(doc_id=np.array(["c"]), embedding=matrix(("c",))),
                   dict(doc_id=np.array(["b", "a"]), embedding=matrix(("b", "a")))]
        for devices in (1, 2):
            for pipeline in ("fused", "staged"):
                modules, graph = self.graph(batches)
                executor = RayDataImageExecutor(model="fixture", batch_size=2, gpu_workers=devices,
                                                cpu_workers=3, pipeline=pipeline)
                with self.subTest(devices=devices, pipeline=pipeline), patch.dict(sys.modules, modules):
                    for _ in range(2):
                        result = executor.embed_batch(("a", "b", "c"), [Image.new("RGB", (4, 5))] * 3)
                        np.testing.assert_array_equal(result.embeddings, matrix(("a", "b", "c")))
                maps = graph.map_batches.call_args_list
                self.assertTrue(all(call.kwargs["max_restarts"] == 0 and call.kwargs["max_task_retries"] == 0 for call in maps))
                self.assertEqual(maps[-1].kwargs["compute"], {"size": devices})
                if pipeline == "staged":
                    self.assertEqual(maps[0].kwargs["compute"], {"size": 3})

    def test_duplicate_or_missing_output_poison_the_executor_without_a_retry(self):
        for ids in (("a", "a", "c"), ("a", "b")):
            modules, graph = self.graph([dict(doc_id=ids, embedding=matrix(ids))])
            executor = RayDataImageExecutor(model="fixture", gpu_workers=2)
            with self.subTest(ids=ids), patch.dict(sys.modules, modules):
                with self.assertRaises(ValueError):
                    executor.embed_batch(("a", "b", "c"), [Image.new("RGB", (4, 5))] * 3)
                with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                    executor.embed_batch(("a",), [Image.new("RGB", (4, 5))])
                self.assertEqual(modules["ray.data"].from_items.call_count, 1)


if __name__ == "__main__":
    unittest.main()
