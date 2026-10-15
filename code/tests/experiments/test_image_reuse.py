"""Local observation fixtures: no real SDK, model, GPU, HTTP or child process."""
from contextlib import nullcontext
import gc
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import weakref

import numpy as np
from PIL import Image

from src.experiments import image_reuse as reuse
from src.modalities.image.clip import ClipTensorActor
from src.modalities.image.contracts import ImageEmbeddingBatch
from src.baselines.image.frameworks.ray_data import RayDataClipPredictor
from src.semantic_methods.lotus.image_backend import RayDataImageExecutor


class HookModule:
    """Only the standard Torch hook signature, explicitly a software fixture."""
    def __init__(self):
        self.before, self.after = {}, {}
        self.output = object()
        self.error = self.setup_error = None

    def _register(self, mapping, hook):
        key = len(mapping)
        mapping[key] = hook
        return SimpleNamespace(remove=lambda: mapping.pop(key, None))

    def register_forward_pre_hook(self, hook, *, with_kwargs):
        assert with_kwargs is True
        return self._register(self.before, hook)

    def register_forward_hook(self, hook, *, with_kwargs):
        assert with_kwargs is True
        if self.setup_error is not None:
            raise self.setup_error
        return self._register(self.after, hook)

    def __call__(self, *args, **kwargs):
        for hook in tuple(self.before.values()):
            assert hook(self, args, kwargs) is None
        if self.error is not None:
            raise self.error
        for hook in tuple(self.after.values()):
            assert hook(self, args, kwargs, self.output) is None
        return self.output


class StockModel:
    def __init__(self):
        self.vision_model, self.text_model = HookModule(), HookModule()


class Tensor:
    def __init__(self, value):
        self.value = np.asarray(value, dtype=np.float32)
        self.shape, self.ndim = self.value.shape, self.value.ndim

    def float(self): return self
    def cpu(self): return self
    def numpy(self): return self.value
    def numel(self): return self.value.size
    def element_size(self): return self.value.itemsize
    def norm(self, *, dim, keepdim):
        return Tensor(np.linalg.norm(self.value, axis=dim, keepdims=keepdim))
    def clamp_min(self, value): return Tensor(np.maximum(self.value, value))
    def __truediv__(self, other): return Tensor(self.value / other.value)


class ClipModel:
    def __init__(self):
        self.config = SimpleNamespace(projection_dim=2)
        self.error = None

    def eval(self): return self
    def to(self, **kwargs): return self
    def get_image_features(self, *, pixel_values):
        if self.error is not None:
            raise self.error
        return Tensor(np.tile([3.0, 4.0], (pixel_values.shape[0], 1)))


class ImageReuseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name) / "reuse"
        self.env = patch.dict(os.environ, {"SEMLOOM_IMAGE_REUSE_DIR": str(self.directory)})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.temporary.cleanup()
        gc.collect()

    def test_default_off_reads_no_clock_writes_no_event_and_attaches_no_hook(self):
        model = StockModel()
        with patch.dict(os.environ, {"SEMLOOM_IMAGE_REUSE_DIR": ""}), \
             patch.object(reuse.time, "monotonic_ns", side_effect=AssertionError("no clock")), \
             patch.object(reuse.uuid, "uuid4", side_effect=AssertionError("no UUID")), \
             patch.object(reuse, "write_private_json", side_effect=AssertionError("no writer")):
            self.assertIsNone(reuse.observe_model(model, role="fixture", device="cpu"))
            self.assertIsNone(reuse.observe_stock_model(model, device="cpu"))
            self.assertEqual(reuse.snapshot_models(), [])
        self.assertFalse(hasattr(model, reuse._ATTRIBUTE))
        self.assertEqual(model.vision_model.before, {})
        self.assertFalse(self.directory.exists())

    def test_one_durable_construction_and_plain_integer_counters_for_same_model(self):
        model = ClipModel()
        probe = reuse.observe_model(model, role="fixture", device="cpu")
        paths = list(self.directory.glob("model-*.json"))
        self.assertEqual(len(paths), 1)
        self.assertEqual(stat.S_IMODE(paths[0].stat().st_mode), 0o600)
        initial = paths[0].read_bytes()
        construction = json.loads(initial)
        self.assertEqual(construction["pid"], os.getpid())
        self.assertEqual(construction["model_object_id"], id(model))
        self.assertEqual(construction["returns"], 0)
        with patch.object(reuse.time, "monotonic_ns", side_effect=AssertionError("no call clock")), \
             patch.object(reuse, "write_private_json", side_effect=AssertionError("no call writer")):
            self.assertIs(reuse.observe_model(model, role="fixture", device="cpu"), probe)
            probe.begin(3)
            probe.returned(3)
            probe.begin(1, "text")
            state = probe.snapshot()
            live = reuse.snapshot_models()
        self.assertEqual((state["attempts"], state["returns"]), (2, 1))
        self.assertEqual((state["attempted_rows"], state["returned_rows"]), (4, 3))
        self.assertEqual((state["image_returns"], state["text_attempts"]), (1, 1))
        self.assertIn(state, live)
        self.assertTrue(all(type(value) in (int, str, bool, type(None)) for value in state.values()))
        self.assertEqual(paths[0].read_bytes(), initial)
        self.assertEqual(list(self.directory.glob("*.json")), paths)

    def test_probe_and_registry_do_not_keep_model_or_actor_alive(self):
        model = ClipModel()
        probe = reuse.observe_model(model, role="fixture", device="cpu")
        model_ref, probe_ref = weakref.ref(model), weakref.ref(probe)
        uid = probe.model_uid
        del model
        gc.collect()
        self.assertIsNone(model_ref())
        self.assertFalse(probe.snapshot()["model_alive"])
        del probe
        gc.collect()
        self.assertIsNone(probe_ref())
        self.assertNotIn(uid, reuse._models)

    def test_private_directory_rejects_git_before_recording(self):
        root = Path(self.temporary.name) / "checkout"
        (root / ".git").mkdir(parents=True)
        model = ClipModel()
        with patch.dict(os.environ, {"SEMLOOM_IMAGE_REUSE_DIR": str(root / "data")}):
            with self.assertRaisesRegex(ValueError, "outside a Git"):
                reuse.observe_model(model, role="fixture", device="cpu")
        self.assertFalse((root / "data").exists())
        self.assertFalse(hasattr(model, reuse._ATTRIBUTE))

    def test_constructor_write_error_is_explicit_and_not_registered(self):
        model = ClipModel()
        error = OSError("fixture write failed")
        with patch.object(reuse, "write_private_json", side_effect=error):
            with self.assertRaises(OSError) as caught:
                reuse.observe_model(model, role="fixture", device="cpu")
        self.assertIs(caught.exception, error)
        self.assertFalse(hasattr(model, reuse._ATTRIBUTE))

    def test_counter_rejects_invalid_rows_and_kind_without_changing_state(self):
        model = ClipModel()
        probe = reuse.observe_model(model, role="fixture", device="cpu")
        initial = probe.snapshot()
        for rows, kind in ((True, "image"), (-1, "image"), (1.0, "image"), (1, "other")):
            with self.subTest(rows=rows, kind=kind), self.assertRaises(ValueError):
                probe.begin(rows, kind)
        self.assertEqual(probe.snapshot(), initial)

    def test_stock_actual_submodule_calls_keep_outputs_and_count_image_text_separately(self):
        model = StockModel()
        probe = reuse.observe_stock_model(model, device="cpu")
        self.assertIs(reuse.observe_stock_model(model, device="cpu"), probe)
        pixels = np.zeros((3, 3, 2, 2))
        tokens = np.zeros((2, 5))
        self.assertIs(model.vision_model(pixel_values=pixels), model.vision_model.output)
        self.assertIs(model.text_model(tokens), model.text_model.output)
        self.assertIs(model.vision_model(pixels), model.vision_model.output)
        state = probe.snapshot()
        self.assertEqual((state["image_returns"], state["image_returned_rows"]), (2, 6))
        self.assertEqual((state["text_returns"], state["text_returned_rows"]), (1, 2))
        self.assertEqual(state["hook_errors"], 0)
        self.assertEqual((len(model.vision_model.before), len(model.vision_model.after)), (1, 1))

    def test_stock_native_error_remains_original_with_unreturned_attempt(self):
        model = StockModel()
        probe = reuse.observe_stock_model(model, device="cpu")
        error = ValueError("native forward failed")
        model.vision_model.error = error
        with self.assertRaises(ValueError) as caught:
            model.vision_model(pixel_values=np.zeros((2, 3, 2, 2)))
        self.assertIs(caught.exception, error)
        self.assertEqual((probe.snapshot()["attempts"], probe.snapshot()["returns"]), (1, 0))

    def test_hook_input_observation_failure_does_not_replace_output_or_native_error(self):
        model = StockModel()
        probe = reuse.observe_stock_model(model, device="cpu")
        self.assertIs(model.vision_model(object()), model.vision_model.output)
        error = RuntimeError("native remains primary")
        model.vision_model.error = error
        with self.assertRaises(RuntimeError) as caught:
            model.vision_model(object())
        self.assertIs(caught.exception, error)
        self.assertEqual(probe.snapshot()["hook_errors"], 3)
        self.assertEqual(probe.snapshot()["hook_error_type"], "AttributeError")

    def test_stock_partial_hook_setup_error_removes_installed_hooks(self):
        model = StockModel()
        error = RuntimeError("fixture registration failed")
        model.text_model.setup_error = error
        with self.assertRaises(RuntimeError) as caught:
            reuse.observe_stock_model(model, device="cpu")
        self.assertIs(caught.exception, error)
        for module in (model.vision_model, model.text_model):
            self.assertEqual((module.before, module.after), ({}, {}))
        self.assertEqual(getattr(model, reuse._ATTRIBUTE).hook_errors, 1)

    def test_stock_hooks_do_not_retain_the_actual_model(self):
        model = StockModel()
        model_ref = weakref.ref(model)
        probe = reuse.observe_stock_model(model, device="cpu")
        del model
        gc.collect()
        self.assertIsNone(model_ref())
        self.assertFalse(probe.snapshot()["model_alive"])

    def actor(self):
        model = ClipModel()
        torch = SimpleNamespace(float16="f16", float32="f32", bfloat16="bf16",
            inference_mode=nullcontext, as_tensor=lambda payload, **kwargs: Tensor(payload),
            get_num_threads=lambda: 1, get_num_interop_threads=lambda: 1)
        transformers = SimpleNamespace(CLIPModel=SimpleNamespace(from_pretrained=lambda *args, **kwargs: model))
        with patch.dict(sys.modules, {"torch": torch, "transformers": transformers}):
            actor = ClipTensorActor("fixture", device="cpu", dtype="float32")
        return actor

    def request(self):
        return ImageEmbeddingBatch(("a", "b"), np.zeros((2, 3, 2, 2), dtype=np.float32),
                                   "preprocessed_tensor", 8, "pixels")

    def test_tensor_actor_default_schema_and_math_are_unchanged(self):
        with patch.dict(os.environ, {"SEMLOOM_IMAGE_REUSE_DIR": ""}), \
             patch.object(reuse.time, "monotonic_ns", side_effect=AssertionError("no clock")):
            actor = self.actor()
            result = actor.embed(self.request())
        self.assertNotIn("model_reuse", actor.ready())
        np.testing.assert_array_equal(result.embeddings, np.tile(np.float32([0.6, 0.8]), (2, 1)))

    def test_tensor_actor_counts_completed_numpy_return_and_preserves_model_error(self):
        actor = self.actor()
        result = actor.embed(self.request())
        np.testing.assert_array_equal(result.embeddings, np.tile(np.float32([0.6, 0.8]), (2, 1)))
        error = RuntimeError("fixture model failed")
        actor._model.error = error
        with self.assertRaises(RuntimeError) as caught:
            actor.embed(self.request())
        self.assertIs(caught.exception, error)
        state = actor.ready()["model_reuse"]
        self.assertEqual((state["image_attempts"], state["image_returns"]), (2, 1))
        self.assertEqual((state["attempted_rows"], state["returned_rows"]), (4, 2))

    def test_tensor_actor_numpy_failure_does_not_count_as_a_success(self):
        actor = self.actor()
        error = RuntimeError("fixture host return failed")
        with patch.object(Tensor, "numpy", side_effect=error):
            with self.assertRaises(RuntimeError) as caught:
                actor.embed(self.request())
        self.assertIs(caught.exception, error)
        state = actor.ready()["model_reuse"]
        self.assertEqual((state["image_attempts"], state["image_returns"]), (1, 0))

    def test_ray_predictor_metadata_follows_actual_success_and_default_has_no_columns(self):
        batch = {"doc_id": np.array(["a", "b"]), "pixel_values": np.zeros((2, 3, 2, 2))}
        for enabled in (False, True):
            with self.subTest(enabled=enabled), patch.dict(os.environ,
                    {"SEMLOOM_IMAGE_REUSE_DIR": str(self.directory) if enabled else ""}):
                actor = self.actor()
                predictor = RayDataClipPredictor.__new__(RayDataClipPredictor)
                predictor._preprocessing_identity = None
                predictor._actor = actor
                output = predictor(batch)
                np.testing.assert_array_equal(output["embedding"], np.tile(np.float32([0.6, 0.8]), (2, 1)))
                if enabled:
                    self.assertEqual(output["_semloom_model_uid"].tolist(), [actor._reuse_probe.model_uid] * 2)
                    self.assertEqual(output["_semloom_model_pid"].tolist(), [os.getpid()] * 2)
                    self.assertEqual(output["_semloom_model_embed_calls"].tolist(), [1, 1])
                    self.assertEqual(output["_semloom_model_rows"].tolist(), [2, 2])
                    self.assertEqual(predictor(batch)["_semloom_model_embed_calls"].tolist(), [2, 2])
                else:
                    self.assertEqual(set(output), {"doc_id", "embedding"})

    def test_ray_driver_preserves_distinct_native_counts_without_forcing_graph_reuse(self):
        values = np.zeros((3, 512), dtype=np.float32)
        values[:, 0] = 1
        batch = dict(doc_id=["a", "b", "c"], embedding=values,
            _semloom_model_uid=["one", "one", "two"], _semloom_model_pid=[12, 12, 34],
            _semloom_model_embed_calls=[1, 1, 2], _semloom_model_rows=[2, 2, 4])
        owner = SimpleNamespace(shutdown=Mock())
        graph = Mock(_current_executor=owner)
        graph.map_batches.return_value = graph
        graph.iter_batches.return_value = iter([batch])
        ray, data = ModuleType("ray"), ModuleType("ray.data")
        ray.data, ray.__path__, ray.is_initialized = data, [], lambda: True
        data.from_items = Mock(return_value=graph)
        data.ActorPoolStrategy = lambda **kwargs: object()
        executor = RayDataImageExecutor(model="fixture")
        with patch.dict(sys.modules, {"ray": ray, "ray.data": data}):
            result = executor.embed_batch(("a", "b", "c"), [Image.new("RGB", (4, 5), "red") for _ in range(3)])
        np.testing.assert_array_equal(result.embeddings, values)
        self.assertEqual(executor.reuse_observations, [
            dict(model_uid="one", pid=12, embed_calls=1, rows=2, batch_rows=2),
            dict(model_uid="two", pid=34, embed_calls=2, rows=4, batch_rows=1)])
        owner.shutdown.assert_called_once_with(force=True)
        self.assertEqual(len(graph.map_batches.call_args_list), 1)

    def test_ray_driver_rejects_partial_metadata_instead_of_claiming_reuse(self):
        executor = RayDataImageExecutor(model="fixture")
        with self.assertRaisesRegex(ValueError, "complete and aligned"):
            executor._observe_reuse_batch(dict(doc_id=["a"], _semloom_model_uid=["one"]))
        self.assertEqual(executor.reuse_observations, [])

    def test_ray_driver_does_not_truncate_fractional_counters_into_reuse_evidence(self):
        executor = RayDataImageExecutor(model="fixture")
        with self.assertRaisesRegex(ValueError, "must be integers"):
            executor._observe_reuse_batch(dict(doc_id=["a"], _semloom_model_uid=["one"],
                _semloom_model_pid=[os.getpid()], _semloom_model_embed_calls=[1.5],
                _semloom_model_rows=[1]))
        self.assertEqual(executor.reuse_observations, [])


if __name__ == "__main__":
    unittest.main()
