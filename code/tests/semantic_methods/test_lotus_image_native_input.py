"""Input/graph fixtures; only the explicit processor test uses a real SDK."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import numpy as np
from PIL import Image
import pytest

from src.modalities.image import clip
from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingResult
from src.semantic_methods.lotus import image_backend, image_rm
from src.baselines.image.frameworks.ray_data import RayDataClipImagePredictor, RayDataClipPredictor


def png(image):
    stream = io.BytesIO()
    image.save(stream, format="PNG")
    return stream.getvalue()


def pictures():
    rng = np.random.default_rng(39)
    return [Image.fromarray(rng.integers(0, 256, (13 + i, 19 + i, 3), dtype=np.uint8)) for i in range(3)]


def source_entry(image, raw):
    return image, raw, image.size, hashlib.sha256(image.tobytes()).hexdigest()


class SlowProcessor:
    """Explicit fixture kernel, not a CLIP mathematical qualification."""
    instances = []
    def __init__(self, *args, **kwargs):
        self.encoded_rows = 0
        self.decoded_calls = []
        type(self).instances.append(self)

    def preprocess(self, images):
        self.encoded_rows += len(images)
        return self.preprocess_decoded([clip.decode_rgb_image(value) for value in images])

    def preprocess_decoded(self, images):
        self.decoded_calls.append(images)
        return np.asarray([np.asarray(image).mean(axis=(0, 1)) for image in images],
                          dtype=np.float32)[:, :, None, None]


class Actor:
    def __init__(self, model_revision, processor_revision=None, **kwargs):
        self.semantics = EmbeddingSemantics(model_revision, processor_revision or model_revision, 512)
        self._reuse_probe = None
        self.batches = []

    def embed(self, batch):
        self.batches.append(batch)
        matrix = np.zeros((len(batch.doc_ids), 512), dtype=np.float32)
        matrix[:, :3] = batch.payload[:, :, 0, 0] + 1
        matrix /= np.linalg.norm(matrix, axis=1, keepdims=True)
        return ImageEmbeddingResult(batch.doc_ids, matrix, self.semantics, 0)

    def synchronize(self): pass


@pytest.fixture
def model_fixture(monkeypatch):
    SlowProcessor.instances.clear()
    monkeypatch.setattr(clip, "ClipImagePreprocessor", SlowProcessor)
    monkeypatch.setattr(clip, "ClipTensorActor", Actor)
    monkeypatch.setattr(image_backend, "ClipImagePreprocessor", SlowProcessor)
    monkeypatch.setattr(image_backend, "ClipTensorActor", Actor)


@pytest.fixture
def rm_fixture(monkeypatch):
    class RM:
        def __call__(self, docs): return self._embed(docs)
    monkeypatch.setitem(sys.modules, "lotus.models.rm", SimpleNamespace(RM=RM))


def test_slow_preprocessor_extracts_the_identical_processor_call_without_forcing_a_copy():
    images = pictures()
    native = clip.ClipImagePreprocessor.__new__(clip.ClipImagePreprocessor)
    expected = np.ones((3, 3, 2, 2), dtype=np.float32)
    native.processor = Mock(return_value={"pixel_values": expected})
    assert native.preprocess_decoded(images) is expected
    native.processor.assert_called_once_with(images=images, return_tensors="np")
    native.processor.reset_mock()
    assert native.preprocess([png(image) for image in images]) is expected
    received = native.processor.call_args.kwargs["images"]
    for before, after in zip(images, received, strict=True):
        np.testing.assert_array_equal(before, after)
    assert native.processor.call_args.kwargs["return_tensors"] == "np"


def test_decoded_arrays_keep_native_inference_and_invalid_images_fail_before_processor():
    native = clip.ClipImagePreprocessor.__new__(clip.ClipImagePreprocessor)
    native.processor = Mock(return_value={"pixel_values": np.ones((1, 3, 2, 2), np.float32)})
    image = np.zeros((3, 9, 3), np.uint8)
    native.preprocess_decoded([image])
    assert "input_data_format" not in native.processor.call_args.kwargs
    native.processor.reset_mock()
    for invalid in (Image.new("RGBA", (4, 5)), np.zeros((4, 5, 3), np.float32), "path.jpg"):
        with pytest.raises(TypeError, match="RGB PIL"):
            native.preprocess_decoded([invalid])
    native.processor.assert_not_called()


def test_direct_defaults_to_rgb_and_keeps_batch_tail_ids_and_full_vectors(model_fixture):
    images, ids = pictures(), ("a", "图", "tail")
    native = image_backend.DirectImageExecutor(model="fixture", batch_size=2)
    actual = native.embed_batch(ids, images)
    assert native.input_kind == "rgb_pil" and actual.doc_ids == ids
    expected = np.zeros((3, 512), np.float32)
    expected[:, :3] = np.asarray([np.asarray(image).mean(axis=(0, 1)) for image in images], np.float32) + 1
    expected /= np.linalg.norm(expected, axis=1, keepdims=True)
    np.testing.assert_array_equal(actual.embeddings, expected)
    assert native.processor.encoded_rows == 0
    assert [batch.doc_ids for batch in native.actor.batches] == [("a", "图"), ("tail",)]
    assert native.processor.decoded_calls[0][0] is images[0]


def test_ray_pipeline_default_and_invalid_choice():
    assert image_backend.RayDataImageExecutor(model="fixture").pipeline == "fused"
    with pytest.raises(ValueError, match="fused or staged"):
        image_backend.RayDataImageExecutor(model="fixture", pipeline="unknown")


def test_rm_native_pil_passes_objects_without_encoding_and_keeps_mixed_order(model_fixture, rm_fixture, monkeypatch):
    executor = image_backend.DirectImageExecutor(model="fixture")
    def text(docs):
        values = np.zeros((len(docs), 512), dtype=np.float32)
        values[:, 7] = 1
        return values
    monkeypatch.setattr(image_rm, "image_bytes", Mock(side_effect=AssertionError("no PNG")))
    images = pictures()
    rm = image_rm.make_image_rm(executor, text)
    actual = rm([images[0], "bird", images[1]])
    assert actual.shape == (3, 512) and actual.dtype == np.float32
    assert actual[1, 7] == 1
    assert executor.processor.decoded_calls[0] == [images[0], images[1]]
    record = rm.observation.calls[0]
    assert record["input_kind"] == "rgb_pil" and record["image_encoding"] == "not_performed"
    assert record["encoded_total_bytes"] == record["encoded_peak_single_bytes"] == 0
    assert record["image_encode_wall_ns"] is record["image_encode_cpu_ns"] is None
    assert record["png_compress_level"] is None


def test_rm_native_keeps_reordered_ids_and_rejects_missing_ids(rm_fixture):
    class Executor:
        input_kind = "rgb_pil"
        semantics = EmbeddingSemantics("fixture", "fixture", 512)
        missing = False
        def embed_batch(self, ids, images):
            matrix = np.eye(512, dtype=np.float32)[:len(ids)]
            output_ids = ("unexpected",) if self.missing else ids[::-1]
            return ImageEmbeddingResult(output_ids, matrix[::-1], self.semantics, 0)
    executor = Executor()
    rm = image_rm.make_image_rm(executor, None)
    actual = rm(pictures()[:2])
    assert actual.shape == (2, 512) and actual.argmax(axis=1).tolist() == [0, 1]
    executor.missing = True
    with pytest.raises(ValueError, match="missing or unexpected"):
        rm(pictures()[:1])
    assert rm.observation.calls[-1]["status"] == "failed"


@pytest.mark.parametrize("kind", ["RGB_PIL", "unknown", None])
def test_rm_rejects_unknown_representation_before_native_rm_import(kind):
    with pytest.raises(ValueError, match="input_kind"):
        image_rm.make_image_rm(SimpleNamespace(input_kind=kind), None)


def test_rm_native_rejects_nonrgb_image_before_model(model_fixture, rm_fixture):
    executor = image_backend.DirectImageExecutor(model="fixture")
    rm = image_rm.make_image_rm(executor, None)
    with pytest.raises(TypeError, match="RGB PIL"):
        rm([Image.new("RGBA", (3, 4))])
    assert executor.actor.batches == []


class EncodedExecutor:
    """Record transported bytes; this fixture does not qualify a model kernel."""
    input_kind = "encoded_bytes"
    semantics = EmbeddingSemantics("fixture", "fixture", 512)

    def __init__(self):
        self.calls = []

    def embed_batch(self, ids, payload):
        self.calls.append((ids, payload))
        matrix = np.zeros((len(ids), 512), np.float32)
        matrix[:, 0] = 1
        return ImageEmbeddingResult(ids, matrix, self.semantics, 0)


def test_rm_source_bytes_reuses_the_identical_decoded_image_without_encoding(rm_fixture, monkeypatch):
    encoded = io.BytesIO()
    pictures()[0].save(encoded, format="JPEG")
    raw = encoded.getvalue()
    image = clip.decode_rgb_image(raw)
    executor = EncodedExecutor()
    source_images = {id(image): source_entry(image, raw)}
    rm = image_rm.make_image_rm(executor, None, source_images=source_images)
    monkeypatch.setattr(image_rm, "image_bytes", Mock(side_effect=AssertionError("no PNG fallback")))
    assert rm([image]).shape == (1, 512)
    assert executor.calls[0][1][0] is raw
    record = rm.observation.calls[-1]
    assert record["input_kind"] == "encoded_bytes"
    assert record["image_encoding"] == "source_bytes_reused"
    assert (record["source_reused_rows"], record["png_fallback_rows"]) == (1, 0)
    assert record["image_encode_wall_ns"] is record["image_encode_cpu_ns"] is None
    assert record["encoded_total_bytes"] == record["encoded_peak_single_bytes"] == len(raw)
    assert record["png_compress_level"] is None


@pytest.mark.parametrize("invalid", ["missing", "different_object", "not_bytes", "empty", "size", "digest", "malformed", "numpy_size", "numpy_digest"])
def test_unusable_source_entry_falls_back_to_current_pixels(rm_fixture, invalid):
    image = pictures()[0]
    source = {id(image): source_entry(image, png(image))}
    if invalid == "missing": source.clear()
    if invalid == "different_object": source[id(image)] = source_entry(image.copy(), png(image))
    if invalid == "not_bytes": source[id(image)] = source_entry(image, "path.jpg")
    if invalid == "empty": source[id(image)] = source_entry(image, b"")
    if invalid == "size": source[id(image)] = (image, png(image), (1, 1), source[id(image)][3])
    if invalid == "digest": source[id(image)] = (image, png(image), image.size, "0" * 64)
    if invalid == "malformed": source[id(image)] = (image, png(image))
    if invalid == "numpy_size": source[id(image)] = (image, png(image), np.asarray(image.size), source[id(image)][3])
    if invalid == "numpy_digest": source[id(image)] = (image, png(image), image.size, np.asarray([source[id(image)][3]]))
    executor = EncodedExecutor()
    rm = image_rm.make_image_rm(executor, None, source_images=source)
    rm([image])
    payload = executor.calls[0][1][0]
    assert payload.startswith(b"\x89PNG\r\n\x1a\n")
    np.testing.assert_array_equal(clip.decode_rgb_image(payload), image)
    record = rm.observation.calls[-1]
    assert (record["source_reused_rows"], record["png_fallback_rows"]) == (0, 1)
    assert record["image_encoding"] == "lossless_png_fallback"
    assert record["png_compress_level"] == 1


@pytest.mark.parametrize("mutation", ["copy", "crop", "transpose", "putpixel", "thumbnail"])
def test_source_copy_transform_or_inplace_mutation_encodes_current_pixels(rm_fixture, mutation):
    original = pictures()[0]
    raw = png(original)
    source = {id(original): source_entry(original, raw)}
    image = original
    if mutation == "copy": image = original.copy()
    elif mutation == "crop": image = original.crop((1, 2, 9, 10))
    elif mutation == "transpose": image = original.transpose(Image.Transpose.FLIP_LEFT_RIGHT)
    elif mutation == "putpixel": image.putpixel((0, 0), (1, 2, 3))
    else: image.thumbnail((5, 5))
    executor = EncodedExecutor()
    rm = image_rm.make_image_rm(executor, None, source_images=source)
    rm([image])
    payload = executor.calls[0][1][0]
    assert payload is not raw
    np.testing.assert_array_equal(clip.decode_rgb_image(payload), image)
    assert rm.observation.calls[-1]["png_fallback_rows"] == 1


def test_mixed_source_cache_reuses_only_matching_rows_and_submits_once(rm_fixture):
    matched, changed, uncached = pictures()
    raw, stale = png(matched), png(changed)
    sources = {id(matched): source_entry(matched, raw), id(changed): source_entry(changed, stale)}
    changed.putpixel((0, 0), (255, 1, 7))
    executor = EncodedExecutor()
    rm = image_rm.make_image_rm(executor, None, source_images=sources)
    rm([matched, changed, uncached])
    assert len(executor.calls) == 1
    ids, payloads = executor.calls[0]
    assert ids == ("0", "1", "2") and payloads[0] is raw and payloads[1] is not stale
    for payload, image in zip(payloads, (matched, changed, uncached), strict=True):
        np.testing.assert_array_equal(clip.decode_rgb_image(payload), image)
    record = rm.observation.calls[-1]
    assert (record["source_reused_rows"], record["png_fallback_rows"]) == (1, 2)
    assert record["image_encoding"] == "mixed_source_and_png"
    assert record["encoded_total_bytes"] == sum(map(len, payloads))


def test_source_cache_configuration_must_be_a_mapping(rm_fixture):
    with pytest.raises(ValueError, match="identity mapping"):
        image_rm.make_image_rm(EncodedExecutor(), None, source_images=[])


@pytest.mark.parametrize("source_bytes", [False, True])
@pytest.mark.parametrize("invalid", ["rgba", "empty", "closed", "lazy_closed"])
def test_entire_native_rm_input_is_validated_before_payload_or_submit(rm_fixture, monkeypatch, source_bytes, invalid):
    images = pictures()
    if invalid == "rgba": bad = Image.new("RGBA", (4, 5))
    elif invalid == "empty": bad = Image.new("RGB", (0, 5))
    elif invalid == "closed":
        bad = Image.new("RGB", (4, 5))
        bad.close()
    else:
        stream = io.BytesIO(png(images[1]))
        bad = Image.open(stream)
        stream.close()
    executor = SimpleNamespace(input_kind="encoded_bytes" if source_bytes else "rgb_pil",
        semantics=EmbeddingSemantics("fixture", "fixture", 512), embed_batch=Mock())
    source = {} if source_bytes else None
    rm = image_rm.make_image_rm(executor, None, source_images=source)
    encoder = Mock(side_effect=AssertionError("payload prepared before suffix validation"))
    monkeypatch.setattr(image_rm, "image_bytes", encoder)
    with pytest.raises((TypeError, ValueError, OSError)):
        rm([images[0], bad])
    executor.embed_batch.assert_not_called()
    encoder.assert_not_called()
    assert rm.observation.calls[-1]["status"] == "failed"


def test_ray_fused_native_callable_keeps_processor_math_and_inherited_reuse_columns(model_fixture):
    predictor = RayDataClipImagePredictor("fixture", "fixture", "float32", 1, 1)
    images = pictures()
    probe = dict(model_uid="fixture-model", pid=19, image_returns=1, image_returned_rows=3)
    predictor._actor._reuse_probe = SimpleNamespace(snapshot=lambda: probe)
    native = predictor({"doc_id": ["a", "b", "c"], "image": [np.asarray(image) for image in images]})
    direct = image_backend.DirectImageExecutor(model="fixture")
    expected = direct.embed_batch(("a", "b", "c"), images)
    np.testing.assert_array_equal(native["embedding"], expected.embeddings)
    assert native["_semloom_model_uid"].tolist() == ["fixture-model"] * 3
    assert native["_semloom_model_embed_calls"].tolist() == [1] * 3
    assert predictor._preprocessor.encoded_rows == 0
    assert issubclass(RayDataClipImagePredictor, RayDataClipPredictor)


def test_ray_staged_cpu_callable_accepts_rgb_arrays_without_encoded_roundtrip(model_fixture):
    prepare = image_backend.SlowClipPrepare.__new__(image_backend.SlowClipPrepare)
    prepare.processor = SlowProcessor()
    images = [np.asarray(image) for image in pictures()]
    result = prepare(dict(doc_id=["a", "图", "tail"], image=images))
    assert result["doc_id"].tolist() == ["a", "图", "tail"]
    assert prepare.processor.encoded_rows == 0
    assert prepare.processor.decoded_calls[0][0] is images[0]
    expected = np.asarray([image.mean(axis=(0, 1)) for image in images], np.float32)[:, :, None, None]
    np.testing.assert_array_equal(result["pixel_values"], expected)


@pytest.mark.parametrize("array", [np.zeros((9, 7, 3), np.float32), np.zeros((3, 9, 7), np.uint8),
                                    np.zeros((9, 7, 4), np.uint8), np.zeros((9, 7), np.uint8)])
def test_ray_native_array_layout_rejected_before_model_call(model_fixture, array):
    predictor = RayDataClipImagePredictor("fixture", "fixture", "float32", 1, 1)
    with pytest.raises(ValueError, match="uint8 HWC RGB"):
        predictor({"doc_id": ["a"], "image": [array]})
    assert predictor._actor.batches == []


def ray_graph(monkeypatch, *, error=None):
    values = np.zeros((3, 512), np.float32)
    values[:, 0] = 1
    iterator = Mock()
    iterator.__iter__ = Mock(return_value=iterator)
    batches = iter([dict(doc_id=["a", "b", "c"], embedding=values)])
    iterator.__next__ = Mock(side_effect=error if error is not None else lambda: next(batches))
    iterator.close = Mock()
    owner = SimpleNamespace(shutdown=Mock())
    graph = Mock(_current_executor=owner)
    graph.map_batches.return_value = graph
    graph.iter_batches.return_value = iterator
    ray, data = ModuleType("ray"), ModuleType("ray.data")
    ray.data, ray.__path__, ray.is_initialized = data, [], lambda: True
    data.from_items = Mock(return_value=graph)
    data.ActorPoolStrategy = Mock(side_effect=lambda **kwargs: kwargs)
    monkeypatch.setitem(sys.modules, "ray", ray)
    monkeypatch.setitem(sys.modules, "ray.data", data)
    return data, graph, iterator, owner


@pytest.mark.parametrize("pipeline", ["fused", "staged"])
def test_ray_graph_keeps_native_fused_or_cpu_gpu_stages_with_rgb_source(monkeypatch, pipeline):
    data, graph, iterator, owner = ray_graph(monkeypatch)
    images = pictures()
    executor = image_backend.RayDataImageExecutor(model="fixture", pipeline=pipeline)
    result = executor.embed_batch(("a", "b", "c"), images)
    assert result.doc_ids == ("a", "b", "c")
    maps = graph.map_batches.call_args_list
    assert len(maps) == (1 if pipeline == "fused" else 2)
    assert maps[-1].args[0] is (RayDataClipImagePredictor if pipeline == "fused" else RayDataClipPredictor)
    assert maps[-1].kwargs["num_cpus"] == maps[-1].kwargs["num_gpus"] == 1
    assert maps[-1].kwargs["batch_size"] == 32
    assert maps[-1].kwargs["compute"] == {"size": 1}
    assert maps[-1].kwargs["fn_constructor_kwargs"] == dict(model_revision="fixture",
        processor_revision="fixture", dtype="float32", torch_intraop_threads=1,
        torch_interop_threads=1)
    for row, image in zip(data.from_items.call_args.args[0], images, strict=True):
        assert row["image"].dtype == np.uint8 and row["image"].shape[-1] == 3
        np.testing.assert_array_equal(row["image"], image)
    if pipeline == "staged":
        assert maps[0].args[0] is image_backend.SlowClipPrepare
        assert maps[0].kwargs["num_cpus"] == 1 and maps[0].kwargs.get("num_gpus", 0) == 0
        assert maps[0].kwargs["compute"] == {"size": 1}
        assert maps[0].kwargs["fn_constructor_kwargs"] == {"processor_revision": "fixture"}
    iterator.close.assert_called_once()
    owner.shutdown.assert_called_once_with(force=True)


@pytest.mark.parametrize("pipeline", ["fused", "staged"])
def test_ray_native_failure_keeps_primary_and_poisoning_cleanup(monkeypatch, pipeline):
    error = RuntimeError("native graph failed")
    data, graph, iterator, owner = ray_graph(monkeypatch, error=error)
    executor = image_backend.RayDataImageExecutor(model="fixture", pipeline=pipeline)
    with pytest.raises(RuntimeError) as caught:
        executor.embed_batch(("a", "b", "c"), pictures())
    assert caught.value is error
    iterator.close.assert_called_once()
    owner.shutdown.assert_called_once_with(force=True)
    with pytest.raises(RuntimeError, match="unconfirmed"):
        executor.embed_batch(("a", "b", "c"), pictures())
    assert data.from_items.call_count == 1


def cli():
    path = Path(__file__).parents[2] / "scripts/experiments/run_lotus_image_retrieval.py"
    spec = importlib.util.spec_from_file_location("native_image_input_cli_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cli_args(tmp_path, arm):
    return ["--manifest", str(tmp_path / "manifest"), "--image-root", str(tmp_path),
        "--stock-model", str(tmp_path), "--model", str(tmp_path),
        "--embedding-identity", str(tmp_path / "identity.json"), "--arm", arm,
        "--index-dir", str(tmp_path / "index"), "--output-dir", str(tmp_path / "out")]


@pytest.mark.parametrize("arm,pipeline", [("direct", None), ("ray_data", None),
    ("ray_data", "staged"), ("daft_native", None), ("semloom_static", None), ("semloom_hse", None)])
def test_cli_routes_backend_default_input_and_semloom_source_map(tmp_path, monkeypatch, arm, pipeline):
    module = cli()
    for name in ("validate_image_source", "validate_embedding_identity"):
        monkeypatch.setattr(module, name, lambda *args, **kwargs: {})
    monkeypatch.setattr(module, "version", lambda name: "fixture")
    monkeypatch.setattr(module, "configure_torch_thread_pools", lambda *args: {})
    monkeypatch.setitem(sys.modules, "torch", SimpleNamespace(float32="float32"))
    class Stock:
        def __init__(self, *args, **kwargs):
            self.transformer = SimpleNamespace(to=lambda **kwargs: None)
    monkeypatch.setitem(sys.modules, "lotus.models", SimpleNamespace(SentenceTransformersRM=Stock))
    monkeypatch.setitem(sys.modules, "ray", SimpleNamespace(is_initialized=lambda: False,
        init=lambda **kwargs: None, shutdown=Mock()))
    constructor = []
    def executor_factory(**kwargs):
        constructor.append(kwargs)
        return SimpleNamespace(input_kind="encoded_bytes" if arm.startswith("semloom_") else "rgb_pil",
                               identity={}, close=lambda: None)
    for name in ("DirectImageExecutor", "RayDataImageExecutor", "SemLoomImageExecutor"):
        monkeypatch.setattr(module, name, executor_factory)
    if arm == "daft_native":
        from src.baselines.image.frameworks import daft_clip
        monkeypatch.setattr(daft_clip, "DaftClipExecutor", executor_factory)
        monkeypatch.setattr(module, "validate_executor_identity", lambda executor, *a, **k: executor.identity)
    received = []
    def rm_factory(executor, stock, **kwargs):
        received.append(kwargs)
        return SimpleNamespace(observation=SimpleNamespace(calls=[], vectors={}))
    monkeypatch.setattr(module, "make_image_rm", rm_factory)
    raw = png(pictures()[0])
    (tmp_path / "image.png").write_bytes(raw)
    manifest = SimpleNamespace(root=tmp_path, sha256="fixture", entries=[SimpleNamespace(
        path="image.png", sha256=hashlib.sha256(raw).hexdigest(), doc_id="a")])
    monkeypatch.setattr(module, "load_image_file_manifest", lambda *args: manifest)
    frames = []
    def workflow(frame, rm, queries, **kwargs):
        frames.append(frame)
        kwargs["report"].update(status="succeeded", index_s=0.0, queries=[])
    monkeypatch.setattr(module, "run_workflow", workflow)
    (tmp_path / "identity.json").write_text(json.dumps(dict(qualified=True, model_dtype="float32")))
    args = cli_args(tmp_path, arm) + ["--rows", "1", "--ray-temp-dir", str(tmp_path / "ray")]
    if pipeline is not None: args.extend(["--ray-image-pipeline", pipeline])
    assert module.main(args) == 0
    report = json.loads((tmp_path / "out/result.json").read_text())
    assert report["status"] == "succeeded"
    assert report["input_policy"] == "backend_native"
    assert report["executor_input_kind"] == ("encoded_bytes" if arm.startswith("semloom_") else "rgb_pil")
    if arm == "ray_data":
        assert constructor[0]["pipeline"] == (pipeline or "fused")
        assert report["ray_image_pipeline"] == "native_image_arrays_" + (pipeline or "fused") + "_slow_preprocess_clip"
    if arm == "daft_native":
        assert constructor[0]["input_kind"] == "rgb_pil"
    if arm in ("semloom_static", "semloom_hse"):
        image = frames[0].iloc[0]["image"]
        source = received[0]["source_images"][id(image)]
        assert source[0] is image and source[1] == raw
        assert source[2] == image.size and source[3] == hashlib.sha256(image.tobytes()).hexdigest()
    else:
        assert "source_images" not in received[0]


@pytest.mark.skipif(os.environ.get("SEMLOOM_IMAGE_NATIVE_PROCESSOR_TEST") != "1",
                    reason="opt-in Transformers4.57.6 CPU processor check; no model/GPU")
def test_real_slow_processor_png_pil_and_hwc_arrays_are_bitwise_identical():
    from importlib.metadata import version
    assert version("transformers") == "4.57.6"
    processor_path = os.environ["SEMLOOM_CLIP_PROCESSOR_PATH"]
    preprocessor = clip.ClipImagePreprocessor(processor_path, local_files_only=True)
    rng = np.random.default_rng(39)
    for height, width in ((13, 19), (231, 299), (224, 224), (371, 244)):
        image = Image.fromarray(rng.integers(0, 256, (height, width, 3), dtype=np.uint8))
        expected = preprocessor.preprocess([png(image)])
        np.testing.assert_array_equal(preprocessor.preprocess_decoded([image]), expected)
        np.testing.assert_array_equal(preprocessor.preprocess_decoded([np.asarray(image)]), expected)
