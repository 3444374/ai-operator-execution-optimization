"""Image identity, full-vector and ordering checks without loading a model."""
import sys
import types

import numpy as np
from PIL import Image
import pytest

from src.modalities.image.clip import decode_rgb_image
from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingResult
from src.semantic_methods.lotus.image_rm import image_bytes, make_image_rm
from src.semantic_methods.lotus.image_workflow import gallery_identity, image_frame, native_settings


@pytest.fixture
def rm_module(monkeypatch):
    module = types.ModuleType("lotus.models.rm")
    class RM:
        def __call__(self, docs):
            return self._embed(docs)
    module.RM = RM
    monkeypatch.setitem(sys.modules, "lotus.models.rm", module)


class ReverseExecutor:
    semantics = EmbeddingSemantics("weights", "processor", 512)
    def __init__(self):
        self.inputs = []
    def embed_batch(self, doc_ids, images):
        self.inputs.extend(images)
        vectors = np.zeros((len(images), 512), dtype=np.float32)
        for i, raw in enumerate(images):
            rgb = decode_rgb_image(raw).getpixel((0, 0))
            vectors[i, max(range(3), key=lambda x: rgb[x])] = 1
        return ImageEmbeddingResult(tuple(reversed(doc_ids)), vectors[::-1].copy(), self.semantics, 0)


def text_rm(docs):
    values = np.zeros((len(docs), 512), dtype=np.float32)
    values[:, 3] = 1
    return values


def test_pil_inputs_are_encoded_as_images_and_reordered(rm_module):
    executor = ReverseExecutor()
    rm = make_image_rm(executor, text_rm)
    result = rm([Image.new("RGB", (13, 17), "red"), "bird", Image.new("RGB", (13, 17), "green")])
    assert result.shape == (3, 512) and result.dtype == np.float32
    assert result.argmax(axis=1).tolist() == [0, 3, 1]
    assert all(decode_rgb_image(x).mode == "RGB" for x in executor.inputs)
    assert rm.observation.calls[0]["image_rows"] == 2
    assert rm.observation.calls[0]["text_rows"] == 1


def test_same_pixels_different_names_and_changed_pixels(rm_module):
    first = Image.new("RGB", (23, 29), "red")
    copy = first.copy()
    first.filename, copy.filename = "bird.jpg", "not-bird.jpg"
    rm = make_image_rm(ReverseExecutor(), text_rm)
    result = rm([first, copy, Image.new("RGB", first.size, "blue")])
    np.testing.assert_array_equal(result[0], result[1])
    assert not np.array_equal(result[0], result[2])
    np.testing.assert_array_equal(np.asarray(decode_rgb_image(image_bytes(first))), np.asarray(first))


def test_object_column_is_one_dimensional_pil_and_paths_are_rejected():
    images = [Image.new("RGB", (13, 17), "red"), Image.new("RGB", (13, 17), "blue")]
    frame = image_frame(("a", "b"), images)
    assert frame["image"].to_numpy().shape == (2,)
    assert all(isinstance(x, Image.Image) for x in frame["image"].tolist())
    with pytest.raises(TypeError, match="never paths"):
        image_frame(("a",), ("bird.jpg",))


def test_matrix_bound_rejects_before_execution(rm_module):
    executor = ReverseExecutor()
    rm = make_image_rm(executor, text_rm, max_output_bytes=2048)
    with pytest.raises(ValueError, match="collection size"):
        rm([Image.new("RGB", (1, 1)), Image.new("RGB", (1, 1))])
    assert executor.inputs == []


@pytest.mark.parametrize("failure", ["duplicate", "missing", "nan", "float64", "norm"])
def test_bad_executor_results_fail_closed(rm_module, failure):
    class Bad(ReverseExecutor):
        def embed_batch(self, ids, images):
            good = super().embed_batch(ids, images)
            if failure == "duplicate":
                return ImageEmbeddingResult((ids[0], ids[0]), good.embeddings, self.semantics, 0)
            if failure == "missing":
                return ImageEmbeddingResult((ids[0], "other"), good.embeddings, self.semantics, 0)
            if failure == "nan":
                return types.SimpleNamespace(doc_ids=good.doc_ids, semantics=self.semantics,
                                             embeddings=np.full((2, 512), np.nan, np.float32))
            values = good.embeddings.astype("float64") if failure == "float64" else good.embeddings * 2
            return ImageEmbeddingResult(good.doc_ids, values, self.semantics, 0)
    rm = make_image_rm(Bad(), text_rm)
    with pytest.raises(ValueError):
        rm([Image.new("RGB", (1, 1), "red"), Image.new("RGB", (1, 1), "blue")])
    assert rm.observation.calls[-1]["status"] == "failed"


def test_index_identity_changes_with_pixel_order_and_model():
    images = [Image.new("RGB", (2, 2), "red"), Image.new("RGB", (2, 2), "blue")]
    a = gallery_identity(image_frame(("a", "b"), images), {"revision": "one"})
    b = gallery_identity(image_frame(("b", "a"), images[::-1]), {"revision": "one"})
    c = gallery_identity(image_frame(("a", "b"), images), {"revision": "two"})
    assert len({a["sha256"], b["sha256"], c["sha256"]}) == 3


def test_entry_rejects_another_owner_and_releases_after_error(monkeypatch):
    from contextlib import contextmanager
    @contextmanager
    def context(**kwargs):
        yield
    monkeypatch.setitem(sys.modules, "lotus", types.SimpleNamespace(settings=types.SimpleNamespace(context=context)))
    with pytest.raises(RuntimeError, match="boom"):
        with native_settings(None, None):
            with pytest.raises(RuntimeError, match="owns this process"):
                with native_settings(None, None):
                    pass
            raise RuntimeError("boom")
    with native_settings(None, None):
        pass
