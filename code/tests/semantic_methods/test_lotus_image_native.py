"""Actual LOTUS/FAISS qualification with image-aware deterministic RM, no CLIP."""
import numpy as np
from PIL import Image
import pytest

from src.modalities.image.clip import decode_rgb_image
from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingResult
from src.semantic_methods.lotus.image_rm import make_image_rm
from src.semantic_methods.lotus.image_source import validate_image_source
from src.semantic_methods.lotus.image_workflow import RetrievalQuery, image_frame, run_workflow


@pytest.fixture
def rm():
    pytest.importorskip("lotus")
    validate_image_source()
    class Executor:
        semantics = EmbeddingSemantics("fake-weights", "fake-processor", 512)
        def embed_batch(self, ids, images):
            values = np.zeros((len(ids), 512), np.float32)
            for i, raw in enumerate(images):
                rgb = decode_rgb_image(raw).getpixel((0, 0))
                values[i, max(range(3), key=lambda x: rgb[x])] = 1
            return ImageEmbeddingResult(tuple(reversed(ids)), values[::-1].copy(), self.semantics, 0)
    def text(docs):
        values = np.zeros((len(docs), 512), np.float32)
        values[:, 0] = 1
        return values
    return make_image_rm(Executor(), text)


def frame():
    return image_frame(("red", "green", "blue"),
                       [Image.new("RGB", (13, 17), x) for x in ("red", "green", "blue")])


def test_native_index_search_reload_and_online_image_encoding(tmp_path, rm):
    index = tmp_path / "index"
    query = RetrievalQuery("blue", Image.new("RGB", (13, 17), "blue"), "blue")
    original = run_workflow(frame(), rm, [query, RetrievalQuery("bird", "bird")],
                            index_dir=index, embedding_identity={"fixture": True}, k=1)
    assert original["queries"][0]["doc_ids"] == ["blue"]
    assert original["queries"][1]["doc_ids"] == ["red"]
    assert original["queries"][0]["rm_calls"][0]["image_rows"] == 1
    assert original["queries"][1]["rm_calls"][0]["image_rows"] == 0
    assert (index / "vecs").is_file() and (index / "index").is_file()
    before = rm.observation.image_rows
    loaded = run_workflow(frame(), rm, [query], index_dir=index,
                          embedding_identity={"fixture": True}, k=1, mode="search")
    assert rm.observation.image_rows - before == 1
    assert loaded["index_s"] is None
    assert loaded["queries"][0]["doc_ids"] == ["blue"]


def test_native_preserves_rm_failure_and_restores_settings(tmp_path, rm):
    import lotus
    old = lotus.settings.rm
    def fail(docs):
        raise RuntimeError("actual executor failure")
    rm._embed = fail
    with pytest.raises(RuntimeError, match="actual executor failure"):
        run_workflow(frame(), rm, [], index_dir=tmp_path / "index",
                     embedding_identity={}, mode="index")
    assert lotus.settings.rm is old


def test_wrong_topk_preserves_actual_rank_scores_and_query_vectors(tmp_path, rm):
    rm.observation.capture_vectors = True
    report = {}
    with pytest.raises(ValueError, match="did not retrieve"):
        run_workflow(frame(), rm, [RetrievalQuery("wrong", Image.new("RGB", (13, 17), "blue"), "red")],
                     index_dir=tmp_path / "index", embedding_identity={}, k=1, report=report)
    query = report["queries"][0]
    assert query["status"] == "failed"
    assert query["observed_doc_ids"] == ["blue"]
    assert query["observed_positions"] == [2]
    assert query["observed_scores"] == [1.0]
    assert query["elapsed_s"] > 0
    assert rm.observation.vectors["call_1"].shape == (1, 512)


def test_index_refuses_reordered_gallery_and_modified_bytes(tmp_path, rm):
    index = tmp_path / "index"
    run_workflow(frame(), rm, [], index_dir=index, embedding_identity={}, mode="index")
    with pytest.raises(ValueError, match="original RangeIndex"):
        run_workflow(frame().iloc[::-1], rm, [RetrievalQuery("q", "bird")],
                     index_dir=index, embedding_identity={}, mode="search")
    (index / "index").write_bytes(b"modified")
    with pytest.raises(ValueError, match="contents changed"):
        run_workflow(frame(), rm, [RetrievalQuery("q", "bird")],
                     index_dir=index, embedding_identity={}, mode="search")


def test_stock_imagearray_paths_take_text_branch_but_pil_takes_image_branch(tmp_path):
    lotus = pytest.importorskip("lotus")
    import torch
    from lotus.models import SentenceTransformersRM
    from lotus.dtype_extensions import ImageArray
    from lotus.vector_store import FaissVS
    import pandas as pd
    validate_image_source()
    seen = []
    class Spy:
        def encode(self, docs, **kwargs):
            seen.extend(isinstance(x, Image.Image) for x in docs)
            matrix = torch.zeros((len(docs), 512), dtype=torch.float32)
            matrix[:, 0] = 1
            return matrix
    stock = SentenceTransformersRM.__new__(SentenceTransformersRM)
    stock.transformer, stock.max_batch_size, stock.normalize_embeddings = Spy(), 64, True
    paths = pd.DataFrame({"image": ImageArray(["bird-a.jpg", "bird-b.jpg"])})
    with lotus.settings.context(rm=stock, vs=FaissVS(), enable_cache=False):
        paths.sem_index("image", str(tmp_path / "paths"))
    assert seen == [False, False]
    seen.clear()
    with lotus.settings.context(rm=stock, vs=FaissVS(), enable_cache=False):
        frame().sem_index("image", str(tmp_path / "pil"))
    assert seen == [True, True, True]
