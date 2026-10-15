"""Real Arrow24 vector conversion; Daft graph fixtures load no model or GPU."""
import gc
import sys
import threading
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock
import weakref

import numpy as np
import pytest

pa = pytest.importorskip("pyarrow", reason="real PyArrow required for vector buffer checks")

from src.baselines.image.frameworks import daft_clip
from src.modalities.image.contracts import EmbeddingSemantics


def fixed(matrix):
    return pa.FixedSizeListArray.from_arrays(pa.array(matrix.reshape(-1)), matrix.shape[1])


def oracle(column):
    return np.asarray(column.to_pylist(), dtype=np.float32).reshape(len(column), 512)


def test_single_array_matches_python_oracle_and_views_the_actual_float32_buffer():
    source = np.arange(3 * 512, dtype=np.float32).reshape(3, 512)
    column = fixed(source)
    actual = daft_clip._arrow_embedding_matrix(column)
    np.testing.assert_array_equal(actual, oracle(column))
    assert actual.shape == (3, 512) and actual.dtype == np.float32
    assert np.shares_memory(actual, source)
    assert actual.flags.c_contiguous and not actual.flags.writeable


@pytest.mark.parametrize("child_offset", [0, 512])
def test_parent_slice_applies_row_offset_and_preserves_independent_child_offset(child_offset):
    source = np.arange(9 * 512, dtype=np.float32)
    child = pa.array(source).slice(child_offset, 8 * 512)
    column = pa.FixedSizeListArray.from_arrays(child, 512).slice(2, 3)
    actual = daft_clip._arrow_embedding_matrix(column)
    np.testing.assert_array_equal(actual, oracle(column))
    np.testing.assert_array_equal(actual, source[child_offset + 2 * 512:child_offset + 5 * 512].reshape(3, 512))
    assert np.shares_memory(actual, source)


def test_multiple_chunks_and_empty_chunk_preserve_exact_native_row_order():
    source = np.arange(7 * 512, dtype=np.float32).reshape(7, 512)
    base = fixed(source)
    column = pa.chunked_array([base.slice(3, 2), base.slice(0, 0), base.slice(1, 1)])
    actual = daft_clip._arrow_embedding_matrix(column)
    np.testing.assert_array_equal(actual, oracle(column))
    np.testing.assert_array_equal(actual, source[[3, 4, 1]])
    assert actual.flags.owndata


@pytest.mark.parametrize("chunked", [False, True])
def test_empty_column_has_complete_zero_by_512_float32_shape(chunked):
    typ = pa.list_(pa.float32(), 512)
    column = pa.chunked_array([], type=typ) if chunked else pa.array([], type=typ)
    actual = daft_clip._arrow_embedding_matrix(column)
    assert actual.shape == (0, 512) and actual.dtype == np.float32


@pytest.mark.parametrize("table", [False, True])
def test_real_table_chunked_column_and_recordbatch_array_are_supported(table):
    source = np.arange(2 * 512, dtype=np.float32).reshape(2, 512)
    constructor = pa.table if table else pa.record_batch
    batch = constructor({"embedding": fixed(source)})
    column = batch["embedding"]
    assert isinstance(column, pa.ChunkedArray if table else pa.FixedSizeListArray)
    np.testing.assert_array_equal(daft_clip._arrow_embedding_matrix(column), source)


@pytest.mark.parametrize("where", ["parent", "child"])
def test_null_rows_or_values_are_rejected_instead_of_dropped(where):
    values = np.arange(3 * 512, dtype=np.float32)
    if where == "parent":
        column = pa.FixedSizeListArray.from_arrays(pa.array(values), 512, mask=pa.array([False, True, False]))
    else:
        mask = np.zeros(values.size, dtype=bool)
        mask[512 + 17] = True
        column = pa.FixedSizeListArray.from_arrays(pa.array(values, mask=mask), 512)
    with pytest.raises(ValueError, match="null"):
        daft_clip._arrow_embedding_matrix(column.slice(1, 1))


def test_null_outside_logical_slice_does_not_poison_valid_rows():
    source = np.arange(3 * 512, dtype=np.float32)
    mask = np.zeros(source.size, dtype=bool)
    mask[0] = True
    column = pa.FixedSizeListArray.from_arrays(pa.array(source, mask=mask), 512,
                                               mask=pa.array([True, False, False])).slice(1, 2)
    np.testing.assert_array_equal(daft_clip._arrow_embedding_matrix(column), oracle(column))


@pytest.mark.parametrize("column", [fixed(np.ones((1, 511), np.float32)),
                                     fixed(np.ones((1, 512), np.float64)),
                                     pa.array([[1.0] * 512], type=pa.list_(pa.float32()))])
def test_width_or_child_type_changes_are_rejected_without_numeric_cast(column):
    with pytest.raises(ValueError, match="float32 lists of width 512"):
        daft_clip._arrow_embedding_matrix(column)


def test_numpy_view_keeps_the_buffer_valid_after_arrow_parent_is_collected():
    def create():
        source = np.arange(2 * 512, dtype=np.float32).reshape(2, 512)
        parent = fixed(source)
        return weakref.ref(parent), daft_clip._arrow_embedding_matrix(parent)
    reference, actual = create()
    gc.collect()
    assert reference() is None
    np.testing.assert_array_equal(actual, np.arange(2 * 512, dtype=np.float32).reshape(2, 512))


def executor_fixture(monkeypatch, batches, *, close_error=None):
    class Iterator:
        def __init__(self):
            self.batches = iter(batches)
            self.returned = 0
            self.closed = 0
        def __iter__(self): return self
        def __next__(self):
            batch = next(self.batches)
            self.returned += 1
            return batch
        def close(self):
            self.closed += 1
            self.batches = None
            if close_error is not None:
                raise close_error
    iterator = Iterator()
    class Frame:
        def __getitem__(self, key): return object()
        def into_batches(self, count): return self
        def with_column(self, *args, **kwargs): return self
        def select(self, *args): return self
        def to_arrow_iter(self): return iterator
    daft, functions = ModuleType("daft"), ModuleType("daft.functions")
    daft.__path__, daft.functions = [], functions
    daft.from_pydict = lambda values: Frame()
    daft.ImageMode = SimpleNamespace(RGB="RGB")
    functions.decode_image = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "daft", daft)
    monkeypatch.setitem(sys.modules, "daft.functions", functions)
    executor = daft_clip.DaftClipExecutor.__new__(daft_clip.DaftClipExecutor)
    executor.input_kind = "encoded_bytes"
    executor._embedder = SimpleNamespace(embed=Mock())
    executor._lock = threading.Lock()
    executor._closed = executor._unconfirmed_work = False
    executor.identity = {"batch_size": 2, "max_batch_rows": 4096}
    executor.semantics = EmbeddingSemantics("fixture", "fixture", 512)
    return executor, iterator


def test_final_result_consumes_all_batches_orders_ids_and_owns_its_matrix(monkeypatch):
    source = np.zeros((3, 512), dtype=np.float32)
    source[np.arange(3), np.arange(3)] = 1
    batches = [pa.table({"doc_id": ["b", "a"], "embedding": fixed(source[[1, 0]])}),
               pa.table({"doc_id": ["c"], "embedding": fixed(source[2:])})]
    executor, iterator = executor_fixture(monkeypatch, batches)
    result = executor.embed_batch(("a", "b", "c"), [b"fixture"] * 3)
    assert iterator.returned == 2 and iterator.closed == 1
    assert result.doc_ids == ("a", "b", "c")
    np.testing.assert_array_equal(result.embeddings, source)
    assert result.embeddings.flags.owndata and result.embeddings.flags.writeable
    batches.clear()
    gc.collect()
    np.testing.assert_array_equal(result.embeddings, source)
    assert not executor._unconfirmed_work


def test_converter_failure_preserves_primary_through_close_and_poisons_unknown_work(monkeypatch):
    batch = pa.table({"doc_id": ["a"], "embedding": fixed(np.ones((1, 511), np.float32))})
    executor, iterator = executor_fixture(monkeypatch, [batch], close_error=OSError("fixture close failed"))
    with pytest.raises(ValueError, match="width 512") as caught:
        executor.embed_batch(("a",), [b"fixture"])
    assert iterator.closed == 1 and executor._unconfirmed_work
    assert any("iterator close also failed: OSError" in note for note in caught.value.__notes__)
    with pytest.raises(RuntimeError, match="unconfirmed"):
        executor.embed_batch(("b",), [b"fixture"])


@pytest.mark.parametrize("invalid", ["nonfinite", "norm"])
def test_final_finite_and_norm_checks_still_happen_after_graph_eof(monkeypatch, invalid):
    values = np.zeros((2, 512), np.float32)
    values[:, 0] = 1
    values[1, 1 if invalid == "nonfinite" else 0] = np.nan if invalid == "nonfinite" else 2
    batches = [pa.record_batch({"doc_id": ["a"], "embedding": fixed(values[:1])}),
               pa.record_batch({"doc_id": ["b"], "embedding": fixed(values[1:])})]
    executor, iterator = executor_fixture(monkeypatch, batches)
    with pytest.raises(ValueError, match="complete finite" if invalid == "nonfinite" else "unit normalized"):
        executor.embed_batch(("a", "b"), [b"fixture"] * 2)
    assert iterator.returned == 2 and iterator.closed == 1
    assert not executor._unconfirmed_work
