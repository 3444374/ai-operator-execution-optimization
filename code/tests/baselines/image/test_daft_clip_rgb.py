"""Opt-in real Daft RGB graphs with the real slow processor and a CPU actor fixture.

SEMLOOM_DAFT_IMAGE_SDK_TEST=1 enables SDK cases; SEMLOOM_DAFT_RGB_PROCESSOR points
to the pinned processor directory. No model weights, Torch, GPU or service are
used. The actor records every full input tensor rather than simulating decode.
"""
import gc
import hashlib
import io
import json
import os
from pathlib import Path
import threading
import uuid
from unittest.mock import Mock

import numpy as np
from PIL import Image
import pytest

from src.baselines.image.frameworks import daft_clip as native
from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingResult


PROCESSOR_SHA256 = "910e70b3956ac9879ebc90b22fb3bc8a75b6a0677814500101a4c072bd7857bd"


def record(event, **fields):
    root = Path(os.environ["SEMLOOM_DAFT_RGB_TRACE"])
    line = json.dumps(dict(event=event, pid=os.getpid(), **fields)) + "\n"
    descriptor = os.open(root / "trace.jsonl", os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, line.encode())
    finally:
        os.close(descriptor)


def vectors(pixels):
    # Only a deterministic CPU output fixture. Full real processor tensors are
    # checked separately, so no CLIP/model equivalence is inferred from this.
    matrix = pixels.reshape(len(pixels), -1)[:, ::294][:, :512].copy() + np.float32(4)
    return matrix / np.linalg.norm(matrix, axis=1, keepdims=True)


class ObservedProcessor:
    def __init__(self, path):
        from transformers import CLIPImageProcessor

        self.processor = CLIPImageProcessor.from_pretrained(path, local_files_only=True)

    def __call__(self, *, images, return_tensors, input_data_format):
        assert return_tensors == "np" and input_data_format == "channels_last"
        archive = uuid.uuid4().hex + ".npz"
        np.savez(Path(os.environ["SEMLOOM_DAFT_RGB_TRACE"]) / archive,
                 **{f"row_{index}": np.asarray(image) for index, image in enumerate(images)})
        record("decoded", archive=archive, rows=len(images))
        return self.processor(images=images, return_tensors=return_tensors,
                              input_data_format=input_data_format)


def load_observed_processor(path):
    return ObservedProcessor(path)


class TensorActorFixture:
    def __init__(self, model_path, **options):
        assert options["device"] == "cpu" and not options["detailed_stage_timing"]
        self.semantics = EmbeddingSemantics("CPU tensor fixture", "fixed slow processor", 512)
        self.uid = uuid.uuid4().hex
        record("actor_load", uid=self.uid)

    def __del__(self):
        if os.environ.get("SEMLOOM_DAFT_RGB_TRACE"):
            record("actor_destroy", uid=self.uid)

    def embed(self, batch):
        archive = uuid.uuid4().hex + ".npz"
        np.savez(Path(os.environ["SEMLOOM_DAFT_RGB_TRACE"]) / archive,
                 pixels=batch.payload, ids=np.asarray(batch.doc_ids))
        record("embed", uid=self.uid, archive=archive, ids=batch.doc_ids)
        failure = os.environ.get("SEMLOOM_DAFT_RGB_FAILURE")
        if failure == "model":
            raise ValueError("RGB fixture model failure")
        matrix = vectors(batch.payload)
        if failure == "norm":
            matrix *= 2
        record("embed_done", uid=self.uid, ids=batch.doc_ids)
        return ImageEmbeddingResult(batch.doc_ids, matrix, self.semantics, 0)

    def synchronize(self):
        record("synchronize", uid=self.uid)


def executor_shell():
    executor = native.DaftClipExecutor.__new__(native.DaftClipExecutor)
    assert executor.input_kind == "rgb_pil"
    executor.identity = dict(batch_size=4, max_batch_rows=64)
    executor.semantics = EmbeddingSemantics("fixture", "fixture", 512)
    executor._lock = threading.Lock()
    executor._closed = executor._unconfirmed_work = False
    executor._execute = Mock(side_effect=AssertionError("invalid batch was submitted"))
    return executor


@pytest.mark.parametrize("value", [None, False, "RGB", "png", ""])
def test_invalid_input_kind_is_rejected_before_sdk_or_model_access(value):
    with pytest.raises(ValueError, match="input_kind must be rgb_pil or encoded_bytes"):
        native.DaftClipExecutor(model_path="missing", input_kind=value)


@pytest.mark.parametrize("suffix", ["bytes", "path", "array", "gray", "rgba", "empty", "closed", "corrupt"])
def test_invalid_rgb_suffix_cannot_submit_a_valid_prefix(suffix):
    rng = np.random.default_rng(14)
    good = [Image.fromarray(rng.integers(0, 256, (9, 13, 3), dtype=np.uint8)) for _ in range(5)]
    if suffix == "corrupt":
        stream = io.BytesIO()
        good[0].save(stream, format="PNG")
        invalid = Image.open(io.BytesIO(stream.getvalue()[:60]))
    elif suffix == "closed":
        invalid = Image.new("RGB", (13, 9))
        invalid.close()
    else:
        invalid = {"bytes": b"encoded", "path": "image.png", "array": np.zeros((9, 13, 3), np.uint8),
                   "gray": Image.new("L", (13, 9)), "rgba": Image.new("RGBA", (13, 9)),
                   "empty": Image.new("RGB", (0, 9))}[suffix]
    executor = executor_shell()
    with pytest.raises((ValueError, OSError)):
        executor.embed_batch(tuple(str(i) for i in range(6)), [*good, invalid])
    executor._execute.assert_not_called()
    assert not executor._unconfirmed_work


def test_native_empty_batch_does_not_submit_or_load():
    executor = executor_shell()
    result = executor.embed_batch((), [])
    assert result.doc_ids == () and result.embeddings.shape == (0, 512) and result.service_s == 0
    executor._execute.assert_not_called()


def test_oversized_rgb_batch_rejected_before_any_lazy_image_load_or_submission(monkeypatch):
    stream = io.BytesIO()
    Image.new("RGB", (13, 9), (1, 2, 3)).save(stream, format="PNG")
    images = [Image.open(io.BytesIO(stream.getvalue())) for _ in range(3)]
    loads = []
    for image in images:
        load = Mock(side_effect=OSError("oversized lazy image must not be loaded"))
        monkeypatch.setattr(image, "load", load)
        loads.append(load)
    executor = executor_shell()
    executor.identity["max_batch_rows"] = 2
    with pytest.raises(ValueError, match="caller batch exceeds max_batch_rows"):
        executor.embed_batch(("a", "b", "c"), images)
    assert all(load.call_count == 0 for load in loads)
    executor._execute.assert_not_called()
    assert not executor._unconfirmed_work


@pytest.fixture
def sdk(tmp_path, monkeypatch):
    if os.environ.get("SEMLOOM_DAFT_IMAGE_SDK_TEST") != "1":
        pytest.skip("set SEMLOOM_DAFT_IMAGE_SDK_TEST=1 for fixed real Daft CPU SDK tests")
    daft = pytest.importorskip("daft")
    assert daft.__version__ == "0.7.21"
    processor = Path(os.environ["SEMLOOM_DAFT_RGB_PROCESSOR"]) / "preprocessor_config.json"
    payload = processor.read_bytes()
    assert hashlib.sha256(payload).hexdigest() == PROCESSOR_SHA256
    model = tmp_path / "CPU-fixture-no-model-weights"
    model.mkdir()
    (model / "preprocessor_config.json").write_bytes(payload)
    # Constructor provenance files only; TensorActorFixture never reads these.
    (model / "pytorch_model.bin").write_bytes(b"not a model: CPU tensor fixture only")
    (model / "config.json").write_text('{"fixture":true}\n')
    monkeypatch.setattr(native, "_load_processor", load_observed_processor)
    monkeypatch.setattr(native, "ClipTensorActor", TensorActorFixture)
    executors = []

    def make(name, *, input_kind=None):
        trace = tmp_path / name
        trace.mkdir()
        monkeypatch.setenv("SEMLOOM_DAFT_RGB_TRACE", str(trace))
        options = {} if input_kind is None else {"input_kind": input_kind}
        executor = native.DaftClipExecutor(model_path=str(model), batch_size=4,
                                           device="cpu", gpus_per_worker=0, **options)
        executors.append((executor, trace))
        return executor, trace

    yield make, model, daft
    for executor, trace in executors:
        monkeypatch.setenv("SEMLOOM_DAFT_RGB_TRACE", str(trace))
        if not executor._unconfirmed_work:
            executor.close()


def events(root):
    return [json.loads(line) for line in (root / "trace.jsonl").read_text().splitlines()]


def tensor_rows(root):
    result = {}
    for event in events(root):
        if event["event"] == "embed":
            with np.load(root / event["archive"]) as archive:
                for identifier, tensor in zip(archive["ids"], archive["pixels"], strict=True):
                    assert identifier not in result
                    result[str(identifier)] = tensor.copy()
    return result


def decoded_rows(root):
    rows = []
    for event in events(root):
        if event["event"] == "decoded":
            with np.load(root / event["archive"]) as archive:
                rows.extend(archive[f"row_{index}"].copy() for index in range(event["rows"]))
    return rows


def fixtures():
    rng = np.random.default_rng(15)
    shapes = [(1, 1), (1, 3), (3, 1), (3, 3), (3, 9), (9, 3), (13, 9), (9, 13), (225, 317)]
    arrays = [rng.integers(0, 256, (height, width, 3), dtype=np.uint8) for height, width in shapes]
    images = [Image.fromarray(array) for array in arrays]
    pngs = []
    for image in images:
        stream = io.BytesIO()
        image.save(stream, format="PNG", compress_level=1)
        pngs.append(stream.getvalue())
    return arrays, images, pngs


def test_default_rgb_matches_explicit_encoded_pixels_full_slow_tensors_tail_ids_and_owned_output(sdk):
    make, model, _ = sdk
    arrays, images, pngs = fixtures()
    ids = ("suffix/8", "unicode-图", "0", "3", "x/y", "6", "5", "partial/7", "last")
    actual = {}
    traces = {}
    for name, payload, input_kind in (("png", pngs, "encoded_bytes"), ("rgb", images, None)):
        executor, trace = make(name, input_kind=input_kind)
        assert executor.input_kind == ("rgb_pil" if input_kind is None else input_kind)
        assert executor.identity["input_kind"] == executor.input_kind
        if input_kind is None:
            assert executor.identity["decode"] == "none (caller-decoded RGB)"
            assert executor.identity["input_adapter"] == "PIL RGB -> HWC uint8 -> Daft Image[RGB]"
        else:
            assert executor.identity["decode"] == "Daft RGB"
            assert "input_adapter" not in executor.identity
        result = executor.embed_batch(ids, payload)
        actual[name] = result.embeddings
        traces[name] = trace
        assert result.doc_ids == ids and result.embeddings.shape == (9, 512)
        assert result.embeddings.dtype == np.float32
        assert result.embeddings.flags.owndata and result.embeddings.flags.writeable
        assert not executor._unconfirmed_work
        executor.close()
        logged = events(trace)
        calls = [event for event in logged if event["event"] == "embed"]
        assert sorted(len(event["ids"]) for event in calls) == [1, 4, 4]
        assert sum(event["event"] == "embed_done" for event in logged) == 3
        assert sum(event["event"] == "actor_load" for event in logged) == 1
        assert sum(event["event"] == "actor_destroy" for event in logged) == 1
        for before, decoded in zip(arrays, decoded_rows(trace), strict=True):
            assert decoded.dtype == np.uint8
            np.testing.assert_array_equal(decoded, before)
    from transformers import CLIPImageProcessor

    processor = CLIPImageProcessor.from_pretrained(model, local_files_only=True)
    expected = processor(images=arrays, return_tensors="np", input_data_format="channels_last")["pixel_values"]
    png_rows, rgb_rows = tensor_rows(traces["png"]), tensor_rows(traces["rgb"])
    assert set(png_rows) == set(rgb_rows) == set(ids)
    for index, identifier in enumerate(ids):
        np.testing.assert_array_equal(png_rows[identifier], expected[index])
        np.testing.assert_array_equal(rgb_rows[identifier], expected[index])
    np.testing.assert_array_equal(actual["png"], vectors(expected))
    np.testing.assert_array_equal(actual["rgb"], actual["png"])
    for image in images:
        image.close()
    arrays.clear()
    gc.collect()
    np.testing.assert_array_equal(actual["rgb"], vectors(expected))
    actual["rgb"][0, 0] = 0
    assert actual["png"][0, 0] != 0


def test_real_rgb_reuses_its_native_stateful_wrapper_across_complete_graphs(sdk):
    make, _, _ = sdk
    executor, trace = make("reuse")
    _, images, _ = fixtures()
    for index in range(2):
        result = executor.embed_batch(tuple(f"{index}/{i}" for i in range(5)), images[:5])
        assert result.embeddings.shape == (5, 512) and not executor._unconfirmed_work
    logged = events(trace)
    loads = [event for event in logged if event["event"] == "actor_load"]
    calls = [event for event in logged if event["event"] == "embed_done"]
    assert len(loads) == 1 and len(calls) == 4
    assert all((event["pid"], event["uid"]) == (loads[0]["pid"], loads[0]["uid"]) for event in calls)
    executor.close()
    assert sum(event["event"] == "actor_destroy" for event in events(trace)) == 1
    with pytest.raises(RuntimeError, match="closed"):
        executor.embed_batch(("after-close",), images[:1])


def test_real_rgb_error_keeps_primary_and_poisoned_close_semantics(sdk, monkeypatch):
    make, _, _ = sdk
    executor, trace = make("failure")
    _, images, _ = fixtures()
    monkeypatch.setenv("SEMLOOM_DAFT_RGB_FAILURE", "model")
    with pytest.raises(Exception, match="RGB fixture model failure"):
        executor.embed_batch(("a",), images[:1])
    assert executor._unconfirmed_work
    assert sum(event["event"] == "synchronize" for event in events(trace)) == 1
    with pytest.raises(RuntimeError, match="unconfirmed"):
        executor.embed_batch(("retry",), images[:1])
    with pytest.raises(RuntimeError, match="unconfirmed"):
        executor.close()


def test_real_rgb_invalid_output_is_rejected_only_after_complete_graph_consumption(sdk, monkeypatch):
    make, _, _ = sdk
    executor, trace = make("invalid-norm")
    _, images, _ = fixtures()
    monkeypatch.setenv("SEMLOOM_DAFT_RGB_FAILURE", "norm")
    with pytest.raises(ValueError, match="unit normalized"):
        executor.embed_batch(tuple(str(i) for i in range(9)), images)
    assert sum(event["event"] == "embed_done" for event in events(trace)) == 3
    assert not executor._unconfirmed_work


def test_real_rgb_iterator_close_error_is_preserved_and_exit_stays_unconfirmed(sdk, monkeypatch):
    make, _, daft = sdk
    executor, trace = make("close-error")
    _, images, _ = fixtures()
    original = daft.DataFrame.to_arrow_iter
    consumed = []

    class Iterator:
        def __init__(self, iterator):
            self.iterator = iterator

        def __iter__(self):
            return self

        def __next__(self):
            value = next(self.iterator)
            consumed.append(len(value))
            return value

        def close(self):
            self.iterator.close()
            raise OSError("RGB fixture iterator close failure")

    monkeypatch.setattr(daft.DataFrame, "to_arrow_iter", lambda frame: Iterator(original(frame)))
    with pytest.raises(OSError, match="RGB fixture iterator close failure"):
        executor.embed_batch(tuple(str(i) for i in range(9)), images)
    assert sum(consumed) == 9
    assert sum(event["event"] == "embed_done" for event in events(trace)) == 3
    assert executor._unconfirmed_work
    with pytest.raises(RuntimeError, match="unconfirmed"):
        executor.close()
