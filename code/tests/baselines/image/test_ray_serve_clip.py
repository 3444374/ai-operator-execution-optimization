"""Resident CLIP contracts with fixtures; no Ray runtime or model is started.

Native Ray/Serve behavior and real CLIP equivalence need separate qualification.
These tests exercise the production client, replica and owner failure boundaries.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import threading
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import numpy as np
from PIL import Image
import pytest

from src.baselines.image.frameworks import ray_serve_clip as native
from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingResult
from src.serving.ray_data_clip_client import clip_service_batch


def vectors(count):
    return np.eye(512, dtype=np.float32)[:count].copy()


def rgb(count):
    return [np.full((7, 9, 3), index, dtype=np.uint8) for index in range(count)]


class Response:
    def __init__(self, value=None, error=None):
        self.value, self.error, self.timeouts = value, error, []

    def result(self, *, timeout_s):
        self.timeouts.append(timeout_s)
        if self.error is not None:
            raise self.error
        return self.value


class BatchIterator:
    def __init__(self, batches=(), *, error=None, close_error=None):
        self.batches = iter(batches)
        self.error, self.close_error = error, close_error
        self.close_calls = 0

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.batches)
        except StopIteration:
            if self.error is not None:
                raise self.error
            raise

    def close(self):
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


@pytest.fixture
def sdk(monkeypatch):
    """Record public calls and return chosen batches, without simulating Ray."""
    state = SimpleNamespace(initialized=False, events=[], retried_map_errors=False,
                            iterator=BatchIterator(), items=[], maps=[], iterations=[],
                            ready_response=Response(), failures={}, graph_executor=None)
    state.synchronize_remote = Mock(return_value=Response(dict(service_uid="service-fixture")))

    def event(event_name, *args, **kwargs):
        state.events.append((event_name, args, kwargs))
        error = state.failures.get(event_name)
        if error is not None:
            raise error

    ray, data, serve = (ModuleType(name) for name in ("ray", "ray.data", "ray.serve"))
    ray.__path__ = []
    ray.data, ray.serve = data, serve
    ray.is_initialized = lambda: state.initialized

    def initialize(**kwargs):
        state.initialized = True  # Include a partially initialized startup failure.
        event("init", **kwargs)

    def shutdown():
        event("shutdown")
        state.initialized = False

    ray.init, ray.shutdown = initialize, shutdown
    serve.start = lambda **kwargs: event("start", **kwargs)
    serve.delete = lambda name: event("delete", name)

    def deployment(**kwargs):
        event("deployment", **kwargs)

        def decorate(cls):
            def bind(*args):
                event("bind", cls, *args)
                return SimpleNamespace(deployment=cls)
            return SimpleNamespace(bind=bind)
        return decorate

    serve.deployment = deployment

    def run(application, **kwargs):
        event("run", application, **kwargs)
        return SimpleNamespace(ready=SimpleNamespace(remote=lambda: state.ready_response),
            synchronize=SimpleNamespace(remote=state.synchronize_remote))

    serve.run = run
    data.DataContext = SimpleNamespace(get_current=lambda: state)
    data.TaskPoolStrategy = lambda **kwargs: ("task_pool", kwargs)

    class Dataset:
        @property
        def _current_executor(self):
            return state.graph_executor

        def map_batches(self, function, **kwargs):
            state.maps.append((function, kwargs))
            return self

        def iter_batches(self, **kwargs):
            state.iterations.append(kwargs)
            return state.iterator

    def from_items(items):
        state.items.append(items)
        return Dataset()

    data.from_items = from_items
    for module in (ray, data, serve):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    state.ray, state.serve = ray, serve
    return state


def owner(sdk):
    executor = object.__new__(native.RayServeClipExecutor)
    executor.semantics = EmbeddingSemantics("fixture-model", "fixture-processor", 512)
    executor.batch_size, executor.max_batch_rows = 32, 5000
    executor.request_timeout_s = 17
    executor._lock = threading.Lock()
    executor._closed = executor._unconfirmed_work = False
    executor._owns_runtime = True
    executor._close_error = None
    executor._handle, executor._service_uid = object(), "service-fixture"
    executor._app_name = "fixture-owned-application"
    executor._ray, executor._serve = sdk.ray, sdk.serve
    executor.identity, executor.reuse_observations = {}, []
    sdk.initialized = True
    return executor


@pytest.fixture
def startup(monkeypatch, tmp_path, sdk):
    model, processor = tmp_path / "model", tmp_path / "processor"
    model.mkdir()
    processor.mkdir()
    artifacts = dict(weights="a" * 64, model_config="b" * 64, processor_config="c" * 64)
    software = dict(ray="2.56.1", torch="fixture", transformers="fixture", numpy="fixture",
                    pyarrow="fixture", Pillow="fixture")
    monkeypatch.setattr(native, "_artifacts", lambda *_: artifacts)
    monkeypatch.setattr(native, "_software", lambda: software)
    sdk.ready_response = Response(dict(service_uid="service-fixture", artifacts_sha256=artifacts,
                                       software=software))
    return dict(model_path=model, processor_path=processor, device="cpu",
                ray_temp_dir=tmp_path / "ray")


def test_fresh_client_import_does_not_load_models_or_ray():
    script = """
import importlib.abc
import sys
class RejectModels(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'torch', 'transformers', 'ray'}:
            raise AssertionError('unexpected import: ' + fullname)
sys.meta_path.insert(0, RejectModels())
import src.serving.ray_data_clip_client
assert not {'torch', 'transformers', 'ray'} & set(sys.modules)
"""
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[3]),
               PYTHONDONTWRITEBYTECODE="1")
    subprocess.run([sys.executable, "-c", script], env=env, check=True,
                   capture_output=True, text=True, timeout=30)


@pytest.mark.parametrize("ids,images,exception", [
    ((), [], ValueError), (("a", "a"), rgb(2), ValueError),
    (("",), rgb(1), ValueError), ((1,), rgb(1), ValueError),
    (("a",), [], ValueError), (("a",), [np.zeros((7, 9, 3), np.float32)], TypeError),
    (("a",), [np.zeros((7, 9), np.uint8)], TypeError),
    (("a",), [np.zeros((7, 9, 4), np.uint8)], TypeError),
    (("a",), [np.zeros((0, 9, 3), np.uint8)], TypeError),
])
def test_client_rejects_invalid_complete_batch_before_submission(ids, images, exception):
    handle = SimpleNamespace(embed=SimpleNamespace(remote=Mock()))
    with pytest.raises(exception):
        clip_service_batch(dict(doc_id=ids, image=images), handle=handle,
                           service_uid="service", call_id="call", timeout_s=17)
    handle.embed.remote.assert_not_called()


def test_client_one_unary_request_per_batch_and_fresh_association():
    images, requests, responses = rgb(2), [], []

    def remote(request):
        requests.append(request)
        reply = dict(doc_id=("b", "a"), embedding=vectors(2)[::-1],
                     service_uid=request["service_uid"], request_id=request["request_id"],
                     model_reuse=dict(model_uid="model", pid=19, image_returns=3,
                                      image_returned_rows=6))
        responses.append(Response(reply))
        return responses[-1]

    handle = SimpleNamespace(embed=SimpleNamespace(remote=remote))
    for _ in range(2):
        result = clip_service_batch(dict(doc_id=("a", "b"), image=images), handle=handle,
                                   service_uid="service", call_id="call", timeout_s=17)
        assert tuple(result["doc_id"]) == ("b", "a")
        np.testing.assert_array_equal(result["embedding"], vectors(2)[::-1])
        np.testing.assert_array_equal(result["_semloom_model_uid"], ["model", "model"])
        np.testing.assert_array_equal(result["_semloom_model_embed_calls"], [3, 3])
    assert len(requests) == 2 and requests[0]["request_id"] != requests[1]["request_id"]
    assert all(x["request_id"].startswith("call:") for x in requests)
    assert all(response.timeouts == [17] for response in responses)
    assert requests[0]["doc_ids"] == ("a", "b")
    assert requests[0]["images"][0] is images[0]


@pytest.mark.parametrize("change", ["service", "request", "duplicate", "missing", "dtype", "shape"])
def test_client_rejects_unassociated_or_incomplete_reply(change):
    def remote(request):
        reply = dict(doc_id=("a", "b"), embedding=vectors(2),
                     service_uid="service", request_id=request["request_id"])
        if change == "service":
            reply["service_uid"] = "restarted"
        elif change == "request":
            reply["request_id"] = "other-call"
        elif change == "duplicate":
            reply["doc_id"] = ("a", "a")
        elif change == "missing":
            reply["doc_id"] = ("a",)
        elif change == "dtype":
            reply["embedding"] = vectors(2).astype(np.float64)
        else:
            reply["embedding"] = np.ones((2, 511), np.float32)
        return Response(reply)

    with pytest.raises(ValueError):
        clip_service_batch(dict(doc_id=("a", "b"), image=rgb(2)),
            handle=SimpleNamespace(embed=SimpleNamespace(remote=remote)),
            service_uid="service", call_id="call", timeout_s=17)


def test_client_preserves_timeout_as_primary():
    primary = TimeoutError("fixture service timeout")
    response = Response(error=primary)
    with pytest.raises(TimeoutError) as caught:
        clip_service_batch(dict(doc_id=("a",), image=rgb(1)),
            handle=SimpleNamespace(embed=SimpleNamespace(remote=lambda _: response)),
            service_uid="service", call_id="call", timeout_s=17)
    assert caught.value is primary and response.timeouts == [17]


@pytest.fixture
def replica(monkeypatch):
    from src.modalities.image import clip
    processor = SimpleNamespace(preprocess_decoded=Mock(return_value=np.zeros((2, 3, 224, 224), np.float32)))
    actor = SimpleNamespace(semantics=EmbeddingSemantics("model", "processor", 512),
        embed=Mock(), synchronize=Mock(), ready=Mock(return_value=dict(model_reuse=dict(
            model_uid="model-fixture", pid=19, image_returns=1, image_returned_rows=2))))
    actor.embed.return_value = ImageEmbeddingResult(("a", "b"), vectors(2), actor.semantics, 0)
    load_processor, load_actor = Mock(return_value=processor), Mock(return_value=actor)
    monkeypatch.setattr(clip, "ClipImagePreprocessor", load_processor)
    monkeypatch.setattr(clip, "ClipTensorActor", load_actor)
    monkeypatch.setattr(native, "_artifacts", lambda *_: {"fixture": "hash"})
    monkeypatch.setattr(native, "_software", lambda: {"ray": "2.56.1"})
    instance = native._ClipReplica("model", "processor", "cpu", 32)
    return instance, processor, actor, load_processor, load_actor


def request(replica, ids=("a", "b")):
    return dict(service_uid=replica.uid, request_id="request-fixture", doc_ids=ids,
                images=rgb(len(ids)))


def test_replica_reuses_existing_slow_processor_and_tensor_kernel(replica):
    instance, processor, actor, load_processor, load_actor = replica
    load_processor.assert_called_once_with("processor", local_files_only=True)
    load_actor.assert_called_once_with("model", processor_revision="processor", device="cpu",
        dtype="float32", normalize=True, local_files_only=True, detailed_stage_timing=False,
        torch_intraop_threads=1, torch_interop_threads=1)
    inputs = request(instance)
    reply = instance.embed(inputs)
    decoded = processor.preprocess_decoded.call_args.args[0]
    assert decoded[0] is inputs["images"][0]
    batch = actor.embed.call_args.args[0]
    assert batch.payload is processor.preprocess_decoded.return_value
    assert batch.doc_ids == ("a", "b") and batch.input_kind == "preprocessed_tensor"
    assert (batch.work_units, batch.work_unit) == (2 * 224 * 224, "pixels")
    assert reply["service_uid"] == instance.uid and reply["request_id"] == "request-fixture"
    assert reply["embedding"] is actor.embed.return_value.embeddings
    assert reply["model_reuse"]["model_uid"] == "model-fixture"
    ready = instance.synchronize()
    assert (ready["forward_attempts"], ready["forward_returns"], ready["returned_rows"]) == (1, 1, 2)
    actor.synchronize.assert_called_once_with()


def test_replica_rejects_restarted_service_or_oversized_request_before_model(replica):
    instance, processor, actor, *_ = replica
    bad = request(instance)
    bad["service_uid"] = "previous-instance"
    with pytest.raises(RuntimeError, match="instance changed"):
        instance.embed(bad)
    with pytest.raises(ValueError, match="native model batch size"):
        instance.embed(request(instance, tuple(map(str, range(33)))))
    processor.preprocess_decoded.assert_not_called()
    actor.embed.assert_not_called()


@pytest.mark.parametrize("stage", ["prepare", "forward"])
def test_replica_failure_poisoning_preserves_primary_and_synchronize_note(replica, stage):
    instance, processor, actor, *_ = replica
    primary = ValueError("fixture " + stage)
    (processor.preprocess_decoded if stage == "prepare" else actor.embed).side_effect = primary
    actor.synchronize.side_effect = OSError("fixture synchronize")
    with pytest.raises(ValueError) as caught:
        instance.embed(request(instance))
    assert caught.value is primary and instance._failed
    assert any("synchronize also failed: OSError" in note for note in primary.__notes__)
    assert instance.forward_attempts == int(stage == "forward")
    assert instance.forward_returns == instance.rows == 0
    prior = processor.preprocess_decoded.call_count
    with pytest.raises(RuntimeError, match="service failed"):
        instance.embed(request(instance))
    assert processor.preprocess_decoded.call_count == prior


def test_owner_collects_full_matrix_in_input_order_with_native_data_options(sdk):
    executor = owner(sdk)
    matrix = vectors(3)
    sdk.iterator = BatchIterator([
        dict(doc_id=np.asarray(["c", "a"]), embedding=matrix[[2, 0]]),
        dict(doc_id=np.asarray(["b"]), embedding=matrix[[1]],
             _semloom_model_uid=np.asarray(["model-fixture"]), _semloom_model_pid=np.asarray([19]),
             _semloom_model_embed_calls=np.asarray([2]), _semloom_model_rows=np.asarray([3]))])
    images = [Image.fromarray(x) for x in rgb(3)]
    result = executor.embed_batch(("a", "b", "c"), images)
    assert result.doc_ids == ("a", "b", "c") and result.service_s >= 0
    np.testing.assert_array_equal(result.embeddings, matrix)
    assert result.embeddings.dtype == np.float32 and not executor._unconfirmed_work
    assert sdk.iterator.close_calls == 1
    assert sdk.iterations == [dict(batch_format="numpy", prefetch_batches=1)]
    function, kwargs = sdk.maps[0]
    assert function is clip_service_batch
    assert {k: v for k, v in kwargs.items() if k != "fn_kwargs"} == dict(
        batch_format="numpy", batch_size=32, compute=("task_pool", dict(size=1)),
        num_cpus=1, num_gpus=0, max_retries=0)
    assert set(kwargs["fn_kwargs"]) == {"handle", "service_uid", "call_id", "timeout_s"}
    assert kwargs["fn_kwargs"]["handle"] is executor._handle
    assert executor.reuse_observations == [dict(model_uid="model-fixture", pid=19,
        embed_calls=2, rows=3, batch_rows=1)]
    np.testing.assert_array_equal(sdk.items[0][2]["image"], np.asarray(images[2]))
    sdk.iterator = BatchIterator([dict(doc_id=np.asarray(["a"]), embedding=vectors(1))])
    executor.embed_batch(("a",), images[:1])
    assert len(sdk.items) == 2
    assert sdk.maps[0][1]["fn_kwargs"]["call_id"] != sdk.maps[1][1]["fn_kwargs"]["call_id"]


@pytest.mark.parametrize("defect", ["dtype", "nan", "norm", "shape", "missing", "duplicate", "unexpected"])
def test_owner_rejects_invalid_full_output_and_forbids_reuse(sdk, defect):
    executor = owner(sdk)
    ids, matrix = ("a", "b"), vectors(2)
    if defect == "dtype":
        matrix = matrix.astype(np.float64)
    elif defect == "nan":
        matrix[0, 0] = np.nan
    elif defect == "norm":
        matrix *= 2
    elif defect == "shape":
        matrix = matrix[:, :511]
    elif defect == "missing":
        ids, matrix = ("a",), matrix[:1]
    elif defect == "duplicate":
        ids = ("a", "a")
    else:
        ids = ("a", "unknown")
    sdk.iterator = BatchIterator([dict(doc_id=np.asarray(ids), embedding=matrix)])
    images = [Image.fromarray(x) for x in rgb(2)]
    with pytest.raises(ValueError):
        executor.embed_batch(("a", "b"), images)
    assert executor._unconfirmed_work and sdk.iterator.close_calls == 1
    with pytest.raises(RuntimeError, match="completion unknown"):
        executor.embed_batch(("a", "b"), images)
    assert len(sdk.items) == 1


@pytest.mark.parametrize("primary_error", [True, False])
def test_owner_iterator_cleanup_keeps_first_error_and_poisoned_state(sdk, primary_error):
    executor = owner(sdk)
    primary, cleanup = ValueError("fixture iteration"), OSError("fixture iterator close")
    sdk.iterator = BatchIterator([dict(doc_id=np.asarray(["a"]), embedding=vectors(1))],
                                error=primary if primary_error else None, close_error=cleanup)
    with pytest.raises(ValueError if primary_error else OSError) as caught:
        executor.embed_batch(("a",), [Image.fromarray(rgb(1)[0])])
    assert caught.value is (primary if primary_error else cleanup)
    assert executor._unconfirmed_work and sdk.iterator.close_calls == 1
    if primary_error:
        assert any("iterator close also failed: OSError" in note for note in primary.__notes__)


@pytest.mark.parametrize("cleanup_failure", [None, "close", "shutdown", "both"])
def test_constructor_failure_cleans_unclosed_graph_and_preserves_primary(sdk, cleanup_failure):
    executor = owner(sdk)
    primary = MemoryError("fixture batch iterator construction")
    sdk.iterator = BatchIterator(error=primary, close_error=(
        OSError("fixture close") if cleanup_failure in ("close", "both") else None))
    graph = SimpleNamespace(_shutdown=False)
    def shutdown(*, force):
        assert force is True
        if cleanup_failure in ("shutdown", "both"):
            raise RuntimeError("fixture graph shutdown")
        graph._shutdown = True
    graph.shutdown = Mock(side_effect=shutdown)
    sdk.graph_executor = graph
    with pytest.raises(MemoryError) as caught:
        executor.embed_batch(("a",), [Image.fromarray(rgb(1)[0])])
    assert caught.value is primary and executor._unconfirmed_work
    assert sdk.iterator.close_calls == 1 and sdk.events == []
    graph.shutdown.assert_called_once_with(force=True)
    if cleanup_failure in ("close", "both"):
        assert any("iterator close also failed: OSError" in note for note in primary.__notes__)
    if cleanup_failure in ("shutdown", "both"):
        assert any("graph shutdown also failed: RuntimeError" in note for note in primary.__notes__)
    with pytest.raises(RuntimeError, match="completion unknown"):
        executor.embed_batch(("retry",), [Image.fromarray(rgb(1)[0])])
    assert len(sdk.items) == 1


@pytest.mark.parametrize("error", [None, ValueError("fixture started iteration")])
def test_owner_does_not_repeat_native_graph_shutdown(sdk, error):
    executor = owner(sdk)
    sdk.graph_executor = SimpleNamespace(_shutdown=False, shutdown=Mock())
    class NativeIterator(BatchIterator):
        def close(self):
            super().close()
            sdk.graph_executor.shutdown(force=True)
            sdk.graph_executor._shutdown = True
    sdk.iterator = NativeIterator([dict(doc_id=np.asarray(["a"]), embedding=vectors(1))],
                                  error=error)
    if error is None:
        executor.embed_batch(("a",), [Image.fromarray(rgb(1)[0])])
    else:
        with pytest.raises(ValueError) as caught:
            executor.embed_batch(("a",), [Image.fromarray(rgb(1)[0])])
        assert caught.value is error and executor._unconfirmed_work
    assert sdk.iterator.close_calls == 1
    sdk.graph_executor.shutdown.assert_called_once_with(force=True)


def test_owner_does_not_guess_unknown_graph_shutdown_state(sdk):
    executor = owner(sdk)
    sdk.graph_executor = SimpleNamespace(shutdown=Mock())
    sdk.iterator = BatchIterator(error=MemoryError("fixture unrecognized executor"))
    with pytest.raises(MemoryError):
        executor.embed_batch(("a",), [Image.fromarray(rgb(1)[0])])
    sdk.graph_executor.shutdown.assert_not_called()
    assert executor._unconfirmed_work


def test_cancelled_batch_retains_primary_releases_lock_and_closes_owned_work(sdk):
    executor = owner(sdk)
    synchronize = Mock()
    executor._handle = SimpleNamespace(synchronize=SimpleNamespace(remote=synchronize))
    primary = asyncio.CancelledError("fixture cancelled iteration")
    sdk.iterator = BatchIterator(error=primary, close_error=OSError("fixture iterator close"))
    images = [Image.fromarray(rgb(1)[0])]
    with pytest.raises(asyncio.CancelledError) as caught:
        executor.embed_batch(("a",), images)
    assert caught.value is primary
    assert any("iterator close also failed: OSError" in note for note in primary.__notes__)
    assert executor._unconfirmed_work and not executor._lock.locked()
    with pytest.raises(RuntimeError, match="completion unknown"):
        executor.embed_batch(("a",), images)
    assert len(sdk.items) == 1
    with pytest.raises(RuntimeError, match="work completion unknown"):
        executor.close()
    synchronize.assert_not_called()
    assert [name for name, *_ in sdk.events] == ["delete", "shutdown"]
    assert not executor._lock.locked()


def test_owner_validates_entire_batch_before_native_graph(sdk):
    executor = owner(sdk)
    image = Image.fromarray(rgb(1)[0])
    for ids, images in [(["a"], [image]), (("a", "a"), [image, image]),
                        (("a", "b"), [image, image.convert("L")]), (("a",), [])]:
        with pytest.raises((ValueError, TypeError)):
            executor.embed_batch(ids, images)
    assert sdk.items == [] and not executor._unconfirmed_work


def test_active_batch_excludes_concurrent_embed_and_close(sdk):
    executor = owner(sdk)
    with executor._lock:
        with pytest.raises(RuntimeError, match="active batch"):
            executor.embed_batch(("a",), [Image.fromarray(rgb(1)[0])])
        with pytest.raises(RuntimeError, match="batch is active"):
            executor.close()
    assert not executor._closed and not sdk.events


def test_close_synchronizes_then_deletes_only_own_application_and_runtime(sdk):
    executor = owner(sdk)
    synchronize = Mock(side_effect=lambda: (sdk.events.append(("synchronize", (), {})),
                        Response(dict(service_uid="service-fixture")))[1])
    executor._handle = SimpleNamespace(synchronize=SimpleNamespace(remote=synchronize))
    executor.close()
    executor.close()
    assert [name for name, *_ in sdk.events] == ["synchronize", "delete", "shutdown"]
    assert sdk.events[1][1] == ("fixture-owned-application",)
    assert executor.identity["service_before_close"]["service_uid"] == "service-fixture"
    assert executor._closed and executor._handle is None


def test_close_attempts_all_cleanup_and_preserves_first_failure(sdk):
    executor = owner(sdk)
    primary = ValueError("fixture synchronization")
    executor._handle = SimpleNamespace(synchronize=SimpleNamespace(remote=lambda: Response(error=primary)))
    sdk.failures.update(delete=OSError("fixture delete"), shutdown=RuntimeError("fixture shutdown"))
    with pytest.raises(ValueError) as caught:
        executor.close()
    assert caught.value is primary and executor._close_error is primary
    assert [name for name, *_ in sdk.events] == ["delete", "shutdown"]
    assert len(primary.__notes__) == 2
    with pytest.raises(RuntimeError, match="Prior CLIP close failed") as repeated:
        executor.close()
    assert repeated.value.__cause__ is primary


def test_close_keeps_changed_instance_receipt_and_cleans_owned_resources(sdk):
    executor = owner(sdk)
    raw_ready = dict(service_uid="restarted-service", failed=False, returned_rows=0)
    executor._handle = SimpleNamespace(synchronize=SimpleNamespace(remote=lambda: Response(raw_ready)))
    sdk.failures.update(delete=OSError("fixture delete"), shutdown=ValueError("fixture shutdown"))
    with pytest.raises(RuntimeError, match="instance changed or failed") as caught:
        executor.close()
    assert executor.identity["service_before_close"] is raw_ready
    assert executor._close_error is caught.value
    assert [name for name, *_ in sdk.events] == ["delete", "shutdown"]
    assert len(caught.value.__notes__) == 2
    with pytest.raises(RuntimeError, match="Prior CLIP close failed") as repeated:
        executor.close()
    assert repeated.value.__cause__ is caught.value


def test_unknown_work_skips_sync_and_stays_unknown_after_cleanup(sdk):
    executor = owner(sdk)
    executor._unconfirmed_work = True
    synchronize = Mock()
    executor._handle = SimpleNamespace(synchronize=SimpleNamespace(remote=synchronize))
    with pytest.raises(RuntimeError, match="work completion unknown") as caught:
        executor.close()
    synchronize.assert_not_called()
    assert [name for name, *_ in sdk.events] == ["delete", "shutdown"]
    assert executor._close_error is caught.value and executor._unconfirmed_work
    with pytest.raises(RuntimeError, match="Prior CLIP close failed"):
        executor.close()


def test_unknown_owner_does_not_delete_or_shutdown_a_foreign_runtime(sdk):
    executor = owner(sdk)
    executor._owns_runtime = False
    executor._unconfirmed_work = True
    with pytest.raises(RuntimeError, match="work completion unknown"):
        executor.close()
    assert sdk.events == [] and sdk.initialized
    assert executor._unconfirmed_work


def test_startup_owns_local_runtime_disabled_proxy_and_bounded_replica(sdk, startup):
    executor = native.RayServeClipExecutor(**startup)
    events = {name: (args, kwargs) for name, args, kwargs in sdk.events}
    assert events["init"][1] == dict(address="local", num_cpus=4, num_gpus=0,
        include_dashboard=False, _temp_dir=str(startup["ray_temp_dir"].resolve()), log_to_driver=False)
    assert events["start"][1] == dict(proxy_location="Disabled")
    assert events["deployment"][1] == dict(num_replicas=1,
        ray_actor_options=dict(num_cpus=1, num_gpus=0), max_ongoing_requests=1,
        max_queued_requests=1, max_constructor_retry_count=1)
    assert events["bind"][0] == (native._ClipReplica, str(startup["model_path"]),
        str(startup["processor_path"]), "cpu", 32)
    assert events["run"][1] == dict(name=executor._app_name, route_prefix=None)
    assert executor.identity["max_constructor_retry_count"] == 1
    assert executor.identity["data_task_max_retries"] == 0
    assert executor.identity["data_retried_map_errors"] is False
    assert sdk.ready_response.timeouts == [60]


@pytest.mark.parametrize("reason", ["existing_runtime", "map_retry"])
def test_startup_refuses_foreign_runtime_or_map_replay_without_mutations(sdk, startup, reason):
    sdk.initialized = reason == "existing_runtime"
    sdk.retried_map_errors = reason == "map_retry"
    with pytest.raises(ValueError):
        native.RayServeClipExecutor(**startup)
    assert sdk.events == []


@pytest.mark.parametrize("stage", ["init", "start", "run", "ready", "identity"])
def test_partial_startup_failure_shuts_owned_runtime_and_keeps_first_error(sdk, startup, stage):
    primary = ValueError("fixture startup " + stage)
    if stage == "ready":
        sdk.ready_response = Response(error=primary)
    elif stage == "identity":
        sdk.ready_response.value["artifacts_sha256"] = {"changed": "artifact"}
    else:
        sdk.failures[stage] = primary
    sdk.failures["shutdown"] = OSError("fixture shutdown")
    with pytest.raises(ValueError) as caught:
        native.RayServeClipExecutor(**startup)
    if stage != "identity":
        assert caught.value is primary
    assert "shutdown" in [name for name, *_ in sdk.events]
    assert any("startup cleanup also failed" in note for note in caught.value.__notes__)


@pytest.mark.parametrize("failure", ["ready_timeout", "identity_mismatch"])
def test_startup_failure_skips_additional_sync_and_preserves_original(sdk, startup, monkeypatch, failure):
    primary = TimeoutError("fixture ready timeout")
    if failure == "ready_timeout":
        sdk.ready_response = Response(error=primary)
    else:
        sdk.ready_response.value["artifacts_sha256"] = {"changed": "artifact"}
    # A real handle has synchronize.remote. Do not let a missing fixture method
    # replace the waiting RPC that startup failure cleanup must avoid.
    sdk.synchronize_remote.side_effect = AssertionError("startup cleanup waited on synchronize")
    observed_errors = []
    original_close = native.RayServeClipExecutor.close

    def observe_close(executor):
        observed_errors.append(sys.exception())
        return original_close(executor)

    monkeypatch.setattr(native.RayServeClipExecutor, "close", observe_close)
    with pytest.raises(TimeoutError if failure == "ready_timeout" else ValueError) as caught:
        native.RayServeClipExecutor(**startup)
    assert observed_errors[0] is caught.value
    if failure == "ready_timeout":
        assert caught.value is primary
    else:
        assert "artifact or software identity differs" in str(caught.value)
    sdk.synchronize_remote.assert_not_called()
    assert [name for name, *_ in sdk.events][-2:] == ["delete", "shutdown"]


@pytest.mark.parametrize("options", [dict(batch_size=True), dict(batch_size=0),
    dict(batch_size=33, max_batch_rows=32), dict(max_batch_rows=5001), dict(dtype="float16"),
    dict(device="mps"), dict(request_timeout_s=True), dict(request_timeout_s=float("nan")),
    dict(request_timeout_s=float("inf")), dict(request_timeout_s=0)])
def test_invalid_configuration_rejected_before_model_or_sdk(options):
    with pytest.raises(ValueError):
        native.RayServeClipExecutor(model_path="missing", ray_temp_dir="missing", **options)


def test_artifact_identity_hashes_actual_model_and_independent_processor(tmp_path):
    model, processor = tmp_path / "model", tmp_path / "processor"
    model.mkdir()
    processor.mkdir()
    payloads = {"weights": b"actual-weight", "model_config": b"actual-model-config",
                "processor_config": b"independent-processor"}
    (model / "pytorch_model.bin").write_bytes(payloads["weights"])
    (model / "config.json").write_bytes(payloads["model_config"])
    (processor / "preprocessor_config.json").write_bytes(payloads["processor_config"])
    assert native._artifacts(model, processor) == {
        name: hashlib.sha256(value).hexdigest() for name, value in payloads.items()}
    (model / "model.safetensors").write_bytes(b"preferred-weight")
    assert native._artifacts(model, processor)["weights"] == hashlib.sha256(b"preferred-weight").hexdigest()
