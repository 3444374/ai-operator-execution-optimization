"""Opt-in real Ray Data CPU cleanup; the model/Serve workload is a stand-in.

Run with SEMLOOM_NATIVE_CALL_TEST=1 in the pinned Ray 2.56.1 environment.
"""
from importlib.util import find_spec
import os
import tempfile
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
from PIL import Image
import pytest

from src.baselines.image.frameworks import ray_serve_clip as native
from src.modalities.image.contracts import EmbeddingSemantics


pytestmark = pytest.mark.skipif(
    os.environ.get("SEMLOOM_NATIVE_CALL_TEST") != "1" or find_spec("ray") is None,
    reason="actual Ray 2.56.1 CPU cleanup needs SEMLOOM_NATIVE_CALL_TEST=1 and Ray")


def cpu_embeddings(batch, **kwargs):
    vectors = np.zeros((len(batch["doc_id"]), 512), np.float32)
    vectors[:, 0] = 1
    return dict(doc_id=batch["doc_id"], embedding=vectors)


def test_native_constructor_gap_stops_only_its_graph_and_keeps_owner_unknown(monkeypatch):
    import ray
    from ray.data import DataContext
    import ray.data.iterator as iterator_module
    from ray.data._internal.iterator.iterator_impl import DataIteratorImpl
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

    assert ray.__version__ == native.RAY_VERSION
    assert not ray.is_initialized(), "test must own a fresh Ray runtime"
    ray.init(address="local", num_cpus=2, num_gpus=0, include_dashboard=False,
             _node_ip_address="127.0.0.1", log_to_driver=False,
             _temp_dir=tempfile.mkdtemp(prefix="ray-clip-cleanup-", dir="/tmp"))
    try:
        DataContext.get_current().enable_progress_bars = False
        DataContext.get_current().enable_auto_log_stats = False
        assert DataContext.get_current().retried_map_errors is False
        owner = object.__new__(native.RayServeClipExecutor)
        owner.semantics = EmbeddingSemantics("cpu-fixture", "cpu-fixture", 512)
        owner.batch_size, owner.max_batch_rows, owner.request_timeout_s = 1, 8, 10
        owner._lock = threading.Lock()
        owner._closed = owner._unconfirmed_work = False
        owner._owns_runtime, owner._close_error = True, None
        owner._handle, owner._service_uid = object(), "cpu-fixture"
        owner._app_name = "cpu-fixture-owned-application"
        owner._ray, owner._serve = ray, SimpleNamespace(delete=Mock())
        owner.identity, owner.reuse_observations = {}, []
        monkeypatch.setattr(native, "clip_service_batch", cpu_embeddings)
        primary = MemoryError("injected native BatchIterator constructor")
        executors, forced_shutdowns, end_hooks = [], [], []
        to_refs = DataIteratorImpl._to_ref_bundle_iterator
        shutdown = StreamingExecutor.shutdown
        on_end = DataIteratorImpl._on_iteration_end

        def capture_executor(iterator):
            result = to_refs(iterator)
            executors.append(result[3])
            return result

        def fail_constructor(*args, **kwargs):
            assert executors[-1]._execution_started is True
            assert executors[-1]._shutdown is False
            raise primary

        def record_shutdown(executor, force, exception=None):
            if force:
                forced_shutdowns.append(executor)
            return shutdown(executor, force=force, exception=exception)

        def record_end(iterator, executor):
            end_hooks.append(executor)
            return on_end(iterator, executor)

        monkeypatch.setattr(DataIteratorImpl, "_to_ref_bundle_iterator", capture_executor)
        monkeypatch.setattr(iterator_module, "BatchIterator", fail_constructor)
        monkeypatch.setattr(StreamingExecutor, "shutdown", record_shutdown)
        monkeypatch.setattr(DataIteratorImpl, "_on_iteration_end", record_end)
        images = [Image.new("RGB", (7, 9)) for _ in range(8)]
        with pytest.raises(MemoryError) as caught:
            owner.embed_batch(tuple(str(index) for index in range(8)), images)
        assert caught.value is primary and end_hooks == []
        assert forced_shutdowns == executors and len(executors) == 1
        assert executors[0]._shutdown is True and not executors[0].is_alive()
        assert owner._unconfirmed_work and ray.is_initialized()
        with pytest.raises(RuntimeError, match="completion unknown"):
            owner.embed_batch(("retry",), images[:1])
        with pytest.raises(RuntimeError, match="work completion unknown"):
            owner.close()
        owner._serve.delete.assert_called_once_with(owner._app_name)
        assert owner._unconfirmed_work and not ray.is_initialized()
    finally:
        ray.shutdown()
