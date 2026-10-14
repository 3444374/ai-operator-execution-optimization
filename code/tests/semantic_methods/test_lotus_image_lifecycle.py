"""Fault fixtures for LOTUS image ownership; no SDK, model, GPU or HTTP."""
import asyncio
from contextlib import contextmanager
import importlib.util
import hashlib
import json
from pathlib import Path
import sys
import threading
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import tempfile

import numpy as np

from src.modalities.image.contracts import EmbeddingSemantics
from src.semantic_methods.lotus.image_backend import RayDataImageExecutor, SemLoomImageExecutor
from src.baselines.image.frameworks import daft_clip


def timing_error():
    error = OSError("fixture timing writer failed")
    error._semloom_timing_failure = True
    return error


def load_cli(filename):
    path = Path(__file__).parents[2] / "scripts" / "experiments" / filename
    spec = importlib.util.spec_from_file_location("fixture_" + path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SemLoomImageLifecycleTests(unittest.TestCase):
    def startup(self, *, created=2, kill_error=None):
        handles = [SimpleNamespace(ready=SimpleNamespace(remote=Mock())) for _ in range(2)]
        ray = SimpleNamespace(is_initialized=lambda: True, get=Mock(side_effect=ValueError("startup failed")),
                              kill=Mock(side_effect=kill_error))
        factories = [SimpleNamespace(remote=Mock(return_value=handle)) for handle in handles]
        if created == 1:
            factories[1].remote.side_effect = ValueError("startup failed")
        ray.remote = lambda **options: lambda cls: factories.pop(0)
        with patch.dict(sys.modules, {"ray": ray}):
            with self.assertRaisesRegex(ValueError, "startup failed") as caught:
                SemLoomImageExecutor(model="fixture")
        return caught.exception, ray, handles[:created]

    def test_partial_startup_requests_all_known_actors_and_retains_unknown_exit(self):
        for created in (1, 2):
            with self.subTest(created=created):
                error, ray, handles = self.startup(created=created)
                self.assertEqual(ray.kill.call_count, created)
                failure = error.image_worker_pool_startup
                self.assertEqual(failure.actor_handles, tuple(handles))
                self.assertEqual(failure.physical_exit_status, "unconfirmed")
                self.assertEqual(len(failure.termination_request_outcomes), created)

    def test_startup_keeps_primary_when_every_actor_kill_fails(self):
        error, ray, handles = self.startup(kill_error=RuntimeError("kill failed"))
        self.assertEqual(ray.kill.call_count, 2)
        self.assertEqual(str(error), "startup failed")
        self.assertTrue(any("termination" in note for note in error.__notes__))
        self.assertEqual(error.image_worker_pool_startup.actor_handles, tuple(handles))

    def executor(self):
        executor = SemLoomImageExecutor.__new__(SemLoomImageExecutor)
        executor.pool = SimpleNamespace(preprocessors=(object(),), gpu_actors=(object(),))
        executor._closed = executor._unconfirmed_work = False
        executor._lock = threading.Lock()
        executor.semantics = EmbeddingSemantics("fixture", "fixture", 512)
        executor.hse = False
        executor.batch_size, executor.window = 1, 1
        executor.model, executor.processor, executor.dtype = "fixture", "fixture", "float32"
        executor.encoded_bytes_limit = executor.encoded_block_bytes_upper_bound = None
        executor.execution_observations = []
        return executor

    def test_failed_stop_seals_executor_but_retains_cleanup_handles(self):
        executor = self.executor()
        pool = executor.pool
        with patch("src.modalities.image.execution.stop_semloom_ray_worker_pool", side_effect=RuntimeError("stop failed")):
            with self.assertRaisesRegex(RuntimeError, "stop failed"):
                executor.close()
        self.assertIs(executor.pool, pool)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            executor.embed_batch(("a",), [b"fixture"])
        with patch("src.modalities.image.execution.stop_semloom_ray_worker_pool") as stop:
            executor.close()
        stop.assert_called_once_with(pool)
        self.assertIsNone(executor.pool)

    def test_cancelled_graph_cannot_reuse_pool_before_owner_close(self):
        executor = self.executor()
        daft = SimpleNamespace(from_pydict=lambda values: object())
        with patch.dict(sys.modules, {"daft": daft}), \
                patch("src.modalities.image.execution.run_semloom_ray_pipeline", side_effect=asyncio.CancelledError("cancelled")):
            with self.assertRaisesRegex(asyncio.CancelledError, "cancelled"):
                executor.embed_batch(("a",), [b"fixture"])
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                executor.embed_batch(("b",), [b"fixture"])

    def test_close_rejects_active_batch(self):
        executor = self.executor()
        executor._lock.acquire()
        try:
            with self.assertRaisesRegex(RuntimeError, "active"):
                executor.close()
        finally:
            executor._lock.release()

    def test_gallery_and_query_keep_static_hse_and_reuse_confirmed_pool(self):
        executor = self.executor()
        executor.hse = True
        values = np.zeros((1, 512), dtype=np.float32)
        values[:, 0] = 1

        def run(source, **kwargs):
            self.assertIsNone(kwargs.get("model_dispatch"))
            kwargs["embedding_capture"].add(("a",), values)
            return SimpleNamespace(encoded_bytes_peak=7, ready_bytes_peak=0,
                batch_preprocess_s=(), batch_host_copy_s=(), batch_actor_service_s=(),
                batch_submit_s=(), device_input_bytes=0)

        with patch.dict(sys.modules, {"daft": SimpleNamespace(from_pydict=lambda values: object())}), \
                patch("src.modalities.image.staged_execution.run_semloom_ray_hse_pipeline", side_effect=run) as pipeline:
            for _ in range(2):
                result = executor.embed_batch(("a",), [b"fixture"])
                np.testing.assert_array_equal(result.embeddings, values)
                self.assertFalse(executor._unconfirmed_work)
            self.assertEqual(pipeline.call_count, 2)


class RayImageLifecycleTests(unittest.TestCase):
    def graph(self, *, error=None, close_error=None, shutdown_error=None, open_error=None):
        values = np.zeros((1, 512), dtype=np.float32)
        values[:, 0] = 1
        batches = iter([{"doc_id": ["a"], "embedding": values}])
        iterator = Mock()
        iterator.__iter__ = Mock(return_value=iterator)
        iterator.__next__ = Mock(side_effect=error if error is not None else lambda: next(batches))
        iterator.close = Mock(side_effect=close_error)
        owner = SimpleNamespace(shutdown=Mock(side_effect=shutdown_error))
        graph = Mock(_current_executor=owner)
        graph.map_batches.return_value = graph
        graph.iter_batches = Mock(side_effect=open_error, return_value=iterator)
        ray = ModuleType("ray")
        data = ModuleType("ray.data")
        ray.data, ray.__path__ = data, []
        ray.is_initialized = lambda: True
        data.from_items = Mock(return_value=graph)
        data.ActorPoolStrategy = lambda **kwargs: object()
        return {"ray": ray, "ray.data": data}, graph, iterator, owner

    def test_eof_closes_iterator_and_graph_and_seals_explicit_close(self):
        modules, graph, iterator, owner = self.graph()
        executor = RayDataImageExecutor(model="fixture")
        with patch.dict(sys.modules, modules):
            result = executor.embed_batch(("a",), [b"fixture"])
            self.assertEqual(result.doc_ids, ("a",))
            iterator.close.assert_called_once()
            owner.shutdown.assert_called_once_with(force=True)
            executor.close()
            with self.assertRaisesRegex(RuntimeError, "closed"):
                executor.embed_batch(("a",), [b"fixture"])

    def test_execution_and_cancel_errors_survive_both_cleanup_failures(self):
        for error in (ValueError("bad output"), asyncio.CancelledError("cancelled")):
            with self.subTest(error=type(error).__name__):
                modules, graph, iterator, owner = self.graph(error=error,
                    close_error=RuntimeError("iterator failed"), shutdown_error=OSError("shutdown failed"))
                executor = RayDataImageExecutor(model="fixture")
                with patch.dict(sys.modules, modules):
                    with self.assertRaises(type(error)) as caught:
                        executor.embed_batch(("a",), [b"fixture"])
                    self.assertIs(caught.exception, error)
                    iterator.close.assert_called_once()
                    owner.shutdown.assert_called_once_with(force=True)
                    self.assertTrue(executor._unconfirmed_work)
                    with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                        executor.embed_batch(("a",), [b"fixture"])
                    with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                        executor.close()

    def test_iterator_open_error_still_shuts_down_graph(self):
        modules, graph, iterator, owner = self.graph(open_error=RuntimeError("open failed"))
        executor = RayDataImageExecutor(model="fixture")
        with patch.dict(sys.modules, modules):
            with self.assertRaisesRegex(RuntimeError, "open failed"):
                executor.embed_batch(("a",), [b"fixture"])
        owner.shutdown.assert_called_once_with(force=True)
        iterator.close.assert_not_called()

    def test_cleanup_failure_overrides_only_timing_failure(self):
        diagnostic = timing_error()
        actual = RuntimeError("shutdown failed")
        modules, graph, iterator, owner = self.graph(error=diagnostic, shutdown_error=actual)
        executor = RayDataImageExecutor(model="fixture")
        with patch.dict(sys.modules, modules):
            with self.assertRaises(RuntimeError) as caught:
                executor.embed_batch(("a",), [b"fixture"])
        self.assertIs(caught.exception, actual)
        self.assertTrue(any("timing" in note for note in actual.__notes__))

    def test_eof_cleanup_failure_refuses_new_graph(self):
        modules, graph, iterator, owner = self.graph(close_error=OSError("iterator failed"))
        executor = RayDataImageExecutor(model="fixture")
        with patch.dict(sys.modules, modules):
            with self.assertRaisesRegex(OSError, "iterator failed"):
                executor.embed_batch(("a",), [b"fixture"])
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                executor.embed_batch(("a",), [b"fixture"])
        self.assertEqual(modules["ray.data"].from_items.call_count, 1)
        owner.shutdown.assert_called_once_with(force=True)

    def test_ray_iterable_wrapper_closes_actual_generator_after_consumer_error(self):
        modules, graph, iterator, owner = self.graph(error=ValueError("consumer failed"))

        class Iterable:
            def __iter__(self):
                return iterator

        graph.iter_batches.return_value = Iterable()
        executor = RayDataImageExecutor(model="fixture")
        with patch.dict(sys.modules, modules):
            with self.assertRaisesRegex(ValueError, "consumer failed"):
                executor.embed_batch(("a",), [b"fixture"])
        iterator.close.assert_called_once()
        owner.shutdown.assert_called_once_with(force=True)


class DaftImageLifecycleTests(unittest.TestCase):
    def executor(self, *, open_error=None, close_error=None, consume_error=None):
        values = np.zeros((1, 512), dtype=np.float32)
        values[:, 0] = 1
        batches = iter([{"doc_id": SimpleNamespace(to_pylist=lambda: ["a"]),
                         "embedding": SimpleNamespace(to_pylist=lambda: values.tolist())}])
        iterator = Mock()
        iterator.__iter__ = Mock(return_value=iterator)
        iterator.__next__ = Mock(side_effect=consume_error if consume_error else lambda: next(batches))
        iterator.close = Mock(side_effect=close_error)

        class Frame:
            def __getitem__(self, key):
                return object()

            def into_batches(self, rows):
                return self

            def with_column(self, *args):
                return self

            def select(self, *args):
                return self

            def to_arrow_iter(self):
                if open_error is not None:
                    raise open_error
                return iterator

        daft = ModuleType("daft")
        functions = ModuleType("daft.functions")
        daft.__path__, daft.functions = [], functions
        daft.from_pydict = lambda values: Frame()
        daft.ImageMode = SimpleNamespace(RGB="RGB")
        functions.decode_image = lambda *args, **kwargs: None
        executor = daft_clip.DaftClipExecutor.__new__(daft_clip.DaftClipExecutor)
        executor._embedder = SimpleNamespace(embed=Mock())
        executor._lock = threading.Lock()
        executor._closed = executor._unconfirmed_work = False
        executor.identity = {"batch_size": 1, "max_batch_rows": 1}
        executor.semantics = EmbeddingSemantics("fixture", "fixture", 512)
        return executor, iterator, {"daft": daft, "daft.functions": functions}

    def test_failed_iterator_open_retains_unknown_native_work(self):
        executor, iterator, modules = self.executor(open_error=RuntimeError("open failed"))
        with patch.dict(sys.modules, modules):
            with self.assertRaisesRegex(RuntimeError, "open failed"):
                executor.embed_batch(("a",), [b"fixture"])
            self.assertTrue(executor._unconfirmed_work)
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                executor.embed_batch(("a",), [b"fixture"])
        iterator.close.assert_not_called()

    def test_real_close_failure_overrides_timing_diagnostic(self):
        actual = RuntimeError("close failed")
        executor, iterator, modules = self.executor(close_error=actual)

        @contextmanager
        def span(name):
            yield
            if name == "daft_image.graph_consume":
                raise timing_error()

        with patch.dict(sys.modules, modules), patch.object(daft_clip, "_span", span):
            with self.assertRaises(RuntimeError) as caught:
                executor.embed_batch(("a",), [b"fixture"])
        self.assertIs(caught.exception, actual)
        self.assertTrue(executor._unconfirmed_work)
        iterator.close.assert_called_once()

    def test_cancellation_keeps_priority_over_close_failure(self):
        primary = asyncio.CancelledError("cancelled")
        executor, iterator, modules = self.executor(consume_error=primary, close_error=OSError("close failed"))
        with patch.dict(sys.modules, modules):
            with self.assertRaises(asyncio.CancelledError) as caught:
                executor.embed_batch(("a",), [b"fixture"])
        self.assertIs(caught.exception, primary)
        iterator.close.assert_called_once()


class LotusImageCliLifecycleTests(unittest.TestCase):
    def run_retrieval(self, directory, primary, *, close_error=None, shutdown_error=None,
                      vectors=False, vector_error=None, write_error=False, receipt_error=False,
                      startup=False):
        module = load_cli("run_lotus_image_retrieval.py")
        root = Path(directory)
        identity = root / "identity.json"
        identity.write_text(json.dumps(dict(qualified=True, model_dtype="float32")))
        output = root / "output"
        args = ["--manifest", str(root / "unused"), "--image-root", str(root), "--stock-model", str(root),
                "--model", str(root), "--embedding-identity", str(identity), "--arm", "semloom_hse",
                "--index-dir", str(root / "index"), "--output-dir", str(output), "--ray-temp-dir", str(root / "ray"), "--rows", "1"]
        executor = SimpleNamespace(close=Mock(side_effect=close_error))
        ray = SimpleNamespace(is_initialized=lambda: False, init=Mock(), shutdown=Mock(side_effect=shutdown_error))
        observation = SimpleNamespace(calls=[], vectors={"fixture": np.zeros((1, 512))} if vectors else {})
        stock = lambda *args, **kwargs: SimpleNamespace(transformer=SimpleNamespace(to=lambda **kwargs: None))
        original_write = Path.write_text
        raw = b"fixture"
        (root / "entry.bin").write_bytes(raw)
        manifest = SimpleNamespace(root=root, sha256="fixture", entries=[
            SimpleNamespace(path="entry.bin", doc_id="a", sha256=hashlib.sha256(raw).hexdigest())])

        def workflow(*args, **kwargs):
            kwargs["report"].update(status="succeeded", index_s=0, queries=[])

        def write(path, *args, **kwargs):
            if write_error and path.name == "result.json":
                raise OSError("evidence failed")
            if receipt_error and path.name == "evidence-write.json":
                raise OSError("receipt failed")
            return original_write(path, *args, **kwargs)

        with patch.dict(sys.modules, {"lotus.models": SimpleNamespace(SentenceTransformersRM=stock),
                                    "torch": SimpleNamespace(float32="fixture"), "ray": ray}), \
                patch.multiple(module, validate_image_source=Mock(return_value={}),
                    validate_embedding_identity=Mock(), version=Mock(return_value="fixture"),
                    configure_torch_thread_pools=Mock(return_value={}),
                    SemLoomImageExecutor=Mock(return_value=executor, side_effect=primary if startup else None),
                    make_image_rm=Mock(return_value=SimpleNamespace(observation=observation)),
                    load_image_file_manifest=Mock(return_value=manifest, side_effect=None if startup else primary),
                    decode_rgb_image=Mock(), image_frame=Mock(), run_workflow=Mock(side_effect=workflow)), \
                patch("numpy.savez_compressed", side_effect=vector_error), patch.object(Path, "write_text", write):
            status = module.main(args)
        return status, output, executor, ray

    def test_timing_failure_yields_to_stop_failure_and_shutdown_still_attempted(self):
        with tempfile.TemporaryDirectory() as directory:
            status, output, executor, ray = self.run_retrieval(directory, timing_error(),
                close_error=RuntimeError("stop failed"), shutdown_error=ValueError("shutdown failed"))
            self.assertEqual(status, 1)
            result = json.loads((output / "result.json").read_text())
            self.assertEqual(result["error_type"], "RuntimeError")
            ray.shutdown.assert_called_once()

    def test_result_write_failure_preserves_original_error_and_exit_code(self):
        error = asyncio.CancelledError("cancelled")
        with tempfile.TemporaryDirectory() as directory:
            status, output, executor, ray = self.run_retrieval(directory, error, write_error=True)
            self.assertEqual(status, 1)
            executor.close.assert_called_once()
            ray.shutdown.assert_called_once()
            self.assertTrue(any("evidence" in note for note in error.__notes__))
            self.assertTrue((output / "evidence-write.json").is_file())

    def test_vector_write_failure_keeps_execution_failure_in_result(self):
        error = ValueError("original execution failed")
        with tempfile.TemporaryDirectory() as directory:
            status, output, executor, ray = self.run_retrieval(directory, error, vectors=True,
                vector_error=OSError("vector evidence failed"))
            self.assertEqual(status, 1)
            result = json.loads((output / "result.json").read_text())
            self.assertEqual(result["reason"], str(error))
            self.assertTrue(result["evidence_write_errors"])

    def test_identity_write_failure_keeps_qualification_failure(self):
        module = load_cli("qualify_lotus_clip.py")
        error = ValueError("identity guard failed")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = ["--stock-model", directory, "--model", directory, "--manifest", directory,
                    "--image-root", directory, "--output-dir", str(root / "output")]
            with patch.object(module, "validate_image_source", side_effect=error), \
                    patch.object(Path, "write_text", side_effect=OSError("identity evidence failed")):
                self.assertEqual(module.main(argv), 1)
            self.assertTrue(any("evidence" in note for note in error.__notes__))

    def test_failed_vector_write_without_execution_error_marks_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            status, output, executor, ray = self.run_retrieval(directory, None, vectors=True,
                vector_error=OSError("vector evidence failed"))
            self.assertEqual(status, 1)
            result = json.loads((output / "result.json").read_text())
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["error_type"], "OSError")
            executor.close.assert_called_once()
            ray.shutdown.assert_called_once()

    def test_failed_receipt_updates_completed_result_without_hiding_execution_error(self):
        error = ValueError("original execution failed")
        with tempfile.TemporaryDirectory() as directory:
            status, output, executor, ray = self.run_retrieval(directory, error, receipt_error=True)
            self.assertEqual(status, 1)
            result = json.loads((output / "result.json").read_text())
            self.assertEqual(result["reason"], str(error))
            self.assertEqual(result["evidence_write_errors"], ["receipt: OSError"])

    def test_partial_startup_ownership_is_consumed_before_ray_shutdown(self):
        failure = SimpleNamespace(request_termination=Mock(), actor_handles=("a", "b"),
                                  termination_request_outcomes=("requested", "requested"),
                                  physical_exit_status="unconfirmed")
        error = ValueError("startup failed")
        error.image_worker_pool_startup = failure
        with tempfile.TemporaryDirectory() as directory:
            status, output, executor, ray = self.run_retrieval(directory, error, startup=True)
            self.assertEqual(status, 1)
            failure.request_termination.assert_called_once()
            executor.close.assert_not_called()
            ray.shutdown.assert_called_once()
            result = json.loads((output / "result.json").read_text())
            self.assertEqual(result["startup_actor_count"], 2)
            self.assertEqual(result["startup_physical_exit"], "unconfirmed")

    def test_identity_cleanup_failure_overrides_only_timing_diagnostic(self):
        module = load_cli("qualify_lotus_clip.py")
        actor = SimpleNamespace(close=Mock(side_effect=RuntimeError("stop failed")))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = ["--stock-model", directory, "--model", directory, "--manifest", directory,
                    "--image-root", directory, "--output-dir", str(root / "output")]
            with patch.dict(sys.modules, {"lotus.models": SimpleNamespace(SentenceTransformersRM=Mock())}), \
                    patch.multiple(module, validate_image_source=Mock(return_value={}), version=Mock(return_value="fixture"),
                        configure_torch_thread_pools=Mock(), DirectImageExecutor=Mock(return_value=actor),
                        snapshot_files=Mock(side_effect=timing_error())):
                self.assertEqual(module.main(argv), 1)
            result = json.loads((root / "output/identity.json").read_text())
            self.assertEqual(result["error_type"], "RuntimeError")
            self.assertFalse(result["qualified"])


if __name__ == "__main__":
    unittest.main()
