"""Full HSE source cleanup with deterministic Arrow/Ray substitutes."""

import gc
import inspect
import unittest
import weakref
from unittest.mock import patch

from src.modalities.image import staged_execution
from src.modalities.image.contracts import ImageEmbeddingResult
from src.modalities.image.execution import ProjectRayWorkerPool
from src.scheduling.runtime.stage_broker import BoundedStageBroker
from tests.modalities.image import test_hse_static_lifecycle as lifecycle
from tests.modalities.image import test_staged_image_execution as fixture


class _ClosingIterator:
    def __init__(self, batches, events, *, next_error=None, close_error=None):
        self.batches, self.events = batches, events
        self.next_error, self.close_error = next_error, close_error
        self.read = self.close_calls = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.read == len(self.batches):
            if self.next_error is not None:
                raise self.next_error
            raise StopIteration
        batch = self.batches[self.read]
        self.read += 1
        return batch

    def close(self):
        self.close_calls += 1
        self.events.append("source_close")
        if self.close_error is not None:
            raise self.close_error


class _Source(fixture._Source):
    def __init__(self, batches, events, **kwargs):
        super().__init__(batches)
        self.iterator = _ClosingIterator(batches, events, **kwargs)

    def to_arrow_iter(self, *, results_buffer_size):
        assert results_buffer_size == 2
        return self.iterator


class _FailedScalar:
    def __init__(self, error):
        self.error = error

    def as_py(self):
        raise self.error


def _pool(events, *, cpu_count=1, gpu_count=1, fence_error=None):
    return ProjectRayWorkerPool(
        tuple(lifecycle.FenceCpu(events, fence_error=fence_error) for _ in range(cpu_count)),
        tuple(lifecycle.FenceGpu(events, fence_error=fence_error) for _ in range(gpu_count)),
    )


class HSESourceClosureTests(unittest.TestCase):
    def test_normal_eof_closes_once_without_abort_fences(self):
        events = []
        source = _Source(lifecycle.source(("a", "b"))._batches, events)
        with fixture._fake_ray_module(lifecycle.AbortRay()):
            result = lifecycle.run(source, _pool(events))
        self.assertTrue(result.audit["exactly_once"])
        self.assertEqual(source.iterator.read, 2)
        self.assertEqual(source.iterator.close_calls, 1)
        self.assertEqual(events, ["source_close"])

    def test_empty_source_closes_once(self):
        events = []
        source = _Source([], events)
        with fixture._fake_ray_module(lifecycle.AbortRay()):
            result = lifecycle.run(source, _pool(events), rows=())
        self.assertTrue(result.audit["exactly_once"])
        self.assertEqual(source.iterator.close_calls, 1)

    def test_materialization_error_finalizes_generator_with_traceback_retained(self):
        events = []
        error = ValueError("fixture row materialization failed")

        class GeneratorSource(fixture._Source):
            def to_arrow_iter(self, *, results_buffer_size):
                assert results_buffer_size == 2

                def generate():
                    try:
                        yield from self._batches
                    finally:
                        events.append("source_finalized")

                iterator = generate()
                self.iterator_ref = weakref.ref(iterator)
                return iterator

        source = GeneratorSource([
            fixture._RecordBatch(doc_id=[_FailedScalar(error)], image=[fixture._Scalar(b"a")]),
        ])
        retained = None
        with fixture._fake_ray_module(lifecycle.AbortRay()):
            try:
                lifecycle.run(source, _pool(events), rows=("a",), active=1)
            except BaseException as caught:
                # assertRaises removes the traceback; keep the actual owner here.
                retained = caught
        gc.collect()
        iterator = source.iterator_ref()
        try:
            self.assertIs(retained, error)
            self.assertIsNotNone(retained.__traceback__)
            self.assertEqual(events, ["source_finalized"])
            if iterator is not None:
                self.assertEqual(inspect.getgeneratorstate(iterator), inspect.GEN_CLOSED)
        finally:
            if iterator is not None:
                iterator.close()

    def test_partial_materialization_error_closes_without_reading_tail(self):
        for column in ("doc_id", "image"):
            with self.subTest(column=column):
                events = []
                error = ValueError("fixture scalar failed")
                batches = lifecycle.source(("a", "b", "c"))._batches
                batches[1][column] = [_FailedScalar(error)]
                source = _Source(batches, events, close_error=RuntimeError("fixture close failed"))
                with fixture._fake_ray_module(lifecycle.AbortRay()):
                    with self.assertRaises(ValueError) as caught:
                        lifecycle.run(source, _pool(events), rows=("a", "b", "c"), active=3,
                                      encoded=3, ready=3)
                self.assertIs(caught.exception, error)
                self.assertEqual(source.iterator.read, 2)
                self.assertEqual(source.iterator.close_calls, 1)
                self.assertIn("HSE source close raised RuntimeError", error.__notes__)

    def test_next_error_closes_before_first_row_and_after_partial_admission(self):
        for rows in ((), ("a",)):
            with self.subTest(rows=rows):
                events = []
                error = ValueError("fixture next failed")
                source = _Source(lifecycle.source(rows)._batches, events, next_error=error)
                with fixture._fake_ray_module(lifecycle.AbortRay()):
                    with self.assertRaises(ValueError) as caught:
                        lifecycle.run(source, _pool(events), rows=("a", "b"))
                self.assertIs(caught.exception, error)
                self.assertEqual(source.iterator.close_calls, 1)
                self.assertEqual(events, ["source_close"])

    def test_eof_close_error_is_propagated_once(self):
        for error in (RuntimeError("fixture close failed"), KeyboardInterrupt("fixture close stop")):
            with self.subTest(error=type(error).__name__):
                events = []
                source = _Source(lifecycle.source(("a",))._batches, events, close_error=error)
                pool = _pool(events)
                with fixture._fake_ray_module(lifecycle.AbortRay()):
                    with self.assertRaises(type(error)) as caught:
                        lifecycle.run(source, pool, rows=("a",), active=1)
                self.assertIs(caught.exception, error)
                self.assertEqual(source.iterator.close_calls, 1)
                self.assertEqual(events, ["source_close"])
                self.assertNotIn(pool, staged_execution._unconfirmed_runs)

    def test_cancel_closes_partial_source_before_fencing_each_stage(self):
        for stage in ("prepare", "model"):
            for close_error in (None, RuntimeError("fixture close failed"),
                                KeyboardInterrupt("fixture close stop")):
                with self.subTest(stage=stage, close_error=type(close_error).__name__):
                    events, brokers = [], []
                    error = KeyboardInterrupt("fixture cancelled")

                    class CancelRay(lifecycle.AbortRay):
                        def wait(self, refs, *, num_returns, timeout=None):
                            if timeout is None and (stage == "prepare" or any(
                                    isinstance(ref.value, ImageEmbeddingResult) for ref in refs)):
                                raise error
                            return super().wait(refs, num_returns=num_returns, timeout=timeout)

                    class ObservedBroker(BoundedStageBroker):
                        def __init__(self, limits):
                            super().__init__(limits)
                            brokers.append(self)

                        def fail_prepare(self, lease_id, *, requeue):
                            events.append("prepare_released")
                            return super().fail_prepare(lease_id, requeue=requeue)

                        def fail_model(self, lease_id, *, requeue):
                            events.append("model_released")
                            return super().fail_model(lease_id, requeue=requeue)

                    source = _Source(lifecycle.source(("a", "b"))._batches, events,
                                     close_error=close_error)
                    with fixture._fake_ray_module(CancelRay()), patch.object(
                            staged_execution, "BoundedStageBroker", ObservedBroker):
                        with self.assertRaises(KeyboardInterrupt) as caught:
                            lifecycle.run(source, _pool(events), active=1)
                    self.assertIs(caught.exception, error)
                    self.assertEqual(source.iterator.read, 1)
                    self.assertEqual(source.iterator.close_calls, 1)
                    self.assertEqual(events, ["source_close", "cpu_fence" if stage == "prepare"
                                             else "gpu_fence", stage + "_released"])
                    self.assertTrue(brokers[0].is_drained())
                    self.assertEqual(brokers[0].snapshot().ready_held_bytes, 0)
                    if close_error is not None:
                        self.assertIn("HSE source close raised " + type(close_error).__name__,
                                      error.__notes__)

    def test_next_and_close_errors_keep_pending_model_unknown_after_failed_fence(self):
        events = []
        error = ValueError("fixture next failed with model pending")
        source = _Source(lifecycle.source(("a", "b"))._batches, events, next_error=error,
                         close_error=RuntimeError("fixture close failed"))
        pool = _pool(events, cpu_count=2, gpu_count=2,
                     fence_error=RuntimeError("fixture fence unavailable"))
        with fixture._fake_ray_module(lifecycle.AbortRay()):
            with self.assertRaises(ValueError) as caught:
                lifecycle.run(source, pool)
            retained = caught.exception.hse_unconfirmed_work
            self.assertIs(caught.exception, error)
            self.assertEqual(source.iterator.close_calls, 1)
            self.assertEqual(events, ["source_close", "gpu_fence"])
            self.assertEqual(retained.broker.snapshot().model_inflight, 1)
            self.assertEqual(retained.broker.snapshot().ready_held_bytes, 48)
            self.assertEqual(len(retained.calls), 1)
            self.assertIsNotNone(retained.calls[0].payload_ref)
            self.assertIn("HSE source close raised RuntimeError", error.__notes__)
            rejected = _Source(lifecycle.source(("c",))._batches, events)
            with self.assertRaisesRegex(RuntimeError, "unconfirmed work"):
                lifecycle.run(rejected, pool, rows=("c",))
            self.assertEqual(rejected.iterator.read, 0)
            self.assertEqual(rejected.iterator.close_calls, 0)


if __name__ == "__main__":
    unittest.main()
