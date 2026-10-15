"""HSE ownership and dispatch checks with local actor/Arrow substitutes."""

import unittest
import weakref
from unittest.mock import patch

from src.modalities.image import staged_execution
from src.modalities.image.contracts import ImageEmbeddingBatch, ImageEmbeddingResult
from src.modalities.image.execution import ProjectRayWorkerPool
from src.scheduling.runtime.stage_broker import BoundedStageBroker, StageBrokerLimits
from tests.modalities.image import test_staged_image_execution as fixture


def source(rows):
    return fixture._Source([
        fixture._RecordBatch(doc_id=[fixture._Scalar(row)],
                             image=[fixture._Scalar(row.encode())])
        for row in rows
    ])


def run(source_df, pool, *, rows=("a", "b"), active=2, encoded=2, ready=2):
    return staged_execution.run_project_ray_hse_pipeline(
        source_df, worker_pool=pool, expected_doc_ids=frozenset(rows),
        batch_size=1, max_active_batches=active, encoded_block_bytes_upper_bound=1,
        limits=StageBrokerLimits(encoded, ready * 48, ready * 4,
            len(pool.preprocessors), len(pool.gpu_actors)),
        model_revision="model", processor_revision="processor", model_dtype="float16",
        input_size=2, embedding_dimension=2,
    )


class AbortRay(fixture._FakeRay):
    def wait(self, refs, *, num_returns, timeout=None):
        ready = [ref for ref in refs if getattr(ref, "ready", True)][:num_returns]
        return ready, [ref for ref in refs if ref not in ready]

    def get(self, ref, *, timeout=None):
        if isinstance(ref.value, BaseException):
            raise ref.value
        return super().get(ref)


class FenceMethod(fixture._OptionsMethod):
    def __init__(self, callback, ready):
        super().__init__(callback)
        self._ready = ready

    def remote(self):
        reference = super().remote()
        reference.ready = self._ready
        return reference


class FenceCpu(fixture._CpuActor):
    def __init__(self, events, *, error=None, fence_ready=True, fence_error=None):
        super().__init__()
        self.events, self.error = events, error
        self.fence_ready, self.fence_error = fence_ready, fence_error
        self.ready = FenceMethod(self._fence, fence_ready)

    def _preprocess(self, descriptor, encoded):
        if self.error:
            raise self.error
        return super()._preprocess(descriptor, encoded)

    def _fence(self):
        self.events.append("cpu_fence")
        ref_value = self.fence_error or {}
        return ref_value


class FenceGpu(fixture._GpuActor):
    def __init__(self, events, *, error=None, fence_ready=True, fence_error=None):
        super().__init__()
        self.events, self.error, self.fence_error = events, error, fence_error
        self.synchronize = FenceMethod(self._fence, fence_ready)

    def _embed(self, batch):
        if self.error:
            raise self.error
        return super()._embed(batch)

    def _fence(self):
        self.events.append("gpu_fence")
        return self.fence_error


class HSEStaticLifecycleTests(unittest.TestCase):
    def test_drops_settled_payload_refs_before_next_prepare(self):
        live_at_creation, payload_refs = [], []

        class TrackedRef(fixture._Ref):
            def __init__(self, value):
                if isinstance(value, ImageEmbeddingBatch):
                    live_at_creation.append([ref().value.doc_ids for ref in payload_refs
                                             if ref() is not None])
                    payload_refs.append(weakref.ref(self))
                super().__init__(value)

        pool = ProjectRayWorkerPool((fixture._CpuActor(),), (fixture._GpuActor(),))
        with fixture._fake_ray_module(fixture._FakeRay()), patch.object(fixture, "_Ref", TrackedRef):
            result = run(source(("a", "b")), pool, active=1, encoded=1, ready=1)
        self.assertTrue(result.audit["exactly_once"])
        self.assertEqual(live_at_creation, [[], []])
        self.assertTrue(all(ref() is None for ref in payload_refs))

    def test_dispatches_existing_ready_work_before_reading_more_source(self):
        events = []

        class LoggedSource(fixture._Source):
            def to_arrow_iter(self, *, results_buffer_size):
                for batch in self._batches:
                    events.append("source_" + batch["doc_id"][0].as_py())
                    yield batch

        class LoggedGpu(fixture._GpuActor):
            def _embed(self, batch_ref):
                events.append("model_" + batch_ref.value.doc_ids[0])
                return super()._embed(batch_ref)

        pool = ProjectRayWorkerPool((fixture._CpuActor(), fixture._CpuActor()), (LoggedGpu(),))
        rows = ("a", "b", "c", "d")
        with fixture._fake_ray_module(fixture._FakeRay()):
            result = run(LoggedSource(source(rows)._batches), pool, rows=rows, active=3,
                         encoded=3, ready=3)
        self.assertTrue(result.audit["exactly_once"])
        self.assertLess(events.index("model_b"), events.index("source_d"), events)


    def test_ready_dispatch_before_source_error_keeps_abort_ownership(self):
        for error in (ValueError("source refill failed"), KeyboardInterrupt("source cancelled")):
            for fence_error in (None, RuntimeError("device confirmation unknown")):
                with self.subTest(error=type(error).__name__, confirmed=fence_error is None):
                    events, brokers = [], []

                    class FailingSource(fixture._Source):
                        def to_arrow_iter(self, *, results_buffer_size):
                            try:
                                yield from self._batches
                                events.append("source_error")
                                raise error
                            finally:
                                events.append("source_close")

                    class LoggedGpu(FenceGpu):
                        def _embed(self, batch_ref):
                            events.append("model_" + batch_ref.value.doc_ids[0])
                            return super()._embed(batch_ref)

                    class ObservedBroker(BoundedStageBroker):
                        def __init__(self, limits):
                            super().__init__(limits)
                            brokers.append(self)

                    pool = ProjectRayWorkerPool((FenceCpu(events), FenceCpu(events)),
                        (LoggedGpu(events, fence_error=fence_error),))
                    rows = ("a", "b", "c")
                    with fixture._fake_ray_module(AbortRay()), patch.object(
                            staged_execution, "BoundedStageBroker", ObservedBroker):
                        with self.assertRaises(type(error)) as caught:
                            run(FailingSource(source(rows)._batches), pool, rows=rows,
                                active=3, encoded=3, ready=3)
                        self.assertIs(caught.exception, error)
                        self.assertIn("model_b", events)
                        self.assertLess(events.index("model_b"), events.index("source_error"), events)
                        self.assertLess(events.index("source_close"), events.index("gpu_fence"), events)
                        snapshot = brokers[0].snapshot()
                        if fence_error is None:
                            self.assertTrue(brokers[0].is_drained())
                            self.assertEqual(snapshot.ready_held_bytes, 0)
                            self.assertNotIn(pool, staged_execution._unconfirmed_runs)
                        else:
                            self.assertEqual((snapshot.model_inflight, snapshot.ready_held_bytes), (1, 48))
                            self.assertEqual(len(error.hse_unconfirmed_work.calls), 1)
                            self.assertIsNotNone(error.hse_unconfirmed_work.calls[0].payload_ref)
                            with self.assertRaisesRegex(RuntimeError, "unconfirmed work"):
                                run(source(("d",)), pool, rows=("d",), active=1)

    def test_dispatches_ready_work_before_eof_and_drains_without_abort_fences(self):
        events = []

        class LoggedSource(fixture._Source):
            def to_arrow_iter(self, *, results_buffer_size):
                try:
                    yield from self._batches
                    events.append("source_eof")
                finally:
                    events.append("source_close")

        class LoggedGpu(FenceGpu):
            def _embed(self, batch_ref):
                events.append("model_" + batch_ref.value.doc_ids[0])
                return super()._embed(batch_ref)

        pool = ProjectRayWorkerPool((FenceCpu(events), FenceCpu(events)), (LoggedGpu(events),))
        rows = ("a", "b", "c")
        with fixture._fake_ray_module(AbortRay()):
            result = run(LoggedSource(source(rows)._batches), pool, rows=rows,
                         active=3, encoded=3, ready=3)
        self.assertTrue(result.audit["exactly_once"])
        self.assertLess(events.index("model_b"), events.index("source_eof"), events)
        self.assertEqual(events.count("source_close"), 1)
        self.assertNotIn("cpu_fence", events)
        self.assertNotIn("gpu_fence", events)

    def test_records_admission_peak_when_preparing_is_capacity_blocked(self):
        rows = ("a", "b", "c", "d", "e")
        pool = ProjectRayWorkerPool((fixture._CpuActor(),), (fixture._GpuActor(),))
        with fixture._fake_ray_module(fixture._FakeRay()):
            result = run(source(rows), pool, rows=rows, active=4, encoded=2, ready=2)
        self.assertEqual(result.pending_batches_peak, 4)

    def test_submit_failure_fences_actor_before_releasing_lease(self):
        for stage in ("prepare", "model"):
            with self.subTest(stage=stage):
                events, brokers = [], []

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

                error = RuntimeError("ambiguous submit")
                cpu = FenceCpu(events, error=error if stage == "prepare" else None)
                gpu = FenceGpu(events, error=error if stage == "model" else None)
                pool = ProjectRayWorkerPool((cpu,), (gpu,))
                with fixture._fake_ray_module(AbortRay()), patch.object(
                        staged_execution, "BoundedStageBroker", ObservedBroker):
                    with self.assertRaisesRegex(RuntimeError, "ambiguous submit"):
                        run(source(("a",)), pool, rows=("a",), active=1)
                self.assertLess(events.index("cpu_fence" if stage == "prepare" else "gpu_fence"),
                                events.index(stage + "_released"))
                self.assertTrue(brokers[0].is_drained())
                self.assertEqual(brokers[0].snapshot().ready_held_bytes, 0)

    def test_failed_confirmation_keeps_unknown_capacity_and_blocks_pool_reuse(self):
        events, brokers = [], []
        error = RuntimeError("ambiguous submit")
        pool = ProjectRayWorkerPool((FenceCpu(events, error=error,
            fence_error=RuntimeError("unreachable actor")),), (FenceGpu(events),))

        class ObservedBroker(BoundedStageBroker):
            def __init__(self, limits):
                super().__init__(limits)
                brokers.append(self)

        with fixture._fake_ray_module(AbortRay()), patch.object(
                staged_execution, "BoundedStageBroker", ObservedBroker):
            with self.assertRaisesRegex(RuntimeError, "ambiguous submit") as caught:
                run(source(("a",)), pool, rows=("a",), active=1)
            self.assertIs(caught.exception, error)
            self.assertEqual(brokers[0].snapshot().prepare_inflight, 1)
            self.assertEqual(brokers[0].snapshot().ready_held_bytes, 48)
            self.assertIs(caught.exception.hse_unconfirmed_work.broker, brokers[0])
            with self.assertRaisesRegex(RuntimeError, "unconfirmed work"):
                run(source(("b",)), pool, rows=("b",), active=1)

    def test_model_get_error_retains_prepared_ref_until_device_confirmation(self):
        error, events, payload_refs = RuntimeError("model transport unknown"), [], []

        class GetErrorRay(AbortRay):
            def get(self, ref, *, timeout=None):
                if isinstance(ref.value, ImageEmbeddingResult):
                    raise error
                return super().get(ref, timeout=timeout)

        class TrackedRef(fixture._Ref):
            def __init__(self, value):
                if isinstance(value, ImageEmbeddingBatch):
                    payload_refs.append(weakref.ref(self))
                super().__init__(value)

        pool = ProjectRayWorkerPool((FenceCpu(events),),
            (FenceGpu(events, fence_error=RuntimeError("device confirmation unavailable")),))
        with fixture._fake_ray_module(GetErrorRay()), patch.object(fixture, "_Ref", TrackedRef):
            with self.assertRaisesRegex(RuntimeError, "model transport unknown") as caught:
                run(source(("a",)), pool, rows=("a",), active=1)
        retained = caught.exception.hse_unconfirmed_work
        self.assertIs(caught.exception, error)
        self.assertEqual(retained.broker.snapshot().model_inflight, 1)
        self.assertEqual(retained.broker.snapshot().ready_held_bytes, 48)
        self.assertIs(retained.calls[0].payload_ref, payload_refs[0]())
        self.assertEqual(events, ["gpu_fence"])

    def test_partial_confirmation_settles_only_finished_actors_and_local_queue(self):
        error, events = RuntimeError("prepare transport unknown"), []

        class GetErrorRay(AbortRay):
            def get(self, ref, *, timeout=None):
                if timeout is None:
                    raise error
                return super().get(ref, timeout=timeout)

        pool = ProjectRayWorkerPool((FenceCpu(events, fence_ready=False), FenceCpu(events)),
                                    (FenceGpu(events),))
        with fixture._fake_ray_module(GetErrorRay()):
            with self.assertRaisesRegex(RuntimeError, "prepare transport unknown") as caught:
                run(source(("a", "b", "c", "d")), pool, rows=("a", "b", "c", "d"),
                    active=4, encoded=4, ready=4)
        snapshot = caught.exception.hse_unconfirmed_work.broker.snapshot()
        self.assertEqual((snapshot.encoded_queued, snapshot.prepare_inflight, snapshot.failed),
                         (0, 1, 3))
        self.assertEqual((snapshot.encoded_held_bytes, snapshot.ready_held_bytes), (1, 48))
        self.assertEqual(events, ["cpu_fence", "cpu_fence"])

    def test_wait_interrupt_preserves_original_error_and_confirms_all_active_work(self):
        error, events, brokers = KeyboardInterrupt("stop"), [], []

        class InterruptedRay(AbortRay):
            def wait(self, refs, *, num_returns, timeout=None):
                if timeout is None:
                    raise error
                return super().wait(refs, num_returns=num_returns, timeout=timeout)

        class ObservedBroker(BoundedStageBroker):
            def __init__(self, limits):
                super().__init__(limits)
                brokers.append(self)

        pool = ProjectRayWorkerPool((FenceCpu(events), FenceCpu(events)), (FenceGpu(events),))
        with fixture._fake_ray_module(InterruptedRay()), patch.object(
                staged_execution, "BoundedStageBroker", ObservedBroker):
            with self.assertRaises(KeyboardInterrupt) as caught:
                run(source(("a", "b")), pool)
        self.assertIs(caught.exception, error)
        self.assertEqual(events, ["cpu_fence", "cpu_fence"])
        self.assertTrue(brokers[0].is_drained())
        self.assertEqual(brokers[0].snapshot().ready_held_bytes, 0)

    def test_cleanup_submission_interrupt_keeps_original_error_and_unknown_owner(self):
        error, events = RuntimeError("original unknown submit"), []

        class InterruptFenceCpu(FenceCpu):
            def _fence(self):
                raise KeyboardInterrupt("cleanup interrupted")

        pool = ProjectRayWorkerPool((InterruptFenceCpu(events, error=error),), (FenceGpu(events),))
        with fixture._fake_ray_module(AbortRay()):
            with self.assertRaisesRegex(RuntimeError, "original unknown submit") as caught:
                run(source(("a",)), pool, rows=("a",), active=1)
            self.assertIs(caught.exception, error)
            self.assertEqual(caught.exception.hse_unconfirmed_work.broker.snapshot().prepare_inflight, 1)
            with self.assertRaisesRegex(RuntimeError, "unconfirmed work"):
                run(source(("b",)), pool, rows=("b",), active=1)

    def test_interrupt_after_prepare_transition_reconciles_stale_call_and_other_owner(self):
        error, events, brokers = KeyboardInterrupt("after prepare transition"), [], []

        class InterruptedBroker(BoundedStageBroker):
            def __init__(self, limits):
                super().__init__(limits)
                brokers.append(self)

            def complete_prepare(self, *args, **kwargs):
                super().complete_prepare(*args, **kwargs)
                raise error

        pool = ProjectRayWorkerPool((FenceCpu(events), FenceCpu(events, fence_ready=False)),
                                    (FenceGpu(events),))
        with fixture._fake_ray_module(AbortRay()), patch.object(
                staged_execution, "BoundedStageBroker", InterruptedBroker):
            with self.assertRaises(KeyboardInterrupt) as caught:
                run(source(("a", "b")), pool)
            self.assertIs(caught.exception, error)
            snapshot = brokers[0].snapshot()
            self.assertEqual((snapshot.prepare_inflight, snapshot.ready_queued, snapshot.failed),
                             (1, 0, 1))
            self.assertEqual(snapshot.ready_held_bytes, 48)
            with self.assertRaisesRegex(RuntimeError, "unconfirmed work"):
                run(source(("c",)), pool, rows=("c",), active=1)

    def test_cleanup_bookkeeping_error_keeps_source_error_and_pool_protection(self):
        error, events = ValueError("source failed"), []

        class FailedSource(fixture._Source):
            def to_arrow_iter(self, *, results_buffer_size):
                yield self._batches[0]
                raise error

        class InterruptedBroker(BoundedStageBroker):
            def cancel_queued(self, block_id):
                super().cancel_queued(block_id)
                raise KeyboardInterrupt("cleanup bookkeeping interrupted")

        pool = ProjectRayWorkerPool((FenceCpu(events),), (FenceGpu(events),))
        with fixture._fake_ray_module(AbortRay()), patch.object(
                staged_execution, "BoundedStageBroker", InterruptedBroker):
            with self.assertRaisesRegex(ValueError, "source failed") as caught:
                run(FailedSource(source(("a",))._batches), pool, rows=("a",))
            self.assertIs(caught.exception, error)
            self.assertTrue(caught.exception.hse_unconfirmed_work.broker.is_drained())
            with self.assertRaisesRegex(RuntimeError, "unconfirmed work"):
                run(source(("b",)), pool, rows=("b",), active=1)


if __name__ == "__main__":
    unittest.main()
