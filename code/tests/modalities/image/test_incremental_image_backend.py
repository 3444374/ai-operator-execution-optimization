from __future__ import annotations

import unittest
from dataclasses import replace
from types import SimpleNamespace

import numpy as np

from src.execution_provider.semantic_image import SemanticImagePlan
from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingResult
from src.modalities.image.incremental_backend import RayImageBackend
from src.modalities.image.staged import build_prepared_image_block_descriptor
from src.scheduling.core.session_contract import (
    Acceptance, BackendTask, OfferedTask, SessionSpec, TaskKey, Terminal, Uncertain,
)
from src.scheduling.runtime.stage_broker import StageBrokerLimits


PLAN = SemanticImagePlan("fixture", "a" * 40, "fixture", "b" * 40, "float32", 2, 2)


class Ref:
    def __init__(self, value, ready=True):
        self.value, self.ready = value, ready


class FakeRay:
    def __init__(self):
        self.cancelled = []
        self.gets = []

    def wait(self, refs, *, num_returns, timeout):
        assert timeout == 0
        ready = [ref for ref in refs if ref.ready][:num_returns]
        return ready, [ref for ref in refs if ref not in ready]

    def get(self, ref, *, timeout):
        assert timeout == 0 and ref.ready
        self.gets.append(ref)
        if isinstance(ref.value, Exception):
            raise ref.value
        return ref.value

    def cancel(self, ref, *, force, recursive):
        assert not force and not recursive
        self.cancelled.append(ref)


class Method:
    def __init__(self, callback):
        self.callback = callback

    def options(self, *, num_returns):
        assert num_returns == 2
        return self

    def remote(self, *args):
        return self.callback(*args)


class Actors:
    def __init__(self):
        self.prepares, self.models, self.barriers = [], [], []
        self.prepare_ready = self.model_ready = self.barrier_ready = True
        self.bad_shape = False
        self.bad_result = False
        self.cpu = SimpleNamespace(preprocess_staged=Method(self.prepare), ready=Method(self.barrier))
        self.gpu = SimpleNamespace(embed=Method(self.embed), ready=Method(self.barrier))
        self.pool = SimpleNamespace(preprocessors=(self.cpu,), gpu_actors=(self.gpu,))

    def prepare(self, descriptor, encoded):
        pixels = np.zeros((1, 3, 2, 2), dtype=np.float32)
        prepared = build_prepared_image_block_descriptor(
            descriptor, pixels, ready_at_s=descriptor.created_at_s)
        if self.bad_shape:
            prepared = replace(prepared, shape=(1, 3, 1, 4))
        refs = Ref(prepared, self.prepare_ready), Ref(descriptor)
        self.prepares.append(refs)
        return refs

    def embed(self, payload):
        result = ImageEmbeddingResult(
            doc_ids=payload.value.row_ids,
            embeddings=np.array([[1, 0]], dtype=np.float32),
            semantics=EmbeddingSemantics("a" * 40, "b" * 40, 2), service_s=0,
        )
        if self.bad_result:
            result.embeddings[0, 0] = np.nan
        ref = Ref(result, self.model_ready)
        self.models.append(ref)
        return ref

    def barrier(self):
        ref = Ref({}, self.barrier_ready)
        self.barriers.append(ref)
        return ref


class IncrementalImageTest(unittest.TestCase):
    def setUp(self):
        self.actors, self.ray = Actors(), FakeRay()
        self.events = []
        self.backend = RayImageBackend(
            plan=PLAN, worker_pool=self.actors.pool, max_tasks=2,
            limits=StageBrokerLimits(32, 96, 8, 1, 1), ray_api=self.ray,
            observer=lambda event, key, snapshot: self.events.append((event, key, snapshot)),
        )
        self.handles = {}

    def offer(self, sequence=0, session=1, payload=b"same"):
        task = BackendTask(TaskKey(session, sequence),
            SessionSpec(f"job-{session}", "images", PLAN.digest, "ai_embed", "pixels"),
            OfferedTask(sequence, payload, 4, 8))
        submission = self.backend.try_submit(task, "image")
        if submission.acceptance is Acceptance.ACCEPTED:
            self.handles[task.key] = submission.handle
        return task.key, submission

    def poll(self):
        events = self.backend.poll(tuple(self.handles.items()), 2)
        for event in events:
            if type(event) is Terminal:
                del self.handles[event.key]
        return events

    def drain(self):
        results = []
        for _ in range(20):
            results.extend(self.poll())
            if not self.handles:
                break
        self.assertFalse(self.handles)
        return results

    def test_duplicate_inputs_remain_distinct_and_prepared_tensor_stays_remote(self):
        a, _ = self.offer(session=1)
        b, _ = self.offer(session=2)
        self.assertEqual(self.offer(1)[1].acceptance, Acceptance.NOT_ACCEPTED)
        results = self.drain()
        self.assertEqual({item.key for item in results}, {a, b})
        self.assertTrue(all(item.result == PLAN.encode_result([1, 0]) for item in results))
        self.assertTrue(all(payload not in self.ray.gets for _, payload in self.actors.prepares))
        self.assertEqual(self.backend.snapshot().active_blocks, 0)

    def test_prepare_cancel_retains_reservation_until_remote_finishes(self):
        self.actors.prepare_ready = False
        key, submission = self.offer()
        self.poll()
        self.backend.request_cancel(key, submission.handle)
        self.assertEqual(len(self.ray.cancelled), 1)
        self.assertEqual(self.backend.snapshot().prepare_inflight, 1)
        self.assertEqual(self.backend.snapshot().ready_held_bytes, PLAN.prepared_bytes)
        self.assertEqual(self.poll(), ())
        self.actors.prepares[0][0].ready = True
        self.assertEqual(self.drain()[0].status, "cancelled")
        self.assertEqual(self.actors.models, [])

    def test_model_cancel_does_not_destroy_shared_actor_or_other_query(self):
        self.actors.model_ready = False
        key, submission = self.offer()
        self.poll()
        self.poll()
        self.backend.request_cancel(key, submission.handle)
        other, _ = self.offer(session=2)
        self.poll()
        self.assertEqual(self.backend.snapshot().model_inflight, 1)
        self.actors.model_ready = True
        self.actors.models[0].ready = True
        results = {item.key: item for item in self.drain()}
        self.assertEqual(results[key].status, "cancelled")
        self.assertEqual(results[other].status, "completed")

    def test_error_requires_actor_completion_confirmation(self):
        self.actors.barrier_ready = False
        self.offer()
        self.poll()
        self.actors.prepares[0][0].value = RuntimeError("transport failure")
        events = self.poll()
        self.assertIsInstance(events[0], Uncertain)
        self.assertEqual(self.backend.snapshot().prepare_inflight, 1)
        self.assertEqual(self.poll(), ())
        self.assertFalse(self.backend.close())
        self.actors.barriers[0].ready = True
        self.assertEqual(self.drain()[0].status, "cancelled")
        self.assertTrue(self.backend.close())

    def test_unreachable_actor_remains_unknown(self):
        self.offer()
        self.poll()
        self.actors.prepares[0][0].value = RuntimeError("lost")
        self.poll()
        self.actors.barriers[0].value = RuntimeError("actor unreachable")
        self.assertEqual(self.poll(), ())
        self.assertEqual(self.backend.snapshot().prepare_inflight, 1)
        self.assertFalse(self.backend.close())

    def test_invalid_prepared_shape_and_nonfinite_result_fail_without_new_stage(self):
        self.actors.bad_shape = True
        self.offer()
        self.assertEqual(self.drain()[0].status, "failed")
        self.assertEqual(self.actors.models, [])
        self.actors.bad_shape = False
        self.actors.bad_result = True
        self.offer(1)
        self.assertEqual(self.drain()[0].status, "failed")
        self.assertEqual(self.backend.snapshot().ready_held_bytes, 0)

    def test_cumulative_rows_do_not_accumulate_broker_history(self):
        for sequence in range(100):
            self.offer(sequence)
            self.drain()
        self.assertEqual(len(self.backend._broker._states), 0)
        self.assertEqual(len(self.backend._broker._admitted_rows), 0)
        self.assertEqual(len(self.backend._broker._completed_rows), 0)

    def test_queued_cancel_and_ready_cancel_release_only_local_work(self):
        key, submission = self.offer()
        self.backend.request_cancel(key, submission.handle)
        self.assertEqual(self.drain()[0].status, "cancelled")
        self.assertEqual(self.ray.cancelled, [])
        self.assertEqual(self.actors.prepares, [])


if __name__ == "__main__":
    unittest.main()
