"""HSE policy integration with real broker ownership and local actor substitutes."""

import json
import sys
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

from src.modalities.image import staged_execution
from src.modalities.image.execution import ProjectRayWorkerPool
from src.modalities.image.hse_dispatch import bind_hse_dispatcher, observe_ray_stage_destination
from src.scheduling.runtime.stage_broker import BoundedStageBroker, StageBrokerLimits
from src.scheduling.runtime.stage_dispatch import (
    StageActorCapacity, StageCapacity, StageDestination, StageExecutionGroupCapacity, StageModelDispatcher,
)
from tests.modalities.image import test_hse_static_lifecycle as lifecycle
from tests.modalities.image import test_staged_image_execution as fixture


class Clock:
    def __init__(self):
        self.now = 10.0
        self.sleeps = []

    def perf_counter(self):
        return self.now

    def sleep(self, delay):
        self.sleeps.append(delay)
        self.now += delay


class TimedRay(lifecycle.AbortRay):
    def __init__(self, clock):
        super().__init__()
        self.clock, self.timeouts = clock, []

    def wait(self, refs, *, num_returns, timeout=None):
        def ready():
            return [ref for ref in refs if getattr(ref, "ready", True)
                    and getattr(ref, "available_at_s", 0) <= self.clock.now][:num_returns]

        selected = ready()
        if not selected:
            if timeout is not None:
                self.timeouts.append(timeout)
                self.clock.now += timeout
            else:
                self.clock.now = min(ref.available_at_s for ref in refs if getattr(ref, "ready", True))
            selected = ready()
        return selected, [ref for ref in refs if ref not in selected]


class TimedMethod(fixture._OptionsMethod):
    def __init__(self, callback, clock, delay):
        super().__init__(callback)
        self.clock, self.delay = clock, delay

    def remote(self, *args):
        ref = super().remote(*args)
        ref.available_at_s = self.clock.now + self.delay
        return ref


class RecordingGpu(lifecycle.FenceGpu):
    def __init__(self, index, calls, *, service_s=0.02, clock=None, delay=0,
                 get_error=None, fence_ready=True, bad_output=False):
        super().__init__([], fence_ready=fence_ready)
        self.index, self.calls, self.service_s = index, calls, service_s
        self.clock, self.get_error, self.bad_output = clock, get_error, bad_output
        if clock is not None:
            self.embed = TimedMethod(self._embed, clock, delay)

    def _embed(self, batch_ref):
        batch = batch_ref.value if isinstance(batch_ref, fixture._Ref) else batch_ref
        self.calls.append((self.index, batch.doc_ids, self.clock.now if self.clock else None))
        if self.get_error is not None:
            return self.get_error
        result = replace(super()._embed(batch_ref), service_s=self.service_s)
        if self.bad_output:
            result.embeddings[0, 0] = float("nan")
        return result


def policy_for(pool, *, actor_work=8, shared=False, feedback_ttl_s=5):
    actors = tuple(StageActorCapacity(index, StageDestination(
        f"actor-{index}", "shared" if shared else f"group-{index}"), StageCapacity(1, actor_work))
        for index in range(len(pool.gpu_actors)))
    groups = ((StageExecutionGroupCapacity("shared", StageCapacity(len(actors), actor_work * len(actors))),)
              if shared else tuple(StageExecutionGroupCapacity(f"group-{index}", StageCapacity(1, actor_work))
                                   for index in range(len(actors))))
    return StageModelDispatcher(actors, groups, feedback_ttl_s=feedback_ttl_s)


def run(pool, batches, *, policy=None, model_slots=None):
    rows = frozenset(str(item.as_py()) for batch in batches for item in batch["doc_id"])
    return staged_execution.run_project_ray_hse_pipeline(
        fixture._Source(batches), worker_pool=pool, expected_doc_ids=rows,
        batch_size=2, max_active_batches=4, encoded_block_bytes_upper_bound=2,
        limits=StageBrokerLimits(8, 4 * 2 * 48, 4 * 2 * 4, len(pool.preprocessors),
                                len(pool.gpu_actors) if model_slots is None else model_slots),
        model_revision="model", processor_revision="processor", model_dtype="float16",
        input_size=2, embedding_dimension=2, model_dispatch=policy,
    )


def batches(*rows):
    return [fixture._RecordBatch(doc_id=[fixture._Scalar(row) for row in group],
                                 image=[fixture._Scalar(row.encode()) for row in group])
            for group in rows]


class HSEDeviceDispatchTest(unittest.TestCase):
    def test_default_keeps_static_minfree_and_payload_dependency(self):
        calls = []
        pool = ProjectRayWorkerPool((fixture._CpuActor(),),
            (RecordingGpu(0, calls), RecordingGpu(1, calls)))
        fake_ray = fixture._FakeRay()
        with fixture._fake_ray_module(fake_ray), patch.object(StageModelDispatcher, "choose",
                side_effect=AssertionError("static path invoked optional policy")):
            result = run(pool, batches(("a",), ("b",), ("c",)), model_slots=1)
        self.assertEqual([item[0] for item in calls], [0, 0, 0])
        self.assertEqual(result.execution_mode, "hse_static")
        self.assertNotIn("model_dispatch", json.loads(result.engine_stats))
        self.assertNotIn("ImageEmbeddingBatch", fake_ray.get_types)

    def test_one_slot_bootstraps_both_actors_then_routes_full_and_tail_batches(self):
        calls = []
        pool = ProjectRayWorkerPool((fixture._CpuActor(),),
            (RecordingGpu(0, calls, service_s=0.04), RecordingGpu(1, calls, service_s=0.02)))
        policy = policy_for(pool)
        fake_ray = fixture._FakeRay()
        with fixture._fake_ray_module(fake_ray):
            result = run(pool, batches(("a", "b"), ("c", "d"), ("e",)), policy=policy, model_slots=1)
        self.assertEqual([item[:2] for item in calls], [(0, ("a", "b")), (1, ("c", "d")), (1, ("e",))])
        self.assertTrue(result.audit["exactly_once"])
        self.assertEqual(result.model_inflight_peak, 1)
        self.assertEqual(result.execution_mode, "hse_device_dispatch")
        self.assertEqual(json.loads(result.engine_stats)["execution_mode"], result.execution_mode)
        self.assertEqual(policy.snapshot()["sampled_actor_indices"], [0, 1])
        self.assertNotIn("ImageEmbeddingBatch", fake_ray.get_types)

    def test_zero_service_samples_do_not_starve_other_actor(self):
        calls = []
        pool = ProjectRayWorkerPool((fixture._CpuActor(),),
            (RecordingGpu(0, calls, service_s=0), RecordingGpu(1, calls, service_s=0)))
        policy = policy_for(pool)
        with fixture._fake_ray_module(fixture._FakeRay()):
            result = run(pool, batches(("a",), ("b",), ("c",), ("d",)), policy=policy, model_slots=1)
        self.assertEqual([item[0] for item in calls], [0, 1, 0, 1])
        self.assertTrue(result.audit["exactly_once"])
        self.assertEqual(policy.snapshot()["sampled_actor_indices"], [])

    def test_one_bad_service_sample_keeps_both_actors_in_fallback_rotation(self):
        calls = []
        pool = ProjectRayWorkerPool((fixture._CpuActor(),),
            (RecordingGpu(0, calls, service_s=0), RecordingGpu(1, calls, service_s=0.02)))
        policy = policy_for(pool)
        with fixture._fake_ray_module(fixture._FakeRay()):
            result = run(pool, batches(("a",), ("b",), ("c",), ("d",)), policy=policy, model_slots=1)
        self.assertEqual([item[0] for item in calls], [0, 1, 0, 1])
        self.assertTrue(result.audit["exactly_once"])
        self.assertEqual(policy.snapshot()["sampled_actor_indices"], [1])

    def test_capacity_expiry_recovers_with_no_pending_rpc_without_extra_lease(self):
        clock, calls, observed_brokers = Clock(), [], []
        pool = ProjectRayWorkerPool((fixture._CpuActor(),), (RecordingGpu(0, calls, clock=clock),))
        policy = policy_for(pool)
        policy.observe_actor_capacity(0, StageCapacity(0, 0), observed_at_s=clock.now)

        class ObservedBroker(BoundedStageBroker):
            def __init__(self, limits):
                super().__init__(limits)
                observed_brokers.append(self)

            def peek_model(self, **kwargs):
                candidate = super().peek_model(**kwargs)
                if candidate is not None and clock.now < 15:
                    assert candidate.retry_count == 0
                    assert self.snapshot().model_inflight == 0
                return candidate

        with fixture._fake_ray_module(TimedRay(clock)), patch.object(staged_execution, "time", clock), \
                patch.object(staged_execution, "BoundedStageBroker", ObservedBroker):
            result = run(pool, batches(("a",)), policy=policy)
        self.assertEqual(calls[0][2], 15)
        self.assertEqual(clock.sleeps, [1] * 5)
        self.assertTrue(result.audit["exactly_once"])
        self.assertEqual(observed_brokers[0].snapshot().completed, 1)

    def test_capacity_expiry_wakes_while_other_actor_has_long_pending_rpc(self):
        clock, calls = Clock(), []
        pool = ProjectRayWorkerPool((fixture._CpuActor(), fixture._CpuActor()),
            (RecordingGpu(0, calls, clock=clock, delay=100), RecordingGpu(1, calls, clock=clock, delay=1)))
        policy = policy_for(pool)
        policy.observe_actor_capacity(1, StageCapacity(0, 0), observed_at_s=clock.now)
        fake_ray = TimedRay(clock)
        with fixture._fake_ray_module(fake_ray), patch.object(staged_execution, "time", clock):
            result = run(pool, batches(("a",), ("b",)), policy=policy)
        self.assertEqual(calls, [(0, ("a",), 10), (1, ("b",), 15)])
        self.assertEqual(fake_ray.timeouts, [1] * 5)
        self.assertEqual(result.model_inflight_peak, 2)
        self.assertTrue(result.audit["exactly_once"])

    def test_group_capacity_serializes_distinct_actors_from_actual_live_calls(self):
        calls = []
        pool = ProjectRayWorkerPool((fixture._CpuActor(), fixture._CpuActor()),
            (RecordingGpu(0, calls), RecordingGpu(1, calls)))
        original = policy_for(pool, shared=True)
        policy = StageModelDispatcher(original.actors,
            (StageExecutionGroupCapacity("shared", StageCapacity(1, 4)),))
        with fixture._fake_ray_module(fixture._FakeRay()):
            result = run(pool, batches(("a",), ("b",)), policy=policy)
        self.assertEqual(result.model_inflight_peak, 1)
        self.assertEqual([item[0] for item in calls], [0, 1])

    def test_large_fifo_head_waits_for_large_actor_before_tail_uses_small_actor(self):
        clock, calls = Clock(), []
        pool = ProjectRayWorkerPool((fixture._CpuActor(), fixture._CpuActor()),
            (RecordingGpu(0, calls, clock=clock, delay=100), RecordingGpu(1, calls, clock=clock, delay=1)))
        initial = policy_for(pool)
        policy = StageModelDispatcher((initial.actors[0], replace(initial.actors[1], capacity=StageCapacity(1, 4))),
            (initial.groups[0], replace(initial.groups[1], capacity=StageCapacity(1, 4))))
        with fixture._fake_ray_module(TimedRay(clock)), patch.object(staged_execution, "time", clock):
            result = run(pool, batches(("a", "b"), ("c", "d"), ("e",)), policy=policy)
        self.assertEqual(calls, [(0, ("a", "b"), 10), (0, ("c", "d"), 110), (1, ("e",), 110)])
        self.assertTrue(result.audit["exactly_once"])

    def test_cancel_interrupt_keeps_unknown_owner_and_does_not_train_fences(self):
        pool = ProjectRayWorkerPool((lifecycle.FenceCpu([]), lifecycle.FenceCpu([])),
            (RecordingGpu(0, [], fence_ready=False), RecordingGpu(1, [])))
        policy = policy_for(pool)

        class InterruptRay(lifecycle.AbortRay):
            def wait(self, refs, *, num_returns, timeout=None):
                if timeout is None and all(isinstance(ref.value, fixture.ImageEmbeddingResult) for ref in refs):
                    raise KeyboardInterrupt("simulated cancel")
                return super().wait(refs, num_returns=num_returns, timeout=timeout)

        with fixture._fake_ray_module(InterruptRay()):
            with self.assertRaisesRegex(KeyboardInterrupt, "simulated cancel") as raised:
                run(pool, batches(("a",), ("b",)), policy=policy)
        retained = raised.exception.hse_unconfirmed_work
        self.assertEqual([call.actor_index for call in retained.calls], [0])
        self.assertEqual(retained.broker.snapshot().ready_held_work, 4)
        self.assertEqual(policy.snapshot()["sampled_actor_indices"], [])

    def test_failed_two_owner_confirmation_retains_only_unknown_actor_and_payload(self):
        calls = []
        error = RuntimeError("simulated model OOM")
        pool = ProjectRayWorkerPool((lifecycle.FenceCpu([]), lifecycle.FenceCpu([])),
            (RecordingGpu(0, calls, get_error=error, fence_ready=False), RecordingGpu(1, calls)))
        policy = policy_for(pool, shared=True)
        with fixture._fake_ray_module(lifecycle.AbortRay()):
            with self.assertRaisesRegex(RuntimeError, "simulated model OOM") as raised:
                run(pool, batches(("a",), ("b",)), policy=policy)
            retained = raised.exception.hse_unconfirmed_work
            self.assertEqual([call.actor_index for call in retained.calls], [0])
            self.assertIsNotNone(retained.calls[0].payload_ref)
            self.assertEqual(retained.broker.snapshot().ready_held_bytes, 48)
            self.assertEqual(retained.broker.snapshot().ready_held_work, 4)
            self.assertEqual(retained.broker.snapshot().model_inflight, 1)
            self.assertEqual(policy.snapshot()["sampled_actor_indices"], [])
            with self.assertRaisesRegex(RuntimeError, "unconfirmed work"):
                run(pool, batches(("a",)), policy=policy)

    def test_feedback_cleanup_interrupt_keeps_original_error_and_owner_confirmation(self):
        error = RuntimeError("original model failure")
        pool = ProjectRayWorkerPool((lifecycle.FenceCpu([]), lifecycle.FenceCpu([])),
            (RecordingGpu(0, [], get_error=error, fence_ready=False), RecordingGpu(1, [])))
        policy = policy_for(pool)
        with fixture._fake_ray_module(lifecycle.AbortRay()), \
                patch.object(policy, "invalidate_actor", side_effect=KeyboardInterrupt("feedback interrupted")):
            with self.assertRaisesRegex(RuntimeError, "original model failure") as raised:
                run(pool, batches(("a",), ("b",)), policy=policy)
        self.assertIs(raised.exception, error)
        retained = raised.exception.hse_unconfirmed_work
        self.assertEqual([call.actor_index for call in retained.calls], [0])
        self.assertEqual(retained.broker.snapshot().ready_held_work, 4)
        self.assertIsNotNone(retained.calls[0].payload_ref)
        self.assertIn(pool, staged_execution._unconfirmed_runs)
        self.assertTrue(any("feedback cleanup raised KeyboardInterrupt" in note
                            for note in raised.exception.__notes__))

    def test_invalid_completed_output_does_not_train_service_rate(self):
        pool = ProjectRayWorkerPool((fixture._CpuActor(),), (RecordingGpu(0, [], bad_output=True),))
        policy = policy_for(pool)
        with fixture._fake_ray_module(lifecycle.AbortRay()):
            with self.assertRaisesRegex(ValueError, "non-finite"):
                run(pool, batches(("a",)), policy=policy)
        self.assertEqual(policy.snapshot()["sampled_actor_indices"], [])

    def test_permanent_actor_work_overflow_fails_ready_without_model_or_retry(self):
        calls = []
        pool = ProjectRayWorkerPool((lifecycle.FenceCpu([]),), (RecordingGpu(0, calls),))
        policy = policy_for(pool, actor_work=3)
        with fixture._fake_ray_module(lifecycle.AbortRay()):
            with self.assertRaisesRegex(BufferError, "every declared"):
                run(pool, batches(("a",)), policy=policy)
        self.assertEqual(calls, [])
        self.assertNotIn(pool, staged_execution._unconfirmed_runs)

    def test_pool_mapping_reuse_is_rejected_and_new_run_clears_warmup_rates(self):
        first_pool = ProjectRayWorkerPool((fixture._CpuActor(),), (RecordingGpu(0, []),))
        policy = policy_for(first_pool)
        bind_hse_dispatcher(policy, first_pool)
        second_pool = ProjectRayWorkerPool((fixture._CpuActor(),), (RecordingGpu(0, []),))
        with self.assertRaisesRegex(ValueError, "another pool"):
            bind_hse_dispatcher(policy, second_pool)
        calls = []
        pool = ProjectRayWorkerPool((fixture._CpuActor(),),
            (RecordingGpu(0, calls), RecordingGpu(1, calls)))
        policy = policy_for(pool)
        from tests.scheduling.test_stage_broker import _descriptor
        policy.observe_completion(0, _descriptor(0), service_s=100, now_s=1)
        policy.observe_completion(1, _descriptor(0), service_s=0.01, now_s=1)
        with fixture._fake_ray_module(fixture._FakeRay()):
            run(pool, batches(("a",)), policy=policy)
        self.assertEqual(calls[0][0], 0)
        self.assertEqual(policy.snapshot()["sampled_actor_indices"], [0])

    def test_data_owner_locality_callback_receives_metadata_only(self):
        pool = ProjectRayWorkerPool((fixture._CpuActor(),), (RecordingGpu(0, []),))
        policy = policy_for(pool)
        seen = []

        def estimate(descriptor, destination, now_s):
            seen.append((descriptor.representation, destination.endpoint_id, now_s))
            return None  # Exactly the current input_residency callback result.

        policy.placement_estimator = estimate

        fake_ray = fixture._FakeRay()
        with fixture._fake_ray_module(fake_ray):
            result = run(pool, batches(("a",)), policy=policy)
        self.assertEqual(seen[0][:2], ("prepared_fp32_nchw", "actor-0"))
        self.assertEqual(policy.snapshot()["locality_observations"], {"unknown": 1})
        self.assertTrue(result.audit["exactly_once"])
        self.assertNotIn("ImageEmbeddingBatch", fake_ray.get_types)

    def test_runtime_identity_helper_uses_actor_context_and_never_invents_gpu_identity(self):
        context = SimpleNamespace(get_actor_id=lambda: "runtime-actor", get_node_id=lambda: "node",
                                  get_accelerator_ids=lambda: {"GPU": ["0"]})
        with patch.dict(sys.modules, {"ray": SimpleNamespace(get_runtime_context=lambda: context)}):
            destination = observe_ray_stage_destination(endpoint_id="endpoint", execution_group_id="declared-group")
        self.assertEqual(destination.runtime_actor_id, "runtime-actor")
        self.assertEqual(destination.execution_group_id, "declared-group")
        self.assertEqual(destination.accelerator_ids, ("0",))
        context.get_actor_id = lambda: None
        with patch.dict(sys.modules, {"ray": SimpleNamespace(get_runtime_context=lambda: context)}):
            with self.assertRaisesRegex(RuntimeError, "inside its actor"):
                observe_ray_stage_destination(endpoint_id="endpoint", execution_group_id="group")


if __name__ == "__main__":
    unittest.main()
