"""Composition with the separately delivered actor input-residency option.

Requires the residency slice (42186dbe). Torch and Ray execution remain local
substitutes; the actual cache/actor, HSE loop and broker are used unchanged.
"""

import importlib.util
import unittest
from unittest.mock import patch

from src.modalities.image import staged_execution
from src.modalities.image.clip import ClipTensorActor
from src.modalities.image.execution import ProjectRayWorkerPool
from src.scheduling.runtime.stage_broker import StageBrokerLimits
from tests.modalities.image import test_hse_device_dispatch as dispatch
from tests.modalities.image import test_hse_static_lifecycle as lifecycle
from tests.modalities.image import test_staged_image_execution as fixture


RESIDENCY_AVAILABLE = importlib.util.find_spec("src.modalities.image.input_residency") is not None


class ActorHandle:
    def __init__(self, actor):
        self.actor = actor
        self.embed = fixture._OptionsMethod(self._embed)
        self.synchronize = fixture._OptionsMethod(lambda: self._call(actor.synchronize))

    @staticmethod
    def _call(method, *args):
        try:
            return method(*args)
        except BaseException as error:
            return error  # Remote exceptions are observed through ray.get.

    def _embed(self, reference):
        return self._call(self.actor.embed, reference.value)


def run(pool, policy, groups):
    rows = frozenset(row for group in groups for row in group)
    source = fixture._Source([fixture._RecordBatch(
        doc_id=[fixture._Scalar(row) for row in group],
        image=[fixture._Scalar(b"x") for _row in group]) for group in groups])
    return staged_execution.run_project_ray_hse_pipeline(
        source, worker_pool=pool, expected_doc_ids=rows, batch_size=2,
        max_active_batches=4, encoded_block_bytes_upper_bound=2,
        limits=StageBrokerLimits(8, 384, 32, 2, 1 if len(groups) > 2 else 2),
        model_revision="fixed-model", processor_revision="fixed-processor",
        model_dtype="float32", input_size=2, embedding_dimension=3, model_dispatch=policy,
    )


@unittest.skipUnless(RESIDENCY_AVAILABLE, "requires the separately delivered input-residency slice")
class HSEResidencyDispatchTest(unittest.TestCase):
    def pool(self, budget):
        from src.modalities.image.input_residency import estimate
        from src.modalities.image.reusable_clip import ReusableClipTensorActor
        from tests.modalities.image.test_reusable_clip import _initialize

        with patch.object(ClipTensorActor, "__init__", _initialize):
            actors = tuple(ReusableClipTensorActor("fixed-model", input_cache_bytes=budget,
                                                   dtype="float32") for _index in range(2))
        pool = ProjectRayWorkerPool((lifecycle.FenceCpu([]), lifecycle.FenceCpu([])),
                                    tuple(ActorHandle(actor) for actor in actors))
        policy = dispatch.policy_for(pool)
        policy.placement_estimator = estimate  # The actual data-owner definition.
        return pool, policy, actors

    def test_off_on_reuses_only_input_and_keeps_every_full_forward_and_row(self):
        groups = (("a", "b"), ("c", "d"), ("a#pass=2", "b#pass=2"), ("c#pass=2", "d#pass=2"))
        results = []
        for budget in (0, 96):
            pool, policy, actors = self.pool(budget)
            fake_ray = lifecycle.AbortRay()
            with fixture._fake_ray_module(fake_ray):
                result = run(pool, policy, groups)
            self.assertTrue(result.audit["exactly_once"])
            self.assertEqual(sum(actor._model.calls for actor in actors), 4)
            self.assertEqual(sum(actor._torch.conversions for actor in actors), 4 if budget == 0 else 2)
            self.assertEqual(policy.snapshot()["sampled_actor_indices"], [0, 1])
            self.assertGreater(policy.snapshot()["locality_observations"]["unknown"], 0)
            self.assertNotIn("ImageEmbeddingBatch", fake_ray.get_types)
            for actor in actors:
                snapshot = actor.input_residency_snapshot()
                self.assertEqual(snapshot.held_references, 0)
                self.assertEqual(snapshot.unknown_references, 0)
                self.assertEqual(snapshot.resident_bytes, budget)
            results.append(result)
        self.assertEqual(results[0].audit, results[1].audit)

    def test_failed_device_fence_retains_both_input_owner_and_hse_unknown_work(self):
        pool, policy, actors = self.pool(96)
        actors[0]._model.failure = RuntimeError("simulated forward failure")
        actors[0]._torch.fail_fence = True
        with fixture._fake_ray_module(lifecycle.AbortRay()):
            with self.assertRaises(RuntimeError) as raised:
                run(pool, policy, (("a", "b"), ("c", "d")))
        retained = raised.exception.hse_unconfirmed_work
        self.assertEqual([call.actor_index for call in retained.calls], [0])
        self.assertEqual(retained.broker.snapshot().ready_held_bytes, 96)
        self.assertEqual(retained.broker.snapshot().ready_held_work, 8)
        first, second = [actor.input_residency_snapshot() for actor in actors]
        self.assertEqual((first.resident_bytes, first.held_references, first.unknown_references), (96, 1, 1))
        self.assertEqual((second.resident_bytes, second.held_references, second.unknown_references), (96, 0, 0))
        self.assertEqual(policy.snapshot()["sampled_actor_indices"], [])


if __name__ == "__main__":
    unittest.main()
