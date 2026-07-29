from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.runtime.shared_credit_ray import (  # noqa: E402
    get_or_create_shared_credit_client,
)


class _RemoteMethod:
    def __init__(self, function):
        self._function = function

    def remote(self, *args, **kwargs):
        return self._function(*args, **kwargs)


class _ActorHandle:
    def __init__(self, instance):
        self._instance = instance

    def __getattr__(self, name):
        return _RemoteMethod(getattr(self._instance, name))


class _RemoteActorBuilder:
    def __init__(self, ray_module, actor_class):
        self._ray_module = ray_module
        self._actor_class = actor_class
        self._options = {}

    def options(self, **options):
        self._options = options
        return self

    def remote(self, *args):
        key = (self._options["namespace"], self._options["name"])
        if key not in self._ray_module.actors:
            self._ray_module.actors[key] = _ActorHandle(
                self._actor_class(*args)
            )
        return self._ray_module.actors[key]


class _FakeRay:
    def __init__(self):
        self.actors = {}

    def remote(self, actor_class):
        return _RemoteActorBuilder(self, actor_class)

    @staticmethod
    def get(value):
        return value


class SharedCreditRayTests(unittest.TestCase):
    def test_named_actor_is_reused_and_tracks_jobs_independently(self) -> None:
        ray = _FakeRay()
        capacities = {"gpu0": (2, 200)}
        first = get_or_create_shared_credit_client(
            ray,
            name="credits",
            namespace="tests",
            capacities=capacities,
            quantum=100,
        )
        second = get_or_create_shared_credit_client(
            ray,
            name="credits",
            namespace="tests",
            capacities=capacities,
            quantum=100,
        )

        for client, job_id in ((first, "a"), (second, "b")):
            self.assertTrue(
                client.try_acquire(
                    request_id="batch-0",
                    job_id=job_id,
                    endpoint_id="gpu0",
                    estimated_work=100,
                )
            )
        first.release("batch-0", job_id="a")
        self.assertEqual(len(ray.actors), 1)
        snapshot = second.snapshot("gpu0")
        self.assertEqual(snapshot.active_by_job, (("b", 1),))
        self.assertEqual(
            snapshot.granted_requests_by_job,
            (("a", 1), ("b", 1)),
        )

    def test_existing_actor_rejects_mismatched_capacity(self) -> None:
        ray = _FakeRay()
        get_or_create_shared_credit_client(
            ray,
            name="credits",
            namespace="tests",
            capacities={"gpu0": (2, 200)},
            quantum=100,
        )

        with self.assertRaisesRegex(ValueError, "does not match"):
            get_or_create_shared_credit_client(
                ray,
                name="credits",
                namespace="tests",
                capacities={"gpu0": (3, 300)},
                quantum=100,
            )


if __name__ == "__main__":
    unittest.main()
