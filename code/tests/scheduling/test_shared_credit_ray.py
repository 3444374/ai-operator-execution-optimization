from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.runtime.shared_credit_ray import (  # noqa: E402
    get_or_create_shared_credit_client,
)
from src.scheduling.submission_control.saor import (  # noqa: E402
    SaorReleaseConfig,
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
    def test_bounded_saor_events_cross_actor_boundary_once_in_order(self) -> None:
        ray = _FakeRay()
        client = get_or_create_shared_credit_client(
            ray,
            name="bounded-credits",
            namespace="tests",
            capacities={"gpu0": (2, 200)},
            quantum=100,
            policy="saor_bounded_priority",
            saor_release_config=SaorReleaseConfig(1.0, 1.0, 1.0, 0.0),
        )

        for request_id in ("batch-0", "batch-1"):
            self.assertTrue(
                client.try_acquire(
                    request_id=request_id,
                    job_id="bulk",
                    endpoint_id="gpu0",
                    estimated_work=100,
                    fairness_debt_cap=100.0,
                )
            )

        events = client.drain_release_events("gpu0")
        self.assertEqual([event.event_seq for event in events], [1, 2])
        self.assertEqual(client.drain_release_events("gpu0"), ())

    def test_previous_non_saor_actor_configuration_is_compatible(self) -> None:
        class PreviousActor:
            def configuration(self):
                return {"gpu0": (2, 200)}, 100, "drr"

        ray = _FakeRay()
        ray.actors[("tests", "credits")] = _ActorHandle(PreviousActor())

        client = get_or_create_shared_credit_client(
            ray,
            name="credits",
            namespace="tests",
            capacities={"gpu0": (2, 200)},
            quantum=100,
        )

        self.assertIsNotNone(client)

    def test_runtime_capacity_update_preserves_creation_identity(self) -> None:
        ray = _FakeRay()
        initial = {"gpu0": (2, 200)}
        first = get_or_create_shared_credit_client(
            ray,
            name="credits",
            namespace="tests",
            capacities=initial,
            quantum=100,
        )

        updated = first.update_capacity(
            "gpu0",
            request_limit=3,
            work_limit=300,
        )
        second = get_or_create_shared_credit_client(
            ray,
            name="credits",
            namespace="tests",
            capacities=initial,
            quantum=100,
        )

        self.assertEqual(updated.request_limit, 3)
        self.assertEqual(second.snapshot("gpu0").work_limit, 300)

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

    def test_existing_actor_rejects_mismatched_policy(self) -> None:
        ray = _FakeRay()
        get_or_create_shared_credit_client(
            ray,
            name="credits",
            namespace="tests",
            capacities={"gpu0": (2, 200)},
            quantum=100,
            policy="fifo",
        )

        with self.assertRaisesRegex(ValueError, "does not match"):
            get_or_create_shared_credit_client(
                ray,
                name="credits",
                namespace="tests",
                capacities={"gpu0": (2, 200)},
                quantum=100,
                policy="drr",
            )


if __name__ == "__main__":
    unittest.main()
