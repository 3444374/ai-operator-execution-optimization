"""Ray ownership boundary for a shared fair-work credit coordinator."""

from __future__ import annotations

from ..submission_control.shared_credit import FairEndpointCreditCoordinator


class _FairCreditActor:
    def __init__(
        self,
        capacities: dict[str, tuple[int, int]],
        quantum: int,
        policy: str,
    ) -> None:
        self.capacities = capacities
        self.quantum = quantum
        self.policy = policy
        self.coordinator = FairEndpointCreditCoordinator(
            capacities,
            quantum=quantum,
            policy=policy,
        )

    def configuration(self) -> tuple[dict[str, tuple[int, int]], int, str]:
        return self.capacities, self.quantum, self.policy

    def try_acquire(self, **kwargs) -> bool:
        return self.coordinator.try_acquire(**kwargs)

    def release(self, request_id: str, *, job_id: str) -> None:
        self.coordinator.release(request_id, job_id=job_id)

    def snapshot(self, endpoint_id: str):
        return self.coordinator.snapshot(endpoint_id)


class RaySharedCreditClient:
    def __init__(self, ray_module, actor) -> None:
        self.ray_module = ray_module
        self.actor = actor

    def try_acquire(self, **kwargs) -> bool:
        return bool(
            self.ray_module.get(
                self.actor.try_acquire.remote(**kwargs)
            )
        )

    def release(self, request_id: str, *, job_id: str) -> None:
        self.ray_module.get(
            self.actor.release.remote(request_id, job_id=job_id)
        )

    def snapshot(self, endpoint_id: str):
        return self.ray_module.get(
            self.actor.snapshot.remote(endpoint_id)
        )


def get_or_create_shared_credit_client(
    ray_module,
    *,
    name: str,
    namespace: str,
    capacities: dict[str, tuple[int, int]],
    quantum: int,
    policy: str = "drr",
) -> RaySharedCreditClient:
    if not name:
        raise ValueError("shared credit actor name must be non-empty")
    if not namespace:
        raise ValueError("shared credit namespace must be non-empty")
    remote_actor = ray_module.remote(_FairCreditActor)
    actor = remote_actor.options(
        name=name,
        namespace=namespace,
        lifetime="detached",
        get_if_exists=True,
        num_cpus=0,
    ).remote(capacities, quantum, policy)
    configured_capacities, configured_quantum, configured_policy = ray_module.get(
        actor.configuration.remote()
    )
    if (
        configured_capacities != capacities
        or configured_quantum != quantum
        or configured_policy != policy
    ):
        raise ValueError(
            "existing shared credit actor configuration does not match "
            "this job"
        )
    return RaySharedCreditClient(ray_module, actor)
