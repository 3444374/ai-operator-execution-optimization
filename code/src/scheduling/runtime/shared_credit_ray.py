"""Ray ownership boundary for a shared fair-work credit coordinator."""

from __future__ import annotations

from ..submission_control.shared_credit import FairEndpointCreditCoordinator
from ..submission_control.saor import SaorReleaseConfig


class _FairCreditActor:
    def __init__(
        self,
        capacities: dict[str, tuple[int, int]],
        quantum: int,
        policy: str,
        saor_release_config: SaorReleaseConfig | None = None,
        record_ready_lifecycle_events: bool = False,
    ) -> None:
        self.capacities = capacities
        self.quantum = quantum
        self.policy = policy
        self.saor_release_config = saor_release_config
        self.record_ready_lifecycle_events = record_ready_lifecycle_events
        self.coordinator = FairEndpointCreditCoordinator(
            capacities,
            quantum=quantum,
            policy=policy,
            saor_release_config=saor_release_config,
            record_ready_lifecycle_events=record_ready_lifecycle_events,
        )

    def configuration(
        self,
    ) -> tuple[
        dict[str, tuple[int, int]],
        int,
        str,
        SaorReleaseConfig | None,
        bool,
    ]:
        return (
            self.capacities,
            self.quantum,
            self.policy,
            self.saor_release_config,
            self.record_ready_lifecycle_events,
        )

    def try_acquire(self, **kwargs) -> bool:
        return self.coordinator.try_acquire(**kwargs)

    def release(
        self,
        request_id: str,
        *,
        job_id: str,
        actual_work: int | None = None,
    ) -> None:
        self.coordinator.release(
            request_id,
            job_id=job_id,
            actual_work=actual_work,
        )

    def finish_job(self, job_id: str) -> None:
        self.coordinator.finish_job(job_id)

    def cancel_waiter(self, request_id: str, *, job_id: str) -> bool:
        return self.coordinator.cancel_waiter(request_id, job_id=job_id)

    def snapshot(self, endpoint_id: str):
        return self.coordinator.snapshot(endpoint_id)

    def drain_release_events(self, endpoint_id: str):
        return self.coordinator.drain_release_events(endpoint_id)

    def update_capacity(self, endpoint_id: str, **kwargs):
        return self.coordinator.update_capacity(endpoint_id, **kwargs)


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

    def release(
        self,
        request_id: str,
        *,
        job_id: str,
        actual_work: int | None = None,
    ) -> None:
        self.ray_module.get(
            self.actor.release.remote(
                request_id,
                job_id=job_id,
                actual_work=actual_work,
            )
        )

    def finish_job(self, job_id: str) -> None:
        self.ray_module.get(self.actor.finish_job.remote(job_id))

    def cancel_waiter(self, request_id: str, *, job_id: str) -> bool:
        return bool(
            self.ray_module.get(
                self.actor.cancel_waiter.remote(request_id, job_id=job_id)
            )
        )

    def snapshot(self, endpoint_id: str):
        return self.ray_module.get(
            self.actor.snapshot.remote(endpoint_id)
        )

    def drain_release_events(self, endpoint_id: str):
        return tuple(
            self.ray_module.get(
                self.actor.drain_release_events.remote(endpoint_id)
            )
        )

    def update_capacity(self, endpoint_id: str, **kwargs):
        return self.ray_module.get(
            self.actor.update_capacity.remote(endpoint_id, **kwargs)
        )


def get_or_create_shared_credit_client(
    ray_module,
    *,
    name: str,
    namespace: str,
    capacities: dict[str, tuple[int, int]],
    quantum: int,
    policy: str = "drr",
    saor_release_config: SaorReleaseConfig | None = None,
    record_ready_lifecycle_events: bool = False,
) -> RaySharedCreditClient:
    if not name:
        raise ValueError("shared credit actor name must be non-empty")
    if not namespace:
        raise ValueError("shared credit namespace must be non-empty")
    remote_actor = ray_module.remote(_FairCreditActor)
    actor_builder = remote_actor.options(
        name=name,
        namespace=namespace,
        lifetime="detached",
        get_if_exists=True,
        num_cpus=0,
    )
    actor = actor_builder.remote(
        capacities,
        quantum,
        policy,
        saor_release_config,
        record_ready_lifecycle_events,
    )
    configured = tuple(ray_module.get(actor.configuration.remote()))
    if len(configured) == 3:
        # A non-SAOR detached actor created by the previous runtime revision is
        # safe to reuse.  SAOR itself still fails closed because the old actor
        # cannot carry a release configuration.
        (
            configured_capacities,
            configured_quantum,
            configured_policy,
        ) = configured
        configured_saor_release = None
        configured_ready_lifecycle = False
    elif len(configured) == 4:
        (
            configured_capacities,
            configured_quantum,
            configured_policy,
            configured_saor_release,
        ) = configured
        configured_ready_lifecycle = False
    elif len(configured) == 5:
        (
            configured_capacities,
            configured_quantum,
            configured_policy,
            configured_saor_release,
            configured_ready_lifecycle,
        ) = configured
    else:
        raise ValueError("existing shared credit actor configuration is invalid")
    if (
        configured_capacities != capacities
        or configured_quantum != quantum
        or configured_policy != policy
        or configured_saor_release != saor_release_config
        or configured_ready_lifecycle != record_ready_lifecycle_events
    ):
        raise ValueError(
            "existing shared credit actor configuration does not match "
            "this job"
        )
    return RaySharedCreditClient(ray_module, actor)
