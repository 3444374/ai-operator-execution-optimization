"""Ray-specific submission adapter for the typed scheduling core."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence

from ..models import CollectedSubmission, PayloadEnvelope, SubmissionCompletion


class ActorWorkerPoolSubmitter:
    def __init__(self, actors: Sequence[object], method_name: str):
        if not actors:
            raise ValueError("actors must not be empty")
        if not method_name:
            raise ValueError("method_name must not be empty")
        self._actors = tuple(actors)
        self._method_name = method_name
        self._next_index = 0
        self._submission_counts = [0] * len(self._actors)

    @property
    def worker_count(self) -> int:
        return len(self._actors)

    @property
    def submission_counts(self) -> tuple[int, ...]:
        return tuple(self._submission_counts)

    def __call__(self, payload: object) -> object:
        index = self._next_index
        self._next_index = (index + 1) % len(self._actors)
        self._submission_counts[index] += 1
        return getattr(self._actors[index], self._method_name).remote(payload)


class RoundRobinSubmitter:
    """Persist round-robin position across independent submission calls."""

    def __init__(self, submitters: Sequence[Callable[[object], object]]):
        if not submitters:
            raise ValueError("submitters must not be empty")
        self._submitters = tuple(submitters)
        self._next_index = 0

    def __call__(self, payload: object) -> object:
        index = self._next_index
        self._next_index = (index + 1) % len(self._submitters)
        return self._submitters[index](payload)


class ActorSubmissionState:
    """Actor submitters whose endpoint-local and legacy positions span a run."""

    def __init__(
        self,
        actor_pools: Mapping[str, Sequence[object]],
        method_name: str,
    ):
        self._actor_pools = {
            endpoint_id: tuple(actors)
            for endpoint_id, actors in actor_pools.items()
        }
        self._method_name = method_name
        self.pool_submitters = {
            endpoint_id: ActorWorkerPoolSubmitter(actors, method_name)
            for endpoint_id, actors in self._actor_pools.items()
        }
        self.legacy_endpoint_submitter = RoundRobinSubmitter(
            list(self.pool_submitters.values())
        )

    def validate(
        self,
        actor_pools: Mapping[str, Sequence[object]],
        method_name: str,
    ) -> None:
        if method_name != self._method_name:
            raise ValueError("submission_state must use the same method_name")
        if set(actor_pools) != set(self._actor_pools):
            raise ValueError(
                "submission_state and actor_pools must have identical "
                "service endpoint IDs"
            )
        for endpoint_id, expected in self._actor_pools.items():
            actual = tuple(actor_pools[endpoint_id])
            if len(actual) != len(expected) or any(
                current is not original
                for current, original in zip(actual, expected)
            ):
                raise ValueError(
                    "submission_state must use the same actor workers"
                )


class RaySubmissionAdapter:
    def __init__(
        self,
        ray_module,
        submitters: Mapping[str, Callable[[object], object]],
    ):
        self.ray_module = ray_module
        self.submitters = dict(submitters)

    def submit(self, envelope: PayloadEnvelope, endpoint_id: str) -> object:
        submitter = self.submitters.get(endpoint_id)
        if submitter is None:
            raise RuntimeError(f"no Ray submitter for endpoint {endpoint_id}")
        return submitter(envelope.payload)

    def wait_one(self, pending) -> CollectedSubmission:
        wait_start = time.perf_counter()
        ready, _ = self.ray_module.wait(
            [handle for handle, _ in pending],
            num_returns=1,
        )
        wait_s = time.perf_counter() - wait_start
        ready_handle = ready[0]
        matches = [
            (item, envelope)
            for item, envelope in pending
            if item == ready_handle
        ]
        if len(matches) != 1:
            raise RuntimeError("Ray returned an unknown or duplicate pending handle")
        # Return the canonical object stored in ``pending``. Ray normally
        # returns the same ObjectRef instance, but the scheduler deliberately
        # uses identity matching so equal-valued duplicate handles cannot
        # remove the wrong submission.
        handle, matched_envelope = matches[0]
        result_start = time.perf_counter()
        try:
            result = self.ray_module.get(ready_handle)
        except Exception as exc:
            completion = SubmissionCompletion(
                matched_envelope.request.request_id,
                "failed",
                error=f"{type(exc).__name__}: {exc}",
            )
        else:
            completion = SubmissionCompletion(
                matched_envelope.request.request_id,
                "completed",
                result=result,
            )
        result_s = time.perf_counter() - result_start
        return CollectedSubmission(
            handle,
            completion,
            wait_s,
            result_s,
        )
