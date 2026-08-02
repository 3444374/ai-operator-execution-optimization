"""Ray-specific submission adapter for the typed scheduling core."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from ..core.models import CollectedSubmission, PayloadEnvelope, SubmissionCompletion


@dataclass(frozen=True)
class ActorWorkerAssignment:
    endpoint_id: str
    worker_id: str
    worker_index: int
    estimated_work: int
    submitted_at_s: float


@dataclass(frozen=True)
class ActorWorkerSnapshot:
    endpoint_id: str
    worker_id: str
    worker_index: int
    running: int
    active_work: int
    submitted: int
    completed: int
    failed: int
    max_running: int
    max_active_work: int
    slot_held_s: float


@dataclass
class _ActorWorkerState:
    running: int = 0
    active_work: int = 0
    submitted: int = 0
    completed: int = 0
    failed: int = 0
    max_running: int = 0
    max_active_work: int = 0
    slot_held_s: float = 0.0


class ActorWorkerPoolSubmitter:
    def __init__(
        self,
        actors: Sequence[object],
        method_name: str,
        *,
        endpoint_id: str = "endpoint",
        max_concurrency_per_worker: int = 2**31 - 1,
        routing_policy: Literal[
            "round_robin",
            "least_active_work",
        ] = "round_robin",
        clock: Callable[[], float] = time.perf_counter,
    ):
        if not actors:
            raise ValueError("actors must not be empty")
        if not method_name:
            raise ValueError("method_name must not be empty")
        if max_concurrency_per_worker <= 0:
            raise ValueError("max_concurrency_per_worker must be positive")
        if routing_policy not in {"round_robin", "least_active_work"}:
            raise ValueError("unsupported actor worker routing policy")
        self._actors = tuple(actors)
        self._method_name = method_name
        self._endpoint_id = endpoint_id
        self._max_concurrency_per_worker = max_concurrency_per_worker
        self._routing_policy = routing_policy
        self._clock = clock
        self._next_index = 0
        self._states = [_ActorWorkerState() for _ in self._actors]
        self._assignments: list[tuple[object, ActorWorkerAssignment]] = []

    @property
    def worker_count(self) -> int:
        return len(self._actors)

    @property
    def submission_counts(self) -> tuple[int, ...]:
        return tuple(state.submitted for state in self._states)

    def __call__(self, payload: object) -> object:
        return self.submit(payload, estimated_work=0)

    def submit(self, payload: object, *, estimated_work: int) -> object:
        if estimated_work < 0:
            raise ValueError("estimated_work must be non-negative")
        index = self._select_worker()
        handle = getattr(self._actors[index], self._method_name).remote(payload)
        state = self._states[index]
        state.running += 1
        state.active_work += estimated_work
        state.submitted += 1
        state.max_running = max(state.max_running, state.running)
        state.max_active_work = max(state.max_active_work, state.active_work)
        assignment = ActorWorkerAssignment(
            endpoint_id=self._endpoint_id,
            worker_id=f"{self._endpoint_id}:worker:{index}",
            worker_index=index,
            estimated_work=estimated_work,
            submitted_at_s=self._clock(),
        )
        self._assignments.append((handle, assignment))
        return handle

    def assignment(self, handle: object) -> ActorWorkerAssignment:
        matches = [
            assignment
            for canonical, assignment in self._assignments
            if canonical is handle
        ]
        if len(matches) != 1:
            raise RuntimeError("unknown or duplicate actor worker assignment")
        return matches[0]

    def complete(self, handle: object, *, failed: bool) -> None:
        matches = [
            index
            for index, (canonical, _) in enumerate(self._assignments)
            if canonical is handle
        ]
        if len(matches) != 1:
            raise RuntimeError("unknown or duplicate actor worker assignment")
        _, assignment = self._assignments.pop(matches[0])
        state = self._states[assignment.worker_index]
        state.running -= 1
        state.active_work -= assignment.estimated_work
        state.slot_held_s += max(0.0, self._clock() - assignment.submitted_at_s)
        if failed:
            state.failed += 1
        else:
            state.completed += 1

    def snapshots(self) -> tuple[ActorWorkerSnapshot, ...]:
        return tuple(
            ActorWorkerSnapshot(
                endpoint_id=self._endpoint_id,
                worker_id=f"{self._endpoint_id}:worker:{index}",
                worker_index=index,
                running=state.running,
                active_work=state.active_work,
                submitted=state.submitted,
                completed=state.completed,
                failed=state.failed,
                max_running=state.max_running,
                max_active_work=state.max_active_work,
                slot_held_s=state.slot_held_s,
            )
            for index, state in enumerate(self._states)
        )

    def _select_worker(self) -> int:
        eligible = [
            index
            for index, state in enumerate(self._states)
            if state.running < self._max_concurrency_per_worker
        ]
        if not eligible:
            raise RuntimeError("actor worker pool capacity exhausted")
        if self._routing_policy == "least_active_work":
            return min(
                eligible,
                key=lambda index: (
                    self._states[index].active_work
                    / self._max_concurrency_per_worker,
                    self._states[index].running,
                    index,
                ),
            )
        for offset in range(len(self._states)):
            index = (self._next_index + offset) % len(self._states)
            if index in eligible:
                self._next_index = (index + 1) % len(self._states)
                return index
        raise RuntimeError("actor worker pool capacity selection failed")


class RoundRobinSubmitter:
    """Persist round-robin position across independent submission calls."""

    def __init__(self, submitters: Sequence[Callable[[object], object]]):
        if not submitters:
            raise ValueError("submitters must not be empty")
        self._submitters = tuple(submitters)
        self._next_index = 0
        self._stateful_handles: list[
            tuple[object, ActorWorkerPoolSubmitter]
        ] = []

    def __call__(self, payload: object) -> object:
        index = self._next_index
        self._next_index = (index + 1) % len(self._submitters)
        submitter = self._submitters[index]
        if isinstance(submitter, ActorWorkerPoolSubmitter):
            handle = submitter.submit(payload, estimated_work=0)
            self._stateful_handles.append((handle, submitter))
            return handle
        return submitter(payload)

    def complete(self, handle: object, *, failed: bool) -> None:
        matches = [
            (index, canonical, submitter)
            for index, (canonical, submitter) in enumerate(
                self._stateful_handles
            )
            if canonical == handle
        ]
        if not matches:
            return
        if len(matches) != 1:
            raise RuntimeError("duplicate round-robin worker assignment")
        index, canonical, submitter = matches[0]
        submitter.complete(canonical, failed=failed)
        self._stateful_handles.pop(index)


class ActorSubmissionState:
    """Actor submitters whose endpoint-local and legacy positions span a run."""

    def __init__(
        self,
        actor_pools: Mapping[str, Sequence[object]],
        method_name: str,
        *,
        max_concurrency_per_worker: int = 2**31 - 1,
        routing_policy: Literal[
            "round_robin",
            "least_active_work",
        ] = "round_robin",
    ):
        self._actor_pools = {
            endpoint_id: tuple(actors)
            for endpoint_id, actors in actor_pools.items()
        }
        self._method_name = method_name
        self._max_concurrency_per_worker = max_concurrency_per_worker
        self._routing_policy = routing_policy
        self.pool_submitters = {
            endpoint_id: ActorWorkerPoolSubmitter(
                actors,
                method_name,
                endpoint_id=endpoint_id,
                max_concurrency_per_worker=max_concurrency_per_worker,
                routing_policy=routing_policy,
            )
            for endpoint_id, actors in self._actor_pools.items()
        }
        self.legacy_endpoint_submitter = RoundRobinSubmitter(
            list(self.pool_submitters.values())
        )

    def wait_until_ready(
        self,
        ray_module,
        *,
        clock: Callable[[], float] = time.perf_counter,
    ) -> tuple[float, tuple[object, ...]]:
        """Resolve every actor's explicit ready method before measurement."""

        started_at_s = clock()
        ready_refs = [
            actor.ready.remote()
            for actors in self._actor_pools.values()
            for actor in actors
        ]
        evidence = tuple(ray_module.get(ready_refs))
        if len(evidence) != len(ready_refs):
            raise RuntimeError("actor ready barrier returned incomplete evidence")
        return max(0.0, clock() - started_at_s), evidence

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
        self._stateful_handles: list[
            tuple[object, ActorWorkerPoolSubmitter]
        ] = []

    def submit(self, envelope: PayloadEnvelope, endpoint_id: str) -> object:
        submitter = self.submitters.get(endpoint_id)
        if submitter is None:
            raise RuntimeError(f"no Ray submitter for endpoint {endpoint_id}")
        if isinstance(submitter, ActorWorkerPoolSubmitter):
            handle = submitter.submit(
                envelope.payload,
                estimated_work=envelope.request.estimated_work_units,
            )
            self._stateful_handles.append((handle, submitter))
            return handle
        return submitter(envelope.payload)

    def wait_one(self, pending) -> CollectedSubmission:
        collected = self._wait_one(pending, timeout_s=None)
        if collected is None:
            raise RuntimeError("blocking Ray wait returned no completion")
        return collected

    def poll_one(self, pending) -> CollectedSubmission | None:
        return self._wait_one(pending, timeout_s=0.0)

    def _wait_one(
        self,
        pending,
        *,
        timeout_s: float | None,
    ) -> CollectedSubmission | None:
        wait_start = time.perf_counter()
        wait_kwargs = {"num_returns": 1}
        if timeout_s is not None:
            wait_kwargs["timeout"] = timeout_s
        ready, _ = self.ray_module.wait(
            [handle for handle, _ in pending],
            **wait_kwargs,
        )
        wait_s = time.perf_counter() - wait_start
        if not ready:
            return None
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
        stateful_matches = [
            (index, submitter)
            for index, (canonical, submitter) in enumerate(
                self._stateful_handles
            )
            if canonical is handle
        ]
        assignment = (
            stateful_matches[0][1].assignment(handle)
            if len(stateful_matches) == 1
            else None
        )
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
        finally:
            if len(stateful_matches) == 1:
                stateful_index, stateful_submitter = stateful_matches[0]
                stateful_submitter.complete(
                    handle,
                    failed=completion.status == "failed",
                )
                self._stateful_handles.pop(stateful_index)
        result_s = time.perf_counter() - result_start
        return CollectedSubmission(
            handle,
            completion,
            wait_s,
            result_s,
            actor_worker_id=assignment.worker_id if assignment is not None else "",
            actor_worker_index=(
                assignment.worker_index if assignment is not None else -1
            ),
            actor_worker_pid=(
                int(completion.result.get("actor_worker_pid", 0))
                if assignment is not None
                and completion.status == "completed"
                and isinstance(completion.result, dict)
                else 0
            ),
        )
