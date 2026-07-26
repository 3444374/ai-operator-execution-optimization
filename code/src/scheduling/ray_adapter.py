"""Ray-specific submission adapter for the typed scheduling core."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence

from .models import CollectedSubmission, PayloadEnvelope, SubmissionCompletion


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
        handle = ready[0]
        matches = [envelope for item, envelope in pending if item == handle]
        if len(matches) != 1:
            raise RuntimeError("Ray returned an unknown or duplicate pending handle")
        result_start = time.perf_counter()
        result = self.ray_module.get(handle)
        result_s = time.perf_counter() - result_start
        return CollectedSubmission(
            handle,
            SubmissionCompletion(matches[0].request.request_id, "completed", result=result),
            wait_s,
            result_s,
        )
