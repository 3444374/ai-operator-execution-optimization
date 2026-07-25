"""Ray-specific submission adapter for the typed scheduling core."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping

from .models import CollectedSubmission, PayloadEnvelope, SubmissionCompletion


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
