"""Typed staged-work and runtime-state contracts for text completion."""

from __future__ import annotations

import hashlib
import json

from ...planning.work import (
    RuntimeStateSnapshot,
    StageStateSnapshot,
    StageWork,
    WorkDescriptor,
)


def text_work_calibration_signature(
    *,
    model_revision: str,
    serving_revision: str,
    protocol: str,
    cost_model_revision: str,
) -> str:
    """Identify estimates that are safe to compare in one control loop."""
    fields = {
        "model_revision": model_revision,
        "serving_revision": serving_revision,
        "protocol": protocol,
        "cost_model_revision": cost_model_revision,
    }
    if any(not value for value in fields.values()):
        raise ValueError("text work calibration identity fields must be non-empty")
    payload = {"schema": "text-stage-work-v1", **fields}
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"text-stage-work-v1:{digest}"


def build_text_work_descriptor(
    *,
    prompt_tokens: int,
    estimated_output_tokens: int,
    prompt_bytes: int,
    result_bytes_upper: int,
    calibration_signature: str,
    prefix_key: str = "",
) -> WorkDescriptor:
    """Describe source, organization, model, and result work for a request."""
    values = (
        prompt_tokens,
        estimated_output_tokens,
        prompt_bytes,
        result_bytes_upper,
    )
    if any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in values
    ):
        raise ValueError("text work values must be non-negative integers")
    if not calibration_signature:
        raise ValueError("calibration_signature must be non-empty")
    model_work = prompt_tokens + estimated_output_tokens
    return WorkDescriptor(
        stages=(
            StageWork("source", prompt_bytes, "bytes"),
            StageWork("organizer", model_work, "tokens"),
            StageWork("model", model_work, "tokens"),
            StageWork("result", result_bytes_upper, "bytes"),
        ),
        primary_stage="model",
        calibration_signature=calibration_signature,
        locality_key=prefix_key,
        lower_primary_units=prompt_tokens,
        upper_primary_units=model_work,
    )


def build_text_runtime_snapshot(
    *,
    active_work: int,
    upstream_queued_work: int,
    service_waiting_requests: int,
    active_requests: int,
    oldest_upstream_age_s: float,
    observed_at_s: float,
    capacity_work: int,
    calibration_signature: str,
    service_rate_tokens_s: float | None = None,
) -> RuntimeStateSnapshot:
    """Translate shared-credit and vLLM observations into staged token work.

    vLLM exposes waiting request count rather than waiting token work. The
    conversion uses current mean active request work and is therefore an
    estimate; when no request is active it conservatively reports zero model
    queue work instead of inventing a token size.
    """
    integer_values = (
        active_work,
        upstream_queued_work,
        service_waiting_requests,
        active_requests,
        capacity_work,
    )
    if any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in integer_values
    ) or capacity_work <= 0:
        raise ValueError("runtime work/count values must be valid non-negative integers")
    mean_active_work = active_work / active_requests if active_requests else 0.0
    service_queued_work = int(round(service_waiting_requests * mean_active_work))
    stages = (
        StageStateSnapshot(
            stage="organizer",
            active_work=0,
            queued_work=upstream_queued_work,
            service_rate_units_s=None,
            oldest_queue_age_s=oldest_upstream_age_s,
            observed_at_s=observed_at_s,
        ),
        StageStateSnapshot(
            stage="model",
            active_work=active_work,
            queued_work=service_queued_work,
            service_rate_units_s=service_rate_tokens_s,
            oldest_queue_age_s=(oldest_upstream_age_s if service_queued_work else 0.0),
            observed_at_s=observed_at_s,
            capacity_work=capacity_work,
        ),
    )
    return RuntimeStateSnapshot(
        stages=stages,
        observed_at_s=observed_at_s,
        calibration_signature=calibration_signature,
    )
