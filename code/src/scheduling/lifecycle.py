"""Pure request-lifecycle models and exactly-once trace assembly."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal, Mapping, Sequence

from .models import SubmissionLifecycleEvent


_TIME_TOLERANCE_S = 1e-6
OutputTokenSource = Literal[
    "submission_aggregate_unavailable",
    "endpoint_request",
]


@dataclass(frozen=True)
class RequestLifecycleSeed:
    request_id: str
    submission_id: str
    doc_id: str
    prompt_tokens: int
    estimated_output_tokens: int
    prefix_key: str
    arrival_epoch_s: float
    flush_epoch_s: float

    def __post_init__(self) -> None:
        if not self.request_id or not self.submission_id or not self.doc_id:
            raise ValueError(
                "request_id, submission_id, and doc_id must be non-empty"
            )
        if self.prompt_tokens < 0 or self.estimated_output_tokens < 0:
            raise ValueError("token counts must be non-negative")
        _ordered_times(
            ("arrival_epoch_s", self.arrival_epoch_s),
            ("flush_epoch_s", self.flush_epoch_s),
        )


@dataclass(frozen=True)
class SubmissionServiceTiming:
    submission_id: str
    service_start_epoch_s: float | None
    service_end_epoch_s: float | None

    def __post_init__(self) -> None:
        if not self.submission_id:
            raise ValueError("submission_id must be non-empty")
        if (self.service_start_epoch_s is None) != (
            self.service_end_epoch_s is None
        ):
            raise ValueError("service timestamps must both be present or absent")
        if self.service_start_epoch_s is not None:
            _ordered_times(
                ("service_start_epoch_s", self.service_start_epoch_s),
                ("service_end_epoch_s", self.service_end_epoch_s),
            )


@dataclass(frozen=True)
class RequestTraceRow:
    request_id: str
    submission_id: str
    doc_id: str
    pool_id: str
    endpoint_id: str
    gpu_id: str
    prompt_tokens: int
    estimated_output_tokens: int
    client_estimated_output_tokens: int | None
    actual_output_tokens: int | None
    output_token_source: OutputTokenSource
    total_tokens: int | None
    prefix_key: str
    status: str
    error_type: str
    arrival_epoch_s: float
    flush_epoch_s: float
    submit_epoch_s: float
    service_start_epoch_s: float | None
    completion_epoch_s: float
    buffer_s: float
    submit_to_service_s: float | None
    service_s: float | None
    e2e_s: float
    latency_granularity: Literal["submission", "request"]
    slo_target_s: float | None
    slo_met: bool | None


def build_request_trace_rows(
    seeds: Sequence[RequestLifecycleSeed],
    submission_events: Sequence[SubmissionLifecycleEvent],
    service_by_submission_id: Mapping[str, SubmissionServiceTiming],
    client_estimated_output_tokens_by_doc_id: Mapping[str, int],
    actual_output_tokens_by_doc_id: Mapping[str, int],
    *,
    slo_target_s: float | None,
) -> tuple[RequestTraceRow, ...]:
    """Join row, submission, service, and token facts without inventing data."""

    if slo_target_s is not None and (
        not math.isfinite(slo_target_s) or slo_target_s <= 0
    ):
        raise ValueError("slo_target_s must be finite and positive when present")

    _require_unique((item.request_id for item in seeds), "request_id")
    _require_unique((item.doc_id for item in seeds), "doc_id")
    _require_unique(
        (item.submission_id for item in submission_events),
        "submission lifecycle event",
    )

    events_by_id = {
        item.submission_id: item for item in submission_events
    }
    seeded_submission_ids = {item.submission_id for item in seeds}
    if seeded_submission_ids != set(events_by_id):
        raise ValueError("submission lifecycle events do not match request seeds")
    if set(service_by_submission_id) != set(events_by_id):
        raise ValueError("service timing keys do not match submission events")
    for submission_id, timing in service_by_submission_id.items():
        if timing.submission_id != submission_id:
            raise ValueError("service timing key does not match submission_id")

    successful_doc_ids = {
        item.doc_id
        for item in seeds
        if events_by_id[item.submission_id].status == "completed"
    }
    if set(client_estimated_output_tokens_by_doc_id) != successful_doc_ids:
        raise ValueError(
            "client-estimated output token keys do not match successful requests"
        )
    if not set(actual_output_tokens_by_doc_id).issubset(successful_doc_ids):
        raise ValueError("actual output token keys contain unknown requests")
    _validate_token_map(
        client_estimated_output_tokens_by_doc_id,
        "client-estimated output tokens",
    )
    _validate_token_map(actual_output_tokens_by_doc_id, "actual output tokens")

    rows = []
    for item in seeds:
        event = events_by_id[item.submission_id]
        service = service_by_submission_id[item.submission_id]
        client_estimated_output_tokens = (
            client_estimated_output_tokens_by_doc_id.get(item.doc_id)
        )
        actual_output_tokens = actual_output_tokens_by_doc_id.get(item.doc_id)

        if event.status == "completed":
            if (
                service.service_start_epoch_s is None
                or service.service_end_epoch_s is None
            ):
                raise ValueError(
                    "completed submission must have service timing"
                )
            _ordered_times(
                ("arrival_epoch_s", item.arrival_epoch_s),
                ("flush_epoch_s", item.flush_epoch_s),
                ("submit_epoch_s", event.submit_epoch_s),
                ("service_start_epoch_s", service.service_start_epoch_s),
                ("service_end_epoch_s", service.service_end_epoch_s),
                ("completion_epoch_s", event.completion_epoch_s),
            )
            submit_to_service_s = _duration(
                event.submit_epoch_s,
                service.service_start_epoch_s,
            )
            service_s = _duration(
                service.service_start_epoch_s,
                service.service_end_epoch_s,
            )
        else:
            if (
                service.service_start_epoch_s is not None
                or service.service_end_epoch_s is not None
            ):
                raise ValueError("failed submission cannot have service timing")
            _ordered_times(
                ("arrival_epoch_s", item.arrival_epoch_s),
                ("flush_epoch_s", item.flush_epoch_s),
                ("submit_epoch_s", event.submit_epoch_s),
                ("completion_epoch_s", event.completion_epoch_s),
            )
            submit_to_service_s = None
            service_s = None

        e2e_s = _duration(item.arrival_epoch_s, event.completion_epoch_s)
        if slo_target_s is None:
            slo_met = None
        else:
            slo_met = (
                event.status == "completed"
                and e2e_s <= slo_target_s + _TIME_TOLERANCE_S
            )

        output_token_source: OutputTokenSource
        if actual_output_tokens is None:
            output_token_source = "submission_aggregate_unavailable"
            total_tokens = None
        else:
            output_token_source = "endpoint_request"
            total_tokens = item.prompt_tokens + actual_output_tokens

        rows.append(
            RequestTraceRow(
                request_id=item.request_id,
                submission_id=item.submission_id,
                doc_id=item.doc_id,
                pool_id=event.pool_id,
                endpoint_id=event.endpoint_id,
                gpu_id=event.gpu_id,
                prompt_tokens=item.prompt_tokens,
                estimated_output_tokens=item.estimated_output_tokens,
                client_estimated_output_tokens=client_estimated_output_tokens,
                actual_output_tokens=actual_output_tokens,
                output_token_source=output_token_source,
                total_tokens=total_tokens,
                prefix_key=item.prefix_key,
                status=event.status,
                error_type=event.error,
                arrival_epoch_s=item.arrival_epoch_s,
                flush_epoch_s=item.flush_epoch_s,
                submit_epoch_s=event.submit_epoch_s,
                service_start_epoch_s=service.service_start_epoch_s,
                completion_epoch_s=event.completion_epoch_s,
                buffer_s=_duration(
                    item.arrival_epoch_s,
                    item.flush_epoch_s,
                ),
                submit_to_service_s=submit_to_service_s,
                service_s=service_s,
                e2e_s=e2e_s,
                latency_granularity="submission",
                slo_target_s=slo_target_s,
                slo_met=slo_met,
            )
        )
    return tuple(rows)


def _duration(start_s: float, end_s: float) -> float:
    difference = end_s - start_s
    if difference < -_TIME_TOLERANCE_S:
        raise ValueError("lifecycle timestamp order is invalid")
    return max(0.0, difference)


def _ordered_times(*values: tuple[str, float]) -> None:
    for name, value in values:
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{name} must be finite and non-negative")
    for (_, previous), (_, current) in zip(values, values[1:]):
        if current < previous - _TIME_TOLERANCE_S:
            raise ValueError("lifecycle timestamp order is invalid")


def _require_unique(values, label: str) -> None:
    seen = set()
    for value in values:
        if value in seen:
            raise ValueError(f"duplicate {label}: {value}")
        seen.add(value)


def _validate_token_map(values: Mapping[str, int], label: str) -> None:
    if any(
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        for value in values.values()
    ):
        raise ValueError(f"{label} must be non-negative integers")
