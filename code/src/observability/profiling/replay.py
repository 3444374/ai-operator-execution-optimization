"""Arrow payload and arrival-replay assembly for profiler runs."""

from __future__ import annotations

import argparse
import math
from collections.abc import Iterable
from dataclasses import replace

import pyarrow as pa

from src.modalities.text.costs import OutputCostMode, resolve_output_tokens
from src.scheduling.organization.batching import (
    ArrivalReplayBatcher,
    PendingBatch,
    PendingBatchBuilder,
    ReplayServiceObservation,
    RowArrival,
    SystemReplayClock,
)
from src.scheduling.submission_control.flush import (
    FixedTimeoutFlush,
    ImmediateFlush,
    QueueAdaptiveFlush,
    SloAwareEwmaFlush,
)
from src.scheduling.core.lifecycle import MonotonicEpochClock, RequestLifecycleSeed
from src.scheduling.core.models import BatchRequest, PayloadEnvelope
from src.scheduling.organization.service_quantum import slice_service_quanta
from src.scheduling.organization.token_budget import (
    ServiceQuantumTokenBudgetController,
    StaticTokenBudgetController,
)


def _requires_replay_feedback(args) -> bool:
    return (
        args.flush_policy in {"queue_adaptive", "slo_ewma"}
        or getattr(args, "token_budget_policy", "static") == "service_quantum"
    )


def _batch_envelopes(
    batches: Iterable[pa.RecordBatch | pa.Table],
    job_id: str,
    operator: str,
    completion_max_tokens: int,
    output_cost_mode: OutputCostMode = "fixed_output_cap",
    batch_index_start: int = 0,
) -> list[PayloadEnvelope]:
    envelopes = []
    for index, batch in enumerate(batches):
        request_id = f"{job_id}:batch:{batch_index_start + index}"
        envelopes.append(
            _batch_envelope(
                batch,
                request_id=request_id,
                job_id=job_id,
                operator=operator,
                completion_max_tokens=completion_max_tokens,
                output_cost_mode=output_cost_mode,
                planning_batch_id=request_id,
            )
        )
    return envelopes


def _batch_envelope(
    batch: pa.RecordBatch | pa.Table,
    *,
    request_id: str,
    job_id: str,
    operator: str,
    completion_max_tokens: int,
    output_cost_mode: OutputCostMode,
    planning_batch_id: str,
    service_quantum_index: int = -1,
    service_quantum_oversized: bool = False,
) -> PayloadEnvelope:
    prompt_tokens = sum(
        _row_prompt_tokens(batch, row_index)
        for row_index in range(batch.num_rows)
    )
    prefix_key = ""
    if "prefix_key" in batch.column_names and batch.num_rows:
        prefix_values = {
            str(value.as_py() or "") for value in batch.column("prefix_key")
        }
        if len(prefix_values) == 1:
            prefix_key = prefix_values.pop()
    preferred_endpoint_id = _preferred_endpoint_id(batch)
    arrival_times = []
    if "arrival_time_s" in batch.column_names:
        arrival_times = [
            float(value.as_py())
            for value in batch.column("arrival_time_s")
            if value.as_py() is not None
        ]
    oldest_arrival_s = min(arrival_times, default=0.0)
    return PayloadEnvelope(
        request=BatchRequest(
            request_id=request_id,
            job_id=job_id,
            operator=operator,
            row_count=batch.num_rows,
            prompt_tokens=prompt_tokens,
            estimated_output_tokens=sum(
                _row_output_tokens(
                    batch,
                    row_index,
                    output_cost_mode=output_cost_mode,
                    completion_max_tokens=completion_max_tokens,
                )
                for row_index in range(batch.num_rows)
            ),
            prefix_key=prefix_key,
            first_arrival_s=oldest_arrival_s,
            oldest_arrival_s=oldest_arrival_s,
            payload_id=request_id,
            planning_batch_id=planning_batch_id,
            estimated_payload_bytes=int(batch.nbytes),
            service_quantum_index=service_quantum_index,
            service_quantum_oversized=service_quantum_oversized,
            preferred_endpoint_id=preferred_endpoint_id,
        ),
        payload=batch,
    )


def _preferred_endpoint_id(batch: pa.Table | pa.RecordBatch) -> str:
    if "preferred_endpoint_id" not in batch.column_names or not batch.num_rows:
        return ""
    values = {
        str(value.as_py() or "")
        for value in batch.column("preferred_endpoint_id")
    }
    return values.pop() if len(values) == 1 else ""


def _service_quantum_envelopes(
    batch: pa.RecordBatch | pa.Table,
    *,
    planning_batch_id: str,
    job_id: str,
    operator: str,
    completion_max_tokens: int,
    output_cost_mode: OutputCostMode,
    target_tokens: int,
) -> tuple[PayloadEnvelope, ...]:
    """Slice a planning batch between rows, never within a model request."""

    row_costs = [
        _row_prompt_tokens(batch, row_index)
        + _row_output_tokens(
            batch,
            row_index,
            output_cost_mode=output_cost_mode,
            completion_max_tokens=completion_max_tokens,
        )
        for row_index in range(batch.num_rows)
    ]
    envelopes = []
    for quantum_index, quantum in enumerate(
        slice_service_quanta(row_costs, target_tokens)
    ):
        request_id = f"{planning_batch_id}:quantum:{quantum_index}"
        envelopes.append(
            _batch_envelope(
                batch.slice(quantum.start, quantum.row_count),
                request_id=request_id,
                job_id=job_id,
                operator=operator,
                completion_max_tokens=completion_max_tokens,
                output_cost_mode=output_cost_mode,
                planning_batch_id=planning_batch_id,
                service_quantum_index=quantum_index,
                service_quantum_oversized=quantum.oversized,
            )
        )
    return tuple(envelopes)


def _offline_request_envelopes(
    batch: pa.RecordBatch | pa.Table,
    *,
    planning_batch_id: str,
    job_id: str,
    operator: str,
    completion_max_tokens: int,
    output_cost_mode: OutputCostMode,
) -> tuple[PayloadEnvelope, ...]:
    if "doc_id" not in batch.column_names:
        raise ValueError("doc_id column is required for request expansion")
    envelopes = []
    for row_index in range(batch.num_rows):
        doc_value = batch.column("doc_id")[row_index].as_py()
        if doc_value is None:
            raise ValueError("doc_id must be non-null for request expansion")
        request_id = f"{job_id}:request:{doc_value}"
        envelopes.append(
            _batch_envelope(
                batch.slice(row_index, 1),
                request_id=request_id,
                job_id=job_id,
                operator=operator,
                completion_max_tokens=completion_max_tokens,
                output_cost_mode=output_cost_mode,
                planning_batch_id=planning_batch_id,
            )
        )
    return tuple(envelopes)


def _offline_batch_envelopes(
    batches: Iterable[pa.Table | pa.RecordBatch],
    *,
    job_id: str,
    operator: str,
    completion_max_tokens: int,
    output_cost_mode: OutputCostMode,
    batch_index_start: int,
    job_start_epoch_s: float,
    ready_epoch_s: float,
    submission_granularity: str = "batch",
    service_quantum_tokens: int = 0,
    planning_sink=None,
    quantum_sink=None,
) -> tuple[list[PayloadEnvelope], list[RequestLifecycleSeed]]:
    materialized_batches = list(batches)
    envelopes = []
    seeds = []
    for batch_offset, batch in enumerate(materialized_batches):
        planning_batch_id = f"{job_id}:batch:{batch_index_start + batch_offset}"
        planning_work = sum(
            _row_prompt_tokens(batch, row_index)
            + _row_output_tokens(
                batch,
                row_index,
                output_cost_mode=output_cost_mode,
                completion_max_tokens=completion_max_tokens,
            )
            for row_index in range(batch.num_rows)
        )
        if planning_sink is not None:
            planning_sink.append((planning_work, batch.num_rows))
        if submission_granularity == "service_quantum":
            batch_envelopes = _service_quantum_envelopes(
                batch,
                planning_batch_id=planning_batch_id,
                job_id=job_id,
                operator=operator,
                completion_max_tokens=completion_max_tokens,
                output_cost_mode=output_cost_mode,
                target_tokens=service_quantum_tokens,
            )
        elif submission_granularity == "request":
            batch_envelopes = _offline_request_envelopes(
                batch,
                planning_batch_id=planning_batch_id,
                job_id=job_id,
                operator=operator,
                completion_max_tokens=completion_max_tokens,
                output_cost_mode=output_cost_mode,
            )
        elif submission_granularity == "batch":
            batch_envelopes = (
                _batch_envelope(
                    batch,
                    request_id=planning_batch_id,
                    job_id=job_id,
                    operator=operator,
                    completion_max_tokens=completion_max_tokens,
                    output_cost_mode=output_cost_mode,
                    planning_batch_id=planning_batch_id,
                ),
            )
        else:
            raise ValueError(
                "offline envelope expansion supports batch, request, "
                "or service_quantum"
            )
        envelopes.extend(batch_envelopes)
        if quantum_sink is not None and submission_granularity == "service_quantum":
            quantum_sink.extend(
                (
                    envelope.request.estimated_total_tokens,
                    envelope.request.row_count,
                    envelope.request.service_quantum_oversized,
                )
                for envelope in batch_envelopes
            )
        submission_ids = [
            envelope.request.request_id
            for envelope in batch_envelopes
            for _ in range(envelope.request.row_count)
        ]
        if "doc_id" not in batch.column_names:
            raise ValueError("doc_id column is required for request tracing")
        for row_index, submission_id in enumerate(submission_ids):
            doc_value = batch.column("doc_id")[row_index].as_py()
            if doc_value is None:
                raise ValueError(
                    "doc_id must be non-null for request tracing"
                )
            prompt_tokens = (
                int(
                    batch.column("prompt_tokens")[row_index].as_py()
                    or 0
                )
                if "prompt_tokens" in batch.column_names
                else 0
            )
            prefix_key = (
                str(batch.column("prefix_key")[row_index].as_py() or "")
                if "prefix_key" in batch.column_names
                else ""
            )
            seeds.append(
                RequestLifecycleSeed(
                    request_id=f"{job_id}:row:{doc_value}",
                    submission_id=submission_id,
                    doc_id=str(doc_value),
                    prompt_tokens=prompt_tokens,
                    estimated_output_tokens=_row_output_tokens(
                        batch,
                        row_index,
                        output_cost_mode=output_cost_mode,
                        completion_max_tokens=completion_max_tokens,
                    ),
                    prefix_key=prefix_key,
                    arrival_epoch_s=job_start_epoch_s,
                    flush_epoch_s=ready_epoch_s,
                    request_time_origin="offline_job_start",
                    latency_granularity=(
                        "request"
                        if submission_granularity == "request"
                        else "submission"
                    ),
                )
            )
    return envelopes, seeds


def _row_prompt_tokens(
    table: pa.Table | pa.RecordBatch,
    row_index: int,
) -> int:
    if "prompt_tokens" not in table.column_names:
        return 0
    return int(table.column("prompt_tokens")[row_index].as_py() or 0)


def _row_output_tokens(
    table: pa.Table | pa.RecordBatch,
    row_index: int,
    *,
    output_cost_mode: OutputCostMode,
    completion_max_tokens: int,
) -> int:
    target_value = (
        table.column("target_output_tokens")[row_index].as_py()
        if "target_output_tokens" in table.column_names
        else None
    )
    return resolve_output_tokens(
        output_cost_mode,
        completion_max_tokens=completion_max_tokens,
        target_output_tokens=target_value,
    )

def _row_arrivals(
    table: pa.Table | pa.RecordBatch,
    completion_max_tokens: int,
    output_cost_mode: OutputCostMode = "fixed_output_cap",
) -> list[RowArrival]:
    if "arrival_time_s" not in table.column_names:
        raise ValueError("arrival_time_s column is required for arrival replay")
    previous_arrival_s: float | None = None
    arrivals = []
    for index in range(table.num_rows):
        arrival_value = table.column("arrival_time_s")[index].as_py()
        if (
            not isinstance(arrival_value, (int, float))
            or isinstance(arrival_value, bool)
            or not math.isfinite(arrival_value)
            or arrival_value < 0
        ):
            raise ValueError(
                "arrival_time_s must be present, finite, and non-negative"
            )
        arrival_s = float(arrival_value)
        if previous_arrival_s is not None and arrival_s < previous_arrival_s:
            raise ValueError("arrival_time_s values must be non-decreasing")
        previous_arrival_s = arrival_s

        prompt_tokens = 0
        if "prompt_tokens" in table.column_names:
            prompt_value = table.column("prompt_tokens")[index].as_py()
            prompt_tokens = int(prompt_value or 0)
        prefix_key = ""
        if "prefix_key" in table.column_names:
            prefix_value = table.column("prefix_key")[index].as_py()
            prefix_key = str(prefix_value or "")
        row_value = (
            table.column("doc_id")[index].as_py()
            if "doc_id" in table.column_names
            else index
        )
        arrivals.append(
            RowArrival(
                row_id=str(row_value),
                arrival_s=arrival_s,
                prompt_tokens=prompt_tokens,
                estimated_output_tokens=_row_output_tokens(
                    table,
                    index,
                    output_cost_mode=output_cost_mode,
                    completion_max_tokens=completion_max_tokens,
                ),
                prefix_key=prefix_key,
                payload_ref=table.slice(index, 1),
            )
        )
    return arrivals


def _arrow_envelope(
    pending: PendingBatch,
    batch_index: int,
    job_id: str,
    operator: str,
) -> PayloadEnvelope:
    payloads = [row.payload_ref for row in pending.rows]
    if not all(
        isinstance(payload, (pa.Table, pa.RecordBatch)) and payload.num_rows == 1
        for payload in payloads
    ):
        raise ValueError("each replay payload_ref must be a one-row Arrow payload")
    payload = pa.concat_tables(
        [
            item
            if isinstance(item, pa.Table)
            else pa.Table.from_batches([item])
            for item in payloads
        ]
    )
    prefix_values = {row.prefix_key for row in pending.rows}
    prefix_key = prefix_values.pop() if len(prefix_values) == 1 else ""
    request_id = f"{job_id}:batch:{batch_index}"
    return PayloadEnvelope(
        request=BatchRequest(
            request_id=request_id,
            job_id=job_id,
            operator=operator,
            row_count=pending.row_count,
            prompt_tokens=pending.prompt_tokens,
            estimated_output_tokens=pending.estimated_output_tokens,
            prefix_key=prefix_key,
            first_arrival_s=pending.rows[0].arrival_s,
            oldest_arrival_s=pending.oldest_arrival_s,
            payload_id=request_id,
            planning_batch_id=request_id,
            preferred_endpoint_id=_preferred_endpoint_id(payload),
            estimated_payload_bytes=int(payload.nbytes),
        ),
        payload=payload,
    )


def _request_envelopes(
    pending: PendingBatch,
    *,
    job_id: str,
    operator: str,
    planning_batch_id: str,
) -> tuple[PayloadEnvelope, ...]:
    envelopes = []
    for row in pending.rows:
        request_id = f"{job_id}:request:{row.row_id}"
        envelopes.append(
            PayloadEnvelope(
                request=BatchRequest(
                    request_id=request_id,
                    job_id=job_id,
                    operator=operator,
                    row_count=1,
                    prompt_tokens=row.prompt_tokens,
                    estimated_output_tokens=row.estimated_output_tokens,
                    prefix_key=row.prefix_key,
                    first_arrival_s=row.arrival_s,
                    oldest_arrival_s=row.arrival_s,
                    payload_id=request_id,
                    planning_batch_id=planning_batch_id,
                    preferred_endpoint_id=_preferred_endpoint_id(
                        row.payload_ref
                    ),
                    estimated_payload_bytes=int(row.payload_ref.nbytes),
                ),
                payload=row.payload_ref,
            )
        )
    return tuple(envelopes)


def _arrival_replay_envelopes(
    tables: Iterable[pa.Table | pa.RecordBatch],
    args: argparse.Namespace,
    job_id: str,
    operator: str,
    service_observation,
    trace_sink,
    lifecycle_seed_sink=None,
    packing_sink=None,
    quantum_sink=None,
    epoch_clock=None,
    service_endpoint_count: int = 1,
    replay_origin_epoch_s: float | None = None,
) -> Iterable[PayloadEnvelope]:
    completion_max_tokens = (
        args.completion_max_tokens if operator == "ai_complete" else 0
    )

    first_source_arrival_s: float | None = None
    replay_start_epoch_s: float | None = replay_origin_epoch_s
    arrival_time_scale = getattr(args, "arrival_time_scale", 1.0)
    replay_clock = getattr(args, "_replay_clock", None) or SystemReplayClock()
    lifecycle_epoch_clock = epoch_clock or MonotonicEpochClock()

    def rows() -> Iterable[RowArrival]:
        nonlocal first_source_arrival_s
        previous_arrival_s: float | None = None
        for table in tables:
            for arrival in _row_arrivals(
                table,
                completion_max_tokens,
                output_cost_mode=getattr(
                    args,
                    "output_cost_mode",
                    "fixed_output_cap",
                ),
            ):
                if (
                    previous_arrival_s is not None
                    and arrival.arrival_s < previous_arrival_s
                ):
                    raise ValueError(
                        "arrival_time_s values must be non-decreasing across fetch chunks"
                    )
                previous_arrival_s = arrival.arrival_s
                if first_source_arrival_s is None:
                    first_source_arrival_s = arrival.arrival_s
                yield arrival

    policies = {
        "immediate": lambda: ImmediateFlush(),
        "fixed_timeout": lambda: FixedTimeoutFlush(
            timeout_s=args.flush_timeout_ms / 1000.0
        ),
        "queue_adaptive": lambda: QueueAdaptiveFlush(
            min_wait_s=args.flush_timeout_ms / 1000.0,
            max_wait_s=args.flush_max_wait_ms / 1000.0,
            pressure_running=args.max_inflight,
        ),
        "slo_ewma": lambda: SloAwareEwmaFlush(
            min_wait_s=args.flush_timeout_ms / 1000.0,
            max_wait_s=args.flush_max_wait_ms / 1000.0,
            request_slo_s=args.request_slo_ms / 1000.0,
            ewma_alpha=args.flush_ewma_alpha,
            deadband_ratio=args.flush_deadband_ratio,
            endpoint_count=service_endpoint_count,
            service_capacity_tokens_s_per_endpoint=(
                getattr(
                    args,
                    "flush_service_capacity_tokens_s_per_endpoint",
                    0,
                )
                or None
            ),
        ),
    }
    try:
        flush_policy = policies[args.flush_policy]()
    except KeyError as exc:
        raise ValueError(f"unsupported flush policy: {args.flush_policy}") from exc

    def observe() -> ReplayServiceObservation:
        if not _requires_replay_feedback(args):
            return ReplayServiceObservation(
                fresh=False,
                running=None,
                waiting=None,
                kv_usage=None,
            )
        if hasattr(service_observation, "latest"):
            observation = service_observation.latest(0)
            return ReplayServiceObservation(
                fresh=observation.fresh,
                running=observation.running,
                waiting=observation.waiting,
                kv_usage=observation.kv_usage,
                service_rate_tokens_s_per_endpoint=(
                    observation.service_rate_tokens_s_per_endpoint
                ),
            )
        return service_observation()

    batch_index = 0

    submission_granularity = getattr(args, "submission_granularity", "batch")

    def close_batch(pending: PendingBatch) -> tuple[PayloadEnvelope, ...]:
        nonlocal batch_index
        if packing_sink is not None:
            packing_sink.append(
                (pending.estimated_total_tokens, pending.row_count)
            )
        envelope = _arrow_envelope(
            pending,
            batch_index=batch_index,
            job_id=str(job_id),
            operator=operator,
        )
        if submission_granularity == "request":
            closed_envelopes = _request_envelopes(
                pending,
                job_id=str(job_id),
                operator=operator,
                planning_batch_id=envelope.request.request_id,
            )
        elif submission_granularity == "service_quantum":
            closed_envelopes = _service_quantum_envelopes(
                envelope.payload,
                planning_batch_id=envelope.request.request_id,
                job_id=str(job_id),
                operator=operator,
                completion_max_tokens=completion_max_tokens,
                output_cost_mode=getattr(
                    args,
                    "output_cost_mode",
                    "fixed_output_cap",
                ),
                target_tokens=args.service_quantum_tokens,
            )
            if quantum_sink is not None:
                quantum_sink.extend(
                    (
                        item.request.estimated_total_tokens,
                        item.request.row_count,
                        item.request.service_quantum_oversized,
                    )
                    for item in closed_envelopes
                )
        else:
            closed_envelopes = (envelope,)
        row_submission_ids = [
            item.request.request_id
            for item in closed_envelopes
            for _ in range(item.request.row_count)
        ]
        if replay_start_epoch_s is None or first_source_arrival_s is None:
            raise RuntimeError("replay epoch origin is not initialized")
        intended_arrival_epochs = [
            replay_start_epoch_s
            + (row.arrival_s - first_source_arrival_s) * arrival_time_scale
            for row in pending.rows
        ]
        flush_epoch_s = lifecycle_epoch_clock()
        # Intended arrival timestamps are evidence and SLO inputs. Clamp them
        # to the observed flush boundary so scheduling never sees future age.
        arrival_epochs = [
            min(arrival_epoch_s, flush_epoch_s)
            for arrival_epoch_s in intended_arrival_epochs
        ]
        oldest_epoch_by_submission: dict[str, float] = {}
        for submission_id, arrival_epoch_s in zip(
            row_submission_ids,
            arrival_epochs,
        ):
            oldest_epoch_by_submission[submission_id] = min(
                oldest_epoch_by_submission.get(submission_id, arrival_epoch_s),
                arrival_epoch_s,
            )
        closed_envelopes = tuple(
            replace(
                item,
                request=replace(
                    item.request,
                    oldest_arrival_epoch_s=oldest_epoch_by_submission[
                        item.request.request_id
                    ],
                ),
            )
            for item in closed_envelopes
        )
        if lifecycle_seed_sink is not None:
            seeds = [
                RequestLifecycleSeed(
                    request_id=f"{job_id}:row:{row.row_id}",
                    submission_id=submission_id,
                    doc_id=row.row_id,
                    prompt_tokens=row.prompt_tokens,
                    estimated_output_tokens=row.estimated_output_tokens,
                    prefix_key=row.prefix_key,
                    arrival_epoch_s=arrival_epoch_s,
                    flush_epoch_s=flush_epoch_s,
                    request_time_origin="replayed_arrival",
                    latency_granularity=(
                        "request"
                        if submission_granularity == "request"
                        else "submission"
                    ),
                )
                for row, arrival_epoch_s, submission_id in zip(
                    pending.rows,
                    arrival_epochs,
                    row_submission_ids,
                )
            ]
            for seed in seeds:
                if callable(lifecycle_seed_sink):
                    lifecycle_seed_sink(seed)
                else:
                    lifecycle_seed_sink.append(seed)
        batch_index += 1
        return closed_envelopes

    token_budget = args.token_budget if args.batching_policy == "token_budget" else 0
    token_budget_policy = None
    if args.batching_policy == "token_budget":
        if getattr(args, "token_budget_policy", "static") == "static":
            token_budget_policy = StaticTokenBudgetController(token_budget)
        else:
            candidates = tuple(
                int(value.strip())
                for value in getattr(
                    args,
                    "token_budget_candidates",
                    str(token_budget),
                ).split(",")
                if value.strip()
            )
            token_budget_policy = ServiceQuantumTokenBudgetController(
                candidates,
                fallback_budget=token_budget,
                target_service_s=(
                    getattr(
                        args,
                        "token_budget_target_service_ms",
                        2000.0,
                    )
                    / 1000.0
                ),
                max_fill_wait_s=args.flush_max_wait_ms / 1000.0,
            )
    max_rows = (
        1
        if getattr(args, "strategy", "coalesced") == "fine"
        else args.ray_batch_rows
    )
    batcher = ArrivalReplayBatcher(
        rows=rows(),
        builder_factory=lambda: PendingBatchBuilder(
            max_rows=max_rows,
            token_budget=token_budget,
        ),
        flush_policy=flush_policy,
        close_batch=close_batch,
        service_observation=observe,
        clock=replay_clock,
        arrival_time_scale=arrival_time_scale,
        token_budget_policy=token_budget_policy,
        arrival_rate_ewma_alpha=getattr(
            args,
            "token_budget_arrival_ewma_alpha",
            0.3,
        ),
    )

    def replay() -> Iterable[PayloadEnvelope]:
        nonlocal replay_start_epoch_s
        if replay_start_epoch_s is None:
            replay_start_epoch_s = lifecycle_epoch_clock()
        try:
            for closed_envelopes in batcher:
                yield from closed_envelopes
        finally:
            for event in batcher.trace:
                if callable(trace_sink):
                    trace_sink(event)
                else:
                    trace_sink.append(event)

    return replay()
