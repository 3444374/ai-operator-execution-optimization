"""Arrow payload and arrival-replay assembly for profiler runs."""

from __future__ import annotations

import argparse
import math
from collections.abc import Iterable

import pyarrow as pa

from ..request_costs import OutputCostMode, resolve_output_tokens
from ..scheduling.batching import (
    ArrivalReplayBatcher,
    PendingBatch,
    PendingBatchBuilder,
    ReplayServiceObservation,
    RowArrival,
    SystemReplayClock,
)
from ..scheduling.flush import FixedTimeoutFlush, ImmediateFlush, QueueAdaptiveFlush
from ..scheduling.lifecycle import MonotonicEpochClock, RequestLifecycleSeed
from ..scheduling.models import BatchRequest, PayloadEnvelope
from ..scheduling.organization.token_budget import (
    ServiceQuantumTokenBudgetController,
    StaticTokenBudgetController,
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
        prompt_tokens = 0
        if "prompt_tokens" in batch.column_names:
            prompt_tokens = sum(
                int(value.as_py() or 0) for value in batch.column("prompt_tokens")
            )
        prefix_key = ""
        if "prefix_key" in batch.column_names and batch.num_rows:
            prefix_values = {
                str(value.as_py() or "") for value in batch.column("prefix_key")
            }
            if len(prefix_values) == 1:
                prefix_key = prefix_values.pop()
        arrival_times = []
        if "arrival_time_s" in batch.column_names:
            arrival_times = [
                float(value.as_py())
                for value in batch.column("arrival_time_s")
                if value.as_py() is not None
            ]
        oldest_arrival_s = min(arrival_times, default=0.0)
        request = BatchRequest(
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
        )
        envelopes.append(PayloadEnvelope(request=request, payload=batch))
    return envelopes


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
) -> tuple[list[PayloadEnvelope], list[RequestLifecycleSeed]]:
    materialized_batches = list(batches)
    envelopes = _batch_envelopes(
        materialized_batches,
        job_id=job_id,
        operator=operator,
        completion_max_tokens=completion_max_tokens,
        output_cost_mode=output_cost_mode,
        batch_index_start=batch_index_start,
    )
    seeds = []
    for batch, envelope in zip(materialized_batches, envelopes):
        if "doc_id" not in batch.column_names:
            raise ValueError("doc_id column is required for request tracing")
        for row_index in range(batch.num_rows):
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
                    submission_id=envelope.request.request_id,
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
                )
            )
    return envelopes, seeds


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
        ),
        payload=payload,
    )


def _request_envelopes(
    pending: PendingBatch,
    *,
    job_id: str,
    operator: str,
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
    epoch_clock=None,
) -> Iterable[PayloadEnvelope]:
    completion_max_tokens = (
        args.completion_max_tokens if operator == "ai_complete" else 0
    )

    first_source_arrival_s: float | None = None
    replay_start_epoch_s: float | None = None
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
    }
    try:
        flush_policy = policies[args.flush_policy]()
    except KeyError as exc:
        raise ValueError(f"unsupported flush policy: {args.flush_policy}") from exc

    def observe() -> ReplayServiceObservation:
        needs_feedback = (
            args.flush_policy == "queue_adaptive"
            or getattr(args, "token_budget_policy", "static")
            == "service_quantum"
        )
        if not needs_feedback:
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
        closed_envelopes = (
            _request_envelopes(
                pending,
                job_id=str(job_id),
                operator=operator,
            )
            if submission_granularity == "request"
            else (envelope,)
        )
        if lifecycle_seed_sink is not None:
            if replay_start_epoch_s is None or first_source_arrival_s is None:
                raise RuntimeError("replay epoch origin is not initialized")
            intended_arrival_epochs = [
                replay_start_epoch_s
                + (row.arrival_s - first_source_arrival_s)
                * arrival_time_scale
                for row in pending.rows
            ]
            flush_epoch_s = lifecycle_epoch_clock()
            # The replay clock and epoch-shaped lifecycle clock are separate
            # monotonic domains. Scheduler jitter can make an intended replay
            # deadline a few milliseconds later than the epoch observed when
            # the batch actually closes. Request traces record observed
            # lifecycle times, so clamp such intended arrivals at the observed
            # flush boundary instead of pushing flush into the future and
            # making the subsequent submit timestamp appear to precede it.
            arrival_epochs = [
                min(arrival_epoch_s, flush_epoch_s)
                for arrival_epoch_s in intended_arrival_epochs
            ]
            seeds = [
                RequestLifecycleSeed(
                    request_id=f"{job_id}:row:{row.row_id}",
                    submission_id=(
                        f"{job_id}:request:{row.row_id}"
                        if submission_granularity == "request"
                        else envelope.request.request_id
                    ),
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
                for row, arrival_epoch_s in zip(
                    pending.rows,
                    arrival_epochs,
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
