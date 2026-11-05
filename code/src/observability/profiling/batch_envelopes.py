"""Arrow batch envelopes for the maintained Ray profiling entry points."""

from __future__ import annotations

from collections.abc import Iterable

import pyarrow as pa

from src.modalities.text.costs import OutputCostMode, resolve_output_tokens
from src.scheduling.core.models import BatchRequest, PayloadEnvelope


def _batch_envelopes(
    batches: Iterable[pa.RecordBatch | pa.Table],
    job_id: str,
    operator: str,
    completion_max_tokens: int,
    output_cost_mode: OutputCostMode = "fixed_output_cap",
    batch_index_start: int = 0,
    prompt_token_overhead_per_request: int = 0,
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
                prompt_token_overhead_per_request=(
                    prompt_token_overhead_per_request
                ),
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
    prompt_token_overhead_per_request: int = 0,
) -> PayloadEnvelope:
    if prompt_token_overhead_per_request < 0:
        raise ValueError("prompt token overhead must be non-negative")
    prompt_tokens = sum(
        _row_prompt_tokens(batch, row_index)
        for row_index in range(batch.num_rows)
    ) + prompt_token_overhead_per_request * batch.num_rows
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
