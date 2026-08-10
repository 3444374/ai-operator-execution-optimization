"""Build deterministic upstream traces compatible with two VTC workloads.

This module preserves VTC's client counts, rates, on/off shape, and 256-token
work unit while using database-owned prompts. It is not an S-LoRA/VTC runtime
reproduction and does not implement VTC inside continuous batching.
"""

from __future__ import annotations

import hashlib
import math
import random
from dataclasses import dataclass
from pathlib import Path

from src.baselines.common.contracts import ChatRequest
from src.baselines.common.manifests import assign_endpoint_equal_rows
from src.baselines.text.orchestration.postgres_manifest import source_row_hash


OFFICIAL_VTC_ARTIFACT_COMMIT = "192c2e2014c69c8c6c699d7113c3822e4db632e6"


@dataclass(frozen=True)
class VtcSuiteSpec:
    suite_id: str
    rates_per_s: tuple[float, ...]
    duration_s: float
    official_duration_s: float
    input_tokens: int = 256
    output_tokens: int = 256
    on_off_period_s: float | None = None
    on_off_clients: tuple[int, ...] = ()


@dataclass(frozen=True)
class VtcSourceRow:
    doc_id: int
    tenant_id: int
    category: str
    text: str
    prompt_tokens: int
    session_id: str
    prefix_key: str


@dataclass(frozen=True)
class VtcMaterializedRow:
    doc_id: int
    tenant_id: int
    category: str
    text: str
    workload_name: str
    prompt_tokens: int
    target_output_tokens: int
    arrival_time_s: float
    session_id: str
    prefix_key: str
    client_index: int
    source_doc_id: int


def suite_spec(suite_id: str, *, duration_s: float | None = None) -> VtcSuiteSpec:
    """Return the preregistered local duration and official workload shape."""
    if suite_id == "on_off_overload":
        spec = VtcSuiteSpec(
            suite_id=suite_id,
            rates_per_s=(2.0, 3.0),
            duration_s=240.0,
            official_duration_s=600.0,
            on_off_period_s=60.0,
            on_off_clients=(0,),
        )
    elif suite_id == "overload_multi":
        spec = VtcSuiteSpec(
            suite_id=suite_id,
            rates_per_s=(0.4, 0.4, 0.4, 0.6, 0.6, 0.6, 0.6, 0.6),
            duration_s=180.0,
            official_duration_s=360.0,
        )
    else:
        raise ValueError(f"unsupported VTC-compatible suite: {suite_id}")
    if duration_s is None:
        return spec
    if not math.isfinite(duration_s) or duration_s <= 0:
        raise ValueError("duration_s must be finite and positive")
    if suite_id == "on_off_overload" and duration_s < 240.0:
        raise ValueError("on_off_overload requires two complete 120s cycles")
    if suite_id == "overload_multi" and duration_s < 180.0:
        raise ValueError("overload_multi requires at least 180s")
    return VtcSuiteSpec(**{**spec.__dict__, "duration_s": float(duration_s)})


def _client_arrivals(spec: VtcSuiteSpec, client_index: int, seed: int) -> tuple[float, ...]:
    rate = spec.rates_per_s[client_index]
    rng = random.Random(seed + 104729 * client_index)
    arrivals: list[float] = []
    interval_start = 0.0
    period = spec.on_off_period_s
    while interval_start < spec.duration_s:
        if period is None or client_index not in spec.on_off_clients:
            interval_end = spec.duration_s
            active = True
        else:
            interval_index = int(interval_start // period)
            interval_end = min(spec.duration_s, (interval_index + 1) * period)
            active = interval_index % 2 == 0
        if active:
            arrival = interval_start
            while True:
                arrival += rng.expovariate(rate)
                if arrival >= interval_end:
                    break
                arrivals.append(arrival)
        interval_start = interval_end
    return tuple(arrivals)


def _source_order(rows: tuple[VtcSourceRow, ...], target: int, seed: int) -> list[VtcSourceRow]:
    def key(row: VtcSourceRow) -> tuple[int, str, int]:
        digest = hashlib.sha256(f"{seed}:{row.doc_id}".encode()).hexdigest()
        return (abs(row.prompt_tokens - target), digest, row.doc_id)

    return sorted(rows, key=key)


def build_suite(
    source_rows: tuple[VtcSourceRow, ...],
    *,
    spec: VtcSuiteSpec,
    workload_name: str,
    doc_id_base: int,
    seed: int,
    endpoint_count: int,
) -> tuple[tuple[VtcMaterializedRow, ...], tuple[tuple[ChatRequest, ...], ...]]:
    """Materialize disjoint database rows and one immutable manifest per client."""
    if not workload_name or doc_id_base < 0 or endpoint_count <= 0:
        raise ValueError("workload name, doc-id base, and endpoint count are invalid")
    arrivals = tuple(
        _client_arrivals(spec, client_index, seed)
        for client_index in range(len(spec.rates_per_s))
    )
    events = sorted(
        (arrival, client_index, request_index)
        for client_index, client_arrivals in enumerate(arrivals)
        for request_index, arrival in enumerate(client_arrivals)
    )
    if len(source_rows) < len(events):
        raise ValueError(
            f"content pool has {len(source_rows)} rows but trace needs {len(events)}"
        )
    if len({row.doc_id for row in source_rows}) != len(source_rows):
        raise ValueError("content pool contains duplicate doc_id values")
    selected = _source_order(source_rows, spec.input_tokens, seed)[: len(events)]
    by_client: list[list[VtcMaterializedRow]] = [
        [] for _ in spec.rates_per_s
    ]
    for global_index, ((arrival, client_index, _request_index), source) in enumerate(
        zip(events, selected)
    ):
        by_client[client_index].append(
            VtcMaterializedRow(
                doc_id=doc_id_base + global_index,
                tenant_id=source.tenant_id,
                category=source.category,
                text=source.text,
                workload_name=workload_name,
                prompt_tokens=source.prompt_tokens,
                target_output_tokens=spec.output_tokens,
                arrival_time_s=arrival,
                session_id=f"vtc-client-{client_index}",
                prefix_key=source.prefix_key,
                client_index=client_index,
                source_doc_id=source.doc_id,
            )
        )
    materialized = tuple(tuple(rows) for rows in by_client)
    manifests = []
    for rows in materialized:
        requests = tuple(
            ChatRequest(
                doc_id=row.doc_id,
                prompt=row.text,
                arrival_time_s=row.arrival_time_s,
                prompt_tokens=row.prompt_tokens,
                max_output_tokens=spec.output_tokens,
                estimated_output_tokens=spec.output_tokens,
                source_row_hash=source_row_hash(
                    workload_name=row.workload_name,
                    doc_id=row.doc_id,
                    prompt=row.text,
                    arrival_time_s=row.arrival_time_s,
                    prompt_tokens=row.prompt_tokens,
                    target_output_tokens=row.target_output_tokens,
                ),
                endpoint_index=-1,
            )
            for row in rows
        )
        manifests.append(assign_endpoint_equal_rows(requests, endpoint_count, seed))
    return materialized, tuple(manifests)


def runner_environment(
    audit: dict[str, object],
    contract_dir: Path,
) -> dict[str, str]:
    """Resolve fail-closed config variables from one preparation audit."""
    if audit.get("status") != "prepared":
        raise ValueError("VTC-compatible audit status must be prepared")
    suite = audit.get("suite")
    if not isinstance(suite, dict):
        raise ValueError("VTC-compatible audit suite is missing")
    suite_id = str(suite.get("suite_id", ""))
    prefix = {
        "on_off_overload": "VTC_ON_OFF",
        "overload_multi": "VTC_OVERLOAD",
    }.get(suite_id)
    if prefix is None:
        raise ValueError("VTC-compatible audit suite is unsupported")
    counts = audit.get("job_row_counts")
    offsets = audit.get("job_first_arrival_s")
    expected = len(suite.get("rates_per_s", []))
    if (
        not isinstance(counts, list)
        or not isinstance(offsets, list)
        or len(counts) != expected
        or len(offsets) != expected
    ):
        raise ValueError("VTC-compatible audit Job counts/offsets are invalid")
    environment = {
        f"{prefix}_WORKLOAD": str(audit.get("target_workload", "")),
    }
    if not environment[f"{prefix}_WORKLOAD"]:
        raise ValueError("VTC-compatible target workload is missing")
    for index, (count, offset) in enumerate(zip(counts, offsets)):
        if int(count) <= 0 or float(offset) < 0:
            raise ValueError("VTC-compatible Job count/offset is invalid")
        manifest = (contract_dir / f"client_{index}.jsonl").resolve()
        if not manifest.is_file():
            raise ValueError(f"VTC-compatible manifest is missing: {manifest}")
        environment[f"{prefix}_CLIENT{index}_ROWS"] = str(int(count))
        environment[f"{prefix}_CLIENT{index}_OFFSET_S"] = str(float(offset))
        environment[f"{prefix}_CLIENT{index}_MANIFEST"] = str(manifest)
    return environment
