#!/usr/bin/env python3
"""Prepare an immutable project-derived two-Job phase-change workload.

This is not an official VTC reproduction. It creates an OFF-first low/high
arrival trace with one global output cap, writes canonical request manifests,
and imports PostgreSQL rows only when ``--apply`` is explicit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import statistics
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.contracts import ChatRequest  # noqa: E402
from src.baselines.common.manifests import (  # noqa: E402
    assign_endpoint_equal_rows,
    partition_summary,
    write_manifest,
)
from src.baselines.text.orchestration.postgres_manifest import (  # noqa: E402
    source_row_hash,
)
from src.experiments.phase_change.contract import DERIVED_NOTE  # noqa: E402


@dataclass(frozen=True)
class PhaseChangeSpec:
    rate_a: float
    rate_b: float
    duration_s: float = 240.0
    period_s: float = 60.0
    short_target_tokens: int = 256
    long_target_tokens: int = 1024
    short_max_distance: int = 64
    long_max_distance: int = 256
    output_cap: int = 512

    def __post_init__(self) -> None:
        finite_positive = (
            self.rate_a,
            self.rate_b,
            self.duration_s,
            self.period_s,
        )
        if any(not math.isfinite(value) or value <= 0 for value in finite_positive):
            raise ValueError("rates, duration, and period must be finite and positive")
        if not math.isclose(self.duration_s, 4 * self.period_s):
            raise ValueError("duration must contain exactly two complete OFF/ON cycles")
        if self.short_target_tokens <= 0 or self.long_target_tokens <= 0:
            raise ValueError("prompt-token targets must be positive")
        if self.short_max_distance < 0 or self.long_max_distance < 0:
            raise ValueError("prompt-token distances must be non-negative")
        if self.output_cap <= 0:
            raise ValueError("output cap must be positive")


@dataclass(frozen=True)
class SourceRow:
    doc_id: int
    tenant_id: int
    category: str
    text: str
    prompt_tokens: int
    session_id: str
    prefix_key: str


@dataclass(frozen=True)
class MaterializedRow:
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


def phase_segments(
    duration_s: float,
    period_s: float,
) -> tuple[tuple[float, float, bool], ...]:
    """Return consecutive ``(start, end, job_b_active)`` OFF-first phases."""
    if (
        not math.isfinite(duration_s)
        or not math.isfinite(period_s)
        or duration_s <= 0
        or period_s <= 0
    ):
        raise ValueError("phase duration and period must be finite and positive")
    segments = []
    start = 0.0
    index = 0
    while start < duration_s:
        end = min(duration_s, start + period_s)
        segments.append((start, end, index % 2 == 1))
        start = end
        index += 1
    return tuple(segments)


def poisson_arrivals(
    rate: float,
    start_s: float,
    end_s: float,
    seed: int,
) -> tuple[float, ...]:
    """Generate deterministic Poisson arrivals strictly inside one interval."""
    if not math.isfinite(rate) or rate <= 0:
        raise ValueError("Poisson rate must be finite and positive")
    if not math.isfinite(start_s) or not math.isfinite(end_s) or end_s <= start_s:
        raise ValueError("Poisson interval must be finite and non-empty")
    rng = random.Random(seed)
    current = start_s
    arrivals = []
    while True:
        current += rng.expovariate(rate)
        if current >= end_s:
            break
        arrivals.append(current)
    return tuple(arrivals)


def _select_source_rows(
    rows: Sequence[SourceRow],
    *,
    target_tokens: int,
    max_distance: int,
    count: int,
    seed: int,
    label: str,
) -> tuple[SourceRow, ...]:
    eligible = [
        row
        for row in rows
        if abs(row.prompt_tokens - target_tokens) <= max_distance
    ]
    eligible.sort(
        key=lambda row: (
            abs(row.prompt_tokens - target_tokens),
            hashlib.sha256(f"{seed}:{row.doc_id}".encode("utf-8")).hexdigest(),
            row.doc_id,
        )
    )
    if len({row.doc_id for row in eligible}) != len(eligible):
        raise ValueError(f"{label} source pool contains duplicate doc_id values")
    if len(eligible) < count:
        raise ValueError(
            f"{label} source pool has {len(eligible)} exact-distance rows but "
            f"trace needs {count}; stop without repeating or loosening"
        )
    return tuple(eligible[:count])


def build_phase_change(
    short_source_rows: Sequence[SourceRow],
    long_source_rows: Sequence[SourceRow],
    *,
    spec: PhaseChangeSpec,
    workload_name: str,
    doc_id_base: int,
    seed: int,
    endpoint_count: int,
) -> tuple[
    tuple[tuple[MaterializedRow, ...], ...],
    tuple[tuple[ChatRequest, ...], ...],
]:
    """Build disjoint database rows and canonical requests for both Jobs."""
    if not workload_name or doc_id_base < 0 or endpoint_count <= 0:
        raise ValueError("workload, doc-id base, and endpoint count are invalid")
    segments = phase_segments(spec.duration_s, spec.period_s)
    arrivals_a = poisson_arrivals(spec.rate_a, 0.0, spec.duration_s, seed)
    arrivals_b = tuple(
        arrival
        for index, (start, end, active) in enumerate(segments)
        if active
        for arrival in poisson_arrivals(
            spec.rate_b,
            start,
            end,
            seed + 104729 + index,
        )
    )
    if not arrivals_a or not arrivals_b:
        raise ValueError("both Jobs must contain at least one arrival")
    selected_a = _select_source_rows(
        short_source_rows,
        target_tokens=spec.short_target_tokens,
        max_distance=spec.short_max_distance,
        count=len(arrivals_a),
        seed=seed,
        label="short",
    )
    selected_b = _select_source_rows(
        long_source_rows,
        target_tokens=spec.long_target_tokens,
        max_distance=spec.long_max_distance,
        count=len(arrivals_b),
        seed=seed + 7,
        label="long",
    )
    overlap = {row.doc_id for row in selected_a} & {
        row.doc_id for row in selected_b
    }
    if overlap:
        raise ValueError("short and long source selections overlap")

    selected = (selected_a, selected_b)
    arrivals = (arrivals_a, arrivals_b)
    positions = [0, 0]
    by_client: list[list[MaterializedRow]] = [[], []]
    events = sorted(
        (arrival, client_index)
        for client_index, client_arrivals in enumerate(arrivals)
        for arrival in client_arrivals
    )
    for global_index, (arrival, client_index) in enumerate(events):
        source = selected[client_index][positions[client_index]]
        positions[client_index] += 1
        by_client[client_index].append(
            MaterializedRow(
                doc_id=doc_id_base + global_index,
                tenant_id=source.tenant_id,
                category=source.category,
                text=source.text,
                workload_name=workload_name,
                prompt_tokens=source.prompt_tokens,
                target_output_tokens=spec.output_cap,
                arrival_time_s=arrival,
                session_id=f"phase-client-{client_index}",
                prefix_key=source.prefix_key,
                client_index=client_index,
                source_doc_id=source.doc_id,
            )
        )
    jobs = tuple(tuple(rows) for rows in by_client)
    manifests = tuple(
        assign_endpoint_equal_rows(
            (
                ChatRequest(
                    doc_id=row.doc_id,
                    prompt=row.text,
                    arrival_time_s=row.arrival_time_s,
                    prompt_tokens=row.prompt_tokens,
                    max_output_tokens=spec.output_cap,
                    estimated_output_tokens=spec.output_cap,
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
            ),
            endpoint_count,
            seed,
        )
        for rows in jobs
    )
    return jobs, manifests


def _load_source_rows(
    connection,
    workload: str,
    *,
    target_tokens: int,
    max_distance: int,
) -> tuple[SourceRow, ...]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT doc_id, tenant_id, category, text, prompt_tokens,
                   COALESCE(session_id, ''), COALESCE(prefix_key, '')
            FROM documents
            WHERE workload_name = %s
              AND prompt_tokens BETWEEN %s AND %s
            ORDER BY doc_id
            """,
            (
                workload,
                target_tokens - max_distance,
                target_tokens + max_distance,
            ),
        )
        return tuple(SourceRow(*row) for row in cursor.fetchall())


def _prepare_destination(destination: Path) -> None:
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("output directory must be absent or empty")
    destination.mkdir(parents=True, exist_ok=True)


def _flatten_jobs(
    jobs: Iterable[Iterable[MaterializedRow]],
) -> list[MaterializedRow]:
    return [row for job in jobs for row in job]


def _insert_rows(connection, target_workload: str, jobs) -> dict[str, object]:
    rows = _flatten_jobs(jobs)
    doc_ids = [row.doc_id for row in rows]
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM documents WHERE workload_name = %s",
            (target_workload,),
        )
        if int(cursor.fetchone()[0]) != 0:
            raise ValueError(
                "target workload already exists; choose a new immutable name"
            )
        cursor.execute(
            "SELECT doc_id FROM documents WHERE doc_id = ANY(%s) LIMIT 1",
            (doc_ids,),
        )
        collision = cursor.fetchone()
        if collision is not None:
            raise ValueError(f"prepared doc_id collides with existing row: {collision[0]}")
        cursor.executemany(
            """
            INSERT INTO documents (
                doc_id, tenant_id, category, text, workload_name, prompt_tokens,
                target_output_tokens, arrival_time_s, session_id, prefix_key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    row.doc_id,
                    row.tenant_id,
                    row.category,
                    row.text,
                    row.workload_name,
                    row.prompt_tokens,
                    row.target_output_tokens,
                    row.arrival_time_s,
                    row.session_id,
                    row.prefix_key,
                )
                for row in rows
            ],
        )
        cursor.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT doc_id),
                   MIN(target_output_tokens), MAX(target_output_tokens)
            FROM documents WHERE workload_name = %s
            """,
            (target_workload,),
        )
        count, distinct_count, output_min, output_max = cursor.fetchone()
        if (
            int(count) != len(rows)
            or int(distinct_count) != len(rows)
            or int(output_min) != rows[0].target_output_tokens
            or int(output_max) != rows[0].target_output_tokens
        ):
            raise RuntimeError("database import verification failed")
        cursor.execute("SHOW server_version")
        server_version = str(cursor.fetchone()[0])
        cursor.execute(
            "SELECT extversion FROM pg_extension WHERE extname = 'vector'"
        )
        vector_row = cursor.fetchone()
    connection.commit()
    return {
        "schema_version": 1,
        "status": "imported",
        "target_workload": target_workload,
        "inserted_rows": len(rows),
        "distinct_doc_ids": len(rows),
        "doc_id_range": [min(doc_ids), max(doc_ids)],
        "output_cap": rows[0].target_output_tokens,
        "server_version": server_version,
        "pgvector_version": str(vector_row[0]) if vector_row else "not_installed",
    }


def _distance_summary(values: Sequence[int]) -> dict[str, float | int]:
    ordered = sorted(values)
    p95_index = min(len(ordered) - 1, math.ceil(0.95 * len(ordered)) - 1)
    return {
        "count": len(ordered),
        "p50": float(statistics.median(ordered)),
        "p95": float(ordered[p95_index]),
        "max": int(ordered[-1]),
    }


def _write_contract(
    destination: Path,
    *,
    jobs,
    manifests,
    spec: PhaseChangeSpec,
    short_source: str,
    long_source: str,
    target_workload: str,
    doc_id_base: int,
    endpoint_count: int,
    seed: int,
) -> dict[str, object]:
    rows = _flatten_jobs(jobs)
    workload_payload = "".join(
        json.dumps(
            asdict(row),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
        for row in rows
    )
    (destination / "workload.jsonl").write_text(
        workload_payload,
        encoding="utf-8",
    )
    manifest_metadata = []
    for index, manifest in enumerate(manifests):
        path = destination / f"client_{index}.jsonl"
        metadata = write_manifest(path, manifest)
        manifest_metadata.append(
            {
                "client_index": index,
                "path": str(path.resolve()),
                **asdict(metadata),
                "partition": partition_summary(manifest, endpoint_count),
            }
        )
    segments = phase_segments(spec.duration_s, spec.period_s)
    audit = {
        "schema_version": 1,
        "status": "prepared",
        "label": DERIVED_NOTE,
        "target_workload": target_workload,
        "source_workloads": [short_source, long_source],
        "spec": asdict(spec),
        "phase_segments": [
            {
                "start_s": start,
                "end_s": end,
                "job_b_active": active,
            }
            for start, end, active in segments
        ],
        "arrival_time_scale": 1.0,
        "seed": seed,
        "endpoint_count": endpoint_count,
        "doc_id_base": doc_id_base,
        "doc_id_range": [min(row.doc_id for row in rows), max(row.doc_id for row in rows)],
        "job_row_counts": [len(job) for job in jobs],
        "job_first_arrival_s": [job[0].arrival_time_s for job in jobs],
        "job_prompt_token_distance": [
            _distance_summary(
                [
                    abs(
                        row.prompt_tokens
                        - (
                            spec.short_target_tokens
                            if index == 0
                            else spec.long_target_tokens
                        )
                    )
                    for row in job
                ]
            )
            for index, job in enumerate(jobs)
        ],
        "source_doc_id_overlap": 0,
        "total_rows": len(rows),
        "workload_sha256": hashlib.sha256(
            workload_payload.encode("utf-8")
        ).hexdigest(),
        "manifests": manifest_metadata,
    }
    (destination / "audit.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return audit


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--short-source", default="squad_v11_dev_short_answer")
    parser.add_argument("--long-source", default="sharegpt_concentrated")
    parser.add_argument("--target-workload", required=True)
    parser.add_argument("--doc-id-base", type=int, required=True)
    parser.add_argument("--rate-a", type=float, required=True)
    parser.add_argument("--rate-b", type=float, required=True)
    parser.add_argument("--short-target", type=int, default=256)
    parser.add_argument("--long-target", type=int, default=1024)
    parser.add_argument("--short-max-dist", type=int, default=64)
    parser.add_argument("--long-max-dist", type=int, default=256)
    parser.add_argument("--output-cap", type=int, default=512)
    parser.add_argument("--duration-s", type=float, default=240.0)
    parser.add_argument("--period-s", type=float, default=60.0)
    parser.add_argument("--endpoint-count", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20260811)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Import prepared rows into PostgreSQL after writing the contract",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    spec = PhaseChangeSpec(
        rate_a=args.rate_a,
        rate_b=args.rate_b,
        duration_s=args.duration_s,
        period_s=args.period_s,
        short_target_tokens=args.short_target,
        long_target_tokens=args.long_target,
        short_max_distance=args.short_max_dist,
        long_max_distance=args.long_max_dist,
        output_cap=args.output_cap,
    )
    destination = args.output_dir.resolve()
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("output directory must be absent or empty")
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("psycopg is required for workload preparation") from exc
    with psycopg.connect(args.database_url) as connection:
        short_rows = _load_source_rows(
            connection,
            args.short_source,
            target_tokens=spec.short_target_tokens,
            max_distance=spec.short_max_distance,
        )
        long_rows = _load_source_rows(
            connection,
            args.long_source,
            target_tokens=spec.long_target_tokens,
            max_distance=spec.long_max_distance,
        )
    jobs, manifests = build_phase_change(
        short_rows,
        long_rows,
        spec=spec,
        workload_name=args.target_workload,
        doc_id_base=args.doc_id_base,
        seed=args.seed,
        endpoint_count=args.endpoint_count,
    )
    _prepare_destination(destination)
    audit = _write_contract(
        destination,
        jobs=jobs,
        manifests=manifests,
        spec=spec,
        short_source=args.short_source,
        long_source=args.long_source,
        target_workload=args.target_workload,
        doc_id_base=args.doc_id_base,
        endpoint_count=args.endpoint_count,
        seed=args.seed,
    )
    if args.apply:
        with psycopg.connect(args.database_url) as connection:
            receipt = _insert_rows(connection, args.target_workload, jobs)
        (destination / "database_import_receipt.json").write_text(
            json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(audit, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
