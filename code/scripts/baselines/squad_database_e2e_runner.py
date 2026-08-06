#!/usr/bin/env python3
"""SQuAD bounded-output **database-E2E** runner (top-level, DuckDB-ai arm first).

The capability gate (`squad_capability_gate.py`) measures only the OPERATOR-ONLY
boundary (prompts ready → DuckDB `ai` op → materialize). This runner adds the
missing **database-E2E** boundary defined in
`experiments/plans/bounded_output_duckdb_comparison_protocol_20260805.md` §3:
one timed wall around persistent-table scan → prompt construction → model call
→ **unified sink**. It is the prerequisite for any formal database-system
ranking (protocol §5 step 4).

Scope (codex ruling): the runner unifies PostgreSQL source / prompt / cap=64 /
model service config / sink across arms; it does ONLY static sharding, timing
(both boundaries), audit, writeback. The DuckDB `ai` extension keeps owning
batching/concurrency/retry/cache. NO project credit / actor pool / dynamic
backpressure is injected. `correct rows/s` (not raw `rows/s`) is the primary
headline; raw `rows/s` is reported but never the ranking key.

Timing contract: E2E timing is summary-level (the report's `timing` block) --
`BaselineRequestResult` is NOT extended (its operator-only timestamps stay
intact for the per-row CSV). ``database_e2e_wall_s = scan_s + construct_s +
adapter_wall_s + sink_s`` (metrics settle + after-scrape are outside the wall).

State fields are DECOUPLED per codex audit: ``single_run_valid`` (this shot:
0 error/NULL), ``formal_run_gate_passed`` (always False from this single-shot
runner -- the 1-warmup+3-formal repeat gate is a separate protocol),
``comparison_admission`` ("pending_formal_repeat" -- a single shot cannot
confer/exclude formal admission). ``failure_rate`` is the de-duplicated
row-level rate (a row that is both error AND NULL counts once); error/null/
max_tokens rates are reported separately and may overlap. ``operator_only_jct``
is the full result span (min started -> max completed), correct for both
barrier arms (DuckDB-ai) and per-request arms (direct_client). The zero-error
validity check is NOT weakened -- failed cells are fully retained and still
feed EM/F1, failure rate, and successful/correct rows/s.

Arms: ``duckdb_ai`` (in-process, reuses ``run_duckdb_ai_complete``) and
``direct_client`` (in-process per-request HTTP) are implemented. ``project_static``
is implemented as a SHELL-OUT: ``_run_project_static`` branches before the common
scan and delegates scan+operator+sink to ``postgres_ai_operator_profile.py``. The
profiler emits independent completion evidence and fingerprints of the exact
source rows handed to the organizer. The runner's post-run database read supplies
references, checks importer integrity, and must match those exact-scan fingerprints;
it never feeds a second scan to the model or performs a second sink.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import time
import traceback
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.contracts import ChatRequest  # noqa: E402
from src.baselines.common.provenance import adapter_provenance  # noqa: E402
from src.baselines.common.redact import (  # noqa: E402
    redact_argument_list,
    redact_database_url,
    redact_text,
)
from src.baselines.common.squad_identity import (  # noqa: E402
    EXPECTED_DEV_COUNT,
    _assess_attribution,
    _delta,
    _endpoint_base_url,
    _endpoint_idle,
    _git_commit,
    _gpu_identity,
    _load_importer_provenance,
    _pg_server_identity,
    _scrape_status,
    _structured_content_hash,
    _validate_workload_integrity,
    _vllm_version,
)
from src.baselines.text.products.direct_client import (  # noqa: E402
    DirectClientConfig,
    run_direct_client,
)
from src.baselines.text.products.project_static import (  # noqa: E402
    ProjectStaticConfig,
    run_project_static,
)
from src.baselines.text.products.duckdb_ai import (  # noqa: E402
    DuckDBAiConfig,
    inspect_duckdb_ai_runtime,
    run_duckdb_ai_complete,
)
from src.data.sinks.postgres import write_completions  # noqa: E402
from src.observability.metrics import (  # noqa: E402
    scrape_prometheus_metrics,
    squad_quality_metrics,
)


def _scan_workload(conn, workload: str, limit: int = 0) -> tuple[list[dict], dict[int, tuple], float]:
    """Timed persistent-table scan. Returns (rows, doc_id->(tenant,category), scan_s).

    Rows carry everything downstream needs: ChatRequest build (doc_id/text),
    quality (source_example_id/reference_answers), and the sink sidecar
    (tenant_id/category). The scan is the unified front-end for every arm.
    ``limit > 0`` scans only that many rows (smoke / small-scale gate); the
    full-workload integrity check is then relaxed by the caller.
    """

    import psycopg
    t0 = time.time()
    with conn.cursor() as cur:
        if limit > 0:
            cur.execute(
                "SELECT doc_id, text, tenant_id, category, source_example_id, "
                "reference_answers FROM documents WHERE workload_name = %s "
                "ORDER BY doc_id LIMIT %s",
                (workload, limit),
            )
        else:
            cur.execute(
                "SELECT doc_id, text, tenant_id, category, source_example_id, "
                "reference_answers FROM documents WHERE workload_name = %s "
                "ORDER BY doc_id",
                (workload,),
            )
        fetched = cur.fetchall()
    rows: list[dict] = []
    sidecar: dict[int, tuple] = {}
    for doc_id, text, tenant_id, category, source_example_id, reference_answers in fetched:
        answers = reference_answers
        if isinstance(answers, str):
            answers = json.loads(answers)
        rows.append(
            {
                "doc_id": doc_id,
                "text": text,
                "source_example_id": source_example_id,
                "answers": list(answers),
            }
        )
        sidecar[doc_id] = (tenant_id, category)
    return rows, sidecar, time.time() - t0


def _fetch_workload_integrity_and_scoring(
    conn, workload: str, limit: int, importer_count: int, expected_hash: str,
) -> tuple[
    dict[int, str], dict[str, list[str]], dict[int, str], str, str, bool, list[str]
]:
    """Read the workload ONCE for integrity + scoring (project_static arm).

    Returns ``(doc_id -> source_example_id, source_example_id -> reference_answers,
    doc_id -> text_sha256, structured_content_hash, integrity_label, integrity_ok,
    problems)``. This post-run read is NOT the operator scan. Its prompt
    fingerprints are compared with profiler evidence captured from the exact
    Arrow rows handed to the organizer; the structured hash independently checks
    the database snapshot (including references) against importer provenance.
    For full runs the hash
    MUST equal the importer's; for ``--limit`` smoke it is the subset hash (not
    comparable to the importer, recorded as such).
    """

    with conn.cursor() as cur:
        if limit > 0:
            cur.execute(
                "SELECT doc_id, text, source_example_id, reference_answers "
                "FROM documents WHERE workload_name = %s "
                "ORDER BY doc_id LIMIT %s",
                (workload, min(limit, importer_count)),
            )
        else:
            cur.execute(
                "SELECT doc_id, text, source_example_id, reference_answers "
                "FROM documents WHERE workload_name = %s ORDER BY doc_id",
                (workload,),
            )
        fetched = cur.fetchall()
    rows: list[dict] = []
    doc_to_source: dict[int, str] = {}
    references: dict[str, list[str]] = {}
    prompt_fingerprints: dict[int, str] = {}
    for doc_id, text, source_id, reference_answers in fetched:
        answers = reference_answers
        if isinstance(answers, str):
            answers = json.loads(answers)
        rows.append({
            "doc_id": doc_id, "text": text,
            "source_example_id": source_id, "answers": list(answers),
        })
        doc_to_source[int(doc_id)] = source_id
        references[source_id] = list(answers)
        prompt_fingerprints[int(doc_id)] = hashlib.sha256(
            str(text).encode("utf-8")
        ).hexdigest()
    content_hash = _structured_content_hash(rows)
    if limit > 0:
        integrity_ok, problems = _smoke_integrity(rows, limit, importer_count)
        label = f"verified_smoke_limit_{limit}" if integrity_ok else "failed"
    else:
        integrity_ok, problems = _validate_workload_integrity(
            rows, importer_count, expected_hash,
        )
        label = "verified" if integrity_ok else "failed"
    # source_example_id uniqueness (predictions are keyed by source_id; a dup would
    # silently overwrite and mis-score). Mirrors _smoke_integrity for in-process arms.
    if len(set(doc_to_source.values())) != len(doc_to_source):
        integrity_ok = False
        problems.append("duplicate source_example_id across doc_ids")
        label = "failed"
    return (
        doc_to_source, references, prompt_fingerprints, content_hash,
        label, integrity_ok, problems,
    )


def _results_to_sink_payload(
    results, sidecar: dict[int, tuple], default_tenant: int, default_category: str,
) -> list[dict]:
    """Adapt BaselineRequestResult tuple -> write_completions' list-of-columns shape."""

    doc_ids: list = []
    tenant_ids: list = []
    categories: list = []
    outputs: list = []
    for r in results:
        doc_ids.append(r.doc_id)
        tenant, category = sidecar.get(r.doc_id, (default_tenant, default_category))
        tenant_ids.append(tenant)
        categories.append(category)
        outputs.append(r.output_text if r.output_text is not None else "")
    return [{
        "doc_id": doc_ids,
        "tenant_id": tenant_ids,
        "category": categories,
        "output_text": outputs,
    }]


def _sink_write(
    conn, results, sidecar, writeback_mode: str, write_batch_rows: int,
    default_tenant: int, default_category: str,
) -> tuple[int, float]:
    payload = _results_to_sink_payload(results, sidecar, default_tenant, default_category)
    t0 = time.time()
    written = write_completions(conn, payload, writeback_mode, write_batch_rows)
    return written, time.time() - t0


def _sink_readback(conn, sunk_pairs: list[tuple], category: str) -> dict:
    """Post-wall verification that the sunk rows persisted WITH EXPECTED CONTENT.

    Digests ``(doc_id, completion_text)`` for the sunk pairs and compares to what
    is read back from ``document_completions``. A count-only check would let
    historical residual rows with the same doc_id (but stale text) pass; the
    content digest catches that. ``matched`` is True only when both the row count
    AND the content digest agree.
    """

    import hashlib

    def _digest(pairs: list[tuple]) -> str:
        canon = sorted(([str(d), t]) for d, t in pairs)
        return hashlib.sha256(
            json.dumps(canon, ensure_ascii=False).encode("utf-8")
        ).hexdigest()

    expected_digest = _digest(sunk_pairs)
    expected_n = len(sunk_pairs)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT doc_id, completion_text FROM document_completions "
                "WHERE doc_id = ANY(%s) AND category = %s",
                ([d for d, _ in sunk_pairs], category),
            )
            rows = [(int(d), str(t)) for d, t in cur.fetchall()]
        actual_digest = _digest(rows)
        actual_n = len(rows)
        return {
            "expected_rows": expected_n, "present_rows": actual_n,
            "expected_digest": expected_digest, "actual_digest": actual_digest,
            "matched": (actual_n == expected_n and actual_digest == expected_digest),
        }
    except Exception as exc:
        return {"expected_rows": expected_n, "error": redact_text(str(exc)[:160]),
                "matched": False}


def _readback_ok(readback: dict, writeback_mode: str) -> bool:
    """A run is single-run-valid only if the sink readback matched (when sinking).

    Pure -- unit-testable. ``writeback_mode='none'`` sinks nothing, so there is
    nothing to verify (vacuously ok).
    """

    if writeback_mode == "none":
        return True
    return readback.get("matched") is True


def _runner_metrics(
    em_rows: int, success_count: int, row_count: int,
    error_count: int, null_count: int, max_tokens_errors: int,
    truncation_count: int, wall_s: float, sunk_rows: int,
) -> dict[str, float]:
    """Runner-layer headline metrics (division here, NOT in metrics.squad).

    ``correct_rows_per_s`` is the primary headline; ``raw_rows_per_s`` is
    reported for transparency but is never the ranking key.

    ``failure_rate`` is the **de-duplicated row-level** failure rate
    (``row_count - success_count``): a row that is both an error AND a NULL
    response is one failed row, not two. ``error_rate`` / ``null_rate`` /
    ``max_tokens_rate`` are reported separately and MAY overlap with each other
    (a single failed row can carry both an error and a NULL output).

    ``truncation_count`` / ``truncation_rate`` are the **unified, arm-agnostic**
    cap-hit metrics: direct_client exposes ``finish_reason=length``; duckdb_ai
    maps truncation to a ``max_tokens`` error. Both count as "the model hit cap"
    so the 3-arm comparison doesn't misread direct's 0 failure_rate as "no
    truncation" when it actually had length-truncated rows.
    """

    wall = wall_s if wall_s > 0 else 0.0
    failed_rows = max(0, row_count - success_count)
    def _rate(num: int) -> float:
        return round(num / row_count, 6) if row_count else 0.0
    return {
        "correct_rows_per_s": round(em_rows / wall, 4) if wall else 0.0,
        "successful_rows_per_s": round(success_count / wall, 4) if wall else 0.0,
        "raw_rows_per_s": round(row_count / wall, 4) if wall else 0.0,
        "failure_rate": _rate(failed_rows),
        "error_rate": _rate(error_count),
        "null_rate": _rate(null_count),
        "max_tokens_rate": _rate(max_tokens_errors),
        "truncation_count": truncation_count,
        "truncation_rate": _rate(truncation_count),
        "failed_rows": failed_rows,
        "sunk_rows": sunk_rows,
    }


def _operator_span(results) -> tuple[float, float]:
    """Operator-only JCT + setup as the full result span (not results[0]).

    ``operator_only_jct`` = max(completed) - min(started); ``setup_s`` =
    min(started) - min(submitted). Correct for BOTH barrier arms (DuckDB-ai:
    every row shares the barrier boundaries, so this == results[0]'s span) and
    per-request arms (direct_client: each row has its own started/completed).
    """

    if not results:
        return 0.0, 0.0
    started = [r.started_at_s for r in results]
    completed = [r.completed_at_s for r in results]
    submitted = [r.submitted_at_s for r in results]
    return (max(completed) - min(started)), (min(started) - min(submitted))


def _smoke_integrity(
    rows: list[dict], limit: int, importer_count: int,
) -> tuple[bool, list[str]]:
    """Relaxed integrity for --limit smoke runs: row-local uniqueness + non-empty
    + the scan actually returned the requested number of rows, WITHOUT the
    full-workload canonical content-hash comparison (a limited scan's hash cannot
    match the importer's full-workload hash).

    ``len(rows)`` must equal ``min(limit, importer_count)``: a short read (the DB
    returned fewer rows than requested, e.g. the workload is smaller than
    ``--limit`` silently implies) must NOT be labelled ``verified_smoke_limit_N``
    -- otherwise a 100-row scan hides behind a ``--limit 256`` label.
    """

    problems: list[str] = []
    expected = min(limit, importer_count)
    if len(rows) != expected:
        problems.append(
            f"expected {expected} rows (min(limit={limit}, "
            f"importer_count={importer_count})), got {len(rows)}"
        )
    if not rows:
        problems.append("no rows scanned (empty result)")
    if len({r["doc_id"] for r in rows}) != len(rows):
        problems.append("duplicate doc_id present")
    source_ids = [r["source_example_id"] for r in rows]
    if len(set(source_ids)) != len(source_ids):
        problems.append("duplicate source_example_id present")
    if not all(r["source_example_id"] for r in rows):
        problems.append("empty source_example_id present")
    if not all(r["answers"] and all(s.strip() for s in r["answers"]) for r in rows):
        problems.append("empty/blank reference_answers present")
    return (not problems), problems


def _finish_reason_summary(results, arm: str):
    """Per-arm finish_reason summary at the report level.

    DuckDB-ai hides finish_reason (returns the unavailable note); direct_client
    exposes it per row, so summarize its distribution (stop/length/...).
    """

    if arm == "duckdb_ai":
        return "unavailable (DuckDB-ai extension does not expose finish_reason)"
    counts: dict[str, int] = {}
    for r in results:
        key = r.finish_reason or "none"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


# The two CSV writers below mirror the inline fieldnames in _run. _run_project_static
# uses them; _run keeps its inline blocks (untouched). A future cleanup could unify.
_PER_ROW_FIELDS = [
    "source_example_id", "status", "error", "output_chars",
    "prediction", "reference_answers",
    "finish_reason", "output_tokens",
    "submitted_at_s", "started_at_s", "completed_at_s",
    "queue_wait_s", "latency_s",
    "server_version", "pgvector_version",
]
_SUNK_STATUS_FIELDS = ["doc_id", "source_example_id", "status", "error", "output_chars"]


def _write_per_row_evidence(output_dir: Path, evidence_rows: list[dict]) -> None:
    with (output_dir / "per_row_evidence.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_PER_ROW_FIELDS)
        writer.writeheader()
        writer.writerows(evidence_rows)


def _write_sunk_status(output_dir: Path, sunk_status_rows: list[dict]) -> None:
    with (output_dir / "sunk_status.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_SUNK_STATUS_FIELDS)
        writer.writeheader()
        writer.writerows(sunk_status_rows)


def _write_failure_report(output_dir, args, stage, exc, started_at) -> None:
    existing = sorted(p.name for p in output_dir.iterdir()) if output_dir.exists() else []
    endpoint_base = _endpoint_base_url(args.endpoint_url)
    report = {
        "status": "failure",
        "failure_stage": stage,
        "exception_type": type(exc).__name__,
        "sanitized_error": redact_text(str(exc)[:500]),
        "traceback": redact_text("".join(traceback.format_exception(exc))[-4000:]),
        "started_at_s": round(started_at, 3),
        "finished_at_s": round(time.time(), 3),
        "redacted_command": redact_argument_list(list(sys.argv)),
        "identity": {
            "git_commit": _git_commit(CODE_ROOT),
            "model": args.model,
            "endpoint": redact_database_url(endpoint_base),
            "arm": args.arm,
            "database_url": redact_database_url(args.database_url),
        },
        "partial_files": existing,
    }
    _write_json(output_dir / "failure_report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))


def _parse(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--arm", choices=("duckdb_ai", "direct_client", "project_static"),
                   default="duckdb_ai")
    p.add_argument("--database-url", required=True)
    p.add_argument("--workload-name", default="squad_v11_dev_short_answer")
    p.add_argument("--importer-provenance", required=True)
    p.add_argument("--endpoint-url", required=True)
    p.add_argument("--metrics-url", required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--api-key", default="EMPTY")
    p.add_argument("--max-tokens", type=int, default=64)
    p.add_argument("--max-concurrent-requests", type=int, default=32)
    p.add_argument("--request-timeout-s", type=float, default=120.0,
                   help="shared per-request timeout for BOTH arms (formal comparison "
                        "must freeze the same value); default 120 matches DuckDB's "
                        "extension default.")
    # project_static-only args (the frozen text-track static values). Required only
    # when --arm project_static; ignored otherwise. The wrapper rejects zero/negative.
    p.add_argument("--token-budget", type=int, default=0,
                   help="project_static only: frozen token-budget organizer value.")
    p.add_argument("--project-max-inflight", type=int, default=0,
                   help="project_static only: frozen per-endpoint static K (admission scope per_endpoint).")
    p.add_argument("--project-max-active-work-per-endpoint", type=int, default=0,
                   help="project_static only: frozen per-endpoint token-work credit; "
                        "the established text-track point is 65536.")
    p.add_argument("--project-actor-workers", type=int, default=0,
                   help="project_static only: frozen Ray HTTP actors per endpoint. "
                        "actor_workers x ray_actor_max_concurrency must be >= "
                        "--project-max-inflight so effective K == declared K.")
    p.add_argument("--project-ray-actor-max-concurrency", type=int, default=0,
                   help="project_static only: frozen per-actor max concurrency "
                        "(pairs with --project-actor-workers to set the slot count).")
    p.add_argument("--project-ray-batch-rows", type=int, default=64,
                   help="project_static only: hard per-submission row cap under token_budget.")
    p.add_argument("--project-python", default="",
                   help="project_static only: python executable with project deps (ray/daft) "
                        "to run postgres_ai_operator_profile.py; empty = sys.executable.")
    p.add_argument("--service-prefix-caching", choices=("enabled", "disabled"),
                   default="enabled")
    p.add_argument("--service-config-hash", default=None)
    p.add_argument("--metrics-settle-s", type=float, default=5.0)
    p.add_argument("--strict-attribution", action="store_true")
    p.add_argument("--writeback-mode", choices=("none", "json_text"), default="json_text")
    p.add_argument("--write-batch-rows", type=int, default=500)
    p.add_argument("--sink-tenant", type=int, default=0,
                   help="fallback tenant_id when the scanned row lacks one")
    p.add_argument("--sink-category", default="squad",
                   help="fallback category when the scanned row lacks one")
    p.add_argument("--limit", type=int, default=0,
                   help="scan only N rows (smoke / small-scale gate); 0 = full workload. "
                        "When >0 the full-workload count+hash integrity is relaxed to a "
                        "row-local uniqueness/non-empty smoke check.")
    p.add_argument("--output-dir", required=True)
    p.add_argument("--force", action="store_true")
    return p.parse_args(argv)


def _run(args: argparse.Namespace, output_dir: Path) -> int:
    if args.arm == "project_static":
        # project_static delegates scan + operator + sink to the profiler
        # (run_project_static); it must branch BEFORE this runner's common scan
        # path to avoid double-scan / double-writeback.
        return _run_project_static(args, output_dir)
    if args.arm not in ("duckdb_ai", "direct_client"):
        raise NotImplementedError(
            f"arm {args.arm!r} not implemented (duckdb_ai, direct_client, project_static available)"
        )
    # Per-arm provenance (scheduler owner / implementation source / formal
    # eligibility) is written into EVERY report so a reader can audit who owned
    # execution+scheduling without re-re-reading the adapter source. project_static
    # is handled above via _run_project_static (which looks up its own provenance).
    arm_provenance = adapter_provenance(args.arm)
    importer = _load_importer_provenance(args.importer_provenance)
    expected_content_hash = importer["content_hash"]
    expected_count = int(importer.get("sample_count", EXPECTED_DEV_COUNT))

    import psycopg
    pg_identity = _pg_server_identity(args.database_url)
    server_version = pg_identity.get("pg_server_version", "unknown")
    pgvector_version = pg_identity.get("pgvector_version", "unknown")
    conn = psycopg.connect(args.database_url)

    # Arm config + identity (arm-aware). Both arms share the same endpoint /
    # model / cap / concurrency so the only difference is the execution model:
    # duckdb_ai = set-oriented barrier (extension owns batching); direct_client
    # = per-request HTTP at fixed concurrency (exposes finish_reason/tokens/latency).
    endpoint_base = _endpoint_base_url(args.endpoint_url)
    if args.arm == "duckdb_ai":
        arm_config = DuckDBAiConfig(
            endpoint_base_url=endpoint_base,
            model=args.model, api_key=args.api_key, max_tokens=args.max_tokens,
            max_concurrent_requests=args.max_concurrent_requests,
            timeout_seconds=args.request_timeout_s,
        )
        arm_identity = {
            "arm_protocol": "duckdb_ai_barrier",
            **inspect_duckdb_ai_runtime(arm_config),
        }
    else:  # direct_client
        arm_config = DirectClientConfig(
            endpoint_url=args.endpoint_url,
            model=args.model, api_key=args.api_key, max_tokens=args.max_tokens,
            max_concurrent_requests=args.max_concurrent_requests,
            timeout_s=args.request_timeout_s,
        )
        arm_identity = {
            "arm_protocol": "direct_http_per_request",
            "transport": "httpx_async",
            "concurrency": args.max_concurrent_requests,
        }

    # ---- database-E2E timed barrier ----
    t0 = time.time()
    all_rows, sidecar, scan_s = _scan_workload(conn, args.workload_name, args.limit)
    tc0 = time.time()
    if args.limit > 0:
        # smoke / small-scale gate: relax full-workload count+hash to row-local
        # check, but still require the scan returned exactly min(limit, importer
        # count) rows (a short read must not hide behind a --limit N label).
        integrity_ok, integrity_problems = _smoke_integrity(
            all_rows, args.limit, expected_count
        )
        integrity_label = f"verified_smoke_limit_{args.limit}"
    else:
        integrity_ok, integrity_problems = _validate_workload_integrity(
            all_rows, expected_count, expected_content_hash
        )
        integrity_label = "verified" if integrity_ok else "failed"
    if not integrity_ok:
        raise SystemExit(
            "FAIL: workload integrity check failed: " + "; ".join(integrity_problems)
        )
    requests = tuple(
        ChatRequest(
            doc_id=row["doc_id"], prompt=row["text"], arrival_time_s=0.0,
            prompt_tokens=max(1, len(row["text"]) // 4),
            max_output_tokens=args.max_tokens, estimated_output_tokens=10,
            source_row_hash=row["source_example_id"], endpoint_index=0,
        )
        for row in all_rows
    )
    references = {row["source_example_id"]: row["answers"] for row in all_rows}
    construct_s = time.time() - tc0

    metrics_before = scrape_prometheus_metrics(args.metrics_url)
    metrics_before_idle, _ = _endpoint_idle(metrics_before)
    op0 = time.time()
    if args.arm == "duckdb_ai":
        results = run_duckdb_ai_complete(requests, arm_config)
    else:
        results = run_direct_client(requests, arm_config)
    adapter_wall_s = time.time() - op0
    operator_only_jct, setup_s = _operator_span(results)

    written, sink_s = _sink_write(
        conn, results, sidecar, args.writeback_mode, args.write_batch_rows,
        args.sink_tenant, args.sink_category,
    )
    database_e2e_wall_s = time.time() - t0

    # metrics settle + after-scrape + sink readback are OUTSIDE the E2E wall.
    # Keep the PG connection open for the post-wall sink readback (actual DB
    # verification that rows persisted), then close.
    time.sleep(max(0.0, args.metrics_settle_s))
    metrics_after = scrape_prometheus_metrics(args.metrics_url)
    sink_readback = (
        _sink_readback(
            conn,
            [(r.doc_id, r.output_text if r.output_text is not None else "") for r in results],
            args.sink_category,
        )
        if args.writeback_mode != "none"
        else {"skipped": "writeback_mode=none"}
    )
    conn.close()

    # exactly-once (full set)
    input_doc_ids = {r.doc_id for r in requests}
    result_doc_ids = {r.doc_id for r in results}
    source_ids = [row["source_example_id"] for row in all_rows]
    source_id_set = set(source_ids)
    exactly_once = (
        len(results) == len(requests)
        and len(result_doc_ids) == len(requests)
        and result_doc_ids == input_doc_ids
        and len(source_id_set) == len(all_rows)
        and all(source_ids)
    )
    if not exactly_once:
        raise SystemExit(
            "FAIL: exactly-once violated (result id set != input id set, "
            "or source_example_id not unique/non-empty)"
        )

    doc_to_source = {row["doc_id"]: row["source_example_id"] for row in all_rows}
    evidence_rows = []
    sunk_status_rows = []
    predictions: dict[str, str | None] = {}
    for r in results:
        source_id = doc_to_source.get(r.doc_id)
        is_ok = r.status == "completed" and not r.error and r.output_text is not None
        predictions[source_id] = r.output_text if is_ok else None
        evidence_rows.append({
            "source_example_id": source_id,
            "status": r.status,
            "error": redact_text(r.error or ""),
            "output_chars": len(r.output_text) if r.output_text else 0,
            "prediction": r.output_text or "",
            "reference_answers": json.dumps(
                references.get(source_id, []), ensure_ascii=False
            ),
            "finish_reason": r.finish_reason or "",
            "output_tokens": r.output_tokens,
            "submitted_at_s": round(r.submitted_at_s, 6),
            "started_at_s": round(r.started_at_s, 6),
            "completed_at_s": round(r.completed_at_s, 6),
            "queue_wait_s": round(r.started_at_s - r.submitted_at_s, 6),
            "latency_s": round(r.completed_at_s - r.started_at_s, 6),
            "server_version": server_version,
            "pgvector_version": pgvector_version,
        })
        # Execution-status sidecar (NOT a DB readback): links each sunk doc_id to
        # its execution status so the unified sink's empty completion_text
        # (NULL->"" for failed rows) can be cross-referenced to the real status.
        # The actual sink persistence is verified separately by _sink_readback.
        sunk_status_rows.append({
            "doc_id": r.doc_id,
            "source_example_id": source_id,
            "status": r.status,
            "error": redact_text(r.error or ""),
            "output_chars": len(r.output_text) if r.output_text else 0,
        })
    with (output_dir / "per_row_evidence.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["source_example_id", "status", "error", "output_chars",
                        "prediction", "reference_answers",
                        "finish_reason", "output_tokens",
                        "submitted_at_s", "started_at_s", "completed_at_s",
                        "queue_wait_s", "latency_s",
                        "server_version", "pgvector_version"],
        )
        writer.writeheader()
        writer.writerows(evidence_rows)
    with (output_dir / "sunk_status.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["doc_id", "source_example_id", "status", "error", "output_chars"],
        )
        writer.writeheader()
        writer.writerows(sunk_status_rows)

    quality = squad_quality_metrics(predictions, references)
    success_count = sum(
        1 for r in results
        if r.status == "completed" and not r.error and r.output_text is not None
    )
    null_response = sum(1 for r in results if r.output_text is None)
    error_count = sum(1 for r in results if r.status != "completed" or r.error)
    max_tokens_errors = sum(
        1 for r in results if r.error and "max_tokens" in (r.error or "")
    )
    # UNIFIED truncation count across arms: direct_client exposes finish_reason=length;
    # duckdb_ai maps truncation to a max_tokens error. Both are "the model hit cap".
    truncation_count = sum(
        1 for r in results
        if r.finish_reason == "length" or (r.error and "max_tokens" in (r.error or ""))
    )
    # single_run_valid = 0 error/NULL AND the sink readback matched (when sinking).
    # A readback mismatch (rows did not persist with expected content, or the
    # readback query itself errored) FAILS the run -- a silent sink failure
    # cannot hide behind a clean operator result.
    readback_ok = _readback_ok(sink_readback, args.writeback_mode)
    passed = error_count == 0 and null_response == 0 and readback_ok
    _reasons: list[str] = []
    if error_count or null_response:
        _reasons.append(
            f"{error_count} row-level error(s), {null_response} NULL response(s) "
            f"(of which {max_tokens_errors} identifiable max_tokens error(s))"
        )
    if not readback_ok:
        _reasons.append(
            "sink readback failed: "
            + (sink_readback.get("error") or "content/count mismatch")
        )
    failure_reason = "; ".join(_reasons) if _reasons else None

    attribution, attribution_ok = _assess_attribution(
        metrics_before, metrics_after, requests_sent=len(all_rows)
    )
    if args.strict_attribution and not attribution_ok:
        raise SystemExit(
            "FAIL: --strict-attribution and vLLM counter attribution failed: "
            + "; ".join(attribution["reasons"])
        )

    if attribution_ok:
        prompt_delta = int(_delta(metrics_before, metrics_after, "vllm:prompt_tokens_total"))
        gen_delta = int(_delta(metrics_before, metrics_after, "vllm:generation_tokens_total"))
        pc_queries = int(_delta(metrics_before, metrics_after, "vllm:prefix_cache_queries_total"))
        pc_hits = int(_delta(metrics_before, metrics_after, "vllm:prefix_cache_hits_total"))
        vllm_metrics = {
            "attribution": "attributable",
            "prompt_tokens_delta": prompt_delta,
            "generation_tokens_delta": gen_delta,
            "prefix_cache_queries_delta": pc_queries,
            "prefix_cache_hits_delta": pc_hits,
            "prefix_cache_hit_rate": round(pc_hits / pc_queries, 4) if pc_queries else 0.0,
        }
    else:
        vllm_metrics = {"attribution": "unavailable", "reasons": attribution["reasons"]}

    # runner-layer metrics (division here, not in metrics.squad)
    runner_metrics = _runner_metrics(
        em_rows=quality["squad_exact_match_rows"],
        success_count=success_count,
        row_count=len(all_rows),
        error_count=error_count,
        null_count=null_response,
        max_tokens_errors=max_tokens_errors,
        truncation_count=truncation_count,
        wall_s=database_e2e_wall_s,
        sunk_rows=written,
    )

    report = {
        "runner": "squad_database_e2e",
        "role": "database_e2e_boundary",
        "arm": args.arm,
        "provenance": arm_provenance.summary_fields(),
        "status": "success" if passed else "failure",
        "failure_reason": failure_reason,
        # Three DECOUPLED judgments (per codex audit):
        #  - single_run_valid: THIS single shot had 0 error/NULL.
        #  - formal_run_gate_passed: ALWAYS False here -- this is a single-shot
        #    runner; the 1-warmup+3-formal repeat gate (CV/CI checks) is a
        #    separate protocol that only a formal repeat runner can set True.
        #  - comparison_admission: a single shot cannot confer or exclude
        #    formal comparison admission; it stays pending the formal repeat.
        "single_run_valid": bool(passed),
        "formal_run_gate_passed": False,
        "formal_run_gate_note": (
            "single-shot runner; the 1-warmup+3-formal repeat gate is a "
            "separate protocol not implemented here"
        ),
        "comparison_admission": "pending_formal_repeat",
        "cap": args.max_tokens,
        "row_count": len(all_rows),
        "exactly_once": exactly_once,
        "success_count": success_count,
        "null_response_count": null_response,
        "error_count": error_count,
        "max_tokens_error_count": max_tokens_errors,
        "truncation_count": truncation_count,
        "workload_integrity": integrity_label,
        "workload_content_hash": _structured_content_hash(all_rows),
        "importer_content_hash": expected_content_hash,
        "timing": {
            "boundary": "database_e2e",
            "database_e2e_wall_s": round(database_e2e_wall_s, 3),
            "scan_s": round(scan_s, 3),
            "construct_s": round(construct_s, 3),
            "adapter_wall_s": round(adapter_wall_s, 3),
            "operator_only_jct_s": round(operator_only_jct, 3),
            "setup_s": round(setup_s, 3),
            "sink_s": round(sink_s, 3),
            "note": ("database_e2e_wall_s = scan + construct + adapter(operator) + sink; "
                     "metrics settle + after-scrape are outside the wall"),
        },
        "runner_metrics": runner_metrics,
        "squad_quality": quality,
        "vllm_metrics": vllm_metrics,
        "attribution": attribution,
        "sink": {
            "writeback_mode": args.writeback_mode,
            "write_batch_rows": args.write_batch_rows,
            "table": "document_completions",
            "rows_written": written,
            # Post-wall DB readback: verifies the sunk rows actually persisted,
            # distinct from the sunk_status.csv execution-status sidecar.
            "readback": sink_readback,
        },
        "finish_reason": _finish_reason_summary(results, args.arm),
        "identity": {
            "git_commit": _git_commit(CODE_ROOT),
            "model": args.model,
            "endpoint": redact_database_url(endpoint_base),
            "workload": args.workload_name,
            **arm_identity,
            "vllm_version": _vllm_version(endpoint_base),
            "service_prefix_caching": args.service_prefix_caching,
            "service_config_hash": args.service_config_hash or "not_provided",
            **pg_identity,
            **_gpu_identity(),
            "metrics_snapshot": {
                "before": _scrape_status(metrics_before),
                "before_idle": metrics_before_idle,
                "after": _scrape_status(metrics_after),
            },
        },
        "evidence_files": {
            "per_row_csv": "per_row_evidence.csv",
            "sunk_status_csv": "sunk_status.csv",
        },
        "command": redact_argument_list(list(sys.argv)),
    }
    _write_json(output_dir / "report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
    return 0 if passed else 1


def _run_project_static(args: argparse.Namespace, output_dir: Path) -> int:
    """Run the frozen project path and assemble independently auditable evidence.

    The profiler owns PG scan -> Daft token-budget organizer -> Ray actor ->
    static per-endpoint K + token-work admission -> vLLM -> unified sink
    (``document_completions``). This function invokes ``run_project_static`` (which
    subprocess-calls ``postgres_ai_operator_profile.py``), then reads its independent
    completion and exact-source-scan evidence. A post-run DB read supplies references
    and verifies that the scanned prompt fingerprints still match the imported
    workload. The runner does not execute a second model-feeding scan or sink.
    """

    if (args.token_budget <= 0 or args.project_max_inflight <= 0
            or args.project_max_active_work_per_endpoint <= 0
            or args.project_actor_workers <= 0
            or args.project_ray_actor_max_concurrency <= 0):
        raise SystemExit(
            "FAIL: --arm project_static requires the frozen static values "
            "--token-budget, --project-max-inflight, "
            "--project-max-active-work-per-endpoint, --project-actor-workers, "
            "--project-ray-actor-max-concurrency (all > 0; actor_workers x "
            "concurrency must be >= max-inflight so effective K == declared K)"
        )
    if args.writeback_mode == "none":
        # Completion evidence is independent of the sink, but this runner's stated
        # boundary is database E2E and therefore requires a real unified sink.
        raise SystemExit(
            "FAIL: --arm project_static requires --writeback-mode json_text "
            "because the database-E2E comparison contract includes the unified sink"
        )
    arm_provenance = adapter_provenance("project_static")
    importer = _load_importer_provenance(args.importer_provenance)
    expected_content_hash = importer["content_hash"]
    expected_count = int(importer.get("sample_count", EXPECTED_DEV_COUNT))
    endpoint_base = _endpoint_base_url(args.endpoint_url)

    config = ProjectStaticConfig(
        database_url=args.database_url,
        workload_name=args.workload_name,
        endpoint_url=args.endpoint_url,
        model=args.model, max_tokens=args.max_tokens,
        token_budget=args.token_budget,
        max_inflight=args.project_max_inflight,
        max_active_work_per_endpoint=args.project_max_active_work_per_endpoint,
        actor_workers_per_endpoint=args.project_actor_workers,
        ray_actor_max_concurrency=args.project_ray_actor_max_concurrency,
        api_key=args.api_key,
        writeback_mode=args.writeback_mode,
        write_batch_rows=args.write_batch_rows,
        total_rows=args.limit if args.limit > 0 else expected_count,
        ray_batch_rows=args.project_ray_batch_rows,
        request_timeout_s=args.request_timeout_s,
        completion_temperature=0.0,
        completion_http_transport="httpx_async",
        service_prefix_caching=args.service_prefix_caching,
        python_executable=args.project_python,
    )
    arm_identity = {
        "arm_protocol": "project_ray_frozen_static",
        "transport": "ray_actor",
        "http_transport": "httpx_async",
        "temperature": 0.0,
        "declared_max_inflight": args.project_max_inflight,
        "effective_k": config.effective_k,
        "max_active_work_per_endpoint": args.project_max_active_work_per_endpoint,
        "actor_workers_per_endpoint": args.project_actor_workers,
        "ray_actor_max_concurrency": args.project_ray_actor_max_concurrency,
        "token_budget": args.token_budget,
        "admission_scope": "per_endpoint",
        "profiler_script": config.profiler_script,
    }
    # ProjectStaticConfig validates actor_workers x concurrency >= max_inflight,
    # so effective_k == declared K (no silent clamp). Surface any config error as
    # a fail-closed SystemExit (main() writes failure_report.json).
    if config.effective_k != args.project_max_inflight:
        raise SystemExit(
            f"FAIL: project_static effective_k ({config.effective_k}) != declared "
            f"max_inflight ({args.project_max_inflight}); actor topology must cover K"
        )

    pg_identity = _pg_server_identity(args.database_url)
    server_version = pg_identity.get("pg_server_version", "unknown")
    pgvector_version = pg_identity.get("pgvector_version", "unknown")

    metrics_before = scrape_prometheus_metrics(args.metrics_url)
    metrics_before_idle, _ = _endpoint_idle(metrics_before)
    wrapper_t0 = time.time()
    # Connection-free: the wrapper reads all per-doc evidence from profiler output
    # files (completion-evidence CSV + summary CSV). The conn is opened only AFTER
    # the subprocess, for the integrity read + sink readback.
    run = run_project_static(config, output_dir / "_profiler_work")
    wrapper_wall_s = time.time() - wrapper_t0
    time.sleep(max(0.0, args.metrics_settle_s))
    metrics_after = scrape_prometheus_metrics(args.metrics_url)

    if run.exit_code != 0 or not run.formal_row_found:
        raise SystemExit(
            f"FAIL: project_static profiler did not produce a formal ok run "
            f"(exit={run.exit_code}, formal_ok={run.formal_row_found}); "
            f"stderr tail: {redact_text(run.stderr_tail)}"
        )
    results = run.results
    operator_only_jct, setup_s = _operator_span(results)

    # Workload integrity + scoring ground truth (one read; NOT the operator scan
    # the profiler owns -- this reads no model-feeding prompts of its own, it
    # only hashes the workload the profiler scanned and fetches reference answers
    # for EM/F1). For full runs the structured content hash must match the
    # importer; for --limit smoke it records the subset hash (not comparable).
    import psycopg  # noqa: F401
    conn = psycopg.connect(args.database_url)
    (
        doc_to_source, references, prompt_fingerprints, workload_content_hash,
        integrity_label, integrity_ok, integrity_problems,
    ) = (
        _fetch_workload_integrity_and_scoring(
            conn, args.workload_name, args.limit, expected_count, expected_content_hash,
        )
    )
    if not integrity_ok:
        conn.close()
        raise SystemExit(
            "FAIL: project_static workload integrity failed: "
            + "; ".join(integrity_problems)
        )
    actual_scan_fingerprints = dict(run.source_scan_fingerprints)
    if actual_scan_fingerprints != prompt_fingerprints:
        conn.close()
        raise SystemExit(
            "FAIL: profiler actual source-scan fingerprints do not match the "
            "post-run imported workload snapshot"
        )
    input_doc_ids = set(doc_to_source)
    result_doc_ids = {r.doc_id for r in results}
    expected_rows = min(args.limit, expected_count) if args.limit > 0 else expected_count
    count_ok = len(results) == expected_rows
    exactly_once = (
        len(results) == len(result_doc_ids)
        and result_doc_ids == input_doc_ids
        and len(doc_to_source) == len(input_doc_ids)
        and all(doc_to_source.values())
    )
    if not (count_ok and exactly_once):
        conn.close()
        raise SystemExit(
            f"FAIL: project_static count/exactly-once violated "
            f"(got {len(results)} results, expected {expected_rows}; "
            f"result_doc_ids==workload_doc_ids: {result_doc_ids == input_doc_ids})"
        )

    # Verify the profiler's sink persisted the evidence output_text (content-digest
    # readback). sunk_pairs come from the profiler's run-scoped completion evidence
    # (independent of document_completions), so this compares TWO independent
    # sources -- a stale residual row with the same doc_id cannot self-prove.
    sink_readback = (
        _sink_readback(conn, list(run.sunk_pairs), args.sink_category)
        if args.writeback_mode != "none"
        else {"skipped": "writeback_mode=none"}
    )
    conn.close()

    predictions: dict[str, str | None] = {}
    evidence_rows = []
    sunk_status_rows = []
    for r in results:
        source_id = doc_to_source.get(r.doc_id)
        is_ok = r.status == "completed" and not r.error and r.output_text is not None
        predictions[source_id] = r.output_text if is_ok else None
        evidence_rows.append({
            "source_example_id": source_id,
            "status": r.status,
            "error": redact_text(r.error or ""),
            "output_chars": len(r.output_text) if r.output_text else 0,
            "prediction": r.output_text or "",
            "reference_answers": json.dumps(
                references.get(source_id, []), ensure_ascii=False
            ),
            "finish_reason": r.finish_reason or "",
            "output_tokens": r.output_tokens,
            "submitted_at_s": round(r.submitted_at_s, 6),
            "started_at_s": round(r.started_at_s, 6),
            "completed_at_s": round(r.completed_at_s, 6),
            "queue_wait_s": round(r.started_at_s - r.submitted_at_s, 6),
            "latency_s": round(r.completed_at_s - r.started_at_s, 6),
            "server_version": server_version,
            "pgvector_version": pgvector_version,
        })
        sunk_status_rows.append({
            "doc_id": r.doc_id,
            "source_example_id": source_id,
            "status": r.status,
            "error": redact_text(r.error or ""),
            "output_chars": len(r.output_text) if r.output_text else 0,
        })
    _write_per_row_evidence(output_dir, evidence_rows)
    _write_sunk_status(output_dir, sunk_status_rows)

    quality = squad_quality_metrics(predictions, references)
    success_count = sum(
        1 for r in results
        if r.status == "completed" and not r.error and r.output_text is not None
    )
    null_response = sum(1 for r in results if r.output_text is None)
    error_count = sum(1 for r in results if r.status != "completed" or r.error)
    max_tokens_errors = sum(
        1 for r in results if r.error and "max_tokens" in (r.error or "")
    )
    truncation_count = sum(
        1 for r in results
        if r.finish_reason == "length" or (r.error and "max_tokens" in (r.error or ""))
    )
    readback_ok = _readback_ok(sink_readback, args.writeback_mode)
    passed = error_count == 0 and null_response == 0 and readback_ok
    _reasons: list[str] = []
    if error_count or null_response:
        _reasons.append(
            f"{error_count} row-level error(s), {null_response} NULL response(s) "
            f"(of which {max_tokens_errors} identifiable max_tokens error(s))"
        )
    if not readback_ok:
        _reasons.append(
            "sink readback failed: "
            + (sink_readback.get("error") or "content/count mismatch")
        )
    failure_reason = "; ".join(_reasons) if _reasons else None

    attribution, attribution_ok = _assess_attribution(
        metrics_before, metrics_after, requests_sent=len(results)
    )
    if args.strict_attribution and not attribution_ok:
        raise SystemExit(
            "FAIL: --strict-attribution and vLLM counter attribution failed: "
            + "; ".join(attribution["reasons"])
        )
    if attribution_ok:
        prompt_delta = int(_delta(metrics_before, metrics_after, "vllm:prompt_tokens_total"))
        gen_delta = int(_delta(metrics_before, metrics_after, "vllm:generation_tokens_total"))
        pc_queries = int(_delta(metrics_before, metrics_after, "vllm:prefix_cache_queries_total"))
        pc_hits = int(_delta(metrics_before, metrics_after, "vllm:prefix_cache_hits_total"))
        vllm_metrics = {
            "attribution": "attributable",
            "prompt_tokens_delta": prompt_delta,
            "generation_tokens_delta": gen_delta,
            "prefix_cache_queries_delta": pc_queries,
            "prefix_cache_hits_delta": pc_hits,
            "prefix_cache_hit_rate": round(pc_hits / pc_queries, 4) if pc_queries else 0.0,
        }
    else:
        vllm_metrics = {"attribution": "unavailable", "reasons": attribution["reasons"]}

    t = run.timing
    construct_s = (
        t.get("arrow_build_s", 0.0) + t.get("organizer_from_arrow_s", 0.0)
        + t.get("organizer_plan_s", 0.0) + t.get("organizer_collect_s", 0.0)
    )
    database_e2e_wall_s = t.get("e2e_s") or wrapper_wall_s
    runner_metrics = _runner_metrics(
        em_rows=quality["squad_exact_match_rows"],
        success_count=success_count,
        row_count=len(results),
        error_count=error_count, null_count=null_response,
        max_tokens_errors=max_tokens_errors,
        truncation_count=truncation_count,
        wall_s=database_e2e_wall_s,
        sunk_rows=len(run.sunk_pairs),
    )

    report = {
        "runner": "squad_database_e2e",
        "role": "database_e2e_boundary",
        "arm": "project_static",
        "provenance": arm_provenance.summary_fields(),
        "status": "success" if passed else "failure",
        "failure_reason": failure_reason,
        "single_run_valid": bool(passed),
        "formal_run_gate_passed": False,
        "formal_run_gate_note": (
            "single-shot runner; project_static additionally lacks a timing wall "
            "identical to the in-process arms, so it cannot enter cross-arm formal "
            "ranking until that boundary is implemented"
        ),
        "comparison_admission": "blocked_unified_timing_boundary",
        "cap": args.max_tokens,
        "row_count": len(results),
        "exactly_once": exactly_once,
        "success_count": success_count,
        "null_response_count": null_response,
        "error_count": error_count,
        "max_tokens_error_count": max_tokens_errors,
        "truncation_count": truncation_count,
        "workload_integrity": integrity_label,
        # Structured hash of the post-run database integrity/scoring read. The
        # separately archived source_scan_csv proves which prompts the profiler
        # actually handed to the organizer. For full runs this hash MUST equal
        # importer_content_hash (verified by _validate_workload_integrity); for
        # --limit smoke it is the subset hash (not comparable, per integrity_label).
        "workload_content_hash": workload_content_hash,
        "importer_content_hash": expected_content_hash,
        "timing": {
            "boundary": "database_e2e",
            "cross_arm_comparable": False,
            "database_e2e_wall_s": round(database_e2e_wall_s, 3),
            "scan_s": round(t.get("db_fetch_s", 0.0), 3),
            "construct_s": round(construct_s, 3),
            "adapter_wall_s": round(t.get("operator_wall_s", 0.0), 3),
            "operator_only_jct_s": round(operator_only_jct, 3),
            "setup_s": round(setup_s, 3),
            "sink_s": round(t.get("writeback_s", 0.0), 3),
            "wrapper_wall_s": round(wrapper_wall_s, 3),
            "note": (
                "project_static timing sourced from the profiler --output CSV row; "
                "NOT directly comparable to the in-process arms' database_e2e_wall_s. "
                "The profiler e2e_s is a STRICTLY BROADER boundary: it includes the "
                "post-loop vLLM metrics scrape + trace-CSV IO + finish_job, and "
                "excludes actor-ready/Ray-init (which the in-process arms do not run). "
                "construct_s is synthesized (Arrow build + organizer stages). For the "
                "tightest adapter-equivalent span see adapter_wall_s (profiler "
                "operator_wall_s); wrapper_wall_s is the full subprocess wall. "
                "Cross-arm absolute-wall comparison requires a future unified boundary."
            ),
        },
        "runner_metrics": runner_metrics,
        "squad_quality": quality,
        "vllm_metrics": vllm_metrics,
        "attribution": attribution,
        "sink": {
            "writeback_mode": args.writeback_mode,
            "write_batch_rows": args.write_batch_rows,
            "table": "document_completions",
            "rows_written": len(run.sunk_pairs),
            "readback": sink_readback,
        },
        "finish_reason": _finish_reason_summary(results, "project_static"),
        "identity": {
            "git_commit": _git_commit(CODE_ROOT),
            "model": args.model,
            "endpoint": redact_database_url(endpoint_base),
            "workload": args.workload_name,
            **arm_identity,
            "vllm_version": _vllm_version(endpoint_base),
            "service_prefix_caching": args.service_prefix_caching,
            "service_config_hash": args.service_config_hash or "not_provided",
            **pg_identity,
            **_gpu_identity(),
            "metrics_snapshot": {
                "before": _scrape_status(metrics_before),
                "before_idle": metrics_before_idle,
                "after": _scrape_status(metrics_after),
            },
        },
        "evidence_files": {
            "per_row_csv": "per_row_evidence.csv",
            "sunk_status_csv": "sunk_status.csv",
            "profiler_source_scan_csv": "_profiler_work/project_static_source_scan.csv",
        },
        "command": redact_argument_list(list(sys.argv)),
    }
    _write_json(output_dir / "report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
    return 0 if passed else 1


def main(argv=None) -> int:
    args = _parse(sys.argv[1:] if argv is None else argv)
    output_dir = Path(args.output_dir)
    if output_dir.exists() and not args.force:
        raise SystemExit(f"output dir {output_dir} exists; pass --force to overwrite")
    output_dir.mkdir(parents=True, exist_ok=True)
    started_at = time.time()
    try:
        return _run(args, output_dir)
    except BaseException as exc:
        _write_failure_report(output_dir, args, "run", exc, started_at)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
