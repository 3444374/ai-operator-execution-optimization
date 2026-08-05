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

Arms: ``duckdb_ai`` is implemented (reuses ``run_duckdb_ai_complete``); ``direct_client``
and ``project_static`` raise NotImplementedError (added in later passes).
"""

from __future__ import annotations

import argparse
import csv
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


def _scan_workload(conn, workload: str) -> tuple[list[dict], dict[int, tuple], float]:
    """Timed persistent-table scan. Returns (rows, doc_id->(tenant,category), scan_s).

    Rows carry everything downstream needs: ChatRequest build (doc_id/text),
    quality (source_example_id/reference_answers), and the sink sidecar
    (tenant_id/category). The scan is the unified front-end for every arm.
    """

    import psycopg
    t0 = time.time()
    with conn.cursor() as cur:
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


def _runner_metrics(
    em_rows: int, success_count: int, row_count: int,
    error_count: int, null_count: int, max_tokens_errors: int,
    wall_s: float, sunk_rows: int,
) -> dict[str, float]:
    """Runner-layer headline metrics (division here, NOT in metrics.squad).

    ``correct_rows_per_s`` is the primary headline; ``raw_rows_per_s`` is
    reported for transparency but is never the ranking key.

    ``failure_rate`` is the **de-duplicated row-level** failure rate
    (``row_count - success_count``): a row that is both an error AND a NULL
    response is one failed row, not two. ``error_rate`` / ``null_rate`` /
    ``max_tokens_rate`` are reported separately and MAY overlap with each other
    (a single failed row can carry both an error and a NULL output).
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


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


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
    p.add_argument("--output-dir", required=True)
    p.add_argument("--force", action="store_true")
    return p.parse_args(argv)


def _run(args: argparse.Namespace, output_dir: Path) -> int:
    if args.arm != "duckdb_ai":
        raise NotImplementedError(
            f"arm {args.arm!r} not implemented; only duckdb_ai in this pass"
        )
    importer = _load_importer_provenance(args.importer_provenance)
    expected_content_hash = importer["content_hash"]
    expected_count = int(importer.get("sample_count", EXPECTED_DEV_COUNT))

    import psycopg
    pg_identity = _pg_server_identity(args.database_url)
    server_version = pg_identity.get("pg_server_version", "unknown")
    pgvector_version = pg_identity.get("pgvector_version", "unknown")
    conn = psycopg.connect(args.database_url)

    config = DuckDBAiConfig(
        endpoint_base_url=_endpoint_base_url(args.endpoint_url),
        model=args.model, api_key=args.api_key, max_tokens=args.max_tokens,
        max_concurrent_requests=args.max_concurrent_requests,
    )
    runtime_id = inspect_duckdb_ai_runtime(config)

    # ---- database-E2E timed barrier ----
    t0 = time.time()
    all_rows, sidecar, scan_s = _scan_workload(conn, args.workload_name)
    tc0 = time.time()
    integrity_ok, integrity_problems = _validate_workload_integrity(
        all_rows, expected_count, expected_content_hash
    )
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
    results = run_duckdb_ai_complete(requests, config)
    adapter_wall_s = time.time() - op0
    operator_only_jct, setup_s = _operator_span(results)

    written, sink_s = _sink_write(
        conn, results, sidecar, args.writeback_mode, args.write_batch_rows,
        args.sink_tenant, args.sink_category,
    )
    database_e2e_wall_s = time.time() - t0
    conn.close()

    # metrics settle + after-scrape are OUTSIDE the E2E wall
    time.sleep(max(0.0, args.metrics_settle_s))
    metrics_after = scrape_prometheus_metrics(args.metrics_url)

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
    sink_audit_rows = []
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
            "server_version": server_version,
            "pgvector_version": pgvector_version,
        })
        # Run-scoped sink audit: link each sunk doc_id to its execution status so
        # the unified sink's empty completion_text (NULL->"" for failed rows) can
        # be cross-referenced back to the real status/error (not overwriting the
        # shared document_completions contract).
        sink_audit_rows.append({
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
                        "server_version", "pgvector_version"],
        )
        writer.writeheader()
        writer.writerows(evidence_rows)
    with (output_dir / "sink_audit.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["doc_id", "source_example_id", "status", "error", "output_chars"],
        )
        writer.writeheader()
        writer.writerows(sink_audit_rows)

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
    passed = error_count == 0 and null_response == 0
    failure_reason = (
        None if passed
        else (f"{error_count} row-level error(s), {null_response} NULL response(s) "
              f"(of which {max_tokens_errors} identifiable max_tokens error(s))")
    )

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
        wall_s=database_e2e_wall_s,
        sunk_rows=written,
    )

    report = {
        "runner": "squad_database_e2e",
        "role": "database_e2e_boundary",
        "arm": args.arm,
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
        "workload_integrity": "verified" if integrity_ok else "failed",
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
        },
        "finish_reason": "unavailable (DuckDB-ai extension does not expose finish_reason)",
        "identity": {
            "git_commit": _git_commit(CODE_ROOT),
            "model": args.model,
            "endpoint": redact_database_url(config.endpoint_base_url),
            "workload": args.workload_name,
            "duckdb_version": runtime_id.get("duckdb_version"),
            "duckdb_ai_extension_version": runtime_id.get("duckdb_ai_extension_version"),
            "duckdb_ai_extension_source": runtime_id.get("duckdb_ai_extension_source"),
            "vllm_version": _vllm_version(config.endpoint_base_url),
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
            "sink_audit_csv": "sink_audit.csv",
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
