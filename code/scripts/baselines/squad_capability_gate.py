#!/usr/bin/env python3
"""SQuAD v1.1 dev capability gate (DuckDB-ai arm) -- rewritten for rigor.

Fixes per codex fifth review:
  * --mode {sampled,full}: full mode selects ALL rows (no broken modulo).
  * sampled mode is a DETERMINISTIC STRATIFIED sample by reference-answer
    word-count bucket (short/medium/long), proportional allocation, even
    spacing within bucket, sorted by source_example_id. The sampled IDs +
    a sample hash are saved as a manifest.
  * per-row evidence CSV (source_example_id, prediction, reference_answers,
    status, error, output_len) so EM/F1 is independently recomputable.
  * --metrics-url is USED: vLLM prompt/generation token delta + prefix-cache
    queries/hits recorded as the actual output workload.
  * truncation wording tightened: DuckDB-ai does not expose finish_reason, so
    we report "no error/NULL/identifiable-max_tokens-error observed" + the
    vLLM generation-token delta, NOT "zero truncation".
  * exactly-once is a 3-way check (result_count == input_count == unique_ids).
  * timing labels are correct: adapter_wall vs operator_only_jct vs setup.
  * 7-step README + full identity (git commit, model, endpoint, DuckDB/ext
    version, PG server identity, prefix-cache state, workload content hash).

Capability gate, NOT a formal ranking: one arm, operator-only boundary only
(database-E2E runner not implemented), no cross-arm comparison.
"""

from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.contracts import ChatRequest  # noqa: E402
from src.baselines.text.products.duckdb_ai import (  # noqa: E402
    DuckDBAiConfig,
    inspect_duckdb_ai_runtime,
    run_duckdb_ai_complete,
)
from src.observability.metrics import (  # noqa: E402
    scrape_prometheus_metrics,
    squad_quality_metrics,
)


def _answer_bucket(answers: list[str]) -> str:
    word_count = len(answers[0].split()) if answers else 0
    if word_count <= 1:
        return "short"
    if word_count <= 4:
        return "medium"
    return "long"


def stratified_sample(rows: list[dict], target: int) -> list[dict]:
    """Deterministic stratified sample by reference-answer word-count bucket.

    Proportional allocation across short/medium/long buckets, even spacing
    within each bucket (sorted by source_example_id), then deterministic
    trim/top-up to exactly ``target``. Pure function -- unit-testable.
    """

    if target >= len(rows):
        return sorted(rows, key=lambda r: r["source_example_id"])
    buckets: dict[str, list[dict]] = collections.defaultdict(list)
    for row in rows:
        buckets[_answer_bucket(row["answers"])].append(row)
    total = len(rows)
    sampled: list[dict] = []
    seen: set = set()
    for bucket_name in sorted(buckets):
        in_bucket = sorted(buckets[bucket_name], key=lambda r: r["source_example_id"])
        n = max(1, round(target * len(in_bucket) / total))
        stride = max(1, len(in_bucket) // n)
        picked = in_bucket[::stride][:n]
        for row in picked:
            if row["source_example_id"] not in seen:
                sampled.append(row)
                seen.add(row["source_example_id"])
    # deterministic top-up / trim to exactly target
    remaining = sorted(
        (r for r in rows if r["source_example_id"] not in seen),
        key=lambda r: r["source_example_id"],
    )
    for row in remaining:
        if len(sampled) >= target:
            break
        sampled.append(row)
        seen.add(row["source_example_id"])
    return sorted(sampled[:target], key=lambda r: r["source_example_id"])


def _sample_hash(sampled: list[dict]) -> str:
    digest = hashlib.sha256()
    for row in sampled:
        digest.update(row["source_example_id"].encode("utf-8"))
    return digest.hexdigest()


def _workload_content_hash(database_url: str, workload: str) -> str:
    import psycopg
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT md5(string_agg(text, '|' ORDER BY doc_id)) "
                "FROM documents WHERE workload_name = %s",
                (workload,),
            )
            row = cur.fetchone()
    return str(row[0]) if row else "unknown"


def _load_workload(database_url: str, workload: str) -> list[dict]:
    import psycopg
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT doc_id, text, source_example_id, reference_answers "
                "FROM documents WHERE workload_name = %s ORDER BY doc_id",
                (workload,),
            )
            rows_out = []
            for doc_id, text, source_example_id, reference_answers in cur.fetchall():
                answers = reference_answers
                if isinstance(answers, str):
                    answers = json.loads(answers)
                rows_out.append(
                    {
                        "doc_id": doc_id,
                        "text": text,
                        "source_example_id": source_example_id,
                        "answers": list(answers),
                    }
                )
    return rows_out


def _pg_server_identity(database_url: str) -> dict:
    import psycopg
    identity: dict[str, str] = {}
    try:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                identity["pg_server_version"] = str(cur.fetchone()[0])
    except Exception as exc:
        identity["pg_error"] = str(exc)[:120]
    return identity


def _git_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=CODE_ROOT,
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    except Exception:
        return "unknown"


def _endpoint_base_url(endpoint_url: str) -> str:
    suffix = "/chat/completions"
    if not endpoint_url.endswith(suffix):
        raise SystemExit("endpoint-url must end with /v1/chat/completions")
    return endpoint_url[: -len(suffix)]


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--workload-name", default="squad_v11_dev_short_answer")
    parser.add_argument("--mode", choices=("sampled", "full"), default="sampled")
    parser.add_argument("--sample-count", type=int, default=256)
    parser.add_argument("--endpoint-url", required=True)
    parser.add_argument("--metrics-url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--api-key", default="EMPTY")
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--max-concurrent-requests", type=int, default=32)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse(sys.argv[1:] if argv is None else argv)
    output_dir = Path(args.output_dir)
    if output_dir.exists() and not args.force:
        raise SystemExit(f"output dir {output_dir} exists; pass --force to overwrite")
    output_dir.mkdir(parents=True, exist_ok=True)

    all_rows = _load_workload(args.database_url, args.workload_name)
    workload_hash = _workload_content_hash(args.database_url, args.workload_name)
    if args.mode == "full":
        selected = sorted(all_rows, key=lambda r: r["source_example_id"])
    else:
        selected = stratified_sample(all_rows, args.sample_count)
    sample_hash = _sample_hash(selected)

    requests = tuple(
        ChatRequest(
            doc_id=row["doc_id"], prompt=row["text"], arrival_time_s=0.0,
            prompt_tokens=max(1, len(row["text"]) // 4),
            max_output_tokens=args.max_tokens, estimated_output_tokens=10,
            source_row_hash=row["source_example_id"], endpoint_index=0,
        )
        for row in selected
    )
    config = DuckDBAiConfig(
        endpoint_base_url=_endpoint_base_url(args.endpoint_url),
        model=args.model, api_key=args.api_key, max_tokens=args.max_tokens,
        max_concurrent_requests=args.max_concurrent_requests,
    )
    runtime_id = inspect_duckdb_ai_runtime(config)

    metrics_before = scrape_prometheus_metrics(args.metrics_url)
    run_started = time.time()
    results = run_duckdb_ai_complete(requests, config)
    run_finished = time.time()
    metrics_after = scrape_prometheus_metrics(args.metrics_url)
    adapter_wall_s = run_finished - run_started

    # 3-way exactly-once
    input_count = len(requests)
    result_count = len(results)
    unique_result_ids = len({r.doc_id for r in results})
    exactly_once = (result_count == input_count == unique_result_ids)

    doc_to_source = {row["doc_id"]: row["source_example_id"] for row in selected}
    references = {row["source_example_id"]: row["answers"] for row in selected}

    # per-row evidence CSV
    evidence_rows = []
    predictions: dict[str, str | None] = {}
    for r in results:
        source_id = doc_to_source.get(r.doc_id)
        is_ok = r.status == "completed" and not r.error and r.output_text is not None
        predictions[source_id] = r.output_text if is_ok else None
        evidence_rows.append({
            "source_example_id": source_id,
            "status": r.status,
            "error": r.error or "",
            "output_len": len(r.output_text) if r.output_text else 0,
            "prediction": r.output_text or "",
            "reference_answers": json.dumps(references.get(source_id, []), ensure_ascii=False),
        })
    csv_path = output_dir / "per_row_evidence.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["source_example_id", "status", "error", "output_len", "prediction", "reference_answers"])
        writer.writeheader()
        writer.writerows(evidence_rows)

    # manifest (sampled IDs + hash)
    manifest = {
        "mode": args.mode,
        "sample_count": len(selected),
        "sample_hash": sample_hash,
        "workload_content_hash": workload_hash,
        "source_example_ids": [row["source_example_id"] for row in selected],
    }
    (output_dir / "sample_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    quality = squad_quality_metrics(predictions, references)

    success = sum(1 for r in results if r.status == "completed" and not r.error and r.output_text is not None)
    null_response = sum(1 for r in results if r.output_text is None)
    error_count = sum(1 for r in results if r.status != "completed" or r.error)
    max_tokens_errors = sum(1 for r in results if r.error and "max_tokens" in (r.error or ""))

    # vLLM token deltas (actual output workload, NOT a finish_reason proxy)
    prompt_delta = metrics_after.get("vllm:prompt_tokens_total", 0) - metrics_before.get("vllm:prompt_tokens_total", 0)
    gen_delta = metrics_after.get("vllm:generation_tokens_total", 0) - metrics_before.get("vllm:generation_tokens_total", 0)
    pc_queries = metrics_after.get("vllm:prefix_cache_queries_total", 0) - metrics_before.get("vllm:prefix_cache_queries_total", 0)
    pc_hits = metrics_after.get("vllm:prefix_cache_hits_total", 0) - metrics_before.get("vllm:prefix_cache_hits_total", 0)
    avg_gen_tokens = (gen_delta / len(selected)) if selected and gen_delta else 0.0

    operator_only_jct = (results[0].completed_at_s - results[0].started_at_s) if results else 0.0
    setup_s = (results[0].started_at_s - results[0].submitted_at_s) if results else 0.0

    report = {
        "gate": "squad_v11_dev_capability",
        "role": "capability_NOT_formal_ranking",
        "arm": "duckdb_ai",
        "mode": args.mode,
        "row_count": len(selected),
        "cap": args.max_tokens,
        "timing": {
            "adapter_wall_s": round(adapter_wall_s, 3),
            "operator_only_jct_s": round(operator_only_jct, 3),
            "setup_s": round(setup_s, 3),
            "boundary": "operator-only (database-E2E runner not implemented; no E2E ranking)",
        },
        "exactly_once": exactly_once,
        "result_count": result_count,
        "unique_result_ids": unique_result_ids,
        "success_count": success,
        "null_response_count": null_response,
        "error_count": error_count,
        "max_tokens_error_count": max_tokens_errors,
        "finish_reason": "unavailable (DuckDB-ai extension does not expose finish_reason)",
        "truncation_claim": (
            f"On this {len(selected)}-row {args.mode} sample, DuckDB-ai returned "
            f"{success} completed, {error_count} errors, {max_tokens_errors} identifiable "
            "max_tokens errors. DuckDB-ai does not expose finish_reason, so truncation "
            "cannot be directly confirmed; the vLLM generation-token delta is the actual "
            "output workload signal."
        ),
        "vllm_metrics": {
            "prompt_tokens_delta": int(prompt_delta),
            "generation_tokens_delta": int(gen_delta),
            "avg_generation_tokens_per_row": round(avg_gen_tokens, 2),
            "prefix_cache_queries_delta": int(pc_queries),
            "prefix_cache_hits_delta": int(pc_hits),
        },
        "squad_quality": quality,
        "sample_hash": sample_hash,
        "workload_content_hash": workload_hash,
        "identity": {
            "git_commit": _git_commit(),
            "model": args.model,
            "endpoint": config.endpoint_base_url,
            "workload": args.workload_name,
            "duckdb_version": runtime_id.get("duckdb_version"),
            "duckdb_ai_extension_version": runtime_id.get("duckdb_ai_extension_version"),
            **_pg_server_identity(args.database_url),
        },
        "evidence_files": {
            "per_row_csv": "per_row_evidence.csv",
            "sample_manifest": "sample_manifest.json",
        },
        "command": "python " + " ".join(sys.argv),
    }
    (output_dir / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
