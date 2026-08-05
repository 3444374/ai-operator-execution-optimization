#!/usr/bin/env python3
"""256-row SQuAD v1.1 dev capability gate (DuckDB-ai arm).

Single verifiable goal: on a 256-row stratified slice of the SQuAD dev
workload, confirm the DuckDB-ai path produces **parseable, example-ID-aligned
predictions** that the shared ``squad_quality_metrics`` evaluator can score,
and record success/error/NULL/truncation + EM/F1 + exactly-once under the
operator-only timing boundary. This is a CAPABILITY gate, not a formal ranking:
one arm (DuckDB-ai), no database-E2E (top-level E2E runner not implemented),
no cross-arm comparison.

Records (per codex directive):
  * output parsing + example-ID alignment (doc_id -> source_example_id);
  * success / error / NULL-response / truncation counts;
  * finish_reason: the DuckDB-ai extension does not expose it -> "unavailable";
  * EM/F1, missing rows, exactly-once (via squad_quality_metrics);
  * operator-only JCT (adapter submitted->started = setup, started->completed
    = operator-only);
  * explicitly NOT a database-E2E ranking.

The same evaluator + manifest serve the direct-client and project arms (their
alignment is structurally identical); this gate verifies the DuckDB-ai arm,
which carries the finish_reason=length-as-error semantic.
"""

from __future__ import annotations

import argparse
import json
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
from src.observability.metrics import squad_quality_metrics  # noqa: E402


def _load_stratified(database_url, workload, row_count):
    import psycopg
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "WITH ranked AS ("
                " SELECT doc_id, text, source_example_id, reference_answers,"
                " row_number() OVER (ORDER BY doc_id) AS rn"
                " FROM documents WHERE workload_name = %s"
                ") SELECT doc_id, text, source_example_id, reference_answers "
                "FROM ranked WHERE rn %% %s = 1 LIMIT %s",
                (workload, max(1, 10570 // row_count), row_count),
            )
            rows = cursor.fetchall()
    refs = {}
    for doc_id, text, source_example_id, reference_answers in rows:
        answers = reference_answers
        if isinstance(answers, str):
            answers = json.loads(answers)
        refs[doc_id] = (source_example_id, list(answers), text)
    return refs


def _endpoint_base_url(endpoint_url):
    suffix = "/chat/completions"
    if not endpoint_url.endswith(suffix):
        raise SystemExit("endpoint-url must end with /v1/chat/completions")
    return endpoint_url[: -len(suffix)]


def _parse(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--workload-name", default="squad_v11_dev_short_answer")
    parser.add_argument("--row-count", type=int, default=256)
    parser.add_argument("--endpoint-url", required=True)
    parser.add_argument("--metrics-url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--api-key", default="EMPTY")
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--max-concurrent-requests", type=int, default=32)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = _parse(sys.argv[1:] if argv is None else argv)
    output_dir = Path(args.output_dir)
    if output_dir.exists() and not args.force:
        raise SystemExit(f"output dir {output_dir} exists; pass --force to overwrite")

    rows = _load_stratified(args.database_url, args.workload_name, args.row_count)
    if len(rows) != args.row_count:
        raise SystemExit(
            f"FAIL: requested {args.row_count} rows, got {len(rows)} from "
            f"{args.workload_name!r}"
        )
    requests = tuple(
        ChatRequest(
            doc_id=doc_id,
            prompt=text,
            arrival_time_s=0.0,
            prompt_tokens=max(1, len(text) // 4),
            max_output_tokens=args.max_tokens,
            estimated_output_tokens=10,
            source_row_hash=str(doc_id),
            endpoint_index=0,
        )
        for doc_id, (_src, _answers, text) in rows.items()
    )
    config = DuckDBAiConfig(
        endpoint_base_url=_endpoint_base_url(args.endpoint_url),
        model=args.model,
        api_key=args.api_key,
        max_tokens=args.max_tokens,
        max_concurrent_requests=args.max_concurrent_requests,
    )
    runtime_id = inspect_duckdb_ai_runtime(config)

    print(
        f"[squad-cap] rows={len(requests)} cap={args.max_tokens} model={args.model}",
        flush=True,
    )
    started = time.time()
    results = run_duckdb_ai_complete(requests, config)
    wall = time.time() - started

    total = len(results)
    doc_ids_seen = {r.doc_id for r in results}
    exactly_once = doc_ids_seen == set(rows)
    success = sum(1 for r in results if r.status == "completed" and not r.error and r.output_text is not None)
    null_response = sum(1 for r in results if r.output_text is None)
    error_rows = sum(1 for r in results if r.status != "completed" or r.error)
    truncation_errors = sum(
        1 for r in results if r.error and "max_tokens" in (r.error or "")
    )

    doc_to_source = {doc_id: src for doc_id, (src, _a, _t) in rows.items()}
    references = {src: answers for _doc, (src, answers, _t) in rows.items()}
    predictions = {}
    for r in results:
        source_id = doc_to_source.get(r.doc_id)
        if source_id is None:
            continue
        predictions[source_id] = r.output_text if (
            r.status == "completed" and not r.error and r.output_text is not None
        ) else None

    quality = squad_quality_metrics(predictions, references)
    op_only_s = (results[0].completed_at_s - results[0].started_at_s) if results else 0.0
    setup_s = (results[0].started_at_s - results[0].submitted_at_s) if results else 0.0

    report = {
        "gate": "squad_v11_dev_capability_256",
        "role": "capability_NOT_formal_ranking",
        "arm": "duckdb_ai",
        "row_count": total,
        "cap": args.max_tokens,
        "wall_s": round(wall, 3),
        "operator_only_jct_s": round(op_only_s, 3),
        "setup_s": round(setup_s, 3),
        "timing_boundary": "operator-only (database-E2E runner not implemented; no E2E ranking)",
        "exactly_once": exactly_once,
        "success_count": success,
        "null_response_count": null_response,
        "error_count": error_rows,
        "truncation_error_count": truncation_errors,
        "finish_reason": "unavailable (DuckDB-ai extension does not expose finish_reason)",
        "squad_quality": quality,
        "identity": {
            "model": args.model,
            "endpoint": config.endpoint_base_url,
            "workload": args.workload_name,
            "duckdb_version": runtime_id.get("duckdb_version"),
            "duckdb_ai_extension_version": runtime_id.get("duckdb_ai_extension_version"),
        },
        "note": (
            "Capability gate: verifies DuckDB-ai output parsing, example-ID "
            "alignment, and squad_quality_metrics wiring. Direct-client and "
            "project arms share the same evaluator + manifest (alignment "
            "structurally identical); not ranked here."
        ),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "squad_capability_256.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
