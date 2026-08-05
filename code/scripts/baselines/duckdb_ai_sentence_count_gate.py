#!/usr/bin/env python3
"""DuckDB-ai full semantic gate on the synthetic bounded-output sentence-count track.

Single verifiable goal: confirm DuckDB-ai can execute the full 2048-row
sentence-count workload with **zero row-level failures** (no max_tokens
truncation), and that the returned integers are actually correct against a
deterministic sentence splitter ground truth.

This is a capability / microbenchmark gate, NOT a formal product ranking:
1-token output only exercises SQL/framework call overhead, prompt prefill,
request concurrency and per-row materialization. It does not exercise decode,
streaming, long/short output variance or submission scheduling. Any speedup
here supports only "project is more efficient on short-scalar LLM call chains",
not "project AI_COMPLETE is universally better than DuckDB".

Ground truth uses a deterministic regex sentence splitter on the ORIGINAL text
(recovered from the wrapped prompt); exact-match accuracy and invalid-output
rate are reported alongside the zero-failure check.
"""

from __future__ import annotations

import argparse
import re
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

try:
    import psycopg
except ImportError as exc:  # pragma: no cover
    raise SystemExit("requires psycopg; run inside the driver venv") from exc

from src.baselines.common.contracts import ChatRequest  # noqa: E402
from src.baselines.text.products.duckdb_ai import (  # noqa: E402
    DuckDBAiConfig,
    run_duckdb_ai_complete,
)

_WRAP_PREFIX = (
    "Reply with ONLY a single integer equal to the number of sentences "
    "in the text. Output nothing else. Text: "
)
_INTEGER_RE = re.compile(r"-?\d+")


def _sentence_count(text: str) -> int:
    """Deterministic sentence splitter: count clause segments on .!? boundaries."""

    cleaned = text.strip()
    if not cleaned:
        return 0
    segments = re.split(r"(?<=[.!?])\s+", cleaned)
    return sum(1 for seg in segments if seg.strip())


def _recover_original(wrapped: str) -> str:
    idx = wrapped.find(_WRAP_PREFIX)
    if idx < 0:
        return wrapped
    return wrapped[idx + len(_WRAP_PREFIX) :]


def _load_rows(database_url: str, workload: str, row_count: int):
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT doc_id, text, prompt_tokens, arrival_time_s "
                "FROM documents WHERE workload_name = %s "
                "ORDER BY doc_id LIMIT %s",
                (workload, row_count),
            )
            return cursor.fetchall()


def _parse(args: argparse.Namespace) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--workload-name", default="sharegpt_sentence_count")
    parser.add_argument("--row-count", type=int, default=2048)
    parser.add_argument("--endpoint-url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--api-key", default="EMPTY")
    parser.add_argument("--max-tokens", type=int, default=16)
    parser.add_argument("--max-concurrent-requests", type=int, default=32)
    parser.add_argument("--output", help="write JSON summary here")
    return parser.parse_args(args)


def main(argv: list[str] | None = None) -> int:
    args = _parse(sys.argv[1:] if argv is None else argv)
    rows = _load_rows(args.database_url, args.workload_name, args.row_count)
    if len(rows) < args.row_count:
        print(
            f"WARNING: requested {args.row_count} rows, workload has "
            f"{len(rows)}; proceeding with {len(rows)}"
        )
    base_url = args.endpoint_url
    suffix = "/chat/completions"
    if base_url.endswith(suffix):
        base_url = base_url[: -len(suffix)]
    else:
        raise SystemExit(
            "endpoint-url must end with /v1/chat/completions; got "
            + args.endpoint_url
        )
    config = DuckDBAiConfig(
        endpoint_base_url=base_url,
        model=args.model,
        api_key=args.api_key,
        max_tokens=args.max_tokens,
        max_concurrent_requests=args.max_concurrent_requests,
    )
    requests = tuple(
        ChatRequest(
            doc_id=int(doc_id),
            prompt=text,
            arrival_time_s=float(arrival or 0.0),
            prompt_tokens=int(prompt_tokens or 0),
            max_output_tokens=args.max_tokens,
            estimated_output_tokens=2,
            source_row_hash=f"sc-{doc_id}",
            endpoint_index=0,
        )
        for doc_id, text, prompt_tokens, arrival in rows
    )
    originals = {req.doc_id: _recover_original(req.prompt) for req in requests}
    ground_truth = {doc_id: _sentence_count(orig) for doc_id, orig in originals.items()}

    print(
        f"[sc-gate] rows={len(requests)} cap={args.max_tokens} "
        f"endpoint={base_url} model={args.model}",
        flush=True,
    )
    started = time.time()
    results = run_duckdb_ai_complete(requests, config)
    wall = time.time() - started

    total = len(results)
    failed = [r for r in results if r.status != "completed" or r.error or r.output_text is None]
    valid = {}
    invalid_outputs = []
    for r in results:
        out = (r.output_text or "").strip()
        match = _INTEGER_RE.search(out)
        if match is None:
            invalid_outputs.append((r.doc_id, out))
        else:
            valid[r.doc_id] = int(match.group())
    correct = sum(1 for doc_id, val in valid.items() if val == ground_truth.get(doc_id))
    near = sum(
        1
        for doc_id, val in valid.items()
        if abs(val - ground_truth.get(doc_id, -999)) == 1
    )

    summary = {
        "track": "synthetic_bounded_output_sentence_count",
        "role": "capability_microbenchmark_NOT_formal_product_ranking",
        "row_count": total,
        "cap": args.max_tokens,
        "wall_s": round(wall, 3),
        "failed_count": len(failed),
        "zero_failure_pass": len(failed) == 0,
        "valid_integer_count": len(valid),
        "invalid_output_count": len(invalid_outputs),
        "exact_match_accuracy": round(correct / total, 4) if total else 0.0,
        "off_by_one_count": near,
        "off_by_one_rate": round(near / total, 4) if total else 0.0,
        "rows_per_s": round(total / wall, 3) if wall else 0.0,
        "ground_truth_distribution": _dist(list(ground_truth.values())),
        "model_output_distribution": _dist(list(valid.values())),
        "invalid_samples": invalid_outputs[:5],
    }
    import json

    print(json.dumps(summary, indent=2, sort_keys=True))
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    return 0 if summary["zero_failure_pass"] else 2


def _dist(values: list[int]) -> dict[str, int]:
    buckets: dict[str, int] = {}
    for v in values:
        key = "0" if v == 0 else ("1-2" if v <= 2 else ("3-5" if v <= 5 else ("6-10" if v <= 10 else "11+")))
        buckets[key] = buckets.get(key, 0) + 1
    return buckets


if __name__ == "__main__":
    raise SystemExit(main())
