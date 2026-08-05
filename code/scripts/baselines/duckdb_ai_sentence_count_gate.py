#!/usr/bin/env python3
"""DuckDB-ai full semantic gate on the synthetic bounded-output sentence-count track.

Single verifiable goal: confirm DuckDB-ai can execute the full 2048-row
sentence-count workload with **zero row-level failures AND zero invalid-format
outputs** (every row a bare integer; DuckDB-ai reports no truncation/NULL error),
and report exact-match accuracy against a deterministic sentence-splitter ground
truth. The DuckDB-ai extension does not expose ``finish_reason``, so this gate
proves "no truncation error reported", NOT an explicitly observed
``finish_reason=stop``.

Capability / microbenchmark only, NOT a formal product ranking: 1-token output
exercises only SQL/framework call overhead, prompt prefill, request concurrency
and per-row materialization. Any speedup supports only "project is more efficient
on short-scalar LLM call chains", not "project AI_COMPLETE is universally better".

Fail-closed rules (exit 2 unless ALL hold):
  * workload has exactly --row-count rows (no silent partial run);
  * every row status completed with non-null response (zero truncation/NULL);
  * every response is a bare integer (strict fullmatch, not substring search);
  * output JSON is written atomically and never silently overwritten (--force).

Ground truth uses a deterministic regex sentence splitter on the ORIGINAL text
(recovered from the wrapped prompt). On ShareGPT multi-turn dialogue this is
ambiguous (model caps 3-10, regex counts 11+), so accuracy is a reported
diagnostic, not a pass/fail gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
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

_WRAP_PREFIX = (
    "Reply with ONLY a single integer equal to the number of sentences "
    "in the text. Output nothing else. Text: "
)
_INTEGER_RE = re.compile(r"-?\d+\Z")


def _sentence_count(text: str) -> int:
    cleaned = text.strip()
    if not cleaned:
        return 0
    segments = re.split(r"(?<=[.!?])\s+", cleaned)
    return sum(1 for seg in segments if seg.strip())


def _recover_original(wrapped: str) -> str:
    idx = wrapped.find(_WRAP_PREFIX)
    return wrapped[idx + len(_WRAP_PREFIX):] if idx >= 0 else wrapped


def _validate_exact_row_count(rows: list, expected: int, workload_name: str) -> None:
    """Fail closed unless exactly ``expected`` rows were read.

    Callers MUST query with ``LIMIT expected + 1`` so that a workload with MORE
    rows than expected is caught (a bare ``LIMIT expected`` would silently hide
    the extra row and pass).
    """

    if len(rows) != expected:
        comparison = "more than" if len(rows) > expected else "fewer than"
        raise SystemExit(
            f"FAIL: workload {workload_name!r} has {comparison} {expected} rows "
            f"(read {len(rows)} with LIMIT {expected + 1}); refusing run — "
            "use exactly the pre-registered row count"
        )


def _git_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=CODE_ROOT, capture_output=True, text=True, check=True,
        ).stdout.strip()
    except Exception:
        return "unknown"


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--workload-name", default="sharegpt_sentence_count")
    parser.add_argument("--row-count", type=int, default=2048)
    parser.add_argument("--endpoint-url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--api-key", default="EMPTY")
    parser.add_argument("--max-tokens", type=int, default=16)
    parser.add_argument("--max-concurrent-requests", type=int, default=32)
    parser.add_argument("--output", required=True, help="JSON summary output path")
    parser.add_argument(
        "--force", action="store_true",
        help="allow overwriting an existing --output file",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse(sys.argv[1:] if argv is None else argv)
    output_path = Path(args.output)
    if output_path.exists() and not args.force:
        raise SystemExit(
            f"output {output_path} already exists; refuse to silently overwrite "
            "(pass --force to replace)"
        )

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("requires psycopg; run inside the driver venv") from exc
    with psycopg.connect(args.database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT doc_id, text, prompt_tokens, arrival_time_s "
                "FROM documents WHERE workload_name = %s "
                "ORDER BY doc_id LIMIT %s",
                (args.workload_name, args.row_count + 1),
            )
            rows = cursor.fetchall()
    _validate_exact_row_count(rows, args.row_count, args.workload_name)

    suffix = "/chat/completions"
    if not args.endpoint_url.endswith(suffix):
        raise SystemExit("endpoint-url must end with /v1/chat/completions")
    base_url = args.endpoint_url[: -len(suffix)]

    config = DuckDBAiConfig(
        endpoint_base_url=base_url,
        model=args.model,
        api_key=args.api_key,
        max_tokens=args.max_tokens,
        max_concurrent_requests=args.max_concurrent_requests,
    )
    runtime_id = inspect_duckdb_ai_runtime(config)

    requests = tuple(
        ChatRequest(
            doc_id=int(doc_id),
            prompt=text,
            arrival_time_s=float(arrival or 0.0),
            prompt_tokens=int(prompt_tokens or 0),
            max_output_tokens=args.max_tokens,
            estimated_output_tokens=2,
            source_row_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
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
    valid: dict[int, int] = {}
    invalid_outputs: list[tuple[int, str]] = []
    for r in results:
        out = (r.output_text or "").strip()
        if _INTEGER_RE.fullmatch(out):
            valid[r.doc_id] = int(out)
        else:
            invalid_outputs.append((r.doc_id, out))
    correct = sum(1 for doc_id, val in valid.items() if val == ground_truth.get(doc_id))
    near = sum(
        1 for doc_id, val in valid.items()
        if abs(val - ground_truth.get(doc_id, -999)) == 1
    )

    zero_failure = len(failed) == 0
    zero_invalid = len(invalid_outputs) == 0
    passed = zero_failure and zero_invalid
    summary = {
        "track": "synthetic_bounded_output_sentence_count",
        "role": "capability_microbenchmark_NOT_formal_product_ranking",
        "passed": passed,
        "pass_criteria": {
            "zero_row_failure": zero_failure,
            "zero_invalid_format": zero_invalid,
            "row_count_exact": len(rows) == args.row_count,
        },
        "row_count": total,
        "cap": args.max_tokens,
        "wall_s": round(wall, 3),
        "failed_count": len(failed),
        "valid_integer_count": len(valid),
        "invalid_output_count": len(invalid_outputs),
        "exact_match_accuracy": round(correct / total, 4) if total else 0.0,
        "off_by_one_rate": round(near / total, 4) if total else 0.0,
        "rows_per_s": round(total / wall, 3) if wall else 0.0,
        "ground_truth_distribution": _dist(list(ground_truth.values())),
        "model_output_distribution": _dist(list(valid.values())),
        "invalid_samples": invalid_outputs[:5],
        "evidence": {
            "git_commit": _git_commit(),
            "duckdb_version": runtime_id.get("duckdb_version"),
            "duckdb_ai_extension_version": runtime_id.get("duckdb_ai_extension_version"),
            "model": args.model,
            "endpoint_base_url": base_url,
            "workload_name": args.workload_name,
            "max_concurrent_requests": args.max_concurrent_requests,
            "args": {
                "row_count": args.row_count,
                "max_tokens": args.max_tokens,
                "workload_name": args.workload_name,
            },
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_path.with_name("." + output_path.name + ".tmp")
    tmp.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(output_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if passed else 2


def _dist(values: list[int]) -> dict[str, int]:
    buckets: dict[str, int] = {}
    for v in values:
        key = "0" if v == 0 else ("1-2" if v <= 2 else ("3-5" if v <= 5 else ("6-10" if v <= 10 else "11+")))
        buckets[key] = buckets.get(key, 0) + 1
    return buckets


if __name__ == "__main__":
    raise SystemExit(main())
