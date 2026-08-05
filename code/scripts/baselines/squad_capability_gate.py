#!/usr/bin/env python3
"""SQuAD v1.1 dev capability gate (DuckDB-ai arm) -- final rigorous version.

Capability gate (NOT a formal ranking): one arm, operator-only timing boundary
only. The database-E2E top-level runner is not implemented, so no full
database-system ranking is produced here.

Fixes per codex sixth review:
  * command redaction: ``sys.argv`` is redacted via the shared
    ``src.baselines.common.redact`` module (DB-URL password stripped,
    ``--api-key`` -> ``***``); no raw secret reaches evidence.
  * ``--mode full`` is fail-closed: verifies row_count == expected (10570),
    unique doc_id, unique source_example_id, non-empty reference_answers, AND
    recomputes the importer's canonical content hash (SHA256 over
    ``{id, prompt, references}`` sorted by id) matching the importer
    provenance sidecar. These workload-integrity checks run in BOTH modes.
  * workload + sample hashes are structured JSON-per-row SHA256 (not an
    MD5 of prompt text and not a bare ID concat); the workload hash must
    equal the importer's archived ``content_hash``.
  * vLLM counter attribution: endpoint must be idle (running == 0,
    waiting == 0) and scraped successfully before AND after a settle delay;
    ``request_success_delta`` must equal the number of requests sent;
    counters must be monotonic. Any failure marks token/cache metrics
    ``attribution = unavailable`` (or, with ``--strict-attribution``, fails
    the whole gate).
  * bucketing uses ``max`` normalized reference token count over ALL answers
    (not ``answers[0]``); quota allocation is largest-remainder so per-bucket
    quotas sum exactly to target without dropping a guaranteed row.
  * exactly-once is a full set check (result id set == input id set) plus
    source_example_id uniqueness/non-empty.
  * ``output_len`` renamed to ``output_chars`` (it is ``len(text)``, not
    tokens).
  * failures are archived: a top-level wrapper writes a structured
    ``failure_report.json`` (status, failure_stage, exception_type,
    sanitized_error, timing, redacted command, identity, partial files) and
    returns non-zero.
  * identity records service_prefix_caching, vLLM version, service config
    hash, DuckDB extension source, GPU/server identity, metrics snapshot
    status.
"""

from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import json
import math
import socket
import subprocess
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
from src.baselines.text.products.duckdb_ai import (  # noqa: E402
    DuckDBAiConfig,
    inspect_duckdb_ai_runtime,
    run_duckdb_ai_complete,
)
from src.observability.metrics import (  # noqa: E402
    normalize_squad_answer,
    scrape_prometheus_metrics,
    squad_quality_metrics,
)

# Must equal import_squad_workload.EXPECTED_DEV_COUNT and the importer's
# compute_content_hash definition. Pinned by test_content_hash_matches_importer.
EXPECTED_DEV_COUNT = 10570

# Cumulative vLLM counters checked for monotonicity (no reset) and attribution.
_ATTRIBUTION_COUNTERS = (
    "vllm:prompt_tokens_total",
    "vllm:generation_tokens_total",
    "vllm:request_success_total",
    "vllm:prefix_cache_queries_total",
    "vllm:prefix_cache_hits_total",
)


def _answer_bucket(answers: list[str]) -> str:
    """Bucket by the LONGEST reference answer (max normalized token count).

    SQuAD dev questions carry up to 3 reference answers of differing length;
    using the max covers the longest acceptable answer and stresses the cap.
    """

    if not answers:
        return "short"
    max_words = max(
        len(normalize_squad_answer(answer).split()) for answer in answers
    )
    if max_words <= 1:
        return "short"
    if max_words <= 4:
        return "medium"
    return "long"


def largest_remainder_allocation(sizes: list[int], target: int) -> list[int]:
    """Proportional allocation with sum == target via the largest-remainder rule.

    Floors each exact quota, then distributes the leftover seats to the
    buckets with the largest fractional remainder. Pure and unit-testable.
    Correct for ``target <= sum(sizes)``; the ``min(floor, size)`` cap is
    defensive for floating-point edges.
    """

    n = len(sizes)
    total = sum(sizes)
    if target <= 0 or n == 0 or total == 0:
        return [0] * n
    raw = [target * size / total for size in sizes]
    base = [min(int(math.floor(value)), sizes[i]) for i, value in enumerate(raw)]
    leftover = target - sum(base)
    order = sorted(
        range(n),
        key=lambda i: raw[i] - math.floor(raw[i]),
        reverse=True,
    )
    # Pass 1: at most ONE bonus seat per distinct bucket (true largest
    # remainder) -- this handles the normal case where leftover < #buckets.
    for idx in order:
        if leftover <= 0:
            break
        if base[idx] < sizes[idx]:
            base[idx] += 1
            leftover -= 1
    # Pass 2: if seats remain (target close to total after size caps),
    # distribute round-robin to any bucket that still has room.
    guard = 0
    while leftover > 0 and guard < sum(sizes) + 1:
        progressed = False
        for idx in order:
            if leftover <= 0:
                break
            if base[idx] < sizes[idx]:
                base[idx] += 1
                leftover -= 1
                progressed = True
        if not progressed:
            break
        guard += 1
    return base


def stratified_sample(rows: list[dict], target: int) -> list[dict]:
    """Deterministic stratified sample by reference-answer word-count bucket.

    Largest-remainder allocation across short/medium/long buckets, even
    spacing within each bucket (sorted by source_example_id), deterministic
    top-up to exactly ``target``. Every non-empty bucket is represented when
    its proportional share is at least one seat; a deliberately tiny target
    can yield zero seats for a small minority bucket (correct proportional
    behavior). Pure function -- unit-testable.
    """

    if target >= len(rows):
        return sorted(rows, key=lambda r: r["source_example_id"])
    buckets: dict[str, list[dict]] = collections.defaultdict(list)
    for row in rows:
        buckets[_answer_bucket(row["answers"])].append(row)
    bucket_names = sorted(buckets)
    sizes = [len(buckets[name]) for name in bucket_names]
    quotas = largest_remainder_allocation(sizes, target)
    sampled: list[dict] = []
    seen: set = set()
    for name, quota in zip(bucket_names, quotas):
        in_bucket = sorted(buckets[name], key=lambda r: r["source_example_id"])
        if quota <= 0:
            continue
        if quota >= len(in_bucket):
            picked = in_bucket
        else:
            stride = len(in_bucket) / quota
            indices = [int(i * stride) for i in range(quota)]
            picked = [in_bucket[j] for j in indices]
        for row in picked:
            if row["source_example_id"] not in seen:
                sampled.append(row)
                seen.add(row["source_example_id"])
    if len(sampled) < target:
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


def _structured_content_hash(rows: list[dict]) -> str:
    """SHA256 over structured JSON ``{id, prompt, references}`` sorted by id.

    MUST match ``import_squad_workload.compute_content_hash`` so the gate's
    workload hash is directly comparable to the importer's archived
    provenance ``content_hash``. Pinned by test_content_hash_matches_importer.
    """

    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda r: r["source_example_id"]):
        payload = json.dumps(
            {
                "id": row["source_example_id"],
                "prompt": row["text"],
                "references": list(row["answers"]),
            },
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
        digest.update(payload)
    return digest.hexdigest()


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


def _validate_workload_integrity(
    rows: list[dict],
    expected_count: int,
    expected_content_hash: str,
) -> tuple[bool, list[str]]:
    """Fail-closed workload integrity checks (applied in BOTH modes)."""

    problems: list[str] = []
    if len(rows) != expected_count:
        problems.append(f"row_count={len(rows)} != expected {expected_count}")
    if len({r["doc_id"] for r in rows}) != len(rows):
        problems.append("duplicate doc_id present")
    source_ids = [r["source_example_id"] for r in rows]
    if len(set(source_ids)) != len(source_ids):
        problems.append("duplicate source_example_id present")
    if not all(r["source_example_id"] for r in rows):
        problems.append("empty source_example_id present")
    if not all(r["answers"] and all(s.strip() for s in r["answers"]) for r in rows):
        problems.append("empty/blank reference_answers present")
    actual_hash = _structured_content_hash(rows)
    if actual_hash != expected_content_hash:
        problems.append(
            f"workload content_hash {actual_hash} != importer provenance "
            f"{expected_content_hash}"
        )
    return (not problems), problems


def _delta(before: dict[str, float], after: dict[str, float], name: str) -> float:
    return max(0.0, after.get(name, 0.0) - before.get(name, 0.0))


def _endpoint_idle(metrics: dict[str, float]) -> tuple[bool, str]:
    if not metrics:
        return False, "scrape_empty"
    if (
        "vllm:num_requests_running" not in metrics
        or "vllm:num_requests_waiting" not in metrics
    ):
        return False, "gauge_missing"
    running = int(metrics.get("vllm:num_requests_running", 0))
    waiting = int(metrics.get("vllm:num_requests_waiting", 0))
    if running == 0 and waiting == 0:
        return True, "idle"
    return False, f"not_idle(running={running},waiting={waiting})"


def _scrape_status(metrics: dict[str, float]) -> str:
    if not metrics:
        return "empty"
    if (
        "vllm:num_requests_running" not in metrics
        or "vllm:num_requests_waiting" not in metrics
    ):
        return "gauge_missing"
    return "ok"


def _assess_attribution(
    metrics_before: dict[str, float],
    metrics_after: dict[str, float],
    requests_sent: int,
) -> tuple[dict[str, object], bool]:
    """Decide whether vLLM counter deltas are attributable to this run only."""

    reasons: list[str] = []
    before_ok, before_reason = _endpoint_idle(metrics_before)
    after_ok, after_reason = _endpoint_idle(metrics_after)
    if not before_ok:
        reasons.append(f"before:{before_reason}")
    if not after_ok:
        reasons.append(f"after:{after_reason}")
    resets = [
        name
        for name in _ATTRIBUTION_COUNTERS
        if metrics_after.get(name, 0.0) < metrics_before.get(name, 0.0)
    ]
    if resets:
        reasons.append(f"counter_reset:{resets}")
    request_success_delta = int(
        _delta(metrics_before, metrics_after, "vllm:request_success_total")
    )
    if request_success_delta != requests_sent:
        reasons.append(
            f"request_success_delta={request_success_delta} != "
            f"requests_sent={requests_sent}"
        )
    attribution_ok = not reasons
    summary = {
        "attributable": attribution_ok,
        "before_idle": before_ok,
        "after_idle": after_ok,
        "request_success_delta": request_success_delta,
        "requests_sent": requests_sent,
        "reasons": reasons,
    }
    return summary, attribution_ok


def _vllm_version(base_url: str) -> str:
    from urllib import error, request
    # vLLM serves /version at the ROOT (http://host:port/version), not under
    # /v1. Remove the full suffix; removing only ``v1`` leaves a trailing slash
    # and produces ``//version``, which some servers do not normalize.
    root = base_url.removesuffix("/v1").rstrip("/")
    for candidate in (f"{root}/version", f"{base_url}/version"):
        try:
            with request.urlopen(candidate, timeout=3.0) as response:
                body = json.loads(response.read().decode("utf-8", errors="replace"))
            return str(body.get("version", "unknown"))
        except (OSError, error.URLError, ValueError):
            continue
    return "unavailable"


def _gpu_identity() -> dict[str, object]:
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,name,memory.total,driver_version",
             "--format=csv,noheader"],
            capture_output=True, text=True, timeout=5.0,
        )
        if out.returncode == 0 and out.stdout.strip():
            lines = [line.strip() for line in out.stdout.splitlines() if line.strip()]
            return {"nvidia_smi": lines, "hostname": socket.gethostname()}
    except (OSError, subprocess.SubprocessError):
        pass
    return {"nvidia_smi": "unavailable", "hostname": socket.gethostname()}


def _pg_server_identity(database_url: str) -> dict[str, str]:
    import psycopg
    identity: dict[str, str] = {}
    try:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                identity["pg_server_version"] = str(cur.fetchone()[0])
    except Exception as exc:
        identity["pg_error"] = redact_text(str(exc)[:120])
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


def _load_importer_provenance(path: str) -> dict:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if "content_hash" not in data:
        raise SystemExit(
            f"importer provenance {path} missing content_hash; "
            "point --importer-provenance at the SQuAD importer sidecar JSON"
        )
    return data


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--workload-name", default="squad_v11_dev_short_answer")
    parser.add_argument("--mode", choices=("sampled", "full"), default="sampled")
    parser.add_argument("--sample-count", type=int, default=256)
    parser.add_argument(
        "--importer-provenance", required=True,
        help="path to the SQuAD importer provenance sidecar JSON "
        "(provides canonical content_hash + expected count)",
    )
    parser.add_argument("--expected-count", type=int, default=EXPECTED_DEV_COUNT)
    parser.add_argument("--endpoint-url", required=True)
    parser.add_argument("--metrics-url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--api-key", default="EMPTY")
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--max-concurrent-requests", type=int, default=32)
    parser.add_argument(
        "--service-prefix-caching", choices=("enabled", "disabled"),
        default="enabled",
        help="operator-declared vLLM prefix-cache state (must match live flags)",
    )
    parser.add_argument(
        "--service-config-hash", default=None,
        help="optional hash/label of the frozen vLLM service config",
    )
    parser.add_argument(
        "--metrics-settle-s", type=float, default=2.0,
        help="delay between run completion and the after-scrape so counters flush",
    )
    parser.add_argument(
        "--strict-attribution", action="store_true",
        help="fail the whole gate (non-zero) if vLLM counter attribution fails; "
        "default just marks token/cache metrics unavailable",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args(argv)


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


_GENERATED_EVIDENCE_FILES = (
    "failure_report.json",
    "partial_results.csv",
    "per_row_evidence.csv",
    "report.json",
    "sample_manifest.jsonl",
)


def _prepare_output_dir(output_dir: Path, *, force: bool) -> None:
    """Create a clean evidence target without deleting unrelated files."""

    if output_dir.exists() and not force:
        raise SystemExit(f"output dir {output_dir} exists; pass --force to overwrite")
    output_dir.mkdir(parents=True, exist_ok=True)
    if force:
        for name in _GENERATED_EVIDENCE_FILES:
            (output_dir / name).unlink(missing_ok=True)


def _write_sample_manifest(path: Path, rows: list[dict]) -> None:
    """Archive the exact structured rows needed to recompute the sample hash."""

    with path.open("w", encoding="utf-8") as handle:
        for row in sorted(rows, key=lambda item: item["source_example_id"]):
            payload = {
                "id": row["source_example_id"],
                "prompt": row["text"],
                "references": list(row["answers"]),
            }
            handle.write(
                json.dumps(payload, sort_keys=True, ensure_ascii=False) + "\n"
            )


def _write_failure_report(
    output_dir: Path, args: argparse.Namespace, stage: str, exc: BaseException,
    started_at: float,
) -> None:
    existing = sorted(p.name for p in output_dir.iterdir()) if output_dir.exists() else []
    # Scrub embedded scheme://user:password@ credentials so a DSN echoed by a
    # database driver in the exception/traceback cannot leak into evidence.
    truncated_error = redact_text(str(exc)[:500])
    tb = redact_text("".join(traceback.format_exception(exc))[-4000:])
    suffix = "/chat/completions"
    endpoint_base = (
        args.endpoint_url[: -len(suffix)]
        if args.endpoint_url.endswith(suffix) else args.endpoint_url
    )
    report = {
        "status": "failure",
        "failure_stage": stage,
        "exception_type": type(exc).__name__,
        "sanitized_error": truncated_error,
        "traceback": tb,
        "started_at_s": round(started_at, 3),
        "finished_at_s": round(time.time(), 3),
        "redacted_command": redact_argument_list(list(sys.argv)),
        "identity": {
            "git_commit": _git_commit(),
            "hostname": socket.gethostname(),
            "model": args.model,
            "endpoint": redact_database_url(endpoint_base),
            "service_prefix_caching": args.service_prefix_caching,
            "service_config_hash": args.service_config_hash or "not_provided",
            "database_url": redact_database_url(args.database_url),
        },
        "partial_files": existing,
    }
    _write_json(output_dir / "failure_report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))


def _run(args: argparse.Namespace, output_dir: Path) -> int:
    importer = _load_importer_provenance(args.importer_provenance)
    expected_content_hash = importer["content_hash"]
    if importer.get("sample_count"):
        args.expected_count = int(importer["sample_count"])

    all_rows = _load_workload(args.database_url, args.workload_name)
    integrity_ok, integrity_problems = _validate_workload_integrity(
        all_rows, args.expected_count, expected_content_hash
    )
    if not integrity_ok:
        raise SystemExit(
            "FAIL: workload integrity check failed: " + "; ".join(integrity_problems)
        )

    if args.mode == "full":
        selected = sorted(all_rows, key=lambda r: r["source_example_id"])
    else:
        selected = stratified_sample(all_rows, args.sample_count)
    workload_content_hash = _structured_content_hash(all_rows)
    sample_content_hash = _structured_content_hash(selected)
    _write_sample_manifest(output_dir / "sample_manifest.jsonl", selected)

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
    time.sleep(max(0.0, args.metrics_settle_s))
    metrics_after = scrape_prometheus_metrics(args.metrics_url)
    adapter_wall_s = run_finished - run_started

    # 3-way exactly-once: counts + full id-set equality.
    input_count = len(requests)
    input_doc_ids = {r.doc_id for r in requests}
    result_doc_ids = {r.doc_id for r in results}
    source_ids = [row["source_example_id"] for row in selected]
    source_id_set = set(source_ids)
    exactly_once = (
        len(results) == input_count
        and len(result_doc_ids) == input_count
        and result_doc_ids == input_doc_ids
        and len(source_id_set) == len(selected)
        and all(source_ids)
    )
    if not exactly_once:
        # Archive whatever came back so the failure is auditable.
        _archive_partial_evidence(output_dir, results, selected)
        raise SystemExit(
            "FAIL: exactly-once violated (result id set != input id set, "
            "or source_example_id not unique/non-empty)"
        )

    doc_to_source = {row["doc_id"]: row["source_example_id"] for row in selected}
    references = {row["source_example_id"]: row["answers"] for row in selected}

    evidence_rows = []
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
        })
    with (output_dir / "per_row_evidence.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["source_example_id", "status", "error", "output_chars",
                        "prediction", "reference_answers"],
        )
        writer.writeheader()
        writer.writerows(evidence_rows)

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

    attribution, attribution_ok = _assess_attribution(
        metrics_before, metrics_after, requests_sent=len(selected)
    )
    if args.strict_attribution and not attribution_ok:
        # Preserve completed predictions for forensics before failing the run.
        _archive_partial_evidence(output_dir, results, selected)
        raise SystemExit(
            "FAIL: --strict-attribution and vLLM counter attribution failed: "
            + "; ".join(attribution["reasons"])
        )

    if attribution_ok:
        prompt_delta = int(_delta(metrics_before, metrics_after, "vllm:prompt_tokens_total"))
        gen_delta = int(_delta(metrics_before, metrics_after, "vllm:generation_tokens_total"))
        pc_queries = int(_delta(metrics_before, metrics_after, "vllm:prefix_cache_queries_total"))
        pc_hits = int(_delta(metrics_before, metrics_after, "vllm:prefix_cache_hits_total"))
        avg_gen_tokens = (gen_delta / len(selected)) if selected and gen_delta else 0.0
        vllm_metrics = {
            "attribution": "attributable",
            "prompt_tokens_delta": prompt_delta,
            "generation_tokens_delta": gen_delta,
            "avg_generation_tokens_per_row": round(avg_gen_tokens, 2),
            "prefix_cache_queries_delta": pc_queries,
            "prefix_cache_hits_delta": pc_hits,
            "prefix_cache_hit_rate": round(pc_hits / pc_queries, 4) if pc_queries else 0.0,
        }
    else:
        vllm_metrics = {
            "attribution": "unavailable",
            "reasons": attribution["reasons"],
            "note": ("token/cache deltas recorded only when the endpoint is "
                     "exclusive and idle before/after; see attribution block"),
        }

    operator_only_jct = (
        results[0].completed_at_s - results[0].started_at_s if results else 0.0
    )
    setup_s = (
        results[0].started_at_s - results[0].submitted_at_s if results else 0.0
    )

    report = {
        "gate": "squad_v11_dev_capability",
        "role": "capability_NOT_formal_ranking",
        "status": "success",
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
        "result_count": len(results),
        "unique_result_ids": len(result_doc_ids),
        "success_count": success_count,
        "null_response_count": null_response,
        "error_count": error_count,
        "max_tokens_error_count": max_tokens_errors,
        "finish_reason": "unavailable (DuckDB-ai extension does not expose finish_reason)",
        "truncation_claim": (
            f"On this {len(selected)}-row {args.mode} selection, DuckDB-ai returned "
            f"{success_count} completed, {error_count} errors, {max_tokens_errors} "
            "identifiable max_tokens errors. DuckDB-ai does not expose finish_reason, "
            "so truncation cannot be directly confirmed; the vLLM generation-token "
            "delta (when attributable) is the actual output workload signal."
        ),
        "vllm_metrics": vllm_metrics,
        "attribution": attribution,
        "squad_quality": quality,
        "sample_content_hash": sample_content_hash,
        "workload_content_hash": workload_content_hash,
        "importer_content_hash": expected_content_hash,
        "workload_integrity": "verified" if integrity_ok else "failed",
        "identity": {
            "git_commit": _git_commit(),
            "model": args.model,
            "endpoint": redact_database_url(config.endpoint_base_url),
            "workload": args.workload_name,
            "duckdb_version": runtime_id.get("duckdb_version"),
            "duckdb_ai_extension_version": runtime_id.get("duckdb_ai_extension_version"),
            "duckdb_ai_extension_source": runtime_id.get("duckdb_ai_extension_source"),
            "vllm_version": _vllm_version(config.endpoint_base_url),
            "service_prefix_caching": args.service_prefix_caching,
            "service_config_hash": args.service_config_hash or "not_provided",
            **_pg_server_identity(args.database_url),
            **_gpu_identity(),
            "metrics_snapshot": {
                "before": _scrape_status(metrics_before),
                "after": _scrape_status(metrics_after),
            },
        },
        "evidence_files": {
            "per_row_csv": "per_row_evidence.csv",
            "sample_manifest_jsonl": "sample_manifest.jsonl",
        },
        "command": redact_argument_list(list(sys.argv)),
    }
    _write_json(output_dir / "report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


def _archive_partial_evidence(
    output_dir: Path, results, selected,
) -> None:
    """Best-effort archive of whatever rows came back after a failure."""

    doc_to_source = {row["doc_id"]: row["source_example_id"] for row in selected}
    rows_out = []
    for r in results:
        rows_out.append({
            "doc_id": r.doc_id,
            "source_example_id": doc_to_source.get(r.doc_id),
            "status": r.status,
            "error": redact_text(r.error or ""),
            "output_chars": len(r.output_text) if r.output_text else 0,
        })
    with (output_dir / "partial_results.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["doc_id", "source_example_id", "status", "error", "output_chars"]
        )
        writer.writeheader()
        writer.writerows(rows_out)


def main(argv=None) -> int:
    args = _parse(sys.argv[1:] if argv is None else argv)
    output_dir = Path(args.output_dir)
    _prepare_output_dir(output_dir, force=args.force)
    started_at = time.time()
    try:
        return _run(args, output_dir)
    except BaseException as exc:
        _write_failure_report(output_dir, args, "run", exc, started_at)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
