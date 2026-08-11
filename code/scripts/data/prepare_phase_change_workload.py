#!/usr/bin/env python3
"""Build a project-derived phase-change workload (NOT an official VTC reproduction).

Two jobs, same output cap (manifest guard requires global completion_max_tokens ==
manifest output cap), heterogeneous PROMPT shape only:
  Job A (client 0): short prompts (~256 tok) from a short pool, continuous Poisson.
  Job B (client 1): long prompts (~1024 tok) from a long pool, Poisson only during
                     ON phases [60,120] and [180,240] (OFF-first: 0-60 A-only).

Provenance MUST read: "project-derived phase-change workload; not official VTC reproduction".
arrival_time_scale must be 1.0 at run time (no compression of 60s phases).
"""
from __future__ import annotations
import argparse, hashlib, json, math, random, statistics, sys
from collections import defaultdict
from pathlib import Path
import psycopg

CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))
from src.baselines.common.contracts import ChatRequest  # noqa: E402
from src.baselines.common.manifests import assign_endpoint_equal_rows  # noqa: E402
from src.baselines.text.orchestration.postgres_manifest import source_row_hash  # noqa: E402

DERIVED_NOTE = "project-derived phase-change workload; not official VTC reproduction"


def fetch_pool(dsn, workload, target, max_dist, seed):
    rng = random.Random(seed)
    rows = []
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT doc_id, tenant_id, category, text, prompt_tokens, session_id, prefix_key "
            "FROM documents WHERE workload_name=%s ORDER BY doc_id", (workload,))
        for d, t, cat, text, pt, sess, pk in cur.fetchall():
            rows.append({"doc_id": d, "tenant_id": t, "category": cat, "text": text,
                         "prompt_tokens": pt, "session_id": sess or f"src-{d}", "prefix_key": pk or ""})
    if not rows:
        raise ValueError(f"source pool {workload} empty")
    used = set()
    order = sorted(rows, key=lambda r: (abs(r["prompt_tokens"] - target),
              hashlib.sha256(f"{seed}:{r['doc_id']}".encode()).hexdigest(), r["doc_id"]))
    for r in order:
        if abs(r["prompt_tokens"] - target) <= max_dist and r["doc_id"] not in used:
            used.add(r["doc_id"]); yield r
    # relax once: closest remaining within 2*max_dist
    for r in order:
        if abs(r["prompt_tokens"] - target) <= 2 * max_dist and r["doc_id"] not in used:
            used.add(r["doc_id"]); yield r


def poisson(rate, t_start, t_end, seed):
    rng = random.Random(seed)
    t = t_start; out = []
    while True:
        t += rng.expovariate(rate)
        if t >= t_end: break
        out.append(t)
    return out


def build(args):
    duration = args.duration_s
    on_windows = []  # Job B active windows (OFF-first: 60-120, 180-240)
    k = 0
    while True:
        a, b = k * 2 * args.period_s + args.period_s, (k + 1) * 2 * args.period_s
        if a >= duration: break
        on_windows.append((a, min(b, duration))); k += 1
    A = poisson(args.rate_a, 0.0, duration, args.seed + 104729 * 0)
    B = []
    for i, (a, b) in enumerate(on_windows):
        B.extend(poisson(args.rate_b, a, b, args.seed + 104729 * 1 + i))
    short_pool = list(fetch_pool(args.database_url, args.short_source, args.short_target, args.short_max_dist, args.seed))
    long_pool = list(fetch_pool(args.database_url, args.long_source, args.long_target, args.long_max_dist, args.seed + 7))
    need_a, need_b = len(A), len(B)
    if len(short_pool) < need_a:
        raise ValueError(f"short pool {len(short_pool)} < Job A needs {need_a}; stop (no repeat/loosen)")
    if len(long_pool) < need_b:
        raise ValueError(f"long pool {len(long_pool)} < Job B needs {need_b}; stop (no repeat/loosen)")
    short_pool = short_pool[:need_a]; long_pool = long_pool[:need_b]
    events = sorted([(t, 0) for t in A] + [(t, 1) for t in B])
    # materialize + PG import
    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    manifest_paths = []
    doc = args.doc_id_base
    by_client = defaultdict(list)
    dist_a, dist_b = [], []
    with psycopg.connect(args.database_url) as conn, conn.cursor() as cur:
        for arrival, ci in events:
            src = short_pool.pop(0) if ci == 0 else long_pool.pop(0)
            (dist_a if ci == 0 else dist_b).append(abs(src["prompt_tokens"] - (args.short_target if ci == 0 else args.long_target)))
            cur.execute(
                "INSERT INTO documents (doc_id, tenant_id, category, text, workload_name, prompt_tokens, "
                "target_output_tokens, arrival_time_s, session_id, prefix_key) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (doc_id) DO NOTHING",
                (doc, src["tenant_id"], src["category"], src["text"], args.target_workload,
                 src["prompt_tokens"], args.output_cap, arrival, f"phase-client-{ci}", src["prefix_key"]))
            by_client[ci].append((doc, src, arrival)); doc += 1
        conn.commit()
    # write manifests
    for ci in (0, 1):
        rows = by_client[ci]
        reqs = tuple(ChatRequest(doc_id=d, prompt=s["text"], arrival_time_s=ar,
                                 prompt_tokens=s["prompt_tokens"], max_output_tokens=args.output_cap,
                                 estimated_output_tokens=args.output_cap,
                                 source_row_hash=source_row_hash(workload_name=args.target_workload,
                                     doc_id=d, prompt=s["text"], arrival_time_s=ar,
                                     prompt_tokens=s["prompt_tokens"], target_output_tokens=args.output_cap),
                                 endpoint_index=-1) for d, s, ar in rows)
        reqs = assign_endpoint_equal_rows(reqs, args.endpoint_count, args.seed)
        mp = out / f"client_{ci}.jsonl"
        with mp.open("w", encoding="utf-8") as f:
            for r in reqs:
                f.write(json.dumps({"doc_id": r.doc_id, "prompt": r.prompt, "arrival_time_s": r.arrival_time_s,
                                    "prompt_tokens": r.prompt_tokens, "target_output_tokens": args.output_cap,
                                    "endpoint_index": r.endpoint_index, "session_id": f"phase-client-{ci}"},
                                   ensure_ascii=False) + "\n")
        manifest_paths.append(str(mp))
    def stats(xs): return {"n": len(xs), "p50": round(statistics.median(xs), 1) if xs else None,
                           "p95": round(sorted(xs)[int(0.95 * len(xs))] if xs and len(xs) > 1 else (xs[0] if xs else 0), 1),
                           "max": max(xs) if xs else None}
    meta = {"note": DERIVED_NOTE, "target_workload": args.target_workload,
            "short_source": args.short_source, "long_source": args.long_source,
            "short_target_tokens": args.short_target, "long_target_tokens": args.long_target,
            "output_cap": args.output_cap, "rate_a": args.rate_a, "rate_b": args.rate_b,
            "duration_s": duration, "period_s": args.period_s, "arrival_time_scale": 1.0,
            "on_windows_b": on_windows, "phase_order": "A-only(0-60)->A+B(60-120)->A-only(120-180)->A+B(180-240)",
            "job_row_counts": [len(by_client[0]), len(by_client[1])],
            "job_first_arrival_s": [min(A) if A else 0.0, min(B) if B else 0.0],
            "doc_id_base": args.doc_id_base, "doc_id_range": [args.doc_id_base, doc - 1],
            "short_token_distance": stats(dist_a), "long_token_distance": stats(dist_b),
            "manifest_sha256": [hashlib.sha256(open(mp, "rb").read()).hexdigest() for mp in manifest_paths],
            "manifest_paths": manifest_paths, "seed": args.seed}
    (out / "meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(meta, indent=2, ensure_ascii=False))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--database-url", required=True)
    p.add_argument("--short-source", default="squad_v11_dev_short_answer")
    p.add_argument("--long-source", default="sharegpt_concentrated")
    p.add_argument("--target-workload", required=True)
    p.add_argument("--doc-id-base", type=int, required=True)
    p.add_argument("--rate-a", type=float, required=True, help="Job A continuous Poisson rate (req/s)")
    p.add_argument("--rate-b", type=float, required=True, help="Job B ON-phase Poisson rate (req/s)")
    p.add_argument("--short-target", type=int, default=256)
    p.add_argument("--long-target", type=int, default=1024)
    p.add_argument("--short-max-dist", type=int, default=64)
    p.add_argument("--long-max-dist", type=int, default=256)
    p.add_argument("--output-cap", type=int, default=512)
    p.add_argument("--duration-s", type=float, default=240.0)
    p.add_argument("--period-s", type=float, default=60.0)
    p.add_argument("--endpoint-count", type=int, default=2)
    p.add_argument("--seed", type=int, default=20260811)
    p.add_argument("--output-dir", required=True)
    a = p.parse_args()
    if a.duration_s < 240: raise ValueError("duration must be >= 240s (two full cycles)")
    build(a)


if __name__ == "__main__":
    main()
