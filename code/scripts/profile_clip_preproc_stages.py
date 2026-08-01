#!/usr/bin/env python
"""Break CLIPProcessor cpu_preprocess into sub-stages and time each.

Verifiable goal
---------------
The bottleneck profiler measures `processor(images=..., return_tensors="pt")`
as ONE ~5.2 ms/img stage (`cpu_preprocess`). This script wraps
CLIPImageProcessor's OWN resize / center_crop / rescale / normalize methods with
timers (same code path as real preproc -> faithful), answering WHICH sub-step
dominates the ~5.2 ms. Also times the whole processor() call and reports the
residual = total - sum(substeps) (PIL->numpy, tensor stacking, and any rescale
folded outside the wrapped methods).

Verified on transformers 5.14.1: CLIPImageProcessor fires resize / center_crop /
normalize during processor(); rescale is folded (0 separate calls) -> its cost
shows up inside normalize or the residual.

Single-process; reads COCO bytea from PG; decodes via shared `pil_decode`; only
preproc sub-steps are timed (no model forward, no H2D transfer). Reuses
`pil_decode` / `fetch_image_bytes` from profile_image_clip_bottleneck.py.

Usage
-----
    python profile_clip_preproc_stages.py \\
        --model /root/autodl-tmp/models/clip-vit-base-patch32 \\
        --pg-dsn "$DATABASE_URL" --limit 5000 --batch-sizes 1,32,128 --iters 100
"""

import argparse
import csv
import os
import sys
import time
from collections import defaultdict

SUBSTEPS = ["resize", "center_crop", "rescale", "normalize"]


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--model", default="/root/autodl-tmp/models/clip-vit-base-patch32")
    p.add_argument(
        "--pg-dsn",
        default=os.environ.get("DATABASE_URL") or os.environ.get("PG_DSN", ""),
    )
    p.add_argument("--table", default="image_documents")
    p.add_argument("--id-column", default="doc_id")
    p.add_argument("--image-column", default="image")
    p.add_argument("--limit", type=int, default=5000)
    p.add_argument("--batch-sizes", default="1,32,128")
    p.add_argument("--iters", type=int, default=100)
    p.add_argument("--warmup", type=int, default=5)
    p.add_argument("--out-csv", default="clip_preproc_stages.csv")
    return p.parse_args()


def wrap_substeps(ip):
    """Wrap resize/center_crop/rescale/normalize to accumulate per-call time.

    Returns (totals, orig, reset):
      totals: dict substep -> accumulated seconds during the last processor() call
      orig:   dict substep -> original method (to restore)
      reset:  callable zeroing `totals` before each measured processor() call
    """
    totals = {n: 0.0 for n in SUBSTEPS}
    orig = {}
    for name in SUBSTEPS:
        if not hasattr(ip, name):
            continue
        orig[name] = getattr(ip, name)

        def make(n, f):
            def w(*a, **k):
                t0 = time.perf_counter()
                r = f(*a, **k)
                totals[n] += time.perf_counter() - t0
                return r
            return w

        setattr(ip, name, make(name, orig[name]))

    def reset():
        for n in SUBSTEPS:
            totals[n] = 0.0

    return totals, orig, reset


def percentile(vals, q):
    if not vals:
        return float("nan")
    s = sorted(vals)
    k = (len(s) - 1) * q
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def main():
    args = parse_args()
    if not args.pg_dsn:
        print("ERROR: --pg-dsn required (or set DATABASE_URL/PG_DSN)", file=sys.stderr)
        sys.exit(2)

    from transformers import CLIPProcessor
    from profile_image_clip_bottleneck import pil_decode, fetch_image_bytes
    import psycopg2

    processor = CLIPProcessor.from_pretrained(args.model)
    ip = processor.image_processor
    print(
        f"image_processor: {type(ip).__name__} | "
        f"size={getattr(ip, 'size', None)} resample={getattr(ip, 'resample', None)} "
        f"crop_size={getattr(ip, 'crop_size', None)}"
    )

    conn = psycopg2.connect(args.pg_dsn)
    pool, _ = fetch_image_bytes(
        conn, args.table, args.id_column, args.image_column, args.limit
    )
    conn.close()
    byteas = [b for _, b in pool]
    n = len(byteas)
    if n == 0:
        print("ERROR: no image bytea fetched", file=sys.stderr)
        sys.exit(3)
    print(f"{n} images; batch-sizes={args.batch_sizes} iters={args.iters}")

    totals, orig, reset = wrap_substeps(ip)
    batch_sizes = [int(x) for x in args.batch_sizes.split(",") if x.strip()]

    results = []
    for B in batch_sizes:
        per_img = defaultdict(list)  # substep -> per-img seconds across iters
        total_pre = []  # whole-processor per-img seconds
        for i in range(args.warmup):
            chunk = [byteas[(i * B + j) % n] for j in range(B)]
            reset()
            processor(images=[pil_decode(b) for b in chunk], return_tensors="pt")
        for i in range(args.iters):
            chunk = [byteas[((args.warmup + i) * B + j) % n] for j in range(B)]
            imgs = [pil_decode(b) for b in chunk]  # decode is NOT part of preproc timing
            reset()
            t0 = time.perf_counter()
            processor(images=imgs, return_tensors="pt")
            total = time.perf_counter() - t0
            total_pre.append(total / B)
            for s in SUBSTEPS:
                per_img[s].append(totals[s] / B)
        row = {"batch_size": B, "n_measured": args.iters}
        sum_p50 = 0.0
        for s in SUBSTEPS:
            p50 = percentile(per_img[s], 0.50)
            row[f"{s}_p50_ms"] = p50 * 1000
            sum_p50 += p50
        tot_p50 = percentile(total_pre, 0.50)
        row["substeps_sum_p50_ms"] = sum_p50 * 1000
        row["total_preproc_p50_ms"] = tot_p50 * 1000
        row["residual_p50_ms"] = (tot_p50 - sum_p50) * 1000
        results.append(row)

    # restore original methods
    for name, f in orig.items():
        setattr(ip, name, f)

    # table
    print("\n=== CLIPProcessor preproc sub-stage cost (p50, ms/img) ===")
    hdr = (
        f"{'B':>4} {'resize':>8} {'crop':>8} {'rescale':>8} {'normalize':>10} "
        f"{'sum':>8} {'total':>8} {'residual':>9}"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in results:
        print(
            f"{r['batch_size']:>4} {r['resize_p50_ms']:>8.3f} "
            f"{r['center_crop_p50_ms']:>8.3f} {r['rescale_p50_ms']:>8.3f} "
            f"{r['normalize_p50_ms']:>10.3f} {r['substeps_sum_p50_ms']:>8.3f} "
            f"{r['total_preproc_p50_ms']:>8.3f} {r['residual_p50_ms']:>9.3f}"
        )

    mid = results[len(results) // 2]
    candidates = {s: mid[f"{s}_p50_ms"] for s in SUBSTEPS}
    candidates["residual"] = mid["residual_p50_ms"]
    dom = max(candidates, key=candidates.get)
    print(
        f"\ndominant sub-step (B={mid['batch_size']}): {dom} "
        f"= {candidates[dom]:.3f} ms/img "
        f"({candidates[dom] / mid['total_preproc_p50_ms'] * 100:.0f}% of total preproc)"
    )

    cols = [
        "batch_size", "n_measured", "resize_p50_ms", "center_crop_p50_ms",
        "rescale_p50_ms", "normalize_p50_ms", "substeps_sum_p50_ms",
        "total_preproc_p50_ms", "residual_p50_ms",
    ]
    with open(args.out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in results:
            w.writerow([f"{r[c]:.4f}" if isinstance(r[c], float) else r[c] for c in cols])
    print(f"\ncsv -> {args.out_csv}")


if __name__ == "__main__":
    main()
