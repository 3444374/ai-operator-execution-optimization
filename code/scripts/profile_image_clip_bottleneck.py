#!/usr/bin/env python
"""Per-image pipeline-stage profiler for image AI_EMBED (CLIP) + go/no-go gate.

Verifiable goal
---------------
For each batch size B, measure per-image milliseconds of every stage between
"image bytea in PostgreSQL" and "512-d embedding on GPU":

    pg_read        bulk-fetch bytea from PG (measured once, amortized /img)
    pil_decode     JPEG decode (Image.open + convert RGB)            [CPU]
    cpu_preprocess resize(224) + normalize via CLIPProcessor         [CPU]
    transfer       CPU tensor -> GPU (.to(cuda))                     [H2D]
    gpu_embed      CLIP forward, .pooler_output (transformers 5.x)   [GPU]

Then compute ratio = cpu_total_per_img / gpu_embed_per_img  where
cpu_total = pil_decode + cpu_preprocess (all CPU prep before the GPU).

Verdict (motivation go/no-go for the heterogeneous-scheduling direction):
    ratio > 0.3 at practical batches (>=16)  -> GO   (CPU data-prep is a real
                                                      stage; build path-B runner)
    ratio < 0.1                              -> NO-GO (CPU prep too light)
    otherwise                                -> BORDERLINE

Design
------
Single-process by design: isolate per-stage cost, no Ray/Daft/scheduler noise
(minimal experiment per karpathy-guidelines). DB is in the path (reads image
bytea from PostgreSQL, per project requirement). Reusable CLIP output/decode
semantics live in ``src.modalities.image``; this script owns only profiling orchestration.

Usage
-----
    python profile_image_clip_bottleneck.py \\
        --model /root/autodl-tmp/models/clip-vit-base-patch32 \\
        --pg-dsn "$PG_DSN" --out-csv bottleneck.csv
"""

import argparse
import csv
import os
import sys
import time
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.modalities.image.clip import (  # noqa: E402
    decode_rgb_image,
    extract_clip_image_features,
)


# --------------------------------------------------------------------------- #
# Config / args
# --------------------------------------------------------------------------- #
def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--model", default="/root/autodl-tmp/models/clip-vit-base-patch32")
    p.add_argument(
        "--pg-dsn",
        default=os.environ.get(
            "PG_DSN", "dbname=postgres user=postgres host=127.0.0.1 port=5432"
        ),
    )
    p.add_argument("--table", default="image_documents")
    p.add_argument("--id-column", default="doc_id")
    p.add_argument("--image-column", default="image")
    p.add_argument("--limit", type=int, default=1024, help="max images to bulk-fetch")
    p.add_argument("--batch-sizes", default="1,16,32,64,128")
    p.add_argument(
        "--iters",
        type=int,
        default=50,
        help="measured batches per batch size (after warmup)",
    )
    p.add_argument("--warmup", type=int, default=2, help="warmup batches discarded per size")
    p.add_argument("--device", default="cuda")
    p.add_argument("--out-csv", default="image_clip_bottleneck.csv")
    return p.parse_args()


# --------------------------------------------------------------------------- #
# Stage functions (profiling wrappers around reusable src.modalities.image semantics)
# --------------------------------------------------------------------------- #
def fetch_image_bytes(conn, table, id_col, img_col, limit):
    """PG -> list[(id, bytes)], bulk-fetched. Returns (rows, fetch_seconds)."""
    sql = (
        f"SELECT {id_col}, {img_col} FROM {table} "
        f"WHERE {img_col} IS NOT NULL ORDER BY {id_col} LIMIT %s"
    )
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    fetch_s = time.perf_counter() - t0
    # Normalize bytea values so profiling is independent of driver return type.
    out = [(r[0], bytes(r[1])) for r in rows]
    return out, fetch_s


def get_versions(conn):
    """Record actual server/pgvector version (per code/AGENTS.md rule)."""
    versions = {"server_version": "n/a", "pgvector_version": "n/a"}
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW server_version;")
            versions["server_version"] = str(cur.fetchone()[0])
            cur.execute("SELECT extversion FROM pg_extension WHERE extname='vector';")
            row = cur.fetchone()
            if row:
                versions["pgvector_version"] = str(row[0])
    except Exception as exc:  # noqa: BLE001 - version probe must not abort the run
        versions["error"] = str(exc)
    return versions


def load_clip(model_path, device):
    """Load CLIP model + processor onto device. Returns (model, processor)."""
    import torch  # local import: keep top-level import-light
    from transformers import CLIPModel, CLIPProcessor

    model = CLIPModel.from_pretrained(model_path).to(device).eval()
    processor = CLIPProcessor.from_pretrained(model_path)
    return model, processor


def pil_decode(raw):
    """JPEG bytes -> RGB PIL.Image (forces decode)."""
    return decode_rgb_image(raw)


def cpu_preprocess(images, processor):
    """list[PIL.Image] -> CLIPProcessor BatchFeature on CPU (resize+normalize)."""
    return processor(images=images, return_tensors="pt")


def clip_encode(model, inputs):
    """CLIP forward -> (B, 512) embedding.

    transformers 5.x get_image_features returns BaseModelOutputWithPooling; the
    512-d projection lives on .pooler_output (NOT .image_embeds, which is absent;
    last_hidden_state is the (B,50,768) patch tokens). See image_serving.md §3.3.
    """
    import torch

    with torch.inference_mode():
        out = model.get_image_features(**inputs)
    return extract_clip_image_features(out)


# --------------------------------------------------------------------------- #
# Timing + sweep
# --------------------------------------------------------------------------- #
def measure_batch(model, processor, batch_bytes, device):
    """Time the 4 per-batch stages for one batch. Returns dict stage->seconds.

    Stages: pil_decode (CPU) -> cpu_preprocess (CPU) -> transfer (H2D) -> gpu_embed.
    GPU stages are bounded by torch.cuda.synchronize() so the wall clock reflects
    actual device work, not async stream return.
    """
    import torch

    t0 = time.perf_counter()
    imgs = [pil_decode(b) for b in batch_bytes]          # pil_decode [CPU]
    t1 = time.perf_counter()
    inputs = cpu_preprocess(imgs, processor)             # resize + normalize [CPU]
    t2 = time.perf_counter()
    inputs = inputs.to(device)                           # H2D transfer
    if device.startswith("cuda"):
        torch.cuda.synchronize()                         # wait for transfer
    t3 = time.perf_counter()
    emb = clip_encode(model, inputs)                     # CLIP forward [GPU]
    if device.startswith("cuda"):
        torch.cuda.synchronize()                         # wait for forward
    t4 = time.perf_counter()
    stages = {
        "pil_decode": t1 - t0,
        "cpu_preprocess": t2 - t1,
        "transfer": t3 - t2,
        "gpu_embed": t4 - t3,
    }
    return stages, emb.shape


def percentile(vals, q):
    """Linear-interpolated percentile of a list."""
    if not vals:
        return float("nan")
    s = sorted(vals)
    k = (len(s) - 1) * q
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def run_sweep(model, processor, pool, batch_sizes, iters, warmup, device):
    """For each batch size: warmup, then measure `iters` batches cycling the pool.

    Returns list of result dicts (per-image seconds aggregated to mean/p50/p95).
    """
    n = len(pool)
    byteas = [b for _, b in pool]
    results = []
    for B in batch_sizes:
        per_img = {k: [] for k in ("pil_decode", "cpu_preprocess", "transfer", "gpu_embed")}
        # warmup (discard) - also primes CUDA kernels / JIT
        for i in range(warmup):
            chunk = [byteas[(i * B + j) % n] for j in range(B)]
            measure_batch(model, processor, chunk, device)
        # measured batches
        for i in range(iters):
            chunk = [byteas[((warmup + i) * B + j) % n] for j in range(B)]
            stages, _ = measure_batch(model, processor, chunk, device)
            for k in per_img:
                per_img[k].append(stages[k] / B)  # per-image seconds
        agg = {
            k: {
                "mean": sum(v) / len(v),
                "p50": percentile(v, 0.50),
                "p95": percentile(v, 0.95),
            }
            for k, v in per_img.items()
        }
        cpu_total_p50 = agg["pil_decode"]["p50"] + agg["cpu_preprocess"]["p50"]
        gpu_p50 = agg["gpu_embed"]["p50"]
        ratio = cpu_total_p50 / gpu_p50 if gpu_p50 > 0 else float("inf")
        results.append(
            {
                "batch_size": B,
                "n_measured": iters,
                **agg,
                "cpu_total_p50": cpu_total_p50,
                "ratio": ratio,
            }
        )
    return results


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def write_csv(path, results, pg_read_per_img_ms, versions):
    cols = [
        "batch_size",
        "n_measured",
        "pg_read_per_img_ms",
        "pil_decode_mean_ms",
        "pil_decode_p50_ms",
        "pil_decode_p95_ms",
        "cpu_preprocess_mean_ms",
        "cpu_preprocess_p50_ms",
        "cpu_preprocess_p95_ms",
        "transfer_p50_ms",
        "gpu_embed_mean_ms",
        "gpu_embed_p50_ms",
        "gpu_embed_p95_ms",
        "cpu_total_p50_ms",
        "ratio_cpu_embed",
        "server_version",
        "pgvector_version",
    ]
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in results:
            w.writerow(
                [
                    r["batch_size"],
                    r["n_measured"],
                    f"{pg_read_per_img_ms:.4f}",
                    f"{r['pil_decode']['mean'] * 1000:.4f}",
                    f"{r['pil_decode']['p50'] * 1000:.4f}",
                    f"{r['pil_decode']['p95'] * 1000:.4f}",
                    f"{r['cpu_preprocess']['mean'] * 1000:.4f}",
                    f"{r['cpu_preprocess']['p50'] * 1000:.4f}",
                    f"{r['cpu_preprocess']['p95'] * 1000:.4f}",
                    f"{r['transfer']['p50'] * 1000:.4f}",
                    f"{r['gpu_embed']['mean'] * 1000:.4f}",
                    f"{r['gpu_embed']['p50'] * 1000:.4f}",
                    f"{r['gpu_embed']['p95'] * 1000:.4f}",
                    f"{r['cpu_total_p50'] * 1000:.4f}",
                    f"{r['ratio']:.3f}",
                    versions.get("server_version", "n/a"),
                    versions.get("pgvector_version", "n/a"),
                ]
            )


def print_table(results, pg_read_per_img_ms):
    hdr = (
        f"{'B':>4} {'pg_read':>8} {'decode':>8} {'preproc':>8} "
        f"{'xfer':>7} {'embed':>8} {'cpu_tot':>8} {'ratio':>7}"
    )
    print("\n=== per-image stage cost (p50, ms) ===")
    print(hdr)
    print("-" * len(hdr))
    for r in results:
        print(
            f"{r['batch_size']:>4} "
            f"{pg_read_per_img_ms:>8.3f} "
            f"{r['pil_decode']['p50'] * 1000:>8.3f} "
            f"{r['cpu_preprocess']['p50'] * 1000:>8.3f} "
            f"{r['transfer']['p50'] * 1000:>7.3f} "
            f"{r['gpu_embed']['p50'] * 1000:>8.3f} "
            f"{r['cpu_total_p50'] * 1000:>8.3f} "
            f"{r['ratio']:>7.3f}"
        )


def print_verdict(results):
    practical = [r for r in results if r["batch_size"] >= 16] or results
    ratios = [r["ratio"] for r in practical]
    r_min, r_max = min(ratios), max(ratios)
    print("\n=== GO/NO-GO (ratio = cpu_total_per_img / gpu_embed_per_img, practical B>=16) ===")
    print(f"ratio range: min={r_min:.2f}  max={r_max:.2f}")
    if r_min > 0.3:
        print("VERDICT: GO   -- ratio > 0.3 at all practical batches; CPU data-prep is a")
        print("               real stage, heterogeneous scheduling has something to overlap.")
    elif r_max < 0.1:
        print(
            "VERDICT: NO-GO -- ratio < 0.1; CPU prep too light vs GPU "
            "embed, no stage to overlap."
        )
    else:
        print("VERDICT: BORDERLINE -- some batches >0.3, some below; inspect the curve vs B.")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    args = parse_args()

    import psycopg  # local import: fail late with a clear message if missing

    try:
        conn = psycopg.connect(args.pg_dsn)
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: cannot connect to PG ({args.pg_dsn}): {exc}", file=sys.stderr)
        sys.exit(2)

    versions = get_versions(conn)
    pool, fetch_s = fetch_image_bytes(
        conn, args.table, args.id_column, args.image_column, args.limit
    )
    conn.close()

    if not pool:
        print(
            f"ERROR: no image bytea fetched from {args.table}.{args.image_column}; "
            "check --table/--image-column/--pg-dsn/--limit.",
            file=sys.stderr,
        )
        sys.exit(3)

    pg_read_per_img_ms = (fetch_s / len(pool)) * 1000
    print(f"fetched {len(pool)} images in {fetch_s:.2f}s "
          f"-> pg_read {pg_read_per_img_ms:.3f} ms/img (bulk, amortized)")
    print(f"versions: {versions}")
    print(f"model: {args.model}  device: {args.device}")

    model, processor = load_clip(args.model, args.device)
    batch_sizes = [int(x) for x in args.batch_sizes.split(",") if x.strip()]
    results = run_sweep(
        model, processor, pool, batch_sizes, args.iters, args.warmup, args.device
    )

    write_csv(args.out_csv, results, pg_read_per_img_ms, versions)
    print_table(results, pg_read_per_img_ms)
    print_verdict(results)
    print(f"\ncsv -> {args.out_csv}")


if __name__ == "__main__":
    main()
