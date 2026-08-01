#!/usr/bin/env python
"""Import images from a directory into PostgreSQL `image_documents` as bytea.

Verifiable goal
---------------
Read every `--pattern` file (default *.jpg) under `--dir`, sorted by filename
for deterministic doc_id, and INSERT as bytea into
`image_documents(doc_id, workload_name, image, image_bytes)`, REPLACING the
table (TRUNCATE first). doc_id = sorted filename index (0..N-1). embedding is
left NULL (the path-B runner fills it); workload_name defaults via the table.

TRUNCATE + all INSERTs run in ONE transaction (`with conn:`), so a failure rolls
back and the table is preserved (no half-load). Records server/pgvector version
per code/AGENTS.md rule.

Usage
-----
    python import_coco_images.py \\
        --dir /root/autodl-tmp/data/raw/coco_val2017/val2017 \\
        --pg-dsn "$DATABASE_URL" --workload coco_val2017
"""

import argparse
import os
import sys
import time
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--dir", required=True, help="directory containing images")
    p.add_argument(
        "--pg-dsn",
        default=os.environ.get("DATABASE_URL") or os.environ.get("PG_DSN", ""),
    )
    p.add_argument("--table", default="image_documents")
    p.add_argument("--workload", default="coco_val2017")
    p.add_argument("--pattern", default="*.jpg")
    p.add_argument("--batch", type=int, default=200, help="rows per INSERT")
    p.add_argument("--limit", type=int, default=0, help="0 = all")
    return p.parse_args()


def list_images(directory, pattern, limit):
    """Sorted list of image paths -> deterministic doc_id assignment."""
    paths = sorted(Path(directory).glob(pattern))
    return paths[:limit] if limit > 0 else paths


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
    except Exception as exc:  # noqa: BLE001
        versions["error"] = str(exc)
    return versions


def main():
    args = parse_args()
    if not args.pg_dsn:
        print("ERROR: --pg-dsn required (or set DATABASE_URL/PG_DSN)", file=sys.stderr)
        sys.exit(2)

    import psycopg2
    from psycopg2.extras import execute_values

    paths = list_images(args.dir, args.pattern, args.limit)
    if not paths:
        print(f"ERROR: no {args.pattern} under {args.dir}", file=sys.stderr)
        sys.exit(3)
    print(f"loading {len(paths)} images from {args.dir} -> {args.table} "
          f"(workload={args.workload}, batch={args.batch})")

    conn = psycopg2.connect(args.pg_dsn)
    versions = get_versions(conn)
    print(f"versions: {versions}")

    insert_sql = (
        f"INSERT INTO {args.table} (doc_id, workload_name, image, image_bytes) VALUES %s"
    )
    t0 = time.perf_counter()
    total_bytes = 0
    try:
        # Single transaction: TRUNCATE + all INSERTs (atomic; rollback on error).
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {args.table};")
                buf = []
                for i, p in enumerate(paths):
                    b = p.read_bytes()
                    buf.append((i, args.workload, b, len(b)))
                    total_bytes += len(b)
                    if len(buf) >= args.batch:
                        execute_values(cur, insert_sql, buf)
                        buf.clear()
                    if (i + 1) % 1000 == 0:
                        print(f"  {i + 1}/{len(paths)} ({(i + 1) / len(paths) * 100:.0f}%)...")
                if buf:
                    execute_values(cur, insert_sql, buf)
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR during load (transaction rolled back, table preserved): {exc}",
              file=sys.stderr)
        conn.close()
        sys.exit(4)

    # Verify (read after commit).
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*), count(image), min(doc_id), max(doc_id) FROM {args.table};"
        )
        n, ni, mn, mx = cur.fetchone()
    conn.close()
    elapsed = time.perf_counter() - t0
    print(f"loaded {n} rows (image non-null {ni}, doc_id {mn}..{mx}) in {elapsed:.1f}s, "
          f"{total_bytes / 1e6:.0f} MB -> PG {versions.get('server_version')}")


if __name__ == "__main__":
    main()
