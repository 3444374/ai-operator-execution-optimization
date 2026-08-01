#!/usr/bin/env python
"""Import directory or ZIP images into PostgreSQL `image_documents` as bytea.

Verifiable goal
---------------
Read every `--pattern` file (default *.jpg) under exactly one of `--dir` or
`--zip`, preserve the numeric COCO source ID from the filename, and INSERT as bytea into
`image_documents(doc_id, workload_name, image, image_bytes)`. Only rows for the
selected workload are replaced; unrelated workloads remain intact.

DELETE + all INSERTs run in ONE transaction (`with conn:`), so a failure rolls
back and the prior workload remains intact (no half-load). Records
server/pgvector version per code/AGENTS.md rule.

Usage
-----
    python import_coco_images.py \\
        --dir /root/autodl-tmp/data/raw/coco_val2017/val2017 \\
        --pg-dsn "$DATABASE_URL" --workload coco_val2017

    python import_coco_images.py \\
        --zip /root/autodl-tmp/data/raw/coco_train2017/train2017.zip \\
        --limit 60000 --pg-dsn "$DATABASE_URL" --workload coco_train2017_60k
"""

import argparse
import os
import sys
import time
from pathlib import Path, PurePath, PurePosixPath
from zipfile import ZipFile


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument("--dir", help="directory containing images")
    source.add_argument(
        "--zip", help="ZIP archive containing images; read without extraction"
    )
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
    """Return a deterministic path list; IDs come from COCO filenames."""
    paths = sorted(Path(directory).glob(pattern))
    return paths[:limit] if limit > 0 else paths


def list_zip_images(archive: ZipFile, pattern: str, limit: int) -> list[PurePosixPath]:
    """Return deterministic matching ZIP members without extracting the archive."""
    paths = sorted(
        PurePosixPath(info.filename)
        for info in archive.infolist()
        if not info.is_dir() and PurePosixPath(info.filename).match(pattern)
    )
    return paths[:limit] if limit > 0 else paths


def coco_doc_id(path: PurePath) -> int:
    """Preserve the stable numeric COCO image ID encoded in the filename."""
    try:
        return int(path.stem)
    except ValueError as exc:
        raise ValueError(f"COCO image filename must have a numeric stem: {path}") from exc


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


def primary_key_columns(conn, table: str) -> tuple[str, ...]:
    """Return ordered primary-key columns for the target table."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT attribute.attname "
            "FROM pg_index AS index_definition "
            "CROSS JOIN LATERAL unnest(index_definition.indkey) "
            "WITH ORDINALITY AS key_column(attnum, ordinality) "
            "JOIN pg_attribute AS attribute "
            "ON attribute.attrelid = index_definition.indrelid "
            "AND attribute.attnum = key_column.attnum "
            "WHERE index_definition.indrelid = %s::regclass "
            "AND index_definition.indisprimary "
            "ORDER BY key_column.ordinality",
            (table,),
        )
        return tuple(str(row[0]) for row in cur.fetchall())


def main():
    args = parse_args()
    if not args.pg_dsn:
        print("ERROR: --pg-dsn required (or set DATABASE_URL/PG_DSN)", file=sys.stderr)
        sys.exit(2)

    import psycopg
    from psycopg import sql

    archive = ZipFile(args.zip) if args.zip else None
    source_label = args.zip or args.dir
    paths = (
        list_zip_images(archive, args.pattern, args.limit)
        if archive is not None
        else list_images(args.dir, args.pattern, args.limit)
    )
    if not paths:
        if archive is not None:
            archive.close()
        print(f"ERROR: no {args.pattern} under {source_label}", file=sys.stderr)
        sys.exit(3)
    print(f"loading {len(paths)} images from {source_label} -> {args.table} "
          f"(workload={args.workload}, batch={args.batch})")

    conn = psycopg.connect(args.pg_dsn)
    versions = get_versions(conn)
    print(f"versions: {versions}")
    identity_columns = primary_key_columns(conn, args.table)
    if identity_columns != ("workload_name", "doc_id"):
        if archive is not None:
            archive.close()
        conn.close()
        print(
            "ERROR: image_documents identity must be PRIMARY KEY "
            "(workload_name, doc_id); run "
            "deploy/autodl/image_documents_workload_key.sql first "
            f"(found {identity_columns})",
            file=sys.stderr,
        )
        sys.exit(5)

    table_identifier = sql.Identifier(args.table)
    insert_sql = sql.SQL(
        "INSERT INTO {} (doc_id, workload_name, image, image_bytes) "
        "VALUES (%s, %s, %s, %s)"
    ).format(table_identifier)
    t0 = time.perf_counter()
    total_bytes = 0
    try:
        # Single transaction: replace only this workload (atomic on failure).
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DELETE FROM {} WHERE workload_name = %s").format(
                        table_identifier
                    ),
                    (args.workload,),
                )
                buf = []
                for i, p in enumerate(paths):
                    b = archive.read(str(p)) if archive is not None else p.read_bytes()
                    buf.append((coco_doc_id(p), args.workload, b, len(b)))
                    total_bytes += len(b)
                    if len(buf) >= args.batch:
                        cur.executemany(insert_sql, buf)
                        buf.clear()
                    if (i + 1) % 1000 == 0:
                        print(f"  {i + 1}/{len(paths)} ({(i + 1) / len(paths) * 100:.0f}%)...")
                if buf:
                    cur.executemany(insert_sql, buf)
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR during load (transaction rolled back, table preserved): {exc}",
              file=sys.stderr)
        if archive is not None:
            archive.close()
        conn.close()
        sys.exit(4)

    if archive is not None:
        archive.close()

    # Verify (read after commit).
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT count(*), count(image), min(doc_id), max(doc_id) "
                "FROM {} WHERE workload_name = %s"
            ).format(table_identifier),
            (args.workload,),
        )
        n, ni, mn, mx = cur.fetchone()
    conn.close()
    elapsed = time.perf_counter() - t0
    print(f"loaded {n} rows (image non-null {ni}, doc_id {mn}..{mx}) in {elapsed:.1f}s, "
          f"{total_bytes / 1e6:.0f} MB -> PG {versions.get('server_version')}")


if __name__ == "__main__":
    main()
