#!/usr/bin/env python3
"""Wrap an existing text workload into a bounded-output short-completion workload.

Reads ``text`` rows from a source workload, wraps each as
``Summarize the following text in at most {N} words: {text}``, and inserts the
wrapped prompts as a NEW ``workload_name`` with ``target_output_tokens``
reflecting the bounded task. Used to build a manifest where every comparator
(including DuckDB-ai, which treats ``finish_reason=length`` as a row-level
error) completes with zero row-level errors under a small fixed output cap.

Idempotent: deletes the target workload first. New ``doc_id`` values are
assigned from a high base to avoid PK collision with source workloads, since
``documents.doc_id`` is a single-column primary key shared across workloads.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import psycopg

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

# Rough token overhead of the wrap instruction
# "Summarize the following text in at most 10 words: ".
INSTRUCTION_TOKENS = 12


def wrap_prompt(text: str, max_words: int) -> str:
    return (
        f"Summarize the following text in at most {max_words} words: {text}"
    )


DEFAULT_TEMPLATE = (
    "Summarize the following text in at most {max_words} words: {text}"
)


def _apply_template(template: str, text: str, max_words: int) -> str:
    """Render a wrap template; supports {text} and {max_words} placeholders."""

    return template.replace("{max_words}", str(max_words)).replace("{text}", text)


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--source-workload", required=True)
    parser.add_argument("--target-workload", required=True)
    parser.add_argument("--row-count", type=int, required=True)
    parser.add_argument("--max-words", type=int, default=10)
    parser.add_argument("--target-output-tokens", type=int, default=15)
    parser.add_argument(
        "--doc-id-base",
        type=int,
        default=50_000_000,
        help="new doc_ids are base+i to avoid PK collision with source",
    )
    parser.add_argument(
        "--template",
        default=DEFAULT_TEMPLATE,
        help=(
            "wrap instruction; supports {text} and {max_words} placeholders. "
            "Default is the loose 'at most N words' summary, which models often "
            "violate with bullet lists. For zero-error bounded output prefer a "
            "stricter template or a 1-token task."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse(sys.argv[1:] if argv is None else argv)
    with psycopg.connect(args.database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM documents WHERE workload_name = %s",
                (args.target_workload,),
            )
            cursor.execute(
                "SELECT doc_id, tenant_id, category, text, prompt_tokens, "
                "arrival_time_s, session_id, prefix_key "
                "FROM documents WHERE workload_name = %s "
                "ORDER BY doc_id LIMIT %s",
                (args.source_workload, args.row_count),
            )
            rows = cursor.fetchall()
            if not rows:
                raise SystemExit(
                    f"source workload {args.source_workload!r} has no rows"
                )
            values = []
            for index, row in enumerate(rows):
                _src_doc_id, tenant_id, category, text, prompt_tokens, arrival, session, prefix_key = row
                wrapped = _apply_template(args.template, text, args.max_words)
                est_prompt = (prompt_tokens or max(1, len(text) // 4)) + INSTRUCTION_TOKENS
                values.append(
                    (
                        args.doc_id_base + index,
                        tenant_id,
                        category,
                        wrapped,
                        args.target_workload,
                        est_prompt,
                        args.target_output_tokens,
                        arrival,
                        session,
                        prefix_key,
                    )
                )
            cursor.executemany(
                "INSERT INTO documents (doc_id, tenant_id, category, text, "
                "workload_name, prompt_tokens, target_output_tokens, "
                "arrival_time_s, session_id, prefix_key) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                values,
            )
        connection.commit()
    print(
        f"inserted {len(values)} rows into workload "
        f"{args.target_workload!r} (source {args.source_workload!r}, "
        f"max_words={args.max_words}, target_output_tokens="
        f"{args.target_output_tokens}, doc_id_base={args.doc_id_base})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
