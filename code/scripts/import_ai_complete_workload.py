#!/usr/bin/env python3
"""Import ShareGPT prompts with BurstGPT trace metadata into PostgreSQL."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

import psycopg

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))


DOCUMENTS_SQL = """
CREATE TABLE IF NOT EXISTS documents (
  doc_id BIGINT PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  category TEXT NOT NULL,
  text TEXT NOT NULL,
  workload_name TEXT NOT NULL DEFAULT 'synthetic',
  prompt_tokens INTEGER,
  target_output_tokens INTEGER,
  arrival_time_s DOUBLE PRECISION,
  session_id TEXT,
  prefix_key TEXT,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

DOCUMENTS_ALTER_SQL = [
    "ALTER TABLE documents ADD COLUMN IF NOT EXISTS workload_name TEXT NOT NULL DEFAULT 'synthetic'",
    "ALTER TABLE documents ADD COLUMN IF NOT EXISTS prompt_tokens INTEGER",
    "ALTER TABLE documents ADD COLUMN IF NOT EXISTS target_output_tokens INTEGER",
    "ALTER TABLE documents ADD COLUMN IF NOT EXISTS arrival_time_s DOUBLE PRECISION",
    "ALTER TABLE documents ADD COLUMN IF NOT EXISTS session_id TEXT",
    "ALTER TABLE documents ADD COLUMN IF NOT EXISTS prefix_key TEXT",
]


@dataclass(frozen=True)
class BurstTraceRow:
    timestamp_s: float
    model: str
    request_tokens: int
    response_tokens: int
    total_tokens: int
    log_type: str


@dataclass(frozen=True)
class WorkloadRow:
    doc_id: int
    tenant_id: int
    category: str
    text: str
    workload_name: str
    prompt_tokens: int
    target_output_tokens: int
    arrival_time_s: float
    session_id: str
    prefix_key: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a ShareGPT+BurstGPT AI_COMPLETE workload.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument(
        "--sharegpt-json",
        default="data/raw/sharegpt_vicuna/ShareGPT_V3_unfiltered_cleaned_split.json",
    )
    parser.add_argument("--burstgpt-csv", default="data/raw/burstgpt/BurstGPT_1.csv")
    parser.add_argument("--workload-name", default="sharegpt_burstgpt")
    parser.add_argument("--max-rows", type=int, default=1024)
    parser.add_argument("--start-doc-id", type=int, default=0)
    parser.add_argument("--batch-rows", type=int, default=1000)
    parser.add_argument("--max-prompt-chars", type=int, default=6000)
    parser.add_argument("--max-request-tokens", type=int, default=1800)
    parser.add_argument(
        "--tokenizer-path",
        help="Optional local tokenizer path used to store and filter model-specific prompt token counts.",
    )
    parser.add_argument(
        "--max-model-len",
        type=int,
        help="Drop prompts whose tokenizer count plus completion tokens exceeds this limit.",
    )
    parser.add_argument("--completion-max-tokens", type=int, default=16)
    parser.add_argument("--reset-documents", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def first_human_prompt(record: dict) -> str | None:
    conversations = record.get("conversations")
    if not isinstance(conversations, list):
        return None
    for turn in conversations:
        if not isinstance(turn, dict):
            continue
        if turn.get("from") != "human":
            continue
        value = turn.get("value")
        if isinstance(value, str) and value.strip():
            return normalize_text(value)
    return None


def normalize_text(text: str) -> str:
    return " ".join(text.replace("\x00", " ").split())


def prefix_key(text: str) -> str:
    prefix = normalize_text(text).lower()[:160]
    return hashlib.sha1(prefix.encode("utf-8")).hexdigest()[:16]


def length_bucket(tokens: int) -> str:
    if tokens < 512:
        return "short"
    if tokens < 1536:
        return "medium"
    return "long"


def category_for(trace: BurstTraceRow, prompt_tokens: int | None = None) -> str:
    model = trace.model.lower().replace(" ", "_").replace("-", "_")
    return f"{length_bucket(prompt_tokens or trace.request_tokens)}_{model}"


def load_prompt_token_counter(tokenizer_path: str) -> Callable[[str], int]:
    from transformers import AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, local_files_only=True)

    def count_tokens(text: str) -> int:
        return len(tokenizer.encode(text, add_special_tokens=False))

    return count_tokens


def load_sharegpt_prompts(path: Path, max_prompt_chars: int) -> list[tuple[str, str]]:
    records = json.load(path.open(encoding="utf-8"))
    prompts: list[tuple[str, str]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        prompt = first_human_prompt(record)
        if not prompt or len(prompt) > max_prompt_chars:
            continue
        session_id = str(record.get("id") or f"sharegpt-{index}")
        prompts.append((session_id, prompt))
    return prompts


def burstgpt_rows_from_dicts(rows: Iterable[dict[str, str]], max_request_tokens: int) -> list[BurstTraceRow]:
    traces: list[BurstTraceRow] = []
    for row in rows:
        request_tokens = int(row["Request tokens"])
        response_tokens = int(row["Response tokens"])
        if request_tokens <= 0 or response_tokens <= 0 or request_tokens > max_request_tokens:
            continue
        traces.append(
            BurstTraceRow(
                timestamp_s=float(row["Timestamp"]),
                model=row["Model"],
                request_tokens=request_tokens,
                response_tokens=response_tokens,
                total_tokens=int(row["Total tokens"]),
                log_type=row["Log Type"],
            )
        )
    return traces


def load_burstgpt_trace(path: Path, max_request_tokens: int) -> list[BurstTraceRow]:
    rows: list[BurstTraceRow] = []
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = burstgpt_rows_from_dicts(reader, max_request_tokens)
    return rows


def build_workload_rows(
    prompts: list[tuple[str, str]],
    traces: list[BurstTraceRow],
    workload_name: str,
    start_doc_id: int,
    max_rows: int,
    prompt_token_counter: Callable[[str], int] | None = None,
    max_model_len: int | None = None,
    completion_max_tokens: int = 0,
) -> list[WorkloadRow]:
    if max_rows <= 0:
        raise ValueError("--max-rows must be positive")
    if not prompts:
        raise ValueError("ShareGPT prompt list is empty after filtering")
    if not traces:
        raise ValueError("BurstGPT trace list is empty after filtering")

    rows: list[WorkloadRow] = []
    for prompt_record, trace in zip(prompts, traces):
        if len(rows) >= max_rows:
            break
        session_id, prompt = prompt_record
        prompt_tokens = trace.request_tokens
        if prompt_token_counter is not None:
            prompt_tokens = prompt_token_counter(prompt)
        if prompt_tokens <= 0:
            continue
        if max_model_len is not None and prompt_tokens + completion_max_tokens > max_model_len:
            continue
        doc_id = start_doc_id + len(rows)
        rows.append(
            WorkloadRow(
                doc_id=doc_id,
                tenant_id=stable_tenant_id(session_id),
                category=category_for(trace, prompt_tokens),
                text=prompt,
                workload_name=workload_name,
                prompt_tokens=prompt_tokens,
                target_output_tokens=trace.response_tokens,
                arrival_time_s=trace.timestamp_s,
                session_id=session_id,
                prefix_key=prefix_key(prompt),
            )
        )
    if not rows:
        raise ValueError("No workload rows remain after tokenizer/model-length filtering")
    return rows


def stable_tenant_id(session_id: str) -> int:
    digest = hashlib.sha1(session_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:2], "big") % 16


def setup_documents(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DOCUMENTS_SQL)
        for sql in DOCUMENTS_ALTER_SQL:
            cur.execute(sql)
    conn.commit()


def reset_documents(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE documents CASCADE")
    conn.commit()


def insert_rows(conn, rows: list[WorkloadRow], batch_rows: int) -> None:
    if batch_rows <= 0:
        raise ValueError("--batch-rows must be positive")
    sql = """
        INSERT INTO documents (
            doc_id, tenant_id, category, text, workload_name, prompt_tokens,
            target_output_tokens, arrival_time_s, session_id, prefix_key
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (doc_id) DO UPDATE SET
            tenant_id = EXCLUDED.tenant_id,
            category = EXCLUDED.category,
            text = EXCLUDED.text,
            workload_name = EXCLUDED.workload_name,
            prompt_tokens = EXCLUDED.prompt_tokens,
            target_output_tokens = EXCLUDED.target_output_tokens,
            arrival_time_s = EXCLUDED.arrival_time_s,
            session_id = EXCLUDED.session_id,
            prefix_key = EXCLUDED.prefix_key,
            updated_at = CURRENT_TIMESTAMP
    """
    values = [
        (
            row.doc_id,
            row.tenant_id,
            row.category,
            row.text,
            row.workload_name,
            row.prompt_tokens,
            row.target_output_tokens,
            row.arrival_time_s,
            row.session_id,
            row.prefix_key,
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        for offset in range(0, len(values), batch_rows):
            cur.executemany(sql, values[offset : offset + batch_rows])
    conn.commit()


def summarize(rows: list[WorkloadRow]) -> dict[str, object]:
    prompt_tokens = [row.prompt_tokens for row in rows]
    output_tokens = [row.target_output_tokens for row in rows]
    categories = sorted({row.category for row in rows})
    return {
        "rows": len(rows),
        "categories": categories,
        "prompt_tokens_min": min(prompt_tokens) if prompt_tokens else 0,
        "prompt_tokens_max": max(prompt_tokens) if prompt_tokens else 0,
        "output_tokens_min": min(output_tokens) if output_tokens else 0,
        "output_tokens_max": max(output_tokens) if output_tokens else 0,
        "arrival_time_min_s": min((row.arrival_time_s for row in rows), default=0.0),
        "arrival_time_max_s": max((row.arrival_time_s for row in rows), default=0.0),
    }


def main() -> None:
    args = parse_args()
    if args.max_model_len is not None and args.tokenizer_path is None:
        raise ValueError("--max-model-len requires --tokenizer-path")
    if args.completion_max_tokens < 0:
        raise ValueError("--completion-max-tokens must be non-negative")
    token_counter = load_prompt_token_counter(args.tokenizer_path) if args.tokenizer_path else None
    prompts = load_sharegpt_prompts(Path(args.sharegpt_json), args.max_prompt_chars)
    traces = load_burstgpt_trace(Path(args.burstgpt_csv), args.max_request_tokens)
    rows = build_workload_rows(
        prompts,
        traces,
        args.workload_name,
        args.start_doc_id,
        args.max_rows,
        prompt_token_counter=token_counter,
        max_model_len=args.max_model_len,
        completion_max_tokens=args.completion_max_tokens,
    )
    summary = summarize(rows)
    if args.dry_run:
        print(json.dumps({"status": "dry_run", **summary}, ensure_ascii=False, indent=2))
        return

    with psycopg.connect(args.database_url) as conn:
        setup_documents(conn)
        if args.reset_documents:
            reset_documents(conn)
        insert_rows(conn, rows, args.batch_rows)
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
