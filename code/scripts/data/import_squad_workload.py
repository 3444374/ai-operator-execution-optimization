#!/usr/bin/env python3
"""Dedicated SQuAD v1.1 dev importer for the bounded-output AI_COMPLETE track.

Imports the SQuAD v1.1 validation/dev split (10,570 questions) into the
``documents`` table as a new workload for the bounded-output DuckDB-vs-project
comparison. This importer ONLY imports; quality evaluation (EM/F1) lives in a
separate evaluator.

Locked contract (do not change without bumping the workload name + provenance):
  * dataset: SQuAD v1.1, split = validation/dev, official ``dev-v1.1.json``
    (10,570 questions). NOT train.
  * reference_answers stored as JSONB (the full multi-answer text array); EM/F1
    takes the max over references. The SQuAD original qa id is preserved in
    ``source_example_id`` (doc_id is a separate synthetic PK).
  * prompt template is the exact ``PROMPT_TEMPLATE`` below (content + newlines
    are part of the recorded hash).
  * cap (max_output_tokens) is fixed at 64 and set at manifest export time, not
    here; this importer records ``target_output_tokens`` as a rough estimate only.

Provenance (version, split, source SHA256, URL, sample count, importer commit,
content hash) is written to a sidecar JSON next to the run. The import is
idempotent (DELETE workload first) and transactional (rollback on any error).
Schema migration (``reference_answers``, ``source_example_id`` columns) is
additive ALTER ... IF NOT EXISTS.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib import request

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

SQUAD_DEV_URL = "https://rajpurkar.github.io/SQuAD-explorer/dataset/dev-v1.1.json"
SQUAD_VERSION = "1.1"
SQUAD_SPLIT = "validation/dev"
EXPECTED_DEV_COUNT = 10570
# Canonical dev-v1.1.json SHA256 (verified from the official rajpurkar/SQuAD-explorer
# GitHub repo). The importer fail-closes if the input file does not match, so a
# tampered file with the right row count is still rejected.
EXPECTED_DEV_SHA256 = "95aa6a52d5d6a735563366753ca50492a658031da74f301ac5238b03966972c9"
SQUAD_SOURCE_REPO = "rajpurkar/SQuAD-explorer"
SQUAD_SOURCE_URL = "https://github.com/rajpurkar/SQuAD-explorer/blob/master/dataset/dev-v1.1.json"
SQUAD_SOURCE_REVISION = "master"
SQUAD_SOURCE_DOWNLOAD_METHOD = (
    "git sparse clone (--depth 1 --filter=blob:none --sparse) of "
    "rajpurkar/SQuAD-explorer, then sparse-checkout dataset/dev-v1.1.json "
    "(turbo accelerates github.com; rajpurkar.github.io is ~5KB/s and infeasible "
    "from AutoDL)"
)

PROMPT_TEMPLATE = (
    "Answer the question using only the context.\n"
    "Return only the shortest answer span. Do not explain.\n"
    "\n"
    "Context:\n"
    "{context}\n"
    "\n"
    "Question:\n"
    "{question}\n"
    "\n"
    "Answer:\n"
)

SCHEMA_ALTER_SQL = [
    "ALTER TABLE documents ADD COLUMN IF NOT EXISTS reference_answers JSONB",
    "ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_example_id TEXT",
]


@dataclass(frozen=True)
class SquadRow:
    source_example_id: str
    context: str
    question: str
    reference_answers: tuple[str, ...]
    prompt: str


def build_prompt(context: str, question: str) -> str:
    return PROMPT_TEMPLATE.format(context=context, question=question)


def parse_squad_dev(parsed: dict) -> list[SquadRow]:
    """Parse the dev-v1.1.json dict into one SquadRow per question.

    Preserves the full multi-answer text array (SQuAD dev has up to 3 references)
    and unicode/special characters verbatim. Duplicate qa ids within the file are
    rejected (each row must map to one SQuAD example).
    """

    rows: list[SquadRow] = []
    seen_ids: set[str] = set()
    for article in parsed.get("data", []):
        for paragraph in article.get("paragraphs", []):
            context = paragraph.get("context", "")
            for qa in paragraph.get("qas", []):
                qa_id = qa.get("id")
                if not qa_id:
                    raise ValueError("SQuAD qa missing id")
                if qa_id in seen_ids:
                    raise ValueError(f"duplicate SQuAD qa id: {qa_id!r}")
                seen_ids.add(qa_id)
                answers = qa.get("answers", [])
                if isinstance(answers, dict):
                    # HF/datasets collapsed format {answer_start:[...], text:[...]}
                    texts = tuple(answers.get("text", []))
                elif isinstance(answers, list):
                    # raw dev-v1.1.json format: [{answer_start, text}, ...]
                    texts = tuple(a.get("text", "") for a in answers if isinstance(a, dict))
                else:
                    texts = ()
                if not texts:
                    raise ValueError(f"SQuAD qa {qa_id!r} has no reference answers")
                rows.append(
                    SquadRow(
                        source_example_id=qa_id,
                        context=context,
                        question=qa.get("question", ""),
                        reference_answers=texts,
                        prompt=build_prompt(context, qa.get("question", "")),
                    )
                )
    return rows


def compute_content_hash(rows: list[SquadRow]) -> str:
    """Stable hash over the imported content (id + prompt + references)."""

    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda r: r.source_example_id):
        payload = json.dumps(
            {
                "id": row.source_example_id,
                "prompt": row.prompt,
                "references": list(row.reference_answers),
            },
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
        digest.update(payload)
    return digest.hexdigest()


def prompt_template_hash() -> str:
    return hashlib.sha256(PROMPT_TEMPLATE.encode("utf-8")).hexdigest()


def _validate_dev_count(row_count: int, expected: int = EXPECTED_DEV_COUNT) -> None:
    """Fail closed unless exactly ``expected`` SQuAD dev rows were parsed."""

    if row_count != expected:
        raise SystemExit(
            f"FAIL: parsed {row_count} SQuAD dev rows, expected exactly {expected}"
        )


def _validate_dev_sha256(sha256: str, expected: str = EXPECTED_DEV_SHA256) -> None:
    """Fail closed unless the input file matches the canonical dev-v1.1.json SHA256.

    A tampered file that happens to have 10570 rows still fails this check.
    """

    if sha256 != expected:
        raise SystemExit(
            f"FAIL: input SHA256 {sha256} != canonical dev-v1.1.json {expected}; "
            "refusing unexpected/tampered file"
        )


def download_dev(url: str) -> tuple[bytes, str]:
    with request.urlopen(url, timeout=60) as response:
        raw = response.read()
    return raw, hashlib.sha256(raw).hexdigest()


def run_import(
    database_url: str,
    workload_name: str,
    rows: list[SquadRow],
    doc_id_base: int,
) -> int:
    try:
        import psycopg
        from psycopg.types.json import Json
    except ImportError as exc:
        raise SystemExit("requires psycopg") from exc
    with psycopg.connect(database_url) as connection:
        connection.autocommit = False
        with connection.cursor() as cursor:
            for statement in SCHEMA_ALTER_SQL:
                cursor.execute(statement)
            cursor.execute(
                "DELETE FROM documents WHERE workload_name = %s", (workload_name,)
            )
            values = [
                (
                    doc_id_base + index,
                    0,
                    "squad",
                    row.prompt,
                    workload_name,
                    max(1, len(row.prompt) // 4),
                    10,
                    None,
                    None,
                    None,
                    Json(list(row.reference_answers)),
                    row.source_example_id,
                )
                for index, row in enumerate(rows)
            ]
            cursor.executemany(
                "INSERT INTO documents (doc_id, tenant_id, category, text, "
                "workload_name, prompt_tokens, target_output_tokens, "
                "arrival_time_s, session_id, prefix_key, reference_answers, "
                "source_example_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                values,
            )
            inserted = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else len(values)
        connection.commit()
    return inserted


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--workload-name", default="squad_v11_dev_short_answer")
    parser.add_argument(
        "--input",
        help="local dev-v1.1.json path; if omitted, download from --url",
    )
    parser.add_argument("--url", default=SQUAD_DEV_URL)
    parser.add_argument("--doc-id-base", type=int, default=60_000_000)
    parser.add_argument(
        "--provenance",
        required=True,
        help="path to write the provenance JSON sidecar",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse(sys.argv[1:] if argv is None else argv)
    if args.input:
        raw = Path(args.input).read_bytes()
        source_sha256 = hashlib.sha256(raw).hexdigest()
        source_local_path = args.input
    else:
        print(f"downloading {args.url}", flush=True)
        raw, source_sha256 = download_dev(args.url)
        source_local_path = None
    _validate_dev_sha256(source_sha256)
    parsed = json.loads(raw.decode("utf-8"))
    rows = parse_squad_dev(parsed)
    _validate_dev_count(len(rows))
    content_hash = compute_content_hash(rows)
    inserted = run_import(args.database_url, args.workload_name, rows, args.doc_id_base)

    import subprocess
    try:
        importer_commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=CODE_ROOT,
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    except Exception:
        importer_commit = "unknown"

    provenance = {
        "dataset": "SQuAD",
        "version": SQUAD_VERSION,
        "split": SQUAD_SPLIT,
        "source_repo": SQUAD_SOURCE_REPO,
        "source_url": SQUAD_SOURCE_URL,
        "source_revision": SQUAD_SOURCE_REVISION,
        "source_download_method": SQUAD_SOURCE_DOWNLOAD_METHOD,
        "source_local_path": source_local_path,
        "source_file_sha256": source_sha256,
        "source_file_sha256_expected": EXPECTED_DEV_SHA256,
        "sample_count": len(rows),
        "imported_rows": inserted,
        "workload_name": args.workload_name,
        "doc_id_base": args.doc_id_base,
        "prompt_template_sha256": prompt_template_hash(),
        "content_hash": content_hash,
        "cap_max_output_tokens": 64,
        "importer_commit": importer_commit,
    }
    out = Path(args.provenance)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(provenance, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
