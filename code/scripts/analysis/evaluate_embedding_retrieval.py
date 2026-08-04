#!/usr/bin/env python3
"""Evaluate saved AI_EMBED vectors against explicit retrieval relevance."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import numpy as np

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.observability.metrics import retrieval_quality_metrics  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--embeddings", required=True, type=Path)
    parser.add_argument(
        "--relevance-csv",
        required=True,
        type=Path,
        help="CSV with query_id,relevant_id columns.",
    )
    parser.add_argument("--k", default="1,5,10")
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def evaluate(
    embeddings_path: Path,
    relevance_path: Path,
    *,
    k_values: tuple[int, ...],
) -> dict[str, object]:
    with np.load(embeddings_path, allow_pickle=False) as payload:
        doc_ids = [str(value) for value in payload["doc_ids"]]
        embeddings = np.asarray(payload["embeddings"], dtype=float)
    if embeddings.ndim != 2 or len(embeddings) != len(doc_ids):
        raise ValueError("embedding archive must contain aligned doc_ids and 2-D embeddings")
    if len(set(doc_ids)) != len(doc_ids):
        raise ValueError("embedding doc_ids must be unique")
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    if np.any(norms == 0) or not np.all(np.isfinite(embeddings)):
        raise ValueError("embeddings must be finite and non-zero")
    normalized = embeddings / norms
    relevance: dict[str, set[str]] = {}
    with relevance_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            query_id = str(row["query_id"])
            relevance.setdefault(query_id, set()).add(str(row["relevant_id"]))
    index_by_id = {doc_id: index for index, doc_id in enumerate(doc_ids)}
    rankings: dict[str, list[str]] = {}
    for query_id in relevance:
        if query_id not in index_by_id:
            continue
        query_index = index_by_id[query_id]
        scores = normalized @ normalized[query_index]
        order = np.argsort(-scores, kind="stable")
        rankings[query_id] = [
            doc_ids[index] for index in order if index != query_index
        ]
    return {
        "schema_version": 1,
        "embedding_archive": str(embeddings_path),
        "relevance_csv": str(relevance_path),
        "self_match_excluded": True,
        "embedding_rows": len(doc_ids),
        **retrieval_quality_metrics(
            rankings,
            relevance,
            k_values=k_values,
        ),
    }


def main() -> None:
    args = parse_args()
    if args.output.exists():
        raise FileExistsError(f"output already exists: {args.output}")
    k_values = tuple(int(value) for value in args.k.split(",") if value.strip())
    result = evaluate(args.embeddings, args.relevance_csv, k_values=k_values)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
