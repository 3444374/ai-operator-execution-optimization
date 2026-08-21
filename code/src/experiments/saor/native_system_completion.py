"""Completion-trace correctness evidence for the SAOR five-arm matrix.

The matched experiment ends at the model-completion barrier and performs no
database writeback.  This module validates executor-owned completion traces
against the frozen manifest identities after the measured boundary.
"""

from __future__ import annotations

import csv
import hashlib
import json
import time
from collections.abc import Iterable
from pathlib import Path


def collect_completion_rows(output_dir: Path) -> list[tuple[int, str]]:
    """Collect exactly one completed output per doc_id from executor traces."""

    jobs_root = output_dir / "jobs"
    paths = sorted(jobs_root.glob("**/*.completions.csv"))
    if not paths:
        paths = sorted({
            *jobs_root.glob("**/*.requests.csv"),
            *jobs_root.glob("**/requests.csv"),
        })
    if not paths:
        raise RuntimeError("completion evidence cannot find request traces")
    by_doc: dict[int, str] = {}
    for path in paths:
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            if "output_text" not in (reader.fieldnames or ()):
                raise RuntimeError("completion evidence trace omits output_text")
            for row in reader:
                if row.get("status") != "completed":
                    raise RuntimeError("completion evidence observed a failed request")
                doc_id = int(row["doc_id"])
                if doc_id in by_doc:
                    raise RuntimeError("completion evidence observed duplicate doc_id")
                by_doc[doc_id] = str(row.get("output_text", "") or "")
    if not by_doc:
        raise RuntimeError("completion evidence collected no rows")
    return sorted(by_doc.items())


def expected_doc_ids_from_manifests(paths: Iterable[Path]) -> tuple[int, ...]:
    """Load the exact expected document identity set from frozen JSONL manifests."""

    doc_ids: set[int] = set()
    for path in paths:
        with path.open(encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                if not line.strip():
                    continue
                try:
                    value = json.loads(line)
                    doc_id = int(value["doc_id"])
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise RuntimeError(
                        f"manifest {path.name} line {line_number} lacks a valid doc_id"
                    ) from exc
                if doc_id in doc_ids:
                    raise RuntimeError("frozen manifests contain duplicate doc_id")
                doc_ids.add(doc_id)
    if not doc_ids:
        raise RuntimeError("frozen manifests contain no doc_id")
    return tuple(sorted(doc_ids))


def build_completion_evidence(
    rows: list[tuple[int, str]],
    *,
    expected_doc_ids: Iterable[int],
    producer: str,
) -> dict[str, object]:
    """Validate completion identities and return a no-writeback evidence seal."""

    if producer not in {"native_official_adapter", "project_profiler"}:
        raise ValueError("completion evidence producer is invalid")
    expected = tuple(sorted(int(doc_id) for doc_id in expected_doc_ids))
    if len(expected) != len(set(expected)):
        raise RuntimeError("expected completion identities contain duplicates")
    observed = tuple(doc_id for doc_id, _text in sorted(rows))
    if len(observed) != len(set(observed)):
        raise RuntimeError("completion evidence observed duplicate doc_id")
    if observed != expected:
        raise RuntimeError("completion evidence doc_id set mismatch")
    expected_digest = _json_digest(expected)
    observed_digest = _json_digest(observed)
    return {
        "status": "passed",
        "mode": "completion_trace_digest",
        "producer": producer,
        "expected_rows": len(expected),
        "observed_rows": len(observed),
        "expected_doc_id_digest": expected_digest,
        "observed_doc_id_digest": observed_digest,
        "output_digest": _json_digest(sorted(rows)),
        "exactly_once": True,
        "verified_epoch_s": time.time(),
    }


def _json_digest(value: object) -> str:
    """Return one stable SHA-256 digest for JSON-compatible evidence."""

    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")).hexdigest()
