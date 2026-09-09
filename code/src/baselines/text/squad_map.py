"""Prepare versioned PG Map inputs and join predictions to SQuAD references.

This offline adapter owns neither model execution nor scheduling. Original
SQuAD parsing and answer scoring remain in their existing implementations.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from typing import Protocol

from ...observability.metrics.squad import squad_quality_metrics


WORKLOAD = "squad_v11_pg_map_v1"
INSTRUCTION = (
    "Answer the question using only the context. "
    "Return only the shortest answer span. Do not explain."
)
INPUT_TEMPLATE = "Context:\n{context}\n\nQuestion:\n{question}\n\nAnswer:\n"
SPLITS = ("tuning", "evaluation")


class SquadExample(Protocol):
    source_example_id: str
    context: str
    question: str
    reference_answers: Sequence[str]


def content_digest(value: object) -> str:
    """Hash the full structured value, including list order and Unicode text."""
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def map_messages(input_text: str) -> list[dict[str, str]]:
    return [
        {"role": "system", "content": INSTRUCTION},
        {"role": "user", "content": input_text},
    ]


def prepare_manifest(
    examples: Sequence[SquadExample], source_sha256: str, *, rows_per_split: int = 64,
) -> dict:
    """Choose disjoint context groups, then bounded prefixes in source order."""
    if type(rows_per_split) is not int or rows_per_split < 1:
        raise ValueError("rows_per_split must be a positive integer")
    identities = [row.source_example_id for row in examples]
    if not identities or any(not value for value in identities) or len(set(identities)) != len(identities):
        raise ValueError("source IDs must be nonempty and unique")
    context_hashes = [hashlib.sha256(row.context.encode("utf-8")).hexdigest() for row in examples]
    assignment = {key: SPLITS[index % 2] for index, key in enumerate(sorted(set(context_hashes)))}
    splits: dict[str, list[dict]] = {key: [] for key in SPLITS}
    for index, (example, context_hash) in enumerate(zip(examples, context_hashes)):
        target = splits[assignment[context_hash]]
        if len(target) == rows_per_split:
            continue
        references = list(example.reference_answers)
        if not references or not all(isinstance(value, str) and value.strip() for value in references):
            raise ValueError("reference answers must be nonempty strings")
        input_text = INPUT_TEMPLATE.format(context=example.context, question=example.question)
        target.append({
            "source_example_id": example.source_example_id,
            "source_position": index,
            "context_sha256": context_hash,
            "input_text": input_text,
            "input_bytes": len(input_text.encode("utf-8")),
            "messages_sha256": content_digest(map_messages(input_text)),
            "reference_answers": references,
        })
    if any(len(rows) != rows_per_split for rows in splits.values()):
        raise ValueError("insufficient rows in disjoint context partitions")
    manifest = {
        "schema": WORKLOAD,
        "source_sha256": source_sha256,
        "source_rows": len(examples),
        "partition": "sorted_context_sha256_alternating_then_source_order_prefix",
        "instruction": INSTRUCTION,
        "input_template": INPUT_TEMPLATE,
        "generation": {"temperature": 0, "max_tokens": 64},
        "model_identity": "pending_runtime_binding",
        "splits": splits,
    }
    manifest["sha256"] = content_digest(manifest)
    return manifest


def validate_manifest(manifest: Mapping) -> None:
    """Reject altered manifests before reading predictions or running SQL."""
    payload = {key: value for key, value in manifest.items() if key != "sha256"}
    if manifest.get("schema") != WORKLOAD or manifest.get("sha256") != content_digest(payload):
        raise ValueError("manifest identity mismatch")
    if manifest["instruction"] != INSTRUCTION or manifest["input_template"] != INPUT_TEMPLATE:
        raise ValueError("unsupported message contract")
    if manifest["generation"] != {"temperature": 0, "max_tokens": 64}:
        raise ValueError("unsupported generation contract")
    all_ids: set[str] = set()
    contexts: dict[str, set[str]] = {}
    for split in SPLITS:
        rows = manifest["splits"][split]
        if not rows:
            raise ValueError("empty split")
        contexts[split] = set()
        for row in rows:
            identity = row["source_example_id"]
            if not isinstance(identity, str) or not identity or identity in all_ids:
                raise ValueError("duplicate or invalid manifest ID")
            all_ids.add(identity)
            contexts[split].add(row["context_sha256"])
            if row["messages_sha256"] != content_digest(map_messages(row["input_text"])):
                raise ValueError("message identity mismatch")
            references = row["reference_answers"]
            if not references or not all(isinstance(value, str) and value.strip() for value in references):
                raise ValueError("invalid reference answers")
    if contexts["tuning"] & contexts["evaluation"]:
        raise ValueError("context leakage between partitions")


def evaluate_predictions(manifest: Mapping, split: str, records: Iterable[Mapping]) -> dict:
    """Score raw predictions without dropping failed rows or collapsing duplicates."""
    validate_manifest(manifest)
    if split not in SPLITS:
        raise ValueError("unknown split")
    references = {row["source_example_id"]: row["reference_answers"] for row in manifest["splits"][split]}
    predictions: dict[str, str | None] = {}
    for record in records:
        identity, prediction = record["source_example_id"], record["prediction"]
        if identity in predictions:
            raise ValueError("duplicate prediction ID")
        if identity not in references:
            raise ValueError("unknown prediction ID")
        if prediction is not None and not isinstance(prediction, str):
            raise ValueError("prediction must be text or null")
        predictions[identity] = prediction
    return {
        "workload": WORKLOAD,
        "manifest_sha256": manifest["sha256"],
        "split": split,
        "prediction_sha256": content_digest(predictions),
        "association_complete": set(predictions) == set(references),
        "null_predictions": sum(value is None for value in predictions.values()),
        "empty_predictions": sum(value == "" for value in predictions.values()),
        "metrics": squad_quality_metrics(predictions, references),
        "performance": "unavailable: offline evaluation has no execution timestamps",
    }
