"""Check exact text roundtrips and complete Map message context before execution."""

import hashlib
import json

from ..common.private_artifacts import content_digest
from ...execution_provider.semantic_map import canonical_messages, SemanticMapPlan
from ...modalities.text.tokenization import chat_token_count


def text_rows_by_id(rows):
    indexed = {}
    for identity, text in rows:
        if not isinstance(identity, str) or not identity or identity in indexed:
            raise ValueError("text row IDs must be unique nonempty strings")
        if not isinstance(text, str):
            raise ValueError("input must be text")
        text.encode("utf-8")
        indexed[identity] = text
    return indexed


def verify_text_roundtrip(expected, actual) -> dict:
    """Compare by ID without normalizing spaces, JSON escapes, or Unicode values."""
    left, right = text_rows_by_id(expected), text_rows_by_id(actual)
    if left.keys() != right.keys():
        raise ValueError("text roundtrip ID set mismatch")
    if any(left[key].encode("utf-8") != right[key].encode("utf-8") for key in left):
        raise ValueError("text roundtrip content mismatch")
    return {"rows": len(left), "text_values_sha256": content_digest(left), "matched": True}


def context_report(rows, instruction, tokenizer, *, model_id, model_revision,
                   output_tokens, context_tokens) -> dict:
    """Inspect all complete inputs; an over-context row makes fits_context false."""
    plan = SemanticMapPlan(instruction, model_id, output_tokens)
    if type(context_tokens) is not int or context_tokens < 1 or not model_revision:
        raise ValueError("context capacity and model revision must be explicit")
    indexed = text_rows_by_id(rows)
    if not indexed:
        raise ValueError("context check requires input rows")
    details = []
    for identity, text in indexed.items():
        messages = json.loads(canonical_messages(instruction, text))
        count = chat_token_count(tokenizer, messages)
        details.append({
            "source_example_id": identity,
            "input_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "messages_sha256": content_digest(messages),
            "input_tokens": count,
            "fits_context": count + output_tokens <= context_tokens,
        })
    return {
        "schema": "semloom.map.context_check.v1", "model_id": model_id,
        "model_revision": model_revision, "semantic_spec_sha256": plan.digest,
        "context_tokens": context_tokens, "output_tokens": output_tokens,
        "fits_context": all(row["fits_context"] for row in details), "rows": details,
    }
