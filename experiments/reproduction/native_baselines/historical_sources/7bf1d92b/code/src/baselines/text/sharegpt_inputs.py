"""Select first-human ShareGPT text verbatim, independently of task or prompt choice."""

from collections import Counter
import hashlib
import re

from ..common.private_artifacts import content_digest
from .map_inputs import text_rows_by_id

SCHEMA = "semloom.sharegpt.first_human.v1"


def first_human_verbatim(record):
    turns = record.get("conversations", [])
    if not isinstance(turns, list):
        return None
    for turn in turns:
        if isinstance(turn, dict) and turn.get("from") == "human":
            value = turn.get("value")
            return value if isinstance(value, str) and value.strip() else None
    return None


def prepare_sharegpt_manifest(records, source_sha256, *, count=16,
                              minimum_bytes=256, maximum_bytes=2048):
    if not isinstance(records, list):
        raise ValueError("ShareGPT source must be a JSON record array")
    if not re.fullmatch(r"[0-9a-f]{64}", source_sha256):
        raise ValueError("source SHA256 is required")
    if any(type(value) is not int for value in (count, minimum_bytes, maximum_bytes)):
        raise ValueError("selection sizes must be integers")
    if count < 1 or minimum_bytes < 0 or maximum_bytes < minimum_bytes:
        raise ValueError("invalid selection sizes")
    rows, skipped, examined = [], Counter(), 0
    for position, record in enumerate(records):
        if len(rows) == count:
            break
        examined += 1
        if not isinstance(record, dict):
            skipped["invalid_record"] += 1
            continue
        text = first_human_verbatim(record)
        if text is None:
            skipped["missing_nonempty_first_human"] += 1
            continue
        size = len(text.encode("utf-8"))
        if not minimum_bytes <= size <= maximum_bytes:
            skipped["outside_byte_range"] += 1
            continue
        rows.append({
            "source_example_id": record.get("id"), "source_position": position,
            "input_text": text, "input_bytes": size,
            "input_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        })
    text_rows_by_id((row["source_example_id"], row["input_text"]) for row in rows)
    if len(rows) != count:
        raise ValueError("not enough eligible first-human records")
    manifest = {
        "schema": SCHEMA, "source_sha256": source_sha256,
        "source_rows": len(records), "selection": {
            "count": count, "minimum_bytes": minimum_bytes, "maximum_bytes": maximum_bytes,
        }, "skipped_before_selection": dict(skipped),
        "unexamined_rows": len(records) - examined, "rows": rows,
    }
    manifest["sha256"] = content_digest(manifest)
    return manifest


def validate_sharegpt_manifest(manifest):
    if manifest.get("schema") != SCHEMA:
        raise ValueError("not a private ShareGPT workload")
    payload = {key: value for key, value in manifest.items() if key != "sha256"}
    if manifest.get("sha256") != content_digest(payload):
        raise ValueError("ShareGPT manifest identity mismatch")
    rows = manifest["rows"]
    if not isinstance(manifest.get("source_sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", manifest["source_sha256"]):
        raise ValueError("invalid ShareGPT source identity")
    selection = manifest["selection"]
    if any(type(selection.get(key)) is not int for key in ("count", "minimum_bytes", "maximum_bytes")):
        raise ValueError("invalid ShareGPT selection")
    if selection["count"] < 1 or not 0 <= selection["minimum_bytes"] <= selection["maximum_bytes"]:
        raise ValueError("invalid ShareGPT selection")
    text_rows_by_id((row["source_example_id"], row["input_text"]) for row in rows)
    if len(rows) != manifest["selection"]["count"]:
        raise ValueError("ShareGPT selection count mismatch")
    previous = -1
    for row in rows:
        position = row["source_position"]
        if type(position) is not int or not previous < position < manifest["source_rows"]:
            raise ValueError("invalid ShareGPT source positions")
        previous = position
        encoded = row["input_text"].encode("utf-8")
        if not selection["minimum_bytes"] <= len(encoded) <= selection["maximum_bytes"]:
            raise ValueError("ShareGPT selection byte range mismatch")
        if row["input_sha256"] != hashlib.sha256(encoded).hexdigest() or row["input_bytes"] != len(encoded):
            raise ValueError("ShareGPT text identity mismatch")
