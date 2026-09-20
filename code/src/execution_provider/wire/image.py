"""Wire seven: strict binary image tasks and float4 results, sync or incremental."""

from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass

from ..semantic_image import MAX_IMAGE_BYTES, SemanticImagePlan, canonical_text
from .framing import ProtocolError, has_duplicate_fields
from .semantic import decimal_uint64


VERSION = 7
MODES = ("reference", "staged")


def execution_id(mode: str) -> str:
    if mode not in MODES:
        raise ValueError("unsupported image execution mode")
    return f"semloom.provider.image-{mode}.uds.v7"


def physical_digest(mode: str) -> str:
    execution_id(mode)
    return hashlib.sha256(b"semloom-image-physical-v1\0" + canonical_text(mode)).hexdigest()


def execution_digest(plan: SemanticImagePlan, mode: str) -> str:
    return hashlib.sha256(
        b"semloom-image-execution-v1\0" + canonical_text(execution_id(mode))
        + plan.digest.encode("ascii")
    ).hexdigest()


def identities(plan, mode):
    return dict(semantic_spec_digest=plan.digest, physical_algorithm_digest=physical_digest(mode),
                provider_execution_digest=execution_digest(plan, mode))


def open_message(plan: SemanticImagePlan, mode: str, window: int) -> dict:
    if type(window) is not int or window < 1 or (mode == "reference" and window != 1):
        raise ValueError("invalid image input window")
    return dict(type="open", protocol_version=VERSION, **identities(plan, mode),
                provider_execution_id=execution_id(mode), execution_mode=mode,
                plan=plan.to_record(), max_inflight_tasks=window)


def validate_open(message, expected_plan: SemanticImagePlan):
    try:
        if type(message) is not dict or has_duplicate_fields(message):
            raise ValueError("invalid object")
        plan = SemanticImagePlan.from_record(message["plan"])
        mode, window = message["execution_mode"], message["max_inflight_tasks"]
        expected = open_message(plan, mode, window)
        if (message != expected or set(message) != set(expected)
                or type(message["protocol_version"]) is not int or plan != expected_plan):
            raise ValueError("invalid image open identity")
        return mode, window
    except (ValueError, TypeError, KeyError, UnicodeError) as error:
        raise ProtocolError("INVALID_OPEN") from error


@dataclass(frozen=True)
class ImageRequest:
    encoded: bytes
    payload_digest: str


def task_message(plan, mode, sequence: int, encoded: bytes):
    if type(sequence) is not int or not 0 <= sequence < 2**64:
        raise ValueError("sequence must be uint64")
    return dict(type="task" if mode == "reference" else "offer", protocol_version=VERSION,
                **identities(plan, mode), sequence=str(sequence),
                semantic_payload_digest=plan.payload_digest(encoded), encoded_hex=encoded.hex())


def validate_task(message, plan, mode, expected_sequence):
    try:
        if type(message) is not dict or has_duplicate_fields(message):
            raise ValueError("invalid object")
        sequence = decimal_uint64(message["sequence"])
        encoded_hex = message["encoded_hex"]
        if (type(encoded_hex) is not str or len(encoded_hex) % 2
                or not 2 <= len(encoded_hex) <= MAX_IMAGE_BYTES * 2
                or any(ch not in "0123456789abcdef" for ch in encoded_hex)):
            raise ValueError("invalid encoded bytes")
        encoded = bytes.fromhex(encoded_hex)
        expected = task_message(plan, mode, sequence, encoded)
        if (message != expected or set(message) != set(expected)
                or type(message["protocol_version"]) is not int or sequence != expected_sequence):
            raise ValueError("invalid image task identity")
        return ImageRequest(encoded, expected["semantic_payload_digest"])
    except (ValueError, TypeError, KeyError, UnicodeError) as error:
        raise ProtocolError("INVALID_TASK") from error


def completion_digest(plan, mode, sequence, payload_digest, result):
    plan.validate_result(result)
    ids = identities(plan, mode)
    return hashlib.sha256(
        b"semloom-image-completion-v1\0"
        + b"".join(value.encode("ascii") for value in ids.values())
        + payload_digest.encode("ascii") + struct.pack("!Q", sequence) + result
    ).hexdigest()


def completion_message(plan, mode, sequence, payload_digest, result):
    return dict(type="completion", protocol_version=VERSION, **identities(plan, mode),
                sequence=str(sequence), semantic_payload_digest=payload_digest,
                output_hex=plan.validate_result(result).hex(),
                completion_evidence_digest=completion_digest(plan, mode, sequence, payload_digest, result))
