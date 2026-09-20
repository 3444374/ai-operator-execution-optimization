"""PostgreSQL-owned encoded-image embedding semantics, independent of Ray."""

from __future__ import annotations

import hashlib
import math
import re
import struct
from dataclasses import asdict, dataclass


MAX_IMAGE_BYTES = 262_144
MAX_IMAGE_PIXELS = 16_777_216
IMAGE_SPEC_ID = "semloom.semantic.image_embed.clip.v1"
IMAGE_PARSER_ID = "semloom.image_embed.finite_float4.v1"
IMAGE_PREPARE_ID = "semloom.image_embed.clip_rgb_fp32.v1"


def canonical_text(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack("!I", len(encoded)) + encoded


@dataclass(frozen=True)
class SemanticImagePlan:
    """All physical paths consume exactly this database-selected identity.

    Projection and L2 normalization are mandatory in this first CLIP profile.
    The output uses network-order IEEE float32; model arithmetic may use another
    declared dtype. Revisions identify immutable assets, never local paths.
    """

    model_id: str
    model_revision: str
    processor_id: str
    processor_revision: str
    dtype: str
    dimension: int
    input_size: int

    def __post_init__(self) -> None:
        for name in ("model_id", "processor_id"):
            value = getattr(self, name)
            if (
                type(value) is not str or not 1 <= len(value.encode("utf-8")) <= 128
                or "\0" in value
            ):
                raise ValueError(f"{name} must contain 1..128 UTF-8 bytes without NUL")
        for name in ("model_revision", "processor_revision"):
            value = getattr(self, name)
            if type(value) is not str or re.fullmatch(r"[0-9a-f]{40}", value) is None:
                raise ValueError(f"{name} must be an immutable 40-character revision")
        if self.dtype not in ("float16", "float32", "bfloat16"):
            raise ValueError("unsupported CLIP arithmetic dtype")
        for name, maximum in (("dimension", 4096), ("input_size", 1024)):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"{name} must be in 1..{maximum}")

    @property
    def prepared_bytes(self) -> int:
        return 3 * self.input_size * self.input_size * 4

    @property
    def result_bytes(self) -> int:
        return self.dimension * 4

    def to_record(self) -> dict:
        return asdict(self)

    @classmethod
    def from_record(cls, value: object) -> SemanticImagePlan:
        if type(value) is not dict or set(value) != set(cls.__dataclass_fields__):
            raise ValueError("image plan fields do not match the versioned schema")
        return cls(**value)

    @property
    def digest(self) -> str:
        fields = (
            IMAGE_SPEC_ID, "bytea", "real[]", IMAGE_PREPARE_ID, IMAGE_PARSER_ID,
            "PROPAGATE_NULL", "FAIL_QUERY", "INPUT_ORDER", "projected_l2",
            self.model_id, self.model_revision, self.processor_id,
            self.processor_revision, self.dtype,
        )
        canonical = b"semloom-image-spec-v1\0" + b"".join(map(canonical_text, fields))
        canonical += struct.pack("!IIII", self.dimension, self.input_size,
                                 MAX_IMAGE_BYTES, MAX_IMAGE_PIXELS)
        return hashlib.sha256(canonical).hexdigest()

    def payload_digest(self, encoded: bytes) -> str:
        self.validate_input(encoded)
        return hashlib.sha256(
            b"semloom-image-payload-v1\0" + self.digest.encode("ascii")
            + struct.pack("!Q", len(encoded)) + encoded
        ).hexdigest()

    @staticmethod
    def validate_input(encoded: bytes) -> None:
        if type(encoded) is not bytes or not 1 <= len(encoded) <= MAX_IMAGE_BYTES:
            raise ValueError("image must contain 1..262144 encoded bytes")

    def encode_result(self, values) -> bytes:
        # Iteration is capped before materializing a provider's unknown shape.
        result = bytearray()
        for index, value in enumerate(values):
            if index >= self.dimension or isinstance(value, (bool, str, bytes)):
                raise ValueError("image result is not a fixed-dimensional vector")
            try:
                number = float(value)
                if not math.isfinite(number):
                    raise ValueError("image result contains a non-finite value")
                packed = struct.pack("!f", number)
            except (TypeError, OverflowError, struct.error) as error:
                raise ValueError("image result is not finite float4") from error
            if not math.isfinite(struct.unpack("!f", packed)[0]):
                raise ValueError("image result overflows float4")
            result.extend(packed)
        return self.validate_result(bytes(result))

    def validate_result(self, result: bytes) -> bytes:
        if type(result) is not bytes or len(result) != self.result_bytes:
            raise ValueError("image result length does not match its dimension")
        if not all(math.isfinite(value[0]) for value in struct.iter_unpack("!f", result)):
            raise ValueError("image result contains a non-finite float4")
        return result
