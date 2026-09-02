"""Immutable choice-profile values and canonical identity, not vendor settings."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import struct


TRISTATE_PROFILE_ID = "semloom.generation.choice.tristate"
TRISTATE_PROFILE_VERSION = 1
CHOICE_CONSTRAINT_KIND = "CHOICE"
TRISTATE_CHOICES = ("TRUE", "FALSE", "UNKNOWN")


@dataclass(frozen=True)
class GenerationProfile:
    """The complete contents of the one supported tristate choice profile."""

    profile_id: str
    profile_version: int
    constraint_kind: str
    choices: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            type(self.profile_id) is not str or self.profile_id != TRISTATE_PROFILE_ID
            or type(self.profile_version) is not int or self.profile_version != TRISTATE_PROFILE_VERSION
            or type(self.constraint_kind) is not str or self.constraint_kind != CHOICE_CONSTRAINT_KIND
            or type(self.choices) is not tuple or self.choices != TRISTATE_CHOICES
            or any(type(choice) is not str for choice in self.choices)
        ):
            raise ValueError("invalid generation profile")

    def canonical_bytes(self) -> bytes:
        """Encode every profile field in its identity domain, preserving choice order."""
        def text(value: str) -> bytes:
            encoded = value.encode("utf-8")
            return struct.pack("!I", len(encoded)) + encoded

        return (
            b"semloom-generation-profile-v1\0"
            + text(self.profile_id)
            + struct.pack("!I", self.profile_version)
            + text(self.constraint_kind)
            + struct.pack("!I", len(self.choices))
            + b"".join(text(choice) for choice in self.choices)
        )

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def to_record(self) -> dict[str, object]:
        """Return a fresh record; its mutable list never aliases the profile."""
        return {
            "profile_id": self.profile_id,
            "profile_version": self.profile_version,
            "constraint_kind": self.constraint_kind,
            "choices": list(self.choices),
            "profile_digest": self.digest,
        }

    @classmethod
    def from_record(cls, record: object) -> GenerationProfile:
        """Validate complete, already-decoded data without consulting a registry.

        JSON syntax/duplicate-key validation belongs to the versioned wire decoder.
        Invalid data is rejected with a static message, never echoed to a caller.
        """
        fields = {"profile_id", "profile_version", "constraint_kind", "choices", "profile_digest"}
        if type(record) is not dict or len(record) != len(fields) or set(record) != fields:
            raise ValueError("invalid generation profile")
        if type(record["choices"]) is not list or len(record["choices"]) != len(TRISTATE_CHOICES):
            raise ValueError("invalid generation profile")
        profile = cls(
            profile_id=record["profile_id"], profile_version=record["profile_version"],
            constraint_kind=record["constraint_kind"], choices=tuple(record["choices"]),
        )
        if type(record["profile_digest"]) is not str or record["profile_digest"] != profile.digest:
            raise ValueError("invalid generation profile")
        return profile
