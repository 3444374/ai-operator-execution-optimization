"""Validate actual PostgreSQL and pgvector identities in stored evidence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable, Mapping


_INVALID_VERSION_VALUES = frozenset(
    {"not_applicable", "not_installed", "unavailable", "unknown"}
)


class DatabaseIdentityError(ValueError):
    """Raised when database identity evidence is missing or inconsistent."""


@dataclass(frozen=True)
class DatabaseIdentity:
    """One actual PostgreSQL/pgvector version pair."""

    server_version: str
    pgvector_version: str

    @classmethod
    def from_record(
        cls,
        record: Mapping[str, object],
        evidence_name: str,
    ) -> "DatabaseIdentity":
        """Parse one record, rejecting sentinels and empty version fields."""

        values: dict[str, str] = {}
        for field in ("server_version", "pgvector_version"):
            raw = record.get(field)
            if not isinstance(raw, str):
                raise DatabaseIdentityError(
                    f"{evidence_name} {field} is missing or drifted or not actual evidence"
                )
            value = raw.strip()
            if not value or value.lower() in _INVALID_VERSION_VALUES:
                raise DatabaseIdentityError(
                    f"{evidence_name} {field} is missing or drifted or not actual evidence"
                )
            values[field] = value
        return cls(**values)

    @classmethod
    def consistent(
        cls,
        records: Iterable[Mapping[str, object]],
        evidence_name: str,
    ) -> "DatabaseIdentity":
        """Require one identical database identity across all records."""

        identities = tuple(
            cls.from_record(record, evidence_name) for record in records
        )
        if not identities:
            raise DatabaseIdentityError(f"{evidence_name} is empty")
        if len(set(identities)) != 1:
            raise DatabaseIdentityError(
                f"{evidence_name} database versions are missing or drifted"
            )
        return identities[0]

    def as_dict(self) -> dict[str, str]:
        """Return stable evidence fields for JSON/CSV records."""

        return asdict(self)


def consistent_database_versions(
    records: Iterable[Mapping[str, object]],
    evidence_name: str,
) -> dict[str, str]:
    """Return the consistent typed identity as stable evidence fields."""

    try:
        return DatabaseIdentity.consistent(records, evidence_name).as_dict()
    except DatabaseIdentityError as error:
        # Engineering decision: preserve the existing runner-facing exception
        # contract while the typed core
        # remains a value validator reusable by baselines and summarizers.
        raise RuntimeError(str(error)) from error
