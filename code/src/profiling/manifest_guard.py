"""Fail-closed request-manifest validation for project profiler inputs."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import pyarrow as pa

from ..baselines.contracts import ChatRequest
from ..baselines.manifests import read_manifest


@dataclass(frozen=True)
class ProfileManifestEvidence:
    manifest_sha256: str
    manifest_rows: int
    validated_rows: int


def validate_profile_manifest_contract(
    requests: Sequence[ChatRequest],
    *,
    total_rows: int,
    operator: str,
    model_backend: str,
    endpoint_count: int,
    completion_protocol: str,
    completion_prompt_format: str,
    completion_temperature: float | None,
    completion_max_tokens: int,
    output_cost_mode: str,
    source_order: str,
    executor: str,
    submission_granularity: str,
    endpoint_routing: str,
    arrival_replay: bool,
) -> None:
    if total_rows != len(requests):
        raise ValueError(
            f"profile row count {total_rows} does not match manifest row count "
            f"{len(requests)}"
        )
    if operator != "ai_complete":
        raise ValueError("request manifest requires ai_complete")
    if model_backend != "compatible_http":
        raise ValueError("request manifest requires compatible_http")
    if endpoint_count != 2:
        raise ValueError("request manifest comparison requires two endpoints")
    if completion_protocol != "chat_completions":
        raise ValueError("request manifest requires chat_completions")
    if completion_prompt_format != "raw":
        raise ValueError("request manifest requires raw prompt format")
    if completion_temperature != 0.0:
        raise ValueError("request manifest requires temperature=0")
    output_caps = {request.max_output_tokens for request in requests}
    if output_caps != {completion_max_tokens}:
        raise ValueError(
            "profile max output does not match manifest max output"
        )
    if output_cost_mode != "trace_target_output":
        raise ValueError("request manifest requires trace_target_output")
    if source_order != "doc_id":
        raise ValueError("request manifest requires doc_id source order")
    if executor != "ray_actor":
        raise ValueError("request manifest requires ray_actor")
    if submission_granularity != "request":
        raise ValueError("request manifest requires request granularity")
    if endpoint_routing != "manifest_pinned":
        raise ValueError("request manifest requires manifest_pinned routing")
    if arrival_replay:
        raise ValueError("request manifest comparison forbids arrival replay")


class ProfileManifestGuard:
    """Validate source rows and attach their frozen endpoint assignment."""

    def __init__(
        self,
        *,
        requests: Sequence[ChatRequest],
        manifest_sha256: str,
        endpoint_ids: Sequence[str],
    ) -> None:
        if len(manifest_sha256) != 64:
            raise ValueError("manifest_sha256 must be a SHA-256 digest")
        if not endpoint_ids or any(not endpoint_id for endpoint_id in endpoint_ids):
            raise ValueError("endpoint_ids must be non-empty")
        by_doc_id: dict[int, ChatRequest] = {}
        for request in requests:
            if request.doc_id in by_doc_id:
                raise ValueError(f"duplicate manifest doc_id: {request.doc_id}")
            if not 0 <= request.endpoint_index < len(endpoint_ids):
                raise ValueError(
                    f"manifest endpoint_index is outside topology: "
                    f"{request.endpoint_index}"
                )
            by_doc_id[request.doc_id] = request
        self._requests = tuple(requests)
        self._by_doc_id = by_doc_id
        self._manifest_sha256 = manifest_sha256
        self._endpoint_ids = tuple(endpoint_ids)
        self._seen: set[int] = set()

    @property
    def requests(self) -> tuple[ChatRequest, ...]:
        return self._requests

    @property
    def manifest_sha256(self) -> str:
        return self._manifest_sha256

    @classmethod
    def from_path(
        cls,
        path: str | Path,
        endpoint_ids: Sequence[str],
    ) -> "ProfileManifestGuard":
        source = Path(path)
        payload = source.read_bytes()
        return cls(
            requests=read_manifest(source),
            manifest_sha256=hashlib.sha256(payload).hexdigest(),
            endpoint_ids=endpoint_ids,
        )

    def validate_and_annotate(
        self,
        table: pa.Table | pa.RecordBatch,
    ) -> pa.Table | pa.RecordBatch:
        required = {
            "doc_id",
            "text",
            "prompt_tokens",
            "target_output_tokens",
        }
        missing = required - set(table.column_names)
        if missing:
            raise ValueError(
                f"source rows are missing manifest fields: {sorted(missing)}"
            )
        if "preferred_endpoint_id" in table.column_names:
            raise ValueError("source rows already contain preferred_endpoint_id")

        validated_doc_ids: list[int] = []
        preferred_endpoint_ids: list[str] = []
        batch_doc_ids: set[int] = set()
        for row_index in range(table.num_rows):
            doc_id = int(table.column("doc_id")[row_index].as_py())
            if doc_id in self._seen or doc_id in batch_doc_ids:
                raise ValueError(f"duplicate manifest row: {doc_id}")
            request = self._by_doc_id.get(doc_id)
            if request is None:
                raise ValueError(f"source row is absent from manifest: {doc_id}")
            prompt = table.column("text")[row_index].as_py()
            if prompt != request.prompt:
                raise ValueError(f"prompt mismatch for doc_id {doc_id}")
            prompt_tokens = table.column("prompt_tokens")[row_index].as_py()
            if prompt_tokens != request.prompt_tokens:
                raise ValueError(
                    f"prompt_tokens mismatch for doc_id {doc_id}"
                )
            output_tokens = table.column("target_output_tokens")[
                row_index
            ].as_py()
            if output_tokens != request.estimated_output_tokens:
                raise ValueError(
                    f"target_output_tokens mismatch for doc_id {doc_id}"
                )
            batch_doc_ids.add(doc_id)
            validated_doc_ids.append(doc_id)
            preferred_endpoint_ids.append(
                self._endpoint_ids[request.endpoint_index]
            )

        self._seen.update(validated_doc_ids)
        return table.append_column(
            "preferred_endpoint_id",
            pa.array(preferred_endpoint_ids, type=pa.string()),
        )

    def finish(self) -> ProfileManifestEvidence:
        missing = set(self._by_doc_id) - self._seen
        if missing:
            preview = sorted(missing)[:10]
            raise ValueError(
                f"missing manifest rows: {preview}"
                + ("..." if len(missing) > len(preview) else "")
            )
        return ProfileManifestEvidence(
            manifest_sha256=self._manifest_sha256,
            manifest_rows=len(self._requests),
            validated_rows=len(self._seen),
        )
