"""Same-request baseline adapters and artifact contracts."""

from .contracts import BaselineRequestResult, ChatRequest, ManifestMetadata
from .manifests import (
    assign_endpoint_shards,
    read_manifest,
    write_manifest,
)
from .results import summarize_results, validate_results

__all__ = [
    "BaselineRequestResult",
    "ChatRequest",
    "ManifestMetadata",
    "assign_endpoint_shards",
    "read_manifest",
    "summarize_results",
    "validate_results",
    "write_manifest",
]
