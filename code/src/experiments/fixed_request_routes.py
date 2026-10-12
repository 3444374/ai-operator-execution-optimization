"""Declared, load-independent HTTP destinations for a finite matched workload.

Selection reads complete JSON values but never rewrites their encoded bytes.
Equivalent input occurrences select the same replica. This transport experiment
does not identify database source rows from HTTP arrival sequence numbers.
"""
from __future__ import annotations

import hashlib
import json
from types import MappingProxyType
from typing import Mapping
from urllib.parse import urlsplit

from src.observability.request_gateway import GatewayRoute


def request_values_sha256(value: dict) -> str:
    if not isinstance(value, dict):
        raise ValueError("complete request values must be a JSON object")
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False,
                         separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON request key")
        result[key] = value
    return result


class FixedRequestRoutes:
    """Choose only from an immutable complete-request digest manifest."""

    def __init__(self, endpoints: Mapping[str, str], assignments: Mapping[str, str]):
        if not endpoints or not assignments:
            raise ValueError("fixed routing requires endpoints and request assignments")
        checked = dict(endpoints)
        if len(set(checked.values())) != len(checked):
            raise ValueError("replicas require different endpoint URLs")
        for identity, url in checked.items():
            parsed = urlsplit(url)
            if (not identity or parsed.scheme not in ("http", "https") or not parsed.hostname
                    or parsed.username or parsed.password or parsed.fragment):
                raise ValueError("invalid declared HTTP replica")
        manifest = dict(assignments)
        for digest, identity in manifest.items():
            if (len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest)
                    or identity not in checked):
                raise ValueError("request digest must name one declared replica")
        self._endpoints = MappingProxyType(checked)
        self._assignments = MappingProxyType(manifest)
        public = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        self.manifest_sha256 = hashlib.sha256(public).hexdigest()

    def __call__(self, original: GatewayRoute, body: bytes) -> GatewayRoute:
        values = json.loads(body, object_pairs_hook=_unique_object)
        digest = request_values_sha256(values)
        identity = self._assignments[digest]  # Unknown requests fail before sending.
        return GatewayRoute(original.job_id, identity, self._endpoints[identity])
