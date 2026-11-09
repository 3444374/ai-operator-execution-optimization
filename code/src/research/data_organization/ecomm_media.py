"""Small CPU-only building blocks for the authored SemBench Ecomm Q11 probe."""

from collections import Counter
from contextlib import contextmanager
import hashlib
import json
import math
from statistics import median
from time import perf_counter


def asset_tiles(assets, tile_assets):
    """Keep duplicate occurrences; visit equal source references in one tile."""
    if tile_assets < 1:
        raise ValueError("tile_assets must be positive")
    groups = {}
    for position, asset in enumerate(assets):
        groups.setdefault(asset, []).append(position)
    groups = list(groups.values())
    return [tuple(p for group in groups[i:i + tile_assets] for p in group)
            for i in range(0, len(groups), tile_assets)]


class TileImages:
    """Reuse only immutable native base64 strings until a tile has completed.

    The caller owns synchronous completion and clears this object at tile end.
    Native ImageArray's per-index cache remains active in both controls.
    """

    def __init__(self, fetch):
        self.fetch = fetch
        self.values = {}
        self.hits = 0

    def __call__(self, image, image_type="Image"):
        if image_type != "base64" or not isinstance(image, str):
            return self.fetch(image, image_type)
        key = (image, image_type)
        if key not in self.values:
            value = self.fetch(image, image_type)
            if not isinstance(value, str):
                raise TypeError("the native base64 image preparation did not return a string")
            self.values[key] = value
        else:
            self.hits += 1
        return self.values[key]

    def clear(self):
        self.values.clear()


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


class RequestAudit:
    """Record complete private API inputs and replay a response per occurrence.

    Only hashes and response strings remain in memory. The audit file includes
    media bytes and belongs in a private run directory, never a public result.
    """

    def __init__(self, stream, *, fixed=None, pattern="all_true", max_requests):
        self.stream = stream
        self.fixed = fixed
        self.pattern = pattern
        self.max_requests = max_requests
        self.requests = {}
        self.responses = {}
        self.issue_order = []
        self.stage_counts = Counter()
        self.input_sizes = []
        self.audit_s = 0.0
        self._pending = None

    @contextmanager
    def operation(self, stage, occurrences):
        if self._pending is not None:
            raise RuntimeError("nested logical operations are not part of this Q11 probe")
        self._pending = list(occurrences)
        self._stage, self._offset = stage, 0
        try:
            yield
            if self._offset != len(self._pending):
                raise ValueError("the native operator did not consume every occurrence")
        finally:
            self._pending = None

    def reply(self, messages, *, model, params):
        start = perf_counter()
        if self._pending is None:
            raise RuntimeError("a model input arrived outside an authored operator")
        output = []
        for message in messages:
            if self._offset >= len(self._pending):
                raise ValueError("the native operator emitted extra model inputs")
            key, dependencies = self._pending[self._offset]
            self._offset += 1
            if key in self.requests:
                raise ValueError("a logical request occurrence was emitted twice: " + key)
            if len(self.requests) >= self.max_requests:
                raise ValueError("the explicit CPU logical-request allowance was exceeded")
            request = dict(occurrence=key, dependencies=dependencies, model=model,
                           params=params, messages=message)
            encoded = json_bytes(request)
            digest = hashlib.sha256(encoded).hexdigest()
            if self.fixed is not None:
                response = self.fixed[key]
            else:
                keep = (self.pattern == "all_true" or
                        int(hashlib.sha256(key.encode()).hexdigest(), 16) % 3 != 0)
                response = "Answer: " + str(keep)
            self.stream.write(json_bytes(dict(request=request, sha256=digest,
                                              response=response)).decode() + "\n")
            self.requests[key] = digest
            self.responses[key] = response
            self.issue_order.append(key)
            self.stage_counts[self._stage] += 1
            self.input_sizes.append(len(encoded))
            output.append(response)
        self.audit_s += perf_counter() - start
        return output

    def assert_same_requests(self, other):
        if self.requests != other.requests or self.responses != other.responses:
            raise ValueError("per-occurrence requests, dependencies or replay responses differ")

    def report(self):
        sizes = sorted(self.input_sizes)
        distribution = (dict(min=min(sizes), median=median(sizes),
                             p95=sizes[math.ceil(0.95 * len(sizes)) - 1], max=max(sizes))
                        if sizes else None)
        return dict(logical_requests=len(self.requests), stages=dict(self.stage_counts),
                    request_json_bytes=distribution,
                    request_set_sha256=hashlib.sha256(json_bytes(self.requests)).hexdigest(),
                    issue_order_sha256=hashlib.sha256(json_bytes(self.issue_order)).hexdigest(),
                    audit_s=self.audit_s)
