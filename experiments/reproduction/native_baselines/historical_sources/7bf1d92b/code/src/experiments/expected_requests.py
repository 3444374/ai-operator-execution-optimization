"""Check a bounded multiset of complete model request values before HTTP dispatch."""

from collections import Counter
import json
from pathlib import Path
import re
import threading

from src.baselines.common.private_artifacts import content_digest
from src.experiments.attempt_ledger import BudgetError

SCHEMA = "semloom.expected_model_requests.v1"


def expected_request_manifest(bodies):
    counts = Counter(content_digest(body) for body in bodies)
    return {"schema": SCHEMA, "counts": dict(counts)}


class ExpectedRequests:
    def __init__(self, manifest, *, available_attempts):
        if manifest.get("schema") != SCHEMA or not isinstance(manifest.get("counts"), dict):
            raise ValueError("invalid expected-request manifest")
        counts = manifest["counts"]
        if not counts or any(
            not isinstance(key, str) or not re.fullmatch(r"[0-9a-f]{64}", key)
            or type(value) is not int or value < 1 for key, value in counts.items()
        ):
            raise ValueError("invalid expected-request counts")
        if sum(counts.values()) > available_attempts:
            raise ValueError("expected requests exceed remaining durable budget")
        self._remaining = Counter(counts)
        self._lock = threading.Lock()

    @classmethod
    def load(cls, path: Path, *, available_attempts):
        if path.stat().st_size > 16 * 1024 * 1024:
            raise ValueError("expected-request manifest too large")
        return cls(json.loads(path.read_text()), available_attempts=available_attempts)

    def accept(self, body):
        key = content_digest(body)
        with self._lock:
            if self._remaining[key] == 0:
                raise BudgetError("request values or multiplicity differ from prepared workload")
            self._remaining[key] -= 1

    @property
    def remaining(self):
        with self._lock:
            return sum(self._remaining.values())
