"""Shared identity / attribution / integrity helpers for the SQuAD baseline gates.

Used by both the operator-only capability gate
(``code/scripts/baselines/squad_capability_gate.py``) and the database-E2E runner
(``code/scripts/baselines/squad_database_e2e_runner.py``) so that identity,
vLLM-counter attribution, and the canonical workload-integrity / content-hash
contract are computed by ONE implementation (no third copy).

Sampling (``_answer_bucket`` / ``largest_remainder_allocation`` /
``stratified_sample``) and ``_load_workload`` stay in the capability gate --
only the sampled gate needs them; the E2E runner scans the full set.
"""

from __future__ import annotations

import hashlib
import json
import socket
import subprocess
from pathlib import Path

from .redact import redact_text

# Must equal import_squad_workload.EXPECTED_DEV_COUNT and the importer's
# compute_content_hash definition. Pinned by test_content_hash_matches_importer.
EXPECTED_DEV_COUNT = 10570

# Cumulative vLLM counters checked for monotonicity (no reset) and attribution.
_ATTRIBUTION_COUNTERS = (
    "vllm:prompt_tokens_total",
    "vllm:generation_tokens_total",
    "vllm:request_success_total",
    "vllm:prefix_cache_queries_total",
    "vllm:prefix_cache_hits_total",
)


def _structured_content_hash(rows: list[dict]) -> str:
    """SHA256 over structured JSON ``{id, prompt, references}`` sorted by id.

    MUST match ``import_squad_workload.compute_content_hash`` so a gate's
    workload hash is directly comparable to the importer's archived provenance
    ``content_hash``. Pinned by test_content_hash_matches_importer.
    """

    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda r: r["source_example_id"]):
        payload = json.dumps(
            {
                "id": row["source_example_id"],
                "prompt": row["text"],
                "references": list(row["answers"]),
            },
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
        digest.update(payload)
    return digest.hexdigest()


def _validate_workload_integrity(
    rows: list[dict],
    expected_count: int,
    expected_content_hash: str,
) -> tuple[bool, list[str]]:
    """Fail-closed workload integrity checks (applied in BOTH gate modes)."""

    problems: list[str] = []
    if len(rows) != expected_count:
        problems.append(f"row_count={len(rows)} != expected {expected_count}")
    if len({r["doc_id"] for r in rows}) != len(rows):
        problems.append("duplicate doc_id present")
    source_ids = [r["source_example_id"] for r in rows]
    if len(set(source_ids)) != len(source_ids):
        problems.append("duplicate source_example_id present")
    if not all(r["source_example_id"] for r in rows):
        problems.append("empty source_example_id present")
    if not all(r["answers"] and all(s.strip() for s in r["answers"]) for r in rows):
        problems.append("empty/blank reference_answers present")
    actual_hash = _structured_content_hash(rows)
    if actual_hash != expected_content_hash:
        problems.append(
            f"workload content_hash {actual_hash} != importer provenance "
            f"{expected_content_hash}"
        )
    return (not problems), problems


def _delta(before: dict[str, float], after: dict[str, float], name: str) -> float:
    return max(0.0, after.get(name, 0.0) - before.get(name, 0.0))


def _endpoint_idle(metrics: dict[str, float]) -> tuple[bool, str]:
    if not metrics:
        return False, "scrape_empty"
    if (
        "vllm:num_requests_running" not in metrics
        or "vllm:num_requests_waiting" not in metrics
    ):
        return False, "gauge_missing"
    running = int(metrics.get("vllm:num_requests_running", 0))
    waiting = int(metrics.get("vllm:num_requests_waiting", 0))
    if running == 0 and waiting == 0:
        return True, "idle"
    return False, f"not_idle(running={running},waiting={waiting})"


def _scrape_status(metrics: dict[str, float]) -> str:
    if not metrics:
        return "empty"
    if (
        "vllm:num_requests_running" not in metrics
        or "vllm:num_requests_waiting" not in metrics
    ):
        return "gauge_missing"
    return "ok"


def _assess_attribution(
    metrics_before: dict[str, float],
    metrics_after: dict[str, float],
    requests_sent: int,
) -> tuple[dict[str, object], bool]:
    """Decide whether vLLM counter deltas are attributable to this run only."""

    reasons: list[str] = []
    before_ok, before_reason = _endpoint_idle(metrics_before)
    after_ok, after_reason = _endpoint_idle(metrics_after)
    if not before_ok:
        reasons.append(f"before:{before_reason}")
    if not after_ok:
        reasons.append(f"after:{after_reason}")
    resets = [
        name
        for name in _ATTRIBUTION_COUNTERS
        if metrics_after.get(name, 0.0) < metrics_before.get(name, 0.0)
    ]
    if resets:
        reasons.append(f"counter_reset:{resets}")
    request_success_delta = int(
        _delta(metrics_before, metrics_after, "vllm:request_success_total")
    )
    if request_success_delta != requests_sent:
        reasons.append(
            f"request_success_delta={request_success_delta} != "
            f"requests_sent={requests_sent}"
        )
    attribution_ok = not reasons
    summary = {
        "attributable": attribution_ok,
        "before_idle": before_ok,
        "after_idle": after_ok,
        "request_success_delta": request_success_delta,
        "requests_sent": requests_sent,
        "reasons": reasons,
    }
    return summary, attribution_ok


def _vllm_version(base_url: str) -> str:
    from urllib import error, request
    # vLLM serves /version at the ROOT (http://host:port/version), not under
    # /v1. Remove the full suffix; removing only ``v1`` leaves a trailing slash
    # and produces ``//version``, which some servers do not normalize.
    root = base_url.removesuffix("/v1").rstrip("/")
    for candidate in (f"{root}/version", f"{base_url}/version"):
        try:
            with request.urlopen(candidate, timeout=3.0) as response:
                body = json.loads(response.read().decode("utf-8", errors="replace"))
            return str(body.get("version", "unknown"))
        except (OSError, error.URLError, ValueError):
            continue
    return "unavailable"


def _gpu_identity() -> dict[str, object]:
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,name,memory.total,driver_version",
             "--format=csv,noheader"],
            capture_output=True, text=True, timeout=5.0,
        )
        if out.returncode == 0 and out.stdout.strip():
            lines = [line.strip() for line in out.stdout.splitlines() if line.strip()]
            return {"nvidia_smi": lines, "hostname": socket.gethostname()}
    except (OSError, subprocess.SubprocessError):
        pass
    return {"nvidia_smi": "unavailable", "hostname": socket.gethostname()}


def _pg_server_identity(database_url: str) -> dict[str, str]:
    import psycopg
    identity: dict[str, str] = {}
    try:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                identity["pg_server_version"] = str(cur.fetchone()[0])
                # pgvector is optional; the extension ships under the name
                # ``vector`` (not ``pgvector``). Recorded as pgvector_version so
                # per-row evidence satisfies AGENTS.md §5.
                cur.execute(
                    "SELECT extversion FROM pg_extension WHERE extname = 'vector'"
                )
                row = cur.fetchone()
                identity["pgvector_version"] = str(row[0]) if row else "not_installed"
    except Exception as exc:
        identity["pg_error"] = redact_text(str(exc)[:120])
    return identity


def _git_commit(cwd: Path) -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=cwd,
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    except Exception:
        return "unknown"


def _endpoint_base_url(endpoint_url: str) -> str:
    suffix = "/chat/completions"
    if not endpoint_url.endswith(suffix):
        raise SystemExit("endpoint-url must end with /v1/chat/completions")
    return endpoint_url[: -len(suffix)]


def _load_importer_provenance(path: str) -> dict:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if "content_hash" not in data:
        raise SystemExit(
            f"importer provenance {path} missing content_hash; "
            "point --importer-provenance at the SQuAD importer sidecar JSON"
        )
    return data
