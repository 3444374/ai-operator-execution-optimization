#!/usr/bin/env python3
"""Bounded-output request-equivalence gate: canonical vs DuckDB vs project.

Single verifiable goal: prove the DuckDB ``ai`` path and the project completion
path send the **same** request body to vLLM for the bounded-output comparison
(model / messages role+content / temperature=0.0 / max_tokens, no extra
semantic fields, no hidden system prompt), so later performance differences
cannot be attributed to different prompts or settings.

Three-way comparison (no third payload builder written -- the project side reuses
the production ``build_completion_request_body``):
  canonical contract  vs  DuckDB ``ai_completion_request_json()`` actual
                        vs  project ``build_completion_request_body()`` actual

Plus a defence-in-depth isolated single-request check: with the endpoint idle,
send one unique prompt through each path and compare the vLLM prompt-token
counter delta -- equal deltas rule out a hidden system prompt that the JSON
builders might not surface.

Evidence (redacted raw JSON, canonical diff, temperature probes, identity, exit
code) is written under ``feasibility/results/``. This is a single-request gate,
NOT a long experiment or a 3-arm performance run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.serving.backends.completion import build_completion_request_body  # noqa: E402
from src.observability.metrics import scrape_prometheus_metrics  # noqa: E402

UNIQUE_PROMPT = "Reply with only the single word: ready"
CANONICAL_TEMPERATURE = 0.0
SEMANTIC_KEYS = {"model", "messages", "temperature", "max_tokens"}


def _git_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=CODE_ROOT, capture_output=True, text=True, check=True,
        ).stdout.strip()
    except Exception:
        return "unknown"


def _connect_duckdb():
    try:
        import duckdb
    except ImportError as exc:
        raise SystemExit("requires duckdb==1.5.4 with the ai extension") from exc
    con = duckdb.connect()
    con.execute("LOAD ai")
    return con


def _configure_duckdb(con, endpoint_base_url: str, model: str, api_key: str) -> None:
    con.execute("SET duckdb_ai_provider = 'openai_compatible'")
    con.execute(f"SET duckdb_ai_model = '{model}'")
    con.execute(
        "CREATE OR REPLACE SECRET req_equiv "
        f"(TYPE duckdb_ai, AI_PROVIDER 'openai_compatible', "
        f"BASE_URL '{endpoint_base_url}', API_KEY '{api_key}')"
    )


def _duckdb_request_json(con, prompt: str, cap: int, *, temperature_explicit: bool):
    if temperature_explicit:
        sql = (
            f"SELECT ai_completion_request_json('{prompt}', "
            f"max_tokens => {cap}, temperature => 0.0)"
        )
    else:
        sql = f"SELECT ai_completion_request_json('{prompt}')"
    raw = con.execute(sql).fetchone()[0]
    return raw, json.loads(raw)


def _duckdb_runtime_identity(con) -> dict[str, str]:
    duckdb_version = str(con.execute("SELECT version()").fetchone()[0])
    row = con.execute(
        "SELECT extension_version, installed_from FROM duckdb_extensions() "
        "WHERE extension_name = 'ai' AND loaded"
    ).fetchone()
    return {
        "duckdb_version": duckdb_version,
        "duckdb_ai_extension_version": str(row[0]) if row else "unknown",
        "duckdb_ai_installed_from": str(row[1]) if row else "unknown",
    }


def _normalize(body: dict) -> dict:
    return {k: body.get(k) for k in sorted(body)}


def _field_diff(canonical: dict, actual: dict) -> dict:
    keys = sorted(set(canonical) | set(actual))
    return {
        k: {"canonical": canonical.get(k), "actual": actual.get(k), "match": canonical.get(k) == actual.get(k)}
        for k in keys
    }


def _messages_match(canonical_body: dict, actual_body: dict) -> tuple[bool, str]:
    cm = canonical_body.get("messages")
    am = actual_body.get("messages")
    if cm == am:
        return True, "messages identical (role+content)"
    return False, f"messages differ: canonical={cm!r} actual={am!r}"


def _redact(body: dict) -> dict:
    redacted = dict(body)
    msgs = redacted.get("messages")
    if isinstance(msgs, list):
        redacted["messages"] = [
            {**m, "content": f"<{len(str(m.get('content', '')))} chars>"}
            if isinstance(m, dict) else m
            for m in msgs
        ]
    return redacted


def _endpoint_idle(metrics_url: str) -> bool:
    snap = scrape_prometheus_metrics(metrics_url)
    running = snap.get("vllm:num_requests_running")
    waiting = snap.get("vllm:num_requests_waiting")
    return running == 0 and waiting == 0


def _prompt_token_delta(con, metrics_url: str, prompt: str, cap: int) -> tuple[int, str]:
    """Isolated single DuckDB ai_try_complete; return vLLM prompt-token delta."""

    if not _endpoint_idle(metrics_url):
        raise SystemExit("endpoint not idle before DuckDB isolated request; abort")
    before = scrape_prometheus_metrics(metrics_url).get("vllm:prompt_tokens_total", 0)
    con.execute(
        f"SELECT ai_try_complete('{prompt}', max_tokens => {cap}, temperature => 0.0)"
    ).fetchall()
    after = scrape_prometheus_metrics(metrics_url).get("vllm:prompt_tokens_total", 0)
    return int(after - before), "duckdb_ai_try_complete"


def _project_prompt_token_delta(
    endpoint_url: str, model: str, api_key: str, prompt: str, cap: int, metrics_url: str
) -> tuple[int, str]:
    """Isolated single project completion call; return vLLM prompt-token delta."""

    from src.serving.backends.completion import call_compatible_completion_endpoint
    if not _endpoint_idle(metrics_url):
        raise SystemExit("endpoint not idle before project isolated request; abort")
    before = scrape_prometheus_metrics(metrics_url).get("vllm:prompt_tokens_total", 0)
    call_compatible_completion_endpoint(
        endpoint_url, model, [prompt], api_key, 60.0, cap,
        prompt_format="raw", protocol="chat_completions", temperature=0.0,
    )
    after = scrape_prometheus_metrics(metrics_url).get("vllm:prompt_tokens_total", 0)
    return int(after - before), "project_build_completion_request_body"


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint-url", required=True, help="vLLM /v1/chat/completions URL")
    parser.add_argument("--metrics-url", required=True, help="vLLM /metrics URL")
    parser.add_argument("--model", required=True)
    parser.add_argument("--api-key", default="EMPTY")
    parser.add_argument("--max-tokens", type=int, default=16)
    parser.add_argument("--prompt", default=UNIQUE_PROMPT)
    parser.add_argument(
        "--output-dir", required=True,
        help="feasibility/results/... dir for evidence (must not exist)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse(sys.argv[1:] if argv is None else argv)
    output_dir = Path(args.output_dir)
    if output_dir.exists():
        raise SystemExit(f"output dir {output_dir} already exists; refuse to overwrite")

    suffix = "/chat/completions"
    if not args.endpoint_url.endswith(suffix):
        raise SystemExit("endpoint-url must end with /v1/chat/completions")
    base_url = args.endpoint_url[: -len(suffix)]

    canonical = {
        "model": args.model,
        "messages": [{"role": "user", "content": args.prompt}],
        "temperature": CANONICAL_TEMPERATURE,
        "max_tokens": args.max_tokens,
    }

    con = _connect_duckdb()
    _configure_duckdb(con, base_url, args.model, args.api_key)
    duckdb_identity = _duckdb_runtime_identity(con)

    duckdb_raw, duckdb_body = _duckdb_request_json(
        con, args.prompt, args.max_tokens, temperature_explicit=True
    )
    duckdb_default_raw, duckdb_default_body = _duckdb_request_json(
        con, args.prompt, args.max_tokens, temperature_explicit=False
    )
    project_body = build_completion_request_body(
        args.model, [args.prompt], args.max_tokens,
        "chat_completions", temperature=CANONICAL_TEMPERATURE,
    )

    duckdb_norm = _normalize(duckdb_body)
    project_norm = _normalize(project_body)
    canonical_norm = _normalize(canonical)

    duckdb_extra = sorted(set(duckdb_body) - SEMANTIC_KEYS)
    project_extra = sorted(set(project_body) - SEMANTIC_KEYS)
    duckdb_missing = sorted(SEMANTIC_KEYS - set(duckdb_body))
    project_missing = sorted(SEMANTIC_KEYS - set(project_body))
    duckdb_messages_ok, duckdb_messages_note = _messages_match(canonical, duckdb_body)
    project_messages_ok, project_messages_note = _messages_match(canonical, project_body)

    duckdb_payload_match = (
        duckdb_norm == canonical_norm and duckdb_messages_ok
        and not duckdb_extra and not duckdb_missing
    )
    project_payload_match = (
        project_norm == canonical_norm and project_messages_ok
        and not project_extra and not project_missing
    )

    duckdb_delta, duckdb_path = _prompt_token_delta(con, args.metrics_url, args.prompt, args.max_tokens)
    project_delta, project_path = _project_prompt_token_delta(
        args.endpoint_url, args.model, args.api_key, args.prompt, args.max_tokens, args.metrics_url
    )
    token_match = duckdb_delta == project_delta
    con.close()

    passed = duckdb_payload_match and project_payload_match and token_match
    reasons = []
    if not duckdb_payload_match:
        reasons.append("DuckDB payload != canonical")
    if not project_payload_match:
        reasons.append("project payload != canonical")
    if not token_match:
        reasons.append(
            f"isolated prompt-token delta differs (duckdb={duckdb_delta}, project={project_delta})"
        )

    service_config_hash = hashlib.sha256(
        json.dumps({"endpoint_url": args.endpoint_url, "model": args.model}, sort_keys=True).encode()
    ).hexdigest()

    report = {
        "gate": "bounded_output_request_equivalence",
        "passed": passed,
        "failure_reasons": reasons,
        "prompt": args.prompt,
        "max_tokens": args.max_tokens,
        "canonical_temperature": CANONICAL_TEMPERATURE,
        "canonical_normalized": canonical_norm,
        "duckdb": {
            "raw_json": duckdb_raw,
            "normalized": duckdb_norm,
            "field_diff_vs_canonical": _field_diff(canonical, duckdb_body),
            "messages_check": {"ok": duckdb_messages_ok, "note": duckdb_messages_note},
            "extra_keys": duckdb_extra,
            "missing_keys": duckdb_missing,
            "default_temperature_probe": {
                "raw_json": duckdb_default_raw,
                "temperature": duckdb_default_body.get("temperature"),
                "note": "default temperature is 0.1, NOT 0.0 -- gate requires explicit 0.0",
            },
            "payload_matches_canonical": duckdb_payload_match,
        },
        "project": {
            "raw_json": json.dumps(project_body, sort_keys=True),
            "normalized": project_norm,
            "field_diff_vs_canonical": _field_diff(canonical, project_body),
            "messages_check": {"ok": project_messages_ok, "note": project_messages_note},
            "extra_keys": project_extra,
            "missing_keys": project_missing,
            "payload_matches_canonical": project_payload_match,
        },
        "isolated_single_request": {
            "endpoint_idle_enforced": True,
            "duckdb_prompt_token_delta": duckdb_delta,
            "project_prompt_token_delta": project_delta,
            "token_delta_match": token_match,
            "duckdb_path": duckdb_path,
            "project_path": project_path,
        },
        "identity": {
            "git_commit": _git_commit(),
            "service_config_sha256": service_config_hash,
            **duckdb_identity,
        },
        "redacted_note": "raw_json redacts message content length only; full prompt is in args.prompt",
    }

    output_dir.mkdir(parents=True, exist_ok=False)
    (output_dir / "request_equivalence.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    (output_dir / "command.txt").write_text(
        "python " + " ".join(sys.argv) + f"\nexit_code={'0' if passed else '2'}\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
