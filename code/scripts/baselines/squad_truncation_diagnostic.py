#!/usr/bin/env python3
"""Targeted truncation diagnostic for a single archived SQuAD row.

For one ``source_example_id``, replay its EXACT archived prompt against both:

  * **direct vLLM** ``/v1/chat/completions`` -- exposes ``finish_reason``,
    ``completion_tokens`` and the (possibly truncated) text; and
  * **DuckDB ``ai_try_complete``** -- exposes the ``{response, error}`` struct
    that the capability gate sees (DuckDB-ai's truncation-as-error product
    semantic).

at the gate cap (64) and higher caps (128, then 256 only if 128 still
truncates), ``--repeats`` times each, with the DuckDB response cache OFF and
retry OFF. The DuckDB request body is also captured via
``ai_completion_request_json`` so the two paths are provably the same request.

This is a DIAGNOSTIC ONLY: higher caps never feed back into the formal gate
(locked at cap=64). Decision rule:
  * direct cap=64 stable ``finish_reason=length`` + higher cap ``stop`` =>
    model output is genuinely overlong (a stable, not occasional, property);
  * DuckDB cap=64 mapping the same case to NULL/error => confirms DuckDB's
    product-semantic difference vs the direct client;
  * if cap=64 does NOT reproduce stably across repeats, record "occasional
    generation tail risk", NOT "deterministic".
"""

from __future__ import annotations

import argparse
import hashlib
import json
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


def _load_prompt(database_url: str, workload: str, source_example_id: str) -> str:
    import psycopg
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT text FROM documents WHERE workload_name=%s "
                "AND source_example_id=%s",
                (workload, source_example_id),
            )
            row = cur.fetchone()
    if not row:
        raise SystemExit(f"no row for source_example_id={source_example_id!r}")
    return str(row[0])


def _direct_vllm(endpoint_url: str, model: str, api_key: str, prompt: str,
                 cap: int) -> dict:
    import httpx
    body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": cap,
        "stream": False,
    }
    started = time.time()
    try:
        resp = httpx.post(
            endpoint_url, json=body, headers={"Authorization": f"Bearer {api_key}"},
            timeout=180.0,
        )
        elapsed = time.time() - started
        if resp.status_code != 200:
            return {"http_status": resp.status_code, "elapsed_s": round(elapsed, 3),
                    "error": resp.text[:400]}
        data = resp.json()
        choice = (data.get("choices") or [{}])[0]
        usage = data.get("usage") or {}
        return {
            "http_status": 200, "elapsed_s": round(elapsed, 3),
            "finish_reason": choice.get("finish_reason"),
            "completion_tokens": usage.get("completion_tokens"),
            "prompt_tokens": usage.get("prompt_tokens"),
            "text": choice.get("message", {}).get("content"),
            "request_body": body,
        }
    except Exception as exc:  # noqa: BLE001 - record any failure mode
        return {"http_status": None, "elapsed_s": round(time.time() - started, 3),
                "error": f"{type(exc).__name__}: {exc}"[:400]}


def _duckdb_once(endpoint_base_url: str, model: str, api_key: str, prompt: str,
                 cap: int) -> dict:
    import duckdb
    conn = duckdb.connect()
    try:
        conn.execute("LOAD ai")
        conn.execute("SET duckdb_ai_provider = 'openai_compatible'")
        conn.execute(f"SET duckdb_ai_model = '{model}'")
        conn.execute("SET duckdb_ai_max_concurrent_requests = 1")
        conn.execute("SET duckdb_ai_cache = false")
        conn.execute("SET duckdb_ai_prompt_cache = false")
        conn.execute("SET duckdb_ai_retry_count = 0")
        conn.execute("SET duckdb_ai_timeout_seconds = 180")
        conn.execute(
            "CREATE SECRET duckdb_ai_endpoint "
            f"(TYPE duckdb_ai, AI_PROVIDER 'openai_compatible', "
            f"BASE_URL '{endpoint_base_url}', API_KEY '{api_key}')"
        )
        # Capture the exact request body the extension would send.
        req_body = conn.execute(
            "SELECT ai_completion_request_json(?, max_tokens => ?, temperature => 0.0)",
            [prompt, cap],
        ).fetchone()[0]
        started = time.time()
        row = conn.execute(
            "SELECT ai_try_complete(?, max_tokens => ?, temperature => 0.0)",
            [prompt, cap],
        ).fetchone()
        elapsed = time.time() - started
        result = row[0]
        # ai_try_complete returns a struct {response, error}.
        response = None
        error = None
        try:
            response = result.response
            error = result.error
        except AttributeError:
            response, error = (result.get("response"), result.get("error")) if isinstance(result, dict) else (None, None)
        return {
            "elapsed_s": round(elapsed, 3),
            "response": str(response) if response is not None else None,
            "error": str(error) if error is not None else None,
            "response_chars": len(str(response)) if response is not None else 0,
            "request_body_json": req_body,
        }
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"[:500]}
    finally:
        conn.close()


def _parse(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source-example-id", required=True)
    p.add_argument("--database-url", required=True)
    p.add_argument("--workload-name", default="squad_v11_dev_short_answer")
    p.add_argument("--endpoint-url", required=True,
                   help="direct: .../v1/chat/completions")
    p.add_argument("--endpoint-base-url", required=True,
                   help="DuckDB BASE_URL, e.g. http://127.0.0.1:8000/v1")
    p.add_argument("--model", required=True)
    p.add_argument("--api-key", default="EMPTY")
    p.add_argument("--caps", default="64,128,256",
                   help="comma-sep caps to probe (256 used only if 128 truncates)")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--output", required=True)
    p.add_argument("--force", action="store_true")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = _parse(sys.argv[1:] if argv is None else argv)
    out = Path(args.output)
    if out.exists() and not args.force:
        raise SystemExit(f"output {out} exists; pass --force")
    out.parent.mkdir(parents=True, exist_ok=True)

    prompt = _load_prompt(args.database_url, args.workload_name, args.source_example_id)
    caps = [int(c) for c in args.caps.split(",") if c.strip()]
    prompt_sha = hashlib.sha256(prompt.encode("utf-8")).hexdigest()

    direct: dict[int, list[dict]] = {}
    duckdb_runs: dict[int, list[dict]] = {}
    duckdb_req_body: dict[int, str] = {}

    for cap in caps:
        direct[cap] = [_direct_vllm(args.endpoint_url, args.model, args.api_key, prompt, cap)
                       for _ in range(args.repeats)]
        # Exactly `repeats` DuckDB calls per cap (a previous version did an
        # extra 1 call just to capture the request body; now capture it from
        # the first repeat).
        duckdb_runs[cap] = [_duckdb_once(args.endpoint_base_url, args.model, args.api_key, prompt, cap)
                            for _ in range(args.repeats)]
        duckdb_req_body[cap] = (
            duckdb_runs[cap][0].get("request_body_json") if duckdb_runs[cap] else None
        )

    # Decision summary. Guard every all() against the all-HTTP-failed case
    # (an empty filtered list would make all() vacuously True); require >=1
    # HTTP-200 direct call before claiming a stable finish_reason.
    def _direct_ok(direct_runs: list[dict], want: str) -> bool:
        ok_runs = [r for r in direct_runs if r.get("http_status") == 200]
        return bool(ok_runs) and all(r.get("finish_reason") == want for r in ok_runs)

    d64 = direct.get(64, [])
    d64_ok = [r for r in d64 if r.get("http_status") == 200]
    stable_length_at_64 = bool(d64_ok) and all(
        r.get("finish_reason") == "length" for r in d64_ok
    )
    higher_stop = False
    for cap in caps:
        if cap <= 64:
            continue
        if _direct_ok(direct.get(cap, []), "stop"):
            higher_stop = True
            break
    duckdb_64_null = bool(duckdb_runs.get(64)) and duckdb_runs[64] and all(
        (r.get("response") is None) for r in duckdb_runs[64]
    )

    report = {
        "source_example_id": args.source_example_id,
        "prompt_sha256": prompt_sha,
        "prompt_len": len(prompt),
        "model": args.model,
        "temperature": 0.0,
        "repeats": args.repeats,
        "caps": caps,
        "direct_vllm": direct,
        "duckdb_ai_try_complete": duckdb_runs,
        "duckdb_request_body_per_cap": duckdb_req_body,
        "decision": {
            "direct_http_200_at_cap64": len(d64_ok),
            "direct_http_all_failed_at_cap64": len(d64_ok) == 0,
            "stable_length_at_cap64": stable_length_at_64,
            "higher_cap_stop": higher_stop,
            "duckdb_cap64_null": duckdb_64_null,
            "interpretation": (
                "model output genuinely overlong (stable) + DuckDB maps to NULL"
                if (stable_length_at_64 and higher_stop and duckdb_64_null)
                else "no successful direct call at cap=64 -- cannot decide stability"
                if not d64_ok
                else "see per-run finish_reasons; cap=64 not stably length -> occasional tail risk"
                if not stable_length_at_64
                else "partial -- inspect runs"
            ),
        },
        "note": ("High caps are DIAGNOSTIC ONLY; the formal gate is locked at "
                 "cap=64. Response cache and retries are OFF."),
    }
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report["decision"], indent=2, ensure_ascii=False))
    # Concise per-cap direct finish_reasons for the log.
    for cap in caps:
        frs = [r.get("finish_reason") for r in direct.get(cap, [])]
        cts = [r.get("completion_tokens") for r in direct.get(cap, [])]
        print(f"  direct cap={cap}: finish_reasons={frs} completion_tokens={cts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
