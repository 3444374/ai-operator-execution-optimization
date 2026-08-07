#!/usr/bin/env python3
"""Multicard scale-ramp driver: 2048 -> 4096 -> 8192 -> 10570 at fixed c=32/K=32.

WHY
---
The 2048-row screening (operator wall ~5s, 16-22 GPU samples) is too short to
show steady-state. This driver ramps the WORKLOAD scale at the already-calibrated
saturation operating point (c=32/K=32 per endpoint), holding concurrency fixed so
scale is the only moving variable (per the calibration rule: never move two
dimensions at once). It observes whether tokens/s / rows/s plateau, whether GPU
util / running stay stable, whether TTFT P95/P99 degrade with scale, and whether
the three-arm throughput ordering stays stable across scales.

ARMS
----
- bounded_http / duckdb_ai (2-endpoint, sharded gate cells): run via run_core_gate
  (single cell per invocation), WRAPPED in cell_instrumentation.instrumented_cell
  so each gate cell gets a TTFT histogram delta (vllm_metric_delta_stats, before
  the cell idle -> after idle) + a per-cell gpu_resource.csv recording gpu0 AND
  gpu1. The shared gate_runner itself is left untouched.
- project_static (2-endpoint, via endpoint_urls): run via run_project_static; the
  profiler already emits TTFT + a resource CSV natively, so no wrapper needed.

OUTPUT LAYOUT (per scale, per arm, per repeat)
----------------------------------------------
<output_root>/scale_<S>/<arm>_c<K>_rep<R>/
    gate_output/...                  # run_core_gate cell evidence (gate arms)
    ttft_metrics.json                # gate arms only (from instrumented_cell)
    gpu_resource.csv                 # gate arms only (from instrumented_cell)
    project.csv + project_evidence.csv + project_resource.csv + project_trace.csv  # project

The aggregator is then run once per scale_<S>/ to produce a per-scale report; the
ramp README combines the per-scale summaries into the plateau/ordering narrative.

This driver does NOT add project scheduling logic; it only orchestrates the
existing, separately-tested runners + the instrumented_cell wrapper. It is
config-driven (deploy/autodl/multicard_scale_ramp.example.json) so the run is
reproducible, not an ad-hoc server script.
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.cell_instrumentation import (  # noqa: E402
    instrumented_cell,
)
from src.baselines.common.manifests import read_manifest  # noqa: E402
from src.baselines.text.orchestration.gate_runner import (  # noqa: E402
    CoreGateCell,
    CoreGateConfig,
    run_core_gate,
)
from src.infrastructure.config_env import expand_structure  # noqa: E402

GATE_ARMS = ("bounded_http", "duckdb_ai")


@dataclass(frozen=True)
class RampArm:
    arm: str
    concurrency: int  # c for gate arms, K for project
    reps: int


@dataclass(frozen=True)
class RampScale:
    rows: int
    manifest: Path


@dataclass(frozen=True)
class RampConfig:
    experiment_id: str
    endpoint_urls: tuple[str, ...]
    completion_protocol: str
    model: str
    tokenizer: str | None
    service_prefix_caching: str
    service_max_num_seqs: int
    service_max_num_batched_tokens: int
    scales: tuple[RampScale, ...]
    arms: tuple[RampArm, ...]
    output_root: Path
    driver_python: str
    vllm_python: str
    database_url: str  # project_static only
    workload_name: str  # project_static only
    max_tokens: int  # project_static only (cap)
    project_token_budget: int
    project_active_work: int
    project_actor_workers: int
    project_ray_concurrency: int
    lb_endpoint_url: str = ""  # nginx LB URL for the lb_rr arm (1 DuckDB -> LB -> 2 backends)
    # Cache control (deploy §9.1 / AGENTS §7.5 point #4): run a uniform cache-hot
    # warmup before each measured cell so arm/scale/order don't contaminate via the
    # shared prefix cache. Warmup = bounded_http on the cell's manifest at
    # warmup_concurrency (both endpoints) -- prefix cache is prompt-keyed, so this
    # warms every arm to the same state regardless of which adapter is measured next.
    warmup_per_cell: bool = False
    warmup_concurrency: int = 32
    idle_timeout_s: float = 120.0
    # strict vLLM config preflight: fail-closed (raise) when a declared service
    # param is absent from the vLLM cmdline, instead of WARN. 复审: default WARN
    # lets screening runs proceed on vLLM defaults; formal runs set true to force
    # declared == effective.
    vllm_config_strict: bool = False


def _load_ramp_config(path: str | Path, *, driver_python: str, vllm_python: str) -> RampConfig:
    payload = expand_structure(json.loads(Path(path).read_text(encoding="utf-8")), "ramp_config")
    if not isinstance(payload, dict):
        raise ValueError("ramp config must be an object")
    scales = tuple(
        RampScale(rows=int(s["rows"]), manifest=Path(s["manifest"]))
        for s in payload["scales"]
    )
    for s in scales:
        if not s.manifest.is_file():
            raise ValueError(f"scale {s.rows} manifest missing: {s.manifest}")
        manifest_rows = len(read_manifest(s.manifest))
        if manifest_rows != s.rows:
            raise ValueError(
                f"scale {s.rows} manifest has {manifest_rows} rows (mismatch)"
            )
    arms = tuple(
        RampArm(arm=str(a["arm"]), concurrency=int(a["concurrency"]), reps=int(a.get("reps", 1)))
        for a in payload["arms"]
    )
    for a in arms:
        if a.arm not in GATE_ARMS and a.arm not in ("project_static", "lb_rr"):
            raise ValueError(f"unsupported arm {a.arm!r}; expected {GATE_ARMS}, project_static, or lb_rr")
        if a.concurrency <= 0 or a.reps <= 0:
            raise ValueError(f"arm {a.arm} concurrency/reps must be positive")
    return RampConfig(
        experiment_id=str(payload.get("experiment_id", "multicard_scale_ramp")),
        endpoint_urls=tuple(payload["endpoint_urls"]),
        completion_protocol=str(payload.get("completion_protocol", "chat_completions")),
        model=str(payload["model"]),
        tokenizer=payload.get("tokenizer"),
        service_prefix_caching=str(payload.get("service_prefix_caching", "enabled")),
        service_max_num_seqs=int(payload.get("service_max_num_seqs", -1)),
        service_max_num_batched_tokens=int(payload.get("service_max_num_batched_tokens", -1)),
        scales=scales,
        arms=arms,
        output_root=Path(payload["output_root"]),
        driver_python=driver_python,
        vllm_python=vllm_python,
        database_url=str(payload["database_url"]),
        workload_name=str(payload["workload_name"]),
        max_tokens=int(payload["max_tokens"]),
        project_token_budget=int(payload["project_token_budget"]),
        project_active_work=int(payload["project_active_work"]),
        project_actor_workers=int(payload["project_actor_workers"]),
        project_ray_concurrency=int(payload["project_ray_concurrency"]),
        lb_endpoint_url=str(payload.get("lb_endpoint_url", "")),
        warmup_per_cell=bool(payload.get("warmup_per_cell", False)),
        warmup_concurrency=int(payload.get("warmup_concurrency", 32)),
        idle_timeout_s=float(payload.get("idle_timeout_s", 120.0)),
        vllm_config_strict=bool(payload.get("vllm_config_strict", False)),
    )


def _gate_config_for_cell(
    ramp: RampConfig, scale: RampScale, arm: RampArm, cell_output: Path
) -> tuple[Path, CoreGateConfig]:
    """Build an in-memory single-cell gate config + its resolved CoreGateConfig."""
    cell_id = f"{arm.arm}_c{arm.concurrency}"
    cfg_dict = {
        "schema_version": 1,
        "experiment_id": f"{ramp.experiment_id}_{arm.arm}_{scale.rows}r",
        "formal": False,
        "rows_total": scale.rows,
        "endpoint_urls": list(ramp.endpoint_urls),
        "model": ramp.model,
        "tokenizer": ramp.tokenizer,
        "completion_protocol": ramp.completion_protocol,
        "temperature": 0.0,
        "service": {
            "prefix_caching": ramp.service_prefix_caching,
            "max_num_seqs": ramp.service_max_num_seqs,
            "max_num_batched_tokens": ramp.service_max_num_batched_tokens,
        },
        "manifest": str(scale.manifest),
        "output_root": str(cell_output / "gate_output"),
        "cells": [
            {"id": cell_id, "adapter": arm.arm, "concurrency_per_endpoint": arm.concurrency}
        ],
        "hard_gates": {
            "provenance_fields_present": True,
            "native_arms_have_no_project_scheduler": True,
            "exactly_once": True,
            "failed_rows": 0,
            "worker_failures": 0,
            "both_endpoints_used": True,
            "service_counter_consistency": True,
            "endpoint_predicted_work_skew_max": 0.02,
            "vllm_running_final": 0,
            "vllm_waiting_final": 0,
            "same_model": True,
            "same_protocol": True,
            "same_service_config": True,
        },
    }
    cfg_path = cell_output / "gate_config.json"
    cfg_path.write_text(json.dumps(cfg_dict, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    # Reuse the gate runner's own loader to get a validated CoreGateConfig
    from src.baselines.text.orchestration.gate_runner import load_core_gate_config
    core = load_core_gate_config(
        cfg_path,
        output_root_override=str(cell_output / "gate_output"),
    )
    return cfg_path, core


def _metrics_urls(endpoint_urls: Sequence[str]) -> tuple[str, ...]:
    return tuple(u.split("/v1/", maxsplit=1)[0] + "/metrics" for u in endpoint_urls)


def _write_identity(arm: str, cell_output: Path) -> None:
    """Write a ramp-layer identity sidecar (复审 #1: STANDARD 主字段 = 系统角色).

    ``comparison_role`` (the STANDARD primary field that every consumer reads)
    = the AUTHORITATIVE role of the composed system under test at the ramp layer
    -- NOT the single-shard component. 复审第四轮 #1: previously comparison_role
    held the component role (database_product_native_baseline) with the system
    role only in a side ``system_comparison_role`` field, so any general consumer
    reading the standard field still misjudged gateway/harness as product-native.
    Now comparison_role IS the system role; the single-shard component role moves
    to ``component_comparison_role``. scheduler_owner lists ALL scheduling parties;
    formal_baseline_eligible=false. See experiment_report_honesty_checklist.md §2.
    """
    if arm == "bounded_http":
        ident = {"comparison_role": "direct_client_control",
                 "component_comparison_role": "direct_client_control",
                 "formal_baseline_eligible": False, "formal_control_eligible": True,
                 "scheduler_owner": "project_asyncio_control"}
    elif arm == "duckdb_ai":
        ident = {"comparison_role": "harness_pre_split_diagnostic",  # system role (PRIMARY, 复审 #1)
                 "component_comparison_role": "database_product_native_baseline",  # single-shard component
                 "formal_baseline_eligible": False,
                 "scheduler_owner": "experiment_harness + duckdb_ai_extension + vllm",
                 "reason": "harness pre-split manifest + 2 independent DuckDB processes; DuckDB ai single BASE_URL; protocol §2.6 -> not product-native multi-endpoint"}
    elif arm == "lb_rr":
        ident = {"comparison_role": "gateway_system_diagnostic",  # system role (PRIMARY), protocol §2.6 gateway
                 "component_comparison_role": "database_product_native_baseline",  # single-shard component
                 "formal_baseline_eligible": False,
                 "scheduler_owner": "duckdb_ai_extension + nginx_round_robin + vllm",
                 "reason": "single DuckDB process (single BASE_URL) via nginx third-party gateway to 2 vLLM endpoints; protocol §2.6 gateway 完整系统轨 (line 112) -> system-level only, not DuckDB-native, not formal-eligible"}
    elif arm == "project_static":
        ident = {"comparison_role": "project_scheduled_method",
                 "component_comparison_role": "project_scheduled_method",
                 "formal_baseline_eligible": False,
                 "scheduler_owner": "project_scheduler",
                 "reason": "project Ray-actor multi-endpoint scheduled method; not a product baseline"}
    else:
        return
    (cell_output / "identity.json").write_text(
        json.dumps(ident, indent=2, ensure_ascii=False), encoding="utf-8")


def _run_gate_cell(ramp: RampConfig, scale: RampScale, arm: RampArm, rep: int) -> dict:
    cell_output = ramp.output_root / f"scale_{scale.rows}" / f"{arm.arm}_c{arm.concurrency}_rep{rep}"
    if cell_output.exists():
        raise FileExistsError(f"cell output already exists: {cell_output}")
    cell_output.mkdir(parents=True)
    _write_identity(arm.arm, cell_output)
    cfg_path, _core = _gate_config_for_cell(ramp, scale, arm, cell_output)
    metrics_urls = _metrics_urls(ramp.endpoint_urls)
    record = {"arm": arm.arm, "scale": scale.rows, "concurrency": arm.concurrency, "rep": rep,
              "cell": str(cell_output), "kind": "gate"}
    try:
        with instrumented_cell(
            metrics_urls,
            cell_output / "gpu_resource.csv",
            interval_s=0.3,
        ) as instr:
            run_core_gate(
                cfg_path,
                driver_python=ramp.driver_python,
                vllm_python=ramp.vllm_python,
                output_root_override=str(cell_output / "gate_output"),
                idle_timeout_s=ramp.idle_timeout_s,
            )
    except Exception as exc:
        record["status"] = "failed"
        record["error"] = f"{type(exc).__name__}: {exc}"
        record["traceback"] = traceback.format_exc()
        (cell_output / "run_error.json").write_text(
            json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        return record
    ttft_path = cell_output / "ttft_metrics.json"
    if instr.ttft_deltas is not None:
        ttft_path.write_text(
            json.dumps(
                {str(ep): deltas for ep, deltas in instr.ttft_deltas.items()},
                indent=2, ensure_ascii=False, sort_keys=True,
            ),
            encoding="utf-8",
        )
    record["status"] = "passed"
    record["gpu_summary"] = instr.gpu_summary
    record["ttft_metrics"] = str(ttft_path) if ttft_path.is_file() else None
    return record


def _run_project_cell(ramp: RampConfig, scale: RampScale, arm: RampArm, rep: int) -> dict:
    from src.baselines.text.products.project_static import (
        ProjectStaticConfig,
        run_project_static,
    )
    cell_output = ramp.output_root / f"scale_{scale.rows}" / f"{arm.arm}_K{arm.concurrency}_rep{rep}"
    if cell_output.exists():
        raise FileExistsError(f"cell output already exists: {cell_output}")
    cell_output.mkdir(parents=True)
    _write_identity("project_static", cell_output)
    cfg = ProjectStaticConfig(
        database_url=ramp.database_url,
        workload_name=ramp.workload_name,
        endpoint_url=ramp.endpoint_urls[0],
        endpoint_urls=tuple(ramp.endpoint_urls),
        model=ramp.model,
        max_tokens=ramp.max_tokens,
        token_budget=ramp.project_token_budget,
        max_inflight=arm.concurrency,
        max_active_work_per_endpoint=ramp.project_active_work,
        actor_workers_per_endpoint=ramp.project_actor_workers,
        ray_actor_max_concurrency=ramp.project_ray_concurrency,
        total_rows=scale.rows,
        python_executable=ramp.driver_python,
    )
    record = {"arm": "project_static", "scale": scale.rows, "concurrency": arm.concurrency,
              "rep": rep, "cell": str(cell_output), "kind": "project"}
    try:
        run_result = run_project_static(cfg, cell_output)
        # Gate the cell on BOTH exit_code==0 AND a formal status==ok row (audit F16): exit 0 alone
        # does not strictly guarantee the profiler produced a formal-ok row, so a future code path
        # that exits 0 without one would otherwise be a silent false-pass.
        record["status"] = (
            "passed" if (run_result.exit_code == 0 and run_result.formal_row_found) else "failed"
        )
        record["exit_code"] = run_result.exit_code
        record["formal_row_found"] = run_result.formal_row_found
        record["effective_k"] = run_result.effective_k
        if record["status"] == "failed" and not run_result.formal_row_found:
            record["error"] = (
                f"project_static cell not passed: exit={run_result.exit_code}, "
                f"formal_row_found=False; stderr_tail={run_result.stderr_tail[-200:]!r}"
            )
    except Exception as exc:
        record["status"] = "failed"
        record["error"] = f"{type(exc).__name__}: {exc}"
        record["traceback"] = traceback.format_exc()
    return record


def _backend_skew(success_deltas: dict) -> float:
    """Relative skew between two backends' request_success_delta (0 = perfect
    round-robin, 1 = one backend idle). Pure function (no I/O) so the lb_rr
    balance gate can be unit-tested without nginx/vLLM."""
    vals = [v for v in success_deltas.values() if v > 0]
    if len(vals) < 2:
        return 0.0
    return abs(vals[0] - vals[1]) / max(vals)


def _run_lb_rr_cell(ramp: RampConfig, scale: RampScale, arm: RampArm, rep: int) -> dict:
    """lb_rr: ONE duckdb_ai shard via the nginx LB (1 process -> LB -> 2 backends).

    Not a gate arm (gate requires 2 direct endpoints). Concurrency here is the
    DuckDB process's TOTAL in-flight (nginx round-robins to the 2 backends), so
    arm.concurrency for lb_rr is total, not per-endpoint. The instrumented_cell
    scrapes /metrics on BOTH backend endpoints (8000/8001) since the LB splits
    load across them. Uses the endpoint_count=1 manifest (all rows -> LB).
    """
    import subprocess
    if not ramp.lb_endpoint_url:
        raise ValueError("lb_rr arm requires lb_endpoint_url in the ramp config")
    cell_output = ramp.output_root / f"scale_{scale.rows}" / f"{arm.arm}_c{arm.concurrency}_rep{rep}"
    if cell_output.exists():
        raise FileExistsError(f"cell output already exists: {cell_output}")
    cell_output.mkdir(parents=True)
    _write_identity("lb_rr", cell_output)
    shard_dir = cell_output / "gate_output" / f"{arm.arm}_c{arm.concurrency}" / "shard_0"
    # NOTE: do NOT pre-create shard_dir -- run_official_baseline.py run-shard refuses
    # to write to an existing output-dir (it creates it itself).
    script = Path(__file__).resolve().parent / "run_official_baseline.py"
    cmd = [
        ramp.driver_python, str(script), "run-shard",
        "--adapter", "duckdb_ai",
        "--manifest", str(scale.manifest),
        "--endpoint-index", "0",
        "--endpoint-url", ramp.lb_endpoint_url,
        "--model", ramp.model,
        "--concurrency", str(arm.concurrency),
        "--batch-size", "1",
        "--output-dir", str(shard_dir),
        "--disable-arrival-replay",
        "--service-prefix-caching", ramp.service_prefix_caching,
        "--service-max-num-seqs", str(ramp.service_max_num_seqs),
        "--service-max-num-batched-tokens", str(ramp.service_max_num_batched_tokens),
        "--duckdb-max-concurrent-requests", str(arm.concurrency),
    ]
    metrics_urls = _metrics_urls(ramp.endpoint_urls)
    record = {"arm": "lb_rr", "scale": scale.rows, "concurrency": arm.concurrency,
              "rep": rep, "cell": str(cell_output), "kind": "lb_rr"}
    try:
        with instrumented_cell(
            metrics_urls, cell_output / "gpu_resource.csv", interval_s=0.3,
        ) as instr:
            completed = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        if completed.returncode != 0:
            raise RuntimeError(
                f"lb_rr shard rc={completed.returncode}: {(completed.stderr or '')[-400:]}"
            )
        ttft_path = cell_output / "ttft_metrics.json"
        if instr.ttft_deltas is not None:
            ttft_path.write_text(
                json.dumps({str(ep): d for ep, d in instr.ttft_deltas.items()},
                           indent=2, ensure_ascii=False, sort_keys=True),
                encoding="utf-8",
            )
        # backend-balance fail-closed gate (复审 #3/#6): nginx round-robin must
        # split ~50/50; a large skew on request_count OR token-work signals
        # routing failure. Missing ttft_deltas = metrics scrape failed -> the
        # cell must NOT pass (previously FAIL-OPEN: missing -> silently passed).
        if not instr.ttft_deltas:
            raise RuntimeError(
                "lb_rr backend-balance gate FAILED: ttft_metrics missing -- cannot "
                "verify backend balance; cell must not pass (fail-closed).")
        succ, work = {}, {}
        for ep, d in instr.ttft_deltas.items():
            try:
                rs = float(d.get("vllm_request_success_delta", 0) or 0)
                tw = (float(d.get("vllm_prompt_tokens_delta", 0) or 0)
                      + float(d.get("vllm_generation_tokens_delta", 0) or 0))
            except (TypeError, ValueError):
                rs, tw = 0.0, 0.0
            if rs > 0:
                succ[ep] = rs
            if tw > 0:
                work[ep] = tw
        if len(succ) < 2:
            raise RuntimeError(f"lb_rr balance gate FAILED: only {len(succ)} backend has successful requests {succ}")
        if len(work) < 2:
            raise RuntimeError(f"lb_rr balance gate FAILED: only {len(work)} backend has token-work {work}")
        req_skew = _backend_skew(succ)
        work_skew = _backend_skew(work)
        record["backend_request_skew"] = round(req_skew, 4)
        record["backend_token_work_skew"] = round(work_skew, 4)
        if req_skew > 0.10 or work_skew > 0.10:
            raise RuntimeError(
                f"lb_rr backend-balance gate FAILED: request_skew {req_skew:.1%} "
                f"({succ}) / token_work_skew {work_skew:.1%} ({work}) > 10%")
        record["status"] = "passed"
        record["gpu_summary"] = instr.gpu_summary
        record["ttft_metrics"] = str(ttft_path) if ttft_path.is_file() else None
    except Exception as exc:
        record["status"] = "failed"
        record["error"] = f"{type(exc).__name__}: {exc}"
        record["traceback"] = traceback.format_exc()
        (cell_output / "run_error.json").write_text(
            json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    return record


def _cmdline_for_port(cmdlines, port):
    """Return the cmdline carrying ``--port <port>`` / ``--port=<port>``, else None.

    Pure (no /proc I/O) so it is unit-testable with synthetic cmdline strings.
    """
    needles = (f"--port {port}", f"--port={port}")
    for c in cmdlines:
        if any(n in c for n in needles):
            return c
    return None


def _flag_value_present(cmdline, flag, value):
    """True iff cmdline carries ``<flag> <value>`` or ``<flag>=<value>`` (codex
    preflight). Pure for unit testing."""
    return f"{flag} {value}" in cmdline or f"{flag}={value}" in cmdline


def _prefix_cache_flag_enabled(cmdline):
    """Prefix-cache effective state from the cmdline.

    Returns True (ON) / False (OFF) / None (absent -> vLLM default, unverified
    from cmdline). TOKEN-based so ``--enable-prefix-caching=false`` is NOT
    misread as ON by a naive substring test (codex #8). Pure for unit testing.
    """
    tokens = cmdline.split()
    flagged = [t for t in tokens if t == "--enable-prefix-caching" or t.startswith("--enable-prefix-caching=")]
    if not flagged:
        return None
    val = flagged[-1]
    if "=" not in val:
        return True  # bare flag == ON
    return val.split("=", 1)[1].strip().lower() not in ("false", "0", "off", "no")


def _verify_endpoint_cmdlines(cmdline_pool, endpoint_urls, declared_flags, strict):
    """Pure verifier: raise (strict) or WARN (non-strict) on per-endpoint cmdline
    mismatches. ``declared_flags``: {flag: value_str}. Raises RuntimeError on any
    strict failure (codex #1/#3/#7/#8); non-strict only prints WARNs. Unit-tested
    directly with synthetic cmdline strings -- no /proc, no subprocess."""
    for url in endpoint_urls:
        port = url.rsplit(":", 1)[-1].split("/")[0]
        c = _cmdline_for_port(cmdline_pool, port)
        if c is None:
            msg = f"port {port} ({url}): no matching vLLM process cmdline"
            if strict:
                raise RuntimeError(f"[ramp][preflight] {msg} (strict)")
            print(f"[ramp][preflight] WARN: {msg}", flush=True)
            continue
        for flag, val in declared_flags.items():
            if _flag_value_present(c, flag, val):
                print(f"[ramp][preflight] port {port} cmdline carries {flag} {val} (declared == effective)", flush=True)
            elif strict:
                raise RuntimeError(
                    f"port {port} cmdline missing {flag} {val} (vllm_config_strict=true; declared != effective). "
                    f"Start vLLM with the flag or set vllm_config_strict=false for screening.")
            else:
                print(f"[ramp][preflight] WARN: port {port} missing {flag} {val}; vLLM DEFAULT (effective != declared)", flush=True)
        pc = _prefix_cache_flag_enabled(c)
        if pc is True:
            print(f"[ramp][preflight] port {port} cmdline has --enable-prefix-caching (effective ON)", flush=True)
        elif pc is False:
            print(f"[ramp][preflight] port {port} cmdline has --enable-prefix-caching=false (effective OFF)", flush=True)
        elif strict:
            raise RuntimeError(
                f"port {port}: --enable-prefix-caching NOT on cmdline; effective prefix-cache UNVERIFIED "
                f"(strict mode requires a verifiable prefix-cache; add the flag or confirm via EngineCore log "
                f"then relax vllm_config_strict). 复审 #3.")
        else:
            print(f"[ramp][preflight] port {port}: --enable-prefix-caching NOT on cmdline -> vLLM DEFAULT "
                  f"(effective unverified from cmdline; check EngineCore log)", flush=True)


def _verify_vllm_config(ramp: RampConfig) -> None:
    """Preflight: each endpoint's vLLM cmdline must carry the declared service
    params (codex server audit: the ramp config declared max_num_seqs /
    max_num_batched_tokens but the vLLM processes did not carry them on the
    cmdline, so they were vLLM defaults -- reports must not present declared as
    effective). Per-endpoint (复审 #7: previously a flag on ANY process passed,
    missing a per-endpoint mismatch). ``vllm_config_strict=true`` fail-closes on
    any missing flag / unverifiable prefix-cache; false only WARNs. The matching
    logic lives in the unit-tested pure helpers above (_verify_endpoint_cmdlines).
    """
    import subprocess
    pg = subprocess.run(["pgrep", "-f", "vllm.entrypoints"], capture_output=True, text=True)
    pids = [p for p in pg.stdout.split() if p]
    if not pids:
        msg = "[ramp][preflight] no vllm.entrypoints process (pgrep)"
        if ramp.vllm_config_strict:
            raise RuntimeError(f"{msg} -- strict mode: cannot verify effective config (codex #1)")
        print(f"{msg} WARN; skipping config verify", flush=True)
        return
    cmdlines = {}
    for pid in pids:
        try:
            cmdlines[pid] = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            pass
    declared = {
        "--max-num-seqs": str(ramp.service_max_num_seqs),
        "--max-num-batched-tokens": str(ramp.service_max_num_batched_tokens),
    }
    _verify_endpoint_cmdlines(list(cmdlines.values()), ramp.endpoint_urls, declared, ramp.vllm_config_strict)


def _ensure_ray_head() -> None:
    """Start a Ray head + clear the stale cluster pointer (deploy §10.5).

    project_static uses --executor ray_actor, so ray.init() must connect to a
    live head. Two deploy-documented failure modes:
    1. /tmp/ray/ray_current_cluster left behind by a killed Ray process points at
       a dead GCS; a subsequent ray.init() WITHOUT an explicit address reads it
       and hangs ~14 min. This is the exact "project hangs, gate arms pass"
       symptom (confirmed: after rm-ing the pointer, the 2-endpoint project
       isolation completed rc=0 in 15s).
    2. No head running at RAY_ADDRESS -> ray.init() retries forever.

    So: remove the stale pointer, start a head on 127.0.0.1:6380, and verify it
    is reachable. Failures are raised (fail-closed) -- proceeding would hang.
    """
    import os
    import shutil
    import subprocess
    os.environ.setdefault("RAY_ADDRESS", "127.0.0.1:6380")
    # 0) reuse an existing healthy head if present. A previous ramp leaves its
    # head running, and ``ray start --head`` again fails with a session-name KV
    # assertion against the persisted session. ray.init to 127.0.0.1:6380
    # succeeds iff a healthy head is already there (confirmed: REUSE_OK on the
    # leftover Phase-1a head), so prefer reuse and only fall back to a fresh
    # start when nothing answers.
    # reuse ONLY if a healthy head exists AND it is clean (no leftover named
    # actors from a prior run). codex server audit: previously reused any head
    # that answered ray.nodes(), without verifying the cluster was idle/clean --
    # a prior project_static run that leaked actors would contaminate the next
    # run. A non-clean or absent head falls through to stop + fresh start.
    reuse = subprocess.run(
        ["/root/miniconda3/bin/python", "-c",
         "import ray; ray.init(address='127.0.0.1:6380', ignore_reinit_error=True); "
         "assert ray.nodes(), 'no nodes'; "
         "named = ray.util.list_named_actors(); "
         "assert not named, f'leftover named actors: {named}'; "
         "cluster = ray.cluster_resources(); avail = ray.available_resources(); "
         "held = {k: cluster[k]-avail.get(k,0) for k in cluster if cluster[k]-avail.get(k,0) > 0.5}; "
         "assert not held, f'cluster has held resources (unnamed actors/tasks): {held}'; "
         "ray.shutdown()"],
        capture_output=True, text=True, timeout=60,
    )
    if reuse.returncode == 0:
        print("[ramp] reusing CLEAN Ray head at 127.0.0.1:6380", flush=True)
        return
    print(f"[ramp] head not reusable/clean ({(reuse.stderr or reuse.stdout or '')[-200:]}); fresh start", flush=True)
    # No healthy head -> clean slate (deploy §10.5): clear the stale pointer,
    # stop any half-dead head so its session name is gone from the in-GCS KV,
    # then start a fresh head.
    try:
        Path("/tmp/ray/ray_current_cluster").unlink(missing_ok=True)
    except OSError:
        pass
    ray_cli = shutil.which("ray") or "/root/miniconda3/bin/ray"
    subprocess.run([ray_cli, "stop", "--force"], capture_output=True, text=True, timeout=60)
    result = subprocess.run(
        [ray_cli, "start", "--head", "--node-ip-address=127.0.0.1",
         "--port=6380", "--disable-usage-stats"],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"ray start --head failed (rc={result.returncode}): "
            f"{(result.stderr or result.stdout or '')[-400:]}"
        )
    # 3) health check via python (connect + cluster_resources)
    os.environ.setdefault("RAY_ADDRESS", "127.0.0.1:6380")
    check = subprocess.run(
        ["/root/miniconda3/bin/python", "-c",
         "import ray; ray.init(address='127.0.0.1:6380', ignore_reinit_error=True); "
         "print(ray.cluster_resources()); ray.shutdown()"],
        capture_output=True, text=True, timeout=60,
    )
    if check.returncode != 0:
        raise RuntimeError(
            f"Ray head health check failed: {(check.stderr or '')[-400:]}"
        )


def _manifest_is_single_endpoint(manifest_path: Path) -> bool:
    """A manifest whose meta ``endpoint_row_counts`` has a single key assigns all
    rows to endpoint 0 (e.g. lb_rr's lbrr_dev). Used by warmup to send the full
    prompt set to BOTH backends (independent vLLM prefix caches do not share, so
    warming only ep_index 0 leaves backend 1 cold). 复审 #4.
    """
    meta = manifest_path.with_suffix(manifest_path.suffix + ".meta.json")
    if meta.is_file():
        try:
            d = json.loads(meta.read_text(encoding="utf-8"))
            return len(d.get("endpoint_row_counts", {})) <= 1
        except (json.JSONDecodeError, ValueError, TypeError):
            pass
    return False


def _warmup_endpoint_indices(single_endpoint: bool, n_endpoints: int) -> tuple[int, ...]:
    """Per-backend endpoint_index for cache warmup (pure fn, 复审 rerun-contract (b)).

    Single-endpoint manifest (lb_rr lbrr_dev): ALL backends warm with the full
    prompt set (endpoint_index=0), because independent vLLM prefix caches do NOT
    share -- warming only one leaves the other cold. 2-endpoint manifest: each
    backend warms its own shard (endpoint_index = backend index).
    """
    return tuple(0 for _ in range(n_endpoints)) if single_endpoint else tuple(range(n_endpoints))


def _warmup_cache(ramp: RampConfig, scale: RampScale) -> None:
    """Uniform cache-hot warmup before a measured cell (deploy §9.1 point #4).

    Runs bounded_http on the cell's manifest at ``warmup_concurrency`` on BOTH
    endpoints (output discarded). The vLLM prefix cache is prompt-keyed, so this
    puts every arm's following measured cell into the same cache-hot state
    regardless of which adapter is measured, removing run-order contamination.
    Caveat: for an endpoint_count=1 (lb_rr) manifest only endpoint 0's rows exist,
    so backend 1 is warmed indirectly via shared prefix -- acceptable for screening;
    the formal 1w+3f per arm is the rigorous cache control.
    """
    import subprocess
    import tempfile
    script = Path(__file__).resolve().parent / "run_official_baseline.py"
    # 复审 #4: an endpoint_count=1 manifest (lb_rr lbrr_dev) assigns every row to
    # endpoint 0; warming ep_index 0/1 sends 0 rows to backend 1, and the two
    # independent vLLM processes do NOT share prefix caches -> backend 1 stays cold.
    # For single-endpoint manifests warm BOTH backends with the full prompt set
    # (endpoint_index=0); for 2-endpoint manifests warm each backend's own shard.
    single_endpoint = _manifest_is_single_endpoint(scale.manifest)
    warmup_eis = _warmup_endpoint_indices(single_endpoint, len(ramp.endpoint_urls))
    with tempfile.TemporaryDirectory(prefix="ramp_warmup_") as tmp:
        procs = []
        for ep_index, (url, warmup_ei) in enumerate(zip(ramp.endpoint_urls, warmup_eis)):
            shard_dir = Path(tmp) / f"shard_{ep_index}"
            warmup_ei = 0 if single_endpoint else ep_index
            cmd = [ramp.driver_python, str(script), "run-shard",
                   "--adapter", "bounded_http", "--manifest", str(scale.manifest),
                   "--endpoint-index", str(warmup_ei), "--endpoint-url", url,
                   "--model", ramp.model, "--concurrency", str(ramp.warmup_concurrency),
                   "--batch-size", "1", "--output-dir", str(shard_dir),
                   "--disable-arrival-replay", "--service-prefix-caching", ramp.service_prefix_caching,
                   "--service-max-num-seqs", str(ramp.service_max_num_seqs),
                   "--service-max-num-batched-tokens", str(ramp.service_max_num_batched_tokens)]
            procs.append(subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        import time
        # Shared deadline across siblings: a per-shard timeout previously killed
        # only the expired shard and raised, leaving sibling warmup procs running
        # (leak). On any timeout kill ALL siblings before raising.
        deadline = time.time() + 900
        for p in procs:
            remaining = max(1.0, deadline - time.time())
            try:
                p.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                for q in procs:
                    try:
                        q.kill()
                    except Exception:
                        pass
                raise RuntimeError("cache warmup timed out (>900s total); all sibling shards killed")
        # fail-closed (codex audit #4): a failed warmup leaves the prefix cache
        # in an unknown state -- do NOT proceed to the measured cell.
        failed = [i for i, p in enumerate(procs) if p.returncode != 0]
        if failed:
            raise RuntimeError(
                f"cache warmup shard(s) rc!=0 on endpoint index(es) {failed}; "
                f"aborting before measured cell -- prefix cache state unknown")


def _write_run_log(output_root: Path, experiment_id: str, records: list[dict]) -> None:
    """Atomically write ``ramp_run.json`` (tmp + rename) after every cell.

    A crash or kill mid-ramp still leaves an authoritative per-cell status on
    disk (codex server audit: previously ramp_run.json was written once at the
    end, so a mid-ramp abort lost all cell status -- ramp_run.json records
    stayed empty while 100s of cell files existed).
    """
    summary = {"experiment_id": experiment_id, "records": records,
               "n_passed": sum(1 for r in records if r.get("status") == "passed"),
               "n_failed": sum(1 for r in records if r.get("status") != "passed")}
    tmp = output_root / "ramp_run.json.tmp"
    tmp.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(output_root / "ramp_run.json")


def run_ramp(ramp: RampConfig) -> dict:
    ramp.output_root.mkdir(parents=True, exist_ok=True)
    _verify_vllm_config(ramp)
    if any(arm.arm == "project_static" for arm in ramp.arms):
        print("[ramp] project_static arm present -- ensuring Ray head", flush=True)
        _ensure_ray_head()
    records: list[dict] = []
    for scale in ramp.scales:
        for arm in ramp.arms:
            for rep in range(1, arm.reps + 1):
                if ramp.warmup_per_cell:
                    print(f"[ramp] warmup scale={scale.rows} (bounded_http c={ramp.warmup_concurrency})", flush=True)
                    _warmup_cache(ramp, scale)
                print(f"[ramp] scale={scale.rows} arm={arm.arm} "
                      f"{'c' if arm.arm in GATE_ARMS else 'K'}={arm.concurrency} rep={rep}", flush=True)
                if arm.arm in GATE_ARMS:
                    record = _run_gate_cell(ramp, scale, arm, rep)
                elif arm.arm == "lb_rr":
                    record = _run_lb_rr_cell(ramp, scale, arm, rep)
                else:
                    record = _run_project_cell(ramp, scale, arm, rep)
                records.append(record)
                _write_run_log(ramp.output_root, ramp.experiment_id, records)
                print(f"  -> {record['status']}", flush=True)
    return {"experiment_id": ramp.experiment_id, "records": records,
            "n_passed": sum(1 for r in records if r.get("status") == "passed"),
            "n_failed": sum(1 for r in records if r.get("status") != "passed")}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", required=True)
    p.add_argument("--driver-python", required=True)
    p.add_argument("--vllm-python", required=True)
    args = p.parse_args()
    ramp = _load_ramp_config(args.config, driver_python=args.driver_python, vllm_python=args.vllm_python)
    summary = run_ramp(ramp)
    print(json.dumps({k: v for k, v in summary.items() if k != "records"},
                     indent=2, ensure_ascii=False))
    return 0 if summary["n_failed"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
