#!/usr/bin/env python3
"""Aggregate the multicard scale-ramp results into a per-scale x arm table.

Reads the ramp driver's output layout
(<ramp_root>/scale_<S>/<arm>_c<K>_rep<R>/) and computes the ramp's performance
metrics at each scale, for each arm. The authoritative per-cell pass/fail status
comes from the driver's ``ramp_run.json`` (NOT from shard-file presence -- a
failed cell still leaves shard summaries behind, which previously caused failed
cells to be mislabelled ``passed``).

Per-rep metrics are collected into a list and aggregated to mean/CV across the
formal repeats (previously repeats were silently overwritten by the last one).

Metrics:
- service tokens/s unified (gate: sum service_total_tokens_delta / max jct_s;
  project: model_request_tokens_per_s)
- rows/s = COMPLETED request rows / model-serving wall (NOT output_tokens/wall,
  which is generation-tokens/s -- a different quantity)
- TTFT P50/P95/P99 (gate: ttft_metrics.json histogram delta; project: summary)
- per-request E2E P50/P95/P99
- GPU util/power per GPU
- prefix-cache hit rate, scheduling overhead (project)

The ramp's purpose is the TREND across scale (does tokens/s plateau? does TTFT
degrade? is the 3-arm ordering stable?). Quality (EM/F1) is established
separately; this aggregator does NOT claim quality equivalence -- it reports the
deterministic per-cell numbers and the repeat spread only.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import sys
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))


GATE_ARMS = ("bounded_http", "duckdb_ai")
# Headline numeric metrics aggregated across reps with mean + CV.
# 补齐 §7.5D (audit-followup): ITL tail + MFU + energy now surfaced from the already-captured
# per-cell vLLM /metrics delta (estimated_flops for MFU) + GPU power samples (energy).
_MEAN_CV_METRICS = (
    "service_tokens_per_s", "rows_per_s", "model_serving_wall_s", "query_jct_s",
    "request_e2e_s_p50", "request_e2e_s_p95", "request_e2e_s_p99",
    "ttft_s_p50", "ttft_s_p95", "ttft_s_p99",
    "itl_s_p50", "itl_s_p95", "itl_s_p99",
    "request_decode_time_mean_s", "request_prefill_time_mean_s", "request_inference_time_mean_s",
    "request_queue_time_mean_s",
    "prefix_cache_hit_rate", "scheduling_overhead_pct",
    # mfu_fraction is a [0,1] FRACTION (not %; §7.5F + the unit-lesson); the .md header says so.
    "mfu_fraction", "energy_j_per_1k_tokens",
    # §7.5C(1) feeding-saturation gauges (during-cell, Σ endpoints); None for old raw w/o sampler.
    "vllm_running_total_mean", "vllm_running_total_max", "vllm_waiting_total_max",
    "vllm_kv_cache_usage_max",
)


def _f(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# RTX 4090 dense peak for MFU (NVIDIA spec: 82.58 FP32, 165.2 FP16/BF16 dense fp32-accumulate,
# 330 FP8). The ramp runs qwen2.5-7b in dtype auto (bf16), so 165 TFLOPS is the correct MFU
# denominator -- matches operator_cost_profile_dual4090_formal's gpu_peak_tflops + §7.5D.
# NOTE: the ramp project arm's per-run gpu_peak_tflops column was 0.0 (a config wiring gap that
# blocked the profiler's own MFU), so the aggregator overrides with this constant.
GPU_PEAK_TFLOPS_BF16 = 165.0


def _compute_efficiency(
    per_gpu_flops: list[float],
    *,
    service_total_tokens: float,
    service_wall_s: float,
    gpu_power_mean_by_gpu: dict[str, float],
    n_gpus: int = 2,
    gpu_peak_tflops: float = GPU_PEAK_TFLOPS_BF16,
) -> dict[str, float | None]:
    """MFU + energy per cell (audit-followup: 补齐 ramp 到 §7.5D).

    Pure (no I/O) so it is unit-testable. All inputs are per-cell means/deltas.
    - ``per_gpu_flops``: ``vllm_estimated_flops_per_gpu_delta`` per GPU (one entry per GPU).
      MFU is the per-GPU fraction (matches operator_cost_profile: ``mean(per_gpu_flops) /
      (gpu_peak_tflops × 1e12 × wall)``); returned as a [0,1] FRACTION, not %.
    - ``gpu_power_mean_by_gpu``: e.g. {"gpu0": 380.0, "gpu1": 375.0}; energy = Σpower × wall.
    - ``service_total_tokens`` / ``service_wall_s``: same caliber as service_tokens_per_s
      (tokens = Σ(prompt+gen) delta; wall = model_serving_wall_s for request arms,
      query_jct_s for query_barrier arms -- documented per cell via timing_granularity).

    NOTE (§7.5F + the MFU unit-lesson): mfu is a [0,1] fraction; do NOT multiply by 100.
    energy_j_per_1k_tokens uses service_total_tokens (prompt+gen), the project's caliber.
    """
    out: dict[str, float | None] = {
        "mfu_fraction": None,
        "energy_j": None,
        "energy_j_per_1k_tokens": None,
    }
    flops = [f for f in per_gpu_flops if f and f > 0]
    if flops and service_wall_s and service_wall_s > 0 and gpu_peak_tflops and gpu_peak_tflops > 0:
        out["mfu_fraction"] = (
            sum(flops) / len(flops) / (gpu_peak_tflops * 1e12 * service_wall_s)
        )
    powers = [p for p in gpu_power_mean_by_gpu.values() if isinstance(p, (int, float)) and p == p and p > 0]
    if powers and service_wall_s and service_wall_s > 0:
        out["energy_j"] = sum(powers) * service_wall_s
        if service_total_tokens and service_total_tokens > 0:
            out["energy_j_per_1k_tokens"] = out["energy_j"] / (service_total_tokens / 1000.0)
    return out


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _identity(cell: Path) -> dict:
    """Ramp-layer identity sidecar (authoritative comparison_role; codex #1).

    Empty when absent (old raw without sidecar) -> aggregator reports None and
    the report's harness_sharded_diagnostic label stands on its own. When
    present, this OVERRIDES the single-shard ``summary.json::comparison_role``
    (which still says database_product_native_baseline for duckdb_ai).
    """
    ij = cell / "identity.json"
    if ij.is_file():
        try:
            return _read_json(ij)
        except (json.JSONDecodeError, ValueError, TypeError):
            pass
    return {}


def _parse_cell_name(name: str) -> tuple[str, int, int]:
    """Parse a cell directory name into (arm, concurrency, rep).

    Handles both the gate/lb_rr form ``<arm>_c<K>_rep<R>`` (e.g.
    ``bounded_http_c32_rep1``, ``lb_rr_c128_rep1``) and the project form
    ``project_static_K<K>_rep<R>``. ``_c``/``_K`` is the concurrency marker
    -- arm names (bounded_http, duckdb_ai, lb_rr, project_static) contain no
    such substring, so the first match is the real concurrency.
    """
    m = re.search(r"_c(\d+)_rep(\d+)$", name) or re.search(r"_K(\d+)_rep(\d+)$", name)
    if not m:
        raise ValueError(f"cannot parse cell name {name!r}")
    return name[: m.start()], int(m.group(1)), int(m.group(2))


def _identity_role_fields(cell: Path) -> dict:
    """Spread ramp-layer identity role fields into a cell-metrics dict.

    ``comparison_role`` (the STANDARD primary field every consumer reads) IS the
    AUTHORITATIVE role of the composed system under test (复审 #1/#4);
    ``component_comparison_role`` is the single-shard component role. All None
    when no identity.json (old raw) -- the aggregate then shows None rather than
    a misleading product-native role.
    """
    ident = _identity(cell)
    return {
        "comparison_role": ident.get("comparison_role"),  # system role (PRIMARY, 复审 #1)
        "component_comparison_role": ident.get("component_comparison_role"),  # single-shard component
        "formal_baseline_eligible": ident.get("formal_baseline_eligible"),
        "scheduler_owner": ident.get("scheduler_owner"),
    }


def _completed_rows(shard_dirs: list[Path], gate_dir: Path | None = None) -> int:
    """Count completed request rows, robust to ``requests.csv`` pruning.

    The ramp's ``requests.csv`` is large and gets pruned before commit; a
    previous version counted only ``requests.csv`` status=completed, which
    silently yielded 0 (and thus ``rows_per_s=0``) on pruned evidence even for
    passed cells. Prefer the structured ``summary.json::completed_count``
    (always present), then count ``requests.csv`` if summaries are missing,
    then fall back to ``gate.json::metrics.result_rows``.
    """
    # 1) summary.json.completed_count (structured, survives pruning)
    n, have_summary = 0, False
    for shard in shard_dirs:
        sj = shard / "summary.json"
        if sj.is_file():
            try:
                cc = _read_json(sj).get("completed_count")
            except (json.JSONDecodeError, ValueError, TypeError):
                cc = None
            if cc is not None:
                have_summary = True
                n += int(cc)
    if have_summary:
        return n
    # 2) requests.csv status=completed (when summaries are missing)
    for shard in shard_dirs:
        req = shard / "requests.csv"
        if not req.is_file():
            continue
        with req.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                if (row.get("status") or "").lower() == "completed":
                    n += 1
    if n > 0:
        return n
    # 3) final fallback: gate.json metrics.result_rows
    if gate_dir is not None:
        gj = gate_dir / "gate.json"
        if gj.is_file():
            try:
                return int(_read_json(gj).get("metrics", {}).get("result_rows", 0))
            except (json.JSONDecodeError, ValueError, TypeError):
                pass
    return n


def _gate_cell_metrics(cell: Path) -> dict:
    """bounded_http/duckdb_ai (2 shards) OR lb_rr (1 shard) -> ramp metrics."""
    gate_dir = next((c for c in (cell / "gate_output").iterdir() if c.is_dir()), None)
    if gate_dir is None:
        return {"status": "missing_gate_output"}
    shard_dirs = sorted(gate_dir.glob("shard_*"))
    summaries = [_read_json(s / "summary.json") for s in shard_dirs if (s / "summary.json").is_file()]
    if not summaries:
        return {"status": "missing_shards"}
    max_jct = max(_f(s.get("jct_s")) for s in summaries)
    # Authoritative service tokens/s (复审 #2/#5): previously total_tokens/max_jct
    # used the wrong wall and, for lb_rr, the wrong numerator.
    # Priority: (1) gate.json group 口径 (bounded/duckdb gate arms):
    #   group_service_total_tokens / group_service_wall_s;
    # (2) ttft_metrics vLLM counters (lb_rr, no gate.json): Σ(后端 prompt+gen delta) / shard wall;
    # (3) fallback summary total_tokens/max_jct (duckdb input-token est, misses gen/chat-template).
    gate_json = _read_json(gate_dir / "gate.json") if (gate_dir / "gate.json").is_file() else {}
    gmetrics = gate_json.get("metrics", {}) if isinstance(gate_json, dict) else {}
    group_wall = _f(gmetrics.get("group_service_wall_s"))
    group_tokens = _f(gmetrics.get("group_service_total_tokens"))
    ttft_path = cell / "ttft_metrics.json"
    ttft_data = _read_json(ttft_path) if ttft_path.is_file() else {}
    ttft_eps = [ttft_data[str(i)] for i in range(len(ttft_data)) if str(i) in ttft_data]
    ttft_service_tokens = sum(
        _f(e.get("vllm_prompt_tokens_delta")) + _f(e.get("vllm_generation_tokens_delta")) for e in ttft_eps)
    if group_wall > 0 and group_tokens > 0:
        unified_tps = group_tokens / group_wall
        service_total, service_wall, token_source = int(group_tokens), group_wall, "gate_group"
    elif ttft_service_tokens > 0 and max_jct > 0:
        unified_tps = ttft_service_tokens / max_jct
        service_total, service_wall, token_source = int(ttft_service_tokens), max_jct, "ttft_vllm_counters"
    else:
        # 复审 #5: summary.total_tokens/max_jct is a FORBIDDEN 口径 (duckdb
        # total_tokens misses generation/chat-template; max_jct != group wall).
        # Do NOT emit a rankable number -- mark metric unavailable so the cell
        # cannot enter a ranking with a wrong-metric number.
        unified_tps = None
        service_total, service_wall, token_source = 0, max_jct, "metric_unavailable"
    # Authoritative status: run_status.json (gate arms) / run_error.json (any).
    run_status_path = cell / "gate_output" / "run_status.json"
    status = "unknown"
    if run_status_path.is_file():
        status = str(_read_json(run_status_path).get("status", "unknown"))
    if (cell / "run_error.json").is_file():
        status = "failed"
    completed = _completed_rows(shard_dirs, gate_dir)
    rows_per_s = completed / service_wall if service_wall > 0 else 0.0
    latency = {q: max(_f(s.get(f"latency_{q}_s")) for s in summaries) for q in ("p50", "p95", "p99")}
    # 复审 #5: duckdb_ai timing_granularity=query_barrier -> latency_*_s is the whole-SQL JCT,
    # NOT per-request E2E; surface the granularity so callers do not misread it as request E2E.
    granularities = {s.get("timing_granularity") for s in summaries}
    timing_granularity = summaries[0].get("timing_granularity") if summaries else None
    if len(granularities) > 1:
        # 复审 #4 (codex timing): shards disagree on timing granularity -> the aggregate
        # cannot be read under one timing semantics (one shard's JCT is a query barrier,
        # another's is per-request E2E). Fail closed; do NOT emit a rankable number.
        return {"status": "failed",
                "error": f"mixed timing_granularity across shards: {sorted(str(g) for g in granularities)}",
                "timing_granularity": "mixed"}
    # ttft/itl/prefix_hit/per_gpu_flops/phase-times reuse ttft_eps (vLLM /metrics per-backend deltas).
    # 补齐 (audit-followup §7.5D): the per-cell ttft_metrics.json already carries estimated_flops
    # (for MFU), ITL p50/p95/p99, decode/prefill/inference/queue means -- surface them, don't recollect.
    ttft = {"p50": None, "p95": None, "p99": None}
    itl = {"p50": None, "p95": None, "p99": None}
    prefix_hit = None
    per_gpu_flops: list[float] = []
    phase_times: dict[str, float] = {}
    if ttft_eps:
        for q in ttft:
            vals = [_f(e.get(f"vllm_time_to_first_token_{q}_s")) for e in ttft_eps]
            vals = [v for v in vals if v > 0]
            if vals:
                ttft[q] = statistics.mean(vals)
            itl_vals = [_f(e.get(f"vllm_inter_token_latency_{q}_s")) for e in ttft_eps]
            itl_vals = [v for v in itl_vals if v > 0]
            if itl_vals:
                itl[q] = statistics.mean(itl_vals)
        hits = [_f(e.get("vllm_prefix_cache_hit_rate")) for e in ttft_eps]
        hits = [h for h in hits if h > 0]
        if hits:
            prefix_hit = statistics.mean(hits)
        per_gpu_flops = [_f(e.get("vllm_estimated_flops_per_gpu_delta")) for e in ttft_eps]
        for phase in ("decode", "prefill", "inference", "queue"):
            vals = [_f(e.get(f"vllm_request_{phase}_time_mean_s")) for e in ttft_eps]
            vals = [v for v in vals if v > 0]
            if vals:
                phase_times[f"request_{phase}_time_mean_s"] = statistics.mean(vals)
    gpu_summary = _gpu_csv_summary(cell / "gpu_resource.csv")
    gpu_power_by_gpu = {
        k: v for k, v in {
            "gpu0": gpu_summary.get("gpu0_power_mean") if isinstance(gpu_summary, dict) else None,
            "gpu1": gpu_summary.get("gpu1_power_mean") if isinstance(gpu_summary, dict) else None,
        }.items()
    }
    efficiency = _compute_efficiency(
        per_gpu_flops,
        service_total_tokens=service_total,
        service_wall_s=service_wall,
        gpu_power_mean_by_gpu=gpu_power_by_gpu,
    )
    # §7.5C(1) feeding-saturation: during-cell vLLM gauges (running/waiting/KV mean/max), written
    # by the ramp when the VllmGaugeSampler ran. Absent for old raw (before the sampler) -> None.
    gauge_path = cell / "vllm_gauges.json"
    gauge = _read_json(gauge_path) if gauge_path.is_file() else {}
    return {
        "status": status,
        "service_tokens_per_s": round(unified_tps, 1) if unified_tps is not None else None,
        "service_total_tokens": service_total,
        # 复审 #2/#4 (codex timing): at query_barrier granularity service_wall IS the whole-SQL
        # query barrier (operator wall), NOT pure model serving -- emit it as query_jct_s and
        # leave model_serving_wall_s None so consumers do not misread the query barrier as a
        # model-serving wall. At request granularity (bounded) service_wall is the model-serving
        # wall; query_jct_s is None (no SQL barrier).
        "model_serving_wall_s": None if timing_granularity == "query_barrier" else round(service_wall, 3),
        "query_jct_s": round(service_wall, 3) if timing_granularity == "query_barrier" else None,
        "service_tokens_source": token_source,
        "completed_rows": completed,
        "rows_per_s": round(rows_per_s, 2),
        "timing_granularity": timing_granularity,
        # 复审 #2: when timing_granularity=query_barrier the latency_*_s is the whole-SQL JCT,
        # NOT per-request E2E -- do NOT emit it under the request_e2e name (a separate
        # timing_granularity field is not enough; the misnamed field itself must be absent).
        "request_e2e_s_p50": None if timing_granularity == "query_barrier" else round(latency["p50"], 3),
        "request_e2e_s_p95": None if timing_granularity == "query_barrier" else round(latency["p95"], 3),
        "request_e2e_s_p99": None if timing_granularity == "query_barrier" else round(latency["p99"], 3),
        "ttft_s_p50": round(ttft["p50"], 4) if ttft["p50"] else None,
        "ttft_s_p95": round(ttft["p95"], 4) if ttft["p95"] else None,
        "ttft_s_p99": round(ttft["p99"], 4) if ttft["p99"] else None,
        # 补齐 §7.5D: inter-token latency (gate arms only -- project summary has no ITL histogram).
        "itl_s_p50": round(itl["p50"], 4) if itl["p50"] else None,
        "itl_s_p95": round(itl["p95"], 4) if itl["p95"] else None,
        "itl_s_p99": round(itl["p99"], 4) if itl["p99"] else None,
        **phase_times,
        "prefix_cache_hit_rate": round(prefix_hit, 4) if prefix_hit is not None else None,
        # 补齐 §7.5D: MFU (FRACTION [0,1], not %; per-GPU, basis=service_wall) + energy.
        "mfu_fraction": round(efficiency["mfu_fraction"], 4) if efficiency["mfu_fraction"] is not None else None,
        "energy_j": round(efficiency["energy_j"], 1) if efficiency["energy_j"] is not None else None,
        "energy_j_per_1k_tokens": round(efficiency["energy_j_per_1k_tokens"], 2) if efficiency["energy_j_per_1k_tokens"] is not None else None,
        # §7.5C(1) feeding-saturation: during-cell gauge mean/max (None if old raw w/o sampler).
        # vllm_running_total_* = Σ across endpoints; KV is [0,1] fraction (§7.5F).
        "vllm_running_total_mean": round(gauge["vllm_running_mean"], 1) if gauge.get("vllm_running_mean") is not None else None,
        "vllm_running_total_max": round(gauge["vllm_running_max"], 1) if gauge.get("vllm_running_max") is not None else None,
        "vllm_waiting_total_mean": round(gauge["vllm_waiting_mean"], 1) if gauge.get("vllm_waiting_mean") is not None else None,
        "vllm_waiting_total_max": round(gauge["vllm_waiting_max"], 1) if gauge.get("vllm_waiting_max") is not None else None,
        "vllm_kv_cache_usage_mean": round(gauge["vllm_kv_cache_usage_mean"], 3) if gauge.get("vllm_kv_cache_usage_mean") is not None else None,
        "vllm_kv_cache_usage_max": round(gauge["vllm_kv_cache_usage_max"], 3) if gauge.get("vllm_kv_cache_usage_max") is not None else None,
        "gpu": gpu_summary,
        **_identity_role_fields(cell),
    }


def _project_cell_metrics(cell: Path) -> dict:
    """project_static cell -> ramp metrics (project_static_* files)."""
    summary = cell / "project_static_summary.csv"
    resource = cell / "project_static_resource.csv"
    evidence = cell / "project_static_completion_evidence.csv"
    run_error = cell / "run_error.json"
    # Authoritative status: the ramp driver writes run_error.json on failure; a
    # present summary CSV with a formal row means it produced output. Without
    # ramp_run.json we treat (summary present AND no run_error) as passed.
    if run_error.is_file():
        return {"status": "failed", "error": str(_read_json(run_error).get("error", ""))}
    if not summary.is_file():
        return {"status": "missing_project_summary"}
    with summary.open(encoding="utf-8") as handle:
        prof = next(csv.DictReader(handle))
    gpu_summary = _gpu_csv_summary(resource)
    # 补齐 §7.5D: project MFU + energy. prof has vllm_estimated_flops_per_gpu_delta (per-GPU)
    # + model_request_wall_s; gpu_power from the resource csv. (project summary has no ITL
    # histogram, so itl_* stay None for this arm -- only the gate arms carry ITL.) Use the
    # aggregator's GPU_PEAK_TFLOPS_BF16 (165) because the ramp project arm's own gpu_peak_tflops
    # column was 0.0 (config gap); prefer the prof value only if it is a positive, finite peak.
    prof_peak = _f(prof.get("gpu_peak_tflops"))
    gpu_peak = prof_peak if prof_peak and prof_peak > 0 else GPU_PEAK_TFLOPS_BF16
    efficiency = _compute_efficiency(
        [_f(prof.get("vllm_estimated_flops_per_gpu_delta"))],
        service_total_tokens=_f(prof.get("vllm_prompt_tokens_delta")) + _f(prof.get("vllm_generation_tokens_delta")),
        service_wall_s=_f(prof.get("model_request_wall_s")),
        gpu_power_mean_by_gpu={
            k: v for k, v in {
                "gpu0": gpu_summary.get("gpu0_power_mean") if isinstance(gpu_summary, dict) else None,
                "gpu1": gpu_summary.get("gpu1_power_mean") if isinstance(gpu_summary, dict) else None,
                "gpu_aggregated": gpu_summary.get("gpu_aggregated_power_mean") if isinstance(gpu_summary, dict) else None,
            }.items()
        },
        gpu_peak_tflops=gpu_peak,
    )
    out = {
        "status": "passed",
        "service_tokens_per_s": round(_f(prof.get("model_request_tokens_per_s")), 1),
        "service_total_tokens": int(_f(prof.get("vllm_prompt_tokens_delta")) + _f(prof.get("vllm_generation_tokens_delta"))),
        "model_serving_wall_s": round(_f(prof.get("model_request_wall_s")), 3),
        "query_jct_s": None,  # project arm submits via Ray actors (request granularity, no SQL barrier)
        "timing_granularity": "request",
        "rows_per_s": round(_f(prof.get("rows_per_s")), 2),
        "request_e2e_s_p50": round(_f(prof.get("request_e2e_s_p50")), 3),
        "request_e2e_s_p95": round(_f(prof.get("request_e2e_s_p95")), 3),
        "request_e2e_s_p99": round(_f(prof.get("request_e2e_s_p99")), 3),
        "ttft_s_p50": round(_f(prof.get("vllm_time_to_first_token_p50_s")), 4) or None,
        "ttft_s_p95": round(_f(prof.get("vllm_time_to_first_token_p95_s")), 4) or None,
        "ttft_s_p99": round(_f(prof.get("vllm_time_to_first_token_p99_s")), 4) or None,
        # project summary has no ITL histogram -> None (gate arms carry ITL).
        "itl_s_p50": None, "itl_s_p95": None, "itl_s_p99": None,
        "request_decode_time_mean_s": round(_f(prof.get("vllm_request_decode_time_mean_s")), 4) or None,
        "request_prefill_time_mean_s": round(_f(prof.get("vllm_request_prefill_time_mean_s")), 4) or None,
        "request_inference_time_mean_s": round(_f(prof.get("vllm_request_inference_time_mean_s")), 4) or None,
        "request_queue_time_mean_s": round(_f(prof.get("vllm_request_queue_time_mean_s")), 4) or None,
        "prefix_cache_hit_rate": round(_f(prof.get("vllm_prefix_cache_hit_rate")), 4) or None,
        "scheduling_overhead_pct": round(_f(prof.get("scheduling_control_overhead_pct")), 2) or None,
        "submit_s": round(_f(prof.get("submit_s")), 3) or None,
        # 补齐 §7.5D: MFU (FRACTION [0,1], per-GPU, basis=model_request_wall_s) + energy.
        "mfu_fraction": round(efficiency["mfu_fraction"], 4) if efficiency["mfu_fraction"] is not None else None,
        "energy_j": round(efficiency["energy_j"], 1) if efficiency["energy_j"] is not None else None,
        "energy_j_per_1k_tokens": round(efficiency["energy_j_per_1k_tokens"], 2) if efficiency["energy_j_per_1k_tokens"] is not None else None,
        "gpu": gpu_summary,
        **_identity_role_fields(cell),
    }
    if evidence.is_file():
        out["n_evidence_rows"] = sum(1 for _ in csv.DictReader(evidence.open(encoding="utf-8")))
    return out


def _gpu_csv_summary(resource_csv: Path) -> dict:
    if not resource_csv.is_file():
        return {"status": "missing"}
    by_gpu: dict[str, dict[str, list[float]]] = {}
    n_samples = 0
    with resource_csv.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            n_samples += 1
            idx = row.get("gpu_index")
            key = f"gpu{idx}" if idx is not None and str(idx).isdigit() else "gpu_aggregated"
            slot = by_gpu.setdefault(key, {"util": [], "power": []})
            u = _f(row.get("gpu_utilization_pct"), default=float("nan"))
            p = _f(row.get("gpu_power_w"), default=float("nan"))
            if u == u:
                slot["util"].append(u)
            if p == p:
                slot["power"].append(p)
    out = {"status": "ok", "n_samples": n_samples}
    for key, slot in sorted(by_gpu.items()):
        out[f"{key}_util_mean"] = round(statistics.mean(slot["util"]), 1) if slot["util"] else None
        out[f"{key}_power_mean"] = round(statistics.mean(slot["power"]), 1) if slot["power"] else None
    return out


def _mean_cv(values: list[float]) -> tuple[float | None, float | None]:
    vals = [v for v in values if v is not None]
    if not vals:
        return (None, None)
    mean = statistics.mean(vals)
    cv = (statistics.stdev(vals) / mean) if (len(vals) > 1 and mean) else 0.0  # sample stdev (n-1), 复审 #5
    return (round(mean, 4), round(cv * 100, 2))


def _aggregate_reps(reps: list[dict]) -> dict:
    """Collapse a list of per-rep metrics into one summary with mean/CV + status."""
    n = len(reps)
    passed = [r for r in reps if r.get("status") == "passed"]
    overall_status = "passed" if passed and len(passed) == n else ("partial" if passed else "failed")
    agg: dict[str, object] = {
        "status": overall_status,
        "n_reps": n,
        "n_passed": len(passed),
        "reps": sorted(reps, key=lambda r: r.get("rep", 0)),
    }
    for metric in _MEAN_CV_METRICS:
        mean, cv = _mean_cv([r.get(metric) for r in passed])
        if mean is not None:
            agg[f"{metric}_mean"] = mean
            agg[f"{metric}_cv_pct"] = cv
    # GPU + service tokens totals: take the first passed rep's (they're per-cell totals)
    if passed:
        agg["gpu"] = passed[0].get("gpu", {})
        agg["service_total_tokens"] = passed[0].get("service_total_tokens")
        agg["completed_rows"] = passed[0].get("completed_rows")
        agg["comparison_role"] = passed[0].get("comparison_role")  # system role (PRIMARY)
        agg["component_comparison_role"] = passed[0].get("component_comparison_role")
        agg["formal_baseline_eligible"] = passed[0].get("formal_baseline_eligible")
        agg["scheduler_owner"] = passed[0].get("scheduler_owner")
        agg["service_tokens_source"] = passed[0].get("service_tokens_source")
        agg["timing_granularity"] = passed[0].get("timing_granularity")
        if "scheduling_overhead_pct" in passed[0]:
            agg["scheduling_overhead_pct_mean"] = passed[0].get("scheduling_overhead_pct")
    if any(r.get("status") != "passed" for r in reps):
        agg["failed_rep_errors"] = [r.get("error", r.get("status")) for r in reps if r.get("status") != "passed"]
    return agg


def aggregate(ramp_root: Path) -> dict:
    # Authoritative per-cell status from the driver's run log. Keyed by
    # (scale, arm, concurrency, rep) so that a concurrency-sweep (one arm at
    # many concurrencies on a fixed scale) does not collapse distinct cells
    # into one -- previously the key dropped concurrency, so bounded@c1 and
    # bounded@c64 at scale 2048 were mis-grouped as two reps of the same cell.
    status_map: dict[tuple[int, str, int, int], tuple[str, str]] = {}
    run_json = ramp_root / "ramp_run.json"
    if run_json.is_file():
        for r in _read_json(run_json).get("records", []):
            try:
                status_map[(int(r["scale"]), str(r["arm"]), int(r["concurrency"]), int(r["rep"]))] = (
                    str(r.get("status", "unknown")), str(r.get("error", "")),
                )
            except (KeyError, ValueError, TypeError):
                continue
    result: dict[str, object] = {}
    for scale_dir in sorted(ramp_root.glob("scale_*")):
        try:
            rows = int(scale_dir.name.split("_")[1])
        except (IndexError, ValueError):
            continue
        # Group reps by (arm, concurrency). Under a concurrency-sweep the same
        # arm appears at many concurrencies on one scale; concurrency is a real
        # grouping key, not noise in the cell name.
        per_key_reps: dict[tuple[str, int], list[dict]] = {}
        for cell in sorted(scale_dir.iterdir()):
            if not cell.is_dir():
                continue
            try:
                arm, conc, rep = _parse_cell_name(cell.name)
            except ValueError:
                continue
            if arm in GATE_ARMS or arm == "lb_rr":
                metrics = _gate_cell_metrics(cell)
            elif arm == "project_static":
                metrics = _project_cell_metrics(cell)
            else:
                continue
            # Driver's run log is authoritative for status.
            key = (rows, arm, conc, rep)
            if key in status_map:
                metrics["status"], metrics["error"] = status_map[key]
            metrics["rep"] = rep
            per_key_reps.setdefault((arm, conc), []).append(metrics)
        arms_tree: dict[str, dict] = {}
        for (arm, conc), reps in per_key_reps.items():
            arms_tree.setdefault(arm, {})[f"c{conc}"] = _aggregate_reps(reps)
        result[f"scale_{rows}"] = {"rows": rows, "arms": arms_tree}
    return result


def _fmt(v) -> str:
    return "—" if v is None else str(v)


def _md(result: dict) -> str:
    scales = sorted(int(k.split("_")[1]) for k in result)
    title_range = f"{scales[0]}→{scales[-1]}" if scales else "?"
    lines = [f"# 多卡 ramp 聚合（规模 {title_range}，mean across passed reps）", ""]
    lines.append("| scale | arm | conc | status | tok/s mean | tok/s CV | rows/s | TTFT P50 | E2E P50 | prefix-hit | GPU0 util | GPU1 util | n_passed/n |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for key in sorted(result, key=lambda k: int(k.split("_")[1])):
        scale = result[key]
        for arm in sorted(scale["arms"]):
            for conc_key in sorted(scale["arms"][arm], key=lambda c: int(c[1:])):
                m = scale["arms"][arm][conc_key]
                ttft = m.get("ttft_s_p50_mean")
                ttft_s = f"{ttft*1000:.1f}ms" if ttft else "—"
                gpu = m.get("gpu", {})
                g0 = gpu.get("gpu0_util_mean") or gpu.get("gpu_aggregated_util_mean") or "—"
                g1 = gpu.get("gpu1_util_mean", "—")
                hit = m.get("prefix_cache_hit_rate_mean")
                hit_s = f"{hit:.2f}" if hit is not None else "—"
                lines.append(
                    f"| {scale['rows']} | {arm} | {conc_key} | {m.get('status','—')} | "
                    f"{_fmt(m.get('service_tokens_per_s_mean'))} | {m.get('service_tokens_per_s_cv_pct','—')}% | "
                    f"{_fmt(m.get('rows_per_s_mean'))} | {ttft_s} | {_fmt(m.get('request_e2e_s_p50_mean'))} | "
                    f"{hit_s} | {g0} | {g1} | {m.get('n_passed','?')}/{m.get('n_reps','?')} |"
                )
    # 补齐 §7.5D efficiency table (audit-followup): MFU + ITL tail + decode/prefill + energy.
    # MFU is a [0,1] FRACTION (vLLM estimated_flops heuristic, conservative -- NOT theoretical
    # 2N; see operator_cost_profile §5.4). ITL is gate-arms-only (project summary has no ITL).
    lines += [
        "",
        "## 效率与尾延迟（§7.5D 补齐；MFU=[0,1] 分数，非 %；vLLM estimated_flops 保守估计）",
        "",
        "| scale | arm | conc | MFU(frac) | ITL p95 | ITL p99 | TTFT p99 | decode | prefill | J/1k-tok |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for key in sorted(result, key=lambda k: int(k.split("_")[1])):
        scale = result[key]
        for arm in sorted(scale["arms"]):
            for conc_key in sorted(scale["arms"][arm], key=lambda c: int(c[1:])):
                m = scale["arms"][arm][conc_key]
                def _ms(field, mul=1, unit=""):
                    v = m.get(f"{field}_mean")
                    return f"{v*mul:.4g}{unit}" if isinstance(v, (int, float)) and v == v and v > 0 else "—"
                mfu = m.get("mfu_fraction_mean")
                mfu_s = f"{mfu:.3f}" if isinstance(mfu, (int, float)) and mfu == mfu and mfu >= 0 else "—"
                lines.append(
                    f"| {scale['rows']} | {arm} | {conc_key} | {mfu_s} | "
                    f"{_ms('itl_s_p95', 1000, 'ms')} | {_ms('itl_s_p99', 1000, 'ms')} | "
                    f"{_ms('ttft_s_p99', 1000, 'ms')} | {_ms('request_decode_time_mean_s', 1000, 'ms')} | "
                    f"{_ms('request_prefill_time_mean_s', 1000, 'ms')} | "
                    f"{_fmt(m.get('energy_j_per_1k_tokens_mean'))} |"
                )
    return "\n".join(lines) + "\n"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ramp-root", required=True, type=Path)
    p.add_argument("--output-json", required=True, type=Path)
    p.add_argument("--output-md", type=Path)
    args = p.parse_args()
    result = aggregate(args.ramp_root)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {args.output_json}", file=sys.stderr)
    if args.output_md:
        args.output_md.write_text(_md(result), encoding="utf-8")
        print(f"wrote {args.output_md}", file=sys.stderr)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
