#!/usr/bin/env python3
"""Aggregate the multicard scale-ramp results into a per-scale x arm table.

Reads the ramp driver's output layout
(<ramp_root>/scale_<S>/<arm>_c<K>_rep<R>/) and computes the ramp's performance
metrics at each scale, for each arm:

- service tokens/s unified (gate: sum service_total_tokens_delta / max jct_s;
  project: model_request_tokens_per_s)
- rows/s, model-serving wall
- TTFT P50/P95/P99 (gate: ttft_metrics.json histogram delta; project: summary)
- per-request E2E P50/P95/P99
- GPU util/power per GPU (gate: per-GPU from gpu_resource.csv; project:
  aggregated mean from project_static_resource.csv -- the profiler sums both GPUs
  per sample)
- prefix-cache hit rate, scheduling overhead (project)

The ramp's purpose is the TREND across scale (does tokens/s plateau? does TTFT
degrade? is the 3-arm ordering stable? how does prefix-hit move with the working
set?). Quality (EM/F1) is established separately at 2048 (~82.3%); per-scale
quality needs per-scale references and is reported only if a references file for
that scale is provided.

This is the D-phase aggregator for the ramp; it complements
multicard_rich_aggregate.py (which handles the 2048 rich_formal layout).
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.observability.metrics.squad import squad_quality_metrics  # noqa: E402


def _f(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _gate_cell_metrics(cell: Path) -> dict:
    """bounded_http/duckdb_ai cell -> ramp metrics."""
    gate_dir = next((c for c in (cell / "gate_output").iterdir() if c.is_dir()), None)  # the cell_id dir (not resolved_config.json/run_status.json files)
    if gate_dir is None:
        return {"status": "missing_gate_output"}
    shards = [gate_dir / "shard_0", gate_dir / "shard_1"]
    summaries = [_read_json(s / "summary.json") for s in shards if (s / "summary.json").is_file()]
    if len(summaries) != 2:
        return {"status": f"missing_shards ({len(summaries)}/2)"}
    total_tokens = sum(s.get("service_total_tokens_delta", 0) for s in summaries)
    max_jct = max(_f(s.get("jct_s")) for s in summaries)
    unified_tps = total_tokens / max_jct if max_jct > 0 else 0.0
    latency = {q: max(_f(s.get(f"latency_{q}_s")) for s in summaries) for q in ("p50", "p95", "p99")}
    # TTFT + prefix-hit from the instrumented_cell delta (per-endpoint; average ep0+ep1)
    ttft_path = cell / "ttft_metrics.json"
    ttft = {"p50": None, "p95": None, "p99": None}
    prefix_hit = None
    if ttft_path.is_file():
        deltas = _read_json(ttft_path)
        eps = [deltas[str(i)] for i in range(len(deltas)) if str(i) in deltas]
        for q in ttft:
            vals = [_f(e.get(f"vllm_time_to_first_token_{q}_s")) for e in eps]
            vals = [v for v in vals if v > 0]
            if vals:
                ttft[q] = statistics.mean(vals)
        hits = [_f(e.get("vllm_prefix_cache_hit_rate")) for e in eps]
        hits = [h for h in hits if h > 0]
        if hits:
            prefix_hit = statistics.mean(hits)
    return {
        "status": "passed",
        "service_tokens_per_s": round(unified_tps, 1),
        "service_total_tokens": int(total_tokens),
        "model_serving_wall_s": round(max_jct, 3),
        "rows_per_s": round(sum(int(s.get("output_tokens", 0) or 0) for s in summaries) / max_jct, 1)
        if max_jct > 0 else 0.0,  # gen-rows/s approximation
        "request_e2e_s_p50": round(latency["p50"], 3),
        "request_e2e_s_p95": round(latency["p95"], 3),
        "request_e2e_s_p99": round(latency["p99"], 3),
        "ttft_s_p50": round(ttft["p50"], 4) if ttft["p50"] else None,
        "ttft_s_p95": round(ttft["p95"], 4) if ttft["p95"] else None,
        "ttft_s_p99": round(ttft["p99"], 4) if ttft["p99"] else None,
        "prefix_cache_hit_rate": round(prefix_hit, 4) if prefix_hit is not None else None,
        "gpu": _gpu_csv_summary(cell / "gpu_resource.csv"),
    }


def _project_cell_metrics(cell: Path) -> dict:
    """project_static cell -> ramp metrics (project_static_* files)."""
    summary = cell / "project_static_summary.csv"
    resource = cell / "project_static_resource.csv"
    evidence = cell / "project_static_completion_evidence.csv"
    if not summary.is_file():
        return {"status": "missing_project_summary"}
    with summary.open(encoding="utf-8") as handle:
        prof = next(csv.DictReader(handle))
    out = {
        "status": "passed",
        "service_tokens_per_s": round(_f(prof.get("model_request_tokens_per_s")), 1),
        "service_total_tokens": int(_f(prof.get("vllm_prompt_tokens_delta")) + _f(prof.get("vllm_generation_tokens_delta"))),
        "model_serving_wall_s": round(_f(prof.get("model_request_wall_s")), 3),
        "rows_per_s": round(_f(prof.get("rows_per_s")), 1),
        "request_e2e_s_p50": round(_f(prof.get("request_e2e_s_p50")), 3),
        "request_e2e_s_p95": round(_f(prof.get("request_e2e_s_p95")), 3),
        "request_e2e_s_p99": round(_f(prof.get("request_e2e_s_p99")), 3),
        "ttft_s_p50": round(_f(prof.get("vllm_time_to_first_token_p50_s")), 4) or None,
        "ttft_s_p95": round(_f(prof.get("vllm_time_to_first_token_p95_s")), 4) or None,
        "ttft_s_p99": round(_f(prof.get("vllm_time_to_first_token_p99_s")), 4) or None,
        "prefix_cache_hit_rate": round(_f(prof.get("vllm_prefix_cache_hit_rate")), 4) or None,
        "scheduling_overhead_pct": round(_f(prof.get("scheduling_control_overhead_pct")), 2) or None,
        "submit_s": round(_f(prof.get("submit_s")), 3) or None,
        "gpu": _gpu_csv_summary(resource),
    }
    if evidence.is_file():
        out["n_evidence_rows"] = sum(1 for _ in csv.DictReader(evidence.open(encoding="utf-8")))
    return out


def _gpu_csv_summary(resource_csv: Path) -> dict:
    """Mean util/power per gpu_index (cell_instrumentation layout) OR aggregated
    (profiler layout: one row per sample, single util/power)."""
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
            if u == u:  # not NaN
                slot["util"].append(u)
            if p == p:
                slot["power"].append(p)
    out = {"status": "ok", "n_samples": n_samples}
    for key, slot in sorted(by_gpu.items()):
        out[f"{key}_util_mean"] = round(statistics.mean(slot["util"]), 1) if slot["util"] else None
        out[f"{key}_power_mean"] = round(statistics.mean(slot["power"]), 1) if slot["power"] else None
    return out


def aggregate(ramp_root: Path) -> dict:
    result: dict[str, object] = {}
    for scale_dir in sorted(ramp_root.glob("scale_*")):
        try:
            rows = int(scale_dir.name.split("_")[1])
        except (IndexError, ValueError):
            continue
        cells: dict[str, dict] = {}
        for cell in sorted(scale_dir.iterdir()):
            if not cell.is_dir():
                continue
            name = cell.name  # <arm>_c<K>_rep<R> or project_static_K<K>_rep<R>
            arm = "project_static" if name.startswith("project_static") else name.split("_c")[0]
            if arm in ("bounded_http", "duckdb_ai"):
                metrics = _gate_cell_metrics(cell)
            elif arm == "project_static":
                metrics = _project_cell_metrics(cell)
            else:
                continue
            cells.setdefault(arm, {}).update(metrics)
            cells[arm]["cell"] = name
        result[f"scale_{rows}"] = {"rows": rows, "arms": cells}
    return result


def _md(result: dict) -> str:
    lines = ["# 多卡 scale-ramp 聚合（c=32/K=32 固定，规模 4096→8192→10570）", ""]
    lines.append("| scale | arm | service tok/s | rows/s | TTFT P50 | E2E P50 | E2E P95 | prefix-hit | GPU0 util | GPU1 util | gpu_samples |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for key in sorted(result):
        scale = result[key]
        for arm, m in sorted(scale["arms"].items()):
            ttft = m.get("ttft_s_p50")
            ttft_s = f"{ttft*1000:.1f}ms" if ttft else "—"
            gpu = m.get("gpu", {})
            g0 = gpu.get("gpu0_util_mean") or gpu.get("gpu_aggregated_util_mean") or "—"
            g1 = gpu.get("gpu1_util_mean", "—")
            nsamp = gpu.get("n_samples", "—")
            hit = m.get("prefix_cache_hit_rate")
            hit_s = f"{hit:.2f}" if hit is not None else "—"
            lines.append(
                f"| {scale['rows']} | {arm} | {m.get('service_tokens_per_s','—')} | "
                f"{m.get('rows_per_s','—')} | {ttft_s} | {m.get('request_e2e_s_p50','—')} | "
                f"{m.get('request_e2e_s_p95','—')} | {hit_s} | {g0} | {g1} | {nsamp} |"
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
