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
_MEAN_CV_METRICS = (
    "service_tokens_per_s", "rows_per_s", "model_serving_wall_s",
    "request_e2e_s_p50", "request_e2e_s_p95", "request_e2e_s_p99",
    "ttft_s_p50", "ttft_s_p95", "ttft_s_p99", "prefix_cache_hit_rate",
    "scheduling_overhead_pct",
)


def _f(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _completed_rows(shard_dirs: list[Path]) -> int:
    """Count request rows with status=completed across both shards."""
    n = 0
    for shard in shard_dirs:
        req = shard / "requests.csv"
        if not req.is_file():
            continue
        with req.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                if (row.get("status") or "").lower() == "completed":
                    n += 1
    return n


def _gate_cell_metrics(cell: Path) -> dict:
    """bounded_http/duckdb_ai cell -> ramp metrics."""
    gate_dir = next((c for c in (cell / "gate_output").iterdir() if c.is_dir()), None)
    if gate_dir is None:
        return {"status": "missing_gate_output"}
    shard_dirs = [gate_dir / "shard_0", gate_dir / "shard_1"]
    summaries = [_read_json(s / "summary.json") for s in shard_dirs if (s / "summary.json").is_file()]
    if len(summaries) != 2:
        return {"status": f"missing_shards ({len(summaries)}/2)"}
    # Authoritative status: run_status.json (written by run_core_gate on pass/fail).
    run_status_path = cell / "gate_output" / "run_status.json"
    status = "unknown"
    if run_status_path.is_file():
        status = str(_read_json(run_status_path).get("status", "unknown"))
    if (cell / "run_error.json").is_file():
        status = "failed"
    total_tokens = sum(s.get("service_total_tokens_delta", 0) for s in summaries)
    max_jct = max(_f(s.get("jct_s")) for s in summaries)
    unified_tps = total_tokens / max_jct if max_jct > 0 else 0.0
    completed = _completed_rows(shard_dirs)
    rows_per_s = completed / max_jct if max_jct > 0 else 0.0
    latency = {q: max(_f(s.get(f"latency_{q}_s")) for s in summaries) for q in ("p50", "p95", "p99")}
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
        "status": status,
        "service_tokens_per_s": round(unified_tps, 1),
        "service_total_tokens": int(total_tokens),
        "model_serving_wall_s": round(max_jct, 3),
        "completed_rows": completed,
        "rows_per_s": round(rows_per_s, 2),
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
    out = {
        "status": "passed",
        "service_tokens_per_s": round(_f(prof.get("model_request_tokens_per_s")), 1),
        "service_total_tokens": int(_f(prof.get("vllm_prompt_tokens_delta")) + _f(prof.get("vllm_generation_tokens_delta"))),
        "model_serving_wall_s": round(_f(prof.get("model_request_wall_s")), 3),
        "rows_per_s": round(_f(prof.get("rows_per_s")), 2),
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
    cv = (statistics.pstdev(vals) / mean) if (len(vals) > 1 and mean) else 0.0
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
        if "scheduling_overhead_pct" in passed[0]:
            agg["scheduling_overhead_pct_mean"] = passed[0].get("scheduling_overhead_pct")
    if any(r.get("status") != "passed" for r in reps):
        agg["failed_rep_errors"] = [r.get("error", r.get("status")) for r in reps if r.get("status") != "passed"]
    return agg


def aggregate(ramp_root: Path) -> dict:
    # Authoritative per-cell status from the driver's run log.
    status_map: dict[tuple[int, str, int], tuple[str, str]] = {}
    run_json = ramp_root / "ramp_run.json"
    if run_json.is_file():
        for r in _read_json(run_json).get("records", []):
            try:
                status_map[(int(r["scale"]), str(r["arm"]), int(r["rep"]))] = (
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
        per_arm_reps: dict[str, list[dict]] = {}
        for cell in sorted(scale_dir.iterdir()):
            if not cell.is_dir():
                continue
            name = cell.name
            arm = "project_static" if name.startswith("project_static") else name.split("_c")[0]
            try:
                rep = int(name.rsplit("_rep", 1)[1])
            except (IndexError, ValueError):
                rep = 1
            if arm in GATE_ARMS:
                metrics = _gate_cell_metrics(cell)
            elif arm == "project_static":
                metrics = _project_cell_metrics(cell)
            else:
                continue
            # Driver's run log is authoritative for status.
            key = (rows, arm, rep)
            if key in status_map:
                metrics["status"], metrics["error"] = status_map[key]
            metrics["rep"] = rep
            per_arm_reps.setdefault(arm, []).append(metrics)
        arms = {arm: _aggregate_reps(reps) for arm, reps in per_arm_reps.items() if reps}
        result[f"scale_{rows}"] = {"rows": rows, "arms": arms}
    return result


def _fmt(v) -> str:
    return "—" if v is None else str(v)


def _md(result: dict) -> str:
    scales = sorted(int(k.split("_")[1]) for k in result)
    title_range = f"{scales[0]}→{scales[-1]}" if scales else "?"
    lines = [f"# 多卡 scale-ramp 聚合（规模 {title_range}，mean across passed reps）", ""]
    lines.append("| scale | arm | status | tok/s mean | tok/s CV | rows/s | TTFT P50 | E2E P50 | prefix-hit | GPU0 util | GPU1 util | n_passed/n |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for key in sorted(result, key=lambda k: int(k.split("_")[1])):
        scale = result[key]
        for arm, m in sorted(scale["arms"].items()):
            ttft = m.get("ttft_s_p50_mean")
            ttft_s = f"{ttft*1000:.1f}ms" if ttft else "—"
            gpu = m.get("gpu", {})
            g0 = gpu.get("gpu0_util_mean") or gpu.get("gpu_aggregated_util_mean") or "—"
            g1 = gpu.get("gpu1_util_mean", "—")
            hit = m.get("prefix_cache_hit_rate_mean")
            hit_s = f"{hit:.2f}" if hit is not None else "—"
            lines.append(
                f"| {scale['rows']} | {arm} | {m.get('status','—')} | "
                f"{_fmt(m.get('service_tokens_per_s_mean'))} | {m.get('service_tokens_per_s_cv_pct','—')}% | "
                f"{_fmt(m.get('rows_per_s_mean'))} | {ttft_s} | {_fmt(m.get('request_e2e_s_p50_mean'))} | "
                f"{hit_s} | {g0} | {g1} | {m.get('n_passed','?')}/{m.get('n_reps','?')} |"
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
