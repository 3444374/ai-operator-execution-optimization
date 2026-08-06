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

    ``system_comparison_role`` is the AUTHORITATIVE primary role of the composed
    system under test (复审 #1); ``comparison_role`` is the single-shard component
    role. All None when no identity.json (old raw) -- the aggregate then shows
    None rather than a misleading product-native component role.
    """
    ident = _identity(cell)
    return {
        "comparison_role": ident.get("comparison_role"),
        "system_comparison_role": ident.get("system_comparison_role"),
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
    # ttft/prefix_hit reuse ttft_eps read above (vLLM /metrics per-backend deltas)
    ttft = {"p50": None, "p95": None, "p99": None}
    prefix_hit = None
    if ttft_eps:
        for q in ttft:
            vals = [_f(e.get(f"vllm_time_to_first_token_{q}_s")) for e in ttft_eps]
            vals = [v for v in vals if v > 0]
            if vals:
                ttft[q] = statistics.mean(vals)
        hits = [_f(e.get("vllm_prefix_cache_hit_rate")) for e in ttft_eps]
        hits = [h for h in hits if h > 0]
        if hits:
            prefix_hit = statistics.mean(hits)
    return {
        "status": status,
        "service_tokens_per_s": round(unified_tps, 1) if unified_tps is not None else None,
        "service_total_tokens": service_total,
        "model_serving_wall_s": round(service_wall, 3),
        "service_tokens_source": token_source,
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
        agg["comparison_role"] = passed[0].get("comparison_role")
        agg["system_comparison_role"] = passed[0].get("system_comparison_role")
        agg["formal_baseline_eligible"] = passed[0].get("formal_baseline_eligible")
        agg["scheduler_owner"] = passed[0].get("scheduler_owner")
        agg["service_tokens_source"] = passed[0].get("service_tokens_source")
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
