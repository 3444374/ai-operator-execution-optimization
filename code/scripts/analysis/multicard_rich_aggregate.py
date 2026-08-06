#!/usr/bin/env python3
"""Reproducible aggregator for the multicard SQuAD rich-metric comparison.

WHY THIS EXISTS
---------------
The earlier ``rich_results.json`` for the multicard comparison was produced by
an ad-hoc, never-committed script and contained three reporting errors the
codex audit caught:

1. EM/F1 used the full 10570-row SQuAD denominator with only 2048 predictions
   (15.96% instead of the correct 2048-subset 82.37%).
2. The field labelled "TTFT" was really ``submit_to_service_s`` (~1.6ms), not
   the vLLM first-token latency (P50 ~52ms).
3. The project arm read ``n_outputs=0`` because the script looked for the wrong
   evidence filename -- the 2048-row completion evidence was never joined.

This script is the committed, reproducible replacement. It reads the raw gate
shards (bounded_http / duckdb_ai), the project profiler CSVs, and the 2048-row
subset references, and computes per arm x repeat:

- ``service_tokens_per_s_unified`` -- vLLM service-counter token delta / model
  serving wall. For gate arms: sum over shards of ``service_total_tokens_delta``
  divided by the max shard ``jct_s`` (shards run concurrently). For project:
  ``(vllm_prompt+generation_tokens_delta) / model_request_wall_s``. This is the
  only throughput number comparable across arms; the project CSV's own
  ``tokens_per_s`` (delta / e2e) and ``operator_tokens_per_s`` (delta /
  operator_wall) are reported separately for transparency.
- ``correct_rows`` and EM/F1 with BOTH denominators (2048 subset = headline;
  10570 full = the earlier mis-denominated value, kept for audit).
- per-request E2E P50/P95/P99 (gate: shard ``latency_p*``; project:
  ``request_e2e_s_p*``).
- TTFT P50/P95/P99 (project: ``vllm_time_to_first_token_p*_s``; gate arms: read
  from the shard summary IF the B1 histogram stamp is present, else null with a
  ``pending_rerun`` marker -- the existing pre-B1 raw does not have it).
- project scheduling overhead / submit / writeback / scan spans.
- GPU util/power (project: from the resource CSV; gate arms: from the shard
  summary IF stamped by the B1 sampler, else null).

It does NOT re-time anything; it only re-derives metrics from committed raw.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path
from typing import Mapping

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.observability.metrics.squad import squad_quality_metrics  # noqa: E402


GATE_ARMS = ("bounded_http", "duckdb_ai")
REPEAT_TAGS = ("formal0", "formal1", "formal2")  # warmup excluded from headline
SQUAD_FULL_DENOMINATOR = 10570  # full SQuAD v1.1 dev short-answer workload rows


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_references(path: Path) -> dict[str, list[str]]:
    """doc_id (str) -> reference_answers list, for the manifest subset only."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {str(doc_id): list(entry["reference_answers"]) for doc_id, entry in raw.items()}


def _predictions_from_requests_csv(shard_paths: list[Path]) -> dict[str, str]:
    """Join shard requests.csv output_text into {doc_id: output_text}."""
    predictions: dict[str, str] = {}
    for shard in shard_paths:
        with shard.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                doc_id = str(row["doc_id"])
                output_text = row.get("output_text") or ""
                predictions[doc_id] = output_text
    return predictions


def _predictions_from_evidence_csv(path: Path) -> dict[str, str]:
    """project2_*_evidence.csv -> {doc_id: output_text}."""
    predictions: dict[str, str] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            predictions[str(row["doc_id"])] = row.get("output_text") or ""
    return predictions


def _quality(predictions: Mapping[str, str], references: Mapping[str, list[str]]) -> dict:
    """SQuAD EM/F1 with the correct subset denominator (len(references))."""
    result = squad_quality_metrics(
        {doc_id: (text or None) for doc_id, text in predictions.items()},
        references,
    )
    correct_rows = int(result["squad_exact_match_rows"])
    subset_n = len(references)
    em_subset = 100.0 * correct_rows / subset_n
    # The evaluator normalizes over len(references) == subset_n already, so its
    # F1 percent IS the subset value. The "full" variants below re-express the
    # same numerator over the 10570-row workload for audit parity with the
    # earlier mis-denominated report.
    f1_subset = float(result["squad_token_f1_percent"])
    f1_sum = f1_subset * subset_n / 100.0
    return {
        "correct_rows": correct_rows,
        "em_pct_subset": em_subset,
        "f1_pct_subset": f1_subset,
        "em_pct_full": 100.0 * correct_rows / SQUAD_FULL_DENOMINATOR,
        "f1_pct_full": 100.0 * f1_sum / SQUAD_FULL_DENOMINATOR,
        "evaluated_rows": subset_n,
        "observed_prediction_rows": int(result["squad_prediction_rows"]),
        "missing_prediction_rows": int(result["squad_missing_prediction_rows"]),
    }


def _f(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _aggregate_gate_arm(
    raw_dir: Path, arm: str, references: Mapping[str, list[str]]
) -> list[dict]:
    rows = []
    for tag in REPEAT_TAGS:
        cell = raw_dir / f"{arm}_{tag}" / arm
        shard_dirs = [cell / "shard_0", cell / "shard_1"]
        if not all(d.is_dir() for d in shard_dirs):
            continue
        summaries = [_read_json(d / "summary.json") for d in shard_dirs]
        predictions = _predictions_from_requests_csv([d / "requests.csv" for d in shard_dirs])
        total_tokens = sum(s.get("service_total_tokens_delta", 0) for s in summaries)
        max_jct = max(_f(s.get("jct_s")) for s in summaries) or 0.0
        unified_tps = total_tokens / max_jct if max_jct > 0 else 0.0
        # latency_p* is per-request within a shard; report the slower shard's p*
        # (shards run concurrently; the cell's tail is the max across shards).
        latency = {
            q: max(_f(s.get(f"latency_{q}_s")) for s in summaries)
            for q in ("p50", "p95", "p99")
        }
        # TTFT only present after the B1 /metrics-histogram stamp on gate summaries.
        ttft = {
            q: _f(summaries[0].get(f"vllm_time_to_first_token_{q}_s"))
            for q in ("p50", "p95", "p99")
        }
        ttft_status = "ok" if any(ttft.values()) else "pending_rerun:no_histogram_in_summary"
        gpu = {
            "gpu0_util_mean": _f(summaries[0].get("gpu0_utilization_pct_mean")),
            "gpu1_util_mean": _f(summaries[0].get("gpu1_utilization_pct_mean")),
        }
        rows.append(
            {
                "arm": arm,
                "tag": tag,
                "service_tokens_per_s_unified": round(unified_tps, 1),
                "service_total_tokens": int(total_tokens),
                "model_serving_wall_s": round(max_jct, 3),
                "correct_rows_per_s": round(
                    _quality(predictions, references)["correct_rows"] / max_jct, 2
                ) if max_jct > 0 else 0.0,
                **_quality(predictions, references),
                "request_e2e_s_p50": round(latency["p50"], 3),
                "request_e2e_s_p95": round(latency["p95"], 3),
                "request_e2e_s_p99": round(latency["p99"], 3),
                "ttft_s_p50": round(ttft["p50"], 4) if ttft["p50"] else None,
                "ttft_s_p95": round(ttft["p95"], 4) if ttft["p95"] else None,
                "ttft_s_p99": round(ttft["p99"], 4) if ttft["p99"] else None,
                "ttft_status": ttft_status,
                "prefix_cache_hit_rate": _f(summaries[0].get("service_prefix_cache_hit_rate")),
                "gpu_util_mean": gpu,
                "vllm_running_final": int(summaries[0].get("vllm_num_requests_running_final", 0)),
                "vllm_waiting_final": int(summaries[0].get("vllm_num_requests_waiting_final", 0)),
            }
        )
    return rows


def _aggregate_project_arm(raw_dir: Path, references: Mapping[str, list[str]]) -> list[dict]:
    rows = []
    for tag in REPEAT_TAGS:
        profiler_csv = raw_dir / f"project2_{tag}.csv"
        evidence_csv = raw_dir / f"project2_{tag}_evidence.csv"
        resource_csv = raw_dir / f"project2_{tag}_resource.csv"
        if not profiler_csv.is_file() or not evidence_csv.is_file():
            continue
        with profiler_csv.open(encoding="utf-8") as handle:
            prof = next(csv.DictReader(handle))
        predictions = _predictions_from_evidence_csv(evidence_csv)
        prompt_delta = _f(prof.get("vllm_prompt_tokens_delta"))
        gen_delta = _f(prof.get("vllm_generation_tokens_delta"))
        total_service_tokens = prompt_delta + gen_delta
        model_request_wall = _f(prof.get("model_request_wall_s"))
        unified_tps = (
            total_service_tokens / model_request_wall if model_request_wall > 0 else 0.0
        )
        # GPU util/power from the resource CSV (mean across samples, both GPUs).
        gpu = _resource_summary(resource_csv)
        rows.append(
            {
                "arm": "project_static",
                "tag": tag,
                "service_tokens_per_s_unified": round(unified_tps, 1),
                "service_total_tokens": int(total_service_tokens),
                "model_serving_wall_s": round(model_request_wall, 3),
                # project also exposes two alternative denominators -- report them
                "tokens_per_s_e2e_denom": round(_f(prof.get("tokens_per_s")), 1),
                "operator_tokens_per_s": round(_f(prof.get("operator_tokens_per_s")), 1),
                "correct_rows_per_s": round(
                    _quality(predictions, references)["correct_rows"]
                    / _f(prof.get("operator_wall_s")), 2
                ),
                **_quality(predictions, references),
                "request_e2e_s_p50": round(_f(prof.get("request_e2e_s_p50")), 3),
                "request_e2e_s_p95": round(_f(prof.get("request_e2e_s_p95")), 3),
                "request_e2e_s_p99": round(_f(prof.get("request_e2e_s_p99")), 3),
                "ttft_s_p50": round(_f(prof.get("vllm_time_to_first_token_p50_s")), 4),
                "ttft_s_p95": round(_f(prof.get("vllm_time_to_first_token_p95_s")), 4),
                "ttft_s_p99": round(_f(prof.get("vllm_time_to_first_token_p99_s")), 4),
                "ttft_status": "ok",
                "submit_to_service_s_p50": round(_f(prof.get("submit_to_service_s_p50")), 4)
                if prof.get("submit_to_service_s_p50") else None,
                "submit_s": round(_f(prof.get("submit_s")), 3),
                "operator_wall_s": round(_f(prof.get("operator_wall_s")), 3),
                "e2e_s": round(_f(prof.get("e2e_s")), 3),
                "db_fetch_s": round(_f(prof.get("db_fetch_s")), 3),
                "writeback_s": round(_f(prof.get("writeback_s")), 3),
                "scheduling_control_overhead_pct": round(
                    _f(prof.get("scheduling_control_overhead_pct")), 2
                ),
                "prefix_cache_hit_rate": _f(prof.get("vllm_prefix_cache_hit_rate")),
                "gpu_util_mean": gpu,
                "vllm_running_mean": round(_f(prof.get("vllm_num_requests_running_mean")), 1)
                if prof.get("vllm_num_requests_running_mean") else None,
            }
        )
    return rows


def _resource_summary(resource_csv: Path) -> dict:
    """Mean GPU util/power across the resource-trace samples, per GPU."""
    if not resource_csv.is_file():
        return {"status": "missing"}
    by_gpu: dict[str, dict[str, list[float]]] = {}
    with resource_csv.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            gpu = row.get("gpu_name") or row.get("gpu_id") or "gpu"
            slot = by_gpu.setdefault(gpu, {"util": [], "power": []})
            slot["util"].append(_f(row.get("gpu_utilization_pct")))
            slot["power"].append(_f(row.get("gpu_power_w")))
    out = {"status": "ok", "n_samples": max((len(v["util"]) for v in by_gpu.values()), default=0)}
    for gpu, slot in sorted(by_gpu.items()):
        short = "gpu0" if "0" in gpu or len(by_gpu) == 2 else gpu
        if list(sorted(by_gpu)).index(gpu) == 1 and len(by_gpu) == 2:
            short = "gpu1"
        out[f"{short}_util_mean"] = round(statistics.mean(slot["util"]), 1) if slot["util"] else None
        out[f"{short}_power_mean"] = round(statistics.mean(slot["power"]), 1) if slot["power"] else None
    return out


def _mean_cv(values: list[float]) -> tuple[float, float]:
    if not values:
        return (0.0, 0.0)
    mean = statistics.mean(values)
    stdev = statistics.pstdev(values) if len(values) > 1 else 0.0
    cv = abs(stdev / mean) if mean else 0.0
    return (mean, cv)


def _summary_block(arm_rows: list[dict]) -> dict:
    """Mean + CV across the formal repeats for the headline metrics."""
    if not arm_rows:
        return {}
    tps_values = [r["service_tokens_per_s_unified"] for r in arm_rows]
    em_values = [r["em_pct_subset"] for r in arm_rows]
    correct_values = [r["correct_rows"] for r in arm_rows]
    mean_tps, cv_tps = _mean_cv(tps_values)
    return {
        "n_repeats": len(arm_rows),
        "service_tokens_per_s_mean": round(mean_tps, 1),
        "service_tokens_per_s_cv_pct": round(cv_tps * 100, 2),
        "service_tokens_per_s_reps": [round(v, 1) for v in tps_values],
        "em_pct_subset_mean": round(statistics.mean(em_values), 2),
        "correct_rows_mean": round(statistics.mean(correct_values), 1),
    }


def aggregate(raw_dir: Path, references_path: Path) -> dict:
    references = _load_references(references_path)
    result: dict[str, object] = {"repeat_tags": list(REPEAT_TAGS), "reference_rows": len(references)}
    per_arm: dict[str, list[dict]] = {}
    for arm in GATE_ARMS:
        per_arm[arm] = _aggregate_gate_arm(raw_dir, arm, references)
    per_arm["project_static"] = _aggregate_project_arm(raw_dir, references)
    result["per_repeat"] = per_arm
    result["summary"] = {arm: _summary_block(rows) for arm, rows in per_arm.items() if rows}
    return result


def _md(result: dict) -> str:
    lines = [
        "# 多卡 rich-metric 聚合（committed aggregator 重算，子集 2048 分母）",
        "",
        f"reference rows: {result['reference_rows']}（SQuAD 2048 子集，committed `squad_eq2048_references.json`）。",
        "",
        "## per-repeat service tokens/s（unified = vLLM service-counter token delta ÷ model serving wall）",
        "",
        "| arm | tag | unified tok/s | model_serving_wall_s | EM%(2048) | correct_rows | req E2E P50 | TTFT P50 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for arm, rows in result["per_repeat"].items():
        for r in rows:
            ttft = r.get("ttft_s_p50")
            ttft_s = f"{ttft*1000:.1f}ms" if ttft else r.get("ttft_status", "—")
            lines.append(
                f"| {arm} | {r['tag']} | {r['service_tokens_per_s_unified']} | "
                f"{r['model_serving_wall_s']} | {r['em_pct_subset']:.2f} | "
                f"{r['correct_rows']} | {r['request_e2e_s_p50']} | {ttft_s} |"
            )
    lines += ["", "## summary (mean across formal repeats)", ""]
    lines.append("| arm | unified tok/s mean | CV% | EM%(2048) | correct_rows |")
    lines.append("|---|---|---|---|---|")
    for arm, s in result["summary"].items():
        lines.append(
            f"| {arm} | {s['service_tokens_per_s_mean']} | {s['service_tokens_per_s_cv_pct']} | "
            f"{s['em_pct_subset_mean']} | {s['correct_rows_mean']} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--raw-dir", required=True, type=Path,
                   help="dir containing {arm}_{tag}/ gate cells + project2_*.csv")
    p.add_argument("--references", required=True, type=Path,
                   help="squad_eq2048_references.json (doc_id -> reference_answers)")
    p.add_argument("--output-json", required=True, type=Path)
    p.add_argument("--output-md", type=Path)
    args = p.parse_args()

    result = aggregate(args.raw_dir, args.references)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"wrote {args.output_json}", file=sys.stderr)
    if args.output_md:
        args.output_md.write_text(_md(result), encoding="utf-8")
        print(f"wrote {args.output_md}", file=sys.stderr)
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
