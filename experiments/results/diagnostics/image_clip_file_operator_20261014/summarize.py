#!/usr/bin/env python3
"""Recompute the retained cold-operator summaries, without SDKs or a model."""
import hashlib
import json
from pathlib import Path
import statistics


def summarize(root):
    records = [json.loads(line) for line in (root / "raw/measurements.jsonl").read_text().splitlines()]
    provenance = json.loads((root / "provenance.json").read_text())
    mapping = {item["public_record"].split("#")[1]: item for item in provenance["measurement_mapping"]}
    assert len(records) == 15 and len({item["run_id"] for item in records}) == 15
    for item in records:
        encoded = (json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        assert hashlib.sha256(encoded).hexdigest() == mapping[item["run_id"]]["public_record_sha256"]
        row = item["row"]
        assert row["exactly_once"] and row["output_rows"] == row["rows"]
        assert row["declared_total_cpus"] == 7 and row["gpu_workers"] == 1
        assert row["detailed_stage_timing"] is False
    repeated = [item for item in records if item["group"] == "repeat"]
    assert [item["label"] for item in repeated] == ["direct", "hse", "ray_data", "hse", "ray_data", "direct", "ray_data", "direct", "hse"]
    result = {"schema_version": 1, "measurement_count": len(records), "timed_rows": sum(item["row"]["rows"] for item in records), "warmup_rows": len(records) * 64, "summary_scope": "nine cold-operator repetitions only; qualification/scale retained separately", "arms": {}}
    for label in ("direct", "hse", "ray_data"):
        selected = [item for item in repeated if item["label"] == label]
        rows = [item["row"] for item in selected]
        values = [row["operator_e2e_s"] for row in rows]
        assert len(values) == 3
        result["arms"][label] = {
            "run_ids": [item["run_id"] for item in selected],
            "operator_e2e_s": values,
            "median_operator_e2e_s": statistics.median(values),
            "sample_stdev_s": statistics.stdev(values),
            "sample_cv": statistics.stdev(values) / statistics.mean(values),
            "first_output_s": [row["first_output_s"] for row in rows],
            "embedding_checksum": [row["embedding_checksum"] for row in rows],
            "embedding_sum_all": [row["embedding_sum_all"] for row in rows],
            "embedding_digest_xor_rounded5": [row["embedding_digest_xor_rounded5"] for row in rows],
            "max_norm_error": max(row["max_norm_error"] for row in rows),
        }
    a, b = result["arms"]["direct"], result["arms"]["ray_data"]
    result["ray_data_minus_direct"] = {
        "first_column_sum": b["embedding_checksum"][0] - a["embedding_checksum"][0],
        "all_component_sum": b["embedding_sum_all"][0] - a["embedding_sum_all"][0],
        "per_row_vectors_available": False,
        "pointwise_equivalence": "unverified",
    }
    return result


if __name__ == "__main__":
    print(json.dumps(summarize(Path(__file__).resolve().parent), ensure_ascii=False, indent=2))
