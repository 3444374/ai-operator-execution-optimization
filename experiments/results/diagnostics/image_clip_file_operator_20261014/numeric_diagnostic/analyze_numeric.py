#!/usr/bin/env python3
"""Offline, ID-aligned numeric and producer/consumer fingerprint diagnosis."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

import numpy as np


def sha(value, dtype="<f4"):
    return hashlib.sha256(np.asarray(value, dtype=dtype).tobytes(order="C")).hexdigest()


def load_arm(root, label, manifest):
    with np.load(root / "diagnostic" / (label + ".npz"), allow_pickle=False) as data:
        ids = tuple(str(value) for value in data["doc_ids"])
        values = np.asarray(data["embeddings"], dtype=np.float32)
    assert values.shape == (257, 512) and len(set(ids)) == 257 and np.isfinite(values).all()
    expected = {item["doc_id"] for item in manifest["entries"][:257]}
    assert set(ids) == expected
    by_id = {doc_id: values[index] for index, doc_id in enumerate(ids)}
    events = []
    for path in sorted((root / "diagnostic" / (label + "-trace")).glob("*.jsonl")):
        events.extend(json.loads(line) for line in path.read_text().splitlines())
    starts = [item["time_ns"] for item in events if item["kind"] == "driver_boundary" and item["event"] == "start" and item["stage"] == "formal"]
    ends = [item["time_ns"] for item in events if item["kind"] == "driver_boundary" and item["event"] == "end" and item["stage"] == "formal"]
    assert len(starts) == len(ends) == 1 and starts[0] < ends[0]
    selected = [item for item in events if starts[0] <= item["time_ns"] <= ends[0]]
    model = sorted([item for item in selected if item["kind"] == "gpu_model"], key=lambda item: item["started_ns"])
    prep = [item for item in selected if item["kind"] == "cpu_preprocess"]
    seen, pixels, half, gpu_outputs, model_batch = set(), {}, {}, {}, {}
    for index, batch in enumerate(model):
        assert batch["rows"] == len(batch["doc_ids"]) and batch["model_dtype"] == "torch.float16"
        assert batch["shape"] == [batch["rows"], 3, 224, 224]
        assert batch["output_shape"] == [batch["rows"], 512] and batch["output_dtype"] == "float32"
        assert not seen.intersection(batch["doc_ids"])
        seen.update(batch["doc_ids"])
        for item, output_sha in zip(batch["input_fingerprints"], batch["output_fp32_sha256"], strict=True):
            doc_id = item["doc_id"]
            pixels[doc_id] = item["pixel_fp32_sha256"]
            half[doc_id] = item["cpu_cast_fp16_sha256"]
            gpu_outputs[doc_id] = output_sha
            model_batch[doc_id] = {"index": index, "rows": batch["rows"]}
    assert seen == expected
    assert all(gpu_outputs[doc_id] == sha(by_id[doc_id]) for doc_id in expected)
    source = {item["sha256"]: item for item in manifest["entries"][:257]}
    assert len(source) == 257, "encoded-content alias needs explicit multiset handling"
    prepare_seen = set()
    for batch in prep:
        assert batch["shape"] == [batch["rows"], 3, 224, 224] and batch["dtype"] == "float32"
        for item in batch["items"]:
            original = source[item["encoded_sha256"]]
            doc_id = original["doc_id"]
            assert doc_id not in prepare_seen and item["encoded_bytes"] == original["image_bytes"]
            assert item["pixel_fp32_sha256"] == pixels[doc_id]
            prepare_seen.add(doc_id)
    assert prepare_seen == expected
    # Includes both warmup and formal calls, independently checks the963 budget.
    all_model = [item for item in events if item["kind"] == "gpu_model"]
    assert sum(item["rows"] for item in all_model) == 321
    schema = [item for item in selected if item["kind"] == "arrow_schema"]
    conversion = [item for item in selected if item["kind"] == "arrow_to_pylist"]
    return {"by_id": by_id, "pixels": pixels, "half": half, "model_batch": model_batch,
            "metadata": {"formal_model_batch_rows": [item["rows"] for item in model],
                         "formal_preprocess_batch_rows": sorted(item["rows"] for item in prep),
                         "all_model_images_including_warmup": sum(item["rows"] for item in all_model),
                         "encoded_content_and_bytes_match_shared_manifest": True,
                         "prepared_pixels_equal_model_inputs_per_id": True,
                         "GPU_return_values_equal_captured_consumer_FP32_per_id": True,
                         "native_arrow_schema": schema, "native_arrow_to_pylist": conversion,
                         "native_arrow_to_pylist_total_s": sum(item["seconds"] for item in conversion),
                         "numpy_asarray_conversion_s": None,
                         "numpy_asarray_reason": "original call remains unmodified and unmeasured"}}


def compare(a, b, ids):
    x = np.asarray([a["by_id"][doc_id] for doc_id in ids], dtype=np.float64)
    y = np.asarray([b["by_id"][doc_id] for doc_id in ids], dtype=np.float64)
    error = np.abs(x-y)
    cosine = np.sum(x*y, axis=1)/(np.linalg.norm(x, axis=1)*np.linalg.norm(y, axis=1))
    bitwise = np.all(x.astype(np.float32).view(np.uint32) == y.astype(np.float32).view(np.uint32), axis=1)
    pixels = np.asarray([a["pixels"][doc_id] == b["pixels"][doc_id] for doc_id in ids])
    half = np.asarray([a["half"][doc_id] == b["half"][doc_id] for doc_id in ids])
    summary = {"rows": len(ids), "bitwise_equal_rows": int(bitwise.sum()),
               "input_FP32_identical_rows": int(pixels.sum()), "CPU_cast_FP16_identical_rows": int(half.sum()),
               "max_abs_error": float(error.max()), "mean_abs_error": float(error.mean()),
               "max_row_l2_error": float(np.linalg.norm(x-y, axis=1).max()),
               "per_row_max_abs_percentiles": {str(q): float(np.percentile(error.max(axis=1), q)) for q in (0, 1, 50, 99, 100)},
               "cosine_percentiles": {str(q): float(np.percentile(cosine, q)) for q in (0, 1, 50, 99, 100)},
               "explicit_absolute_tolerance_counts": {str(atol): {"components_outside": int((error > atol).sum()), "rows_outside": int((error.max(axis=1) > atol).sum())} for atol in (0, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3)},
               "tolerance_purpose": "descriptive error-scale counts only; no new overall pass gate",
               "performance_eligible": False}
    return summary, (error.max(axis=1), np.linalg.norm(x-y, axis=1), cosine, bitwise, pixels, half)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-root", required=True, type=Path)
    parser.add_argument("--input-manifest", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.input_manifest.read_text())
    arms = {label: load_arm(args.evidence_root, label, manifest) for label in ("direct", "hse", "ray_data")}
    ids = sorted(arms["direct"]["by_id"])
    args.out_dir.mkdir(parents=True, exist_ok=False)
    comparisons, per_row = {}, {}
    for label in ("hse", "ray_data"):
        comparisons[label], per_row[label] = compare(arms["direct"], arms[label], ids)
    with (args.out_dir / "per_id.csv").open("x", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["doc_id", "direct_model_batch_rows", "hse_model_batch_rows", "ray_data_model_batch_rows"] + [f"direct_vs_{label}_{field}" for label in ("hse", "ray_data") for field in ("max_abs_error", "l2_error", "cosine", "bitwise_equal", "input_fp32_identical", "cpu_cast_fp16_identical")])
        for index, doc_id in enumerate(ids):
            writer.writerow([doc_id] + [arms[label]["model_batch"][doc_id]["rows"] for label in ("direct", "hse", "ray_data")] + [array[index].item() for label in ("hse", "ray_data") for array in per_row[label]])
    output = {"scope": "one fixed257-image diagnostic each plus64 warmup each; original15 primary measurements unchanged", "total_model_images_verified": 963,
              "arms": {label: value["metadata"] for label, value in arms.items()}, "direct_comparisons": comparisons,
              "inference_limits": ["Matching fingerprints excludes observed source/preprocessing/consumer changes only on this257 subset.", "If identical per-ID inputs yield differing GPU-return vectors under the unchanged FP16 kernel, difference is localized to model computation/output; batch shapes may explain it but no causal replay was run.", "No dtype/batch change or additional model call to force equality; no automatic semantic or5000-row equivalence verdict."]}
    (args.out_dir / "numeric-analysis.json").write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps({"model_images": output["total_model_images_verified"], "comparisons": comparisons}, indent=2))


if __name__ == "__main__":
    main()
