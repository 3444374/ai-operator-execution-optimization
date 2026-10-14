"""Recompute complete vectors, original row IDs and native top-K from compact evidence."""
from pathlib import Path
import gzip
import hashlib
import json

import numpy as np


def verify(directory):
    directory = Path(directory)
    evidence = json.loads(gzip.decompress((directory / "observations.json.gz").read_bytes()))
    mapping = json.loads((directory / "vector_map.json").read_text())["runs"]
    manifest = json.loads((directory / "source_manifest.json").read_text())["entries"]
    arrays = np.load(directory / "vectors.npz", allow_pickle=False)
    for name in arrays.files:
        value = arrays[name]
        assert name == "v_" + hashlib.sha256(value.tobytes()).hexdigest()
        assert value.dtype == np.float32 and value.ndim == 2 and value.shape[1] == 512
        assert np.isfinite(value).all()
        assert np.allclose(np.linalg.norm(value, axis=1), 1, atol=2e-5, rtol=0)

    def vector(run_id, call):
        record = mapping[run_id]["call_" + str(call)]
        value = arrays[record["array"]]
        assert list(value.shape) == record["shape"]
        return value

    stock_indexes = {}
    for run_id, run in evidence["runs"].items():
        if run["status"] == "succeeded" and run["mode"] != "search" and run["arm"] == "stock":
            stock_indexes[run["index_identity_sha256"]] = (run_id, run)
    checked, image_queries, text_queries, failures = 0, 0, 0, []
    max_error = 0.0
    for run_id, run in evidence["runs"].items():
        if run["status"] != "succeeded":
            failures.append(run_id)
            continue
        rows = run["gallery_rows"]
        ref_id, ref = stock_indexes[run["index_identity_sha256"]]
        reference_gallery = vector(ref_id, 0)
        gallery = reference_gallery if run["mode"] == "search" else vector(run_id, 0)
        assert gallery.shape == (rows, 512)
        assert np.allclose(gallery, reference_gallery, atol=1e-5, rtol=1e-5)
        max_error = max(max_error, float(np.max(np.abs(gallery - reference_gallery))))
        ids = [x["doc_id"] for x in manifest[:rows]]
        if run["mode"] == "search":
            assert run["index_s"] is None and run["index_load_s"] is not None
            assert not any(x["image_rows"] > 1 for x in run["rm_calls"])
        else:
            assert run["rm_calls"][0]["image_rows"] == rows
        ref_queries = {x["query_id"]: x for x in ref["queries"]}
        for query in run["queries"]:
            assert query["status"] == "succeeded" and len(query["rm_calls"]) == 1
            values = vector(run_id, query["rm_calls"][0]["call_index"])
            assert values.shape == (1, 512)
            reference = ref_queries[query["query_id"]]
            reference_values = vector(ref_id, reference["rm_calls"][0]["call_index"])
            assert np.allclose(values, reference_values, atol=1e-5, rtol=1e-5)
            scores = gallery @ values[0]
            positions = np.argsort(-scores)[:min(run["k"], rows)].tolist()
            assert positions == query["positions"] == reference["positions"]
            assert [ids[p] for p in positions] == query["doc_ids"] == reference["doc_ids"]
            assert np.allclose(scores[positions], query["scores"], atol=1e-5, rtol=1e-5)
            assert query["observed_positions"] == positions
            assert query["observed_doc_ids"] == query["doc_ids"]
            if query["kind"] == "image":
                assert query["rm_calls"][0]["image_rows"] == 1
                assert query["query_id"].removeprefix("image-") == query["doc_ids"][0]
                image_queries += 1
            else:
                assert query["rm_calls"][0]["text_rows"] == 1
                text_queries += 1
            checked += 1
    result = dict(successful_queries_checked=checked, image_queries=image_queries,
                  text_queries=text_queries, failed_runs_retained=failures,
                  recorded_preparation_failures=len(evidence["failures"]),
                  gallery_max_abs_error=max_error, unique_full_matrices=len(arrays.files))
    if (directory / "png_candidate.json").exists():
        result["png_candidate"] = verify_png_candidate(directory, evidence, mapping, arrays, manifest)
    return result


def verify_png_candidate(directory, evidence, mapping, arrays, manifest):
    """Check the separate candidate using exact restored matrices and native output."""
    candidate = json.loads((directory / "png_candidate.json").read_text())
    review_path = directory / "png_candidate_independent_review.json"
    if review_path.exists():
        review = json.loads(review_path.read_text())
        assert review["outcome"] == "passed_with_stated_scope" and not review["blocking_findings"]
        assert review["implementation_commit"] == candidate["implementation_commit"]
        assert review["inputs"]["public_candidate"]["sha256"] == hashlib.sha256(
            (directory / "png_candidate.json").read_bytes()).hexdigest()
    assert candidate["integration_status"] == "independent_candidate_not_adopted"
    assert candidate["implementation_commit"] == "eab9f11a0338ef632f208dd178edcf87cd0a9799"
    expected = ["direct-12-1", "semloom_hse-12-1", "direct-256-default",
                "semloom_hse-256-1", "semloom_hse-256-default", "direct-256-1"]
    supervision = candidate["supervision"]
    assert supervision["status"] == "succeeded" and supervision["sdk_items"] == 99
    assert supervision["lease_released"] and supervision["small_passed_before_scale"]
    assert supervision["whole_wall_s"] < supervision["whole_budget_s"] == 1200
    assert [x["label"] for x in supervision["runs"]] == ["preflight", "sdk", *expected]
    assert all(x["exit_code"] == 0 and not x["timed_out"] for x in supervision["runs"])
    assert all(x["confirmed"] and not x["live_owned_processes"] and not x["gpu_compute_pids"]
               for x in supervision["cleanup"])
    assert set(candidate["runs"]) == set(candidate["vector_map"]) == set(expected)
    counts = dict(model_cli_cells=6, complete_matrices=0, image_queries=0, text_queries=0)
    max_gallery_error, max_query_error = 0.0, 0.0
    for label, run in candidate["runs"].items():
        rows = run["gallery_rows"]
        reference_id = ("lotus-retrieval-20261014-d/stock-12" if rows == 12
                        else "lotus-retrieval-20261014-e/stock-256")
        reference = evidence["runs"][reference_id]
        assert run["index_identity_sha256"] == reference["index_identity_sha256"]
        assert run["lotus_source"] == reference["lotus_source"]
        assert run["model_dtype"] == "float32" and run["direct_model_batch_size"] == 32
        assert run["torch_thread_pools"] == {"torch_intraop_threads": 1, "torch_interop_threads": 1}
        assert run["mode"] == "combined" and run["status"] == "succeeded" and not run["native_cache"]
        assert not run["detailed_timing"] and run["tail_quantiles"]["p99"] is None
        assert run["tail_quantiles"]["p99_99"] is None
        assert len(run["rm_calls"]) == 5 and len(run["queries"]) == 4
        assert run["rm_calls"][0]["image_rows"] == rows
        records = candidate["vector_map"][label]
        assert set(records) == {"call_" + str(i) for i in range(5)}
        for record in records.values():
            assert list(arrays[record["array"]].shape) == record["shape"]
            counts["complete_matrices"] += 1
        gallery = arrays[records["call_0"]["array"]]
        ref_gallery = arrays[mapping[reference_id]["call_0"]["array"]]
        assert gallery.shape == (rows, 512) and np.allclose(gallery, ref_gallery, atol=1e-5, rtol=1e-5)
        max_gallery_error = max(max_gallery_error, float(np.max(np.abs(gallery - ref_gallery))))
        ids = [x["doc_id"] for x in manifest[:rows]]
        for query, ref_query in zip(run["queries"], reference["queries"], strict=True):
            assert query["status"] == "succeeded" and query["query_id"] == ref_query["query_id"]
            call = query["rm_calls"][0]["call_index"]
            vector = arrays[records["call_" + str(call)]["array"]]
            ref_vector = arrays[mapping[reference_id]["call_" + str(call)]["array"]]
            assert vector.shape == (1, 512) and np.allclose(vector, ref_vector, atol=1e-5, rtol=1e-5)
            max_query_error = max(max_query_error, float(np.max(np.abs(vector - ref_vector))))
            scores = gallery @ vector[0]
            positions = np.argsort(-scores)[:run["k"]].tolist()
            assert positions == query["positions"] == query["observed_positions"] == ref_query["positions"]
            assert [ids[p] for p in positions] == query["doc_ids"] == query["observed_doc_ids"] == ref_query["doc_ids"]
            assert np.allclose(scores[positions], query["scores"], atol=1e-5, rtol=1e-5)
            assert np.allclose(query["observed_scores"], query["scores"], atol=1e-5, rtol=1e-5)
            counts[query["kind"] + "_queries"] += 1
        costs = run["execution_observations"]
        assert len(costs) == 4
        for call in run["rm_calls"][:4]:
            assert call["png_compress_level"] == run["png_compress_level"]
            assert min(call["image_encode_wall_ns"], call["image_encode_cpu_ns"], call["encoded_total_bytes"]) > 0
        if run["arm"] == "semloom_hse":
            for cost in costs:
                assert cost["encoded_bytes_limit"] == 58110400
                assert cost["encoded_block_bytes_upper_bound"] == 29055200
                assert cost["encoded_bytes_peak"] <= cost["encoded_bytes_limit"]
                assert cost["transport_time_s"] is None and not cost["detailed_cuda_timing"]
    for arm in ("direct", "semloom_hse"):
        default, level1 = arm + "-256-default", arm + "-256-1"
        # Identical content keys restore each of the five complete arrays exactly.
        assert candidate["vector_map"][default] == candidate["vector_map"][level1]
        for a, b in zip(candidate["runs"][default]["queries"], candidate["runs"][level1]["queries"], strict=True):
            assert a["doc_ids"] == b["doc_ids"] and a["positions"] == b["positions"]
            assert np.allclose(a["scores"], b["scores"], atol=1e-5, rtol=1e-5)
    return dict(counts, gallery_max_abs_error=max_gallery_error,
                query_max_abs_error_vs_stock=max_query_error, extra_vector_arrays=0)


if __name__ == "__main__":
    print(json.dumps(verify(Path(__file__).resolve().parent), sort_keys=True))
