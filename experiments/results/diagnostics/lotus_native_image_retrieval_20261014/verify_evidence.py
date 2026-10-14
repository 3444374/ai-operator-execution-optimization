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
    return dict(successful_queries_checked=checked, image_queries=image_queries,
                text_queries=text_queries, failed_runs_retained=failures,
                recorded_preparation_failures=len(evidence["failures"]),
                gallery_max_abs_error=max_error, unique_full_matrices=len(arrays.files))


if __name__ == "__main__":
    print(json.dumps(verify(Path(__file__).resolve().parent), sort_keys=True))
