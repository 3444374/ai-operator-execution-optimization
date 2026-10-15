"""Recompute complete vectors, original row IDs and native top-K from compact evidence."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from retained_evidence import read_bytes, read_json, open_archive
import gzip
import hashlib
import json

import numpy as np


def verify(directory):
    directory = Path(directory)
    evidence = json.loads(gzip.decompress((directory / "observations.json.gz").read_bytes()))
    mapping = read_json(directory/"vector_map.json")["runs"]
    manifest = read_json(directory/"source_manifest.json")["entries"]
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
    if (directory / "native_image_queries.json").exists():
        result["native_image_queries"] = verify_native_image_queries(directory)
    return result


def verify_png_candidate(directory, evidence, mapping, arrays, manifest):
    """Check the separate candidate using exact restored matrices and native output."""
    candidate = read_json(directory/"png_candidate.json")
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


def verify_native_image_queries(directory):
    """Restore complete matrices and verify native query/actual-model receipts."""
    import io
    import math
    import re

    def integer(value, minimum=0):
        assert type(value) is int and value >= minimum
        return value

    evidence = read_json(directory / "native_image_queries.json")
    assert evidence["schema"] == "lotus.native_image_queries.compact.v1"
    storage = evidence["complete_matrix_storage"]
    assert storage["file"] in ("vectors.npz", "native_image_vectors.npz")
    raw = read_bytes(directory / storage["file"])
    assert len(raw) == storage["bytes"] and hashlib.sha256(raw).hexdigest() == storage["sha256"]
    assert evidence["p99"] is None and evidence["p99_99"] is None
    assert evidence["cache"] is evidence["performance_ranking"] is False
    assert evidence["warmups_discarded"] == 0 and evidence["detailed_timing"] is False
    embedding_bytes = read_bytes(directory / "embedding_identity.json")
    embedding = json.loads(embedding_bytes)
    assert embedding["lotus_source"]["commit"] == "b1a85fd7a66fabed8a1585d44d7597d592b4433f"
    assert embedding["qualified"] and embedding["parameters_equal"] and embedding["effective_pixels_equal"]
    assert embedding["stock_parameters"] == embedding["candidate_parameters"]
    source_ids = [x["doc_id"] for x in read_json(directory / "source_manifest.json")["entries"][:12]]
    expected_groups = {
        "ray_serve": ("460d3870c183c21b1c4144b522dc8896d60d7d7e", 3, 2, 193, 4),
        "automatic_input": ("17ac2aeb95723494a47c2677e65691c962c62646", 8, 1, 247, 31),
    }
    suffixes = ("attempts", "returns", "attempted_rows", "returned_rows")
    stable = ("pid", "process_start_ticks", "constructed_monotonic_ns", "model_object_id", "role", "device")

    def models(cell, site):
        proof = cell["model_reuse"]
        if "available" in proof:
            assert proof["available"] is True
        constructors = proof["constructors"]
        ctors = {x["model_uid"]: x for x in constructors}
        assert len(ctors) == len(constructors) and ctors
        exits = {(x["kind"], x["pid"], x["birth"]) for x in site["checks"]}
        for uid, ctor in ctors.items():
            assert re.fullmatch("[0-9a-f]{32}", uid)
            integer(ctor["pid"], 1)
            integer(ctor["constructed_monotonic_ns"], 1)
            assert re.fullmatch("[0-9]+", ctor["process_start_ticks"])
            assert (cell["label"], ctor["pid"], ctor["process_start_ticks"]) in exits
            assert ctor["model_alive"] is True and ctor["hook_errors"] == 0
            for prefix in ("", "image_", "text_"):
                for name in suffixes:
                    assert integer(ctor[prefix + name]) == 0

        def probe(value, image_calls, image_rows, text_calls):
            ctor = ctors[value["model_uid"]]
            assert all(value[name] == ctor[name] for name in stable)
            assert value["model_alive"] is True and integer(value["hook_errors"]) == 0
            for prefix in ("", "image_", "text_"):
                for name in suffixes:
                    integer(value[prefix + name])
            assert tuple(value["image_" + k] for k in suffixes) == (image_calls, image_calls, image_rows, image_rows)
            assert tuple(value["text_" + k] for k in suffixes) == (text_calls,) * 4
            assert all(value[k] == value["image_" + k] + value["text_" + k] for k in suffixes)

        stock = cell["arm"] == "stock"
        expected_text_device = "cuda" if stock else "cpu"
        assert cell["stock_text_device"] == expected_text_device
        text_uids = {uid for uid, c in ctors.items() if c["role"] == "lotus_stock_clip" and c["device"] == expected_text_device}
        image_uids = text_uids if stock else {uid for uid, c in ctors.items() if c["role"] == "clip_tensor_actor" and c["device"] == "cuda"}
        assert len(text_uids) == 1 and set(ctors) == text_uids | image_uids
        image_calls = [c for c in cell["rm_calls"] if c["image_rows"]]
        image_rows = sum(c["image_rows"] for c in image_calls)
        assert all(c["image_rows"] <= cell["configuration"]["direct_model_batch_size"] for c in image_calls)
        text_calls = sum(c["text_rows"] for c in cell["rm_calls"])
        graph_owned = cell["arm"] == "ray_data"
        assert integer(proof["image_model_count"], 1) == len(image_uids) == (len(image_calls) if graph_owned else 1)
        assert proof["cross_query_resident"] is (not graph_owned)
        phases = cell["actual_model_phases"]
        assert [x["phase"] for x in phases] == ["startup_complete", "after_index_or_register", "after_queries", "before_close"]
        clocks = [integer(x["observed_monotonic_ns"], 1) for x in phases]
        assert clocks == sorted(clocks)
        for index, phase in enumerate(phases):
            seen = set()
            observations = phase["local_models"] + [x["model_reuse"] for x in phase["worker_models"]]
            for value in observations:
                uid = value["model_uid"]
                assert uid not in seen and uid in ctors
                seen.add(uid)
                assert value["constructed_monotonic_ns"] <= clocks[index]
                expected_images = (0, 0) if index == 0 else ((1, cell["rm_calls"][0]["image_rows"]) if index == 1 else (len(image_calls), image_rows))
                if uid in text_uids:
                    probe(value, *(expected_images if stock else (0, 0)), 0 if index < 2 else text_calls)
                else:
                    assert not graph_owned
                    probe(value, *expected_images, 0)
            assert text_uids <= seen
            if not stock and not graph_owned and cell["arm"] != "external" and index >= 1:
                assert image_uids <= seen
        batches = proof.get("native_batches", [])
        if graph_owned or cell["arm"] == "external":
            assert len(batches) == len(image_calls)
            cumulative = 0
            seen = set()
            for index, (batch, call) in enumerate(zip(batches, image_calls, strict=True)):
                uid = batch["model_uid"]
                assert uid in image_uids and integer(batch["pid"], 1) == ctors[uid]["pid"]
                cumulative += call["image_rows"]
                assert integer(batch["batch_rows"], 1) == call["image_rows"]
                # Native columns carry image_returns/completed rows. They do not
                # reveal failed internal forwards or prove a no-retry property.
                assert integer(batch["embed_calls"], 1) == (1 if graph_owned else index + 1)
                assert integer(batch["rows"], 1) == (call["image_rows"] if graph_owned else cumulative)
                seen.add(uid)
                if graph_owned:
                    query = None if index == 0 else cell["queries"][call["call_index"] - 1]
                    lower, upper = (clocks[0], clocks[1]) if query is None else (query["submitted_monotonic_ns"], query["returned_monotonic_ns"])
                    assert lower <= ctors[uid]["constructed_monotonic_ns"] <= upper
            assert seen == image_uids
        if cell["arm"] == "external":
            before, after = proof["service_ready"], proof["service_before_close"]
            assert re.fullmatch("[0-9a-f]{32}", before["service_uid"])
            assert proof["service_uid"] == before["service_uid"] == after["service_uid"]
            for value, n, rows in ((before, 0, 0), (after, len(image_calls), image_rows)):
                p = value["model"]["model_reuse"]
                assert p["model_uid"] in image_uids
                assert value["pid"] == value["model"]["actor_worker_pid"] == p["pid"]
                assert value["failed"] is False and value["model"]["embedding_dimension"] == 512 and value["model"]["normalized"] is True
                assert value["model"]["torch_intraop_threads"] == value["model"]["torch_interop_threads"] == 1
                assert (integer(value["forward_attempts"]), integer(value["forward_returns"]), integer(value["returned_rows"])) == (n, n, rows)
                probe(p, n, rows, 0)
            assert before["model"]["model_reuse"]["constructed_monotonic_ns"] <= clocks[0]
        return len(image_uids)

    def verify_hse_costs(cell):
        config = cell["configuration"]
        assert config["hse_encoded_bytes"] == 58110400
        assert config["hse_encoded_block_bytes_upper_bound"] == 29055200
        costs = cell["execution_observations"]
        assert len(costs) == sum(bool(call["image_rows"]) for call in cell["rm_calls"])
        for cost in costs:
            assert integer(cost["encoded_bytes_limit"], 1) == 58110400
            assert integer(cost["encoded_block_bytes_upper_bound"], 1) == 29055200
            assert integer(cost["encoded_bytes_peak"]) <= 58110400
            assert integer(cost["ready_bytes_peak"]) <= 38535168
            assert cost["transport_time_s"] is None and cost["detailed_cuda_timing"] is False

    checked_matrices = checked_queries = 0
    checked_models = {}
    max_error = 0.0
    referenced = set()
    with np.load(io.BytesIO(raw), allow_pickle=False) as arrays:
        for group_name, (commit, cell_count, repeats, sdk_items, sdk_subtests) in expected_groups.items():
            if group_name not in evidence:
                continue
            group = evidence[group_name]
            assert group["measured_commit"] == commit
            assert hashlib.sha256(embedding_bytes).hexdigest() == group["embedding_identity_sha256"]
            supervisor = group["supervision"]
            assert supervisor["status"] == "succeeded" and supervisor["lease_released"] is True
            assert not supervisor["post_phase_live_owned"] and not supervisor["post_phase_gpu_compute_pids"]
            assert all(x["exit_code"] == 0 and x["timed_out"] is False and not x["error_type"] for x in supervisor["runs"])
            assert all(x["confirmed"] is True and not x["cleanup_error"] and not x["live_owned_processes"] and not x["gpu_compute_pids"] for x in supervisor["cleanup"])
            assert [x["label"] for x in supervisor["runs"]] == [x["label"] for x in supervisor["cleanup"]]
            assert supervisor["sdk_subtests"]["failed"] == supervisor["sdk_subtests"]["skipped"] == 0
            assert supervisor["sdk_standard_items"] == sdk_items and supervisor["sdk_subtests"]["passed"] == sdk_subtests
            site = group["post_exit"]
            assert site["all_original_births_gone"] is True and not site["gpu_compute_pids"] and site["shared_lease_present"] is False
            assert all(x["current"] is None and x["original_alive"] is False for x in site["checks"])
            keys = {(x["kind"], x["pid"], x["birth"]) for x in site["checks"]}
            assert len(keys) == len(site["checks"]) == sum(x["tracked_processes"] for x in supervisor["cleanup"]) + 2
            for item in supervisor["cleanup"]:
                assert sum(x["kind"] == item["label"] for x in site["checks"]) == item["tracked_processes"]
            cells = group["cells"]
            assert len(cells) == cell_count and len({c["label"] for c in cells}) == cell_count and cells[0]["arm"] == "stock"
            assert [x["label"] for x in supervisor["runs"]][3:] == [c["label"] for c in cells]
            reference = {v["call"]: arrays[v["array"]] for v in cells[0]["complete_vectors"]}
            matrices = queries = 0
            for cell in cells:
                assert cell["status"] == "succeeded"
                config = cell["configuration"]
                assert config["mode"] == "combined" and config["k"] == 10 and config["model_dtype"] == "float32"
                assert config["direct_model_batch_size"] == config["stock_encode_batch_size"] == 32 and config["stock_outer_batch_size"] == 64
                assert config["torch_thread_pools"] == {"torch_intraop_threads": 1, "torch_interop_threads": 1}
                assert config["native_cache"] is config["detailed_timing"] is False
                if "query_policy" in config:
                    assert config["query_policy"] == dict(repeats=repeats, payload_count=2, retained_first_round=True, discarded_warmup_queries=0,
                        input_ready="RGB PIL images and text prepared before submission", arrival="synchronous closed loop; one outstanding query", native_cache=False)
                else:
                    # The pinned workflow only emits policy/repeat metadata for repeats>1.
                    assert repeats == 1
                assert len(cell["queries"]) == repeats * 2
                calls = cell["rm_calls"]
                assert len(calls) == len(cell["complete_vectors"]) == len(cell["queries"]) + 1
                assert [integer(c["call_index"]) for c in calls] == list(range(len(calls)))
                assert all(c["status"] == "succeeded" for c in calls)
                assert (calls[0]["image_rows"], calls[0]["text_rows"]) == (12, 0)
                assert {x["call"] for x in cell["complete_vectors"]} == {"call_" + str(i) for i in range(len(calls))}
                assert len({x["call"] for x in cell["complete_vectors"]}) == len(calls)
                values = {}
                for record in cell["complete_vectors"]:
                    value = arrays[record["array"]]
                    assert record["array"] == "v_" + hashlib.sha256(value.tobytes()).hexdigest()
                    assert record["raw_array_sha256"] == record["array"][2:]
                    assert record["dtype"] == str(value.dtype) == "float32" and list(value.shape) == record["shape"]
                    call = calls[int(record["call"].removeprefix("call_"))]
                    assert value.shape == (integer(call["image_rows"]) + integer(call["text_rows"]), 512)
                    assert np.isfinite(value).all() and np.allclose(np.linalg.norm(value, axis=1), 1, atol=2e-5, rtol=0)
                    assert np.allclose(value, reference[record["call"]], atol=1e-5, rtol=1e-5)
                    max_error = max(max_error, float(np.abs(value - reference[record["call"]]).max()))
                    values[record["call"]] = value
                    referenced.add(record["array"])
                    matrices += 1
                ids = cell["gallery_doc_ids"]
                assert ids == source_ids and len(ids) == len(set(ids)) == 12
                assert len({q["query_id"] for q in cell["queries"]}) == len(cell["queries"])
                previous_return = 0
                for index, query in enumerate(cell["queries"]):
                    base = "image-139" if index % 2 == 0 else "text-0"
                    expected_id = base if repeats == 1 else base + "@" + str(index // 2)
                    assert query["query_id"] == expected_id and query["payload"] == base
                    if repeats > 1:
                        assert query["source_query_id"] == base and query["repeat_index"] == index // 2 and query["query_ordinal"] == index
                    assert query["status"] == "succeeded" and query["rm_calls"] == [calls[index + 1]]
                    call = query["rm_calls"][0]
                    assert (call["image_rows"], call["text_rows"]) == ((1, 0) if index % 2 == 0 else (0, 1))
                    payload = cell["output_payloads"][query["payload"]]
                    assert payload["kind"] == ("image" if index % 2 == 0 else "text")
                    vector = values["call_" + str(call["call_index"])]
                    assert vector.shape == (1, 512)
                    scores = values["call_0"] @ vector[0]
                    positions = np.argsort(-scores, kind="stable")[:config["k"]].tolist()
                    assert positions == payload["positions"] and [ids[p] for p in positions] == payload["doc_ids"]
                    assert np.allclose(scores[positions], payload["scores"], atol=1e-5, rtol=1e-5)
                    submitted = integer(query["submitted_monotonic_ns"], 1)
                    returned = integer(query["returned_monotonic_ns"], submitted)
                    assert submitted >= previous_return
                    previous_return = returned
                    assert query["native_call_return_elapsed_s"] == (returned - submitted) / 1e9
                    assert math.isfinite(query["elapsed_s"]) and 0 <= query["native_call_return_elapsed_s"] <= query["elapsed_s"]
                    queries += 1
                if group_name == "automatic_input" and cell["arm"] != "stock":
                    for call in calls:
                        if not call["image_rows"]:
                            continue
                        if cell["arm"] in ("semloom_static", "semloom_hse"):
                            assert call["source_reused_rows"] == call["image_rows"] and call["png_fallback_rows"] == 0
                        else:
                            assert call["image_encoding"] == "not_performed" and call["encoded_total_bytes"] == 0
                    if cell["arm"] == "semloom_hse":
                        verify_hse_costs(cell)
                checked_models[group_name + "/" + cell["label"]] = models(cell, site)
            assert matrices == group["complete_matrices"] and queries == group["complete_queries"]
            checked_matrices += matrices
            checked_queries += queries
    assert "ray_serve" in evidence
    assert len(referenced) == storage["referenced_unique_arrays"]
    return dict(complete_matrices=checked_matrices, complete_queries=checked_queries, actual_image_models_by_cell=checked_models,
                referenced_unique_arrays=len(referenced), max_component_error_vs_own_stock=max_error)








if __name__ == "__main__":
    print(json.dumps(verify(Path(__file__).resolve().parent), sort_keys=True))
