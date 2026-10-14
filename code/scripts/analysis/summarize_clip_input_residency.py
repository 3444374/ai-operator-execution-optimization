#!/usr/bin/env python3
"""Recompute the fixed single-GPU residency diagnostic from retained evidence."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path
import statistics
import tarfile


def summarize(evidence: Path) -> dict:
    with tarfile.open(evidence, "r:gz") as archive:
        def read(name):
            return archive.extractfile(name).read()

        def document(name):
            return json.loads(read(name))

        provenance = document("provenance.json")
        status = document("attempt2/status.json")
        qualification = document("attempt2/qualification/actual_tensor_ray_lifecycle.observations.json")
        lifecycle_exit = document("attempt2/qualification/actual_tensor_ray_lifecycle.exit.json")
        topology = document("attempt2/qualification/hardware_topology.observations.json")
        runs = []
        for item in status["completed"]:
            if "manifest" not in item:
                continue
            base = "attempt2/" + item["group"] + "/" + item["id"]
            row = document(base + ".json")["row"]
            observation = document(base + ".observations.json")
            actor = next(pool for pool in observation["pools"] if pool["phase"] == "formal")["actors"][0]
            calls = actor["calls"]
            reuse = [call["input_reuse_observation"] for call in calls if call["input_reuse_observation"]]
            copy_bytes = sum(call["input_reuse_observation"]["host_ownership_copy_bytes"]
                             if call["input_reuse_observation"] else call["host_ownership_copy_bytes"]
                             for call in calls)
            warmup_rows = sum(call["row_count"] for pool in observation["pools"]
                              if pool["phase"] == "warmup" for worker in pool["actors"] for call in worker["calls"])
            runs.append({"id": item["id"], "group": item["group"], "rows": row["rows"],
                "dataset_passes": item["dataset_passes"], "cache_bytes": item["cache_bytes"],
                "cuda_events": item["cuda_events"], "operator_e2e_s": row["operator_e2e_s"],
                "wrapper_invocation_wall_s": observation["wrapper_invocation_wall_s"],
                "warmup_rows": warmup_rows, "observed_formal_input_rows": sum(c["row_count"] for c in calls),
                "formal_forward_calls": actor["forward_calls"],
                "cache": {k: v for k, v in actor["cache"].items() if k not in ("entries", "cache_id")},
                "torch_initial_after_model_load": actor["torch_initial_after_model_load"],
                "torch_memory": actor["torch_memory"],
                "nvml_sampled_peak_mib": row["gpu_memory_peak_mib"],
                "nvml_after_formal": next(p for p in observation["pools"] if p["phase"] == "formal")["nvml_device_memory"],
                "identity_s_sum": sum(r["identity_s"] for r in reuse),
                "identity_copy_bytes_sum": sum(r["identity_copy_bytes"] for r in reuse),
                "actual_host_ownership_copy_bytes_sum": copy_bytes,
                "host_copy_s_sum": sum(c.get("host_copy_s", 0) for c in calls),
                "settle_s_sum": sum(r["settle_s"] or 0 for r in reuse),
                "actor_service_s_sum": sum(c["service_s"] for c in calls),
                "as_tensor_cuda_event_s_sum": sum(c["as_tensor_cuda_s"] for c in calls) if item["cuda_events"] else None,
                "clip_forward_cuda_event_s_sum": sum(c["clip_forward_cuda_s"] for c in calls) if item["cuda_events"] else None,
                "all_recorded_events_done": all(c[k] for c in calls for k in c if k.endswith("_event_done")) if item["cuda_events"] else None,
                "quality": {k: row[k] for k in ("output_rows", "exactly_once", "max_norm_error",
                    "embedding_checksum", "embedding_sum_all", "embedding_digest_xor_rounded5")},
                "exit": document(base + ".exit.json")})
        pairs = []
        for passes in (1, 2):
            cases = [run for run in runs if run["group"] == "primary" and run["dataset_passes"] == passes]
            off = [run["operator_e2e_s"] for run in cases if run["cache_bytes"] == 0]
            on = [run["operator_e2e_s"] for run in cases if run["cache_bytes"] > 0]
            pairs.append({"dataset_passes": passes, "off_s": off, "on_s": on,
                          "off_mean_s": statistics.mean(off), "on_mean_s": statistics.mean(on),
                          "mean_on_vs_off_percent": 100 * (statistics.mean(on) / statistics.mean(off) - 1),
                          "rounded5_row_id_digest_matches": len({r["quality"]["embedding_digest_xor_rounded5"] for r in cases}) == 1})
        import numpy as np
        matrix_bytes = read("attempt2/qualification/" + qualification["embedding_artifact"]["file"])
        with np.load(io.BytesIO(matrix_bytes), allow_pickle=False) as matrices:
            matrix_checks = [{"name": name, "shape": list(matrices[name].shape), "dtype": str(matrices[name].dtype),
                "finite": bool(np.isfinite(matrices[name]).all()),
                "max_norm_error": float(np.max(np.abs(np.linalg.norm(matrices[name], axis=1) - 1)))} for name in matrices.files]
            matrix_pairs = [{"case": case, "max_abs_diff": float(np.max(np.abs(matrices["off_" + case] - matrices["on_" + case])))}
                            for case in ("first_miss", "new_rows_hit", "content_change_miss", "original_reload")]
            async_difference = float(np.max(np.abs(matrices["async_full_forward"] - matrices["off_first_miss"])))
        first = document("attempt1/status.json")
        return {"schema_version": 1, "identity": "internal_single_gpu_clip_input_residency_diagnostic",
            "provenance": provenance, "measured_source": provenance["measured_source"],
            "status": status["status"], "started_utc": status["started_utc"], "finished_utc": status["finished_utc"],
            "machine": provenance["machine"], "resources": provenance["resources"],
            "operator_time_semantics": "original worker_setup_s + pipeline.total_s; other finalization and invocation wall separate",
            "memory_semantics": "logical cache bytes, Torch allocator peaks and sampled NVML device totals are distinct",
            "cuda_event_semantics": "device-stream elapsed intervals include allocation/conversion/launch gaps; not bus counters or compute utilization",
            "known_measured_trace_issue": "9f top-level host_ownership_copy_bytes predicts readonly copy on hits; use input_reuse_observation actual bytes",
            "qualification": {"passed": qualification["passed"], "physical_device_validation": qualification["physical_device_validation"],
                "checks": qualification["checks"], "lifecycle": qualification["lifecycle"],
                "phases": [{"mode": p["mode"], "nvml_device_memory": p["nvml_device_memory"],
                    **{k: p["snapshot"][k] for k in ("ready", "cache", "torch_memory", "torch_initial_after_model_load", "forward_calls")},
                    "normal_calls": [{k: c[k] for k in c if k in ("row_ids_sha256", "row_count", "service_s",
                        "as_tensor_cuda_s", "clip_forward_cuda_s", "input_reuse_observation")}
                                     for c in p["snapshot"]["calls"][:4]]} for p in qualification["phases"]],
                "matrix_sha256": hashlib.sha256(matrix_bytes).hexdigest(), "matrices": matrix_checks,
                "off_on_pairs": matrix_pairs, "async_max_abs_diff": async_difference, "exit": lifecycle_exit},
            "runs": runs, "primary_pairs": pairs,
            "model_images": {"cli_formal_plus_warmup": sum(r["rows"] + r["warmup_rows"] for r in runs),
                "qualification": 64 * sum(p["snapshot"]["forward_calls"] for p in qualification["phases"]),
                "cap": 83000},
            "first_failed_attempt": {"status": first["status"], "error": first["error"],
                "subprocesses_started": len(first["attempted"]), "model_images": 0,
                "physical_exit": first["final_physical_exit"], "host_lease_released": first["host_lease_released"]},
            "final_physical_exit": status["final_physical_exit"], "host_lease_released": status["host_lease_released"],
            "future_two_gpu": {"status": topology["status"], "visible_cuda_devices": topology["visible_cuda_devices"],
                "direct_transfer_implemented": topology["semloom_peer_copy_implemented"],
                "directional_peer_access": topology["directional_peer_access"], "fallback": topology["fallback"]}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.evidence)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
