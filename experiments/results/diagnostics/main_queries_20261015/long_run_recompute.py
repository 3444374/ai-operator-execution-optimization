"""Offline summary of retained SemBench long-run observations; stdlib only.

Consumes compact original observations and their original-file manifest. It never
contacts a service, loads model weights, or starts a benchmark.
"""
import argparse
import bisect
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import statistics


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def stats(values):
    return {"n": len(values), "mean": statistics.mean(values),
            "median": statistics.median(values), "min": min(values),
            "max": max(values)} if values else None


def latency(values):
    values = sorted(values)
    n = len(values)
    return {"n": n, **{f"p{p}_ms": values[(p*n+99)//100-1]
                       for p in (50, 95, 99)}, "max_ms": values[-1]}


def identity(process):
    return process["pid"], process["born"]


def summarize(export, manifest, config):
    assert digest(export) == config["export_sha256"], "unexpected export"
    assert digest(manifest) == config["manifest_sha256"], "unexpected manifest"
    queries, resources, metadata = [], [], defaultdict(list)
    requests = defaultdict(list)
    counts = Counter()
    with gzip.open(export, "rt") as stream:
        for line in stream:
            row = json.loads(line)
            kind, value = row["type"], row["value"]
            counts[kind] += 1
            if kind == "query":
                queries.append(value)
            elif kind == "request":
                requests[value["kind"], value["arm"], value["round"]].append(value)
            elif kind == "resource":
                resources.append(value)
            else:
                metadata[kind].append(value)
    terminal = metadata["terminal"][0]
    final = terminal["final"]
    assert final["status"] == terminal["exit"]["status"] == "completed"
    assert metadata["plan"][0]["source_commit"] == config["source_commit"]
    assert counts["query"] == 108 and counts["request"] == 154048
    assert not metadata["request-verification"][0]["errors"]
    assert not metadata["request-verification"][0]["overlapping_query_windows"]
    owned = {identity(p): p for p in final["owned_process_identities"]}
    ready = {}
    for worker in metadata["worker-ready"]:
        matches = [i for i in owned if i[0] == worker["value"]["pid"]]
        assert len(matches) == 1
        ready[matches[0]] = worker["kind"] + "/" + worker["arm"]

    def owner(process):
        current, seen = identity(process), set()
        while current not in seen:
            if current in ready:
                return ready[current]
            seen.add(current)
            parent = owned.get(current)
            if not parent or parent.get("parent_pid") is None:
                return None
            current = parent["parent_pid"], parent["parent_born"]
        return None

    model = metadata["model-config"][0]
    assert model == config["movie_model_config"]
    h, layers, ff = (model[k] for k in ("hidden_size", "num_hidden_layers", "intermediate_size"))
    kv_width = model["num_key_value_heads"] * h // model["num_attention_heads"]
    a = 2*layers*(2*h*h+2*h*kv_width+3*h*ff)
    b, c = 2*layers*h, 2*h*model["vocab_size"]
    assert (a, b, c) == (13050576896, 200704, 1089994752)
    peak = config["dense_bf16_fp32_accumulate_peak_flops_per_second"]
    times = [r["monotonic_ns"] for r in resources]
    grouped, movie_windows, all_ids = defaultdict(list), [], set()
    for q in queries:
        grouped[q["kind"] + "/" + q["arm"]].append(q)
    result_groups = []
    movie_flops = 0
    for group, qs in sorted(grouped.items()):
        qs.sort(key=lambda q: q["round"])
        expected = 19 if qs[0]["kind"] == "movie" else 8
        assert [q["round"] for q in qs] == list(range(expected))
        arm_index = config["arms"][qs[0]["kind"]].index(qs[0]["arm"])
        per_query, sample_indices = [], {}
        for q in qs:
            assert q["first_query"] == (q["round"] == 0)
            reqs = requests[q["kind"], q["arm"], q["round"]]
            assert len(reqs) == q["actual_model_posts"] == (2000 if expected == 19 else 64)
            flops = 0
            latencies = []
            for r in reqs:
                request_identity = r["kind"], r["gateway_request_id"]
                assert request_identity not in all_ids
                all_ids.add(request_identity)
                assert r["status"] == "completed" and r["upstream_response_status"] == 200 and r["retry_count"] == 0
                start, end = r["upstream_dispatch_started_monotonic_ns"], r["upstream_response_body_read_completed_monotonic_ns"]
                assert q["entered_monotonic_ns"] <= start <= end <= q["ended_monotonic_ns"]
                latencies.append((end-start)/1e6)
                if expected == 19:
                    p, o = r["actual_prompt_tokens"], r["actual_output_tokens"]
                    assert type(p) is int and type(o) is int and p > 0 and o > 0
                    t = p+o-1
                    flops += a*t+b*t*(t+1)+c*o
            for rk, qk in (("actual_prompt_tokens", "prompt_tokens"), ("actual_output_tokens", "completion_tokens")):
                assert sum(r[rk] for r in reqs) == q["token_usage"][qk]
            start = min(r["upstream_dispatch_started_monotonic_ns"] for r in reqs)
            end = max(r["upstream_response_body_read_completed_monotonic_ns"] for r in reqs)
            if expected == 19:
                movie_windows.append((start, end))
                movie_flops += flops
            lo = bisect.bisect_left(times, q["entered_monotonic_ns"])
            hi = bisect.bisect_right(times, q["ended_monotonic_ns"])
            sample_indices[q["round"]] = list(range(lo, hi))
            cover = [q["entered_monotonic_ns"], *times[lo:hi], q["ended_monotonic_ns"]]
            per_query.append({"round": q["round"], "position": q["order"].index(arm_index)+1,
                "full_query_seconds": q["full_query_seconds"], "application_seconds": q.get("application_seconds"),
                "requests": len(reqs), "http_ms": latency(latencies),
                "useful_flops": flops if expected == 19 else None,
                "service_window_seconds": (end-start)/1e9,
                "resource_samples": hi-lo,
                "outer_envelope_seconds": (q["ended_monotonic_ns"]-q["entered_monotonic_ns"])/1e9,
                "max_unsampled_envelope_span_seconds": max((v-u)/1e9 for u,v in zip(cover,cover[1:]))})
        phases = {}
        for phase, selected in (("first", per_query[:1]), ("subsequent", per_query[1:]), ("all", per_query)):
            rounds = {r["round"] for r in selected}
            lats = [(r["upstream_response_body_read_completed_monotonic_ns"]-r["upstream_dispatch_started_monotonic_ns"])/1e6
                    for q in qs if q["round"] in rounds for r in requests[q["kind"], q["arm"], q["round"]]]
            indices = [i for rnd in rounds for i in sample_indices[rnd]]
            rs = [resources[i] for i in sorted(indices)]
            resource_values = {"client_total_rss_GiB": [], "active_owner_rss_GiB": [],
                "service_rss_GiB": [], "gpu_busy_percent": [], "gpu_memory_MiB": [], "gpu_power_W": [], "gpu_temp_C": []}
            for r in rs:
                ps = r["processes"]
                for label, selected_ps in (("client_total_rss_GiB", [p for p in ps if p["role"] == "client"]),
                    ("active_owner_rss_GiB", [p for p in ps if p["role"] == "client" and owner(p) == group]),
                    ("service_rss_GiB", [p for p in ps if p["role"] == "service"])):
                    resource_values[label].append(sum(p["rss_bytes"] for p in selected_ps)/2**30)
                for label, key in (("gpu_busy_percent", "utilization.gpu"), ("gpu_memory_MiB", "memory.used"),
                    ("gpu_power_W", "power.draw"), ("gpu_temp_C", "temperature.gpu")):
                    resource_values[label].append(float(r["gpu"][0][key]))
            cpu = defaultdict(list)
            unmatched_cpu_identities = 0
            for rnd in rounds:
                indices = sample_indices[rnd]
                for previous_i, current_i in zip(indices, indices[1:]):
                    previous, current = resources[previous_i], resources[current_i]
                    seconds = (current["monotonic_ns"]-previous["monotonic_ns"])/1e9
                    old = {identity(p): p for p in previous["processes"]}
                    deltas = defaultdict(float)
                    for p in current["processes"]:
                        prior = old.get(identity(p))
                        if prior is None:
                            unmatched_cpu_identities += 1
                            continue
                        delta = max(0, p["cpu_user_s"]+p["cpu_system_s"]-prior["cpu_user_s"]-prior["cpu_system_s"])
                        deltas[p["role"]] += delta
                        if p["role"] == "client":
                            assigned = owner(p)
                            deltas["active_owner" if assigned == group else "idle_owners" if assigned else "unattributed_clients"] += delta
                    for label in ("client", "service", "active_owner", "idle_owners", "unattributed_clients"):
                        cpu[label+"_observed_cores"].append(deltas[label]/seconds)
            full = sum(r["full_query_seconds"] for r in selected)
            upstream = sum(r["service_window_seconds"] for r in selected)
            f = sum(r["useful_flops"] for r in selected) if expected == 19 else None
            phases[phase] = {"query_seconds": stats([r["full_query_seconds"] for r in selected]),
                "application_seconds": stats([r["application_seconds"] for r in selected if r["application_seconds"] is not None]),
                "http_ms": latency(lats), "resources": {"samples": len(rs),
                    "outer_envelope_seconds_sum": sum(r["outer_envelope_seconds"] for r in selected),
                    "max_unsampled_envelope_span_seconds": max(r["max_unsampled_envelope_span_seconds"] for r in selected),
                    "cpu_unmatched_current_identities_omitted": unmatched_cpu_identities,
                    **{k: stats(v) for k,v in resource_values.items()}, **{k: stats(v) for k,v in cpu.items()}},
                "mfu": {"flops": f, "sum_full_query_seconds": full,
                    "sum_service_window_seconds": upstream, "full_query_ratio": f/(peak*full),
                    "service_window_ratio": f/(peak*upstream)} if f is not None else None}
        later = per_query[1:]
        xbar = statistics.mean(r["round"] for r in later)
        ybar = statistics.mean(r["full_query_seconds"] for r in later)
        slope = sum((r["round"]-xbar)*(r["full_query_seconds"]-ybar) for r in later)/sum((r["round"]-xbar)**2 for r in later)
        half = len(later)//2
        audits = [q["quality"].get("classification_audit", q.get("classification_audit")) for q in qs]
        f1s = [2*x["true_positive"]/(2*x["true_positive"]+x["false_positive"]+x["false_negative"]) for x in audits]
        result_groups.append({"group": group, "phases": phases, "per_query": per_query,
            "trend": {"scope": "subsequent queries only; descriptive, no causal adjustment",
                "linear_slope_seconds_per_round": slope,
                "early_rounds": [r["round"] for r in later[:half]], "late_rounds": [r["round"] for r in later[-half:]],
                "early_median_seconds": statistics.median(r["full_query_seconds"] for r in later[:half]),
                "late_median_seconds": statistics.median(r["full_query_seconds"] for r in later[-half:]),
                "by_position": {str(p): stats([r["full_query_seconds"] for r in later if r["position"] == p]) for p in range(1,5)}},
            "quality": {"per_query_confusion": audits, "classification_f1": stats(f1s),
                "official_metrics": [q["quality"]["original_metric"] for q in qs],
                "quality_equivalence_proven": False if expected == 19 else None},
            "http_observed_peaks": sorted({q["observed_model_http_peak"] for q in qs}),
            "same_movie_owner": len({q.get("persistent_lifecycle", {}).get("owner_id") for q in qs}) == 1 if expected == 19 else None})
    movie_windows.sort()
    assert all(a[1] <= b[0] for a,b in zip(movie_windows,movie_windows[1:]))
    wall = (movie_windows[-1][1]-movie_windows[0][0])/1e9
    service_walls = sum((end-start)/1e9 for start,end in movie_windows)
    files = json.loads(Path(manifest).read_text())["manifest"]
    areas = defaultdict(lambda: {"files": 0, "bytes": 0})
    for f in files:
        parts = Path(f["path"]).parts
        area = "/".join(parts[:3]) if len(parts) > 3 else f["path"]
        areas[area]["files"] += 1
        areas[area]["bytes"] += f["bytes"]
    animal_roots = {i for i, group in ready.items() if group.startswith("animals/")}
    after_first = max(q["ended_monotonic_ns"] for q in queries if q["kind"] == "animals" and q["round"] == 0)
    fixed = [r for r in resources if r["phase"] == "query-animals" and r["monotonic_ns"] >= after_first
             and {identity(p) for p in r["processes"] if p["role"] == "client"} == animal_roots]
    growth = []
    for i in sorted(animal_roots):
        ps = [next(p for p in r["processes"] if identity(p) == i) for r in fixed]
        growth.append({"group": ready[i], "first_rss_bytes": ps[0]["rss_bytes"],
            "last_rss_bytes": ps[-1]["rss_bytes"], "growth_bytes": ps[-1]["rss_bytes"]-ps[0]["rss_bytes"]})
    temporary_processes = []
    descendant_rounds = defaultdict(list)
    for q in queries:
        if q["kind"] != "animals" or q["arm"] not in ("ray-data", "semloom"):
            continue
        group = "animals/"+q["arm"]
        lo, hi = bisect.bisect_left(times, q["entered_monotonic_ns"]), bisect.bisect_right(times, q["ended_monotonic_ns"])
        descendants = {identity(p) for r in resources[lo:hi] for p in r["processes"]
                       if p["role"] == "client" and owner(p) == group and identity(p) not in animal_roots}
        descendant_rounds[group].append(descendants)
        after = resources[hi]
        temporary_processes.append({"group": group, "round": q["round"], "sampled_descendants": len(descendants),
            "descendants_in_first_post_envelope_sample": len(descendants & {identity(p) for p in after["processes"]}),
            "first_post_envelope_sample_delay_seconds": (after["monotonic_ns"]-q["ended_monotonic_ns"])/1e9})
    reused_descendant_pairs = sum(bool(a & b) for sets in descendant_rounds.values()
                                 for j,a in enumerate(sets) for b in sets[j+1:])
    movie_core = [q["persistent_lifecycle"] for q in queries if q["kind"] == "movie" and q["arm"] == "lotus-method-semloom"]
    animals_core = [q["core_cleanup"] for q in queries if q["kind"] == "animals" and q["arm"] == "semloom"]
    return {"schema": "sembench.long_run_summary.v1", "source_commit": config["source_commit"],
        "export_sha256": config["export_sha256"], "manifest_sha256": config["manifest_sha256"],
        "record_counts": dict(counts), "groups": result_groups,
        "service_lifecycle": [{"kind": r["kind"], "event": r["name"],
            **{k:v for k,v in r["value"].items() if k in ("ready_seconds", "utc", "monotonic_ns", "returncode")}}
            for r in metadata["service-file"] if r["name"] in ("service-ready.json", "service-stop.json")],
        "movie_campaign_mfu": {"queries": 76, "requests": 152000, "useful_flops": movie_flops,
            "wall_seconds": wall, "sum_nonoverlapping_service_windows_seconds": service_walls,
            "interquery_gaps_seconds": wall-service_walls, "ratio": movie_flops/(peak*wall)},
        "resource_intervals_seconds": stats([(b-a)/1e9 for a,b in zip(times,times[1:])]),
        "animals_same_four_roots": {"samples": len(fixed), "per_arm": growth,
            "first_epoch_ns": fixed[0]["epoch_ns"], "last_epoch_ns": fixed[-1]["epoch_ns"]},
        "animals_temporary_processes": {"queries": temporary_processes,
            "cross_round_pairs_reusing_descendant_identity": reused_descendant_pairs},
        "semloom_core": {"movie_queries": len(movie_core), "animals_queries": len(animals_core),
            "movie_all_recorded_usage_and_jobs_zero": all(not any(r["core_usage"].values()) and r["core_jobs"] == 0 for r in movie_core),
            "animals_all_closed_and_usage_and_jobs_zero": all(r["closed"] and not any(r["usage"].values()) and r["open_jobs"] == 0 for r in animals_core)},
        "cleanup": {"terminal_survivors": len(terminal["surviving_owned"]), "lease_released": final["lease_released"],
            "final_sample_client_processes": sum(p["role"] == "client" for p in resources[-1]["processes"]),
            "terminal_gpu_compute_processes": len(terminal["gpu_processes"])},
        "archive": {"files": len(files), "raw_bytes": sum(f["bytes"] for f in files),
            "compressed_bytes": config["archive_bytes"], "sha256": config["archive_sha256"],
            "areas": dict(sorted(areas.items()))}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("long_run_config.json"))
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = summarize(args.export, args.manifest, json.loads(args.config.read_text()))
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)+"\n")
