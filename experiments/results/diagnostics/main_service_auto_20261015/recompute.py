"""CPU-only replay of fixed-main single-card auto/explicit1 original queries."""

from collections import Counter
from pathlib import Path
import argparse
import json
import re
import runpy


def validate_service_evidence(folder, mode, scope_uuid):
    from src.serving.vllm_resources import (
        validate_resource_identity,
        verify_resource_argv,
    )

    def read(name):
        return json.loads((folder / name).read_text())

    live = read("live-resource-identity.json")
    runtime, ready = read("service-identity.json"), read("service-ready.json")
    assert live["runtime"] == runtime
    requested, resolved = runtime["resources_requested"], runtime["resources_resolved"]
    validate_resource_identity(requested, resolved)
    verify_resource_argv(live["live_argv"], resolved)
    assert requested["gpu_count"] == ("auto" if mode == "auto" else 1)
    assert requested["allowed_gpus"] == resolved["selected_gpu_uuids"] == [scope_uuid]
    assert (
        resolved["resolved_count"]
        == resolved["data_parallel_size"]
        == resolved["tensor_parallel_size"]
        == 1
    )
    assert runtime["pid"] == ready["pid"] == live["root_pid"]
    assert runtime["process_start_time_ticks"] == live["root_start_time_ticks"]
    assert ready["born"] == live["root_born"]
    assert live["live_argv"][0] == runtime["python_executable_argv0"]
    owners = {r["pid"]: r for r in live["owned_service_processes"]}
    assert len(owners) == len(live["owned_service_processes"])
    assert owners[runtime["pid"]]["born"] == ready["born"]
    assert live["gpu_rows"]
    for gpu in live["gpu_rows"]:
        owner = owners[gpu["pid"]]
        assert owner["role"] == "service" and owner["born"] > 0
        assert not owner.get("ownership_status")
        assert (
            owner["parent_pid"] == ready["pid"]
            and owner["parent_born"] == ready["born"]
        )
        assert gpu["gpu_uuid"] == scope_uuid
    stopped = read("service-stop.json")
    assert stopped["returncode"] == 0 and not stopped["remaining_owned"]
    return ready["seconds"]


def recompute(args):
    # Reuse the published original-query replay, including its pinned native SDK
    # response conversion and consume-once associations. It performs no HTTP.
    shared = runpy.run_path(
        str(Path(__file__).parents[1] / "main_queries_20261015/recompute.py")
    )
    read, lines, frame, parsed, stats, associate = (
        shared[name]
        for name in (
            "read",
            "lines",
            "frame",
            "parsed",
            "request_stats",
            "associate_movie_protocols",
        )
    )
    shared["validate_source"]()
    shared["validate_source"](message_profile=shared["IMAGE_TEXT_PROFILE"])
    root = args.root
    controller = read(root / "controller-result.json")
    assert controller["status"] == "completed"
    assert controller["source_commit"] == "2041da67d883f54285952042482493a4a76a6ba1"
    assert (
        controller["actual_model_posts"] == controller["returned_model_posts"] == 1472
    )
    assert (
        controller["unconfirmed_model_attempts"]
        == controller["local_unforwarded_requests"]
        == 0
    )
    assert not controller["http_errors"] and not controller["cleanup_errors"]
    assert controller["lease_released"] and controller["allocation_stop_confirmed"]
    cleanup = controller["cleanup"]
    assert cleanup["observer_stopped"] and cleanup["allocations_closed"]
    assert not any(
        cleanup[key] for key in ("survivors", "inventory_error", "errors", "actions")
    )
    plan = read(root / "plan.json")
    identity = read(root / "source-identity.json")
    assert plan["source_commit"] == identity["commit"] == controller["source_commit"]
    assert len(identity["files"]) == 780
    assert read(root / "helpers-identity.json") == plan["helpers"]
    model_fingerprints = {
        name: value["fingerprint"]
        for name, value in read(root / "model-assets.json").items()
    }
    assert model_fingerprints == {
        "Qwen2.5-7B-Instruct": "42e668a0fdf19c707174fec29df11020c768a77238bc8646e633a87dcf201295",
        "Qwen2-VL-2B-Instruct": "8cab19cbb8c5bc0da3305ac296605bcbbf19cada1d8bdc2c72b3fd08cc961500",
    }
    assert len(controller["units"]) == 16
    movie = shared["MovieDataset"](args.checkout, args.movie_manifest, args.movie_csv)
    movie.preflight()
    movie_rows = movie.load_rows()
    animals = shared["AnimalsDataset"](
        args.checkout,
        args.animals_manifest,
        "04816fecdbad5c7f39b3483cb3a0fdac87c3bc840d22bb006944f8b21a589758",
    )
    animals.preflight()
    animal_rows = animals.read()["rows"]
    pixels = {
        (r["image"]["width"], r["image"]["height"], r["image"]["rgb_sha256"]): r[
            "row_id"
        ]
        for r in animal_rows
    }
    assert len(pixels) == 64
    rows, equivalence, service_counts, service_startups = [], [], {}, {}
    reference_values, reference_decisions = {}, {}
    scope_uuid = read(root / "movie-auto/service-identity.json")["resources_resolved"][
        "selected_gpu_uuids"
    ][0]
    for kind, mode in (
        ("movie", "auto"),
        ("movie", "explicit1"),
        ("animals", "auto"),
        ("animals", "explicit1"),
    ):
        group = f"{kind}-{mode}"
        folder = root / group
        service_startups[group] = validate_service_evidence(folder, mode, scope_uuid)
        traces = sorted(
            lines(folder / "model-http-trace.jsonl"),
            key=lambda r: r["gateway_request_id"],
        )
        offset = 0
        arms = (
            (
                "lotus-adapted-native",
                "lotus-method-native-daft",
                "lotus-method-native-ray-data",
                "lotus-method-semloom",
            )
            if kind == "movie"
            else ("lotus-native", "daft", "ray-data", "semloom")
        )
        for arm in arms:
            output = folder / arm / "output"
            n = 120 if kind == "movie" else 64
            unit_traces = traces[offset : offset + n]
            offset += n
            assert len(unit_traces) == n
            if kind == "movie":
                matrix = read(output / "matrix.json")
                assert matrix["status"] == "passed" and len(matrix["cells"]) == 1
                unit = output / "a0-measurement-0-q2"
                summary = read(unit / "summary.json")
                protocols = lines(unit / "protocols.jsonl")
                assert len(protocols) == n and all(
                    r["http_status"] == 200 for r in protocols
                )
                prompts = {
                    v["row_id"]: v["messages"]
                    for v in read(unit / "filter-prompts.json")
                }
                replay = {
                    call["row_id"]: parsed(
                        record["response"]["choices"][0]["message"]["content"],
                        record["response"]["model"],
                    )
                    for call, record in associate(
                        protocols, summary["call_timing"]["calls"], prompts
                    )
                }
                decisions = read(unit / "filter-decisions.json")
                assert dict(decisions) == replay and len(decisions) == n
                query = shared["MovieQueryResult"](
                    shared["MovieQuery"](movie, 2), movie_rows, unit
                )
                query.result = frame(
                    read(unit / "original-query-result.json")["dataframe"]
                )
                query.decisions = decisions
                quality = query.quality()
                assert quality == summary["quality"]
                assert Counter(
                    r["request_body_sha256"] for r in unit_traces
                ) == Counter(
                    r["request_body_sha256"] for r in lines(unit / "http-trace.jsonl")
                )
                assert summary["observed_peak_http"] <= 4
                values = Counter(
                    shared["content_digest"](r["request"]) for r in protocols
                )
                row = dict(
                    workload="Movie",
                    query=2,
                    mode=mode,
                    arm=arm,
                    ready_query_s=summary["execution"]["ready_query_seconds"],
                    application_s=None,
                    application_unavailable_reason="not measured by this Movie runner",
                    request_response_row_associations=n,
                )
            else:
                summary = read(output / "summary.json")
                requests, responses = (
                    lines(output / "requests.jsonl"),
                    lines(output / "responses.jsonl"),
                )
                assert (
                    summary["status"] == "completed"
                    and summary["model_calls"] == len(requests) == len(responses) == n
                )
                replay = shared["associate_animals_protocols"](
                    requests, responses, pixels
                )
                decisions = read(output / "filter-decisions.json")
                assert dict(decisions) == replay
                audit = shared["classification_audit"](animal_rows, decisions)
                assert audit == summary["classification_audit"]
                quality = shared["evaluate_original"](
                    args.checkout,
                    frame(read(output / "original-result.json")),
                    animals.frame(references=True),
                )
                assert (
                    quality == summary["quality"] and summary["resource_gate"]["passed"]
                )
                assert Counter(
                    r["request_body_sha256"] for r in unit_traces
                ) == Counter(v["body_sha256"] for v in requests)
                values = Counter(shared["content_digest"](v["body"]) for v in requests)
                row = dict(
                    workload="Animals",
                    query=1,
                    mode=mode,
                    arm=arm,
                    classification_audit=audit,
                    application_s=summary["application_seconds"],
                    request_response_pixel_associations=n,
                )
            first_values = reference_values.setdefault(kind, values)
            first_decisions = reference_decisions.setdefault(kind, dict(decisions))
            assert values == first_values, (
                "actual request values differ between modes or execution owners"
            )
            equivalence.append(
                dict(
                    workload=kind,
                    mode=mode,
                    arm=arm,
                    actual_request_values_equal=True,
                    decisions_equal=first_decisions == dict(decisions),
                )
            )
            row.update(
                model_requests=n,
                full_query_s=summary["full_query_seconds"],
                quality=quality,
                original_parser_replayed=n,
                request_stats=stats(unit_traces),
            )
            rows.append(row)
        assert offset == len(traces) == (480 if kind == "movie" else 256)
        text = (folder / "service-final-metrics.txt").read_text()
        successes = re.findall(
            r"^vllm:request_success_total(?:\{[^\n]*\})?\s+([0-9.eE+-]+)\s*$",
            text,
            re.M,
        )
        assert successes and sum(float(v) for v in successes) == offset
        service_counts[group] = offset
    assert len(rows) == 16 and sum(r["model_requests"] for r in rows) == 1472
    return dict(
        schema="semloom.main_auto_queries_recomputed.v1",
        source_commit=controller["source_commit"],
        actual_model_requests=1472,
        query_count=16,
        repeats=1,
        warmups=0,
        query_tail_quantiles=None,
        query_tail_reason="one complete query per cell; no query P99/P99.99",
        rows=rows,
        equivalence=equivalence,
        vllm_request_success_counts=service_counts,
        service_startup_s=service_startups,
        model_fingerprints=model_fingerprints,
        all_official_quality_and_original_parser_recomputed=True,
        all_request_associations_verified=True,
        all_measured_model_http_peaks_within_four=True,
        no_performance_ranking=True,
        physical_cleanup_and_lease_release_verified=True,
    )


def main():
    parser = argparse.ArgumentParser()
    for name in (
        "root",
        "checkout",
        "movie-manifest",
        "movie-csv",
        "animals-manifest",
        "output",
    ):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    report = recompute(args)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(json.dumps(dict(queries=16, requests=1472, quality_recomputed=True)))


if __name__ == "__main__":
    main()
