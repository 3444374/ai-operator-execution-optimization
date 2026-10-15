"""Recompute fixed-main query quality, request associations and empirical HTTP tails."""

from pathlib import Path
from collections import Counter, defaultdict, deque
from types import SimpleNamespace
import argparse
import base64
import gzip
import hashlib
import io
import json
import math
import re
import os

os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"

import pandas as pd
from PIL import Image
from lotus.sem_ops.postprocessors import filter_postprocess
from src.baselines.common.private_artifacts import content_digest
from src.baselines.image.sembench_animals import (
    AnimalsDataset,
    classification_audit,
    evaluate_original,
)
from src.experiments.postgresql.sembench_queries import (
    MovieDataset,
    MovieQuery,
    MovieQueryResult,
)
from src.semantic_methods.lotus.sdk import validate_source, IMAGE_TEXT_PROFILE


def lines(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines()]


def read(path):
    return json.loads(Path(path).read_text())


def frame(value):
    return pd.DataFrame(value["data"], columns=value["columns"], index=value["index"])


def parsed(text, model):
    return filter_postprocess(
        [text], SimpleNamespace(get_model_name=lambda: model)
    ).outputs[0]


def native_sdk_response_values(value):
    # Replay the pinned native LiteLLM OpenAI path, including OpenAI SDK defaults.
    # The lower-level adapter response_model intentionally has a different shape.
    from openai.types.chat import ChatCompletion
    from litellm.types.utils import LlmProviders, ModelResponse
    from litellm.utils import ProviderConfigManager, convert_to_model_response_object

    config = ProviderConfigManager.get_provider_chat_config(
        model=value["model"], provider=LlmProviders.OPENAI
    )
    decoded = config.transform_parsed_response_dict(
        ChatCompletion.model_validate(value).model_dump()
    )
    return convert_to_model_response_object(
        response_object=decoded, model_response_object=ModelResponse()
    ).model_dump()


def associate_movie_protocols(protocols, calls, prompts):
    assert len(protocols) == len(calls) == len(prompts)
    by_response = defaultdict(deque)
    for record in protocols:
        assert record["response"]["model"] == "Qwen2.5-7B-Instruct"
        response = native_sdk_response_values(record["response"])
        key = hashlib.sha256(
            json.dumps(
                response, sort_keys=True, ensure_ascii=False, allow_nan=False
            ).encode()
        ).hexdigest()
        by_response[key].append(record)
    associated = []
    seen_rows = set()
    for call in calls:
        row_id = call["row_id"]
        assert row_id in prompts and row_id not in seen_rows
        records = by_response[call["response_bytes_sha256"]]
        assert records, "response reused or absent"
        record = records.popleft()
        seen_rows.add(row_id)
        assert record["request"]["messages"] == prompts[row_id]
        if call["request_values_sha256"] is not None:
            assert call["request_values_sha256"] == content_digest(record["request"])
        associated.append((call, record))
    assert seen_rows == set(prompts) and not any(by_response.values())
    return associated


def associate_animals_protocols(requests, responses, pixels):
    requests_by_hash = {v["body_sha256"]: v["body"] for v in requests}
    assert len(requests) == len(responses) == len(pixels)
    assert len(requests_by_hash) == len(pixels)
    replay = {}
    for response in responses:
        assert response["status"] == 200
        body = requests_by_hash.pop(response["request_sha256"])
        assert body["model"] == "Qwen2-VL-2B-Instruct"
        parts = [
            part
            for msg in body["messages"]
            if type(msg["content"]) is list
            for part in msg["content"]
            if part.get("type") == "image_url"
        ]
        assert len(parts) == 1
        raw = base64.b64decode(
            parts[0]["image_url"]["url"].split(",", 1)[1], validate=True
        )
        with Image.open(io.BytesIO(raw)) as image:
            rgb = image.convert("RGB")
            identity = (
                rgb.width,
                rgb.height,
                hashlib.sha256(rgb.tobytes()).hexdigest(),
            )
        row_id = pixels[identity]
        assert row_id not in replay
        raw = base64.b64decode(response["body_base64"])
        raw = gzip.decompress(raw) if raw.startswith(b"\x1f\x8b") else raw
        value = json.loads(raw)
        assert value["model"] == "Qwen2-VL-2B-Instruct"
        replay[row_id] = parsed(
            value["choices"][0]["message"]["content"], value["model"]
        )
    assert not requests_by_hash and set(replay) == set(pixels.values())
    return replay


def request_stats(traces):
    samples = []
    points = []
    for row in traces:
        start = row["upstream_dispatch_started_monotonic_ns"]
        end = row["upstream_response_body_read_completed_monotonic_ns"]
        assert (
            row["forwarded"]
            and type(start) is int
            and type(end) is int
            and end >= start
        )
        assert row["upstream_response_status"] == 200
        samples.append((end - start) / 1e9)
        points.extend(((start, 1), (end, -1)))
    active = peak = 0
    for _, delta in sorted(points):
        active += delta
        assert active >= 0
        peak = max(peak, active)
    assert active == 0 and peak <= 4
    values = sorted(samples)
    n = len(values)
    result = dict(
        samples=n,
        scope="actual model gateway dispatch to complete upstream body, one parent monotonic clock",
        quantile_method="empirical nearest rank; no population-tail estimate",
        peak_http=peak,
        minimum_s=min(values),
        maximum_s=max(values),
        p50_s=values[math.ceil(0.50 * n) - 1],
        p95_s=values[math.ceil(0.95 * n) - 1],
        p99_s=values[math.ceil(0.99 * n) - 1] if n >= 1000 else None,
        p99_unavailable_reason=None if n >= 1000 else "fewer than 1000 model requests",
        p99_99=None,
    )
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--checkout", type=Path, required=True)
    p.add_argument("--movie-manifest", type=Path, required=True)
    p.add_argument("--movie-csv", type=Path, required=True)
    p.add_argument("--animals-manifest", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    validate_source()
    validate_source(message_profile=IMAGE_TEXT_PROFILE)
    root = args.root
    controller = read(root / "controller-result.json")
    assert (
        controller["status"] == "completed" and controller["actual_model_posts"] == 9216
    )
    assert (
        controller["unconfirmed_model_attempts"] == 0
        and controller["local_unforwarded_requests"] == 0
    )
    assert (
        controller["lease_released"]
        and not controller["cleanup"]["survivors"]
        and not controller["cleanup"]["inventory_error"]
    )
    movie = MovieDataset(args.checkout, args.movie_manifest, args.movie_csv)
    movie.preflight()
    source_rows = movie.load_rows()
    rows = []
    equivalence = {}
    request_values = {}
    decisions_by_query = {}
    total = 0
    model_traces = sorted(
        lines(root / "movie/model-http-trace.jsonl"),
        key=lambda r: r["gateway_request_id"],
    )
    offset = 0
    for arm in (
        "lotus-adapted-native",
        "lotus-method-native-daft",
        "lotus-method-native-ray-data",
        "lotus-method-semloom",
    ):
        base = root / "movie" / arm / "output"
        matrix = read(base / "matrix.json")
        assert matrix["status"] == "passed"
        for q, n in ((1, 2000), (2, 120), (3, 120)):
            unit = base / ("a0-measurement-0-q" + str(q))
            summary = read(unit / "summary.json")
            protocols = lines(unit / "protocols.jsonl")
            assert len(protocols) == n and all(
                v["http_status"] == 200 for v in protocols
            )
            prompts = {
                v["row_id"]: v["messages"] for v in read(unit / "filter-prompts.json")
            }
            calls = summary["call_timing"]["calls"]
            assert len(calls) == n and len(prompts) == n
            replay = {}
            for call, record in associate_movie_protocols(protocols, calls, prompts):
                replay[call["row_id"]] = parsed(
                    record["response"]["choices"][0]["message"]["content"],
                    record["response"]["model"],
                )
            decisions = read(unit / "filter-decisions.json")
            assert dict(decisions) == replay and len(decisions) == n
            query = MovieQueryResult(MovieQuery(movie, q), source_rows, unit)
            query.result = frame(read(unit / "original-query-result.json")["dataframe"])
            query.decisions = decisions
            recomputed = query.quality()
            assert recomputed == summary["quality"]
            traces = model_traces[offset : offset + n]
            offset += n
            assert Counter(r["request_body_sha256"] for r in traces) == Counter(
                r["request_body_sha256"] for r in lines(unit / "http-trace.jsonl")
            )
            stats = request_stats(traces)
            assert summary["observed_peak_http"] <= 4
            body_counts = Counter(content_digest(r["request"]) for r in protocols)
            first = request_values.setdefault(q, body_counts)
            same = first == body_counts
            first_decisions = decisions_by_query.setdefault(q, dict(decisions))
            equivalence.setdefault(str(q), []).append(
                dict(
                    arm=arm,
                    actual_request_values_equal=same,
                    decisions_equal=first_decisions == dict(decisions),
                )
            )
            rows.append(
                dict(
                    workload="Movie",
                    query=q,
                    arm=arm,
                    model_requests=n,
                    full_query_s=summary["full_query_seconds"],
                    ready_query_s=summary["execution"]["ready_query_seconds"],
                    quality=recomputed,
                    original_parser_replayed=n,
                    request_response_row_associations=n,
                    request_stats=stats,
                )
            )
            total += n
    assert offset == len(model_traces) == 8960
    animals = AnimalsDataset(
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
    model_traces = sorted(
        lines(root / "animals/model-http-trace.jsonl"),
        key=lambda r: r["gateway_request_id"],
    )
    offset = 0
    animal_reference = animal_values = None
    for arm in ("lotus-native", "daft", "ray-data", "semloom"):
        unit = root / "animals" / arm / "output"
        summary = read(unit / "summary.json")
        requests = lines(unit / "requests.jsonl")
        responses = lines(unit / "responses.jsonl")
        assert (
            summary["status"] == "completed"
            and summary["model_calls"] == len(requests) == len(responses) == 64
        )
        replay = associate_animals_protocols(requests, responses, pixels)
        decisions = read(unit / "filter-decisions.json")
        assert dict(decisions) == replay
        audit = classification_audit(animal_rows, decisions)
        assert audit == summary["classification_audit"]
        quality = evaluate_original(
            args.checkout,
            frame(read(unit / "original-result.json")),
            animals.frame(references=True),
        )
        assert quality == summary["quality"]
        traces = model_traces[offset : offset + 64]
        offset += 64
        assert Counter(r["request_body_sha256"] for r in traces) == Counter(
            v["body_sha256"] for v in requests
        )
        stats = request_stats(traces)
        assert summary["resource_gate"]["passed"]
        values = Counter(content_digest(v["body"]) for v in requests)
        if animal_reference is None:
            animal_reference = dict(decisions)
            animal_values = values
        equivalence.setdefault("AnimalsQ1", []).append(
            dict(
                arm=arm,
                actual_request_values_equal=animal_values == values,
                decisions_equal=animal_reference == dict(decisions),
            )
        )
        rows.append(
            dict(
                workload="Animals",
                query=1,
                arm=arm,
                model_requests=64,
                full_query_s=summary["full_query_seconds"],
                quality=quality,
                classification_audit=audit,
                original_parser_replayed=64,
                request_response_pixel_associations=64,
                request_stats=stats,
            )
        )
        total += 64
    assert offset == len(model_traces) == 256 and total == 9216
    server_counts = {}
    for kind, expected in (("movie", 8960), ("animals", 256)):
        text = (root / kind / "service-final-metrics.txt").read_text()
        values = re.findall(
            r"^vllm:request_success_total(?:\{[^\n]*\})?\s+([0-9.eE+-]+)\s*$",
            text,
            re.M,
        )
        assert values, "request success counter missing"
        count = sum(float(value) for value in values)
        assert count == expected, (kind, count)
        server_counts[kind] = int(count)
    report = dict(
        schema="semloom.main_queries_recomputed.v1",
        source_commit=controller["source_commit"],
        actual_model_requests=total,
        query_count=len(rows),
        repeats=1,
        warmups=0,
        query_tail_quantiles=None,
        query_tail_reason="one complete query per cell; no query P99 or P99.99",
        rows=rows,
        equivalence=equivalence,
        vllm_request_success_counts=server_counts,
        all_official_quality_and_original_parser_recomputed=True,
        all_request_associations_verified=True,
        all_measured_model_http_peaks_within_four=True,
        no_performance_ranking=True,
        physical_cleanup_and_lease_release_verified=True,
    )
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(
        json.dumps(
            dict(
                queries=len(rows),
                requests=total,
                quality_recomputed=True,
                server_counts=server_counts,
            )
        )
    )


if __name__ == "__main__":
    main()
