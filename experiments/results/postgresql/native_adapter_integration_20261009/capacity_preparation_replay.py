"""Recheck capacity preparation evidence without contacting a model service."""

from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path

root = Path(__file__).parent
identity = json.loads((root / "capacity-preparation-verification.json").read_text())
archive = root / identity["public_archive"]["path"]
compressed = archive.read_bytes()
assert len(compressed) == identity["public_archive"]["bytes"]
assert hashlib.sha256(compressed).hexdigest() == identity["public_archive"]["sha256"]
rows = [json.loads(line) for line in gzip.decompress(compressed).splitlines()]
assert len(rows) == identity["public_archive"]["rows"]
assert Counter(row["kind"] for row in rows) == identity["projection_counts"]
assert sum(row["observations"] for row in rows if row["kind"] == "object_observation") == identity["object_observations_original"]
cases = {row["case"]: row["value"] for row in rows if row["kind"] == "case"}
summaries = {row["case"]: row for row in rows if row["kind"] == "summary"}
cleanups = {row["case"]: row for row in rows if row["kind"] == "cleanup"}
requests, objects = defaultdict(list), defaultdict(list)
for row in rows:
    if row["kind"] == "request":
        requests[row["case"]].append(row)
    elif row["kind"] == "object_observation":
        objects[row["case"]].append(row["value"])
assert len(cases) == 61 and len(summaries) == len(cleanups) == 59
source_counts, source_posts = Counter(), Counter()
for name, case in cases.items():
    assert not Path(name).is_absolute() and ".." not in Path(name).parts
    assert case["real_model_posts"] == 0 and len(requests[name]) == case["fixture_posts"]
    assert [row["ordinal"] for row in requests[name]] == list(range(case["fixture_posts"]))
    events = []
    for row in requests[name]:
        assert len(row["request_sha256"]) == 64
        assert row["body_read_ns"] <= row["response_ready_ns"]
        events += [(row["body_read_ns"], 1), (row["response_ready_ns"], -1)]
    active = peak = 0
    for _, delta in sorted(events):
        active += delta
        assert active >= 0
        peak = max(peak, active)
    assert active == 0 and peak == case["observed_http_peak"]
    assert max((event["object_bytes"] for event in objects[name]), default=0) == case["peak_charged_object_bytes"]
    assert all(0 <= event["object_bytes"] <= event["object_limit_bytes"] == 8 * 2**20 for event in objects[name])
    if case["status"] != "passed":
        assert case["fixture_posts"] == 0 and case["failure"]
        continue
    source_counts[case["source_commit"]] += 1
    source_posts[case["source_commit"]] += case["fixture_posts"]
    summary, cleanup = summaries[name], cleanups[name]
    assert summary["status"] == "passed" and summary["rows"] == case["rows"]
    assert summary["actual_posts"] == case["fixture_posts"] and not case["gate_timeout"]
    if case["required_http_peak"] is not None:
        assert peak == case["required_http_peak"]
    for limits in case["core_limits"]:
        assert limits["held_tasks"] == 128 and limits["active_requests"] == case["capacity"]
        assert limits["input_bytes"] == limits["result_bytes"] == 128 * 2**20
    if cleanup["group"]:
        group = cleanup["group"]
        assert group["status"] == "passed" and not group["cleanup_errors"]
        for owner in group["owners"].values():
            assert not any((owner.get("core_usage") or {}).values())
            assert owner.get("core_jobs", 0) in (0, None)
    if cleanup["service"]:
        service = cleanup["service"]
        assert service["first_error"] is None and not service["cleanup_errors"]
        assert not service["service_thread_alive"] and service["io_loop_closed"]
        assert service["listeners_remaining"] == service["http_handlers_remaining"] == service["http_connections_remaining"] == 0
    if case["arm"] == "duckdb-method-semloom" and case["source_commit"] == identity["final_source_commit"]:
        assert not summary["identity"]["native_provider_pool_used"]
final = identity["final_source_commit"]
assert source_counts[final] == identity["final_source_queries"] == 43
assert source_posts[final] == identity["final_source_fixture_posts"] == 19160
assert sum(source_counts.values()) == 59 and sum(source_posts.values()) == 27352
supplier_groups = (
    ("lotus-method-semloom", "lotus-adapted-native", "lotus-method-semloom-local-diagnostic"),
    ("duckdb-method-semloom", "duckdb-adapted-native"),
    ("sema-method-semloom-request-service", "sema-native-direct", "sema-native-transparent"),
    ("fixed-map-semloom", "fixed-map-native-daft", "fixed-map-native-ray"),
)
for arms in supplier_groups:
    selected = [name for name, case in cases.items() if case["source_commit"] == final and case["arm"] in arms and case["rows"] == 512]
    hashes = [Counter(row["request_sha256"] for row in requests[name]) for name in selected]
    assert hashes and all(hashes[0] == other for other in hashes[1:])
for arm in ("lotus-method-semloom", "duckdb-method-semloom", "sema-method-semloom-request-service"):
    assert {case["capacity"] for case in cases.values() if case["source_commit"] == final and case["arm"] == arm and case["rows"] == 512} == {4, 8, 16, 32, 64, 128}
reports = {row["name"]: row["value"] for row in rows if row["kind"] == "report"}
audit = reports["preparation-audit03.json"]
assert audit["status"] == "passed" and audit["all_complete_requests_fit_context"]
assert audit["same_requests_across_capacity"] and audit["same_requests_within_supplier_pairs"]
assert audit["movie_disjoint"] and audit["text_disjoint"] and audit["real_model_posts"] == 0
assert reports["model-runtime-check.json"]["bf16_tensor_check"]
manifest = next(row["value"] for row in rows if row["kind"] == "prepared_manifest")
assert manifest["status"] == "prepared, not started" and manifest["source_commit"] == final
assert len(manifest["cells"]) == 17 and manifest["total_queries"] == 102
assert manifest["max_posts"] == 43656 and manifest["max_seconds"] == 3600
assert all(row["status"] == "ok" for row in rows if row["kind"] == "preflight")
regression = next(row["text"] for row in rows if row["kind"] == "regression")
assert "Ran 19 tests" in regression and "OK (skipped=3)" in regression
print("Verified 43 final-source queries/19,160 fixture POST; six capacities per SemLoom supplier; request equality, bytes, cleanup and prepared screening; zero model POST.")
