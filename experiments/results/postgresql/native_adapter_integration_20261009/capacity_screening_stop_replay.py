"""Verify the retained first screening failure and label correction offline."""
import gzip
import hashlib
import json
from pathlib import Path

root = Path(__file__).parent
identity = json.loads((root / "capacity-screening-stop-verification.json").read_text())
compressed = (root / identity["public_archive"]["path"]).read_bytes()
assert len(compressed) == identity["public_archive"]["bytes"]
assert hashlib.sha256(compressed).hexdigest() == identity["public_archive"]["sha256"]
raw, originals = {}, {}
for line in gzip.decompress(compressed).splitlines():
    row = json.loads(line)
    name = row["path"]
    assert name not in raw and not Path(name).is_absolute() and ".." not in Path(name).parts
    assert hashlib.sha256(row["text"].encode()).hexdigest() == row["sha256"]
    raw[name], originals[name] = row["text"], row["original_sha256"]
assert len(raw) == identity["public_archive"]["members"]
def value(name):
    return json.loads(raw[name])
def lines(name):
    return [json.loads(line) for line in raw[name].splitlines()]
old, new = "screening-prepared01/", "screening-prepared03/"
analysis = value("screening-stop-analysis01.json")
assert analysis["status"] == "failed" and analysis["queries_completed"] == 0
assert analysis["real_model_posts"] == analysis["request_attempts"] == analysis["allocated_requests"] == 8
assert not analysis["corrected_model_run_started"] and analysis["post_stop_real_model_posts"] == 0
assert "'--allowed-output','true','--allowed-output','false'" in raw[old + "calibration_worker01.py"]
plan = value(old + "plan.json")
assert "POSITIVE" in plan["instruction"] and "NEGATIVE" in plan["instruction"]
summary = value(old + "cell-00/query-0/summary.json")
assert summary["status"] == "failed" and summary["attempted_posts"] == 8
assert summary["errors"]["query"]["message"] == "supplier returned an undeclared application value"
protocols = lines(old + "cell-00/query-0/protocols.jsonl")
http = lines(old + "cell-00/query-0/http-trace.jsonl")
assert len(protocols) == len(http) == len({p["response"]["id"] for p in protocols}) == 8
assert all(p["http_status"] == 200 and p["response"]["choices"][0]["message"]["content"] in ("POSITIVE", "NEGATIVE") for p in protocols)
assert all(h["status"] == "completed" and h["retry_count"] == 0 and h["upstream_headers_send_count"] == 1 for h in http)
service = value(old + "service-count.json")
assert service["success_delta"] == 8 and service["running"] == service["waiting"] == 0
owner = value(old + "owner-exit.json")
assert owner["status"] == "failed" and not owner["cleanup_errors"]
assert not owner["remaining_gpu_compute"] and not any(owner["ports_open"].values())
manifest = value(new + "screening-manifest.json")
assert manifest["status"] == "corrected, not started"
assert manifest["allowed_outputs"] == ["POSITIVE", "NEGATIVE"]
mapping = manifest["reference_label_mapping"]
assert mapping == {"true": "POSITIVE", "false": "NEGATIVE"}
for name, entry in manifest["inputs"].items():
    references = value(new + name + "-references.json")
    assert len(references) == entry["rows"] and set(references.values()).issubset(set(mapping.values()))
    canonical = (json.dumps(references, sort_keys=True) + "\n").encode()
    assert hashlib.sha256(canonical).hexdigest() == entry["reference_identity"]["sha256"]
    inverse = {row: "true" if label == "POSITIVE" else "false" for row, label in references.items()}
    assert hashlib.sha256((json.dumps(inverse, sort_keys=True) + "\n").encode()).hexdigest() == entry["original_reference_identity"]["sha256"]
launcher = raw[new + "launch_screening01.py"]
assert launcher.index("assert manifest['allowed_outputs']") < launcher.index("CellBudgetLedger.create")
references = value(new + "qualification-references.json")
for arm in ("lotus-adapted-native", "duckdb-method-semloom", "sema-method-semloom-request-service"):
    prefix = "classification-" + arm + "-01/"
    outcome, summary = value(prefix + "outcome.json"), value(prefix + "query/summary.json")
    results = lines(prefix + "query/q0/results.jsonl")
    assert outcome["status"] == summary["status"] == "passed"
    assert outcome["fixture_posts"] == len(results) == summary["quality"]["rows"] == 8
    assert outcome["real_model_posts"] == 0
    assert summary["quality"]["correct"] == sum(r["row"][1] == references[r["row"][0]] for r in results) == 7
print("Verified failed screening:8 real responses, zero completed queries; preserved stop/cleanup, exact label conversion and3 fixture queries/24 fixture POST; no model rerun.")
