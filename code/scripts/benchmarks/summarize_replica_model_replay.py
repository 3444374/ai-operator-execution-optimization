"""Audit retained numeric model evidence and emit a payload-free public record."""

import argparse
from collections import Counter
import csv
import datetime
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = args.metadata
    for item in json.loads((root / "metadata-manifest.json").read_text()):
        assert hashlib.sha256((root / item["path"]).read_bytes()).hexdigest() == item["sha256"]
    base = root / "evidence-affinity"
    receipt = json.loads((base / "receipt.json").read_text())
    assert receipt["status"] == "passed" and not receipt["remaining_owned_pids"] and not receipt["gpu_compute_after"]
    with sqlite3.connect("file:" + str(root / "evidence/budget.sqlite") + "?mode=ro", uri=True) as db:
        attempted = db.execute("SELECT COUNT(*) FROM shared_requests").fetchone()[0]
        unclosed = db.execute("SELECT COUNT(*) FROM shared_units s LEFT JOIN closed_shared_units c ON s.unit_id=c.unit_id WHERE c.unit_id IS NULL").fetchone()[0]
    count_receipt = json.loads((root / "count-receipt.json").read_text())
    assert attempted == sum(count_receipt["server_success_posts"].values()) == 820 and unclosed == 0
    # shared_units is a historical registration table. Its row count is not
    # the count of live units; closed_shared_units is the matching tombstone.
    cells, pressure, gpu, summary = [], {}, [], []
    reference = {}
    for topology in ("single", "dual"):
        group = json.loads((base / topology / "group.json").read_text())
        pressure[topology] = [json.loads(line) for line in (base / topology / "pressure.jsonl").read_text().splitlines()]
        for path in sorted((base / topology).glob("*.json.gz")):
            result = json.load(gzip.open(path, "rt"))
            assert result["count"] == len(result["outcomes"]) == len(result["attempts"])
            cleanup = json.loads((base / topology / result["label"] / "cleanup.json").read_text())
            assert cleanup["clean"] and cleanup["usage"]["held_tasks"] == cleanup["usage"]["active_requests"] == 0
            for outcome in result["outcomes"]:
                seq = outcome["source_sequence"]
                canonical = (outcome["content_sha256"], outcome["finish_reason"], outcome["usage"])
                if seq in reference:
                    assert reference[seq] == canonical, (topology, result["label"], seq)
                else:
                    reference[seq] = canonical
            events = []
            for original in result["events"]:
                event = {k: v for k, v in original.items() if k not in ("task_info", "job_id", "flow_id", "location_id")}
                events.append(event)
            # The source row IDs, call IDs, payload and full response stay in
            # private evidence. Anonymous sequence and output digests suffice.
            outcomes = [{k: v for k, v in o.items() if k not in ("source_row_id", "source_call_id")} for o in result["outcomes"]]
            cells.append(dict(result, events=events, outcomes=outcomes))
        for arm in ("round_robin", "fixed_rate", "adaptive_route", "adaptive_supply"):
            part = sorted((c for c in cells if c["topology"] == topology and c["label"].startswith("measure") and c["arm"] == arm), key=lambda c: c["label"])
            assert len(part) == 3
            assert all([o["source_sequence"] for o in sorted(c["outcomes"], key=lambda o:o["source_sequence"])] == list(range(24)) for c in part)
            snapshots = [e for c in part for e in c["events"] if e["event"] == "core_supply_snapshot"]
            assert max(e["usage"]["active_requests"] for e in snapshots) <= group["selected_k"]
            assert all(max(e["endpoint_inflight"].values()) <= 4 for e in snapshots)
            summary.append(dict(topology=topology, arm=arm, k=group["selected_k"],
                jct_s=[c["jct_s"] for c in part], median_jct_s=statistics.median(c["jct_s"] for c in part),
                policy_us_per_call=[c["policy_s"] / c["policy_calls"] * 1e6 for c in part],
                process_cpu_s=[c["process_cpu_s"] for c in part], assembly_s=[c["assembly_s"] for c in part],
                supply_limits=sorted(set(e["supply_limit"] for e in snapshots)),
                peak_owner_inflight=max(e["usage"]["active_requests"] for e in snapshots),
                replica_splits=[dict(Counter(e["endpoint_id"] for e in c["events"] if e["event"] == "replica_transport_started")) for c in part]))
    assert sum(c["count"] for c in cells) == 820 and len(cells) == 40
    for row in csv.reader((base / "gpu-samples.log").open()):
        row = [x.strip() for x in row]
        stamp = datetime.datetime.strptime(row[0], "%Y/%m/%d %H:%M:%S.%f").replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8))).timestamp()
        gpu.append(dict(utc_s=stamp, gpu=int(row[1]), utilization_percent=float(row[3]), memory_activity_percent=float(row[4]),
            power_w=float(row[5]), memory_used_mib=float(row[6]), memory_total_mib=float(row[7]),
            sm_clock_mhz=float(row[8]), memory_clock_mhz=float(row[9])))
    assert json.loads((base / "single-identity.json").read_text())["independent_devices"]
    assert json.loads((base / "dual-identity.json").read_text())["independent_devices"]
    data = dict(schema="semloom.replica_model_numeric_evidence.v1", evidence_type="model_request_subset_replay",
        source_sha=receipt["source_sha"], started_utc=receipt["started_utc"], ended_utc=receipt["ended_utc"],
        loading_s=receipt["loading_s"], model_posts=820, evaluation_posts=576, original_sql_queries=0,
        canonical_outputs_equal=True, unclosed_units=unclosed, controller_default_changed=False,
        server_success_posts=count_receipt["server_success_posts"], mfu="not_measured", query_p99="not_qualified",
        summary=summary, cells=cells, pressure=pressure, gpu=gpu)
    args.output.mkdir(exist_ok=False)
    with gzip.open(args.output / "raw.json.gz", "wt", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    (args.output / "summary.json").write_text(json.dumps({k:v for k,v in data.items() if k not in ("cells", "pressure", "gpu")}, indent=2) + "\n")
    print(json.dumps(dict(audited_cells=len(cells), model_posts=820, canonical_rows=len(reference), unclosed_units=unclosed)))


if __name__ == "__main__":
    main()
