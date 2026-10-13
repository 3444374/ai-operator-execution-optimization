"""Read-only derivation of first-sample age, unique updates and original-call chains."""

import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3


def require(value, message):
    if not value:
        raise ValueError(message)


def summarize(raw_path, receipt_path, *, budget_path=None, overlay_path=None, base_sha=None):
    raw = Path(raw_path).read_bytes()
    data = json.loads(gzip.decompress(raw))
    receipt = json.loads(Path(receipt_path).read_bytes())
    if base_sha is not None and receipt.get("base_sha") is not None:
        require(base_sha == receipt["base_sha"], "receipt base SHA differs")
    cells = data["cells"]
    count = sum(c["count"] for c in cells)
    require(count == receipt.get("completed_cell_posts", receipt.get("posts")) and receipt["all_owners_clean"], "incomplete fixture receipt")
    unclosed = durable = None
    if budget_path is not None:
        with sqlite3.connect(f'file:{Path(budget_path).resolve()}?mode=ro', uri=True) as conn:
            durable = conn.execute('SELECT COUNT(*) FROM shared_requests').fetchone()[0]
            unclosed = conn.execute('SELECT COUNT(*) FROM shared_units LEFT JOIN closed_shared_units USING(unit_id) WHERE closed_shared_units.unit_id IS NULL').fetchone()[0]
        require(durable == count and unclosed == 0, "fixture budget is incomplete")
    canonical, evaluations, chains = {}, [], []
    for c in cells:
        require(len(c["outcomes"]) == c["count"] and len(c["attempts"]) == c["count"], "fixture occurrence counts differ")
        for o in c["outcomes"]:
            value = (o["content_sha256"], o["finish_reason"], o["usage"], o["body_sha256"])
            require(canonical.setdefault(o["source_sequence"], value) == value, "fixture canonical/body equality differs")
        if "-measure-" not in c["label"]:
            continue
        events = c["events"]
        route = next(e for e in events if e["event"] == "route_decision")
        supply = [e for e in events if e["event"] == "supply_decision" and e["monotonic_s"] <= route["monotonic_s"]][-1]
        samples = {k: dict(age_s=None if v["rate_observed_at_s"] is None else route["monotonic_s"] - v["rate_observed_at_s"], sample=v)
            for k, v in supply["feedback"].items()}
        feedback_counts = {k: {f: c["feedback_after"][k][f] - c["feedback_before"][k][f]
            for f in ("rate_updates", "tail_corrections", "low_support_holds", "successful_work", "successful_requests")} for k in c["feedback_after"]}
        supply_counts = {k: {f: c["supply_after"][k][f] - c["supply_before"][k][f]
            for f in ("down", "recovery", "hold", "fallback")} for k in c["supply_after"]}
        evaluations.append(dict(label=c["label"], arm=c["arm"], case=c["case"], first_route=route,
            first_supply_feedback=samples, route_reasons=dict(Counter(e["decision"]["reason"] for e in events if e["event"] == "route_decision")),
            feedback_counts=feedback_counts, supply_counts=supply_counts,
            no_ready_snapshots=sum(e.get("queued") == 0 for e in events if e["event"] == "core_supply_snapshot")))
        if c["case"] != "change":
            continue
        records = c["service_records"]["0"]
        degraded = next(x for x in records if x["seconds_per_work"] == .06)
        restored = next(x for x in records if x["seconds_per_work"] == .01 and x["service_at_s"] - c["query_started_s"] >= .7)
        last = c["feedback_before"]["0"]["rate_updates"]
        updates = []
        for e in events:
            if e["event"] == "feedback_updated" and e["endpoint_id"] == "0" and e["diagnostics"]["rate_updates"] > last:
                updates.append(e)
                last = e["diagnostics"]["rate_updates"]
        phases = []
        for name, record in (("degrade", degraded), ("restore", restored)):
            observed = next((e for e in updates if e["sample"]["rate_observed_at_s"] >= record["done_at_s"]), None)
            later = next((e for e in events if e["event"] == "route_decision" and observed is not None and e["monotonic_s"] > observed["monotonic_s"]), None)
            outcome = next_record = None
            if later is not None:
                row = int(later["request_id"].rsplit(":", 1)[1])
                outcome = next(o for o in c["outcomes"] if o["source_sequence"] == row)
                next_record = next(x for x in c["service_records"][later["decision"]["endpoint_id"]] if x["row"] == row)
            phases.append(dict(phase=name, original_service=record, feedback=observed, subsequent_original_route=later,
                actual_routing_feedback=None if later is None else later.get("routing_feedback"),
                subsequent_original_service=next_record, subsequent_result=outcome))
        chains.append(dict(label=c["label"], updates_by_phase=[sum(lo <= e["sample"]["rate_observed_at_s"] - c["query_started_s"] < hi for e in updates)
            for lo, hi in ((0, .2), (.2, .7), (.7, 99))], chains=phases))
    stale = {a: sum(any(v["age_s"] is not None and v["age_s"] > 2 for v in e["first_supply_feedback"].values())
        for e in evaluations if e["arm"] == a) for a in ("adaptive_route", "adaptive_supply", "fixed_rate")}
    return dict(evidence_type="offline_retained_software_fixture", raw_sha256=hashlib.sha256(raw).hexdigest(),
        actual_source_overlay_sha256=None if overlay_path is None else hashlib.sha256(Path(overlay_path).read_bytes()).hexdigest(),
        base_sha=receipt.get("base_sha", base_sha), durable_reserved_attempts=durable, unclosed_units=unclosed, cells=len(cells),
        canonical_and_fixture_response_body_hashes_equal=True, http_envelope_bytes_equal="not_qualified",
        stale_first_measurements=stale, evaluations=evaluations, change_chains=chains, summary=data["summary"],
        observer=[{k: c[k] for k in ("label", "trace_flush_each", "jct_s", "full_replay_s", "diagnostics_s", "feedback_s", "diagnostics_close_s", "policy_s")}
            for c in cells if c["case"] == "trace"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=Path)
    parser.add_argument("receipt", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--budget", type=Path)
    parser.add_argument("--overlay", type=Path)
    parser.add_argument("--base-sha")
    args = parser.parse_args()
    result = summarize(args.raw, args.receipt, budget_path=args.budget, overlay_path=args.overlay, base_sha=args.base_sha)
    with args.output.open("x") as handle:
        json.dump(result, handle, indent=2)
        handle.write("\n")
