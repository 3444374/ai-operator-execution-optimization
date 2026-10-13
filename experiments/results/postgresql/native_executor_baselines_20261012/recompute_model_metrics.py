"""Recompute finite model-smoke statistics from the complete public observations."""
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path


def distribution(values: list[float]) -> dict:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("empty measurement population")
    def nearest(q: float) -> float:
        return ordered[math.ceil(q * len(ordered)) - 1]
    return {
        "n": len(ordered), "unit": "seconds", "P50": nearest(.5),
        "P95": nearest(.95), "P99": nearest(.99), "max": ordered[-1],
        "quantile": "nearest rank", "P99_is_sample_maximum": nearest(.99) == ordered[-1],
        "stable_tail_estimate": False,
    }


def recompute(root: Path, dual: bool = False) -> dict:
    prefix = "dual" if dual else "model"
    with (root / (prefix + "_queries.csv")).open(newline="") as handle:
        queries = list(csv.DictReader(handle))
    with (root / (prefix + "_requests.csv")).open(newline="") as handle:
        requests = list(csv.DictReader(handle))
    if dual:
        with (root / "dual_inner_requests.csv").open(newline="") as handle:
            inner = list(csv.DictReader(handle))
    else:
        inner = requests
    if len({(r["unit_id"], r["gateway_request_id"]) for r in requests}) != len(requests):
        raise ValueError("duplicate observed HTTP occurrence")
    if any(int(r["http_status"]) != 200 or int(r["retry_count"]) for r in requests):
        raise ValueError("non-success or retry in the successful round")
    names = ("gateway_request_e2e", "model_http", "gateway_before_dispatch", "gateway_dispatch_to_body")
    result = {}
    for arm in dict.fromkeys(q["arm"] for q in queries):
        qs = [q for q in queries if q["arm"] == arm and q["phase"] == "measurement"]
        rs = [r for r in requests if r["arm"] == arm and r["phase"] == "measurement"]
        observed = [r for r in inner if r["arm"] == arm and r["phase"] == "measurement"]
        if sum(int(q["actual_posts"]) for q in qs) != len(rs):
            raise ValueError("measured query/request count mismatch")
        durations = [float(q["entry_to_full_consumption_seconds"]) for q in qs]
        result[arm] = {
            "query_samples": len(qs), "request_samples": len(rs), "query": distribution(durations),
            "requests": {n: distribution([float(r[n + "_seconds"]) for r in
                         (rs if n == "model_http" else observed)]) for n in names},
            "throughput_rows_per_second": sum(int(q["rows"]) for q in qs) / sum(durations),
            "errors": sum(int(q["errors"]) for q in qs),
            "timeouts": sum(int(q["timeouts"]) for q in qs),
        }
    return {"observed_queries": len(queries), "observed_requests": len(requests), "measured_statistics": result}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--check", action="store_true", help="Compare with stored model_results.json")
    parser.add_argument("--dual", action="store_true", help="Use dual-GPU common and inner observations")
    args = parser.parse_args()
    result = recompute(args.root, args.dual)
    if args.check:
        expected = json.loads((args.root / ("dual_model_results.json" if args.dual else "model_results.json")).read_text())
        if result["measured_statistics"] != expected["measured_statistics"]:
            raise ValueError("stored statistics do not match the original observations")
        if result["observed_requests"] != expected["new_round"]["actual_model_posts"]:
            raise ValueError("stored POST count mismatch")
        if result["observed_queries"] != expected["new_round"]["queries"]:
            raise ValueError("stored query count mismatch")
        print("18 queries / 144 requests; stored measured statistics match all observations")
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
