"""Run one coordinated topology's finite preserved Q2-request replay group."""

import argparse
from dataclasses import replace
from functools import partial
import gzip
import json
from pathlib import Path
import sys
import time

CODE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE_ROOT))

from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.replica_model_replay import JsonEvents, endpoint_rates, load_preserved_calls, replay_batch
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.model_replicas import ModelReplicaSet
from src.execution_provider.adapters.replica_metrics import service_pressure
from src.observability.metrics.vllm import scrape_prometheus_metrics
from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--replicas", required=True, type=Path)
    parser.add_argument("--budget", required=True, type=Path)
    parser.add_argument("--budget-id", required=True)
    parser.add_argument("--topology", choices=("single", "dual"), required=True)
    parser.add_argument("--stop-new-post-utc", type=float, required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    calls = load_preserved_calls(args.source, args.source_sha256)
    original = ModelReplicaSet.load(args.replicas)
    if len(original.replicas) != 2 or any(r.max_active_requests != 4 or r.max_active_work != 4 for r in original.replicas):
        parser.error("this plan requires two declared 4-request replicas")
    chosen = original.replicas[:1] if args.topology == "single" else original.replicas
    directory = replace(original, replicas=chosen, max_active_requests=4 * len(chosen), max_active_work=4 * len(chosen))
    budget = CellBudgetLedger(args.budget, AttemptBudget(args.budget_id, 820))
    args.output.mkdir(exist_ok=False)
    config = FixedModelConfig(chosen[0].endpoint_url, directory.model_id, 30000)
    results = []
    pressure = JsonEvents(args.output / "pressure.jsonl")
    providers = {}

    def sample(replica):
        url = replica.endpoint_url.rsplit("/v1/", 1)[0] + "/metrics"
        start = time.monotonic()
        snapshot = scrape_prometheus_metrics(url, timeout_s=1)
        pressure(dict(event="service_metrics", endpoint_id=replica.endpoint_id,
                      elapsed_s=time.monotonic() - start, metrics=snapshot))
        return service_pressure(snapshot)

    def run(label, group, *, arm="round_robin", requests=1, rates=None, subset=None):
        if time.time() >= args.stop_new_post_utc:
            raise TimeoutError("replay group reached its POST cutoff")
        selected_calls = tuple(calls[group[0]:group[1]])
        selected_directory = directory if subset is None else replace(directory, replicas=subset,
            max_active_requests=4 * len(subset), max_active_work=4 * len(subset))
        budget.reserve_unit(args.topology + "-" + label, len(selected_calls))
        unit = budget.claim_shared_unit(args.topology + "-" + label)
        try:
            result = replay_batch(config, selected_directory, selected_calls, arm=arm, requests=requests,
                unit=unit, output=args.output / label, stop_new_post_utc=args.stop_new_post_utc,
                providers={r.endpoint_id: providers[r.endpoint_id] for r in selected_directory.replicas}, fixed_rates=rates)
        finally:
            budget.close_shared_unit(args.topology + "-" + label)
        result.update(label=label, topology=args.topology)
        results.append(result)
        with gzip.open(args.output / (label + ".json.gz"), "wt", encoding="utf-8") as f:
            json.dump(result, f, separators=(",", ":"))
        print(json.dumps(dict(label=label, count=result["count"], jct_s=result["jct_s"], arm=arm,
                              allocated_requests=budget.snapshot()["allocated_requests"])), flush=True)
        return result

    try:
        for replica in directory.replicas:
            providers[replica.endpoint_id] = NonBlockingMetricsObservationProvider(partial(sample, replica),
                poll_interval_s=0.25, stale_after_s=2, close_timeout_s=1.5)
        # The original service A stays alive; only B receives dual protocol calls.
        run("protocol", (60, 62) if args.topology == "single" else (62, 64),
            subset=chosen[:1] if args.topology == "single" else chosen[1:])
        candidates = (1, 2, 4) if args.topology == "single" else (2, 4, 8)
        calibration = [run("cal-k" + str(k), (24, 48), requests=k) for k in candidates]
        selected = min(calibration, key=lambda r: r["jct_s"])
        rates = endpoint_rates(selected["events"], [r.endpoint_id for r in chosen], with_support=True)
        k = selected["requests"]
        arms = ("round_robin", "fixed_rate", "adaptive_route", "adaptive_supply")
        for arm in arms:
            run("warm-" + arm, (48, 60), arm=arm, requests=k, rates=rates)
        orders = (arms, arms[::-1], ("fixed_rate", "round_robin", "adaptive_supply", "adaptive_route"))
        for repeat, order in enumerate(orders):
            for arm in order:
                run(f"measure-{repeat}-{arm}", (0, 24), arm=arm, requests=k, rates=rates)
        (args.output / "group.json").write_text(json.dumps(dict(topology=args.topology, selected_k=k,
            fixed_rates={k: v.service_rate_work_units_s for k, v in rates.items()},
            source_sha256=args.source_sha256, completed_cells=len(results),
            budget=budget.snapshot()), indent=2))
        return 0
    except BaseException as error:
        (args.output / "failure.json").write_text(json.dumps(dict(exception_type=type(error).__name__,
            completed_labels=[r["label"] for r in results], budget=budget.snapshot()), indent=2))
        raise
    finally:
        for provider in providers.values():
            provider.close()
        pressure.close()


if __name__ == "__main__":
    raise SystemExit(main())
