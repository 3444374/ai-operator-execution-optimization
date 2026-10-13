"""Finite localhost capacity-change/recovery diagnostic with the common transport boundary."""

from contextlib import ExitStack
from dataclasses import asdict
import gzip
import hashlib
import json
from pathlib import Path
import sys
import time

CODE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE_ROOT))
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.replica_adaptation_probe import _Endpoint
from src.experiments.replica_model_replay import endpoint_rates, replay_batch
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.model_replicas import ModelReplica, ModelReplicaSet


def main(output):
    output = Path(output)
    output.mkdir(exist_ok=False)
    start, cutoff = time.time(), time.time() + 90
    budget = CellBudgetLedger.create(output / "budget.sqlite", AttemptBudget("capacity-change", 1800), deadline_utc=cutoff)
    origin = [None]
    calls = tuple(dict(source_sequence=i, source_row_id=i, source_call_id=f"fixture-{i}",
        body=json.dumps(dict(model="fixture", messages=[dict(role="user", content=f"row-{i}")],
        temperature=0, max_tokens=8), separators=(",", ":")).encode()) for i in range(96))
    results, calibration = [], []
    with ExitStack() as stack:
        sites = [stack.enter_context(_Endpoint(str(i), speed, [1] * 96)) for i, speed in enumerate((0.006, 0.018))]
        replicas = tuple(ModelReplica(str(i), s.url, "localhost-fixture", (), "a" * 64, 4, 4) for i, s in enumerate(sites))
        directory = ModelReplicaSet("fixture", replicas, 8, 8)
        config = FixedModelConfig(sites[0].url, "fixture", 2000)

        def run(label, *, k, arm, count=96, rates=None):
            budget.reserve_unit(label, count)
            unit = budget.claim_shared_unit(label)
            for s in sites:
                s.records.clear()
            try:
                result = replay_batch(config, directory, calls[:count], arm=arm, requests=k,
                    unit=unit, output=output / label, stop_new_post_utc=cutoff, fixed_rates=rates,
                    query_started=lambda value: origin.__setitem__(0, value))
                result.update(label=label, query_started_s=origin[0], service_records={s.endpoint_id: list(s.records) for s in sites})
                assert sum(len(s.records) for s in sites) == count
                assert all(s.peak_http <= 4 and s.peak_running <= 2 for s in sites)
                with gzip.open(output / (label + ".json.gz"), "wt") as f:
                    json.dump(result, f, separators=(",", ":"))
                print(json.dumps(dict(label=label, k=k, jct_s=result["jct_s"], allocated=budget.snapshot()["allocated_requests"])), flush=True)
                return result
            finally:
                budget.close_shared_unit(label)

        for k in (2, 4, 8):
            calibration.append(run("cal-k" + str(k), k=k, arm="round_robin", count=24))
        best_k = min(calibration, key=lambda r: r["jct_s"])["requests"]
        # Fixed service changes relative to first offer. A request already in
        # service retains its non-preemptive duration. No extra calls create load.
        def changing_a(service_at):
            elapsed = service_at - origin[0]
            return 0.040 if 0.12 <= elapsed < 0.40 else 0.006
        sites[0].seconds_per_work = changing_a
        arms = ("round_robin", "fixed_rate", "adaptive_route")
        for k in (2, 8):
            rates = endpoint_rates(next(c for c in calibration if c["requests"] == k)["events"], ("0", "1"), with_support=True)
            for repeat, order in enumerate((arms, arms[::-1], ("fixed_rate", "round_robin", "adaptive_route"))):
                for arm in order:
                    results.append(run(f"k{k}-repeat{repeat}-{arm}", k=k, arm=arm, rates=rates))
    (output / "receipt.json").write_text(json.dumps(dict(evidence_type="software_fixture", gpu_count=0,
        model_requests=0, localhost_posts=1800, budget=budget.snapshot(), duration_s=time.time() - start,
        a_service_seconds=[0.006, 0.040, 0.006], transitions_s=[0.12, 0.40], b_service_seconds=0.018,
        fixed_work_per_call=1, post_count_per_measurement=96, repeats=3, static_selected_k=best_k,
        k2_is_mechanism_diagnostic_not_optimal_static=True, target_drain_s=None,
        rates_boundary="same_existing_completion_accumulator_transport_clock", source_hashes={
            str(p.relative_to(CODE_ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in (
                CODE_ROOT/'src/experiments/replica_model_replay.py',
                CODE_ROOT/'src/experiments/replica_adaptation_probe.py', Path(__file__))}), indent=2))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: replica_capacity_change_probe.py <new-private-directory-outside-git>")
    main(sys.argv[1])
