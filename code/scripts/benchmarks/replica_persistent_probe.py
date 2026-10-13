"""Finite persistent-owner factor and observer-cost diagnostics; localhost only."""

from collections import Counter
from contextlib import ExitStack
from dataclasses import asdict, replace
import gzip
import hashlib
import json
import platform
from pathlib import Path
import statistics
import sys
import sqlite3
import subprocess
import time

CODE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE_ROOT))
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.replica_adaptation_probe import _Endpoint
from src.experiments.replica_model_replay import ReplicaReplayOwner, endpoint_rates, replay_batch
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.model_replicas import ModelReplica, ModelReplicaSet
from src.scheduling.endpoint_routing.replica_adaptation import ReplicaAdaptationConfig
from src.scheduling.runtime.observations import CachedMetricsObservationProvider
from src.baselines.common.redact import redact_text


ARMS = ("round_robin", "adaptive_route", "rr_supply", "adaptive_supply", "fixed_rate")
COUNTS = dict(change=256, homogeneous=64, no_ready=40, client=32, tail=12)


def require(value, message):
    if not value:
        raise ValueError(message)


def arrivals(case, count):
    if case == "no_ready":
        return [0.0] * (count // 2) + [.6] * (count - count // 2)
    if case == "client":
        return [.02 * i for i in range(count)]
    return [0.0] * count


def main(output, *, started=None, causal_pair=False):
    output = Path(output)
    output.mkdir(exist_ok=False)
    started = time.time() if started is None else started
    window = 30 if causal_pair else 120
    cutoff, cleanup_deadline = started + window - 15, started + window - 5
    expected = 1808 if causal_pair else 8156
    maximum = 1920 if causal_pair else 8200
    scenarios = {"change": 256} if causal_pair else COUNTS
    arms = ("round_robin", "adaptive_route") if causal_pair else ARMS
    budget = CellBudgetLedger.create(output / "budget.sqlite", AttemptBudget("persistent-replica", maximum), deadline_utc=started + window)
    source_hashes = {str(p.relative_to(CODE_ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (CODE_ROOT / 'src/experiments/replica_model_replay.py', CODE_ROOT / 'src/experiments/replica_adaptation_probe.py',
                  CODE_ROOT / 'src/execution_provider/adapters/replica_feedback.py',
                  CODE_ROOT / 'src/scheduling/endpoint_routing/replica_adaptation.py', Path(__file__))}
    base_sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=CODE_ROOT, text=True).strip()
    origin = [None]
    calls = tuple(dict(source_sequence=i, source_row_id=i, source_call_id=f"fixture-{i}",
        body=json.dumps(dict(model="fixture", messages=[dict(role="user", content=f"row-{i}")],
        temperature=0, max_tokens=8), separators=(",", ":")).encode()) for i in range(256))
    tuning = ReplicaAdaptationConfig(sample_interval_s=.05, min_rate_support_s=.05)
    results, calibrations, signatures = [], [], []
    owners = []
    primary = None
    current_label = None
    with ExitStack() as stack:
        sites = [stack.enter_context(_Endpoint(str(i), speed, [1] * 256)) for i, speed in enumerate((.010, .020))]
        replicas = tuple(ModelReplica(str(i), s.url, "localhost-fixture", (), "a" * 64, 4, 4) for i, s in enumerate(sites))
        directory = ModelReplicaSet("fixture", replicas, 8, 8)
        config = FixedModelConfig(sites[0].url, "fixture", 5000)

        def close(owner):
            if owner.closed:
                return True
            remaining_s = cleanup_deadline - time.time()
            return remaining_s > 0 and owner.close(min(5, remaining_s / 2))

        def create(arm, k, samples=None, *, flush=True):
            prior = {e: replace(s, rate_observed_at_s=time.monotonic()) for e, s in (samples or {}).items()}
            providers = {s.endpoint_id: CachedMetricsObservationProvider(s.metrics, min_sample_interval_s=.05) for s in sites}
            owner = ReplicaReplayOwner(config, directory, arm=arm, requests=k, max_tasks=256,
                providers=providers, adaptation=tuning, prior=prior or None,
                fixed_rates=samples if arm == "fixed_rate" else None, flush_each=flush)
            owners.append(owner)
            return owner

        def run(label, case, owner, count):
            nonlocal current_label
            current_label = label
            require(time.time() < cutoff, "persistent fixture deadline reached")
            budget.reserve_unit(label, count)
            unit = budget.claim_shared_unit(label)
            for site in sites:
                site.records.clear()
            try:
                cell = replay_batch(config, directory, calls[:count], arm=owner.arm, requests=owner.requests,
                    unit=unit, output=output / label, stop_new_post_utc=cutoff, owner=owner,
                    arrival_s=arrivals(case, count), query_started=lambda t: origin.__setitem__(0, t),
                    batch_deadline_utc=cutoff, cleanup_deadline_utc=cleanup_deadline)
                records = [r for s in sites for r in s.records]
                require(Counter(r["row"] for r in records) == Counter(range(count)), "fixture original-call multiplicity differs")
                require(all(r["payload_sha256"] == hashlib.sha256(calls[r["row"]]["body"]).hexdigest() for r in records), "fixture request bytes differ")
                require(all(s.peak_http <= 4 and s.peak_running <= 2 for s in sites), "fixture endpoint hard capacity exceeded")
                for event in cell["events"]:
                    usage = event.get("usage", {})
                    require(usage.get("active_requests", 0) <= owner.requests and usage.get("active_work", 0) <= owner.requests,
                        "fixture Core global capacity exceeded")
                    if event["event"] == "core_supply_snapshot":
                        require(all(v <= 4 for v in event["endpoint_inflight"].values()), "fixture Core endpoint capacity exceeded")
                require(all(o["content_sha256"] == hashlib.sha256(f'row-{o["source_sequence"]}'.encode()).hexdigest()
                    and o["finish_reason"] == "stop" and o["usage"] == dict(prompt_tokens=1, completion_tokens=1, total_tokens=2)
                    for o in cell["outcomes"]), "fixture canonical response differs")
                cell.update(label=label, case=case, query_started_s=origin[0],
                    arm_adaptation=asdict(owner.directory.adaptation) if owner.directory.adaptation is not None else None,
                    service_records={s.endpoint_id: list(s.records) for s in sites})
                with gzip.open(output / (label + ".json.gz"), "wt") as handle:
                    json.dump(cell, handle, separators=(",", ":"))
                results.append(cell)
                print(json.dumps(dict(label=label, count=count, k=owner.requests, jct_s=cell["jct_s"], allocated=budget.snapshot()["allocated_requests"])), flush=True)
                return cell
            finally:
                budget.close_shared_unit(label)

        try:
            for case, count in scenarios.items():
                sites[0].seconds_per_work = .010
                sites[1].seconds_per_work = .010 if case == "homogeneous" else .020
                case_cal = []
                for k in (2, 4, 8):
                    owner = create("round_robin", k)
                    try:
                        case_cal.append(run(f"{case}-cal-k{k}", case, owner, 32 if case == "client" else 80))
                    finally:
                        require(close(owner), "calibration owner did not close")
                calibrations.extend(case_cal)
                selected = min(case_cal, key=lambda c: c["jct_s"])
                samples = endpoint_rates(selected["events"], ("0", "1"), with_support=True)
                require(all(tuning.supports_rate(s) for s in samples.values()), "independent calibration lacks rate support")
                signatures.append(dict(case=case, selected_k=selected["requests"], calibration_label=selected["label"],
                    samples={e: asdict(s) for e, s in samples.items()}, common_tuning=asdict(tuning),
                    arm_overrides={a: dict(adapt_supply=a in ("rr_supply", "adaptive_supply")) for a in arms},
                    capacities=dict(endpoint_c=4, endpoint_w=4)))
                group = {arm: create(arm, selected["requests"], samples) for arm in arms}
                try:
                    for arm in arms:
                        run(f"{case}-warm-{arm}", "warm", group[arm], 16)
                    if case == "change":
                        sites[0].seconds_per_work = lambda at: .060 if .2 <= at - origin[0] < .7 else .010
                    orders = ((arms, arms[::-1], arms[::-1]) if causal_pair else
                        (arms, arms[::-1], ("rr_supply", "fixed_rate", "round_robin", "adaptive_supply", "adaptive_route")))
                    for repeat, order in enumerate(orders):
                        for arm in order:
                            run(f"{case}-measure-{repeat}-{arm}", case, group[arm], count)
                finally:
                    for owner in group.values():
                        require(close(owner), "measurement owner did not close")
            sites[0].seconds_per_work, sites[1].seconds_per_work = .010, .020
            for repeat in range(0 if causal_pair else 2):
                owner = create("adaptive_route", 8)
                try:
                    run(f"cold-{repeat}", "cold", owner, 128)
                finally:
                    require(close(owner), "cold owner did not close")
            for repeat in range(0 if causal_pair else 3):
                for flush in ((True, False) if repeat % 2 == 0 else (False, True)):
                    owner = create("round_robin", 8, flush=flush)
                    try:
                        run(f"trace-{repeat}-{'sync' if flush else 'buffered'}", "trace", owner, 64)
                    finally:
                        require(close(owner), "observer owner did not close")
            require(budget.snapshot()["allocated_requests"] == expected, "fixture finite matrix count differs")
            summary = []
            for case in scenarios:
                for arm in arms:
                    cells = [c for c in results if c["case"] == case and c["arm"] == arm and "-measure-" in c["label"]]
                    summary.append(dict(case=case, arm=arm, k=cells[0]["requests"],
                        jct_s=[c["jct_s"] for c in cells], median_jct_s=statistics.median(c["jct_s"] for c in cells),
                        policy_us=[c["policy_s"] * 1e6 / c["policy_calls"] for c in cells],
                        diagnostics_s=[c["diagnostics_s"] for c in cells], feedback_s=[c["feedback_s"] for c in cells]))
            with gzip.open(output / "raw.json.gz", "wt") as handle:
                json.dump(dict(signatures=signatures, summary=summary, cells=results), handle, separators=(",", ":"))
        except BaseException as error:
            primary = error
            (output / "failure.json").write_text(redact_text(json.dumps(dict(type=type(error).__name__, message=str(error),
                completed=[c["label"] for c in results], failed_cell=current_label, budget=budget.snapshot()), indent=2)))
            raise
        finally:
            cleanup = []
            for owner in owners:
                try:
                    cleanup.append(close(owner))
                except BaseException as error:
                    cleanup.append(type(error).__name__)
                    if primary is not None:
                        primary.add_note("fixture owner cleanup: " + type(error).__name__)
            with sqlite3.connect(f'file:{output / "budget.sqlite"}?mode=ro', uri=True) as conn:
                durable_reserved = conn.execute('SELECT COUNT(*) FROM shared_requests').fetchone()[0]
            failed_cleanup = output / str(current_label) / "cleanup.json"
            (output / "receipt.json").write_text(json.dumps(dict(evidence_type="software_fixture_persistent_owner",
                model_requests=0, gpu_count=0, server_load=False, completed_cell_posts=sum(c["count"] for c in results),
                durable_reserved_attempts=durable_reserved,
                failed_cell=None if primary is None else dict(label=current_label,
                    cleanup=json.loads(failed_cleanup.read_text()) if failed_cleanup.is_file() else None),
                budget=budget.snapshot(), duration_s=time.time() - started, all_owners_clean=all(v is True for v in cleanup),
                cleanup=cleanup, source_hashes=source_hashes, base_sha=base_sha,
                python=sys.version, platform=platform.platform(), causal_pair=causal_pair,
                deadlines=dict(post=cutoff, cleanup=cleanup_deadline, process=started+window)), indent=2))


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "--worker":
        main(sys.argv[2], started=float(sys.argv[3]), causal_pair=sys.argv[4] == "pair")
    elif len(sys.argv) == 2 or (len(sys.argv) == 3 and sys.argv[1] == "--causal-pair"):
        pair = len(sys.argv) == 3
        output_path = sys.argv[-1]
        window = 30 if pair else 120
        started = time.time()
        try:
            subprocess.run([sys.executable, __file__, "--worker", output_path, str(started), "pair" if pair else "full"],
                check=True, timeout=max(0, started+window-time.time()))
        except subprocess.TimeoutExpired:
            output = Path(output_path)
            if output.is_dir():
                (output / "supervisor-timeout.json").write_text(json.dumps(dict(hard_process_deadline=started+window,
                    cleanup_confirmed=False, budgets_refunded=False)))
            raise
    else:
        raise SystemExit("usage: replica_persistent_probe.py [--causal-pair] <new-private-directory-outside-git>")
