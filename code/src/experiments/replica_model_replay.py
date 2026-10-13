"""Finite preserved-request replay through real replica assembly, without an SQL method claim."""

from collections import Counter
from dataclasses import asdict, replace
import base64
import hashlib
import json
import math
from pathlib import Path
import threading
import time

from ..execution_provider.adapters.full_response import decode_full_response
from ..execution_provider.adapters.model_replicas import build_model_replica_execution
from ..execution_provider.adapters.replica_feedback import ReplicaCompletionFeedback
from ..execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task
from ..scheduling.core.session_contract import State
from ..scheduling.endpoint_routing.replica_adaptation import ReplicaAdaptationConfig, ReplicaFeedback
from ..execution_provider.adapters.model_replicas import StaticReplicaRouter
from .attempt_ledger import observe_async_http_posts
from .attempt_ledger import AttemptBudget
from .cell_budget import CellBudgetLedger


def reuse_unused_budget(path, budget_id, deadline_utc):
    ledger = CellBudgetLedger(path, AttemptBudget(budget_id, 820))
    snapshot = ledger.snapshot()
    if snapshot["allocated_requests"] or snapshot["units"] or snapshot["deadline_utc"] != deadline_utc:
        raise ValueError("continuation requires the same untouched ledger and original deadline")
    return ledger


def load_preserved_calls(path, expected_sha256):
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("preserved replay source digest differs")
    calls = [json.loads(line) for line in raw.splitlines()]
    if len(calls) != 64 or [c["source_sequence"] for c in calls] != list(range(64)):
        raise ValueError("preserved replay requires the fixed first 64 occurrences")
    for call in calls:
        call["body"] = base64.b64decode(call["payload"], validate=True)
        obj = json.loads(call["body"])
        expected = dict(model="Qwen2.5-7B-Instruct", temperature=0, top_p=1,
                        stop=["\n"], max_completion_tokens=8, logprobs=False)
        if {k: v for k, v in obj.items() if k != "messages"} != expected:
            raise ValueError("preserved replay generation fields differ")
    return tuple(calls)


class JsonEvents:
    """One finite writer shared by the Core owner, HTTP loop and metrics sampler."""
    def __init__(self, path, *, flush_each=True):
        self.handle = Path(path).open("x", encoding="utf-8")
        self.lock = threading.Lock()
        self.events = []
        self.flush_each = flush_each
        self.ns = self.calls = 0

    def __call__(self, event):
        started = time.perf_counter_ns()
        value = dict(event)
        value.setdefault("monotonic_s", time.monotonic())
        value.setdefault("utc_s", time.time())
        with self.lock:
            self.events.append(value)
            self.handle.write(json.dumps(value, separators=(",", ":")) + "\n")
            if self.flush_each:
                self.handle.flush()
            self.ns += time.perf_counter_ns() - started
            self.calls += 1

    def close(self):
        self.handle.close()


class RecordedFeedback(ReplicaCompletionFeedback):
    """Record the exact existing accumulator clock edges for all experimental arms."""
    def __init__(self, endpoint_ids, config, observer, *, observations=None, prior=None):
        self.observer, self.edge = observer, None
        self.ns = self.calls = 0
        super().__init__(endpoint_ids, config, observations=observations, clock=self.record_clock, prior=prior)

    def record_clock(self):
        now = time.monotonic()
        self.observer(dict(self.edge, monotonic_s=now))
        return now

    def started(self, endpoint_id):
        started = time.perf_counter_ns()
        self.edge = dict(event="feedback_started", endpoint_id=endpoint_id)
        super().started(endpoint_id)
        self.ns += time.perf_counter_ns() - started
        self.calls += 1

    def finished(self, endpoint_id, work, *, successful):
        started = time.perf_counter_ns()
        self.edge = dict(event="feedback_finished", endpoint_id=endpoint_id,
                         work=work, successful=successful)
        super().finished(endpoint_id, work, successful=successful)
        self.observer(dict(event="feedback_updated", endpoint_id=endpoint_id,
            sample=asdict(self.latest(endpoint_id)), diagnostics=self.diagnostics(endpoint_id)))
        self.ns += time.perf_counter_ns() - started
        self.calls += 1


class _SupplyRoundRobin:
    """Keep the existing RR choice, applying only the pressure target's eligibility."""
    def __init__(self, directory, controller):
        self.router = StaticReplicaRouter(replace(directory, routing="round_robin", adaptation=None))
        self.controller = controller

    def route(self, request, topology, pool_id):
        self.controller._refresh()
        limits = self.controller.supply()
        eligible = replace(topology, endpoints=tuple(replace(e, available=e.available
            and e.running + e.waiting < limits[e.endpoint_id]) for e in topology.endpoints))
        return self.router.route(request, eligible, pool_id)

    def supply(self):
        return self.controller.supply()

    @property
    def _samples(self):
        return self.controller._samples


class ReplicaReplayOwner:
    """Experiment-only lifetime: reuse one existing execution over sequential sessions."""
    def __init__(self, config, directory, *, arm, requests, max_tasks, providers=None,
                 fixed_rates=None, prior=None, adaptation=None, flush_each=True):
        if arm not in ("round_robin", "fixed_rate", "adaptive_route", "adaptive_supply", "rr_supply"):
            raise ValueError("unknown replica replay arm")
        if arm == "fixed_rate" and (not fixed_rates
                or set(fixed_rates) != {r.endpoint_id for r in directory.replicas}
                or any(type(v) is not ReplicaFeedback for v in fixed_rates.values())):
            raise ValueError("frozen rates require measured calibration support for every replica")
        self.arm, self.requests, self.base_directory = arm, requests, directory
        self.observer = None
        self.failed = self.closed = False
        self.flush_each = flush_each
        tuning = replace(adaptation or ReplicaAdaptationConfig(),
            adapt_supply=arm in ("adaptive_supply", "rr_supply"))
        self.directory = replace(directory, max_active_requests=requests, max_active_work=requests,
            routing="round_robin" if arm == "round_robin" else "adaptive",
            adaptation=None if arm == "round_robin" else tuning)
        self.feedback = RecordedFeedback(tuple(r.endpoint_id for r in directory.replicas),
            tuning, self.observe, observations=providers, prior=prior)
        self.execution = build_model_replica_execution(config, replicas=self.directory,
            response_mode="full", max_tasks=max_tasks, observer=self.observe)
        self.execution.engine.backend._execute.__self__.feedback = self.feedback
        self.controller = self.execution.engine.policies.router
        if arm != "round_robin":
            self.controller.feedback = self.feedback.latest
        if arm == "fixed_rate":
            self.controller.feedback = lambda endpoint_id: replace(fixed_rates[endpoint_id], rate_observed_at_s=time.monotonic())
        router = (_SupplyRoundRobin(self.directory, self.controller) if arm == "rr_supply" else self.controller)
        self.timed = TimedPolicy(self.execution.engine.policies.admission, router, self.observe)
        self.execution.engine.policies = replace(self.execution.engine.policies, admission=self.timed, router=self.timed)

    def observe(self, event):
        if self.observer is not None:
            self.observer(event)

    def close(self, timeout=5.0):
        if self.closed:
            return True
        clean = self.execution.close(timeout)
        self.closed = clean
        return clean


class TimedPolicy:
    def __init__(self, admission, router, observer):
        self.admission, self.router = admission, router
        self.observer = observer
        self.ns = self.calls = 0

    @property
    def limit(self):
        return self.admission.limit

    def call(self, method, *args, **kwargs):
        start = time.perf_counter_ns()
        try:
            return method(*args, **kwargs)
        finally:
            self.calls += 1
            self.ns += time.perf_counter_ns() - start

    def decide(self, *args, **kwargs):
        result = self.call(self.admission.decide, *args, **kwargs)
        self.observer(dict(event="supply_decision", inflight=args[0], decision=asdict(result),
            endpoint_supply=self.router.supply() if hasattr(self.router, "supply") else None,
            feedback={k: asdict(v) for k, v in getattr(self.router, "_samples", {}).items()}))
        return result

    def route(self, *args, **kwargs):
        result = self.call(self.router.route, *args, **kwargs)
        self.observer(dict(event="route_decision", request_id=args[0].request_id, decision=asdict(result),
            routing_feedback={k: asdict(v) for k, v in getattr(self.router, "_samples", {}).items()}))
        return result


def endpoint_rates(events, endpoint_ids, *, with_support=False):
    """Same accumulator edges and work as online feedback; freeze the batch rate."""
    markers = {k: [] for k in endpoint_ids}
    completed = Counter()
    counts = Counter()
    for event in events:
        if event.get("event") not in ("feedback_started", "feedback_finished"):
            continue
        endpoint = event["endpoint_id"]
        if event["event"] == "feedback_started":
            markers[endpoint].append((event["monotonic_s"], 1))
        else:
            markers[endpoint].append((event["monotonic_s"], -1))
            if event["successful"]:
                completed[endpoint] += event["work"]
                counts[endpoint] += 1
    result = {}
    for endpoint, edges in markers.items():
        if not edges:
            raise ValueError("calibration did not reach every selected replica")
        active, busy, area, peak, previous = 0, 0.0, 0.0, 0, edges[0][0]
        for stamp, delta in sorted(edges):
            if active:
                busy += stamp - previous
                area += (stamp - previous) * active
            active += delta
            peak = max(peak, active)
            previous = stamp
        if active or busy <= 0 or completed[endpoint] <= 0:
            raise ValueError("calibration completion edges did not settle")
        rate = completed[endpoint] / busy
        result[endpoint] = (ReplicaFeedback(rate_observed_at_s=previous,
            service_rate_work_units_s=rate, rate_support_s=busy,
            rate_completed_requests=counts[endpoint], rate_completed_work=completed[endpoint],
            rate_mean_active_requests=area / busy, rate_peak_active_requests=peak,
            rate_sample_started_at_s=edges[0][0], rate_provenance="calibration") if with_support else rate)
    return result


def replay_batch(config, directory, calls, *, arm, requests, unit, output,
                 stop_new_post_utc, providers=None, fixed_rates=None, query_started=None,
                 owner=None, arrival_s=None, batch_deadline_utc=None, cleanup_deadline_utc=None):
    """One sealed batch, one original POST per occurrence; every response stays private."""
    for deadline in (batch_deadline_utc, cleanup_deadline_utc):
        if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline)):
            raise ValueError("replay deadlines must be finite UTC seconds")
    if (batch_deadline_utc is not None and cleanup_deadline_utc is not None
            and cleanup_deadline_utc < batch_deadline_utc):
        raise ValueError("cleanup deadline precedes batch deadline")
    if owner is None and arm == "fixed_rate" and (not fixed_rates
            or set(fixed_rates) != {r.endpoint_id for r in directory.replicas}
            or any(type(v) is not ReplicaFeedback for v in fixed_rates.values())):
        raise ValueError("frozen rates require measured calibration support for every replica")
    if owner is not None and (owner.arm != arm or owner.requests != requests
            or owner.base_directory != directory or owner.observer is not None or owner.failed or owner.closed):
        raise ValueError("persistent replay owner differs or is already in use")
    arrivals = [0.0] * len(calls) if arrival_s is None else arrival_s
    if (len(arrivals) != len(calls) or any(type(t) not in (int, float) or not 0 <= t < 60 for t in arrivals)
            or list(arrivals) != sorted(arrivals)):
        raise ValueError("replay arrival offsets must be finite and ordered")
    output = Path(output)
    output.mkdir(exist_ok=False)
    observer = JsonEvents(output / "events.jsonl", flush_each=True if owner is None else owner.flush_each)
    remaining = Counter()
    attempts, outcomes = [], []
    started = time.monotonic()
    cpu_start = time.process_time()
    persistent = owner is not None
    execution = flow = None
    wall_now = time.time()
    batch_end = min(started + 60, started + max(0, batch_deadline_utc - wall_now)) if batch_deadline_utc is not None else started + 60
    cleanup_end = started + max(0, cleanup_deadline_utc - wall_now) if cleanup_deadline_utc is not None else None
    primary = None
    clean = False
    result = None

    def close_timeout():
        if cleanup_end is None:
            return 5.0
        remaining_s = cleanup_end - time.monotonic()
        if remaining_s <= 0:
            raise TimeoutError("replay cleanup deadline reached")
        # Backend close may spend the timeout in both finalization and join.
        return min(5.0, remaining_s / 2)

    def supply_snapshot():
        records = tuple(execution.engine.capacity.records.values())
        queued = tuple(r for r in records if r.phase == "QUEUED")
        observer(dict(event="core_supply_snapshot", queued=len(queued),
            hard_dispatchable=execution.engine.capacity.any_dispatchable(queued, flow.limits),
            usage=asdict(execution.engine.capacity.usage()), supply_limit=timed.limit,
            endpoint_supply=router.supply() if hasattr(router, "supply") else None,
            endpoint_inflight={r.endpoint_id: sum(v.compute and v.endpoint == r.endpoint_id for v in records)
                               for r in directory.replicas}))

    def before_send(attempt, payload):
        # The existing observer reserves durably before entering this callback.
        # A failed callback sends nothing, and its reservation remains charged.
        if time.time() >= stop_new_post_utc:
            raise TimeoutError("replay POST cutoff reached")
        digest = hashlib.sha256(payload).hexdigest()
        if remaining[digest] < 1:
            raise ValueError("replay POST changed its bytes or multiplicity")
        remaining[digest] -= 1
        attempts.append(attempt)
        observer(dict(event="durable_post_reserved", attempt=attempt, payload_sha256=digest))

    query_start = None
    offered = 0

    def offer_ready():
        nonlocal offered
        end = offered
        while end < len(calls) and arrivals[end] <= time.monotonic() - query_start:
            end += 1
        if end == offered:
            return False
        offer = flow.offer(tasks[offered:end])
        if offer.accepted_prefix_count != end - offered:
            raise ValueError("finite replay input was not fully accepted")
        offered = end
        if offered == len(tasks):
            flow.end_input()
        return True

    try:
        owner = owner or ReplicaReplayOwner(config, directory, arm=arm, requests=requests,
            max_tasks=len(calls), providers=providers if arm == "adaptive_supply" else None, fixed_rates=fixed_rates)
        owner.observer = observer
        execution, feedback, timed = owner.execution, owner.feedback, owner.timed
        router = timed.router
        selected = owner.directory
        if time.monotonic() >= batch_end:
            raise TimeoutError("replay batch deadline reached")
        remaining = Counter(hashlib.sha256(c["body"]).hexdigest() for c in calls)
        before = dict(policy_ns=timed.ns, policy_calls=timed.calls, feedback_ns=feedback.ns,
            feedback_calls=feedback.calls, feedback={k: feedback.diagnostics(k) for k in feedback._rates},
            supply=owner.controller.diagnostics() if hasattr(owner.controller, "diagnostics") else {})
        flow = NativeTaskSession(execution, output.name, "preserved-q2-replay")
        tasks = tuple(prepare_native_task(c["body"], i, row_sequence=c["source_sequence"],
            call_id=c["source_call_id"], max_result_bytes=1048576) for i, c in enumerate(calls))
        query_start = time.monotonic()
        if query_started is not None:
            query_started(query_start)
        with observe_async_http_posts(unit, before_send):
            offer_ready()
            supply_snapshot()
            received = set()
            while len(received) < len(calls):
                if time.monotonic() >= batch_end:
                    raise TimeoutError("replay batch deadline reached")
                if offer_ready():
                    supply_snapshot()
                progress = flow.advance(len(calls))
                try:
                    for delivery in progress.deliveries:
                        index = delivery.key.sequence
                        if index in received or not 0 <= index < len(calls):
                            raise ValueError("replay response association differs")
                        received.add(index)
                        (output / f"response-{index:03d}.bin").write_bytes(delivery.result)
                        full = decode_full_response(delivery.result)
                        if full.status_code != 200:
                            raise ValueError("replay upstream returned a non-success response")
                        body = json.loads(full.body)
                        if len(body["choices"]) != 1 or type(body["choices"][0]["message"]["content"]) is not str:
                            raise ValueError("replay response choice representation differs")
                        call = calls[index]
                        if delivery.info.row_sequence != call["source_sequence"] or delivery.info.call_id != call["source_call_id"]:
                            raise ValueError("replay lost source occurrence identity")
                        content = body["choices"][0]["message"]["content"]
                        outcomes.append(dict(source_sequence=call["source_sequence"], source_row_id=call["source_row_id"],
                            source_call_id=call["source_call_id"], result_at_s=time.monotonic(),
                            body_sha256=hashlib.sha256(full.body).hexdigest(),
                            content_sha256=hashlib.sha256(content.encode()).hexdigest(),
                            finish_reason=body["choices"][0].get("finish_reason"), usage=body.get("usage")))
                finally:
                    flow.release(tuple(d.lease_id for d in progress.deliveries))
                supply_snapshot()
                if progress.state in (State.FAILED, State.CANCELLED):
                    raise RuntimeError("replay Core failed: " + str(progress.error))
                if len(received) < len(calls):
                    next_offer = max(0.0, arrivals[offered] - (time.monotonic() - query_start)) if offered < len(calls) else None
                    wait_s = max(0, batch_end - time.monotonic())
                    flow.wait(progress, timeout_s=wait_s if next_offer is None else min(wait_s, next_offer))
        consumed = time.monotonic()
        if any(remaining.values()) or unit.attempts != len(calls):
            raise ValueError("replay request budget and occurrences differ")
        flow.close(clean=True)
        execution.engine.advance()
        clean = not execution.engine.jobs.jobs and not execution.engine.capacity.records
        if not persistent:
            clean = owner.close(close_timeout()) and clean
        if not clean or execution.engine.capacity.usage().held_tasks:
            raise RuntimeError("replay cleanup could not be confirmed")
        result = dict(arm=arm, requests=requests, count=len(calls), jct_s=consumed - query_start,
            assembly_s=query_start - started, process_cpu_s=time.process_time() - cpu_start,
            policy_s=(timed.ns - before["policy_ns"]) / 1e9,
            policy_calls=timed.calls - before["policy_calls"], attempts=attempts,
            feedback_s=(feedback.ns - before["feedback_ns"]) / 1e9,
            feedback_calls=feedback.calls - before["feedback_calls"],
            diagnostics_s=observer.ns / 1e9, diagnostics_calls=observer.calls,
            feedback_timing_includes_its_diagnostics=True, trace_flush_each=owner.flush_each,
            feedback_before=before["feedback"], feedback_after={k: feedback.diagnostics(k) for k in feedback._rates},
            supply_before=before["supply"], supply_after=owner.controller.diagnostics() if hasattr(owner.controller, "diagnostics") else {},
            persistent_execution=persistent, outcomes=outcomes, events=observer.events, directory_sha256=selected.sha256)
        return result
    except BaseException as error:
        if owner is not None:
            owner.failed = True
        primary = error
        raise
    finally:
        if not clean and execution is not None:
            try:
                if flow is not None:
                    flow.close()
                end = min(time.monotonic() + 32, cleanup_end) if cleanup_end is not None else time.monotonic() + 32
                while time.monotonic() < end and any(not p.future.done() for p in execution.engine.backend._slots.values()):
                    execution.engine.advance()
                    execution.engine.wake.wait(execution.engine.wake.generation, max(0, min(.02, end - time.monotonic())))
                execution.engine.advance()
                if not execution.engine.backend.close(close_timeout()):
                    raise RuntimeError("replay backend cleanup unconfirmed")
            except BaseException as cleanup:
                if primary is not None:
                    primary.add_note("replay cleanup also failed: " + type(cleanup).__name__)
                else:
                    raise
        try:
            (output / "cleanup.json").write_text(json.dumps(dict(clean=clean,
                usage=asdict(execution.engine.capacity.usage()) if execution is not None else None,
                error=type(primary).__name__ if primary else None,
                outcomes=outcomes, reserved_attempts=attempts,
                durable_reserved_attempts=unit.attempts), separators=(",", ":")))
        except BaseException as cleanup:
            if owner is not None:
                owner.failed = True
            if primary is not None:
                primary.add_note("replay cleanup evidence also failed: " + type(cleanup).__name__)
            else:
                raise
        finally:
            closing = time.perf_counter_ns()
            try:
                observer.close()
            except BaseException as cleanup:
                if owner is not None:
                    owner.failed = True
                if primary is not None:
                    primary.add_note("replay observer close also failed: " + type(cleanup).__name__)
                else:
                    raise
            finally:
                if result is not None:
                    result["diagnostics_close_s"] = (time.perf_counter_ns() - closing) / 1e9
                    result["full_replay_s"] = time.monotonic() - started
                if owner is not None:
                    owner.observer = None
