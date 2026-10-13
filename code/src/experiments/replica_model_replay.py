"""Finite preserved-request replay through real replica assembly, without an SQL method claim."""

from collections import Counter
from dataclasses import asdict, replace
import base64
import hashlib
import json
from pathlib import Path
import threading
import time

from ..execution_provider.adapters.full_response import decode_full_response
from ..execution_provider.adapters.model_replicas import build_model_replica_execution
from ..execution_provider.adapters.replica_feedback import ReplicaCompletionFeedback
from ..execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task
from ..scheduling.core.session_contract import State
from ..scheduling.endpoint_routing.replica_adaptation import ReplicaAdaptationConfig, ReplicaFeedback
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
    def __init__(self, path):
        self.handle = Path(path).open("x", encoding="utf-8")
        self.lock = threading.Lock()
        self.events = []

    def __call__(self, event):
        value = dict(event)
        value.setdefault("monotonic_s", time.monotonic())
        value.setdefault("utc_s", time.time())
        with self.lock:
            self.events.append(value)
            self.handle.write(json.dumps(value, separators=(",", ":")) + "\n")
            self.handle.flush()

    def close(self):
        self.handle.close()


class RecordedFeedback(ReplicaCompletionFeedback):
    """Record the exact existing accumulator clock edges for all experimental arms."""
    def __init__(self, endpoint_ids, config, observer, *, observations=None):
        self.observer, self.edge = observer, None
        super().__init__(endpoint_ids, config, observations=observations, clock=self.record_clock)

    def record_clock(self):
        now = time.monotonic()
        self.observer(dict(self.edge, monotonic_s=now))
        return now

    def started(self, endpoint_id):
        self.edge = dict(event="feedback_started", endpoint_id=endpoint_id)
        super().started(endpoint_id)

    def finished(self, endpoint_id, work, *, successful):
        self.edge = dict(event="feedback_finished", endpoint_id=endpoint_id,
                         work=work, successful=successful)
        super().finished(endpoint_id, work, successful=successful)


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
        self.observer(dict(event="route_decision", request_id=args[0].request_id, decision=asdict(result)))
        return result


def endpoint_rates(events, endpoint_ids):
    """Same accumulator edges and work as online feedback; freeze the batch rate."""
    markers = {k: [] for k in endpoint_ids}
    completed = Counter()
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
    result = {}
    for endpoint, edges in markers.items():
        if not edges:
            raise ValueError("calibration did not reach every selected replica")
        active, busy, previous = 0, 0.0, edges[0][0]
        for stamp, delta in sorted(edges):
            if active:
                busy += stamp - previous
            active += delta
            previous = stamp
        if active or busy <= 0 or completed[endpoint] <= 0:
            raise ValueError("calibration completion edges did not settle")
        result[endpoint] = completed[endpoint] / busy
    return result


def replay_batch(config, directory, calls, *, arm, requests, unit, output,
                 stop_new_post_utc, providers=None, fixed_rates=None):
    """One sealed batch, one original POST per occurrence; every response stays private."""
    output = Path(output)
    output.mkdir(exist_ok=False)
    observer = JsonEvents(output / "events.jsonl")
    remaining = Counter(hashlib.sha256(c["body"]).hexdigest() for c in calls)
    attempts, outcomes = [], []
    started = time.monotonic()
    cpu_start = time.process_time()
    adaptation = None if arm == "round_robin" else ReplicaAdaptationConfig(target_drain_s=None)
    selected = replace(directory, max_active_requests=requests, max_active_work=requests,
        routing="round_robin" if arm == "round_robin" else "adaptive", adaptation=adaptation)
    execution = build_model_replica_execution(config, replicas=selected, response_mode="full",
        max_tasks=len(calls), observer=observer,
        replica_observations=providers if arm == "adaptive_supply" else None)
    router = execution.engine.policies.router
    feedback = RecordedFeedback(tuple(r.endpoint_id for r in directory.replicas),
        ReplicaAdaptationConfig(), observer, observations=providers if arm == "adaptive_supply" else None)
    # Experimental observation only: the production factory/transport are unchanged.
    execution.engine.backend._execute.__self__.feedback = feedback
    if arm != "round_robin":
        router.feedback = feedback.latest
    if arm == "fixed_rate":
        router.feedback = lambda endpoint_id: ReplicaFeedback(time.monotonic(), fixed_rates[endpoint_id])
    timed = TimedPolicy(execution.engine.policies.admission, router, observer)
    execution.engine.policies = replace(execution.engine.policies, admission=timed, router=timed)
    flow = NativeTaskSession(execution, output.name, "preserved-q2-replay")
    tasks = tuple(prepare_native_task(c["body"], i, row_sequence=c["source_sequence"],
        call_id=c["source_call_id"], max_result_bytes=1048576) for i, c in enumerate(calls))
    primary = None
    clean = False

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

    query_start = time.monotonic()
    try:
        with observe_async_http_posts(unit, before_send):
            offer = flow.offer(tasks)
            if offer.accepted_prefix_count != len(tasks):
                raise ValueError("finite replay input was not fully accepted")
            flow.end_input()
            supply_snapshot()
            received = set()
            while len(received) < len(calls):
                if time.monotonic() - query_start > 60:
                    raise TimeoutError("replay batch deadline reached")
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
                    flow.wait(progress)
        consumed = time.monotonic()
        if any(remaining.values()) or unit.attempts != len(calls):
            raise ValueError("replay request budget and occurrences differ")
        flow.close(clean=True)
        clean = execution.close()
        if not clean or execution.engine.capacity.usage().held_tasks:
            raise RuntimeError("replay cleanup could not be confirmed")
        return dict(arm=arm, requests=requests, count=len(calls), jct_s=consumed - query_start,
            assembly_s=query_start - started, process_cpu_s=time.process_time() - cpu_start,
            policy_s=timed.ns / 1e9, policy_calls=timed.calls, attempts=attempts,
            outcomes=outcomes, events=observer.events, directory_sha256=selected.sha256)
    except BaseException as error:
        primary = error
        raise
    finally:
        if not clean:
            try:
                flow.close()
                end = time.monotonic() + 32
                while time.monotonic() < end and any(not p.future.done() for p in execution.engine.backend._slots.values()):
                    execution.engine.advance()
                    execution.engine.wake.wait(execution.engine.wake.generation, 0.02)
                execution.engine.advance()
                execution.engine.backend.close()
            except BaseException as cleanup:
                if primary is not None:
                    primary.add_note("replay cleanup also failed: " + type(cleanup).__name__)
                else:
                    raise
        (output / "cleanup.json").write_text(json.dumps(dict(clean=clean,
            usage=asdict(execution.engine.capacity.usage()), error=type(primary).__name__ if primary else None,
            outcomes=outcomes, reserved_attempts=attempts), separators=(",", ":")))
        observer.close()
