"""Real Core/HTTP software fixture; artificial service times are not model evidence."""

from collections import Counter
from contextlib import ExitStack
from dataclasses import asdict, replace
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import statistics
import threading
import time

from ..execution_provider.adapters.full_response import decode_full_response
from ..execution_provider.adapters.model_config import FixedModelConfig
from ..execution_provider.adapters.model_replicas import ModelReplica, ModelReplicaSet, build_model_replica_execution
from ..execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task
from ..planning.work import StageWork, WorkDescriptor
from ..scheduling.core.session_contract import State
from ..scheduling.endpoint_routing.replica_adaptation import ReplicaAdaptationConfig, ReplicaFeedback
from ..scheduling.runtime.observations import CachedMetricsObservationProvider, ServiceMetricsSnapshot


class _Endpoint:
    def __init__(self, endpoint_id, seconds_per_work, actual_work):
        self.endpoint_id, self.seconds_per_work, self.actual_work = endpoint_id, seconds_per_work, actual_work
        self.lock = threading.Lock()
        self.slots = threading.Semaphore(2)
        self.running = self.waiting = self.peak_running = self.peak_http = 0
        self.records = []
        site = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                payload = self.rfile.read(int(self.headers["Content-Length"]))
                value = json.loads(payload)
                row = int(value["messages"][0]["content"].split("-")[-1])
                started = time.monotonic()
                with site.lock:
                    site.waiting += 1
                    site.peak_http = max(site.peak_http, site.waiting + site.running)
                with site.slots:
                    service_at = time.monotonic()
                    with site.lock:
                        site.waiting -= 1
                        site.running += 1
                        site.peak_running = max(site.peak_running, site.running)
                    duration = (site.seconds_per_work(service_at) if callable(site.seconds_per_work)
                                else site.seconds_per_work)
                    time.sleep(duration * site.actual_work[row])
                    done = time.monotonic()
                    with site.lock:
                        site.running -= 1
                        site.records.append(dict(row=row, payload_sha256=hashlib.sha256(payload).hexdigest(),
                            arrived_at_s=started, service_at_s=service_at, done_at_s=done,
                            actual_work=site.actual_work[row], seconds_per_work=duration))
                body = json.dumps(dict(model="fixture", choices=[dict(message=dict(content=f"row-{row}"),
                    finish_reason="stop")], usage=dict(prompt_tokens=1, completion_tokens=1, total_tokens=2)),
                    separators=(",", ":")).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(target=partial(self.httpd.serve_forever, poll_interval=0.01), daemon=True)
        self.url = f"http://127.0.0.1:{self.httpd.server_port}/v1/chat/completions"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.httpd.shutdown()
        self.thread.join(2)
        self.httpd.server_close()

    def metrics(self):
        with self.lock:
            return ServiceMetricsSnapshot(self.running, self.waiting, 0.2)


class _TimedPolicy:
    def __init__(self, admission, router):
        self.admission, self.router = admission, router
        self.ns = self.calls = 0

    @property
    def limit(self):
        return self.admission.limit

    def _call(self, method, *args, **kwargs):
        started = time.perf_counter_ns()
        try:
            return method(*args, **kwargs)
        finally:
            self.ns += time.perf_counter_ns() - started
            self.calls += 1

    def decide(self, *args, **kwargs):
        return self._call(self.admission.decide, *args, **kwargs)

    def route(self, *args, **kwargs):
        return self._call(self.router.route, *args, **kwargs)


def _busy_union(records):
    events = sorted([(r["service_at_s"], 1) for r in records] + [(r["done_at_s"], -1) for r in records])
    busy = 0.0
    active = 0
    previous = events[0][0] if events else 0
    for stamp, delta in events:
        if active:
            busy += stamp - previous
        active += delta
        previous = stamp
    return busy


def run_cell(speeds, actual_work, estimates, *, arm, requests, arrival_s=None, fixed_rates=None):
    """One finite query; all HTTP payloads and full responses are checked by row."""
    count = len(actual_work)
    arrival_s = [0.0] * count if arrival_s is None else arrival_s
    trace, received = [], {}
    started = time.monotonic()
    with ExitStack() as stack:
        sites = [stack.enter_context(_Endpoint(str(i), speed, actual_work)) for i, speed in enumerate(speeds)]
        replicas = tuple(ModelReplica(s.endpoint_id, s.url, "localhost-fixture", (), "a" * 64, 4, 64) for s in sites)
        adaptation = None if arm == "round_robin" else ReplicaAdaptationConfig(
            target_drain_s=0.15 if arm == "adaptive_supply" else None, sample_interval_s=0.03,
            stale_after_s=2, rate_alpha=0.3)
        directory = ModelReplicaSet("fixture", replicas, requests, requests * 16,
            routing="round_robin" if arm == "round_robin" else "adaptive", adaptation=adaptation)
        providers = ({s.endpoint_id: CachedMetricsObservationProvider(s.metrics, min_sample_interval_s=0.03)
                      for s in sites} if arm == "adaptive_supply" else None)
        config = FixedModelConfig(sites[0].url, "fixture", 15000)
        execution = build_model_replica_execution(config, replicas=directory, response_mode="full", max_tasks=count,
            replica_observations=providers, observer=lambda event: trace.append(dict(event, at_s=time.monotonic())))
        policy = execution.engine.policies.router
        if arm == "fixed_rate":
            policy.feedback = lambda endpoint_id: ReplicaFeedback(time.monotonic(), fixed_rates[endpoint_id])
        timed = _TimedPolicy(execution.engine.policies.admission, policy)
        execution.engine.policies = replace(execution.engine.policies, admission=timed, router=timed)
        flow = NativeTaskSession(execution, "fixture-query", "fixture-map")
        payloads = [json.dumps(dict(model="fixture", messages=[dict(role="user", content=f"row-{i}")],
            temperature=0, max_tokens=16), separators=(",", ":")).encode() for i in range(count)]
        tasks = tuple(prepare_native_task(payload, i, row_sequence=i, call_id=f"call-{i}", max_result_bytes=4096,
            work=WorkDescriptor((StageWork("model", estimates[i], "work_units"),), "model", "fixture-estimate"))
            for i, payload in enumerate(payloads))
        offered = 0
        ready = time.monotonic()
        query_start = ready
        try:
            while len(received) < count:
                if time.monotonic() - query_start > 20:
                    raise TimeoutError("software fixture query deadline")
                end = offered
                while end < count and arrival_s[end] <= time.monotonic() - query_start:
                    end += 1
                if end > offered:
                    offer = flow.offer(tasks[offered:end])
                    offered += offer.accepted_prefix_count
                    if offered == count:
                        flow.end_input()
                progress = flow.advance(count)
                if progress.state in (State.FAILED, State.CANCELLED):
                    raise RuntimeError(f"software fixture flow {progress.state}: {progress.error}")
                for delivery in progress.deliveries:
                    row = delivery.key.sequence
                    if row in received:
                        raise AssertionError("duplicate software fixture result")
                    response = decode_full_response(delivery.result)
                    assert response.status_code == 200
                    assert json.loads(response.body)["choices"][0]["message"]["content"] == f"row-{row}"
                    assert delivery.info.row_sequence == row and delivery.info.call_id == f"call-{row}"
                    received[row] = time.monotonic()
                flow.release(tuple(d.lease_id for d in progress.deliveries))
                if len(received) < count:
                    execution.engine.wake.wait(execution.engine.wake.generation, 0.003)
            consumed_at = time.monotonic()
            flow.close(clean=True)
            assert execution.engine.capacity.usage().held_tasks == 0
            assert execution.engine.capacity.usage().active_requests == 0
            assert execution.close()
        except BaseException:
            flow.close()
            execution.engine.backend.close()
            raise
        endpoints = []
        all_requests = []
        for site in sites:
            records = [dict(r, estimated_work=estimates[r["row"]],
                result_at_s=received[r["row"]], offered_at_s=query_start + arrival_s[r["row"]]) for r in site.records]
            all_requests.extend(records)
            assert site.peak_http <= 4 and site.peak_running <= 2
            busy = _busy_union(records)
            endpoints.append(dict(endpoint_id=site.endpoint_id, peak_http=site.peak_http, peak_service=site.peak_running,
                busy_s=busy, completed_work=sum(r["actual_work"] for r in records), records=records))
        assert Counter(r["row"] for r in all_requests) == Counter(range(count))
        assert all(r["payload_sha256"] == hashlib.sha256(payloads[r["row"]]).hexdigest() for r in all_requests)
        for event in trace:
            usage = event.get("usage", {})
            assert usage.get("active_requests", 0) <= requests
            assert usage.get("active_work", 0) <= requests * 16
        return dict(arm=arm, requests=requests, count=count, jct_s=consumed_at - query_start,
            startup_s=ready - started, policy_s=timed.ns / 1e9, policy_calls=timed.calls,
            policy_us_per_call=timed.ns / max(1, timed.calls) / 1000,
            sum_service_wait_s=sum(r["service_at_s"] - r["arrived_at_s"] for r in all_requests),
            observed_max_request_s=max(received[i] - query_start - arrival_s[i] for i in received),
            max_core_requests=max((e.get("usage", {}).get("active_requests", 0) for e in trace), default=0),
            endpoints=endpoints, core=trace, directory_sha256=directory.sha256)


def run_probe(repeats=3):
    """Five predefined scenarios; independent static K calibration, no post-hoc tuning."""
    cases = dict(heterogeneous=(0.008, 0.032), burst=(0.008, 0.032),
                 estimate_error=(0.008, 0.032), homogeneous=(0.012, 0.012), single=(0.012,))
    result = dict(schema=1, evidence_type="software_fixture", model_requests=0, gpu_count=0,
        hard_per_endpoint_requests=4, simulated_service_slots=2, target_drain_s=0.15, repeats=repeats,
        calibration=[], cells=[], summary=[])
    start = time.monotonic()
    calibrations = {}
    for speeds in set(cases.values()):
        candidates = (1, 2, 4) if len(speeds) == 1 else (2, 4, 8)
        runs = [run_cell(speeds, [1] * 24, [1] * 24, arm="round_robin", requests=k) for k in candidates]
        for run in runs:
            result["calibration"].append(dict(speeds=speeds, **run))
        selected = min(runs, key=lambda r: r["jct_s"])
        rates = {e["endpoint_id"]: e["completed_work"] / e["busy_s"] for e in selected["endpoints"]}
        calibrations[speeds] = selected["requests"], rates
    for name, speeds in cases.items():
        work = [1, 1, 3, 1, 2, 1] * 8
        estimates = ([4 if i % 2 == 0 else 1 for i in range(48)] if name == "estimate_error" else work)
        arrivals = [0.15 * (i // 16) for i in range(48)] if name == "burst" else None
        capacity, rates = calibrations[speeds]
        arms = ["round_robin", "fixed_rate", "adaptive_route", "adaptive_supply"]
        for repeat in range(repeats):
            for arm in arms if repeat % 2 == 0 else arms[::-1]:
                if time.monotonic() - start > 300:
                    raise TimeoutError("software fixture probe deadline")
                cell = run_cell(speeds, work, estimates, arm=arm, requests=capacity, arrival_s=arrivals, fixed_rates=rates)
                result["cells"].append(dict(case=name, repeat=repeat, speeds=speeds, **cell))
        for arm in arms:
            cells = [c for c in result["cells"] if c["case"] == name and c["arm"] == arm]
            result["summary"].append(dict(case=name, arm=arm, requests=capacity,
                all_jct_s=[c["jct_s"] for c in cells], median_jct_s=statistics.median(c["jct_s"] for c in cells),
                median_policy_s=statistics.median(c["policy_s"] for c in cells),
                median_policy_us_per_call=statistics.median(c["policy_us_per_call"] for c in cells),
                median_wait_s=statistics.median(c["sum_service_wait_s"] for c in cells),
                endpoint_counts=[[len(e["records"]) for e in c["endpoints"]] for c in cells]))
    result["elapsed_s"] = time.monotonic() - start
    result["http_requests"] = sum(c["count"] for c in result["cells"] + result["calibration"])
    return result
