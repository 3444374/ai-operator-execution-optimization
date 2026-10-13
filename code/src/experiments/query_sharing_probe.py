"""Deterministic two-service diagnostic using the real SessionEngine, no models.

The offline fixture defines service times. Online selectors see only accepted
estimates and sealed input. FCFS here is a diagnostic, never a native baseline.
"""

from dataclasses import asdict, dataclass, replace
import hashlib
import gzip
import json
from pathlib import Path
import time

from src.scheduling.core.models import EndpointSnapshot, TopologySnapshot
from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import (
    Acceptance, OfferedTask, SessionLimits, SessionSpec, SessionTimeouts, State, Submission, Terminal,
)
from src.scheduling.core.session_jobs import JobBudget, round_robin_flow
from src.scheduling.core.session_policy import SessionPolicies
from src.scheduling.endpoint_routing.policies import LeastQueuedEndpointRouter
from src.scheduling.submission_control.admission import StaticAdmissionController
from src.scheduling.submission_control.query_sharing import BoundedShortJobBurst


@dataclass(frozen=True)
class Query:
    label: str
    rows: int
    release_s: float
    service_s: float = 1.0
    estimated_work: int = 1


class SyntheticServices:
    """One serial, nonpreemptive worker per endpoint; accepted queues share K."""

    def __init__(self, endpoints):
        self.now = 0.0
        self.available = {e: 0.0 for e in endpoints}
        self.pending = {}
        self.events = []
        self.cancelled = set()
        self.trace = []

    def try_submit(self, task, endpoint):
        handle = f'{task.key.session_id}:{task.key.sequence}'
        start = max(self.now, self.available[endpoint])
        finish = start + float(task.task.payload)
        self.available[endpoint] = finish
        self.pending[task.key] = (handle, task, finish)
        for event, when in (('submitted', self.now), ('service_start', start), ('service_finish', finish)):
            self.trace.append(dict(event=event, t=when, key=asdict(task.key),
                                   job_id=task.spec.job_id, endpoint=endpoint,
                                   estimated_work=task.task.estimated_work))
        return Submission(Acceptance.ACCEPTED, handle)

    def poll(self, handles, max_events):
        due = sorted(((value[2], key) for key, value in self.pending.items() if value[2] <= self.now),
                     key=lambda item: (item[0], item[1].session_id, item[1].sequence))
        result = []
        for _, key in due[:max_events]:
            handle, _, _ = self.pending.pop(key)
            status = 'cancelled' if key in self.cancelled else 'completed'
            result.append(Terminal(key, handle, status, str(key.sequence).encode()))
        return tuple(result)

    def request_cancel(self, key, handle):
        # Cancellation notification is not stop proof. The original terminal
        # still ends the accepted reservation, including queued remote work.
        self.cancelled.add(key)


def least_active_work(ready, history):
    least = min(j.work.active_work for j in ready)
    return round_robin_flow(tuple(j for j in ready if j.work.active_work == least), history)


def run(queries, arm, *, admission=4, endpoints=2):
    """All arms use the same source, storage, router, physical workers and work."""
    started = time.perf_counter()
    names = tuple(f'e{i}' for i in range(endpoints))
    service = SyntheticServices(names)
    max_work = max(q.estimated_work for q in queries)
    # Strong static uses less committed work under the SAME global ceiling.
    limits = SessionLimits(16 * len(queries), 1024 * len(queries), 128 * len(queries),
                           4, 4 * max_work, 16, 64, 8, 256, 64, 120.0, .001, SessionTimeouts())
    policies = SessionPolicies(StaticAdmissionController(admission), LeastQueuedEndpointRouter(),
        TopologySnapshot(tuple(EndpointSnapshot(e, 'http://localhost/fixture', 'default', str(i),
                               True, 0, 0, None, 0.0) for i, e in enumerate(names)), 0.0), 'default')
    selector = {
        'diagnostic_fcfs': lambda ready, history: round_robin_flow(ready[:1], history),
        'work_conserving': least_active_work,
        'shared_rr': round_robin_flow,
        'strong_static': round_robin_flow,
        'bounded_short': BoundedShortJobBurst(work_unit='synthetic_work', short_work_limit=4,
                                               bypass_work_limit=4),
    }['bounded_short' if arm == 'bounded_short_k4' else arm]
    resources = []
    decisions = []
    def resource_sample():
        return dict(t=service.now, **asdict(core.capacity.usage()),
                    jobs={j.handle.job_id: asdict(core.capacity.usage(job_id=j.handle.job_id))
                          for j in core.jobs.jobs.values()})
    def observe(event, key):
        resources.append(resource_sample())
    def choose(ready, history):
        selected = selector(ready, history)
        decisions.append(dict(t=service.now, ready=[asdict(job) for job in ready],
                              history=asdict(history), selected=asdict(selected)))
        return selected
    core = SessionEngine(limits, service, policies, clock=lambda: service.now, sink=observe,
                         max_jobs=len(queries), observe_job_work=True, choose_flow=choose)
    waiting = list(sorted(queries, key=lambda q: q.release_s))
    live, done, peaks = {}, {}, dict(held_tasks=0, input_bytes=0, result_bytes=0,
                                   active_requests=0, active_work=0)
    steps = 0
    while waiting or live:
        steps += 1
        if steps > 10000:
            raise RuntimeError('finite probe exceeded its owner-tick limit')
        while waiting and waiting[0].release_s <= service.now:
            query = waiting.pop(0)
            job = core.register_job(query.label, JobBudget(16, 1024, 128, 4, limits.active_work))
            session = core.open(SessionSpec(job.job_id, 'map', 'fixture', work_unit='synthetic_work'),
                                replace(limits, held_tasks=16, input_bytes=1024, result_bytes=128), job=job)
            live[job.job_id] = dict(query=query, job=job, session=session, next_input=0,
                                   delivered=set(), sealed=False)
        changed = False
        for entry in tuple(live.values()):
            session, query = entry['session'], entry['query']
            if entry['next_input'] < query.rows:
                first = entry['next_input']
                tasks = tuple(OfferedTask(i, str(query.service_s).encode(), query.estimated_work, 8)
                              for i in range(first, min(query.rows, first + 16)))
                offered = session.offer(tasks)
                if offered.status == 'REJECTED':
                    raise RuntimeError('probe producer sent invalid work')
                entry['next_input'] += offered.accepted_prefix_count
                changed |= bool(offered.accepted_prefix_count)
            if entry['next_input'] == query.rows and not entry['sealed']:
                session.seal()
                core.seal_job(entry['job'])
                entry['sealed'] = True
        resources.append(resource_sample())
        progress = core.advance()
        changed |= bool(progress.events)
        for job_id, entry in tuple(live.items()):
            session, query = entry['session'], entry['query']
            delivery = session.advance(16)
            for row in delivery.deliveries:
                if (row.key.sequence in entry['delivered'] or row.result != str(row.key.sequence).encode()):
                    raise RuntimeError('probe result identity or exactly-once check failed')
                entry['delivered'].add(row.key.sequence)
            session.release(tuple(row.lease_id for row in delivery.deliveries))
            changed |= bool(delivery.deliveries)
            if session.state == State.FINISHED:
                if len(entry['delivered']) != query.rows:
                    raise RuntimeError('probe query ended without every required result')
                session.close_consumer(clean=True)
                core.close_job(entry['job'])
                done[query.label] = dict(job_id=job_id, jct_s=service.now - query.release_s,
                                        finish_s=service.now, rows=query.rows)
                del live[job_id]
        usage = asdict(core.capacity.usage())
        resources.append(resource_sample())
        if core.error:
            raise RuntimeError(core.error)
        if changed:
            continue
        future = [v[2] for v in service.pending.values() if v[2] > service.now]
        future += [waiting[0].release_s] if waiting else []
        if not future and live:
            raise RuntimeError('probe made no progress')
        if future:
            service.now = min(future)
    if core.capacity.records or core.jobs.jobs or service.pending:
        raise RuntimeError('probe did not release all Job resources')
    for sample in resources:
        for name in peaks:
            peaks[name] = max(peaks[name], sample[name])
    trace = sorted(service.trace, key=lambda e: e['t'])
    for query in queries:
        row = done[query.label]
        submits = [e['t'] for e in trace if e['job_id'] == row['job_id'] and e['event'] == 'submitted']
        finishes = [e['t'] for e in trace if e['job_id'] == row['job_id'] and e['event'] == 'service_finish']
        row['first_dispatch_s'] = min(submits) - query.release_s
        row['max_no_dispatch_while_unsubmitted_s'] = max(b - a for a, b in
            zip([query.release_s] + submits, submits))
        row['max_no_completed_service_s'] = max(b - a for a, b in
            zip([query.release_s] + finishes, finishes))
        row['actual_service_s'] = query.rows * query.service_s
    group_s = max(row['finish_s'] for row in done.values()) - min(q.release_s for q in queries)
    return dict(arm=arm, admission=admission, endpoints=endpoints, queries=[asdict(q) for q in queries],
                jobs=done, group_jct_s=group_s, rows_per_simulated_s=sum(q.rows for q in queries) / group_s,
                peaks=peaks, final_usage=asdict(core.capacity.usage()), trace=trace,
                resources=resources, decisions=decisions, owner_ticks=steps,
                host_elapsed_s=time.perf_counter() - started)


def experiment():
    scenarios = {
        'delayed_short': (Query('long', 64, 0), Query('short', 4, .5)),
        'no_contention': (Query('long', 64, 0), Query('short', 4, 40)),
        'equal_queries': (Query('long', 16, 0), Query('short', 16, .5)),
        'nonpreemptive_block': (Query('long', 2, 0, 20, 20), Query('short', 1, .5)),
        'underestimated_short': (Query('long', 7, 0), Query('short', 3, .5, 5, 1)),
        'overestimated_short': (Query('long', 64, 0), Query('short', 4, .5, 1, 20)),
        'short_arrival_train': (Query('long', 128, 0),) +
            tuple(Query(f'short{i}', 4, .5 + i) for i in range(8)),
    }
    # Independent long-only calibration: choose smallest saturating admission.
    calibration = [run((Query('long', 64, 0),), 'shared_rr', admission=k) for k in (2, 4)]
    best = min(r['group_jct_s'] for r in calibration)
    static_k = min(r['admission'] for r in calibration if r['group_jct_s'] <= best * 1.01)
    trials = []
    solo = []
    for name, queries in scenarios.items():
        for repetition in range(3):
            for arm in ('diagnostic_fcfs', 'work_conserving', 'shared_rr', 'strong_static',
                        'bounded_short', 'bounded_short_k4'):
                trials.append(dict(scenario=name, repetition=repetition, **run(queries, arm,
                    admission=static_k if arm in ('strong_static', 'bounded_short') else 4)))
        for query in queries:
            for count in (1, 2):
                solo.append(dict(scenario=name, resource_role='reserve_solo' if count == 1 else 'full_solo',
                                 **run((replace(query, release_s=0),), 'shared_rr', endpoints=count)))
    signature = hashlib.sha256(json.dumps({k: [asdict(q) for q in v] for k, v in scenarios.items()},
                                         sort_keys=True).encode()).hexdigest()
    return dict(schema_version=1, evidence='synthetic mechanism diagnostic', model_calls=0,
                gpu_calls=0, http_calls=0, postgres_queries=0, fixture_sha256=signature,
                global_active_requests=4, storage_per_job=dict(held_tasks=16, input_bytes=1024, result_bytes=128),
                physical_service='two nonpreemptive serial workers; not vLLM continuous batching',
                candidate=dict(short_work_limit=4, bypass_work_limit=4, work_unit='synthetic_work'),
                calibration=calibration, static_admission=static_k, trials=trials, solo=solo,
                native_lotus='pending; diagnostic_fcfs is not native LOTUS')


def enumerate_counterexamples():
    """Exploratory finite grid; retain every cell, not only the counterexamples."""
    rows = []
    for n in (5, 7, 9, 11, 16, 32):
        for m in (1, 2, 3, 4):
            for delay in (.5, 1.5, 2.5, 4.5):
                for duration in (1, 5, 20):
                    queries = (Query('long', n, 0), Query('short', m, delay, duration, 1))
                    a = run(queries, 'bounded_short', admission=2)
                    b = run(queries, 'strong_static', admission=2)
                    rows.append(dict(long_rows=n, short_rows=m, delay_s=delay, short_service_s=duration,
                        candidate_long=a['jobs']['long']['jct_s'], static_long=b['jobs']['long']['jct_s'],
                        candidate_short=a['jobs']['short']['jct_s'], static_short=b['jobs']['short']['jct_s'],
                        candidate_group=a['group_jct_s'], static_group=b['group_jct_s']))
    return rows


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    output = experiment()
    output['exploratory_grid'] = enumerate_counterexamples()
    root = Path(__file__).resolve().parents[3]
    files = ('code/src/scheduling/core/session.py', 'code/src/scheduling/core/session_jobs.py',
             'code/src/scheduling/submission_control/query_sharing.py',
             'code/src/experiments/query_sharing_probe.py')
    output['source_base'] = '9bb6c4154a3fa321c49a84f45eaeb6c7e12b129c'
    output['source_sha256'] = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in files}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    opener = gzip.open if args.output.suffix == '.gz' else open
    with opener(args.output, 'xt', encoding='utf-8') as stream:
        json.dump(output, stream, ensure_ascii=False, separators=(',', ':'))
    print(json.dumps(dict(output=str(args.output), trials=len(output['trials']),
                          static_admission=output['static_admission'], model_calls=0)))


if __name__ == '__main__':
    main()
