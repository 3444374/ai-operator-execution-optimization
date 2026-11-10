"""Join legal-ready waits, Core handoffs and reported native queue snapshots.

An empty reported queue is a research lead, not proof of GPU idleness or usable
capacity. No time interpolation, utilization threshold or row-only join is used.
"""
from collections import Counter, defaultdict

from .observer_events import read_observer_events


EVENTS = {'method_ready_boundary', 'Core_submitted', 'Core_dispatch_capacity_blocked',
          'Core_flow_capacity_blocked', 'native_queue_snapshot', 'native_queue_observation_unavailable'}


def _count(value):
    return type(value) is int and value >= 0


def _time(event):
    value = event.get('observation_ns', event.get('monotonic_ns'))
    written = event.get('monotonic_ns')
    return value if _count(value) and _count(written) and value <= written else None


def _domain(event):
    value = event.get('clock_domain')
    if type(value) is str and len(value) == 64 and all(c in '0123456789abcdef' for c in value):
        return 'shared:' + value
    return 'process:' + str(event['pid'])


def _task(event):
    key, core = event.get('key'), event.get('core_uid')
    if (type(core) is str and core and type(key) is dict
            and all(_count(key.get(k)) for k in ('session_id', 'sequence'))):
        return event['run_id'], event['observer_session_id'], core, key['session_id'], key['sequence']


def diagnose_supply(events, *, capture_issues=None):
    issues = ['sink_receipts_not_checked'] if capture_issues is None else list(capture_issues)
    selected = []
    for event in events:
        if event.get('event') not in EVENTS:
            continue
        if (not all(type(event.get(k)) is str and event[k] for k in ('run_id', 'observer_session_id'))
                or type(event.get('pid')) is not int or event['pid'] <= 0 or _time(event) is None):
            issues.append('event_identity_or_clock_missing')
            continue
        selected.append(event)
    selected.sort(key=lambda e: (e['run_id'], e['observer_session_id'], _domain(e), _time(e),
                                e.get('sequence', 0) if type(e.get('sequence', 0)) is int else -1))
    ready, accepted, submits, blocks = {}, {}, defaultdict(list), defaultdict(list)
    waits, points, unresolved = [], [], []

    def interval(start, end, phase, identity):
        if (_domain(start) != _domain(end) or _time(end) < _time(start)
                or start.get('engine_uid') != end.get('engine_uid')):
            issues.append('wait_clock_or_engine_binding_changed')
            return
        waits.append(dict(interval_id=len(waits), run_id=start['run_id'],
            observer_session_id=start['observer_session_id'], driver_uid=start.get('driver_uid'),
            core_uid=start.get('core_uid'), engine_uid=start.get('engine_uid'),
            method_row_ref=start.get('method_row_ref'), stage_index=start.get('stage_index'),
            phase=phase, start_ns=_time(start), end_ns=_time(end), duration_ns=_time(end)-_time(start),
            clock_domain=_domain(start), task=identity,
            dispatch_phase=end.get('record_phase') if phase == 'Core_to_transport_return' else None,
            evidence=[start.get('_evidence'), end.get('_evidence')]))

    for event in selected:
        name = event['event']
        if name == 'method_ready_boundary':
            identity = (event['run_id'], event['observer_session_id'], event.get('driver_uid'),
                        event.get('method_row_ref'), event.get('stage_index'))
            if (not all(type(v) is str and v for v in identity[2:4]) or not _count(identity[4])):
                issues.append('method_ready_identity_incomplete'); continue
            action = event.get('action')
            if action == 'method_ready':
                if identity in ready:
                    issues.append('duplicate_method_ready')
                else:
                    ready[identity] = event
            elif action == 'Core_offer_accepted':
                start = ready.pop(identity, None)
                if start is None:
                    issues.append('offer_without_observed_method_ready')
                else:
                    interval(start, event, 'method_to_Core_offer', _task(event))
                task = _task(event)
                if task is None:
                    issues.append('accepted_task_identity_missing')
                elif task in accepted:
                    issues.append('duplicate_accepted_task')
                else:
                    accepted[task] = event
        elif name == 'Core_submitted':
            task = _task(event)
            if task is None:
                issues.append('submitted_task_identity_missing')
            else:
                submits[task].append(event)
        elif name in ('Core_dispatch_capacity_blocked', 'Core_flow_capacity_blocked'):
            task = _task(event)
            if task is not None:
                blocks[task].append(event)
        elif name == 'native_queue_snapshot':
            points.append(event)
        else:
            issues.append('native_queue_observation_unavailable')
    for task, start in accepted.items():
        ends = submits.get(task, [])
        if len(ends) == 1:
            before = len(waits)
            interval(start, ends[0], 'Core_to_transport_return', task)
            if len(waits) > before:
                waits[-1]['observed_capacity_block_events'] = [e.get('_evidence') for e in blocks[task]
                    if _domain(e) == _domain(start) and _time(start) <= _time(e) <= _time(ends[0])]
        elif ends:
            issues.append('duplicate_transport_return')
        else:
            unresolved.append(dict(kind='accepted_without_transport_return', task=task, evidence=start.get('_evidence')))
    for identity, event in ready.items():
        unresolved.append(dict(kind='ready_without_offer', identity=identity, evidence=event.get('_evidence')))

    timelines = defaultdict(list)
    for wait in waits:
        engine = wait['engine_uid']
        if type(engine) is not str or not engine:
            continue
        scope = wait['run_id'], wait['observer_session_id'], engine, wait['clock_domain']
        if wait['start_ns'] < wait['end_ns']:
            timelines[scope].extend(((wait['start_ns'], 1, wait['interval_id']),
                                     (wait['end_ns'], 0, wait['interval_id'])))
    identities = Counter((p['run_id'], p['observer_session_id'], p.get('engine_uid'), p.get('source_sequence'))
                         for p in points if type(p.get('engine_uid')) is str and _count(p.get('source_sequence')))
    for point in points:
        engine, sequence = point.get('engine_uid'), point.get('source_sequence')
        if (type(engine) is not str or not engine or not _count(sequence)
                or not all(_count(point.get(k)) for k in ('running', 'waiting'))):
            issues.append('native_queue_snapshot_incomplete'); continue
        identity = point['run_id'], point['observer_session_id'], engine, sequence
        if identities[identity] != 1:
            issues.append('duplicate_native_queue_sequence'); continue
        scope = point['run_id'], point['observer_session_id'], engine, _domain(point)
        timelines[scope].append((_time(point), 2, point))
    findings, matched_points = [], 0
    for scope, timeline in sorted(timelines.items()):
        active = set()
        for at, kind, value in sorted(timeline, key=lambda v: (v[0], v[1])):
            if kind == 0:
                active.discard(value)
            elif kind == 1:
                active.add(value)
            elif active:
                matched_points += 1
                name = ('ready_with_reported_empty_engine_queue' if value['running'] == value['waiting'] == 0
                        else 'engine_reports_waiting_work' if value['waiting'] > 0
                        else 'engine_reports_running_work')
                members = sorted(active)
                findings.append(dict(phenomenon=name, run_id=scope[0], observer_session_id=scope[1],
                    engine_uid=scope[2], clock_domain=scope[3], observed_at_ns=at,
                    running=value['running'], waiting=value['waiting'], waiting_tasks=len(active),
                    interval_ids=members[:16], interval_ids_complete=len(members) <= 16,
                    source_sequence=value['source_sequence'], evidence=value.get('_evidence'),
                    physical_starvation='unknown'))
    if not points:
        issues.append('no_native_queue_snapshots')
    if not waits and not unresolved:
        issues.append('no_method_ready_waits')
    if points and waits and matched_points == 0:
        issues.append('no_matching_engine_clock_and_wait_interval')
    return dict(schema='semloom.supply_diagnostics.v1', source_type='observer_event_analysis',
        capture_status='incomplete' if issues else 'closed_without_recorded_loss',
        capture_issues=sorted(set(issues)), waits=waits, unresolved_waits=unresolved, findings=findings,
        summary=dict(closed_wait_intervals=len(waits), unresolved_wait_intervals=len(unresolved),
                     native_snapshot_points=len(points), matched_snapshot_points=matched_points,
                     phenomena=dict(Counter(row['phenomenon'] for row in findings))),
        interpretation='Closed waits and queue delivery points only; no interpolation between snapshots, no proof of GPU idleness, usable capacity, or causes of an API wait.',
        next_interventions=[
            dict(when='ready_with_reported_empty_engine_queue', action='Align a bounded GPU trace and native schedule with these events; vary only supply or preparation overlap while preserving capacity and complete requests.'),
            dict(when='engine_reports_waiting_work', action='Inspect actual model work, graph/kernel behavior and cache state before increasing upstream supply.'),
            dict(when='observed_capacity_block_events', action='Separate active requests, active work, storage and result consumption; vary one declared budget within the same physical allocation.'),
        ],
        unresolved=['physical_gpu_idle_intervals', 'actual_usable_service_capacity', 'compute_or_bandwidth_limit',
                    'causal_query_speedup'])


def analyze_directory(directory, *, max_events=1_000_000):
    events, issues, sources = read_observer_events(directory, max_events=max_events, event_names=EVENTS)
    report = diagnose_supply(events, capture_issues=issues)
    report['sources'] = sources
    return report
