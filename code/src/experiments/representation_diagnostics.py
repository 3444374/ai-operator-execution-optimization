"""Reconstruct declared representation ownership, without claiming physical RSS."""
from collections import Counter, defaultdict

from .observer_events import read_observer_events


def _count(value):
    return type(value) is int and value >= 0


def _owner(scope, events):
    issues, retained, borrowed, preparing = [], {}, {}, {}
    preparations, lifetimes, retention_intervals = [], [], []
    finished, abandoned = set(), set()
    bypasses = Counter()
    opened = closed = False
    peak = hits = errors = evictions = 0
    capacity = declared = None
    last_sequence = 0
    previous_ns = None
    for event in events:
        sequence, now = event.get('owner_sequence'), event.get('observation_ns')
        if sequence != last_sequence + 1:
            issues.append('owner_event_gap_or_duplicate')
        if type(sequence) is int:
            last_sequence = sequence
        if not _count(now) or previous_ns is not None and now < previous_ns:
            issues.append('owner_clock_unknown_or_reversed')
            now = None
        else:
            previous_ns = now
        action = event.get('action')
        key, consumer = event.get('representation_key'), event.get('consumer_ref')
        edge = key, consumer
        evidence = event.get('_evidence')
        if closed:
            issues.append('event_after_owner_close')
        if action == 'owner_open':
            if opened:
                issues.append('duplicate_owner_open')
            opened = True
            capacity, declared = event.get('capacity_bytes'), event.get('declared_consumers')
            if not _count(capacity) or capacity == 0 or not _count(declared):
                issues.append('owner_capacity_or_plan_missing')
        elif not opened:
            issues.append('owner_open_missing')
        if action in ('prepare_start', 'prepare_end', 'prepare_error', 'borrow',
                      'consumer_terminal', 'consumer_abandoned') and not all(type(v) is str and v for v in edge):
            issues.append('consumer_identity_missing')
            continue
        if action == 'prepare_start':
            if edge in preparing:
                issues.append('duplicate_prepare_start')
            preparing[edge] = dict(start_ns=now, evidence=evidence)
        elif action in ('prepare_end', 'prepare_error'):
            start = preparing.pop(edge, None)
            if start is None:
                issues.append('prepare_start_missing')
            if action == 'prepare_error':
                errors += 1
            else:
                measured = event.get('rebuild_ns')
                preparations.append(dict(representation_key=key, consumer_ref=consumer,
                    payload_bytes=event.get('payload_bytes'), rebuild_ns=measured if _count(measured) else None,
                    start_ns=None if start is None else start['start_ns'], end_ns=now,
                    evidence=[None if start is None else start['evidence'], evidence]))
        elif action == 'retained_acquire':
            if key in retained or not _count(event.get('payload_bytes')):
                issues.append('retained_acquire_inconsistent')
            retained[key] = dict(storage_id=event.get('storage_id'), payload_bytes=event.get('payload_bytes'),
                                 start_ns=now, evidence=evidence)
        elif action == 'borrow':
            if edge in borrowed or edge in finished or edge in abandoned:
                issues.append('duplicate_or_terminal_borrow')
            if event.get('cache_hit') is True:
                hits += 1
                if key not in retained or retained[key]['storage_id'] != event.get('storage_id'):
                    issues.append('hit_without_matching_retained_representation')
            elif event.get('cache_hit') is not False:
                issues.append('borrow_source_unknown')
            borrowed[edge] = dict(start_ns=now, storage_id=event.get('storage_id'), evidence=evidence)
        elif action == 'consumer_terminal':
            value = borrowed.pop(edge, None)
            if value is None or edge in finished:
                issues.append('terminal_without_unique_borrow')
            else:
                start = value['start_ns']
                lifetimes.append(dict(representation_key=key, consumer_ref=consumer, start_ns=start, end_ns=now,
                    duration_ns=now-start if now is not None and start is not None and now >= start else None,
                    evidence=[value['evidence'], evidence]))
            finished.add(edge)
        elif action == 'consumer_abandoned':
            if edge in borrowed or edge in finished or edge in abandoned:
                issues.append('abandonment_of_active_or_terminal_consumer')
            abandoned.add(edge)
        elif action == 'retention_bypass':
            bypasses[event.get('reason', 'unknown')] += 1
        elif action == 'retained_release':
            value = retained.pop(key, None)
            if value is None or any(k == key for k, _ in borrowed):
                issues.append('retained_release_missing_or_while_borrowed')
            if value is not None:
                start = value['start_ns']
                retention_intervals.append(dict(representation_key=key, start_ns=start, end_ns=now,
                    payload_bytes=value['payload_bytes'], reason=event.get('reason'),
                    duration_ns=now-start if now is not None and start is not None and now >= start else None,
                    evidence=[value['evidence'], evidence]))
            evictions += event.get('reason') in ('consumer_rebuild_value_choice', 'lru_choice')
        elif action == 'owner_close':
            closed = True
            if borrowed or retained or preparing or event.get('declared_pending') != 0:
                issues.append('owner_closed_with_unresolved_responsibility')
            if event.get('observation_errors') != 0:
                issues.append('owner_observer_reported_errors')
            if declared != len(finished) + len(abandoned):
                issues.append('declared_consumers_not_fully_accounted')
        values = [v['payload_bytes'] for v in retained.values()]
        if all(_count(v) for v in values):
            used = sum(values)
            peak = max(peak, used)
            if (event.get('retained_bytes') != used or event.get('retained_entries') != len(retained)
                    or _count(capacity) and used > capacity):
                issues.append('retained_payload_accounting_inconsistent')
        if event.get('active_borrows') != len(borrowed):
            issues.append('borrow_accounting_inconsistent')
    if not opened or not closed:
        issues.append('owner_lifecycle_incomplete')
    per_key = Counter(v['representation_key'] for v in preparations)
    return dict(run_id=scope[0], observer_session_id=scope[1], pid=scope[2], owner_uid=scope[3],
        status='incomplete' if issues else 'observed', issues=sorted(set(issues)),
        successful_preparations=len(preparations), failed_preparations=errors, observed_hits=hits,
        repeated_preparations={key: count for key, count in per_key.items() if count > 1},
        retained_payload_peak_bytes=peak, capacity_bytes=capacity, retention_evictions=evictions,
        retention_bypasses=dict(bypasses),
        unresolved_borrows=len(borrowed), unresolved_retained=len(retained),
        preparations=preparations, consumer_lifetimes=lifetimes, retention_intervals=retention_intervals)


def diagnose_representations(events, *, capture_issues=None):
    groups, issues = defaultdict(list), list(capture_issues or ())
    if capture_issues is None:
        issues.append('sink_receipts_not_checked')
    for event in events:
        if event.get('event') != 'representation_lifecycle':
            continue
        scope = tuple(event.get(k) for k in ('run_id', 'observer_session_id', 'pid', 'owner_uid'))
        if (not all(type(scope[i]) is str and scope[i] for i in (0, 1, 3))
                or type(scope[2]) is not int or scope[2] <= 0):
            issues.append('owner_identity_missing')
            continue
        groups[scope].append(event)
    owners = [_owner(scope, sorted(values, key=lambda e: e.get('owner_sequence', -1)
                                  if type(e.get('owner_sequence')) is int else -1))
              for scope, values in sorted(groups.items())]
    if not owners:
        issues.append('no_representation_events')
    findings = []
    for owner in owners:
        if owner['status'] != 'observed':
            continue
        if owner['repeated_preparations']:
            findings.append(dict(owner_uid=owner['owner_uid'], phenomenon='repeated_compatible_preparation',
                representation_count=len(owner['repeated_preparations']),
                next_intervention='Keep representation, consumer sequence and capacity fixed; compare retention disabled, keep-until-last, LRU and consumer-value selection. Include preparation, control and complete-query costs.'))
    return dict(schema='semloom.representation_diagnostics.v1', source_type='observer_event_analysis',
        capture_status='incomplete' if issues or any(v['status'] != 'observed' for v in owners)
                       else 'closed_without_recorded_loss', capture_issues=sorted(set(issues)), owners=owners,
        findings=findings, scope='Owner-retained ASCII URI payload accounting and declared consumer responsibility; not process RSS, encoder, KV, or aliases held outside this owner.',
        unresolved=['processor_and_device_representation_lifetimes', 'physical_memory_savings', 'complete_query_benefit'])


def analyze_directory(directory, *, max_events=1_000_000):
    events, issues, sources = read_observer_events(directory, max_events=max_events,
                                                  event_names={'representation_lifecycle'})
    report = diagnose_representations(events, capture_issues=issues)
    report['sources'] = sources
    return report
