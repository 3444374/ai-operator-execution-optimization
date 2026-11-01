"""Offline joins of existing record/Q7 evidence; no execution or policy changes."""
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _opaque(value):
    return hashlib.sha256(str(value).encode()).hexdigest()


def _key(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def _file(root, relative):
    path = Path(relative)
    if path.is_absolute() or '..' in path.parts or path.as_posix() != relative:
        raise ValueError('capture path must be relative and contained')
    target = root / path
    if any(p.is_symlink() for p in [target, *target.parents] if p != root.parent):
        raise ValueError('capture symlinks are not supported')
    if not target.resolve().is_relative_to(root):
        raise ValueError('capture path escapes root')
    return target


def _read(root, reference):
    value = json.loads(_file(root, reference['path']).read_text())
    for part in reference.get('pointer', []):
        value = value[part]
    return value


def _union(intervals):
    result = []
    for a, b in sorted(intervals):
        if result and a <= result[-1][1]:
            result[-1][1] = max(b, result[-1][1])
        else:
            result.append([a, b])
    return result


def _duration(intervals):
    return sum(b - a for a, b in _union(intervals))


def _overlap(left, right):
    return _duration([(max(a, c), min(b, d)) for a, b in _union(left)
                      for c, d in _union(right) if max(a, c) < min(b, d)])


def _peak(intervals):
    points = sorted([(a, 1) for a, b in intervals if a < b] +
                    [(b, -1) for a, b in intervals if a < b])
    active = peak = 0
    for _, delta in points:
        active += delta
        peak = max(peak, active)
    return peak


def summarize_capture(root, description, *, cpu_spans=()):
    """Read selected finished artifacts, retaining unknown identity/clock coverage.

    Explicit clock bindings attest one common monotonic clock, not an offset fit.
    Without one, only the same source file is comparable. Structure is a declared
    plan, separate from observed call lineage and from actual execution order.
    """
    root = Path(root).resolve(strict=True)
    if description.get('schema') != 'semloom.record_dag_observation.input.v1':
        raise ValueError('explicit record DAG observation input schema required')
    issues, provenance, events, samples, calls = [], {}, [], [], []
    bindings = description.get('clock_bindings', {})

    def domain(path):
        binding = bindings.get(path)
        return ('bound:' + binding['domain'] if isinstance(binding, dict)
                and binding.get('domain') and binding.get('basis') else 'file:' + path)

    def remember(path):
        data = _file(root, path).read_bytes()
        provenance[path] = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        return data

    for ref in description.get('calls', []):
        remember(ref['path'])
        value = _read(root, ref)
        if not isinstance(value, list):
            raise ValueError('calls reference must select a list')
        calls.extend(value)
    for field, target in [('events', events), ('samples', samples)]:
        for path in description.get(field, []):
            for line in remember(path).decode().splitlines():
                value = json.loads(line)
                target.append(dict(value, _source=path, _domain=domain(path)))

    nodes, lineage, intervals = {}, [], defaultdict(lambda: defaultdict(list))
    identities, task_calls = defaultdict(list), defaultdict(list)
    query_counts = Counter()

    def node(stage):
        return nodes.setdefault(stage, dict(stage_id=stage, observed_calls=0, statuses={},
            input_occurrences=None, output_occurrences=None, input_rows=None, output_rows=None,
            prompt_tokens=None, output_tokens=None, waits=[], cpu=[], windows=[], resources=[]))

    def identity(call):
        return (call.get('engine_uid'), _key(call.get('core_task_key')),
                call.get('query_id'), call.get('call_id'))

    unique, conflicting = {}, set()
    for call in calls:
        if not isinstance(call, dict) or not all(call.get(k) for k in ('query_id', 'algorithm_stage', 'call_id')):
            issues.append('call_identity_incomplete'); continue
        if description.get('query_id') is not None and call['query_id'] != description['query_id']:
            continue
        scope = identity(call)
        if scope in unique:
            if unique[scope] != call:
                issues.append('conflicting_call_receipts')
                conflicting.add(scope)
            continue
        unique[scope] = call
        identities[(call['query_id'], call['call_id'])].append(call)
        if call.get('core_task_key') is not None:
            task_calls[_key(call['core_task_key'])].append(call)
        query_counts[call['query_id']] += 1
    if len(query_counts) > 1:
        raise ValueError('select one query_id; do not mix first/reuse or different queries')
    sessions = {(c.get('core_task_key') or {}).get('session_id') for c in unique.values()}
    stage_calls = defaultdict(list)
    for call in unique.values():
        stage_calls[call['algorithm_stage']].append(call)
        for parent in call.get('parent_call_ids', []):
            candidates = [p for p in identities[(call['query_id'], parent)]
                          if p.get('engine_uid') == call.get('engine_uid')
                          and (p.get('core_task_key') or {}).get('session_id') ==
                              (call.get('core_task_key') or {}).get('session_id')]
            if len(candidates) != 1:
                issues.append('parent_lineage_unresolved'); continue
            p = candidates[0]
            lineage.append(dict(source=p['algorithm_stage'], target=call['algorithm_stage'],
                parent_ref=_opaque(_key(identity(p))), child_ref=_opaque(_key(identity(call))),
                evidence='observed_call_lineage'))
    for stage, rows in stage_calls.items():
        n = node(stage)
        n['observed_calls'] = len(rows)
        n['statuses'] = dict(Counter('conflicting_receipt' if identity(r) in conflicting
                                    else r.get('status', 'unknown') for r in rows))
        occurrences = {(r.get('row_id'), r.get('source_position')) for r in rows}
        n['input_occurrences'] = len(occurrences) if all(None not in p for p in occurrences) else None
        # A successful Filter API return is not evidence that the row was kept.
        for target, field in [('prompt_tokens', 'prompt_tokens'), ('output_tokens', 'completion_tokens')]:
            values = [r.get(field) for r in rows]
            n[target] = sum(values) if not any(identity(r) in conflicting for r in rows) and all(
                type(v) is int and v >= 0 for v in values) else None

    if description.get('animals_summary'):
        ref = description['animals_summary']; remember(ref['path']); summary = _read(root, ref)
        if summary.get('query_id') != 7 or [s.get('stage_id') for s in summary.get('stages', [])] != ['filter-0', 'filter-1']:
            raise ValueError('Animals adapter requires explicit original Q7 stages')
        for stage in summary['stages']:
            n = node(stage['stage_id']); count = stage.get('prompt_rows')
            n['input_rows'] = count if type(count) is int and count >= 0 else None
            decisions = stage.get('decisions')
            n['output_rows'] = (sum(v[1] for v in decisions) if isinstance(decisions, list)
                and len(decisions) == count and all(isinstance(v, (list, tuple)) and len(v) == 2
                    and type(v[1]) is bool for v in decisions) else None)
            a, b = stage.get('started_monotonic_ns'), stage.get('terminal_monotonic_ns')
            if _number(a) and _number(b) and a <= b:
                intervals[stage['stage_id']][domain(ref['path'])].append((a, b))
        structure = dict(stages=[dict(stage_id=s) for s in ('source', 'filter-0', 'filter-1', 'intersection')],
            edges=[dict(source=a, target=b, kind='data') for a, b in
                [('source', 'filter-0'), ('source', 'filter-1'), ('filter-0', 'intersection'), ('filter-1', 'intersection')]])
    else:
        structure = description.get('structure', dict(stages=[], edges=[]))
    declared = [dict(e, evidence='declared_structure_not_measured_readiness') for e in structure.get('edges', [])]
    for item in structure.get('stages', []):
        node(item['stage_id'])

    aliases = {}

    def event_calls(e):
        found = []
        for i in e.get('identities') or e.get('members') or []:
            if not isinstance(i.get('call_id'), str) or not i['call_id']:
                issues.append('event_member_unresolved'); continue
            if description.get('query_id') is not None and i.get('query_id') != description['query_id']:
                continue
            candidates = identities[(i.get('query_id'), i.get('call_id'))]
            if e.get('engine_uid'):
                candidates = [c for c in candidates if c.get('engine_uid') == e['engine_uid']]
            if len(candidates) == 1:
                found.append(candidates[0])
            else:
                # Old Movie receipts and model rows use distinct call-ID schemes.
                # Native request identity is an explicit bridge, never a row-only join.
                native = [c for c in unique.values() if e.get('engine_uid') and e.get('request_id')
                    and c.get('engine_uid') == e['engine_uid'] and c.get('engine_request_id') == e['request_id']
                    and all(c.get(f) == i.get(f) and i.get(f) is not None for f in
                        ('query_id', 'algorithm_stage', 'model_role', 'row_id', 'source_position', 'call_index'))]
                if len(native) == 1:
                    c = native[0]; found.append(c)
                    a, b = _opaque(i.get('call_id')), _opaque(_key(identity(c)))
                    aliases[(a, b)] = dict(event_call_ref=a, receipt_call_ref=b,
                        evidence='engine_UID_native_request_ID_and_full_occurrence_identity')
                else: issues.append('event_member_unresolved')
        return found

    opens, ready, blocked = {}, {}, defaultdict(list)
    footer_sources, usage_peaks = set(), {}
    for path in description.get('events', []):
        footer = str(Path(path).with_suffix('.status.json'))
        if _file(root, footer).is_file():
            f = json.loads(remember(footer))
            buffer = f.get('feedback_buffer')
            if (f.get('closed') is True and f.get('flush_confirmed') is True
                    and f.get('dropped') == 0 and f.get('errors') == 0
                    and (buffer is None or buffer.get('drain_complete') is True
                         and buffer.get('remaining') == 0)):
                footer_sources.add(path)
            else: issues.append('sink_incomplete')
    for e in sorted(events, key=lambda e: (e['_domain'], e.get('monotonic_ns')
                    if _number(e.get('monotonic_ns')) else math.inf)):
        t, d = e.get('monotonic_ns'), e['_domain']
        if not _number(t) or t < 0:
            issues.append('event_clock_unknown'); continue
        name = e.get('event')
        if name and name.startswith('Core_'):
            for metric, value in (e.get('usage') or {}).items():
                if type(value) is int and value >= 0:
                    usage_peaks[metric] = max(usage_peaks.get(metric, 0), value)
        if name in ('native_open', 'native_close'):
            for call in event_calls(e):
                k = (d, identity(call))
                if name == 'native_open':
                    if k in opens: issues.append('duplicate_native_open')
                    else: opens[k] = t
                elif k in opens and opens[k] <= t:
                    intervals[call['algorithm_stage']][d].append((opens.pop(k), t))
                else: issues.append('native_boundary_unpaired')
        if name == 'method_ready_boundary':
            if sessions and e.get('session_id') not in sessions:
                continue
            k = (d, e.get('session_id'), e.get('method_row_ref'), e.get('stage_index'))
            action = e.get('action')
            if action == 'method_ready':
                if k in ready: issues.append('duplicate_method_ready')
                else: ready[k] = t
            elif action == 'Core_backpressure':
                blocked[k].append(t)
            elif action == 'Core_offer_accepted':
                candidates = task_calls[_key(e.get('key'))]
                if len(candidates) == 1 and k in ready and ready[k] <= t:
                    stage = candidates[0]['algorithm_stage']
                    node(stage)['waits'].append(dict(start_ns=ready.pop(k), end_ns=t, clock_domain=d,
                        evidence='observed_method_ready_to_Core_offer',
                        reason='Core_backpressure_observed_constraint_unknown' if blocked[k] else 'unclassified_ready_wait',
                        backpressure_events=len(blocked.pop(k, []))))
                else: issues.append('ready_offer_join_unresolved')
    if opens: issues.append('native_interval_unfinished')
    if ready: issues.append('ready_work_unfinished')
    missing_sinks = sorted(set(description.get('events', [])) - footer_sources)
    if missing_sinks: issues.append('sink_footer_missing_or_unqualified')

    physical_cpu, conflicts = {}, set()
    for span in cpu_spans:
        ref = span.get('span_id')
        if not ref:
            issues.append('CPU_span_identity_unknown'); continue
        if ref in physical_cpu and physical_cpu[ref] != span:
            conflicts.add(ref)
        physical_cpu[ref] = span
    cpu_rows = []
    for ref, span in physical_cpu.items():
        if ref in conflicts:
            issues.append('CPU_span_conflicting'); continue
        if span.get('status') != 'observed':
            issues.append('CPU_span_unknown'); continue
        members = span.get('members') or []
        if description.get('query_id') is not None:
            members = [m for m in members if m.get('query_id') == description['query_id']]
        stages = sorted({m['algorithm_stage'] for m in members if m.get('algorithm_stage')})
        if not stages:
            issues.append('CPU_span_members_unknown'); continue
        row = dict(span_ref=_opaque(ref), operation=span['operation'], stages=stages,
            wall_ns=span.get('wall_ns'), exclusive_thread_CPU_ns=span.get('exclusive_thread_CPU_ns'))
        cpu_rows.append(row)
        for stage in stages:
            node(stage)['cpu'].append(dict(span_ref=_opaque(ref), operation=span['operation'],
                wall_ns=span.get('wall_ns'), exclusive_thread_CPU_ns=span.get('exclusive_thread_CPU_ns'),
                shared_stage_members=stages, attribution='shared_span_not_per_member_cost'))

    for stage, domains in intervals.items():
        n = node(stage)
        for d, windows in domains.items():
            merged = _union(windows)
            n['windows'].append(dict(clock_domain=d, union_ns=_duration(merged),
                envelope_ns=max(b for a, b in merged) - min(a for a, b in merged),
                peak_observed_API_intervals=_peak(windows), kind='API_or_operator_window_not_GPU_time'))
            for path in description.get('samples', []):
                selected = [v for v in samples if v['_source'] == path and v['_domain'] == d
                    and _number(v.get('sample_start_monotonic_ns')) and _number(v.get('sample_end_monotonic_ns'))
                    and any(a <= v['sample_start_monotonic_ns'] <= v['sample_end_monotonic_ns'] <= b for a, b in merged)]
                values = {}
                for metric in ('gpu_utilization_pct', 'gpu_memory_used_mib', 'host_cpu_busy_cores',
                               'host_memory_available_mib', 'vllm_num_requests_running', 'vllm_num_requests_waiting',
                               'vllm_kv_cache_usage_perc'):
                    measured = []
                    for v in selected:
                        valid = metric not in v.get('missing', {})
                        if metric.startswith('gpu_'):
                            expected = description.get('gpu_uuids')
                            source = v.get('sources', {}).get('gpu', {})
                            actual = source.get('physical_gpu_uuids')
                            status = source.get('status')
                            valid = valid and bool(expected) and actual == expected and bool(status) and not status.startswith('unavailable')
                        measured.append(v.get('values', {}).get(metric) if valid else None)
                    known = [v for v in measured if _number(v)]
                    values[metric] = dict(observed=len(known), missing=len(measured)-len(known),
                        minimum=min(known) if known else None, maximum=max(known) if known else None,
                        unavailable_reason=None if known else 'metric_missing_or_unbound_device' if selected
                            else 'no_same_domain_contained_sample')
                n['resources'].append(dict(source=path, sample_count=len(selected), values=values,
                    attribution='shared_source_samples_inside_window_not_stage_exclusive',
                    status='observed_sample_overlap' if selected else 'unknown:no_qualified_samples'))
    overlaps = []
    for a in sorted(intervals):
        for b in sorted(intervals):
            if a >= b: continue
            common = set(intervals[a]) & set(intervals[b])
            overlaps.append(dict(stages=[a, b], status='observed' if common else 'unknown:clock_domains_unjoined',
                by_domain=[dict(clock_domain=d, overlap_ns=_overlap(intervals[a][d], intervals[b][d])) for d in sorted(common)],
                meaning='API_or_operator_overlap_not_proof_of_concurrent_GPU_kernels'))
    for n in nodes.values():
        n['unavailable'] = {}
        for field, reason in [('input_rows', 'source_input_progress_not_recorded'),
                              ('output_rows', 'row_selection_or_relation_output_not_recorded'),
                              ('prompt_tokens', 'complete_prompt_usage_not_recorded'),
                              ('output_tokens', 'complete_output_usage_not_recorded')]:
            if n[field] is None:
                n['unavailable'][field] = reason
        if not n['waits']: n['unavailable']['waits'] = 'no_matched_ready_offer_boundaries'
        if not n['windows']: n['unavailable']['windows'] = 'no_matched_API_or_operator_boundaries'
        if not n['resources']: n['unavailable']['resources'] = 'no_same_domain_sample_windows'
        wait_domains = defaultdict(list)
        for w in n['waits']:
            wait_domains[w['clock_domain']].append((w['start_ns'], w['end_ns']))
        n['ready_wait_summary'] = [dict(clock_domain=d, closed_intervals=len(w),
            request_wait_sum_ns=sum(b-a for a,b in w), wall_union_ns=_duration(w),
            observed_closed_ready_peak=_peak(w)) for d,w in sorted(wait_domains.items())]
    return dict(schema='semloom.record_dag_observation.v1', status='observations_with_explicit_limits',
        capture_kind=description.get('kind', 'unspecified'), source_commit=description.get('source_commit'),
        comparability='common observation schema only; model/generation/workload/cache/lifecycle equivalence not established here',
        file_provenance=provenance, observed_call_count=len(unique), query_count=len(query_counts),
        stages=[nodes[k] for k in sorted(nodes)], data_lineage=lineage, declared_edges=declared,
        native_call_aliases=list(aliases.values()),
        overlaps=overlaps, issues=dict(Counter(issues)),
        physical_CPU_spans=cpu_rows,
        clock_binding_scope='same-file clocks or explicitly supplied monotonic binding; no offset estimated or host assertion certified',
        capture_complete=None, sink_footer_missing_or_unqualified=missing_sinks,
        unresolved_ready_count=len(ready), unresolved_native_count=len(opens),
        Core_usage_peaks=dict(values=usage_peaks or None, scope='all_recorded_Core_events_not_exclusive_physical_memory'),
        limits=['Missing timing or reason is unknown, never zero/idle.',
                'A successful model Filter call is not a kept-row decision.',
                'Resource samples and native API windows are shared observations, not exclusive demand or GPU kernel time.',
                'No MFU/HBM, physical last-use, causal speedup or cross-SemBench general rule is inferred.'])
