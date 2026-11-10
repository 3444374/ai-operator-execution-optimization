"""Interpret observed token positions and encoder cache references by iteration."""
from collections import defaultdict

from .native_gpu_diagnostics import diagnose
from .observer_events import read_observer_events


def scheduled_position_split(iteration):
    members = iteration['actual_members']
    prompt, computed = iteration.get('prompt_token_counts'), iteration.get('computed_before')
    if (iteration['status'] != 'observed' or not isinstance(prompt, dict) or not isinstance(computed, dict)
            or prompt.keys() != members.keys() or computed.keys() != members.keys()
            or any(type(v) is not int or v < 0 for v in (*prompt.values(), *computed.values()))):
        return dict(status='unavailable', reason='matched_prompt_and_computed_positions_required')
    requests = []
    for key, count in members.items():
        prefill = min(count, max(0, prompt[key] - computed[key]))
        requests.append(dict(request_id=key, scheduled_prompt_positions=prefill,
                             scheduled_nonprompt_positions=count-prefill,
                             already_computed_prompt_positions=min(prompt[key], computed[key])))
    return dict(status='observed', requests=requests,
                scheduled_prompt_positions=sum(r['scheduled_prompt_positions'] for r in requests),
                scheduled_nonprompt_positions=sum(r['scheduled_nonprompt_positions'] for r in requests))


def analyze_directory(directory, *, max_events=1_000_000):
    events, issues, sources = read_observer_events(directory, max_events=max_events)
    report = diagnose(events, capture_issues=issues)
    for iteration in report['iterations']:
        iteration['stage_positions'] = scheduled_position_split(iteration)
    snapshots = defaultdict(list)
    for event in events:
        if event.get('event') == 'native_encoder_cache_snapshot':
            key = tuple(event.get(k) for k in ('run_id', 'observer_session_id', 'pid', 'observer_execute_id'))
            snapshots[key].append(event)
    cache_changes = []
    for key, values in snapshots.items():
        before = [e for e in values if e.get('action') == 'before_execute']
        after = [e for e in values if e.get('action') == 'after_execute']
        row = dict(run_id=key[0], observer_session_id=key[1], pid=key[2], observer_execute_id=key[3],
                   evidence=[e.get('_evidence') for e in values])
        complete = all(isinstance(e.get('cache_entries'), list)
                       and len(e['cache_entries']) == e.get('retained_entries')
                       and all(isinstance(v, dict) and isinstance(v.get('representation_key'), str)
                               for v in e['cache_entries']) for e in values)
        if len(before) == len(after) == 1 and complete:
            old = {e['representation_key'] for e in before[0]['cache_entries']}
            new = {e['representation_key'] for e in after[0]['cache_entries']}
            row.update(status='observed', added=sorted(new-old), removed=sorted(old-new),
                       before_logical_bytes=before[0]['logical_bytes'], after_logical_bytes=after[0]['logical_bytes'])
        else:
            row.update(status='unavailable', reason='complete_before_after_cache_snapshots_required')
        cache_changes.append(row)
    report.update(schema='semloom.native_stage_diagnostics.v1', encoder_cache_changes=cache_changes, sources=sources,
                  stage_scope='Scheduled prompt/nonprompt positions, not FLOPs or accepted output tokens. Already computed positions may come from prior chunks, recomputation history or cache/connector state; they are not independently proven cache hits.',
                  cache_scope='Runner tensor references and logical bytes, not physical HBM release, global memory use or consumer lifetime beyond the runner.')
    return report
