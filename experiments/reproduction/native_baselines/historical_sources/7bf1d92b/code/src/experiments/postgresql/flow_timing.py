"""Correlate test-build PG node stages, actual Core submissions and client rows.

This diagnostic is restricted to one completed, non-NULL Map. Resource and
semantic audits remain in the existing query evaluator. No policy is selected here.
"""
from collections import Counter
import hashlib
import json
from pathlib import Path
import re

from .map_bindings import parse_pg_bindings


_TRACE = re.compile(r'LOG:\s+SEMLOOM_FLOW_TRACE (\{.*\})\s*$')
_STAGES = ('child_ready', 'task_prepared', 'offer_accepted', 'result_ready', 'node_return', 'row_release')


def analyze_flow(directory, *, expected_rows, window):
    root = Path(directory)
    settings = json.loads((root/'flow-settings.json').read_text())
    if settings.get('clock_source') != 'clock_gettime(CLOCK_MONOTONIC)':
        raise ValueError('PG and Python diagnostic timestamps need the same monotonic clock')
    execution = json.loads((root/'q0/execution.json').read_text())
    content = (root/'q0/results.jsonl').read_bytes()
    if (execution['status'] != 'completed' or execution['query_status'] != 'completed'
            or execution['recorded_rows'] != expected_rows or len(content) != execution['recorded_bytes']
            or hashlib.sha256(content).hexdigest() != execution['results_sha256']):
        raise ValueError('flow diagnostic requires an intact completed recording')
    results = [json.loads(line) for line in content.splitlines()]
    client = {r['row'][0]: r['received_ns'] for r in results}
    if len(client) != expected_rows or len(results) != expected_rows:
        raise ValueError('duplicate or missing diagnostic client rows')
    pid = json.loads((root/'pg-backend.json').read_text())['backend_pid']
    lines = (root/'q0-producer.log').read_text().splitlines()
    bindings = [b for b in parse_pg_bindings(lines) if b.backend_pid == pid]
    by_sequence = {b.sequence: b.row_id for b in bindings}
    if (len(bindings) != expected_rows or len(by_sequence) != expected_rows
            or len({b.stream for b in bindings}) != 1 or set(by_sequence.values()) != set(client)):
        raise ValueError('client timing has no complete independent producer mapping')
    events = [json.loads(m.group(1)) for line in lines if (m := _TRACE.search(line))]
    events = [e for e in events if e.get('backend_pid') == pid]
    if not events or len({e.get('pump_id') for e in events}) != 1:
        raise ValueError('flow diagnostic requires one observed PG pump')
    previous = -1
    for event in events:
        if (event.get('version') != 1 or type(event.get('monotonic_ns')) is not int
                or event['monotonic_ns'] < previous or type(event.get('input_index')) is not int
                or not 0 <= event['input_index'] <= expected_rows
                or type(event.get('retained_rows')) is not int or not 0 <= event['retained_rows'] <= window
                or any(type(event.get(k)) is not bool for k in ('head_ready', 'head_present', 'has_sequence'))):
            raise ValueError('invalid or unordered PG flow observation')
        previous = event['monotonic_ns']
    stage = {}
    for name in _STAGES:
        observed = [e for e in events if e['event'] == name]
        if Counter(e['input_index'] for e in observed) != Counter(range(expected_rows)):
            raise ValueError('incomplete or duplicate PG stage: '+name)
        stage[name] = {e['input_index']: e for e in observed}
    if [e['input_index'] for e in events if e['event'] == 'node_return'] != list(range(expected_rows)):
        raise ValueError('node delivery changed the input order')
    core = [json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()]
    submitted = [e for e in core if e['event'] == 'core_submitted']
    if (len(submitted) != expected_rows or len({e['key']['session_id'] for e in submitted}) != 1
            or Counter(e['key']['sequence'] for e in submitted) != Counter(by_sequence.keys())):
        raise ValueError('actual Core submissions differ from producer identities')
    submission_ns = {e['key']['sequence']: e['monotonic_ns'] for e in submitted}
    rows = []
    for index in range(expected_rows):
        chain = [stage[name][index] for name in _STAGES]
        sequence = stage['offer_accepted'][index]['sequence']
        if (sequence not in by_sequence or any(not e['has_sequence'] or e['sequence'] != sequence for e in chain[2:])
                or any(a['monotonic_ns'] > b['monotonic_ns'] for a, b in zip(chain, chain[1:]))):
            raise ValueError('inconsistent producer sequence or PG stage order')
        times = {name: stage[name][index]['monotonic_ns'] for name in _STAGES}
        times.update(core_submitted=submission_ns[sequence], client_received=client[by_sequence[sequence]])
        if times['client_received'] < times['node_return']:
            raise ValueError('client timestamp precedes node return')
        rows.append(dict(input_index=index, sequence=sequence, row_id=by_sequence[sequence], times_ns=times,
            ready_to_node_s=(times['node_return']-times['result_ready'])/1e9,
            node_to_client_s=(times['client_received']-times['node_return'])/1e9))
    if len({r['sequence'] for r in rows}) != expected_rows:
        raise ValueError('input ordinals reused a producer sequence')
    receives = [e['monotonic_ns'] for e in events if e['event'] == 'receive_begin']
    if not receives:
        raise ValueError('missing initial PG receive/poll')
    first_submit = min(submission_ns.values())
    if first_submit < receives[0]:
        raise ValueError('current first-poll dispatch assumption does not hold')
    pauses = []
    for event in events:
        if event['event'] != 'child_pause': continue
        index = event['input_index']
        if index >= expected_rows:
            raise ValueError('diagnostic pause was placed at EOF')
        pauses.append(dict(input_index=index, head_ready=event['head_ready'],
            head_input_index=event['head_input_index'] if event['head_present'] else None,
            begin_ns=event['monotonic_ns'], end_ns=stage['child_ready'][index]['monotonic_ns']))
    expected_pauses = [] if settings['pause_input'] < 0 else [settings['pause_input']]
    if [p['input_index'] for p in pauses] != expected_pauses:
        raise ValueError('observed pause differs from declared diagnostic')
    first_input = min(e['monotonic_ns'] for e in stage['child_ready'].values())
    return dict(schema='semloom.pg.flow_diagnostic.v1', settings=settings, rows=rows, pauses=pauses,
        first_input_to_submit_s=(first_submit-first_input)/1e9,
        first_poll_to_submit_s=(first_submit-receives[0])/1e9,
        first_node_return_s=(stage['node_return'][0]['monotonic_ns']-execution['t_release_ns'])/1e9,
        client_first_row_s=(execution['t_first_row_ns']-execution['t_release_ns'])/1e9,
        query_jct_s=execution['query_jct_seconds'],
        startup_candidate_rows=[len(e['candidates']) for e in core if e['event'] == 'core_map_organized'][:1],
        scope='instrumented test build with injected source pauses; no model or throughput qualification')
