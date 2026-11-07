"""Account for waiting around submission without moving logical input eligibility.

The caller declares a sealed, immutable full scan available at run_query invocation.
This model is unsuitable for dependencies, LIMIT, mutable inputs or arbitrary SQL.
The persistent driver and shared model service are outside the per-query boundary;
query-specific work is inside it. New-per-query gateway/tokenizer preparation is
included; a persistent group records its shared service startup separately.
"""
import json
from pathlib import Path

from .flow_timing import analyze_flow


def _percentile(values, fraction):
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    low = int(index)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)


def analyze_waiting_positions(directory, *, expected_rows, window):
    root = Path(directory)
    contract = json.loads((root / 'waiting-contract.json').read_text())
    summary = json.loads((root / 'summary.json').read_text())
    if (contract.get('eligibility') != 'sealed-immutable-full-scan-at-invocation'
            or contract.get('manifest_sha256') != summary.get('manifest_sha256')
            or summary.get('status') != 'passed'
            or summary['config']['arm'] != 'pg' or summary['config']['task'] != 'map'
            or summary['config'].get('movie_id') is not None):
        raise ValueError('waiting analysis requires a matched immutable full-scan contract')
    flow = analyze_flow(root, expected_rows=expected_rows, window=window)
    execution = json.loads((root / 'q0/execution.json').read_text())
    start = summary.get('query_preparation_started_ns')
    release, eof = execution['t_release_ns'], execution['t_query_terminal_ns']
    if (type(start) is not int or start < 0 or type(release) is not int
            or type(eof) is not int or not start <= release <= eof):
        raise ValueError('missing or invalid query preparation boundary')
    events = [json.loads(line) for line in (root / 'events.jsonl').read_text().splitlines()]
    kinds = ('core_submitted', 'core_terminal', 'core_http_started', 'core_http_finished')
    indexed = {}
    for kind in kinds:
        selected = [event for event in events if event['event'] == kind]
        keys = [(event['key']['session_id'], event['key']['sequence']) for event in selected]
        if len(keys) != expected_rows or len(set(keys)) != expected_rows:
            raise ValueError('incomplete or duplicate task timing: ' + kind)
        indexed[kind] = {key: event['monotonic_ns'] for key, event in zip(keys, selected)}
    keys = set(indexed[kinds[0]])
    if any(set(indexed[kind]) != keys for kind in kinds[1:]):
        raise ValueError('Core and HTTP timing identities differ')
    if len({key[0] for key in keys}) != 1:
        raise ValueError('waiting analysis requires one Core flow')
    session = next(iter(keys))[0]
    rows, http_durations = [], []
    for row in flow['rows']:
        times = row['times_ns']
        key = (session, row['sequence'])
        submitted = indexed['core_submitted'][key]
        terminal = indexed['core_terminal'][key]
        http_start, http_end = indexed['core_http_started'][key], indexed['core_http_finished'][key]
        points = [start, submitted, terminal, times['result_ready'], times['node_return'], times['client_received']]
        # HTTP may begin just before the owner emits submitted; it must finish before terminal.
        if (any(type(t) is not int for t in points + [http_start, http_end])
                or any(a > b for a, b in zip(points, points[1:]))
                or not release <= submitted or not http_start <= http_end <= terminal
                or times['client_received'] > eof):
            raise ValueError('inconsistent cross-layer timing order')
        names = ('eligible_to_submit', 'submitted_to_terminal', 'terminal_to_pg_ready',
                 'pg_ready_to_node', 'node_to_client')
        durations = {name: b - a for name, a, b in zip(names, points, points[1:])}
        total = times['client_received'] - start
        if sum(durations.values()) != total:
            raise ValueError('waiting decomposition does not conserve elapsed time')
        http_durations.append(http_end - http_start)
        rows.append(dict(row_id=row['row_id'], sequence=row['sequence'], eligibility_ns=start,
                         durations_ns=durations, total_to_client_ns=total))
    count = len(rows)
    return dict(schema='semloom.waiting_positions.v1', contract=contract, rows=rows,
        preparation_seconds=(release-start)/1e9, sql_to_eof_seconds=(eof-release)/1e9,
        preparation_to_eof_seconds=(eof-start)/1e9,
        mean_seconds={name: sum(r['durations_ns'][name] for r in rows)/count/1e9 for name in names},
        occupancy_area_task_seconds={name: sum(r['durations_ns'][name] for r in rows)/1e9 for name in names},
        http_p99_seconds=_percentile(http_durations, .99)/1e9,
        http_scope='client transport including connection/queue/response read; not GPU kernel time',
        interpretation='per-query preparation included; eligibility waiting includes not-yet-materialized rows; areas are not retained-memory bytes')
