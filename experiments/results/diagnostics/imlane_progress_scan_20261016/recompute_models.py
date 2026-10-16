#!/usr/bin/env python3
"""Recompute published aggregates, optionally checking closed raw timing/ledgers.

Standard library only. Reads local files; never starts a model, service, or SDK.
The optional raw check is not a replacement for the recorded full SDK audit.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
import csv
import hashlib
import json
import math
from pathlib import Path
import sqlite3

HERE = Path(__file__).resolve().parent
COLUMNS = ('sequence','phase','slot','unit','kind','pid','url','at_epoch','at_ns',
           'body_sha256','values_sha256','returned_ns','status','error_type')
KINDS = ('gateway','router','model','fixture')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def member(root, relative):
    relative = Path(relative)
    require(not relative.is_absolute() and '..' not in relative.parts,
            'raw member escapes supplied root')
    path = root / relative
    require(not any(p.is_symlink() for p in (path, *path.parents)),
            'use canonical raw paths without symbolic links')
    require(path.is_file(), 'missing raw member: ' + str(relative))
    return path


def mean(rows, field):
    return sum(float(r[field]) for r in rows) / len(rows)


def aggregates(rows):
    result = {'original_long_run': [], 'matched_history': []}
    for mode in ('CPython', 'author_ray'):
        selected = [r for r in rows if r['run_group'] == 'long_run' and r['mode'] == mode]
        selected.sort(key=lambda r: int(r['round_one_based']))
        require([int(r['round_one_based']) for r in selected] == list(range(1, 20)),
                'incomplete original 19-query coverage')
        for label, group in [('first', selected[:1]), ('subsequent', selected[1:])]:
            result['original_long_run'].append(dict(mode=mode, phase=label, queries=len(group),
                mean_query_seconds=mean(group, 'release_query_seconds'),
                min_query_seconds=min(float(r['release_query_seconds']) for r in group),
                max_query_seconds=max(float(r['release_query_seconds']) for r in group),
                mean_per_query_p99_ms=mean(group, 'request_p99_ms'),
                mean_http_inflight=mean(group, 'http_mean_inflight')))
    for history, before_ids, after_ids in [
        (0, ['m0-low-before','m7-low-before'], ['m3-low-after','m4-low-after']),
        (114000, ['m1-high-before','m6-high-before'], ['m2-high-after','m5-high-after']),
    ]:
        by_id = {r['query_key']: r for r in rows if r['run_group'] == 'matched_history'}
        before, after = [by_id[k] for k in before_ids], [by_id[k] for k in after_ids]
        require(all(r['mode'] == 'CPython' and int(r['history_rows']) == history
                    for r in before + after), 'history/mode mismatch')
        b, a = mean(before, 'release_query_seconds'), mean(after, 'release_query_seconds')
        result['matched_history'].append(dict(history_rows=history, queries_per_variant=2,
            mean_before_seconds=b, mean_after_seconds=a,
            after_minus_before_seconds=a-b, relative_change_percent=100*(a/b-1),
            forward_pair_after_minus_before=float(after[0]['release_query_seconds'])-float(before[0]['release_query_seconds']),
            reverse_pair_after_minus_before=float(after[1]['release_query_seconds'])-float(before[1]['release_query_seconds']),
            mean_per_query_p99_ms=dict(before=mean(before,'request_p99_ms'),after=mean(after,'request_p99_ms')),
            mean_http_inflight=dict(before=mean(before,'http_mean_inflight'),after=mean(after,'http_mean_inflight'))))
    return result


def check_timing(root, row, response_ids, generation):
    obs = member(root, row['observations_member'])
    record_path = member(root, row['persistent_query_member'])
    require(digest(obs) == row['observations_sha256'] and digest(record_path) == row['persistent_query_sha256'],
            'raw source SHA differs from publication')
    record = json.loads(record_path.read_text())
    for csv_key, raw_key in [('release_ns','t_release_ns'),('submit_ns','t_submit_ns'),('eof_ns','t_eof_ns')]:
        require(int(row[csv_key]) == record[raw_key], 'query clock differs: ' + csv_key)
    data = [json.loads(line) for line in obs.read_text().splitlines() if line]
    require(len(data) == 2000, 'incomplete original Q1')
    events = defaultdict(int)
    durations, occurrences, query_response_ids = [], set(), set()
    tokens = Counter()
    for request in data:
        require(request['status'] == request['client_status'] == 200 and request['retry_count'] == 0,
                'failed/retried request retained: raw check cannot qualify this query')
        identity = tuple(request[k] for k in ('row_id','stage_id','call_id'))
        require(identity not in occurrences and request['response_id'] not in response_ids,
                'duplicate request/response identity')
        occurrences.add(identity); response_ids.add(request['response_id'])
        query_response_ids.add(request['response_id'])
        start, end = request['dispatch_monotonic_ns'], request['body_completed_monotonic_ns']
        require(int(row['submit_ns']) <= start < end <= int(row['eof_ns']), 'HTTP clock outside query')
        require(end-start == request['latency_ns'], 'request latency clock differs')
        events[start] += 1; events[end] -= 1; durations.append(end-start)
        p, o = request['prompt_tokens'], request['output_tokens']; t = p+o-1
        tokens.update(prompt_tokens=p, output_tokens=o, sum_effective_prefill_tokens=t,
                      sum_effective_prefill_times_next=t*(t+1))
    active = peak = 0
    for at in sorted(events):
        active += events[at]; peak = max(peak, active)
        require(active >= 0, 'negative interval concurrency')
    require(active == 0, 'unclosed interval')
    durations.sort(); span = max(events)-min(events)
    calculated = dict(request_p50_ms=durations[999]/1e6, request_p95_ms=durations[1899]/1e6,
        request_p99_ms=durations[1979]/1e6, request_max_ms=durations[-1]/1e6,
        http_peak=peak, http_mean_inflight=sum(durations)/span, http_span_seconds=span/1e9,
        pre_first_http_seconds=(min(events)-int(row['release_ns']))/1e9,
        post_last_body_seconds=(int(row['eof_ns'])-max(events))/1e9, **tokens)
    for key, actual in calculated.items():
        require(math.isclose(actual,float(row[key]),rel_tol=1e-12,abs_tol=1e-9),
                'raw recomputation differs: ' + key)
    protocols = member(root, row['protocols_member'])
    require(digest(protocols) == row['protocols_sha256'], 'protocol source SHA differs')
    protocol_ids = set()
    with protocols.open() as stream:
        for line in stream:
            protocol = json.loads(line)
            require(protocol['http_status'] == 200 and
                    {key:protocol['request'][key] for key in generation} == generation,
                    'effective generation settings differ')
            response_id = protocol['response']['id']
            require(response_id not in protocol_ids, 'duplicate protocol response')
            protocol_ids.add(response_id)
    require(protocol_ids == query_response_ids, 'protocol/observation responses differ')


def check_ledger(root, floor, expected_model, expected_sha=None, prefix_sha=None):
    path = member(root, 'sends.sqlite')
    require(not any(Path(str(path)+suffix).exists() for suffix in ('-wal','-shm','-journal')),
            'expected a closed standalone ledger')
    before = digest(path)
    if expected_sha:
        require(before == expected_sha, 'original ledger SHA differs')
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)) as database:
        require(tuple(r[1] for r in database.execute('PRAGMA table_info(sends)')) == COLUMNS,
                'ledger schema differs')
        counts = dict(database.execute('SELECT kind,count(*) FROM sends WHERE sequence>? GROUP BY kind',(floor,)))
        require({k:counts.get(k,0) for k in KINDS} == dict(gateway=expected_model,router=expected_model,model=expected_model,fixture=0),
                'new-only ledger counts differ')
        require(database.execute('SELECT id,closed FROM caller_gate').fetchall() == [(1,1)], 'caller gate not closed')
        if floor:
            h = hashlib.sha256(); n = 0
            for row in database.execute('SELECT * FROM sends WHERE sequence<=? ORDER BY sequence',(floor,)):
                h.update((json.dumps(row,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False)+'\n').encode()); n += 1
            require(n == floor and h.hexdigest() == prefix_sha, 'retained historical prefix differs')
    require(digest(path) == before, 'ledger changed during read-only check')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--original-scope',type=Path,help='canonical extracted original model scope')
    parser.add_argument('--paired-scopes',type=Path,help='canonical folder containing the eight extracted case scopes')
    args = parser.parse_args()
    metadata = json.loads((HERE/'model_runs.json').read_text()); csv_path = HERE/'model_queries.csv'
    require(digest(csv_path) == metadata['public_files']['model_queries_csv_sha256'],'published CSV changed')
    rows = list(csv.DictReader(csv_path.open()))
    require(len(rows) == 46 and sum(int(r['model_calls']) for r in rows) == 92000,
            'original and paired budgets differ')
    for row in rows:
        for result, end, start in [('release_query_seconds','eof_ns','release_ns'),('postprep_query_seconds','eof_ns','submit_ns')]:
            require(math.isclose(float(row[result]),(int(row[end])-int(row[start]))/1e9,abs_tol=1e-12), 'query duration differs')
        flops = sum(a*int(row[b]) for a,b in zip(metadata['metric_definitions']['mfu']['coefficients'],
                    ('sum_effective_prefill_tokens','sum_effective_prefill_times_next','output_tokens')))
        require(flops == int(row['useful_forward_flops']), 'FLOPs sufficient statistics differ')
        for ratio, seconds in [('mfu_release_query','release_query_seconds'),('mfu_postprep_query','postprep_query_seconds'),('mfu_http_span','http_span_seconds')]:
            require(math.isclose(float(row[ratio]),flops/(165200000000000*float(row[seconds])),rel_tol=1e-12),'MFU ratio differs')
    response_ids = set(); checked = 0
    for row in rows:
        root = args.original_scope if row['run_group'] == 'long_run' else (args.paired_scopes/row['query_key'] if args.paired_scopes else None)
        if root is not None:
            check_timing(root.absolute(),row,response_ids,metadata['common_runtime']['generation']); checked += 1
    if args.original_scope:
        check_ledger(args.original_scope.absolute(),0,76000,metadata['long_run']['source_ledger_sha256'])
    if args.paired_scopes:
        for case in metadata['matched_history']['cases']:
            check_ledger((args.paired_scopes/case['case_id']).absolute(),case['history_rows'],2000,
                         prefix_sha=metadata['matched_history']['history_prefix_sha256'])
    print(json.dumps(dict(passed=True,rows=46,raw_queries_checked=checked,
        raw_responses_checked=len(response_ids),aggregates=aggregates(rows)),indent=2))


if __name__ == '__main__':
    main()
