"""Offline LOTUS row/response and Core responsibility replay; no HTTP or models."""

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.query_sharing_lotus import audit


def compute_ledger(run):
    active, submitted, terminal = set(), set(), set()
    opened = {e['session_id']: e['query_id'] for e in run['bridge_events'] if e['event'] == 'query_open'}
    at_cancel = set()
    cancellation = run.get('cancel_observed')
    long_after_cancel = 0
    for event in run['core_events']:
        if 'usage' not in event:
            continue
        key = (event['key']['session_id'], event['key']['sequence'])
        if event['event'] == 'submitted':
            if key in submitted:
                raise ValueError('duplicate Core submission')
            submitted.add(key)
            active.add(key)
            if cancellation is not None and event['t'] > cancellation and opened[key[0]] == 'long':
                long_after_cancel += 1
        elif event['event'] == 'terminal':
            if key not in active or key in terminal:
                raise ValueError('terminal without exactly one outstanding submission')
            active.remove(key)
            terminal.add(key)
        # This protocol estimates every request as one work_unit. Compute
        # responsibility ends at terminal, independently of result lease release.
        if any(event['usage'][field] != len(active) for field in ('active_requests', 'active_work')):
            raise ValueError('Core compute responsibility ended before terminal or differs')
        if cancellation is not None and event['t'] <= cancellation:
            at_cancel = active.copy()
    if active or terminal != submitted:
        raise ValueError('Core terminal responsibility remains')
    query_to_session = {query: session for session, query in opened.items()}
    http_keys = {(query_to_session[c['query_id']], c['sequence']) for c in run['calls']}
    if not http_keys <= submitted:
        raise ValueError('HTTP work did not belong to a Core submission')
    for query, result in run['frontends'].items():
        if result['event'] == 'finished':
            expected = [[i, 'POSITIVE' if i % 2 == 0 else 'NEGATIVE'] for i in range(run['counts'][query])]
            if result['rows'] != expected or result['cleanup_errors']:
                raise ValueError('completed query row association or consumer cleanup failed')
    if cancellation is not None:
        result = run['frontends']['long']
        if (result['event'] != 'error' or result['error_type'] != 'RuntimeError'
                or result['error'] not in ('LOTUS batch cancelled', 'LOTUS execution stopped')):
            raise ValueError('long cancellation outcome differs')
        short = {sequence for session, sequence in http_keys if opened[session] == 'short'}
        if short != set(range(run['counts']['short'])) or run['frontends']['short']['event'] != 'finished':
            raise ValueError('another query lost required work during cancellation')
    return dict(submitted=len(submitted), terminal=len(terminal), physical_http=len(http_keys),
        locally_terminal_without_http=len(submitted - http_keys), compute_at_cancel=len(at_cancel),
        long_compute_at_cancel=sum(opened[key[0]] == 'long' for key in at_cancel),
        long_submitted_after_cancel=long_after_cancel,
        long_inflight_cancellation_exercised=any(opened[key[0]] == 'long' for key in at_cancel))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('output exists; retain the previous audit')
    report = json.loads(gzip.decompress(args.input.read_bytes()))
    summaries = audit(report['runs'])
    if report['status'] != 'passed' or summaries != report['summary']:
        raise ValueError('stored summary differs from the independent trace replay')
    result = dict(schema=1, source_commit=report['source_commit'], scope='offline lifecycle qualification; no performance claim',
        input_sha256=hashlib.sha256(args.input.read_bytes()).hexdigest(),
        audit_script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        request_audit_sha256=hashlib.sha256((Path(__file__).resolve().parents[2]/'src/experiments/query_sharing_lotus.py').read_bytes()).hexdigest(),
        source_sha256=report['source_sha256'], budget=report['budget'], runs=[])
    for run, summary in zip(report['runs'], summaries):
        result['runs'].append(dict(run_id=run['run_id'], arm=run['arm'], summary=summary, compute_ledger=compute_ledger(run)))
    with args.output.open('x') as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
    print(json.dumps(dict(status='passed', runs=len(summaries), fixture_posts=report['budget']['fixture_posts'])))


if __name__ == '__main__':
    main()
