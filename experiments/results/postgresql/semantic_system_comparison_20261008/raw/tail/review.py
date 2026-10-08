"""Describe the retained cohort and matched rounds; no population-tail inference."""
from collections import Counter, defaultdict
import gzip, hashlib, itertools, json, math, statistics
from pathlib import Path

root = Path(__file__).resolve().parent
load = lambda name: json.loads((root / name).read_text())
records = load('records.json')
analysis = load('tail-analysis.json')
by_id = {r['unit_id']: r for r in records}


def stats(values):
    x = sorted(values)
    n = len(x)
    return dict(samples=n, minimum=min(x), median=statistics.median(x),
                mean=statistics.mean(x), maximum=max(x),
                sample_cv_percent=100*statistics.stdev(x)/statistics.mean(x) if n > 1 else None,
                p99=x[math.ceil(.99*n)-1], p9999_sample=x[math.ceil(.9999*n)-1])


repeated = defaultdict(list)
same_round_request = {}
first_predictions = {}
different_from_first = defaultdict(list)
different_from_request = defaultdict(list)
with gzip.open(root/'outputs.jsonl.gz', 'rt') as f:
    lines = (json.loads(line) for line in f)
    for identity, group in itertools.groupby(lines, lambda r: r['unit_id']):
        r = by_id[identity]
        rows = list(group)
        if r['stage'] != 'tail-evaluation':
            continue
        pred = {v['occurrence_sha256']: v['prediction'] for v in rows}
        role, repeat = r['role'], r['repeat']
        first_predictions.setdefault(role, pred)
        assert set(first_predictions[role]) == set(pred)
        different_from_first[role].append(dict(repeat=repeat,
            rows=sum(pred[k] != first_predictions[role][k] for k in pred)))
        if role == 'pg-request':
            same_round_request[repeat] = pred
        elif role in ('pg-token-wide', 'pg-work-limited'):
            reference = same_round_request[repeat]
            assert set(reference) == set(pred)
            different_from_request[role].append(dict(repeat=repeat,
                rows=sum(pred[k] != reference[k] for k in pred)))

rows = []
sql_by_role = {}
for a in analysis['rows']:
    role = a['role']
    measured = sorted((r for r in records if r['role'] == role and r['stage'] == 'tail-evaluation'),
                      key=lambda r: r['repeat'])
    sql_by_role[role] = [r['full_query_seconds'] for r in measured[:136]]
    qualities = [100*(r['quality']['true_positive']+r['quality']['true_negative'])/512 for r in measured]
    assert all(r['quality']['invalid'] == r['quality']['missing_rows'] == 0 for r in measured)
    counters = ('vllm:prompt_tokens_total', 'vllm:generation_tokens_total', 'vllm:request_success_total')
    quarters = []
    for lo, hi in ((1, 34), (35, 68), (69, 102), (103, 136)):
        chosen = [r for r in measured if lo <= r['repeat'] <= hi]
        quarters.append(dict(first_round=lo, last_round=hi,
                             full_query_s=stats([r['full_query_seconds'] for r in chosen])))
    units = {u['unit_id']: u for u in a['units']}
    slow = []
    for r in sorted(measured, key=lambda r: r['full_query_seconds'], reverse=True)[:5]:
        u = units[r['unit_id']]
        slow.append(dict(unit_id=r['unit_id'], full_query_s=u['full_query_s'],
                         preparation_s=u['preparation_s'],
                         release_to_eof_s=u['segments']['release_to_eof_query']['p50_s'],
                         http_p99_s=u['segments']['http_observed']['p99_s']))
    rows.append(dict(role=role, complete_queries=len(measured), sql_s=stats([r['full_query_seconds'] for r in measured]),
        quality_percent=dict(minimum=min(qualities), median=statistics.median(qualities), maximum=max(qualities),
                             independent_source_rows=512,
                             unique_confusion_counts=[list(v) for v in sorted(set(tuple(r['quality'][k] for k in
                                 ('true_positive', 'false_positive', 'true_negative', 'false_negative')) for r in measured))]),
        http_peak_histogram=dict(Counter(r['observed_peak_http'] for r in measured)),
        counters_per_query={k:stats([r['model_counter_delta'][k] for r in measured]) for k in counters},
        same_role_prediction_differences_from_first=different_from_first[role],
        same_round_prediction_differences_from_pg_request=different_from_request[role],
        first_136_quarters=quarters, slow_queries=slow))

paired = []
for role in ('pg-token-wide', 'pg-work-limited', 'pg-map'):
    baseline, candidate = sql_by_role['pg-request'], sql_by_role[role]
    assert len(baseline) == len(candidate) == 136
    diffs = [c-b for b, c in zip(baseline, candidate)]
    relative = [100*(c/b-1) for b, c in zip(baseline, candidate)]
    paired.append(dict(candidate=role, baseline='pg-request', matched_rounds=136,
                       median_candidate_minus_baseline_s=statistics.median(diffs),
                       median_relative_query_percent=statistics.median(relative),
                       candidate_faster_rounds=sum(v < 0 for v in diffs),
                       differences_s=diffs))

out = dict(schema='semloom.semantic_tail_review.v1', new_model_requests=0,
           rows=rows, paired=paired,
           interpretation='descriptive fixed-order repeated cohort; 512 distinct source rows; same-round pairing is not randomized or a superiority test',
           sources={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in
                    ('records.json', 'tail-analysis.json', 'outputs.jsonl.gz')})
(root/'tail-review.json').write_text(json.dumps(out, sort_keys=True, indent=2)+'\n')
print(json.dumps(dict(rows=[{k:v for k,v in r.items() if k not in
    ('same_role_prediction_differences_from_first','same_round_prediction_differences_from_pg_request','slow_queries')}
    for r in rows], paired=[{k:v for k,v in r.items() if k != 'differences_s'} for r in paired])))
