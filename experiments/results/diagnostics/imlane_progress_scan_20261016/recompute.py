"""Recompute every cell from the retained, compressed interval observations."""
import gzip
import json
import math
from pathlib import Path
import statistics
import sys


def milliseconds(values):
    values = sorted(value/1e6 for value in values)
    if not values:
        return None
    return dict(n=len(values), mean=statistics.fmean(values),
                p50=values[math.ceil(.5*len(values))-1],
                p99=values[math.ceil(.99*len(values))-1], maximum=values[-1], total=sum(values))


def summarize(bundle):
    cells = []
    for cell in bundle['cells']:
        assert cell['final'][0]-cell['initial'][0]==768
        assert cell['unfinished']==12 and cell['failed']==68
        assert cell['http_posts']==244 and cell['peak_http']==4
        assert len(cell['workers'])==4 and all(len(w['logical'])==64 for w in cell['workers'])
        polls = cell['polls']
        for before, after in zip(polls, polls[1:]):
            assert all(x is None or y is not None and y>=x for x, y in zip(before['events'], after['events']))
        assert all(x is None or y is not None and y>=x for x, y in zip(polls[-1]['events'], cell['final']))
        samples = cell['initialization']+[row for w in cell['workers'] for row in w['timing']]
        operations = sorted({row['operation'] for row in samples})
        stages = {operation: milliseconds([r['end_ns']-r['start_ns'] for r in samples if r['operation']==operation])
                  for operation in operations}
        cells.append(dict(history_rows=cell['history_rows'], order=cell['order'], arm=cell['arm'],
                          full_fixture_s=cell['total_fixture_ns']/1e9, work_s=cell['work_wall_ns']/1e9,
                          progress_wall_ms=milliseconds([r['end_ns']-r['start_ns'] for r in polls]),
                          progress_thread_cpu_ms=milliseconds([r['cpu_ns'] for r in polls]),
                          poll_interval_ms=milliseconds([b['start_ns']-a['start_ns'] for a, b in zip(polls, polls[1:])]),
                          stages_ms=stages))
    pairs = []
    for size in bundle['metadata']['plan']['history_rows']:
        runs = [cell for cell in cells if cell['history_rows']==size]
        comparisons = [('forward', runs[0], runs[1]), ('reverse', runs[3], runs[2])]
        if 'index_scan' in bundle['metadata']['plan']['order']:
            comparisons = [('index_write_forward', runs[0], runs[1]), ('index_write_reverse', runs[5], runs[4]),
                           ('indexed_read_forward', runs[1], runs[2]), ('indexed_read_reverse', runs[4], runs[3]),
                           ('net_forward', runs[0], runs[2]), ('net_reverse', runs[5], runs[3])]
        for direction, before, after in comparisons:
            pairs.append(dict(history_rows=size, direction=direction,
                              full_before_s=before['full_fixture_s'], full_after_s=after['full_fixture_s'],
                              full_change_percent=100*(after['full_fixture_s']/before['full_fixture_s']-1),
                              work_change_percent=100*(after['work_s']/before['work_s']-1)))
    result = dict(quantiles='nearest rank over all retained calls, not independent-query confidence intervals',
                  pure_lock_wait='unavailable for natural polling; SQLite call intervals include SQL/I/O and waiting',
                  cells=cells, pairs=pairs)
    if 'attribution' in bundle:
        result['attribution'] = summarize(bundle['attribution'])
    return result


if __name__ == '__main__':
    root = Path(__file__).resolve().parent
    with gzip.open(root/'raw/paired.json.gz', 'rt') as stream:
        result = summarize(json.load(stream))
    if len(sys.argv)>1:
        Path(sys.argv[1]).write_text(json.dumps(result, separators=(',', ':'))+'\n')
    else:
        print(json.dumps(result, indent=2))
