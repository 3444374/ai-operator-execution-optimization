"""Recompute all eight Q2 timings from complete compact observations, offline.

Usage: python recompute_ray_first_use_warm.py ray-first-use-warm-q2-timings.json.gz
No SDK, model, raw request/response or private path is needed.
"""
import argparse
import gzip
import json
import math
from pathlib import Path

def describe(values):
    ordered=sorted(values);n=len(ordered)
    if not n or any(not math.isfinite(v) or v<0 for v in ordered):
        raise ValueError('invalid completed duration observations')
    return dict(count=n,unit='seconds',minimum=ordered[0],maximum=ordered[-1],mean=math.fsum(ordered)/n,
        p50=ordered[(n*50+99)//100-1],p95=ordered[(n*95+99)//100-1],p99=ordered[(n*99+99)//100-1],
        method='nearest rank ceil(n*p), descriptive observations; not population tail')

def compute(observations):
    output=[]
    for query in observations['queries']:
        events=query['owner_proxy_http_ns']
        if len(events)!=120 or any(len(e)!=4 or e!=sorted(e) for e in events):
            raise ValueError('complete original Q2 timing count/order differs')
        arrival,ready,eof=query['query_arrival_ready_eof_ns']
        merged=[]
        for start,_,_,end in sorted(events):
            if merged and start<=merged[-1][1]:merged[-1][1]=max(merged[-1][1],end)
            else:merged.append([start,end])
        if not arrival<=ready<=merged[0][0]<=merged[-1][1]<=eof:
            raise ValueError('query/HTTP timing bounds differ')
        active=peak=0
        for _,delta in sorted([(e[0],1) for e in events]+[(e[3],-1) for e in events]):
            active+=delta;peak=max(active,peak)
            if active<0:raise ValueError('negative completed HTTP interval count')
        if active:raise ValueError('HTTP activity not terminal')
        gaps=[merged[i][0]-merged[i-1][1] for i in range(1,len(merged))]
        repeated=query['sdk_batch_wait_seconds']
        if len(repeated)!=120 or len(set(repeated))!=1:
            raise ValueError('whole original SDK batch wait labels differ')
        output.append(dict(owner=query['owner'],unit_id=query['unit_id'],role=query['role'],
            task_completion_seconds=(eof-arrival)/1e9,ready_completion_seconds=(eof-ready)/1e9,
            arrival_to_first_http_seconds=(merged[0][0]-arrival)/1e9,
            first_to_last_http_seconds=(merged[-1][1]-merged[0][0])/1e9,
            http_activity_covered_seconds=sum(e-s for s,e in merged)/1e9,
            http_activity_gap_seconds=sum(gaps)/1e9,http_gap_count=len(gaps),
            maximum_http_gap_seconds=max(gaps,default=0)/1e9,
            last_http_to_consumer_eof_seconds=(eof-merged[-1][1])/1e9,observed_peak_http=peak,
            request_statistics=dict(owner_proxy_request=describe([(e[3]-e[0])/1e9 for e in events]),
                gateway_before_dispatch=describe([(e[1]-e[0])/1e9 for e in events]),
                upstream_roundtrip=describe([(e[2]-e[1])/1e9 for e in events]),
                gateway_after_upstream=describe([(e[3]-e[2])/1e9 for e in events])),
            sdk_batch_wait=dict(seconds=repeated[0],row_labels=120,distinct_measured_spans=1,
                request_quantiles='unavailable: one original SDK batch wait repeated as row labels'),
            query_p99='unavailable: few first-use/serial warm observations do not establish population tail'))
    if len(output)!=8 or len({(q['owner'],q['unit_id']) for q in output})!=8:
        raise ValueError('all eight owner/query observations required')
    return dict(queries=output,model_or_network_calls=0)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('observations',type=Path);args=parser.parse_args()
    with gzip.open(args.observations,'rt') as stream:output=compute(json.load(stream))
    print(json.dumps(output,ensure_ascii=False,indent=2))
