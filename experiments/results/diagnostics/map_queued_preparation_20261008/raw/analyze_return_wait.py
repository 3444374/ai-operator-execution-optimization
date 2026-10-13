"""Bound synchronous-guard overlap with return waits from preserved observations."""
import argparse
import bisect
import hashlib
import json
from pathlib import Path
import statistics


class Intervals:
    def __init__(self, values):
        merged=[]
        for start,end in sorted(values):
            if start>=end:continue
            if merged and start<=merged[-1][1]:merged[-1]=(merged[-1][0],max(end,merged[-1][1]))
            else:merged.append((start,end))
        self.starts=[s for s,_ in merged]
        self.ends=[e for _,e in merged]
        self.prefix=[0]
        for start,end in merged:self.prefix.append(self.prefix[-1]+end-start)

    def integral(self,at):
        index=bisect.bisect_right(self.ends,at)
        covered=self.prefix[index]
        if index<len(self.starts):covered+=max(0,at-self.starts[index])
        return covered

    def overlap(self,start,end):return self.integral(end)-self.integral(start)


def analyze(root):
    records=[]
    for mode in ('immediate','prepared'):
        for repeat in (1,2,3):
            directory=root/'real01/worker'/(mode+'1024')/('measure'+str(repeat))
            assert json.loads((directory/'summary.json').read_text())['config']['remote_budget_mode']=='synchronous'
            path=directory/'events.jsonl'
            events=[json.loads(line) for line in path.read_text().splitlines()]
            def keyed(name):
                values=[e for e in events if e['event']==name]
                result={(e['key']['session_id'],e['key']['sequence']):e for e in values}
                assert len(result)==len(values)==1024
                return result
            inner,outer,calls=(keyed(name) for name in ('remote_request_guard','core_ray_request_guard','core_ray_http_completed'))
            assert inner.keys()==outer.keys()==calls.keys()
            lower,upper,widths=[],[],[]
            for key,item in outer.items():
                internal=inner[key]
                assert item['status']==internal['status']=='completed'
                start,end=internal['monotonic_ns'],item['monotonic_ns']
                elapsed=item['elapsed_ns']
                assert 0<elapsed and start<=end<=calls[key]['rpc_started_ns']
                lower.append((end-elapsed,start))
                upper.append((start-elapsed,end))
                widths.append(end-start)
            certain,possible=Intervals(lower),Intervals(upper)
            total=low=high=0
            for key,call in calls.items():
                start,end=call['worker_ended_ns'],call['received_ns']
                assert call['shared_clock'] and call['rpc_started_ns']<=call['worker_started_ns']<=start<=end
                total+=end-start
                low+=certain.overlap(start,end)
                high+=possible.overlap(start,end)
            assert 0<=low<=high<=total
            records.append(dict(mode=mode,repeat=repeat,events_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                requests=1024,after_worker_sum_seconds=total/1e9,mean_after_worker_ms=total/1024/1e6,
                certain_guard_overlap_seconds=low/1e9,possible_guard_overlap_seconds=high/1e9,
                certain_overlap_percent=100*low/total,possible_overlap_percent=100*high/total,
                outer_minus_inner_median_ms=statistics.median(widths)/1e6,
                outer_minus_inner_max_ms=max(widths)/1e6,guard_elapsed_sum_seconds=sum(e['elapsed_ns'] for e in outer.values())/1e9,
                guard_time_union_seconds=possible.prefix[-1]/1e9))
    medians={mode:{name:statistics.median(r[name] for r in records if r['mode']==mode) for name in
        ('mean_after_worker_ms','certain_overlap_percent','possible_overlap_percent','guard_elapsed_sum_seconds')}
        for mode in ('immediate','prepared')}
    return dict(status='passed',queries=records,medians=medians,verified_completions=6144,http_requests=0,model_requests=0,
        formula='outer end measurement lies between inner and outer observation; intersection/union of all candidate synchronous spans; overlap summed independently for each return interval',
        limitation='overlap is not causal attribution; Ray driver future completion was not recorded in the original run')


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--evidence',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    result=analyze(args.evidence)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
