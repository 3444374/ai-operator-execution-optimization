"""CPU checks for native cumulative MFU counters; no model or CUDA calls."""
from types import SimpleNamespace as NS
import copy
import json
import unittest
from src.observability.metrics.mfu import NativeMfuTotals, prometheus_snapshot, estimate, optional_packet

def stats(flops=1000, finished=4, cached=0):
    scheduler = NS(perf_stats=NS(num_flops_per_gpu=flops, num_read_bytes_per_gpu=20, num_write_bytes_per_gpu=10),
                   prefix_cache_stats=NS(queries=400, hits=cached))
    iteration = NS(prompt_token_stats=NS(total=400, computed=400-cached, local_cache_hit=cached, external_kv_transfer=0),
                   num_generation_tokens=16, finished_requests=[NS(finish_reason=NS(name='length')) for _ in range(finished)])
    return scheduler, iteration


def watermarks():
    totals = NativeMfuTotals('u', clock=lambda: 10.)
    before = totals.snapshot();totals.record(*stats())
    return totals, before, totals.snapshot()


def calculate(before, after, **kwargs):
    return estimate(before, after, wall_s=kwargs.pop('wall_s', 2), expected_calls=4,
        expected_prompt_tokens=400, expected_output_tokens=16, **kwargs)


class MFUTests(unittest.TestCase):
    def test_exact_terminal_window_and_per_GPU_denominator(self):
        _, before, after = watermarks();out = calculate(before, after)
        self.assertEqual(out['status'], 'estimated')
        self.assertAlmostEqual(out['estimated_MFU'], 1000/(165.2e12*2))


    def test_delayed_last_frame_remains_unavailable_until_complete(self):
        totals, before, after = watermarks();partial = copy.deepcopy(after)
        partial['counters']['finished'] = 3
        self.assertEqual(calculate(before, partial)['status'], 'unavailable')
        self.assertEqual(calculate(before, after)['status'], 'estimated')


    def test_task_switch_does_not_reset_and_reuse_delta_is_not_double_counted(self):
        totals, _, first = watermarks();totals.record(*stats(flops=500, cached=256));second = totals.snapshot()
        out=calculate(first,second)
        self.assertEqual(out['native_counter_delta']['flops_per_gpu'],500)
        self.assertEqual(out['native_counter_delta']['prompt_local_cached'],256)


    def test_missing_perf_never_looks_like_zero_work(self):
        totals, before, _ = watermarks();s,i=stats();s.perf_stats=None;totals.record(s,i)
        self.assertEqual(calculate(before,totals.snapshot())['status'],'unavailable')

    def test_missing_prompt_only_prefill_perf_is_incomplete_at_terminal(self):
        totals = NativeMfuTotals('u')
        before = totals.snapshot()
        scheduler, iteration = stats()
        scheduler.perf_stats = None
        iteration.num_generation_tokens = 0
        iteration.finished_requests = []
        totals.record(scheduler, iteration)
        scheduler, iteration = stats()
        iteration.prompt_token_stats = NS(total=0, computed=0, local_cache_hit=0, external_kv_transfer=0)
        totals.record(scheduler, iteration)
        after = totals.snapshot()
        self.assertEqual(after['counters']['prompt_computed'], 400)
        self.assertEqual(after['counters']['finished'], 4)
        self.assertEqual(calculate(before, after)['status'], 'unavailable')

    def test_missing_scheduler_prefill_is_incomplete_at_terminal(self):
        totals = NativeMfuTotals('u')
        before = totals.snapshot()
        _, iteration = stats()
        iteration.num_generation_tokens = 0
        iteration.finished_requests = []
        totals.record(None, iteration)
        scheduler, iteration = stats()
        iteration.prompt_token_stats = NS(total=0, computed=0, local_cache_hit=0, external_kv_transfer=0)
        totals.record(scheduler, iteration)
        after = totals.snapshot()
        self.assertEqual(after['counters']['prompt_computed'], 400)
        self.assertEqual(after['missing_perf_updates'], 1)
        self.assertEqual(after['observation_errors'], 0)
        self.assertEqual(calculate(before, after)['status'], 'unavailable')

    def test_missing_scheduler_cached_only_frame_does_not_invent_missing_flops(self):
        totals = NativeMfuTotals('u')
        before = totals.snapshot()
        _, iteration = stats(cached=400)
        iteration.num_generation_tokens = 0
        iteration.finished_requests = []
        totals.record(None, iteration)
        scheduler, iteration = stats()
        iteration.prompt_token_stats = NS(total=0, computed=0, local_cache_hit=0, external_kv_transfer=0)
        totals.record(scheduler, iteration)
        self.assertEqual(totals.snapshot()['missing_perf_updates'], 0)
        self.assertEqual(calculate(before, totals.snapshot())['status'], 'estimated')

    def test_fully_cached_prompt_without_compute_does_not_invent_missing_flops(self):
        totals = NativeMfuTotals('u')
        before = totals.snapshot()
        scheduler, iteration = stats(cached=400)
        scheduler.perf_stats = None
        iteration.num_generation_tokens = 0
        iteration.finished_requests = []
        totals.record(scheduler, iteration)
        scheduler, iteration = stats()
        iteration.prompt_token_stats = NS(total=0, computed=0, local_cache_hit=0, external_kv_transfer=0)
        totals.record(scheduler, iteration)
        self.assertEqual(calculate(before, totals.snapshot())['status'], 'estimated')


    def test_bad_native_fields_are_observation_errors_not_query_errors(self):
        totals=NativeMfuTotals('u');s,i=stats();s.perf_stats.num_flops_per_gpu=float('nan')
        totals.record(s,i);self.assertEqual(totals.errors,1)


    def test_identity_reset_label_change_negative_and_unknown_refused(self):
        _,before,after=watermarks()
        for change in ('uid','labels','counter','missing','gpu','absent'):
            with self.subTest(change=change):
                a=copy.deepcopy(after)
                if change=='uid':a['engine_uid']='other'
                elif change=='labels':a['labels']=[['engine','other']]
                elif change=='counter':a['counters']['read_bytes_per_gpu']=-1
                elif change=='missing':a['observation_errors']+=1
                elif change=='gpu':a['GPU_count']=2
                else:a['status']='unavailable'
                self.assertEqual(calculate(before,a)['status'],'unavailable')


    def test_overlap_and_bad_denominators_are_unavailable(self):
        _,before,after=watermarks()
        self.assertEqual(calculate(before,after,exclusive_query=False)['status'],'unavailable')
        for wall in (0,-1,float('nan')):
            self.assertEqual(calculate(before,after,wall_s=wall)['status'],'unavailable')


    def test_aborted_or_extra_work_is_not_successful_query_MFU(self):
        _,before,after=watermarks()
        for key in ('finished','aborted','prompt_total','generation_tokens'):
            a=copy.deepcopy(after);a['counters'][key]+=1
            self.assertEqual(calculate(before,a)['status'],'unavailable')


    def test_above_peak_is_not_clipped_or_hidden(self):
        _,before,after=watermarks();after['counters']['flops_per_gpu']=2*165.2e12*2
        out=calculate(before,after);self.assertEqual(out['estimated_MFU'],2);self.assertTrue(out['above_theoretical_peak'])


    def test_frame_attachment_error_cannot_change_business_frame(self):
        event=dict(op='terminal',result=dict(status='succeeded'))
        encode=lambda x:json.dumps(x).encode()
        def broken():raise RuntimeError('optional')
        self.assertEqual(optional_packet(event,broken,encode),encode(event))
        def limited(x):
            if 'native_mfu' in x:raise ValueError('capacity')
            return encode(x)
        self.assertEqual(optional_packet(event,lambda:{'a':'large'},limited),encode(event))
        self.assertEqual(event,dict(op='terminal',result=dict(status='succeeded')))


    def test_batch_four_has_one_cumulative_delta_not_four_copies(self):
        _,before,after=watermarks()
        output=calculate(before,after)
        self.assertEqual(output['native_counter_delta']['finished'],4)
        self.assertEqual(output['native_counter_delta']['flops_per_gpu'],1000)


    def test_prometheus_matches_native_counters_and_preserves_labels(self):
        _,_,after=watermarks();c=after['counters'];lines=[]
        from src.observability.metrics.mfu import PROM
        for name,key in PROM.items():lines.append(f'{name}{{engine="0",model_name="m"}} {c[key]}')
        for source,key in [('local_compute','prompt_computed'),('local_cache_hit','prompt_local_cached'),('external_kv_transfer','prompt_transferred')]:
            lines.append(f'vllm:prompt_tokens_by_source_total{{engine="0",model_name="m",source="{source}"}} {c[key]}')
        for reason,key in [('length','finished'),('abort','aborted')]:
            lines.append(f'vllm:request_success_total{{engine="0",model_name="m",finished_reason="{reason}"}} {c[key]}')
        result=prometheus_snapshot('\n'.join(lines),'u')
        self.assertEqual(result['counters'],c)
        metrics=[NS(name=line.split('{')[0],labels={'engine':'0','model_name':'m'},value=0) for line in lines[:1]]
        self.assertEqual(prometheus_snapshot(metrics,'u')['status'],'unavailable')
        self.assertEqual(prometheus_snapshot('', 'u')['status'],'unavailable')
        self.assertEqual(prometheus_snapshot('\n'.join(lines)+ '\nvllm:estimated_flops_per_gpu_total{engine="1",model_name="m"} 0','u')['status'],'unavailable')
