"""Optional vLLM 0.25.1 scheduled-work FLOPs observations, not hardware MFU.

No I/O in the native logger. Missing/invalid observations never affect inference.
Counters are per physical GPU; the current verified binding is TP=PP=DP=1.
"""
from __future__ import annotations

import json
import math
import re
import time

FIELDS = ('flops_per_gpu', 'read_bytes_per_gpu', 'write_bytes_per_gpu',
          'prompt_total', 'prompt_computed', 'prompt_local_cached', 'prompt_transferred',
          'generation_tokens', 'finished', 'aborted', 'prefix_queries', 'prefix_hits')
PROM = {
    'vllm:estimated_flops_per_gpu_total': 'flops_per_gpu',
    'vllm:estimated_read_bytes_per_gpu_total': 'read_bytes_per_gpu',
    'vllm:estimated_write_bytes_per_gpu_total': 'write_bytes_per_gpu',
    'vllm:prompt_tokens_total': 'prompt_total',
    'vllm:generation_tokens_total': 'generation_tokens',
    'vllm:prefix_cache_queries_total': 'prefix_queries',
    'vllm:prefix_cache_hits_total': 'prefix_hits',
}


def unavailable(reason):
    return dict(status='unavailable', reason=reason, estimated_MFU=None)


def configuration_snapshot(config):
    try:
        return dict(status='observed',model_dtype=str(config.model_config.dtype),
            quantization=config.model_config.quantization,
            prefix_caching=config.cache_config.enable_prefix_caching,
            cache_block_size=config.cache_config.block_size,
            enable_mfu_metrics=config.observability_config.enable_mfu_metrics,
            tensor_parallel_size=config.parallel_config.tensor_parallel_size,
            pipeline_parallel_size=config.parallel_config.pipeline_parallel_size,
            data_parallel_size=config.parallel_config.data_parallel_size,
            accumulation_dtype_per_kernel='not instrumented; peak uses stated BF16/FP32 accumulate reference')
    except Exception as error:
        return unavailable('optional effective engine config: '+type(error).__name__)


def optional_packet(value, snapshot, encode, *args):
    """Observation bytes cannot turn an otherwise valid business frame invalid."""
    original = encode(value, *args)
    try:
        return encode(dict(value, native_mfu=snapshot()), *args)
    except Exception:
        return original


def number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


class NativeMfuTotals:
    """One owner-lifetime accumulator; never resets at query/task boundaries."""
    def __init__(self, engine_uid, *, clock=time.monotonic):
        self.uid, self.clock = engine_uid, clock
        self.totals = dict.fromkeys(FIELDS, 0)
        self.records = self.perf_records = self.missing_perf = self.errors = 0
        self.callback_wall_s = 0.

    def record(self, scheduler, iteration, engine_idx=0):
        start = self.clock()
        try:
            if engine_idx != 0:
                raise ValueError('current MFU binding supports one engine/GPU')
            delta = dict.fromkeys(FIELDS, 0)
            if scheduler is not None:
                perf = scheduler.perf_stats
                if perf is not None:
                    delta.update(flops_per_gpu=perf.num_flops_per_gpu,
                        read_bytes_per_gpu=perf.num_read_bytes_per_gpu,
                        write_bytes_per_gpu=perf.num_write_bytes_per_gpu)
                    self.perf_records += 1
                elif iteration is not None and (iteration.num_generation_tokens or iteration.finished_requests):
                    self.missing_perf += 1
                delta.update(prefix_queries=scheduler.prefix_cache_stats.queries,
                             prefix_hits=scheduler.prefix_cache_stats.hits)
            if iteration is not None:
                pts = iteration.prompt_token_stats
                delta.update(prompt_total=pts.total, prompt_computed=pts.computed,
                    prompt_local_cached=pts.local_cache_hit, prompt_transferred=pts.external_kv_transfer,
                    generation_tokens=iteration.num_generation_tokens)
                for item in iteration.finished_requests:
                    aborted = getattr(item.finish_reason, 'name', str(item.finish_reason)).lower() == 'abort'
                    delta['aborted' if aborted else 'finished'] += 1
            if not all(number(x) for x in delta.values()):
                raise ValueError('invalid native counter increment')
            for key, value in delta.items():
                self.totals[key] += value
            self.records += 1
        except Exception:
            self.errors += 1
        finally:
            self.callback_wall_s += max(0., self.clock() - start)

    def snapshot(self):
        return dict(schema='semloom.native_mfu.v1', status='observed' if self.perf_records else 'initialized',
            engine_uid=self.uid, engine_idx=0, GPU_count=1, counters=dict(self.totals),
            sequence=self.records, perf_records=self.perf_records,
            missing_perf_updates=self.missing_perf, observation_errors=self.errors,
            observed_at_s=self.clock(), source='vLLM scheduler_stats.perf_stats per-step delta',
            callback_wall_s=self.callback_wall_s, estimate_kind='native scheduler-aware analytic FLOPs')


def _prom_rows(text):
    # The installed exporter emits standard numeric samples without timestamps.
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        item, _, raw = line.rpartition(' ')
        name, brace, labels = item.partition('{')
        if name not in PROM and name not in ('vllm:prompt_tokens_by_source_total', 'vllm:request_success_total'):
            continue
        parsed = {}
        if brace:
            if not labels.endswith('}'):
                raise ValueError('malformed selected metric labels')
            for match in re.finditer(r'(\w+)=("(?:[^"\\]|\\.)*")', labels[:-1]):
                parsed[match[1]] = json.loads(match[2])
        yield dict(name=name, labels=parsed, value=float(raw))


def prometheus_snapshot(metrics, engine_uid, *, clock=time.monotonic):
    """Same counters from HTTP exposition or public LLM.get_metrics snapshots."""
    try:
        if isinstance(metrics, str):
            rows = list(_prom_rows(metrics))
        else:
            # Installed vLLM public reader uses the Counter family's base
            # name, whereas HTTP exposition uses its *_total sample name.
            names=set(PROM)|{'vllm:prompt_tokens_by_source_total','vllm:request_success_total'}
            rows=[dict(name=m.name+'_total' if m.name+'_total' in names else m.name,
                labels=dict(m.labels),value=m.value) for m in metrics
                if m.name in names or m.name+'_total' in names]
        counters, labels_seen = {}, set()
        for row in rows:
            name, labels, value = row['name'], row['labels'], row['value']
            # vLLM's public reader retains the exported *_total counter name.
            key = PROM.get(name)
            if name == 'vllm:prompt_tokens_by_source_total':
                key = {'local_compute': 'prompt_computed', 'local_cache_hit': 'prompt_local_cached',
                       'external_kv_transfer': 'prompt_transferred'}.get(labels.get('source'))
            elif name == 'vllm:request_success_total':
                key = 'aborted' if labels.get('finished_reason', '').lower() == 'abort' else 'finished'
            if key is None:
                continue
            if not number(value):
                raise ValueError('invalid cumulative native metric')
            identity = tuple(sorted((k, v) for k, v in labels.items() if k not in ('source', 'finished_reason')))
            labels_seen.add(identity)
            counters[key] = counters.get(key, 0) + value
        if len(labels_seen) != 1 or any(key not in counters for key in FIELDS):
            return unavailable('native counters missing or multiple engine/model labels')
        return dict(schema='semloom.native_mfu.v1', status='observed', engine_uid=engine_uid,
            engine_idx=0, GPU_count=1, labels=list(next(iter(labels_seen))), counters=counters,
            missing_perf_updates=0, observation_errors=0, observed_at_s=clock(),
            source='vLLM 0.25.1 native cumulative Prometheus counters',
            estimate_kind='native scheduler-aware analytic FLOPs')
    except Exception as error:
        return unavailable('optional native metrics: ' + type(error).__name__)


def estimate(before, after, *, wall_s, expected_calls, expected_prompt_tokens,
             expected_output_tokens, exclusive_query=True, peak_flops_per_gpu=165.2e12,
             precision='BF16 Tensor / FP32 accumulate, dense'):
    """No clipping or guessed zeros. Exact watermarks certify the last window."""
    try:
        if not exclusive_query:
            return unavailable('overlapping queries cannot be attributed by global counter delta')
        if before['status'] not in ('observed', 'initialized') or after['status'] != 'observed':
            return unavailable('native boundary unavailable')
        if before['engine_uid'] != after['engine_uid'] or before.get('labels') != after.get('labels'):
            return unavailable('engine/label restart or mismatch')
        if before['GPU_count'] != after['GPU_count'] or after['GPU_count'] != 1:
            return unavailable('multi-GPU attribution not validated')
        if not number(wall_s) or wall_s == 0 or not number(peak_flops_per_gpu) or peak_flops_per_gpu == 0:
            return unavailable('invalid denominator')
        if any(after.get(k, 0) != before.get(k, 0) for k in ('missing_perf_updates', 'observation_errors')):
            return unavailable('native statistics incomplete or collector failed')
        delta = {key: after['counters'][key] - before['counters'][key] for key in FIELDS}
        if not all(number(x) for x in delta.values()):
            return unavailable('counter reset or nonfinite delta')
        if expected_calls and delta['flops_per_gpu'] <= 0:
            return unavailable('no positive native computed-work counter')
        close = lambda x, y: abs(x-y) <= max(1e-6, 1e-12 * max(abs(x), abs(y)))
        if not (close(delta['finished'], expected_calls) and close(delta['aborted'], 0)
                and close(delta['prompt_total'], expected_prompt_tokens)
                and close(delta['generation_tokens'], expected_output_tokens)
                and close(delta['prompt_computed'] + delta['prompt_local_cached'] + delta['prompt_transferred'],
                          delta['prompt_total'])):
            return unavailable('last-window/usage watermark incomplete or extra work observed')
        ratio = delta['flops_per_gpu'] / (peak_flops_per_gpu * wall_s)
        return dict(status='estimated', estimated_MFU=ratio, estimated_MFU_pct=ratio*100,
            native_counter_delta=delta, wall_s=wall_s, GPU_count=1,
            per_GPU_numerator_not_divided_again=True, peak_flops_per_gpu=peak_flops_per_gpu,
            precision=precision, peak_basis='NVIDIA Ada v2.02 appendix A p30, boost-clock dense BF16/FP32-accumulate',
            peak_source='https://images.nvidia.cn/aem-dam/Solutions/geforce/ada/nvidia-ada-gpu-architecture.pdf#page=30',
            actual_clock_adjusted=False, actual_forward_kernel_time='unavailable',
            interpretation='native analytic scheduled work / theoretical dense peak / declared wall; not hardware instruction utilization',
            above_theoretical_peak=ratio > 1., terminal_watermark_complete=True)
    except Exception as error:
        return unavailable('optional MFU estimate: ' + type(error).__name__)
