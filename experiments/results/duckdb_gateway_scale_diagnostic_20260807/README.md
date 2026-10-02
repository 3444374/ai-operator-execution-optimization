# DuckDB经轮询网关的规模扩展诊断

2026-10-02存储整理：原始文件的路径、字节数和校验值见[恢复清单](raw/storage-manifest.jsonl)，
原始数据与脚本的恢复方式见[文件恢复说明](../../../code/scripts/README.md#实验结果恢复)。直接引用的数据和脚本仍可就地读取。

## Status and scope

This directory preserves the compact, auditable evidence for the 2026-08-07
enhanced LBRR scale ramp. The path is one DuckDB AI process through an nginx
round-robin gateway to two vLLM endpoints. Its identity remains
`gateway_system_diagnostic`; it is not a DuckDB-native multi-endpoint baseline.

- Workload: SQuAD v1.1 short-answer, scales 64 through 10,570 over the same
  nine-point grid as the bounded/DuckDB ramp.
- Service: 2 x RTX 4090, two Qwen2.5-7B vLLM endpoints, prefix cache enabled,
  `max_num_seqs=256`, `max_num_batched_tokens=8192`.
- Concurrency: 64 at the single gateway-facing DuckDB process.
- Repetitions: three measured cells per scale; 27/27 passed.

## Evidence layout

- `ramp_run.json`: authoritative status and backend request/work skew for all
  27 cells.
- `ramp_aggregate.json` and [规模聚合表](#scale-aggregate): all repetition values, means
  and sample-CV summaries.
- Each `scale_*/lb_rr_c64_rep*/` directory keeps `identity.json`,
  `gpu_resource.csv`, `ttft_metrics.json`, `vllm_gauges.json`, shard
  `summary.json` and `manifest_metadata.json`.

The aggregate is reproducible from these files with
`code/scripts/analysis/multicard_ramp_aggregate.py`; a 2026-08-12 clean rebuild
matched both committed aggregate files byte for byte.

上述复算描述对应2026-08-12的JSON与独立Markdown输出；本轮将表格并入本报告，复算脚本仍可生成独立表，当前数字来源继续是JSON与原始观测。

## Archive policy and conclusion boundary

The server audit intentionally excluded per-request `requests.csv` and logs.
The full server directory remains untouched. The committed evidence is enough
to audit status, identity, backend balance, service counters, TTFT/ITL,
running/waiting/KV and GPU/energy metrics without committing generated text.

The LBRR path uses query-barrier timing and a third-party gateway, so it must
not be ranked as a native DuckDB scheduler or mixed with request-level E2E
latency. Its scale curve is a gateway-system diagnostic. The joint four-path
interpretation is maintained in
`../multicard_proj_scale_ramp_formal_20260807/README.md` section 9.

本节合并原独立补充报告，数值、全部重复、失败说明与结论按原记录保留。

<a id="scale-aggregate"></a>
<a id="scale-aggregate-多卡-ramp-聚合规模-6410570mean-across-passed-reps"></a>
## 规模聚合表
| scale | arm | conc | status | tok/s mean | tok/s CV | rows/s | TTFT P50 | E2E P50 | prefix-hit | GPU0 util | GPU1 util | n_passed/n |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 64 | lb_rr | c64 | passed | 16997.2667 | 0.72% | 79.0733 | 57.5ms | — | 0.95 | 24.0 | 13.5 | 3/3 |
| 128 | lb_rr | c64 | passed | 24303.1667 | 2.39% | 136.4467 | 55.9ms | — | 0.94 | 44.5 | 27.2 | 3/3 |
| 256 | lb_rr | c64 | passed | 50786.0667 | 2.27% | 228.6733 | 54.1ms | — | 0.96 | 66.0 | 23.2 | 3/3 |
| 512 | lb_rr | c64 | passed | 62155.4667 | 1.38% | 284.7867 | 52.7ms | — | 0.96 | 56.1 | 47.6 | 3/3 |
| 1024 | lb_rr | c64 | passed | 67570.0667 | 0.81% | 297.93 | 52.5ms | — | 0.96 | 57.0 | 68.8 | 3/3 |
| 2048 | lb_rr | c64 | passed | 72457.6 | 0.63% | 342.4467 | 52.0ms | — | 0.96 | 66.7 | 66.7 | 3/3 |
| 4096 | lb_rr | c64 | passed | 39350.1667 | 0.41% | 180.1233 | 151.2ms | — | 0.62 | 85.5 | 85.5 | 3/3 |
| 8192 | lb_rr | c64 | passed | 39546.8667 | 1.03% | 163.9567 | 159.5ms | — | 0.61 | 90.8 | 90.3 | 3/3 |
| 10570 | lb_rr | c64 | passed | 38618.4667 | 0.25% | 160.5633 | 160.9ms | — | 0.60 | 91.0 | 91.0 | 3/3 |

<a id="scale-aggregate-效率与尾延迟75d-补齐mfu01-分数非-vllm-estimated_flops-保守估计"></a>
### 效率与尾延迟（§7.5D 补齐；MFU=[0,1] 分数，非 %；vLLM estimated_flops 保守估计）

| scale | arm | conc | MFU(frac) | ITL p95 | ITL p99 | TTFT p99 | decode | prefill | J/1k-tok |
|---|---|---|---|---|---|---|---|---|---|
| 64 | lb_rr | c64 | 0.046 | 26.6ms | 28.7ms | 82.4ms | 50.6ms | 37ms | 21.0167 |
| 128 | lb_rr | c64 | 0.078 | 24.3ms | 24.9ms | 85.2ms | 54ms | 37.8ms | 17.3233 |
| 256 | lb_rr | c64 | 0.122 | 24.4ms | 27.5ms | 84.5ms | 58.9ms | 38.5ms | 9.47 |
| 512 | lb_rr | c64 | 0.146 | 24.3ms | 24.9ms | 78.9ms | 59.5ms | 38.6ms | 8.0 |
| 1024 | lb_rr | c64 | 0.160 | 24.3ms | 24.9ms | 79.3ms | 70.8ms | 38.7ms | 8.3233 |
| 2048 | lb_rr | c64 | 0.188 | 24.3ms | 24.9ms | 79ms | 77.1ms | 38.9ms | 8.67 |
| 4096 | lb_rr | c64 | 0.620 | 76.4ms | 99.3ms | 248.3ms | 179.6ms | 91.9ms | 20.0633 |
| 8192 | lb_rr | c64 | 0.642 | 87ms | 121.6ms | 377.6ms | 207.8ms | 99.9ms | 20.57 |
| 10570 | lb_rr | c64 | 0.640 | 87.1ms | 123.8ms | 395.8ms | 214.2ms | 101ms | 21.13 |
