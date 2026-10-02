# DuckDB与有界HTTP的规模扩展诊断

2026-10-02存储整理：原始文件的路径、字节数和校验值见[恢复清单](raw/storage-manifest.jsonl)，
原始数据与脚本的恢复方式见[文件恢复说明](../../../code/scripts/README.md#实验结果恢复)。直接引用的数据和脚本仍可就地读取。

## Status and scope

This directory preserves the compact, auditable evidence for the 2026-08-07
enhanced scale ramp. The rerun added during-cell vLLM gauge sampling and the
identity sidecar needed by the project evidence contract; it did not introduce
a new scheduling method or change the existing comparison boundary.

- Workload: SQuAD v1.1 short-answer, scales 64, 128, 256, 512, 1,024, 2,048,
  4,096, 8,192 and 10,570.
- Service: 2 x RTX 4090, two Qwen2.5-7B vLLM endpoints, prefix cache enabled,
  `max_num_seqs=256`, `max_num_batched_tokens=8192`.
- Arms: direct bounded HTTP at concurrency 32 per endpoint, and the
  harness-pre-split DuckDB AI diagnostic at concurrency 32 per endpoint.
- Repetitions: three measured cells per scale and arm.

The direct arm passed 27/27 cells. DuckDB AI passed 22/27: repetitions 2 and 3
at scale 8,192 and all three repetitions at scale 10,570 failed. These failed
cells remain part of the archive through `run_error.json`, `run_status.json`
and the available partial evidence; they were not dropped from aggregation.

## Evidence layout

- `ramp_run.json`: authoritative status for all 54 cells.
- `ramp_aggregate.json` and [规模聚合表](#scale-aggregate): all passed-repetition values,
  means and sample-CV summaries.
- `scale_*/<arm>_rep*/identity.json`: comparison role and scheduler owner.
- `gpu_resource.csv`, `ttft_metrics.json` and `vllm_gauges.json`: GPU/energy,
  TTFT/ITL/service counters and during-cell running/waiting/KV observations.
- `gate_config.json`, `resolved_config.json`, `commands.json`, `gate.json`,
  `service_counters.json`, shard `summary.json` and
  `manifest_metadata.json`: executable contract, correctness gate and compact
  per-shard evidence.

The aggregate is reproducible from the archived files with
`code/scripts/analysis/multicard_ramp_aggregate.py`; a 2026-08-12 clean rebuild
matched both committed aggregate files byte for byte.

上述复算描述对应2026-08-12的JSON与独立Markdown输出；本轮将表格并入本报告，复算脚本仍可生成独立表，当前数字来源继续是JSON与原始观测。

## Archive policy and conclusion boundary

The server audit intentionally excluded `requests.csv` and shard logs. They
accounted for most of the remaining volume, may contain generated text, and
are unnecessary for rebuilding the aggregate or checking cell status,
identity, service pressure and resource metrics. The full server directories
remain untouched.

This is a two-path scale/capacity diagnostic, not a native multi-endpoint
product ranking and not evidence that a project scheduler wins. Bounded HTTP
is a direct-client control; the DuckDB arm is explicitly
`harness_pre_split_diagnostic`. Request-granularity and query-barrier timing
must remain separate. The cross-path interpretation and the four-path metric
table are maintained in
`../multicard_proj_scale_ramp_formal_20260807/README.md` section 9.

本节合并原独立补充报告，数值、全部重复、失败说明与结论按原记录保留。

<a id="scale-aggregate"></a>
<a id="scale-aggregate-多卡-ramp-聚合规模-6410570mean-across-passed-reps"></a>
## 规模聚合表
| scale | arm | conc | status | tok/s mean | tok/s CV | rows/s | TTFT P50 | E2E P50 | prefix-hit | GPU0 util | GPU1 util | n_passed/n |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 64 | bounded_http | c32 | passed | 48154.7667 | 18.21% | 224.17 | 67.0ms | 0.1587 | 0.95 | 28.7 | 0.0 | 3/3 |
| 64 | duckdb_ai | c32 | passed | 18430.3333 | 11.05% | 85.7933 | 69.4ms | — | 0.95 | 25.2 | 25.0 | 3/3 |
| 128 | bounded_http | c32 | passed | 43516.6333 | 7.85% | 244.3433 | 62.3ms | 0.2303 | 0.94 | 25.7 | 22.0 | 3/3 |
| 128 | duckdb_ai | c32 | passed | 21864.4667 | 1.43% | 122.79 | 66.7ms | — | 0.94 | 25.0 | 25.0 | 3/3 |
| 256 | bounded_http | c32 | passed | 86843.6333 | 1.43% | 391.0333 | 58.3ms | 0.3637 | 0.96 | 25.0 | 25.0 | 3/3 |
| 256 | duckdb_ai | c32 | passed | 47281.1333 | 1.17% | 212.88 | 57.4ms | — | 0.96 | 43.4 | 35.2 | 3/3 |
| 512 | bounded_http | c32 | passed | 90091.5333 | 1.78% | 412.8 | 57.2ms | 0.6417 | 0.96 | 60.0 | 60.0 | 3/3 |
| 512 | duckdb_ai | c32 | passed | 60399.2667 | 3.78% | 276.7467 | 54.4ms | — | 0.96 | 50.0 | 50.0 | 3/3 |
| 1024 | bounded_http | c32 | passed | 81914.5 | 2.57% | 361.2167 | 60.9ms | 1.2307 | 0.96 | 66.7 | 77.1 | 3/3 |
| 1024 | duckdb_ai | c32 | passed | 69108.6333 | 4.09% | 304.73 | 53.0ms | — | 0.96 | 54.5 | 56.3 | 3/3 |
| 2048 | bounded_http | c32 | passed | 88623.1 | 0.53% | 418.8633 | 55.8ms | 2.3753 | 0.96 | 85.7 | 85.7 | 3/3 |
| 2048 | duckdb_ai | c32 | passed | 75254.5333 | 3.59% | 355.6767 | 51.9ms | — | 0.96 | 66.9 | 70.6 | 3/3 |
| 4096 | bounded_http | c32 | passed | 42467.6 | 0.22% | 194.39 | 159.4ms | 9.519 | 0.63 | 94.1 | 94.7 | 3/3 |
| 4096 | duckdb_ai | c32 | passed | 40649.7 | 1.0% | 186.07 | 151.3ms | — | 0.63 | 86.7 | 86.7 | 3/3 |
| 8192 | bounded_http | c32 | passed | 42937.0333 | 0.75% | 178.01 | 162.7ms | 20.931 | 0.62 | 96.4 | 95.8 | 3/3 |
| 8192 | duckdb_ai | c32 | partial | 41108.6 | 0.0% | 170.43 | 157.0ms | — | 0.62 | 90.4 | 90.6 | 1/3 |
| 10570 | bounded_http | c32 | passed | 41975.4 | 0.18% | 174.5233 | 163.6ms | 28.746 | 0.61 | 96.3 | 96.9 | 3/3 |
| 10570 | duckdb_ai | c32 | failed | — | —% | — | — | — | — | — | — | 0/3 |

<a id="scale-aggregate-效率与尾延迟75d-补齐mfu01-分数非-vllm-estimated_flops-保守估计"></a>
### 效率与尾延迟（§7.5D 补齐；MFU=[0,1] 分数，非 %；vLLM estimated_flops 保守估计）

| scale | arm | conc | MFU(frac) | ITL p95 | ITL p99 | TTFT p99 | decode | prefill | J/1k-tok |
|---|---|---|---|---|---|---|---|---|---|
| 64 | bounded_http | c32 | 0.129 | 24.3ms | 24.9ms | 89.2ms | 49.7ms | 38ms | 5.7267 |
| 64 | duckdb_ai | c32 | 0.049 | 32.4ms | 36.5ms | 98.9ms | 50.2ms | 39.4ms | 14.2533 |
| 128 | bounded_http | c32 | 0.140 | 24.3ms | 25.5ms | 90.6ms | 53.5ms | 38.2ms | 8.67 |
| 128 | duckdb_ai | c32 | 0.070 | 25.6ms | 35.1ms | 98.5ms | 53.4ms | 39ms | 14.1267 |
| 256 | bounded_http | c32 | 0.209 | 24.4ms | 28ms | 96.7ms | 58.4ms | 38.5ms | 5.39 |
| 256 | duckdb_ai | c32 | 0.114 | 24.6ms | 38.8ms | 94.2ms | 59.9ms | 39.1ms | 8.2767 |
| 512 | bounded_http | c32 | 0.212 | 24.3ms | 24.9ms | 90.6ms | 58.4ms | 38.6ms | 6.4767 |
| 512 | duckdb_ai | c32 | 0.142 | 24.3ms | 27.5ms | 89.7ms | 59.6ms | 39ms | 8.11 |
| 1024 | bounded_http | c32 | 0.194 | 24.3ms | 25.2ms | 114.7ms | 69.4ms | 38.5ms | 8.0733 |
| 1024 | duckdb_ai | c32 | 0.164 | 24.3ms | 24.9ms | 82.8ms | 70.8ms | 39.1ms | 8.49 |
| 2048 | bounded_http | c32 | 0.230 | 24.3ms | 24.9ms | 82.3ms | 75.9ms | 38.6ms | 8.3667 |
| 2048 | duckdb_ai | c32 | 0.195 | 24.3ms | 24.9ms | 81.6ms | 77.1ms | 39ms | 8.6 |
| 4096 | bounded_http | c32 | 0.652 | 91.3ms | 135.1ms | 301.5ms | 170.3ms | 92.7ms | 19.69 |
| 4096 | duckdb_ai | c32 | 0.624 | 76ms | 103.3ms | 269.4ms | 174.8ms | 90.5ms | 19.5733 |
| 8192 | bounded_http | c32 | 0.676 | 98ms | 142.4ms | 399.3ms | 196.9ms | 98.5ms | 19.82 |
| 8192 | duckdb_ai | c32 | 0.647 | 88.9ms | 131ms | 323.2ms | 201.3ms | 97.2ms | 19.81 |
| 10570 | bounded_http | c32 | 0.678 | 95.9ms | 141.7ms | 387.9ms | 205.2ms | 98.4ms | 20.3133 |
| 10570 | duckdb_ai | c32 | — | — | — | — | — | — | — |
