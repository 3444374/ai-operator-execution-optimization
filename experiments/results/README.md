# Research Experiment Results

## Output-aware Packing (2026-07-26)

### Current mechanism decision

| Directory | Content | Boundary |
|---|---|---|
| `row_cap_aware_packing_512_20260726/` | Prefix-cache-audited 512-row row-cap/token-budget/packing screening plus three-repeat confirmation. | Row-cap-first has a small 512-row signal; cache-enabled exploratory runs are retained only as invalid-ordering audit evidence. |
| `row_cap_aware_packing_1024_20260726/` | Held-out 1024-row sequential/classic-BFD/row-cap-first comparison with request SLO, energy, and MFU. | Negative default-adoption result: about 1% throughput gain caused SLO violation to rise from 50.39% to 88.67%; sequential remains default. |

### Earlier gates and superseded runs

| Directory | Content | Boundary |
|---|---|---|
| `row_cap_aware_packing_gate_20260726/` | 64-row real PostgreSQL→Daft→Ray→vLLM gate for sequential, classic BFD, and BFD-inspired row-cap-first placement. | Infrastructure validation only; 6/6 runs and all request/resource/MFU invariants pass, but one formal repeat cannot support performance ranking. |
| `output_aware_bfd_gate_v2_20260726/` | 64-row real-component gate for output-cost modes, sequential/BFD packing, request/resource traces, power, energy, and MFU. | Infrastructure validation only; all token-budget policies share token and row caps. |
| `output_aware_bfd_512_v2_20260726/` | Six-cell 512-row sequential/BFD × output-cost comparison, with 18 formal runs and plot-ready summaries. | BFD trace is a positive candidate at 512 rows, but n=3 and trace metadata is not a paired output oracle. |
| `output_aware_bfd_1024_20260726/` | Held-out 1024-row confirmation against same-cost sequential and strongest practical baseline. | Negative scale confirmation: current BFD does not generalize; row-cap-aware joint tuning is required. |
| `output_aware_bfd_512_20260726/` | Superseded failed run that exposed inconsistent sequential/BFD row caps and a timeout incident. | Audit evidence only; excluded from performance conclusions. |

## Local Baselines

| Directory | Content | Boundary |
|---|---|---|
| `adaptive_flush_randomized_20260726/` | Natural-EOS gate, randomized 512-request fixed-25/fixed-50/queue-adaptive repeats, and exact output-token/finish tracing. | Fixed-50 and adaptive both beat fixed-25 by about 32% tokens/s and are indistinguishable; fixed-50 is the simplest current candidate. |
| `joint_batching_submission_512_20260726/` | Real 18-cell token-budget × K_max × flush screen plus randomized repeated validation of independent splice, joint candidate, and fixed-50 mechanism control. | Under the 1% SLO gate, independent splice and joint search are indistinguishable; fixed-50 is the simplest current workload-specific candidate. |
| `local_vllm_qwen15b_baseline/` | Local `AI_COMPLETE` baseline for `PostgreSQL -> Daft -> Ray -> vLLM Qwen2.5-1.5B`, including synthetic smoke, ShareGPT/BurstGPT fixed row-batch sweep CSVs, and a latency metric probe. | Local PG rehearsal, fixed row-batch baseline only; not a token-aware scheduling result and not a PostgreSQL 18.3 internal-platform result. |
| `accelerated_arrival_flush_20260725/` | Real single-GPU accelerated-arrival comparison of immediate, fixed-timeout, and queue-adaptive flush, with run, submission, flush, and resource traces. | Controlled accelerated replay on one RTX 5070. Fixed timeout reduced submissions but did not yield a statistically separable throughput gain; the current queue-adaptive rule formed no multi-row batches. |
| `adaptive_flush_window_20260725/` | Corrected dual-window adaptive flush gates, 1024-row probe, and 512-row repeated comparison with plot-ready traces. | Positive single-GPU candidate evidence under accelerated replay; fixed policy-group order, fixed 16-token output cap, and missing per-request E2E tails still require follow-up. |
| `request_lifecycle_gate_20260725/` | Real 64-prompt PostgreSQL→Daft→Arrow→Ray→vLLM gate for request lifecycle, seeded runner, SLO fields, and explicit request→submission identity. | Infrastructure validation only: one run per strategy, fixed then adaptive; not policy performance evidence. |

本目录保存正式研究实验结果和小范围优化测试记录。

## 当前状态

正式优化实验已经开始；本目录保存方法实验。早期 GPU-backed 画像和动机实验仍位于：

```text
motivation/results/gpu/
motivation/results/pg18_4_fake/
motivation/results/fake_cpu/
```

当后续开始验证三项研究内容中的方法或调优策略时，再在本目录登记结果。

## 结果命名建议

```text
YYYYMMDD_<research_area>_<short_name>.md
YYYYMMDD_<research_area>_<short_name>.csv
```

示例：

```text
20260720_sink_pgvector_writeback.md
20260720_scheduling_bounded_inflight.md
20260720_batching_partition_ablation.md
```

## 记录要求

- 明确对应研究内容。
- 明确 baseline 和优化方案。
- 明确运行命令、参数、CSV 和日志。
- 明确结论边界，不把局部调优写成完整论文贡献。
- 如需图表，放入 `figures/` 并在结果报告中引用。
