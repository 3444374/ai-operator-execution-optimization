# Research Experiment Results

## Local Baselines

| Directory | Content | Boundary |
|---|---|---|
| `local_vllm_qwen15b_baseline/` | Local `AI_COMPLETE` baseline for `PostgreSQL -> Daft -> Ray -> vLLM Qwen2.5-1.5B`, including synthetic smoke, ShareGPT/BurstGPT fixed row-batch sweep CSVs, and a latency metric probe. | Local PG rehearsal, fixed row-batch baseline only; not a token-aware scheduling result and not a PostgreSQL 18.3 internal-platform result. |

本目录保存正式研究实验结果和小范围优化测试记录。

## 当前状态

正式优化实验尚未开始。已有 GPU-backed 画像和动机实验结果仍位于：

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
