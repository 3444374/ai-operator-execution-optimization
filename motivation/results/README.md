# Motivation Results

本目录保存数据库驱动 AI workload 的端到端动机测试、系统画像、瓶颈定位和可优化点分析。

## 结果分层

| 路径 | 地位 |
|---|---|
| `gpu/` | 真实 GPU-backed E2E 主动机结果，优先级最高 |
| `pg18_4_fake/` | PG18.4 本地同构 fake-model 历史结果 |
| `fake_cpu/` | fake/CPU 历史预研结果 |

只证明环境连通性的结果不放这里，放 `feasibility/results/`。

## 阅读顺序

1. `gpu/ai_embed_chain_breakdown_20260712.md`：真实 GPU-backed embedding 链路拆分结果，当前开题动机优先引用。
2. `gpu/multi_endpoint_ray_motivation_20260712.md`：两个本地 GPU endpoint 下 Ray 价值的初步动机测试。
3. `gpu/pgai_integrated_key_rerun_20260714.md`：pgai trigger-surface 验证后的 GPU-backed 关键复测。
4. `gpu/pgvector_writeback_20260714.md`：同一 GPU-backed Ray actor 链路中的 no writeback / JSON text / pgvector(384) sink 对比。
5. `gpu/ai_embed_profile.md`：第一组 GPU-backed 真实 embedding 端到端画像。
6. `cpu/cpu_vs_gpu_embed_comparison_20260712.md`：真实 embedding CPU/GPU 对比和时间边界解释。
7. `gpu/README.md`：GPU-backed 主动机结果入口。
6. `pg18_4_fake/pgvector_scaling.md`：看规模变化后 Ray 与 writeback 的瓶颈迁移。
7. `pg18_4_fake/vector_writeback.md`：确认 pgvector 批量写回与单行 upsert 的差异。
8. `pg18_4_fake/baseline_matrix.md`：比较 Python、Ray task、Ray actor 与 batch/worker。
9. `pg18_4_fake/system_profile.md`：理解 PG18.4 本地同构链路的初始系统画像。
10. `pg18_4_fake/simulated_embed_test_20260712.md`：查看 2026-07-12 在 GPU endpoint 缺失时补跑的本地模拟 embedding 测试结果。
11. `fake_cpu/analysis.md`：回看历史 fake/CPU 动机测试。

## 下一组主结果

当前 384 维 pgvector writeback 对比已经完成：

```text
gpu/pgvector_writeback_20260714.md
gpu/ai_embed_pgvector_writeback_20260714.csv
```

下一组主动机结果应优先补：

```text
gpu/multi_endpoint_or_ray_serve_profile.md
```

之后再补同一计时框架下的三类 baseline：

```text
gpu/ai_embed_profile.md
gpu/ai_filter_classify_profile.md
gpu/ai_complete_profile.md
```

三类实验都应尽量复用同一套阶段计时：DB trigger/fetch、Arrow/RecordBatch build、Daft/Arrow batch、Ray task/actor、queue wait、GPU model service、fan-in/consolidation、writeback。

## 结论边界

- `gpu/` 才能支撑真实 GPU-backed 链路的主动机和瓶颈归因。
- `pg18_4_fake/` 只能支撑 PG18.4 本地同构 fake-model 历史信号。
- `fake_cpu/` 只能支撑历史预研、脚本调试和机制假设，不能替代 GPU-backed 主动机。
- CPU/fake 阶段瓶颈不能直接写成 GPU-backed 链路瓶颈。

连接验证结果位于：

```text
feasibility/results/pg18_4_connection_validation.md
```


## 2026-07-14 GPU rerun after pgai integration

```text
gpu/pgai_integrated_key_rerun_20260714.md
gpu/ai_embed_pgai_integrated_key_20260714.csv
```

Read this before older GPU files when checking the latest local rerun. It
revalidates key GPU-backed timing signals after the pgai SQL trigger surface was
integrated, while keeping pgai SQL performance claims out of the GPU result.

## 2026-07-14 pgvector(384) writeback comparison

```text
gpu/pgvector_writeback_20260714.md
gpu/ai_embed_pgvector_writeback_20260714.csv
```

This result compares no writeback, JSON text writeback, and pgvector
`vector(384)` writeback in the same local GPU-backed Ray actor chain. It should
be used when discussing sink/writeback cost instead of treating JSON text as a
proxy for pgvector.
