# Motivation Results

本目录保存数据库 AI 算子的端到端动机测试、系统画像、瓶颈定位和可优化点分析。

## 结果分层

| 路径 | 地位 |
|---|---|
| `gpu/` | 后续生产式 GPU-backed E2E 主动机结果，优先级最高 |
| `pg18_4_fake/` | PG18.4 本地同构 fake-model 历史结果 |
| `fake_cpu/` | fake/CPU 历史预研结果 |

只证明环境连通性的结果不放这里，放 `feasibility/results/`。

## 阅读顺序

1. `gpu/README.md`：GPU-backed 主动机结果入口，目前待补。
2. `pg18_4_fake/pgvector_scaling.md`：看规模变化后 Ray 与 writeback 的瓶颈迁移。
3. `pg18_4_fake/vector_writeback.md`：确认 pgvector 批量写回与单行 upsert 的差异。
4. `pg18_4_fake/baseline_matrix.md`：比较 Python、Ray task、Ray actor 与 batch/worker。
5. `pg18_4_fake/system_profile.md`：理解 PG18.4 本地同构链路的初始系统画像。
6. `fake_cpu/analysis.md`：回看历史 fake/CPU 动机测试。

## 下一组主结果

下一组主动机结果应优先补：

```text
gpu/ai_embed_profile.md
gpu/ai_embed_profile.csv
```

之后再补同一计时框架下的三类 baseline：

```text
gpu/ai_embed_profile.md
gpu/ai_filter_classify_profile.md
gpu/ai_complete_profile.md
```

三类实验都应尽量复用同一套阶段计时：DB trigger/fetch、Arrow/RecordBatch build、external service submit、Ray task/actor、queue wait、GPU model service、fan-in/consolidation、writeback。

## 结论边界

- `gpu/` 才能支撑真实 GPU-backed 链路的主动机和瓶颈归因。
- `pg18_4_fake/` 只能支撑 PG18.4 本地同构 fake-model 历史信号。
- `fake_cpu/` 只能支撑历史预研、脚本调试和机制假设，不能替代 GPU-backed 主动机。
- CPU/fake 阶段瓶颈不能直接写成 GPU-backed 链路瓶颈。

连接验证结果位于：

```text
feasibility/results/pg18_4_connection_validation.md
```

