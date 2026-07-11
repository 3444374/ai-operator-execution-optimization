# Motivation Results

本目录保存数据库 AI 算子动机测试、端到端系统画像、瓶颈定位和可优化点分析结果。

规则：

- 只证明环境能连接的结果不放这里，放 `validation/results/`。
- 能回答“为什么这个问题值得做”“瓶颈在哪里”“可优化点是什么”的结果放这里。

## 文件

| 文件 | 内容 |
|---|---|
| `fake_ai_embed_pipeline.csv` | fake `AI_EMBED(text)` 端到端动机测试结果 |
| `ai_operator_scenario_motivation.csv` | embedding / classify-filter / offline LLM 三类 AI 算子场景对比结果 |
| `ai_operator_granularity_attribution.csv` | task/object/fan-in/operator invocation 收益来源拆分结果 |
| `ai_operator_backpressure.csv` | 模型服务 queue wait / token backlog / backpressure 模拟结果 |
| `pg18_4_system_profile_fake_ai_embed.csv` | PG18.4 真实数据库触发链路上的 fake AI_EMBED 系统画像 CSV |
| `pg18_4_system_profile_fake_ai_embed.md` | PG18.4 系统画像、瓶颈定位和可优化点分析 |
| `pg18_4_baseline_matrix.csv` | PG18.4 executor × strategy baseline 原始结果 |
| `pg18_4_actor_batch_workers.csv` | PG18.4 Ray actor batch size × worker 数原始结果 |
| `pg18_4_baseline_matrix.md` | PG18.4 baseline 矩阵、actor batch/worker 矩阵与下一步分析 |
| `pg18_4_vector_writeback.csv` | PG18.4 JSON TEXT 与 pgvector vector(128) 写回对照原始结果 |
| `pg18_4_vector_writeback.md` | PG18.4 pgvector vector(128) 写回对照实验分析 |
| `pg18_4_pgvector_scaling.csv` | PG18.4 pgvector 批量写回下 total_rows scaling 原始结果 |
| `pg18_4_pgvector_scaling_fine_contrast.csv` | PG18.4 pgvector 批量写回下少量 fine 对照结果 |
| `pg18_4_pgvector_scaling.md` | PG18.4 pgvector 批量写回 scaling 实验分析 |
| `motivation_test_results_analysis.md` | 动机测试综合分析 |

连接验证结果位于：

```text
validation/results/pg18_4_connection_validation.md
```
