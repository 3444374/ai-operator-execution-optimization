# Motivation Directory

本目录回答一个核心问题：

> 在数据库 AI 负载 触发后，数据执行与存储链路是否产生足够严重、可分解、可优化的损耗，从而说明这个课题值得做。

当前主动机应来自生产式 GPU-backed E2E profile：数据库触发 AI workload，数据进入 Daft/Arrow、Ray task/actor、GPU-backed 模型服务和 PostgreSQL / pgvector / Lance sink。CPU/fake 结果只作为历史预研、脚本调试和谨慎消融，不能直接外推为真实 GPU 链路瓶颈。

本目录不承担完整研究实验规划。后续真正围绕三项研究内容做优化、消融、小改动调优和结果记录时，使用根目录 `experiments/`。

进入本目录前先读 `AGENTS.md`。正式结果优先读 `results/README.md`。

## 目录分工

| 路径 | 作用 |
|---|---|
| `plans/` | 动机测试相关的场景、路线和实验设计 |
| `benchmarks/` | 动机实验脚本 |
| `results/gpu/` | 真实 GPU-backed E2E 主动机结果，当前优先级最高 |
| `results/pg18_4_fake/` | PG18.4 本地同构 fake-model 历史结果 |
| `results/fake_cpu/` | fake/CPU 历史预研 CSV 和综合分析 |

## 阅读顺序

1. `plans/integration.md`：看真实 AI-SQL-compatible 算子、worker、GPU 模型服务和写回链路怎么组织，用于建立动机画像。
2. `plans/workloads.md`：看 `AI_EMBED`、`AI_FILTER/AI_CLASSIFY`、`AI_COMPLETE` 三类 baseline 和后续 GPU 动机实验计划。
3. `results/gpu/README.md`：真实 GPU-backed 主动机结果入口，优先读 pgai 集成后的关键复测和 pgvector(384) 写回对比。
4. `results/pg18_4_fake/system_profile.md`：理解 PG18.4 本地同构 fake-model 链路中的早期系统信号。
5. `results/fake_cpu/analysis.md`：只在追溯历史 fake/CPU 预研时阅读。

## 文件索引

### plans

| 文件 | 作用 |
|---|---|
| `plans/integration.md` | PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成计划 |
| `plans/workloads.md` | 三类 AI 算子 workload、动机测试和可优化点 |
| `plans/ai_sql_surface.md` | 数据库 AI 算子场景、业务动机和测试标准 |

### benchmarks

| 文件 | 作用 |
|---|---|
| `benchmarks/fake_embed_pipeline.py` | fake `AI_EMBED(text)` 历史预研脚本 |
| `benchmarks/workload_matrix.py` | 三类 AI 算子场景对比脚本 |
| `benchmarks/granularity.py` | task/object/fan-in/operator invocation 归因脚本 |
| `benchmarks/backpressure.py` | 模型服务 queue wait / token backlog / in-flight 反压模拟脚本 |

这些脚本默认输出到 `results/fake_cpu/`。如果用于新 GPU 链路，只能作为计时框架参考，不能复用旧结论。

## 结果放置规则

- GPU-backed E2E 主动机结果放 `results/gpu/`。
- PG18.4 本地 fake-model 画像放 `results/pg18_4_fake/`。
- fake/CPU 历史 fake/CPU 预研放 `results/fake_cpu/`。
- 只证明环境能连接、脚本能 dry-run、数据库能读写的结果放 `feasibility/results/`。

## 常用命令

```bash
python motivation/benchmarks/fake_embed_pipeline.py \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --embedding-dim 128 \
  --repeats 3 \
  --output motivation/results/fake_cpu/fake_embed_pipeline.csv
```

## 更新要求

完成本目录下的实验、分析或脚本修改后，检查：

- `README.md` 是否需要更新入口、命令或文件索引；
- `results/README.md` 是否需要登记新结果；
- 根 `README.md`、`PROJECT_INDEX.md` 是否需要同步阅读路径；
- `learning/experiment_walkthrough.md` 是否需要补初学者讲解。
- 如果实验已经从“证明值得做”进入“验证方法是否有效”，需要迁入或同步到 `experiments/`。
