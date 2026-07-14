# 项目总纲

生成日期：2026-07-13

## 1. 课题定位

当前课题收敛为：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

数据库 AI 算子在本项目中主要作为 workload 入口和验证场景；研究主体不是数据库内核 UDF、传统数据库 GPU 查询算子或模型 kernel，而是数据从数据库进入 Daft/Arrow 数据组织、Ray task/actor 执行、GPU-backed 模型服务，再到 Lance / pgvector / PostgreSQL sink 的端到端执行与存储协同过程。

候选技术切入点是：

> 面向数据库驱动 AI workload 的特征感知数据组织、并行执行与存储协同优化。

Daft/Ray/Lance 是候选系统机制和可控验证平台，不是论文题目本身，也不是简单产品集成目标。

## 2. 目标链路

```text
PostgreSQL / documents table / SQL workflow
  -> Arrow RecordBatch / Daft batch / partition
  -> Ray task / actor / actor pool
  -> GPU-backed AI_EMBED / AI_FILTER / AI_COMPLETE model service
  -> object transfer / queue wait / fan-in / backpressure
  -> Lance / pgvector / PostgreSQL sink
```

当前本地 PostgreSQL 18.4 + pgvector 只作为 PostgreSQL 18.3 内部平台的同构预演环境。正式结论不能把 PG18.4 本地结果写成 PG18.3 内部平台结果。

## 3. 核心问题

论文必须回答：

> 数据库驱动 AI workload 的端到端执行成本如何拆分？哪些阶段产生可优化损耗？优化数据组织、并行执行、模型服务状态感知和写回协同后，收益能否被测量和解释？

当前最有价值的问题是：

> batch、partition、task/actor、object、endpoint routing、queue wait、fan-in、backpressure 和 writeback 等变量在真实 GPU-backed AI 数据执行链路中如何共同影响端到端性能，是否可以通过 workload 特征感知策略降低系统损耗？

## 4. 当前证据

当前证据分层如下：

- `motivation/results/gpu/`：真实 GPU-backed E2E 主动机结果，优先级最高。
- `motivation/results/pg18_4_fake/`：PG18.4 本地同构 fake-model 历史结果，只作为预演和历史信号。
- `motivation/results/fake_cpu/`：fake/CPU 历史预研结果，只用于解释早期为什么关注 task/object/fan-in/backpressure 等变量。
- `feasibility/results/`：组件级 benchmark、环境验证和连接验证，不能替代端到端性能结论。

当前不能把 CPU/fake 阶段瓶颈直接写成 GPU-backed 链路瓶颈，也不能把连接验证写成性能收益。

## 5. 论文成立条件

一个方向要作为硕士论文，至少需要满足：

1. 有真实或可解释的数据库驱动 AI workload 场景；
2. 有生产式 GPU-backed 端到端系统画像；
3. 有清晰、可拆分的阶段瓶颈；
4. 有可控制的实现范围；
5. 有可复现实验和明确 baseline；
6. 有消融实验支撑优化贡献；
7. 有可解释的性能收益或边界结论；
8. 不是已有系统功能的简单复现。

## 6. 近期工作重点

1. 补充 384 维 pgvector / Lance / PostgreSQL 写回对比，确认真实写回路径的成本边界。
2. 在同一条 GPU-backed 链路上补多 endpoint、Ray Serve / vLLM 或轻量模型服务队列实验。
3. 做大块消融：Python batched worker、Ray task、Ray actor、主控 fan-in 写回、多 worker 写回、bounded / unbounded in-flight。
4. 将 `AI_EMBED`、`AI_FILTER/AI_CLASSIFY`、`AI_COMPLETE` 作为三类 workload，在同一套阶段计时框架下逐步验证。
5. 将稳定代码迁入 `code/`，形成可复现实验工程。
6. 正式图资产统一维护在 `figures/`，报告、PPT、learning、中期汇报和论文共用同一套图。
