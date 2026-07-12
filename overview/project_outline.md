# 项目总纲

生成日期：2026-07-10

## 1. 课题定位

当前课题不聚焦传统 GPU 查询算子，也不做模型 kernel 优化，而是聚焦：

> 面向数据库内置 AI 算子的分布式数据处理执行链路优化研究。

技术切入点：

> 基于 Ray/Daft/Lance 类外部执行链路的数据库 AI 算子批处理系统调优。

数据库场景是落地入口；真正要沉淀的是 AI infra 能力，包括 distributed execution、object store、shuffle、Arrow/RecordBatch、AI data pipeline、批量推理前后处理等。

当前主攻外部执行链路调优，不把“把 AI 算子搬到数据库内核或 GPU kernel 上执行”作为主要工作量。Snowflake Cortex AISQL、pgai vectorizer、pgvector、PostgresML、OceanBase/达梦类分布式数据库作为工业背景、场景依据和 baseline 对照；真正要研究的是数据库 AI 算子触发后，外部 worker / Ray / Daft / 模型服务 / 写回链路如何组织得更高效。

## 2. 目标链路

```text
PostgreSQL / documents table / Parquet
  -> Arrow RecordBatch
  -> Daft / Ray batch execution
  -> AI_EMBED(text) / chunk / preprocess
  -> object transfer / fan-in / shuffle
  -> PostgreSQL + pgvector 或 Lance
```

## 3. 核心问题

论文必须回答：

> 数据库 AI 算子链路中哪里产生系统瓶颈？为什么会产生？优化哪一层？收益能否被测量和解释？

当前最有价值的问题是：

> 数据库 AI 算子外部执行链路对 batch、partition、task/actor、object、fan-in、backpressure 和 writeback 粒度敏感，是否可以通过特征感知任务划分、并行度控制、对象合并、模型服务背压和批量写回降低端到端开销？

## 4. 当前证据

本地 fake/CPU 实验显示：

- Ray small task 不是当前最强瓶颈；
- Arrow IPC 本身不是明显瓶颈；
- fixed-size many-object fan-in 有明显放大；
- Arrow RecordBatch `N upstream -> P downstream` 实验中，fine/coalesced 平均 fan-in 比约 `3.17x`。

因此，当前不建议优先做 scheduler/runtime，也不建议做单纯 Arrow serialization。

## 5. 论文成立条件

一个方向要作为硕士论文，至少需要满足：

1. 有真实或可解释的数据库 AI 算子场景；
2. 有清晰系统瓶颈；
3. 有可控制的实现范围；
4. 有可复现实验；
5. 有明确 baseline；
6. 有可解释的性能收益或边界结论；
7. 不是已有系统功能的简单复现。

## 6. 近期工作重点

1. 搭建 fake `AI_EMBED(text)` 端到端动机测试。
2. 验证 RecordBatch fan-in 现象是否迁移到批量 embedding / RAG 数据准备链路。
3. 若成立，进入 PostgreSQL 18.3 内部验证平台或同构预演链路，真实采集数据库 AI 算子外部执行画像。
4. 优先补外部链路 baseline：Python batched worker、Ray task、Ray actor、主控 fan-in 写回、多 worker 各自写回、Ray Serve / vLLM 或轻量模型服务队列。
5. 将稳定代码迁入 `code/`，形成可复现实验工程。
