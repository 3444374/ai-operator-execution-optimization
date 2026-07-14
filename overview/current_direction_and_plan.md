# 当前方向与计划

生成日期：2026-07-10

## 1. 当前方向

推荐题目：

> 面向数据库内置 AI 算子的分布式数据处理执行链路优化研究。

候选技术副标题：

> 基于 Daft/Ray/Lance 的 Object Transfer、fan-in 与 Shuffle 优化。

候选升级表述：

> 面向数据库 AI 算子的特征感知并行执行与跨层调度方法研究。

该方向比传统数据库内核方向更贴近用户的 AI infra / inference infra 目标，也能通过数据库 AI 算子场景与达梦需求对齐。但具体优化点尚未最终确定，当前需要先围绕 AI 算子场景做场景发现、动机测试、可行性测试，再收敛到最终论文主线。Object/fan-in/coalescing 是已有实验证据支持的入口，不应成为整个课题的全部。

## 2. 不能做成什么

当前不把论文写成：

- 跑通 Daft/Ray/Lance；
- 改造整个 Ray；
- 重写 Ray Core / Ray Data / Ray Serve 的完整调度器；
- 传统数据库 GPU 查询算子优化；
- 单纯模型推理 kernel 优化；
- 单纯 Arrow 序列化优化；
- 没有真实 workload 的 benchmark 堆砌。

## 3. 当前系统链路

```text
PostgreSQL 18.3 documents table / parquet
  -> Arrow RecordBatch
  -> task partitioning / batch construction
  -> Ray tasks / actors / object store
  -> CPU preprocess / tokenizer
  -> GPU or model-service AI operator
  -> routing / batching / backpressure
  -> fan-in / shuffle / writeback
  -> pgvector / Lance / output table
```

## 4. 当前证据判断

已完成的可行性验证结果位于 `validation/results/`。

关键结论：

- Ray small task：暂不支持优先做 scheduler/runtime。
- Arrow serialization：不是当前主瓶颈。
- Ray many-object fan-in：object 数量增加会放大下游 fan-in。
- Ray Arrow fan-out/fan-in：fine/coalesced 平均 fan-in 比约 `3.17x`。
- fake `AI_EMBED(text)` 端到端链路：fine/coalesced 平均 fan-in 比约 `2.16x`，端到端耗时比约 `2.51x`。

阶段性判断：

> 当前最值得继续验证的候选方向是数据库 AI 算子外部执行链路中的特征感知任务划分、并行度控制和跨层调度，而不是 runtime 重写。但现有证据仍偏 fake workload，只能证明 object/task 粒度敏感；是否能上升到资源调度、模型服务路由和 backpressure，需要新增实验验证。

真实形态验证判断：

> 下一步不能只继续扩展 fake benchmark。必须在公司内部统一采用的 PostgreSQL 18.3 平台上，或在本地低端设备上预演同构链路，真实跑通数据库表/SQL 触发、AI 算子外部执行、Ray/Arrow 中间链路、模型服务或 fake/local 模型、fan-in/writeback 和指标采集。只有该链路中的画像数据才能回答真实数据库 AI 算子外部执行链路到底卡在哪里。

## 5. 当前端到端动机实验

已搭建 fake `AI_EMBED(text)` 端到端动机测试。

当前链路：

```text
generate documents
  -> build Arrow RecordBatch
  -> Ray fake embedding
  -> fine vs coalesced object
  -> fan-in
  -> write output / CSV
```

必须记录：

- rows/s；
- object_count；
- average object size；
- `ray.put` time；
- fan-in time；
- fake embedding time；
- end-to-end time。

当前结果文件：

- `motivation/fake_ai_embed_pipeline_benchmark.py`
- `validation/results/fake_ai_embed_pipeline.csv`
- `validation/results/fake_ai_embed_outputs/`

下一步应继续拆分 coalescing 收益来源：区分 object 数减少、Ray task 数减少、`ray.put` 次数减少、fan-in 依赖数减少和写回阶段变化。同时增加跨层调度实验轴：task vs actor、batch_size × concurrency、CPU preprocess worker 与 GPU/model actor 配比、模型服务队列长度、token backlog、backpressure。场景语义上，offline LLM 需要 token-aware / prefix-aware workload，AI_FILTER 需要 selectivity-aware workload，embedding/RAG 需要真实数据库写回 baseline。

## 6. 后续阶段

如果 AI 算子场景和系统瓶颈继续成立：

1. 在低端设备上先搭 PostgreSQL 18.3 同构预演链路，必要时用普通 PostgreSQL + pgvector 作为接口替身；
2. 把已有或开源 AI 算子迁移/等价封装为 PostgreSQL 18.3 可触发的 `AI_EMBED(text)` / batch function / 外部执行入口；
3. 外部 worker 读取 documents 表，并转换为 Arrow RecordBatch；
4. 通过 Ray task/actor 批量执行 fake、本地小模型或真实 embedding service；
5. 记录 batch、task、ObjectRef、operator invocation、fan-in refs、queue wait、writeback 等指标；
6. 写回 embeddings 表、pgvector/Lance 或普通 output table，并验证 vector search / downstream query 可用。

如果后续真实形态实验不再显示 object/fan-in 瓶颈，应回到 AI 算子链路重新定位瓶颈，不继续强行做 coalescing。

## 7. 四周计划

第 1 周：完成 fake `AI_EMBED(text)` pipeline、CSV 分析、收益来源拆分，并列出 2-3 个候选 AI 算子场景。

第 2 周：修正候选场景动机测试：拆分 task/object/put/fan-in 收益来源；增加 task/actor/concurrency 对比；为 offline LLM 增加 token-aware / prefix-aware batching；为 AI_FILTER 增加 selectivity / cascade。

第 3 周：围绕 PostgreSQL 18.3 内部验证平台设计并预演真实画像实验。低端设备阶段先用小规模 documents 表、小模型或 fake/local embedding 跑通 SQL/表触发、外部 worker、Ray/Arrow、AI 算子、fan-in/writeback；如果条件允许，再接本地小模型、vLLM 或 Ray Serve endpoint，并记录模型服务队列、token backlog、actor 利用率和 backpressure 指标。

第 4 周：用 idea-evaluator 视角做 fatal-flaws audit，整理开题材料、实验设计、baseline、反证条件和论文贡献边界。当前不要把单个场景 C 写成唯一主线，也不要在没有跨层实验数据前把题目写成完整调度系统优化。
