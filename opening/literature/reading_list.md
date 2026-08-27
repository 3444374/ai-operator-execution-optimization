# 文献精读清单

更新日期：2026-08-27

## 选择原则

- Top 15 优先选择 CCF-A 正式 research paper。
- Companion、Demo、Tutorial、CIDR、MLSys、arXiv 单独标注，不借所属会议/期刊抬高等级。
- 先核验作者、标题、年份、轨道、卷期和 DOI，再进入 Top 15。
- 精读必须覆盖问题、方法、baseline/workload/指标、假设与局限，以及与本项目的可迁移关系。
- 不根据摘要直接调整 Top 15。

## 当前统计

- `research/reading_notes/` 现有 **49 篇历史文献笔记**，从 2026-08-21 起按泛读库管理（不含 README 和泛读模板）。
- 新的全文精读以 `research/精读文献笔记/` 为唯一权威来源；当前包含原十五篇开题主线笔记与已从下载目录原件逐字节恢复的 Kalypso，共十六篇主笔记、146 张论文原图裁剪件。Kalypso 按 arXiv 核心补充管理，尚未并入下方十五篇开题主线或已定稿正文；本目录不维护阅读状态。
- 旧文档“33 篇已完成”已经过时；原编号到 41 还混入了两篇未下载条目，也不能作为实体笔记数。
- 当前 Top 15 为 15/15 严格 CCF-A 正式论文，快照在 `top15_reading_notes/`。
- `research/reference/` 当前工作区有 6 份可解析 PDF 实体（Galois、Abacus、Palimpzest、Sema、Parrot、Kalypso）；其余 Top 15 的历史题录保留在索引中，使用前需恢复并核验文件。

## Top 15 精读顺序

| 类别 | 论文 | 笔记 |
|---|---|---|
| AI 算子 | LOTUS | `lotus_pvldb2025.md` |
| AI 算子 | Galois | `galois_sigmod2025.md` |
| AI 算子 | GaussML | `gaussml_icde2024.md` |
| Serving | vLLM | `vllm_sosp2023.md` |
| Serving | Orca | `orca_osdi2022.md` |
| Serving | Sarathi-Serve | `sarathi_serve_osdi2024.md` |
| Serving | SGLang | `sglang_neurips2024.md` |
| 公平调度 | VTC | `vtc_osdi2024.md` |
| 动态调度 | Llumnix | `llumnix_osdi2024.md` |
| Serving | DistServe | `distserve_osdi2024.md` |
| Ray | Ray | `ray_osdi2018.md` |
| 代价估计 | How Good Are Learned Cost Models, Really? | `learned_cost_models_sigmod2025.md` |
| 代价估计 | GRACEFUL | `graceful_udf_cost_icde2025.md` |
| 代价估计 | COSTREAM | `costream_icde2024.md` |
| 代价优化 | Abacus | `abacus_pvldb2026.md` |

## 核心补充精读

### 当前开题正文的十五篇精读主线

| 方向 | 论文 | 正文作用 |
|---|---|---|
| 数据库 AI 查询 | Cortex AISQL | AI 代价进入数据库计划选择，说明生产系统需求 |
| 数据库 AI 查询 | LOTUS | 语义算子、质量要求与声明式优化 |
| 数据库 AI 查询 | Palimpzest | 声明式语义计划、物理实现枚举和质量、时间与费用选择 |
| 数据库 AI 代价优化 | Abacus | 逐算子采样、Pareto 计划搜索与决策质量评价 |
| 数据库 AI 执行 | Sema | 数据库计划中的语义算子、组批、融合和自适应执行 |
| 数据库 AI 优化 | Galois | 大语言模型调用与传统 SQL 算子的逻辑和物理优化 |
| 数据库 AI 物理执行 | IMBridge | 分离数据库数据交付批次与模型调用批次，显式化预测算子的初始化和批处理 |
| 数据组织 | Optimizing LLM Queries in Relational Data Analytics Workloads | 行与字段重排、关系统计和前缀缓存复用 |
| 分布式执行 | Ray | 动态任务图与有状态执行单元 |
| 异构流水线 | Ray Data Streaming Batch | 动态分区、内存控制与 CPU/GPU 流水执行 |
| 应用编排 | AYO | 任务单元、阶段依赖和数据流图驱动的批处理 |
| 应用感知模型服务 | Parrot | 保留多次模型调用的变量、依赖、最终目标和共享提示词信息 |
| 多作业调度 | VTC | 在线服务量记账、空闲后重新加入和不依赖预测的基础方法 |
| 公平性与前缀复用 | DLPM/D²LPM | 用服务余额限制最长前缀匹配的连续服务，并在多个模型服务副本间兼顾前缀复用与队列长度 |
| 数据组织与模型服务 | BlendServe | 资源需求均衡与前缀局部性之间的取舍 |

这十五篇是当前开题报告第二章的主要论证来源。正文按各论文与课题问题的直接程度分配篇幅，不强求每篇等长。其余 Top 15 和核心补充文献继续用于补充数据库实现路线、模型服务机制、评价指标和代价估计方法。

### 数据库 AI 系统与 benchmark

| 文献 | 状态 | 作用 |
|---|---|---|
| Palimpzest | CIDR 2025 | 声明式 plan search、系统 baseline |
| Sema | VLDB 2026 Research Track 已录用；当前精读全文为 arXiv:2603.11622v1 | DuckDB 原生 semantic operator、expression optimization 与运行时 AQE；正式卷期页码待发布 |
| SemBench | PVLDB 2026 benchmark | 跨系统 workload、质量/延迟/成本/内存指标 |
| Database Perspective on LLM Inference | PVLDB Tutorial | 技术版图与 open problem |
| Cortex AISQL | 按实际轨道引用 | 工业需求证据 |
| NeurDB | CIDR 2025 | AI-native database vision |
| Kalypso | arXiv:2607.23815v2，2026；未标注正式 venue | query-plan-aware relational LLM serving；直接覆盖跨语义算子流水与 KV-cache memory-aware admission，用于收窄本课题增量边界 |

### 公平与 program/job 调度

| 文献 | 状态 | 作用 |
|---|---|---|
| FairServe | arXiv 2024 | weighted service、interaction throttling |
| DLPM/D2LPM | arXiv 2025 | deficit fairness 与 prefix locality |
| Agentix（arXiv v1 名称 Autellix） | NSDI 2026，2443–2459 | program-level attained service |
| Chiron | arXiv 2025 | 分层 backpressure/autoscaling |
| Clipper | NSDI 2017 | AIMD batching 历史来源 |
| Splitwise | ISCA 2024 | prefill/decode 分池边界 |
| Ray Data Streaming Batch | arXiv 2025 | 官方数据引擎执行模型 |

### 其他已有泛读/历史笔记

```text
bucketserve_2025.md
clipper_nsdi2017.md
colora_2026.md
concerto_cost_estimation_arxiv2024.md
concur_2025.md
cortex_aisql_sigmod2026.md
db_perspective_llm_pvldb2025.md
diskann_neurips2019.md
flashattention_neurips2022.md
flexgen_icml2023.md
inferdb_pvldb2024.md
lance_2025.md
leads_pvldb2024.md
llm4dm_pvldb2024.md
milvus_sigmod2021.md
mooncake_acmtos2025.md
multibin_batching_2024.md
neurdb_cidr2025.md
proserve_2025.md
ray_data_streaming_batch_2025.md
redefining_cost_estimation_arxiv2025.md
saber_2025.md
scorpio_llm_serving_2025.md
serverlessllm_osdi2024.md
sfs_latency_routing_arxiv2026.md
smart_vldb_journal_2025.md
smartlite_pvldb2024.md
splitwise_isca2024.md
```

## 代价估计精读问题

每篇代价估计论文都必须回答：

1. 预测对象是 runtime、cardinality、quality、money cost，还是 plan ranking？
2. 输入特征能否覆盖 prompt/output token、模型、GPU、arrival、concurrency 和 operator type？
3. 如何迁移到新 workload、模型或硬件？
4. 预测误差是否真的改善 plan/config 选择？
5. profile 成本和在线校正成本是多少？

本项目首版仍采用“简单解析模型 + profile 校准 + residual correction”，不因新增四篇成本论文就扩张为 learned optimizer 课题。

## 可直接支撑开题的观点

- vLLM/Orca/Sarathi-Serve 说明下游 continuous batching 和 token/KV 约束；上游优化应先达到 serving capacity ceiling，再比较压力效率、尾延迟和多 job 隔离。
- VTC 说明 token-cost 公平可以 work-conserving 地实现；它不等于单请求 GPU 加速。
- Parrot 说明多次模型调用之间的依赖、共享提示词和最终目标会影响服务端的分组与放置；它没有覆盖数据库中尚未形成请求的记录和上游数据准备过程。
- DLPM/D²LPM 说明公平服务与前缀缓存复用需要共同考虑；其公平性分析依赖模型服务副本内部也采用相应调度，不能直接外推到无法修改内部调度的服务接口。
- LOTUS、Galois、GaussML、Palimpzest、Abacus 说明 AI 算子需要声明式物理实现、质量/成本/延迟的联合选择。
- IMBridge 说明数据库算子的数据交付批次不等于模型合适的调用批次；独立预测算子为每个函数单独组织输入提供了相关工作依据，但其演示结果不能直接证明远程模型服务和多作业调度收益。
- Learned Cost Models、GRACEFUL、COSTREAM 说明代价模型的价值应由下游决策质量验证，而不是只比较误差。
- SemBench 说明数据库 AI baseline 必须同时报告质量、调用数、token work、延迟、成本、内存与失败。

## 不能过度引用

- 不把 CIDR、MLSys、arXiv、Companion、Tutorial 写成 CCF-A research paper。
- 不把 serving 内部调度论文的收益直接归因到本项目上游 Ray 调度。
- 不把减少 LLM 调用数的系统收益与“相同 work 执行更快”混为一谈。
- 不把单 job 饱和 throughput 平台解释为调度无价值；多 job 公平、压力效率与 transient ramp 是不同问题。
- 不把 learned cost model 作为既定实现，除非简单模型在 held-out 决策上确实不足。

## 模板

- 泛读模板：`research/reading_notes/tpl-文献泛读.md`
