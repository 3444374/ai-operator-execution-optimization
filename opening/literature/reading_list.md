# 文献精读清单

## 选择原则

- 优先选择 CCF-A 会议/期刊和权威系统论文。
- 其次选择与 Ray、Daft、Lance、Arrow、Snowflake AISQL、pgai、PostgresML、pgvector 直接相关的官方文档或源码 README。
- 精读文献不只记录标题，要能说明论文解决的问题、核心方法、与本课题的关系和不足。
- 未核验资料只能标注为“待核验”，不能写入正式开题报告的确定性结论。

## 当前核心候选

| 类别 | 文献 / 资料 | 来源级别 | 状态 | 开题中用途 |
|---|---|---|---|---|
| AI SQL 工业系统 | Snowflake Cortex AISQL paper / docs | 论文 + 官方文档 | ✅ 已精读（2026-07-15） | 证明 AI SQL 算子是现实需求 |
| AI SQL 工业系统 | Snowflake Cortex AI Functions docs | 官方文档 | 已整理入口 | 支撑 `AI_EMBED`、`AI_FILTER`、`AI_COMPLETE` 场景 |
| 分布式 AI 执行 | Ray paper | 系统论文 | ✅ 已精读（2026-07-22） | 支撑 Ray 作为分布式 AI 执行框架 |
| Ray Core | Ray objects / task anti-pattern / scheduling docs | 官方文档 | 已整理入口 | 支撑 task、object、ObjectRef、资源调度机制 |
| Ray Data / Serve | `map_batches`、dynamic batching、routing、autoscaling docs | 官方文档 | 已整理入口 | 支撑 batch、routing、backpressure 实验接口 |
| Daft | Daft on Ray、partitioning、shuffle、join strategy docs | 官方文档 | 已整理入口 | 支撑 partition、shuffle、`M × N` object slots 风险 |
| 列式传输 | Arrow Flight benchmark paper | 论文 | 待精读 | 支撑 Arrow/Flight 在跨系统数据传输中的背景 |
| AI 数据存储 | Lance paper | 论文 | 待精读 | 支撑 Lance 作为 AI / columnar 数据存储参考 |
| PostgreSQL AI 生态 | pgai README | 工程资料 | 已整理入口 | 支撑外部 vectorizer worker + 写回形态 |
| PostgreSQL AI 生态 | pgvector README | 工程资料 | 已整理入口 | 支撑 PostgreSQL 向量写回和检索 baseline |
| PostgreSQL AI 生态 | PostgresML README | 工程资料 | 已整理入口 | 作为近数据库 / 数据库内模型执行对照路线 |
| 分布式 SQL 优化 | Spark SQL performance tuning docs | 官方文档 | 已整理入口 | 支撑 partition、shuffle coalescing、join strategy 是成熟问题 |
| 数据库侧工业背景 | OceanBase Mercury / Bacchus 等论文 | 论文 | 待核验细节 | 作为分布式数据库、列式/向量化、写回和资源管理背景 |

## 精读笔记索引（2026-07-23 更新）

**33 篇精读笔记已完成**：原 16 篇（inventory 建议精读 15 篇 + DiskANN）+ 2026-07-23 新增 17 篇（由 codex 与 Claude Code 两环境协同完成：P0 自适应控制 / P1 策略实现 / P2 背景对照 + Mooncake + 补齐 Multi-Bin/Lance/Milvus + Ray Data 引擎基础）。所有笔记位于 `opening/literature/reading_notes/`，已同步至知识库 `raw/papers/`。

| # | 笔记文件 | 论文 | 出处 | 完成日期 |
|---|---------|------|------|----------|
| 1 | `cortex_aisql_sigmod2026.md` | Cortex AISQL | SIGMOD 2026 | 2026-07-15 |
| 2 | `smart_vldb_journal_2025.md` | Smart | VLDB Journal 2025 | 2026-07-15 |
| 3 | `gaussml_icde2024.md` | GaussML | ICDE 2024 | 2026-07-15 |
| 4 | `galois_sigmod2025.md` | Galois | SIGMOD 2025 | 2026-07-22 |
| 5 | `neurdb_cidr2025.md` | NeurDB | CIDR 2025 | 2026-07-22 |
| 6 | `leads_pvldb2024.md` | LEADS | PVLDB 2024 | 2026-07-22 |
| 7 | `inferdb_pvldb2024.md` | InferDB | PVLDB 2024 | 2026-07-22 |
| 8 | `smartlite_pvldb2024.md` | SmartLite | PVLDB 2024 | 2026-07-22 |
| 9 | `vllm_sosp2023.md` | vLLM | SOSP 2023 | 2026-07-22 |
| 10 | `orca_osdi2022.md` | Orca | OSDI 2022 | 2026-07-22 |
| 11 | `sarathi_serve_osdi2024.md` | Sarathi-Serve | OSDI 2024 | 2026-07-22 |
| 12 | `serverlessllm_osdi2024.md` | ServerlessLLM | OSDI 2024 | 2026-07-22 |
| 13 | `llm4dm_pvldb2024.md` | LLM for Data Management | PVLDB 2024 | 2026-07-22 |
| 14 | `db_perspective_llm_pvldb2025.md` | DB Perspective on LLM Inference | PVLDB 2025 | 2026-07-22 |
| 15 | `ray_osdi2018.md` | Ray | OSDI 2018 | 2026-07-22 |
| 16 | `diskann_neurips2019.md` | DiskANN | NeurIPS 2019 | 2026-07-22 |
| — | **2026-07-23 新增（P0 自适应控制 / RC2）** | | | |
| 17 | `clipper_nsdi2017.md` | Clipper — AIMD 自适应 batching | NSDI 2017 | 2026-07-23 |
| 18 | `concur_2025.md` | CONCUR — agent 级主动准入控制（AIMD，非 EWMA） | arXiv 2026 | 2026-07-23 |
| 19 | `colora_2026.md` | CoLoRA — 多租户 LoRA 协同调度 | ASP-DAC 2026 | 2026-07-23 |
| 20 | `saber_2025.md` | SABER — USL 估计 + SLA-aware 准入 | arXiv 2025 | 2026-07-23 |
| — | **2026-07-23 新增（P1 策略设计与工程实现 / RC1+RC2）** | | | |
| 21 | `scorpio_llm_serving_2025.md` | Scorpio — SLO 异构 + token 级延迟建模 | arXiv 2025 | 2026-07-23 |
| 22 | `bucketserve_2025.md` | BucketServe — sequence-length 分桶 batching | arXiv 2025 | 2026-07-23 |
| 23 | `sglang_neurips2024.md` | SGLang — RadixAttention prefix 复用 | NeurIPS 2024 | 2026-07-23 |
| 24 | `splitwise_isca2024.md` | Splitwise — prefill/decode 分池 | ISCA 2024 | 2026-07-23 |
| — | **2026-07-23 新增（P2 背景与对照）** | | | |
| 25 | `proserve_2025.md` | ProServe — 多优先级请求调度 | arXiv 2026 | 2026-07-23 |
| 26 | `distserve_osdi2024.md` | DistServe — prefill/decode disaggregation（goodput） | OSDI 2024 | 2026-07-23 |
| 27 | `flashattention_neurips2022.md` | FlashAttention — IO-aware attention 原语 | NeurIPS 2022 | 2026-07-23 |
| 28 | `flexgen_icml2023.md` | FlexGen — 离线高吞吐 offloading 推理 | ICML 2023 | 2026-07-23 |
| 29 | `mooncake_acmtos2025.md` | Mooncake — KVCache-centric 分离架构 | ACM TOS 2025 | 2026-07-23 |
| — | **2026-07-23 补充（RC1 理论 / 数据引擎 / 写回）** | | | |
| 30 | `multibin_batching_2024.md` | Multi-Bin Batching — length-align 装箱理论（order-statistics） | arXiv 2024 | 2026-07-23 |
| 31 | `lance_2025.md` | Lance — 列式 AI 存储格式（Daft 底层） | arXiv 2025 | 2026-07-23 |
| 32 | `milvus_sigmod2021.md` | Milvus — 向量库写回/索引（pgvector 工程对照） | SIGMOD 2021 | 2026-07-23 |
| — | **2026-07-23 补（引擎基础）** | | | |
| 33 | `ray_data_streaming_batch_2025.md` | Ray Data: Streaming Batch Model — 异构执行引擎（Daft 对照，RC1/RC2 引擎层） | arXiv 2025 | 2026-07-23 |

> **2026-07-23 精读勘误（重要，两环境协同修正）**：精读代理据论文原文修正了多处早期误述——
> (1) **SABER** 确实**使用 USL**（§IV.B Step 2 明写 "select the USL with the best R²"，Fig 7 对比三变体）作为估计机制，SLA-aware 是其系统目的，二者是机制与目的关系，非二选一；
> (2) **CONCUR** 用 **AIMD**（非 EWMA），双信号 U_t/H_t + 0.3 死区；
> (3) **DistServe** 全文用 simple FCFS 调度，**无** "AFGM attention-focused fairness" 与 "prediction-based prefill-decode pairing"；
> (4) **ProServe** 真实主题是**多优先级请求调度**（TDG 收益 + SlideBatching + GoRouting），非"预测式 PD 分离调度"——PD 分离仅是其部署模式之一。
> 笔记均按论文真实内容撰写。

> **命名说明**：CoLoRA 笔记文件名为 `colora_2026.md`（codex 命名），对应 PDF 为 `colora_aspdac2026.pdf`；Clipper 笔记 `clipper_nsdi2017.md` ↔ PDF `clipper_nsdi2017.pdf`。

精读笔记模板：`tpl-文献精读-深度版.md`（四层：基本信息 → 论文结构分析 → 批判性评估 → 与课题连接）
泛读笔记模板：`tpl-文献泛读.md`

## GPU 调度与数据放置补充调研

已新增补充调研文件：

```text
opening/literature/gpu_scheduler_data_placement_supplement_20260715.md
```

该文件用于回答策略控制器设计从哪些前沿系统思想中来，重点覆盖：

- GPU / LLM 推理服务调度：continuous batching、iteration-level scheduling、SLO-aware scheduling、KV/prefix reuse。
- 异构数据管线：Ray / Ray Data 中的 task、actor、partition-at-a-time 和 CPU/GPU pipeline。
- GPU 数据库算子与数据放置：GPU-resident 结构、materialization、数据是否值得搬到 GPU。
- 数据库 AI 算子：Cortex AISQL、GaussML、Galois、LEADS、NeurDB 等 AI-aware 查询执行背景。

当前定位是“策略依据与后续精读清单”，不是最终综述；其中未下载或未逐篇核验的条目仍需标注为待核验。

## 本地已下载 PDF 子集

用户已将部分参考文献 PDF 下载到：

```text
opening/literature/reference/
```

该目录当前只是**部分文献子集**，用于精读、看论文机制图和核验引用细节；不能视为完整文献库。目录索引见：

```text
opening/literature/reference/README.md
```

后续继续下载 PDF 时，先追加登记到该 README，再决定是否补充精读笔记。

## 开题优先精读顺序

1. Snowflake Cortex AISQL paper / docs：回答“为什么数据库 AI 算子是现实问题”。
2. Ray paper + Ray Core objects / task anti-pattern：回答“为什么 task/actor/object 是合理机制入口”。
3. Daft partitioning / shuffle docs：回答“为什么 partition、shuffle、object slots 会成为链路成本”。
4. pgai / pgvector / PostgresML：回答“PostgreSQL 生态中外部 worker、向量写回、近数据库模型路线分别是什么”。
5. Ray Serve dynamic batching / routing：回答“为什么模型服务状态感知调度有实验接口”。
6. Arrow Flight / Lance：回答“为什么列式中间表示和 AI 数据存储与本课题相关”。

## 可直接支撑开题的观点

- 数据库 AI 算子是工业真实需求：Snowflake、BigQuery、Oracle 和 PostgreSQL 生态均已有相关能力或路线。
- 外部 worker + embedding endpoint + 写回数据库不是本项目凭空发明：pgai vectorizer 形态可作为工程参考。
- Ray task/actor/object store 是分布式 AI 执行中的常见机制，但不能据此声称所有数据库 AI 算子都使用 Ray。
- Daft / Spark 等系统说明 partition、shuffle、batch、coalescing 是分布式数据处理中的成熟问题；本课题的特殊性在于加入了数据库 AI 算子、模型服务队列和写回。
- Ray Serve / vLLM 类模型服务说明 dynamic batching、routing、backpressure 和 token-aware 调度是推理 infra 中的真实问题。

## 不能过度引用的地方

- 不能写成 Snowflake 或 BigQuery 公开使用 Ray / Daft / Lance。
- 不能写成 pgai 是后续长期核心依赖；它更适合作为外部 worker 架构参考。
- 不能用 Ray / Daft 文档直接证明本项目链路中一定存在瓶颈；瓶颈必须由本地 GPU-backed E2E profile 证明。
- 不能把 fake/CPU microbenchmark 写成真实 GPU-backed 链路结论。

## 精读笔记模板

```text
论文 / 资料：
来源级别：
解决的问题：
核心方法：
关键实验：
和本课题的关系：
可以引用的观点：
不能过度引用的地方：
```
