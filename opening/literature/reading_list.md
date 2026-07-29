# 文献精读清单

更新日期：2026-07-29

## 选择原则

- Top 15 优先选择 CCF-A 正式 research paper。
- Companion、Demo、Tutorial、CIDR、MLSys、arXiv 单独标注，不借所属会议/期刊抬高等级。
- 先核验作者、标题、年份、轨道、卷期和 DOI，再进入 Top 15。
- 精读必须覆盖问题、方法、baseline/workload/指标、假设与局限，以及与本项目的可迁移关系。
- 不根据摘要直接调整 Top 15。

## 当前统计

- `research/reading_notes/` 现有 **49 篇权威精读笔记**（不含 README 和两个模板）。
- 旧文档“33 篇已完成”已经过时；原编号到 41 还混入了两篇未下载条目，也不能作为实体笔记数。
- 当前 Top 15 为 15/15 严格 CCF-A 正式论文，快照在 `top15_reading_notes/`。
- `research/reference/` 当前有 21 份可解析 PDF；Top 15 的 15 份全部齐全。

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

### 数据库 AI 系统与 benchmark

| 文献 | 状态 | 作用 |
|---|---|---|
| Palimpzest | CIDR 2025 | 声明式 plan search、系统 baseline |
| SemBench | PVLDB 2026 benchmark | 跨系统 workload、质量/延迟/成本/内存指标 |
| Database Perspective on LLM Inference | PVLDB Tutorial | 技术版图与 open problem |
| Cortex AISQL | 按实际轨道引用 | 工业需求证据 |
| NeurDB | CIDR 2025 | AI-native database vision |

### 公平与 program/job 调度

| 文献 | 状态 | 作用 |
|---|---|---|
| FairServe | arXiv 2024 | weighted service、interaction throttling |
| DLPM/D2LPM | arXiv 2025 | deficit fairness 与 prefix locality |
| Autellix | arXiv 2025 | program-level attained service |
| Chiron | arXiv 2025 | 分层 backpressure/autoscaling |
| Clipper | NSDI 2017 | AIMD batching 历史来源 |
| Splitwise | ISCA 2024 | prefill/decode 分池边界 |
| Ray Data Streaming Batch | arXiv 2025 | 官方数据引擎执行模型 |

### 其他已有权威笔记

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
- LOTUS、Galois、GaussML、Palimpzest、Abacus 说明 AI 算子需要声明式物理实现、质量/成本/延迟的联合选择。
- Learned Cost Models、GRACEFUL、COSTREAM 说明代价模型的价值应由下游决策质量验证，而不是只比较误差。
- SemBench 说明数据库 AI baseline 必须同时报告质量、调用数、token work、延迟、成本、内存与失败。

## 不能过度引用

- 不把 CIDR、MLSys、arXiv、Companion、Tutorial 写成 CCF-A research paper。
- 不把 serving 内部调度论文的收益直接归因到本项目上游 Ray 调度。
- 不把减少 LLM 调用数的系统收益与“相同 work 执行更快”混为一谈。
- 不把单 job 饱和 throughput 平台解释为调度无价值；多 job 公平、压力效率与 transient ramp 是不同问题。
- 不把 learned cost model 作为既定实现，除非简单模型在 held-out 决策上确实不足。

## 模板

- 权威深度模板：`research/reading_notes/tpl-文献精读-深度版.md`
- 泛读模板：`research/reading_notes/tpl-文献泛读.md`
