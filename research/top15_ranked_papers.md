# 项目最相关 Top 15 论文（按学术标准重排）

整理日期：2026-07-24（按学术研究标准重排；原"对本项目贡献度"排序见 git 历史）
候选池：`ai_operator_literature_inventory.md`（66 篇，含 Clockwork）

## 排名标准

按**学术研究标准**从 66 篇中选前 15：

- **基础工作**：课题立足的基石（开山论文、部署平台/框架的直系前身）；
- **核心技术**：直接支撑某项策略机制（token-budget / length-align / prefix-aware / queue-adaptive flush / K_max / actor pool 路由）；
- **相关工作**：定位与"空白"论证必引。

优先级：**CCF-A > 顶会 > arXiv**；某核心策略若**无 CCF-A 来源**，允许一篇极重要 arXiv 破例入选。正好 15 篇。

> **与旧版差异**：Orca、DistServe 进；SABER、Multi-Bin 出（移至"值得注意"）。CCF-A/顶会 12 篇 + 重要 arXiv 3 篇（旧版 10+5）。核心理由：开山基础论文即使被后继实现也该占席位，不应以"机制被覆盖"为由排除。

---

## Top 15

### 一、基础工作（4 篇）

| # | 论文 | 出处 | CCF | 入选理由 | 笔记 |
|---|---|---|---|---|---|
| 1 | **vLLM**: PagedAttention + continuous batching | SOSP 2023 · Best Paper | A | 部署平台，所有实验其上运行；PagedAttention 内存模型是 prefix-aware 策略的作用面前提 | `vllm_sosp2023.md` |
| 2 | **Orca**: iteration-level scheduling | OSDI 2022 | A | continuous batching / 迭代级调度**开山**；vLLM 直系前身；开题正文"vLLM/Orca"并称 5 次，必引 | `orca_osdi2022.md` |
| 3 | **Ray**: 分布式 AI 框架 | OSDI 2018 | A | actor 模型（stateful+async）+ object store 是 RC2 所有策略的载体 | `ray_osdi2018.md` |
| 4 | **Clipper**: AIMD 自适应 batching | NSDI 2017 | A | RC2 queue-adaptive flush 的**方法论奠基**（additive increase / multiplicative decrease 控制律） | `clipper_nsdi2017.md` |

### 二、核心技术（7 篇）

| # | 论文 | 出处 | CCF | 支撑策略 | 笔记 |
|---|---|---|---|---|---|
| 5 | **Sarathi-Serve**: chunked prefill | OSDI 2024 | A | RC1 token-budget（为什么必须控制每 batch 的 token 量） | `sarathi_serve_osdi2024.md` |
| 6 | **SGLang**: RadixAttention | NeurIPS 2024 | A | RC1 prefix-aware（radix tree 管理 KV cache 复用） | `sglang_neurips2024.md` |
| 7 | **DistServe**: prefill-decode 分离 + goodput | OSDI 2024 | A | RC2 阶段干扰/K_max 概念基础；goodput 吞吐-延迟评估范式 | `distserve_osdi2024.md` |
| 8 | **Splitwise**: phase splitting | ISCA 2024 | A | RC2 actor pool 分池路由（prefill/decode 异构） | `splitwise_isca2024.md` |
| 9 | **CONCUR**: agent AIMD + KV 双信号 | arXiv 2026 | 预印本 | RC2 控制器**直接机制来源**（AIMD+死区+KV 信号；无 CCF-A 替代） | `concur_2025.md` |
| 10 | **Ray Data Streaming Batch** | arXiv 2025 | 预印本 | Daft+Ray 引擎执行模型（streaming batch / partition-at-a-time；无 CCF-A 替代） | `ray_data_streaming_batch_2025.md` |
| 11 | **BucketServe**: sequence-length 分桶 | arXiv 2025 | 预印本 | RC1 length-align（无 CCF-A 替代） | `bucketserve_2025.md` |

### 三、相关工作（4 篇）

| # | 论文 | 出处 | CCF | 定位作用 | 笔记 |
|---|---|---|---|---|---|
| 12 | **Cortex AISQL** | SIGMOD 2026 | A | DB4AI 工业锚点；其 batching 下放给 vLLM、上游不做 token 组织——本项目"空白"论证支点 | `cortex_aisql_sigmod2026.md` |
| 13 | **NeurDB** | CIDR 2025 | 顶会 | "AI inside DB" 路线最强 vision paper，必须正面回应 | `neurdb_cidr2025.md` |
| 14 | **Galois**: LLM as storage | SIGMOD 2025 | A | DB4AI 多样性；划定本项目"LLM as compute"分支对照 | `galois_sigmod2025.md` |
| 15 | **Database Perspective on LLM Inference** | PVLDB 2025 | A | 定位综述框架 + 文献追溯入口 | `db_perspective_llm_pvldb2025.md` |

---

## 覆盖度

| 策略 | 论文支撑 | 覆盖度 |
|---|---|---|
| Token-budget | Sarathi-Serve, vLLM | 🟢 充分 |
| Length-align | BucketServe | 🟡 仅 arXiv，无 CCF-A |
| Prefix-aware | SGLang | 🟢 充分 |
| Queue-adaptive flush | Clipper, CONCUR | 🟢 充分 |
| K_max 动态控制 | DistServe, CONCUR, Clipper | 🟢 充分 |
| Actor pool 分池路由 | Splitwise, Ray | 🟢 充分 |
| 多模态泛化 | — | 🔴 15 篇中无多模态特定论文 |

## 证据层级

| 层级 | 篇数 | 论文 |
|---|---|---|
| CCF-A 会议/期刊 | 11 | vLLM, Orca, Ray, Clipper, Sarathi-Serve, SGLang, DistServe, Splitwise, Cortex, Galois, DB Perspective |
| 顶会（非 CCF） | 1 | NeurDB (CIDR) |
| arXiv 预印本 | 3 | CONCUR, Ray Data Streaming, BucketServe |

---

## 不在 Top 15 但值得注意

| 论文 | 不入选原因 | 何时可能需要 |
|---|---|---|
| **SABER** (arXiv 2025) | USL 理论框架——K_max 的数学支撑，但 AIMD 已由 Clipper+CONCUR 覆盖；按 CCF-A 优先让位 DistServe | 需 USL 建模 / 在线 K_max 理论时 |
| **Multi-Bin Batching** (arXiv 2024) | length-align 数学理论；length-align 已由 BucketServe 工程代表 | 需 length-align 桶边界理论优化时 |
| **ServerlessLLM** (OSDI 2024) | 模型加载优化，与上游调度正交 | cold start 场景 |
| **Parrot** (OSDI 2024) | 语义变量 prefix 共享；prefix 已由 SGLang 覆盖 | 跨应用 prefix 抽象 |
| **Mooncake** (ACM TOS 2025) | KVCache 分离架构 | KVCache 跨请求共享架构 |
| **Scorpio** (arXiv 2025) | per-token 延迟建模，被 CONCUR+SABER 覆盖 | 精细 per-token 延迟 |
| **ProServe** (arXiv 2026) | 多优先级调度 | 引入请求优先级 |
| **Clockwork** (OSDI 2020) | 确定性/可预测推理调度——queue-adaptive flush 的调度思想来源之一（knowledge_hub §5.2 引），但 AIMD 角度已被 Clipper/CONCUR 覆盖 | 需确定性调度论证时 |
| **FlashAttention** (NeurIPS 2022) | kernel 级，与"上游调度"定位不匹配 | 解释 vLLM 底层机制 |
| Lance / Smart / LEADS / InferDB / DiskANN / Milvus | 存储或 DB4AI 方向，与上游调度核心正交 | 写回 / DB4AI 相关工作补全 |

---

## 后续行动项

- ✅ Top 15 精读笔记均已完成，位于 `research/reading_notes/`；开题自包含拷贝在 `opening/literature/top15_reading_notes/`。
- **多模态覆盖缺口**：15 篇中无多模态特定论文。Daft `@daft.cls` GPU UDF 官方文档 + Snowflake Cortex Multimodal 文档需作为补充。
- **Clockwork**：已补入 `ai_operator_literature_inventory.md`（66 篇）；原文 PDF 待放入 `research/reference/clockwork_osdi2020.pdf` 后登记 REFERENCE_INDEX 并补全精确题录。

---

*排名标准：学术研究（基础工作 / 核心技术 / 相关工作）+ CCF-A 优先 + 极重要 arXiv 破例。非单纯"论文质量"排名，而是"对本课题的学术支撑度"——基础开山论文即使被后继实现也占席位。*
