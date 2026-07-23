# 项目最相关 Top 15 论文（排序 + 四维评估）

整理日期：2026-07-23
数据来源：`ai_operator_literature_inventory.md`（65 篇）
评估框架：idea-evaluator 五维雷达 + deep-research 证据层级 + karpathy-guidelines 边界标注

---

## 评估维度

| 维度 | 含义 |
|------|------|
| **基础设施依赖** | 项目能否在不理解该论文的情况下存在？ |
| **方法论直接性** | 论文方法能多直接地迁移到 RC1/RC2？ |
| **定位必要性** | 论文对定义项目贡献边界有多关键？ |
| **实现指导性** | 论文提供多少可直接使用的参数/算法/架构？ |

每个维度 1-5 分（5 最高）。排序按加权综合得分，但"基础设施依赖"为否决性加权的第一排序键。

---

## Top 15 排序

### #1 · vLLM: Efficient Memory Management for Large Language Model Serving with PagedAttention
**SOSP 2023 (CCF-A). Best Paper.** Kwon, Li, Zhuang, Sheng, Zheng, Yu, Gonzalez, Zhang, Stoica.

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 5 | **部署平台，无法替代。** 所有实验在 vLLM 上运行。Continuous batching + PagedAttention 是整个项目的前提假设——没有 PagedAttention 的 KV cache 管理，token-budget/length-align/prefix-aware 策略都失去作用面。 |
| 方法论直接性 | 4 | Continuous batching 的语义（请求级完整性、iteration-level scheduling）直接约束 RC1 的 batch 构造边界——"每行是独立完整的 vLLM 请求，不可行内拆分"。 |
| 定位必要性 | 3 | 项目不修改 vLLM 内部，论文本身不是"相关工作的竞争者"，而是"我们站在谁的肩膀上"。 |
| 实现指导性 | 4 | PagedAttention 的内存模型（block table、KV cache 复用语义）直接决定 prefix-aware batching 的可行性边界。Prometheus 指标（`prompt_tokens_total`、`generation_tokens_total`）是实验 CSV 必含字段。 |

**项目用途**：部署平台 + 实验指标来源 + RC1 策略边界约束。**笔记**：`vllm_sosp2023.md`（已完成 + 图复审）。

---

### #2 · Ray: A Distributed Framework for Emerging AI Applications
**OSDI 2018 (CCF-A).** Moritz, Nishihara, Wang, Tumanov, Liaw, Liang, Elibol, Yang, Paul, Jordan, Stoica.

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 5 | **架构设计空间。** Actor 模型（stateful + async）、task 调度、object store 是 RC2 所有策略的载体。没有 Ray actor 的 stateful 能力，去中心化 queue-adaptive flush 和 actor pool 分池路由无法实现。 |
| 方法论直接性 | 3 | Ray 论文本身不涉及 AI 推理调度策略——它是通用分布式框架。策略设计是项目自己的贡献。 |
| 定位必要性 | 3 | 需要说明"为什么选 Ray 而不是其他框架"，但不能把 Ray 写成研究对象本身。 |
| 实现指导性 | 4 | Actor 生命周期、`ray.get`/`ray.put` 语义、object store 内存模型、task vs actor 的权衡——这些是实现 RC2 策略时必须精确理解的基础机制。 |

**项目用途**：RC2 调度框架 + actor pool 设计空间。**笔记**：`ray_osdi2018.md`（已完成 + 图复审）。

---

### #3 · The Streaming Batch Model for Efficient and Fault-Tolerant Heterogeneous Execution
**arXiv:2501.12407, 2025.** Luan, Mao, Wang et al. (Ray Data 团队)

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 4 | Daft+Ray 管线的**引擎级执行模型**。CPU/GPU 异构 streaming、partition-at-a-time、3-8× 吞吐声明——这是 RC1 和 RC2 的引擎层基础。 |
| 方法论直接性 | 4 | Streaming batch 的粒度控制（batch size、partition、concurrency）直接对接到 RC1 的数据组织参数空间和 RC2 的 `max_concurrency` 控制。 |
| 定位必要性 | 2 | 引擎层论文，不是竞争对手。 |
| 实现指导性 | 5 | 提供 Daft/Ray Data 的 batch/concurrency/partition 参数语义——这是消融实验的引擎级变量来源。 |

**项目用途**：引擎级参数空间定义 + Daft→Ray 管线执行模型。**笔记**：❌ **缺失——需精读。**

---

### #4 · Clipper: A Low-Latency Online Prediction Serving System
**NSDI 2017 (CCF-A).** Crankshaw, Wang, Alivio, Gonzalez, Stoica et al.

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 1 | 不依赖（Clipper 是 pre-LLM 时代的 ML 预测服务，与本项目技术栈不同）。 |
| 方法论直接性 | 5 | **RC2 的方法论祖先。** AIMD 自适应 batching（additive increase +1, multiplicative decrease ×0.9, 10% SLO backoff）是 queue-adaptive flush 的直接前身。延迟-吞吐权衡框架（"在 SLO 约束下最大化 batch size"）精确对应本项目 RC2 的问题定义。 |
| 定位必要性 | 3 | 需要在相关工作 § 中引用作为自适应 batching 的奠基工作。 |
| 实现指导性 | 4 | 提供可复用的 AIMD 控制律参数（+1/×0.9），以及"延迟分位数作为反馈信号"的架构模式。选择性 batching（按模型/延迟 SLO 分组）可启发 actor pool 分池设计。 |

**项目用途**：RC2 AIMD 控制律的直接来源 + 相关工作定位。**笔记**：`clipper_nsdi2017.md`（已验证准确）。

---

### #5 · SGLang: Efficient Execution of Structured Language Model Programs
**NeurIPS 2024 (CCF-A).** Zheng et al.

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 1 | 不依赖（SGLang 是 vLLM 的替代方案，项目以 vLLM 为平台）。 |
| 方法论直接性 | 5 | **RC1 prefix-aware batching 的核心方法论来源。** RadixAttention 通过 radix tree 管理 KV cache 复用——请求间共享 prefix 时避免重复计算 prefill。这直接定义了 prefix-aware batching 的优化目标：按 prefix 相似度分组请求以最大化 KV cache 命中率。 |
| 定位必要性 | 2 | 作为 prefix 复用技术的代表引用。 |
| 实现指导性 | 4 | RadixAttention 的数据结构（radix tree LRU eviction）、prefix caching 的命中/未命中成本模型——可直接指导 prefix-aware 分组策略的 cost-benefit 分析。 |

**项目用途**：RC1 prefix-aware batching 的方法论来源。**笔记**：`sglang_neurips2024.md`（agents 完成）。

---

### #6 · BucketServe: Sequence-Length Bucketed Batching for LLM Serving
**arXiv 2025.**

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 1 | 不依赖。 |
| 方法论直接性 | 5 | **RC1 length-align 策略最直接的方法论文。** 按序列长度分桶以减少 padding 浪费——这正是本项目 "length-align" 的工程实例。提供了长度分组减少 padding 的量化证据和分桶策略。 |
| 定位必要性 | 2 | 作为 length-align 方向的最新代表引用。 |
| 实现指导性 | 4 | 分桶策略（桶的数量、边界选择方法）、padding 开销的量化模型——可直接指导 RC1 length-align 的实现和消融实验设计。 |

**项目用途**：RC1 length-align 策略的直接方法论来源。**笔记**：`bucketserve_2025.md`（agents 完成）。

---

### #7 · CONCUR: Adaptive and Agent-based Concurrency Control for LLM Serving
**arXiv 2026.**

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 1 | 不依赖。 |
| 方法论直接性 | 5 | **RC2 自适应并发控制的最直接方法论文。** Agent 级 AIMD（α=2, β=0.5），双信号 U_t（GPU 利用率）+ H_t（KV cache 命中率），死区 0.2-0.5。这恰好是 RC2 queue-adaptive flush 的三个核心要素：(1) 自适应步长控制律、(2) 模型服务状态作为反馈信号、(3) 避免振荡的 hysteresis 机制。 |
| 定位必要性 | 2 | 作为自适应并发控制最新代表引用。 |
| 实现指导性 | 5 | **所有参数已验证可复用**（α=2/β=0.5/U_low=0.2/U_high=0.5/H_thresh=0.2），不需要从头调参。双信号设计（利用率 + 缓存命中）比 Clipper 的纯延迟信号更适合 LLM 场景。但需注意其 agentic workload 假设与项目不同。 |

**项目用途**：RC2 AIMD 参数直接来源 + 双信号架构参考。**笔记**：`concur_2025.md`（已验证准确）。

---

### #8 · SABER: SLA-Aware Batching for Efficient LLM Serving
**arXiv 2025.**

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 1 | 不依赖。 |
| 方法论直接性 | 4 | **RC2 K_max 动态控制的理论框架来源。** Universal Scalability Law (USL) 建模并发度与吞吐的非线性关系：v̂(L) = λ/(1+α(L−1)+βL(L−1))。USL 提供了确定"最优并发度"的数学框架，可作为 K_max 自适应调整的理论基础。 |
| 定位必要性 | 2 | 作为 SLA-aware batching 最新代表引用。 |
| 实现指导性 | 3 | USL 拟合方法（R² 选择、~1000 采样点、`scipy.optimize`）可直接复用。但需注意 SABER 是 per-request 准入（非 batch 级），其 USL 从历史数据拟合而非在线更新——项目可改进为在线 USL。 |

**项目用途**：RC2 K_max 动态控制的理论框架。**笔记**：`saber_2025.md`（已验证准确）。

---

### #9 · Splitwise: Efficient Generative LLM Inference Using Phase Splitting
**ISCA 2024 (CCF-A 顶会).** Patel, Choukse, Zhang et al.

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 1 | 不依赖。 |
| 方法论直接性 | 4 | **RC2 异构 actor pool 的设计参考。** Prefill（compute-bound）和 decode（memory-bound）分池部署，各自独立扩缩容——这直接启发本项目的异构 actor pool 设计：不同 batch 类型（token-heavy vs latency-sensitive）可分到不同 GPU/actor 池。 |
| 定位必要性 | 2 | 作为 phase-aware resource partitioning 的代表。 |
| 实现指导性 | 3 | 分池的资源配比策略、prefill/decode 的负载特征量化——可指导 actor pool 的 GPU 分配和路由策略设计。但 Splitwise 是机器级分池（固定），项目要做的是请求级动态路由（更细粒度）。 |

**项目用途**：RC2 异构 actor pool 设计的灵感来源。**笔记**：`splitwise_isca2024.md`（agents 完成）。

---

### #10 · Sarathi-Serve: Taming Throughput-Latency Tradeoff in LLM Inference
**OSDI 2024 (CCF-A).** Agrawal, Kedia, Panwar et al.

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 1 | 不依赖（vLLM 已实现 chunked prefill）。 |
| 方法论直接性 | 4 | **RC1 token-budget 策略的关键分析框架。** Chunked prefill 将单次 prefill 拆分为多个 chunk，避免长 prefill 阻塞 decode——这直接解释了为什么 token-budget batching 需要控制每个 batch 的总 token 量：超大 batch 的 prefill 会 stall 所有在途请求的 decode。 |
| 定位必要性 | 2 | 作为 chunked prefill 和 throughput-latency 分析的权威来源。 |
| 实现指导性 | 3 | "stall-free batching"的调度约束（prefill chunk 大小上限、与 decode 的交错策略）——这些约束直接转化为 RC1 token-budget 的硬上限参数。 |

**项目用途**：RC1 token-budget 的调度约束来源（为什么需要控制 token 量）。**笔记**：`sarathi_serve_osdi2024.md`（已完成 + 图复审，28.3× 幻觉已修复）。

---

### #11 · Multi-Bin Batching: Length-Align Packing via Order Statistics
**arXiv 2024.**

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 1 | 不依赖。 |
| 方法论直接性 | 4 | **RC1 length-align 的数学理论基础。** Order statistics 建模序列长度分布与装箱效率的关系——这为 length-align 分组策略提供了超越"工程直觉"的理论支撑。回答了"为什么按长度分组能减少 padding"的数学原因。 |
| 定位必要性 | 1 | 理论支撑论文，不需要在开题中重点引用。 |
| 实现指导性 | 3 | 装箱效率的数学 bound、分组数量与 waste 的 tradeoff 公式——可指导 RC1 length-align 的桶边界优化（而非纯经验调参）。 |

**项目用途**：RC1 length-align 策略的数学基础。**笔记**：`multibin_batching_2024.md`（agents 完成）。

---

### #12 · Cortex AISQL: A Production SQL Engine for Unstructured Data
**SIGMOD Companion 2026 (CCF-A 工业轨).** Aggarwal, Chen, Datta, Han et al.

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 1 | 不依赖（Snowflake 闭源）。 |
| 方法论直接性 | 2 | 方法（AI-aware 查询优化、模型级联、语义 Join）与项目不同层——Cortex 做计划级优化，项目做批内组织与提交控制。但架构图（Fig 1）揭示了一个关键事实：Cortex 的 batching 被下放给 vLLM，上游不做 token-budget 组织——这正是项目填补的空白。 |
| 定位必要性 | 5 | **最重要的定位对照论文。** 开题 §2 必须精确区分"内部执行/DB4AI 路线"（Cortex 代表）与"外部执行链路优化"（本项目）。Cortex 的生产数据（AI 算子主导查询成本、~40% 多表查询）直接支撑动机。 |
| 实现指导性 | 1 | 闭源系统，无实现可复用。但其"AI 感知代价模型"C_op(n) = n × c_model 可作为项目的 baseline 假设（然后论证为何 batching 下该假设不成立）。 |

**项目用途**：动机证据 + DB4AI 定位对照 + "空白"论证。**笔记**：`cortex_aisql_sigmod2026.md`（⭐你读 + 图复审，假设 2/3 已修正）。

---

### #13 · NeurDB: On the Design and Implementation of an AI-powered Autonomous Database
**CIDR 2025 (顶会 Vision Paper).** Zhao, Cai, Ooi et al.

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 1 | 不依赖（NeurDB 是 PG fork，不可直接使用）。 |
| 方法论直接性 | 2 | AI Engine + Data Streaming Protocol 与本项目的 Daft→Ray→vLLM 链路有表面相似性，但 NeurDB 面向轻量 ML（ARM-Net），本项目面向 LLM/VLM。图复审发现的最关键事实：NeurDB 的推理运行时也在"外部节点"——"内部 vs 外部"不是真正的区分轴。 |
| 定位必要性 | 5 | **最重要的路线对照论文。** 开题必须论证"为什么 AI 推理（尤其是大模型）应该在数据库外部"。NeurDB 是"AI inside DB"路线的最新、最强 vision paper——必须正面回应。 |
| 实现指导性 | 2 | FRP（Filter-and-Refine）两阶段策略可迁移到 actor pool 分池路由（粗筛→精调度）。但 NeurDB 的核心贡献（增量模型更新、模型存储）与项目正交。 |

**项目用途**：路线对照（AI inside DB）+ FRP 设计参考。**笔记**：`neurdb_cidr2025.md`（⭐你读 + 图复审，定位从"库内 vs 库外"修正为"控制平面位置 + 请求组织层 + 提交控制层"三维区分）。

---

### #14 · Galois: Logical and Physical Optimizations for SQL Query Execution over LLMs
**SIGMOD 2025 (CCF-A).** Satriani, Veltri, Santoro et al.

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 1 | 不依赖。 |
| 方法论直接性 | 2 | "LLM 作为存储层"与项目"LLM 作为推理引擎"方向正交。但 Key-Scan 的分解式调用思路（先取 Key 再取值，第二步可并行）对 batch 分解有启发——虽然目标函数不同（质量 vs 吞吐）。 |
| 定位必要性 | 4 | **DB4AI 多样性的最佳例证。** 与 Cortex（AI inside DB 工业版）+ NeurDB（AI inside DB 学术版）共同构成 DB4AI 路线谱系——Galois 代表"LLM as storage"分支，项目代表"LLM as compute"分支。图复审证实 Galois 完全没有请求 batching/队列/并发控制——正好划定项目空白。 |
| 实现指导性 | 1 | 方法服务于语义提取质量，非吞吐优化。直接可复用内容少。 |

**项目用途**：DB4AI 路线多样性论证 + 图级空白证据。**笔记**：`galois_sigmod2025.md`（⭐你读 + 图复审，Key-Scan 借鉴价值已温和修正）。

---

### #15 · A Database Perspective on LLM Inference Systems
**PVLDB Vol.18, 2025 (CCF-A).** Pan, Li.

| 维 | 分 | 理由 |
|----|-----|------|
| 基础设施依赖 | 1 | 不需要。 |
| 方法论直接性 | 1 | 综述论文，不提供新方法。 |
| 定位必要性 | 4 | **权威定位框架。** 从数据库视角系统梳理 LLM 推理系统——这正是项目开题 §2 需要的"研究版图"。可引用其分类框架（模型服务、查询优化、数据管理）来定位本项目的贡献（填补"数据组织 + 提交控制"的交叉空白）。 |
| 实现指导性 | 1 | 综述论文，无实现细节。但其参考文献列表是补充检索的优质来源。 |

**项目用途**：开题 §2 定位框架 + 文献追溯入口。**笔记**：`db_perspective_llm_pvldb2025.md`（⭐你读）。

---

## 覆盖度分析

### 按角色分布

| 角色 | 篇数 | 论文 |
|------|------|------|
| **基础设施**（项目运行在其上） | 3 | vLLM #1, Ray #2, Ray Data Streaming #3 |
| **RC1 方法**（数据组织策略） | 4 | SGLang #5, BucketServe #6, Sarathi-Serve #10, Multi-Bin #11 |
| **RC2 方法**（调度提交控制） | 4 | Clipper #4, CONCUR #7, SABER #8, Splitwise #9 |
| **定位对照**（定义项目边界） | 4 | Cortex #12, NeurDB #13, Galois #14, DB Perspective #15 |

### 按研究内容覆盖

| 策略 | 论文支撑 | 覆盖度 |
|------|---------|--------|
| Token-budget batching | Sarathi-Serve (约束), vLLM (平台语义) | 🟢 充分 |
| Length-align batching | BucketServe (工程), Multi-Bin Batching (理论) | 🟢 充分 |
| Prefix-aware batching | SGLang (RadixAttention) | 🟡 有核心参考，但理论深度不足 |
| Queue-adaptive flush | Clipper (AIMD), CONCUR (AIMD+双信号) | 🟢 充分 |
| K_max 动态控制 | SABER (USL), CONCUR (AIMD) | 🟢 充分 |
| Actor pool 分池路由 | Splitwise (分池), Ray (actor 模型) | 🟡 有设计参考，但需更多路由策略文献 |
| 多模态泛化 | — | 🔴 15 篇中无多模态特定论文；需从 Daft/VLM 文献补 |

### 按证据层级

| 层级 | 篇数 | 论文 |
|------|------|------|
| CCF-A 会议/期刊 | 8 | vLLM, Ray, Clipper, SGLang, Splitwise, Sarathi-Serve, Cortex, Galois |
| CCF-A 期刊 | 1 | DB Perspective (PVLDB) |
| 顶会 Vision Paper | 1 | NeurDB (CIDR) |
| arXiv 预印本 | 5 | Ray Data Streaming, BucketServe, CONCUR, SABER, Multi-Bin |

---

## 不在 Top 15 但值得注意的论文

| 论文 | 不入选原因 | 何时可能需要 |
|------|-----------|-------------|
| **Orca (OSDI 2022)** | vLLM + Sarathi 已覆盖 iteration-level scheduling 的核心概念 | 需要引用"continuous batching 前身"时 |
| **ServerlessLLM (OSDI 2024)** | 模型加载优化，与本项目调度策略正交 | 需要考虑 cold start 场景时 |
| **Scorpio (arXiv 2025)** | SLO 异构 + token 级延迟建模——对 RC2 有方法论价值，但被 CONCUR + SABER 覆盖了核心机制 | 需要更精细的 per-token 延迟建模时 |
| **ProServe (arXiv 2026)** | 多优先级调度——对 actor pool routing 有启发，但优先级机制不是项目当前重点 | 引入请求优先级时 |
| **Mooncake (ACM TOS 2025)** | KVCache 分离架构——对 prefix-aware batching 有基础设施层面的启发 | 考虑 KVCache 跨请求共享架构时 |
| **FlashAttention (NeurIPS 2022)** | Kernel 级优化，太底层——与项目"上游调度"定位不匹配 | 需要解释 vLLM 底层机制时 |
| **Lance (arXiv 2025)** | Daft 的存储格式——与项目调度策略正交 | 需要解释 Daft 列式存储优势时 |
| **Smart (VLDB Journal 2025)** | ML 谓词优化，面向传统 ML 非 LLM | 需要更完整的 DB4AI 相关工作章节时 |
| **LEADS (PVLDB 2024)** | 数据库内模型切片，面向结构化数据 | 同上 |
| **DiskANN (NeurIPS 2019)** | 向量检索——与写回阶段有关但非调度核心 | 写回优化讨论时 |
| **Milvus (SIGMOD 2021)** | 向量数据库——pgvector 对照 | 写回方案对比时 |

---

## 立即行动项

1. **补 Ray Data Streaming Batch Model 精读笔记**（#3，唯一缺失的 Top 15 笔记）。PDF: `ray_data_streaming_batch_2025.pdf`。
2. **Top 15 中 agents 完成的 7 篇**（SGLang #5, BucketServe #6, CONCUR #7, SABER #8, Splitwise #9, Multi-Bin #11）——你扫结论，确认无误。
3. **多模态覆盖缺口**：15 篇中无一篇多模态特定论文。Daft `@daft.cls` GPU UDF 官方文档 + Snowflake Cortex Multimodal 文档需作为补充。

---

*评估框架：idea-evaluator 五维雷达（Higher/Faster/Stronger/Cheaper/Broader）+ deep-research 证据层级（CCF-A > 顶会 > arXiv）+ karpathy-guidelines（每篇标注"事实/推断/待确认"边界）。排序非单纯"论文质量"排名，而是"对本项目特定课题的贡献度"排名。*
