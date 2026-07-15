# Research Directory

本目录保存背景调研、论文、官方文档和外部系统证据。这里的材料用于支撑开题、方向判断和实验设计，不存放原始实验 CSV。

## 重点入口

| 文件 | 作用 |
|---|---|
| `literature_and_evidence_review.md` | Ray / Daft / Lance / Snowflake / pgai 等方向的综合证据记录 |
| `existing_ai_operator_execution_chains.md` | 现有数据库 AI 算子与 AI 数据处理系统的执行链路对比 |

## 使用规则

- 优先引用官方文档、论文、源码 README 和本项目真实实验结果。
- 外部系统只作为背景、对照路线和实验设计参考；不能把闭源系统的内部实现当成已知事实。
- Snowflake 这类托管闭源系统不作为本地必测 baseline，除非后续有账号、预算和明确的用户可见 SQL benchmark 目标。
- pgai / PostgresML / pgvector 可以作为 PostgreSQL 生态路线参考，但是否纳入实验要看它能否回答本项目的链路瓶颈问题。

## 文献优先设计方法论（Literature-First Design）

当用户询问"怎么设计 X 系统""怎么构建 Y 算法""怎么设计 Z 实验/流程"时，遵循以下方法。核心原则：**从已有顶会文献中提取设计模式和策略思路，不凭空设计，不凭工程常识拍板。**

### 为什么需要这个方法

- 你已有一个 57 篇 CCF-A/顶会的文献清单（`opening/literature/ai_operator_literature_inventory.md`）
- 四个研究岛（DB AI 算子、GPU 推理服务、分布式数据管线、结果持久化）各自有几十篇 CCF-A 论文
- 凭空设计的系统/算法很可能和已有顶会工作的思路重复，或漏掉已知最优 practice
- Reviewer 会问"你的 baseline 是什么？为什么比 X 论文的方案更好？"

### 四步执行流程

**Step 1：定位问题方向**

X 属于哪个研究岛？可能需要查阅文献清单中的哪个组？

| 研究岛 | 文献清单对应组 | 代表论文 |
|---|---|---|
| 数据库 AI 算子 | 第一组 + A 组 | Cortex AISQL, GaussML, Smart, Galois, NeurDB |
| GPU 推理服务 | 第三组 + B 组 | vLLM, Orca, Sarathi-Serve, ServerlessLLM, SGLang |
| 分布式数据管线 | 第五组 + C 组 | Ray, Ray Data, Daft, Spark, Arrow Flight |
| 结果持久化与写回 | 第六组 + E 组 | TurboVecDB, Delta Lake, FlexPushdownDB, WiscKey, DiskANN |

**Step 2：从文献提取候选方案**

- **精读组论文的核心技术/架构** → 作为首选设计参考（这些是你已经确认过的）
- **补充组论文的关键机制** → 作为对照或变体（这些是已筛选的高相关论文）
- **产业系统的工程实践** → 作为可行性参考（这些证明方案在工程上是可行的）

**Step 3：对比与差异化**

对每个候选方案回答三个问题：
1. 它的适用场景是什么？
2. 它的边界/不足是什么？（这通常是你的机会点）
3. 你的场景和它的场景有什么不同？

示例：
> vLLM 的 continuous batching 优化了 GPU 侧的 request-to-batch 调度，但它的输入是抽象的 request queue，不感知输入数据来自数据库表、输出需要写回数据库。我们借鉴 vLLM 的 batching 思路，但在 batch 构造时加入了 writeback-aware 的约束。

> TurboVecDB 通过 io_uring 和空间感知插入将 pgvector 索引构建时间减少了 98.4%，但它假设数据已经在数据库侧就绪。我们在此之上研究 GPU worker 产生的向量如何以最优批量和时序写入。

**Step 4：提出综合方案**

标注每个设计决策的来源：

| 设计决策 | 来源 |
|---|---|
| Continuous batching 思路 | vLLM (SOSP 2023) |
| 代价驱动的跨层决策模型 | FlexPushdownDB (VLDB 2021) |
| Worker-direct blind append | Delta Lake (VLDB 2020) |
| 延迟建 HNSW 索引 | pgvector Issues + TurboVecDB (VLDB 2025) |
| 本文新增：GPU batch × write batch joint optimization | 本文 |

### 同理适用于实验 Baseline 选择

设计实验对照时，**优先从文献提取最优 baseline**，不使用 strawman。

- GPU 调度 baseline → vLLM / Orca / Sarathi-Serve（G1-G6）
- 写回 baseline → TurboVecDB + COPY 延迟建索引（W1-W7）
- 跨层 baseline → FlexPushdownDB / AIDB / Deferred View Maintenance（X1-X3）

完整 baseline 矩阵见 `experiments/plans/baseline_reference.md`。

### 示例：正确 vs 错误的做法

**❌ 错误做法（凭空设计）**：
> "我们设计了一个 GPU batch 调度器，根据 batch size 动态调整 worker 数量..."

这不是新东西。vLLM (SOSP 2023) 已经在做 continuous batching。你的设计应该写明"vLLM 提供了 iteration-level continuous batching，但它的 request queue 不感知数据来自数据库表。我们在此之上加入了 database-fetch-aware batch construction..."

**✅ 正确做法（文献优先）**：
> "vLLM (Kwon et al., SOSP 2023) 的 PagedAttention 和 continuous batching 将 GPU 内存利用率提升到 >96%，Orca (Yu et al., OSDI 2022) 的 iteration-level scheduling 将吞吐提升了最高 36.9×。但这些系统的优化范围止于 GPU 侧——它们不感知数据来自数据库表、计算结果需要写回 pgvector/Lance。我们借鉴 vLLM 的 batching 机制，但将 batch 构造策略扩展为 writeback-aware：GPU batch size 的选择不仅考虑 GPU utilization，还考虑下游写回的最优批量..."

### 执行检查清单

设计任何新系统/算法/实验方案时：
- [ ] 是否先查阅了文献清单中对应研究方向组的论文？
- [ ] 是否从至少 2 篇 CCF-A 论文中提取了候选方案？
- [ ] 是否写清楚了本文方案与已有方案的差异？
- [ ] 是否标注了每个关键设计决策的来源？
- [ ] Baseline 是否来自文献而非凭空选择？

