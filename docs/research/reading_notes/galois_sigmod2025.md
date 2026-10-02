---
type: paper-note
tags:
  - deep-reading
  - paper/galois
  - db4ai
  - sigmod2025
aliases:
  - "Galois (SIGMOD 2025)"
status: 精读完成
read_date: 2026-07-22
---

# 精读笔记：Galois — Logical and Physical Optimizations for SQL over LLMs (SIGMOD 2025)

---

## ▎第一层 · 基本信息

| 字段 | 内容 |
|------|------|
| **论文** | Satriani, Veltri, Santoro, Rosato, Varriale, Papotti. *Logical and Physical Optimizations for SQL Query Execution over Large Language Models.* SIGMOD 2025. DOI:10.1145/3725411 |
| **来源级别** | CCF-A 会议论文（University of Basilicata + EURECOM） |
| **链接** | DOI:10.1145/3725411 / 本地 PDF：`raw/papers/galois_sigmod2025.pdf` / 代码：https://github.com/dbunibas/galois |
| **阅读日期** | 2026-07-22 |
| **状态** | 精读完成 |
| **相关论文组** | DB4AI（数据库 AI 算子） |

### 一句话核心结论

Galois 作为 SQL 查询和 LLM 之间的中间件，将 LLM 视为"存储层"，设计了 LLM 专用的 Table-Scan / Key-Scan 物理算子 + 基于置信度的逻辑/物理优化，比直接 NL 提问质量提升 144%，比直接 SQL 质量提升 29%，且比同类多步 baseline 节省 11 倍 token 成本。

`#DB4AI` `#LLM-as-storage` `#cost-qualitytradeoff` `#SQL-over-LLM` `#SIGMOD2025`

---

## ▎第二层 · 论文结构分析

### 1. 问题拆解

| 问题 | 论文的回答 |
|------|-----------|
| 要解决什么痛点？ | 直接向 LLM 提问（NL 或 SQL）获取结构化数据时，结果质量差——低精度、低召回，尤其是复杂查询（多条件、聚合、Join）|
| 之前的方法为什么不够？ | NL 提问有歧义；直接 SQL 提示语虽好但仍不够；传统查询优化假设（有 catalog、有直方图、I/O 成本可预测）在 LLM 场景不成立 |
| 论文的**核心论点** | 应该用数据库管理系统来处理查询执行，而把 LLM 当作存储层——两者各司其职 |
| 它的**关键假设** | LLM 内部确实存储了可提取的结构化知识（parametric knowledge），并且可以通过精心设计的 prompt 链逐步提取出来 |

### 2. 方法拆解

```mermaid
flowchart LR
    A[SQL Query] --> B[Galois 中间件]
    B --> C[逻辑优化]
    C --> D{置信度评估}
    D --> E[无下推]
    D --> F[单条件下推]
    D --> G[全条件下推]
    E --> H[物理优化]
    F --> H
    G --> H
    H --> I{置信度阈值τ}
    I -->|conf ≥ τ| J[Key-Scan]
    I -->|conf < τ| K[Table-Scan]
    J --> L[LLM]
    K --> L
    L --> M[结构化结果]
```

**核心技术要点**：

1. **LLMScan 逻辑算子族**：LLMScan（无条件数据获取）和 Filter-LLMScan（带条件下推的数据获取）。LLMScan 是唯一与 LLM 交互的算子，其他算子（Selection/Projection/Join/Agg）在内存中执行，不涉及 LLM。支持三种下推策略：无下推 / 单条件 / 全条件。
2. **Table-Scan vs Key-Scan 物理算子**：Table-Scan 直接迭代式 prompt 获取所有属性值，利用上下文记忆提高召回；Key-Scan 先获取所有 Key 值，再对每个 Key 获取其他属性——类似 CoT 的分解推理，质量更高但 token 成本更大。Key-Scan 的第二步可并行化。
3. **基于 LLM 自身置信度的逻辑/物理优化**：**核心创新**。利用 LLM 的分类能力评估每个 WHERE 谓词的置信度（high/low），只下推 high 置信度的条件；同时评估整体置信度分数（0-1），超过阈值 τ 则用 Key-Scan，否则用 Table-Scan。用 LLM 自己的判断来优化 LLM 的查询计划。
4. **选择性属性检索**：分析查询的 SELECT 子句，只让 LLM 输出相关属性而非全表，减少 token 消耗。

### 3. 实验拆解

| 维度 | 内容 |
|------|------|
| **数据集** | 7 个数据集：Flight/Geo/World/Scholar（IK 内参知识）+ Movies/Presidents/Premier/Fortune（MC 上下文知识）+ Geo-Test（阈值校准） |
| **Baseline** | NL（自然语言提问）、SQL（直接 SQL 提示语）、Galois_baseline（无优化的多步 cell-by-cell）、Palimpzest（RAG 场景） |
| **评价指标** | **质量**：F1-Cell、Cardinality、Tuple Constraint、AVG-Score（前三者平均）；**成本**：#Tokens、Time（秒） |
| **消融实验** | ✅ 分别评估逻辑优化（NO-PUSH vs ALL-PUSH vs CONFIDENCE）和物理优化（Table vs Key 选择） |
| **统计显著性** | ❌ 未报告方差/置信区间（但多数据集覆盖部分缓解此问题） |
| **复现条件** | 🟢 代码开源（GitHub: dbunibas/galois），使用公开 LLM API（GPT-4o mini / Together AI LLaMa） |

### 4. 关键数字

| Claim | 数字 | 条件 |
|-------|------|------|
| 质量提升 vs NL | 144% AVG-Score 提升（0.254 → 0.622） | LLaMa 3.1 70B，IK 场景 |
| 质量提升 vs SQL | 29% AVG-Score 提升（0.481 → 0.622） | 同上 |
| 物理优化准确率 | 75% 情况下选中最优物理计划 | GEO 数据集 |
| token 节省 vs baseline | Galois_baseline 的 11 倍 token 消耗（19.71M vs 1.72M） | 全数据集平均 |
| RAG 场景质量 | AVG-Score 0.711（vs Palimpzest 0.720，但 token 仅 1/11） | Premier + Fortune |
| 迭代次数 | 优化后平均 3.92 次 vs 无优化 6.82 次 | 全数据集 |

---

## ▎第三层 · 批判性评估

### 1. 假设检验

- **假设 1**：LLM 的 parametric knowledge 可以像数据库一样被"扫描"提取
  - 反例 / 边界：只有高频/常见数据容易被提取。论文自己的实验证实，关于 Venezuela 总统的数据质量远低于 USA 总统（AVG-Score 0.482 vs 0.862）——**实体流行度偏差（popularity bias）是一个根本性限制，论文没有深入讨论其影响**。
- **假设 2**：LLM 自身对查询条件的置信度估计是可靠的
  - 反例 / 边界：LLM 已知有过度自信问题（overconfidence）。论文用阈值 τ 来缓解，但 τ 的校准需要额外的 golden dataset（GEO-Test），且只对模型固定时才有效。
- **假设 3**：查询所涉及的知识全部或主要存在于 LLM 的预训练数据中
  - 反例 / 边界：对于时效性强的数据（如 2024 年的英超联赛），必须使用 RAG。论文确实考虑了 RAG 场景（MC），但性能明显低于 IK 场景。

### 2. 边界探查

- **方法适用边界**：仅适用于 LLM 中**存在且可提取**的结构化知识。对高度专业化、稀缺或时效性强的数据，效果急剧下降。
- **扩展性限制**：Key-Scan 需要为每个 Key 值调用一次 LLM——如果表有数千个 Key，token 成本爆炸。论文实验中最多的数据集也仅 267.5 个单元，未测试大规模场景。
- **复现难度**：🟢 代码已开源，使用公开 API，实验可复现。

### 3. 可信度评估

| 维度 | 评价 | 依据 |
|------|------|------|
| 实验公平性 | 🟢 较公平 | 4 种 baseline（含 NL/SQL/baseline/PZ），7 个数据集，IK + MC 双场景 |
| 结果显著性 | 🟢 显著 | 144% / 29% 提升 + 11× token 节省，数字一致且有实际意义 |
| 开源/可复现 | 🟢 全开 | 代码 + 数据公开，使用标准 API |
| 论文自身局限 | 🟡 一般 | 讨论了 popular 数据偏差，未深入讨论扩展性问题 |

### 4. 与同行工作的对比

- 比 **Cortex AISQL**（SIGMOD 2026）：Galois 是学术系统（开源），Cortex 是工业系统（闭源）。两者目标相反——Cortex 把 AI 算子**塞进**数据库，Galois 把 LLM **当作**数据库。
- 比 **Smart**（VLDB 2025）：Smart 优化传统 ML 谓词（可分析决策边界），Galois 优化 LLM prompt（黑盒，靠置信度估计）。Smart 的方法更精确，但适用面更窄（只对传统 ML）。
- 比 **GaussML**（ICDE 2024）：GaussML 硬编码 20+ ML 算子进数据库，Galois 不硬编码任何模型——只要 LLM API 能调用就行。Galois 更灵活但更慢。
- 在 **[你的课题]** 的坐标系中：Galois 属于 **DB4AI 路线中"LLM 作为数据源"的分支**。它与你的课题（数据出数据库→外部执行→写回）的区别在于：Galois 是把 LLM 当成数据源来查询（读），你的课题是把数据从 DB 送到 LLM 推理再到 DB（写）。

---

## ▎第四层 · 与你课题的连接

### 1. 可引用的观点（配精确位置）

> §1 Introduction：将 LLM 作为存储层，用 DBMS 处理查询执行。
> → 这是一个新颖的论点——与你的课题（DB 数据→外部 LLM→写回 DB）形成有趣对照。Galois 认为 LLM 是"存储"，你把它当"计算引擎"。

> §3.2 Key-Scan：两步扫描——先取 Key 再取值，可并行化第二步。
> → 这个"分解式 scan 策略"的思路可以借鉴到你的 batch construction 中——将大批量请求分解为可并行处理的子任务。

> §4 Logical/Physical Optimization：用 LLM 自身置信度做优化决策。
> → 思路有参考价值：在外部链路中，也可以用模型服务的 confidence/logprob 来决定 batch size 或路由策略。

> §5 Exp-8：Galois_full 在所有数据集中最接近最优计划。
> → 证明基于置信度的优化策略是有效的。

### 2. ⚠️ 不能过度引用的地方

- ❌ **不声称** "Galois 证明 LLM 可以替代数据库存储"——它只证明在小规模、常见数据上可以提取结构化信息
- ❌ **不声称** "Galois 的 Key-Scan 适合你的外部执行链路"——Key-Scan 是为 LLM 作为数据源设计的，你的场景是 LLM 作为推理引擎
- ❌ **不声称** "144% 质量提升在 AI_FILTER/AI_CLASSIFY 场景也成立"——实验针对的是语义数据提取，不是语义过滤

### 3. 对本课题的实际用途

| 用途类型 | 具体方式 | 优先级 |
|----------|----------|--------|
| ✅ 对照区分 | 开题 §2 中作为 DB4AI 路线的最新代表（2025 年，支持 LLM） | ⭐⭐⭐ |
| ✅ 空白论证 | 与 Cortex AISQL 一起说明"DB4AI 路线都在数据库内部"，外部执行是空白 | ⭐⭐⭐ |
| ⚠️ 设计参考 | Key-Scan 的分解思路可借鉴，但需适配外部推理场景 | ⭐⭐ |

### 4. 不足 → 你的机会

| 论文的不足 | 你的课题可能如何填补 |
|-----------|---------------------|
| LLM 用作"存储"（读数据），不涉及"推理后写回" | 你的课题是推理后写回——这是完全不同的方向 |
| Key-Scan 第二步可并行但受限于 Key 数量 | 你的 vLLM continuous batching 可以更高效地批量处理大批量请求 |
| 质量受限于实体流行度（USA 总统远好于 Venezuela） | 你的场景是推理而不是"已知知识检索"，不依赖 LLM 预训练数据 |
| 未考虑大规模表（实验最大 267 个单元） | 你的链路天然面对数据库表（万/百万级行），必须考虑扩展性 |

### 5. 可论文化的措辞

> 与 Satriani et al. [Galois, SIGMOD 2025] 将 LLM 视作可 SQL 查询的"存储层"不同，本课题研究的是另一方向——将 LLM 作为外部推理引擎，数据经由 Arrow/Ray 传输至模型服务完成推理后写回数据库。两条路线互补：Galois 回答"如何从 LLM 中读数据"，本课题回答"如何向 LLM 送数据再写回结果"。

> Galois 提出的基于模型置信度的物理算子选择策略（§4）与 Smart 的推理重写（VLDB 2025）共同表明，在 AI/ML 算子的执行优化中，"感知 AI 算子特征"是核心思路。本课题将这一思路延续到外部执行场景，提出 token-budget-aware batch construction 和 queue-adaptive flush。

### 6. 后续待读

- [ ] [[cortex_aisql_sigmod2026]] — 已精读，同方向产业对照
- [ ] [[smart_vldb_journal_2025]] — 已精读，更早的 ML 谓词优化
- [ ] **LOTUS** (Patel et al., 2024) — 另一语义查询系统，Galois 引用中提及
- [ ] **TAG** (Biswal et al., 2024) — "Text2SQL is Not Enough"，Galois 引用中提及

---

## 元反思

- **精读收益**：🟢 高（本文是同方向最新论文，与你的课题对照价值大）
- **是否纳入核心文献库**：是
- **计划复习周期**：4 周后复习
- **一句话自评**：理解到位。Galois 的"LLM 作为存储层"视角与你的"LLM 作为推理引擎"视角恰好对称，是开题 §2 中论证"DB4AI 路线多样性"的绝佳案例。

---

## 相关笔记

- [[cortex_aisql_sigmod2026]] — 同方向产业代表
- [[gaussml_icde2024]] — 同方向更早代表
- [[smart_vldb_journal_2025]] — 同方向 ML 谓词优化
- [[文献地图]] — 文献全景

---

## ▎图复审补充（2026-07-23，读图能力补遗）

> **配图**（论文原图，pypdfium2 渲染自一手 PDF；本环境无可靠视觉读图，以下数值以你的肉眼核对为准）：
>
> ![图 3 · Table-Scan vs Key-Scan 执行模型（无请求队列/batch 构造器/并发限制）](figs/galois_fig3.png)

**复审动机**：原笔记的方法拆解（§2）用 mermaid 流程图复述了 Galois 的优化逻辑，但**未按论文图号引用任何 figure**。本节通过直接读图（Fig 1/2/3/5/7-11），补回对课题定位 load-bearing 的图级证据，重点回答：Galois 是否做请求 batching / 跨查询组织 / 并发控制？答案是**否**——这恰好框出本课题的空白。

**图 1（p3）· NL/SQL/Galois 三种查询方法对比（概念图，非架构图）**
- **已有处理**：原笔记 §1 已说明 Galois 与 NL/SQL 的质量对比关系。
- **读图新发现**：（事实·from 图）Fig 1 把 Galois 画成一个**橙色黑盒**，只标注 "SQL → Galois → Prompt → LLM → Result"，**没有展示任何内部组件**（无 middleware、无 query executor、无 request pool、无 storage-layer 接口）。三种方法都是**单条 Prompt → 单次 LLM 调用 → 单个结果**的顺序流，图中不存在并行箭头、批处理标签或多行数据结构。（推断）这意味着 Fig 1 的定位是"质量对比的概念示意"，不是系统架构图——Galois 论文从头到尾**没有给出系统架构图**，这与 Cortex AISQL 等工业系统形成鲜明对照。
- **对课题含义**：可在开题报告中明确——Galois 的图级表达止于"单查询-单提示"，没有可被引用的执行链路架构；本课题的架构图填补的是"数据出库→组织→批量提交→写回"这一 Galois 未覆盖的链路表达空白。
- **证据层级**：事实（图本身可视内容）/ 推断（无架构图的整体判断）。

**图 2（p5）· 4 个逻辑计划（n1/p1/s1/c1，按谓词下推策略区分）**
- **已有处理**：原笔记 §2 第 1 点已描述三种下推策略（无/单/全），mermaid 图体现了计划选择逻辑。
- **读图新发现**：（事实·from 图）Fig 2 的 4 棵计划树都是**单查询的二元树**（scan → filter），根节点是单一查询输出。四种计划的区别仅在 `LLMScan` vs `Filter-LLMScan`（带条件下推）以及 post-filter 的有无；**没有任何跨查询、跨算子、跨行的组织结构**。（推断）Galois 的"优化"完全发生在**单查询的逻辑计划层**——选哪条计划、推哪个谓词——这正是 §4.4 中本课题"跨查询/跨算子上游组织"定位的反面坐标。
- **对课题含义**：Fig 2 可作为开题 §2 中"Galois = 单查询内逻辑优化"的图级证据。本课题的 token-budget/length-align/prefix-aware 是在 Galois 不触及的"多行如何打包成一个模型服务请求"层操作，二者**层次互补，不竞争**。
- **证据层级**：事实（图示计划树结构）/ 推断（层次互补判断）。

**图 3（p5）· Table-Scan (c1') 与 Key-Scan (c1'') 两个物理算子【最 load-bearing】**
- **已有处理**：原笔记 §2 第 2 点描述了 Table-Scan / Key-Scan 的语义，并称"Key-Scan 的第二步可并行化"。但**未引用 Fig 3 本身**。
- **读图新发现**：
  1. （事实·from 图）**Fig 3 中两个物理算子都没有任何请求队列、batch 构造器、并发限制器、actor pool 或多 LLM 调用在途的视觉元素**。Table-Scan 画成单次 "populate the table" 提示 → LLM 返回多行；Key-Scan 画成两阶段（阶段 1 取 keys，阶段 2 按 key 取 values）。
  2. （事实·from 图）**Fig 3 中 Key-Scan 阶段 2 用顺序箭头 1→2→3→4 画**，图本身**不展示并行**。
  3. （事实·from 论文文本 p10/p16）原文明确写"the operations in the second loop (lines 18-22) are parallelized"和"once the keys are obtained, the remaining values of the tuples can be retrieved in parallel"——所以原笔记"第二步可并行化"的判断**文本正确**。
  4. （推断·关键）并行性是算法**属性**（各 key 的取值调用彼此无上下文依赖，所以*可以*并行），**不是 Galois 实现或测量过的机制**。论文没有给出并行执行实验、没有并发度控制、没有 K_max、没有 queue-adaptive flush。Galois 止步于"这些调用理论上独立"，而**本课题回答的正是"如何调度这些独立调用"**。
- **对课题含义**：（1）Fig 3 是本课题 vs Galois 定位最有力的一张图——它把 Galois 的执行模型固定在"迭代式独立单提示"，与本课题"跨行组织 + 提交控制"形成直接对照。（2）可在开题报告中写：Galois 识别了 LLM 调用的独立性但**未提供提交控制层**；本课题在该层引入 queue-adaptive flush 与 K_max 动态控制。（3）**温和勘误原笔记 §4.2**：原笔记写"Key-Scan 的分解思路可以借鉴到你的 batch construction"——读图后修正：Key-Scan 的 key→value 分解服务于**推理质量**（CoT 式分步推理），**不是**服务于按 token 预算打包请求的吞吐目标；两者目标函数不同，借鉴价值宜表述为"分解思想"而非"batch 构造直接借鉴"。
- **证据层级**：事实（图示 + 文本原文）/ 推断（互补定位、勘误判断）。

**图 5（p9）· Table Prompt 语法模板 + Table-Scan 算法伪码**
- **已有处理**：原笔记未单独引用 Fig 5。
- **读图新发现**：（事实·from 图）Fig 5 的提示模板要求 LLM"populate the table with actual values"，**单次提示可返回多行**（"at each iteration multiple tuples could be extracted"）；算法用 `noNewTuples` 作停止条件的迭代循环。（推断·关键）这是**响应侧多行抽取**，不是**请求侧行间打包**：由 LLM 决定每次返回多少行，而非由客户端按 token 预算/长度/prefix 把多行逻辑请求组织成一个 batch。本课题的 token-budget 组织作用在**请求形成**侧，Galois 从不触及请求形成——它只调一个语义完整的 "scan this table" 提示。
- **对课题含义**：Fig 5 从请求格式层面证实 Galois 与本课题**作用层不同**：Galois 的"多行"是 LLM 自主返回的结果，本课题的"多行"是客户端按计算量预算主动组织的请求。开题报告可借此区分"响应侧多元组"与"请求侧多行打包"。
- **证据层级**：事实（提示模板原文）/ 推断（层差异判断）。

**图 7-11（p18-21）· 全部为质量/成本/阈值实验图**
- **已有处理**：原笔记 §3 实验拆解覆盖了质量与成本维度。
- **读图新发现**：（事实·from 图）Fig 7（按 logprob 阈值的 precision/recall）、Fig 8（质量 vs 查询复杂度）、Fig 9（token 成本 vs 复杂度）、Fig 10（τ 阈值选择）、Fig 11（AVG-Score vs 最优计划）——**5 张实验图全部围绕质量、token 成本、阈值校准**，**没有一张测量吞吐（rows/s、tokens/s）、尾延迟、在途并发数、队列行为或 batching 收益**。（推断）这证实 Galois 把 LLM 视为"质量-成本权衡的黑盒"，**完全不是作为需要被调度的服务**——本课题关注的 service_p99、inflight 时间序列、queue-adaptive 行为在 Galois 的实验空间里不存在。
- **对课题含义**：开题 §3 评价指标对比中可明确：Galois 的指标体系（F1-Cell / AVG-Score / #Tokens）属于"质量-成本"平面，本课题的指标体系（tokens/s / service_p99 / inflight）属于"吞吐-尾延迟"平面——**两套指标不重叠**，再次印证层次互补而非竞争。
- **证据层级**：事实（5 张图的主题）/ 推断（指标体系不相交）。

### 图复审一句话结论

读图后，Galois 与本课题的关系可精确表述为：**Galois 在单查询内做逻辑/物理计划选择（Fig 2/3/4），把每次 LLM 调用视为独立顺序提示（Fig 1/3/5），从未触及跨查询/跨算子的请求组织、并发控制或提交调度（Fig 7-11 无任何吞吐/并发实验）**。本课题填补的正是 Galois 执行模型之下、模型服务之上的**提交控制与数据组织层**——二者层次互补，不构成竞争关系。原笔记的判断方向正确，本节补充的是**图级证据**与**对 Key-Scan 并行性、batch 借鉴两处措辞的温和修正**。
