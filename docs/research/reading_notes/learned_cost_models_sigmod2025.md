---
type: paper-note
tags:
  - deep-reading
  - paper/learned-cost-models
  - sigmod2025
  - cost-estimation
  - query-optimization
  - learned-cost-model
  - hybrid-cost-model
  - ranking-metrics
  - training-data-bias
  - rc4-reference
aliases:
  - "Heinrich Learned Cost Models (SIGMOD 2025)"
status: 精读完成
read_date: 2026-07-27
---

# 精读笔记：How Good are Learned Cost Models, Really? Insights from Query Optimization Tasks (SIGMOD 2025)

---

## ▎第一层 · 基本信息

| 字段 | 内容 |
|------|------|
| **论文** | Roman Heinrich, Manisha Luthra, Johannes Wehrstein, Harald Kornmayer, Carsten Binnig. *How Good are Learned Cost Models, Really? Insights from Query Optimization Tasks.* Proc. ACM Manag. Data, Vol. 3, No. 3 (SIGMOD 2025), Article 172, 27 pages. |
| **来源级别** | CCF-A 会议（SIGMOD 2025）。作者来自 TU Darmstadt + DFKI Darmstadt + DHBW Mannheim。 |
| **链接** | arXiv:2502.01229 / 开源代码：github.com/DataManagementLab/lcm-eval / 实验数据：osf.io/rb5tn |
| **阅读日期** | 2026-07-27 |
| **状态** | 逐字精读完成 |
| **相关论文组** | 学习型代价估计 / 查询优化 / hybrid cost model / 训练数据偏差 / 排序指标 |

### 一句话核心结论

7 个 SOTA Learned Cost Model 在预测精度（Q-error）上全面超过 PostgreSQL 传统代价模型，但在 Join Ordering、Access Path Selection、Physical Operator Selection 三个核心优化任务上**没有一个能真正提升查询计划质量**——PostgreSQL 传统模型反而更优。根因有三：(1) 预测精度 ≠ 优化质量（排序和选择比点估计重要）；(2) 训练数据只包含传统优化器选出的近优计划，LCM 从未见过真正差计划；(3) 丢弃传统专家知识是错的，把 PostgreSQL 代价估计作为 LCM 输入特征效果显著。

`#learned-cost-model` `#query-optimization` `#SIGMOD2025` `#hybrid-cost-model` `#ranking-metrics` `#training-data-bias` `#RC4`

---

## ▎第二层 · 论文结构分析

### 1. 问题拆解

| 问题 | 论文的回答 |
|------|-----------|
| 要解决什么痛点？ | 近年大量 LCM 论文只报告预测精度（Q-error），极少评估 LCM 在实际查询优化任务中的效果。预测得准不代表能选对计划。 |
| 之前的方法为什么不够？ | 传统评估策略只评估单候选计划的预测精度，不考虑多候选计划间的排序和选择——而这恰恰是查询优化的核心任务。 |
| 论文的**核心论点** | LCM 的准确率提升没有转化为优化质量的提升，因为 (a) 中位精度高但尾部误差大，(b) 训练数据有系统性偏差（只含近优计划 + timeout 截断），(c) 丢弃了传统代价模型中数十年积累的专家知识。 |
| 它的**关键假设** | (1) 用 PostgreSQL v10.23 和 v16.4 作为传统 baseline 代表足够；(2) JOB-Light/IMDB/Baseball/TPC-H 四个数据集能覆盖典型查询优化场景；(3) 30s timeout 是合理的训练数据收集约束；(4) exhaustive enumeration（非 DP）能隔离枚举策略的影响，纯测代价模型的选择能力。 |

### 2. 方法拆解

**2.1 LCM 分类学（Taxonomy）**

论文将 LCM 按 5 个维度分类：
- **输入特征**：SQL String vs Query Plan vs Cardinalities vs Data Distribution vs DB Cost Estimates
- **查询表示**：Flat Vector vs Graph-based（是否利用 parent-child 结构）
- **数据库依赖**：DB-specific（需 per-DB 训练）vs DB-agnostic（zero-shot 泛化到新数据库）
- **模型架构**：Regression Tree / Deep Sets / Tree-structured NN / Neural Units / Transformer / GNN

**2.2 选中的 7 个 LCM**

| 模型 | 架构 | 输入特征 | DB 依赖 | 年份 |
|------|------|---------|---------|------|
| Flat Vector | LightGBM | Flat vector (per-operator cardinality sums) | DB-agnostic | 2009 |
| MSCN | Deep Sets | SQL String + Sample Bitmaps | DB-specific | 2019 |
| End-To-End | Tree-structured NN | Query Plan + Cardinalities | DB-specific | 2019 |
| QPP-Net | Neural Units (per-operator MLP) | Query Plan + Cardinalities + DB Costs | DB-specific | 2019 |
| QueryFormer | Tree Transformer | Query Plan + Histograms + Bitmaps | DB-agnostic | 2022 |
| Zero-Shot | GNN | Query Plan + transferable features | DB-agnostic | 2022 |
| DACE | Transformer + GNN | Query Plan + DB Costs | DB-agnostic | 2024 |

**2.3 三个评估任务**

1. **Join Ordering**（§4）：JOB-Light benchmark（IMDB，70 SPJA queries），exhaustive enumeration 所有 join permutation
2. **Access Path Selection**（§5）：SeqScan vs IndexScan，在 Baseball/IMDB/TPC-H 三数据集上跨不同 selectivity 测试
3. **Physical Operator Selection**（§6）：Hash Join vs Sort Merge Join vs Index Nested Loop Join，300 queries × 3 join types

**2.4 新引入的评估指标（这是论文的核心方法论贡献）**

| 指标 | 定义 | 测量什么 |
|------|------|---------|
| **Selected Runtime (S_R)** | LCM 会选中的计划的实际执行时间 | 选出的计划到底快不快 |
| **Surpassed Plans (S_P)** | 选中计划超过多少比例的候选计划 | 相对排名（100%=最优） |
| **Rank Correlation (ρ)** | Spearman 秩相关系数 | 预测排名与真实排名的一致性 |
| **Max Under-/Overestimation** | 最大低/高估因子 | 选错计划的概率 |
| **Balanced Accuracy (BA)** | (TPR+TNR)/2 | 在类别不平衡时的分类正确率（access path） |
| **Pick Rate (p)** | 选中最优物理算子的比例 | 物理算子选择的正确率 |

作者明确指出："accuracy alone is not sufficient to evaluate cost models for query optimization... only the combination of multiple metrics can help to understand the overall behavior."

### 3. 实验拆解

#### 3.1 Join Ordering (§4)

| 维度 | 内容 |
|------|------|
| **数据集** | JOB-Light benchmark，IMDB，70 SPJA queries，4表 join 共 120 permutations |
| **Baseline** | Scaled PG10、Scaled PG16（PostgreSQL 代价 + linear regression scaling） |
| **核心结果** | Scaled PG10 total S_R = 518s（最优 S_R = 446s，即 16.2% slow-down）；最差 LCM（QueryFormer）S_R = 830s；Zero-Shot 最好 = 530s |
| **关键发现** | Zero-Shot 在 Spearman ρ 上排名第一（优于 PG），但因同时存在严重的高估和低估，导致实际计划选择仍不如 PG |
| **完美基数实验** | 给 LCM 输入 actual cardinalities（非估算值）：Flat Vector S_R 从 588s → 548s，QPP-Net 从 765s → 644s。但最好的 LCM 仍不如使用估算基数的 PG10。而给 PG 完美基数后近乎最优（1.0% slow-down），LCM 却不行——说明 LCM 不能充分利用完美基数信息 |
| **DB-agnostic 优势** | 表现最好的 3 个 LCM 全是 DB-agnostic（Zero-Shot, Flat Vector, DACE），因为它们见过更多样的 workload |
| **SCAN 的失败** | MSCN 基于 SQL string 无法区分 join order——对所有 120 个 permutation 输出相同预测值，无法用于任何 plan-level 优化 |

#### 3.2 Access Path Selection (§5)

| 维度 | 内容 |
|------|------|
| **数据集** | Baseball (25 tables)、IMDB (8 tables)、TPC-H，numeric columns with range predicates |
| **核心结果** | 所有模型在 Balanced Accuracy 上都差——最好的 Scaled PG10/16 和 DACE 约 BA=0.64（1.0 为完美，0.5 为随机猜）；许多 LCM 恒选 IndexScan 不论 selectivity |
| **关键发现** | 训练数据中 90% 是 SeqScan（因传统优化器偏好），IndexScan 只出现在确实有利的 10% 查询中。LCM 学到的是"IndexScan 总是好"——这是典型的**训练数据偏差** |
| **基数影响** | 扫描查询的基数估计已很准（Q-error ≤ 1.05），所以换成完美基数对 access path selection 帮助不大 |
| **偏好分析** | 在 selectivity=0 到 1 的范围内，Flat Vector、Zero-Shot、QPP-Net、QueryFormer 恒选 IndexScan（恒定偏好）——说明没有理解 selectivity 的影响 |

#### 3.3 Physical Operator Selection (§6)

| 维度 | 内容 |
|------|------|
| **数据集** | 100 queries × 3 datasets（IMDB, Baseball, TPC-H），每 query 3 种 join（HJ, SMJ, INLJ）= 900 predictions |
| **核心结果** | 没有人接近最优。但 DACE 在 IMDB 上 pick rate p=82%（vs PG10 的 60%），TPC-H 上也略优。Selected runtime 上 PG 仍然略优 |
| **关键发现** | LCM 对 INLJ 有系统性过度偏好（QueryFormer 95% 选 INLJ，Zero-Shot 85%），原因同上——训练数据中 INLJ 只在它确实快时出现，LCM 学到了"INLJ=快"但没有学到"INLJ 慢的情况" |
| **PostgreSQL costs 的贡献** | 从 DACE 和 QPP-Net 中移除 PostgreSQL cost 特征后，DACE 的 pick rate 从 82%→44%（IMDB），证明 hybrid 特征至关重要（R4） |
| **增加索引后** | 在 filter columns 上加索引改变了最优算子分布。PG 模型性能严重退化（仍偏好 HJ），而 LCM 因为恰好偏好 INLJ 反而变好——这是"蒙对的"，非真正理解 |

#### 3.4 训练数据多样化实验 (§7 R3)

在 Access Path Selection 中，用 500 条强制双路径执行（每条 query 强制执行一次 SeqScan + 一次 IndexScan）的数据 fine-tune LCM 后：
- 多数列的 Balanced Accuracy 提升
- 总 Selected Runtime 最多降低 45%
- **Zero-Shot fine-tuned 首次超过 Scaled PG10**（95s vs 116s）→ 证明 LCM 可以超越传统模型，前提是训练数据正确

---

## ▎第三层 · 批判性评估

### 论文优势

1. **实验设计严谨**：20 个真实数据库、10,000 queries/db、3 次执行取平均、3 个 seed 平均预测、PG hint 强制计划选择——每个环节都有控制
2. **指标创新**：Selected Runtime、Surpassed Plans、Spearman ρ、Balanced Accuracy、Pick Rate——这套指标为"代价估计应该怎么评估"立了标杆
3. **根因分析透彻**：不只是说 LCM 不行，而是解释了为什么不行——训练数据偏差、尾部误差、缺乏传统知识
4. **可复现**：全开源（代码 + 模型 + 数据）

### 局限与边界

- **仅限 SPJA queries**（Select-Project-Join-Aggregate），不含子查询、窗口函数、递归 CTE
- **仅测试 3 种 join algorithm**（HJ/SMJ/INLJ），未覆盖 bitmap scan、parallel scan 等
- **LCM 实现在 PG 外部**——模型跑在 Python 里，不是集成进优化器内部。虽然这不影响"代价模型质量"本身的判断，但论文未讨论 inference latency 问题
- **训练数据规模固定**（10,000 queries/db），未探索更大规模的影响
- **未讨论 LCM 的计算开销**：inference time、training time 对比 PG 代价模型可忽略不计，但对生产系统仍有意义
- **Fine-tuning 实验仅 access path 一项**，未扩展到 join ordering 和 physical operator selection

### 与外界的对话

- 与 Leis et al. (VLDB 2015) "How Good Are Query Optimizers, Really?" 一脉相承——都是"把 SOTA 拿来老实测一测，发现没那么好"
- 与 Lehmann et al. (VLDB 2024) "Is Your Learned Query Optimizer Behaving As You Expect?" 形成互补——后者发现 LQO 的评估策略有偏
- 论文明确指出 DACE 和 QPP-Net 使用 PostgreSQL costs 作为输入——这种 hybrid 设计是最有前景的方向

---

## ▎第四层 · 与课题连接

### 对本课题代价估计（RC4/§6.1）的直接启示

**1. 排序指标的必要性（R2 → 项目已计划补充）**

论文的核心论点是"预测精度 ≠ 选择质量"，并引入了 Spearman ρ、Surpassed Plans 等排序指标。本课题代价估计 README 中已列出 Spearman / pairwise accuracy / Top-K precision 三项待补充——这篇论文为这些指标提供了**最强方法论支撑**。

**具体迁移**：在 `estimate_operator_cost.py` 中补充排序指标时，可以引用本文的 Definition 3-4 作为方法论依据。我们的场景（70 个配置组中选择最优配置）与论文的"从多个候选计划中选择最优"在结构上完全同构。

**2. Hybrid 架构（R4 → 项目可立即实施）**

论文发现"把传统代价估计作为 LCM 输入特征"（DACE/QPP-Net 的做法）是最有效的设计模式。本课题可以类比：

```
传统公式（类比 PG cost）：
  E2E_base = (prompt_token_count + estimated_output_tokens) / estimated_throughput + fixed_overhead

Hybrid（类比 DACE）：
  Ridge(features + E2E_base)  → 学习传统公式无法捕获的偏差
```

当前 Ridge 的 15 个特征中缺少一个显式的"传统公式估计值"——加上它可能显著改善 R² 和跨 seed 稳定性。

**3. 训练数据偏差（R3 → 提示当前 profile 数据的潜在问题）**

论文发现 LCM 只在近优计划上训练导致对差计划的代价估计系统性偏差。本课题的 283 行 profile 数据是否有类似问题？
- 当前数据全部来自正式实验的 `status=ok` 行——没有包含"失败/超时"的配置
- 如果后续需要估计"差配置"的代价（例如编排时需要判断某个计划是否太慢），当前模型可能低估差配置的 E2E
- Mitigation：在后续收集中有意加入一些"已知慢"的配置变体

**4. 简单模型足够（R1 → 印证当前 Ridge 选择）**

Flat Vector（最简单的 LightGBM on fixed-size vector）在三个任务中经常排名前三。这说明对于代价估计，模型复杂度不是瓶颈——特征工程和训练数据质量才是。当前 Ridge（161 行）的选择是正确的。

**5. DB-agnostic 的优势（→ 跨 workload 泛化的依据）**

DB-agnostic LCM 在 join ordering 中表现最好。本课题的 grouped hold-out（按配置组而非行做 split）正是评估这种跨配置泛化能力——论文的实验设计验证了这种评估策略的合理性。

### 不能直接迁移的地方

- **论文评估的是传统 SQL 算子的代价**（scan/join/aggregate），不涉及 AI 算子（LLM inference）。AI 算子的代价特征不同——最主要的不确定性来自输出长度而非基数估计
- **论文的 Selected Runtime 指标需要 exhaustive enumeration**——我们有 70 个配置组，但在线编排不可能穷举所有候选计划。实际使用时需要配合某种候选生成策略
- **论文的 30s timeout 偏差**在我们的场景中对应什么？如果 profile 数据只含成功运行（status=ok），是否对 OOM/timeout 的配置盲视？

### 可引用的观点

- "High accuracy in cost is not sufficient: LCMs need to fulfill other properties such as reliable ranking and selection of plans" (§1 Key Insight 1) → 直接支撑项目排序指标的补充
- "Don't throw away, what we know" (§1 Key Insight 3) → 支撑 hybrid 架构
- "Simple model architectures like Flat Vector often perform relatively well, making it questionable whether very complex architectures are necessary" (§7 R1) → 支撑 Ridge 选择的合理性
- "DB-agnostic LCMs often outperformed DB-specific models" (§7 R1) → 支撑 grouped hold-out 评估策略

### 不能过度引用的地方

- 不能把本文的结论直接写成"LCM 永远不如传统模型"——论文在 §7 R3 fine-tuning 实验中明确展示了 LCM 可以超越 PG（Zero-Shot fine-tuned 95s vs PG10 116s）
- 不能把本文的 SQL 算子代价估计结论直接套用到 AI 算子代价估计——论文没有涉及 LLM inference
- 论文中 PostgreSQL cost model 的系数是通过 linear regression scaling 得到的——这不是标准的 PostgreSQL 用法，而是为了与 LCM 做公平比较的 normalization
