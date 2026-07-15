# 精读笔记：Smart — SQL with ML Predicates (VLDB Journal 2025)

**论文**：Guo, Li, Hu, Wang. *In-database query optimization on SQL with ML predicates.* VLDB Journal, Vol.34(1), Article 12, 2025. DOI:10.1007/s00778-024-00888-3

**来源级别**：CCF-A 期刊论文（清华大学李国良团队）

**阅读日期**：2026-07-15

---

## 一句话核心结论

将 ML 模型视作黑盒 UDF 会导致查询优化器无法重写或成本优化包含 ML 谓词的 SQL 查询；Smart 系统通过推理重写→渐进式推理→成本最优推理三个模块，在 PostgreSQL 中实现最高三个数量级的查询加速。

---

## 解决的问题

SQL 查询中嵌入 ML 谓词（如 `WHERE classifier(x) = 'positive'`）时，传统数据库优化器将 ML 模型视为黑盒 UDF，存在三个挑战：

- **C1**：无法为重写优化生成有效的 SQL 谓词（不知道该 ML 模型等价于哪些经典 SQL 条件）
- **C2**：无法判断哪些推理出的 SQL 谓词值得保留（有些谓词求值开销大但剪枝能力弱）
- **C3**：无法在物理计划中做成本最优的谓词放置

---

## 核心方法：Smart 三模块

### 模块 1：Inference Rewrite（推理重写）— 对应 C1

分析 ML 模型的决策边界，生成**严格有效**（sound）且尽可能**紧致**（tight）的经典 SQL 谓词（范围条件、等值条件）。关键约束：生成的谓词绝不能剪掉 ML 模型会接受的行。

### 模块 2：Progressive Inference（渐进式推理）— 对应 C2

从模块 1 生成的候选谓词中，按剪枝能力/求值开销比率排序，渐进式选取谓词。当额外谓词的求值成本超过其预期剪枝收益时停止。

### 模块 3：Cost-Optimal Inference（成本最优推理）— 对应 C3

将选定谓词集成到物理执行计划中（索引扫描条件、过滤位置决策），最小化总查询成本。综合考虑传统谓词求值与剩余 ML 推理调用之间的交互。

---

## 实现与评估

- **实现平台**：PostgreSQL（优化器扩展）
- **测试基准**：JOB（Join Order Benchmark）、TPC-H、SSB（Star Schema Benchmark）、Flight
- **关键结果**：相比 SOTA baseline 最高 **1000×（三个数量级）** 加速

---

## 三个模块的关系

```
ML 模型决策边界分析
        ↓
[模块 1] 推理重写 → 候选 SQL 谓词集合
        ↓
[模块 2] 渐进式推理 → 筛选高性价比谓词子集
        ↓
[模块 3] 成本最优推理 → 最优物理执行计划
        ↓
PostgreSQL 执行器（索引扫描 + 过滤 + ML 推理调用）
```

---

## 本论文的不足（对本课题而言）

1. **数据库内优化**：整个方案在 PostgreSQL 优化器内部运行，不涉及外部执行系统
2. **无分布式执行**：不涉及 Ray task/actor 调度、模型服务并发
3. **不关心数据传输**：不研究 Arrow/RecordBatch 组织、object transfer
4. **不涉及写回**：论文关注 ML 谓词的评估优化，不关注结果持久化
5. **ML 谓词 vs AI 算子**：论文的"ML 谓词"是传统 ML 模型（分类器等），不是 LLM embedding/generation；Snowflake 的 AI 算子更接近本课题场景

---

## 对本课题的含义

**作为"数据库内 ML"路线的代表，明确本课题的差异定位。**

1. Smart 证明"ML 推理在 SQL 中的执行优化"是值得研究的问题（CCF-A 期刊论文）
2. Smart 走的是"数据库内核优化"路线——推理重写、谓词下推、物理计划选择——完全在 PostgreSQL 内部
3. 本课题走的是不同路线——不是优化"数据库内部如何省 ML 调用"，而是优化"数据出数据库之后如何在外部执行系统中高效批处理、调度、推理和写回"
4. 开题报告 §2.2 中应明确：Smart/GaussML/NeurDB 代表 DB4AI 路线（模型进数据库），本课题代表外部执行路线（数据出数据库再回来）
