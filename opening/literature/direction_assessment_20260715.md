# 文献阅读总结与课题方向评估

评估日期：2026-07-15
基于：精读 5 篇核心论文 + 搜索审查 15 篇补充引用 + 产业系统 7 个

---

## 一、文献阅读经验总结

### 1.1 阅读方法

本次使用的方法是"问题驱动式系统阅读"：
1. **第一遍（扫读）**：看 abstract + introduction 末尾的 contribution + 实验表格的 baseline 对比，定位论文在文献地图中的位置
2. **第二遍（精读核心技术）**：逐段理解方法细节，重点关注"解决了什么问题、用了什么手段、有什么边界条件"
3. **第三遍（对照阅读）**：将多篇论文放在一起对照，找它们的共同假设、各自盲区和互补关系

### 1.2 关键发现

**最重要的发现不是某一篇论文的具体方法，而是一组论文共同揭示的"研究岛"格局**：

```
岛 1: DB4AI（数据库内核 AI 化）        岛 2: AI 推理服务            岛 3: AI 数据存储
Snowflake Cortex AISQL (SIGMOD '26)   vLLM (SOSP '23 Best)       Lance (arXiv '25)
Smart (VLDB J '25, 李国良组)          Orca (OSDI '22)              pgvector
GaussML (ICDE '24, 李国良组+华为)      Ray Data (arXiv '25)         Parquet
NeurDB (CIDR '25, Ooi组)              SGLang (NeurIPS '24)
LEADS (VLDB '24, Ooi组)
Galois (SIGMOD '25)
```

**每个岛内部已有很强的 CCF-A 论文，但三个岛之间几乎没有桥。**

### 1.3 经验教训

1. **不要只读一篇论文就定方向**。单看 Cortex AISQL 会觉得"AI SQL 算子优化已经被做完了"；单看 GaussML 会觉得"数据库内 ML 不需要外部执行"。只有多篇对照才能看出研究空白。
2. **产业论文（SIGMOD Industry Track）和学术论文要分开看**。Snowflake 论文用产业数据证明了问题存在，但它的方法受限于闭源系统，不能直接复用——这恰好是学术研究可以切入的地方。
3. **李国良组的论文是理解 DB4AI 路线的最佳入口**。Smart (VLDB J)、GaussML (ICDE) 和两篇 VLDB Tutorial 共同勾勒了 DB4AI 的全景——了解这个全景之后，才能准确定位本课题的差异。
4. **文献地图比文献清单更重要**。不是堆 40 篇参考文献就够，而是要让读者（导师、评审）清楚地看到每篇文献在"岛屿地图"中的位置，以及课题恰好落在空白处。

---

## 二、方向评估：是否需要调整？

### 2.1 当前课题定位

> 数据库 AI 负载的执行优化与调度研究

核心链路：数据库 → Arrow RecordBatch → Ray task/actor → GPU model service → fan-in → pgvector/Lance writeback

### 2.2 文献审查看法

**当前方向不需要根本调整，但需要在三个维度上做更精准的定位。**

#### 维度 1：与 DB4AI 路线的区分（最关键的定位问题）

**现状**：开题报告 §2.2 已提到 pgai/pgvector/PostgresML，但缺少对 GaussML、Smart、NeurDB 等 CCF-A 学术工作的引用和对照。

**风险**：如果评审看到"数据库 + AI + 执行优化"，第一反应可能是"GaussML/Smart 不是已经做了吗？"——必须主动区分。

**建议**：
- 在 §2.2 中加入 GaussML/Smart/NeurDB 作为"数据库内 ML"对照路线
- 明确本课题走的是不同的技术路线：不是"把模型拉进数据库"（DB4AI），而是"数据库触发后经外部系统执行 AI 再回来"
- 引用 pgai vectorizer worker 的架构形态说明外部执行路线的工程合理性

#### 维度 2：与 Snowflake Cortex AISQL 的关系

**现状**：开题报告已引用 Snowflake 作为产业需求证据，但 SIGMOD 2026 论文的三项优化技术尚未被充分讨论。

**风险**：评审可能认为"Snowflake 已经在数据库内解决 AI 算子优化了，外部执行没有优势"。

**建议**：
- 承认 Snowflake 的 AI-aware 优化是重要工作，但指出其闭源实现不可拆分阶段
- 论证外部执行链路有其不可替代的场景：多模型服务、异构 GPU、独立 scaling、与 pgai/vectorizer worker 架构一致
- 将 Snowflake 的三项优化技术（AI-aware 优化、模型级联、语义 Join 重写）作为本课题在外部执行链路中可类比借鉴的设计原则

#### 维度 3：vLLM/Orca/Ray Data 与本课题的关系

**现状**：开题报告 §2.3 主要讨论 Ray/Daft，未引用推理服务系统的 CCF-A 论文。

**风险**：评审可能认为本课题"不了解推理服务侧的最新进展"。

**建议**：
- 在 §2.3 中加入 vLLM (SOSP '23)、Orca (OSDI '22)、Ray Data Streaming Batch (Anyscale/UCB)
- 明确这些工作研究了 GPU 侧的内存/批处理/调度，但数据源和写回目标不在它们的研究范围内
- 本课题的独特位置恰在数据库侧和推理服务侧之间

### 2.3 方向调整建议

**不需要改题目。** 当前题目"面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究"准确覆盖了本课题的位置。

**需要调整的是论证结构**：

| 当前 §2 | 建议调整 |
|---|---|
| §2.1 只提 Snowflake 文档 | 加入 SIGMOD 2026 论文的技术细节和产业数据 |
| §2.2 只提 pgai/pgvector/PostgresML | 加入 GaussML、Smart、NeurDB、LEADS 作为 DB4AI 对照路线 |
| §2.3 只提 Ray/Daft 文档 | 加入 vLLM、Orca、Ray Data Streaming Batch |
| §2.4 研究空白表述较泛 | 用"三个研究岛"的精确框架：DB4AI、推理服务、AI 存储之间的空白 |

---

## 三、怎么做更合理

### 3.1 论证策略（开题报告 §2 改写）

建议按以下结构重写 §2：

```
2.1 数据库 AI SQL 算子：产业现状（Snowflake SIGMOD + BigQuery + Oracle）
    → 结论：AI SQL 算子已是工业现实，但内部执行不可见

2.2 数据库内 AI 推理：学术前沿（GaussML, Smart, NeurDB, LEADS）
    → 结论：DB4AI 路线已有大量 CCF-A 工作，本课题走不同路线

2.3 外部执行与模型服务：基础设施现状（Ray, Daft, vLLM, Orca, Lance）
    → 结论：各岛内部基础设施完善，但缺少跨岛协同

2.4 研究空白与本课题定位
    → 用"三岛模型"精确表述：没有现有工作同时覆盖
       ① 数据库侧数据出口与写回入口
       ② 外部数据组织/调度/执行
       ③ GPU 推理服务的动态状态
       ④ AI 数据存储格式
```

### 3.2 实验优先级

文献阅读后建议的实验优先级：

1. **当前链路 + pgvector(384) 写回对比**（已完成 ✅）
2. **worker-side writeback vs driver fan-in writeback**（下一步）——因为这个变量直接区分"外部执行+写回"与"数据库内 ML"
3. **多 endpoint / bounded in-flight 对照**（下一步）——因为这个变量区分"静态调度"与"感知模型服务状态"
4. **三类 workload 的 selectivity/token/prefix 对照**——验证方法不是只在 embedding 上有效

### 3.3 参考文献更新

从 23 条扩展到 40 条（详见 `opening/literature/ai_operator_literature_inventory.md`）：
- 新增 17 条外部文献（15 篇 CCF-A + 顶会论文，2 篇 arXiv）
- 标注 15 篇精读（均为 CCF-A 或顶会）
- 新增文献覆盖：SIGMOD 2026/2025/2024, VLDB 2024/2025, VLDB Journal 2025, ICDE 2024, SOSP 2023, OSDI 2022/2024, CIDR 2025

---

## 四、不能声称的结论

1. 不能说"现有研究没有关注数据库 AI 算子"——Snowflake SIGMOD 论文和 Smart/GaussML/NeurDB 已经充分证明这个方向被关注
2. 不能说"外部执行一定优于数据库内 ML"——两条路线的优劣取决于场景（GPU 集群规模、数据量、延迟要求）
3. 不能说"Ray/Daft/Lance 是数据库 AI 算子的标准执行方案"——Snowflake 和 GaussML 使用了完全不同的技术栈
4. 本课题的合理表述是："在数据库触发 AI workload 后经由外部系统执行并写回的场景中，数据组织、调度、推理和存储之间的协同优化尚缺乏系统研究"

---

## 五、下一步行动

1. ✅ 文献清单 → `opening/literature/ai_operator_literature_inventory.md`
2. ✅ 精读笔记 → `opening/literature/reading_notes/`
3. ⬜ 更新开题报告 §2 国内外研究现状
4. ⬜ 更新开题报告参考文献（23→40 条）
5. ⬜ 更新 `opening/literature/reading_list.md` 关联新文献
6. ⬜ 用户审阅后，同步飞书
