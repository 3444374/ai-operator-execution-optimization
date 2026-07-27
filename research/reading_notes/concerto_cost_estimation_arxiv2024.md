---
type: paper-note
tags:
  - deep-reading
  - paper/concerto
  - cost-estimation
  - query-performance-prediction
  - OLAP
  - DAG
  - GAT
  - TCN
  - resource-competition
  - rc4-reference
aliases:
  - "CONCERTO (arXiv 2024)"
status: 精读完成
read_date: 2026-07-27
---

# 精读笔记：CONCERTO — Complex Query Execution Mechanism-Aware Learned Cost Estimation (arXiv 2024)

---

## ▎第一层 · 基本信息

| 字段 | 内容 |
|------|------|
| **论文** | Kaixin Zhang, Hongzhi Wang, Kunkai Gu, Ziqi Li, Chunyu Zhao, Yingze Li, Yu Yan (Harbin Institute of Technology). *CONCERTO: Complex Query Execution Mechanism-Aware Learned Cost Estimation.* arXiv:2412.00749v2, Mar 2025. |
| **来源级别** | arXiv 预印本（cs.DB），哈尔滨工业大学。投稿状态未确认（可能在投 VLDB/ICDE）。 |
| **链接** | arXiv:2412.00749 |
| **阅读日期** | 2026-07-27 |
| **状态** | 逐字精读完成 |
| **相关论文组** | 学习型代价估计 / 查询性能预测 / OLAP 执行引擎 / 资源竞争建模 |

### 一句话核心结论

CONCERTO 针对带 SIMD 向量化、动态并行 pipeline、自适应查询执行（AQE）的高性能 OLAP 数据库（如 ClickHouse），提出三阶段解耦代价估计：Stage 1 为每种物理算子建立独立资源代价模型（OCP），Stage 2 用 GAT + 注意力机制校准并发算子间的资源竞争影响，Stage 3 用 TCN 自底向上聚合代价向量。在 ClickHouse + TPC-H/TPC-DS 上，mean Q-Error 分别为 1.46/1.84，模型仅 0.094 MB、推理延迟 4.2 ms，全面优于 QPPNet、QueryFormer、RAAL 等 baseline。

`#cost-estimation` `#OLAP` `#DAG` `#GAT` `#TCN` `#resource-competition` `#ClickHouse` `#RC4`

---

## ▎第二层 · 论文结构分析

### 1. 问题拆解

| 问题 | 论文的回答 |
|------|-----------|
| 要解决什么痛点？ | 现有 learned QPP 方法假设 tree-shaped query plan + static serial executor。但 ClickHouse/SparkSQL/DuckDB 等现代 OLAP 使用 SIMD 向量化算子、DAG 并行 pipeline、动态算子替换（如 hash join → merge join），导致：(1) 并发算子间资源竞争影响各自代价，(2) 执行前 EXPLAIN 得到的 pipeline 与运行时实际 pipeline 不同（动态修改），(3) end-to-end 模型难以捕获这些效应。 |
| 之前的方法为什么不够？ | QPPNet 假设 static serial plan trees；QueryFormer 虽用 Transformer 但同样不建模资源竞争和动态 pipeline；RAAL 考虑了 SparkSQL 资源分配但用 RNN 推理慢。没有方法同时解决"资源竞争校准"和"动态 pipeline 表达"。 |
| 论文的**核心论点** | 解耦三步走——per-operator cost prediction → resource competition calibration → tree aggregation——是应对复杂执行机制的正确架构。 |
| 它的**关键假设** | (1) 各算子的"孤立资源成本"可以通过一个轻量回归模型可靠估计；(2) 并发算子间的资源竞争可通过 GAT 的注意力权重捕获；(3) probe execution mode（只跑前几个 chunk）能足够准确地反映完整执行的 pipeline 结构（ClickHouse probe phase 的工程假设）；(4) 目标 DBMS 提供 serial executor mode 用于训练数据收集。 |

### 2. 方法拆解：三阶段架构

```
Query → Runtime Tracker (probe exec) → Dynamic Data-Flow Tree + OCP costs
  → Stage 1: OCPs predict per-operator resource cost vectors
  → Stage 2: Graph Constructor builds resource competition DAG
              GAT + ResAttn calibrates costs
  → Stage 3: Differentiable graph→tree conversion
              TCN aggregates bottom-up → final query latency
```

**两个关键工程组件**：

1. **Runtime Tracker**（低层运行时追踪器）：在 ClickHouse 上实现的探针，支持两种模式：
   - *Full-collection mode*：修改 serial executor，逐 chunk 记录每个算子的资源消耗（CPU/IO/memory）和 runtime features，用于训练 OCP
   - *Probe execution mode*：只启动前几个 chunk，快速获取 pipeline 结构和算子特征，同时捕获动态 pipeline 修改（如 join 算法被替换、新 MergeSort 被插入）——这些是 EXPLAIN 看不到的

2. **Graph Constructor**：将 probe execution 得到的数据流树构建为 DAG：
   - backbone = data-flow tree（父子关系）
   - 新增 edges = 资源竞争关系（同一资源组的并发算子之间）
   - 资源分组依据：CPU-bound / IO-bound / Memory-bound（通过静态代码分析 + benchmark profiling 分类）

**三阶段的机器学习设计**：

- **Stage 1 (OCP)**：每个物理算子类型一个轻量 MLP，输入为 chunk 级特征（input rows、cardinality、SIMD width、memory allocation 等），输出为资源代价向量（CPU time、IO time、memory peak）
- **Stage 2 (GAT + ResAttn)**：Graph Attention Network 在 data-flow tree + resource competition edges 上运行。核心创新是"资源注意力自适应机制"（ResAttn）——不同资源类型（CPU/IO/Mem）有各自独立的注意力权重矩阵，并由 data-flow 结构耦合调节
- **Stage 3 (TCN)**：将校准后的 DAG 以可微分方式转为树（保留父子关系，合并 resource competition 信息为节点特征），然后用 Temporal Convolutional Network 自底向上聚合，输出最终 query latency

### 3. 实验拆解

| 维度 | 内容 |
|------|------|
| **平台** | ClickHouse（实现了 Runtime Tracker + 修改 serial executor）；6× RTX A6000 (48GB) + 2× Xeon Silver 4210R + 504GB RAM |
| **数据集** | TPC-H (scale factor=1) + TPC-DS (sf=1)。约 10,000 queries/workload。训练/测试按模板分离（TPC-H: 4/17 模板=23.5% test；TPC-DS: 9/58 模板=15.5% test） |
| **Baseline** | GCN、TCN（end-to-end graph/tree model on pipeline DAG）、QPPNet、QueryFormer、RAAL |
| **指标** | Q-Error（mean / 50th / 90th / 95th / 99th / max） |
| **消融** | CONCERTO w/o ResAttn（移除资源注意力）、CONCERTO w/o OCP（移除 per-operator cost model，直接用端到端特征） |

**主实验结果（Table II）**：

| Method | TPC-H mean Q | TPC-DS mean Q |
|--------|-------------|---------------|
| GCN | 1.63 | 3.48 |
| TCN | 1.60 | 3.48 |
| QPPNet | 2.75 | 3.91 |
| QueryFormer | 1.60 | 2.59 |
| RAAL | 2.24 | 2.00 |
| **CONCERTO** | **1.46** | **1.84** |
| CONCERTO w/o OCP | 1.70 | 2.05 |
| CONCERTO w/o ResAttn | 2.09 | 2.48 |

- CONCERTO 在所有指标（mean/50th/90th/95th/99th/max Q-Error）上均优于所有 baseline
- TPC-DS（更复杂 workload，58 模板）上优势更大——因为资源竞争在复杂查询中更突出
- 消融分析：ResAttn（资源注意力）的贡献 > OCP（per-operator cost model）——移除 ResAttn 后 mean Q-Error 从 1.46 → 2.09

**性能对比**：
- 模型大小：0.094 MB（最小，含所有 OCP）
- 推理延迟：4.2 ms（最快，比 QueryFormer 的 7.1ms 快 41%）
- 训练速度：78.4 s/epoch（第二快，仅次于 QueryFormer 的 53.4s）

**基数误差鲁棒性**：
- =1.0 时 CONCERTO Q-Error 1.49（vs QueryFormer 1.59, RAAL 2.27）
- =1.5 时 CONCERTO Q-Error 2.59（vs QueryFormer 1.61, RAAL 2.30）
- 作者承认 CONCERTO 对较大基数误差的鲁棒性不如 QueryFormer，需改进

**Q-Error 按延迟分组分析**：
- CONCERTO 和 RAAL 在不同延迟组间保持相对稳定的 Q-Error（长尾分布较温和）
- QPPNet 在特定模板上 Q-Error 有显著尖峰——因为它是 plan-template 绑定的，泛化到新模板时崩溃

---

## ▎第三层 · 批判性评估

### 论文优势

1. **工程深度**：在 ClickHouse 上实现 Runtime Tracker、修改 serial executor、实现 probe execution mode——不只是"套模型"，而是深入 DBMS 内核
2. **解耦设计清晰**：OCP → GAT → TCN 三阶段，每个阶段解决一个独立问题，消融实验也证明每个阶段都有贡献
3. **模型小、推理快**：0.094 MB + 4.2ms 的推理延迟适合嵌入查询优化器

### 局限与边界

- **仅 ClickHouse 验证**：虽然声称可跨 DBMS，但 Runtime Tracker 是 ClickHouse 专用的，作者自己也说"future work to create cross-DBMS plugin"
- **arXiv 预印本，未确认 CCF 级别**：在投或在审，可能最终发表于 VLDB/ICDE 级别但尚不确定
- **不涉及 AI 算子**：建模的是传统 SQL 算子（scan/join/aggregate）在并行执行中的资源竞争，与 LLM 推理毫无关系
- **probe execution 假设**：假设前几个 chunk 的执行路径代表整个查询——这在 ClickHouse 中成立（因为动态修改主要发生在执行初期），但未必适用于其他 DBMS
- **OCP 训练数据收集重**：需要 full-collection mode 跑 serial executor——这需要 DBMS 支持 serial execution mode（ClickHouse 恰好有），迁移到其他 DBMS 需要等价机制
- **基数鲁棒性 gap**： =1.5 时 Q-Error 退化明显（1.49→2.59），比 QueryFormer 差，作者未给出改进方案

---

## ▎第四层 · 与课题连接

### 对本课题代价估计（RC4）的启示

**1. 解耦建模思路（可直接迁移）**

CONCERTO 的"per-operator cost → resource calibration → aggregation"三阶段是本课题代价估计可借鉴的框架：

```
当前（单一 Ridge）:
  features → Ridge → e2e_s（端到端，不区分阶段）

可参考的三阶段:
  Stage 1: per-component cost
           - DB fetch time = f(row_count, batch_size, ...)
           - vLLM inference time = f(prompt_tokens, output_cap, K_max, ...)
           - Writeback time = f(row_count, embedding_dim, ...)
  Stage 2: pipeline calibration
           - 多 endpoint 并发时的资源竞争（共享 vLLM 实例的 KV cache 争用）
           - 并发写入时的 I/O 竞争
  Stage 3: aggregation → e2e_s
```

不过这需要比当前 283 行 profile 更多的标注数据——当前 CSV 只有 total e2e_s，没有 per-stage breakdown。

**2. Resource Competition 建模对多 endpoint 场景的价值**

当扩展到多 endpoint/多 GPU 场景时，Stage 2 的 GAT + ResAttn 模式直接可参考。本课题的 shared-vLLM K_max 实验已经显示了并发对前台/后台的影响——CONCERTO 的 DAG + resource competition edges 提供了一种形式化建模方式。

**3. Probe Execution 的类比：profile 阶段的"前几个 chunk"**

CONCERTO 的 probe execution（只跑前几个 chunk 来推断完整 pipeline）与本课题的 profile 数据有类比关系：都是用少量执行来估计完整执行的特征。区别在于 CONCERTO 在 OLAP chunk 粒度，本课题在请求级别。

**4. 轻量模型的工程价值**

CONCERTO 0.094 MB + 4.2ms 推理 vs 当前 Ridge 161 行代码——两者都在追求"足够好且足够快"。CONCERTO 证明了解耦 + 轻量模型在代价估计场景下是正确选择。

### 不能直接迁移的地方

- **CONCERTO 建模 SQL 算子**（scan/join/aggregate）的资源竞争，不涉及 LLM inference。AI 算子的"资源竞争"是 GPU compute / KV cache / memory bandwidth，与 CPU/IO 竞争本质不同
- **Runtime Tracker 是 ClickHouse 专用基础设施**——本课题没有等价的 vLLM 内核级追踪器。vLLM 的 Prometheus 指标（running/waiting/KV cache usage）是粗粒度的，无法做到 per-operator/per-chunk 级别
- **三阶段需要 per-stage ground truth**——当前 profile CSV 只有 e2e_s 和 model_service_s，没有"DB fetch time""prefill time""decode time""writeback time"等细分标签

### 可引用的观点

- "Decoupling operator cost prediction from query performance prediction... provides more accurate forecasting for database systems with complex underlying query mechanisms" (§1 Contribution 1) → 支撑解耦建模方案
- "resource competition among operators is crucial to the accuracy of QPP tasks" (§VI-B) → 支撑多 endpoint 场景的资源竞争建模
- "the resource attention mechanism has the biggest improvement in the prediction accuracy" (§VI-B Ablation Study) → 资源注意力是最重要的设计元素

### 不能过度引用的地方

- CONCERTO 发表在 arXiv 预印本上，尚未被顶级会议接收——不能写成 CCF-A 论文
- 文中所有实验在 ClickHouse 上完成，不能声称"适用于所有 OLAP 数据库"
- 不能把 CONCERTO 的 DAG 资源竞争建模直接等同于 vLLM 并发请求的资源竞争——一个是 CPU/IO/Memory，一个是 GPU compute/KV cache
