# 耦合验证实验计划

整理日期：2026-07-16

> **2026-07-17 口径更新**：本文中的"跨层""RC3"等旧术语已统一。当前耦合验证范围为研究内容一（数据组织策略）× 研究内容二（提交控制策略）× 引擎级参数（Daft `into_batches`/`batch_size`/`max_concurrency`）。写回已降为实验设置。最新定义以 `AGENTS.md` §1 和 `PROJECT_OUTLINE.md` 为准。
对应：耦合验证——研究内容一和研究内容二的策略是否需要联合调优

> **2026-07-16 更新**：本文件用于验证两项策略是否需要联合调优。实验设计：① 分别独立搜索两项策略的最优配置后拼接 ② 联合 grid search 对比。联合显著优于拼接则说明需要联合调优，两者接近则分层独立优化即可。无论哪种结果，课题的核心贡献（上游调度优化）不受影响。具体方法尚未锁定，以下 BL 矩阵、代价模型和消融瀑布为候选实验骨架。

---

## 0. 前置依赖（先读这个）

**本实验应在研究内容一和研究内容二各自的最优策略确定后运行：**

```
前置：vLLM + 小 LLM baseline 建立
前置：研究内容一 动态 batching 最优策略确定
前置：研究内容二 自适应提交最优策略确定
```
P1c: 研究内容三 三路架构对比完成 → 确定 BL2 的 write_mode*, B_write*

当前状态: 以上全部待完成。当前所有实验数据均为过渡期数据，不能作为论文最终结果。
```

**为什么这个顺序不能乱**：BL3（Independent Best）是 研究内容一、二、三 各维独立最优值的拼装。如果任何一个维度的 参数组合穷举 不充分，BL3 就不是真正的”阶段级最优拼装”，该对照就不能用来分析阶段间耦合。

---

## 1. 研究问题

**当前主线问题**：把数据组织、模型服务调度和结果写回三个阶段分别调优后，端到端执行流程的耗时、吞吐、排队和写回占比是否整体改善？

**本增强实验的问题**：在阶段级调优已经完成的前提下，独立最优组合（BL3：各阶段各自最优配置的拼装）、naive pipeline 和端到端联合配置之间是否存在系统性差异？如果存在，说明阶段间耦合值得进一步建模；如果不存在，则论文可以收敛为分阶段剖析与阶段级流程调优。

**反证条件**：如果 BL3 和完整优化流程的端到端差异 < 10%（且统计不显著），则不应强行声称跨层联合优化是核心贡献；此时重点应放在分阶段剖析、阶段级调优和工程化流程优化。

---

## 2. 假设（Hypotheses）

**这些假设用于增强论证，不是当前开题主线的生死线。**

| 编号 | 假设 | 待检验 | 对应实验段 |
|---|---|---|---|
| **H_X.1** | BL3（Independent Best：研究内容一/二/三 独立最优拼装）和完整优化流程的端到端吞吐差异 < 10% | 若被推翻，可支持阶段间耦合论证 | §4.1 主实验 |
| H_X.2 | 写回瓶颈（B_write joint）的 joint 贡献 > GPU 调度（B_gpu joint）的 joint 贡献 | 基于动机发现（写回占 36-54%）| §5 消融瀑布 |
| H_X.3 | 联合代价模型的 R² > 0.85（能够准确预测 T_e2e）| 待验证 | §4.3 代价模型 |
| H_X.4 | 联合优化的收益在 ≥ 2/3 workload 类型上系统性地存在（非 EMBED 特例）| 待验证 | §6 跨 workload 泛化 |
| H_X.5 | Pipeline overlap（BL4）不能替代 joint optimization（Ours serial > BL4，Ours pipeline >> BL4）| 待验证 | §5 overlap 消融 |

**如果 H_X.1 没有被推翻**（Δ < 10%）：不要强行推进“联合优化优于独立最优”的主张，论文主线收敛为“端到端执行流程的分阶段剖析与阶段级调优”。

---

## 3. 联合代价模型

### 2.1 模型形式

```
T_e2e(B_gpu, B_write, W, mode, K_max) =
    T_fetch                                   # 从 DB 读取，近似常数
  + T_arrow(N, B_gpu)                         # Arrow RecordBatch 构建，与 batch 大小相关
  + T_gpu(N, B_gpu, W, K_max)                 # GPU 推理墙钟时间，B_gpu × W × K_max 三维
  + T_fanin(P, W, mode)                       # fan-in 等待时间，取决于 partition 数和写回模式
  + T_write(N, B_write, mode, sink, idx)      # 写回时间
```

**关键耦合项**：
- `T_gpu` 随 `B_gpu` 增大而降低（per-row latency 下降），但 `B_gpu` 增大会导致第一批结果到达写回阶段的时间推迟
- `T_write` 随 `B_write` 增大而降低（per-row 写回开销摊薄），但 `B_write` 过大会增加事务锁持有时间
- `T_fanin` 在 driver 模式下随 W（worker 数）增多而增大；在 worker-direct 模式下趋近 0
- Pipeline overlap：`B_gpu` 和 `B_write` 的相对大小决定 GPU-写回的 overlap 机会——B_gpu << B_write 时 GPU 产出频繁但每次量少，写回可以边收边写

### 2.2 实现方式

**Phase 2（规则法）**：基于 参数组合穷举 + regression 建立 `T_gpu(B_gpu)` 和 `T_write(B_write)` 的拟合曲线，然后用简单的 参数组合穷举 在二维空间中找到联合最优点 `(B_gpu_joint*, B_write_joint*)`。

**Phase 3（学习法，可选）**：用更复杂的模型（如 XGBoost 或小型 MLP）学习 `(B_gpu, B_write, W, mode, N, workload_type) → T_e2e` 的映射，替代 参数组合穷举 的穷举方式。这个不作为硕士论文必做，但如果规则法效果已经足够，可以讨论"学习法在什么情况下有价值"。

---

## 3. Baseline 对照

| 编号 | 研究内容一 配置 | 研究内容二 配置 | 研究内容三 配置 | 来源 | 代表什么 |
|---|---|---|---|---|---|
| **BL1** | A1.1 coalesced batch（研究内容一 参数组合穷举 最优 B_gpu）| A2.3 自适应 K_max（tuned for GPU throughput）| A3.1 driver fan-in（默认写回）| vLLM/Orca 的最优 GPU 配置 + 不关心写回 | GPU 岛最优，不管写回 |
| **BL2** | A1.1 coalesced batch（不做特殊优化）| A2.1 固定 K_max（不调优）| B 系列最优 COPY + deferred index + 最优 B_write | TurboVecDB + COPY 的组合 | 写回岛最优，不管 GPU |
| **BL3** | BL1 的 B_gpu, W | BL1 的 K_max, routing | BL2 的 write_mode, B_write, sink | 组合 BL1 + BL2 | **关键对照**：独立最优组合 |
| **BL4** | A1.1 coalesced batch | 固定 K_max | Pipeline overlap（边算边写，但 B_gpu / B_write 固定）| Ray Data streaming batch model (G4) | 只做 overlap，不做 joint optimization |
| **你的方法** | joint-cost-model 选择的 B_gpu | joint-cost-model 选择的 K_max | joint-cost-model 选择的 B_write, mode | 本文 | 联合最优 |

**最低必跑**：BL1, BL2, BL3, BL4, 你的方法。BL3 是最关键对照。

---

## 4. Killer Experiment 矩阵

### 4.1 主实验：Independent Best vs Joint Best

```
配置:
  BL3: (B_gpu*, K_max*, B_write*, mode*)      ← 各维独立 参数组合穷举 最优
  BL4: (固定 B_gpu, 固定 K_max, pipeline)       ← 只有 overlap，不优化参数
  Ours: (B_gpu_joint, K_max_joint, B_write_joint, mode_joint)  ← 联合 cost model

Workload × 数据规模:
  EMBED × {1024, 4096, 16384}
  FILTER (simulated, selectivity=0.3) × {4096, 16384}
  COMPLETE (simulated, token=medium) × {1024, 4096}
────────────────────────────────────────────────
总组合: 4 (配置) × (3 + 2 + 2) = 28
每组合: 5 次重复（核心实验需要更多重复）
总运行: 140 次
```

**核心指标**：每个配置的端到端吞吐-延迟曲线。不只是单点对比。

### 4.2 瓶颈迁移分析

```
对 BL1/BL2/BL3/BL4/Ours 五个配置，分别做阶段拆解：
  T_fetch / T_arrow / T_gpu / T_fanin / T_write

展示方式: 五个并排的堆叠柱状图（x = 配置，y = 时间，stacked = 阶段）
```

**要证明**：你的方案不是消除了某一个阶段瓶颈，而是将各阶段的时间重新分布——GPU 瓶颈和写回瓶颈被**联合缓解**，单一优化的天花板被打破。

### 4.3 代价模型准确性

```
对 28 个组合（4.1 的矩阵），分别：
  - 用你的 cost model 预测 T_e2e
  - 实测 T_e2e
  
展示方式: 散点图（x = 预测 T_e2e, y = 实测 T_e2e）+ R² + 偏差分析
```

---

## 5. 消融设计

| 消融项 | 做法 | 期望发现 |
|---|---|---|
| B_gpu joint 的贡献 | Ours vs Ours 但 B_gpu 固定为 BL1 最优值 | B_gpu 的 joint 选择有独立贡献，但可能 < B_write joint |
| B_write joint 的贡献 | Ours vs Ours 但 B_write 固定为 BL2 最优值 | B_write joint 贡献可能 > B_gpu joint（写回是主要瓶颈） |
| K_max joint 的贡献 | Ours vs Ours 但 K_max 固定为 研究内容二 最优静态值 | K_max 的 joint 选择在 workload 变化时贡献更大 |
| mode joint 的贡献 | Ours vs Ours 但 write_mode 固定为 driver_fanin | mode 选择是离散的——在什么条件下 joint cost model 选择 worker_direct？ |
| Pipeline overlap 的贡献 | Ours (serial) vs Ours (pipeline) vs BL4 (naive pipeline) | overlap 有帮助但不能替代 joint |
| 代价模型的贡献 | 参数组合穷举 穷举最优 vs cost model 预测最优 | cost model 预测的最优是否接近 参数组合穷举 的全局最优点？ |

**消融结果展示**：瀑布图（waterfall chart）——从 BL3 开始，逐步加入每个 joint 优化，展示累计收益。

---

## 6. 跨 Workload 泛化

```
每种 workload (EMBED, FILTER, COMPLETE) 分别跑：
  - BL3（该 workload 的独立最优配置）
  - Ours（该 workload 的联合最优配置）

额外: 一种 workload 的最优配置直接应用于另一种 workload（cross-test）
      → 验证 joint cost model 的跨 workload 泛化能力
```

**要检验的假设**：联合代价模型在不同 workload 类型下是否一致有效？还是只在 EMBED 场景有效？

---

## 7. 指标

| 指标 | 为什么重要 | 论文参照 |
|---|---|---|
| **端到端吞吐-延迟曲线** | 论文核心指标——展示全工作点 | vLLM/Orca 的标准做法 |
| **阶段拆解** | 证明瓶颈被联合缓解而非单一消除 | TurboVecDB 的层级拆解 |
| **代价模型 R²** | 证明你的 cost model 是准确的 | FlexPushdownDB 的 cost model 评估 |
| **P99 延迟** | 展示联合优化对尾部延迟的改善 | vLLM 的 P99 |
| **统计显著性** | 5 次重复 → 置信区间 → 证明 BL3 和 Ours 的差异是系统性的 | 基本科学要求 |

---

## 8. 结果展示图

| 图号 | 内容 | 类型 | 论文参照 |
|---|---|---|---|
| **Fig_KL_1** | 主结果：吞吐-延迟曲线（BL1/BL2/BL3/BL4/Ours 五条线）| 折线图，X=延迟约束, Y=吞吐 | vLLM Fig.6, Orca Fig.7 |
| **Fig_KL_2** | 瓶颈迁移：五配置的阶段拆解并排堆叠柱 | 分组堆叠柱状 | TurboVecDB 优化前后 |
| **Fig_KL_3** | 消融瀑布图：从 BL3 开始逐步加优化 | 瀑布图 | — |
| **Fig_KL_4** | 代价模型准确性：预测 vs 实测散点图 | 散点图 + R² | FlexPushdownDB cost model eval |
| **Fig_KL_5** | 跨 workload 泛化：三种 workload 的 BL3 vs Ours | 分组柱状 | Orca 的多模型图 |

---

## 9. 统计严谨性（参照 CCF-A 论文标准）

| 要求 | 做法 |
|---|---|
| **重复次数** | 核心实验（Killer Experiment）至少 5 次重复；其他 3 次 |
| **集中趋势** | 取中位数，不取平均值（系统实验中 outlier 会拉偏平均值）|
| **离散度** | 报告 IQR（四分位距）或标准差 |
| **Ray 内存状态** | 每次重复之间重启 Ray（`ray stop` → `ray start`），避免内存缓存效应 |
| **数据库状态** | 每次重复之间 TRUNCATE 表（写回实验）或 re-create 表（B 系列）|
| **Warm-up** | 每组配置先跑 1 次 warm-up（不计入结果），后续 N 次计入 |
| **随机种子** | 数据生成固定 seed，确保不同配置跑的是同一批数据 |

---

## 10. 成功标准（claim 成立的阈值）

| 条件 | 阈值 |
|---|---|
| Ours 端到端吞吐 > BL3 | **> 10%**（中位数差，且 5 次重复的 Mann-Whitney U p < 0.05）|
| Ours 的 P99 延迟 ≤ BL3 | 不能以牺牲延迟为代价换吞吐 |
| 消融瀑布显示 B_write joint 贡献最大 | 符合动机发现（写回是主要瓶颈）|
| 代价模型 R² | > 0.85（否则 cost model 不够准） |
| 跨 workload 泛化 | 至少 2/3 workload 上 Ours > BL3 |

**如果 Δ < 10%**：重新检查 BL3 是否真正独立最优（可能 研究内容一/二/三 的 参数组合穷举 不够细）。如果确认是最优且 Δ < 10%，则不再主张”联合优化优于独立最优”，论文重点回到端到端执行流程的分阶段剖析、阶段级调优和工程化验证。

---

## 11. "When does it NOT help?" 自检

- [ ] 如果 B 系列实验后写回占比 < 10% → 跨层协同的绝对收益空间缩小
- [ ] 如果 vLLM 接入后 GPU 侧剩余优化空间 < 10% → B_gpu joint 几乎无贡献，只靠 B_write joint
- [ ] 如果 GPU 产出速率 << 写回速率（GPU 是绝对瓶颈）→ B_gpu 和 B_write 的耦合几乎不存在
- [ ] 如果 workload 特征极其均匀 → 固定配置不会比自适应差多少

**这些不是失败条件**——它们是论文 §7.6 "When does our approach NOT help?" 的材料，证明你理解自己方法的边界。

---

## 12. 从五篇 CCF-A 论文提取的评估原则汇总

| 原则 | 来源论文 | 在本实验中的体现 |
|---|---|---|
| **曲线 > 单点** | vLLM, Orca | Fig_KL_1 吞吐-延迟曲线，不报单一数字 |
| **先暴露瓶颈再讲优化** | TurboVecDB | §4 动机用 coalesced mode 暴露 36-54% 写回瓶颈 |
| **同硬件公平 baseline** | GaussML | BL3 是你自己实现的独立最优，跑在同一 RTX 5070 上 |
| **承认两边各有边界** | FlexPushdownDB | §11 "When does it NOT help?" 诚实分析 |
| **消融揭示交互效应** | TurboVecDB, FlexPushdownDB | Fig_KL_3 瀑布图，每项优化的独立贡献 |
| **诚实报告局限性** | Orca（合成权重）| FILTER/COMPLETE 标注为 simulated workload |

---

## 13. 运行检查清单

- [ ] P0 (前置): 研究内容一、二、三 各自的 参数组合穷举 完成，确立 BL1/BL2 的独立最优值
- [ ] P0 (前置): B 系列实验完成，确立 A 级写回 baseline
- [ ] P0 (前置): vLLM 接入完成，确立 S 级 GPU baseline
- [ ] P1: Killer Experiment 主矩阵（4.1）完成
- [ ] P1: 瓶颈迁移分析（4.2）完成
- [ ] P1: 代价模型准确率（4.3）完成
- [ ] P2: 消融瀑布（§5）完成
- [ ] P2: 跨 workload 泛化（§6）完成
- [ ] 所有结果 CSV 保存在 `experiments/results/cross_layer/`
- [ ] 每个图标注：数据来源、硬件、模型、warm-up 策略、重复次数、集中趋势方法
- [ ] 论文 §7.6 写好 "When does our approach NOT help?"
