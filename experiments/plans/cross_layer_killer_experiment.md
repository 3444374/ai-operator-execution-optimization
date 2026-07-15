# 跨层联合优化：Killer Experiment 实验计划

整理日期：2026-07-16
对应研究内容：跨层协同优化（论文核心 claim）
方法候选编号：AX.1-AX.3（详见 `research_design_catalog.md` §6）
评估方法论来源：vLLM (SOSP 2023)、Orca (OSDI 2022)、FlexPushdownDB (VLDB 2021)、TurboVecDB (VLDB 2025)、GaussML (ICDE 2024)

---

## 0. 前置依赖（先读这个）

**Killer Experiment 是本论文最后跑的实验。所有前置条件必须先满足：**

```
P0a: vLLM / Ray Serve 接入 → GPU 基线升级到 S 级
P0b: B 系列写回工程实验完成 → 写回基线升级到 A 级
P1a: RC1 Grid Search 完成 → 确定 BL1 的 B_gpu*, W*
P1b: RC2 K_max Sweep 完成 → 确定 BL1 的 K_max*
P1c: RC3 三路架构对比完成 → 确定 BL2 的 write_mode*, B_write*

当前状态: 以上全部待完成。当前所有实验数据均为过渡期数据，不能作为论文最终结果。
```

**为什么这个顺序不能乱**：BL3（Independent Best）是 RC1、RC2、RC3 各维独立最优值的拼装。如果任何一个维度的 grid search 不充分，BL3 就不是真正的"最优"——那你即使赢了 BL3 也证明不了核心 claim。

---

## 1. 研究问题

**论文的核心 claim**：GPU batch size（`B_gpu`）和数据库写回 batch size（`B_write`）的最优选择不是独立的——GPU 偏好大 batch（吞吐高）但大 batch 延迟了写回的开始时间（减少了 GPU-写回 pipeline overlap）；独立优化这两个参数会陷入**局部最优**，需要一个感知两者耦合代价的跨层联合决策模型。

**Killer Experiment 的问题**：独立最优组合（BL3：各岛各自最优配置的拼装）vs 联合最优（你的方法）——联合优化能否在**端到端吞吐-延迟曲线上**系统性地优于独立最优？

**反证条件**：如果 BL3 和你的方案的端到端差异 < 10%（且统计不显著），则核心 claim 不成立——跨层协同不是问题，各层独立优化足够。

---

## 2. 假设（Hypotheses）

**这些假设是整个论文的生死线。**

| 编号 | 假设 | 待检验 | 对应实验段 |
|---|---|---|---|
| **H_X.1** | BL3（Independent Best：RC1/RC2/RC3 独立最优拼装）和 Ours（联合代价模型优化）的端到端吞吐差异 < 10% | **必须被推翻** | §4.1 主实验 |
| H_X.2 | 写回瓶颈（B_write joint）的 joint 贡献 > GPU 调度（B_gpu joint）的 joint 贡献 | 基于动机发现（写回占 36-54%）| §5 消融瀑布 |
| H_X.3 | 联合代价模型的 R² > 0.85（能够准确预测 T_e2e）| 待验证 | §4.3 代价模型 |
| H_X.4 | 联合优化的收益在 ≥ 2/3 workload 类型上系统性地存在（非 EMBED 特例）| 待验证 | §6 跨 workload 泛化 |
| H_X.5 | Pipeline overlap（BL4）不能替代 joint optimization（Ours serial > BL4，Ours pipeline >> BL4）| 待验证 | §5 overlap 消融 |

**如果 H_X.1 没有被推翻**（Δ < 10%）：论文核心 claim 不成立。需要收紧为"联合优化提供统计显著的边际改进"。

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

**Phase 2（规则法）**：基于 grid search + regression 建立 `T_gpu(B_gpu)` 和 `T_write(B_write)` 的拟合曲线，然后用简单的 grid search 在二维空间中找到联合最优点 `(B_gpu_joint*, B_write_joint*)`。

**Phase 3（学习法，可选）**：用更复杂的模型（如 XGBoost 或小型 MLP）学习 `(B_gpu, B_write, W, mode, N, workload_type) → T_e2e` 的映射，替代 grid search 的穷举方式。这个不作为硕士论文必做，但如果规则法效果已经足够，可以讨论"学习法在什么情况下有价值"。

---

## 3. Baseline 对照

| 编号 | RC1 配置 | RC2 配置 | RC3 配置 | 来源 | 代表什么 |
|---|---|---|---|---|---|
| **BL1** | A1.1 coalesced batch（RC1 grid search 最优 B_gpu）| A2.3 自适应 K_max（tuned for GPU throughput）| A3.1 driver fan-in（默认写回）| vLLM/Orca 的最优 GPU 配置 + 不关心写回 | GPU 岛最优，不管写回 |
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
  BL3: (B_gpu*, K_max*, B_write*, mode*)      ← 各维独立 grid search 最优
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
| K_max joint 的贡献 | Ours vs Ours 但 K_max 固定为 RC2 最优静态值 | K_max 的 joint 选择在 workload 变化时贡献更大 |
| mode joint 的贡献 | Ours vs Ours 但 write_mode 固定为 driver_fanin | mode 选择是离散的——在什么条件下 joint cost model 选择 worker_direct？ |
| Pipeline overlap 的贡献 | Ours (serial) vs Ours (pipeline) vs BL4 (naive pipeline) | overlap 有帮助但不能替代 joint |
| 代价模型的贡献 | Grid search 穷举最优 vs cost model 预测最优 | cost model 预测的最优是否接近 grid search 的全局最优点？ |

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

**如果 Δ < 10%**：重新检查 BL3 是否真正独立最优（可能 RC1/RC2/RC3 的 grid search 不够细）。如果确认是最优且 Δ < 10%，则论文核心 claim 需要收紧为"联合优化提供统计显著的边际改进，对写回占比较高的场景有实用价值"。

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

- [ ] P0 (前置): RC1、RC2、RC3 各自的 grid search 完成，确立 BL1/BL2 的独立最优值
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
