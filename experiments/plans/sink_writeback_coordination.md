# 写回工程参考

整理日期：2026-07-16

> **2026-07-17 口径更新**：写回已从独立实验阶段降为实验设置中的工程细节（PostgreSQL + pgvector + COPY + deferred index baseline）。不作为独立研究内容或实验阶段。本文保留原始设计推演作为工程参考。
对应：端到端验证（不作为独立方法贡献）
方法候选编号：A3.1-A3.7（详见 `archive/research_design_catalog.md` §5，已归档）

> **2026-07-16 更新**：本验证实验不作为独立方法贡献。核心目标：采用工程最优写回方案作为 baseline，确认上游优化收益是否被持久化阶段吞噬。具体写回优化方法（worker-direct writeback 等）仅作为候选储备，当前大概率不做。以下内容中的 B 系列工程实验和三路架构对比保留为可选增强对照。详细背景见 `research/knowledge_hub.md`。

---

## 0. 前置依赖（先读这个）

**本实验应在研究内容一和研究内容二的最优配置确定后运行：**

```
前置：vLLM + 小 LLM baseline 建立
前置：研究内容一 动态 batching 最优策略确定
前置：研究内容二 自适应提交最优策略确定

当前状态: 写回 = execute_values() UPSERT + logged table + online index（仅预研可用）
```

**为什么**：拿 "未优化的 driver UPSERT" 当 baseline 来证明 worker-direct 好——那是 strawman。Worker-direct 的公平对照是 **最优 driver-side 写回配置**（= A3.1 的工程最优版 = B 系列结果）。

---

## 1. 研究问题

在数据库触发的外部 AI 执行链路中，GPU worker 产生的 embedding/分类/生成结果如何以最优批量和模式写回 PostgreSQL/pgvector（或 Lance）？driver fan-in 后统一写回、worker 各自写回、queue-worker 解耦写回——三种模式各自的适用场景是什么？

**核心假设**：driver fan-in 后统一写回（当前默认）在多数场景下不是最优——因为 fan-in 本身有同步开销，且大 batch 写入在 PostgreSQL 中有最优点；worker-direct 写回避开了 driver 瓶颈但丢失了写入批量的规模效益；最优选择取决于 GPU 端产出速率和写回速率的比值。

---

## 2. 假设（Hypotheses）

| 编号 | 假设 | 待检验 | 对应实验段 |
|---|---|---|---|
| H3.1 | `execute_values()` UPSERT 在 batch insert 场景下与 COPY 的性能差异 < 2× | 能否被推翻？| §4 B1 |
| H3.2 | WAL 对 batch insert 的影响可忽略 | 能否被推翻？| §4 B2 |
| H3.3 | 在线建索引的开销对 batch write 影响不大 | 能否被推翻？| §4 B3 |
| H3.4 | driver fan-in 统一写回在所有场景下不劣于 worker-direct 写回 | 能否被推翻？| §5.1 三路对比 |
| H3.5 | pgvector 是对所有写回场景最优的 sink | 能否被推翻？| §5.2 sink 对照 |

**最可能被推翻的假设决定 研究内容三 的核心贡献**：如果 H3.1 被推翻（COPY >> UPSERT）→ B 系列本身就是有价值的 baseline 贡献；如果 H3.4 被推翻（worker-direct > driver）→ 研究内容三 核心发现成立，但必须在最优 driver baseline 上证明。

---

## 3. 变量

| 变量 | 含义 | 取值范围 |
|---|---|---|
| `write_mode` | 写回架构模式 | {driver_fanin, worker_direct, queue_worker} |
| `B_write` | 写回 batch size（每次 INSERT/COPY 的行数）| {32, 64, 128, 256, 512, 1024} |
| `sink_type` | 存储目标 | {pgvector(vector(384)), Lance/Parquet, JSON text (baseline)} |
| `index_strategy` | HNSW 索引构建策略 | {online (边插边建), deferred (写完再建)} |
| `table_type` | 表类型 | {logged, unlogged} |
| `upsert_method` | 写入 SQL 方法 | {execute_values UPSERT, COPY + INSERT ON CONFLICT} |
| `concurrent_writers` | 并发写回的 worker 数 | {1, 2, 4} |

---

## 3. Baseline 对照

| 编号 | 描述 | 级别 | 来源 |
|---|---|---|---|
| **A3.1** | Driver fan-in + `execute_values()` UPSERT（当前方式）| 合理默认 | 已有 |
| **W1** | COPY + unlogged staging + deferred HNSW index | A 级（工程最优）| PostgreSQL §14.4 + pgvector Issues |
| **W2** | TurboVecDB 的 io_uring + 空间感知插入（若 pgvector 版本支持）| S 级 | TurboVecDB (VLDB 2025) |
| **W4** | pgai-style Queue-Worker 解耦写回 | B 级 | pgai Vectorizer |

---

## 4. 前置实验：B 系列——写回工程 baseline 确认（必须最先做）

**这是整个 研究内容三 的前提。不跑 B 系列，所有 研究内容三 方法对比用的 baseline 都是 suboptimal。**

### B1: UPSERT vs COPY

```
upsert_method ∈ {execute_values UPSERT, COPY + INSERT ON CONFLICT}
B_write ∈ {64, 128, 256, 512, 1024}
table_type = logged
index_strategy = online（边插边建，当前方式）
────────────────────────────────────────────────
总组合: 2 × 5 = 10
每组合: 3 次重复，每次 re-create 表
总运行: 30 次

要推翻的假设: "execute_values UPSERT 在 batch insert 场景下已经足够好"
```

### B2: Logged vs Unlogged

```
table_type ∈ {logged, unlogged}
B_write = B1 找出的最优值
upsert_method = B1 找出的最优方法
index_strategy = deferred（先写后建索引）
────────────────────────────────────────────────
总组合: 2
每组合: 3 次重复
总运行: 6 次

要推翻的假设: "WAL 对 batch insert 的影响可以忽略"
```

### B3: Online vs Deferred Index

```
index_strategy ∈ {online, deferred}
其他 = B1 + B2 找出的最优组合
HNSW params = {m=16, ef_construction=200}（pgvector 默认）
────────────────────────────────────────────────
总组合: 2
每组合: 3 次重复
总运行: 6 次

要推翻的假设: "在线建索引的开销对 batch write 影响不大"
```

**B 系列输出**：写回侧 A 级 baseline 的最优配置 `(upsert_method*, table_type*, index_strategy*, B_write*)`。

---

## 5. 实验矩阵

### 5.1 写回架构三路对比（核心方法实验）

```
write_mode ∈ {driver_fanin, worker_direct, queue_worker}
B_write ∈ {B_write*, B_write*/2, B_write*×2}（从 B 系列取最优值）
sink_type = pgvector(vector(384))
────────────────────────────────────────────────
总组合: 3 × 3 = 9
每组合: 3 次重复
总运行: 27 次

固定条件:
  - 数据规模: 16384 行
  - Workload: AI_EMBED
  - GPU: vLLM (S 级 baseline)
  - batch_size: 研究内容一 参数组合穷举 最优值
  - K_max: 研究内容二 参数组合穷举 最优值
```

**要推翻的假设**："driver fan-in 后统一写回是最自然的做法，worker-direct 不会更好。"

**注意**：不能拿 "A3.1 driver fan-in 未优化版"当 baseline 来证明 worker-direct 好——那是 strawman。Worker-direct 的对照是 **最优 driver-side 配置**（= A3.1 的工程最优版 = B 系列结果）。

### 5.2 Sink 对照（存储格式对比）

```
sink_type ∈ {pgvector(vector(384)), Lance, JSON_text}
write_mode = 5.1 找出的最优架构
B_write = 5.1 找出的最优值
────────────────────────────────────────────────
总组合: 3
每组合: 3 次重复
总运行: 9 次

数据规模: {1024, 4096, 16384} → 额外 ×3 = 27 次
```

**要推翻的假设**："pgvector 是对所有写回场景最优的 sink。"

### 5.3 并发写回度（仅 worker_direct/queue_worker）

```
concurrent_writers ∈ {1, 2, 4}
write_mode = {worker_direct, queue_worker}
────────────────────────────────────────────────
总组合: 3 × 2 = 6
每组合: 3 次重复
总运行: 18 次
```

---

## 6. 指标

| 指标 | 测量方法 | 论文参照 |
|---|---|---|
| **T_write** | 写回阶段墙钟时间（从第一个 INSERT 到最后一个 COMMIT）| TurboVecDB 的 index build time |
| **写回吞吐 (rows/s)** | `total_rows / T_write` | TurboVecDB 的 QPS |
| **索引构建时间** | `CREATE INDEX` 的 wall time（deferred 模式）| TurboVecDB: 减少 98.4% |
| **端到端 T_e2e** | 完整链路 | 所有论文 |
| **写回占比** | `T_write / T_e2e` | 动机指标——你的 36-54% 发现 |
| **fan-in 等待时间** | 最后一个 task 完成到 writeback 开始的时间差（driver 模式）| 诊断指标 |
| **磁盘写入量** | pg_stat_user_tables 的 n_tup_ins / 表大小 | 诊断指标 |

---

## 7. 消融设计

| 消融项 | 做法 | 期望发现 |
|---|---|---|
| 写回架构 vs 写回方法 | 同一 write_mode 下，UPSERT vs COPY | COPY 对所有架构都有收益，但不同架构的收益幅度不同 |
| B_write 的独立贡献 | 固定 write_mode，B_write 扫描 | 存在最优点，过大/过小均退化 |
| 写回架构 × B_write 交互 | write_mode × B_write 交叉 | worker_direct 的最优 B_write 可能与 driver_fanin 不同 |
| Sink 格式的独立贡献 | 固定 write_mode + B_write*，换 sink | Lance 在小规模下可能不如 pgvector（无 DB 开销） |

---

## 8. 结果展示图

| 图号 | 内容 | 类型 | 论文参照 |
|---|---|---|---|
| Fig_RC3_1 | B 系列结果：upsert_method × B_write 的热力图 | 热力图 | 展示二维交互 |
| Fig_RC3_2 | 三 write_mode 的 T_write 分组柱 + 阶段拆解 | 分组柱状 | TurboVecDB 的优化前后对比 |
| Fig_RC3_3 | write_mode × sink_type 的端到端阶段拆解 | 堆叠柱状 | FlexPushdownDB 的混合 vs 单策略 |
| Fig_RC3_4 | B_write → T_write 曲线（三种 write_mode 各一条线）| 折线图 | vLLM 风格的参数扫参图 |

---

## 9. "When does it NOT help?" 边界验证

每个边界条件必须对应一个**可跑的实验点**。

| 边界条件 | 验证实验 | 期望结果 |
|---|---|---|
| COPY + deferred index 已把写回占比压到 < 10% | B 系列完成后，检查 `T_write / T_e2e` | 占比 < 10% → 写回架构差异的绝对收益小（但相对收益仍可能有意义） |
| GPU 产出速率 << 写回速率 | 在单 endpoint + 低 batch_size 下，比较 driver vs worker-direct | 无显著差异 → GPU 是瓶颈，写回架构选择不重要 |
| 数据量 < 500 行 | 512 行规模下，比较 driver UPSERT vs driver COPY vs worker-direct | 各配置 T_write 差异 < 0.1s → 边界成立 |
| 无索引需求（只写不查）| 不加索引下，比较 online vs deferred "index"（即无索引） | 无差异 → deferred index 无收益 |

---

## 10. 统计规范（参照 TurboVecDB 标准）

| 要求 | 做法 |
|---|---|
| **重复次数** | B 系列 3 次；架构对比 3 次；核心发现额外补到 5 次 |
| **集中趋势** | 取**中位数** |
| **离散度** | 报告 IQR |
| **数据库状态重置** | 每次重复之间 DROP TABLE → re-CREATE（确保索引和表状态完全一致） |
| **Warm-up** | B 系列不需要 warm-up（每次 re-create 表本身就是冷启动） |
| **随机种子** | 数据生成固定 seed |
| **HNSW 参数** | 固定 `m=16, ef_construction=200`（pgvector 默认），变形实验时才改 |

---

## 11. 从 CCF-A 论文借鉴的评估原则

1. **先暴露瓶颈再讲优化**：B 系列先量化"当前做法 vs 工程最优"的差距，再进入架构对比（TurboVecDB 的 "SSD 利用率仅 1.98%"思路）
2. **公平 baseline 是核心**：不用未优化的 driver UPSERT 证明 worker-direct 好——必须用 B 系列结果作为 baseline（GaussML 的 "同一硬件、同一数据" 思路）
3. **层内拆解**：不只报总 T_write，还拆 T_copy、T_index、T_commit（TurboVecDB 的 HNSW 层级拆解思路）
4. **诚实报告"如果 COPY 已经解决了大部分问题"**：这是好消息，写进 §7——"工程最优 baseline 建立本身就是贡献"（FlexPushdownDB 承认两边各有边界的思路）

---

## 12. 运行检查清单

- [ ] P0: B1 (UPSERT vs COPY) 完成
- [ ] P0: B2 (Logged vs Unlogged) 完成
- [ ] P0: B3 (Online vs Deferred Index) 完成
- [ ] P0: B 系列结果确认 A 级写回 baseline 的最优配置
- [ ] P1: 三路写回架构对比（5.1）完成
- [ ] P1: Sink 对照（5.2）完成
- [ ] P2: 并发写回度（5.3）完成
- [ ] 所有结果 CSV 保存在 `experiments/results/rc3/`
- [ ] 每个图标注数据来源、排除 warm-up、表是否 re-create、HNSW 参数
