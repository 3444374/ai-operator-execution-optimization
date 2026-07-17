# 研究内容一：动态数据组织与批处理构造策略实验计划

整理日期：2026-07-16
对应研究内容：研究内容一
方法候选编号：A1.1-A1.6（详见 `research_design_catalog.md` §3）

> **2026-07-16 方向更新**：主场景从 AI_EMBED 转向 AI_COMPLETE（生成式 LLM 推理）。具体优化方法尚未锁定——动态 batching（token-budget / length-align / prefix-aware grouping）是当前重点探索方向，但静态 batch_size 参数穷举的结果仍作为 baseline 对照保留。以下内容中的实验骨架和参数矩阵为候选方案，最终消融设计将在 vLLM baseline 建立后根据实际数据确定。详细背景见 `research/knowledge_hub.md`。

---

## 0. 前置依赖（先读这个）

**本计划中所有实验必须在 vLLM + 小 LLM baseline 建立后才能产生论文可用的最终数据：**

```
前置：vLLM + Qwen2.5-1.5B 级 LLM baseline 建立（替代手动 HTTP endpoint）
前置：COPY + deferred index 写回 baseline 建立
前置：模型 batch scaling 曲线（§4 前置实验）

当前状态: GPU = 手动 HTTP endpoint、写回 = execute_values() UPSERT（仅预研可用）
```

**为什么**：在 suboptimal GPU/写回 baseline 上搜出来的"最优 batch_size"会因为 GPU 端或写回端的瓶颈位置不同而偏移。论文必须用 S 级 GPU + A 级写回上的 参数组合穷举 结果。

**过渡期**：可以用当前 baseline 跑一遍 研究内容一 来验证脚本、确认趋势、调试阶段拆解——但最终数据必须来自 P0 完成后的重跑。

---

## 1. 研究问题

在"数据库触发 → 外部执行"链路中，行数据如何组织为 Arrow RecordBatch、partition 和 Ray object，才能匹配下游 AI 算子的执行特征？什么情况下需要感知 workload 类型（EMBED/FILTER/COMPLETE）来选择数据组织策略？

---

## 2. 假设（Hypotheses）

每个实验段在跑之前必须先写清楚要推翻什么。**不是盲目扫参。**

| 编号 | 假设 | 待检验 | 对应实验段 |
|---|---|---|---|
| H1.1 | 固定 batch=64 在所有 workload 和规模下已经接近最优 | 能否被推翻？| §6.1 参数组合穷举 |
| H1.2 | batch_size 的最优值与 partition_count 独立（无交互效应）| 能否被推翻？| §6.1 参数组合穷举 |
| H1.3 | 不同 workload 类型（EMBED/FILTER/COMPLETE）的最优 batch_size 相同 | 能否被推翻？| §6.2 workload 对比 |
| H1.4 | selectivity 不应影响 batch 构造策略 | 能否被推翻？| §6.3 selectivity-aware |
| H1.5 | 模型自身的 batch scaling 在 batch=64 时已达到吞吐平台期 | 能否被推翻？| §4 前置实验 |

**最可能被推翻的假设决定 研究内容一 的核心贡献**：如果 H1.3（不同 workload 的最优 batch 相同）被推翻 → 研究内容一 有独立贡献；如果 H1.3 成立但 H1.5 被推翻（模型在 >64 时继续 scaling）→ 研究内容一 的贡献移到"模型 scaling 行为驱动 batch 选择"而非"workload 感知"。

---

## 3. 变量

| 变量 | 含义 | 取值范围 |
|---|---|---|
| `batch_size` | 每次提交到 GPU 的行数 | {8, 16, 32, 64, 128, 256, 512} |
| `partition_count` | Ray task/actor 数 | {1, 2, 4, 8} |
| `object_merge` | Arrow RecordBatch 的合并策略 | {none, coalesce_input, coalesce_output} |
| `workload_type` | AI 算子类型 | {EMBED (真实), FILTER (模拟), COMPLETE (模拟)} |
| `selectivity` (仅 FILTER) | 语义过滤的选择率 | {0.1, 0.3, 0.5, 0.8} |
| `text_length` (仅 COMPLETE) | 平均 token 数 | {short <128, medium 128-512, long >512} |

**关于 FILTER/COMPLETE 的诚实标注**（参照 Orca 合成权重的做法）：

| Workload | 当前状态 | 论文中标注 |
|---|---|---|
| EMBED | ✅ 真实 GPU embedding（all-MiniLM-L6-v2, 384d）| 真实 workload |
| FILTER | ⚠️ 模拟——用 embedding 相似度 + 阈值模拟布尔输出，selectivity 人工控制 | "simulated AI_FILTER with known selectivity" |
| COMPLETE | ⚠️ 模拟——用随机长度处理延迟模拟 token generation | "simulated AI_COMPLETE with controlled token length distribution" |

---

## 4. 前置实验：模型 batch scaling 曲线（P0c，必须在 研究内容一 所有实验之前跑）

### 4.0 研究问题

在讨论"batch_size 如何影响端到端延迟"之前，必须先搞清楚：**GPU 模型自身的吞吐是怎么随 batch_size 变化的？** 如果模型在 batch=32 就饱和了，那讨论 batch=256 毫无意义。

### 4.0 假设

H1.5：模型自身的 batch scaling 在 batch=64 时已达到吞吐平台期。

### 4.0 方法

```
脱离数据库/Ray 链路，直接用模型推理：
  model = SentenceTransformer("all-MiniLM-L6-v2")
  texts = [random text of length ~200 chars] × N

  batch_size ∈ {1, 2, 4, 8, 16, 32, 64, 128, 256, 512}
  N = max(batch_size) × 10（确保有足够多 batch）
  
  每 batch_size: 跑 20 个 batch，忽略前 5 个（warm-up），取后 15 个的中位数
  指标: T_per_batch, T_per_row, rows/s

预计耗时: ~30 分钟（不需要数据库、不需要 Ray）
```

### 4.0 输出

- 一条 `batch_size → rows/s` 曲线（X=batch_size, Y=吞吐）
- 标注吞吐平台期的起始 batch_size
- 如果平台期在 batch=32：研究内容一 讨论区间应聚焦 8-128，256/512 只有验证价值
- 如果平台期在 batch=256：研究内容一 有更大 tuning space，batch 选择对 GPU 利用率影响更大

**这条曲线是 研究内容一 所有讨论的前提。画好了才能解释后续所有实验里 batch_size 的影响。**

---

## 5. Baseline 对照

| 编号 | 描述 | 级别 | 来源 |
|---|---|---|---|
| **A1.1** | 固定策略 Baseline（coalesced vs fine 互相对照）| 合理默认 | 已有 |
| **D1** | Fixed Partition + Fixed Batch（Daft/Spark 默认，不做 workload 感知）| B 级 | Daft 文档 + Spark SQL Tuning |

---

## 6. 实验矩阵

### 6.1 参数组合穷举：建立静态最优 baseline

**假设**：H1.1（固定 batch=64 已最优）、H1.2（batch_size 和 partition_count 独立）。

```
batch_size      ∈ {8, 16, 32, 64, 128, 256, 512}
partition_count ∈ {1, 2, 4, 8}
object_merge    ∈ {coalesce_output}  # 当前已知最优
──────────────────────────────────────────
总组合: 7 × 4 = 28
每组合: 3 次重复（Ray 重启、warm-up 1 次不计入）
总运行: 84 次

固定条件（P0 完成后）:
  - GPU: vLLM / Ray Serve（S 级 baseline）
  - 写回: COPY + unlogged staging + deferred HNSW index（A 级 baseline）
  - 数据规模: 16384 行
  - Workload: AI_EMBED（真实）
```

**输出**：联合最优的 `(batch_size*, partition_count*)` = 研究内容一 的 A 级 baseline。同时检验 H1.2（是否存在交互效应——某些 batch_size 在特定 partition_count 下表现异常）。

### 6.2 Workload 对比

**假设**：H1.3（不同 workload 的最优 batch_size 相同）。

| Workload | batch_size | partition_count | 数据规模 | 标注 |
|---|---|---|---|---|
| EMBED | 参数组合穷举 最优 × 3 | 参数组合穷举 最优 × 3 | 1024, 4096, 16384 | ✅ 真实 |
| FILTER | selectivity ∈ {0.1, 0.5} × 参数组合穷举 | 参数组合穷举 最优 | 4096, 16384 | ⚠️ 模拟 |
| COMPLETE | text_length ∈ {short, long} × 参数组合穷举 | 参数组合穷举 最优 | 1024, 4096 | ⚠️ 模拟 |

每种组合 3 次重复，Ray 重启，warm-up 1 次不计入。

**如果 H1.3 被推翻**（不同 workload 的最优 batch 不同）→ 研究内容一 核心发现成立。
**如果 H1.3 成立**（所有 workload 下 batch=64 都最优）→ 研究内容一 的贡献变为"验证了固定策略的鲁棒性"，workload-aware 的增量价值需重新评估。

### 6.3 Selectivity-Aware 策略（当 FILTER 场景可用时）

**假设**：H1.4（selectivity 不应影响 batch 构造策略）。

| selectivity | 假设最优策略 | 为什么 |
|---|---|---|
| < 0.2 | 小 batch (32)、多 partition | 大部分行被过滤，小 batch 减少 GPU 浪费 |
| > 0.5 | 大 batch (128)、单 partition | 大部分行都过，大 batch 省 invocation 开销 |

**对照**：同 selectivity 下，固定 batch=64 作为基线。

---

## 7. 指标

| 指标 | 测量方法 | 论文参照 |
|---|---|---|
| **端到端延迟** | `time.perf_counter()` 从 DB fetch 开始到 writeback 结束 | vLLM/Orca 的端到端 serving latency |
| **阶段拆解** | DB fetch → Arrow build → GPU request wall → fan-in → writeback | TurboVecDB 的 HNSW 层级拆解思路 |
| **吞吐 (rows/s)** | `total_rows / T_e2e` | vLLM 的 requests/second |
| **Ray object 数** | `ray.objects()` 计数 | 诊断指标 |
| **GPU 利用率** (如有) | vLLM 可采集；手动 HTTP endpoint 无此指标 | vLLM 的 GPU utilization |

**关键**：不报"coalesced 比 fine 快 13.4×"这样的单点数字，而是画 `batch_size → T_e2e` 全景曲线，让 reviewer 看到全工作点。

---

## 8. 消融设计

对 A1.2（Workload-Aware Partition）的消融：

| 消融项 | 做法 | 要检验什么 |
|---|---|---|
| 规则表 vs 固定策略 | A1.2 规则表 vs A1.1 参数组合穷举 最优固定值 | 规则表在 workload 变化时是否优于固定策略？ |
| 规则表 vs 随机 | A1.2 规则表 vs 随机选配置（5 次取中位数）| 排除"随便选也能中"——规则表必须好于随机 |

---

## 9. 结果展示图

| 图号 | 内容 | 类型 | 论文参照 |
|---|---|---|---|
| Fig_RC1_0 | 模型 batch scaling 曲线：batch_size → rows/s | 折线图（前置实验）| — |
| Fig_RC1_1 | batch_size → T_e2e 曲线（不同 partition_count 各一条线）| 折线图 | vLLM 的吞吐-延迟曲线 |
| Fig_RC1_2 | 三 workload 的阶段拆解并排柱状图 | 堆叠柱状 | TurboVecDB 的层级拆解 |
| Fig_RC1_3 | selectivity → T_e2e（固定策略 vs workload-aware）| 折线图 | Orca 的多模型尺度图 |

---

## 10. 统计规范（参照 vLLM/Orca 标准）

| 要求 | 做法 |
|---|---|
| **重复次数** | 每组配置 3 次（参数组合穷举）。核心发现（被推翻的假设）额外补到 5 次 |
| **集中趋势** | 取**中位数**（不取平均值——系统实验的临时 outlier 会拉偏平均值）|
| **离散度** | 报告 IQR（四分位距），5 次以上报告标准差 |
| **Ray 状态重置** | 每次重复之间 `ray stop` → `ray start`，避免内存缓存/对象复用 |
| **数据库状态** | 每次重复之间 TRUNCATE 目标表，确保写入量一致 |
| **Warm-up** | 每组配置先跑 1 次 warm-up（不计入结果），后面 N 次计入 |
| **随机种子** | 数据生成固定 seed（`random.seed(42)`），确保不同配置跑同一批数据 |

---

## 11. "When does it NOT help?" 边界验证

每个边界条件必须对应一个**可跑的实验点**，不是空洞的自省。

| 边界条件 | 验证实验 | 期望结果 |
|---|---|---|
| workload 特征在运行前已知且不变 | 固定 1 种 workload，比较 "规则表选择" vs "固定 batch=64" | 差异 < 5% → 边界成立 |
| 数据量 < 500 行 | 256 行规模下，比较 batch_size ∈ {8, 32, 64, 256} | 各配置 T_e2e 差异 < 10% → 边界成立 |
| GPU 模型对所有 batch_size 吞吐几乎恒定 | 看 §4 前置实验的 batch scaling 曲线 | 如果平台期从 batch=8 开始 → batch_size 选择不重要 |

---

## 12. 运行检查清单

- [ ] P0c: 模型 batch scaling 曲线（§4）完成
- [ ] P0a: vLLM/Ray Serve 接入完成
- [ ] P0b: COPY + deferred index 写回 baseline 确认
- [ ] P1: 参数组合穷举（batch_size × partition_count）在 P0 完成后重跑，确立 `(batch_size*, partition_count*)`
- [ ] P1: 三 workload（EMBED + FILTER/sim + COMPLETE/sim）完成
- [ ] P1: 阶段拆解数据可以画 Fig_RC1_2
- [ ] P2: selectivity-aware 策略对照（当 FILTER workload 可用时）
- [ ] §11 的边界验证实验点完成
- [ ] 所有结果 CSV 保存在 `experiments/results/rc1/`
- [ ] 每个图标注：数据来源、排除 warm-up、硬件/模型/数据库版本、重复次数、取中位数还是平均值
