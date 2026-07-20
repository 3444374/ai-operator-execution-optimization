# 研究内容一：动态数据组织与批处理构造策略实验计划

整理日期：2026-07-16
对应研究内容：研究内容一
方法候选编号：A1.1-A1.6（详见 `archive/research_design_catalog.md` §3，已归档）

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

## 2.5 分组策略设计空间：按相似度分还是按均衡分

### 2.5.1 问题定义

Token-budget 策略确定"每个 batch 放多少 token 总量"（batch 边界），但**不决定"哪些行放入同一个 batch"**（分组策略）。分组策略的选择直接影响：
- 每个 batch 内的 prefill 时间同质性
- vLLM chunked prefill 的 prefill-decode 交错效率
- 异构 actor pool 的路由可行性
- 与 prefix-aware grouping 的兼容性

### 2.5.2 两种分组策略

| 策略 | 机制 | 示例（token budget = 4096） |
|---|---|---|
| **A: Length-Align** | 按 token 长度相似度分组，短的和短的在一起，长的和长的在一起 | Batch 1: [50, 60, 45, 55, …] × 80 行 ≈ 4000 tok；Batch 2: [3500, 4000, 3800] ≈ 11300 tok（可能超过 budget，需单独处理） |
| **B: Bin-Packing** | 混合不同长度，使每个 batch 的总 token 量尽量接近 budget | Batch 1: [50, 3500, 500] ≈ 4050 tok；Batch 2: [4000, 60] ≈ 4060 tok |

**关键区分**（来源：2026-07-20 chunked prefill 交叉分析）：
- **A 操作的是"batch 内的同质性"**——batch 之间差异大，batch 内部差异小
- **B 操作的是"batch 间的均衡性"**——batch 之间差异小，batch 内部差异大

### 2.5.3 两种策略在 vLLM Chunked Prefill 下的行为差异

vLLM chunked prefill 的调度器采用 **decode-priority** 策略（事实，来源：vLLM 官方文档 v0.4.2+）：每轮迭代优先调度 decode 请求，剩余 token 预算分配给 prefill chunk。

**方案 A（Length-Align）的行为**（推断）：
- 短 batch：所有请求 prefill 快速完成 → 全部进入 decode → decode 阶段有多请求并发
- 长 batch：所有请求 prefill 都很大 → prefill 被 chunked 分步执行 → **没有短 decode 请求可交错** → chunked prefill 的 "prefill-decode 混合" 优势减弱
- 如果短 batch 和长 batch 到达 vLLM 的时间错开，内部队列只有同类请求 → 失去混合调度的多样性

**方案 B（Bin-Packing）的行为**（推断）：
- 每个 batch 天然混合长短请求 → 提交后，短请求第一个 chunk 就完成 prefill 进入 decode，长请求继续跨 chunk prefill
- vLLM 调度器在后续 iteration 中：decode（来自短请求）+ prefill chunk（来自长请求）在同一 forward pass 中混跑
- **这正好是 chunked prefill 设计的最优场景**：compute-bound prefill 与 memory-bound decode 交错

### 2.5.4 两种策略的 Fatal Flaw

| 策略 | Fatal Flaw | 触发条件 | 验证方式 |
|---|---|---|---|
| A: Length-Align | 数据单峰分布 → 退化为随机分组 | 数据集中 > 80% 行集中在同一 token 长度区间 | 实验前画 token 长度分布直方图，确认多峰或长尾 |
| B: Bin-Packing | 极端 outlier 稀释优势 | 存在单行 token 量 > budget → 独占整个 batch，其他行与 length-align 无异 | 检查 P99/P50 token 比；如果 max > 2× budget，bin-packing 退化为 "outlier 独占 + 其余正常打包" |

### 2.5.5 与异构 Actor Pool 和 Prefix-Aware Grouping 的交互

| 交互对 | 兼容性 | 说明 |
|---|---|---|
| Length-Align × 异构 Actor Pool | ✅ **天然兼容** | 短 batch → 普通 actor，长 batch → 高容量 actor，路由清晰 |
| Bin-Packing × 异构 Actor Pool | ❌ 冲突 | 所有 batch 特征相同，无法按特征分池——分池路由失去意义 |
| Length-Align × Prefix-Aware | ✅ **可叠加** | 先按前缀分组（最大化 APC 命中率），再按长度子分组 |
| Bin-Packing × Prefix-Aware | ⚠️ 冲突 | 为均衡 token 量可能拆散同前缀的行 → 降低 APC 命中率 |

### 2.5.6 实验策略建议

**主推方案 B（Bin-Packing）作为 RC1 主策略**（推断，待实验验证）：
- 与 vLLM chunked prefill 的 prefill-decode 交错机制天然协同
- 优化目标清晰："每个 batch 的 GPU 计算量均衡"
- 文献依据：Orca 的 selective batching、vLLM 的 continuous batching 本质上都在混合不同长度的请求

**方案 A（Length-Align）保留为消融对比**：
- 在异构 actor pool 场景下（§5.3 或后续实验）：length-align + 分池路由 vs bin-packing + 统一路由
- 在 prefix-aware 联合实验中：length-align + prefix-aware 两级分组 vs bin-packing-only

**新增假设**：

| 编号 | 假设 | 待检验 | 对应实验段 |
|---|---|---|---|
| H1.6 | Bin-packing 分组在统一 actor pool + vLLM chunked prefill 下的端到端吞吐优于 length-align 分组 | 能否被推翻？| §6.1 扩展 |

### 2.5.7 语义安全边界：行内 prompt 不可拆分

**红线**（事实，来源：2026-07-20 vLLM chunked prefill deep-research 验证）：
> 将一份逻辑完整的 prompt 手动拆分成多条独立 vLLM 请求 → KV cache 隔离、上下文断裂、输出语义错误。vLLM 的 `--enable-chunked-prefill` 是引擎内部 token 级分片（数学等价），与手动 request 级拆分是完全不同的机制。

**对本实验的约束**（推断）：
- **每行数据 = 一个独立完整的 vLLM 请求**。Token-budget 策略决定的是 "多少行合并为一个 batch"，不是 "如何切割一行内的 prompt 文本"
- 如果某单行的 prompt token 量超过模型 context window（如 Qwen2.5-1.5B 的 32K），**禁止在上游 Daft/Ray 层自动切分该行的 prompt 内容为多条请求**
- 超长单行的处理方式：① 在数据准备阶段截断（truncate）到 context window 内；② 或将超长行标记为单独处理（独占一个 batch，不做拆分）；③ 或从数据集中排除

**实验前检查清单**（添加到 §12）：
- [ ] 确认数据集中每行的 prompt 是自包含的（self-contained），行间无语义依赖
- [ ] 确认所有单行的 token 量 < 模型 context window（32K for Qwen2.5-1.5B）
- [ ] 如果存在超长行：明确处理策略（truncate / 独占 batch / 排除），并记录在实验报告中

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
| `grouping_strategy` | 如何选择哪些行放入同一 batch | {random (baseline), length_align, bin_packing} |
| `token_budget` (仅 grouping ≠ random) | 每个 batch 的目标 token 总量上限 | {1024, 2048, 4096, 8192}（根据模型 context window 调整）|

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
| 数据集中存在超过 context window 的单行 | 检查 max(token_count) 是否 > 模型 context window（32K for Qwen2.5-1.5B）| 如有 → 预处理截断或排除；**禁止在 Ray 层自动拆分单行内容为多条请求**（会导致语义断裂，参见 §2.5.7） |
| 分组策略与 chunked prefill 的交互 | length_align vs bin_packing 在 `--enable-chunked-prefill` on/off 下的对比（仅 V0；V1 强制开启）| bin_packing 在 chunked prefill on 时优势更大（prefill-decode 天然混合） |

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
- [ ] **语义安全检查**（来自 §2.5.7）：每行 prompt 自包含、行间无语义依赖
- [ ] **语义安全检查**：max(token_count) < 模型 context window，超长行的处理策略已明确
- [ ] **分组策略检查**：数据集的 token 长度分布直方图已画出，确认分布特征（多峰/单峰/长尾）→ 据此决定 length_align 是否有区分度
- [ ] **Chunked Prefill 状态**：确认实验使用的 vLLM 版本及 chunked prefill 是否开启，记录在 CSV 的 `server_version` 字段中
