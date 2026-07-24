# 研究内容二：调度与提交控制策略实验计划

整理日期：2026-07-16

> **2026-07-17 口径更新**：本文中的"运行层"等旧术语已统一为当前口径。最新研究内容定义、优先级和边界以 `AGENTS.md` §1、`PROJECT_OUTLINE.md` 和 `research/knowledge_hub.md` 为准。写回已降为实验设置，不作为独立研究内容。
对应研究内容：研究内容二
方法候选编号：A2.1-A2.7（详见 `archive/research_design_catalog.md` §4，已归档）

> **2026-07-16 方向更新**：具体优化方法尚未锁定。K_max 扫描、routing 策略对比、adaptive vs static K_max 均为有效的候选优化手段。去中心化自适应提交（queue-adaptive flush）和 actor pool 分池路由是当前重点探索方向，但不排除其他策略。以下内容中的实验骨架为候选方案，最终消融设计将在 vLLM baseline 建立后根据实际数据确定。详细背景见 `research/knowledge_hub.md`。

---

## 0. 前置依赖（先读这个）

**本计划中所有实验必须在 vLLM + 小 LLM baseline 建立后才能产生论文可用的最终数据：**

```
前置：vLLM + Qwen2.5-1.5B 级 LLM baseline 建立（替代手动 HTTP endpoint）
前置：研究内容一 动态 batching 策略消融完成

当前状态: GPU = 手动 HTTP endpoint（仅预研可用）
```

**为什么**：在手动 HTTP endpoint 上做 K_max 扫描，搜出来的"最优 K_max"可能只是因为 endpoint 的队列处理能力不同。vLLM continuous batching 改变了 GPU 侧的请求处理模式，K_max 的最优值和时间分布都会变化。

**关键反证条件**（必须在 P0a 后检验）：如果 vLLM continuous batching 内部已将请求排队和 batch 做得很好，则外部 Ray 层的 K_max 控制价值可能有限 → 研究内容二 贡献重新定位为"外部调度 + 跨层协同"而非"独立 GPU 调度优化"。

**两层调度关系：外部 Request 级（RC2）vs 内部 Token 级（vLLM chunked prefill）**（来源：2026-07-20 chunked prefill 交叉分析）：

vLLM 的 `--enable-chunked-prefill` 和你的 queue-adaptive flush / K_max 控制操作在**不同层面**，功能互补而非冲突：

| 层面 | 谁控制 | 粒度 | 做什么 |
|---|---|---|---|
| 外部 request 级 | Ray actor（你的 RC2 策略）| 请求 | 决定**何时**向 vLLM 提交下一个 batch、控制 in-flight 请求数 |
| 内部 token 级 | vLLM scheduler（chunked prefill）| Token | 决定每个 forward pass 中 prefill chunk 和 decode 的配比 |

**关键认知**（事实 + 推断）：
- vLLM chunked prefill 的 **decode-priority 调度**确保 decode 不会被长 prefill 饥饿——这减少了外部 K_max 控制的部分必要性（vLLM 内部已经在做流控）
- 但 vLLM 的调度器**不感知上游数据注入速率**——它只能对已提交的请求做调度，不能阻止上游过快提交导致 `num_requests_waiting` 堆积
- 你的 queue-adaptive flush 在 **vLLM 队列堆积之前**做前置调节——这是 vLLM 内部调度器做不到的
- **操作原则**（事实）：外部调度只改变"何时提交"和"提交多少"，不改变单个请求的内容完整性。每个 vLLM 请求必须包含完整、自包含的推理上下文

**实验配置约束**：
- 如果使用 vLLM ≥ 0.8.0（V1），chunked prefill 强制开启且不可关闭——实验中应固定此变量（不作为消融维度）
- 如果使用 vLLM V0（< 0.8.0），建议固定 `--enable-chunked-prefill` 开启——因为这更接近生产环境且与你的 bin-packing 策略（RC1 §2.5）协同
- 无论哪个版本，在 CSV 中记录 `vllm_version` 和 `chunked_prefill_enabled` 字段

---

## 0.5 自回归生成作为提交控制的物理前提（2026-07-24 补充）

本课题的提交控制（K_max 自适应 / queue-adaptive flush）依赖**自回归生成的两个特性**，而数据组织（token-budget）不依赖——这条边界是后续实验和代码设计的认知前提。

**自回归生成贡献的两个不确定性**（论文依据）：

- **decode 阶段 memory-bound**：逐 token 生成、矩阵-向量乘、GPU 计算利用率低（FlexGen 测 OPT-175B decode 仅 13%），占单请求延迟大头。来源：vLLM SOSP 2023 §2.2、FlexGen ICML 2023、Sarathi-Serve OSDI 2024。
- **输出长度不可预测 / 完成时间异质**：何时出 `<EOS>` 不可预测，同 batch 内不同请求完成时间差可达 10×+。来源：Orca OSDI 2022（iteration-level scheduling 动机）、Clipper NSDI 2017（自回归打破"批原子完成"假设）。

**K_max 与 flush 各吃一条**：

- K_max 吃"资源占用随时间增长 + 停留时间不确定"——decode memory-bound 使多塞请求能摊销开销（batching 有收益），但塞太多 KV cache 爆、触发 preemption；最优并发度因此**负载相关**（DistServe M/D/1 排队证明）。这是 K_max 要做成 adaptive 的根源。
- flush 吃"完成时间异质"——提交时机要响应服务端实时 running/waiting 状态（queue 空赶紧塞防 GPU 饥饿，queue 满暂停积攒防堆积）。

**架构边界（代码确证）**：

- 数据组织（token-budget / length-align / prefix-aware）依据**已知输入 prompt token**，**不依赖自回归**——`code/src/organizers.py:230` `_row_token_cost = prompt_tokens + completion_max_tokens`，全在发请求之前决策。
- 提交控制（adaptive K_max）依据**运行时 vLLM 指标**（running/waiting/kv_cache），**依赖自回归**——`code/scripts/postgres_ai_operator_profile.py:512` `adaptive_inflight_limit`。

**含义**：实验若把 output 固定（如 `--completion-max-tokens 64`），会消除"输出长度不可预测"这个变异源，使 adaptive 的运行时动态优势无从发挥——这是 RC2 当前负结果（P0-1）的一个待排除混淆变量，见 `experiment_status_and_gaps.md` P0-1。

---

## 1. 研究问题

在数据库触发的外部 AI 执行链路中，Ray task/actor 的并行度（`K_max`）、GPU endpoint 路由和反压策略如何根据下游 GPU 推理服务和上游数据注入速率**联合决策**？

**核心假设**：Ray 默认调度（无界 in-flight）在 GPU 服务成为瓶颈时会导致 queue wait 累积，但简单加固定 `K_max` 不能适配 workload 变化——需要感知 GPU 服务状态的 adaptive backpressure。

**关键反证条件**：如果 vLLM continuous batching 内部已经很好地消化了请求波动，那外部 Ray 层的调度优化空间可能很小。

---

## 2. 假设（Hypotheses）

| 编号 | 假设 | 待检验 | 对应实验段 |
|---|---|---|---|
| H2.1 | Ray 默认调度（K_max = ∞）在 GPU 服务成为瓶颈时的端到端性能与有界 K_max 无显著差异 | 能否被推翻？| §5.1 K_max 扫描 |
| H2.2 | round-robin 路由在多 endpoint 下的性能与 least_queued 无显著差异 | 能否被推翻？| §5.2 routing 对比 |
| H2.3 | 静态 K_max*（从 EMBED workload 调优）在所有 workload 和注入模式下表现一致 | 能否被推翻？| §5.3 adaptive vs static |
| H2.4 | vLLM continuous batching 接入后，外部 Ray 层的 K_max 控制仍有 > 10% 的吞吐增益 | 能否被推翻？| §4.0b 前置实验 |

**最可能被推翻的假设决定 研究内容二 的核心贡献**：如果 H2.4 被推翻（vLLM 已消化了大部分收益）→ 研究内容二 独立贡献有限，其价值体现在跨层协同（与 研究内容三 联合优化）；如果 H2.3 被推翻（adaptive > static）→ 研究内容二 有独立贡献。

---

## 3. 变量

| 变量 | 含义 | 取值范围 |
|---|---|---|
| `K_max` | 最大 in-flight Ray task/actor 数 | {1, 2, 4, 8, 16, 32, ∞ (Ray 默认)} |
| `endpoint_count` | GPU 模型服务进程数 | {1, 2, 4} |
| `routing_strategy` | task 到 endpoint 的路由策略 | {round_robin, least_queued, random} |
| `backpressure_mode` | 反压策略 | {none (Ray 默认), static_K, adaptive_K} |
| `workload_type` | AI 算子类型 | {EMBED (真实), FILTER (模拟), COMPLETE (模拟)} |

**关于 FILTER/COMPLETE 的诚实标注**（参照 Orca 合成权重的做法）：同 研究内容一。FILTER 为模拟布尔输出（已知 selectivity），COMPLETE 为模拟 token generation（受控 token 长度分布）。

---

## 4. Baseline 对照

| 编号 | 描述 | 级别 | 来源 |
|---|---|---|---|
| **A2.1** | Ray 默认行为（无显式 `K_max`，框架自动排队）| 合理默认 | Ray 默认调度 |
| **G1** | vLLM continuous batching + 固定 actor pool + round-robin | S 级 | vLLM (SOSP 2023) |
| **G2** | Ray Serve 内置调度 + autoscaling | S 级 | Orca (OSDI 2022) 思路 + Ray Serve 文档 |

---

## 4. 前置实验（必须在 研究内容二 方法实验前完成）

### 4.0 模型 batch scaling 曲线

```
脱离数据库/Ray 链路，单独测模型：
  batch_size ∈ {1, 2, 4, 8, 16, 32, 64, 128, 256, 512}
  endpoint_count ∈ {1, 2}
  指标: T_gpu(batch), rows/s, GPU utilization (如有)

目的: 确认 GPU 模型的吞吐平台期在哪个 batch size
      → 如果 batch=32 就饱和了，那讨论 batch=256 vs 512 没意义
```

### 4.0b vLLM baseline 确认

```
接入 vLLM offline inference mode:
  batch_size ∈ {32, 64, 128, 256}
  指标: T_gpu(batch), rows/s
  对照: vLLM vs 当前手动 HTTP endpoint

目的: 确认 vLLM continuous batching 比手动 endpoint 快多少
      → 如果 vLLM 把 GPU 侧独立优化空间压缩到 < 10%，
        则 研究内容二 的贡献应重新定位为"外部调度 + 跨层协同"而非"GPU 调度优化"
```

---

## 5. 实验矩阵

### 5.1 K_max 扫描（Bounded vs Unbounded—验证 backpressure 价值）

```
K_max ∈ {1, 2, 4, 8, 16, 32, ∞}
endpoint_count ∈ {1, 2}
───────────────────────────
总组合: 7 × 2 = 14
每组合: 3 次重复
总运行: 42 次

固定条件:
  - 数据规模: 16384 行（最大规模，queue wait 最显著）
  - Workload: AI_EMBED
  - batch_size: 参数组合穷举 最优值
  - 路由: round_robin
```

**要推翻的假设**："Ray 默认调度足够好，不需要显式 backpressure。"

**期望发现**：
- `K_max = ∞`：吞吐高但 P99 延迟差（queue wait 累积）
- `K_max` 太小：延迟好但 GPU 空闲（吞吐低）
- 存在一个甜点区域 `K_max*`：吞吐 ≈ 无穷大，P99 延迟大幅改善

### 5.2 Routing 策略对比

```
routing ∈ {round_robin, least_queued}
endpoint_count ∈ {1, 2, 4}
K_max = K_max*（从 5.1 取最优值）
───────────────────────────
总组合: 2 × 3 = 6
每组合: 3 次重复
总运行: 18 次
```

**要推翻的假设**："round-robin 足够好，不需要感知 endpoint 队列状态。"

**期望发现**：
- 单 endpoint：routing 策略无差异（只有一个 destination）
- 多 endpoint + 均匀 workload：round_robin ≈ least_queued
- 多 endpoint + 不均匀 workload（有 straggler）：least_queued > round_robin

### 5.3 Adaptive vs Static K_max（当 workload 变化时）

```
策略:
  - static_K: K_max = K_max*（来自 5.1，固定不变）
  - adaptive_K: K_max 根据 queue depth 动态调整

测试场景:
  - 均匀注入（benchmark 到 benchmark 的直接对比）
  - 突发注入（模拟生产中的 spike）
  - workload 混合（EMBED + FILTER，两种不同 GPU 耗时特征）

总组合: 2 (策略) × 3 (场景) = 6
每组合: 3 次重复
总运行: 18 次
```

**要推翻的假设**："静态 K_max 在不同 workload 下表现一致。"

**期望发现**：adaptive 在均匀场景 ≈ static，在突发/混合场景 > static。

---

## 6. 指标

| 指标 | 测量方法 | 论文参照 |
|---|---|---|
| **端到端延迟** | `T_e2e` | 所有论文 |
| **P99 延迟** | per-batch 延迟分布 | vLLM 的 P99 latency |
| **阶段拆解** | GPU request wall、queue wait（提交到开始执行的时间差）、fan-in | TurboVecDB 的层级拆解 |
| **吞吐 (rows/s)** | `total_rows / T_e2e` | vLLM 的 requests/second |
| **GPU 空闲率** | `1 - (GPU_busy_time / T_e2e)`（近似）| vLLM 的 GPU utilization |
| **queue_depth** | 每个 endpoint 前等待的 task 数 | Orca 的 batch queue 分析 |

**关键**：不报"adaptive 比 static 好 X%"，而是画 **"延迟-吞吐曲线"**——在不同吞吐水平下 P99 延迟如何变化。这是 vLLM/Orca 的标准做法。

---

## 7. 消融设计

对 A2.3（自适应 In-Flight）的消融：

| 消融项 | 做法 | 期望发现 |
|---|---|---|
| K_max 的贡献 | K_max = ∞ vs K_max* (static) vs adaptive | static K_max 已经拿走了大部分收益，adaptive 在 workload 变化时提供额外保护 |
| endpoint_count 的贡献 | 1 vs 2 vs 4 endpoint（固定 K_max*）| 2 endpoint 有显著收益，4 endpoint 可能边际递减 |
| routing 的贡献 | round_robin vs least_queued（固定 K_max*）| 多 endpoint 下 least_queued > round_robin，但与 K_max 的交互效应可能更大 |

---

## 8. 结果展示图

| 图号 | 内容 | 类型 | 论文参照 |
|---|---|---|---|
| Fig_RC2_1 | K_max → (吞吐, P99延迟) 双 Y 轴 | 折线图 | vLLM Fig. 6/7 的吞吐-延迟曲线 |
| Fig_RC2_2 | endpoint_count × routing 的延迟分布 | 箱线图/小提琴图 | 展示 P50/P99 差异 |
| Fig_RC2_3 | adaptive vs static 在不同 workload 场景下的延迟对比 | 分组柱状图 | Orca 的多模型对比 |
| Fig_RC2_4 | GPU queue wait 占总延迟的比例（随 K_max 变化）| 堆叠面积图 | TurboVecDB 的层级拆解思路 |

---

## 9. "When does it NOT help?" 边界验证

每个边界条件必须对应一个**可跑的实验点**。

| 边界条件 | 验证实验 | 期望结果 |
|---|---|---|
| GPU 远快于数据注入 | 手动将 GPU endpoint 换成极快的空操作（返回固定值），比较 K_max=∞ vs K_max* | 无显著差异 → 边界成立 |
| 单 endpoint + 单 model | 在 endpoint_count=1 下对比 round_robin vs least_queued（§5.2）| 差异 < 3% → 边界成立 |
| vLLM 内部已消化请求波动 | §4.0b 前置实验：vLLM 接入后 K_max=∞ vs K_max* 的差异 | 差异 < 5% → 外部 K_max 控制价值有限 |
| workload 完全均匀 | 同 model、固定 text length 的均匀 workload vs 混合 length 的不均匀 workload（§5.3）| 均匀场景 adaptive ≈ static，混合场景 adaptive > static → 边界成立 |

---

## 10. 统计规范（参照 vLLM/Orca 标准）

| 要求 | 做法 |
|---|---|
| **重复次数** | 每组配置 3 次。核心发现（被推翻的假设）额外补到 5 次 |
| **集中趋势** | 取**中位数** |
| **离散度** | 报告 IQR。5 次以上报告标准差 |
| **Ray 状态重置** | 每次重复之间 `ray stop` → `ray start` |
| **数据库状态** | 写回实验 TRUNCATE 表；非写回实验可复用 |
| **Warm-up** | 每组配置先跑 1 次（不计入结果），后面 N 次计入 |
| **随机种子** | 数据生成固定 seed，确保不同配置跑同一批数据 |

---

## 11. 从 CCF-A 论文借鉴的评估原则

1. **吞吐-延迟曲线，不是单点数字**：每个实验输出的是整条曲线，让 reviewer 看到全工作点（vLLM/Orca）
2. **先证明 baseline 是已知最优**：vLLM 接入是 P0 前置条件——否则 reviewer 可以说"你应该跟 vLLM 比，而不是跟手动 HTTP endpoint 比"
3. **诚实报告 vLLM 可能缩小优化空间**：论文 §7/§8 必须写"vLLM continuous batching 可能使外部 K_max 控制的边际收益变小"
4. **消融揭示交互效应**：不只报 K_max 的独立收益，还报 K_max × endpoint_count × routing 的交互（FlexPushdownDB 的混合 vs 单策略对比思路）

---

## 12. 运行检查清单

- [ ] P0: 模型 batch scaling 曲线（脱离数据库/Ray，纯模型测）
- [ ] P0: vLLM 接入并跑通 baseline 对比（vs 手动 HTTP endpoint）
- [ ] P1: K_max 扫描（5.1）完成，确定 K_max*
- [ ] P1: endpoint_count × routing 对照（5.2）
- [ ] P2: adaptive vs static K_max 在突发/混合场景下（5.3）
- [ ] P2: 消融数据可以画 Fig_RC2_1 到 Fig_RC2_4
- [ ] 所有结果 CSV 保存在 `experiments/results/rc2/`
- [ ] 每个图标注数据来源、warm-up 策略、重复次数、取中位数还是平均值
