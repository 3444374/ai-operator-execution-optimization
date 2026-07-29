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

当前状态：vLLM baseline 与真实双 endpoint 已建立；global/per-endpoint static
admission、request-level 补位、active-work credit、least-work routing 和
shared multi-job credit 已实现。后四项及 service-quantum 动态预算仍需正式
GPU 重复，代码完成不等于策略有效。
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
| `routing_strategy` | task 到 endpoint 的路由策略 | {round_robin, least_queued, least_work, prefix_affinity} |
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

### 5.0 多 endpoint 公平性与执行顺序（2026-07-28）

`K_max` 必须显式注明 scope。历史 `global K=16` 在两个 endpoint 间共享，不能
与单 endpoint K16 直接比较。容量曲线使用相同 **per-GPU K**：

```text
single endpoint: global K ∈ {8, 16, 24}
dual endpoint:   per-endpoint K ∈ {8, 16, 24}
```

每个点 1 次 warm-up + 3 次随机交错 formal repeat。单 endpoint control 只采样
其实际 GPU，双 endpoint 记录两端 metrics。报告 dual/single scaling 时按同一
per-GPU K 配对，同时展示 P99、tokens/s、MFU、能耗和 endpoint submission 分布。

执行顺序固定为：

1. 在 request-level 路径扫描 per-endpoint active-token credit，直接标定
   offered work 饱和区，禁止先调 AIMD；
2. 固定 active work 后扫描 token budget，再做离线 membership 隔离实验，
   避免预算增大同时增加 request concurrency，也避免 flush 抢先关闭 batch；
3. 用 `organization_batch_rows_mean` 换算 request credit，比较
   whole-submission 与 request-level replenishment；
4. 在突发、异长和 SLO-sensitive workload 下验证 adaptive 控制；
5. 固定已标定 credit 后分别消融 least-work routing、service-quantum budget
   和 adaptive flush；单项成立后才运行联合候选；
6. 单作业门禁通过后再运行共享 endpoint 的多 job 公平调度。

当前 AIMD/HOL 不进入正式矩阵：vLLM waiting 近零，看不到 Ray 侧 backlog；
HOL-age 又包含正常 4–5 秒服务时间，3 秒阈值会把正常服务误判为拥塞。继续扫
AIMD 参数不能修复观测信号错位。

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

### 5.4 Whole-submission barrier vs continuous replenishment

本实验不修改 vLLM continuous batching，只改变 Ray 上游释放 credit 和补充请求
的粒度。

```text
策略:
  - whole_submission: 整个 Ray submission 返回后释放 admission credit
  - request_replenishment: 任一请求完成后释放自身 credit 并立即补位
  - token_credit_replenishment: 在上一项基础上限制 active estimated token work

控制变量:
  - 同一 Daft source batch
  - 同一 workload / arrival replay
  - 同一 vLLM 配置、静态 K_max 和 flush timeout
  - 同一 request identity、重试和失败语义
```

先用固定最佳 timeout 隔离补位机制；只有补位相对 whole-submission barrier 有
可辨认收益后，才加入 adaptive flush。

**要推翻的假设**：“vLLM 内部已有 continuous batching，因此上游 submission
完成粒度不会限制持续供给。”

**必须记录**：request/submission 首末完成时间、submission 内 completion span、
credit idle ratio、refill 次数和间隔、active request/token work、vLLM
running/waiting/KV、observed tokens/s、SLO goodput 和 request P99。

### 5.5 多 job 共享 endpoint：局部 K 不构成全局保护

数据库同时执行多个 AI 算子时，每个 job 启动一个独立 scheduler 的现有方式
存在结构性缺口：两个 `per_endpoint K=16` 的 job 会在同一 endpoint 上形成
最多 32 份局部在途预算，第三个 job 加入后继续线性膨胀。因此，多 job 优化的
控制点必须高于 job-local scheduler，是所有 job 共用的 endpoint-local
admission coordinator。

第一轮矩阵：

```text
job count       ∈ {1, 2, 4}
workload mix    ∈ {short+short, short+long, burst+bulk}
policy          ∈ {
  independent_local_K,
  static_endpoint_partition,
  shared_request_cap,
  shared_request_and_work_credit,
  shared_work_credit_plus_weighted_fair_queue
}
arrival offset  ∈ {0 s, staggered}
```

每个 job 必须先单独运行，得到 solo JCT、P99 和 service rate；共享结果报告
`slowdown_j = JCT_shared_j / JCT_solo_j`。除总吞吐外，至少报告最大 slowdown、
P95/P99、饥饿时间、SLO goodput 和基于 normalized service 的 Jain fairness。

要推翻的假设：

1. “每个 job 各自设 K 就足够保护共享 vLLM”；
2. “总 tokens/s 高就代表多 job 调度好”；
3. “把两个 GPU 静态各分一个 job 总是最优”。

晋级条件：共享 work-credit + 公平队列相对 `independent_local_K` 明显降低最大
slowdown/饥饿，相对静态分区保持更高总 goodput，并且单 job 时能够借用全部
空闲容量。

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
| **补位效率** | credit idle ratio、refill interval、completion span | Orca/vLLM continuous batching 的上游迁移验证 |
| **有效吞吐** | observed tokens/s、SLO goodput | vLLM/Clipper 的吞吐—SLO评估范式 |

**关键**：不报"adaptive 比 static 好 X%"，而是画 **"延迟-吞吐曲线"**——在不同吞吐水平下 P99 延迟如何变化。这是 vLLM/Orca 的标准做法。

MFU 只作为机制诊断，不作为控制器 reward 或“是否压满 GPU”的唯一判据。
prefill 主要受计算吞吐限制，decode 常受 HBM 带宽和 KV-cache 读写限制；decode
已经达到服务容量上界时，MFU 仍可能远低于 100%。反过来，扩大 prefill、
padding 或无效计算可以抬高 MFU，却未必增加有用 output tokens/s。正式报告
必须同时给出：

- observed prompt/output tokens/s 与 request goodput；
- 相同模型/workload 的 direct-vLLM service-only capacity ceiling；
- `capacity_efficiency = pipeline_tokens_s / direct_service_tokens_s`；
- MFU、GPU utilization、KV/cache 与 P99，作为解释容量损失发生在哪一层的证据。

infra 的目标是明显优于 naive DB→Daft→Ray baseline，并逼近 direct-vLLM
service-only 上界；不能要求上游链路超过同 workload 的服务物理上界。

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

---

## 13. Shared-vLLM 1/2/4-job 预注册实验（2026-07-29）

### 13.1 研究问题与候选方案

研究问题不是“增加 job 能否让 GPU 更忙”，而是：在已标定的每 endpoint
`256 request slots + 65,536 predicted active-work` 饱和容量下，多个数据库
AI job 能否共享同一 vLLM endpoint，并同时获得容量保护、work conservation
和可解释的公平性。

实现前比较三种路径：

1. **直接复用旧 interference runner**：改动最少，但只支持前后台两个 job，
   每个 profiler 都执行 `--setup`，并发 profiler 还会重复计算同一份 vLLM
   全局 token 增量；不能作为正式证据。
2. **继续扩展旧 runner**：可以加入 N-job，但会把旧的前后台语义、覆盖写入、
   无租约生命周期和新的公平性语义堆在一个脚本中。
3. **新增正式 group runner（采用）**：复用现有 profiler、runner lease、
   Ray named coordinator 与 trace 契约；每个 group run 同时启动 N 个隔离
   profiler，统一记录组级 service 指标和全局 credit 状态。

### 13.2 Fatal-flaw audit

正式运行前必须消除以下混淆：

- **全局指标重复计数**：并发 profiler 的 vLLM Prometheus delta 相互重叠，
  不能相加；总吞吐、MFU 和 endpoint 分布必须由 group runner 在组级采样。
- **重复环境准备**：并发 job 禁止携带 `--setup` 或 reset；schema/workload
  在 runner 启动前只读校验，环境准备独立执行一次。
- **总容量随 job 数膨胀**：策略比较固定 endpoint 总 request/work 上限；
  `independent_full` 只作为当前系统的过量认购 baseline，不能与共享策略混称
  为相同 offered-work 对照。
- **共享上限缺少精确证据**：周期采样可能漏掉峰值；coordinator snapshot
  必须累计精确的 request/work 峰值和按 job grant work。
- **启动偏移污染公平性**：主矩阵使用同一未来 epoch 的 replay start；
  staggered arrival 作为单独 work-conservation 场景，不混入同时启动结果。
- **Ray 集群分裂**：所有 profiler 和 monitor 必须使用同一个显式
  `--ray-address`；禁止隐式创建各自的本地 Ray cluster。
- **detached actor 污染下一 run**：每个 group run 使用包含实验、场景、重复
  序号的唯一 coordinator name；结束后核对 active/waiting 均为零，再清理
  该 actor。失败时保留 manifest、日志和最终 snapshot。
- **相同输入行的语义**：首轮 equal-workload 使用相同确定性行序列，exactly-once
  按 job 校验；它验证调度隔离，不声称处理了全局唯一文档。不同 workload mix
  与 arrival offset 在核心门禁后单独扩展。

### 13.3 策略消融

| 策略 | 每 job 本地 credit | endpoint 全局 credit | 借用/公平语义 |
|---|---:|---:|---|
| `independent_full` | 每个 job 都持有 256/65,536 | 无 | 当前系统 baseline；可能过量认购 |
| `static_partition` | 将 256/65,536 静态均分给 N job | 无 | 容量安全但空闲份额不可借用 |
| `shared_drr` | 本地上限不成为瓶颈 | Ray named actor 持有 256/65,536 | weighted DRR；空闲份额可借用 |

首轮正式矩阵为 `job_count ∈ {1,2,4}` × 上述三种策略，每个场景
`1 warmup + 3 formal repeats`。同一 job-count 内固定 workload、行数、
token budget、arrival replay、endpoint、随机种子和总 endpoint 容量。
`job_count=1` 是协调开销与语义等价性检查；`job_count=2/4` 才用于公平性结论。

核心矩阵通过后，再运行两个机制场景：

- `staggered_2job`：一个 job 延迟到达，验证空闲 credit 能否被在场 job 借用；
- `weighted_2job_3to1`：权重 3:1，验证 normalized service 是否按权重收敛。

### 13.4 最小真实双 GPU gate

gate 只验证基础设施，不生成性能结论：

- 双 vLLM endpoint，显式同一 Ray address；
- `job_count=2`，每 job 64 行，`independent_full/static_partition/shared_drr`
  各一次，使用全新输出目录；
- 每 job 独立 runs/request/submission trace；group runner 生成 manifest、
  group summary、global service/resource trace 和 shared-credit trace；
- 必须满足：0 worker failure、每 job 64/64 exactly-once、endpoint 均有流量、
  shared actor 最终 active/waiting 为 0、精确峰值不超过 256/65,536、日志与
  manifest 无密钥、退出后无 runner lease 和该 run 的 detached actor。

任一项失败都禁止启动 formal；保留目录和租约/进程证据，按系统化调试定位。

### 13.5 指标与预注册门槛

事实指标：

- group observed prompt/output tokens/s、MFU、GPU utilization、endpoint 分布；
- 每 job JCT、request P50/P95/P99、SLO goodput、completion lag、worker failure；
- 每 job processed request/submission exactly-once；
- coordinator 精确 request/work 峰值、waiting、active/granted work by job；
- `slowdown_j = JCT_shared_j / JCT_solo_j`；
- equal-weight Jain fairness：
  `J = (Σ service_j)^2 / (N × Σ service_j^2)`，其中
  `service_j = completed predicted work_j / JCT_j`，weighted 场景再除以配置
  权重。禁止直接使用每个 job 固定的 offered work 计算公平性，否则等量输入会
 机械地产生 `J=1`，无法观察完成速率差异。首轮 formal 只包含同步、等权场景；
 staggered/weighted 仍保持禁用，后续启用前必须增加共同重叠时间窗内的完成
 work rate，不能直接沿用全 JCT 指标做机制结论。

正确性门槛是硬门槛：0 incident、0 worker failure、每 job exactly-once、
全局 request/work 上限不越界、结束状态归零。

策略晋升门槛：

- `job_count=1` 的 `shared_drr` 相对等价静态配置吞吐损失不超过 3%；
- equal-weight `job_count=2/4` 的 Jain fairness 中位数至少 0.95，且任一
  job 的 normalized service 不低于均值的 90%；
- 相对 `independent_full`，`shared_drr` 的最大 slowdown 或 request P99
  至少改善 5%，且 group tokens/s 退化不超过 5%；
- 在 staggered 场景，相对 `static_partition`，`shared_drr` 的 group
  tokens/s 或完成时间至少改善 5%，同时后到 job 不发生 starvation；
- 未过门槛则保留容量安全/诊断基础设施，不把 DRR 记为性能优化。

### 13.6 证据边界

该矩阵验证的是外部 Ray 调度层对共享 vLLM endpoint 的多 job 隔离与
work conservation，不修改、也不声称改进 vLLM 内部 continuous batching。
同质 equal-workload 下若三种策略没有差异，是有效的边界结果；只有在并发
竞争或 arrival/workload 异质性下出现稳定改善，才能把收益归因于共享 credit
与公平队列。

### 13.7 正式结果与决策（2026-07-29）

- 正式矩阵 36/36 group run 完成、0 incident；63 个 formal job 共
  32,256 request，每 job 512/512 completed，request id 全局唯一。
- 9 个 formal `shared_drr` credit trace 全部满足 256 request /
  65,536 predicted-work 上限并最终归零；2/4-job 有等待时 active-work
  ratio 均值为 0.9966/0.9960，未发现 fit-eligible waiting 采样点。
- 1-job shared 相对 static 吞吐 -0.02%，通过协调开销门槛；2-job shared
  相对 independent 吞吐 -0.04%、max P99 -0.04%，未达到 5% 收益门槛。
- 4-job shared 相对 independent 吞吐 +9.57%、max P99 -22.52%、
  max JCT -15.89%；Jain fairness median 0.9961，最低 normalized
  service/mean 0.9193，聚合值通过预注册门槛。
- 4-job 三次吞吐收益为 +8.43%、-0.28%、+22.60%，策略不是逐 repeat
  稳定胜出。决策为“高竞争条件性候选”：保留 shared credit/DRR 作为容量
  安全与公平基础设施，但在 held-out 复验前不晋升为通用默认。
- staggered 和 weighted 场景仍未运行；必须先补共同 overlap window 的
  service-rate 指标，再分别验证 idle borrowing 与 3:1 weighted fairness。

完整七步报告见
`experiments/results/dual_gpu_shared_vllm_formal_20260729_1135/README.md`。
