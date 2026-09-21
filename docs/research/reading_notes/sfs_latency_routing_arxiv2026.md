---
type: paper-note
tags:
  - deep-reading
  - paper/sfs
  - latency-estimation
  - query-routing
  - vllm
  - serving-framework-simulation
  - TTFT
  - continuous-batching
  - rc2-reference
  - submission-strategy
aliases:
  - "SFS Latency-Aware Routing (arXiv 2026)"
status: 精读完成
read_date: 2026-07-27
---

# 精读笔记：Beyond Accuracy and Cost: Latency-Aware LLM Query Routing for Dynamic Workloads (arXiv 2026)

---

## ▎第一层 · 基本信息

| 字段 | 内容 |
|------|------|
| **论文** | Shivam Patel*, Akaash R. Parthasarathy* (CMU), Ankur Mallick (Microsoft), Gauri Joshi (CMU). *Beyond Accuracy and Cost: Latency-Aware LLM Query Routing for Dynamic Workloads.* arXiv:2607.18253v1, May 2026. (*Equal contribution) |
| **来源级别** | arXiv 预印本（cs.AI）。CMU + Microsoft 合作。投稿状态未确认（可能在投 MLSys/OSDI 级别）。 |
| **链接** | arXiv:2607.18253 / 开源代码：github.com/akaashrp/sfs |
| **阅读日期** | 2026-07-27 |
| **状态** | 逐字精读完成 |
| **相关论文组** | LLM 推理延迟估计 / 查询路由 / serving framework simulation / continuous batching / 提交策略 |

### 一句话核心结论

提出 Serving Framework Simulation (SFS)——一种确定性轻量模拟器，在亚毫秒开销下模拟 vLLM continuous batching + chunked prefill 的逐 token batch 处理过程，预测新请求的 TTFT（Time-to-First-Token），MAPE <5%。将 SFS 集成到 latency-aware router 中，联合优化 accuracy-cost-latency，相对 baselines 实现最高 40% 的 utility 提升。还提供了无需实时 workload 信息的 average-case 版本（基于 Limited Processor Sharing queueing model）。

`#latency-estimation` `#TTFT` `#vLLM` `#continuous-batching` `#serving-framework-simulation` `#submission-strategy`

---

## ▎第二层 · 论文结构分析

### 1. 问题拆解

| 问题 | 论文的回答 |
|------|-----------|
| 要解决什么痛点？ | 现有 LLM query router 只考虑 accuracy 和 cost，**延迟是 blind spot**——可能把请求路由到已过载的实例导致排队延迟和 SLO 违背。系统层的 load balancer（round-robin/JSQ）控制了延迟但忽略了 accuracy-cost 偏好。两者之间存在 gap。 |
| 之前的方法为什么不够？ | 延迟估计困难：TTFT 不仅取决于 prompt 长度，还取决于模型实例当前的 prefill/decode workload 组成、serving framework 的 batching/scheduling 策略、以及硬件特性。简单的 throughput-based estimator（"用 prefill throughput 除剩余 prefill tokens"）MAPE 高达 85%。 |
| 论文的**核心论点** | 轻量确定性模拟（SFS）可以精确估计 TTFT，且开销足够低（亚毫秒）用于在线路由决策。 |
| 它的**关键假设** | (1) Token batch 处理时间可以通过 4 参数线性回归可靠估计（MAPE ~4%）；(2) 每个 decode 序列每 token batch 恰好产生 1 个 decode token（standard autoregressive generation 假设）；(3) vLLM continuous batching + chunked prefill 行为可被确定性模拟；(4) 路由侧可获取模型实例的实时 workload 状态（prefill/decode token composition）。 |

### 2. 方法拆解

**2.1 SFS (Serving Framework Simulation) TTFT Estimator**

核心思想：给定当前时间 t 模型实例 j 的 workload snapshot——所有驻留请求的剩余 prefill tokens `tok_q,j^pre(t)` 和剩余 decode tokens `tok_q,j^dec(t)`——SFS 确定性模拟后续每个 token batch 的组成和处理时间，直到新请求 qi 生成第一个 decode token。

**TTFT 公式**：
```
L_i,j^SFS(t) = Σ_{τ=1}^{TTFT_j(t,i)} T_j^(τ)(t)
```
其中 `TTFT_j(t,i)` 是 qi 产生第一个 decode token 时的 token batch 序号，`T_j^(τ)(t)` 是第 τ 个 token batch 的处理时间。

**Token-batch 处理时间估计器**（4 参数线性回归）：
```
T_j^(τ)(t) = β_0,j
  + β_1,j · Σ(tok_q^pre + tok_q^dec)        ← dense-layer computation（与 token 数线性）
  + β_2,j · Σ(c_q · tok_q^dec)               ← attention + KV-cache reads（与 context 长度线性）
  + β_3,j · Σ(tok_q^pre · c_q + tok_q^pre(tok_q^pre+1)/2)  ← prefill attention cost
```

4 个系数 (`β_0` 到 `β_3`) 对每个模型实例离线校准。MAPE ~4%。

**为什么 SFS 比 throughput-based estimator 好那么多？**

Throughput-based estimator（eq.6）简单地用 prefill throughput 除剩余 prefill tokens——完全忽略了：(a) decode workload 也在竞争 GPU，(b) chunked prefill 的 chunk 大小限制，(c) 排队等待时间。SFS 通过逐 batch 模拟捕获了所有这些效应。实验：SFS MAPE <5% vs throughput-based MAPE 85%。

**SFS 对输出长度误差的鲁棒性**：TTFT 估计只需要模拟到新请求的第一个 decode token，而不是所有驻留请求完成。每个 decode 序列每 batch 最多贡献 1 个 decode token，因此活跃序列的**数量**比每个序列的精确剩余 decode 长度更重要。

**2.2 Average-Case Estimator（无实时 workload 信息时）**

在无法获取实时 workload 信息时（如路由到第三方云服务），用 Limited Processor Sharing (LPS) queueing model：

```
W_i,j^avg = (λ_j/μ_j)^k / (μ_j - λ_j)
L_i,j^avg = W_i,j^avg + tok_qi^pre / μ_j^pre + T_j^dec
```

k 是模型实例的最大并发服务数（类比 K_max），μ_j 是服务速率。实验表明 LPS 模型与 Qwen3-0.6B 实测等待时间曲线高度一致。

**2.3 Latency-Aware Routing**

路由目标：在满足 TTFT 约束 i 的模型实例中选择 utility 最高的：
```
m(i) = argmax_{j: L_ttft ≤ τ_i} U_i,j(λ)
```
其中 `U_i,j(λ) = acc_i,j - λ · cost_i,j`（accuracy-cost utility）。

评估指标：**OnTimeUtility**——只给满足延迟约束的请求计分，违反约束的 utility 计为 0。

**2.4 预测器架构**

- Accuracy predictor：LightGBM Huber regressor on prompt hashing features (512-dim signed hash → PCA 16-dim) + model features
- Output-length predictor：LightGBM on same features
- Cost = prompt_tokens × price_in + predicted_output_tokens × price_out

### 3. 实验拆解

| 维度 | 内容 |
|------|------|
| **模型与硬件** | Qwen3-0.6B (1×H100)、Qwen3-8B (1×H100)、Qwen3-32B (2×H100, tensor parallelism)。vLLM serving framework。 |
| **Workload** | 4 种任务覆盖 (short/long)×(short/long) prompt/response：Alpaca、HotpotQA、GovReport-Summarization、WritingPrompts。各 2.5K queries。 |
| **到达过程** | Poisson (baseline) + MMPP-2 bursty arrivals (rate ratio r=3/6) |
| **Baseline** | Round Robin、Shortest Queue (JSQ)、Latency-Agnostic (只优化 utility) |
| **评价指标** | OnTimeUtility、Utility-Latency tradeoff curve (varying λ) |

**核心实验结果**：

| 场景 | 结果 |
|------|------|
| **Varying offered load (3-9 qps)** | SFS 在所有负载下 OnTimeUtility 最高；AUC 比 best baseline 高 33%；5 qps 时 OnTimeUtility 高 46% |
| **Varying latency emphasis (λ)** | SFS 持续优于所有 baseline，utility 比 Shortest Queue 高 40% |
| **Bursty arrivals (MMPP-2)** | r=3 和 r=6 下 SFS 仍优于所有 baseline，且差距稳定 |
| **SFS overhead** | 亚毫秒级（单个请求的 TTFT 估计时间），支持在线路由 |
| **Token-batch estimator accuracy** | ~4% MAPE（Figure 3，Qwen3-0.6B on H100） |
| **TTFT estimation** | <5% MAPE（vs throughput-based 85%） |

---

## ▎第三层 · 批判性评估

### 论文优势

1. **延迟估计方法精巧且实用**：SFS 抓住了核心（模拟 token batch 演化），但不沉溺于低层 kernel 模拟。4 参数线性回归校准简单，MAPE 却只有 4%。
2. **双模式设计全面**：有实时 workload 信息时用 SFS，无信息时用 LPS queueing model——覆盖了实际部署的两种场景。
3. **实验覆盖广**：3 种模型规模、4 种 workload、Poisson + MMPP-2 到达、varying load + varying latency emphasis——评估维度全面
4. **全开源**：代码 + 数据可用

### 局限与边界

- **仅估计 TTFT，非完整延迟**：论文选择 TTFT 作为延迟指标（理由是"captures responsiveness"），不涉及 TPOT (Time Per Output Token) 或端到端完整延迟。对于长生成任务，TTFT 只是用户体验的一部分。
- **路由场景而非调度场景**：论文是在多个不同模型实例之间做路由选择（"which model to use"），而不是在单个实例内做请求调度（"when to admit"）。与项目提交策略的"K_max/flush 控制"场景有差异。
- **假设每 decode 序列每 batch 恰好 1 个 decode token**：在标准 autoregressive generation 下成立，但在 speculative decoding 或其他变体下可能不成立。
- **SFS 需要模型实例的实时 workload snapshot**：`tok_q,j^pre(t)` 和 `tok_q,j^dec(t)` 需要从 serving framework 获取，vLLM Prometheus 可能不够细粒度。
- **arXiv 预印本**：投稿状态未确认
- **未涉及共享 vLLM 多 job 场景**：实验是单模型实例内的多请求路由，不是跨多个独立 job 的并发

---

## ▎第四层 · 与课题连接

### 对提交策略（RC2）的直接启示

**1. SFS 可作为"what-if"预演工具**

这是最直接的迁移价值。当前项目的提交策略（K_max、flush timeout）本质上在做决策："如果现在把 pending batch 提交给 vLLM，这些请求的延迟会是多少？"SFS 恰好回答了这个问题——给定当前 vLLM workload state + 新请求的特征，预测 TTFT。

具体应用：
```
当前：K_max 是静态的（K=8），queue-adaptive flush 只看 queue depth
升级：在每次 flush 决策时，用 SFS 模拟"如果提交这个 batch，TTFT 会是多少"→ 只提交 TTFT 预测值在 SLO 内的 batch
```

**2. Token-batch 处理时间估计器的方法可直接复用**

SFS 的 4 参数线性回归（`T = β0 + β1·Σtok + β2·Σ(c·tok) + β3·Σ(tok·c + tok²)`）是一种"用简单回归建模复杂系统行为"的方法论范例。项目的代价估计也可以用类似思路：从 profile 数据中校准 per-model 参数，而不是训练全局 Ridge。

**3. 双模式设计对应项目的两种使用场景**

- **离线编排**（优化器选计划）：对应 SFS 的 average-case estimator（LPS queueing model）——不需要实时 workload 信息
- **在线提交控制**：对应 SFS 的 real-time estimator——需要 vLLM Prometheus 的实时 workload snapshot

**4. LPS Queueing Model 的 k 参数 = 项目的 K_max**

SFS 的 average-case estimator 用 LPS 模型（k 个并发服务槽位，超出的排队）。这个 k 恰好是项目的 K_max。SFS 的公式 `W_avg = (λ/μ)^k / (μ - λ)` 提供了一种解析方式估计"给定 K_max 和到达率下的平均等待时间"——可用于指导 K_max 的选择（而不是纯实验搜索）。

### 对代价估计（RC4）的启示

**5. 输出长度预测的重要性**

SFS 的一个重要组件是 output-length predictor（LightGBM on prompt features）。项目的代价估计当前只用 `completion_max_tokens`（用户设定的上限），但实际 E2E 时间与自然 EOS 下的实际输出长度高度相关。如果加入更好的 output length predictor（即使是 coarse 的），可能显著改善代价估计精度。

### 不能直接迁移的地方

- SFS 做 routing（"选哪个 model instance"），项目做 scheduling（"什么时候提交"）。路由决策是 per-request、从多个 candidate 中选一个；提交决策是 per-batch、判断要不要发送。虽然共享延迟估计这个基础组件，但决策框架不同。
- SFS 在路由侧运行（外部），可以访问所有 model instance 的 workload state。项目的提交控制运行在 Ray actor 中（靠近 vLLM），能获取的 vLLM Prometheus 信号更有限。
- SFS 的 token-batch 处理时间需要离线校准（per model instance），项目的 profile 数据可以支持这一点。

### 可引用的观点

- "SFS achieves <5% MAPE on TTFT predictions with sub-millisecond test-time overhead" → 支撑"轻量模拟是可行的延迟估计方法"
- "Unlike the prefill-throughput estimate... SFS does not separately approximate waiting, prefill, and first decode token computation... instead capturing these components directly via the simulated token batches" → 支撑"解耦模拟优于粗粒度公式"
- "The number of active decode sequences is more important for TTFT estimation than the exact remaining decode length of every resident sequence" → 支撑"对输出长度误差鲁棒"

### 不能过度引用的地方

- 不能把 SFS 的 routing 框架直接称为提交策略方案——作者场景是 multi-model routing，项目是 single-model admission control
- 不能把 SFS 的 <5% MAPE 直接预期为项目的 E2E 预测 MAPE——SFS 只估计 TTFT（不含队列前的系统延迟和 writeback），项目目标是完整 e2e_s
- SFS 是 arXiv 预印本，投稿状态未确认——不能写成已接收的顶会论文
