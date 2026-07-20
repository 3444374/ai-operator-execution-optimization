# 实验状态与缺口分析

Date: 2026-07-20

本文档是对 2026-07-18/19 本地 vLLM + Qwen2.5-1.5B AI_COMPLETE baseline 系列的全面审计，记录已完成实验、已证明的 claim、未完成的缺口、指标盲区，以及下一步实验路线图。

## 1. 实验全景：已完成 vs 未完成

### 1.1 研究内容一：数据组织策略

| 实验 | 状态 | 证明了什么 | 没证明什么 |
|---|---|---|---|
| 固定行 batch sweep（synthetic prompt） | ✅ 07-18 | 链路跑通 | 不是真实 workload baseline |
| ShareGPT/BurstGPT Ray 静态 batch sweep | ✅ 07-18 | Ray task > Ray actor；batch=16 时 ~260 rows/s | 离线扫表（doc_id 序），不反映在线到达 |
| Token-tail 修订版（batch 1~128, 512 行）| ✅ 07-19 | **固定行 batch 是计算量的弱代理**：batch=8 时 token 跨度 13.9×；batch=128 时 token P95=26678 | — |
| Token-budget vs Fixed Row（timeout=300）| ✅ 07-19 | **Token-budget 能约束 token tail**：6144/8192 吞吐接近 fixed 32/64，token P95 大幅降低 | 4096 吞吐更低（tradeoff）；未证明在所有场景下优于 fixed |
| Length-align + Prefix-aware ablation | ✅ 07-19 | length+fixed 是负结果（token P95=33407）；prefix+token6144 吞吐最高（339 rows/s）但 prefix ratio 仅 6.4% | length-align 需配 token-budget；prefix 信号太弱 |
| **Prefix 受控 workload 实验** | ❌ 未做 | — | prefix ratio=0/30/70/100% 下的 prefix-aware 有效性 |

**RC1 当前状态**：✅ 动机成立，策略机制已验证。⚠️ 但不是"全面胜利"——token-budget 控制 token tail 的代价是更多 HTTP 调用，这个 tradeoff 本身是论文的讨论点。

### 1.2 研究内容二：调度与提交控制策略

| 实验 | 状态 | 证明了什么 | 没证明什么 |
|---|---|---|---|
| Arrival-aware K_max sweep（token6144 固定）| ✅ 07-19 | K_max=1→8 吞吐 140→329 rows/s；超 8 无收益 | 单 shape 扫参，已被后续实验替代 |
| Batch Policy × K_max 矩阵 | ✅ 07-19 | K_max 和 batch shape 耦合：fixed128 只有 4 个请求，K_max>4 无调度空间 | 仍是单 job 离线场景 |
| Shared-vLLM K_max 干扰（2-job）| ✅ 07-19 | **K_max 在共享 vLLM 下必要**：bulk unbounded 时 foreground E2E 恶化 2.3×（4.9→11.4s），bulk 自身吞吐几乎不变 | 只有 2 个 job；只有一种 foreground size |
| Shared-vLLM K_max Sweep + Adaptive | ✅ 07-19 | K_max=8 是最佳静态 guardrail；adaptive 触发了 downshift（102 次/run）| **❌ adaptive 不如 static K=8**（foreground E2E 10.2s vs 7.3s） |
| **改进 adaptive 控制器** | ❌ 未做 | — | 渐进 ramp-up、比例控制、per-request 检查 |
| **多 job/多 foreground size 扩展** | ❌ 未做 | — | 不同 foreground size、arrival offset、background policy 下的公平性 |

**RC2 当前状态**：✅ 动机成立（shared-vLLM interference 是关键证据）。❌ 核心策略未验证——queue-adaptive flush 已实现但效果不如静态 K_max=8。这是当前最高风险的 gap。

### 1.3 耦合验证

| 实验 | 状态 | 证明了什么 |
|---|---|---|
| **独立最优拼接 vs 联合 grid search** | ❌ 未做 | — |

**状态**：完全没有实验。这是 AGENTS.md §1 写死的核心实验——"分别独立搜索最优配置后拼接，再与联合 grid search 对比"。无论结果如何（联合显著优于拼接 / 两者接近），都不改变课题的核心贡献。

### 1.4 多模态泛化验证

| 实验 | 状态 |
|---|---|
| CLIP embedding + ImageNet subset | ❌ 未做（scope 缩减条件：文本 RC1+RC2 消融完成前不启动）|

### 1.5 算子代价估计 & 写回

均已降级（不作为独立研究内容），不在当前实验计划中。

---

## 2. 证据链完整性评估

```
✅ 已证明（可写进论文正文）：
   ├── "固定行 batch 是模型请求代价的弱代理"（token-tail revision）
   ├── "Token-budget batching 能约束 per-request token tail"（token-budget vs fixed）
   └── "共享 vLLM 下无界 inflight 伤害并发小作业延迟"（shared-vLLM interference）

⚠️ 部分证明（有信号但需补实验）：
   ├── "Token-budget 在约束 token tail 同时保持吞吐竞争力"（tradeoff 存在）
   ├── "K_max 作为 admission control guardrail 调节吞吐-延迟 tradeoff"（coupling 已显示）
   └── "Length-align 配合 token-budget 有效"（仅 ablation，无正式对照）

❌ 未证明（关键缺口）：
   ├── "Queue-adaptive flush 优于静态 K_max"（当前反了）
   ├── "两项策略独立优化 ≈ 联合优化"（或 "联合显著优于独立"）
   ├── "Prefix-aware 在受控 prefix 比例下有效"（当前 prefix ratio 6.4%）
   └── "策略代码对多模态 workload 可复用"（未启动）
```

---

## 3. 指标盲区

### 3.1 已采集但未充分利用

当前 CSV 中已有但未在分析中充分利用的列：
- `batch_service_s_p99`：仅在 latency probe 中使用，未系统化到每个实验
- `vllm_request_prefill_time_mean_s` / `vllm_request_decode_time_mean_s`：prefill vs decode 占比可用于判断 batch 压力的类型
- `bounded_wait_s`：已在 K_max sweep 中使用，但未与 token P95、service P95 做交叉分析

### 3.2 关键缺失指标

| 缺失指标 | 为什么重要 | 对应实验 |
|---|---|---|
| **`tokens/s`** | 比 `rows/s` 更公平的效率指标——归一化了不同行的计算量差异。token-budget=4096 的 rows/s（301）低于 fixed 32（325），但 tokens/s 可能持平 | 所有实验 |
| **per-request e2e latency 分布** | batch-level P95 掩盖了 batch 内部单个请求的真实延迟。对 length-align/prefix-aware 论证至关重要 | RC1 分组策略实验 |
| **inflight/queue 时间序列** | 当前只有 final gauge。没有时间序列无法诊断 adaptive 为什么不如 static：初始 overshoot 的伤害有多大？downshift 后恢复需要多久？ | RC2 adaptive 实验 |
| **`service_p99`**（系统性采集） | 系统论文审稿人关心 tail。当前仅在 latency probe 中有 batch_service_s_p99 | 所有实验 |
| **`K_max` 时间序列**（adaptive 模式）| 当前只有 `adaptive_upshifts/downshifts` 计数和 `adaptive_limit_mean`，没有每次变化的时间戳和新值 | RC2 adaptive 实验 |

### 3.3 AI_EMBED vs AI_COMPLETE 指标选择差异

AI_EMBED 时期测"时延"（按阶段拆分的 wall time）是有意义的，因为每行计算量相等，"一行"是可比较的工作单位。

AI_COMPLETE 的根本差异：每行 token 量可差 13.9×，"一行"不再是有意义的比较单位。应该用：
- **计算量归一化指标**：`tokens/s` 替代/补充 `rows/s`
- **分布指标**：token P50/P95/P99、service P50/P95/P99
- **服务端压力指标**：queue time、running/waiting requests
- **控制器行为指标**：K_max 时间序列、upshift/downshift 时间戳

详细分析见 `learning/metric_selection_methodology.md`。

---

## 4. 下一步实验路线图

### P0：修 RC2 核心 claim（最高优先，1-2 周）

**目标**：让 queue-adaptive flush 在同一 shared-vLLM setup 下超越静态 K_max=8。

**改进方向**：
1. 渐进 ramp-up：从 min=4 开始，每 N 次成功提交无 queue buildup 则 +2
2. 比例控制：不是两档切换，而是 `K_max = max(min, min(max, target × factor))`
3. 每次提交前检查 vLLM metrics，而非批量提交后

**放弃条件**：如果 3 轮改进后 adaptive 仍不能达到静态 K=8 的 90% 性能（foreground E2E ≤ 8s），RC2 降级为"K_max admission control 必要性论证 + queue-adaptive 作为 Discussion 探索方向"。

**同时追加指标**：inflight/queue 时间序列、K_max 时间序列、`tokens/s`。

### P0（并列）：两项策略联合消融（1 周）

**目标**：回答"分层独立优化是否足够"。

**设计**：
- best token-budget（当前 6144）+ best K_max（当前 8）独立拼接
- vs token-budget × K_max 联合 grid search
- 保持同一 workload（ShareGPT/BurstGPT, 512 rows, arrival_time 序）

**同时追加指标**：`tokens/s`、`service_p99`。

### P1：Prefix 受控实验 + 规模扩展（1-2 周）

**目标**：
1. 在 prefix ratio ≥ 30% 的条件下评估 prefix-aware 有效性
2. 至少一个实验 scale 到 2048 行验证趋势

**设计**：
- 构造 prefix ratio = 0/30/70/100% 的受控 workload
- 仅在 prefix+token6144 条件下评估
- 选取 token-budget vs fixed 实验 scale 到 2048 行

**同时追加指标**：per-request e2e latency 分布（对 prefix-aware 论证至关重要）。

### P2：多模态泛化（触发条件：P0 和 P1 完成）

**目标**：验证策略代码的模态无关性。

**设计**：
- CLIP embedding + ImageNet/HF subset
- 同一套 `organizers.py` + `model_backends.py` 代码
- 验证 frame-budget ↔ token-budget 类比、queue-adaptive flush ↔ 完全复用

---

## 5. 审稿人视角：如果现在投稿会被拒在哪里

基于 idea-evaluator + ars-reviewer 模拟审稿的共识：

| 审稿人 concern | 严重度 | 修复路径 |
|---|---|---|
| Adaptive < static 是负面结果 | **MAJOR** | 改进控制器或重构 claim |
| 两项策略缺乏联合分析 | **MAJOR** | P0 联合消融实验 |
| 实验规模仅 512 行、单 GPU | Concern | P1 规模扩展至 2048 行 |
| Token-budget 方法 novelty 薄（贪心算法）| Concern | 诚实 framing：贡献是"表征优化空间"而非"发明新算法" |
| 无写回、单 endpoint | Minor（已声明）| Discussion 中讨论边界 |

---

## 6. 更新检查清单

当本文件中的缺口被新的实验结果填补时，同步更新：
- `experiments/results/local_vllm_qwen15b_baseline/README.md`
- `PROJECT_OUTLINE.md` §当前最重要证据、§近期优先级
- `PROJECT_LOG.md`
- `figures/README.md`（如有新增图）
- `learning/local_vllm_ray_baseline_walkthrough.md`（如实验结果影响讲解）
