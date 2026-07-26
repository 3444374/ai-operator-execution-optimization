# 实验状态与缺口分析

Date: 2026-07-20（最后更新：2026-07-26，补充文献驱动执行链缺口）

本文档是对 2026-07-18/19 本地 vLLM + Qwen2.5-1.5B AI_COMPLETE baseline 系列的全面审计，记录已完成实验、已证明的 claim、未完成的缺口、指标盲区、下一步实验路线图，以及 2026-07-23 完整问题审计（P0/P1/P2 分级 + 认知债务清单）。

## 1. 实验全景：已完成 vs 未完成

### 1.1 研究内容一：数据组织策略

| 实验 | 状态 | 证明了什么 | 没证明什么 |
|---|---|---|---|
| 固定行 batch sweep（synthetic prompt） | ✅ 07-18 | 链路跑通 | 不是真实 workload baseline |
| ShareGPT/BurstGPT Ray 静态 batch sweep | ✅ 07-18 | Ray task > Ray actor；batch=16 时 ~260 rows/s | 离线扫表（doc_id 序），不反映在线到达 |
| Token-tail 修订版（batch 1~128, 512 行）| ✅ 07-19 | **固定行 batch 是计算量的弱代理**：batch=8 时 token 跨度 13.9×；batch=128 时 token P95=26678 | — |
| Token-budget vs Fixed Row（timeout=300）| ✅ 07-19 | **Token-budget 能约束 token tail**：6144/8192 吞吐接近 fixed 32/64，token P95 大幅降低 | 4096 吞吐更低（tradeoff）；未证明在所有场景下优于 fixed |
| Length-align + Prefix-aware ablation | ✅ 07-19 | length+fixed 是负结果（token P95=33407）；prefix+token6144 吞吐最高（339 rows/s）但 prefix ratio 仅 6.4% | length-align 需配 token-budget；prefix 信号太弱 |
| **Prefix 受控 workload 实验** | ✅ 07-26 | 0/30/70/100% cache-off screen；修复唯一 prefix 重排与隐式 length-align 耦合 | prefix cache 开启后的命中收益仍未验证 |

**RC1 当前状态**：✅ 动机成立，策略机制已验证。⚠️ 但不是"全面胜利"——token-budget 控制 token tail 的代价是更多 HTTP 调用，这个 tradeoff 本身是论文的讨论点。

### 1.2 研究内容二：调度与提交控制策略

| 实验 | 状态 | 证明了什么 | 没证明什么 |
|---|---|---|---|
| Arrival-aware K_max sweep（token6144 固定）| ✅ 07-19 | K_max=1→8 吞吐 140→329 rows/s；超 8 无收益 | 单 shape 扫参，已被后续实验替代 |
| Batch Policy × K_max 矩阵 | ✅ 07-19 | K_max 和 batch shape 耦合：fixed128 只有 4 个请求，K_max>4 无调度空间 | 仍是单 job 离线场景 |
| Shared-vLLM K_max 干扰（2-job）| ✅ 07-19 | **K_max 在共享 vLLM 下必要**：bulk unbounded 时 foreground E2E 恶化 2.3×（4.9→11.4s），bulk 自身吞吐几乎不变 | 只有 2 个 job；只有一种 foreground size |
| Shared-vLLM K_max Sweep + Adaptive | ✅ 07-19 | K_max=8 是最佳静态 guardrail；adaptive 触发了 downshift（102 次/run）| **❌ adaptive 不如 static K=8**（foreground E2E 10.2s vs 7.3s） |
| AIMD/EWMA-AIMD/PID 单作业 GPU 矩阵 | ✅ 07-26 | 三者相对 static K=8 快约 30–32% E2E，但都把窗口升到 K≈16 | AIMD 与 static K=16 不可分辨；未证明反馈控制增量，也未复验 shared-vLLM 前台保护 |
| Shared-vLLM typed AIMD + adaptive flush | ✅ 07-26 | static K8 保护前台；K16 提升后台吞吐。AIMD 三轮 0 decrease、窗口均值 15.953；adaptive flush 约 89.4% 选择 50ms | 只有 128/512 双作业规模；flush 分支为连续时间块，不是完整随机化 2×2 |
| **改进 adaptive flush** | ✅ 07-26 | 自然 EOS 重复、跨 arrival-rate 与 2048 held-out 均完成 | adaptive 未优于 fixed-50；当前默认 fixed 50ms |
| **Request-level continuous replenishment** | ❌ 未做 | vLLM 内部已有 continuous batching | Ray 上游仍按 submission 回收，尚未证明逐请求完成补位能减少 HOL 或提高 SLO goodput |
| **SLO-aware EWMA flush** | ❌ 未做 | 当前 two-level queue-adaptive 只作为真实链路 baseline | 尚未使用 oldest-request slack、服务速率、token backlog、EWMA/滞回形成完整控制律 |
| **多 job/多 foreground size 扩展** | ❌ 未做 | — | 不同 foreground size、arrival offset、background policy 下的公平性 |

**RC2 当前状态**：✅ static K8 guardrail 与 fixed 50ms coalescing 均有真实
证据。跨 arrival-rate、2048 held-out 和 shared-vLLM 双作业均未显示
queue-adaptive 稳定增量，因此当前单 GPU 默认采用 static K8 + fixed 50ms。
单作业与 shared-vLLM 复验均表明 AIMD 未优于同上限 static K=16；不继续在
当前稳态 workload 上调 PID 参数。

### 1.3 耦合验证

| 实验 | 状态 | 证明了什么 |
|---|---|---|
| **独立最优拼接 vs 联合 grid search** | ✅ 07-26 | 18 单元筛选 + 4 候选重复；联合相对独立 -0.26% ± 2.07%，不可分辨 |

**状态**：本地单 GPU 已完成。当前证据支持分层独立优化，不支持增加联合在线
控制器；多模型、多 GPU 与跨 arrival-rate 的外推仍未验证。

### 1.4 多模态泛化验证

| 实验 | 状态 |
|---|---|
| CLIP embedding + ImageNet subset | ❌ 未做（scope 缩减条件：文本 RC1+RC2 消融完成前不启动）|

### 1.5 算子代价估计 & 写回

算子代价估计已完成补充性二次分析：283 条真实 profile、70 个配置组，五个
grouped held-out 切分平均 MAE 11.68s、MAPE 50.60%、R² 0.776。它仍不作为
独立研究内容；写回继续使用 PostgreSQL + pgvector 工程 baseline。

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
   ├── "Queue-adaptive flush 优于最佳静态 timeout"（未证明）
   ├── "上游 request-level continuous replenishment 能放大 vLLM continuous batching 收益"（未实现）
   ├── "SLO-aware 动态 flush 优于最佳静态窗口"（未实现）
   ├── "Prefix-aware 在 cache-off 受控 prefix 比例下有效"（未证明）
   └── "策略代码对多模态 workload 可复用"（未启动）
```

新增的部分证据：

- queue-adaptive 相对 fixed-25 有稳定收益，但 fixed-50 与其不可分辨；
- AIMD/EWMA/PID 相对 static K=8 的收益来自把窗口升至约 16；AIMD 与
  static K=16 不可分辨，尚无动态反馈增量证据；
- 当前单 GPU 下独立拼接与联合候选不可分辨，分层优化足够。

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

### 候选机制优先级（跨论文，2026-07-24）

设计各阶段实验时，"先试哪个机制"见下表。深度（控制律/旋钮/反馈信号）见对应精读笔记与 `research/knowledge_hub.md` §5；fatal flaw 见 `strategy_design_literature_basis.md` §3.1，不在此重复。

| 阶段 | 候选机制 | 来源指针 | 先试? | 隔离实验 |
|---|---|---|---|---|
| RC2（P0-1） | CONCUR 死区非对称 AIMD（无 EWMA，KV 信号，α=2 增/β=0.5 减） | `concur_2025.md`；§5.5 | ⭐⭐ 首选（= 下文 P0 改进方向的文献具象） | CONCUR-AIMD vs 两档 bang-bang vs 静态 K=8，记 K_max 时序 |
| RC2 | Clipper AIMD（加性增 + 10% 乘减） | `clipper_nsdi2017.md`（论文 §4.3.1）；§5.2 | ⭐ 同系族 ablation 对照 | 同上 |
| RC2 | Delayed batching（flush 时机子问题） | `clipper_nsdi2017.md`（论文 §4.3.2）；§5.2 | ⭐ | 扫 flush wait timeout |
| RC2 | DistServe M/D/1 / SABER USL | `distserve_osdi2024.md` / `saber_2025.md`；§5.5 | P2 | USL 拟合 + out-of-sample 残差审计 |
| RC1（P1-2） | Length-align+token-budget / Bin-packing | `bucketserve_2025.md` / `multibin_batching_2024.md`；§5.5 | ⭐ 正式对照未做 | token-only vs +length vs +bin-packing |
| RC1（P1-1） | Prefix-aware（受控 prefix ratio） | `vllm_sosp2023.md`（APC）；§5.1 | ⭐ 受控实验未做 | prefix ratio=0/30/70/100% |

CONCUR-AIMD 首选理由：无 EWMA 契合 `code/AGENTS.md` "保持简单"（Ray `ConcurrencyCapBackpressurePolicy` 因 ~400 行被废弃）、原生用 KV cache 信号（我们有 vLLM Prometheus）、非对称 AIMD 直接对应 P0-1 改进方向。**RC2 P0 前置**先做变长 output 重验（见 §6.1 P0-1 混淆变量 H），排除 `--completion-max-tokens 64` 固定 output 消除自回归不可预测性这个混淆变量，再投入控制器改进。

### P0：修 RC2 核心 claim（最高优先，1-2 周）

#### Arrival replay 单 GPU smoke 门禁（2026-07-25）

正式 flush 对比前先固定 `token_budget=6144`、静态 `K_max=8`，分别运行
`immediate`、`fixed_timeout`、`queue_adaptive`。每个策略执行 1 次 warm-up
和 1 次 smoke，链路必须是 PostgreSQL → Daft → Arrow → Ray task/actor →
真实 vLLM；不得用 fake backend 形成新结论。

只有以下产物均非空时才进入正式重复：

- 主运行 CSV（含 server/pgvector 版本、tokens/s、service p99）；
- per-request/submission 明细；
- flush trace 与 admission/control trace；
- GPU、vLLM queue/running/KV 时间序列；
- 保存完整命令、版本、workload、endpoint 和随机种子的 manifest。

`--source-order arrival_time` 只负责排序；必须同时使用
`--arrival-replay` 才能称为在线 flush 实验。本地 Daft/Ray contract 只证明
执行语义，不是性能证据。

**目标**：让 queue-adaptive flush 在同一 shared-vLLM setup 下超越静态 K_max=8。

**前置（2026-07-24 补充）**：变长 output 重验。当前实验 `--completion-max-tokens 64` 固定 output，消除了自回归"输出长度不可预测"特性（adaptive 的物理前提，见 `service_scheduling_backpressure.md` §0.5）。在改控制器前，先用变长 output（让模型按 EOS 自然早停）重跑 adaptive vs static K_max=8，排除这个混淆变量；保留固定 output 组作对照（隔离 prefill 异质性）。CSV 记录每请求实际 `completion_tokens`。详见 P0-1 的"混淆变量排查"段与假设 H。

**改进方向**：
1. 渐进 ramp-up：从 min=4 开始，每 N 次成功提交无 queue buildup 则 +2
2. 比例控制：不是两档切换，而是 `K_max = max(min, min(max, target × factor))`
3. 每次提交前检查 vLLM metrics，而非批量提交后

**放弃条件**：如果 3 轮改进后 adaptive 仍不能达到静态 K=8 的 90% 性能（foreground E2E ≤ 8s），RC2 降级为"K_max admission control 必要性论证 + queue-adaptive 作为 Discussion 探索方向"。

**同时追加指标**：inflight/queue 时间序列、K_max 时间序列、`tokens/s`。

**2026-07-26 执行结果**：变长输出、完整 control/request/resource trace 和
单作业 GPU 矩阵已完成。AIMD、EWMA-AIMD、PID 都迅速升到 K≈16；追加
static K=16 机制 control 后，AIMD 的 E2E +0.66%、tokens/s -0.69%，差异
不可分辨。shared-vLLM foreground/background 的 static K8/static K16/AIMD
三次重复已完成：AIMD 0 次 decrease、窗口均值 15.953，相对 K16 的前台 E2E
+1.22%、P99 +1.98%、后台真实 tokens/s -1.45%。追加 adaptive flush 后四项
变化仍小于 0.3%；当前收敛为 static K8 + fixed 50ms。

### P0（并列）：两项策略联合消融（1 周）

**目标**：回答"分层独立优化是否足够"。

**设计**：
- best token-budget（当前 6144）+ best K_max（当前 8）独立拼接
- vs token-budget × K_max 联合 grid search
- 保持同一 workload（ShareGPT/BurstGPT, 512 rows, arrival_time 序）

**同时追加指标**：`tokens/s`、`service_p99`。

### P1：Prefix cache 机制确认 + 显式联合消融

受控 prefix 0/30/70/100% 和 2048 请求扩展已经完成。下一步只在单独启用
prefix cache 并能记录命中证据时重验 prefix-aware；length-align 与 prefix
grouping 必须作为两个独立因素做显式联合消融。

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

## 6. 完整问题审计（2026-07-23）

以下审计覆盖所有已知问题（不含"ML as Native Operator"叙事定位问题，该问题已在 2026-07-23 对话中单独讨论，结论为搁置至后续阶段）。问题按 P0/P1/P2 分级。

### 6.1 P0 阻塞级：不解决无法写论文

#### P0-1：RC2 核心策略为负结果（adaptive < static K_max=8）

**事实**：Shared-vLLM interference 实验（07-19）：adaptive tuned 的 foreground E2E=10.2s，静态 K_max=8 的 foreground E2E=7.3s。Adaptive 触发了 102 次 downshift（`adaptive_limit_mean=9.2`），控制器在运作，但效果比简单静态 guardrail 差 ~40%。

**影响**：研究内容二（调度与提交控制策略）当前只能 claim "K_max admission control 是必要的"（动机），不能 claim "自适应提交控制是有效的"（方法）。

**放弃条件**（2026-07-20 约定）：3 轮改进后 foreground E2E 仍 > 8s（static K=8 的 90%）→ RC2 降级为"K_max 必要性论证 + queue-adaptive 探索性讨论"。

**改进方向**：渐进 ramp-up（min=4，每 N 次成功无 queue buildup 则 +2）、比例控制（非两档切换）、每次提交前检查 vLLM metrics（非批量提交后）。

**混淆变量排查（2026-07-24 补充）**：在投入控制器改进前，先排除一个未被识别的混淆变量——当前实验 `--completion-max-tokens 64` 把 output 固定，消除了自回归"输出长度不可预测"特性；请求完成时间因此相对可预测（主要由 prompt/prefill 决定），running/waiting 波动主要来自到达节奏而非完成异质，adaptive 基于运行时信号的动态优势可能无从发挥。**建议在 3 轮改进中并行纳入变长 output 重验**（让模型按 EOS 自然早停），避免在固定 output 设置上耗尽改进轮次。这是假设（H），非结论——output 固定可能只是原因之一。

**同时追加指标**：inflight/queue 时间序列、K_max 时间序列、`tokens/s`、`service_p99`。

#### P0-2：两项策略联合消融完全没有数据

**事实**：AGENTS.md §1 写死的核心验证——"分别独立搜索最优配置后拼接，再与联合 grid search 对比"。当前状态：batch_policy × K_max matrix 实验（07-19）已证明两者耦合（如 fixed128 只有 4 个 submission，K_max>4 无调度空间），但独立最优拼接 vs 联合 grid search 未跑。

**需要回答**：token-budget 最优值（当前 6144）+ K_max 最优值（当前 8）独立拼接，是否与 joint space 中搜索的 (token_budget*, K_max*) 一致？
- 一致 → 分层独立优化即可，论文可分开写两项策略
- 不一致 → 必须联合优化，论文只有一个贡献（joint scheduling）

**设计**：token_budget ∈ {4096, 6144, 8192} × K_max ∈ {4, 8, 16}，共 9 点 grid。同一 workload（ShareGPT/BurstGPT, 512 rows, arrival_time 序）。

**同时追加指标**：`tokens/s`、`service_p99`。

#### P0-3：关键指标 `tokens/s` 缺失，`rows/s` 在 AI_COMPLETE 场景下是有偏指标

**2026-07-25 状态更新：本轮新增实验已修复。** profiler 现在直接输出
`tokens_per_s`；加速到达 flush 实验对已经完成的 15 条正式运行使用 vLLM
Prometheus 的实际 `prompt_tokens_delta + generation_tokens_delta` 事后无损补算，
结果见 `experiments/results/accelerated_arrival_flush_20260725/`。旧实验仍需在
跨 workload 比较前补算，不能把本项标记为全历史数据已修复。

**事实**：所有实验使用 `rows/s` 作为主吞吐指标，但同一 workload 中每行 token 量可差 13.9×（batch=8 时 token 跨度从几十到几千）。Token-budget=4096 的 rows/s（301）低于 fixed 32（325），但如果计算 `tokens/s`，4096 可能持平甚至更高。

**影响**：无法公平比较不同策略的效率。token-budget 策略的核心 tradeoff（更多小请求 vs 更少大请求）在 `rows/s` 指标下被扭曲。

**同样缺失**：
- `service_p99`：系统性 tail latency 采集（当前仅 P95）
- inflight/queue 时间序列：只有终值 gauge，无法诊断 adaptive 行为
- per-request e2e latency 分布：对 length-align/prefix-aware 分组策略论证至关重要

**量化方法**：`tokens/s = SUM(prompt_tokens + completion_tokens) / operator_wall_s`。对于使用同一 tokenizer 的 workload，`prompt_tokens` 列已存在；可累计每行的 `prompt_tokens + completion_max_tokens` 作为计算量 proxy。

#### 2026-07-25 加速到达 flush 策略筛选结果

**事实（真实单 GPU E2E）**：在 ShareGPT/BurstGPT 前 512 条、
arrival scale `0.0005`、token budget 6144、静态 `K_max=8` 下，每策略 1 次
预热 + 5 次正式重复。fixed timeout 相对 immediate 将 submission 减少
8.984%，但 tokens/s 仅提高 0.185%，置信区间重叠。当前 queue-adaptive
平均 batch rows 为 1.0，tokens/s 低 0.966%，没有形成有效 coalescing。

**设计判定**：当前 queue-adaptive 版本不进入联合搜索。下一轮必须先在 64 条
真实门禁中同时满足 exactly-once、平均 batch rows > 1 和 service P99
guardrail，再运行 512 条矩阵。完整证据、故障恢复记录和 claim boundary 见
`experiments/results/accelerated_arrival_flush_20260725/README.md`。

#### 2026-07-25 双窗口 adaptive flush 改进结果

**事实（真实单 GPU E2E）**：修正低负载 fallback、`K_max` 压力阈值和
event-time catch-up 后，64 行与 1024 行门禁均通过。512 行、每策略 1 次预热 +
5 次正式重复中，adaptive 相对新版 fixed timeout 的 observed tokens/s 提升
3.671%，submissions 减少 23.500%，平均 batch rows 提升 30.732%，batch
service P99 均值降低 8.010%。每轮 512 个文档 exactly-once。

**设计判定**：queue-adaptive flush 已从“不能形成 batch”推进为正向候选策略，
可以进入随机化复验和 batching × submission 联合搜索候选池。仍不能标记为最终
验证完成：当前策略按组运行而非逐 repeat 随机化，生成上限固定为 16 tokens，
尚缺 per-request E2E P99、变长输出和 2048 行 held-out。完整结果见
`experiments/results/adaptive_flush_window_20260725/README.md`。

### 6.2 P1 严重级：需补实验，但不会动摇论文根基

#### P1-1：Prefix-aware 在自然 workload 上信号太弱（6.4%），未做受控实验

**事实**（07-19 ablation）：prefix ratio 从 4.1%（random）提升到 6.4%（prefix-aware），不足以支撑 prefix-aware 有效性论证。

**需要**：构造 prefix ratio = 0/30/70/100% 的受控 workload，仅在 prefix+token6144 条件下评估。需采集 vLLM APC/cache metrics（如果 vLLM 暴露）。

**诚实考量**：如果自然 workload（ShareGPT/BurstGPT）只有 4-6% prefix share，prefix-aware 在实际场景中的收益也许天然有限——这本身是一个有价值的发现，需诚实面对。

#### P1-2：Length-align + fixed rows 是负结果，正确组合（length-align + token-budget）未做正式对照

**事实**（07-19 ablation）：length + fixed 32 导致 token P95=33407（因为长文本被集中到同一 fixed-row batch）。`length + token 6144` 的 token P95=6126，效果好，但它是 ablation 的一部分而非正式对照实验。

**需要**：正式对比 token-budget-only vs token-budget+length-align vs token-budget+bin-packing，在同一 workload 和 metric 下。

#### P1-3：所有实验 512 行规模，无 scale-out 验证

**事实**：所有 07-18/19 实验均为 `total_rows=512`。2048 行扩展在计划中但未执行。

**风险**：512 行下 K_max=8 饱和；2048 行下最优 K_max 可能是 16 或 32。当前的"最优"参数组合可能只是小规模 artifact。

**需要**：至少一个实验（最优 token-budget + 最优 K_max）scale 到 2048 行。

#### P1-4：Token-budget 的 tradeoff 未系统表征

**事实**：Token-budget=4096 约束 token P95 至 4092，但 model calls 从 4（fixed128）增至 19。Tradeoff 存在但未被定量分析。

**需要**：系统表征"token tail 每降低 X%，HTTP 调用增加 Y%"的关系曲线。这本身是论文的有效讨论点——"token-budget 不是免费午餐，但在 token tail 敏感的 scenario 下是合理的 tradeoff"。

### 6.3 P2 方法论/设计问题

#### P2-1：Daft 引擎级参数实验空间完全未探索

**事实**：优化空间定义为"策略级决策 + 引擎级参数"，但当前实验仅覆盖策略级。Daft 的 `into_batches`、`repartition`、`@daft.cls batch_size`、`max_concurrency` 等引擎级参数无系统实验数据。

**选择**：要么砍掉"引擎级参数系统表征" claim（诚实说明"本文聚焦策略级决策，引擎级参数使用推荐值"），要么花 1 周跑参数 sweep。

#### P2-2：单 job 离线扫表 vs arrival-aware 之间的叙事断层

**事实**：早期实验（token-tail revision、token-budget vs fixed）用 `--source-order doc_id`（离线扫表模式），后期 K_max 实验才切换到 `--source-order arrival_time`。论文不能从离线扫表实验直接跳到"arrival-aware scheduling 需要 K_max"的结论。

**缓解**：在论文中明确区分两种实验模式的角色——离线扫表回答"数据组织"，arrival-aware 回答"提交控制"。或对关键实验用两种 source_order 各跑一遍。

#### P2-3：Baseline 矩阵大量未实际运行

**事实**：`baseline_reference.md` 定义 G1-G6、W1-W7、D1-D4、X1-X3 共 20 个 baseline，实际跑过的 <5 个。不影响核心贡献，但审稿人可能问"为什么不和 X baseline 比"。

**缓解**：投稿前清理 baseline 文档——实际跑过的标 ✅，计划但未跑的标"不在本文 scope 内"，避免给审稿人留下"承诺了但没做"的印象。

#### P2-4：无多 endpoint / 多 GPU 实验

**事实**：所有 AI_COMPLETE 实验均为单 RTX 5070 + 单 vLLM 实例。多 endpoint 是"actor pool 分池路由"的前置场景——无多 endpoint 则分池路由无意义。AI_EMBED 预研做了双 endpoint，但 AI_COMPLETE（主场景）无。

#### P2-5：跨查询 batching 是隐含效果而非显式策略

**事实**：vLLM 内部做 continuous batching（请求自动合并），但 Ray 层没有显式的"跨查询请求融合"机制。当前 Shared-vLLM K_max Interference 实验是两 job 共享同一 endpoint（跨查询共享服务），不是跨查询主动合并请求。

**多模态场景下的重要性提升（2026-07-23 更新）**：在纯文本场景下 vLLM 的 continuous batching 掩盖了"没有跨查询请求池"这个问题——所有 AI_COMPLETE 请求都走同一个 vLLM endpoint，vLLM 内部自动合并。但在多模态场景下：
- AI_COMPLETE → vLLM（Qwen2.5-1.5B）
- AI_EMBED → CLIP endpoint（**没有 continuous batching**）
- AI_CLASSIFY → Qwen2.5-VL endpoint

CLIP embedding 模型通常没有类似 vLLM 的 continuous batching 调度器，不同 SQL 查询的 AI_EMBED 请求如果不显式合并，就是各自发小 batch → GPU 利用率低。因此跨查询请求池在多模态场景下从"vLLM 代劳"变为"必须自己做"。

**论文影响**：如果 claim "跨查询 continuous batching"作为方法贡献，需要在 Ray 层实现显式的全局请求池 + 算子类型感知路由（同类合并、异类分池）。纯文本场景下这个贡献被 vLLM 内部机制掩盖，多模态场景才是它真正体现价值的地方。

**与 RC2 的关系**：如果 adaptive 控制器在 P0 阶段降级，跨查询合并 + 算子类型感知路由可以作为 RC2 的方法补充贡献，不依赖 adaptive 控制器的性能。

### 6.4 认知债务：文档承诺 vs 实际交付

| 文档中的承诺 | 实际状态 |
|---|---|
| baseline_reference.md：G1-G6 + W1-W7 + D1-D4 + X1-X3（20 个 baseline）| 实际跑过 <5 个 |
| knowledge_hub.md §10.5.1：优化空间三层框架，"引擎级参数系统表征" | 引擎级参数实验为 0 |
| knowledge_hub.md §7.2：实验五阶段（前置→一→二→三→四）| 阶段三（耦合验证）未做、阶段四（写回）降级 |
| PROJECT_OUTLINE.md：actor pool 分池路由、异构 actor pool | 无多 endpoint 实验，分池路由无场景 |

**行动**：投稿前必须清理——要么补齐关键 baseline，要么诚实标注"不在本文 scope"。

---

## 7. 更新检查清单

当本文件中的缺口被新的实验结果填补时，同步更新：
- `experiments/results/local_vllm_qwen15b_baseline/README.md`
- `PROJECT_OUTLINE.md` §当前最重要证据、§近期优先级
- `PROJECT_LOG.md`
- `figures/README.md`（如有新增图）
- `learning/local_vllm_ray_baseline_walkthrough.md`（如实验结果影响讲解）
- 本文件 §6 完整问题审计（标记已修复的问题）

## 8. 2026-07-25 Request lifecycle 基础设施门禁

**已补齐的观测缺口**：

- 真实 64-prompt `PostgreSQL -> Daft -> Arrow -> Ray task -> vLLM` 路径已输出
  client-observed request E2E P50/P95/P99、SLO violation/goodput、request 与
  submission 显式外键；
- seeded runner 已验证固定 seed 顺序、运行前空闲门禁、失败即停、incident
  审计、凭据脱敏和原子 manifest；
- request、submission、flush、resource 和 run CSV 均带 PostgreSQL/pgvector
  版本，最终 exactly-once 与分位数重算通过。

**边界**：该门禁只有 fixed/adaptive 各一次，且规模为 64，不替代多轮正式对比；
1 秒 SLO 是 instrumentation 阈值而非业务 SLO；submission endpoint 内的 prompt
共享 completion timestamp，仍不是 vLLM 内部逐 sequence 完成时间。

原始数据和七步解释见
`experiments/results/request_lifecycle_gate_20260725/README.md`。

## 9. 2026-07-26 提交控制与联合实验闭环

### 已补齐

- vLLM 每个 choice 的真实 output-token count 与 finish reason；
- ChatML 自然 EOS 门禁，以及不截断 prompt 的 context-safe 数据源过滤；
- 512 请求 fixed-25 vs queue-adaptive 随机化 n=5；
- token budget `{4096,6144,8192}` × K_max `{4,8,16}` ×
  fixed/adaptive 的 18 单元 SLO-constrained 筛选；
- 独立拼接、联合候选、fixed-25 baseline、fixed-50 机制对照各 n=3。

### 当前结论

- K16 吞吐最高，但所有单元均超过 1% SLO violation 门槛；
- 独立拼接相对 fixed-25 tokens/s `+4.76% ± 2.29%`；
- 联合候选相对独立拼接 `-0.26% ± 2.07%`，没有可分辨增量；
- 相同 8192/K8 下 adaptive 相对 fixed-50 `-0.75% ± 0.97%`；
- 当前默认应保持 sequential token-budget + static K8，并在本 workload 使用
  简单 fixed-50；adaptive 只保留为跨 arrival-rate 候选。

### 剩余关键缺口

1. 上游 request-level continuous replenishment 与 whole-submission barrier 对照；
2. SLO-aware EWMA flush 与最佳静态窗口、现有 two-level baseline 对照；
3. prefix cache-on、多模态复用、多 endpoint/多 GPU；
4. shared-vLLM 的多 foreground size、arrival offset 和多 job 公平性。

原始数据与七步解释见：

- `experiments/results/adaptive_flush_randomized_20260726/README.md`
- `experiments/results/joint_batching_submission_512_20260726/README.md`

## 10. 2026-07-26 文献驱动执行链缺口重审

### 10.1 关键边界校正

- Orca/vLLM 的 iteration-level/continuous batching 位于模型服务内部：完成请求
  会被移出执行集合，waiting 请求可在后续迭代中进入；
- 当前 Daft/Ray 上游没有修改 vLLM，但 submission 仍可能形成整批完成屏障；
- 因此“vLLM 已有 continuous batching”和“上游已能逐请求持续补位”不是同一件事；
- 当前两档 `QueueAdaptiveFlush` 已完成代码和实验，但只是 baseline，不能标成
  Clipper/Clockwork/CONCUR 等文献机制的完整落地。

### 10.2 新增代码缺口

| 缺口 | 当前状态 | 最小闭环 |
|---|---|---|
| Request-level completion/credit release | lifecycle 有 request trace，但 admission 主要按 submission 回收 | 任一请求完成释放自身 credit，保持 exactly-once |
| Continuous replenishment | vLLM 内部具备；上游未显式实现 | whole-submission vs request-credit 对照 |
| Token-work admission | 以 submission/request 上限为主 | active request cap + active estimated token-work cap |
| SLO-aware adaptive flush | two-level 25/50ms baseline | oldest age/slack + fill + EWMA service/arrival + hard deadline |
| Completion-span/HOL 观测 | 有 request/submission join key | 记录同 submission 首末完成跨度和 credit idle |
| Endpoint-local controller | topology/接口具备 | 两个真实 endpoint 后验证独立状态与回退 |

### 10.3 推荐顺序与成功标准

1. 先补 request-level completion/replenishment，不同时修改 flush 控制律；
2. 用最佳静态 K/timeout 比较 whole-submission 与 request-credit；
3. 再实现 SLO-aware EWMA flush，比较 fixed-best、two-level 和 EWMA；
4. 只在两项独立收益成立后做小规模联合矩阵；
5. UCB 必须等 epoch reward 能按产生请求的 arm 正确归因后再接入。

晋级要求是相对最佳静态基线改善 observed tokens/s 或 SLO goodput，且 request
P99、failure、exactly-once 不退化。否则记录负结果，不增加控制复杂度。

完整机制卡、文献映射、fatal-flaw audit 和候选池见
`literature_driven_pipeline_optimization_guide.md`。
