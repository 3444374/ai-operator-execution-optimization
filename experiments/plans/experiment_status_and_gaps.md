# 实验状态与缺口分析

Date: 2026-07-20（最后更新：2026-07-29，补充扩展饱和曲线与 Ray actor/service-quantum 门禁）

本文档是对 2026-07-18/19 本地 vLLM + Qwen2.5-1.5B AI_COMPLETE baseline 系列的全面审计，记录已完成实验、已证明的 claim、未完成的缺口、指标盲区、下一步实验路线图，以及 2026-07-23 完整问题审计（P0/P1/P2 分级 + 认知债务清单）。

## 1. 实验全景：已完成 vs 未完成

### 1.1 研究内容一：数据组织策略

| 实验 | 状态 | 证明了什么 | 没证明什么 |
|---|---|---|---|
| 固定行 batch sweep（synthetic prompt） | ✅ 07-18 | 链路跑通 | 不是真实 workload baseline |
| ShareGPT/BurstGPT Ray 静态 batch sweep | ✅ 07-18 | Ray task > Ray actor；batch=16 时 ~260 rows/s | 离线扫表（doc_id 序），不反映在线到达 |
| Token-tail 修订版（batch 1~128, 512 行）| ✅ 07-19 | **固定行 batch 是计算量的弱代理**：batch=8 时 token 跨度 13.9×；batch=128 时 token P95=26678 | — |
| Token-budget vs Fixed Row（timeout=300）| ✅ 07-19 | **Token-budget 能约束 token tail**：6144/8192 吞吐接近 fixed 32/64，token P95 大幅降低 | 4096 吞吐更低（tradeoff）；未证明在所有场景下优于 fixed |
| **Token-budget 1024–32768 容量曲线** | ⏳ 配置完成 | — | 预算甜点、过大预算的 completion barrier/HOL 代价、动态预算动作集 |
| Length-align + Prefix-aware ablation | ✅ 07-19 | length+fixed 是负结果（token P95=33407）；prefix+token6144 吞吐最高（339 rows/s）但 prefix ratio 仅 6.4% | length-align 需配 token-budget；prefix 信号太弱 |
| **Prefix 受控 workload 实验** | ✅ 07-26 | 0/30/70/100% cache-off screen；修复唯一 prefix 重排与隐式 length-align 耦合 | prefix cache 开启后的命中收益仍未验证 |

**RC1 当前状态**：✅ 动机成立，策略机制已验证。⚠️ 但不是"全面胜利"——token-budget 控制 token tail 的代价是更多 HTTP 调用，这个 tradeoff 本身是论文的讨论点。

### 1.2 研究内容二：调度与提交控制策略

| 实验 | 状态 | 证明了什么 | 没证明什么 |
|---|---|---|---|
| Arrival-aware K_max sweep（token6144 固定）| ✅ 07-19 | K_max=1→8 吞吐 140→329 rows/s；超 8 无收益 | 单 shape 扫参，已被后续实验替代 |
| Batch Policy × K_max 矩阵 | ✅ 07-19 | K_max 和 batch shape 耦合：fixed128 只有 4 个请求，K_max>4 无调度空间 | 仍是单 job 离线场景 |
| Shared-vLLM K_max 干扰（2-job）| ✅ 07-19 | **K_max 在共享 vLLM 下必要**：bulk unbounded 时 foreground E2E 恶化 2.3×（4.9→11.4s），bulk 自身吞吐几乎不变 | 早期预研；已被 07-26 typed AIMD + 机制 control 复验取代 |
| Shared-vLLM K_max Sweep + Adaptive | ✅ 07-19 | K_max=8 是最佳静态 guardrail；adaptive 触发了 downshift（102 次/run）| **❌ adaptive 不如 static K=8**（foreground E2E 10.2s vs 7.3s）；早期 adaptive 实现已被 07-26 版本取代 |
| AIMD/EWMA-AIMD/PID 单作业 GPU 矩阵 | ✅ 07-26 | 三者相对 static K=8 快约 30–32% E2E，但都把窗口升到 K≈16 | AIMD 与 static K=16 不可分辨；未证明反馈控制增量，也未复验 shared-vLLM 前台保护 |
| Shared-vLLM typed AIMD + adaptive flush | ✅ 07-26 | **static K8 保护前台**（E2E -27.9%、P99 -40.0% vs K16）；AIMD 0 decrease、窗口均值 15.953，与 K16 不可分辨。**根因诊断**：vLLM waiting=0 但前台已慢 38.9%——AIMD 盯着 vLLM waiting 做决策，但请求在 Ray actor 侧排队、waiting 始终为 0 | 只有 128/512 双作业；flush 分支不是完整 2×2 随机化；多 foreground size/arrival offset/>2 job 均未测试 |
| **改进 adaptive flush** | ✅ 07-26 | 自然 EOS 重复、跨 arrival-rate 与 2048 held-out 均完成 | adaptive 未优于 fixed-50；当前默认 fixed 50ms |
| **Request-level continuous replenishment** | ⚠️ 双卡重复已完成 | global K32≈per-endpoint K16，确认 K 语义；work-matched request K48≈batch K16；request K64 为最高已测吞吐 | K64 同时增加约 33% offered work 且 P99 更差，尚未隔离补位机制的独立吞吐/SLO 收益；需固定 active work 复验 |
| **Per-endpoint active-work capacity** | ✅ 07-29 扩展曲线完成 | 双 4090 八档各三次 formal；32/32 成功，65K 达最大吞吐 97.80%，下一档 +0.92% | 按预注册规则选择 65,536；98K→131K 吞吐持平而 P99/SLO 更差 |
| **SLO-aware EWMA flush** | ❌ 未做 | 当前 two-level queue-adaptive 只作为真实链路 baseline | 尚未使用 oldest-request slack、服务速率、token backlog、EWMA/滞回形成完整控制律 |
| **多 job/多 foreground size 扩展** | ⏳ 代码完成，GPU 未测 | job-local K 不构成共享 endpoint 的全局保护 | 1/2/4 job、不同 mix/offset；shared request/work credit 与公平队列待远端门禁和正式验证 |

**RC2 当前状态**：✅ static K8 guardrail 与 fixed 50ms coalescing 均有真实
证据。跨 arrival-rate、2048 held-out 和 shared-vLLM 双作业均未显示
queue-adaptive 稳定增量，因此当前单 GPU 默认采用 static K8 + fixed 50ms。
单作业与 shared-vLLM 复验均表明 AIMD 未优于同上限 static K=16，且根因不
是控制器参数问题——shared-vLLM 实验中 vLLM waiting 始终为 0（请求在 Ray
侧排队），AIMD 看的拥塞信号（vLLM waiting > 0 / KV usage 高）不反映 Ray 侧积压——形成"软拥塞"，即请求在 Ray actor 侧排队但 vLLM waiting 仍显示空闲。
不继续在当前稳态 workload 上调 PID 参数；动态控制在负载阶段变化/多租户/
多 GPU 场景下仍是开放问题。

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
grouped held-out 切分平均 MAE 11.68s、MAPE 50.60%、R² 0.776。定位为补充
讨论，不作为独立研究内容；写回继续使用 PostgreSQL + pgvector 工程 baseline。

**两个预期用途**：
1. **数据库优化编排**（主要）：为查询优化器提供 AI 算子代价估计，辅助
   选择执行计划和资源分配；
2. **提交策略辅助**（探索性）：作为 vLLM Prometheus 信号的补充，提供
   pending batch 的粗粒度工作量预估（轻/中/重分类），但不替代 Orca 式
   持续供给和 vLLM 反馈驱动的提交机制。

**当前缺口**：
- 排序能力未评估：R² 0.776 暗示排序大概率不错，但未计算 Spearman 秩相关、
  pairwise accuracy 和 Top-K precision——这些是编排决策更关心的指标；
- 提交策略集成未经验证：代价估计能否将配置可靠地分为"轻/中/重"三档？
  分档后同档内 E2E 方差是否显著小于全局？决定了能否用于提交侧 workload
  分类；
- 无独立 workload/时间留出验证：所有数据来自 07-18 至 07-26，外推退化
  程度未知；
- 点估计无预测区间：编排决策仅靠一个数字，无法评估风险。

详见 `experiments/results/operator_cost_estimation_20260726/README.md`。

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
   ├── "上游 request-level continuous replenishment 能放大 vLLM continuous batching 收益"（双卡链路已跑通，但 work-matched K48 与 batch K16 吞吐不可分辨）
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

#### P0-1：RC2 核心策略——动态控制未证明优于最佳静态（已从"负结果"推进为"信号选择诊断"）

**07-19 初始发现**：Shared-vLLM interference 实验：adaptive tuned 的 foreground E2E=10.2s，静态 K_max=8 的 foreground E2E=7.3s。Adaptive 触发了 102 次 downshift，控制器在运作，但效果比简单静态 guardrail 差 ~40%。

**07-26 复验（typed AIMD + 机制 control）**：

- **单作业**：AIMD/EWMA-AIMD/PID 全部迅速升到 K≈16；加入 static K=16 机制对照后，AIMD 的 E2E +0.66%、tokens/s -0.69%，差异不可分辨。三者相对 static K=8 的 ~30% E2E 改善来自"更高并发"而非"动态反馈"。
- **Shared-vLLM 前台/后台（128/512）**：static K=8 将前台 E2E 降低 27.9%、P99 降低 40.0%（相对 K=16），确认 guardrail 价值。但 AIMD 三轮 **0 次 decrease**、774 次决策仅 12 次 increase，窗口从 8 快速升至 16 后不变（均值 15.953）。相对 K=16 前台 E2E +1.22%、P99 +1.98%、后台 tokens/s -1.45%。Adaptive flush 追加后四项变化 <0.3%。
- **根因诊断**：AIMD 的拥塞信号（vLLM waiting > 0 / KV usage 高）在 shared-vLLM 场景下完全看不到（vLLM waiting=0 但请求已在 Ray 侧积压）——前台延迟已恶化 38.9%，但 vLLM `waiting` 始终为 0（请求在 Ray 侧排队，尚未进入 vLLM waiting 队列）。控制器观测不到"软拥塞"，自然不会降载。这不是控制器参数问题，是**观测信号的表达能力不足**。

**影响**：研究内容二可以 claim "K_max admission control 在共享 vLLM 下是必要的 guardrail"（✅ 强证据），但不能 claim "自适应提交控制是有效的"。当前默认使用 static K=8 + fixed 50ms。

**演进判断**：变长 output（自然 EOS 上限 512）已纳入 07-26 实验，排除了固定 output 混淆变量。不再继续在稳态 workload 上调 AIMD/EWMA/PID 参数。若继续动态控制方向，必须改用反映 Ray 侧积压的信号——候选路径包括逐请求完成时间（request-level replenishment 的副产品）或端到端 SLO slack 作为反馈信号。

**放弃条件**（已触发，但结论从"负结果"变为"边界条件"）：动态控制器在稳态 workload 的三种独立实现均未优于最佳静态上限。论文可诚实 framing 为"在稳态 workload 下简单静态 guardrail 足够；负载阶段变化/多租户/多 GPU 下的动态控制仍是开放问题"，而非回避或强行包装为正面结果。

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
- 独立拼接、联合候选、fixed-25 baseline、fixed-50 机制对照各 n=3；
- **单作业 AIMD/EWMA-AIMD/PID 矩阵**：三者基于不同控制律，均在
  workload backlog 下迅速升到 K≈16；加入同上限 static K=16 对照后不可分辨；
- **Shared-vLLM typed AIMD + adaptive flush（128 前台 / 512 后台）**：
  static K=8 保护前台（E2E -27.9%、P99 -40.0%），AIMD 0 decrease、
  窗口均值 15.953，与 K=16 不可分辨；adaptive flush 约 89.4% 选 50ms，
  行为接近 fixed-50。

### 当前结论

- K16 吞吐最高，但所有单元均超过 1% SLO violation 门槛；
- 独立拼接相对 fixed-25 tokens/s `+4.76% ± 2.29%`；
- 联合候选相对独立拼接 `-0.26% ± 2.07%`，没有可分辨增量；
- 相同 8192/K8 下 adaptive 相对 fixed-50 `-0.75% ± 0.97%`；
- 当前默认应保持 sequential token-budget + static K8，并在本 workload 使用
  简单 fixed-50；adaptive 只保留为跨 arrival-rate 候选；
- **Shared-vLLM**：K_max guardrail 价值已在 07-19 初步证明、07-26 typed AIMD
  复验证实；但 AIMD 无法观测 Ray 侧软拥塞（vLLM waiting=0），动态控制
  相对最佳静态无增量；当前共享服务默认继续使用 static K=8 + fixed 50ms。

### 剩余关键缺口

1. 用相同 per-GPU K 完成单/双 endpoint 容量曲线，替代历史 global K 同值
   的不公平对照；
2. 07-29 八档 request-level active-work 扩展曲线已完成，按预注册规则选择
   65,536；
3. 已在该饱和点固定每 endpoint 256 actor slots，完成
   1×256/2×128/4×64 三次重复。16-slot 草案按当前 332 work/request 与
   1337 work/organization-batch 估算会严重欠载，已在启动前否决；三个
   arm 的每 endpoint Ray CPU reservation 同时固定为 0.5。2×128/4×64
   相对 1×256 仅 +2.00%/+0.75%，未过 5% 门槛，保留 1×256；
4. 已固定 1×256 pool、planning budget 和 active work，完成 whole batch、
   complete-row service quantum 512/1024/2048/4096 与 request diagnostic。
   512/request 将 credit-held 降约 16%，但吞吐相对 batch 最高仅 +1.75%，
   固定 quantum 不晋升；8192 因会退化为 batch control 未运行；
5. SLO-aware EWMA flush 与最佳静态窗口、现有 two-level baseline 对照；
6. prefix cache-on、多模态复用，以及 shared-vLLM 的 1/2/4 job、workload
   mix、arrival offset 和共享 endpoint credit/fairness；
7. 动态控制的信号选择问题——逐请求完成时间或端到端 SLO slack 可能替代
   当前 vLLM waiting 信号（不反映 Ray 侧积压），但尚未验证。

原始数据与七步解释见：

- `experiments/results/adaptive_flush_randomized_20260726/README.md`
- `experiments/results/joint_batching_submission_512_20260726/README.md`
- `experiments/results/shared_vllm_adaptive_admission_20260726/README.md`
- `experiments/results/adaptive_admission_controller_20260726/README.md`

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
| Request-level completion/credit release | 已实现并完成双 4090 重复 | 固定 active work 后继续验证 exactly-once 与逐请求释放 |
| Continuous replenishment | 上游已实现；K-count 双卡对照未隔离独立收益 | 固定 active work 的 whole-submission vs request-credit 对照 |
| Token-work admission | 已实现；八档扩展曲线完成并选定 65,536 | 后续策略固定该 work，不再靠增加 offered load 获得表面收益 |
| Complete-row service quantum | gate 与 24-run 正式重复完成 | credit-held 降约 16% 但吞吐增益不足 5%；不晋升固定 quantum，request 保留作精确控制基础 |
| Bounded Ray actor pool | 固定 slots、worker routing 与失败清理已实现并完成正式重复 | 多 actor 未过 5% 晋升门槛；当前保留 1×256，多 job 分池另行验证 |
| SLO-aware adaptive flush | two-level 25/50ms baseline | oldest age/slack + fill + EWMA service/arrival + hard deadline |
| Completion-span/HOL 观测 | 有 request/submission join key | 记录同 submission 首末完成跨度和 credit idle |
| Endpoint-local controller | topology/接口具备 | 两个真实 endpoint 后验证独立状态与回退 |

**2026-07-27 shared-vLLM 信号选择诊断补充**：shared-vLLM 实验中 AIMD 0 次
decrease 的根因不是控制器参数问题，而是 vLLM Prometheus `waiting` 始终为 0
（请求在 Ray 侧排队，尚未进入 vLLM waiting 队列），当前观测信号无法识别
"软拥塞"。Completion-span/HOL 观测和 request-level replenishment 的副产品——
逐请求完成时间——可作为反映 Ray 侧积压的信号，使动态控制真正有价值。这
将 request-level replenishment 的优先级从"工程改进"提升为"可能解锁动态控制
价值的必要前置"。

### 10.3 推荐顺序与成功标准

1. 完成 16K–131K active-work 扩展曲线，按最大安全吞吐 97% 与下一安全档
   增益 <3% 的预注册规则选择最小饱和点；
2. 固定该 work 和每 endpoint 256 slots，先用 request granularity 比较
   1×256/2×128/4×64 actor pool；
3. 固定最佳 pool、planning budget、work、row cap 与 timeout，比较
   whole-batch、service quantum 512/1024/2048/4096 和 request diagnostic；
4. 只有出现 worker imbalance 时才增加 least-active-work routing；
5. 再实现 SLO-aware EWMA flush，比较 fixed-best、two-level 和 EWMA；
6. 只在独立收益成立后做小规模联合矩阵；
7. UCB 必须等 epoch reward 能按产生请求的 arm 正确归因后再接入。

晋级要求是相对最佳静态基线改善 observed tokens/s 或 SLO goodput，且 request
P99、failure、exactly-once 不退化。否则记录负结果，不增加控制复杂度。

完整机制卡、文献映射、fatal-flaw audit 和候选池见
`literature_driven_pipeline_optimization_guide.md`。

### 10.4 RC2 核心瓶颈：AIMD 选错了观测信号（2026-07-27 集中梳理）

**问题**：07-26 shared-vLLM 实验中 AIMD 0 次 decrease，根因是 AIMD 盯着
vLLM Prometheus `vllm:num_requests_waiting` 做决策——但请求在 Ray actor
侧排队，尚未进入 vLLM waiting queue，该信号始终为 0。vLLM 本身暴露了
完整的 Prometheus 指标（`num_requests_running`、`gpu_cache_usage_perc`、
`generation_tokens_total` 等），不需要修改即可获取。问题不在"拿不到信号"，而
在 **AIMD 选了不反映 Ray 侧排队状态的信号**。

真正需要但 vLLM 不暴露的细粒度信息（无论是否修改 vLLM 都拿不到）：

- per-iteration token batch composition（每次 forward pass 的具体 prefill/decode token 组成）
- per-request in-flight progress（请求 X 当前已生成多少 decode token）

**三种使用已有 vLLM Prometheus 信号做提交决策的方式**（均不需要修改 vLLM）：

| 方式 | 核心思路 | 落地成本 | 精度 | 已文档化 |
|---|---|---|---|---|
| **1. 模拟器模拟 vLLM 内部调度** | SFS 确定性 token-batch 模拟器重建 vLLM 内部调度过程。利用"我们提交了什么 + vLLM Prometheus running count + 离线校准的 β 参数"，模拟每个 token batch 的组成，预测 TTFT | 中（~200 行 Python + 离线 β 校准） | 高（TTFT MAPE <5%） | 模式 10 / §11 方案 A |
| **2. 解析模型估计系统能力** | LPS + USL：Prometheus `generation_tokens_total` 差分 → μ（服务率），arrival config → λ（到达率）。LPS 给等待时间、USL 给吞吐退化曲线和峰值并发 | 低（所有信号已有，无需新基础设施） | 中（平均行为，无 per-request 分布） | 模式 11+16 / §11 方案 B+E |
| **3. 客户端推断积压状态** | 用自己的 request lifecycle trace（submit + completion time）+ vLLM Prometheus gauge 做 EWMA：推断当前服务速率、oldest request slack、inflight token backlog——这些 Ray 侧信号才真正反映"软拥塞" | 低（trace + Prometheus 已有，~50 行 EWMA 状态） | 中（间接推断，滞后 1-2 个请求完成周期） | §10.2 缺口表 + §10.3 推荐顺序 |

**三种方式之间的关系**：
- 方式 2 和 3 共享同一套外部信号（Prometheus + lifecycle trace），可以**同时启用**——方式 2 给 K_max 解析上界，方式 3 给 flush timeout 动态调节
- 方式 1 可以**叠加**在 2+3 之上：当 2+3 的解析推断显示"当前接近饱和"时，用 1 做精确的 per-request TTFT 预测来决定哪些请求立即提交、哪些等待
- 推荐**渐进式推进**：先方式 3（零新依赖）→ 再方式 2（验证 K=8 解析依据）→ 最后方式 1（需要模拟器基础设施）

## 11. 2026-07-27 提交策略（RC2）文献驱动备选方案

以下从新精读的 SFS (arXiv 2026) 及其他 5 篇代价估计论文中提取的
提交策略备选技术方案。每个方案标注来源、落地难度、和与当前 K_max +
queue-adaptive flush 的关系。设计模式全文见
`research/knowledge_hub.md` §5.7。

### 方案 A：SFS What-If 预演（模式 10）

**来源**：SFS (Patel et al., arXiv 2026, §4.1)

**核心思路**：在每次 flush 决策时，用确定性 token-batch 模拟器预测
"如果现在提交这个 pending batch，每个请求的 TTFT 是多少"，只放行
TTFT 在 SLO 内的请求。

**与当前方案的关系**：
- 当前：`QueueAdaptiveFlush` 看 queue depth + vLLM waiting 做 25ms/50ms
  二元决策——粗粒度，不感知 per-request SLO
- 升级后：SFS 模拟器输出 per-request TTFT → 按 SLO 做精确准入 →
  flush 不再是"全部提交/全部等待"而是"选择性提交"

**实现步骤**：
1. 实现 token-batch simulator（Python, ~200 行），逻辑：给定 vLLM
   workload snapshot → 逐 iteration 模拟 token batch 组成和处理时间 →
   输出新请求的 TTFT 估计
2. 为 Qwen2.5-1.5B 校准 4 个 β 参数（离线 profile：记录若干 token batch
   的 composition→time 映射，线性回归）
3. 接入 vLLM Prometheus 获取 running request count 和 prefill/decode
   composition

**预期效果**：TTFT MAPE <5%（SFS 论文结果），亚毫秒决策开销

**风险评估**：
- vLLM Prometheus 可能不够细粒度（prefill/decode token composition
  无法直接从 `vllm:running_requests` 获取）
- SFS 假设每 decode 序列每 batch 恰好 1 token——在 chunked prefill
  下成立，但 speculative decoding 下不成立
- **放弃条件**：如果 Prometheus 信号粒度不足以支撑 token-batch 模拟，
  回退到方案 B（LPS 解析模型）

### 方案 B：LPS Queueing Model 指导 K_max 选择（模式 11）

**来源**：SFS §4.2（Average-case estimator, eq. 10-11）

**核心思路**：用 Limited Processor Sharing 公式估计给定 (λ, μ, K) 下的
平均等待时间：`W_avg = (λ/μ)^K / (μ - λ)`。不替代 K_max 动态调节，
但提供 K_max 初始值和解空间约束。

**与当前方案的关系**：
- 当前：K=8 来自实验暴力搜索（"对比 K=8/16/32 选最好的"）
- 升级后：从 profile 数据估计 μ（请求服务率 ≈ tokens/s /
  avg_tokens_per_request），从 arrival replay 参数获取 λ → LPS 公式
  输出推荐的 K 范围 → 作为 AIMD 的搜索边界

**实现步骤**：
1. 从已有 profile CSV 估计 μ（`observed_tokens_per_second /
   avg_prompt_tokens` per workload type）
2. 对每个 λ（arrival rate）计算使 `W_avg < SLO_slack` 的最小 K
3. 将 LPS-K 作为 K_max 的初始值或搜索下界

**预期效果**：减少 K_max 搜索空间，提供解析可解释性

**风险评估**：LPS 假设 Poisson 到达 + 指数服务时间（现实中请求
服务时间是 token-length 相关的，非无记忆）。SFS 论文显示 LPS
与实测高度一致（Qwen3-0.6B），但需在本地环境验证

### 方案 C：Token-Batch 处理时间线性回归（模式 12）

**来源**：SFS §4.1（eq. 9, 4-parameter regression）

**核心思路**：不模拟低层 GPU kernel——用 4 参数线性回归直接从 token
batch composition 估计 batch 处理时间。参数有物理含义：β1（dense
计算 ∝ tokens）、β2（attention ∝ context·decode_tokens）、β3（prefill
attention ∝ prefill_chunk·context + prefill²）

**与方案 A 的关系**：方案 C 是方案 A（SFS 预演）的子组件——SFS 模拟器
需要 `T_j(τ)` 函数来估计每个 token batch 的处理时间。方案 C 提供了
这个函数的校准方法。

**落地难度**：中——需获取 per-iteration token batch composition。
如果 vLLM 不暴露此信息，可用 Prometheus 的 `vllm:prompt_tokens_total`
和 `vllm:generation_tokens_total` 的差分做粗粒度近似

### 方案 D：轻/中/重 Workload 分档提交（模式 13）

**来源**：SPOS + Heinrich R3 + 项目已有计划（operator_cost README）

**核心思路**：不追求精确预测 E2E 秒数做准入——将 pending batch 分为
"轻（E2E < t_low）/ 中（t_low < E2E < t_high）/ 重（E2E > t_high）"
三档。提交策略按档位差异化：
- 轻 batch：激进提交（低风险，无需等待更多合并）
- 中 batch：正常 waiting window
- 重 batch：延长等待（需更多请求摊销 compute overhead）

**与当前方案的关系**：在 `QueueAdaptiveFlush` 的 25ms/50ms 两层之上
增加第三维度——batch weight classification

**落地难度**：低——当前 Ridge 可能已有足够排序能力做分档（MAE 11.68s
vs E2E 范围 ~5-300s）。需做的只是定义档位阈值并验证

### 方案 E：USL 并发-吞吐估计（模式 16）

**来源**：SABER (arXiv 2025, §IV.B) — USL 拟合 LLM 推理 per-request
速度退化曲线，R²=0.99

**核心思路**：USL `σ(N) = λN / (1 + σ(N-1) + κN(N-1))` 从 concurrency
sweep 数据（K=1,2,4,8,16,32,64）拟合出完整并发-吞吐退化曲线。峰值并发
`N* = √((1-σ)/κ)` 给出"再多发也没用"的解析上界。

**与当前方案的关系**：
- 当前：K=8 来自实验暴力搜索（"对比 K=8/16/32 选最好的"）
- 升级后：USL 给出解析 K_max 上界，与经验值互相校验——一致则经验值
  有理论支撑，不一致则说明 vLLM KV cache 抢占机制不服从 USL 平滑退化
  假设（同样有论文价值）
- 与方案 B（LPS）互补：LPS 建模等待时间随并发变化，USL 建模吞吐随
  并发退化——两者共同提供 K_max 的完整解析依据

**预期效果**：为 K_max 选择提供理论支撑，减少对暴力扫参的依赖

**风险评估**：USL 假设平滑二次退化（σ(N-1) 争用项 + κN(N-1) 一致性项），
vLLM 的 KV cache 抢占是不连续的阶跃退化——USL 可能只在"内存未耗尽"
区间拟合良好。SABER 代码未开源（仅方法论可迁移），~1000 采样点需在
本地重新采集

### 方案 F：双信号 Deadband 控制架构（模式 17）

**来源**：CONCUR (arXiv 2026, §4.3) — proactive + reactive 双信号 +
deadband 宽度 0.3

**核心思路**：不用单一信号驱动自适应——用两个独立信号（如 proactive
预警信号 + reactive 确认信号），仅在两者同时越界且变化幅度超出 deadband
时才触发动作。核心价值在于防止控制器振荡。

**与当前方案的关系**：
- 当前：queue-adaptive flush 看 queue depth 一个信号 → 25ms/50ms 二元；
  AIMD 控制器用单一信号 → 102 次 downshift/run 振荡（07-19 实验）
- 升级后：双信号（如 queue_depth + oldest_request_age 或 token_backlog
  + arrival/service_ratio）+ deadband（如 30%）——两个信号同时"说该
  发了"才改 timeout，变化量不够 deadband 不动作

**预期效果**：消除或大幅减少控制器振荡，使自适应 flush 在稳态 workload
下行为接近最优静态（fixed-50），在负载变化时及时切换

**风险评估**：需选定第二信号并调 deadband 参数。CONCUR 的双信号
（U_t KV cache 使用率 + H_t 命中率）是针对 agentic KV cache 抖动的，
数据库 AI 算子的"第二信号"需要独立选择。如果所选信号对不独立
（高度相关），退化为单信号 + 死区——仍有改善但不如双信号

### 方案 G：Credit-Based Admission（模式 18）

**来源**：SCORPIO (arXiv 2025, §3.4) — TRP credit accumulation +
VBS admission control

**核心思路**：不设全局 K_max——每个请求按 SLO 紧松度获得不同 credit
累积速率 TRP(r) = min S_TP / S_TP(r)，credit ≥ 1.0 时准入。紧 SLO
请求更快被放行（不被大 batch 拖累），松 SLO 请求在 credit 慢速累积中
自然合并（摊销 overhead）。

**与当前方案的关系**：
- 当前：K_max 是全局固定值，所有请求不分紧迫度按 FIFO 顺序提交
- 升级后：per-request deadline tracking + credit accumulation →
  "该不该发"由请求的 SLO 紧迫度决定而非全局 K_max

**预期效果**：在 SLO 异构场景下（混合 workload、多模态请求混跑、
在线+离线混合）提升 SLO goodput

**风险评估**：当前批量离线场景 SLO 同质 → credit 退化为均匀累积 = FIFO，
不体现区分度。只有在 SLO 异构性存在时才发挥价值——可能需要等到多模态
或多 job 场景。需增加 per-request deadline tracking 基础设施

**放弃条件**：如果未来 workload 始终保持 SLO 同质（纯离线批处理），
Credit-based admission 退化为 FIFO，与当前方案等价——不值得额外复杂度

### 提交策略备选方案优先级

```
落地难度 →        低              中              高
收益 ↓
高               方案D 分档提交   方案A SFS预演
                方案F Deadband
中               方案B LPS模型    方案C Batch回归
                方案E USL估计
低               方案G Credit-Based（需 SLO 异构场景）
```

**建议推进顺序**：
1. **先做方案 D**（最低风险，已有 Ridge 模型）：验证 Ridge 的分档能力
2. **再做方案 B + E**（解析指导，不需改 pipeline）：LPS + USL 联合审计
   当前 K=8 选择
3. **再做方案 F**（控制架构升级，改动 ~50 行）：双信号 deadband 架构
   解决振荡问题
4. **最后做方案 A**（需要 SFS 模拟器 + Prometheus 接入）：在前几项确认
   有效后再投入
5. **方案 G 待 SLO 异构场景出现后启动**

### 与现有 RC2 缺口的整合

| 现有缺口 (§9) | 新增文献方案 | 整合方式 |
|--------------|------------|---------|
| request-level continuous replenishment | 方案 D（分档提交）+ 方案 G（Credit-Based） | 分档/credit 决定"哪些请求可以立即补位" |
| SLO-aware EWMA flush | 方案 A（SFS 预演）+ 方案 F（Deadband） | Deadband 消振 + SFS 提供 per-request TTFT |
| 软拥塞（Ray 侧积压 vLLM 不可见） | 方案 A + C + 方案 F | 双信号架构的第二信号可选逐请求完成时间 |
| K_max 选择 | 方案 B（LPS）+ 方案 E（USL） | LPS 等待时间 + USL 吞吐退化，联合推导 |

### 不纳入 RC2 的方案

- **GNN/图表示的代价模型**（CONCERTO/GRACEFUL/COSTREAM）：属于 RC4
  代价估计范畴，不直接用于在线提交决策
- **SFS 的 accuracy-cost-latency 路由框架**：SFS 论文做 multi-model
  routing（"选哪个模型实例"），项目是 single-model admission control
  （"什么时候提交给同一个 vLLM"），决策框架不同

### 跨 RC4→RC2 的辅助技术

以下技术主要属于代价估计（RC4）范畴，但对提交策略（RC2）有直接辅助
价值，在对应 README 中有详细方案：

- **Output-Length 预测器**（模式 15 | `operator_cost_estimation_20260726/README.md` 第一批 #3）：
  用 LightGBM 从 prompt 特征预测实际输出 token 数，替代 `completion_max_tokens`
  作为 Ridge 特征。对 RC2 的价值：更准确的实际计算量估计 → 更好的
  per-request 工作量预估 → SLO-aware flush 和 token-work admission 的
  输入质量提升。
- **Probe Execution**（模式 14 | `operator_cost_estimation_20260726/README.md` 第三批 #11）：
  验证 partial execution → full E2E 的相关性，加速 profile 数据收集。
  对 RC2 的价值：更快的 (λ, μ) 参数校准，减少 LPS K_max 选择和新
  workload 接入的 profile 成本。

---

## 12. 关于"已排除"技术的状态说明（2026-07-27 审计）

以下技术在 07-26 实验中未表现出优于当前 baselines 的结果，但代码和
实验记录均已保留，**不视为永久排除**。当前结论受限于单 GPU（RTX 5070）、
Qwen2.5-1.5B、512 行稳态 workload 等测试条件——在不同硬件/模型/负载
/多租户场景下可能重新体现出价值：

| 技术 | 当前结论 | 保留位置 | 重新激活条件 |
|------|---------|---------|------------|
| AIMD/EWMA-AIMD/PID 自适应准入 | 相对 static K=16 无增量；shared-vLLM 下 vLLM waiting=0，AIMD 看的信号不反映 Ray 侧积压 | `code/src/adaptive_admission.py` | 改用反映 Ray 侧积压的信号后（逐请求 completion time 观测→可能解锁动态控制价值，见 §10.3 诊断） |
| Two-level queue-adaptive flush | 相对 fixed-50ms 无稳定增量（89.4% 时间选 50ms，行为接近 fixed-50） | `code/src/queue_adaptive_flush.py` | 多 workload shape、变长输出、多租户到达模式下重新评估 |
| GNN/Transformer 代价模型 | 283 行数据远未达到需要 GNN 的规模（Heinrich R1 + Pathak & Mankodi 一致结论） | 未实现（仅保留设计文档） | profile 数据增长到千级/万级行后 |

**重要**：上述技术不是"被否定"，而是"在已测试条件下未优于更简单的
baseline"。代码实现均保持可用状态，后续重新激活时改动量预计较小（主要
是接入新观测信号或切换 workload 配置）。特别是 AIMD 自适应准入——
§10.2 的诊断指出选错信号是根因而非控制器参数问题，一旦
request-level completion replenishment 提供了逐请求完成时间信号，
动态控制的价值可能需要重新评估。
