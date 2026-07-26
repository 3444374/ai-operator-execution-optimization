# 文献驱动的 AI 算子执行链优化指南

Date: 2026-07-26

## 1. 用途与边界

本文档是“如何继续从文献中提取可落地优化机制”的单一入口，同时登记当前
Daft → Ray → vLLM → PostgreSQL 执行链中尚未闭环的机制缺口。它不替代：

- `research/reading_notes/`：论文事实与逐篇精读；
- `strategy_design_literature_basis.md`：论文写作口径与不能过度声称的边界；
- `strategy_design_implementation_reference.md`：已有模块与工程接口；
- `experiment_status_and_gaps.md`：实验完成度和证据强弱；
- 具体结果目录：真实 CSV、运行命令和结果解释。

本文使用四种来源标签：

| 标签 | 含义 |
|---|---|
| **论文直接机制** | 论文明确实现并评估过，可准确描述原机制 |
| **迁移设计** | 从论文机制迁移到本项目，但控制对象或执行层不同 |
| **工程候选** | 根据现有接口提出，尚无直接论文算法对应 |
| **待确认** | 机制或观测接口尚未核实，不能进入正式 claim |

任何“代码已接通”都不等于“方法有效”；任何“论文有收益”都不等于迁移到
本项目后仍有收益。

## 2. 三层 batch 必须分开

```text
Daft data batch
  数据读取、Arrow 传输和算子调用粒度

Ray submission / micro-batch
  上游反压、HTTP 调用和完成通知粒度

vLLM iteration batch
  GPU 内部每轮调度的动态执行集合
```

vLLM/Orca 的 continuous batching 属于第三层。本项目不修改 vLLM scheduler，
但必须避免第二层形成过粗的完成屏障，从而妨碍第三层持续获得请求。

### 2.1 Orca/vLLM 已经做了什么

**论文直接机制**：iteration-level scheduling 不等待一组生成请求全部结束。
某个请求完成后，服务端可以在后续调度迭代中从 waiting queue 接纳新请求。
vLLM 在部署平台内部提供 continuous batching 和 KV-cache 管理。

### 2.2 当前上游还没有做什么

当前 Ray 路径以 submission 为主要回收粒度。一个 submission 中即使部分请求
较早结束，上游通常也要等整个 submission 返回后才释放 admission credit。
因此存在：

- submission 内 head-of-line blocking；
- 短请求完成后不能立即从上游 pending queue 补位；
- `K_max` 统计的是在途 submission，而不一定是真正在途 request/token work；
- 大 Daft batch、Ray submission 和模型执行 batch 容易被错误地视为同一层。

这不是 vLLM 缺少 continuous batching，而是上游尚未充分利用它。

## 3. 当前两个主要实现缺口

### 3.1 Request-level continuous replenishment

目标不是重写 Orca/vLLM，而是让上游按独立完成事件持续供给：

```text
Daft batch
  → immutable request rows
  → Ray actor/gateway pending queue
  → bounded independent request or small micro-submission
  → 任一请求完成即释放 request credit
  → 立即从 pending queue 补位
  → vLLM 自己完成 iteration-level batching
```

最小设计要求：

1. **身份保持**：`request_id`、`submission_id` 和结果行 exactly-once；
2. **完成粒度明确**：至少能够按单请求完成释放 credit，不依赖整批返回；
3. **有界供给**：同时限制 request 数和估计 token work，不能把队列搬进 vLLM；
4. **Daft/Ray 解耦**：Daft batch 继续作为数据粒度，不充当模型完成屏障；
5. **故障语义**：失败请求只释放自身 credit，重试策略仍显式关闭或受控；
6. **可观测**：记录 pending、active request、active token work、refill 次数、
   credit idle time 和 submission 内完成跨度。

第一轮公平对照：

| 对照 | 完成与补位粒度 |
|---|---|
| Whole-submission barrier | 整个 submission 返回后补位 |
| Fixed request replenishment | 单请求完成后立即补位，固定 request cap |
| Token-credit replenishment | 单请求完成后补位，同时限制估计 token work |
| Replenishment + adaptive flush | 补位运行时叠加动态聚合等待 |

关键指标：

- observed tokens/s、SLO goodput、request P50/P95/P99；
- submission 内 `last_finish - first_finish`；
- credit idle ratio、refill interval、pending/active 时间序列；
- vLLM running/waiting/KV cache、GPU utilization、MFU；
- HTTP/Ray 调用数和调度开销。

Fatal flaws：

- 如果当前模型后端只能返回整批结果且无法拆成独立 future，先实现完成粒度，
  不伪造逐请求时间；
- 如果逐请求 RPC 开销吞噬收益，保留小 micro-submission，但必须能按其真实
  完成边界释放 credit；
- 如果 vLLM 已持续饱和且 replenishment 相对合理静态基线差异不可辨认，则把
  结果写成边界，不继续增加控制复杂度。

### 3.2 SLO-aware Adaptive Flush

当前 `QueueAdaptiveFlush` 是两档基线：根据瞬时 running/waiting/KV 指标在
25ms 与 50ms 之间切换。它已接入真实链路，但不能代表完整文献驱动控制器。

下一版控制器应只增加能够被实验隔离的状态：

```text
observations:
  pending_rows / pending_cost_units
  batch_fill_ratio
  oldest_request_age
  arrival_rate_ewma
  service_rate_ewma
  vLLM running / waiting / KV usage
  recent TTFT/P99 or SLO slack

state:
  EWMA + deadband/hysteresis
  last_window / last_transition

actions:
  flush now
  wait bounded Δt
  hard-deadline flush
```

控制目标应写成“在 request SLO guardrail 下最大化 observed tokens/s 或
goodput”，而不是笼统地“GPU 压力高就等待更久”。

第一版推荐 **SLO-aware EWMA rule controller**，而不是直接上 PID/UCB：

1. budget 满或 oldest request 到 hard deadline：立即 flush；
2. metrics stale/missing：回退到已验证的 fixed 50ms；
3. 低 fill 且服务端繁忙：在剩余 SLO slack 内继续聚合；
4. 服务端即将空闲或 pending token work 足够：尽快 flush；
5. 使用 EWMA 和滞回，避免瞬时采样造成频繁切换；
6. 每个真实 endpoint 独立维护状态。

之后才比较：

- fixed timeout sweep；
- 当前 two-level baseline；
- SLO-aware EWMA；
- SLO-aware EWMA + request-level replenishment；
- 正确 epoch 归因建立后再比较有限动作 UCB。

## 4. 文献机制到本项目的准确映射

| 来源 | 原机制 | 本项目可迁移部分 | 不能直接声称 |
|---|---|---|---|
| Orca / vLLM | iteration-level/continuous batching | 上游持续供给、缩小 completion barrier | 本项目发明或修改 continuous batching |
| Clipper | SLO 下 AIMD batch size；delayed batching | SLO guardrail、延迟聚合、per-replica controller | Clipper AIMD 已被当前 two-level flush 复现 |
| Clockwork | deadline、可预测执行与集中控制 | deadline/slack、可解释决策、失败保护 | 自回归 LLM 服务时间完全可预测 |
| CONCUR | KV/cache 信号驱动的 agent admission | deadband、AIMD、主动并发保护 | agent pause/resume 等同于上游 request gate |
| CoLoRA | queue delay、SLA urgency、adapter residency 联合调度 | 多信号优先级与联合决策思想 | 论文使用 running/waiting/KV 三信号 |
| BucketServe / Sarathi | 长度分桶、token/chunk 预算 | token-work backlog、长短请求隔离 | 上游重做 vLLM chunked prefill |
| Ray | actor 状态、异步执行、背压和资源约束 | endpoint-local queue/controller、bounded futures | 修改 Ray 全局 scheduler |

## 5. 后续从文献发现优化点的固定流程

### Step 1：冻结问题和层级

先写清楚瓶颈发生在：

- 数据组织；
- Ray submission；
- vLLM 服务入口；
- vLLM 内部；
- fan-in/writeback。

如果机制只优化 vLLM 内部，而项目边界禁止修改 vLLM，则只能提取接口侧思想，
不能照搬算法或把它写成项目贡献。

### Step 2：提取论文机制卡

每篇候选论文至少记录：

```text
problem:
controlled_resource:
observation_signals:
decision_unit:
action:
objective:
hard_constraints:
assumptions:
baseline:
reported_metrics:
failure_boundary:
source_location:
```

没有 `assumptions` 和 `failure_boundary` 的摘要不能直接进入设计。

### Step 3：做假设迁移审计

逐项判断：

- feed-forward batch 原子完成假设在 autoregressive LLM 中是否失效；
- 单模型/单租户假设是否适用于共享 vLLM；
- request count 是否能代表 token/frame cost；
- 原论文能否观测的指标，vLLM Prometheus 是否真实暴露；
- 原论文控制的是 batch size、admission、routing 还是 GPU 内部 scheduling；
- 原论文硬件、模型规模和 endpoint 数量是否可比。

### Step 4：构造“信号—动作—目标”闭环

任何候选必须能回答：

```text
观测到什么？
改变哪个上游变量？
多久改变一次？
优化哪个主指标？
用什么 guardrail 防止尾延迟或公平性恶化？
指标失效时回退到什么静态策略？
```

只列技术名、不明确 actuator 的候选不进入代码。

### Step 5：检查是否被下游机制吸收

先做 fatal-flaws audit：

- vLLM continuous batching 是否已经吸收收益；
- prefix cache 关闭时 prefix-aware 是否失去物理基础；
- 单 endpoint 下 routing 是否没有动作空间；
- 单 GPU 稳态负载下 adaptive 是否只会吸附到一个静态值；
- Daft batch 是否只是数据传输粒度，无法改变模型请求节奏。

### Step 6：设计最小隔离实验

先比较强静态基线，再比较动态机制：

1. 静态 sweep 找到 workload-specific 最优点；
2. 只改变一个 actuator；
3. 记录完整时间序列和 per-request trace；
4. 使用变负载或干扰场景给 adaptive 足够动作空间；
5. 至少一次 held-out workload/scale；
6. 报告均值、方差/置信区间和负结果。

### Step 7：设置晋级与放弃条件

候选只有在以下条件同时满足时才进入默认路径：

- 相对最佳静态基线而非 strawman 有可辨认收益；
- exactly-once、失败率和 SLO guardrail 不退化；
- 收益跨重复或 held-out 条件保持；
- 增加的状态和参数能够解释、能够回退。

否则保留为实验候选或负结果，不继续堆叠控制器。

### Step 8：确认 reward 归因后再用学习控制

UCB/学习型控制器必须先解决：

- epoch 内发出的请求可能在后续 epoch 才完成；
- reward 应归因到产生该请求的 arm；
- warm-up、切换成本和延迟反馈必须单独记录；
- safety fallback 固定为已验证静态配置。

没有正确归因时，UCB 代码通过单测也不构成可用策略。

### Step 9：更新证据登记

每次机制实验完成后同步：

- `experiments/results/<run>/README.md` 和原始 CSV；
- `experiment_status_and_gaps.md`；
- `code/INFRA_STATUS.md`；
- `PROJECT_OUTLINE.md` 与 `PROJECT_LOG.md`；
- 新的文献候选同步 `research/knowledge_hub.md`。

## 6. 当前候选优化池

### 6.1 近期、单 GPU 可验证

| 候选 | 来源类型 | 最小验证 |
|---|---|---|
| Request-level continuous replenishment | Orca/vLLM 机制的上游迁移 | whole-submission vs request-credit |
| Token-credit admission | vLLM token budget 的上游迁移 | request cap vs token-work cap |
| SLO-aware EWMA flush | Clipper/Clockwork/Ray 思想组合迁移 | fixed-best vs two-level vs EWMA |
| Long/short request isolation | Sarathi/BucketServe 迁移 | single queue vs two logical pools |
| Completion-span-aware micro-batch | 工程候选 | 不同 micro-submission 大小与 HOL |
| Service-rate/backlog drain estimator | 排队与控制思想迁移 | 瞬时阈值 vs EWMA drain time |
| Shared-vLLM fairness guardrail | 多租户调度迁移 | 多 foreground size/offset/job 数 |

### 6.2 需要 prefix cache 或多 endpoint

| 候选 | 前置条件 |
|---|---|
| Prefix/KV affinity routing | vLLM prefix cache 开启且可记录命中证据 |
| Endpoint-local controller | 至少两个真实、独立地址的 model endpoint |
| Least-work/token-aware routing | 可获得每 endpoint backlog 与完成速率 |
| Heterogeneous actor pools | 多模型、异构 GPU 或可重复的服务能力差异 |
| Failure-aware migration | 多 endpoint 且有受控故障注入 |

### 6.3 暂不优先

- 同时在线搜索 batching、flush、admission、routing 的大联合控制器；
- 没有正确 epoch reward 的 UCB；
- 单 endpoint 上复杂 routing；
- 只因论文报告收益就引入的 PID/MPC/学习模型；
- 修改 vLLM 或 Ray 内部 scheduler。

## 7. 推荐实施顺序

```text
补齐 request-level completion / replenishment trace
  → whole-submission 与 continuous replenishment 对照
  → token-credit guardrail
  → SLO-aware EWMA flush
  → flush × replenishment 小矩阵
  → shared-vLLM 多 job/fairness
  → prefix-cache 与真实多 endpoint
  → reward 归因正确后再考虑 UCB
```

每一步都允许得出“最佳静态策略已经足够”的负结果；不以使用更多技术为目标。
