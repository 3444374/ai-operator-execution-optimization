# 策略设计思路的文献依据与边界

整理日期：2026-07-15

> **2026-07-17 口径更新**：本文中的"计划层/运行层/服务端层""三层策略""RC3/研究内容三"等旧术语已统一为当前口径。最新研究内容定义（两项策略 + 多模态泛化验证 + 算子代价估计补充）、优先级和边界以 `AGENTS.md` §1、`PROJECT_OUTLINE.md` 和 `research/knowledge_hub.md` 为准。写回已降为实验设置。本文保留原始设计推演过程作为历史参考，术语不匹配处以上述主文档为准。

用途：在绘制策略设计图、撰写开题报告方法部分和设计正式实验前，先明确“哪些优化思想可以借鉴、哪些只适合作为边界或 baseline、本文自己的策略到底是什么”。

> **与 `strategy_design_implementation_reference.md` 的分工（2026-07-24）**：本文是**边界论证**——为什么这样设计、不能过度声称什么、fatal flaws、可借鉴 vs baseline vs 本文策略；`implementation_reference` 是**工程映射**——信号→变量→指标→§8 目标代码架构→实现优先级。两者用途不同（答辩/写论文 vs 写代码），不合并不替换，见 `README.md` §二。

本文件是策略口径源，不替代具体实验计划。具体变量、运行矩阵和结果记录仍以 `data_organization_batching.md`、`service_scheduling_backpressure.md`、`sink_writeback_coordination.md` 和 `baseline_reference.md` 为准。

---

## 1. 一句话定位

本课题的策略不是重新发明数据库优化器、推理引擎或存储引擎，而是在“数据库触发 AI 算子 -> 外部批处理/调度 -> GPU 模型服务 -> 写回”的链路中，针对已定位瓶颈选择上游优化动作：

```text
阶段画像定位瓶颈
  -> 按 workload 特征选择数据组织动作
  -> 按模型服务状态选择提交、路由和反压动作
  -> 将写回作为端到端约束和 baseline
  -> 用端到端指标验证收益是否成立
```

更准确的策略名称可以写成：

> 面向数据库 AI 算子外部执行链路的 workload-aware 数据组织与模型服务状态感知调度策略。

---

## 2. 可直接借鉴的优化思想

### 2.1 AI 算子语义感知：从 Cortex AISQL / Smart 借鉴

可借鉴思想：

- AI 算子的成本和选择率不能按普通 SQL UDF 处理。
- `AI_FILTER` / `AI_CLASSIFY` 的 selectivity 会影响后续模型调用数、batch 构造和 partition 粒度。
- 对语义过滤、分类、补全等算子，应把 selectivity、token length、prefix sharing、output size 等特征显式纳入策略输入。

对本课题的落点：

- `AI_FILTER`：优先研究 selectivity-aware partition / cascade / reordering。
- `AI_COMPLETE`：优先研究 token-aware batching、prefix-aware grouping 和 routing。
- `AI_EMBED`：重点看 batch size、partition count、输出向量写回量和 fan-in。

边界：

- Cortex AISQL 和 Smart 主要在数据库内部做 AI-aware query optimization 或 ML predicate rewrite。
- 本课题不能声称沿用了它们的内部优化器，也不能说“现有研究没做 AI SQL 优化”。
- 合理表述是：它们证明 AI 算子语义会改变执行策略；本课题把这一思想迁移到外部执行链路的 batch / partition / routing / backpressure 决策中。

### 2.2 模型服务调度：从 vLLM / Orca / Sarathi-Serve 借鉴

可借鉴思想：

- 推理服务的核心指标不是单点 latency，而是吞吐-延迟曲线、queue wait、P99 latency 和 GPU utilization。
- batch 不是越大越好，需要在吞吐、排队和尾延迟之间找工作点。
- 对 `AI_COMPLETE`，token length、prefill/decode、prefix sharing 会显著影响服务调度。

对本课题的落点：

- 把 `K_max`、actor pool、endpoint routing、bounded in-flight、queue/backlog 作为外部调度控制变量。
- 对 `AI_COMPLETE` 保留 token-aware / prefix-aware dispatch 的设计空间。
- 实验展示优先采用吞吐-延迟曲线、P50/P99、queue wait 占比，而不是只报“快了多少倍”。

边界：

- vLLM/Orca 优化的是 GPU 推理引擎内部的 batching、memory 和 iteration-level scheduling。
- 本课题的调度位置在数据库外部执行链路和模型服务入口之前。
- 如果后续接入 vLLM 后外部 `K_max` 控制收益消失，不能强行说研究内容二独立贡献成立；应收窄为“外部调度与数据库数据组织/写回约束的协同”。

### 2.3 分布式数据组织：从 Ray Data / Daft / Spark 借鉴

可借鉴思想：

- batch size、partition count、object count、shuffle/fan-in 形态会影响批处理链路。
- `map_batches()`、actor concurrency、object coalescing、pre-shuffle merge 都是可操作的系统旋钮。
- 分布式数据系统的调参必须结合下游算子成本，而不能只看数据搬运本身。

对本课题的落点：

- 数据组织动作包括：调整 batch size / partition count，合并 operator invocation，控制 object 数和 fan-in 形态。
- 对不同 workload 使用不同的 batch/partition 策略，而不是全局固定一个 batch size。
- 在端到端链路中观察数据组织变化是否减少模型服务请求数、queue wait、fan-in 或 writeback pressure。

边界：

- Ray/Daft 文档只能证明这些控制接口和潜在风险存在，不能单独证明当前链路一定有瓶颈。
- 任何“object / fan-in 是核心瓶颈”的说法都必须回到 GPU-backed E2E profile 或正式实验数据。

### 2.4 写回路径：从 COPY / pgai / Delta Lake / TurboVecDB 借鉴

可借鉴思想：

- 写回不是简单的最后一步；它可能吞掉上游批处理和模型服务调度带来的收益。
- 工程上应优先比较 COPY、unlogged staging、deferred index、worker-direct、queue-worker 等形态。
- 多 worker 盲追加、队列解耦和延迟物化是可参考的持久化设计模式。

对本课题的落点：

- 写回在当前策略图中作为约束处理和端到端验证，而不是第一核心贡献。
- 优先建立写回 baseline：COPY + deferred index、driver fan-in、worker-direct、queue-worker。
- 如果 writeback ratio 很高，再考虑切换 sink mode、write batch rows 或 worker-direct。

边界：

- TurboVecDB、Delta Lake、pgai 等更适合作为写回 baseline、工程形态或边界条件。
- 不能把“改造写回引擎”写成当前主线，除非后续实验显示写回是绝对主瓶颈且上游优化收益被完全吞噬。

---

## 3. 只适合作为 baseline 或边界的内容

| 来源 | 适合作为什么 | 不应写成什么 |
|---|---|---|
| vLLM / Orca | S 级 GPU 推理 baseline；吞吐-延迟评估范式；内部 batching 的强对照 | 本文重新提出 continuous batching 或 iteration-level scheduling |
| Ray Serve | routing / backpressure / actor pool 的工程接口参考 | 本文实现了新的通用 Ray Serve 调度器 |
| Daft / Spark tuning | fixed batch/partition、pre-shuffle merge、object coalescing baseline | 本文贡献是普通 Spark/Daft 调参 |
| Cortex AISQL | AI SQL 产业需求、AI-aware optimization 思想、selectivity/cascade 参考 | 本文复现或超越 Snowflake 内部优化器 |
| Smart / GaussML | DB4AI 对照路线，证明数据库内 ML 优化已被充分研究 | 本文继续做数据库内核 ML predicate rewrite |
| COPY + deferred index | 写回工程最优 baseline | 本文发明新的 PostgreSQL 写入机制 |
| pgai vectorizer | 外部 worker + 队列 + 写回架构参考 | 本文依赖 pgai 作为长期核心系统 |
| Delta Lake / TurboVecDB | worker-direct、blind append、写回/索引构建边界 | 本文主要贡献是存储引擎或向量索引优化 |
| FlexPushdownDB / AIDB | 跨层决策思想和增强对照 | 把“独立最优拼装 vs 联合最优”写成当前开题主线必须证明的核心 claim |

### 3.1 借鉴论文的适用边界（fatal flaws，2026-07-24 补充）

以下 fatal flaw 已在精读笔记确认，借鉴对应论文机制时需显式避开：

| 论文 | 不适用之处 | 出处 |
|---|---|---|
| Clipper (NSDI 2017) | delayed batching 假设 Poisson 到达；数据库批量 AI 算子是 burst 到达，等待收益曲线需重测 | `research/reading_notes/clipper_nsdi2017.md` §4.3.2 |
| CONCUR (2025) | “中段抖动”前提依赖 ReAct 多步 agent；本课题 DB AI 算子多为无状态单轮，前提可能不成立 | `concur_2025.md`；`research/knowledge_hub.md:364` |
| BucketServe (2025) | 并发上限只在 prefill 侧（静态输入）成立；decode 侧输出不可预测，用 continuous batching 规避 | `bucketserve_2025.md:122,132` |
| vLLM continuous batching | 可能已消化外部 K_max 大部分收益（H2.4 反证） | `service_scheduling_backpressure.md` §9 |

---

## 4. 本课题自己的策略定义

### 4.1 策略输入

策略输入不是抽象的“workload 画像”，而是阶段画像之后已经可观测或可估计的信号：

| 类别 | 信号 |
|---|---|
| Workload 特征 | 算子类型、行数、平均文本长度、token length、prefix sharing、selectivity、输出大小、sink 类型 |
| 数据组织信号 | batch size、partition count、object count、operator invocation 数、fan-in 形态 |
| 模型服务信号 | queue wait、backlog、endpoint 负载、model wall time、GPU utilization、P99 latency |
| 写回信号 | writeback time、writeback ratio、write batch rows、索引维护成本、driver/worker 写回路径 |

### 4.2 策略动作

策略动作分为三组，其中第二组是当前主重点。

| 动作组 | 具体动作 | 主要解决的问题 |
|---|---|---|
| 数据组织优化 | 调整 batch size / partition count；合并 operator invocation；控制 object 和 fan-in；按 selectivity / prefix / output size 重排 | 减少无效调用、过细任务、过多 object 和 fan-in 成本 |
| 模型服务状态感知调度 | 调整 actor pool 与 bounded in-flight；按 queue wait / backlog 做路由；token-aware / prefix-aware dispatch；避免把等待堆到模型服务队列 | 控制 GPU 服务入口压力，减少 queue wait 和尾延迟，提高吞吐稳定性 |
| 写回约束处理 | COPY + deferred index baseline；sink mode / write batch rows 对比；writeback ratio 高时切换 worker-direct；判断上游收益是否被写回吞噬 | 防止只优化上游局部指标而端到端无收益 |

### 4.3 策略输出

策略输出不是“最终最优算法”，而是一组可执行配置：

```text
(batch_size, partition_count, object_merge)
(actor_pool_size, K_max, routing_policy, dispatch_policy)
(sink_mode, write_batch_rows, fan-in/writeback_path)
```

当前阶段推荐用 rule table + parameter sweep 建立第一版策略。只有当数据量足够、规则表在边界处表现不稳定时，再升级到 learned cost model 或 bandit control。

### 4.4 策略验证

策略是否成立，必须由端到端指标决定：

- latency / rows per second；
- tokens per second；
- queue wait / P99 latency；
- model wall time；
- writeback ratio；
- GPU utilization；
- 不同 workload 下的泛化表现。

如果只在 operator wall time 上变好，但 writeback ratio 上升导致端到端不变，则不能算策略成功。

---

## 5. Reviewer-style 风险审查

### 5.1 Idea-evaluator 视角

Paper type：更像 New Setting + System Method，而不是全新算法。

一句话故事：

> 现有 DB4AI、推理服务和写回系统分别优化各自阶段，但数据库 AI 算子外部执行链路需要把 workload 语义、上游数据组织、模型服务状态和写回约束放在同一条可观测链路中调优。

主要 fatal flaw：

| 风险 | 严重性 | 防御方式 |
|---|---|---|
| novelty 被质疑：看起来只是把 Ray/Daft/vLLM/COPY 拼起来 | MAJOR | 明确每篇文献只贡献一个局部思想；本文贡献是数据库 AI 算子外部链路中的策略选择和端到端验证，不声称发明各局部机制 |
| baseline 不够强：只和手动 HTTP endpoint 或未优化 UPSERT 比 | MAJOR | P0 必须建立 vLLM/Ray Serve 和 COPY + deferred index baseline；否则正式结论降级为预研 |

五维贡献判断：

| 维度 | 当前判断 | 说明 |
|---|---|---|
| Higher | 中 | 不主要提升模型质量，除非 FILTER cascade 需要质量-成本曲线 |
| Faster | 高 | 主贡献在吞吐、延迟、queue wait、端到端时间 |
| Stronger | 中高 | 如果能跨 EMBED/FILTER/COMPLETE 展示策略边界，则鲁棒性较强 |
| Cheaper | 中高 | 减少无效模型调用、GPU 等待和写回浪费，可转化为成本降低 |
| Broader | 中 | 外部执行链路是新 setting，但需要多 workload 支撑，不能只靠 EMBED |

结论：Accept with Revisions。方向值得继续，但必须先补强 baseline 和反证实验，避免把工程拼装误写成方法创新。

### 5.2 Nature-style reviewer 视角

Reviewer 1 likely concern：范围是否过大。数据组织、模型服务、写回都讲，容易像系统工程清单。

应对：图和文字中把主策略收敛到“上游数据组织 + 模型服务状态感知调度”，写回只作为端到端约束和 baseline。

Reviewer 2 likely concern：已有系统已经做了 batching/backpressure/writeback，本文差异在哪里。

应对：逐项说明差异：vLLM 在 GPU 内部，Cortex/Smart 在数据库内部，TurboVecDB/Delta 在存储侧；本文位于数据库外部执行链路的策略选择层。

Reviewer 3 likely concern：证据是否足以证明策略，而不是调参。

应对：实验必须包含强 baseline、消融、曲线、边界验证和跨 workload 泛化；只报单点加速不够。

Devil's Advocate 最强反驳：

> 如果 vLLM 已经把模型服务侧 batch 和 queue 做得足够好，COPY + deferred index 又把写回成本压低，那么本文剩下的可能只是 batch size / partition count 的常规调参。要使论文站住，必须证明数据库 AI 算子的 workload 特征会改变上游提交和路由策略，并且这种改变在端到端指标上超过强 baseline。

---

## 6. 反证条件

以下结果如果出现，需要调整策略主线：

| 反证条件 | 含义 | 调整 |
|---|---|---|
| vLLM/Ray Serve 接入后，外部 `K_max` / routing 对端到端差异 < 5% | 模型服务内部已消化大部分调度收益 | 研究内容二从独立调度贡献收窄为数据库侧数据组织与服务入口约束 |
| COPY + deferred index 后 writeback ratio < 5% | 写回不是主要边界 | 写回保留为 baseline，不再强调 worker-direct |
| Workload-aware batch/partition 与固定最优配置差异 < 5% | workload 特征未带来足够策略价值 | 收窄到单 workload 或转向更强特征，如 token/prefix/selectivity |
| AI_FILTER/AI_COMPLETE 无法按时跑通 | 多 workload 泛化证据不足 | 论文主线以 AI_EMBED 为核心，FILTER/COMPLETE 降级为模拟或讨论 |
| 阶段级最优组合已经等于端到端最优 | “联合最优”不成立 | 不再把跨层联合优化作为核心 claim，保留为端到端效果评估 |

---

## 7. 当前推荐策略版本

实现与实验矩阵参考见 `strategy_design_implementation_reference.md`。本文件负责回答“为什么这些策略有文献依据、哪些不能过度声称”；实现参考文件负责回答“每层有哪些信号、变量、baseline 和指标”。

### 7.1 重新评判结论：从 static plan-time 到 dynamic upstream batching（2026-07-16 更新）

**方向更新**：2026-07-16 经讨论，主场景从 AI_EMBED 转向 AI_COMPLETE（生成式 LLM 推理），上游数据组织从静态固定 batch_size 转向动态 batching policy，Ray 从 task executor 升级为架构设计空间。

核心变化：

- **计划层不再是”选 batch_size”**，而是设计动态 batching policy：
  - Token-budget batching：`max_tokens_per_submission`（借鉴 vLLM `max_num_batched_tokens`），按 token 预算累积行，不按固定行数
  - Length-aligned grouping：相似 token 长度的行合并发送，减少 generation straggler
  - Prefix-aware grouping：共享 system prompt 的行合并为一个请求，利用 vLLM APC（Automatic Prefix Caching）
- **Ray actor 异构化**：不同 token 长度的行路由到不同配置的 actor（ShortTokenActor / LongTokenActor / PrefixAffinityActor），利用 actor 的 stateful + async loop 实现去中心化自适应提交
- **每个 actor 自主决策 flush**：观测模型服务队列深度，queue 空时立刻提交（防止 GPU 饥饿）、queue 满时暂停积攒（防止堆积），不需要中央 scheduler
- **K_max 变为动态**：不再固定上限，而是 queue-adaptive——vLLM `num_running_seqs` 低时增大提交速率，高时减小

仍保留的三层结构：

```text
Three-layer upstream execution strategy（2026-07-16 更新）

Plan-time dynamic batching policy:
  workload/token distribution/prefix share
    -> batching policy type（token-budget / length-align / prefix-aware）
    + actor pool config（actor types × counts × token budget per type）
    + partition_count, object_merge

Run-time adaptive submission:
  queue/GPU/E2E signals -> per-actor flush decision, routing, backpressure
  每个 actor 独立运行 async loop，自主决定提交时机
  K_max 由各 actor 的 queue-adaptive 行为自然形成，不设全局上限

Service-side continuous batching（vLLM 已提供）:
  vLLM scheduler + PagedAttention + APC -> dynamic micro-batch
  本文不修改 vLLM，但上游策略的目标是最大化 vLLM 的调度效率

Guardrail:
  P99/TTFT/TPOT, throughput, writeback ratio, GPU utilization
```

与旧版的关键区别：

| 维度 | 旧版（2026-07-15） | 新版（2026-07-16） |
|---|---|---|
| 主场景 | AI_EMBED | AI_COMPLETE（AI_EMBED 为预研验证） |
| 计划层 | 静态选择 batch_size/partition_count | 动态 batching policy（token-budget/length-align/prefix-aware） |
| Ray 角色 | task executor | 架构设计空间（异构 actor pool + 去中心化 + 自适应） |
| K_max | 固定上限参数 | queue-adaptive 动态行为 |
| 借鉴来源 | 一般性 Ray/Daft 调优 | vLLM continuous batching 原则 + Ray actor 模型 |
| 交互变量 | batch_size × K_max | batching policy × queue-adaptive submission |
| 模型服务 | 手动 HTTP endpoint | vLLM（部署平台 + baseline） |
| RC3 | 研究内容三 | 端到端验证实验 |

保守原则不变：

- 不采用”运行时重切数据库侧已物化 RecordBatch”的方案
- 动态 batching 发生在上游 Ray actor 的攒批阶段，不改变数据库侧已经构造的数据
- vLLM 内部的 continuous batching 不修改，上游策略的目标是给它提供最优的请求流

计划层的具体实现不是在运行中改 batch，而是：

1. SQL 进入外部执行链路前，读取可低成本获得的 workload 特征：行数估计、文本长度分布采样、AI 算子类型、目标 embedding 维度或 token 长度边界。
2. 查询已有 profile / 参数 sweep 结果表，选择一组执行配置：`batch_size`、`partition_count`、`object_merge`、初始 `K_max`。
3. 按该配置构造 RecordBatch / Ray task / actor 输入队列，一次执行中不对已经构造的 batch 做合并重切。
4. 执行过程中用运行层规则调整尚未提交请求的 admission/routing/backpressure 参数。
5. 模型服务侧维护一个短等待队列，在 `max_batch_size`、`max_tokens`、`batch_wait_timeout`、兼容性 key 等约束下形成推理 micro-batch。

因此，计划层更接近数据库系统中的 cost/profile-guided plan selection；运行层更接近推理服务中的 admission control 和 queue-aware routing；服务端 micro-batching 更接近 vLLM continuous batching、Ray Serve dynamic request batching 和 Triton dynamic batcher。三者分开写，避免把低成本调度说成高成本重分区。

在开题和第一阶段实验中，建议采用以下策略口径：

```text
Ray actor-based dynamic upstream batching + vLLM continuous batching

1. 前置：用阶段画像定位瓶颈；建立 vLLM + 小 LLM baseline（Qwen2.5-1.5B 级）
2. 计划层动态 batching policy：根据 token 长度分布、prefix share ratio 选择
   batching policy 类型和 actor pool 配置（异构 actor × token budget × count）
3. 运行层自适应提交：每个 Ray actor 独立观测模型服务队列深度，自主决定 flush
   时机——queue 空时立刻提交，queue 满时积攒
4. 服务端 continuous batching：vLLM 已提供，不修改；上游策略目标是最大化其效率
5. 写回：使用 COPY + deferred index 工程最优 baseline
6. 耦合验证：独立最优 batching policy + 独立最优 submission policy 
   vs 联合 grid search (policy type, actor config, queue threshold)
```

这一定义比旧版”静态参数选择”更贴近 LLM 推理服务的实际需求：它强调上游 batching 策略可以探索按 token 量而非固定行数的动态组织方式，并利用 Ray actor 架构实现去中心化自适应提交。

---

## 8. 对策略设计图的要求

策略设计图应表达：

```text
前置诊断结论
  -> 针对瓶颈的优化设计
  -> 配置与端到端验证
```

图中不应表达：

- 阶段画像本身就是策略；
- 本文已经有 finalized learned optimizer；
- 写回是当前主优化贡献；
- 必须证明“独立最优拼装 vs 联合最优”；
- 使用 `RC/BL`、`联合决策面`、`边界确认`、未解释 `vs` 等内部或模糊标签。

后续如果实验数据支持，再单独画“策略选择规则表 / 控制逻辑图”，而不是在当前方法总图里提前画成复杂算法。
