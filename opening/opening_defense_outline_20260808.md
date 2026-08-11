# 开题答辩内容大纲（20 页主线）

日期：2026-08-08（2026-08-10 按文献—证据—方法链重构）
状态：20 页主讲内容大纲；当前只审内容，不制作 PPT 成品

## 1. 一句话主线

数据库把数据行交给外部 AI 服务时，记录数不能准确表示计算工作量，固定提交上限也不能适应运行状态与多阶段瓶颈的变化。因此，本课题研究数据库 AI 算子外部执行链路中的两项问题：一是把数据组织成携带分阶段工作量、局部性与期限的 work unit；二是先标定当前执行条件下的安全容量范围，再依据实时运行状态进行准入、路由与多作业共享。轻量算子代价估计同时为两项研究内容提供 stage/service/remaining work、SLO slack 和不确定区间，是共同使能部件，不单列为第三项研究内容。

**答辩沟通任务**：汇报结束时，评委应当相信本课题研究的是一个真实、独立且可证伪的系统问题，而不是 Daft/Ray/vLLM 的工程拼接；现有证据已经证明问题、设计动机与技术可行性，但最终动态策略是否超过同上限强静态点仍需论文阶段实验回答。

## 2. 背景、文献与研究空白的论证逻辑

### 2.1 从 AI-Native Data Infrastructure 的演进讲起

背景采用五步递进：

1. **数据基础设施正在 AI-Native 化**：大模型、多模态和数据分析智能体使数据平台从“存储＋SQL 分析”扩展到“SQL＋LLM 推理＋向量计算＋多模态处理”。数据库不再只返回关系结果，还会触发生成、嵌入、分类和语义过滤等 AI 操作。
2. **现实入口已经形成**：Snowflake Cortex AI、BigQuery ML/AI、Oracle AI Vector Search、PostgreSQL pgai/pgvector 等产品已经形成数据库调用模型服务并管理结果的工程路径。
3. **AI 算子改变了传统执行假设**：传统关系算子常用行数、选择率和 I/O 描述成本；AI 算子的成本还取决于 prompt/frame 内容、输出长度、CPU 预处理、tensor 传输、模型阶段、prefix locality 和共享服务状态。同样行数不再对应同样工作量，CPU-only、结构化数据导向的执行方式也难以直接覆盖 CPU＋GPU 异构链路。
4. **已有研究分别推进相邻层**：数据库 AI 系统研究语义算子、质量/成本和物理计划；推理系统研究 KV、continuous batching、prefill/decode 和服务内部公平；Ray Data/Daft 等数据框架研究分区、批处理和 CPU/GPU 流水线。
5. **本课题聚焦 AI Data Infra 的一个关键切片**：在不修改模型服务内部的前提下，研究数据库 AI 算子进入外部异构执行链路后，数据如何形成可比较的 work，系统如何感知状态并控制提交、路由和多 Job 共享，以及代价估计如何支持这些决策。

### 2.2 论文名称按研究问题组织

| 文献主线 | 代表工作 | 已解决的问题 | 留给本课题的问题 |
|---|---|---|---|
| 数据库 AI 算子与语义优化 | Cortex AISQL、LOTUS、Galois、GaussML、Palimpzest、Abacus | AI 算子表达、质量/成本权衡、模型/计划选择、数据库内执行 | 固定模型服务下，上游批数据如何形成 work、控制 active work 和协调多 Job |
| GPU 推理服务与公平调度 | Orca、vLLM、Sarathi-Serve、DistServe、SGLang、VTC、Llumnix | continuous batching、KV 管理、prefill/decode、服务内部公平与迁移 | 不修改 serving scheduler 时，数据库/数据框架如何调节请求到达与共享容量 |
| 分布式数据执行与异构流水线 | Ray、Ray Data Streaming Batch、Daft、NeuStream | task/actor、partition/batch、backpressure、CPU/GPU 流水线 | 缺少面向 AI operator work、服务状态与多 Job SLO 的统一控制接口 |
| 代价估计与配置选择 | Learned Cost Models、GRACEFUL、COSTREAM、Abacus | 查询/UDF/放置成本估计，强调 plan ranking 与 regret | 将解析 work、profile、运行状态和不确定性连接到组织、路由与准入决策 |

PPT 中需要直接出现论文名称，但每条路线只突出 2–4 篇代表工作及其解决的问题；完整题录和更多相近工作进入报告正文与备注。推荐可见组合为：

- 数据库 AI：Cortex AISQL、LOTUS、Galois、GaussML；
- 推理服务：Orca、vLLM、Sarathi-Serve、VTC；
- 数据执行：Ray、Ray Data Streaming Batch、Daft、NeuStream；
- 代价与决策：How Good are Learned Cost Models, Really?、GRACEFUL、COSTREAM、Abacus。

这一页的结论是：**已有工作覆盖了相邻层，但数据库任务语义、外部数据执行和模型服务运行状态之间仍缺少统一的上游控制闭环。**

### 2.3 本课题在 AI-Native Data Infra 中的位置

AI-Native Data Infra 是总背景，覆盖 AI 算子、异构执行、向量计算、Serverless 调度、SQL＋LLM 和数据分析智能体等广泛方向。本课题不试图覆盖整个技术栈，而是选择其中与现有证据和工程平台最匹配的一段：

```text
Database / DataFrame
  -> AI operator data execution
       -> work representation and organization
       -> runtime sensing
       -> admission, routing and multi-job coordination
       -> operator cost estimation
  -> Model Service / GPU Executor
  -> Database / Vector Sink
```

研究对象可表述为：**面向数据库 AI 算子的 AI Data Infrastructure 上游执行与调度方法**。它属于 AI Infra 与数据系统的交叉问题，但不扩张到通用 Serverless 资源管理、模型 kernel、数据分析智能体或完整数据库内核改造。

### 2.4 四条同等严格的证据链

```text
Work Unit：同行数的文本 token work 可差 14.3×，图像 prepare/model 成本也不同
  -> 不能用 rows/images 定义可比 work，需要 staged descriptor 和局部性字段
状态感知：相同静态上限在 high/arrival-limited 下对应完全不同的 running/MFU
  -> 需要 fresh stage/service/job snapshot，过期或签名不匹配时回退强静态点
动态调度：多 job 错峰到达时，预分配份额可能空闲，无约束全局 FIFO 又可能伤害前台 tail/SLO
  -> 固定总 K，动态完成 idle borrowing、completion-time reclaim 和 ordered release，并同时对比 global FIFO/static/DRR
算子代价估计：不同 context 的四个 active-work 候选 E2E 差 12.0%–86.5%，简单均值/解析/lookup 选择失败
  -> 使用解析结构 + profile 校准 + residual correction，以 ranking/regret 而非只看 MAE 验收
```

四条证据链权重相同：每条都必须说清“为什么做、为什么这样设计、证据支持到哪、尚未证明什么”。代价估计仍是两项研究内容的共同使能部件，不单列为第三项研究内容。

PPT 与报告应把 baseline、动机和研究内容组织成连续论证。每一组材料统一使用
同一条四步句法：`baseline/动机现象 → 暴露的缺口 → 对应研究内容与设计 → 验证实验`。
文本 baseline 分产品 database-E2E 与官方 Chat graph 两轨，导出 work 表达、正确性和状态
感知问题；图像动机先用 prepare/model、transfer 形态和 active-window 现象导出 staged work、
CPU/GPU 队列感知和跨阶段提交，再由独立 baseline 图分开能力门禁、12K 结构诊断与
120K matched-resource 排名。不可排名的合同边界必须显式保留，不能为了版面完整
合并成总排行榜。

## 3. 二十页主讲大纲

建议按 18–20 分钟准备。若实际只有 15 分钟，第 6 页文本 baseline 和第 17 页图像 baseline 移入备份，不改变主线。

| 页 | take-away 标题 | 页面任务与必须讲清的内容 | 主视觉/证据 | 依据与讲述重点 |
|---:|---|---|---|---|
| 1 | 数据库 AI 负载的执行优化与调度研究 | 题目、研究对象、姓名与单位 | 极简封面 | 研究平台名称放在实验设置，不进入贡献标题 |
| 2 | 数据基础设施正在从 SQL 分析平台走向 AI-Native Data Infra | 展示 SQL、LLM 推理、向量计算和多模态处理进入同一数据平台的趋势 | AI-Native Data Infra 演进图 | 结合 Snowflake Cortex AI、BigQuery ML/AI、Oracle AI Vector Search、pgai/pgvector |
| 3 | AI 算子改变了以行数和 CPU 为中心的执行假设 | 对比传统 SQL 算子与 AI 算子的成本来源、资源形态和运行状态 | 两条执行路径的平面对比 | 从成本结构和 CPU＋GPU 异构性引出执行问题，不在此页讲项目方案 |
| 4 | 代表性工作分别推进数据库 AI、推理服务和数据流水线 | 论文名称、核心机制和优化边界同时出现 | 三条横向研究线 | LOTUS/Galois/GaussML；Orca/vLLM/Sarathi/VTC；Ray/Ray Data/Daft/NeuStream |
| 5 | AI Data Infra 仍缺少面向数据库任务的上游执行闭环 | 明确本课题在 AI-Native Data Infra 中的位置，以及 work、state、control、cost 四个接口 | `opening_ai_data_execution_boundary` | 聚焦数据库 AI 算子的外部异构执行与上游调度 |
| 6 | 同一任务经过不同执行图会落入不同供给与排队状态 | 左侧产品 database-E2E，右侧官方 Chat graph | `opening_text_baseline_evidence_map` | 两轨分别解释产品语义与服务供给，不合并为绝对排行榜 |
| 7 | 记录数和静态上限都不能描述真实 AI 工作状态 | 同行数 token work 14.3×；同一 W 下运行状态不同；存在最小近饱和点 | `opening_motivation_work_state` | 由数据自然导出 work 表征、状态观测和预先标定的安全容量范围 |
| 8 | 图像把同一问题扩展为 prepare、transfer 与 model 多阶段失配 | CPU prepare/model 比、transfer 形态和 active-window 回退 | `opening_image_stage_aware_evidence` | 说明文本 token work 需要扩展为跨阶段 work |
| 9 | 四 Job 并发会同时延长前台 Short 和全部 Long Job | 各原生系统内部比较 isolated→four-job 的 JCT 变化 | `opening_native_fourjob_normalized_impact` | 强调多作业管理是任务级问题，而非单一框架现象 |
| 10 | 实验现象导出四项同等重要的设计要求 | work→WorkDescriptor；state→sensing；interference→dynamic scheduling；decision risk→cost estimation | 四行因果映射 | 两项研究内容、共同使能和多模态验证在此正式定义 |
| 11 | AI 数据执行层把数据组织、状态感知和调度连接成闭环 | source→organizer→scheduler→executor→sink 与反馈流 | `opening_work_to_schedule_overview` | 研究发生在数据库与模型服务之间，不修改模型内部调度 |
| 12 | WorkDescriptor 把一行数据变成可估计、可组织、可调度的工作单元 | 文本与图像字段、估计来源和运行时更新关系 | WorkDescriptor 字段与消费者关系图 | 字段设计对应后续组织、路由、准入和公平决策 |
| 13 | 数据组织没有全局最优，服务压力会改变 balance 与 locality 的权衡 | 低/高压力下吞吐与 prefix cache hit 的共同趋势 | `opening_work_organization_regime_v2` | 区分互斥实验臂与可联合的设计维度 |
| 14 | 冻结总容量，再随 Job 活跃集调整释放顺序 | per-Job ready/active/completed work、entitlement、idle borrowing/reclaim、SLO debt | 方法流程图或总体图局部放大 | dynamic K 已退出主线；状态无效时回退 global FIFO/DRR，先用 killer baseline 证伪必要性 |
| 15 | 共享调度提高总效率时，也会改变隔离与公平 | Project full/quarter/static/shared 对照，解释 idle borrowing 与 fair queue | `opening_multijob_interference_tradeoff` | 当前结果用于呈现效率—隔离—公平权衡 |
| 16 | 代价估计需要同时评价预测质量和决策质量 | 解析结构＋profile＋residual；pairwise、平均/中位/最坏 regret | `opening_cost_model_decision_quality_v2` | 重点回答估计结果能否正确选择配置 |
| 17 | 图像 baseline 展示不同原生路径的能力与扩展边界 | Direct CLIP、Daft Built-in、Ray Data、Project 的数据与角色 | `opening_image_baseline_evidence_map` | 12K 结构诊断与 120K matched-resource 正式比较分开解释 |
| 18 | 图像四 Job 重现跨模态任务干扰 | Daft Built-in、Ray Data、Project static/shared 的 Short/Long slowdown | `opening_image_fourjob_normalized_impact` | 说明统一 work/state 接口具有跨模态研究价值 |
| 19 | 主实验用同上限强静态 A/B 逐层验证 | descriptor 等价性→observe-only→fallback→admission-only→routing→fairness | 因果实验路线与时间表 | 每一步保存状态、动作和效果 trace；correctness、feeding、stability 是前置条件 |
| 20 | 预期贡献是可估计、可感知、可调度的 AI 数据执行方法 | 两项研究内容、共同使能、跨模态验证和进度风险 | 一条闭环总结 | 回扣第 2 页 AI-Native Data Infra 背景，以本课题的具体切片收束 |

### 3.1 前九页的现场转场

1. 第 2 页结尾：AI 能力正在成为数据基础设施的一部分，但新的算子也改变了原有执行假设。
2. 第 3 页结尾：这些变化同时涉及数据库、推理服务和异构数据执行，因此需要先看相关研究已经做到哪里。
3. 第 4 页结尾：三条研究线都很成熟，但它们之间仍有一段面向数据库任务的上游执行链路没有闭合。
4. 第 5 页结尾：本课题聚焦这段 AI Data Infra；接下来先用现有系统和实验说明问题具体表现在哪里。
5. 第 6 页结尾：不同系统不是简单快慢差异，而是把同一服务推入不同压力状态。
6. 第 7 页结尾：文本已经暴露 work 表征和状态问题；图像进一步说明 work 需要分阶段。
7. 第 8 页结尾：单 Job 阶段失配之外，多任务共享服务还会产生任务级干扰。
8. 第 9 页结尾：这些现象共同导出四项设计要求，形成后续方法部分的入口。

### 3.2 主讲图与备份图分层

主讲只使用第 6–9、13、15–18 页对应的数据图。`opening_native_single_job_request_latency`、
`opening_native_single_job_state_fingerprint`、DuckDB 语义失败明细、两 Job arrival-regime、
K/active-work 全扫描、完整 estimator 表、WorkDescriptor 全字段和指标定义进入备份。
原生四 Job 图负责证明问题存在，Project 四 Job 图负责展示机制权衡，两者不重复；图像 baseline
负责路径能力与可比边界，图像四 Job 图负责跨模态干扰，也不重复。

### 3.3 逐页内容卡片

以下内容卡片是后续报告和 PPT 的共同上游。当前先审内容顺序、论证强度和信息密度，不规定
具体版式，也不要求立即制作页面。

#### 第 1 页：数据库 AI 负载的执行优化与调度研究

- **本页回答**：研究对象是什么。
- **核心内容**：题目、姓名、专业与导师；副标题可写“面向 AI Data Execution Layer 的
  work-unit 构造与状态感知调度”。
- **口头开场**：数据库正在直接触发生成、嵌入和多模态分析，本课题研究这些数据进入模型
  服务前的执行组织与调度问题。
- **页面结论**：本课题研究数据库 AI 算子进入模型服务前的数据执行与上游调度。
- **转场**：先从数据基础设施为什么出现这条执行链路讲起。

#### 第 2 页：数据基础设施正在从 SQL 分析平台走向 AI-Native Data Infra

- **本页回答**：为什么这是一个具有现实基础的研究方向。
- **核心内容**：传统数据平台以结构化数据、SQL 和 CPU 执行为主；新一代平台开始同时承载
  LLM 推理、向量计算、多模态处理和 AI Agent 驱动的分析。用 `SQL分析 → 内置AI算子 →
  AI-Native Data Infra` 表示能力扩展，而不是画成产品发展史。
- **现实入口**：Snowflake Cortex AISQL、BigQuery ML/AI、Oracle AI Vector Search、
  PostgreSQL pgai/pgvector 分别说明生成、嵌入、语义查询和结果管理已经进入数据工作流。
- **页面结论**：数据库正在成为 AI workload 的数据入口和结果管理载体。
- **转场**：新的能力进入数据库后，原有执行假设随之发生变化。

#### 第 3 页：AI 算子改变了以行数和 CPU 为中心的执行假设

- **本页回答**：AI 算子与传统关系算子究竟有什么不同。
- **对比维度**：传统算子常以 rows、selectivity、I/O、CPU cost 描述；AI 算子还受
  prompt/output token、frame/pixel、CPU prepare、tensor transfer、model stage、prefix
  locality 和共享服务状态影响。
- **资源路径**：传统路径以 CPU 与存储为主；文本和图像 AI 路径是 source→prepare→GPU
  model→result 的异构流水线。
- **状态差异**：同一批数据在不同到达率、队列、KV 和多 Job 条件下可能呈现不同执行代价。
- **页面结论**：固定行数、固定 batch size 和固定并发都不足以单独描述 AI 执行。
- **转场**：这些变化分别被数据库、推理服务和数据执行领域研究，但关注层次不同。

#### 第 4 页：代表性工作分别推进数据库 AI、推理服务和数据流水线

- **本页回答**：相关研究已经解决了什么，为什么本课题不是重复已有工作。
- **数据库 AI 线**：LOTUS、Galois、GaussML、Cortex AISQL——解决 semantic operator、
  质量/成本、SQL over LLM 和数据库内 AI 执行。
- **推理服务线**：Orca、vLLM、Sarathi-Serve、VTC——解决 continuous batching、KV 管理、
  prefill/decode 协调和服务内部公平。
- **数据执行线**：Ray、Ray Data Streaming Batch、Daft、NeuStream——解决 task/actor、
  partition/batch、backpressure 和 CPU/GPU 流水线。
- **横向共同线**：Learned Cost Models、GRACEFUL、COSTREAM、Abacus——说明预测误差之外，
  还需评价配置排序和决策损失。
- **页面结论**：相邻层已有强工作，但它们没有共同拥有数据库 Job、上游数据阶段和模型服务
  状态三类信息。
- **转场**：本课题聚焦的正是三者交界处。

#### 第 5 页：AI Data Infra 仍缺少面向数据库任务的上游执行闭环

- **本页回答**：研究空白具体位于哪里。
- **左边界**：数据库知道行、查询、Job、SLO 和结果语义，但通常不知道模型服务实时压力。
- **右边界**：模型服务知道 running、waiting、KV 和完成节奏，但不知道 source backlog、
  Job remaining work、跨行 locality 和写回语义。
- **中间层职责**：把数据库记录变成可比较的 work，感知数据阶段与服务状态，控制 admission、
  routing 和 multi-job credit，并使用代价估计支撑选择。
- **研究边界**：不修改数据库内核、vLLM continuous batching、模型结构或 GPU kernel。
- **页面结论**：研究对象是 Database 与 Model Service 之间的 AI Data Execution Layer。
- **转场**：下面先用同环境 baseline 说明这段链路确实会把服务推入不同状态。

#### 第 6 页：同一任务经过不同执行图会落入不同供给与排队状态

- **本页回答**：现有产品/框架路径表现出的主要问题是什么。
- **产品轨**：SQuAD database-E2E 中 direct、DuckDB AI、Project frozen-static 在统一 source、
  sink、输出质量和 K128 下近似中性，说明后续方法必须面对强静态基线。
- **官方 Chat graph 轨**：bounded control、Daft Native、Daft Ray、Ray Data 使用同一
  ShareGPT manifest；Daft 两臂呈高 waiting/KV，Ray Data 当前 graph 呈低 running/MFU。
- **Project 位置**：右侧 Chat graph 暂不填入不可比的 Project 点；已有 Project 数据使用
  不同 source/sink 或计时边界，只在对应 Project A/B 页面出现。
- **页面结论**：现有路径的关键差异不仅是 JCT，而是过量排队、合理供给或上游欠供给等状态。
- **转场**：要解释这些状态，首先需要重新定义 work 和容量。

#### 第 7 页：记录数和静态上限都不能描述真实 AI 工作状态

- **本页回答**：为什么需要 Work Unit、状态感知和有界控制。
- **work 现象**：固定 16 行时 batch token 最小/最大为 474/6,793，相差 14.3×。
- **state 现象**：同为 W65K，高负载约 169–172 running、MFU 约 35%；arrival-limited
  约 19 running、MFU 约 7%。
- **capacity 现象**：65K/endpoint 达已测峰值约 97.8%，继续增加 work 的吞吐收益很小，
  P99 继续升高。
- **页面结论**：系统需要 work-aware 表征、新鲜状态快照和预先标定范围内的控制动作。
- **转场**：文本主要表现为 token/KV 问题，图像则把 work 扩展为更明显的多阶段问题。

#### 第 8 页：图像把同一问题扩展为 prepare、transfer 与 model 多阶段失配

- **本页回答**：为什么统一 WorkDescriptor 不能只把 token 改名为 image count。
- **阶段失衡**：实用 batch 下 CPU prepare 为 GPU actor 时间的 13.8–31.2×。
- **传输差异**：GPU-resident、pinned H2D 与 pageable ownership-copy 的路径代价显著不同，
  固定图片数不能表示 bytes、prepare 和 model work。
- **提交窗口**：active window 从 16 增到 32 只有小幅收益，继续增到 64 出现等待/回退，
  说明阶段供给和 buffer 压力必须一起观测。
- **页面结论**：跨模态公共接口应表达 source/prepare/model/result stage，而非单一 scalar。
- **转场**：当多个图像或文本 Job 同时运行时，阶段失配还会转化为任务级干扰。

#### 第 9 页：四 Job 并发会同时延长前台 Short 和全部 Long Job

- **本页回答**：多 Job 管理是否只是设想中的场景。
- **比较口径**：每条原生路径内部计算 `four-job JCT / isolated-single JCT`，不做跨框架
  绝对性能排名。
- **现象**：Daft Native、Daft Ray、Ray Data 中 Short 均受到影响，三个 Long 也全部退化，
  但退化幅度和服务压力形态不同。
- **任务含义**：只关注 Short 不足以描述系统；还需 per-Job completed/remaining work、
  isolation、Jain fairness 和 long spread。
- **页面结论**：模型服务只能看到请求，多 Job 的到达、活跃、drain 和公平需要上游管理。
- **转场**：至此，已有现象可以归纳为四项设计要求。

#### 第 10 页：实验现象导出四项同等重要的设计要求

- **本页回答**：前面三组动机如何对应后续方案。
- **Work Unit**：同行数工作量差异和图像阶段失衡 → staged `WorkDescriptor`。
- **状态感知**：同一静态上限对应不同运行状态 → fresh stage/service/job snapshot。
- **动态调度**：多 Job 活跃集变化和前台干扰 → 固定总 envelope、shared work credit、
  idle borrowing/reclaim、ordered release 与 fairness/SLO guard。
- **代价估计**：候选配置选错代价显著 → 解析结构＋profile＋residual，以 ranking/regret 验收。
- **关系**：Work Unit 属研究内容一；状态感知和动态调度属研究内容二；代价估计是共同使能；
  图像负责跨模态验证。
- **页面结论**：四项要求权重相同，但组织与调度仍是两项可独立消融的研究内容。
- **转场**：下一页给出它们在系统中的连接方式。

#### 第 11 页：AI 数据执行层把数据组织、状态感知和调度连接成闭环

- **本页回答**：总体技术路线如何工作。
- **前向数据流**：Database/DataFrame→cost/descriptor builder→Organizer→Scheduler→GPU
  Executor→gather/sink。
- **反馈流**：completion、stage queue、running/waiting/KV、GPU/MFU 和 per-Job progress
  返回 RuntimeStateSnapshot。
- **两项研究内容关系**：Organizer 决定一个 work unit 放什么；Scheduler 决定何时、向哪里、
  以多少 active work 提交。
- **共同使能关系**：代价估计给 Organizer 提供 stage work/uncertainty，给 Scheduler 提供
  service/remaining work/SLO slack。
- **页面结论**：这不是四个松散模块，而是一条有前向 work 描述和反向状态反馈的执行链。
- **转场**：先展开研究内容一的数据结构和组织策略。

#### 第 12 页：WorkDescriptor 把一行数据变成可估计、可组织、可调度的工作单元

- **本页回答**：Work Unit 具体是什么，而不是一个抽象名词。
- **公共字段**：record/job ID、source/prepare/model/result work、primary work、locality key、
  arrival/deadline、uncertainty interval、calibration signature。
- **文本映射**：prompt/output tokens、prefix key、result bytes；图像映射 encoded bytes、
  decode/resize work、tensor/pixel work、embedding bytes。
- **消费者**：Organizer 使用 work/locality 做预算与分组；admission 使用 primary/remaining
  work；router 使用 locality 与 predicted drain；multi-job 使用 remaining work、weight 和 SLO。
- **更新方式**：执行前由解析＋profile 估计，completion 后使用实际完成 work 更新 remaining。
- **页面结论**：WorkDescriptor 既是数据组织的输出，也是状态感知调度的输入。
- **转场**：有了 work 描述后，首先检验怎样组织这些 work unit。

#### 第 13 页：数据组织没有全局最优，服务压力会改变 balance 与 locality 的权衡

- **本页回答**：研究内容一为什么不是简单的 token budget。
- **低压力**：2 endpoint 下五种组织策略吞吐约 50–56k token/s，差异较小，cache hit
  约 60.2%–76.3%。
- **高压力**：4 endpoint consolidation 下保序策略 cache hit 仍约 46.7%–47.5%，重排/
  装箱策略降到 6.4%–7.4%，吞吐同步降到约 39–40k token/s。
- **策略关系**：固定行数、按 token 工作量成批、length-align、best-fit、row-cap 是一次
  消融中的互斥候选；work budget、balance 和 locality 是可联合约束的设计维度。
- **页面结论**：组织策略要同时考虑工作量均衡和 locality，且其价值依赖服务压力状态。
- **转场**：研究内容二利用运行状态决定这些 work unit 何时进入服务。

#### 第 14 页：冻结总容量，再随 Job 活跃集调整释放顺序

- **本页回答**：状态感知如何真正变成控制动作。
- **离线标定**：在固定机器、模型、协议和 workload 下冻结最小饱和、安全的总 K/work
  envelope；K160 只是当前签名的强静态点，不是通用常数。
- **在线观测**：per-Job model-ready/active/remaining/completed work、arrival/drain、queue age、
  SLO debt，以及 running/waiting/KV、TTFT/ITL；GPU/MFU 只作交叉验证。
- **动态动作**：空闲 Job 的未用份额可借；前台/新 Job 到达后，不抢占已进入 vLLM 的请求，
  只在 completion 释放 credit 时按 entitlement、service lag 和 SLO debt 回收未来份额。
- **回退**：状态过期、签名不符或 ledger 异常时保持冻结总 K，并退回简单 DRR/FIFO；不在线猜
  新容量档位。
- **当前基础**：已有 trace、observe-only snapshot、completion release、shared credit 和
  控制器原型；capacity-only SAOR 未超过 K160，dynamic K 已退出主线。
- **页面结论**：动态调度不等于动态 K；本项目要验证的是固定总容量下“下一份 credit 给谁”。
- **转场**：接下来用多 Job 数据观察借用带来的效率与公平代价。

#### 第 15 页：共享调度提高总效率时，也会改变隔离与公平

- **本页回答**：shared work credit 的收益和代价是什么。
- **反事实控制**：full/quarter single 用于分离配额损失；static partition 与 shared pool 才是
  同一全局上限下互斥的调度 A/B。
- **当前结果**：shared 相对 static 的 group throughput +8.68%、Group JCT −7.97%、MFU
  相对 +22.41%，但不同 Job 收益不均。按实际完成 work 计算的 group Jain 为 0.960→0.923；
  图中按各自 single control 归一化的进度 Jain 为 0.998→0.876。两种 Jain 口径不能混用。
- **机制关系**：idle borrowing 提高 work conservation；per-Job floor/cap、work-fair deficit
  和 SLO guard 约束隔离；状态感知再决定总准入与路由。
- **页面结论**：动态调度不是单目标提吞吐，需要同时评价 efficiency、isolation 和 fairness。
- **证据缺口**：现有矩阵缺 global FIFO/no project Job scheduler；下一项 formal 必须加入
  FIFO 与 DRR killer baseline，简单策略达到同一 Pareto 前沿即淘汰 SAOR。
- **转场**：组织、准入和公平决策都需要一个可比较的代价信号。

#### 第 16 页：代价估计需要同时评价预测质量和决策质量

- **本页回答**：为什么算子代价估计不能只报告执行时间 MAE。
- **模型路线**：解析 work 特征保留物理结构，少量 profile 校准，再对 residual 做轻量修正；
  不预设复杂模型更好。
- **实验规模**：429 个 formal 观测、20 个 context、每个 context 四个 active-work 候选。
- **两个门槛**：pairwise accuracy 检查候选相对顺序；decision regret 检查选错配置的实际代价，
  同时报告平均、中位与最坏 context。
- **初步结果**：Hybrid pairwise 0.808、macro regret 2.90%、max regret 14.72%，只比 15%
  门槛低 0.28 个百分点，因此属于初步可行而非成熟模型。
- **页面结论**：代价估计是否有用，要看它能否选对组织/容量/路由配置，而不只是拟合单点时间。
- **转场**：最后用图像 baseline 和多 Job 结果检验统一抽象是否跨模态成立。

#### 第 17 页：图像 baseline 展示不同原生路径的能力与扩展边界

- **本页回答**：图像场景有哪些可比较 baseline，项目静态路径处于什么位置。
- **12K 诊断**：Daft Built-in、Ray Data、Project 均 exactly-once；Daft Built-in 约 65.2s、
  Ray Data 约 17.8s、Project 约 15.9s，但快臂时间较短，因此只作结构诊断。
- **120K 正式比较**：matched CPU8/16 下只比较 Ray Data 与 Project frozen-static；Project
  JCT 约低 10%/17%，跨两轮冻结 headline 为约 13%–15% 静态结构信号。
- **Daft Built-in 边界**：12K 可运行，20K 出现 object-store OutOfDisk，故不能被补进
  120K matched-resource 排名；这属于扩展边界，不是漏画。
- **其他路径**：Direct CLIP 是容量参照；vLLM pooling capability gate 阻塞，不生成虚构值。
- **页面结论**：图像项目静态结构已有初步可重复信号，但状态感知动态增量仍待验证。
- **转场**：单 Job 静态结果之外，图像多 Job 也出现任务级干扰。

#### 第 18 页：图像四 Job 重现跨模态任务干扰

- **本页回答**：文本中的多 Job 问题是否只与 vLLM/token 有关。
- **统一画法**：与文本四 Job 图相同，行是执行路径/互斥策略，列是 Short 和三个 Long，
  每格只表示本路径内 `four-job/isolated-single` JCT。
- **原生现象**：Daft Built-in 的 Long 为 2.13×–3.19×，Ray Data 为 1.06×–1.64×，
  干扰形态明显不同。
- **Project 现象**：static 四个 Job 约 1.74×–1.81×；shared 为 1.12×–1.78×，但 group JCT
  只差 0.98%，且状态快照仍是 observe-only。
- **页面结论**：图像同样需要 per-Job staged work、ready/active/remaining 状态、隔离和公平，
  但现有结果不代表图像动态策略已经胜出。
- **转场**：最后给出怎样把上述方案逐层验证为论文结论。

#### 第 19 页：主实验用同上限强静态 A/B 逐层验证

- **本页回答**：开题后怎样把拟研究方法变成可归因结果。
- **研究内容一**：fixed rows/images→scalar work budget→staged work→balance/locality→最小组合；
  固定调度和资源，只改变组织。
- **研究内容二**：descriptor/legacy 等价→observe-only→stale/fallback→admission-only→
  routing-only→fair-sharing→最小联合候选。
- **场景**：steady near-saturation、arrival-limited、low↔high phase change、burst、多 Job overlap，
  以及“上游无 ready work”的阴性控制。
- **共同合同**：同一 manifest、模型、资源、最大 K/work、输出语义；1 warmup＋交错 formal；
  correctness、feeding、stability 先通过。
- **评价**：correct throughput/SLO goodput、JCT/P99、MFU/energy、Jain/isolation、状态—动作—
  效果 trace；代价模型另报 ranking/regret。
- **停止规则**：相对 strong static 未达到预注册增量，或关键指标退化，则记录失效边界并停止
  扩扫，不更换 workload 追求正结果。
- **页面结论**：每个设计都有独立对照、同上限约束和可接受的阴性结论。
- **转场**：由此形成预期创新、工程产出和阶段计划。

#### 第 20 页：预期贡献是可估计、可感知、可调度的 AI 数据执行方法

- **本页回答**：课题最终准备交付什么。
- **预期创新一**：统一 token/frame 与分阶段 work、locality、期限和不确定性的 WorkDescriptor，
  并刻画 balance/locality 的 regime dependence。
- **预期创新二**：在预先标定范围内，利用 fresh runtime state 调节 active work、路由与
  多 Job 份额，同时约束 SLO、隔离和公平。
- **共同使能**：面向配置选择的轻量代价估计，以 ranking 与 decision regret 验收。
- **验证与产出**：文本 AI_COMPLETE＋图像 AI_EMBED/CLASSIFY，同资源强 baseline、可复现实验
  系统、原始 trace、实验报告和论文。
- **风险口径**：若动态方法未超过静态点，收敛为容量标定、regime 诊断和失效边界；若图像
  持续由 CPU prepare 主导，则限定为异构流水线组织，不外推为模型服务优化。
- **页面结论**：预期形成两项研究内容、一个共同使能组件和一套跨模态验证方法。
- **结束句**：本课题希望回答的不是“如何让一次模型调用更快”，而是“数据库 AI 任务如何
  形成可比较的 work，并在变化状态下稳定、公平地使用固定 GPU 执行容量”。

### 3.4 背景、动机、研究内容与实验的对应关系

| 论证环节 | 大纲页 | 现象或文献依据 | 导出的设计 | 后续验证 |
|---|---:|---|---|---|
| 研究背景与空白 | 2–5 | 数据库 AI、LLM serving、分布式数据执行三条研究线 | Database 与 Model Service 之间的 AI Data Execution Layer | 第19页统一实验合同 |
| Work Unit 与数据组织 | 7–8、12–13 | 同行数 token work 14.3×；图像 prepare/model 与 transfer 差异；高压力下 locality/吞吐同步下降 | staged WorkDescriptor；work budget＋locality 约束 | fixed rows→scalar/staged work→balance/locality 消融 |
| 状态感知 | 6–8、14 | 同一 W 下 high/arrival-limited 状态不同；原生 graph 呈 overqueue/underfeed；图像阶段供给不足 | fresh stage/service/job snapshot；校准签名；静态回退 | observe-only、stale/fallback、状态识别准确性与控制开销 |
| 动态与多 Job 调度 | 9、14–15、18 | active-work 存在平台/过载；文本和图像四 Job 均出现 short/long 干扰 | bounded work credit、completion replenishment、idle borrowing、fair/SLO guard | 同最大 K/work 的 admission→routing→fair-sharing A/B |
| 算子代价估计 | 16 | 候选选错代价显著，逐行误差低不等于配置选择正确 | 解析结构＋profile＋residual；uncertainty | context-LOO、pairwise、mean/max regret 与 online decision regret |
| 多模态泛化 | 8、17–18 | 文本以 token/KV 为主，图像以 prepare/transfer/model 为主，但都存在 work/state/job 问题 | 公共 descriptor/snapshot/credit 接口，模态 adapter 只负责 work 映射 | 文本 AI_COMPLETE＋图像 AI_EMBED/CLASSIFY 使用同一策略代码 |

这张对应表用于检查主线，不直接放入汇报页面。若某个后续实验不能落到表中的一行，就不应
进入开题主任务；若某个设计没有前置现象或文献依据，也不能仅因为代码已经存在而写进方案。

### 3.5 主讲时间与删减顺序

| 部分 | 页码 | 建议时间 | 必须讲清的内容 |
|---|---:|---:|---|
| 背景与研究空白 | 1–5 | 4 分钟 | AI-Native Data Infra 背景、AI 算子执行变化、研究层次与边界 |
| Baseline 与动机证据 | 6–10 | 5 分钟 | 原生系统行为差异、work/state/multi-Job 现象、四项要求如何被导出 |
| 研究内容与共同使能 | 11–16 | 6 分钟 | 总体闭环、数据组织、状态感知提交、多 Job 权衡、代价估计 |
| 多模态、验证与贡献 | 17–20 | 4 分钟 | 图像可行性与干扰、后续实验合同、预期贡献和风险 |

按 18–20 分钟准备主讲。若现场只给 15 分钟，不删除任何研究问题，而是：第 4 页只保留每条
文献线的一项代表工作；第 6 页只讲 JCT 与 queue/TTFT 的因果补充；第 17 页并入第 18 页；
第 19 页只保留“strong static→observe-only→单动作 A/B→联合验证”四步。被删细节进入答辩
备份，不通过加快语速压缩。

## 4. 开题报告结构参考

报告与 PPT 使用同一事实，但不按 20 页顺序逐页扩写。报告建议保持八章：

| 章节 | 核心任务 | 主要内容 | 章节落点 |
|---|---|---|---|
| 1 背景、目的与意义 | 从 AI-Native Data Infra 演进收敛到本课题 | 数据平台能力扩展、AI 算子成本变化、CPU＋GPU 异构执行、数据库触发外部 AI 的现实链路 | 数据库 AI 算子的外部执行链路具有独立研究价值 |
| 2 国内外研究现状 | 用代表论文组织研究边界 | 数据库 AI、GPU serving、分布式数据执行、代价估计四条文献线 | 相邻层已有强工作，交界处仍需要 work/state/control 闭环 |
| 3 研究目标、问题与边界 | 把文献空白和本地现象转成研究问题 | Work Unit、感知、动态调度、代价估计与多模态验证 | 两项研究内容和共同使能关系清楚 |
| 4 研究内容与技术路线 | 定义数据结构、算法接口和消融顺序 | WorkDescriptor、runtime snapshot、controller、cost estimator、multimodal contract | 形成从数据组织到调度决策的完整方法链 |
| 5 前期工作与可行性 | 按研究问题组织已有实验 | baseline、work/state 动机、组织、多 Job、图像、cost | 说明问题真实、技术可实现，并给出初步设计信号 |
| 6 后续实验与进度 | 给出同上限、可停止的验证路线 | strong static、observe-only、单动作消融、跨模态矩阵 | 每个实验都对应一个可回答的研究问题 |
| 7 预期成果、创新与风险 | 说明方法产出、系统产出和降级路径 | 表征、控制、估计、代码、数据、报告和风险门禁 | 创新落到具体方法与严格验证合同 |
| 8 参考文献 | 提供论文和工程资料的可追溯来源 | 顶会论文、正式期刊、官方文档、产业系统资料 | 文献类型与其在论证中的作用相匹配 |

第 5 章内部重组为：`5.1 baseline 合同与比较边界 → 5.2 work/state 动机 → 5.3 研究内容一
初步证据 → 5.4 研究内容二多 Job 证据 → 5.5 图像跨模态证据 → 5.6 代价估计 →
5.7 结论层级与后续研究问题`。当前报告中独立的“最小饱和 active work”并入 5.2，避免与
动机小节重复；原生单 Job 的完整 queue/TTFT/KV/MFU 表进入 5.1 支撑或附录，不再独占主线。

## 5. 已落地的数据与可排名边界

### 5.1 两组 replacement database-E2E

| workload | 作用 | 三臂 | 当前动作 | 必过门禁 |
|---|---|---|---|---|
| SQuAD 均匀控制组 | 验证统一 source/sink 和质量口径 | bounded direct static-sharded、DuckDB AI static-sharded、project frozen-static | correctness/稳定性通过；project/direct service=1.0087，近似中性 | 只作该 workload 静态地基，不外推到 ShareGPT |
| ShareGPT 受控异质组 | 验证长短 work 异质下的容量与语义边界 | 同上 | correctness 护栏通过；后续 C32–C256 扫描冻结 bounded C128；旧 project/C32-direct=1.5457 不排名 | C128 达 C256 已测峰值 98.22%；正式原生矩阵用 C128 |

每臂至少汇总：correct rows/s、database-E2E wall time、service/operator tokens/s、request P50/P95/P99、GPU util time-series、MFU、显存、功耗/能耗、J/1k token、running/waiting、KV usage、prefix hit、各 pipeline 阶段时间、质量、failure 和成本假设。图中只放支持主结论的 3–5 个指标，其余进入结果报告表。

### 5.2 文本原生系统同环境对照

| 轨道 | arms | 作用 | 最小合同 |
|---|---|---|---|
| Chat原生框架轨（已完成） | bounded Chat control、Daft `prompt()` Native/Ray、Ray Data HTTP Processor | 测量现有框架在异质work下的JCT、service throughput、feeding、资源与可观测性边界 | 同一ShareGPT controlled-skew manifest；各臂独立冻结运行点；1+3交错formal；不与Project不同T0/arrival合同作绝对排名 |
| Project eager诊断（已完成） | project frozen-static all-at-t0及full/half/static/shared多Job匹配控制 | 排除71.24s来源并分离quota与competition | 只作Project内因果和与Daft Native对齐T3诊断；不升级为完整框架容量排名 |
| DuckDB有界输出产品轨（已完成） | DuckDB AI vs同manifest direct/project | 检验数据库AI产品入口的database-E2E、质量、错误和可观测性 | SQuAD/cap=64可作静态地基；ShareGPT fixed-cap语义不兼容，只作产品护栏 |

两轨分开是语义门禁，不是为了选择性报告。同环境原生框架对照用来发现问题，不预设项目一定赢；只有计时、语义和 scheduler-owner 边界一致的指标才同表排名。

官方 Chat graph 单 Job 图中不加入 Project 数值。现有 Project database-E2E 点包含 PostgreSQL
source/sink，eager 诊断又只有 512 行且计时起点不同；当前没有同一 2,048-row
graph→gather 正式点。答辩时直接说明“合同不匹配，保留空缺”，不能用不可比点补齐版面。

### 5.3 多 Job 原生观察与项目机制对照

| 层次 | arms | 回答的问题 | 边界 |
|---|---|---|---|
| 原生系统观察 | Daft Native、Daft Ray、Ray Data 各自启动 short/long 两个错峰 job | 独立数据作业共享同一模型服务时，是否出现全局压力叠加、干扰、资源超卖或可观测性缺口 | 框架自己拥有 batching/backpressure；Job 启动后完整 manifest 可用，不重放项目逐请求 arrival；不注入项目 credit/router；不把 barrier 冒充 request P99 |
| 项目因果 A/B | `project_static_partition` vs `project_shared_work` | 感知 job 活跃/完成状态并借用空闲 work credit，能否在相同 endpoint 总 K/work 下改善 JCT/tail/fairness | 只改共享与 idle-borrowing 策略；1+3 后无论正负均停止，不扫 offset/weight 追正 |

2026-08-09 已完成统一5s offset：原生三轨 short JCT 相对各自single增加
82.42%/104.84%/32.76%，均有实际overlap。项目在线replay下quota-only≈0，shared提高
总吞吐但short/Jain回退；统一eager后quota-only已使short JCT+59.00%，matched
static+long又+58.77%，matched shared+long+28.90%。eager shared相对static使short JCT
−48.94%、总吞吐+31.85%、long JCT−25.75%、Jain 0.894→0.972。因此开题结论是
“多Job管理必须感知arrival regime、支持idle borrowing并显式约束SLO/fairness”，不是
“shared/dynamic全面优于static”。

在线5s矩阵只统一Job级启动；新增Project eager矩阵又把DB arrival span压到66.76µs，
但Daft仍缺准备前T0。因此不作跨轨绝对JCT比较；只展示项目matched-cap因果A/B和各原生轨
内部single→overlap变化。

主指标是 per-job/group JCT、goodput、Jain fairness、isolation、global running/waiting/KV/GPU/MFU 时序。`borrowed_work_seconds` 只在项目 A/B 中由请求/credit trace 计算；原生框架无 job-level active-work 标注时必须明确标记不可观测。

### 5.4 设计—实现—后续验证状态

| 等权部件 | 动机证据 | 当前实现 | 开题落点 | 后续验证 |
|---|---|---|---|---|
| Work Unit | 同行数 token work 14.3×；图像 prepare/model 阶段失衡 | staged descriptor 类型、neutral work consumer 和图像携带接口已存在；正式 runner 尚未构造 production descriptor | 字段设计由现象导出，接口可执行 | staged organization 已端到端胜出 |
| 状态感知 | 同 W 下 high/arrival-limited 状态不同；原生路径出现 underfeed/overqueue | endpoint、vLLM 和 GPU/MFU trace 已正式采集；图像 fresh stage snapshot 已 observe-only 接入 | 必须联合 ready/active work、完成速率、queue、KV/MFU/tail，并校验 freshness/signature；GPU/MFU 不单独触发动作 | snapshot 正式驱动 fixed-K Job release 后产生可归因增量 |
| 动态调度 | 5s 两 job 显示真实前台干扰和效率—隔离—公平权衡 | completion release、least-work、shared DRR credit 已进入调度器并完成 A/B；capacity-only SAOR 未晋级 | 总 K 固定；动态对象是 active-set entitlement、idle borrowing/reclaim 和 release order；缺 global FIFO killer baseline | fixed-K FIFO/static/DRR/VTC-style/SAOR 决定性 A/B 尚未完成 |
| 算子代价估计 | 候选选错代价 12.0%–86.5%；简单 estimator 决策失败 | CE1–CE5 离线分析器与 context-LOO 已完成；尚未在线驱动调度 | 文本配置选择有 marginal feasibility | 已预测跨模态 remaining work/SLO 并改善在线决策 |

工程下一步按 production descriptor builder → 统一状态快照 → global FIFO/no-op gate → fixed-K
ordered-release 执行器 → 单动作消融推进。先闭合“状态→决策→动作→效果 trace”，不把四个部件
同时接入后再做无法归因的总对比。

### 5.5 可直接复用的正式/初步证据

| 证据组 | 目的 | 当前结论 | 还需动作 |
|---|---|---|---|
| token-work 异质性 | 证明 fixed rows 不是成本代理 | 固定 16 行 batch token 最大/最小 14.3× | 核对 CSV 溯源并保留直接标注 |
| active-work frontier 与状态差异 | 证明低供给、最小近饱和点、边际收益递减及状态变化 | 65K/endpoint 约达已测峰值 97.8%；继续加压主要抬高 P99 | 图中分开画容量结果与运行状态，不用未定义区间着色 |
| organization regime | 证明组织策略受 serving/KV/locality 状态影响 | 相同双卡硬件下，2 endpoint 低 KV 压力时策略范围约 12%；4 endpoint consolidation 高 KV 压力下分化约 27% 且重排破坏 prefix group | 保留一张机制图；严格 feeding-saturation 边界可见，不等于动态方法收益 |
| image exact-path profile | 证明跨模态存在分阶段瓶颈 | CPU prepare/GPU service 13.8–31.2× | 统一单位与质量合同，暂不做 proposed 胜出 claim |
| cost decision quality | 证明代价估计有资格作为共同使能候选 | pooled regret 1.67%、macro 2.90%、max 14.72%，pairwise 0.808 | 主图只保留决策质量；完整 estimator 对比放附录 |

## 6. 开题后必须补齐的论文实验

### 6.1 强 baseline 与 provenance

- 文本和图像都按相同环境、当前 commit、统一 source 与输出语义比较；调度主实验统一到完整结果 gather，database-E2E 护栏才统一 sink；
- Daft built-in 和 Ray Data native API graph 必须由框架自身拥有调度；
- project typed actor frozen-static 是 proposed 的强静态对照；
- vLLM pooling 只有在模型与任务语义等价时进入图像 baseline；
- 记录 upstream URL/commit、实现来源、scheduler owner 和适配 diff。

### 6.2 数据组织独立实验

1. fixed rows/images；
2. scalar token/frame budget；
3. staged work budget；
4. balance-aware；
5. locality-aware；
6. balance + locality 组合。

先固定调度与资源，只改变 organization。报告 batch-work CV、packing、oversize、stage/endpoint skew、locality preservation、queue age、throughput、tail、quality 和 energy。

答辩口径必须区分实验臂与设计维度：fixed rows、sequential token budget、length-align、
best-fit、row-cap 在单次组织消融中是互斥候选；work budget、balance、locality 是可以联合约束
的设计维度。数据组织输出 WorkDescriptor，状态感知调度消费它；两项研究先独立搜索，再做
独立最优拼接与联合 grid 对照，不能把候选策略收益直接相加。

研究内容二也采用相同口径：full/quarter single是配额反事实控制；static partition与shared
pool是同上限互斥A/B臂；runtime sensing为总准入/路由提供信号，idle borrowing与fair queue
是shared内部可联动的分配机制。答辩按“容量上限→运行状态→跨Job分配→隔离护栏”解释，实验按
`static → shared → fair → state-aware admission`逐层消融，不把各层收益直接相加。

### 6.3 同上限 static–dynamic 因果实验

按顺序运行 steady underload、steady near-saturation、overload guardrail、low→high/high→low、burst arrival、short/long 或 easy/hard mix，并保留“上游无 ready work”的阴性控制。每个场景在相同最大 K、active-work、buffer bytes、CPU/GPU 和 actor 数下比较 frozen-static、observe-only、admission-only、routing-only 和最小联合候选。正式结果同时保存 ready/active work、状态 freshness、控制动作、work-credit 变化和后续完成速率，证明负载变化确由系统而非实验人员手动完成。

动态策略只有在吞吐、SLO goodput、P99/JCT 或资源效率至少一项改善约 5%，且 correctness、failure、其他关键指标无不可接受退化时才晋级。steady 场景的目标是 no-regression，不要求制造正收益。

### 6.4 多 job

覆盖 1/2/4 job、staggered overlap、3:1 weighted、异构 work mix 和 arrival offset。报告 per-job JCT/P99/goodput、Jain fairness、isolation、work conservation 和 idle borrowing。按请求数公平与按预计 work 公平必须同时出现，说明为什么 WorkDescriptor 会改变结论。

### 6.5 图像完整验证

统一 CLIP 模型/processor/dtype/normalization、PostgreSQL BYTEA source、到 gather 完成的 operator-E2E 边界、CPU/GPU reservation 和冻结 ground truth，比较 bounded direct、Daft built-in、Ray Data native、project frozen-static，最后才加入 project dynamic。workload 至少包含 uniform、decode-cost skew、phase shift、burst 和 two-job mix；报告阶段队列、tensor bytes、correct embeddings/s、JCT、energy、embedding finite/norm/digest。Recall@K/nDCG 与 pgvector exactly-once sink 作为小规模质量/工程闭环单列，不进入调度性能主排名。

### 6.6 算子代价估计作为共同使能的独立门禁

代价估计需要分别回答“预测准不准”和“决策是否因此更好”：

- 文本：input、output、service、remaining work 与 SLO slack；
- 图像：prepare work、model work、tensor/buffer pressure；
- 指标：MAPE/区间覆盖仅作预测质量，ranking、pairwise、configuration regret 和 online decision regret 才是主指标；
- 消融：无估计、简单解析、profile 校准、residual correction、带不确定区间；
- 外部有效性：独立时间段或 held-out workload，必要时第二硬件 calibration signature；
- 若估计器不能稳定排序候选，它只能作为 tracing 字段，不能驱动 organization 或 scheduler。

## 7. 需要绘制的图与数据合同

| 图 | 唯一问题 | 画法 | 数据来源 | 完成条件 |
|---|---|---|---|---|
| A 动机：work 与状态 | 为什么 rows 和固定上限不足 | 左：固定行数的 work 范围；右：低供给—最小近饱和点—边际收益递减及 high/arrival-limited 状态 | 正式 CSV 聚合 | 每个点/线直接标义；不出现无数据定义的区间色带 |
| B 研究边界与主线 | 两项研究和共同使能如何连接 | 数据流 + 反馈流；cost estimator 同时连 organizer 与 scheduler | 方法合同 | 不把 cost 画成第三项研究内容 |
| C organization regime | 组织收益为何依赖 serving regime | 低压力/高压力 small multiples 或 dumbbell；附 locality 机制注释 | cache-on 正式结果 | 一张图只讲 regime dependence |
| D 图像 stage-aware | 为什么跨模态需要 staged work 和阶段状态 | CPU prepare/GPU service 比 + transfer 形态 + active-window screening | image exact-path profile 与 screening | 不把 microprofile/screening 当系统排名 |
| I 图像 baseline | 图像路径的能力边界与可比数据分别是什么 | 报告独立表格讲角色/门禁；数据图仅画 12K 诊断与 120K matched-resource 对照 | image operator formal + vLLM capability gate | 结构图与数据图不混用；只有 120K Ray Data/Project panel 可排名 |
| J 图像 multi-job | 图像 Short/Long 并发影响是否依赖执行路径，现有共享额度改变什么 | 三栏无连线归一化点图；Project static/shared-credit 并列 | image native + project four-job formal compact summary | 只作各路径内部比较；Project 状态仅观测；原生路径无统一阶段计时 |
| E cost decision quality | 代价估计是否能帮助选择 | median/macro/max regret、pairwise 与门槛；不堆所有预测散点 | cost-profile formal | 明确共同使能和 conditional 结论 |
| F 原生单 Job 状态指纹 | 现有原生 graph 如何落入不同服务压力区 | 左：JCT/tok/s；右：running、waiting、KV、MFU 原单位 small multiples；标 underfeed/minimum-saturation/overqueue | `opening_text_native_single_job_formal_20260808` 12 formal | 只解释外部现象；database-E2E 三臂降为 appendix correctness/语义表 |
| G static–dynamic | 状态变化下动态是否超过同上限静态 | 当前只保留 workload phase 与同上限 A/B 实验合同 | 无结果不画图；论文正式运行后再决定图型 | 最大 K/work/resources 完全匹配 |
| H multi-job | 多Job把Short/Long分别影响多少；shared credit如何改变效率与公平 | 四Job按同一Job连接独立→1/4配额→Static→Shared；右侧给组效率、Static→Shared进度、Jain与long spread | 开题两/四作业 formal；论文阶段扩展 weighted/held-out | 连线表示受控场景顺序而非时间；Static/Shared仍是同上限互斥A/B；仅一个offset/equal-weight workload |

2026-08-10 已完成 A/T/N/C/H/D/I/J/E 九张正文数据图与 F 状态备份图。G 无结果且不画，
database-E2E 只保留附录表。N 使用三条原生轨各自的 four-job/isolated-single 归一化
影响，H 使用 `Short@0s → 3 Long@5s` 的 Project quota/competition/shared 与效率—公平
权衡；两Job arrival-regime 放附录。所有误差线表示三次 formal 的离散，warm-up 不进入
统计，三次原始点在关键图中直接显示；J 的本地紧凑归档只有 formal 汇总，因此主图明确
使用三次 formal 均值比值，离散度保留在结果表而不伪造原始点。

## 8. 停止规则

- replacement 三臂、文本 Chat 原生单 job、两 job 因果点和四 job 扩展均已完成；开题前不再换模型、数据库、workload、offset、weight、Job 数或扩大并发扫描追正结果；
- DuckDB 仅保留在语义成立的有界输出产品轨；Daft/Ray Data 多 job 只做原生系统观察，不给它们注入项目调度器；
- K256 已覆盖当前每 endpoint 校准上界；K512/endpoint 只用于独立过载退化研究；
- 动态未超过同上限强静态点时记录失效边界，不换弱 baseline 或挑 workload；
- 代价估计现有 429-run 仅声称文本配置选择初步可行；图像 held-out 仅在无法用已有 profile 数据构造决策对照时才新跑，不扩为 TPC-H 或复杂模型搜索；
- 图像 official baseline 未满足同语义和 scheduler-owner 合同时不进入主排名；
- 未通过 feeding、correctness、quality 或稳定性门禁的数据不能进入结论图；
- 当前只冻结本大纲、实验数据和图，不制作或同步新的 PPT 成品，也不同步 Wiki。
