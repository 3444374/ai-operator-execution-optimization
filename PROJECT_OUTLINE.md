# 项目大纲

本文维护研究问题、两项研究内容与近期顺序。实现事实见[代码状态](code/INFRA_STATUS.md)，
运行身份与可支持的判断见[证据台账](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)，数字与失败见原报告。
系统名称为 SemLoom，DB-AIEL 仅表示架构层；术语见[CONTEXT.md](CONTEXT.md)。

## 0. 当前优先级与历史记录范围

当前主线是 SemLoom 外部执行核心与语义 API 工作负载。PG 实现、接线和资料归档保留，
当前不作为开发或验收前提，是否恢复以后另行决定。

近期首先回答：**同一查询／一批数据，能否通过少准备、少搬运和少做重复计算更早得到全部必要结果？**
完整任务时间与有效吞吐是主要结果，GPU 利用率用于解释供给与等待。
优先采用原生可复用模型 worker、索引已存在的完整查询；启动、首次建库与退出另报，
每查询原生必需的重建仍计入，具体计时见[对照规范](experiments/plans/reference/对照规范.md#resident-query-timing)。

LOTUS 主线由 SemBench 提供任务与评价，保留 LOTUS 原方法及全局校准。
近期聚焦**面向语义算子链的 GPU 资源编排**：依据合法依赖、实际工作与资源状态协调模型供给，
同时考虑 CPU 准备、传输、主机驻留和结果消费，以完整查询时间评价；职责和单卡／未来多副本边界见[架构](experiments/plans/系统架构.md#semantic-chain-gpu-orchestration)。
近期按[物理阶段观察方案](experiments/plans/数据组织.md#dag-resource-observation)推进：真实执行观察、跨任务特性归纳、
可解释的编排动作、同条件验证。以单算子、分支汇合和选择性串联覆盖不同需求，先核源码与数据资格，
再定位准备、搬运、模型计算、缓存和消费的实际成本；流水重叠是强工程起点，不是充分的研究增量。
[选择性语义链的合法依赖与物理数据寿命](experiments/plans/数据组织.md#selective-chain-plan)是其中一个候选，
与[GPU／执行引擎协同候选](docs/research/优化方法依据.md#gpu-engine-coordination)一并依据观察选择，不预定图片缓存为唯一主线。
保持原请求的执行优化与字段／提示布局变化分别评价；后者先定义语义和质量要求，不能由字段集合相同推定模型输出等价。
普通／级联 Filter 是方法参照、无机会反例与全局依赖场景；[动态供给扫描](#dynamic-resource-feedback-goal)与[阶段资源编排](experiments/plans/系统架构.md#physical-stage-research-goal)分别验证，单纯增大并发本身不作为研究增量。
近期先落实[首批真实查询观察](experiments/plans/数据组织.md#dag-first-observation)，既有逐记录后继推进作为可复用能力与对照；
实际默认、接入与资格从[代码状态](code/INFRA_STATUS.md#record-local-sections-c827)核对。

records→结果独立建设，旧 request 接点按实际适用范围复用。LOTUS 目标配置为 Daft 扫描／普通关系处理
加 Ray Core／Actor，SemLoom 自管准备、供给与回收；Ray Data、Daft、IMLane 对照各自原生执行。
职责见[架构](experiments/plans/系统架构.md#lotus-data-execution)，接口、模型资格与候选状态从代码状态和证据台账核对。

## 1. 题目与研究对象

题目为：

> AI 语义算子与多模态任务的物理执行、数据组织与调度研究

统一研究对象是语义 API、数据库等前端交付的 AI 任务物理执行与数据组织：

```text
Semantic API / database AI operator
  -> frontend-owned semantic plan / input identity / lifecycle
  -> physical data execution layer
       -> research content 1: work-unit construction and organization
       -> research content 2: state-aware admission, routing and multi-job
       -> shared cost estimator
            -> stage/service/remaining work
            -> SLO slack and uncertainty
  -> Model Service / GPU Executor
  -> Frontend result / Database / Vector Sink
```

对外口径：AI 语义算子与多模态数据任务的物理执行、数据组织与调度优化；PostgreSQL 是其中一种接入。

未来若恢复 PostgreSQL 路径，由 extension 先提供 SQL/planner/query-lifecycle seam；若目标 LOTUS/Cortex rewrite、plan identity
或 lifecycle 出现可复现阻断，允许维护 `REL_18_3` 最小 core semantic patch。默认语义由中立
plan/task/result 协议定义，LOTUS v1.2.4 不再是核心依赖。Daft、Ray、vLLM、pgvector 和 CLIP
是物理执行与验证平台，不是贡献名称。允许为具体编排假说调整 vLLM、Ray 的调度、缓存和 GPU 执行接点；
原生能力、上游修改与 SemLoom 策略分别比较，采用规则见[后端修改比较](experiments/plans/reference/对照规范.md#backend-change-comparison)。
不以改后端的代码量为贡献，也不将当前课题改成通用模型结构或传统 GPU 查询算子研究。

## 2. 研究内容

### 2.1 研究内容一：workload-aware work-unit 构造

研究同一逻辑任务的输入表示、阶段工作量、准备位置与后继消费者如何共同决定批次和留存。
物理阶段图记录实际工作单元和数据交接；候选包括相容表示共享、准备／传输／模型批次分别组织、
前缀与 GPU 执行形态感知的数据组织，以及有依据的字段／提示布局选择。具体机制和采用条件归方法文档。
`WorkDescriptor` 描述 source/prepare/model/result work、局部性、校准签名与不确定性；
需要实测信息获取成本和实际动作，不能把字段存在当作成本自适应。
原生 tensor 路径首先检验重复准备／计算、表示膨胀、复制和消费等待；
HTTP 路径只能利用外部可见的请求及服务信息，不能假定持有内部 tensor／KV。

长度或工作量分组、准备复用与局部性选择都是候选，先与合理原生及同信息静态路径比较。
B表示每批工作量目标，文本 token 模式以累计 token 决定成员与封批边界，替代固定行数封批，缓解各批工作量差异。
同一查询的合法阶段内应跨读取块继续累积；仍先固定64行、再只在块内拆小，不代表已经实现该组批目标。
B与并发C、在途保护W分别配置；当前记录链的接入与资格见[代码状态](code/INFRA_STATUS.md#finite-query-token-candidate)。
以完整任务时间、有效结果吞吐及适用的质量为主要结果，准备次数、实际字节、阶段 work、
峰值留存和 GPU 供给用于解释；没有净收益时保留静态方案。

### 2.2 研究内容二：容量感知的提交、路由与多作业调度

研究已合法就绪的阶段何时推进、取得多少执行机会及使用哪个兼容实例；CPU 准备、模型供给、
结果消费和表示持有共同影响选择。动态供给是独立研究问题，代价估计提供信息；
它可以与数据组织分别验证，再检验联合收益，不以图片复用或依赖细化成功为前提。

<a id="dynamic-resource-feedback-goal"></a>
动态扫描以寻找**当前可利用资源下的性能平台**为目标：改变供给窗口时，观察有效吞吐、完整查询时间、可比请求的 P95／P99 尾延迟，
以及 GPU 供给／忙闲、CPU 准备速率与每核利用率、队列增长、KV／显存、主机内存、传输及抢占等响应，再反馈到后续探索。
C的计数单位须随路径明示为在途请求或批；扫描仍应包含资源允许的较高并发，不能由 token 组批B取代，也不预设64为平台。
模型算力利用率（MFU）须有可信工作量、时间分母及对应精度峰值，不能用 GPU 利用率代替；缺少的信号标未知，采集成本计入。
性能相近时较小并发可能降低占用，但持续上涨、选择最小或次小并发只是候选做法，不规定唯一算法、单指标或固定权重／阈值。
新增信号及其解释先记为假设，须由可比观察验证；扫描与[物理阶段的资源观察及编排](experiments/plans/系统架构.md#physical-stage-research-goal)互补，不能相互替代。

旧具名实现的10秒探索、未提交数≤64及与引擎配置耦合的外层C64，是[旧实现和实验的工程条件](code/src/scheduling/runtime/calibration.README.md#原实验配置选项)，
并非研究需求规定的停止规则。历史结果及负结果保留；尚无可比平台证据时，因预算、任务结束、样本不足或声明容量停止，应报告平台未确认，不能据此宣布最优。
真实内存／KV／IPC／结果责任、超时和取消保护仍须成立；外层尚未完成请求数与引擎活跃序列容量分别解释。
B可调及B/C联动属于后续研究；现有普通 records 动态入口扫描C，不表示已实现B扫描或联动。
W是可选在途保护，当前链默认关闭；它不决定批次边界，亦不同于引擎内部的`max_num_seqs`和`max_num_batched_tokens`，计量与入口见[配置总览](docs/执行配置与优化开关.md#budgets)。
现有轨迹与计数不证明多指标反馈闭环已实现，实际默认、可选入口及验证范围只从[代码状态](code/INFRA_STATUS.md)与[配置总览](docs/执行配置与优化开关.md#defaults)核对。

先在单查询／单批次内核对连续供给、完成回收与计算／存储责任，再按实际需要研究多卡负载与服务路由。
既有 request/work credit 和有界 session 是执行基础；数据是否就绪、实际执行何时结束与逻辑接纳分开观测。
Ray Data 和 Daft 保留自己的批次、actor 池、背压和调度，不注入 SemLoom coordinator 构造弱对照。

多查询扩展继续限定单租户多 Job，按各计划比较完整完成、服务分配和隔离；
FIFO、DRR／VTC-style 在项目 coordinator 内的实现属于内部对照，不能冒充上游原生算法。
SAOR、bounded-ready、动态 K 及旧公平性观察按[历史证据](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)
和[专项审计](docs/research/saor_model_scenario_audit_20260811.md)解释，不是当前隐含待办。
多租户身份／份额及 KV 迁移仍需独立需求与验证。

### 2.3 共同使能组件：算子代价估计

首版采用解析 work 特征、profile 校准和 residual correction，预测文本/图像的 stage work、operator service、JCT、remaining work 与 SLO slack。它同时服务于 active-work 初始化、`WorkDescriptor`/组织预算、路由、提交与多 job，不单列为第三项研究内容。

评价 MAE/MAPE 之外的候选配置 ranking、pairwise accuracy、selection regret、最坏 context 与预测区间；平均误差好不能替代决策质量。

### 2.4 多模态泛化

文本 `AI_COMPLETE` 和图像 `AI_EMBED/AI_CLASSIFY` 保留各自真实接入与语义；研究优先级由实际瓶颈决定。
公共策略消费阶段工作量、可见状态和完成事件，模态 adapter 解释输入表示及可用动作；不支持的能力显式声明。
HSE（Heterogeneous Staged Execution，异构分阶段执行）已有 encoded／ready 字节、work 和阶段在途额度，
底层由 Ray Core actor 执行。额度不覆盖全部源缓冲、模型或 device 分配，也不构成硬显存保证。
已有 batch／有界流水是执行底座；表示、驻留、复用和后继消费的净增量仍需匹配对照。

## 3. 系统与实验边界

PG 接入由数据库拥有 SQL、计划、快照、权限、取消与结果生命周期，外部执行层消费合法任务；
独立 producer 的验证保持原身份。调用与结果绑定见[数据库接入](experiments/plans/数据库接入.md)，
公共会话与方法推进见[增量执行](experiments/plans/增量执行.md)，职责与移植条件见[系统架构](experiments/plans/系统架构.md)。
同任务执行比较与改变调用结构的语义方法比较分别评价，质量、实际工作量、资源与完整查询时间共同解释。
应用质量以任务 gold 和原 evaluator 为主要依据；迁移严格等价、任务质量和实际工作量分别判断，见[评价口径](experiments/plans/reference/对照规范.md#quality-equivalence-work)。

## 8. 当前执行顺序

1. 先读取代码状态与对应证据，复用已接纳接口；普通文本小单元与原生 LOTUS 核对，不重复建设已完成接点。
2. 按[机制实现与诊断安排](experiments/plans/数据组织.md#gpu-implementation-starts)推进请求／GPU 批、表示／消费者、实际工作／供给三条主线；每项承担采集、分析、原型与干预验证。按[首批观察方案](experiments/plans/数据组织.md#dag-first-observation)分别取得单算子、分支汇合和真实串联的有效轨迹，
   识别准备、GPU 工作、缓存、搬运和消费特性；采集缺项和无机会反例保留，不以一题推断通用规律。
3. 只对观察支持的[编排候选](docs/research/优化方法依据.md#gpu-engine-coordination)做单独及联合比较；
   调优 fused、原生流水、缓存、同信息简单规则和阶段预算为强对照，后端补丁单列。
   实际模型先核输出／工作量与生命周期；布局变化另做质量评价，文本与图文分别取得资格。
   已停止候选的原因只从台账读取，新运行按现行实验规则另有具体计划，不继承历史额度。
4. 多卡负载、拓扑执行组、必要跨卡传输与多 Job 共享由真实瓶颈决定；训练、KV 迁移和多租户为后续适用问题。

合理原生批执行、actor 池、背压与 CPU/GPU 流水是共同起点，不以技术栈组合本身主张创新。
原生静态或同信息参照已经足够时保留简单方案；研究反证与最近邻见[研究定位](docs/research/研究定位.md)。

## 历史范围

开题、SAOR、旧默认配置及逐次实验摘要按原日期解释；原结果、负结果、配置和复现材料仍由原目录保存。
旧大纲的研究问题表、实验叙事与章节可从[精简前固定版本](https://github.com/3444374/ai-operator-execution-optimization/blob/0b8aca5ff599e1b6525b77608e3865b20a4fd996/PROJECT_OUTLINE.md)回查。
