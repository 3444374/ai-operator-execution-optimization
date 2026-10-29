# 项目大纲

本文维护研究问题、两项研究内容与近期顺序。实现事实见[代码状态](code/INFRA_STATUS.md)，
运行身份与可支持的判断见[证据台账](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)，数字与失败见原报告。
系统名称为 SemLoom，DB-AIEL 仅表示架构层；术语见[CONTEXT.md](CONTEXT.md)。

## 0. 当前优先级与历史记录范围

近期首先回答：**同一查询／一批数据，能否通过少准备、少搬运和少做重复计算更早得到全部必要结果？**
完整任务时间与有效吞吐是主要结果，GPU 利用率用于解释供给与等待。
优先采用原生可复用模型 worker、索引已存在的完整查询；启动、首次建库与退出另报，
每查询原生必需的重建仍计入，具体计时见[对照规范](experiments/plans/reference/对照规范.md#resident-query-timing)。

LOTUS 主线由 SemBench 提供任务与评价，保留 LOTUS 原方法及全局校准。
近期研究集中于[选择性语义链的合法依赖与物理数据寿命](experiments/plans/数据组织.md#selective-chain-plan)：
固定逻辑计划、模型、prompt 与生成规则，检验依赖范围细化及有限物理重组是否改善完整查询。
普通／级联 Filter 是方法参照、无机会反例与全局依赖场景；[动态供给扫描](#dynamic-resource-feedback-goal)与[阶段资源编排](experiments/plans/系统架构.md#physical-stage-research-goal)分别验证，单纯增大并发本身不作为研究增量。
近期优先验证合法记录的后继推进；实际默认、接入与资格从[代码状态](code/INFRA_STATUS.md#record-local-sections-c827)核对。

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

PostgreSQL 路径由 extension 先提供 SQL/planner/query-lifecycle seam；若目标 LOTUS/Cortex rewrite、plan identity
或 lifecycle 出现可复现阻断，允许维护 `REL_18_3` 最小 core semantic patch。默认语义由中立
plan/task/result 合同定义，LOTUS v1.2.4 不再是核心依赖。Daft、Ray、vLLM、pgvector 和 CLIP
是物理执行与验证平台，不是贡献名称。项目不修改 vLLM continuous batching、Ray 调度器、模型结构
或 GPU kernel，也不回到传统 GPU 查询算子。

## 2. 研究内容

### 2.1 研究内容一：workload-aware work-unit 构造

研究同一逻辑任务的输入表示、阶段工作量、准备位置与后继消费者如何共同决定批次和留存。
`WorkDescriptor` 描述 source/prepare/model/result work、局部性、校准签名与不确定性；
需要实测信息获取成本和实际动作，不能把字段存在当作成本自适应。
原生 tensor 路径首先检验重复准备／计算、表示膨胀、复制和消费等待；
HTTP 路径只能利用外部可见的请求及服务信息，不能假定持有内部 tensor／KV。

长度或工作量分组、准备复用与局部性选择都是候选，先与合理原生及同信息静态路径比较。
文本组批的原目标是以每批累计 token 上限决定成员及边界，缓解固定行数下各批工作量差异；
它与防止下游积压的在途 token 安全上限分别配置，当前记录链的接入缺口见[代码状态](code/INFRA_STATUS.md#finite-query-token-candidate)。
以完整任务时间、有效结果吞吐及适用的质量为主要结果，准备次数、实际字节、阶段 work、
峰值留存和 GPU 供给用于解释；没有净收益时保留静态方案。

### 2.2 研究内容二：容量感知的提交、路由与多作业调度

<a id="dynamic-resource-feedback-goal"></a>
动态扫描以寻找**当前可利用资源下的性能平台**为目标：改变供给窗口时，观察有效吞吐、完整查询时间、可比请求的 P95／P99 尾延迟，
以及 GPU 供给／忙闲、CPU 准备速率与每核利用率、队列增长、KV／显存、主机内存、传输及抢占等响应，再反馈到后续探索。
模型算力利用率（MFU）须有可信工作量、时间分母及对应精度峰值，不能用 GPU 利用率代替；缺少的信号标未知，采集成本计入。
性能相近时较小并发可能降低占用，但持续上涨、选择最小或次小并发只是候选做法，不规定唯一算法、单指标或固定权重／阈值。
新增信号及其解释先记为假设，须由可比观察验证；扫描与[物理阶段的资源观察及编排](experiments/plans/系统架构.md#physical-stage-research-goal)互补，不能相互替代。

旧具名实现的10秒探索、未提交数≤64及与引擎配置耦合的外层C64，是[旧实现和实验的工程条件](code/src/scheduling/runtime/calibration.README.md#原实验配置选项)，
并非研究需求规定的停止规则。历史结果及负结果保留；尚无可比平台证据时，因预算、任务结束、样本不足或声明容量停止，应报告平台未确认，不能据此宣布最优。
真实内存／KV／IPC／结果责任、超时和取消保护仍须成立；外层尚未完成请求数与引擎活跃序列容量分别解释。
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
2. 按[选择性链方案](experiments/plans/数据组织.md#selective-chain-plan)验证合法推进、真实消费者与 buffer 寿命；
   调优 fused、有界 FIFO、通用字节背压和阶段预算构成强对照。局部队列改善须能解释完整查询收益。
3. 实际模型验证先核输出／工作量可比性与生命周期，再比较完整查询；文本与 Animals 图文分别取得资格。
   已停止候选的原因只从台账读取，新运行按现行实验规则另有具体计划，不继承历史额度。
4. 多卡负载、拓扑执行组、必要跨卡传输与多 Job 共享由真实瓶颈决定；训练、KV 迁移和多租户为后续适用问题。

合理原生批执行、actor 池、背压与 CPU/GPU 流水是共同起点，不以技术栈组合本身主张创新。
原生静态或同信息参照已经足够时保留简单方案；研究反证与最近邻见[研究定位](docs/research/研究定位.md)。

## 历史范围

开题、SAOR、旧默认配置及逐次实验摘要按原日期解释；原结果、负结果、配置和复现材料仍由原目录保存。
旧大纲的研究问题表、实验叙事与章节可从[精简前固定版本](https://github.com/3444374/ai-operator-execution-optimization/blob/0b8aca5ff599e1b6525b77608e3865b20a4fd996/PROJECT_OUTLINE.md)回查。
