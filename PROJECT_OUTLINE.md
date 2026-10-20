# 项目大纲

[正式 Q3 候选](experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#formal-controller-cpu-20261020)已完成五臂配置／有限控制器的CPU接纳与来源锁；冻结851的[Native独立120次复验](experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#native-q3-revalidation-20261020)通过单臂执行资格，COUNT7／gold14的误差及旧失败保留。其余四臂和条件正式尚未启动，无正式性能成绩。

固定a503的[六个有限链模型单元](experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#finite-chain-model-20261020)已补齐Core／Ray／Daft的非空M→F→E与E→M；各源旧失败保留，helper／IMLane公开CLI及正式性能继续按独立profile验证。

本轮已补[固定a503取消交接观测的有限验收](experiments/results/diagnostics/async_cancel_close_20261019/README.md#cap1-handoff-observation-20261020)、[原独立native后继CPU](experiments/results/diagnostics/lotus_extract_relation_20261019/README.md#independent-native-successors-20261020)与[固定53d公开CLI作者CPU](experiments/results/diagnostics/imlane_chain_cpu_20261019/README.md#fixed-public-cli-author-cpu-20261019)。逻辑unknown保持不等于物理资源未退出；其他模型／级联和[正式baseline准备](experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#records-readiness-20261020)仍按各自新配置推进，尚无新版正式性能成绩。

固定来源的新增结果分别见[IMLane查询API模型检查](experiments/results/diagnostics/imlane_chain_cpu_20261019/README.md#fixed-api-model-20261019)与[Async迟到供给／取消检查](experiments/results/diagnostics/async_records_actor_20261019/README.md#model-late-cancel-20261019)。前者的有限关系链通过，后续公开selector的模型范围仍另验；后者late通过而cancel保留失败及unknown责任，均不提供性能结论。

[Daft 有限退出观察复验](experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#daft-exit-followup-20261019)在 `52216739` 的独立两调用样本通过，短暂 resource tracker 自然退出且无信号；原 Daft 失败与 MFE 负结果仍保留，其他方法与性能另验。

[独立 Map→Extract 覆盖](experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#map-extract-coverage-20261019)补齐终端 Extract 的实际模型调用：Core／Ray 小轮通过，Daft 退出资格仍有负结果。它使用独立命名和预算，原 MFE 失败及源 index 物化问题继续保留。

固定 8be 的[非 Q3 有限 MFE 模型检查](experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#finite-mfe-model-20261019)保留三后端相同负结果及零模型诊断：完整链与 Extract 模型资格均未通过，不能转授 CPU 的组合能力声明。

图像能力按固定实现 `bdae61bb` 与[能力表](code/INFRA_STATUS.md#current-image-status)核对；其他旧记录保留原 [`9c59dc9b` 来源](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/code/INFRA_STATUS.md)。
main新接纳的[CLIP必要工程闭包](experiments/results/diagnostics/main_lotus_clip_20261015/README.md)单列8834 CPU及main570严格单卡40行实机资格；其他分支接口与旧模型资格不因文档出现在main而成为main已验证能力。

维护日期：2026-10-20。系统名称为 **SemLoom**，DB-AIEL 仅表示架构层；术语见 [CONTEXT](CONTEXT.md)。
本文维护方向、研究内容与执行顺序；实现事实归[代码状态](code/INFRA_STATUS.md)，
证据身份归[台账](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)，完整数字、失败与重复归原报告。

研究对象是 AI 语义算子与多模态数据任务的物理执行、数据组织与调度；SemLoom 面向语义 API 与数据库等前端，
PostgreSQL 是其中一种接入。各前端保留自身语义与生命周期，资格按实际入口分别核对。

## 0. 当前优先级与历史记录范围

近期首先回答：**同一查询／一批数据，能否通过少准备、少搬运和少做重复计算更早得到全部必要结果？**
优先用原生可复用模型 worker、索引已存在的完整查询，从提交到完整结果计时；
启动、首次建库与退出另报，每查询原生必需的重建成本仍计入。完整任务及有效吞吐优先，GPU 利用率作为解释指标。
多请求服务分配和多 Job 共享保留为后续问题，不能代替这一首要目标。

工程优先减少可避免的表示往返、整批输入物化与重复启动；固定资源下的候选联合选择及真实消费者范围见[研究定位](docs/research/研究定位.md#representation-consumer-choice)。

LOTUS 主线保留 SemBench 任务评价与 LOTUS 原算法及优化；下一单元限定[普通／级联 Filter 的记录级执行](experiments/plans/数据组织.md#lotus-filter-data-execution)。
records→结果独立建设；旧 request 冻结接入范围作参照和选择性复用，不作为新主链的接口或重构前提。
LOTUS 目标配置以 Daft 扫描／普通关系处理＋Ray Core／Actor 为底座，自管准备、供给与回收；Core 的中立接口不要求所有路径采用该配置。
Ray Data／Daft／IMLane 各自原生执行，不经过 SemLoom Core。职责及当前请求接点与目标入口的区别见[架构](experiments/plans/系统架构.md#lotus-data-execution)。
实际模型实例纳入执行 worker／actor 的[第二层接点](experiments/plans/系统架构.md#model-engine-execution)沿 records 合同接受受支持模型输入并调用真实推理 API；分支实施与 main 接纳分开解释。
先以文本小单元取得资格，再保持 Animals 原任务验证 VLM；不等待全部 HTTP 后端／图像矩阵，HTTP 流水仍作阶段实现与参照。

独立records分支已按各自固定源码验证普通Q3的原生参照／Ray Data／静态参照、Daft关闭修正及[Core同引擎双查询](experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#core-model-qualification-20261019)。IMLane 首模型尝试的[原unknown与独立物理结清](experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#imlane-model-initialization-attempt-20261019)分列，actual／API计数不补造。这些有限模型资格与后续共享接口CPU检查分列；不转授给main、有限链、级联、IMLane、持续供给或性能结论。

本分支已接显式[Async Core／CLI](experiments/results/diagnostics/async_records_actor_20261019/README.md#root-集成公开入口与关闭回执)、[Extract后继](experiments/results/diagnostics/lotus_extract_relation_20261019/README.md)及[通用原全局级联](code/src/semantic_methods/lotus/cascade.README.md)的CPU实现与独立审核，保持各系统原生调度和原LOTUS方法。Async原parser／COUNT／evaluator的CPU对照和关闭knownusage反例已验；后续模型结果按上方固定来源解释。固定53d公开CLI的实际作者CPU执行已验，当前公开CLI模型、helper模型、未覆盖的组合及性能继续逐项验证。

研究按真实路径测量、已有原生批执行与有界流水、数据驻留／复用和多卡负载、拓扑执行组、必要跨卡传输推进；
后几项须由前面的实际瓶颈决定，训练为远期适用场景。保持合理调优的原生 Ray Data 与高效静态参照，
不把 batch、actor 池、背压、CPU/GPU 流水或技术栈组合本身称为创新，不重写 CUDA／NCCL。

| 核对对象 | 当前解释与事实入口 |
|---|---|
| 图像可用起点 | LOTUS 公共 RM 已使用 batch32 的 slow CLIP 准备与 tensor actor；PG 单图接口单独解释，见[研究定位](docs/research/研究定位.md#existing-image-execution-paths) |
| 图像基础修正 | 固定 `bdae61bb` 贯通真实身份、恢复已有HSE小优化及结果／已导出观测边界；[本轮检查](https://github.com/3444374/ai-operator-execution-optimization/blob/42ba085f80dba78937a3e2b3efb3e00685401d90/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md#foundation-restoration-20261015)保留CPU组件与GPU原值，不将小轮视为收益或新机制 |
| 图像证据 | [最终融合资格](https://github.com/3444374/ai-operator-execution-optimization/blob/42ba085f80dba78937a3e2b3efb3e00685401d90/experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md#final-integrated-qualification)保留固定 `2afcaf47` 的六路径64图及已有索引结果；[普通输入与Serve补充](https://github.com/3444374/ai-operator-execution-optimization/blob/42ba085f80dba78937a3e2b3efb3e00685401d90/experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md#native-input-and-serve)分列`460d3870`三格与`17ac2aeb`八格的28查询／39矩阵，均仅支持有限资格，不提供性能排名 |
| 编码与内存 | 普通入口按后端自动接RGB或经当前像素核对的源字节，不可复用时无损PNG1回退；历史显式开关已移除。HSE逻辑额度不覆盖RM列表／复制，也不是RSS或硬显存限，见[实现状态](code/INFRA_STATUS.md#current-image-status) |
| main 已接入路径 | [原Movie／Animals四臂实测](experiments/results/diagnostics/main_queries_20261015/README.md#long-run)分列`464de926`单次与`eb697477`连续运行，保留质量不足及RSS待归因边界；[可选服务auto的合并后复验](experiments/results/diagnostics/main_service_auto_20261015/README.md)另绑`2041da67`，仅Movie Q2／Animals Q1单卡两配置；各自来源与模型范围独立，不推稳定收益 |
| main CLIP接点 | [最小闭包与来源](experiments/results/diagnostics/main_lotus_clip_20261015/README.md)：原RM／FAISS／完整结果、单卡显式资源；8834 CPU／SDK与main570四路径八臂40行实机分列，默认精度和HSE环境失败保留 |
| 文本与其他接点 | [原 Q2 历史观察](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/experiments/results/diagnostics/native_sembench_model_dual_supplement_20261012/README.md)与[原生执行接入](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/experiments/results/postgresql/native_executor_baselines_20261012/README.md)按实际源码、质量、规模和生命周期解释 |
| 尚待补证 | 文本三条 Q2 仍按各自固定来源资格解释；图像同次 workflow 九格连续查询已有固定 `485c86a8` 观测，服务级 RM／索引跨调用复用仍未认证，见[能力表](code/INFRA_STATUS.md#current-image-status) |
| 研究候选 | 表示／中间物化、uint8 延后 FP32 扩展与批次切分仅有云端 CPU 探索；尚未采纳为本基线方法，GPU／生产收益未验证 |

逐次交付摘要已归并为以上导航；原始报告、负结果与[项目日志](PROJECT_LOG.md)保留。
精简前正文可从[固定来源总纲](https://github.com/3444374/ai-operator-execution-optimization/blob/277d8f4862c291bc2830cc6622f5a0b4f28b5c90/PROJECT_OUTLINE.md)追溯。
下文 §0.3、§5–7 以及注明历史的机制记录按其原日期／来源解释，不覆盖本节及 §8 的顺序。

### 0.1 PostgreSQL AI 语义算子实施入口

自有 `semloom_pg` 与 SemLoom 执行核心按已有真实路径继续推进；PG拥有SQL、语义、快照／权限及查询生命周期，
provider承接合法任务。当前支持范围、Filter真实校准缺口、Map增量能力与组合限制从[代码状态](code/INFRA_STATUS.md)及原报告核对，
本纲不再复制状态标识和逐项测试。独立核心／外部workload可以先验证执行机制，保持原身份，不自动取得PG资格。

参考公司demo的SQL注册、算子方法、取数／结果映射与生命周期，未来分别移植自有算子优化和执行能力；
工程参照、采用条件与自有落点由[系统架构](experiments/plans/系统架构.md#frontend-adapter-strategy)维护。
目标数据库的语义／资源责任单独验证，内网复用与外部发布／部署按授权处理。

### 0.2 数据库路径的核心研究链路

```text
SQL semantic intent
  -> PostgreSQL logical operator + reference/quality policy
  -> reference / optimized physical paths + semantic AI-work cost
  -> sealed tasks through the execution-provider seam
  -> SemLoom work organization + admission/routing/multi-Job execution
  -> PostgreSQL completion validation + relational result
```

数据库 semantic optimizer 决定**产生什么 AI work**：operator identity、reference behavior、近似授权、
模型角色、调用结构、选择率和下游关系基数。SemLoom 决定**这些已封闭 work 如何执行**：物理分组、
提交时机、在途上限、endpoint route 和多 Job 份额。前者使用 calls/tokens/model role/selectivity/quality
比较 semantic paths；后者使用 stage/service work、queue/capacity/locality 比较 execution policies。
两类 cost 通过 task work hint 与 completion telemetry 衔接，但不合并成一个模糊标量。

当前 recording、deterministic-golden 与 fixed-model paths 证明 carrier、生命周期、seam 和最小真实语义合同；
项目已具备 reference calibration 的生成、验证和 planner 消费机制，但 Filter 双路径成本比较仍需要真实
model/workload/service 的 held-out artifact，以及同一逻辑语义下可由 PostgreSQL 区分和选择的第二
physical path。当前 deterministic fixture 不提供真实可比较成本。
数据执行研究随后在固定 semantic task set 上比较，避免把“少做 work”和
“相同 work 执行更快”混为一个结论。

### 0.3 SAOR 系统对照准备记录（历史）

迁移前待执行的 SAOR 系统对照在运行前确定为五臂 PostgreSQL-source→validated-completion operator-E2E（Daft Native、Daft Native/Ray、Ray Data
native graph、project frozen-static、SAOR；共同 vLLM FCFS）。原生臂保留 framework-owned
执行，不注入项目控制；五臂均 `writeback=none`，不把 PostgreSQL sink 混入调度排名。FIFO/DRR/VTC-style/strict-priority 只保留历史项目内消融身份。官方 VTC 的 S-LoRA 同栈组与四臂跨层尝试已退役，源码与配置从
[固定提交](https://github.com/3444374/ai-operator-execution-optimization/tree/928a339334a4075fb2cf96a2dddc4727dec984fe)恢复。
官方 S-LoRA 的运行兼容性，以及跨层所需 Job identity／custom-FCFS 资格均未完成，没有运行真实模型比较。
共同外部到达使用 typed Job release；request arrival replay 不再被误写成 native baseline 的必需能力。
本轮 MFU denominator 已写入配置和证据指纹，但统一 FLOP numerator 不可用，故 MFU 不发布数值。
五臂 eager SAOR 的旧 profiler 冲突已在本地修复：只有旧 single-head bounded-priority 继续强制
request replay；bounded-ready 在完整 concrete pre-registration 门下直接消费 eager request envelopes。
截至 2026-08-19，当时尚无成功、可比较的完整五臂 rehearsal；formal 从未运行且继续禁止。
服务器曾有两次 fail-closed rehearsal：`ea4cbb3b` 对应保留 root
`saor_native_system_matched_matrix_20260819_r2/`，在 warmup 第 1 个 Project
selector-sanity cell 因 `unavailable:missing_gpu_peak_tflops` 被 MFU guard 拒绝；
`58154151` 对应保留 root `saor_native_system_matched_matrix_20260819_r3/`，在同一阶段因
`job 0 has no unique successful summary` 被 summary guard 拒绝。两次均通过
`run_saor_native_system_matched.py ... --rehearsal` 入口、无 formal authorization，服务器未发现
独立 tar archive；原 shell history 未保留逐字命令，因此这里只登记由 matrix index 证明的执行模式、
commit/root/cell/原因和预先选定的 runbook 的等价入口，不伪造历史 argv。

2026-08-21 本地 readiness 合同进一步 fail-closed：外层 runner 固定为独立 `DRIVER_PYTHON`，
`VLLM_PYTHON` 只用于子进程 source/package 重审；live endpoint 绑定 PID、进程 start time、未解析
argv0、`sys.prefix` 与实际 vLLM package path/version，并明确设置 `scheduling_policy=fcfs`。
readiness 拆为 static config、service identity、system preflight、correctness smoke 四阶段，仅四者
全部通过才置 `rehearsal_ready=true`。三份实际 config SHA、Daft/Ray upstream tag commit 与薄
adapter SHA 进入证据身份；formal 还必须绑定实际 rehearsal validation/root/archive SHA。`862d0008`
已在服务器完成一次 gateway 前的五臂 correctness smoke 与 rehearsal，证明可运行性但无法给原生臂
提供同口径 request tail/fairness；formal 未运行。

2026-08-21 的当前修订为五臂统一增加严格透传 observation-only gateway，并预先确定 T0--T4：T0 在
PostgreSQL 读取和 child/Ray 初始化前，T1 为首批 source data，T2/T3 为首请求到达/末请求完成，T4
为完整正确结果在内存中可见。Job/group JCT 与 correct throughput 使用完整系统边界，同时分列
source/execution/service span。共同 gateway 不排队、不重试、不重写、不接管原生 scheduler；只用
endpoint actual token usage 在共同积压窗口计算 P99/SLO、weighted Jain、service lag 与最长无服务。
within-run victim impact/recovery 可跨五臂比较，full-solo slowdown 仍需另跑 matched-solo control。
`93271012` 已在服务器通过四阶段 readiness、五臂 correctness smoke 与独立封存的五臂 rehearsal；
五臂 P99/SLO/Jain/lag/no-service 现均可用。单次观察中 SAOR 相对同 executor frozen-static 的 correct
throughput +31.01%、group/bulk JCT −23.70%，但 request P99 +18.70%/+24.11%、Jain −1.50%、
lag P95 +42.47%，继续呈效率—尾延迟—公平权衡。0s/5s 主矩阵缺少统一 pre/post service 样本，
victim inflation/recovery 只标 partial；full-solo 仍需独立控制。该 root 是 rehearsal，不作显著性排名，
formal 从未运行且继续禁止。

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
以完整任务时间、有效结果吞吐及适用的质量为主要结果，准备次数、实际字节、阶段 work、
峰值留存和 GPU 供给用于解释；没有净收益时保留静态方案。

### 2.2 研究内容二：容量感知的提交、路由与多作业调度

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

下图仅描述数据库接入配置；LOTUS 等路径由各自工作负载与方法提供语义，不以 SQL 输入或同一数据读取设施为前提。

```text
PostgreSQL SQL `ai_semantic.map(...)`
  -> extension CustomPath/CustomScan or conditional native semantic node
  -> explicit reference/LOTUS-like/Cortex-like path + ordinary child plan
  -> PreparedSemanticTask + bounded open/drive/close session
  -> recording | direct HTTP | SemLoom execution provider
  -> SemLoom: Shared Cost Estimator + WorkDescriptor + Organizer
  -> SemLoom: Ray actor admission / shared credit / routing
  -> text: vLLM generation
     image: typed CLIP GPU actor
  -> unified PostgreSQL / pgvector sink
```

- 写回采用 PostgreSQL + pgvector、COPY + deferred index，属于统一 correctness/E2E guardrail，不是独立研究内容。
- 数据库内算子主矩阵共享 PostgreSQL child-plan source 与中立 semantic plan/task/result 合同；正式 baseline 必须由
  被测 backend 拥有执行与调度，SemLoom 只做数据库交接、sink、质量审计和指标适配。
- 未修改 LOTUS DataConnector/`pd.read_sql` 路径保留为外部 LOTUS 完整系统 baseline，不进入数据库内算子主矩阵。
- 自行编排 actor pool、credit、inflight/backpressure 属于 SemLoom 方法或诊断；模型 workload UDF 可由原生引擎调度，按实际执行所有者与适配差异确定身份。
- `BoundedReadyWindow` 属于 SemLoom 方法：不得注入 Daft、Ray Data、vLLM 或数据库产品的原生 baseline；只在 SemLoom 内部 selector 归因消融中保持一致。
- 模型/数据下载不等于数据库 workload 已导入；必须继续执行 importer，并核对 schema、行数及每条记录恰好处理一次。
- 性能参数绑定“机器 + 模型/服务配置 + 协议 + workload 分布/规模”签名，签名变化重新校准；
  同一签名只校准一次并复用预先选定的配置。K 不是逐实验手调，也不在 SAOR formal 中在线变化。

## 4. 研究问题与因果设计

1. 单查询／一批数据中，哪些重复准备、表示扩展、复制或重复计算位于关键路径，减少它们能否改善完整任务？
2. 在保持同一语义、结果与实际资源时，怎样联合选择表示、阶段工作量、准备位置和后继消费，收益能否覆盖信息与控制成本？
3. 多GPU有真实负载或局部性差异时，驻留／复用与负载安排能否优于合理原生及静态路径，什么时候才需要拓扑组和跨卡传输？
4. 后续存在多查询竞争时，共享容量、路由与服务分配能否改善各查询完成，同时解释吞吐、干扰及隔离的取舍？

固定任务的执行实验保持逻辑调用、模型、输入与输出；减少模型工作或改变语义方法时另行评价质量与实际工作量。
主要结果使用完整任务与有效吞吐，准备次数、字节、峰值留存、服务计数及GPU时序解释机制；
单次资格、CPU探索或模拟不替代可比的完整查询结果。具体比较与缺项按[对照规范](experiments/plans/reference/对照规范.md)维护。

## 5. 当前证据等级

以下分级保留早期实验的原适用范围；其中“已证明／待验证”和旧 HSE、SAOR 后续条件均对应原来源，
不能解释为整个当前集成版本的能力清单。最新能力与未验证项只从[代码状态](code/INFRA_STATUS.md)和[证据台账](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)核对。

### 5.1 已证明

- 固定行数不是稳定 work 代理：固定 16 行批次的 work 最小/最大中位数为 474/6,793 token，相差 14.3 倍。
- 同一静态上限不是运行状态：W65K 下 high offered load 的运行内峰值 active work/MFU 约为 100%/35%，arrival-limited 约为 29%/7%；前者不是时间平均 active work。
- 当前双 4090/Qwen/vLLM 签名下，65,536 active work/endpoint 达最大已测吞吐均值的 97.80%；下一档只增 0.92%，继续增压会恶化 P99。
- 复杂动态控制不天然优于强静态点：AIMD/PID/EWMA、adaptive flush、service quantum 与多 actor 多数未满足预先规定的约 5% 改善条件。
- 数据组织策略排名受 serving regime 影响：双 endpoint 大 KV 池下策略范围约 12%；四 endpoint 小 KV 池饱和时分化约 27% 且排名反转，重排序可使 prefix hit 降至 0.06–0.07。
- 图像 matched-resource 静态执行结构有可重复收益：主报告保留约 13%–15% operator-JCT 改善；旧 45.7% 资源不匹配，不再使用。
- 多 Job 干扰已从两作业扩展到受控 `short@0s → 3 long@5s`：Project full/quarter
  single 将 quota 与竞争分离；三条原生路径全部 Job 均出现轨内退化。Project shared 相对
  static 总吞吐 +8.68%，四个 Job JCT 分别 −72.23%/−8.28%/−20.24%/−52.66%，因此在
  效率/JCT 子向量上是相对 static 的经验性 Pareto 改善；但 raw-work Jain 0.960→0.923、
  long 收益/稳定性不均，且 long1/2 相对 quarter-solo 仍慢 29%/14%。这支撑 idle borrowing
  与 fairness/SLO guard 同时存在的研究问题，不证明 dynamic 普遍胜出、份额保证或理论
  Pareto efficiency。

### 5.2 条件性

- Hybrid 代价估计在 429 个 formal 观测、20 context × 4 candidate 的 context-LOO 中取得 pooled regret 1.67%、macro 2.90%、pairwise 0.808、max regret 14.72%。最大 regret 距 15% 门仅 0.28 个百分点，属于 marginal pass。
- prefix-affinity routing 在四 endpoint/小 KV 池条件下出现约 5.9% 增量，但 endpoint consolidation 与饱和深度尚未完全隔离，不外推为普遍有效。

### 5.3 待验证

- SAOR 的 dynamic-K capacity-only 分支仍为 `not-promoted/parked-conditional`。fixed-envelope
  2-Job formal 已在 2×4090/Qwen2.5-7B 上完成 40/40 cell、0 incident、exactly-once：SAOR
  12,393 tok/s、fg JCT/P99 57.0/50.3s，在 credit 臂内前台最好；static 9,508 tok/s、fg
  JCT/P99 36.2/29.2s、SLO violation 0%，仍是更强隔离 Pareto 点。原始 validation 的 DRR/VTC
  rep2 假阴性已按 250 ms trace-resolution 规则修复；服务器完整 artifact 的 resolution-aware v2
  validation passed、credit mechanism effective 12/12，原 failed 文件只作审计历史。仍不能声称
  SAOR 胜出。当前 formal 的 `slo_weight=0`，服务状态仍
  observe-only；它验证的是 fairness-aware release，不是完整 SLO-aware controller。第一性原理
  审计确认：soft fairness release score 无法复制 static 的即时隔离；但 strict-priority 两轮 GPU
  短测达到 11,791 tok/s、fg P99 14.27s、SLO 0%，说明已知 foreground 存活信号下 release-only
  可达。`saor-v0.5` 已在运行前确定为通用有界词典序 release：显式 per-Job priority/剩余 SLO 预算，
  completion-corrected actual-work debt cap 优先阻止饥饿，无 guard 时回退 SAOR；debt-critical
  ready head 不 fit 时只为该队首建立 reclaim barrier，其余只在 fitting heads 间选择；首轮只做
  两 Job 的 $0.125W_e/0.25W_e$ 两个 cap（$W_e$ 是 endpoint work limit，不是 request K）。v0.5.1 已完成 selector/coordinator/scheduler/Ray/runner、
  timeout 清理、lossless event ledger、readiness 与两轮汇总器；事件机制门不再依赖 250 ms
  snapshot。旧 single-head 双轮 GPU development gate 没有满足容量候选的采用条件：$0.25W_e$ 第 2 轮 debt-recovery=0，两个
  cap 的 fg P99 约 49–56s、SLO violation 85%–95%。request/event 交叉验证定位 per-Job 单-head
  pull 没有把完整 Daft/Ray ready backlog 暴露给 coordinator；该失败版本状态为
  `development-run/not-promoted/not-formal-registered`。2026-08-13 已完成独立
  `saor_bounded_ready` 修订：旧
  bounded-priority 保持单-head 回归语义，新路径只预注册已经到达的具体 request，窗口由预先确定
  effective K 与 endpoint 数×W 自动派生；submission trace schema 6 分开 ready、registered、
  granted、submit/service；coordinator release-event schema 2 对 ready registration 与 grant
  统一记录 request ID 和 epoch。runner 先用 submission trace 证明 concrete-ready lifecycle 完整，
  再在 actor 同一时钟域内配对 foreground register→grant，并 fail closed 检查区间内 foreign
  fallback=0。两轮 GPU development gate 已完成：$0.125W_e$ 两轮以约 12.36K tok/s、foreground
  P99 17.58–18.15s、foreground SLO 0% 和 bulk SLO 65.8%–66.6% 通过全部门，注册 formal
  candidate；$0.25W_e$ 因 bulk 30s miss 74.4%–75.2% 两轮越界拒绝。状态为
  `development-gated/formal-registration-candidate-0125k-only`，不是 formal 胜出。post-hoc
  归因审核确认 bounded-ready 同时改变 observation/execution path 与 selector；随后已完成
  **项目内部** FIFO、DRR/WFQ、external VTC-style、strict-priority 和 proposed 的同 ready-window
  双轮归因，以及 single-head shared FIFO→bounded-ready FIFO 的 observation bridge。proposed
  相对 VTC-style 用约 4.8% 吞吐、5.2% bulk JCT 和更长 no-service interval 换取约 31.8%
  foreground P99 改善，是观测非支配折中而不是 selector 胜出；固定顺序 n=2 且未预注册
  selector non-inferiority margin，故历史结果保持 `formal_authorized=false`。现已另建位置平衡的
  Project mechanism 1+3 合同，预先确定 VTC-style 为公平参照、5% headline、吞吐/bulk JCT/SLO/
  longest-no-service 非劣与 30s empirical debt-repayment 门；首次最终 rehearsal 在 SAOR cell
  发现单 recovery 在途无法赶上 debt 产生，10/10 recovery completion 仍留下 2 个未退出 episode，
  已正确 fail closed。修正版使用 residual-aware projected-debt work budget，按活动集同时计入
  全部 own active work 与 foreign residual，并由 schema 5 raw event 离线复算。旧 `d6259f5f`
  root 因缺少逐请求固定输出上界门降为 diagnostic；最终 `63d17300` 全新六臂 root 才是有效
  rehearsal：96/96 recovery completion、15/15 repayment completed、P95 3.234s、0 unresolved，
  1,108/1,108 projection 离线一致，estimate/overshoot-bound violation 均为 0；6,144 条 request
  独立证明当前 chat template overhead 恒为 29、`fixed_output_cap=256`，且公平 service work 使用
  endpoint total token。单次 SAOR 相对 VTC-style 吞吐 +0.43%、foreground P99 +0.11%、P95
  service lag −13.15%、longest no-service +0.014%，在 feeding gate 前只构成 Pareto 候选，
  未证明胜出。独立 raw/SHA/指标复核已通过；授权 validator 已逐字段绑定 validation SHA、
  commit/root/archive/valid-rehearsal，不完整 fairness trace 分支也已 fail-closed 修复，六臂全组件
  指标已重汇总。当前一次性完整签名 direct ceiling 为 13,684.90 tok/s，SAOR
  feeding=92.898%<95%；两侧 group/manifest/运行合同/validation/archive SHA 已绑定，足以按预先确定 gate
  停止当前 formal，但缺结构化 PG/Ray clean record，不能声称稳定损失 7.10%。wrapper/formal contract 已在运行前确定为
  `locked_failed_feeding/formal_authorized=false`，当前 1+3 formal 停止。不能下调门槛、调 K/W 或
  重跑六臂追正。当前仅先做同签名 D0 direct K-only、D1 direct K+W、有界就绪 FIFO K+W 诊断
  的三臂 1+3 配对诊断，分别隔离 W envelope 与 Project plumbing；旧负判决不可被诊断结果撤销。
  代码/合同/结构化 PG-Ray-endpoint clean gate 已就绪，服务器关机故尚无新性能数据。诊断后才补
  同一 2-Job workload 的 Daft Native/Daft Ray/Ray Data native/project static/proposed
  matched comparison；
  原生臂保留自身调度且不接 Project bounded-ready，历史数据签名不完全一致即重跑；
- runtime-state-aware 请求成形、提交或路由能否超过同上限 frozen-static；
- fixed-K active-set change、burst、mixed-cost 下 ordered release 的响应时间、SLO goodput 与 tail；
- 多 job 的 5s 两作业与 1-short+3-long 四作业均已完成；仍待新 workload held-out、
  加权/SLO、公平 guard、Long→Short 与故障迁移；
- 代价模型跨时间段、新 workload 和硬件的稳定性；
- 图像 Daft built-in、Ray Data native 与 project frozen-static 的 operator-E2E/provenance
  证据已完成。现有数据把瓶颈进一步定位为 CPU prepare 与 driver/Ray submission 的组合：
  HSE static core 已显式拆出 pending-prepare、ready-block、pending-model，并以 descriptor/
  lease 做 physical-byte/work 预留；result 当前由 driver 即时审计，独立 sink queue 尚未接。
  串行流水线满足
  $X\le\min_s\mu_s$；现有 1666 image/s 与约 19K GPU-resident ceiling 的约 8.8% 比值和约
  9.6% GPU busy 同量级，说明调度/buffer 不能消灭 CPU prepare 木桶。derived-image cache、
  packed uint8/GPU normalize 与 DALI GPU/mixed preprocess 作为正交
  work-reduction 消融。仍待 static HSE GPU 对照门、packed/pinned/DALI、动态 SAOR runner、
  跨 workload 外推与小规模 sink 写回、读回和质量核对，sink 不是性能排名 blocker。
- prompt 变化感知、exact/semantic 结果复用、数据库级/模型内部增量推理已进入
  `parked-conditional` 清单；当前不实现，主路径完成后仅在真实 reuse opportunity≥10% 且扣除
  lookup/build/refresh 后 oracle 潜力≥5% 时重新激活。
- 图像 short→3×long 多作业已完成原生 Daft built-in/Ray Data 40/40 runs、30 formal group，
  Project staged descriptor + observe-only snapshot 也已完成 24/24 group；这些结果只证明各原生
  执行图内的多 Job 干扰和 Project 观测接入，不证明动态策略胜出。DuckDB bounded-output 四作业
  仅完成 128-row native gate，single controls/formal 尚未运行且不阻塞当前主线。图像 proposed
  角色已与具体算法名解耦，后续状态感知/动态调度调整只需版本化并重跑 project static/proposed。

### 5.4 不能声称

- 项目路径普遍优于 direct、DuckDB AI、Daft、Ray Data 或 vLLM 官方路径；
- sequential、length-align 或 prefix-aware 是全局最优 organizer；
- 65,536 是 vLLM 通用容量或最佳并发；
- 动态策略已经胜出；
- 图像路径提升 45.7%；
- 代价模型已经稳健解决。

## 6. 开题前统一文本 database-E2E（历史）

本节保留开题时期的结果解释，不作为后续实验配置或执行顺序。

2026-08-07 首轮三臂因 project feeding 仅为 direct 的 89.9%/91.38%，保留为 failed-feeding 历史诊断。2026-08-08 K128 replacement 的 24/24 单元、18 formal 满足预先规定的正确性、写回、身份和稳定性要求；但随后 ShareGPT bounded C32–C256 扫描证明 C32 只有已测峰值的 52.07%，故 ShareGPT 三臂性能排名降级，正式原生矩阵改用达到峰值 98.22% 的最小点 C128。

开题静态地基先完成 SQuAD short-answer 均匀控制组与 ShareGPT controlled-skew 异质组。两组均比较：

- `direct_static_sharded`；
- `duckdb_ai_static_sharded`；
- `project_frozen_static`。

统一合同：PostgreSQL source、immutable equal-row manifest、双 Qwen2.5-7B vLLM endpoint、prefix cache ON、统一 PostgreSQL sink、外部 database-E2E、质量与资源指标、1 warmup + 3 formal。

SQuAD replacement 三次 formal 均值：direct、DuckDB AI、project 的 correct rows/s 为 136.63、136.68、137.77，service tokens/s 为 40,920.72、40,955.99、41,277.95；三臂 EM/F1 接近。Project 路径的外层计时还包含更多指标采集、记录写入和结束处理，因此这些记录值只用于核对完成性与质量，不能根据不到 1% 的差异判断性能高低。

ShareGPT replacement 三次 formal 均值：direct、DuckDB AI、project 的 correct rows/s 为 11.36、2.26、17.55，service tokens/s 为 9,425.25、9,421.31、14,568.91。后续 bounded C32/C64/C128/C256 扫描为 9,454.88/14,057.93/17,834.14/18,158.19 tok/s；C128 实测达到 C256 的 98.22%，是第一个满足预先规定 97% 选择条件的并发点。C256 仅增 1.82%，却使 waiting mean=116.8、KV max=0.9996、TTFT mean=6.18s。旧 project/C32-direct=1.5457 因对照欠供给而不作方法排名。DuckDB fixed-cap 产品语义失败 4,921/6,144 行的结论仍有效。

同一 ShareGPT Chat manifest 的原生单 job 1+3 已完成：bounded C128、Daft Native、Daft Ray、Ray Data 的 service tok/s 为 17,800/17,286/16,747/3,551，四臂 CV<0.6%。Daft 两臂 waiting mean 为 783/742、KV max≈1，呈现过量提前提交；Ray Data running mean=17.3、MFU=0.112，呈现供给不足；bounded C128 位于最小饱和区。该结果只证明官方 graph 在预先选定配置下的外部压力形态，不证明项目方法胜出或某个框架内部算法有缺陷。

5s guaranteed-overlap 对照已完成：Daft Native/Ray、Ray Data 的 short JCT 相对各自 single 增加82.42%/104.84%/32.76%，只作外部观察。项目在线 replay 下 quota-only≈0，static/shared 加入 long 后 short JCT增加3.79%/8.95%；shared 提高总吞吐但 short/Jain回退。统一 eager Project 12 formal 又显示 full→half quota-only 已使short JCT+59.00%，matched half→static+long再+58.77%，matched full→shared+long+28.90%；eager shared 相对static使short JCT−48.94%、总吞吐+31.85%、long JCT−25.75%、Jain 0.894→0.972。两种到达regime方向相反，只作为“多Job管理必须感知arrival/active/drain状态、支持idle borrowing并保留SLO/fairness guard”的证据，不称动态普遍胜出；开题前不再扫offset/weight/更多Job追正。

Project all-at-t0 single-short 诊断已补齐统一 T0–T4 计时：T0 profiler E2E14.957s，T3 earliest model submit→latest completion11.354s，service14,361tok/s、MFU42.93%；Daft Native同一short T3为11.059s、14,727tok/s、MFU44.04%，差异仅约2.5%–2.7%。Daft缺准备前T0，因此完整E2E仍不排名；该结果排除了“Project模型请求路径慢6.4×”。随后Project eager多Job只补full single、half single、static+long、shared+long，不重跑原生三臂；arrival span均为66.76µs、12/12 formal通过。逐阶段显示matched static竞争使short service mean/P99 +50.34%/+78.62%，shared为+14.63%/+28.70%；submit→service仍约2ms。在线replay与eager结论分轨保留。

## 7. 开题叙事图（历史）

1. `opening_motivation_work_state`：固定行隐藏 work、静态上限不是状态、提交压力存在最小近饱和点与边际收益递减区，分别导出 WorkDescriptor、感知和有界控制。
2. `opening_ai_data_execution_boundary`：两项研究内容并列，算子代价估计作为共同使能部件。
3. `opening_work_to_schedule_overview`：组织输出 work/locality/deadline，调度结合 fresh state 消费。
4. `opening_work_organization_regime_v2`：work-aware 组织的必要性与 regime 局限。
5. `opening_image_stage_aware_evidence`：图像 prepare/model、transfer 形态和 active-window 动机，只承担 staged work 与状态感知必要性。
6. `opening_image_baseline_evidence_map`：Direct、Daft Built-in、Ray Data、vLLM Pooling、Project 的功能验证、12K 结构诊断与 120K matched-resource 正式排名范围。
7. `opening_cost_model_decision_quality_v2`：代价模型 selection regret 与最坏风险。

权威输出位于 `figures/data/report_main/` 与 `figures/architecture/`，生成脚本为 `figures/scripts/generate_opening_story_figures_20260808.py`，claim 与视觉审计见 `figures/audit/opening_story_figures_contract_20260808.md`。无同上限正式结果的 static–dynamic 示意图继续保持 `do-not-draw-no-result`。制作 PPT 或报告时统一从 `figures/opening_figure_set/` 进入：图集当前有 21 张主讲候选图、10 张 Draw.io 编辑源和 2 张备份图，权威数据与可复现源仍留在原目录。当前 PPT 成品为已独立完成 26/26 页渲染和视觉检查的 v9；2026-08-22 至 08-25 的报告图文更新没有自动回灌该 PPT，跨材料差异审查仍待执行。

## 8. 当前执行顺序

本表安排目标工作；功能分支已完成的接口和各自资格从[固定实施来源](experiments/plans/数据组织.md#model-engine-roadmap)核对，不重复列为新建任务。

| 顺序与对象 | 要回答的问题 | 参照与当前范围 |
|---|---|---|
| 原生 Ray Data 普通 Filter 首闭环 | 独立 records 主链能否保留同一 SemBench 查询和原方法结果 | 直接与原生 LOTUS 核对，旧 adapter 仅补充参照；[方法差分与真实库资格](experiments/plans/数据组织.md#lotus-filter-data-execution)先于性能 |
| 同范围原全局级联 | Q3 helper、样本、校准、阈值、回退与合并是否保留 | 在普通闭环上逐项核对原 LOTUS 动态算法，不按物理批次重跑校准 |
| Ray Data／SemLoom 静态首两臂 | Daft＋Ray Core／Actor 能否独立完成同任务，是否仍有可归属损耗 | 首切片可先行；最终对照必须补齐 Daft、Ray Data、IMLane，保留原生 LOTUS 及 SemLoom 静态／候选 |
| 并行：模型执行纳入数据链 | 常驻引擎能否在同一方法合同下消费准备输入并正确恢复结果 | [实施与验证路线](experiments/plans/数据组织.md#model-engine-roadmap)分别解释已完成接口、固定来源模型资格和剩余范围，不等待完整 HTTP 矩阵 |
| 并行：普通方法／有限链复用 | 换任务或组合 Map→Filter 能否复用方法、模型与原生接线 | [扩展路线](experiments/plans/数据组织.md#lotus-operator-extension)与原级联接线并行；已支持节点按实际组合核验，不为每条链复制执行器 |
| 真实完整路径测量 | 时间花在准备、搬运、执行还是结果消费；原生生命周期需要什么 | [数据组织](experiments/plans/数据组织.md#single-gpu-image-critical-path)与[计时规范](experiments/plans/reference/对照规范.md#resident-query-timing)；冷算子和常驻查询分列 |
| 已有原生批执行与有界流水 | 合理 batch、CPU/GPU callable 池和背压下，还剩哪些可归属损耗 | 原生 Ray Data、Direct 及已有高效静态／HSE；不重新实现这些能力作为创新 |
| 表示、驻留／复用与多卡负载 | 是否减少准备、实际复制或重复计算，是否改善整批完成 | 保留原生热缓存与生命周期；表示选择和渐进准备尚未实现，多卡净收益尚未验证 |
| 拓扑执行组 | 只有实际放置／消费信息可得时，分组是否值得 | 先验证信息与必要动作，不用 unknown locality 推导收益 |
| 必要跨卡传输 | 本地复用和负载安排仍不足时，传输是否在关键路径 | 使用已有传输设施；CUDA／NCCL 不在重写范围 |
| 后续共享与训练 | 何时存在多查询竞争或训练供给需求 | 各自定义任务与质量，不能由推理软件检查推定训练或公平性收益 |

图像复用、多卡与训练按各自任务独立验证，不作为所有现有文本路径或 CPU 接口检查的前置项。

PG 接入随实际路径核对同步语义、关联／顺序、快照／权限、取消与资源责任；
独立核心和外部 LOTUS／PZ 工作负载保持原身份，不等待无关 Filter 第二路径，也不自动取得 PG 资格。
本轮不扩 PZ、所有算子、任意 DAG、多节点或通用 compiler；IMLane 新数据函数／schema 是最终对照必需项，CPython 与作者 Ray 分列，不阻塞首组 Ray Data／SemLoom 对照。
记录级新链与旧 request 的[分支安排及实施次序](experiments/plans/数据组织.md#lotus-filter-data-execution)单独登记，不把旧请求资格视为新链已完成。
PZ 的[独立计划](experiments/plans/reference/对照规范.md#palimpzest-fixed-plan)保留，后续先复现 SemBench Movie Q3 完整过滤与 count，再固定物理计划逐一检查五组执行目标；
Q2 `LIMIT 5` 另保留，实际提交、完成及残余工作按观测记录，不由结果上限推定只有五次模型请求。
共享 GPU 按既有 lease 及 LOTUS 优先安排；文档更新不表示 PZ 接入、真实模型或五组性能比较已经完成。
公司移植按[架构计划](experiments/plans/系统架构.md#company-integration-questions)核对实际接口和授权。
原 Q2 后续工作点、常驻连续查询与其他新运行仅在其对应计划及实际结果就绪后更新状态；
不恢复旧 GPU 矩阵或沿用已结束运行额度。

## 9. 结果解释与写作规则

每个正式实验按以下顺序记录：目的、设置、合规自检、设计、全组件数据、解释、对课题含义、下一步。解释明确区分事实、推断、待确认和不能声称。

完整任务时间与有效结果吞吐优先；GPU 利用率作为解释指标，使用 time-series mean/p50/p95/max；KV usage 按 0–1 分数读取。feeding-saturation 以同协议 bounded direct 为参照；未过门的臂不抽策略性能结论。raw rows/s、correct rows/s 和 service tokens/s 不得互相替代，语义失败必须保留在总行数分母。

正式报告、论文、PPT 和图表不使用内部实验缩写。既有 PG18.4 AutoDL 结果必须按实际链路标为
rehearsal/compatibility evidence，不能冒充已经验证 `REL_18_3` planner-visible semantic operator。

## 10. 当前入口与历史材料

- 开题历史材料：`docs/archive/opening/README.md`；不再随项目进展同步
- 当前方向速览：`README.md`
- 实验证据：`experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`
- 研究问题与已有工作：[研究定位](docs/research/研究定位.md)；文献题录与版本：[文献清单](docs/research/ai_operator_literature_inventory.md)
- 十五篇精读方法速览：`docs/research/精读文献笔记/paper_deep_reading_digest/paper_deep_reading_digest.tex`（同目录本地 PDF）
- 变更日志：`PROJECT_LOG.md`

影响方向、实验结论或关键入口的修改必须同步 `PROJECT_LOG.md`、`PROJECT_INDEX.md`、根 README 和受影响目录 README。
