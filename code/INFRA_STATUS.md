# SemLoom 实现状态

本页维护 main 已接纳能力与尚未验证的范围。源码与接口从[代码入口](README.md)核对，
逐次运行、源码身份、校验值和失败由[证据台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)与主报告维护。
Git 接纳、CPU 接线、模型功能、质量及性能分别判断。

records Core 容量纠偏候选将应用层请求额度与 vLLM 每轮序列上限分开；
显式 async／反馈 owner 继续按调用方 `max_active_batches` 分配原有 Core、actor 与 IPC 额度，
`max_num_seqs` 保持原引擎配置，普通同步默认不变。CPU 检查覆盖高于引擎每轮
序列上限的外层请求、真实 CPU IPC、取消与已知 usage 回收；没有 GPU 或性能资格。
完整记录链的 record-local 分支也经此共享 resolver 检查；其 B1、方法／角色、行／字节及
生命周期检查仍在，fused 默认不变。不代表资源平台已找到；[接口边界](src/execution_provider/adapters/README.md#records-core-可选动态接纳)以实际路径区分。
同一候选继续取消显式动态配置隐含的10秒、剩余32／64条与8次调整截止，默认改为
`exploration_s=None, min_remaining=0, max_changes=None`；原 sync/fixed 默认不变。
两具名配置明确为 legacy 参数对照，正值仍可显式设置；历史源码投影、旧失败与负结果保留。
缺失 pending 反馈不作结束证明；短查询即使未 probe 也标证据不足。这只清理 C 扫描的
无依据截止，没有接入 CPU／GPU 联合资源决策或 B 扫描；当前验证仍限 CPU，不能转授GPU资格。

[PR73](https://github.com/3444374/ai-operator-execution-optimization/pull/73)已合入 main `a421c9d4`，将固定四配置的完整记录查询检查入口纳入Git；机器配置、额度、输入与结果仍在仓外。
复用现有监督函数，不迁入历史原件或改变生产算法／默认。
[入口与私有配置边界](scripts/README.md#通过-git-同步完整记录查询检查)、21项CPU检查及独审不代替当前源码的GPU验收。
原W129零调用失败保留；修复改用排序后最小两项之和减1，并将count与token资格分开，迁移未改生产算法或默认。
main0689底座配合已审现场验收入口完成四臂各48次；两token臂W134均有42个不同调用阻塞后恢复，后继竞争未触发。
W134是刻意触发阻塞／恢复的功能测试值，不是每批 token 目标或推荐运行值；常规限流应为按实际容量确定的宽松安全护栏，防止下游过量排队。
上述交付时Git入口组合只有CPU回归；`check`不加载真实tokenizer。2026-10-29后续当前源码的模型功能复验及严格输出／性能边界见[配置总览的证据索引](../docs/执行配置与优化开关.md#e1)，不将小功能检查推广为稳定性能。

[PR75](https://github.com/3444374/ai-operator-execution-optimization/pull/75)已合入的[同容量检查](scripts/README.md#matched-record-chain-execution)固定fused／逐记录两臂C1、B1和每查询一个新模型，
保留默认四配置、fused容量限制和真实Ray归属；增加owner阶段及worker的Ray启停计时，并拒绝混用资源／模型生命周期的回执。
CPU检查覆盖完整F→M的相同结果／父调用和每查询一次模型创建；模型与Ray RPC为替身，逐记录子进程IPC实际运行。
这不增加跨查询模型复用或纯粒度／性能资格；新增对照尚未部署或GPU验证。

<a id="pr52-g1-qualified-scope"></a>
## Records、有限链与本地模型

[PR52](https://github.com/3444374/ai-operator-execution-optimization/pull/52)已接纳 records/Core 方法底座、
Native／Ray／Daft／IMLane 薄适配、Movie runner、整批有限链及文本本地模型接点。
通用 Filter 要求显式非空 instruction；本地 records 格式器与公共文本引擎拒绝图像输入。
CSV／本地 records 按所选环境能力预检，只有 database／sql-readers 需要数据库配置与实际连接，见[运行手册](../deploy/runtime/README.md)。

模型实测仍绑定 `45d184dfaf0ef3a85ccccc805be7fc38e646b527`，最终 e2bc 来源检查不转授新源码推理资格。
Native resident、Core 同步／显式 async、Ray／Daft 及 Kalypso、IMLane 入口分别按
[G1](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#pr52-g1-negative-records)、
[G3](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-g3-small-qualification)与
[短 RPC 补测](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-imlane-rpc-qualification)核对；旧失败与 unknown 保留。
这些小样只覆盖两条 eligible 负例，非空有限链、helper cascade、正例质量、完整正式 CLI/sealed packet 与稳定性能仍须另证。
[CPU 资产绑定](tests/records_cpu.md)、[具名历史复现](../experiments/reproduction/native_baselines/README.md)和
[e2bc 零模型部署来源](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-e2bc-v0)分别保存接线与来源要求。

其他尝试做到了哪一步、是否正在推进及下一步，统一见下方[优化与观测进展](#优化与观测进展2026-10-27)。

<a id="record-results-migration"></a>
## 共享链结果模块

[PR56](https://github.com/3444374/ai-operator-execution-optimization/pull/56)已将结果身份实现归入
[方法公共模块](src/semantic_methods/lotus/README.md)，旧 import 保留同对象重导出，旧 pickle 可读。
worker 须同时部署新模块和兼容层并冷启动，不混用旧源码。CPU 迁移与历史投影修复不增加模型资格；
逐次检查与来源路由见台账和具名复现入口。

<a id="本地目录与依赖整理尚未发布"></a>
## 目录与依赖整理（PR62、PR64）

[PR62](https://github.com/3444374/ai-operator-execution-optimization/pull/62)已合入公共进程工具、同步模型进程移动和运行依赖整理：进程检查与退出工具在
[`infrastructure/owned_processes.py`](src/infrastructure/owned_processes.py)，同步模型进程及阶段清理在
[`serving/backends/vllm_sync_process.py`](src/serving/backends/vllm_sync_process.py)和
[`stage_process_cleanup.py`](src/serving/backends/stage_process_cleanup.py)。旧导入、旧同步进程命令和已有错误处理保留。

运行所需的记录、计时与请求身份已移到`observability/`，纯请求预算类型移到`infrastructure/request_budget.py`；
17处运行代码对实验实现的依赖已清除。仍有3处明确的旧比较入口兼容导入，不属于默认生产执行。
五份历史复现继续生成原有文件与摘要，不用新实现替换旧实验源码。

PR62整合版本已通过239项CPU检查和4次取消对照；原先受目录权限影响的12项检查也已在原位置通过。
这些检查没有调用模型或GPU，不代替真实查询与性能验证。

[PR64](https://github.com/3444374/ai-operator-execution-optimization/pull/64)已将逐阶段模型进程移到
[`serving/backends/staged_generation_process.py`](src/serving/backends/staged_generation_process.py)，共享链执行移到
[`execution_provider/method_workers/lotus_chain_batch.py`](src/execution_provider/method_workers/lotus_chain_batch.py)。Core、Ray和Daft共用该实现，旧路径保留兼容入口。
<a id="shared-chain-ipc-scope"></a>
冻结组合`baa45eb7`的[共享链进程通信测试](tests/serving/test_shared_chain_migration.py)已在Linux单项通过：测试0.447秒，含清理1.253秒，自有进程退出、租约释放，零失败／跳过、零模型、零重试。
PR67整合`563bb65b`复用相同同步IPC组件字节，并通过定向CPU组合检查；这不是新整合整体的Linux执行回执。Mac EPERM原失败保留，不再将该同步组件列为待测缺口。
逐记录查询的接纳与验证范围见[下方说明](#record-local-sections-c827)。

本地清理竞态修正让空闲stop／deadline先通过健康IPC取得真实API关闭回执，再禁止复用；已损坏的通道仍保留unknown及进程清理责任。原先空闲reaper先标记broken会跳过交握，该顺序在beb和b0均可复现。定序Core与同步进程CPU测试覆盖成功前缀、实际关闭回执及坏通道unknown；不据此增加GPU退出或性能资格。

<a id="候选交接状态2026-10-27"></a>
## 优化与观测进展（2026-10-27）

研究目标见[公共物理阶段描述与实测](../experiments/plans/系统架构.md#physical-stage-research-goal)。
下表区分已在 main 的基础、单独保存的尝试及未完成工作；候选标识只用于追溯对应检查，不表示已合入 main。
候选行依据本轮实现、独立检查及运行准备的进展回报；没有实际执行回执时，不填写真实查询或资源结果。
未默认启用、尚未合入或一次对照无净收益，分别表示采用或验证范围，不表示优化方向被淘汰。
固定历史源码用于恢复当时版本；算法继续完善与该版本保持不变分别记录。是否正在运行与是否仍待开发也分开判断。

| 尝试或检查 | 已做／未做 | 是否正在推进 | 依据 | 下一步 |
|---|---|---|---|---|
| 已有图像与文本执行基础 | main 已有图像分阶段执行（HSE）、完整矩阵收集、records 文本记录及整批有限链；不同路径的模型结果分别解释 | 已有实现；后续改进按下列项判断 | [图像执行](src/modalities/image/staged_execution.py)、[矩阵收集](src/semantic_methods/lotus/image_backend.py)、上方 records 说明及下方各路径报告 | 复用已支持的行为；完整矩阵和必要结果不能因省内存被丢弃 |
| 动态参数扫描、查询控制与参数缓存 | 保留`IncreasingSearch`、完整矩阵和公共校准API；已交付工作量扫描与计算工作量／局部配对两种原实现及原参数选项均已接main，普通Movie与session共享显式入口，默认固定。已完成CFDC/main43与ee39/main0689的原参数GPU单轮n1首次／复用 | 两组均保留输出ID差异；当前n1不与历史源码n3合并，不宣称严格等价或稳定收益 | [原配置与验收条件](src/scheduling/runtime/calibration.README.md#原实验配置选项)、[旧短查询实测边界](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-dynamic-sad24-621) | 与历史源码、模型、输入、生成、预热、缓存和计时逐项对应；明显差异须诊断，不按期望成绩调参。旧负结果保留，截断搜索不当最优 |
| 逐请求参数、先到先处理与 token 工作量限制 | `49d`已验证W阻塞和完成后恢复；PR63已含预算、FIFO和actor组件。PR71已将同角色完整F/M查询接入显式`token_budget`，Extract和角色切换仍拒绝token模式；C64输出差异和无净收益结果保留 | 已合入main；本轮两token臂各48次成功，W阻塞／恢复均42次 | [本地候选范围](#finite-query-token-candidate)、[原对照及停止原因](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#token-c64-comparability-gate) | 原接口已合并；本轮W功能通过但输出ID不完全相同，默认count保持，Extract的token支持仍待做，不将CPU结果写成模型质量或性能收益 |
| 逐记录有限查询与 Filter／Map 局部归并 | PR63已合入main，允许显式选择`execution="record_local_sections"`；默认fused保持。原c827有17次正常成功及1次取消unknown；本轮main0689底座四配置各48次正常调用完成，失败／取消与性能另计 | 可选功能已合并；性能改善仍待验证 | [PR63及验证边界](https://github.com/3444374/ai-operator-execution-optimization/pull/63) | 后续优化在当前接口上逐项检查；不把功能成功或API重叠当作稳定加速 |
| 减少结果审计对象存活时间 | [PR67](https://github.com/3444374/ai-operator-execution-optimization/pull/67)已合入；Core fused、Ray／Daft有限链Python入口及各Extract阶段支持同一`release_completed_audit`选项，默认False；普通Movie单Filter CLI不经过此worker，record_local_sections明确拒绝开启。成功编码且无失败、取消、unknown或清理错误时只撤掉worker自身列表引用，消费者账本保持 | 可选实现与组合检查完成；不证明RSS或查询收益 | [选项与边界](src/semantic_methods/lotus/README.md#completed-chain-audit-lifetime)、[IPC组件检查](#shared-chain-ipc-scope) | 按真实持有与消费者行为评估，区分Python引用、物理内存和查询性能；不自动开启 |
| 共享链结果关联索引 | 原审七字段完整身份索引由Core fused、Ray／Daft有限链共用，固定、随机差分、碰撞及取消检查通过；普通Movie单Filter和record_local_sections不适用 | 实现与组合检查完成；真实查询收益仍待验证 | [范围与测试](src/semantic_methods/lotus/README.md#finite-chain-result-index)、[共享worker](src/execution_provider/method_workers/lotus_chain_batch.py) | 保留原异常、顺序和身份规则；关联操作减少不代表完整查询收益 |
| 图像准备分离、字节背压与多工作进程 | main的HSE底层已有独立准备／模型额度、字节broker、ready优先提交；`hse_static`诊断入口仍在。普通LOTUS资源入口拒绝HSE和CPU>1，执行器将worker及阶段深度固定为1 | 已有实现，确有普通入口缺口 | [HSE循环](src/modalities/image/staged_execution.py)、[资源解析](src/modalities/image/resource_budget.py)、[LOTUS图像入口](src/semantic_methods/lotus/image_backend.py) | 动态接入后先恢复单GPU、多CPU准备的显式参数与转发，补CPU及输出回归；保留当前TF32和单GPU检查，不默认开启、不声称多卡或位置优化已验证 |
| 可选图像阶段dispatcher | `StageModelDispatcher`及HSE接入仍在`reference/optional-baselines-image-runtime`，main尚无这两个模块；是可继续维护的可选实现，未成为位置／拷贝代价优化器 | 待按小范围接入核对，不整支合并 | [阶段选择器](https://github.com/3444374/ai-operator-execution-optimization/blob/e9a54e3e43ef9ce9e3b6973530715655028b1da5/code/src/scheduling/runtime/stage_dispatch.py)、[HSE接入](https://github.com/3444374/ai-operator-execution-optimization/blob/e9a54e3e43ef9ce9e3b6973530715655028b1da5/code/src/modalities/image/hse_dispatch.py) | 单独适配显式配置、容量和速率过期合同，保持默认与原生baseline边界；不因为保存在参考分支就淘汰 |
| 私有记录方式的对照 | 原`6a263`准备记录不作为当前运行状态；已有`464`／`416`外层观察，未证明硬件瓶颈 | 已有观察；本页不表示GPU正在运行 | [既有对照的解释边界](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#record-layout-comparison-interpretation) | 复用已有事件，按阶段方案补缺失证据；启动布局、并发与实际工作量不同须单列 |
| 小规模真实事件观测 | 已完成 5 次调用、取得 56 条事件；这不是完整画像，也没有提供新的优化收益结论 | 否，本次观测已完成 | 已有 5 调用／56 事件回报 | 复用已有事件，按实际缺失项补充，不把调用成功当成瓶颈已定位 |
| 字节限制与阶段预算组合对照 | 14 项 CPU 检查仅覆盖参考实现，尚未使用真实执行接口 | 否，当前为参考实现 | 本轮 14 项 CPU 检查回报 | 公共阶段具备真实接口后，按同一查询验证这一简单对照，不能把模拟结果当原生系统结果 |
| 普通输入准备 | 已有`TaskPreparation`接口，普通准备尚未使用它；静态／原生路径的准备批次与Core显式async仍在，Core的B1限制不能只删检查就解除 | 已有接口、实际准备接入仍缺；不是配置被废弃 | [接口定义](src/scheduling/core/session_contract.py)、[Core入口](src/execution_provider/adapters/lotus_records_core.py) | 在已有输入／结果身份和准备输出容量合同上实现，核取消、持有和归还；本轮不把大批次或物理释放写成已支持 |
| 最终结果消费的字节预算 | 执行程序仍积累 `raw` 结果，尚未把消费期间持有的数据纳入统一字节预算 | 否，当前缺项 | 本轮完整查询与内存检查回报 | 记录真实消费者和最后使用时刻，纳入字节预算并验证慢消费；模型完成不能代替结果已消费 |
| 公共物理阶段描述与实测 | 已有外层事件和资源观察，公共阶段描述与完整时间线尚未齐备；画像只作支撑 | 仍需补齐阶段观测，不是已经定位硬件瓶颈 | [架构中的目标](../experiments/plans/系统架构.md#physical-stage-research-goal)、[观测方案](../experiments/plans/数据组织.md#dag-resource-observation) | 先选一个完整查询补实际缺项，不预设瓶颈或自动向别的链迁移 |
| 后继优先、反序与等待老化 | PR72已恢复9231f088原driver及ready时钟修正，并透传完整F/M查询选项。原两轮约10%变慢及输出差异保留 | 已合入main；两后继臂各48次正常完成，均无竞争选择 | [本地候选](#finite-query-successor-candidate)、[原对照与负结果](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#priority-reverse-small-pair) | 功能正常不等于优先效果；本轮未触发争用，轮转/FIFO各自默认保持，不把当前fresh-query接口称为历史resident first/reuse复现 |
| 跨模型、任意依赖图、预加载、多 GPU 与查询时限策略 | 本轮暂缓，不作为以上工作的前置项 | 否，暂缓 | 当前研究范围 | 先完成单卡完整查询和公共阶段实测，再按实际问题决定是否需要扩展 |

## main 与集成分支的阅读范围

| 已接纳能力 | 接口与资格来源 |
|---|---|
| Core 推进、完整 HTTP 响应、取消／unknown 与结果租约 | [adapter 接口](src/execution_provider/adapters/README.md)、[CPU／真实 Ray 基础检查](../experiments/results/diagnostics/main_foundation_20261015/README.md) |
| LOTUS 原 Movie Q1/Q2/Q3 四臂完整请求 | [LOTUS 模块](src/semantic_methods/lotus/README.md)、[文本接线](../experiments/results/diagnostics/main_lotus_text_20261015/README.md) |
| Animals 原图 Q1 四臂与观测模式 | [图文接线](../experiments/results/diagnostics/main_lotus_animals_20261015/README.md)、[固定 main 模型与连续查询](../experiments/results/diagnostics/main_queries_20261015/README.md)；保留质量不足及 RSS 待归因 |
| Animals 显式 Q7 与共享图像表示 | [PR68](https://github.com/3444374/ai-operator-execution-optimization/pull/68)已合入；原双Filter／City交集及默认关闭的有界URI保留；[冻结来源native128正常功能与限制](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#animals-q7-shared-media-native128)。main适配已通过CPU及历史还原检查；其他执行路径、真实unknown／abort和性能对照未验证，逻辑持有不代表RSS回收 |
| LOTUS 原 RM／FAISS 建库／检索与显式单卡入口 | [CLIP 接纳与资格](../experiments/results/diagnostics/main_lotus_clip_20261015/README.md)；默认精度失败、HSE 环境缺项与多卡未验证分列 |
| 可选 vLLM 部署资源解析与身份核验 | [服务及原查询单卡复验](../experiments/results/diagnostics/main_service_auto_20261015/README.md)；不借此认证 CLIP、多卡或其他平台 |
| 公共统计与离线结果工具 | [统计定义](src/observability/README.md#repeat-mean-confidence)、[离线输入与 CPU 范围](scripts/analysis/OFFLINE_RESULTS.md) |
| PG 同步／增量 Map、受限 Filter→Map 与 query-job | [extension 支持形状](postgres/semloom_pg/README.md#supported-query-shapes)、[增量执行](README.md#当前-postgresql-文本-map-的执行流程)及对应报告；更宽 SQL 与真实匹配 Filter 校准另验 |

接口说明只解释相应路径，模型结果仍按报告固定源码、输入、质量和生命周期使用。
未列为 main 接纳的集成能力从[固定 9c59dc9b 来源](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/code/INFRA_STATUS.md)回查。

<a id="current-image-status"></a>
## 固定图像来源与可选实现

`bdae61bb` 的图像能力、默认及未验证项属于该固定实现，不能认证 main 的执行代码；
完整表及原报告链接保留在[精简前能力表](https://github.com/3444374/ai-operator-execution-optimization/blob/0b8aca5ff599e1b6525b77608e3865b20a4fd996/code/INFRA_STATUS.md#current-image-status)。
可选实现的当前归属见上表；固定来源只用于恢复当时版本，不能据此判定整个优化方向已经结束。

文本 Map 的 worker／gateway 复用、Arrow、准备分段、尾块追加与计数诊断已有独立结果；
默认与接口见[诊断选项](README.md#文本map诊断选项)，完整失败与重复从原报告读取；既有单轮诊断结束不表示同类优化方向被淘汰。
2026-10-08 及更早的实现流水、历史调度默认与完成度表见[精简前状态](https://github.com/3444374/ai-operator-execution-optimization/blob/0b8aca5ff599e1b6525b77608e3865b20a4fd996/code/INFRA_STATUS.md#原-main-实现记录)。

<a id="record-local-sections-c827"></a>
## 单模型完整查询的可选逐记录入口

[PR63](https://github.com/3444374/ai-operator-execution-optimization/pull/63)已合入显式可选的逐记录查询入口。
实现来源为`c82714bb`运行快照；快照不是Git提交。`SemLoomRecordChainCore`显式
`execution="record_local_sections"`使用一个prepared Async model owner跨合法F/M段和完整Extract；
默认fused保持，prepare actor为0，B1和请求数C显式给定，不顺带启用token准入或优先级。
同模型有限组合的CPU验收为144个不同测试／232子用例，默认37份计划／150检查点相同；
这些数量属于原候选，不与新检查相加。PR63所用组合源码`35a0e292`的432个运行文件
已通过226个不同CPU测试／261子用例；该组合不是原c827实机源码，不改写原实机身份。

实机只覆盖3行M→E→M→F的12成功及3行F→M的3F+2M成功；同查询一个model UID，
实际token／generation／TaskKey及原方法回放通过，资源已清理。补测第0行Map在另一行Filter完成前
有88.156ms模型API区间重叠；这是功能时序，不是kernel并行或净收益。
另一个新owner的取消项保留1次unknown，只证明IPC queued后触发取消，未证明原生运行中abort完成。
没有独立gold、任意链质量、跨查询模型复用、RSS/KV物理上限、可信P99或稳定加速资格。
原始计时、资源采样及局限见[证据身份](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#record-local-query-owner-c827)。

<a id="record-local-completion-progression"></a>
### 同异步后端的可选完成推进

[PR74](https://github.com/3444374/ai-operator-execution-optimization/pull/74)已在既有逐记录异步owner上接入可选
`progression="record"`（默认）和`"batch_stage"`／显式`progression_batch_rows`。
后者按接纳输入顺序固定有限组，每阶段完成后放行合法后继；末尾EOF封组，过滤终结不等待后继。
原单记录请求、方法转换、Core容量、取消与unknown结清不变，已确认回执及时释放，组内状态仍计费。
首版限同模型同角色完整F/M、请求计数和FIFO，组必须同时容纳于行数与字节grant；不实现token组批或调整W。
原默认fused及Extract关系屏障保留；Ray继续由调用环境持有，该batch-stage组合每查询模型仍新建并关闭，不提供跨查询借用。独立rolling resident候选见[模型寿命范围](#model-query-reuse-scope)。
CPU检查与差分范围见[接口与测试](src/execution_provider/adapters/README.md#optional-completion-progression-on-the-same-async-backend)；
GPU／质量资格仍按各固定源码的报告解释；新main组合尚未GPU复测，不作完整质量或启动提速／稳定加速结论。该组屏障为内部机制消融，不替代原生完整对照。
PR74同时提供[同一Git入口的双策略套件](scripts/README.md#record-progression-validation)，
将策略／组大小传入真实owner，并检查实际策略、同C、held／行／字节grant和配置摘要；默认四臂保持。

<a id="finite-query-token-candidate"></a>
## 完整F/M查询的可选token在途配置

原候选基于`77fe087b`，已与main`43a463b6`本地组合并通过36项CPU检查；`SemLoomRecordChainCore(..., execution="record_local_sections", token_budget=budget)`
复用原`ChainTokenBudget`、FIFO、actor和Core账本，仅补查询owner的配置、工作量单位和容量传递。
默认`None`仍按请求数；一个模型部署及一个角色内的完整Filter/Map计划可选token在途W，Extract、角色切换及fused明确拒绝该`token_budget`组合。
这里的 `token_budget` 是在途上限 W：按每条实际完整消息的估计输入 token 加阶段输出 cap 预留。取消或unknown本身不释放W；权威终态或现有协议确认的生产者退出证明可结算预留，后者不将unknown结果改为成功。它没有决定一批成员和批次边界。
用户原目标是以每批累计 token 上限 B 代替固定行数作为主要组批条件；同步fused单阶段的范围见[跨读取块组批](#prepared-request-batching)，异步记录链固定B/C_batch的实现与实机范围见[PR84组批接口与验收](#async-record-token-batches)。
W应保留为较高的安全护栏，与B和请求数C分开；输入／输出估计不等于实际计算量或物理KV用量。接口细节见[adapter说明](src/execution_provider/adapters/README.md#chain-token-batching-boundary)。
本地126项相关CPU检查通过，含12项新检查；真实LOTUS格式器、微型tokenizer、Core和CPU子进程通信工作，Ray与模型推理为替身。
另3项历史还原检查通过，五个具名版本的完整文件摘要及原身份保持，未改历史清单。
前述CPU范围核对实际生成列、后继参数、W阻止/恢复、取消unknown、晚到终态和原count/Extract路径；本轮现场修复后的两token臂各48次正常完成，输出ID仍有差异，不作性能结论。
独审33项通过；首次集成迟到关闭失败保留，后续独立复现确认释放测试信号早于transport Future交付。测试仅等真实交付后再检查，原断言和timeout保持；生产unknown及W预留不提前清零。
[PR71](https://github.com/3444374/ai-operator-execution-optimization/pull/71)已合入main`230789c4`，默认仍关闭；本轮现场仅覆盖同部署同角色M→M→F的小功能检查，不作性能结论。接口见[adapter说明](src/execution_provider/adapters/README.md#optional-single-model-finite-record-plans)，
CPU身份及原49d负结果从[台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#finite-query-token-local-cpu)读取。

<a id="prepared-request-batching"></a>
## 跨读取块token组批（同步单阶段与异步记录链）

PR78的`c228d59d`候选保留历史身份：它仅在固定外层输入块内拆分，未完成跨读取块token组批。
[PR80](https://github.com/3444374/ai-operator-execution-optimization/pull/80)后续修正复用既有`PendingBatchBuilder`、原LOTUS方法和Core/私有引擎路径；默认`prepared_batching=None`不变。
显式token模式对一个Filter或Map按完整prompt token加阶段输出上限累计B，读取块尾不封模型批次。
token配置默认`max_rows=None`，没有每批行数上限；按B、独立逻辑字节额度或EOF封批，不以更大的固定行数替代。
读取块、B、字节额度、并发批数C和vLLM内部容量分别记录。显式正整数行帽只供历史受限配置复现；固定行模式作为对照。
旧B=17506、行帽128的测量保留，但不作为无行数上限目标验收；后续SemBench使用准确新提交和`max_rows:null`，不修改在跑配置。
Core调用数记账仅受整条查询已声明调用总量约束，不作为batch封批条件。单条超过B仍完整单独提交并标记。
准备结果复用修正让token请求只在producer按原方法准备并估算一次，worker复用消息及成本，仍核对身份、顺序、方法/部署/生成参数签名和B。
完整prepared身份与消息计入Core输入字节额度；因此字节受限组可能比旧近似计费更早封批。固定行与默认路径保持原准备行为。
准确`2212e331`已完成原MovieQ1的beb/2212同条件n2实机对照：输入、成员、参数、工作量与关闭责任通过核验，准备/计数记录工作量由各4000降为2000。Q1原检索F1均为1，但严格逐行等价未通过；观察到的计时下降不代表稳定收益、P99收益或token优于固定行，不把全部调用间隙归因于重复准备。
C仍固定1、W默认关闭的边界均未在本修正中改变；逻辑字节额度不宣称是进程RSS或GPU/KV显存上限。
同步fused路径的多阶段token链在消费源或模型预检前拒绝；该路径的跨阶段后继缓冲、反馈动态调整B/C、阻塞源定时flush仍未实现。
原固定行链与默认路径保留。同步历史实机范围按准确head区分：f3为旧行帽功能，beb为无行帽功能及MovieQ1/Q2/Q3，2212仅MovieQ1；2212 的 Q2/Q3 尚未实机验证。Q3现有COUNT为7、gold为14，不能因Q1/Q2原检索F1通过而宣称全任务质量通过。各范围与局限见[PR83验收说明](https://github.com/3444374/ai-operator-execution-optimization/pull/83)，不以新异步组合覆盖旧head的资格。
接口、逻辑字节边界及unknown责任见[所属说明](src/execution_provider/adapters/README.md#prepared-request-batching)。

<a id="async-record-token-batches"></a>
### 异步记录链token组批（PR84）

早期独立候选 `c79fcc6d` 基于 `beb5966c`，未包含PR83的同步重复准备修复。
[PR84](https://github.com/3444374/ai-operator-execution-optimization/pull/84)的准确验收头 `41ce4c24` 以 `2212e331` 为基底，接入异步核心、关闭分页修复及正常入口，已合入main；`cc3980c9`与该验收头仅两份文档不同，生产代码一致。
它复用原MethodRun/Core/actor/engine，以一次数据RPC及IPC提交一个token批，保留逐成员完成、后继推进、取消与unknown责任及Extract关系屏障。
可选范围为固定B、固定应用批并发C_batch、FIFO、record推进、W关闭；无每批行帽，R仍同时约束在途成员与resident行。
默认仍为`prepared_batching=None`、关闭；同步对照保留。原records工厂已接正常Movie `check/run`和isolated session，保留冻结任务、source及原评价器绑定，并核对实际B/C_batch/R和逐成员回执。

本地可选`rolling_refill=True`候选复用同一路径，以既有请求窗口R滚动补交token批；慢成员不再占一个整批供给槽阻塞已释放请求额度。
默认关闭，原C_batch模式保留；启用时省略`batch_concurrency`，并拒绝混入`batch_feedback`。
B只组织每次传输，批成员提交后不追加；请求、驻留、字节、ACK/unknown与Extract屏障责任保持。
正常Movie JSON和isolated session沿既有工厂进入，回执分别记录`C_request=R`与`C_batch=null`。
目前仅[CPU场景](tests/execution_provider/test_lotus_token_rolling_refill.py)与既有入口检查；真实Ray/模型/GPU及性能收益尚未验，不能借用下述历史整批成绩。

本地默认关闭的请求级动态候选在上述rolling中显式增加`core_profile.admission.kind="resource_feedback"`；
初始C_request=1，硬R仍独立，C_batch=null、B固定，复用原请求扫描、需求门控与已修复的单RPC/session反馈。
每条完成即可补位，缩容只停止新选择，已准入成员（含后端暂拒的不可拆分组）继续有界排空；原unknown、关系屏障及资源责任不变。
默认无候选次数／探索时间硬停止、不读gold或全表token参考；仍沿用性能窗口确认，未增加快速大步策略或动态B。
仅[CPU机制](tests/execution_provider/test_lotus_request_supply_feedback.py)和[正常入口](tests/experiments/test_movie_request_feedback_entry.py)验证，待独审和独立实机对照；资源收益与物理CPU/GPU余量未知。

准确`41ce4c24`的真实Ray/模型功能范围独审通过：198条提交含174成功、17项预期确认容量失败、7项取消unknown；覆盖129+1组批、后继提前推进、关系屏障、取消、关闭分页7→3→0及ACK释放。unknown不计为成功或确认失败。

Movie Q1/Q2/Q3的同步fixed128、同步token及异步token C_batch=1/2各n2，共24组、17,920次成功原生请求完成，全部物理清理与租约释放通过核对。token臂均固定B=17506、无每批行帽；异步臂另设R=259、W关闭，CB1/CB2为手动固定批并发，不是资源反馈自动选择，也不是最优推荐。
Q2/Q3各120条，固定128行和token预算17506在本轮均实际只有一批，CB2峰值仍为1；仅Q1的CB2实测到两批重叠。

本轮三任务沿用同一Movie Q1全量2000条输入的token长度预分析所设参数。已找到的早期准备脚本按`ceil(mean(prompt_tokens + output_cap) * 64)`给B；沿用的17506与`ceil((128.76 + 8) * 128)`一致，但首次64→128决策脚本尚未找到，不能将算术一致当作原始决策已溯清。
R由全量token reference按B=17506源序预分组的最大组129推导为`2 * 129 + 1 = 259`；native128／fixed128继承旧对照，现有原件未给出平台搜索依据。矩阵明确`concurrency_search=False`，属于输入统计预设的固定工程对照。

token reference生成、长度统计、预分组／R推导及此前实验均在worker开始前，未计入本轮冷时间；正常查询内仍重新执行方法准备和token计量，不以该离线reference替代。已核对的B/R准备代码未见gold标签选参证据，但未知任务零先验自适应及包含离线选参成本的总时间均未获验证。

未证明完整冷查询加速；冷计时从worker入口标记至原Movie查询结果就绪，含其后的必要启动，不含父控制器／Python bootstrap及后置评价、写证据与最终清理。启动栈、实际分组和缓存工作差异不允许只归因于推进粒度。Q1严格原生输出等价未通过；Q3各配置共同COUNT=7、gold=14，仍是质量失败。

未覆盖MMF独立完整返回payload等价、完整Core的GPU容量失败映射（GPU已测direct actor，完整映射为CPU）、四种独立读取块GPU对照、P99或资源最优；该次验收不覆盖后续rolling resident候选，动态B/C联合控制仍未实现。具体配置、耗时、CPU跳过与资产摘要限制见[公开验收摘要](https://github.com/3444374/ai-operator-execution-optimization/pull/84)；无CI通过声明，后续源码修正须另行验收。
准确参数单位、尾批行为、字节边界与测试入口见[异步组批接口](src/execution_provider/adapters/README.md#optional-async-record-token-batches)。

<a id="model-query-reuse-scope"></a>
## 模型所有者与跨查询复用范围

`e7373c5b` 尚无 token 跨查询复用；其后本地候选沿用现有 Session／Core／异步模型 owner，显式接通下表的 rolling resident 路径。当前仅有 CPU fixture 资格，真实 Ray／模型复用及查询性能仍待验收；架构目标及三类复用见[所属章节](../experiments/plans/系统架构.md#model-query-lifetime)。

| 实际入口 | 模型与查询寿命 |
|---|---|
| SemLoom Core 的 Movie 单查询 CLI | 每次调用创建自己的 owner 并在 `finally` 关闭；再次启动 CLI 不构成同模型复用 |
| 非 token Core 的 `resident` Movie Session | [`SemLoomRecordsCore.start()`](src/execution_provider/adapters/lotus_records_core.py)在 Session 查询执行前等待真实模型 ready；`evaluate()`可连续执行独立查询，最终 Session 关闭模型 |
| token 整批与默认 `isolated` RF／RD | 工厂使用 [`PreparedRecordsCore`](src/execution_provider/adapters/lotus_records_batches.py)；保持原有限查询启动及 EOF 关闭，不跨查询复用 |
| 显式 `resident` 的 token 固定滚动 RF／请求级动态 RD | 只接通 `rolling_refill=true`、`C_batch=null`；`start()`等待实际模型 ready，正常 EOF 证明查询责任／RPC 已排空后保留模型／worker／IPC，Session 最终关闭模型。查询账本、额度、组织器与控制器重新建立，Core 的数值 session ID 继续递增 |

普通非 token Core 可在同模型、引擎／生成配置及兼容数据合同下逐查询绑定受支持的 Filter 指令；这不代表任意 Map／Extract 链或模型／角色切换可共用实例。
其 `_CoreFeedback.begin()`逐查询重建／重置控制器、进度和测量；保留模型或采样 provider 不等于自动继承推荐窗口，显式调优缓存／提示另按其配置解释。
token RD 的 resident 查询也各自从 `C_request=1` 开始，不继承上一查询的候选或测量；RF 保持声明的请求容量。模型原生缓存仍可能跨查询保留，不能将其计算复用直接算作调度收益。

[`movie_records_session.check_plan()`](src/experiments/movie_records_session.py)只放行上述 rolling resident 组合；批槽反馈、admission cache 及非 rolling token resident 仍拒绝。不同模型身份或运行绑定拒绝；更换部署／生成／容量配置须创建新的 owner。执行失败、取消、提前关闭或未确认排空会关闭当前 owner 并禁止继续复用，不清零 unknown。
[复用 CPU 测试](tests/experiments/test_movie_token_resident_session.py)覆盖连续查询、不同合法 Filter 指令、空查询、账本／原生请求身份隔离、实际 RD 试探后的重置以及失败／取消和最终关闭；模型为 CPU 子进程替代，Ray 为 threaded fixture。这不是 GPU 模型常驻或加速证据，也不开放任意链、动态 B 或跨查询学习。
已测首次查询按原边界保留，不能从时间中减去估计加载成本后补成复用成绩；Movie 与历史 `802b0b50` 的口径见[对照规范](../experiments/plans/reference/对照规范.md#movie-session-query-timing)。

<a id="batch-supply-feedback"></a>
## 固定B的供给反馈（独立本地CPU候选）

基于`cc3980c9`的可选切片复用原异步token组织器、Core、actor、IPC原生统计和工作量比较器，只调整应用批并发C_batch，B保持固定。
显式`SupplyFeedbackConfig(max_batches=...)`从一批开始，默认`batch_feedback=None`不安装采样器；R、字节／IPC责任、后继和关系屏障保持。
实际就绪WorkDescriptor、方法到Core准备／供给速率、确认完成工作量与同owner/session的running/waiting/KV／抢占信息进入可解释判断；缺失／陈旧、供给不足、后端排队、资源压力、尾部／依赖等待及配置碰顶分列。
重用原在线比较及保护逻辑，不读同任务token reference或gold；配置上限不表示性能平台，在线观察和探索成本不扣除。默认computed比较缺计数时不确认收益。
反馈间断使当前比较失效并回退，只恢复被中断候选的重试资格、保留已发生次数和耗时；采样RPC取消失败时持有原引用直到终态，不逐次叠发。
供给策略默认不设时间／变化次数／候选尝试次数预算，`min_remaining=0`；旧有限预算仅在显式沿用时保留，不把64条日志保留量当作并发上限。默认规则与源码依据集中在所属说明。
Python owner／普通Core工厂及正常Movie `check/run` JSON、isolated session共享同一`batch_feedback`配置；省略或null默认关闭，仍拒绝逐请求admission/cache和resident组合。
B运行时调整或联合控制尚未接入；CPU/GPU利用率、物理可用内存和原生启动容量readback仍未知，仅有IPC回执与声明容量。
接口及CPU边界见[所属说明](src/execution_provider/adapters/README.md#optional-fixed-b-supply-feedback-local-cpu-candidate)。本轮修复与入口增量仍待独审及GPU验收，不代表资源收益、饱和点或Kalypso复现。

<a id="finite-query-successor-candidate"></a>
## 后继优先与等待老化的可选配置

原候选基于`77fe087b`，先在main`43a463b6`独立整合，再在已含PR71的main`230789c4`本地组合；通用`MethodDriver`与9231f088的driver逐字相同，包含可选观察读时异常及必需策略读时失败的处理。
`LotusChainFIFO`保留已在main的token参数，另接原ready选项；完整查询owner提供相同选择与`ready_clock`透传。
通用driver默认轮转，FIFO wrapper及查询默认FIFO；自定义选项限同部署同角色F/M计划，Extract、角色切换及fused明确拒绝该组合。
原main43a独立组合77项CPU检查通过；当前main230组合106项CPU检查通过（17项联合检查，含原token生命周期重放）。逐文件摘要与五份历史还原身份保持。
该接口支持同部署同角色F/M内单独选择或与token联合选择；Core队列中的已接纳调用不被重排，token W只限制活跃调用的估计工作量。
PR72已合入main0689；本轮两个后继配置各48次真实调用完成，但竞争选择均为0，不证明优先调度效果。新的Git验收入口组合另行版本化，不继承整树模型资格。
原两轮负结果与输出差异仍从[台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#priority-reverse-small-pair)引用；
新接口不是原resident first/reuse的实机复现，适用条件见[adapter说明](src/execution_provider/adapters/README.md#optional-method-ready-successor-selection)。


## Git 同步的资源观测入口

测试采集器和最小 on/off 工作流位于 [profiling 工具](scripts/profiling/record_observation/README.md)，
复用既有 Git/CPU 资格、原监督／额度及逐文件私有原件检查；配置、认证、数据和回执不进仓库。
CPU 探针覆盖单请求和现有 token 批 poll／成员 ACK 路径，批页重读不重复统计成员首次返回；
源码没有批 API 时明确标记该 CPU 范围不支持，事件、sink 或时钟缺失仍不能宣称完整。
设备 UUID 绑定与 native 反馈身份审计继续复用该入口；新增有界缓冲、消费阶段与有效容量回执的
范围见上述 README。该增量只有本地 CPU 验证，准确新树的 NVML/Ray/vLLM 与 on/off 开销待实机核对，
不继承旧树结果；未改变生产策略、默认或 B/C 配置。
