# SemLoom 实现状态

本页维护 main 已接纳能力与尚未验证的范围。源码与接口从[代码入口](README.md)核对，
逐次运行、源码身份、校验值和失败由[证据台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)与主报告维护。
Git 接纳、CPU 接线、模型功能、质量及性能分别判断。

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
| 动态参数扫描、查询控制与参数缓存 | 保留`IncreasingSearch`、`calibrate/run_calibrated/run_query`及有限矩阵扫描；普通Movie与维护session可显式选择原单遍反馈入口和精确提示复用，默认固定。原冻结来源完成短查询功能检查，未证明反馈调节或收益 | 可选实现已整合；真实反馈、探索和调窗仍待验证 | [公共接口](src/scheduling/runtime/calibration.README.md)、[实测身份与局限](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-dynamic-sad24-621) | 按真实查询验证反馈、探索和完整成本，保留旧未回本及负结果；不把容量边界或短查询初值当作最优 |
| 逐请求参数、先到先处理与 token 工作量限制 | `49d`已验证W阻塞和完成后恢复；PR63含共用调用审计，但完整查询的阶段传递和Extract仍按请求数，token尚未贯通。C64输出可比性失败和无净收益结果保留。这不同于main已有的token分批 | 候选组件保留待接入；不能只加一个开关 | [token 对照及停止原因](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#token-c64-comparability-gate)、[PR63](https://github.com/3444374/ai-operator-execution-optimization/pull/63)、[已有token分批](src/data/materializers/text.py) | 动态接入稳定后，贯通共享预算、阶段传递、FIFO与Extract的一致工作量、取消及unknown结算，再暴露显式选项；默认count保持 |
| 逐记录有限查询与 Filter／Map 局部归并 | PR63已合入main，允许显式选择`execution="record_local_sections"`；默认fused保持。原c827有17次正常成功及1次取消unknown，组合版本未整体实机验证 | 可选功能已合并；性能改善仍待验证 | [PR63及验证边界](https://github.com/3444374/ai-operator-execution-optimization/pull/63) | 后续优化在当前接口上逐项检查；不把功能成功或API重叠当作稳定加速 |
| 减少结果审计对象存活时间 | [PR67](https://github.com/3444374/ai-operator-execution-optimization/pull/67)已合入；Core fused、Ray／Daft有限链Python入口及各Extract阶段支持同一`release_completed_audit`选项，默认False；普通Movie单Filter CLI不经过此worker，record_local_sections明确拒绝开启。成功编码且无失败、取消、unknown或清理错误时只撤掉worker自身列表引用，消费者账本保持 | 可选实现与组合检查完成；不证明RSS或查询收益 | [选项与边界](src/semantic_methods/lotus/README.md#completed-chain-audit-lifetime)、[IPC组件检查](#shared-chain-ipc-scope) | 按真实持有与消费者行为评估，区分Python引用、物理内存和查询性能；不自动开启 |
| 共享链结果关联索引 | 原审七字段完整身份索引由Core fused、Ray／Daft有限链共用，固定、随机差分、碰撞及取消检查通过；普通Movie单Filter和record_local_sections不适用 | 实现与组合检查完成；真实查询收益仍待验证 | [范围与测试](src/semantic_methods/lotus/README.md#finite-chain-result-index)、[共享worker](src/execution_provider/method_workers/lotus_chain_batch.py) | 保留原异常、顺序和身份规则；关联操作减少不代表完整查询收益 |
| 图像准备分离、字节背压与多工作进程 | main的HSE底层已有独立准备／模型额度、字节broker、ready优先提交；`hse_static`诊断入口仍在。普通LOTUS资源入口拒绝HSE和CPU>1，执行器将worker及阶段深度固定为1 | 已有实现，确有普通入口缺口 | [HSE循环](src/modalities/image/staged_execution.py)、[资源解析](src/modalities/image/resource_budget.py)、[LOTUS图像入口](src/semantic_methods/lotus/image_backend.py) | 动态接入后先恢复单GPU、多CPU准备的显式参数与转发，补CPU及输出回归；保留当前TF32和单GPU检查，不默认开启、不声称多卡或位置优化已验证 |
| 可选图像阶段dispatcher | `StageModelDispatcher`及HSE接入仍在`reference/optional-baselines-image-runtime`，main尚无这两个模块；是可继续维护的可选实现，未成为位置／拷贝代价优化器 | 待按小范围接入核对，不整支合并 | [阶段选择器](https://github.com/3444374/ai-operator-execution-optimization/blob/e9a54e3e43ef9ce9e3b6973530715655028b1da5/code/src/scheduling/runtime/stage_dispatch.py)、[HSE接入](https://github.com/3444374/ai-operator-execution-optimization/blob/e9a54e3e43ef9ce9e3b6973530715655028b1da5/code/src/modalities/image/hse_dispatch.py) | 单独适配显式配置、容量和速率过期合同，保持默认与原生baseline边界；不因为保存在参考分支就淘汰 |
| 私有记录方式的对照 | `6a263` 已通过 CPU 独立检查；真实对照正在恢复运行准备，已登录并等待确认机器现场状态，尚无执行回执 | 是：服务器运行准备 | `6a263` 独立检查及本轮运行准备回报 | 确认机器现场状态后执行限定对照并返回结果；准备状态不写成已经运行 |
| 小规模真实事件观测 | 已完成 5 次调用、取得 56 条事件；这不是完整画像，也没有提供新的优化收益结论 | 否，本次观测已完成 | 已有 5 调用／56 事件回报 | 复用已有事件，按实际缺失项补充，不把调用成功当成瓶颈已定位 |
| 字节限制与阶段预算组合对照 | 14 项 CPU 检查仅覆盖参考实现，尚未使用真实执行接口 | 否，当前为参考实现 | 本轮 14 项 CPU 检查回报 | 公共阶段具备真实接口后，按同一查询验证这一简单对照，不能把模拟结果当原生系统结果 |
| 普通输入准备 | 已有`TaskPreparation`接口，普通准备尚未使用它；静态／原生路径的准备批次与Core显式async仍在，Core的B1限制不能只删检查就解除 | 已有接口、实际准备接入仍缺；不是配置被废弃 | [接口定义](src/scheduling/core/session_contract.py)、[Core入口](src/execution_provider/adapters/lotus_records_core.py) | 在已有输入／结果身份和准备输出容量合同上实现，核取消、持有和归还；本轮不把大批次或物理释放写成已支持 |
| 最终结果消费的字节预算 | 执行程序仍积累 `raw` 结果，尚未把消费期间持有的数据纳入统一字节预算 | 否，当前缺项 | 本轮完整查询与内存检查回报 | 记录真实消费者和最后使用时刻，纳入字节预算并验证慢消费；模型完成不能代替结果已消费 |
| 公共物理阶段描述与实测 | 已明确准备、模型、消费和交接的研究目标；尚未完成公共描述和真实完整查询测量，画像只作支撑 | 是：阶段描述与测量设计 | 本轮研究目标修订；[架构中的目标](../experiments/plans/系统架构.md#physical-stage-research-goal) | 先选一个完整查询，说明阶段、数据与允许动作，测展开／计算／收缩及准备消费竞争；不以任务间迁移为主线 |
| 后继优先、反序与等待老化 | 私有原型保留`SuccessorFirst`与等待／连续选择上限，main尚未接入。两轮当前负载无收益，停止的是该失败试法的盲目扩展，不是整个方向 | 当前试法暂停，原型待适配；未安排新模型实验 | [原对照与负结果](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#priority-reverse-small-pair) | 在动态与token入口稳定后核ready选择、时钟和FIFO转交，保持原默认；保留源等待转移及输出变化，不因未收益删除实现 |
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
