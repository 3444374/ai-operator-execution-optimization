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

## 本地目录与依赖整理（尚未发布）

本地已按审查顺序整合公共进程工具、同步模型进程移动和运行依赖整理：进程检查与退出工具在
[`infrastructure/owned_processes.py`](src/infrastructure/owned_processes.py)，同步模型进程及阶段清理在
[`serving/backends/vllm_sync_process.py`](src/serving/backends/vllm_sync_process.py)和
[`stage_process_cleanup.py`](src/serving/backends/stage_process_cleanup.py)。旧导入、旧同步进程命令和已有错误处理保留。

运行所需的记录、计时与请求身份已移到`observability/`，纯请求预算类型移到`infrastructure/request_budget.py`；
17处运行代码对实验实现的依赖已清除。仍有3处明确的旧比较入口兼容导入，不属于默认生产执行。
五份历史复现继续生成原有文件与摘要，不用新实现替换旧实验源码。

整合版本已通过239项CPU检查和4次取消对照；原先受目录权限影响的12项检查也已在原位置通过。
这些检查没有调用模型或GPU，不代替真实查询与性能验证。

仍待移动的是`baselines/text/frameworks/lotus_chain_engine_process.py`和`lotus_chain_batch.py`。
本地整合可选择接入`c827`的单模型完整查询实现，默认fused保持；原实验源码不修改。小样本功能收据不证明完整查询性能收益。

<a id="候选交接状态2026-10-27"></a>
## 优化与观测进展（2026-10-27）

研究目标见[公共物理阶段描述与实测](../experiments/plans/系统架构.md#physical-stage-research-goal)。
下表区分已在 main 的基础、单独保存的尝试及未完成工作；候选标识只用于追溯对应检查，不表示已合入 main。
候选行依据本轮实现、独立检查及运行准备的进展回报；没有实际执行回执时，不填写真实查询或资源结果。

| 尝试或检查 | 已做／未做 | 是否正在推进 | 依据 | 下一步 |
|---|---|---|---|---|
| 已有图像与文本执行基础 | main 已有图像分阶段执行（HSE）、完整矩阵收集、records 文本记录及整批有限链；不同路径的模型结果分别解释 | 已有基础，不另立优化任务 | [图像执行](src/modalities/image/staged_execution.py)、[矩阵收集](src/semantic_methods/lotus/image_backend.py)、上方 records 说明及下方各路径报告 | 复用已支持的行为；完整矩阵和必要结果不能因省内存被丢弃 |
| 逐请求参数、先到先处理与 token 工作量限制 | `49d` 的独立尝试已验证工作量上限 W 会阻塞并在完成后恢复；已有对照没有证明完整查询净收益，C64 输出严格可比性检查未通过 | 否，保留已有结果 | [token 对照及停止原因](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#token-c64-comparability-gate) | 先处理输出和实际工作量的可比性，不自动扩大参数扫描 |
| 单模型有限计划与 Filter／Map 局部归并 | `f712`／`0441`／`457`及`c827`已通过CPU检查；c827两轮正常入口17次成功，另1次取消unknown保留；当前仅本地接纳候选 | 本轮接线功能已完成；代表性链性能尚未验证 | [c827实机范围](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#record-local-query-owner-c827) | 核对组合源码与默认路径后接纳，不把n=1或API重叠当作稳定加速 |
| 减少中间数据存活时间 | 内存 `r2` 默认关闭；目前只证明 Python 对象持有时间减少，未证明物理内存归还或完整查询更快 | 否，保留待评估 | `r2` 的 CPU 检查与独立检查回报 | 在公共阶段测量中区分对象存活、实际缓冲与进程内存，再决定是否启用 |
| 私有记录方式的对照 | `6a263` 已通过 CPU 独立检查；真实对照正在恢复运行准备，已登录并等待确认机器现场状态，尚无执行回执 | 是：服务器运行准备 | `6a263` 独立检查及本轮运行准备回报 | 确认机器现场状态后执行限定对照并返回结果；准备状态不写成已经运行 |
| 小规模真实事件观测 | 已完成 5 次调用、取得 56 条事件；这不是完整画像，也没有提供新的优化收益结论 | 否，本次观测已完成 | 已有 5 调用／56 事件回报 | 复用已有事件，按实际缺失项补充，不把调用成功当成瓶颈已定位 |
| 字节限制与阶段预算组合对照 | 14 项 CPU 检查仅覆盖参考实现，尚未使用真实执行接口 | 否，当前为参考实现 | 本轮 14 项 CPU 检查回报 | 公共阶段具备真实接口后，按同一查询验证这一简单对照，不能把模拟结果当原生系统结果 |
| 普通输入准备 | 已有 `TaskPreparation` 接口，普通准备路径尚未使用它 | 否，当前缺项 | [接口定义](src/scheduling/core/session_contract.py)与本轮实现回报 | 让普通准备使用已有接口，并核对取消、输入持有和完成后的归还 |
| 最终结果消费的字节预算 | 执行程序仍积累 `raw` 结果，尚未把消费期间持有的数据纳入统一字节预算 | 否，当前缺项 | 本轮完整查询与内存检查回报 | 记录真实消费者和最后使用时刻，纳入字节预算并验证慢消费；模型完成不能代替结果已消费 |
| 公共物理阶段描述与实测 | 已明确准备、模型、消费和交接的研究目标；尚未完成公共描述和真实完整查询测量，画像只作支撑 | 是：阶段描述与测量设计 | 本轮研究目标修订；[架构中的目标](../experiments/plans/系统架构.md#physical-stage-research-goal) | 先选一个完整查询，说明阶段、数据与允许动作，测展开／计算／收缩及准备消费竞争；不以任务间迁移为主线 |
| 后继优先与反序 | 两轮在当前负载均无收益，已停止不加区分地扩大实验 | 否，已停止扩展 | [原对照与负结果](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#priority-reverse-small-pair) | 保留负结果；只有新负载出现明确机会时才重新提出对照 |
| 跨模型、任意依赖图、预加载、多 GPU 与查询时限策略 | 本轮暂缓，不作为以上工作的前置项 | 否，暂缓 | 当前研究范围 | 先完成单卡完整查询和公共阶段实测，再按实际问题决定是否需要扩展 |

## main 与集成分支的阅读范围

| 已接纳能力 | 接口与资格来源 |
|---|---|
| Core 推进、完整 HTTP 响应、取消／unknown 与结果租约 | [adapter 接口](src/execution_provider/adapters/README.md)、[CPU／真实 Ray 基础检查](../experiments/results/diagnostics/main_foundation_20261015/README.md) |
| LOTUS 原 Movie Q1/Q2/Q3 四臂完整请求 | [LOTUS 模块](src/semantic_methods/lotus/README.md)、[文本接线](../experiments/results/diagnostics/main_lotus_text_20261015/README.md) |
| Animals 原图 Q1 四臂与观测模式 | [图文接线](../experiments/results/diagnostics/main_lotus_animals_20261015/README.md)、[固定 main 模型与连续查询](../experiments/results/diagnostics/main_queries_20261015/README.md)；保留质量不足及 RSS 待归因 |
| LOTUS 原 RM／FAISS 建库／检索与显式单卡入口 | [CLIP 接纳与资格](../experiments/results/diagnostics/main_lotus_clip_20261015/README.md)；默认精度失败、HSE 环境缺项与多卡未验证分列 |
| 可选 vLLM 部署资源解析与身份核验 | [服务及原查询单卡复验](../experiments/results/diagnostics/main_service_auto_20261015/README.md)；不借此认证 CLIP、多卡或其他平台 |
| 公共统计与离线结果工具 | [统计定义](src/observability/README.md#repeat-mean-confidence)、[离线输入与 CPU 范围](scripts/analysis/OFFLINE_RESULTS.md) |
| PG 同步／增量 Map、受限 Filter→Map 与 query-job | [extension 支持形状](postgres/semloom_pg/README.md#supported-query-shapes)、[增量执行](README.md#当前-postgresql-文本-map-的执行流程)及对应报告；更宽 SQL 与真实匹配 Filter 校准另验 |

接口说明只解释相应路径，模型结果仍按报告固定源码、输入、质量和生命周期使用。
未列为 main 接纳的集成能力从[固定 9c59dc9b 来源](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/code/INFRA_STATUS.md)回查。

<a id="current-image-status"></a>
## 固定图像来源与旧实现记录

`bdae61bb` 的图像能力、默认及未验证项属于该固定实现，不能认证 main 的执行代码；
完整表及原报告链接保留在[精简前能力表](https://github.com/3444374/ai-operator-execution-optimization/blob/0b8aca5ff599e1b6525b77608e3865b20a4fd996/code/INFRA_STATUS.md#current-image-status)。
可选 dispatcher、历史 broker 优化、服务级 RM／索引复用及表示候选均按各自来源判断，未由文档接纳为 main 能力。

文本 Map 的 worker／gateway 复用、Arrow、准备分段、尾块追加与计数诊断已有独立结果；
默认与接口见[诊断选项](README.md#文本map诊断选项)，完整失败与重复从原报告读取，已结束清单不自动恢复为任务。
2026-10-08 及更早的实现流水、历史调度默认与完成度表见[精简前状态](https://github.com/3444374/ai-operator-execution-optimization/blob/0b8aca5ff599e1b6525b77608e3865b20a4fd996/code/INFRA_STATUS.md#原-main-实现记录)。

<a id="record-local-sections-c827"></a>
## 单模型完整查询的可选逐记录入口

累计接线来自`c82714bb`运行快照；快照不是Git提交。`SemLoomRecordChainCore`显式
`execution="record_local_sections"`使用一个prepared Async model owner跨合法F/M段和完整Extract；
默认fused保持，prepare actor为0，B1和请求数C显式给定，不顺带启用token准入或优先级。
同模型有限组合的CPU验收为144个不同测试／232子用例，默认37份计划／150检查点相同；
这些数量属于原候选，不与新检查相加。本地PR62+c827组合源码`35a0e292`的432个运行文件
已通过226个不同CPU测试／261子用例；该组合不是原c827实机源码，不改写原实机身份。

实机只覆盖3行M→E→M→F的12成功及3行F→M的3F+2M成功；同查询一个model UID，
实际token／generation／TaskKey及原方法回放通过，资源已清理。补测第0行Map在另一行Filter完成前
有88.156ms模型API区间重叠；这是功能时序，不是kernel并行或净收益。
另一个新owner的取消项保留1次unknown，只证明IPC queued后触发取消，未证明原生运行中abort完成。
没有独立gold、任意链质量、跨查询模型复用、RSS/KV物理上限、可信P99或稳定加速资格。
原始计时、资源采样及局限见[证据身份](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#record-local-query-owner-c827)。
