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

独立逐记录 FIFO／token 候选未进 main；后继优先与反序对照的无收益结果，以及 C64 输出可比性停止原因，
分别见[后继策略](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#priority-reverse-small-pair)和
[token 候选](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#token-c64-comparability-gate)。main 默认不因候选机制证据改变。

<a id="record-results-migration"></a>
## 共享链结果模块

[PR56](https://github.com/3444374/ai-operator-execution-optimization/pull/56)已将结果身份实现归入
[方法公共模块](src/semantic_methods/lotus/README.md)，旧 import 保留同对象重导出，旧 pickle 可读。
worker 须同时部署新模块和兼容层并冷启动，不混用旧源码。CPU 迁移与历史投影修复不增加模型资格；
逐次检查与来源路由见台账和具名复现入口。

## 候选交接状态（2026-10-27）

本轮交接回执：`f712`／`0441`已完成独立审查；457 的 F/M 局部归并第二步已独审通过，owner 尚未完整接线；不据此开放新执行模式或增加实机资格。
小规模真实观测已执行5次调用、取得56条事件；完整来源与支持范围按代码线程原回执核对。
这些候选未据此列为 main 已接纳能力，后续只更新本页状态。

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
