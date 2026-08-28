# 实验计划与设计文档

更新日期：2026-08-28

本目录只承担三件事：维护当前实验合同、记录完成度、保存可复用的设计依据。实验数据与结论必须落在
`../results/`；动机实验落在 `../../motivation/results/`。不要从历史计划推断当前优先级。

## 1. 权威入口

| 问题 | 入口 |
|---|---|
| 当前先做什么、哪些仍有缺口 | [`experiment_status_and_gaps.md`](experiment_status_and_gaps.md) |
| 当前系统架构与实现顺序 | [`postgresql_ai_semantic_operator_architecture_20260827.md`](postgresql_ai_semantic_operator_architecture_20260827.md) |
| LOTUS 历史源码审计与兼容设计 | [`archive/lotus_semantic_frontend_execution_integration_20260821.md`](archive/lotus_semantic_frontend_execution_integration_20260821.md) |
| baseline 身份、准入和指标合同 | [`baseline_reference.md`](baseline_reference.md) |
| work-unit、状态感知和图像动态实验 | [`state_aware_work_unit_evaluation_20260808.md`](state_aware_work_unit_evaluation_20260808.md) |
| 真实数字与结论 | [`../results/EXPERIMENT_EVIDENCE_REGISTRY.md`](../results/EXPERIMENT_EVIDENCE_REGISTRY.md) |

当前短期顺序是：

1. `REL_18_3` extension / planner-visible `SemMap` 已验证受限 `SELECT`、direct `INSERT ... SELECT`、
   ordinary child plan、snapshot、query lifecycle、初始 typed seam，以及同步单在途 UDS recording provider；
2. 在已验证的初始 digest/UDS slice 上补 accepted-prefix backpressure、多在途、乱序 completion、
   有界 reorder 与显式 early-stop close disposition；
3. 审查 extension 能否承载目标 LOTUS/Cortex semantic alternatives；能表达则保留 extension，只有已复现
   阻断才增加最小 core patch；
4. 抽取增量 SemLoom session，再接 HTTP/SemLoom provider，并以 `SemFilter` 验证 cardinality 与首条
   database semantic optimization；
5. 上述步骤完成前不扩展 GPU 矩阵、不调整 SAOR，也不把既有 external runner 写成数据库内算子。

## 2. 状态分层

### 当前计划（本目录顶层）

| 文件 | 当前状态与用途 |
|---|---|
| [`postgresql_ai_semantic_operator_architecture_20260827.md`](postgresql_ai_semantic_operator_architecture_20260827.md) | 当前实施主计划；extension/core 条件性载体、最小 LOTUS/Cortex semantic path、provider interface；IMLane 为资格后验证，Kalypso 为后续参考 |
| [`state_aware_work_unit_evaluation_20260808.md`](state_aware_work_unit_evaluation_20260808.md) | 已含项目内部机制与五臂共同观测 rehearsal；剩余图像动态、五臂 formal/隔离补测等待上游资格项 |
| [`opening_database_e2e_p0_20260807.md`](opening_database_e2e_p0_20260807.md) | 主矩阵已完成；仅 ShareGPT C128 双臂纠正补测待条件满足后执行 |
| [`saor_cross_layer_scheduler_capability_20260820.md`](saor_cross_layer_scheduler_capability_20260820.md) | `blocked`；formal 未授权，不是当前执行项 |
| [`data_organization_batching.md`](data_organization_batching.md) | 文本主矩阵已完成；保留后续模态复用与条件性扩展合同 |
| [`service_scheduling_backpressure.md`](service_scheduling_backpressure.md) | 静态/shared credit 主证据已完成；动态候选未证明普遍胜出 |
| [`cross_layer_killer_experiment.md`](cross_layer_killer_experiment.md) | 独立最优拼接与联合搜索的条件性耦合验证 |
| [`full_grid_sweep_plan.md`](full_grid_sweep_plan.md) | 暂停的可选扩展矩阵；无当前运行授权 |

顶层另保留两份横向入口：

- [`baseline_reference.md`](baseline_reference.md)：baseline 唯一总入口；
- [`experiment_status_and_gaps.md`](experiment_status_and_gaps.md)：完成度、证据强度与缺口的唯一状态入口。

### 已完成计划

[`completed/`](completed/) 保存已执行完成、已被结果替代，或其当前范围已经闭合的合同。正文不删除，
以便追溯预注册变量与执行边界；不得把正文中的“下一步”自动视为当前任务。

### 设计参考

[`reference/`](reference/) 保存跨实验复用的协议、检查清单、文献边界和历史工程映射。它们不是待执行计划，
也不单独产生实验结论。

### 历史归档

[`archive/`](archive/) 保存被当前方向替代、暂停且没有运行授权的候选方案与旧矩阵。归档不等于删除，
只表示它们不能覆盖当前总纲和状态文件。

2026-08-21 的 PostgreSQL+LOTUS 主计划与 LOTUS frontend 子计划已进入归档；其中的 v1.2.4 源码
审计、Q1–Q23 决策和反例测试仍可追溯，但当前架构不再以 LOTUS 为语义所有者或前置依赖。

## 3. 当前研究内容与实验对应

| 研究内容 | 当前证据 | 剩余工作 |
|---|---|---|
| 数据组织策略 | 文本 cache-on 双/四 endpoint 重测已完成，效果随 KV 压力 regime 变化 | 在资格项完成后，用同一抽象验证图像 frame/work budget；不重复无目的文本扫描 |
| 调度与提交控制 | static/shared credit、1/2/4 Job、重叠作业与五臂共同观测 rehearsal 已完成；呈现效率、隔离与公平权衡 | 五臂 formal 尚未运行；动态策略必须同上限对比冻结静态点，并补所需 isolation control |
| 多模态泛化 | 图像画像、原生静态 baseline、多 Job 观察和 descriptor/observe-only 已归档 | HSE/static 非劣验证后再接受控动态动作与质量闭环 |
| 算子代价估计 | 双 4090 v2 cache-on 320/320 有效，首次无效运行独立保留 | 新时间段或新 workload 校准；是否用于在线决策由 regret/区间结果决定 |

写回固定使用 PostgreSQL + pgvector 的 COPY + deferred index 工程 baseline，不作为独立研究内容。

## 4. 新实验的最低要求

每个正式实验必须：

1. 指向一个明确研究问题和当前计划；
2. 记录平台、模型、协议、workload、资源上限、重复和随机化方式；
3. 先通过 correctness、provenance、feeding-saturation 与稳定性检查；
4. 区分服务上限、直接客户端、框架原生、数据库产品原生和项目方法；
5. 把完整配置、命令、CSV/manifest、异常与结论边界写入对应结果目录；
6. 更新本目录状态入口、证据注册表与 `PROJECT_LOG.md`。

详细执行规则以本目录 [`AGENTS.md`](AGENTS.md) 和根 `AGENTS.md` 为准；报告前使用
[`reference/experiment_report_honesty_checklist.md`](reference/experiment_report_honesty_checklist.md)。

## 5. 维护纪律

- 新信息优先并入已有权威文件，只有不存在自然归属时才新增文档。
- 计划完成后移动到 `completed/`，并在文件首部写明完成范围、结果入口和仍未覆盖的事项。
- 仅供方法复用的材料进入 `reference/`；被方向替代或暂停的方案进入 `archive/`。
- 历史正文可保留当时术语，但文件首部必须说明其历史身份；当前术语以根总纲为准。
- 不在计划首页复制易漂移的详细参数或实验数字；数字只从结果报告和证据注册表读取。
- 不删除 raw、manifest、失败运行或事故证据；无效结果必须与有效结果分开并明确排除原因。
