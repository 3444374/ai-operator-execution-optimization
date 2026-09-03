# 实验计划与设计文档

更新日期：2026-09-03

本目录只承担三件事：维护当前实验合同、记录完成度、保存可复用的设计依据。实验数据与结论必须落在
`../results/`；动机实验落在 `../../motivation/results/`。不要从历史计划推断当前优先级。

## 1. 权威入口

| 问题 | 入口 |
|---|---|
| 当前先做什么、哪些仍有缺口 | [`experiment_status_and_gaps.md`](experiment_status_and_gaps.md) |
| 当前系统架构与实现顺序 | [`postgresql_ai_semantic_operator_architecture_20260827.md`](postgresql_ai_semantic_operator_architecture_20260827.md) |
| 生成型 Map 的具体行为、数据表示与验收 | [`postgresql_semmap_generation_contract.md`](postgresql_semmap_generation_contract.md)：消息、纯值与 Python v5 已合入 main；独立分支 PG plan/权限、C v5/golden 执行已验证，真实模型与资源检查按 §8 继续 |
| 回查 choice 已完成的字段、协议、请求预算与实施验收要求 | [`completed/postgresql_choice_profile_engineering.md`](completed/postgresql_choice_profile_engineering.md) |
| 全链路算子工程如何对比、采用及向公司移植 | [主计划 §8.7](postgresql_ai_semantic_operator_architecture_20260827.md#frontend-adapter-strategy)：SQL/PG 接入、语义与请求、取数/结果、资源/外部执行的具体对照、改动位置和验证 |
| 从 pgml 借鉴哪些模型接入做法 | [主计划 §8.8](postgresql_ai_semantic_operator_architecture_20260827.md#pgml-engineering-reference)：SQL 入口、公共模型调用、资源复用与单项/批量接口的采用时机和验证；保持 PG 外执行，不增加四 C 任务 |
| LOTUS 历史源码审计与兼容设计 | [`archive/lotus_semantic_frontend_execution_integration_20260821.md`](archive/lotus_semantic_frontend_execution_integration_20260821.md) |
| baseline 身份、准入和指标合同 | [`baseline_reference.md`](baseline_reference.md) |
| work-unit、状态感知和图像动态实验 | [`state_aware_work_unit_evaluation_20260808.md`](state_aware_work_unit_evaluation_20260808.md) |
| 真实数字与结论 | [`../results/EXPERIMENT_EVIDENCE_REGISTRY.md`](../results/EXPERIMENT_EVIDENCE_REGISTRY.md) |

主架构只维护分工、依赖与完成条件，详细的 choice 与生成型 Map 行为和验收由各自专项维护；源码与实验事实分别看
INFRA_STATUS 和证据台账。四 C 已收尾，后续按完整工程对照的决定先做四 D 真实生成型 SemMap，
再做[可组合执行 / 有界多会话](postgresql_ai_semantic_operator_architecture_20260827.md#composable-operators-work-package)；SemLoom 可独立开展
增量核心表征与 fixture 测试，公司接口可只读核对。四 C 的 PG choice SELECT 已接通 C 与 gateway v4，
Filter INSERT 的既有接管问题已独立修复并验证；受控 fixture 资源与受限真实 choice 服务检查均已通过，
四 C 工程验证完成，专项已归入 completed。多会话/组合、独立核心与公司接口仍待实施，当前集成版本已包含实现与证据。
完整工程参考按 §8.7 覆盖两个链路表；后续切片只重查受影响项，未来移植同时覆盖算子方法和执行能力。
Filter 的 reference 质量、matched cost 与第二路径继续保留，但不阻塞独立核心研发。校准失败结论不变。

## 2. 状态分层

### 当前计划（本目录顶层）

| 文件 | 当前状态与用途 |
|---|---|
| [`postgresql_ai_semantic_operator_architecture_20260827.md`](postgresql_ai_semantic_operator_architecture_20260827.md) | PostgreSQL 工程架构与实施顺序的唯一主计划；理论依据回指 `research/`，实现与证据回指各自状态入口 |
| [`postgresql_semmap_generation_contract.md`](postgresql_semmap_generation_contract.md) | 四 D 定稿；消息、纯值/Python v5 已集成，独立 PG plan/权限及 C v5/golden 执行已验证；真实模型与资源仍待验证 |
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

[四 C choice 专项](completed/postgresql_choice_profile_engineering.md)已完成工程验证，保存字段/预算/验收条件，
结果见[真实服务记录](../results/postgresql/choice_service_20260902/README.md)。不表示质量合格或完成整个优化系统。

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
2026-09-01 的[串行架构历史快照](archive/postgresql_ai_semantic_operator_architecture_serial_20260901.md)
保留原有完整资格尝试条件、接口原文与历史数字；其中的“当前/下一步”不覆盖现行工作包依赖。

## 3. 当前研究内容与实验对应

| 研究内容 | 当前证据 | 剩余工作 |
|---|---|---|
| 数据组织策略 | 文本 cache-on 双/四 endpoint 重测已完成，效果随 KV 压力 regime 变化 | 在资格项完成后，用同一抽象验证图像 frame/work budget；不重复无目的文本扫描 |
| 调度与提交控制 | static/shared credit、1/2/4 Job、重叠作业与五臂共同观测 rehearsal 已完成；呈现效率、隔离与公平权衡 | 五臂 formal 尚未运行；动态策略必须与同上限、预先选定的静态配置对比，并补所需 isolation control |
| 多模态泛化 | 图像画像、原生静态 baseline、多 Job 观察和 descriptor/observe-only 已归档 | HSE/static 非劣验证后再接受控动态动作，并核对写回、读回和结果质量 |
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
