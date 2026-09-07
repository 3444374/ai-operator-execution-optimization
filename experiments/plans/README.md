# 实验计划与设计文档

更新日期：2026-09-07

本目录只承担三件事：维护当前实验合同、记录完成度、保存可复用的设计依据。实验数据与结论必须落在
`../results/`；动机实验落在 `../../motivation/results/`。不要从历史计划推断当前优先级。

## 1. 权威入口

| 问题 | 入口 |
|---|---|
| 当前实验完成度、证据缺口与运行条件 | [`experiment_status_and_gaps.md`](experiment_status_and_gaps.md) |
| 当前系统架构与实现顺序 | [主设计](postgresql_ai_semantic_operator_architecture_20260827.md)：长期能力、PG/方法/Core职责、调用与任务关系、资源所有权；[实施与验收](postgresql_ai_semantic_operator_architecture_20260827.md#implementation-sequence) |
| 近期PG公共调用/绑定与首个组合怎样编码 | [PG调用与绑定详细设计](postgresql_call_binding_design.md)：A1及A2a一个Filter→一个生成Map，含对象/时序/新旧绑定/具体预期；实施待进行 |
| 增量session如何接纳、推进、取消和回收 | [增量session详细设计](semloom_incremental_session_design.md)：B1静态复核/B2单流合同、完成lease与Engine账本、B4分层依赖；动态表征/实施待进行 |
| 生成型 Map 的具体行为、数据表示与验收 | [`postgresql_semmap_generation_contract.md`](postgresql_semmap_generation_contract.md)：消息、纯值、PG plan/权限与 C/Python v5/golden 已纳入 main；真实模型与资源检查按 §8 继续 |
| 回查 choice 已完成的字段、协议、请求预算与实施验收要求 | [`completed/postgresql_choice_profile_engineering.md`](completed/postgresql_choice_profile_engineering.md) |
| 全链路算子工程如何对比、采用及向公司移植 | [主计划 §8.7](postgresql_ai_semantic_operator_architecture_20260827.md#frontend-adapter-strategy)：SQL/PG 接入、语义与请求、取数/结果、资源/外部执行的具体对照、改动位置和验证 |
| 从 pgml 借鉴哪些模型接入做法 | [主计划 §8.8](postgresql_ai_semantic_operator_architecture_20260827.md#pgml-engineering-reference)：SQL 入口、公共模型调用、资源复用与单项/批量接口的采用时机和验证；保持 PG 外执行，不增加四 C 任务 |
| LOTUS 历史源码审计与兼容设计 | [`archive/lotus_semantic_frontend_execution_integration_20260821.md`](archive/lotus_semantic_frontend_execution_integration_20260821.md) |
| baseline 身份、准入和指标合同 | [`baseline_reference.md`](baseline_reference.md) |
| 固定服务如何扩到可审计的多 endpoint | [主计划 §6.5](postgresql_ai_semantic_operator_architecture_20260827.md#execution-deployment-identity)：显式选择、部署快照、query-fixed 身份与逐任务记录；待实现，不修改四 D v5 |
| 怎样区分 gateway、PG 接入和 SemLoom 的开销 | [baseline reference §0.2](baseline_reference.md#gateway-layered-controls)：A/B/C/D 匹配诊断与方法消融；待执行，不是运行授权或原生排名 |
| 前缀/表示候选为何值得做、怎样进入代码 | [研究审查](../../research/semantic_prefix_reuse_design_audit_20260903.md)说明依据与待证命题；[主计划 §7](postgresql_ai_semantic_operator_architecture_20260827.md#research-mechanism-slices)规定 Module 落点；[对照 §0.3](baseline_reference.md#semantic-prefix-causal-controls)区分质量、任务量与执行收益。均不改变四 D 或授权实验 |
| work-unit、状态感知和图像动态实验 | [`state_aware_work_unit_evaluation_20260808.md`](state_aware_work_unit_evaluation_20260808.md) |
| 真实数字与结论 | [`../results/EXPERIMENT_EVIDENCE_REGISTRY.md`](../results/EXPERIMENT_EVIDENCE_REGISTRY.md) |

主设计已按项目长期能力重写，choice与生成型Map具体语义仍由专项维护，实际代码/实验状态只看
INFRA_STATUS和证据台账。当前双Filter/多会话实现已合并main；数据库公共调用/绑定与
独立增量Core可分别推进，再完成对应PG桥接。Filter质量/成本资格独立，复杂SQL、方法优化与公司
移植按实际需求验证。全部工程依赖统一见主设计§9；上述两份下级规格只拥有近期细节，不另定义总体方向。
本次设计更新没有新增实现、模型运行或实验资格。
后续A1首步已完成Map调用分析的行为保持提取，见[PG规格§8](postgresql_call_binding_design.md)
及[验证记录](../results/postgresql/semantic_call_extraction_20260907/README.md)；共同调用/绑定与新组合仍待实施。

## 2. 状态分层

### 当前计划（本目录顶层）

| 文件 | 当前状态与用途 |
|---|---|
| [`postgresql_ai_semantic_operator_architecture_20260827.md`](postgresql_ai_semantic_operator_architecture_20260827.md) | PostgreSQL 工程架构与实施顺序的唯一主计划；理论依据回指 `research/`，实现与证据回指各自状态入口 |
| [`postgresql_semmap_generation_contract.md`](postgresql_semmap_generation_contract.md) | 四 D 定稿；消息、纯值、PG plan/权限及 C v5/golden 已集成；真实模型与资源仍待验证 |
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
