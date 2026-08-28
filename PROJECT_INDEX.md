# 项目导航

更新时间：2026-08-28

本文件只回答“应该从哪里读、到哪里改”。它不复制实验数字，不承担项目日志或历史资产清单。
精确结果以对应结果目录的原始文件为准。

## 1. 权威来源

| 内容 | 单一入口 | 说明 |
|---|---|---|
| 项目长期规则 | [`AGENTS.md`](AGENTS.md) | 全项目规则、继承顺序与目录规则路由；进入子目录后再读沿途 `AGENTS.md` |
| 系统名与领域术语 | [`CONTEXT.md`](CONTEXT.md) | SemLoom、DB-AIEL、AI semantic operator、execution provider 与历史身份的规范含义 |
| 项目总纲 | [`PROJECT_OUTLINE.md`](PROJECT_OUTLINE.md) | 题目、研究内容、证据等级和当前执行顺序 |
| 当前方向速览 | [`overview/current_direction_and_plan.md`](overview/current_direction_and_plan.md) | 两分钟交接卡片；不保存详细历史 |
| 实现状态 | [`code/INFRA_STATUS.md`](code/INFRA_STATUS.md) | 代码模块、已接线能力和未完成项 |
| 实验证据台账 | [`experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) | 机制、代码、测试、结果和证据等级映射 |
| 开题判断表 | [`opening/claim_matrix.md`](opening/claim_matrix.md) | 对外 claim、依据和不能声称的范围 |
| 文献知识入口 | [`research/knowledge_hub.md`](research/knowledge_hub.md) | 文献、设计候选和研究问题 |
| 变更历史 | [`PROJECT_LOG.md`](PROJECT_LOG.md) | 结构、方向、结论和关键入口变更记录 |

冲突处理顺序：原始结果/源码 > 领域权威入口 > `PROJECT_OUTLINE.md` > 快速说明 > 历史设计和
实施计划。历史文件中的“当前”“下一步”只对其文件日期有效。

规则按路径继承：根 `AGENTS.md` 始终生效，目标目录沿途的 `AGENTS.md` 只追加局部职责与验证要求，
README 保存目录内容和当前状态。`CLAUDE.md` 只是 Claude Code 的根入口，不再复制或无条件导入
所有子目录规则。根“文档受众与对外表达”规则适用于任意目录中的读者型文档；子目录不能允许
对外稿重新使用“冻结、门禁、闭环”等未解释的项目管理词。

## 2. 常用阅读路径

### 了解课题

1. [`README.md`](README.md)
2. [`CONTEXT.md`](CONTEXT.md)
3. [`overview/current_direction_and_plan.md`](overview/current_direction_and_plan.md)
4. [`PROJECT_OUTLINE.md`](PROJECT_OUTLINE.md)
5. [`opening/claim_matrix.md`](opening/claim_matrix.md)

### 继续当前实现

1. [`code/AGENTS.md`](code/AGENTS.md)
2. [`code/README.md`](code/README.md)
3. [`code/INFRA_STATUS.md`](code/INFRA_STATUS.md)
4. [`experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md`](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md)

当前最短期任务是 `REL_18_4` extension/planner-visible `SemMap` capability，然后实现中立
plan/task/result 合同、`open/drive/close` 与 UDS recording gateway；载体审查决定是否需要最小 core
patch，之后才接增量 SemLoom、HTTP 与 `SemFilter` semantic alternative。
既有 Daft/Ray/static/SAOR 代码作为 SemLoom provider 后方的可替换 backend，不先扩大 GPU 参数矩阵。

### 运行实验或迁移机器

1. [`deploy/runtime/AGENTS.md`](deploy/runtime/AGENTS.md)
2. [`deploy/runtime/README.md`](deploy/runtime/README.md)
3. 平台 runbook，例如 [`deploy/autodl/README.md`](deploy/autodl/README.md)
4. [`experiments/AGENTS.md`](experiments/AGENTS.md)
5. [`experiments/plans/README.md`](experiments/plans/README.md)
6. 对应实验计划和结果目录 README

任何新机器、依赖、模型或数据任务先运行 `manage_environment.py check`。正式运行使用机器、模型、
服务、协议和 workload 共同签名下在运行期间保持不变的配置，不继承另一签名的 K/batch/actor 参数。

### 判断一个结论能否使用

1. [`experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)
2. 对应 `experiments/results/<experiment>/README.md`
3. 同目录 CSV/JSON/manifest/raw
4. [`opening/claim_matrix.md`](opening/claim_matrix.md)（若用于开题或对外材料）

### 写开题材料

1. [`opening/AGENTS.md`](opening/AGENTS.md)
2. [`opening/README.md`](opening/README.md)
3. [`opening/report/opening_report.md`](opening/report/opening_report.md)
4. [`opening/opening_defense_outline_20260808.md`](opening/opening_defense_outline_20260808.md)
5. [`opening/qa_bank.md`](opening/qa_bank.md)
6. [`figures/opening_figure_set/README.md`](figures/opening_figure_set/README.md)

## 3. 目录职责

| 目录 | 内容 | 当前入口 | 不放什么 |
|---|---|---|---|
| `overview/` | 当前方向速览 | `overview/README.md` | 详细实验历史 |
| `research/` | 文献、知识和设计依据 | `research/README.md`、`research/knowledge_hub.md` | 运行结果 |
| `motivation/` | 课题动机与 GPU-backed 系统画像 | `motivation/README.md` | 方法胜出结论 |
| `feasibility/` | 组件、环境、capability 和 smoke | `feasibility/README.md` | 正式方法排名 |
| `experiments/` | 方法计划和正式结果 | `experiments/README.md` | 临时环境产物 |
| `code/` | 可复用实现、CLI 和测试 | `code/README.md` | 一次性图脚本 |
| `deploy/` | 环境合同、配置和 runbook | `deploy/README.md` | 研究结论 |
| `data/` | 数据来源、哈希和导入合同 | `data/README.md` | raw payload |
| `figures/` | 图源、导出和审计 | `figures/README.md` | 无来源截图 |
| `opening/` | 开题正文、答辩和文献快照 | `opening/README.md` | 实验原始数据 |
| `learning/` | 教学式讲解 | `learning/README.md` | 权威规则 |
| `notes/` | 导师/企业沟通 | `notes/README.md` | 正式研究证据 |
| `docs/` | 已完成的跨目录设计记录 | `docs/README.md` | 当前执行指令 |
| `code_doc/` | 已完成代码设计与实施计划 | `code_doc/README.md` | 当前代码状态 |
| `projects/` | 旧 PPT 工程归档 | `projects/README.md` | 新开题材料 |

## 4. 当前实现与代码入口

| 需求 | 入口 |
|---|---|
| 代码目录与模块关系 | [`code/README.md`](code/README.md) |
| 已实现/未实现边界 | [`code/INFRA_STATUS.md`](code/INFRA_STATUS.md) |
| CLI 说明 | [`code/scripts/README.md`](code/scripts/README.md) |
| 数据源、物化和 sink | `code/src/data/` |
| work 与代价估计 | `code/src/planning/` |
| 数据组织、credit、routing 和调度 | `code/src/scheduling/` |
| completion/embedding backend | `code/src/serving/` |
| 文本/图像模态适配 | `code/src/modalities/` |
| 指标与 profiler | `code/src/observability/` |
| baseline 适配 | `code/src/baselines/` |
| 测试 | `code/tests/` |

[`code/ARCHITECTURE_REFACTOR_PLAN.md`](code/ARCHITECTURE_REFACTOR_PLAN.md) 是 2026-08-03 已完成
的迁移记录，只用于解释现有结构，不再提供待执行步骤。

## 5. 计划入口

| 主题 | 当前入口 | 状态 |
|---|---|---|
| PostgreSQL AI 语义算子整体架构 | [`experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md`](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md) | 当前工程主线 |
| baseline 身份与选择 | [`experiments/plans/baseline_reference.md`](experiments/plans/baseline_reference.md) | 当前规则入口 |
| 全部实验状态与缺口 | [`experiments/plans/experiment_status_and_gaps.md`](experiments/plans/experiment_status_and_gaps.md) | 状态审计；历史“下一步”按日期读取 |
| 数据组织 | [`experiments/plans/data_organization_batching.md`](experiments/plans/data_organization_batching.md) | 方法计划 |
| 提交与反压 | [`experiments/plans/service_scheduling_backpressure.md`](experiments/plans/service_scheduling_backpressure.md) | 方法计划 |
| 图像 workload | [`experiments/plans/completed/image_clip_workload_lock_20260731.md`](experiments/plans/completed/image_clip_workload_lock_20260731.md) | 静态范围已完成；动态部分见 state-aware 当前计划 |

`experiments/plans/archive/`、`docs/` 和 `code_doc/` 只用于追溯。若历史计划仍有有效待办，应把它
迁入上表对应的当前入口，而不是继续追加历史文件。

## 6. 证据入口

| 证据类型 | 入口 |
|---|---|
| 正式方法结果 | [`experiments/results/README.md`](experiments/results/README.md) |
| 机制—实现—证据映射 | [`experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| GPU-backed 动机画像 | [`motivation/results/gpu/README.md`](motivation/results/gpu/README.md) |
| capability/smoke | [`feasibility/results/README.md`](feasibility/results/README.md) |
| 开题 claim 与证据等级 | [`opening/claim_matrix.md`](opening/claim_matrix.md) |

CPU/fake、PG18.4 rehearsal、development gate、diagnostic、rehearsal 和 formal 是不同证据等级；
目录名或“passed”字段不能替代结果报告中的身份和适用范围。

## 7. 文献与知识

- 总入口：[`research/README.md`](research/README.md)
- 知识汇总：[`research/knowledge_hub.md`](research/knowledge_hub.md)
- Sema-like 架构审计：[`research/sema_native_semantic_operator_architecture_reference_20260827.md`](research/sema_native_semantic_operator_architecture_reference_20260827.md)
- 文献库存：[`research/ai_operator_literature_inventory.md`](research/ai_operator_literature_inventory.md)
- Top 15：[`research/top15_ranked_papers.md`](research/top15_ranked_papers.md)
- 泛读笔记：[`research/reading_notes/README.md`](research/reading_notes/README.md)
- 精读笔记：[`research/精读文献笔记/README.md`](research/精读文献笔记/README.md)
- Kalypso 核心补充精读：[`research/精读文献笔记/kalypso_arxiv2026/kalypso_arxiv2026.md`](research/精读文献笔记/kalypso_arxiv2026/kalypso_arxiv2026.md)（正文 Figure 1–12 已配图；选图与视觉检查见 [`figures/audit/kalypso_deep_reading_figures_audit_20260827.md`](figures/audit/kalypso_deep_reading_figures_audit_20260827.md)；不进入当前 Top 15、十五篇速览或开题正文）
- IMLane 正式论文精读：[`research/精读文献笔记/imlane_pvldb2026/imlane_pvldb2026.md`](research/精读文献笔记/imlane_pvldb2026/imlane_pvldb2026.md)（正文 Figure 1–15 已配图；Figures 9–10 共用同页联合裁剪件；选图与视觉检查见 [`figures/audit/imlane_deep_reading_figures_audit_20260828.md`](figures/audit/imlane_deep_reading_figures_audit_20260828.md)）
- 本地参考资料索引：[`research/reference/REFERENCE_INDEX.md`](research/reference/REFERENCE_INDEX.md)

## 8. 历史与归档规则

- 历史结果和失败证据保留原目录，不因结论被替代而删除。
- 已完成计划在目录 README 中标记状态，并指向当前入口。
- `projects/` 保留旧 PPT 工程的输入、输出和验证记录；不再用于生成新材料。
- `code_doc/` 和 `docs/` 的计划文本可以包含过时分支名或“下一步”，但必须按文件日期阅读。
- 本地 `.venv`、`tmp/`、raw workload、模型、缓存和 `__pycache__` 不进入 Git。
- 新增、移动或删除文件后更新本索引、根 README、受影响目录 README 和 `PROJECT_LOG.md`。
