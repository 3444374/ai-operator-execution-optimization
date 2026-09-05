# SemLoom

**A Database-Aware Execution and Scheduling Layer for AI Semantic Operators**

**面向数据库 AI 语义算子的工作量感知执行与多作业调度系统**

本仓库正在实现 SemLoom，并研究 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
DB-AIEL（Database-Aware AI Execution Layer）是架构层名称，不作为代码接口前缀。参考 Sema 一类数据库
原生语义算子系统，PostgreSQL 拥有 SQL、关系 child plan、snapshot、权限、语义计划和 query
lifecycle；数据库管理的有界数据流把规范化任务交给可替换的 Daft/Ray/vLLM/CLIP backend 执行。

当前状态（2026-09-06）：`REL_18_3` extension 已完成受限、deterministic recording `SemMap` 与 exact
`SemFilter` reference paths、PostgreSQL-private shared runtime、同步单在途 provider seam 和公共
compatibility tests。这些结果证明 PostgreSQL 可以拥有 ordinary child plan、snapshot、权限、取消、
错误和结果生命周期，并通过可替换 adapter 调用外部执行器。当前 planner 还会把 recording reference
与 exact reference 实际消费的 operator/value/policy、instruction、prompt/parser、model/generation、
semantic spec identity、physical algorithm 和 physical role 写入版本化、
可复制的最小 plan spec；executor 严格解码后再映射为 `AiOpenSpec`。provider error interface 只保留
中立类别、`errno`、长度和定长脱敏详情，socket/JSON/frame 细分由 adapter 本地产生。同步 fixed-model
adapter 已在相同 wire v3、PostgreSQL parser 与 keep/drop 路径上接通 OpenAI-compatible endpoint；它尚未
实现 quality policy、第二 physical path、异步调度或性能优化。

execution-provider gateway 实现位于公共 `code/src/execution_provider/`，统一通过
`code/scripts/services/run_execution_provider_gateway.py` 启动；旧 extension import/CLI 别名已删除，
调用方与测试使用公共入口。wire v2 bytes、digest 和 SQL 行为保持不变。
同一 plan/task/result contract 已先通过 deterministic golden adapter，再通过同步 fixed-model endpoint；
固定 endpoint、model identity、timeout 与认证只来自 gateway 进程外配置。reference path 已独立
区分 semantic-input rows、NULL rate、output selectivity、model calls、prompt/output usage、
model role 和 AI-work cost，并在执行时分列实际 usage；该工程启发式还没有校准为性能模型。
最新[两算子完整验证](experiments/results/postgresql/semmap_resource_lifecycle_20260906/README.md#main-integration)
覆盖全部 PostgreSQL18.3 回归与 TAP；Filter v3/v4 和生成型 Map v5 的真实 SELECT/INSERT、
NULL 零调用、结果与写回均通过。两个算子目前各自同步执行，同查询组合与异步执行仍待实现。
接下来分别推进自有 PG 算子、SemLoom 核心和公司接口对照。
[可选 choice 生成配置](experiments/plans/completed/postgresql_choice_profile_engineering.md)的工程验证已完成；
在完整工程对照后先做[真实生成型 SemMap](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md#real-semmap-work-package)与必要公共实现整理，
再扩展[可组合执行与有界多会话](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md#composable-operators-work-package)；
生成型 Map 的输入输出与验收要求已确定，详见[实现说明](experiments/plans/postgresql_semmap_generation_contract.md)，
消息编译、C/Python 值表示、PG plan/权限和 C client→wire v5→gateway 接线已纳入 main，
三参 Map 已能通过 PostgreSQL＋golden 返回文本，详见[执行与复核记录](experiments/results/postgresql/semmap_pg_wire_20260903/README.md)。
后续[真实模型与资源检查](experiments/results/postgresql/semmap_real_model_resource_20260904/README.md)
以持久账本完成 25/32 个 Qwen2.5-7B 请求：SELECT、INSERT、NULL 零调用、取消、模型拒绝和恢复通过。
同轮 3×2,000 个大输入/大输出 fixture task 功能完成，但至少一项固定 RSS/FD 条件失败，后置 fault
子项未运行；因此资源资格和四 D 整体仍待完成。真实或 golden completion 都不代表模型质量或性能。
choice SELECT 与受限单表 Filter INSERT 已接通 PG plan、公共 runtime 和 gateway v4，并完成合成测试；
当前代码已完成[受控 fixture 资源检查](experiments/results/postgresql/choice_resources_20260902/README.md)；
后续[真实服务检查](experiments/results/postgresql/choice_service_20260902/README.md)也已通过，但不表示模型判断质量合格。
生成型 Map 的资源补证、多算子组合与 Filter → Map 仍待完成。SemLoom 可以先用公开任务与可控测试验证增量执行、数据组织和调度，不等待 Filter
分类质量或第二路径；接入 PG 后仍须验证本路径的语义、关联、取消和资源使用，才能做数据库端到端比较。
Filter 的真实校准仍暂停，其质量、成本与 LOTUS/Cortex-like 第二路径继续单独推进，不降低既有要求。
carrier 检查随实际路径进行，只有可复现的限制才触发最小 core patch。IMLane-like 组批位置对照
需要真实 PG 增量接入；Kalypso-like 多阶段机制仍按实际需求另行决定。

主实现继续完成两部分：自有 `semloom_pg` 语义算子，以及 SemLoom 数据执行与调度；目标是不依赖
公司私有仓库也能复现。现在对照公司 demo 的 SQL 注册、PG 接入、算子实现、请求/结果、生命周期与
外部执行，选择能服务本项目的工程经验；保持计划内语义、公共执行层和 PG 外的 SemLoom 分工。
未来可把自有算子语义、处理/优化方法和 SemLoom 执行能力移植到公司系统。算子方法由目标
planner/executor 承接，执行能力接入同一个 SemLoom 核心，两者分别验证。具体参考对象与取舍见
[工程参照与成果移植计划](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md#frontend-adapter-strategy)。
Filter 的共同目的为按自然语言条件筛行；二值或三值输出由具体任务定义，当前三值配置不限制未来
所有 Filter。内网代码复用与外部发布/部署分别确认权限，公司移植不作为公开主实现的私有前置依赖。

目标执行链路是（第二优化路径与调度部分尚未接入数据库）：

```text
SQL semantic intent
  -> PostgreSQL logical semantics and reference policy
  -> reference / optimized physical paths and AI-work costing
  -> sealed tasks through the execution-provider seam
  -> SemLoom organization, admission, routing and multi-Job execution
  -> PostgreSQL validates completions and restores relational results
```

## 先读什么

| 需求 | 权威入口 |
|---|---|
| 两分钟了解当前方向 | [`overview/current_direction_and_plan.md`](overview/current_direction_and_plan.md) |
| 核对题目、研究内容、证据等级和执行顺序 | [`PROJECT_OUTLINE.md`](PROJECT_OUTLINE.md) |
| 查找文件和阅读路径 | [`PROJECT_INDEX.md`](PROJECT_INDEX.md) |
| 核对项目长期规则和边界 | [`AGENTS.md`](AGENTS.md) |
| 核对系统名和领域术语 | [`CONTEXT.md`](CONTEXT.md) |
| 理解 Sema/Cortex/LOTUS/IMLane/Kalypso 等机制与可迁移范围 | [`research/knowledge_hub.md`](research/knowledge_hub.md) |
| 核对当前源码真实完成度 | [`code/INFRA_STATUS.md`](code/INFRA_STATUS.md) |
| 判断某项机制是否已实现、验证或淘汰 | [`experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| 继续 PostgreSQL AI 语义算子实现（CustomScan、公共层、解耦、core patch 条件与工作包） | [`experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md`](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md) |
| 回查已完成的 choice 配置（字段、版本、预算与验收） | [`completed/postgresql_choice_profile_engineering.md`](experiments/plans/completed/postgresql_choice_profile_engineering.md) |
| 在新机器或 GPU 环境运行 | [`deploy/runtime/README.md`](deploy/runtime/README.md) |
| 准备开题报告或答辩 | [`opening/README.md`](opening/README.md) |

当文档冲突时，按“原始结果/代码 → 领域权威入口 → 项目总纲 → 快速说明 → 历史计划”的顺序
核对，不从文件日期或文件名猜当前状态。

## 研究内容

1. 数据组织：依据 token、frame、prepare/model work 与局部性构造工作单元，比较固定行数、
   work budget、长度对齐和 prefix-aware 等策略在不同服务状态下的效果。
2. 提交、路由与多 Job 调度：在固定 request/work capacity 下研究持续补位、状态感知提交、
   endpoint 路由、idle borrowing、回收和 Job 级服务区分。

算子代价估计为两项内容提供共同信息，并支持数据库计划比较；它不是第三项研究内容。文本
`AI_COMPLETE` 是主场景，图像 `AI_EMBED/AI_CLASSIFY` 用于验证策略抽象能否跨模态复用。

数据库 semantic optimizer 是两项研究内容的语义前提和实验入口：它决定产生什么 AI work，并以
reference/quality policy 约束优化；两项研究内容比较这些 sealed work 如何组织和执行。数据库路径
代价与 provider 执行代价分开建模，不能用减少模型调用掩盖低效执行，也不能用更快调度绕过质量要求。

项目不做广泛 PostgreSQL fork，也不修改 vLLM continuous batching、Ray scheduler、模型结构或 GPU
kernel；只有 extension carrier 出现已复现 optimizer/node-lifecycle 阻断时，才采用最小 PG18.3 core
semantic patch。项目也不以传统 GPU 查询算子、逐行 HTTP UDF 或
`SELECT/fetchall → Python → HTTP → INSERT` 作为主线。

## 目录层级

```text
.
├── AGENTS.md / CONTEXT.md / PROJECT_OUTLINE.md / PROJECT_INDEX.md  # 规则、术语、总纲、导航
├── overview/       # 当前方向速览
├── research/       # 文献、知识与方法依据
├── motivation/     # 动机画像与 GPU-backed 端到端证据
├── feasibility/    # 环境、组件和 capability/smoke 验证
├── experiments/    # 方法计划与正式实验结果
├── code/           # 可复用实现、脚本和测试
├── deploy/         # 跨机器环境合同与运行手册
├── data/           # 数据来源和导入合同；raw 不进 Git
├── figures/        # 图资产、生成脚本和审计记录
├── opening/        # 开题报告、答辩材料和文献快照
├── learning/       # 教学式讲解材料
├── notes/          # 导师/企业沟通记录
├── docs/           # 已完成的一次性跨目录设计记录
├── code_doc/       # 已完成的代码设计与实施计划
└── projects/       # 旧 PPT 生成工程归档
```

进入目录前从根到目标逐级读取沿途的 `AGENTS.md`，再读目标目录 `README.md`。根规则负责全项目
范围与安全，子目录规则只增加本地职责和验证要求。`docs/`、`code_doc/`、
`projects/` 和 `experiments/plans/archive/` 是历史追溯面，不得覆盖当前总纲、源码、结果台账或
部署 runbook。

## 当前证据能支持什么

- 固定行数不能稳定表示 AI work；数据组织策略在低 KV 压力与高 KV 压力下会出现不同排序。
- 当前双 RTX 4090/Qwen/vLLM 签名下存在最小近饱和 active-work 区间；该数值不能跨机器、模型、
  endpoint 拓扑或 workload 直接复用。
- 多种复杂动态控制没有稳定超过强静态点；现有结果支持“效率—尾延迟—公平性权衡”，不支持
  动态方法普遍胜出。
- 图像路径已经证明分阶段 work/state 可观测和原生多 Job 干扰存在，但尚未证明状态感知动态
  调度胜出。
- SAOR 的当前 formal 合同因 feeding ratio 未达到预注册阈值而保持
  `locked_failed_feeding/formal_authorized=false`；相关五臂结果仍是 rehearsal/诊断证据。
- 开题报告和图件已在本地完成多轮内容、引用和可读性审计；云文档/Wiki 不作为权威源。

精确数字、运行身份和“能/不能声称”的范围只从结果目录 README、CSV/JSON/manifest 和
[`experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)
引用。

## 运行与维护

- 新机器、容器、GPU、依赖、模型或数据任务必须先读 `deploy/runtime/AGENTS.md` 和
  `deploy/runtime/README.md`，运行只读 preflight 后再安装、下载或实验。
- 正式 baseline 必须由被测系统拥有执行和调度；项目适配只负责 source、sink、质量审计和指标。
- 历史实验 raw 和失败证据不因“过时”删除。确认被替代的文档保留在历史层并写明替代入口。
- Git 只保存可复现源、必要证据和轻量汇总；本地环境、模型、raw workload、缓存和临时产物不提交。
- 结构、方向、实验结论或关键入口变化必须同步 `PROJECT_LOG.md` 和受影响目录 README。

提交前至少运行：相关单元测试、Markdown 本地链接检查、`git diff --check` 和
`python code/scripts/environment/scan_git_secrets.py`。
