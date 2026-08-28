# SemLoom

**A Database-Aware Execution and Scheduling Layer for AI Semantic Operators**

**面向数据库 AI 语义算子的工作量感知执行与多作业调度系统**

本仓库正在实现 SemLoom，并研究 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
DB-AIEL（Database-Aware AI Execution Layer）是架构层名称，不作为代码接口前缀。参考 Sema 一类数据库
原生语义算子系统，PostgreSQL 拥有 SQL、关系 child plan、snapshot、权限、语义计划和 query
lifecycle；数据库管理的有界数据流把规范化任务交给可替换的 Daft/Ray/vLLM/CLIP backend 执行。

当前状态（2026-08-28）：研究方向保持“两项研究内容 + 共同代价估计 + 多模态验证”，数据库集成
架构改为 Sema-like 中立语义算子核心。短期工程锁定 `REL_18_3`，先用 extension 验证 planner-visible
`SemMap` 与 query lifecycle；当前受限 `SELECT`、direct `INSERT ... SELECT`、typed
`open/drive/close` seam，以及单在途、同步的 Unix-domain socket（UDS）recording provider 已通过
PostgreSQL 18.3 功能测试。协议 v2 C/Python 分域 identity/payload/completion digest、长度帧、Unicode、lazy open、PostgreSQL-owned
`PROPAGATE_NULL`、per-drive scratch、编码前输入上限、UTF8 校验与可取消 nonblocking connect 已验证。
下一步先拆分 PG scan/pump 与 neutral provider port、recording/UDS adapters，再实现 exact `SemFilter`
和最小第二 semantic path。extension 能承载目标 LOTUS/Cortex semantic paths 时继续使用，只有已复现
阻断才增加最小 core patch；accepted-prefix、多在途和增量 SemLoom 在数据库语义资格之后实现。
数据库资格完成后优先做 IMLane-like batch 对照。Kalypso-like
dependency execution、`SemJoin`、fusion/AQE 等只作后续参考，不纳入当前排期。既有 profiler、manifest
和 GPU 实验仍是外部物理执行证据，不能改称已经实现数据库内算子。

## 先读什么

| 需求 | 权威入口 |
|---|---|
| 两分钟了解当前方向 | [`overview/current_direction_and_plan.md`](overview/current_direction_and_plan.md) |
| 核对题目、研究内容、证据等级和执行顺序 | [`PROJECT_OUTLINE.md`](PROJECT_OUTLINE.md) |
| 查找文件和阅读路径 | [`PROJECT_INDEX.md`](PROJECT_INDEX.md) |
| 核对项目长期规则和边界 | [`AGENTS.md`](AGENTS.md) |
| 核对系统名和领域术语 | [`CONTEXT.md`](CONTEXT.md) |
| 判断某项机制是否已实现、验证或淘汰 | [`experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| 继续 PostgreSQL AI 语义算子实现 | [`experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md`](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md) |
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
