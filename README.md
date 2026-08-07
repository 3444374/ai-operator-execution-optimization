# 数据库 AI 负载执行优化与调度研究工作区

本工作区用于组织硕士论文 / 达梦实习课题材料。当前开题正式题目是：

> 数据库 AI 负载的执行优化与调度研究方向。

> **当前状态（2026-08-07）**：开题阶段先冻结统一的 AI Data Execution Layer 叙事和
> `opening/claim_matrix.md`，再补两组统一 database-E2E 文本三臂实验。文本
> `AI_COMPLETE` 与图像 `AI_EMBED/AI_CLASSIFY` 都是统一 work-unit、credit、routing
> 抽象的证据轨道。Daft/Ray 是实现与 baseline，cost estimator 是共同使能组件，
> state-aware 方法仍是待验证方案。两组实验完成后停止新增开题 baseline，转入四图、
> 报告/PPT 和答辩审计；图像 A+B 工程主实验在开题材料冻结后恢复。

当前重点不是传统数据库 GPU 查询算子，也不是模型 kernel 优化。研究对象是数据库
触发后的外部链路：数据读取与物化、代价估计与组织、准入/路由/提交、模型执行、观测
和写回。Daft 是数据引擎，Ray actor 是可控执行机制，vLLM/CLIP 等是模型执行后端，
PostgreSQL + pgvector 是 source/sink 工程 baseline。

后续真实端到端实验平台优先使用公司内部统一采用的 PostgreSQL 18.3；当前 PG18.4 本地同构预演只能作为平台暂不可用时的替身。

AutoDL 双 GPU 远端实验的新对话入口固定为：
`PROJECT_INDEX.md`“要在 AutoDL 远端继续实验” →
`deploy/autodl/README.md`“新对话 / 新 agent 的唯一操作入口”。全新实例环境
准备、每次开机恢复、服务门禁、64 行 gate、正式后台运行和 `--resume`
恢复均以该 runbook 为单一来源，不从历史聊天重新推断。

Baseline / benchmark 不再从多份旧计划拼接：统一从
`experiments/plans/baseline_reference.md` 选择比较层级、原生 arm、证据等级和指标；
再进入文本或图像专项执行合同。`experiments/plans/archive/` 与 `code_doc/` 只用于追溯
历史设计，不能覆盖当前门禁和实验顺序。

## 目录结构

```text
.
├── AGENTS.md                         # 项目级长期规则
├── CLAUDE.md                         # Claude Code 环境入口
├── PROJECT_INDEX.md                  # 文件索引和阅读顺序
├── PROJECT_OUTLINE.md                # 项目总纲（题目、研究内容、关键证据、优先级）
├── PROJECT_LOG.md                    # 项目级简要操作日志
├── README.md                         # 本文件
├── overview/                         # 项目总览、当前路线
│   ├── AGENTS.md
│   ├── README.md
│   └── current_direction_and_plan.md
├── research/                         # 背景调研、文献依据（第一入口：knowledge_hub.md）
│   ├── AGENTS.md
│   ├── README.md
│   ├── knowledge_hub.md
│   ├── vllm_continuous_batching_reference.md
│   ├── ray_actor_dynamic_batching_reference.md
│   ├── daft_ray_multimodal_reference.md
│   ├── inference_pipeline_interaction_literature.md
│   ├── literature_and_evidence_review.md
│   ├── existing_ai_operator_execution_chains.md
│   ├── ai_operator_literature_inventory.md   # Top 15 + 核心补充 + 题录勘误
│   ├── top15_ranked_papers.md                # 项目最相关 Top 15 排序
│   ├── reading_notes/                        # 单篇精读笔记（49 篇）+ 模板
│   └── reference/                            # 已下载参考文献 PDF（67 个）+ 索引
├── motivation/                       # 动机场景、端到端测试
│   ├── AGENTS.md
│   ├── README.md
│   ├── benchmarks/                   # 动机测试脚本
│   │   ├── fake_embed_pipeline.py
│   │   ├── workload_matrix.py
│   │   ├── granularity.py
│   │   └── backpressure.py
│   ├── plans/                        # 场景设计、集成计划
│   │   ├── workloads.md
│   │   ├── integration.md
│   │   └── ai_sql_surface.md
│   └── results/                      # 动机测试结果
│       ├── README.md
│       ├── fake_cpu/                 # CPU/fake 历史预研
│       ├── cpu/                      # CPU baseline 对照
│       ├── gpu/                      # GPU-backed E2E 主动机结果
│       └── pg18_4_fake/             # PG18.4 同构预演
├── feasibility/                      # 可行性验证（组件、环境、脚本）
│   ├── AGENTS.md
│   ├── README.md
│   ├── benchmarks/                   # 组件级 microbenchmark 脚本
│   └── results/                      # 连接验证、smoke、dry-run CSV
│       ├── README.md
│       ├── pg18_4_connection_validation.md
│       ├── pgai_sql_smoke_20260714.md
│       └── trigger_surface_validation_20260714.md
├── experiments/                      # 正式研究实验（方法有效性验证）
│   ├── AGENTS.md
│   ├── README.md
│   ├── plans/
│   └── results/
├── code/                             # 可复用工程代码
│   ├── AGENTS.md
│   ├── README.md
│   ├── src/
│   │   ├── data/                     # source、materializer、sink、workload
│   │   ├── planning/                 # 纯代价估计与 work-unit packing
│   │   ├── scheduling/               # 组织、准入、routing、Ray runtime
│   │   ├── serving/                  # completion/embedding backend、vLLM probe
│   │   ├── modalities/{text,image}/  # 文本/图像专属语义，不复制 scheduler
│   │   ├── observability/            # metrics、profiler、trace
│   │   ├── baselines/{common,text,image}/
│   │   ├── experiments/              # calibration、scenario、shared-vLLM
│   │   └── infrastructure/           # config/profile/assets、runtime env、runner lease
│   ├── scripts/{data,services,baselines,profiling,experiments,analysis,environment}/
│   ├── tests/                        # 按生产域镜像；含架构边界测试
│   ├── configs/                      # vendor pin 与可复现配置
│   └── requirements.txt
├── code_doc/                         # 自动生成的代码文档（辅助）
├── data/                             # 本地 workload 数据（raw 被 git ignore）
├── deploy/                           # 本地 Docker 与 AutoDL 部署/runbook
│   ├── runtime/                      # 跨机器 profile、依赖能力和模型/数据资产合同
│   ├── autodl/
│   ├── pgai/
│   └── postgres18.4/
├── figures/                          # 项目级图资产
│   ├── AGENTS.md
│   ├── README.md
│   ├── architecture/
│   ├── data/report_main/
│   ├── data/backup/
│   ├── audit/
│   ├── learning/
│   └── scripts/
├── learning/                         # 学习讲解材料
│   ├── AGENTS.md
│   ├── README.md
│   └── experiment_walkthrough.md
├── opening/                          # 开题材料
│   ├── AGENTS.md
│   ├── README.md
│   ├── report/opening_report.md
│   ├── slides/
│   ├── feishu/
│   └── literature/                   # reading_list.md + top15_reading_notes/（精读全集在 research/）
├── projects/                         # PPT 项目工程文件
└── notes/                            # 沟通记录、待确认问题
    ├── AGENTS.md
    └── communication_notes.md
```

## 当前证据

根 README 只保留结论边界；精确数字以结果目录的 CSV、manifest 和 README 为准，
汇总入口见 `PROJECT_OUTLINE.md` §当前最重要证据与
`experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`。

- **文本 feeding 已闭合**：同协议项目链路达到 direct control 的约 97.7%；固定
  token-aware active-work 65,536/endpoint 达当前最大吞吐的 97.8%，是简单强静态点。
- **复杂动态策略尚未普遍胜出**：AIMD/PID/EWMA、动态 flush、service quantum 和
  多 actor 多数未超过预注册的约 5% 晋级门槛；不能因“动态”命名就声称更优。
- **文本策略具有 regime 依赖**：2-endpoint KV 无压力时多数数据组织策略接近；
  4-endpoint KV 饱和时排名和 prefix-cache 行为明显分化。相关结论不能脱离 endpoint/
  KV 条件外推。
- **图像瓶颈仍需逐阶段判定**：CLIP 画像支持 CPU preprocess 是候选限制，但尚未证明
  PCIe/H2D 是主瓶颈。项目静态 staged 路径优于项目自写 fused UDF 只构成动机证据。
- **原生 baseline 身份已收紧**：图像 Daft built-in、Ray Data native graph 和固定
  upstream vendor code 才进入正式 baseline；项目自写 Daft fused/staged UDF 只作诊断。
  256 图资源/正确性 gate 与 Daft built-in/project 逐行 embedding parity 已通过，独立
  calibration 与正式重复尚未完成。
- **可运行性验证**：最新代码在 AutoDL 完整依赖环境通过 679/679 单测；文本 512 行
  双 endpoint、图像 256 行 Daft/Ray Data correctness gate、两条默认无 capture 路径均
  跑通。它们是 smoke，不是论文性能排名。

早期 CPU/fake 结果仅作历史参考；PG18.4 AutoDL rehearsal 不能冒充 PostgreSQL 18.3
内部平台结论。

## 近期目标

当前执行顺序以 `opening/claim_matrix.md` 和
`experiments/plans/experiment_status_and_gaps.md` 顶部的 2026-08-07 开题冻结优先级为准：

1. 冻结题目、系统抽象、Claim Matrix 和不能声称边界。
2. 冻结并运行 SQuAD short-answer 三臂统一 database-E2E，1 warmup + 3 formal。
3. 冻结并运行一个 ShareGPT controlled-skew 三臂实验，完成后停止增加开题 baseline。
4. 用现有正式结果生成 serving capacity、work-aware 组织、图像 matched-resource、
   cost-model decision quality 四组核心图。
5. 重构同步总纲、开题报告和 PPT，完成逐页 claim 绑定与答辩攻击面审计。

开题前不做第二数据库、文本 Daft/Ray Data 全矩阵、multi-job 五 baseline、TPC-H cost
planning 或完整 scale×concurrency grid。图像 state-aware A+B、统一 pgvector system-E2E
和 held-out robustness 均进入开题后的论文实验 backlog。

写回使用 PostgreSQL + pgvector（COPY + deferred index），不作为独立实验阶段。

当前 CLI 入口已按职责放在
`code/scripts/{data,services,baselines,profiling,experiments,analysis,environment}/`；
不要继续使用重构前的扁平脚本路径。具体命令见 `code/scripts/README.md`、
`deploy/autodl/README.md` 与对应实验计划。

动机测试正式结果和分析优先看：

```text
motivation/results/README.md
motivation/results/gpu/README.md
```

项目总纲和最新证据见：

```text
PROJECT_OUTLINE.md
```
