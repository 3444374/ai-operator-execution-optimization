# 数据库 AI 负载执行优化与调度研究工作区

本工作区用于组织硕士论文 / 达梦实习课题材料。当前开题正式题目是：

> 数据库 AI 负载的执行优化与调度研究方向。

> **当前状态（2026-08-03）**：研究内容仍是数据组织与提交控制；文本
> `AI_COMPLETE + vLLM` 保留为已建立的主证据轨道，当前工程验证优先推进图像
> `AI_EMBED/AI_CLASSIFY`，检验同一套 work-unit、credit、routing 与观测抽象能否跨模态
> 复用。图像正式 baseline 必须运行框架内置函数、官方 API graph 或固定 upstream
> vendor code；项目自写 Daft UDF 只作为诊断参考。最终题目是否采用“数据库↔GPU 经
> Daft 桥接”的外部 framing 仍待导师确认，不影响当前实现顺序。

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
- **可运行性验证**：最新代码在 AutoDL 完整依赖环境通过 624/624 单测；文本 512 行
  双 endpoint、图像 256 行 Daft/Ray Data correctness gate、两条默认无 capture 路径均
  跑通。它们是 smoke，不是论文性能排名。

早期 CPU/fake 结果仅作历史参考；PG18.4 AutoDL rehearsal 不能冒充 PostgreSQL 18.3
内部平台结论。

## 近期目标

当前执行顺序以 `experiments/plans/experiment_status_and_gaps.md` §0 为准：

1. 完成图像 R0→R4 表示/传输阶梯，明确 source、decode/preprocess、host copy、H2D、
   GPU forward、D2H、写回和未归因等待，不能先验指定 PCIe 为瓶颈。
2. 分别校准 Daft built-in、Ray Data native graph、官方 ResNet18 vendor code、bounded
   direct、vLLM pooling、naive 与 frozen project static，再做同硬件、同质量、同计时边界
   的稳态交错重复；项目自写 diagnostic 不进入 native baseline 主排名。
3. 给正式系统臂接统一 PostgreSQL + pgvector sink，补完整 system-E2E、质量门禁与资源
   账本。AI_CLASSIFY 报 accuracy/F1/mAP；embedding 正确性先用 digest/norm，检索任务再
   报 Recall@K、MRR/nDCG。
4. 只有 workload 变化会让最佳静态点稳定分离且错配代价约超过 5%，才继续复杂动态
   控制；否则保留固定 token/frame-aware credit。多 job 异质竞争单独验证 shared credit、
   idle borrowing、JCT/SLO 与公平性。
5. 文本遗留 formal 保持 `parked-conditional`；需要进入论文时按新的原生 baseline 与
   provenance 合同复测。

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
