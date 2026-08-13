# 数据库 AI 负载执行优化与调度研究工作区

本工作区用于组织硕士论文 / 达梦实习课题材料。当前开题正式题目是：

> 数据库 AI 负载的执行优化与调度研究方向。

> **当前状态（2026-08-13）**：开题 framing、四级 Claim Matrix、K128 replacement
> database-E2E、文本原生单/多 Job 与图像静态 baseline 证据已冻结；PPT/飞书/Wiki 暂停，
> 本地 Markdown 与 CSV 是权威来源。图像原生 single→four-job 40/40 passed；Project
> staged descriptor + observe-only snapshot 正式矩阵也已 24/24 passed、99K formal rows
> exactly-once，snapshot 100% fresh 且构建均值 0.141 ms，但 static/proposed group JCT 只差
> 0.98%，不能写成 state-aware 胜出。SAOR capacity-only 未超过 K160/简单 threshold，
> dynamic K 已退出主线；fixed-envelope 2-Job formal 已 40/40，原始 failed gate 保留作审计。
> resolution-aware v2 已在完整 artifact 上重汇总为 passed、credit mechanism effective 12/12；SAOR 在
> credit 臂内 fg 最好、仍未越过 static。strict-priority release-only 两轮短测达到 fg P99 14.27s、
> SLO 0%，但仅是能力上界，尚非 formal/proposed。旧 single-head bounded-priority 双轮 GPU
> development gate 因 ready-backlog 不可见而未晋级；独立 bounded-ready 修订随后完成 8/8 cell，
> 仅 $0.125W_e$ 以约 12.36K tok/s、foreground P99 17.58–18.15s、foreground SLO 0% 通过开发门，
> $0.25W_e$ 被 bulk guard 拒绝。同 ready-window 的 Project FIFO/DRR/VTC-style/strict-priority/
> guarded-debt 双轮归因与 single-head→bounded-ready observation bridge 均已完成：guarded-debt
> 是用约 4.8% 吞吐和约 5.2% bulk JCT 换取更低 foreground tail 的观测非支配折中点，不是
> selector winner，`formal_authorized=false`。下一步只补同一 2-Job 合同的 Daft Native、Daft Ray、
> Ray Data、project frozen-static 与 proposed 系统级 matched comparison；原生臂不注入 Project
> bounded-ready/K/W。selector formal、4-Job、reservation 和 dynamic K 继续后置。

> **状态感知补充（2026-08-11）**：修正执行与门禁后的两 Job phase-change 实验在
> pressure gate 提前停止。A-only K160 相对 K128 每 endpoint service rate +7.77%，
> 但 B=2.5/3.5/4.5 均未稳定触发双 endpoint、双周期降档条件；未运行 action/formal，
> 不能据此判断动态策略有效或无效。完整边界见
> `experiments/results/phase_change_state_aware_corrected_early_stop_20260811/`。

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
│   ├── opening_figure_set/           # 开题专用图集：按页码命名的主讲、SVG、Draw.io 与备份图
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

- **统一文本三臂已闭合，但项目臂未过 feeding 门**：24/24 单元通过 source/sink 与
  exactly-once；project 在 SQuAD/ShareGPT 仅为 direct service tokens/s 的
  89.93%/91.38%。固定 active-work 65,536/endpoint 仍是历史校准签名下的最小近饱和点，
  但不能覆盖本轮统一 database-E2E 的负结果。
- **复杂动态策略尚未普遍胜出**：AIMD/PID/EWMA、动态 flush、service quantum 和
  多 actor 多数未超过预注册的约 5% 晋级门槛；SAOR capacity-only 开发门相对 K128
  +4.36%，但相对 K160 仅 +0.52%、相对简单 threshold −1.46%，同样未晋级。不能因“动态”
  命名就声称更优；K160 是强效率 baseline，但已有 Job B tail/Jain 代价。
- **SAOR fixed-envelope formal 给出有效但未晋级的权衡证据**：40/40、0 incident、exactly-once；SAOR
  12,393 tok/s、fg P99 50.3s，在 credit 臂内最好，但 static 以 9,508 tok/s 换得 fg P99
  29.2s 和 0% SLO violation。原始 gate 因 DRR/VTC rep2 无 post-drain 样本而 fail-closed；
  250 ms resolution-aware v2 已在完整 artifact 上重汇总为 passed，仅修审计假阴性、不改变排序。当前
  `slo_weight=0`；strict-priority 两轮短测虽显著改善 fg，但 hard priority 缺少 anti-starvation/lag guard，不能称 SLO-aware 或策略胜出。`saor-v0.5.1` 双轮 development gate 未晋级：0.25K 机制不稳定，两个 cap 的 fg P99/SLO 均未过门；根因收紧为 ready-set 只暴露单个 pull head。formal、4-Job 与 reservation 消融继续阻塞。
- **文本策略具有 regime 依赖**：2-endpoint KV 无压力时多数数据组织策略接近；
  4-endpoint KV 饱和时排名和 prefix-cache 行为明显分化。相关结论不能脱离 endpoint/
  KV 条件外推。
- **图像瓶颈是 CPU prepare 与提交链路的组合，不是 GPU 单指标问题**：60K×2 matched-resource
  正式结果和 host-path 筛选共同表明，增加 CPU actor/读取线程只有有限边际收益，PCIe/H2D
  也未被证明是主瓶颈。后续先显式拆出 ready-tensor 队列做两级 backpressure，再把
  derived-image cache 与 DALI mixed/GPU preprocess 作为独立 work-reduction 消融。
- **图像多 Job 与状态观测门已闭合**：Daft built-in/Ray Data 1+3 原生矩阵 40/40 passed，
  两条执行图都出现非均匀 Job slowdown；Project observe-only 矩阵 24/24 passed，snapshot
  构建成本约 0.141 ms，但总体 JCT 与 static 仅差 0.98%。它们证明跨模态 staged work/state
  观测可行，不证明动态调度胜出，也不构成跨框架绝对排名。
- **原生 baseline 身份已收紧**：图像 Daft built-in、Ray Data native graph 和固定
  upstream vendor code 才进入正式 baseline；项目自写 Daft fused/staged UDF 只作诊断。
  256 图资源/正确性 gate 与 Daft built-in/project 逐行 embedding parity 已通过，独立
  matched-resource 正式重复已完成；动态两级 broker 与 sink 质量闭环尚未完成。
- **可运行性验证**：最新代码在 AutoDL 完整依赖环境通过 679/679 单测；文本 512 行
  双 endpoint、图像 256 行 Daft/Ray Data correctness gate、两条默认无 capture 路径均
  跑通。它们是 smoke，不是论文性能排名。

早期 CPU/fake 结果仅作历史参考；PG18.4 AutoDL rehearsal 不能冒充 PostgreSQL 18.3
内部平台结论。

## 近期目标

当前执行顺序以 `opening/claim_matrix.md` 和
`experiments/plans/experiment_status_and_gaps.md` 顶部的 2026-08-07 开题冻结优先级为准：

1. 保持题目、系统抽象、四级 Claim Matrix 与开题 baseline 停止规则冻结。
2. 以 `opening/report/opening_report.md` 和 `opening/slides/opening_defense_20260807_v6.pptx`
   作为本地答辩材料；不因统一三臂负结果补跑新产品或 workload。
3. 在飞书用户授权恢复后覆盖同步线上报告并插入四张核心图；当前本地飞书源稿与报告完全一致。
4. 开题材料确认后恢复 image state-aware A+B、system database-E2E 与论文阶段 held-out。

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
