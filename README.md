# 数据库 AI 负载执行优化与调度研究工作区

本工作区用于组织硕士论文 / 达梦实习课题材料。当前开题正式题目是：

> 数据库 AI 负载的执行优化与调度研究方向。

当前重点不是传统数据库 GPU 查询算子，也不是模型 kernel 优化。数据库 AI 算子在本项目中主要作为 workload 入口和验证场景。主场景为 `AI_COMPLETE`（生成式 LLM 推理），经由 Daft/Arrow 数据组织、Ray 动态 batching（异构 actor pool）、GPU 推理引擎、最终写回数据库 sink。研究重点是上游 Ray 数据执行层的数据组织策略和调度提交控制。`AI_EMBED` 已完成真实 GPU-backed 预研验证，用于支撑实验框架可行性。

后续真实端到端实验平台优先使用公司内部统一采用的 PostgreSQL 18.3；当前 PG18.4 本地同构预演只能作为平台暂不可用时的替身。

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
│   ├── ai_operator_literature_inventory.md   # 66 篇文献清单
│   ├── top15_ranked_papers.md                # 项目最相关 Top 15 排序
│   ├── reading_notes/                        # 单篇精读笔记（33 篇）+ figs/
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
│   ├── scripts/
│   │   ├── README.md
│   │   ├── postgres_ai_operator_profile.py
│   │   ├── pgai_sql_operator_profile.py
│   │   └── local_embedding_server.py
│   └── requirements.txt
├── code_doc/                         # 自动生成的代码文档（辅助）
├── data/                             # 本地 workload 数据（raw 被 git ignore）
├── deploy/                           # Docker 部署配置
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

正式论证优先引用（详见 `PROJECT_OUTLINE.md` §当前最重要证据）：

- ✅ **vLLM + Qwen2.5-1.5B AI_COMPLETE baseline**（2026-07-18/19 已建立）：固定行 batch=8 时 token 跨度 13.9×；token-budget vs 固定行对照；shared-vLLM K_max 干扰（bulk unbounded 时前景 E2E 恶化 2.3×）。详见 `experiments/results/local_vllm_qwen15b_baseline/`。
- ✅ **AI_EMBED 真实 GPU-backed 预研**（已完成）：1024 行下 fine/coalesced 端到端约 `13.4x`；双 endpoint 下 Ray task/actor 体现并发 routing 价值，端到端收益仍受 writeback 约束。详见 `motivation/results/gpu/`。
- pgvector(384) 写回 0.897s vs JSON text 1.567s。
- 早期 CPU/fake 实验保留在 `feasibility/benchmarks/` 与 `motivation/results/fake_cpu/` 仅作历史参考。

**下一步**：改进 queue-adaptive 控制器（在 shared-vLLM 下超越静态 K_max=8）+ 两项策略联合消融。详见 `PROJECT_OUTLINE.md` §近期优先级 和 `experiments/plans/experiment_status_and_gaps.md`。

当前更值得继续验证的候选优化对象是：

> 数据库 AI 负载进入 Daft/Arrow 数据组织层、Ray task/actor 执行层、GPU-backed 模型服务和 Lance / pgvector / PostgreSQL sink 后的分布式执行与存储协同问题。

正式论证优先引用 `motivation/results/gpu/` 下的 GPU-backed 结果，PG18.4 / fake / CPU 结果只能作为预研背景和工具解释。

## 近期目标

vLLM baseline 与 Daft 文本阶段接入已完成（见上"当前证据"）。当前缺口（详见 `experiments/plans/experiment_status_and_gaps.md`）：

1. **P0**：改进 queue-adaptive 控制器，在 shared-vLLM 下超越静态 K_max=8（3 轮改进未超越则研究内容二降级）；两项策略联合消融（独立最优拼接 vs 联合 grid search，判定分层独立优化是否足够）。
2. **P1**：Prefix 受控 workload（prefix ratio 0/30/70/100%）+ 至少一个实验 scale 到 2048 行；研究内容一/二完整消融 + Daft 引擎级参数。
3. **P2（P0+P1 完成后）**：多模态泛化验证（CLIP embedding，同一套策略代码）；算子代价估计（补充讨论，不新增实验）。
4. 后续进入 PostgreSQL 18.3 内部平台复测，避免把 PG18.4 本地预演写成正式平台结论。

写回使用 PostgreSQL + pgvector（COPY + deferred index），不作为独立实验阶段。

当前主实验入口在 `code/scripts/`（vLLM / K_max / token-budget 等运行脚本）和 `experiments/results/local_vllm_qwen15b_baseline/`；早期 fake/CPU 管道 `motivation/benchmarks/fake_embed_pipeline.py` 仅作历史参考，不再是主运行命令。具体脚本与参数见 `code/scripts/README.md` 和 `experiments/plans/`。

动机测试正式结果和分析优先看：

```text
motivation/results/README.md
motivation/results/gpu/README.md
```

项目总纲和最新证据见：

```text
PROJECT_OUTLINE.md
```
