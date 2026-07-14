# 数据库驱动 AI 工作负载执行与存储协同优化工作区

本工作区用于组织硕士论文 / 达梦实习课题材料。当前开题正式题目是：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

当前重点不是传统数据库 GPU 查询算子，也不是模型 kernel 优化。数据库 AI 算子在本项目中主要作为 workload 入口和验证场景，研究主体是数据进入 Daft/Arrow 数据组织层、Ray task/actor 执行层、GPU-backed 模型服务和 Lance / pgvector / PostgreSQL sink 后的分布式执行与存储协同问题。

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
│   ├── current_direction_and_plan.md
│   └── project_outline.md
├── research/                         # 背景调研、文献依据
│   ├── AGENTS.md
│   ├── README.md
│   ├── literature_and_evidence_review.md
│   └── existing_ai_operator_execution_chains.md
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
│   ├── guide.md
│   ├── analysis.md
│   ├── benchmarks/
│   │   ├── README.md
│   │   ├── common.py
│   │   ├── analyze_results.py
│   │   ├── ray_many_objects_benchmark.py
│   │   ├── ray_arrow_fanout_fanin_benchmark.py
│   │   └── ...（含早期排除性实验脚本）
│   └── results/
│       ├── README.md
│       ├── feasibility_report.md
│       ├── current_direction_analysis.md
│       └── ...（连接验证、smoke、dry-run CSV）
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
│   ├── outline.md
│   ├── report/opening_report.md
│   ├── slides/
│   ├── feishu/
│   └── literature/
├── projects/                         # PPT 项目工程文件
└── notes/                            # 沟通记录、待确认问题
    ├── AGENTS.md
    └── communication_notes.md
```

## 当前证据

已有本地实验显示：

- GPU-backed 真实 embedding 链路：1024 行下 fine/coalesced 端到端约 `13.4x`，16384 行下 operator 和 writeback 均为大块成本；
- 双 endpoint 下 Ray task/actor 开始体现并发 routing 价值，但端到端收益仍受 writeback 约束；
- pgvector(384) 写回：JSON text 与真实 vector 写回的成本对比已记录在 `motivation/results/gpu/pgvector_writeback_20260714.md`；
- pgai-integrated GPU-backed rerun 进一步验证了 batch granularity、stage breakdown 和 endpoint comparison。

早期 CPU/fake 实验（Ray small task、object transfer、Arrow serialization、shuffle simulation 等）保留在 `feasibility/benchmarks/` 中作为历史组件参考，不代表真实 GPU-backed 数据库 AI 算子链路瓶颈。

当前更值得继续验证的候选优化对象是：

> 数据库驱动 AI 工作负载进入 Daft/Arrow 数据组织层、Ray task/actor 执行层、GPU-backed 模型服务和 Lance / pgvector / PostgreSQL sink 后的分布式执行与存储协同问题。

正式论证优先引用 `motivation/results/gpu/` 下的 GPU-backed 结果，PG18.4 / fake / CPU 结果只能作为预研背景和工具解释。

## 近期目标

1. 补 384 维 pgvector 写回实验，比较 JSON text 与真实 vector 写回。
2. 比较 driver fan-in 写回、Ray worker 写回、vectorizer-like queue worker 写回。
3. 在真实 GPU-backed 链路中做 bounded/unbounded in-flight 对照。
4. 扩展到 `AI_FILTER/AI_CLASSIFY`，验证 selectivity-aware predicate pipeline。
5. 扩展到 `AI_COMPLETE`，验证 token-aware batching、prefix-aware routing 和 queue-aware backpressure。
6. 后续进入 PostgreSQL 18.3 内部平台复测，避免把 PG18.4 本地预演写成正式平台结论。

推荐运行命令：

```bash
python motivation/benchmarks/fake_embed_pipeline.py \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --embedding-dim 128 \
  --repeats 3 \
  --output motivation/results/fake_cpu/fake_embed_pipeline.csv
```

动机测试正式结果和分析优先看：

```text
motivation/results/README.md
motivation/results/gpu/README.md
```

项目总纲和最新证据见：

```text
PROJECT_OUTLINE.md
```
