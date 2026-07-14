# PROJECT_INDEX.md

本文件是项目索引，供 Codex 快速定位材料。先读 `AGENTS.md`，再按任务类型读本文件中的对应材料。

## 1. 快速阅读顺序

### 只想了解当前课题

1. `AGENTS.md`：长期规则、用户目标、当前确定方向。
2. `README.md`：项目概览和目录结构。
3. `PROJECT_OUTLINE.md`：当前题目、研究内容、关键证据、近期优先级。
4. `overview/current_direction_and_plan.md`：阶段性技术路线和计划。
5. `motivation/results/gpu/README.md`：真实 GPU-backed E2E 结果入口。

### 要继续做实验

1. `AGENTS.md`：实验规则。
2. `motivation/plans/workloads.md`：三类 AI 算子场景、动机测试和后续实验优先级。
3. `motivation/plans/integration.md`：PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线。
4. `feasibility/benchmarks/README.md`：组件 benchmark 脚本和运行命令。
5. `motivation/results/README.md`：动机测试正式结果阅读顺序和结论边界。
6. `motivation/results/gpu/README.md`：真实 GPU-backed E2E 结果入口。

### 要写调研/和导师沟通

1. `AGENTS.md`：沟通边界和不能声称什么。
2. `motivation/plans/ai_sql_surface.md`：数据库 AI 算子现状和 AI 算子 SQL 触发面分析。
3. `research/literature_and_evidence_review.md`：文献与官方资料依据。
4. `notes/communication_notes.md`：已有沟通问题和话术。

## 2. 核心文件地图

| 文件 | 内容 | 什么时候读 |
|---|---|---|
| `AGENTS.md` | 项目长期规则、用户真实目标、当前选题边界 | 每次开始任务先读 |
| `PROJECT_INDEX.md` | 文件索引和阅读顺序 | 不知道材料在哪里时读 |
| `PROJECT_OUTLINE.md` | 项目总纲：当前题目、研究内容、关键证据、近期优先级 | 快速了解最新进展 |
| `README.md` | 工作区总览、当前方向、目录结构 | 了解项目背景 |
| `overview/AGENTS.md` | 总览目录规则 | 修改总纲、当前计划时读 |
| `overview/project_outline.md` | 项目总纲（旧版） | 已被根 `PROJECT_OUTLINE.md` 替代，仅作历史参考 |
| `overview/current_direction_and_plan.md` | 阶段性技术路线和计划 | 了解阶段规划时读 |
| `research/AGENTS.md` | 背景调研规则 | 写文献、资料依据时读 |
| `research/README.md` | 调研目录入口 | 了解 research/ 下有什么 |
| `research/literature_and_evidence_review.md` | 文献与官方资料依据 | 写调研、论文动机时读 |
| `research/existing_ai_operator_execution_chains.md` | 现有数据库 AI 算子与 AI 数据处理执行链路对比 | 比较外部系统路线时读 |
| `motivation/AGENTS.md` | 动机实验规则 | 搭建 AI 算子场景或端到端动机测试前读 |
| `motivation/README.md` | 动机测试目录详细说明 | 了解 motivation/ 下有什么、怎么组织 |
| `motivation/plans/workloads.md` | 三类 AI 算子场景、动机测试和 idea-evaluator 评估 | 比较候选场景、决定下一步测试时读 |
| `motivation/plans/integration.md` | PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线 | 规划集成和测试时读 |
| `motivation/plans/ai_sql_surface.md` | 数据库 AI 算子现状、推荐业务场景、动机测试标准 | 搭建业务场景前读 |
| `motivation/benchmarks/fake_embed_pipeline.py` | fake `AI_EMBED(text)` 端到端动机测试脚本 | 验证 embedding / RAG 链路中的 fan-in 成本 |
| `motivation/benchmarks/workload_matrix.py` | 三类候选 AI 算子场景动机测试脚本 | 比较不同 AI 算子的瓶颈形态 |
| `motivation/benchmarks/granularity.py` | AI 算子粒度归因动机测试脚本 | 拆分 task/object/fan-in/invocation 的收益 |
| `motivation/benchmarks/backpressure.py` | AI 算子模型服务反压模拟脚本 | 验证 queue wait、token backlog、in-flight 和 backpressure |
| `motivation/results/README.md` | 动机测试结果阅读顺序和结论边界 | 讲解动机测试、整理实验结论时读 |
| `motivation/results/gpu/README.md` | 真实 GPU-backed E2E 结果入口 | 当前最优先引用的正式证据 |
| `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md` | GPU-backed embedding 链路拆分 | 引用 stage breakdown 和 fine/coalesced 对比 |
| `motivation/results/gpu/pgai_integrated_key_rerun_20260714.md` | pgai-integrated GPU-backed rerun | 引用最新 rerun 结果 |
| `motivation/results/fake_cpu/analysis.md` | fake/CPU 历史预研分析 | 了解早期为什么关注 task/object/invocation/fan-in/backpressure |
| `motivation/results/pg18_4_fake/` | PG18.4 本地同构预演 | 只作为预演和历史信号，不代表真实 GPU-backed 结论 |
| `feasibility/AGENTS.md` | 可行性验证规则 | 做组件 benchmark 或环境验证前读 |
| `feasibility/README.md` | 可行性验证目录入口 | 了解 feasibility/ 下有什么、怎么组织 |
| `feasibility/guide.md` | 早期组件可行性验证指南 | 不再作为当前实验主线大纲，仅作历史参考 |
| `feasibility/analysis.md` | 前期可行性分析和阶段性判断 | 了解早期排除性结论 |
| `feasibility/benchmarks/README.md` | benchmark 说明和运行命令 | 运行组件 benchmark |
| `feasibility/results/README.md` | 可行性结果索引 | 查看组件验证、连接验证、smoke 结果 |
| `experiments/AGENTS.md` | 正式研究实验规则 | 设计优化实验、消融实验前读 |
| `experiments/README.md` | 正式研究实验入口 | 了解三项研究内容的实验规划 |
| `figures/AGENTS.md` | 图表长期规则 | 做图、改图、审查图前必读 |
| `figures/README.md` | 图资产入口 | 查找正式图、备份图和绘图脚本 |
| `learning/AGENTS.md` | 学习讲解规则 | 写学习材料前读 |
| `learning/README.md` | 学习材料入口 | 了解实验 walkthrough 和术语讲解 |
| `learning/experiment_walkthrough.md` | 按推进顺序讲解已完成实验 | 学习实验链路、参数和结果读法 |
| `opening/AGENTS.md` | 开题工作规则 | 写开题报告、PPT、飞书材料前读 |
| `opening/README.md` | 开题工作区入口 | 了解开题材料分布和同步规则 |
| `opening/navigation.md` | 开题材料导航 | 不知道开题材料在哪时读 |
| `opening/report/opening_report.md` | 开题报告正文 | 写报告、和导师沟通、定方向 |
| `code/AGENTS.md` | 正式工程代码规则 | 后续迁移可复用代码前读 |
| `code/scripts/README.md` | 脚本详细说明 | 运行 PostgreSQL 画像、pgai SQL profile、本地 embedding server |
| `deploy/pgai/` | pgai Docker Compose 部署 | 启动 pgai 测试环境 |
| `deploy/postgres18.4/` | PostgreSQL 18.4 Docker Compose 部署 | 启动 PG18.4 同构预演环境 |
| `notes/AGENTS.md` | 沟通材料规则 | 整理导师/企业侧反馈时读 |
| `notes/communication_notes.md` | 和同事/导师需要确认的问题和沟通话术 | 准备沟通 |

## 3. 实验规划在哪里

全局项目路线和近期实验任务：

`PROJECT_OUTLINE.md`

主要内容：

- 当前开题题目、三项研究内容；
- 实验主线和当前最重要证据；
- 近期优先级；
- 双向同步规则。

动机测试的计划（场景设计、集成路线）：

`motivation/plans/workloads.md`：三类 AI 算子场景、动机测试和后续实验优先级。

`motivation/plans/integration.md`：PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线和分阶段实验。

可行性验证的参考（组件、环境、脚本可用性）：

`feasibility/guide.md`（历史参考），`feasibility/analysis.md`（阶段性判断）。

正式研究实验（方法有效性验证）：

`experiments/plans/`（按研究内容组织），`experiments/results/`（优化和消融结果）。

## 4. 实验代码在哪里

活跃可行性 benchmark：

`feasibility/benchmarks/`

| 文件 | 作用 |
|---|---|
| `README.md` | benchmark 说明和运行命令 |
| `requirements.txt` | Python 依赖：Ray、NumPy、PyArrow |
| `common.py` | CSV 输出、表格打印、依赖检查等公共函数 |
| `ray_many_objects_benchmark.py` | 固定总数据量下 Ray many-object fan-in |
| `ray_arrow_fanout_fanin_benchmark.py` | Arrow RecordBatch 版 Ray `N upstream -> P downstream` fan-out/fan-in |
| `analyze_results.py` | 汇总 CSV 并生成可行性报告 |

早期排除性实验（Ray small task、object transfer、Arrow serialization、shuffle simulation）保留在 `feasibility/benchmarks/` 中作为历史组件参考。这些实验证明了对应方向不是当前瓶颈，不代表真实 GPU-backed 数据库 AI 算子链路瓶颈。

动机测试脚本：

`motivation/benchmarks/`

| 文件 | 作用 |
|---|---|
| `fake_embed_pipeline.py` | fake `AI_EMBED(text)` 端到端链路 |
| `workload_matrix.py` | 三类候选 AI 算子场景对比 |
| `granularity.py` | task/object/fan-in 收益来源拆分 |
| `backpressure.py` | 模型服务反压离散事件模拟 |

动机测试正式结果位于：

`motivation/results/`

- `fake_cpu/`：CPU/fake 历史预研（仅作背景参考）
- `cpu/`：CPU baseline 对照
- `gpu/`：GPU-backed E2E 主动机结果（当前最优先引用）
- `pg18_4_fake/`：PG18.4 本地同构预演（不代表真实平台结论）

推荐命令：

```bash
python motivation/benchmarks/fake_embed_pipeline.py \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --embedding-dim 128 \
  --repeats 3 \
  --output motivation/results/fake_cpu/fake_embed_pipeline.csv
```

```bash
python feasibility/benchmarks/analyze_results.py \
  --results-dir feasibility/results \
  --output feasibility/results/feasibility_report.md
```

运行环境：

- 使用 `.venv`；
- 当前没有必要使用 conda；
- Ray benchmark 在 macOS 沙箱中可能需要提权运行。

## 5. 实验结果在哪里

可行性验证结果（组件、环境、连接）：

`feasibility/results/`

动机测试正式结果（唯一来源）：

`motivation/results/`

讲解动机实验时引用 `motivation/results/gpu/`（GPU-backed，最优先）或 `motivation/results/fake_cpu/`（历史预研）。`feasibility/results/` 仅保留组件 benchmark 结果和环境验证。

正式论证优先引用：

1. `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md`：GPU-backed embedding 链路拆分。
2. `motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md`：双 endpoint Ray task/actor 动机。
3. `motivation/results/gpu/pgai_integrated_key_rerun_20260714.md`：pgai-integrated GPU-backed rerun。
4. `motivation/results/gpu/pgvector_writeback_20260714.md`：pgvector(384) 写回对比。

## 6. 文献和资料依据在哪里

业务场景和动机测试文件：

`motivation/plans/ai_sql_surface.md`

主要内容：

- 现有数据库 AI 算子例子；
- AI 算子、数据库 AI 算子、模型 kernel、传统查询算子的区别；
- 推荐初步场景：批量 Embedding / RAG 数据准备；
- 最小原型设计；
- 瓶颈矩阵和动机测试判定标准。

集成与测试方法文件：

`motivation/plans/integration.md`

主要内容：

- PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线；
- 无设备/低端设备/PostgreSQL 18.3 平台的分阶段实验；
- AI_EMBED 算子集成形态；
- 瓶颈与优化点映射。

文献审查文件：

`research/literature_and_evidence_review.md`

主要内容：

- Ray 论文和官方文档；
- Daft Ray runner、partitioning、shuffle、join strategy；
- Spark partition / shuffle / AQE 类比；
- Arrow / Lance 论文背景；
- Snowflake / pgai / PostgresML / pgvector 外部系统依据；
- 本地实验和外部证据如何对应；
- 当前不能声称什么。

外部系统执行链路对比：

`research/existing_ai_operator_execution_chains.md`

使用原则：

- 写调研、汇报、论文动机时优先引用该文件；
- 不要只引用本地 microbenchmark；
- 结论必须区分"文献/官方文档""本地实验""合理推断""待确认"。

## 7. 当前方向边界

开题报告正式题目：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

三项研究内容：

1. AI workload 感知的数据组织与批处理构造方法。
2. GPU 推理服务状态感知的 Ray 并行调度与反压控制方法。
3. 面向 AI 数据流的结果汇聚与 Lance / 数据库持久化协同方法。

具体优化方向尚未最终锁定。当前必须围绕数据库驱动 AI 工作负载寻找真实瓶颈，通过动机测试、可行性测试和正式研究实验收敛方向。Object/fan-in/coalescing 是早期入口；task/actor 并行度、GPU 资源配比、模型服务路由与 backpressure、写回协同是正在验证的扩展轴。

当前不要优先做：

- 改造整个 Ray；
- 泛泛 Daft + Ray + Lance 集成；
- 单纯 Arrow serialization 优化；
- 单纯数据库 GPU 查询算子优化；
- 没有真实 workload 的 toy benchmark。
- 把 PG18.4 本地预演写成 PostgreSQL 18.3 内部平台结论。

## 8. 下一步优先工作

已完成优先实验：

- GPU-backed 真实 embedding 链路拆分和阶段画像；
- pgai-integrated GPU-backed rerun（granularity、stage breakdown、endpoint comparison）；
- pgvector(384) 写回对比（no writeback / JSON text / vector）。

下一步优先工作：

1. 在真实 GPU-backed 链路中做 bounded/unbounded in-flight 对照。
2. 比较 driver fan-in 写回、Ray worker 写回、vectorizer-like queue worker 写回。
3. 扩展到 `AI_FILTER/AI_CLASSIFY`，验证 selectivity-aware predicate pipeline。
4. 扩展到 `AI_COMPLETE`，验证 token-aware batching、prefix-aware routing 和 queue-aware backpressure。
5. 后续进入 PostgreSQL 18.3 内部平台复测。
6. 将优化方法和消融实验登记到 `experiments/`。

需要回答：

- 当前 PG18.4 + pgai GPU-backed 链路的瓶颈是否能迁移到 PostgreSQL 18.3 内部平台；
- writeback 路径（driver fan-in、worker-side、queue worker）哪种在真实场景中最可行且有效；
- 是否存在比 AI_EMBED 更贴合 AI infra / inference infra 的 AI 算子场景；
- 最终优化方向应落在数据组织、调度反压、写回协同三层中的哪些组合。

优先沟通问题：

- 达梦实际是否会使用 Ray/Daft/Lance 做数据库内置 AI 算子；
- 数据从数据库到外部执行链路的格式是什么；
- 真实 AI 算子是否批处理，是否涉及 join/groupby/repartition/embedding preprocessing；
- 为什么需要 Ray，而不是数据库内部线程池或普通服务。
