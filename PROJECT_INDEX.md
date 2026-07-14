# PROJECT_INDEX.md

> 图资产规则入口：新增、修改、迁移或审查图表前，先读 `figures/AGENTS.md`、`figures/README.md` 和 `figures/data/selected_motivation_figures.md`。其中 `figures/AGENTS.md` 记录做图 skill、系统架构图返工经验、Python 绘图条件和正式图质检清单。

本文件是项目索引，供 Codex 快速定位材料。先读 `AGENTS.md`，再按任务类型读本文件中的对应材料。

## 1. 快速阅读顺序

### 只想了解当前课题

1. `AGENTS.md`：长期规则、用户目标、当前确定方向。
2. `PROJECT_OUTLINE.md`：根目录项目总纲，汇总当前题目、研究内容、实验主线、关键证据和近期优先级。
3. `README.md`：项目概览和目录结构。
4. `opening/report/opening_report.md`：当前开题报告正文；其中的正式题目、研究内容和实验边界会反向约束后续项目规划。
5. `overview/current_direction_and_plan.md`：当前技术路线、论文边界、近期任务。
6. `feasibility/results/current_direction_analysis.md`：基于实验结果的当前方向分析。

阅读时要把 `opening/report/opening_report.md` 和 `overview/current_direction_and_plan.md` 视为同一方向的两个视角：前者面向开题论证，后者面向项目执行。二者不应长期存在题目、研究内容、实验优先级或边界口径冲突。

### 要继续做实验

1. `AGENTS.md`：实验规则。
2. `motivation/README.md`：端到端动机实验、GPU-backed 数据执行与存储链路系统画像和结果入口。
3. `feasibility/README.md`：可行性 benchmark、环境验证和结果入口。
4. `motivation/plans/integration.md`：PostgreSQL 18.3 内部平台、GPU-backed 外部模型服务和 AI 算子集成路线。
5. `feasibility/benchmarks/README.md`：实验脚本和运行命令。
6. `feasibility/results/README.md`：结果文件命名和报告生成方式。

### 要写调研/和导师沟通

1. `AGENTS.md`：沟通边界和不能声称什么。
2. `motivation/plans/ai_sql_surface.md`：数据库 AI 算子场景和动机测试方案。
3. `research/literature_and_evidence_review.md`：文献与官方资料依据。
4. `notes/communication_notes.md`：已有沟通问题和话术。

### 想从零理解实验和术语

1. `learning/README.md`：学习材料目录说明。
2. `learning/AGENTS.md`：实验结论、数据分析和术语讲解的精细度标准。
3. `learning/experiment_walkthrough.md`：按实验时间线解释 Ray/Daft/Lance、Arrow、pgvector、batch、task/actor、fan-in、backpressure、writeback 等术语和实验目的。
4. 再回到 `motivation/results/` 和 `feasibility/results/` 阅读正式报告。

## 2. 核心文件地图

| 文件 | 内容 | 什么时候读 |
|---|---|---|
| `AGENTS.md` | 项目长期规则、用户真实目标、当前选题边界 | 每次开始任务先读 |
| `PROJECT_OUTLINE.md` | 根目录项目总纲，汇总当前方向、实验主线、关键证据、近期优先级和同步规则 | 想快速确认当前主线和大纲时读 |
| `PROJECT_LOG.md` | 项目级简要操作日志 | 复盘跨目录调整、入口变更和方向同步时读 |
| `PROJECT_INDEX.md` | 文件索引和阅读顺序 | 不知道材料在哪里时读 |
| `README.md` | 工作区总览、当前确定方向、目录结构 | 了解项目背景 |
| `overview/AGENTS.md` | 总览目录规则 | 修改总纲、当前计划时读 |
| `overview/project_outline.md` | 当前总纲，说明 AI infra / 数据库 AI 算子定位 | 判断方向是否偏题 |
| `overview/current_direction_and_plan.md` | 技术路线、论文不能是什么、成立条件、近期任务 | 做方向判断、写计划 |
| `research/AGENTS.md` | 背景调研规则 | 写文献、资料依据时读 |
| `research/literature_and_evidence_review.md` | 文献与官方资料依据 | 写调研、论文动机时读 |
| `motivation/AGENTS.md` | 动机实验规则 | 搭建 AI 算子场景或端到端动机测试前读 |
| `motivation/README.md` | 动机目录动态入口、脚本索引、结果分流和常用结果入口 | 操作 `motivation/` 前读完 AGENTS 后继续读 |
| `motivation/plans/workloads.md` | AI infra 候选 AI 算子场景、动机测试和可优化点分析 | 比较候选场景、决定下一步测试时读 |
| `motivation/benchmarks/fake_embed_pipeline.py` | fake `AI_EMBED(text)` 历史预研脚本 | 仅用于追溯早期 task/object/fan-in 信号和调试计时边界 |
| `motivation/benchmarks/workload_matrix.py` | 三类候选 AI 算子场景动机测试脚本 | 比较 embedding、classify/filter、offline LLM 的瓶颈形态 |
| `motivation/benchmarks/granularity.py` | AI 算子粒度归因动机测试脚本 | 拆分 task 数、Ray refs、fan-in refs、operator invocation 的影响 |
| `motivation/benchmarks/backpressure.py` | AI 算子模型服务反压模拟脚本 | 验证 queue wait、token backlog、in-flight 请求和 backpressure 动机 |
| `motivation/results/` | 动机测试正式结果和分析，按 `gpu/`、`pg18_4_fake/`、`fake_cpu/` 分层 | 讲解动机测试、写开题动机、整理实验结论时优先读 |
| `motivation/plans/ai_sql_surface.md` | 数据库 AI 算子现状、推荐业务场景、动机测试标准 | 搭建业务场景前读 |
| `motivation/plans/integration.md` | 无设备/低端设备/PostgreSQL 18.3 平台的外部执行链路集成与测试方法 | 规划集成和测试时读 |
| `feasibility/AGENTS.md` | 可行性验证规则 | 写 benchmark 或实验结果前读 |
| `feasibility/README.md` | feasibility 目录动态入口、benchmark/结果分流和常用验证入口 | 操作 `feasibility/` 前读完 AGENTS 后继续读 |
| `code/AGENTS.md` | 正式工程代码规则 | 后续迁移可复用代码前读 |
| `code/scripts/postgres_ai_operator_profile.py` | PostgreSQL 18.3 / 同构 PostgreSQL AI 算子外部执行链路画像脚本 | 真实采集数据库触发、Ray/Arrow、AI operator、fan-in/writeback、backpressure 时读 |
| `code/scripts/README.md` | PG 连接、画像函数映射、运行命令和结果位置 | 查找数据库连接与测试流程代码时读 |
| `code/requirements.txt` | 真实数据库画像脚本的额外 Python 依赖 | 准备 PostgreSQL 18.3 / 同构实例前读 |
| `deploy/postgres18.4/AGENTS.md` | 本地 PostgreSQL 18.4 同构预演环境规则 | 修改或运行数据库容器前读 |
| `deploy/postgres18.4/README.md` | PostgreSQL + pgvector 的启动、连接和验证说明 | 运行本地真实数据库链路时读 |
| `deploy/pgai/AGENTS.md` | pgai SQL 算子触发面隔离预演环境规则 | 修改或运行 pgai / Ollama 容器前读 |
| `deploy/pgai/README.md` | pgai + Ollama 的启动、模型拉取和 SQL 冒烟验证说明 | 验证真实 SQL 触发 `ai.ollama_embed(...)` 时读 |
| `feasibility/results/pgai_sql_smoke_20260714.md` | pgai SQL embedding 触发面与 pgvector 写回冒烟验证 | 判断真实 SQL 触发面是否已跑通时读 |
| `feasibility/results/trigger_surface_validation_20260714.md` | PG18.4 job-table 健康检查、pgai SQL 1024 行 profile 和触发面小对比 | 判断 job_table 与 pgai_sql 两种触发面是否都能跑通时读 |
| `learning/AGENTS.md` | 学习材料目录规则 | 修改学习讲解材料前读 |
| `learning/README.md` | 学习材料目录说明 | 想从小白视角理解项目和实验时读 |
| `learning/experiment_walkthrough.md` | 按时间线讲解已完成实验、术语、目的、流程和结果 | 看不懂实验报告、需要系统学习时读 |
| `figures/README.md` | 项目级图资产库说明，覆盖 learning、开题、中期汇报和论文图 | 查找、复用或新增图表时读 |
| `figures/data/selected_motivation_figures.md` | 开题动机图表筛选、A/B/C 图层级和使用边界 | 决定哪些实验图进入报告、PPT、飞书或备份页时读 |
| `feasibility/results/pg18_4_connection_validation.md` | PG18.4 本地连接与环境验证，只证明系统能连接数据库、读写表和完成冒烟链路 | 判断数据库连接进度时读 |
| `motivation/results/pg18_4_fake/system_profile.md` | PG18.4 本地同构链路 4096 行系统画像实验，含 python/ray_actor 与 fine/coalesced 对照 | 判断 PG18.4 预演链路、历史 baseline、真实模型服务边界和写回瓶颈时读 |
| `notes/AGENTS.md` | 沟通材料规则 | 整理导师/企业侧反馈时读 |
| `notes/communication_notes.md` | 和同事/导师需要确认的问题和沟通话术 | 准备沟通 |

## 3. 实验主线与证据入口在哪里

当前实验主线不再由 `feasibility/guide.md` 承担。该文件只记录早期组件可行性验证和 fake `AI_EMBED(text)` 预研思路，不能作为现在的实验大纲或开题论证主入口。

当前更可信的实验主线入口是：

| 文件 | 地位 | 主要用途 |
|---|---|---|
| `motivation/README.md` | 动机测试目录总入口 | 判断端到端动机实验、GPU-backed 系统画像和结果分层应该读哪里 |
| `motivation/plans/workloads.md` | workload 与实验设计主线 | 说明三类 AI 算子场景、为什么不能拆成三个独立方向、下一步动机测试优先级 |
| `motivation/plans/integration.md` | 真实链路集成路线 | 说明 PostgreSQL / 外部 worker / Ray / GPU model service / writeback 如何组织 |
| `motivation/results/README.md` | 动机测试结果总入口 | 给出当前 GPU、PG18.4 fake、fake/CPU 结果的阅读顺序和结论边界 |
| `motivation/results/gpu/README.md` | 真实 GPU-backed 结果入口 | 记录当前真实 embedding endpoint、链路拆分和 multi-endpoint Ray 动机测试 |
| `figures/README.md` | 项目级图表入口 | 为 learning、开题、中期汇报和毕业论文提供统一图资产 |
| `opening/report/opening_report.md` | 开题论证入口 | 将项目进展、研究内容、技术路线和可行性分析组织成正式报告 |

当前正式论证优先引用：

1. `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md`：真实 GPU-backed embedding 链路拆分，当前开题动机中最强证据。
2. `motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md`：双 endpoint 下 Ray task / actor 的初步动机测试。
3. `motivation/results/pg18_4_fake/system_profile.md`：PG18.4 本地同构 fake-model 历史画像，只作预演与历史信号。
4. `motivation/results/fake_cpu/analysis.md`：fake/CPU 历史预研，只用于解释早期为什么关注 task/object/invocation/fan-in/backpressure。

`feasibility/guide.md` 和 `feasibility/analysis.md` 的当前定位是：

- 记录组件级可行性验证、早期 benchmark 和环境验证思路；
- 说明哪些结果只能作为 feasibility evidence；
- 不承担当前实验大纲、开题主线或 GPU-backed 性能结论职责。

## 4. 实验代码在哪里

实验代码目录：

`feasibility/benchmarks/`

| 文件 | 作用 |
|---|---|
| `README.md` | benchmark 说明和运行命令 |
| `requirements.txt` | Python 依赖：Ray、NumPy、PyArrow |
| `common.py` | CSV 输出、表格打印、依赖检查等公共函数 |
| `ray_small_task_benchmark.py` | Ray 小 task 调度/执行开销 |
| `ray_object_transfer_benchmark.py` | bytes / numpy / Arrow object transfer |
| `arrow_recordbatch_serialization_benchmark.py` | Arrow RecordBatch IPC 序列化 |
| `shuffle_simulation_benchmark.py` | 本地 Python hash shuffle 模拟 |
| `ray_many_objects_benchmark.py` | 固定总数据量下 Ray many-object fan-in |
| `ray_arrow_fanout_fanin_benchmark.py` | Arrow RecordBatch 版 Ray `N upstream -> P downstream` fan-out/fan-in |
| `analyze_results.py` | 汇总 CSV 并生成可行性报告 |

现有 benchmark 当前都作为 fake/CPU 基线保留。`ray_arrow_fanout_fanin_benchmark.py` 是当前最贴近数据库 AI 算子链路的本地证据；`ray_many_objects_benchmark.py` 是 object 数量放大的基础证据；`shuffle_simulation_benchmark.py` 暂作为负结果/对照。

动机脚本位于 `motivation/benchmarks/`：

| 文件 | 作用 |
|---|---|
| `fake_embed_pipeline.py` | fake `AI_EMBED(text)` 历史预研 |
| `workload_matrix.py` | 三类候选 AI 算子场景对比 |
| `granularity.py` | task/object/fan-in/operator invocation 归因 |
| `backpressure.py` | 模型服务 queue wait / token backlog / in-flight 反压模拟 |

动机测试正式结果位于：

`motivation/results/`

其中 `motivation/results/fake_cpu/analysis.md` 是当前最完整的动机测试结果分析。

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

运行环境：

- 使用 `.venv`；
- 需要 conda 时使用项目内前缀环境，例如 `.conda/pg-ai-profile`，不要把 `.conda/` 作为结果版本化；
- Ray benchmark 在当前 macOS 沙箱中可能需要提权运行。

## 5. 实验结果在哪里

结果目录分工：

- `feasibility/results/`：组件级可行性 benchmark、环境验证、PG18.4 连接验证。
- `motivation/results/`：端到端动机测试、系统画像、瓶颈定位和可优化点分析。

讲解动机实验和系统画像时优先引用 `motivation/results/`。

写实验结论和分析实验数据时，精细程度参考 `learning/AGENTS.md`：需要说明实验目的、链路流程、参数含义、真实数据来源、结果读法、不能证明的内容、结论类型和下一步验证。正式报告可以更简洁，但不能省略结论边界。

| 文件 | 内容 |
|---|---|
| `README.md` | 结果文件命名和生成报告命令 |
| `ray_small_task.csv` | Ray small task 实验结果 |
| `ray_object_transfer.csv` | Ray object transfer 实验结果 |
| `arrow_serialization.csv` | Arrow RecordBatch serialization 结果 |
| `shuffle_simulation.csv` | 本地 shuffle simulation 结果 |
| `ray_many_objects.csv` | Ray many-object fan-in 结果 |
| `ray_arrow_fanout_fanin.csv` | Arrow RecordBatch fan-out/fan-in 结果 |
| `motivation/results/gpu/` | GPU-backed E2E 主动机结果；当前优先读 `ai_embed_chain_breakdown_20260712.md` |
| `motivation/results/pg18_4_fake/` | PG18.4 本地同构 fake-model 历史结果 |
| `motivation/results/fake_cpu/fake_embed_pipeline.csv` | fake `AI_EMBED(text)` 历史预研结果 |
| `motivation/results/fake_cpu/workload_matrix.csv` | embedding / classify-filter / offline LLM 三类 AI 算子场景历史结果 |
| `motivation/results/fake_cpu/granularity.csv` | task/object/fan-in/operator invocation 收益来源拆分结果 |
| `motivation/results/fake_cpu/backpressure.csv` | 模型服务 queue wait / token backlog / backpressure 模拟结果 |
| `feasibility_report.md` | 自动生成的前期可行性实验分析 |
| `current_direction_analysis.md` | 人工整理的当前方向分析 |

当前最强实验信号：

> `ray_arrow_fanout_fanin.csv` 显示：固定 `65536` 行、`128` 维 embedding 下，fine/coalesced 平均 fan-in 比约 `3.17x`。

## 6. 文献和资料依据在哪里

业务场景和动机测试文件：

`motivation/plans/ai_sql_surface.md`

主要内容：

- 现有数据库 AI 算子例子；
- AI 算子、数据库 AI 算子、模型 kernel、传统查询算子的区别；
- 推荐初步场景：批量 Embedding / RAG 数据准备；
- 最小原型设计；
- 要测的瓶颈；
- 动机测试判定标准。

集成与测试方法文件：

`motivation/plans/integration.md`

主要内容：

- 当前无设备约束；
- PostgreSQL 18.3 内部统一验证平台作为后续真实端到端实验平台；
- 已有或开源 AI 算子迁移/等价集成到 PostgreSQL 18.3 的路径；
- 本地预研 / 集成验证 / 正式画像 分阶段实验；
- AI_EMBED 算子集成形态；
- PostgreSQL + pgvector / 外部 worker / Daft + Ray 链路；
- 瓶颈与优化点映射。

文献审查文件：

`research/literature_and_evidence_review.md`

主要内容：

- Ray 论文和官方文档；
- Ray task/object anti-pattern；
- Daft Ray runner、partitioning、shuffle、join strategy；
- Spark partition / shuffle / AQE 类比；
- Arrow / Lance 论文背景；
- 本地实验和外部证据如何对应；
- 当前不能声称什么；
- 下一步严谨验证计划。

使用原则：

- 写调研、汇报、论文动机时优先引用该文件；
- 不要只引用本地 microbenchmark；
- 结论必须区分“文献/官方文档”“本地实验”“合理推断”“待确认”。

## 7. 当前方向边界

确定场景主线：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究

候选技术切入点：

> 基于 Daft/Ray/Lance 类系统机制的 AI 数据执行链路调优

候选升级表述：

> 面向数据库驱动 AI workload 的特征感知数据组织、并行执行与存储协同优化

具体优化方向尚未最终锁定。当前必须先用生产式 GPU-backed E2E profile 建立主动机：数据库驱动 AI workload 进入 Daft/Ray/Lance-like 数据执行链路后，数据组织、Ray 调度、GPU 模型服务和 Lance / 数据库写回是否产生足够严重、可分解、可优化的损耗。Object/fan-in/coalescing 是已有证据支持的机制入口；task/actor 并行度、Daft/Arrow batch、模型服务路由、backpressure 和 writeback 是下一步需要验证的扩展轴。GPU-backed model service 是主实验的真实计算端点；GPU/数据库内执行迁移、GPU kernel 和 CPU-only 链路只作为背景或少量必要 baseline，不作为主攻方向。

当前不要优先做：

- 改造整个 Ray；
- 泛泛 Daft + Ray + Lance 集成；
- 单纯 Arrow serialization 优化；
- 单纯数据库 GPU 查询算子优化；
- 大量投入 AI 算子 GPU 化或数据库内执行 baseline；
- 没有真实 workload 的 toy benchmark。

## 8. 下一步优先工作

已完成历史预研实验：

> fake `AI_EMBED(text)` 端到端预研测试，曾用于发现 RecordBatch / task / fan-in 粒度信号；这些结果不再作为真实 GPU-backed 链路的瓶颈归因。

下一步优先工作：

- 已补充 granularity attribution、backpressure 和真实 GPU-backed embedding 链路拆分实验。下一步优先补 384 维 pgvector 写回、多 endpoint / Ray Serve / vLLM 形态、主控 fan-in 写回 vs 多 worker 写回，并继续用同一套阶段计时回答“为什么数据执行与存储链路值得优化”；
- 已新增 `deploy/pgai/` 隔离预演环境，用于先跑通 pgai SQL embedding 触发面；该环境当前不是 PG18.4 结果，后续只作为替代 `ai_operator_jobs` 模拟触发的参考 baseline；
- `deploy/pgai/` 已完成 2026-07-14 SQL 冒烟验证：`ai.ollama_embed(...)` 可调用 Ollama `all-minilm` 并写入 `vector(384)`；该结果只证明 SQL surface 可用，不是性能结论；
- 已完成 2026-07-14 三步触发面验证：PG18.4 job-table 健康检查、pgai SQL 1024 行 profile、job_table vs pgai_sql 小规模可运行性对照；该对照不是性能结论；
- 消融实验优先在同一条 GPU-backed E2E 链路上做大块对照；CPU/fake 链路只做脚本调试、计时边界验证和历史对照，不能把 CPU-only 分阶段瓶颈直接写成 GPU-backed 链路瓶颈；
- 把真实链路画像实验收敛到 PostgreSQL 18.3 内部验证平台：SQL/表触发、Daft/Arrow batch、Ray 执行、GPU 模型服务、写回和指标采集必须真实跑通；
- 基于 `motivation/plans/workloads.md` 比较 3 个候选 AI 算子场景；
- 不把 3 个候选场景写成 3 个独立方向，而是围绕“数据库驱动 AI workload 的特征感知数据组织、并行执行与存储协同”组织；
- 不把“粒度控制”写成全部贡献，而是评估是否能升级为“AI workload 特征感知数据组织、并行执行与存储协同”；
- 对候选场景继续做 task/object 解耦、task/actor/concurrency 控制、token-aware / prefix-aware batching、selectivity-aware predicate pipeline、resource/backpressure 等动机测试；
- 使用 `idea-evaluator` 的 fatal-flaws / 五维贡献视角评估方向；
- 使用 `deep-research` 的 scoping / Socratic 视角澄清研究问题和证据标准。

需要回答：

- 当前 RecordBatch fan-in 放大是否会迁移到端到端 AI_EMBED 链路；
- `N × P` object slots 在真实 GPU-backed AI 数据执行链路中是否仍导致明显成本；
- object coalescing 是否稳定降低端到端耗时；
- 这个现象能否映射到 Daft join/groupby/repartition。
- 是否存在比 batch embedding / RAG 准备更贴合 AI infra / inference infra 的 AI 算子场景；
- PostgreSQL / 开源数据库生态里的离线 LLM 生成是否足够成熟，能否作为主 workload，还是只能作为 inference infra 扩展 workload；
- 最终优化方向应落在 object transfer/fan-in、batching、partition、task/actor 并行调度、推理服务调用/路由/backpressure、写回链路，还是 scan/filter/pushdown。

优先沟通问题：

- 达梦实际是否会使用 Ray/Daft/Lance 做数据库内置 AI 算子；
- 数据从数据库到外部执行链路的格式是什么；
- 真实 AI 算子是否批处理，是否涉及 join/groupby/repartition/embedding preprocessing；
- 为什么需要 Ray，而不是数据库内部线程池或普通服务。
## 开题材料入口

开题报告、开题汇报 PPT、飞书进度汇报、文献精读、答辩问答和材料同步日志统一放在：

```text
opening/README.md
```

当前开题题目：

```text
面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究
```

开题材料不是独立展示稿。报告中的三项研究内容：AI workload 感知的数据组织与批处理构造、GPU 推理服务状态感知的 Ray 并行调度与反压控制、面向 AI 数据流的结果汇聚与 Lance / 数据库持久化协同，应同步作为后续实验计划、PPT、飞书文档和 overview 规划的共同口径。

## 开题与项目规划同步规则

开题材料和项目规划必须双向同步：

- 项目进展、实验结果和后续规划是开题报告的事实来源。
- 开题报告一旦调整题目、研究内容、技术路线、创新点、实验边界或侧重点，就会反向影响项目的实验计划、优先级和对外表述。
- 后续做项目规划或实验设计时，不能只看旧的 `overview/` 或 `motivation/` 文档，也要检查当前开题报告是否已经收敛到新的研究口径。
- 后续修改开题报告时，必须检查项目级 `README.md`、本文件、`overview/current_direction_and_plan.md`、`motivation/plans/workloads.md`、`motivation/plans/integration.md`、`opening/README.md` 和 `opening/work_rules.md` 是否需要同步。
- 如果只是润色正文或调整格式，可以不改项目规划；如果改变研究问题、实验优先级、可行性结论或贡献边界，必须同步项目规划。

开题工作规则见：

```text
opening/work_rules.md
opening/ppt_rules.md
opening/outline.md
```

## 2026-07-14 latest GPU rerun entry

After pgai SQL trigger-surface validation, the latest local GPU-backed key rerun
is:

```text
motivation/results/gpu/pgai_integrated_key_rerun_20260714.md
motivation/results/gpu/ai_embed_pgai_integrated_key_20260714.csv
```

Use this entry for the newest batch granularity, full-chain writeback,
single/dual endpoint, and data-size timing evidence. The pgai SQL trigger
surface timing remains in `feasibility/results/` and should not be mixed into
GPU-backed performance conclusions.

The latest sink-mode follow-up is:

```text
motivation/results/gpu/pgvector_writeback_20260714.md
motivation/results/gpu/ai_embed_pgvector_writeback_20260714.csv
figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png
```

Use this entry for no writeback / JSON text / pgvector `vector(384)` comparison
in the same local GPU-backed Ray actor chain.
