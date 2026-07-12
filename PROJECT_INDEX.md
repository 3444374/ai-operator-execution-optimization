# PROJECT_INDEX.md

本文件是项目索引，供 Codex 快速定位材料。先读 `AGENTS.md`，再按任务类型读本文件中的对应材料。

## 1. 快速阅读顺序

### 只想了解当前课题

1. `AGENTS.md`：长期规则、用户目标、当前确定方向。
2. `README.md`：项目概览和目录结构。
3. `overview/current_direction_and_plan.md`：当前技术路线、论文边界、近期任务。
4. `feasibility/results/current_direction_analysis.md`：基于实验结果的当前方向分析。

### 要继续做实验

1. `AGENTS.md`：实验规则。
2. `motivation/README.md`：端到端动机实验、GPU-backed 外部链路系统画像和结果入口。
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
2. `learning/experiment_walkthrough.md`：按实验时间线解释 Ray/Daft/Lance、Arrow、pgvector、batch、task/actor、fan-in、backpressure、writeback 等术语和实验目的。
3. 再回到 `motivation/results/` 和 `feasibility/results/` 阅读正式报告。

## 2. 核心文件地图

| 文件 | 内容 | 什么时候读 |
|---|---|---|
| `AGENTS.md` | 项目长期规则、用户真实目标、当前选题边界 | 每次开始任务先读 |
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
| `learning/AGENTS.md` | 学习材料目录规则 | 修改学习讲解材料前读 |
| `learning/README.md` | 学习材料目录说明 | 想从小白视角理解项目和实验时读 |
| `learning/experiment_walkthrough.md` | 按时间线讲解已完成实验、术语、目的、流程和结果 | 看不懂实验报告、需要系统学习时读 |
| `feasibility/results/pg18_4_connection_validation.md` | PG18.4 本地连接与环境验证，只证明系统能连接数据库、读写表和完成冒烟链路 | 判断数据库连接进度时读 |
| `motivation/results/pg18_4_fake/system_profile.md` | PG18.4 本地同构链路 4096 行系统画像实验，含 python/ray_actor 与 fine/coalesced 对照 | 判断 PG18.4 系统瓶颈、外部链路 baseline、真实模型服务边界和写回瓶颈时读 |
| `notes/AGENTS.md` | 沟通材料规则 | 整理导师/企业侧反馈时读 |
| `notes/communication_notes.md` | 和同事/导师需要确认的问题和沟通话术 | 准备沟通 |

## 3. 实验大纲在哪里

实验大纲文件：

`feasibility/guide.md`

主要内容：

- 当前验证目标；
- 已完成 fake/CPU 实验；
- fake `AI_EMBED(text)` 历史预研测试设计；
- fine/coalesced object 对比指标；
- 当前路线继续推进的判定标准；
- 动机实验报告模板。

补充分析文件：

`feasibility/analysis.md`

主要内容：

- 已完成实验的阶段性结论；
- fake `AI_EMBED(text)` 历史预研假设；
- 当前路线是否继续成立；
- 风险和近期执行顺序。

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

> 面向数据库内置 AI 算子的分布式数据处理执行链路优化研究

候选技术切入点：

> 基于 Ray/Daft/Lance 类外部执行链路的数据库 AI 算子批处理系统调优

候选升级表述：

> 面向数据库 AI 算子的特征感知并行执行与外部链路跨层调度方法研究

具体优化方向尚未最终锁定。当前必须先用生产式 GPU-backed E2E profile 建立主动机：真实数据库 AI 算子触发后，外部执行链路是否产生足够严重、可分解、可优化的损耗。Object/fan-in/coalescing 是已有证据支持的机制入口；task/actor 并行度、外部 worker 形态、模型服务路由、backpressure 和 writeback 是下一步需要验证的扩展轴。GPU-backed model service 是主实验的真实计算端点；GPU/数据库内执行迁移、GPU kernel 和 CPU-only 链路只作为背景或少量必要 baseline，不作为主攻方向。

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

- 已补充 granularity attribution、backpressure 和真实 GPU-backed embedding 链路拆分实验。下一步优先补 384 维 pgvector 写回、多 endpoint / Ray Serve / vLLM 形态、主控 fan-in 写回 vs 多 worker 写回，并继续用同一套阶段计时回答“为什么外部链路值得优化”；
- 消融实验优先在同一条 GPU-backed E2E 链路上做大块对照；CPU/fake 链路只做脚本调试、计时边界验证和历史对照，不能把 CPU-only 分阶段瓶颈直接写成 GPU-backed 链路瓶颈；
- 把真实链路画像实验收敛到 PostgreSQL 18.3 内部验证平台：SQL/表触发、外部 worker、AI 算子执行、写回和指标采集必须真实跑通；
- 基于 `motivation/plans/workloads.md` 比较 3 个候选 AI 算子场景；
- 不把 3 个候选场景写成 3 个独立方向，而是围绕“数据库 AI 算子的特征感知并行执行与跨层调度”组织；
- 不把“粒度控制”写成全部贡献，而是评估是否能升级为“AI 算子特征感知并行执行与跨层调度”；
- 对候选场景继续做 task/object 解耦、task/actor/concurrency 控制、token-aware / prefix-aware batching、selectivity-aware predicate pipeline、resource/backpressure 等动机测试；
- 使用 `idea-evaluator` 的 fatal-flaws / 五维贡献视角评估方向；
- 使用 `deep-research` 的 scoping / Socratic 视角澄清研究问题和证据标准。

需要回答：

- 当前 RecordBatch fan-in 放大是否会迁移到端到端 AI_EMBED 链路；
- `N × P` object slots 在真实 GPU-backed AI 算子外部服务链路中是否仍导致明显成本；
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

开题工作规则见：

```text
opening/work_rules.md
opening/ppt_rules.md
opening/outline.md
```
