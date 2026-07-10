# PROJECT_INDEX.md

本文件是项目索引，供 Codex 快速定位材料。先读 `AGENTS.md`，再按任务类型读本文件中的对应材料。

## 1. 快速阅读顺序

### 只想了解当前课题

1. `AGENTS.md`：长期规则、用户目标、当前确定方向。
2. `README.md`：项目概览和目录结构。
3. `overview/current_direction_and_plan.md`：当前技术路线、论文边界、近期任务。
4. `validation/results/current_direction_analysis.md`：基于实验结果的当前方向分析。

### 要继续做实验

1. `AGENTS.md`：实验规则。
2. `motivation/ai_operator_integration_test_plan.md`：PostgreSQL 18.3 内部平台、低端设备小规模实验和 AI 算子集成路线。
3. `validation/feasibility_validation_guide.md`：Phase 0 实验大纲。
4. `validation/benchmarks/README.md`：实验脚本和运行命令。
5. `validation/results/README.md`：结果文件命名和报告生成方式。
6. `validation/benchmarks/analyze_results.py`：自动分析脚本。

### 要写调研/和导师沟通

1. `AGENTS.md`：沟通边界和不能声称什么。
2. `motivation/ai_operator_scenario_and_motivation_test.md`：数据库 AI 算子场景和动机测试方案。
3. `research/literature_and_evidence_review.md`：文献与官方资料依据。
4. `notes/communication_notes.md`：已有沟通问题和话术。

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
| `motivation/ai_infra_candidate_scenarios_and_motivation_tests.md` | AI infra 候选 AI 算子场景、动机测试和可优化点分析 | 比较候选场景、决定下一步测试时读 |
| `motivation/fake_ai_embed_pipeline_benchmark.py` | fake `AI_EMBED(text)` 端到端动机测试脚本 | 验证批量 embedding / RAG 链路中的 object fan-in 成本 |
| `motivation/ai_operator_scenario_motivation_benchmark.py` | 三类候选 AI 算子场景动机测试脚本 | 比较 embedding、classify/filter、offline LLM 的瓶颈形态 |
| `motivation/ai_operator_granularity_attribution_benchmark.py` | AI 算子粒度归因动机测试脚本 | 拆分 task 数、Ray refs、fan-in refs、operator invocation 的影响 |
| `motivation/ai_operator_backpressure_benchmark.py` | AI 算子模型服务反压模拟脚本 | 验证 queue wait、token backlog、in-flight 请求和 backpressure 动机 |
| `motivation/results/` | 动机测试正式结果和分析 | 讲解动机测试、写开题动机、整理实验结论时优先读 |
| `motivation/ai_operator_scenario_and_motivation_test.md` | 数据库 AI 算子现状、推荐业务场景、动机测试标准 | 搭建业务场景前读 |
| `motivation/ai_operator_integration_test_plan.md` | 无设备/低端设备/PostgreSQL 18.3 平台的集成与测试方法 | 规划集成和测试时读 |
| `validation/AGENTS.md` | 可行性验证规则 | 写 benchmark 或实验结果前读 |
| `code/AGENTS.md` | 正式工程代码规则 | 后续迁移可复用代码前读 |
| `code/scripts/postgres_ai_operator_profile.py` | PostgreSQL 18.3 / 同构 PostgreSQL AI 算子外部执行链路画像脚本 | 真实采集数据库触发、Ray/Arrow、AI operator、fan-in/writeback、backpressure 时读 |
| `code/scripts/README.md` | PG 连接、画像函数映射、运行命令和结果位置 | 查找数据库连接与测试流程代码时读 |
| `code/requirements.txt` | 真实数据库画像脚本的额外 Python 依赖 | 准备 PostgreSQL 18.3 / 同构实例前读 |
| `deploy/postgres18.4/AGENTS.md` | 本地 PostgreSQL 18.4 同构预演环境规则 | 修改或运行数据库容器前读 |
| `deploy/postgres18.4/README.md` | PostgreSQL + pgvector 的启动、连接和验证说明 | 运行本地真实数据库链路时读 |
| `validation/results/postgres18_local_environment_validation.md` | 本地 PG18/pgvector 已验证项与项目画像待办 | 判断数据库连接进度时读 |
| `notes/AGENTS.md` | 沟通材料规则 | 整理导师/企业侧反馈时读 |
| `notes/communication_notes.md` | 和同事/导师需要确认的问题和沟通话术 | 准备沟通 |

## 3. 实验大纲在哪里

实验大纲文件：

`validation/feasibility_validation_guide.md`

主要内容：

- 当前验证目标；
- 已完成 Phase 0 实验；
- fake `AI_EMBED(text)` 端到端动机测试设计；
- fine/coalesced object 对比指标；
- 当前路线继续推进的判定标准；
- 动机实验报告模板。

补充分析文件：

`validation/preliminary_feasibility_analysis.md`

主要内容：

- 已完成实验的阶段性结论；
- fake `AI_EMBED(text)` 待验证假设；
- 当前路线是否继续成立；
- 风险和近期执行顺序。

## 4. 实验代码在哪里

实验代码目录：

`validation/benchmarks/`

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

现有 benchmark 当前都作为 Phase 0 基线保留。`ray_arrow_fanout_fanin_benchmark.py` 是当前最贴近数据库 AI 算子链路的本地证据；`ray_many_objects_benchmark.py` 是 object 数量放大的基础证据；`shuffle_simulation_benchmark.py` 暂作为负结果/对照。

端到端动机脚本位于：

`motivation/fake_ai_embed_pipeline_benchmark.py`

候选 AI 算子场景对比脚本位于：

`motivation/ai_operator_scenario_motivation_benchmark.py`

动机补强脚本位于：

- `motivation/ai_operator_granularity_attribution_benchmark.py`
- `motivation/ai_operator_backpressure_benchmark.py`

动机测试正式结果位于：

`motivation/results/`

其中 `motivation/results/motivation_test_results_analysis.md` 是当前最完整的动机测试结果分析。

推荐命令：

```bash
python motivation/fake_ai_embed_pipeline_benchmark.py \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --embedding-dim 128 \
  --repeats 3 \
  --output validation/results/fake_ai_embed_pipeline.csv
```

运行环境：

- 使用 `.venv`；
- 当前没有必要使用 conda；
- Ray benchmark 在当前 macOS 沙箱中可能需要提权运行。

## 5. 实验结果在哪里

结果目录：

`validation/results/`

注意：动机测试正式结果已同步到：

`motivation/results/`

讲解动机实验时优先引用 `motivation/results/`，`validation/results/` 继续保留可行性 benchmark 和历史同步副本。

| 文件 | 内容 |
|---|---|
| `README.md` | 结果文件命名和生成报告命令 |
| `ray_small_task.csv` | Ray small task 实验结果 |
| `ray_object_transfer.csv` | Ray object transfer 实验结果 |
| `arrow_serialization.csv` | Arrow RecordBatch serialization 结果 |
| `shuffle_simulation.csv` | 本地 shuffle simulation 结果 |
| `ray_many_objects.csv` | Ray many-object fan-in 结果 |
| `ray_arrow_fanout_fanin.csv` | Arrow RecordBatch fan-out/fan-in 结果 |
| `fake_ai_embed_pipeline.csv` | fake `AI_EMBED(text)` 端到端动机测试结果 |
| `ai_operator_scenario_motivation.csv` | embedding / classify-filter / offline LLM 三类 AI 算子场景动机测试结果 |
| `ai_operator_granularity_attribution.csv` | task/object/fan-in/operator invocation 收益来源拆分结果 |
| `ai_operator_backpressure.csv` | 模型服务 queue wait / token backlog / backpressure 模拟结果 |
| `feasibility_report.md` | 自动生成的前期可行性实验分析 |
| `current_direction_analysis.md` | 人工整理的当前方向分析 |

当前最强实验信号：

> `ray_arrow_fanout_fanin.csv` 显示：固定 `65536` 行、`128` 维 embedding 下，fine/coalesced 平均 fan-in 比约 `3.17x`。

## 6. 文献和资料依据在哪里

业务场景和动机测试文件：

`motivation/ai_operator_scenario_and_motivation_test.md`

主要内容：

- 现有数据库 AI 算子例子；
- AI 算子、数据库 AI 算子、模型 kernel、传统查询算子的区别；
- 推荐初步场景：批量 Embedding / RAG 数据准备；
- 最小原型设计；
- 要测的瓶颈；
- 动机测试判定标准。

集成与测试方法文件：

`motivation/ai_operator_integration_test_plan.md`

主要内容：

- 当前无设备约束；
- PostgreSQL 18.3 内部统一验证平台作为后续真实端到端实验平台；
- 已有或开源 AI 算子迁移/等价集成到 PostgreSQL 18.3 的路径；
- Phase 0 / Phase 1 / Phase 2 分阶段实验；
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

> 基于 Daft/Ray/Lance 的 Object Transfer、fan-in 与 Shuffle 中间数据传输优化

候选升级表述：

> 面向数据库 AI 算子的特征感知并行执行与跨层调度方法研究

具体优化方向尚未最终锁定。当前必须围绕 AI 算子场景寻找真实瓶颈，通过动机测试、可行性测试和真实形态验证收敛方向，不能因为已有 benchmark 就反向确定论文主线。Object/fan-in/coalescing 是已有证据支持的入口；task/actor 并行度、CPU/GPU 资源配比、模型服务路由与 backpressure 是下一步需要验证的扩展轴。

当前不要优先做：

- 改造整个 Ray；
- 泛泛 Daft + Ray + Lance 集成；
- 单纯 Arrow serialization 优化；
- 单纯数据库 GPU 查询算子优化；
- 没有真实 workload 的 toy benchmark。

## 8. 下一步优先工作

已完成优先实验：

> fake `AI_EMBED(text)` 端到端动机测试，已把 RecordBatch fan-in 现象初步迁移到批量 Embedding / RAG 数据准备链路。

下一步优先工作：

- 拆分 fake `AI_EMBED(text)` 结果中的收益来源；
- 已补充 granularity attribution 与 backpressure 两个动机实验，下一步需要接 Ray actor / Ray Serve / vLLM 或真实模型服务验证；
- 把真实链路画像实验收敛到 PostgreSQL 18.3 内部验证平台：SQL/表触发、外部 worker、AI 算子执行、写回和指标采集必须真实跑通；
- 基于 `ai_infra_candidate_scenarios_and_motivation_tests.md` 比较 3 个候选 AI 算子场景；
- 不把 3 个候选场景写成 3 个独立方向，而是围绕“数据库 AI 算子的特征感知并行执行与跨层调度”组织；
- 不把“粒度控制”写成全部贡献，而是评估是否能升级为“AI 算子特征感知并行执行与跨层调度”；
- 对候选场景继续做 task/object 解耦、task/actor/concurrency 控制、token-aware / prefix-aware batching、selectivity-aware predicate pipeline、resource/backpressure 等动机测试；
- 使用 `idea-evaluator` 的 fatal-flaws / 五维贡献视角评估方向；
- 使用 `deep-research` 的 scoping / Socratic 视角澄清研究问题和证据标准。

需要回答：

- 当前 RecordBatch fan-in 放大是否会迁移到端到端 AI_EMBED 链路；
- `N × P` object slots 在 fake embedding + write path 中是否仍导致明显成本；
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
