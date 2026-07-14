# motivation/AGENTS.md

本目录维护动机场景、AI 算子形态和端到端动机测试计划。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 定义为什么这个课题值得做。
- 把系统 microbenchmark 连接到数据库 AI 算子、批量 embedding、RAG 数据准备等业务场景。
- 规划 fake AI 算子到 PostgreSQL 18.3 内部验证平台的真实画像路径；普通 PostgreSQL + pgvector 只作为平台暂不可用时的同构预演替身。
- 在具体优化方向未定时，优先寻找最适合用户 AI infra / inference infra 目标的 AI 算子场景，再设计动机测试和可行性测试。

## 实验原则

- fake `AI_EMBED(text)` 只用于隔离系统瓶颈的早期验证。
- 后续必须接 PostgreSQL 18.3 真实数据库形态或真实 AI 算子接口验证，不长期停留在模拟脚本。
- 每个动机实验都要输出端到端指标：rows/s、object_count、batch size、fan-in time、write time。
- 动机实验正式结果放在 `motivation/results/`（唯一来源），不再同步到 `feasibility/results/`。
- 如果端到端链路不再显示 object/fan-in 瓶颈，要及时回退并调整课题。
- 不预设最终优化方向。Object Transfer、fan-in、Shuffle、batching、partition、模型服务调用、批量推理前后处理、向量写回、scan/filter/pushdown 都必须通过实验数据比较后再收敛。
- 动机测试必须从真实或合理近似的 AI 算子 workload 出发，不能只为了证明某个系统优化点而构造 toy workload。
- 对每个候选场景，先写清楚：真实用户是谁、AI 算子是什么、输入输出是什么、为什么需要分布式/批处理、可能瓶颈在哪里、什么结果会推翻该方向。
- 对外部建议和生成文本保持批判性使用：可以吸收问题定义、反证条件和实验建议，但不能把其中未验证判断写成事实。尤其是 PostgreSQL / 开源数据库出发的离线 LLM 场景，必须先核查具体系统、接口和维护状态，再决定是否作为主场景。
- “粒度控制”只是入口，不应成为全部课题。后续动机实验应逐步验证 AI 算子特征是否能驱动 task/actor 并行度、CPU/GPU 或 CPU/model-service 资源配比、模型服务路由、数据局部性和 backpressure 决策。

## 当前状态

已搭建 fake `AI_EMBED(text)` 端到端链路：

1. 生成 documents 数据；
2. 构造 Arrow RecordBatch；
3. Ray 执行 fake embedding；
4. 对比 fine object 与 coalesced object；
5. 输出 CSV 和瓶颈分析。

该实验支持继续观察 RecordBatch/object 粒度和 fan-in，但还不能单独锁定最终论文方向。

动机测试正式结果位于：

- `motivation/results/fake_ai_embed_pipeline.csv`
- `motivation/results/ai_operator_scenario_motivation.csv`
- `motivation/results/ai_operator_granularity_attribution.csv`
- `motivation/results/ai_operator_backpressure.csv`
- `motivation/results/motivation_test_results_analysis.md`

## 当前下一步

1. 拆分 fake `AI_EMBED(text)` 结果中的收益来源：object 数、task 数、`ray.put` 次数、fan-in 依赖数、写回阶段。
2. 并行调研和比较更贴近 AI infra / inference infra 的 AI 算子场景，例如批量 embedding / RAG 准备、chunk + embedding + vector write、批量 rerank / classify、离线推理前后处理、数据库外部 AI UDF 执行。
3. 对候选场景使用 `idea-evaluator` 思路做 fatal-flaws audit 和五维贡献评估；对方向不清晰的问题使用 `deep-research` scoping / Socratic 思路先澄清研究问题和证据标准。
4. 下一轮动机测试优先拆分 task 数、object 数、`ray.put` 次数和 fan-in 依赖数；随后做 task/actor/concurrency 实验、token-aware / prefix-aware offline LLM fake workload、selectivity-aware AI_FILTER fake workload，以及 producer-consumer / backpressure 实验。
5. 设计 PostgreSQL 18.3 真实画像实验，确认数据库触发、外部 worker、AI 算子执行、Ray/Arrow 中间链路、fan-in/writeback 和 backpressure 指标如何采集。
6. 只有当场景动机、系统瓶颈、PostgreSQL 18.3 画像数据和用户 AI infra 学习目标同时对齐时，才把某个优化点写成最终主线。
