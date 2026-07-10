# 数据库内置 AI 算子执行链路优化工作区

本工作区用于组织硕士论文 / 达梦实习课题材料。当前方向是：

> 面向数据库内置 AI 算子的分布式数据处理执行链路优化研究。

当前候选技术切入点为：

> 基于 Daft/Ray/Lance 的 Object Transfer、fan-in 与 Shuffle 中间数据传输优化。

更完整的候选路线是：

> 面向数据库 AI 算子的特征感知任务划分、并行执行与跨层调度优化。

其中 object/fan-in/coalescing 是当前已经有本地信号的入口，不是最终全部贡献。具体优化方向还没有最终锁定。项目目标不是传统数据库 GPU 查询算子优化，也不是单纯模型 kernel 优化，而是先围绕 AI 算子场景寻找真实瓶颈，再通过动机测试、可行性测试和真实形态验证收敛到合适的 AI infra / inference infra 方向。

后续真实端到端实验平台优先使用公司内部统一采用的 PostgreSQL 18.3，并把已有或开源 AI 算子迁移/等价集成到该版本上。当前 fake benchmark 只是在设备未到位时做小规模瓶颈隔离；真正需要证明的是 PostgreSQL 18.3 触发 AI 算子后的外部执行链路画像。

## 目录结构

```text
.
├── AGENTS.md                         # 项目级长期规则
├── PROJECT_INDEX.md                   # 文件索引和阅读顺序
├── overview/               # 项目总览、当前路线
│   ├── AGENTS.md
│   ├── current_direction_and_plan.md
│   └── project_outline.md
├── research/            # 背景调研、文献依据
│   ├── AGENTS.md
│   └── literature_and_evidence_review.md
├── motivation/         # 动机场景、AI 算子形态、端到端计划
│   ├── AGENTS.md
│   ├── ai_infra_candidate_scenarios_and_motivation_tests.md
│   ├── fake_ai_embed_pipeline_benchmark.py
│   ├── ai_operator_scenario_motivation_benchmark.py
│   ├── ai_operator_granularity_attribution_benchmark.py
│   ├── ai_operator_backpressure_benchmark.py
│   ├── results/
│   ├── ai_operator_scenario_and_motivation_test.md
│   └── ai_operator_integration_test_plan.md
├── validation/         # 可行性验证、benchmark、结果
│   ├── AGENTS.md
│   ├── feasibility_validation_guide.md
│   ├── preliminary_feasibility_analysis.md
│   ├── benchmarks/
│   └── results/
├── code/                   # 后续正式工程代码
│   ├── AGENTS.md
│   ├── src/
│   ├── tests/
│   ├── scripts/
│   └── configs/
├── deploy/                 # 本地同构预演环境
│   └── postgres18.4/       # PostgreSQL 18.4 + pgvector；PG18.3 内部平台替身
└── notes/                  # 沟通记录、待确认问题
│   ├── AGENTS.md
│   └── communication_notes.md
```

## 当前证据

当前工程状态：本机 Docker 已运行 PostgreSQL 18.4 + pgvector 0.8.2，数据库
基础连通性和向量查询已验证。项目 Python 画像脚本已完成 256 行的
PostgreSQL -> Arrow -> Ray actor -> fake embedding -> PostgreSQL 写回冒烟运行。
该实例仍只作为公司 PostgreSQL 18.3 内部平台的本地同构预演替身；本次单次
fake-model 结果不能作为性能结论。

已有 Phase 0 本地实验显示：

- Ray small task 不是当前最强瓶颈；
- Arrow IPC 本身不是当前明显瓶颈；
- 固定总数据量下，Ray many-object fan-in 会随 object 数量放大；
- Arrow RecordBatch `N upstream -> P downstream` 实验中，fine/coalesced 平均 fan-in 比约 `3.17x`。
- granularity attribution 实验显示，fake 链路收益不只来自 fan-in refs 减少，还明显来自 AI operator task / invocation 数减少。
- backpressure 模拟显示，模型服务吞吐固定时，无界提交不会提高 tokens/s，但会显著放大 queue wait、in-flight 请求数和 token backlog。

当前更值得继续验证的候选优化对象是：

> 数据库 AI 算子外部执行链路中的 batch、partition、task、RecordBatch object、CPU/GPU 并行度、模型 actor 路由、backpressure 和写回粒度控制。

当前不能只把问题写成 object/fan-in/coalescing。已有 fake 实验把 task 数、object 数、`ray.put` 次数和 fan-in 依赖数一起改变了，下一步必须先拆分收益来源，再用更贴近 AI infra / inference infra 的 token-aware、prefix-aware、selectivity-aware 和 resource/backpressure-aware workload 验证。

## 近期目标

1. 拆分 fake `AI_EMBED(text)` 端到端结果中的收益来源。
2. 基于 `ai_infra_candidate_scenarios_and_motivation_tests.md` 比较批量 embedding / RAG、AI_CLASSIFY / AI_FILTER、离线 LLM 生成 / 评测三个场景。
3. 不把三个场景当成三个独立方向，而是围绕“数据库 AI 算子的特征感知并行执行与跨层调度”做统一问题定义。
4. 已补 task/object 解耦和 backpressure 动机实验；下一轮优先接 Ray actor / Ray Serve / vLLM 或真实模型服务验证 queue wait、token backlog 和 actor idle time。
5. 设计并跑通 PostgreSQL 18.3 内部平台上的真实画像实验：数据库表/SQL 触发、外部 worker、AI 算子执行、fan-in/writeback、指标采集。
6. 后续把稳定工程代码迁移到 `code/`。

推荐运行命令：

```bash
python motivation/fake_ai_embed_pipeline_benchmark.py \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --embedding-dim 128 \
  --repeats 3 \
  --output validation/results/fake_ai_embed_pipeline.csv
```

动机测试正式结果和分析优先看：

```text
motivation/results/motivation_test_results_analysis.md
```
