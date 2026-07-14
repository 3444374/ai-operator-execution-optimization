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
├── CLAUDE.md                         # Claude Code 环境入口
├── PROJECT_INDEX.md                  # 文件索引和阅读顺序
├── README.md                         # 本文件
├── overview/                         # 项目总览、当前路线
│   ├── AGENTS.md
│   ├── current_direction_and_plan.md
│   └── project_outline.md
├── research/                         # 背景调研、文献依据
│   ├── AGENTS.md
│   └── literature_and_evidence_review.md
├── motivation/                       # 动机场景、端到端测试脚本与结果
│   ├── AGENTS.md
│   ├── ai_infra_candidate_scenarios_and_motivation_tests.md
│   ├── ai_operator_scenario_and_motivation_test.md
│   ├── ai_operator_integration_test_plan.md
│   ├── fake_ai_embed_pipeline_benchmark.py
│   ├── ai_operator_scenario_motivation_benchmark.py
│   ├── ai_operator_granularity_attribution_benchmark.py
│   ├── ai_operator_backpressure_benchmark.py
│   └── results/
│       ├── README.md
│       ├── *.csv
│       └── motivation_test_results_analysis.md
├── validation/                       # 可行性验证、活跃 benchmark 与结果
│   ├── AGENTS.md
│   ├── feasibility_validation_guide.md
│   ├── preliminary_feasibility_analysis.md
│   ├── archive/                      # 早期排除性实验（已完成使命）
│   │   └── README.md
│   ├── benchmarks/
│   │   ├── README.md
│   │   ├── common.py
│   │   ├── analyze_results.py
│   │   ├── ray_many_objects_benchmark.py
│   │   └── ray_arrow_fanout_fanin_benchmark.py
│   └── results/
│       ├── README.md
│       ├── ray_many_objects.csv
│       ├── ray_arrow_fanout_fanin.csv
│       ├── *_smoke.csv
│       ├── feasibility_report.md
│       └── current_direction_analysis.md
├── code/                             # 后续正式工程代码
│   ├── AGENTS.md
│   ├── README.md
│   ├── src/
│   ├── tests/
│   ├── scripts/
│   └── configs/
└── notes/                            # 沟通记录、待确认问题
    ├── AGENTS.md
    └── communication_notes.md
```

## 当前证据

已有本地实验显示：

- 固定总数据量下，Ray many-object fan-in 会随 object 数量放大；
- Arrow RecordBatch `N upstream -> P downstream` 实验中，fine/coalesced 平均 fan-in 比约 `3.17x`；
- fake `AI_EMBED(text)` 端到端链路中，fine/coalesced 平均 fan-in 比约 `2.16x`，端到端耗时比约 `2.51x`；
- granularity attribution 实验显示，收益不只来自 fan-in refs 减少，还明显来自 AI operator task / invocation 数减少；
- backpressure 模拟显示，无界提交不会提高 tokens/s，但会显著放大 queue wait、in-flight 请求数和 token backlog。

早期排除性实验（Ray small task、object transfer、Arrow serialization、shuffle simulation）已归档至 `validation/archive/`。

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
  --output motivation/results/fake_ai_embed_pipeline.csv
```

动机测试正式结果和分析优先看：

```text
motivation/results/motivation_test_results_analysis.md
```
