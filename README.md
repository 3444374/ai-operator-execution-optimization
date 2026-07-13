# 数据库驱动 AI 工作负载数据执行优化工作区

本工作区用于组织硕士论文 / 达梦实习课题材料。当前方向是：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

根目录快速入口：

```text
PROJECT_OUTLINE.md  # 当前项目大纲、实验主线和近期优先级
PROJECT_LOG.md      # 项目级简要操作日志
PROJECT_INDEX.md    # 文件索引和阅读顺序
```

当前候选技术切入点为：

> 面向数据库驱动 AI workload 的 Daft/Ray/Lance 类分布式数据执行与存储协同优化。

开题材料当前收敛后的正式题目是：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

该题目不是单独的汇报包装，而是后续项目规划的当前方向锚点。后续实验和文档应围绕三类研究内容展开：AI workload 感知的数据组织与批处理执行调度、GPU 推理服务状态感知的 Ray 并行调度与反压控制、面向 AI 数据流的结果汇聚与 Lance / 数据库持久化协同。

项目方向与开题材料采用双向同步机制。一方面，开题报告必须基于当前项目进展、已有实验事实和后续规划来写；另一方面，后续如果开题报告的题目、研究内容、技术路线、实验边界或侧重点发生调整，项目的整体规划、实验优先级、文档入口和对外口径也要同步调整。不能出现开题报告已经收敛到新方向，而 `overview/`、`motivation/`、`PROJECT_INDEX.md` 或 README 仍停留在旧方向的情况。

更完整的候选路线是：

> 面向数据库驱动 AI workload 的特征感知数据组织、并行执行与存储协同优化。

其中 object/fan-in/coalescing 是当前已经有本地信号的入口，不是最终全部贡献。具体优化方向还没有最终锁定。项目目标不是传统数据库 GPU 查询算子优化，也不是单纯模型 kernel 优化，而是先在生产式 GPU-backed AI workload 端到端链路中确认数据组织、Ray 执行、模型服务和持久化写回是否构成主要损耗，再用动机测试、可行性测试和真实形态验证收敛到合适的 AI infra / inference infra 优化点。

当前规划优先研究 GPU-backed AI 数据执行链路，而不是把大量时间投入“AI 算子搬到 GPU / 数据库内核里执行”的 baseline。Snowflake Cortex AISQL、pgai vectorizer、pgvector、PostgresML、OceanBase/达梦类分布式数据库都作为工业背景或对照路线；数据库在本项目中主要作为 AI workload 的入口和结果落点，研究主体是数据进入 Daft/Arrow 数据组织层、Ray task/actor 执行层、GPU 模型服务和 Lance / pgvector / PostgreSQL sink 后的 batch、partition、task/actor、object、fan-in、backpressure、writeback 调优。GPU-backed model service 是主实验链路应尽早接入的真实计算端点；GPU/数据库内执行迁移、GPU kernel 或 CPU-only 链路只保留必要对比，不作为主要实现路线。

后续真实端到端实验平台优先使用公司内部统一采用的 PostgreSQL 18.3，并把已有或开源 AI 算子迁移/等价集成到该版本上。当前 fake / CPU benchmark 只用于脚本调试、计时边界验证和历史对照；最强动机实验应是生产式 GPU-backed E2E profile：PostgreSQL 触发数据库驱动 AI workload，数据进入 Daft/Ray/Lance-like 执行链路，调用 GPU-backed 模型服务，再写回 Lance / pgvector / PostgreSQL sink，并证明数据执行与存储链路损耗足够大、足够可分解、足够值得优化。

## 学习与讲解入口

如果还不熟悉 Ray、Daft、Lance、Arrow、pgvector、batch、partition、task/actor、fan-in、backpressure、writeback 等术语，先读：

```text
learning/experiment_walkthrough.md
```

后续每次完成实验、代码实现或功能测试，都应同步更新 `learning/` 下的学习材料。正式实验记录和结论仍放在 `motivation/results/` 或 `feasibility/results/`，学习材料负责解释“为什么测、怎么测、术语是什么意思、结果数字怎么读、下一步有什么用”。

## 目录结构

```text
.
├── AGENTS.md                         # 项目级长期规则
├── PROJECT_OUTLINE.md                 # 项目总纲、实验主线和同步规则
├── PROJECT_LOG.md                     # 项目级简要操作日志
├── PROJECT_INDEX.md                   # 文件索引和阅读顺序
├── overview/               # 项目总览、当前路线
│   ├── AGENTS.md
│   ├── current_direction_and_plan.md
│   └── project_outline.md
├── research/            # 背景调研、文献依据
│   ├── AGENTS.md
│   ├── README.md
│   ├── existing_ai_operator_execution_chains.md
│   └── literature_and_evidence_review.md
├── motivation/         # 动机场景、端到端主动机、系统画像
│   ├── AGENTS.md
│   ├── README.md
│   ├── plans/          # 场景、路线、集成计划
│   ├── benchmarks/     # 动机实验脚本
│   └── results/        # gpu / pg18_4_fake / fake_cpu 分层结果
├── feasibility/         # 可行性验证、benchmark、结果
│   ├── AGENTS.md
│   ├── README.md
│   ├── guide.md
│   ├── analysis.md
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
├── learning/               # 面向初学者的实验讲解和术语学习材料
│   ├── AGENTS.md
│   ├── README.md
│   └── experiment_walkthrough.md
├── opening/                # 开题报告、PPT、飞书进度汇报、文献和答辩准备
│   ├── AGENTS.md
│   ├── README.md
│   ├── outline.md
│   ├── ppt_rules.md
│   ├── work_rules.md
│   ├── report/
│   ├── slides/
│   ├── feishu/
│   ├── literature/
│   └── logs/
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

2026-07-11 已完成第一组 PG18.4 本地正式对照画像实验：固定 4096 行，对比
`python/ray_actor × fine/coalesced`，每组 1 次 warm-up 与 3 次 formal 重复。
formal 均值显示 Ray actor 链路中 fine/coalesced 端到端耗时比约 `13.52x`，
支持继续验证 batch / invocation / object 粒度控制；但该结果仍是本地
PG18.4 fake-model 证据，不能写成 PostgreSQL 18.3 或真实 GPU 模型结论。
完整记录见 `motivation/results/pg18_4_fake/system_profile.md`。

2026-07-12 曾在 GPU endpoint 缺失时补跑 PG18.4 fake-model 同构预演。该结果现在只作为
历史调试和计时边界记录，不再作为最高优先级动机证据。完整记录见
`motivation/results/pg18_4_fake/simulated_embed_test_20260712.md`。

2026-07-12 已完成第一组 GPU-backed 真实 embedding 端到端画像。模型为
`sentence-transformers/all-MiniLM-L6-v2`，通过本地 CUDA HTTP endpoint
`http://localhost:8000/v1/embeddings` 接入；本次 endpoint 由用户手动启动，
后续复现实验需先检查 8000 端口，不存在时按报告中的命令启动。结果显示：
1024 行真实模型下，逐行调用 fine 比 batch 调用 coalesced 慢约 `13.6x`；
4096 行 coalesced 下，Python / Ray task / Ray actor 差距不大，但 JSON
写回约 `1.55s-1.60s`，已经是大块成本。完整记录见
`motivation/results/gpu/ai_embed_profile.md`。

2026-07-12 已进一步完成真实 GPU-backed embedding 链路拆分实验，正式结果见
`motivation/results/gpu/ai_embed_chain_breakdown_20260712.md`。该实验新增
`operator_wall_s` 与 `model_request_wall_s`，用于避免把 `model_service_s`
请求耗时加和误读成阶段墙钟时间。结果显示：1024 行下逐行调用 fine 比
coalesced 慢约 `13.4x`；16384 行 Ray actor/coalesced 下，外部 operator 阶段
约 `6.47s`，writeback 约 `6.59s`，二者都接近端到端时间的一半。这是当前
开题动机中最应优先引用的真实 GPU-backed AI 数据执行链路证据。

2026-07-12 还补了一组双 endpoint Ray 动机测试，正式结果见
`motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md`。在 4096 行、
16 个 coalesced batch 下，Python 顺序轮询两个 endpoint 的端到端均值约
`3.44s`，Ray task / Ray actor 分别约 `2.76s` / `2.78s`；Ray 主要降低
`operator_wall_s`，但 `writeback_s` 仍约 `1.56s-1.59s`。该结果支持继续验证
Ray 的多 endpoint 路由、反压和 worker 写回价值，但不能写成 Ray 已经普遍优于
Python 或多 GPU 结论。

已有 fake/CPU 本地实验显示：

- Ray small task 不是当前最强瓶颈；
- Arrow IPC 本身不是当前明显瓶颈；
- 固定总数据量下，Ray many-object fan-in 会随 object 数量放大；
- Arrow RecordBatch `N upstream -> P downstream` 实验中，fine/coalesced 平均 fan-in 比约 `3.17x`。
- granularity attribution 实验显示，fake 链路收益不只来自 fan-in refs 减少，还明显来自 AI operator task / invocation 数减少。
- backpressure 模拟显示，模型服务吞吐固定时，无界提交不会提高 tokens/s，但会显著放大 queue wait、in-flight 请求数和 token backlog。

当前更值得继续验证的候选优化对象是：

> 数据库驱动 AI workload 数据执行链路中的 Daft/Arrow batch、partition、Ray task/actor、RecordBatch object、模型服务吞吐、backpressure 和 Lance / 数据库写回粒度控制。

现有系统的执行链路对比见 `research/existing_ai_operator_execution_chains.md`。当前判断是：Snowflake Cortex AISQL 证明数据库 AI SQL 算子是工业真实问题，但其闭源托管链路不适合作为本地必测 baseline；pgai vectorizer 的 PostgreSQL + stateless worker + embedding endpoint + writeback 形态更接近本项目，应作为 worker 写回和异步队列实验的架构参考；Ray 是本项目选择的可控执行机制之一，不是现有 AI 算子的共同事实。

当前不能只把问题写成 object/fan-in/coalescing。已有 fake 实验把 task 数、object 数、`ray.put` 次数和 fan-in 依赖数一起改变了，下一步必须先拆分收益来源，再用更贴近 AI infra / inference infra 的 token-aware、prefix-aware、selectivity-aware 和 resource/backpressure-aware workload 验证。

## 近期目标

1. 拆分 fake `AI_EMBED(text)` 端到端结果中的收益来源。
2. 基于 `motivation/plans/workloads.md` 比较批量 embedding / RAG、AI_CLASSIFY / AI_FILTER、离线 LLM 生成 / 评测三个场景。
3. 不把三个场景当成三个独立方向，而是围绕“数据库驱动 AI workload 的特征感知数据组织、并行执行与存储协同”做统一问题定义。
4. 下一轮第一优先级是生产式 small-scale GPU-backed E2E motivation profile：数据库表/SQL 触发、Daft/Arrow batch、Ray task/actor、GPU-backed embedding 或轻量模型服务、fan-in/writeback 和指标采集必须在同一条链路里跑通，并用阶段占比说明数据执行与存储链路是否是主要损耗来源。
5. 消融实验优先在真实 GPU-backed 链路上做大块对照：不用 Ray vs 用 Ray、Python worker vs Ray task/actor、主控 fan-in 写回 vs 多 worker 各自写回、unbounded vs bounded in-flight、不同 batch/partition 策略。CPU/fake 只保留为脚本调试和历史对照，不作为最终 GPU 链路调优结论。
6. 设计并跑通 PostgreSQL 18.3 内部平台上的真实画像实验：数据库表/SQL 触发、Daft/Arrow batch、Ray 执行、GPU 模型服务、fan-in/writeback、指标采集。
7. 后续把稳定工程代码迁移到 `code/`。

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
motivation/results/fake_cpu/analysis.md
```

如果看不懂实验目的、术语或结果，先看学习材料：

```text
learning/experiment_walkthrough.md
```

开题材料准备入口：

```text
opening/README.md
```

开题报告、PPT 和飞书文档中的题目、研究内容、实验边界会反向约束 `overview/`、`motivation/` 和后续实验计划；如果开题口径发生变化，需要同步检查项目级 README、`PROJECT_INDEX.md` 和 `overview/current_direction_and_plan.md`，避免开题材料与项目路线脱节。

每次调整开题方向后，至少检查以下文件是否需要同步：

```text
README.md
PROJECT_INDEX.md
overview/current_direction_and_plan.md
motivation/plans/workloads.md
motivation/plans/integration.md
opening/README.md
opening/work_rules.md
opening/report/opening_report.md
opening/slides/opening_ppt.md
opening/feishu/opening_report_wiki.md
```

检查不等于全部修改；但如果题目、研究内容、实验优先级或结论边界发生变化，需要在相关文件中同步说明。


