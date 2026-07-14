# AGENTS.md

本文件是项目级长期规则。每次进入本工作区时先读本文件，再读 `PROJECT_INDEX.md`；操作任何子目录前，先检查并读取该目录下的 `AGENTS.md`。若子目录没有 `AGENTS.md`，按最近上级目录规则执行。

## 1. 项目目标

当前课题目标：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

用户长期目标是积累 AI infra / inference infra 能力，不希望硕士论文主线回到传统数据库内核或传统 GPU 查询算子。

当前候选切入点：

> 基于 Daft/Ray/Lance 类系统机制的 AI 数据执行链路调优。

重点关注 batch、partition、task/actor、object、fan-in、backpressure、writeback、模型服务队列、GPU-backed model-service 资源配比等瓶颈。

当前规划优先做 GPU-backed AI 数据执行链路调优。最强主动机实验应是生产式 GPU-backed E2E profile：数据库触发 AI workload，数据进入 Daft/Arrow 数据组织层、Ray task/actor 执行层和 GPU-backed 模型服务，再写回 Lance / pgvector / PostgreSQL sink，并证明数据执行与存储链路损耗足够大、可分解、值得优化。不要把主要工作量放在“证明 AI 算子从数据库搬到 GPU 会更快”、数据库内核 UDF、GPU kernel 或大量 GPU baseline 上。

## 2. 系统边界

研究对象是数据库 AI 算子背后的外部数据处理链路：

```text
PostgreSQL 18.3 internal validation platform / PostgreSQL 18.4 local rehearsal
  -> table / Arrow RecordBatch / Parquet
  -> task partitioning / batch construction
  -> Ray / Daft-like external execution
  -> AI_EMBED / preprocess / model service
  -> object transfer / fan-in / shuffle / backpressure
  -> PostgreSQL + pgvector 或 Lance 写回
```

必须保持这些边界：

- AI 算子场景是主场景。
- Ray/Daft/Lance 是候选外部执行系统和调优对象，不是预设最终产品路线。
- Ray / 非 Ray 对比应作为 baseline 和消融，不应成为论文主问题本身。
- GPU-backed model service 是主实验链路中的现实计算端点，应尽早进入端到端系统画像；GPU/数据库内模型执行迁移只保留必要 baseline，用来界定数据执行与存储链路优化的适用边界，不要把主要时间投入 GPU 迁移、GPU kernel 或数据库内执行 baseline。
- 真实 GPU-backed 链路上的大块消融优先于 CPU/fake 简化实验；CPU/fake 链路只作为脚本调试、计时边界验证和历史对照，不能作为真实瓶颈归因证据，也不能把 CPU-only 分阶段瓶颈写成 GPU-backed 链路瓶颈。
- 本地 PostgreSQL 18.4 + pgvector 只作为 PostgreSQL 18.3 内部平台的同构预演环境，不得写成 PG18.3 结果。
- 不把 microbenchmark 包装成完整论文结论。

不要把主线写成：

- 改造整个 Ray；
- 单纯 Daft + Ray + Lance 集成；
- 单纯 Arrow serialization 优化；
- 传统数据库 GPU 查询算子优化；
- AI 模型 kernel 优化；
- 没有真实数据库触发链路的 Python toy benchmark。

## 3. 目录职责

- `overview/`：项目总纲、当前路线、阶段计划。
- `research/`：背景调研、文献和官方资料依据。
- `motivation/`：AI 算子场景、端到端动机测试、系统画像、瓶颈定位和可优化点分析；回答为什么课题值得做，不承担完整研究实验规划。
- `motivation/results/`：正式动机结果和系统画像结果。
- `experiments/`：正式研究实验、优化方案验证、消融实验和小范围调优测试；回答三项研究内容的方法是否有效。
- `feasibility/`：可行性 benchmark、组件级 microbenchmark、环境验证和连接验证。
- `feasibility/results/`：fake/CPU 预研结果、PG18.4 连接验证、自动报告。
- `code/`：可复用工程代码、脚本和测试。
- `deploy/`：本地数据库和服务部署。
- `learning/`：面向用户的学习讲解材料。
- `notes/`：沟通记录和待确认问题。
- `opening/`：开题报告、开题 PPT、飞书进度汇报、文献精读、答辩问答和材料同步日志。

## 4. 目录规则与 README

- 每个 `AGENTS.md` 只写关键目标、边界、文件职责和必须遵守的规则。
- 保持精简，不堆长篇背景、实验结果、教程或完整计划。
- 操作目录时按层级查阅：先读该目录 `AGENTS.md`；若存在同目录 `README.md`，再读 `README.md`；仍不确定时，再按 README、`PROJECT_INDEX.md` 或 `AGENTS.md` 指向去读具体结果、脚本说明、学习文档或报告。
- 详细内容放到同目录 `README.md`、结果报告、学习文档或索引文件中，再由 `AGENTS.md` 指向。
- `README.md` 是目录的动态说明层，负责当前状态、详细入口、运行方式、文件索引和结果位置。
- 每次在某目录完成实验、代码、测试或文档改动后，都要检查同目录 `README.md` 是否需要同步；不需要修改时，在回复中说明原因。
- 如果目录长期维护且没有 `README.md`，但已有多个入口、命令或结果说明，应补一个简短 `README.md`。
- 动态内容、过时计划和已经被新实验替代的判断要及时迁移、更新或删除，避免与 README、结果报告和项目计划冲突。
- 若长期维护的新目录缺少规则文件，补一个简短 `AGENTS.md`。
- 不确定是否需要修改某个 `AGENTS.md` 时，先问用户。

## 5. 实验规则

- 新实验必须有明确问题、运行命令、参数说明、CSV 输出、结果解释和不能声称的结论。
- 必须区分 DB fetch、Arrow build、serialization、`ray.put`、operator invocation、queue wait、fan-in、writeback 等时间边界。
- GPU-backed E2E profile 必须额外记录或说明模型端点、GPU 资源、batch policy、in-flight/queue、GPU utilization 或其缺失原因；它用于建立课题主动机和真实瓶颈排序。
- 消融实验应优先在同一条 GPU-backed E2E 链路上做大块消融，例如 no-Ray vs Ray、single worker vs Ray actors、主控 fan-in 写回 vs 多 worker 写回、unbounded vs bounded in-flight、不同 batch/partition 策略；不要先把 workload 简化成 CPU/fake 后再做归因。
- warm-up 要忽略或明确标注。
- 每个结论标注来源类型：本地实验事实、模拟实验事实、合理推断、待确认问题。
- 连接验证只能证明系统可用，不能当作性能收益结论。
- conda 可以使用，但优先创建项目内前缀环境，例如 `.conda/pg-ai-profile`，并保持 `.conda/` 不进入版本化结果。
- Docker named volume 不得删除，除非用户明确要求。

## 6. 严谨性规则

遵循 `karpathy-guidelines`：

- 不确定就问，不把假设写成事实。
- 先定义可验证目标，再写代码或下结论。
- 做最小实验，不做 speculative framework。
- 只改与当前请求直接相关的文件。
- 不把 Ray 说成“很慢”而没有上下文。
- 不把 Arrow serialization 说成瓶颈而没有数据。
- 不把 Daft/Ray/Lance 产品化适配说成既定事实。
- 不把“跨层调度”写成既定最终贡献，除非已有 task/actor、资源配比、模型服务队列或 backpressure 的真实实验数据。

## 7. 学习讲解规则

每次完成实验、代码实现或功能测试后，默认同步更新 `learning/` 下的学习材料，除非用户明确说不需要。

讲解实验结果时按以下结构：

1. 实验设置：脚本、运行命令、参数配置。
2. 实验设计与过程：系统链路、每一步做什么、记录哪些指标。
3. 严谨性自检：控制变量、局限、不能过度解释的结论。
4. 实验数据：严格基于真实 CSV / 报告。
5. 结果解释：区分本地实验事实、模拟实验事实、合理推断、待确认问题、不能声称的结论。
6. 对课题方向的含义：说明支撑或不支撑哪些 AI 算子 / AI infra 方向。

不要假设用户熟悉 Ray、Daft、Lance、Arrow、pgvector、batch、partition、task、actor、object store、fan-in、backpressure、writeback 等术语。

## 8. 对外沟通

对外优先表述为：

> 数据库驱动 AI 工作负载的分布式数据执行与存储协同优化。

Daft、Ray、Lance 作为候选系统机制和验证平台出现，不要直接表述为“我要做 Daft/Ray/Lance”，避免显得脱离数据库落地；也不要把研究对象写成传统数据库 GPU 查询算子或模型 kernel 优化。

## 9. 更新检查

当方向、实验、代码、测试、项目规则或长期学习规则变化时，同步检查：

- `README.md`
- `PROJECT_INDEX.md`
- 被操作目录的同目录 `README.md`
- `overview/current_direction_and_plan.md`
- `motivation/results/fake_cpu/analysis.md`
- `feasibility/results/README.md`
- `motivation/results/README.md`
- `research/literature_and_evidence_review.md`
- `learning/README.md`
- `learning/experiment_walkthrough.md`

检查不等于必须修改；若无需修改，在回复中说明原因。修改时同时检查是否存在过时、重复或互相冲突的旧表述。修改 README 时要从读者视角确认入口是否足够显眼，不能只做隐藏在文末的索引式更新。

每次完成改动或实验后，必须做一次影响面自检：哪些 README、索引、结果报告、学习材料、脚本说明、配置说明或代码注释需要同步更新；哪些旧内容需要删除或降级。只同步与本次变更直接相关的文件。
