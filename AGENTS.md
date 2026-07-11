# AGENTS.md

本文件是项目级长期规则。每次进入本工作区时先读本文件，再读 `PROJECT_INDEX.md` 和相关子目录的 `AGENTS.md`。

## 1. 项目目标

用户未来希望做 AI infra / inference infra，不希望硕士论文主线回到传统数据库内核或传统 GPU 查询算子。

当前课题目标：

> 面向数据库内置 AI 算子的分布式数据处理执行链路优化研究。

当前候选技术切入点：

> 基于 Ray/Daft/Lance 类外部执行链路的数据库 AI 算子批处理系统调优，重点关注 batch、partition、task/actor、object、fan-in、backpressure、writeback 等瓶颈。

注意：

- AI 算子场景是必须保留的主场景。
- Ray/Daft/Lance 是候选外部执行系统和主要调优对象，不是已经被证明必须采用的最终产品路线。
- Ray / 非 Ray 对比应作为 baseline 和消融，用来证明系统调优对象合理，不应成为论文主问题本身。
- 不能因为已经写了某个 benchmark，就反向寻找论文问题。

这个课题需要同时满足：

- 对用户：积累 AI infra / inference infra 能力，包括 distributed execution、object store、shuffle、Arrow/RecordBatch、AI data pipeline、批量推理前后处理、embedding / RAG 数据准备、外部执行器、模型服务队列与写回链路。
- 对达梦：能挂靠数据库内置 AI 算子、企业 AI 数据处理、数据库外部执行链路。
- 对论文：有明确问题、方法、实现、实验、baseline、对比和可复现证据。

## 2. 当前系统边界

当前研究对象是数据库 AI 算子背后的外部数据处理链路：

```text
PostgreSQL 18.3 internal validation platform / PostgreSQL 18.4 local rehearsal
  -> table / parquet / Arrow RecordBatch
  -> task partitioning / batch construction
  -> Ray / Daft-like external execution
  -> AI_EMBED / chunk / preprocess / model service
  -> object transfer / fan-in / shuffle / backpressure
  -> PostgreSQL + pgvector 或 Lance 写回
```

真实形态验证平台优先使用公司内部统一采用的 PostgreSQL 18.3。本地 PostgreSQL 18.4 + pgvector 0.8.2 只作为同构预演环境，不得写成 PG18.3 平台结果。

不要把主线写成：

- 改造整个 Ray；
- 单纯 Daft + Ray + Lance 集成；
- 单纯 Arrow serialization 优化；
- 传统数据库 GPU 查询算子优化；
- AI 模型 kernel 优化；
- 没有真实数据库触发链路的 Python toy benchmark。

## 3. 当前证据

已有 Phase 0 / 动机实验事实：

- Ray small task 稳定开销不高，暂不支持优先做 scheduler/runtime。
- Ray 小 object round-trip 有毫秒级固定成本。
- Arrow IPC 本身不是当前明显瓶颈。
- 本地 Python shuffle 模拟不能证明 coalescing 一定更快。
- Ray many-object fan-in：固定 `16MB` 总数据量下，`1 -> 256` objects fan-in 约放大 `2.59x`。
- Ray Arrow RecordBatch fan-out/fan-in：固定 `65536` 行、`128` 维 embedding 下，fine/coalesced 平均 fan-in 比约 `3.17x`。
- fake `AI_EMBED(text)` 端到端链路：固定 `65536` 行、`128` 维 embedding 下，fine/coalesced 平均 fan-in 比约 `2.16x`，端到端耗时比约 `2.51x`。
- granularity attribution 显示收益不只来自 fan-in refs 减少，还来自 task / operator invocation 数量减少。
- backpressure 模拟显示，无界提交不会提高固定模型服务吞吐，但会显著放大 queue wait、in-flight 请求数和 token backlog。

PG18.4 本地进展：

- `validation/results/pg18_4_connection_validation.md`：只证明本地 PG18.4 环境可连接，系统能读写数据库和完成冒烟链路。
- `motivation/results/pg18_4_system_profile_fake_ai_embed.md`：4096 行 PG18.4 真实数据库触发链路系统画像，比较 `python/ray_actor × fine/coalesced`。
- PG18.4 系统画像 formal 均值显示：Python baseline 下 fine/coalesced 端到端耗时比约 `16.93x`，Ray actor 下约 `13.52x`；Ray actor fine 暴露出明显 bounded wait 和 fan-in 成本。

当前最强信号：

> 数据库 AI 算子外部执行链路对 batch / invocation / object 粒度敏感，且收益来源混合了 operator invocation、task/actor 调用、queue wait、fan-in 和 writeback。

但这些结果仍不能直接等价于最终论文贡献。下一步必须补 baseline，并接真实 CPU/GPU 模型或模型服务验证。

## 4. 目录规则

- `overview/`：项目总纲、当前路线、阶段计划，不放原始结果。
- `research/`：背景调研、文献和官方资料依据。
- `motivation/`：数据库 AI 算子场景、端到端动机测试、系统画像、瓶颈定位和可优化点分析。
- `motivation/results/`：正式动机结果和系统画像结果。PG18.4 系统画像属于这里。
- `validation/`：可行性 benchmark、组件级 microbenchmark、环境验证和连接验证。
- `validation/results/`：Phase 0 可行性结果、PG18.4 连接验证、自动报告。
- `code/`：后续正式工程代码和可复用脚本。
- `deploy/`：本地数据库和服务部署。
- `notes/`：沟通记录和待确认问题。

## 5. 实验规则

- 组件 benchmark 代码放在 `validation/benchmarks/`。
- 端到端动机和系统画像结果放在 `motivation/results/`。
- 环境连接验证结果放在 `validation/results/`。
- 新实验必须有明确问题、运行命令、CSV 输出、结果解释和不能声称的结论。
- 必须区分 DB fetch、Arrow build、serialization、`ray.put`、operator invocation、queue wait、fan-in、writeback 等时间边界。
- warm-up 要忽略或明确标注。
- 每个结论标注来源类型：本地实验事实、模拟实验事实、合理推断、待确认。
- conda 可以使用，但优先创建项目内前缀环境，例如 `.conda/pg-ai-profile`，并保持 `.conda/` 不进入版本化结果。
- Docker named volume 不得删除，除非用户明确要求。

下一轮优先 baseline：

1. Python batched worker：只做 batch、不用 Ray 的收益。
2. Ray task baseline：actor 是否必要，Ray task 粒度成本是多少。
3. Ray actor batch size / actor 数：并行度、queue wait 和 throughput 关系。
4. pgvector `vector(128)` 写回：JSON 文本写回是否误导 writeback 成本。
5. 真实 CPU embedding 小模型：fake sleep 信号是否迁移。
6. GPU / Ray Serve / vLLM：batch、in-flight、GPU 利用率、模型服务队列是否成为主瓶颈。

## 6. 严谨性规则

遵循 `karpathy-guidelines`：

- 不确定就问，不把假设写成事实。
- 先定义可验证目标，再写代码或下结论。
- 做最小实验，不做 speculative framework。
- 只改与当前请求直接相关的文件。
- 不把 microbenchmark 包装成完整论文结论。
- 不把 Ray 说成“很慢”而没有上下文。
- 不把 Arrow serialization 说成瓶颈而没有数据。
- 不把 Daft/Ray/Lance 产品化适配说成既定事实。
- 不把“跨层调度”写成既定最终贡献，除非已有 task/actor、资源配比、模型服务队列或 backpressure 的真实实验数据。

遵循 `idea-evaluator`：

- 先做 fatal-flaws audit，尤其防止 F9 solution hunting for a problem。
- 再看能力/周期匹配。
- 再按 Higher/Faster/Stronger/Cheaper/Broader 判断贡献轴。

## 7. 实验结果讲解规则

讲解实验结果时默认按以下结构：

1. 实验设置：脚本、运行命令、参数配置，以及每个参数代表什么。
2. 实验设计与过程：系统链路、每一步做了什么、记录了哪些指标。
3. 严谨性自检：控制变量、不够严谨的地方、不能过度解释的结论。
4. 实验数据：严格基于真实 CSV / 报告，必要时重新汇总 CSV。
5. 结果解释：区分本地实验事实、模拟实验事实、合理推断、待确认问题、不能声称的结论。
6. 对课题方向的含义：说明该实验支撑或不支撑哪些 AI 算子 / AI infra 方向。

## 8. 对外沟通规则

对外优先表述为：

> 数据库内置 AI 算子的外部分布式数据处理执行链路优化。

不要直接表述为“我要做 Daft/Ray/Lance”，避免显得脱离数据库落地。

需要继续确认：

- PostgreSQL 18.3 内部验证平台的安装、扩展、外部 worker、网络和权限约束。
- 已实现 AI 算子的开源数据库/扩展是哪一个，如何迁移或等价集成到 PostgreSQL 18.3。
- AI 算子在 PostgreSQL 18.3 中是 SQL UDF、表函数、外部执行器，还是批处理服务。
- 达梦是否会用 Ray/Daft/Lance 支撑数据库内置 AI 算子。
- 数据是否以 Arrow/Parquet/Lance/IPC 格式流转。
- 是否存在 join/groupby/repartition/embedding preprocessing。
- 为什么需要 Ray/Daft/Lance 类系统，而不是数据库内部线程池或普通服务。

## 9. 更新规则

当方向、实验或项目规则变化时，同步检查：

- `README.md`
- `PROJECT_INDEX.md`
- `overview/current_direction_and_plan.md`
- `motivation/results/motivation_test_results_analysis.md`
- `validation/results/README.md`
- `motivation/results/README.md`
- `research/literature_and_evidence_review.md`
