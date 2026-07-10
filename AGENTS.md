# AGENTS.md

本文件是项目级长期规则。每次进入本工作区时先读本文件，再读 `PROJECT_INDEX.md` 和相关子目录的 `AGENTS.md`。

## 1. 项目目标

用户未来希望做 AI infra / inference infra，不希望硕士论文主线回到传统数据库内核或 GPU 查询算子。

当前课题目标：

> 面向数据库内置 AI 算子的分布式数据处理执行链路优化研究。

当前候选技术切入点：

> 基于 Daft/Ray/Lance 的 Object Transfer、fan-in 与 Shuffle 中间数据传输优化。

更完整的候选技术路线正在从“粒度控制”扩展为：

> AI 算子特征感知的任务划分、并行调度、资源调度、数据局部性与推理批处理协同优化。

注意：AI 算子场景是当前必须保留的主场景；具体优化对象尚未最终锁定。Object Transfer、fan-in、Shuffle、batching、partition 粒度、task/actor 并行度、CPU/GPU 资源配置、模型服务路由与反压、批量推理前后处理、数据写回、Daft/Ray/Lance scan 或调度路径都只能作为候选方向，必须由动机测试、可行性测试和真实形态验证逐步收敛。

这个课题需要同时满足：

- 对用户：积累 AI infra / inference infra 相关能力，包括 distributed execution、object store、shuffle、Arrow/RecordBatch、AI data pipeline、批量推理前后处理、embedding / RAG 数据准备、外部执行器和写回链路。
- 对达梦：能挂靠数据库内置 AI 算子、企业 AI 数据处理、数据库外部执行链路。
- 对论文：有明确问题、方法、实现、实验、对比和可复现证据。

## 2. 当前边界

当前研究对象是数据库 AI 算子背后的数据链路：

```text
PostgreSQL 18.3 internal validation platform / table / parquet
  -> Arrow RecordBatch
  -> Daft / Ray 批处理执行
  -> AI_EMBED / chunk / preprocess
  -> object transfer / fan-in / shuffle
  -> PostgreSQL + pgvector 或 Lance
```

真实形态验证平台优先使用公司内部统一采用的 PostgreSQL 18.3。当前本地 fake benchmark 只用于在设备未到位时隔离系统瓶颈；不能长期停留在 Python toy benchmark。后续需要把已有或开源 AI 算子集成到 PostgreSQL 18.3 上，真实跑通“数据库触发 -> 外部执行链路 -> AI 算子 -> 写回”的端到端画像实验。

不要把主线写成：

- 改造整个 Ray；
- 单纯 Daft + Ray + Lance 集成；
- 单纯 Arrow serialization 优化；
- 传统数据库 GPU 查询算子优化；
- AI 模型 kernel 优化；
- 没有真实 workload 的 Python toy benchmark。

数据库场景是落地入口；核心能力应体现 AI infra，而不是传统数据库内核。

## 3. 当前证据

已有本地实验：

- Ray small task 稳定开销不高，暂不支持优先做 scheduler/runtime。
- Ray 小 object round-trip 有毫秒级固定成本。
- Arrow IPC 本身不是当前明显瓶颈。
- 本地 Python shuffle 模拟不能证明 coalescing 一定更快。
- Ray many-object fan-in：固定 `16MB` 总数据量下，`1 -> 256` objects fan-in 约放大 `2.59x`。
- Ray Arrow RecordBatch fan-out/fan-in：固定 `65536` 行、`128` 维 embedding 下，fine/coalesced 平均 fan-in 比约 `3.17x`。
- fake `AI_EMBED(text)` 端到端链路：固定 `65536` 行、`128` 维 embedding 下，fine/coalesced 平均 fan-in 比约 `2.16x`，端到端耗时比约 `2.51x`。

当前最强信号：

> 大量小 RecordBatch/object 进入下游 fan-in 会产生明显成本。

但该信号仍不能直接等价于最终论文方向。下一步必须拆分收益来源，区分 object 数减少、Ray task 数减少、`ray.put` 次数减少、fan-in 依赖数减少、写回阶段变化；随后验证 AI 算子特征是否能驱动更高层的 task/actor 并行度、CPU/GPU 资源配比、模型服务路由和 backpressure 决策。

## 4. 目录规则

- `overview/`：项目总纲和当前计划。
- `research/`：背景调研、资料依据。
- `motivation/`：数据库 AI 算子场景和端到端动机测试。
- `validation/`：可行性 benchmark、CSV 结果、自动分析。
- `code/`：后续正式工程代码。
- `notes/`：沟通记录和待确认问题。

进入子目录工作前，先读该目录下的 `AGENTS.md`。

## 5. 实验规则

- benchmark 代码放在 `validation/benchmarks/`。
- 实验结果放在 `validation/results/`。
- 使用项目内 `.venv`，当前无必要使用 conda。
- Ray 在 macOS 沙箱中运行可能需要提权。
- 新实验必须有明确问题、运行命令、CSV 输出和结果解释。
- 必须区分数据生成、序列化、`ray.put`、fan-in、写回等时间边界。
- warm-up 要忽略或明确标注。

下一步优先实验：

> 基于 PostgreSQL 18.3 内部验证平台设计真实数据库 AI 算子端到端画像实验；在设备未到位前，用低端设备/小模型/小规模数据先跑通同构链路，并继续拆分 fake `AI_EMBED(text)` 结果中的 task、object、operator invocation、fan-in、queue/backpressure 收益来源。

## 6. 严谨性规则

遵循 `karpathy-guidelines`：

- 不确定就问，不把假设写成事实。
- 先定义可验证目标，再写代码或下结论。
- 做最小实验，不做 speculative framework。
- 只改与当前请求直接相关的文件。
- 每个结论标注来源类型：文献/官方文档、本地实验、合理推断、待确认。
- 方向选择遵循 `idea-evaluator` 视角：先做 fatal-flaws audit，再看能力/周期匹配，再按 Higher/Faster/Stronger/Cheaper/Broader 判断贡献轴。
- 研究路径遵循 `deep-research` scoping 视角：当研究问题不清晰时，先澄清问题、场景、证据标准和反证条件，不直接进入实现。

禁止：

- 凭感觉定题；
- 只用 microbenchmark 支撑完整论文结论；
- 把 Ray 说成“很慢”而没有上下文；
- 把 Arrow serialization 说成瓶颈而没有数据；
- 把 Daft/Ray/Lance 产品化适配说成既定事实。
- 把当前候选优化方向说成最终方向，除非已有动机测试、可行性测试和真实形态验证共同支持。
- 因为已经写了某个 benchmark，就反向寻找论文问题。
- 把“跨层调度”写成既定最终贡献，而没有 task/actor、资源配比、模型服务队列或 backpressure 的实验数据。

## 7. 实验结果讲解规则

用户要求讲解实验结果时，默认按以下结构：

1. 实验设置：说明脚本、运行命令、参数配置，以及每个参数代表什么。
2. 实验设计与实验过程：说明模拟的系统链路、每一步做了什么、记录了哪些指标。
3. 严谨性自检：说明控制了哪些变量，哪些地方还不够严谨，哪些结论不能过度解释。
4. 实验数据：严格基于真实 CSV / 报告讲；必要时重新汇总 CSV，不凭记忆讲。
5. 结果解释：区分本地实验事实、合理推断、待确认问题、不能声称的结论。
6. 对课题方向的含义：结合用户目标和需求分析，说明该实验支撑或不支撑哪些 AI 算子 / AI infra 方向。

讲解时禁止把 microbenchmark 直接包装成完整论文结论；必须说明当前证据链还缺哪一环。

## 8. 沟通规则

对外优先表述为：

> 数据库内置 AI 算子的外部分布式数据处理执行链路优化。

需要继续确认：

- PostgreSQL 18.3 内部验证平台的安装、扩展、外部 worker、网络和权限约束；
- 已实现 AI 算子的开源数据库/扩展是哪一个，如何迁移或等价集成到 PostgreSQL 18.3；
- AI 算子在 PostgreSQL 18.3 中是 SQL UDF、表函数、外部执行器，还是批处理服务；
- 达梦是否会用 Ray/Daft/Lance 支撑数据库内置 AI 算子；
- 数据是否以 Arrow/Parquet/Lance/IPC 格式流转；
- 是否存在 join/groupby/repartition/embedding preprocessing；
- 为什么需要 Ray，而不是数据库内部线程池或普通服务。

## 9. 更新规则

当方向、实验或项目规则变化时，同步检查：

- `README.md`
- `PROJECT_INDEX.md`
- `overview/current_direction_and_plan.md`
- `validation/results/feasibility_report.md`
- `research/literature_and_evidence_review.md`
