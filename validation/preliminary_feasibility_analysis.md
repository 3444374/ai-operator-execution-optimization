# 前期可行性分析

生成日期：2026-07-10

## 1. 阶段结论

当前技术路线初步可行，但主线不能写成“Daft/Ray/Lance 集成”。更合理的主线是：

> 面向数据库 AI 算子的特征感知任务划分、并行执行与跨层调度优化。

其中 RecordBatch object coalescing 与 fan-in 是 Phase 0 已经出现本地信号的入口，不是最终全部贡献。

已有实验显示：

- Ray small task 不是当前最强瓶颈；
- Arrow IPC 本身不是当前明显瓶颈；
- 固定总数据量下，object 数量会放大 Ray fan-in 成本；
- Arrow RecordBatch fan-out/fan-in 中，fine/coalesced 平均 fan-in 比约 `3.17x`。

## 2. 当前假设

待验证核心假设：

> 批量 embedding / RAG、AI_FILTER、offline LLM 等数据库 AI 算子链路会产生不同的输入输出形态、token 分布、selectivity、task/object 数和模型服务队列状态；固定 batch / partition / actor / routing 策略可能导致 object/fan-in 成本、并行度不足、模型服务拥塞或写回低效。

这个假设需要分阶段验证。当前 Phase 0 只能支持 object/task 粒度敏感，不能直接证明 CPU/GPU 资源调度、actor routing 或 backpressure 是主瓶颈。

## 3. 已验证内容

| 实验 | 结论 | 对方向的意义 |
|---|---|---|
| Ray small task | 调度开销不强 | 不优先做 Ray Core scheduler/runtime |
| Ray object transfer | 小对象有固定成本 | 支持关注 object 数量 |
| Arrow serialization | IPC 不是主瓶颈 | 不做单纯 Arrow 序列化优化 |
| Ray many objects | object 数量会放大 fan-in | 支持 object coalescing |
| Ray Arrow fan-out/fan-in | fine/coalesced 差异明显 | 最贴近数据库 AI 算子链路 |
| Shuffle simulation | 本地模拟未证明收益 | 作为负结果/对照保留 |

## 4. 当前风险

| 风险 | 影响 | 应对 |
|---|---|---|
| 真实 AI 算子不是批处理 | object/fan-in 动机变弱 | 做 fake `AI_EMBED` 后再接 PostgreSQL 形态 |
| 端到端瓶颈转移到模型推理 | 当前优化收益有限 | 拆分读、算、传、写各阶段耗时 |
| 数据库到外部 worker 成本过高 | 链路不可落地 | Phase 1 测 PostgreSQL + pgvector |
| microbenchmark 外推不足 | 论文说服力不够 | 做端到端 workload 和真实数据库形态 |
| 跨层调度范围过大 | 硕士论文失控 | 先用 Ray 公开 API 做 task/actor/concurrency/backpressure 原型，不改 Ray Core |

## 5. 下一步

优先做：

> fake `AI_EMBED(text)` 端到端结果的收益来源拆分，以及 task/actor/concurrency/backpressure 扩展动机测试。

需要输出：

- CSV 结果；
- end-to-end time；
- rows/s；
- object_count；
- `ray.put` time；
- fan-in time；
- fake embedding time；
- actor idle time / queue wait time；
- token backlog 或模拟模型服务队列；
- backpressure 次数；
- fine vs coalesced 对比。

如果 object/fan-in 仍是主要成本，保留数据链路优化；如果 task/actor/concurrency 或模型服务队列成为主要成本，再把方向升级为特征感知并行执行与跨层调度。无论哪种情况，都需要进入 PostgreSQL 18.3 内部验证平台验证；普通 PostgreSQL + pgvector 只能作为平台暂不可用时的同构预演替身或输出存储组件。
