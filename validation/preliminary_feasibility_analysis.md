# 前期可行性分析

生成日期：2026-07-11

## 1. 文档定位

本文记录前期可行性判断：哪些组件、环境和执行链路已经证明可用，哪些结论只属于本地实验事实，哪些还需要进入动机测试或真实平台验证。

结果目录按当前项目规则拆分：

- `validation/results/`：组件级 benchmark、环境验证、PG18.4 连接验证、dry-run 和 smoke CSV。
- `motivation/results/`：数据库 AI 算子端到端动机测试、PG18.4 系统画像、瓶颈定位和可优化点分析。

因此，PG18.4 当前分成两类证据：

1. 连接与环境可用性：见 `validation/results/pg18_4_connection_validation.md`。
2. 系统画像与瓶颈定位：见 `motivation/results/pg18_4_system_profile_fake_ai_embed.md`。

## 2. 阶段结论

当前技术路线具备继续推进的前期可行性，但主线不能写成“Daft/Ray/Lance 集成”。更合理的表达是：

> 面向数据库内置 AI 算子的外部分布式数据处理执行链路优化。

当前可作为候选调优对象的是 Ray/Daft/Lance 类外部执行链路中的 task/actor 并行度、object 数量、RecordBatch 粒度、fan-in、模型服务队列、backpressure 和写回路径。不能默认达梦最终一定采用 Daft + Ray + Lance，也不能把 Ray vs non-Ray 写成唯一主问题。

更稳妥的实验策略是：

> 先把 Ray/Daft/Lance 类外部执行链路当作候选系统和调优对象，补齐 Python、Ray task、Ray actor、不同 batch/actor 配置、pgvector 写回、真实 CPU/GPU 模型服务等 baseline，再判断收益来源和优化空间。

## 3. 已验证内容

| 实验/验证 | 当前结论 | 证据类型 | 对方向的意义 |
|---|---|---|---|
| Ray small task | 调度开销不是当前最强瓶颈 | 本地实验事实 | 不优先改 Ray Core scheduler/runtime |
| Ray object transfer | 小对象存在固定成本 | 本地实验事实 | 支持关注 object 数量和粒度 |
| Arrow serialization | Arrow IPC 本身不是当前明显瓶颈 | 本地实验事实 | 不把方向收窄为单纯 Arrow 序列化优化 |
| Ray many-object fan-in | 固定总数据量下，object 数量会放大 fan-in 成本 | 本地实验事实 | 支持继续验证 object coalescing |
| Ray Arrow fan-out/fan-in | fine/coalesced 平均 fan-in 比约 `3.17x` | 本地实验事实 | 是贴近数据库 AI 算子数据链路的组件级证据 |
| fake AI_EMBED 端到端动机测试 | fine/coalesced 有端到端差异 | 本地实验事实 | 支持从组件 benchmark 进入系统画像 |
| PG18.4 连接验证 | 本地 PostgreSQL 18.4 + pgvector 可以连接、读写表并完成 smoke 链路 | 本地实验事实 | 证明本地同构预演环境可用，不证明性能收益 |
| PG18.4 系统画像 | 4096 行下，ray_actor fine/coalesced 端到端均值比约 `13.52x`；python fine/coalesced 约 `16.93x` | 本地实验事实 | 说明小粒度 operator invocation/object/fan-in 会显著影响链路，但仍是 fake model 和本地小规模结果 |

## 4. PG18.4 当前进展解释

PG18.4 已经完成两个层次的验证。

第一层是连接验证：系统可以连接 PostgreSQL 18.4，本地 schema、pgvector、读写表、脚本 dry-run 和 256 行 smoke 链路可用。这个结果只说明环境打通，不能用于说明优化收益。

第二层是系统画像：固定 4096 行、128 维 fake embedding，对比 `python` / `ray_actor` executor 以及 `fine` / `coalesced` strategy。结果显示 fine 粒度会显著增加 operator invocation、object 数量、bounded wait 和 fan-in 相关成本；coalesced 明显降低端到端耗时。这个结果可以作为动机测试和瓶颈定位材料，但不能外推为 PostgreSQL 18.3 内部平台或真实 GPU 模型服务结论。

## 5. 当前假设

待验证核心假设：

> 数据库 AI 算子链路中，不同算子的输入输出形态、token 分布、selectivity、task/object 数量、模型服务队列状态和写回方式会共同影响执行效率；固定 batch、partition、actor、routing 或 backpressure 策略可能导致 object/fan-in 成本、并行度不足、模型服务拥塞或写回低效。

当前证据支持“粒度和 fan-in 敏感”这一局部判断；还不能直接证明 CPU/GPU 资源调度、actor routing、Daft shuffle 或 Lance 写入一定是主瓶颈。

## 6. 当前风险

| 风险 | 影响 | 应对 |
|---|---|---|
| 真实 AI 算子不是批处理形态 | object/fan-in 动机变弱 | 用真实 embedding/RAG 数据准备、AI_FILTER 和离线 LLM 场景分别验证 |
| 端到端瓶颈转移到模型推理 | 数据链路优化收益有限 | 拆分数据库读取、Arrow 构造、Ray 调度、模型服务、fan-in、写回耗时 |
| Ray actor 对照不足 | 无法解释收益来自 Ray、batch 还是 invocation 减少 | 补 Python batched、Ray task、Ray actor batch size/actor count baseline |
| GPU 过早接入 | 无法判断 GPU 收益来自硬件还是链路调优 | 先完成 CPU/fake model baseline，再接真实 CPU embedding 和 GPU |
| PG18.4 本地替身与 PG18.3 内部平台不一致 | 论文证据链不足 | 把 PG18.4 作为同构预演，最终进入 PostgreSQL 18.3 内部验证平台复测 |
| microbenchmark 外推不足 | 论文说服力不足 | 保留组件实验，但正式论证优先引用端到端动机测试和真实形态验证 |
| 跨层调度范围过大 | 硕士论文失控 | 先用公开 API 做 task/actor/concurrency/backpressure 原型，不改 Ray Core |

## 7. 下一步实验顺序

优先补齐 baseline，而不是立即进入 GPU：

1. Python serial 与 Python batched worker，对照 batch 本身的收益。
2. Ray task baseline，对照 Ray task 与 Ray actor 的差异。
3. Ray actor batch size、actor count、max inflight 矩阵，对照并行度和反压。
4. pgvector `vector(128)` 写回路径，对照 fake 写回和真实向量写入成本。
5. 真实 CPU embedding 小模型，对照 fake model 与真实前后处理/推理成本。
6. GPU / Ray Serve / vLLM 形态，对照模型服务路由、队列、backpressure 和硬件加速收益。

每组实验都需要输出 CSV、运行命令、参数表、指标解释和结论边界。正式瓶颈定位和可优化点分析写入 `motivation/results/`；只证明环境和组件可用的结果写入 `validation/results/`。

## 8. 当前可说与不可说

可以说：

- PG18.4 本地同构预演环境已经打通。
- 组件级 benchmark 显示小 object/fan-in 存在可观成本。
- PG18.4 fake AI_EMBED 系统画像显示 fine 粒度显著劣于 coalesced。
- 当前有必要继续做多 baseline，拆分 batch、Ray、actor、fan-in、writeback 和模型服务收益来源。

不能说：

- 达梦已经确定采用 Daft + Ray + Lance。
- 当前结果已经证明 PostgreSQL 18.3 内部平台或真实 GPU 场景收益。
- Ray 本身很慢。
- Arrow serialization 是主要瓶颈。
- 现在应该直接上 GPU 并把 GPU 结果当作优化收益。
