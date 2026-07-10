# 当前方向分析

生成日期：2026-07-10

## 1. 环境结论

当前不需要使用 conda。项目内 `.venv` 已可运行 Ray、PyArrow、NumPy。

| 依赖 | 版本 |
|---|---|
| Ray | 2.56.0 |
| PyArrow | 24.0.0 |
| NumPy | 2.5.1 |

## 2. 已完成实验

结果文件位于 `validation/results/`。

| 实验 | 文件 | 结论 |
|---|---|---|
| Ray small task | `ray_small_task.csv` | small task 不是当前最强瓶颈 |
| Ray object transfer | `ray_object_transfer.csv` | 小 object 有毫秒级固定成本 |
| Arrow serialization | `arrow_serialization.csv` | Arrow IPC 本身不是主瓶颈 |
| Shuffle simulation | `shuffle_simulation.csv` | 本地模拟未证明 coalescing 更快 |
| Ray many objects | `ray_many_objects.csv` | object 数量增加会放大 fan-in |
| Ray Arrow fan-out/fan-in | `ray_arrow_fanout_fanin.csv` | RecordBatch fine/coalesced 差异明显 |
| fake AI_EMBED pipeline | `fake_ai_embed_pipeline.csv` | 端到端批量 embedding 链路中 fine/coalesced 差异明显 |
| AI operator granularity attribution | `ai_operator_granularity_attribution.csv` | 拆分 task 数、Ray 引用数、fan-in 依赖数和 operator invocation 的影响 |
| AI operator backpressure | `ai_operator_backpressure.csv` | 模拟模型服务消费慢时的 queue wait、token backlog 和 backpressure |

## 3. 关键观察

### Ray small task

warm-up 后最高平均 task latency 约 `0.183 ms`。

判断：

> 当前不支持把主线放到 scheduler 或 runtime。

### Arrow serialization

最大 IPC 大小约 `12.21 MB`，平均 serialize 约 `1.015 ms`，deserialize 约 `0.053 ms`。

判断：

> Arrow IPC 本身不是当前明显瓶颈，不建议做单纯 Arrow serialization 优化。

### Ray many objects fan-in

固定 `16MB` 总数据量下：

| object 数量 | fan-in |
|---:|---:|
| 1 | 约 7.27 ms |
| 256 | 约 18.85 ms |

判断：

> 固定总量下，object 数量从 `1` 到 `256` 后，fan-in 时间约放大 `2.59x`。

### Ray Arrow fan-out/fan-in

固定 `65536` 行、`128` 维 embedding：

| upstream | downstream | fine objects | coalesced objects | fine fan-in | coalesced fan-in | 比值 |
|---|---:|---:|---:|---:|---:|---:|
| 8 | 8 | 64 | 8 | 约 7.88 ms | 约 4.71 ms | 约 1.67x |
| 8 | 32 | 256 | 32 | 约 31.22 ms | 约 13.96 ms | 约 2.24x |
| 32 | 8 | 256 | 8 | 约 16.82 ms | 约 4.16 ms | 约 4.04x |
| 32 | 32 | 1024 | 32 | 约 76.85 ms | 约 16.64 ms | 约 4.62x |

自动分析平均 fine/coalesced fan-in 比约 `3.17x`。

判断：

> 在 Arrow RecordBatch 形态下，小 object 多会明显放大 Ray fan-in 成本；coalescing 能稳定降低 fan-in 时间。

### fake AI_EMBED(text) 端到端链路

固定 `65536` 行、`128` 维 embedding、`32` 个文本 token：

| 指标 | 结果 |
|---|---:|
| 最大输入 object 数 | 1024 |
| fine/coalesced 平均 fan-in 比 | 约 2.16x |
| fine/coalesced 平均端到端耗时比 | 约 2.51x |

判断：

> RecordBatch fan-in 现象已经迁移到模拟数据库 `AI_EMBED(text)` / 批量 embedding 链路；当前可以继续验证收益来源，并推进到 PostgreSQL + pgvector 或外部 worker 真实形态。

### AI operator granularity attribution

固定 `65536` 行、`512 bytes/row` payload，比较四种执行组织方式：

| 策略 | 含义 |
|---|---|
| fine | 每个 upstream/downstream slot 一个 AI operator task，下游直接 fan-in |
| two_stage_coalesced | 先细粒度执行，再按 downstream 增加 coalesce task |
| downstream_coalesced | 直接按 downstream 聚合成粗粒度 AI operator task |
| upstream_bundled | 按 upstream 打包执行，保留下游逻辑 payload |

`repeat 1/2` 平均结果中，`upstream=32, downstream=32`：

| 策略 | total Ray tasks | fan-in Ray refs | e2e |
|---|---:|---:|---:|
| fine | 1056 | 1024 | 139.27 ms |
| two_stage_coalesced | 1088 | 32 | 176.00 ms |
| downstream_coalesced | 64 | 32 | 16.41 ms |
| upstream_bundled | 64 | 1024 | 19.24 ms |

判断：

> 当前 fake 链路中，端到端收益主要不只是 fan-in 依赖数减少，还明显来自 AI operator task / invocation 数减少。`two_stage_coalesced` 虽然把 fan-in Ray refs 从 `1024` 降到 `32`，但总 task 更多，端到端反而更慢；`downstream_coalesced` 和 `upstream_bundled` 把 Ray task 数降到 `64` 后，端到端显著下降。

这个结果加强了“任务划分 + 并行度控制”动机，但仍然不能说明真实模型服务一定同样受益。

### AI operator backpressure

该实验不启动 Ray，使用离散事件模拟数据库/CPU 侧持续生产请求、模型 replica 消费 token 的过程。固定 `512` requests、长请求比例 `20%`、长请求 token multiplier `8`。

`repeat 1/2` 平均结果中：

| producer rate | replicas | queue limit | tokens/s | avg queue wait | max in-flight | max token backlog |
|---:|---:|---:|---:|---:|---:|---:|
| 2000 rps | 2 | 0 | 23864.52 | 4768.50 ms | 499 | 231552 |
| 2000 rps | 2 | 8 | 23864.52 | 114.41 ms | 8 | 5568 |
| 2000 rps | 4 | 0 | 47408.83 | 2299.64 ms | 487 | 227904 |
| 2000 rps | 4 | 8 | 47408.83 | 37.98 ms | 8 | 6912 |

判断：

> 在模型服务吞吐固定时，无界提交不会提高 tokens/s，但会显著放大 in-flight 请求数、token backlog 和平均 queue wait。有界 backpressure 在保持同等 tokens/s 的同时，把队列压力降到可控范围。

这个结果支持继续验证“模型服务状态感知 backpressure / routing”，但它仍然是模拟实验，还需要 Ray actor / Ray Serve / vLLM 或真实模型服务验证。

## 4. 当前最强候选方向

当前最值得继续验证的候选方向：

> 数据库 AI 算子的特征感知任务划分、并行执行与跨层调度优化。

原因：

- object/fan-in/coalescing 直接对应已有 Phase 0 最强信号；
- 新方向仍能挂靠批量 embedding / RAG 数据准备；
- 新方向进一步覆盖 task/actor 并行度、资源配比、模型服务路由、backpressure 等 AI infra 能力；
- 比传统数据库内核或单纯系统集成更符合用户目标。

但这还不是最终方向。当前动机测试仍然偏 fake workload，只能证明 object/task 粒度敏感，不能证明资源调度或模型服务路由就是主瓶颈。需要继续比较其他 AI 算子场景，并在真实数据库/真实 AI 算子或外部 worker 形态中验证瓶颈是否仍存在。

## 5. 暂不选择

暂不作为主线：

- 单纯 Arrow serialization；
- scheduler/runtime；
- 传统数据库 GPU 查询算子；
- 泛泛 Daft/Ray/Lance 集成；
- 泛泛数据库 AI 算子框架。

## 6. 下一步

已补：

> fake `AI_EMBED(text)` 端到端动机测试。

> AI operator granularity attribution 与 AI operator backpressure 动机补强实验。

下一步目的：

- 基于 granularity attribution 结果，继续做更真实的 Ray task vs actor、actor pool size、`batch_size × concurrency` 对照；
- 基于 backpressure 结果，接入 Ray actor 或 Ray Serve endpoint，记录真实 queue wait / token backlog / actor idle time；
- 做 Daft local vs Ray、Lance/Parquet scan、以及其他 AI 算子场景对照；
- 进入 PostgreSQL + pgvector / PostgreSQL 18.3 真实 AI 算子形态验证。

如果真实形态中端到端实验仍显示 object/fan-in 是瓶颈，可收敛为数据链路优化；如果 task/actor、模型队列或 backpressure 证据更强，再收敛为特征感知并行执行与跨层调度。
