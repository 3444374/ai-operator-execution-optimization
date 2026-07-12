# 前期可行性实验结果分析

## 1. 当前方向

> 数据库 AI 算子的特征感知任务划分、并行执行与跨层调度优化

## 2. 当前依据

- fake AI_EMBED(text) 端到端实验已显示 coalescing 对整体链路有收益，object/fan-in 可作为 组件可行性入口证据；下一步应拆分收益来源，并验证 task/actor/concurrency、模型服务队列和 backpressure 是否形成更高层瓶颈。

## 3. 实验证据

| 实验 | 状态 | 证据 | 证据分 |
|---|---|---|---|
| Ray small task | done | 最大 task 数 5000，warm-up 后最高平均 task latency 0.183 ms。 | 0 |
| Ray object transfer | done | 小对象平均 round-trip 1.750 ms；大对象平均吞吐 691.6 MB/s。 | 1 |
| Arrow serialization | done | 最大 IPC 大小 12.21 MB；平均 serialize 1.015 ms，deserialize 0.053 ms。 | 1 |
| Shuffle simulation | done | 最大 object 数 4096；fine/coalesced 平均耗时比 0.94x。 | 0 |
| Ray many objects | done | 固定总数据量下，1 objects fan-in 7.268 ms；256 objects fan-in 18.849 ms；放大 2.59x。 | 2 |
| Ray Arrow fan-out/fan-in | done | 最大 RecordBatch object 数 1024；fine/coalesced 平均 fan-in 比 3.17x。 | 2 |
| fake AI_EMBED pipeline | done | 最大输入 object 数 1024；fine/coalesced 平均 fan-in 比 2.16x；端到端耗时比 2.51x。 | 2 |

## 4. 当前方向依据

| 观察结果 | 更适合的方向 |
|---|---|
| small task 开销强 | task batching；仅作为备选，不作为当前主线 |
| object transfer 或 Arrow 成本强 | Object Transfer 优化 / Arrow buffer 路径优化 |
| fine shuffle 明显慢于 coalesced | Shuffle batching / partition-aware execution |
| 固定总量下 object 数量越多 fan-in 越慢 | Object coalescing / fan-in 优化 |
| Arrow RecordBatch fine fan-in 明显慢于 coalesced | 数据库 AI 算子批处理链路中的 object 合并优化 |
| fake AI_EMBED 端到端 fine 明显慢于 coalesced | 继续推进真实数据库 AI 算子外部执行链路验证 |
| 上述证据都弱，但 scan 成本强 | Lance scan / filter / projection pushdown |
| 所有证据都弱 | 回到数据库 AI 算子链路做端到端动机验证 |

## 5. 下一步

1. 基于 fake `AI_EMBED(text)` 端到端结果，继续拆分 build、Ray put、fake embedding、fan-in、write 阶段，判断收益主要来自 object coalescing、task 数减少还是 operator invocation 数减少。
2. 增加 Ray task vs actor、actor pool size、`batch_size × concurrency`、producer-consumer / backpressure 动机实验。
3. 为 offline LLM 增加 token-aware / prefix-aware routing，为 AI_FILTER 增加 selectivity / cascade。
4. 如果端到端证据变弱，优先回到 PostgreSQL 18.3 内部验证平台或同构预演链路，真实采集数据库 AI 算子外部执行画像。
