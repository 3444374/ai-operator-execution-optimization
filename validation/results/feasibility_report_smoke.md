# 前期可行性实验结果分析

## 1. 当前方向

> 数据库 AI 算子链路中的 Object Transfer 与 fan-in 优化

## 2. 当前依据

- 部分关键实验尚未完成，当前结论只能作为阶段性判断。
- object transfer、RecordBatch fan-in 或 shuffle 证据较强，贴合数据库 AI 算子链路中的中间数据传输问题。

## 3. 实验证据

| 实验 | 状态 | 证据 | 证据分 |
|---|---|---|---|
| Ray small task | done | 最大 task 数 5000，warm-up 后最高平均 task latency 0.183 ms。 | 0 |
| Ray object transfer | done | 小对象平均 round-trip 1.750 ms；大对象平均吞吐 691.6 MB/s。 | 1 |
| Arrow serialization | done | 最大 IPC 大小 12.21 MB；平均 serialize 1.015 ms，deserialize 0.053 ms。 | 1 |
| Shuffle simulation | done | 最大 object 数 4096；fine/coalesced 平均耗时比 0.94x。 | 0 |
| Ray many objects | done | 固定总数据量下，1 objects fan-in 7.268 ms；256 objects fan-in 18.849 ms；放大 2.59x。 | 2 |
| Ray Arrow fan-out/fan-in | done | 最大 RecordBatch object 数 1024；fine/coalesced 平均 fan-in 比 3.17x。 | 2 |
| fake AI_EMBED pipeline | missing | 未发现 fake AI_EMBED(text) 端到端结果。 | 0 |

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

1. 搭建 fake `AI_EMBED(text)` 端到端动机测试，确认 RecordBatch fan-in 现象是否会迁移到批量 embedding / RAG 数据准备链路。
2. 如果端到端结果仍显示 Object/Shuffle 证据最强，再做 Daft local vs Ray 和 Lance/Parquet scan 对照。
3. 如果 small task 证据后续变强，再评估 task batching 是否值得作为辅助优化。
4. 如果端到端证据变弱，优先回到 PostgreSQL + pgvector / PostgreSQL 18.3 真实 AI 算子形态验证。
