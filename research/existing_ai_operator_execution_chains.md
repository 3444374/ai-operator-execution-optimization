# Existing AI Operator Execution Chains

更新日期：2026-08-01

## 结论

现有数据库 AI 算子和 AI 数据处理系统并不都使用 Ray。更准确的分类是：

| 系统 / 路线 | 用户看到的算子形态 | 公开可确认的执行链路 | 是否公开使用 Ray | 对本项目的意义 |
|---|---|---|---|---|
| Snowflake Cortex AISQL | SQL / Python AI functions，如 `AI_COMPLETE`、`AI_FILTER`、`AI_EMBED` | 托管在 Snowflake Service perimeter 内的 AI functions；官方强调 throughput 与 batch processing | 未见公开证据 | 证明数据库 AI SQL 算子是真实工业问题，但不能复现其闭源内部链路 |
| pgai Vectorizer | PostgreSQL 中声明 vectorizer pipeline | PostgreSQL + stateless vectorizer workers；worker 读取队列、调用 embedding endpoint、写回数据库 | 否 | 与本项目“外部 worker + 模型服务 + 写回”最接近 |
| OceanBase AI Function | SQL `AI_COMPLETE` / `AI_PROMPT` / `AI_EMBED` / `AI_RERANK` | 数据库内注册模型与 HTTP endpoint，由 SQL 表达式调用 OpenAI-compatible 模型服务 | 否 | 无 Daft/Ray 的产品级 SQL→endpoint baseline；当前 AutoDL 容器不可部署 observer，只能等待合适环境 |
| PolarDB Polar_AI + EAS | SQL 函数调用自定义模型 | PolarDB 将 SQL 数据转换为服务协议，调用 EAS endpoint，再转回数据库类型 | 否 | 与 OceanBase 同属 SQL→remote endpoint 路线；云端硬件不同，只作工业参考，除非能锁定同物理环境 |
| PolarDB Daft on Ray | DataFrame / 多模态异构算子 | CPU 下载、解码、缩放与 GPU 类 UDF 在同一 Daft 流水线中按算子声明资源，并由 Ray runner 执行 | 是 | 与本项目图像链路最接近；其 staged CPU→GPU 形态是必须补的强系统 baseline，不可由 fused UDF 代替 |
| PostgresML | PostgreSQL 扩展中的 `pgml.embed`、`pgml.transform` 等 | 模型靠近数据库或在数据库内/近数据库执行，强调减少数据搬运 | 否 | 代表“把模型移到数据附近”的对照路线 |
| pgvector | PostgreSQL 向量类型、索引与相似度查询 | 存储和查询向量，不负责 embedding 计算 | 否 | 是本项目 PostgreSQL 写回与检索 baseline |
| Daft + Ray | DataFrame / batch inference / AI functions | Daft 可运行在 Ray 上，负责 DataFrame、partition、batch、shuffle 与按算子资源声明 | 是，可选 Ray runner | 既是项目数据引擎，也是必须独立校准的框架 baseline；要区分 fused GPU UDF 与 staged CPU/GPU pipeline |
| Ray Serve / Ray Data | Python API / serving API | Ray Data 做 batch data processing，Ray Serve 做 model serving、batching、routing、autoscaling | 是 | 适合作为本项目多 endpoint、backpressure、routing 的实验机制 |

因此，本项目不要表述为“现有 AI 算子都用 Ray，所以我们优化 Ray”。更稳的表述是：

> 现有系统已经证明数据库 AI 算子、vectorizer worker、模型服务调用、batch processing 和写回是实际工程形态；本项目选择 Ray/Daft/Lance-like 系统机制作为可控实验平台，研究数据库 AI 负载 触发后的 batch、partition、task/actor、模型服务路由、backpressure 和 writeback 优化。

## 四类可复现实行形态

不同产品都可能经过磁盘、内存、CPU、网络和 GPU，但软件边界不同，不能因为物理层
相似就直接横向排名。当前按以下四类组织 baseline：

| 类型 | 典型系统 | 关键边界 | 本项目如何比较 |
|---|---|---|---|
| in-database inference | PostgresML、Oracle ONNX 类路线 | 模型在数据库进程内或近数据库执行 | 只在同模型/同硬件可部署时做严格性能臂，否则作架构参照 |
| SQL→remote endpoint | OceanBase AI Function、Polar_AI + EAS | 数据库序列化请求并调用外部模型服务 | 指向同一 endpoint、同协议、同并发和同输出语义；否则仅作工业参考 |
| queue-worker | pgai Vectorizer | 数据库触发/队列，外部 worker 异步调用并写回 | 比较 JCT、freshness、重试、写回和多 job，而非只比模型 tokens/s |
| distributed data pipeline | PolarDB Daft on Ray、Ray Data、本项目 | CPU 数据准备与 GPU 推理按 stage/actor 流水化 | 图像主战场；必须比较 fused 与 staged，并分别做 matched-resource 和 independently calibrated best |

这四类不是互斥产品标签，而是执行路径抽象。同一产品可能提供多条路线，例如 PolarDB
同时存在 Polar_AI→EAS 和 Daft-on-Ray 异构流水线。

## 严格 baseline 的比较层级

1. **GPU-resident compute ceiling**：只测模型 forward，定位硬件计算平台，不是项目对手。
2. **direct service ceiling**：绕过数据库/Daft/Ray，测相同协议下服务可实现容量。
3. **framework/system baseline**：Daft fused、Daft staged Native/Ray、Ray Data staged。
4. **product SQL baseline**：OceanBase、PolarDB、pgai 等可部署产品路径。
5. **frozen best project static**：calibration 后在 held-out workload 冻结的项目静态点。
6. **project adaptive policy**：只与第 5 层比较策略增量，并报告相对每 workload oracle 的 regret。

严格排名必须固定输入、模型 revision、输出语义、CPU/GPU 配额、生命周期、计时边界和
sink。`matched-resource` 与各系统 `independently calibrated best-achievable` 分开报告。
云端闭源系统若不能复现同硬件，只能报告用户可见工业参考，不参与 raw throughput 排名。

## 数据路径不能统称“传输”

图像/多模态链路至少拆为：存储→数据库/数据引擎、数据库→worker 序列化与网络、Ray
object store/host copy、pageable/pinned host→device H2D、device→host 结果和写回。
只有某一段位于关键路径、占比可观，并且减少该段能改善 E2E，才可称其为瓶颈。

GPUDirect Storage 只针对存储→GPU 的 I/O/bounce-buffer 路径。当前 CLIP 链路仍需 CPU
JPEG decode/processor，不能仅启用 GDS 就假设绕过 CPU；除非同时引入 GPU decode
（如 nvJPEG/DALI）并作为改变预处理归属的独立实验臂。

## 对 PolarDB 的两条路线如何使用

Polar_AI + EAS 官方流程是 SQL→模型服务调用→数据库类型转换，适合作为 remote-endpoint
产品形态参考。Daft on Ray 官方文档则明确支持在同一 DataFrame 流水线中把下载、解码、
缩放等 CPU 算子与 GPU 类 UDF 分开声明资源并流式重叠。后者意味着当前项目只测
“preprocess+forward 融合在一个 GPU UDF”的 Daft 对照仍不完整：它能证明 stage
separation 相对 fused 形态的收益，但不能代表最强 Daft/PolarDB-style staged baseline。

## 对 Snowflake 是否需要测性能

当前不建议把 Snowflake 性能作为本项目必测 baseline。

原因：

1. Snowflake Cortex AISQL 是托管闭源系统，内部执行器、模型服务队列、调度、写回路径不可见。
2. 本项目目标是优化可控的数据执行与存储过程；Snowflake 测出来的端到端时间只能说明用户可见性能，不能拆分 `DB fetch -> batch -> scheduling -> model service -> writeback`。
3. 如果后续有 Snowflake 账号和预算，可以做“小规模用户可见参考实验”，例如同等语义的 `AI_EMBED` / `AI_COMPLETE` SQL 吞吐，但只能作为工业参照，不能作为严格 apples-to-apples baseline。

更适合当前阶段的做法是：复刻 Snowflake 用户可见的算子语义，而不是复刻 Snowflake 闭源实现。

```text
AI_EMBED(text)      -> vector
AI_FILTER(text, p)  -> boolean
AI_CLASSIFY(text)   -> label
AI_COMPLETE(prompt) -> text / json
```

然后在本项目可控链路中测：

```text
PostgreSQL fetch
  -> Arrow / batch
  -> Ray task / actor / endpoint routing
  -> HTTP / Ray Serve model service
  -> fan-in
  -> PostgreSQL / pgvector / Lance writeback
```

## 对 pgai 是否需要测性能

pgai 更值得参考，是否要测取决于实验问题。

可以测的部分：

1. PostgreSQL + vectorizer worker 的异步写回形态。
2. worker 轮询队列、失败重试、限流、批处理对端到端延迟和吞吐的影响。
3. PostgreSQL 写回与 pgvector 查询链路。

不建议把 pgai 当成长期核心依赖。它的仓库 README 标注自 2026 年 2 月起不再维护或支持，因此更适合作为架构参考和 baseline 思路，而不是论文系统的核心组件。

对本项目最有价值的是借鉴它的链路形态：

```text
application / SQL declaration
  -> PostgreSQL source table
  -> queue / vectorizer config
  -> stateless workers
  -> embedding endpoint
  -> destination table / pgvector
```

这说明后续实验必须加入“worker 写回”而不只是“driver fan-in 后统一写回”。

## 对 OceanBase 是否需要测性能

需要，且优先于继续增加新的 Ray 内部策略。OceanBase V4.4.1+ 的官方文档公开
了 `AI_COMPLETE` SQL 表达式、模型/endpoint 管理和 OpenAI-compatible
Chat Completions URL，理论上可以直接指向同一台 AutoDL 上的 vLLM。

它作为产品级 baseline 回答“现有数据库 AI 算子能否达到相同效果”。但它会
同时把 PostgreSQL 换成 OceanBase，因此必须配套一个同 PostgreSQL 的 bounded
AsyncIO 强 baseline，才能把数据库种类差异与 Daft/Ray 的净贡献分开。

正式性能 arm 的前置条件：

1. 可部署 Community Edition 版本实际包含 AI Function；
2. 直连同机 vLLM，不经过云 AI 网关；
3. Chat messages、输出上限和真实 token 工作量等价；
4. 独立标定 OceanBase 原生并行/多 session 能力，避免串行 strawman；
5. exactly-once、失败和完成时间可审计。

## 对方向的微调

当前方向不需要推翻，但需要从“Ray 调度优化”微调为：

> 面向数据库 AI 算子的模型服务感知外部执行链路优化。

更具体地说，Ray 是机制之一，不是唯一研究对象：

- Ray：task / actor / endpoint routing / backpressure / resource control。
- Daft：batch / partition / DataFrame / map_batches / shuffle 表达层。
- Lance：AI-native 或向量数据写回与外部存储候选。
- PostgreSQL / pgvector：数据库触发、结果写回、向量查询 baseline。

## 下一步实验优先级

1. 继续完善当前 GPU-backed E2E profile，固定链路分段：

```text
PostgreSQL fetch
  -> Arrow / batch 构造
  -> Ray task / actor 调度
  -> HTTP 模型服务调用墙钟时间
  -> fan-in
  -> writeback
```

2. 做 worker 写回对照：

| 对照 | 目的 |
|---|---|
| driver fan-in 后统一写回 | 当前 baseline，计时边界清楚 |
| Ray worker / actor 各自写回 | 验证是否减少 driver fan-in 与单点写回瓶颈 |
| queue / vectorizer-like worker 写回 | 模拟 pgai 式异步 worker 形态 |

3. 做写回 sink 对照：

| sink | 目的 |
|---|---|
| PostgreSQL JSON text | 当前真实模型临时 baseline |
| PostgreSQL `pgvector(384)` | 真实 embedding 维度下的数据库向量写回 |
| Lance / Parquet | 外部 AI-native 存储或文件式落盘 baseline |

4. 保留 Snowflake 为工作负载与论证参照，不把它作为当前必须复现实验。

## 证据来源

- Snowflake Cortex AISQL 官方文档：`https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql`
- Snowflake `COMPLETE` / `AI_COMPLETE` 文档：`https://docs.snowflake.com/en/sql-reference/functions/complete-snowflake-cortex`
- pgai README：`https://github.com/timescale/pgai`
- OceanBase AI Function 语法：`https://en.oceanbase.com/docs/common-oceanbase-database-10000000003678975`
- OceanBase AI Function quick start：`https://en.oceanbase.com/docs/common-oceanbase-database-10000000003450338`
- PolarDB Polar_AI + EAS：`https://help.aliyun.com/en/polardb/polardb-for-postgresql/polar-ai-and-eas-implement-custom-in-library-model-inference`
- PolarDB Daft on Ray 异构算子调度：`https://help.aliyun.com/en/polardb/polardb-for-postgresql/heterogeneous-operator-scheduling`
- PostgresML README：`https://github.com/postgresml/postgresml`
- pgvector README：`https://github.com/pgvector/pgvector`
- Daft on Ray 文档：`https://docs.daft.ai/en/stable/distributed/ray/`
- Ray Data `map_batches`：`https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.map_batches.html`
- Ray Data `ActorPoolStrategy`：`https://docs.ray.io/en/latest/data/api/doc/ray.data.ActorPoolStrategy.html`
- Ray Serve 文档：`https://docs.ray.io/en/latest/serve/index.html`
- NVIDIA GPUDirect Storage：`https://docs.nvidia.com/gpudirect-storage/index.html`
