# 可行性验证指南

生成日期：2026-07-10

## 1. 验证目标

本阶段目标不是完整系统实现，而是判断：

> 数据库 AI 算子 / 批量 embedding / RAG 数据准备链路中，RecordBatch object 数量和 fan-in 是否形成可优化瓶颈。

更新后的阶段目标还包括：

> 判断该瓶颈是否只是 object/fan-in 问题，还是会进一步扩展为 task/actor 并行度、CPU/GPU 或 CPU/model-service 流水线、模型服务队列和 backpressure 问题。

## 2. 第一阶段不做

- 不搭集群；
- 不改 Ray / Daft / Lance 源码；
- 不做完整数据库内核接入；
- 不做传统 GPU 查询算子；
- 不做模型 kernel 优化；
- 不用 microbenchmark 直接代替论文结论。

## 3. 已有实验

| 实验 | 目的 | 状态 |
|---|---|---|
| Ray small task | 判断 task 调度是否是主瓶颈 | 已完成 |
| Ray object transfer | 判断小 object 固定成本 | 已完成 |
| Arrow serialization | 判断 Arrow IPC 是否主瓶颈 | 已完成 |
| Ray many objects | 判断固定总量下 object 数量影响 | 已完成 |
| Ray Arrow fan-out/fan-in | 判断 RecordBatch object 数量影响 | 已完成 |
| Shuffle simulation | 本地对照，不代表真实 Ray shuffle | 已完成 |

## 4. 当前最关键实验

下一步做 fake `AI_EMBED(text)` 端到端动机测试。

链路：

```text
generate documents
  -> Arrow RecordBatch
  -> Ray fake embedding
  -> fine objects / coalesced objects
  -> fan-in
  -> output CSV
```

对比：

| 策略 | 含义 |
|---|---|
| fine | 上游产生大量小 RecordBatch object |
| coalesced | 合并为更少、更大的 RecordBatch object |

指标：

- total rows；
- embedding dim；
- object_count；
- avg object size；
- data build time；
- `ray.put` time；
- fake embedding time；
- fan-in time；
- end-to-end time；
- rows/s。

完成该实验后，不直接定题，而是继续做：

- task/object/put/fan-in 收益来源拆分；
- Ray task vs actor 对比；
- `batch_size × concurrency` 扫描；
- CPU preprocess worker 与 model actor 的 producer-consumer / backpressure 模拟；
- offline LLM token-aware / prefix-aware batching；
- AI_FILTER selectivity / cascade。

## 5. 判定标准

| 观察 | 结论 |
|---|---|
| coalesced 明显快 | object coalescing 方向成立 |
| fine 主要慢在 fan-in | 重点优化 fan-in / ObjectRef 拉取 |
| fine 主要慢在 put | 重点优化 object 数量和 batch size |
| 主要慢在 embedding | 数据链路优化不是当前主瓶颈 |
| 主要慢在写回 | 需要转向批量写入 / COPY / staging table |
| 差异不明显 | 不能继续强推 object coalescing |
| actor idle time 高 | 重点优化上游供给、batch 形成或数据局部性 |
| queue wait / token backlog 高 | 重点优化模型服务路由、concurrency 或 backpressure |
| object store 压力高 | 重点优化 in-flight batch、object 生命周期和生产消费速率 |

## 6. 报告模板

```markdown
# 动机实验报告

## 1. 实验目的
说明要验证的数据库 AI 算子链路瓶颈。

## 2. 实验设计
说明数据规模、embedding dim、batch size、fine/coalesced 策略。

## 3. 实验结果
列出 CSV 结果和关键指标。

## 4. 分析
拆分 read/build/put/embed/fan-in/write 时间。

## 5. 结论
说明方向是否继续、下一步是否接 PostgreSQL + pgvector。
```
