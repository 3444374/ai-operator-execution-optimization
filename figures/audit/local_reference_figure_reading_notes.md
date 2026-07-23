# Local Reference Figure Reading Notes

整理日期：2026-07-15

用途：记录对 `opening/literature/reference/` 中本地 PDF 图形表达方式的初步观察，并将其转化为本项目下一版策略设计图的画法。该目录只是部分文献子集，不替代完整文献清单。

---

## 1. 本轮看图范围

本轮重点看了三类图：

| 组别 | 本地 PDF | 关注点 |
|---|---|---|
| 异构执行与推理服务 | `ray_data_streaming_batch_2025.pdf`, `vllm_sosp2023.pdf`, `sarathi_serve_osdi2024.pdf` | pipeline/timeline、系统总览、阶段拆分、吞吐-延迟权衡 |
| 分布式执行与 DB AI 优化 | `ray_osdi2018.pdf`, `gaussml_icde2024.pdf`, `galois_sigmod2025.pdf` | code example -> task graph、SQL running example、logical/physical plan 变体 |
| 存储与 DB AI 架构 | `lance_2025.pdf`, `leads_pvldb2024.pdf`, `neurdb_cidr2025.pdf` | 存储布局对比、UDF/模型切片 workflow、AI database 系统边界 |

---

## 2. 对本项目最有用的图形经验

### 2.1 用一个 running example 锚定全图

Ray 论文用代码片段和 task graph 解释系统机制；Galois/GaussML 用 SQL 和 plan 变体解释优化。

对本项目的转化：

```sql
SELECT doc_id, AI_EMBED(text)
FROM documents
WHERE project_id = ?
```

下一版策略图应把这条 SQL 放在左上角。读者先看到“数据库触发 AI 算子”，再沿着 batch queue、submit gate、endpoint queue、GPU service、sink 读下去。

不要在主图入口平均铺开 `AI_EMBED`、`AI_FILTER`、`AI_COMPLETE` 三张卡。三类 workload 可以放在规则表或后续扩展图中。

### 2.2 把策略动作贴到执行位置上

vLLM 的机制图不是列出所有模块，而是在系统总览旁边放 PagedAttention 机制放大；Ray Data 的图把不同执行模型放到同一 pipeline/timeline 下比较。

对本项目的转化：

- `batch / partition` 贴在 `RecordBatch queue` 旁；
- `K_max` 贴在 `Ray submit gate` 旁；
- `routing` 贴在 `Endpoint queues` 旁；
- `writeback ratio` 贴在 `Results / sink` 旁，作为 guardrail。

这样比“右侧动作清单”更直观：读者能看到每个策略动作到底改了执行链路中的哪个位置。

### 2.3 区分数据流、控制流和反馈流

系统论文通常不会让所有箭头都长得一样。Ray Data 和 Sarathi-Serve 的图能看出阶段/资源 lane；vLLM 系统图能看出 engine、scheduler 和 worker 的边界。

对本项目的转化：

- 实线：数据批次流动。
- 虚线：状态观测和端到端指标反馈。
- 小标签：当前状态，如 `queue wait high`、`GPU under-utilized`、`writeback ratio high`。

箭头不交叉、不穿框，控制反馈只回到 `Strategy selector` 或 `next-round config`，不要画成复杂网状图。

### 2.4 用 mini timeline/规则表补充机制，而不是堆术语

Ray Data 的 timeline 展示 batch/streaming 模式差异；Sarathi-Serve 用吞吐-延迟曲线和阶段拆分解释 trade-off。

对本项目的转化：

主策略图下半部分二选一：

1. 简洁规则表：

| Observed state | Next action | Guardrail |
|---|---|---|
| GPU 未饱和，invocation 多 | 增大 batch / 合并 partition | P99 不明显上升 |
| endpoint backlog 高 | 降低 `K_max` 或 least-queued routing | 吞吐不明显下降 |
| writeback ratio 高 | 调整 write batch / path | 端到端收益仍保留 |

2. mini timeline：

```text
默认：many small submits -> endpoint backlog -> late writeback
Ours：coalesced batches -> bounded submits -> stable endpoint queue -> guarded writeback
```

开题报告阶段优先用规则表；后续实验数据充分后，再把 mini timeline 或真实结果曲线放入论文实验图。

### 2.5 不要画成数据库内部优化器或推理引擎内部机制

GaussML、LEADS、NeurDB 的系统图强调数据库内部 ML/AI 能力；vLLM/Sarathi 强调推理引擎内部调度。本课题的位置不同。

对本项目的图形边界：

- 不把策略选择器画进 PostgreSQL optimizer。
- 不把策略选择器画进 vLLM engine。
- 不把 sink guardrail 画成存储引擎创新。
- 明确写成“数据库触发后的外部执行链路入口策略”。

---

## 3. 下一版策略图建议

图题：

```text
Ours-v0: 运行时状态驱动的 AI 算子上游执行策略
```

核心图注：

```text
Ours-v0 在数据库触发的 AI_EMBED 执行过程中观测批处理、队列和写回状态，并据此调整下一轮上游执行配置。
```

主图线框：

```text
SQL example
  -> RecordBatch queue       [state: rows/text length]     [action: batch/partition]
  -> Ray submit gate         [state: in-flight count]      [action: K_max]
  -> Endpoint queues         [state: backlog/queue wait]   [action: routing]
  -> GPU model service
  -> Results / sink          [guardrail: writeback ratio]
  -> E2E metrics             [feedback to next-round config]
```

视觉布局：

- 横向主链路，占画布上半部分。
- 每个策略动作放在对应执行组件下方或右上角。
- 底部放 3 行 `Observed state -> Next action -> Guardrail` 表。
- 右侧很小位置放文献边界：`ideas from AI SQL / LLM serving / data pipeline; baselines from vLLM, Ray Data, COPY`。

不要使用：

- `RC/BL`
- `联合决策面`
- `边界确认`
- 没解释对象的 `vs`
- 三类 workload 平铺的大入口卡片

---

## 4. 需要继续看图的论文

当前本地子集还缺少或尚未重点看：

- Orca：request pool 和 iteration-level scheduling 图，非常适合学习调度循环。
- DistServe / Splitwise：阶段拆分和资源 disaggregation 图，可辅助 `AI_COMPLETE` 扩展。
- FlexPushdownDB / TurboVecDB：写回/边界/成本决策图，可辅助 guardrail 和 baseline 图。
- Cortex AISQL：AI SQL 生产系统边界图，可辅助说明本课题与数据库内部 AI optimizer 的差异。

这些不影响当前 Ours-v0 草图，但会影响后续论文级方法图和实验图。

