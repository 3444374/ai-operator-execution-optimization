# Strategy Figure Micro-design Points

整理日期：2026-07-15

用途：在继续重绘“优化策略设计图”之前，把策略拆成若干可画的小机制点。每个机制点先说明优化对象、参考论文图形范式、适合的画法、需要的实验证据和当前风险。这个文件不替代 `experiments/plans/strategy_design_literature_basis.md`，只服务后续画图设计。

---

## 0. 当前判断

不要急着把所有策略塞进一张总图。更稳的做法是：

1. 主图只画一个运行时控制闭环：数据库 AI 查询 -> batch/partition 队列 -> 策略选择器 -> Ray actor / GPU endpoint -> sink -> 端到端指标回填。
2. 机制小图分别画 4-5 个优化点：数据组织、反压、路由、写回约束、跨 workload 规则。
3. 每个机制小图只回答一个问题，避免“看起来很完整，但不知道到底优化了什么”。

---

## 1. 可画的小优化点

### 1.1 Workload-aware batch / partition 选择

优化对象：

- `batch_size`
- `partition_count`
- `object_merge`
- operator invocation 数

核心问题：

> 同样是数据库 AI 算子，不同 workload 的输入长度、输出大小、selectivity 和模型 batch scaling 会改变最优 batch / partition 工作点。

参考图形范式：

- vLLM：用曲线展示 batch / throughput / latency 的工作点，而不是只报单点。
- Ray Data：用 CPU/GPU stage lane 展示 partition streaming 和异构阶段。
- Cortex AISQL / Smart：用 AI operator 语义影响优化决策。

建议画法：

- 小图 A：`AI_EMBED(document.text)` 的 running example，从 `RecordBatch` 被切成 3 种 partition/batch 形态。
- 小图 B：右侧放一个小规则表：
  - rows 少、GPU 未饱和 -> 合并 batch；
  - rows 多、fan-in 高 -> 控制 partition 数和 object 数；
  - FILTER selectivity 低 -> 小 batch / early filter；
  - COMPLETE token 长 -> 小 batch / token-aware grouping。

需要证据：

- 模型 batch scaling 曲线。
- `batch_size x partition_count` grid search。
- 阶段拆解：GPU request wall、fan-in、writeback ratio。

当前风险：

- 如果固定最优 batch 在所有 workload 下都足够好，这个点只能作为 baseline 调参，不足以单独支撑方法贡献。

---

### 1.2 Bounded in-flight 与服务入口反压

优化对象：

- `K_max`
- actor pool size
- endpoint queue depth
- queue wait / P99 latency

核心问题：

> 上游 Ray task 提交过快时，等待可能堆积到模型服务入口；过度限制又会让 GPU 空闲，因此需要找到吞吐和尾延迟之间的工作点。

参考图形范式：

- Orca：画 request pool、scheduler 和每个 iteration 的执行交互。
- vLLM：画吞吐-延迟曲线，关注 tail latency。
- Sarathi-Serve / DistServe：用阶段分离解释为什么不同阶段要不同调度。

建议画法：

- 控制环小图：`batch queue -> K_max gate -> endpoint queue -> GPU service -> metrics`。
- 用虚线从 `queue wait / backlog / P99` 回到 `K_max gate`，表示状态反馈。
- 不画成“Ray 很慢 -> 我们加速 Ray”，而是画成“服务入口压力控制”。

需要证据：

- `K_max` sweep 的吞吐-延迟曲线。
- endpoint queue wait 占比。
- adaptive vs static `K_max` 在 workload 变化下的比较。

当前风险：

- 如果接入 vLLM / Ray Serve 后外部 `K_max` 收益很小，应把这个点降级为“外部入口约束”，不要写成独立 GPU 调度创新。

---

### 1.3 Endpoint routing 与负载感知提交

优化对象：

- routing policy：round-robin / least-queued / workload-aware
- endpoint load
- actor-to-endpoint mapping

核心问题：

> 多 endpoint 时，简单 round-robin 不一定能处理长短 batch、不同 token 长度或 endpoint backlog 的不均衡。

参考图形范式：

- Orca：请求池中的请求动态进入执行引擎。
- Ray Serve：路由器根据 replica 状态选择后端。
- DistServe / Splitwise：阶段差异会导致资源侧负载不均。

建议画法：

- 画 3 条 endpoint lane，每条 lane 前有 queue。
- 左侧策略选择器读 queue depth 和 batch type，选择目标 endpoint。
- 箭头必须清楚落到具体 queue，不穿过 lane 或卡片。

需要证据：

- endpoint_count x routing_strategy 对比。
- 均匀 workload 与混合 workload 分开。
- P50/P99 或 queue wait 分布，而不只是总耗时。

当前风险：

- 单 endpoint 场景没有 routing 贡献；图中必须显示这是 multi-endpoint 条件下才成立。

---

### 1.4 写回约束与上游收益保护

优化对象：

- write batch rows
- driver fan-in / worker-direct / queue-worker
- COPY / UPSERT
- deferred index
- sink type

核心问题：

> 上游 batch 和调度变快后，如果写回占比升高，端到端收益会被吞噬；写回是约束和验证环节，不一定是当前主优化贡献。

参考图形范式：

- TurboVecDB：先拆开索引构建/写入瓶颈，再讲优化。
- Delta Lake / pgai：多 worker append 或 queue-worker 解耦作为工程形态参考。
- FlexPushdownDB：用 cost boundary 表达什么时候选择一种执行位置。

建议画法：

- 不画成存储引擎创新图。
- 在主控制环右侧画 `sink guardrail`：如果 `writeback ratio` 高，触发 sink / write batch / write path 检查。
- 可以单独画三路写回对比小图：driver fan-in、worker-direct、queue-worker 三个泳道，输出同一个 sink。

需要证据：

- B 系列：UPSERT vs COPY、logged vs unlogged、online vs deferred index。
- 三路写回架构对比。
- 端到端阶段拆解。

当前风险：

- 如果 COPY + deferred index 已把写回占比压到很低，写回只保留为强 baseline，不应继续作为主图中心。

---

### 1.5 Trigger -> Action -> Guardrail 规则表

优化对象：

- 策略表达方式本身。
- 将阶段画像结果转化为可执行配置。

核心问题：

> 现在最适合开题阶段的策略不是 learned optimizer，而是 rule-table guided upstream execution strategy；规则必须来自可观测 trigger，而不是抽象标签。

参考图形范式：

- 系统论文常把机制图和规则/代价表分开：上半部画机制，下半部用小表约束决策。
- vLLM/Orca 这类论文不会把 baseline 画成自己系统模块；baseline 放在实验图。

建议画法：

| Trigger | Action | Guardrail |
|---|---|---|
| GPU 未饱和且 invocation 多 | 增大 batch / 合并 invocation | P99 不明显变差 |
| endpoint backlog 高 | 降低 `K_max` 或改 routing | 吞吐不明显下降 |
| FILTER selectivity 低 | early filter / 小 batch | 质量指标不下降 |
| token 长尾明显 | token-aware dispatch | 长请求不阻塞短请求 |
| writeback ratio 高 | 检查 COPY / write batch / path | 端到端收益仍保留 |

需要证据：

- 每条 trigger 至少对应一个可跑实验或计划中的验证点。
- 不能把未验证规则写成已证明结论。

当前风险：

- 如果规则表太大，会退化成术语清单。最终论文图最多保留 4-5 条关键规则。

---

## 2. 推荐图组，而不是单图

### Figure S1：研究方案总览

现有 `cross_layer_method_framework.*` 继续承担这个角色。它回答“课题做什么”，不承担具体策略机制证明。

### Figure S2：Ours-v0 运行时策略闭环

下一版重点图。结构：

```text
Database AI query
  -> RecordBatch / partition queue
  -> Strategy selector
  -> Ray actor pool / endpoint queues
  -> GPU model service
  -> sink / writeback guardrail
  -> E2E metrics
       -> feedback to Strategy selector
```

画法：

- 上半部为控制闭环。
- 下半部为 `Trigger -> Action -> Guardrail` 规则表。
- 只用一个 running example：优先 `AI_EMBED(document.text)`。
- `AI_FILTER` 和 `AI_COMPLETE` 只出现在规则表扩展行。

### Figure S2 的直接明了版线框

核心目标：

> 让读者用一个 `AI_EMBED(document.text)` 例子看懂：系统观察到什么状态，策略改了哪个执行参数，端到端指标如何回填下一轮。

建议不要画成 7 个同等重要的大模块，而是画成“一个例子 + 三个决策点”：

```text
[SQL: SELECT AI_EMBED(text) FROM docs]
                 |
                 v
        [RecordBatch queue]
       state: rows, text length
       decision 1: batch / partition
                 |
                 v
          [Ray submit gate]
       state: in-flight count
       decision 2: K_max
                 |
                 v
 [Endpoint queues] -> [GPU model service]
       state: backlog, queue wait
       decision 3: routing
                 |
                 v
          [Results / sink]
       guardrail: writeback ratio
                 |
                 v
          [E2E metrics]
          dashed feedback to selector
```

画面组织：

- 左上角只放一条 SQL，不放三类 workload 卡片。
- 中间主链路只放 5 个实体：`RecordBatch queue`、`Ray submit gate`、`Endpoint queues`、`GPU model service`、`Results / sink`。
- 三个决策点贴在链路旁边：`batch / partition`、`K_max`、`routing`。
- 写回不画成第四个主优化点，而是放在 sink 旁边作为 `guardrail: writeback ratio`。
- 底部放 3 行规则表，不超过 3 行：

| Observed state | Next action | Guardrail |
|---|---|---|
| GPU 未饱和，invocation 多 | 增大 batch / 合并 partition | P99 不明显上升 |
| endpoint backlog 高 | 降低 `K_max` 或 least-queued routing | 吞吐不明显下降 |
| writeback ratio 高 | 调整 write batch / path | 端到端收益仍保留 |

为什么这个版本更直观：

- 它不要求读者先理解”研究内容一/二/三”或”联合决策面”。
- 它把策略动作直接贴到执行位置上，而不是放在右侧动作清单里。
- 它保留反馈闭环，但反馈只回到 `Strategy selector / next-round config`，不会把全图画成复杂控制系统。

标题建议：

```text
Ours-v0: 运行时状态驱动的 AI 算子上游执行策略
```

图注第一句建议：

```text
Ours-v0 observes batch, queue and writeback states during a database-triggered AI_EMBED execution and adjusts the next-round upstream execution configuration.
```

中文版：

```text
Ours-v0 在数据库触发的 AI_EMBED 执行过程中观测批处理、队列和写回状态，并据此调整下一轮上游执行配置。
```

### Figure S3：服务入口反压机制

如果研究内容二成为主贡献，再单独画。结构：

```text
partition queue -> K_max gate -> endpoint queues -> GPU service lanes
                        ^                    |
                        | queue wait/P99     |
                        +--------------------+
```

### Figure S4：写回约束三路对比

如果研究内容三数据显示写回占比高，再单独画。结构：

```text
driver fan-in -> COPY/sink
worker-direct -> COPY/sink
queue-worker -> COPY/sink
```

---

## 3. 优先下载/精读论文链接

以下链接用于下载 PDF 或查看 paper page。优先顺序按“对策略图设计的帮助”排序。

| 优先级 | 论文/系统 | 主要学习点 | 链接 |
|---|---|---|---|
| P0 | vLLM / PagedAttention, SOSP 2023 | 吞吐-延迟曲线、核心机制图、KV block/page 的可视化方式 | https://arxiv.org/abs/2309.06180 |
| P0 | Orca, OSDI 2022 | request pool、scheduler、iteration-level interaction 的机制图 | https://www.usenix.org/conference/osdi22/presentation/yu |
| P0 | Ray Data Streaming Batch Inference, 2025 | CPU/GPU lane、streaming batch、异构 pipeline 图 | https://arxiv.org/abs/2501.12407 |
| P0 | Cortex AISQL, SIGMOD 2026 | AI SQL operator、optimizer 边界、AI cost/selectivity 叙事 | https://arxiv.org/abs/2511.07663 |
| P1 | Sarathi-Serve, OSDI 2024 | chunked prefill、避免长 prefill 阻塞 decode 的时序图 | https://www.usenix.org/conference/osdi24/presentation/agrawal |
| P1 | DistServe, OSDI 2024 | prefill/decode disaggregation、阶段分离和资源 lane 图 | https://www.usenix.org/conference/osdi24/presentation/zhong-yinmin |
| P1 | Splitwise, ISCA 2024 | prompt/decode phase splitting，适合借鉴阶段拆分表达 | https://dl.acm.org/doi/10.1145/3651890.3672264 |
| P1 | FlexPushdownDB, PVLDB 2021 | cost-driven decision boundary，适合作为边界/对照，不直接搬到主图 | https://www.vldb.org/pvldb/vol14/p3881-yang.pdf |
| P1 | TurboVecDB, PVLDB 2025 | 先拆瓶颈再优化的存储/索引实验图思路 | https://www.vldb.org/pvldb/vol18/ |
| P2 | GaussML, ICDE 2024 | DB4AI 内部路线对照，避免我们画成数据库内核优化器 | https://ieeexplore.ieee.org/ |
| P2 | Smart, VLDB Journal 2025 | ML predicate rewrite / progressive inference 叙事，对 AI_FILTER 有参考 | https://link.springer.com/ |

说明：

- P0 是画下一版策略图前最值得先看的。
- P1 用于补机制细节和实验图表达。
- P2 主要作为边界和路线对照，防止把本课题画成 DB4AI 内部优化。
- 如果下载 PDF，优先把 P0/P1 放到 `research/reference/`，并在 `research/ai_operator_literature_inventory.md` 登记来源。

---

## 4. Reviewer 风险检查

画图前逐项检查：

- 图是否让人看出本文策略在“数据库外部执行链路入口/上游”生效，而不是在 vLLM 内部或数据库优化器内部生效？
- 图里是否有可观测 trigger，而不是“阶段画像”“联合验证”这类抽象词？
- 每个动作是否对应可执行参数，如 `batch_size`、`partition_count`、`K_max`、`routing_policy`、`write_batch_rows`？
- 写回是否作为 guardrail，而不是被画成当前主贡献？
- 是否避免了 `RC/BL`、未解释的 `vs`、`联合决策面`、`边界确认`？
- 箭头是否只表达必要的数据流或控制流，不交叉、不穿过卡片、不越界？

---

## 5. 下一步

1. 先精读 P0 四篇的图，确认下一版 `optimization_strategy_logic.*` 是否采用控制闭环。
2. 如果决定重画，先画线框草图，再写 Python/PIL 脚本。
3. 重画后必须同时检查 PNG 和 SVG，一并更新 audit。
