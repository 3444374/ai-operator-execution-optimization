# Top-venue Strategy Figure Design Notes

整理日期：2026-07-15

用途：在继续绘制本项目“策略设计图”前，先从顶会系统论文中抽取设计图范式，避免把方法图画成术语堆叠或流程罗列。

本文件是图形设计口径，不替代实验计划或文献综述。后续重绘 `optimization_strategy_logic.*` 或新增算法细节图前，先读本文件。

---

## 1. 参考样本

本轮参考的论文/系统图类型：

| 论文/系统 | 图形启发 | 对本项目的含义 |
|---|---|---|
| vLLM / PagedAttention, SOSP 2023 | 先画“为什么现有系统浪费”，再画核心机制；问题图不是泛泛 pipeline，而是 memory layout + growth curve | 我们的策略图应先让读者看到“哪个状态触发哪个动作”，不能只列模块 |
| Orca, OSDI 2022 | 用一个具体 request/token 例子画出 request pool、scheduler、execution engine 的 per-iteration interaction | 我们应画“运行时调度循环”，而不是静态三列术语表 |
| Cortex AISQL, SIGMOD Companion 2026 | 架构图先定义系统边界；后续 workload 图用简单柱状/组成图证明 AI cost 与多表 workload 的动机 | 我们应把策略图和动机/结果图分开；策略图只讲选择逻辑，动机用数据图支撑 |
| Ray Data Streaming Batch, arXiv 2025 | 用 CPU/GPU/resource lanes 和 partition streaming 表达异构执行 | 我们的数据组织图应使用资源/阶段 lane，而不是普通箭头堆叠 |
| Smart / DB4AI 类工作 | 方法图通常把 query rewrite、cost decision、execution plan 分层 | 我们应强调“外部执行链路的策略选择层”，不要画成数据库内部优化器 |

---

## 2. 顶会系统论文方法图的共性

### 2.1 它们不会只画“大框 + 小框”

好的系统方法图通常有一个具体机制：

- vLLM：KV cache 被拆成 block，block table 映射 logical blocks 到 physical blocks。
- Orca：request pool 中不同请求在每个 iteration 被 scheduler 重新选择。
- Cortex AISQL：AI operators 进入 SQL engine 后，optimizer / scheduler / inference platform 的边界明确。
- Ray Data：partition 在 CPU/GPU stage 之间 streaming，而不是全量 barrier。

对本项目的要求：

> 策略图必须展示“规则如何被触发”和“配置如何改变执行”，不能只是输入、规则、输出三列。

### 2.2 它们常用一个 running example

Orca 用 token 序列 `x1, x2, ...` 展示 early-finished / late-joining requests；Cortex AISQL 用具体 SQL query 和 schema 展示 AI_FILTER / AI_JOIN 为什么改变计划。

对本项目的要求：

> 需要选择一个贯穿图的例子，例如 `AI_EMBED(document.text)` 或 `AI_FILTER + AI_EMBED`。图中每个策略动作都要能落到这个例子上。

不要在同一张图里平均展开 EMBED/FILTER/COMPLETE 三类 workload。三类 workload 可以作为规则表的一小栏，不能抢走主机制。

### 2.3 它们区分 data path 和 control path

顶会系统图通常不会让所有箭头都长得一样：

- 实线表示数据流。
- 虚线表示 control / feedback / per-iteration interaction。
- 分层或泳道表示不同资源或系统边界。

对本项目的要求：

> 后续策略图应明确区分：数据批次如何流动、策略选择器如何读状态、配置如何作用到执行器、端到端指标如何回填。

### 2.4 它们把 baseline 和贡献分开

vLLM 不把 FasterTransformer/Orca 画成自己的模块；Cortex AISQL 不把 workload distribution 图混进 optimizer 机制图。baseline 出现在实验图或对照表中。

对本项目的要求：

> COPY、vLLM、Ray Serve、pgai、TurboVecDB 等应作为 baseline/边界出现在小注释或实验计划里，不应成为策略图主模块。

### 2.5 它们用“少量强符号”而不是大量 pill

顶会图通常用少量视觉符号稳定表达状态：

- queue / pool；
- batch / partition；
- scheduler / selector；
- engine / worker；
- metrics / feedback。

对本项目的要求：

> 当前 `optimization_strategy_logic` 的 pill 太多，适合作为设计备忘，但不是最终论文图。最终图应该减少文字，把 pill 替换成少量状态符号和一张小规则表。

---

## 3. 本项目策略图的推荐范式

推荐采用：

```text
Control-loop + running example + compact rule table
```

而不是：

```text
三列大框：输入信号 -> 规则表 -> 配置输出
```

原因：

- 纯三列图能解释“有哪些要素”，但不能解释“策略如何运行”。
- 控制循环能更清楚表达：阶段画像给状态，策略选择器读状态，执行配置改变同一次 SQL 内后续批次的 batch/route/writeback；离线实验阶段再把端到端指标回填到规则表。
- 这更接近 Orca/vLLM/Ray Data 的系统图语言。

---

## 4. 建议重绘的图形结构

### 4.1 Figure title

建议标题：

```text
Ours-v0：面向数据库 AI 算子的上游执行策略
```

不要写：

- “联合决策面”
- “端到端流程调优”
- “全链路优化器”

### 4.2 Hero panel：运行时控制循环

画布上半部分占 60%。

结构：

```text
Database AI query
  -> RecordBatch / partition queue
  -> Strategy selector
  -> Ray actor pool / GPU endpoint
  -> fan-in / sink
  -> E2E metrics
       └── feedback to Strategy selector
```

视觉要求：

- `RecordBatch / partition queue` 画成一叠 batch blocks。
- `Strategy selector` 画成一个小控制器，不画成巨大黑箱。
- `Ray actor pool / GPU endpoint` 画成 actor lanes + endpoint queue。
- `fan-in / sink` 画成 sink block，标注 writeback ratio。
- 实线表示数据流；虚线表示状态读取和反馈。

核心表达：

> 策略不是离线画表，而是在运行时根据已观测状态选择下一轮上游执行配置。

### 4.3 Lower panel：紧凑规则表

画布下半部分占 40%。

用 3 行规则表，而不是很多卡片：

| Trigger | Strategy action | Guardrail |
|---|---|---|
| `queue wait` 高 / endpoint backlog 高 | 降低或自适应 `K_max`，改 routing | 吞吐不能明显下降 |
| `GPU utilization` 低 / batch 太小 | 增大 batch，合并 invocation | P99 不能失控 |
| `writeback ratio` 高 | 切换 sink / write batch / worker-direct | 上游收益不能被写回吞噬 |
| `FILTER selectivity` 低 | smaller batch / cascade / early filter | 质量指标不下降 |
| `COMPLETE token` 长尾 / prefix 可复用 | token-aware / prefix-aware dispatch | queue wait 不转移到长请求 |

视觉要求：

- 只保留 4-5 条最关键规则。
- 第一列是可观测 trigger，不是抽象 workload 标签。
- 第二列是动作。
- 第三列是防过拟合/防副作用的 guardrail。

### 4.4 右侧小注：文献边界

可以放一个很小的 “borrowed from / used as baseline” 标签区：

```text
Borrowed ideas:
AI-aware selectivity | batching & backpressure | partition streaming

Baselines / boundaries:
vLLM / Ray Serve | COPY + deferred index | pgai / TurboVecDB
```

注意：这不是主图核心，只是防止评审误解为凭空设计。

---

## 5. 与现有两张图的关系

| 现有图 | 当前定位 | 后续处理 |
---|---|---|
| `cross_layer_method_framework.*` | 研究方案总览图 | 保留；回答“研究路线是什么” |
| `upstream_strategy_design.*` | 策略设计概览图 | 可保留作开题 PPT 过渡页；不是最终论文方法图 |
| `optimization_strategy_logic.*` | Ours-v0 规则表逻辑草图 | 应重构为 control-loop + running example；减少 pill |

---

## 6. 后续重绘的 Figure Contract

Core conclusion:

> Ours-v0 uses observed workload and service-state signals to choose the next upstream execution configuration, while end-to-end metrics guard against local optimizations that do not survive writeback.

Figure archetype:

> schematic-led composite.

Hero evidence:

> control loop from database AI query to batch queue, strategy selector, actor/endpoint execution, sink, and metric feedback.

Supporting evidence:

> compact trigger-action-guardrail rule table.

Reviewer risk:

- 如果图里全是术语，评审会认为只是工程调参。
- 如果图像数据库 optimizer，会被 Cortex/Smart/GaussML 对比打掉。
- 如果图像推理 engine，会被 vLLM/Orca 对比打掉。
- 如果图像 storage engine，会被 COPY/TurboVecDB/Delta 对比打掉。

Design guardrails:

- 不用 `RC/BL`。
- 不用 `vs`，除非比较对象明确。
- 不用 “联合决策面”“边界确认”。
- 箭头不交叉，不穿过卡片。
- 每个大模块最多 3 个关键词。
- 图注第一句必须说明策略主张。

---

## 7. 下一版图的具体修改清单

1. 将 `optimization_strategy_logic.*` 从三列规则图改为上半部 control loop、下半部 compact rule table。
2. 将 “输入信号” 四张卡片压缩为 control loop 中的可观测状态标签。
3. 将 “规则表选择器” 改为具体的 `Strategy selector`，内部只保留三项：`rule table`, `state snapshot`, `parameter sweep result`。
4. 将右侧三张动作卡压缩成对执行链路的配置标注：`batch/partition`, `K_max/routing`, `sink/write batch`。
5. 底部规则表改成 `Trigger -> Action -> Guardrail`，最多 5 行。
6. 用虚线回路从 `E2E metrics` 回到 `Strategy selector`，不要再用底部长条代替反馈。
7. 重新运行边框、箭头、关键词残留和 PNG 人工预览检查。
